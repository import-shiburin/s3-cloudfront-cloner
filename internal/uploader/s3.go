package uploader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"s3-cloudfront-cloner/internal/checksum"
	"s3-cloudfront-cloner/pkg/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3 multipart upload limits.
const (
	S3MinPartSize int64 = 5 * 1024 * 1024        // 5 MiB
	S3MaxPartSize int64 = 5 * 1024 * 1024 * 1024 // 5 GiB
	S3MaxParts    int64 = 10000
)

// Uploader interface for uploading objects
type Uploader interface {
	Upload(ctx context.Context, obj types.ObjectInfo, r io.Reader) error
}

// S3Uploader uploads objects to an S3 bucket
type S3Uploader struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
	prefix   string
}

// NewS3Uploader creates a new S3 uploader. If region is non-empty it overrides
// whatever the default AWS config resolves; pass "" to use the default.
func NewS3Uploader(ctx context.Context, bucket, prefix, region string) (*S3Uploader, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	if region != "" {
		cfg.Region = region
	}

	client := s3.NewFromConfig(cfg)

	// Use multipart upload for files > 5MB
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024 // 5MB
	})

	return &S3Uploader{
		client:   client,
		uploader: uploader,
		bucket:   bucket,
		prefix:   prefix,
	}, nil
}

// Upload streams an object to S3 with metadata preservation
func (u *S3Uploader) Upload(ctx context.Context, obj types.ObjectInfo, r io.Reader) error {
	key := u.prefix + obj.Key

	input := &s3.PutObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
		Body:   r,
	}

	applyMetadata(input, obj)

	// Use the upload manager which handles both small and large files,
	// and works with non-seekable readers (like io.PipeReader).
	_, err := u.uploader.Upload(ctx, input)
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	return nil
}

// UploadMultipart performs a parallel S3 multipart upload, fetching each part
// concurrently via the supplied downloadChunk callback. Each chunk maps to one
// multipart part, so chunkSize must satisfy S3 limits (5 MiB to 5 GiB). The
// requested digests are computed in source-aligned order across all chunks
// (using obj.PartSizes when present) and returned for verification.
// On failure, the multipart upload is aborted to avoid orphaned parts.
func (u *S3Uploader) UploadMultipart(
	ctx context.Context,
	obj types.ObjectInfo,
	size, chunkSize int64,
	algos []checksum.Algorithm,
	downloadChunk func(ctx context.Context, start, end int64) ([]byte, error),
) (int64, map[checksum.Algorithm]checksum.Result, error) {
	// Each in-flight chunk holds chunkSize bytes in memory (vs the local-dest
	// path which streams via 32 KB buffers), so this is bounded conservatively.
	// Peak memory ≈ chunkConcurrency * chunkSize per object.
	const chunkConcurrency = 4

	if size <= 0 {
		return 0, nil, fmt.Errorf("size must be positive, got %d", size)
	}
	if chunkSize < S3MinPartSize {
		return 0, nil, fmt.Errorf("chunk-size %d is below S3 minimum part size %d (5 MiB)", chunkSize, S3MinPartSize)
	}
	if chunkSize > S3MaxPartSize {
		return 0, nil, fmt.Errorf("chunk-size %d exceeds S3 maximum part size %d (5 GiB)", chunkSize, S3MaxPartSize)
	}

	numChunks := (size + chunkSize - 1) / chunkSize
	if numChunks > S3MaxParts {
		return 0, nil, fmt.Errorf("file size %d with chunk-size %d would require %d parts (S3 limit is %d); increase chunk-size", size, chunkSize, numChunks, S3MaxParts)
	}

	key := u.prefix + obj.Key

	createInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
	}
	applyCreateMultipartMetadata(createInput, obj)

	initResp, err := u.client.CreateMultipartUpload(ctx, createInput)
	if err != nil {
		return 0, nil, fmt.Errorf("create multipart upload: %w", err)
	}
	uploadID := initResp.UploadId

	type chunkResult struct {
		bytes []byte
		etag  *string
		err   error
	}

	// Workers wait on per-chunk goSignals before starting. The accumulator
	// closes goSignals[i+chunkConcurrency] after processing chunk i, which
	// keeps the in-flight window bounded to chunkConcurrency *and* guarantees
	// that chunks start in ascending order — preventing the head-of-line
	// deadlock where late chunks would grab a global semaphore first, block on
	// send to their unbuffered result slot, and prevent earlier chunks (which
	// the accumulator is waiting on) from ever starting.
	results := make([]chan chunkResult, numChunks)
	goSignals := make([]chan struct{}, numChunks)
	for i := range results {
		results[i] = make(chan chunkResult)
		goSignals[i] = make(chan struct{})
	}
	for i := int64(0); i < chunkConcurrency && i < numChunks; i++ {
		close(goSignals[i])
	}

	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for i := int64(0); i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize - 1
		if end >= size {
			end = size - 1
		}
		idx := i
		partNum := int32(idx + 1)

		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case <-goSignals[idx]:
			case <-workerCtx.Done():
				results[idx] <- chunkResult{err: workerCtx.Err()}
				return
			}

			data, dlErr := downloadChunk(workerCtx, start, end)
			if dlErr != nil {
				results[idx] <- chunkResult{err: fmt.Errorf("download [%d-%d]: %w", start, end, dlErr)}
				return
			}

			partResp, upErr := u.client.UploadPart(workerCtx, &s3.UploadPartInput{
				Bucket:     aws.String(u.bucket),
				Key:        aws.String(key),
				PartNumber: aws.Int32(partNum),
				UploadId:   uploadID,
				Body:       bytes.NewReader(data),
			})
			if upErr != nil {
				results[idx] <- chunkResult{err: fmt.Errorf("upload part %d: %w", partNum, upErr)}
				return
			}

			results[idx] <- chunkResult{bytes: data, etag: partResp.ETag}
		}()
	}

	cw := checksum.NewWriter(algos, obj.PartSizes)
	completedParts := make([]s3types.CompletedPart, 0, numChunks)
	var totalBytes int64
	var firstErr error

	for i := int64(0); i < numChunks; i++ {
		res := <-results[i]

		if res.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("chunk %d: %w", i+1, res.err)
				cancel()
			}
		} else if firstErr == nil {
			cw.Write(res.bytes)
			totalBytes += int64(len(res.bytes))
			completedParts = append(completedParts, s3types.CompletedPart{
				ETag:       res.etag,
				PartNumber: aws.Int32(int32(i + 1)),
			})
		}

		if next := i + chunkConcurrency; next < numChunks {
			close(goSignals[next])
		}
	}

	wg.Wait()

	if firstErr != nil {
		u.abortMultipart(key, uploadID)
		return totalBytes, nil, firstErr
	}

	if _, err := u.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(u.bucket),
		Key:             aws.String(key),
		UploadId:        uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: completedParts},
	}); err != nil {
		u.abortMultipart(key, uploadID)
		return totalBytes, nil, fmt.Errorf("complete multipart upload: %w", err)
	}

	return totalBytes, cw.Sums(), nil
}

// abortMultipart cleans up an in-flight multipart upload so failed runs don't
// accrue storage charges. Uses a fresh context so cancellation of the parent
// doesn't also kill the abort.
func (u *S3Uploader) abortMultipart(key string, uploadID *string) {
	_, err := u.client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to abort multipart upload for %s: %v\n", key, err)
	}
}

func applyMetadata(input *s3.PutObjectInput, obj types.ObjectInfo) {
	if obj.ContentType != "" {
		input.ContentType = aws.String(obj.ContentType)
	}
	if obj.ContentEncoding != "" {
		input.ContentEncoding = aws.String(obj.ContentEncoding)
	}
	if obj.ContentDisposition != "" {
		input.ContentDisposition = aws.String(obj.ContentDisposition)
	}
	if obj.ContentLanguage != "" {
		input.ContentLanguage = aws.String(obj.ContentLanguage)
	}
	if obj.CacheControl != "" {
		input.CacheControl = aws.String(obj.CacheControl)
	}
	if obj.Expires != nil {
		input.Expires = obj.Expires
	}
	if len(obj.Metadata) > 0 {
		input.Metadata = obj.Metadata
	}
}

func applyCreateMultipartMetadata(input *s3.CreateMultipartUploadInput, obj types.ObjectInfo) {
	if obj.ContentType != "" {
		input.ContentType = aws.String(obj.ContentType)
	}
	if obj.ContentEncoding != "" {
		input.ContentEncoding = aws.String(obj.ContentEncoding)
	}
	if obj.ContentDisposition != "" {
		input.ContentDisposition = aws.String(obj.ContentDisposition)
	}
	if obj.ContentLanguage != "" {
		input.ContentLanguage = aws.String(obj.ContentLanguage)
	}
	if obj.CacheControl != "" {
		input.CacheControl = aws.String(obj.CacheControl)
	}
	if obj.Expires != nil {
		input.Expires = obj.Expires
	}
	if len(obj.Metadata) > 0 {
		input.Metadata = obj.Metadata
	}
}
