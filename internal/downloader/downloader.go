package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"s3-cloudfront-cloner/internal/checksum"
	"s3-cloudfront-cloner/internal/cloudfront"
)

// Downloader handles downloading objects from CloudFront
type Downloader struct {
	cloudFrontDomain string
	cookies          *cloudfront.SignedCookies
	client           *http.Client
	maxRetries       int
	retryDelay       time.Duration
	rangeThreshold   int64
	chunkSize        int64
}

// NewDownloader creates a new CloudFront downloader
func NewDownloader(cloudFrontDomain string, cookies *cloudfront.SignedCookies, rangeThreshold, chunkSize int64) *Downloader {
	return &Downloader{
		cloudFrontDomain: cloudFrontDomain,
		cookies:          cookies,
		client: &http.Client{
			Timeout: 5 * time.Minute,
		},
		maxRetries:     3,
		retryDelay:     time.Second,
		rangeThreshold: rangeThreshold,
		chunkSize:      chunkSize,
	}
}

// StreamDownload downloads an object and streams it to a writer while computing
// the requested digests. partSizes carries the source's multipart boundaries
// (empty for single-part sources) so composite digests match what S3 stores.
// If the file size exceeds the range threshold, chunked Range requests are used.
func (d *Downloader) StreamDownload(ctx context.Context, key string, size int64, algos []checksum.Algorithm, partSizes []int64, w io.Writer) (int64, map[checksum.Algorithm]checksum.Result, error) {
	if size > 0 && size > d.rangeThreshold {
		// If the writer supports WriteAt (e.g. *os.File), use fully parallel downloads.
		if wa, ok := w.(io.WriterAt); ok {
			return d.parallelRangedDownload(ctx, key, size, algos, partSizes, wa)
		}
		return d.rangedStreamDownload(ctx, key, size, algos, partSizes, w)
	}
	return d.singleStreamDownload(ctx, key, algos, partSizes, w)
}

// singleStreamDownload is the original single-request download path.
func (d *Downloader) singleStreamDownload(ctx context.Context, key string, algos []checksum.Algorithm, partSizes []int64, w io.Writer) (int64, map[checksum.Algorithm]checksum.Result, error) {
	url := d.buildURL(key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to create request: %w", err)
	}

	d.addCookies(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	cw := checksum.NewWriter(algos, partSizes)
	multiWriter := io.MultiWriter(w, cw)

	n, err := io.Copy(multiWriter, resp.Body)
	if err != nil {
		return n, nil, fmt.Errorf("failed to stream response: %w", err)
	}

	return n, cw.Sums(), nil
}

// rangedStreamDownload downloads a file using multiple concurrent HTTP Range
// requests. Chunks are downloaded in parallel (up to 4 at a time) but written
// to the output in sequential order to preserve correctness.
func (d *Downloader) rangedStreamDownload(ctx context.Context, key string, size int64, algos []checksum.Algorithm, partSizes []int64, w io.Writer) (int64, map[checksum.Algorithm]checksum.Result, error) {
	const chunkConcurrency = 4

	url := d.buildURL(key)
	cw := checksum.NewWriter(algos, partSizes)
	multiWriter := io.MultiWriter(w, cw)

	numChunks := (size + d.chunkSize - 1) / d.chunkSize

	type chunkResult struct {
		data []byte
		err  error
	}

	// One channel per chunk so we can consume results in order.
	results := make([]chan chunkResult, numChunks)
	for i := range results {
		results[i] = make(chan chunkResult, 1)
	}

	// Child context lets us cancel in-flight downloads on failure.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, chunkConcurrency)

	for i := int64(0); i < numChunks; i++ {
		start := i * d.chunkSize
		end := start + d.chunkSize - 1
		if end >= size {
			end = size - 1
		}
		idx := i

		go func() {
			// Acquire semaphore or bail on cancellation.
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[idx] <- chunkResult{nil, ctx.Err()}
				return
			}
			defer func() { <-sem }()

			data, err := d.downloadChunkWithRetry(ctx, url, start, end)
			results[idx] <- chunkResult{data, err}
		}()
	}

	// Write chunks in order.
	var totalWritten int64
	for i := int64(0); i < numChunks; i++ {
		start := i * d.chunkSize
		end := start + d.chunkSize - 1
		if end >= size {
			end = size - 1
		}

		res := <-results[i]
		if res.err != nil {
			return totalWritten, nil, fmt.Errorf("chunk [%d-%d] failed: %w", start, end, res.err)
		}

		n, err := multiWriter.Write(res.data)
		totalWritten += int64(n)
		if err != nil {
			return totalWritten, nil, fmt.Errorf("failed to write chunk [%d-%d]: %w", start, end, err)
		}
	}

	return totalWritten, cw.Sums(), nil
}

// parallelRangedDownload downloads a file using fully parallel HTTP Range
// requests with WriteAt. Each goroutine streams its chunk directly to the
// file at the correct offset using a small buffer (~32 KB). Hashes are
// computed by re-reading the completed file afterward.
func (d *Downloader) parallelRangedDownload(ctx context.Context, key string, size int64, algos []checksum.Algorithm, partSizes []int64, w io.WriterAt) (int64, map[checksum.Algorithm]checksum.Result, error) {
	const chunkConcurrency = 8

	url := d.buildURL(key)
	numChunks := (size + d.chunkSize - 1) / d.chunkSize

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type chunkResult struct {
		n   int64
		err error
	}

	results := make([]chan chunkResult, numChunks)
	for i := range results {
		results[i] = make(chan chunkResult, 1)
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, chunkConcurrency)

	for i := int64(0); i < numChunks; i++ {
		start := i * d.chunkSize
		end := start + d.chunkSize - 1
		if end >= size {
			end = size - 1
		}
		idx := i

		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[idx] <- chunkResult{0, ctx.Err()}
				return
			}
			defer func() { <-sem }()

			n, err := d.streamChunkWithRetry(ctx, url, start, end, w)
			results[idx] <- chunkResult{n, err}
		}()
	}

	// Collect results — order only matters for error reporting.
	var totalWritten int64
	var firstErr error
	for i := int64(0); i < numChunks; i++ {
		res := <-results[i]
		if res.err != nil && firstErr == nil {
			start := i * d.chunkSize
			end := start + d.chunkSize - 1
			if end >= size {
				end = size - 1
			}
			firstErr = fmt.Errorf("chunk [%d-%d] failed: %w", start, end, res.err)
			cancel()
		}
		totalWritten += res.n
	}

	wg.Wait() // ensure all goroutines finish before we return

	if firstErr != nil {
		return totalWritten, nil, firstErr
	}

	// Compute hashes by re-reading the written data. Use a 4MB buffer so we
	// issue large sequential reads instead of millions of 32KB ReadAt syscalls.
	ra, ok := w.(io.ReaderAt)
	if !ok {
		return totalWritten, nil, nil
	}
	cw := checksum.NewWriter(algos, partSizes)
	buf := make([]byte, 4*1024*1024)
	if _, err := io.CopyBuffer(cw, io.NewSectionReader(ra, 0, size), buf); err != nil {
		return totalWritten, nil, fmt.Errorf("failed to compute hashes: %w", err)
	}
	return totalWritten, cw.Sums(), nil
}

// streamChunkWithRetry streams a chunk directly to a WriterAt with retries.
// On retry the entire chunk range is re-downloaded, overwriting any partial data.
func (d *Downloader) streamChunkWithRetry(ctx context.Context, url string, start, end int64, w io.WriterAt) (int64, error) {
	const maxChunkRetries = 5
	var lastErr error

	for attempt := 0; attempt <= maxChunkRetries; attempt++ {
		if attempt > 0 {
			delay := d.retryDelay * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(delay):
			}
		}

		n, err := d.streamChunk(ctx, url, start, end, w)
		if err == nil {
			return n, nil
		}
		lastErr = err
	}

	return 0, fmt.Errorf("failed after %d retries: %w", maxChunkRetries, lastErr)
}

// streamChunk issues a Range request and streams the response body directly
// to a WriterAt at the correct offset using a small read buffer.
func (d *Downloader) streamChunk(ctx context.Context, url string, start, end int64, w io.WriterAt) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	d.addCookies(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("expected 206 Partial Content, got %d", resp.StatusCode)
	}

	buf := make([]byte, 32*1024)
	var written int64
	offset := start
	for {
		nr, readErr := resp.Body.Read(buf)
		if nr > 0 {
			nw, writeErr := w.WriteAt(buf[:nr], offset)
			written += int64(nw)
			offset += int64(nw)
			if writeErr != nil {
				return written, fmt.Errorf("write failed: %w", writeErr)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return written, fmt.Errorf("read failed: %w", readErr)
		}
	}

	return written, nil
}

// DownloadChunk fetches a single byte-range chunk from CloudFront with retries.
// Used by callers that orchestrate their own chunked download (e.g. S3 multipart upload).
func (d *Downloader) DownloadChunk(ctx context.Context, key string, start, end int64) ([]byte, error) {
	url := d.buildURL(key)
	return d.downloadChunkWithRetry(ctx, url, start, end)
}

// downloadChunkWithRetry attempts to download a single byte-range chunk with up to 5 retries.
func (d *Downloader) downloadChunkWithRetry(ctx context.Context, url string, start, end int64) ([]byte, error) {
	const maxChunkRetries = 5
	var lastErr error

	for attempt := 0; attempt <= maxChunkRetries; attempt++ {
		if attempt > 0 {
			delay := d.retryDelay * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		data, err := d.downloadChunk(ctx, url, start, end)
		if err == nil {
			return data, nil
		}
		lastErr = err
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxChunkRetries, lastErr)
}

// downloadChunk issues a single HTTP Range request and returns the response body.
// Reads into a pre-allocated buffer of the exact chunk size to avoid the
// geometric-growth overhead of io.ReadAll.
func (d *Downloader) downloadChunk(ctx context.Context, url string, start, end int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	d.addCookies(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("expected 206 Partial Content, got %d", resp.StatusCode)
	}

	chunkLen := end - start + 1
	data := make([]byte, chunkLen)
	if _, err := io.ReadFull(resp.Body, data); err != nil {
		return nil, fmt.Errorf("failed to read chunk body: %w", err)
	}

	return data, nil
}

// buildURL turns an S3 object key into a properly escaped CloudFront URL.
// Without escaping, keys containing '#' would be truncated at the fragment
// delimiter before the request is sent, and keys with spaces / non-ASCII /
// other reserved characters would either fail to parse or hit CloudFront
// with a path that no longer matches the stored object — both surface as 403.
func (d *Downloader) buildURL(key string) string {
	u := &url.URL{
		Scheme: "https",
		Host:   d.cloudFrontDomain,
		Path:   "/" + key,
	}
	return u.String()
}

// addCookies attaches the CloudFront signed cookies to an HTTP request.
func (d *Downloader) addCookies(req *http.Request) {
	req.AddCookie(&http.Cookie{
		Name:  "CloudFront-Policy",
		Value: d.cookies.Policy,
	})
	req.AddCookie(&http.Cookie{
		Name:  "CloudFront-Signature",
		Value: d.cookies.Signature,
	})
	req.AddCookie(&http.Cookie{
		Name:  "CloudFront-Key-Pair-Id",
		Value: d.cookies.KeyPairID,
	})
}
