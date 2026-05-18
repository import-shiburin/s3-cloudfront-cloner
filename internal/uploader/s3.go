package uploader

import (
	"context"
	"fmt"
	"io"

	"s3-cloudfront-cloner/pkg/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// NewS3Uploader creates a new S3 uploader
func NewS3Uploader(ctx context.Context, bucket, prefix string) (*S3Uploader, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
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

	// Preserve metadata
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

	// Use the upload manager which handles both small and large files,
	// and works with non-seekable readers (like io.PipeReader).
	_, err := u.uploader.Upload(ctx, input)
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}

	return nil
}
