package lister

import (
	"context"
	"fmt"

	"s3-cloudfront-cloner/pkg/types"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Lister handles listing objects from S3 buckets
type S3Lister struct {
	client *s3.Client
}

// NewS3Lister creates a new S3 lister with default AWS configuration
func NewS3Lister(ctx context.Context) (*S3Lister, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return &S3Lister{
		client: s3.NewFromConfig(cfg),
	}, nil
}

// ListObjects lists all objects in a bucket with optional prefix filtering
func (l *S3Lister) ListObjects(ctx context.Context, bucket, prefix string) ([]types.ObjectInfo, error) {
	var objects []types.ObjectInfo

	paginator := s3.NewListObjectsV2Paginator(l.client, &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			info := types.ObjectInfo{
				Key:  *obj.Key,
				Size: *obj.Size,
			}

			if obj.ETag != nil {
				info.ETag = *obj.ETag
			}
			if obj.LastModified != nil {
				info.LastModified = *obj.LastModified
			}
			if obj.StorageClass != "" {
				info.StorageClass = string(obj.StorageClass)
			}

			objects = append(objects, info)
		}
	}

	return objects, nil
}

// FetchMetadata retrieves extended metadata for an object using HeadObject
func (l *S3Lister) FetchMetadata(ctx context.Context, bucket string, obj *types.ObjectInfo) error {
	resp, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &obj.Key,
	})
	if err != nil {
		return fmt.Errorf("HeadObject failed: %w", err)
	}

	if resp.ContentType != nil {
		obj.ContentType = *resp.ContentType
	}
	if resp.ContentEncoding != nil {
		obj.ContentEncoding = *resp.ContentEncoding
	}
	if resp.ContentDisposition != nil {
		obj.ContentDisposition = *resp.ContentDisposition
	}
	if resp.ContentLanguage != nil {
		obj.ContentLanguage = *resp.ContentLanguage
	}
	if resp.CacheControl != nil {
		obj.CacheControl = *resp.CacheControl
	}
	if resp.Expires != nil {
		obj.Expires = resp.Expires
	}
	if resp.Metadata != nil {
		obj.Metadata = resp.Metadata
	}

	return nil
}
