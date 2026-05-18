package lister

import (
	"context"
	"fmt"
	"strings"

	"s3-cloudfront-cloner/pkg/types"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Lister handles listing objects from S3 buckets
type S3Lister struct {
	client *s3.Client
}

// NewS3Lister creates a new S3 lister. If region is non-empty it overrides
// whatever the default AWS config resolves; pass "" to use the default.
func NewS3Lister(ctx context.Context, region string) (*S3Lister, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}
	if region != "" {
		cfg.Region = region
	}

	return &S3Lister{
		client: s3.NewFromConfig(cfg),
	}, nil
}

// DetectBucketRegion returns the region for the given bucket via the SDK's
// GetBucketRegion helper (handles legacy LocationConstraint mappings like
// "" -> "us-east-1" and "EU" -> "eu-west-1"). Uses default AWS config to
// bootstrap; the underlying request follows S3 redirects across regions.
func DetectBucketRegion(ctx context.Context, bucket string) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to load AWS config: %w", err)
	}
	client := s3.NewFromConfig(cfg)
	region, err := manager.GetBucketRegion(ctx, client, bucket)
	if err != nil {
		return "", fmt.Errorf("GetBucketRegion(%s): %w", bucket, err)
	}
	return region, nil
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

// FetchMetadata retrieves extended metadata for an object using HeadObject.
// ChecksumMode=ENABLED is set so any x-amz-checksum-* headers are returned.
func (l *S3Lister) FetchMetadata(ctx context.Context, bucket string, obj *types.ObjectInfo) error {
	resp, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       &bucket,
		Key:          &obj.Key,
		ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil {
		return fmt.Errorf("HeadObject failed: %w", err)
	}

	applyHeadObjectChecksums(obj, resp)

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

// FetchChecksum fetches all x-amz-checksum-* headers and rclone's
// x-amz-meta-md5chksum via HeadObject. Cheaper than FetchMetadata when full
// metadata isn't needed.
func (l *S3Lister) FetchChecksum(ctx context.Context, bucket string, obj *types.ObjectInfo) error {
	resp, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       &bucket,
		Key:          &obj.Key,
		ChecksumMode: s3types.ChecksumModeEnabled,
	})
	if err != nil {
		return fmt.Errorf("HeadObject failed: %w", err)
	}
	applyHeadObjectChecksums(obj, resp)
	if resp.Metadata != nil {
		obj.Metadata = resp.Metadata
	}
	return nil
}

// applyHeadObjectChecksums copies all S3 native checksums and the rclone
// md5chksum metadata onto the ObjectInfo.
func applyHeadObjectChecksums(obj *types.ObjectInfo, resp *s3.HeadObjectOutput) {
	if resp.ChecksumCRC32 != nil {
		obj.ChecksumCRC32 = *resp.ChecksumCRC32
	}
	if resp.ChecksumCRC32C != nil {
		obj.ChecksumCRC32C = *resp.ChecksumCRC32C
	}
	if resp.ChecksumCRC64NVME != nil {
		obj.ChecksumCRC64NVME = *resp.ChecksumCRC64NVME
	}
	if resp.ChecksumSHA1 != nil {
		obj.ChecksumSHA1 = *resp.ChecksumSHA1
	}
	if resp.ChecksumSHA256 != nil {
		obj.ChecksumSHA256 = *resp.ChecksumSHA256
	}
	if resp.ChecksumType != "" {
		obj.ChecksumType = string(resp.ChecksumType)
	}
	// rclone writes "x-amz-meta-md5chksum: <base64-md5>". S3 SDK normalizes
	// metadata keys to lowercase and strips the "x-amz-meta-" prefix, so we
	// look up "md5chksum" case-insensitively.
	for k, v := range resp.Metadata {
		if strings.EqualFold(k, "md5chksum") {
			obj.RcloneMD5Base64 = v
			break
		}
	}
}

// FetchPartSizes fetches per-part sizes via GetObjectAttributes. Only meaningful
// for multipart sources (ETag ending in "-N"). Pages through if S3 returns the
// part list in segments.
//
// Asks for ETag + ObjectParts together — some S3 paths only populate ObjectParts
// when ETag is also requested in the same call.
func (l *S3Lister) FetchPartSizes(ctx context.Context, bucket string, obj *types.ObjectInfo) error {
	if !strings.Contains(strings.Trim(obj.ETag, "\""), "-") {
		return nil // not multipart, nothing to fetch
	}

	var (
		sizes            []int64
		partNumberMarker *string
		lastResp         *s3.GetObjectAttributesOutput
	)

	for {
		resp, err := l.client.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: &bucket,
			Key:    &obj.Key,
			ObjectAttributes: []s3types.ObjectAttributes{
				s3types.ObjectAttributesEtag,
				s3types.ObjectAttributesObjectParts,
			},
			PartNumberMarker: partNumberMarker,
		})
		if err != nil {
			return fmt.Errorf("GetObjectAttributes failed: %w", err)
		}
		lastResp = resp

		if resp.ObjectParts != nil {
			for _, p := range resp.ObjectParts.Parts {
				if p.Size != nil {
					sizes = append(sizes, *p.Size)
				}
			}

			if resp.ObjectParts.NextPartNumberMarker == nil || *resp.ObjectParts.NextPartNumberMarker == "" {
				break
			}
			partNumberMarker = resp.ObjectParts.NextPartNumberMarker
			continue
		}
		break
	}

	if len(sizes) == 0 {
		// Sharpen the diagnostic so we can tell *why* parts came back empty.
		switch {
		case lastResp == nil:
			return fmt.Errorf("GetObjectAttributes returned no response")
		case lastResp.ObjectParts == nil:
			return fmt.Errorf("GetObjectAttributes returned no ObjectParts element (object may not be a true multipart upload — multipart-style ETags can appear for SSE-KMS or some copied objects)")
		case lastResp.ObjectParts.TotalPartsCount != nil && *lastResp.ObjectParts.TotalPartsCount > 0:
			return fmt.Errorf("GetObjectAttributes returned TotalPartsCount=%d but Parts list was empty (possible SDK/response parsing issue)", *lastResp.ObjectParts.TotalPartsCount)
		default:
			return fmt.Errorf("GetObjectAttributes returned ObjectParts with no Parts and TotalPartsCount=0 (S3 has no part metadata for this object)")
		}
	}
	obj.PartSizes = sizes
	return nil
}
