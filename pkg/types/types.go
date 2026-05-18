package types

import (
	"time"
)

// ObjectInfo contains all metadata for an S3 object
type ObjectInfo struct {
	Key          string    `json:"Key"`
	Size         int64     `json:"Size"`
	ETag         string    `json:"ETag"`
	LastModified time.Time `json:"LastModified"`
	StorageClass string    `json:"StorageClass"`

	// S3 native checksums. All are base64-encoded; composite multipart
	// checksums carry a "-N" suffix. Empty when the source object didn't
	// store that algorithm.
	ChecksumCRC32     string `json:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `json:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `json:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `json:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `json:"ChecksumSHA256,omitempty"`
	// ChecksumType is "FULL_OBJECT" or "COMPOSITE". FullObject checksums
	// can be compared directly even for multipart uploads.
	ChecksumType string `json:"ChecksumType,omitempty"`

	// RcloneMD5Base64 is the base64-encoded full-object MD5 that rclone
	// writes to x-amz-meta-md5chksum on uploads. Lets us verify multipart
	// objects rclone created without needing PartSizes.
	RcloneMD5Base64 string `json:"RcloneMD5Base64,omitempty"`

	// PartSizes lists the source upload's part sizes in order. Populated
	// from GetObjectAttributes for multipart sources so verification can
	// recompute composite checksums (and the composite ETag) using the
	// exact part boundaries the source used.
	PartSizes []int64 `json:"PartSizes,omitempty"`

	// Extended metadata (fetched via HeadObject)
	ContentType        string            `json:"ContentType,omitempty"`
	ContentEncoding    string            `json:"ContentEncoding,omitempty"`
	ContentDisposition string            `json:"ContentDisposition,omitempty"`
	ContentLanguage    string            `json:"ContentLanguage,omitempty"`
	CacheControl       string            `json:"CacheControl,omitempty"`
	Expires            *time.Time        `json:"Expires,omitempty"`
	Metadata           map[string]string `json:"Metadata,omitempty"`
}

// ListObjectsOutput represents the AWS CLI list-objects-v2 JSON format
type ListObjectsOutput struct {
	Contents []ObjectInfo `json:"Contents"`
}

// CloneConfig holds all configuration for a clone operation
type CloneConfig struct {
	// Source configuration
	SourceBucket string
	SourceFile   string
	SourceRegion string // overrides bucket region detection; "" means auto-detect
	Prefix       string

	// CloudFront configuration
	CloudFrontDomain string
	PrivateKeyPath   string
	KeyPairID        string
	CookieExpiry     time.Duration

	// Destination configuration
	DestLocal  string
	DestBucket string
	DestPrefix string
	DestRegion string // overrides bucket region detection; "" means auto-detect

	// Operation options
	Concurrency      int
	Verify           bool
	PreserveMetadata bool
	DryRun           bool
	SkipExisting     bool // skip objects whose destination already matches source checksum

	// Ranged download options
	RangeThreshold int64 // bytes; files larger than this use chunked Range requests
	ChunkSize      int64 // bytes; size of each Range request chunk
}

// DownloadResult represents the result of downloading a single object
type DownloadResult struct {
	Object         ObjectInfo
	Success        bool
	Error          error
	BytesRead      int64
	ChecksumSHA256 string // base64-encoded SHA256 of downloaded content
	Verified       bool
	VerifyFail     bool
	Skipped        bool // destination already matched source; nothing transferred
}

// CloneStats holds statistics for a clone operation
type CloneStats struct {
	TotalObjects     int
	SuccessCount     int
	FailedCount      int
	SkippedCount     int
	VerifiedCount    int
	VerifyFailCount  int
	TotalBytes       int64
	TransferredBytes int64
}
