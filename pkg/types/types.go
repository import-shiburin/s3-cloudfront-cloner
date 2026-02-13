package types

import (
	"time"
)

// ObjectInfo contains all metadata for an S3 object
type ObjectInfo struct {
	Key          string            `json:"Key"`
	Size         int64             `json:"Size"`
	ETag         string            `json:"ETag"`
	LastModified time.Time         `json:"LastModified"`
	StorageClass string            `json:"StorageClass"`

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

	// Operation options
	Concurrency      int
	Verify           bool
	PreserveMetadata bool
	DryRun           bool
}

// DownloadResult represents the result of downloading a single object
type DownloadResult struct {
	Object     ObjectInfo
	Success    bool
	Error      error
	BytesRead  int64
	MD5Hash    string
	Verified   bool
	VerifyFail bool
}

// CloneStats holds statistics for a clone operation
type CloneStats struct {
	TotalObjects     int
	SuccessCount     int
	FailedCount      int
	VerifiedCount    int
	VerifyFailCount  int
	TotalBytes       int64
	TransferredBytes int64
}
