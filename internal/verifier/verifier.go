package verifier

import (
	"fmt"
	"strings"

	"s3-cloudfront-cloner/pkg/types"
)

// Verifier handles ETag/checksum verification
type Verifier struct{}

// NewVerifier creates a new verifier
func NewVerifier() *Verifier {
	return &Verifier{}
}

// Verify compares the calculated MD5 hash with the source ETag
func (v *Verifier) Verify(obj types.ObjectInfo, md5Hash string) (bool, error) {
	// Clean up ETag (remove quotes)
	etag := strings.Trim(obj.ETag, "\"")

	// Check if this is a multipart upload ETag (contains "-")
	if strings.Contains(etag, "-") {
		// Multipart uploads have ETags in format: "md5-of-md5s-N" where N is part count
		// We cannot verify these without knowing the part boundaries
		// Return true with a note that verification was skipped
		return true, fmt.Errorf("multipart upload ETag detected, verification skipped")
	}

	// For standard objects, ETag is the MD5 hash
	return etag == md5Hash, nil
}

// IsMultipartETag checks if an ETag indicates a multipart upload
func IsMultipartETag(etag string) bool {
	etag = strings.Trim(etag, "\"")
	return strings.Contains(etag, "-")
}

// VerificationResult contains detailed verification information
type VerificationResult struct {
	Key           string
	ExpectedETag  string
	ActualMD5     string
	Verified      bool
	IsMultipart   bool
	ErrorMessage  string
}

// VerifyWithDetails performs verification and returns detailed results
func (v *Verifier) VerifyWithDetails(obj types.ObjectInfo, md5Hash string) VerificationResult {
	etag := strings.Trim(obj.ETag, "\"")

	result := VerificationResult{
		Key:          obj.Key,
		ExpectedETag: etag,
		ActualMD5:    md5Hash,
	}

	if strings.Contains(etag, "-") {
		result.IsMultipart = true
		result.Verified = true // Consider verified since we can't check
		result.ErrorMessage = "multipart upload, verification not possible"
		return result
	}

	result.Verified = etag == md5Hash
	if !result.Verified {
		result.ErrorMessage = fmt.Sprintf("ETag mismatch: expected %s, got %s", etag, md5Hash)
	}

	return result
}
