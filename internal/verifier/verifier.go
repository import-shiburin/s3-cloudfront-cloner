package verifier

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"s3-cloudfront-cloner/internal/checksum"
	"s3-cloudfront-cloner/pkg/types"
)

// Verifier compares computed digests against the source object's metadata.
type Verifier struct{}

// NewVerifier returns a Verifier.
func NewVerifier() *Verifier {
	return &Verifier{}
}

// Result describes the outcome of a verification attempt.
type Result struct {
	Verified bool
	Method   string // e.g. "checksum:CRC64NVME/FullObject", "etag/composite", "rclone-md5"
	Reason   string // explanation when not verified
}

// AlgosNeeded returns the set of algorithms a Writer must compute to support
// verification of obj. Always includes MD5 (for ETag + rclone metadata) and
// adds any S3 native checksum algorithm the source has.
func AlgosNeeded(obj types.ObjectInfo) []checksum.Algorithm {
	algos := []checksum.Algorithm{checksum.AlgMD5}
	if obj.ChecksumCRC32 != "" {
		algos = append(algos, checksum.AlgCRC32)
	}
	if obj.ChecksumCRC32C != "" {
		algos = append(algos, checksum.AlgCRC32C)
	}
	if obj.ChecksumCRC64NVME != "" {
		algos = append(algos, checksum.AlgCRC64NVME)
	}
	if obj.ChecksumSHA1 != "" {
		algos = append(algos, checksum.AlgSHA1)
	}
	if obj.ChecksumSHA256 != "" {
		algos = append(algos, checksum.AlgSHA256)
	}
	return algos
}

// Verify tries each available source-side checksum in priority order:
//   1. Any FullObject S3 native checksum (CRC64NVME > SHA256 > SHA1 > CRC32C > CRC32)
//   2. rclone's x-amz-meta-md5chksum (full-object MD5)
//   3. Any Composite S3 native checksum (requires obj.PartSizes)
//   4. ETag (single-part MD5 or composite MD5-of-MD5s with -N)
// Returns the first success, or a failure with a reason describing why no
// method matched.
func (v *Verifier) Verify(obj types.ObjectInfo, sums map[checksum.Algorithm]checksum.Result) Result {
	tried := []string{}

	// Priority 1: FullObject S3 native checksums.
	priority := []struct {
		algo  checksum.Algorithm
		field string
	}{
		{checksum.AlgCRC64NVME, obj.ChecksumCRC64NVME},
		{checksum.AlgSHA256, obj.ChecksumSHA256},
		{checksum.AlgSHA1, obj.ChecksumSHA1},
		{checksum.AlgCRC32C, obj.ChecksumCRC32C},
		{checksum.AlgCRC32, obj.ChecksumCRC32},
	}
	for _, p := range priority {
		if p.field == "" || isComposite(p.field) {
			continue
		}
		r, ok := sums[p.algo]
		if !ok || r.FullObject == nil {
			continue
		}
		want := p.field
		got := base64.StdEncoding.EncodeToString(r.FullObject)
		if got == want {
			return Result{Verified: true, Method: "checksum:" + string(p.algo) + "/FullObject"}
		}
		tried = append(tried, fmt.Sprintf("%s(full)=%s want=%s", p.algo, got, want))
	}

	// Priority 2: rclone full-object MD5.
	if obj.RcloneMD5Base64 != "" {
		if r, ok := sums[checksum.AlgMD5]; ok && r.FullObject != nil {
			got := base64.StdEncoding.EncodeToString(r.FullObject)
			if got == obj.RcloneMD5Base64 {
				return Result{Verified: true, Method: "rclone-md5"}
			}
			tried = append(tried, fmt.Sprintf("rcloneMD5=%s want=%s", got, obj.RcloneMD5Base64))
		}
	}

	// Priority 3: Composite S3 native checksums (need PartSizes).
	for _, p := range priority {
		if p.field == "" || !isComposite(p.field) {
			continue
		}
		r, ok := sums[p.algo]
		if !ok || r.Composite == nil {
			continue
		}
		want := p.field
		got := fmt.Sprintf("%s-%d", base64.StdEncoding.EncodeToString(r.Composite), r.NumParts)
		if got == want {
			return Result{Verified: true, Method: "checksum:" + string(p.algo) + "/Composite"}
		}
		tried = append(tried, fmt.Sprintf("%s(composite)=%s want=%s", p.algo, got, want))
	}

	// Priority 4: ETag.
	sourceETag := strings.Trim(obj.ETag, "\"")
	if sourceETag != "" {
		if r, ok := sums[checksum.AlgMD5]; ok {
			var got string
			if strings.Contains(sourceETag, "-") {
				if r.Composite != nil && r.NumParts > 0 {
					got = fmt.Sprintf("%s-%d", hex.EncodeToString(r.Composite), r.NumParts)
				}
			} else if r.FullObject != nil {
				got = hex.EncodeToString(r.FullObject)
			}
			if got != "" && got == sourceETag {
				method := "etag/single"
				if strings.Contains(sourceETag, "-") {
					method = "etag/composite"
				}
				return Result{Verified: true, Method: method}
			}
			if got != "" {
				tried = append(tried, fmt.Sprintf("etag=%s want=%s", got, sourceETag))
			} else if strings.Contains(sourceETag, "-") {
				tried = append(tried, fmt.Sprintf("etag=multipart-%s but PartSizes missing", sourceETag))
			}
		}
	}

	if len(tried) == 0 {
		return Result{Verified: false, Reason: "no checksum or ETag available on source"}
	}
	return Result{Verified: false, Reason: "no method matched: " + strings.Join(tried, "; ")}
}

func isComposite(s string) bool {
	return strings.Contains(s, "-")
}
