package uploader

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"s3-cloudfront-cloner/internal/checksum"
	"s3-cloudfront-cloner/internal/verifier"
	"s3-cloudfront-cloner/pkg/types"
)

// LocalUploader saves objects to local filesystem
type LocalUploader struct {
	basePath    string
	stripPrefix string
}

// NewLocalUploader creates a new local filesystem uploader. stripPrefix is
// removed from each source key before joining with basePath, mirroring the
// behavior of NewS3Uploader so source paths can be collapsed onto a different
// destination layout.
func NewLocalUploader(basePath, stripPrefix string) *LocalUploader {
	return &LocalUploader{
		basePath:    basePath,
		stripPrefix: stripPrefix,
	}
}

// destPath strips stripPrefix from srcKey (when set) and joins it with basePath.
func (u *LocalUploader) destPath(srcKey string) string {
	k := srcKey
	if u.stripPrefix != "" {
		k = strings.TrimPrefix(k, u.stripPrefix)
		k = strings.TrimPrefix(k, "/")
	}
	return filepath.Join(u.basePath, k)
}

// IsIdentical reads the existing local file (if any) at obj.Key, computes the
// algorithms needed to verify against the source's checksums, and reports
// whether they match. Returns (false, nil) when the file is missing or the
// size differs; reads the file only when size already matches, so the
// happy-path cost is one stat for missing/mismatched files.
func (u *LocalUploader) IsIdentical(ctx context.Context, obj types.ObjectInfo) (bool, error) {
	fullPath := u.destPath(obj.Key)

	info, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat %s: %w", fullPath, err)
	}
	if info.Size() != obj.Size {
		return false, nil
	}

	algos := verifier.AlgosNeeded(obj)
	f, err := os.Open(fullPath)
	if err != nil {
		return false, fmt.Errorf("open %s: %w", fullPath, err)
	}
	defer f.Close()

	cw := checksum.NewWriter(algos, obj.PartSizes)
	buf := make([]byte, 4*1024*1024)
	if _, err := io.CopyBuffer(cw, f, buf); err != nil {
		return false, fmt.Errorf("read %s: %w", fullPath, err)
	}

	return verifier.NewVerifier().Verify(obj, cw.Sums()).Verified, nil
}

// CreateFile preallocates a local file for direct parallel writes.
func (u *LocalUploader) CreateFile(obj types.ObjectInfo, size int64) (*os.File, error) {
	fullPath := u.destPath(obj.Key)

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	if err := f.Truncate(size); err != nil {
		f.Close()
		os.Remove(fullPath)
		return nil, fmt.Errorf("failed to preallocate file: %w", err)
	}

	return f, nil
}

// FinishFile closes the file and sets its modification time.
func (u *LocalUploader) FinishFile(f *os.File, obj types.ObjectInfo) error {
	name := f.Name()
	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}
	if !obj.LastModified.IsZero() {
		if err := os.Chtimes(name, obj.LastModified, obj.LastModified); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to set mtime for %s: %v\n", name, err)
		}
	}
	return nil
}

// Upload streams an object to the local filesystem
func (u *LocalUploader) Upload(ctx context.Context, obj types.ObjectInfo, r io.Reader) error {
	// Construct full path preserving directory structure
	fullPath := u.destPath(obj.Key)

	// Create parent directories
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Stream to file
	f, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	_, copyErr := io.Copy(f, r)
	closeErr := f.Close()

	if copyErr != nil {
		os.Remove(fullPath)
		return fmt.Errorf("failed to write file: %w", copyErr)
	}
	if closeErr != nil {
		os.Remove(fullPath)
		return fmt.Errorf("failed to close file: %w", closeErr)
	}

	// Set modification time from LastModified
	if !obj.LastModified.IsZero() {
		if err := os.Chtimes(fullPath, obj.LastModified, obj.LastModified); err != nil {
			// Log warning but don't fail
			fmt.Fprintf(os.Stderr, "Warning: failed to set mtime for %s: %v\n", fullPath, err)
		}
	}

	return nil
}
