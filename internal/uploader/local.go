package uploader

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"s3-cloudfront-cloner/pkg/types"
)

// LocalUploader saves objects to local filesystem
type LocalUploader struct {
	basePath string
}

// NewLocalUploader creates a new local filesystem uploader
func NewLocalUploader(basePath string) *LocalUploader {
	return &LocalUploader{
		basePath: basePath,
	}
}

// CreateFile preallocates a local file for direct parallel writes.
func (u *LocalUploader) CreateFile(obj types.ObjectInfo, size int64) (*os.File, error) {
	fullPath := filepath.Join(u.basePath, obj.Key)

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
	fullPath := filepath.Join(u.basePath, obj.Key)

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
