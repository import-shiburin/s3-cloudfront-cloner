package uploader

import (
	"context"
	"fmt"
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

// Upload saves an object to the local filesystem
func (u *LocalUploader) Upload(ctx context.Context, obj types.ObjectInfo, data []byte) error {
	// Construct full path preserving directory structure
	fullPath := filepath.Join(u.basePath, obj.Key)

	// Create parent directories
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write file
	if err := os.WriteFile(fullPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
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
