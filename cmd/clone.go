package cmd

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"s3-cloudfront-cloner/internal/cloudfront"
	"s3-cloudfront-cloner/internal/downloader"
	"s3-cloudfront-cloner/internal/lister"
	"s3-cloudfront-cloner/internal/uploader"
	"s3-cloudfront-cloner/internal/verifier"
	"s3-cloudfront-cloner/pkg/types"

	"github.com/spf13/cobra"
)

var cloneCmd = &cobra.Command{
	Use:   "clone",
	Short: "Clone objects from S3 via CloudFront",
	Long: `Clone S3 objects via CloudFront using signed cookies.
Objects can be downloaded to local storage or uploaded to another S3 bucket.`,
	RunE: runClone,
}

var config types.CloneConfig

func init() {
	rootCmd.AddCommand(cloneCmd)

	// Source flags
	cloneCmd.Flags().StringVar(&config.SourceBucket, "source-bucket", "", "Source S3 bucket name")
	cloneCmd.Flags().StringVar(&config.SourceFile, "source-file", "", "JSON file with object list (AWS CLI format)")
	cloneCmd.Flags().StringVar(&config.Prefix, "prefix", "", "Prefix to filter objects")

	// CloudFront flags
	cloneCmd.Flags().StringVar(&config.CloudFrontDomain, "cloudfront-domain", "", "CloudFront distribution domain (required)")
	cloneCmd.Flags().StringVar(&config.PrivateKeyPath, "private-key", "", "Path to CloudFront private key PEM file (required)")
	cloneCmd.Flags().StringVar(&config.KeyPairID, "key-pair-id", "", "CloudFront key pair ID (required)")
	cloneCmd.Flags().DurationVar(&config.CookieExpiry, "cookie-expiry", 24*time.Hour, "Cookie expiration duration")

	// Destination flags
	cloneCmd.Flags().StringVar(&config.DestLocal, "dest-local", "", "Local directory for downloads")
	cloneCmd.Flags().StringVar(&config.DestBucket, "dest-bucket", "", "Destination S3 bucket")
	cloneCmd.Flags().StringVar(&config.DestPrefix, "dest-prefix", "", "Prefix for destination objects")

	// Operation flags
	cloneCmd.Flags().IntVar(&config.Concurrency, "concurrency", 10, "Number of parallel downloads")
	cloneCmd.Flags().BoolVar(&config.Verify, "verify", false, "Verify ETag/checksum after download")
	cloneCmd.Flags().BoolVar(&config.PreserveMetadata, "preserve-metadata", false, "Preserve object metadata")
	cloneCmd.Flags().BoolVar(&config.DryRun, "dry-run", false, "List what would be cloned without cloning")

	cloneCmd.MarkFlagRequired("cloudfront-domain")
	cloneCmd.MarkFlagRequired("private-key")
	cloneCmd.MarkFlagRequired("key-pair-id")
}

func runClone(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	// Validate configuration
	if err := validateConfig(); err != nil {
		return err
	}

	// Print configuration in verbose mode
	if Verbose() {
		printVerboseConfig()
	}

	// Get object list
	if Verbose() {
		fmt.Println("[verbose] Fetching object list...")
	}
	objects, err := getObjects(ctx)
	if err != nil {
		return fmt.Errorf("failed to get object list: %w", err)
	}

	if len(objects) == 0 {
		fmt.Println("No objects to clone")
		return nil
	}

	fmt.Printf("Found %d objects to clone\n", len(objects))

	// Calculate total size in verbose mode
	if Verbose() {
		var totalSize int64
		for _, obj := range objects {
			totalSize += obj.Size
		}
		fmt.Printf("[verbose] Total size: %s\n", formatBytes(totalSize))
	}

	// Dry run - just list objects
	if config.DryRun {
		fmt.Println("\nDry run - objects that would be cloned:")
		for _, obj := range objects {
			fmt.Printf("  %s (%d bytes)\n", obj.Key, obj.Size)
		}
		return nil
	}

	// Fetch metadata if needed
	if config.PreserveMetadata && config.SourceBucket != "" {
		fmt.Println("Fetching object metadata...")
		if err := fetchMetadata(ctx, objects); err != nil {
			return fmt.Errorf("failed to fetch metadata: %w", err)
		}
	}

	// Create CloudFront signer
	if Verbose() {
		fmt.Printf("[verbose] Loading private key from: %s\n", config.PrivateKeyPath)
	}
	signer, err := cloudfront.NewSigner(config.PrivateKeyPath, config.KeyPairID)
	if err != nil {
		return fmt.Errorf("failed to create CloudFront signer: %w", err)
	}

	// Generate signed cookies
	resourcePattern := fmt.Sprintf("https://%s/*", config.CloudFrontDomain)
	expiry := time.Now().Add(config.CookieExpiry)
	cookies, err := signer.GenerateSignedCookies(resourcePattern, expiry)
	if err != nil {
		return fmt.Errorf("failed to generate signed cookies: %w", err)
	}

	if Verbose() {
		fmt.Printf("[verbose] Generated signed cookies for: %s\n", resourcePattern)
		fmt.Printf("[verbose] Cookie expiry: %s\n", expiry.Format(time.RFC3339))
	}

	// Create downloader
	dl := downloader.NewDownloader(config.CloudFrontDomain, cookies)

	// Create uploader
	var up uploader.Uploader
	if config.DestLocal != "" {
		if Verbose() {
			fmt.Printf("[verbose] Destination: local filesystem (%s)\n", config.DestLocal)
		}
		up = uploader.NewLocalUploader(config.DestLocal)
	} else {
		if Verbose() {
			fmt.Printf("[verbose] Destination: S3 bucket (%s/%s)\n", config.DestBucket, config.DestPrefix)
		}
		s3Up, err := uploader.NewS3Uploader(ctx, config.DestBucket, config.DestPrefix)
		if err != nil {
			return fmt.Errorf("failed to create S3 uploader: %w", err)
		}
		up = s3Up
	}

	// Create verifier if needed
	var v *verifier.Verifier
	if config.Verify {
		v = verifier.NewVerifier()
		if Verbose() {
			fmt.Println("[verbose] ETag verification enabled")
		}
	}

	if Verbose() {
		fmt.Printf("[verbose] Starting clone with %d workers\n", config.Concurrency)
	}

	// Process objects with worker pool
	stats := processObjects(ctx, objects, dl, up, v)

	// Print summary
	printSummary(stats)

	if stats.FailedCount > 0 {
		return fmt.Errorf("%d objects failed to clone", stats.FailedCount)
	}

	return nil
}

func printVerboseConfig() {
	fmt.Println("[verbose] Configuration:")
	if config.SourceBucket != "" {
		fmt.Printf("[verbose]   Source bucket: %s\n", config.SourceBucket)
	}
	if config.SourceFile != "" {
		fmt.Printf("[verbose]   Source file: %s\n", config.SourceFile)
	}
	if config.Prefix != "" {
		fmt.Printf("[verbose]   Prefix: %s\n", config.Prefix)
	}
	fmt.Printf("[verbose]   CloudFront domain: %s\n", config.CloudFrontDomain)
	fmt.Printf("[verbose]   Key pair ID: %s\n", config.KeyPairID)
	fmt.Printf("[verbose]   Concurrency: %d\n", config.Concurrency)
	fmt.Printf("[verbose]   Verify: %t\n", config.Verify)
	fmt.Printf("[verbose]   Preserve metadata: %t\n", config.PreserveMetadata)
}

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func validateConfig() error {
	if config.SourceBucket == "" && config.SourceFile == "" {
		return fmt.Errorf("either --source-bucket or --source-file is required")
	}

	if config.SourceFile != "" && config.SourceBucket == "" && config.PreserveMetadata {
		return fmt.Errorf("--source-bucket is required when using --source-file with --preserve-metadata")
	}

	if config.DestLocal == "" && config.DestBucket == "" {
		return fmt.Errorf("either --dest-local or --dest-bucket is required")
	}

	if config.DestLocal != "" && config.DestBucket != "" {
		return fmt.Errorf("only one of --dest-local or --dest-bucket can be specified")
	}

	return nil
}

func getObjects(ctx context.Context) ([]types.ObjectInfo, error) {
	if config.SourceFile != "" {
		return lister.ListFromFile(config.SourceFile)
	}

	s3Lister, err := lister.NewS3Lister(ctx)
	if err != nil {
		return nil, err
	}

	return s3Lister.ListObjects(ctx, config.SourceBucket, config.Prefix)
}

func fetchMetadata(ctx context.Context, objects []types.ObjectInfo) error {
	s3Lister, err := lister.NewS3Lister(ctx)
	if err != nil {
		return err
	}

	for i := range objects {
		if err := s3Lister.FetchMetadata(ctx, config.SourceBucket, &objects[i]); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to fetch metadata for %s: %v\n", objects[i].Key, err)
		}
	}

	return nil
}

func processObjects(ctx context.Context, objects []types.ObjectInfo, dl *downloader.Downloader, up uploader.Uploader, v *verifier.Verifier) types.CloneStats {
	var stats types.CloneStats
	stats.TotalObjects = len(objects)

	jobs := make(chan types.ObjectInfo, len(objects))
	results := make(chan types.DownloadResult, len(objects))

	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < config.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range jobs {
				result := processObject(ctx, obj, dl, up, v)
				results <- result
			}
		}()
	}

	// Send jobs
	go func() {
		for _, obj := range objects {
			jobs <- obj
		}
		close(jobs)
	}()

	// Wait for workers and close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var completed int32
	for result := range results {
		atomic.AddInt32(&completed, 1)

		if result.Success {
			stats.SuccessCount++
			stats.TransferredBytes += result.BytesRead
		} else {
			stats.FailedCount++
			fmt.Fprintf(os.Stderr, "Failed: %s - %v\n", result.Object.Key, result.Error)
		}

		if result.Verified {
			stats.VerifiedCount++
		}
		if result.VerifyFail {
			stats.VerifyFailCount++
		}

		// Progress update
		progress := float64(completed) / float64(len(objects)) * 100
		fmt.Printf("\rProgress: %.1f%% (%d/%d)", progress, completed, len(objects))
	}
	fmt.Println()

	return stats
}

func processObject(ctx context.Context, obj types.ObjectInfo, dl *downloader.Downloader, up uploader.Uploader, v *verifier.Verifier) types.DownloadResult {
	result := types.DownloadResult{Object: obj}

	if Verbose() {
		fmt.Printf("[verbose] Downloading: %s (%s)\n", obj.Key, formatBytes(obj.Size))
	}

	// Download from CloudFront
	data, md5Hash, err := dl.Download(ctx, obj.Key)
	if err != nil {
		result.Error = fmt.Errorf("download failed: %w", err)
		return result
	}

	result.BytesRead = int64(len(data))
	result.MD5Hash = md5Hash

	if Verbose() {
		fmt.Printf("[verbose] Downloaded: %s (MD5: %s)\n", obj.Key, md5Hash)
	}

	// Verify if enabled
	if v != nil {
		verified, err := v.Verify(obj, md5Hash)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: verification error for %s: %v\n", obj.Key, err)
		}
		result.Verified = verified
		result.VerifyFail = !verified && err == nil

		if Verbose() {
			if verified {
				fmt.Printf("[verbose] Verified: %s (ETag match)\n", obj.Key)
			} else if err != nil {
				fmt.Printf("[verbose] Verify skipped: %s (%v)\n", obj.Key, err)
			} else {
				fmt.Printf("[verbose] Verify FAILED: %s (expected: %s, got: %s)\n", obj.Key, obj.ETag, md5Hash)
			}
		}
	}

	if Verbose() {
		fmt.Printf("[verbose] Uploading: %s\n", obj.Key)
	}

	// Upload to destination
	if err := up.Upload(ctx, obj, data); err != nil {
		result.Error = fmt.Errorf("upload failed: %w", err)
		return result
	}

	if Verbose() {
		fmt.Printf("[verbose] Completed: %s\n", obj.Key)
	}

	result.Success = true
	return result
}

func printSummary(stats types.CloneStats) {
	fmt.Println("\n=== Clone Summary ===")
	fmt.Printf("Total objects:    %d\n", stats.TotalObjects)
	fmt.Printf("Successful:       %d\n", stats.SuccessCount)
	fmt.Printf("Failed:           %d\n", stats.FailedCount)
	fmt.Printf("Bytes transferred: %d\n", stats.TransferredBytes)

	if config.Verify {
		fmt.Printf("Verified:         %d\n", stats.VerifiedCount)
		fmt.Printf("Verify failed:    %d\n", stats.VerifyFailCount)
	}
}
