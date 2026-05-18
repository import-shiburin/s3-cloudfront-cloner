package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"s3-cloudfront-cloner/internal/checksum"
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
	cloneCmd.Flags().StringVar(&config.SourceRegion, "source-region", "", "Override source bucket region (auto-detected if unset)")
	cloneCmd.Flags().StringVar(&config.Prefix, "prefix", "", "Prefix to filter objects")
	cloneCmd.Flags().StringVar(&config.StripPrefix, "strip-prefix", "", "Leading prefix to strip from each source key before joining with --dest-prefix (e.g. --strip-prefix bbb/ccc/ turns bbb/ccc/ddd/file into ddd/file)")

	// CloudFront flags
	cloneCmd.Flags().StringVar(&config.CloudFrontDomain, "cloudfront-domain", "", "CloudFront distribution domain (required)")
	cloneCmd.Flags().StringVar(&config.PrivateKeyPath, "private-key", "", "Path to CloudFront private key PEM file (required)")
	cloneCmd.Flags().StringVar(&config.KeyPairID, "key-pair-id", "", "CloudFront key pair ID (required)")
	cloneCmd.Flags().DurationVar(&config.CookieExpiry, "cookie-expiry", 24*time.Hour, "Cookie expiration duration")

	// Destination flags
	cloneCmd.Flags().StringVar(&config.DestLocal, "dest-local", "", "Local directory for downloads")
	cloneCmd.Flags().StringVar(&config.DestBucket, "dest-bucket", "", "Destination S3 bucket")
	cloneCmd.Flags().StringVar(&config.DestPrefix, "dest-prefix", "", "Prefix for destination objects")
	cloneCmd.Flags().StringVar(&config.DestRegion, "dest-region", "", "Override destination bucket region (auto-detected if unset)")

	// Operation flags
	cloneCmd.Flags().IntVar(&config.Concurrency, "concurrency", 10, "Number of parallel downloads")
	cloneCmd.Flags().BoolVar(&config.Verify, "verify", false, "Verify ETag/checksum after download")
	cloneCmd.Flags().BoolVar(&config.PreserveMetadata, "preserve-metadata", false, "Preserve object metadata")
	cloneCmd.Flags().BoolVar(&config.DryRun, "dry-run", false, "List what would be cloned without cloning")
	cloneCmd.Flags().BoolVar(&config.SkipExisting, "skip-existing", false, "Skip objects whose destination already matches source checksum")
	cloneCmd.Flags().Int64Var(&config.RangeThreshold, "range-threshold", 5*1024*1024*1024, "File size threshold (bytes) for chunked Range downloads")
	cloneCmd.Flags().Int64Var(&config.ChunkSize, "chunk-size", 256*1024*1024, "Chunk size (bytes) for Range downloads")

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

	// Resolve bucket regions up front so every subsequent S3 client uses the
	// right endpoint. Skipped for buckets the user didn't provide, and for
	// regions that were explicitly overridden via flags.
	if err := resolveRegions(ctx); err != nil {
		return err
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

	// Fetch checksums (and per-part sizes for multipart sources) when verify or
	// skip-existing is enabled — both need source-side checksums to compare against.
	if (config.Verify || config.SkipExisting) && config.SourceBucket != "" {
		fmt.Println("Fetching checksums for verification...")
		if err := fetchVerificationData(ctx, objects); err != nil {
			return fmt.Errorf("failed to fetch verification data: %w", err)
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
	dl := downloader.NewDownloader(config.CloudFrontDomain, cookies, config.RangeThreshold, config.ChunkSize)

	// Create uploader
	var up uploader.Uploader
	if config.DestLocal != "" {
		if Verbose() {
			fmt.Printf("[verbose] Destination: local filesystem (%s)\n", config.DestLocal)
		}
		up = uploader.NewLocalUploader(config.DestLocal, config.StripPrefix)
	} else {
		if Verbose() {
			fmt.Printf("[verbose] Destination: S3 bucket (%s/%s)\n", config.DestBucket, config.DestPrefix)
		}
		s3Up, err := uploader.NewS3Uploader(ctx, config.DestBucket, config.DestPrefix, config.DestRegion, config.StripPrefix)
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
	if config.StripPrefix != "" {
		fmt.Printf("[verbose]   Strip prefix: %s\n", config.StripPrefix)
	}
	fmt.Printf("[verbose]   CloudFront domain: %s\n", config.CloudFrontDomain)
	fmt.Printf("[verbose]   Key pair ID: %s\n", config.KeyPairID)
	fmt.Printf("[verbose]   Concurrency: %d\n", config.Concurrency)
	fmt.Printf("[verbose]   Verify: %t\n", config.Verify)
	fmt.Printf("[verbose]   Preserve metadata: %t\n", config.PreserveMetadata)
	fmt.Printf("[verbose]   Skip existing: %t\n", config.SkipExisting)
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

	if config.Verify && config.SourceBucket == "" {
		return fmt.Errorf("--source-bucket is required when --verify is enabled (needed to fetch x-amz-checksum-sha256 and per-part sizes)")
	}

	if config.SkipExisting && config.SourceBucket == "" {
		return fmt.Errorf("--source-bucket is required when --skip-existing is enabled (needed to fetch checksums for identity comparison)")
	}

	if config.DestLocal == "" && config.DestBucket == "" {
		return fmt.Errorf("either --dest-local or --dest-bucket is required")
	}

	if config.DestLocal != "" && config.DestBucket != "" {
		return fmt.Errorf("only one of --dest-local or --dest-bucket can be specified")
	}

	if config.ChunkSize <= 0 {
		return fmt.Errorf("--chunk-size must be positive, got %d", config.ChunkSize)
	}

	if config.DestBucket != "" {
		if config.ChunkSize < uploader.S3MinPartSize {
			return fmt.Errorf("--chunk-size must be at least %d bytes (5 MiB) for S3 destinations, got %d", uploader.S3MinPartSize, config.ChunkSize)
		}
		if config.ChunkSize > uploader.S3MaxPartSize {
			return fmt.Errorf("--chunk-size must be at most %d bytes (5 GiB) for S3 destinations, got %d", uploader.S3MaxPartSize, config.ChunkSize)
		}
	}

	return nil
}

func getObjects(ctx context.Context) ([]types.ObjectInfo, error) {
	if config.SourceFile != "" {
		return lister.ListFromFile(config.SourceFile)
	}

	s3Lister, err := lister.NewS3Lister(ctx, config.SourceRegion)
	if err != nil {
		return nil, err
	}

	return s3Lister.ListObjects(ctx, config.SourceBucket, config.Prefix)
}

// resolveRegions auto-detects the source and destination bucket regions when
// the user didn't explicitly set them via --source-region / --dest-region.
func resolveRegions(ctx context.Context) error {
	if config.SourceBucket != "" && config.SourceRegion == "" {
		region, err := lister.DetectBucketRegion(ctx, config.SourceBucket)
		if err != nil {
			return fmt.Errorf("detect source-bucket region: %w", err)
		}
		config.SourceRegion = region
		if Verbose() {
			fmt.Printf("[verbose] Source bucket region: %s\n", region)
		}
	}
	if config.DestBucket != "" && config.DestRegion == "" {
		region, err := lister.DetectBucketRegion(ctx, config.DestBucket)
		if err != nil {
			return fmt.Errorf("detect dest-bucket region: %w", err)
		}
		config.DestRegion = region
		if Verbose() {
			fmt.Printf("[verbose] Dest bucket region: %s\n", region)
		}
	}
	return nil
}

func fetchMetadata(ctx context.Context, objects []types.ObjectInfo) error {
	s3Lister, err := lister.NewS3Lister(ctx, config.SourceRegion)
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

// fetchVerificationData populates the source object's checksum-related fields
// so the verifier has something to compare against. Called only when --verify
// is enabled with a source bucket. PartSizes is fetched only when no
// FullObject-style checksum (any S3 algorithm with no "-N" suffix, or rclone's
// x-amz-meta-md5chksum) is already present — without those the verifier needs
// per-part boundaries for composite-checksum or composite-ETag fallback.
//
// Failures are aggregated and surfaced upfront so users see the verifiability
// of their planned clone *before* downloading 200 GB.
func fetchVerificationData(ctx context.Context, objects []types.ObjectInfo) error {
	s3Lister, err := lister.NewS3Lister(ctx, config.SourceRegion)
	if err != nil {
		return err
	}

	var (
		withFullObj   int
		withRclone    int
		withPartSizes int
		needPartSizes int
		checksumFails int
		partSizeFails int
		firstCkErr    error
		firstPartErr  error
	)

	for i := range objects {
		if !hasAnyChecksum(&objects[i]) {
			if err := s3Lister.FetchChecksum(ctx, config.SourceBucket, &objects[i]); err != nil {
				checksumFails++
				if firstCkErr == nil {
					firstCkErr = err
				}
			}
		}

		if hasFullObjectChecksum(&objects[i]) {
			withFullObj++
		}
		if objects[i].RcloneMD5Base64 != "" {
			withRclone++
		}

		// Need PartSizes only if no full-object checksum or rclone MD5 covers
		// verification AND the source is multipart.
		isMultipart := strings.Contains(strings.Trim(objects[i].ETag, "\""), "-")
		if !isMultipart {
			continue
		}
		if hasFullObjectChecksum(&objects[i]) || objects[i].RcloneMD5Base64 != "" {
			continue
		}
		needPartSizes++
		if err := s3Lister.FetchPartSizes(ctx, config.SourceBucket, &objects[i]); err != nil {
			partSizeFails++
			if firstPartErr == nil {
				firstPartErr = fmt.Errorf("%s: %w", objects[i].Key, err)
			}
		} else {
			withPartSizes++
		}
	}

	fmt.Printf("Verification coverage: %d full-object checksums, %d rclone md5chksum, %d/%d composite-only multipart need part sizes (%d fetched)\n",
		withFullObj, withRclone, withPartSizes, needPartSizes, withPartSizes)
	if checksumFails > 0 {
		fmt.Fprintf(os.Stderr, "Warning: %d HeadObject failures (first: %v)\n", checksumFails, firstCkErr)
	}
	if partSizeFails > 0 {
		fmt.Fprintf(os.Stderr, "Warning: %d GetObjectAttributes failures (first: %v)\n", partSizeFails, firstPartErr)
		fmt.Fprintln(os.Stderr, "         Those objects will fall back to ETag with no PartSizes — verification will fail.")
	}

	return nil
}

func hasAnyChecksum(o *types.ObjectInfo) bool {
	return o.ChecksumCRC32 != "" || o.ChecksumCRC32C != "" || o.ChecksumCRC64NVME != "" ||
		o.ChecksumSHA1 != "" || o.ChecksumSHA256 != "" || o.RcloneMD5Base64 != ""
}

func hasFullObjectChecksum(o *types.ObjectInfo) bool {
	for _, c := range []string{o.ChecksumCRC32, o.ChecksumCRC32C, o.ChecksumCRC64NVME, o.ChecksumSHA1, o.ChecksumSHA256} {
		if c != "" && !strings.Contains(c, "-") {
			return true
		}
	}
	return false
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

		switch {
		case result.Skipped:
			stats.SkippedCount++
		case result.Success:
			stats.SuccessCount++
			stats.TransferredBytes += result.BytesRead
		default:
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

	if config.SkipExisting {
		identical, err := up.IsIdentical(ctx, obj)
		if err != nil {
			// Non-fatal: log and proceed with a normal clone.
			fmt.Fprintf(os.Stderr, "Warning: skip-existing check failed for %s: %v\n", obj.Key, err)
		} else if identical {
			if Verbose() {
				fmt.Printf("[verbose] Skipping %s — destination already matches source checksum\n", obj.Key)
			}
			result.Skipped = true
			result.Success = true
			return result
		}
	}

	if Verbose() {
		fmt.Printf("[verbose] Streaming: %s (%s)\n", obj.Key, formatBytes(obj.Size))
	}

	const maxRetries = 3
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Second * time.Duration(1<<(attempt-1))
			if Verbose() {
				fmt.Printf("[verbose] Retrying %s (attempt %d/%d)\n", obj.Key, attempt+1, maxRetries+1)
			}
			select {
			case <-ctx.Done():
				result.Error = ctx.Err()
				return result
			case <-time.After(delay):
			}
		}

		algos := verifier.AlgosNeeded(obj)
		n, sums, err := streamObject(ctx, obj, dl, up, algos)
		if err != nil {
			lastErr = err
			if Verbose() {
				fmt.Printf("[verbose] Failed: %s - %v\n", obj.Key, err)
			}
			continue
		}

		result.BytesRead = n
		if r, ok := sums[checksum.AlgSHA256]; ok && r.FullObject != nil {
			result.ChecksumSHA256 = base64.StdEncoding.EncodeToString(r.FullObject)
		}

		if Verbose() {
			fmt.Printf("[verbose] Streamed: %s\n", obj.Key)
		}

		if v != nil {
			vr := v.Verify(obj, sums)
			result.Verified = vr.Verified
			result.VerifyFail = !vr.Verified
			if !vr.Verified {
				fmt.Fprintf(os.Stderr, "Verify FAILED: %s — %s\n", obj.Key, vr.Reason)
			}
			if Verbose() {
				if vr.Verified {
					fmt.Printf("[verbose] Verified: %s (via %s)\n", obj.Key, vr.Method)
				}
			}
		}

		result.Success = true
		return result
	}

	result.Error = fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
	return result
}

// streamObject pipes data from CloudFront directly to the uploader without
// buffering the entire file in memory. The requested digest algorithms are
// computed as data flows through and returned in a Sums map.
//
// For local destinations with large files, the file is preallocated and chunks
// are written in parallel via WriteAt, bypassing the pipe. For S3 destinations
// with large files, a parallel multipart upload combines parallel Range
// downloads with parallel UploadPart calls — each chunk maps to one S3 part.
func streamObject(ctx context.Context, obj types.ObjectInfo, dl *downloader.Downloader, up uploader.Uploader, algos []checksum.Algorithm) (int64, map[checksum.Algorithm]checksum.Result, error) {
	isLarge := obj.Size > 0 && obj.Size > config.RangeThreshold

	// Fast path: local uploader + ranged download → preallocate and write directly.
	if localUp, ok := up.(*uploader.LocalUploader); ok && isLarge {
		f, err := localUp.CreateFile(obj, obj.Size)
		if err != nil {
			return 0, nil, fmt.Errorf("create file: %w", err)
		}

		n, sums, dlErr := dl.StreamDownload(ctx, obj.Key, obj.Size, algos, obj.PartSizes, f)
		finishErr := localUp.FinishFile(f, obj)

		if dlErr != nil || finishErr != nil {
			os.Remove(f.Name())
			if dlErr != nil {
				return 0, nil, fmt.Errorf("download: %w", dlErr)
			}
			return 0, nil, fmt.Errorf("finish: %w", finishErr)
		}

		return n, sums, nil
	}

	// Fast path: S3 uploader + ranged download → parallel multipart upload.
	// Skipped when the file fits in a single chunk; the pipe path is simpler there.
	if s3Up, ok := up.(*uploader.S3Uploader); ok && isLarge {
		numChunks := (obj.Size + config.ChunkSize - 1) / config.ChunkSize
		if numChunks > 1 {
			downloadFn := func(ctx context.Context, start, end int64) ([]byte, error) {
				return dl.DownloadChunk(ctx, obj.Key, start, end)
			}
			return s3Up.UploadMultipart(ctx, obj, obj.Size, config.ChunkSize, algos, downloadFn)
		}
	}

	// Standard path: pipe download into uploader (S3 or small local files).
	pr, pw := io.Pipe()

	type dlResult struct {
		n    int64
		sums map[checksum.Algorithm]checksum.Result
		err  error
	}
	ch := make(chan dlResult, 1)

	go func() {
		n, sums, err := dl.StreamDownload(ctx, obj.Key, obj.Size, algos, obj.PartSizes, pw)
		pw.CloseWithError(err) // nil means normal EOF
		ch <- dlResult{n, sums, err}
	}()

	uploadErr := up.Upload(ctx, obj, pr)
	pr.CloseWithError(uploadErr) // unblock writer if upload failed

	res := <-ch

	if res.err != nil {
		return 0, nil, fmt.Errorf("download: %w", res.err)
	}
	if uploadErr != nil {
		return 0, nil, fmt.Errorf("upload: %w", uploadErr)
	}

	return res.n, res.sums, nil
}

func printSummary(stats types.CloneStats) {
	fmt.Println("\n=== Clone Summary ===")
	fmt.Printf("Total objects:    %d\n", stats.TotalObjects)
	fmt.Printf("Successful:       %d\n", stats.SuccessCount)
	if config.SkipExisting {
		fmt.Printf("Skipped:          %d\n", stats.SkippedCount)
	}
	fmt.Printf("Failed:           %d\n", stats.FailedCount)
	fmt.Printf("Bytes transferred: %d\n", stats.TransferredBytes)

	if config.Verify {
		fmt.Printf("Verified:         %d\n", stats.VerifiedCount)
		fmt.Printf("Verify failed:    %d\n", stats.VerifyFailCount)
	}
}
