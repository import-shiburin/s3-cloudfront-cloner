package downloader

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"

	"s3-cloudfront-cloner/internal/cloudfront"
)

// Downloader handles downloading objects from CloudFront
type Downloader struct {
	cloudFrontDomain string
	cookies          *cloudfront.SignedCookies
	client           *http.Client
	maxRetries       int
	retryDelay       time.Duration
}

// NewDownloader creates a new CloudFront downloader
func NewDownloader(cloudFrontDomain string, cookies *cloudfront.SignedCookies) *Downloader {
	return &Downloader{
		cloudFrontDomain: cloudFrontDomain,
		cookies:          cookies,
		client: &http.Client{
			Timeout: 5 * time.Minute,
		},
		maxRetries: 3,
		retryDelay: time.Second,
	}
}

// Download fetches an object from CloudFront and returns its content and MD5 hash
func (d *Downloader) Download(ctx context.Context, key string) ([]byte, string, error) {
	url := fmt.Sprintf("https://%s/%s", d.cloudFrontDomain, key)

	var lastErr error
	for attempt := 0; attempt <= d.maxRetries; attempt++ {
		if attempt > 0 {
			delay := d.retryDelay * time.Duration(1<<(attempt-1)) // Exponential backoff
			select {
			case <-ctx.Done():
				return nil, "", ctx.Err()
			case <-time.After(delay):
			}
		}

		data, hash, err := d.doDownload(ctx, url)
		if err == nil {
			return data, hash, nil
		}
		lastErr = err
	}

	return nil, "", fmt.Errorf("download failed after %d retries: %w", d.maxRetries, lastErr)
}

func (d *Downloader) doDownload(ctx context.Context, url string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create request: %w", err)
	}

	// Add signed cookies
	req.AddCookie(&http.Cookie{
		Name:  "CloudFront-Policy",
		Value: d.cookies.Policy,
	})
	req.AddCookie(&http.Cookie{
		Name:  "CloudFront-Signature",
		Value: d.cookies.Signature,
	})
	req.AddCookie(&http.Cookie{
		Name:  "CloudFront-Key-Pair-Id",
		Value: d.cookies.KeyPairID,
	})

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Read body while calculating MD5
	hasher := md5.New()
	reader := io.TeeReader(resp.Body, hasher)

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read response body: %w", err)
	}

	md5Hash := hex.EncodeToString(hasher.Sum(nil))

	return data, md5Hash, nil
}

// StreamDownload downloads an object and streams it to a writer while calculating MD5
func (d *Downloader) StreamDownload(ctx context.Context, key string, w io.Writer) (int64, string, error) {
	url := fmt.Sprintf("https://%s/%s", d.cloudFrontDomain, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "", fmt.Errorf("failed to create request: %w", err)
	}

	// Add signed cookies
	req.AddCookie(&http.Cookie{
		Name:  "CloudFront-Policy",
		Value: d.cookies.Policy,
	})
	req.AddCookie(&http.Cookie{
		Name:  "CloudFront-Signature",
		Value: d.cookies.Signature,
	})
	req.AddCookie(&http.Cookie{
		Name:  "CloudFront-Key-Pair-Id",
		Value: d.cookies.KeyPairID,
	})

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, "", fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Stream to writer while calculating MD5
	hasher := md5.New()
	multiWriter := io.MultiWriter(w, hasher)

	n, err := io.Copy(multiWriter, resp.Body)
	if err != nil {
		return n, "", fmt.Errorf("failed to stream response: %w", err)
	}

	md5Hash := hex.EncodeToString(hasher.Sum(nil))

	return n, md5Hash, nil
}
