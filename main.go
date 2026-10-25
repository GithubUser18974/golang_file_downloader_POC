package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DownloadItem represents a URL and its target filename.
type DownloadItem struct {
	URL      string
	Filename string
}

// DownloadResult stores the outcome of a single file download.
type DownloadResult struct {
	Filename string
	Bytes    int64
	Duration time.Duration
	Err      error
}

// safeFilename generates a clean and unique filename from a URL and index.
func safeFilename(rawURL string, index int) string {
	parsed, err := url.Parse(rawURL)
	if err == nil {
		base := filepath.Base(parsed.Path)
		if base != "" && base != "." && base != "/" {
			return fmt.Sprintf("file_%d_%s", index+1, base)
		}
	}
	return fmt.Sprintf("file_%d.jpg", index+1)
}

// DownloadFile downloads a single file from url and saves it to filePath.
func DownloadFile(ctx context.Context, fileURL string, filePath string) (int64, time.Duration, error) {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fileURL, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, 0, fmt.Errorf("server returned HTTP %d (%s)", resp.StatusCode, resp.Status)
	}

	out, err := os.Create(filePath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create destination file %s: %w", filePath, err)
	}
	defer out.Close()

	written, err := io.Copy(out, resp.Body)
	if err != nil {
		return written, time.Since(start), fmt.Errorf("failed to stream file content: %w", err)
	}

	return written, time.Since(start), nil
}

// sequenceDownload downloads files one by one (sequential).
func sequenceDownload(items []DownloadItem, destDir string) error {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	fmt.Printf("\n=== 1. Sequential Download (%d files) ===\n", len(items))
	start := time.Now()

	for i, item := range items {
		targetPath := filepath.Join(destDir, item.Filename)
		fmt.Printf("[%d/%d] Downloading %s ...\n", i+1, len(items), item.Filename)

		bytes, dur, err := DownloadFile(context.Background(), item.URL, targetPath)
		if err != nil {
			fmt.Printf("   [ERROR] %s: %v\n", item.Filename, err)
			continue
		}
		fmt.Printf("   [DONE]  %s (%.1f KB) in %v\n", item.Filename, float64(bytes)/1024, dur.Round(time.Millisecond))
	}

	fmt.Printf("Total Sequential Time: %v\n", time.Since(start).Round(time.Millisecond))
	return nil
}

// concurrentDownload downloads all files simultaneously using Goroutines and sync.WaitGroup.
func concurrentDownload(items []DownloadItem, destDir string) error {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	fmt.Printf("\n=== 2. Concurrent Download (%d files with Goroutines + sync.WaitGroup) ===\n", len(items))
	start := time.Now()

	var wg sync.WaitGroup
	results := make(chan DownloadResult, len(items))

	// Launch a goroutine for each file
	for _, item := range items {
		wg.Add(1)
		go func(it DownloadItem) {
			defer wg.Done()
			targetPath := filepath.Join(destDir, it.Filename)
			fmt.Printf("   [START] %s\n", it.Filename)

			bytes, dur, err := DownloadFile(context.Background(), it.URL, targetPath)
			results <- DownloadResult{
				Filename: it.Filename,
				Bytes:    bytes,
				Duration: dur,
				Err:      err,
			}
		}(item)
	}

	// Close results channel once all goroutines finish
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and display results as they arrive
	var totalBytes int64
	successCount := 0
	for res := range results {
		if res.Err != nil {
			fmt.Printf("   [FAILED] %s: %v\n", res.Filename, res.Err)
		} else {
			successCount++
			totalBytes += res.Bytes
			fmt.Printf("   [DONE]   %s (%.1f KB) in %v\n", res.Filename, float64(res.Bytes)/1024, res.Duration.Round(time.Millisecond))
		}
	}

	fmt.Printf("Total Concurrent Time: %v (%d/%d downloaded, %.1f KB total)\n",
		time.Since(start).Round(time.Millisecond), successCount, len(items), float64(totalBytes)/1024)
	return nil
}

// concurrentDownloadWithPool downloads files using a worker pool with bounded concurrency.
func concurrentDownloadWithPool(items []DownloadItem, destDir string, maxWorkers int) error {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	fmt.Printf("\n=== 3. Worker Pool Download (%d files, max %d workers) ===\n", len(items), maxWorkers)
	start := time.Now()

	jobs := make(chan DownloadItem, len(items))
	results := make(chan DownloadResult, len(items))
	var wg sync.WaitGroup

	// Start worker goroutines
	for w := 1; w <= maxWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for item := range jobs {
				targetPath := filepath.Join(destDir, item.Filename)
				fmt.Printf("   [Worker %d] Started %s\n", workerID, item.Filename)

				bytes, dur, err := DownloadFile(context.Background(), item.URL, targetPath)
				results <- DownloadResult{
					Filename: item.Filename,
					Bytes:    bytes,
					Duration: dur,
					Err:      err,
				}
			}
		}(w)
	}

	// Send jobs to the worker queue
	for _, item := range items {
		jobs <- item
	}
	close(jobs)

	// Close results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var totalBytes int64
	successCount := 0
	for res := range results {
		if res.Err != nil {
			fmt.Printf("   [FAILED] %s: %v\n", res.Filename, res.Err)
		} else {
			successCount++
			totalBytes += res.Bytes
			fmt.Printf("   [DONE]   %s (%.1f KB) in %v\n", res.Filename, float64(res.Bytes)/1024, res.Duration.Round(time.Millisecond))
		}
	}

	fmt.Printf("Total Worker Pool Time: %v (%d/%d downloaded, %.1f KB total)\n",
		time.Since(start).Round(time.Millisecond), successCount, len(items), float64(totalBytes)/1024)
	return nil
}

func main() {
	// Sample static URLs for Proof of Concept
	urls := []string{
		"https://fastly.picsum.photos/id/237/536/354.jpg?hmac=i0yVXW1ORpyCZpQ-CknuyV-jbtU7_x9EBQVhvT5aRr0",
		"https://picsum.photos/id/10/600/400.jpg",
		"https://picsum.photos/id/20/600/400.jpg",
		"https://picsum.photos/id/28/600/400.jpg",
		"https://picsum.photos/id/40/600/400.jpg",
	}

	// Map URLs to DownloadItems with safe, distinguishable filenames
	items := make([]DownloadItem, len(urls))
	for i, u := range urls {
		items[i] = DownloadItem{
			URL:      u,
			Filename: safeFilename(u, i),
		}
	}

	destDir := "./files"

	fmt.Println("==================================================")
	fmt.Println(" GoLang Multiple File Downloader - Proof of Concept")
	fmt.Println("==================================================")
	fmt.Printf("Target Directory: %s\n", destDir)
	fmt.Printf("Files to download: %d\n", len(items))

	// Run Concurrent Download (Goroutines + sync.WaitGroup)
	if err := concurrentDownload(items, destDir); err != nil {
		fmt.Printf("Concurrent download error: %v\n", err)
	}

	// Also demonstrate Worker Pool Download with bounded concurrency (2 workers)
	// You can also compare with sequenceDownload(items, destDir)
}
