package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"s3-cloudfront-cloner/internal/lister"
	"s3-cloudfront-cloner/pkg/types"

	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List objects from an S3 bucket",
	Long:  `List all objects from an S3 bucket with optional prefix filtering.`,
	RunE:  runList,
}

var (
	listBucket string
	listPrefix string
	listOutput string
)

func init() {
	rootCmd.AddCommand(listCmd)

	listCmd.Flags().StringVar(&listBucket, "bucket", "", "S3 bucket name (required)")
	listCmd.Flags().StringVar(&listPrefix, "prefix", "", "Prefix to filter objects")
	listCmd.Flags().StringVarP(&listOutput, "output", "o", "", "Output file (default: stdout)")

	listCmd.MarkFlagRequired("bucket")
}

func runList(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	if Verbose() {
		fmt.Fprintf(os.Stderr, "[verbose] Listing objects from bucket: %s\n", listBucket)
		if listPrefix != "" {
			fmt.Fprintf(os.Stderr, "[verbose] Using prefix: %s\n", listPrefix)
		}
	}

	s3Lister, err := lister.NewS3Lister(ctx)
	if err != nil {
		return fmt.Errorf("failed to create S3 lister: %w", err)
	}

	objects, err := s3Lister.ListObjects(ctx, listBucket, listPrefix)
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	if Verbose() {
		var totalSize int64
		for _, obj := range objects {
			totalSize += obj.Size
		}
		fmt.Fprintf(os.Stderr, "[verbose] Found %d objects, total size: %d bytes\n", len(objects), totalSize)
	}

	output := types.ListObjectsOutput{
		Contents: objects,
	}

	jsonData, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	if listOutput != "" {
		if err := os.WriteFile(listOutput, jsonData, 0644); err != nil {
			return fmt.Errorf("failed to write output file: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Listed %d objects to %s\n", len(objects), listOutput)
	} else {
		fmt.Println(string(jsonData))
	}

	return nil
}
