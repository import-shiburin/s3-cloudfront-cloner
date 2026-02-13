package lister

import (
	"encoding/json"
	"fmt"
	"os"

	"s3-cloudfront-cloner/pkg/types"
)

// ListFromFile reads object information from a JSON file in AWS CLI format
func ListFromFile(filePath string) ([]types.ObjectInfo, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var output types.ListObjectsOutput
	if err := json.Unmarshal(data, &output); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return output.Contents, nil
}
