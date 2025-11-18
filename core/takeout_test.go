package core

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGetTakeoutTimestamp(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		content   string
		wantTS    int64
		wantErr   bool
		setupFile bool
	}{
		{
			name: "valid metadata",
			content: `{
				"title": "IMG_1234.jpg",
				"photoTakenTime": {
					"timestamp": "1609459200"
				}
			}`,
			wantTS:    1609459200,
			wantErr:   false,
			setupFile: true,
		},
		{
			name: "missing timestamp",
			content: `{
				"title": "IMG_1234.jpg",
				"photoTakenTime": {
					"timestamp": ""
				}
			}`,
			wantErr:   true,
			setupFile: true,
		},
		{
			name: "invalid timestamp format",
			content: `{
				"title": "IMG_1234.jpg",
				"photoTakenTime": {
					"timestamp": "invalid"
				}
			}`,
			wantErr:   true,
			setupFile: true,
		},
		{
			name: "malformed JSON",
			content: `{
				"title": "IMG_1234.jpg",
				"photoTakenTime": {
			}`,
			wantErr:   true,
			setupFile: true,
		},
		{
			name:      "file does not exist",
			wantErr:   true,
			setupFile: false,
		},
		{
			name:      "empty path",
			wantErr:   true,
			setupFile: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filePath string
			if tt.setupFile {
				filePath = filepath.Join(tmpDir, tt.name+".json")
				if err := os.WriteFile(filePath, []byte(tt.content), 0644); err != nil {
					t.Fatalf("failed to create test file: %v", err)
				}
			} else if tt.name != "empty path" {
				filePath = filepath.Join(tmpDir, "nonexistent.json")
			}

			gotTS, err := getTakeoutTimestamp(filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("getTakeoutTimestamp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				wantTime := time.Unix(tt.wantTS, 0).UTC()
				if !gotTS.Equal(wantTime) {
					t.Errorf("getTakeoutTimestamp() = %v, want %v", gotTS, wantTime)
				}
			}
		})
	}
}

func TestTakeoutMetadataStructure(t *testing.T) {
	tmpDir := t.TempDir()

	// Test with a complete metadata structure
	content := `{
		"title": "PXL_20210101_120000000.jpg",
		"description": "A photo",
		"photoTakenTime": {
			"timestamp": "1609502400",
			"formatted": "Jan 1, 2021, 12:00:00 PM UTC"
		},
		"creationTime": {
			"timestamp": "1609502400"
		}
	}`

	filePath := filepath.Join(tmpDir, "complete_metadata.json")
	if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	gotTS, err := getTakeoutTimestamp(filePath)
	if err != nil {
		t.Fatalf("getTakeoutTimestamp() unexpected error = %v", err)
	}

	wantTime := time.Unix(1609502400, 0).UTC()
	if !gotTS.Equal(wantTime) {
		t.Errorf("getTakeoutTimestamp() = %v, want %v", gotTS, wantTime)
	}
}

func TestTakeoutTimestamp_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		timestamp string
		wantErr   bool
	}{
		{
			name:      "zero timestamp",
			timestamp: "0",
			wantErr:   false,
		},
		{
			name:      "negative timestamp",
			timestamp: "-1",
			wantErr:   false,
		},
		{
			name:      "very large timestamp",
			timestamp: "9999999999",
			wantErr:   false,
		},
		{
			name:      "overflow timestamp",
			timestamp: "99999999999999999999",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := `{
				"title": "test.jpg",
				"photoTakenTime": {
					"timestamp": "` + tt.timestamp + `"
				}
			}`

			filePath := filepath.Join(tmpDir, tt.name+".json")
			if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			_, err := getTakeoutTimestamp(filePath)
			if (err != nil) != tt.wantErr {
				t.Errorf("getTakeoutTimestamp() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
