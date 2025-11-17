package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// takeoutMetadata represents the JSON metadata structure from Google Photos Takeout.
type takeoutMetadata struct {
	Title          string `json:"title"`
	PhotoTakenTime struct {
		Timestamp string `json:"timestamp"`
	} `json:"photoTakenTime"`
}

// getTakeoutTimestamp extracts the photo taken timestamp from a Google Photos Takeout metadata file.
func getTakeoutTimestamp(path string) (time.Time, error) {
	if path == "" {
		return time.Time{}, errors.New("metadata path cannot be empty")
	}

	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to open metadata file: %w", err)
	}
	defer f.Close()

	var meta takeoutMetadata
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&meta); err != nil {
		return time.Time{}, fmt.Errorf("failed to decode metadata JSON: %w", err)
	}

	if meta.PhotoTakenTime.Timestamp == "" {
		return time.Time{}, errors.New("photo taken timestamp is empty in metadata")
	}

	timestampUnix, err := strconv.ParseInt(meta.PhotoTakenTime.Timestamp, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse timestamp %q: %w", meta.PhotoTakenTime.Timestamp, err)
	}

	return time.Unix(timestampUnix, 0).UTC(), nil
}
