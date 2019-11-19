package core

import (
	"encoding/json"
	"os"
	"strconv"
	"time"
)

type takeoutMetadata struct {
	Title          string
	PhotoTakenTime struct {
		Timestamp string
	}
}

func getTakeoutTimestamp(path string) (time.Time, error) {
	f, err := os.Open(path)
	if err != nil {
		return time.Time{}, err
	}
	defer f.Close()

	var meta takeoutMetadata
	dec := json.NewDecoder(f)
	if err := dec.Decode(&meta); err != nil {
		return time.Time{}, err
	}

	ts, err := strconv.ParseInt(meta.PhotoTakenTime.Timestamp, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	return time.Unix(ts, 0).UTC(), nil
}
