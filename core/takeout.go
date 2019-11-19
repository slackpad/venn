package core

import (
	"strconv"
	"time"
)

type takeoutMetadata struct {
	Title          string
	PhotoTakenTime struct {
		Timestamp string
	}
}

func (m *takeoutMetadata) Timestamp() (time.Time, error) {
	i, err := strconv.ParseInt(m.PhotoTakenTime.Timestamp, 10, 64)
	if err != nil {
		return time.Unix(0, 0), err
	}

	return time.Unix(i, 0).UTC(), nil
}
