package utils

import (
	"time"
)

// CompareRFC3339Times compares two RFC3339 formatted time strings
// Returns:
//   - bool: true if times are equal, false otherwise
//   - error: any error that occurred during parsing
func CompareRFC3339Times(timeStr1, timeStr2 string) (bool, error) {
	time1, err := time.Parse(time.RFC3339, timeStr1)
	if err != nil {
		return false, err
	}

	time2, err := time.Parse(time.RFC3339, timeStr2)
	if err != nil {
		return false, err
	}

	return time1.Equal(time2), nil
}
