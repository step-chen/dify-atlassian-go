package utils

import (
	"fmt"
	"time"
)

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

func BeforeRFC3339Times(timeStr1, timeStr2 string) bool {
	time1, err := time.Parse(time.RFC3339, timeStr1)
	if err != nil {
		// fmt.Println("Error parsing timeStr1:", err)
		return true
	}

	time2, err := time.Parse(time.RFC3339, timeStr2)
	if err != nil {
		// fmt.Println("Error parsing timeStr2:", err)
		return true
	}

	return time1.Before(time2)
}

func RFC3339ToUnix(s string) (int64, error) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, fmt.Errorf("failed to parse RFC3339 string '%s': %w", s, err)
	}

	return t.Unix(), nil
}
