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
		fmt.Println("Error parsing timeStr1:", err)
		return false
	}

	time2, err := time.Parse(time.RFC3339, timeStr2)
	if err != nil {
		fmt.Println("Error parsing timeStr2:", err)
		return false
	}

	return time1.Before(time2)
}
