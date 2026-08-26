//go:build !darwin && !linux

package main

import (
	"fmt"
	"runtime"
)

func hostCPUModel() (string, error) {
	return "", fmt.Errorf("CPU model detection is unsupported on %s", runtime.GOOS)
}
