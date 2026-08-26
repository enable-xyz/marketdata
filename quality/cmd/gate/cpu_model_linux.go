package main

import (
	"errors"
	"fmt"
	"io"
	"os"
)

func hostCPUModel() (string, error) {
	file, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return "", fmt.Errorf("open /proc/cpuinfo: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, maximumCPUInfoBytes+1))
	if err != nil {
		return "", fmt.Errorf("read /proc/cpuinfo: %w", err)
	}
	if len(data) > maximumCPUInfoBytes {
		return "", errors.New("/proc/cpuinfo exceeds its byte bound")
	}
	model := parseProcCPUModel(data)
	if model == "" {
		return "", errors.New("/proc/cpuinfo has no CPU model")
	}
	return model, nil
}
