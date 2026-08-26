package main

import (
	"bufio"
	"bytes"
	"errors"
	"runtime"
	"strings"
)

const maximumCPUInfoBytes = 1 << 20

var cpuModelCanonicalizer = strings.NewReplacer(" ", "", "-", "", "_", "")

func parseProcCPUModel(data []byte) string {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), maximumCPUInfoBytes)
	fallback := ""
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), ":")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value, err := exactCPUModel(value)
		if err != nil {
			continue
		}
		switch key {
		case "model name":
			return value
		case "Hardware":
			if fallback == "" {
				fallback = value
			}
		}
	}
	return fallback
}

func exactCPUModel(value string) (string, error) {
	model := strings.TrimSpace(value)
	if model == "" {
		return "", errors.New("CPU model is empty")
	}
	canonical := cpuModelCanonicalizer.Replace(strings.ToLower(model))
	runtimeArchitecture := cpuModelCanonicalizer.Replace(strings.ToLower(runtime.GOARCH))
	if canonical == runtimeArchitecture {
		return "", errors.New("CPU model is the generic runtime architecture")
	}
	switch canonical {
	case "arm", "arm32", "arm64", "aarch64", "armv7", "armv7l", "armv8", "armv8l",
		"amd64", "x86", "x8664", "i386", "i486", "i586", "i686",
		"riscv64", "ppc64", "ppc64le", "s390x", "loong64":
		return "", errors.New("CPU model is a generic architecture")
	}
	return model, nil
}
