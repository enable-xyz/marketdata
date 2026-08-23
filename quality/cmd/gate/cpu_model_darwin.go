package main

import "golang.org/x/sys/unix"

func hostCPUModel() (string, error) {
	model, err := unix.Sysctl("machdep.cpu.brand_string")
	if err != nil {
		return "", err
	}
	return exactCPUModel(model)
}
