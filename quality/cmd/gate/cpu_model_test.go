package main

import (
	"runtime"
	"testing"
)

func TestParseProcCPUModel(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
		want string
	}{
		{name: "x86 model name", data: "processor : 0\nmodel name : AMD EPYC 9R14\n", want: "AMD EPYC 9R14"},
		{name: "ARM hardware fallback", data: "processor : 0\nHardware : AWS Graviton3\n", want: "AWS Graviton3"},
		{name: "model name wins", data: "Hardware : fallback\nmodel name : exact model\n", want: "exact model"},
		{name: "generic current architecture", data: "model name : " + runtime.GOARCH + "\n", want: ""},
		{name: "generic architecture alias", data: "model name : aarch64\n", want: ""},
		{name: "exact fallback after generic model", data: "model name : arm64\nHardware : AWS Graviton3\n", want: "AWS Graviton3"},
		{name: "missing", data: "processor : 0\n", want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := parseProcCPUModel([]byte(test.data)); got != test.want {
				t.Fatalf("parseProcCPUModel() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestActualHardwareIdentityUsesHostCPUModel(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		if _, err := hostCPUModel(); err == nil {
			t.Fatal("unsupported host returned a CPU model")
		}
		return
	}
	want, err := hostCPUModel()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := actualHardwareIdentity(true)
	if err != nil {
		t.Fatal(err)
	}
	if identity.CPUModel != want {
		t.Fatalf("CPU model = %q, want %q", identity.CPUModel, want)
	}
}
