package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestEnvironmentSecretResolverReadsValidatedDescriptor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	want := []byte("descriptor-owned secret")
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ENABLE_MARKET_TEST_SECRET", "@"+path)

	got, err := (environmentSecretResolver{}).Resolve(t.Context(), "ENABLE_MARKET_TEST_SECRET")
	if err != nil {
		t.Fatal(err)
	}
	defer clear(got)
	if !bytes.Equal(got, want) {
		t.Fatalf("resolved secret = %q, want validated descriptor bytes", got)
	}
	if cap(got) != len(want) {
		t.Fatalf("resolved secret capacity = %d, want exact descriptor size %d", cap(got), len(want))
	}
}

func TestReadExactSecretRejectsShrinkAndClearsBuffer(t *testing.T) {
	reader := &recordingSecretReader{reader: bytes.NewReader([]byte("short"))}

	secret, err := readExactSecret(reader, int64(len("expected-size")))
	if err == nil {
		clear(secret)
		t.Fatal("readExactSecret accepted a post-stat shrink")
	}
	if secret != nil {
		clear(secret)
		t.Fatal("readExactSecret returned bytes after a post-stat shrink")
	}
	requireClearedReadBuffers(t, reader.reads)
}

func TestReadExactSecretRejectsPostStatGrowthAndClearsBuffers(t *testing.T) {
	const expected = "exact-size"
	reader := &recordingSecretReader{reader: bytes.NewReader([]byte(expected + "!"))}

	secret, err := readExactSecret(reader, int64(len(expected)))
	if err == nil {
		clear(secret)
		t.Fatal("readExactSecret accepted post-stat growth")
	}
	if secret != nil {
		clear(secret)
		t.Fatal("readExactSecret returned bytes after post-stat growth")
	}
	if len(reader.reads) != 2 || len(reader.reads[1]) != 1 {
		t.Fatalf("readExactSecret reads = %d, want one exact read and one bounded extra-byte read", len(reader.reads))
	}
	requireClearedReadBuffers(t, reader.reads)
}

type recordingSecretReader struct {
	reader *bytes.Reader
	reads  [][]byte
}

func (r *recordingSecretReader) Read(buffer []byte) (int, error) {
	r.reads = append(r.reads, buffer)
	return r.reader.Read(buffer)
}

func requireClearedReadBuffers(t *testing.T, buffers [][]byte) {
	t.Helper()
	for readIndex, buffer := range buffers {
		for byteIndex, value := range buffer {
			if value != 0 {
				t.Fatalf("read buffer %d byte %d was not cleared", readIndex, byteIndex)
			}
		}
	}
}

func TestReadValidatedSecretFileRejectsPathSwap(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "secret")
	if err := os.WriteFile(path, []byte("checked inode"), 0o600); err != nil {
		t.Fatal(err)
	}
	checked, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path, filepath.Join(directory, "checked-inode")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement inode"), 0o600); err != nil {
		t.Fatal(err)
	}

	secret, err := readValidatedSecretFile(path, checked)
	if err == nil {
		clear(secret)
		t.Fatal("readValidatedSecretFile accepted a replacement inode")
	}
	if secret != nil {
		clear(secret)
		t.Fatal("readValidatedSecretFile returned bytes from a replacement inode")
	}
}

func TestReadValidatedSecretFileRechecksDescriptorSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	checked, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte{'x'}, maximumResolvedSecretBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}

	secret, err := readValidatedSecretFile(path, checked)
	if err == nil {
		clear(secret)
		t.Fatal("readValidatedSecretFile accepted an oversized validated inode")
	}
	if secret != nil {
		clear(secret)
		t.Fatal("readValidatedSecretFile returned oversized bytes")
	}
}
