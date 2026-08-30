//go:build linux

package segment

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/fileflow"
)

func TestLinuxSpoolRenameNoReplace(t *testing.T) {
	spool := &Spool{flow: fileflow.Flow{
		FindAvailableName: fileflow.FindAvailableNameInc,
		NoCreateDirs:      true,
	}}

	t.Run("destination remains visible after return", func(t *testing.T) {
		root := t.TempDir()
		src := filepath.Join(root, "source")
		dst := filepath.Join(root, "destination")
		mustWrite(t, src, []byte("new"))

		final, err := spool.rename(src, dst)
		if err != nil {
			t.Fatalf("rename: %v", err)
		}
		if final != dst {
			t.Fatalf("final path = %q, want %q", final, dst)
		}
		if got := string(mustRead(t, dst)); got != "new" {
			t.Fatalf("destination content = %q, want new", got)
		}
		if _, err := os.Stat(src); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source still exists after rename: %v", err)
		}
	})

	t.Run("different destination is not replaced", func(t *testing.T) {
		root := t.TempDir()
		src := filepath.Join(root, "source.json")
		dst := filepath.Join(root, "destination.json")
		mustWrite(t, src, []byte("new"))
		mustWrite(t, dst, []byte("existing"))

		final, err := spool.rename(src, dst)
		if err != nil {
			t.Fatalf("rename: %v", err)
		}
		wantFinal := filepath.Join(root, "destination-1.json")
		if final != wantFinal {
			t.Fatalf("final path = %q, want %q", final, wantFinal)
		}
		if got := string(mustRead(t, dst)); got != "existing" {
			t.Fatalf("existing destination content = %q", got)
		}
		if got := string(mustRead(t, final)); got != "new" {
			t.Fatalf("conflict destination content = %q", got)
		}
	})

	t.Run("identical destination deduplicates source", func(t *testing.T) {
		root := t.TempDir()
		src := filepath.Join(root, "source")
		dst := filepath.Join(root, "destination")
		mustWrite(t, src, []byte("same"))
		mustWrite(t, dst, []byte("same"))

		final, err := spool.rename(src, dst)
		if err != nil {
			t.Fatalf("rename: %v", err)
		}
		if final != dst {
			t.Fatalf("final path = %q, want %q", final, dst)
		}
		if got := string(mustRead(t, dst)); got != "same" {
			t.Fatalf("destination content = %q", got)
		}
		if _, err := os.Stat(src); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("source still exists after deduplication: %v", err)
		}
	})
}
