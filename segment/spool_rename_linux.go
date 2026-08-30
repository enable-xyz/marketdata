//go:build linux

package segment

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/spf13/fileflow"
	"golang.org/x/sys/unix"
)

const spoolRenameAttempts = 100

// rename moves one spool file without replacing an existing destination.
// Linux uses renameat2 rather than link+unlink because the latter can make the
// destination transiently disappear on Docker Desktop VirtioFS bind mounts.
func (s *Spool) rename(src, dst string) (string, error) {
	if sameSpoolFile(src, dst) {
		return "", fileflow.ErrSameFile
	}

	candidate := dst
	for range spoolRenameAttempts {
		err := unix.Renameat2(unix.AT_FDCWD, src, unix.AT_FDCWD, candidate, unix.RENAME_NOREPLACE)
		if err == nil {
			return candidate, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return "", &fileflow.ErrFailedMovingFile{Err: err, Src: src, Dst: candidate}
		}

		identical, equalErr := fileflow.Equal(src, candidate)
		if equalErr != nil {
			if errors.Is(equalErr, fs.ErrNotExist) {
				continue
			}
			return "", fmt.Errorf("checking file identity: %w", equalErr)
		}
		if identical {
			if removeErr := os.Remove(src); removeErr != nil {
				return candidate, &fileflow.ErrFailedRemovingOriginal{Err: removeErr, File: src}
			}
			return candidate, nil
		}

		findAvailableName := s.flow.FindAvailableName
		if findAvailableName == nil {
			findAvailableName = fileflow.FindAvailableNameInc
		}
		candidate, err = findAvailableName(dst)
		if err != nil {
			return "", fmt.Errorf("finding available name: %w", err)
		}
	}
	return "", fileflow.ErrMaxAttemptsReached
}

func sameSpoolFile(src, dst string) bool {
	if src == dst {
		return true
	}
	srcInfo, srcErr := os.Stat(src)
	dstInfo, dstErr := os.Stat(dst)
	return srcErr == nil && dstErr == nil && os.SameFile(srcInfo, dstInfo)
}
