//go:build !linux

package segment

func (s *Spool) rename(src, dst string) (string, error) {
	return s.flow.Rename(src, dst)
}
