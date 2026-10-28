// Package fsutil provides durable file writes. The caller writes to a temporary
// file with WriteFileDurable and then renames it into place. Durability is
// reserved for state that must survive a crash; telemetry and metrics skip it.
package fsutil

import (
	"os"
	"path/filepath"
)

// WriteFileDurable writes data to path with mode 0600 and fsyncs both the file
// and its directory, so that the caller's subsequent rename of path is durable.
// It does not rename: the caller writes path+".tmp" and renames it afterwards.
func WriteFileDurable(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
