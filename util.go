package main

import (
	"os"
	"path/filepath"
	"strconv"
)

// writeFileAtomic writes data to path via a uniquely named temp file in the
// same directory: CreateTemp makes it 0600 regardless of any stale file (a
// fixed ".tmp" name inherited an existing file's mode and was shared by
// concurrent writers), it is fsynced before the rename, and the directory is
// fsynced after it so the rename itself survives a power cut.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		if !ok {
			f.Close()
			os.Remove(tmp)
		}
	}()
	if err := f.Chmod(perm); err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

// snippet renders an HTTP body for a log line: at most 300 bytes, quoted, so
// a multi-line or HTML error page stays on one line and cannot forge log
// entries. Never use it on a successful token-endpoint body.
func snippet(b []byte) string {
	const max = 300
	if len(b) > max {
		return strconv.Quote(string(b[:max])) + "...(truncated)"
	}
	return strconv.Quote(string(b))
}
