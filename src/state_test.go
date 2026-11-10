package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDeviceIDCreatedOnceAndStable(t *testing.T) {
	dir := t.TempDir()
	id, created, err := loadOrCreateDeviceID(dir)
	if err != nil || !created || !deviceIDRe.MatchString(id) || id == sharedDeviceID {
		t.Fatalf("first: %q %v %v", id, created, err)
	}
	fi, err := os.Stat(filepath.Join(dir, "device_id"))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("file mode: %v %v", fi.Mode(), err)
	}
	again, created, err := loadOrCreateDeviceID(dir)
	if err != nil || created || again != id {
		t.Fatalf("second: %q %v %v (first %q)", again, created, err, id)
	}
}

func TestDeviceIDNeverSilentlyReplaced(t *testing.T) {
	for _, content := range []string{"zzzzzzzz\n", "0badcafe\n", "0A1B2C3D\n", "", "0a1b2c3d4e\n"} {
		dir := t.TempDir()
		os.WriteFile(filepath.Join(dir, "device_id"), []byte(content), 0o600)
		if _, _, err := loadOrCreateDeviceID(dir); err == nil {
			t.Errorf("%q accepted", content)
		}
		b, _ := os.ReadFile(filepath.Join(dir, "device_id"))
		if string(b) != content {
			t.Errorf("%q was rewritten to %q", content, b)
		}
	}
}

func TestLockIsExclusive(t *testing.T) {
	dir := t.TempDir()
	release, err := acquireLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireLock(dir); err == nil || !strings.Contains(err.Error(), "stop it first") {
		t.Fatalf("second lock: %v", err)
	}
	release()
	release2, err := acquireLock(dir)
	if err != nil {
		t.Fatalf("after release: %v", err)
	}
	release2()
}

func TestCheckStateOwner(t *testing.T) {
	if err := checkStateOwner(t.TempDir()); err != nil {
		t.Errorf("own dir: %v", err)
	}
	if err := checkStateOwner(filepath.Join(t.TempDir(), "missing")); err != nil {
		t.Errorf("missing dir: %v", err)
	}
	if os.Geteuid() != 0 {
		if err := checkStateOwner("/"); err == nil {
			t.Error("root-owned dir accepted for a non-root process")
		}
	}
}
