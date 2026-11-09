package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

// The state directory (state_dir, /var/lib/hcbridge) holds what must
// survive restarts: the device ID the bridge announces to the appliance,
// the lock that keeps two local clients from running at once, and the
// liveness file.

var deviceIDRe = regexp.MustCompile(`^[0-9a-f]{8}$`)

// hcpy and several other tools all announce this ID. Two clients with the
// same ID evict each other on the appliance, so it is never ours.
const sharedDeviceID = "0badcafe"

// checkStateOwner refuses to run as a user other than the owner of the
// state directory: run as root, a mode would leave root-owned files the
// service cannot read or replace.
func checkStateOwner(dir string) error {
	fi, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil
	}
	if euid := os.Geteuid(); uint32(euid) != st.Uid {
		return fmt.Errorf("state directory %s is owned by uid %d but this process runs as uid %d; "+
			"run as the owner (e.g. su -s /bin/sh hcbridge -c '...')", dir, st.Uid, euid)
	}
	return nil
}

// loadOrCreateDeviceID returns the stable device ID, creating it on first
// use. The appliance keys its list of local clients on this ID, and a
// reconnect with the same ID replaces our own stale session there, so it
// must never change on its own: a damaged file is an error, not a reason to
// mint a new one.
func loadOrCreateDeviceID(dir string) (id string, created bool, err error) {
	path := filepath.Join(dir, "device_id")
	b, err := os.ReadFile(path)
	switch {
	case err == nil:
		id = strings.TrimSpace(string(b))
		if !deviceIDRe.MatchString(id) || id == sharedDeviceID {
			return "", false, fmt.Errorf("%s holds %q, not an 8-digit lowercase hex device ID; "+
				"fix or delete it (deleting registers the bridge as a new client on the appliance)", path, id)
		}
		return id, false, nil
	case !errors.Is(err, os.ErrNotExist):
		return "", false, err
	}
	for {
		raw := make([]byte, 4)
		if _, err := rand.Read(raw); err != nil {
			return "", false, err
		}
		if id = hex.EncodeToString(raw); id != sharedDeviceID {
			break
		}
	}
	if err := writeFileAtomic(path, []byte(id+"\n"), 0o600); err != nil {
		return "", false, err
	}
	return id, true, nil
}

// acquireLock takes an exclusive lock on <dir>/lock for the life of the
// process. The service and --mode=local-watch both hold it: two local
// clients with the same device ID would keep evicting each other.
func acquireLock(dir string) (release func(), err error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%s is locked: another hcbridge (the service or local-watch) is "+
				"connected to the appliance; stop it first", path)
		}
		return nil, fmt.Errorf("lock %s: %w", path, err)
	}
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	}, nil
}

// deviceMeta is what the bridge last learned about the hob: from the profile
// and from the handshake's device info. Persisted (device.json) so the HA
// device block carries the firmware versions from the first discovery
// publish, before the first connection of a run.
type deviceMeta struct {
	Brand     string    `json:"brand,omitempty"`
	VIB       string    `json:"vib,omitempty"`
	Serial    string    `json:"serial,omitempty"`
	HaVersion string    `json:"ha_version,omitempty"` // appliance software
	SwVersion string    `json:"sw_version,omitempty"` // network module software
	HwVersion string    `json:"hw_version,omitempty"` // network module hardware
	SeenAt    time.Time `json:"seen_at,omitempty"`
}

// sameVersions reports whether two metas describe the same firmware.
func (m deviceMeta) sameVersions(o deviceMeta) bool {
	return m.HaVersion == o.HaVersion && m.SwVersion == o.SwVersion && m.HwVersion == o.HwVersion
}

func loadDeviceMeta(dir string) (deviceMeta, error) {
	var m deviceMeta
	b, err := os.ReadFile(filepath.Join(dir, "device.json"))
	if errors.Is(err, os.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return m, err
	}
	return m, json.Unmarshal(b, &m)
}

func saveDeviceMeta(dir string, m deviceMeta) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(dir, "device.json"), append(b, '\n'), 0o600)
}

// linkRecord persists the link episode across restarts (link.json): a
// restart in the middle of a 503 episode must neither restart the
// power-cycle clock nor dial the hob again right away.
type linkRecord struct {
	State        string    `json:"state"`
	Since        time.Time `json:"since"`
	RefusedSince time.Time `json:"refused_since,omitempty"`
	Latched      bool      `json:"power_cycle_needed,omitempty"`
	LastDial     time.Time `json:"last_dial,omitempty"`
}

func loadLinkRecord(dir string) (linkRecord, error) {
	var r linkRecord
	b, err := os.ReadFile(filepath.Join(dir, "link.json"))
	if errors.Is(err, os.ErrNotExist) {
		return r, nil
	}
	if err != nil {
		return r, err
	}
	return r, json.Unmarshal(b, &r)
}

func saveLinkRecord(dir string, r linkRecord) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(dir, "link.json"), append(b, '\n'), 0o600)
}
