package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"testing"
	"time"

	"hcbridge/src/internal/homeconnect/hcfake"
)

// mainConfig writes a config for realMain and returns its path and the
// state directory.
func mainConfig(t *testing.T, hobHost, extra string) (path, stateDir string) {
	t.Helper()
	return mainConfigIn(t, hobHost, "", extra)
}

// mainConfigIn is mainConfig with a given state directory ("" for a new one).
func mainConfigIn(t *testing.T, hobHost, stateDir, extra string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	if stateDir == "" {
		stateDir = filepath.Join(dir, "state")
	}
	prof := hcfake.WriteProfileDir(t.TempDir())
	path := filepath.Join(dir, "config.yaml")
	y := fmt.Sprintf("appliance:\n  host: %s\n  profile: %s\nstate_dir: %s\nlogfile: %s\n%s",
		hobHost, prof, stateDir, filepath.Join(dir, "hcbridge.log"), extra)
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	return path, stateDir
}

func runMain(args ...string) (code int, stdout, stderr string) {
	var o, e bytes.Buffer
	code = realMain(args, &o, &e)
	return code, o.String(), e.String()
}

func TestMainFlagsAndModes(t *testing.T) {
	cfgPath, _ := mainConfig(t, "192.0.2.1", "")
	badProf, _ := mainConfig(t, "192.0.2.1", "")
	b, _ := os.ReadFile(badProf)
	os.WriteFile(badProf, []byte(strings.Replace(string(b), "profile: /", "profile: /nonexistent/", 1)), 0o600)

	cases := []struct {
		name string
		args []string
		code int
		out  string // in stdout or stderr
	}{
		{"version", []string{"--version"}, 0, "hcbridge " + version},
		{"help", []string{"-h"}, 0, "-mode"},
		{"bad flag", []string{"--nope"}, 2, "flag provided but not defined"},
		{"no config", []string{"--config=/nonexistent/config.yaml"}, 2, "config: read config"},
		{"check-config", []string{"--config=" + cfgPath, "--mode=check-config"}, 0, ": OK (hob 192.0.2.1"},
		{"check-config, bad profile", []string{"--config=" + badProf, "--mode=check-config"}, 2, "profile:"},
		{"unknown mode", []string{"--config=" + cfgPath, "--mode=fly"}, 2, "unknown mode: fly"},
		{"run without a broker", []string{"--config=" + cfgPath}, 2, "mqtt.host is required"},
	}
	for _, tc := range cases {
		code, out, errOut := runMain(tc.args...)
		if code != tc.code || !strings.Contains(out+errOut, tc.out) {
			t.Errorf("%s: exit %d, want %d; output lacks %q:\n%s%s", tc.name, code, tc.code, tc.out, out, errOut)
		}
	}
}

// The early exits of run and local-watch mode: each is reported and exits
// with its own code, before anything touches the hob or the broker.
func TestMainStartupFailures(t *testing.T) {
	broker := "mqtt:\n  host: 127.0.0.1\n  port: 1\n"

	locked, lockedDir := mainConfig(t, "192.0.2.1", broker)
	release, err := acquireLock(lockedDir)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if code, _, e := runMain("--config="+locked, "--mode=local-watch"); code != 1 || !strings.Contains(e, "local-watch:") || !strings.Contains(e, "locked") {
		t.Errorf("local-watch with the lock held: exit %d: %s", code, e)
	}
	if code, _, e := runMain("--config=" + locked); code != 1 || !strings.Contains(e, "locked") {
		t.Errorf("run with the lock held: exit %d: %s", code, e)
	}

	file := filepath.Join(t.TempDir(), "file")
	os.WriteFile(file, nil, 0o600)
	notADir, _ := mainConfigIn(t, "192.0.2.1", filepath.Join(file, "state"), broker)
	if code, _, e := runMain("--config=" + notADir); code != 2 || !strings.Contains(e, "not a directory") {
		t.Errorf("run with an unusable state dir: exit %d: %s", code, e)
	}

	badProf, _ := mainConfig(t, "192.0.2.1", broker)
	b, _ := os.ReadFile(badProf)
	os.WriteFile(badProf, []byte(strings.Replace(string(b), "profile: /", "profile: /nonexistent/", 1)), 0o600)
	if code, _, e := runMain("--config=" + badProf); code != 2 || !strings.Contains(e, "profile:") {
		t.Errorf("run with a missing profile: exit %d: %s", code, e)
	}

	badID, badIDDir := mainConfig(t, "192.0.2.1", broker)
	os.MkdirAll(badIDDir, 0o700)
	os.WriteFile(filepath.Join(badIDDir, "device_id"), []byte("not-an-id\n"), 0o600)
	if code, _, e := runMain("--config=" + badID); code != 1 || !strings.Contains(e, "device ID") {
		t.Errorf("run with a damaged device ID: exit %d: %s", code, e)
	}
}

// The whole service, from flags to SIGTERM, against an in-process broker
// and the fake hob.
func TestMainRunsTheServiceUntilSIGTERM(t *testing.T) {
	hob := hcfake.New()
	defer hob.Close()
	hob.SetValues(map[int]any{4409: 18})
	fb := newFakeBroker(t)
	host, port := fb.hostPort()
	cfgPath, stateDir := mainConfig(t, hob.Host(), fmt.Sprintf("mqtt:\n  host: %s\n  port: %d\n", host, port))

	done := make(chan int, 1)
	go func() { code, _, _ := runMain("--config=" + cfgPath); done <- code }()
	// The signal handler is installed before the first dial.
	if !waitFor(t, 10*time.Second, func() bool { return len(hob.DialTimes()) > 0 }) {
		t.Fatal("the service never dialled the hob")
	}
	root := testConfig().StateTopic("")
	waitRetained(t, fb, root+"link", "online")
	waitRetained(t, fb, root+"status/fan_level", "4.5")

	syscall.Kill(os.Getpid(), syscall.SIGTERM)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("exit %d", code)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the service did not stop on SIGTERM")
	}
	waitRetained(t, fb, root+"connected", "offline")
	if codes := hob.CloseCodes(); len(codes) != 1 || codes[0] != 1000 {
		t.Fatalf("close codes %v", codes)
	}
	for _, f := range []string{"device_id", "device.json", "link.json", "alive"} {
		if _, err := os.Stat(filepath.Join(stateDir, f)); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}

func TestDescribeBuild(t *testing.T) {
	if got := describeBuild("9.9.9", nil); got != "hcbridge 9.9.9 (rev unknown, unknown)" {
		t.Errorf("no build info: %q", got)
	}
	bi := &debug.BuildInfo{GoVersion: "go1.26.10", Settings: []debug.BuildSetting{
		{Key: "vcs.revision", Value: "0123456789abcdef0123"}, {Key: "vcs.modified", Value: "true"}, {Key: "GOOS", Value: "linux"},
	}}
	if got := describeBuild("9.9.9", bi); got != "hcbridge 9.9.9 (rev 0123456789ab-dirty, go1.26.10)" {
		t.Errorf("vcs build: %q", got)
	}
	bi.Settings = []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}, {Key: "vcs.modified", Value: "false"}}
	if got := describeBuild("9.9.9", bi); got != "hcbridge 9.9.9 (rev abc, go1.26.10)" {
		t.Errorf("clean build: %q", got)
	}
	if !strings.HasPrefix(buildString(), "hcbridge "+version+" (") {
		t.Errorf("buildString %q", buildString())
	}
}

func TestLoggerLevels(t *testing.T) {
	for in, want := range map[string]int{"debug": lvlDebug, "INFO": lvlInfo, "warn": lvlWarn, "WARNING": lvlWarn, "error": lvlError, "": lvlInfo, "loud": lvlInfo} {
		if got := parseLevel(in); got != want {
			t.Errorf("parseLevel(%q) = %d, want %d", in, got, want)
		}
	}
	path := filepath.Join(t.TempDir(), "log")
	l := NewLogger(path, "WARN")
	l.Debug("d")
	l.Info("i")
	l.Warn("w %d", 1)
	l.Error("e %d", 2)
	b, _ := os.ReadFile(path)
	got := string(b)
	if strings.Contains(got, "[DEBUG]") || strings.Contains(got, "[INFO]") || !strings.Contains(got, "[WARN] w 1") || !strings.Contains(got, "[ERROR] e 2") {
		t.Errorf("log at WARN:\n%s", got)
	}
}
