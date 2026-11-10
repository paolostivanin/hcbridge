package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"hcbridge/src/internal/homeconnect/hcfake"
)

func localTestConfig(t *testing.T, host string) *Config {
	t.Helper()
	profDir := hcfake.WriteProfileDir(t.TempDir())
	cfg, err := parseConfig([]byte("log_level: ERROR\nappliance:\n  host: " + host +
		"\n  profile: " + profDir + "\nstate_dir: " + t.TempDir() + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestLocalWatchAgainstFake(t *testing.T) {
	srv := hcfake.New()
	defer srv.Close()
	srv.SetValues(map[int]any{4409: 18, 8204: 13, 99999: 1})
	cfg := localTestConfig(t, srv.Host())
	record := filepath.Join(t.TempDir(), "obs.jsonl")

	out := &syncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runLocalWatch(ctx, cfg, false, record, out) }()

	if !waitFor(t, 5*time.Second, func() bool { return strings.Contains(out.String(), "=== snapshot") }) {
		t.Fatalf("no snapshot:\n%s", out)
	}
	srv.Notify(map[int]any{4409: 95})
	if !waitFor(t, 5*time.Second, func() bool { return strings.Contains(out.String(), "(BoostLevel1)") }) {
		t.Fatalf("no notify:\n%s", out)
	}

	// A second local client must be refused while this one runs.
	if err := runLocalWatch(context.Background(), cfg, false, "", &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "locked") {
		t.Fatalf("second local-watch: %v", err)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	spaces := regexp.MustCompile(` +`)
	got := spaces.ReplaceAllString(out.String(), " ")
	for _, want := range []string{
		"[LINK]   connected",
		"swVersion=" + hcfake.SwVersion,
		"4409 Cooking.Hob.Setting.Ventilation = 18 (Level08)",
		"8204 Cooking.Hob.Status.Zone.100.PowerLevel = 13 (65)",
		"99999 (not in profile) = 1",
		"[VAL]      4409 Cooking.Hob.Setting.Ventilation = 95 (BoostLevel1)",
		"homeconnect/cooktop/status/fan_level                = 4.5 [retained]",
		"homeconnect/cooktop/status/fan_level                = boost1 [retained]",
		"homeconnect/cooktop/zone/front_left/power           = 6.5 [retained]",
	} {
		if want = spaces.ReplaceAllString(want, " "); !strings.Contains(got, want) {
			t.Errorf("output lacks %q\n%s", want, got)
		}
	}
	if codes := srv.CloseCodes(); len(codes) != 1 || codes[0] != 1000 {
		t.Errorf("close codes %v", codes)
	}

	// The recording has the values but never the key or device info.
	f, err := os.Open(record)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	kinds := map[string]int{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.Contains(line, "AAECAwQF") || strings.Contains(line, "serial") {
			t.Fatalf("recording leaks: %s", line)
		}
		var e recEntry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatal(err)
		}
		kinds[e.Kind]++
	}
	if kinds["snapshot"] != 1 || kinds["values"] != 1 || kinds["link"] < 2 {
		t.Fatalf("recorded kinds %v", kinds)
	}
}

func TestProfileSummary(t *testing.T) {
	dir := hcfake.WriteProfileDir(t.TempDir())
	cfg := localTestConfig(t, "127.0.0.1")
	cfg.Appliance.Profile = dir
	prof, err := loadProfileFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	printProfileSummary(&b, prof, dir)
	got := b.String()
	if strings.Contains(got, "WARNING") {
		t.Errorf("0600 profile flagged:\n%s", got)
	}
	os.Chmod(filepath.Join(dir, "TEST-HOB-000000000001.json"), 0o644)
	b.Reset()
	printProfileSummary(&b, prof, dir)
	if !strings.Contains(b.String(), "TEST-HOB-000000000001.json is world-readable") {
		t.Errorf("world-readable key file not flagged:\n%s", b.String())
	}
	for _, want := range []string{
		"connectionType AES",
		"fan: Cooking.Hob.Setting.Ventilation is uid 4409",
		"zone 100 (published as front_left): PowerLevel RemainingProgramTime State",
		"zone 120: PowerLevel RemainingProgramTime State",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("summary lacks %q\n%s", want, got)
		}
	}
}
