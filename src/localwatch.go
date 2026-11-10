package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	hc "hcbridge/src/internal/homeconnect"
)

// --mode=local-watch connects to the appliance like the service does (same
// device ID, same connection rules) and prints every value it pushes,
// resolved against the profile. It exists to learn what the hob actually
// sends before mapping it to MQTT.

func runLocalWatch(ctx context.Context, cfg *Config, raw bool, recordPath string, out io.Writer) error {
	fmt.Fprintln(os.Stderr, "note: local-watch connects to the hob with the service's device ID; "+
		"stop the service first (the state-dir lock enforces it). Stop with Ctrl-C only.")
	if cfg.Appliance.Host == "" || cfg.Appliance.Profile == "" {
		return fmt.Errorf("appliance.host and appliance.profile are required for local-watch")
	}
	dir := cfg.StateDir()
	if err := checkStateOwner(dir); err != nil {
		return err
	}
	release, err := acquireLock(dir)
	if err != nil {
		return err
	}
	defer release()
	prof, err := loadProfileFor(cfg)
	if err != nil {
		return err
	}
	id, created, err := loadOrCreateDeviceID(dir)
	if err != nil {
		return err
	}
	if created {
		fmt.Fprintf(os.Stderr, "created device ID %s in %s\n", id, dir)
	}

	w := &watchPrinter{out: out}
	// A bridge without MQTT prints [PUB] lines: what the service would publish.
	w.bridge = NewBridge(cfg, deviceMeta{}, NewLogger("", cfg.LogLevel))
	w.bridge.out = out
	if recordPath != "" {
		f, err := os.OpenFile(recordPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		defer f.Close()
		w.rec = json.NewEncoder(f)
	}
	log := NewLogger("", cfg.LogLevel) // the terminal, never the service's log file
	client, err := hc.NewClient(hc.Options{
		Host: cfg.Appliance.Host, DeviceID: id, Log: log, Raw: raw,
	}, prof, w.handle)
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "    %s at %s, device ID %s\n", prof, cfg.Appliance.Host, id)
	client.Run(ctx)
	return nil
}

func loadProfileFor(cfg *Config) (*hc.Profile, error) {
	return hc.LoadProfile(cfg.Appliance.Profile, cfg.Appliance.HaID)
}

type watchPrinter struct {
	mu     sync.Mutex
	out    io.Writer
	rec    *json.Encoder
	bridge *Bridge // dry run; nil in tests that only check the raw lines
}

func (w *watchPrinter) line(tag, format string, args ...any) {
	if tag != "" {
		tag = "[" + tag + "]"
	}
	fmt.Fprintf(w.out, "[%s] %-8s %s\n", time.Now().Format("15:04:05.000"), tag, fmt.Sprintf(format, args...))
}

type recValue struct {
	UID    int             `json:"uid"`
	Name   string          `json:"name,omitempty"`
	Value  json.RawMessage `json:"value"`
	Symbol string          `json:"symbol,omitempty"`
}

type recEntry struct {
	T      time.Time  `json:"t"`
	Kind   string     `json:"kind"`
	Link   string     `json:"link,omitempty"`
	Values []recValue `json:"values,omitempty"`
}

func (w *watchPrinter) record(e recEntry) {
	if w.rec == nil {
		return
	}
	e.T = time.Now().UTC()
	_ = w.rec.Encode(e)
}

func (w *watchPrinter) handle(ev hc.Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch ev.Kind {
	case hc.EvLink:
		l := ev.Link
		if l.Err != nil {
			w.line("LINK", "%s: %v", l.State, l.Err)
		} else {
			w.line("LINK", "%s", l.State)
		}
		w.record(recEntry{Kind: "link", Link: string(l.State)})
	case hc.EvInfo:
		i := ev.Info
		rssi := "?"
		if i.RSSI != nil {
			rssi = strconv.Itoa(*i.RSSI)
		}
		w.line("INFO", "%s %s haVersion=%s swVersion=%s hwVersion=%s rssi=%s services=%s",
			i.Brand, i.ENumber, i.HaVersion, i.SwVersion, i.HwVersion, rssi, formatServices(i.Services))
	case hc.EvValues:
		vals := append([]hc.Value(nil), ev.Values...)
		kind := "values"
		if ev.Snapshot {
			kind = "snapshot"
			sort.Slice(vals, func(a, b int) bool { return vals[a].Name < vals[b].Name })
			w.line("SNAP", "=== snapshot: %d values ===", len(vals))
		}
		recs := make([]recValue, 0, len(vals))
		for _, v := range vals {
			tag := "VAL"
			if ev.Snapshot {
				tag = ""
			}
			w.line(tag, "%s", formatWatchValue(v))
			recs = append(recs, recValue{UID: v.UID, Name: v.Name, Value: v.Raw, Symbol: v.Symbol})
		}
		w.record(recEntry{Kind: kind, Values: recs})
		if w.bridge != nil {
			w.bridge.ApplyValues(ev.Values, ev.Snapshot)
		}
	case hc.EvDescription:
		for _, d := range ev.Changes {
			name := d.Name
			if name == "" {
				name = "(not in profile)"
			}
			avail := "?"
			if d.Available != nil {
				avail = strconv.FormatBool(*d.Available)
			}
			extra := ""
			if d.Value != nil {
				extra = " value=" + string(d.Value.Raw)
			}
			w.line("DESC", "%6d %s access=%s available=%s%s", d.UID, name, d.Access, avail, extra)
		}
		if w.bridge != nil {
			w.bridge.ApplyDescriptions(ev.Changes)
		}
	case hc.EvRaw:
		w.line("RAW", "%s %s", ev.Raw.Dir, ev.Raw.Plain)
	}
}

func formatWatchValue(v hc.Value) string {
	name := v.Name
	if name == "" {
		name = "(not in profile)"
	}
	s := fmt.Sprintf("%6d %s = %s", v.UID, name, v.Raw)
	if v.Symbol != "" {
		s += " (" + v.Symbol + ")"
	}
	return s
}

func formatServices(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s:%d", k, m[k]))
	}
	return strings.Join(parts, ",")
}

// printProfileSummary is the profile half of --mode=check-config: what the
// profile says the hob can report, without touching the network.
func printProfileSummary(w io.Writer, prof *hc.Profile, path string) {
	fmt.Fprintf(w, "profile %s: haId %s, %s %s (%s), connectionType %s, description %s, %d named UIDs, %d enums\n",
		path, prof.HaID, prof.Brand, prof.VIB, prof.Type, prof.ConnectionType, prof.DDFVersion, len(prof.Features()), prof.EnumCount())
	for _, f := range worldReadableKeyFiles(path) {
		fmt.Fprintf(w, "WARNING: %s is world-readable; it holds the hob's key (chmod 0640, owner root, group hcbridge)\n", f)
	}
	zones := map[string][]string{}
	var notable []string
	for _, f := range prof.Features() {
		if m := zoneKeyRe.FindStringSubmatch(f.Name); m != nil {
			zones[m[1]] = append(zones[m[1]], m[2])
			continue
		}
		for _, kw := range []string{"Ventilation", "Hood", "CarbonFilter", "JoinZone", "ZoneSelector", "PowerLevel", "RemainingProgramTime"} {
			if strings.Contains(f.Name, kw) {
				notable = append(notable, fmt.Sprintf("%s (uid %d, %s, access %s)", f.Name, f.UID, orDash(f.Kind), orDash(f.Access)))
				break
			}
		}
	}
	if uid, ok := prof.UID("Cooking.Hob.Setting.Ventilation"); ok {
		fmt.Fprintf(w, "fan: Cooking.Hob.Setting.Ventilation is uid %d\n", uid)
	} else {
		fmt.Fprintln(w, "fan: Cooking.Hob.Setting.Ventilation NOT in the profile")
	}
	ids := make([]string, 0, len(zones))
	for id := range zones {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		sort.Strings(zones[id])
		used := ""
		if _, ok := zoneHalves[id]; ok {
			used = " (published as " + zoneSlug(zoneHalves[id]) + ")"
		}
		fmt.Fprintf(w, "zone %s%s: %s\n", id, used, strings.Join(zones[id], " "))
	}
	for _, n := range notable {
		fmt.Fprintf(w, "key: %s\n", n)
	}
}

// worldReadableKeyFiles returns the profile files holding key material
// (the ZIP itself, or the JSON files of a directory) that anyone can read.
func worldReadableKeyFiles(path string) []string {
	fi, err := os.Stat(path)
	if err != nil {
		return nil
	}
	if !fi.IsDir() {
		if fi.Mode().Perm()&0o004 != 0 {
			return []string{path}
		}
		return nil
	}
	var out []string
	matches, _ := filepath.Glob(filepath.Join(path, "*.json"))
	for _, m := range matches {
		if mfi, err := os.Stat(m); err == nil && mfi.Mode().Perm()&0o004 != 0 {
			out = append(out, m)
		}
	}
	return out
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
