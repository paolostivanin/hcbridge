package homeconnect

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	testProfileDir  = "hcfake/profile"
	testHaID        = "TEST-HOB-000000000001"
	testProfileJSON = testHaID + ".json"
	testDDName      = testHaID + "_DeviceDescription.xml"
	testFMName      = testHaID + "_FeatureMapping.xml"
)

func readTestFile(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(testProfileDir, name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func loadTestProfile(t *testing.T) *Profile {
	t.Helper()
	p, err := LoadProfile(testProfileDir, "")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// writeZip builds a ZIP with the given entries (name -> content).
func writeZip(t *testing.T, entries map[string][]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "profile.zip")
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for name, data := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		w.Write(data)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadProfileDir(t *testing.T) {
	p := loadTestProfile(t)
	if p.HaID != testHaID || p.ConnectionType != "AES" || p.Brand != "BOSCH" || p.VIB != "TESTHOB01" || p.DDFVersion != "2-6" {
		t.Fatalf("got %v, DDF %q", p, p.DDFVersion)
	}
	psk, iv := p.Keys()
	if !bytes.Equal(psk, seq(0, 32)) || !bytes.Equal(iv, seq(0xa0, 16)) {
		t.Fatal("key material not decoded")
	}
}

// The downloader's ZIP: XMLs possibly in a subfolder and not named as the
// JSON declares, JSON at the root.
func TestLoadProfileZip(t *testing.T) {
	zipPath := writeZip(t, map[string][]byte{
		testProfileJSON: readTestFile(t, testProfileJSON),
		"files/" + testHaID + "_2-6_DeviceDescription.xml": readTestFile(t, testDDName),
		"files/" + testHaID + "_2-6_FeatureMapping.xml":    readTestFile(t, testFMName),
		"readme.txt": []byte("ignored"),
	})
	p, err := LoadProfile(zipPath, testHaID)
	if err != nil {
		t.Fatal(err)
	}
	if p.Source != zipPath {
		t.Errorf("source %q", p.Source)
	}
	if uid, ok := p.UID("Cooking.Hob.Setting.Ventilation"); !ok || uid != 4409 {
		t.Fatalf("ventilation uid %d %v", uid, ok)
	}
}

func metaWith(t *testing.T, mutate func(m map[string]any)) []byte {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(readTestFile(t, testProfileJSON), &m); err != nil {
		t.Fatal(err)
	}
	mutate(m)
	b, _ := json.Marshal(m)
	return b
}

func parseWithMeta(t *testing.T, meta []byte) (*Profile, error) {
	return ParseProfile(meta, bytes.NewReader(readTestFile(t, testDDName)), bytes.NewReader(readTestFile(t, testFMName)))
}

func TestParseProfileRejectsTLS(t *testing.T) {
	_, err := parseWithMeta(t, metaWith(t, func(m map[string]any) {
		m["connectionType"] = "TLS"
		delete(m, "iv")
	}))
	if err == nil || !strings.Contains(err.Error(), "TLS") || !strings.Contains(err.Error(), "AES") {
		t.Fatalf("got %v", err)
	}
}

func TestParseProfileKeyValidation(t *testing.T) {
	cases := map[string]func(m map[string]any){
		"short key":  func(m map[string]any) { m["key"] = "AAEC" },
		"bad key":    func(m map[string]any) { m["key"] = "!!!!" },
		"short iv":   func(m map[string]any) { m["iv"] = "AAEC" },
		"missing iv": func(m map[string]any) { delete(m, "iv") },
		"odd type":   func(m map[string]any) { m["connectionType"] = "BLE" },
		"no haId":    func(m map[string]any) { delete(m, "haId") },
	}
	for name, mutate := range cases {
		if _, err := parseWithMeta(t, metaWith(t, mutate)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// Padded and standard-alphabet base64 are accepted too.
	p, err := parseWithMeta(t, metaWith(t, func(m map[string]any) {
		m["key"] = base64.StdEncoding.EncodeToString(seq(0xf0, 32)) // contains '+' and '/'... and '='
		m["connectionType"] = ""
	}))
	if err != nil {
		t.Fatal(err)
	}
	if psk, _ := p.Keys(); !bytes.Equal(psk, seq(0xf0, 32)) {
		t.Fatal("std base64 key not decoded")
	}
}

func TestLoadProfileSelection(t *testing.T) {
	other := metaWith(t, func(m map[string]any) { m["haId"] = "TEST-HOB-000000000002" })
	zipPath := writeZip(t, map[string][]byte{
		testProfileJSON:              readTestFile(t, testProfileJSON),
		"TEST-HOB-000000000002.json": other,
		testDDName:                   readTestFile(t, testDDName),
		testFMName:                   readTestFile(t, testFMName),
		"TEST-HOB-000000000002_FeatureMapping.xml":    readTestFile(t, testFMName),
		"TEST-HOB-000000000002_DeviceDescription.xml": readTestFile(t, testDDName),
	})
	if _, err := LoadProfile(zipPath, ""); err == nil || !strings.Contains(err.Error(), "ha_id") {
		t.Fatalf("ambiguous: got %v", err)
	}
	p, err := LoadProfile(zipPath, "TEST-HOB-000000000002")
	if err != nil || p.HaID != "TEST-HOB-000000000002" {
		t.Fatalf("select: %v %v", p, err)
	}
	if _, err := LoadProfile(zipPath, "NOPE"); err == nil || !strings.Contains(err.Error(), "NOPE") {
		t.Fatalf("mismatch: got %v", err)
	}
}

func TestLoadProfileMissingXML(t *testing.T) {
	zipPath := writeZip(t, map[string][]byte{
		testProfileJSON: readTestFile(t, testProfileJSON),
		testFMName:      readTestFile(t, testFMName),
	})
	if _, err := LoadProfile(zipPath, ""); err == nil || !strings.Contains(err.Error(), "DeviceDescription") {
		t.Fatalf("got %v", err)
	}
	if _, err := LoadProfile(filepath.Join(t.TempDir(), "nope.zip"), ""); err == nil {
		t.Fatal("missing file accepted")
	}
	empty := writeZip(t, map[string][]byte{"x.json": []byte(`{}`)})
	if _, err := LoadProfile(empty, ""); err == nil {
		t.Fatal("zip without profile accepted")
	}
}

func TestFeatures(t *testing.T) {
	p := loadTestProfile(t)
	f, ok := p.Feature(531)
	if !ok || f.Name != "BSH.Common.Setting.PowerState" || f.Kind != "setting" || f.EnumID != 0x0102 || f.Access != "readwrite" {
		t.Fatalf("PowerState feature %+v", f)
	}
	f, _ = p.Feature(8209)
	if f.CID != 0x10 || f.Min == nil || *f.Max != 5940 {
		t.Fatalf("zone timer feature %+v", f)
	}
	// In the mapping but not the description, and vice versa.
	if f, ok := p.Feature(20481); !ok || f.Name != "Test.MappingOnly.Feature" || f.Kind != "" {
		t.Fatalf("mapping-only %+v %v", f, ok)
	}
	if f, ok := p.Feature(24577); !ok || f.Name != "" || f.Kind != "status" {
		t.Fatalf("description-only %+v %v", f, ok)
	}
	// Program options carry refUID, not uid: they must not redefine 0x0242.
	if f, _ := p.Feature(578); f.Kind != "option" {
		t.Fatalf("option kind %q", f.Kind)
	}
	if n := len(p.Features()); n != 19 {
		t.Errorf("named features = %d, want 19", n)
	}
}

func TestDecode(t *testing.T) {
	p := loadTestProfile(t)
	cases := []struct {
		uid     int
		raw     string
		name    string
		display any
	}{
		{4409, `18`, "Cooking.Hob.Setting.Ventilation", "Level08"},
		{4409, `95`, "Cooking.Hob.Setting.Ventilation", "BoostLevel1"},
		{4409, `42`, "Cooking.Hob.Setting.Ventilation", 42.0}, // unknown member
		{8204, `13`, "Cooking.Hob.Status.Zone.100.PowerLevel", "65"},
		{8202, `3`, "Cooking.Hob.Status.Zone.100.State", "ResiduelHeat"},
		{552, `3`, "BSH.Common.Status.OperationState", "Run"},
		{537, `true`, "BSH.Common.Setting.ChildLock", true},
		{8209, `754`, "Cooking.Hob.Status.Zone.100.RemainingProgramTime", 754.0},
		{288, `12289`, "BSH.Common.Root.ActiveProgram", "Cooking.Hob.Program.PowerLevelMode"},
		{288, `0`, "BSH.Common.Root.ActiveProgram", nil},
		{525, `1`, "BSH.Common.Event.ProgramFinished", "Present"}, // subset enum borrows names
		{99999, `1`, "", 1.0},
		{552, `not json`, "BSH.Common.Status.OperationState", nil},
	}
	for _, c := range cases {
		v := p.Decode(c.uid, json.RawMessage(c.raw))
		if v.Name != c.name || fmt.Sprint(v.Display()) != fmt.Sprint(c.display) || v.Known() != (c.name != "") {
			t.Errorf("Decode(%d, %s) = name %q display %v (%T), want %q %v", c.uid, c.raw, v.Name, v.Display(), v.Display(), c.name, c.display)
		}
	}
	// The subset enum only knows its own values.
	if v := p.Decode(525, json.RawMessage(`2`)); v.Symbol != "" {
		t.Errorf("subset enum resolved value outside the subset: %q", v.Symbol)
	}
}

func TestProfileNeverPrintsKey(t *testing.T) {
	p := loadTestProfile(t)
	psk, iv := p.Keys()
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		for _, arg := range []any{p, *p} {
			out := fmt.Sprintf(verb, arg)
			for _, secret := range [][]byte{psk, iv} {
				for _, enc := range []string{fmt.Sprint(secret), fmt.Sprintf("%x", secret), base64.RawURLEncoding.EncodeToString(secret)} {
					if strings.Contains(out, enc) {
						t.Fatalf("%s leaks key material: %s", verb, out)
					}
				}
			}
		}
	}
}

func TestParseHex(t *testing.T) {
	for in, want := range map[string]int{"0228": 552, "0x0228": 552, "0X1139": 4409, " 200C ": 8204} {
		if got, err := parseHex(in); err != nil || got != want {
			t.Errorf("parseHex(%q) = %d, %v", in, got, err)
		}
	}
	if _, err := parseHex("zz"); err == nil {
		t.Error("bad hex accepted")
	}
}
