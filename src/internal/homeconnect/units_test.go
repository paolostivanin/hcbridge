package homeconnect

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Failure paths and small units the scenario tests do not reach.

func zipWith(t *testing.T, build func(zw *zip.Writer)) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "profile.zip")
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	build(zw)
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadProfileZipFailures(t *testing.T) {
	notZip := filepath.Join(t.TempDir(), "profile.zip")
	os.WriteFile(notZip, []byte("not a zip"), 0o600)
	tls := metaWith(t, func(m map[string]any) { m["connectionType"] = "TLS" })
	cases := map[string]struct {
		path string
		want string
	}{
		"not a zip": {notZip, "zip"},
		"no feature mapping": {writeZip(t, map[string][]byte{
			testProfileJSON: readTestFile(t, testProfileJSON), testDDName: readTestFile(t, testDDName),
		}), "FeatureMapping"},
		"TLS profile": {writeZip(t, map[string][]byte{
			testProfileJSON: tls, testDDName: readTestFile(t, testDDName), testFMName: readTestFile(t, testFMName),
		}), "TLS"},
		"too many entries": {zipWith(t, func(zw *zip.Writer) {
			for i := 0; i <= maxProfileFiles; i++ {
				zw.Create(fmt.Sprintf("f%d.txt", i))
			}
		}), "too many entries"},
		"entry too large": {zipWith(t, func(zw *zip.Writer) {
			w, _ := zw.Create("big.xml")
			w.Write(make([]byte, maxProfileFileSz+1))
		}), "too large"},
		"unknown compression": {zipWith(t, func(zw *zip.Writer) {
			w, _ := zw.CreateRaw(&zip.FileHeader{Name: "a.json", Method: 99, CompressedSize64: 1, UncompressedSize64: 1})
			w.Write([]byte{1})
		}), "algorithm"},
		"corrupt entry": {zipWith(t, func(zw *zip.Writer) {
			w, _ := zw.CreateRaw(&zip.FileHeader{Name: "a.json", Method: zip.Deflate, CompressedSize64: 4, UncompressedSize64: 100})
			w.Write([]byte{0xff, 0xff, 0xff, 0xff})
		}), "a.json"},
	}
	for name, tc := range cases {
		if _, err := LoadProfile(tc.path, ""); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v (want %q)", name, err, tc.want)
		}
	}
}

func copyProfileDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, n := range []string{testProfileJSON, testDDName, testFMName} {
		os.WriteFile(filepath.Join(dir, n), readTestFile(t, n), 0o600)
	}
	return dir
}

func TestLoadProfileDirLimits(t *testing.T) {
	// Files three directories down and files that are not JSON or XML are
	// ignored.
	dir := copyProfileDir(t)
	os.MkdirAll(filepath.Join(dir, "a", "b", "c"), 0o700)
	os.WriteFile(filepath.Join(dir, "a", "b", "c", "other.json"), []byte(`{"haId":"X","key":"k"}`), 0o600)
	os.WriteFile(filepath.Join(dir, "README.txt"), []byte("hi"), 0o600)
	if p, err := LoadProfile(dir, ""); err != nil || p.HaID != testHaID {
		t.Fatalf("ignored files: %v", err)
	}

	many := copyProfileDir(t)
	for i := 0; i < maxProfileFiles; i++ {
		os.WriteFile(filepath.Join(many, fmt.Sprintf("x%03d.json", i)), []byte("{}"), 0o600)
	}
	if _, err := LoadProfile(many, ""); err == nil || !strings.Contains(err.Error(), "too many files") {
		t.Errorf("too many files: %v", err)
	}

	big := copyProfileDir(t)
	f, _ := os.Create(filepath.Join(big, "big.xml"))
	f.Truncate(maxProfileFileSz + 1) // sparse
	f.Close()
	if _, err := LoadProfile(big, ""); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Errorf("large file: %v", err)
	}

	if os.Geteuid() != 0 {
		locked := copyProfileDir(t)
		os.Chmod(filepath.Join(locked, testDDName), 0)
		if _, err := LoadProfile(locked, ""); err == nil {
			t.Error("unreadable file accepted")
		}
		sub := copyProfileDir(t)
		os.Mkdir(filepath.Join(sub, "private"), 0)
		defer os.Chmod(filepath.Join(sub, "private"), 0o700)
		if _, err := LoadProfile(sub, ""); err == nil {
			t.Error("unreadable directory accepted")
		}
	}
}

func TestFindXMLPrefersTheHaID(t *testing.T) {
	files := []profileFile{
		{name: "OTHER_DeviceDescription.xml", data: []byte("other")},
		{name: testHaID + "_DeviceDescription.xml", data: []byte("ours")},
	}
	if b, err := findXML(files, "", "_DeviceDescription.xml", testHaID); err != nil || string(b) != "ours" {
		t.Errorf("preferred the wrong file: %q %v", b, err)
	}
	files = append(files, profileFile{name: "copy/" + testHaID + "_DeviceDescription.xml", data: []byte("copy")})
	if _, err := findXML(files, "", "_DeviceDescription.xml", testHaID); err == nil || !strings.Contains(err.Error(), "several") {
		t.Errorf("ambiguous files: %v", err)
	}
}

const (
	unitFM = `<?xml version="1.0" encoding="ISO-8859-1"?>
<featureMappingFile>
<featureDescription><feature refUID="0001">A.Status</feature></featureDescription>
<enumDescriptionList>
<enumDescription refENID="0010" enumKey="Super"><enumMember refValue="1">One</enumMember><enumMember refValue="2">Two</enumMember></enumDescription>
<enumDescription refENID="0011" enumKey="Own"><enumMember refValue="1">Mine</enumMember></enumDescription>
</enumDescriptionList>
</featureMappingFile>`
	unitDD = `<device><description><version>3</version><revision>1</revision></description>
<statusList>
<status uid="0001" refCID="01" enumerationType="0012" access="READ" available="true" min="zero" max="3"/>
<status uid="0001" refCID="02"/>
</statusList>
<enumerationTypeList>
<enumerationType enid="0011" subsetOf="0010"><enumeration value="2"/></enumerationType>
<enumerationType enid="0012" subsetOf="0010"><enumeration value="2"/><enumeration value="x"/></enumerationType>
<enumerationType enid="0013" subsetOf="00FF"><enumeration value="1"/></enumerationType>
</enumerationTypeList></device>`
)

func unitMeta(t *testing.T) []byte {
	return metaWith(t, func(map[string]any) {})
}

// Subsets borrow their superset's names unless they have their own or the
// superset is unknown; the first declaration of a UID wins; malformed
// numbers are skipped.
func TestParseProfileSubsetsAndDuplicates(t *testing.T) {
	p, err := ParseProfile(unitMeta(t), strings.NewReader(unitDD), strings.NewReader(unitFM))
	if err != nil {
		t.Fatal(err)
	}
	if p.DDFVersion != "3-1" || p.EnumCount() != 3 {
		t.Fatalf("version %q, %d enums", p.DDFVersion, p.EnumCount())
	}
	f, _ := p.Feature(1)
	if f.CID != 1 || f.EnumID != 0x12 || f.Access != "read" || f.Min != nil || f.Max == nil || *f.Max != 3 {
		t.Fatalf("feature %+v", f)
	}
	if v := p.Decode(1, []byte("2")); v.Symbol != "Two" {
		t.Errorf("subset value 2 = %q, want the superset's name", v.Symbol)
	}
	if own := p.enums[0x11]; own.Members[1] != "Mine" || len(own.Members) != 1 {
		t.Errorf("a subset with its own names was overwritten: %v", own.Members)
	}
	if _, ok := p.enums[0x13]; ok {
		t.Error("a subset of an unknown enum was built")
	}
}

func TestParseProfileFailures(t *testing.T) {
	meta := unitMeta(t)
	cases := map[string]struct {
		meta   []byte
		dd, fm string
		want   string
	}{
		"bad JSON":        {[]byte("{"), unitDD, unitFM, "profile JSON"},
		"no features":     {meta, unitDD, `<featureMappingFile/>`, "no features"},
		"bad refUID":      {meta, unitDD, `<x><feature refUID="zz">A</feature></x>`, "refUID"},
		"bad refENID":     {meta, unitDD, `<x><enumDescription refENID="zz"/></x>`, "refENID"},
		"bad refValue":    {meta, unitDD, `<x><enumMember refValue="one">A</enumMember></x>`, "refValue"},
		"cut-off feature": {meta, unitDD, `<x><feature refUID="1">A`, "feature mapping"},
		"cut-off member":  {meta, unitDD, `<x><enumMember refValue="1">A`, "feature mapping"},
		"broken mapping":  {meta, unitDD, `<x><feature refUID="1">A</feature></y>`, "feature mapping"},
		"bad uid":         {meta, `<device><status uid="zz"/></device>`, unitFM, "uid"},
		"broken device":   {meta, `<device><status uid="1"></x>`, unitFM, "device description"},
	}
	for name, tc := range cases {
		if _, err := ParseProfile(tc.meta, strings.NewReader(tc.dd), strings.NewReader(tc.fm)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v (want %q)", name, err, tc.want)
		}
	}
}

func TestFlexIntForms(t *testing.T) {
	for in, want := range map[string]int64{`null`: 0, ``: 0, `""`: 0, `" 7 "`: 7, `5`: 5, `5.9`: 5, `"6.0"`: 6} {
		var f flexInt
		if err := f.UnmarshalJSON([]byte(in)); err != nil || int64(f) != want {
			t.Errorf("%q: %d %v, want %d", in, f, err, want)
		}
	}
	for _, in := range []string{`"\q"`, `"x"`, `true`} {
		var f flexInt
		if err := f.UnmarshalJSON([]byte(in)); err == nil {
			t.Errorf("%q accepted", in)
		}
	}
	if _, err := parseMessage([]byte(`{"sID":1,"msgID":2,"resource":"/r","action":"GET","data":[1,}`)); err == nil {
		t.Error("broken data list accepted")
	}
	if _, err := parseMessage([]byte(`{"sID":1,"msgID":2,"resource":"/r","action":"GET","data":["a",]}`)); err == nil {
		t.Error("broken data list accepted")
	}
	if got := (&TimeoutError{Resource: "/ro/values"}).Error(); got != "/ro/values: no response" {
		t.Errorf("TimeoutError %q", got)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("no entropy") }

// A codec whose padding source fails is broken for good: nothing more is
// sent with it.
func TestCodecWithoutEntropyIsBroken(t *testing.T) {
	psk, iv := make([]byte, 32), make([]byte, blockLen)
	c, err := NewCodec(psk, iv, RoleClient)
	if err != nil {
		t.Fatal(err)
	}
	c.rand = failingReader{}
	if _, err := c.Seal([]byte("x")); err == nil || !strings.Contains(err.Error(), "padding") {
		t.Fatalf("seal without entropy: %v", err)
	}
	c.rand = bytes.NewReader(make([]byte, 64))
	if _, err := c.Seal([]byte("x")); !errors.Is(err, ErrBroken) {
		t.Fatalf("seal after a failure: %v", err)
	}
}

func TestDeviceInfoParsing(t *testing.T) {
	for v, want := range map[any]string{nil: "", "x": "x", 2.5: "2.5", true: "true"} {
		if got := str(v); got != want {
			t.Errorf("str(%v) = %q, want %q", v, got, want)
		}
	}
	info := &DeviceInfo{SwVersion: "keep"}
	fillInfo(info, &Message{})
	fillInfo(info, &Message{Data: []json.RawMessage{json.RawMessage(`[1]`)}})
	fillInfo(info, &Message{Data: []json.RawMessage{json.RawMessage(`{"haVersion": 1.332, "swVersion": ""}`)}})
	if info.SwVersion != "keep" || info.HaVersion != "1.332" {
		t.Errorf("info %+v", info)
	}
	fillNetwork(info, &Message{Data: []json.RawMessage{
		json.RawMessage(`"bad"`), json.RawMessage(`{"primary":true}`),
		json.RawMessage(`{"rssi":-70}`), json.RawMessage(`{"rssi":-50,"primary":true}`), json.RawMessage(`{"rssi":-90}`),
	}})
	if info.RSSI == nil || *info.RSSI != -50 {
		t.Errorf("rssi %v, want the primary interface's", info.RSSI)
	}
}

func TestSessionHelpers(t *testing.T) {
	if q := quoted(bytes.Repeat([]byte("a"), 400)); !strings.HasSuffix(q, `"...`) || len(q) > 310 {
		t.Errorf("long message quoted as %d bytes", len(q))
	}
	s := &session{services: map[string]int{"ro": 2, "ni": 0}}
	if s.version("/ro/values") != 2 || s.version("/ni/info") != 1 || s.version("/zz/x") != 1 {
		t.Error("service versions")
	}
	if !errors.Is(s.closedErr(), ErrSessionClosed) {
		t.Errorf("closedErr %v", s.closedErr())
	}
}

type lineLog struct{ lines []string }

func (l *lineLog) Debug(f string, a ...any) { l.lines = append(l.lines, "DEBUG "+fmt.Sprintf(f, a...)) }
func (l *lineLog) Info(f string, a ...any)  { l.lines = append(l.lines, "INFO "+fmt.Sprintf(f, a...)) }
func (l *lineLog) Warn(f string, a ...any)  { l.lines = append(l.lines, "WARN "+fmt.Sprintf(f, a...)) }
func (l *lineLog) Error(f string, a ...any) { l.lines = append(l.lines, "ERROR "+fmt.Sprintf(f, a...)) }

// A long episode is summarised once an hour, not logged per attempt.
func TestLinkSummaryHourly(t *testing.T) {
	log := &lineLog{}
	p, err := LoadProfile(testProfileDir, "")
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewClient(Options{Host: "192.0.2.1", DeviceID: "0a1b2c3d", Log: log}, p, func(Event) {})
	if err != nil {
		t.Fatal(err)
	}
	c.link = LinkStatus{State: LinkRefused, Since: time.Now().Add(-2 * time.Hour)}
	c.lastLogged = time.Now().Add(-2 * time.Hour)
	c.setLink(LinkRefused, errors.New("503"))
	c.setLink(LinkRefused, errors.New("503"))
	summaries := 0
	for _, l := range log.lines {
		if strings.Contains(l, "still refused") {
			summaries++
		}
	}
	if summaries != 1 {
		t.Fatalf("%d summaries in %v, want 1", summaries, log.lines)
	}
}
