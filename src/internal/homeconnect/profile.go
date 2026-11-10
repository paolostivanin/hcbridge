package homeconnect

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Profile is one appliance's local-access profile: the key material from
// the account's encryption information and the device description that
// maps the numeric UIDs on the wire to feature names and enum values.
//
// It is produced by homeconnect-profile-downloader as a ZIP holding
// <haId>.json plus the *_DeviceDescription.xml and *_FeatureMapping.xml the
// Home Connect backend serves for the appliance.
type Profile struct {
	HaID           string
	Type           string
	Brand          string
	VIB            string
	Serial         string
	MAC            string
	Created        string
	ConnectionType string
	DDFVersion     string // device description version-revision, e.g. "2-6"
	Source         string // file the profile was loaded from

	key, iv []byte // never printed (see String)

	features map[int]*Feature
	byName   map[string]int
	enums    map[int]*Enum
}

// Feature is one UID of the device description.
type Feature struct {
	UID       int
	Name      string   // from the feature mapping; "" if unmapped
	Kind      string   // XML element: status, setting, event, command, option, program, ...
	CID       int      // content type (refCID)
	EnumID    int      // enumerationType, 0 if none
	Access    string   // lower case: read, readwrite, writeonly, ...
	Available *bool    // nil if not stated
	Min, Max  *float64 // nil if not stated
}

// Enum is one enumeration type of the feature mapping.
type Enum struct {
	ID      int
	Key     string
	Members map[int]string
}

// Value is a received UID value resolved against the profile.
type Value struct {
	UID    int
	Name   string          // feature name; "" if the UID is not in the profile
	Raw    json.RawMessage // as received
	V      any             // JSON-decoded: bool, float64, string, nil, map, slice
	Symbol string          // enum member name, or the name of the program an ActiveProgram/SelectedProgram value points to
}

// Known reports whether the UID has a name in the profile.
func (v Value) Known() bool { return v.Name != "" }

// Display returns Symbol when there is one, else the decoded value.
func (v Value) Display() any {
	if v.Symbol != "" {
		return v.Symbol
	}
	return v.V
}

const (
	cidUIDValue      = 21 // content type whose value is another UID
	maxProfileFiles  = 256
	maxProfileFileSz = 16 << 20
)

// String keeps the key out of %v and %+v output.
func (p Profile) String() string {
	return fmt.Sprintf("profile %s (%s %s %s, %s)", p.HaID, p.Brand, p.VIB, p.Type, p.ConnectionType)
}

// GoString keeps the key out of %#v output.
func (p Profile) GoString() string { return p.String() }

// Keys returns the pre-shared key and IV.
func (p *Profile) Keys() (psk, iv []byte) { return p.key, p.iv }

// Feature looks a UID up.
func (p *Profile) Feature(uid int) (*Feature, bool) {
	f, ok := p.features[uid]
	return f, ok
}

// UID looks a feature name up.
func (p *Profile) UID(name string) (int, bool) {
	uid, ok := p.byName[name]
	return uid, ok
}

// Features returns all named features sorted by name.
func (p *Profile) Features() []*Feature {
	out := make([]*Feature, 0, len(p.byName))
	for _, uid := range p.byName {
		out = append(out, p.features[uid])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// EnumCount returns how many enumeration types the profile defines.
func (p *Profile) EnumCount() int { return len(p.enums) }

// Decode resolves a received value. It never fails: unknown UIDs and
// values that do not parse come back with Name or V empty and Raw set.
func (p *Profile) Decode(uid int, raw json.RawMessage) Value {
	v := Value{UID: uid, Raw: raw}
	if len(raw) > 0 {
		var x any
		if json.Unmarshal(raw, &x) == nil {
			v.V = x
		}
	}
	f, ok := p.features[uid]
	if !ok {
		return v
	}
	v.Name = f.Name
	n, isNum := v.V.(float64)
	if !isNum {
		return v
	}
	switch {
	case f.EnumID != 0:
		if e, ok := p.enums[f.EnumID]; ok {
			v.Symbol = e.Members[int(n)]
		}
	case f.CID == cidUIDValue || isProgramRef(f.Name):
		if n == 0 {
			v.V = nil // no program
		} else if pf, ok := p.features[int(n)]; ok {
			v.Symbol = pf.Name
		}
	}
	return v
}

func isProgramRef(name string) bool {
	return strings.HasSuffix(name, ".ActiveProgram") || strings.HasSuffix(name, ".SelectedProgram") ||
		name == "ActiveProgram" || name == "SelectedProgram"
}

// profileMeta is <haId>.json as written by homeconnect-profile-downloader.
type profileMeta struct {
	HaID                      string `json:"haId"`
	Type                      string `json:"type"`
	SerialNumber              string `json:"serialNumber"`
	Brand                     string `json:"brand"`
	VIB                       string `json:"vib"`
	MAC                       string `json:"mac"`
	FeatureMappingFileName    string `json:"featureMappingFileName"`
	DeviceDescriptionFileName string `json:"deviceDescriptionFileName"`
	Created                   string `json:"created"`
	ConnectionType            string `json:"connectionType"`
	Key                       string `json:"key"`
	IV                        string `json:"iv"`
}

type profileFile struct {
	name string // path inside the ZIP or relative to the directory
	data []byte
}

// LoadProfile reads a profile from a downloader ZIP or from a directory
// holding the extracted files. wantHaID, if set, selects the appliance when
// several profiles are present and must match otherwise.
func LoadProfile(p, wantHaID string) (*Profile, error) {
	st, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	var files []profileFile
	if st.IsDir() {
		files, err = readProfileDir(p)
	} else {
		files, err = readProfileZip(p)
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}

	var metas []profileFile
	var haIDs []string
	for _, f := range files {
		if !strings.EqualFold(path.Ext(f.name), ".json") {
			continue
		}
		var m profileMeta
		if json.Unmarshal(f.data, &m) != nil || m.HaID == "" || m.Key == "" {
			continue
		}
		haIDs = append(haIDs, m.HaID)
		if wantHaID == "" || m.HaID == wantHaID {
			metas = append(metas, f)
		}
	}
	switch {
	case len(metas) == 0 && len(haIDs) == 0:
		return nil, fmt.Errorf("%s: no profile JSON with haId and key found", p)
	case len(metas) == 0:
		return nil, fmt.Errorf("%s: no profile for ha_id %q (found %s)", p, wantHaID, strings.Join(haIDs, ", "))
	case len(metas) > 1:
		return nil, fmt.Errorf("%s: %d profiles found (%s); set appliance.ha_id to pick one", p, len(metas), strings.Join(haIDs, ", "))
	}
	var meta profileMeta
	_ = json.Unmarshal(metas[0].data, &meta)

	dd, err := findXML(files, meta.DeviceDescriptionFileName, "_DeviceDescription.xml", meta.HaID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	fm, err := findXML(files, meta.FeatureMappingFileName, "_FeatureMapping.xml", meta.HaID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	prof, err := ParseProfile(metas[0].data, bytes.NewReader(dd), bytes.NewReader(fm))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	prof.Source = p
	return prof, nil
}

func readProfileDir(dir string) ([]profileFile, error) {
	var files []profileFile
	root := filepath.Clean(dir)
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		if d.IsDir() {
			if rel != "." && strings.Count(rel, string(filepath.Separator)) >= 2 {
				return fs.SkipDir
			}
			return nil
		}
		if !wantedProfileFile(p) {
			return nil
		}
		if len(files) >= maxProfileFiles {
			return errors.New("too many files")
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.Size() > maxProfileFileSz {
			return fmt.Errorf("%s: file too large", rel)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		files = append(files, profileFile{name: filepath.ToSlash(rel), data: b})
		return nil
	})
	return files, err
}

func readProfileZip(file string) ([]profileFile, error) {
	zr, err := zip.OpenReader(file)
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	if len(zr.File) > maxProfileFiles {
		return nil, errors.New("too many entries in ZIP")
	}
	var files []profileFile
	for _, zf := range zr.File {
		if zf.FileInfo().IsDir() || !wantedProfileFile(zf.Name) {
			continue
		}
		if zf.UncompressedSize64 > maxProfileFileSz {
			return nil, fmt.Errorf("%s: entry too large", zf.Name)
		}
		rc, err := zf.Open()
		if err != nil {
			return nil, err
		}
		b, err := io.ReadAll(io.LimitReader(rc, maxProfileFileSz+1))
		rc.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", zf.Name, err)
		}
		if len(b) > maxProfileFileSz {
			return nil, fmt.Errorf("%s: entry too large", zf.Name)
		}
		files = append(files, profileFile{name: zf.Name, data: b})
	}
	return files, nil
}

func wantedProfileFile(name string) bool {
	ext := strings.ToLower(path.Ext(filepath.ToSlash(name)))
	return ext == ".json" || ext == ".xml"
}

// findXML picks the declared file by base name, else the single file with
// the given suffix (preferring one that carries the haId).
func findXML(files []profileFile, declared, suffix, haID string) ([]byte, error) {
	if declared != "" {
		for _, f := range files {
			if path.Base(f.name) == declared {
				return f.data, nil
			}
		}
	}
	var cands []profileFile
	for _, f := range files {
		if strings.HasSuffix(path.Base(f.name), suffix) {
			cands = append(cands, f)
		}
	}
	if len(cands) > 1 {
		var withID []profileFile
		for _, f := range cands {
			if strings.Contains(path.Base(f.name), haID) {
				withID = append(withID, f)
			}
		}
		cands = withID
	}
	switch len(cands) {
	case 1:
		return cands[0].data, nil
	case 0:
		return nil, fmt.Errorf("no *%s found", suffix)
	default:
		return nil, fmt.Errorf("several *%s files; cannot tell which belongs to %s", suffix, haID)
	}
}

// ParseProfile builds a profile from the downloader JSON and the two XML
// documents.
func ParseProfile(meta []byte, deviceDescription, featureMapping io.Reader) (*Profile, error) {
	var m profileMeta
	if err := json.Unmarshal(meta, &m); err != nil {
		return nil, fmt.Errorf("profile JSON: %w", err)
	}
	if m.HaID == "" {
		return nil, errors.New("profile JSON has no haId")
	}
	ct := strings.ToUpper(strings.TrimSpace(m.ConnectionType))
	if ct == "" && m.IV != "" {
		ct = "AES"
	}
	switch ct {
	case "AES":
	case "TLS", "":
		return nil, errors.New("connectionType TLS (port 443, TLS-PSK) is not supported; only AES (port 80) profiles work")
	default:
		return nil, fmt.Errorf("unknown connectionType %q", m.ConnectionType)
	}
	key, err := decodeB64URL(m.Key)
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("profile key must be 32 bytes of base64url (got %d bytes, err %v)", len(key), err)
	}
	iv, err := decodeB64URL(m.IV)
	if err != nil || len(iv) != blockLen {
		return nil, fmt.Errorf("profile iv must be %d bytes of base64url (got %d bytes, err %v)", blockLen, len(iv), err)
	}

	p := &Profile{
		HaID: m.HaID, Type: m.Type, Brand: m.Brand, VIB: m.VIB, Serial: m.SerialNumber,
		MAC: m.MAC, Created: m.Created, ConnectionType: ct,
		key: key, iv: iv,
		features: map[int]*Feature{}, byName: map[string]int{}, enums: map[int]*Enum{},
	}
	names, err := p.parseFeatureMapping(featureMapping)
	if err != nil {
		return nil, fmt.Errorf("feature mapping: %w", err)
	}
	subsets, err := p.parseDeviceDescription(deviceDescription)
	if err != nil {
		return nil, fmt.Errorf("device description: %w", err)
	}
	for uid, name := range names {
		f, ok := p.features[uid]
		if !ok {
			f = &Feature{UID: uid}
			p.features[uid] = f
		}
		f.Name = name
		p.byName[name] = uid
	}
	// An enumerationType declared as a subset of another, with no names of
	// its own, borrows the names of its superset for its values.
	for id, s := range subsets {
		if _, ok := p.enums[id]; ok {
			continue
		}
		super, ok := p.enums[s.of]
		if !ok {
			continue
		}
		e := &Enum{ID: id, Key: super.Key, Members: map[int]string{}}
		for _, v := range s.values {
			if name, ok := super.Members[v]; ok {
				e.Members[v] = name
			}
		}
		p.enums[id] = e
	}
	return p, nil
}

func decodeB64URL(s string) ([]byte, error) {
	s = strings.TrimRight(strings.TrimSpace(s), "=")
	s = strings.NewReplacer("+", "-", "/", "_").Replace(s)
	return base64.RawURLEncoding.DecodeString(s)
}

func parseHex(s string) (int, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	n, err := strconv.ParseInt(s, 16, 64)
	return int(n), err
}

func newXMLDecoder(r io.Reader) *xml.Decoder {
	d := xml.NewDecoder(r)
	d.Strict = false
	// The documents are ASCII in practice; accept any declared charset
	// rather than failing on e.g. ISO-8859-1.
	d.CharsetReader = func(_ string, in io.Reader) (io.Reader, error) { return in, nil }
	return d
}

func attr(se xml.StartElement, name string) (string, bool) {
	for _, a := range se.Attr {
		if a.Name.Local == name {
			return a.Value, true
		}
	}
	return "", false
}

// parseFeatureMapping returns the UID names and fills p.enums.
func (p *Profile) parseFeatureMapping(r io.Reader) (map[int]string, error) {
	names := map[int]string{}
	d := newXMLDecoder(r)
	var cur *Enum
	for {
		tok, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			if ee, ok := tok.(xml.EndElement); ok && ee.Name.Local == "enumDescription" {
				cur = nil
			}
			continue
		}
		switch se.Name.Local {
		case "feature":
			ref, _ := attr(se, "refUID")
			uid, err := parseHex(ref)
			if err != nil {
				return nil, fmt.Errorf("feature refUID %q: %w", ref, err)
			}
			text, err := elementText(d)
			if err != nil {
				return nil, err
			}
			names[uid] = text
		case "enumDescription":
			ref, _ := attr(se, "refENID")
			id, err := parseHex(ref)
			if err != nil {
				return nil, fmt.Errorf("enumDescription refENID %q: %w", ref, err)
			}
			key, _ := attr(se, "enumKey")
			cur = &Enum{ID: id, Key: key, Members: map[int]string{}}
			p.enums[id] = cur
		case "enumMember":
			ref, _ := attr(se, "refValue")
			v, err := strconv.Atoi(strings.TrimSpace(ref))
			if err != nil {
				return nil, fmt.Errorf("enumMember refValue %q: %w", ref, err)
			}
			text, err := elementText(d)
			if err != nil {
				return nil, err
			}
			if cur != nil {
				cur.Members[v] = text
			}
		}
	}
	if len(names) == 0 {
		return nil, errors.New("no features")
	}
	return names, nil
}

// elementText reads the character data of the element just opened, up to
// its end tag.
func elementText(d *xml.Decoder) (string, error) {
	var sb strings.Builder
	depth := 1
	for depth > 0 {
		tok, err := d.Token()
		if err != nil {
			return "", err
		}
		switch t := tok.(type) {
		case xml.CharData:
			sb.Write(t)
		case xml.StartElement:
			depth++
		case xml.EndElement:
			depth--
		}
	}
	return strings.TrimSpace(sb.String()), nil
}

type enumSubset struct {
	of     int
	values []int
}

// parseDeviceDescription fills p.features from every element carrying a
// uid, and returns the enumeration subsets it declares. Attributes it does
// not need (initValue, default, ... which some hobs fill with malformed
// JSON) are never interpreted.
func (p *Profile) parseDeviceDescription(r io.Reader) (map[int]enumSubset, error) {
	subsets := map[int]enumSubset{}
	d := newXMLDecoder(r)
	var curSubset = -1
	var inDescription bool
	var descField string
	var version, revision string
	for {
		tok, err := d.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			name := t.Name.Local
			switch {
			case name == "description":
				inDescription = true
				continue
			case inDescription:
				descField = name
				continue
			case name == "enumerationType":
				curSubset = -1
				enid, _ := attr(t, "enid")
				of, hasOf := attr(t, "subsetOf")
				if !hasOf {
					continue
				}
				id, err1 := parseHex(enid)
				sup, err2 := parseHex(of)
				if err1 == nil && err2 == nil {
					subsets[id] = enumSubset{of: sup}
					curSubset = id
				}
				continue
			case name == "enumeration":
				if curSubset >= 0 {
					if s, ok := attr(t, "value"); ok {
						if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil {
							sub := subsets[curSubset]
							sub.values = append(sub.values, v)
							subsets[curSubset] = sub
						}
					}
				}
				continue
			}
			uidStr, ok := attr(t, "uid")
			if !ok {
				continue
			}
			uid, err := parseHex(uidStr)
			if err != nil {
				return nil, fmt.Errorf("<%s uid=%q>: %w", name, uidStr, err)
			}
			f := &Feature{UID: uid, Kind: name}
			for _, a := range t.Attr {
				switch a.Name.Local {
				case "refCID":
					f.CID, _ = parseHex(a.Value)
				case "enumerationType":
					f.EnumID, _ = parseHex(a.Value)
				case "access":
					f.Access = strings.ToLower(a.Value)
				case "available":
					b := strings.EqualFold(a.Value, "true")
					f.Available = &b
				case "min":
					f.Min = parseFloatPtr(a.Value)
				case "max":
					f.Max = parseFloatPtr(a.Value)
				}
			}
			if prev, ok := p.features[uid]; ok && prev.Kind != "" {
				continue // first declaration wins
			}
			p.features[uid] = f
		case xml.CharData:
			if inDescription {
				switch descField {
				case "version":
					version = strings.TrimSpace(string(t))
				case "revision":
					revision = strings.TrimSpace(string(t))
				}
			}
		case xml.EndElement:
			switch t.Name.Local {
			case "description":
				inDescription = false
			case "enumerationType":
				curSubset = -1
			}
			descField = ""
		}
	}
	if version != "" {
		p.DDFVersion = version
		if revision != "" {
			p.DDFVersion += "-" + revision
		}
	}
	return subsets, nil
}

func parseFloatPtr(s string) *float64 {
	x, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return nil
	}
	return &x
}
