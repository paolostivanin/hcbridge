package homeconnect

import (
	"encoding/json"
	"testing"
)

func TestOutMessageBytes(t *testing.T) {
	b, err := json.Marshal(outMessage{SID: 7, MsgID: 42, Resource: "/ci/services", Version: 1, Action: ActionGet})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"sID":7,"msgID":42,"resource":"/ci/services","version":1,"action":"GET"}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
}

// The initialValues reply must keep deviceType, deviceName, deviceID in
// exactly this order.
func TestInitValuesReplyKeyOrder(t *testing.T) {
	for _, tc := range []struct {
		deviceType any
		want       string
	}{
		{"Application", `[{"deviceType":"Application","deviceName":"hcbridge","deviceID":"0a1b2c3d"}]`},
		{2, `[{"deviceType":2,"deviceName":"hcbridge","deviceID":"0a1b2c3d"}]`},
	} {
		b, err := json.Marshal([]initValuesReply{{DeviceType: tc.deviceType, DeviceName: "hcbridge", DeviceID: "0a1b2c3d"}})
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != tc.want {
			t.Errorf("got  %s\nwant %s", b, tc.want)
		}
	}
}

func TestParseMessageLenient(t *testing.T) {
	m, err := parseMessage([]byte(`{"sID":"1975365718","msgID":"2148567885","resource":"/ei/initialValues","version":"2","action":"post","data":[{"edMsgID":12}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if m.SID != 1975365718 || m.MsgID != 2148567885 || m.Version != 2 || m.Action != ActionPost {
		t.Fatalf("got %+v", m)
	}
	if len(m.Data) != 1 || m.Service() != "ei" {
		t.Fatalf("data %d, service %q", len(m.Data), m.Service())
	}

	// data as a bare object, code present
	m, err = parseMessage([]byte(`{"sID":1,"msgID":2,"resource":"/ci/info","version":2,"action":"RESPONSE","code":400,"data":{"a":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	if m.Code == nil || *m.Code != 400 || len(m.Data) != 1 {
		t.Fatalf("got %+v", m)
	}

	// no data, null code
	m, err = parseMessage([]byte(`{"sID":1,"msgID":2,"resource":"/ro/values","version":1,"action":"RESPONSE","code":null}`))
	if err != nil {
		t.Fatal(err)
	}
	if m.Code != nil || m.Data != nil {
		t.Fatalf("got %+v", m)
	}
}

func TestParseMessageRejects(t *testing.T) {
	for _, in := range []string{
		`not json`,
		`{"sID":1,"msgID":2,"version":1,"action":"GET"}`, // no resource
		`{"sID":1,"msgID":2,"resource":"/ro/values","version":1,"action":"NOTIFY","data":"x"}`,
		`{"sID":"abc","msgID":2,"resource":"/ro/values","version":1,"action":"NOTIFY"}`,
	} {
		if _, err := parseMessage([]byte(in)); err == nil {
			t.Errorf("accepted %s", in)
		}
	}
}

func TestServiceOf(t *testing.T) {
	for in, want := range map[string]string{
		"/ci/services": "ci", "/ro/allMandatoryValues": "ro", "/ei": "ei", "": "", "x/y": "",
	} {
		if got := serviceOf(in); got != want {
			t.Errorf("serviceOf(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDecodeListSkipsBadEntries(t *testing.T) {
	m, err := parseMessage([]byte(`{"sID":1,"msgID":2,"resource":"/ro/values","version":1,"action":"NOTIFY","data":[{"uid":4409,"value":18},{"uid":"x"},{"uid":"539","value":true}]}`))
	if err != nil {
		t.Fatal(err)
	}
	vals, bad := decodeList[uidValue](m.Data)
	if bad != 1 || len(vals) != 2 || vals[0].UID != 4409 || vals[1].UID != 539 || string(vals[1].Value) != "true" {
		t.Fatalf("vals %+v bad %d", vals, bad)
	}
}
