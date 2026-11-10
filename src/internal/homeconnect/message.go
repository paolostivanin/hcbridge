package homeconnect

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Action is the verb of a protocol message.
type Action string

const (
	ActionGet      Action = "GET"
	ActionPost     Action = "POST"
	ActionResponse Action = "RESPONSE"
	ActionNotify   Action = "NOTIFY"
)

// outMessage is what the bridge sends. The field order is the wire order:
// the appliance firmware is picky enough about JSON layout (see
// initValuesReply) that the struct, not a map, defines what goes out.
type outMessage struct {
	SID      int64  `json:"sID"`
	MsgID    int64  `json:"msgID"`
	Resource string `json:"resource"`
	Version  int    `json:"version"`
	Action   Action `json:"action"`
	Data     any    `json:"data,omitempty"`
}

// initValuesReply answers the appliance's POST /ei/initialValues. The key
// order is load-bearing: some firmware rejects the next request with 403
// when deviceID does not come last (go-homeconnect2mqtt issue #64).
type initValuesReply struct {
	DeviceType any    `json:"deviceType"` // "Application", or 2 for ei version 1
	DeviceName string `json:"deviceName"`
	DeviceID   string `json:"deviceID"`
}

// Message is a received protocol message. Data is always a list on the
// wire; a bare object is accepted as a one-element list.
type Message struct {
	SID      int64
	MsgID    int64
	Resource string
	Version  int
	Action   Action
	Data     []json.RawMessage
	Code     *int // non-nil: the request this answers failed
}

// Service returns the service a resource belongs to: "ci" for "/ci/info".
func (m *Message) Service() string { return serviceOf(m.Resource) }

func serviceOf(resource string) string {
	if len(resource) >= 3 && resource[0] == '/' {
		if i := strings.IndexByte(resource[1:], '/'); i > 0 {
			return resource[1 : 1+i]
		}
		return resource[1:]
	}
	return ""
}

// flexInt accepts 5, 5.0, "5" and null (as 0). Appliances have been seen
// sending sID, msgID and version as strings.
type flexInt int64

func (f *flexInt) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		*f = 0
		return nil
	}
	s := string(b)
	if b[0] == '"' {
		var err error
		if s, err = strconv.Unquote(s); err != nil {
			return err
		}
		s = strings.TrimSpace(s)
		if s == "" {
			*f = 0
			return nil
		}
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		*f = flexInt(n)
		return nil
	}
	x, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("not an integer: %s", b)
	}
	*f = flexInt(int64(x))
	return nil
}

type wireMessage struct {
	SID      flexInt         `json:"sID"`
	MsgID    flexInt         `json:"msgID"`
	Resource string          `json:"resource"`
	Version  flexInt         `json:"version"`
	Action   Action          `json:"action"`
	Data     json.RawMessage `json:"data"`
	Code     *flexInt        `json:"code"`
}

// parseMessage decodes one plaintext frame.
func parseMessage(b []byte) (*Message, error) {
	var w wireMessage
	if err := json.Unmarshal(b, &w); err != nil {
		return nil, err
	}
	if w.Resource == "" || w.Action == "" {
		return nil, fmt.Errorf("message without resource or action")
	}
	m := &Message{
		SID:      int64(w.SID),
		MsgID:    int64(w.MsgID),
		Resource: w.Resource,
		Version:  int(w.Version),
		Action:   Action(strings.ToUpper(string(w.Action))),
	}
	if w.Code != nil {
		c := int(*w.Code)
		m.Code = &c
	}
	data := bytes.TrimSpace(w.Data)
	switch {
	case len(data) == 0 || bytes.Equal(data, []byte("null")):
	case data[0] == '[':
		if err := json.Unmarshal(data, &m.Data); err != nil {
			return nil, fmt.Errorf("data: %w", err)
		}
	case data[0] == '{':
		m.Data = []json.RawMessage{data}
	default:
		return nil, fmt.Errorf("data is neither a list nor an object")
	}
	return m, nil
}

// uidValue is one entry of /ro/values and /ro/allMandatoryValues.
type uidValue struct {
	UID   flexInt         `json:"uid"`
	Value json.RawMessage `json:"value"`
}

// wireDescription is one entry of /ro/descriptionChange and
// /ro/allDescriptionChanges.
type wireDescription struct {
	UID       flexInt         `json:"uid"`
	ParentUID flexInt         `json:"parentUID"`
	Access    string          `json:"access"`
	Available *bool           `json:"available"`
	Value     json.RawMessage `json:"value"`
	Min       json.RawMessage `json:"min"`
	Max       json.RawMessage `json:"max"`
}

// decodeList decodes every element of m.Data into T, skipping (and
// counting) elements that do not decode instead of failing the message.
func decodeList[T any](data []json.RawMessage) (out []T, bad int) {
	out = make([]T, 0, len(data))
	for _, raw := range data {
		var v T
		if err := json.Unmarshal(raw, &v); err != nil {
			bad++
			continue
		}
		out = append(out, v)
	}
	return out, bad
}
