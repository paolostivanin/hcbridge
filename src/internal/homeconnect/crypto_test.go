package homeconnect

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
)

// Golden values computed independently with the openssl CLI:
//
//	enckey = openssl dgst -sha256 -mac HMAC -macopt hexkey:<psk> <<< "ENC"
//	ct     = openssl enc -aes-256-cbc -nopad -K <enckey> -iv <chain>
//	mac    = HMAC(mackey, iv || 'E' || prevmac || ct)[:16]
//
// with psk = 00..1f, iv = a0..af and every random padding byte 0x5a.
const (
	goldenEncKey = "82d6e4e94325fe9c0d4f8cd7d4aae032fc9798062139d78df743092b0e174b01"
	goldenMacKey = "218031ac7e80cf358974a84bb6964f3b559e242fc203ca1ad11c8e48b788427c"
)

var goldenFrames = []struct{ msg, frame string }{
	{`{"x":1}`, "2430538e7d57cc624e5a76b5ceaf2d916ece6f25969539a826ae171b7c2ad744"},
	{`{"y":22}`, "ef9bc8e6c696fbfaaf5f2630fa79bab6ffd501335c90da0823173fa072b72bad"},
	{`0123456789abcdef`, "78888b52937f4a748fdf660f800eeeb64725d1f145c7a26fa7b68c5d28851491b4226256bfcaff66e7e4af09d8096531"},
}

func seq(start, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(start + i)
	}
	return b
}

type constReader byte

func (r constReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r)
	}
	return len(p), nil
}

func testPSK() []byte { return seq(0, 32) }
func testIV() []byte  { return seq(0xa0, 16) }

func pair(t *testing.T) (client, device *Codec) {
	t.Helper()
	var err error
	if client, err = NewCodec(testPSK(), testIV(), RoleClient); err != nil {
		t.Fatal(err)
	}
	if device, err = NewCodec(testPSK(), testIV(), RoleDevice); err != nil {
		t.Fatal(err)
	}
	return client, device
}

func TestDeriveKeysGolden(t *testing.T) {
	enc, mac := deriveKeys(testPSK())
	if got := hex.EncodeToString(enc); got != goldenEncKey {
		t.Errorf("enckey = %s, want %s", got, goldenEncKey)
	}
	if got := hex.EncodeToString(mac); got != goldenMacKey {
		t.Errorf("mackey = %s, want %s", got, goldenMacKey)
	}
}

// The client seals the golden messages in order (so CBC chaining and MAC
// chaining are both exercised) and must produce exactly openssl's frames.
func TestSealGolden(t *testing.T) {
	c, err := NewCodec(testPSK(), testIV(), RoleClient)
	if err != nil {
		t.Fatal(err)
	}
	c.rand = constReader(0x5a)
	for i, g := range goldenFrames {
		frame, err := c.Seal([]byte(g.msg))
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(frame); got != g.frame {
			t.Fatalf("frame %d = %s\nwant      %s", i, got, g.frame)
		}
	}
}

// A device-role codec opens the golden client frames.
func TestOpenGolden(t *testing.T) {
	d, err := NewCodec(testPSK(), testIV(), RoleDevice)
	if err != nil {
		t.Fatal(err)
	}
	for i, g := range goldenFrames {
		frame, _ := hex.DecodeString(g.frame)
		msg, err := d.Open(frame)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if string(msg) != g.msg {
			t.Fatalf("frame %d = %q, want %q", i, msg, g.msg)
		}
	}
}

// Every message length across several block boundaries, interleaved in both
// directions on one connection.
func TestRoundTripBothDirections(t *testing.T) {
	client, device := pair(t)
	for n := 0; n <= 80; n++ {
		msg := bytes.Repeat([]byte{byte('a' + n%26)}, n)
		frame, err := client.Seal(msg)
		if err != nil {
			t.Fatal(err)
		}
		if len(frame)%blockLen != 0 || len(frame) < len(msg)+2+macLen {
			t.Fatalf("len %d: bad frame length %d", n, len(frame))
		}
		got, err := device.Open(frame)
		if err != nil || !bytes.Equal(got, msg) {
			t.Fatalf("client->device len %d: %q, %v", n, got, err)
		}

		reply := []byte(fmt.Sprintf(`{"n":%d}`, n))
		frame, err = device.Seal(reply)
		if err != nil {
			t.Fatal(err)
		}
		got, err = client.Open(frame)
		if err != nil || !bytes.Equal(got, reply) {
			t.Fatalf("device->client len %d: %q, %v", n, got, err)
		}
	}
}

func TestPaddingLengths(t *testing.T) {
	cases := map[int]int{0: 16, 1: 16, 14: 16, 15: 32, 16: 32, 17: 32, 30: 32, 31: 48}
	for n, want := range cases {
		c, _ := pair(t)
		frame, err := c.Seal(make([]byte, n))
		if err != nil {
			t.Fatal(err)
		}
		if got := len(frame) - macLen; got != want {
			t.Errorf("message of %d bytes: ciphertext %d bytes, want %d", n, got, want)
		}
	}
}

// A device frame opened by another device codec has the wrong direction
// byte in its MAC.
func TestDirectionMatters(t *testing.T) {
	_, device := pair(t)
	other, _ := NewCodec(testPSK(), testIV(), RoleDevice)
	frame, _ := device.Seal([]byte("hello"))
	if _, err := other.Open(frame); !errors.Is(err, ErrMAC) {
		t.Fatalf("got %v, want ErrMAC", err)
	}
}

func TestTamperBreaksCodec(t *testing.T) {
	for _, pos := range []string{"ciphertext", "mac"} {
		client, device := pair(t)
		frame, _ := client.Seal([]byte(`{"a":1}`))
		if pos == "ciphertext" {
			frame[0] ^= 1
		} else {
			frame[len(frame)-1] ^= 1
		}
		if _, err := device.Open(frame); !errors.Is(err, ErrMAC) {
			t.Fatalf("%s: got %v, want ErrMAC", pos, err)
		}
		// Even a genuine next frame is refused: the chains are gone.
		next, _ := client.Seal([]byte(`{"a":2}`))
		if _, err := device.Open(next); !errors.Is(err, ErrBroken) {
			t.Fatalf("%s: after tamper got %v, want ErrBroken", pos, err)
		}
	}
}

func TestReplayAndReorderRejected(t *testing.T) {
	client, device := pair(t)
	client.Seal([]byte("one")) // sent but lost
	f2, _ := client.Seal([]byte("two"))
	if _, err := device.Open(f2); !errors.Is(err, ErrMAC) {
		t.Fatalf("out of order: got %v, want ErrMAC", err)
	}

	client, device = pair(t)
	f1, _ := client.Seal([]byte("one"))
	if _, err := device.Open(f1); err != nil {
		t.Fatal(err)
	}
	if _, err := device.Open(f1); !errors.Is(err, ErrMAC) {
		t.Fatalf("replay: got %v, want ErrMAC", err)
	}
}

func TestMalformedFrames(t *testing.T) {
	_, device := pair(t)
	if _, err := device.Open(make([]byte, 16)); !errors.Is(err, ErrShortFrame) {
		t.Errorf("16 bytes: got %v", err)
	}
	_, device = pair(t)
	if _, err := device.Open(make([]byte, 40)); !errors.Is(err, ErrUnaligned) {
		t.Errorf("40 bytes: got %v", err)
	}
}

// A frame with a valid MAC but an impossible pad length (built by sealing a
// block whose last byte is 0 without the codec's padding).
func TestBadPadding(t *testing.T) {
	client, device := pair(t)
	block := make([]byte, blockLen) // last byte 0: pad length 0 is invalid
	client.enc.CryptBlocks(block, block)
	mac := client.mac(client.txDir, client.lastTx[:], block)
	if _, err := device.Open(append(block, mac...)); !errors.Is(err, ErrPadding) {
		t.Fatalf("got %v, want ErrPadding", err)
	}
}

func TestNewCodecValidation(t *testing.T) {
	if _, err := NewCodec(nil, testIV(), RoleClient); err == nil {
		t.Error("empty key accepted")
	}
	if _, err := NewCodec(testPSK(), make([]byte, 8), RoleClient); err == nil {
		t.Error("short iv accepted")
	}
	if _, err := NewCodec(testPSK(), testIV(), Role(7)); err == nil {
		t.Error("unknown role accepted")
	}
}
