// Package homeconnect speaks the local Home Connect WebSocket protocol of
// BSH appliances (Bosch, Siemens, Neff, ...): the AES-CBC + HMAC framing used
// on port 80, the session handshake, and the device description profile that
// maps numeric UIDs to feature names. It is read-only by construction: the
// session can only GET resources and answer the appliance's own handshake.
package homeconnect

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
)

// Role selects which side of the connection a Codec frames for. The two
// sides use different direction bytes in the MAC, so a client Codec opens
// what a device Codec seals and vice versa.
type Role int

const (
	RoleClient Role = iota // the bridge (an "app" to the appliance)
	RoleDevice             // the appliance; used by the test fake
)

const (
	dirClient byte = 0x45 // 'E': frames the client sends
	dirDevice byte = 0x43 // 'C': frames the appliance sends
	macLen         = 16
	blockLen       = aes.BlockSize
)

var (
	ErrShortFrame = errors.New("homeconnect: frame shorter than one block plus MAC")
	ErrUnaligned  = errors.New("homeconnect: frame length not a multiple of the block size")
	ErrMAC        = errors.New("homeconnect: frame MAC mismatch")
	ErrPadding    = errors.New("homeconnect: invalid padding")
	ErrBroken     = errors.New("homeconnect: codec unusable after an earlier framing error")
)

// Codec seals and opens the binary WebSocket frames of the AES variant
// (port 80). Both directions are stateful for the whole connection: the CBC
// chain continues from one message to the next, and every MAC covers the
// previous MAC of the same direction. A single lost, reordered or corrupted
// frame therefore desynchronises the connection for good; after any error
// the Codec refuses further use and the caller must reconnect.
//
// Seal and Open keep separate state, so one goroutine may Seal while
// another Opens. Each of them is not safe for concurrent use with itself:
// Seal must be serialised with the frame writes (so the chain order matches
// the wire order) and Open is called only from the read loop.
type Codec struct {
	enc, dec cipher.BlockMode
	mackey   []byte
	iv       []byte
	lastTx   [macLen]byte
	lastRx   [macLen]byte
	txDir    byte
	rxDir    byte
	rand     io.Reader
	txBroken bool // Seal side; touched only by Seal
	rxBroken bool // Open side; touched only by Open
}

// deriveKeys returns the AES and HMAC keys for a pre-shared key.
func deriveKeys(psk []byte) (enckey, mackey []byte) {
	return hmacSHA256(psk, []byte("ENC")), hmacSHA256(psk, []byte("MAC"))
}

func hmacSHA256(key, msg []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(msg)
	return h.Sum(nil)
}

// NewCodec returns a codec for one connection. psk is the appliance key and
// iv its 16-byte IV, both from the device profile.
func NewCodec(psk, iv []byte, role Role) (*Codec, error) {
	if len(psk) == 0 {
		return nil, errors.New("homeconnect: empty key")
	}
	if len(iv) != blockLen {
		return nil, fmt.Errorf("homeconnect: iv must be %d bytes, got %d", blockLen, len(iv))
	}
	enckey, mackey := deriveKeys(psk)
	block, err := aes.NewCipher(enckey)
	if err != nil {
		return nil, err
	}
	c := &Codec{
		enc:    cipher.NewCBCEncrypter(block, iv),
		dec:    cipher.NewCBCDecrypter(block, iv),
		mackey: mackey,
		iv:     append([]byte(nil), iv...),
		rand:   rand.Reader,
	}
	switch role {
	case RoleClient:
		c.txDir, c.rxDir = dirClient, dirDevice
	case RoleDevice:
		c.txDir, c.rxDir = dirDevice, dirClient
	default:
		return nil, fmt.Errorf("homeconnect: unknown role %d", role)
	}
	return c, nil
}

// Seal encrypts and authenticates one message and returns the frame to send
// as a binary WebSocket message.
func (c *Codec) Seal(msg []byte) ([]byte, error) {
	if c.txBroken {
		return nil, ErrBroken
	}
	// The padding is not PKCS#7: a zero byte, random filler, and the pad
	// length as the last byte. At least two bytes are always added, so a
	// message one byte short of a block boundary gets a full extra block.
	pad := blockLen - len(msg)%blockLen
	if pad == 1 {
		pad += blockLen
	}
	buf := make([]byte, len(msg)+pad, len(msg)+pad+macLen)
	copy(buf, msg)
	buf[len(msg)] = 0x00
	if _, err := io.ReadFull(c.rand, buf[len(msg)+1:len(buf)-1]); err != nil {
		c.txBroken = true
		return nil, fmt.Errorf("homeconnect: padding: %w", err)
	}
	buf[len(buf)-1] = byte(pad)
	c.enc.CryptBlocks(buf, buf)
	mac := c.mac(c.txDir, c.lastTx[:], buf)
	copy(c.lastTx[:], mac)
	return append(buf, mac...), nil
}

// Open authenticates and decrypts one received frame.
func (c *Codec) Open(frame []byte) ([]byte, error) {
	if c.rxBroken {
		return nil, ErrBroken
	}
	if len(frame) < blockLen+macLen {
		c.rxBroken = true
		return nil, ErrShortFrame
	}
	if len(frame)%blockLen != 0 {
		c.rxBroken = true
		return nil, ErrUnaligned
	}
	ct, got := frame[:len(frame)-macLen], frame[len(frame)-macLen:]
	if want := c.mac(c.rxDir, c.lastRx[:], ct); !hmac.Equal(got, want) {
		c.rxBroken = true
		return nil, ErrMAC
	}
	copy(c.lastRx[:], got)
	plain := make([]byte, len(ct))
	c.dec.CryptBlocks(plain, ct)
	pad := int(plain[len(plain)-1])
	if pad < 1 || pad > len(plain) {
		c.rxBroken = true
		return nil, ErrPadding
	}
	return plain[:len(plain)-pad], nil
}

// mac is HMAC-SHA256(mackey, iv || dir || previous MAC || ciphertext),
// truncated to 16 bytes. The IV here is always the session IV, not the CBC
// chaining value.
func (c *Codec) mac(dir byte, prev, ct []byte) []byte {
	h := hmac.New(sha256.New, c.mackey)
	h.Write(c.iv)
	h.Write([]byte{dir})
	h.Write(prev)
	h.Write(ct)
	return h.Sum(nil)[:macLen]
}
