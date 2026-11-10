package main

import (
	"net"
	"strconv"
	"sync"
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

// fakeBroker is a minimal MQTT 3.1.1 broker for tests that drive the real
// paho client: it accepts every client, acks QoS 1 publishes, records them
// in arrival order and keeps the retained state. It can drop its clients.
type fakeBroker struct {
	t  *testing.T
	ln net.Listener

	mu       sync.Mutex
	pubs     []pubRec
	retained map[string]string
	conns    map[net.Conn]bool
	connects int
	discons  int // clean DISCONNECTs received
}

func newFakeBroker(t *testing.T) *fakeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fb := &fakeBroker{t: t, ln: ln, retained: map[string]string{}, conns: map[net.Conn]bool{}}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			fb.mu.Lock()
			fb.conns[c] = true
			fb.mu.Unlock()
			go fb.serve(c)
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		fb.dropAll()
	})
	return fb
}

func (fb *fakeBroker) hostPort() (string, int) {
	host, port, _ := net.SplitHostPort(fb.ln.Addr().String())
	p, _ := strconv.Atoi(port)
	return host, p
}

func (fb *fakeBroker) serve(c net.Conn) {
	defer func() {
		c.Close()
		fb.mu.Lock()
		delete(fb.conns, c)
		fb.mu.Unlock()
	}()
	for {
		cp, err := packets.ReadPacket(c)
		if err != nil {
			return
		}
		var reply packets.ControlPacket
		switch p := cp.(type) {
		case *packets.ConnectPacket:
			fb.mu.Lock()
			fb.connects++
			fb.mu.Unlock()
			reply = packets.NewControlPacket(packets.Connack)
		case *packets.PublishPacket:
			fb.mu.Lock()
			fb.pubs = append(fb.pubs, pubRec{topic: p.TopicName, payload: string(p.Payload), qos: p.Qos, retained: p.Retain})
			if p.Retain {
				if len(p.Payload) == 0 {
					delete(fb.retained, p.TopicName)
				} else {
					fb.retained[p.TopicName] = string(p.Payload)
				}
			}
			fb.mu.Unlock()
			if p.Qos == 1 {
				ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
				ack.MessageID = p.MessageID
				reply = ack
			}
		case *packets.PingreqPacket:
			reply = packets.NewControlPacket(packets.Pingresp)
		case *packets.DisconnectPacket:
			fb.mu.Lock()
			fb.discons++
			fb.mu.Unlock()
			return
		}
		if reply != nil {
			if err := reply.Write(c); err != nil {
				return
			}
		}
	}
}

// dropAll cuts every client connection without a DISCONNECT, as a broker
// restart would.
func (fb *fakeBroker) dropAll() {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	for c := range fb.conns {
		c.Close()
	}
}

func (fb *fakeBroker) get(topic string) (string, bool) {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	v, ok := fb.retained[topic]
	return v, ok
}

// wipe deletes a retained topic, as a broker restart without persistence
// would.
func (fb *fakeBroker) wipe(topic string) {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	delete(fb.retained, topic)
}

func (fb *fakeBroker) published() []pubRec {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	return append([]pubRec(nil), fb.pubs...)
}

func (fb *fakeBroker) counts() (connects, discons int) {
	fb.mu.Lock()
	defer fb.mu.Unlock()
	return fb.connects, fb.discons
}
