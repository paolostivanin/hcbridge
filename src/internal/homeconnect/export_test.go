package homeconnect

import (
	"context"
	"net"
)

// Test hooks for the external test package.

func SetJitter(o *Options, f func() float64) { o.jitter = f }

func SetNetDial(o *Options, f func(ctx context.Context, network, addr string) (net.Conn, error)) {
	o.netDial = f
}
