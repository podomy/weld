// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !linux

package rtps

import (
	"context"
	"errors"
	"net"
)

// enableMulticastLoopback reports unsupported outside Linux. The v1
// transport targets localhost on Linux; other platforms need a socket
// option path of their own before multicast loopback can be enabled.
func enableMulticastLoopback(_ *net.UDPConn, _ bool) error {
	return errors.New("rtps: multicast loopback not supported on this platform") //nolint:wrapcheck // new error, nothing to wrap
}

// listenMetaReuse reports unavailable outside Linux, where callers fall
// back to ListenMulticastUDP.
func listenMetaReuse(_ context.Context, _ int, _ net.IP) (*net.UDPConn, error) {
	return nil, errReuseUnavailable
}
