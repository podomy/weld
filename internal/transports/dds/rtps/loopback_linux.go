// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build linux

package rtps

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
)

// enableMulticastLoopback turns IPv4 multicast loopback on or off for conn.
//
// ListenMulticastUDP disables loopback on the socket it returns, which is
// the receive socket here. Sends go out the unicast socket, and Linux
// defaults loopback on for fresh sockets, but v1 sets it explicitly so
// self reception never depends on an OS default.
func enableMulticastLoopback(conn *net.UDPConn, on bool) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("rtps: syscall conn: %w", err)
	}
	value := 0
	if on {
		value = socketOptOn
	}
	var opErr error
	ctrlErr := raw.Control(func(fd uintptr) {
		opErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MULTICAST_LOOP, value)
	})
	if ctrlErr != nil {
		return fmt.Errorf("rtps: multicast loopback control: %w", ctrlErr)
	}
	if opErr != nil {
		return fmt.Errorf("rtps: set multicast loopback: %w", opErr)
	}
	return nil
}

// soReusePort documents the Linux SO_REUSEPORT value for reference.
// It is intentionally not set on the metatraffic socket: see setReuseOptions.
//
//nolint:unused // value documents the deliberately unset REUSEPORT to avoid load-balancing, test helper mirrors it, and the Linux split keeps the reference in place
const soReusePort = 15

// setReuseOptions enables address reuse on fd.
//
// SO_REUSEADDR alone is deliberate: every group binder then receives every
// multicast datagram by fanout. SO_REUSEPORT must stay off: on Linux it
// load-balances inbound datagrams across same-host sockets, so two Weld
// participants on one domain would split discovery and DATA by flow hash
// and one side would starve while the other receives everything.
func setReuseOptions(fd uintptr, first *error) {
	err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, socketOptOn)
	if err != nil && *first == nil {
		*first = err
	}
}

// newReuseListenConfig returns a config that binds with reuse options.
func newReuseListenConfig() net.ListenConfig {
	return net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			var first error
			ctrlErr := c.Control(func(fd uintptr) {
				setReuseOptions(fd, &first)
			})
			if ctrlErr != nil {
				return fmt.Errorf("rtps: reuse socket control: %w", ctrlErr)
			}
			return first
		},
	}
}

// packetConnToUDP asserts a packet conn is UDP with cleanup on mismatch.
func packetConnToUDP(p net.PacketConn) (*net.UDPConn, error) {
	conn, ok := p.(*net.UDPConn)
	if !ok {
		cerr := p.Close()
		return nil, fmt.Errorf("rtps: reuse bind is not udp: %w", errors.Join(errors.New("rtps: not udp"), cerr))
	}
	return conn, nil
}

// joinWithCleanup joins the group, closing conn on failure.
func joinWithCleanup(conn *net.UDPConn, group net.IP) error {
	err := joinGroup(conn, group)
	if err != nil {
		cerr := conn.Close()
		return fmt.Errorf("rtps: reuse bind join: %w", errors.Join(err, cerr))
	}
	return nil
}

// listenMetaReuse binds the SPDP multicast group address with SO_REUSEADDR
// before bind, then joins the group. Binding the group address (not the
// wildcard) is what coexists with vendor DDS participants on the same
// host: a wildcard exclusive binder does not block a group bind, and two
// group binders share with full fanout so every participant sees every
// datagram.
func listenMetaReuse(ctx context.Context, port int, group net.IP) (*net.UDPConn, error) {
	lc := newReuseListenConfig()
	p, err := lc.ListenPacket(ctx, "udp4", net.JoinHostPort(group.String(), strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("rtps: reuse bind spdp multicast: %w", err)
	}
	conn, err := packetConnToUDP(p)
	if err != nil {
		return nil, err
	}
	err = joinWithCleanup(conn, group)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// joinGroup adds conn to the IPv4 multicast group on the default route.
func joinGroup(conn *net.UDPConn, group net.IP) error {
	g4 := group.To4()
	if g4 == nil {
		return fmt.Errorf("rtps: spdp group %q is not ipv4", group.String())
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("rtps: syscall conn: %w", err)
	}
	var opErr error
	ctrlErr := raw.Control(func(fd uintptr) {
		mreq := &syscall.IPMreqn{Multiaddr: [ipv4TailSize]byte{g4[0], g4[1], g4[2], g4[3]}}
		opErr = syscall.SetsockoptIPMreqn(int(fd), syscall.IPPROTO_IP, syscall.IP_ADD_MEMBERSHIP, mreq)
	})
	if ctrlErr != nil {
		return fmt.Errorf("rtps: join group control: %w", ctrlErr)
	}
	if opErr != nil {
		return fmt.Errorf("rtps: join spdp group: %w", opErr)
	}
	return nil
}
