// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package rtps_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// closeTransport registers cleanup that fails the test on close errors.
func closeTransport(t *testing.T, tr *rtps.Transport) {
	t.Helper()

	t.Cleanup(func() {
		if err := tr.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
}

// TestPortMappingTable pins standard DDSI-RTPS 9.6.1 ports per domain.
func TestPortMappingTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		domain        uint32
		participant   uint32
		metaMulticast int
		metaUnicast   int
		userMulticast int
		userUnicast   int
	}{
		{domain: 0, participant: 0, metaMulticast: 7400, metaUnicast: 7410, userMulticast: 7401, userUnicast: 7411},
		{domain: 0, participant: 1, metaMulticast: 7400, metaUnicast: 7412, userMulticast: 7401, userUnicast: 7413},
		{domain: 1, participant: 0, metaMulticast: 7650, metaUnicast: 7660, userMulticast: 7651, userUnicast: 7661},
		{domain: 42, participant: 3, metaMulticast: 17900, metaUnicast: 17916, userMulticast: 17901, userUnicast: 17917},
		{domain: 101, participant: 0, metaMulticast: 32650, metaUnicast: 32660, userMulticast: 32651, userUnicast: 32661},
	}

	for _, tc := range cases {
		metaMc, err := rtps.MetatrafficMulticastPort(tc.domain)
		if err != nil {
			t.Fatalf("domain %d meta multicast: %v", tc.domain, err)
		}
		metaUc, err := rtps.MetatrafficUnicastPort(tc.domain, tc.participant)
		if err != nil {
			t.Fatalf("domain %d meta unicast: %v", tc.domain, err)
		}
		userMc, err := rtps.UsertrafficMulticastPort(tc.domain)
		if err != nil {
			t.Fatalf("domain %d user multicast: %v", tc.domain, err)
		}
		userUc, err := rtps.UsertrafficUnicastPort(tc.domain, tc.participant)
		if err != nil {
			t.Fatalf("domain %d user unicast: %v", tc.domain, err)
		}
		if metaMc != tc.metaMulticast || metaUc != tc.metaUnicast || userMc != tc.userMulticast || userUc != tc.userUnicast {
			t.Fatalf("domain %d participant %d = %d %d %d %d, want %d %d %d %d",
				tc.domain, tc.participant, metaMc, metaUc, userMc, userUc,
				tc.metaMulticast, tc.metaUnicast, tc.userMulticast, tc.userUnicast)
		}
	}
}

// TestPortMappingRejectsDomainAboveMax checks every mapper honors 101.
func TestPortMappingRejectsDomainAboveMax(t *testing.T) {
	t.Parallel()

	if _, err := rtps.MetatrafficMulticastPort(102); err == nil {
		t.Fatalf("meta multicast domain 102 succeeded, want error")
	}
	if _, err := rtps.UsertrafficMulticastPort(102); err == nil {
		t.Fatalf("user multicast domain 102 succeeded, want error")
	}
	if _, err := rtps.MetatrafficUnicastPort(102, 0); err == nil {
		t.Fatalf("meta unicast domain 102 succeeded, want error")
	}
	if _, err := rtps.UsertrafficUnicastPort(102, 0); err == nil {
		t.Fatalf("user unicast domain 102 succeeded, want error")
	}
}

// TestUnicastPortOverflow checks huge participants fail instead of wrap.
func TestUnicastPortOverflow(t *testing.T) {
	t.Parallel()

	if _, err := rtps.MetatrafficUnicastPort(0, 1<<30); err == nil {
		t.Fatalf("huge participant meta unicast succeeded, want error")
	}
	if _, err := rtps.UsertrafficUnicastPort(0, 1<<30); err == nil {
		t.Fatalf("huge participant user unicast succeeded, want error")
	}
}

// TestConfigValidate checks domain and port ranges.
func TestConfigValidate(t *testing.T) {
	t.Parallel()

	if err := (rtps.Config{}).Validate(); err != nil {
		t.Fatalf("zero config: %v", err)
	}
	for _, cfg := range []rtps.Config{
		{DomainID: 102},
		{MetaPort: -1},
		{MetaPort: 65536},
		{UnicastPort: -1},
		{UnicastPort: 65536},
	} {
		if err := cfg.Validate(); err == nil {
			t.Fatalf("config %+v succeeded, want error", cfg)
		}
	}
}

// TestJoinStandardBadDomain fails before binding any socket.
func TestJoinStandardBadDomain(t *testing.T) {
	t.Parallel()

	if _, err := rtps.JoinStandard(t.Context(), 200, 0); err == nil {
		t.Fatalf("join domain 200 succeeded, want error")
	}
}

// TestTransportLoopback exchanges multicast SPDP and unicast data.
func TestTransportLoopback(t *testing.T) {
	t.Parallel()

	first, err := rtps.Join(t.Context(), rtps.Config{DomainID: 0, ParticipantID: 0})
	if err != nil {
		t.Fatalf("join first: %v", err)
	}
	closeTransport(t, first)
	second, err := rtps.Join(t.Context(), rtps.Config{DomainID: 0, ParticipantID: 1})
	if err != nil {
		t.Fatalf("join second: %v", err)
	}
	closeTransport(t, second)

	if first.MetaPort() == second.MetaPort() {
		t.Fatalf("ephemeral meta ports collide on %d", first.MetaPort())
	}

	msg := buildTestMessage(t)
	buf := make([]byte, 65535)

	// Multicast SPDP path: first sends to the group at second meta port.
	// SendTo routes group traffic out the meta socket; the unicast
	// socket refuses it so loopback bound sends cannot vanish.
	exchange(t, second, msg, buf, "meta",
		func() error { return first.SendTo(second.SPDPTarget(), msg) },
		second.ReceiveMeta)

	// Unicast data path: second answers on first unicast socket.
	exchange(t, first, msg, buf, "unicast",
		func() error { return second.SendUnicast(first.LocalUnicastAddr(), msg) },
		first.ReceiveUnicast)

	// Self send proves multicast loopback is on.
	exchange(t, first, msg, buf, "self",
		func() error { return first.SendSPDP(msg) },
		first.ReceiveMeta)
}

// exchange sends msg through send and reads it back through recv.
func exchange(
	t *testing.T,
	recv *rtps.Transport,
	msg, buf []byte,
	op string,
	send func() error,
	take func([]byte) (int, *net.UDPAddr, error),
) {
	t.Helper()

	if err := recv.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("%s deadline: %v", op, err)
	}
	if err := send(); err != nil {
		t.Fatalf("%s send: %v", op, err)
	}
	got, _, err := take(buf)
	if err != nil {
		t.Fatalf("%s receive: %v", op, err)
	}
	if !bytes.Equal(buf[:got], msg) {
		t.Fatalf("%s bytes differ: %d of %d", op, got, len(msg))
	}
}

// TestTransportReceiveTimeout bounds blocking reads with deadlines.
func TestTransportReceiveTimeout(t *testing.T) {
	t.Parallel()

	tr, err := rtps.Join(t.Context(), rtps.Config{})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	closeTransport(t, tr)

	if err := tr.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	var netErr net.Error
	if _, _, err := tr.ReceiveUnicast(make([]byte, 1500)); !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("receive = %v, want timeout", err)
	}
}

// TestTransportSendErrors rejects empty and oversize payloads.
func TestTransportSendErrors(t *testing.T) {
	t.Parallel()

	tr, err := rtps.Join(t.Context(), rtps.Config{})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	closeTransport(t, tr)

	if err := tr.SendUnicast(tr.LocalUnicastAddr(), nil); err == nil {
		t.Fatalf("empty send succeeded, want error")
	}
	if err := tr.SendSPDP(make([]byte, rtps.MaxDatagramSize+1)); err == nil {
		t.Fatalf("oversize send succeeded, want error")
	}
	if err := tr.SendUnicast(tr.SPDPTarget(), buildTestMessage(t)); err == nil {
		t.Fatalf("unicast to group succeeded, want error")
	}
	if err := tr.SendTo(nil, buildTestMessage(t)); err == nil {
		t.Fatalf("send to nil succeeded, want error")
	}
}

// TestTransportCloseIdempotent checks teardown and use after close.
func TestTransportCloseIdempotent(t *testing.T) {
	t.Parallel()

	tr, err := rtps.Join(t.Context(), rtps.Config{})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if err := tr.SendSPDP(buildTestMessage(t)); !errors.Is(err, rtps.ErrClosed) {
		t.Fatalf("send after close = %v, want closed", err)
	}
	if _, _, err := tr.ReceiveMeta(make([]byte, 1500)); !errors.Is(err, rtps.ErrClosed) {
		t.Fatalf("receive after close = %v, want closed", err)
	}
}

// closeUnicast registers cleanup that fails the test on close errors.
func closeUnicast(t *testing.T, c *rtps.UnicastConn) {
	t.Helper()

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close unicast: %v", err)
		}
	})
}

// closeMulticast registers cleanup that fails the test on close errors.
func closeMulticast(t *testing.T, c *rtps.MulticastConn) {
	t.Helper()

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close multicast: %v", err)
		}
	})
}

// TestDialUnicastExchange dials two unicast sockets on one domain and
// exchanges a packet in both directions. The two sockets land on successive
// participant ports, which proves DialUnicast reuses the standard user
// unicast port table instead of duplicating it.
func TestDialUnicastExchange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	const domain uint32 = 70

	first, err := rtps.DialUnicast(ctx, domain)
	if err != nil {
		t.Fatalf("dial first: %v", err)
	}
	closeUnicast(t, first)
	second, err := rtps.DialUnicast(ctx, domain)
	if err != nil {
		t.Fatalf("dial second: %v", err)
	}
	closeUnicast(t, second)

	if first.LocalPort() == second.LocalPort() {
		t.Fatalf("unicast ports collide on %d", first.LocalPort())
	}
	checkUnicastTable(t, domain, first)
	checkUnicastTable(t, domain, second)

	msg := buildTestMessage(t)

	srcIP, srcPort, got := sendUnicastAndReceive(t, ctx, second, first, msg)
	if srcIP != "127.0.0.1" {
		t.Fatalf("src ip = %q, want 127.0.0.1", srcIP)
	}
	if srcPort != second.LocalPort() {
		t.Fatalf("src port = %d, want %d", srcPort, second.LocalPort())
	}

	// Mutating the returned copy must not affect the next datagram.
	got[0] ^= 0xFF
	_, _, got2 := sendUnicastAndReceive(t, ctx, first, second, msg)
	if !bytes.Equal(got2, msg) {
		t.Fatalf("second bytes differ: %d of %d", len(got2), len(msg))
	}
}

// checkUnicastTable verifies a socket matches the standard port table.
func checkUnicastTable(t *testing.T, domain uint32, c *rtps.UnicastConn) {
	t.Helper()

	want, err := rtps.UsertrafficUnicastPort(domain, c.Participant())
	if err != nil {
		t.Fatalf("expected port: %v", err)
	}
	if c.LocalPort() != want {
		t.Fatalf("port = %d, want table %d for participant %d", c.LocalPort(), want, c.Participant())
	}
	if c.Domain() != domain {
		t.Fatalf("domain = %d, want %d", c.Domain(), domain)
	}
}

// sendUnicastAndReceive sends msg from sender to receiver and returns the
// observed source and bytes.
func sendUnicastAndReceive(
	t *testing.T,
	ctx context.Context,
	sender, receiver *rtps.UnicastConn,
	msg []byte,
) (string, int, []byte) {
	t.Helper()

	if err := sender.SendTo(ctx, "127.0.0.1", receiver.LocalPort(), msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	srcIP, srcPort, got, err := receiver.Receive(recvCtx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("bytes differ: %d of %d", len(got), len(msg))
	}
	return srcIP, srcPort, got
}

// TestJoinMulticastLoopback checks self send and receive on an isolated
// domain. The bound port must equal the standard SPDP multicast port.
func TestJoinMulticastLoopback(t *testing.T) {
	t.Parallel()

	const domain uint32 = 71
	conn, err := rtps.JoinMulticast(t.Context(), domain)
	if err != nil {
		t.Fatalf("join multicast: %v", err)
	}
	closeMulticast(t, conn)

	want, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("expected port: %v", err)
	}
	if conn.Port() != want {
		t.Fatalf("port = %d, want %d", conn.Port(), want)
	}
	if conn.Domain() != domain {
		t.Fatalf("domain = %d, want %d", conn.Domain(), domain)
	}

	msg := buildTestMessage(t)
	if err := conn.Send(msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := conn.Receive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("bytes differ: %d of %d", len(got), len(msg))
	}
	// Mutating the copy must not corrupt later receives.
	got[0] ^= 0xFF
	if err := conn.Send(msg); err != nil {
		t.Fatalf("second send: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	got2, err := conn.Receive(ctx2)
	if err != nil {
		t.Fatalf("second receive: %v", err)
	}
	if !bytes.Equal(got2, msg) {
		t.Fatalf("second bytes differ: %d of %d", len(got2), len(msg))
	}
}

// TestUnicastReceiveCancel checks that context cancel unblocks a pending
// unicast receive with the context error and that a canceled context fails
// SendTo with the context error.
func TestUnicastReceiveCancel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	conn, err := rtps.DialUnicast(ctx, 72)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	closeUnicast(t, conn)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, _, err := conn.Receive(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("receive canceled = %v, want canceled", err)
	}
	if err := conn.SendTo(canceled, "127.0.0.1", conn.LocalPort(), buildTestMessage(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("send canceled = %v, want canceled", err)
	}

	blocking, stop := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, _, _, err := conn.Receive(blocking)
		errCh <- err
	}()
	// Receive honors context even if it has not blocked yet, so cancel
	// immediately without a wall sleep for a deterministic assertion.
	stop()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unblocked = %v, want canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("receive did not unblock on cancel")
	}
}

// TestMulticastReceiveCancel checks that context cancel unblocks a pending
// multicast receive with the context error.
func TestMulticastReceiveCancel(t *testing.T) {
	t.Parallel()

	conn, err := rtps.JoinMulticast(t.Context(), 73)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	closeMulticast(t, conn)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := conn.Receive(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("receive canceled = %v, want canceled", err)
	}

	blocking, stop := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := conn.Receive(blocking)
		errCh <- err
	}()
	// Receive honors context even if it has not blocked yet, so cancel
	// immediately without a wall sleep for a deterministic assertion.
	stop()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unblocked = %v, want canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("receive did not unblock on cancel")
	}
}

// TestBackendConnsDoubleClose checks idempotent teardown for both new
// socket types and that use after close reports ErrClosed.
func TestBackendConnsDoubleClose(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	uni, err := rtps.DialUnicast(ctx, 74)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := uni.Close(); err != nil {
		t.Fatalf("close unicast: %v", err)
	}
	if err := uni.Close(); err != nil {
		t.Fatalf("second close unicast: %v", err)
	}
	if err := uni.SendTo(ctx, "127.0.0.1", uni.LocalPort(), buildTestMessage(t)); !errors.Is(err, rtps.ErrClosed) {
		t.Fatalf("send after close = %v, want closed", err)
	}
	if _, _, _, err := uni.Receive(ctx); !errors.Is(err, rtps.ErrClosed) {
		t.Fatalf("receive after close = %v, want closed", err)
	}

	multi, err := rtps.JoinMulticast(t.Context(), 75)
	if err != nil {
		t.Fatalf("join multicast: %v", err)
	}
	if err := multi.Close(); err != nil {
		t.Fatalf("close multicast: %v", err)
	}
	if err := multi.Close(); err != nil {
		t.Fatalf("second close multicast: %v", err)
	}
	if err := multi.Send(buildTestMessage(t)); !errors.Is(err, rtps.ErrClosed) {
		t.Fatalf("multicast send after close = %v, want closed", err)
	}
	if _, err := multi.Receive(ctx); !errors.Is(err, rtps.ErrClosed) {
		t.Fatalf("multicast receive after close = %v, want closed", err)
	}
}

// TestBackendConnsBadDomain fails construction before binding any socket.
func TestBackendConnsBadDomain(t *testing.T) {
	t.Parallel()

	if _, err := rtps.DialUnicast(context.Background(), 102); err == nil {
		t.Fatalf("dial domain 102 succeeded, want error")
	}
	if _, err := rtps.JoinMulticast(t.Context(), 102); err == nil {
		t.Fatalf("join domain 102 succeeded, want error")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rtps.DialUnicast(canceled, 70); !errors.Is(err, context.Canceled) {
		t.Fatalf("dial canceled = %v, want canceled", err)
	}
}

// TestBackendConnsSendErrors rejects bad payloads and destinations.
func TestBackendConnsSendErrors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	uni, err := rtps.DialUnicast(ctx, 76)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	closeUnicast(t, uni)
	multi, err := rtps.JoinMulticast(t.Context(), 77)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	closeMulticast(t, multi)

	if err := uni.SendTo(ctx, "127.0.0.1", uni.LocalPort(), nil); err == nil {
		t.Fatalf("empty unicast send succeeded, want error")
	}
	if err := uni.SendTo(ctx, "127.0.0.1", uni.LocalPort(), make([]byte, rtps.MaxDatagramSize+1)); err == nil {
		t.Fatalf("oversize unicast send succeeded, want error")
	}
	if err := uni.SendTo(ctx, "not-an-ip", uni.LocalPort(), buildTestMessage(t)); err == nil {
		t.Fatalf("bad ip send succeeded, want error")
	}
	if err := uni.SendTo(ctx, "127.0.0.1", 0, buildTestMessage(t)); err == nil {
		t.Fatalf("bad port send succeeded, want error")
	}
	if err := multi.Send(nil); err == nil {
		t.Fatalf("empty multicast send succeeded, want error")
	}
	if err := multi.Send(make([]byte, rtps.MaxDatagramSize+1)); err == nil {
		t.Fatalf("oversize multicast send succeeded, want error")
	}
}

// TestWrongDomainMismatch documents that domains map to distinct ports, so
// a datagram sent on one domain never arrives on another. Backends must
// keep domains separate by port; there is no cross domain delivery.
func TestWrongDomainMismatch(t *testing.T) {
	t.Parallel()

	const domainA uint32 = 78
	const domainB uint32 = 79

	checkDomainPortsDiffer(t, domainA, domainB)

	ctx := context.Background()
	connA, connB := dialTwoDomains(t, ctx, domainA, domainB)
	checkUnicastIsolation(t, ctx, connA, connB)
	checkMulticastIsolation(t, ctx, domainA, domainB)
}

// checkDomainPortsDiffer verifies unicast and multicast ports differ.
func checkDomainPortsDiffer(t *testing.T, domainA, domainB uint32) {
	t.Helper()

	portA, err := rtps.UsertrafficUnicastPort(domainA, 0)
	if err != nil {
		t.Fatalf("port A: %v", err)
	}
	portB, err := rtps.UsertrafficUnicastPort(domainB, 0)
	if err != nil {
		t.Fatalf("port B: %v", err)
	}
	if portA == portB {
		t.Fatalf("unicast ports collide on %d across domains", portA)
	}
	metaA, err := rtps.MetatrafficMulticastPort(domainA)
	if err != nil {
		t.Fatalf("meta A: %v", err)
	}
	metaB, err := rtps.MetatrafficMulticastPort(domainB)
	if err != nil {
		t.Fatalf("meta B: %v", err)
	}
	if metaA == metaB {
		t.Fatalf("multicast ports collide on %d across domains", metaA)
	}
}

// dialTwoDomains dials one unicast socket per domain.
func dialTwoDomains(t *testing.T, ctx context.Context, domainA, domainB uint32) (*rtps.UnicastConn, *rtps.UnicastConn) {
	t.Helper()

	connA, err := rtps.DialUnicast(ctx, domainA)
	if err != nil {
		t.Fatalf("dial A: %v", err)
	}
	closeUnicast(t, connA)
	connB, err := rtps.DialUnicast(ctx, domainB)
	if err != nil {
		t.Fatalf("dial B: %v", err)
	}
	closeUnicast(t, connB)
	return connA, connB
}

// checkUnicastIsolation verifies a packet to connA never appears on connB.
func checkUnicastIsolation(t *testing.T, ctx context.Context, connA, connB *rtps.UnicastConn) {
	t.Helper()

	if err := connA.SendTo(ctx, "127.0.0.1", connA.LocalPort(), buildTestMessage(t)); err != nil {
		t.Fatalf("send A: %v", err)
	}
	recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, _, _, err := connA.Receive(recvCtx); err != nil {
		t.Fatalf("receive A: %v", err)
	}
	expectUnicastDeadline(t, ctx, connB)
}

// expectUnicastDeadline verifies a socket stays quiet until ctx ends.
func expectUnicastDeadline(t *testing.T, ctx context.Context, c *rtps.UnicastConn) {
	t.Helper()

	shortCtx, shortCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer shortCancel()
	if _, _, _, err := c.Receive(shortCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("receive B = %v, want deadline", err)
	}
}

// checkMulticastIsolation verifies multicast isolation across domains.
func checkMulticastIsolation(t *testing.T, ctx context.Context, domainA, domainB uint32) {
	t.Helper()

	multiA, err := rtps.JoinMulticast(ctx, domainA)
	if err != nil {
		t.Fatalf("join A: %v", err)
	}
	closeMulticast(t, multiA)
	multiB, err := rtps.JoinMulticast(ctx, domainB)
	if err != nil {
		t.Fatalf("join B: %v", err)
	}
	closeMulticast(t, multiB)
	if err := multiA.Send(buildTestMessage(t)); err != nil { //nolint:contextcheck // multicast Send is best-effort without context
		t.Fatalf("multicast send A: %v", err)
	}
	multiCtx, multiCancel := context.WithTimeout(ctx, 2*time.Second)
	defer multiCancel()
	if _, err := multiA.Receive(multiCtx); err != nil {
		t.Fatalf("multicast receive A: %v", err)
	}
	missCtx, missCancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer missCancel()
	if _, err := multiB.Receive(missCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("multicast receive B = %v, want deadline", err)
	}
}

// closeMulticastSender registers cleanup that fails the test on close errors.
func closeMulticastSender(t *testing.T, c *rtps.MulticastSender) {
	t.Helper()

	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("close sender: %v", err)
		}
	})
}

// TestDialMulticastSenderSendOnly checks the send-only multicast path never
// binds the group port, so any number of senders coexist without EADDRINUSE.
func TestDialMulticastSenderSendOnly(t *testing.T) {
	t.Parallel()

	const domain uint32 = 80

	first, second := dialTwoSenders(t, domain)
	msg := buildTestMessage(t)
	checkSenderPorts(t, domain, first, second)
	checkSenderSends(t, first, second, msg)
	checkSenderBlockedPort(t, domain, msg)
	checkSenderRejects(t, first)
}

// dialTwoSenders dials two send-only sockets on one domain.
func dialTwoSenders(t *testing.T, domain uint32) (*rtps.MulticastSender, *rtps.MulticastSender) {
	t.Helper()

	first, err := rtps.DialMulticastSender(domain)
	if err != nil {
		t.Fatalf("dial first: %v", err)
	}
	closeMulticastSender(t, first)

	second, err := rtps.DialMulticastSender(domain)
	if err != nil {
		t.Fatalf("dial second: %v", err)
	}
	closeMulticastSender(t, second)

	return first, second
}

// checkSenderPorts verifies both senders target the standard port.
func checkSenderPorts(t *testing.T, domain uint32, first, second *rtps.MulticastSender) {
	t.Helper()

	want, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("expected port: %v", err)
	}

	if first.Port() != want || second.Port() != want {
		t.Fatalf("ports = %d %d, want %d", first.Port(), second.Port(), want)
	}

	if first.Domain() != domain || second.Domain() != domain {
		t.Fatalf("domains = %d %d, want %d", first.Domain(), second.Domain(), domain)
	}
}

// checkSenderSends verifies both senders transmit without error.
func checkSenderSends(t *testing.T, first, second *rtps.MulticastSender, msg []byte) {
	t.Helper()

	if err := first.Send(msg); err != nil {
		t.Fatalf("send first: %v", err)
	}

	if err := second.Send(msg); err != nil {
		t.Fatalf("send second: %v", err)
	}
}

// checkSenderBlockedPort verifies dial still succeeds with the port pre-bound.
func checkSenderBlockedPort(t *testing.T, domain uint32, msg []byte) {
	t.Helper()

	want, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("expected port: %v", err)
	}

	blocker, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: want})
	if err != nil {
		t.Skipf("pre-bind %d: %v", want, err)
	}
	defer func() {
		if err := blocker.Close(); err != nil {
			t.Errorf("close blocker: %v", err)
		}
	}()

	third, err := rtps.DialMulticastSender(domain)
	if err != nil {
		t.Fatalf("dial with blocked port: %v", err)
	}
	closeMulticastSender(t, third)

	if err := third.Send(msg); err != nil {
		t.Fatalf("send third: %v", err)
	}
}

// checkSenderRejects verifies bad payloads and domains fail loudly.
func checkSenderRejects(t *testing.T, sender *rtps.MulticastSender) {
	t.Helper()

	if err := sender.Send(nil); err == nil {
		t.Fatalf("empty send succeeded, want error")
	}

	if err := sender.Send(make([]byte, rtps.MaxDatagramSize+1)); err == nil {
		t.Fatalf("oversize send succeeded, want error")
	}

	if _, err := rtps.DialMulticastSender(102); err == nil {
		t.Fatalf("dial domain 102 succeeded, want error")
	}
}

// TestMulticastSenderDoubleClose checks idempotent teardown and use after close.
func TestMulticastSenderDoubleClose(t *testing.T) {
	t.Parallel()

	sender, err := rtps.DialMulticastSender(81)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	if err := sender.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := sender.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	if err := sender.Send(buildTestMessage(t)); !errors.Is(err, rtps.ErrClosed) {
		t.Fatalf("send after close = %v, want closed", err)
	}
}

// TestTransportGetters checks domain, participant, ports, and addresses.
func TestTransportGetters(t *testing.T) {
	t.Parallel()

	tr, err := rtps.Join(t.Context(), rtps.Config{DomainID: 5, ParticipantID: 3})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	closeTransport(t, tr)

	if got := tr.DomainID(); got != 5 {
		t.Fatalf("domain = %d, want 5", got)
	}
	if got := tr.ParticipantID(); got != 3 {
		t.Fatalf("participant = %d, want 3", got)
	}
	if tr.MetaPort() == 0 {
		t.Fatal("meta port is zero, want ephemeral")
	}
	if tr.UnicastPort() == 0 {
		t.Fatal("unicast port is zero, want ephemeral")
	}
	if addr := tr.LocalUnicastAddr(); addr == nil || addr.Port != tr.UnicastPort() {
		t.Fatalf("local addr = %v, want port %d", addr, tr.UnicastPort())
	}
	if target := tr.SPDPTarget(); target == nil || target.Port != tr.MetaPort() {
		t.Fatalf("spdp target = %v, want port %d", target, tr.MetaPort())
	}
}

// TestBackendGetters checks getters for new socket types.
func TestBackendGetters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	uni, err := rtps.DialUnicast(ctx, 82)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	closeUnicast(t, uni)
	if addr := uni.LocalAddr(); addr == nil || addr.Port != uni.LocalPort() {
		t.Fatalf("local addr = %v, want port %d", addr, uni.LocalPort())
	}

	multi, err := rtps.JoinMulticast(ctx, 83)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	closeMulticast(t, multi)
	if target := multi.Target(); target == nil || target.Port != multi.Port() {
		t.Fatalf("target = %v, want port %d", target, multi.Port())
	}

	sender, err := rtps.DialMulticastSender(84)
	if err != nil {
		t.Fatalf("dial sender: %v", err)
	}
	closeMulticastSender(t, sender)
	if target := sender.Target(); target == nil || target.Port != sender.Port() {
		t.Fatalf("sender target = %v, want port %d", target, sender.Port())
	}
}

// TestJoinCanceled checks canceled context fails fast.
func TestJoinCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := rtps.Join(ctx, rtps.Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("join canceled = %v, want canceled", err)
	}
	if _, err := rtps.Join(ctx, rtps.Config{DomainID: 200}); err == nil {
		t.Fatal("join bad domain with canceled ctx succeeded, want error")
	}
}

// TestSetReadDeadlineAfterClose checks deadline on closed transport fails.
func TestSetReadDeadlineAfterClose(t *testing.T) {
	t.Parallel()

	tr, err := rtps.Join(t.Context(), rtps.Config{})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := tr.SetReadDeadline(time.Now()); err == nil {
		t.Fatal("deadline after close succeeded, want error")
	}
}

// TestDecodeParamsEndianPad checks pad truncation fails.
func TestDecodeParamsEndianPad(t *testing.T) {
	t.Parallel()

	raw := rtps.EncodeParams([]rtps.Param{{ID: 0x0050, Value: []byte{0xAA}}})
	// Corrupt pad: drop last pad byte so value is present but pad is short.
	short := raw[:len(raw)-1]
	if _, _, err := rtps.DecodeParamsEndian(short, true); err == nil {
		t.Fatal("short pad via endian succeeded, want error")
	}
}
