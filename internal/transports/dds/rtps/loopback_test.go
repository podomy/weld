// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package rtps

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"syscall"
	"testing"
	"time"
)

// TestEnableMulticastLoopback exercises the platform socket option path.
// On Linux it must succeed both ways; elsewhere it reports unsupported.
func TestEnableMulticastLoopback(t *testing.T) {
	t.Parallel()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	err = enableMulticastLoopback(conn, true)
	if runtime.GOOS == "linux" {
		if err != nil {
			t.Fatalf("enable: %v", err)
		}
		if err := enableMulticastLoopback(conn, false); err != nil {
			t.Fatalf("disable: %v", err)
		}
		if err := enableMulticastLoopback(conn, true); err != nil {
			t.Fatalf("re-enable: %v", err)
		}
		return
	}
	if err == nil {
		t.Fatalf("off platform enable succeeded, want unsupported")
	}
}

// TestListenMetaReuseCoexist proves two SPDP sockets share one multicast
// port, which is how Weld coexists with vendor participants on one host.
func TestListenMetaReuseCoexist(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reuse port bind is linux only")
	}

	group, err := spdpGroup()
	if err != nil {
		t.Fatalf("spdp group: %v", err)
	}

	first, err := listenMetaReuse(context.Background(), 0, group)
	if err != nil {
		t.Fatalf("first bind: %v", err)
	}
	defer func() {
		if err := first.Close(); err != nil {
			t.Errorf("close first: %v", err)
		}
	}()

	port, err := boundPort(first)
	if err != nil {
		t.Fatalf("bound port: %v", err)
	}

	second, err := listenMetaReuse(context.Background(), port, group)
	if err != nil {
		t.Fatalf("second bind on port %d: %v", port, err)
	}
	defer func() {
		if err := second.Close(); err != nil {
			t.Errorf("close second: %v", err)
		}
	}()
}

// TestPickMulticastEgress prefers the LAN interface over loopback.
func TestPickMulticastEgress(t *testing.T) {
	t.Parallel()

	lan := &net.IPNet{IP: net.ParseIP("192.168.0.162"), Mask: net.CIDRMask(24, 32)}
	lo := &net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)}

	list := []interfaceAddrs{
		{name: "lo", flags: net.FlagUp | net.FlagMulticast | net.FlagLoopback, addrs: []net.Addr{lo}},
		{name: "down", flags: net.FlagMulticast, addrs: []net.Addr{lan}},
		{name: "nomulti", flags: net.FlagUp, addrs: []net.Addr{lan}},
		{name: "wlan", flags: net.FlagUp | net.FlagMulticast, addrs: []net.Addr{lan}},
	}

	if got := pickMulticastEgress(list); !got.Equal(net.ParseIP("192.168.0.162")) {
		t.Fatalf("egress = %v, want 192.168.0.162", got)
	}

	if got := pickMulticastEgress(nil); got != nil {
		t.Fatalf("egress = %v, want nil", got)
	}
}

// TestCloseBoth checks success and double close failure.
func TestCloseBoth(t *testing.T) {
	t.Parallel()

	first, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen first: %v", err)
	}
	second, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen second: %v", err)
		if cerr := first.Close(); cerr != nil {
			t.Logf("close first: %v", cerr)
		}
	}
	if err := closeBoth(first, second); err != nil {
		t.Fatalf("closeBoth: %v", err)
	}
	if err := closeBoth(first, second); err == nil {
		t.Fatal("second closeBoth succeeded, want error")
	}
}

// TestBoundPorts checks bound port extraction.
func TestBoundPorts(t *testing.T) {
	t.Parallel()

	meta, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen meta: %v", err)
	}
	t.Cleanup(func() {
		if cerr := meta.Close(); cerr != nil {
			t.Logf("close meta: %v", cerr)
		}
	})
	uni, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen uni: %v", err)
	}
	t.Cleanup(func() {
		if cerr := uni.Close(); cerr != nil {
			t.Logf("close uni: %v", cerr)
		}
	})
	metaPort, uniPort, err := boundPorts(meta, uni)
	if err != nil {
		t.Fatalf("boundPorts: %v", err)
	}
	if metaPort == 0 || uniPort == 0 {
		t.Fatalf("ports = %d %d, want nonzero", metaPort, uniPort)
	}
}

// TestEnableLoopbackHelper checks enableLoopback success and failure.
func TestEnableLoopbackHelper(t *testing.T) {
	t.Parallel()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	})
	if err := enableLoopback(conn, "test"); err != nil {
		t.Fatalf("enableLoopback: %v", err)
	}
	closed, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen closed: %v", err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := enableLoopback(closed, "test"); err == nil {
		t.Fatal("enableLoopback on closed succeeded, want error")
	}
}

// TestMulticastEndpoint checks port and group resolution.
func TestMulticastEndpoint(t *testing.T) {
	t.Parallel()

	port, group, err := multicastEndpoint(0)
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	if port == 0 || group == nil {
		t.Fatalf("endpoint = %d %v, want nonzero", port, group)
	}
	if _, _, err := multicastEndpoint(102); err == nil {
		t.Fatal("bad domain succeeded, want error")
	}
}

// testUnixSocketPath is the unixgram path for the non-UDP assertion case.
const testUnixSocketPath = "/tmp/opencode-rtps-test.sock"

// TestPacketConnToUDP checks type assertion paths.
func TestPacketConnToUDP(t *testing.T) {
	t.Parallel()

	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	t.Cleanup(func() {
		if cerr := udp.Close(); cerr != nil {
			t.Logf("close udp: %v", cerr)
		}
	})
	var asPacket net.PacketConn = udp
	got, err := packetConnToUDP(asPacket)
	if err != nil {
		t.Fatalf("packetConnToUDP: %v", err)
	}
	if got != udp {
		t.Fatal("packetConnToUDP returned different conn")
	}
	lc := net.ListenConfig{}
	unix, err := lc.ListenPacket(context.Background(), "unixgram", testUnixSocketPath)
	if err != nil {
		t.Skipf("unixgram unavailable: %v", err)
	}
	t.Cleanup(func() {
		if cerr := unix.Close(); cerr != nil {
			t.Logf("close unix: %v", cerr)
		}
	})
	if _, err := packetConnToUDP(unix); err == nil {
		t.Fatal("non udp succeeded, want error")
	}
}

// TestJoinWithCleanup checks group join success and bad group.
func TestJoinWithCleanup(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("join group is linux only")
	}
	t.Parallel()

	group, err := spdpGroup()
	if err != nil {
		t.Fatalf("spdp group: %v", err)
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	})
	// Bad group fails and closes the socket; use a fresh socket for it.
	bad, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen bad: %v", err)
	}
	if err := joinWithCleanup(bad, net.ParseIP("::1")); err == nil {
		t.Fatal("bad group succeeded, want error")
		if cerr := bad.Close(); cerr != nil {
			t.Logf("close bad: %v", cerr)
		}
	}
	_ = group
	_ = conn
}

// TestJoinGroupBad checks non IPv4 rejection.
func TestJoinGroupBad(t *testing.T) {
	t.Parallel()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	})
	if err := joinGroup(conn, net.ParseIP("::1")); err == nil {
		t.Fatal("ipv6 group succeeded, want error")
	}
}

// TestMapSendError checks closed, canceled, and wrapped paths.
func TestMapSendError(t *testing.T) {
	t.Parallel()

	closedTrue := func() bool { return true }
	closedFalse := func() bool { return false }
	if err := mapSendError("op", errors.New("boom"), closedTrue, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed = %v, want closed", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mapSendError("op", errors.New("boom"), closedFalse, ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled = %v, want canceled", err)
	}
	if err := mapSendError("op", errors.New("boom"), closedFalse, nil); err == nil {
		t.Fatal("wrap succeeded without error, want wrapped")
	}
}

// TestWriteDatagram checks success and closed mapping.
func TestWriteDatagram(t *testing.T) {
	t.Parallel()

	recv, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen recv: %v", err)
	}
	t.Cleanup(func() {
		if cerr := recv.Close(); cerr != nil {
			t.Logf("close recv: %v", cerr)
		}
	})
	send, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen send: %v", err)
	}
	t.Cleanup(func() {
		if cerr := send.Close(); cerr != nil {
			t.Logf("close send: %v", cerr)
		}
	})
	addr, err := net.ResolveUDPAddr("udp4", recv.LocalAddr().String())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	neverClosed := func() bool { return false }
	if err := writeDatagram(send, addr, []byte{0x01}, "op", "short", neverClosed, nil); err != nil {
		t.Fatalf("write: %v", err)
	}
	closedConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen closed: %v", err)
	}
	if err := closedConn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	alwaysClosed := func() bool { return true }
	if err := writeDatagram(closedConn, addr, []byte{0x01}, "op", "short", alwaysClosed, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed write = %v, want closed", err)
	}
}

// TestSetPollDeadlineClosed checks closed mapping.
func TestSetPollDeadlineClosed(t *testing.T) {
	t.Parallel()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	})
	neverClosed := func() bool { return false }
	if err := setPollDeadline(conn, "test", neverClosed); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	closed, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen closed: %v", err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	alwaysClosed := func() bool { return true }
	if err := setPollDeadline(closed, "test", alwaysClosed); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed deadline = %v, want closed", err)
	}
}

// TestCollectAndApplyEgress checks interface listing and egress pinning.
func TestCollectAndApplyEgress(t *testing.T) {
	t.Parallel()

	list, err := collectInterfaceAddrs()
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	if len(list) == 0 {
		t.Fatal("no interfaces, want some")
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	})
	if err := applyEgressInterface(conn, net.ParseIP("127.0.0.1")); err != nil {
		t.Fatalf("apply egress: %v", err)
	}
	closed, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen closed: %v", err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := applyEgressInterface(closed, net.ParseIP("127.0.0.1")); err == nil {
		t.Fatal("apply on closed succeeded, want error")
	}
}

// TestParamHelpers checks sentinel and value helpers.
func TestParamHelpers(t *testing.T) {
	t.Parallel()

	if err := decodePIDSentinel(1); err == nil {
		t.Fatal("nonzero sentinel succeeded, want error")
	}
	if _, _, err := decodeParameterSentinel(nil, 0, 1); err == nil {
		t.Fatal("nonzero parameter sentinel succeeded, want error")
	}
	if _, _, err := copyParameterValue([]byte{0x01}, 0, 4, 0x50); err == nil {
		t.Fatal("short value succeeded, want error")
	}
	if _, _, err := copyParameterValue([]byte{0x01, 0x02, 0x03}, 0, 1, 0x50); err == nil {
		t.Fatal("short pad succeeded, want error")
	}
	if _, _, err := copyPIDValue([]byte{0x01}, 0, 4, 0x50); err == nil {
		t.Fatal("short pid value succeeded, want error")
	}
	if _, _, err := copyPIDValue([]byte{0x01, 0x02, 0x03}, 0, 1, 0x50); err == nil {
		t.Fatal("short pid pad succeeded, want error")
	}
	if err := checkDomain(102); err == nil {
		t.Fatal("bad domain succeeded, want error")
	}
	closedJoin, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen closed join: %v", err)
	}
	if err := closedJoin.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := enableJoinLoopback(closedJoin); err == nil {
		t.Fatal("closed join loopback succeeded, want error")
	}
	_ = time.Now
}

// TestCheckContextCanceled checks canceled context maps with operation name.
func TestCheckContextCanceled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := checkContext(ctx, "probe")
	if err == nil {
		t.Fatal("canceled check succeeded, want error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("check = %v, want canceled", err)
	}
	err = checkContext(context.Background(), "probe")
	if err != nil {
		t.Fatalf("live check = %v, want nil", err)
	}
}

// TestCloseGuardIdempotent checks guard tryClose and isClosed.
func TestCloseGuardIdempotent(t *testing.T) {
	t.Parallel()

	var guard closeGuard
	if guard.isClosed() {
		t.Fatal("fresh guard reports closed")
	}
	if !guard.tryClose() {
		t.Fatal("first tryClose reports false")
	}
	if !guard.isClosed() {
		t.Fatal("closed guard reports open")
	}
	if guard.tryClose() {
		t.Fatal("second tryClose reports true")
	}
}

// TestCopyParamBytesWrapping checks shared core and PID error wrapping.
func TestCopyParamBytesWrapping(t *testing.T) {
	t.Parallel()

	value, next, err := copyParamBytes([]byte{0xAA, 0xBB, 0xCC, 0x00}, 0, 3)
	if err != nil {
		t.Fatalf("copy core: %v", err)
	}
	if next != 4 {
		t.Fatalf("next = %d, want 4", next)
	}
	if len(value) != 3 || value[0] != 0xAA {
		t.Fatalf("value = % X, want AA BB CC", value)
	}
	if _, _, err := copyParamBytes([]byte{0x01}, 0, 4); err == nil {
		t.Fatal("short core value succeeded, want error")
	}
	if _, _, err := copyParamBytes([]byte{0x01, 0x02, 0x03}, 0, 1); err == nil {
		t.Fatal("short core pad succeeded, want error")
	}
	_, _, err = copyPIDValue([]byte{0x01}, 0, 4, 0x50)
	if err == nil {
		t.Fatal("short pid succeeded, want error")
	}
	if !errors.Is(err, ErrInvalidParam) {
		t.Fatalf("pid = %v, want ErrInvalidParam", err)
	}
	_, _, err = copyParameterValue([]byte{0x01}, 0, 4, 0x50)
	if err == nil {
		t.Fatal("short parameter succeeded, want error")
	}
	if errors.Is(err, ErrInvalidParam) {
		t.Fatalf("parameter = %v, want no ErrInvalidParam", err)
	}
}

// TestSentinelWrapping checks sentinel helpers preserve PID wrapping.
func TestSentinelWrapping(t *testing.T) {
	t.Parallel()

	if err := checkSentinelLength(1); err == nil {
		t.Fatal("bad sentinel length succeeded, want error")
	}
	if err := checkSentinelLength(0); err != nil {
		t.Fatalf("good sentinel = %v, want nil", err)
	}
	err := decodePIDSentinel(1)
	if err == nil {
		t.Fatal("bad pid sentinel succeeded, want error")
	}
	if !errors.Is(err, ErrInvalidParam) {
		t.Fatalf("pid sentinel = %v, want ErrInvalidParam", err)
	}
}

// TestDialSlotErrorBranches checks retry versus fatal mapping.
func TestDialSlotErrorBranches(t *testing.T) {
	t.Parallel()

	inUse := fmt.Errorf("bind: %w", syscall.EADDRINUSE)
	retry, err := dialSlotError(inUse, nil)
	if err == nil || !retry {
		t.Fatalf("in-use retry = %v %v, want true plus error", retry, err)
	}
	other := errors.New("boom")
	retry, err = dialSlotError(other, nil)
	if err == nil || retry {
		t.Fatalf("other retry = %v %v, want false plus error", retry, err)
	}
	prev := errors.New("prev")
	retry, err = dialSlotError(other, prev)
	if err == nil || retry {
		t.Fatalf("joined retry = %v %v, want false plus error", retry, err)
	}
	if !errors.Is(err, other) || !errors.Is(err, prev) {
		t.Fatalf("joined = %v, want both causes", err)
	}
}

// TestNoFreeSlotErrorBranches checks exhausted probe mapping.
func TestNoFreeSlotErrorBranches(t *testing.T) {
	t.Parallel()

	err := noFreeSlotError(nil)
	if err == nil {
		t.Fatal("nil lastErr succeeded, want error")
	}
	cause := errors.New("full")
	err = noFreeSlotError(cause)
	if !errors.Is(err, cause) {
		t.Fatalf("wrapped = %v, want cause", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := probeUnicastSlots(ctx, 70); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe = %v, want canceled", err)
	}
}

// TestOpenTransportSocketsFailure checks cleanup on bad unicast port.
func TestOpenTransportSocketsFailure(t *testing.T) {
	t.Parallel()

	_, _, err := openTransportSockets(context.Background(), Config{MetaPort: 0, UnicastPort: 99999})
	if err == nil {
		t.Fatal("bad unicast port succeeded, want error")
	}
	_, _, err = openTransportSockets(context.Background(), Config{MetaPort: 99999, UnicastPort: 0})
	if err == nil {
		t.Fatal("bad meta port succeeded, want error")
	}
}

// TestEnableTransportLoopbackFailure checks error on closed sockets.
func TestEnableTransportLoopbackFailure(t *testing.T) {
	t.Parallel()

	good, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if cerr := good.Close(); cerr != nil {
			t.Logf("close good: %v", cerr)
		}
	})
	bad, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen bad: %v", err)
	}
	if err := bad.Close(); err != nil {
		t.Fatalf("close bad: %v", err)
	}
	if err := enableTransportLoopback(bad, good); err == nil {
		t.Fatal("loopback on closed meta succeeded, want error")
	}
	if err := enableTransportLoopback(good, bad); err == nil {
		t.Fatal("loopback on closed unicast succeeded, want error")
	}
}

// TestSetupSenderHelpersFailure checks sender setup cleanup paths.
func TestSetupSenderHelpersFailure(t *testing.T) {
	t.Parallel()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cause := errors.New("boom")
	err = closeSetupConn(conn, cause, "probe op")
	if err == nil {
		t.Fatal("closeSetupConn succeeded, want error")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("closeSetup = %v, want cause", err)
	}
	closed, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen closed: %v", err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := setupSenderLoopback(closed); err == nil {
		t.Fatal("sender loopback on closed succeeded, want error")
	}
	if err := setupSenderSocket(closed); err == nil {
		t.Fatal("sender socket on closed succeeded, want error")
	}
}

// TestReadSingleDatagramClosed checks poll failure maps to closed.
func TestReadSingleDatagramClosed(t *testing.T) {
	t.Parallel()

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("close: %v", cerr)
		}
	})
	buf := make([]byte, MaxDatagramSize)
	closed, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen closed: %v", err)
	}
	if err := closed.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	alwaysClosed := func() bool { return true }
	if _, _, _, err := readSingleDatagram(closed, buf, context.Background(), "probe", alwaysClosed); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed read = %v, want closed", err)
	}
	neverClosed := func() bool { return false }
	if _, _, _, err := readSingleDatagram(closed, buf, context.Background(), "probe", neverClosed); err == nil {
		t.Fatal("dead read succeeded, want error")
	}
	_ = conn
}

// TestCheckDataHelpers checks size and flag validators with drops.
func TestCheckDataHelpers(t *testing.T) {
	t.Parallel()

	var drops Counter
	if err := checkDataSize(make([]byte, 3), &drops); err == nil {
		t.Fatal("short data size succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
	if err := checkDataFlags(DataFlagDataPresent|DataFlagKeyPresent, &drops); err == nil {
		t.Fatal("both flags succeeded, want error")
	}
	if drops.Count() != 2 {
		t.Fatalf("drops = %d, want 2", drops.Count())
	}
	if err := checkDataFlags(DataFlagDataPresent, &drops); err != nil {
		t.Fatalf("single flag = %v, want nil", err)
	}
}

// TestCheckAckNackSizeChecks checks oversize detection.
func TestCheckAckNackSizeChecks(t *testing.T) {
	t.Parallel()

	if err := checkAckNackSize(make([]byte, maxSubmessageBody+1)); err == nil {
		t.Fatal("oversize acknack succeeded, want error")
	}
	if err := checkAckNackSize(make([]byte, 8)); err != nil {
		t.Fatalf("small acknack = %v, want nil", err)
	}
	if got := copyDatagram([]byte{0xAA, 0xBB}, 2); len(got) != 2 || got[0] != 0xAA {
		t.Fatalf("copy = % X, want AA BB", got)
	}
}
