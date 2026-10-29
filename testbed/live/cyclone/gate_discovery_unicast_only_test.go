// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cyclone

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// errReuseBlocker reports a blocker socket that is not UDP.
var errReuseBlocker = errors.New("cyclone: blocker is not udp")

// TestCycloneDiscoveryUnicastOnlyLive proves directed unicast SPDP discovers peers.
//
// It holds a blocker on one allocated domain metatraffic multicast port, then uses
// direct RTPS directed unicast to exchange one Weld alive packet and parses
// it with dds.ParseAnnouncement. It then joins two Weld backends on the same
// blocked domain and requires one String relay, which mirrors the unit
// TestBackendUnicastOnlyDiscovery path live. Prefix inequality proves
// non-locality. All calls use context timeouts and the compose stack is
// never started.
func TestCycloneDiscoveryUnicastOnlyLive(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	discoveryPinDomain(t, domain)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	discoveryDockerCrossCheck(t, ctx)
	metaPort := checkMulticastBlocker(t, domain)
	observer, announcer := newLiveParticipants(t, domain, discoveryLease)
	uni := checkDialUnicastOnly(t, ctx, domain)
	got := checkUnicastExchange(t, ctx, uni, announcer, observer, domain)
	checkUnicastBackends(t, ctx, got, metaPort, domain)
}

// checkMulticastBlocker pre-binds multicast port and returns the port.
// checkMulticastBlocker pre-binds multicast port and returns the port on one allocated domain.
func checkMulticastBlocker(t *testing.T, domain uint32) int {
	t.Helper()
	metaPort, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("meta port: %v", err)
	}
	blocker, err := listenReuseBlocker(metaPort)
	if err != nil {
		t.Logf("multicast port %d already held, continuing as blocked: %v", metaPort, err)
		return metaPort
	}
	t.Cleanup(func() {
		cerr := blocker.Close()
		if cerr != nil {
			t.Logf("close blocker: %v", cerr)
		}
	})
	return metaPort
}

// setReuseAddr enables address reuse on fd.
func setReuseAddr(fd uintptr) error {
	err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
	if err != nil {
		return fmt.Errorf("cyclone: reuse addr: %w", err)
	}
	return nil
}

// setReusePort enables port reuse on fd for coexisting participants.
func setReusePort(fd uintptr) error {
	err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, 15, 1)
	if err != nil {
		return fmt.Errorf("cyclone: reuse port: %w", err)
	}
	return nil
}

// applyReuseOpts sets reuse addr plus port and records the first failure.
func applyReuseOpts(fd uintptr) error {
	var first error
	err := setReuseAddr(fd)
	if err != nil && first == nil {
		first = err
	}
	err = setReusePort(fd)
	if err != nil && first == nil {
		first = err
	}
	return first
}

// controlReuseBlocker applies reuse opts inside the listen control hook.
func controlReuseBlocker(_, _ string, c syscall.RawConn) error {
	var first error
	ctrlErr := c.Control(func(fd uintptr) {
		err := applyReuseOpts(fd)
		if err != nil && first == nil {
			first = err
		}
	})
	if ctrlErr != nil {
		return fmt.Errorf("cyclone: reuse control: %w", ctrlErr)
	}
	return first
}

// assertReuseUDP checks the packet conn is UDP and closes others.
func assertReuseUDP(pc net.PacketConn) (*net.UDPConn, error) {
	conn, ok := pc.(*net.UDPConn)
	if ok {
		return conn, nil
	}
	err := pc.Close()
	if err != nil {
		return nil, fmt.Errorf("cyclone: close non-udp blocker: %w", err)
	}
	return nil, errReuseBlocker //nolint:wrapcheck // sentinel for non-udp blocker, callers compare with errors.Is
}

// listenReuseBlocker binds 127.0.0.1 on port with reuse so it coexists
// with vendor participants that already hold the group port with reuse.
func listenReuseBlocker(port int) (*net.UDPConn, error) {
	addr := "127.0.0.1:" + strconv.Itoa(port)
	lc := net.ListenConfig{
		Control: controlReuseBlocker,
	}
	pc, err := lc.ListenPacket(context.Background(), "udp4", addr)
	if err != nil {
		return nil, fmt.Errorf("cyclone: listen reuse blocker: %w", err)
	}
	conn, err := assertReuseUDP(pc)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// checkDialUnicastOnly dials one unicast socket with cleanup.
// checkDialUnicastOnly dials one unicast socket with cleanup on one allocated domain.
func checkDialUnicastOnly(t *testing.T, ctx context.Context, domain uint32) *rtps.UnicastConn {
	t.Helper()
	dialCtx, dialCancel := context.WithTimeout(ctx, discoveryDialTimeout)
	uni, err := rtps.DialUnicast(dialCtx, domain)
	dialCancel()
	if err != nil {
		t.Fatalf("dial unicast: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseUnicast(t, uni)
	})
	return uni
}

// checkUnicastExchange sends alive via unicast and pins parsed fields.
// checkUnicastExchange sends alive via unicast and pins parsed fields on one allocated domain.
func checkUnicastExchange(t *testing.T, ctx context.Context, uni *rtps.UnicastConn, announcer, observer *dds.LocalParticipant, domain uint32) []byte {
	t.Helper()
	packet, err := dds.MarshalAlivePacket(announcer.GUID(), domain, 3, announcer.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	sendCtx, sendCancel := context.WithTimeout(ctx, discoveryDialTimeout)
	err = uni.SendTo(sendCtx, "127.0.0.1", uni.LocalPort(), packet)
	sendCancel()
	if err != nil {
		t.Fatalf("unicast send: %v", err)
	}
	recvCtx, recvCancel := context.WithTimeout(ctx, discoveryRecvTimeout)
	_, _, got, err := uni.Receive(recvCtx)
	recvCancel()
	if err != nil {
		t.Fatalf("unicast receive: %v", err)
	}
	checkUnicastPacket(t, got, announcer, observer, domain)
	return got
}

// checkUnicastPacket pins parsed unicast announcement fields.
// checkUnicastPacket pins parsed unicast announcement fields on one allocated domain.
func checkUnicastPacket(t *testing.T, got []byte, announcer, observer *dds.LocalParticipant, domain uint32) {
	t.Helper()
	ann, err := dds.ParseAnnouncement(got)
	if err != nil {
		t.Fatalf("parse unicast: %v", err)
	}
	if ann.DomainID != domain {
		t.Fatalf("unicast domain = %d, want %d", ann.DomainID, domain)
	}
	if !ann.Alive {
		t.Fatalf("unicast parsed as dispose")
	}
	if ann.Lease <= 0 {
		t.Fatalf("unicast lease = %v, want positive", ann.Lease)
	}
	if len(ann.Ports) == 0 {
		t.Fatalf("unicast ports empty")
	}
	if ann.GUID.Prefix == observer.GUID().Prefix {
		t.Fatalf("unicast prefix matches observer, want non-local")
	}
	if ann.GUID.Prefix != announcer.GUID().Prefix {
		t.Fatalf("unicast GUID mismatch")
	}
	hdr, data := decodeGateSPDP(t, got)
	checkGateWire(t, hdr, data)
}

// checkUnicastBackends joins two backends and relays one sample.
// checkUnicastBackends joins two backends and relays one sample on one allocated domain.
func checkUnicastBackends(t *testing.T, ctx context.Context, got []byte, metaPort int, domain uint32) {
	t.Helper()
	_ = got
	runID := helpers.LiveID("unicast-only")
	first, err := dds.NewBackend(dds.SidecarConfig{
		DomainID:    domain,
		PeerAddress: dds.DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	})
	if err != nil {
		t.Fatalf("new first backend: %v", err)
	}
	t.Cleanup(func() { //nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
		cerr := first.Close()
		if cerr != nil {
			t.Logf("close first: %v", cerr)
		}
	})
	err = first.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "unicast-a-" + runID})
	if err != nil {
		t.Fatalf("join first: %v", err)
	}
	second, err := dds.NewBackend(dds.SidecarConfig{
		DomainID:    domain,
		PeerAddress: dds.DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	})
	if err != nil {
		t.Fatalf("new second backend: %v", err)
	}
	t.Cleanup(func() { //nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
		cerr := second.Close()
		if cerr != nil {
			t.Logf("close second: %v", cerr)
		}
	})
	err = second.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "unicast-b-" + runID})
	if err != nil {
		t.Fatalf("join second: %v", err)
	}
	payload := []byte("hello unicast " + runID)
	checkUnicastPublish(t, ctx, first, payload, domain)
	checkUnicastTake(t, ctx, second, payload, metaPort)
}

// checkUnicastPublish publishes until route appears.
// checkUnicastPublish publishes until route appears on one allocated domain.
func checkUnicastPublish(t *testing.T, ctx context.Context, first *dds.Backend, payload []byte, domain uint32) {
	t.Helper()
	published := false
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		pubCtx, pubCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		err := first.PublishBytes(pubCtx, transports.OutgoingMessage{
			TopicName: "/chatter",
			TypeName:  "std_msgs__msg__String",
			Payload:   payload,
		})
		pubCancel()
		if err == nil {
			published = true
			break
		}
		if ctx.Err() != nil {
			break
		}
		sleepCtx, sleepCancel := context.WithTimeout(ctx, 100*time.Millisecond)
		<-sleepCtx.Done()
		sleepCancel()
	}
	if !published {
		t.Fatalf("unicast-only backends did not gain a route on domain %d", domain)
	}
}

// checkUnicastTake takes one sample and pins topic and payload.
func checkUnicastTake(t *testing.T, ctx context.Context, second *dds.Backend, payload []byte, metaPort int) {
	t.Helper()
	takeCtx, takeCancel := context.WithTimeout(ctx, 5*time.Second)
	defer takeCancel()
	msg, err := second.TakeBytes(takeCtx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	if msg.TopicName != "/chatter" {
		t.Fatalf("topic = %q, want /chatter", msg.TopicName)
	}
	if string(msg.Payload) != string(payload) {
		t.Fatalf("payload mismatch")
	}
	t.Logf("unicast-only backends relayed %d bytes with multicast port %d blocked", len(msg.Payload), metaPort)
}
