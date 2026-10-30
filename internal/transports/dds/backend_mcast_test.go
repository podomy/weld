// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// waitForMcastRemote polls HasRemoteEndpoint until the writer appears.
// It exists so the multicast SEDP test waits for the shared demux plus the
// listen loop instead of sleeping a fixed interval.
func waitForMcastRemote(t *testing.T, backend *Backend, topic string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)

	for time.Now().Before(deadline) {
		if backend.HasRemoteEndpoint(topic) {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("multicast SEDP for %q did not install", topic)
}

// checkMcastPort verifies the group socket uses the standard domain port.
// It proves the join targets 239.255.0.1 on the domain port, not ephemeral.
func checkMcastPort(t *testing.T, backend *Backend, domain uint32) {
	t.Helper()

	backend.mu.RLock()
	mcastRx := backend.mcastRx
	backend.mu.RUnlock()

	if mcastRx == nil {
		t.Fatalf("multicast receive is nil on domain %d, want joined group", domain)
	}

	want, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("expected port: %v", err)
	}

	if mcastRx.Port() != want {
		t.Fatalf("multicast port = %d, want %d", mcastRx.Port(), want)
	}

	if mcastRx.Domain() != domain {
		t.Fatalf("multicast domain = %d, want %d", mcastRx.Domain(), domain)
	}
}

// checkSelfFiltering verifies loopback self sends are dropped before dispatch.
// It keeps our own multicast SPDP from re-entering discovery as a remote.
func checkSelfFiltering(t *testing.T, backend *Backend) {
	t.Helper()

	backend.mu.RLock()
	local := backend.local
	backend.mu.RUnlock()

	if local == nil {
		t.Fatalf("local participant is nil, want live backend")
	}

	prefix := local.GUID().Prefix
	header := rtps.DefaultHeader([12]byte(prefix))
	raw := header.Encode()

	selfMsg := append([]byte(nil), raw[:]...)
	if !isSelfDatagram(selfMsg, local) {
		t.Fatalf("self header not detected as self, want filtered")
	}

	var otherPrefix [12]byte
	for i := range otherPrefix {
		otherPrefix[i] = 0xAA + byte(i)
	}

	if otherPrefix == [12]byte(prefix) {
		otherPrefix[0] ^= 0xFF
	}

	otherHeader := rtps.DefaultHeader(otherPrefix)
	otherRaw := otherHeader.Encode()
	otherMsg := append([]byte(nil), otherRaw[:]...)

	if isSelfDatagram(otherMsg, local) {
		t.Fatalf("foreign header detected as self, want forwarded")
	}

	if isSelfDatagram([]byte("SPDP"), local) {
		t.Fatalf("legacy prefix detected as self, want forwarded to demux")
	}

	if isSelfDatagram([]byte{0x00, 0x01}, local) {
		t.Fatalf("short datagram detected as self, want forwarded to demux")
	}

	if isSelfDatagram(selfMsg, nil) {
		t.Fatalf("nil local detected as self, want forwarded")
	}
}

// TestBackendMulticastJoinCloseLifecycle checks additive group join plus teardown.
// It proves two backends share one standard port with reuse, the single demux
// snapshot sees both sockets, self loopback filters, and Close clears both
// paths idempotently without touching unicast behavior.
//
//nolint:gocognit,cyclop // test pins join, port, snapshot, filter, and close breadth in one run, splitting would obscure intent
func TestBackendMulticastJoinCloseLifecycle(t *testing.T) {
	const domain uint32 = 90

	first := newLiveBackend(t, domain, "mcast-a")
	defer func() {
		err := first.Close()
		if err != nil {
			t.Errorf("close first: %v", err)
		}
	}()

	checkMcastPort(t, first, domain)

	second := newLiveBackend(t, domain, "mcast-b")
	defer func() {
		err := second.Close()
		if err != nil {
			t.Errorf("close second: %v", err)
		}
	}()

	checkMcastPort(t, second, domain)

	first.mu.RLock()
	firstUni := first.uni
	firstUniPort := 0
	if firstUni != nil {
		firstUniPort = firstUni.LocalPort()
	}
	firstRx := first.mcastRx
	secondRxPort := 0
	second.mu.RLock()
	if second.mcastRx != nil {
		secondRxPort = second.mcastRx.Port()
	}
	secondUniPort := 0
	if second.uni != nil {
		secondUniPort = second.uni.LocalPort()
	}
	second.mu.RUnlock()
	first.mu.RUnlock()

	if firstRx == nil {
		t.Fatalf("first multicast receive is nil, want joined")
	}

	if secondRxPort != firstRx.Port() {
		t.Fatalf("group ports differ %d vs %d, want shared standard port", firstRx.Port(), secondRxPort)
	}

	if firstUniPort == 0 || secondUniPort == 0 {
		t.Fatalf("unicast ports %d %d, want nonzero", firstUniPort, secondUniPort)
	}

	if firstUniPort == secondUniPort {
		t.Fatalf("unicast ports collide on %d, want successive participants", firstUniPort)
	}

	_, _, _, _, _, ok := first.snapshotMcastIngress()
	if !ok {
		t.Fatalf("multicast snapshot not ready, want shared demux channels")
	}

	checkSelfFiltering(t, first)
	checkSelfFiltering(t, second)

	err := first.Close()
	if err != nil {
		t.Fatalf("close first: %v", err)
	}

	err = first.Close()
	if err != nil {
		t.Fatalf("second close first: %v", err)
	}

	first.mu.RLock()
	uni := first.uni
	mcast := first.mcast
	mcastRx := first.mcastRx
	mcastUserRx := first.mcastUserRx
	first.mu.RUnlock()

	if uni != nil || mcast != nil || mcastRx != nil || mcastUserRx != nil {
		t.Fatalf("close left uni %v mcast %v mcastRx %v mcastUserRx %v, want nil nil nil nil", uni != nil, mcast != nil, mcastRx != nil, mcastUserRx != nil)
	}

	if !first.Closed() {
		t.Fatalf("closed is false after Close")
	}

	err = second.Close()
	if err != nil {
		t.Fatalf("close second: %v", err)
	}

	err = second.Close()
	if err != nil {
		t.Fatalf("second close second: %v", err)
	}
}

// TestBackendMulticastSEDPInstallsWriter checks group SEDP reaches the demux.
// It sends one writer DATA only over multicast, which unicast-only mode never
// sees, and proves the shared ingest installs it as a remote endpoint.
//
//nolint:gocognit,cyclop // test pins peer setup, send, install, and take breadth in one run, splitting would obscure intent
func TestBackendMulticastSEDPInstallsWriter(t *testing.T) {
	const domain uint32 = 91

	backend := newLiveBackend(t, domain, "mcast-sedp")
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	checkMcastPort(t, backend, domain)

	if backend.HasRemoteEndpoint("/chatter") {
		t.Fatalf("remote endpoint present before multicast send, want absent")
	}

	var peerPrefix SEDPGUIDPrefix
	for i := range peerPrefix {
		peerPrefix[i] = 0xB0 + byte(i)
	}

	backend.mu.RLock()
	localPrefix := backend.local.GUID().Prefix
	backend.mu.RUnlock()

	matched := true
	for i := range peerPrefix {
		if peerPrefix[i] != localPrefix[i] {
			matched = false
			break
		}
	}

	if matched {
		peerPrefix[0] ^= 0xFF
	}

	peerIdx, err := NewSEDPIndex(peerPrefix, domain)
	if err != nil {
		t.Fatalf("peer index: %v", err)
	}

	ep, err := peerIdx.DeclareWriter("/chatter", "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}

	data, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	sender, err := rtps.DialMulticastSender(domain)
	if err != nil {
		t.Fatalf("dial sender: %v", err)
	}

	defer func() {
		err := sender.Close()
		if err != nil {
			t.Errorf("close sender: %v", err)
		}
	}()

	err = sender.Send(data)
	if err != nil {
		t.Fatalf("multicast send: %v", err)
	}

	waitForMcastRemote(t, backend, "/chatter")

	found := false

	for _, remote := range backend.sedp.RemoteEndpoints() {
		if remote.ROSTopic == "/chatter" && remote.ID == ep.ID {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("writer %s not in remotes after multicast SEDP", ep.ID.String())
	}

	takeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = backend.TakeBytes(takeCtx)
	if err == nil {
		t.Fatalf("take succeeded with only discovery, want timeout or closed")
	}

	if backend.demuxErrorCount() != 0 {
		t.Logf("demux errors=%d after multicast SEDP (informational)", backend.demuxErrorCount())
	}

	_ = transports.DiscoveryRangeSystemDefault
}

// TestBackendUserMulticastDeliversData checks user DATA fanout on the
// user-traffic group. CycloneDDS multicasts user DATA and heartbeats on
// the domain user port; without this join a participant depends entirely
// on per-reader unicast, and a second local participant starves while
// the first receives everything. Two backends on one domain must both
// see one user DATA datagram sent only to the group.
//
//nolint:gocognit,cyclop // test pins two-backend fanout with dial, send, and poll breadth in one run, splitting would obscure intent
func TestBackendUserMulticastDeliversData(t *testing.T) {
	t.Parallel()

	const domain uint32 = 92

	first := newLiveBackend(t, domain, "umcast-a")
	defer func() {
		err := first.Close()
		if err != nil {
			t.Errorf("close first: %v", err)
		}
	}()

	second := newLiveBackend(t, domain, "umcast-b")
	defer func() {
		err := second.Close()
		if err != nil {
			t.Errorf("close second: %v", err)
		}
	}()

	wantPort, err := rtps.UsertrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("user port: %v", err)
	}

	for name, backend := range map[string]*Backend{"first": first, "second": second} {
		backend.mu.RLock()
		userRx := backend.mcastUserRx
		backend.mu.RUnlock()

		if userRx == nil {
			t.Fatalf("%s user multicast receive is nil, want joined group", name)
		}

		if userRx.Port() != wantPort {
			t.Fatalf("%s user port = %d, want %d", name, userRx.Port(), wantPort)
		}
	}

	writer := rtps.EntityID{0x00, 0x00, 0x0a, 0x03}
	msg := buildDemuxMessage(t, writer, []byte("hello user multicast"))

	group := net.ParseIP(rtps.SPDPMulticastGroup)
	dst := &net.UDPAddr{IP: group, Port: wantPort}

	sender, err := net.DialUDP("udp4", nil, dst)
	if err != nil {
		t.Fatalf("dial user group: %v", err)
	}

	defer func() {
		err := sender.Close()
		if err != nil {
			t.Errorf("close sender: %v", err)
		}
	}()

	_, err = sender.Write(msg)
	if err != nil {
		t.Fatalf("send user DATA: %v", err)
	}

	for name, backend := range map[string]*Backend{"first": first, "second": second} {
		backend.mu.RLock()
		dataCh := backend.dataCh
		backend.mu.RUnlock()

		deadline := time.Now().Add(5 * time.Second)
		got := false

		for time.Now().Before(deadline) {
			select {
			case item := <-dataCh:
				var guid WriterGUID
				copy(guid[:12], item.guid[:12])

				if item.guid[12] == 0x00 && item.guid[13] == 0x00 && item.guid[14] == 0x0a && item.guid[15] == 0x03 {
					got = true
				}
			case <-time.After(50 * time.Millisecond):
			}

			if got {
				break
			}
		}

		if !got {
			t.Fatalf("%s backend missed user multicast DATA, want fanout", name)
		}
	}
}
