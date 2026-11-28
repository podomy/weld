// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// TestNormalizePeerHostCases pins host validation for unicast fanout.
func TestNormalizePeerHostCases(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		raw  string
		want string
	}{
		{name: "empty", raw: "", want: ""},
		{name: "spaces", raw: "   ", want: ""},
		{name: "loopback", raw: "127.0.0.1", want: ""},
		{name: "loopback padded", raw: "  127.0.0.1  ", want: ""},
		{name: "localhost", raw: "localhost", want: ""},
		{name: "ipv6 loopback", raw: "::1", want: ""},
		{name: "remote", raw: "192.168.1.10", want: "192.168.1.10"},
		{name: "remote padded", raw: "  192.168.1.10  ", want: "192.168.1.10"},
		{name: "test net", raw: "192.0.2.1", want: "192.0.2.1"},
		{name: "hostname", raw: "peer.local", want: ""},
		{name: "host port", raw: "192.168.1.10:7411", want: "192.168.1.10"},
		{name: "loopback port", raw: "127.0.0.1:7411", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := normalizePeerHost(tc.raw)
			if got != tc.want {
				t.Fatalf("normalizePeerHost(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

// TestInitialPeerHostSeeded pins that a configured remote peer seeds fanout.
func TestInitialPeerHostSeeded(t *testing.T) {
	t.Parallel()

	plain := newTestBackend(t, PeerConfig{DomainID: 0, PeerAddress: DefaultPeerAddress})
	if hosts := plain.snapshotPeerHosts(); len(hosts) != 0 {
		t.Fatalf("default hosts = %v, want empty", hosts)
	}

	remote := newTestBackend(t, PeerConfig{DomainID: 0, PeerAddress: "192.0.2.1"})
	hosts := remote.snapshotPeerHosts()
	if !slices.Contains(hosts, "192.0.2.1") {
		t.Fatalf("remote hosts = %v, want 192.0.2.1", hosts)
	}
}

// TestNotePeerHostFiltersAndCaps pins loopback filtering and the host cap.
func TestNotePeerHostFiltersAndCaps(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t, PeerConfig{DomainID: 0, PeerAddress: DefaultPeerAddress})
	backend.notePeerHost("127.0.0.1")
	backend.notePeerHost("")
	backend.notePeerHost("not-a-host")
	if hosts := backend.snapshotPeerHosts(); len(hosts) != 0 {
		t.Fatalf("filtered hosts = %v, want empty", hosts)
	}

	backend.notePeerHost("192.0.2.1")
	backend.notePeerHost("192.0.2.1")
	if hosts := backend.snapshotPeerHosts(); len(hosts) != 1 {
		t.Fatalf("dup hosts = %v, want one entry", hosts)
	}

	for i := 1; i <= maxPeerHosts+10; i++ {
		backend.notePeerHost(fmt.Sprintf("198.51.100.%d", i))
	}
	if hosts := backend.snapshotPeerHosts(); len(hosts) != maxPeerHosts {
		t.Fatalf("capped hosts = %d, want %d", len(hosts), maxPeerHosts)
	}
}

// TestAnnouncementHostsCarryAdvertiseIP pins SPDP locator host parsing.
func TestAnnouncementHostsCarryAdvertiseIP(t *testing.T) {
	t.Parallel()

	clearBackendEnv(t)

	local, err := NewLocalParticipant(0, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local participant: %v", err)
	}

	data, err := MarshalAlivePacket(local.GUID(), 0, 1, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}

	ann, err := ParseAnnouncement(data)
	if err != nil {
		t.Fatalf("parse announcement: %v", err)
	}

	if len(ann.Ports) == 0 {
		t.Fatal("announcement carries no ports")
	}

	if len(ann.Hosts) == 0 {
		t.Fatal("announcement carries no hosts")
	}

	want := rtps.AdvertiseIPv4().String()
	for _, host := range ann.Hosts {
		if host != want {
			t.Fatalf("ann host = %q, want %q", host, want)
		}
	}
}

// TestUnroutablePeerPreservesLoopbackDiscovery pins that an unreachable
// configured peer does not break same host discovery.
func TestUnroutablePeerPreservesLoopbackDiscovery(t *testing.T) {
	t.Parallel()

	a := newTestBackend(t, PeerConfig{DomainID: 0, PeerAddress: "192.0.2.1"})
	b := newTestBackend(t, PeerConfig{DomainID: 0, PeerAddress: DefaultPeerAddress})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := a.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "peer-a"})
	if err != nil {
		t.Fatalf("a join: %v", err)
	}
	defer func() {
		err := a.Close()
		if err != nil {
			t.Errorf("close a: %v", err)
		}
	}()

	err = b.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "peer-b"})
	if err != nil {
		t.Fatalf("b join: %v", err)
	}
	defer func() {
		err := b.Close()
		if err != nil {
			t.Errorf("close b: %v", err)
		}
	}()

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if len(a.snapshotRoutes()) != 0 && len(b.snapshotRoutes()) != 0 {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("routes a=%v b=%v, want both non-empty", a.snapshotRoutes(), b.snapshotRoutes())
}
