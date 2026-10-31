// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package gates_test

import (
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// gate0PortCase pins the well-known ports for one gate domain.
type gate0PortCase struct {
	domain  uint32
	metaMC  int
	userMC  int
	metaUni int
	userUni int
}

// TestGate0PortsAndSPDP is the offline unit gate.
//
// It runs with plain go test with no container and no daemon. It pins the
// SPDP and user ports for the gate domains 42, 47, and 48, the multicast
// group, a Marshal and Parse round trip on one allocated exclusive domain,
// and domain range handling. The round trip uses explicit domain plumbing
// with no process env, so it is safe for future t.Parallel workers.
// It is gate 0 of the Go gates and does not shell out to gofumpt or
// golangci-lint.
//
//nolint:gocognit,cyclop // test sequences many assertions, splitting would obscure intent
func TestGate0PortsAndSPDP(t *testing.T) {
	cases := []gate0PortCase{
		{domain: 42, metaMC: 17900, userMC: 17901, metaUni: 17910, userUni: 17911},
		{domain: 47, metaMC: 19150, userMC: 19151, metaUni: 19160, userUni: 19161},
		{domain: 48, metaMC: 19400, userMC: 19401, metaUni: 19410, userUni: 19411},
	}
	for _, tc := range cases {
		got, err := rtps.MetatrafficMulticastPort(tc.domain)
		if err != nil {
			t.Fatalf("domain %d multicast meta: %v", tc.domain, err)
		}
		if got != tc.metaMC {
			t.Fatalf("domain %d multicast meta = %d, want %d", tc.domain, got, tc.metaMC)
		}
		got, err = rtps.UsertrafficMulticastPort(tc.domain)
		if err != nil {
			t.Fatalf("domain %d multicast user: %v", tc.domain, err)
		}
		if got != tc.userMC {
			t.Fatalf("domain %d multicast user = %d, want %d", tc.domain, got, tc.userMC)
		}
		got, err = rtps.MetatrafficUnicastPort(tc.domain, 0)
		if err != nil {
			t.Fatalf("domain %d unicast meta: %v", tc.domain, err)
		}
		if got != tc.metaUni {
			t.Fatalf("domain %d unicast meta = %d, want %d", tc.domain, got, tc.metaUni)
		}
		got, err = rtps.UsertrafficUnicastPort(tc.domain, 0)
		if err != nil {
			t.Fatalf("domain %d unicast user: %v", tc.domain, err)
		}
		if got != tc.userUni {
			t.Fatalf("domain %d unicast user = %d, want %d", tc.domain, got, tc.userUni)
		}
	}
	if rtps.SPDPMulticastGroup != "239.255.0.1" {
		t.Fatalf("group = %q, want 239.255.0.1", rtps.SPDPMulticastGroup)
	}
	group := net.ParseIP(rtps.SPDPMulticastGroup)
	if group == nil {
		t.Fatalf("group parse failed")
	}
	if !group.IsMulticast() {
		t.Fatalf("group %v is not multicast", group)
	}
	domain := helpers.MustAllocateDomain(t)
	err := helpers.ValidateDomain(domain)
	if err != nil {
		t.Fatalf("allocated domain valid rejected: %v", err)
	}
	err = helpers.ValidateDomain(102)
	if err == nil {
		t.Fatalf("domain 102 accepted, want rejection above maximum")
	}
	err = helpers.ValidateDomain(-1)
	if err == nil {
		t.Fatalf("domain -1 accepted, want rejection")
	}
	_, err = transports.ResolveDomainID(999)
	if err == nil {
		t.Fatalf("caller 999 accepted, want rejection above maximum")
	}
	env := helpers.EnvWithDomain(domain)
	wantEnv := transports.DomainEnvVar + "=" + strconv.Itoa(domain)
	if !slices.Contains(env, wantEnv) {
		t.Fatalf("explicit env misses %q", wantEnv)
	}
	observer, err := dds.NewLocalParticipant(uint32(domain), 10*time.Second) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	if err != nil {
		t.Fatalf("new observer: %v", err)
	}
	announcer, err := dds.NewLocalParticipant(uint32(domain), 10*time.Second) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	if err != nil {
		t.Fatalf("new announcer: %v", err)
	}
	if observer.GUID().Prefix == announcer.GUID().Prefix {
		t.Fatalf("observer and announcer prefixes match, want distinct random prefixes")
	}
	packet, err := dds.MarshalAlivePacket(announcer.GUID(), uint32(domain), 1, announcer.LeaseDuration()) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	ann, err := dds.ParseAnnouncement(packet)
	if err != nil {
		t.Fatalf("parse self: %v", err)
	}
	if ann.DomainID != uint32(domain) { //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
		t.Fatalf("self domain = %d, want %d", ann.DomainID, domain)
	}
	if !ann.Alive {
		t.Fatalf("self parsed as dispose")
	}
	if ann.Lease <= 0 {
		t.Fatalf("self lease = %v, want positive", ann.Lease)
	}
	if len(ann.Ports) == 0 {
		t.Fatalf("self ports empty")
	}
	if ann.GUID.Prefix != announcer.GUID().Prefix {
		t.Fatalf("self GUID mismatch")
	}
	helpers.CheckWire(t, packet, domain, "gate0")
}

// TestGate0LaunchCorpusPresent pins the launch-argument gate files.
//
// It stats the launch conformance test plus one fixture plus one expected
// file. It performs no execution and runs offline with plain go test.
func TestGate0LaunchCorpusPresent(t *testing.T) {
	root := helpers.RepoRoot(t)
	files := []string{
		filepath.Join(root, "testbed", "unit", "launch", "conformance_test.go"),
		filepath.Join(root, "testbed", "unit", "launch", "shared_nodes.yaml"),
		filepath.Join(root, "testbed", "unit", "launch", "shared_nav.expected"),
	}
	for _, path := range files {
		_, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
	}
}
