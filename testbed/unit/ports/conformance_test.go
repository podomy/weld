// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_ports holds port and bootstrap conformance tests.
//
// These tests run pure Go against public rtps and dds APIs only and assume
// already running containers without starting compose. They pin the DDSI
// port table, ephemeral versus standard binds, DialUnicast probing, the
// 32 target bootstrap probe shape, SPDP locator patching, and EADDRINUSE
// avoidance on shared host networking.
//
//nolint:cyclop // test package holds many sequential port table assertions, high average reflects breadth
package unit_ports

import (
	"bytes"
	"context"
	"net"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// conformDomains lists domains required by the conformance table.
var conformDomains = []uint32{0, 1, 42, 48, 101}

// conformParticipants lists participant ids required by the table.
var conformParticipants = []uint32{0, 1, 2, 3}

// basePortLocal replicates PB locally so the formula is pinned in the test.
const basePortLocal = 7400

// domainGainLocal replicates DG locally.
const domainGainLocal = 250

// participantGainLocal replicates PG locally.
const participantGainLocal = 2

// expectedBase returns B for a domain using the local replica.
func expectedBase(domain uint32) int {
	return basePortLocal + domainGainLocal*int(domain)
}

// checkPortHelpers verifies one domain and participant against the table.
func checkPortHelpers(t *testing.T, domain, participant uint32) {
	t.Helper()

	base := expectedBase(domain)
	wantMetaMC := base
	wantUserMC := base + 1
	wantMetaUC := base + 10 + participantGainLocal*int(participant)
	wantUserUC := base + 11 + participantGainLocal*int(participant)

	metaMC, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("domain %d meta multicast: %v", domain, err)
	}
	userMC, err := rtps.UsertrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("domain %d user multicast: %v", domain, err)
	}
	metaUC, err := rtps.MetatrafficUnicastPort(domain, participant)
	if err != nil {
		t.Fatalf("domain %d participant %d meta unicast: %v", domain, participant, err)
	}
	userUC, err := rtps.UsertrafficUnicastPort(domain, participant)
	if err != nil {
		t.Fatalf("domain %d participant %d user unicast: %v", domain, participant, err)
	}
	if metaMC != wantMetaMC {
		t.Fatalf("domain %d meta multicast = %d, want %d", domain, metaMC, wantMetaMC)
	}
	if userMC != wantUserMC {
		t.Fatalf("domain %d user multicast = %d, want %d", domain, userMC, wantUserMC)
	}
	if metaUC != wantMetaUC {
		t.Fatalf("domain %d participant %d meta unicast = %d, want %d", domain, participant, metaUC, wantMetaUC)
	}
	if userUC != wantUserUC {
		t.Fatalf("domain %d participant %d user unicast = %d, want %d", domain, participant, userUC, wantUserUC)
	}
}

// TestPortMappingFormula pins B and the multicast versus unicast split.
//
//nolint:gocognit,cyclop // port table test asserts many domain and participant cases in one run
func TestPortMappingFormula(t *testing.T) {
	t.Parallel()

	if rtps.BasePort != basePortLocal {
		t.Fatalf("BasePort = %d, want %d", rtps.BasePort, basePortLocal)
	}
	if rtps.DomainGain != domainGainLocal {
		t.Fatalf("DomainGain = %d, want %d", rtps.DomainGain, domainGainLocal)
	}
	if rtps.ParticipantGain != participantGainLocal {
		t.Fatalf("ParticipantGain = %d, want %d", rtps.ParticipantGain, participantGainLocal)
	}
	if rtps.MetatrafficMulticastOffset != 0 {
		t.Fatalf("meta multicast offset = %d, want 0", rtps.MetatrafficMulticastOffset)
	}
	if rtps.UsertrafficMulticastOffset != 1 {
		t.Fatalf("user multicast offset = %d, want 1", rtps.UsertrafficMulticastOffset)
	}
	if rtps.MetatrafficUnicastOffset != 10 {
		t.Fatalf("meta unicast offset = %d, want 10", rtps.MetatrafficUnicastOffset)
	}
	if rtps.UsertrafficUnicastOffset != 11 {
		t.Fatalf("user unicast offset = %d, want 11", rtps.UsertrafficUnicastOffset)
	}

	for _, domain := range conformDomains {
		for _, participant := range conformParticipants {
			checkPortHelpers(t, domain, participant)
		}
		spdpPort, err := dds.SPDPUnicastPort(domain)
		if err != nil {
			t.Fatalf("domain %d SPDP unicast: %v", domain, err)
		}
		metaUC, err := rtps.MetatrafficUnicastPort(domain, 0)
		if err != nil {
			t.Fatalf("domain %d meta unicast participant 0: %v", domain, err)
		}
		if int(spdpPort) != metaUC {
			t.Fatalf("domain %d SPDP unicast = %d, want meta unicast %d", domain, spdpPort, metaUC)
		}
	}

	for _, domain := range []uint32{102, 200} {
		_, err := rtps.MetatrafficMulticastPort(domain)
		if err == nil {
			t.Fatalf("meta multicast domain %d succeeded, want error", domain)
		}
		_, err = rtps.UsertrafficMulticastPort(domain)
		if err == nil {
			t.Fatalf("user multicast domain %d succeeded, want error", domain)
		}
		_, err = rtps.MetatrafficUnicastPort(domain, 0)
		if err == nil {
			t.Fatalf("meta unicast domain %d succeeded, want error", domain)
		}
		_, err = rtps.UsertrafficUnicastPort(domain, 0)
		if err == nil {
			t.Fatalf("user unicast domain %d succeeded, want error", domain)
		}
	}
}

// closeTransport closes a Transport and reports errors to the test.
func closeTransport(t *testing.T, tr *rtps.Transport) {
	t.Helper()

	t.Cleanup(func() {
		err := tr.Close()
		if err != nil {
			t.Errorf("close transport: %v", err)
		}
	})
}

// closeUnicast closes a UnicastConn and reports errors to the test.
func closeUnicast(t *testing.T, c *rtps.UnicastConn) {
	t.Helper()

	t.Cleanup(func() {
		err := c.Close()
		if err != nil {
			t.Errorf("close unicast: %v", err)
		}
	})
}

// closeMulticast closes a MulticastConn and reports errors to the test.
func closeMulticast(t *testing.T, c *rtps.MulticastConn) {
	t.Helper()

	t.Cleanup(func() {
		err := c.Close()
		if err != nil {
			t.Errorf("close multicast: %v", err)
		}
	})
}

// TestStandardVsEphemeralBinds covers zero port ephemeral binds and table binds.
//
//nolint:gocognit,cyclop // bind test asserts ephemeral plus table shapes in one run
func TestStandardVsEphemeralBinds(t *testing.T) {
	const ephemeralDomain uint32 = 46
	const standardDomain uint32 = 49
	const standardParticipant uint32 = 0

	ctx := context.Background()

	first, err := rtps.Join(ctx, rtps.Config{DomainID: ephemeralDomain})
	if err != nil {
		t.Fatalf("join ephemeral first: %v", err)
	}
	closeTransport(t, first)

	second, err := rtps.Join(ctx, rtps.Config{DomainID: ephemeralDomain})
	if err != nil {
		t.Fatalf("join ephemeral second: %v", err)
	}
	closeTransport(t, second)

	if first.MetaPort() == 0 {
		t.Fatalf("first meta port is zero, want ephemeral")
	}
	if first.UnicastPort() == 0 {
		t.Fatalf("first unicast port is zero, want ephemeral")
	}
	if second.MetaPort() == 0 {
		t.Fatalf("second meta port is zero, want ephemeral")
	}
	if second.UnicastPort() == 0 {
		t.Fatalf("second unicast port is zero, want ephemeral")
	}
	if first.MetaPort() == second.MetaPort() {
		t.Fatalf("ephemeral meta ports collide on %d", first.MetaPort())
	}
	if first.UnicastPort() == second.UnicastPort() {
		t.Fatalf("ephemeral unicast ports collide on %d", first.UnicastPort())
	}

	stdMeta, err := rtps.MetatrafficMulticastPort(standardDomain)
	if err != nil {
		t.Fatalf("standard meta port: %v", err)
	}
	stdUser, err := rtps.UsertrafficUnicastPort(standardDomain, standardParticipant)
	if err != nil {
		t.Fatalf("standard user port: %v", err)
	}

	joined, err := rtps.JoinStandard(ctx, standardDomain, standardParticipant)
	if err != nil {
		t.Fatalf("join standard: %v", err)
	}
	closeTransport(t, joined)

	if joined.MetaPort() != stdMeta {
		t.Fatalf("standard meta = %d, want %d", joined.MetaPort(), stdMeta)
	}
	if joined.UnicastPort() != stdUser {
		t.Fatalf("standard unicast = %d, want %d", joined.UnicastPort(), stdUser)
	}
	if joined.DomainID() != standardDomain {
		t.Fatalf("standard domain = %d, want %d", joined.DomainID(), standardDomain)
	}
	if joined.ParticipantID() != standardParticipant {
		t.Fatalf("standard participant = %d, want %d", joined.ParticipantID(), standardParticipant)
	}

	msg := []byte("standard-ephemeral-probe")
	deadline := time.Now().Add(2 * time.Second)
	err = joined.SetReadDeadline(deadline)
	if err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	err = joined.SendSPDP(msg)
	if err != nil {
		t.Fatalf("send spdp: %v", err)
	}
	buf := make([]byte, rtps.MaxDatagramSize)
	got, _, err := joined.ReceiveMeta(buf)
	if err != nil {
		t.Fatalf("receive meta: %v", err)
	}
	if !bytes.Equal(buf[:got], msg) {
		t.Fatalf("loopback bytes differ: %d of %d", got, len(msg))
	}
}

// TestDialUnicastProbesFirstFreeSlot checks successive dials land successive ports.
//
//nolint:gocognit,cyclop // dial test asserts successive port probing in one run
func TestDialUnicastProbesFirstFreeSlot(t *testing.T) {
	const domain uint32 = 50
	const dials = 4

	ctx := context.Background()
	conns := make([]*rtps.UnicastConn, 0, dials)
	for i := range dials {
		c, err := rtps.DialUnicast(ctx, domain)
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		conns = append(conns, c)
		t.Cleanup(func() {
			err := c.Close()
			if err != nil {
				t.Errorf("close dial: %v", err)
			}
		})
	}

	for i, c := range conns {
		want, err := rtps.UsertrafficUnicastPort(domain, c.Participant())
		if err != nil {
			t.Fatalf("dial %d expected port: %v", i, err)
		}
		if c.LocalPort() != want {
			t.Fatalf("dial %d port = %d, want table %d for participant %d", i, c.LocalPort(), want, c.Participant())
		}
		if c.Domain() != domain {
			t.Fatalf("dial %d domain = %d, want %d", i, c.Domain(), domain)
		}
		if i > 0 {
			prev := conns[i-1]
			if c.Participant() != prev.Participant()+1 {
				t.Fatalf("dial %d participant = %d, want %d", i, c.Participant(), prev.Participant()+1)
			}
			if c.LocalPort() != prev.LocalPort()+2 {
				t.Fatalf("dial %d port = %d, want %d", i, c.LocalPort(), prev.LocalPort()+2)
			}
		}
	}

	msg := []byte("probe-order-payload")
	sender := conns[0]
	receiver := conns[1]
	err := sender.SendTo(ctx, "127.0.0.1", receiver.LocalPort(), msg)
	if err != nil {
		t.Fatalf("send probe: %v", err)
	}
	recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	srcIP, srcPort, got, err := receiver.Receive(recvCtx)
	if err != nil {
		t.Fatalf("receive probe: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("probe bytes differ: %d of %d", len(got), len(msg))
	}
	if srcIP != "127.0.0.1" {
		t.Fatalf("src ip = %q, want 127.0.0.1", srcIP)
	}
	if srcPort != sender.LocalPort() {
		t.Fatalf("src port = %d, want %d", srcPort, sender.LocalPort())
	}
}

// localBootstrapPorts replicates spdpBootstrapPorts using only public helpers.
//
// It walks participants 0 through 15, appends meta unicast then user unicast,
// skips selfPort, dedupes, and caps at 32 entries.
func localBootstrapPorts(domain uint32, selfPort int) []int {
	out := make([]int, 0, 32)
	appendProbe := func(port int) {
		if port == selfPort {
			return
		}
		if slices.Contains(out, port) {
			return
		}
		out = append(out, port)
	}
	for i := range 16 {
		meta, err := rtps.MetatrafficUnicastPort(domain, uint32(i))
		if err != nil {
			continue
		}
		appendProbe(meta)
		user, err := rtps.UsertrafficUnicastPort(domain, uint32(i))
		if err != nil {
			continue
		}
		appendProbe(user)
	}
	return out
}

// checkBootstrapSet validates dedup, self skip, and max targets for one set.
func checkBootstrapSet(t *testing.T, domain uint32, selfPort int, ports []int) {
	t.Helper()

	if len(ports) > 32 {
		t.Fatalf("domain %d bootstrap len = %d, want max 32", domain, len(ports))
	}
	seen := make(map[int]struct{}, len(ports))
	for _, p := range ports {
		if p == selfPort {
			t.Fatalf("domain %d bootstrap contains self %d", domain, selfPort)
		}
		if _, ok := seen[p]; ok {
			t.Fatalf("domain %d bootstrap duplicates %d", domain, p)
		}
		seen[p] = struct{}{}
		if p <= 0 || p > 65535 {
			t.Fatalf("domain %d bootstrap port %d out of range", domain, p)
		}
	}
}

// TestBootstrapPortsProbeBothMetaUser covers the 0..15 meta plus user shape.
//
//nolint:gocognit,cyclop // bootstrap test asserts meta plus user shapes across participants
func TestBootstrapPortsProbeBothMetaUser(t *testing.T) {
	t.Parallel()

	for _, domain := range conformDomains {
		ports := localBootstrapPorts(domain, -1)
		if len(ports) != 32 {
			t.Fatalf("domain %d bootstrap len = %d, want 32", domain, len(ports))
		}
		checkBootstrapSet(t, domain, -1, ports)
		for i := range 16 {
			meta, err := rtps.MetatrafficUnicastPort(domain, uint32(i))
			if err != nil {
				t.Fatalf("domain %d participant %d meta: %v", domain, i, err)
			}
			user, err := rtps.UsertrafficUnicastPort(domain, uint32(i))
			if err != nil {
				t.Fatalf("domain %d participant %d user: %v", domain, i, err)
			}
			if !slices.Contains(ports, meta) {
				t.Fatalf("domain %d missing meta participant %d port %d", domain, i, meta)
			}
			if !slices.Contains(ports, user) {
				t.Fatalf("domain %d missing user participant %d port %d", domain, i, user)
			}
			if meta == user {
				t.Fatalf("domain %d participant %d meta equals user %d", domain, i, meta)
			}
		}
	}

	const liveDomain uint32 = 51
	ctx := context.Background()
	live, err := rtps.DialUnicast(ctx, liveDomain)
	if err != nil {
		t.Fatalf("dial live: %v", err)
	}
	defer func() {
		err := live.Close()
		if err != nil {
			t.Errorf("close live: %v", err)
		}
	}()

	selfPort := live.LocalPort()
	ports := localBootstrapPorts(liveDomain, selfPort)
	if len(ports) != 31 {
		t.Fatalf("live bootstrap len = %d, want 31 when self %d is probed", len(ports), selfPort)
	}
	checkBootstrapSet(t, liveDomain, selfPort, ports)

	for i := range 16 {
		meta, err := rtps.MetatrafficUnicastPort(liveDomain, uint32(i))
		if err != nil {
			t.Fatalf("live meta %d: %v", i, err)
		}
		skipped := localBootstrapPorts(liveDomain, meta)
		if slices.Contains(skipped, meta) {
			t.Fatalf("self meta %d not skipped", meta)
		}
		if len(skipped) != 31 {
			t.Fatalf("self meta %d len = %d, want 31", meta, len(skipped))
		}
		user, err := rtps.UsertrafficUnicastPort(liveDomain, uint32(i))
		if err != nil {
			t.Fatalf("live user %d: %v", i, err)
		}
		skippedUser := localBootstrapPorts(liveDomain, user)
		if slices.Contains(skippedUser, user) {
			t.Fatalf("self user %d not skipped", user)
		}
		if len(skippedUser) != 31 {
			t.Fatalf("self user %d len = %d, want 31", user, len(skippedUser))
		}
	}

	ephemeral := localBootstrapPorts(liveDomain, 0)
	if len(ephemeral) != 32 {
		t.Fatalf("ephemeral self len = %d, want 32", len(ephemeral))
	}
	checkBootstrapSet(t, liveDomain, 0, ephemeral)
}

// testGUIDPrefix builds a deterministic non zero GUID prefix for SPDP tests.
func testGUIDPrefix(fill byte) [12]byte {
	var prefix [12]byte
	for i := range prefix {
		prefix[i] = fill + byte(i)
	}
	if prefix[0] == 0 {
		prefix[0] = 0x5A
	}
	return prefix
}

// findLocatorParams extracts locator values for one PID set from decoded params.
func findLocatorParams(params []rtps.Param, ids map[uint16]bool) [][]byte {
	var out [][]byte
	for _, p := range params {
		if ids[p.ID] {
			out = append(out, p.Value)
		}
	}
	return out
}

// decodeSPDPParams decodes the SPDP DATA payload params using public helpers.
func decodeSPDPParams(t *testing.T, raw []byte) []rtps.Param {
	t.Helper()

	var drops rtps.Counter
	_, raws, err := rtps.DecodeMessage(raw, &drops)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	for _, r := range raws {
		sub, err := rtps.DecodeSubmessage(r, &drops)
		if err != nil {
			t.Fatalf("decode submessage: %v", err)
		}
		if sub.Data == nil {
			continue
		}
		if sub.Data.WriterID != rtps.EntitySPDPWriter {
			continue
		}
		payload := sub.Data.Payload
		if len(payload) < 4 {
			t.Fatalf("payload need 4 have %d", len(payload))
		}
		params, err := rtps.DecodeParams(payload[4:])
		if err != nil {
			t.Fatalf("decode params: %v", err)
		}
		return params
	}
	t.Fatalf("no SPDP DATA found")

	return nil
}

// TestPatchStructurePreservesIPAndMulticast checks locator separation in templates.
//
//nolint:gocognit,cyclop // patch test asserts locator separation across templates in one run
func TestPatchStructurePreservesIPAndMulticast(t *testing.T) {
	t.Parallel()

	const domain uint32 = 42
	prefix := testGUIDPrefix(0x5B)
	guid := dds.GUID{}
	copy(guid.Prefix[:], prefix[:])
	guid.EntityID = [4]byte{0x00, 0x00, 0x01, 0xC1}

	packet, err := dds.MarshalAlivePacket(guid, domain, 1, 10*time.Second)
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}

	ann, err := dds.ParseAnnouncement(packet)
	if err != nil {
		t.Fatalf("parse announcement: %v", err)
	}
	if !ann.Alive {
		t.Fatalf("announcement not alive")
	}
	if ann.DomainID != domain {
		t.Fatalf("domain = %d, want %d", ann.DomainID, domain)
	}
	if len(ann.Ports) != 2 {
		t.Fatalf("unicast ports = %v, want 2 entries", ann.Ports)
	}

	metaUC, err := rtps.MetatrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("meta unicast: %v", err)
	}
	userUC, err := rtps.UsertrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("user unicast: %v", err)
	}
	if int(ann.Ports[0]) != metaUC || int(ann.Ports[1]) != userUC {
		t.Fatalf("ann ports = %v, want [%d %d]", ann.Ports, metaUC, userUC)
	}

	params := decodeSPDPParams(t, packet)

	unicastIDs := map[uint16]bool{
		rtps.PIDMetatrafficUnicastLocator:     true,
		rtps.PIDDefaultUnicastLocatorStandard: true,
		rtps.PIDDefaultUnicastLocator:         true,
	}
	multicastIDs := map[uint16]bool{
		rtps.PIDMetatrafficMulticastLocator: true,
		rtps.PIDDefaultMulticastLocator:     true,
	}

	unicastVals := findLocatorParams(params, unicastIDs)
	multicastVals := findLocatorParams(params, multicastIDs)

	if len(unicastVals) != 2 {
		t.Fatalf("unicast locators = %d, want 2", len(unicastVals))
	}
	if len(multicastVals) != 2 {
		t.Fatalf("multicast locators = %d, want 2", len(multicastVals))
	}

	base := expectedBase(domain)
	wantMetaMC := base
	wantUserMC := base + 1
	advertised := rtps.AdvertiseIPv4()
	group := net.ParseIP(rtps.SPDPMulticastGroup)

	for i, raw := range unicastVals {
		kind, port, ip, err := rtps.DecodeLocator(raw)
		if err != nil {
			t.Fatalf("decode unicast %d: %v", i, err)
		}
		if kind != rtps.LocatorKindUDPv4 {
			t.Fatalf("unicast %d kind = %d, want UDPv4", i, kind)
		}
		if !ip.Equal(advertised) {
			t.Fatalf("unicast %d ip = %s, want %s", i, ip.String(), advertised.String())
		}
		_ = port
	}
	for i, raw := range multicastVals {
		kind, port, ip, err := rtps.DecodeLocator(raw)
		if err != nil {
			t.Fatalf("decode multicast %d: %v", i, err)
		}
		if kind != rtps.LocatorKindUDPv4 {
			t.Fatalf("multicast %d kind = %d, want UDPv4", i, kind)
		}
		if !ip.Equal(group) {
			t.Fatalf("multicast %d ip = %s, want %s", i, ip.String(), group.String())
		}
		if !slices.Contains([]int{wantMetaMC, wantUserMC}, int(port)) {
			t.Fatalf("multicast %d port = %d, want %d or %d", i, port, wantMetaMC, wantUserMC)
		}
	}

	kind0, port0, ip0, err := rtps.DecodeLocator(unicastVals[0])
	if err != nil {
		t.Fatalf("decode unicast 0: %v", err)
	}
	kind1, port1, ip1, err := rtps.DecodeLocator(unicastVals[1])
	if err != nil {
		t.Fatalf("decode unicast 1: %v", err)
	}
	if kind0 != rtps.LocatorKindUDPv4 || kind1 != rtps.LocatorKindUDPv4 {
		t.Fatalf("unicast kinds = %d %d, want UDPv4", kind0, kind1)
	}
	if !ip0.Equal(advertised) || !ip1.Equal(advertised) {
		t.Fatalf("unicast ips = %s %s, want %s", ip0.String(), ip1.String(), advertised.String())
	}
	if int(port0) != metaUC {
		t.Fatalf("unicast 0 port = %d, want %d", port0, metaUC)
	}
	if int(port1) != userUC {
		t.Fatalf("unicast 1 port = %d, want %d", port1, userUC)
	}
}

// TestBootstrapLivePatchAndProbe exercises the real patched bootstrap path.
//
//nolint:gocognit,cyclop // live patch test asserts real bootstrap path across many steps
func TestBootstrapLivePatchAndProbe(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "")

	const domain uint32 = 52

	ctx := context.Background()
	probe, err := rtps.DialUnicast(ctx, domain)
	if err != nil {
		t.Fatalf("dial probe: %v", err)
	}
	defer func() {
		err := probe.Close()
		if err != nil {
			t.Errorf("close probe: %v", err)
		}
	}()

	metaPort, err := rtps.MetatrafficUnicastPort(domain, 7)
	if err != nil {
		t.Fatalf("meta port: %v", err)
	}
	metaAddr, err := net.ResolveUDPAddr("udp4", net.JoinHostPort("0.0.0.0", strconv.Itoa(metaPort)))
	if err != nil {
		t.Fatalf("resolve meta: %v", err)
	}
	metaConn, err := net.ListenUDP("udp4", metaAddr)
	if err != nil {
		t.Fatalf("listen meta %d: %v", metaPort, err)
	}
	defer func() {
		err := metaConn.Close()
		if err != nil {
			t.Errorf("close meta: %v", err)
		}
	}()

	backend, err := dds.NewBackend(dds.SidecarConfig{DomainID: domain, PeerAddress: "127.0.0.1"})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}
	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close backend: %v", err)
		}
	}()

	joinCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err = backend.JoinDiscovery(joinCtx, transports.DiscoveryInfo{ParticipantName: "testbed-bootstrap"})
	if err != nil {
		t.Fatalf("join discovery: %v", err)
	}

	recvCtx, recvCancel := context.WithTimeout(ctx, 6*time.Second)
	defer recvCancel()
	srcIP, _, got, err := probe.Receive(recvCtx)
	if err != nil {
		t.Fatalf("probe receive: %v", err)
	}
	if srcIP != "127.0.0.1" {
		t.Fatalf("probe src ip = %q, want 127.0.0.1", srcIP)
	}

	ann, err := dds.ParseAnnouncement(got)
	if err != nil {
		t.Fatalf("parse probe announcement: %v", err)
	}
	if !ann.Alive {
		t.Fatalf("probe announcement not alive")
	}
	if ann.DomainID != domain {
		t.Fatalf("probe domain = %d, want %d", ann.DomainID, domain)
	}
	if len(ann.Ports) != 2 {
		t.Fatalf("probe ports = %v, want 2 entries", ann.Ports)
	}
	if ann.Ports[0] != ann.Ports[1] {
		t.Fatalf("patched ports differ %v, want both equal live port", ann.Ports)
	}

	livePort := int(ann.Ports[0])
	if livePort == probe.LocalPort() {
		t.Fatalf("live port %d equals probe port, want distinct backend port", livePort)
	}
	found := false
	for i := range 16 {
		candidate, err := rtps.UsertrafficUnicastPort(domain, uint32(i))
		if err != nil {
			t.Fatalf("candidate user %d: %v", i, err)
		}
		if candidate == livePort {
			found = true
		}
	}
	if !found {
		t.Fatalf("live port %d not in user table for 0..15", livePort)
	}
	probeSet := localBootstrapPorts(domain, livePort)
	if slices.Contains(probeSet, livePort) {
		t.Fatalf("bootstrap set contains backend live %d, want skipped", livePort)
	}
	if !slices.Contains(probeSet, probe.LocalPort()) {
		t.Fatalf("bootstrap set missing probe port %d, want probed", probe.LocalPort())
	}
	if !slices.Contains(probeSet, metaPort) {
		t.Fatalf("bootstrap set missing meta port %d, want probed", metaPort)
	}

	params := decodeSPDPParams(t, got)
	advertised := rtps.AdvertiseIPv4()
	group := net.ParseIP(rtps.SPDPMulticastGroup)
	unicastIDs := map[uint16]bool{
		rtps.PIDMetatrafficUnicastLocator:     true,
		rtps.PIDDefaultUnicastLocatorStandard: true,
		rtps.PIDDefaultUnicastLocator:         true,
	}
	multicastIDs := map[uint16]bool{
		rtps.PIDMetatrafficMulticastLocator: true,
		rtps.PIDDefaultMulticastLocator:     true,
	}
	unicastVals := findLocatorParams(params, unicastIDs)
	multicastVals := findLocatorParams(params, multicastIDs)
	if len(unicastVals) != 2 {
		t.Fatalf("live unicast locators = %d, want 2", len(unicastVals))
	}
	if len(multicastVals) != 2 {
		t.Fatalf("live multicast locators = %d, want 2", len(multicastVals))
	}
	for i, raw := range unicastVals {
		kind, port, ip, err := rtps.DecodeLocator(raw)
		if err != nil {
			t.Fatalf("live unicast %d: %v", i, err)
		}
		if kind != rtps.LocatorKindUDPv4 {
			t.Fatalf("live unicast %d kind = %d, want UDPv4", i, kind)
		}
		if int(port) != livePort {
			t.Fatalf("live unicast %d port = %d, want live %d", i, port, livePort)
		}
		if !ip.Equal(advertised) {
			t.Fatalf("live unicast %d ip = %s, want %s", i, ip.String(), advertised.String())
		}
	}
	base := expectedBase(domain)
	for _, raw := range multicastVals {
		kind, port, ip, err := rtps.DecodeLocator(raw)
		if err != nil {
			t.Fatalf("live multicast decode: %v", err)
		}
		if kind != rtps.LocatorKindUDPv4 {
			t.Fatalf("live multicast kind = %d, want UDPv4", kind)
		}
		if !ip.Equal(group) {
			t.Fatalf("live multicast ip = %s, want %s", ip.String(), group.String())
		}
		if int(port) != base && int(port) != base+1 {
			t.Fatalf("live multicast port = %d, want %d or %d", port, base, base+1)
		}
	}

	err = metaConn.SetReadDeadline(time.Now().Add(6 * time.Second))
	if err != nil {
		t.Fatalf("meta deadline: %v", err)
	}
	metaBuf := make([]byte, rtps.MaxDatagramSize)
	n, _, err := metaConn.ReadFromUDP(metaBuf)
	if err != nil {
		t.Fatalf("meta receive: %v", err)
	}
	metaAnn, err := dds.ParseAnnouncement(metaBuf[:n])
	if err != nil {
		t.Fatalf("parse meta announcement: %v", err)
	}
	if !metaAnn.Alive {
		t.Fatalf("meta announcement not alive")
	}
	if metaAnn.DomainID != domain {
		t.Fatalf("meta domain = %d, want %d", metaAnn.DomainID, domain)
	}
	if len(metaAnn.Ports) != 2 || int(metaAnn.Ports[0]) != livePort {
		t.Fatalf("meta ports = %v, want both %d", metaAnn.Ports, livePort)
	}
}

// TestEADDRINUSEAvoidedViaWildcardProbe checks sharing on host networking.
//
//nolint:gocognit,cyclop // wildcard test asserts sharing across many bind shapes in one run
func TestEADDRINUSEAvoidedViaWildcardProbe(t *testing.T) {
	ctx := context.Background()

	const reuseDomain uint32 = 53
	firstMC, err := rtps.JoinMulticast(ctx, reuseDomain)
	if err != nil {
		t.Fatalf("join multicast first domain %d: %v", reuseDomain, err)
	}
	closeMulticast(t, firstMC)
	secondMC, err := rtps.JoinMulticast(ctx, reuseDomain)
	if err != nil {
		t.Fatalf("join multicast second domain %d: %v", reuseDomain, err)
	}
	closeMulticast(t, secondMC)
	wantMC, err := rtps.MetatrafficMulticastPort(reuseDomain)
	if err != nil {
		t.Fatalf("expected multicast: %v", err)
	}
	if firstMC.Port() != wantMC || secondMC.Port() != wantMC {
		t.Fatalf("multicast ports = %d %d, want %d", firstMC.Port(), secondMC.Port(), wantMC)
	}
	msg := []byte("reuse-probe")
	err = firstMC.Send(msg)
	if err != nil {
		t.Fatalf("multicast send: %v", err)
	}
	recvCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	got, err := secondMC.Receive(recvCtx)
	if err != nil {
		t.Fatalf("multicast receive: %v", err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("multicast bytes differ: %d of %d", len(got), len(msg))
	}

	const probeDomain uint32 = 54
	firstUni, err := rtps.DialUnicast(ctx, probeDomain)
	if err != nil {
		t.Fatalf("dial first: %v", err)
	}
	closeUnicast(t, firstUni)
	secondUni, err := rtps.DialUnicast(ctx, probeDomain)
	if err != nil {
		t.Fatalf("dial second: %v", err)
	}
	closeUnicast(t, secondUni)
	if firstUni.LocalPort() == secondUni.LocalPort() {
		t.Fatalf("unicast probe ports collide on %d", firstUni.LocalPort())
	}
	if secondUni.Participant() != firstUni.Participant()+1 {
		t.Fatalf("participants = %d %d, want successive", firstUni.Participant(), secondUni.Participant())
	}
	addr := firstUni.LocalAddr()
	advertised := rtps.AdvertiseIPv4()
	if addr == nil || addr.Port != firstUni.LocalPort() {
		t.Fatalf("local addr = %v, want port %d", addr, firstUni.LocalPort())
	}
	if !addr.IP.Equal(advertised) {
		t.Fatalf("local ip = %s, want advertised %s", addr.IP.String(), advertised.String())
	}

	blockedAddr, err := net.ResolveUDPAddr("udp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(firstUni.LocalPort())))
	if err != nil {
		t.Fatalf("resolve blocked: %v", err)
	}
	blocked, err := net.ListenUDP("udp4", blockedAddr)
	if err == nil {
		err = blocked.Close()
		if err != nil {
			t.Errorf("close blocked: %v", err)
		}
		t.Fatalf("direct bind to probed port %d succeeded, want EADDRINUSE", firstUni.LocalPort())
	}

	const senderDomain uint32 = 56
	senders := make([]*rtps.MulticastSender, 0, 3)
	for i := range 3 {
		s, err := rtps.DialMulticastSender(senderDomain)
		if err != nil {
			t.Fatalf("dial sender %d: %v", i, err)
		}
		senders = append(senders, s)
		t.Cleanup(func() {
			err := s.Close()
			if err != nil {
				t.Errorf("close sender: %v", err)
			}
		})
	}
	wantSender, err := rtps.MetatrafficMulticastPort(senderDomain)
	if err != nil {
		t.Fatalf("sender port: %v", err)
	}
	for i, s := range senders {
		if s.Port() != wantSender {
			t.Fatalf("sender %d port = %d, want %d", i, s.Port(), wantSender)
		}
		err = s.Send(msg)
		if err != nil {
			t.Fatalf("sender %d send: %v", i, err)
		}
	}
}
