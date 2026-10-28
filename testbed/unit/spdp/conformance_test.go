// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop // spdp package holds sequential conformance assertions, high average reflects breadth
package spdp_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// conformDomains lists domains required for port and wire checks.
var conformDomains = []uint32{0, 1, 42, 48, 101}

// conformParticipants lists participant ids for gain checks.
var conformParticipants = []uint32{0, 1, 2, 3}

// basePortLocal mirrors rtps.BasePort for formula pinning.
const basePortLocal = 7400

// domainGainLocal mirrors rtps.DomainGain.
const domainGainLocal = 250

// participantGainLocal mirrors rtps.ParticipantGain.
const participantGainLocal = 2

// domainTagPID is the domain tag parameter id on the wire.
const domainTagPID = 0x000f

// userDataPID is the UserData parameter id.
const userDataPID = 0x002c

// builtinEndpointPID is the builtin endpoint set parameter id.
const builtinEndpointPID = 0x0058

// leasePID is the lease duration parameter id.
const leasePID = 0x0002

// expectedBuiltinSet is the participant plus publication plus subscription mask.
const expectedBuiltinSet = uint32(0x3F)

// expectedLease is the lease pinned for alive packets.
const expectedLease = 10 * time.Second

// expectedBase returns B for a domain.
func expectedBase(domain uint32) int {
	return basePortLocal + domainGainLocal*int(domain)
}

// conformanceSender records one packet for Announce ordering checks.
type conformanceSender struct {
	last []byte
}

// Send records one packet.
func (s *conformanceSender) Send(_ context.Context, data []byte) error {
	s.last = append([]byte(nil), data...)
	return nil
}

// testGUID builds a deterministic non zero participant GUID.
func testGUID(fill byte) dds.GUID {
	var prefix dds.GUIDPrefix
	for i := range prefix {
		prefix[i] = fill
	}
	if prefix[0] == 0x00 {
		prefix[0] = 0x01
	}
	return dds.GUID{Prefix: prefix, EntityID: [4]byte{0x00, 0x00, 0x01, 0xC1}}
}

// mustMarshalAlive marshals an alive packet or fails the test.
func mustMarshalAlive(t *testing.T, guid dds.GUID, domain uint32, seq uint64, lease time.Duration) []byte { //nolint:unparam // lease is pinned to 10s for conformance
	t.Helper()
	data, err := dds.MarshalAlivePacket(guid, domain, seq, lease)
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	return data
}

// mustMarshalDispose marshals a dispose packet or fails the test.
func mustMarshalDispose(t *testing.T, guid dds.GUID, domain uint32, seq uint64) []byte {
	t.Helper()
	data, err := dds.MarshalDisposePacket(guid, domain, seq)
	if err != nil {
		t.Fatalf("marshal dispose: %v", err)
	}
	return data
}

// mustParse parses an announcement or fails the test.
func mustParse(t *testing.T, raw []byte) dds.Announcement {
	t.Helper()
	ann, err := dds.ParseAnnouncement(raw)
	if err != nil {
		t.Fatalf("parse announcement: %v", err)
	}
	return ann
}

// decodeSPDPData decodes one datagram and returns the SPDP DATA.
func decodeSPDPData(t *testing.T, raw []byte) (rtps.Header, rtps.Data) {
	t.Helper()
	var drops rtps.Counter
	hdr, raws, err := rtps.DecodeMessage(raw, &drops)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	for i := range raws {
		sub, err := rtps.DecodeSubmessage(raws[i], &drops)
		if err != nil {
			t.Fatalf("decode submessage %d: %v", i, err)
		}
		if sub.Data == nil {
			continue
		}
		if sub.Data.WriterID != rtps.EntitySPDPWriter {
			continue
		}
		return hdr, *sub.Data
	}
	t.Fatalf("no SPDP DATA in datagram")
	return rtps.Header{}, rtps.Data{}
}

// decodePayloadParams decodes SPDP payload params with the public codec.
func decodePayloadParams(t *testing.T, d rtps.Data) []rtps.Param {
	t.Helper()
	payload := d.Payload
	if len(payload) < 4 {
		t.Fatalf("payload need 4 have %d", len(payload))
	}
	params, err := rtps.DecodeParams(payload[4:])
	if err != nil {
		t.Fatalf("decode payload params: %v", err)
	}
	return params
}

// findParam returns the first value for one PID or fails the test.
func findParam(t *testing.T, params []rtps.Param, id uint16) []byte {
	t.Helper()
	for _, p := range params {
		if p.ID == id {
			return p.Value
		}
	}
	t.Fatalf("missing param %#04x", id)
	return nil
}

// rawParamLength scans raw payload bytes for one PID length field.
func rawParamLength(t *testing.T, payload []byte, wantID uint16) int {
	t.Helper()
	if len(payload) < 4 {
		t.Fatalf("payload need 4 have %d", len(payload))
	}
	body := payload[4:]
	off := 0
	for len(body)-off >= 4 {
		id := binary.LittleEndian.Uint16(body[off : off+2])
		length := int(binary.LittleEndian.Uint16(body[off+2 : off+4]))
		off += 4
		if id == 0x0001 {
			break
		}
		if len(body)-off < length {
			t.Fatalf("param %#04x need %d have %d", id, length, len(body)-off)
		}
		if id == wantID {
			return length
		}
		off += length
		pad := (4 - length%4) % 4
		if len(body)-off < pad {
			t.Fatalf("param %#04x pad need %d have %d", id, pad, len(body)-off)
		}
		off += pad
	}
	t.Fatalf("param %#04x not found in raw payload", wantID)
	return 0
}

// checkPortOne verifies multicast B and B+1 plus unicast B+10 and B+11.
func checkPortOne(t *testing.T, domain, participant uint32) {
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
		t.Fatalf("domain %d meta multicast = %d want %d", domain, metaMC, wantMetaMC)
	}
	if userMC != wantUserMC {
		t.Fatalf("domain %d user multicast = %d want %d", domain, userMC, wantUserMC)
	}
	if metaUC != wantMetaUC {
		t.Fatalf("domain %d participant %d meta unicast = %d want %d", domain, participant, metaUC, wantMetaUC)
	}
	if userUC != wantUserUC {
		t.Fatalf("domain %d participant %d user unicast = %d want %d", domain, participant, userUC, wantUserUC)
	}
}

// TestPortMulticastVsUnicastWithGain pins B and gain across domains.
//
//nolint:gocognit,cyclop // test sequences many assertions, splitting would obscure intent
func TestPortMulticastVsUnicastWithGain(t *testing.T) {
	t.Parallel()
	if rtps.BasePort != basePortLocal {
		t.Fatalf("BasePort = %d want %d", rtps.BasePort, basePortLocal)
	}
	if rtps.DomainGain != domainGainLocal {
		t.Fatalf("DomainGain = %d want %d", rtps.DomainGain, domainGainLocal)
	}
	if rtps.ParticipantGain != participantGainLocal {
		t.Fatalf("ParticipantGain = %d want %d", rtps.ParticipantGain, participantGainLocal)
	}
	if rtps.MetatrafficMulticastOffset != 0 {
		t.Fatalf("meta multicast offset = %d want 0", rtps.MetatrafficMulticastOffset)
	}
	if rtps.UsertrafficMulticastOffset != 1 {
		t.Fatalf("user multicast offset = %d want 1", rtps.UsertrafficMulticastOffset)
	}
	if rtps.MetatrafficUnicastOffset != 10 {
		t.Fatalf("meta unicast offset = %d want 10", rtps.MetatrafficUnicastOffset)
	}
	if rtps.UsertrafficUnicastOffset != 11 {
		t.Fatalf("user unicast offset = %d want 11", rtps.UsertrafficUnicastOffset)
	}
	for _, domain := range conformDomains {
		for _, participant := range conformParticipants {
			checkPortOne(t, domain, participant)
		}
		spdpPort, err := dds.SPDPUnicastPort(domain)
		if err != nil {
			t.Fatalf("domain %d SPDP unicast: %v", domain, err)
		}
		metaUC, err := rtps.MetatrafficUnicastPort(domain, 0)
		if err != nil {
			t.Fatalf("domain %d meta unicast: %v", domain, err)
		}
		if int(spdpPort) != metaUC {
			t.Fatalf("domain %d SPDP port = %d want meta %d", domain, spdpPort, metaUC)
		}
	}
	for _, domain := range []uint32{102, 200} {
		_, err := rtps.MetatrafficMulticastPort(domain)
		if err == nil {
			t.Fatalf("meta multicast domain %d succeeded want error", domain)
		}
		_, err = rtps.MetatrafficUnicastPort(domain, 0)
		if err == nil {
			t.Fatalf("meta unicast domain %d succeeded want error", domain)
		}
	}
}

// TestAdvertiseIPv4IsLANNotLoopback checks LAN advertisement.
func TestAdvertiseIPv4IsLANNotLoopback(t *testing.T) {
	t.Parallel()
	ip := rtps.AdvertiseIPv4()
	if ip == nil {
		t.Fatalf("AdvertiseIPv4 is nil")
	}
	v4 := ip.To4()
	if v4 == nil {
		t.Fatalf("AdvertiseIPv4 %v is not IPv4", ip)
	}
	if ip.IsLoopback() {
		t.Skipf("no LAN interface, AdvertiseIPv4 falls back to loopback")
	}
	if ip.String() == rtps.LoopbackIPv4 {
		t.Fatalf("AdvertiseIPv4 = %q want LAN not loopback", ip.String())
	}
	if ip.String() == "127.0.0.1" {
		t.Fatalf("AdvertiseIPv4 is 127.0.0.1 want LAN")
	}
}

// TestWildcardBindIsUnspecified checks the wildcard bind constant.
func TestWildcardBindIsUnspecified(t *testing.T) {
	t.Parallel()
	if rtps.AnyIPv4 != "0.0.0.0" {
		t.Fatalf("AnyIPv4 = %q want 0.0.0.0", rtps.AnyIPv4)
	}
	parsed := net.ParseIP(rtps.AnyIPv4)
	if parsed == nil {
		t.Fatalf("AnyIPv4 does not parse")
	}
	if !parsed.IsUnspecified() {
		t.Fatalf("AnyIPv4 %v is not unspecified", parsed)
	}
	ctx := context.Background()
	conn, err := rtps.DialUnicast(ctx, 86)
	if err != nil {
		t.Fatalf("dial unicast: %v", err)
	}
	t.Cleanup(func() {
		err := conn.Close()
		if err != nil {
			t.Errorf("close unicast: %v", err)
		}
	})
	addr := conn.LocalAddr()
	if addr == nil {
		t.Fatalf("local addr is nil")
	}
	advertised := rtps.AdvertiseIPv4()
	if !addr.IP.Equal(advertised) {
		t.Fatalf("local ip = %s want advertised %s", addr.IP.String(), advertised.String())
	}
}

// TestHeaderVendorAndEndpoints pins RTPS 2.1 plus vendor plus routing.
//
//nolint:gocognit // test sequences many assertions, splitting would obscure intent
func TestHeaderVendorAndEndpoints(t *testing.T) {
	t.Parallel()
	for _, domain := range conformDomains {
		guid := testGUID(byte(domain + 0x10)) //nolint:gosec // G115: domains 0-101 plus offset fit in byte
		raw := mustMarshalAlive(t, guid, domain, 1, expectedLease)
		hdr, data := decodeSPDPData(t, raw)
		if hdr.Major != 2 {
			t.Fatalf("domain %d major = %d want 2", domain, hdr.Major)
		}
		if hdr.Minor != 1 {
			t.Fatalf("domain %d minor = %d want 1", domain, hdr.Minor)
		}
		wantVendor := [2]byte{0x01, 0x10}
		if hdr.Vendor != wantVendor {
			t.Fatalf("domain %d vendor = %v want 01 10", domain, hdr.Vendor)
		}
		if hdr.Vendor != rtps.DefaultVendorID() {
			t.Fatalf("domain %d vendor mismatch default", domain)
		}
		if data.ReaderID != rtps.EntityUnknown {
			t.Fatalf("domain %d reader = %v want unknown", domain, data.ReaderID)
		}
		if data.WriterID != rtps.EntitySPDPWriter {
			t.Fatalf("domain %d writer = %v want SPDP writer", domain, data.WriterID)
		}
		if data.SequenceNumber != 1 {
			t.Fatalf("domain %d seq = %d want 1", domain, data.SequenceNumber)
		}
		prefix := [12]byte(guid.Prefix)
		if hdr.Prefix != prefix {
			t.Fatalf("domain %d header prefix mismatch", domain)
		}
	}
}

// TestUserDataPadInLength checks length includes pad with zero failures.
func TestUserDataPadInLength(t *testing.T) {
	t.Parallel()
	guid := testGUID(0x33)
	raw := mustMarshalAlive(t, guid, 42, 1, expectedLease)
	_, data := decodeSPDPData(t, raw)
	params := decodePayloadParams(t, data)
	value := findParam(t, params, userDataPID)
	if len(value)%4 != 0 {
		t.Fatalf("UserData len = %d want multiple of 4", len(value))
	}
	if !bytes.Contains(value, []byte("enclave=/;")) {
		t.Fatalf("UserData misses enclave: %q", string(value))
	}
	wireLen := rawParamLength(t, data.Payload, userDataPID)
	if wireLen != len(value) {
		t.Fatalf("UserData wire len = %d want value len %d", wireLen, len(value))
	}
	if wireLen%4 != 0 {
		t.Fatalf("UserData wire len = %d want padded", wireLen)
	}
	var drops rtps.Counter
	_, _, err := rtps.DecodeMessage(raw, &drops)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	if drops.Count() != 0 {
		t.Fatalf("drops = %d want 0", drops.Count())
	}
	ann := mustParse(t, raw)
	if !ann.Alive {
		t.Fatalf("alive parsed as dispose")
	}
	if drops.Count() != 0 {
		t.Fatalf("drops after parse = %d want 0", drops.Count())
	}
}

// TestLocatorsUnicastPlusMulticast checks four locators and group.
//
//nolint:gocognit,cyclop // test sequences many assertions, splitting would obscure intent
func TestLocatorsUnicastPlusMulticast(t *testing.T) {
	t.Parallel()
	if rtps.SPDPMulticastGroup != "239.255.0.1" {
		t.Fatalf("group = %q want 239.255.0.1", rtps.SPDPMulticastGroup)
	}
	group := net.ParseIP(rtps.SPDPMulticastGroup)
	if group == nil {
		t.Fatalf("group parse failed")
	}
	advertised := rtps.AdvertiseIPv4()
	for _, domain := range conformDomains {
		guid := testGUID(byte(domain + 0x20)) //nolint:gosec // G115: domains 0-101 plus offset fit in byte
		raw := mustMarshalAlive(t, guid, domain, 1, expectedLease)
		_, data := decodeSPDPData(t, raw)
		params := decodePayloadParams(t, data)
		metaRaw := findParam(t, params, rtps.PIDMetatrafficUnicastLocator)
		defaultRaw := findParam(t, params, rtps.PIDDefaultUnicastLocatorStandard)
		metaMCRaw := findParam(t, params, rtps.PIDMetatrafficMulticastLocator)
		defaultMCRaw := findParam(t, params, rtps.PIDDefaultMulticastLocator)
		metaPort, err := rtps.MetatrafficUnicastPort(domain, 0)
		if err != nil {
			t.Fatalf("domain %d meta port: %v", domain, err)
		}
		defaultPort, err := rtps.UsertrafficUnicastPort(domain, 0)
		if err != nil {
			t.Fatalf("domain %d default port: %v", domain, err)
		}
		metaMCPort, err := rtps.MetatrafficMulticastPort(domain)
		if err != nil {
			t.Fatalf("domain %d meta mc port: %v", domain, err)
		}
		defaultMCPort, err := rtps.UsertrafficMulticastPort(domain)
		if err != nil {
			t.Fatalf("domain %d default mc port: %v", domain, err)
		}
		kind, port, ip, err := rtps.DecodeLocator(metaRaw)
		if err != nil {
			t.Fatalf("domain %d decode meta unicast: %v", domain, err)
		}
		if kind != rtps.LocatorKindUDPv4 {
			t.Fatalf("domain %d meta unicast kind = %d want UDPv4", domain, kind)
		}
		if int(port) != metaPort {
			t.Fatalf("domain %d meta unicast port = %d want %d", domain, port, metaPort)
		}
		if !ip.Equal(advertised) {
			t.Fatalf("domain %d meta unicast ip = %s want %s", domain, ip.String(), advertised.String())
		}
		kind, port, ip, err = rtps.DecodeLocator(defaultRaw)
		if err != nil {
			t.Fatalf("domain %d decode default unicast: %v", domain, err)
		}
		if kind != rtps.LocatorKindUDPv4 {
			t.Fatalf("domain %d default unicast kind = %d want UDPv4", domain, kind)
		}
		if int(port) != defaultPort {
			t.Fatalf("domain %d default unicast port = %d want %d", domain, port, defaultPort)
		}
		if !ip.Equal(advertised) {
			t.Fatalf("domain %d default unicast ip = %s want %s", domain, ip.String(), advertised.String())
		}
		kind, port, ip, err = rtps.DecodeLocator(metaMCRaw)
		if err != nil {
			t.Fatalf("domain %d decode meta multicast: %v", domain, err)
		}
		if kind != rtps.LocatorKindUDPv4 {
			t.Fatalf("domain %d meta multicast kind = %d want UDPv4", domain, kind)
		}
		if int(port) != metaMCPort {
			t.Fatalf("domain %d meta multicast port = %d want %d", domain, port, metaMCPort)
		}
		if !ip.Equal(group) {
			t.Fatalf("domain %d meta multicast ip = %s want %s", domain, ip.String(), group.String())
		}
		kind, port, ip, err = rtps.DecodeLocator(defaultMCRaw)
		if err != nil {
			t.Fatalf("domain %d decode default multicast: %v", domain, err)
		}
		if kind != rtps.LocatorKindUDPv4 {
			t.Fatalf("domain %d default multicast kind = %d want UDPv4", domain, kind)
		}
		if int(port) != defaultMCPort {
			t.Fatalf("domain %d default multicast port = %d want %d", domain, port, defaultMCPort)
		}
		if !ip.Equal(group) {
			t.Fatalf("domain %d default multicast ip = %s want %s", domain, ip.String(), group.String())
		}
	}
}

// TestDomainTagLeaseBuiltin checks tag plus lease plus endpoint set.
//
//nolint:gocognit // test sequences many assertions, splitting would obscure intent
func TestDomainTagLeaseBuiltin(t *testing.T) {
	t.Parallel()
	for _, domain := range conformDomains {
		guid := testGUID(byte(domain + 0x40)) //nolint:gosec // G115: domains 0-101 plus offset fit in byte
		raw := mustMarshalAlive(t, guid, domain, 1, expectedLease)
		_, data := decodeSPDPData(t, raw)
		params := decodePayloadParams(t, data)
		domainRaw := findParam(t, params, domainTagPID)
		if len(domainRaw) != 4 {
			t.Fatalf("domain %d tag len = %d want 4", domain, len(domainRaw))
		}
		gotDomain := binary.LittleEndian.Uint32(domainRaw)
		if gotDomain != domain {
			t.Fatalf("domain tag = %d want %d", gotDomain, domain)
		}
		leaseRaw := findParam(t, params, leasePID)
		if len(leaseRaw) != 8 {
			t.Fatalf("domain %d lease len = %d want 8", domain, len(leaseRaw))
		}
		sec := binary.LittleEndian.Uint32(leaseRaw[0:4])
		nanos := binary.LittleEndian.Uint32(leaseRaw[4:8])
		if sec != 10 {
			t.Fatalf("domain %d lease sec = %d want 10", domain, sec)
		}
		if nanos != 0 {
			t.Fatalf("domain %d lease nanos = %d want 0", domain, nanos)
		}
		builtinRaw := findParam(t, params, builtinEndpointPID)
		if len(builtinRaw) != 4 {
			t.Fatalf("domain %d builtin len = %d want 4", domain, len(builtinRaw))
		}
		gotSet := binary.LittleEndian.Uint32(builtinRaw)
		if gotSet != expectedBuiltinSet {
			t.Fatalf("domain %d builtin = %#x want 0x3F", domain, gotSet)
		}
		manualRaw := findParam(t, params, 0x0034)
		if len(manualRaw) != 4 {
			t.Fatalf("domain %d manual len = %d want 4", domain, len(manualRaw))
		}
		if gotManual := binary.LittleEndian.Uint32(manualRaw); gotManual != 0 {
			t.Fatalf("domain %d manual = %d want 0", domain, gotManual)
		}
		builtinQosRaw := findParam(t, params, 0x0077)
		if len(builtinQosRaw) != 4 {
			t.Fatalf("domain %d builtin QoS len = %d want 4", domain, len(builtinQosRaw))
		}
		if gotQos := binary.LittleEndian.Uint32(builtinQosRaw); gotQos != 0 {
			t.Fatalf("domain %d builtin QoS = %d want 0", domain, gotQos)
		}
		ann := mustParse(t, raw)
		if ann.DomainID != domain {
			t.Fatalf("domain %d parsed domain = %d", domain, ann.DomainID)
		}
		if ann.Lease != expectedLease {
			t.Fatalf("domain %d parsed lease = %v want 10s", domain, ann.Lease)
		}
		if ann.ManualLivelinessCount != 0 {
			t.Fatalf("domain %d manual count = %d want 0", domain, ann.ManualLivelinessCount)
		}
		if ann.BuiltinEndpointQos != 0 {
			t.Fatalf("domain %d builtin QoS = %d want 0", domain, ann.BuiltinEndpointQos)
		}
	}
}

// TestGUIDSeqDisposeRemoves checks identity plus sequence plus removal.
//
//nolint:cyclop,gocognit // test sequences many assertions, splitting would obscure intent
func TestGUIDSeqDisposeRemoves(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "")
	part, err := dds.NewLocalParticipant(0, expectedLease)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}
	if part.Sequence() != 0 {
		t.Fatalf("initial seq = %d want 0", part.Sequence())
	}
	prefix := part.GUID().Prefix
	zero := dds.GUIDPrefix{}
	if prefix == zero {
		t.Fatalf("prefix is zero")
	}
	if part.GUID().EntityID != [4]byte{0x00, 0x00, 0x01, 0xC1} {
		t.Fatalf("entity mismatch")
	}
	sender := &conformanceSender{}
	err = part.Announce(context.Background(), sender)
	if err != nil {
		t.Fatalf("announce 1: %v", err)
	}
	if part.Sequence() != 1 {
		t.Fatalf("announce seq = %d want 1", part.Sequence())
	}
	announced := mustParse(t, sender.last)
	if announced.Sequence != 1 {
		t.Fatalf("announced seq = %d want 1", announced.Sequence)
	}
	if !announced.Alive {
		t.Fatalf("announced parsed as dispose")
	}
	err = part.Announce(context.Background(), sender)
	if err != nil {
		t.Fatalf("announce 2: %v", err)
	}
	if part.Sequence() != 2 {
		t.Fatalf("announce seq = %d want 2", part.Sequence())
	}
	announced2 := mustParse(t, sender.last)
	if announced2.Sequence != 2 {
		t.Fatalf("announced seq = %d want 2", announced2.Sequence)
	}
	table := dds.NewRemoteTable(nil)
	now := time.Unix(1000, 0).UTC()
	alive1 := mustMarshalAlive(t, part.GUID(), 0, 1, expectedLease)
	ann1 := mustParse(t, alive1)
	if ann1.Sequence != 1 {
		t.Fatalf("seq = %d want 1", ann1.Sequence)
	}
	if !ann1.Alive {
		t.Fatalf("alive parsed as dispose")
	}
	err = table.HandlePacket(alive1, now)
	if err != nil {
		t.Fatalf("handle alive: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("len = %d want 1", table.Len())
	}
	alive2 := mustMarshalAlive(t, part.GUID(), 0, 2, expectedLease)
	err = table.HandlePacket(alive2, now.Add(time.Second))
	if err != nil {
		t.Fatalf("handle second: %v", err)
	}
	info, ok := table.Get(prefix)
	if !ok {
		t.Fatalf("missing after update")
	}
	if info.LastSeq != 2 {
		t.Fatalf("stored seq = %d want 2", info.LastSeq)
	}
	if table.Generation() != 2 {
		t.Fatalf("gen = %d want 2 after monotonic update", table.Generation())
	}
	err = table.HandlePacket(alive1, now.Add(time.Second))
	if err != nil {
		t.Fatalf("handle stale alive: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("len = %d want 1 after stale", table.Len())
	}
	staleInfo, ok := table.Get(prefix)
	if !ok {
		t.Fatalf("missing after stale")
	}
	if staleInfo.LastSeq != 2 {
		t.Fatalf("stored seq = %d want 2 after stale", staleInfo.LastSeq)
	}
	if table.Generation() != 2 {
		t.Fatalf("gen = %d want 2 after stale", table.Generation())
	}
	bye := mustMarshalDispose(t, part.GUID(), 0, 3)
	byeAnn := mustParse(t, bye)
	if byeAnn.Alive {
		t.Fatalf("dispose parsed as alive")
	}
	if byeAnn.Sequence != 3 {
		t.Fatalf("dispose seq = %d want 3", byeAnn.Sequence)
	}
	err = table.HandlePacket(bye, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("handle dispose: %v", err)
	}
	if table.Len() != 0 {
		t.Fatalf("len = %d want 0 after dispose", table.Len())
	}
	if table.Generation() != 3 {
		t.Fatalf("gen = %d want 3 after dispose", table.Generation())
	}
	err = table.HandlePacket(bye, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("handle missing dispose: %v", err)
	}
	if table.Generation() != 3 {
		t.Fatalf("gen = %d want 3 after missing dispose", table.Generation())
	}
	alive4 := mustMarshalAlive(t, part.GUID(), 0, 4, expectedLease)
	err = table.HandlePacket(alive4, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("handle re-add alive: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("len = %d want 1 after re-add", table.Len())
	}
	staleBye := mustMarshalDispose(t, part.GUID(), 0, 2)
	err = table.HandlePacket(staleBye, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("handle stale dispose: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("len = %d want 1 after stale dispose", table.Len())
	}
	kept, ok := table.Get(prefix)
	if !ok {
		t.Fatalf("missing after stale dispose")
	}
	if kept.LastSeq != 4 {
		t.Fatalf("stored seq = %d want 4 after stale dispose", kept.LastSeq)
	}
}

// TestMultiPortRoutesStored checks both meta and default ports are kept.
func TestMultiPortRoutesStored(t *testing.T) {
	t.Parallel()
	for _, domain := range conformDomains {
		guid := testGUID(byte(domain + 0x60)) //nolint:gosec // G115: domains 0-101 plus offset fit in byte
		raw := mustMarshalAlive(t, guid, domain, 9, expectedLease)
		ann := mustParse(t, raw)
		if len(ann.Ports) != 2 {
			t.Fatalf("domain %d ports = %v want 2 entries", domain, ann.Ports)
		}
		metaPort, err := rtps.MetatrafficUnicastPort(domain, 0)
		if err != nil {
			t.Fatalf("domain %d meta port: %v", domain, err)
		}
		defaultPort, err := rtps.UsertrafficUnicastPort(domain, 0)
		if err != nil {
			t.Fatalf("domain %d default port: %v", domain, err)
		}
		if int(ann.Ports[0]) != metaPort {
			t.Fatalf("domain %d port 0 = %d want meta %d", domain, ann.Ports[0], metaPort)
		}
		if int(ann.Ports[1]) != defaultPort {
			t.Fatalf("domain %d port 1 = %d want default %d", domain, ann.Ports[1], defaultPort)
		}
	}
}

// checkParamGolden pins one param ID with golden bytes and round trip.
func checkParamGolden(t *testing.T, id uint16, value, want []byte, label string) {
	t.Helper()
	raw := rtps.EncodeParams([]rtps.Param{{ID: id, Value: value}})
	if !bytes.Equal(raw, want) {
		t.Fatalf("%s golden = % X, want % X", label, raw, want)
	}
	back, err := rtps.DecodeParams(raw)
	if err != nil {
		t.Fatalf("decode %s: %v", label, err)
	}
	if len(back) != 1 || back[0].ID != id {
		t.Fatalf("%s decode = %+v", label, back)
	}
}

// TestManualLivelinessGolden pins 0x0034 with golden bytes.
func TestManualLivelinessGolden(t *testing.T) {
	t.Parallel()
	checkParamGolden(t, 0x0034, []byte{0x05, 0x00, 0x00, 0x00}, []byte{0x34, 0x00, 0x04, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}, "manual")
}

// TestBuiltinEndpointQosGolden pins 0x0077 with golden bytes.
func TestBuiltinEndpointQosGolden(t *testing.T) {
	t.Parallel()
	checkParamGolden(t, 0x0077, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0x77, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}, "builtin QoS")
}

// TestVendorPreservation checks 0x8003, 0x8007, and 0x8019 survive a round trip.
func TestVendorPreservation(t *testing.T) {
	t.Parallel()
	params := []rtps.Param{
		{ID: 0x8003, Value: []byte{0x00, 0x00, 0x00, 0x00}},
		{ID: 0x8007, Value: []byte{0x01, 0x02, 0x03, 0x04}},
		{ID: 0x8019, Value: []byte{0x00, 0x00, 0x10, 0x00}},
	}
	raw := rtps.EncodeParams(params)
	back, err := rtps.DecodeParams(raw)
	if err != nil {
		t.Fatalf("decode vendor: %v", err)
	}
	if len(back) != len(params) {
		t.Fatalf("vendor params = %d, want %d", len(back), len(params))
	}
	for i := range params {
		if back[i].ID != params[i].ID || !bytes.Equal(back[i].Value, params[i].Value) {
			t.Fatalf("vendor param %d mismatch", i)
		}
	}
}

// TestPadSkipped checks PIDPad entries never surface.
func TestPadSkipped(t *testing.T) {
	t.Parallel()
	blob := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x16, 0x00, 0x04, 0x00, 0x01, 0x10, 0x00, 0x00,
		0x00, 0x00, 0x02, 0x00, 0xAA, 0xBB, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
	}
	back, err := rtps.DecodeParams(blob)
	if err != nil {
		t.Fatalf("decode pad: %v", err)
	}
	if len(back) != 1 || back[0].ID != rtps.PIDVendorID {
		t.Fatalf("pad decode = %+v, want single vendor", back)
	}
}
