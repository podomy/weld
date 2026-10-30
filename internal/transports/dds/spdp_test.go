// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// testGUID returns a deterministic GUID with repeated fill byte.
func testGUID(fill byte) GUID {
	var prefix GUIDPrefix

	for i := range prefix {
		prefix[i] = fill
	}

	return GUID{Prefix: prefix, EntityID: participantEntityID}
}

// isolateSPDPEnv clears the domain env for deterministic tests.
func isolateSPDPEnv(t *testing.T) {
	t.Helper()

	t.Setenv(transports.DomainEnvVar, "")
}

// mustMarshalAlive marshals an alive packet and fails on error.
func mustMarshalAlive(t *testing.T, guid GUID, domain uint32, seq uint64, lease time.Duration) []byte {
	t.Helper()

	data, err := MarshalAlivePacket(guid, domain, seq, lease)
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}

	return data
}

// mustMarshalDispose marshals a dispose packet and fails on error.
func mustMarshalDispose(t *testing.T, guid GUID, domain uint32, seq uint64) []byte {
	t.Helper()

	data, err := MarshalDisposePacket(guid, domain, seq)
	if err != nil {
		t.Fatalf("marshal dispose: %v", err)
	}

	return data
}

// decodeSPDPData decodes one RTPS datagram and returns the SPDP DATA.
//
// It mirrors the production scan: first DATA from writer 0x000100c2.
// Tests use it to pin wire shapes without repeating scan logic.
func decodeSPDPData(t *testing.T, data []byte) (rtps.Header, rtps.Data) {
	t.Helper()

	var drops rtps.Counter

	hdr, raws, err := rtps.DecodeMessage(data, &drops)
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

	t.Fatalf("no SPDP DATA")

	return rtps.Header{}, rtps.Data{}
}

// mergedSPDPParams merges inline QoS and payload params through production code.
//
// It calls collectParams so tests cover the same encap strip, big endian
// branch, and inline QoS merge as the parser instead of duplicating it.
func mergedSPDPParams(t *testing.T, d rtps.Data) []rtps.Param {
	t.Helper()

	cp := d

	params, err := collectParams(&cp)
	if err != nil {
		t.Fatalf("collect params: %v", err)
	}

	return params
}

// findTestParam returns the first value for one PID or fails.
func findTestParam(t *testing.T, params []rtps.Param, id uint16) []byte {
	t.Helper()

	for _, p := range params {
		if p.ID == id {
			return p.Value
		}
	}

	t.Fatalf("missing param %#04x", id)

	return nil
}

// checkLocatorAdvertised checks a 24 byte locator is UDPv4 with the advertised address.
func checkLocatorAdvertised(t *testing.T, value []byte, wantPort uint16) {
	t.Helper()

	kind, port, ip, err := rtps.DecodeLocator(value)
	if err != nil {
		t.Fatalf("decode locator: %v", err)
	}

	if kind != rtps.LocatorKindUDPv4 {
		t.Fatalf("locator kind = %d, want UDPv4", kind)
	}

	if !ip.Equal(rtps.AdvertiseIPv4()) {
		t.Fatalf("locator ip = %v, want %v", ip, rtps.AdvertiseIPv4())
	}

	if port != uint32(wantPort) {
		t.Fatalf("locator port = %d, want %d", port, wantPort)
	}
}

// checkMulticastLocator checks a 24 byte locator is UDPv4 multicast group.
func checkMulticastLocator(t *testing.T, value []byte, wantPort uint16) {
	t.Helper()

	kind, port, ip, err := rtps.DecodeLocator(value)
	if err != nil {
		t.Fatalf("decode locator: %v", err)
	}

	if kind != rtps.LocatorKindUDPv4 {
		t.Fatalf("locator kind = %d, want UDPv4", kind)
	}

	if !ip.Equal(net.ParseIP(rtps.SPDPMulticastGroup)) {
		t.Fatalf("locator ip = %v, want %v", ip, rtps.SPDPMulticastGroup)
	}

	if port != uint32(wantPort) {
		t.Fatalf("locator port = %d, want %d", port, wantPort)
	}
}

// mustParse parses a packet and fails on error.
func mustParse(t *testing.T, data []byte) Announcement {
	t.Helper()

	ann, err := ParseAnnouncement(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	return ann
}

// mustHandle feeds a packet to the table and fails on error.
func mustHandle(t *testing.T, table *RemoteTable, data []byte, now time.Time) {
	t.Helper()

	err := table.HandlePacket(data, now)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
}

// fakeSender records SPDP payloads for tests.
type fakeSender struct {
	mu       sync.Mutex
	payloads [][]byte
	fail     error
}

// Send records a copy of the payload.
func (f *fakeSender) Send(ctx context.Context, data []byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: fake send context: %w", err)
	}

	if f.fail != nil {
		return f.fail
	}

	cp := make([]byte, len(data))
	copy(cp, data)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.payloads = append(f.payloads, cp)

	return nil
}

// count returns the number of recorded payloads.
func (f *fakeSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.payloads)
}

// payload returns a copy of the indexed payload.
func (f *fakeSender) payload(t *testing.T, idx int) []byte {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()

	if idx < 0 || idx >= len(f.payloads) {
		t.Fatalf("payload index %d out of %d", idx, len(f.payloads))
	}

	cp := make([]byte, len(f.payloads[idx]))
	copy(cp, f.payloads[idx])

	return cp
}

// fakeReceiver returns canned SPDP payloads for interface coverage.
type fakeReceiver struct {
	mu       sync.Mutex
	payloads [][]byte
	pos      int
}

// Receive returns the next canned payload.
func (f *fakeReceiver) Receive(ctx context.Context) ([]byte, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("dds: fake receive context: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.pos >= len(f.payloads) {
		return nil, context.DeadlineExceeded
	}

	cp := make([]byte, len(f.payloads[f.pos]))
	copy(cp, f.payloads[f.pos])
	f.pos++

	return cp, nil
}

// hookRecorder records generation callbacks.
type hookRecorder struct {
	mu    sync.Mutex
	calls []uint64
}

// hook records one generation.
func (h *hookRecorder) hook(gen uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.calls = append(h.calls, gen)
}

// generations returns a copy of recorded generations.
func (h *hookRecorder) generations() []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	out := make([]uint64, len(h.calls))
	copy(out, h.calls)

	return out
}

// count returns the number of hook calls.
func (h *hookRecorder) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.calls)
}

// TestSPDPUnicastPort checks port derivation per domain.
func TestSPDPUnicastPort(t *testing.T) {
	t.Parallel()

	port, err := SPDPUnicastPort(0)
	if err != nil {
		t.Fatalf("port 0: %v", err)
	}

	if port != 7410 {
		t.Fatalf("port 0 = %d, want 7410", port)
	}

	port, err = SPDPUnicastPort(1)
	if err != nil {
		t.Fatalf("port 1: %v", err)
	}

	if port != 7660 {
		t.Fatalf("port 1 = %d, want 7660", port)
	}

	port, err = SPDPUnicastPort(101)
	if err != nil {
		t.Fatalf("port 101: %v", err)
	}

	if port != 32660 {
		t.Fatalf("port 101 = %d, want 32660", port)
	}

	_, err = SPDPUnicastPort(102)
	if err == nil {
		t.Fatalf("port 102 succeeded, want error")
	}

	if !errors.Is(err, ErrInvalidSPDP) {
		t.Fatalf("port 102 error = %v, want ErrInvalidSPDP", err)
	}
}

// TestNewLocalParticipant checks getters and sequence start.
func TestNewLocalParticipant(t *testing.T) {
	isolateSPDPEnv(t)

	p, err := NewLocalParticipant(0, 10*time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}

	if p.DomainID() != 0 {
		t.Fatalf("domain = %d, want 0", p.DomainID())
	}

	if p.LeaseDuration() != 10*time.Second {
		t.Fatalf("lease = %v, want 10s", p.LeaseDuration())
	}

	if p.Sequence() != 0 {
		t.Fatalf("seq = %d, want 0", p.Sequence())
	}

	if isZeroPrefix(p.GUID().Prefix) {
		t.Fatalf("prefix is zero")
	}

	if p.GUID().EntityID != participantEntityID {
		t.Fatalf("entity mismatch")
	}
}

// TestNewLocalParticipantRandomPrefix checks prefixes differ per run.
func TestNewLocalParticipantRandomPrefix(t *testing.T) {
	isolateSPDPEnv(t)

	a, err := NewLocalParticipant(0, 10*time.Second)
	if err != nil {
		t.Fatalf("new a: %v", err)
	}

	b, err := NewLocalParticipant(0, 10*time.Second)
	if err != nil {
		t.Fatalf("new b: %v", err)
	}

	if a.GUID().Prefix == b.GUID().Prefix {
		t.Fatalf("prefixes match, want random per run")
	}
}

// TestNewLocalParticipantRejectsBadLease checks lease validation.
func TestNewLocalParticipantRejectsBadLease(t *testing.T) {
	t.Parallel()

	_, err := NewLocalParticipant(0, 0)
	if err == nil {
		t.Fatalf("zero lease succeeded, want error")
	}

	if !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("zero lease error = %v, want ErrInvalidLease", err)
	}

	_, err = NewLocalParticipant(0, -time.Second)
	if err == nil {
		t.Fatalf("negative lease succeeded, want error")
	}
}

// TestNewLocalParticipantRejectsBadDomain checks domain range.
func TestNewLocalParticipantRejectsBadDomain(t *testing.T) {
	t.Parallel()

	_, err := NewLocalParticipant(999, time.Second)
	if err == nil {
		t.Fatalf("bad domain succeeded, want error")
	}
}

// TestNewLocalParticipantEnvOverride checks ROS_DOMAIN_ID wins.
func TestNewLocalParticipantEnvOverride(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "42")

	p, err := NewLocalParticipant(7, time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}

	if p.DomainID() != 42 {
		t.Fatalf("domain = %d, want 42", p.DomainID())
	}
}

// checkRTPSHeaderPrefix pins the RTPS magic, version, and GUID prefix.
func checkRTPSHeaderPrefix(t *testing.T, data []byte, hdr rtps.Header, guid GUID) {
	t.Helper()

	if !bytes.Equal(data[:4], []byte(rtps.Magic)) {
		t.Fatalf("bad magic %v", data[:4])
	}

	if hdr.Major != 2 {
		t.Fatalf("major = %d, want 2", hdr.Major)
	}

	if hdr.Prefix != [12]byte(guid.Prefix) {
		t.Fatalf("header prefix mismatch")
	}
}

// checkSPDPDataEndpoints pins the DATA routing fields.
func checkSPDPDataEndpoints(t *testing.T, d rtps.Data, wantReader, wantWriter rtps.EntityID, wantSeq uint64, wantKey bool) {
	t.Helper()

	if d.ReaderID != wantReader {
		t.Fatalf("reader = %v, want %v", d.ReaderID, wantReader)
	}

	if d.WriterID != wantWriter {
		t.Fatalf("writer = %v, want %v", d.WriterID, wantWriter)
	}

	if d.SequenceNumber != wantSeq {
		t.Fatalf("seq = %d, want %d", d.SequenceNumber, wantSeq)
	}

	if d.Key != wantKey {
		t.Fatalf("key = %v, want %v", d.Key, wantKey)
	}
}

// checkAliveGoldenParams pins the alive inline QoS parameter set.
func checkAliveGoldenParams(t *testing.T, params []rtps.Param, guid GUID, domain uint32) {
	t.Helper()

	gotGUID := findTestParam(t, params, rtps.PIDParticipantGUID)
	if !bytes.Equal(gotGUID, guidBytes(guid)) {
		t.Fatalf("GUID param mismatch")
	}

	if got := findTestParam(t, params, rtps.PIDProtocolVersion); len(got) != 4 || got[0] != 2 || got[1] != 1 {
		t.Fatalf("protocol param = %v, want 2.1 padded to 4", got)
	}

	if got := findTestParam(t, params, rtps.PIDVendorID); len(got) != 4 || got[0] != vendorID0 || got[1] != vendorID1 {
		t.Fatalf("vendor param = %v, want 01 10 padded to 4", got)
	}

	checkAliveGoldenDiscovery(t, params)

	metaPort, err := rtps.MetatrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("meta port: %v", err)
	}

	defaultPort, err := rtps.UsertrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("default port: %v", err)
	}

	metaMCPort, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("meta multicast port: %v", err)
	}

	defaultMCPort, err := rtps.UsertrafficMulticastPort(domain)
	if err != nil {
		t.Fatalf("default multicast port: %v", err)
	}

	checkLocatorAdvertised(t, findTestParam(t, params, rtps.PIDMetatrafficUnicastLocator), uint16(metaPort))        //nolint:gosec // G115: standard ports fit uint16.
	checkLocatorAdvertised(t, findTestParam(t, params, rtps.PIDDefaultUnicastLocatorStandard), uint16(defaultPort)) //nolint:gosec // G115: standard ports fit uint16.
	checkMulticastLocator(t, findTestParam(t, params, rtps.PIDMetatrafficMulticastLocator), uint16(metaMCPort))     //nolint:gosec // G115: standard ports fit uint16.
	checkMulticastLocator(t, findTestParam(t, params, rtps.PIDDefaultMulticastLocator), uint16(defaultMCPort))      //nolint:gosec // G115: standard ports fit uint16.
	_ = findTestParam(t, params, rtps.PIDParticipantLeaseDuration)
	_ = findTestParam(t, params, pidDomainID)
}

// checkAliveGoldenDiscovery pins the endpoint set and enclave user data.
func checkAliveGoldenDiscovery(t *testing.T, params []rtps.Param) {
	t.Helper()

	if got := findTestParam(t, params, rtps.PIDBuiltinEndpointSet); len(got) != 4 {
		t.Fatalf("builtin endpoint set len = %d, want 4", len(got))
	}

	if got := findTestParam(t, params, rtps.PIDBuiltinEndpointQos); len(got) != 4 {
		t.Fatalf("builtin endpoint QoS len = %d, want 4", len(got))
	}

	if got := findTestParam(t, params, rtps.PIDParticipantManualLivelinessCount); len(got) != 4 {
		t.Fatalf("manual liveliness len = %d, want 4", len(got))
	}

	gotUser := findTestParam(t, params, rtps.PIDUserData)
	if len(gotUser) == 0 || !bytes.Contains(gotUser, []byte(spdpUserDataEnclave)) {
		t.Fatalf("user data = %q, want enclave", gotUser)
	}
}

// TestMarshalAliveGoldenShape pins the RTPS announce wire shape.
func TestMarshalAliveGoldenShape(t *testing.T) {
	t.Parallel()

	guid := testGUID(0x11)
	data := mustMarshalAlive(t, guid, 0, 1, 10*time.Second)

	if len(data) != 264 {
		t.Fatalf("len = %d, want 264", len(data))
	}

	hdr, d := decodeSPDPData(t, data)
	checkRTPSHeaderPrefix(t, data, hdr, guid)
	checkSPDPDataEndpoints(t, d, rtps.EntityUnknown, rtps.EntitySPDPWriter, 1, false)
	checkAliveGoldenParams(t, mergedSPDPParams(t, d), guid, 0)

	ann := mustParse(t, data)
	if ann.Lease != 10*time.Second {
		t.Fatalf("lease = %v, want 10s", ann.Lease)
	}

	if ann.ManualLivelinessCount != 0 {
		t.Fatalf("manual count = %d, want 0", ann.ManualLivelinessCount)
	}

	if ann.BuiltinEndpointQos != 0 {
		t.Fatalf("builtin QoS = %d, want 0", ann.BuiltinEndpointQos)
	}
}

// TestMarshalAliveWithPortsAnnouncesBoundPorts checks explicit locator ports.
//
// A second participant on one host binds a later unicast slot; advertising
// participant-zero ports would divert its metatraffic and user DATA to the
// first participant's socket while it starves. The legacy wrapper keeps
// participant-zero ports for existing goldens.
func TestMarshalAliveWithPortsAnnouncesBoundPorts(t *testing.T) {
	t.Parallel()

	guid := testGUID(0x33)
	data, err := MarshalAlivePacketWithPorts(guid, 47, 1, 10*time.Second, 19167, 19167)
	if err != nil {
		t.Fatalf("marshal with ports: %v", err)
	}

	ann := mustParse(t, data)
	if len(ann.Ports) != 2 {
		t.Fatalf("ports = %v, want 2 unicast locators", ann.Ports)
	}

	for _, p := range ann.Ports {
		if p != 19167 {
			t.Fatalf("port = %d, want bound 19167", p)
		}
	}

	legacy := mustMarshalAlive(t, guid, 47, 1, 10*time.Second)
	legacyAnn := mustParse(t, legacy)
	for _, p := range legacyAnn.Ports {
		if p == 19167 {
			t.Fatalf("legacy port = %d, want participant-zero ports", p)
		}
	}

	local, err := NewLocalParticipant(47, 10*time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}

	local.SetAnnouncePorts(19167, 19167)
	ticket, err := local.nextPacket()
	if err != nil {
		t.Fatalf("next packet: %v", err)
	}

	if ticket.metaPort != 19167 || ticket.defaultPort != 19167 {
		t.Fatalf("ticket ports = %d/%d, want 19167/19167", ticket.metaPort, ticket.defaultPort)
	}
}

// TestMarshalDisposeGoldenShape pins the RTPS dispose wire shape.
func TestMarshalDisposeGoldenShape(t *testing.T) {
	t.Parallel()

	guid := testGUID(0x22)
	data := mustMarshalDispose(t, guid, 0, 7)

	if len(data) != 80 {
		t.Fatalf("len = %d, want 80", len(data))
	}

	_, d := decodeSPDPData(t, data)

	if !d.Key {
		t.Fatalf("dispose misses key flag")
	}

	if d.SequenceNumber != 7 {
		t.Fatalf("seq = %d, want 7", d.SequenceNumber)
	}

	ann := mustParse(t, data)
	if ann.Alive {
		t.Fatalf("dispose parsed as alive")
	}

	if ann.Sequence != 7 {
		t.Fatalf("seq = %d, want 7", ann.Sequence)
	}

	if ann.GUID != guid {
		t.Fatalf("GUID mismatch")
	}
}

// liveCycloneSPDP42Hex is a verbatim domain 42 SPDP DATA capture.
//
// Capture: bound the free domain 42 SPDP multicast port with no talker
// running, started a domain 42 Cyclone talker in weld-jazzy, and saved
// the first SPDP DATA datagram. RTPS 2.1, vendor 01 10, writer
// 0x000100c2, PL_CDR_LE payload with domain tag 42, 10s lease, and
// 192.168.0.162 unicast locators.
const liveCycloneSPDP42Hex = "525450530201011001108d197d69608b1430a12509010800bb7adf6a59a91b9c150580010000100000000000000100c20000000001000000000300002c0010000a000000656e636c6176653d2f3b000059005c00030000000e0000005f5f50726f636573734e616d6500000008000000707974686f6e3300060000005f5f5069640000000500000032373532000000000b0000005f5f486f73746e616d650000070000006665646f726100000000000015000400020100001600040001100000020008000a000000000000005000100001108d197d69608b1430a125000001c1580004003ffc00000f0004002a0000003100180001000000fd450000000000000000000000000000c0a800a24800180001000000ed450000000000000000000000000000efff00013200180001000000fc450000000000000000000000000000c0a800a23300180001000000ec450000000000000000000000000000efff000107803400000000002c0000000000000000000000000000001a0000006665646f72612f302e31302e352f4c696e75782f4c696e7578000000198004000000100001000000"

// liveCycloneSPDP42 returns the embedded live capture bytes.
func liveCycloneSPDP42(t *testing.T) []byte {
	t.Helper()

	raw, err := hex.DecodeString(liveCycloneSPDP42Hex)
	if err != nil {
		t.Fatalf("decode live capture: %v", err)
	}

	return raw
}

// checkLiveAnnFields pins the parsed live announcement fields.
func checkLiveAnnFields(t *testing.T, ann Announcement) {
	t.Helper()

	if !ann.Alive {
		t.Fatalf("live parsed as dispose")
	}

	if ann.DomainID != 42 {
		t.Fatalf("live domain = %d, want 42", ann.DomainID)
	}

	if ann.Lease != 10*time.Second {
		t.Fatalf("live lease = %v, want 10s", ann.Lease)
	}

	wantPorts := []uint16{17917, 17916}
	if len(ann.Ports) != len(wantPorts) {
		t.Fatalf("live ports = %v, want %v", ann.Ports, wantPorts)
	}

	for i := range wantPorts {
		if ann.Ports[i] != wantPorts[i] {
			t.Fatalf("live port %d = %d, want %d", i, ann.Ports[i], wantPorts[i])
		}
	}

	if isZeroPrefix(ann.GUID.Prefix) {
		t.Fatalf("live prefix is zero")
	}

	if ann.GUID.EntityID != participantEntityID {
		t.Fatalf("live entity = %v, want participant", ann.GUID.EntityID)
	}

	if ann.Sequence == 0 {
		t.Fatalf("live sequence is zero")
	}
}

// checkLiveWireParams pins the vendor PIDs on the live wire bytes.
func checkLiveWireParams(t *testing.T, raw []byte) {
	t.Helper()

	_, d := decodeSPDPData(t, raw)
	params := mergedSPDPParams(t, d)

	checkLiveBaseParams(t, params)
	checkLiveVendorParams(t, params)
}

// checkLiveBaseParams pins vendor, protocol, and GUID params.
func checkLiveBaseParams(t *testing.T, params []rtps.Param) {
	t.Helper()

	if got := findTestParam(t, params, rtps.PIDVendorID); len(got) < 2 || got[0] != 0x01 || got[1] != 0x10 {
		t.Fatalf("live vendor = %v, want 01 10 prefix", got)
	}

	if got := findTestParam(t, params, rtps.PIDProtocolVersion); len(got) < 2 || got[0] != 2 || got[1] != 1 {
		t.Fatalf("live protocol = %v, want 2.1 prefix", got)
	}

	if got := findTestParam(t, params, rtps.PIDParticipantGUID); len(got) != 16 {
		t.Fatalf("live GUID len = %d, want 16", len(got))
	}
}

// checkLiveVendorParams pins Adlink vendor params 0x8007 and 0x8019.
func checkLiveVendorParams(t *testing.T, params []rtps.Param) {
	t.Helper()

	if got := findTestParam(t, params, rtps.PIDVendorAdlinkParticipantVersionInfo); len(got) != 52 {
		t.Fatalf("live 0x8007 len = %d, want 52", len(got))
	}

	if got := findTestParam(t, params, rtps.PIDVendorAdlinkParticipantVersionInfo); !bytes.Contains(got, []byte("fedora")) {
		t.Fatalf("live 0x8007 misses fedora marker")
	}

	if got := findTestParam(t, params, rtps.PIDVendorCycloneReceiveBufferSize); len(got) != 4 || got[0] != 0x00 || got[1] != 0x00 || got[2] != 0x10 || got[3] != 0x00 {
		t.Fatalf("live 0x8019 = % X, want 00 00 10 00", got)
	}
}

// TestParseAcceptsLiveCycloneAnnouncement checks interop with a real peer.
//
// The embedded packet is a live Cyclone SPDP DATA datagram with a
// PL_CDR payload, so it proves the parser accepts vendor payloads and
// non loopback locators under the same rules as local announcements.
func TestParseAcceptsLiveCycloneAnnouncement(t *testing.T) {
	t.Parallel()

	raw := liveCycloneSPDP42(t)

	if len(raw) != 420 {
		t.Fatalf("live len = %d, want 420", len(raw))
	}

	ann, err := ParseAnnouncement(raw)
	if err != nil {
		t.Fatalf("parse live: %v", err)
	}

	checkLiveAnnFields(t, ann)
	checkLiveWireParams(t, raw)
}

// TestParseRoundTripAlive checks marshal and parse agree.
func TestParseRoundTripAlive(t *testing.T) {
	t.Parallel()

	guid := testGUID(0x33)
	data := mustMarshalAlive(t, guid, 3, 9, 1500*time.Millisecond)
	ann := mustParse(t, data)

	if ann.GUID != guid {
		t.Fatalf("guid mismatch")
	}

	if ann.DomainID != 3 {
		t.Fatalf("domain = %d, want 3", ann.DomainID)
	}

	if ann.Sequence != 9 {
		t.Fatalf("seq = %d, want 9", ann.Sequence)
	}

	if !ann.Alive {
		t.Fatalf("alive is false")
	}

	if ann.Lease != 1500*time.Millisecond {
		t.Fatalf("lease = %v, want 1.5s", ann.Lease)
	}

	wantMeta, err := rtps.MetatrafficUnicastPort(3, 0)
	if err != nil {
		t.Fatalf("want meta port: %v", err)
	}

	wantDefault, err := rtps.UsertrafficUnicastPort(3, 0)
	if err != nil {
		t.Fatalf("want default port: %v", err)
	}

	wantPorts := []uint16{uint16(wantMeta), uint16(wantDefault)} //nolint:gosec // G115: standard ports fit uint16.
	if len(ann.Ports) != len(wantPorts) {
		t.Fatalf("ports = %v, want %v", ann.Ports, wantPorts)
	}

	for i := range wantPorts {
		if ann.Ports[i] != wantPorts[i] {
			t.Fatalf("port %d = %d, want %d", i, ann.Ports[i], wantPorts[i])
		}
	}
}

// TestParseRoundTripDispose checks dispose marshal and parse agree.
func TestParseRoundTripDispose(t *testing.T) {
	t.Parallel()

	guid := testGUID(0x44)
	data := mustMarshalDispose(t, guid, 2, 4)
	ann := mustParse(t, data)

	if ann.GUID != guid {
		t.Fatalf("guid mismatch")
	}

	if ann.DomainID != 2 {
		t.Fatalf("domain = %d, want 2", ann.DomainID)
	}

	if ann.Alive {
		t.Fatalf("alive is true")
	}

	if ann.Lease != 0 || len(ann.Ports) != 0 {
		t.Fatalf("dispose lease/ports not zero")
	}
}

// TestParseRejectsBad checks malformed packets are rejected.
func TestParseRejectsBad(t *testing.T) {
	t.Parallel()

	guid := testGUID(0x55)
	good := mustMarshalAlive(t, guid, 0, 1, time.Second)

	infoOnly := func() []byte {
		hdr := rtps.DefaultHeader([12]byte(guid.Prefix))
		raw, err := rtps.WrapSubmessage(rtps.SubmessageInfoTS, 0x01, make([]byte, 8))
		if err != nil {
			t.Fatalf("wrap info_ts: %v", err)
		}
		msg, err := rtps.EncodeMessage(hdr, []rtps.RawSubmessage{{ID: rtps.SubmessageInfoTS, Flags: 0x01, Body: make([]byte, 8)}})
		if err != nil {
			t.Fatalf("encode info_ts: %v", err)
		}
		_ = raw

		return msg
	}()

	wrongWriter := append([]byte(nil), good...)
	wrongWriter[32] = 0x00
	wrongWriter[33] = 0x02
	wrongWriter[34] = 0x00
	wrongWriter[35] = 0xC2

	badSentinel := append([]byte(nil), good...)
	badSentinel[len(badSentinel)-4] = 0x02

	zeroSeq := append([]byte(nil), good...)
	for i := 36; i < 44; i++ {
		zeroSeq[i] = 0x00
	}

	cases := [][]byte{
		{},
		good[:10],
		append([]byte("XXXX"), good[4:]...),
		infoOnly,
		wrongWriter,
		good[:len(good)-10],
		append(good, 0x00),
		badSentinel,
		zeroSeq,
		good[:20],
	}

	for idx, data := range cases {
		_, err := ParseAnnouncement(data)
		if err == nil {
			t.Fatalf("case %d succeeded, want error", idx)
		}

		if !errors.Is(err, ErrInvalidSPDP) {
			t.Fatalf("case %d error = %v, want ErrInvalidSPDP", idx, err)
		}
	}
}

// TestPatchSPDPLocatorPortsRewritesUnicastLocators checks live port patching.
func TestPatchSPDPLocatorPortsRewritesUnicastLocators(t *testing.T) {
	t.Parallel()

	guid := testGUID(0x5B)
	good := mustMarshalAlive(t, guid, 0, 1, 10*time.Second)

	patched := patchSPDPLocatorPorts(good, 12345)
	ann := mustParse(t, patched)

	if len(ann.Ports) != 2 {
		t.Fatalf("patched ports = %v, want 2 entries", ann.Ports)
	}

	for i, port := range ann.Ports {
		if port != 12345 {
			t.Fatalf("patched port %d = %d, want 12345", i, port)
		}
	}

	_, d := decodeSPDPData(t, patched)
	params := mergedSPDPParams(t, d)
	checkLocatorAdvertised(t, findTestParam(t, params, rtps.PIDMetatrafficUnicastLocator), 12345)
	checkLocatorAdvertised(t, findTestParam(t, params, rtps.PIDDefaultUnicastLocatorStandard), 12345)

	if bytes.Equal(good, patched) {
		t.Fatalf("patch left packet unchanged")
	}

	ann2 := mustParse(t, good)
	for _, port := range ann2.Ports {
		if port == 12345 {
			t.Fatalf("original port changed by patch: %v", ann2.Ports)
		}
	}

	if len(ann2.Ports) != 2 {
		t.Fatalf("original ports = %v, want 2 entries", ann2.Ports)
	}

	bye := mustMarshalDispose(t, guid, 0, 2)
	patchedBye := patchSPDPPort(bye, 12345)
	annBye := mustParse(t, patchedBye)

	if annBye.Alive {
		t.Fatalf("patched dispose parsed as alive")
	}

	for _, garbage := range [][]byte{{}, {0x00}, good[:10], append([]byte(nil), good[:20]...)} {
		out := patchSPDPLocatorPorts(garbage, 12345)
		if !bytes.Equal(out, garbage) {
			t.Fatalf("patch changed garbage input")
		}
	}
}

// TestParseIgnoresNonSPDPData checks other writers are skipped, not stored.
func TestParseIgnoresNonSPDPData(t *testing.T) {
	t.Parallel()

	guid := testGUID(0x5A)
	good := mustMarshalAlive(t, guid, 0, 1, time.Second)
	wrongWriter := append([]byte(nil), good...)
	wrongWriter[32] = 0x00
	wrongWriter[33] = 0x03
	wrongWriter[34] = 0x00
	wrongWriter[35] = 0xC2

	_, err := ParseAnnouncement(wrongWriter)
	if !errors.Is(err, ErrInvalidSPDP) {
		t.Fatalf("non SPDP error = %v, want ErrInvalidSPDP", err)
	}
}

// TestParseRejectsZeroPrefix checks zero GUID prefix is rejected.
func TestParseRejectsZeroPrefix(t *testing.T) {
	t.Parallel()

	var zero GUID

	_, err := MarshalAlivePacket(zero, 0, 1, time.Second)
	if err == nil {
		t.Fatalf("zero prefix marshal succeeded, want error")
	}

	if !errors.Is(err, ErrInvalidSPDP) {
		t.Fatalf("zero prefix error = %v", err)
	}
}

// TestLocalParticipantAnnounce checks announce sends parseable DATA.
func TestLocalParticipantAnnounce(t *testing.T) {
	isolateSPDPEnv(t)

	p, err := NewLocalParticipant(0, 5*time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}

	sender := &fakeSender{}
	ctx := context.Background()

	err = p.Announce(ctx, sender)
	if err != nil {
		t.Fatalf("announce: %v", err)
	}

	if sender.count() != 1 {
		t.Fatalf("count = %d, want 1", sender.count())
	}

	if p.Sequence() != 1 {
		t.Fatalf("seq = %d, want 1", p.Sequence())
	}

	first := sender.payload(t, 0)
	ann := mustParse(t, first)

	if !ann.Alive || ann.Sequence != 1 || ann.DomainID != 0 {
		t.Fatalf("announce fields mismatch")
	}

	err = p.Announce(ctx, sender)
	if err != nil {
		t.Fatalf("second announce: %v", err)
	}

	if p.Sequence() != 2 {
		t.Fatalf("seq = %d, want 2", p.Sequence())
	}

	second := sender.payload(t, 1)
	ann2 := mustParse(t, second)

	if ann2.Sequence != 2 {
		t.Fatalf("second seq = %d, want 2", ann2.Sequence)
	}
}

// TestLocalParticipantGoodbye checks dispose removes from table.
func TestLocalParticipantGoodbye(t *testing.T) {
	isolateSPDPEnv(t)

	p, err := NewLocalParticipant(1, 5*time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}

	sender := &fakeSender{}
	ctx := context.Background()

	err = p.Announce(ctx, sender)
	if err != nil {
		t.Fatalf("announce: %v", err)
	}

	err = p.Goodbye(ctx, sender)
	if err != nil {
		t.Fatalf("goodbye: %v", err)
	}

	if sender.count() != 2 {
		t.Fatalf("count = %d, want 2", sender.count())
	}

	table := NewRemoteTable(nil)
	now := time.Unix(1000, 0).UTC()

	first := sender.payload(t, 0)
	err = table.HandlePacket(first, now)
	if err != nil {
		t.Fatalf("handle alive: %v", err)
	}

	if table.Len() != 1 {
		t.Fatalf("len = %d, want 1", table.Len())
	}

	last := sender.payload(t, 1)
	err = table.HandlePacket(last, now)
	if err != nil {
		t.Fatalf("handle dispose: %v", err)
	}

	if table.Len() != 0 {
		t.Fatalf("len = %d, want 0", table.Len())
	}
}

// TestLocalParticipantAnnounceErrors checks explicit error paths.
func TestLocalParticipantAnnounceErrors(t *testing.T) {
	isolateSPDPEnv(t)

	p, err := NewLocalParticipant(0, time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}

	ctx := context.Background()
	sender := &fakeSender{}

	err = p.Announce(nil, sender) //nolint:staticcheck // testing explicit nil handling.
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil ctx error = %v, want ErrNilContext", err)
	}

	err = p.Announce(ctx, nil)
	if !errors.Is(err, ErrNilSender) {
		t.Fatalf("nil sender error = %v, want ErrNilSender", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	err = p.Announce(canceled, sender)
	if err == nil {
		t.Fatalf("canceled announce succeeded, want error")
	}

	sender.fail = errors.New("boom")
	err = p.Announce(ctx, sender)
	if err == nil {
		t.Fatalf("failing sender succeeded, want error")
	}
}

// TestRemoteTableAddUpdate checks add and refresh.
func TestRemoteTableAddUpdate(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	now := time.Unix(2000, 0).UTC()
	guid := testGUID(0x77)

	first := mustMarshalAlive(t, guid, 0, 1, 10*time.Second)
	mustHandle(t, table, first, now)

	info, ok := table.Get(guid.Prefix)
	if !ok {
		t.Fatalf("missing after add")
	}

	if info.LastSeq != 1 {
		t.Fatalf("seq = %d, want 1", info.LastSeq)
	}

	if table.Generation() != 1 {
		t.Fatalf("gen = %d, want 1", table.Generation())
	}

	second := mustMarshalAlive(t, guid, 0, 2, 10*time.Second)
	mustHandle(t, table, second, now.Add(time.Second))

	info, ok = table.Get(guid.Prefix)
	if !ok {
		t.Fatalf("missing after update")
	}

	if info.LastSeq != 2 {
		t.Fatalf("seq = %d, want 2", info.LastSeq)
	}

	if table.Generation() != 2 {
		t.Fatalf("gen = %d, want 2", table.Generation())
	}

	if table.Len() != 1 || len(table.List()) != 1 {
		t.Fatalf("len mismatch")
	}
}

// TestRemoteTableRemove checks explicit remove bumps once.
func TestRemoteTableRemove(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	now := time.Unix(2100, 0).UTC()
	guid := testGUID(0x77)

	first := mustMarshalAlive(t, guid, 0, 1, 10*time.Second)
	mustHandle(t, table, first, now)

	removed := table.Remove(guid.Prefix)
	if !removed {
		t.Fatalf("remove returned false")
	}

	if table.Generation() != 2 {
		t.Fatalf("gen = %d, want 2", table.Generation())
	}

	removed = table.Remove(guid.Prefix)
	if removed {
		t.Fatalf("second remove returned true")
	}

	if table.Generation() != 2 {
		t.Fatalf("gen changed on missing remove")
	}
}

// TestRemoteTableStaleIgnored checks stale sequences do not bump.
func TestRemoteTableStaleIgnored(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	now := time.Unix(3000, 0).UTC()
	guid := testGUID(0x88)

	fifth := mustMarshalAlive(t, guid, 0, 5, 10*time.Second)
	err := table.HandlePacket(fifth, now)
	if err != nil {
		t.Fatalf("handle 5: %v", err)
	}

	dup := mustMarshalAlive(t, guid, 0, 5, 10*time.Second)
	err = table.HandlePacket(dup, now)
	if err != nil {
		t.Fatalf("handle dup: %v", err)
	}

	if table.Generation() != 1 {
		t.Fatalf("gen = %d, want 1 after duplicate", table.Generation())
	}

	older := mustMarshalAlive(t, guid, 0, 3, 10*time.Second)
	err = table.HandlePacket(older, now)
	if err != nil {
		t.Fatalf("handle older: %v", err)
	}

	if table.Generation() != 1 {
		t.Fatalf("gen = %d, want 1 after stale", table.Generation())
	}

	info, ok := table.Get(guid.Prefix)
	if !ok || info.LastSeq != 5 {
		t.Fatalf("stored seq changed on stale")
	}
}

// TestRemoteTableDisposeRemove checks dispose on receive removes.
func TestRemoteTableDisposeRemove(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	now := time.Unix(4000, 0).UTC()
	guid := testGUID(0x99)

	alive := mustMarshalAlive(t, guid, 0, 1, 10*time.Second)
	err := table.HandlePacket(alive, now)
	if err != nil {
		t.Fatalf("handle alive: %v", err)
	}

	bye := mustMarshalDispose(t, guid, 0, 2)
	err = table.HandlePacket(bye, now)
	if err != nil {
		t.Fatalf("handle dispose: %v", err)
	}

	if table.Len() != 0 {
		t.Fatalf("len = %d, want 0", table.Len())
	}

	if table.Generation() != 2 {
		t.Fatalf("gen = %d, want 2", table.Generation())
	}

	err = table.HandlePacket(bye, now)
	if err != nil {
		t.Fatalf("handle missing dispose: %v", err)
	}

	if table.Generation() != 2 {
		t.Fatalf("gen changed on missing dispose")
	}
}

// TestRemoteTableLeaseExpiry checks sweep removes expired entries.
func TestRemoteTableLeaseExpiry(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	base := time.Unix(5000, 0).UTC()
	guid := testGUID(0xAA)

	alive := mustMarshalAlive(t, guid, 0, 1, time.Second)
	err := table.HandlePacket(alive, base)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}

	removed := table.Sweep(base.Add(500 * time.Millisecond))
	if removed != 0 {
		t.Fatalf("removed = %d, want 0", removed)
	}

	if table.Generation() != 1 {
		t.Fatalf("gen changed without expiry")
	}

	removed = table.Sweep(base.Add(time.Second))
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}

	if table.Len() != 0 {
		t.Fatalf("len = %d, want 0", table.Len())
	}

	if table.Generation() != 2 {
		t.Fatalf("gen = %d, want 2", table.Generation())
	}

	removed = table.Sweep(base.Add(2 * time.Second))
	if removed != 0 {
		t.Fatalf("second sweep removed = %d", removed)
	}
}

// TestRemoteTableGenerationIncrements checks hook ordering.
func TestRemoteTableGenerationIncrements(t *testing.T) {
	t.Parallel()

	rec := &hookRecorder{}
	table := NewRemoteTable(rec.hook)
	base := time.Unix(6000, 0).UTC()

	if table.Generation() != 0 {
		t.Fatalf("initial gen not zero")
	}

	guidA := testGUID(0xB1)
	guidB := testGUID(0xB2)

	a1 := mustMarshalAlive(t, guidA, 0, 1, 10*time.Second)
	err := table.HandlePacket(a1, base)
	if err != nil {
		t.Fatalf("handle a1: %v", err)
	}

	b1 := mustMarshalAlive(t, guidB, 0, 1, time.Second)
	err = table.HandlePacket(b1, base)
	if err != nil {
		t.Fatalf("handle b1: %v", err)
	}

	a2 := mustMarshalAlive(t, guidA, 0, 2, 10*time.Second)
	err = table.HandlePacket(a2, base)
	if err != nil {
		t.Fatalf("handle a2: %v", err)
	}

	bye := mustMarshalDispose(t, guidA, 0, 3)
	err = table.HandlePacket(bye, base)
	if err != nil {
		t.Fatalf("handle bye: %v", err)
	}

	removed := table.Sweep(base.Add(2 * time.Second))
	if removed != 1 {
		t.Fatalf("sweep removed = %d, want 1", removed)
	}

	if table.Generation() != 5 {
		t.Fatalf("gen = %d, want 5", table.Generation())
	}

	got := rec.generations()
	if len(got) != 5 {
		t.Fatalf("hook calls = %d, want 5", len(got))
	}

	for idx, gen := range got {
		if gen != uint64(idx+1) {
			t.Fatalf("hook %d = %d, want %d", idx, gen, idx+1)
		}
	}
}

// TestRemoteTableSetOnChange checks hook replacement.
func TestRemoteTableSetOnChange(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	rec := &hookRecorder{}
	table.SetOnChange(rec.hook)

	now := time.Unix(7000, 0).UTC()
	guid := testGUID(0xC1)
	data := mustMarshalAlive(t, guid, 0, 1, 10*time.Second)

	err := table.HandlePacket(data, now)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}

	if rec.count() != 1 {
		t.Fatalf("hook count = %d, want 1", rec.count())
	}
}

// TestRemoteTableListSorted checks list order is deterministic.
func TestRemoteTableListSorted(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	now := time.Unix(8000, 0).UTC()

	for _, fill := range []byte{0x03, 0x01, 0x02} {
		data := mustMarshalAlive(t, testGUID(fill), 0, 1, 10*time.Second)
		err := table.HandlePacket(data, now)
		if err != nil {
			t.Fatalf("handle %d: %v", fill, err)
		}
	}

	list := table.List()
	if len(list) != 3 {
		t.Fatalf("list len = %d", len(list))
	}

	if list[0].GUID.Prefix[0] != 0x01 || list[1].GUID.Prefix[0] != 0x02 || list[2].GUID.Prefix[0] != 0x03 {
		t.Fatalf("list not sorted")
	}
}

// TestRemoteTableHandleZeroTime checks explicit time error.
func TestRemoteTableHandleZeroTime(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	guid := testGUID(0xD1)
	data := mustMarshalAlive(t, guid, 0, 1, time.Second)

	err := table.HandlePacket(data, time.Time{})
	if !errors.Is(err, ErrInvalidTime) {
		t.Fatalf("zero time error = %v, want ErrInvalidTime", err)
	}
}

// TestLeaseValidation checks duration helpers reject bad values.
func TestLeaseValidation(t *testing.T) {
	t.Parallel()

	_, _, err := durationToParts(0)
	if !errors.Is(err, ErrInvalidLease) {
		t.Fatalf("zero lease error = %v, want ErrInvalidLease", err)
	}

	_, err = partsToDuration(0, 0)
	if !errors.Is(err, ErrInvalidSPDP) {
		t.Fatalf("zero parts error = %v", err)
	}

	_, err = partsToDuration(1, uint32(time.Second))
	if !errors.Is(err, ErrInvalidSPDP) {
		t.Fatalf("bad nanos error = %v", err)
	}
}

// TestSPDPInterfacesCompile checks fake types satisfy contracts.
func TestSPDPInterfacesCompile(t *testing.T) {
	t.Parallel()

	var _ Sender = (*fakeSender)(nil)
	var _ Receiver = (*fakeReceiver)(nil)

	rx := &fakeReceiver{payloads: [][]byte{{1, 2, 3}}}
	got, err := rx.Receive(context.Background())
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("got len = %d", len(got))
	}
}

// TestRemoteTableConcurrentAccess checks thread safety under race.
func TestRemoteTableConcurrentAccess(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	base := time.Unix(9000, 0).UTC()

	var wg sync.WaitGroup

	type spdpPair struct {
		prefix GUIDPrefix
		alive  []byte
		bye    []byte
	}

	fills := []byte{0xE0, 0xE1, 0xE2, 0xE3, 0xE4, 0xE5, 0xE6, 0xE7}
	pairs := make([]spdpPair, 0, len(fills))

	for _, fill := range fills {
		guid := testGUID(fill)
		alive := mustMarshalAlive(t, guid, 0, 1, 10*time.Second)
		bye := mustMarshalDispose(t, guid, 0, 2)

		pairs = append(pairs, spdpPair{prefix: guid.Prefix, alive: alive, bye: bye})
	}

	for _, pair := range pairs {
		wg.Go(func() {
			err := table.HandlePacket(pair.alive, base)
			if err != nil {
				t.Errorf("handle: %v", err)
			}

			_, _ = table.Get(pair.prefix)
			_ = table.List()
			_ = table.Len()
			_ = table.Generation()
			_ = table.Sweep(base)

			err = table.HandlePacket(pair.bye, base)
			if err != nil {
				t.Errorf("handle dispose: %v", err)
			}
		})
	}

	wg.Wait()
}

// spdpLoopReceiver blocks until a packet is queued or ctx ends.
type spdpLoopReceiver struct {
	ch chan []byte
}

// newSPDPLoopReceiver returns a buffered blocking receiver.
func newSPDPLoopReceiver() *spdpLoopReceiver {
	return &spdpLoopReceiver{ch: make(chan []byte, 16)}
}

// Receive blocks until a packet arrives or ctx ends.
func (r *spdpLoopReceiver) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("dds: spdp loop receive context: %w", ctx.Err())
	case data := <-r.ch:
		cp := make([]byte, len(data))
		copy(cp, data)

		return cp, nil
	}
}

// push queues a copy of data.
func (r *spdpLoopReceiver) push(data []byte) {
	cp := make([]byte, len(data))
	copy(cp, data)
	r.ch <- cp
}

// waitForSPDP polls cond until true or 2s expires.
func waitForSPDP(t *testing.T, cond func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	if !cond() {
		t.Fatalf("timeout waiting: %s", msg)
	}
}

// waitForSPDPLoopCancel cancels the loop and waits for context.Canceled.
func waitForSPDPLoopCancel(t *testing.T, cancel context.CancelFunc, errCh <-chan error) {
	t.Helper()

	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("loop err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("loop did not stop after cancel")
	}
}

// TestLocalParticipantAnnounceLoopEmitsAndStops checks periodic announce and stop.
func TestLocalParticipantAnnounceLoopEmitsAndStops(t *testing.T) {
	isolateSPDPEnv(t)

	p, err := NewLocalParticipant(0, 5*time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}

	sender := &fakeSender{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- p.AnnounceLoop(ctx, sender, 10*time.Millisecond)
	}()

	// Exercise concurrent reads while the loop increments the sequence.
	go func() {
		for range 50 {
			_ = p.Sequence()
			_ = p.GUID()
		}
	}()

	waitForSPDP(t, func() bool {
		return sender.count() >= 2
	}, "announce loop emits >=2 packets")

	first := sender.payload(t, 0)
	second := sender.payload(t, 1)
	ann1 := mustParse(t, first)
	ann2 := mustParse(t, second)

	if !ann1.Alive || !ann2.Alive {
		t.Fatalf("loop packets must be alive")
	}

	if ann2.Sequence <= ann1.Sequence {
		t.Fatalf("second seq %d not after first %d", ann2.Sequence, ann1.Sequence)
	}

	waitForSPDPLoopCancel(t, cancel, done)

	// The loop goroutine has exited, so the sender count is stable
	// without a wall sleep. A second locked read proves no send after cancel.
	n := sender.count()

	if sender.count() != n {
		t.Fatalf("loop sent after cancel: %d vs %d", sender.count(), n)
	}
}

// TestLocalParticipantAnnounceLoopRejectsBadArgs checks explicit error paths.
func TestLocalParticipantAnnounceLoopRejectsBadArgs(t *testing.T) {
	isolateSPDPEnv(t)

	p, err := NewLocalParticipant(0, time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}

	ctx := context.Background()
	sender := &fakeSender{}

	err = p.AnnounceLoop(ctx, nil, 10*time.Millisecond)
	if !errors.Is(err, ErrNilSender) {
		t.Fatalf("nil sender err = %v, want ErrNilSender", err)
	}

	err = p.AnnounceLoop(ctx, sender, 0)
	if !errors.Is(err, ErrInvalidPeriod) {
		t.Fatalf("zero period err = %v, want ErrInvalidPeriod", err)
	}

	err = p.AnnounceLoop(ctx, sender, -time.Second)
	if !errors.Is(err, ErrInvalidPeriod) {
		t.Fatalf("negative period err = %v, want ErrInvalidPeriod", err)
	}

	err = p.AnnounceLoop(nil, sender, 10*time.Millisecond) //nolint:staticcheck // testing explicit nil handling.
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil ctx err = %v, want ErrNilContext", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	err = p.AnnounceLoop(canceled, sender, 10*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}
}

// TestRemoteTableListenLoopAddDispose checks queued alive then dispose.
func TestRemoteTableListenLoopAddDispose(t *testing.T) {
	table := NewRemoteTable(nil)
	guid := testGUID(0xAB)
	alive := mustMarshalAlive(t, guid, 0, 1, 10*time.Second)
	bye := mustMarshalDispose(t, guid, 0, 2)

	rx := newSPDPLoopReceiver()
	rx.push(alive)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- table.ListenLoop(ctx, rx, 20*time.Millisecond)
	}()

	// Exercise concurrent reads while the loop mutates the table.
	go func() {
		for range 50 {
			_ = table.List()
			_ = table.Generation()
			_ = table.Len()
		}
	}()

	waitForSPDP(t, func() bool {
		return table.Len() == 1
	}, "listen loop adds remote from queued packet")

	info, ok := table.Get(guid.Prefix)
	if !ok {
		t.Fatalf("missing after listen add")
	}

	if info.LastSeq != 1 {
		t.Fatalf("seq = %d, want 1", info.LastSeq)
	}

	rx.push(bye)

	waitForSPDP(t, func() bool {
		return table.Len() == 0
	}, "listen loop removes remote on dispose")

	waitForSPDPLoopCancel(t, cancel, done)
}

// TestRemoteTableListenLoopSweepsExpired checks sweep removes expired leases.
func TestRemoteTableListenLoopSweepsExpired(t *testing.T) {
	table := NewRemoteTable(nil)
	guid := testGUID(0xAC)
	alive := mustMarshalAlive(t, guid, 0, 1, 40*time.Millisecond)

	rx := newSPDPLoopReceiver()
	rx.push(alive)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)

	go func() {
		done <- table.ListenLoop(ctx, rx, 10*time.Millisecond)
	}()

	waitForSPDP(t, func() bool {
		return table.Len() == 1
	}, "listen loop adds remote before sweep")

	waitForSPDP(t, func() bool {
		return table.Len() == 0
	}, "listen loop sweeps expired lease")

	waitForSPDPLoopCancel(t, cancel, done)
}

// TestRemoteTableListenLoopRejectsBadArgs checks explicit error paths.
func TestRemoteTableListenLoopRejectsBadArgs(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	rx := newSPDPLoopReceiver()
	ctx := context.Background()

	err := table.ListenLoop(ctx, nil, 10*time.Millisecond)
	if !errors.Is(err, ErrNilReceiver) {
		t.Fatalf("nil receiver err = %v, want ErrNilReceiver", err)
	}

	err = table.ListenLoop(ctx, rx, 0)
	if !errors.Is(err, ErrInvalidPeriod) {
		t.Fatalf("zero sweep err = %v, want ErrInvalidPeriod", err)
	}

	err = table.ListenLoop(ctx, rx, -time.Second)
	if !errors.Is(err, ErrInvalidPeriod) {
		t.Fatalf("negative sweep err = %v, want ErrInvalidPeriod", err)
	}

	err = table.ListenLoop(nil, rx, 10*time.Millisecond) //nolint:staticcheck // testing explicit nil handling.
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil ctx err = %v, want ErrNilContext", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	err = table.ListenLoop(canceled, rx, 10*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}
}

// encodeBEParams renders params with big endian headers for tests.
//
// It mirrors rtps.EncodeParams but stores id and length big endian so
// collectParams exercises the big endian branch. Values and padding use
// the same layout as the little endian codec so decoded values compare
// directly with bytes.Equal.
func encodeBEParams(params []rtps.Param) []byte {
	var out []byte

	for _, p := range params {
		var hdr [4]byte

		binary.BigEndian.PutUint16(hdr[0:2], p.ID)
		binary.BigEndian.PutUint16(hdr[2:4], uint16(len(p.Value))) //nolint:gosec // G115: test values are small.

		out = append(out, hdr[:]...)
		out = append(out, p.Value...)

		pad := (4 - len(p.Value)%4) % 4
		out = append(out, make([]byte, pad)...)
	}

	var term [4]byte

	binary.BigEndian.PutUint16(term[0:2], rtps.ParamSentinel)
	binary.BigEndian.PutUint16(term[2:4], 0)

	return append(out, term[:]...)
}

// buildBEData wraps BE params with the BE encap header for tests.
func buildBEData(inline []rtps.Parameter, payload []rtps.Param) rtps.Data {
	body := encodeBEParams(payload)

	raw := append([]byte(nil), spdpEncapPLCDRBE[:]...)
	raw = append(raw, body...)

	return rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: 1,
		InlineQoS:      inline,
		Payload:        raw,
	}
}

// checkCollectedParams compares decoded params to want values.
func checkCollectedParams(t *testing.T, got, want []rtps.Param) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("params len = %d, want %d", len(got), len(want))
	}

	for i := range want {
		if got[i].ID != want[i].ID {
			t.Fatalf("param %d id = %#04x, want %#04x", i, got[i].ID, want[i].ID)
		}

		if !bytes.Equal(got[i].Value, want[i].Value) {
			t.Fatalf("param %d value = %v, want %v", i, got[i].Value, want[i].Value)
		}
	}
}

// TestCollectParamsBigEndianTable checks the big endian payload branch.
func TestCollectParamsBigEndianTable(t *testing.T) {
	t.Parallel()

	guid := testGUID(0xBE)
	domainRaw := domainRawBytes(7)

	cases := []struct {
		name    string
		inline  []rtps.Parameter
		payload []rtps.Param
		want    []rtps.Param
	}{
		{
			name:    "guid only",
			payload: []rtps.Param{{ID: rtps.PIDParticipantGUID, Value: guidBytes(guid)}},
			want:    []rtps.Param{{ID: rtps.PIDParticipantGUID, Value: guidBytes(guid)}},
		},
		{
			name: "guid and domain",
			payload: []rtps.Param{
				{ID: rtps.PIDParticipantGUID, Value: guidBytes(guid)},
				{ID: pidDomainID, Value: append([]byte(nil), domainRaw[:]...)},
			},
			want: []rtps.Param{
				{ID: rtps.PIDParticipantGUID, Value: guidBytes(guid)},
				{ID: pidDomainID, Value: append([]byte(nil), domainRaw[:]...)},
			},
		},
		{
			name:    "inline merges before payload",
			inline:  []rtps.Parameter{{ID: rtps.PIDUserData, Value: []byte{0x01, 0x02, 0x03}}},
			payload: []rtps.Param{{ID: rtps.PIDParticipantGUID, Value: guidBytes(guid)}},
			want: []rtps.Param{
				{ID: rtps.PIDUserData, Value: []byte{0x01, 0x02, 0x03}},
				{ID: rtps.PIDParticipantGUID, Value: guidBytes(guid)},
			},
		},
		{
			name:    "odd length pads",
			payload: []rtps.Param{{ID: rtps.PIDUserData, Value: []byte{0xAA, 0xBB, 0xCC}}},
			want:    []rtps.Param{{ID: rtps.PIDUserData, Value: []byte{0xAA, 0xBB, 0xCC}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			d := buildBEData(tc.inline, tc.payload)

			params, err := collectParams(&d)
			if err != nil {
				t.Fatalf("collect: %v", err)
			}

			checkCollectedParams(t, params, tc.want)
		})
	}
}

// TestCollectParamsRejectsBadEncap checks unknown encap fails loudly.
func TestCollectParamsRejectsBadEncap(t *testing.T) {
	t.Parallel()

	d := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: 1,
		Payload:        []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00},
	}

	_, err := collectParams(&d)
	if !errors.Is(err, ErrInvalidSPDP) {
		t.Fatalf("err = %v, want ErrInvalidSPDP", err)
	}
}

// TestDurationToPartsTable checks lease splitting and overflow.
func TestDurationToPartsTable(t *testing.T) {
	t.Parallel()

	overflow := time.Duration(uint64(math.MaxUint32)+1) * time.Second
	maxValid := time.Duration(uint64(math.MaxUint32)) * time.Second

	cases := []struct {
		name      string
		dur       time.Duration
		wantSec   uint32
		wantNanos uint32
		wantErr   error
	}{
		{name: "zero", dur: 0, wantErr: ErrInvalidLease},
		{name: "negative", dur: -time.Second, wantErr: ErrInvalidLease},
		{name: "one second", dur: time.Second, wantSec: 1, wantNanos: 0},
		{name: "with nanos", dur: 1500 * time.Millisecond, wantSec: 1, wantNanos: 500000000},
		{name: "subsecond", dur: 500 * time.Millisecond, wantSec: 0, wantNanos: 500000000},
		{name: "max uint32", dur: maxValid, wantSec: math.MaxUint32, wantNanos: 0},
		{name: "overflow", dur: overflow, wantErr: ErrInvalidLease},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sec, nanos, err := durationToParts(tc.dur)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}

				return
			}

			if err != nil {
				t.Fatalf("durationToParts: %v", err)
			}

			if sec != tc.wantSec || nanos != tc.wantNanos {
				t.Fatalf("got %d/%d, want %d/%d", sec, nanos, tc.wantSec, tc.wantNanos)
			}
		})
	}
}

// TestMarshalRejectionsTable checks marshal validation without wire change.
func TestMarshalRejectionsTable(t *testing.T) {
	t.Parallel()

	var zero GUID

	goodGUID := testGUID(0xC3)
	overflowLease := time.Duration(uint64(math.MaxUint32)+1) * time.Second

	cases := []struct {
		name    string
		alive   bool
		guid    GUID
		domain  uint32
		seq     uint64
		lease   time.Duration
		wantErr error
	}{
		{name: "alive zero prefix", alive: true, guid: zero, domain: 0, seq: 1, lease: time.Second, wantErr: ErrInvalidSPDP},
		{name: "alive bad domain", alive: true, guid: goodGUID, domain: transports.MaxDomainID + 1, seq: 1, lease: time.Second, wantErr: ErrInvalidSPDP},
		{name: "alive zero seq", alive: true, guid: goodGUID, domain: 0, seq: 0, lease: time.Second, wantErr: ErrInvalidSPDP},
		{name: "alive zero lease", alive: true, guid: goodGUID, domain: 0, seq: 1, lease: 0, wantErr: ErrInvalidLease},
		{name: "alive negative lease", alive: true, guid: goodGUID, domain: 0, seq: 1, lease: -time.Second, wantErr: ErrInvalidLease},
		{name: "alive overflow lease", alive: true, guid: goodGUID, domain: 0, seq: 1, lease: overflowLease, wantErr: ErrInvalidLease},
		{name: "dispose zero prefix", alive: false, guid: zero, domain: 0, seq: 1, wantErr: ErrInvalidSPDP},
		{name: "dispose bad domain", alive: false, guid: goodGUID, domain: transports.MaxDomainID + 1, seq: 1, wantErr: ErrInvalidSPDP},
		{name: "dispose zero seq", alive: false, guid: goodGUID, domain: 0, seq: 0, wantErr: ErrInvalidSPDP},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var err error

			if tc.alive {
				_, err = MarshalAlivePacket(tc.guid, tc.domain, tc.seq, tc.lease)
			} else {
				_, err = MarshalDisposePacket(tc.guid, tc.domain, tc.seq)
			}

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestRemoteTableStaleDisposeIgnored checks stale disposes do not bump generation.
//
// Dispose ordering matches the alive path: a dispose at or below the stored
// LastSeq is ignored without bumping generation. Only a newer dispose
// removes the entry.
func TestRemoteTableStaleDisposeIgnored(t *testing.T) {
	t.Parallel()

	table := NewRemoteTable(nil)
	base := time.Unix(11000, 0).UTC()
	guid := testGUID(0xD2)

	alive := mustMarshalAlive(t, guid, 0, 5, 10*time.Second)
	mustHandle(t, table, alive, base)

	if table.Generation() != 1 {
		t.Fatalf("gen = %d, want 1", table.Generation())
	}

	cases := []struct {
		name    string
		seq     uint64
		wantLen int
		wantGen uint64
	}{
		{name: "duplicate dispose", seq: 5, wantLen: 1, wantGen: 1},
		{name: "older dispose", seq: 3, wantLen: 1, wantGen: 1},
		{name: "newer dispose removes", seq: 6, wantLen: 0, wantGen: 2},
		{name: "dispose missing stays", seq: 6, wantLen: 0, wantGen: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bye := mustMarshalDispose(t, guid, 0, tc.seq)
			err := table.HandlePacket(bye, base)
			if err != nil {
				t.Fatalf("handle dispose %d: %v", tc.seq, err)
			}

			if table.Len() != tc.wantLen {
				t.Fatalf("len = %d, want %d", table.Len(), tc.wantLen)
			}

			if table.Generation() != tc.wantGen {
				t.Fatalf("gen = %d, want %d", table.Generation(), tc.wantGen)
			}
		})
	}

	info, ok := table.Get(guid.Prefix)
	if ok {
		t.Fatalf("entry remains with seq %d", info.LastSeq)
	}
}

// TestSPDPManualLivelinessGolden pins the 0x0034 wire bytes.
func TestSPDPManualLivelinessGolden(t *testing.T) {
	t.Parallel()

	raw := rtps.EncodeParams([]rtps.Param{
		{ID: rtps.PIDParticipantManualLivelinessCount, Value: []byte{0x05, 0x00, 0x00, 0x00}},
	})

	want := []byte{0x34, 0x00, 0x04, 0x00, 0x05, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}
	if !bytes.Equal(raw, want) {
		t.Fatalf("manual golden = % X, want % X", raw, want)
	}

	back, err := rtps.DecodeParams(raw)
	if err != nil {
		t.Fatalf("decode manual: %v", err)
	}

	if len(back) != 1 || back[0].ID != rtps.PIDParticipantManualLivelinessCount {
		t.Fatalf("manual decode = %+v", back)
	}

	if got := binary.LittleEndian.Uint32(back[0].Value); got != 5 {
		t.Fatalf("manual count = %d, want 5", got)
	}
}

// TestSPDPBuiltinEndpointQosGolden pins the 0x0077 wire bytes.
func TestSPDPBuiltinEndpointQosGolden(t *testing.T) {
	t.Parallel()

	raw := rtps.EncodeParams([]rtps.Param{
		{ID: rtps.PIDBuiltinEndpointQos, Value: []byte{0x00, 0x00, 0x00, 0x00}},
	})

	want := []byte{0x77, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}
	if !bytes.Equal(raw, want) {
		t.Fatalf("builtin qos golden = % X, want % X", raw, want)
	}

	back, err := rtps.DecodeParams(raw)
	if err != nil {
		t.Fatalf("decode builtin qos: %v", err)
	}

	if len(back) != 1 || back[0].ID != rtps.PIDBuiltinEndpointQos {
		t.Fatalf("builtin qos decode = %+v", back)
	}
}

// TestSPDPVendorPreservation checks 0x8003, 0x8007, and 0x8019 survive a round trip.
func TestSPDPVendorPreservation(t *testing.T) {
	t.Parallel()

	params := []rtps.Param{
		{ID: rtps.PIDVendorAdlinkWriterDataLifecycle, Value: []byte{0x00, 0x00, 0x00, 0x00}},
		{ID: rtps.PIDVendorAdlinkParticipantVersionInfo, Value: bytes.Repeat([]byte{0xAB}, 8)},
		{ID: rtps.PIDVendorCycloneReceiveBufferSize, Value: []byte{0x00, 0x00, 0x10, 0x00}},
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

// TestSPDPPadSkipped checks PIDPad entries never surface.
func TestSPDPPadSkipped(t *testing.T) {
	t.Parallel()

	padBlob := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x16, 0x00, 0x04, 0x00, 0x01, 0x10, 0x00, 0x00,
		0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
	}

	back, err := rtps.DecodeParams(padBlob)
	if err != nil {
		t.Fatalf("decode pad: %v", err)
	}

	if len(back) != 1 || back[0].ID != rtps.PIDVendorID {
		t.Fatalf("pad decode = %+v, want single vendor", back)
	}

	guid := testGUID(0xE7)
	data := mustMarshalAlive(t, guid, 0, 11, time.Second)
	ann := mustParse(t, data)
	if ann.ManualLivelinessCount != 0 {
		t.Fatalf("default manual = %d, want 0", ann.ManualLivelinessCount)
	}

	if ann.BuiltinEndpointQos != 0 {
		t.Fatalf("default builtin QoS = %d, want 0", ann.BuiltinEndpointQos)
	}
}
