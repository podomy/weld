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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// mustSEDPIndexWithPrefix returns an index with a distinct prefix for tests.
func mustSEDPIndexWithPrefix(t *testing.T, prefixByte byte) *SEDPIndex {
	t.Helper()

	var prefix SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = prefixByte + byte(i)
	}

	idx, err := NewSEDPIndex(prefix, 0)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}

	return idx
}

// mustSEDPWriter declares a String writer and fails the test on error.
func mustSEDPWriter(t *testing.T, idx *SEDPIndex, rosTopic, typeHash string) SEDPEndpoint {
	t.Helper()

	ep, err := idx.DeclareWriter(rosTopic, "std_msgs__msg__String", typeHash, nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}

	return ep
}

// mustSEDPReader declares a String reader and fails the test on error.
func mustSEDPReader(t *testing.T, idx *SEDPIndex, rosTopic, typeHash string) SEDPEndpoint {
	t.Helper()

	ep, err := idx.DeclareReader(rosTopic, "std_msgs__msg__String", typeHash, nil)
	if err != nil {
		t.Fatalf("declare reader: %v", err)
	}

	return ep
}

// mustDecodeFixtureHex decodes a hex fixture and fails the test on error.
func mustDecodeFixtureHex(t *testing.T, raw string) []byte {
	t.Helper()

	data, err := hex.DecodeString(strings.Join(strings.Fields(raw), ""))
	if err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	return data
}

// fakeSEDPSender records DATA bytes sent through the SEDP sender.
type fakeSEDPSender struct {
	mu   sync.Mutex
	sent [][]byte
}

// SendSEDP records one DATA sample with a copy.
func (f *fakeSEDPSender) SendSEDP(ctx context.Context, data []byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("sedp_test: send context done: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.sent = append(f.sent, append([]byte(nil), data...))

	return nil
}

// count returns the number of recorded samples.
func (f *fakeSEDPSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.sent)
}

// snapshot returns copies of the recorded samples.
func (f *fakeSEDPSender) snapshot() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([][]byte, len(f.sent))
	for i, raw := range f.sent {
		out[i] = append([]byte(nil), raw...)
	}

	return out
}

// fakeSEDPReceiver replays queued DATA bytes through the receiver.
type fakeSEDPReceiver struct {
	mu    sync.Mutex
	queue [][]byte
}

// push queues one DATA sample with a copy.
func (f *fakeSEDPReceiver) push(data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.queue = append(f.queue, append([]byte(nil), data...))
}

// ReceiveSEDP returns the next queued sample or an empty slice.
func (f *fakeSEDPReceiver) ReceiveSEDP(ctx context.Context) ([]byte, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("sedp_test: receive context done: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.queue) == 0 {
		return []byte{}, nil
	}

	next := f.queue[0]
	f.queue = f.queue[1:]

	return append([]byte(nil), next...), nil
}

// checkSEDPWriterMapping asserts topic and type mapping agree.
func checkSEDPWriterMapping(t *testing.T, ep SEDPEndpoint) {
	t.Helper()

	if ep.DDSTopic != "rt/chatter" {
		t.Fatalf("dds topic = %q, want rt/chatter", ep.DDSTopic)
	}

	if ep.ROSTopic != "/chatter" {
		t.Fatalf("ros topic = %q, want /chatter", ep.ROSTopic)
	}

	if ep.TypeDDS != "std_msgs::msg::dds_::String_" {
		t.Fatalf("dds type = %q", ep.TypeDDS)
	}

	if ep.Kind != TopicKindTopic {
		t.Fatalf("kind = %q, want topic", ep.Kind)
	}

	ddsTopic, err := TopicName("/chatter", false)
	if err != nil {
		t.Fatalf("topic name: %v", err)
	}

	if ddsTopic != ep.DDSTopic {
		t.Fatalf("topic name = %q, want %q", ddsTopic, ep.DDSTopic)
	}

	decodedROS, kind, err := ParseTopicName(ep.DDSTopic)
	if err != nil {
		t.Fatalf("parse topic: %v", err)
	}

	if decodedROS != ep.ROSTopic || kind != TopicKindTopic {
		t.Fatalf("parse = (%q, %q)", decodedROS, kind)
	}

	decodedIDL, err := DemangleTypeName(ep.TypeDDS)
	if err != nil {
		t.Fatalf("demangle: %v", err)
	}

	if decodedIDL != "std_msgs__msg__String" {
		t.Fatalf("demangled = %q", decodedIDL)
	}
}

// checkSEDPWriterRoundTrip asserts serialize and parse agree.
func checkSEDPWriterRoundTrip(t *testing.T, ep SEDPEndpoint) {
	t.Helper()

	data, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	if string(data[0:4]) != sedpRTPSMagic {
		t.Fatalf("magic = %q, want RTPS", data[0:4])
	}

	decoded, err := ParseSEDPDATA(data)
	if err != nil {
		t.Fatalf("parse data: %v", err)
	}

	checkSEDPParsedEndpoint(t, decoded, ep)
}

// checkSEDPParsedEndpoint asserts a parsed endpoint matches the declared one.
func checkSEDPParsedEndpoint(t *testing.T, decoded, ep SEDPEndpoint) {
	t.Helper()

	if decoded.ROSTopic != ep.ROSTopic || decoded.DDSTopic != ep.DDSTopic {
		t.Fatalf("round trip topic = (%q, %q)", decoded.ROSTopic, decoded.DDSTopic)
	}

	if decoded.TypeDDS != ep.TypeDDS || decoded.TypeIDL != ep.TypeIDL {
		t.Fatalf("round trip type = (%q, %q)", decoded.TypeDDS, decoded.TypeIDL)
	}

	if decoded.TypeHash != ep.TypeHash {
		t.Fatalf("round trip hash = %q, want %q", decoded.TypeHash, ep.TypeHash)
	}

	if decoded.IsReader != ep.IsReader {
		t.Fatalf("round trip reader flag = %v", decoded.IsReader)
	}

	if decoded.DomainID != ep.DomainID {
		t.Fatalf("round trip domain = %d", decoded.DomainID)
	}

	if !decoded.ID.Equal(ep.ID) {
		t.Fatalf("round trip guid mismatch")
	}

	if !bytes.Equal(decoded.QoS, ep.QoS) {
		t.Fatalf("round trip qos = %x, want %x", decoded.QoS, ep.QoS)
	}
}

func TestSEDPDeclareWriterMappingReuse(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0x10)

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	checkSEDPWriterMapping(t, ep)
	checkSEDPWriterRoundTrip(t, ep)
}

// TestSEDPDeclareEntityNumbering checks user entities avoid builtin ids.
func TestSEDPDeclareEntityNumbering(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0x20)

	writer := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")
	reader := mustSEDPReader(t, idx, "/chatter", "RIHS01_abc")

	if writer.ID.Entity[3] != sedpUserWriterKind {
		t.Fatalf("writer kind = %#x, want %#x", writer.ID.Entity[3], sedpUserWriterKind)
	}

	if reader.ID.Entity[3] != sedpUserReaderKind {
		t.Fatalf("reader kind = %#x, want %#x", reader.ID.Entity[3], sedpUserReaderKind)
	}

	if writer.ID.Equal(reader.ID) {
		t.Fatalf("writer and reader share GUID %s", writer.ID.String())
	}

	counterOf := func(e SEDPEntityID) uint32 {
		return uint32(e[0])<<16 | uint32(e[1])<<8 | uint32(e[2])
	}

	if counterOf(writer.ID.Entity) < sedpUserEntityBase {
		t.Fatalf("writer counter below base")
	}

	if counterOf(reader.ID.Entity) < sedpUserEntityBase {
		t.Fatalf("reader counter below base")
	}

	if counterOf(writer.ID.Entity) == counterOf(reader.ID.Entity) {
		t.Fatalf("writer and reader share counter")
	}

	locals := idx.LocalEndpoints()
	if len(locals) != 2 {
		t.Fatalf("locals = %d, want 2", len(locals))
	}
}

// checkSEDPRemoteAdd asserts first upsert stores and bumps generation.
func checkSEDPRemoteAdd(t *testing.T, local *SEDPIndex, writer SEDPEndpoint) {
	t.Helper()

	err := local.UpsertRemote(writer)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	if got := local.Generation(); got != 1 {
		t.Fatalf("generation = %d, want 1", got)
	}

	remotes := local.RemoteEndpoints()
	if len(remotes) != 1 {
		t.Fatalf("remotes = %d, want 1", len(remotes))
	}
}

// checkSEDPRemoteNoop asserts identical upsert changes nothing.
func checkSEDPRemoteNoop(t *testing.T, local *SEDPIndex, writer SEDPEndpoint) {
	t.Helper()

	err := local.UpsertRemote(writer)
	if err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	if got := local.Generation(); got != 1 {
		t.Fatalf("generation after no-op = %d, want 1", got)
	}
}

// checkSEDPRemoteChange asserts changed upsert bumps generation.
func checkSEDPRemoteChange(t *testing.T, local *SEDPIndex, writer SEDPEndpoint) {
	t.Helper()

	changed := writer
	changed.TypeHash = "RIHS01_other"

	err := local.UpsertRemote(changed)
	if err != nil {
		t.Fatalf("changed upsert: %v", err)
	}

	if got := local.Generation(); got != 2 {
		t.Fatalf("generation after change = %d, want 2", got)
	}
}

// checkSEDPRemoteRemove asserts remove bumps once then misses cleanly.
func checkSEDPRemoteRemove(t *testing.T, local *SEDPIndex, writer SEDPEndpoint) {
	t.Helper()

	if removed := local.RemoveRemote(writer.ID); !removed {
		t.Fatalf("remove returned false, want true")
	}

	if got := local.Generation(); got != 3 {
		t.Fatalf("generation after remove = %d, want 3", got)
	}

	if removed := local.RemoveRemote(writer.ID); removed {
		t.Fatalf("second remove returned true, want false")
	}

	if got := local.Generation(); got != 3 {
		t.Fatalf("generation after miss = %d, want 3", got)
	}

	if n := len(local.RemoteEndpoints()); n != 0 {
		t.Fatalf("remotes = %d, want 0", n)
	}
}

// TestSEDPUpsertRemoveRemote checks remote add, no-op, and delete.
func TestSEDPUpsertRemoveRemote(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0x30)
	peer := mustSEDPIndexWithPrefix(t, 0x40)

	writer := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	checkSEDPRemoteAdd(t, local, writer)
	checkSEDPRemoteNoop(t, local, writer)
	checkSEDPRemoteChange(t, local, writer)
	checkSEDPRemoteRemove(t, local, writer)
}

// TestSEDPEndpointsMatchTable checks the topic type hash domain predicate.
func TestSEDPEndpointsMatchTable(t *testing.T) {
	t.Parallel()

	baseReader := SEDPEndpoint{
		ROSTopic: "/chatter",
		DDSTopic: "rt/chatter",
		Kind:     TopicKindTopic,
		TypeDDS:  "std_msgs::msg::dds_::String_",
		TypeIDL:  "std_msgs__msg__String",
		TypeHash: "RIHS01_abc",
		DomainID: 0,
		IsReader: true,
	}

	baseWriter := SEDPEndpoint{
		ROSTopic: "/chatter",
		DDSTopic: "rt/chatter",
		Kind:     TopicKindTopic,
		TypeDDS:  "std_msgs::msg::dds_::String_",
		TypeIDL:  "std_msgs__msg__String",
		TypeHash: "RIHS01_abc",
		DomainID: 0,
		IsReader: false,
	}

	cases := []struct {
		name   string
		reader SEDPEndpoint
		writer SEDPEndpoint
		want   bool
	}{
		{name: "match", reader: baseReader, writer: baseWriter, want: true},
		{name: "hash mismatch no-match", reader: baseReader, writer: sedpWithHash(baseWriter, "RIHS01_other"), want: false},
		{name: "topic mismatch no-match", reader: baseReader, writer: sedpWithROSTopic(baseWriter, "/other"), want: false},
		{name: "dds topic mismatch no-match", reader: baseReader, writer: sedpWithDDSTopic(baseWriter, "rt/other"), want: false},
		{name: "type mismatch no-match", reader: baseReader, writer: sedpWithType(baseWriter, "std_msgs::msg::dds_::Bool_"), want: false},
		{name: "domain mismatch no-match", reader: baseReader, writer: sedpWithDomain(baseWriter, 1), want: false},
		{name: "remote reader no-match", reader: baseReader, writer: sedpWithReaderFlag(baseWriter, true), want: false},
		{name: "local writer no-match", reader: sedpWithReaderFlag(baseReader, false), writer: baseWriter, want: false},
		{name: "empty hash matches empty", reader: sedpWithHash(baseReader, ""), writer: sedpWithHash(baseWriter, ""), want: true},
		// Empty hash wildcards for Humble pre-hash peers.
		{name: "empty vs set hash match", reader: sedpWithHash(baseReader, ""), writer: baseWriter, want: true},
		{name: "set vs empty hash match", reader: baseReader, writer: sedpWithHash(baseWriter, ""), want: true},
	}

	for _, tc := range cases {
		got := SEDPEndpointsMatch(tc.reader, tc.writer)
		if got != tc.want {
			t.Errorf("%s: match = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// sedpWithHash returns a copy with a replaced type hash.
func sedpWithHash(ep SEDPEndpoint, hash string) SEDPEndpoint {
	ep.TypeHash = hash

	return ep
}

// sedpWithROSTopic returns a copy with a replaced ROS topic.
func sedpWithROSTopic(ep SEDPEndpoint, topic string) SEDPEndpoint {
	ep.ROSTopic = topic

	return ep
}

// sedpWithDDSTopic returns a copy with a replaced DDS topic.
func sedpWithDDSTopic(ep SEDPEndpoint, topic string) SEDPEndpoint {
	ep.DDSTopic = topic

	return ep
}

// sedpWithType returns a copy with a replaced DDS type.
func sedpWithType(ep SEDPEndpoint, dtype string) SEDPEndpoint {
	ep.TypeDDS = dtype

	return ep
}

// sedpWithDomain returns a copy with a replaced domain.
func sedpWithDomain(ep SEDPEndpoint, domain uint32) SEDPEndpoint {
	ep.DomainID = domain

	return ep
}

// sedpWithReaderFlag returns a copy with a replaced reader flag.
func sedpWithReaderFlag(ep SEDPEndpoint, isReader bool) SEDPEndpoint {
	ep.IsReader = isReader

	return ep
}

// TestSEDPMatchingWriters checks reader to writer lookup through the index.
func TestSEDPMatchingWriters(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0x50)
	peer := mustSEDPIndexWithPrefix(t, 0x60)

	reader := mustSEDPReader(t, local, "/chatter", "RIHS01_abc")

	good := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")
	badHash := mustSEDPWriter(t, peer, "/chatter", "RIHS01_other")

	badHash.ID.Entity[2]++

	otherPeer := mustSEDPIndexWithPrefix(t, 0x70)
	otherTopic := mustSEDPWriter(t, otherPeer, "/other", "RIHS01_abc")

	for _, ep := range []SEDPEndpoint{good, badHash, otherTopic} {
		err := local.UpsertRemote(ep)
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	matched, err := local.MatchingWriters(reader.ID)
	if err != nil {
		t.Fatalf("matching writers: %v", err)
	}

	if len(matched) != 1 {
		t.Fatalf("matched = %d, want 1", len(matched))
	}

	if !matched[0].ID.Equal(good.ID) {
		t.Fatalf("matched wrong writer")
	}

	_, err = local.MatchingWriters(good.ID)
	if err == nil {
		t.Fatalf("matching unknown reader succeeded, want error")
	}

	writerLocal := mustSEDPWriter(t, local, "/chatter", "RIHS01_abc")

	_, err = local.MatchingWriters(writerLocal.ID)
	if !errors.Is(err, ErrSEDPInvalidEndpoint) {
		t.Fatalf("matching local writer = %v, want ErrSEDPInvalidEndpoint", err)
	}
}

// TestSEDPGenerationHookFires checks the hook runs on remote mutations.
func TestSEDPGenerationHookFires(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0x80)
	peer := mustSEDPIndexWithPrefix(t, 0x90)

	var mu sync.Mutex

	var calls []uint64

	local.SetOnChange(func(gen uint64) {
		mu.Lock()
		defer mu.Unlock()

		calls = append(calls, gen)
	})

	writer := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	err := local.UpsertRemote(writer)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	err = local.UpsertRemote(writer)
	if err != nil {
		t.Fatalf("no-op upsert: %v", err)
	}

	removed := local.RemoveRemote(writer.ID)
	if !removed {
		t.Fatalf("remove returned false")
	}

	removed = local.RemoveRemote(writer.ID)
	if removed {
		t.Fatalf("second remove returned true")
	}

	mu.Lock()
	defer mu.Unlock()

	if len(calls) != 2 {
		t.Fatalf("hook calls = %d, want 2", len(calls))
	}

	if calls[0] != 1 || calls[1] != 2 {
		t.Fatalf("hook calls = %v, want [1 2]", calls)
	}
}

// TestSEDPSenderReceiverInterfaces checks DATA flows through byte interfaces.
func TestSEDPSenderReceiverInterfaces(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xA0)

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	data, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	var sender SEDPSender = &fakeSEDPSender{}
	err = sender.SendSEDP(context.Background(), data)
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	fake, ok := sender.(*fakeSEDPSender)
	if !ok {
		t.Fatalf("sender has unexpected type")
	}

	if fake.count() != 1 {
		t.Fatalf("sent = %d, want 1", fake.count())
	}

	var receiver SEDPReceiver = &fakeSEDPReceiver{}

	rx, ok := receiver.(*fakeSEDPReceiver)
	if !ok {
		t.Fatalf("receiver has unexpected type")
	}

	rx.push(data)

	backBytes, err := receiver.ReceiveSEDP(context.Background())
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	if !bytes.Equal(backBytes, data) {
		t.Fatalf("receiver bytes differ")
	}

	decoded, err := ParseSEDPDATA(backBytes)
	if err != nil {
		t.Fatalf("parse received: %v", err)
	}

	if decoded.ROSTopic != "/chatter" {
		t.Fatalf("received topic = %q", decoded.ROSTopic)
	}
}

// mustMutateSEDP returns a copy of raw with one byte range replaced.
func mustMutateSEDP(t *testing.T, raw []byte, off int, patch []byte) []byte {
	t.Helper()

	if off < 0 || off+len(patch) > len(raw) {
		t.Fatalf("patch [%d:%d] out of %d", off, off+len(patch), len(raw))
	}

	out := append([]byte(nil), raw...)
	copy(out[off:], patch)

	return out
}

// TestSEDPParseRejectsBad checks malformed samples are rejected.
func TestSEDPParseRejectsBad(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xB0)

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	good, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	badMagic := mustMutateSEDP(t, good, 0, []byte{'X', 'X', 'X', 'X'})
	badMajor := mustMutateSEDP(t, good, sedpRTPSMajorOff, []byte{0x03})
	truncatedHeader := good[:10]
	truncatedSubmessage := good[:sedpRTPSHeaderLen+2]
	overrunLength := mustMutateSEDP(t, good, sedpRTPSHeaderLen+sedpSubmessageLengthOff, []byte{0xFF, 0xFF})

	shortDATA := append([]byte(nil), good[:sedpRTPSHeaderLen]...)
	shortDATA = append(shortDATA, sedpSubmessageDATA, sedpFlagLittleEndian|sedpFlagDataPresent, 0x04, 0x00, 0x01, 0x02, 0x03, 0x04)

	nonEndpointWriter := mustMutateSEDP(t, good, sedpRTPSHeaderLen+sedpSubmessageHeaderLen+sedpDATAWriterOff, []byte{0x00, 0x01, 0x00, 0xC1})

	noInlineNoPayload := append([]byte(nil), good[:sedpRTPSHeaderLen]...)
	noInlineNoPayload = append(noInlineNoPayload, sedpSubmessageDATA, sedpFlagLittleEndian)
	noInlineNoPayload = binary.LittleEndian.AppendUint16(noInlineNoPayload, sedpDataFixedLen)
	noInlineNoPayload = append(noInlineNoPayload, make([]byte, sedpDataFixedLen)...)

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "empty", data: []byte{}},
		{name: "short", data: []byte{1, 2, 3}},
		{name: "bad magic", data: badMagic},
		{name: "bad major", data: badMajor},
		{name: "truncated header", data: truncatedHeader},
		{name: "truncated submessage", data: truncatedSubmessage},
		{name: "overrun length", data: overrunLength},
		{name: "short DATA", data: shortDATA},
		{name: "non endpoint writer", data: nonEndpointWriter},
		{name: "no inline no payload", data: noInlineNoPayload},
	} {
		_, err := ParseSEDPDATA(tc.data)
		if err == nil {
			t.Errorf("%s: parse succeeded, want error", tc.name)
		}
	}
}

// mustSEDPMessage wraps one DATA body in an RTPS message for tests.
func mustSEDPMessage(t *testing.T, prefix SEDPGUIDPrefix, flags byte, body []byte) []byte {
	t.Helper()

	return mustSEDPMessageOrder(t, prefix, flags, body, true)
}

// mustSEDPMessageOrder wraps one DATA body with the given byte order.
//
// The submessage length follows the requested order: big endian when the
// flags omit the little endian bit.
func mustSEDPMessageOrder(t *testing.T, prefix SEDPGUIDPrefix, flags byte, body []byte, littleEndian bool) []byte {
	t.Helper()

	if len(body) > sedpMaxBodyLen {
		t.Fatalf("body %d above %d", len(body), sedpMaxBodyLen)
	}

	out := make([]byte, 0, sedpRTPSHeaderLen+sedpSubmessageHeaderLen+len(body))
	out = append(out, sedpRTPSMagic...)
	out = append(out, sedpRTPSMajor, sedpRTPSMinor, sedpVendorID0, sedpVendorID1)
	out = append(out, prefix[:]...)
	out = append(out, sedpSubmessageDATA, flags)

	if littleEndian {
		out = binary.LittleEndian.AppendUint16(out, uint16(len(body))) //nolint:gosec // G115: length checked above
	} else {
		out = binary.BigEndian.AppendUint16(out, uint16(len(body))) //nolint:gosec // G115: length checked above
	}

	out = append(out, body...)

	return out
}

// mustSEDPDataBody builds the fixed DATA prefix plus inline params for tests.
func mustSEDPDataBody(writer SEDPEntityID, params []rtps.Param) []byte {
	return mustSEDPDataBodyOrder(writer, params, true)
}

// mustSEDPDataBodyOrder builds the DATA prefix plus params in one byte order.
//
// Params encode little endian then shift to big endian on request, so
// value bytes must already carry the requested order.
func mustSEDPDataBodyOrder(writer SEDPEntityID, params []rtps.Param, littleEndian bool) []byte {
	encoded := rtps.EncodeParams(params)
	if !littleEndian {
		encoded = sedpSwapParamOrder(encoded)
	}

	body := make([]byte, 0, sedpDataFixedLen+len(encoded))
	body = append(body, sedpDATAReserved, sedpDATAReserved)

	if littleEndian {
		body = binary.LittleEndian.AppendUint16(body, sedpOctetsToInlineQoS)
	} else {
		body = binary.BigEndian.AppendUint16(body, sedpOctetsToInlineQoS)
	}

	body = append(body, sedpEntityUnknown[:]...)
	body = append(body, writer[:]...)

	if littleEndian {
		body = binary.LittleEndian.AppendUint64(body, 1)
	} else {
		body = binary.BigEndian.AppendUint64(body, 1)
	}

	body = append(body, encoded...)

	return body
}

// sedpEncodeBECDRString renders a CDR string in big endian order for tests.
func sedpEncodeBECDRString(s string) []byte {
	out := binary.BigEndian.AppendUint32(nil, uint32(len(s)+1)) //nolint:gosec // G115: test strings are short
	out = append(out, s...)

	return append(out, 0x00)
}

// sedpEncodeBEOctetSequence renders a CDR octet sequence in big endian for tests.
func sedpEncodeBEOctetSequence(raw string) []byte {
	out := binary.BigEndian.AppendUint32(nil, uint32(len(raw))) //nolint:gosec // G115: test strings are short

	return append(out, raw...)
}

// TestSEDPParseRejectsMissingIdentity checks identity params are required.
func TestSEDPParseRejectsMissingIdentity(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xB1)

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	build := func(t *testing.T, params []rtps.Param) []byte {
		t.Helper()

		body := mustSEDPDataBody(sedpPublicationsWriter, params)

		return mustSEDPMessage(t, ep.ID.Prefix, sedpFlagLittleEndian|sedpFlagInlineQoS|sedpFlagDataPresent, body)
	}

	full := build(t, []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/chatter")},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(ep.ID)},
	})

	topicOnly := build(t, []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/chatter")},
	})

	typeOnly := build(t, []rtps.Param{
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
	})

	badNUL := build(t, []rtps.Param{
		{ID: rtps.PIDTopicName, Value: []byte{0x04, 0x00, 0x00, 0x00, 'r', 't', 'x', 'x'}},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(ep.ID)},
	})

	zeroEntity := ep.ID
	zeroEntity.Entity = SEDPEntityID{}

	zeroGUID := build(t, []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/chatter")},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(zeroEntity)},
	})

	mismatchUserdata := build(t, []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/chatter")},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(ep.ID)},
		{ID: rtps.PIDUserData, Value: sedpEncodeOctetSequence([]byte("typehash=h;dds_topic=rt/other;domain=0"))},
	})

	badDomain := build(t, []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/chatter")},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(ep.ID)},
		{ID: rtps.PIDUserData, Value: sedpEncodeOctetSequence([]byte("typehash=h;dds_topic=rt/chatter;domain=999"))},
	})

	decoded, err := ParseSEDPDATA(full)
	if err != nil {
		t.Fatalf("parse full: %v", err)
	}

	if decoded.DDSTopic != "rt/chatter" || decoded.TypeHash != "" {
		t.Fatalf("full = (%q, %q)", decoded.DDSTopic, decoded.TypeHash)
	}

	for _, tc := range []struct {
		name string
		data []byte
	}{
		{name: "topic only", data: topicOnly},
		{name: "type only", data: typeOnly},
		{name: "missing NUL", data: badNUL},
		{name: "zero entity", data: zeroGUID},
		{name: "dds topic mismatch", data: mismatchUserdata},
		{name: "bad domain", data: badDomain},
	} {
		_, err := ParseSEDPDATA(tc.data)
		if err == nil {
			t.Errorf("%s: parse succeeded, want error", tc.name)
		}
	}
}

// TestSEDPDeclareRejectsBad checks invalid declares return explicit errors.
func TestSEDPDeclareRejectsBad(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xC0)

	_, err := idx.DeclareWriter("", "std_msgs__msg__String", "RIHS01_abc", nil)
	if err == nil {
		t.Fatalf("empty topic succeeded, want error")
	}

	_, err = idx.DeclareWriter("/chatter", "broken", "RIHS01_abc", nil)
	if err == nil {
		t.Fatalf("broken type succeeded, want error")
	}

	_, err = idx.DeclareWriter("/chatter", "std_msgs__msg__String", "bad;hash", nil)
	if err == nil {
		t.Fatalf("separator hash succeeded, want error")
	}

	_, err = NewSEDPIndex(SEDPGUIDPrefix{}, 999)
	if !errors.Is(err, ErrSEDPInvalidDomain) {
		t.Fatalf("bad domain = %v, want ErrSEDPInvalidDomain", err)
	}

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	badQoS := ep
	badQoS.QoS = []byte{1, 2, 3}

	data, err := SerializeSEDPDATA(badQoS)
	if err != nil {
		t.Fatalf("serialize with qos: %v", err)
	}

	decoded, err := ParseSEDPDATA(data)
	if err != nil {
		t.Fatalf("parse with qos: %v", err)
	}

	if !bytes.Equal(decoded.QoS, []byte{1, 2, 3}) {
		t.Fatalf("qos = %v", decoded.QoS)
	}

	if !strings.Contains(decoded.DDSTopic, "rt/") {
		t.Fatalf("dds topic %q misses rt prefix", decoded.DDSTopic)
	}
}

// waitForSEDP polls cond until true or the timeout expires.
func waitForSEDP(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	if !cond() {
		t.Fatalf("timeout waiting: %s", msg)
	}
}

// waitForSEDPLoopCancel cancels the loop and waits for ctx.Err.
func waitForSEDPLoopCancel(t *testing.T, cancel context.CancelFunc, errCh <-chan error) {
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

// collectSEDPGUIDs parses samples and returns GUID strings.
func collectSEDPGUIDs(t *testing.T, raws [][]byte) map[string]bool {
	t.Helper()

	seen := make(map[string]bool, len(raws))

	for _, raw := range raws {
		ep, err := ParseSEDPDATA(raw)
		if err != nil {
			t.Fatalf("parse announced: %v", err)
		}

		seen[ep.ID.String()] = true
	}

	return seen
}

// TestSEDPAnnounceLoopEmitsLocalsThenStops checks initial publish and stop.
func TestSEDPAnnounceLoopEmitsLocalsThenStops(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xD0)

	writer := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")
	reader := mustSEDPReader(t, idx, "/chatter", "RIHS01_abc")

	sender := &fakeSEDPSender{}

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)

	go func() {
		errCh <- idx.AnnounceLoop(ctx, sender)
	}()

	waitForSEDP(t, 2*time.Second, func() bool {
		return sender.count() >= 2
	}, "initial announce of 2 locals")

	waitForSEDPLoopCancel(t, cancel, errCh)

	seen := collectSEDPGUIDs(t, sender.snapshot())
	if !seen[writer.ID.String()] {
		t.Fatalf("initial announce misses writer %s", writer.ID.String())
	}

	if !seen[reader.ID.String()] {
		t.Fatalf("initial announce misses reader %s", reader.ID.String())
	}
}

// TestSEDPAnnounceLoopReannouncesOnLocalChange checks a declare triggers send.
func TestSEDPAnnounceLoopReannouncesOnLocalChange(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xD1)

	first := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	sender := &fakeSEDPSender{}

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)

	go func() {
		errCh <- idx.AnnounceLoop(ctx, sender)
	}()

	waitForSEDP(t, 2*time.Second, func() bool {
		return sender.count() >= 1
	}, "initial announce of 1 local")

	base := sender.count()

	second := mustSEDPReader(t, idx, "/other", "RIHS01_other")

	waitForSEDP(t, 3*time.Second, func() bool {
		return sender.count() >= base+2
	}, "re-announce after local declare")

	waitForSEDPLoopCancel(t, cancel, errCh)

	seen := collectSEDPGUIDs(t, sender.snapshot())
	if !seen[first.ID.String()] {
		t.Fatalf("re-announce misses first %s", first.ID.String())
	}

	if !seen[second.ID.String()] {
		t.Fatalf("re-announce misses second %s", second.ID.String())
	}
}

// TestSEDPListenLoopUpsertsThenChangeBumpsGeneration checks receive path.
func TestSEDPListenLoopUpsertsThenChangeBumpsGeneration(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xD2)
	peer := mustSEDPIndexWithPrefix(t, 0xD3)

	writer := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	data, err := SerializeSEDPDATA(writer)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	rx := &fakeSEDPReceiver{}
	rx.push(data)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)

	go func() {
		errCh <- local.ListenLoop(ctx, rx)
	}()

	waitForSEDP(t, 2*time.Second, func() bool {
		return local.Generation() == 1
	}, "listen upsert bumps generation to 1")

	remotes := local.RemoteEndpoints()
	if len(remotes) != 1 {
		t.Fatalf("remotes = %d, want 1", len(remotes))
	}

	if !remotes[0].ID.Equal(writer.ID) {
		t.Fatalf("remote GUID mismatch")
	}

	changed := writer
	changed.TypeHash = "RIHS01_other"

	changedData, err := SerializeSEDPDATA(changed)
	if err != nil {
		t.Fatalf("serialize changed: %v", err)
	}

	rx.push(changedData)

	waitForSEDP(t, 2*time.Second, func() bool {
		return local.Generation() == 2
	}, "listen change bumps generation to 2")

	waitForSEDPLoopCancel(t, cancel, errCh)
}

// TestSEDPAnnounceListenCancelSemantics checks ctx and nil handling.
func TestSEDPAnnounceListenCancelSemantics(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xD4)

	sender := &fakeSEDPSender{}
	rx := &fakeSEDPReceiver{}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	err := idx.AnnounceLoop(cancelled, sender)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("announce cancelled = %v, want context.Canceled", err)
	}

	err = idx.ListenLoop(cancelled, rx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("listen cancelled = %v, want context.Canceled", err)
	}

	err = idx.AnnounceLoop(context.Background(), nil)
	if err == nil {
		t.Fatalf("nil sender succeeded, want error")
	}

	err = idx.ListenLoop(context.Background(), nil)
	if err == nil {
		t.Fatalf("nil receiver succeeded, want error")
	}

	err = idx.AnnounceLoop(nil, sender) //nolint:staticcheck // SA1012: exercise nil context guard
	if err == nil {
		t.Fatalf("nil announce context succeeded, want error")
	}

	err = idx.ListenLoop(nil, rx) //nolint:staticcheck // SA1012: exercise nil context guard
	if err == nil {
		t.Fatalf("nil listen context succeeded, want error")
	}

	var nilIdx *SEDPIndex

	err = nilIdx.AnnounceLoop(context.Background(), sender)
	if err == nil {
		t.Fatalf("nil announce index succeeded, want error")
	}

	err = nilIdx.ListenLoop(context.Background(), rx)
	if err == nil {
		t.Fatalf("nil listen index succeeded, want error")
	}
}

// TestSEDPListenLoopSkipsMalformed checks one poison datagram cannot kill discovery.
func TestSEDPListenLoopSkipsMalformed(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xD5)

	rx := &fakeSEDPReceiver{}
	rx.push([]byte{1, 2, 3})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- local.ListenLoop(ctx, rx)
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("loop err = %v, want context.Canceled after poison skip", err)
	}

	if got := local.Generation(); got != 0 {
		t.Fatalf("generation = %d, want 0 after malformed", got)
	}
}

// TestSEDPSerializeBuiltinEntities checks the DATA writer and reader ids.
func TestSEDPSerializeBuiltinEntities(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xE0)

	writer := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")
	reader := mustSEDPReader(t, idx, "/chatter", "RIHS01_abc")

	writerData, err := SerializeSEDPDATA(writer)
	if err != nil {
		t.Fatalf("serialize writer: %v", err)
	}

	readerData, err := SerializeSEDPDATA(reader)
	if err != nil {
		t.Fatalf("serialize reader: %v", err)
	}

	checkSEDPDATAHeader(t, writerData, sedpPublicationsWriterCyclone)
	checkSEDPDATAHeader(t, readerData, sedpSubscriptionsWriter)
}

// checkSEDPDATAHeader asserts RTPS framing, writer ids, flags 0x07, and payload copy.
func checkSEDPDATAHeader(t *testing.T, raw []byte, wantWriter SEDPEntityID) {
	t.Helper()

	if len(raw) < sedpRTPSHeaderLen+sedpSubmessageHeaderLen+sedpDataFixedLen {
		t.Fatalf("sample too short: %d", len(raw))
	}

	if string(raw[0:4]) != sedpRTPSMagic {
		t.Fatalf("magic = %q", raw[0:4])
	}

	if raw[sedpRTPSMajorOff] != sedpRTPSMajor {
		t.Fatalf("major = %d", raw[sedpRTPSMajorOff])
	}

	if raw[sedpRTPSHeaderLen] != sedpSubmessageDATA {
		t.Fatalf("submessage = %#02x", raw[sedpRTPSHeaderLen])
	}

	if raw[sedpRTPSHeaderLen+sedpSubmessageFlagsOff] != sedpFlagLittleEndian|sedpFlagInlineQoS|sedpFlagDataPresent {
		t.Fatalf("flags = %#02x", raw[sedpRTPSHeaderLen+sedpSubmessageFlagsOff])
	}

	body := raw[sedpRTPSHeaderLen+sedpSubmessageHeaderLen:]
	if !bytes.Equal(body[sedpDATAReaderOff:sedpDATAReaderOff+sedpEntityLen], sedpEntityUnknown[:]) {
		t.Fatalf("reader id = %x, want unknown", body[sedpDATAReaderOff:sedpDATAReaderOff+sedpEntityLen])
	}

	if !bytes.Equal(body[sedpDATAWriterOff:sedpDATAWriterOff+sedpEntityLen], wantWriter[:]) {
		t.Fatalf("writer id = %x, want %x", body[sedpDATAWriterOff:sedpDATAWriterOff+sedpEntityLen], wantWriter[:])
	}

	dataBody, _, err := sedpFindEndpointDATA(raw)
	if err != nil {
		t.Fatalf("find endpoint DATA: %v", err)
	}

	rest := dataBody[sedpDataFixedLen:]
	_, consumed, err := sedpDecodeParams(rest, true)
	if err != nil {
		t.Fatalf("decode inline params for payload split: %v", err)
	}

	payload := rest[consumed:]
	if len(payload) < sedpEncapLen {
		t.Fatalf("payload copy too short: %d, want at least %d", len(payload), sedpEncapLen)
	}

	if !sedpIsParamListEncap(payload[:sedpEncapLen]) {
		t.Fatalf("payload encap = % X, want PL_CDR list", payload[:sedpEncapLen])
	}
}

// TestSEDPParseAcceptsCyclonePubsWriter checks the observed writer entity.
func TestSEDPParseAcceptsCyclonePubsWriter(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xE1)

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	params := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/chatter")},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(ep.ID)},
	}

	body := mustSEDPDataBody(sedpPublicationsWriterCyclone, params)

	raw := mustSEDPMessage(t, ep.ID.Prefix, sedpFlagLittleEndian|sedpFlagInlineQoS|sedpFlagDataPresent, body)

	decoded, err := ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse cyclone writer: %v", err)
	}

	if decoded.IsReader {
		t.Fatalf("cyclone pubs writer parsed as reader")
	}

	if !decoded.ID.Equal(ep.ID) {
		t.Fatalf("guid mismatch")
	}
}

// TestSEDPParseSkipsForeignDATA checks non endpoint DATA is skipped.
func TestSEDPParseSkipsForeignDATA(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xE2)

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	good, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	foreignBody := make([]byte, sedpDataFixedLen)
	foreignBody[sedpDATAWriterOff] = 0x00
	foreignBody[sedpDATAWriterOff+1] = 0x01
	foreignBody[sedpDATAWriterOff+2] = 0x00
	foreignBody[sedpDATAWriterOff+3] = 0xC1

	foreign := mustSEDPMessage(t, ep.ID.Prefix, sedpFlagLittleEndian|sedpFlagDataPresent, foreignBody)

	combined := append(append([]byte(nil), foreign...), good[sedpRTPSHeaderLen:]...)

	decoded, err := ParseSEDPDATA(combined)
	if err != nil {
		t.Fatalf("parse combined: %v", err)
	}

	if !decoded.ID.Equal(ep.ID) {
		t.Fatalf("guid mismatch")
	}
}

// TestSEDPParamCodecRoundTrip checks the local PID codec both orders.
func TestSEDPParamCodecRoundTrip(t *testing.T) {
	t.Parallel()

	params := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/chatter")},
		{ID: rtps.PIDUserData, Value: sedpEncodeOctetSequence([]byte("typehash=h;"))},
		{ID: rtps.PIDEndpointGUIDWire, Value: bytes.Repeat([]byte{0xAB}, 16)},
	}

	for _, littleEndian := range []bool{true, false} {
		encoded := rtps.EncodeParams(params)
		if !littleEndian {
			encoded = sedpSwapParamOrder(encoded)
		}

		decoded, consumed, err := sedpDecodeParams(encoded, littleEndian)
		if err != nil {
			t.Fatalf("decode le=%v: %v", littleEndian, err)
		}

		if consumed != len(encoded) {
			t.Fatalf("le=%v consumed = %d, want %d", littleEndian, consumed, len(encoded))
		}

		if len(decoded) != len(params) {
			t.Fatalf("le=%v params = %d, want %d", littleEndian, len(decoded), len(params))
		}

		for i := range params {
			if decoded[i].ID != params[i].ID || !bytes.Equal(decoded[i].Value, params[i].Value) {
				t.Fatalf("le=%v param %d mismatch", littleEndian, i)
			}
		}
	}
}

// sedpSwapParamOrder converts a little endian param blob to big endian.
//
// It walks the list with the little endian framing, then rewrites every
// id and length in big endian order for codec tests.
func sedpSwapParamOrder(blob []byte) []byte {
	out := append([]byte(nil), blob...)

	off := 0

	for off+4 <= len(out) {
		id := binary.LittleEndian.Uint16(out[off : off+2])
		length := int(binary.LittleEndian.Uint16(out[off+2 : off+4]))

		binary.BigEndian.PutUint16(out[off:off+2], id)
		binary.BigEndian.PutUint16(out[off+2:off+4], uint16(length)) //nolint:gosec // G115: length walked from a valid little endian blob

		off += 4 + length + (4-length%4)%4

		if id == rtps.PIDSentinel {
			break
		}
	}

	return out
}

// TestSEDPDecodeParamsToleratesInlinePad checks Cyclone style lengths.
//
// CycloneDDS includes the alignment pad in the parameter length, for
// example a 2 byte vendor id with length 4. The decoder must accept
// both padded and unpadded lengths for small fixed values.
func TestSEDPDecodeParamsToleratesInlinePad(t *testing.T) {
	t.Parallel()

	blob := []byte{
		0x16, 0x00, 0x04, 0x00, 0x01, 0x10, 0x00, 0x00,
		0x15, 0x00, 0x02, 0x00, 0x02, 0x01, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
	}

	decoded, consumed, err := sedpDecodeParams(blob, true)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if consumed != len(blob) {
		t.Fatalf("consumed = %d, want %d", consumed, len(blob))
	}

	if len(decoded) != 2 {
		t.Fatalf("params = %d, want 2", len(decoded))
	}

	if decoded[0].ID != rtps.PIDVendorID || len(decoded[0].Value) != 4 {
		t.Fatalf("vendor = (%#04x, %d)", decoded[0].ID, len(decoded[0].Value))
	}
}

// TestSEDPUserdataVariants checks hash optionality and unknown keys.
func TestSEDPUserdataVariants(t *testing.T) {
	t.Parallel()

	checkSEDPUserdataRosStyle(t)
	checkSEDPUserdataExtended(t)
	checkSEDPUserdataEmpty(t)

	if _, err := sedpParseEndpointUserdata("bare-token"); err == nil {
		t.Fatalf("bare token succeeded, want error")
	}

	if _, err := sedpEncodeEndpointUserdata("bad;hash", "rt/chatter", 0); err == nil {
		t.Fatalf("separator hash succeeded, want error")
	}
}

// checkSEDPUserdataRosStyle checks the trailing semicolon ROS 2 emits.
func checkSEDPUserdataRosStyle(t *testing.T) {
	t.Helper()

	rosStyle, err := sedpParseEndpointUserdata("typehash=RIHS01_abc;")
	if err != nil {
		t.Fatalf("parse ros style: %v", err)
	}

	if rosStyle.hash != "RIHS01_abc" {
		t.Fatalf("hash = %q", rosStyle.hash)
	}
}

// checkSEDPUserdataExtended checks unknown keys and the Weld additions.
func checkSEDPUserdataExtended(t *testing.T) {
	t.Helper()

	extended, err := sedpParseEndpointUserdata("serviceid= 1.2.3;typehash=RIHS01_abc;dds_topic=rt/chatter;domain=42")
	if err != nil {
		t.Fatalf("parse extended: %v", err)
	}

	if extended.hash != "RIHS01_abc" || !extended.hasDDSTopic || !extended.hasDomain {
		t.Fatalf("extended = %+v", extended)
	}

	if extended.ddsTopic != "rt/chatter" || extended.domain != 42 {
		t.Fatalf("extended values = (%q, %d)", extended.ddsTopic, extended.domain)
	}
}

// checkSEDPUserdataEmpty checks empty userdata parses to zero fields.
func checkSEDPUserdataEmpty(t *testing.T) {
	t.Helper()

	empty, err := sedpParseEndpointUserdata("")
	if err != nil {
		t.Fatalf("parse empty: %v", err)
	}

	if empty.hash != "" || empty.hasDDSTopic || empty.hasDomain {
		t.Fatalf("empty = %+v", empty)
	}
}

// CycloneDDS endpoint DATA fixtures.
//
// Both samples were captured on the loopback interface on domain 42
// while a CycloneDDS talker (ros2 topic pub /chatter) and listener
// (ros2 topic echo /sedpcap2) discovered a fake participant announced
// through a replayed SPDP packet. The endpoint DATA itself arrived on
// the SPDP multicast group 239.255.0.1 port 17900, which is where
// CycloneDDS multicasts SEDP announcements; directed SPDP, heartbeats,
// and ACKNACKs addressed to the fake participant unicast port were
// observed separately with INFO_DST naming the fake GUID prefix.
//
// Observed wire facts pinned by these fixtures:
//   - RTPS version 2.1 with vendor id 01:10.
//   - Publications writer entity 0x000003c2, subscriptions writer
//     entity 0x000004c2, readers 0x000003c7 and 0x000004c7. Note the
//     publications writer differs from the spec value 0x000002c2, which
//     this package still sends; parsing accepts both.
//   - DATA flags 0x05 (little endian plus data present) with the
//     unknown reader 0x00000000 and no inline QoS: endpoint params live
//     in the payload behind the PL_CDR_LE encapsulation 0x00030000.
//   - Parameter lengths include the alignment pad, for example vendor id
//     length 4 for 2 value bytes. Decoding accepts both styles.
//   - Endpoint PIDs: TOPIC_NAME 0x0005, TYPE_NAME 0x0007, HISTORY 0x0040,
//     RELIABILITY 0x001A (kind 2 with infinite blocking time on the
//     reliable writer, omitted on the best-effort reader), protocol
//     version 0x0015, vendor id 0x0016, ENDPOINT_GUID 0x005A with a 16
//     byte GUID, vendor PIDs 0x800C and 0x8003, then the sentinel.
//   - USER_DATA 0x002C carries an octet sequence with the ROS 2 style
//     "typehash=RIHS01_...;" string including the trailing semicolon.
//   - Endpoint GUID entities are small counters with kinds 0x03 for
//     writers and 0x04 for readers, matching the user entity kinds used
//     by DeclareWriter and DeclareReader.
//   - Each message bundles INFO_TS, the endpoint DATA, and heartbeats;
//     parsing skips every non DATA submessage by length.

// cycloneSubscriptionFixture is a CycloneDDS subscription DATA sample.
//
// Writer 0x000004c2 announces a reader for rt/sedpcap2 of type
// std_msgs::msg::dds_::String_ with the type hash below. The endpoint
// GUID entity is 0x00000a04 and the domain is absent, so parsing yields
// domain zero and ListenLoop stamps the receiver domain.
const cycloneSubscriptionFixture = "" +
	"525450530201011001103e68905df84386383be4090108004f89df6ae4a4d65f" +
	"1505ec000000100000000000000004c20000000003000000000300002c005800" +
	"5100000074797065686173683d5249485330315f646636363863373430343832" +
	"6262643438666233396437366137306466643462643539646231323838303231" +
	"37343335303332353965393438663662316131383b000000050010000c000000" +
	"72742f736564706361703200070024001d0000007374645f6d7367733a3a6d73" +
	"673a3a6464735f3a3a537472696e675f00000000400008000000000005000000" +
	"150004000201000016000400011000005a00100001103e68905df84386383be4" +
	"00000a040c8004000100000001000000"

// cyclonePublicationFixture is a CycloneDDS publication DATA sample.
//
// Writer 0x000003c2 announces a writer for rt/chatter of type
// std_msgs::msg::dds_::String_ with the type hash below. The reliability
// is best-effort safe to ignore here: the writer is reliable with kind 2
// and infinite blocking time. The endpoint GUID entity is 0x00000a03.
const cyclonePublicationFixture = "" +
	"52545053020101100110171e1c945c4d36c653e0090108004b89df6a4e032143" +
	"150504010000100000000000000003c20000000005000000000300002c005800" +
	"5100000074797065686173683d5249485330315f646636363863373430343832" +
	"6262643438666233396437366137306466643462643539646231323838303231" +
	"37343335303332353965393438663662316131383b000000050010000b000000" +
	"72742f636861747465720000070024001d0000007374645f6d7367733a3a6d73" +
	"673a3a6464735f3a3a537472696e675f000000001a000c0002000000ffffff7f" +
	"ffffffff40000800000000000a00000015000400020100001600040001100000" +
	"5a0010000110171e1c945c4d36c653e000000a030c8004000100000003800400" +
	"0000000001000000"

// TestSEDPParseCycloneSubscriptionFixture checks the captured reader DATA.
func TestSEDPParseCycloneSubscriptionFixture(t *testing.T) {
	t.Parallel()

	raw := mustDecodeFixtureHex(t, cycloneSubscriptionFixture)

	ep, err := ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse cyclone subscription: %v", err)
	}

	if !ep.IsReader {
		t.Fatalf("subscription parsed as writer")
	}

	if ep.ROSTopic != "/sedpcap2" || ep.DDSTopic != "rt/sedpcap2" {
		t.Fatalf("topic = (%q, %q)", ep.ROSTopic, ep.DDSTopic)
	}

	if ep.TypeDDS != "std_msgs::msg::dds_::String_" {
		t.Fatalf("type = %q", ep.TypeDDS)
	}

	if ep.TypeIDL != "std_msgs__msg__String" {
		t.Fatalf("idl = %q", ep.TypeIDL)
	}

	if ep.TypeHash != "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18" {
		t.Fatalf("hash = %q", ep.TypeHash)
	}

	var wantPrefix SEDPGUIDPrefix

	copy(wantPrefix[:], []byte{0x01, 0x10, 0x3e, 0x68, 0x90, 0x5d, 0xf8, 0x43, 0x86, 0x38, 0x3b, 0xe4})

	wantID := SEDPGUID{Prefix: wantPrefix, Entity: SEDPEntityID{0x00, 0x00, 0x0a, 0x04}}
	if !ep.ID.Equal(wantID) {
		t.Fatalf("guid = %s, want %s", ep.ID.String(), wantID.String())
	}

	if ep.DomainID != 0 {
		t.Fatalf("domain = %d, want 0 for missing domain", ep.DomainID)
	}

	if len(ep.QoS) == 0 {
		t.Fatalf("qos is empty, want opaque payload bytes")
	}
}

// TestSEDPParseCyclonePublicationFixture checks the captured writer DATA.
func TestSEDPParseCyclonePublicationFixture(t *testing.T) {
	t.Parallel()

	raw := mustDecodeFixtureHex(t, cyclonePublicationFixture)

	ep, err := ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse cyclone publication: %v", err)
	}

	if ep.IsReader {
		t.Fatalf("publication parsed as reader")
	}

	if ep.ROSTopic != "/chatter" || ep.DDSTopic != "rt/chatter" {
		t.Fatalf("topic = (%q, %q)", ep.ROSTopic, ep.DDSTopic)
	}

	if ep.TypeDDS != "std_msgs::msg::dds_::String_" {
		t.Fatalf("type = %q", ep.TypeDDS)
	}

	if ep.TypeHash != "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18" {
		t.Fatalf("hash = %q", ep.TypeHash)
	}

	var wantPrefix SEDPGUIDPrefix

	copy(wantPrefix[:], []byte{0x01, 0x10, 0x17, 0x1e, 0x1c, 0x94, 0x5c, 0x4d, 0x36, 0xc6, 0x53, 0xe0})

	wantID := SEDPGUID{Prefix: wantPrefix, Entity: SEDPEntityID{0x00, 0x00, 0x0a, 0x03}}
	if !ep.ID.Equal(wantID) {
		t.Fatalf("guid = %s, want %s", ep.ID.String(), wantID.String())
	}

	if ep.Kind != TopicKindTopic {
		t.Fatalf("kind = %q", ep.Kind)
	}
}

// TestSEDPListenLoopStampsCycloneDomain checks domain defaulting on receive.
func TestSEDPListenLoopStampsCycloneDomain(t *testing.T) {
	t.Parallel()

	var prefix SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = 0xF0 + byte(i)
	}

	local, err := NewSEDPIndex(prefix, 42)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}

	reader, err := local.DeclareReader("/chatter", "std_msgs__msg__String", "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18", nil)
	if err != nil {
		t.Fatalf("declare reader: %v", err)
	}

	rx := &fakeSEDPReceiver{}
	rx.push(mustDecodeFixtureHex(t, cyclonePublicationFixture))

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)

	go func() {
		errCh <- local.ListenLoop(ctx, rx)
	}()

	waitForSEDP(t, 2*time.Second, func() bool {
		return local.Generation() == 1
	}, "listen upsert of cyclone writer")

	waitForSEDPLoopCancel(t, cancel, errCh)

	remotes := local.RemoteEndpoints()
	if len(remotes) != 1 {
		t.Fatalf("remotes = %d, want 1", len(remotes))
	}

	if remotes[0].DomainID != 42 {
		t.Fatalf("remote domain = %d, want 42", remotes[0].DomainID)
	}

	matched, err := local.MatchingWriters(reader.ID)
	if err != nil {
		t.Fatalf("matching writers: %v", err)
	}

	if len(matched) != 1 {
		t.Fatalf("matched = %d, want 1", len(matched))
	}
}

// mustSEDPIdentityParams returns the topic, type, and GUID params for tests.
func mustSEDPIdentityParams(t *testing.T, ep SEDPEndpoint) []rtps.Param {
	t.Helper()

	return []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/chatter")},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(ep.ID)},
	}
}

// TestSEDPParamCountCap checks the sedpMaxParams bound through EncodeParams.
func TestSEDPParamCountCap(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xE9)

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	identity := mustSEDPIdentityParams(t, ep)

	build := func(t *testing.T, filler int) []byte {
		t.Helper()

		params := append([]rtps.Param(nil), identity...)

		// Filler uses an unknown vendor id so identity extraction
		// ignores the values while the count still grows.
		for range filler {
			params = append(params, rtps.Param{ID: 0x8000, Value: []byte{0x01}})
		}

		body := mustSEDPDataBody(sedpPublicationsWriter, params)

		return mustSEDPMessage(t, ep.ID.Prefix, sedpFlagLittleEndian|sedpFlagInlineQoS|sedpFlagDataPresent, body)
	}

	fits := build(t, sedpMaxParams-len(identity))

	decoded, err := ParseSEDPDATA(fits)
	if err != nil {
		t.Fatalf("parse at cap: %v", err)
	}

	if decoded.DDSTopic != "rt/chatter" {
		t.Fatalf("topic = %q, want rt/chatter", decoded.DDSTopic)
	}

	over := build(t, sedpMaxParams-len(identity)+1)

	_, err = ParseSEDPDATA(over)
	if !errors.Is(err, ErrSEDPInvalidDATA) {
		t.Fatalf("over cap = %v, want ErrSEDPInvalidDATA", err)
	}
}

// TestSEDPOverlongLengthPrefix checks 1025 byte CDR values are rejected.
func TestSEDPOverlongLengthPrefix(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", sedpMaxNameLen+1)
	atCap := strings.Repeat("a", sedpMaxNameLen)

	cases := []struct {
		name         string
		raw          []byte
		isString     bool
		littleEndian bool
		wantErr      bool
	}{
		{name: "string overlong LE", raw: sedpEncodeCDRString(long), isString: true, littleEndian: true, wantErr: true},
		{name: "string overlong BE", raw: sedpEncodeBECDRString(long), isString: true, littleEndian: false, wantErr: true},
		{name: "string at cap", raw: sedpEncodeCDRString(atCap), isString: true, littleEndian: true, wantErr: false},
		{name: "sequence overlong LE", raw: sedpEncodeOctetSequence([]byte(long)), littleEndian: true, wantErr: true},
		{name: "sequence overlong BE", raw: sedpEncodeBEOctetSequence(long), littleEndian: false, wantErr: true},
		{name: "sequence at cap", raw: sedpEncodeOctetSequence([]byte(atCap)), littleEndian: true, wantErr: false},
		{name: "string short prefix", raw: []byte{0x01}, isString: true, littleEndian: true, wantErr: true},
		{name: "sequence short prefix", raw: []byte{0x01}, littleEndian: true, wantErr: true},
	}

	for _, tc := range cases {
		var err error

		if tc.isString {
			_, err = sedpDecodeCDRString(tc.raw, tc.littleEndian)
		} else {
			_, err = sedpDecodeOctetSequence(tc.raw, tc.littleEndian)
		}

		if tc.wantErr && !errors.Is(err, ErrSEDPInvalidDATA) {
			t.Errorf("%s: decode = %v, want ErrSEDPInvalidDATA", tc.name, err)
		}

		if !tc.wantErr && err != nil {
			t.Errorf("%s: decode = %v, want nil", tc.name, err)
		}
	}
}

// TestSEDPDeclareEntitySpaceExhausted checks the counter cap and boundary.
func TestSEDPDeclareEntitySpaceExhausted(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xEA)
	idx.next = sedpUserEntityMax + 1

	_, err := idx.DeclareWriter("/chatter", "std_msgs__msg__String", "RIHS01_abc", nil)
	if !errors.Is(err, ErrSEDPInvalidEndpoint) {
		t.Fatalf("exhausted declare = %v, want ErrSEDPInvalidEndpoint", err)
	}

	boundary := mustSEDPIndexWithPrefix(t, 0xEB)
	boundary.next = sedpUserEntityMax

	_, err = boundary.DeclareWriter("/chatter", "std_msgs__msg__String", "RIHS01_abc", nil)
	if err != nil {
		t.Fatalf("boundary declare: %v", err)
	}

	_, err = boundary.DeclareReader("/chatter", "std_msgs__msg__String", "RIHS01_abc", nil)
	if !errors.Is(err, ErrSEDPInvalidEndpoint) {
		t.Fatalf("post boundary declare = %v, want ErrSEDPInvalidEndpoint", err)
	}
}

// TestSEDPUpsertRemoteInvalidTable checks invalid remote records fail loudly.
func TestSEDPUpsertRemoteInvalidTable(t *testing.T) {
	t.Parallel()

	peer := mustSEDPIndexWithPrefix(t, 0xEC)

	valid := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	zeroEntity := func(ep SEDPEndpoint) SEDPEndpoint {
		ep.ID.Entity = SEDPEntityID{}

		return ep
	}

	blankROS := func(ep SEDPEndpoint) SEDPEndpoint {
		ep.ROSTopic = "   "

		return ep
	}

	blankDDS := func(ep SEDPEndpoint) SEDPEndpoint {
		ep.DDSTopic = ""

		return ep
	}

	blankType := func(ep SEDPEndpoint) SEDPEndpoint {
		ep.TypeDDS = ""

		return ep
	}

	badDomain := func(ep SEDPEndpoint) SEDPEndpoint {
		ep.DomainID = sedpMaxDomainID + 1

		return ep
	}

	cases := []struct {
		name   string
		mutate func(SEDPEndpoint) SEDPEndpoint
		want   error
	}{
		{name: "zero entity", mutate: zeroEntity, want: ErrSEDPInvalidEndpoint},
		{name: "blank ros topic", mutate: blankROS, want: ErrSEDPInvalidTopic},
		{name: "blank dds topic", mutate: blankDDS, want: ErrSEDPInvalidTopic},
		{name: "blank type", mutate: blankType, want: ErrSEDPInvalidType},
		{name: "domain above max", mutate: badDomain, want: ErrSEDPInvalidDomain},
	}

	local := mustSEDPIndexWithPrefix(t, 0xED)

	for _, tc := range cases {
		err := local.UpsertRemote(tc.mutate(valid))
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: upsert = %v, want %v", tc.name, err, tc.want)
		}
	}

	if got := local.Generation(); got != 0 {
		t.Fatalf("generation = %d, want 0 after invalid upserts", got)
	}
}

// TestSEDPMatchingWritersKindAndIDL checks kind mismatch and IDL passthrough.
func TestSEDPMatchingWritersKindAndIDL(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xEE)
	peer := mustSEDPIndexWithPrefix(t, 0xEF)

	reader := mustSEDPReader(t, local, "/chatter", "RIHS01_abc")
	good := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	kindMismatch := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")
	kindMismatch.Kind = TopicKind("service")

	staleIDL := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")
	staleIDL.TypeIDL = "other__msg__Other"

	for _, ep := range []SEDPEndpoint{good, kindMismatch, staleIDL} {
		err := local.UpsertRemote(ep)
		if err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}

	matched, err := local.MatchingWriters(reader.ID)
	if err != nil {
		t.Fatalf("matching writers: %v", err)
	}

	if len(matched) != 2 {
		t.Fatalf("matched = %d, want 2 (good plus stale IDL)", len(matched))
	}

	seen := make(map[string]bool, len(matched))
	for _, ep := range matched {
		seen[ep.ID.String()] = true
	}

	if !seen[good.ID.String()] {
		t.Fatalf("matched misses good writer %s", good.ID.String())
	}

	// Matching compares the DDS type name on the wire, so a stale IDL
	// label still matches when the wire type agrees.
	if !seen[staleIDL.ID.String()] {
		t.Fatalf("matched misses stale IDL writer %s", staleIDL.ID.String())
	}

	if seen[kindMismatch.ID.String()] {
		t.Fatalf("matched includes kind mismatch %s", kindMismatch.ID.String())
	}
}

// TestSEDPParseBigEndianEndToEnd checks a big endian DATA sample parses.
func TestSEDPParseBigEndianEndToEnd(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xF0)

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	params := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeBECDRString("rt/chatter")},
		{ID: rtps.PIDTypeName, Value: sedpEncodeBECDRString("std_msgs::msg::dds_::String_")},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(ep.ID)},
		{ID: rtps.PIDUserData, Value: sedpEncodeBEOctetSequence("typehash=RIHS01_abc;dds_topic=rt/chatter;domain=0")},
	}

	body := mustSEDPDataBodyOrder(sedpPublicationsWriter, params, false)
	raw := mustSEDPMessageOrder(t, ep.ID.Prefix, sedpFlagInlineQoS|sedpFlagDataPresent, body, false)

	decoded, err := ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse big endian: %v", err)
	}

	checkSEDPParsedEndpoint(t, decoded, ep)
}

// TestSEDPListenLoopSkipsEmpty checks empty samples are skipped, not stored.
func TestSEDPListenLoopSkipsEmpty(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xF1)
	peer := mustSEDPIndexWithPrefix(t, 0xF2)

	writer := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	data, err := SerializeSEDPDATA(writer)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	rx := &fakeSEDPReceiver{}
	rx.push([]byte{})
	rx.push(data)

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)

	go func() {
		errCh <- local.ListenLoop(ctx, rx)
	}()

	waitForSEDP(t, 2*time.Second, func() bool {
		return local.Generation() == 1
	}, "listen skips empty then upserts")

	remotes := local.RemoteEndpoints()
	if len(remotes) != 1 {
		t.Fatalf("remotes = %d, want 1", len(remotes))
	}

	if !remotes[0].ID.Equal(writer.ID) {
		t.Fatalf("remote GUID mismatch after empty skip")
	}

	waitForSEDPLoopCancel(t, cancel, errCh)
}

// TestSEDPConcurrentSmoke checks concurrent declare, upsert, and remove.
func TestSEDPConcurrentSmoke(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xF3)
	peer := mustSEDPIndexWithPrefix(t, 0xF4)

	remote := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	const workers = 8

	const declaresPerWorker = 25

	var wg sync.WaitGroup

	for i := range workers {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()

			for j := range declaresPerWorker {
				topic := "/smoke" + strconv.Itoa((worker+j)%4)

				_, err := idx.DeclareWriter(topic, "std_msgs__msg__String", "RIHS01_abc", nil)
				if err != nil {
					t.Errorf("declare: %v", err)
				}

				err = idx.UpsertRemote(remote)
				if err != nil {
					t.Errorf("upsert: %v", err)
				}

				_ = idx.LocalEndpoints()
				_ = idx.RemoteEndpoints()

				idx.RemoveRemote(remote.ID)
			}
		}(i)
	}

	wg.Wait()

	locals := idx.LocalEndpoints()
	if len(locals) != workers*declaresPerWorker {
		t.Fatalf("locals = %d, want %d", len(locals), workers*declaresPerWorker)
	}
}

// TestSEDPOnChangeNilSafe checks a nil hook never fires and never panics.
func TestSEDPOnChangeNilSafe(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xF5)
	peer := mustSEDPIndexWithPrefix(t, 0xF6)

	local.SetOnChange(nil)

	writer := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	err := local.UpsertRemote(writer)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	if removed := local.RemoveRemote(writer.ID); !removed {
		t.Fatalf("remove returned false, want true")
	}

	if got := local.Generation(); got != 2 {
		t.Fatalf("generation = %d, want 2", got)
	}
}

// TestSEDPRemoteSortOrder checks remote snapshots sort by GUID.
func TestSEDPRemoteSortOrder(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0x10)
	high := mustSEDPIndexWithPrefix(t, 0x90)
	low := mustSEDPIndexWithPrefix(t, 0x20)

	highWriter := mustSEDPWriter(t, high, "/chatter", "RIHS01_abc")
	lowWriter := mustSEDPWriter(t, low, "/chatter", "RIHS01_abc")

	err := local.UpsertRemote(highWriter)
	if err != nil {
		t.Fatalf("upsert high: %v", err)
	}

	err = local.UpsertRemote(lowWriter)
	if err != nil {
		t.Fatalf("upsert low: %v", err)
	}

	remotes := local.RemoteEndpoints()
	if len(remotes) != 2 {
		t.Fatalf("remotes = %d, want 2", len(remotes))
	}

	if !remotes[0].ID.Equal(lowWriter.ID) || !remotes[1].ID.Equal(highWriter.ID) {
		t.Fatalf("remotes not sorted: %s, %s", remotes[0].ID.String(), remotes[1].ID.String())
	}
}

// TestSEDPAnnounceLoopZeroLocals checks an empty index sends nothing.
func TestSEDPAnnounceLoopZeroLocals(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xE4)

	sender := &fakeSEDPSender{}

	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)

	go func() {
		errCh <- idx.AnnounceLoop(ctx, sender)
	}()

	// Zero locals send nothing on the initial snapshot, and the poll
	// ticker also sends nothing while locals stay empty. Check the
	// deterministic fake sender before and after cancel without a wall sleep.
	if got := sender.count(); got != 0 {
		t.Fatalf("sent = %d, want 0 with zero locals", got)
	}

	waitForSEDPLoopCancel(t, cancel, errCh)

	if got := sender.count(); got != 0 {
		t.Fatalf("sent = %d, want 0 with zero locals", got)
	}
}

// TestSEDPQoSNilEmptyEqual checks nil and empty QoS compare as no change.
func TestSEDPQoSNilEmptyEqual(t *testing.T) {
	t.Parallel()

	peer := mustSEDPIndexWithPrefix(t, 0xE5)
	local := mustSEDPIndexWithPrefix(t, 0xE6)

	writer := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	err := local.UpsertRemote(writer)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	empty := writer
	empty.QoS = []byte{}

	err = local.UpsertRemote(empty)
	if err != nil {
		t.Fatalf("upsert empty qos: %v", err)
	}

	if got := local.Generation(); got != 1 {
		t.Fatalf("generation = %d, want 1 after nil to empty qos", got)
	}
}

// TestSEDPGenerationIgnoresLocals checks declares leave generation alone.
func TestSEDPGenerationIgnoresLocals(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xE2)
	peer := mustSEDPIndexWithPrefix(t, 0xE3)

	writer := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	err := local.UpsertRemote(writer)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	_, err = local.DeclareWriter("/chatter", "std_msgs__msg__String", "RIHS01_abc", nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}

	_, err = local.DeclareReader("/chatter", "std_msgs__msg__String", "RIHS01_abc", nil)
	if err != nil {
		t.Fatalf("declare reader: %v", err)
	}

	if got := local.Generation(); got != 1 {
		t.Fatalf("generation = %d, want 1 after local declares", got)
	}

	if n := len(local.LocalEndpoints()); n != 2 {
		t.Fatalf("locals = %d, want 2", n)
	}
}

// TestSEDPSerializeRejectsMismatch checks marshal time mapping checks.
func TestSEDPSerializeRejectsMismatch(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xE7)

	valid := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	zeroEntity := func(ep SEDPEndpoint) SEDPEndpoint {
		ep.ID.Entity = SEDPEntityID{}

		return ep
	}

	badDomain := func(ep SEDPEndpoint) SEDPEndpoint {
		ep.DomainID = sedpMaxDomainID + 1

		return ep
	}

	rosMismatch := func(ep SEDPEndpoint) SEDPEndpoint {
		ep.ROSTopic = "/other"

		return ep
	}

	kindMismatch := func(ep SEDPEndpoint) SEDPEndpoint {
		ep.Kind = TopicKind("service")

		return ep
	}

	idlMismatch := func(ep SEDPEndpoint) SEDPEndpoint {
		ep.TypeIDL = "other__msg__Other"

		return ep
	}

	cases := []struct {
		name   string
		mutate func(SEDPEndpoint) SEDPEndpoint
		want   error
	}{
		{name: "zero entity", mutate: zeroEntity, want: ErrSEDPInvalidEndpoint},
		{name: "domain above max", mutate: badDomain, want: ErrSEDPInvalidDomain},
		{name: "ros topic mismatch", mutate: rosMismatch, want: ErrSEDPInvalidTopic},
		{name: "kind mismatch", mutate: kindMismatch, want: ErrSEDPInvalidTopic},
		{name: "idl mismatch", mutate: idlMismatch, want: ErrSEDPInvalidType},
	}

	for _, tc := range cases {
		_, err := SerializeSEDPDATA(tc.mutate(valid))
		if !errors.Is(err, tc.want) {
			t.Errorf("%s: serialize = %v, want %v", tc.name, err, tc.want)
		}
	}

	_, err := SerializeSEDPDATA(valid)
	if err != nil {
		t.Fatalf("serialize valid: %v", err)
	}
}

// TestSEDPFirstDuplicateWins checks the first identity param wins.
func TestSEDPFirstDuplicateWins(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xE8)

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	params := []rtps.Param{
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/chatter")},
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/other")},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(ep.ID)},
	}

	body := mustSEDPDataBody(sedpPublicationsWriter, params)
	raw := mustSEDPMessage(t, ep.ID.Prefix, sedpFlagLittleEndian|sedpFlagInlineQoS|sedpFlagDataPresent, body)

	decoded, err := ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if decoded.DDSTopic != "rt/chatter" {
		t.Fatalf("topic = %q, want first rt/chatter", decoded.DDSTopic)
	}
}

// sedpTestSeqOf returns the sequence number of a serialized DATA sample.
func sedpTestSeqOf(t *testing.T, raw []byte) uint64 {
	t.Helper()

	body := raw[sedpRTPSHeaderLen+sedpSubmessageHeaderLen:]

	return binary.LittleEndian.Uint64(body[sedpDATASeqOff : sedpDATASeqOff+sedpSeqLen])
}

// TestSEDPSequenceIncreases checks sent sequence numbers grow monotonically.
func TestSEDPSequenceIncreases(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xDF)

	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	first, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize first: %v", err)
	}

	second, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize second: %v", err)
	}

	firstSeq := sedpTestSeqOf(t, first)
	secondSeq := sedpTestSeqOf(t, second)

	if firstSeq == 0 {
		t.Fatalf("first sequence is zero, want nonzero")
	}

	if secondSeq <= firstSeq {
		t.Fatalf("second sequence %d not above first %d", secondSeq, firstSeq)
	}
}

// failSEDPSender returns a fixed error from SendSEDP.
type failSEDPSender struct {
	err error
}

// SendSEDP returns the fixed error unless ctx is done.
func (f *failSEDPSender) SendSEDP(ctx context.Context, data []byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("sedp_test: fail send context done: %w", err)
	}

	_ = data

	return f.err
}

// failSEDPReceiver returns a fixed error from ReceiveSEDP.
type failSEDPReceiver struct {
	err error
}

// ReceiveSEDP returns the fixed error unless ctx is done.
func (f *failSEDPReceiver) ReceiveSEDP(ctx context.Context) ([]byte, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("sedp_test: fail receive context done: %w", err)
	}

	return nil, f.err
}

// TestSEDPAnnounceLoopSendError checks send failures surface the cause.
func TestSEDPAnnounceLoopSendError(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xE1)

	mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	boom := errors.New("sedp_test: boom send")

	err := idx.AnnounceLoop(context.Background(), &failSEDPSender{err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("announce = %v, want boom", err)
	}
}

// TestSEDPListenLoopReceiveError checks receive failures surface the cause.
func TestSEDPListenLoopReceiveError(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xE0)

	boom := errors.New("sedp_test: boom receive")

	err := local.ListenLoop(context.Background(), &failSEDPReceiver{err: boom})
	if !errors.Is(err, boom) {
		t.Fatalf("listen = %v, want boom", err)
	}
}

// TestSEDPVendorGoldenPinsCycloneParams checks vendor PIDs from live captures.
func TestSEDPVendorGoldenPinsCycloneParams(t *testing.T) {
	t.Parallel()

	checkSEDPSubVendor(t)
	checkSEDPPubVendor(t)
}

// checkSEDPSubVendor pins subscription vendor 0x800c from live capture.
func checkSEDPSubVendor(t *testing.T) {
	t.Helper()

	subRaw := mustDecodeFixtureHex(t, cycloneSubscriptionFixture)
	subBody, subFlags, err := sedpFindEndpointDATA(subRaw)
	if err != nil {
		t.Fatalf("find sub data: %v", err)
	}

	subParams, _, err := sedpCollectParams(subBody, subFlags)
	if err != nil {
		t.Fatalf("collect sub params: %v", err)
	}

	found800c := false
	for _, p := range subParams {
		if p.ID == rtps.PIDVendorAdlinkEntityFactory {
			found800c = true
			if len(p.Value) != 4 || p.Value[0] != 0x01 {
				t.Fatalf("sub 0x800c = % X, want 01 00 00 00", p.Value)
			}
		}
	}

	if !found800c {
		t.Fatalf("sub misses vendor 0x800c")
	}
}

// checkSEDPPubVendor pins publication vendor 0x8003 and 0x800c.
func checkSEDPPubVendor(t *testing.T) {
	t.Helper()

	pubRaw := mustDecodeFixtureHex(t, cyclonePublicationFixture)
	pubBody, pubFlags, err := sedpFindEndpointDATA(pubRaw)
	if err != nil {
		t.Fatalf("find pub data: %v", err)
	}

	pubParams, _, err := sedpCollectParams(pubBody, pubFlags)
	if err != nil {
		t.Fatalf("collect pub params: %v", err)
	}

	found8003 := false
	found800cPub := false
	for _, p := range pubParams {
		if p.ID == rtps.PIDVendorAdlinkWriterDataLifecycle {
			found8003 = true
			if len(p.Value) != 4 {
				t.Fatalf("pub 0x8003 len = %d, want 4", len(p.Value))
			}
		}
		if p.ID == rtps.PIDVendorAdlinkEntityFactory {
			found800cPub = true
		}
	}

	if !found8003 {
		t.Fatalf("pub misses vendor 0x8003")
	}

	if !found800cPub {
		t.Fatalf("pub misses vendor 0x800c")
	}
}

// TestSEDPBuiltinQosGolden pins the 0x0077 wire bytes.
func TestSEDPBuiltinQosGolden(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xA5)
	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")
	ep.BuiltinEndpointQos = 0x3F

	data, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	decoded, err := ParseSEDPDATA(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if decoded.BuiltinEndpointQos != 0x3F {
		t.Fatalf("builtin QoS = %#x, want 0x3F", decoded.BuiltinEndpointQos)
	}

	body, flags, err := sedpFindEndpointDATA(data)
	if err != nil {
		t.Fatalf("find data: %v", err)
	}

	params, _, err := sedpCollectParams(body, flags)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}

	found := false
	for _, p := range params {
		if p.ID == rtps.PIDBuiltinEndpointQos {
			found = true
			if len(p.Value) != 4 {
				t.Fatalf("builtin QoS len = %d, want 4", len(p.Value))
			}
		}
	}

	if !found {
		t.Fatalf("builtin QoS PID 0x0077 missing on wire")
	}
}

// TestSEDPTypeObjectStub checks the XTypes TypeObject passthrough.
func TestSEDPTypeObjectStub(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xA6)
	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")
	ep.TypeObject = []byte{0x01, 0x02, 0x03, 0x04}

	data, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	decoded, err := ParseSEDPDATA(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if !bytes.Equal(decoded.TypeObject, ep.TypeObject) {
		t.Fatalf("type object = % X, want % X", decoded.TypeObject, ep.TypeObject)
	}

	if decoded.DDSTopic != "rt/chatter" {
		t.Fatalf("topic = %q", decoded.DDSTopic)
	}
}

// TestSEDPTypeObjectAbsentKeepsNil checks default endpoints carry no TypeObject.
func TestSEDPTypeObjectAbsentKeepsNil(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xA7)
	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	data, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	decoded, err := ParseSEDPDATA(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if len(decoded.TypeObject) != 0 {
		t.Fatalf("type object = % X, want empty", decoded.TypeObject)
	}
}

// TestSEDPPadSkipped checks PIDPad never surfaces in endpoint params.
func TestSEDPPadSkipped(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xA8)
	ep := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	params := []rtps.Param{
		{ID: rtps.PIDPad, Value: []byte{}},
		{ID: rtps.PIDTopicName, Value: sedpEncodeCDRString("rt/chatter")},
		{ID: rtps.PIDPad, Value: []byte{0x00, 0x00}},
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
		{ID: rtps.PIDEndpointGUIDWire, Value: sedpEncodeEndpointGUID(ep.ID)},
	}

	body := mustSEDPDataBody(sedpPublicationsWriter, params)
	raw := mustSEDPMessage(t, ep.ID.Prefix, sedpFlagLittleEndian|sedpFlagInlineQoS|sedpFlagDataPresent, body)

	decoded, err := ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse with pad: %v", err)
	}

	if decoded.DDSTopic != "rt/chatter" {
		t.Fatalf("topic = %q, want rt/chatter", decoded.DDSTopic)
	}
}

// TestSEDPAdvertisesTransientLocal checks the writer wire offer matches the facade.
//
//nolint:gocognit,cyclop // test pins durability, history, and facade match breadth in one run, splitting would obscure intent
func TestSEDPAdvertisesTransientLocal(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xA9)
	writer := mustSEDPWriter(t, idx, "/chatter", "RIHS01_abc")

	for _, ep := range []SEDPEndpoint{writer} {
		data, err := SerializeSEDPDATA(ep)
		if err != nil {
			t.Fatalf("serialize: %v", err)
		}

		body, flags, err := sedpFindEndpointDATA(data)
		if err != nil {
			t.Fatalf("find data: %v", err)
		}

		params, _, err := sedpCollectParams(body, flags)
		if err != nil {
			t.Fatalf("collect: %v", err)
		}

		byID := sedpFirstParams(params)

		rawDurability, ok := byID[rtps.PIDDurability]
		if !ok {
			t.Fatalf("durability PID missing on wire")
		}

		if len(rawDurability) != 4 {
			t.Fatalf("durability len = %d, want 4", len(rawDurability))
		}

		durability := binary.LittleEndian.Uint32(rawDurability)
		if durability != 1 {
			t.Fatalf("durability = %d, want 1 for transient local", durability)
		}

		if durability != sedpDurabilityTransientLocal {
			t.Fatalf("durability = %d, want constant %d", durability, sedpDurabilityTransientLocal)
		}

		rawHistory, ok := byID[rtps.PIDHistory]
		if !ok {
			t.Fatalf("history PID missing on wire")
		}

		if len(rawHistory) != 8 {
			t.Fatalf("history len = %d, want 8", len(rawHistory))
		}

		kind := binary.LittleEndian.Uint32(rawHistory[:4])
		depth := binary.LittleEndian.Uint32(rawHistory[4:])

		if kind != sedpHistoryKeepLast {
			t.Fatalf("history kind = %d, want keep last %d", kind, sedpHistoryKeepLast)
		}

		if depth != 100 {
			t.Fatalf("history depth = %d, want 100 for facade match", depth)
		}

		if depth != sedpHistoryDepth {
			t.Fatalf("history depth = %d, want constant %d", depth, sedpHistoryDepth)
		}
	}
}

// TestSEDPReaderRequestsVolatile checks user readers announce volatile
// durability like ros2 topic echo: a volatile request flows against both
// volatile talkers (ros2 topic pub default) and transient talkers
// including their backlog, while a transient request is incompatible
// with volatile talkers and would receive nothing.
func TestSEDPReaderRequestsVolatile(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xAA)
	reader := mustSEDPReader(t, idx, "/chatter", "RIHS01_abc")

	if !reader.VolatileDurability {
		t.Fatalf("reader volatile = false, want true")
	}

	data, err := SerializeSEDPDATA(reader)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	body, flags, err := sedpFindEndpointDATA(data)
	if err != nil {
		t.Fatalf("find data: %v", err)
	}

	params, _, err := sedpCollectParams(body, flags)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}

	byID := sedpFirstParams(params)

	rawDurability, ok := byID[rtps.PIDDurability]
	if !ok {
		t.Fatalf("durability PID missing on wire")
	}

	if len(rawDurability) != 4 {
		t.Fatalf("durability len = %d, want 4", len(rawDurability))
	}

	durability := binary.LittleEndian.Uint32(rawDurability)
	if durability != sedpDurabilityVolatile {
		t.Fatalf("reader durability = %d, want volatile %d", durability, sedpDurabilityVolatile)
	}
}

// TestSEDPBuiltinWriterCoversAllWriters checks all SEDP writers route.
func TestSEDPBuiltinWriterCoversAllWriters(t *testing.T) {
	t.Parallel()

	writers := []rtps.EntityID{
		{0x00, 0x00, sedpWriterEntityB14, 0xC2},
		{0x00, 0x00, sedpWriterEntityB14Cyclone, 0xC2},
		{0x00, 0x00, sedpWriterEntityB14Alt, 0xC2},
	}

	for _, writer := range writers {
		if !sedpBuiltinWriter(writer) {
			t.Fatalf("writer %v not builtin, want true", writer)
		}

		reader, ok := sedpBuiltinReaderForWriter(writer)
		if !ok {
			t.Fatalf("reader for %v missing, want pairing", writer)
		}

		if reader[3] != 0xC7 {
			t.Fatalf("reader %v kind = %#02x, want 0xC7", reader, reader[3])
		}

		if reader[2] != writer[2] {
			t.Fatalf("reader %v third = %#02x, want %#02x", reader, reader[2], writer[2])
		}
	}
}

// mustSEDPDataSubmessage extracts the first DATA submessage bytes from a sample.
func mustSEDPDataSubmessage(t *testing.T, sample []byte) []byte {
	t.Helper()

	if len(sample) < sedpRTPSHeaderLen+sedpSubmessageHeaderLen {
		t.Fatalf("sample too short: %d", len(sample))
	}

	flags := sample[sedpRTPSHeaderLen+sedpSubmessageFlagsOff]

	var bodyLen int

	if flags&sedpFlagLittleEndian != 0 {
		bodyLen = int(binary.LittleEndian.Uint16(sample[sedpRTPSHeaderLen+sedpSubmessageLengthOff : sedpRTPSHeaderLen+sedpSubmessageHeaderLen]))
	} else {
		bodyLen = int(binary.BigEndian.Uint16(sample[sedpRTPSHeaderLen+sedpSubmessageLengthOff : sedpRTPSHeaderLen+sedpSubmessageHeaderLen]))
	}

	end := sedpRTPSHeaderLen + sedpSubmessageHeaderLen + bodyLen
	if end > len(sample) {
		t.Fatalf("data submessage overruns sample: end %d len %d", end, len(sample))
	}

	return append([]byte(nil), sample[sedpRTPSHeaderLen:end]...)
}

// mustSEDPBatchedWithSubs combines one RTPS header with DATA submessages.
func mustSEDPBatchedWithSubs(t *testing.T, headerSample []byte, subs [][]byte) []byte {
	t.Helper()

	if len(headerSample) < sedpRTPSHeaderLen {
		t.Fatalf("header sample too short: %d", len(headerSample))
	}

	out := append([]byte(nil), headerSample[:sedpRTPSHeaderLen]...)
	for _, sub := range subs {
		out = append(out, sub...)
	}

	return out
}

// mustSEDPBatchedDatagram combines serialized samples into one batched datagram.
func mustSEDPBatchedDatagram(t *testing.T, samples [][]byte) []byte {
	t.Helper()

	if len(samples) == 0 {
		t.Fatalf("no samples for batched datagram")
	}

	subs := make([][]byte, 0, len(samples))
	for _, s := range samples {
		subs = append(subs, mustSEDPDataSubmessage(t, s))
	}

	return mustSEDPBatchedWithSubs(t, samples[0], subs)
}

// TestSEDPBatchedDatagramInstallsAll checks batched DATA installs every endpoint.
//
//nolint:gocognit,cyclop // test pins declare, serialize, parse, and ingest breadth in one run, splitting would obscure intent
func TestSEDPBatchedDatagramInstallsAll(t *testing.T) {
	t.Parallel()

	peer := mustSEDPIndexWithPrefix(t, 0xB8)
	local := mustSEDPIndexWithPrefix(t, 0xC8)

	disc, err := peer.DeclareWriter("/discovery_info", "std_msgs__msg__String", "RIHS01_disc", nil)
	if err != nil {
		t.Fatalf("declare discovery: %v", err)
	}

	rosout, err := peer.DeclareWriter("/rosout", "rcl_interfaces__msg__Log", "RIHS01_log", nil)
	if err != nil {
		t.Fatalf("declare rosout: %v", err)
	}

	param, err := peer.DeclareWriter("/parameter_events", "rcl_interfaces__msg__ParameterEvent", "RIHS01_param", nil)
	if err != nil {
		t.Fatalf("declare parameter events: %v", err)
	}

	chatter, err := peer.DeclareWriter("/chatter", "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare chatter: %v", err)
	}

	ordered := []SEDPEndpoint{disc, rosout, param, chatter}

	samples := make([][]byte, 0, len(ordered))
	for _, ep := range ordered {
		data, err := SerializeSEDPDATA(ep)
		if err != nil {
			t.Fatalf("serialize %s: %v", ep.ROSTopic, err)
		}

		samples = append(samples, data)
	}

	batched := mustSEDPBatchedDatagram(t, samples)

	eps, err := ParseSEDPDATAs(batched)
	if err != nil {
		t.Fatalf("parse batched: %v", err)
	}

	if len(eps) != 4 {
		t.Fatalf("parsed = %d, want 4", len(eps))
	}

	if !eps[3].ID.Equal(chatter.ID) {
		t.Fatalf("last parsed guid = %s, want chatter %s", eps[3].ID.String(), chatter.ID.String())
	}

	if eps[3].ROSTopic != "/chatter" {
		t.Fatalf("last parsed topic = %q, want /chatter", eps[3].ROSTopic)
	}

	err = local.ingestSample(batched)
	if err != nil {
		t.Fatalf("ingest batched: %v", err)
	}

	remotes := local.RemoteEndpoints()
	if len(remotes) != 4 {
		t.Fatalf("remotes = %d, want 4", len(remotes))
	}

	found := make(map[string]bool, len(remotes))
	for _, ep := range remotes {
		found[ep.ID.String()] = true
	}

	for _, want := range ordered {
		if !found[want.ID.String()] {
			t.Fatalf("remotes miss %s %s", want.ROSTopic, want.ID.String())
		}
	}

	if got := local.Generation(); got != 4 {
		t.Fatalf("generation = %d, want 4", got)
	}
}

// TestSEDPBatchedDatagramIsolatesBadEntry checks one bad entry skips without dropping others.
//
//nolint:gocognit,cyclop // test pins good, bad, and ingest isolation breadth in one run, splitting would obscure intent
func TestSEDPBatchedDatagramIsolatesBadEntry(t *testing.T) {
	t.Parallel()

	peer := mustSEDPIndexWithPrefix(t, 0xBA)
	local := mustSEDPIndexWithPrefix(t, 0xCA)

	disc, err := peer.DeclareWriter("/discovery_info", "std_msgs__msg__String", "RIHS01_disc", nil)
	if err != nil {
		t.Fatalf("declare discovery: %v", err)
	}

	param, err := peer.DeclareWriter("/parameter_events", "rcl_interfaces__msg__ParameterEvent", "RIHS01_param", nil)
	if err != nil {
		t.Fatalf("declare parameter events: %v", err)
	}

	chatter, err := peer.DeclareWriter("/chatter", "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare chatter: %v", err)
	}

	good := []SEDPEndpoint{disc, param, chatter}

	var subs [][]byte

	firstSample, err := SerializeSEDPDATA(disc)
	if err != nil {
		t.Fatalf("serialize discovery: %v", err)
	}

	subs = append(subs, mustSEDPDataSubmessage(t, firstSample))

	badBody := mustSEDPDataBody(sedpPublicationsWriterCyclone, []rtps.Param{
		{ID: rtps.PIDTypeName, Value: sedpEncodeCDRString("std_msgs::msg::dds_::String_")},
	})
	badFlags := sedpFlagLittleEndian | sedpFlagInlineQoS | sedpFlagDataPresent
	badSub := []byte{sedpSubmessageDATA, badFlags}
	badSub = binary.LittleEndian.AppendUint16(badSub, uint16(len(badBody))) //nolint:gosec // G115: single-param SEDP body fits uint16
	badSub = append(badSub, badBody...)
	subs = append(subs, badSub)

	for _, ep := range []SEDPEndpoint{param, chatter} {
		data, err := SerializeSEDPDATA(ep)
		if err != nil {
			t.Fatalf("serialize %s: %v", ep.ROSTopic, err)
		}

		subs = append(subs, mustSEDPDataSubmessage(t, data))
	}

	batched := mustSEDPBatchedWithSubs(t, firstSample, subs)

	eps, err := ParseSEDPDATAs(batched)
	if err != nil {
		t.Fatalf("parse batched with bad: %v", err)
	}

	if len(eps) != 3 {
		t.Fatalf("parsed = %d, want 3 good entries", len(eps))
	}

	err = local.ingestSample(batched)
	if err != nil {
		t.Fatalf("ingest batched with bad: %v", err)
	}

	remotes := local.RemoteEndpoints()
	if len(remotes) != 3 {
		t.Fatalf("remotes = %d, want 3", len(remotes))
	}

	for _, want := range good {
		seen := false
		for _, got := range remotes {
			if got.ID.Equal(want.ID) {
				seen = true
				break
			}
		}

		if !seen {
			t.Fatalf("remotes miss %s %s", want.ROSTopic, want.ID.String())
		}
	}
}

// TestSEDPProvisionalPromotesOnConfirmedUpsert pins promotion adoption.
//
//nolint:cyclop // test pins provisional, promotion, and generation checks in one run, splitting would obscure intent
func TestSEDPProvisionalPromotesOnConfirmedUpsert(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xC1)
	peer := mustSEDPIndexWithPrefix(t, 0xC2)

	confirmed := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	provisional := confirmed
	provisional.TypeHash = "RIHS01_prov"
	provisional.QoS = []byte{0x09}

	err := local.ProvisionalInstall(provisional)
	if err != nil {
		t.Fatalf("provisional install: %v", err)
	}

	if got := local.Generation(); got != 1 {
		t.Fatalf("generation = %d, want 1 after provisional install", got)
	}

	snap := local.RemoteEndpoints()
	if len(snap) != 1 {
		t.Fatalf("remotes = %d, want 1", len(snap))
	}

	if !snap[0].Provisional {
		t.Fatalf("provisional flag = false, want true")
	}

	if snap[0].FirstSeen.IsZero() {
		t.Fatalf("first seen is zero, want non-zero")
	}

	err = local.UpsertRemote(confirmed)
	if err != nil {
		t.Fatalf("confirmed upsert: %v", err)
	}

	if got := local.Generation(); got != 2 {
		t.Fatalf("generation = %d, want 2 after promotion", got)
	}

	promoted := local.RemoteEndpoints()
	if len(promoted) != 1 {
		t.Fatalf("remotes = %d, want 1", len(promoted))
	}

	if promoted[0].Provisional {
		t.Fatalf("provisional flag = true after promotion, want false")
	}

	if !promoted[0].FirstSeen.IsZero() {
		t.Fatalf("first seen = %v after promotion, want zero", promoted[0].FirstSeen)
	}

	if promoted[0].TypeHash != confirmed.TypeHash {
		t.Fatalf("hash = %q, want %q", promoted[0].TypeHash, confirmed.TypeHash)
	}

	if !bytes.Equal(promoted[0].QoS, confirmed.QoS) {
		t.Fatalf("qos = %x, want %x", promoted[0].QoS, confirmed.QoS)
	}
}

// TestSEDPConfirmedImmuneToProvisional pins confirmed stability.
func TestSEDPConfirmedImmuneToProvisional(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xC3)
	peer := mustSEDPIndexWithPrefix(t, 0xC4)

	confirmed := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	err := local.UpsertRemote(confirmed)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	stale := confirmed
	stale.TypeHash = "RIHS01_stale"
	stale.QoS = []byte{0x07, 0x08}

	err = local.ProvisionalInstall(stale)
	if err != nil {
		t.Fatalf("provisional install over confirmed: %v", err)
	}

	if got := local.Generation(); got != 1 {
		t.Fatalf("generation = %d, want 1 after ignored provisional", got)
	}

	snap := local.RemoteEndpoints()
	if len(snap) != 1 {
		t.Fatalf("remotes = %d, want 1", len(snap))
	}

	if snap[0].Provisional {
		t.Fatalf("provisional flag = true, want false for confirmed")
	}

	if snap[0].TypeHash != confirmed.TypeHash {
		t.Fatalf("hash = %q, want confirmed %q", snap[0].TypeHash, confirmed.TypeHash)
	}

	if !bytes.Equal(snap[0].QoS, confirmed.QoS) {
		t.Fatalf("qos = %x, want confirmed %x", snap[0].QoS, confirmed.QoS)
	}
}

// TestSEDPProvisionalExpiryRemovesStale pins TTL sweep and generation.
func TestSEDPProvisionalExpiryRemovesStale(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xC5)
	peer := mustSEDPIndexWithPrefix(t, 0xC6)

	now := time.Now()

	stale := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")
	stale.FirstSeen = now.Add(-sedpProvisionalTTL - time.Second)

	err := local.ProvisionalInstall(stale)
	if err != nil {
		t.Fatalf("provisional install stale: %v", err)
	}

	if got := local.Generation(); got != 1 {
		t.Fatalf("generation = %d, want 1 after install", got)
	}

	removed := local.ExpireProvisional(now)
	if removed != 1 {
		t.Fatalf("removed = %d, want 1", removed)
	}

	if got := local.Generation(); got != 2 {
		t.Fatalf("generation = %d, want 2 after expiry", got)
	}

	if n := len(local.RemoteEndpoints()); n != 0 {
		t.Fatalf("remotes = %d, want 0 after expiry", n)
	}

	freshPeer := mustSEDPIndexWithPrefix(t, 0xC7)
	fresh := mustSEDPWriter(t, freshPeer, "/chatter", "RIHS01_abc")

	err = local.UpsertRemote(fresh)
	if err != nil {
		t.Fatalf("upsert confirmed: %v", err)
	}

	before := local.Generation()

	removed = local.Sweep(now)
	if removed != 0 {
		t.Fatalf("sweep confirmed removed = %d, want 0", removed)
	}

	if got := local.Generation(); got != before {
		t.Fatalf("generation = %d, want %d after confirmed sweep", got, before)
	}
}
