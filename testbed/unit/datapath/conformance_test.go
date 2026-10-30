// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_datapath holds DATA path conformance tests.
//
// Tests run pure Go with no Docker.
//
//nolint:cyclop // test breadth
package unit_datapath

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// dataPathFrameFixedSize mirrors the Weld DATA frame fixed header width.
const dataPathFrameFixedSize = 36

// dataPathGuidSize is the 16 byte Weld writer GUID width.
const dataPathGuidSize = 16

// dataPathSeqSize is the 8 byte sequence width on the Weld frame.
const dataPathSeqSize = 8

// dataPathLenSize is the 4 byte width of each length prefix.
const dataPathLenSize = 4

// dataPathTestDomain is the domain used for constructed SPDP and SEDP packets.
const dataPathTestDomain uint32 = 42

// dataPathRecordingWriter records Weld DATA frames for round trip checks.
type dataPathRecordingWriter struct {
	mu     sync.Mutex
	frames [][]byte
}

// WriteFrame records one frame with a copy.
func (w *dataPathRecordingWriter) WriteFrame(frame []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	cp := append([]byte(nil), frame...)
	w.frames = append(w.frames, cp)
	return nil
}

// count returns the number of recorded frames.
func (w *dataPathRecordingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.frames)
}

// at returns a copy of the frame at index i.
func (w *dataPathRecordingWriter) at(t *testing.T, i int) []byte {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	if i < 0 || i >= len(w.frames) {
		t.Fatalf("frame index %d out of range %d", i, len(w.frames))
	}
	return append([]byte(nil), w.frames[i]...)
}

// dataPathSliceFrameSource replays RTPS payloads for TakeFrom checks.
type dataPathSliceFrameSource struct {
	mu       sync.Mutex
	guids    []dds.WriterGUID
	payloads [][]byte
	idx      int
}

// ReadFrame returns the next queued payload.
func (s *dataPathSliceFrameSource) ReadFrame(ctx context.Context) (dds.WriterGUID, uint64, []byte, error) {
	err := ctx.Err()
	if err != nil {
		return dds.WriterGUID{}, 0, nil, fmt.Errorf("data path frame source: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx >= len(s.payloads) {
		return dds.WriterGUID{}, 0, nil, context.DeadlineExceeded
	}
	guid := s.guids[s.idx]
	cp := append([]byte(nil), s.payloads[s.idx]...)
	s.idx++
	return guid, 0, cp, nil
}

// queue appends one guid plus payload pair for later reads.
func (s *dataPathSliceFrameSource) queue(guid dds.WriterGUID, payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.guids = append(s.guids, guid)
	s.payloads = append(s.payloads, append([]byte(nil), payload...))
}

// dataPathItem carries one demultiplexed user payload with its GUID.
type dataPathItem struct {
	guid    dds.WriterGUID
	payload []byte
}

// dataPathWriterGUID returns a fixed GUID for deterministic tests.
func dataPathWriterGUID() dds.WriterGUID {
	var guid dds.WriterGUID
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	return guid
}

// dataPathParseFrame parses one Weld DATA frame without panicking.
func dataPathParseFrame(frame []byte) (dds.WriterGUID, uint64, string, string, uint32, []byte, error) {
	var guid dds.WriterGUID
	if len(frame) < dataPathFrameFixedSize {
		return guid, 0, "", "", 0, nil, fmt.Errorf("testbed: frame need %d have %d: %w", dataPathFrameFixedSize, len(frame), dds.ErrTruncated)
	}
	copy(guid[:], frame[0:dataPathGuidSize])
	seq := binary.LittleEndian.Uint64(frame[dataPathGuidSize : dataPathGuidSize+dataPathSeqSize])
	topicLen := binary.LittleEndian.Uint32(frame[dataPathGuidSize+dataPathSeqSize : dataPathGuidSize+dataPathSeqSize+dataPathLenSize])
	typeLen := binary.LittleEndian.Uint32(frame[dataPathGuidSize+dataPathSeqSize+dataPathLenSize : dataPathGuidSize+dataPathSeqSize+dataPathLenSize*2])
	userLen := binary.LittleEndian.Uint32(frame[dataPathGuidSize+dataPathSeqSize+dataPathLenSize*2 : dataPathFrameFixedSize])
	need := uint64(dataPathFrameFixedSize) + uint64(topicLen) + uint64(typeLen) + uint64(types.HeaderSize)
	if uint64(len(frame)) < need {
		return guid, 0, "", "", 0, nil, fmt.Errorf("testbed: frame truncated need %d have %d: %w", need, len(frame), dds.ErrTruncated)
	}
	off := dataPathFrameFixedSize
	topicEnd := off + int(topicLen)
	typeEnd := topicEnd + int(typeLen)
	ddsTopic := string(frame[off:topicEnd])
	typeName := string(frame[topicEnd:typeEnd])
	envelope := frame[typeEnd:]
	return guid, seq, ddsTopic, typeName, userLen, envelope, nil
}

// dataPathBuildUserPayload publishes one sample and returns the RTPS payload.
func dataPathBuildUserPayload(t *testing.T, guid dds.WriterGUID, payload []byte) []byte {
	t.Helper()
	sink := &dataPathRecordingWriter{}
	pub := helpers.MustNewPublisher(t, guid, sink)
	msg := transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: payload}
	err := pub.Publish(msg)
	if err != nil {
		t.Fatalf("publish user sample: %v", err)
	}
	if sink.count() != 1 {
		t.Fatalf("user frames = %d want 1", sink.count())
	}
	frame := sink.at(t, 0)
	rtpsPayload, err := dds.MarshalRTPSPayload(frame)
	if err != nil {
		t.Fatalf("marshal user frame: %v", err)
	}
	return rtpsPayload
}

// dataPathBuildBatched builds one RTPS datagram with SPDP plus SEDP plus user DATA.
//
//nolint:gocognit,cyclop // conformance test builds batched discovery plus user datagram in one helper
func dataPathBuildBatched(t *testing.T) ([]byte, rtps.Header, dds.WriterGUID, []byte) {
	t.Helper()
	var spdpPrefix dds.GUIDPrefix
	for i := range spdpPrefix {
		spdpPrefix[i] = byte(0xA0 + i)
	}
	spdpGUID := dds.GUID{Prefix: spdpPrefix, EntityID: [4]byte{0x00, 0x00, 0x01, 0xC1}}
	spdpMsg, err := dds.MarshalAlivePacket(spdpGUID, dataPathTestDomain, 1, dds.DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("marshal SPDP: %v", err)
	}
	var drops rtps.Counter
	_, spdpRaws, err := rtps.DecodeMessage(spdpMsg, &drops)
	if err != nil {
		t.Fatalf("decode SPDP message: %v", err)
	}
	if len(spdpRaws) == 0 {
		t.Fatalf("SPDP message has no submessages")
	}
	var spdpData rtps.RawSubmessage
	foundSPDP := false
	for _, raw := range spdpRaws {
		if raw.ID == rtps.SubmessageData {
			spdpData = raw
			foundSPDP = true
			break
		}
	}
	if !foundSPDP {
		t.Fatalf("SPDP message has no DATA")
	}
	var sedpPrefix dds.SEDPGUIDPrefix
	copy(sedpPrefix[:], spdpPrefix[:])
	sedpIdx, err := dds.NewSEDPIndex(sedpPrefix, dataPathTestDomain)
	if err != nil {
		t.Fatalf("new SEDP index: %v", err)
	}
	ep, err := sedpIdx.DeclareWriter("/chatter", "std_msgs__msg__String", dds.StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare SEDP writer: %v", err)
	}
	sedpMsg, err := dds.SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize SEDP: %v", err)
	}
	_, sedpRaws, err := rtps.DecodeMessage(sedpMsg, &drops)
	if err != nil {
		t.Fatalf("decode SEDP message: %v", err)
	}
	var sedpData rtps.RawSubmessage
	foundSEDP := false
	for _, raw := range sedpRaws {
		if raw.ID == rtps.SubmessageData {
			sedpData = raw
			foundSEDP = true
			break
		}
	}
	if !foundSEDP {
		t.Fatalf("SEDP message has no DATA")
	}
	var userGUID dds.WriterGUID
	copy(userGUID[:12], spdpPrefix[:])
	userGUID[12] = 0x00
	userGUID[13] = 0x00
	userGUID[14] = 0x02
	userGUID[15] = 0x03
	userPayload := dataPathBuildUserPayload(t, userGUID, []byte{0xAA, 0xBB, 0xCC, 0xDD})
	var userEntity rtps.EntityID
	copy(userEntity[:], userGUID[12:16])
	userData := rtps.Data{ReaderID: rtps.EntityUnknown, WriterID: userEntity, SequenceNumber: 7, Payload: userPayload}
	userRaw, err := userData.Submessage(true)
	if err != nil {
		t.Fatalf("user submessage: %v", err)
	}
	var hdrPrefix [12]byte
	copy(hdrPrefix[:], spdpPrefix[:])
	header := rtps.DefaultHeader(hdrPrefix)
	combined, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{spdpData, sedpData, userRaw})
	if err != nil {
		t.Fatalf("encode batched: %v", err)
	}
	if len(combined) > rtps.MaxDatagramSize {
		t.Fatalf("batched length %d above max %d", len(combined), rtps.MaxDatagramSize)
	}
	return combined, header, userGUID, userPayload
}

// dataPathRouteBatched routes one batched datagram to SPDP plus SEDP plus user channels.
//
//nolint:gocognit,cyclop // conformance test routes batched datagram across three channels in one helper
func dataPathRouteBatched(t *testing.T, datagram []byte, header rtps.Header, spdpCh, sedpCh chan []byte, dataCh chan dataPathItem) {
	t.Helper()
	var drops rtps.Counter
	decodedHeader, subs, err := rtps.DecodeMessage(datagram, &drops)
	if err != nil {
		t.Fatalf("decode batched: %v", err)
	}
	if decodedHeader.Prefix != header.Prefix {
		t.Fatalf("header prefix mismatch")
	}
	ann, err := dds.ParseAnnouncement(datagram)
	if err != nil {
		t.Fatalf("batched misses SPDP: %v", err)
	}
	if ann.DomainID != dataPathTestDomain {
		t.Fatalf("SPDP domain = %d want %d", ann.DomainID, dataPathTestDomain)
	}
	spdpCopy := append([]byte(nil), datagram...)
	select {
	case spdpCh <- spdpCopy:
	default:
		t.Fatalf("spdpCh blocked, no loss allowed")
	}
	_, err = dds.ParseSEDPDATA(datagram)
	if err != nil {
		t.Fatalf("batched misses SEDP: %v", err)
	}
	sedpCopy := append([]byte(nil), datagram...)
	select {
	case sedpCh <- sedpCopy:
	default:
		t.Fatalf("sedpCh blocked, no loss allowed")
	}
	for _, sub := range subs {
		if sub.ID != rtps.SubmessageData {
			continue
		}
		decoded, err := rtps.DecodeSubmessage(sub, &drops)
		if err != nil {
			t.Fatalf("decode batched sub: %v", err)
		}
		if decoded.Data == nil {
			continue
		}
		if decoded.Data.WriterID == rtps.EntitySPDPWriter {
			continue
		}
		single, err := rtps.EncodeMessage(decodedHeader, []rtps.RawSubmessage{sub})
		if err != nil {
			t.Fatalf("encode single for SEDP probe: %v", err)
		}
		_, err = dds.ParseSEDPDATA(single)
		if err == nil {
			continue
		}
		frame, err := dds.ParseRTPSPayload(decoded.Data.Payload)
		if err != nil {
			t.Fatalf("parse user payload: %v", err)
		}
		var guid dds.WriterGUID
		copy(guid[:12], decodedHeader.Prefix[:])
		copy(guid[12:16], decoded.Data.WriterID[:])
		payloadCopy := append([]byte(nil), frame...)
		select {
		case dataCh <- dataPathItem{guid: guid, payload: payloadCopy}:
		default:
			t.Fatalf("dataCh blocked, no loss allowed")
		}
	}
}

// TestBatchedRTPSRoutesDiscoveryAndUser checks batched SPDP plus SEDP plus user routing.
//
//nolint:gocognit,cyclop // conformance test asserts discovery plus user routing in one run
func TestBatchedRTPSRoutesDiscoveryAndUser(t *testing.T) {
	t.Parallel()
	combined, header, userGUID, userPayload := dataPathBuildBatched(t)
	spdpCh := make(chan []byte, 128)
	sedpCh := make(chan []byte, 128)
	dataCh := make(chan dataPathItem, 128)
	dataPathRouteBatched(t, combined, header, spdpCh, sedpCh, dataCh)
	if len(spdpCh) != 1 {
		t.Fatalf("spdpCh length = %d want 1", len(spdpCh))
	}
	if len(sedpCh) != 1 {
		t.Fatalf("sedpCh length = %d want 1", len(sedpCh))
	}
	if len(dataCh) != 1 {
		t.Fatalf("dataCh length = %d want 1", len(dataCh))
	}
	spdpGot := <-spdpCh
	ann, err := dds.ParseAnnouncement(spdpGot)
	if err != nil {
		t.Fatalf("spdpCh parse: %v", err)
	}
	if ann.DomainID != dataPathTestDomain {
		t.Fatalf("spdpCh domain = %d want %d", ann.DomainID, dataPathTestDomain)
	}
	sedpGot := <-sedpCh
	ep, err := dds.ParseSEDPDATA(sedpGot)
	if err != nil {
		t.Fatalf("sedpCh parse: %v", err)
	}
	if ep.ROSTopic != "/chatter" {
		t.Fatalf("sedpCh topic = %q want /chatter", ep.ROSTopic)
	}
	item := <-dataCh
	if item.guid != userGUID {
		t.Fatalf("user guid mismatch")
	}
	if !bytes.Equal(item.payload, userPayload) {
		t.Fatalf("user payload mismatch")
	}
	recv := dds.NewDataReceiver()
	msg, err := recv.Take(item.payload)
	if err != nil {
		t.Fatalf("take user payload: %v", err)
	}
	if msg.TopicName != "/chatter" {
		t.Fatalf("user topic = %q want /chatter", msg.TopicName)
	}
	if recv.LostCount() != 0 {
		t.Fatalf("lost after single user take = %d want 0", recv.LostCount())
	}
	if recv.DroppedCount() != 0 {
		t.Fatalf("drops after single user take = %d want 0", recv.DroppedCount())
	}
	for _, subPayload := range [][]byte{spdpGot, sedpGot} {
		_, _, err := rtps.DecodeMessage(subPayload, &rtps.Counter{})
		if err != nil {
			t.Fatalf("decode routed datagram: %v", err)
		}
	}
}

// TestWeldDataFrameRoundTrip checks GUID plus seq plus lengths plus envelope.
//
//nolint:gocognit,cyclop // frame test asserts GUID, sequence, lengths and envelope in one run
func TestWeldDataFrameRoundTrip(t *testing.T) {
	t.Parallel()
	guid := dataPathWriterGUID()
	sink := &dataPathRecordingWriter{}
	pub := helpers.MustNewPublisher(t, guid, sink)
	payload := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	msg := transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: payload}
	err := pub.Publish(msg)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if sink.count() != 1 {
		t.Fatalf("frames = %d want 1", sink.count())
	}
	frame := sink.at(t, 0)
	gotGUID, seq, ddsTopic, typeName, userLen, envelope, err := dataPathParseFrame(frame)
	if err != nil {
		t.Fatalf("parse frame: %v", err)
	}
	if gotGUID != guid {
		t.Fatalf("guid mismatch")
	}
	if seq != 1 {
		t.Fatalf("seq = %d want 1", seq)
	}
	if ddsTopic != "rt/chatter" {
		t.Fatalf("dds topic = %q want rt/chatter", ddsTopic)
	}
	if typeName != "std_msgs__msg__String" {
		t.Fatalf("type = %q", typeName)
	}
	if userLen != uint32(len(payload)) { //nolint:gosec // G115: test payload length is small and non-negative
		t.Fatalf("user length = %d want %d", userLen, len(payload))
	}
	if len(envelope) < types.HeaderSize {
		t.Fatalf("envelope length %d below header %d", len(envelope), types.HeaderSize)
	}
	if envelope[0] != 0x00 || envelope[1] != 0x01 || envelope[2] != 0x00 || envelope[3] != 0x00 {
		t.Fatalf("envelope header = %v want LE header", envelope[0:4])
	}
	var metrics types.Metrics
	view, _, err := types.UnmarshalMessage(envelope, &metrics)
	if err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if uint64(len(view)) < uint64(userLen) {
		t.Fatalf("envelope view %d below user %d", len(view), userLen)
	}
	if !bytes.Equal(view[:len(payload)], payload) {
		t.Fatalf("envelope payload mismatch")
	}
	rtpsPayload, err := dds.MarshalRTPSPayload(frame)
	if err != nil {
		t.Fatalf("marshal rtps payload: %v", err)
	}
	if !bytes.Equal(rtpsPayload, frame) {
		t.Fatalf("marshal result differs from frame")
	}
	back, err := dds.ParseRTPSPayload(rtpsPayload)
	if err != nil {
		t.Fatalf("parse rtps payload: %v", err)
	}
	if !bytes.Equal(back, frame) {
		t.Fatalf("parse result differs from frame")
	}
	recv := dds.NewDataReceiver()
	got, err := recv.Take(back)
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	if got.TopicName != "/chatter" {
		t.Fatalf("take topic = %q want /chatter", got.TopicName)
	}
	if got.TypeName != "std_msgs__msg__String" {
		t.Fatalf("take type = %q", got.TypeName)
	}
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("take payload = %v want %v", got.Payload, payload)
	}
	if recv.LostCount() != 0 {
		t.Fatalf("lost = %d want 0", recv.LostCount())
	}
	if recv.DroppedCount() != 0 {
		t.Fatalf("drops = %d want 0", recv.DroppedCount())
	}
}

// TestDataPathPlainStringFallback checks plain XCDR1 String decoding for /chatter.
//
//nolint:gocognit,cyclop // fallback test asserts plain string decoding across shapes in one run
func TestDataPathPlainStringFallback(t *testing.T) {
	t.Parallel()
	enc := types.NewEncoder(true)
	err := enc.WriteString("hello weld")
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	wire := enc.Bytes()
	dec, err := types.NewDecoder(wire)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read string: %v", err)
	}
	if got != "hello weld" {
		t.Fatalf("string = %q want hello weld", got)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder drops = %d want 0", dec.Dropped())
	}
	src := &dataPathSliceFrameSource{}
	var cycloneGUID dds.WriterGUID
	for i := range cycloneGUID {
		cycloneGUID[i] = byte(0xC0 + i)
	}
	src.queue(cycloneGUID, wire)
	var sedpPrefix dds.SEDPGUIDPrefix
	copy(sedpPrefix[:], cycloneGUID[:12])
	sedpIdx, err := dds.NewSEDPIndex(sedpPrefix, dataPathTestDomain)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}
	var sedpEntity dds.SEDPEntityID
	copy(sedpEntity[:], cycloneGUID[12:16])
	sedpID := dds.SEDPGUID{Prefix: sedpPrefix, Entity: sedpEntity}
	sedpEp := dds.SEDPEndpoint{ID: sedpID, ROSTopic: "/chatter", DDSTopic: "rt/chatter", TypeDDS: "std_msgs::msg::dds_::String_", TypeIDL: "std_msgs__msg__String", IsReader: false, DomainID: dataPathTestDomain}
	err = sedpIdx.UpsertRemote(sedpEp)
	if err != nil {
		t.Fatalf("upsert sedp remote: %v", err)
	}
	recv := dds.NewDataReceiver()
	recv.SetSEDPIndex(sedpIdx)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	incoming, err := recv.TakeFrom(ctx, src)
	if err != nil {
		t.Fatalf("take from cyclone: %v", err)
	}
	if incoming.TopicName != "/chatter" {
		t.Fatalf("cyclone topic = %q want /chatter", incoming.TopicName)
	}
	if incoming.TypeName != "std_msgs__msg__String" {
		t.Fatalf("cyclone type = %q want std_msgs__msg__String", incoming.TypeName)
	}
	if !bytes.Equal(incoming.Payload, wire) {
		t.Fatalf("cyclone payload mismatch")
	}
	if recv.DroppedCount() != 0 {
		t.Fatalf("drops after valid cyclone = %d want 0", recv.DroppedCount())
	}
	dec2, err := types.NewDecoder(incoming.Payload)
	if err != nil {
		t.Fatalf("new decoder on take payload: %v", err)
	}
	back, err := dec2.ReadString()
	if err != nil {
		t.Fatalf("read string on take payload: %v", err)
	}
	if back != "hello weld" {
		t.Fatalf("take payload string = %q want hello weld", back)
	}
	err = dec2.Finish()
	if err != nil {
		t.Fatalf("finish on take payload: %v", err)
	}
}

// TestDataPathCorruptCountsDrop checks corrupt plain String inputs count drops.
//
//nolint:gocognit // corrupt input test asserts drop counters across shapes in one run
func TestDataPathCorruptCountsDrop(t *testing.T) {
	t.Parallel()
	enc := types.NewEncoder(true)
	err := enc.WriteString("hello")
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	valid := enc.Bytes()
	withTrailing := append(append([]byte(nil), valid...), 0xFF)
	truncated := valid[:len(valid)-1]
	badHeader := append([]byte(nil), valid...)
	badHeader[0] = 0x01
	empty := []byte{}
	cases := [][]byte{withTrailing, truncated, badHeader, empty, nil}
	for i, corrupt := range cases {
		dec, derr := types.NewDecoder(corrupt)
		if derr == nil {
			_, rerr := dec.ReadString()
			if rerr == nil {
				ferr := dec.Finish()
				if ferr == nil && i == 0 {
					t.Fatalf("case %d trailing bytes passed Finish, want error", i)
				}
			}
		}
		src := &dataPathSliceFrameSource{}
		var guid dds.WriterGUID
		guid[0] = byte(i + 1)
		cp := append([]byte(nil), corrupt...)
		src.queue(guid, cp)
		recv := dds.NewDataReceiver()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, terr := recv.TakeFrom(ctx, src)
		cancel()
		if terr == nil {
			t.Fatalf("case %d corrupt take succeeded, want error", i)
		}
		if recv.DroppedCount() != 1 {
			t.Fatalf("case %d drops = %d want 1", i, recv.DroppedCount())
		}
	}
}

// TestBestEffortOrdering checks first accepted plus gap plus stale handling.
//
//nolint:gocognit,cyclop // ordering test asserts accept, gap and stale branches in one run
func TestBestEffortOrdering(t *testing.T) {
	t.Parallel()
	guid := dataPathWriterGUID()
	sink := &dataPathRecordingWriter{}
	pub := helpers.MustNewPublisher(t, guid, sink)
	payloads := [][]byte{{1, 1, 1, 1}, {2, 2, 2, 2}, {3, 3, 3, 3}}
	for _, p := range payloads {
		err := pub.Publish(transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: p})
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if sink.count() != 3 {
		t.Fatalf("frames = %d want 3", sink.count())
	}
	first := sink.at(t, 0)
	third := sink.at(t, 2)
	second := sink.at(t, 1)
	recv := dds.NewDataReceiver()
	firstMsg, err := recv.Take(first)
	if err != nil {
		t.Fatalf("take first: %v", err)
	}
	if firstMsg.Payload[0] != 1 {
		t.Fatalf("first payload = %v want first byte 1", firstMsg.Payload)
	}
	if recv.LostCount() != 0 {
		t.Fatalf("lost after first = %d want 0", recv.LostCount())
	}
	if recv.DroppedCount() != 0 {
		t.Fatalf("drops after first = %d want 0", recv.DroppedCount())
	}
	gapMsg, err := recv.Take(third)
	if err != nil {
		t.Fatalf("take gap: %v", err)
	}
	if gapMsg.Payload[0] != 3 {
		t.Fatalf("gap payload = %v want first byte 3", gapMsg.Payload)
	}
	if recv.LostCount() != 1 {
		t.Fatalf("lost after gap = %d want 1", recv.LostCount())
	}
	_, err = recv.Take(second)
	if !errors.Is(err, dds.ErrStaleSequence) {
		t.Fatalf("stale take = %v want ErrStaleSequence", err)
	}
	if recv.DroppedCount() != 1 {
		t.Fatalf("drops after stale = %d want 1", recv.DroppedCount())
	}
	if recv.LostCount() != 1 {
		t.Fatalf("lost after stale = %d want 1", recv.LostCount())
	}
	_, err = recv.Take(third)
	if !errors.Is(err, dds.ErrStaleSequence) {
		t.Fatalf("duplicate take = %v want ErrStaleSequence", err)
	}
	if recv.DroppedCount() != 2 {
		t.Fatalf("drops after duplicate = %d want 2", recv.DroppedCount())
	}
}

// TestMaxDatagramSizeAndTruncation checks size bound plus truncation safety.
//
//nolint:gocognit,cyclop // size test asserts bound plus truncation safety across shapes in one run
func TestMaxDatagramSizeAndTruncation(t *testing.T) {
	t.Parallel()
	if rtps.MaxDatagramSize != 65507 {
		t.Fatalf("MaxDatagramSize = %d want 65507", rtps.MaxDatagramSize)
	}
	combined, _, _, _ := dataPathBuildBatched(t)
	if len(combined) > rtps.MaxDatagramSize {
		t.Fatalf("batched %d above max %d", len(combined), rtps.MaxDatagramSize)
	}
	_, err := rtps.WrapSubmessage(rtps.SubmessageData, 0x01, make([]byte, 0x10000))
	if err == nil {
		t.Fatalf("oversize submessage succeeded, want error")
	}
	truncPoints := []int{0, 1, 10, 19, 21, len(combined) - 5, len(combined) - 1}
	for _, n := range truncPoints {
		if n < 0 || n > len(combined) {
			continue
		}
		var drops rtps.Counter
		_, _, derr := rtps.DecodeMessage(combined[:n], &drops)
		if derr == nil {
			t.Fatalf("truncated length %d succeeded, want error", n)
		}
		if drops.Count() != 1 {
			t.Fatalf("truncated length %d drops = %d want 1", n, drops.Count())
		}
	}
	var headerDrops rtps.Counter
	_, headerSubs, herr := rtps.DecodeMessage(combined[:20], &headerDrops)
	if herr != nil {
		t.Fatalf("header only decode: %v", herr)
	}
	if len(headerSubs) != 0 {
		t.Fatalf("header only subs = %d want 0", len(headerSubs))
	}
	if headerDrops.Count() != 0 {
		t.Fatalf("header only drops = %d want 0", headerDrops.Count())
	}
	fullFrame := dataPathBuildUserPayload(t, dataPathWriterGUID(), []byte{9, 9, 9, 9})
	frameTruncs := []int{0, 1, 10, 35, 36, len(fullFrame) - 1}
	for _, n := range frameTruncs {
		_, perr := dds.ParseRTPSPayload(fullFrame[:n])
		if perr == nil {
			t.Fatalf("frame truncated %d succeeded, want error", n)
		}
		if !errors.Is(perr, dds.ErrTruncated) && !errors.Is(perr, dds.ErrInvalidFrame) {
			t.Fatalf("frame truncated %d error %v misses sentinel", n, perr)
		}
	}
	_, merr := dds.MarshalRTPSPayload(nil)
	if merr == nil {
		t.Fatalf("marshal nil succeeded, want error")
	}
	_, perr := dds.ParseRTPSPayload([]byte{})
	if perr == nil {
		t.Fatalf("parse empty succeeded, want error")
	}
	enc := types.NewEncoder(true)
	err = enc.WriteString("truncate me")
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	wire := enc.Bytes()
	for _, n := range []int{0, 1, 2, 3, 4, 5, len(wire) - 1} {
		dec, derr := types.NewDecoder(wire[:n])
		if derr != nil {
			continue
		}
		_, rerr := dec.ReadString()
		if rerr != nil {
			continue
		}
		ferr := dec.Finish()
		if ferr != nil {
			continue
		}
	}
}
