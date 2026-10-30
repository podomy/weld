// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/types"
)

// recordingWriter records DATA frames for golden and ordering checks.
//
// It copies each frame on write per copy once alignment. fail, when non nil,
// is returned instead of recording. It is safe for concurrent use.
type recordingWriter struct {
	mu     sync.Mutex
	frames [][]byte
	fail   error
}

// WriteFrame records one frame or returns the injected failure.
func (w *recordingWriter) WriteFrame(frame []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fail != nil {
		return w.fail
	}

	cp := append([]byte(nil), frame...)
	w.frames = append(w.frames, cp)

	return nil
}

// frameCount returns the number of recorded frames.
func (w *recordingWriter) frameCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.frames)
}

// frameAt returns a copy of the frame at index i.
func (w *recordingWriter) frameAt(t *testing.T, i int) []byte {
	t.Helper()

	w.mu.Lock()
	defer w.mu.Unlock()

	if i < 0 || i >= len(w.frames) {
		t.Fatalf("frame index %d out of range %d", i, len(w.frames))
	}

	return append([]byte(nil), w.frames[i]...)
}

// lastFrame returns a copy of the most recent frame.
func (w *recordingWriter) lastFrame(t *testing.T) []byte {
	t.Helper()

	if w.frameCount() == 0 {
		t.Fatalf("no frames recorded")
	}

	return w.frameAt(t, w.frameCount()-1)
}

// testWriterGUID returns a fixed GUID for deterministic goldens.
func testWriterGUID() WriterGUID {
	var guid WriterGUID

	for i := range guid {
		guid[i] = byte(i + 1)
	}

	return guid
}

// mustNewPublisher returns a Publisher or fails the test.
//
// It mirrors helpers.MustNewPublisher for package local use; importing the
// harness here would cycle back to dds.
func mustNewPublisher(t *testing.T, guid WriterGUID, sink Writer) *Publisher {
	t.Helper()

	pub, err := NewPublisher(guid, sink)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	return pub
}

// mustPublish publishes one message or fails the test.
func mustPublish(t *testing.T, pub *Publisher, msg transports.OutgoingMessage) {
	t.Helper()

	err := pub.Publish(msg)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// mustTake takes one frame or fails the test.
func mustTake(t *testing.T, recv *DataReceiver, frame []byte) transports.IncomingMessage {
	t.Helper()

	msg, err := recv.Take(frame)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	return msg
}

// checkEnvelopeHeader verifies the LE envelope header at offset off.
func checkEnvelopeHeader(t *testing.T, frame []byte, off int) {
	t.Helper()

	if len(frame) < off+4 {
		t.Fatalf("frame length %d below envelope header at %d", len(frame), off)
	}

	if frame[off] != 0x00 || frame[off+1] != 0x01 || frame[off+2] != 0x00 || frame[off+3] != 0x00 {
		t.Fatalf("bad envelope header %v at %d", frame[off:off+4], off)
	}
}

// TestDataPathPublishGolden checks header plus DATA plus payload bytes.
func TestDataPathPublishGolden(t *testing.T) {
	t.Parallel()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)

	payload := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	mustPublish(t, pub, transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   payload,
	})

	if sink.frameCount() != 1 {
		t.Fatalf("frames = %d, want 1", sink.frameCount())
	}

	frame := sink.lastFrame(t)

	if len(frame) != 75 {
		t.Fatalf("frame length = %d, want 75", len(frame))
	}

	decoded, err := decodeFrame(frame)
	if err != nil {
		t.Fatalf("decode golden: %v", err)
	}

	if decoded.seq != 1 {
		t.Fatalf("seq = %d, want 1", decoded.seq)
	}

	if decoded.ddsTopic != "rt/chatter" {
		t.Fatalf("dds topic = %q, want rt/chatter", decoded.ddsTopic)
	}

	if decoded.typeName != "std_msgs__msg__String" {
		t.Fatalf("type = %q", decoded.typeName)
	}

	if decoded.userLen != 4 {
		t.Fatalf("user length = %d, want 4", decoded.userLen)
	}

	envelopeOff := frameFixedSize + len(decoded.ddsTopic) + len(decoded.typeName)
	checkEnvelopeHeader(t, frame, envelopeOff)

	tail := frame[envelopeOff+4:]
	if !bytes.Equal(tail, payload) {
		t.Fatalf("payload tail = %v, want %v", tail, payload)
	}

	if decoded.guid != testWriterGUID() {
		t.Fatalf("guid mismatch")
	}
}

// TestDataPathSequenceStartsAtOne checks the first sequence is 1.
func TestDataPathSequenceStartsAtOne(t *testing.T) {
	t.Parallel()

	if FirstSequenceNumber != 1 {
		t.Fatalf("first sequence = %d, want 1", FirstSequenceNumber)
	}

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)

	if pub.NextSequence() != 1 {
		t.Fatalf("next = %d, want 1", pub.NextSequence())
	}

	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{1, 2, 3, 4}})

	if pub.NextSequence() != 2 {
		t.Fatalf("next after one publish = %d, want 2", pub.NextSequence())
	}

	first := sink.lastFrame(t)
	decodedFirst, err := decodeFrame(first)
	if err != nil {
		t.Fatalf("decode first: %v", err)
	}

	if decodedFirst.seq != 1 {
		t.Fatalf("first seq = %d, want 1", decodedFirst.seq)
	}

	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{5, 6, 7, 8}})

	if pub.NextSequence() != 3 {
		t.Fatalf("next after two publishes = %d, want 3", pub.NextSequence())
	}

	second := sink.lastFrame(t)
	decodedSecond, err := decodeFrame(second)
	if err != nil {
		t.Fatalf("decode second: %v", err)
	}

	if decodedSecond.seq != 2 {
		t.Fatalf("second seq = %d, want 2", decodedSecond.seq)
	}
}

// TestDataPathGapDropCounter checks gap lost count and stale drop.
func TestDataPathGapDropCounter(t *testing.T) {
	t.Parallel()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)

	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{1, 1, 1, 1}})
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{2, 2, 2, 2}})
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{3, 3, 3, 3}})

	first := sink.frameAt(t, 0)
	third := sink.frameAt(t, 2)

	recv := NewDataReceiver()

	mustTake(t, recv, first)

	if recv.LostCount() != 0 {
		t.Fatalf("lost after first = %d, want 0", recv.LostCount())
	}

	msg, err := recv.Take(third)
	if err != nil {
		t.Fatalf("take gap frame: %v", err)
	}

	if recv.LostCount() != 1 {
		t.Fatalf("lost after gap = %d, want 1", recv.LostCount())
	}

	if len(msg.Payload) != 4 || msg.Payload[0] != 3 {
		t.Fatalf("gap payload = %v, want [3 3 3 3]", msg.Payload)
	}

	second := sink.frameAt(t, 1)
	_, err = recv.Take(second)
	if !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("stale take = %v, want ErrStaleSequence", err)
	}

	if recv.DroppedCount() != 1 {
		t.Fatalf("drops after stale = %d, want 1", recv.DroppedCount())
	}

	if recv.LostCount() != 1 {
		t.Fatalf("lost after stale = %d, want 1", recv.LostCount())
	}
}

// TestDataPathTruncatedDrop checks truncated input drops without panic.
func TestDataPathTruncatedDrop(t *testing.T) {
	t.Parallel()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{9, 9, 9, 9}})

	full := sink.lastFrame(t)
	recv := NewDataReceiver()

	truncations := []int{0, 1, 10, 35, 36, len(full) - 5, len(full) - 1}
	for _, n := range truncations {
		_, err := recv.Take(full[:n])
		if err == nil {
			t.Fatalf("truncated len %d succeeded, want error", n)
		}

		if !errors.Is(err, ErrTruncated) && !errors.Is(err, ErrInvalidFrame) {
			t.Fatalf("truncated len %d error %v misses truncated or invalid", n, err)
		}
	}

	if recv.DroppedCount() != uint64(len(truncations)) {
		t.Fatalf("drops = %d, want %d", recv.DroppedCount(), len(truncations))
	}

	if recv.LostCount() != 0 {
		t.Fatalf("lost = %d, want 0", recv.LostCount())
	}

	bad := append([]byte(nil), full...)
	envelopeOff := len(bad) - 8
	bad[envelopeOff] = 0x01

	_, err := recv.Take(bad)
	if err == nil {
		t.Fatalf("bad envelope header succeeded, want error")
	}

	if recv.DroppedCount() != uint64(len(truncations))+1 {
		t.Fatalf("drops after bad header = %d", recv.DroppedCount())
	}
}

// TestDataPathDemangleRoundTrip checks topic mapping both directions.
func TestDataPathDemangleRoundTrip(t *testing.T) {
	t.Parallel()

	topics := []string{"/chatter", "/ns/topic", "/a/b/c"}

	for _, ros := range topics {
		sink := &recordingWriter{}
		pub := mustNewPublisher(t, testWriterGUID(), sink)
		mustPublish(t, pub, transports.OutgoingMessage{TopicName: ros, TypeName: "std_msgs__msg__String", Payload: []byte{7, 7, 7, 7}})

		frame := sink.lastFrame(t)
		recv := NewDataReceiver()
		msg := mustTake(t, recv, frame)

		if msg.TopicName != ros {
			t.Fatalf("round trip %q = %q", ros, msg.TopicName)
		}
	}

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{7, 7, 7, 7}})

	frame := sink.lastFrame(t)
	decoded, err := decodeFrame(frame)
	if err != nil {
		t.Fatalf("decode for demangle: %v", err)
	}

	back, kind, err := ParseTopicName(decoded.ddsTopic)
	if err != nil {
		t.Fatalf("parse demangle: %v", err)
	}

	if back != "/chatter" || kind != TopicKindTopic {
		t.Fatalf("demangle = (%q, %q), want (/chatter, topic)", back, kind)
	}

	recv := NewDataReceiver()
	msg := mustTake(t, recv, frame)

	if msg.TopicName != back {
		t.Fatalf("take topic = %q, want %q", msg.TopicName, back)
	}
}

// TestDataPathEmptyPayloadDummyRule checks empty encodes a dummy byte.
func TestDataPathEmptyPayloadDummyRule(t *testing.T) {
	t.Parallel()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String"})

	frame := sink.lastFrame(t)
	decoded, err := decodeFrame(frame)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}

	if decoded.userLen != 0 {
		t.Fatalf("empty user length = %d, want 0", decoded.userLen)
	}

	envelopeOff := frameFixedSize + len(decoded.ddsTopic) + len(decoded.typeName)
	checkEnvelopeHeader(t, frame, envelopeOff)

	if len(decoded.envelope) < 5 || decoded.envelope[4] != 0x00 {
		t.Fatalf("empty envelope misses dummy byte: %v", decoded.envelope)
	}

	recv := NewDataReceiver()
	msg := mustTake(t, recv, frame)

	if len(msg.Payload) != 0 {
		t.Fatalf("empty take payload len = %d, want 0", len(msg.Payload))
	}

	if recv.DroppedCount() != 0 {
		t.Fatalf("empty take drops = %d, want 0", recv.DroppedCount())
	}

	singleSink := &recordingWriter{}
	singlePub := mustNewPublisher(t, testWriterGUID(), singleSink)
	mustPublish(t, singlePub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{0x00}})

	singleFrame := singleSink.lastFrame(t)
	singleDecoded, err := decodeFrame(singleFrame)
	if err != nil {
		t.Fatalf("decode single zero: %v", err)
	}

	if singleDecoded.userLen != 1 {
		t.Fatalf("single zero user length = %d, want 1", singleDecoded.userLen)
	}

	singleRecv := NewDataReceiver()
	singleMsg := mustTake(t, singleRecv, singleFrame)

	if len(singleMsg.Payload) != 1 || singleMsg.Payload[0] != 0x00 {
		t.Fatalf("single zero payload = %v, want [0]", singleMsg.Payload)
	}
}

// TestDataPathCopyOnceOwnership checks caller mutation does not alias.
func TestDataPathCopyOnceOwnership(t *testing.T) {
	t.Parallel()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)

	payload := []byte{1, 2, 3, 4}
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: payload})

	payload[0] = 99

	frame := sink.lastFrame(t)
	decoded, err := decodeFrame(frame)
	if err != nil {
		t.Fatalf("decode ownership: %v", err)
	}

	envelopePayload := decoded.envelope[4:]
	if len(envelopePayload) < 4 || envelopePayload[0] != 1 {
		t.Fatalf("stored payload aliased caller mutation: %v", envelopePayload)
	}

	recv := NewDataReceiver()
	frameCopy := append([]byte(nil), frame...)
	msg := mustTake(t, recv, frameCopy)

	frameCopy[len(frameCopy)-1] = 99

	if msg.Payload[3] == 99 {
		t.Fatalf("take aliased input frame mutation")
	}

	msg.Payload[0] = 99

	fresh := append([]byte(nil), frame...)
	fresh[0] = testWriterGUID()[0]

	secondRecv := NewDataReceiver()
	second, err := secondRecv.Take(fresh)
	if err != nil {
		t.Fatalf("second take: %v", err)
	}

	if second.Payload[0] != 1 {
		t.Fatalf("second payload = %v, want first byte 1", second.Payload)
	}
}

// TestDataPathRejectsBadInput checks explicit errors on bad publish and take.
func TestDataPathRejectsBadInput(t *testing.T) {
	t.Parallel()

	_, err := NewPublisher(testWriterGUID(), nil)
	if err == nil {
		t.Fatalf("nil sink succeeded, want error")
	}

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)

	err = pub.Publish(transports.OutgoingMessage{TopicName: "///"})
	if err == nil {
		t.Fatalf("bad topic publish succeeded, want error")
	}

	injected := errors.New("dds_test: injected write failure")
	failingSink := &recordingWriter{fail: injected}
	failingPub := mustNewPublisher(t, testWriterGUID(), failingSink)

	err = failingPub.Publish(transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{1, 2, 3, 4}})
	if !errors.Is(err, injected) {
		t.Fatalf("write failure = %v, want injected", err)
	}

	recv := NewDataReceiver()

	_, err = recv.Take(nil)
	if err == nil {
		t.Fatalf("nil frame take succeeded, want error")
	}

	badTopicFrame, err := encodeFrame(testWriterGUID(), 1, "rq/svc", "t", 1, []byte{0x00, 0x01, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00})
	if err != nil {
		t.Fatalf("encode bad topic frame: %v", err)
	}

	_, err = recv.Take(badTopicFrame)
	if err == nil {
		t.Fatalf("bad service topic take succeeded, want error")
	}

	overflowPub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})
	overflowPub.next = 0

	err = overflowPub.Publish(transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("overflow publish = %v, want ErrInvalidFrame", err)
	}
}

// recordingFrameSink records RTPS payloads for bridge golden checks.
//
// It copies each payload on write per copy once alignment. fail, when non
// nil, is returned instead of recording. It is safe for concurrent use.
type recordingFrameSink struct {
	mu       sync.Mutex
	guids    []WriterGUID
	payloads [][]byte
	fail     error
}

// WriteFrame records one RTPS payload or returns the injected failure.
func (s *recordingFrameSink) WriteFrame(ctx context.Context, guid WriterGUID, payload []byte) error {
	err := ctx.Err()
	if err != nil {
		return err //nolint:wrapcheck // ctx error passes through unchanged
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fail != nil {
		return s.fail
	}

	cp := append([]byte(nil), payload...)
	s.guids = append(s.guids, guid)
	s.payloads = append(s.payloads, cp)

	return nil
}

// count returns the number of recorded payloads.
func (s *recordingFrameSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.payloads)
}

// payloadAt returns a copy of the payload at index i.
func (s *recordingFrameSink) payloadAt(t *testing.T, i int) []byte {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if i < 0 || i >= len(s.payloads) {
		t.Fatalf("payload index %d out of range %d", i, len(s.payloads))
	}

	return append([]byte(nil), s.payloads[i]...)
}

// guidAt returns the GUID at index i.
func (s *recordingFrameSink) guidAt(t *testing.T, i int) WriterGUID {
	t.Helper()

	s.mu.Lock()
	defer s.mu.Unlock()

	if i < 0 || i >= len(s.guids) {
		t.Fatalf("guid index %d out of range %d", i, len(s.guids))
	}

	return s.guids[i]
}

// sliceFrameSource replays recorded RTPS payloads for TakeFrom checks.
//
// It is safe for concurrent use.
type sliceFrameSource struct {
	mu       sync.Mutex
	guids    []WriterGUID
	payloads [][]byte
	idx      int
	fail     error
}

// ReadFrame returns the next queued payload or the injected failure.
func (s *sliceFrameSource) ReadFrame(ctx context.Context) (WriterGUID, uint64, []byte, error) {
	err := ctx.Err()
	if err != nil {
		return WriterGUID{}, 0, nil, err //nolint:wrapcheck // ctx error passes through unchanged
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.fail != nil {
		return WriterGUID{}, 0, nil, s.fail
	}

	if s.idx >= len(s.payloads) {
		return WriterGUID{}, 0, nil, context.DeadlineExceeded
	}

	guid := s.guids[s.idx]
	cp := append([]byte(nil), s.payloads[s.idx]...)
	s.idx++

	return guid, 0, cp, nil
}

// setFail installs an injected failure under lock for race safety.
func (s *sliceFrameSource) setFail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.fail = err
}

// queue appends one guid plus payload pair for later reads.
func (s *sliceFrameSource) queue(guid WriterGUID, payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.guids = append(s.guids, guid)
	s.payloads = append(s.payloads, append([]byte(nil), payload...))
}

// checkBridgeRoundTrip verifies one payload through marshal, parse, and take.
func checkBridgeRoundTrip(t *testing.T, want []byte) {
	t.Helper()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: want})

	frame := sink.lastFrame(t)

	rtps, err := MarshalRTPSPayload(frame)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if !bytes.Equal(rtps, frame) {
		t.Fatalf("rtps payload differs from frame")
	}

	back, err := ParseRTPSPayload(rtps)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if !bytes.Equal(back, frame) {
		t.Fatalf("parse result differs from frame")
	}

	recv := NewDataReceiver()
	msg := mustTake(t, recv, back)

	if msg.TopicName != "/chatter" {
		t.Fatalf("topic = %q, want /chatter", msg.TopicName)
	}

	if len(want) == 0 {
		if len(msg.Payload) != 0 {
			t.Fatalf("empty payload = %v, want empty", msg.Payload)
		}

		return
	}

	if !bytes.Equal(msg.Payload, want) {
		t.Fatalf("payload = %v, want %v", msg.Payload, want)
	}
}

// TestBridgeMarshalParseRoundTrip checks frame to payload and back.
func TestBridgeMarshalParseRoundTrip(t *testing.T) {
	t.Parallel()

	payloads := [][]byte{
		{0xAA, 0xBB, 0xCC, 0xDD},
		nil,
		{0x00},
		{1, 2, 3, 4, 5, 6, 7, 8},
	}

	for _, want := range payloads {
		checkBridgeRoundTrip(t, want)
	}
}

// checkBridgeSentinel verifies both bridge directions reject bad input.
func checkBridgeSentinel(t *testing.T, bad []byte) {
	t.Helper()

	_, err := MarshalRTPSPayload(bad)
	if err == nil {
		t.Fatalf("marshal bad input succeeded, want error")
	}

	if !errors.Is(err, ErrTruncated) && !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("marshal bad error %v misses sentinel", err)
	}

	_, err = ParseRTPSPayload(bad)
	if err == nil {
		t.Fatalf("parse bad input succeeded, want error")
	}

	if !errors.Is(err, ErrTruncated) && !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("parse bad error %v misses sentinel", err)
	}
}

// mustBridgeGoldenFrame publishes one sample and returns the frame.
func mustBridgeGoldenFrame(t *testing.T) []byte {
	t.Helper()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{9, 9, 9, 9}})

	return sink.lastFrame(t)
}

// TestBridgeRejectsTruncated checks sentinel errors on short input.
func TestBridgeRejectsTruncated(t *testing.T) {
	t.Parallel()

	full := mustBridgeGoldenFrame(t)
	truncations := []int{0, 1, 10, 35, 36, len(full) - 5, len(full) - 1}

	for _, n := range truncations {
		checkBridgeSentinel(t, full[:n])
	}
}

// TestBridgeRejectsCorrupt checks sentinel errors on bad sequence and header.
func TestBridgeRejectsCorrupt(t *testing.T) {
	t.Parallel()

	full := mustBridgeGoldenFrame(t)

	badSeq := append([]byte(nil), full...)
	for i := range seqSize {
		badSeq[guidSize+i] = 0x00
	}

	checkBridgeSentinel(t, badSeq)
	checkBridgeSentinel(t, nil)
	checkBridgeSentinel(t, []byte{})

	badHeader := append([]byte(nil), full...)
	envelopeOff := len(badHeader) - 8
	badHeader[envelopeOff] = 0x01
	checkBridgeSentinel(t, badHeader)
}

// TestBridgeRejectsBadTopic checks invalid topic maps to ErrInvalidFrame.
func TestBridgeRejectsBadTopic(t *testing.T) {
	t.Parallel()

	badTopicFrame, err := encodeFrame(testWriterGUID(), 1, "rq/svc", "t", 1, []byte{0x00, 0x01, 0x00, 0x00, 0x09, 0x00, 0x00, 0x00})
	if err != nil {
		t.Fatalf("encode bad topic frame: %v", err)
	}

	_, err = MarshalRTPSPayload(badTopicFrame)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("marshal bad topic = %v, want ErrInvalidFrame", err)
	}

	_, err = ParseRTPSPayload(badTopicFrame)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("parse bad topic = %v, want ErrInvalidFrame", err)
	}
}

// mustBridgeGoldenPair publishes one message on both paths and returns the frame.
func mustBridgeGoldenPair(t *testing.T, ctx context.Context, msg transports.OutgoingMessage) ([]byte, []byte) {
	t.Helper()

	writerSink := &recordingWriter{}
	writerPub := mustNewPublisher(t, testWriterGUID(), writerSink)
	mustPublish(t, writerPub, msg)

	bridgeSink := &recordingFrameSink{}
	bridgePub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})

	err := bridgePub.PublishTo(ctx, bridgeSink, msg)
	if err != nil {
		t.Fatalf("publish to: %v", err)
	}

	if bridgeSink.count() != 1 {
		t.Fatalf("sink count = %d, want 1", bridgeSink.count())
	}

	if got := bridgeSink.guidAt(t, 0); got != testWriterGUID() {
		t.Fatalf("bridge guid mismatch")
	}

	if bridgePub.NextSequence() != 2 {
		t.Fatalf("bridge next = %d, want 2", bridgePub.NextSequence())
	}

	frameGolden := writerSink.lastFrame(t)
	bridgePayload := bridgeSink.payloadAt(t, 0)

	return frameGolden, bridgePayload
}

// TestBridgeSinkSourceGoldens checks PublishTo and TakeFrom match Publish and Take.
func TestBridgeSinkSourceGoldens(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	msg := transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: []byte{0xAA, 0xBB, 0xCC, 0xDD}}

	frameGolden, bridgePayload := mustBridgeGoldenPair(t, ctx, msg)

	if !bytes.Equal(bridgePayload, frameGolden) {
		t.Fatalf("bridge payload differs from writer golden")
	}

	recvDirect := NewDataReceiver()
	direct := mustTake(t, recvDirect, frameGolden)

	src := &sliceFrameSource{}
	src.queue(testWriterGUID(), bridgePayload)

	recvBridge := NewDataReceiver()
	bridged, err := recvBridge.TakeFrom(ctx, src)
	if err != nil {
		t.Fatalf("take from: %v", err)
	}

	if bridged.TopicName != direct.TopicName || bridged.TypeName != direct.TypeName || !bytes.Equal(bridged.Payload, direct.Payload) {
		t.Fatalf("bridged %+v differs from direct %+v", bridged, direct)
	}

	if recvBridge.LostCount() != recvDirect.LostCount() {
		t.Fatalf("lost bridge %d direct %d", recvBridge.LostCount(), recvDirect.LostCount())
	}
}

// TestBridgeGapMatches checks gap lost counts agree on both paths.
func TestBridgeGapMatches(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	payloads := [][]byte{{1, 1, 1, 1}, {2, 2, 2, 2}, {3, 3, 3, 3}}

	writerSink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), writerSink)

	for _, p := range payloads {
		mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: p})
	}

	bridgeSink := &recordingFrameSink{}
	bridgePub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})

	for _, p := range payloads {
		err := bridgePub.PublishTo(ctx, bridgeSink, transports.OutgoingMessage{TopicName: "/chatter", Payload: p})
		if err != nil {
			t.Fatalf("publish to gap: %v", err)
		}
	}

	recvDirect := NewDataReceiver()
	mustTake(t, recvDirect, writerSink.frameAt(t, 0))

	_, err := recvDirect.Take(writerSink.frameAt(t, 2))
	if err != nil {
		t.Fatalf("direct gap take: %v", err)
	}

	src := &sliceFrameSource{}
	src.queue(testWriterGUID(), bridgeSink.payloadAt(t, 0))
	src.queue(testWriterGUID(), bridgeSink.payloadAt(t, 2))

	recvBridge := NewDataReceiver()

	_, err = recvBridge.TakeFrom(ctx, src)
	if err != nil {
		t.Fatalf("bridge gap first: %v", err)
	}

	_, err = recvBridge.TakeFrom(ctx, src)
	if err != nil {
		t.Fatalf("bridge gap third: %v", err)
	}

	if recvBridge.LostCount() != 1 {
		t.Fatalf("gap lost bridge = %d, want 1", recvBridge.LostCount())
	}

	if recvBridge.LostCount() != recvDirect.LostCount() {
		t.Fatalf("gap lost mismatch bridge %d direct %d", recvBridge.LostCount(), recvDirect.LostCount())
	}
}

// TestBridgeGuidMismatchDrops checks outer GUID mismatch drops with ErrInvalidFrame.
func TestBridgeGuidMismatchDrops(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	msg := transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{0xAA, 0xBB, 0xCC, 0xDD}}

	_, bridgePayload := mustBridgeGoldenPair(t, ctx, msg)

	var otherGUID WriterGUID

	otherGUID[0] = 0xFF

	src := &sliceFrameSource{}
	src.queue(otherGUID, bridgePayload)

	recv := NewDataReceiver()

	_, err := recv.TakeFrom(ctx, src)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("guid mismatch = %v, want ErrInvalidFrame", err)
	}

	if recv.DroppedCount() != 1 {
		t.Fatalf("mismatch drops = %d, want 1", recv.DroppedCount())
	}
}

// TestBridgeNilSinkSource checks nil sink and source report ErrInvalidFrame.
func TestBridgeNilSinkSource(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	msg := transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{1, 2, 3, 4}}

	pub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})

	err := pub.PublishTo(ctx, nil, msg)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("nil sink publish = %v, want ErrInvalidFrame", err)
	}

	_, err = NewDataReceiver().TakeFrom(ctx, nil)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("nil source take = %v, want ErrInvalidFrame", err)
	}
}

// TestBridgeContextCancel checks canceled context reports ctx error.
func TestBridgeContextCancel(t *testing.T) {
	t.Parallel()

	msg := transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{1, 2, 3, 4}}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	pub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})
	before := pub.NextSequence()

	err := pub.PublishTo(canceled, &recordingFrameSink{}, msg)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publish = %v, want context.Canceled", err)
	}

	if pub.NextSequence() != before {
		t.Fatalf("canceled publish advanced sequence")
	}

	src := &sliceFrameSource{}

	_, err = NewDataReceiver().TakeFrom(canceled, src)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled take = %v, want context.Canceled", err)
	}
}

// TestBridgeMarshalOwnership checks marshal and parse copy ownership.
func TestBridgeMarshalOwnership(t *testing.T) {
	t.Parallel()

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{1, 2, 3, 4}})

	frame := sink.lastFrame(t)

	rtps, err := MarshalRTPSPayload(frame)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	frame[0] ^= 0xFF
	if rtps[0] == frame[0] {
		t.Fatalf("marshal aliased input mutation")
	}

	frame[0] ^= 0xFF
	origFirst := frame[0]

	rtps[0] ^= 0xFF
	if frame[0] != origFirst {
		t.Fatalf("marshal result aliases input storage")
	}

	rtps[0] ^= 0xFF

	parsed, err := ParseRTPSPayload(rtps)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	origLast := rtps[len(rtps)-1]

	parsed[len(parsed)-1] ^= 0xFF
	if rtps[len(rtps)-1] != origLast {
		t.Fatalf("parse result aliases input storage")
	}
}

// TestBridgePublishToOwnership checks PublishTo copies caller payload.
func TestBridgePublishToOwnership(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	bridgeSink := &recordingFrameSink{}
	bridgePub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})
	payload := []byte{5, 6, 7, 8}

	err := bridgePub.PublishTo(ctx, bridgeSink, transports.OutgoingMessage{TopicName: "/chatter", Payload: payload})
	if err != nil {
		t.Fatalf("publish to: %v", err)
	}

	payload[0] = 99

	stored := bridgeSink.payloadAt(t, 0)
	decoded, err := decodeFrame(stored)
	if err != nil {
		t.Fatalf("decode stored: %v", err)
	}

	if decoded.envelope[4] == 99 {
		t.Fatalf("publish to aliased caller mutation")
	}

	stored[0] ^= 0xFF
	fresh := bridgeSink.payloadAt(t, 0)
	if bytes.Equal(stored, fresh) {
		t.Fatalf("sink copy aliases internal storage")
	}
}

// TestBridgeTakeFromOwnership checks TakeFrom copies source bytes.
func TestBridgeTakeFromOwnership(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	sink := &recordingFrameSink{}
	pub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})

	err := pub.PublishTo(ctx, sink, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{5, 6, 7, 8}})
	if err != nil {
		t.Fatalf("publish to: %v", err)
	}

	fresh := sink.payloadAt(t, 0)

	src := &sliceFrameSource{}
	src.queue(testWriterGUID(), fresh)

	got, err := NewDataReceiver().TakeFrom(ctx, src)
	if err != nil {
		t.Fatalf("take from: %v", err)
	}

	if len(got.Payload) == 0 || got.Payload[0] != 5 {
		t.Fatalf("take from payload = %v, want first byte 5", got.Payload)
	}

	got.Payload[0] = 99

	src2 := &sliceFrameSource{}
	src2.queue(testWriterGUID(), fresh)

	got2, err := NewDataReceiver().TakeFrom(ctx, src2)
	if err != nil {
		t.Fatalf("second take from: %v", err)
	}

	if got2.Payload[0] != 5 {
		t.Fatalf("second take from payload = %v, want first byte 5", got2.Payload)
	}
}

// TestPublishToOverflowAndCaps checks sequence overflow plus payload caps on PublishTo.
func TestPublishToOverflowAndCaps(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	overflowPub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})
	overflowPub.next = 0

	err := overflowPub.PublishTo(ctx, &recordingFrameSink{}, transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("overflow publish to = %v, want ErrInvalidFrame", err)
	}

	big := make([]byte, maxPayloadLen+1)
	bigPub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})

	err = bigPub.PublishTo(ctx, &recordingFrameSink{}, transports.OutgoingMessage{TopicName: "/chatter", Payload: big})
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("oversize publish to = %v, want ErrInvalidFrame", err)
	}

	if bigPub.NextSequence() != FirstSequenceNumber {
		t.Fatalf("oversize advanced sequence to %d", bigPub.NextSequence())
	}

	nilPub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})
	before := nilPub.NextSequence()

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	err = nilPub.PublishTo(canceled, &recordingFrameSink{}, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{1}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publish to = %v, want context.Canceled", err)
	}

	if nilPub.NextSequence() != before {
		t.Fatalf("canceled publish to advanced sequence")
	}
}

// TestPublishConcurrentOrdering checks concurrent Publish calls keep sequence order.
func TestPublishConcurrentOrdering(t *testing.T) {
	t.Parallel()

	const writers = 16

	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)

	var wg sync.WaitGroup

	for i := range writers {
		wg.Go(func() {
			payload := []byte{byte(i + 1), 0x00, 0x01, 0x02} //nolint:gosec // G115: writers is 16, conversion safe
			err := pub.Publish(transports.OutgoingMessage{TopicName: "/chatter", Payload: payload})
			if err != nil {
				t.Errorf("concurrent publish %d: %v", i, err)
			}
		})
	}

	wg.Wait()

	if sink.frameCount() != writers {
		t.Fatalf("frames = %d, want %d", sink.frameCount(), writers)
	}

	if pub.NextSequence() != FirstSequenceNumber+writers {
		t.Fatalf("next = %d, want %d", pub.NextSequence(), FirstSequenceNumber+writers)
	}

	seen := make(map[uint64]bool, writers)

	for i := range writers {
		frame := sink.frameAt(t, i)

		decoded, err := decodeFrame(frame)
		if err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}

		if seen[decoded.seq] {
			t.Fatalf("duplicate seq %d", decoded.seq)
		}

		seen[decoded.seq] = true
	}

	for seq := uint64(1); seq <= writers; seq++ {
		if !seen[seq] {
			t.Fatalf("missing seq %d", seq)
		}
	}
}

// TestPublishToConcurrentOrdering checks concurrent PublishTo calls keep sequence order.
func TestPublishToConcurrentOrdering(t *testing.T) {
	t.Parallel()

	const writers = 16

	sink := &recordingFrameSink{}
	pub := mustNewPublisher(t, testWriterGUID(), &recordingWriter{})

	ctx := t.Context()

	var wg sync.WaitGroup

	for i := range writers {
		wg.Go(func() {
			payload := []byte{byte(i + 1), 0x10, 0x11, 0x12} //nolint:gosec // G115: writers is 16, conversion safe
			err := pub.PublishTo(ctx, sink, transports.OutgoingMessage{TopicName: "/chatter", Payload: payload})
			if err != nil {
				t.Errorf("concurrent publish to %d: %v", i, err)
			}
		})
	}

	wg.Wait()

	if sink.count() != writers {
		t.Fatalf("payloads = %d, want %d", sink.count(), writers)
	}

	if pub.NextSequence() != FirstSequenceNumber+writers {
		t.Fatalf("next = %d, want %d", pub.NextSequence(), FirstSequenceNumber+writers)
	}

	seen := make(map[uint64]bool, writers)

	for i := range writers {
		payload := sink.payloadAt(t, i)

		decoded, err := decodeFrame(payload)
		if err != nil {
			t.Fatalf("decode %d: %v", i, err)
		}

		if seen[decoded.seq] {
			t.Fatalf("duplicate seq %d", decoded.seq)
		}

		seen[decoded.seq] = true
	}

	for seq := uint64(1); seq <= writers; seq++ {
		if !seen[seq] {
			t.Fatalf("missing seq %d", seq)
		}
	}
}

// TestTakeFromCancelAndRouteUnknown checks TakeFrom cancel plus read failure plus unknown route.
func TestTakeFromCancelAndRouteUnknown(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	canceled, cancel := context.WithCancel(ctx)
	cancel()

	_, err := NewDataReceiver().TakeFrom(canceled, &sliceFrameSource{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled take from = %v, want context.Canceled", err)
	}

	failSrc := &sliceFrameSource{}
	failSrc.setFail(errors.New("dds_test: injected read failure"))

	_, err = NewDataReceiver().TakeFrom(ctx, failSrc)
	if err == nil {
		t.Fatalf("failing source take succeeded, want error")
	}

	emptySrc := &sliceFrameSource{}
	_, err = NewDataReceiver().TakeFrom(ctx, emptySrc)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty source take = %v, want DeadlineExceeded", err)
	}
}

// encodeTestString encodes text as XCDR1 String bytes for plain CDR tests.
func encodeTestString(t *testing.T, text string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := enc.WriteString(text)
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	wire := enc.Bytes()
	if len(wire) == 0 {
		t.Fatalf("encoded string is empty")
	}
	return wire
}

// writerGUIDForSEDP builds a WriterGUID from an SEDP GUID for test mapping.
func writerGUIDForSEDP(id SEDPGUID) WriterGUID {
	var guid WriterGUID
	copy(guid[:12], id.Prefix[:])
	copy(guid[12:16], id.Entity[:])
	return guid
}

// newPerRunReceiver installs one remote writer for a per-run topic and returns the receiver plus guid.
func newPerRunReceiver(t *testing.T, rosTopic string) (*DataReceiver, WriterGUID) {
	t.Helper()
	var prefix SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = byte(i + 11)
	}
	idx, err := NewSEDPIndex(prefix, 65)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}
	ddsTopic, err := TopicName(rosTopic, false)
	if err != nil {
		t.Fatalf("topic name %q: %v", rosTopic, err)
	}
	ddsType, err := MessageTypeName("std_msgs__msg__String")
	if err != nil {
		t.Fatalf("message type name: %v", err)
	}
	id := SEDPGUID{Prefix: prefix, Entity: SEDPEntityID{0x00, 0x00, 0x02, 0x03}}
	ep := SEDPEndpoint{
		ID:       id,
		ROSTopic: rosTopic,
		DDSTopic: ddsTopic,
		TypeDDS:  ddsType,
		TypeIDL:  "std_msgs__msg__String",
		IsReader: false,
		DomainID: 65,
	}
	err = idx.UpsertRemote(ep)
	if err != nil {
		t.Fatalf("upsert remote: %v", err)
	}
	recv := NewDataReceiver()
	recv.SetSEDPIndex(idx)
	return recv, writerGUIDForSEDP(id)
}

// TestDecodeCycloneChatterMapsPerRunTopic checks the fallback returns the mapped topic.
func TestDecodeCycloneChatterMapsPerRunTopic(t *testing.T) {
	t.Parallel()
	rosTopic := "/chatter_ros_late_123_fwd"
	recv, guid := newPerRunReceiver(t, rosTopic)
	payload := encodeTestString(t, "hello late")
	msg, ok := recv.decodeCycloneChatter(guid, payload)
	if !ok {
		t.Fatalf("decode chatter on %q returned false, want true", rosTopic)
	}
	if msg.TopicName != rosTopic {
		t.Fatalf("chatter topic = %q, want %q", msg.TopicName, rosTopic)
	}
	if msg.TypeName != "std_msgs__msg__String" {
		t.Fatalf("chatter type = %q, want std_msgs__msg__String", msg.TypeName)
	}
	if !bytes.Equal(msg.Payload, payload) {
		t.Fatalf("chatter payload mismatch")
	}
}

// TestDecodeCycloneChatterUnknownWriterDrops checks unknown writers never map to /chatter.
func TestDecodeCycloneChatterUnknownWriterDrops(t *testing.T) {
	t.Parallel()
	recv := NewDataReceiver()
	payload := encodeTestString(t, "hello stray")
	var unknown WriterGUID
	unknown[0] = 0xFF
	_, ok := recv.decodeCycloneChatter(unknown, payload)
	if ok {
		t.Fatalf("decode chatter with unknown writer returned true, want false")
	}
}

// TestTakeFromPlainMapsPerRunTopic checks TakeFrom returns the SEDP mapped topic for plain CDR.
func TestTakeFromPlainMapsPerRunTopic(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rosTopic := "/chatter_ros_late_456_fwd"
	recv, guid := newPerRunReceiver(t, rosTopic)
	payload := encodeTestString(t, "hello late plain")
	src := &sliceFrameSource{}
	src.queue(guid, payload)
	msg, err := recv.TakeFrom(ctx, src)
	if err != nil {
		t.Fatalf("take from plain: %v", err)
	}
	if msg.TopicName != rosTopic {
		t.Fatalf("take from topic = %q, want %q", msg.TopicName, rosTopic)
	}
	if msg.TypeName != "std_msgs__msg__String" {
		t.Fatalf("take from type = %q, want std_msgs__msg__String", msg.TypeName)
	}
}

// TestTakeFromUnknownWriterPrefixCollisionDrops checks DATA from an unknown writer sharing a participant prefix drops.
func TestTakeFromUnknownWriterPrefixCollisionDrops(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	rosTopic := "/chatter_ros_unknown_789_fwd"
	recv, guid := newPerRunReceiver(t, rosTopic)
	var unknown WriterGUID
	copy(unknown[:12], guid[:12])
	copy(unknown[12:16], []byte{0x00, 0x00, 0x02, 0x04})
	if unknown == guid {
		t.Fatalf("unknown GUID equals known GUID")
	}
	payload := encodeTestString(t, "hello stray early")
	if _, ok := recv.remoteForGUID(unknown); ok {
		t.Fatalf("remote for unknown prefix collision returned true, want false")
	}
	if _, ok := recv.decodePlainCDR(unknown, payload); ok {
		t.Fatalf("decode plain CDR with unknown writer returned true, want false")
	}
	if _, ok := recv.decodeCycloneChatter(unknown, payload); ok {
		t.Fatalf("decode chatter with unknown writer returned true, want false")
	}
	src := &sliceFrameSource{}
	src.queue(unknown, payload)
	_, err := recv.TakeFrom(ctx, src)
	if err == nil {
		t.Fatalf("take from unknown writer succeeded, want drop")
	}
	if !errors.Is(err, ErrTruncated) && !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("take from unknown writer error %v misses truncated or invalid", err)
	}
	if recv.DroppedCount() != 1 {
		t.Fatalf("drops = %d, want 1", recv.DroppedCount())
	}
	if recv.LostCount() != 0 {
		t.Fatalf("lost = %d, want 0", recv.LostCount())
	}
}

// provisionalQoSSource replays one payload plus inline QoS for provisional pins.
//
// It implements FrameSource plus ReadFrameWithQoS so TakeFrom installs a
// provisional with topic, type, and hash from the DATA inline QoS where
// present. It is safe for concurrent use.
type provisionalQoSSource struct {
	mu       sync.Mutex
	guids    []WriterGUID
	payloads [][]byte
	qos      []ProvisionalQoS
	idx      int
}

// ReadFrame returns the next queued payload without QoS metadata.
func (s *provisionalQoSSource) ReadFrame(ctx context.Context) (WriterGUID, uint64, []byte, error) {
	guid, _, payload, _, _, err := s.ReadFrameWithQoS(ctx)

	return guid, 0, payload, err
}

// ReadFrameWithQoS returns the next queued payload plus its inline QoS.
func (s *provisionalQoSSource) ReadFrameWithQoS(ctx context.Context) (WriterGUID, uint64, []byte, ProvisionalQoS, bool, error) {
	err := ctx.Err()
	if err != nil {
		return WriterGUID{}, 0, nil, ProvisionalQoS{}, false, err //nolint:wrapcheck // ctx error passes through unchanged
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.idx >= len(s.payloads) {
		return WriterGUID{}, 0, nil, ProvisionalQoS{}, false, context.DeadlineExceeded
	}

	guid := s.guids[s.idx]
	cp := append([]byte(nil), s.payloads[s.idx]...)
	qos := s.qos[s.idx]
	s.idx++

	return guid, 0, cp, qos, true, nil
}

// queueProvisional appends one guid plus payload plus inline QoS for later reads.
func (s *provisionalQoSSource) queueProvisional(guid WriterGUID, payload []byte, qos ProvisionalQoS) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.guids = append(s.guids, guid)
	s.payloads = append(s.payloads, append([]byte(nil), payload...))
	s.qos = append(s.qos, qos)
}

// mustStringDDS returns the DDS type name for String or fails the test.
func mustStringDDS(t *testing.T) string {
	t.Helper()

	ddsType, err := MessageTypeName("std_msgs__msg__String")
	if err != nil {
		t.Fatalf("message type name: %v", err)
	}

	return ddsType
}

// TestProvisionalInstallDeliversPayload checks first DATA from an unknown writer installs provisional and delivers.
//
//nolint:cyclop // test pins provisional install, delivery, and counter checks in one run, splitting would obscure intent
func TestProvisionalInstallDeliversPayload(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	recv := NewDataReceiver()

	var guid WriterGUID
	guid[0] = 0xAA

	for i := 1; i < len(guid); i++ {
		guid[i] = byte(i)
	}

	payload := encodeTestString(t, "hello provisional")
	qos := ProvisionalQoS{DDSTopic: "rt/chatter", TypeDDS: mustStringDDS(t), HasHash: false}

	src := &provisionalQoSSource{}
	src.queueProvisional(guid, payload, qos)

	msg, err := recv.TakeFrom(ctx, src)
	if err != nil {
		t.Fatalf("take from provisional: %v", err)
	}

	if msg.TopicName != "/chatter" {
		t.Fatalf("provisional topic = %q, want /chatter", msg.TopicName)
	}

	if msg.TypeName != "std_msgs__msg__String" {
		t.Fatalf("provisional type = %q, want std_msgs__msg__String", msg.TypeName)
	}

	if string(append([]byte(nil), msg.Payload...)) == "" {
		t.Fatalf("provisional payload is empty")
	}

	prov, ok := recv.ProvisionalForGUID(guid)
	if !ok {
		t.Fatalf("provisional missing for unknown writer")
	}

	if !prov.Provisional {
		t.Fatalf("provisional flag = false, want true")
	}

	if prov.FirstSeen.IsZero() {
		t.Fatalf("provisional first-seen is zero")
	}

	if prov.DDSTopic != "rt/chatter" {
		t.Fatalf("provisional dds topic = %q, want rt/chatter", prov.DDSTopic)
	}

	if prov.HashSet {
		t.Fatalf("provisional hash-set = true, want false for hash-unset")
	}

	if recv.DroppedCount() != 0 {
		t.Fatalf("drops after provisional deliver = %d, want 0", recv.DroppedCount())
	}

	if recv.DropReasonCount(DropReasonProvisionalDecodeFail) != 0 {
		t.Fatalf("provisional-decode-fail = %d, want 0", recv.DropReasonCount(DropReasonProvisionalDecodeFail))
	}
}

// TestProvisionalConfirmedTakesPrecedence checks SEDP-confirmed records win over stale provisional fields.
func TestProvisionalConfirmedTakesPrecedence(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var prefix SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = byte(i + 21)
	}

	idx, err := NewSEDPIndex(prefix, 65)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}

	recv := NewDataReceiver()
	recv.SetSEDPIndex(idx)

	id := SEDPGUID{Prefix: prefix, Entity: SEDPEntityID{0x00, 0x00, 0x02, 0x03}}
	var guid WriterGUID
	copy(guid[:12], prefix[:])
	copy(guid[12:16], id.Entity[:])

	staleQoS := ProvisionalQoS{DDSTopic: "rt/stale_topic", TypeDDS: mustStringDDS(t), TypeHash: StringTypeHash, HasHash: true}
	payload := encodeTestString(t, "hello stale then confirmed")

	first := &provisionalQoSSource{}
	first.queueProvisional(guid, payload, staleQoS)

	firstMsg, err := recv.TakeFrom(ctx, first)
	if err != nil {
		t.Fatalf("first provisional take: %v", err)
	}

	if firstMsg.TopicName != "/stale_topic" {
		t.Fatalf("first topic = %q, want /stale_topic", firstMsg.TopicName)
	}

	stale, ok := recv.ProvisionalForGUID(guid)
	if !ok {
		t.Fatalf("stale provisional missing")
	}

	if stale.DDSTopic != "rt/stale_topic" {
		t.Fatalf("stale dds topic = %q, want rt/stale_topic", stale.DDSTopic)
	}

	confirmed := SEDPEndpoint{
		ID:       id,
		ROSTopic: "/confirmed_topic",
		DDSTopic: "rt/confirmed_topic",
		TypeDDS:  mustStringDDS(t),
		TypeIDL:  "std_msgs__msg__String",
		IsReader: false,
		DomainID: 65,
	}

	err = idx.UpsertRemote(confirmed)
	if err != nil {
		t.Fatalf("upsert confirmed: %v", err)
	}

	second := &provisionalQoSSource{}
	second.queueProvisional(guid, payload, staleQoS)

	secondMsg, err := recv.TakeFrom(ctx, second)
	if err != nil {
		t.Fatalf("second confirmed take: %v", err)
	}

	if secondMsg.TopicName != "/confirmed_topic" {
		t.Fatalf("second topic = %q, want /confirmed_topic (confirmed must win over stale %q)", secondMsg.TopicName, stale.DDSTopic)
	}

	if secondMsg.TypeName != "std_msgs__msg__String" {
		t.Fatalf("second type = %q, want std_msgs__msg__String", secondMsg.TypeName)
	}

	if recv.DroppedCount() != 0 {
		t.Fatalf("drops after confirmed precedence = %d, want 0", recv.DroppedCount())
	}
}

// TestProvisionalDecodeFailDropsWithReason checks undecodable provisional payloads drop with the new reason code.
func TestProvisionalDecodeFailDropsWithReason(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	recv := NewDataReceiver()

	var guid WriterGUID
	guid[0] = 0xBB

	for i := 1; i < len(guid); i++ {
		guid[i] = byte(0xC0 + i)
	}

	valid := encodeTestString(t, "hello provisional fail")
	bad := append(append([]byte(nil), valid...), 0xFF)
	qos := ProvisionalQoS{DDSTopic: "rt/chatter", TypeDDS: mustStringDDS(t), TypeHash: StringTypeHash, HasHash: true}

	src := &provisionalQoSSource{}
	src.queueProvisional(guid, bad, qos)

	_, err := recv.TakeFrom(ctx, src)
	if err == nil {
		t.Fatalf("undecodable provisional take succeeded, want drop")
	}

	if !errors.Is(err, ErrProvisionalDecodeFail) {
		t.Fatalf("provisional error %v misses ErrProvisionalDecodeFail", err)
	}

	if errors.Is(err, ErrUnknownWriter) {
		t.Fatalf("provisional error %v reuses unknown-writer reason", err)
	}

	if recv.DroppedCount() != 1 {
		t.Fatalf("drops = %d, want 1", recv.DroppedCount())
	}

	if recv.DropReasonCount(DropReasonProvisionalDecodeFail) != 1 {
		t.Fatalf("provisional-decode-fail = %d, want 1", recv.DropReasonCount(DropReasonProvisionalDecodeFail))
	}

	if recv.DropReasonCount(DropReasonUnknownWriter) != 0 {
		t.Fatalf("unknown-writer = %d, want 0", recv.DropReasonCount(DropReasonUnknownWriter))
	}

	if _, ok := recv.ProvisionalForGUID(guid); !ok {
		t.Fatalf("provisional missing after decode fail")
	}
}

// TestProvisionalConfirmCleanupAndTTL checks confirmed records evict stale
// receiver-local provisionals and aged provisionals expire on next insert,
// so the fast-path map stays bounded under writer churn.
func TestProvisionalConfirmCleanupAndTTL(t *testing.T) {
	t.Parallel()

	recv := NewDataReceiver()
	qos := ProvisionalQoS{DDSTopic: "rt/chatter", TypeDDS: mustStringDDS(t), HasHash: false}

	var guidA WriterGUID
	guidA[0] = 0xCC

	for i := 1; i < len(guidA); i++ {
		guidA[i] = byte(i)
	}

	if _, ok := recv.ensureProvisional(guidA, qos, true); !ok {
		t.Fatalf("provisional A install failed")
	}

	recv.mu.Lock()
	stale := recv.provisional[guidA]
	stale.FirstSeen = time.Now().Add(-sedpProvisionalTTL - time.Second)
	recv.provisional[guidA] = stale
	recv.mu.Unlock()

	var guidB WriterGUID
	guidB[0] = 0xCD

	for i := 1; i < len(guidB); i++ {
		guidB[i] = byte(0x40 + i)
	}

	if _, ok := recv.ensureProvisional(guidB, qos, true); !ok {
		t.Fatalf("provisional B install failed")
	}

	if _, ok := recv.ProvisionalForGUID(guidA); ok {
		t.Fatalf("stale provisional A survived TTL eviction")
	}

	var prefix SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = byte(i + 31)
	}

	idx, err := NewSEDPIndex(prefix, 65)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}

	recv.SetSEDPIndex(idx)

	id := SEDPGUID{Prefix: prefix, Entity: SEDPEntityID{0x00, 0x00, 0x02, 0x03}}

	var guidC WriterGUID
	copy(guidC[:12], prefix[:])
	copy(guidC[12:16], id.Entity[:])

	if _, ok := recv.ensureProvisional(guidC, qos, true); !ok {
		t.Fatalf("provisional C install failed")
	}

	confirmed := SEDPEndpoint{
		ID:       id,
		ROSTopic: "/chatter",
		DDSTopic: "rt/chatter",
		TypeDDS:  mustStringDDS(t),
		TypeIDL:  "std_msgs__msg__String",
		IsReader: false,
		DomainID: 65,
	}

	if err := idx.UpsertRemote(confirmed); err != nil {
		t.Fatalf("upsert confirmed: %v", err)
	}

	if _, ok := recv.ensureProvisional(guidC, qos, true); ok {
		t.Fatalf("ensure over confirmed returned provisional, want confirmed precedence")
	}

	if _, ok := recv.ProvisionalForGUID(guidC); ok {
		t.Fatalf("stale provisional C survived confirmed cleanup")
	}
}
