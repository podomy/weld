// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// fragSendDiscardWriter discards frames for Publisher construction.
type fragSendDiscardWriter struct{}

// WriteFrame discards one frame and reports success.
func (fragSendDiscardWriter) WriteFrame(_ []byte) error {
	return nil
}

// fragSendRecord holds one recorded fragment.
type fragSendRecord struct {
	seq          uint64
	fragNum      uint32
	total        uint32
	fragmentSize uint16
	sampleSize   uint32
	data         []byte
}

// fragSendTestSink records single writes plus ordered fragments.
//
// It implements FrameSink always and FragSink for large samples. Copies are
// taken on write per copy once alignment. failFragNum injects a failure at
// the given one based fragment number, or 0 for no failure. failFrame
// injects a failure for single writes.
type fragSendTestSink struct {
	mu          sync.Mutex
	guids       []WriterGUID
	singles     [][]byte
	frags       []fragSendRecord
	failFragNum uint32
	failFrame   error
}

// WriteFrame records one RTPS payload.
func (s *fragSendTestSink) WriteFrame(ctx context.Context, guid WriterGUID, payload []byte) error {
	err := ctx.Err()
	if err != nil {
		return err //nolint:wrapcheck // ctx error passes through unchanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failFrame != nil {
		return s.failFrame
	}
	s.guids = append(s.guids, guid)
	s.singles = append(s.singles, append([]byte(nil), payload...))
	return nil
}

// WriteFrag records one fragment in emission order.
func (s *fragSendTestSink) WriteFrag(ctx context.Context, guid WriterGUID, seq uint64, fragNum, total uint32, fragmentSize uint16, sampleSize uint32, data []byte) error {
	err := ctx.Err()
	if err != nil {
		return err //nolint:wrapcheck // ctx error passes through unchanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failFragNum != 0 && fragNum == s.failFragNum {
		return errors.New("dds_test: injected frag failure") //nolint:wrapcheck // test-only injected failure, nothing to wrap
	}
	s.guids = append(s.guids, guid)
	s.frags = append(s.frags, fragSendRecord{
		seq:          seq,
		fragNum:      fragNum,
		total:        total,
		fragmentSize: fragmentSize,
		sampleSize:   sampleSize,
		data:         append([]byte(nil), data...),
	})
	return nil
}

// singleCount returns the number of recorded single writes.
func (s *fragSendTestSink) singleCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.singles)
}

// fragCount returns the number of recorded fragments.
func (s *fragSendTestSink) fragCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.frags)
}

// fragAt returns a copy of the fragment record at index i.
func (s *fragSendTestSink) fragAt(t *testing.T, i int) fragSendRecord {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.frags) {
		t.Fatalf("frag index %d out of range %d", i, len(s.frags))
	}
	rec := s.frags[i]
	rec.data = append([]byte(nil), rec.data...)
	return rec
}

// fragSendWriterGUID returns a fixed GUID for deterministic tests.
func fragSendWriterGUID() WriterGUID {
	var guid WriterGUID
	for i := range guid {
		guid[i] = byte(0x40 + i)
	}
	return guid
}

// fragSendBuildPayload encodes one message and returns the RTPS payload.
func fragSendBuildPayload(t *testing.T, guid WriterGUID, seq uint64, user []byte) []byte {
	t.Helper()
	frame, err := encodeNextFrame(guid, seq, transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   user,
	}, "publish to")
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	payload, err := MarshalRTPSPayload(frame)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return payload
}

// TestFragSendSmallSingle checks small payloads use one WriteFrame.
func TestFragSendSmallSingle(t *testing.T) {
	t.Parallel()
	guid := fragSendWriterGUID()
	pub, err := NewPublisher(guid, fragSendDiscardWriter{})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	sink := &fragSendTestSink{}
	ctx := t.Context()
	user := []byte{1, 2, 3, 4}
	err = pub.PublishTo(ctx, sink, transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   user,
	})
	if err != nil {
		t.Fatalf("publish small: %v", err)
	}
	if sink.singleCount() != 1 {
		t.Fatalf("singles = %d, want 1", sink.singleCount())
	}
	if sink.fragCount() != 0 {
		t.Fatalf("frags = %d, want 0", sink.fragCount())
	}
	if pub.NextSequence() != 2 {
		t.Fatalf("next = %d, want 2", pub.NextSequence())
	}
}

// TestFragSendOrderedReassemble checks large payloads fragment in order and reassemble.
//
//nolint:gocognit,cyclop // test pins fragment, reassemble, take, and stale breadth in one run, splitting would obscure intent
func TestFragSendOrderedReassemble(t *testing.T) {
	t.Parallel()
	guid := fragSendWriterGUID()
	pub, err := NewPublisher(guid, fragSendDiscardWriter{})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	sink := &fragSendTestSink{}
	ctx := t.Context()
	user := bytes.Repeat([]byte{0xAB}, 80000)
	err = pub.PublishTo(ctx, sink, transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   user,
	})
	if err != nil {
		t.Fatalf("publish large: %v", err)
	}
	if sink.singleCount() != 0 {
		t.Fatalf("singles = %d, want 0 for fragmented sample", sink.singleCount())
	}
	total := sink.fragCount()
	if total < 2 {
		t.Fatalf("frags = %d, want at least 2", total)
	}
	wantPayload := fragSendBuildPayload(t, guid, 1, user)
	sampleSize := uint32(len(wantPayload)) //nolint:gosec // G115: test payload under 32MiB
	_, wantTotal, err := planFragSend(sampleSize)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if uint32(total) != wantTotal { //nolint:gosec // G115: test fragment count small
		t.Fatalf("frags = %d, want %d", total, wantTotal)
	}
	var writerID rtps.EntityID
	copy(writerID[:], guid[12:16])
	reasm := newFragReassembler(nil)
	var complete []byte
	var done bool
	for i := range total {
		rec := sink.fragAt(t, i)
		if rec.fragNum != uint32(i+1) {
			t.Fatalf("frag %d num = %d, want %d", i, rec.fragNum, i+1)
		}
		if rec.total != wantTotal {
			t.Fatalf("frag %d total = %d, want %d", i, rec.total, wantTotal)
		}
		if rec.seq != 1 {
			t.Fatalf("frag %d seq = %d, want 1", i, rec.seq)
		}
		if rec.fragmentSize != fragSendSize {
			t.Fatalf("frag %d size = %d, want %d", i, rec.fragmentSize, fragSendSize)
		}
		if rec.sampleSize != sampleSize {
			t.Fatalf("frag %d sample = %d, want %d", i, rec.sampleSize, sampleSize)
		}
		frag := rtps.DataFrag{
			ReaderID:              rtps.EntityUnknown,
			WriterID:              writerID,
			SequenceNumber:        rec.seq,
			FragmentStartingNum:   rec.fragNum,
			FragmentsInSubmessage: 1,
			FragmentSize:          rec.fragmentSize,
			SampleSize:            rec.sampleSize,
			Payload:               rec.data,
		}
		var fragErr error
		complete, done, fragErr = reasm.add(guid, frag)
		if fragErr != nil {
			t.Fatalf("reassemble frag %d: %v", i, fragErr)
		}
		if i < total-1 && done {
			t.Fatalf("done after frag %d of %d, want incomplete", i+1, total)
		}
	}
	if !done {
		t.Fatalf("reassembly incomplete after %d frags", total)
	}
	if !bytes.Equal(complete, wantPayload) {
		t.Fatalf("reassembled length %d differs from want %d", len(complete), len(wantPayload))
	}
	recv := NewDataReceiver()
	msg, err := recv.Take(complete)
	if err != nil {
		t.Fatalf("take reassembled: %v", err)
	}
	if !bytes.Equal(msg.Payload, user) {
		t.Fatalf("take payload length %d differs from want %d", len(msg.Payload), len(user))
	}
	if recv.LostCount() != 0 {
		t.Fatalf("lost after first reassembled = %d, want 0", recv.LostCount())
	}
	_, err = recv.Take(complete)
	if !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("duplicate take = %v, want ErrStaleSequence", err)
	}
	if recv.DroppedCount() != 1 {
		t.Fatalf("drops after duplicate = %d, want 1", recv.DroppedCount())
	}
	if pub.NextSequence() != 2 {
		t.Fatalf("next after fragmented publish = %d, want 2", pub.NextSequence())
	}
}

// TestFragSendNeedsFragSink checks large payloads without FragSink report ErrInvalidFrame.
func TestFragSendNeedsFragSink(t *testing.T) {
	t.Parallel()
	guid := fragSendWriterGUID()
	pub, err := NewPublisher(guid, fragSendDiscardWriter{})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	plain := &recordingFrameSinkForFragSend{}
	ctx := t.Context()
	user := bytes.Repeat([]byte{0xCD}, 80000)
	err = pub.PublishTo(ctx, plain, transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   user,
	})
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("large without frag sink = %v, want ErrInvalidFrame", err)
	}
	if pub.NextSequence() != 1 {
		t.Fatalf("next after failed frag = %d, want 1", pub.NextSequence())
	}
}

// recordingFrameSinkForFragSend implements only FrameSink without fragments.
type recordingFrameSinkForFragSend struct {
	mu       sync.Mutex
	countNum int
}

// WriteFrame records one payload without fragment support.
func (s *recordingFrameSinkForFragSend) WriteFrame(ctx context.Context, _ WriterGUID, _ []byte) error {
	err := ctx.Err()
	if err != nil {
		return err //nolint:wrapcheck // ctx error passes through unchanged
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.countNum++
	return nil
}

// TestFragSendFailureKeepsSequence checks a fragment failure keeps the sequence.
func TestFragSendFailureKeepsSequence(t *testing.T) {
	t.Parallel()
	guid := fragSendWriterGUID()
	pub, err := NewPublisher(guid, fragSendDiscardWriter{})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	sink := &fragSendTestSink{failFragNum: 2}
	ctx := t.Context()
	user := bytes.Repeat([]byte{0xEF}, 80000)
	err = pub.PublishTo(ctx, sink, transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   user,
	})
	if err == nil {
		t.Fatalf("failing frag publish succeeded, want error")
	}
	if pub.NextSequence() != 1 {
		t.Fatalf("next after failed frag = %d, want 1", pub.NextSequence())
	}
}

// TestFragSendBounds checks frag bounds for send planning.
//
//nolint:cyclop // test pins single, split, plan, and slice breadth in one run, splitting would obscure intent
func TestFragSendBounds(t *testing.T) {
	t.Parallel()
	if fragSingleMax != 65535-20 {
		t.Fatalf("single max = %d, want %d", fragSingleMax, 65535-20)
	}
	if fragSendSize == 0 {
		t.Fatalf("send size is 0, want nonzero")
	}
	if !fragNeedsSplit(uint32(fragSingleMax + 1)) {
		t.Fatalf("needs split above single max returned false, want true")
	}
	if fragNeedsSplit(uint32(fragSingleMax)) {
		t.Fatalf("needs split at single max returned true, want false")
	}
	sampleSize := uint32(102512)
	fragmentSize, total, err := planFragSend(sampleSize)
	if err != nil {
		t.Fatalf("plan 102512: %v", err)
	}
	if fragmentSize != fragSendSize {
		t.Fatalf("fragment size = %d, want %d", fragmentSize, fragSendSize)
	}
	wantTotal := (uint64(sampleSize) + uint64(fragmentSize) - 1) / uint64(fragmentSize)
	if uint64(total) != wantTotal {
		t.Fatalf("total = %d, want %d", total, wantTotal)
	}
	_, _, err = planFragSend(uint32(maxPayloadLen + 1))
	if err == nil {
		t.Fatalf("oversize plan succeeded, want error")
	}
	if !errors.Is(err, ErrDataFragDropped) {
		t.Fatalf("oversize plan = %v, want ErrDataFragDropped", err)
	}
	_, err = checkFragBounds(100, 0)
	if err == nil {
		t.Fatalf("zero fragment size succeeded, want error")
	}
	if !errors.Is(err, ErrDataFragDropped) {
		t.Fatalf("zero size = %v, want ErrDataFragDropped", err)
	}
	sample := bytes.Repeat([]byte{0x11}, 100)
	slice := fragSlice(sample, 1, 1, 100)
	if !bytes.Equal(slice, sample) {
		t.Fatalf("single slice differs from sample")
	}
	if got := fragSlice(sample, 2, 1, 100); got != nil {
		t.Fatalf("out of range slice is not nil")
	}
}

// TestFragSendEncodeDecode checks one encoded body decodes with matching geometry.
func TestFragSendEncodeDecode(t *testing.T) {
	t.Parallel()
	guid := fragSendWriterGUID()
	user := []byte{9, 8, 7, 6, 5}
	payload := fragSendBuildPayload(t, guid, 4, user)
	sampleSize := uint32(len(payload))   //nolint:gosec // G115: test payload small
	fragmentSize := uint16(len(payload)) //nolint:gosec // G115: test payload small
	var writerID rtps.EntityID
	copy(writerID[:], guid[12:16])
	body := encodeFragBody(rtps.EntityUnknown, writerID, 4, 1, fragmentSize, sampleSize, payload)
	// DATA_FRAG octetsToInlineQoS is 28 (4+4+8+4+2+2+4), not the DATA value 16.
	if len(body) < 32 {
		t.Fatalf("frag body length = %d, want at least 32", len(body))
	}
	if body[2] != 28 || body[3] != 0 {
		t.Fatalf("octetsToInlineQoS = %d, want 28", int(body[2])+int(body[3])*256)
	}
	decoded, err := rtps.DecodeDataFrag(body, rtps.FlagLittleEndian, nil)
	if err != nil {
		t.Fatalf("decode frag body: %v", err)
	}
	if decoded.SequenceNumber != 4 {
		t.Fatalf("seq = %d, want 4", decoded.SequenceNumber)
	}
	if decoded.FragmentStartingNum != 1 {
		t.Fatalf("start = %d, want 1", decoded.FragmentStartingNum)
	}
	if decoded.FragmentsInSubmessage != 1 {
		t.Fatalf("count = %d, want 1", decoded.FragmentsInSubmessage)
	}
	if decoded.FragmentSize != fragmentSize {
		t.Fatalf("frag size = %d, want %d", decoded.FragmentSize, fragmentSize)
	}
	if decoded.SampleSize != sampleSize {
		t.Fatalf("sample size = %d, want %d", decoded.SampleSize, sampleSize)
	}
	if !bytes.Equal(decoded.Payload, payload) {
		t.Fatalf("frag payload differs from sample")
	}
}
