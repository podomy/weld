// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later
//
//nolint:cyclop // test breadth
package unit_negative

// Negative and limits conformance tests for Weld first contact paths.
//
// Scope is pure Go via public dds, rtps, and types APIs with no Docker.
// Each test asserts that invalid input is rejected with an explicit error,
// that truncated input never panics, and that drop counters advance.
// Error handling uses explicit err assignments without inline if err forms.
// No em dashes appear in this file by policy.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// negClearDomainEnv isolates ROS_DOMAIN_ID for deterministic domain tests.
func negClearDomainEnv(t *testing.T) {
	t.Helper()
	t.Setenv(transports.DomainEnvVar, "")
}

// negMakeTestGUID builds a deterministic non zero participant GUID.
func negMakeTestGUID(fill byte) dds.GUID {
	tPrefix := dds.GUIDPrefix{}
	for i := range tPrefix {
		tPrefix[i] = fill
	}
	if tPrefix[0] == 0x00 {
		tPrefix[0] = 0x01
	}
	return dds.GUID{
		Prefix:   tPrefix,
		EntityID: [4]byte{0x00, 0x00, 0x01, 0xC1},
	}
}

// negMakeZeroGUID builds a GUID with a zero prefix for rejection tests.
func negMakeZeroGUID() dds.GUID {
	return dds.GUID{
		Prefix:   dds.GUIDPrefix{},
		EntityID: [4]byte{0x00, 0x00, 0x01, 0xC1},
	}
}

// negMakeWriterGUID builds a deterministic 16 byte writer GUID.
func negMakeWriterGUID(fill byte) dds.WriterGUID {
	var guid dds.WriterGUID
	for i := range guid {
		guid[i] = fill
	}
	if guid[0] == 0x00 {
		guid[0] = 0x01
	}
	return guid
}

// negTestWriter records DATA frames for publisher tests.
type negTestWriter struct {
	frames [][]byte
	fail   error
}

// WriteFrame records one frame or returns the injected failure.
func (w *negTestWriter) WriteFrame(frame []byte) error {
	if w.fail != nil {
		return w.fail
	}
	cp := append([]byte(nil), frame...)
	w.frames = append(w.frames, cp)
	return nil
}

// lastFrame returns a copy of the most recent frame.
func (w *negTestWriter) lastFrame(t *testing.T) []byte {
	t.Helper()
	if len(w.frames) == 0 {
		t.Fatalf("no frames recorded")
	}
	return append([]byte(nil), w.frames[len(w.frames)-1]...)
}

// frameAt returns a copy of the frame at index i.
func (w *negTestWriter) frameAt(t *testing.T, i int) []byte {
	t.Helper()
	if i < 0 || i >= len(w.frames) {
		t.Fatalf("frame index %d out of range %d", i, len(w.frames))
	}
	return append([]byte(nil), w.frames[i]...)
}

// negMustPublish publishes one message or fails the test.
func negMustPublish(t *testing.T, pub *dds.Publisher, msg transports.OutgoingMessage) {
	t.Helper()
	err := pub.Publish(msg)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// negMustMarshalAlive marshals an alive SPDP packet or fails the test.
//
//nolint:unparam // test helper keeps explicit domain param for clarity across domain rejection cases
func negMustMarshalAlive(t *testing.T, guid dds.GUID, domain uint32, seq uint64, lease time.Duration) []byte {
	t.Helper()
	data, err := dds.MarshalAlivePacket(guid, domain, seq, lease)
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	return data
}

// negMustMarshalDispose marshals a dispose SPDP packet or fails the test.
func negMustMarshalDispose(t *testing.T, guid dds.GUID, domain uint32, seq uint64) []byte {
	t.Helper()
	data, err := dds.MarshalDisposePacket(guid, domain, seq)
	if err != nil {
		t.Fatalf("marshal dispose: %v", err)
	}
	return data
}

// negCtxErr returns a wrapped context error for negative test fakes.
func negCtxErr(ctx context.Context, prefix string) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	return nil
}

// negFakeSender implements dds.Sender for nil and loop tests.
type negFakeSender struct {
	payloads [][]byte
	fail     error
}

// Send records one payload or returns the injected failure.
func (f *negFakeSender) Send(ctx context.Context, data []byte) error {
	err := negCtxErr(ctx, "neg fake sender")
	if err != nil {
		return err
	}
	if f.fail != nil {
		return f.fail
	}
	cp := append([]byte(nil), data...)
	f.payloads = append(f.payloads, cp)
	return nil
}

// negFakeReceiver implements dds.Receiver for loop tests.
type negFakeReceiver struct {
	payloads [][]byte
	pos      int
}

// Receive returns the next canned payload or DeadlineExceeded.
func (f *negFakeReceiver) Receive(ctx context.Context) ([]byte, error) {
	err := negCtxErr(ctx, "neg fake receiver")
	if err != nil {
		return nil, err
	}
	if f.pos >= len(f.payloads) {
		return nil, context.DeadlineExceeded
	}
	cp := append([]byte(nil), f.payloads[f.pos]...)
	f.pos++
	return cp, nil
}

// negSedpFakeSender implements dds.SEDPSender for nil argument tests.
type negSedpFakeSender struct{}

// SendSEDP accepts one sample without storing it.
func (s *negSedpFakeSender) SendSEDP(ctx context.Context, data []byte) error {
	err := negCtxErr(ctx, "neg sedp sender")
	if err != nil {
		return err
	}
	_ = data
	return nil
}

// negSedpFakeReceiver implements dds.SEDPReceiver for nil argument tests.
type negSedpFakeReceiver struct{}

// ReceiveSEDP blocks until context ends for nil argument tests.
func (r *negSedpFakeReceiver) ReceiveSEDP(ctx context.Context) ([]byte, error) {
	err := negCtxErr(ctx, "neg sedp receiver")
	if err != nil {
		return nil, err
	}
	return nil, context.DeadlineExceeded
}

// negFrameSinkStub implements dds.FrameSink for publish path tests.
type negFrameSinkStub struct {
	guids    []dds.WriterGUID
	payloads [][]byte
}

// WriteFrame records one RTPS payload.
func (s *negFrameSinkStub) WriteFrame(ctx context.Context, guid dds.WriterGUID, payload []byte) error {
	err := negCtxErr(ctx, "neg frame sink")
	if err != nil {
		return err
	}
	s.guids = append(s.guids, guid)
	s.payloads = append(s.payloads, append([]byte(nil), payload...))
	return nil
}

// negFrameSourceStub implements dds.FrameSource for take path tests.
type negFrameSourceStub struct {
	guids    []dds.WriterGUID
	payloads [][]byte
	idx      int
}

// ReadFrame returns the next queued payload.
func (s *negFrameSourceStub) ReadFrame(ctx context.Context) (dds.WriterGUID, uint64, []byte, error) {
	err := negCtxErr(ctx, "neg frame source")
	if err != nil {
		return dds.WriterGUID{}, 0, nil, err
	}
	if s.idx >= len(s.payloads) {
		return dds.WriterGUID{}, 0, nil, context.DeadlineExceeded
	}
	guid := s.guids[s.idx]
	cp := append([]byte(nil), s.payloads[s.idx]...)
	s.idx++
	return guid, 0, cp, nil
}

// negBuildValidRTPSMessage builds one header plus INFO_TS plus DATA datagram.
func negBuildValidRTPSMessage(t *testing.T) []byte {
	t.Helper()
	stamp := rtps.InfoTS{Valid: true, Seconds: 42, Fraction: 7}
	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: 1,
		Payload:        []byte{0x00, 0x01, 0x00, 0x00, 0x41},
	}
	sub, err := data.Submessage(true)
	if err != nil {
		t.Fatalf("data submessage: %v", err)
	}
	msg, err := rtps.EncodeMessage(rtps.DefaultHeader([12]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C}), []rtps.RawSubmessage{
		{ID: rtps.SubmessageInfoTS, Flags: stamp.Flags(true), Body: stamp.Encode(true)},
		sub,
	})
	if err != nil {
		t.Fatalf("encode message: %v", err)
	}
	return msg
}

// negBuildValidSEDPData builds one valid SEDP DATA sample for truncation tests.
func negBuildValidSEDPData(t *testing.T) []byte {
	t.Helper()
	var prefix dds.SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = byte(i + 1)
	}
	idx, err := dds.NewSEDPIndex(prefix, 0)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}
	ep, err := idx.DeclareWriter("/chatter", "std_msgs__msg__String", "", nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}
	data, err := dds.SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize sedp: %v", err)
	}
	return data
}

// negAssertNoPanic runs fn and fails if it panics.
func negAssertNoPanic(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r != nil {
			t.Fatalf("%s panicked: %v", name, r)
		}
	}()
	fn()
}

// TestNegativeBadDomainRejected checks domain ids above 101 are rejected.
func TestNegativeBadDomainRejected(t *testing.T) {
	negClearDomainEnv(t)

	_, err := transports.ResolveDomainID(102)
	if err == nil {
		t.Fatalf("caller domain 102 succeeded, want error")
	}

	cfg := transports.DomainConfig{DomainID: 200}
	_, err = cfg.Resolve()
	if err == nil {
		t.Fatalf("domain config 200 succeeded, want error")
	}

	sideCfg := dds.SidecarConfig{
		DomainID:    102,
		PeerAddress: "127.0.0.1",
		Range:       transports.DiscoveryRangeLocalhost,
	}
	_, err = dds.NewSidecar(sideCfg)
	if err == nil {
		t.Fatalf("sidecar domain 102 succeeded, want error")
	}

	_, err = dds.NewLocalParticipant(102, time.Second)
	if err == nil {
		t.Fatalf("local participant domain 102 succeeded, want error")
	}

	var sedpPrefix dds.SEDPGUIDPrefix
	for i := range sedpPrefix {
		sedpPrefix[i] = 0x01
	}
	_, err = dds.NewSEDPIndex(sedpPrefix, 102)
	if err == nil {
		t.Fatalf("sedp index domain 102 succeeded, want error")
	}
	if !errors.Is(err, dds.ErrSEDPInvalidDomain) {
		t.Fatalf("sedp domain error = %v, want ErrSEDPInvalidDomain", err)
	}

	guid := negMakeTestGUID(0x11)
	_, err = dds.MarshalAlivePacket(guid, 102, 1, time.Second)
	if err == nil {
		t.Fatalf("alive packet domain 102 succeeded, want error")
	}

	_, err = dds.MarshalDisposePacket(guid, 500, 1)
	if err == nil {
		t.Fatalf("dispose packet domain 500 succeeded, want error")
	}
}

// TestNegativeBadTopicRejected checks empty, leading slash DDS, and overlong topics.
func TestNegativeBadTopicRejected(t *testing.T) {
	negClearDomainEnv(t)

	_, err := dds.TopicName("", false)
	if err == nil {
		t.Fatalf("empty ros topic succeeded, want error")
	}

	_, err = dds.TopicName("/", false)
	if err == nil {
		t.Fatalf("slash only ros topic succeeded, want error")
	}

	_, err = dds.TopicName("///", false)
	if err == nil {
		t.Fatalf("slashes ros topic succeeded, want error")
	}

	_, _, err = dds.ParseTopicName("")
	if err == nil {
		t.Fatalf("empty dds topic succeeded, want error")
	}

	_, _, err = dds.ParseTopicName("/rt/chatter")
	if err == nil {
		t.Fatalf("leading slash dds topic succeeded, want error")
	}

	_, _, err = dds.ParseTopicName("rt/chatter/")
	if err == nil {
		t.Fatalf("trailing slash dds topic succeeded, want error")
	}

	// Bare ROS names normalize to rt prefix, so document that path still works.
	got, err := dds.TopicName("chatter", false)
	if err != nil {
		t.Fatalf("bare ros topic: %v", err)
	}
	if got != "rt/chatter" {
		t.Fatalf("bare ros topic = %q, want rt/chatter", got)
	}

	// Overlong ROS topic above 1024 must be rejected on publish.
	longTopic := "/" + strings.Repeat("a", 1025)
	sink := &negTestWriter{}
	pub := helpers.MustNewPublisher(t, negMakeWriterGUID(0x21), sink)
	err = pub.Publish(transports.OutgoingMessage{TopicName: longTopic, TypeName: "std_msgs__msg__String", Payload: []byte{0x01}})
	if err == nil {
		t.Fatalf("overlong topic publish succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidFrame) {
		t.Fatalf("overlong topic error = %v, want ErrInvalidFrame", err)
	}

	// Overlong DDS topic via direct parse path is covered by publish,
	// and empty publish is rejected.
	err = pub.Publish(transports.OutgoingMessage{TopicName: "", Payload: []byte{0x01}})
	if err == nil {
		t.Fatalf("empty topic publish succeeded, want error")
	}
}

// TestNegativeOverlongCDRStringSequenceRejected checks 1024 bounds on CDR values.
//
//nolint:cyclop // negative test asserts many sequential rejection cases in one run
func TestNegativeOverlongCDRStringSequenceRejected(t *testing.T) {
	t.Parallel()

	longStr := strings.Repeat("a", 1025)
	bound := types.StringBound{StringUpperBound: 1024}
	err := types.CheckStringLength(longStr, bound)
	if err == nil {
		t.Fatalf("string 1025 above bound 1024 succeeded, want error")
	}

	_, err = types.NewRosString(longStr, bound)
	if err == nil {
		t.Fatalf("new ros string 1025 succeeded, want error")
	}

	seqBound := types.SequenceBound{ArraySize: 1024, IsUpperBound: true}
	err = types.CheckSequenceLength(1025, seqBound)
	if err == nil {
		t.Fatalf("sequence 1025 above bound 1024 succeeded, want error")
	}

	err = types.ValidateStringForSerialize(longStr, bound, types.DescriptionField{})
	if err == nil {
		t.Fatalf("serialize string 1025 succeeded, want error")
	}

	err = types.ValidateSequenceForSerialize(1025, seqBound, types.DescriptionField{})
	if err == nil {
		t.Fatalf("serialize sequence 1025 succeeded, want error")
	}

	// Bounded decode must reject overlong wire values and count a drop.
	enc := types.NewEncoder(true)
	err = enc.WriteString(longStr)
	if err != nil {
		t.Fatalf("write long string: %v", err)
	}
	wire := enc.Bytes()
	dec, err := types.NewDecoder(wire)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	_, err = dec.ReadBoundedString(1024)
	if err == nil {
		t.Fatalf("bounded read 1025 succeeded, want error")
	}
	if dec.Dropped() == 0 {
		t.Fatalf("bounded string drop not counted")
	}

	enc2 := types.NewEncoder(true)
	err = enc2.WriteSequenceLength(1025)
	if err != nil {
		t.Fatalf("write long sequence length: %v", err)
	}
	wire2 := enc2.Bytes()
	dec2, err := types.NewDecoder(wire2)
	if err != nil {
		t.Fatalf("new decoder seq: %v", err)
	}
	_, err = dec2.ReadBoundedSequenceLength(1024)
	if err == nil {
		t.Fatalf("bounded sequence 1025 succeeded, want error")
	}
	if dec2.Dropped() == 0 {
		t.Fatalf("bounded sequence drop not counted")
	}

	// Datapath type names share the 1024 cap.
	sink := &negTestWriter{}
	pub := helpers.MustNewPublisher(t, negMakeWriterGUID(0x22), sink)
	longType := strings.Repeat("b", 1025)
	err = pub.Publish(transports.OutgoingMessage{TopicName: "/chatter", TypeName: longType, Payload: []byte{0x01}})
	if err == nil {
		t.Fatalf("overlong type publish succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidFrame) {
		t.Fatalf("overlong type error = %v, want ErrInvalidFrame", err)
	}
}

// TestNegativeTruncatedNeverPanicsAndCountsDrops checks truncated inputs drop without panic.
//
//nolint:gocognit,cyclop // negative test drives many truncation shapes through shared drop counters
func TestNegativeTruncatedNeverPanicsAndCountsDrops(t *testing.T) {
	t.Parallel()

	// RTPS header truncations.
	negAssertNoPanic(t, "rtps header", func() {
		full := negBuildValidRTPSMessage(t)
		for _, n := range []int{0, 1, 10, 19} {
			_, err := rtps.DecodeHeader(full[:n])
			if err == nil {
				t.Fatalf("header len %d succeeded, want error", n)
			}
		}
	})

	// RTPS message truncations count drops.
	negAssertNoPanic(t, "rtps message", func() {
		full := negBuildValidRTPSMessage(t)
		var drops rtps.Counter
		for _, n := range []int{0, 1, 10, 19, len(full) - 1} {
			_, _, err := rtps.DecodeMessage(full[:n], &drops)
			if err == nil {
				t.Fatalf("message len %d succeeded, want error", n)
			}
		}
		if drops.Count() == 0 {
			t.Fatalf("message drops not counted")
		}
	})

	// RTPS submessage and DATA truncations count drops.
	negAssertNoPanic(t, "rtps submessage", func() {
		full := negBuildValidRTPSMessage(t)
		var drops rtps.Counter
		_, subs, err := rtps.DecodeMessage(full, &drops)
		if err != nil {
			t.Fatalf("decode valid: %v", err)
		}
		if len(subs) == 0 {
			t.Fatalf("no submessages in valid message")
		}
		raw := subs[1]
		trunc := raw.Body[:10]
		_, err = rtps.DecodeData(trunc, raw.Flags, &drops)
		if err == nil {
			t.Fatalf("truncated DATA succeeded, want error")
		}
		if drops.Count() == 0 {
			t.Fatalf("DATA drops not counted")
		}
		badRaw := rtps.RawSubmessage{ID: raw.ID, Flags: raw.Flags, Body: trunc}
		_, err = rtps.DecodeSubmessage(badRaw, &drops)
		if err == nil {
			t.Fatalf("truncated submessage succeeded, want error")
		}
	})

	// Envelope truncations count drops.
	negAssertNoPanic(t, "envelope", func() {
		var m types.Metrics
		_, _, err := types.UnmarshalMessage(nil, &m)
		if err == nil {
			t.Fatalf("nil envelope succeeded, want error")
		}
		if m.Count() == 0 {
			t.Fatalf("nil envelope drop not counted")
		}
		wire := types.MarshalMessage([]byte{0x01, 0x02}, true)
		for _, n := range []int{0, 1, 2, 3} {
			var mm types.Metrics
			_, _, err := types.UnmarshalMessage(wire[:n], &mm)
			if err == nil {
				t.Fatalf("envelope len %d succeeded, want error", n)
			}
			if mm.Count() == 0 {
				t.Fatalf("envelope len %d drop not counted", n)
			}
		}
		dec, err := types.NewDecoder(wire[:2])
		if err == nil {
			t.Fatalf("decoder short header succeeded, want error")
		}
		if dec.Dropped() == 0 {
			t.Fatalf("decoder drop not counted")
		}
	})

	// Datapath truncated frames count drops.
	negAssertNoPanic(t, "datapath", func() {
		sink := &negTestWriter{}
		pub := helpers.MustNewPublisher(t, negMakeWriterGUID(0x23), sink)
		negMustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{0x09, 0x09, 0x09, 0x09}})
		full := sink.lastFrame(t)
		recv := dds.NewDataReceiver()
		for _, n := range []int{0, 1, 10, 35, 36, len(full) - 5, len(full) - 1} {
			_, err := recv.Take(full[:n])
			if err == nil {
				t.Fatalf("take len %d succeeded, want error", n)
			}
		}
		if recv.DroppedCount() == 0 {
			t.Fatalf("take drops not counted")
		}
	})

	// SPDP and SEDP truncated inputs return errors without panic.
	negAssertNoPanic(t, "spdp sedp", func() {
		_, err := dds.ParseAnnouncement([]byte{0x00, 0x01})
		if err == nil {
			t.Fatalf("short spdp succeeded, want error")
		}
		_, err = dds.ParseAnnouncement(nil)
		if err == nil {
			t.Fatalf("nil spdp succeeded, want error")
		}
		validSEDP := negBuildValidSEDPData(t)
		_, err = dds.ParseSEDPDATA(validSEDP[:10])
		if err == nil {
			t.Fatalf("short sedp succeeded, want error")
		}
		_, err = dds.ParseSEDPDATA(nil)
		if err == nil {
			t.Fatalf("nil sedp succeeded, want error")
		}
	})
}

// TestNegativeZeroGUIDPrefixRejected checks zero prefixes are rejected.
func TestNegativeZeroGUIDPrefixRejected(t *testing.T) {
	negClearDomainEnv(t)

	zero := negMakeZeroGUID()
	_, err := dds.MarshalAlivePacket(zero, 0, 1, time.Second)
	if err == nil {
		t.Fatalf("zero prefix alive succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidSPDP) {
		t.Fatalf("zero prefix alive error = %v, want ErrInvalidSPDP", err)
	}

	_, err = dds.MarshalDisposePacket(zero, 0, 1)
	if err == nil {
		t.Fatalf("zero prefix dispose succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidSPDP) {
		t.Fatalf("zero prefix dispose error = %v, want ErrInvalidSPDP", err)
	}
}

// TestNegativeZeroSequenceRejected checks zero sequences are rejected.
func TestNegativeZeroSequenceRejected(t *testing.T) {
	negClearDomainEnv(t)

	guid := negMakeTestGUID(0x31)
	_, err := dds.MarshalAlivePacket(guid, 0, 0, time.Second)
	if err == nil {
		t.Fatalf("zero sequence alive succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidSPDP) {
		t.Fatalf("zero sequence alive error = %v, want ErrInvalidSPDP", err)
	}

	_, err = dds.MarshalDisposePacket(guid, 0, 0)
	if err == nil {
		t.Fatalf("zero sequence dispose succeeded, want error")
	}

	// Datapath frame with zeroed sequence must be rejected on take.
	sink := &negTestWriter{}
	pub := helpers.MustNewPublisher(t, negMakeWriterGUID(0x32), sink)
	negMustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{0x01, 0x02}})
	full := sink.lastFrame(t)
	bad := append([]byte(nil), full...)
	for i := 16; i < 24; i++ {
		bad[i] = 0x00
	}
	recv := dds.NewDataReceiver()
	_, err = recv.Take(bad)
	if err == nil {
		t.Fatalf("zero sequence take succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidFrame) {
		t.Fatalf("zero sequence take error = %v, want ErrInvalidFrame", err)
	}
}

// TestNegativeStaleDuplicateSequenceDropped checks stale sequences drop with ErrStaleSequence.
func TestNegativeStaleDuplicateSequenceDropped(t *testing.T) {
	t.Parallel()

	sink := &negTestWriter{}
	pub := helpers.MustNewPublisher(t, negMakeWriterGUID(0x41), sink)
	negMustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{0x01, 0x01}})
	negMustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{0x02, 0x02}})
	negMustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{0x03, 0x03}})
	first := sink.frameAt(t, 0)
	second := sink.frameAt(t, 1)
	third := sink.frameAt(t, 2)

	recv := dds.NewDataReceiver()
	_, err := recv.Take(first)
	if err != nil {
		t.Fatalf("take first: %v", err)
	}
	_, err = recv.Take(third)
	if err != nil {
		t.Fatalf("take third with gap: %v", err)
	}
	if recv.LostCount() != 1 {
		t.Fatalf("lost = %d, want 1", recv.LostCount())
	}

	_, err = recv.Take(second)
	if err == nil {
		t.Fatalf("stale take succeeded, want error")
	}
	if !errors.Is(err, dds.ErrStaleSequence) {
		t.Fatalf("stale error = %v, want ErrStaleSequence", err)
	}

	_, err = recv.Take(third)
	if err == nil {
		t.Fatalf("duplicate take succeeded, want error")
	}
	if !errors.Is(err, dds.ErrStaleSequence) {
		t.Fatalf("duplicate error = %v, want ErrStaleSequence", err)
	}
	if recv.DroppedCount() == 0 {
		t.Fatalf("stale drops not counted")
	}
}

// TestNegativeDisposeRemovesAndStaleDisposeIgnored checks dispose removal and stale ignore.
func TestNegativeDisposeRemovesAndStaleDisposeIgnored(t *testing.T) {
	t.Parallel()

	table := dds.NewRemoteTable(nil)
	base := time.Unix(4000, 0).UTC()
	guid := negMakeTestGUID(0x51)

	alive := negMustMarshalAlive(t, guid, 0, 5, 10*time.Second)
	err := table.HandlePacket(alive, base)
	if err != nil {
		t.Fatalf("handle alive: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("len = %d, want 1", table.Len())
	}
	genAfterAlive := table.Generation()

	// Stale alive at same sequence must not bump generation.
	dupAlive := negMustMarshalAlive(t, guid, 0, 5, 10*time.Second)
	err = table.HandlePacket(dupAlive, base)
	if err != nil {
		t.Fatalf("handle dup alive: %v", err)
	}
	if table.Generation() != genAfterAlive {
		t.Fatalf("gen changed on duplicate alive")
	}

	// Stale dispose at same and older sequence must be ignored.
	staleBye := negMustMarshalDispose(t, guid, 0, 5)
	err = table.HandlePacket(staleBye, base)
	if err != nil {
		t.Fatalf("handle stale dispose: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("stale dispose removed entry, want len 1")
	}
	if table.Generation() != genAfterAlive {
		t.Fatalf("gen changed on stale dispose")
	}

	olderBye := negMustMarshalDispose(t, guid, 0, 3)
	err = table.HandlePacket(olderBye, base)
	if err != nil {
		t.Fatalf("handle older dispose: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("older dispose removed entry, want len 1")
	}

	// Newer dispose must remove.
	bye := negMustMarshalDispose(t, guid, 0, 6)
	err = table.HandlePacket(bye, base)
	if err != nil {
		t.Fatalf("handle dispose: %v", err)
	}
	if table.Len() != 0 {
		t.Fatalf("len = %d, want 0 after dispose", table.Len())
	}
}

// TestNegativeLeaseExpirySweepRemovesAtOrAfterExpiry checks sweep at and after expiry.
func TestNegativeLeaseExpirySweepRemovesAtOrAfterExpiry(t *testing.T) {
	t.Parallel()

	table := dds.NewRemoteTable(nil)
	base := time.Unix(5000, 0).UTC()
	guid := negMakeTestGUID(0x61)
	alive := negMustMarshalAlive(t, guid, 0, 1, time.Second)
	err := table.HandlePacket(alive, base)
	if err != nil {
		t.Fatalf("handle: %v", err)
	}

	removed := table.Sweep(base.Add(500 * time.Millisecond))
	if removed != 0 {
		t.Fatalf("early sweep removed = %d, want 0", removed)
	}
	if table.Len() != 1 {
		t.Fatalf("early sweep len = %d, want 1", table.Len())
	}

	// At expiry must remove.
	removed = table.Sweep(base.Add(time.Second))
	if removed != 1 {
		t.Fatalf("at expiry removed = %d, want 1", removed)
	}
	if table.Len() != 0 {
		t.Fatalf("at expiry len = %d, want 0", table.Len())
	}

	// After expiry on a fresh entry must also remove.
	alive2 := negMustMarshalAlive(t, guid, 0, 2, time.Second)
	err = table.HandlePacket(alive2, base)
	if err != nil {
		t.Fatalf("handle second: %v", err)
	}
	removed = table.Sweep(base.Add(2 * time.Second))
	if removed != 1 {
		t.Fatalf("after expiry removed = %d, want 1", removed)
	}
}

// TestNegativeNilContextSenderReceiverPeriodExplicitErrors checks nil and period errors.
//
//nolint:gocognit,cyclop // nil context test asserts explicit errors across sender, receiver and SEDP paths
func TestNegativeNilContextSenderReceiverPeriodExplicitErrors(t *testing.T) {
	negClearDomainEnv(t)

	part, err := dds.NewLocalParticipant(0, time.Second)
	if err != nil {
		t.Fatalf("new participant: %v", err)
	}
	sender := &negFakeSender{}
	receiver := &negFakeReceiver{payloads: [][]byte{{0x01}}}

	err = part.Announce(nil, sender) //nolint:staticcheck // SA1012: test explicitly passes nil context to verify ErrNilContext
	if err == nil {
		t.Fatalf("announce nil context succeeded, want error")
	}
	if !errors.Is(err, dds.ErrNilContext) {
		t.Fatalf("announce nil context error = %v, want ErrNilContext", err)
	}

	err = part.Announce(context.Background(), nil)
	if err == nil {
		t.Fatalf("announce nil sender succeeded, want error")
	}
	if !errors.Is(err, dds.ErrNilSender) {
		t.Fatalf("announce nil sender error = %v, want ErrNilSender", err)
	}

	err = part.AnnounceLoop(nil, sender, time.Second) //nolint:staticcheck // SA1012: test explicitly passes nil context to verify ErrNilContext
	if err == nil {
		t.Fatalf("announce loop nil context succeeded, want error")
	}
	if !errors.Is(err, dds.ErrNilContext) {
		t.Fatalf("announce loop nil context error = %v, want ErrNilContext", err)
	}

	err = part.AnnounceLoop(context.Background(), nil, time.Second)
	if err == nil {
		t.Fatalf("announce loop nil sender succeeded, want error")
	}
	if !errors.Is(err, dds.ErrNilSender) {
		t.Fatalf("announce loop nil sender error = %v, want ErrNilSender", err)
	}

	err = part.AnnounceLoop(context.Background(), sender, 0)
	if err == nil {
		t.Fatalf("announce loop zero period succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidPeriod) {
		t.Fatalf("announce loop period error = %v, want ErrInvalidPeriod", err)
	}

	table := dds.NewRemoteTable(nil)
	err = table.ListenLoop(nil, receiver, time.Second) //nolint:staticcheck // SA1012: test explicitly passes nil context to verify ErrNilContext
	if err == nil {
		t.Fatalf("listen loop nil context succeeded, want error")
	}
	if !errors.Is(err, dds.ErrNilContext) {
		t.Fatalf("listen nil context error = %v, want ErrNilContext", err)
	}

	err = table.ListenLoop(context.Background(), nil, time.Second)
	if err == nil {
		t.Fatalf("listen loop nil receiver succeeded, want error")
	}
	if !errors.Is(err, dds.ErrNilReceiver) {
		t.Fatalf("listen nil receiver error = %v, want ErrNilReceiver", err)
	}

	err = table.ListenLoop(context.Background(), receiver, 0)
	if err == nil {
		t.Fatalf("listen loop zero period succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidPeriod) {
		t.Fatalf("listen period error = %v, want ErrInvalidPeriod", err)
	}

	err = table.HandlePacket(negMustMarshalAlive(t, negMakeTestGUID(0x71), 0, 1, time.Second), time.Time{})
	if err == nil {
		t.Fatalf("handle zero time succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidTime) {
		t.Fatalf("zero time error = %v, want ErrInvalidTime", err)
	}

	var sedpPrefix dds.SEDPGUIDPrefix
	for i := range sedpPrefix {
		sedpPrefix[i] = 0x02
	}
	idx, err := dds.NewSEDPIndex(sedpPrefix, 0)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}

	err = idx.AnnounceLoop(nil, &negSedpFakeSender{}) //nolint:staticcheck // SA1012: test explicitly passes nil context to verify explicit error
	if err == nil {
		t.Fatalf("sedp announce nil context succeeded, want error")
	}

	err = idx.AnnounceLoop(context.Background(), nil)
	if err == nil {
		t.Fatalf("sedp announce nil sender succeeded, want error")
	}

	err = idx.ListenLoop(nil, &negSedpFakeReceiver{}) //nolint:staticcheck // SA1012: test explicitly passes nil context to verify explicit error
	if err == nil {
		t.Fatalf("sedp listen nil context succeeded, want error")
	}

	err = idx.ListenLoop(context.Background(), nil)
	if err == nil {
		t.Fatalf("sedp listen nil receiver succeeded, want error")
	}

	_, err = dds.NewPublisher(negMakeWriterGUID(0x72), nil)
	if err == nil {
		t.Fatalf("new publisher nil sink succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidFrame) {
		t.Fatalf("nil sink error = %v, want ErrInvalidFrame", err)
	}

	pub := helpers.MustNewPublisher(t, negMakeWriterGUID(0x73), &negTestWriter{})
	err = pub.PublishTo(context.Background(), nil, transports.OutgoingMessage{TopicName: "/chatter"})
	if err == nil {
		t.Fatalf("publish to nil sink succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidFrame) {
		t.Fatalf("publish nil sink error = %v, want ErrInvalidFrame", err)
	}

	_, err = dds.NewDataReceiver().TakeFrom(context.Background(), nil)
	if err == nil {
		t.Fatalf("take from nil source succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidFrame) {
		t.Fatalf("nil source error = %v, want ErrInvalidFrame", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err = pub.PublishTo(canceled, &negFrameSinkStub{}, transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publish error = %v, want context.Canceled", err)
	}

	_, err = dds.NewDataReceiver().TakeFrom(canceled, &negFrameSourceStub{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled take error = %v, want context.Canceled", err)
	}
}

// TestNegativeEmptyPayloadAllowedWithDummyByte checks empty payloads encode a dummy byte.
//
//nolint:cyclop // empty payload test walks encode, decode and counter branches in one run
func TestNegativeEmptyPayloadAllowedWithDummyByte(t *testing.T) {
	t.Parallel()

	wire := types.MarshalMessage(nil, true)
	if len(wire)%4 != 0 {
		t.Fatalf("empty wire length %d not padded", len(wire))
	}
	var m types.Metrics
	payload, le, err := types.UnmarshalMessage(wire, &m)
	if err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}
	if le != true {
		t.Fatalf("empty endian not LE")
	}
	if len(payload) == 0 || payload[0] != 0x00 {
		t.Fatalf("empty payload misses dummy byte: %v", payload)
	}

	enc := types.NewEncoder(true)
	wire2 := enc.Bytes()
	if len(wire2)%4 != 0 {
		t.Fatalf("encoder empty length %d not padded", len(wire2))
	}
	if len(wire2) < 5 || wire2[4] != 0x00 {
		t.Fatalf("encoder empty misses dummy byte: %v", wire2)
	}

	sink := &negTestWriter{}
	pub := helpers.MustNewPublisher(t, negMakeWriterGUID(0x81), sink)
	negMustPublish(t, pub, transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String"})
	frame := sink.lastFrame(t)
	recv := dds.NewDataReceiver()
	msg, err := recv.Take(frame)
	if err != nil {
		t.Fatalf("take empty: %v", err)
	}
	if len(msg.Payload) != 0 {
		t.Fatalf("empty take payload len = %d, want 0", len(msg.Payload))
	}
	if recv.DroppedCount() != 0 {
		t.Fatalf("empty take drops = %d, want 0", recv.DroppedCount())
	}

	// Single zero byte is a non empty sample and must round trip.
	sink2 := &negTestWriter{}
	pub2 := helpers.MustNewPublisher(t, negMakeWriterGUID(0x82), sink2)
	negMustPublish(t, pub2, transports.OutgoingMessage{TopicName: "/chatter", Payload: []byte{0x00}})
	recv2 := dds.NewDataReceiver()
	msg2, err := recv2.Take(sink2.lastFrame(t))
	if err != nil {
		t.Fatalf("take single zero: %v", err)
	}
	if len(msg2.Payload) != 1 || msg2.Payload[0] != 0x00 {
		t.Fatalf("single zero payload = %v, want [0]", msg2.Payload)
	}
}

// TestNegativeOversizePayloadAbove32MiBRejected checks payloads above 32MiB are rejected.
func TestNegativeOversizePayloadAbove32MiBRejected(t *testing.T) {
	t.Parallel()

	big := make([]byte, (32<<20)+1)
	for i := range big {
		big[i] = 0xAB
		if i > 1024 {
			break
		}
	}
	sink := &negTestWriter{}
	pub := helpers.MustNewPublisher(t, negMakeWriterGUID(0x91), sink)
	before := pub.NextSequence()
	err := pub.Publish(transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: big})
	if err == nil {
		t.Fatalf("oversize publish succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidFrame) {
		t.Fatalf("oversize error = %v, want ErrInvalidFrame", err)
	}
	if pub.NextSequence() != before {
		t.Fatalf("oversize publish advanced sequence")
	}

	// Bridge validation must also reject oversize frames.
	err = pub.PublishTo(context.Background(), &negFrameSinkStub{}, transports.OutgoingMessage{TopicName: "/chatter", Payload: big})
	if err == nil {
		t.Fatalf("oversize publish to succeeded, want error")
	}
	if !errors.Is(err, dds.ErrInvalidFrame) {
		t.Fatalf("oversize publish to error = %v, want ErrInvalidFrame", err)
	}

	// CDR checked writes must reject strings above the 32MiB cap.
	enc := types.NewEncoder(true)
	err = enc.WriteStringChecked(strings.Repeat("x", (32<<20)+1))
	if err == nil {
		t.Fatalf("oversize checked string succeeded, want error")
	}
}
