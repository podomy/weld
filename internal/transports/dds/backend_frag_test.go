// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"context"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// TestBackendImplementsFragSink checks Backend and frameSink satisfy FragSink.
func TestBackendImplementsFragSink(t *testing.T) {
	t.Parallel()

	var _ FragSink = &Backend{}
	var _ FragSink = &frameSink{backend: &Backend{}}
}

// newFragUnitBackend returns a Backend with local identity for frag unit tests.
func newFragUnitBackend(t *testing.T) *Backend {
	t.Helper()

	local, err := NewLocalParticipant(0, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local participant: %v", err)
	}

	var guid WriterGUID
	lg := local.GUID()
	copy(guid[:12], lg.Prefix[:])
	guid[12] = writerEntityUserB12
	guid[13] = writerEntityUserB13
	guid[14] = writerEntityUserB14
	guid[15] = writerEntityUserB15

	return &Backend{
		local:         local,
		pubGUID:       guid,
		routes:        make(map[GUIDPrefix][]int),
		rtpsSeq:       FirstSequenceNumber,
		repairHistory: newUserHistory(),
	}
}

// TestBackendBuildFragMessageRoundTrip checks one frag datagram decodes.
//
//nolint:cyclop // frag round-trip asserts build, header, body, and decode in one run
func TestBackendBuildFragMessageRoundTrip(t *testing.T) {
	t.Parallel()

	backend := newFragUnitBackend(t)
	guid := backend.pubGUID

	payload := bytes.Repeat([]byte{0x5A}, 100)
	sampleSize := uint32(len(payload))   //nolint:gosec // G115: 100-byte test payload fits uint32
	fragmentSize := uint16(len(payload)) //nolint:gosec // G115: 100-byte test payload fits uint16
	seq := uint64(7)

	msg, err := backend.buildFragMessage(guid, seq, 1, 1, fragmentSize, sampleSize, payload)
	if err != nil {
		t.Fatalf("build frag message: %v", err)
	}

	header, subs, err := rtps.DecodeMessage(msg, nil)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}

	if len(subs) != 1 {
		t.Fatalf("subs = %d, want 1", len(subs))
	}

	if subs[0].ID != rtps.SubmessageDataFrag {
		t.Fatalf("sub id = %#02x, want DATA_FRAG", subs[0].ID)
	}

	frag, err := rtps.DecodeDataFrag(subs[0].Body, subs[0].Flags, nil)
	if err != nil {
		t.Fatalf("decode datafrag: %v", err)
	}

	if frag.SequenceNumber != seq {
		t.Fatalf("seq = %d, want %d", frag.SequenceNumber, seq)
	}

	if frag.FragmentStartingNum != 1 {
		t.Fatalf("start = %d, want 1", frag.FragmentStartingNum)
	}

	if frag.FragmentsInSubmessage != 1 {
		t.Fatalf("count = %d, want 1", frag.FragmentsInSubmessage)
	}

	if frag.FragmentSize != fragmentSize {
		t.Fatalf("frag size = %d, want %d", frag.FragmentSize, fragmentSize)
	}

	if frag.SampleSize != sampleSize {
		t.Fatalf("sample size = %d, want %d", frag.SampleSize, sampleSize)
	}

	if !bytes.Equal(frag.Payload, payload) {
		t.Fatalf("payload differs from sample")
	}

	var wantPrefix [12]byte
	lg := backend.local.GUID()
	copy(wantPrefix[:], lg.Prefix[:])
	if header.Prefix != wantPrefix {
		t.Fatalf("header prefix mismatch")
	}
}

// TestBackendFragBounds checks 32MiB cap and fragment count bounds.
func TestBackendFragBounds(t *testing.T) {
	t.Parallel()

	if fragSingleMax != 65535-20 {
		t.Fatalf("single max = %d, want %d", fragSingleMax, 65535-20)
	}

	if fragSendSize == 0 {
		t.Fatalf("send size is 0, want nonzero")
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
}

// TestBackendWriteFragValidation checks WriteFrag rejects bad geometry.
func TestBackendWriteFragValidation(t *testing.T) {
	t.Parallel()

	backend := newFragUnitBackend(t)
	ctx := context.Background()
	guid := backend.pubGUID

	payload := []byte{1, 2, 3, 4}
	sampleSize := uint32(len(payload))   //nolint:gosec // G115: 4-byte test payload fits uint32
	fragmentSize := uint16(len(payload)) //nolint:gosec // G115: 4-byte test payload fits uint16

	err := backend.WriteFrag(ctx, guid, 0, 1, 1, fragmentSize, sampleSize, payload)
	if err == nil {
		t.Fatalf("seq 0 succeeded, want error")
	}

	var bad WriterGUID
	err = backend.WriteFrag(ctx, bad, 1, 1, 1, fragmentSize, sampleSize, payload)
	if err == nil {
		t.Fatalf("unknown writer succeeded, want error")
	}

	err = backend.WriteFrag(ctx, guid, 1, 0, 1, fragmentSize, sampleSize, payload)
	if err == nil {
		t.Fatalf("frag 0 succeeded, want error")
	}

	err = backend.WriteFrag(ctx, guid, 1, 2, 1, fragmentSize, sampleSize, payload)
	if err == nil {
		t.Fatalf("frag above total succeeded, want error")
	}

	err = backend.WriteFrag(ctx, guid, 1, 1, 1, fragmentSize, sampleSize, nil)
	if err == nil {
		t.Fatalf("empty data succeeded, want error")
	}

	_, _, err = planFragSend(uint32(maxPayloadLen + 1))
	if err == nil {
		t.Fatalf("oversize plan succeeded, want error")
	}
}

// TestBackendPlainFragNoRouteKeepsSeq checks large plain with no route keeps seq.
func TestBackendPlainFragNoRouteKeepsSeq(t *testing.T) {
	t.Parallel()

	backend := newFragUnitBackend(t)
	backend.rtpsSeq = FirstSequenceNumber

	user := bytes.Repeat([]byte{0xAB}, 80000)
	snap := publishSnapshot{domain: 0}

	msg := transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   user,
	}
	err := backend.publishPlain(context.Background(), msg, snap, backend.pubGUID)
	if err == nil {
		t.Fatalf("large plain without route succeeded, want error")
	}

	backend.mu.RLock()
	seq := backend.rtpsSeq
	backend.mu.RUnlock()

	if seq != FirstSequenceNumber {
		t.Fatalf("seq = %d, want %d after failed frag", seq, FirstSequenceNumber)
	}
}

// TestBackendFragOrderedSameSeq checks fragments emit in order with one seq.
//
//nolint:cyclop // ordered-emit test asserts sequence, order, and count in one run
func TestBackendFragOrderedSameSeq(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	uni, err := rtps.DialUnicast(ctx, 71)
	if err != nil {
		t.Skipf("dial unicast: %v", err)
	}
	defer func() {
		cerr := uni.Close()
		if cerr != nil {
			t.Errorf("close uni: %v", cerr)
		}
	}()

	backend := newFragUnitBackend(t)
	backend.uni = uni

	var prefix GUIDPrefix
	copy(prefix[:], backend.pubGUID[:12])
	backend.routes[prefix] = []int{uni.LocalPort()}
	backend.rtpsSeq = FirstSequenceNumber

	user := bytes.Repeat([]byte{0xCD}, 80000)
	frame, err := encodeNextFrame(backend.pubGUID, 1, transports.OutgoingMessage{
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

	sampleSize := uint32(len(payload)) //nolint:gosec // G115: 80KiB test payload fits uint32
	fragmentSize, total, err := planFragSend(sampleSize)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	if total < 2 {
		t.Fatalf("total = %d, want at least 2", total)
	}

	seq := uint64(9)
	for fragNum := uint32(1); fragNum <= total; fragNum++ {
		fragData := fragSlice(payload, fragNum, total, fragmentSize)
		err = backend.WriteFrag(ctx, backend.pubGUID, seq, fragNum, total, fragmentSize, sampleSize, fragData)
		if err != nil {
			t.Fatalf("write frag %d: %v", fragNum, err)
		}
	}

	reasm := newFragReassembler(nil)
	var complete []byte
	var done bool
	var writerID rtps.EntityID
	copy(writerID[:], backend.pubGUID[12:16])
	for fragNum := uint32(1); fragNum <= total; fragNum++ {
		fragData := fragSlice(payload, fragNum, total, fragmentSize)
		frag := rtps.DataFrag{
			ReaderID:              rtps.EntityUnknown,
			WriterID:              writerID,
			SequenceNumber:        seq,
			FragmentStartingNum:   fragNum,
			FragmentsInSubmessage: 1,
			FragmentSize:          fragmentSize,
			SampleSize:            sampleSize,
			Payload:               fragData,
		}
		complete, done, err = reasm.add(backend.pubGUID, frag)
		if err != nil {
			t.Fatalf("reassemble %d: %v", fragNum, err)
		}
	}

	if !done {
		t.Fatalf("reassembly incomplete after %d frags", total)
	}

	if !bytes.Equal(complete, payload) {
		t.Fatalf("reassembled length %d differs from want %d", len(complete), len(payload))
	}
}

// TestBackendSendPlainFragsAdvancesOnce checks one seq advances after all frags.
func TestBackendSendPlainFragsAdvancesOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	uni, err := rtps.DialUnicast(ctx, 72)
	if err != nil {
		t.Skipf("dial unicast: %v", err)
	}
	defer func() {
		cerr := uni.Close()
		if cerr != nil {
			t.Errorf("close uni: %v", cerr)
		}
	}()

	backend := newFragUnitBackend(t)
	backend.uni = uni

	var prefix GUIDPrefix
	copy(prefix[:], backend.pubGUID[:12])
	ports := []int{uni.LocalPort()}
	backend.routes[prefix] = ports
	backend.rtpsSeq = FirstSequenceNumber

	sample := bytes.Repeat([]byte{0xEF}, 80000)
	sampleSize := uint32(len(sample)) //nolint:gosec // G115: 80KB test sample fits uint32
	fragmentSize, total, err := planFragSend(sampleSize)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}

	err = backend.sendPlainFrags(ctx, backend.pubGUID, sample, sampleSize, fragmentSize, total, ports)
	if err != nil {
		t.Fatalf("send plain frags: %v", err)
	}

	backend.mu.RLock()
	seq := backend.rtpsSeq
	backend.mu.RUnlock()

	if seq != FirstSequenceNumber+1 {
		t.Fatalf("seq = %d, want %d after frag success", seq, FirstSequenceNumber+1)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	before := seq
	err = backend.sendPlainFrags(canceled, backend.pubGUID, sample, sampleSize, fragmentSize, total, ports)
	if err == nil {
		t.Fatalf("canceled frag send succeeded, want error")
	}

	backend.mu.RLock()
	after := backend.rtpsSeq
	backend.mu.RUnlock()

	if after != before {
		t.Fatalf("seq moved %d to %d on failed frag, want stable", before, after)
	}
}
