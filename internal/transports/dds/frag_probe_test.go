// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"testing"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// probeLargeSampleLen pins the 100KiB live sample scale.
const probeLargeSampleLen = 102400

// buildProbeLargeSample returns deterministic bytes of probe length.
func buildProbeLargeSample() []byte {
	data := make([]byte, probeLargeSampleLen)
	for i := range data {
		data[i] = byte(i % 256)
	}
	return data
}

// TestFragProbeLargeNeedsSplit checks 100KiB samples need bounded splits.
func TestFragProbeLargeNeedsSplit(t *testing.T) {
	t.Parallel()
	if !fragNeedsSplit(uint32(probeLargeSampleLen)) {
		t.Fatalf("needs split for %d returned false, want true", probeLargeSampleLen)
	}
	if fragNeedsSplit(uint32(fragSingleMax)) {
		t.Fatalf("needs split at single max %d returned true, want false", fragSingleMax)
	}
	sampleSize := uint32(probeLargeSampleLen)
	fragmentSize, total, err := planFragSend(sampleSize)
	if err != nil {
		t.Fatalf("plan %d: %v", sampleSize, err)
	}
	if fragmentSize != fragSendSize {
		t.Fatalf("fragment size = %d, want %d", fragmentSize, fragSendSize)
	}
	wantTotal := (uint64(sampleSize) + uint64(fragmentSize) - 1) / uint64(fragmentSize)
	if uint64(total) != wantTotal {
		t.Fatalf("total = %d, want %d", total, wantTotal)
	}
	if total < 2 {
		t.Fatalf("total = %d, want at least 2", total)
	}
	if uint64(total) > fragMaxCount {
		t.Fatalf("total = %d above max %d", total, fragMaxCount)
	}
	if uint64(sampleSize) > uint64(maxPayloadLen) {
		t.Fatalf("sample %d above cap %d", sampleSize, maxPayloadLen)
	}
}

// feedProbeFrag stores one fragment run in the reassembler.
func feedProbeFrag(t *testing.T, reasm *fragReassembler, guid WriterGUID, seq uint64, fragNum uint32, fragmentSize uint16, sampleSize uint32, data []byte) ([]byte, bool) {
	t.Helper()
	var writerID rtps.EntityID
	copy(writerID[:], guid[12:16])
	frag := rtps.DataFrag{
		ReaderID:              rtps.EntityUnknown,
		WriterID:              writerID,
		SequenceNumber:        seq,
		FragmentStartingNum:   fragNum,
		FragmentsInSubmessage: 1,
		FragmentSize:          fragmentSize,
		SampleSize:            sampleSize,
		Payload:               data,
	}
	complete, done, err := reasm.add(guid, frag)
	if err != nil {
		t.Fatalf("reassemble frag %d: %v", fragNum, err)
	}
	return complete, done
}

// TestFragProbeLargeOrderedReassemble checks ordered slices reassemble exactly.
func TestFragProbeLargeOrderedReassemble(t *testing.T) {
	t.Parallel()
	sample := buildProbeLargeSample()
	sampleSize := uint32(len(sample)) //nolint:gosec // G115: probe length under 32MiB cap
	fragmentSize, total, err := planFragSend(sampleSize)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	var guid WriterGUID
	for i := range guid {
		guid[i] = byte(0x50 + i)
	}
	reasm := newFragReassembler(nil)
	var complete []byte
	var done bool
	for fragNum := uint32(1); fragNum <= total; fragNum++ {
		complete, done = feedProbeFrag(t, reasm, guid, 1, fragNum, fragmentSize, sampleSize, fragSlice(sample, fragNum, total, fragmentSize))
		if fragNum < total && done {
			t.Fatalf("done after frag %d of %d, want incomplete", fragNum, total)
		}
	}
	if !done {
		t.Fatalf("reassembly incomplete after %d frags", total)
	}
	if !bytes.Equal(complete, sample) {
		t.Fatalf("reassembled len %d differs from want %d", len(complete), len(sample))
	}
}

// TestFragProbeLargeOutOfOrderDuplicate checks reverse order plus duplicates.
func TestFragProbeLargeOutOfOrderDuplicate(t *testing.T) {
	t.Parallel()
	sample := buildProbeLargeSample()
	sampleSize := uint32(len(sample)) //nolint:gosec // G115: probe length under 32MiB cap
	fragmentSize, total, err := planFragSend(sampleSize)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	var guid WriterGUID
	for i := range guid {
		guid[i] = byte(0x60 + i)
	}
	reasm := newFragReassembler(nil)
	var complete []byte
	var done bool
	for fragNum := total; fragNum >= 1; fragNum-- {
		complete, done = feedProbeFrag(t, reasm, guid, 9, fragNum, fragmentSize, sampleSize, fragSlice(sample, fragNum, total, fragmentSize))
		if fragNum > 1 && done {
			t.Fatalf("done before first frag, have frag %d of %d", fragNum, total)
		}
		if fragNum == 1 {
			break
		}
	}
	if !done {
		t.Fatalf("reverse reassembly incomplete after %d frags", total)
	}
	if !bytes.Equal(complete, sample) {
		t.Fatalf("reverse reassembled len %d differs from want %d", len(complete), len(sample))
	}
	_, done = feedProbeFrag(t, reasm, guid, 9, 1, fragmentSize, sampleSize, fragSlice(sample, 1, total, fragmentSize))
	if done {
		t.Fatalf("duplicate frag completed new entry, want incomplete")
	}
}

// TestFragProbeEncodeDecodeGeometry checks frag bodies decode with matching geometry.
//
//nolint:gocognit // test pins two fragment geometries with full wire checks in one run, splitting would obscure intent
func TestFragProbeEncodeDecodeGeometry(t *testing.T) {
	t.Parallel()
	sample := buildProbeLargeSample()
	sampleSize := uint32(len(sample)) //nolint:gosec // G115: probe length under 32MiB cap
	fragmentSize, total, err := planFragSend(sampleSize)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	var writerID rtps.EntityID
	writerID[0] = 0x00
	writerID[1] = 0x00
	writerID[2] = 0x02
	writerID[3] = 0x03
	for _, fragNum := range []uint32{1, total} {
		payload := fragSlice(sample, fragNum, total, fragmentSize)
		body := encodeFragBody(rtps.EntityUnknown, writerID, 11, fragNum, fragmentSize, sampleSize, payload)
		decoded, err := rtps.DecodeDataFrag(body, rtps.FlagLittleEndian, nil)
		if err != nil {
			t.Fatalf("decode frag %d: %v", fragNum, err)
		}
		if decoded.SequenceNumber != 11 {
			t.Fatalf("frag %d seq = %d, want 11", fragNum, decoded.SequenceNumber)
		}
		if decoded.FragmentStartingNum != fragNum {
			t.Fatalf("frag %d start = %d, want %d", fragNum, decoded.FragmentStartingNum, fragNum)
		}
		if decoded.FragmentsInSubmessage != 1 {
			t.Fatalf("frag %d count = %d, want 1", fragNum, decoded.FragmentsInSubmessage)
		}
		if decoded.FragmentSize != fragmentSize {
			t.Fatalf("frag %d size = %d, want %d", fragNum, decoded.FragmentSize, fragmentSize)
		}
		if decoded.SampleSize != sampleSize {
			t.Fatalf("frag %d sample = %d, want %d", fragNum, decoded.SampleSize, sampleSize)
		}
		if !bytes.Equal(decoded.Payload, payload) {
			t.Fatalf("frag %d payload differs from slice", fragNum)
		}
	}
}
