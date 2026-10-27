// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later
package types

import (
	"bytes"
	"testing"
)

// checkEnvelopeLEHeader verifies LE header and padding.
func checkEnvelopeLEHeader(t *testing.T, wire, payload []byte) {
	t.Helper()
	if len(wire)%4 != 0 {
		t.Fatalf("wire length %d not padded", len(wire))
	}
	if wire[0] != 0x00 || wire[1] != 0x01 || wire[2] != 0x00 || wire[3] != 0x00 {
		t.Fatalf("bad LE header %v", wire[:4])
	}
	if !bytes.Equal(wire[4:7], payload) {
		t.Fatalf("payload mismatch %v", wire[4:7])
	}
	for _, b := range wire[7:] {
		if b != 0x00 {
			t.Fatalf("tail byte 0x%02x want zero", b)
		}
	}
}

// checkEnvelopeLERoundTrip verifies LE unmarshal round trip.
func checkEnvelopeLERoundTrip(t *testing.T, wire, payload []byte) {
	t.Helper()
	var m Metrics
	got, le, err := UnmarshalMessage(wire, &m)
	if err != nil {
		t.Fatalf("UnmarshalMessage failed: %v", err)
	}
	if le != true {
		t.Fatalf("expected little endian")
	}
	if !bytes.Equal(got[:3], payload) {
		t.Fatalf("payload mismatch %v", got[:3])
	}
	if m.Drops != 0 {
		t.Fatalf("unexpected drops %d", m.Drops)
	}
}

// TestCdrEnvelopeRoundTripLE checks header plus payload LE wrapping.
func TestCdrEnvelopeRoundTripLE(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03}
	wire := MarshalMessage(payload, true)
	t.Run("header", func(t *testing.T) { checkEnvelopeLEHeader(t, wire, payload) })
	t.Run("roundtrip", func(t *testing.T) { checkEnvelopeLERoundTrip(t, wire, payload) })
}

// TestCdrEnvelopeRoundTripBE checks header plus payload BE wrapping.
func TestCdrEnvelopeRoundTripBE(t *testing.T) {
	payload := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	wire := MarshalMessage(payload, false)
	if wire[1] != 0x00 {
		t.Fatalf("expected BE flag got 0x%02x", wire[1])
	}
	var m Metrics
	got, le, err := UnmarshalMessage(wire, &m)
	if err != nil {
		t.Fatalf("UnmarshalMessage failed: %v", err)
	}
	if le != false {
		t.Fatalf("expected big endian")
	}
	if !bytes.Equal(got[:4], payload) {
		t.Fatalf("payload mismatch %v", got[:4])
	}
}

// TestCdrEnvelopeBadHeaderRejected checks nonzero and endian violations.
func TestCdrEnvelopeBadHeaderRejected(t *testing.T) {
	cases := [][]byte{
		{0x01, 0x01, 0x00, 0x00},
		{0x00, 0x02, 0x00, 0x00},
		{0x00, 0xFF, 0x00, 0x00},
		{0xCD, 0x00, 0x00, 0x00},
	}
	for i, c := range cases {
		var m Metrics
		_, _, err := UnmarshalMessage(c, &m)
		if err == nil {
			t.Fatalf("case %d: expected bad header error", i)
		}
		if m.Drops != 1 {
			t.Fatalf("case %d: expected 1 drop got %d", i, m.Drops)
		}
		_, err = NewDecoder(c)
		if err == nil {
			t.Fatalf("case %d: expected NewDecoder error", i)
		}
	}
}

// TestCdrEnvelopeTruncatedEmptyDrops checks drops without panic.
func TestCdrEnvelopeTruncatedEmptyDrops(t *testing.T) {
	var m Metrics
	_, _, err := UnmarshalMessage(nil, &m)
	if err == nil {
		t.Fatalf("expected error on nil buffer")
	}
	_, _, err = UnmarshalMessage([]byte{}, &m)
	if err == nil {
		t.Fatalf("expected error on empty buffer")
	}
	_, _, err = UnmarshalMessage([]byte{0x00, 0x01}, &m)
	if err == nil {
		t.Fatalf("expected error on 2 byte buffer")
	}
	if m.Drops != 3 {
		t.Fatalf("drops %d want 3", m.Drops)
	}
	payload := []byte{0x01, 0x02, 0x03, 0x04, 0x05}
	wire := MarshalMessage(payload, true)
	for i := range wire {
		checkEnvelopeTruncatedPrefix(t, wire, i)
	}
}

// checkEnvelopeTruncatedPrefix exercises one truncated prefix without panic.
func checkEnvelopeTruncatedPrefix(t *testing.T, wire []byte, i int) {
	t.Helper()
	func() {
		defer func() {
			r := recover()
			if r != nil {
				t.Fatalf("panic on trunc len %d: %v", i, r)
			}
		}()
		var mm Metrics
		if _, _, err := UnmarshalMessage(wire[:i], &mm); err != nil {
			t.Logf("truncated prefix %d: %v", i, err)
		}
		dec, err := NewDecoder(wire[:i])
		if err != nil {
			return
		}
		if _, err := dec.ReadUint32(); err != nil {
			t.Logf("truncated body %d: %v", i, err)
		}
		if err := dec.Finish(); err != nil {
			t.Logf("truncated finish %d: %v", i, err)
		}
	}()
}

// TestCdrServiceGuidSequence checks service GUID plus sequence round trip.
// checkServiceGuidUnmarshal verifies service unmarshal for one endian.
func checkServiceGuidUnmarshal(t *testing.T, wire []byte, guid [8]byte, seq uint64, le bool, payload []byte) {
	t.Helper()
	if len(wire) < ServiceEnvelopeSize {
		t.Fatalf("le=%v wire too short %d", le, len(wire))
	}
	if wire[0] != 0x00 {
		t.Fatalf("bad service header first byte")
	}
	var m Metrics
	got, gotGUID, gotSeq, gotLE, err := UnmarshalServiceMessage(wire, &m)
	if err != nil {
		t.Fatalf("UnmarshalServiceMessage failed: %v", err)
	}
	if gotLE != le {
		t.Fatalf("endian mismatch got %v want %v", gotLE, le)
	}
	if gotGUID != guid {
		t.Fatalf("guid mismatch %v want %v", gotGUID, guid)
	}
	if gotSeq != seq {
		t.Fatalf("seq mismatch %x want %x", gotSeq, seq)
	}
	if !bytes.Equal(got[:3], payload) {
		t.Fatalf("payload mismatch %v", got[:3])
	}
}

// checkServiceGuidDecoder verifies decoder and encoder round trip.
func checkServiceGuidDecoder(t *testing.T, wire []byte, guid [8]byte, seq uint64, le bool, payload []byte) {
	t.Helper()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	dGUID, dSeq, err := dec.ReadServiceHeader()
	if err != nil {
		t.Fatalf("ReadServiceHeader failed: %v", err)
	}
	if dGUID != guid || dSeq != seq {
		t.Fatalf("decoder service header mismatch")
	}
	if dec.Offset() != ServiceEnvelopeSize {
		t.Fatalf("le=%v user payload not rebased at 20, off %d", le, dec.Offset())
	}
	enc := NewEncoder(le)
	err = enc.WriteServiceHeader(guid, seq)
	if err != nil {
		t.Fatalf("le=%v WriteServiceHeader failed: %v", le, err)
	}
	for _, b := range payload {
		enc.WriteUint8(b)
	}
	wire2 := enc.Bytes()
	if len(wire2) != len(wire) {
		t.Fatalf("le=%v encoder wire %d want %d", le, len(wire2), len(wire))
	}
	if !bytes.Equal(wire2, wire) {
		t.Fatalf("le=%v encoder bytes differ", le)
	}
}

// checkServiceGuidEndian runs both checks for one endian.
func checkServiceGuidEndian(t *testing.T, guid [8]byte, le bool) {
	t.Helper()
	payload := []byte{0x0A, 0x0B, 0x0C}
	seq := uint64(0x0102030405060708)
	wire := MarshalServiceMessage(payload, guid, seq, le)
	checkServiceGuidUnmarshal(t, wire, guid, seq, le, payload)
	checkServiceGuidDecoder(t, wire, guid, seq, le, payload)
}

func TestCdrServiceGuidSequence(t *testing.T) {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	for _, le := range []bool{true, false} {
		checkServiceGuidEndian(t, guid, le)
	}
}

// TestCdrServiceEmptyPayload checks empty service stays empty for ROS.
func TestCdrServiceEmptyPayload(t *testing.T) {
	var guid [8]byte
	wire := MarshalServiceMessage(nil, guid, 9, true)
	if len(wire)%4 != 0 {
		t.Fatalf("wire length %d not padded", len(wire))
	}
	var m Metrics
	got, _, seq, _, err := UnmarshalServiceMessage(wire, &m)
	if err != nil {
		t.Fatalf("UnmarshalServiceMessage failed: %v", err)
	}
	if seq != 9 {
		t.Fatalf("seq %d want 9", seq)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty payload, got %v", got)
	}
}

// TestCdrServiceTruncatedDrops checks short service buffers drop cleanly.
func TestCdrServiceTruncatedDrops(t *testing.T) {
	var guid [8]byte
	wire := MarshalServiceMessage([]byte{0x01}, guid, 1, true)
	for _, n := range []int{0, 1, 4, 10, 19} {
		var m Metrics
		_, _, _, _, err := UnmarshalServiceMessage(wire[:n], &m)
		if err == nil {
			t.Fatalf("len %d: expected truncation error", n)
		}
		if m.Drops != 1 {
			t.Fatalf("len %d: drops %d want 1", n, m.Drops)
		}
	}
}

// TestCdrServiceEmptyEncoderEqualsMarshal checks one empty rule both paths.
// checkServiceEmptyWire verifies encoder equals marshal for one endian.
func checkServiceEmptyWire(t *testing.T, guid [8]byte, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteServiceHeader(guid, 42)
	if err != nil {
		t.Fatalf("le=%v WriteServiceHeader failed: %v", le, err)
	}
	encWire := enc.Bytes()
	marshalWire := MarshalServiceMessage(nil, guid, 42, le)
	if !bytes.Equal(encWire, marshalWire) {
		t.Fatalf("le=%v service empty mismatch enc %v marshal %v", le, encWire, marshalWire)
	}
	emptyEnc := NewEncoder(le)
	emptyWire := emptyEnc.Bytes()
	emptyMarshal := MarshalMessage(nil, le)
	if !bytes.Equal(emptyWire, emptyMarshal) {
		t.Fatalf("le=%v plain empty mismatch enc %v marshal %v", le, emptyWire, emptyMarshal)
	}
}

// checkServiceEmptyDecode verifies empty decode and pad for one endian.
func checkServiceEmptyDecode(t *testing.T, guid [8]byte, le bool) {
	t.Helper()
	enc := NewEncoder(le)
	err := enc.WriteServiceHeader(guid, 42)
	if err != nil {
		t.Fatalf("le=%v WriteServiceHeader failed: %v", le, err)
	}
	encWire := enc.Bytes()
	dec, err := NewDecoder(encWire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		t.Fatalf("ReadServiceHeader failed: %v", err)
	}
	if gotGUID != guid {
		t.Fatalf("guid mismatch")
	}
	if gotSeq != 42 {
		t.Fatalf("seq %d want 42", gotSeq)
	}
	if dec.Remaining() != 0 {
		t.Fatalf("empty service has %d trailing bytes, want 0", dec.Remaining())
	}
	if err := dec.Finish(); err != nil {
		t.Fatalf("empty service Finish failed: %v", err)
	}
}

// checkServiceEmptyPad verifies remaining pad bytes are zero.
//
//nolint:unused // helper kept to document empty service pad invariant alongside decode checks
func checkServiceEmptyPad(t *testing.T, dec *Decoder) {
	t.Helper()
	for dec.Remaining() > 0 {
		b, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("pad read failed: %v", err)
		}
		if b != 0x00 {
			t.Fatalf("pad byte 0x%02x want zero", b)
		}
	}
}

func TestCdrServiceEmptyEncoderEqualsMarshal(t *testing.T) {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 7)
	}
	for _, le := range []bool{true, false} {
		checkServiceEmptyWire(t, guid, le)
		checkServiceEmptyDecode(t, guid, le)
	}
}

// checkOpaquePlainFinish verifies plain trailing garbage needs Finish.
func checkOpaquePlainFinish(t *testing.T, dec *Decoder) {
	t.Helper()
	for i := range 4 {
		_, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("ReadUint8 %d failed: %v", i, err)
		}
	}
	err := dec.Finish()
	if err == nil {
		t.Fatalf("expected Finish drop on trailing garbage")
	}
	if dec.Dropped() == 0 {
		t.Fatalf("expected drop counter on trailing garbage")
	}
}

// checkOpaqueServiceFinish verifies service trailing garbage needs Finish.
func checkOpaqueServiceFinish(t *testing.T, dec *Decoder) {
	t.Helper()
	_, _, err := dec.ReadServiceHeader()
	if err != nil {
		t.Fatalf("ReadServiceHeader failed: %v", err)
	}
	_, err = dec.ReadUint8()
	if err != nil {
		t.Fatalf("ReadUint8 service failed: %v", err)
	}
	err = dec.Finish()
	if err == nil {
		t.Fatalf("expected Finish drop on service trailing garbage")
	}
	if dec.Dropped() == 0 {
		t.Fatalf("expected drop on service trailing garbage")
	}
}

// TestCdrOpaqueTrailingGarbage checks unvalidated prefix needs Finish.
func TestCdrOpaqueTrailingGarbage(t *testing.T) {
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	wire := MarshalMessage(payload, true)
	trailing := append(append([]byte{}, wire...), 0x09, 0x09, 0x09, 0x09)
	var m Metrics
	got, _, err := UnmarshalMessage(trailing, &m)
	if err != nil {
		t.Fatalf("UnmarshalMessage should accept trailing garbage as unvalidated view: %v", err)
	}
	if len(got) < len(payload) {
		t.Fatalf("opaque payload too short")
	}
	dec, err := NewDecoder(trailing)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	checkOpaquePlainFinish(t, dec)
	var guid [8]byte
	svc := MarshalServiceMessage([]byte{0x01}, guid, 1, true)
	svcTrailing := append(append([]byte{}, svc...), 0x07, 0x07)
	var m2 Metrics
	_, _, _, _, err = UnmarshalServiceMessage(svcTrailing, &m2)
	if err != nil {
		t.Fatalf("service unvalidated view should accept trailing: %v", err)
	}
	dec2, err := NewDecoder(svcTrailing)
	if err != nil {
		t.Fatalf("NewDecoder service failed: %v", err)
	}
	checkOpaqueServiceFinish(t, dec2)
}

// TestCdrNilMetrics checks nil Metrics never panics and counts zero.
func TestCdrNilMetrics(t *testing.T) {
	_, _, err := UnmarshalMessage(nil, nil)
	if err == nil {
		t.Fatalf("expected error on nil buffer with nil metrics")
	}
	var nilM *Metrics
	if nilM.Count() != 0 {
		t.Fatalf("nil Count want 0")
	}
	nilM.RecordDrop()
	if nilM.Count() != 0 {
		t.Fatalf("nil RecordDrop must stay zero")
	}
	_, _, err = UnmarshalMessage([]byte{0x01, 0x01, 0x00, 0x00}, nil)
	if err == nil {
		t.Fatalf("expected bad header error with nil metrics")
	}
	_, _, _, _, err = UnmarshalServiceMessage(nil, nil)
	if err == nil {
		t.Fatalf("expected error on nil service buffer")
	}
	_, _, _, _, err = UnmarshalServiceMessage([]byte{0x00, 0x01}, nil)
	if err == nil {
		t.Fatalf("expected error on short service buffer")
	}
}

// checkReservedBytes verifies one reserved header variant.
func checkReservedBytes(t *testing.T, reserved []byte) {
	t.Helper()
	wire := []byte{0x00, 0x01, reserved[0], reserved[1], 0xAA}
	le, err := ParseHeader(wire)
	if err != nil {
		t.Fatalf("ParseHeader rejected reserved %v: %v", reserved, err)
	}
	if le != true {
		t.Fatalf("expected LE for reserved %v", reserved)
	}
	checkReservedDecode(t, wire, reserved)
	checkReservedUnmarshal(t, wire)
}

// checkReservedDecode verifies decoder read for reserved wire.
func checkReservedDecode(t *testing.T, wire, reserved []byte) {
	t.Helper()
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder rejected reserved %v: %v", reserved, err)
	}
	v, err := dec.ReadUint8()
	if err != nil {
		t.Fatalf("ReadUint8 failed: %v", err)
	}
	if v != 0xAA {
		t.Fatalf("payload mismatch %x", v)
	}
}

// checkReservedUnmarshal verifies unmarshal view for reserved wire.
func checkReservedUnmarshal(t *testing.T, wire []byte) {
	t.Helper()
	var m Metrics
	got, gotLE, err := UnmarshalMessage(wire, &m)
	if err != nil {
		t.Fatalf("UnmarshalMessage rejected reserved: %v", err)
	}
	if gotLE != true {
		t.Fatalf("expected LE")
	}
	if len(got) == 0 || got[0] != 0xAA {
		t.Fatalf("opaque payload mismatch")
	}
}

// TestCdrReservedBytesAccepted checks reserved header bytes are ignored.
func TestCdrReservedBytesAccepted(t *testing.T) {
	for _, reserved := range [][]byte{{0x00, 0x00}, {0xFF, 0xFF}, {0x12, 0x34}} {
		checkReservedBytes(t, reserved)
	}
}
