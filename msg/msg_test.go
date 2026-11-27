// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package msg_test

import (
	"testing"

	"github.com/podomy/weld/msg"
)

// registerInt32 registers a minimal Int32 type and returns the registry.
func registerInt32(t *testing.T) *msg.Registry {
	t.Helper()
	r := msg.NewRegistry()
	desc := msg.IndividualTypeDescription{
		TypeName: "std_msgs/msg/Int32",
		Fields: []msg.Field{
			{Name: "data", Type: msg.FieldType{TypeID: msg.FieldTypeInt32}},
		},
	}
	err := r.RegisterType(desc)
	if err != nil {
		t.Fatalf("RegisterType failed: %v", err)
	}
	return r
}

// checkHashRoundTrip verifies hash string parse roundtrip and registry agreement.
func checkHashRoundTrip(t *testing.T, r *msg.Registry, full msg.TypeDescription) {
	t.Helper()
	h, err := msg.ComputeTypeHash(full)
	if err != nil {
		t.Fatalf("ComputeTypeHash failed: %v", err)
	}
	text := h.String()
	parsed, err := msg.ParseTypeHash(text)
	if err != nil {
		t.Fatalf("ParseTypeHash failed: %v", err)
	}
	if !parsed.Equal(h) {
		t.Fatalf("hash roundtrip mismatch: %q versus %q", parsed.String(), text)
	}
	byRegistry, err := r.ComputeHash("std_msgs/msg/Int32")
	if err != nil {
		t.Fatalf("Registry ComputeHash failed: %v", err)
	}
	if !byRegistry.Equal(h) {
		t.Fatalf("registry hash mismatch: %q versus %q", byRegistry.String(), text)
	}
}

// checkTypedRoundTrip verifies one XCDR1 encoder to decoder roundtrip.
func checkTypedRoundTrip(t *testing.T) {
	t.Helper()
	enc := msg.NewEncoder(true)
	enc.WriteUint32(42)
	err := enc.WriteString("hello")
	if err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}
	wire := enc.Bytes()
	dec, err := msg.NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	gotNum, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("ReadUint32 failed: %v", err)
	}
	if gotNum != 42 {
		t.Fatalf("uint32 mismatch: %d", gotNum)
	}
	gotStr, err := dec.ReadString()
	if err != nil {
		t.Fatalf("ReadString failed: %v", err)
	}
	if gotStr != "hello" {
		t.Fatalf("string mismatch: %q", gotStr)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
}

// checkEnvelopeRoundTrip verifies Marshal plus Unmarshal envelope roundtrip.
func checkEnvelopeRoundTrip(t *testing.T, raw []byte) {
	t.Helper()
	envWire := msg.MarshalMessage(raw, true)
	metrics := &msg.Metrics{}
	prefix, littleEndian, err := msg.UnmarshalMessage(envWire, metrics)
	if err != nil {
		t.Fatalf("UnmarshalMessage failed: %v", err)
	}
	if !littleEndian {
		t.Fatalf("UnmarshalMessage endian = false, want true")
	}
	if len(prefix) < len(raw) {
		t.Fatalf("UnmarshalMessage payload short: %d", len(prefix))
	}
	for i := range raw {
		if prefix[i] != raw[i] {
			t.Fatalf("envelope payload mismatch at %d", i)
		}
	}
}

// checkServiceRoundTrip verifies service envelope roundtrip with GUID and sequence.
func checkServiceRoundTrip(t *testing.T, raw []byte) {
	t.Helper()
	var guid [8]byte
	guid[0] = 7
	svcWire := msg.MarshalServiceMessage(raw, guid, 9, true)
	svcMetrics := &msg.Metrics{}
	svcPayload, svcGUID, svcSeq, svcEndian, err := msg.UnmarshalServiceMessage(svcWire, svcMetrics)
	if err != nil {
		t.Fatalf("UnmarshalServiceMessage failed: %v", err)
	}
	if !svcEndian {
		t.Fatalf("service endian = false, want true")
	}
	if svcGUID != guid {
		t.Fatalf("service guid mismatch: %v versus %v", svcGUID, guid)
	}
	if svcSeq != 9 {
		t.Fatalf("service seq mismatch: %d", svcSeq)
	}
	if len(svcPayload) == 0 {
		t.Fatalf("service payload empty")
	}
}

// checkBounds verifies the re-exported bound checks accept valid inputs.
func checkBounds(t *testing.T) {
	t.Helper()
	err := msg.CheckStringLength("hi", msg.StringBound{})
	if err != nil {
		t.Fatalf("CheckStringLength failed: %v", err)
	}
	err = msg.CheckSequenceLength(2, msg.SequenceBound{IsUpperBound: true})
	if err != nil {
		t.Fatalf("CheckSequenceLength failed: %v", err)
	}
	err = msg.CheckMapLength(2, msg.MapBound{})
	if err != nil {
		t.Fatalf("CheckMapLength failed: %v", err)
	}
	err = msg.ValidateStringForSerialize("hi", msg.StringBound{}, msg.DescriptionField{})
	if err != nil {
		t.Fatalf("ValidateStringForSerialize failed: %v", err)
	}
	err = msg.ValidateSequenceForSerialize(2, msg.SequenceBound{IsUpperBound: true}, msg.DescriptionField{})
	if err != nil {
		t.Fatalf("ValidateSequenceForSerialize failed: %v", err)
	}
}

// TestRegistryRegister covers registry register plus lookup plus closure.
func TestRegistryRegister(t *testing.T) {
	r := registerInt32(t)
	got, ok := r.LookupType("std_msgs/msg/Int32")
	if !ok {
		t.Fatal("LookupType missing after register")
	}
	if got.TypeName != "std_msgs/msg/Int32" {
		t.Fatalf("LookupType name mismatch: %q", got.TypeName)
	}
	full, err := r.TransitiveClosure("std_msgs/msg/Int32")
	if err != nil {
		t.Fatalf("TransitiveClosure failed: %v", err)
	}
	if full.Description.TypeName != "std_msgs/msg/Int32" {
		t.Fatalf("closure name mismatch: %q", full.Description.TypeName)
	}
}

// TestHashCompute covers type hash compute plus string parse plus registry hash.
func TestHashCompute(t *testing.T) {
	r := registerInt32(t)
	full, err := r.TransitiveClosure("std_msgs/msg/Int32")
	if err != nil {
		t.Fatalf("TransitiveClosure failed: %v", err)
	}
	checkHashRoundTrip(t, r, full)
}

// TestCDRRoundTrip covers XCDR1 typed roundtrip plus envelope plus bounds.
func TestCDRRoundTrip(t *testing.T) {
	checkTypedRoundTrip(t)
	raw := []byte{0x01, 0x02, 0x03}
	checkEnvelopeRoundTrip(t, raw)
	checkServiceRoundTrip(t, raw)
	checkBounds(t)
}
