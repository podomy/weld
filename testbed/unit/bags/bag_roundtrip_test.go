// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_bags proves bag exchange fidelity with no live peers.
//
//nolint:cyclop // bags package holds sequential conformance assertions, high average reflects breadth
package unit_bags

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/bags"
	"github.com/podomy/weld/internal/types"
)

// bagStringTopic is the string topic name.
const bagStringTopic = "/chatter"

// bagNumericTopic is the numeric topic name.
const bagNumericTopic = "/counter"

// bagHeaderTopic is the nested header topic name.
const bagHeaderTopic = "/header"

// bagNumericType is the numeric test type name.
const bagNumericType = "example/msg/Numeric"

// bagRoundtripRegistry registers string plus numeric plus header types.
func bagRoundtripRegistry(t *testing.T) (*types.Registry, string, string, string) {
	t.Helper()
	reg := types.NewRegistry()
	stringDesc := types.IndividualTypeDescription{
		TypeName: "std_msgs/msg/String",
		Fields: []types.Field{
			{Name: "data", Type: types.FieldType{TypeID: types.FieldTypeString}},
		},
	}
	err := reg.RegisterType(stringDesc)
	if err != nil {
		t.Fatalf("register string type: %v", err)
	}
	stringHash, err := reg.ComputeHash("std_msgs/msg/String")
	if err != nil {
		t.Fatalf("hash string type: %v", err)
	}
	numericDesc := types.IndividualTypeDescription{
		TypeName: bagNumericType,
		Fields: []types.Field{
			{Name: "count", Type: types.FieldType{TypeID: types.FieldTypeInt32}},
			{Name: "ratio", Type: types.FieldType{TypeID: types.FieldTypeDouble}},
		},
	}
	err = reg.RegisterType(numericDesc)
	if err != nil {
		t.Fatalf("register numeric type: %v", err)
	}
	numericHash, err := reg.ComputeHash(bagNumericType)
	if err != nil {
		t.Fatalf("hash numeric type: %v", err)
	}
	timeDesc := types.IndividualTypeDescription{
		TypeName: "builtin_interfaces/msg/Time",
		Fields: []types.Field{
			{Name: "sec", Type: types.FieldType{TypeID: types.FieldTypeInt32}},
			{Name: "nanosec", Type: types.FieldType{TypeID: types.FieldTypeUInt32}},
		},
	}
	err = reg.RegisterType(timeDesc)
	if err != nil {
		t.Fatalf("register time type: %v", err)
	}
	headerDesc := types.IndividualTypeDescription{
		TypeName: "std_msgs/msg/Header",
		Fields: []types.Field{
			{Name: "stamp", Type: types.FieldType{TypeID: types.FieldTypeNestedType, NestedTypeName: "builtin_interfaces/msg/Time"}},
			{Name: "frame_id", Type: types.FieldType{TypeID: types.FieldTypeString}},
		},
	}
	err = reg.RegisterType(headerDesc)
	if err != nil {
		t.Fatalf("register header type: %v", err)
	}
	headerHash, err := reg.ComputeHash("std_msgs/msg/Header")
	if err != nil {
		t.Fatalf("hash header type: %v", err)
	}
	return reg, stringHash.String(), numericHash.String(), headerHash.String()
}

// bagRoundtripEncodeString encodes one string payload.
func bagRoundtripEncodeString(t *testing.T, s string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := enc.WriteString(s)
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	return enc.Bytes()
}

// bagRoundtripEncodeNumeric encodes one numeric payload.
func bagRoundtripEncodeNumeric(t *testing.T, count int32, ratio float64) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	enc.WriteInt32(count)
	enc.WriteFloat64(ratio)
	return enc.Bytes()
}

// bagRoundtripEncodeHeader encodes one nested header payload.
func bagRoundtripEncodeHeader(t *testing.T, sec int32, nanosec uint32, frame string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	enc.WriteInt32(sec)
	enc.WriteUint32(nanosec)
	err := enc.WriteString(frame)
	if err != nil {
		t.Fatalf("encode header frame: %v", err)
	}
	return enc.Bytes()
}

// bagRoundtripNewBag builds a three topic bag with out of order appends.
func bagRoundtripNewBag(t *testing.T, reg *types.Registry, stringHash, numericHash, headerHash string) *bags.Bag {
	t.Helper()
	b, err := bags.New(bags.StorageSQLite3)
	if err != nil {
		t.Fatalf("new bag: %v", err)
	}
	err = b.AddTopic(bags.Topic{ID: 1, Name: bagStringTopic, Type: "std_msgs/msg/String", Format: bags.FormatCDR, OfferedQoS: []string{"reliable"}, TypeHash: stringHash})
	if err != nil {
		t.Fatalf("add topic chatter: %v", err)
	}
	err = b.AddTopic(bags.Topic{ID: 2, Name: bagNumericTopic, Type: bagNumericType, Format: bags.FormatCDR, OfferedQoS: []string{"best_effort"}, TypeHash: numericHash})
	if err != nil {
		t.Fatalf("add topic counter: %v", err)
	}
	err = b.AddTopic(bags.Topic{ID: 3, Name: bagHeaderTopic, Type: "std_msgs/msg/Header", Format: bags.FormatCDR, OfferedQoS: []string{"reliable", "transient_local"}, TypeHash: headerHash})
	if err != nil {
		t.Fatalf("add topic header: %v", err)
	}
	err = b.Append(bags.Message{Topic: bagNumericTopic, Data: bagRoundtripEncodeNumeric(t, -7, 3.25), ReceiveNs: 30, SendNs: 29, Sequence: 3, Format: bags.FormatCDR}, reg)
	if err != nil {
		t.Fatalf("append numeric: %v", err)
	}
	err = b.Append(bags.Message{Topic: bagHeaderTopic, Data: bagRoundtripEncodeHeader(t, 7, 8, "map"), ReceiveNs: 20, SendNs: 19, Sequence: 2, Format: bags.FormatCDR}, reg)
	if err != nil {
		t.Fatalf("append header: %v", err)
	}
	err = b.Append(bags.Message{Topic: bagStringTopic, Data: bagRoundtripEncodeString(t, "hello"), ReceiveNs: 10, SendNs: 9, Sequence: 1, Format: bags.FormatCDR}, reg)
	if err != nil {
		t.Fatalf("append string: %v", err)
	}
	return b
}

// bagRoundtripCheckTopics requires identical topic records in name order.
func bagRoundtripCheckTopics(t *testing.T, got, want []bags.Topic) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("topic count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID != want[i].ID || got[i].Name != want[i].Name || got[i].Type != want[i].Type || got[i].Format != want[i].Format || got[i].TypeHash != want[i].TypeHash {
			t.Fatalf("topic %d = %+v, want %+v", i, got[i], want[i])
		}
		if len(got[i].OfferedQoS) != len(want[i].OfferedQoS) {
			t.Fatalf("topic %q qos = %v, want %v", want[i].Name, got[i].OfferedQoS, want[i].OfferedQoS)
		}
		for j := range want[i].OfferedQoS {
			if got[i].OfferedQoS[j] != want[i].OfferedQoS[j] {
				t.Fatalf("topic %q qos %d = %q, want %q", want[i].Name, j, got[i].OfferedQoS[j], want[i].OfferedQoS[j])
			}
		}
	}
}

// bagRoundtripCheckMessages requires identical ordering plus byte identical payloads.
func bagRoundtripCheckMessages(t *testing.T, got, want []bags.Message) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("message count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Topic != want[i].Topic {
			t.Fatalf("message %d topic = %q, want %q", i, got[i].Topic, want[i].Topic)
		}
		if got[i].ReceiveNs != want[i].ReceiveNs || got[i].SendNs != want[i].SendNs || got[i].Sequence != want[i].Sequence || got[i].Format != want[i].Format {
			t.Fatalf("message %d meta = %+v, want %+v", i, got[i], want[i])
		}
		if !bytes.Equal(got[i].Data, want[i].Data) {
			t.Fatalf("message %d on %q payload bytes differ: %d vs %d", i, want[i].Topic, len(got[i].Data), len(want[i].Data))
		}
	}
}

// bagRoundtripWriteTemp stages marshaled bytes in an isolated temp dir.
func bagRoundtripWriteTemp(t *testing.T, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "roundtrip.weld")
	err := os.WriteFile(path, data, 0o600)
	if err != nil {
		t.Fatalf("write temp bag: %v", err)
	}
	return path
}

// bagRoundtripReadTemp reads staged bytes back from the temp dir.
func bagRoundtripReadTemp(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // G304: test reads back its own temp dir file
	if err != nil {
		t.Fatalf("read temp bag: %v", err)
	}
	return data
}

// TestBagRoundtripFileFidelity pins byte identical payloads plus topics plus order.
func TestBagRoundtripFileFidelity(t *testing.T) {
	t.Parallel()
	reg, stringHash, numericHash, headerHash := bagRoundtripRegistry(t)
	b := bagRoundtripNewBag(t, reg, stringHash, numericHash, headerHash)
	wantTopics := b.Topics()
	wantMessages := b.OrderedMessages()
	if len(wantMessages) != 3 {
		t.Fatalf("fixture message count = %d, want 3", len(wantMessages))
	}
	if wantMessages[0].Topic != bagStringTopic || wantMessages[1].Topic != bagHeaderTopic || wantMessages[2].Topic != bagNumericTopic {
		t.Fatalf("fixture order = %q %q %q, want chatter header counter", wantMessages[0].Topic, wantMessages[1].Topic, wantMessages[2].Topic)
	}
	wire, err := b.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	path := bagRoundtripWriteTemp(t, wire)
	raw := bagRoundtripReadTemp(t, path)
	if !bytes.Equal(raw, wire) {
		t.Fatal("file bytes differ from marshal bytes")
	}
	var m bags.Metrics
	parsed, err := bags.Parse(raw, &m)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if m.Count() != 0 {
		t.Fatalf("valid parse drops = %d, want 0", m.Count())
	}
	err = parsed.Verify(reg)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	bagRoundtripCheckTopics(t, parsed.Topics(), wantTopics)
	bagRoundtripCheckMessages(t, parsed.OrderedMessages(), wantMessages)
	again, err := parsed.Marshal()
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if !bytes.Equal(again, wire) {
		t.Fatal("re-marshal bytes differ, exchange is not deterministic")
	}
}

// TestBagCorruptByteNamesTopic pins that one flipped payload byte fails loudly.
func TestBagCorruptByteNamesTopic(t *testing.T) {
	t.Parallel()
	reg, stringHash, numericHash, headerHash := bagRoundtripRegistry(t)
	b := bagRoundtripNewBag(t, reg, stringHash, numericHash, headerHash)
	wire, err := b.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	stringPayload := bagRoundtripEncodeString(t, "hello")
	off := bytes.Index(wire, stringPayload)
	if off < 0 {
		t.Fatal("string payload not found in wire, no offset to corrupt")
	}
	if len(stringPayload) < 8 {
		t.Fatalf("string payload length = %d, want at least 8", len(stringPayload))
	}
	bad := bytes.Clone(wire)
	bad[off+4] ^= 0xFF
	var m bags.Metrics
	parsed, err := bags.Parse(bad, &m)
	if err != nil {
		if !strings.Contains(err.Error(), bagStringTopic) {
			t.Fatalf("corrupt parse error misses topic %q: %v", bagStringTopic, err)
		}
		return
	}
	if len(parsed.OrderedMessages()) != 3 {
		t.Fatalf("corrupt parse message count = %d, want 3 without silent truncation", len(parsed.OrderedMessages()))
	}
	err = parsed.Verify(reg)
	if err == nil {
		t.Fatal("corrupt payload verified, want loud failure")
	}
	if !strings.Contains(err.Error(), bagStringTopic) {
		t.Fatalf("corrupt verify error misses topic %q: %v", bagStringTopic, err)
	}
}
