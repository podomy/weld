// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package bags

import (
	"bytes"
	"testing"

	"github.com/podomy/weld/internal/types"
)

// registerStringType registers std_msgs String plus its hash.
func registerStringType(t *testing.T, r *types.Registry) string {
	t.Helper()
	desc := types.IndividualTypeDescription{
		TypeName: "std_msgs/msg/String",
		Fields: []types.Field{
			{Name: "data", Type: types.FieldType{TypeID: types.FieldTypeString}},
		},
	}
	err := r.RegisterType(desc)
	if err != nil {
		t.Fatalf("RegisterType String failed: %v", err)
	}
	h, err := r.ComputeHash("std_msgs/msg/String")
	if err != nil {
		t.Fatalf("ComputeHash String failed: %v", err)
	}
	return h.String()
}

// registerHeaderType registers Time plus Header and returns the header hash.
func registerHeaderType(t *testing.T, r *types.Registry) string {
	t.Helper()
	timeDesc := types.IndividualTypeDescription{
		TypeName: "builtin_interfaces/msg/Time",
		Fields: []types.Field{
			{Name: "sec", Type: types.FieldType{TypeID: types.FieldTypeInt32}},
			{Name: "nanosec", Type: types.FieldType{TypeID: types.FieldTypeUInt32}},
		},
	}
	err := r.RegisterType(timeDesc)
	if err != nil {
		t.Fatalf("RegisterType Time failed: %v", err)
	}
	headerDesc := types.IndividualTypeDescription{
		TypeName: "std_msgs/msg/Header",
		Fields: []types.Field{
			{Name: "stamp", Type: types.FieldType{TypeID: types.FieldTypeNestedType, NestedTypeName: "builtin_interfaces/msg/Time"}},
			{Name: "frame_id", Type: types.FieldType{TypeID: types.FieldTypeString}},
		},
	}
	err = r.RegisterType(headerDesc)
	if err != nil {
		t.Fatalf("RegisterType Header failed: %v", err)
	}
	h, err := r.ComputeHash("std_msgs/msg/Header")
	if err != nil {
		t.Fatalf("ComputeHash Header failed: %v", err)
	}
	return h.String()
}

// encodeStringPayload encodes one std_msgs String payload.
func encodeStringPayload(t *testing.T, s string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := enc.WriteString(s)
	if err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}
	return enc.Bytes()
}

// encodeHeaderPayload encodes one Header payload with stamp plus frame id.
func encodeHeaderPayload(t *testing.T, sec int32, nanosec uint32, frame string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	enc.WriteInt32(sec)
	enc.WriteUint32(nanosec)
	err := enc.WriteString(frame)
	if err != nil {
		t.Fatalf("WriteString frame failed: %v", err)
	}
	return enc.Bytes()
}

// newStringBag builds a bag with one String topic plus two messages.
func newStringBag(t *testing.T, r *types.Registry, hash string) *Bag {
	t.Helper()
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = b.AddTopic(Topic{
		ID:         1,
		Name:       "/chatter",
		Type:       "std_msgs/msg/String",
		Format:     FormatCDR,
		OfferedQoS: []string{"reliable", "volatile"},
		TypeHash:   hash,
	})
	if err != nil {
		t.Fatalf("AddTopic failed: %v", err)
	}
	first := Message{
		Topic:     "/chatter",
		Data:      encodeStringPayload(t, "hello"),
		ReceiveNs: 200,
		SendNs:    190,
		Sequence:  2,
		Format:    FormatCDR,
	}
	err = b.Append(first, r)
	if err != nil {
		t.Fatalf("Append first failed: %v", err)
	}
	second := Message{
		Topic:     "/chatter",
		Data:      encodeStringPayload(t, "world"),
		ReceiveNs: 100,
		SendNs:    90,
		Sequence:  1,
		Format:    FormatCDR,
	}
	err = b.Append(second, r)
	if err != nil {
		t.Fatalf("Append second failed: %v", err)
	}
	return b
}

func TestNewStorageIDs(t *testing.T) {
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New sqlite3 failed: %v", err)
	}
	if b.Storage() != StorageSQLite3 {
		t.Fatalf("storage = %q, want sqlite3", b.Storage())
	}
	b, err = New(StorageMCAP)
	if err != nil {
		t.Fatalf("New mcap failed: %v", err)
	}
	if b.Storage() != StorageMCAP {
		t.Fatalf("storage = %q, want mcap", b.Storage())
	}
	b, err = New("")
	if err != nil {
		t.Fatalf("New empty failed: %v", err)
	}
	if b.Storage() != StorageSQLite3 {
		t.Fatalf("empty storage = %q, want sqlite3", b.Storage())
	}
	_, err = New("bogus")
	if err == nil {
		t.Fatal("bogus storage accepted, want rejection")
	}
}

func TestAddTopicValidation(t *testing.T) {
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = b.AddTopic(Topic{ID: 1, Name: "/a", Type: "std_msgs/msg/String", Format: FormatCDR})
	if err != nil {
		t.Fatalf("AddTopic valid failed: %v", err)
	}
	err = b.AddTopic(Topic{ID: 2, Name: "/a", Type: "std_msgs/msg/String", Format: FormatCDR})
	if err == nil {
		t.Fatal("duplicate name accepted, want rejection")
	}
	err = b.AddTopic(Topic{ID: 1, Name: "/b", Type: "std_msgs/msg/String", Format: FormatCDR})
	if err == nil {
		t.Fatal("duplicate id accepted, want rejection")
	}
	err = b.AddTopic(Topic{ID: 3, Name: "noslash", Type: "std_msgs/msg/String", Format: FormatCDR})
	if err == nil {
		t.Fatal("missing slash accepted, want rejection")
	}
	err = b.AddTopic(Topic{ID: 4, Name: "/c", Type: "std_msgs/msg/String", Format: "json"})
	if err == nil {
		t.Fatal("bad format accepted, want rejection")
	}
	err = b.AddTopic(Topic{ID: 5, Name: "/d", Type: "std_msgs/msg/String", Format: FormatCDR, TypeHash: "bogus"})
	if err == nil {
		t.Fatal("bad hash accepted, want rejection")
	}
	if b.Drops() == 0 {
		t.Fatal("drops stayed zero after rejected topics")
	}
}

func TestAppendUnknownTopic(t *testing.T) {
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	msg := Message{Topic: "/missing", Data: encodeStringPayload(t, "x"), Format: FormatCDR}
	err = b.Append(msg, nil)
	if err == nil {
		t.Fatal("unknown topic accepted, want rejection")
	}
	if b.Drops() == 0 {
		t.Fatal("drops stayed zero after unknown topic")
	}
}

func TestAppendSequenceZero(t *testing.T) {
	r := types.NewRegistry()
	hash := registerStringType(t, r)
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = b.AddTopic(Topic{ID: 7, Name: "/chatter", Type: "std_msgs/msg/String", Format: FormatCDR, TypeHash: hash})
	if err != nil {
		t.Fatalf("AddTopic failed: %v", err)
	}
	msg := Message{Topic: "/chatter", Data: encodeStringPayload(t, "hi"), Format: FormatCDR}
	err = b.Append(msg, r)
	if err != nil {
		t.Fatalf("Append sequence zero failed: %v", err)
	}
	got := b.OrderedMessages()
	if len(got) != 1 || got[0].Sequence != 0 {
		t.Fatalf("sequence default not preserved: %+v", got)
	}
}

func TestInfoAndOrdering(t *testing.T) {
	r := types.NewRegistry()
	hash := registerStringType(t, r)
	b := newStringBag(t, r, hash)
	info := b.Info()
	if info.TopicCount != 1 || info.MessageCount != 2 {
		t.Fatalf("info counts = %d/%d, want 1/2", info.TopicCount, info.MessageCount)
	}
	if info.StartNs != 100 || info.EndNs != 200 {
		t.Fatalf("info range = %d/%d, want 100/200", info.StartNs, info.EndNs)
	}
	if len(info.Topics) != 1 || info.Topics[0].Count != 2 {
		t.Fatalf("info topics mismatch: %+v", info.Topics)
	}
	msgs := b.OrderedMessages()
	if msgs[0].ReceiveNs != 100 || msgs[1].ReceiveNs != 200 {
		t.Fatalf("messages not in replay order: %d, %d", msgs[0].ReceiveNs, msgs[1].ReceiveNs)
	}
	if string(msgs[0].Data) == "" || bytes.Equal(msgs[0].Data, msgs[1].Data) {
		t.Fatal("payload bytes not preserved distinctly")
	}
}

func TestMarshalRoundTripByteIdentical(t *testing.T) {
	r := types.NewRegistry()
	hash := registerStringType(t, r)
	b := newStringBag(t, r, hash)
	first, err := b.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	var m Metrics
	parsed, err := Parse(first, &m)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	second, err := parsed.Marshal()
	if err != nil {
		t.Fatalf("re-marshal failed: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("round trip bytes differ: %d vs %d", len(first), len(second))
	}
	err = parsed.Verify(r)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}
	pinfo := parsed.Info()
	if pinfo.TopicCount != 1 || pinfo.MessageCount != 2 {
		t.Fatalf("parsed info = %+v", pinfo)
	}
}

// newDeterministicBag builds the two topic reorder fixture.
func newDeterministicBag(t *testing.T, r *types.Registry, hash string) *Bag {
	t.Helper()
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = b.AddTopic(Topic{ID: 2, Name: "/b", Type: "std_msgs/msg/String", Format: FormatCDR, TypeHash: hash})
	if err != nil {
		t.Fatalf("AddTopic /b failed: %v", err)
	}
	err = b.AddTopic(Topic{ID: 1, Name: "/a", Type: "std_msgs/msg/String", Format: FormatCDR, TypeHash: hash})
	if err != nil {
		t.Fatalf("AddTopic /a failed: %v", err)
	}
	err = b.Append(Message{Topic: "/b", Data: encodeStringPayload(t, "two"), ReceiveNs: 5, Format: FormatCDR}, r)
	if err != nil {
		t.Fatalf("Append /b failed: %v", err)
	}
	err = b.Append(Message{Topic: "/a", Data: encodeStringPayload(t, "one"), ReceiveNs: 5, Format: FormatCDR}, r)
	if err != nil {
		t.Fatalf("Append /a failed: %v", err)
	}
	return b
}

func TestDeterministicIteration(t *testing.T) {
	r := types.NewRegistry()
	hash := registerStringType(t, r)
	first, err := newDeterministicBag(t, r, hash).Marshal()
	if err != nil {
		t.Fatalf("Marshal first failed: %v", err)
	}
	second, err := newDeterministicBag(t, r, hash).Marshal()
	if err != nil {
		t.Fatalf("Marshal second failed: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("marshal order not deterministic")
	}
	var m Metrics
	parsed, err := Parse(first, &m)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}
	topics := parsed.Topics()
	if len(topics) != 2 || topics[0].Name != "/a" || topics[1].Name != "/b" {
		t.Fatalf("topics not sorted: %+v", topics)
	}
	msgs := parsed.OrderedMessages()
	if len(msgs) != 2 || msgs[0].Topic != "/a" || msgs[1].Topic != "/b" {
		t.Fatalf("messages not sorted by topic: %+v", msgs)
	}
}

func TestTruncatedInputBounds(t *testing.T) {
	r := types.NewRegistry()
	hash := registerStringType(t, r)
	b := newStringBag(t, r, hash)
	full, err := b.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	points := []int{0, 1, 4, 7, 8, 12, len(full) / 2, len(full) - 1}
	for _, n := range points {
		var m Metrics
		_, err = Parse(full[:n], &m)
		if err == nil {
			t.Fatalf("truncated %d accepted, want rejection", n)
		}
		if m.Count() == 0 {
			t.Fatalf("truncated %d recorded no drop", n)
		}
	}
	var m Metrics
	_, err = Parse(nil, &m)
	if err == nil {
		t.Fatal("nil input accepted, want rejection")
	}
	if m.Count() == 0 {
		t.Fatal("nil input recorded no drop")
	}
}

func TestCorruptMagicAndTrailing(t *testing.T) {
	r := types.NewRegistry()
	hash := registerStringType(t, r)
	b := newStringBag(t, r, hash)
	full, err := b.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	bad := bytes.Clone(full)
	bad[0] ^= 0xff
	var m Metrics
	_, err = Parse(bad, &m)
	if err == nil {
		t.Fatal("bad magic accepted, want rejection")
	}
	if m.Count() == 0 {
		t.Fatal("bad magic recorded no drop")
	}
	withTrailing := append(bytes.Clone(full), 0x00)
	var m2 Metrics
	_, err = Parse(withTrailing, &m2)
	if err == nil {
		t.Fatal("trailing bytes accepted, want rejection")
	}
	if m2.Count() == 0 {
		t.Fatal("trailing bytes recorded no drop")
	}
	var m3 Metrics
	_, err = Parse(full, &m3)
	if err != nil {
		t.Fatalf("valid parse failed: %v", err)
	}
	if m3.Count() != 0 {
		t.Fatalf("valid parse recorded %d drops", m3.Count())
	}
}

func TestEnvelopeValidation(t *testing.T) {
	r := types.NewRegistry()
	hash := registerStringType(t, r)
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = b.AddTopic(Topic{ID: 1, Name: "/chatter", Type: "std_msgs/msg/String", Format: FormatCDR, TypeHash: hash})
	if err != nil {
		t.Fatalf("AddTopic failed: %v", err)
	}
	good := encodeStringPayload(t, "ok")
	badHeader := bytes.Clone(good)
	badHeader[0] = 0xff
	err = b.Append(Message{Topic: "/chatter", Data: badHeader, Format: FormatCDR}, r)
	if err == nil {
		t.Fatal("bad header accepted, want rejection")
	}
	odd := bytes.Clone(good)
	odd = append(odd, 0x00)
	err = b.Append(Message{Topic: "/chatter", Data: odd, Format: FormatCDR}, r)
	if err == nil {
		t.Fatal("odd length accepted, want rejection")
	}
	err = b.Append(Message{Topic: "/chatter", Data: nil, Format: FormatCDR}, r)
	if err == nil {
		t.Fatal("empty data accepted, want rejection")
	}
}

func TestHashMismatch(t *testing.T) {
	r := types.NewRegistry()
	_ = registerStringType(t, r)
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	other := types.NewRegistry()
	otherHash := registerStringType(t, other)
	_ = otherHash
	wrong := "RIHS01_0000000000000000000000000000000000000000000000000000000000000000"
	err = b.AddTopic(Topic{ID: 1, Name: "/chatter", Type: "std_msgs/msg/String", Format: FormatCDR, TypeHash: wrong})
	if err != nil {
		t.Fatalf("AddTopic wrong hash failed: %v", err)
	}
	err = b.Append(Message{Topic: "/chatter", Data: encodeStringPayload(t, "x"), Format: FormatCDR}, r)
	if err == nil {
		t.Fatal("hash mismatch accepted, want rejection")
	}
}

func TestUnknownTypeFirstClass(t *testing.T) {
	r := types.NewRegistry()
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = b.AddTopic(Topic{ID: 1, Name: "/mystery", Type: "pkg/msg/Mystery", Format: FormatCDR})
	if err != nil {
		t.Fatalf("AddTopic unknown failed: %v", err)
	}
	err = b.Append(Message{Topic: "/mystery", Data: encodeStringPayload(t, "raw"), Format: FormatCDR}, r)
	if err != nil {
		t.Fatalf("Append unknown type failed: %v", err)
	}
	err = b.Verify(r)
	if err != nil {
		t.Fatalf("Verify unknown type failed: %v", err)
	}
	raw, err := b.Marshal()
	if err != nil {
		t.Fatalf("Marshal unknown failed: %v", err)
	}
	var m Metrics
	parsed, err := Parse(raw, &m)
	if err != nil {
		t.Fatalf("Parse unknown failed: %v", err)
	}
	err = parsed.Verify(r)
	if err != nil {
		t.Fatalf("re-verify unknown failed: %v", err)
	}
}

func TestNestedSchemaValidation(t *testing.T) {
	r := types.NewRegistry()
	hash := registerHeaderType(t, r)
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = b.AddTopic(Topic{ID: 1, Name: "/header", Type: "std_msgs/msg/Header", Format: FormatCDR, TypeHash: hash})
	if err != nil {
		t.Fatalf("AddTopic failed: %v", err)
	}
	good := encodeHeaderPayload(t, 1, 2, "map")
	err = b.Append(Message{Topic: "/header", Data: good, ReceiveNs: 10, Format: FormatCDR}, r)
	if err != nil {
		t.Fatalf("Append header failed: %v", err)
	}
	truncated := bytes.Clone(good)
	truncated = truncated[:len(truncated)-2]
	err = b.Append(Message{Topic: "/header", Data: truncated, ReceiveNs: 11, Format: FormatCDR}, r)
	if err == nil {
		t.Fatal("truncated nested payload accepted, want rejection")
	}
	withGarbage := append(bytes.Clone(good), 0x01, 0x02, 0x03, 0x04)
	err = b.Append(Message{Topic: "/header", Data: withGarbage, ReceiveNs: 12, Format: FormatCDR}, r)
	if err == nil {
		t.Fatal("trailing garbage accepted, want rejection")
	}
	err = b.Verify(r)
	if err != nil {
		t.Fatalf("Verify header failed: %v", err)
	}
}

func TestEmptyBagRoundTrip(t *testing.T) {
	b, err := New(StorageMCAP)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	raw, err := b.Marshal()
	if err != nil {
		t.Fatalf("Marshal empty failed: %v", err)
	}
	var m Metrics
	parsed, err := Parse(raw, &m)
	if err != nil {
		t.Fatalf("Parse empty failed: %v", err)
	}
	if parsed.Storage() != StorageMCAP {
		t.Fatalf("storage = %q, want mcap", parsed.Storage())
	}
	info := parsed.Info()
	if info.TopicCount != 0 || info.MessageCount != 0 {
		t.Fatalf("empty info = %+v", info)
	}
	again, err := parsed.Marshal()
	if err != nil {
		t.Fatalf("re-marshal empty failed: %v", err)
	}
	if !bytes.Equal(raw, again) {
		t.Fatal("empty round trip bytes differ")
	}
}

func TestHugeLengthPrefixNoPanic(t *testing.T) {
	b, err := New(StorageSQLite3)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	err = b.AddTopic(Topic{ID: 1, Name: "/chatter", Type: "std_msgs/msg/String", Format: FormatCDR})
	if err != nil {
		t.Fatalf("AddTopic failed: %v", err)
	}
	raw, err := b.Marshal()
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	bad := bytes.Clone(raw)
	if len(bad) < 20 {
		t.Fatalf("marshal too short: %d", len(bad))
	}
	for i := 8; i < 12 && i < len(bad); i++ {
		bad[i] = 0xff
	}
	var m Metrics
	_, err = Parse(bad, &m)
	if err == nil {
		t.Fatal("huge storage length accepted, want rejection")
	}
	if m.Count() == 0 {
		t.Fatal("huge prefix recorded no drop")
	}
}
