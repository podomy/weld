// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bag_test

import (
	"testing"

	"github.com/podomy/weld/bag"
	"github.com/podomy/weld/msg"
)

// encodeStringPayload encodes one String payload with a valid XCDR1 envelope.
func encodeStringPayload(t *testing.T, s string) []byte {
	t.Helper()

	enc := msg.NewEncoder(true)
	err := enc.WriteString(s)
	if err != nil {
		t.Fatalf("write string: %v", err)
	}

	return enc.Bytes()
}

// mustNewBag returns an empty bag or fails the test.
func mustNewBag(t *testing.T) *bag.Bag {
	t.Helper()

	b, err := bag.New("sqlite3")
	if err != nil {
		t.Fatalf("new bag: %v", err)
	}

	return b
}

// mustAddChatter adds the chatter topic or fails the test.
func mustAddChatter(t *testing.T, b *bag.Bag) {
	t.Helper()

	err := b.AddTopic(bag.Topic{
		ID:     1,
		Name:   "/chatter",
		Type:   "std_msgs/msg/String",
		Format: "cdr",
	})
	if err != nil {
		t.Fatalf("add topic: %v", err)
	}
}

// mustAppendHello appends one hello message or fails the test.
func mustAppendHello(t *testing.T, b *bag.Bag) {
	t.Helper()

	err := b.Append(bag.Message{
		Topic:     "/chatter",
		Data:      encodeStringPayload(t, "hello"),
		ReceiveNs: 100,
		SendNs:    90,
		Sequence:  1,
		Format:    "cdr",
	}, nil)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
}

// mustMarshalBag marshals the bag or fails the test.
func mustMarshalBag(t *testing.T, b *bag.Bag) []byte {
	t.Helper()

	raw, err := bag.Marshal(b)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	return raw
}

// mustParseBag parses the buffer or fails the test.
func mustParseBag(t *testing.T, raw []byte) *bag.Bag {
	t.Helper()

	var m bag.Metrics
	parsed, err := bag.Parse(raw, &m)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	return parsed
}

// checkBagInfo verifies one topic plus one message in the parsed bag.
func checkBagInfo(t *testing.T, parsed *bag.Bag) {
	t.Helper()

	info := parsed.Info()
	if info.TopicCount != 1 || info.MessageCount != 1 {
		t.Fatalf("info = %+v, want 1 topic 1 message", info)
	}
}

// checkBagMessages verifies the ordered messages hold one chatter sample.
func checkBagMessages(t *testing.T, parsed *bag.Bag) {
	t.Helper()

	msgs := parsed.OrderedMessages()
	if len(msgs) != 1 || msgs[0].Topic != "/chatter" {
		t.Fatalf("messages = %+v, want one /chatter", msgs)
	}
}

// TestBagAddTopic checks topic registration.
func TestBagAddTopic(t *testing.T) {
	b := mustNewBag(t)
	mustAddChatter(t, b)
	topics := b.Topics()
	if len(topics) != 1 || topics[0].Name != "/chatter" {
		t.Fatalf("topics = %+v, want one /chatter", topics)
	}
}

// TestBagAppendMarshalParse checks append plus round trip.
func TestBagAppendMarshalParse(t *testing.T) {
	b := mustNewBag(t)
	mustAddChatter(t, b)
	mustAppendHello(t, b)
	raw := mustMarshalBag(t, b)
	parsed := mustParseBag(t, raw)
	checkBagInfo(t, parsed)
	checkBagMessages(t, parsed)
}
