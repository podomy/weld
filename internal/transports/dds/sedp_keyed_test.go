// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"testing"
)

// TestSEDPKeyedDeclareEntityKinds checks keyed declares mint with-key entities.
func TestSEDPKeyedDeclareEntityKinds(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xE1)

	writer, err := idx.DeclareKeyedWriter("/keyed", "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare keyed writer: %v", err)
	}

	if !EndpointKeyed(writer) {
		t.Fatalf("keyed writer entity %v not detected as keyed", writer.ID.Entity)
	}

	if writer.ID.Entity[3] != keyedUserWriterKind {
		t.Fatalf("keyed writer kind = %#02x, want %#02x", writer.ID.Entity[3], keyedUserWriterKind)
	}

	reader, err := idx.DeclareKeyedReader("/keyed", "std_msgs__msg__String", StringTypeHash, nil, true)
	if err != nil {
		t.Fatalf("declare keyed reader: %v", err)
	}

	if !EndpointKeyed(reader) {
		t.Fatalf("keyed reader entity %v not detected as keyed", reader.ID.Entity)
	}

	if reader.ID.Entity[3] != keyedUserReaderKind {
		t.Fatalf("keyed reader kind = %#02x, want %#02x", reader.ID.Entity[3], keyedUserReaderKind)
	}

	plain := mustSEDPWriter(t, idx, "/keyed", StringTypeHash)
	if EndpointKeyed(plain) {
		t.Fatalf("plain writer entity %v detected as keyed, want unkeyed", plain.ID.Entity)
	}

	if !SEDPEndpointsMatch(reader, writer) {
		t.Fatalf("keyed reader missed keyed writer, want match")
	}

	if SEDPEndpointsMatch(reader, plain) {
		t.Fatalf("keyed reader matched unkeyed writer, want loud mismatch")
	}

	plainReader := mustSEDPReader(t, idx, "/keyed", StringTypeHash)
	if SEDPEndpointsMatch(plainReader, writer) {
		t.Fatalf("unkeyed reader matched keyed writer, want loud mismatch")
	}
}

// TestSEDPKeyedDeclareAnnounceRoundTrip checks keyed endpoints serialize.
func TestSEDPKeyedDeclareAnnounceRoundTrip(t *testing.T) {
	t.Parallel()

	idx := mustSEDPIndexWithPrefix(t, 0xE2)

	writer, err := idx.DeclareKeyedWriter("/keyed", "std_msgs__msg__String", StringTypeHash, nil)
	if err != nil {
		t.Fatalf("declare keyed writer: %v", err)
	}

	raw, err := SerializeSEDPDATA(writer)
	if err != nil {
		t.Fatalf("serialize keyed writer: %v", err)
	}

	back, err := ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("parse keyed writer: %v", err)
	}

	if !EndpointKeyed(back) {
		t.Fatalf("round trip entity %v lost keyed kind", back.ID.Entity)
	}

	if back.TypeHash != StringTypeHash {
		t.Fatalf("round trip hash = %q, want %q", back.TypeHash, StringTypeHash)
	}
}
