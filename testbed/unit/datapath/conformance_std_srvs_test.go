// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_datapath holds DATA path conformance tests.
//
// This file pins std_srvs XCDR service encodings with pure Go and no Docker.
// Trigger and Empty service requests as topic payloads are N/A: services
// travel on rq request and rr reply DDS topics, not on the rt topic path,
// so they are out of topic-gate scope. The test below asserts
// internal/types Encoder and Decoder round trips for the Trigger, Empty,
// and SetBool shapes recorded in the in-repo corpus, and fails loudly when a
// shape is missing or wrong there.
//
//nolint:dupl // live gate decoders repeat similar wire checks across types
package unit_datapath

import (
	"strings"
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// stdSrvsTestGUID returns a fixed 8 byte service GUID for tests.
func stdSrvsTestGUID() [8]byte {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	return guid
}

// stdSrvsTestSeq is the fixed service sequence number for tests.
const stdSrvsTestSeq = uint64(7)

// stdSrvsCheckCorpus returns the corpus entry for pkg and name or fails loudly.
func stdSrvsCheckCorpus(t *testing.T, pkg, name string) types.CorpusEntry {
	t.Helper()
	entry, ok := types.Lookup(pkg, name)
	if !ok {
		t.Fatalf("in-repo corpus misses %s/%s, no shape to pin", pkg, name)
	}
	if entry.Kind != types.KindService {
		t.Fatalf("corpus %s/%s kind %q is not service, no service shape to pin", pkg, name, entry.Kind)
	}
	return entry
}

// stdSrvsRequireSummary fails loudly when Summary misses a required fragment.
func stdSrvsRequireSummary(t *testing.T, entry types.CorpusEntry, fragments ...string) {
	t.Helper()
	summary := strings.ToLower(entry.Summary)
	for _, fragment := range fragments {
		if !strings.Contains(summary, strings.ToLower(fragment)) {
			t.Fatalf("in-repo corpus %s/%s Summary %q misses %q, shape not defined", entry.Package, entry.Name, entry.Summary, fragment)
		}
	}
}

// stdSrvsNewServiceEncoder returns an encoder with the service header written.
func stdSrvsNewServiceEncoder(t *testing.T, le bool, guid [8]byte, seq uint64) *types.Encoder {
	t.Helper()
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		t.Fatalf("write service header: %v", err)
	}
	return enc
}

// stdSrvsNewServiceDecoder returns a decoder positioned after the service header.
func stdSrvsNewServiceDecoder(t *testing.T, wire []byte, guid [8]byte, seq uint64) *types.Decoder {
	t.Helper()
	dec, err := types.NewDecoder(wire)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		t.Fatalf("read service header: %v", err)
	}
	if gotGUID != guid {
		t.Fatalf("service guid = %v, want %v", gotGUID, guid)
	}
	if gotSeq != seq {
		t.Fatalf("service seq = %d, want %d", gotSeq, seq)
	}
	if dec.Offset() != types.ServiceEnvelopeSize {
		t.Fatalf("service payload offset = %d, want %d", dec.Offset(), types.ServiceEnvelopeSize)
	}
	return dec
}

// stdSrvsFinish checks strict consumption with no drops.
func stdSrvsFinish(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	err := dec.Finish()
	if err != nil {
		t.Fatalf("finish %s: %v", label, err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d, want 0", label, dec.Dropped())
	}
}

// stdSrvsEncodeEmptyService encodes a service with no user fields.
func stdSrvsEncodeEmptyService(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := stdSrvsNewServiceEncoder(t, le, guid, seq)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("empty service wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// stdSrvsDecodeEmptyService decodes a service with no user fields.
func stdSrvsDecodeEmptyService(t *testing.T, wire []byte, guid [8]byte, seq uint64, label string) {
	t.Helper()
	dec := stdSrvsNewServiceDecoder(t, wire, guid, seq)
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("read %s pad: %v", label, err)
		}
		if pad != 0x00 {
			t.Fatalf("read %s pad = 0x%02x, want zero", label, pad)
		}
	}
	stdSrvsFinish(t, dec, label)
}

// stdSrvsCheckEmptyService round trips an empty service body in both orders.
func stdSrvsCheckEmptyService(t *testing.T, guid [8]byte, seq uint64, label string) {
	t.Helper()
	for _, le := range []bool{true, false} {
		wire := stdSrvsEncodeEmptyService(t, le, guid, seq)
		if le {
			if wire[1] != 0x01 {
				t.Fatalf("%s LE header flag = 0x%02x, want 0x01", label, wire[1])
			}
		} else {
			if wire[1] != 0x00 {
				t.Fatalf("%s BE header flag = 0x%02x, want 0x00", label, wire[1])
			}
		}
		stdSrvsDecodeEmptyService(t, wire, guid, seq, label)
	}
}

// stdSrvsEncodeBoolRequest encodes a service request carrying one bool.
func stdSrvsEncodeBoolRequest(t *testing.T, le bool, guid [8]byte, seq uint64, data bool) []byte {
	t.Helper()
	enc := stdSrvsNewServiceEncoder(t, le, guid, seq)
	enc.WriteBool(data)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("bool request wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// stdSrvsDecodeBoolRequest decodes a service request carrying one bool.
func stdSrvsDecodeBoolRequest(t *testing.T, wire []byte, guid [8]byte, seq uint64, want bool, label string) {
	t.Helper()
	dec := stdSrvsNewServiceDecoder(t, wire, guid, seq)
	got, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("read %s data: %v", label, err)
	}
	if got != want {
		t.Fatalf("read %s data = %v, want %v", label, got, want)
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("read %s pad: %v", label, err)
		}
		if pad != 0x00 {
			t.Fatalf("read %s pad = 0x%02x, want zero", label, pad)
		}
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d, want 0", label, dec.Dropped())
	}
	stdSrvsFinish(t, dec, label)
}

// stdSrvsCheckBoolRequest round trips a bool request in both orders.
func stdSrvsCheckBoolRequest(t *testing.T, guid [8]byte, seq uint64, label string, data bool) {
	t.Helper()
	for _, le := range []bool{true, false} {
		wire := stdSrvsEncodeBoolRequest(t, le, guid, seq, data)
		stdSrvsDecodeBoolRequest(t, wire, guid, seq, data, label)
	}
}

// stdSrvsEncodeBoolStringResponse encodes a response with bool plus string.
func stdSrvsEncodeBoolStringResponse(t *testing.T, le bool, guid [8]byte, seq uint64, success bool, message string) []byte {
	t.Helper()
	enc := stdSrvsNewServiceEncoder(t, le, guid, seq)
	enc.WriteBool(success)
	err := enc.WriteString(message)
	if err != nil {
		t.Fatalf("encode response message: %v", err)
	}
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("bool string response wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// stdSrvsDecodeBoolStringResponse decodes a response with bool plus string.
func stdSrvsDecodeBoolStringResponse(t *testing.T, wire []byte, guid [8]byte, seq uint64, wantSuccess bool, wantMessage, label string) {
	t.Helper()
	dec := stdSrvsNewServiceDecoder(t, wire, guid, seq)
	gotSuccess, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("read %s success: %v", label, err)
	}
	if gotSuccess != wantSuccess {
		t.Fatalf("read %s success = %v, want %v", label, gotSuccess, wantSuccess)
	}
	gotMessage, err := dec.ReadString()
	if err != nil {
		t.Fatalf("read %s message: %v", label, err)
	}
	if gotMessage != wantMessage {
		t.Fatalf("read %s message = %q, want %q", label, gotMessage, wantMessage)
	}
	stdSrvsFinish(t, dec, label)
}

// stdSrvsCheckBoolStringResponse round trips a bool plus string response in both orders.
func stdSrvsCheckBoolStringResponse(t *testing.T, guid [8]byte, seq uint64, label string, success bool, message string) {
	t.Helper()
	for _, le := range []bool{true, false} {
		wire := stdSrvsEncodeBoolStringResponse(t, le, guid, seq, success, message)
		stdSrvsDecodeBoolStringResponse(t, wire, guid, seq, success, message, label)
	}
}

// stdSrvsCheckServiceKind pins that services map to rq and rr, not to rt topics.
func stdSrvsCheckServiceKind(t *testing.T, service string) {
	t.Helper()
	requestTopic, err := dds.RequestTopic(service, false)
	if err != nil {
		t.Fatalf("request topic %q: %v", service, err)
	}
	replyTopic, err := dds.ReplyTopic(service, false)
	if err != nil {
		t.Fatalf("reply topic %q: %v", service, err)
	}
	_, requestKind, err := dds.ParseTopicName(requestTopic)
	if err != nil {
		t.Fatalf("parse request topic %q: %v", requestTopic, err)
	}
	if requestKind != dds.TopicKindRequest {
		t.Fatalf("request topic %q kind = %q, want %q", requestTopic, requestKind, dds.TopicKindRequest)
	}
	_, replyKind, err := dds.ParseTopicName(replyTopic)
	if err != nil {
		t.Fatalf("parse reply topic %q: %v", replyTopic, err)
	}
	if replyKind != dds.TopicKindReply {
		t.Fatalf("reply topic %q kind = %q, want %q", replyTopic, replyKind, dds.TopicKindReply)
	}
	if requestKind == dds.TopicKindTopic || replyKind == dds.TopicKindTopic {
		t.Fatalf("service topics classified as plain topic, want request and reply outside topic gate")
	}
}

// TestStdSrvs pins std_srvs XCDR service encodings without using topic payloads.
func TestStdSrvs(t *testing.T) {
	t.Parallel()
	trigger := stdSrvsCheckCorpus(t, "std_srvs", "Trigger")
	stdSrvsRequireSummary(t, trigger, "request empty", "bool success", "string message")
	empty := stdSrvsCheckCorpus(t, "std_srvs", "Empty")
	stdSrvsRequireSummary(t, empty, "request empty", "response empty")
	setBool := stdSrvsCheckCorpus(t, "std_srvs", "SetBool")
	stdSrvsRequireSummary(t, setBool, "request bool data", "bool success", "string message")

	stdSrvsCheckServiceKind(t, "trigger_like")

	guid := stdSrvsTestGUID()
	seq := stdSrvsTestSeq

	t.Run("trigger_request_empty", func(t *testing.T) {
		t.Parallel()
		stdSrvsCheckEmptyService(t, guid, seq, "trigger request")
	})
	t.Run("trigger_response", func(t *testing.T) {
		t.Parallel()
		stdSrvsCheckBoolStringResponse(t, guid, seq, "trigger response", true, "trigger ok")
	})
	t.Run("empty_request", func(t *testing.T) {
		t.Parallel()
		stdSrvsCheckEmptyService(t, guid, seq+1, "empty request")
	})
	t.Run("empty_response", func(t *testing.T) {
		t.Parallel()
		stdSrvsCheckEmptyService(t, guid, seq+2, "empty response")
	})
	t.Run("setbool_request", func(t *testing.T) {
		t.Parallel()
		stdSrvsCheckBoolRequest(t, guid, seq+3, "setbool request", true)
	})
	t.Run("setbool_response", func(t *testing.T) {
		t.Parallel()
		stdSrvsCheckBoolStringResponse(t, guid, seq+4, "setbool response", false, "setbool ok")
	})
}
