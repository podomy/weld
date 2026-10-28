// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_datapath pins std_srvs Trigger, Empty, and SetBool service encodings.
//
// Services carry no topic payload, so topic gate checks are N/A: requests
// travel on rq request DDS topics and replies travel on rr reply DDS topics,
// never on the rt topic path. This file pins pure Go XCDR request and
// response round trips with no Docker, plus the ServiceEnvelopeSize rebase
// and the rq and rr topic kind mapping.
//
//nolint:dupl,gocognit // test breadth repeats similar wire checks across types
package unit_datapath

import (
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// extraStdSrvsGUID returns a fixed 8 byte service GUID for tests.
func extraStdSrvsGUID() [8]byte {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	return guid
}

// extraStdSrvsSeq is the fixed service sequence number for tests.
const extraStdSrvsSeq = uint64(7)

// extraStdSrvsNewEncoder returns an encoder with the service header written.
func extraStdSrvsNewEncoder(t *testing.T, le bool, guid [8]byte, seq uint64) *types.Encoder {
	t.Helper()
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		t.Fatalf("write service header: %v", err)
	}
	if enc.Len() != types.ServiceEnvelopeSize {
		t.Fatalf("encoder len after header = %d, want %d", enc.Len(), types.ServiceEnvelopeSize)
	}
	return enc
}

// extraStdSrvsNewDecoder returns a decoder positioned after the service header.
func extraStdSrvsNewDecoder(t *testing.T, wire []byte, guid [8]byte, seq uint64) *types.Decoder {
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

// extraStdSrvsFinish checks strict consumption with no drops.
func extraStdSrvsFinish(t *testing.T, dec *types.Decoder, label string) {
	t.Helper()
	err := dec.Finish()
	if err != nil {
		t.Fatalf("finish %s: %v", label, err)
	}
	if dec.Dropped() != 0 {
		t.Fatalf("decoder %s drops = %d, want 0", label, dec.Dropped())
	}
}

// extraStdSrvsEncodeEmpty encodes a service body with no user fields.
func extraStdSrvsEncodeEmpty(t *testing.T, le bool, guid [8]byte, seq uint64) []byte {
	t.Helper()
	enc := extraStdSrvsNewEncoder(t, le, guid, seq)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("empty service wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// extraStdSrvsDecodeEmpty decodes a service body with no user fields.
func extraStdSrvsDecodeEmpty(t *testing.T, wire []byte, guid [8]byte, seq uint64, label string) {
	t.Helper()
	dec := extraStdSrvsNewDecoder(t, wire, guid, seq)
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			t.Fatalf("read %s pad: %v", label, err)
		}
		if pad != 0x00 {
			t.Fatalf("read %s pad = 0x%02x, want zero", label, pad)
		}
	}
	extraStdSrvsFinish(t, dec, label)
}

// extraStdSrvsCheckEmpty round trips an empty service body in both orders.
func extraStdSrvsCheckEmpty(t *testing.T, guid [8]byte, seq uint64, label string) {
	t.Helper()
	for _, le := range []bool{true, false} {
		wire := extraStdSrvsEncodeEmpty(t, le, guid, seq)
		if le {
			if wire[1] != 0x01 {
				t.Fatalf("%s LE header flag = 0x%02x, want 0x01", label, wire[1])
			}
		} else {
			if wire[1] != 0x00 {
				t.Fatalf("%s BE header flag = 0x%02x, want 0x00", label, wire[1])
			}
		}
		extraStdSrvsDecodeEmpty(t, wire, guid, seq, label)
	}
}

// extraStdSrvsEncodeBoolRequest encodes a service request carrying one bool.
func extraStdSrvsEncodeBoolRequest(t *testing.T, le bool, guid [8]byte, seq uint64, data bool) []byte {
	t.Helper()
	enc := extraStdSrvsNewEncoder(t, le, guid, seq)
	enc.WriteBool(data)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("bool request wire length %d is not padded to 4", len(wire))
	}
	return wire
}

// extraStdSrvsDecodeBoolRequest decodes a service request carrying one bool.
func extraStdSrvsDecodeBoolRequest(t *testing.T, wire []byte, guid [8]byte, seq uint64, want bool, label string) {
	t.Helper()
	dec := extraStdSrvsNewDecoder(t, wire, guid, seq)
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
	extraStdSrvsFinish(t, dec, label)
}

// extraStdSrvsCheckBoolRequest round trips a bool request in both orders.
func extraStdSrvsCheckBoolRequest(t *testing.T, guid [8]byte, seq uint64, label string, data bool) {
	t.Helper()
	for _, le := range []bool{true, false} {
		wire := extraStdSrvsEncodeBoolRequest(t, le, guid, seq, data)
		extraStdSrvsDecodeBoolRequest(t, wire, guid, seq, data, label)
	}
}

// extraStdSrvsEncodeBoolStringResponse encodes a response with bool plus string.
func extraStdSrvsEncodeBoolStringResponse(t *testing.T, le bool, guid [8]byte, seq uint64, success bool, message string) []byte {
	t.Helper()
	enc := extraStdSrvsNewEncoder(t, le, guid, seq)
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

// extraStdSrvsDecodeBoolStringResponse decodes a response with bool plus string.
func extraStdSrvsDecodeBoolStringResponse(t *testing.T, wire []byte, guid [8]byte, seq uint64, wantSuccess bool, wantMessage, label string) {
	t.Helper()
	dec := extraStdSrvsNewDecoder(t, wire, guid, seq)
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
	extraStdSrvsFinish(t, dec, label)
}

// extraStdSrvsCheckBoolStringResponse round trips a bool plus string response.
func extraStdSrvsCheckBoolStringResponse(t *testing.T, guid [8]byte, seq uint64, label string, success bool, message string) {
	t.Helper()
	for _, le := range []bool{true, false} {
		wire := extraStdSrvsEncodeBoolStringResponse(t, le, guid, seq, success, message)
		extraStdSrvsDecodeBoolStringResponse(t, wire, guid, seq, success, message, label)
	}
}

// extraStdSrvsCheckServiceKind pins that a service maps to rq and rr topics.
func extraStdSrvsCheckServiceKind(t *testing.T, service string) {
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

// TestStdSrvsExtra pins Trigger, Empty, and SetBool XCDR round trips.
//
// Trigger and Empty requests and the Empty response carry no user fields and
// Encode a bool request body after the 20 byte service prefix. SetBool
// requests carry one bool and Trigger and SetBool responses carry bool plus
// string. All cases assert the ServiceEnvelopeSize rebase for both endian
// orders. Topic gate coverage is N/A because services use rq and rr topics,
// so the test pins the request kind as TopicKindRequest and the reply kind
// as TopicKindReply and asserts neither is plain Topic.
func TestStdSrvsExtra(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Empty", "SetBool", "Trigger"} {
		entry, ok := types.Lookup("std_srvs", name)
		if !ok {
			t.Fatalf("corpus misses std_srvs/%s, no shape to pin", name)
		}
		if entry.Kind != types.KindService {
			t.Fatalf("corpus std_srvs/%s kind = %q, want service", name, entry.Kind)
		}
	}

	guid := extraStdSrvsGUID()
	seq := extraStdSrvsSeq

	t.Run("trigger_request_empty", func(t *testing.T) {
		t.Parallel()
		extraStdSrvsCheckEmpty(t, guid, seq, "trigger request")
	})
	t.Run("trigger_response", func(t *testing.T) {
		t.Parallel()
		extraStdSrvsCheckBoolStringResponse(t, guid, seq, "trigger response", true, "trigger ok")
	})
	t.Run("empty_request", func(t *testing.T) {
		t.Parallel()
		extraStdSrvsCheckEmpty(t, guid, seq+1, "empty request")
	})
	t.Run("empty_response", func(t *testing.T) {
		t.Parallel()
		extraStdSrvsCheckEmpty(t, guid, seq+2, "empty response")
	})
	t.Run("setbool_request", func(t *testing.T) {
		t.Parallel()
		extraStdSrvsCheckBoolRequest(t, guid, seq+3, "setbool request", true)
		extraStdSrvsCheckBoolRequest(t, guid, seq+4, "setbool request false", false)
	})
	t.Run("setbool_response", func(t *testing.T) {
		t.Parallel()
		extraStdSrvsCheckBoolStringResponse(t, guid, seq+5, "setbool response", false, "setbool ok")
	})
	t.Run("service_envelope_rebase", func(t *testing.T) {
		t.Parallel()
		for _, le := range []bool{true, false} {
			enc := extraStdSrvsNewEncoder(t, le, guid, seq+6)
			if enc.Len() != types.ServiceEnvelopeSize {
				t.Fatalf("le=%v encoder len = %d, want %d", le, enc.Len(), types.ServiceEnvelopeSize)
			}
			wire := enc.Bytes()
			dec, err := types.NewDecoder(wire)
			if err != nil {
				t.Fatalf("le=%v new decoder: %v", le, err)
			}
			gotGUID, gotSeq, err := dec.ReadServiceHeader()
			if err != nil {
				t.Fatalf("le=%v read service header: %v", le, err)
			}
			if gotGUID != guid {
				t.Fatalf("le=%v guid = %v, want %v", le, gotGUID, guid)
			}
			if gotSeq != seq+6 {
				t.Fatalf("le=%v seq = %d, want %d", le, gotSeq, seq+6)
			}
			if dec.Offset() != types.ServiceEnvelopeSize {
				t.Fatalf("le=%v offset = %d, want %d", le, dec.Offset(), types.ServiceEnvelopeSize)
			}
			extraStdSrvsDecodeEmpty(t, wire, guid, seq+6, "rebase empty")
		}
	})
	t.Run("service_topic_kinds", func(t *testing.T) {
		t.Parallel()
		extraStdSrvsCheckServiceKind(t, "/trigger")
		extraStdSrvsCheckServiceKind(t, "/empty")
		extraStdSrvsCheckServiceKind(t, "/set_bool")
	})
}
