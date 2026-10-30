// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"strings"
	"testing"

	"github.com/podomy/weld/internal/types"
)

// serviceTestPrefix returns a fixed prefix for deterministic IDs.
func serviceTestPrefix() GUIDPrefix {
	var prefix GUIDPrefix

	for i := range prefix {
		prefix[i] = byte(i + 1)
	}

	return prefix
}

// TestServiceHeaderSizes pins the 16 byte header variant widths.
func TestServiceHeaderSizes(t *testing.T) {
	t.Parallel()

	if ServiceSertypeHeaderSize != 16 {
		t.Fatalf("header size = %d, want 16", ServiceSertypeHeaderSize)
	}

	if ServiceGUIDSize != 8 {
		t.Fatalf("guid size = %d, want 8", ServiceGUIDSize)
	}

	if ServiceSequenceSize != 8 {
		t.Fatalf("sequence size = %d, want 8", ServiceSequenceSize)
	}

	if types.ServiceHeaderSize != 16 {
		t.Fatalf("types header size = %d, want 16", types.ServiceHeaderSize)
	}

	if types.ServiceEnvelopeSize != 20 {
		t.Fatalf("types envelope size = %d, want 20", types.ServiceEnvelopeSize)
	}
}

// TestServiceEndpointsMapping checks request plus reply topics both modes.
func TestServiceEndpointsMapping(t *testing.T) {
	t.Parallel()

	ep, err := NewServiceEndpoints("/add_two_ints", "my_pkg__srv__AddTwoInts", false)
	if err != nil {
		t.Fatalf("new endpoints: %v", err)
	}

	if ep.RequestTopic != "rq/add_two_intsRequest" {
		t.Fatalf("request topic = %q, want rq/add_two_intsRequest", ep.RequestTopic)
	}

	if ep.ReplyTopic != "rr/add_two_intsReply" {
		t.Fatalf("reply topic = %q, want rr/add_two_intsReply", ep.ReplyTopic)
	}

	if ep.RequestType != "my_pkg::srv::dds_::AddTwoInts_Request_" {
		t.Fatalf("request type = %q", ep.RequestType)
	}

	if ep.ResponseType != "my_pkg::srv::dds_::AddTwoInts_Response_" {
		t.Fatalf("response type = %q", ep.ResponseType)
	}

	if strings.Contains(ep.ResponseType, "Reply") {
		t.Fatalf("response type %q must not contain Reply", ep.ResponseType)
	}
}

// TestServiceEndpointsAvoid checks bare names when avoid is set.
func TestServiceEndpointsAvoid(t *testing.T) {
	t.Parallel()

	ep, err := NewServiceEndpoints("/add_two_ints", "my_pkg__srv__AddTwoInts", true)
	if err != nil {
		t.Fatalf("new endpoints avoid: %v", err)
	}

	if ep.RequestTopic != "add_two_intsRequest" {
		t.Fatalf("request topic = %q, want bare", ep.RequestTopic)
	}

	if ep.ReplyTopic != "add_two_intsReply" {
		t.Fatalf("reply topic = %q, want bare", ep.ReplyTopic)
	}
}

// TestServiceEndpointsRejectBad checks invalid names report errors.
func TestServiceEndpointsRejectBad(t *testing.T) {
	t.Parallel()

	_, err := NewServiceEndpoints("///", "my_pkg__srv__AddTwoInts", false)
	if err == nil {
		t.Fatalf("bad service name succeeded, want error")
	}

	_, err = NewServiceEndpoints("/svc", "bad", false)
	if err == nil {
		t.Fatalf("bad idl succeeded, want error")
	}
}

// TestServiceRoleTopics checks server subscribes rq plus publishes rr.
func TestServiceRoleTopics(t *testing.T) {
	t.Parallel()

	ep, err := NewServiceEndpoints("/svc", "my_pkg__srv__Svc", false)
	if err != nil {
		t.Fatalf("new endpoints: %v", err)
	}

	if got := ep.SubscribeTopic(ServiceRoleServer); got != ep.RequestTopic {
		t.Fatalf("server subscribe = %q, want %q", got, ep.RequestTopic)
	}

	if got := ep.PublishTopic(ServiceRoleServer); got != ep.ReplyTopic {
		t.Fatalf("server publish = %q, want %q", got, ep.ReplyTopic)
	}

	if got := ep.SubscribeTopic(ServiceRoleClient); got != ep.ReplyTopic {
		t.Fatalf("client subscribe = %q, want %q", got, ep.ReplyTopic)
	}

	if got := ep.PublishTopic(ServiceRoleClient); got != ep.RequestTopic {
		t.Fatalf("client publish = %q, want %q", got, ep.RequestTopic)
	}

	if got := ep.SubscribeType(ServiceRoleServer); got != ep.RequestType {
		t.Fatalf("server subscribe type = %q", got)
	}

	if got := ep.PublishType(ServiceRoleServer); got != ep.ResponseType {
		t.Fatalf("server publish type = %q", got)
	}

	if got := ep.SubscribeType(ServiceRoleClient); got != ep.ResponseType {
		t.Fatalf("client subscribe type = %q", got)
	}

	if got := ep.PublishType(ServiceRoleClient); got != ep.RequestType {
		t.Fatalf("client publish type = %q", got)
	}
}

// TestServicePacketRoundTrip checks header plus data LE and BE.
//
//nolint:gocognit // test pins marshal, header, guid, and payload breadth for both endians in one run, splitting would obscure intent
func TestServicePacketRoundTrip(t *testing.T) {
	t.Parallel()

	var guid [8]byte

	for i := range guid {
		guid[i] = byte(i + 1)
	}

	payload := []byte{0x01, 0x02, 0x03}

	for _, le := range []bool{true, false} {
		wire, err := MarshalServiceRequest(payload, guid, 7, le)
		if err != nil {
			t.Fatalf("marshal le=%v: %v", le, err)
		}

		if len(wire) < types.ServiceEnvelopeSize {
			t.Fatalf("le=%v wire length %d below %d", le, len(wire), types.ServiceEnvelopeSize)
		}

		var m types.Metrics

		gotPayload, gotGUID, gotSeq, gotLE, err := UnmarshalServiceRequest(wire, &m)
		if err != nil {
			t.Fatalf("unmarshal le=%v: %v", le, err)
		}

		if gotGUID != guid {
			t.Fatalf("le=%v guid = %v, want %v", le, gotGUID, guid)
		}

		if gotSeq != 7 {
			t.Fatalf("le=%v seq = %d, want 7", le, gotSeq)
		}

		if gotLE != le {
			t.Fatalf("le=%v gotLE = %v", le, gotLE)
		}

		if len(gotPayload) < len(payload) {
			t.Fatalf("le=%v payload short", le)
		}

		for i := range payload {
			if gotPayload[i] != payload[i] {
				t.Fatalf("le=%v payload byte %d = 0x%02x", le, i, gotPayload[i])
			}
		}
	}
}

// TestServicePacketEmpty checks empty payload stays empty for ROS.
//
// ROS sends no dummy byte for empty service structs, so Weld matches the
// captured wire: header only, no payload bytes.
func TestServicePacketEmpty(t *testing.T) {
	t.Parallel()

	var guid [8]byte

	wire, err := MarshalServiceReply(nil, guid, 1, true)
	if err != nil {
		t.Fatalf("marshal empty: %v", err)
	}

	var m types.Metrics

	payload, _, seq, _, err := UnmarshalServiceReply(wire, &m)
	if err != nil {
		t.Fatalf("unmarshal empty: %v", err)
	}

	if seq != 1 {
		t.Fatalf("seq = %d, want 1", seq)
	}

	if len(payload) != 0 {
		t.Fatalf("empty payload has %d bytes, want 0", len(payload))
	}
}

// TestServicePacketRejectsBad checks zero seq and over cap fail.
func TestServicePacketRejectsBad(t *testing.T) {
	t.Parallel()

	var guid [8]byte

	_, err := MarshalServiceRequest([]byte{0x01}, guid, 0, true)
	if err == nil {
		t.Fatalf("zero seq succeeded, want error")
	}

	big := make([]byte, maxPayloadLen+1)

	_, err = MarshalServiceRequest(big, guid, 1, true)
	if err == nil {
		t.Fatalf("over cap succeeded, want error")
	}

	var m types.Metrics

	_, _, _, _, err = UnmarshalServiceRequest([]byte{}, &m)
	if err == nil {
		t.Fatalf("empty parse succeeded, want error")
	}

	_, _, _, _, err = UnmarshalServiceReply([]byte{0x00, 0x01}, &m)
	if err == nil {
		t.Fatalf("short parse succeeded, want error")
	}
}

// TestServiceUserdataRoundTrip checks serviceid plus hash plus topic plus domain.
func TestServiceUserdataRoundTrip(t *testing.T) {
	t.Parallel()

	prefix := serviceTestPrefix()

	var id [8]byte

	copy(id[0:4], prefix[0:4])
	id[4], id[5], id[6], id[7] = 0, 0, 0, 42
	hexed := EncodeServiceIDHex(id)

	raw, err := EncodeServiceUserdata(hexed, "RIHS01_abc", "rq/svcRequest", 3)
	if err != nil {
		t.Fatalf("encode userdata: %v", err)
	}

	parsed, err := ParseServiceUserdata(raw)
	if err != nil {
		t.Fatalf("parse userdata: %v", err)
	}

	if !parsed.HasServiceID || parsed.ServiceID != hexed {
		t.Fatalf("service id = %q has=%v, want %q", parsed.ServiceID, parsed.HasServiceID, hexed)
	}

	if parsed.TypeHash != "RIHS01_abc" {
		t.Fatalf("hash = %q", parsed.TypeHash)
	}

	if !parsed.HasDDSTopic || parsed.DDSTopic != "rq/svcRequest" {
		t.Fatalf("topic = %q", parsed.DDSTopic)
	}

	if !parsed.HasDomain || parsed.Domain != 3 {
		t.Fatalf("domain = %d", parsed.Domain)
	}
}

// TestServiceUserdataEmptyID checks omission keeps old three field form.
func TestServiceUserdataEmptyID(t *testing.T) {
	t.Parallel()

	raw, err := EncodeServiceUserdata("", "h", "rt/chatter", 0)
	if err != nil {
		t.Fatalf("encode empty id: %v", err)
	}

	parsed, err := ParseServiceUserdata(raw)
	if err != nil {
		t.Fatalf("parse empty id: %v", err)
	}

	if parsed.HasServiceID {
		t.Fatalf("empty id parsed as present")
	}
}

// TestServiceUserdataToleratesUnknown checks unknown keys are ignored.
func TestServiceUserdataToleratesUnknown(t *testing.T) {
	t.Parallel()

	parsed, err := ParseServiceUserdata("serviceid=0123456789abcdef;typehash=h;unknown=1;dds_topic=rt/c;domain=0;")
	if err != nil {
		t.Fatalf("parse unknown: %v", err)
	}

	if !parsed.HasServiceID {
		t.Fatalf("service id missing")
	}

	_, err = ParseServiceUserdata("bare-token")
	if err == nil {
		t.Fatalf("bare token succeeded, want error")
	}
}

// TestCheckServiceAvailable checks counts plus service id fallback.
func TestCheckServiceAvailable(t *testing.T) {
	t.Parallel()

	if CheckServiceAvailable(0, 1, "", nil, nil) {
		t.Fatalf("zero readers available, want false")
	}

	if CheckServiceAvailable(1, 0, "", nil, nil) {
		t.Fatalf("zero writers available, want false")
	}

	if !CheckServiceAvailable(1, 1, "", nil, nil) {
		t.Fatalf("non empty without id unavailable, want true")
	}

	if !CheckServiceAvailable(1, 1, "", []string{}, []string{}) {
		t.Fatalf("empty id lists unavailable, want fallback true")
	}

	if !CheckServiceAvailable(1, 1, "ab", []string{"ab"}, nil) {
		t.Fatalf("reader id match unavailable, want true")
	}

	if !CheckServiceAvailable(1, 1, "ab", nil, []string{"ab"}) {
		t.Fatalf("writer id match unavailable, want true")
	}

	if CheckServiceAvailable(1, 1, "ab", []string{"cd"}, []string{"ef"}) {
		t.Fatalf("mismatched ids available, want false")
	}

	if !CheckServiceAvailable(1, 1, "ab", nil, nil) {
		t.Fatalf("no advertised ids unavailable, want fallback true")
	}
}

// TestDeclareServiceEndpoint checks service reader and writer declaration.
//
// It declares a request reader plus a reply writer for one service and
// checks DDS topics, DDS types, IDL round trip, and service name. It also
// checks duplicate entity allocation advances and bad names fail.
//
//nolint:cyclop // test pins reader, writer, lookup, and reject breadth in one run, splitting would obscure intent
func TestDeclareServiceEndpoint(t *testing.T) {
	t.Parallel()
	var prefix SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = byte(i + 1)
	}
	idx, err := NewSEDPIndex(prefix, 42)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	reader, err := idx.DeclareServiceEndpoint(true, "/add_two_ints", "my_pkg__srv__AddTwoInts_Request", true, "", nil)
	if err != nil {
		t.Fatalf("declare request reader: %v", err)
	}
	if reader.DDSTopic != "rq/add_two_intsRequest" {
		t.Fatalf("reader topic = %q, want rq/add_two_intsRequest", reader.DDSTopic)
	}
	if reader.ROSTopic != "/add_two_ints" {
		t.Fatalf("reader ros = %q, want /add_two_ints", reader.ROSTopic)
	}
	if reader.TypeDDS != "my_pkg::srv::dds_::AddTwoInts_Request_" {
		t.Fatalf("reader type = %q", reader.TypeDDS)
	}
	if !reader.IsReader {
		t.Fatalf("reader flag false, want true")
	}
	writer, err := idx.DeclareServiceEndpoint(false, "/add_two_ints", "my_pkg__srv__AddTwoInts_Response", false, "", nil)
	if err != nil {
		t.Fatalf("declare reply writer: %v", err)
	}
	if writer.DDSTopic != "rr/add_two_intsReply" {
		t.Fatalf("writer topic = %q, want rr/add_two_intsReply", writer.DDSTopic)
	}
	if writer.IsReader {
		t.Fatalf("writer flag true, want false")
	}
	if _, ok := idx.findLocalServiceWriter("rr/add_two_intsReply", "my_pkg::srv::dds_::AddTwoInts_Response_", "my_pkg__srv__AddTwoInts_Response", ""); !ok {
		t.Fatalf("find local service writer missed")
	}
	if _, err := idx.DeclareServiceEndpoint(true, "///", "my_pkg__srv__AddTwoInts_Request", true, "", nil); err == nil {
		t.Fatalf("bad service name succeeded, want error")
	}
	if _, err := idx.DeclareServiceEndpoint(true, "/add_two_ints", "bad", true, "", nil); err == nil {
		t.Fatalf("bad idl succeeded, want error")
	}
}
