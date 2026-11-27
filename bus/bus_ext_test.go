// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/bus"
)

// TestForwardOnceInMemory checks one bridged sample between backends.
func TestForwardOnceInMemory(t *testing.T) {
	src := openTestMQTT(t)
	defer closeTestBackend(t, src)

	dst := openTestMQTT(t)
	defer closeTestBackend(t, dst)

	out := publishTestSample(t, src)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	forwarded, err := bus.ForwardOnce(ctx, src, dst)
	if err != nil {
		t.Fatalf("forward once: %v", err)
	}

	checkTestSample(t, forwarded, out)

	got := takeTestSample(t, dst)

	checkTestSample(t, got, out)
}

// checkDDSTopicPair checks one ROS to DDS to ROS round trip.
func checkDDSTopicPair(t *testing.T, ros string) {
	t.Helper()

	wire, err := bus.TopicName(ros, false)
	if err != nil {
		t.Fatalf("topic name %q: %v", ros, err)
	}

	back, kind, err := bus.ParseTopicName(wire)
	if err != nil {
		t.Fatalf("parse topic %q: %v", wire, err)
	}

	if back != ros {
		t.Fatalf("topic round trip = %q, want %q", back, ros)
	}

	if kind != bus.TopicKindTopic {
		t.Fatalf("topic kind = %q, want %q", kind, bus.TopicKindTopic)
	}
}

// checkDDSServicePair checks request plus reply round trips for one service.
func checkDDSServicePair(t *testing.T, service string) {
	t.Helper()

	req, err := bus.RequestTopic(service, false)
	if err != nil {
		t.Fatalf("request topic %q: %v", service, err)
	}

	back, kind, err := bus.ParseTopicName(req)
	if err != nil {
		t.Fatalf("parse request %q: %v", req, err)
	}

	if back != service {
		t.Fatalf("request round trip = %q, want %q", back, service)
	}

	if kind != bus.TopicKindRequest {
		t.Fatalf("request kind = %q, want %q", kind, bus.TopicKindRequest)
	}

	rep, err := bus.ReplyTopic(service, false)
	if err != nil {
		t.Fatalf("reply topic %q: %v", service, err)
	}

	back, kind, err = bus.ParseTopicName(rep)
	if err != nil {
		t.Fatalf("parse reply %q: %v", rep, err)
	}

	if back != service {
		t.Fatalf("reply round trip = %q, want %q", back, service)
	}

	if kind != bus.TopicKindReply {
		t.Fatalf("reply kind = %q, want %q", kind, bus.TopicKindReply)
	}
}

// checkDDSMessageType checks one IDL message type round trip.
func checkDDSMessageType(t *testing.T, idl string) {
	t.Helper()

	wire, err := bus.MessageTypeName(idl)
	if err != nil {
		t.Fatalf("message type %q: %v", idl, err)
	}

	back, err := bus.DemangleTypeName(wire)
	if err != nil {
		t.Fatalf("demangle %q: %v", wire, err)
	}

	if back != idl {
		t.Fatalf("type round trip = %q, want %q", back, idl)
	}
}

// checkDDSServiceType checks service request plus response type round trips.
func checkDDSServiceType(t *testing.T, base string) {
	t.Helper()

	req, err := bus.ServiceRequestTypeName(base)
	if err != nil {
		t.Fatalf("service request type %q: %v", base, err)
	}

	back, err := bus.DemangleTypeName(req)
	if err != nil {
		t.Fatalf("demangle request %q: %v", req, err)
	}

	wantReq := base + "_Request"
	if back != wantReq {
		t.Fatalf("request type round trip = %q, want %q", back, wantReq)
	}

	rsp, err := bus.ServiceResponseTypeName(base)
	if err != nil {
		t.Fatalf("service response type %q: %v", base, err)
	}

	back, err = bus.DemangleTypeName(rsp)
	if err != nil {
		t.Fatalf("demangle response %q: %v", rsp, err)
	}

	wantRsp := base + "_Response"
	if back != wantRsp {
		t.Fatalf("response type round trip = %q, want %q", back, wantRsp)
	}
}

// TestDDSTopicHelpersRoundTrip checks DDS topic and type name codecs.
func TestDDSTopicHelpersRoundTrip(t *testing.T) {
	checkDDSTopicPair(t, "/chatter")
	checkDDSTopicPair(t, "/a/b")
	checkDDSServicePair(t, "/add_two_ints")
	checkDDSMessageType(t, "std_msgs__msg__String")
	checkDDSServiceType(t, "my_pkg__srv__AddTwoInts")

	if bus.TopicPrefix != "rt" {
		t.Fatalf("topic prefix = %q, want %q", bus.TopicPrefix, "rt")
	}

	if bus.RequestPrefix != "rq" {
		t.Fatalf("request prefix = %q, want %q", bus.RequestPrefix, "rq")
	}

	if bus.ReplyPrefix != "rr" {
		t.Fatalf("reply prefix = %q, want %q", bus.ReplyPrefix, "rr")
	}

	if bus.DDSMarker != "::dds_::" {
		t.Fatalf("dds marker = %q, want %q", bus.DDSMarker, "::dds_::")
	}

	if bus.TypeSuffix != "_" {
		t.Fatalf("type suffix = %q, want %q", bus.TypeSuffix, "_")
	}
}

// checkMQTTDataTopic checks one data topic encode plus decode round trip.
func checkMQTTDataTopic(t *testing.T, prefix string, domain uint32, kind bus.Kind, ros string) {
	t.Helper()

	wire, err := bus.EncodeTopic(prefix, domain, kind, ros)
	if err != nil {
		t.Fatalf("encode topic %q: %v", ros, err)
	}

	gotDomain, gotKind, gotROS, err := bus.DecodeTopic(prefix, wire)
	if err != nil {
		t.Fatalf("decode topic %q: %v", wire, err)
	}

	if gotDomain != domain {
		t.Fatalf("domain = %d, want %d", gotDomain, domain)
	}

	if gotKind != kind {
		t.Fatalf("kind = %q, want %q", gotKind, kind)
	}

	if gotROS != ros {
		t.Fatalf("ros topic = %q, want %q", gotROS, ros)
	}
}

// checkMQTTDiscoveryTopic checks one discovery topic encode plus decode.
func checkMQTTDiscoveryTopic(t *testing.T) {
	t.Helper()

	wire, err := bus.EncodeDiscoveryTopic("weld", 0, "talker", bus.KindData, "/chatter")
	if err != nil {
		t.Fatalf("encode discovery: %v", err)
	}

	domain, participant, kind, ros, err := bus.DecodeDiscoveryTopic("weld", wire)
	if err != nil {
		t.Fatalf("decode discovery %q: %v", wire, err)
	}

	if domain != 0 {
		t.Fatalf("discovery domain = %d, want 0", domain)
	}

	if participant != "talker" {
		t.Fatalf("participant = %q, want %q", participant, "talker")
	}

	if kind != bus.KindData {
		t.Fatalf("discovery kind = %q, want %q", kind, bus.KindData)
	}

	if ros != "/chatter" {
		t.Fatalf("discovery ros = %q, want %q", ros, "/chatter")
	}
}

// TestMQTTTopicCodecRoundTrip checks MQTT topic and discovery codecs.
func TestMQTTTopicCodecRoundTrip(t *testing.T) {
	checkMQTTDataTopic(t, "weld", 0, bus.KindData, "/chatter")
	checkMQTTDataTopic(t, "weld", 2, bus.KindRequest, "/svc")
	checkMQTTDataTopic(t, "", 0, bus.KindReply, "/svc")
	checkMQTTDiscoveryTopic(t)

	normalized := bus.NormalizePrefix("")
	if normalized != "weld" {
		t.Fatalf("normalize empty = %q, want %q", normalized, "weld")
	}

	normalized, err := bus.EncodeTopic("weld", 0, bus.KindData, "/chatter")
	if err != nil {
		t.Fatalf("encode for prefix check: %v", err)
	}

	if normalized == "" {
		t.Fatal("encode returned empty topic")
	}
}
