// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

//nolint:cyclop // naming tests hold many sequential mapping assertions, breadth is the point
package dds_test

import (
	"strings"
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
)

// mustTopic maps a ROS topic and fails the test on error.
func mustTopic(t *testing.T, ros string, avoid bool) string {
	t.Helper()

	got, err := dds.TopicName(ros, avoid)
	if err != nil {
		t.Fatalf("TopicName(%q): %v", ros, err)
	}

	return got
}

// TestTopicName checks normal and bare topic mapping.
func TestTopicName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		ros   string
		avoid bool
		want  string
	}{
		{ros: "/chatter", avoid: false, want: "rt/chatter"},
		{ros: "chatter", avoid: false, want: "rt/chatter"},
		{ros: "/chatter", avoid: true, want: "chatter"},
		{ros: "/ns/topic", avoid: false, want: "rt/ns/topic"},
		{ros: "/ns/topic", avoid: true, want: "ns/topic"},
	}

	for _, tc := range cases {
		got, err := dds.TopicName(tc.ros, tc.avoid)
		if err != nil {
			t.Fatalf("TopicName(%q): %v", tc.ros, err)
		}

		if got != tc.want {
			t.Fatalf("TopicName(%q, %v) = %q, want %q", tc.ros, tc.avoid, got, tc.want)
		}
	}
}

// TestTopicNameRejectsEmptySegments checks slash misuse is rejected.
func TestTopicNameRejectsEmptySegments(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", "/", "//", "/a//b", "a//b", "/chatter/", "chatter/", "/a/", "   "} {
		_, err := dds.TopicName(raw, false)
		if err == nil {
			t.Fatalf("TopicName(%q) succeeded, want error", raw)
		}

		_, err = dds.TopicName(raw, true)
		if err == nil {
			t.Fatalf("TopicName bare(%q) succeeded, want error", raw)
		}
	}
}

// TestServiceTopics checks request and reply naming both modes.
func TestServiceTopics(t *testing.T) {
	t.Parallel()

	got, err := dds.RequestTopic("add_two_ints", false)
	if err != nil {
		t.Fatalf("request topic: %v", err)
	}

	if got != "rq/add_two_intsRequest" {
		t.Fatalf("request = %q, want rq/add_two_intsRequest", got)
	}

	got, err = dds.ReplyTopic("add_two_ints", false)
	if err != nil {
		t.Fatalf("reply topic: %v", err)
	}

	if got != "rr/add_two_intsReply" {
		t.Fatalf("reply = %q, want rr/add_two_intsReply", got)
	}

	got, err = dds.RequestTopic("/add_two_ints", true)
	if err != nil {
		t.Fatalf("bare request topic: %v", err)
	}

	if got != "add_two_intsRequest" {
		t.Fatalf("bare request = %q", got)
	}

	got, err = dds.ReplyTopic("/add_two_ints", true)
	if err != nil {
		t.Fatalf("bare reply topic: %v", err)
	}

	if got != "add_two_intsReply" {
		t.Fatalf("bare reply = %q", got)
	}
}

// TestServiceTopicsRejectEmpty checks service slash misuse is rejected.
func TestServiceTopicsRejectEmpty(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", "/", "//", "/svc/", "svc/", "/a//b"} {
		_, err := dds.RequestTopic(raw, false)
		if err == nil {
			t.Fatalf("RequestTopic(%q) succeeded, want error", raw)
		}

		_, err = dds.ReplyTopic(raw, false)
		if err == nil {
			t.Fatalf("ReplyTopic(%q) succeeded, want error", raw)
		}
	}
}

// TestTopicReplyVsTypeResponseDistinction checks Reply topics differ from Response types.
func TestTopicReplyVsTypeResponseDistinction(t *testing.T) {
	t.Parallel()

	reply, err := dds.ReplyTopic("/add_two_ints", false)
	if err != nil {
		t.Fatalf("reply topic: %v", err)
	}

	if !strings.HasSuffix(reply, "Reply") {
		t.Fatalf("reply topic %q misses Reply suffix", reply)
	}

	rsp, err := dds.ServiceResponseTypeName("my_pkg__srv__AddTwoInts")
	if err != nil {
		t.Fatalf("response type: %v", err)
	}

	if !strings.HasSuffix(rsp, "_Response_") {
		t.Fatalf("response type %q misses _Response_ suffix", rsp)
	}

	if strings.Contains(reply, "Response") {
		t.Fatalf("reply topic %q must not contain Response", reply)
	}

	if strings.Contains(rsp, "Reply") {
		t.Fatalf("response type %q must not contain Reply", rsp)
	}
}

// TestParseTopicName checks prefixed and bare reverse mapping.
func TestParseTopicName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		dds  string
		ros  string
		kind dds.TopicKind
	}{
		{dds: "rt/chatter", ros: "/chatter", kind: dds.TopicKindTopic},
		{dds: "rt/ns/topic", ros: "/ns/topic", kind: dds.TopicKindTopic},
		{dds: "rq/add_two_intsRequest", ros: "/add_two_ints", kind: dds.TopicKindRequest},
		{dds: "rr/add_two_intsReply", ros: "/add_two_ints", kind: dds.TopicKindReply},
		{dds: "rq/ns/svcRequest", ros: "/ns/svc", kind: dds.TopicKindRequest},
		{dds: "rr/ns/svcReply", ros: "/ns/svc", kind: dds.TopicKindReply},
		{dds: "chatter", ros: "/chatter", kind: dds.TopicKindTopic},
		{dds: "ns/topic", ros: "/ns/topic", kind: dds.TopicKindTopic},
		{dds: "add_two_intsRequest", ros: "/add_two_ints", kind: dds.TopicKindRequest},
		{dds: "add_two_intsReply", ros: "/add_two_ints", kind: dds.TopicKindReply},
		{dds: "ns/svcRequest", ros: "/ns/svc", kind: dds.TopicKindRequest},
		{dds: "ns/svcReply", ros: "/ns/svc", kind: dds.TopicKindReply},
	}

	for _, tc := range cases {
		got, kind, err := dds.ParseTopicName(tc.dds)
		if err != nil {
			t.Fatalf("ParseTopicName(%q): %v", tc.dds, err)
		}

		if got != tc.ros || kind != tc.kind {
			t.Fatalf("ParseTopicName(%q) = (%q, %q), want (%q, %q)", tc.dds, got, kind, tc.ros, tc.kind)
		}
	}
}

// TestParseTopicNameBarePin pins bare names without prefix.
func TestParseTopicNameBarePin(t *testing.T) {
	t.Parallel()

	cases := []struct {
		dds  string
		ros  string
		kind dds.TopicKind
	}{
		{dds: "chatter", ros: "/chatter", kind: dds.TopicKindTopic},
		{dds: "ns/topic", ros: "/ns/topic", kind: dds.TopicKindTopic},
		{dds: "myRequest", ros: "/my", kind: dds.TopicKindRequest},
		{dds: "myReply", ros: "/my", kind: dds.TopicKindReply},
	}

	for _, tc := range cases {
		got, kind, err := dds.ParseTopicName(tc.dds)
		if err != nil {
			t.Fatalf("ParseTopicName(%q): %v", tc.dds, err)
		}

		if got != tc.ros || kind != tc.kind {
			t.Fatalf("ParseTopicName(%q) = (%q, %q), want (%q, %q)", tc.dds, got, kind, tc.ros, tc.kind)
		}
	}
}

// TestParseTopicNameSingleSegmentRT checks lone rt, rq, and rr are bare.
func TestParseTopicNameSingleSegmentRT(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"rt", "rq", "rr"} {
		got, kind, err := dds.ParseTopicName(raw)
		if err != nil {
			t.Fatalf("ParseTopicName(%q): %v", raw, err)
		}

		if got != "/"+raw || kind != dds.TopicKindTopic {
			t.Fatalf("ParseTopicName(%q) = (%q, %q), want (%q, topic)", raw, got, kind, "/"+raw)
		}
	}
}

// TestParseTopicNameExactRequestReply checks exact suffix words are topics.
func TestParseTopicNameExactRequestReply(t *testing.T) {
	t.Parallel()

	got, kind, err := dds.ParseTopicName("Request")
	if err != nil {
		t.Fatalf("ParseTopicName(Request): %v", err)
	}

	if got != "/Request" || kind != dds.TopicKindTopic {
		t.Fatalf("Request = (%q, %q), want (/Request, topic)", got, kind)
	}

	got, kind, err = dds.ParseTopicName("Reply")
	if err != nil {
		t.Fatalf("ParseTopicName(Reply): %v", err)
	}

	if got != "/Reply" || kind != dds.TopicKindTopic {
		t.Fatalf("Reply = (%q, %q), want (/Reply, topic)", got, kind)
	}
}

// TestParseTopicNameUppercaseFallsThrough checks prefixes are lowercase.
func TestParseTopicNameUppercaseFallsThrough(t *testing.T) {
	t.Parallel()

	got, kind, err := dds.ParseTopicName("RT/chatter")
	if err != nil {
		t.Fatalf("ParseTopicName(RT/chatter): %v", err)
	}

	if got != "/RT/chatter" || kind != dds.TopicKindTopic {
		t.Fatalf("RT/chatter = (%q, %q), want (/RT/chatter, topic)", got, kind)
	}

	got, kind, err = dds.ParseTopicName("RQ/svcRequest")
	if err != nil {
		t.Fatalf("ParseTopicName(RQ/svcRequest): %v", err)
	}

	if kind != dds.TopicKindRequest {
		t.Fatalf("RQ/svcRequest kind = %q, want request (bare fallthrough)", kind)
	}

	if got != "/RQ/svc" {
		t.Fatalf("RQ/svcRequest = %q, want /RQ/svc", got)
	}
}

// checkTopicRoundTrip checks topic forward and reverse mapping agree.
func checkTopicRoundTrip(t *testing.T) {
	t.Helper()

	topic, err := dds.TopicName("/chatter", false)
	if err != nil {
		t.Fatalf("topic name: %v", err)
	}

	back, kind, err := dds.ParseTopicName(topic)
	if err != nil {
		t.Fatalf("parse topic: %v", err)
	}

	if back != "/chatter" || kind != dds.TopicKindTopic {
		t.Fatalf("round trip = (%q, %q)", back, kind)
	}
}

// checkRequestRoundTrip checks request forward and reverse mapping agree.
func checkRequestRoundTrip(t *testing.T) {
	t.Helper()

	req, err := dds.RequestTopic("/add_two_ints", false)
	if err != nil {
		t.Fatalf("request topic: %v", err)
	}

	back, kind, err := dds.ParseTopicName(req)
	if err != nil {
		t.Fatalf("parse request: %v", err)
	}

	if back != "/add_two_ints" || kind != dds.TopicKindRequest {
		t.Fatalf("request round trip = (%q, %q)", back, kind)
	}
}

// checkReplyRoundTrip checks reply forward and reverse mapping agree.
func checkReplyRoundTrip(t *testing.T) {
	t.Helper()

	rep, err := dds.ReplyTopic("/add_two_ints", false)
	if err != nil {
		t.Fatalf("reply topic: %v", err)
	}

	back, kind, err := dds.ParseTopicName(rep)
	if err != nil {
		t.Fatalf("parse reply: %v", err)
	}

	if back != "/add_two_ints" || kind != dds.TopicKindReply {
		t.Fatalf("reply round trip = (%q, %q)", back, kind)
	}
}

// TestParseTopicNameRoundTrip checks forward and reverse mapping agree.
func TestParseTopicNameRoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("topic", func(t *testing.T) {
		t.Parallel()
		checkTopicRoundTrip(t)
	})

	t.Run("request", func(t *testing.T) {
		t.Parallel()
		checkRequestRoundTrip(t)
	})

	t.Run("reply", func(t *testing.T) {
		t.Parallel()
		checkReplyRoundTrip(t)
	})
}

// TestParseTopicNameRejectsBad checks malformed DDS names are rejected.
func TestParseTopicNameRejectsBad(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", "   ", "/rt/chatter", "rt/chatter/", "rt//chatter", "rq/svc", "rr/svc", "rq/Request", "rr/Reply", "rt/", "rq/", "a//b"} {
		_, _, err := dds.ParseTopicName(raw)
		if err == nil {
			t.Fatalf("ParseTopicName(%q) succeeded, want error", raw)
		}
	}
}

// TestMessageTypeName checks the dds_ marker and trailing underscore.
func TestMessageTypeName(t *testing.T) {
	t.Parallel()

	got, err := dds.MessageTypeName("geometry_msgs__msg__Twist")
	if err != nil {
		t.Fatalf("type name: %v", err)
	}

	want := "geometry_msgs::msg::dds_::Twist_"
	if got != want {
		t.Fatalf("type = %q, want %q", got, want)
	}

	_, err = dds.MessageTypeName("broken")
	if err == nil {
		t.Fatalf("broken type succeeded, want error")
	}
}

// TestMessageTypeNameFourPart checks longer namespaces map and round trip.
func TestMessageTypeNameFourPart(t *testing.T) {
	t.Parallel()

	got, err := dds.MessageTypeName("a__b__c__D")
	if err != nil {
		t.Fatalf("type name: %v", err)
	}

	if got != "a::b::c::dds_::D_" {
		t.Fatalf("type = %q, want a::b::c::dds_::D_", got)
	}

	back, err := dds.DemangleTypeName(got)
	if err != nil {
		t.Fatalf("demangle: %v", err)
	}

	if back != "a__b__c__D" {
		t.Fatalf("demangled = %q, want a__b__c__D", back)
	}
}

// TestServiceTypeNames checks Request_ and Response_ suffix forms.
func TestServiceTypeNames(t *testing.T) {
	t.Parallel()

	req, err := dds.ServiceRequestTypeName("example__srv__AddTwoInts")
	if err != nil {
		t.Fatalf("request type: %v", err)
	}

	if req != "example::srv::dds_::AddTwoInts_Request_" {
		t.Fatalf("request = %q", req)
	}

	rsp, err := dds.ServiceResponseTypeName("example__srv__AddTwoInts")
	if err != nil {
		t.Fatalf("response type: %v", err)
	}

	if rsp != "example::srv::dds_::AddTwoInts_Response_" {
		t.Fatalf("response = %q", rsp)
	}
}

// TestDemangleTypeName checks the reverse mapping including services.
func TestDemangleTypeName(t *testing.T) {
	t.Parallel()

	back, err := dds.DemangleTypeName("my_pkg::srv::dds_::AddTwoInts_Request_")
	if err != nil {
		t.Fatalf("demangle: %v", err)
	}

	if back != "my_pkg__srv__AddTwoInts_Request" {
		t.Fatalf("demangled = %q", back)
	}

	_, err = dds.DemangleTypeName("no-marker")
	if err == nil {
		t.Fatalf("bad demangle succeeded, want error")
	}
}

// TestDemangleErrorMatrix checks malformed DDS type names are rejected.
func TestDemangleErrorMatrix(t *testing.T) {
	t.Parallel()

	bads := []string{
		"",
		"no-marker",
		"my_pkg::srv::dds_::MissingUnderscore",
		"::dds_::Name_",
		"pkg::dds_::_",
		"pkg::dds_::Name",
		"pkg::dds_::Has:Scope_",
		"pkg::dds_::has space_",
		"pkg::dds_::9Bad_",
		"pkg::::dds_::Name_",
		"::msg::dds_::Name_",
		"pkg::<bad>::dds_::Name_",
		"a::dds_::b::dds_::c_",
		"pkg::msg::dds_::",
		"pkg::msg::dds_::_",
	}

	for _, raw := range bads {
		_, err := dds.DemangleTypeName(raw)
		if err == nil {
			t.Fatalf("DemangleTypeName(%q) succeeded, want error", raw)
		}
	}
}

// TestDemangleResponseRoundTrip checks response types demangle with suffix.
func TestDemangleResponseRoundTrip(t *testing.T) {
	t.Parallel()

	typed, err := dds.ServiceResponseTypeName("my_pkg__srv__AddTwoInts")
	if err != nil {
		t.Fatalf("response type: %v", err)
	}

	back, err := dds.DemangleTypeName(typed)
	if err != nil {
		t.Fatalf("demangle: %v", err)
	}

	if back != "my_pkg__srv__AddTwoInts_Response" {
		t.Fatalf("demangled = %q", back)
	}
}

// TestDemangleRequestRoundTrip checks request types demangle with suffix.
func TestDemangleRequestRoundTrip(t *testing.T) {
	t.Parallel()

	typed, err := dds.ServiceRequestTypeName("my_pkg__srv__AddTwoInts")
	if err != nil {
		t.Fatalf("request type: %v", err)
	}

	back, err := dds.DemangleTypeName(typed)
	if err != nil {
		t.Fatalf("demangle: %v", err)
	}

	if back != "my_pkg__srv__AddTwoInts_Request" {
		t.Fatalf("demangled = %q", back)
	}
}

// TestMustTopicHelper checks the test helper maps topics.
func TestMustTopicHelper(t *testing.T) {
	t.Parallel()

	if got := mustTopic(t, "/chatter", false); got != "rt/chatter" {
		t.Fatalf("helper = %q", got)
	}
}
