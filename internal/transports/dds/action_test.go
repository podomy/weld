// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds_test

import (
	"errors"
	"testing"

	"github.com/podomy/weld/internal/transports/dds"
)

// mustActionTopics returns the DDS topics for an action or fails the test.
func mustActionTopics(t *testing.T, action string, avoid bool) dds.ActionDDSTopics {
	t.Helper()
	topics, err := dds.ActionDDSTopicsFor(action, avoid)
	if err != nil {
		t.Fatalf("ActionDDSTopicsFor(%q): %v", action, err)
	}
	return topics
}

// TestActionROSNaming checks ROS exchange names for one action.
func TestActionROSNaming(t *testing.T) {
	t.Parallel()
	send, err := dds.SendGoalServiceName("/fibonacci")
	if err != nil {
		t.Fatalf("send goal name: %v", err)
	}
	if send != "/fibonacci/_action/send_goal" {
		t.Fatalf("send goal = %q, want /fibonacci/_action/send_goal", send)
	}
	cancel, err := dds.CancelGoalServiceName("/fibonacci")
	if err != nil {
		t.Fatalf("cancel name: %v", err)
	}
	if cancel != "/fibonacci/_action/cancel_goal" {
		t.Fatalf("cancel = %q", cancel)
	}
	result, err := dds.GetResultServiceName("/fibonacci")
	if err != nil {
		t.Fatalf("result name: %v", err)
	}
	if result != "/fibonacci/_action/get_result" {
		t.Fatalf("result = %q", result)
	}
	feedback, err := dds.FeedbackTopicName("/fibonacci")
	if err != nil {
		t.Fatalf("feedback name: %v", err)
	}
	if feedback != "/fibonacci/_action/feedback" {
		t.Fatalf("feedback = %q", feedback)
	}
	status, err := dds.StatusTopicName("/fibonacci")
	if err != nil {
		t.Fatalf("status = %q", status)
	}
	if status != "/fibonacci/_action/status" {
		t.Fatalf("status = %q, want /fibonacci/_action/status", status)
	}
}

// TestActionDDSTopics checks the eight DDS topics for goal plus cancel plus result plus feedback plus status.
func TestActionDDSTopics(t *testing.T) {
	t.Parallel()
	topics := mustActionTopics(t, "/fibonacci", false)
	if topics.SendGoalRequest != "rq/fibonacci/_action/send_goalRequest" {
		t.Fatalf("send req = %q", topics.SendGoalRequest)
	}
	if topics.SendGoalReply != "rr/fibonacci/_action/send_goalReply" {
		t.Fatalf("send rep = %q", topics.SendGoalReply)
	}
	if topics.CancelGoalRequest != "rq/fibonacci/_action/cancel_goalRequest" {
		t.Fatalf("cancel req = %q", topics.CancelGoalRequest)
	}
	if topics.CancelGoalReply != "rr/fibonacci/_action/cancel_goalReply" {
		t.Fatalf("cancel rep = %q", topics.CancelGoalReply)
	}
	if topics.GetResultRequest != "rq/fibonacci/_action/get_resultRequest" {
		t.Fatalf("result req = %q", topics.GetResultRequest)
	}
	if topics.GetResultReply != "rr/fibonacci/_action/get_resultReply" {
		t.Fatalf("result rep = %q", topics.GetResultReply)
	}
	if topics.Feedback != "rt/fibonacci/_action/feedback" {
		t.Fatalf("feedback = %q", topics.Feedback)
	}
	if topics.Status != "rt/fibonacci/_action/status" {
		t.Fatalf("status = %q", topics.Status)
	}
	bare := mustActionTopics(t, "/fibonacci", true)
	if bare.SendGoalRequest != "fibonacci/_action/send_goalRequest" {
		t.Fatalf("bare send req = %q", bare.SendGoalRequest)
	}
	if bare.Feedback != "fibonacci/_action/feedback" {
		t.Fatalf("bare feedback = %q", bare.Feedback)
	}
	if bare.Status != "fibonacci/_action/status" {
		t.Fatalf("bare status = %q", bare.Status)
	}
}

// TestActionTopicsRejectBad checks malformed action names are rejected.
func TestActionTopicsRejectBad(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"", "/", "//", "/a//b", "/fibonacci/", "   "} {
		if _, err := dds.SendGoalServiceName(raw); err == nil {
			t.Fatalf("SendGoalServiceName(%q) succeeded, want error", raw)
		}
		if _, err := dds.ActionDDSTopicsFor(raw, false); err == nil {
			t.Fatalf("ActionDDSTopicsFor(%q) succeeded, want error", raw)
		}
		if _, err := dds.ActionStatusTopic(raw, false); err == nil {
			t.Fatalf("ActionStatusTopic(%q) succeeded, want error", raw)
		}
		if _, err := dds.ActionFeedbackTopic(raw, false); err == nil {
			t.Fatalf("ActionFeedbackTopic(%q) succeeded, want error", raw)
		}
	}
}

// TestActionSplitAndParse checks ROS split plus DDS reverse mapping.
func TestActionSplitAndParse(t *testing.T) {
	t.Parallel()
	base, exchange, err := dds.SplitActionROSName("/fibonacci/_action/send_goal")
	if err != nil {
		t.Fatalf("split send: %v", err)
	}
	if base != "/fibonacci" || exchange != dds.ActionSendGoal {
		t.Fatalf("split send = (%q, %q)", base, exchange)
	}
	topics := mustActionTopics(t, "/ns/fibonacci", false)
	cases := map[string]string{
		topics.SendGoalRequest:   "/ns/fibonacci",
		topics.SendGoalReply:     "/ns/fibonacci",
		topics.CancelGoalRequest: "/ns/fibonacci",
		topics.GetResultRequest:  "/ns/fibonacci",
		topics.Feedback:          "/ns/fibonacci",
		topics.Status:            "/ns/fibonacci",
	}
	for ddsTopic, wantBase := range cases {
		base, _, _, err := dds.ParseActionDDSTopic(ddsTopic)
		if err != nil {
			t.Fatalf("parse %q: %v", ddsTopic, err)
		}
		if base != wantBase {
			t.Fatalf("parse %q base = %q, want %q", ddsTopic, base, wantBase)
		}
	}
	_, _, _, err = dds.ParseActionDDSTopic("rt/chatter")
	if err == nil {
		t.Fatalf("parse non action topic succeeded, want error")
	}
	if _, _, err := dds.SplitActionROSName("/fibonacci"); err == nil {
		t.Fatalf("split bare action succeeded, want error")
	}
}

// TestActionTypes checks per action DDS types plus fixed generic types.
//
//nolint:cyclop // test pins action DDS type shapes for all roles in one run, splitting would obscure intent
func TestActionTypes(t *testing.T) {
	t.Parallel()
	base := "nav2_msgs__action__NavigateToPose"
	sendReq, err := dds.ActionSendGoalRequestType(base)
	if err != nil {
		t.Fatalf("send req type: %v", err)
	}
	if sendReq != "nav2_msgs::action::dds_::NavigateToPose_SendGoal_Request_" {
		t.Fatalf("send req = %q", sendReq)
	}
	sendRsp, err := dds.ActionSendGoalResponseType(base)
	if err != nil {
		t.Fatalf("send rsp type: %v", err)
	}
	if sendRsp != "nav2_msgs::action::dds_::NavigateToPose_SendGoal_Response_" {
		t.Fatalf("send rsp = %q", sendRsp)
	}
	resultReq, err := dds.ActionGetResultRequestType(base)
	if err != nil {
		t.Fatalf("result req type: %v", err)
	}
	if resultReq != "nav2_msgs::action::dds_::NavigateToPose_GetResult_Request_" {
		t.Fatalf("result req = %q", resultReq)
	}
	fbMsg, err := dds.ActionFeedbackMessageType(base)
	if err != nil {
		t.Fatalf("feedback msg type: %v", err)
	}
	if fbMsg != "nav2_msgs::action::dds_::NavigateToPose_FeedbackMessage_" {
		t.Fatalf("feedback msg = %q", fbMsg)
	}
	all, err := dds.ActionDDSTypesFor(base)
	if err != nil {
		t.Fatalf("types for: %v", err)
	}
	if all.CancelGoalRequest != dds.CancelGoalRequestDDSType {
		t.Fatalf("cancel req = %q", all.CancelGoalRequest)
	}
	if all.StatusArray != dds.GoalStatusArrayDDSType {
		t.Fatalf("status array = %q", all.StatusArray)
	}
	back, err := dds.DemangleTypeName(sendReq)
	if err != nil {
		t.Fatalf("demangle send req: %v", err)
	}
	if back != base+"_SendGoal_Request" {
		t.Fatalf("demangled = %q", back)
	}
	if _, err := dds.ActionSendGoalRequestType("std_msgs__msg__String"); err == nil {
		t.Fatalf("non action type succeeded, want error")
	}
	if _, err := dds.ActionDDSTypesFor("broken"); err == nil {
		t.Fatalf("broken type succeeded, want error")
	}
}

// TestActionGoalStatus checks terminal plus active plus preempt states.
//
//nolint:gocognit,cyclop // test pins terminal, active, preempt, and name checks in one run, splitting would obscure intent
func TestActionGoalStatus(t *testing.T) {
	t.Parallel()
	terminals := []int8{dds.GoalStatusSucceeded, dds.GoalStatusCanceled, dds.GoalStatusAborted}
	for _, s := range terminals {
		if !dds.IsTerminalGoalStatus(s) {
			t.Fatalf("status %d is not terminal", s)
		}
		if dds.IsActiveGoalStatus(s) {
			t.Fatalf("terminal %d is active", s)
		}
		if err := dds.ValidateGoalStatus(s); err != nil {
			t.Fatalf("validate terminal %d: %v", s, err)
		}
	}
	actives := []int8{dds.GoalStatusAccepted, dds.GoalStatusExecuting, dds.GoalStatusCanceling}
	for _, s := range actives {
		if !dds.IsActiveGoalStatus(s) {
			t.Fatalf("status %d is not active", s)
		}
		if dds.IsTerminalGoalStatus(s) {
			t.Fatalf("active %d is terminal", s)
		}
	}
	if dds.IsTerminalGoalStatus(dds.GoalStatusUnknown) {
		t.Fatalf("unknown is terminal")
	}
	if dds.IsActiveGoalStatus(dds.GoalStatusUnknown) {
		t.Fatalf("unknown is active")
	}
	if err := dds.ValidateGoalStatus(42); !errors.Is(err, dds.ErrActionInvalidStatus) {
		t.Fatalf("status 42 = %v, want ErrActionInvalidStatus", err)
	}
	if !dds.AllowsPreempt(dds.GoalStatusAccepted) || !dds.AllowsPreempt(dds.GoalStatusExecuting) {
		t.Fatalf("accepted plus executing must allow preempt")
	}
	if dds.AllowsPreempt(dds.GoalStatusCanceling) || dds.AllowsPreempt(dds.GoalStatusSucceeded) {
		t.Fatalf("canceling plus terminal must not allow preempt")
	}
	if got := dds.PreemptTerminal(true); got != dds.GoalStatusCanceled {
		t.Fatalf("preempt canceled = %d, want canceled", got)
	}
	if got := dds.PreemptTerminal(false); got != dds.GoalStatusAborted {
		t.Fatalf("preempt aborted = %d, want aborted", got)
	}
	if dds.GoalStatusName(dds.GoalStatusSucceeded) != "succeeded" {
		t.Fatalf("status name mismatch")
	}
}

// TestActionCancelScope checks the empty goal plus zero stamp policy.
func TestActionCancelScope(t *testing.T) {
	t.Parallel()
	var empty dds.GoalID
	if got := dds.ClassifyCancelRequest(empty, 0, 0); got != dds.CancelScopeAll {
		t.Fatalf("empty plus zero = %v, want all", got)
	}
	if got := dds.ClassifyCancelRequest(empty, 10, 0); got != dds.CancelScopeBefore {
		t.Fatalf("empty plus stamp = %v, want before", got)
	}
	var single dds.GoalID
	single[0] = 7
	if got := dds.ClassifyCancelRequest(single, 0, 0); got != dds.CancelScopeSingle {
		t.Fatalf("single plus zero = %v, want single", got)
	}
	if got := dds.ClassifyCancelRequest(single, 10, 5); got != dds.CancelScopeSingleAndBefore {
		t.Fatalf("single plus stamp = %v, want single_and_before", got)
	}
	if err := dds.ValidateCancelCode(dds.CancelErrorNone); err != nil {
		t.Fatalf("validate cancel none: %v", err)
	}
	if err := dds.ValidateCancelCode(9); !errors.Is(err, dds.ErrActionInvalidCancel) {
		t.Fatalf("cancel 9 = %v, want ErrActionInvalidCancel", err)
	}
}

// TestActionGoalID checks zero plus pairing plus deterministic ordering.
func TestActionGoalID(t *testing.T) {
	t.Parallel()
	var zero dds.GoalID
	if !zero.IsZero() {
		t.Fatalf("zero goal is not zero")
	}
	raw := make([]byte, 16)
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	id, err := dds.GoalIDFromBytes(raw)
	if err != nil {
		t.Fatalf("goal from bytes: %v", err)
	}
	if id.IsZero() {
		t.Fatalf("parsed goal is zero")
	}
	if !dds.GoalPaired(id, id) {
		t.Fatalf("self pairing is false")
	}
	if dds.GoalPaired(id, zero) {
		t.Fatalf("distinct goals pair")
	}
	if _, err := dds.GoalIDFromBytes(raw[:8]); !errors.Is(err, dds.ErrActionInvalidGoal) {
		t.Fatalf("short goal = %v, want ErrActionInvalidGoal", err)
	}
	var other dds.GoalID
	other[0] = 0xFF
	if dds.CompareGoalIDs(id, other) == 0 {
		t.Fatalf("distinct goals compare equal")
	}
	sorted := dds.SortGoalIDs([]dds.GoalID{other, id, zero})
	if !sorted[0].Equal(zero) {
		t.Fatalf("sorted first is not zero")
	}
	a := dds.GoalSlot{ID: id, Generation: 2}
	b := dds.GoalSlot{ID: other, Generation: 2}
	winner := dds.PickPreemptWinner(a, b)
	if !winner.ID.Equal(id) {
		t.Fatalf("tie winner is not smaller id")
	}
	c := dds.GoalSlot{ID: zero, Generation: 9}
	winner = dds.PickPreemptWinner(a, c)
	if !winner.ID.Equal(zero) {
		t.Fatalf("higher generation did not win")
	}
}

// TestActionSequencePairing checks service GUID plus sequence pairing.
//
//nolint:cyclop // test pins sequence, request ID, and marshal pairing checks in one run, splitting would obscure intent
func TestActionSequencePairing(t *testing.T) {
	t.Parallel()
	if err := dds.ValidateActionSequence(0); !errors.Is(err, dds.ErrActionInvalidSequence) {
		t.Fatalf("seq 0 = %v, want ErrActionInvalidSequence", err)
	}
	if err := dds.ValidateActionSequence(dds.FirstActionSequence); err != nil {
		t.Fatalf("first seq: %v", err)
	}
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	req := dds.ActionRequestID{ClientGUID: guid, Sequence: 7}
	if err := dds.ValidateActionRequestID(req); err != nil {
		t.Fatalf("validate req: %v", err)
	}
	if err := dds.ValidateActionRequestID(dds.ActionRequestID{ClientGUID: guid}); !errors.Is(err, dds.ErrActionInvalidSequence) {
		t.Fatalf("zero seq req succeeded, want error")
	}
	payload := []byte{0x01, 0x02, 0x03}
	wire := dds.MarshalActionServiceRequest(payload, req, true)
	gotPayload, gotID, le, err := dds.ParseActionServiceRequest(wire)
	if err != nil {
		t.Fatalf("parse service request: %v", err)
	}
	if !le {
		t.Fatalf("endian is not little")
	}
	if !dds.IsPairedReply(req, gotID) {
		t.Fatalf("request and parsed reply do not pair")
	}
	if len(gotPayload) == 0 || gotPayload[0] != 0x01 {
		t.Fatalf("payload = %v, want leading 0x01", gotPayload)
	}
	other := dds.ActionRequestID{ClientGUID: guid, Sequence: 8}
	if dds.IsPairedReply(req, other) {
		t.Fatalf("distinct sequences pair")
	}
	payload[0] = 0xFF
	if gotPayload[0] == 0xFF {
		t.Fatalf("marshal aliased caller payload")
	}
	if _, _, _, err := dds.ParseActionServiceRequest([]byte{0x00}); err == nil {
		t.Fatalf("short service sample succeeded, want error")
	}
}

// TestActionQoSFanout checks dual QoS fanout for services plus feedback plus status.
//
//nolint:cyclop // test pins fanout, validate, and reject checks in one run, splitting would obscure intent
func TestActionQoSFanout(t *testing.T) {
	t.Parallel()
	fanout := dds.DefaultActionQoSFanout()
	if err := fanout.Validate(); err != nil {
		t.Fatalf("default fanout: %v", err)
	}
	if fanout.SendGoal.Request != fanout.SendGoal.Reply {
		t.Fatalf("send goal sides differ")
	}
	if fanout.CancelGoal.Request != fanout.CancelGoal.Reply {
		t.Fatalf("cancel sides differ")
	}
	if fanout.GetResult.Request != fanout.GetResult.Reply {
		t.Fatalf("result sides differ")
	}
	if fanout.Status.Durability != dds.ActionTransientLocal {
		t.Fatalf("status durability = %v, want transient_local", fanout.Status.Durability)
	}
	if fanout.Status.Depth != dds.ActionStatusHistoryDepth {
		t.Fatalf("status depth = %d, want 1", fanout.Status.Depth)
	}
	if fanout.Feedback.Reliability != dds.ActionBestEffort {
		t.Fatalf("feedback reliability = %v, want best_effort", fanout.Feedback.Reliability)
	}
	base := dds.DefaultActionServiceQoS()
	dual := dds.FanoutServiceQoS(base)
	if dual.Request != base || dual.Reply != base {
		t.Fatalf("service fanout did not duplicate base")
	}
	custom := dds.FanoutActionQoS(base, dds.DefaultActionFeedbackQoS(), dds.DefaultActionStatusQoS())
	if err := custom.Validate(); err != nil {
		t.Fatalf("custom fanout: %v", err)
	}
	bad := dds.ActionQoS{Reliability: dds.ActionReliability(9)}
	if err := bad.Validate(); !errors.Is(err, dds.ErrActionInvalidQoS) {
		t.Fatalf("bad qos = %v, want ErrActionInvalidQoS", err)
	}
	zeroDepth := dds.ActionQoS{Reliability: dds.ActionReliable, Durability: dds.ActionVolatile, History: dds.ActionKeepLast}
	if err := zeroDepth.Validate(); !errors.Is(err, dds.ErrActionInvalidQoS) {
		t.Fatalf("zero depth = %v, want ErrActionInvalidQoS", err)
	}
}
