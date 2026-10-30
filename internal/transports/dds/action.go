// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/podomy/weld/internal/types"
)

// DDS action wire mapping for ROS 2 actions.
//
// Scope is translation only. Core owns goal admission, cancel propagation,
// feedback buffering, result caching, and terminal guarantees. This file
// maps ROS action names to standard DDS topics and types, exposes goal
// status and cancel codes with terminal and preempt helpers, pairs service
// sequences with goal IDs, and fans out QoS where the wire needs two
// entities for one logical profile. It implements no state machine and
// retains no goal state.
//
// Standard wire shape follows the ROS 2 action design. One action named
// <base> uses three services and two topics on ROS names:
// <base>/_action/send_goal, <base>/_action/cancel_goal,
// <base>/_action/get_result, <base>/_action/feedback, and
// <base>/_action/status. DDS topics reuse the service mapping with
// rq and rr prefixes plus Request and Reply suffixes for the three
// services and rt prefix for feedback and status. DDS types use the
// ::dds_:: marker with per action SendGoal, GetResult, and FeedbackMessage
// suffixes plus fixed action_msgs CancelGoal and GoalStatusArray types.
// Status values follow the ROS GoalStatus enumeration with three active
// and three terminal states. Cancel follows the empty goal plus zero
// stamp policy. Sequence pairing uses the service envelope client GUID
// plus sequence for request and reply correlation and the 16 byte goal
// UUID for feedback, result, and status correlation. QoS fanout duplicates
// one logical profile to request and reply entities for services and
// keeps distinct status and feedback profiles per the standard defaults.

// Action exchange tokens on ROS names.
const (
	// ActionToken is the hidden token separating the action base from exchanges.
	ActionToken = "_action"
	// ActionSendGoal is the send goal service exchange.
	ActionSendGoal = "send_goal"
	// ActionCancelGoal is the cancel goal service exchange.
	ActionCancelGoal = "cancel_goal"
	// ActionGetResult is the get result service exchange.
	ActionGetResult = "get_result"
	// ActionFeedback is the feedback topic exchange.
	ActionFeedback = "feedback"
	// ActionStatus is the status topic exchange.
	ActionStatus = "status"
)

// Goal status values on the standard wire.
const (
	// GoalStatusUnknown is the unset status, never active nor terminal.
	GoalStatusUnknown int8 = 0
	// GoalStatusAccepted is an accepted goal awaiting execution.
	GoalStatusAccepted int8 = 1
	// GoalStatusExecuting is a goal currently executing.
	GoalStatusExecuting int8 = 2
	// GoalStatusCanceling is a goal the server agreed to cancel.
	GoalStatusCanceling int8 = 3
	// GoalStatusSucceeded is a terminal success.
	GoalStatusSucceeded int8 = 4
	// GoalStatusCanceled is a terminal cancel after external request.
	GoalStatusCanceled int8 = 5
	// GoalStatusAborted is a terminal abort without external request.
	GoalStatusAborted int8 = 6
)

// Cancel return codes on the standard wire.
const (
	// CancelErrorNone reports the cancel request was accepted.
	CancelErrorNone int8 = 0
	// CancelErrorRejected reports the cancel request was rejected.
	CancelErrorRejected int8 = 1
	// CancelErrorUnknownGoal reports the goal ID is unknown.
	CancelErrorUnknownGoal int8 = 2
	// CancelErrorTerminated reports the goal already terminated.
	CancelErrorTerminated int8 = 3
)

// Fixed DDS types for generic action exchanges.
const (
	// CancelGoalRequestDDSType is the DDS type for cancel requests.
	CancelGoalRequestDDSType = "action_msgs::srv::dds_::CancelGoal_Request_"
	// CancelGoalResponseDDSType is the DDS type for cancel responses.
	CancelGoalResponseDDSType = "action_msgs::srv::dds_::CancelGoal_Response_"
	// GoalStatusArrayDDSType is the DDS type for status arrays.
	GoalStatusArrayDDSType = "action_msgs::msg::dds_::GoalStatusArray_"
	// GoalInfoDDSType is the DDS type for goal info entries.
	GoalInfoDDSType = "action_msgs::msg::dds_::GoalInfo_"
)

// Action QoS depth defaults on the wire.
const (
	// ActionServiceHistoryDepth is the keep last depth for action services.
	ActionServiceHistoryDepth uint32 = 10
	// ActionStatusHistoryDepth is the keep last depth for status with transient local.
	ActionStatusHistoryDepth uint32 = 1
	// ActionFeedbackHistoryDepth is the keep last depth for feedback.
	ActionFeedbackHistoryDepth uint32 = 10
)

// FirstActionSequence is the first service sequence for action exchanges.
const FirstActionSequence uint64 = 1

// Sentinel errors for action wire mapping failures.
var (
	// ErrActionInvalidName reports a malformed ROS action name.
	ErrActionInvalidName = errors.New("dds: invalid action name")
	// ErrActionInvalidType reports a malformed action IDL name.
	ErrActionInvalidType = errors.New("dds: invalid action type")
	// ErrActionInvalidGoal reports a malformed goal ID.
	ErrActionInvalidGoal = errors.New("dds: invalid action goal")
	// ErrActionInvalidStatus reports an unknown goal status value.
	ErrActionInvalidStatus = errors.New("dds: invalid action status")
	// ErrActionInvalidCancel reports an unknown cancel return code.
	ErrActionInvalidCancel = errors.New("dds: invalid action cancel code")
	// ErrActionInvalidSequence reports a zero service sequence.
	ErrActionInvalidSequence = errors.New("dds: invalid action sequence")
	// ErrActionInvalidQoS reports a malformed action QoS profile.
	ErrActionInvalidQoS = errors.New("dds: invalid action qos")
)

// GoalID is the 16 byte UUID identifying one action goal.
type GoalID [16]byte

// IsZero reports whether the goal ID is the empty cancel all value.
func (g GoalID) IsZero() bool {
	return g == GoalID{}
}

// Equal reports whether two goal IDs match.
func (g GoalID) Equal(other GoalID) bool {
	return g == other
}

// CompareGoalIDs orders goal IDs lexicographically for deterministic preempt.
func CompareGoalIDs(a, b GoalID) int {
	return bytes.Compare(a[:], b[:])
}

// SortGoalIDs returns a sorted copy of the input without mutating it.
func SortGoalIDs(ids []GoalID) []GoalID {
	out := append([]GoalID(nil), ids...)
	slices.SortFunc(out, CompareGoalIDs)
	return out
}

// GoalIDFromBytes builds a goal ID from 16 bytes with a copy.
func GoalIDFromBytes(b []byte) (GoalID, error) {
	var id GoalID
	if len(b) != len(id) {
		return id, fmt.Errorf("dds: goal id length %d, want %d: %w", len(b), len(id), ErrActionInvalidGoal)
	}
	copy(id[:], b)
	return id, nil
}

// ActionRequestID pairs a service client GUID with a service sequence.
type ActionRequestID struct {
	// ClientGUID is the 8 byte service client identity, never swapped.
	ClientGUID [8]byte
	// Sequence is the per client service sequence, never zero.
	Sequence uint64
}

// ValidateActionSequence rejects zero as an invalid service sequence.
func ValidateActionSequence(seq uint64) error {
	if seq == 0 {
		return fmt.Errorf("dds: action sequence 0: %w", ErrActionInvalidSequence)
	}
	return nil
}

// ValidateActionRequestID rejects a zero sequence pairing ID.
func ValidateActionRequestID(id ActionRequestID) error {
	err := ValidateActionSequence(id.Sequence)
	if err != nil {
		return err
	}
	return nil
}

// IsPairedReply reports whether a reply pairs with its request.
func IsPairedReply(req, rep ActionRequestID) bool {
	return req.ClientGUID == rep.ClientGUID && req.Sequence == rep.Sequence
}

// GoalPaired reports whether two goal IDs refer to the same goal.
func GoalPaired(a, b GoalID) bool {
	return a.Equal(b)
}

// MarshalActionServiceRequest wraps payload with header plus GUID and sequence.
func MarshalActionServiceRequest(payload []byte, id ActionRequestID, littleEndian bool) []byte {
	cp := append([]byte(nil), payload...)
	return types.MarshalServiceMessage(cp, id.ClientGUID, id.Sequence, littleEndian)
}

// ParseActionServiceRequest splits a service sample into payload and pairing ID.
func ParseActionServiceRequest(data []byte) ([]byte, ActionRequestID, bool, error) {
	var id ActionRequestID
	var metrics types.Metrics
	payload, guid, seq, le, err := types.UnmarshalServiceMessage(data, &metrics)
	if err != nil {
		return nil, id, false, fmt.Errorf("dds: parse action service request: %w", err)
	}
	id.ClientGUID = guid
	id.Sequence = seq
	err = ValidateActionRequestID(id)
	if err != nil {
		return nil, ActionRequestID{}, false, err
	}
	cp := append([]byte(nil), payload...)
	return cp, id, le, nil
}

// GoalSlot names one preempt candidate with its generation.
type GoalSlot struct {
	// ID is the goal UUID.
	ID GoalID
	// Generation orders goals from the same or distinct clients.
	Generation uint64
}

// PickPreemptWinner selects the deterministic preempt winner.
func PickPreemptWinner(a, b GoalSlot) GoalSlot {
	if a.Generation != b.Generation {
		if a.Generation > b.Generation {
			return a
		}
		return b
	}
	if CompareGoalIDs(a.ID, b.ID) <= 0 {
		return a
	}
	return b
}

// AllowsPreempt reports whether a status admits preempt by a new goal.
func AllowsPreempt(status int8) bool {
	return status == GoalStatusAccepted || status == GoalStatusExecuting
}

// PreemptTerminal maps a preempt outcome to its terminal wire status.
func PreemptTerminal(canceled bool) int8 {
	if canceled {
		return GoalStatusCanceled
	}
	return GoalStatusAborted
}

// ValidateGoalStatus rejects unknown status values.
func ValidateGoalStatus(status int8) error {
	switch status {
	case GoalStatusUnknown,
		GoalStatusAccepted,
		GoalStatusExecuting,
		GoalStatusCanceling,
		GoalStatusSucceeded,
		GoalStatusCanceled,
		GoalStatusAborted:
		return nil
	default:
		return fmt.Errorf("dds: goal status %d: %w", status, ErrActionInvalidStatus)
	}
}

// IsTerminalGoalStatus reports whether a status ends the goal.
func IsTerminalGoalStatus(status int8) bool {
	return status == GoalStatusSucceeded || status == GoalStatusCanceled || status == GoalStatusAborted
}

// IsActiveGoalStatus reports whether a status keeps the goal in flight.
func IsActiveGoalStatus(status int8) bool {
	return status == GoalStatusAccepted || status == GoalStatusExecuting || status == GoalStatusCanceling
}

// GoalStatusName renders the status in lowercase wire form.
func GoalStatusName(status int8) string {
	switch status {
	case GoalStatusUnknown:
		return "unknown"
	case GoalStatusAccepted:
		return "accepted"
	case GoalStatusExecuting:
		return "executing"
	case GoalStatusCanceling:
		return "canceling"
	case GoalStatusSucceeded:
		return "succeeded"
	case GoalStatusCanceled:
		return "canceled"
	case GoalStatusAborted:
		return "aborted"
	default:
		return "invalid"
	}
}

// ValidateCancelCode rejects unknown cancel return codes.
func ValidateCancelCode(code int8) error {
	switch code {
	case CancelErrorNone,
		CancelErrorRejected,
		CancelErrorUnknownGoal,
		CancelErrorTerminated:
		return nil
	default:
		return fmt.Errorf("dds: cancel code %d: %w", code, ErrActionInvalidCancel)
	}
}

// CancelCodeName renders the cancel code in lowercase wire form.
func CancelCodeName(code int8) string {
	switch code {
	case CancelErrorNone:
		return "none"
	case CancelErrorRejected:
		return "rejected"
	case CancelErrorUnknownGoal:
		return "unknown_goal"
	case CancelErrorTerminated:
		return "terminated"
	default:
		return "invalid"
	}
}

// CancelScope names which goals a cancel request covers.
type CancelScope int

// Cancel scopes per the empty goal plus zero stamp policy.
const (
	// CancelScopeAll cancels every goal when ID is empty and stamp is zero.
	CancelScopeAll CancelScope = iota
	// CancelScopeBefore cancels goals accepted at or before the stamp.
	CancelScopeBefore
	// CancelScopeSingle cancels one goal ID regardless of stamp.
	CancelScopeSingle
	// CancelScopeSingleAndBefore cancels one goal plus goals before the stamp.
	CancelScopeSingleAndBefore
)

// String renders the cancel scope in lowercase form.
func (s CancelScope) String() string {
	switch s {
	case CancelScopeAll:
		return "all"
	case CancelScopeBefore:
		return "before"
	case CancelScopeSingle:
		return "single"
	case CancelScopeSingleAndBefore:
		return "single_and_before"
	default:
		return "invalid"
	}
}

// ClassifyCancelRequest maps goal ID plus stamp to its cancel scope.
func ClassifyCancelRequest(goal GoalID, sec int32, nsec uint32) CancelScope {
	empty := goal.IsZero()
	zeroStamp := sec == 0 && nsec == 0
	switch {
	case empty && zeroStamp:
		return CancelScopeAll
	case empty && !zeroStamp:
		return CancelScopeBefore
	case !empty && zeroStamp:
		return CancelScopeSingle
	default:
		return CancelScopeSingleAndBefore
	}
}

// cleanActionBase trims an action name to its stripped base.
func cleanActionBase(action string) (string, error) {
	trimmed := strings.TrimSpace(action)
	if trimmed == "" {
		return "", fmt.Errorf("dds: action name is empty: %w", ErrActionInvalidName)
	}
	if strings.Contains(trimmed, "//") {
		return "", fmt.Errorf("dds: action name %q has an empty segment: %w", action, ErrActionInvalidName)
	}
	if strings.HasSuffix(trimmed, "/") {
		return "", fmt.Errorf("dds: action name %q has a trailing slash: %w", action, ErrActionInvalidName)
	}
	if strings.ContainsAny(trimmed, " \t\n\r") {
		return "", fmt.Errorf("dds: action name %q has whitespace: %w", action, ErrActionInvalidName)
	}
	stripped := strings.Trim(trimmed, "/")
	if stripped == "" {
		return "", fmt.Errorf("dds: action name %q is empty: %w", action, ErrActionInvalidName)
	}
	if slices.Contains(strings.Split(stripped, "/"), "") {
		return "", fmt.Errorf("dds: action name %q has an empty segment: %w", action, ErrActionInvalidName)
	}
	return stripped, nil
}

// actionROSExchangeName joins a stripped base with the action token and exchange.
func actionROSExchangeName(stripped, exchange string) string {
	return "/" + stripped + "/" + ActionToken + "/" + exchange
}

// SendGoalServiceName returns the ROS service name for send goal.
func SendGoalServiceName(action string) (string, error) {
	stripped, err := cleanActionBase(action)
	if err != nil {
		return "", err
	}
	return actionROSExchangeName(stripped, ActionSendGoal), nil
}

// CancelGoalServiceName returns the ROS service name for cancel goal.
func CancelGoalServiceName(action string) (string, error) {
	stripped, err := cleanActionBase(action)
	if err != nil {
		return "", err
	}
	return actionROSExchangeName(stripped, ActionCancelGoal), nil
}

// GetResultServiceName returns the ROS service name for get result.
func GetResultServiceName(action string) (string, error) {
	stripped, err := cleanActionBase(action)
	if err != nil {
		return "", err
	}
	return actionROSExchangeName(stripped, ActionGetResult), nil
}

// FeedbackTopicName returns the ROS topic name for feedback.
func FeedbackTopicName(action string) (string, error) {
	stripped, err := cleanActionBase(action)
	if err != nil {
		return "", err
	}
	return actionROSExchangeName(stripped, ActionFeedback), nil
}

// StatusTopicName returns the ROS topic name for status.
func StatusTopicName(action string) (string, error) {
	stripped, err := cleanActionBase(action)
	if err != nil {
		return "", err
	}
	return actionROSExchangeName(stripped, ActionStatus), nil
}

// SendGoalRequestTopic returns the DDS request topic for send goal.
func SendGoalRequestTopic(action string, avoidConventions bool) (string, error) {
	ros, err := SendGoalServiceName(action)
	if err != nil {
		return "", err
	}
	ddsTopic, err := RequestTopic(ros, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("dds: send goal request topic: %w", err)
	}
	return ddsTopic, nil
}

// SendGoalReplyTopic returns the DDS reply topic for send goal.
func SendGoalReplyTopic(action string, avoidConventions bool) (string, error) {
	ros, err := SendGoalServiceName(action)
	if err != nil {
		return "", err
	}
	ddsTopic, err := ReplyTopic(ros, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("dds: send goal reply topic: %w", err)
	}
	return ddsTopic, nil
}

// CancelGoalRequestTopic returns the DDS request topic for cancel goal.
func CancelGoalRequestTopic(action string, avoidConventions bool) (string, error) {
	ros, err := CancelGoalServiceName(action)
	if err != nil {
		return "", err
	}
	ddsTopic, err := RequestTopic(ros, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("dds: cancel goal request topic: %w", err)
	}
	return ddsTopic, nil
}

// CancelGoalReplyTopic returns the DDS reply topic for cancel goal.
func CancelGoalReplyTopic(action string, avoidConventions bool) (string, error) {
	ros, err := CancelGoalServiceName(action)
	if err != nil {
		return "", err
	}
	ddsTopic, err := ReplyTopic(ros, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("dds: cancel goal reply topic: %w", err)
	}
	return ddsTopic, nil
}

// GetResultRequestTopic returns the DDS request topic for get result.
func GetResultRequestTopic(action string, avoidConventions bool) (string, error) {
	ros, err := GetResultServiceName(action)
	if err != nil {
		return "", err
	}
	ddsTopic, err := RequestTopic(ros, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("dds: get result request topic: %w", err)
	}
	return ddsTopic, nil
}

// GetResultReplyTopic returns the DDS reply topic for get result.
func GetResultReplyTopic(action string, avoidConventions bool) (string, error) {
	ros, err := GetResultServiceName(action)
	if err != nil {
		return "", err
	}
	ddsTopic, err := ReplyTopic(ros, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("dds: get result reply topic: %w", err)
	}
	return ddsTopic, nil
}

// ActionFeedbackTopic returns the DDS topic for feedback.
func ActionFeedbackTopic(action string, avoidConventions bool) (string, error) {
	ros, err := FeedbackTopicName(action)
	if err != nil {
		return "", err
	}
	ddsTopic, err := TopicName(ros, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("dds: feedback topic: %w", err)
	}
	return ddsTopic, nil
}

// ActionStatusTopic returns the DDS topic for status.
func ActionStatusTopic(action string, avoidConventions bool) (string, error) {
	ros, err := StatusTopicName(action)
	if err != nil {
		return "", err
	}
	ddsTopic, err := TopicName(ros, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("dds: status topic: %w", err)
	}
	return ddsTopic, nil
}

// ActionDDSTopics carries the eight DDS topics for one action.
type ActionDDSTopics struct {
	// SendGoalRequest is the send goal request DDS topic.
	SendGoalRequest string
	// SendGoalReply is the send goal reply DDS topic.
	SendGoalReply string
	// CancelGoalRequest is the cancel goal request DDS topic.
	CancelGoalRequest string
	// CancelGoalReply is the cancel goal reply DDS topic.
	CancelGoalReply string
	// GetResultRequest is the get result request DDS topic.
	GetResultRequest string
	// GetResultReply is the get result reply DDS topic.
	GetResultReply string
	// Feedback is the feedback DDS topic.
	Feedback string
	// Status is the status DDS topic.
	Status string
}

// ActionDDSTopicsFor returns the eight DDS topics for one action.
func ActionDDSTopicsFor(action string, avoidConventions bool) (ActionDDSTopics, error) {
	var out ActionDDSTopics
	sendReq, err := SendGoalRequestTopic(action, avoidConventions)
	if err != nil {
		return out, err
	}
	sendRep, err := SendGoalReplyTopic(action, avoidConventions)
	if err != nil {
		return out, err
	}
	cancelReq, err := CancelGoalRequestTopic(action, avoidConventions)
	if err != nil {
		return out, err
	}
	cancelRep, err := CancelGoalReplyTopic(action, avoidConventions)
	if err != nil {
		return out, err
	}
	resultReq, err := GetResultRequestTopic(action, avoidConventions)
	if err != nil {
		return out, err
	}
	resultRep, err := GetResultReplyTopic(action, avoidConventions)
	if err != nil {
		return out, err
	}
	feedback, err := ActionFeedbackTopic(action, avoidConventions)
	if err != nil {
		return out, err
	}
	status, err := ActionStatusTopic(action, avoidConventions)
	if err != nil {
		return out, err
	}
	out = ActionDDSTopics{
		SendGoalRequest:   sendReq,
		SendGoalReply:     sendRep,
		CancelGoalRequest: cancelReq,
		CancelGoalReply:   cancelRep,
		GetResultRequest:  resultReq,
		GetResultReply:    resultRep,
		Feedback:          feedback,
		Status:            status,
	}
	return out, nil
}

// SplitActionROSName splits a ROS action exchange into base plus exchange.
func SplitActionROSName(rosName string) (string, string, error) {
	trimmed := strings.TrimSpace(rosName)
	if trimmed == "" {
		return "", "", fmt.Errorf("dds: action ros name is empty: %w", ErrActionInvalidName)
	}
	if !strings.HasPrefix(trimmed, "/") {
		return "", "", fmt.Errorf("dds: action ros name %q misses leading slash: %w", rosName, ErrActionInvalidName)
	}
	suffixes := map[string]bool{
		ActionSendGoal:   true,
		ActionCancelGoal: true,
		ActionGetResult:  true,
		ActionFeedback:   true,
		ActionStatus:     true,
	}
	for exchange := range suffixes {
		suffix := "/" + ActionToken + "/" + exchange
		if base, found := strings.CutSuffix(trimmed, suffix); found {
			if base == "" || base == "/" {
				return "", "", fmt.Errorf("dds: action ros name %q has an empty base: %w", rosName, ErrActionInvalidName)
			}
			if strings.Contains(base[1:], "//") {
				return "", "", fmt.Errorf("dds: action base %q has an empty segment: %w", base, ErrActionInvalidName)
			}
			return base, exchange, nil
		}
	}
	return "", "", fmt.Errorf("dds: action ros name %q misses %q exchange: %w", rosName, ActionToken, ErrActionInvalidName)
}

// ParseActionDDSTopic maps a DDS topic back to action base plus exchange.
func ParseActionDDSTopic(ddsTopic string) (string, string, TopicKind, error) {
	ros, kind, err := ParseTopicName(ddsTopic)
	if err != nil {
		return "", "", "", fmt.Errorf("dds: parse action dds topic: %w", err)
	}
	base, exchange, err := SplitActionROSName(ros)
	if err != nil {
		return "", "", "", err
	}
	return base, exchange, kind, nil
}

// splitActionIDL validates an action IDL base and returns namespace plus name.
func splitActionIDL(idlBase string) (string, string, error) {
	parts := strings.Split(idlBase, "__")
	if len(parts) < 3 {
		return "", "", fmt.Errorf("dds: action idl %q needs at least pkg__action__Name: %w", idlBase, ErrActionInvalidType)
	}
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return "", "", fmt.Errorf("dds: action idl %q has an empty segment: %w", idlBase, ErrActionInvalidType)
		}
	}
	if parts[len(parts)-2] != "action" {
		return "", "", fmt.Errorf("dds: action idl %q misses action group: %w", idlBase, ErrActionInvalidType)
	}
	ns := strings.Join(parts[:len(parts)-1], "::")
	name := parts[len(parts)-1]
	return ns, name, nil
}

// actionDDSType builds a per action DDS type with the given suffix.
func actionDDSType(idlBase, suffix string) (string, error) {
	ns, name, err := splitActionIDL(idlBase)
	if err != nil {
		return "", err
	}
	out := ns + DDSMarker + name + suffix
	_, err = DemangleTypeName(out)
	if err != nil {
		return "", fmt.Errorf("dds: action type round trip: %w", err)
	}
	return out, nil
}

// ActionSendGoalRequestType returns the DDS request type for send goal.
func ActionSendGoalRequestType(idlBase string) (string, error) {
	out, err := actionDDSType(idlBase, "_SendGoal_Request_")
	if err != nil {
		return "", fmt.Errorf("dds: send goal request type: %w", err)
	}
	return out, nil
}

// ActionSendGoalResponseType returns the DDS response type for send goal.
func ActionSendGoalResponseType(idlBase string) (string, error) {
	out, err := actionDDSType(idlBase, "_SendGoal_Response_")
	if err != nil {
		return "", fmt.Errorf("dds: send goal response type: %w", err)
	}
	return out, nil
}

// ActionGetResultRequestType returns the DDS request type for get result.
func ActionGetResultRequestType(idlBase string) (string, error) {
	out, err := actionDDSType(idlBase, "_GetResult_Request_")
	if err != nil {
		return "", fmt.Errorf("dds: get result request type: %w", err)
	}
	return out, nil
}

// ActionGetResultResponseType returns the DDS response type for get result.
func ActionGetResultResponseType(idlBase string) (string, error) {
	out, err := actionDDSType(idlBase, "_GetResult_Response_")
	if err != nil {
		return "", fmt.Errorf("dds: get result response type: %w", err)
	}
	return out, nil
}

// ActionFeedbackMessageType returns the DDS type for feedback messages.
func ActionFeedbackMessageType(idlBase string) (string, error) {
	out, err := actionDDSType(idlBase, "_FeedbackMessage_")
	if err != nil {
		return "", fmt.Errorf("dds: feedback message type: %w", err)
	}
	return out, nil
}

// ActionGoalType returns the DDS type for the action goal structure.
func ActionGoalType(idlBase string) (string, error) {
	out, err := actionDDSType(idlBase, "_Goal_")
	if err != nil {
		return "", fmt.Errorf("dds: action goal type: %w", err)
	}
	return out, nil
}

// ActionResultType returns the DDS type for the action result structure.
func ActionResultType(idlBase string) (string, error) {
	out, err := actionDDSType(idlBase, "_Result_")
	if err != nil {
		return "", fmt.Errorf("dds: action result type: %w", err)
	}
	return out, nil
}

// ActionFeedbackType returns the DDS type for the action feedback structure.
func ActionFeedbackType(idlBase string) (string, error) {
	out, err := actionDDSType(idlBase, "_Feedback_")
	if err != nil {
		return "", fmt.Errorf("dds: action feedback type: %w", err)
	}
	return out, nil
}

// ActionDDSTypes carries the per action DDS types plus fixed generic types.
type ActionDDSTypes struct {
	// SendGoalRequest is the send goal request DDS type.
	SendGoalRequest string
	// SendGoalResponse is the send goal response DDS type.
	SendGoalResponse string
	// GetResultRequest is the get result request DDS type.
	GetResultRequest string
	// GetResultResponse is the get result response DDS type.
	GetResultResponse string
	// FeedbackMessage is the feedback message DDS type.
	FeedbackMessage string
	// Goal is the goal structure DDS type.
	Goal string
	// Result is the result structure DDS type.
	Result string
	// Feedback is the feedback structure DDS type.
	Feedback string
	// CancelGoalRequest is the fixed cancel request DDS type.
	CancelGoalRequest string
	// CancelGoalResponse is the fixed cancel response DDS type.
	CancelGoalResponse string
	// StatusArray is the fixed status array DDS type.
	StatusArray string
}

// ActionDDSTypesFor returns the DDS types for one action IDL base.
func ActionDDSTypesFor(idlBase string) (ActionDDSTypes, error) {
	var out ActionDDSTypes
	sendReq, err := ActionSendGoalRequestType(idlBase)
	if err != nil {
		return out, err
	}
	sendRsp, err := ActionSendGoalResponseType(idlBase)
	if err != nil {
		return out, err
	}
	resultReq, err := ActionGetResultRequestType(idlBase)
	if err != nil {
		return out, err
	}
	resultRsp, err := ActionGetResultResponseType(idlBase)
	if err != nil {
		return out, err
	}
	fbMsg, err := ActionFeedbackMessageType(idlBase)
	if err != nil {
		return out, err
	}
	goal, err := ActionGoalType(idlBase)
	if err != nil {
		return out, err
	}
	result, err := ActionResultType(idlBase)
	if err != nil {
		return out, err
	}
	feedback, err := ActionFeedbackType(idlBase)
	if err != nil {
		return out, err
	}
	out = ActionDDSTypes{
		SendGoalRequest:    sendReq,
		SendGoalResponse:   sendRsp,
		GetResultRequest:   resultReq,
		GetResultResponse:  resultRsp,
		FeedbackMessage:    fbMsg,
		Goal:               goal,
		Result:             result,
		Feedback:           feedback,
		CancelGoalRequest:  CancelGoalRequestDDSType,
		CancelGoalResponse: CancelGoalResponseDDSType,
		StatusArray:        GoalStatusArrayDDSType,
	}
	return out, nil
}

// ActionReliability names the reliability kind for action exchanges.
type ActionReliability int

// Action reliability kinds.
const (
	// ActionReliable retries until delivery for services and status.
	ActionReliable ActionReliability = iota
	// ActionBestEffort sends once without retry for feedback.
	ActionBestEffort
)

// String renders the reliability in lowercase form.
func (r ActionReliability) String() string {
	switch r {
	case ActionReliable:
		return "reliable"
	case ActionBestEffort:
		return "best_effort"
	default:
		return "invalid"
	}
}

// ActionDurability names the durability kind for action exchanges.
type ActionDurability int

// Action durability kinds.
const (
	// ActionVolatile keeps no history for late joiners.
	ActionVolatile ActionDurability = iota
	// ActionTransientLocal keeps history for late joiners.
	ActionTransientLocal
)

// String renders the durability in lowercase form.
func (d ActionDurability) String() string {
	switch d {
	case ActionVolatile:
		return "volatile"
	case ActionTransientLocal:
		return "transient_local"
	default:
		return "invalid"
	}
}

// ActionHistory names the history kind for action exchanges.
type ActionHistory int

// Action history kinds.
const (
	// ActionKeepLast keeps the newest depth samples.
	ActionKeepLast ActionHistory = iota
	// ActionKeepAll keeps every sample up to the retention bound.
	ActionKeepAll
)

// String renders the history in lowercase form.
func (h ActionHistory) String() string {
	switch h {
	case ActionKeepLast:
		return "keep_last"
	case ActionKeepAll:
		return "keep_all"
	default:
		return "invalid"
	}
}

// ActionQoS is one logical QoS profile for action exchanges.
type ActionQoS struct {
	// Reliability is reliable or best effort.
	Reliability ActionReliability
	// Durability is volatile or transient local.
	Durability ActionDurability
	// History is keep last or keep all.
	History ActionHistory
	// Depth is meaningful for keep last only. Zero means not set.
	Depth uint32
}

// Validate checks an action QoS profile for usable values.
func (q ActionQoS) Validate() error {
	switch q.Reliability {
	case ActionReliable, ActionBestEffort:
	default:
		return fmt.Errorf("dds: action qos reliability %d: %w", int(q.Reliability), ErrActionInvalidQoS)
	}
	switch q.Durability {
	case ActionVolatile, ActionTransientLocal:
	default:
		return fmt.Errorf("dds: action qos durability %d: %w", int(q.Durability), ErrActionInvalidQoS)
	}
	switch q.History {
	case ActionKeepLast, ActionKeepAll:
	default:
		return fmt.Errorf("dds: action qos history %d: %w", int(q.History), ErrActionInvalidQoS)
	}
	if q.History == ActionKeepLast && q.Depth == 0 {
		return fmt.Errorf("dds: action qos keep last depth 0: %w", ErrActionInvalidQoS)
	}
	return nil
}

// DefaultActionServiceQoS returns reliable volatile keep last 10 for services.
func DefaultActionServiceQoS() ActionQoS {
	return ActionQoS{
		Reliability: ActionReliable,
		Durability:  ActionVolatile,
		History:     ActionKeepLast,
		Depth:       ActionServiceHistoryDepth,
	}
}

// DefaultActionStatusQoS returns reliable transient local keep last 1 for status.
func DefaultActionStatusQoS() ActionQoS {
	return ActionQoS{
		Reliability: ActionReliable,
		Durability:  ActionTransientLocal,
		History:     ActionKeepLast,
		Depth:       ActionStatusHistoryDepth,
	}
}

// DefaultActionFeedbackQoS returns best effort volatile keep last 10 for feedback.
func DefaultActionFeedbackQoS() ActionQoS {
	return ActionQoS{
		Reliability: ActionBestEffort,
		Durability:  ActionVolatile,
		History:     ActionKeepLast,
		Depth:       ActionFeedbackHistoryDepth,
	}
}

// DualQoS carries the request and reply sides for one service.
type DualQoS struct {
	// Request is the request side profile.
	Request ActionQoS
	// Reply is the reply side profile.
	Reply ActionQoS
}

// Validate checks both sides of a dual QoS profile.
func (d DualQoS) Validate() error {
	err := d.Request.Validate()
	if err != nil {
		return fmt.Errorf("dds: dual qos request: %w", err)
	}
	err = d.Reply.Validate()
	if err != nil {
		return fmt.Errorf("dds: dual qos reply: %w", err)
	}
	return nil
}

// FanoutServiceQoS duplicates one logical profile to request and reply.
func FanoutServiceQoS(base ActionQoS) DualQoS {
	return DualQoS{Request: base, Reply: base}
}

// ActionQoSFanout carries the wire QoS for all action exchanges.
type ActionQoSFanout struct {
	// SendGoal carries the send goal request and reply profiles.
	SendGoal DualQoS
	// CancelGoal carries the cancel goal request and reply profiles.
	CancelGoal DualQoS
	// GetResult carries the get result request and reply profiles.
	GetResult DualQoS
	// Feedback carries the feedback topic profile.
	Feedback ActionQoS
	// Status carries the status topic profile.
	Status ActionQoS
}

// Validate checks every profile in the fanout.
func (f ActionQoSFanout) Validate() error {
	for _, named := range []struct {
		name string
		dual DualQoS
	}{
		{"send_goal", f.SendGoal},
		{"cancel_goal", f.CancelGoal},
		{"get_result", f.GetResult},
	} {
		err := named.dual.Validate()
		if err != nil {
			return fmt.Errorf("dds: action qos %s: %w", named.name, err)
		}
	}
	err := f.Feedback.Validate()
	if err != nil {
		return fmt.Errorf("dds: action qos feedback: %w", err)
	}
	err = f.Status.Validate()
	if err != nil {
		return fmt.Errorf("dds: action qos status: %w", err)
	}
	return nil
}

// FanoutActionQoS fans one service profile plus feedback and status into wire QoS.
func FanoutActionQoS(service, feedback, status ActionQoS) ActionQoSFanout {
	return ActionQoSFanout{
		SendGoal:   FanoutServiceQoS(service),
		CancelGoal: FanoutServiceQoS(service),
		GetResult:  FanoutServiceQoS(service),
		Feedback:   feedback,
		Status:     status,
	}
}

// DefaultActionQoSFanout returns the standard service plus feedback plus status fanout.
func DefaultActionQoSFanout() ActionQoSFanout {
	return FanoutActionQoS(DefaultActionServiceQoS(), DefaultActionFeedbackQoS(), DefaultActionStatusQoS())
}
