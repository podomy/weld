// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus

import (
	"fmt"

	"github.com/podomy/weld/internal/transports/dds"
)

// DDS action goal and request identities.
type (
	// GoalID is the 16 byte UUID identifying one action goal.
	GoalID = dds.GoalID
	// ActionRequestID pairs a service client GUID with a service sequence.
	ActionRequestID = dds.ActionRequestID
	// GoalSlot names one preempt candidate with its generation.
	GoalSlot = dds.GoalSlot
	// ActionDDSTopics carries the eight DDS topics for one action.
	ActionDDSTopics = dds.ActionDDSTopics
	// ActionDDSTypes carries the per action DDS types plus fixed generic types.
	ActionDDSTypes = dds.ActionDDSTypes
	// ActionQoS is one logical QoS profile for action exchanges.
	ActionQoS = dds.ActionQoS
	// DualQoS carries the request and reply sides for one service.
	DualQoS = dds.DualQoS
	// ActionQoSFanout carries the wire QoS for all action exchanges.
	ActionQoSFanout = dds.ActionQoSFanout
	// CancelScope names which goals a cancel request covers.
	CancelScope = dds.CancelScope
	// ActionReliability names the reliability kind for action exchanges.
	ActionReliability = dds.ActionReliability
	// ActionDurability names the durability kind for action exchanges.
	ActionDurability = dds.ActionDurability
	// ActionHistory names the history kind for action exchanges.
	ActionHistory = dds.ActionHistory
)

// DDS action exchange tokens on ROS names.
const (
	// ActionToken is the hidden token separating the action base from exchanges.
	ActionToken = dds.ActionToken
	// ActionSendGoal is the send goal service exchange.
	ActionSendGoal = dds.ActionSendGoal
	// ActionCancelGoal is the cancel goal service exchange.
	ActionCancelGoal = dds.ActionCancelGoal
	// ActionGetResult is the get result service exchange.
	ActionGetResult = dds.ActionGetResult
	// ActionFeedback is the feedback topic exchange.
	ActionFeedback = dds.ActionFeedback
	// ActionStatus is the status topic exchange.
	ActionStatus = dds.ActionStatus
)

// Goal status values on the standard wire.
const (
	// GoalStatusUnknown is the unset status, never active nor terminal.
	GoalStatusUnknown = dds.GoalStatusUnknown
	// GoalStatusAccepted is an accepted goal awaiting execution.
	GoalStatusAccepted = dds.GoalStatusAccepted
	// GoalStatusExecuting is a goal currently executing.
	GoalStatusExecuting = dds.GoalStatusExecuting
	// GoalStatusCanceling is a goal the server agreed to cancel.
	GoalStatusCanceling = dds.GoalStatusCanceling
	// GoalStatusSucceeded is a terminal success.
	GoalStatusSucceeded = dds.GoalStatusSucceeded
	// GoalStatusCanceled is a terminal cancel after external request.
	GoalStatusCanceled = dds.GoalStatusCanceled
	// GoalStatusAborted is a terminal abort without external request.
	GoalStatusAborted = dds.GoalStatusAborted
)

// Cancel return codes on the standard wire.
const (
	// CancelErrorNone reports the cancel request was accepted.
	CancelErrorNone = dds.CancelErrorNone
	// CancelErrorRejected reports the cancel request was rejected.
	CancelErrorRejected = dds.CancelErrorRejected
	// CancelErrorUnknownGoal reports the goal ID is unknown.
	CancelErrorUnknownGoal = dds.CancelErrorUnknownGoal
	// CancelErrorTerminated reports the goal already terminated.
	CancelErrorTerminated = dds.CancelErrorTerminated
)

// Fixed DDS types for generic action exchanges.
const (
	// CancelGoalRequestDDSType is the DDS type for cancel requests.
	CancelGoalRequestDDSType = dds.CancelGoalRequestDDSType
	// CancelGoalResponseDDSType is the DDS type for cancel responses.
	CancelGoalResponseDDSType = dds.CancelGoalResponseDDSType
	// GoalStatusArrayDDSType is the DDS type for status arrays.
	GoalStatusArrayDDSType = dds.GoalStatusArrayDDSType
	// GoalInfoDDSType is the DDS type for goal info entries.
	GoalInfoDDSType = dds.GoalInfoDDSType
)

// Action QoS depth defaults on the wire.
const (
	// ActionServiceHistoryDepth is the keep last depth for action services.
	ActionServiceHistoryDepth = dds.ActionServiceHistoryDepth
	// ActionStatusHistoryDepth is the keep last depth for status with transient local.
	ActionStatusHistoryDepth = dds.ActionStatusHistoryDepth
	// ActionFeedbackHistoryDepth is the keep last depth for feedback.
	ActionFeedbackHistoryDepth = dds.ActionFeedbackHistoryDepth
)

// FirstActionSequence is the first service sequence for action exchanges.
const FirstActionSequence = dds.FirstActionSequence

// Action reliability kinds.
const (
	// ActionReliable retries until delivery for services and status.
	ActionReliable = dds.ActionReliable
	// ActionBestEffort sends once without retry for feedback.
	ActionBestEffort = dds.ActionBestEffort
)

// Action durability kinds.
const (
	// ActionVolatile keeps no history for late joiners.
	ActionVolatile = dds.ActionVolatile
	// ActionTransientLocal keeps history for late joiners.
	ActionTransientLocal = dds.ActionTransientLocal
)

// Action history kinds.
const (
	// ActionKeepLast keeps the newest depth samples.
	ActionKeepLast = dds.ActionKeepLast
	// ActionKeepAll keeps every sample up to the retention bound.
	ActionKeepAll = dds.ActionKeepAll
)

// Cancel scopes per the empty goal plus zero stamp policy.
const (
	// CancelScopeInvalid is the zero value returned with an error.
	CancelScopeInvalid = dds.CancelScopeInvalid
	// CancelScopeAll cancels every goal when ID is empty and stamp is zero.
	CancelScopeAll = dds.CancelScopeAll
	// CancelScopeBefore cancels goals accepted at or before the stamp.
	CancelScopeBefore = dds.CancelScopeBefore
	// CancelScopeSingle cancels one goal ID regardless of stamp.
	CancelScopeSingle = dds.CancelScopeSingle
	// CancelScopeSingleAndBefore cancels one goal plus goals before the stamp.
	CancelScopeSingleAndBefore = dds.CancelScopeSingleAndBefore
)

// SendGoalServiceName returns the ROS service name for send goal.
func SendGoalServiceName(action string) (string, error) {
	name, err := dds.SendGoalServiceName(action)
	if err != nil {
		return "", fmt.Errorf("bus: dds send goal service name: %w", err)
	}

	return name, nil
}

// CancelGoalServiceName returns the ROS service name for cancel goal.
func CancelGoalServiceName(action string) (string, error) {
	name, err := dds.CancelGoalServiceName(action)
	if err != nil {
		return "", fmt.Errorf("bus: dds cancel goal service name: %w", err)
	}

	return name, nil
}

// GetResultServiceName returns the ROS service name for get result.
func GetResultServiceName(action string) (string, error) {
	name, err := dds.GetResultServiceName(action)
	if err != nil {
		return "", fmt.Errorf("bus: dds get result service name: %w", err)
	}

	return name, nil
}

// FeedbackTopicName returns the ROS topic name for feedback.
func FeedbackTopicName(action string) (string, error) {
	name, err := dds.FeedbackTopicName(action)
	if err != nil {
		return "", fmt.Errorf("bus: dds feedback topic name: %w", err)
	}

	return name, nil
}

// StatusTopicName returns the ROS topic name for status.
func StatusTopicName(action string) (string, error) {
	name, err := dds.StatusTopicName(action)
	if err != nil {
		return "", fmt.Errorf("bus: dds status topic name: %w", err)
	}

	return name, nil
}

// SendGoalRequestTopic returns the DDS request topic for send goal.
func SendGoalRequestTopic(action string, avoidConventions bool) (string, error) {
	name, err := dds.SendGoalRequestTopic(action, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds send goal request topic: %w", err)
	}

	return name, nil
}

// SendGoalReplyTopic returns the DDS reply topic for send goal.
func SendGoalReplyTopic(action string, avoidConventions bool) (string, error) {
	name, err := dds.SendGoalReplyTopic(action, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds send goal reply topic: %w", err)
	}

	return name, nil
}

// CancelGoalRequestTopic returns the DDS request topic for cancel goal.
func CancelGoalRequestTopic(action string, avoidConventions bool) (string, error) {
	name, err := dds.CancelGoalRequestTopic(action, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds cancel goal request topic: %w", err)
	}

	return name, nil
}

// CancelGoalReplyTopic returns the DDS reply topic for cancel goal.
func CancelGoalReplyTopic(action string, avoidConventions bool) (string, error) {
	name, err := dds.CancelGoalReplyTopic(action, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds cancel goal reply topic: %w", err)
	}

	return name, nil
}

// GetResultRequestTopic returns the DDS request topic for get result.
func GetResultRequestTopic(action string, avoidConventions bool) (string, error) {
	name, err := dds.GetResultRequestTopic(action, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds get result request topic: %w", err)
	}

	return name, nil
}

// GetResultReplyTopic returns the DDS reply topic for get result.
func GetResultReplyTopic(action string, avoidConventions bool) (string, error) {
	name, err := dds.GetResultReplyTopic(action, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds get result reply topic: %w", err)
	}

	return name, nil
}

// ActionFeedbackTopic returns the DDS topic for feedback.
func ActionFeedbackTopic(action string, avoidConventions bool) (string, error) {
	name, err := dds.ActionFeedbackTopic(action, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds action feedback topic: %w", err)
	}

	return name, nil
}

// ActionStatusTopic returns the DDS topic for status.
func ActionStatusTopic(action string, avoidConventions bool) (string, error) {
	name, err := dds.ActionStatusTopic(action, avoidConventions)
	if err != nil {
		return "", fmt.Errorf("bus: dds action status topic: %w", err)
	}

	return name, nil
}

// ActionDDSTopicsFor returns the eight DDS topics for one action.
func ActionDDSTopicsFor(action string, avoidConventions bool) (ActionDDSTopics, error) {
	topics, err := dds.ActionDDSTopicsFor(action, avoidConventions)
	if err != nil {
		return ActionDDSTopics{}, fmt.Errorf("bus: dds action topics: %w", err)
	}

	return topics, nil
}

// ActionSendGoalRequestType returns the DDS request type for send goal.
func ActionSendGoalRequestType(idlBase string) (string, error) {
	name, err := dds.ActionSendGoalRequestType(idlBase)
	if err != nil {
		return "", fmt.Errorf("bus: dds send goal request type: %w", err)
	}

	return name, nil
}

// ActionSendGoalResponseType returns the DDS response type for send goal.
func ActionSendGoalResponseType(idlBase string) (string, error) {
	name, err := dds.ActionSendGoalResponseType(idlBase)
	if err != nil {
		return "", fmt.Errorf("bus: dds send goal response type: %w", err)
	}

	return name, nil
}

// ActionGetResultRequestType returns the DDS request type for get result.
func ActionGetResultRequestType(idlBase string) (string, error) {
	name, err := dds.ActionGetResultRequestType(idlBase)
	if err != nil {
		return "", fmt.Errorf("bus: dds get result request type: %w", err)
	}

	return name, nil
}

// ActionGetResultResponseType returns the DDS response type for get result.
func ActionGetResultResponseType(idlBase string) (string, error) {
	name, err := dds.ActionGetResultResponseType(idlBase)
	if err != nil {
		return "", fmt.Errorf("bus: dds get result response type: %w", err)
	}

	return name, nil
}

// ActionFeedbackMessageType returns the DDS type for feedback messages.
func ActionFeedbackMessageType(idlBase string) (string, error) {
	name, err := dds.ActionFeedbackMessageType(idlBase)
	if err != nil {
		return "", fmt.Errorf("bus: dds feedback message type: %w", err)
	}

	return name, nil
}

// ActionGoalType returns the DDS type for the action goal structure.
func ActionGoalType(idlBase string) (string, error) {
	name, err := dds.ActionGoalType(idlBase)
	if err != nil {
		return "", fmt.Errorf("bus: dds action goal type: %w", err)
	}

	return name, nil
}

// ActionResultType returns the DDS type for the action result structure.
func ActionResultType(idlBase string) (string, error) {
	name, err := dds.ActionResultType(idlBase)
	if err != nil {
		return "", fmt.Errorf("bus: dds action result type: %w", err)
	}

	return name, nil
}

// ActionFeedbackType returns the DDS type for the action feedback structure.
func ActionFeedbackType(idlBase string) (string, error) {
	name, err := dds.ActionFeedbackType(idlBase)
	if err != nil {
		return "", fmt.Errorf("bus: dds action feedback type: %w", err)
	}

	return name, nil
}

// ActionDDSTypesFor returns the DDS types for one action IDL base.
func ActionDDSTypesFor(idlBase string) (ActionDDSTypes, error) {
	types, err := dds.ActionDDSTypesFor(idlBase)
	if err != nil {
		return ActionDDSTypes{}, fmt.Errorf("bus: dds action types: %w", err)
	}

	return types, nil
}

// SplitActionROSName splits a ROS action exchange into base plus exchange.
func SplitActionROSName(rosName string) (string, string, error) {
	base, exchange, err := dds.SplitActionROSName(rosName)
	if err != nil {
		return "", "", fmt.Errorf("bus: split action ros name: %w", err)
	}

	return base, exchange, nil
}

// ParseActionDDSTopic maps a DDS topic back to action base plus exchange.
func ParseActionDDSTopic(ddsTopic string) (string, string, TopicKind, error) {
	base, exchange, kind, err := dds.ParseActionDDSTopic(ddsTopic)
	if err != nil {
		return "", "", "", fmt.Errorf("bus: parse action dds topic: %w", err)
	}

	return base, exchange, kind, nil
}

// GoalIDFromBytes builds a goal ID from 16 bytes with a copy.
func GoalIDFromBytes(b []byte) (GoalID, error) {
	id, err := dds.GoalIDFromBytes(b)
	if err != nil {
		return GoalID{}, fmt.Errorf("bus: goal id from bytes: %w", err)
	}

	return id, nil
}

// SortGoalIDs returns a sorted copy of the input without mutating it.
func SortGoalIDs(ids []GoalID) []GoalID {
	return dds.SortGoalIDs(ids)
}

// CompareGoalIDs orders goal IDs lexicographically for deterministic preempt.
func CompareGoalIDs(a, b GoalID) int {
	return dds.CompareGoalIDs(a, b)
}

// ValidateActionSequence rejects zero as an invalid service sequence.
func ValidateActionSequence(seq uint64) error {
	err := dds.ValidateActionSequence(seq)
	if err != nil {
		return fmt.Errorf("bus: validate action sequence: %w", err)
	}

	return nil
}

// ValidateActionRequestID rejects a zero sequence pairing ID.
func ValidateActionRequestID(id ActionRequestID) error {
	err := dds.ValidateActionRequestID(id)
	if err != nil {
		return fmt.Errorf("bus: validate action request id: %w", err)
	}

	return nil
}

// IsPairedReply reports whether a reply pairs with its request.
func IsPairedReply(req, rep ActionRequestID) bool {
	return dds.IsPairedReply(req, rep)
}

// GoalPaired reports whether two goal IDs refer to the same goal.
func GoalPaired(a, b GoalID) bool {
	return dds.GoalPaired(a, b)
}

// MarshalActionServiceRequest wraps payload with header plus GUID and sequence.
func MarshalActionServiceRequest(payload []byte, id ActionRequestID, littleEndian bool) ([]byte, error) {
	out, err := dds.MarshalActionServiceRequest(payload, id, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("bus: marshal action service request: %w", err)
	}

	return out, nil
}

// ParseActionServiceRequest splits a service sample into payload and pairing ID.
func ParseActionServiceRequest(data []byte) ([]byte, ActionRequestID, bool, error) {
	payload, id, endian, err := dds.ParseActionServiceRequest(data)
	if err != nil {
		return nil, ActionRequestID{}, false, fmt.Errorf("bus: parse action service request: %w", err)
	}

	return payload, id, endian, nil
}

// PickPreemptWinner selects the deterministic preempt winner.
func PickPreemptWinner(a, b GoalSlot) GoalSlot {
	return dds.PickPreemptWinner(a, b)
}

// AllowsPreempt reports whether a status admits preempt by a new goal.
func AllowsPreempt(status int8) bool {
	return dds.AllowsPreempt(status)
}

// PreemptTerminal maps a preempt outcome to its terminal wire status.
func PreemptTerminal(canceled bool) int8 {
	return dds.PreemptTerminal(canceled)
}

// ValidateGoalStatus rejects unknown status values.
func ValidateGoalStatus(status int8) error {
	err := dds.ValidateGoalStatus(status)
	if err != nil {
		return fmt.Errorf("bus: validate goal status: %w", err)
	}

	return nil
}

// IsTerminalGoalStatus reports whether a status ends the goal.
func IsTerminalGoalStatus(status int8) bool {
	return dds.IsTerminalGoalStatus(status)
}

// IsActiveGoalStatus reports whether a status keeps the goal in flight.
func IsActiveGoalStatus(status int8) bool {
	return dds.IsActiveGoalStatus(status)
}

// GoalStatusName renders the status in lowercase wire form.
func GoalStatusName(status int8) string {
	return dds.GoalStatusName(status)
}

// ValidateCancelCode rejects unknown cancel return codes.
func ValidateCancelCode(code int8) error {
	err := dds.ValidateCancelCode(code)
	if err != nil {
		return fmt.Errorf("bus: validate cancel code: %w", err)
	}

	return nil
}

// CancelCodeName renders the cancel code in lowercase wire form.
func CancelCodeName(code int8) string {
	return dds.CancelCodeName(code)
}

// ClassifyCancelRequest maps goal ID plus stamp to its cancel scope.
func ClassifyCancelRequest(goal GoalID, sec int32, nsec uint32) (CancelScope, error) {
	scope, err := dds.ClassifyCancelRequest(goal, sec, nsec)
	if err != nil {
		return CancelScopeInvalid, fmt.Errorf("bus: classify cancel request: %w", err)
	}

	return scope, nil
}

// FanoutServiceQoS duplicates one logical profile to request and reply.
func FanoutServiceQoS(base ActionQoS) DualQoS {
	return dds.FanoutServiceQoS(base)
}

// FanoutActionQoS fans one service profile plus feedback and status into wire QoS.
func FanoutActionQoS(service, feedback, status ActionQoS) ActionQoSFanout {
	return dds.FanoutActionQoS(service, feedback, status)
}

// DefaultActionServiceQoS returns reliable volatile keep last 10 for services.
func DefaultActionServiceQoS() ActionQoS {
	return dds.DefaultActionServiceQoS()
}

// DefaultActionStatusQoS returns reliable transient local keep last 1 for status.
func DefaultActionStatusQoS() ActionQoS {
	return dds.DefaultActionStatusQoS()
}

// DefaultActionFeedbackQoS returns best effort volatile keep last 10 for feedback.
func DefaultActionFeedbackQoS() ActionQoS {
	return dds.DefaultActionFeedbackQoS()
}

// DefaultActionQoSFanout returns the standard service plus feedback plus status fanout.
func DefaultActionQoSFanout() ActionQoSFanout {
	return dds.DefaultActionQoSFanout()
}
