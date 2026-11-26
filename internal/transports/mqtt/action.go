// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"errors"
	"fmt"
	"strings"
)

// Action exchange names for the MQTT backend.
//
// Actions ride the legs the transport already serves: send goal,
// cancel goal, and get result are services on the rq and rr legs,
// while feedback and status are data topics on the rt leg. Payloads
// stay opaque; matching operation names to ROS exchange names is the
// only mapping this file owns. Service calls use PublishService plus
// TakeService with the names below, feedback and status use
// PublishBytes plus TakeBytes with LegData.

// Action exchange tokens mirroring the DDS backend names.
const (
	// ActionToken separates the action base from its exchanges.
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

// cleanActionBase validates one action base name and strips its slash.
func cleanActionBase(action string) (string, error) {
	trimmed := strings.TrimSpace(action)

	if trimmed == "" {
		return "", errors.New("mqtt: action name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if !strings.HasPrefix(trimmed, "/") {
		return "", fmt.Errorf("mqtt: action name %q misses leading slash", action)
	}

	stripped := strings.TrimPrefix(trimmed, "/")

	if stripped == "" {
		return "", fmt.Errorf("mqtt: action name %q is empty", action)
	}

	if strings.Contains(stripped, "//") {
		return "", fmt.Errorf("mqtt: action name %q has an empty segment", action)
	}

	if strings.HasSuffix(stripped, "/") {
		return "", fmt.Errorf("mqtt: action name %q has a trailing slash", action)
	}

	if strings.ContainsAny(stripped, " \t\n\r") {
		return "", fmt.Errorf("mqtt: action name %q has whitespace", action)
	}

	return stripped, nil
}

// actionExchangeName joins a stripped base with its exchange.
func actionExchangeName(stripped, exchange string) string {
	return "/" + stripped + "/" + ActionToken + "/" + exchange
}

// SendGoalServiceName returns the ROS service name for send goal.
func SendGoalServiceName(action string) (string, error) {
	stripped, err := cleanActionBase(action)
	if err != nil {
		return "", err
	}

	return actionExchangeName(stripped, ActionSendGoal), nil
}

// CancelGoalServiceName returns the ROS service name for cancel goal.
func CancelGoalServiceName(action string) (string, error) {
	stripped, err := cleanActionBase(action)
	if err != nil {
		return "", err
	}

	return actionExchangeName(stripped, ActionCancelGoal), nil
}

// GetResultServiceName returns the ROS service name for get result.
func GetResultServiceName(action string) (string, error) {
	stripped, err := cleanActionBase(action)
	if err != nil {
		return "", err
	}

	return actionExchangeName(stripped, ActionGetResult), nil
}

// FeedbackTopicName returns the ROS topic name for feedback.
func FeedbackTopicName(action string) (string, error) {
	stripped, err := cleanActionBase(action)
	if err != nil {
		return "", err
	}

	return actionExchangeName(stripped, ActionFeedback), nil
}

// StatusTopicName returns the ROS topic name for status.
func StatusTopicName(action string) (string, error) {
	stripped, err := cleanActionBase(action)
	if err != nil {
		return "", err
	}

	return actionExchangeName(stripped, ActionStatus), nil
}
