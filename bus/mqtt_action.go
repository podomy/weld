// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus

import (
	"fmt"

	"github.com/podomy/weld/internal/transports/mqtt"
)

// MQTT action exchange tokens mirroring the DDS backend names.
const (
	// MQTTActionToken separates the action base from its exchanges.
	MQTTActionToken = mqtt.ActionToken
	// MQTTActionSendGoal is the send goal service exchange.
	MQTTActionSendGoal = mqtt.ActionSendGoal
	// MQTTActionCancelGoal is the cancel goal service exchange.
	MQTTActionCancelGoal = mqtt.ActionCancelGoal
	// MQTTActionGetResult is the get result service exchange.
	MQTTActionGetResult = mqtt.ActionGetResult
	// MQTTActionFeedback is the feedback topic exchange.
	MQTTActionFeedback = mqtt.ActionFeedback
	// MQTTActionStatus is the status topic exchange.
	MQTTActionStatus = mqtt.ActionStatus
)

// SendGoalServiceName returns the ROS service name for send goal on MQTT.
func MQTTSendGoalServiceName(action string) (string, error) {
	name, err := mqtt.SendGoalServiceName(action)
	if err != nil {
		return "", fmt.Errorf("bus: mqtt send goal service name: %w", err)
	}

	return name, nil
}

// CancelGoalServiceName returns the ROS service name for cancel goal on MQTT.
func MQTTCancelGoalServiceName(action string) (string, error) {
	name, err := mqtt.CancelGoalServiceName(action)
	if err != nil {
		return "", fmt.Errorf("bus: mqtt cancel goal service name: %w", err)
	}

	return name, nil
}

// GetResultServiceName returns the ROS service name for get result on MQTT.
func MQTTGetResultServiceName(action string) (string, error) {
	name, err := mqtt.GetResultServiceName(action)
	if err != nil {
		return "", fmt.Errorf("bus: mqtt get result service name: %w", err)
	}

	return name, nil
}

// FeedbackTopicName returns the ROS topic name for feedback on MQTT.
func MQTTFeedbackTopicName(action string) (string, error) {
	name, err := mqtt.FeedbackTopicName(action)
	if err != nil {
		return "", fmt.Errorf("bus: mqtt feedback topic name: %w", err)
	}

	return name, nil
}

// StatusTopicName returns the ROS topic name for status on MQTT.
func MQTTStatusTopicName(action string) (string, error) {
	name, err := mqtt.StatusTopicName(action)
	if err != nil {
		return "", fmt.Errorf("bus: mqtt status topic name: %w", err)
	}

	return name, nil
}
