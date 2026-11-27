// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"fmt"

	"github.com/podomy/weld/internal/core"
)

// SerialGroup delivers attached entities one at a time in priority plus
// insertion order with lifecycle supervision and bond liveliness. The zero
// value is unusable; build with NewSerialGroup. Do not copy after first use.
type SerialGroup = core.SerialGroup

// SerialChannel is a subscriber owned bounded queue with drop oldest overflow.
// TryPublish never blocks; Take pops the oldest without blocking. Do not copy
// a channel after first use.
type SerialChannel[T any] = core.SerialChannel[T]

// PooledGroup is a bounded worker pool replacing reentrant callback groups.
// The group owns exactly Workers goroutines started at construction. Do not
// copy a group after first use.
type PooledGroup[T any] = core.PooledGroup[T]

// NewSerialGroup returns an empty serial supervision group. An empty name is
// an error. The group starts unconfigured and inactive.
func NewSerialGroup(name string) (*SerialGroup, error) {
	group, err := core.NewSerialGroup(name)
	if err != nil {
		return nil, fmt.Errorf("node: new serial group: %w", err)
	}
	return group, nil
}

// NewSerialChannel returns an empty subscriber channel holding depth samples.
// Depth must be positive; derive it from RetentionCapacity for QoS profiles.
// There is no upper bound; the caller owns sizing and the lower bound is
// validated inside.
func NewSerialChannel[T any](depth int) (*SerialChannel[T], error) {
	channel, err := core.NewSerialChannel[T](depth)
	if err != nil {
		return nil, fmt.Errorf("node: new serial channel: %w", err)
	}
	return channel, nil
}

// AttachSerialChannel binds a subscriber channel to the group so its handler
// runs in serial order. A nil group, nil channel, nil handler, or empty name
// is an error.
func AttachSerialChannel[T any](g *SerialGroup, name string, priority int, ch *SerialChannel[T], handle func(T)) error {
	err := core.AttachSerialChannel(g, name, priority, ch, handle)
	if err != nil {
		return fmt.Errorf("node: attach serial channel: %w", err)
	}
	return nil
}

// NewPooledGroup returns a pooled group with workers dedicated threads and
// depth queue slots per subscription lane. Workers and depth must be
// positive: zero workers cannot drain and zero depth cannot retain.
// There is no upper bound; the caller owns sizing and the lower bound is
// validated inside.
func NewPooledGroup[T any](workers, depth int) (*PooledGroup[T], error) {
	group, err := core.NewPooledGroup[T](workers, depth)
	if err != nil {
		return nil, fmt.Errorf("node: new pooled group: %w", err)
	}
	return group, nil
}

// PooledSubscription is a handle to one lane of a pooled group. See core.PooledSubscription.
type PooledSubscription[T any] = core.PooledSubscription[T]

// SerialEntity is one unit of ordered work plus lifecycle and bond identity. See core.SerialEntity.
type SerialEntity = core.SerialEntity

const (
	// SerialBondHeartbeatPeriodNanos is the bond heartbeat period of 0.25 s.
	SerialBondHeartbeatPeriodNanos = core.SerialBondHeartbeatPeriodNanos
	// SerialBondCheckPeriodNanos is the bond check polling period of 0.2 s.
	SerialBondCheckPeriodNanos = core.SerialBondCheckPeriodNanos
	// SerialBondManagerTimeoutNanos is the manager bond timeout of 4.0 s.
	SerialBondManagerTimeoutNanos = core.SerialBondManagerTimeoutNanos
	// SerialBondServiceTimeoutNanos is the service bond timeout of 5.0 s.
	SerialBondServiceTimeoutNanos = core.SerialBondServiceTimeoutNanos
)

// NewSerialChannelFromQoS returns a subscriber channel with depth from the profile retention bound.
func NewSerialChannelFromQoS[T any](p QoSProfile) (*SerialChannel[T], error) {
	channel, err := core.NewSerialChannelFromQoS[T](p)
	if err != nil {
		return nil, fmt.Errorf("node: new serial channel from qos: %w", err)
	}
	return channel, nil
}
