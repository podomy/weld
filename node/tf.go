// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"github.com/podomy/weld/internal/core"
)

// TFStore holds dynamic plus latched static transforms. The zero value is
// unusable because the maps are nil; build with NewTFStore and do not copy a
// store after first use.
type TFStore = core.TFStore

// Transform is a rigid child pose in the parent frame.
type Transform = core.Transform

// TransformStamped is one stamped edge with parent frame plus child target.
type TransformStamped = core.TransformStamped

// Vector3 is a three component translation vector.
type Vector3 = core.Vector3

// Quaternion is a rotation quaternion.
type Quaternion = core.Quaternion

const (
	// TFTopic carries dynamic transforms as volatile reliable.
	TFTopic = core.TFTopic
	// TFStaticTopic carries latched static transforms as transient local reliable.
	TFStaticTopic = core.TFStaticTopic
	// TFHistoryDepth bounds dynamic retention per frame pair.
	TFHistoryDepth = core.TFHistoryDepth
	// TFStaticHistoryDepth is the wire depth for the latched static topic.
	TFStaticHistoryDepth = core.TFStaticHistoryDepth
)

// NewTFStore returns an empty store.
func NewTFStore() *TFStore {
	return core.NewTFStore()
}

// IdentityTransform returns the transform that changes nothing.
func IdentityTransform() Transform {
	return core.IdentityTransform()
}

// TFQoS returns the profile Weld publishes and subscribes for dynamic
// transforms: reliable volatile keep last with TF history depth.
func TFQoS() QoSProfile {
	return core.TFQoS()
}

// TFStaticQoS returns the profile Weld publishes and subscribes for static
// transforms: reliable transient local keep last with static depth.
func TFStaticQoS() QoSProfile {
	return core.TFStaticQoS()
}

// NormalizeFrameID returns the lookup key form of a frame id.
func NormalizeFrameID(id string) string {
	return core.NormalizeFrameID(id)
}
