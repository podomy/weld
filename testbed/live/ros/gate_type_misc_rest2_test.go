// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Misc rest2 live gate constants for timeouts.
//
// The test allocates one exclusive domain from the parallel pool, so the
// per-run topic plus the allocated domain isolates runs. Timeouts bound
// every docker and backend call through context. All probing uses docker
// argv with no shell through the shared helpers.
const (
	miscRest2TestTimeout = 2 * time.Minute
	miscRest2ProbeWait   = 5 * time.Second
)

// miscRest2CoveredEntry names one already gated type plus its home file.
//
// RosType is the slash form used for ros2 interface show probes. GateFile
// is the existing live gate file that already covers forward plus reverse
// for the type, so this rest2 file adds no duplicate traffic.
type miscRest2CoveredEntry struct {
	Pkg     string
	Name    string
	RosType string
	Gate    string
}

// miscRest2CoveredTypes returns the verified complete set in corpus order.
//
// builtin_interfaces holds exactly two messages (Duration plus Time), both
// covered by gate_type_builtin_test.go. shape_msgs holds exactly four
// messages (MeshTriangle, Mesh, Plane, SolidPrimitive), with SolidPrimitive
// in gate_type_shape_test.go and the other three in
// gate_type_shape_extra_test.go. stereo_msgs holds one corpus message
// (DisparityImage) covered by gate_type_stereo_test.go. Grep over
// gate_type_builtin*.go, gate_type_shape*.go, and gate_type_stereo*.go
// confirmed each entry before this file was written, so no new encode or
// decode path is added here.
func miscRest2CoveredTypes() []miscRest2CoveredEntry {
	return []miscRest2CoveredEntry{
		{Pkg: "builtin_interfaces", Name: "Duration", RosType: "builtin_interfaces/msg/Duration", Gate: "gate_type_builtin_test.go"},
		{Pkg: "builtin_interfaces", Name: "Time", RosType: "builtin_interfaces/msg/Time", Gate: "gate_type_builtin_test.go"},
		{Pkg: "stereo_msgs", Name: "DisparityImage", RosType: "stereo_msgs/msg/DisparityImage", Gate: "gate_type_stereo_test.go"},
		{Pkg: "shape_msgs", Name: "MeshTriangle", RosType: "shape_msgs/msg/MeshTriangle", Gate: "gate_type_shape_extra_test.go"},
		{Pkg: "shape_msgs", Name: "Mesh", RosType: "shape_msgs/msg/Mesh", Gate: "gate_type_shape_extra_test.go"},
		{Pkg: "shape_msgs", Name: "Plane", RosType: "shape_msgs/msg/Plane", Gate: "gate_type_shape_extra_test.go"},
		{Pkg: "shape_msgs", Name: "SolidPrimitive", RosType: "shape_msgs/msg/SolidPrimitive", Gate: "gate_type_shape_test.go"},
	}
}

// checkMiscRest2Corpus verifies every covered type exists in the corpus.
func checkMiscRest2Corpus(t *testing.T) {
	t.Helper()
	for _, e := range miscRest2CoveredTypes() {
		entry, ok := types.Lookup(e.Pkg, e.Name)
		if !ok {
			t.Errorf("corpus misses %s/%s, want entry from %s", e.Pkg, e.Name, e.Gate)
			continue
		}
		if entry.Package != e.Pkg || entry.Name != e.Name {
			t.Errorf("corpus lookup %s/%s returned %s/%s", e.Pkg, e.Name, entry.Package, entry.Name)
		}
	}
}

// TestROSTypeMiscRest2Completeness checks rest2 scope adds no missing gate.
//
// Offline it verifies the seven corpus entries across builtin_interfaces,
// stereo_msgs, and shape_msgs are present. Live it skips when the peer is
// absent and skips when the peer lacks any one of the ROS types, using
// docker argv probes on the allocated domain against weld-jazzy. A per-run live ID
// plus topic is minted for isolation parity with the matrix runners even
// though this file sends no traffic.
func TestROSTypeMiscRest2Completeness(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), miscRest2TestTimeout)
	defer cancel()
	err := helpers.ValidateDomain(domain)
	if err != nil {
		t.Fatalf("misc rest2 domain invalid: %v", err)
	}
	checkMiscRest2Corpus(t)
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	live := helpers.LiveID("ros-misc-rest2")
	topic := helpers.LiveTopic(live)
	t.Logf("misc rest2 live %s topic %s domain %d", live, topic, domain)
	for _, e := range miscRest2CoveredTypes() {
		helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, e.RosType, miscRest2ProbeWait)
	}
}
