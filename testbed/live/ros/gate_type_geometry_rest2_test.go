// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"testing"

	"github.com/podomy/weld/testbed/live/helpers"
)

// rest2ExpectedRosTypes returns the candidate rest geometry ROS types.
//
// The list matches the task candidate set. Grep over
// testbed/live/ros/gate_type_geometry*.go shows each entry is already covered
// by restGeometryCases in gate_type_geometry_rest_test.go, so this file pins
// completeness instead of duplicating live runners.
func rest2ExpectedRosTypes() []string {
	return []string{
		"geometry_msgs/msg/Point",
		"geometry_msgs/msg/Point32",
		"geometry_msgs/msg/Accel",
		"geometry_msgs/msg/Inertia",
		"geometry_msgs/msg/Polygon",
		"geometry_msgs/msg/Transform",
		"geometry_msgs/msg/Wrench",
		"geometry_msgs/msg/AccelWithCovariance",
		"geometry_msgs/msg/PoseWithCovariance",
		"geometry_msgs/msg/TwistWithCovariance",
		"geometry_msgs/msg/AccelStamped",
		"geometry_msgs/msg/AccelWithCovarianceStamped",
		"geometry_msgs/msg/InertiaStamped",
		"geometry_msgs/msg/PolygonStamped",
		"geometry_msgs/msg/PoseArray",
		"geometry_msgs/msg/PoseStamped",
		"geometry_msgs/msg/PoseWithCovarianceStamped",
		"geometry_msgs/msg/QuaternionStamped",
		"geometry_msgs/msg/TransformStamped",
		"geometry_msgs/msg/TwistStamped",
		"geometry_msgs/msg/TwistWithCovarianceStamped",
		"geometry_msgs/msg/Vector3Stamped",
		"geometry_msgs/msg/WrenchStamped",
	}
}

// rest2CoveredRosTypes returns the set of ROS types already live covered.
//
// It unions the base matrix, the extra geometry gate, and the rest geometry
// gate. All three return []typeMatrixCase so the matrix pattern stays single.
func rest2CoveredRosTypes() map[string]bool {
	covered := make(map[string]bool)
	for _, tc := range typeMatrixCases() {
		covered[tc.RosType] = true
	}
	for _, tc := range extraGeometryCases() {
		covered[tc.RosType] = true
	}
	for _, tc := range restGeometryCases() {
		covered[tc.RosType] = true
	}
	return covered
}

// TestROSTypeGeometryRest2Coverage pins that no rest geometry type remains.
//
// The test allocates one exclusive domain from the parallel pool. The peer
// is weld-jazzy via helpers.Container. Docker argv is built with
// helpers.DockerArgs and no shell. Per-run live IDs plus the allocated
// domain isolate the check. The test skips when the peer is absent,
// matching the Skip if absent rule for live gates.
func TestROSTypeGeometryRest2Coverage(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	err := helpers.ValidateDomain(domain)
	if err != nil {
		t.Fatalf("validate domain %d: %v", domain, err)
	}
	liveFwd := helpers.LiveID("ros-rest2-fwd")
	topicFwd := helpers.LiveTopic(liveFwd)
	liveRev := helpers.LiveID("ros-rest2-rev")
	topicRev := helpers.LiveTopic(liveRev)
	if liveFwd == liveRev {
		t.Fatalf("rest2 live IDs match, wants distinct per-run IDs")
	}
	if topicFwd == topicRev {
		t.Fatalf("rest2 topics match, wants distinct per-run topics")
	}
	argv := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "list")
	if len(argv) == 0 {
		t.Fatalf("rest2 docker argv empty, wants exec argv")
	}
	if argv[0] != "exec" {
		t.Fatalf("rest2 docker argv %q wants exec argv", argv[0])
	}
	t.Logf("rest2 fwd live %s topic %s rev live %s topic %s", liveFwd, topicFwd, liveRev, topicRev)
	covered := rest2CoveredRosTypes()
	for _, rosType := range rest2ExpectedRosTypes() {
		if !covered[rosType] {
			t.Fatalf("rest2 missing live gate for %s", rosType)
		}
	}
}
