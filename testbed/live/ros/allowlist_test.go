// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"fmt"
	"time"

	"github.com/podomy/weld/testbed/live/helpers"
)

// ROS allowlisted product skips.
//
// Static declarations for every live ROS gate that can product-skip. The
// skipping tests keep only NoteProductSkip plus AssertNoExcessSkips;
// registration lives here so a missing allowance fails loudly instead of
// self-approving. Reasons and 2027-05-01 expiry are verbatim from the
// original call sites.
//
// Helpers only match by test name with a non-empty reason before expiry,
// so one entry per distinct test name covers all reason variants for that
// name. Probe failures with %v errors and hash lookups with per-run topics
// share the same test name allowance as the static fallback for that test.
// Forward plus reverse interop gaps share the same top level test name;
// both triples are registered below with identical strings so either
// direction stays allowlisted. Label embedded reasons use the same Sprintf
// format and args as the runtime call sites with concrete label values.
func init() {
	expiry := time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC)
	registerRosQoSParents(expiry)
	registerRosDHKeepAll(expiry)
	registerRosFullCoreBestAvailable(expiry)
	registerRosFullLiveMarker(expiry)
	registerRosTypeMatrix(expiry)
	registerRosShapeStereoRest(expiry)
	registerRosWriterQoSParents(expiry)
}

// registerRosQoSParents allowlists the parent QoS gates.
//
// Probe failures carry a %v error and share the same parent test name as
// the static fallback, so the static verbatim reason covers both.
func registerRosQoSParents(expiry time.Time) {
	helpers.RegisterAllowlistedSkip("TestROSQoSDurabilityHistoryCompatibleDelivers", "peer weld-jazzy Jazzy lacks --qos-durability, --qos-history, or --qos-depth, distro pin Jazzy", expiry)
	helpers.RegisterAllowlistedSkip("TestROSQoSDurabilityHistoryIncompatibleLoud", "peer weld-jazzy Jazzy lacks --qos-durability, --qos-history, or --qos-depth, distro pin Jazzy", expiry)
	helpers.RegisterAllowlistedSkip("TestROSQoSFullCompatibleDelivers", "peer weld-jazzy Jazzy lacks --qos-depth, distro pin Jazzy", expiry)
	helpers.RegisterAllowlistedSkip("TestROSQoSFullIncompatibleLoud", "peer weld-jazzy Jazzy lacks --qos-depth, distro pin Jazzy", expiry)
	helpers.RegisterAllowlistedSkip("TestROSQoSCompatibleDelivers", "peer weld-jazzy Jazzy lacks QoS flags, distro pin Jazzy", expiry)
	helpers.RegisterAllowlistedSkip("TestROSQoSIncompatibleLoud", "peer weld-jazzy Jazzy lacks QoS flags, distro pin Jazzy", expiry)
	helpers.RegisterAllowlistedSkip("TestROSQoSReliabilityCompatibleDelivers", "peer weld-jazzy Jazzy lacks --qos-reliability, distro pin Jazzy", expiry)
	helpers.RegisterAllowlistedSkip("TestROSQoSReliabilityIncompatibleLoud", "peer weld-jazzy Jazzy lacks --qos-reliability, distro pin Jazzy", expiry)
}

// registerRosDHKeepAll allowlists the keep_all subtests.
//
// Runtime uses fmt.Sprintf("case %s: peer weld-jazzy Jazzy lacks keep_all value, distro pin Jazzy", c.name)
// with c.name from the compatible plus incompatible case lists filtered to
// keep_all. The init entries use the same format with concrete names.
func registerRosDHKeepAll(expiry time.Time) {
	keepAll := []struct {
		parent string
		name   string
	}{
		{"TestROSQoSDurabilityHistoryCompatibleDelivers", "forward_transient_keepall"},
		{"TestROSQoSDurabilityHistoryCompatibleDelivers", "reverse_transient_keepall"},
		{"TestROSQoSDurabilityHistoryCompatibleDelivers", "reverse_volatile_keepall"},
		{"TestROSQoSDurabilityHistoryIncompatibleLoud", "forward_volatile_keepall"},
	}
	for _, c := range keepAll {
		reason := fmt.Sprintf("case %s: peer weld-jazzy Jazzy lacks keep_all value, distro pin Jazzy", c.name)
		helpers.RegisterAllowlistedSkip(c.parent+"/"+c.name, reason, expiry)
	}
}

// registerRosFullCoreBestAvailable allowlists the core best_available subtests.
//
// Runtime uses fmt.Sprintf("case %s: profile mapping unsupported on weld-jazzy Jazzy: %v", c.name, err)
// where err is qos full unsupported history "best_available" for every
// best_available history case. The init entries use the same format with
// the same deterministic error text.
func registerRosFullCoreBestAvailable(expiry time.Time) {
	detail := fmt.Sprintf("qos full: unsupported history %q", "best_available")
	for _, c := range qosFullGenerateFullCases() {
		if c.history != "best_available" {
			continue
		}
		reason := fmt.Sprintf("case %s: profile mapping unsupported on weld-jazzy Jazzy: %v", c.name, detail)
		helpers.RegisterAllowlistedSkip("TestROSQoSFullCore/"+c.name, reason, expiry)
	}
}

// registerRosFullLiveMarker allowlists the live marker subtests.
//
// Runtime uses fmt.Sprintf("case %s: marker value needs resolution on weld-jazzy Jazzy, core truth pinned above", c.name)
// for marker cases. Live cases are all concrete so this path is currently
// unreachable, but the entries keep the triple allowlisted. Lacks flag and
// profile mapping variants for the same subtest names share the same test
// name allowance.
func registerRosFullLiveMarker(expiry time.Time) {
	for _, c := range qosFullLiveCases() {
		reason := fmt.Sprintf("case %s: marker value needs resolution on weld-jazzy Jazzy, core truth pinned above", c.name)
		if qosFullWantCompatible(c) {
			// Compatible live cases run under the compatible delivers gate.
			// Incompatible cases run under the incompatible loud gate below.
			// Both parents are registered per case so the split stays exact.
			helpers.RegisterAllowlistedSkip("TestROSQoSFullCompatibleDelivers/"+c.name, reason, expiry)
		} else {
			helpers.RegisterAllowlistedSkip("TestROSQoSFullIncompatibleLoud/"+c.name, reason, expiry)
		}
	}
	// Register the complementary parent for each live case as well so a
	// future compatible split change cannot leave a subtest unallowlisted.
	// The reason text stays identical; only the parent prefix differs.
	for _, c := range qosFullLiveCases() {
		reason := fmt.Sprintf("case %s: marker value needs resolution on weld-jazzy Jazzy, core truth pinned above", c.name)
		if qosFullWantCompatible(c) {
			helpers.RegisterAllowlistedSkip("TestROSQoSFullIncompatibleLoud/"+c.name, reason, expiry)
		} else {
			helpers.RegisterAllowlistedSkip("TestROSQoSFullCompatibleDelivers/"+c.name, reason, expiry)
		}
	}
}

// registerRosTypeMatrix allowlists every shared matrix type test.
//
// Runtime forward uses "weld take misses " + tc.Label + " user DATA but
// local codec ok, known interop gap forward matrix" and reverse uses
// "ros echo misses " + tc.Label + " user DATA but pub ok, known interop
// gap reverse matrix" with tc.Label from the case lists. Hash unavailable
// paths use the same test names with label plus %v error and share the
// same test name allowance. Both forward and reverse triples are
// registered per test with identical strings.
func registerRosTypeMatrix(expiry time.Time) {
	cases := []struct {
		test  string
		label string
	}{
		{"TestROSTypeBuiltinDurationNeg", "builtin-duration-neg"},
		{"TestROSTypeBuiltinDurationZero", "builtin-duration-zero"},
		{"TestROSTypeBuiltinTimeMax", "builtin-time-max"},
		{"TestROSTypeBuiltinTimeZero", "builtin-time-zero"},
		{"TestROSTypeDiagArray", "diag-array"},
		{"TestROSTypeDiagKeyValue", "diag-keyvalue"},
		{"TestROSTypeDiagStatus", "diag-status"},
		{"TestROSTypeGeometryAccel", "geometry-accel"},
		{"TestROSTypeGeometryAccelStamped", "geometry-accelstamped"},
		{"TestROSTypeGeometryAccelWithCovariance", "geometry-accelcov"},
		{"TestROSTypeGeometryAccelWithCovarianceStamped", "geometry-accelcovstamped"},
		{"TestROSTypeGeometryInertia", "geometry-inertia"},
		{"TestROSTypeGeometryInertiaStamped", "geometry-inertiastamped"},
		{"TestROSTypeGeometryPoint", "geometry-point"},
		{"TestROSTypeGeometryPoint32", "geometry-point32"},
		{"TestROSTypeGeometryPointStamped", "geometry-pointstamped"},
		{"TestROSTypeGeometryPolygon", "geometry-polygon"},
		{"TestROSTypeGeometryPolygonStamped", "geometry-polygonstamped"},
		{"TestROSTypeGeometryPose", "geometry-pose"},
		{"TestROSTypeGeometryPoseArray", "geometry-posearray"},
		{"TestROSTypeGeometryPoseStamped", "geometry-posestamped"},
		{"TestROSTypeGeometryPoseWithCovariance", "geometry-posecov"},
		{"TestROSTypeGeometryPoseWithCovarianceStamped", "geometry-posecovstamped"},
		{"TestROSTypeGeometryQuaternion", "geometry-quaternion"},
		{"TestROSTypeGeometryQuaternionStamped", "geometry-quatstamped"},
		{"TestROSTypeGeometryTransform", "geometry-transform"},
		{"TestROSTypeGeometryTransformStamped", "geometry-tfstamped"},
		{"TestROSTypeGeometryTwist", "geometry-twist"},
		{"TestROSTypeGeometryTwistStamped", "geometry-twiststamped"},
		{"TestROSTypeGeometryTwistWithCovariance", "geometry-twistcov"},
		{"TestROSTypeGeometryTwistWithCovarianceStamped", "geometry-twistcovstamped"},
		{"TestROSTypeGeometryVector3", "geometry-vector3"},
		{"TestROSTypeGeometryVector3Stamped", "geometry-vecstamped"},
		{"TestROSTypeGeometryWrench", "geometry-wrench"},
		{"TestROSTypeGeometryWrenchStamped", "geometry-wrenchstamped"},
		{"TestROSTypeNavGridCells", "nav-gridcells"},
		{"TestROSTypeNavMapMetaData", "nav-mapmetadata"},
		{"TestROSTypeNavOccupancyGrid", "nav-occupancygrid"},
		{"TestROSTypeNavOdometry", "nav-odometry"},
		{"TestROSTypeNavPath", "nav-path"},
		{"TestROSTypeSensorBatteryState", "sensor-batterystate"},
		{"TestROSTypeSensorCameraInfo", "sensor-camerainfo"},
		{"TestROSTypeSensorChannelFloat", "sensor-channelfloat"},
		{"TestROSTypeSensorCompressedImage", "sensor-compressedimage"},
		{"TestROSTypeSensorFluidPressure", "sensor-fluidpressure"},
		{"TestROSTypeSensorHumidity", "sensor-humidity"},
		{"TestROSTypeSensorIlluminance", "sensor-illuminance"},
		{"TestROSTypeSensorImage", "sensor-image"},
		{"TestROSTypeSensorImu", "sensor-imu"},
		{"TestROSTypeSensorJointState", "sensor-jointstate"},
		{"TestROSTypeSensorJoy", "sensor-joy"},
		{"TestROSTypeSensorJoyFeedback", "sensor-joyfeedback"},
		{"TestROSTypeSensorJoyFeedbackArray", "sensor-joyfeedbackarray"},
		{"TestROSTypeSensorLaserEcho", "sensor-laserecho"},
		{"TestROSTypeSensorLaserScan", "sensor-laserscan"},
		{"TestROSTypeSensorMagField", "sensor-magfield"},
		{"TestROSTypeSensorMultiDOF", "sensor-multidof"},
		{"TestROSTypeSensorMultiEcho", "sensor-multiecho"},
		{"TestROSTypeSensorNavSatFix", "sensor-navsatfix"},
		{"TestROSTypeSensorNavSatStatus", "sensor-navsatstatus"},
		{"TestROSTypeSensorPointCloud", "sensor-pointcloud"},
		{"TestROSTypeSensorPointCloud2", "sensor-pointcloud2"},
		{"TestROSTypeSensorPointField", "sensor-pointfield"},
		{"TestROSTypeSensorROI", "sensor-roi"},
		{"TestROSTypeSensorRange", "sensor-range"},
		{"TestROSTypeSensorTemperature", "sensor-temperature"},
		{"TestROSTypeSensorTimeRef", "sensor-timeref"},
		{"TestROSTypeShapeMesh", "shape-mesh"},
		{"TestROSTypeShapeMeshTriangle", "shape-meshtriangle"},
		{"TestROSTypeShapePlane", "shape-plane"},
		{"TestROSTypeStdBool", "std-bool"},
		{"TestROSTypeStdByte", "std-byte"},
		{"TestROSTypeStdByteMultiArray", "std-bytemultiarray"},
		{"TestROSTypeStdChar", "std-char"},
		{"TestROSTypeStdColorRGBA", "std-colorrgba"},
		{"TestROSTypeStdEmpty", "std-empty"},
		{"TestROSTypeStdFloat32", "std-float32"},
		{"TestROSTypeStdFloat32MultiArray", "std-float32multiarray"},
		{"TestROSTypeStdFloat64", "std-float64"},
		{"TestROSTypeStdFloat64MultiArray", "std-float64multiarray"},
		{"TestROSTypeStdHeader", "std-header"},
		{"TestROSTypeStdInt16", "std-int16"},
		{"TestROSTypeStdInt16MultiArray", "std-int16multiarray"},
		{"TestROSTypeStdInt32", "std-int32"},
		{"TestROSTypeStdInt32MultiArray", "std-int32multiarray"},
		{"TestROSTypeStdInt64", "std-int64"},
		{"TestROSTypeStdInt64MultiArray", "std-int64multiarray"},
		{"TestROSTypeStdInt8", "std-int8"},
		{"TestROSTypeStdInt8MultiArray", "std-int8multiarray"},
		{"TestROSTypeStdMultiArrayDimension", "std-multiarraydimension"},
		{"TestROSTypeStdMultiArrayLayout", "std-multiarraylayout"},
		{"TestROSTypeStdString", "std-string"},
		{"TestROSTypeStdUInt16", "std-uint16"},
		{"TestROSTypeStdUInt16MultiArray", "std-uint16multiarray"},
		{"TestROSTypeStdUInt32", "std-uint32"},
		{"TestROSTypeStdUInt32MultiArray", "std-uint32multiarray"},
		{"TestROSTypeStdUInt64", "std-uint64"},
		{"TestROSTypeStdUInt64MultiArray", "std-uint64multiarray"},
		{"TestROSTypeStdUInt8", "std-uint8"},
		{"TestROSTypeStdUInt8MultiArray", "std-uint8multiarray"},
		{"TestROSTypeTrajectoryJointTrajectory", "trajectory-jointtrajectory"},
		{"TestROSTypeTrajectoryJointTrajectoryPoint", "trajectory-jointtrajectorypoint"},
		{"TestROSTypeTrajectoryMultiDOFJointTrajectory", "trajectory-multidofjointtrajectory"},
		{"TestROSTypeTrajectoryMultiDOFJointTrajectoryPoint", "trajectory-multidofjointtrajectorypoint"},
		{"TestROSTypeVisualizationMarker", "viz-marker"},
		{"TestROSTypeVizImageMarker", "viz-imagemarker"},
		{"TestROSTypeVizInteractiveMarker", "viz-imarker"},
		{"TestROSTypeVizInteractiveMarkerControl", "viz-imcontrol"},
		{"TestROSTypeVizInteractiveMarkerFeedback", "viz-ifeedback"},
		{"TestROSTypeVizInteractiveMarkerInit", "viz-iinit"},
		{"TestROSTypeVizInteractiveMarkerPose", "viz-ipose"},
		{"TestROSTypeVizInteractiveMarkerUpdate", "viz-iupdate"},
		{"TestROSTypeVizMarkerArray", "viz-markerarray"},
		{"TestROSTypeVizMenuEntry", "viz-menuentry"},
		{"TestROSTypeVizRest2ImageMarker", "viz-imagemarker"},
		{"TestROSTypeVizRest2InteractiveMarker", "viz-imarker"},
		{"TestROSTypeVizRest2InteractiveMarkerControl", "viz-imcontrol"},
		{"TestROSTypeVizRest2InteractiveMarkerFeedback", "viz-ifeedback"},
		{"TestROSTypeVizRest2InteractiveMarkerInit", "viz-iinit"},
		{"TestROSTypeVizRest2InteractiveMarkerPose", "viz-ipose"},
		{"TestROSTypeVizRest2InteractiveMarkerUpdate", "viz-iupdate"},
		{"TestROSTypeVizRest2MarkerArray", "viz-markerarray"},
		{"TestROSTypeVizRest2MenuEntry", "viz-menuentry"},
	}
	for _, c := range cases {
		forward := "weld take misses " + c.label + " user DATA but local codec ok, known interop gap forward matrix"
		reverse := "ros echo misses " + c.label + " user DATA but pub ok, known interop gap reverse matrix"
		helpers.RegisterAllowlistedSkip(c.test, forward, expiry)
		helpers.RegisterAllowlistedSkip(c.test, reverse, expiry)
	}
}

// registerRosShapeStereoRest allowlists the shape plus stereo plus byte array gates.
//
// Hash unavailable paths carry per-run topics plus %v errors and share the
// same test name allowance as the static interop gaps below.
func registerRosShapeStereoRest(expiry time.Time) {
	helpers.RegisterAllowlistedSkip("TestROSTypeShapeSolidPrimitive", "weld take misses shape user DATA but local codec ok, known interop gap forward shape", expiry)
	helpers.RegisterAllowlistedSkip("TestROSTypeShapeSolidPrimitive", "ros echo misses shape user DATA but pub ok, known interop gap reverse shape", expiry)
	helpers.RegisterAllowlistedSkip("TestROSTypeStereoDisparity", "weld take misses stereo disparity user DATA but local codec ok, known interop gap forward stereo", expiry)
	helpers.RegisterAllowlistedSkip("TestROSTypeStereoDisparity", "ros echo misses stereo disparity user DATA but pub ok, known interop gap reverse stereo", expiry)
	helpers.RegisterAllowlistedSkip("TestROSTypeStdByteMultiArrayReverse", "std-bytemultiarray hash unavailable for pooled domain weld-jazzy Jazzy", expiry)
}

// registerRosWriterQoSParents allowlists the writer QoS gates.
//
// Probe failures carry a %v error and share the same parent test names as
// the static fallback below.
func registerRosWriterQoSParents(expiry time.Time) {
	helpers.RegisterAllowlistedSkip("TestWriterQoSLiveBestEffortOfferFlows", "peer weld-jazzy Jazzy lacks QoS flags, distro pin Jazzy, skipping writerqos gate", expiry)
	helpers.RegisterAllowlistedSkip("TestWriterQoSLiveVolatileServiceEquivalent", "peer weld-jazzy Jazzy lacks QoS flags, distro pin Jazzy, skipping writerqos gate", expiry)
	helpers.RegisterAllowlistedSkip("TestWriterQoSLiveIncompatibleRefusedLoudly", "peer weld-jazzy Jazzy lacks QoS flags, distro pin Jazzy, skipping writerqos gate", expiry)
}
