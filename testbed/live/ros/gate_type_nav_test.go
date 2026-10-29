// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Static nav type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each nav type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime and these
// values are the fallback when the probe cannot run.
const (
	navHashOdometry = "RIHS01_3cc97dc7fb7502f8714462c526d369e35b603cfc34d946e3f2eda2766dfec6e0"
	navHashPath     = "RIHS01_1957a5bb3cee5da65c4e52e52b65a93df227efce4c20f8458b36e73066ca334b"
)

// Fixed nav sentinels for Odometry and Path.
//
// Header frame_id carries the per-run live ID and child or pose headers
// carry the live ID with a suffix so stale runs never match. Numeric
// fields use fixed values shared by YAML and XCDR1 codec checks.
const (
	navPosX     float64 = 1.0
	navPosY     float64 = 2.0
	navPosZ     float64 = 3.0
	navOrientX  float64 = 0.0
	navOrientY  float64 = 0.0
	navOrientZ  float64 = 0.0
	navOrientW  float64 = 1.0
	navLinearX  float64 = 0.5
	navLinearY  float64 = -0.5
	navLinearZ  float64 = 1.0
	navAngularX float64 = 0.1
	navAngularY float64 = 0.2
	navAngularZ float64 = 0.3
	navCovLen           = 36
)

// navCovarianceYAML returns YAML flow list with 36 zero entries.
func navCovarianceYAML() string {
	parts := make([]string, 0, navCovLen)
	for range navCovLen {
		parts = append(parts, "0.0")
	}
	return strings.Join(parts, ", ")
}

// yamlNavOdometry returns ROS YAML for Odometry with live frame IDs.
func yamlNavOdometry(live string) string {
	cov := navCovarianceYAML()
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, child_frame_id: '%s-child', pose: {pose: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}, covariance: [%s]}, twist: {twist: {linear: {x: 0.5, y: -0.5, z: 1.0}, angular: {x: 0.1, y: 0.2, z: 0.3}}, covariance: [%s]}}", live, live, cov, cov)
}

// encodeNavPose writes Point plus Quaternion in Pose order.
func encodeNavPose(enc *types.Encoder) {
	enc.WriteFloat64(navPosX)
	enc.WriteFloat64(navPosY)
	enc.WriteFloat64(navPosZ)
	enc.WriteFloat64(navOrientX)
	enc.WriteFloat64(navOrientY)
	enc.WriteFloat64(navOrientZ)
	enc.WriteFloat64(navOrientW)
}

// encodeNavCovariance writes 36 zero covariance entries.
func encodeNavCovariance(enc *types.Encoder) {
	for range navCovLen {
		enc.WriteFloat64(0.0)
	}
}

// encodeNavOdometry encodes Odometry with live frame IDs as XCDR1.
func encodeNavOdometry(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode odometry header: %w", err)
	}
	err = enc.WriteString(live + "-child")
	if err != nil {
		return nil, fmt.Errorf("encode odometry child: %w", err)
	}
	encodeNavPose(enc)
	encodeNavCovariance(enc)
	enc.WriteFloat64(navLinearX)
	enc.WriteFloat64(navLinearY)
	enc.WriteFloat64(navLinearZ)
	enc.WriteFloat64(navAngularX)
	enc.WriteFloat64(navAngularY)
	enc.WriteFloat64(navAngularZ)
	encodeNavCovariance(enc)
	return enc.Bytes(), nil
}

// readNavPosition reads position xyz with checks.
func readNavPosition(dec *types.Decoder, label string) (float64, float64, float64, error) {
	x, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode %s position x: %w", label, err)
	}
	y, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode %s position y: %w", label, err)
	}
	z, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode %s position z: %w", label, err)
	}
	return x, y, z, nil
}

// readNavOrientation reads orientation xyzw with checks.
func readNavOrientation(dec *types.Decoder, label string) (float64, float64, float64, float64, error) {
	ox, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("decode %s orientation x: %w", label, err)
	}
	oy, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("decode %s orientation y: %w", label, err)
	}
	oz, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("decode %s orientation z: %w", label, err)
	}
	w, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("decode %s orientation w: %w", label, err)
	}
	return ox, oy, oz, w, nil
}

// decodeNavPose reads Point plus Quaternion and checks sentinels.
func decodeNavPose(dec *types.Decoder, label string) error {
	x, y, z, err := readNavPosition(dec, label)
	if err != nil {
		return err
	}
	if x != navPosX || y != navPosY || z != navPosZ {
		return fmt.Errorf("%s position (%v, %v, %v) wants (1.0, 2.0, 3.0)", label, x, y, z)
	}
	ox, oy, oz, w, err := readNavOrientation(dec, label)
	if err != nil {
		return err
	}
	if ox != navOrientX || oy != navOrientY || oz != navOrientZ || w != navOrientW {
		return fmt.Errorf("%s orientation (%v, %v, %v, %v) wants (0, 0, 0, 1)", label, ox, oy, oz, w)
	}
	return nil
}

// decodeNavCovariance reads 36 entries and checks all zeros.
func decodeNavCovariance(dec *types.Decoder, label string) error {
	for i := range navCovLen {
		v, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode %s covariance %d: %w", label, i, err)
		}
		if v != 0.0 {
			return fmt.Errorf("%s covariance %d = %v wants 0.0", label, i, v)
		}
	}
	return nil
}

// readNavLinear reads odometry linear xyz with checks.
func readNavLinear(dec *types.Decoder) (float64, float64, float64, error) {
	lx, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode odometry linear x: %w", err)
	}
	ly, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode odometry linear y: %w", err)
	}
	lz, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode odometry linear z: %w", err)
	}
	return lx, ly, lz, nil
}

// readNavAngular reads odometry angular xyz with checks.
func readNavAngular(dec *types.Decoder) (float64, float64, float64, error) {
	ax, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode odometry angular x: %w", err)
	}
	ay, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode odometry angular y: %w", err)
	}
	az, err := dec.ReadFloat64()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("decode odometry angular z: %w", err)
	}
	return ax, ay, az, nil
}

// decodeNavOdometryHead checks frame plus child plus pose plus covariance.
func decodeNavOdometryHead(dec *types.Decoder, live string) error {
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode odometry frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("odometry frame %q wants %q", frame, live)
	}
	child, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode odometry child: %w", err)
	}
	if child != live+"-child" {
		return fmt.Errorf("odometry child %q wants %q", child, live+"-child")
	}
	err = decodeNavPose(dec, "odometry")
	if err != nil {
		return err
	}
	err = decodeNavCovariance(dec, "odometry pose")
	if err != nil {
		return err
	}
	return nil
}

// decodeNavOdometryTwist checks linear plus angular plus twist covariance.
func decodeNavOdometryTwist(dec *types.Decoder) error {
	lx, ly, lz, err := readNavLinear(dec)
	if err != nil {
		return err
	}
	if lx != navLinearX || ly != navLinearY || lz != navLinearZ {
		return fmt.Errorf("odometry linear (%v, %v, %v) wants (0.5, -0.5, 1.0)", lx, ly, lz)
	}
	ax, ay, az, err := readNavAngular(dec)
	if err != nil {
		return err
	}
	if ax != navAngularX || ay != navAngularY || az != navAngularZ {
		return fmt.Errorf("odometry angular (%v, %v, %v) wants (0.1, 0.2, 0.3)", ax, ay, az)
	}
	err = decodeNavCovariance(dec, "odometry twist")
	if err != nil {
		return err
	}
	return nil
}

// decodeNavOdometry decodes and checks live frame IDs plus sentinels.
func decodeNavOdometry(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode odometry header: %w", err)
	}
	err = decodeNavOdometryHead(dec, live)
	if err != nil {
		return err
	}
	err = decodeNavOdometryTwist(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish odometry: %w", err)
	}
	return nil
}

// wantNavOdometry returns the live ID expected in echo output.
func wantNavOdometry(live string) string {
	return live
}

// yamlNavPath returns ROS YAML for Path with live frame IDs.
func yamlNavPath(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, poses: [{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s-pose'}, pose: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}}]}", live, live)
}

// encodeNavPath encodes Path with one PoseStamped as XCDR1.
func encodeNavPath(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode path header: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode path length: %w", err)
	}
	err = encodeMatrixHeader(enc, live+"-pose")
	if err != nil {
		return nil, fmt.Errorf("encode path pose header: %w", err)
	}
	encodeNavPose(enc)
	return enc.Bytes(), nil
}

// decodeNavPath decodes and checks live frame IDs plus pose sentinels.
func decodeNavPath(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode path header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode path frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("path frame %q wants %q", frame, live)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode path length: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("path length %d wants 1", n)
	}
	poseFrame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode path pose frame: %w", err)
	}
	if poseFrame != live+"-pose" {
		return fmt.Errorf("path pose frame %q wants %q", poseFrame, live+"-pose")
	}
	err = decodeNavPose(dec, "path")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish path: %w", err)
	}
	return nil
}

// wantNavPath returns the live ID expected in echo output.
func wantNavPath(live string) string {
	return live
}

// navTypeCases returns the live nav matrix in corpus order.
func navTypeCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "nav-odometry",
			RosType:    "nav_msgs/msg/Odometry",
			IDLName:    "nav_msgs__msg__Odometry",
			staticHash: navHashOdometry,
			yaml:       yamlNavOdometry,
			encode:     encodeNavOdometry,
			decode:     decodeNavOdometry,
			want:       wantNavOdometry,
		},
		{
			Label:      "nav-path",
			RosType:    "nav_msgs/msg/Path",
			IDLName:    "nav_msgs__msg__Path",
			staticHash: navHashPath,
			yaml:       yamlNavPath,
			encode:     encodeNavPath,
			decode:     decodeNavPath,
			want:       wantNavPath,
		},
	}
}

// runNavLabel runs forward plus reverse for one nav label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. Forward subscribes with the RIHS01 hash and takes
// via the Weld backend. Reverse publishes via the Weld backend and watches
// ros2 topic echo. Domain 42 and docker argv handling come from the shared
// matrix helpers.
func runNavLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, navTypeCases())
}

// TestROSTypeNavOdometry checks Odometry forward plus reverse.
//
// Forward starts ros2 topic pub for nav_msgs Odometry and decodes via
// NewDecoder on the Weld take path with Subscribe hash. Reverse publishes
// the fixed sentinels via Weld and watches ros2 topic echo. Header
// frame_id and child_frame_id carry per-run live IDs so stale runs never
// match.
func TestROSTypeNavOdometry(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runNavLabel(t, ctx, domain, "nav-odometry")
}

// TestROSTypeNavPath checks Path forward plus reverse.
//
// Forward starts ros2 topic pub for nav_msgs Path and decodes via
// NewDecoder on the Weld take path with Subscribe hash. Reverse publishes
// one PoseStamped via Weld and watches ros2 topic echo. Outer plus inner
// frame IDs carry per-run live IDs so stale runs never match.
func TestROSTypeNavPath(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runNavLabel(t, ctx, domain, "nav-path")
}
