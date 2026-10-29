// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:dupl // live gate decoders repeat similar wire checks across types
package ros_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Extra geometry static type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each ROS type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime, these values
// are the fallback when the probe cannot run. Values were captured from
// weld-jazzy Jazzy.
const (
	extraHashPose       = "RIHS01_d501954e9476cea2996984e812054b68026ae0bfae789d9a10b23daf35cc90fa"
	extraHashQuaternion = "RIHS01_8a765f66778c8ff7c8ab94afcc590a2ed5325a1d9a076ffff38fbce36f458684"
	extraHashTwist      = "RIHS01_9c45bf16fe0983d80e3cfe750d6835843d265a9a6c46bd2e609fcddde6fb8d2a"
)

// Extra geometry fixed sentinels for types without a string field.
//
// Pose, Quaternion, and Twist carry no Header and no frame_id, so the
// per-run live ID cannot ride in the payload. Isolation uses per-run topic
// plus domain 42, matching the Vector3 matrix case. Values stay fixed so
// Weld take and ros2 echo checks are deterministic.
const (
	extraPosePosX  float64 = 1.0
	extraPosePosY  float64 = 2.0
	extraPosePosZ  float64 = 3.0
	extraPoseOriX  float64 = 0.0
	extraPoseOriY  float64 = 0.0
	extraPoseOriZ  float64 = 0.0
	extraPoseOriW  float64 = 1.0
	extraQuatX     float64 = 0.0
	extraQuatY     float64 = 0.0
	extraQuatZ     float64 = 0.0
	extraQuatW     float64 = 1.0
	extraTwistLinX float64 = 1.0
	extraTwistLinY float64 = 0.0
	extraTwistLinZ float64 = 0.0
	extraTwistAngX float64 = 0.0
	extraTwistAngY float64 = 0.0
	extraTwistAngZ float64 = 0.5
)

// yamlExtraPose returns ROS YAML for the Pose sentinel.
func yamlExtraPose(live string) string {
	return "{position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}"
}

// encodeExtraPose encodes the fixed Pose sentinel as XCDR1.
func encodeExtraPose(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteFloat64(extraPosePosX)
	enc.WriteFloat64(extraPosePosY)
	enc.WriteFloat64(extraPosePosZ)
	enc.WriteFloat64(extraPoseOriX)
	enc.WriteFloat64(extraPoseOriY)
	enc.WriteFloat64(extraPoseOriZ)
	enc.WriteFloat64(extraPoseOriW)
	return enc.Bytes(), nil
}

// readExtraPosePosition reads pose position xyz with checks.
func readExtraPosePosition(dec *types.Decoder) (float64, float64, float64, error) {
	px, py, pz, err := helpers.ReadFloat64Triple(dec, "decode pose position x", "decode pose position y", "decode pose position z")
	if err != nil {
		return 0, 0, 0, fmt.Errorf("read pose position: %w", err)
	}
	return px, py, pz, nil
}

// readExtraPoseOrientation reads pose orientation xyzw with checks.
func readExtraPoseOrientation(dec *types.Decoder) (float64, float64, float64, float64, error) {
	ox, oy, oz, ow, err := helpers.ReadFloat64Quad(dec, "decode pose orientation x", "decode pose orientation y", "decode pose orientation z", "decode pose orientation w")
	if err != nil {
		return 0, 0, 0, 0, fmt.Errorf("read pose orientation: %w", err)
	}
	return ox, oy, oz, ow, nil
}

// decodeExtraPose decodes and checks the Pose sentinel.
func decodeExtraPose(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode pose header: %w", err)
	}
	px, py, pz, err := readExtraPosePosition(dec)
	if err != nil {
		return err
	}
	ox, oy, oz, ow, err := readExtraPoseOrientation(dec)
	if err != nil {
		return err
	}
	if px != extraPosePosX || py != extraPosePosY || pz != extraPosePosZ {
		return fmt.Errorf("pose position (%v, %v, %v) wants (1.0, 2.0, 3.0)", px, py, pz)
	}
	if ox != extraPoseOriX || oy != extraPoseOriY || oz != extraPoseOriZ || ow != extraPoseOriW {
		return fmt.Errorf("pose orientation (%v, %v, %v, %v) wants (0.0, 0.0, 0.0, 1.0)", ox, oy, oz, ow)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish pose: %w", err)
	}
	return nil
}

// wantExtraPose returns the echo substring for the Pose sentinel.
func wantExtraPose(live string) string {
	return "z: 3.0"
}

// yamlExtraQuaternion returns ROS YAML for the Quaternion sentinel.
func yamlExtraQuaternion(live string) string {
	return "{x: 0.0, y: 0.0, z: 0.0, w: 1.0}"
}

// encodeExtraQuaternion encodes the fixed Quaternion sentinel as XCDR1.
func encodeExtraQuaternion(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteFloat64(extraQuatX)
	enc.WriteFloat64(extraQuatY)
	enc.WriteFloat64(extraQuatZ)
	enc.WriteFloat64(extraQuatW)
	return enc.Bytes(), nil
}

// decodeExtraQuaternion decodes and checks the Quaternion sentinel.
func decodeExtraQuaternion(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode quaternion header: %w", err)
	}
	x, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode quaternion x: %w", err)
	}
	y, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode quaternion y: %w", err)
	}
	z, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode quaternion z: %w", err)
	}
	w, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode quaternion w: %w", err)
	}
	if x != extraQuatX || y != extraQuatY || z != extraQuatZ || w != extraQuatW {
		return fmt.Errorf("quaternion (%v, %v, %v, %v) wants (0.0, 0.0, 0.0, 1.0)", x, y, z, w)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish quaternion: %w", err)
	}
	return nil
}

// wantExtraQuaternion returns the echo substring for the Quaternion sentinel.
func wantExtraQuaternion(live string) string {
	return "w: 1.0"
}

// yamlExtraTwist returns ROS YAML for the Twist sentinel.
func yamlExtraTwist(live string) string {
	return "{linear: {x: 1.0, y: 0.0, z: 0.0}, angular: {x: 0.0, y: 0.0, z: 0.5}}"
}

// encodeExtraTwist encodes the fixed Twist sentinel as XCDR1.
func encodeExtraTwist(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteFloat64(extraTwistLinX)
	enc.WriteFloat64(extraTwistLinY)
	enc.WriteFloat64(extraTwistLinZ)
	enc.WriteFloat64(extraTwistAngX)
	enc.WriteFloat64(extraTwistAngY)
	enc.WriteFloat64(extraTwistAngZ)
	return enc.Bytes(), nil
}

// readExtraTwistLinear reads twist linear xyz with checks.
func readExtraTwistLinear(dec *types.Decoder) (float64, float64, float64, error) {
	lx, ly, lz, err := helpers.ReadFloat64Triple(dec, "decode twist linear x", "decode twist linear y", "decode twist linear z")
	if err != nil {
		return 0, 0, 0, fmt.Errorf("read twist linear: %w", err)
	}
	return lx, ly, lz, nil
}

// readExtraTwistAngular reads twist angular xyz with checks.
func readExtraTwistAngular(dec *types.Decoder) (float64, float64, float64, error) {
	ax, ay, az, err := helpers.ReadFloat64Triple(dec, "decode twist angular x", "decode twist angular y", "decode twist angular z")
	if err != nil {
		return 0, 0, 0, fmt.Errorf("read twist angular: %w", err)
	}
	return ax, ay, az, nil
}

// decodeExtraTwist decodes and checks the Twist sentinel.
func decodeExtraTwist(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode twist header: %w", err)
	}
	lx, ly, lz, err := readExtraTwistLinear(dec)
	if err != nil {
		return err
	}
	ax, ay, az, err := readExtraTwistAngular(dec)
	if err != nil {
		return err
	}
	if lx != extraTwistLinX || ly != extraTwistLinY || lz != extraTwistLinZ {
		return fmt.Errorf("twist linear (%v, %v, %v) wants (1.0, 0.0, 0.0)", lx, ly, lz)
	}
	if ax != extraTwistAngX || ay != extraTwistAngY || az != extraTwistAngZ {
		return fmt.Errorf("twist angular (%v, %v, %v) wants (0.0, 0.0, 0.5)", ax, ay, az)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish twist: %w", err)
	}
	return nil
}

// wantExtraTwist returns the echo substring for the Twist sentinel.
func wantExtraTwist(live string) string {
	return "z: 0.5"
}

// extraGeometryCases returns the extra geometry live matrix in fixed order.
func extraGeometryCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "geometry-pose",
			RosType:    "geometry_msgs/msg/Pose",
			IDLName:    "geometry_msgs__msg__Pose",
			staticHash: extraHashPose,
			yaml:       yamlExtraPose,
			encode:     encodeExtraPose,
			decode:     decodeExtraPose,
			want:       wantExtraPose,
		},
		{
			Label:      "geometry-quaternion",
			RosType:    "geometry_msgs/msg/Quaternion",
			IDLName:    "geometry_msgs__msg__Quaternion",
			staticHash: extraHashQuaternion,
			yaml:       yamlExtraQuaternion,
			encode:     encodeExtraQuaternion,
			decode:     decodeExtraQuaternion,
			want:       wantExtraQuaternion,
		},
		{
			Label:      "geometry-twist",
			RosType:    "geometry_msgs/msg/Twist",
			IDLName:    "geometry_msgs__msg__Twist",
			staticHash: extraHashTwist,
			yaml:       yamlExtraTwist,
			encode:     encodeExtraTwist,
			decode:     decodeExtraTwist,
			want:       wantExtraTwist,
		},
	}
}

// runExtraGeometryLabel runs forward plus reverse for one extra geometry label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. Forward and reverse reuse the shared matrix runners
// so domain 42 plus docker argv plus hash probing stay identical.
func runExtraGeometryLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, extraGeometryCases())
}

// TestROSTypeGeometryPose checks Pose forward plus reverse.
//
// Pose carries no string field, so the fixed position plus orientation
// sentinel uses per-run topic plus domain isolation.
func TestROSTypeGeometryPose(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraGeometryLabel(t, ctx, domain, "geometry-pose")
}

// TestROSTypeGeometryQuaternion checks Quaternion forward plus reverse.
//
// Quaternion carries no string field, so the fixed unit sentinel uses
// per-run topic plus domain isolation.
func TestROSTypeGeometryQuaternion(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraGeometryLabel(t, ctx, domain, "geometry-quaternion")
}

// TestROSTypeGeometryTwist checks Twist forward plus reverse.
//
// Twist carries no string field, so the fixed linear plus angular sentinel
// uses per-run topic plus domain isolation.
func TestROSTypeGeometryTwist(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runExtraGeometryLabel(t, ctx, domain, "geometry-twist")
}
