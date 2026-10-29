// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop,dupl // test breadth repeats similar wire branches across decoders
package ros_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Extra trajectory static type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each ROS type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime, these values
// are the fallback when the probe cannot run. Values were captured from
// weld-jazzy Jazzy.
const (
	trajExtraHashJointTrajectory = "RIHS01_179b33eba59d676f6d967ac71fe35e7ca2f64b2f3928f4a018cec115e213796e"
	trajExtraHashMDOF            = "RIHS01_3a18fd095292a65cfde8833c72985a30af981f3ec44494655c6267262b443a4a"
	trajExtraHashMDOFPoint       = "RIHS01_6ada1085b5ee64eaa069b074968e69f0e27c8c5e6f5bb0586dd1c834ef0e32b8"
)

// Extra trajectory fixed sentinels.
//
// JointTrajectory and MultiDOFJointTrajectory carry a Header, so the per-run
// live ID rides in frame_id and stale runs never match. Joint names stay
// fixed at joint1 and points stay at one element so the wire stays small.
// MultiDOFJointTrajectoryPoint carries no string field, so per-run topic
// plus domain isolation is used, matching the JointTrajectoryPoint gate.
// All float sentinels use exact binary fractions so local codec checks are
// stable. Duration stays at 1 sec plus 2 nanosec across all three types.
const (
	trajExtraJointName = "joint1"
	trajExtraPos0      = 42.5
	trajExtraPos1      = 1.0
	trajExtraVel0      = 2.5
	trajExtraAcc0      = 3.5
	trajExtraEff0      = 4.5
	trajExtraSec       = int32(1)
	trajExtraNanos     = uint32(2)

	trajExtraTransX = 1.0
	trajExtraTransY = 2.0
	trajExtraTransZ = 3.0
	trajExtraRotX   = 0.0
	trajExtraRotY   = 0.0
	trajExtraRotZ   = 0.0
	trajExtraRotW   = 1.0

	trajExtraVelLinX = 4.0
	trajExtraVelLinY = 5.0
	trajExtraVelLinZ = 6.0
	trajExtraVelAngX = 7.0
	trajExtraVelAngY = 8.0
	trajExtraVelAngZ = 9.0

	trajExtraAccLinX = 10.0
	trajExtraAccLinY = 11.0
	trajExtraAccLinZ = 12.0
	trajExtraAccAngX = 13.0
	trajExtraAccAngY = 14.0
	trajExtraAccAngZ = 15.0
)

// yamlTrajExtraJointTrajectory returns ROS YAML for JointTrajectory with live frame_id.
func yamlTrajExtraJointTrajectory(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, joint_names: ['joint1'], points: [{positions: [42.5, 1.0], velocities: [2.5], accelerations: [3.5], effort: [4.5], time_from_start: {sec: 1, nanosec: 2}}]}", live)
}

// yamlTrajExtraMDOFPoint returns ROS YAML for the fixed MultiDOFJointTrajectoryPoint.
func yamlTrajExtraMDOFPoint(live string) string {
	return "{transforms: [{translation: {x: 1.0, y: 2.0, z: 3.0}, rotation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}], velocities: [{linear: {x: 4.0, y: 5.0, z: 6.0}, angular: {x: 7.0, y: 8.0, z: 9.0}}], accelerations: [{linear: {x: 10.0, y: 11.0, z: 12.0}, angular: {x: 13.0, y: 14.0, z: 15.0}}], time_from_start: {sec: 1, nanosec: 2}}"
}

// yamlTrajExtraMDOF returns ROS YAML for MultiDOFJointTrajectory with live frame_id.
func yamlTrajExtraMDOF(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, joint_names: ['joint1'], points: [{transforms: [{translation: {x: 1.0, y: 2.0, z: 3.0}, rotation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}], velocities: [{linear: {x: 4.0, y: 5.0, z: 6.0}, angular: {x: 7.0, y: 8.0, z: 9.0}}], accelerations: [{linear: {x: 10.0, y: 11.0, z: 12.0}, angular: {x: 13.0, y: 14.0, z: 15.0}}], time_from_start: {sec: 1, nanosec: 2}}]}", live)
}

// decodeTrajExtraFloatSeq reads one float64 sequence and checks values.
func decodeTrajExtraFloatSeq(dec *types.Decoder, label string, want []float64) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode traj extra %s length: %w", label, err)
	}
	if n != len(want) {
		return fmt.Errorf("traj extra %s length %d wants %d", label, n, len(want))
	}
	for i, w := range want {
		got, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode traj extra %s %d: %w", label, i, err)
		}
		if got != w {
			return fmt.Errorf("traj extra %s %d %v wants %v", label, i, got, w)
		}
	}
	return nil
}

// encodeTrajExtraJointPointFields writes one JointTrajectoryPoint body.
func encodeTrajExtraJointPointFields(enc *types.Encoder) error {
	err := enc.WriteSequenceLength(2)
	if err != nil {
		return fmt.Errorf("encode traj extra positions length: %w", err)
	}
	enc.WriteFloat64(trajExtraPos0)
	enc.WriteFloat64(trajExtraPos1)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode traj extra velocities length: %w", err)
	}
	enc.WriteFloat64(trajExtraVel0)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode traj extra accelerations length: %w", err)
	}
	enc.WriteFloat64(trajExtraAcc0)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode traj extra effort length: %w", err)
	}
	enc.WriteFloat64(trajExtraEff0)
	enc.WriteInt32(trajExtraSec)
	enc.WriteUint32(trajExtraNanos)
	return nil
}

// decodeTrajExtraJointPointFields reads and checks one JointTrajectoryPoint body.
func decodeTrajExtraJointPointFields(dec *types.Decoder) error {
	err := decodeTrajExtraFloatSeq(dec, "positions", []float64{trajExtraPos0, trajExtraPos1})
	if err != nil {
		return err
	}
	err = decodeTrajExtraFloatSeq(dec, "velocities", []float64{trajExtraVel0})
	if err != nil {
		return err
	}
	err = decodeTrajExtraFloatSeq(dec, "accelerations", []float64{trajExtraAcc0})
	if err != nil {
		return err
	}
	err = decodeTrajExtraFloatSeq(dec, "effort", []float64{trajExtraEff0})
	if err != nil {
		return err
	}
	sec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode traj extra sec: %w", err)
	}
	if sec != trajExtraSec {
		return fmt.Errorf("traj extra sec %d wants %d", sec, trajExtraSec)
	}
	nanos, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode traj extra nanosec: %w", err)
	}
	if nanos != trajExtraNanos {
		return fmt.Errorf("traj extra nanosec %d wants %d", nanos, trajExtraNanos)
	}
	return nil
}

// encodeTrajExtraJointTrajectory encodes JointTrajectory with live frame_id as XCDR1.
func encodeTrajExtraJointTrajectory(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode traj extra header: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode traj extra names length: %w", err)
	}
	err = enc.WriteString(trajExtraJointName)
	if err != nil {
		return nil, fmt.Errorf("encode traj extra name: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode traj extra points length: %w", err)
	}
	err = encodeTrajExtraJointPointFields(enc)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeTrajExtraNames checks the single joint_names entry.
func decodeTrajExtraNames(dec *types.Decoder, label string) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode %s names length: %w", label, err)
	}
	if n != 1 {
		return fmt.Errorf("%s names length %d wants 1", label, n)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode %s name: %w", label, err)
	}
	if name != trajExtraJointName {
		return fmt.Errorf("%s name %q wants %q", label, name, trajExtraJointName)
	}
	return nil
}

// decodeTrajExtraPointsLen checks the single points entry length.
func decodeTrajExtraPointsLen(dec *types.Decoder, label string) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode %s points length: %w", label, err)
	}
	if n != 1 {
		return fmt.Errorf("%s points length %d wants 1", label, n)
	}
	return nil
}

// decodeTrajExtraHeader checks the Header frame_id against the live ID.
func decodeTrajExtraHeader(dec *types.Decoder, live, label string) error {
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode %s frame: %w", label, err)
	}
	if frame != live {
		return fmt.Errorf("%s frame %q wants %q", label, frame, live)
	}
	return nil
}

// decodeTrajExtraJointTrajectory decodes and checks live frame_id plus sentinels.
func decodeTrajExtraJointTrajectory(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode traj extra header: %w", err)
	}
	err = decodeTrajExtraHeader(dec, live, "joint trajectory")
	if err != nil {
		return err
	}
	err = decodeTrajExtraNames(dec, "joint trajectory")
	if err != nil {
		return err
	}
	err = decodeTrajExtraPointsLen(dec, "joint trajectory")
	if err != nil {
		return err
	}
	err = decodeTrajExtraJointPointFields(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish traj extra joint trajectory: %w", err)
	}
	return nil
}

// wantTrajExtraJointTrajectory returns the live ID expected in echo output.
func wantTrajExtraJointTrajectory(live string) string {
	return live
}

// encodeTrajExtraMDOFPointFields writes one MultiDOFJointTrajectoryPoint body.
func encodeTrajExtraMDOFPointFields(enc *types.Encoder) error {
	err := enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode mdof extra transforms length: %w", err)
	}
	enc.WriteFloat64(trajExtraTransX)
	enc.WriteFloat64(trajExtraTransY)
	enc.WriteFloat64(trajExtraTransZ)
	enc.WriteFloat64(trajExtraRotX)
	enc.WriteFloat64(trajExtraRotY)
	enc.WriteFloat64(trajExtraRotZ)
	enc.WriteFloat64(trajExtraRotW)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode mdof extra velocities length: %w", err)
	}
	enc.WriteFloat64(trajExtraVelLinX)
	enc.WriteFloat64(trajExtraVelLinY)
	enc.WriteFloat64(trajExtraVelLinZ)
	enc.WriteFloat64(trajExtraVelAngX)
	enc.WriteFloat64(trajExtraVelAngY)
	enc.WriteFloat64(trajExtraVelAngZ)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode mdof extra accelerations length: %w", err)
	}
	enc.WriteFloat64(trajExtraAccLinX)
	enc.WriteFloat64(trajExtraAccLinY)
	enc.WriteFloat64(trajExtraAccLinZ)
	enc.WriteFloat64(trajExtraAccAngX)
	enc.WriteFloat64(trajExtraAccAngY)
	enc.WriteFloat64(trajExtraAccAngZ)
	enc.WriteInt32(trajExtraSec)
	enc.WriteUint32(trajExtraNanos)
	return nil
}

// decodeTrajExtraTransform reads one Transform and checks sentinels.
func decodeTrajExtraTransform(dec *types.Decoder) error {
	tx, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra trans x: %w", err)
	}
	ty, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra trans y: %w", err)
	}
	tz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra trans z: %w", err)
	}
	if tx != trajExtraTransX || ty != trajExtraTransY || tz != trajExtraTransZ {
		return fmt.Errorf("mdof extra trans (%v, %v, %v) wants (1.0, 2.0, 3.0)", tx, ty, tz)
	}
	rx, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra rot x: %w", err)
	}
	ry, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra rot y: %w", err)
	}
	rz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra rot z: %w", err)
	}
	rw, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra rot w: %w", err)
	}
	if rx != trajExtraRotX || ry != trajExtraRotY || rz != trajExtraRotZ || rw != trajExtraRotW {
		return fmt.Errorf("mdof extra rot (%v, %v, %v, %v) wants (0, 0, 0, 1)", rx, ry, rz, rw)
	}
	return nil
}

// decodeTrajExtraTwist reads one Twist and checks want values.
func decodeTrajExtraTwist(dec *types.Decoder, label string, lx, ly, lz, ax, ay, az float64, wantText string) error {
	gotLx, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra %s linear x: %w", label, err)
	}
	gotLy, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra %s linear y: %w", label, err)
	}
	gotLz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra %s linear z: %w", label, err)
	}
	if gotLx != lx || gotLy != ly || gotLz != lz {
		return fmt.Errorf("mdof extra %s linear (%v, %v, %v) wants %s", label, gotLx, gotLy, gotLz, wantText)
	}
	gotAx, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra %s angular x: %w", label, err)
	}
	gotAy, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra %s angular y: %w", label, err)
	}
	gotAz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode mdof extra %s angular z: %w", label, err)
	}
	if gotAx != ax || gotAy != ay || gotAz != az {
		return fmt.Errorf("mdof extra %s angular (%v, %v, %v) wants %s", label, gotAx, gotAy, gotAz, wantText)
	}
	return nil
}

// decodeTrajExtraMDOFPointFields reads and checks one MultiDOFJointTrajectoryPoint body.
func decodeTrajExtraMDOFPointFields(dec *types.Decoder) error {
	nTrans, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode mdof extra transforms length: %w", err)
	}
	if nTrans != 1 {
		return fmt.Errorf("mdof extra transforms length %d wants 1", nTrans)
	}
	err = decodeTrajExtraTransform(dec)
	if err != nil {
		return err
	}
	nVel, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode mdof extra velocities length: %w", err)
	}
	if nVel != 1 {
		return fmt.Errorf("mdof extra velocities length %d wants 1", nVel)
	}
	err = decodeTrajExtraTwist(dec, "velocities", trajExtraVelLinX, trajExtraVelLinY, trajExtraVelLinZ, trajExtraVelAngX, trajExtraVelAngY, trajExtraVelAngZ, "(4, 5, 6) (7, 8, 9)")
	if err != nil {
		return err
	}
	nAcc, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode mdof extra accelerations length: %w", err)
	}
	if nAcc != 1 {
		return fmt.Errorf("mdof extra accelerations length %d wants 1", nAcc)
	}
	err = decodeTrajExtraTwist(dec, "accelerations", trajExtraAccLinX, trajExtraAccLinY, trajExtraAccLinZ, trajExtraAccAngX, trajExtraAccAngY, trajExtraAccAngZ, "(10, 11, 12) (13, 14, 15)")
	if err != nil {
		return err
	}
	sec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode mdof extra sec: %w", err)
	}
	if sec != trajExtraSec {
		return fmt.Errorf("mdof extra sec %d wants %d", sec, trajExtraSec)
	}
	nanos, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode mdof extra nanosec: %w", err)
	}
	if nanos != trajExtraNanos {
		return fmt.Errorf("mdof extra nanosec %d wants %d", nanos, trajExtraNanos)
	}
	return nil
}

// encodeTrajExtraMDOFPoint encodes the fixed MultiDOFJointTrajectoryPoint as XCDR1.
func encodeTrajExtraMDOFPoint(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeTrajExtraMDOFPointFields(enc)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeTrajExtraMDOFPoint decodes and checks the fixed MultiDOFJointTrajectoryPoint.
func decodeTrajExtraMDOFPoint(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode mdof extra header: %w", err)
	}
	err = decodeTrajExtraMDOFPointFields(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish mdof extra point: %w", err)
	}
	return nil
}

// wantTrajExtraMDOFPoint returns the echo substring for the MDOF point sentinel.
func wantTrajExtraMDOFPoint(live string) string {
	return "15.0"
}

// encodeTrajExtraMDOF encodes MultiDOFJointTrajectory with live frame_id as XCDR1.
func encodeTrajExtraMDOF(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode mdof extra header: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode mdof extra names length: %w", err)
	}
	err = enc.WriteString(trajExtraJointName)
	if err != nil {
		return nil, fmt.Errorf("encode mdof extra name: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode mdof extra points length: %w", err)
	}
	err = encodeTrajExtraMDOFPointFields(enc)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeTrajExtraMDOF decodes and checks live frame_id plus MDOF sentinels.
func decodeTrajExtraMDOF(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode mdof extra header: %w", err)
	}
	err = decodeTrajExtraHeader(dec, live, "multidof trajectory")
	if err != nil {
		return err
	}
	err = decodeTrajExtraNames(dec, "multidof trajectory")
	if err != nil {
		return err
	}
	err = decodeTrajExtraPointsLen(dec, "multidof trajectory")
	if err != nil {
		return err
	}
	err = decodeTrajExtraMDOFPointFields(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish mdof extra trajectory: %w", err)
	}
	return nil
}

// wantTrajExtraMDOF returns the live ID expected in echo output.
func wantTrajExtraMDOF(live string) string {
	return live
}

// trajExtraCases returns the extra trajectory live matrix in fixed order.
func trajExtraCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "trajectory-jointtrajectory",
			RosType:    "trajectory_msgs/msg/JointTrajectory",
			IDLName:    "trajectory_msgs__msg__JointTrajectory",
			staticHash: trajExtraHashJointTrajectory,
			yaml:       yamlTrajExtraJointTrajectory,
			encode:     encodeTrajExtraJointTrajectory,
			decode:     decodeTrajExtraJointTrajectory,
			want:       wantTrajExtraJointTrajectory,
		},
		{
			Label:      "trajectory-multidofjointtrajectory",
			RosType:    "trajectory_msgs/msg/MultiDOFJointTrajectory",
			IDLName:    "trajectory_msgs__msg__MultiDOFJointTrajectory",
			staticHash: trajExtraHashMDOF,
			yaml:       yamlTrajExtraMDOF,
			encode:     encodeTrajExtraMDOF,
			decode:     decodeTrajExtraMDOF,
			want:       wantTrajExtraMDOF,
		},
		{
			Label:      "trajectory-multidofjointtrajectorypoint",
			RosType:    "trajectory_msgs/msg/MultiDOFJointTrajectoryPoint",
			IDLName:    "trajectory_msgs__msg__MultiDOFJointTrajectoryPoint",
			staticHash: trajExtraHashMDOFPoint,
			yaml:       yamlTrajExtraMDOFPoint,
			encode:     encodeTrajExtraMDOFPoint,
			decode:     decodeTrajExtraMDOFPoint,
			want:       wantTrajExtraMDOFPoint,
		},
	}
}

// runTrajExtraLabel runs forward plus reverse for one extra trajectory label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. Forward and reverse reuse the shared matrix runners
// so domain 42 plus docker argv plus hash probing stay identical.
func runTrajExtraLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, trajExtraCases())
}

// TestROSTypeTrajectoryJointTrajectory checks JointTrajectory forward plus reverse.
//
// Forward starts ros2 topic pub for trajectory_msgs JointTrajectory and
// decodes via NewDecoder on the Weld take path. Reverse publishes the fixed
// sentinels via Weld and watches ros2 topic echo. Header frame_id carries
// the per-run live ID so stale runs never match.
func TestROSTypeTrajectoryJointTrajectory(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runTrajExtraLabel(t, ctx, domain, "trajectory-jointtrajectory")
}

// TestROSTypeTrajectoryMultiDOFJointTrajectory checks MultiDOFJointTrajectory forward plus reverse.
//
// Forward starts ros2 topic pub for trajectory_msgs MultiDOFJointTrajectory
// and decodes via NewDecoder on the Weld take path. Reverse publishes one
// MultiDOF point via Weld and watches ros2 topic echo. Header frame_id
// carries the per-run live ID so stale runs never match.
func TestROSTypeTrajectoryMultiDOFJointTrajectory(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runTrajExtraLabel(t, ctx, domain, "trajectory-multidofjointtrajectory")
}

// TestROSTypeTrajectoryMultiDOFJointTrajectoryPoint checks MultiDOFJointTrajectoryPoint forward plus reverse.
//
// Forward starts ros2 topic pub for trajectory_msgs
// MultiDOFJointTrajectoryPoint and decodes via NewDecoder on the Weld take
// path. Reverse publishes the fixed sentinel via Weld and watches ros2 topic
// echo for 15.0. The type carries no string field, so per-run topic plus
// domain isolation keeps concurrent runs apart.
func TestROSTypeTrajectoryMultiDOFJointTrajectoryPoint(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runTrajExtraLabel(t, ctx, domain, "trajectory-multidofjointtrajectorypoint")
}
