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

// Static type hash from ros2 topic info -v on weld-jazzy.
//
// This is the RIHS01 hash reported for JointTrajectoryPoint on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime; this value
// is the fallback when the probe cannot run. Value was captured from
// weld-jazzy Jazzy.
const trajectoryHashJointTrajectoryPoint = "RIHS01_de8907036d8bd45aac6f30cc9044a3d4a329c42cbf719aff7d95a584cfa532d7"

// Fixed sentinels for JointTrajectoryPoint.
//
// The type carries no string field, so per-run isolation relies on per-run
// topic plus domain isolation, the same as Int32 and Vector3 in the matrix.
// Positions holds two elements so sequence length handling is exercised.
// Velocities, accelerations, and effort hold one element each.
const (
	trajectoryPos0  = 42.5
	trajectoryPos1  = 1.0
	trajectoryVel0  = 2.5
	trajectoryAcc0  = 3.5
	trajectoryEff0  = 4.5
	trajectorySec   = int32(1)
	trajectoryNanos = uint32(2)
	trajectoryRos   = "trajectory_msgs/msg/JointTrajectoryPoint"
	trajectoryIDL   = "trajectory_msgs__msg__JointTrajectoryPoint"
	trajectoryLabel = "trajectory-jointtrajectorypoint"
)

// yamlTrajectoryPoint returns ROS YAML for the fixed sentinel.
//
// Arrays are fixed so the only per-run isolation is the topic plus domain.
// Duration is fixed at 1 sec plus 2 nanosec.
func yamlTrajectoryPoint(live string) string {
	return "{positions: [42.5, 1.0], velocities: [2.5], accelerations: [3.5], effort: [4.5], time_from_start: {sec: 1, nanosec: 2}}"
}

// encodeTrajectoryPoint encodes the fixed sentinel as XCDR1.
//
// Layout is positions plus velocities plus accelerations plus effort as
// float64 sequences, then Duration as int32 sec plus uint32 nanosec.
// Only the sequence length writes can fail.
func encodeTrajectoryPoint(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode trajectory positions length: %w", err)
	}
	enc.WriteFloat64(trajectoryPos0)
	enc.WriteFloat64(trajectoryPos1)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode trajectory velocities length: %w", err)
	}
	enc.WriteFloat64(trajectoryVel0)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode trajectory accelerations length: %w", err)
	}
	enc.WriteFloat64(trajectoryAcc0)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode trajectory effort length: %w", err)
	}
	enc.WriteFloat64(trajectoryEff0)
	enc.WriteInt32(trajectorySec)
	enc.WriteUint32(trajectoryNanos)
	return enc.Bytes(), nil
}

// decodeTrajectorySequence reads one float64 sequence and checks values.
//
// It reads the length prefix, checks the count, then reads each element
// and checks against want. Label names the sequence for error messages.
func decodeTrajectorySequence(dec *types.Decoder, label string, want []float64) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode trajectory %s length: %w", label, err)
	}
	if n != len(want) {
		return fmt.Errorf("trajectory %s length %d wants %d", label, n, len(want))
	}
	for i, w := range want {
		got, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode trajectory %s %d: %w", label, i, err)
		}
		if got != w {
			return fmt.Errorf("trajectory %s %d %v wants %v", label, i, got, w)
		}
	}
	return nil
}

// decodeTrajectoryPoint decodes and checks the fixed sentinel.
func decodeTrajectoryPoint(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode trajectory header: %w", err)
	}
	err = decodeTrajectorySequence(dec, "positions", []float64{trajectoryPos0, trajectoryPos1})
	if err != nil {
		return err
	}
	err = decodeTrajectorySequence(dec, "velocities", []float64{trajectoryVel0})
	if err != nil {
		return err
	}
	err = decodeTrajectorySequence(dec, "accelerations", []float64{trajectoryAcc0})
	if err != nil {
		return err
	}
	err = decodeTrajectorySequence(dec, "effort", []float64{trajectoryEff0})
	if err != nil {
		return err
	}
	sec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode trajectory sec: %w", err)
	}
	if sec != trajectorySec {
		return fmt.Errorf("trajectory sec %d wants %d", sec, trajectorySec)
	}
	nanos, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode trajectory nanosec: %w", err)
	}
	if nanos != trajectoryNanos {
		return fmt.Errorf("trajectory nanosec %d wants %d", nanos, trajectoryNanos)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish trajectory: %w", err)
	}
	return nil
}

// wantTrajectoryPoint returns the echo substring for the sentinel.
//
// Positions start with 42.5, so the echo stream must hold that text.
func wantTrajectoryPoint(live string) string {
	return "42.5"
}

// trajectoryPointCase returns the matrix case for JointTrajectoryPoint.
//
// It reuses typeMatrixCase plus runMatrixForward plus runMatrixReverse so
// the trajectory gate shares domain 42, timeouts, docker argv, hash
// resolve, retry, and skip behavior with the matrix.
func trajectoryPointCase() typeMatrixCase {
	return typeMatrixCase{
		Label:      trajectoryLabel,
		RosType:    trajectoryRos,
		IDLName:    trajectoryIDL,
		staticHash: trajectoryHashJointTrajectoryPoint,
		yaml:       yamlTrajectoryPoint,
		encode:     encodeTrajectoryPoint,
		decode:     decodeTrajectoryPoint,
		want:       wantTrajectoryPoint,
	}
}

// TestROSTypeTrajectoryJointTrajectoryPoint checks JointTrajectoryPoint forward plus reverse.
//
// Forward starts ros2 topic pub for trajectory_msgs JointTrajectoryPoint
// and decodes via NewDecoder on the Weld take path. Reverse publishes the
// fixed sentinel via Weld and watches ros2 topic echo for 42.5. The type
// carries no string field, so per-run topic plus domain isolation keeps
// concurrent runs apart. It skips cleanly when the peer or the type is
// absent and never starts the compose stack.
func TestROSTypeTrajectoryJointTrajectoryPoint(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	tc := trajectoryPointCase()
	skipIfNoTypeInterface(t, ctx, domain, tc.RosType)
	liveFwd := helpers.LiveID("ros-fwd-" + trajectoryLabel)
	topicFwd := helpers.LiveTopic(liveFwd)
	runMatrixForward(t, ctx, domain, tc, liveFwd, topicFwd)
	liveRev := helpers.LiveID("ros-rev-" + trajectoryLabel)
	topicRev := helpers.LiveTopic(liveRev)
	runMatrixReverse(t, ctx, domain, tc, liveRev, topicRev)
}
