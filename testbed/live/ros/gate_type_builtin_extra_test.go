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

// Builtin extra coverage documents Tier 0 completeness.
//
// Corpus Tier 0 holds exactly two entries, builtin_interfaces Duration and
// builtin_interfaces Time, each with Summary int32 sec plus uint32 nanosec.
// Both base shapes are already covered by gate_type_builtin_test.go with the
// fixed sentinel sec 42 plus nanosec 123 for Time and Duration. This file
// adds edge shapes on the same wire layout and the same RIHS01 hashes: Time
// zero, Time max, Duration zero, and Duration negative. It reuses the shared
// matrix runners so domain 42, docker argv, SEDP hash resolve, and per-run
// topic isolation stay identical to the base matrix.
const (
	builtinExtraTimeZeroSec        int32  = 0
	builtinExtraTimeZeroNanosec    uint32 = 0
	builtinExtraTimeMaxSec         int32  = 2147483647
	builtinExtraTimeMaxNanosec     uint32 = 999999999
	builtinExtraDurationZeroSec    int32  = 0
	builtinExtraDurationZeroNanos  uint32 = 0
	builtinExtraDurationNegSec     int32  = -1
	builtinExtraDurationNegNanosec uint32 = 0
)

// yamlBuiltinExtraTimeZero returns ROS YAML for Time zero.
func yamlBuiltinExtraTimeZero(live string) string {
	return "{sec: 0, nanosec: 0}"
}

// yamlBuiltinExtraTimeMax returns ROS YAML for Time max.
func yamlBuiltinExtraTimeMax(live string) string {
	return "{sec: 2147483647, nanosec: 999999999}"
}

// yamlBuiltinExtraDurationZero returns ROS YAML for Duration zero.
func yamlBuiltinExtraDurationZero(live string) string {
	return "{sec: 0, nanosec: 0}"
}

// yamlBuiltinExtraDurationNeg returns ROS YAML for Duration negative.
func yamlBuiltinExtraDurationNeg(live string) string {
	return "{sec: -1, nanosec: 0}"
}

// encodeBuiltinExtraPair encodes sec plus nanosec as XCDR1.
func encodeBuiltinExtraPair(sec int32, nanosec uint32) []byte {
	enc := types.NewEncoder(true)
	enc.WriteInt32(sec)
	enc.WriteUint32(nanosec)
	return enc.Bytes()
}

// encodeBuiltinExtraTimeZero encodes Time zero as XCDR1.
func encodeBuiltinExtraTimeZero(live string) ([]byte, error) {
	return encodeBuiltinExtraPair(builtinExtraTimeZeroSec, builtinExtraTimeZeroNanosec), nil
}

// encodeBuiltinExtraTimeMax encodes Time max as XCDR1.
func encodeBuiltinExtraTimeMax(live string) ([]byte, error) {
	return encodeBuiltinExtraPair(builtinExtraTimeMaxSec, builtinExtraTimeMaxNanosec), nil
}

// encodeBuiltinExtraDurationZero encodes Duration zero as XCDR1.
func encodeBuiltinExtraDurationZero(live string) ([]byte, error) {
	return encodeBuiltinExtraPair(builtinExtraDurationZeroSec, builtinExtraDurationZeroNanos), nil
}

// encodeBuiltinExtraDurationNeg encodes Duration negative as XCDR1.
func encodeBuiltinExtraDurationNeg(live string) ([]byte, error) {
	return encodeBuiltinExtraPair(builtinExtraDurationNegSec, builtinExtraDurationNegNanosec), nil
}

// decodeBuiltinExtraPair decodes and checks sec plus nanosec with label.
func decodeBuiltinExtraPair(payload []byte, wantSec int32, wantNanosec uint32, label string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode %s header: %w", label, err)
	}
	sec, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("decode %s sec: %w", label, err)
	}
	nanosec, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode %s nanosec: %w", label, err)
	}
	if sec != wantSec || nanosec != wantNanosec {
		return fmt.Errorf("%s (%d, %d) wants (%d, %d)", label, sec, nanosec, wantSec, wantNanosec)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish %s: %w", label, err)
	}
	return nil
}

// decodeBuiltinExtraTimeZero decodes and checks Time zero.
func decodeBuiltinExtraTimeZero(payload []byte, live string) error {
	err := decodeBuiltinExtraPair(payload, builtinExtraTimeZeroSec, builtinExtraTimeZeroNanosec, "time-zero")
	if err != nil {
		return fmt.Errorf("decode time-zero pair: %w", err)
	}
	return nil
}

// decodeBuiltinExtraTimeMax decodes and checks Time max.
func decodeBuiltinExtraTimeMax(payload []byte, live string) error {
	err := decodeBuiltinExtraPair(payload, builtinExtraTimeMaxSec, builtinExtraTimeMaxNanosec, "time-max")
	if err != nil {
		return fmt.Errorf("decode time-max pair: %w", err)
	}
	return nil
}

// decodeBuiltinExtraDurationZero decodes and checks Duration zero.
func decodeBuiltinExtraDurationZero(payload []byte, live string) error {
	err := decodeBuiltinExtraPair(payload, builtinExtraDurationZeroSec, builtinExtraDurationZeroNanos, "duration-zero")
	if err != nil {
		return fmt.Errorf("decode duration-zero pair: %w", err)
	}
	return nil
}

// decodeBuiltinExtraDurationNeg decodes and checks Duration negative.
func decodeBuiltinExtraDurationNeg(payload []byte, live string) error {
	err := decodeBuiltinExtraPair(payload, builtinExtraDurationNegSec, builtinExtraDurationNegNanosec, "duration-neg")
	if err != nil {
		return fmt.Errorf("decode duration-neg pair: %w", err)
	}
	return nil
}

// wantBuiltinExtraTimeZero returns the echo substring for Time zero.
func wantBuiltinExtraTimeZero(live string) string {
	return "sec: 0"
}

// wantBuiltinExtraTimeMax returns the echo substring for Time max.
func wantBuiltinExtraTimeMax(live string) string {
	return "sec: 2147483647"
}

// wantBuiltinExtraDurationZero returns the echo substring for Duration zero.
func wantBuiltinExtraDurationZero(live string) string {
	return "sec: 0"
}

// wantBuiltinExtraDurationNeg returns the echo substring for Duration negative.
func wantBuiltinExtraDurationNeg(live string) string {
	return "sec: -1"
}

// builtinExtraCases returns the extra builtin matrix in corpus order.
//
// Time variants come before Duration variants. All four share the base
// RIHS01 hashes from gate_type_builtin_test.go because the wire layout is
// identical and only the sentinel values change.
func builtinExtraCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "builtin-time-zero",
			RosType:    "builtin_interfaces/msg/Time",
			IDLName:    "builtin_interfaces__msg__Time",
			staticHash: builtinTimeHash,
			yaml:       yamlBuiltinExtraTimeZero,
			encode:     encodeBuiltinExtraTimeZero,
			decode:     decodeBuiltinExtraTimeZero,
			want:       wantBuiltinExtraTimeZero,
		},
		{
			Label:      "builtin-time-max",
			RosType:    "builtin_interfaces/msg/Time",
			IDLName:    "builtin_interfaces__msg__Time",
			staticHash: builtinTimeHash,
			yaml:       yamlBuiltinExtraTimeMax,
			encode:     encodeBuiltinExtraTimeMax,
			decode:     decodeBuiltinExtraTimeMax,
			want:       wantBuiltinExtraTimeMax,
		},
		{
			Label:      "builtin-duration-zero",
			RosType:    "builtin_interfaces/msg/Duration",
			IDLName:    "builtin_interfaces__msg__Duration",
			staticHash: builtinDurationHash,
			yaml:       yamlBuiltinExtraDurationZero,
			encode:     encodeBuiltinExtraDurationZero,
			decode:     decodeBuiltinExtraDurationZero,
			want:       wantBuiltinExtraDurationZero,
		},
		{
			Label:      "builtin-duration-neg",
			RosType:    "builtin_interfaces/msg/Duration",
			IDLName:    "builtin_interfaces__msg__Duration",
			staticHash: builtinDurationHash,
			yaml:       yamlBuiltinExtraDurationNeg,
			encode:     encodeBuiltinExtraDurationNeg,
			decode:     decodeBuiltinExtraDurationNeg,
			want:       wantBuiltinExtraDurationNeg,
		},
	}
}

// runBuiltinExtraLabel runs forward plus reverse for one extra builtin label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case in the extra set, skips when the peer lacks the
// type, then runs forward with one live ID and reverse with another so the
// two directions never share a topic. It reuses the shared matrix runners
// so docker argv, domain 42, timeouts, SEDP hash wiring, and XCDR checks
// stay identical to the base matrix.
func runBuiltinExtraLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, builtinExtraCases())
}

// TestROSTypeBuiltinTimeZero checks Time zero forward plus reverse.
//
// Forward starts ros2 topic pub for builtin Time zero and decodes via
// NewDecoder on the Weld take path. Reverse publishes the zero sentinel via
// Weld and watches ros2 topic echo. The sentinel uses per-run topic plus
// domain isolation because Time carries no string field.
func TestROSTypeBuiltinTimeZero(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runBuiltinExtraLabel(t, ctx, domain, "builtin-time-zero")
}

// TestROSTypeBuiltinTimeMax checks Time max forward plus reverse.
//
// It mirrors the zero case with sec 2147483647 plus nanosec 999999999 and
// the same topic plus domain isolation.
func TestROSTypeBuiltinTimeMax(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runBuiltinExtraLabel(t, ctx, domain, "builtin-time-max")
}

// TestROSTypeBuiltinDurationZero checks Duration zero forward plus reverse.
//
// Forward starts ros2 topic pub for builtin Duration zero and decodes via
// NewDecoder on the Weld take path. Reverse publishes the zero sentinel via
// Weld and watches ros2 topic echo. The sentinel uses per-run topic plus
// domain isolation because Duration carries no string field.
func TestROSTypeBuiltinDurationZero(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runBuiltinExtraLabel(t, ctx, domain, "builtin-duration-zero")
}

// TestROSTypeBuiltinDurationNeg checks Duration negative forward plus reverse.
//
// It mirrors the zero case with sec -1 plus nanosec 0 and the same topic
// plus domain isolation. Negative Duration exercises the signed int32 path
// that Time zero and Time max do not cover.
func TestROSTypeBuiltinDurationNeg(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runBuiltinExtraLabel(t, ctx, domain, "builtin-duration-neg")
}
