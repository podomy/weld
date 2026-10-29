// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

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
// This is the RIHS01 hash reported for std_msgs String on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime; this value
// is the fallback when the probe cannot run. It matches the pinned
// backend String hash.
const rest2HashString = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"

// typeRest2Cases returns the remaining std_msgs matrix in corpus order.
//
// String is the only std_msgs type without a matrix gate in a
// gate_type_std file. Int32 plus Float64 plus Bool are covered by the base
// matrix, the extra set covers the small primitives plus Char plus Byte
// plus Empty plus Header, the multi set covers Int16 plus Int64 plus UInt64
// plus all fixed width multi arrays, and the rest set covers dimension
// plus layout plus ColorRGBA plus ByteMultiArray. The round trip covers
// String through the CLI path only, so this set adds the codec plus
// backend matrix path for String without duplicating any existing RosType.
func typeRest2Cases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "std-string",
			RosType:    "std_msgs/msg/String",
			IDLName:    "std_msgs__msg__String",
			staticHash: rest2HashString,
			yaml:       yamlRest2String,
			encode:     encodeRest2String,
			decode:     decodeRest2String,
			want:       wantRest2String,
		},
	}
}

// yamlRest2String returns ROS YAML with the live ID as string data.
func yamlRest2String(live string) string {
	return fmt.Sprintf("{data: '%s'}", live)
}

// encodeRest2String encodes String with the live ID as XCDR1.
func encodeRest2String(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode string data: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeRest2String decodes and checks the live ID.
func decodeRest2String(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode string header: %w", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode string data: %w", err)
	}
	if got != live {
		return fmt.Errorf("string data %q wants %q", got, live)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish string: %w", err)
	}
	return nil
}

// wantRest2String returns the live ID expected in echo output.
func wantRest2String(live string) string {
	return live
}

// runRest2Label runs forward plus reverse for one rest2 matrix label.
//
// Domain is the allocated exclusive domain shared by both directions.
func runRest2Label(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, typeRest2Cases())
}

// TestROSTypeStdString checks String forward plus reverse.
//
// Forward starts ros2 topic pub for the string and decodes via NewDecoder
// on the Weld take path. Reverse publishes the live ID via Weld and
// watches ros2 topic echo. The payload carries the per-run live ID so
// stale runs never match.
func TestROSTypeStdString(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRest2Label(t, ctx, domain, "std-string")
}
