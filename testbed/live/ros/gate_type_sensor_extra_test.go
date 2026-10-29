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

// Extra sensor static type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each ROS type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime, these values
// are the fallback when the probe cannot run. Values were captured from
// weld-jazzy Jazzy.
const (
	sensorExtraHashImu       = "RIHS01_7d9a00ff131080897a5ec7e26e315954b8eae3353c3f995c55faf71574000b5b"
	sensorExtraHashLaserScan = "RIHS01_64c191398013af96509d518dac71d5164f9382553fce5c1f8cca5be7924bd828"
	sensorExtraHashNavSatFix = "RIHS01_62223ab3fe210a15976021da7afddc9e200dc9ec75231c1b6a557fc598a65404"
)

// Extra sensor fixed sentinels.
//
// Imu, LaserScan, and NavSatFix all carry a Header, so the per-run live ID
// rides in frame_id and stale runs never match. Remaining fields stay fixed
// so Weld take and ros2 echo checks are deterministic. LaserScan stays small
// with two ranges plus two intensities. Float values use exact binary
// fractions so local codec checks are stable.
const (
	sensorExtraImuOriX float64 = 0.0
	sensorExtraImuOriY float64 = 0.0
	sensorExtraImuOriZ float64 = 0.0
	sensorExtraImuOriW float64 = 1.0
	sensorExtraImuAngX float64 = 1.0
	sensorExtraImuAngY float64 = 2.0
	sensorExtraImuAngZ float64 = 3.0
	sensorExtraImuLinX float64 = 4.0
	sensorExtraImuLinY float64 = 5.0
	sensorExtraImuLinZ float64 = 6.0

	sensorExtraScanAngleMin   float32 = -1.0
	sensorExtraScanAngleMax   float32 = 1.0
	sensorExtraScanAngleIncr  float32 = 0.5
	sensorExtraScanTimeIncr   float32 = 0.0
	sensorExtraScanTime       float32 = 0.5
	sensorExtraScanRangeMin   float32 = 0.5
	sensorExtraScanRangeMax   float32 = 10.0
	sensorExtraScanRange0     float32 = 1.0
	sensorExtraScanRange1     float32 = 2.0
	sensorExtraScanIntensity0 float32 = 10.0
	sensorExtraScanIntensity1 float32 = 20.0

	sensorExtraFixStatus  int8    = 0
	sensorExtraFixService uint16  = 1
	sensorExtraFixLat     float64 = 37.5
	sensorExtraFixLon     float64 = -122.5
	sensorExtraFixAlt     float64 = 10.5
	sensorExtraFixCovType uint8   = 0
)

// yamlSensorExtraImu returns ROS YAML with live frame_id.
func yamlSensorExtraImu(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}, orientation_covariance: [0, 0, 0, 0, 0, 0, 0, 0, 0], angular_velocity: {x: 1.0, y: 2.0, z: 3.0}, angular_velocity_covariance: [0, 0, 0, 0, 0, 0, 0, 0, 0], linear_acceleration: {x: 4.0, y: 5.0, z: 6.0}, linear_acceleration_covariance: [0, 0, 0, 0, 0, 0, 0, 0, 0]}", live)
}

// encodeSensorExtraZeroCov9 writes nine zero float64 covariance entries.
func encodeSensorExtraZeroCov9(enc *types.Encoder) {
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
	enc.WriteFloat64(0.0)
}

// decodeSensorExtraZeroCov9 reads nine float64 entries and requires zero.
func decodeSensorExtraZeroCov9(dec *types.Decoder, label string) error {
	for i := range 9 {
		got, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode %s cov %d: %w", label, i, err)
		}
		if got != 0.0 {
			return fmt.Errorf("%s cov %d %v wants 0.0", label, i, got)
		}
	}
	return nil
}

// encodeSensorExtraImu encodes Imu with live frame_id as XCDR1.
func encodeSensorExtraImu(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode imu header: %w", err)
	}
	enc.WriteFloat64(sensorExtraImuOriX)
	enc.WriteFloat64(sensorExtraImuOriY)
	enc.WriteFloat64(sensorExtraImuOriZ)
	enc.WriteFloat64(sensorExtraImuOriW)
	encodeSensorExtraZeroCov9(enc)
	enc.WriteFloat64(sensorExtraImuAngX)
	enc.WriteFloat64(sensorExtraImuAngY)
	enc.WriteFloat64(sensorExtraImuAngZ)
	encodeSensorExtraZeroCov9(enc)
	enc.WriteFloat64(sensorExtraImuLinX)
	enc.WriteFloat64(sensorExtraImuLinY)
	enc.WriteFloat64(sensorExtraImuLinZ)
	encodeSensorExtraZeroCov9(enc)
	return enc.Bytes(), nil
}

// decodeImuOrientation checks orientation plus covariance.
func decodeImuOrientation(dec *types.Decoder) error {
	ox, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode imu orientation x: %w", err)
	}
	oy, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode imu orientation y: %w", err)
	}
	oz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode imu orientation z: %w", err)
	}
	ow, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode imu orientation w: %w", err)
	}
	if ox != sensorExtraImuOriX || oy != sensorExtraImuOriY || oz != sensorExtraImuOriZ || ow != sensorExtraImuOriW {
		return fmt.Errorf("imu orientation (%v, %v, %v, %v) wants (0.0, 0.0, 0.0, 1.0)", ox, oy, oz, ow)
	}
	err = decodeSensorExtraZeroCov9(dec, "imu orientation")
	if err != nil {
		return err
	}
	return nil
}

// checkImuTriple reads three floats plus covariance with checks.
func checkImuTriple(dec *types.Decoder, label string, wantX, wantY, wantZ float64, covLabel, wantText string) error {
	x, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode imu %s x: %w", label, err)
	}
	y, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode imu %s y: %w", label, err)
	}
	z, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode imu %s z: %w", label, err)
	}
	if x != wantX || y != wantY || z != wantZ {
		return fmt.Errorf("imu %s (%v, %v, %v) wants %s", label, x, y, z, wantText)
	}
	err = decodeSensorExtraZeroCov9(dec, covLabel)
	if err != nil {
		return err
	}
	return nil
}

// decodeImuAngular checks angular plus covariance.
func decodeImuAngular(dec *types.Decoder) error {
	return checkImuTriple(dec, "angular", sensorExtraImuAngX, sensorExtraImuAngY, sensorExtraImuAngZ, "imu angular", "(1.0, 2.0, 3.0)")
}

// decodeImuLinear checks linear plus covariance.
func decodeImuLinear(dec *types.Decoder) error {
	return checkImuTriple(dec, "linear", sensorExtraImuLinX, sensorExtraImuLinY, sensorExtraImuLinZ, "imu linear", "(4.0, 5.0, 6.0)")
}

// decodeSensorExtraImu decodes and checks live frame_id plus sentinels.
func decodeSensorExtraImu(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode imu header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode imu frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("imu frame %q wants %q", frame, live)
	}
	err = decodeImuOrientation(dec)
	if err != nil {
		return err
	}
	err = decodeImuAngular(dec)
	if err != nil {
		return err
	}
	err = decodeImuLinear(dec)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish imu: %w", err)
	}
	return nil
}

// wantSensorExtraImu returns the live ID expected in echo output.
func wantSensorExtraImu(live string) string {
	return live
}

// yamlSensorExtraLaserScan returns ROS YAML with live frame_id.
func yamlSensorExtraLaserScan(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, angle_min: -1.0, angle_max: 1.0, angle_increment: 0.5, time_increment: 0.0, scan_time: 0.5, range_min: 0.5, range_max: 10.0, ranges: [1.0, 2.0], intensities: [10.0, 20.0]}", live)
}

// encodeSensorExtraLaserScan encodes small LaserScan with live frame_id.
func encodeSensorExtraLaserScan(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode scan header: %w", err)
	}
	enc.WriteFloat32(sensorExtraScanAngleMin)
	enc.WriteFloat32(sensorExtraScanAngleMax)
	enc.WriteFloat32(sensorExtraScanAngleIncr)
	enc.WriteFloat32(sensorExtraScanTimeIncr)
	enc.WriteFloat32(sensorExtraScanTime)
	enc.WriteFloat32(sensorExtraScanRangeMin)
	enc.WriteFloat32(sensorExtraScanRangeMax)
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode scan ranges len: %w", err)
	}
	enc.WriteFloat32(sensorExtraScanRange0)
	enc.WriteFloat32(sensorExtraScanRange1)
	err = enc.WriteSequenceLength(2)
	if err != nil {
		return nil, fmt.Errorf("encode scan intensities len: %w", err)
	}
	enc.WriteFloat32(sensorExtraScanIntensity0)
	enc.WriteFloat32(sensorExtraScanIntensity1)
	return enc.Bytes(), nil
}

// decodeScanAnglesOnly checks angle min plus max plus increment.
func decodeScanAnglesOnly(dec *types.Decoder) error {
	angleMin, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan angle_min: %w", err)
	}
	angleMax, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan angle_max: %w", err)
	}
	angleIncr, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan angle_increment: %w", err)
	}
	if angleMin != sensorExtraScanAngleMin || angleMax != sensorExtraScanAngleMax || angleIncr != sensorExtraScanAngleIncr {
		return fmt.Errorf("scan angles (%v, %v, %v) wants (-1.0, 1.0, 0.5)", angleMin, angleMax, angleIncr)
	}
	return nil
}

// decodeScanTimingRange checks timing plus range bounds.
func decodeScanTimingRange(dec *types.Decoder) error {
	timeIncr, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan time_increment: %w", err)
	}
	scanTime, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan scan_time: %w", err)
	}
	rangeMin, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan range_min: %w", err)
	}
	rangeMax, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan range_max: %w", err)
	}
	if timeIncr != sensorExtraScanTimeIncr || scanTime != sensorExtraScanTime {
		return fmt.Errorf("scan timing (%v, %v) wants (0.0, 0.5)", timeIncr, scanTime)
	}
	if rangeMin != sensorExtraScanRangeMin || rangeMax != sensorExtraScanRangeMax {
		return fmt.Errorf("scan range bounds (%v, %v) wants (0.5, 10.0)", rangeMin, rangeMax)
	}
	return nil
}

// decodeScanAngles checks angle plus timing plus range bounds.
func decodeScanAngles(dec *types.Decoder) error {
	err := decodeScanAnglesOnly(dec)
	if err != nil {
		return err
	}
	err = decodeScanTimingRange(dec)
	if err != nil {
		return err
	}
	return nil
}

// decodeScanRanges checks range sequence values.
func decodeScanRanges(dec *types.Decoder) error {
	nRanges, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode scan ranges len: %w", err)
	}
	if nRanges != 2 {
		return fmt.Errorf("scan ranges len %d wants 2", nRanges)
	}
	range0, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan range 0: %w", err)
	}
	range1, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan range 1: %w", err)
	}
	if range0 != sensorExtraScanRange0 || range1 != sensorExtraScanRange1 {
		return fmt.Errorf("scan ranges (%v, %v) wants (1.0, 2.0)", range0, range1)
	}
	return nil
}

// decodeScanIntensities checks intensity sequence values plus finish.
func decodeScanIntensities(dec *types.Decoder) error {
	nIntens, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode scan intensities len: %w", err)
	}
	if nIntens != 2 {
		return fmt.Errorf("scan intensities len %d wants 2", nIntens)
	}
	intens0, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan intensity 0: %w", err)
	}
	intens1, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode scan intensity 1: %w", err)
	}
	if intens0 != sensorExtraScanIntensity0 || intens1 != sensorExtraScanIntensity1 {
		return fmt.Errorf("scan intensities (%v, %v) wants (10.0, 20.0)", intens0, intens1)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish scan: %w", err)
	}
	return nil
}

// newScanFixDecoder creates a decoder plus frame check for scan and fix.
func newScanFixDecoder(payload []byte, live, label string) (*types.Decoder, error) {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return nil, fmt.Errorf("decode %s header: %w", label, err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return nil, fmt.Errorf("decode %s frame: %w", label, err)
	}
	if frame != live {
		return nil, fmt.Errorf("%s frame %q wants %q", label, frame, live)
	}
	return dec, nil
}

// decodeSensorExtraLaserScan decodes and checks live frame_id plus sentinels.
func decodeSensorExtraLaserScan(payload []byte, live string) error {
	dec, err := newScanFixDecoder(payload, live, "scan")
	if err != nil {
		return err
	}
	err = decodeScanAngles(dec)
	if err != nil {
		return err
	}
	err = decodeScanRanges(dec)
	if err != nil {
		return err
	}
	err = decodeScanIntensities(dec)
	if err != nil {
		return err
	}
	return nil
}

// wantSensorExtraLaserScan returns the live ID expected in echo output.
func wantSensorExtraLaserScan(live string) string {
	return live
}

// yamlSensorExtraNavSatFix returns ROS YAML with live frame_id.
func yamlSensorExtraNavSatFix(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, status: {status: 0, service: 1}, latitude: 37.5, longitude: -122.5, altitude: 10.5, position_covariance: [0, 0, 0, 0, 0, 0, 0, 0, 0], position_covariance_type: 0}", live)
}

// encodeSensorExtraNavSatFix encodes NavSatFix with live frame_id.
func encodeSensorExtraNavSatFix(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode fix header: %w", err)
	}
	enc.WriteInt8(sensorExtraFixStatus)
	enc.WriteUint16(sensorExtraFixService)
	enc.WriteFloat64(sensorExtraFixLat)
	enc.WriteFloat64(sensorExtraFixLon)
	enc.WriteFloat64(sensorExtraFixAlt)
	encodeSensorExtraZeroCov9(enc)
	enc.WriteUint8(sensorExtraFixCovType)
	return enc.Bytes(), nil
}

// decodeFixStatus checks status plus service.
func decodeFixStatus(dec *types.Decoder) error {
	status, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("decode fix status: %w", err)
	}
	service, err := dec.ReadUint16()
	if err != nil {
		return fmt.Errorf("decode fix service: %w", err)
	}
	if status != sensorExtraFixStatus || service != sensorExtraFixService {
		return fmt.Errorf("fix status (%v, %v) wants (0, 1)", status, service)
	}
	return nil
}

// decodeFixCoords checks latitude plus longitude plus altitude.
func decodeFixCoords(dec *types.Decoder) error {
	lat, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode fix latitude: %w", err)
	}
	lon, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode fix longitude: %w", err)
	}
	alt, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode fix altitude: %w", err)
	}
	if lat != sensorExtraFixLat || lon != sensorExtraFixLon || alt != sensorExtraFixAlt {
		return fmt.Errorf("fix position (%v, %v, %v) wants (37.5, -122.5, 10.5)", lat, lon, alt)
	}
	return nil
}

// decodeFixCov checks covariance plus type plus pad.
func decodeFixCov(dec *types.Decoder) error {
	err := decodeSensorExtraZeroCov9(dec, "fix position")
	if err != nil {
		return err
	}
	covType, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode fix cov type: %w", err)
	}
	if covType != sensorExtraFixCovType {
		return fmt.Errorf("fix cov type %d wants 0", covType)
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("decode fix pad: %w", err)
		}
		if pad != 0 {
			return fmt.Errorf("fix pad 0x%02x wants zero", pad)
		}
	}
	return nil
}

// decodeSensorExtraNavSatFix decodes and checks live frame_id plus sentinels.
func decodeSensorExtraNavSatFix(payload []byte, live string) error {
	dec, err := newScanFixDecoder(payload, live, "fix")
	if err != nil {
		return err
	}
	err = decodeFixStatus(dec)
	if err != nil {
		return err
	}
	err = decodeFixCoords(dec)
	if err != nil {
		return err
	}
	err = decodeFixCov(dec)
	if err != nil {
		return err
	}
	return nil
}

// wantSensorExtraNavSatFix returns the live ID expected in echo output.
func wantSensorExtraNavSatFix(live string) string {
	return live
}

// sensorExtraCases returns the extra sensor live matrix in fixed order.
func sensorExtraCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "sensor-imu",
			RosType:    "sensor_msgs/msg/Imu",
			IDLName:    "sensor_msgs__msg__Imu",
			staticHash: sensorExtraHashImu,
			yaml:       yamlSensorExtraImu,
			encode:     encodeSensorExtraImu,
			decode:     decodeSensorExtraImu,
			want:       wantSensorExtraImu,
		},
		{
			Label:      "sensor-laserscan",
			RosType:    "sensor_msgs/msg/LaserScan",
			IDLName:    "sensor_msgs__msg__LaserScan",
			staticHash: sensorExtraHashLaserScan,
			yaml:       yamlSensorExtraLaserScan,
			encode:     encodeSensorExtraLaserScan,
			decode:     decodeSensorExtraLaserScan,
			want:       wantSensorExtraLaserScan,
		},
		{
			Label:      "sensor-navsatfix",
			RosType:    "sensor_msgs/msg/NavSatFix",
			IDLName:    "sensor_msgs__msg__NavSatFix",
			staticHash: sensorExtraHashNavSatFix,
			yaml:       yamlSensorExtraNavSatFix,
			encode:     encodeSensorExtraNavSatFix,
			decode:     decodeSensorExtraNavSatFix,
			want:       wantSensorExtraNavSatFix,
		},
	}
}

// runSensorExtraLabel runs forward plus reverse for one extra sensor label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. Forward and reverse reuse the shared matrix runners
// so domain 42 plus docker argv plus hash probing stay identical.
func runSensorExtraLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, sensorExtraCases())
}

// TestROSTypeSensorImu checks Imu forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
// Orientation stays identity, covariances stay zero for unknown, and
// angular plus linear sentinels stay fixed.
func TestROSTypeSensorImu(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorExtraLabel(t, ctx, domain, "sensor-imu")
}

// TestROSTypeSensorLaserScan checks LaserScan small forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
// The scan stays small with two ranges plus two intensities and fixed
// angle plus timing sentinels.
func TestROSTypeSensorLaserScan(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorExtraLabel(t, ctx, domain, "sensor-laserscan")
}

// TestROSTypeSensorNavSatFix checks NavSatFix forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
// Status stays fix with GPS service, position stays fixed, covariance stays
// zero, and covariance type stays unknown.
func TestROSTypeSensorNavSatFix(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runSensorExtraLabel(t, ctx, domain, "sensor-navsatfix")
}
