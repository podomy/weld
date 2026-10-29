// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop // test breadth pins many wire branches in one decoder
package ros_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Rest geometry static type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each ROS type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime, these values
// are the fallback when the probe cannot run. Values were captured from
// weld-jazzy Jazzy. PointStamped plus Vector3 plus Pose plus Quaternion plus
// Twist are already covered by the base matrix and the extra geometry gate,
// so this file covers the remaining geometry_msgs types in corpus order.
const (
	restHashPoint                      = "RIHS01_6963084842a9b04494d6b2941d11444708d892da2f4b09843b9c43f42a7f6881"
	restHashPoint32                    = "RIHS01_2fc4db7cae16a4582c79a56b66173a8d48d52c7dc520ddc55a0d4bcf2a4bfdbc"
	restHashAccel                      = "RIHS01_dc448243ded9b1fcbcca24aba0c22f013dae06c354ba2d849571c0a2a3f57ca0"
	restHashInertia                    = "RIHS01_2ddd5dab5c347825ba2e56c895ddccfd0b8efe53ae931bf67f905529930b4bd7"
	restHashPolygon                    = "RIHS01_3782f9f0bf044964d692d6c017d705e37611afb1f0bf6a9dee248a7dda0f784a"
	restHashTransform                  = "RIHS01_beb83fbe698636351461f6f35d1abb20010c43d55374d81bd041f1ba2581fddc"
	restHashWrench                     = "RIHS01_018e8519d57c16adbe97c9fe1460ef21fec7e31bc541de3d653a35895677ce52"
	restHashAccelWithCovariance        = "RIHS01_230d51bd53bc36f260574e73b42941cefe44684753480b6fc330c032c5db5997"
	restHashPoseWithCovariance         = "RIHS01_9a7c0fd234b7f45c6098745ecccd773ca1085670e64107135397aee31c02e1bb"
	restHashTwistWithCovariance        = "RIHS01_49f574f033f095d8b6cd1beaca5ca7925e296e84af1716d16c89d38b059c8c18"
	restHashAccelStamped               = "RIHS01_ef1df9eabae0a708cc049a061ebcddc4e2a5f745730100ba680e086a9698b165"
	restHashAccelWithCovarianceStamped = "RIHS01_61c9ad8928e71dd95ce791b2f02809ee2a0bbcc42cd0e4047fd00a822a08e444"
	restHashInertiaStamped             = "RIHS01_766be45976252babf7f9d8ac4ae7c912a7ceccf71035622529f27518b695aa09"
	restHashPolygonStamped             = "RIHS01_b7cf07932f1523d4b4088075945c1a0141f7cd21da87cc940fc61652e9138b46"
	restHashPoseArray                  = "RIHS01_af0cc36d190e104d546d168d6b39df04fa4b4ccecf59cb4c9ed328d3d5004aa0"
	restHashPoseStamped                = "RIHS01_10f3786d7d40fd2b54367835614bff85d4ad3b5dab62bf8bca0cc232d73b4cd8"
	restHashPoseWithCovarianceStamped  = "RIHS01_26432f9803e43727d3c8f668d1fdb3c630f548af631e2f4e31382371bfea3b6e"
	restHashQuaternionStamped          = "RIHS01_381add86c6c3160644d228ca342182c7fd6c7fab11c7a85ad817a9cc22dbac6e"
	restHashTransformStamped           = "RIHS01_0a241f87d04668d94099cbb5ba11691d5ad32c2f29682e4eb5653424bd275206"
	restHashTwistStamped               = "RIHS01_5f0fcd4f81d5d06ad9b4c4c63e3ea51b82d6ae4d0558f1d475229b1121db6f64"
	restHashTwistWithCovarianceStamped = "RIHS01_77b67434531e6529b7a0091357b186b6ebdb17fd9ffd3e0c7ce9d3fb11a44563"
	restHashVector3Stamped             = "RIHS01_d4829622288cbb443886e7ea94ea5671a3b1be6bab4ad04224432a65f7d7887a"
	restHashWrenchStamped              = "RIHS01_8dc3deaf06b2ab281f9f9a742a8961c328ca7cec16e3fd6586d3a5c83fa78f77"
)

// Rest geometry fixed sentinels.
//
// Types with a Header carry the per-run live ID in frame_id, with child
// headers using a suffix, so stale runs never match. Types without a string
// field use fixed sentinels and rely on per-run topic plus domain isolation.
// Numeric values stay fixed so Weld take and ros2 echo checks are
// deterministic. Float values use exact binary fractions for stability.
const (
	restPosX        float64 = 1.0
	restPosY        float64 = 2.0
	restPosZ        float64 = 3.0
	restOriX        float64 = 0.0
	restOriY        float64 = 0.0
	restOriZ        float64 = 0.0
	restOriW        float64 = 1.0
	restLinX        float64 = 1.0
	restLinY        float64 = 0.0
	restLinZ        float64 = 0.0
	restAngX        float64 = 0.0
	restAngY        float64 = 0.0
	restAngZ        float64 = 0.5
	restForceX      float64 = 1.0
	restForceY      float64 = 0.0
	restForceZ      float64 = 0.0
	restTorqueX     float64 = 0.0
	restTorqueY     float64 = 1.0
	restTorqueZ     float64 = 0.0
	restInertiaM    float64 = 1.0
	restInertiaComX float64 = 0.5
	restInertiaComY float64 = 1.0
	restInertiaComZ float64 = 1.5
	restInertiaIxx  float64 = 1.0
	restInertiaIxy  float64 = 0.0
	restInertiaIxz  float64 = 0.0
	restInertiaIyy  float64 = 2.0
	restInertiaIyz  float64 = 0.0
	restInertiaIzz  float64 = 3.0
	restPoint32Ax   float32 = 1.0
	restPoint32Ay   float32 = 0.0
	restPoint32Az   float32 = 0.0
	restPoint32Bx   float32 = 0.0
	restPoint32By   float32 = 1.0
	restPoint32Bz   float32 = 0.0
	restCovLen              = 36
)

// restCovarianceYAML returns a YAML flow list with 36 zero entries.
func restCovarianceYAML() string {
	parts := make([]string, 0, restCovLen)
	for range restCovLen {
		parts = append(parts, "0")
	}
	return strings.Join(parts, ", ")
}

// encodeRestCovariance writes 36 zero covariance entries.
func encodeRestCovariance(enc *types.Encoder) {
	for range restCovLen {
		enc.WriteFloat64(0.0)
	}
}

// decodeRestCovariance reads 36 entries and checks all zeros.
func decodeRestCovariance(dec *types.Decoder, label string) error {
	for i := range restCovLen {
		got, err := dec.ReadFloat64()
		if err != nil {
			return fmt.Errorf("decode %s covariance %d: %w", label, i, err)
		}
		if got != 0.0 {
			return fmt.Errorf("%s covariance %d %v wants 0.0", label, i, got)
		}
	}
	return nil
}

// encodeRestPosition writes position xyz with fixed sentinels.
func encodeRestPosition(enc *types.Encoder) {
	enc.WriteFloat64(restPosX)
	enc.WriteFloat64(restPosY)
	enc.WriteFloat64(restPosZ)
}

// encodeRestOrientation writes orientation xyzw with fixed sentinels.
func encodeRestOrientation(enc *types.Encoder) {
	enc.WriteFloat64(restOriX)
	enc.WriteFloat64(restOriY)
	enc.WriteFloat64(restOriZ)
	enc.WriteFloat64(restOriW)
}

// encodeRestPose writes Point plus Quaternion in Pose order.
func encodeRestPose(enc *types.Encoder) {
	encodeRestPosition(enc)
	encodeRestOrientation(enc)
}

// decodeRestPose reads Point plus Quaternion and checks sentinels.
func decodeRestPose(dec *types.Decoder, label string) error {
	px, py, pz, err := helpers.ReadFloat64Triple(dec, "decode "+label+" position x", "decode "+label+" position y", "decode "+label+" position z")
	if err != nil {
		return fmt.Errorf("read %s position: %w", label, err)
	}
	if px != restPosX || py != restPosY || pz != restPosZ {
		return fmt.Errorf("%s position (%v, %v, %v) wants (1.0, 2.0, 3.0)", label, px, py, pz)
	}
	ox, oy, oz, ow, err := helpers.ReadFloat64Quad(dec, "decode "+label+" orientation x", "decode "+label+" orientation y", "decode "+label+" orientation z", "decode "+label+" orientation w")
	if err != nil {
		return fmt.Errorf("read %s orientation: %w", label, err)
	}
	if ox != restOriX || oy != restOriY || oz != restOriZ || ow != restOriW {
		return fmt.Errorf("%s orientation (%v, %v, %v, %v) wants (0, 0, 0, 1)", label, ox, oy, oz, ow)
	}
	return nil
}

// encodeRestLinear100 writes linear xyz 1.0 plus 0.0 plus 0.0.
func encodeRestLinear100(enc *types.Encoder) {
	enc.WriteFloat64(restLinX)
	enc.WriteFloat64(restLinY)
	enc.WriteFloat64(restLinZ)
}

// encodeRestAngular005 writes angular xyz 0.0 plus 0.0 plus 0.5.
func encodeRestAngular005(enc *types.Encoder) {
	enc.WriteFloat64(restAngX)
	enc.WriteFloat64(restAngY)
	enc.WriteFloat64(restAngZ)
}

// decodeRestLinear100 reads linear xyz and checks 1.0 plus 0.0 plus 0.0.
func decodeRestLinear100(dec *types.Decoder, label string) error {
	lx, ly, lz, err := helpers.ReadFloat64Triple(dec, "decode "+label+" linear x", "decode "+label+" linear y", "decode "+label+" linear z")
	if err != nil {
		return fmt.Errorf("read %s linear: %w", label, err)
	}
	if lx != restLinX || ly != restLinY || lz != restLinZ {
		return fmt.Errorf("%s linear (%v, %v, %v) wants (1.0, 0.0, 0.0)", label, lx, ly, lz)
	}
	return nil
}

// decodeRestAngular005 reads angular xyz and checks 0.0 plus 0.0 plus 0.5.
func decodeRestAngular005(dec *types.Decoder, label string) error {
	ax, ay, az, err := helpers.ReadFloat64Triple(dec, "decode "+label+" angular x", "decode "+label+" angular y", "decode "+label+" angular z")
	if err != nil {
		return fmt.Errorf("read %s angular: %w", label, err)
	}
	if ax != restAngX || ay != restAngY || az != restAngZ {
		return fmt.Errorf("%s angular (%v, %v, %v) wants (0.0, 0.0, 0.5)", label, ax, ay, az)
	}
	return nil
}

// encodeRestAccelValues writes linear plus angular for Accel.
func encodeRestAccelValues(enc *types.Encoder) {
	encodeRestLinear100(enc)
	encodeRestAngular005(enc)
}

// decodeRestAccelValues reads linear plus angular and checks sentinels.
func decodeRestAccelValues(dec *types.Decoder, label string) error {
	err := decodeRestLinear100(dec, label)
	if err != nil {
		return err
	}
	err = decodeRestAngular005(dec, label)
	if err != nil {
		return err
	}
	return nil
}

// encodeRestForce100 writes force xyz 1.0 plus 0.0 plus 0.0.
func encodeRestForce100(enc *types.Encoder) {
	enc.WriteFloat64(restForceX)
	enc.WriteFloat64(restForceY)
	enc.WriteFloat64(restForceZ)
}

// encodeRestTorque010 writes torque xyz 0.0 plus 1.0 plus 0.0.
func encodeRestTorque010(enc *types.Encoder) {
	enc.WriteFloat64(restTorqueX)
	enc.WriteFloat64(restTorqueY)
	enc.WriteFloat64(restTorqueZ)
}

// decodeRestForce100 reads force xyz and checks sentinels.
func decodeRestForce100(dec *types.Decoder, label string) error {
	fx, fy, fz, err := helpers.ReadFloat64Triple(dec, "decode "+label+" force x", "decode "+label+" force y", "decode "+label+" force z")
	if err != nil {
		return fmt.Errorf("read %s force: %w", label, err)
	}
	if fx != restForceX || fy != restForceY || fz != restForceZ {
		return fmt.Errorf("%s force (%v, %v, %v) wants (1.0, 0.0, 0.0)", label, fx, fy, fz)
	}
	return nil
}

// decodeRestTorque010 reads torque xyz and checks sentinels.
func decodeRestTorque010(dec *types.Decoder, label string) error {
	tx, ty, tz, err := helpers.ReadFloat64Triple(dec, "decode "+label+" torque x", "decode "+label+" torque y", "decode "+label+" torque z")
	if err != nil {
		return fmt.Errorf("read %s torque: %w", label, err)
	}
	if tx != restTorqueX || ty != restTorqueY || tz != restTorqueZ {
		return fmt.Errorf("%s torque (%v, %v, %v) wants (0.0, 1.0, 0.0)", label, tx, ty, tz)
	}
	return nil
}

// encodeRestWrenchValues writes force plus torque for Wrench.
func encodeRestWrenchValues(enc *types.Encoder) {
	encodeRestForce100(enc)
	encodeRestTorque010(enc)
}

// decodeRestWrenchValues reads force plus torque and checks sentinels.
func decodeRestWrenchValues(dec *types.Decoder, label string) error {
	err := decodeRestForce100(dec, label)
	if err != nil {
		return err
	}
	err = decodeRestTorque010(dec, label)
	if err != nil {
		return err
	}
	return nil
}

// encodeRestTransformValues writes translation plus rotation.
func encodeRestTransformValues(enc *types.Encoder) {
	encodeRestPosition(enc)
	encodeRestOrientation(enc)
}

// decodeRestTransformValues reads translation plus rotation and checks sentinels.
func decodeRestTransformValues(dec *types.Decoder, label string) error {
	err := decodeRestPose(dec, label)
	if err != nil {
		return err
	}
	return nil
}

// encodeRestInertiaValues writes m plus com plus six tensor entries.
func encodeRestInertiaValues(enc *types.Encoder) {
	enc.WriteFloat64(restInertiaM)
	enc.WriteFloat64(restInertiaComX)
	enc.WriteFloat64(restInertiaComY)
	enc.WriteFloat64(restInertiaComZ)
	enc.WriteFloat64(restInertiaIxx)
	enc.WriteFloat64(restInertiaIxy)
	enc.WriteFloat64(restInertiaIxz)
	enc.WriteFloat64(restInertiaIyy)
	enc.WriteFloat64(restInertiaIyz)
	enc.WriteFloat64(restInertiaIzz)
}

// decodeRestInertiaValues reads inertia fields and checks sentinels.
func decodeRestInertiaValues(dec *types.Decoder, label string) error {
	m, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode %s m: %w", label, err)
	}
	if m != restInertiaM {
		return fmt.Errorf("%s m %v wants 1.0", label, m)
	}
	cx, cy, cz, err := helpers.ReadFloat64Triple(dec, "decode "+label+" com x", "decode "+label+" com y", "decode "+label+" com z")
	if err != nil {
		return fmt.Errorf("read %s com: %w", label, err)
	}
	if cx != restInertiaComX || cy != restInertiaComY || cz != restInertiaComZ {
		return fmt.Errorf("%s com (%v, %v, %v) wants (0.5, 1.0, 1.5)", label, cx, cy, cz)
	}
	ixx, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode %s ixx: %w", label, err)
	}
	ixy, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode %s ixy: %w", label, err)
	}
	ixz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode %s ixz: %w", label, err)
	}
	iyy, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode %s iyy: %w", label, err)
	}
	iyz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode %s iyz: %w", label, err)
	}
	izz, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("decode %s izz: %w", label, err)
	}
	if ixx != restInertiaIxx || ixy != restInertiaIxy || ixz != restInertiaIxz || iyy != restInertiaIyy || iyz != restInertiaIyz || izz != restInertiaIzz {
		return fmt.Errorf("%s inertia tensor (%v, %v, %v, %v, %v, %v) wants (1.0, 0.0, 0.0, 2.0, 0.0, 3.0)", label, ixx, ixy, ixz, iyy, iyz, izz)
	}
	return nil
}

// encodeRestPolygonPoints writes two Point32 entries with length prefix.
func encodeRestPolygonPoints(enc *types.Encoder) error {
	err := enc.WriteSequenceLength(2)
	if err != nil {
		return fmt.Errorf("encode polygon length: %w", err)
	}
	enc.WriteFloat32(restPoint32Ax)
	enc.WriteFloat32(restPoint32Ay)
	enc.WriteFloat32(restPoint32Az)
	enc.WriteFloat32(restPoint32Bx)
	enc.WriteFloat32(restPoint32By)
	enc.WriteFloat32(restPoint32Bz)
	return nil
}

// decodeRestPolygonPoints reads two Point32 entries and checks sentinels.
func decodeRestPolygonPoints(dec *types.Decoder, label string) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode %s length: %w", label, err)
	}
	if n != 2 {
		return fmt.Errorf("%s length %d wants 2", label, n)
	}
	ax, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode %s ax: %w", label, err)
	}
	ay, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode %s ay: %w", label, err)
	}
	az, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode %s az: %w", label, err)
	}
	bx, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode %s bx: %w", label, err)
	}
	by, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode %s by: %w", label, err)
	}
	bz, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode %s bz: %w", label, err)
	}
	if ax != restPoint32Ax || ay != restPoint32Ay || az != restPoint32Az {
		return fmt.Errorf("%s point 0 (%v, %v, %v) wants (1.0, 0.0, 0.0)", label, ax, ay, az)
	}
	if bx != restPoint32Bx || by != restPoint32By || bz != restPoint32Bz {
		return fmt.Errorf("%s point 1 (%v, %v, %v) wants (0.0, 1.0, 0.0)", label, bx, by, bz)
	}
	return nil
}

// newRestDecoder creates a decoder and checks the header frame.
func newRestDecoder(payload []byte, live, label string) (*types.Decoder, error) {
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

// yamlRestPoint returns ROS YAML for the Point sentinel.
func yamlRestPoint(live string) string {
	return "{x: 1.0, y: 2.0, z: 3.0}"
}

// encodeRestPoint encodes the fixed Point sentinel as XCDR1.
func encodeRestPoint(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	encodeRestPosition(enc)
	return enc.Bytes(), nil
}

// decodeRestPoint decodes and checks the Point sentinel.
func decodeRestPoint(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode point header: %w", err)
	}
	px, py, pz, err := helpers.ReadFloat64Triple(dec, "decode point x", "decode point y", "decode point z")
	if err != nil {
		return fmt.Errorf("read point: %w", err)
	}
	if px != restPosX || py != restPosY || pz != restPosZ {
		return fmt.Errorf("point (%v, %v, %v) wants (1.0, 2.0, 3.0)", px, py, pz)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish point: %w", err)
	}
	return nil
}

// wantRestPoint returns the echo substring for the Point sentinel.
func wantRestPoint(live string) string {
	return "z: 3.0"
}

// yamlRestPoint32 returns ROS YAML for the Point32 sentinel.
func yamlRestPoint32(live string) string {
	return "{x: 1.0, y: 2.0, z: 3.0}"
}

// encodeRestPoint32 encodes the fixed Point32 sentinel as XCDR1.
func encodeRestPoint32(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteFloat32(1.0)
	enc.WriteFloat32(2.0)
	enc.WriteFloat32(3.0)
	return enc.Bytes(), nil
}

// decodeRestPoint32 decodes and checks the Point32 sentinel.
func decodeRestPoint32(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode point32 header: %w", err)
	}
	x, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode point32 x: %w", err)
	}
	y, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode point32 y: %w", err)
	}
	z, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode point32 z: %w", err)
	}
	if x != 1.0 || y != 2.0 || z != 3.0 {
		return fmt.Errorf("point32 (%v, %v, %v) wants (1.0, 2.0, 3.0)", x, y, z)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish point32: %w", err)
	}
	return nil
}

// wantRestPoint32 returns the echo substring for the Point32 sentinel.
func wantRestPoint32(live string) string {
	return "z: 3.0"
}

// yamlRestAccel returns ROS YAML for the Accel sentinel.
func yamlRestAccel(live string) string {
	return "{linear: {x: 1.0, y: 0.0, z: 0.0}, angular: {x: 0.0, y: 0.0, z: 0.5}}"
}

// encodeRestAccel encodes the fixed Accel sentinel as XCDR1.
func encodeRestAccel(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	encodeRestAccelValues(enc)
	return enc.Bytes(), nil
}

// decodeRestAccel decodes and checks the Accel sentinel.
func decodeRestAccel(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode accel header: %w", err)
	}
	err = decodeRestAccelValues(dec, "accel")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish accel: %w", err)
	}
	return nil
}

// wantRestAccel returns the echo substring for the Accel sentinel.
func wantRestAccel(live string) string {
	return "z: 0.5"
}

// yamlRestInertia returns ROS YAML for the Inertia sentinel.
func yamlRestInertia(live string) string {
	return "{m: 1.0, com: {x: 0.5, y: 1.0, z: 1.5}, ixx: 1.0, ixy: 0.0, ixz: 0.0, iyy: 2.0, iyz: 0.0, izz: 3.0}"
}

// encodeRestInertia encodes the fixed Inertia sentinel as XCDR1.
func encodeRestInertia(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	encodeRestInertiaValues(enc)
	return enc.Bytes(), nil
}

// decodeRestInertia decodes and checks the Inertia sentinel.
func decodeRestInertia(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode inertia header: %w", err)
	}
	err = decodeRestInertiaValues(dec, "inertia")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish inertia: %w", err)
	}
	return nil
}

// wantRestInertia returns the echo substring for the Inertia sentinel.
func wantRestInertia(live string) string {
	return "izz: 3.0"
}

// yamlRestPolygon returns ROS YAML for the Polygon sentinel.
func yamlRestPolygon(live string) string {
	return "{points: [{x: 1.0, y: 0.0, z: 0.0}, {x: 0.0, y: 1.0, z: 0.0}]}"
}

// encodeRestPolygon encodes the fixed Polygon sentinel as XCDR1.
func encodeRestPolygon(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeRestPolygonPoints(enc)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeRestPolygon decodes and checks the Polygon sentinel.
func decodeRestPolygon(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode polygon header: %w", err)
	}
	err = decodeRestPolygonPoints(dec, "polygon")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish polygon: %w", err)
	}
	return nil
}

// wantRestPolygon returns the echo substring for the Polygon sentinel.
func wantRestPolygon(live string) string {
	return "y: 1.0"
}

// yamlRestTransform returns ROS YAML for the Transform sentinel.
func yamlRestTransform(live string) string {
	return "{translation: {x: 1.0, y: 2.0, z: 3.0}, rotation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}"
}

// encodeRestTransform encodes the fixed Transform sentinel as XCDR1.
func encodeRestTransform(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	encodeRestTransformValues(enc)
	return enc.Bytes(), nil
}

// decodeRestTransform decodes and checks the Transform sentinel.
func decodeRestTransform(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode transform header: %w", err)
	}
	err = decodeRestTransformValues(dec, "transform")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish transform: %w", err)
	}
	return nil
}

// wantRestTransform returns the echo substring for the Transform sentinel.
func wantRestTransform(live string) string {
	return "w: 1.0"
}

// yamlRestWrench returns ROS YAML for the Wrench sentinel.
func yamlRestWrench(live string) string {
	return "{force: {x: 1.0, y: 0.0, z: 0.0}, torque: {x: 0.0, y: 1.0, z: 0.0}}"
}

// encodeRestWrench encodes the fixed Wrench sentinel as XCDR1.
func encodeRestWrench(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	encodeRestWrenchValues(enc)
	return enc.Bytes(), nil
}

// decodeRestWrench decodes and checks the Wrench sentinel.
func decodeRestWrench(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode wrench header: %w", err)
	}
	err = decodeRestWrenchValues(dec, "wrench")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish wrench: %w", err)
	}
	return nil
}

// wantRestWrench returns the echo substring for the Wrench sentinel.
func wantRestWrench(live string) string {
	return "y: 1.0"
}

// yamlRestAccelWithCovariance returns ROS YAML for AccelWithCovariance.
func yamlRestAccelWithCovariance(live string) string {
	return fmt.Sprintf("{accel: {linear: {x: 1.0, y: 0.0, z: 0.0}, angular: {x: 0.0, y: 0.0, z: 0.5}}, covariance: [%s]}", restCovarianceYAML())
}

// encodeRestAccelWithCovariance encodes AccelWithCovariance as XCDR1.
func encodeRestAccelWithCovariance(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	encodeRestAccelValues(enc)
	encodeRestCovariance(enc)
	return enc.Bytes(), nil
}

// decodeRestAccelWithCovariance decodes and checks the sentinel.
func decodeRestAccelWithCovariance(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode accelcov header: %w", err)
	}
	err = decodeRestAccelValues(dec, "accelcov")
	if err != nil {
		return err
	}
	err = decodeRestCovariance(dec, "accelcov")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish accelcov: %w", err)
	}
	return nil
}

// wantRestAccelWithCovariance returns the echo substring.
func wantRestAccelWithCovariance(live string) string {
	return "z: 0.5"
}

// yamlRestPoseWithCovariance returns ROS YAML for PoseWithCovariance.
func yamlRestPoseWithCovariance(live string) string {
	return fmt.Sprintf("{pose: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}, covariance: [%s]}", restCovarianceYAML())
}

// encodeRestPoseWithCovariance encodes PoseWithCovariance as XCDR1.
func encodeRestPoseWithCovariance(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	encodeRestPose(enc)
	encodeRestCovariance(enc)
	return enc.Bytes(), nil
}

// decodeRestPoseWithCovariance decodes and checks the sentinel.
func decodeRestPoseWithCovariance(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode posecov header: %w", err)
	}
	err = decodeRestPose(dec, "posecov")
	if err != nil {
		return err
	}
	err = decodeRestCovariance(dec, "posecov")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish posecov: %w", err)
	}
	return nil
}

// wantRestPoseWithCovariance returns the echo substring.
func wantRestPoseWithCovariance(live string) string {
	return "z: 3.0"
}

// yamlRestTwistWithCovariance returns ROS YAML for TwistWithCovariance.
func yamlRestTwistWithCovariance(live string) string {
	return fmt.Sprintf("{twist: {linear: {x: 1.0, y: 0.0, z: 0.0}, angular: {x: 0.0, y: 0.0, z: 0.5}}, covariance: [%s]}", restCovarianceYAML())
}

// encodeRestTwistWithCovariance encodes TwistWithCovariance as XCDR1.
func encodeRestTwistWithCovariance(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	encodeRestAccelValues(enc)
	encodeRestCovariance(enc)
	return enc.Bytes(), nil
}

// decodeRestTwistWithCovariance decodes and checks the sentinel.
func decodeRestTwistWithCovariance(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode twistcov header: %w", err)
	}
	err = decodeRestAccelValues(dec, "twistcov")
	if err != nil {
		return err
	}
	err = decodeRestCovariance(dec, "twistcov")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish twistcov: %w", err)
	}
	return nil
}

// wantRestTwistWithCovariance returns the echo substring.
func wantRestTwistWithCovariance(live string) string {
	return "z: 0.5"
}

// yamlRestAccelStamped returns ROS YAML with live frame_id.
func yamlRestAccelStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, accel: {linear: {x: 1.0, y: 0.0, z: 0.0}, angular: {x: 0.0, y: 0.0, z: 0.5}}}", live)
}

// encodeRestAccelStamped encodes AccelStamped with live frame_id.
func encodeRestAccelStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode accelstamped header: %w", err)
	}
	encodeRestAccelValues(enc)
	return enc.Bytes(), nil
}

// decodeRestAccelStamped decodes and checks live frame_id plus sentinels.
func decodeRestAccelStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "accelstamped")
	if err != nil {
		return err
	}
	err = decodeRestAccelValues(dec, "accelstamped")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish accelstamped: %w", err)
	}
	return nil
}

// wantRestAccelStamped returns the live ID expected in echo output.
func wantRestAccelStamped(live string) string {
	return live
}

// yamlRestAccelWithCovarianceStamped returns ROS YAML with live frame_id.
func yamlRestAccelWithCovarianceStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, accel: {accel: {linear: {x: 1.0, y: 0.0, z: 0.0}, angular: {x: 0.0, y: 0.0, z: 0.5}}, covariance: [%s]}}", live, restCovarianceYAML())
}

// encodeRestAccelWithCovarianceStamped encodes the stamped type as XCDR1.
func encodeRestAccelWithCovarianceStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode accelcovstamped header: %w", err)
	}
	encodeRestAccelValues(enc)
	encodeRestCovariance(enc)
	return enc.Bytes(), nil
}

// decodeRestAccelWithCovarianceStamped decodes and checks the sentinel.
func decodeRestAccelWithCovarianceStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "accelcovstamped")
	if err != nil {
		return err
	}
	err = decodeRestAccelValues(dec, "accelcovstamped")
	if err != nil {
		return err
	}
	err = decodeRestCovariance(dec, "accelcovstamped")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish accelcovstamped: %w", err)
	}
	return nil
}

// wantRestAccelWithCovarianceStamped returns the live ID in echo output.
func wantRestAccelWithCovarianceStamped(live string) string {
	return live
}

// yamlRestInertiaStamped returns ROS YAML with live frame_id.
func yamlRestInertiaStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, inertia: {m: 1.0, com: {x: 0.5, y: 1.0, z: 1.5}, ixx: 1.0, ixy: 0.0, ixz: 0.0, iyy: 2.0, iyz: 0.0, izz: 3.0}}", live)
}

// encodeRestInertiaStamped encodes InertiaStamped with live frame_id.
func encodeRestInertiaStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode inertiastamped header: %w", err)
	}
	encodeRestInertiaValues(enc)
	return enc.Bytes(), nil
}

// decodeRestInertiaStamped decodes and checks live frame_id plus sentinels.
func decodeRestInertiaStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "inertiastamped")
	if err != nil {
		return err
	}
	err = decodeRestInertiaValues(dec, "inertiastamped")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish inertiastamped: %w", err)
	}
	return nil
}

// wantRestInertiaStamped returns the live ID expected in echo output.
func wantRestInertiaStamped(live string) string {
	return live
}

// yamlRestPolygonStamped returns ROS YAML with live frame_id.
func yamlRestPolygonStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, polygon: {points: [{x: 1.0, y: 0.0, z: 0.0}, {x: 0.0, y: 1.0, z: 0.0}]}}", live)
}

// encodeRestPolygonStamped encodes PolygonStamped with live frame_id.
func encodeRestPolygonStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode polygonstamped header: %w", err)
	}
	err = encodeRestPolygonPoints(enc)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeRestPolygonStamped decodes and checks live frame_id plus points.
func decodeRestPolygonStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "polygonstamped")
	if err != nil {
		return err
	}
	err = decodeRestPolygonPoints(dec, "polygonstamped")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish polygonstamped: %w", err)
	}
	return nil
}

// wantRestPolygonStamped returns the live ID expected in echo output.
func wantRestPolygonStamped(live string) string {
	return live
}

// yamlRestPoseArray returns ROS YAML with live frame_id.
func yamlRestPoseArray(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, poses: [{position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}]}", live)
}

// encodeRestPoseArray encodes PoseArray with one Pose as XCDR1.
func encodeRestPoseArray(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode posearray header: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode posearray length: %w", err)
	}
	encodeRestPose(enc)
	return enc.Bytes(), nil
}

// decodeRestPoseArray decodes and checks live frame_id plus one pose.
func decodeRestPoseArray(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "posearray")
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode posearray length: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("posearray length %d wants 1", n)
	}
	err = decodeRestPose(dec, "posearray")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish posearray: %w", err)
	}
	return nil
}

// wantRestPoseArray returns the live ID expected in echo output.
func wantRestPoseArray(live string) string {
	return live
}

// yamlRestPoseStamped returns ROS YAML with live frame_id.
func yamlRestPoseStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, pose: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}}", live)
}

// encodeRestPoseStamped encodes PoseStamped with live frame_id.
func encodeRestPoseStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode posestamped header: %w", err)
	}
	encodeRestPose(enc)
	return enc.Bytes(), nil
}

// decodeRestPoseStamped decodes and checks live frame_id plus pose.
func decodeRestPoseStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "posestamped")
	if err != nil {
		return err
	}
	err = decodeRestPose(dec, "posestamped")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish posestamped: %w", err)
	}
	return nil
}

// wantRestPoseStamped returns the live ID expected in echo output.
func wantRestPoseStamped(live string) string {
	return live
}

// yamlRestPoseWithCovarianceStamped returns ROS YAML with live frame_id.
func yamlRestPoseWithCovarianceStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, pose: {pose: {position: {x: 1.0, y: 2.0, z: 3.0}, orientation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}, covariance: [%s]}}", live, restCovarianceYAML())
}

// encodeRestPoseWithCovarianceStamped encodes the stamped type as XCDR1.
func encodeRestPoseWithCovarianceStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode posecovstamped header: %w", err)
	}
	encodeRestPose(enc)
	encodeRestCovariance(enc)
	return enc.Bytes(), nil
}

// decodeRestPoseWithCovarianceStamped decodes and checks the sentinel.
func decodeRestPoseWithCovarianceStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "posecovstamped")
	if err != nil {
		return err
	}
	err = decodeRestPose(dec, "posecovstamped")
	if err != nil {
		return err
	}
	err = decodeRestCovariance(dec, "posecovstamped")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish posecovstamped: %w", err)
	}
	return nil
}

// wantRestPoseWithCovarianceStamped returns the live ID in echo output.
func wantRestPoseWithCovarianceStamped(live string) string {
	return live
}

// yamlRestQuaternionStamped returns ROS YAML with live frame_id.
func yamlRestQuaternionStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, quaternion: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}", live)
}

// encodeRestQuaternionStamped encodes QuaternionStamped with live frame_id.
func encodeRestQuaternionStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode quatstamped header: %w", err)
	}
	encodeRestOrientation(enc)
	return enc.Bytes(), nil
}

// decodeRestQuaternionStamped decodes and checks live frame_id plus quat.
func decodeRestQuaternionStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "quatstamped")
	if err != nil {
		return err
	}
	ox, oy, oz, ow, err := helpers.ReadFloat64Quad(dec, "decode quatstamped x", "decode quatstamped y", "decode quatstamped z", "decode quatstamped w")
	if err != nil {
		return fmt.Errorf("read quatstamped: %w", err)
	}
	if ox != restOriX || oy != restOriY || oz != restOriZ || ow != restOriW {
		return fmt.Errorf("quatstamped (%v, %v, %v, %v) wants (0, 0, 0, 1)", ox, oy, oz, ow)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish quatstamped: %w", err)
	}
	return nil
}

// wantRestQuaternionStamped returns the live ID expected in echo output.
func wantRestQuaternionStamped(live string) string {
	return live
}

// yamlRestTransformStamped returns ROS YAML with live frame IDs.
func yamlRestTransformStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, child_frame_id: '%s-child', transform: {translation: {x: 1.0, y: 2.0, z: 3.0}, rotation: {x: 0.0, y: 0.0, z: 0.0, w: 1.0}}}", live, live)
}

// encodeRestTransformStamped encodes TransformStamped with live IDs.
func encodeRestTransformStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode tfstamped header: %w", err)
	}
	err = enc.WriteString(live + "-child")
	if err != nil {
		return nil, fmt.Errorf("encode tfstamped child: %w", err)
	}
	encodeRestTransformValues(enc)
	return enc.Bytes(), nil
}

// decodeRestTransformStamped decodes and checks live IDs plus transform.
func decodeRestTransformStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "tfstamped")
	if err != nil {
		return err
	}
	child, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode tfstamped child: %w", err)
	}
	if child != live+"-child" {
		return fmt.Errorf("tfstamped child %q wants %q", child, live+"-child")
	}
	err = decodeRestTransformValues(dec, "tfstamped")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish tfstamped: %w", err)
	}
	return nil
}

// wantRestTransformStamped returns the live ID expected in echo output.
func wantRestTransformStamped(live string) string {
	return live
}

// yamlRestTwistStamped returns ROS YAML with live frame_id.
func yamlRestTwistStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, twist: {linear: {x: 1.0, y: 0.0, z: 0.0}, angular: {x: 0.0, y: 0.0, z: 0.5}}}", live)
}

// encodeRestTwistStamped encodes TwistStamped with live frame_id.
func encodeRestTwistStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode twiststamped header: %w", err)
	}
	encodeRestAccelValues(enc)
	return enc.Bytes(), nil
}

// decodeRestTwistStamped decodes and checks live frame_id plus twist.
func decodeRestTwistStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "twiststamped")
	if err != nil {
		return err
	}
	err = decodeRestAccelValues(dec, "twiststamped")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish twiststamped: %w", err)
	}
	return nil
}

// wantRestTwistStamped returns the live ID expected in echo output.
func wantRestTwistStamped(live string) string {
	return live
}

// yamlRestTwistWithCovarianceStamped returns ROS YAML with live frame_id.
func yamlRestTwistWithCovarianceStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, twist: {twist: {linear: {x: 1.0, y: 0.0, z: 0.0}, angular: {x: 0.0, y: 0.0, z: 0.5}}, covariance: [%s]}}", live, restCovarianceYAML())
}

// encodeRestTwistWithCovarianceStamped encodes the stamped type as XCDR1.
func encodeRestTwistWithCovarianceStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode twistcovstamped header: %w", err)
	}
	encodeRestAccelValues(enc)
	encodeRestCovariance(enc)
	return enc.Bytes(), nil
}

// decodeRestTwistWithCovarianceStamped decodes and checks the sentinel.
func decodeRestTwistWithCovarianceStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "twistcovstamped")
	if err != nil {
		return err
	}
	err = decodeRestAccelValues(dec, "twistcovstamped")
	if err != nil {
		return err
	}
	err = decodeRestCovariance(dec, "twistcovstamped")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish twistcovstamped: %w", err)
	}
	return nil
}

// wantRestTwistWithCovarianceStamped returns the live ID in echo output.
func wantRestTwistWithCovarianceStamped(live string) string {
	return live
}

// yamlRestVector3Stamped returns ROS YAML with live frame_id.
func yamlRestVector3Stamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, vector: {x: 1.0, y: 2.0, z: 3.0}}", live)
}

// encodeRestVector3Stamped encodes Vector3Stamped with live frame_id.
func encodeRestVector3Stamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode vecstamped header: %w", err)
	}
	encodeRestPosition(enc)
	return enc.Bytes(), nil
}

// decodeRestVector3Stamped decodes and checks live frame_id plus vector.
func decodeRestVector3Stamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "vecstamped")
	if err != nil {
		return err
	}
	vx, vy, vz, err := helpers.ReadFloat64Triple(dec, "decode vecstamped x", "decode vecstamped y", "decode vecstamped z")
	if err != nil {
		return fmt.Errorf("read vecstamped: %w", err)
	}
	if vx != restPosX || vy != restPosY || vz != restPosZ {
		return fmt.Errorf("vecstamped (%v, %v, %v) wants (1.0, 2.0, 3.0)", vx, vy, vz)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish vecstamped: %w", err)
	}
	return nil
}

// wantRestVector3Stamped returns the live ID expected in echo output.
func wantRestVector3Stamped(live string) string {
	return live
}

// yamlRestWrenchStamped returns ROS YAML with live frame_id.
func yamlRestWrenchStamped(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, wrench: {force: {x: 1.0, y: 0.0, z: 0.0}, torque: {x: 0.0, y: 1.0, z: 0.0}}}", live)
}

// encodeRestWrenchStamped encodes WrenchStamped with live frame_id.
func encodeRestWrenchStamped(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode wrenchstamped header: %w", err)
	}
	encodeRestWrenchValues(enc)
	return enc.Bytes(), nil
}

// decodeRestWrenchStamped decodes and checks live frame_id plus wrench.
func decodeRestWrenchStamped(payload []byte, live string) error {
	dec, err := newRestDecoder(payload, live, "wrenchstamped")
	if err != nil {
		return err
	}
	err = decodeRestWrenchValues(dec, "wrenchstamped")
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish wrenchstamped: %w", err)
	}
	return nil
}

// wantRestWrenchStamped returns the live ID expected in echo output.
func wantRestWrenchStamped(live string) string {
	return live
}

// restGeometryCases returns the remaining geometry live matrix in corpus order.
//
// Order follows the corpus listing with stamped types after plain types.
// PointStamped plus Vector3 plus Pose plus Quaternion plus Twist are already
// covered by the base matrix and the extra geometry gate, so they are not
// repeated here. Header types carry the live ID, plain types use fixed
// sentinels with per-run topic plus domain isolation.
func restGeometryCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "geometry-point",
			RosType:    "geometry_msgs/msg/Point",
			IDLName:    "geometry_msgs__msg__Point",
			staticHash: restHashPoint,
			yaml:       yamlRestPoint,
			encode:     encodeRestPoint,
			decode:     decodeRestPoint,
			want:       wantRestPoint,
		},
		{
			Label:      "geometry-point32",
			RosType:    "geometry_msgs/msg/Point32",
			IDLName:    "geometry_msgs__msg__Point32",
			staticHash: restHashPoint32,
			yaml:       yamlRestPoint32,
			encode:     encodeRestPoint32,
			decode:     decodeRestPoint32,
			want:       wantRestPoint32,
		},
		{
			Label:      "geometry-accel",
			RosType:    "geometry_msgs/msg/Accel",
			IDLName:    "geometry_msgs__msg__Accel",
			staticHash: restHashAccel,
			yaml:       yamlRestAccel,
			encode:     encodeRestAccel,
			decode:     decodeRestAccel,
			want:       wantRestAccel,
		},
		{
			Label:      "geometry-inertia",
			RosType:    "geometry_msgs/msg/Inertia",
			IDLName:    "geometry_msgs__msg__Inertia",
			staticHash: restHashInertia,
			yaml:       yamlRestInertia,
			encode:     encodeRestInertia,
			decode:     decodeRestInertia,
			want:       wantRestInertia,
		},
		{
			Label:      "geometry-polygon",
			RosType:    "geometry_msgs/msg/Polygon",
			IDLName:    "geometry_msgs__msg__Polygon",
			staticHash: restHashPolygon,
			yaml:       yamlRestPolygon,
			encode:     encodeRestPolygon,
			decode:     decodeRestPolygon,
			want:       wantRestPolygon,
		},
		{
			Label:      "geometry-transform",
			RosType:    "geometry_msgs/msg/Transform",
			IDLName:    "geometry_msgs__msg__Transform",
			staticHash: restHashTransform,
			yaml:       yamlRestTransform,
			encode:     encodeRestTransform,
			decode:     decodeRestTransform,
			want:       wantRestTransform,
		},
		{
			Label:      "geometry-wrench",
			RosType:    "geometry_msgs/msg/Wrench",
			IDLName:    "geometry_msgs__msg__Wrench",
			staticHash: restHashWrench,
			yaml:       yamlRestWrench,
			encode:     encodeRestWrench,
			decode:     decodeRestWrench,
			want:       wantRestWrench,
		},
		{
			Label:      "geometry-accelcov",
			RosType:    "geometry_msgs/msg/AccelWithCovariance",
			IDLName:    "geometry_msgs__msg__AccelWithCovariance",
			staticHash: restHashAccelWithCovariance,
			yaml:       yamlRestAccelWithCovariance,
			encode:     encodeRestAccelWithCovariance,
			decode:     decodeRestAccelWithCovariance,
			want:       wantRestAccelWithCovariance,
		},
		{
			Label:      "geometry-posecov",
			RosType:    "geometry_msgs/msg/PoseWithCovariance",
			IDLName:    "geometry_msgs__msg__PoseWithCovariance",
			staticHash: restHashPoseWithCovariance,
			yaml:       yamlRestPoseWithCovariance,
			encode:     encodeRestPoseWithCovariance,
			decode:     decodeRestPoseWithCovariance,
			want:       wantRestPoseWithCovariance,
		},
		{
			Label:      "geometry-twistcov",
			RosType:    "geometry_msgs/msg/TwistWithCovariance",
			IDLName:    "geometry_msgs__msg__TwistWithCovariance",
			staticHash: restHashTwistWithCovariance,
			yaml:       yamlRestTwistWithCovariance,
			encode:     encodeRestTwistWithCovariance,
			decode:     decodeRestTwistWithCovariance,
			want:       wantRestTwistWithCovariance,
		},
		{
			Label:      "geometry-accelstamped",
			RosType:    "geometry_msgs/msg/AccelStamped",
			IDLName:    "geometry_msgs__msg__AccelStamped",
			staticHash: restHashAccelStamped,
			yaml:       yamlRestAccelStamped,
			encode:     encodeRestAccelStamped,
			decode:     decodeRestAccelStamped,
			want:       wantRestAccelStamped,
		},
		{
			Label:      "geometry-accelcovstamped",
			RosType:    "geometry_msgs/msg/AccelWithCovarianceStamped",
			IDLName:    "geometry_msgs__msg__AccelWithCovarianceStamped",
			staticHash: restHashAccelWithCovarianceStamped,
			yaml:       yamlRestAccelWithCovarianceStamped,
			encode:     encodeRestAccelWithCovarianceStamped,
			decode:     decodeRestAccelWithCovarianceStamped,
			want:       wantRestAccelWithCovarianceStamped,
		},
		{
			Label:      "geometry-inertiastamped",
			RosType:    "geometry_msgs/msg/InertiaStamped",
			IDLName:    "geometry_msgs__msg__InertiaStamped",
			staticHash: restHashInertiaStamped,
			yaml:       yamlRestInertiaStamped,
			encode:     encodeRestInertiaStamped,
			decode:     decodeRestInertiaStamped,
			want:       wantRestInertiaStamped,
		},
		{
			Label:      "geometry-polygonstamped",
			RosType:    "geometry_msgs/msg/PolygonStamped",
			IDLName:    "geometry_msgs__msg__PolygonStamped",
			staticHash: restHashPolygonStamped,
			yaml:       yamlRestPolygonStamped,
			encode:     encodeRestPolygonStamped,
			decode:     decodeRestPolygonStamped,
			want:       wantRestPolygonStamped,
		},
		{
			Label:      "geometry-posearray",
			RosType:    "geometry_msgs/msg/PoseArray",
			IDLName:    "geometry_msgs__msg__PoseArray",
			staticHash: restHashPoseArray,
			yaml:       yamlRestPoseArray,
			encode:     encodeRestPoseArray,
			decode:     decodeRestPoseArray,
			want:       wantRestPoseArray,
		},
		{
			Label:      "geometry-posestamped",
			RosType:    "geometry_msgs/msg/PoseStamped",
			IDLName:    "geometry_msgs__msg__PoseStamped",
			staticHash: restHashPoseStamped,
			yaml:       yamlRestPoseStamped,
			encode:     encodeRestPoseStamped,
			decode:     decodeRestPoseStamped,
			want:       wantRestPoseStamped,
		},
		{
			Label:      "geometry-posecovstamped",
			RosType:    "geometry_msgs/msg/PoseWithCovarianceStamped",
			IDLName:    "geometry_msgs__msg__PoseWithCovarianceStamped",
			staticHash: restHashPoseWithCovarianceStamped,
			yaml:       yamlRestPoseWithCovarianceStamped,
			encode:     encodeRestPoseWithCovarianceStamped,
			decode:     decodeRestPoseWithCovarianceStamped,
			want:       wantRestPoseWithCovarianceStamped,
		},
		{
			Label:      "geometry-quatstamped",
			RosType:    "geometry_msgs/msg/QuaternionStamped",
			IDLName:    "geometry_msgs__msg__QuaternionStamped",
			staticHash: restHashQuaternionStamped,
			yaml:       yamlRestQuaternionStamped,
			encode:     encodeRestQuaternionStamped,
			decode:     decodeRestQuaternionStamped,
			want:       wantRestQuaternionStamped,
		},
		{
			Label:      "geometry-tfstamped",
			RosType:    "geometry_msgs/msg/TransformStamped",
			IDLName:    "geometry_msgs__msg__TransformStamped",
			staticHash: restHashTransformStamped,
			yaml:       yamlRestTransformStamped,
			encode:     encodeRestTransformStamped,
			decode:     decodeRestTransformStamped,
			want:       wantRestTransformStamped,
		},
		{
			Label:      "geometry-twiststamped",
			RosType:    "geometry_msgs/msg/TwistStamped",
			IDLName:    "geometry_msgs__msg__TwistStamped",
			staticHash: restHashTwistStamped,
			yaml:       yamlRestTwistStamped,
			encode:     encodeRestTwistStamped,
			decode:     decodeRestTwistStamped,
			want:       wantRestTwistStamped,
		},
		{
			Label:      "geometry-twistcovstamped",
			RosType:    "geometry_msgs/msg/TwistWithCovarianceStamped",
			IDLName:    "geometry_msgs__msg__TwistWithCovarianceStamped",
			staticHash: restHashTwistWithCovarianceStamped,
			yaml:       yamlRestTwistWithCovarianceStamped,
			encode:     encodeRestTwistWithCovarianceStamped,
			decode:     decodeRestTwistWithCovarianceStamped,
			want:       wantRestTwistWithCovarianceStamped,
		},
		{
			Label:      "geometry-vecstamped",
			RosType:    "geometry_msgs/msg/Vector3Stamped",
			IDLName:    "geometry_msgs__msg__Vector3Stamped",
			staticHash: restHashVector3Stamped,
			yaml:       yamlRestVector3Stamped,
			encode:     encodeRestVector3Stamped,
			decode:     decodeRestVector3Stamped,
			want:       wantRestVector3Stamped,
		},
		{
			Label:      "geometry-wrenchstamped",
			RosType:    "geometry_msgs/msg/WrenchStamped",
			IDLName:    "geometry_msgs__msg__WrenchStamped",
			staticHash: restHashWrenchStamped,
			yaml:       yamlRestWrenchStamped,
			encode:     encodeRestWrenchStamped,
			decode:     decodeRestWrenchStamped,
			want:       wantRestWrenchStamped,
		},
	}
}

// runRestGeometryLabel runs forward plus reverse for one rest geometry label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. Forward and reverse reuse the shared matrix runners
// so domain 42 plus docker argv plus hash probing stay identical.
func runRestGeometryLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, restGeometryCases())
}

// TestROSTypeGeometryPoint checks Point forward plus reverse.
//
// Point carries no string field, so the fixed triple uses per-run topic
// plus domain isolation.
func TestROSTypeGeometryPoint(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-point")
}

// TestROSTypeGeometryPoint32 checks Point32 forward plus reverse.
//
// Point32 carries no string field, so the fixed triple uses per-run topic
// plus domain isolation.
func TestROSTypeGeometryPoint32(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-point32")
}

// TestROSTypeGeometryAccel checks Accel forward plus reverse.
//
// Accel carries no string field, so the fixed linear plus angular sentinel
// uses per-run topic plus domain isolation.
func TestROSTypeGeometryAccel(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-accel")
}

// TestROSTypeGeometryInertia checks Inertia forward plus reverse.
//
// Inertia carries no string field, so the fixed mass plus com plus tensor
// sentinel uses per-run topic plus domain isolation.
func TestROSTypeGeometryInertia(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-inertia")
}

// TestROSTypeGeometryPolygon checks Polygon forward plus reverse.
//
// Polygon carries no string field, so the fixed two point sentinel uses
// per-run topic plus domain isolation.
func TestROSTypeGeometryPolygon(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-polygon")
}

// TestROSTypeGeometryTransform checks Transform forward plus reverse.
//
// Transform carries no string field, so the fixed translation plus rotation
// sentinel uses per-run topic plus domain isolation.
func TestROSTypeGeometryTransform(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-transform")
}

// TestROSTypeGeometryWrench checks Wrench forward plus reverse.
//
// Wrench carries no string field, so the fixed force plus torque sentinel
// uses per-run topic plus domain isolation.
func TestROSTypeGeometryWrench(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-wrench")
}

// TestROSTypeGeometryAccelWithCovariance checks AccelWithCovariance.
//
// AccelWithCovariance carries no string field, so the fixed accel plus zero
// covariance sentinel uses per-run topic plus domain isolation.
func TestROSTypeGeometryAccelWithCovariance(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-accelcov")
}

// TestROSTypeGeometryPoseWithCovariance checks PoseWithCovariance.
//
// PoseWithCovariance carries no string field, so the fixed pose plus zero
// covariance sentinel uses per-run topic plus domain isolation.
func TestROSTypeGeometryPoseWithCovariance(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-posecov")
}

// TestROSTypeGeometryTwistWithCovariance checks TwistWithCovariance.
//
// TwistWithCovariance carries no string field, so the fixed twist plus zero
// covariance sentinel uses per-run topic plus domain isolation.
func TestROSTypeGeometryTwistWithCovariance(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-twistcov")
}

// TestROSTypeGeometryAccelStamped checks AccelStamped forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeGeometryAccelStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-accelstamped")
}

// TestROSTypeGeometryAccelWithCovarianceStamped checks the stamped type.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeGeometryAccelWithCovarianceStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-accelcovstamped")
}

// TestROSTypeGeometryInertiaStamped checks InertiaStamped forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeGeometryInertiaStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-inertiastamped")
}

// TestROSTypeGeometryPolygonStamped checks PolygonStamped forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeGeometryPolygonStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-polygonstamped")
}

// TestROSTypeGeometryPoseArray checks PoseArray forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
// The single pose entry stays fixed.
func TestROSTypeGeometryPoseArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-posearray")
}

// TestROSTypeGeometryPoseStamped checks PoseStamped forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeGeometryPoseStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-posestamped")
}

// TestROSTypeGeometryPoseWithCovarianceStamped checks the stamped type.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeGeometryPoseWithCovarianceStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-posecovstamped")
}

// TestROSTypeGeometryQuaternionStamped checks QuaternionStamped.
//
// Header frame_id carries the per-run live ID so stale runs never match.
// Quaternion stays identity.
func TestROSTypeGeometryQuaternionStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-quatstamped")
}

// TestROSTypeGeometryTransformStamped checks TransformStamped.
//
// Header frame_id plus child_frame_id carry per-run live IDs so stale runs
// never match.
func TestROSTypeGeometryTransformStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-tfstamped")
}

// TestROSTypeGeometryTwistStamped checks TwistStamped forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeGeometryTwistStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-twiststamped")
}

// TestROSTypeGeometryTwistWithCovarianceStamped checks the stamped type.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeGeometryTwistWithCovarianceStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-twistcovstamped")
}

// TestROSTypeGeometryVector3Stamped checks Vector3Stamped forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeGeometryVector3Stamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-vecstamped")
}

// TestROSTypeGeometryWrenchStamped checks WrenchStamped forward plus reverse.
//
// Header frame_id carries the per-run live ID so stale runs never match.
func TestROSTypeGeometryWrenchStamped(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestGeometryLabel(t, ctx, domain, "geometry-wrenchstamped")
}
