// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop,dupl,gocognit // test breadth pins many similar wire checks in one file
package ros_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Fixed sentinels for remaining std_msgs types.
//
// ColorRGBA carries no string field and uses a fixed float sentinel with
// per-run topic plus domain isolation. Dimension plus Layout plus
// ByteMultiArray carry the per-run live ID in dim label so concurrent runs
// never match.
const (
	restColorR        float32 = 1.5
	restColorG        float32 = -2.5
	restColorB        float32 = 3.5
	restColorA        float32 = 0.5
	restDimSize       uint32  = 2
	restDimStride     uint32  = 4
	restLayoutOffset  uint32  = 1
	restByteDimSize   uint32  = 4
	restByteDimStride uint32  = 4
	restByteOffset    uint32  = 0
)

// restByteData holds the 4 byte payload for ByteMultiArray.
//
// Length 4 keeps the wire form 4 byte aligned so Finish reports no trailing
// pad. Values match the ASCII B plus C plus D plus E range used by the
// single Byte gate.
var restByteData = []byte{66, 67, 68, 69}

// Static type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for each ROS type on domain 42.
// Live probing via ros2 topic info -v is preferred at runtime; these values
// are the fallback when the probe cannot run. Values were captured from
// weld-jazzy Jazzy.
const (
	restHashColorRGBA      = "RIHS01_77a7a5b9ae477306097665106e0413ba74440245b1f3d0c6d6405fe5c7813fe8"
	restHashDimension      = "RIHS01_5e773a60a4c7fc8a54985f307c7837aa2994252a126c301957a24e31282c9cbe"
	restHashLayout         = "RIHS01_4c66e6f78e740ac103a94cf63259f968e48c617e7699e829b63c21a5cb50dac6"
	restHashByteMultiArray = "RIHS01_972fec7f50ab3c1d06783c228e79e8a9a509021708c511c059926261ada901d4"
)

// typeRestCases returns the remaining std_msgs matrix in corpus order.
//
// Order is Dimension plus Layout plus ColorRGBA plus ByteMultiArray. Header
// plus Empty plus all numerics plus String plus Char plus Byte are already
// covered by the base and extra sets, so this set closes std_msgs.
func typeRestCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "std-multiarraydimension",
			RosType:    "std_msgs/msg/MultiArrayDimension",
			IDLName:    "std_msgs__msg__MultiArrayDimension",
			staticHash: restHashDimension,
			yaml:       yamlRestDimension,
			encode:     encodeRestDimension,
			decode:     decodeRestDimension,
			want:       wantRestDimension,
		},
		{
			Label:      "std-multiarraylayout",
			RosType:    "std_msgs/msg/MultiArrayLayout",
			IDLName:    "std_msgs__msg__MultiArrayLayout",
			staticHash: restHashLayout,
			yaml:       yamlRestLayout,
			encode:     encodeRestLayout,
			decode:     decodeRestLayout,
			want:       wantRestLayout,
		},
		{
			Label:      "std-colorrgba",
			RosType:    "std_msgs/msg/ColorRGBA",
			IDLName:    "std_msgs__msg__ColorRGBA",
			staticHash: restHashColorRGBA,
			yaml:       yamlRestColorRGBA,
			encode:     encodeRestColorRGBA,
			decode:     decodeRestColorRGBA,
			want:       wantRestColorRGBA,
		},
		{
			Label:      "std-bytemultiarray",
			RosType:    "std_msgs/msg/ByteMultiArray",
			IDLName:    "std_msgs__msg__ByteMultiArray",
			staticHash: restHashByteMultiArray,
			yaml:       yamlRestByteMultiArray,
			encode:     encodeRestByteMultiArray,
			decode:     decodeRestByteMultiArray,
			want:       wantRestByteMultiArray,
		},
	}
}

// yamlRestColorRGBA returns ROS YAML for the ColorRGBA sentinel.
func yamlRestColorRGBA(live string) string {
	return "{r: 1.5, g: -2.5, b: 3.5, a: 0.5}"
}

// encodeRestColorRGBA encodes the fixed ColorRGBA sentinel as XCDR1.
func encodeRestColorRGBA(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteFloat32(restColorR)
	enc.WriteFloat32(restColorG)
	enc.WriteFloat32(restColorB)
	enc.WriteFloat32(restColorA)
	return enc.Bytes(), nil
}

// decodeRestColorRGBA decodes and checks the ColorRGBA sentinel.
func decodeRestColorRGBA(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode color header: %w", err)
	}
	r, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode color r: %w", err)
	}
	g, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode color g: %w", err)
	}
	b, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode color b: %w", err)
	}
	a, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("decode color a: %w", err)
	}
	if r != restColorR || g != restColorG || b != restColorB || a != restColorA {
		return fmt.Errorf("color (%v, %v, %v, %v) wants (1.5, -2.5, 3.5, 0.5)", r, g, b, a)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish color: %w", err)
	}
	return nil
}

// wantRestColorRGBA returns the echo substring for the ColorRGBA sentinel.
func wantRestColorRGBA(live string) string {
	return "b: 3.5"
}

// yamlRestDimension returns ROS YAML with live label.
func yamlRestDimension(live string) string {
	return fmt.Sprintf("{label: '%s', size: 2, stride: 4}", live)
}

// encodeRestDimension encodes MultiArrayDimension with live label.
func encodeRestDimension(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode dimension label: %w", err)
	}
	enc.WriteUint32(restDimSize)
	enc.WriteUint32(restDimStride)
	return enc.Bytes(), nil
}

// decodeRestDimension decodes and checks live label plus size plus stride.
func decodeRestDimension(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode dimension header: %w", err)
	}
	label, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode dimension label: %w", err)
	}
	if label != live {
		return fmt.Errorf("dimension label %q wants %q", label, live)
	}
	size, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode dimension size: %w", err)
	}
	if size != restDimSize {
		return fmt.Errorf("dimension size %d wants %d", size, restDimSize)
	}
	stride, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode dimension stride: %w", err)
	}
	if stride != restDimStride {
		return fmt.Errorf("dimension stride %d wants %d", stride, restDimStride)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish dimension: %w", err)
	}
	return nil
}

// wantRestDimension returns the live ID expected in echo output.
func wantRestDimension(live string) string {
	return live
}

// yamlRestLayout returns ROS YAML with live dim label.
func yamlRestLayout(live string) string {
	return fmt.Sprintf("{dim: [{label: '%s', size: 2, stride: 4}], data_offset: 1}", live)
}

// encodeRestLayout encodes MultiArrayLayout with one live dim.
func encodeRestLayout(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode layout dim len: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode layout label: %w", err)
	}
	enc.WriteUint32(restDimSize)
	enc.WriteUint32(restDimStride)
	enc.WriteUint32(restLayoutOffset)
	return enc.Bytes(), nil
}

// decodeRestLayout decodes and checks live label plus sizes plus offset.
func decodeRestLayout(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode layout header: %w", err)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode layout dim len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("layout dim len %d wants 1", n)
	}
	label, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode layout label: %w", err)
	}
	if label != live {
		return fmt.Errorf("layout label %q wants %q", label, live)
	}
	size, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode layout size: %w", err)
	}
	if size != restDimSize {
		return fmt.Errorf("layout size %d wants %d", size, restDimSize)
	}
	stride, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode layout stride: %w", err)
	}
	if stride != restDimStride {
		return fmt.Errorf("layout stride %d wants %d", stride, restDimStride)
	}
	offset, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode layout offset: %w", err)
	}
	if offset != restLayoutOffset {
		return fmt.Errorf("layout offset %d wants %d", offset, restLayoutOffset)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish layout: %w", err)
	}
	return nil
}

// wantRestLayout returns the live ID expected in echo output.
func wantRestLayout(live string) string {
	return live
}

// yamlRestByteMultiArray returns ROS YAML with live dim label plus data.
//
// The data list uses per-element !!binary form for bytes 66 plus 67 plus 68
// plus 69. The integer list form data [66, 67, 68, 69] crashes ros2 topic pub
// with SIGABRT on the PyBytes_Check assert in _byte_multi_array_s.c, while
// the per-element !!binary list publishes cleanly and echo renders B through
// E. Empty data [] also publishes cleanly.
func yamlRestByteMultiArray(live string) string {
	return fmt.Sprintf("{layout: {dim: [{label: '%s', size: 4, stride: 4}], data_offset: 0}, data: [!!binary Qg==, !!binary Qw==, !!binary RA==, !!binary RQ==]}", live)
}

// encodeRestByteMultiArray encodes ByteMultiArray with live label plus data.
func encodeRestByteMultiArray(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode byte array dim len: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode byte array label: %w", err)
	}
	enc.WriteUint32(restByteDimSize)
	enc.WriteUint32(restByteDimStride)
	enc.WriteUint32(restByteOffset)
	err = enc.WriteOctetSequence(restByteData)
	if err != nil {
		return nil, fmt.Errorf("encode byte array data: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeRestByteMultiArray decodes and checks live label plus data bytes.
func decodeRestByteMultiArray(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode byte array header: %w", err)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode byte array dim len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("byte array dim len %d wants 1", n)
	}
	label, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode byte array label: %w", err)
	}
	if label != live {
		return fmt.Errorf("byte array label %q wants %q", label, live)
	}
	size, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode byte array size: %w", err)
	}
	if size != restByteDimSize {
		return fmt.Errorf("byte array size %d wants %d", size, restByteDimSize)
	}
	stride, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode byte array stride: %w", err)
	}
	if stride != restByteDimStride {
		return fmt.Errorf("byte array stride %d wants %d", stride, restByteDimStride)
	}
	offset, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("decode byte array offset: %w", err)
	}
	if offset != restByteOffset {
		return fmt.Errorf("byte array offset %d wants %d", offset, restByteOffset)
	}
	data, err := dec.ReadOctetSequence()
	if err != nil {
		return fmt.Errorf("decode byte array data: %w", err)
	}
	if len(data) != len(restByteData) {
		return fmt.Errorf("byte array data len %d wants %d", len(data), len(restByteData))
	}
	for i := range data {
		if data[i] != restByteData[i] {
			return fmt.Errorf("byte array data byte %d 0x%02x wants 0x%02x", i, data[i], restByteData[i])
		}
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish byte array: %w", err)
	}
	return nil
}

// wantRestByteMultiArray returns the live ID expected in echo output.
func wantRestByteMultiArray(live string) string {
	return live
}

// runRestLabel runs forward plus reverse for one rest matrix label.
//
// It looks up the case in the rest set, skips when the peer lacks the
// type, then runs forward with one live ID and reverse with another so the
// two directions never share a topic. It reuses the base matrix forward
// and reverse paths so hash resolution and backend handling stay identical.
func runRestLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, typeRestCases())
}

// TestROSTypeStdMultiArrayDimension checks MultiArrayDimension forward plus reverse.
//
// Forward starts ros2 topic pub for the dimension and decodes via NewDecoder
// on the Weld take path. Reverse publishes the live label via Weld and
// watches ros2 topic echo. The label carries the per-run live ID so stale
// runs never match.
func TestROSTypeStdMultiArrayDimension(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestLabel(t, ctx, domain, "std-multiarraydimension")
}

// TestROSTypeStdMultiArrayLayout checks MultiArrayLayout forward plus reverse.
//
// It mirrors the dimension case with one dim plus a fixed data offset. The
// live label in dim isolates per-run topics.
func TestROSTypeStdMultiArrayLayout(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestLabel(t, ctx, domain, "std-multiarraylayout")
}

// TestROSTypeStdColorRGBA checks ColorRGBA forward plus reverse.
//
// Forward starts ros2 topic pub for ColorRGBA and decodes via NewDecoder on
// the Weld take path. Reverse publishes the fixed sentinel via Weld and
// watches ros2 topic echo. The sentinel uses per-run topic plus domain
// isolation because ColorRGBA carries no string field.
func TestROSTypeStdColorRGBA(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestLabel(t, ctx, domain, "std-colorrgba")
}

// TestROSTypeStdByteMultiArray checks ByteMultiArray forward plus reverse.
//
// Forward starts ros2 topic pub for ByteMultiArray and decodes via NewDecoder
// on the Weld take path. Reverse publishes the live label plus fixed bytes
// via Weld and watches ros2 topic echo. The dim label carries the per-run
// live ID so stale runs never match.
func TestROSTypeStdByteMultiArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runRestLabel(t, ctx, domain, "std-bytemultiarray")
}

// startRestByteMultiArrayEcho starts ros2 topic echo for ByteMultiArray into a file.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, so
// parallel workers never share a domain.
func startRestByteMultiArrayEcho(t *testing.T, ctx context.Context, domain int, topic, logPath string) {
	t.Helper()
	echoFile, err := os.Create(logPath) //nolint:gosec // live harness uses controlled temp file path
	if err != nil {
		t.Fatalf("create ros echo log: %v", err)
	}
	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close ros echo log: %v", cerr)
		}
	})
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", topic, "std_msgs/msg/ByteMultiArray")
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with fixed test args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("start ros2 topic echo: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), typeMatrixCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
		if !helpers.WaitForContainerProcGone(cleanupCtx, helpers.Container, topic, helpers.PeerStopTimeout) {
			t.Errorf("stop peer proc survived kill on domain %v pattern %q", domain, topic)
		}
	})
}

// verifyRestByteMultiArrayEcho asserts one echo doc holds the live ID plus byte values.
func verifyRestByteMultiArrayEcho(t *testing.T, echoPath, live string) {
	t.Helper()
	data, err := os.ReadFile(echoPath) //nolint:gosec // live harness reads controlled temp file path
	if err != nil {
		t.Fatalf("read ros echo log: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, live) {
		t.Fatalf("ros echo misses live ID %q\necho output:\n%s", live, text)
	}
	for _, marker := range []string{"- B", "- C", "- D", "- E"} {
		if !strings.Contains(text, marker) {
			t.Fatalf("ros echo misses byte %q for %q\necho output:\n%s", marker, live, text)
		}
	}
}

// TestROSTypeStdByteMultiArrayReverse checks ByteMultiArray reverse only.
//
// Forward stays blocked by the ros2 CLI toolchain crash documented on
// yamlRestByteMultiArray, so the shared forward plus reverse gate cannot
// reach reverse. This test covers reverse alone with the same sentinel the
// forward test uses: it encodes the live label plus fixed bytes locally,
// publishes through Weld, and proves ros2 topic echo sees the live ID plus
// all byte values. It skips only when no peer runs or the type is absent;
// a missing echo with healthy peer fails.
func TestROSTypeStdByteMultiArrayReverse(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, "std_msgs/msg/ByteMultiArray", typeMatrixProbeWait)
	live := helpers.LiveID("ros-rev-std-bytemultiarray")
	topic := helpers.LiveTopic(live)
	payload, err := encodeRestByteMultiArray(live)
	if err != nil {
		t.Fatalf("local encode byte array: %v", err)
	}
	err = decodeRestByteMultiArray(payload, live)
	if err != nil {
		t.Fatalf("local decode byte array: %v", err)
	}
	t.Logf("local byte array codec ok len %d id %s", len(payload), live)
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, restHashByteMultiArray, typeMatrixProbeWait, "std-bytemultiarray-reverse")
	if err != nil {
		t.Skipf("matrix hash unavailable for std-bytemultiarray-reverse topic %s, skipping: %v", topic, err)
	}
	t.Logf("matrix reverse std-bytemultiarray-reverse topic %s hash %s", topic, typeHash)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "ros-rev-std-bytemultiarray-"+live, "std-bytemultiarray-reverse") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	echoPath := filepath.Join(t.TempDir(), "rosecho-rev-std-bytemultiarray-"+live+".log")
	startRestByteMultiArrayEcho(t, ctx, domain, topic, echoPath)
	helpers.Sleep(ctx, typeMatrixEchoNap)
	cerr := ctx.Err()
	if cerr != nil {
		t.Fatalf("context ended before byte array reverse pub: %v", cerr)
	}
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  "std_msgs__msg__ByteMultiArray",
		Payload:   payload,
	}
	want := wantRestByteMultiArray(live)
	if !helpers.PublishUntilEcho(ctx, t, backend, msg, echoPath, want, typeHash, typeMatrixPollWait, typeMatrixRepubWait, typeMatrixPubRetry, typeMatrixPubInterval, "std-bytemultiarray-reverse") {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // live harness reads controlled temp file path
		if rerr != nil {
			t.Fatalf("ros2 topic echo did not see byte array payload %q: %v", want, rerr)
		}
		t.Fatalf("ros2 topic echo did not see byte array payload %q hash %s, pub ok\necho output:\n%s", want, typeHash, string(data))
	}
	verifyRestByteMultiArrayEcho(t, echoPath, live)
	t.Logf("byte array reverse ok id %s", live)
}
