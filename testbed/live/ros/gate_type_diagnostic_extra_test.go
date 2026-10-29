// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop,dupl // test breadth pins many wire branches and repeats independent service shapes
package ros_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Static type hash from ros2 topic info -v on weld-jazzy.
//
// This is the RIHS01 hash reported for diagnostic_msgs DiagnosticArray on
// domain 42. Live probing via ros2 topic info -v is preferred at runtime;
// this value is the fallback when the probe cannot run. The value was
// captured from weld-jazzy Jazzy.
const diagExtraHashArray = "RIHS01_5a8a36efb05fb25070fa0fb3810290c0e6cd4862b54a8fb975a1ee8dc55a333e"

// Fixed sentinels for diagnostic extra service shapes.
//
// SelfTest passed is a byte in Jazzy IDL, not a bool, per
// /opt/ros/jazzy/share/diagnostic_msgs/srv/SelfTest.srv. AddDiagnostics
// request carries a global namespace string and response carries bool
// success plus string message, per AddDiagnostics.srv. Service GUID and
// sequence pin the service envelope for pure Go round trips.
const (
	diagExtraSelfTestPassed uint8  = 1
	diagExtraServiceSeq     uint64 = 7
)

// diagExtraServiceGUID returns a fixed 8 byte service GUID for tests.
func diagExtraServiceGUID() [8]byte {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	return guid
}

// writeDiagExtraStatus writes one DiagnosticStatus with live name and value.
func writeDiagExtraStatus(enc *types.Encoder, live string) error {
	enc.WriteUint8(diagStatusLevel)
	err := enc.WriteString(live)
	if err != nil {
		return fmt.Errorf("encode diag extra status name: %w", err)
	}
	err = enc.WriteString(diagStatusMessage)
	if err != nil {
		return fmt.Errorf("encode diag extra status message: %w", err)
	}
	err = enc.WriteString(diagStatusHardware)
	if err != nil {
		return fmt.Errorf("encode diag extra status hardware: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode diag extra status values len: %w", err)
	}
	err = enc.WriteString(diagKey)
	if err != nil {
		return fmt.Errorf("encode diag extra status key: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return fmt.Errorf("encode diag extra status value: %w", err)
	}
	return nil
}

// readDiagExtraStatus reads and checks one DiagnosticStatus with live name and value.
func readDiagExtraStatus(dec *types.Decoder, live string) error {
	err := decodeDiagIdentity(dec, live)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode diag extra status values len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("diag extra status values len %d wants 1", n)
	}
	key, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag extra status key: %w", err)
	}
	if key != diagKey {
		return fmt.Errorf("diag extra status key %q wants %q", key, diagKey)
	}
	value, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag extra status value: %w", err)
	}
	if value != live {
		return fmt.Errorf("diag extra status value %q wants %q", value, live)
	}
	return nil
}

// yamlDiagExtraArray returns ROS YAML for DiagnosticArray with live IDs.
func yamlDiagExtraArray(live string) string {
	return fmt.Sprintf("{header: {stamp: {sec: 0, nanosec: 0}, frame_id: '%s'}, status: [{level: 0, name: '%s', message: 'weld-ok', hardware_id: 'weld-hw', values: [{key: 'weld_key', value: '%s'}]}]}", live, live, live)
}

// encodeDiagExtraArrayWithEndian encodes DiagnosticArray with live IDs in one status.
func encodeDiagExtraArrayWithEndian(live string, le bool) ([]byte, error) {
	enc := types.NewEncoder(le)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode diag extra array header: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode diag extra array status len: %w", err)
	}
	err = writeDiagExtraStatus(enc, live)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// encodeDiagExtraArray encodes DiagnosticArray with live IDs as XCDR1 LE.
func encodeDiagExtraArray(live string) ([]byte, error) {
	payload, err := encodeDiagExtraArrayWithEndian(live, true)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// decodeDiagExtraArray decodes and checks live frame plus one live status.
func decodeDiagExtraArray(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode diag extra array header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode diag extra array frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("diag extra array frame %q wants %q", frame, live)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode diag extra array status len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("diag extra array status len %d wants 1", n)
	}
	err = readDiagExtraStatus(dec, live)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish diag extra array: %w", err)
	}
	return nil
}

// wantDiagExtraArray returns the live ID expected in echo output.
func wantDiagExtraArray(live string) string {
	return live
}

// diagExtraCases returns the diagnostic extra live matrix.
//
// Only DiagnosticArray is a topic type and can run the shared forward plus
// reverse matrix. SelfTest and AddDiagnostics are services on rq and rr
// DDS topics, not on the rt topic path, so they are out of topic gate
// scope and are pinned by pure Go service round trips below.
func diagExtraCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "diag-array",
			RosType:    "diagnostic_msgs/msg/DiagnosticArray",
			IDLName:    "diagnostic_msgs__msg__DiagnosticArray",
			staticHash: diagExtraHashArray,
			yaml:       yamlDiagExtraArray,
			encode:     encodeDiagExtraArray,
			decode:     decodeDiagExtraArray,
			want:       wantDiagExtraArray,
		},
	}
}

// runDiagExtraLabel runs forward plus reverse for one diagnostic extra label.
//
// Domain is the allocated exclusive domain shared by both directions.
func runDiagExtraLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, diagExtraCases())
}

// TestROSTypeDiagArray checks DiagnosticArray forward plus reverse.
func TestROSTypeDiagArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runDiagExtraLabel(t, ctx, domain, "diag-array")
}

// encodeDiagExtraSelfTestRequestWire encodes an empty SelfTest request with service header.
func encodeDiagExtraSelfTestRequestWire(le bool, guid [8]byte, seq uint64) ([]byte, error) {
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("encode selftest request header: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeDiagExtraSelfTestRequestWire decodes an empty SelfTest request with service header.
func decodeDiagExtraSelfTestRequestWire(wire []byte, guid [8]byte, seq uint64) error {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return fmt.Errorf("decode selftest request header: %w", err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		return fmt.Errorf("decode selftest request service header: %w", err)
	}
	if gotGUID != guid {
		return fmt.Errorf("selftest request guid = %v wants %v", gotGUID, guid)
	}
	if gotSeq != seq {
		return fmt.Errorf("selftest request seq = %d wants %d", gotSeq, seq)
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("decode selftest request pad: %w", err)
		}
		if pad != 0x00 {
			return fmt.Errorf("selftest request pad 0x%02x wants zero", pad)
		}
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish selftest request: %w", err)
	}
	return nil
}

// encodeDiagExtraSelfTestResponseWire encodes a SelfTest response with service header.
func encodeDiagExtraSelfTestResponseWire(le bool, guid [8]byte, seq uint64, live string) ([]byte, error) {
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("encode selftest response header: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode selftest response id: %w", err)
	}
	enc.WriteUint8(diagExtraSelfTestPassed)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode selftest response status len: %w", err)
	}
	err = writeDiagExtraStatus(enc, live)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeDiagExtraSelfTestResponseWire decodes a SelfTest response with service header.
func decodeDiagExtraSelfTestResponseWire(wire []byte, guid [8]byte, seq uint64, live string) error {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return fmt.Errorf("decode selftest response header: %w", err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		return fmt.Errorf("decode selftest response service header: %w", err)
	}
	if gotGUID != guid {
		return fmt.Errorf("selftest response guid = %v wants %v", gotGUID, guid)
	}
	if gotSeq != seq {
		return fmt.Errorf("selftest response seq = %d wants %d", gotSeq, seq)
	}
	id, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode selftest response id: %w", err)
	}
	if id != live {
		return fmt.Errorf("selftest response id %q wants %q", id, live)
	}
	passed, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode selftest response passed: %w", err)
	}
	if passed != diagExtraSelfTestPassed {
		return fmt.Errorf("selftest response passed %d wants %d", passed, diagExtraSelfTestPassed)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode selftest response status len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("selftest response status len %d wants 1", n)
	}
	err = readDiagExtraStatus(dec, live)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish selftest response: %w", err)
	}
	return nil
}

// encodeDiagExtraAddRequestWire encodes an AddDiagnostics request with service header.
func encodeDiagExtraAddRequestWire(le bool, guid [8]byte, seq uint64, live string) ([]byte, error) {
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("encode add request header: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode add request namespace: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeDiagExtraAddRequestWire decodes an AddDiagnostics request with service header.
func decodeDiagExtraAddRequestWire(wire []byte, guid [8]byte, seq uint64, live string) error {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return fmt.Errorf("decode add request header: %w", err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		return fmt.Errorf("decode add request service header: %w", err)
	}
	if gotGUID != guid {
		return fmt.Errorf("add request guid = %v wants %v", gotGUID, guid)
	}
	if gotSeq != seq {
		return fmt.Errorf("add request seq = %d wants %d", gotSeq, seq)
	}
	ns, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode add request namespace: %w", err)
	}
	if ns != live {
		return fmt.Errorf("add request namespace %q wants %q", ns, live)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish add request: %w", err)
	}
	return nil
}

// encodeDiagExtraAddResponseWire encodes an AddDiagnostics response with service header.
func encodeDiagExtraAddResponseWire(le bool, guid [8]byte, seq uint64, live string) ([]byte, error) {
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("encode add response header: %w", err)
	}
	enc.WriteBool(true)
	err = enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode add response message: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeDiagExtraAddResponseWire decodes an AddDiagnostics response with service header.
func decodeDiagExtraAddResponseWire(wire []byte, guid [8]byte, seq uint64, live string) error {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return fmt.Errorf("decode add response header: %w", err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		return fmt.Errorf("decode add response service header: %w", err)
	}
	if gotGUID != guid {
		return fmt.Errorf("add response guid = %v wants %v", gotGUID, guid)
	}
	if gotSeq != seq {
		return fmt.Errorf("add response seq = %d wants %d", gotSeq, seq)
	}
	success, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode add response success: %w", err)
	}
	if !success {
		return errors.New("add response success false wants true") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	message, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode add response message: %w", err)
	}
	if message != live {
		return fmt.Errorf("add response message %q wants %q", message, live)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish add response: %w", err)
	}
	return nil
}

// TestROSDiagExtraArrayLocal checks DiagnosticArray XCDR1 round trip with no peer.
func TestROSDiagExtraArrayLocal(t *testing.T) {
	t.Parallel()
	live := "diag-extra-local"
	for _, le := range []bool{true, false} {
		payload, err := encodeDiagExtraArrayWithEndian(live, le)
		if err != nil {
			t.Fatalf("encode diag extra array le %v: %v", le, err)
		}
		err = decodeDiagExtraArray(payload, live)
		if err != nil {
			t.Fatalf("decode diag extra array le %v: %v", le, err)
		}
	}
}

// TestROSDiagExtraSelfTestLocal checks SelfTest service XCDR1 round trip with no peer.
func TestROSDiagExtraSelfTestLocal(t *testing.T) {
	t.Parallel()
	live := "diag-extra-selftest-local"
	guid := diagExtraServiceGUID()
	for _, le := range []bool{true, false} {
		reqWire, err := encodeDiagExtraSelfTestRequestWire(le, guid, diagExtraServiceSeq)
		if err != nil {
			t.Fatalf("encode selftest request le %v: %v", le, err)
		}
		err = decodeDiagExtraSelfTestRequestWire(reqWire, guid, diagExtraServiceSeq)
		if err != nil {
			t.Fatalf("decode selftest request le %v: %v", le, err)
		}
		respWire, err := encodeDiagExtraSelfTestResponseWire(le, guid, diagExtraServiceSeq+1, live)
		if err != nil {
			t.Fatalf("encode selftest response le %v: %v", le, err)
		}
		err = decodeDiagExtraSelfTestResponseWire(respWire, guid, diagExtraServiceSeq+1, live)
		if err != nil {
			t.Fatalf("decode selftest response le %v: %v", le, err)
		}
	}
}

// TestROSDiagExtraAddDiagnosticsLocal checks AddDiagnostics service XCDR1 round trip with no peer.
func TestROSDiagExtraAddDiagnosticsLocal(t *testing.T) {
	t.Parallel()
	live := "diag-extra-add-local"
	guid := diagExtraServiceGUID()
	for _, le := range []bool{true, false} {
		reqWire, err := encodeDiagExtraAddRequestWire(le, guid, diagExtraServiceSeq+2, live)
		if err != nil {
			t.Fatalf("encode add request le %v: %v", le, err)
		}
		err = decodeDiagExtraAddRequestWire(reqWire, guid, diagExtraServiceSeq+2, live)
		if err != nil {
			t.Fatalf("decode add request le %v: %v", le, err)
		}
		respWire, err := encodeDiagExtraAddResponseWire(le, guid, diagExtraServiceSeq+3, live)
		if err != nil {
			t.Fatalf("encode add response le %v: %v", le, err)
		}
		err = decodeDiagExtraAddResponseWire(respWire, guid, diagExtraServiceSeq+3, live)
		if err != nil {
			t.Fatalf("decode add response le %v: %v", le, err)
		}
	}
}
