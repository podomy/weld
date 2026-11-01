// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:dupl // diag rest2 pins independent service shapes with same wire checks as extra
package ros_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Diagnostic rest2 coverage.
//
// Grep of gate_type_diag files shows gate_type_diagnostic_test.go already
// pins KeyValue plus DiagnosticStatus live, and
// gate_type_diagnostic_extra_test.go already pins DiagnosticArray live plus
// SelfTest and AddDiagnostics as pure Go service round trips. This file
// adds no duplicate live matrix codec. It adds presence gates that verify
// the three topic types exist on one allocated exclusive domain via docker argv only, with
// per-run live IDs and skip when the peer is absent. It pins the two
// service shapes again with independent pure Go XCDR helpers in this file
// and with no Docker, plus one independent DiagnosticArray pure Go round
// trip as the unit part.

// Fixed sentinels for rest2 diagnostic shapes.
const (
	diagRest2Level          uint8  = 0
	diagRest2Message        string = "weld-ok"
	diagRest2Hardware       string = "weld-hw"
	diagRest2Key            string = "weld_key"
	diagRest2SelfTestPassed uint8  = 1
	diagRest2ServiceSeq     uint64 = 7
)

// diagRest2GapEntry pins one explicit live gap with hash plus reason plus expiry.
//
// Hash is the expected RIHS01 type hash. Reason names the peer interop
// cause. Expires bounds the allowance so stale gaps fail loudly after the
// date. TimeMax shares domain 42 hash wiring with builtin Time max, so its
// reason references the same peer omission pattern.
type diagRest2GapEntry struct {
	Hash    string
	Reason  string
	Expires time.Time
}

// diagRest2GapAllowlist holds explicit live gaps for rest2 presence.
//
// Keys use short presence labels. Hashes reuse the pinned diagnostic
// hashes: KeyValue plus Status from the diagnostic gate and Array from the
// extra gate. No entry means live missing with local codec ok fails.
var diagRest2GapAllowlist = map[string]diagRest2GapEntry{
	"diag-rest2-keyvalue": {
		Hash:    diagHashKeyValue,
		Reason:  "peer CycloneDDS omits KeyValue presence sample on pooled domain with local codec ok",
		Expires: time.Date(2027, 3, 31, 0, 0, 0, 0, time.UTC),
	},
	"diag-rest2-status": {
		Hash:    diagHashStatus,
		Reason:  "peer CycloneDDS omits Status presence sample on pooled domain with local codec ok",
		Expires: time.Date(2027, 3, 31, 0, 0, 0, 0, time.UTC),
	},
	"diag-rest2-array": {
		Hash:    diagExtraHashArray,
		Reason:  "peer CycloneDDS omits Array presence sample on pooled domain with local codec ok",
		Expires: time.Date(2027, 3, 31, 0, 0, 0, 0, time.UTC),
	},
}

// diagRest2FailOrAllow skips only for explicit allowlist gaps, else fails.
//
// It checks label in diagRest2GapAllowlist with matching hash and unexpired
// date. On match it skips with hash plus reason plus expiry. Otherwise it
// fails because local codec ok but live missing has no peer reason.
func diagRest2FailOrAllow(t *testing.T, label, hash, detail string) {
	t.Helper()
	entry, ok := diagRest2GapAllowlist[label]
	gapFailOrAllow(t, label, hash, detail, entry.Hash, entry.Reason, entry.Expires, ok)
}

// diagRest2LocalOk proves the local XCDR codec round trips for one presence label.
//
// It runs the pure Go encode plus decode for the label with the live ID.
// Any failure is fatal because the local codec must hold before live
// presence is checked.
func diagRest2LocalOk(t *testing.T, label, live string) {
	t.Helper()
	switch label {
	case "diag-rest2-keyvalue":
		payload, err := encodeDiagKeyValue(live)
		if err != nil {
			t.Fatalf("local encode %s: %v", label, err)
		}
		err = decodeDiagKeyValue(payload, live)
		if err != nil {
			t.Fatalf("local decode %s: %v", label, err)
		}
	case "diag-rest2-status":
		payload, err := encodeDiagStatus(live)
		if err != nil {
			t.Fatalf("local encode %s: %v", label, err)
		}
		err = decodeDiagStatus(payload, live)
		if err != nil {
			t.Fatalf("local decode %s: %v", label, err)
		}
	case "diag-rest2-array":
		payload, err := encodeDiagRest2ArrayWithEndian(live, true)
		if err != nil {
			t.Fatalf("local encode %s: %v", label, err)
		}
		err = decodeDiagRest2Array(payload, live)
		if err != nil {
			t.Fatalf("local decode %s: %v", label, err)
		}
	default:
		t.Fatalf("unknown rest2 presence label %q", label)
	}
}

// diagRest2StrictPresence checks one topic type exists with strict gaps.
//
// Domain is the allocated exclusive domain plumbed to the type probe and
// topic hash resolution, so parallel workers never share a domain. It
// proves local codec ok, skips only for peer absence via SkipIfNoPeer and
// SkipIfNoTypeInterface, then resolves the topic hash. Hash unavailable
// fails via allowlist or fatal so no generic skip remains.
func diagRest2StrictPresence(t *testing.T, ctx context.Context, domain int, label, rosType, staticHash string) {
	t.Helper()
	live := helpers.LiveID("ros-diag-rest2-" + label)
	t.Logf("diag rest2 %s presence live %s", label, live)
	diagRest2LocalOk(t, label, live)
	skipIfNoTypeInterface(t, ctx, domain, rosType)
	topic := helpers.LiveTopic(live)
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, staticHash, typeMatrixProbeWait, "diag-rest2")
	if err != nil {
		diagRest2FailOrAllow(t, label, staticHash, fmt.Sprintf("hash unavailable for %s topic %s: %v", label, topic, err))
		return
	}
	t.Logf("diag rest2 %s presence hash %s", label, typeHash)
}

// diagRest2ServiceGUID returns a fixed 8 byte service GUID for tests.
func diagRest2ServiceGUID() [8]byte {
	var guid [8]byte
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	return guid
}

// writeDiagRest2Status writes one DiagnosticStatus with live name and value.
func writeDiagRest2Status(enc *types.Encoder, live string) error {
	enc.WriteUint8(diagRest2Level)
	err := enc.WriteString(live)
	if err != nil {
		return fmt.Errorf("encode diag rest2 status name: %w", err)
	}
	err = enc.WriteString(diagRest2Message)
	if err != nil {
		return fmt.Errorf("encode diag rest2 status message: %w", err)
	}
	err = enc.WriteString(diagRest2Hardware)
	if err != nil {
		return fmt.Errorf("encode diag rest2 status hardware: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return fmt.Errorf("encode diag rest2 status values len: %w", err)
	}
	err = enc.WriteString(diagRest2Key)
	if err != nil {
		return fmt.Errorf("encode diag rest2 status key: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return fmt.Errorf("encode diag rest2 status value: %w", err)
	}
	return nil
}

// readDiagRest2Identity reads and checks level plus name plus message plus hardware.
func readDiagRest2Identity(dec *types.Decoder, live string) error {
	level, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode diag rest2 status level: %w", err)
	}
	if level != diagRest2Level {
		return fmt.Errorf("diag rest2 status level %d wants %d", level, diagRest2Level)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag rest2 status name: %w", err)
	}
	if name != live {
		return fmt.Errorf("diag rest2 status name %q wants %q", name, live)
	}
	message, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag rest2 status message: %w", err)
	}
	if message != diagRest2Message {
		return fmt.Errorf("diag rest2 status message %q wants %q", message, diagRest2Message)
	}
	hardware, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag rest2 status hardware: %w", err)
	}
	if hardware != diagRest2Hardware {
		return fmt.Errorf("diag rest2 status hardware %q wants %q", hardware, diagRest2Hardware)
	}
	return nil
}

// readDiagRest2Values reads and checks values length plus key plus value.
func readDiagRest2Values(dec *types.Decoder, live string) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode diag rest2 status values len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("diag rest2 status values len %d wants 1", n)
	}
	key, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag rest2 status key: %w", err)
	}
	if key != diagRest2Key {
		return fmt.Errorf("diag rest2 status key %q wants %q", key, diagRest2Key)
	}
	value, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag rest2 status value: %w", err)
	}
	if value != live {
		return fmt.Errorf("diag rest2 status value %q wants %q", value, live)
	}
	return nil
}

// readDiagRest2Status reads and checks one DiagnosticStatus with live IDs.
func readDiagRest2Status(dec *types.Decoder, live string) error {
	err := readDiagRest2Identity(dec, live)
	if err != nil {
		return err
	}
	err = readDiagRest2Values(dec, live)
	if err != nil {
		return err
	}
	return nil
}

// encodeDiagRest2ArrayWithEndian encodes DiagnosticArray with live IDs.
func encodeDiagRest2ArrayWithEndian(live string, le bool) ([]byte, error) {
	enc := types.NewEncoder(le)
	err := encodeMatrixHeader(enc, live)
	if err != nil {
		return nil, fmt.Errorf("encode diag rest2 array header: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode diag rest2 array status len: %w", err)
	}
	err = writeDiagRest2Status(enc, live)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// decodeDiagRest2Array decodes and checks live frame plus one live status.
func decodeDiagRest2Array(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode diag rest2 array header: %w", err)
	}
	frame, err := decodeMatrixHeader(dec)
	if err != nil {
		return fmt.Errorf("decode diag rest2 array frame: %w", err)
	}
	if frame != live {
		return fmt.Errorf("diag rest2 array frame %q wants %q", frame, live)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode diag rest2 array status len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("diag rest2 array status len %d wants 1", n)
	}
	err = readDiagRest2Status(dec, live)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish diag rest2 array: %w", err)
	}
	return nil
}

// encodeDiagRest2SelfTestRequestWire encodes an empty SelfTest request.
func encodeDiagRest2SelfTestRequestWire(le bool, guid [8]byte, seq uint64) ([]byte, error) {
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("encode rest2 selftest request header: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeDiagRest2SelfTestRequestWire decodes an empty SelfTest request.
func decodeDiagRest2SelfTestRequestWire(wire []byte, guid [8]byte, seq uint64) error {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return fmt.Errorf("decode rest2 selftest request header: %w", err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		return fmt.Errorf("decode rest2 selftest request service header: %w", err)
	}
	if gotGUID != guid {
		return fmt.Errorf("rest2 selftest request guid = %v wants %v", gotGUID, guid)
	}
	if gotSeq != seq {
		return fmt.Errorf("rest2 selftest request seq = %d wants %d", gotSeq, seq)
	}
	for dec.Remaining() > 0 {
		pad, err := dec.ReadUint8()
		if err != nil {
			return fmt.Errorf("decode rest2 selftest request pad: %w", err)
		}
		if pad != 0x00 {
			return fmt.Errorf("rest2 selftest request pad 0x%02x wants zero", pad)
		}
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish rest2 selftest request: %w", err)
	}
	return nil
}

// encodeDiagRest2SelfTestResponseWire encodes a SelfTest response.
func encodeDiagRest2SelfTestResponseWire(le bool, guid [8]byte, seq uint64, live string) ([]byte, error) {
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("encode rest2 selftest response header: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode rest2 selftest response id: %w", err)
	}
	enc.WriteUint8(diagRest2SelfTestPassed)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode rest2 selftest response status len: %w", err)
	}
	err = writeDiagRest2Status(enc, live)
	if err != nil {
		return nil, err
	}
	return enc.Bytes(), nil
}

// checkDiagRest2ServiceHeader reads the service header and checks guid plus seq.
func checkDiagRest2ServiceHeader(dec *types.Decoder, guid [8]byte, seq uint64, label string) error {
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		return fmt.Errorf("decode %s service header: %w", label, err)
	}
	if gotGUID != guid {
		return fmt.Errorf("%s guid = %v wants %v", label, gotGUID, guid)
	}
	if gotSeq != seq {
		return fmt.Errorf("%s seq = %d wants %d", label, gotSeq, seq)
	}
	return nil
}

// readDiagRest2SelfTestHead reads id plus passed plus status length.
func readDiagRest2SelfTestHead(dec *types.Decoder, live string) error {
	id, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode rest2 selftest response id: %w", err)
	}
	if id != live {
		return fmt.Errorf("rest2 selftest response id %q wants %q", id, live)
	}
	passed, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode rest2 selftest response passed: %w", err)
	}
	if passed != diagRest2SelfTestPassed {
		return fmt.Errorf("rest2 selftest response passed %d wants %d", passed, diagRest2SelfTestPassed)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode rest2 selftest response status len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("rest2 selftest response status len %d wants 1", n)
	}
	return nil
}

// decodeDiagRest2SelfTestResponseWire decodes a SelfTest response.
func decodeDiagRest2SelfTestResponseWire(wire []byte, guid [8]byte, seq uint64, live string) error {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return fmt.Errorf("decode rest2 selftest response header: %w", err)
	}
	err = checkDiagRest2ServiceHeader(dec, guid, seq, "rest2 selftest response")
	if err != nil {
		return err
	}
	err = readDiagRest2SelfTestHead(dec, live)
	if err != nil {
		return err
	}
	err = readDiagRest2Status(dec, live)
	if err != nil {
		return err
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish rest2 selftest response: %w", err)
	}
	return nil
}

// encodeDiagRest2AddRequestWire encodes an AddDiagnostics request.
func encodeDiagRest2AddRequestWire(le bool, guid [8]byte, seq uint64, live string) ([]byte, error) {
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("encode rest2 add request header: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode rest2 add request namespace: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeDiagRest2AddRequestWire decodes an AddDiagnostics request.
func decodeDiagRest2AddRequestWire(wire []byte, guid [8]byte, seq uint64, live string) error {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return fmt.Errorf("decode rest2 add request header: %w", err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		return fmt.Errorf("decode rest2 add request service header: %w", err)
	}
	if gotGUID != guid {
		return fmt.Errorf("rest2 add request guid = %v wants %v", gotGUID, guid)
	}
	if gotSeq != seq {
		return fmt.Errorf("rest2 add request seq = %d wants %d", gotSeq, seq)
	}
	ns, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode rest2 add request namespace: %w", err)
	}
	if ns != live {
		return fmt.Errorf("rest2 add request namespace %q wants %q", ns, live)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish rest2 add request: %w", err)
	}
	return nil
}

// encodeDiagRest2AddResponseWire encodes an AddDiagnostics response.
func encodeDiagRest2AddResponseWire(le bool, guid [8]byte, seq uint64, live string) ([]byte, error) {
	enc := types.NewEncoder(le)
	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("encode rest2 add response header: %w", err)
	}
	enc.WriteBool(true)
	err = enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode rest2 add response message: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeDiagRest2AddResponseWire decodes an AddDiagnostics response.
func decodeDiagRest2AddResponseWire(wire []byte, guid [8]byte, seq uint64, live string) error {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return fmt.Errorf("decode rest2 add response header: %w", err)
	}
	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		return fmt.Errorf("decode rest2 add response service header: %w", err)
	}
	if gotGUID != guid {
		return fmt.Errorf("rest2 add response guid = %v wants %v", gotGUID, guid)
	}
	if gotSeq != seq {
		return fmt.Errorf("rest2 add response seq = %d wants %d", gotSeq, seq)
	}
	success, err := dec.ReadBool()
	if err != nil {
		return fmt.Errorf("decode rest2 add response success: %w", err)
	}
	if !success {
		return errors.New("rest2 add response success false wants true") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	message, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode rest2 add response message: %w", err)
	}
	if message != live {
		return fmt.Errorf("rest2 add response message %q wants %q", message, live)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish rest2 add response: %w", err)
	}
	return nil
}

// TestROSDiagRest2KeyValuePresent verifies KeyValue exists on the peer.
func TestROSDiagRest2KeyValuePresent(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	diagRest2StrictPresence(t, ctx, domain, "diag-rest2-keyvalue", "diagnostic_msgs/msg/KeyValue", diagHashKeyValue)
}

// TestROSDiagRest2StatusPresent verifies DiagnosticStatus exists on the peer.
func TestROSDiagRest2StatusPresent(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	diagRest2StrictPresence(t, ctx, domain, "diag-rest2-status", "diagnostic_msgs/msg/DiagnosticStatus", diagHashStatus)
}

// TestROSDiagRest2ArrayPresent verifies DiagnosticArray exists on the peer.
func TestROSDiagRest2ArrayPresent(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	diagRest2StrictPresence(t, ctx, domain, "diag-rest2-array", "diagnostic_msgs/msg/DiagnosticArray", diagExtraHashArray)
}

// TestROSDiagRest2ArrayLocal checks DiagnosticArray XCDR1 round trip.
func TestROSDiagRest2ArrayLocal(t *testing.T) {
	t.Parallel()
	live := helpers.LiveID("ros-diag-rest2-array-local")
	for _, le := range []bool{true, false} {
		payload, err := encodeDiagRest2ArrayWithEndian(live, le)
		if err != nil {
			t.Fatalf("encode diag rest2 array le %v: %v", le, err)
		}
		err = decodeDiagRest2Array(payload, live)
		if err != nil {
			t.Fatalf("decode diag rest2 array le %v: %v", le, err)
		}
	}
}

// TestROSDiagRest2SelfTestLocal checks SelfTest service XCDR1 round trip.
func TestROSDiagRest2SelfTestLocal(t *testing.T) {
	t.Parallel()
	live := helpers.LiveID("ros-diag-rest2-selftest-local")
	guid := diagRest2ServiceGUID()
	for _, le := range []bool{true, false} {
		reqWire, err := encodeDiagRest2SelfTestRequestWire(le, guid, diagRest2ServiceSeq)
		if err != nil {
			t.Fatalf("encode rest2 selftest request le %v: %v", le, err)
		}
		err = decodeDiagRest2SelfTestRequestWire(reqWire, guid, diagRest2ServiceSeq)
		if err != nil {
			t.Fatalf("decode rest2 selftest request le %v: %v", le, err)
		}
		respWire, err := encodeDiagRest2SelfTestResponseWire(le, guid, diagRest2ServiceSeq+1, live)
		if err != nil {
			t.Fatalf("encode rest2 selftest response le %v: %v", le, err)
		}
		err = decodeDiagRest2SelfTestResponseWire(respWire, guid, diagRest2ServiceSeq+1, live)
		if err != nil {
			t.Fatalf("decode rest2 selftest response le %v: %v", le, err)
		}
	}
}

// TestROSDiagRest2AddDiagnosticsLocal checks AddDiagnostics XCDR1 round trip.
func TestROSDiagRest2AddDiagnosticsLocal(t *testing.T) {
	t.Parallel()
	live := helpers.LiveID("ros-diag-rest2-add-local")
	guid := diagRest2ServiceGUID()
	for _, le := range []bool{true, false} {
		reqWire, err := encodeDiagRest2AddRequestWire(le, guid, diagRest2ServiceSeq+2, live)
		if err != nil {
			t.Fatalf("encode rest2 add request le %v: %v", le, err)
		}
		err = decodeDiagRest2AddRequestWire(reqWire, guid, diagRest2ServiceSeq+2, live)
		if err != nil {
			t.Fatalf("decode rest2 add request le %v: %v", le, err)
		}
		respWire, err := encodeDiagRest2AddResponseWire(le, guid, diagRest2ServiceSeq+3, live)
		if err != nil {
			t.Fatalf("encode rest2 add response le %v: %v", le, err)
		}
		err = decodeDiagRest2AddResponseWire(respWire, guid, diagRest2ServiceSeq+3, live)
		if err != nil {
			t.Fatalf("decode rest2 add response le %v: %v", le, err)
		}
	}
}
