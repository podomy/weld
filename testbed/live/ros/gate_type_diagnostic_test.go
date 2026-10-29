// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:dupl // diag status pins independent topic shape with same wire checks as rest2
package ros_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Static type hashes from ros2 topic info -v on weld-jazzy.
//
// These are the RIHS01 hashes reported for the diagnostic types on
// domain 42. Live probing via ros2 topic info -v is preferred at runtime;
// these values are the fallback when the probe cannot run.
const (
	diagHashStatus   = "RIHS01_b0e3e692ea2d54a8af2f4ef1930e81556a2db55216b771f8a7d2724ed47bf0e4"
	diagHashKeyValue = "RIHS01_d68081eaa540288c5440753baecef0c4e16e81a5f78ad68902ded5100413bb42"
)

// diagGapEntry pins one explicit live gap with hash plus reason plus expiry.
//
// Hash is the expected RIHS01 type hash. Reason names the peer interop
// cause. Expires bounds the allowance so stale gaps fail loudly after the
// date.
type diagGapEntry struct {
	Hash    string
	Reason  string
	Expires time.Time
}

// diagGapAllowlist holds explicit live gaps for diag_status.
//
// Key diag-status uses diagHashStatus. Key diag-keyvalue is not listed so
// live missing with local codec ok fails instead of skipping.
var diagGapAllowlist = map[string]diagGapEntry{
	"diag-status": {
		Hash:    diagHashStatus,
		Reason:  "peer CycloneDDS omits DiagnosticStatus sample on domain 42 with local codec ok",
		Expires: time.Date(2027, 3, 31, 0, 0, 0, 0, time.UTC),
	},
}

// diagFailOrAllow skips only for explicit allowlist gaps, else fails.
//
// It checks label in diagGapAllowlist with matching hash and unexpired
// date. On match it skips with hash plus reason plus expiry. Otherwise it
// fails because local codec ok but live missing has no peer reason.
func diagFailOrAllow(t *testing.T, label, hash, detail string) {
	t.Helper()
	entry, ok := diagGapAllowlist[label]
	if ok && entry.Hash == hash && time.Now().Before(entry.Expires) && helpers.IsValidRIHSHash(entry.Hash) {
		t.Skipf("allowlisted live gap %s hash %s reason %s expires %s: %s", label, entry.Hash, entry.Reason, entry.Expires.Format("2006-01-02"), detail)
	}
	if ok && !time.Now().Before(entry.Expires) {
		t.Fatalf("expired allowlist %s hash %s reason %s expired %s: %s", label, entry.Hash, entry.Reason, entry.Expires.Format("2006-01-02"), detail)
	}
	t.Fatalf("live missing %s hash %s with local codec ok, no peer reason and no allowlist: %s", label, hash, detail)
}

// runDiagStrictForward checks Weld take sees a ROS talker with strict gaps.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, topic
// hash probing, and the Weld backend. It mirrors runMatrixForward for domain and argv but fails when live is
// missing with local codec ok unless the label is explicitly allowlisted
// with hash plus reason plus expiry. Peer absence still skips via
// SkipIfNoPeer and SkipIfNoTypeInterface only.
//
//nolint:gocognit,cyclop // live forward path asserts identity, hash, codec, delivery in one run
func runDiagStrictForward(t *testing.T, ctx context.Context, domain int, tc typeMatrixCase, live, topic string) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "talker-"+live+"-"+tc.Label+".log")
	logFile, err := os.Create(logPath) //nolint:gosec // test writes controlled temp path
	if err != nil {
		t.Fatalf("create talker log %s: %v", tc.Label, err)
	}
	t.Cleanup(func() {
		cerr := logFile.Close()
		if cerr != nil {
			t.Logf("close talker log %s: %v", tc.Label, cerr)
		}
	})
	yaml := tc.yaml(live)
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", topic, tc.RosType, yaml, "--rate", "2", "--qos-reliability", "reliable", "--qos-durability", "transient_local", "--qos-history", "keep_last", "--qos-depth", "10")
	talker := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // test harness runs docker with controlled args
	talker.Stdout = logFile
	talker.Stderr = logFile
	err = talker.Start()
	if err != nil {
		t.Fatalf("start ROS talker %s: %v", tc.Label, err)
	}
	stopTalker := func() {
		helpers.StopCmd(talker)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), typeMatrixCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
	}
	t.Cleanup(stopTalker)
	helpers.Sleep(ctx, typeMatrixTalkerNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before take %s: %v", tc.Label, ctx.Err())
	}
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, tc.staticHash, typeMatrixProbeWait, "matrix")
	if err != nil {
		diagFailOrAllow(t, tc.Label, tc.staticHash, fmt.Sprintf("hash unavailable for %s topic %s: %v", tc.Label, topic, err))
		return
	}
	t.Logf("matrix forward %s topic %s hash %s", tc.Label, topic, typeHash)
	payload, err := tc.encode(live)
	if err != nil {
		t.Fatalf("local encode %s: %v", tc.Label, err)
	}
	err = tc.decode(payload, live)
	if err != nil {
		t.Fatalf("local decode %s: %v", tc.Label, err)
	}
	backend, err := helpers.OpenLiveBackend(ctx, domain, "weld-type-matrix-forward", "matrix")
	if err != nil {
		t.Fatalf("open backend %s: %v", tc.Label, err)
	}
	//nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
	closeBackend := func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("close backend %s: %v", tc.Label, cerr)
		}
	}
	t.Cleanup(closeBackend)
	err = helpers.SubscribeLiveReader(ctx, backend, topic, tc.IDLName, typeHash, "matrix")
	if err != nil {
		t.Fatalf("subscribe %s: %v", tc.Label, err)
	}
	takeCtx, takeCancel := context.WithTimeout(ctx, typeMatrixTakeWait)
	defer takeCancel()
	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
				break
			}
			text := terr.Error()
			if strings.Contains(text, "not implemented") {
				t.Fatalf("weld take backend error %s: %v", tc.Label, terr)
			}
			if strings.Contains(text, "not wired") {
				t.Fatalf("weld take backend error %s: %v", tc.Label, terr)
			}
			continue
		}
		derr := tc.decode(msg.Payload, live)
		if derr == nil {
			closeBackend()
			stopTalker()
			return
		}
	}
	closeBackend()
	stopTalker()
	diagFailOrAllow(t, tc.Label, typeHash, fmt.Sprintf("weld take did not see %s payload", tc.Label))
}

// diagPublishUntilEcho publishes until ros2 topic echo sees want.
//
// It retries with PublishLiveWithRetry and polls the echo file. A publish
// without routes fails via allowlist or fatal so no generic skip remains.
// It reports true when the echo holds want.
func diagPublishUntilEcho(ctx context.Context, t *testing.T, backend *dds.Backend, msg transports.OutgoingMessage, echoPath, want, typeHash, label string) bool {
	t.Helper()
	deadline := time.Now().Add(typeMatrixPollWait)
	for {
		err := helpers.PublishLiveWithRetry(ctx, backend, msg, typeMatrixPubRetry, typeMatrixPubInterval, label)
		if err != nil {
			if strings.Contains(err.Error(), "no route") {
				diagFailOrAllow(t, label, typeHash, fmt.Sprintf("weld pub has no route: %v", err))
				return false
			}
			t.Fatalf("weld pub %s: %v", label, err)
		}
		if helpers.WaitForFileContains(ctx, echoPath, want, typeMatrixRepubWait) {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		if time.Now().After(deadline) {
			return false
		}
	}
}

// runDiagStrictReverse checks ros2 topic echo sees a Weld pub with strict gaps.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, topic
// hash probing, and the Weld backend. It mirrors runMatrixReverse for domain and argv but fails when echo is
// missing with local codec ok unless the label is explicitly allowlisted
// with hash plus reason plus expiry.
func runDiagStrictReverse(t *testing.T, ctx context.Context, domain int, tc typeMatrixCase, live, topic string) {
	t.Helper()
	echoPath := filepath.Join(t.TempDir(), "rosecho-"+live+"-"+tc.Label+".log")
	echoFile, err := os.Create(echoPath) //nolint:gosec // test writes controlled temp path
	if err != nil {
		t.Fatalf("create ros echo log %s: %v", tc.Label, err)
	}
	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close ros echo log %s: %v", tc.Label, cerr)
		}
	})
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", topic, tc.RosType)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // test harness runs docker with controlled args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("start ros2 topic echo %s: %v", tc.Label, err)
	}
	stopEcho := func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), typeMatrixCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
	}
	t.Cleanup(stopEcho)
	helpers.Sleep(ctx, typeMatrixEchoNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before pub %s: %v", tc.Label, ctx.Err())
	}
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, tc.staticHash, typeMatrixProbeWait, "matrix")
	if err != nil {
		diagFailOrAllow(t, tc.Label, tc.staticHash, fmt.Sprintf("hash unavailable for %s topic %s: %v", tc.Label, topic, err))
		return
	}
	t.Logf("matrix reverse %s topic %s hash %s", tc.Label, topic, typeHash)
	payload, err := tc.encode(live)
	if err != nil {
		t.Fatalf("local encode %s: %v", tc.Label, err)
	}
	err = tc.decode(payload, live)
	if err != nil {
		t.Fatalf("local decode %s: %v", tc.Label, err)
	}
	backend, err := helpers.OpenLiveBackend(ctx, domain, "weld-type-matrix-reverse", "matrix")
	if err != nil {
		t.Fatalf("open backend %s: %v", tc.Label, err)
	}
	//nolint:contextcheck // backend Close is cleanup without caller ctx, goodbye uses bounded Background
	closeBackend := func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("close backend %s: %v", tc.Label, cerr)
		}
	}
	t.Cleanup(closeBackend)
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  tc.IDLName,
		Payload:   payload,
	}
	want := tc.want(live)
	if diagPublishUntilEcho(ctx, t, backend, msg, echoPath, want, typeHash, tc.Label) {
		closeBackend()
		stopEcho()
		return
	}
	data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads controlled temp path
	if rerr != nil {
		t.Fatalf("ros2 topic echo did not see %s payload %q: %v", tc.Label, want, rerr)
	}
	closeBackend()
	stopEcho()
	diagFailOrAllow(t, tc.Label, typeHash, fmt.Sprintf("ros2 topic echo did not see %s payload %q echo output: %s", tc.Label, want, string(data)))
}

// Fixed diagnostic sentinels shared by YAML, encode, and decode.
//
// Name and value carry the per-run live ID so stale runs never match.
// Level stays OK, message and hardware stay fixed, and the KeyValue key
// stays fixed so only the live ID varies per run.
const (
	diagStatusLevel    uint8  = 0
	diagStatusMessage  string = "weld-ok"
	diagStatusHardware string = "weld-hw"
	diagKey            string = "weld_key"
)

// yamlDiagStatus returns ROS YAML for DiagnosticStatus with live name and value.
func yamlDiagStatus(live string) string {
	return fmt.Sprintf("{level: 0, name: '%s', message: 'weld-ok', hardware_id: 'weld-hw', values: [{key: 'weld_key', value: '%s'}]}", live, live)
}

// encodeDiagStatus encodes DiagnosticStatus with live name and value as XCDR1.
func encodeDiagStatus(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	enc.WriteUint8(diagStatusLevel)
	err := enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode diag status name: %w", err)
	}
	err = enc.WriteString(diagStatusMessage)
	if err != nil {
		return nil, fmt.Errorf("encode diag status message: %w", err)
	}
	err = enc.WriteString(diagStatusHardware)
	if err != nil {
		return nil, fmt.Errorf("encode diag status hardware: %w", err)
	}
	err = enc.WriteSequenceLength(1)
	if err != nil {
		return nil, fmt.Errorf("encode diag status values len: %w", err)
	}
	err = enc.WriteString(diagKey)
	if err != nil {
		return nil, fmt.Errorf("encode diag status key: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode diag status value: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeDiagIdentity checks level plus name plus message plus hardware.
func decodeDiagIdentity(dec *types.Decoder, live string) error {
	level, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("decode diag status level: %w", err)
	}
	if level != diagStatusLevel {
		return fmt.Errorf("diag status level %d wants %d", level, diagStatusLevel)
	}
	name, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag status name: %w", err)
	}
	if name != live {
		return fmt.Errorf("diag status name %q wants %q", name, live)
	}
	message, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag status message: %w", err)
	}
	if message != diagStatusMessage {
		return fmt.Errorf("diag status message %q wants %q", message, diagStatusMessage)
	}
	hardware, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag status hardware: %w", err)
	}
	if hardware != diagStatusHardware {
		return fmt.Errorf("diag status hardware %q wants %q", hardware, diagStatusHardware)
	}
	return nil
}

// decodeDiagStatus decodes and checks live name plus fixed fields plus live value.
func decodeDiagStatus(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode diag status header: %w", err)
	}
	err = decodeDiagIdentity(dec, live)
	if err != nil {
		return err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("decode diag status values len: %w", err)
	}
	if n != 1 {
		return fmt.Errorf("diag status values len %d wants 1", n)
	}
	key, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag status key: %w", err)
	}
	if key != diagKey {
		return fmt.Errorf("diag status key %q wants %q", key, diagKey)
	}
	value, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag status value: %w", err)
	}
	if value != live {
		return fmt.Errorf("diag status value %q wants %q", value, live)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish diag status: %w", err)
	}
	return nil
}

// wantDiagStatus returns the live ID expected in echo output.
func wantDiagStatus(live string) string {
	return live
}

// yamlDiagKeyValue returns ROS YAML for KeyValue with live value.
func yamlDiagKeyValue(live string) string {
	return fmt.Sprintf("{key: 'weld_key', value: '%s'}", live)
}

// encodeDiagKeyValue encodes KeyValue with live value as XCDR1.
func encodeDiagKeyValue(live string) ([]byte, error) {
	enc := types.NewEncoder(true)
	err := enc.WriteString(diagKey)
	if err != nil {
		return nil, fmt.Errorf("encode diag keyvalue key: %w", err)
	}
	err = enc.WriteString(live)
	if err != nil {
		return nil, fmt.Errorf("encode diag keyvalue value: %w", err)
	}
	return enc.Bytes(), nil
}

// decodeDiagKeyValue decodes and checks fixed key plus live value.
func decodeDiagKeyValue(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("decode diag keyvalue header: %w", err)
	}
	key, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag keyvalue key: %w", err)
	}
	if key != diagKey {
		return fmt.Errorf("diag keyvalue key %q wants %q", key, diagKey)
	}
	value, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("decode diag keyvalue value: %w", err)
	}
	if value != live {
		return fmt.Errorf("diag keyvalue value %q wants %q", value, live)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish diag keyvalue: %w", err)
	}
	return nil
}

// wantDiagKeyValue returns the live ID expected in echo output.
func wantDiagKeyValue(live string) string {
	return live
}

// diagCases returns the diagnostic live matrix in dependency order.
//
// KeyValue comes first because DiagnosticStatus nests it, so the plain
// pair proves the leaf codec before the parent status with its sequence.
func diagCases() []typeMatrixCase {
	return []typeMatrixCase{
		{
			Label:      "diag-keyvalue",
			RosType:    "diagnostic_msgs/msg/KeyValue",
			IDLName:    "diagnostic_msgs__msg__KeyValue",
			staticHash: diagHashKeyValue,
			yaml:       yamlDiagKeyValue,
			encode:     encodeDiagKeyValue,
			decode:     decodeDiagKeyValue,
			want:       wantDiagKeyValue,
		},
		{
			Label:      "diag-status",
			RosType:    "diagnostic_msgs/msg/DiagnosticStatus",
			IDLName:    "diagnostic_msgs__msg__DiagnosticStatus",
			staticHash: diagHashStatus,
			yaml:       yamlDiagStatus,
			encode:     encodeDiagStatus,
			decode:     decodeDiagStatus,
			want:       wantDiagStatus,
		},
	}
}

// runDiagLabel runs forward plus reverse for one diagnostic label.
//
// Domain is the allocated exclusive domain shared by both directions.
// It looks up the case, skips when the peer lacks the type, then runs
// forward with one live ID and reverse with another so the two directions
// never share a topic. It uses strict runners so live missing with local
// codec ok fails unless the diag_status label is explicitly allowlisted.
func runDiagLabel(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	var tc typeMatrixCase
	found := false
	for _, c := range diagCases() {
		if c.Label == label {
			tc = c
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("unknown matrix label %q", label)
	}
	skipIfNoTypeInterface(t, ctx, domain, tc.RosType)
	liveFwd := helpers.LiveID("ros-fwd-" + label)
	topicFwd := helpers.LiveTopic(liveFwd)
	runDiagStrictForward(t, ctx, domain, tc, liveFwd, topicFwd)
	liveRev := helpers.LiveID("ros-rev-" + label)
	topicRev := helpers.LiveTopic(liveRev)
	runDiagStrictReverse(t, ctx, domain, tc, liveRev, topicRev)
}

// TestROSTypeDiagKeyValue checks KeyValue forward plus reverse.
//
// Forward starts ros2 topic pub for diagnostic_msgs KeyValue and decodes
// via NewDecoder on the Weld take path. Reverse publishes the live value
// via Weld and watches ros2 topic echo. The key stays fixed and the value
// carries the per-run live ID with per-run topic plus domain isolation.
func TestROSTypeDiagKeyValue(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runDiagLabel(t, ctx, domain, "diag-keyvalue")
}

// TestROSTypeDiagStatus checks DiagnosticStatus forward plus reverse.
//
// Forward starts ros2 topic pub for diagnostic_msgs DiagnosticStatus and
// decodes via NewDecoder on the Weld take path. Reverse publishes one
// status with a single KeyValue via Weld and watches ros2 topic echo.
// Name and nested value carry the per-run live ID so stale runs never
// match, while level, message, hardware, and key stay fixed.
func TestROSTypeDiagStatus(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runDiagLabel(t, ctx, domain, "diag-status")
}
