// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:dupl // live gate decoders repeat similar wire checks across types
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

// Builtin Time and Duration live gate constants.
//
// Domain 42 is reused through the shared matrix runners so builtin types
// ride the same standalone first contact domain as the seven type matrix.
// Hashes were captured from weld-jazzy Jazzy via ros2 topic info -v on
// domain 42. Sentinel uses fixed sec plus nanosec with per-run topic plus
// domain isolation because Time and Duration carry no string field.
const (
	builtinTimeHash            = "RIHS01_b106235e25a4c5ed35098aa0a61a3ee9c9b18d197f398b0e4206cea9acf9c197"
	builtinDurationHash        = "RIHS01_e8d009f659816f758b75334ee1a9ca5b5c0b859843261f14c7f937349599d93b"
	builtinSec          int32  = 42
	builtinNanosec      uint32 = 123
)

// builtinGapEntry pins one explicit live gap with hash plus reason plus expiry.
//
// Hash is the expected RIHS01 type hash. Reason names the peer interop
// cause. Expires bounds the allowance so stale gaps fail loudly after the
// date.
type builtinGapEntry struct {
	Hash    string
	Reason  string
	Expires time.Time
}

// builtinGapAllowlist holds explicit live gaps for builtin_time_zero and TimeMax.
//
// Keys use matrix labels builtin-time-zero and builtin-time-max. Both share
// builtinTimeHash because the wire layout is identical and only sentinel
// values change. Base labels builtin-time and builtin-duration are not
// listed so live missing with local codec ok fails instead of skipping.
var builtinGapAllowlist = map[string]builtinGapEntry{
	"builtin-time-zero": {
		Hash:    builtinTimeHash,
		Reason:  "peer CycloneDDS omits zero Time sample on pooled domain with local codec ok",
		Expires: time.Date(2027, 3, 31, 0, 0, 0, 0, time.UTC),
	},
	"builtin-time-max": {
		Hash:    builtinTimeHash,
		Reason:  "peer CycloneDDS omits TimeMax sample on pooled domain with local codec ok",
		Expires: time.Date(2027, 3, 31, 0, 0, 0, 0, time.UTC),
	},
}

// gapFailOrAllow skips only for an explicit allowlist gap, else fails.
//
// It checks the looked-up entry with matching hash and unexpired date. On
// match it skips with hash plus reason plus expiry. Otherwise it fails
// because local codec ok but live missing has no peer reason.
func gapFailOrAllow(t *testing.T, label, hash, detail, entryHash, entryReason string, expires time.Time, ok bool) {
	t.Helper()
	if ok && entryHash == hash && time.Now().Before(expires) && helpers.IsValidRIHSHash(entryHash) {
		t.Skipf("allowlisted live gap %s hash %s reason %s expires %s: %s", label, entryHash, entryReason, expires.Format("2006-01-02"), detail)
	}
	if ok && !time.Now().Before(expires) {
		t.Fatalf("expired allowlist %s hash %s reason %s expired %s: %s", label, entryHash, entryReason, expires.Format("2006-01-02"), detail)
	}
	t.Fatalf("live missing %s hash %s with local codec ok, no peer reason and no allowlist: %s", label, hash, detail)
}

// builtinFailOrAllow skips only for explicit allowlist gaps, else fails.
//
// It checks label in builtinGapAllowlist with matching hash and unexpired
// date. On match it skips with hash plus reason plus expiry. Otherwise it
// fails because local codec ok but live missing has no peer reason.
func builtinFailOrAllow(t *testing.T, label, hash, detail string) {
	t.Helper()
	entry, ok := builtinGapAllowlist[label]
	gapFailOrAllow(t, label, hash, detail, entry.Hash, entry.Reason, entry.Expires, ok)
}

// runBuiltinStrictForward checks Weld take sees a ROS talker with strict gaps.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, topic
// hash probing, and the Weld backend. It mirrors runMatrixForward for domain and argv but fails when live is
// missing with local codec ok unless the label is explicitly allowlisted
// with hash plus reason plus expiry. Peer absence still skips via
// SkipIfNoPeer and SkipIfNoTypeInterface only.
//
//nolint:gocognit,cyclop // live forward path asserts identity, hash, codec, delivery in one run
func runBuiltinStrictForward(t *testing.T, ctx context.Context, domain int, tc typeMatrixCase, live, topic string) {
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
		if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, topic); err != nil {
			t.Errorf("stop peer proc: %v", err)
		}
	}
	t.Cleanup(stopTalker)
	helpers.Sleep(ctx, typeMatrixTalkerNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before take %s: %v", tc.Label, ctx.Err())
	}
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, tc.staticHash, typeMatrixProbeWait, "matrix")
	if err != nil {
		builtinFailOrAllow(t, tc.Label, tc.staticHash, fmt.Sprintf("hash unavailable for %s topic %s: %v", tc.Label, topic, err))
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
	builtinFailOrAllow(t, tc.Label, typeHash, fmt.Sprintf("weld take did not see %s payload", tc.Label))
}

// builtinPublishUntilEcho publishes until ros2 topic echo sees want.
//
// It retries with PublishLiveWithRetry and polls the echo file. A publish
// without routes fails via allowlist or fatal so no generic skip remains.
// It reports true when the echo holds want.
func builtinPublishUntilEcho(ctx context.Context, t *testing.T, backend *dds.Backend, msg transports.OutgoingMessage, echoPath, want, typeHash, label string) bool {
	t.Helper()
	deadline := time.Now().Add(typeMatrixPollWait)
	for {
		err := helpers.PublishLiveWithRetry(ctx, backend, msg, typeMatrixPubRetry, typeMatrixPubInterval, label)
		if err != nil {
			if strings.Contains(err.Error(), "no route") {
				builtinFailOrAllow(t, label, typeHash, fmt.Sprintf("weld pub has no route: %v", err))
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

// runBuiltinStrictReverse checks ros2 topic echo sees a Weld pub with strict gaps.
//
// Domain is the allocated exclusive domain plumbed to DockerArgs, topic
// hash probing, and the Weld backend. It mirrors runMatrixReverse for domain and argv but fails when echo is
// missing with local codec ok unless the label is explicitly allowlisted
// with hash plus reason plus expiry.
//
//nolint:cyclop // live reverse gate sequences echo, probe, and allowlist verdict in one run
func runBuiltinStrictReverse(t *testing.T, ctx context.Context, domain int, tc typeMatrixCase, live, topic string) {
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
		if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, topic); err != nil {
			t.Errorf("stop peer proc: %v", err)
		}
	}
	t.Cleanup(stopEcho)
	helpers.Sleep(ctx, typeMatrixEchoNap)
	if ctx.Err() != nil {
		t.Fatalf("context ended before pub %s: %v", tc.Label, ctx.Err())
	}
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, tc.staticHash, typeMatrixProbeWait, "matrix")
	if err != nil {
		builtinFailOrAllow(t, tc.Label, tc.staticHash, fmt.Sprintf("hash unavailable for %s topic %s: %v", tc.Label, topic, err))
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
	if builtinPublishUntilEcho(ctx, t, backend, msg, echoPath, want, typeHash, tc.Label) {
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
	builtinFailOrAllow(t, tc.Label, typeHash, fmt.Sprintf("ros2 topic echo did not see %s payload %q echo output: %s", tc.Label, want, string(data)))
}

// yamlBuiltinTime returns ROS YAML for the Time sentinel.
func yamlBuiltinTime(live string) string {
	return "{sec: 42, nanosec: 123}"
}

// yamlBuiltinDuration returns ROS YAML for the Duration sentinel.
func yamlBuiltinDuration(live string) string {
	return "{sec: 42, nanosec: 123}"
}

// encodeBuiltinPair encodes sec plus nanosec as XCDR1.
func encodeBuiltinPair() []byte {
	enc := types.NewEncoder(true)
	enc.WriteInt32(builtinSec)
	enc.WriteUint32(builtinNanosec)
	return enc.Bytes()
}

// encodeBuiltinTime encodes the fixed Time sentinel as XCDR1.
func encodeBuiltinTime(live string) ([]byte, error) {
	return encodeBuiltinPair(), nil
}

// encodeBuiltinDuration encodes the fixed Duration sentinel as XCDR1.
func encodeBuiltinDuration(live string) ([]byte, error) {
	return encodeBuiltinPair(), nil
}

// decodeBuiltinPair decodes and checks sec plus nanosec with label.
func decodeBuiltinPair(payload []byte, label string) error {
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
	if sec != builtinSec || nanosec != builtinNanosec {
		return fmt.Errorf("%s (%d, %d) wants (%d, %d)", label, sec, nanosec, builtinSec, builtinNanosec)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("finish %s: %w", label, err)
	}
	return nil
}

// decodeBuiltinTime decodes and checks the Time sentinel.
func decodeBuiltinTime(payload []byte, live string) error {
	err := decodeBuiltinPair(payload, "time")
	if err != nil {
		return fmt.Errorf("decode time pair: %w", err)
	}
	return nil
}

// decodeBuiltinDuration decodes and checks the Duration sentinel.
func decodeBuiltinDuration(payload []byte, live string) error {
	err := decodeBuiltinPair(payload, "duration")
	if err != nil {
		return fmt.Errorf("decode duration pair: %w", err)
	}
	return nil
}

// wantBuiltinTime returns the echo substring for the Time sentinel.
func wantBuiltinTime(live string) string {
	return fmt.Sprintf("sec: %d", builtinSec)
}

// wantBuiltinDuration returns the echo substring for the Duration sentinel.
func wantBuiltinDuration(live string) string {
	return fmt.Sprintf("sec: %d", builtinSec)
}

// builtinTimeCase builds the matrix case for builtin Time.
func builtinTimeCase() typeMatrixCase {
	return typeMatrixCase{
		Label:      "builtin-time",
		RosType:    "builtin_interfaces/msg/Time",
		IDLName:    "builtin_interfaces__msg__Time",
		staticHash: builtinTimeHash,
		yaml:       yamlBuiltinTime,
		encode:     encodeBuiltinTime,
		decode:     decodeBuiltinTime,
		want:       wantBuiltinTime,
	}
}

// builtinDurationCase builds the matrix case for builtin Duration.
func builtinDurationCase() typeMatrixCase {
	return typeMatrixCase{
		Label:      "builtin-duration",
		RosType:    "builtin_interfaces/msg/Duration",
		IDLName:    "builtin_interfaces__msg__Duration",
		staticHash: builtinDurationHash,
		yaml:       yamlBuiltinDuration,
		encode:     encodeBuiltinDuration,
		decode:     decodeBuiltinDuration,
		want:       wantBuiltinDuration,
	}
}

// runBuiltinCase runs forward plus reverse for one builtin case.
//
// Domain is the allocated exclusive domain shared by both directions.
// It skips when the peer lacks the ROS type, then runs forward with one
// live ID and reverse with another so the two directions never share a
// topic. It uses strict runners so docker argv, domain 42, timeouts, SEDP
// hash wiring, and XCDR checks stay identical while live missing with
// local codec ok fails unless allowlisted.
func runBuiltinCase(t *testing.T, ctx context.Context, domain int, tc typeMatrixCase) {
	t.Helper()
	skipIfNoTypeInterface(t, ctx, domain, tc.RosType)
	liveFwd := helpers.LiveID("ros-fwd-" + tc.Label)
	topicFwd := helpers.LiveTopic(liveFwd)
	runBuiltinStrictForward(t, ctx, domain, tc, liveFwd, topicFwd)
	liveRev := helpers.LiveID("ros-rev-" + tc.Label)
	topicRev := helpers.LiveTopic(liveRev)
	runBuiltinStrictReverse(t, ctx, domain, tc, liveRev, topicRev)
}

// TestROSTypeBuiltinTime checks Time forward plus reverse.
//
// Forward starts ros2 topic pub for builtin Time and decodes via NewDecoder
// on the Weld take path with explicit SEDP hash. Reverse publishes the fixed
// sentinel via Weld and watches ros2 topic echo. The sentinel uses per-run
// topic plus domain isolation because Time carries no string field.
func TestROSTypeBuiltinTime(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runBuiltinCase(t, ctx, domain, builtinTimeCase())
}

// TestROSTypeBuiltinDuration checks Duration forward plus reverse.
//
// It mirrors the Time case with the same fixed sentinel layout and the same
// topic plus domain isolation.
func TestROSTypeBuiltinDuration(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runBuiltinCase(t, ctx, domain, builtinDurationCase())
}
