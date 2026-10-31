// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"encoding/hex"
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

// Keyed live gate constants.
//
// Each test allocates one exclusive domain from the parallel pool, so
// per-run topics plus the allocated domain isolate runs. Timeouts bound
// every docker and backend call through context. Standard ROS message
// types are normally unkeyed, so the forward talker publishes stock
// std_msgs/String and the gate asserts the keyed path handles live vendor
// DATA without drops: empty key extraction plus the pinned handle golden
// on live bytes. True vendor key-flagged DATA (Key flag plus KeyHash
// inline QoS plus keyed SEDP announcements) is pinned in unit tests with
// crafted RTPS in internal/transports/dds/keyed_test.go; the live gate
// proves the same helpers on peer bytes and proves Weld emissions stay
// visible to the peer. A missing payload with a healthy peer fails
// loudly, never skips.
const (
	keyedLiveTestTimeout = 2 * time.Minute
	keyedLiveTalkerNap   = 5 * time.Second
	keyedLiveEchoNap     = 6 * time.Second
	keyedLiveTakeWait    = 45 * time.Second
	keyedLivePollWait    = 30 * time.Second
	keyedLiveProbeWait   = 5 * time.Second
	keyedLiveCleanWait   = 10 * time.Second
	keyedLivePubRetry    = 5 * time.Second
	keyedLivePubInterval = 50 * time.Millisecond
	keyedLiveRepubWait   = 2 * time.Second
)

// keyedLiveStringHandleHex is the InstanceHandle golden for the unkeyed
// std_msgs/String type with empty key bytes.
//
// It mirrors the unit golden in internal/transports/dds/keyed_test.go:
// SHA-256 over "std_msgs__msg__String" plus one 0x00 separator plus no
// key bytes, truncated to 16 bytes. The live gate recomputes it from
// peer bytes so a keyed path regression on the wire fails here.
const keyedLiveStringHandleHex = "5e2429d5b77f69e8d2f9da1a6e302165"

// keyedLiveRequirePeer skips when no peer or no String type is present.
//
// It allows only SkipIfNoPeer and SkipIfNoTypeInterface. A missing
// payload with a healthy peer must fail, never skip.
func keyedLiveRequirePeer(t *testing.T, ctx context.Context, domain int) {
	t.Helper()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, helpers.RosType, keyedLiveProbeWait)
}

// keyedLiveStringMembers builds the unkeyed String descriptor for key extraction.
//
// Standard std_msgs/String carries no key fields, so extraction over live
// vendor bytes must return empty. The descriptor is built locally and
// never registered.
func keyedLiveStringMembers() *types.MessageMembers {
	members := []types.MessageMember{
		types.NewStringMember("data", types.RosTypeString, 0, 0),
	}
	return types.NewMessageMembers("std_msgs", "String", 8, members, func(_ any, _ types.MessageInit) error { return nil }, func(_ any) error { return nil })
}

// keyedLiveDecodeString decodes one String payload and checks the live ID.
func keyedLiveDecodeString(payload []byte, live string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("keyed live decode header: %w", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("keyed live decode data: %w", err)
	}
	if got != live {
		return fmt.Errorf("keyed live data %q wants %q", got, live)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("keyed live finish: %w", err)
	}
	return nil
}

// keyedLiveEncodeString encodes the live ID as XCDR1 String for Weld pub.
func keyedLiveEncodeString(t *testing.T, live string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := enc.WriteString(live)
	if err != nil {
		t.Fatalf("keyed live encode: %v", err)
	}
	return enc.Bytes()
}

// keyedLiveAssertUnkeyedHandle asserts empty key extraction plus the handle golden.
func keyedLiveAssertUnkeyedHandle(t *testing.T, payload []byte) {
	t.Helper()
	key, err := dds.ExtractKeyBytes(payload, keyedLiveStringMembers())
	if err != nil {
		t.Fatalf("keyed live extract on vendor bytes: %v", err)
	}
	if len(key) != 0 {
		t.Fatalf("keyed live key length = %d, want empty for unkeyed String", len(key))
	}
	handle := dds.InstanceHandle(helpers.ForwardStringIDL, key)
	if got := hex.EncodeToString(handle[:]); got != keyedLiveStringHandleHex {
		t.Fatalf("keyed live handle = %s, want %s", got, keyedLiveStringHandleHex)
	}
}

// keyedLiveStartTalker starts ros2 topic pub for the live ID and returns stoppers.
func keyedLiveStartTalker(t *testing.T, ctx context.Context, domain int, live, topic string) (stop func()) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "keyed-talker-"+live+".log")
	logFile, err := os.Create(logPath) //nolint:gosec // test writes controlled temp path
	if err != nil {
		t.Fatalf("keyed live create talker log: %v", err)
	}
	t.Cleanup(func() {
		cerr := logFile.Close()
		if cerr != nil {
			t.Logf("close keyed talker log: %v", cerr)
		}
	})
	yaml := "{data: " + live + "}"
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", topic, helpers.RosType, yaml, "--rate", "2", "--qos-reliability", "reliable", "--qos-durability", "transient_local", "--qos-history", "keep_last", "--qos-depth", "10")
	talker := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // test harness runs docker with controlled args
	talker.Stdout = logFile
	talker.Stderr = logFile
	err = talker.Start()
	if err != nil {
		t.Fatalf("keyed live start talker: %v", err)
	}
	stop = func() {
		helpers.StopCmd(talker)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), keyedLiveCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
	}
	t.Cleanup(stop)
	return stop
}

// TestKeyedLiveForwardVendorData checks Weld ingests vendor DATA without keyed drops.
//
// It starts a ROS talker on a per-run topic, probes the RIHS01 hash with
// static fallback, subscribes via the Weld backend, and takes until one
// sample decodes to the live ID. It then runs the keyed helpers over the
// live vendor bytes: extraction must be empty for the unkeyed String type
// and the instance handle must match the pinned golden. A missing payload
// with a healthy peer fails loudly with the take summary.
func TestKeyedLiveForwardVendorData(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), keyedLiveTestTimeout)
	defer cancel()
	keyedLiveRequirePeer(t, ctx, domain)
	live := helpers.LiveID("keyed-fwd")
	topic := helpers.LiveTopic(live)
	stopTalker := keyedLiveStartTalker(t, ctx, domain, live, topic)
	helpers.Sleep(ctx, keyedLiveTalkerNap)
	if ctx.Err() != nil {
		t.Fatalf("keyed live context ended before take: %v", ctx.Err())
	}
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, helpers.ForwardStringHash, keyedLiveProbeWait, "keyed-fwd")
	if err != nil {
		t.Fatalf("keyed live hash unavailable for topic %s: %v", topic, err)
	}
	t.Logf("keyed live forward topic %s hash %s", topic, typeHash)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "keyed-live-forward-"+live, "keyed-fwd") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	closeBackend := func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("close keyed backend: %v", cerr)
		}
	}
	t.Cleanup(closeBackend)
	err = helpers.SubscribeLiveReader(ctx, backend, topic, helpers.ForwardStringIDL, typeHash, "keyed-fwd")
	if err != nil {
		t.Fatalf("keyed live subscribe %s: %v", topic, err)
	}
	payload := keyedLiveTakeVendorPayload(t, ctx, backend, topic, live, keyedLiveTakeWait)
	keyedLiveAssertUnkeyedHandle(t, payload)
	closeBackend()
	stopTalker()
}

// keyedLiveTakeFailure classifies one TakeBytes error.
//
// Backend gap errors fail loudly. Context end reports done true so the
// caller breaks the take loop. Any other per-sample drop reports done
// false so the caller keeps waiting for the live payload.
func keyedLiveTakeFailure(t *testing.T, terr error) bool {
	t.Helper()
	if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
		return true
	}
	text := terr.Error()
	if strings.Contains(text, "not implemented") {
		t.Fatalf("keyed live take backend error: %v", terr)
	}
	if strings.Contains(text, "not wired") {
		t.Fatalf("keyed live take backend error: %v", terr)
	}
	return false
}

// keyedLiveTakeVendorPayload takes until one sample decodes to the live ID.
//
// It skips foreign topics, undecodable samples, and backend gap errors the
// same way the matrix gates do, and fails loudly with the take summary
// when the timeout ends without the live payload.
func keyedLiveTakeVendorPayload(t *testing.T, ctx context.Context, backend *dds.Backend, topic, live string, timeout time.Duration) []byte {
	t.Helper()
	takeCtx, takeCancel := context.WithTimeout(ctx, timeout)
	defer takeCancel()
	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if keyedLiveTakeFailure(t, terr) {
				break
			}
			continue
		}
		if msg.TopicName != "" && msg.TopicName != topic {
			continue
		}
		if keyedLiveDecodeString(msg.Payload, live) != nil {
			continue
		}
		return msg.Payload
	}
	t.Fatalf("weld take did not see keyed live payload %q on %s after %v, drops suspected", live, topic, timeout)
	return nil
}

// TestKeyedLiveReversePeerVisible checks ros2 topic echo sees a Weld pub.
//
// It starts echo on a per-run topic, publishes the live ID via the Weld
// backend with the publish until echo retry, and fails loudly with the
// echo output when the peer never sees it. It then proves the keyed
// helpers on a synthetic keyed sample carrying the same live ID:
// extract plus handle round trip determinism, KeyHash param encode plus
// parse, and StatusInfo param encode plus parse. Weld keyed emission with
// the Key flag plus KeyHash inline QoS needs the backend keyed publish
// hook, so reverse covers standard DATA visibility plus local keyed wire
// proof.
func TestKeyedLiveReversePeerVisible(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), keyedLiveTestTimeout)
	defer cancel()
	keyedLiveRequirePeer(t, ctx, domain)
	live := helpers.LiveID("keyed-rev")
	topic := helpers.LiveTopic(live)
	echoPath := filepath.Join(t.TempDir(), "keyedecho-"+live+".log")
	echoFile, err := os.Create(echoPath) //nolint:gosec // test writes controlled temp path
	if err != nil {
		t.Fatalf("keyed live create echo log: %v", err)
	}
	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close keyed echo log: %v", cerr)
		}
	})
	args := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", topic, helpers.RosType)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // test harness runs docker with controlled args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("keyed live start echo: %v", err)
	}
	stopEcho := func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), keyedLiveCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
	}
	t.Cleanup(stopEcho)
	helpers.Sleep(ctx, keyedLiveEchoNap)
	if ctx.Err() != nil {
		t.Fatalf("keyed live context ended before pub: %v", ctx.Err())
	}
	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic, helpers.ForwardStringHash, keyedLiveProbeWait, "keyed-rev")
	if err != nil {
		t.Fatalf("keyed live hash unavailable for topic %s: %v", topic, err)
	}
	t.Logf("keyed live reverse topic %s hash %s", topic, typeHash)
	payload := keyedLiveEncodeString(t, live)
	err = keyedLiveDecodeString(payload, live)
	if err != nil {
		t.Fatalf("keyed live local codec: %v", err)
	}
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "keyed-live-reverse-"+live, "keyed-rev") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	closeBackend := func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("close keyed backend: %v", cerr)
		}
	}
	t.Cleanup(closeBackend)
	msg := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  helpers.ForwardStringIDL,
		Payload:   payload,
	}
	if !helpers.PublishUntilEcho(ctx, t, backend, msg, echoPath, live, typeHash, keyedLivePollWait, keyedLiveRepubWait, keyedLivePubRetry, keyedLivePubInterval, "keyed-rev") {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads controlled temp path
		if rerr != nil {
			t.Fatalf("ros2 topic echo did not see keyed live payload %q: %v", live, rerr)
		}
		closeBackend()
		stopEcho()
		t.Fatalf("ros2 topic echo did not see keyed live payload %q hash %s\necho output:\n%s", live, typeHash, string(data))
	}
	keyedLiveAssertKeyedWire(t, live)
	closeBackend()
	stopEcho()
}

// keyedLiveEncodeKeyedSample encodes the synthetic keyed shape with the live ID.
//
// Layout mirrors the keydefs flat shape: uint32 seq key, string label
// carrying the live ID, int32 count key. The peer never sees this type;
// it proves the keyed helpers on live adjacent bytes.
func keyedLiveEncodeKeyedSample(t *testing.T, live string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	enc.WriteUint32(0xA11CE)
	err := enc.WriteString(live)
	if err != nil {
		t.Fatalf("keyed live encode sample label: %v", err)
	}
	enc.WriteInt32(-7)
	return enc.Bytes()
}

// keyedLiveAssertKeyedWire proves the keyed helpers on a live ID bearing sample.
func keyedLiveAssertKeyedWire(t *testing.T, live string) {
	t.Helper()
	sample := keyedLiveEncodeKeyedSample(t, live)
	first, err := dds.InstanceHandleForPayload("weld_test__msg__Keyed", sample, types.KeyedTestMembers())
	if err != nil {
		t.Fatalf("keyed live handle for sample: %v", err)
	}
	second, err := dds.InstanceHandleForPayload("weld_test__msg__Keyed", sample, types.KeyedTestMembers())
	if err != nil {
		t.Fatalf("keyed live handle for sample again: %v", err)
	}
	if first != second {
		t.Fatalf("keyed live handle is not deterministic")
	}
	param := dds.EncodeKeyHashParam(first)
	got, err := dds.ParseKeyHashValue(param.Value)
	if err != nil {
		t.Fatalf("keyed live keyhash parse: %v", err)
	}
	if got != first {
		t.Fatalf("keyed live keyhash = %x, want %x", got, first)
	}
	statusParam := dds.EncodeStatusInfoParam(dds.StatusInfoDisposedFlag, true)
	status, err := dds.ParseStatusInfoValue(statusParam.Value, true)
	if err != nil {
		t.Fatalf("keyed live status parse: %v", err)
	}
	if status != dds.StatusInfoDisposedFlag {
		t.Fatalf("keyed live status = %#x, want disposed flag", status)
	}
}
