// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Writer QoS live gate: per-profile translation proven against CycloneDDS.
//
// Unit tests pin the exact SEDP bytes and the equivalence matrix; this gate
// proves the live directions still behave. Each subtest allocates one
// exclusive domain from the parallel pool with per-run topics and payloads,
// skips cleanly only when no peer is present or the ros2 QoS flags are
// unsupported, and fails loudly otherwise: a missing payload on a
// compatible pair is a flow failure, and delivery on an incompatible pair
// is a silence failure. The incompatible case pins the loud refusal
// through CheckWireCompatibility first, so live silence always pairs with
// a named policy in the error text.
//
// Rationale for the offer directions: weld pub announces the facade offer
// (reliable plus transient local) because per-profile writer plumbing is
// not yet wired into the backend publish path. A reliable offer satisfies
// a best effort request under the same RxO rule a best effort offer does,
// so the reverse live leg proves the direction flows while the inline
// CheckWireCompatibility leg proves the translated best effort offer
// itself is compatible. The volatile leg uses a topic exchange with the
// same durability wire value (kind 0) that service endpoints announce.
//
//nolint:dupl // live gate take helpers repeat similar wire checks across QoS gates
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

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Writer QoS live gate constants for Jazzy canonical.
//
// Each test allocates one exclusive domain from the parallel pool. The
// container name and Cyclone RMW come from helpers so the gate stays on
// ros-jazzy with RMW_IMPLEMENTATION=rmw_cyclonedds_cpp. Only one run per
// domain and topic at a time is supported, so tests never run in parallel
// and every payload carries a per-run live ID.
const (
	wqEchoWait     = 10 * time.Second
	wqPollWait     = 20 * time.Second
	wqForwardNap   = 5 * time.Second
	wqReverseNap   = 6 * time.Second
	wqTestTimeout  = 4 * time.Minute
	wqProbeWait    = 15 * time.Second
	wqCleanWait    = 10 * time.Second
	wqMismatchWait = 8 * time.Second
	wqPubRetry     = 5 * time.Second
	wqPubInterval  = 50 * time.Millisecond
	wqRepubWait    = 2 * time.Second
)

// wqReliability sets reliability from its flag name.
func wqReliability(name string, out *core.QoSProfile) error {
	switch name {
	case "reliable":
		out.Reliability = core.ReliabilityReliable
	case "best_effort":
		out.Reliability = core.ReliabilityBestEffort
	default:
		return fmt.Errorf("writerqos: unknown reliability %q", name)
	}

	return nil
}

// wqDurability sets durability from its flag name.
func wqDurability(name string, out *core.QoSProfile) error {
	switch name {
	case "transient_local":
		out.Durability = core.DurabilityTransientLocal
	case "volatile":
		out.Durability = core.DurabilityVolatile
	default:
		return fmt.Errorf("writerqos: unknown durability %q", name)
	}

	return nil
}

// wqProfile builds an explicit core profile with fixed history and liveliness.
//
// History stays keep last with depth 10 and liveliness stays automatic so
// only reliability and durability drive the verdicts.
func wqProfile(reliability, durability string) (core.QoSProfile, error) {
	var out core.QoSProfile

	err := wqReliability(reliability, &out)
	if err != nil {
		return core.QoSProfile{}, err
	}

	err = wqDurability(durability, &out)
	if err != nil {
		return core.QoSProfile{}, err
	}

	out.History = core.HistoryKeepLast
	out.Depth = 10
	out.Liveliness = core.LivelinessAutomatic

	return out, nil
}

// wqForwardOffered translates one offered profile to wire QoS, failing loudly.
//
// The loud refusal carries topic, endpoint pair, and policy kind, so an
// untranslatable offer never passes silently.
func wqForwardOffered(t *testing.T, topic, publisher, subscriber string, offered core.QoSProfile) dds.WireQoS {
	t.Helper()

	wire, err := dds.ForwardQoS(topic, publisher, subscriber, offered)
	if err != nil {
		t.Fatalf("ForwardQoS offered: %v", err)
	}

	return wire
}

// wqCheckWireCompatible asserts a wire gate report has no errors.
func wqCheckWireCompatible(t *testing.T, name string, report core.CompatibilityReport) {
	t.Helper()

	helpers.CheckQoSCompatible(t, name, report)
}

// wqCheckWireIncompatible asserts a wire gate report names the policy loudly.
func wqCheckWireIncompatible(t *testing.T, name string, report core.CompatibilityReport, offered, requested core.QoSProfile, publisher, subscriber string, wantPolicy core.PolicyKind, domain int, runID string) {
	t.Helper()

	helpers.CheckQoSIncompatible(t, name, report, offered, requested, publisher, subscriber, wantPolicy, "writerqos", domain, runID)
}

// wqBudgetedSkip records one allowlisted product skip with distro pin and expiry.
func wqBudgetedSkip(t *testing.T, reason string) {
	t.Helper()
	helpers.NoteProductSkip(t, t.Name(), reason)
}

// wqRequireQoSFlags skips the test when the peer lacks the ros2 QoS flags.
func wqRequireQoSFlags(t *testing.T, ctx context.Context, domain int) {
	t.Helper()

	probeCtx, cancel := context.WithTimeout(ctx, wqProbeWait)
	defer cancel()

	argv := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "pub", "--help")
	cmd := exec.CommandContext(probeCtx, "docker", argv...) //nolint:gosec // live harness probes ros2 help with fixed argv and no shell
	out, err := cmd.CombinedOutput()
	if err != nil {
		wqBudgetedSkip(t, fmt.Sprintf("peer ros-jazzy Jazzy writerqos pub help probe failed, distro pin Jazzy: %v", err))
	}

	if !strings.Contains(string(out), "--qos-reliability") || !strings.Contains(string(out), "--qos-durability") {
		wqBudgetedSkip(t, "peer ros-jazzy Jazzy lacks QoS flags, distro pin Jazzy, skipping writerqos gate")
	}
}

// wqEncodeString encodes one String payload as XCDR1 for Weld publish.
func wqEncodeString(t *testing.T, text string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := enc.WriteString(text)
	if err != nil {
		t.Fatalf("writerqos encode: %v", err)
	}
	return enc.Bytes()
}

// wqDecodeString decodes one String payload and checks the live payload.
func wqDecodeString(payload []byte, want string) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("writerqos decode header: %w", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("writerqos decode data: %w", err)
	}
	if got != want {
		return fmt.Errorf("writerqos data %q wants %q", got, want)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("writerqos finish: %w", err)
	}
	return nil
}

// wqTakeFailure classifies one TakeBytes error for the take loops.
func wqTakeFailure(t *testing.T, terr error) bool {
	t.Helper()
	if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
		return true
	}
	text := terr.Error()
	if strings.Contains(text, "not implemented") {
		t.Fatalf("writerqos take backend error: %v", terr)
	}
	if strings.Contains(text, "not wired") {
		t.Fatalf("writerqos take backend error: %v", terr)
	}
	return false
}

// wqTakeUntilLive takes until one sample decodes to the live payload.
func wqTakeUntilLive(t *testing.T, ctx context.Context, backend *dds.Backend, topic, payload string, timeout time.Duration) {
	t.Helper()
	takeCtx, takeCancel := context.WithTimeout(ctx, timeout)
	defer takeCancel()
	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if wqTakeFailure(t, terr) {
				break
			}
			continue
		}
		if msg.TopicName != "" && msg.TopicName != topic {
			continue
		}
		if wqDecodeString(msg.Payload, payload) != nil {
			continue
		}
		return
	}
	t.Fatalf("weld take did not see writerqos payload %q on %s after %v", payload, topic, timeout)
}

// wqAssertSilent takes for a bounded window and fails on live delivery.
func wqAssertSilent(t *testing.T, ctx context.Context, backend *dds.Backend, topic, payload string, timeout time.Duration) {
	t.Helper()
	takeCtx, takeCancel := context.WithTimeout(ctx, timeout)
	defer takeCancel()
	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if wqTakeFailure(t, terr) {
				break
			}
			continue
		}
		if msg.TopicName != "" && msg.TopicName != topic {
			continue
		}
		if wqDecodeString(msg.Payload, payload) == nil {
			t.Fatalf("incompatible pair delivered payload %q, want no flow", payload)
		}
	}
}

// wqSetup allocates one exclusive domain and skips without a peer or flags.
func wqSetup(t *testing.T) (context.Context, context.CancelFunc, int) {
	t.Helper()
	t.Cleanup(func() {
		helpers.AssertNoExcessSkips(t)
	})

	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), wqTestTimeout)
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	wqRequireQoSFlags(t, ctx, domain)

	return ctx, cancel, domain
}

// wqRunRosPub starts a ROS talker with explicit QoS flags and a per-run payload.
func wqRunRosPub(t *testing.T, ctx context.Context, domain int, pubRel, pubDur, payloadPrefix string) (string, string, string, *exec.Cmd) {
	t.Helper()

	live := helpers.LiveID("wq-" + payloadPrefix)
	topic := helpers.LiveTopic(live)
	payload := "hello wq " + live
	logPath := filepath.Join(t.TempDir(), "wq-talker-"+live+".log")
	logFile, err := os.Create(logPath) //nolint:gosec // test reads and writes controlled temp file path
	if err != nil {
		t.Fatalf("create talker log: %v", err)
	}

	t.Cleanup(func() {
		cerr := logFile.Close()
		if cerr != nil {
			t.Logf("close talker log: %v", cerr)
		}
	})

	pubArg := fmt.Sprintf("{data: %s}", payload)
	argv := []string{
		helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2",
		"--qos-reliability", pubRel, "--qos-durability", pubDur, "--qos-history", "keep_last", "--qos-depth", "10",
	}
	args := helpers.DockerArgs(helpers.Container, domain, argv...)
	talker := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with controlled test args
	talker.Stdout = logFile
	talker.Stderr = logFile

	err = talker.Start()
	if err != nil {
		t.Fatalf("start ROS talker: %v", err)
	}

	t.Cleanup(func() {
		helpers.StopCmd(talker)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), wqCleanWait)
		defer cleanupCancel()
		if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, live); err != nil {
			t.Errorf("stop peer proc: %v", err)
		}
	})

	return topic, payload, live, talker
}

// wqRunRosEcho starts a ROS echo reader with explicit QoS flags into a log file.
func wqRunRosEcho(t *testing.T, ctx context.Context, domain int, topic, echoRel, echoDur string) string {
	t.Helper()

	echoPath := filepath.Join(t.TempDir(), "wq-rosecho-"+strings.ReplaceAll(topic, "/", "_")+".log")
	echoFile, err := os.Create(echoPath) //nolint:gosec // test reads and writes controlled temp file path
	if err != nil {
		t.Fatalf("create ros echo log: %v", err)
	}

	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close ros echo log: %v", cerr)
		}
	})

	argv := []string{
		helpers.RosBin, "topic", "echo", topic, helpers.RosType,
		"--qos-reliability", echoRel, "--qos-durability", echoDur, "--qos-history", "keep_last", "--qos-depth", "10",
	}
	args := helpers.DockerArgs(helpers.Container, domain, argv...)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with controlled test args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile

	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("start ros2 topic echo: %v", err)
	}

	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), wqCleanWait)
		defer cleanupCancel()
		if err := helpers.KillAndWaitContainerProc(cleanupCtx, helpers.Container, topic); err != nil {
			t.Errorf("stop peer proc: %v", err)
		}
	})

	return echoPath
}

// TestWriterQoSLiveBestEffortOfferFlows proves the best effort offer direction flows.
//
// The inline leg translates a best effort volatile offer and a best effort
// volatile request to wire QoS and requires the compatibility gate to pass.
// The live leg runs weld pub to a best effort Cyclone echo and requires
// delivery: the facade announces reliable, which satisfies a best effort
// request under the same RxO rule a best effort offer does, so the
// direction flows while the inline leg pins the translated best effort
// offer itself.
func TestWriterQoSLiveBestEffortOfferFlows(t *testing.T) {
	ctx, cancel, domain := wqSetup(t)
	defer cancel()

	offered, err := wqProfile("best_effort", "volatile")
	if err != nil {
		t.Fatalf("best effort offer profile: %v", err)
	}

	requested, err := wqProfile("best_effort", "volatile")
	if err != nil {
		t.Fatalf("best effort request profile: %v", err)
	}

	topic := helpers.LiveTopic(helpers.LiveID("wq-best-effort"))
	report := dds.CheckWireCompatibility(
		wqForwardOffered(t, topic, "weld-pub", "ros-echo", offered),
		wqForwardOffered(t, topic, "weld-pub", "ros-echo", requested),
	)
	wqCheckWireCompatible(t, "best-effort-offer-flows", report)

	live := helpers.LiveID("wq-best-effort")
	liveTopic := helpers.LiveTopic(live)
	payload := "hello wq best effort " + live
	echoPath := wqRunRosEcho(t, ctx, domain, liveTopic, "best_effort", "volatile")
	helpers.Sleep(ctx, wqReverseNap)

	if ctx.Err() != nil {
		t.Fatalf("context ended before pub: %v", ctx.Err())
	}

	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, liveTopic, helpers.ForwardStringHash, wqProbeWait, "writerqos")
	if err != nil {
		t.Fatalf("hash unavailable for topic %s: %v", liveTopic, err)
	}
	t.Logf("writerqos reverse topic %s hash %s", liveTopic, typeHash)
	encoded := wqEncodeString(t, payload)
	err = wqDecodeString(encoded, payload)
	if err != nil {
		t.Fatalf("local codec: %v", err)
	}
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "wq-best-effort-"+live, "writerqos") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	msg := transports.OutgoingMessage{
		TopicName: liveTopic,
		TypeName:  helpers.ForwardStringIDL,
		Payload:   encoded,
	}
	if !helpers.PublishUntilEcho(ctx, t, backend, msg, echoPath, payload, typeHash, wqPollWait, wqRepubWait, wqPubRetry, wqPubInterval, "writerqos-best-effort") {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // test reads controlled temp file path
		if rerr != nil {
			t.Fatalf("ros echo missed live payload %q: %v", payload, rerr)
		}

		t.Fatalf("ros echo missed live payload %q, want flow for compatible pair\necho output:\n%s", payload, string(data))
	}
}

// TestWriterQoSLiveVolatileServiceEquivalent proves the volatile exchange matches.
//
// The inline leg translates the resolved services default offer and a
// volatile request to wire QoS, requires volatile durability on the wire,
// and requires the compatibility gate to pass. The live leg runs a ROS
// volatile talker to weld echo with volatile durability and requires
// delivery with the same wire durability value service endpoints announce.
func TestWriterQoSLiveVolatileServiceEquivalent(t *testing.T) {
	ctx, cancel, domain := wqSetup(t)
	defer cancel()

	offered := core.ResolveSystemDefaults(core.ServicesDefaultQoS())
	requested, err := wqProfile("reliable", "volatile")
	if err != nil {
		t.Fatalf("volatile request profile: %v", err)
	}

	topic := helpers.LiveTopic(helpers.LiveID("wq-volatile"))
	wireOffered := wqForwardOffered(t, topic, "ros-pub", "weld-echo", offered)
	if wireOffered.DurabilityKind != 0 {
		t.Fatalf("services default wire durability = %d, want volatile 0", wireOffered.DurabilityKind)
	}

	report := dds.CheckWireCompatibility(wireOffered, wqForwardOffered(t, topic, "ros-pub", "weld-echo", requested))
	wqCheckWireCompatible(t, "volatile-service-equivalent", report)

	liveTopic, payload, _, _ := wqRunRosPub(t, ctx, domain, "reliable", "volatile", "volatile")
	helpers.Sleep(ctx, wqForwardNap)

	if ctx.Err() != nil {
		t.Fatalf("context ended before take: %v", ctx.Err())
	}

	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, liveTopic, helpers.ForwardStringHash, wqProbeWait, "writerqos")
	if err != nil {
		t.Fatalf("hash unavailable for topic %s: %v", liveTopic, err)
	}
	t.Logf("writerqos forward topic %s hash %s", liveTopic, typeHash)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "wq-volatile-live", "writerqos") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	err = helpers.SubscribeLiveReader(ctx, backend, liveTopic, helpers.ForwardStringIDL, typeHash, "writerqos")
	if err != nil {
		t.Fatalf("subscribe %s: %v", liveTopic, err)
	}
	wqTakeUntilLive(t, ctx, backend, liveTopic, payload, wqEchoWait)
}

// TestWriterQoSLiveIncompatibleRefusedLoudly proves the blocking direction is refused loudly.
//
// The inline leg translates a best effort offer and a reliable request to
// wire QoS and requires the compatibility gate to fail with the
// reliability policy named. The live leg runs a ROS best effort talker to
// weld echo with a reliable requester and requires silence on the live ID:
// delivery would mean the incompatible pair flowed.
func TestWriterQoSLiveIncompatibleRefusedLoudly(t *testing.T) {
	ctx, cancel, domain := wqSetup(t)
	defer cancel()

	offered, err := wqProfile("best_effort", "transient_local")
	if err != nil {
		t.Fatalf("best effort offer profile: %v", err)
	}

	requested := core.FacadeAdvertisedQoS()
	mismatchLive := helpers.LiveID("wq-mismatch")
	topic := helpers.LiveTopic(mismatchLive)
	report := dds.CheckWireCompatibility(
		wqForwardOffered(t, topic, "ros-pub", "weld-echo", offered),
		wqForwardOffered(t, topic, "ros-pub", "weld-echo", requested),
	)
	wqCheckWireIncompatible(t, "reliable-requester-vs-best-effort-offer", report, offered, requested, "ros-pub", "weld-echo", core.PolicyReliability, domain, mismatchLive)

	subCtx, subCancel := context.WithTimeout(ctx, wqMismatchWait+2*wqEchoWait)
	defer subCancel()

	liveTopic, payload, live, talker := wqRunRosPub(t, subCtx, domain, "best_effort", "transient_local", "mismatch")
	helpers.Sleep(subCtx, wqForwardNap)

	if subCtx.Err() != nil {
		t.Fatalf("context ended before take: %v", subCtx.Err())
	}

	if talker.Process == nil {
		t.Fatalf("ROS talker missing process, cannot judge silence as refusal")
	}
	if talker.ProcessState != nil && talker.ProcessState.Exited() {
		t.Fatalf("ROS talker exited, cannot judge silence as refusal")
	}
	typeHash, err := helpers.ResolveTopicHash(subCtx, helpers.Container, domain, helpers.RosBin, liveTopic, helpers.ForwardStringHash, wqProbeWait, "writerqos")
	if err != nil {
		t.Fatalf("hash unavailable for topic %s: %v", liveTopic, err)
	}
	t.Logf("writerqos mismatch topic %s hash %s", liveTopic, typeHash)
	backend := helpers.MustOpenLiveBackend(t, subCtx, uint32(domain), "wq-mismatch-"+live, "writerqos") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	err = helpers.SubscribeLiveReader(subCtx, backend, liveTopic, helpers.ForwardStringIDL, typeHash, "writerqos")
	if err != nil {
		t.Fatalf("subscribe %s: %v", liveTopic, err)
	}
	probe := helpers.MustOpenLiveBackend(t, subCtx, uint32(domain), "wq-probe-"+live, "wq-probe") //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	probeErr := probe.Subscribe(subCtx, liveTopic, helpers.ForwardStringIDL, helpers.ForwardStringHash)
	if probeErr != nil {
		t.Fatalf("probe subscribe for SEDP visibility: %v", probeErr)
	}
	helpers.WaitForSEDPRemote(t, subCtx, probe, liveTopic, "ROS talker writer", helpers.SEDPRemoteTimeout)
	wqAssertSilent(t, subCtx, backend, liveTopic, payload, wqMismatchWait)

	t.Logf("incompatible pair stayed silent on live ID as required, reliability refusal is loud above")
}
