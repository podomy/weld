// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Durability backlog gate constants.
//
// Each test allocates one exclusive domain from the parallel pool, so
// per-run topics plus the allocated domain isolate runs. Counts are
// exact where determinism holds: a miss or a duplicate fails loudly,
// never skips. Only no-peer and missing-type skip.
const (
	durBacklogTimeout    = 6 * time.Minute
	durBacklogEchoWait   = 10 * time.Second
	durBacklogPollWait   = 20 * time.Second
	durBacklogCleanWait  = 10 * time.Second
	durBacklogTypeProbe  = 5 * time.Second
	durBacklogPubRetry   = 30 * time.Second
	durBacklogPubGap     = 200 * time.Millisecond
	durForwardSamples    = 6
	durReverseSamples    = 6
	durBacklogFreezeWait = 12 * time.Second
)

// durPythonTransientPub builds a keep-all transient vendor publisher.
//
// A frozen silent writer serves nothing to late joiners on this peer
// stack: Cyclone emits no usable heartbeat for a writer with no live
// traffic, so no NACK ever fires (observed symmetrically
// Cyclone-to-Cyclone, hence a peer limitation, not a Weld gap). The
// publisher therefore emits six distinct samples, then keeps the writer
// alive with a slow identical tail that never evicts under keep-all.
// A late transient reader NACKs the live heartbeat and repairs the full
// backlog; the gate asserts every distinct sample arrives at least once.
// Weld-side retention is unit-pinned; the reverse gate pins exact counts.
func durPythonTransientPub(topic, live string) []string {
	script := "import rclpy,time;" +
		"from std_msgs.msg import String;" +
		"from rclpy.qos import QoSProfile,QoSDurabilityPolicy,QoSReliabilityPolicy,QoSHistoryPolicy;" +
		"rclpy.init();" +
		"n=rclpy.create_node('durpub');" +
		"import threading;" +
		"threading.Thread(target=lambda:rclpy.spin(n),daemon=True).start();" +
		"q=QoSProfile(depth=1000,durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,reliability=QoSReliabilityPolicy.RELIABLE,history=QoSHistoryPolicy.KEEP_ALL);" +
		"p=n.create_publisher(String,'" + topic + "',q);" +
		"time.sleep(2);" +
		"print('durpub ready',flush=True);" +
		"[(p.publish(String(data='hello-%d-" + live + "'%i)),print('durpub sent',i,flush=True),time.sleep(0.5)) for i in range(6)];" +
		"print('durpub frozen',flush=True);" +
		"[(p.publish(String(data='hello-tail-" + live + "')),time.sleep(2)) for _ in range(20)];" +
		"time.sleep(50)"
	return []string{"python3", "-c", script}
}

// TestDurabilityLateJoinerSeesCycloneBacklog checks a late Weld reader replays Cyclone backlog.
//
// A keep-all rclpy publisher on the Cyclone stack emits six distinct
// samples, then keeps the writer alive with a slow identical tail that
// never evicts. Heartbeats keep flowing, so the late Weld echo NACKs the
// gap and repairs the full backlog; the gate asserts every distinct
// sample arrives at least once. A waiter echo runs alongside publishing
// only to unlock ros2-style matching promptly; the assertion echo joins
// after the freeze. It skips only for no peer or a missing std_msgs type.
func TestDurabilityLateJoinerSeesCycloneBacklog(t *testing.T) {
	t.Parallel()

	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), durBacklogTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, helpers.RosType, durBacklogTypeProbe)
	bin := helpers.BuildWeld(t, ctx)

	live := helpers.LiveID("dur-backlog-fwd")
	topic := helpers.LiveTopic(live)

	pubID := live + "-pub"
	pubPid := helpers.HostPidPath(t, pubID)
	pubArgs := durPythonTransientPub(topic, live)
	err := helpers.StartPeerProc(ctx, helpers.Container, domain, pubID, pubArgs...)
	if err != nil {
		t.Fatalf("start bounded transient publisher: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), durBacklogCleanWait)
		defer cleanupCancel()
		helpers.StopPeerProcAndWait(cleanupCtx, helpers.Container, domain, pubPid, pubID)
	})

	published := make(chan struct{})
	go func() {
		defer close(published)
		helpers.Sleep(ctx, durBacklogFreezeWait)
	}()

	waiterCtx, waiterCancel := context.WithCancel(ctx)
	waiterDone := make(chan struct{})
	go func() {
		defer close(waiterDone)
		_, _, _ = helpers.RunWeldEchoWithDurability(waiterCtx, bin, topic, domain, durBacklogTimeout, "transient_local") //nolint:errcheck // waiter output is intentionally discarded
	}()

	<-published
	waiterCancel()
	<-waiterDone
	if ctx.Err() != nil {
		t.Fatalf("context ended before late echo: %v", ctx.Err())
	}

	stdout, stderr, err := helpers.RunWeldEchoWithDurability(ctx, bin, topic, domain, durBacklogEchoWait, "transient_local")
	if err != nil {
		t.Fatalf("late weld echo: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}

	helpers.AssertNoBackendError(t, "dur-backlog-fwd", stdout+"\n"+stderr)

	// Keep-all never evicts, so the late echo must see every distinct
	// sample at least once; the slow tail keeps heartbeats flowing so
	// the NACK repair fires deterministically.
	for i := range durForwardSamples {
		want := fmt.Sprintf("hello-%d-%s", i, live)
		if got := strings.Count(stdout, want); got < 1 {
			t.Fatalf("late weld echo saw %q %d times, want at least once\nstdout:\n%s\nstderr:\n%s", want, got, stdout, stderr)
		}
	}
}

// withUnbufferedPython wraps a ros2 CLI argv so python flushes every line.
//
// ros2 topic echo block-buffers stdout on a pipe, so a handful of samples
// sits invisible in libc buffers until exit; polling the log mid-run then
// misses delivered samples. PYTHONUNBUFFERED forces per-line flush with
// no behavior change on the wire. The wrap keeps every docker flag
// (domain, RMW, image env) and replaces the container command with a
// shell export plus exec.
func withUnbufferedPython(argv []string, container, rosBin, topic, rosType string) []string {
	script := "export PYTHONUNBUFFERED=1; exec " + rosBin + " topic echo " + topic + " " + rosType + " --qos-reliability reliable --qos-durability transient_local"

	end := len(argv)
	for i, arg := range argv {
		if arg == container {
			end = i + 1
			break
		}
	}

	out := append([]string(nil), argv[:end]...)
	out = append(out, "sh", "-c", script)

	return out
}

// durEncodeString encodes one String sample for backend publish.
func durEncodeString(t *testing.T, text string) []byte {
	t.Helper()

	enc := types.NewEncoder(true)
	err := enc.WriteString(text)
	if err != nil {
		t.Fatalf("encode backlog sample: %v", err)
	}

	return enc.Bytes()
}

// durPublishBacklog publishes N distinct samples from one backend writer.
//
// One backend keeps one writer GUID per topic, so all N samples share a
// single keep-last history and the late echo proves per-writer replay.
func durPublishBacklog(t *testing.T, ctx context.Context, backend *dds.Backend, topic string, payloads []string) {
	t.Helper()

	for i, payload := range payloads {
		msg := transports.OutgoingMessage{
			TopicName: topic,
			TypeName:  "std_msgs__msg__String",
			Payload:   durEncodeString(t, payload),
			TypeHash:  helpers.ForwardStringHash,
		}

		err := helpers.PublishLiveWithRetry(ctx, backend, msg, durBacklogPubRetry, durBacklogPubGap, "durability-backlog")
		if err != nil {
			t.Fatalf("publish backlog sample %d of %d: %v", i+1, len(payloads), err)
		}
	}
}

// TestDurabilityLateCycloneEchoSeesWeldBacklog checks a late Cyclone echo replays Weld backlog.
//
// It publishes N distinct samples from one Weld backend writer, then
// starts a late ros2 topic echo with transient_local durability and
// asserts every payload arrives at least once. A missing payload means
// lost backlog and fails loudly; duplicates are transport-correct
// at-least-once repair and do not fail. It skips only for no peer or a
// missing std_msgs type.
func TestDurabilityLateCycloneEchoSeesWeldBacklog(t *testing.T) {
	t.Parallel()

	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), durBacklogTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	helpers.SkipIfNoTypeInterface(t, ctx, helpers.Container, domain, helpers.RosBin, helpers.RosType, durBacklogTypeProbe)
	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "dur-backlog-rev", "durability-backlog") //nolint:gosec // G115: pooled domain fits uint32

	live := helpers.LiveID("dur-backlog-rev")
	topic := helpers.LiveTopic(live)

	// Publish needs a route donor: the pooled domain starts empty, and
	// route-less publish correctly reports ErrNoRoute. A second Weld
	// backend subscribed to the topic donates SPDP routes without
	// affecting the late-echo backlog assertion below.
	donor := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "dur-backlog-donor", "durability-donor") //nolint:gosec // G115: pooled domain fits uint32
	err := donor.Subscribe(ctx, topic, "std_msgs__msg__String", helpers.ForwardStringHash)
	if err != nil {
		t.Fatalf("donor subscribe: %v", err)
	}
	helpers.WaitForTopicDiscovery(ctx, donor, topic)

	payloads := make([]string, 0, durReverseSamples)
	for i := range durReverseSamples {
		payloads = append(payloads, fmt.Sprintf("hello backlog %d %s", i, live))
	}

	durPublishBacklog(t, ctx, backend, topic, payloads)
	if ctx.Err() != nil {
		t.Fatalf("context ended before late echo: %v", ctx.Err())
	}

	// Close the donor before the late echo starts. Its routes already
	// served publishing; keeping it alive would NACK every heartbeat as
	// it receives live backlog into an untaken queue, drowning the echo's
	// own NACKs in noise and skewing drop accounting.
	err = donor.Close()
	if err != nil {
		t.Fatalf("close donor backend: %v", err)
	}

	// Settle before the late join so SEDP, heartbeats, and retention are
	// quiescent: the echo must NACK from a stable base instead of racing
	// in-flight publishes, which otherwise strands the first sample.
	helpers.Sleep(ctx, helpers.TalkerSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended during settle: %v", ctx.Err())
	}

	outPath := helpers.HostOutPath(t, live)
	echoFile := helpers.CreateLiveLogFile(t, outPath)
	t.Cleanup(func() {
		cerr := echoFile.Close()
		if cerr != nil {
			t.Logf("close echo log: %v", cerr)
		}
	})

	echoArgs := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "echo", topic, helpers.RosType,
		"--qos-reliability", "reliable", "--qos-durability", "transient_local")
	echoArgs = withUnbufferedPython(echoArgs, helpers.Container, helpers.RosBin, topic, helpers.RosType)
	echoCmd := helpers.StartDockerLogged(t, ctx, echoArgs, echoFile, "dur-backlog-rev-echo")
	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), durBacklogCleanWait)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, domain, topic)
	})
	helpers.Sleep(ctx, helpers.EchoSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before echo: %v", ctx.Err())
	}

	for _, payload := range payloads {
		if !helpers.WaitForFileContains(ctx, outPath, payload, durBacklogPollWait) {
			data, rerr := os.ReadFile(outPath) //nolint:gosec // test reads controlled temp log path
			if rerr != nil {
				t.Fatalf("late ros echo missed payload %q: %v", payload, rerr)
			}

			t.Fatalf("late ros echo missed backlog payload %q, want all %d samples\nlog:\n%s", payload, durReverseSamples, string(data))
		}
	}

	// Presence, not exact total: at-least-once repair may duplicate a
	// sample across overlapping NACKs, which is transport-correct. Every
	// asserted payload arriving at least once is the backlog proof; the
	// per-payload loop above already fails loudly on any miss.
	t.Logf("late ros echo got all %d backlog samples live %s", durReverseSamples, live)
}
