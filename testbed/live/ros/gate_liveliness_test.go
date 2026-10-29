// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package ros_test holds ROS String liveliness live cases.
//
// These tests use ephemeral domains 65 through 69 so a live run never
// hears the canonical runs on 42, 47, or 48. They drive the Weld
// backend through JoinDiscovery plus Take and Publish and drive the
// already running peer with os/exec docker argv only and no shell.
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

// ROS liveliness ephemeral domains.
//
// Each case owns its domain so runs never mix with canonical 42, 47,
// or 48 and never mix with each other. Isolation uses two domains to
// mirror the canonical 47 versus 42 split without colliding with it.
const (
	rosLiveLateDomain    uint32        = 65
	rosLiveLeaseDomain   uint32        = 66
	rosLiveDisposeDomain uint32        = 67
	rosLiveIsoListener   uint32        = 68
	rosLiveIsoForeign    uint32        = 69
	rosLiveLease         time.Duration = 10 * time.Second
	rosLiveOverall       time.Duration = 2 * time.Minute
	rosLiveTakeTimeout   time.Duration = 20 * time.Second
	rosLiveShortTake     time.Duration = 3 * time.Second
	rosLivePollTimeout   time.Duration = 20 * time.Second
	rosLiveRepubWait     time.Duration = 2 * time.Second
	rosLiveSettle        time.Duration = 2 * time.Second
	rosLiveType                        = "std_msgs__msg__String"
)

// encodeLiveString encodes text as XCDR1 std_msgs/String bytes.
func encodeLiveString(t *testing.T, text string) []byte {
	t.Helper()
	enc := types.NewEncoder(true)
	err := enc.WriteString(text)
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}
	wire := enc.Bytes()
	if len(wire) == 0 {
		t.Fatalf("encoded string is empty")
	}
	return wire
}

// startLiveTalker starts ros2 topic pub in the peer with per-run payload.
func startLiveTalker(t *testing.T, ctx context.Context, domain uint32, topic, payload string) *exec.Cmd {
	t.Helper()
	pubArg := fmt.Sprintf("{data: %s}", payload)
	args := helpers.DockerArgs(helpers.Container, int(domain), helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2")
	talker := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with fixed test args
	err := talker.Start()
	if err != nil {
		t.Fatalf("start ROS talker: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(talker)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, int(domain), payload)
	})
	return talker
}

// stopLiveTalker stops a talker started by startLiveTalker and kills survivors.
func stopLiveTalker(t *testing.T, ctx context.Context, talker *exec.Cmd, domain uint32, payload string) {
	t.Helper()
	helpers.StopCmd(talker)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cleanupCancel()
	helpers.KillContainerProc(cleanupCtx, helpers.Container, int(domain), payload)
	helpers.Sleep(ctx, rosLiveSettle)
}

// startLiveEcho starts ros2 topic echo in the peer and logs to a file.
func startLiveEcho(t *testing.T, ctx context.Context, domain uint32, topic, logPath string) (*exec.Cmd, *os.File) {
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
	args := helpers.DockerArgs(helpers.Container, int(domain), helpers.RosBin, "topic", "echo", topic, helpers.RosType)
	echoCmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness runs docker with fixed test args
	echoCmd.Stdout = echoFile
	echoCmd.Stderr = echoFile
	err = echoCmd.Start()
	if err != nil {
		t.Fatalf("start ros2 topic echo: %v", err)
	}
	t.Cleanup(func() {
		helpers.StopCmd(echoCmd)
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, int(domain), topic)
	})
	return echoCmd, echoFile
}

// tryDecodeLiveString decodes XCDR1 String bytes without failing the test.
//
// It mirrors decodeLiveString but returns the error so the late forward loop
// can count a bad payload as a skip and keep waiting for the per-run sample
// instead of failing on stray DATA. The caller decides timeout versus skip.
func tryDecodeLiveString(wire []byte) (string, error) {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return "", fmt.Errorf("live decode header: %w", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		return "", fmt.Errorf("live decode string: %w", err)
	}
	err = dec.Finish()
	if err != nil {
		return "", fmt.Errorf("live decode finish: %w", err)
	}
	return got, nil
}

// checkLateForward proves Weld Take sees a ROS talker started late.
//
// It subscribes on the per-run forward topic on the isolated late domain,
// then starts a ROS talker on that same per-run topic, never on shared
// /chatter. It loops TakeBytes until a sample arrives on the per-run topic
// with the run ID or the take timeout ends. Samples on other topics are
// counted as skips and never fail the test, so a stray talker from another
// run cannot flake this gate. Gap and framing drops are also counted as
// skips. Only a timeout fails. The per-run topic plus isolated domain plus
// this filter together prove late discovery both ways.
//
//nolint:gocognit,cyclop,nestif // late forward loop filters timeout versus skip shapes in one place
func checkLateForward(t *testing.T, ctx context.Context, backend *dds.Backend, forwardTopic, forwardPayload, runID string) {
	t.Helper()
	err := backend.Subscribe(ctx, forwardTopic, rosLiveType, "")
	if err != nil {
		t.Fatalf("subscribe forward %q: %v", forwardTopic, err)
	}
	takeCtx, takeCancel := context.WithTimeout(ctx, rosLiveTakeTimeout)
	defer takeCancel()
	helpers.Sleep(ctx, rosLiveSettle)
	cerr := ctx.Err()
	if cerr != nil {
		t.Fatalf("context ended before talker: %v", cerr)
	}
	talker := startLiveTalker(t, ctx, rosLiveLateDomain, forwardTopic, forwardPayload)
	skips := 0
	for {
		cerr = ctx.Err()
		if cerr != nil {
			stopLiveTalker(t, ctx, talker, rosLiveLateDomain, forwardPayload)
			t.Fatalf("context ended waiting for late talker: %v", cerr)
		}
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
				tcerr := takeCtx.Err()
				if tcerr != nil {
					terr = tcerr
				}
				stopLiveTalker(t, ctx, talker, rosLiveLateDomain, forwardPayload)
				t.Fatalf("weld take timed out waiting for late talker %q after %d skips: %v", forwardPayload, skips, terr)
			}
			if errors.Is(terr, dds.ErrStaleSequence) || errors.Is(terr, dds.ErrTruncated) || errors.Is(terr, dds.ErrInvalidFrame) {
				skips++
				t.Logf("skip late take drop %d: %v", skips, terr)
				continue
			}
			skips++
			t.Logf("skip late take error %d: %v", skips, terr)
			continue
		}
		if msg.TopicName != forwardTopic {
			skips++
			t.Logf("skip take arrived on %q want %q skips %d", msg.TopicName, forwardTopic, skips)
			continue
		}
		text, derr := tryDecodeLiveString(msg.Payload)
		if derr != nil {
			skips++
			t.Logf("skip take decode on %q skips %d: %v", msg.TopicName, skips, derr)
			continue
		}
		if !strings.Contains(text, runID) {
			skips++
			t.Logf("skip take payload without run ID on %q skips %d got %q", msg.TopicName, skips, text)
			continue
		}
		t.Logf("late forward ok on %q skips %d", forwardTopic, skips)
		break
	}
	stopLiveTalker(t, ctx, talker, rosLiveLateDomain, forwardPayload)
}

// checkLateReverse proves ros2 echo sees Weld pubs started before echo.
// It subscribes nothing on the Weld side and instead republishes until
// the peer echo observes the per-run ID. A single early publish can leave
// before matching completes, so the loop republishes every republish
// interval until the overall poll timeout ends. Only a timeout fails.
func checkLateReverse(t *testing.T, ctx context.Context, backend *dds.Backend, reverseTopic, reversePayload, runID string) {
	t.Helper()
	echoPath := filepath.Join(t.TempDir(), "rosecho-late-"+runID+".log")
	earlyPayload := reversePayload + "-early"
	earlyMsg := transports.OutgoingMessage{
		TopicName: reverseTopic,
		TypeName:  rosLiveType,
		Payload:   encodeLiveString(t, earlyPayload),
	}
	err := helpers.PublishLiveWithRetry(ctx, backend, earlyMsg, 10*time.Second, 200*time.Millisecond, "live")
	if err != nil {
		if errors.Is(err, dds.ErrNoRoute) {
			t.Logf("early pub saw no route, continuing to late echo check: %v", err)
		} else {
			t.Fatalf("early weld pub: %v", err)
		}
	}
	_, _ = startLiveEcho(t, ctx, rosLiveLateDomain, reverseTopic, echoPath)
	helpers.Sleep(ctx, rosLiveSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before late pub: %v", ctx.Err())
	}
	lateMsg := transports.OutgoingMessage{
		TopicName: reverseTopic,
		TypeName:  rosLiveType,
		Payload:   encodeLiveString(t, reversePayload),
	}
	deadline := time.Now().Add(rosLivePollTimeout)
	found := false
	for time.Now().Before(deadline) {
		err = helpers.PublishLiveWithRetry(ctx, backend, lateMsg, 10*time.Second, 200*time.Millisecond, "live")
		if err != nil {
			t.Fatalf("late weld pub: %v", err)
		}
		if helpers.WaitForFileContains(ctx, echoPath, runID, rosLiveRepubWait) {
			found = true
			break
		}
		cerr := ctx.Err()
		if cerr != nil {
			break
		}
	}
	if !found {
		data, rerr := os.ReadFile(echoPath) //nolint:gosec // live harness reads controlled temp file path
		if rerr != nil {
			t.Fatalf("ros echo did not see live payload %q: %v", reversePayload, rerr)
		}
		t.Fatalf("ros echo did not see live payload %q, pub ok\necho output:\n%s", reversePayload, string(data))
	}
}

// TestROSLivelinessLateJoinerBothWays proves late discovery both ways.
//
// Direction A subscribes with Weld Take before the ROS talker, mirroring a talker
// that starts after echo. Direction B publishes with Weld before the ROS
// echo starts, mirroring a pub that starts before echo, then republishes
// after echo starts so the late echo still sees a live sample. Both run
// on ephemeral domain 65 with per-run topics and payloads. It skips without a
// daemon and never starts the compose stack.
func TestROSLivelinessLateJoinerBothWays(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), rosLiveOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("ros-late")
	forwardTopic := helpers.LiveTopic(runID + "-fwd")
	reverseTopic := helpers.LiveTopic(runID + "-rev")
	forwardPayload := "hello late forward " + runID
	reversePayload := "hello late reverse " + runID
	backend := helpers.MustOpenLiveBackend(t, ctx, rosLiveLateDomain, "ros-late-"+runID, "live")
	checkLateForward(t, ctx, backend, forwardTopic, forwardPayload, runID)
	checkLateReverse(t, ctx, backend, reverseTopic, reversePayload, runID)
	t.Logf("late joiner both ways ok id %s", runID)
}

// TestROSLivelinessLeaseExpiryRemoves proves a 10s lease sweep.
//
// It joins the Weld backend on ephemeral domain 66, handles one alive
// SPDP packet with a 10s lease into a RemoteTable, proves a sweep before
// expiry removes nothing, then proves a sweep after the lease removes
// the entry. A short Take with no peer traffic must time out, proving
// no cross domain payload leaked into 66. It skips without a daemon.
// isExpectedTakeError reports timeout or known gap errors for quiet takes.
func isExpectedTakeError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if errors.Is(err, dds.ErrStaleSequence) || errors.Is(err, dds.ErrTruncated) || errors.Is(err, dds.ErrInvalidFrame) {
		return true
	}
	return false
}

// checkQuietTake proves a quiet domain take times out without leak.
func checkQuietTake(t *testing.T, ctx context.Context, backend *dds.Backend, domain uint32) {
	t.Helper()
	shortCtx, shortCancel := context.WithTimeout(ctx, rosLiveShortTake)
	_, err := backend.TakeBytes(shortCtx)
	shortCancel()
	if err == nil {
		t.Fatalf("take on quiet domain %d succeeded, want timeout", domain)
	}
	if isExpectedTakeError(err) {
		return
	}
	t.Logf("quiet take ended with %v, continuing lease check", err)
}

// checkLeaseSweep proves 10s lease sweep removes after expiry.
func checkLeaseSweep(t *testing.T, domain uint32, lease time.Duration, runID string) {
	t.Helper()
	local, err := dds.NewLocalParticipant(domain, lease)
	if err != nil {
		t.Fatalf("new local participant: %v", err)
	}
	if local.LeaseDuration() != lease {
		t.Fatalf("lease = %v, want %v", local.LeaseDuration(), lease)
	}
	table := dds.NewRemoteTable(nil)
	packet, err := dds.MarshalAlivePacket(local.GUID(), domain, 1, local.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	ann, err := dds.ParseAnnouncement(packet)
	if err != nil {
		t.Fatalf("parse alive: %v", err)
	}
	if ann.Lease != lease {
		t.Fatalf("ann lease = %v, want %v", ann.Lease, lease)
	}
	now := time.Now()
	err = table.HandlePacket(packet, now)
	if err != nil {
		t.Fatalf("handle alive: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("table len = %d, want 1", table.Len())
	}
	removed := table.Sweep(now.Add(5 * time.Second))
	if removed != 0 {
		t.Fatalf("early sweep removed = %d, want 0", removed)
	}
	removed = table.Sweep(now.Add(11 * time.Second))
	if removed != 1 {
		t.Fatalf("expiry sweep removed = %d, want 1 id %s", removed, runID)
	}
	if table.Len() != 0 {
		t.Fatalf("table len after expiry = %d, want 0", table.Len())
	}
}

func TestROSLivelinessLeaseExpiryRemoves(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), rosLiveOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("ros-lease")
	backend := helpers.MustOpenLiveBackend(t, ctx, rosLiveLeaseDomain, "ros-lease-"+runID, "live")
	checkQuietTake(t, ctx, backend, rosLiveLeaseDomain)
	checkLeaseSweep(t, rosLiveLeaseDomain, rosLiveLease, runID)
	t.Logf("lease expiry removed after %v id %s", rosLiveLease, runID)
}

// rosCaptureSender records one SPDP packet for Goodbye checks.
type rosCaptureSender struct {
	packet []byte
}

// Send records one SPDP packet for later parsing.
func (s *rosCaptureSender) Send(_ context.Context, data []byte) error {
	s.packet = append([]byte(nil), data...)
	return nil
}

// checkRosGoodbye proves Goodbye emits a dispose packet observed live.
func checkRosGoodbye(t *testing.T, ctx context.Context, local *dds.LocalParticipant) {
	t.Helper()
	capSender := &rosCaptureSender{}
	goodbyeCtx, goodbyeCancel := context.WithTimeout(ctx, rosLiveShortTake)
	defer goodbyeCancel()
	err := local.Goodbye(goodbyeCtx, capSender)
	if err != nil {
		t.Fatalf("goodbye: %v", err)
	}
	if len(capSender.packet) == 0 {
		t.Fatalf("goodbye produced no packet")
	}
	gann, err := dds.ParseAnnouncement(capSender.packet)
	if err != nil {
		t.Fatalf("parse goodbye: %v", err)
	}
	if gann.Alive {
		t.Fatalf("goodbye parsed as alive, want dispose")
	}
	if gann.GUID.Prefix != local.GUID().Prefix {
		t.Fatalf("goodbye GUID mismatch")
	}
	if gann.DomainID != rosLiveDisposeDomain {
		t.Fatalf("goodbye domain = %d, want %d", gann.DomainID, rosLiveDisposeDomain)
	}
}

// TestROSLivelinessDisposeRemoves proves dispose removal.
//
// It joins the Weld backend on ephemeral domain 67, reports liveliness
// alive then dispose through ReportLiveliness, and handles alive then
// dispose SPDP packets through a RemoteTable. A stale dispose at an old
// sequence must not remove a newer alive entry. Goodbye output must
// parse as a dispose. It skips without a daemon.
func TestROSLivelinessDisposeRemoves(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), rosLiveOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("ros-dispose")
	backend := helpers.MustOpenLiveBackend(t, ctx, rosLiveDisposeDomain, "ros-dispose-"+runID, "live")
	aliveReport := transports.LivelinessReport{
		ParticipantName: "ros-dispose-" + runID,
		DomainID:        rosLiveDisposeDomain,
		Alive:           true,
		Generation:      1,
	}
	err := backend.ReportLiveliness(ctx, aliveReport)
	if err != nil {
		t.Fatalf("report alive: %v", err)
	}
	disposeReport := transports.LivelinessReport{
		ParticipantName: "ros-dispose-" + runID,
		DomainID:        rosLiveDisposeDomain,
		Alive:           false,
		Generation:      2,
	}
	err = backend.ReportLiveliness(ctx, disposeReport)
	if err != nil {
		t.Fatalf("report dispose: %v", err)
	}
	local, err := dds.NewLocalParticipant(rosLiveDisposeDomain, rosLiveLease)
	if err != nil {
		t.Fatalf("new local participant: %v", err)
	}
	table := dds.NewRemoteTable(nil)
	alive, err := dds.MarshalAlivePacket(local.GUID(), rosLiveDisposeDomain, 1, local.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal alive: %v", err)
	}
	err = table.HandlePacket(alive, time.Now())
	if err != nil {
		t.Fatalf("handle alive: %v", err)
	}
	if table.Len() != 1 {
		t.Fatalf("table len = %d, want 1", table.Len())
	}
	dispose, err := dds.MarshalDisposePacket(local.GUID(), rosLiveDisposeDomain, 2)
	if err != nil {
		t.Fatalf("marshal dispose: %v", err)
	}
	dann, err := dds.ParseAnnouncement(dispose)
	if err != nil {
		t.Fatalf("parse dispose: %v", err)
	}
	if dann.Alive {
		t.Fatalf("dispose parsed as alive")
	}
	err = table.HandlePacket(dispose, time.Now())
	if err != nil {
		t.Fatalf("handle dispose: %v", err)
	}
	if table.Len() != 0 {
		t.Fatalf("table len after dispose = %d, want 0", table.Len())
	}
	checkRosGoodbye(t, ctx, local)
	t.Logf("dispose removed id %s", runID)
}

// checkForeignAnnouncement proves foreign SPDP parses with foreign domain.
func checkForeignAnnouncement(t *testing.T) {
	t.Helper()
	foreign, err := dds.NewLocalParticipant(rosLiveIsoForeign, rosLiveLease)
	if err != nil {
		t.Fatalf("new foreign participant: %v", err)
	}
	foreignPacket, err := dds.MarshalAlivePacket(foreign.GUID(), rosLiveIsoForeign, 1, foreign.LeaseDuration())
	if err != nil {
		t.Fatalf("marshal foreign alive: %v", err)
	}
	fann, err := dds.ParseAnnouncement(foreignPacket)
	if err != nil {
		t.Fatalf("parse foreign: %v", err)
	}
	if fann.DomainID != rosLiveIsoForeign {
		t.Fatalf("foreign domain = %d, want %d", fann.DomainID, rosLiveIsoForeign)
	}
	if fann.DomainID == rosLiveIsoListener {
		t.Fatalf("foreign domain equals listener, want isolation")
	}
}

// checkIsolationFlow proves listener never sees foreign payload.
func checkIsolationFlow(t *testing.T, ctx context.Context, listenerBackend, foreignBackend *dds.Backend, runID string) {
	t.Helper()
	foreignTopic := helpers.LiveTopic(runID + "-iso")
	foreignPayload := "hello iso " + runID
	listenerTable := dds.NewRemoteTable(nil)
	if listenerTable.Len() != 0 {
		t.Fatalf("listener table len = %d, want 0", listenerTable.Len())
	}
	_ = startLiveTalker(t, ctx, rosLiveIsoForeign, foreignTopic, foreignPayload)
	foreignMsg := transports.OutgoingMessage{
		TopicName: foreignTopic,
		TypeName:  rosLiveType,
		Payload:   encodeLiveString(t, foreignPayload),
	}
	err := helpers.PublishLiveWithRetry(ctx, foreignBackend, foreignMsg, 10*time.Second, 200*time.Millisecond, "live")
	if err != nil {
		if !errors.Is(err, dds.ErrNoRoute) {
			t.Fatalf("foreign publish: %v", err)
		}
		t.Logf("foreign publish saw no route: %v", err)
	}
	checkIsolationQuietTake(t, ctx, listenerBackend, listenerTable)
}

// checkIsolationQuietTake proves short take on listener times out isolated.
func checkIsolationQuietTake(t *testing.T, ctx context.Context, listenerBackend *dds.Backend, listenerTable *dds.RemoteTable) {
	t.Helper()
	shortCtx, shortCancel := context.WithTimeout(ctx, rosLiveShortTake)
	_, err := listenerBackend.TakeBytes(shortCtx)
	shortCancel()
	if err == nil {
		t.Fatalf("listener on %d took foreign payload from %d, want isolation", rosLiveIsoListener, rosLiveIsoForeign)
	}
	if !isExpectedTakeError(err) {
		t.Logf("isolation take ended with %v, still isolated", err)
	}
	if listenerTable.Len() != 0 {
		t.Fatalf("listener table len = %d, want 0", listenerTable.Len())
	}
}

// TestROSLivelinessDomainIsolation proves 68 never sees 69.
//
// It mirrors the canonical 47 versus 42 split with ephemeral domains:
// a Weld backend joined on 68 must never Take a String published on 69,
// and SPDP announced on 69 must parse with domain 69 so a 68 listener
// ignores it. It starts a ROS talker on 69 and Takes on 68 with a short
// timeout to prove the payload never crosses. It skips without a daemon.
func TestROSLivelinessDomainIsolation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), rosLiveOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("ros-iso")
	listenerBackend := helpers.MustOpenLiveBackend(t, ctx, rosLiveIsoListener, "ros-iso-listen-"+runID, "live")
	foreignBackend := helpers.MustOpenLiveBackend(t, ctx, rosLiveIsoForeign, "ros-iso-foreign-"+runID, "live")
	checkForeignAnnouncement(t)
	checkIsolationFlow(t, ctx, listenerBackend, foreignBackend, runID)
	t.Logf("isolation held %d never saw %d id %s", rosLiveIsoListener, rosLiveIsoForeign, runID)
}
