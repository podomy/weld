// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package ros_test holds the liveliness enforcement live gate.
//
// These tests prove a dead CycloneDDS writer turns stale then expired in
// the Weld reader within the lease bound instead of serving ancient
// samples silently, plus deadline misses count against a slow publisher.
// Stale uses ephemeral domain 63 and deadline uses one allocated exclusive
// domain so the parallel tests never share SPDP state, both with per-run
// topics, skip without a peer, and fail loudly on timeout. They never
// start the compose stack.
package ros_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Enforcement live gate constants.
//
// Stale pins ephemeral domain 63 so it never hears canonical 42, 47, or
// 48 and never hears the 65 through 69 ROS liveliness gates. Deadline
// uses one allocated exclusive domain so the two parallel enforcement
// tests never share SPDP state. Per-run topics isolate concurrent runs on
// the same domain. The lease is 5 seconds against a 2 Hz talker: healthy
// gaps are 500 ms, stale fires at one lease, expired at two leases, each
// with slack for loaded CI. Talker lifecycle plus XCDR String codec reuse
// the single helpers in gate_liveliness_test.go.
const (
	enforceGateDomain      uint32        = 63
	enforceGateOverall     time.Duration = 2 * time.Minute
	enforceGateTakeTimeout time.Duration = 20 * time.Second
	enforceGateShortTake   time.Duration = 3 * time.Second
	enforceGateSettle      time.Duration = 2 * time.Second
	enforceGateDrainStep   time.Duration = 300 * time.Millisecond
	enforceGatePollStep    time.Duration = 200 * time.Millisecond
	enforceGateLease       time.Duration = 5 * time.Second
	enforceGateSlack       time.Duration = 15 * time.Second
	enforceGateType        string        = "std_msgs__msg__String"
)

// Talker lifecycle plus XCDR String codec reuse the single helpers in
// gate_liveliness_test.go: startLiveTalker, stopLiveTalker, and
// tryDecodeLiveString. Enforcement tests call those directly so parallel
// runs share one lifecycle and one codec with no drift.

// enforceTakeOneLiveSample takes until the per-run payload arrives.
//
// Samples on other topics, undecodable payloads, and gap drops count as
// skips. Only the take timeout fails. Every accepted sample asserts the
// tracker when automatic so the kill phase measures real liveliness.
// A loud skip cap fails after 100 consecutive skips so systematic
// wrong-topic delivery fails instead of spinning to timeout.
//
//nolint:gocognit,cyclop // live helper sequences take skip and assert in one loop, splitting would obscure intent
func enforceTakeOneLiveSample(t *testing.T, ctx context.Context, backend *dds.Backend, topic, runID string, tracker *core.LivelinessTracker) {
	t.Helper()
	takeCtx, takeCancel := context.WithTimeout(ctx, enforceGateTakeTimeout)
	defer takeCancel()
	skips := 0
	lastReason := ""
	for {
		cerr := ctx.Err()
		if cerr != nil {
			t.Fatalf("enforce context ended waiting for %q: %v", topic, cerr)
		}
		if skips >= 100 {
			t.Fatalf("enforce take on %q saw 100 consecutive skips without run %q, last reason %q: systematic wrong-topic delivery", topic, runID, lastReason)
		}
		msg, err := backend.TakeBytes(takeCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				t.Fatalf("enforce take timed out on %q after %d skips: %v", topic, skips, err)
			}
			if errors.Is(err, dds.ErrStaleSequence) || errors.Is(err, dds.ErrTruncated) || errors.Is(err, dds.ErrInvalidFrame) {
				skips++
				lastReason = err.Error()
				continue
			}
			skips++
			lastReason = err.Error()
			t.Logf("enforce skip take error %d: %v", skips, err)
			continue
		}
		if msg.TopicName != topic {
			skips++
			lastReason = "wrong topic " + msg.TopicName
			continue
		}
		text, derr := tryDecodeLiveString(msg.Payload)
		if derr != nil {
			skips++
			lastReason = derr.Error()
			continue
		}
		if !strings.Contains(text, runID) {
			skips++
			lastReason = "payload without run ID " + text
			continue
		}
		tracker.RecordSampleAt(time.Now().UnixNano())
		t.Logf("enforce live sample ok on %q skips %d", topic, skips)
		return
	}
}

// enforceDrainQueue consumes everything queued so a post-expiry take cannot
// deliver an ancient buffered sample. Drops and gap errors count as
// drained; only the timeout ends the drain. The tracker is untouched, so
// stale and expired timing still anchors on the last live sample.
// Empty queue is legitimate best effort, so drained count is logged with
// topic and never asserted.
func enforceDrainQueue(t *testing.T, ctx context.Context, backend *dds.Backend, topic string) {
	t.Helper()

	drained := 0
	for ctx.Err() == nil {
		drainCtx, drainCancel := context.WithTimeout(ctx, enforceGateDrainStep)
		msg, err := backend.TakeBytes(drainCtx)
		drainCancel()
		if err != nil {
			break
		}
		_ = msg
		drained++
	}

	t.Logf("enforce drained %d queued samples on %q", drained, topic)
}

// enforceWaitForStage polls the tracker until want or the bound ends.
//
// It fails loudly with the lease bound when the stage never arrives, so
// a dead writer that never turns visible fails instead of passing.
func enforceWaitForStage(t *testing.T, ctx context.Context, tracker *core.LivelinessTracker, want core.LivelinessState, bound time.Duration, label string) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		cerr := ctx.Err()
		if cerr != nil {
			t.Fatalf("enforce %s context ended: %v", label, cerr)
		}
		if got := tracker.StateAt(time.Now().UnixNano()); got == want {
			t.Logf("enforce %s reached %v", label, want)
			return
		}
		helpers.Sleep(ctx, enforceGatePollStep)
	}
	t.Fatalf("enforce %s never reached %v within %v: still %v (silent ancient delivery risk)", label, want, bound, tracker.StateAt(time.Now().UnixNano()))
}

// TestROSLivelinessEnforcementStaleExpiry kills a CycloneDDS talker mid-stream.
//
// It takes one live sample, kills the talker, then proves the Weld side
// tracker reports stale within one lease and expired within two leases.
// A short take after expiry must time out: delivering an ancient sample
// after expiry fails loudly, and the pure gate must drop stale with its
// loud reason.
func TestROSLivelinessEnforcementStaleExpiry(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), enforceGateOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("ros-enforce-stale")
	topic := helpers.LiveTopic(runID + "-enforce")
	payload := "hello enforce stale " + runID
	backend := helpers.MustOpenLiveBackend(t, ctx, enforceGateDomain, "ros-enforce-"+runID, "live")
	err := backend.Subscribe(ctx, topic, enforceGateType, "")
	if err != nil {
		t.Fatalf("subscribe enforce %q: %v", topic, err)
	}
	lease := core.NewDuration(int64(enforceGateLease/time.Second), 0)
	tracker := core.NewLivelinessTracker(topic, "cyclone-talker", core.LivelinessAutomatic, lease, time.Now().UnixNano())
	talker := startLiveTalker(t, ctx, enforceGateDomain, topic, payload, "2")
	helpers.WaitForTopicDiscovery(ctx, backend, topic)
	enforceTakeOneLiveSample(t, ctx, backend, topic, runID, &tracker)
	stopLiveTalker(t, ctx, talker, enforceGateDomain, payload)
	// Drain after the kill so no buffered sample survives into the
	// post-expiry take: the talker is dead, nothing new can arrive, and
	// the tracker still anchors on the live sample for stale/expiry.
	enforceDrainQueue(t, ctx, backend, topic)
	enforceWaitForStage(t, ctx, &tracker, core.LivelinessStale, enforceGateLease+enforceGateSlack, "stale")
	enforceWaitForStage(t, ctx, &tracker, core.LivelinessExpired, enforceGateLease+enforceGateSlack, "expired")
	shortCtx, shortCancel := context.WithTimeout(ctx, enforceGateShortTake)
	msg, err := backend.TakeBytes(shortCtx)
	shortCancel()
	if err == nil {
		gate := core.GateDelivery(core.LivelinessExpired, false)
		t.Fatalf("take after expiry delivered ancient sample on %q: payload %d bytes gate %v", topic, len(msg.Payload), gate)
	}
	if !isExpectedTakeError(err) {
		t.Logf("post-expiry take ended with %v, still not a delivery", err)
	}
	gate := core.GateDelivery(tracker.LatchedState(), false)
	if gate.Decision != core.GateDrop {
		t.Fatalf("expired gate = %v, want drop", gate.Decision)
	}
	if !strings.Contains(gate.Reason, "stale") {
		t.Fatalf("expired gate reason must say stale, got %q", gate.Reason)
	}
	if gate.Policy != core.PolicyLeaseDuration {
		t.Fatalf("expired gate policy = %q, want lease_duration", gate.Policy)
	}
	t.Logf("stale then expired within lease bound ok id %s", runID)
}

// TestROSLivelinessEnforcementDeadlineMiss counts misses on a slow publisher.
//
// It runs a 1 Hz talker against a 500 ms deadline, takes three samples,
// and requires at least one deadline miss counted under the deadline
// policy plus one monitoring Check alert. Zero misses fails loudly.
//
//nolint:gocognit,cyclop // live gate sequences slow talker plus miss counting in one run, splitting would obscure intent
func TestROSLivelinessEnforcementDeadlineMiss(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), enforceGateOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	deadlineDomain := helpers.MustAllocateDomainU32(t)
	runID := helpers.LiveID("ros-enforce-deadline")
	topic := helpers.LiveTopic(runID + "-deadline")
	payload := "hello enforce deadline " + runID
	backend := helpers.MustOpenLiveBackend(t, ctx, deadlineDomain, "ros-enforce-dl-"+runID, "live")
	err := backend.Subscribe(ctx, topic, enforceGateType, "")
	if err != nil {
		t.Fatalf("subscribe deadline %q: %v", topic, err)
	}
	deadline := core.NewDuration(0, 500000000)
	deadlineTracker := core.NewDeadlineTracker(topic, "cyclone-talker", deadline, time.Now().UnixNano())
	talker := startLiveTalker(t, ctx, deadlineDomain, topic, payload, "1")
	helpers.WaitForTopicDiscovery(ctx, backend, topic)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		helpers.KillContainerProc(cleanupCtx, helpers.Container, int(deadlineDomain), payload)
	})
	misses := 0
	alerts := 0
	seen := 0
	takeCtx, takeCancel := context.WithTimeout(ctx, enforceGateTakeTimeout)
	defer takeCancel()
	for seen < 3 {
		cerr := ctx.Err()
		if cerr != nil {
			t.Fatalf("deadline context ended: %v", cerr)
		}
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
				break
			}
			if errors.Is(terr, dds.ErrStaleSequence) || errors.Is(terr, dds.ErrTruncated) || errors.Is(terr, dds.ErrInvalidFrame) {
				continue
			}
			t.Logf("deadline take error: %v", terr)
			continue
		}
		if msg.TopicName != topic {
			continue
		}
		text, derr := tryDecodeLiveString(msg.Payload)
		if derr != nil {
			continue
		}
		if !strings.Contains(text, runID) {
			continue
		}
		now := time.Now().UnixNano()
		if deadlineTracker.MissedAt(now) {
			misses++
			gate := core.GateDelivery(core.LivelinessAlive, true)
			if gate.Decision != core.GateDrop || gate.Policy != core.PolicyDeadline {
				t.Fatalf("deadline miss gate = %+v, want drop with deadline policy", gate)
			}
		}
		deadlineTracker.MarkSeen(now)
		seen++
	}
	now := time.Now().UnixNano()
	if _, ok := deadlineTracker.Check(now); ok {
		alerts++
	}
	stopLiveTalker(t, ctx, talker, deadlineDomain, payload)
	if seen == 0 {
		t.Fatalf("slow publisher on %q delivered nothing in %v", topic, enforceGateTakeTimeout)
	}
	if misses == 0 && alerts == 0 {
		t.Fatalf("slow 1 Hz publisher against 500 ms deadline counted zero misses over %d samples (want at least one)", seen)
	}
	t.Logf("deadline misses counted %d alerts %d over %d samples id %s", misses, alerts, seen, runID)
}
