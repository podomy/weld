// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package helpers

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
)

// Forward-echo harness mitigation bounds.
//
// The forward direction races ROS talker publishes against Weld reader
// discovery: early talker samples leave before the Weld reader matches and
// never arrive, so a single weld echo run misses the live ID. The waiter
// below polls the probe backend SEDP remotes for the ROS writer before the
// echo runs, and the echo retry constants space repeated echo captures so a
// late match still lands. Timeouts fail loudly and never skip.
const (
	// SEDPRemoteTimeout bounds the wait for a ROS writer remote.
	SEDPRemoteTimeout = 10 * time.Second
	// SEDPRemotePollInterval spaces polls while waiting for a remote writer.
	SEDPRemotePollInterval = 100 * time.Millisecond
	// ForwardEchoAttempts bounds weld echo captures per forward test.
	ForwardEchoAttempts = 3
	// ForwardEchoRetryGap spaces weld echo captures so one more talker publish lands.
	ForwardEchoRetryGap = 500 * time.Millisecond
	// ForwardStringIDL is the IDL type name for std_msgs/String readers.
	ForwardStringIDL = "std_msgs__msg__String"
	// ForwardStringHash is the interop type hash for std_msgs/String.
	ForwardStringHash = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"
)

// WaitForSEDPRemote polls until a remote endpoint for topic appears.
//
// It returns when backend HasRemoteEndpoint reports the topic before the
// timeout. On timeout or context end it fails the test loudly with the
// missing writer description and topic, never skipping. Call it after the
// probe reader subscribes and the ROS talker starts so the talker publishes
// only after the reader is visible and SEDP has settled before echo runs.
//
//nolint:gocognit // wait loop filters timeout, domain, writer, and sequence in one place
func WaitForSEDPRemote(t *testing.T, ctx context.Context, backend *dds.Backend, topic, wantWriter string, timeout time.Duration) {
	t.Helper()
	if backend == nil {
		t.Fatalf("helpers: SEDP remote wait needs a backend for topic %q writer %q", topic, wantWriter)
	}
	if topic == "" {
		t.Fatalf("helpers: SEDP remote wait needs a topic for writer %q", wantWriter)
	}
	if timeout <= 0 {
		timeout = SEDPRemoteTimeout
	}
	if backend.HasRemoteEndpoint(topic) {
		return
	}
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			if backend.HasRemoteEndpoint(topic) {
				return
			}
			t.Fatalf("helpers: SEDP remote writer %q for topic %q not visible after %s", wantWriter, topic, timeout)
		}
		if ctx.Err() != nil {
			t.Fatalf("helpers: SEDP remote writer %q for topic %q wait ended: %v", wantWriter, topic, ctx.Err())
		}
		timer := time.NewTimer(SEDPRemotePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatalf("helpers: SEDP remote writer %q for topic %q wait ended: %v", wantWriter, topic, ctx.Err())
		case <-timer.C:
		}
		if backend.HasRemoteEndpoint(topic) {
			return
		}
	}
}

// WaitForTalkerTopic polls until a remote endpoint for topic appears.
//
// It returns true when backend HasRemoteEndpoint reports the topic before
// the timeout, and false on timeout or context end. The timeout is an upper
// bound only: callers pass the legacy fixed nap constant (TalkerSettle at
// forward call sites) so worst case timing matches the previous Sleep while
// the common case returns as soon as SEDP shows the ROS writer. A false
// return never fails the caller: the echo path still runs and reports the
// gap, so assertion timing semantics are unchanged. Polling reuses
// SEDPRemotePollInterval and the timer pattern from WaitForSEDPRemote.
func WaitForTalkerTopic(ctx context.Context, backend *dds.Backend, topic string, timeout time.Duration) bool {
	if backend == nil {
		return false
	}
	if topic == "" {
		return false
	}
	if timeout <= 0 {
		timeout = TalkerSettle
	}
	if backend.HasRemoteEndpoint(topic) {
		return true
	}
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return backend.HasRemoteEndpoint(topic)
		}
		if ctx.Err() != nil {
			return false
		}
		timer := time.NewTimer(SEDPRemotePollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		if backend.HasRemoteEndpoint(topic) {
			return true
		}
	}
}

// normalizePublishBounds applies defaults for publish retry bounds.
//
// Non-positive maxPubs means one attempt and non-positive pollWait means
// FilePollTimeout, matching PublishUntilSeen semantics for callers that
// pass zero values.
func normalizePublishBounds(maxPubs int, pollWait time.Duration) (int, time.Duration) {
	if maxPubs <= 0 {
		maxPubs = 1
	}
	if pollWait <= 0 {
		pollWait = FilePollTimeout
	}
	return maxPubs, pollWait
}

// checkPublishCtx reports whether ctx already ended.
//
// It returns nil when ctx is live and a wrapped context error otherwise,
// so PublishUntilSeen fails fast without spending an attempt.
func checkPublishCtx(ctx context.Context) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("helpers: publish context ended: %w", err)
	}
	return nil
}

// attemptPollWait caps pollWait to the remaining ctx budget per attempt left.
//
// It splits the time until the caller deadline across attempts left so
// the helper never overruns ctx. Without a deadline it returns pollWait.
// A spent deadline returns a wrapped context error for fail fast.
func attemptPollWait(ctx context.Context, pollWait time.Duration, attemptsLeft int) (time.Duration, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return pollWait, nil
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		err := ctx.Err()
		if err != nil {
			return 0, fmt.Errorf("helpers: publish context ended: %w", err)
		}
		return 0, fmt.Errorf("helpers: publish deadline spent: %w", context.DeadlineExceeded)
	}
	perAttempt := remaining / time.Duration(attemptsLeft)
	if perAttempt < pollWait {
		return perAttempt, nil
	}
	return pollWait, nil
}

// PublishUntilSeen publishes one weld message and polls the ROS echo log until it holds the payload.
//
// It calls RunWeldPub then WaitForFileContains for payload in echoPath, retrying the publish on miss up to maxPubs times.
// A pub error returns immediately as err with seen false and the last stdout and stderr.
// A miss after exhausting attempts returns seen false with nil err so the caller fails on the missing payload.
// Every attempt is bounded by ctx: the remaining time until the caller deadline is split across attempts left and caps pollWait, so the helper never overruns ctx.
// Non-positive maxPubs means one attempt and non-positive pollWait means FilePollTimeout.
func PublishUntilSeen(ctx context.Context, bin, topic string, domain int, payload, echoPath string, pollWait time.Duration, maxPubs int) (seen bool, stdout, stderr string, err error) {
	maxPubs, pollWait = normalizePublishBounds(maxPubs, pollWait)
	for attempt := range maxPubs {
		err = checkPublishCtx(ctx)
		if err != nil {
			return false, stdout, stderr, err
		}
		attemptsLeft := maxPubs - attempt
		var attemptPoll time.Duration
		attemptPoll, err = attemptPollWait(ctx, pollWait, attemptsLeft)
		if err != nil {
			return false, stdout, stderr, err
		}
		stdout, stderr, err = RunWeldPub(ctx, bin, topic, domain, payload)
		if err != nil {
			return false, stdout, stderr, err
		}
		if WaitForFileContains(ctx, echoPath, payload, attemptPoll) {
			return true, stdout, stderr, nil
		}
	}
	return false, stdout, stderr, nil
}
