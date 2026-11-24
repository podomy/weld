// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package ros_test holds offline interop checks that need no peer.
//
// This file covers the offline half of the ROS String round trip on
// topic /chatter with std_msgs/String: domain rejection,
// empty payload acceptance, and stale or duplicate drop tolerance. The
// live directions live in roundtrip_test.go in this package and stay
// separate from the Cyclone SPDP acceptance checks: no SPDP wire asserts
// and no trace parsing live here. Only one run per domain and topic at a
// time is supported, so live tests never run in parallel and every payload
// carries a per-run live ID that stale runs cannot match.
package ros_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// ROS round-trip constants for /chatter.
//
// Domain 48 belongs to the regression gate, so this file never pins 48
// and never hears a regression run. The container name matches
// testbed/docker-compose.yml. Valid domain checks use one allocated
// exclusive domain.
const rosTestTopic = "/chatter"

// checkTransportDomainRejects pins transport layer caller domain rejection.
//
// Caller values above maximum fail without process env, so the check stays
// parallel safe.
func checkTransportDomainRejects(t *testing.T) {
	t.Helper()
	_, err := transports.ResolveDomainID(999)
	if err == nil {
		t.Fatalf("caller 999 accepted, want rejection above maximum")
	}
}

// checkWeldEnvDomainValid pins explicit child env delivery for a valid domain.
//
// Domain is the allocated exclusive domain plumbed to EnvWithDomain, so
// valid domain delivery stays parallel safe without touching process env.
func checkWeldEnvDomainValid(t *testing.T, domain int) {
	t.Helper()
	env := helpers.EnvWithDomain(domain)
	if !slices.Contains(env, "ROS_DOMAIN_ID="+strconv.Itoa(domain)) {
		t.Fatalf("explicit child env misses ROS_DOMAIN_ID=%d", domain)
	}
}

// TestROSBadDomainRejected checks out of range domains fail at the transport layer.
//
// It covers the caller value at the transport layer plus valid child env
// delivery via EnvWithDomain. The weld CLI flag and env rejection checks
// were removed with the deleted cmd/weld binary. No peer is needed, so
// this test never skips.
func TestROSBadDomainRejected(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	checkTransportDomainRejects(t)
	checkWeldEnvDomainValid(t, domain)
}

// TestROSEmptyPayloadAllowed checks an empty String sample stays valid.
//
// An empty std_msgs/String is a legal sample with only the length word
// and NUL on the wire. The codec must round trip it exactly, which is
// what lets Weld pub and echo carry empty payloads without rejection.
func TestROSEmptyPayloadAllowed(t *testing.T) {
	t.Parallel()
	enc := types.NewEncoder(true)
	err := enc.WriteString("")
	if err != nil {
		t.Fatalf("encode empty string: %v", err)
	}
	wire := enc.Bytes()
	if len(wire) == 0 {
		t.Fatalf("empty string encoded to zero bytes, want header plus length")
	}
	dec, err := types.NewDecoder(wire)
	if err != nil {
		t.Fatalf("new decoder: %v", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode empty string: %v", err)
	}
	if got != "" {
		t.Fatalf("decoded = %q, want empty", got)
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("finish empty string: %v", err)
	}
}

// rosFrameSink records DATA frames in memory for ordering checks.
type rosFrameSink struct {
	mu     sync.Mutex
	frames [][]byte
}

// WriteFrame records one frame copy per copy once alignment.
func (s *rosFrameSink) WriteFrame(frame []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]byte(nil), frame...)
	s.frames = append(s.frames, cp)
	return nil
}

// snapshot returns copies of all recorded frames.
func (s *rosFrameSink) snapshot() [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([][]byte, len(s.frames))
	for i, f := range s.frames {
		out[i] = append([]byte(nil), f...)
	}
	return out
}

// rosIsSkippableSample reports whether a Take error is a per-sample drop.
//
// Stale duplicates and truncated or invalid frames count as drops that
// the echo path skips with a counter. Any other error is fatal to the
// run. This mirrors the CLI echo loop contract for the round trip.
func rosIsSkippableSample(err error) bool {
	return errors.Is(err, dds.ErrStaleSequence) ||
		errors.Is(err, dds.ErrTruncated) ||
		errors.Is(err, dds.ErrInvalidFrame)
}

// publishThreeSamples publishes three samples and returns the frames.
func publishThreeSamples(t *testing.T) [][]byte {
	t.Helper()
	var guid dds.WriterGUID
	for i := range guid {
		guid[i] = byte(i + 11)
	}
	sink := &rosFrameSink{}
	pub, err := dds.NewPublisher(guid, sink)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	first := transports.OutgoingMessage{TopicName: rosTestTopic, Payload: []byte{1, 1}}
	err = pub.Publish(first)
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	second := transports.OutgoingMessage{TopicName: rosTestTopic, Payload: []byte{2, 2}}
	err = pub.Publish(second)
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}
	third := transports.OutgoingMessage{TopicName: rosTestTopic, Payload: []byte{3, 3}}
	err = pub.Publish(third)
	if err != nil {
		t.Fatalf("publish third: %v", err)
	}
	frames := sink.snapshot()
	if len(frames) != 3 {
		t.Fatalf("recorded %d frames, want 3", len(frames))
	}
	return frames
}

// checkStaleDuplicate proves a replayed frame reports ErrStaleSequence.
func checkStaleDuplicate(t *testing.T, recv *dds.DataReceiver, frame []byte) {
	t.Helper()
	_, err := recv.Take(frame)
	if err != nil {
		t.Fatalf("take first: %v", err)
	}
	_, err = recv.Take(frame)
	if err == nil {
		t.Fatalf("duplicate take succeeded, want stale rejection")
	}
	if !errors.Is(err, dds.ErrStaleSequence) {
		t.Fatalf("duplicate take = %v, want ErrStaleSequence", err)
	}
	if !rosIsSkippableSample(err) {
		t.Fatalf("stale error not skippable: %v", err)
	}
}

// checkTruncatedDrop proves a truncated frame is skippable and recoverable.
func checkTruncatedDrop(t *testing.T, recv *dds.DataReceiver, frames [][]byte) {
	t.Helper()
	_, err := recv.Take(frames[1])
	if err != nil {
		t.Fatalf("take after stale: %v", err)
	}
	truncated := frames[2][:2]
	_, err = recv.Take(truncated)
	if err == nil {
		t.Fatalf("truncated take succeeded, want rejection")
	}
	if !errors.Is(err, dds.ErrTruncated) && !errors.Is(err, dds.ErrInvalidFrame) {
		t.Fatalf("truncated take = %v, want ErrTruncated or ErrInvalidFrame", err)
	}
	if !rosIsSkippableSample(err) {
		t.Fatalf("truncated error not skippable: %v", err)
	}
	_, err = recv.Take(frames[2])
	if err != nil {
		t.Fatalf("take after truncated: %v", err)
	}
}

// checkSkippableSentinels pins which errors count as skippable drops.
func checkSkippableSentinels(t *testing.T) {
	t.Helper()
	if rosIsSkippableSample(nil) {
		t.Fatalf("nil error reported skippable, want false")
	}
	if rosIsSkippableSample(errors.New("boom")) {
		t.Fatalf("generic error reported skippable, want false")
	}
	if rosIsSkippableSample(context.Canceled) {
		t.Fatalf("context canceled reported skippable, want false")
	}
}

// TestROSDuplicateStaleDroppedNotFatal checks drops never end the stream.
//
// It publishes three samples through an in memory sink, takes the first,
// replays it as a stale duplicate, truncates the third, and proves the
// receiver still accepts the next live sample after each drop. It also
// pins which sentinels count as skippable so new error kinds cannot
// silently become fatal.
func TestROSDuplicateStaleDroppedNotFatal(t *testing.T) {
	t.Parallel()
	frames := publishThreeSamples(t)
	recv := dds.NewDataReceiver()
	checkStaleDuplicate(t, recv, frames[0])
	checkTruncatedDrop(t, recv, frames)
	checkSkippableSentinels(t)
}
