// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// fakeEchoBackend is a Backend stub with a queued TakeBytes stream.
type fakeEchoBackend struct {
	// msgs holds queued messages returned in order.
	msgs []transports.IncomingMessage
	// lost holds the gap loss count reported through LostCount.
	lost uint64
}

// PublishBytes accepts one message without sending.
func (f *fakeEchoBackend) PublishBytes(_ context.Context, _ transports.OutgoingMessage) error {
	return nil
}

// TakeBytes returns one queued message or canceled when drained.
func (f *fakeEchoBackend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, err
	}

	if len(f.msgs) == 0 {
		return transports.IncomingMessage{}, context.Canceled
	}

	next := f.msgs[0]
	f.msgs = f.msgs[1:]

	return next, nil
}

// JoinDiscovery records a successful join without dialing.
func (f *fakeEchoBackend) JoinDiscovery(_ context.Context, _ transports.DiscoveryInfo) error {
	return nil
}

// ReportLiveliness accepts one report without tracking.
func (f *fakeEchoBackend) ReportLiveliness(_ context.Context, _ transports.LivelinessReport) error {
	return nil
}

// Close releases the stub without work.
func (f *fakeEchoBackend) Close() error {
	return nil
}

// LostCount returns the stub gap loss count for summary tests.
func (f *fakeEchoBackend) LostCount() uint64 {
	return f.lost
}

// echoValidPayload encodes text as String bytes for echo tests.
func echoValidPayload(t *testing.T, text string) []byte {
	t.Helper()

	payload, err := encodeStringPayload(text)
	if err != nil {
		t.Fatalf("encode string: %v", err)
	}

	return payload
}

// TestEchoLoopFiltersOtherTopic checks foreign topics drop without error.
func TestEchoLoopFiltersOtherTopic(t *testing.T) {
	hostile := []byte{0, 1, 0, 0, 0xFF, 0xFF, 0xFF, 0x7F}
	backend := &fakeEchoBackend{msgs: []transports.IncomingMessage{
		{TopicName: "/other", TypeName: stringTypeName, Payload: echoValidPayload(t, "noise")},
		{TopicName: "/builtin_time_zero", TypeName: "builtin__msg__Time", Payload: hostile},
		{TopicName: "/chatter", TypeName: stringTypeName, Payload: echoValidPayload(t, "hello")},
	}}

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := echoLoop(context.Background(), backend, "/chatter", &stdout, &stderr)
	if err != nil {
		t.Fatalf("echo loop: %v", err)
	}

	if stdout.String() != "hello\n" {
		t.Fatalf("stdout = %q, want %q", stdout.String(), "hello\n")
	}
}

// TestEchoLoopDropsMismatchedType checks wrong type on same topic drops.
func TestEchoLoopDropsMismatchedType(t *testing.T) {
	hostile := []byte{0, 1, 0, 0, 0xFF, 0xFF, 0xFF, 0x7F}
	backend := &fakeEchoBackend{msgs: []transports.IncomingMessage{
		{TopicName: "/chatter", TypeName: "other__msg__Other", Payload: hostile},
		{TopicName: "/chatter", TypeName: stringTypeName, Payload: echoValidPayload(t, "ok")},
	}}

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := echoLoop(context.Background(), backend, "/chatter", &stdout, &stderr)
	if err != nil {
		t.Fatalf("echo loop: %v", err)
	}

	if stdout.String() != "ok\n" {
		t.Fatalf("stdout = %q, want %q", stdout.String(), "ok\n")
	}
}

// TestEchoLoopFailsCorruptSameType checks corrupt same type still errors.
func TestEchoLoopFailsCorruptSameType(t *testing.T) {
	hostile := []byte{0, 1, 0, 0, 0xFF, 0xFF, 0xFF, 0x7F}
	backend := &fakeEchoBackend{msgs: []transports.IncomingMessage{
		{TopicName: "/chatter", TypeName: stringTypeName, Payload: hostile},
	}}

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := echoLoop(context.Background(), backend, "/chatter", &stdout, &stderr)
	if err == nil {
		t.Fatal("corrupt same type accepted")
	}

	if !strings.HasPrefix(err.Error(), "weld: decode message") {
		t.Fatalf("error = %q, want weld: decode message prefix", err.Error())
	}

	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want empty on decode error", stdout.String())
	}
}

// TestEchoLoopSummaryReportsDrops checks clean exit reports total drops.
func TestEchoLoopSummaryReportsDrops(t *testing.T) {
	backend := &fakeEchoBackend{msgs: []transports.IncomingMessage{
		{TopicName: "/other", TypeName: stringTypeName, Payload: echoValidPayload(t, "noise")},
		{TopicName: "/chatter", TypeName: "other__msg__Other", Payload: echoValidPayload(t, "noise")},
		{TopicName: "/chatter", TypeName: stringTypeName, Payload: echoValidPayload(t, "ok")},
	}}

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := echoLoop(context.Background(), backend, "/chatter", &stdout, &stderr)
	if err != nil {
		t.Fatalf("echo loop: %v", err)
	}

	if stdout.String() != "ok\n" {
		t.Fatalf("stdout = %q, want %q", stdout.String(), "ok\n")
	}

	got := stderr.String()
	if !strings.Contains(got, "weld: echo summary:") {
		t.Fatalf("stderr = %q, want echo summary line", got)
	}

	if !strings.Contains(got, "dropped 2 messages") {
		t.Fatalf("stderr = %q, want dropped 2 messages", got)
	}

	if !strings.Contains(got, "lost 0 messages") {
		t.Fatalf("stderr = %q, want lost 0 messages", got)
	}
}

// TestEchoLoopSummarySilentWhenClean checks no drops and no loss stays quiet.
func TestEchoLoopSummarySilentWhenClean(t *testing.T) {
	backend := &fakeEchoBackend{msgs: []transports.IncomingMessage{
		{TopicName: "/chatter", TypeName: stringTypeName, Payload: echoValidPayload(t, "ok")},
	}}

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := echoLoop(context.Background(), backend, "/chatter", &stdout, &stderr)
	if err != nil {
		t.Fatalf("echo loop: %v", err)
	}

	if stdout.String() != "ok\n" {
		t.Fatalf("stdout = %q, want %q", stdout.String(), "ok\n")
	}

	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want silence without drops or loss", stderr.String())
	}
}

// TestEchoLoopSummaryReportsLost checks clean exit reports backend loss.
func TestEchoLoopSummaryReportsLost(t *testing.T) {
	backend := &fakeEchoBackend{
		msgs: []transports.IncomingMessage{
			{TopicName: "/chatter", TypeName: stringTypeName, Payload: echoValidPayload(t, "ok")},
		},
		lost: 3,
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	err := echoLoop(context.Background(), backend, "/chatter", &stdout, &stderr)
	if err != nil {
		t.Fatalf("echo loop: %v", err)
	}

	got := stderr.String()
	if !strings.Contains(got, "weld: echo summary:") {
		t.Fatalf("stderr = %q, want echo summary line", got)
	}

	if !strings.Contains(got, "dropped 0 messages") {
		t.Fatalf("stderr = %q, want dropped 0 messages", got)
	}

	if !strings.Contains(got, "lost 3 messages") {
		t.Fatalf("stderr = %q, want lost 3 messages", got)
	}
}

// fakeEchoDiscoveryBackend is a Backend stub with controllable discovery generation.
type fakeEchoDiscoveryBackend struct {
	// mu guards gen for the bump goroutine in the wait test.
	mu sync.Mutex
	// gen holds the current discovery generation.
	gen uint64
}

// PublishBytes accepts one message without sending.
func (f *fakeEchoDiscoveryBackend) PublishBytes(_ context.Context, _ transports.OutgoingMessage) error {
	return nil
}

// TakeBytes reports no message without blocking.
func (f *fakeEchoDiscoveryBackend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, err
	}

	return transports.IncomingMessage{}, context.Canceled
}

// JoinDiscovery records a successful join without dialing.
func (f *fakeEchoDiscoveryBackend) JoinDiscovery(_ context.Context, _ transports.DiscoveryInfo) error {
	return nil
}

// ReportLiveliness accepts one report without tracking.
func (f *fakeEchoDiscoveryBackend) ReportLiveliness(_ context.Context, _ transports.LivelinessReport) error {
	return nil
}

// Close releases the stub without work.
func (f *fakeEchoDiscoveryBackend) Close() error {
	return nil
}

// DiscoveryGeneration returns the current stub generation.
func (f *fakeEchoDiscoveryBackend) DiscoveryGeneration() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.gen
}

// setGen replaces the stub generation.
func (f *fakeEchoDiscoveryBackend) setGen(gen uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.gen = gen
}

// TestWaitForEchoDiscoveryEarlyExit checks a generation bump ends the bounded wait.
func TestWaitForEchoDiscoveryEarlyExit(t *testing.T) {
	prevInterval := echoDiscoveryIntervalVar
	prevTimeout := echoDiscoveryTimeoutVar
	t.Cleanup(func() {
		echoDiscoveryIntervalVar = prevInterval
		echoDiscoveryTimeoutVar = prevTimeout
	})

	echoDiscoveryIntervalVar = time.Millisecond
	echoDiscoveryTimeoutVar = 2 * time.Second

	fb := &fakeEchoDiscoveryBackend{gen: 3}

	go func() {
		time.Sleep(10 * time.Millisecond)
		fb.setGen(4)
	}()

	start := time.Now()
	waitForEchoDiscovery(context.Background(), fb)
	elapsed := time.Since(start)

	if elapsed >= echoDiscoveryTimeoutVar {
		t.Fatalf("discovery wait elapsed %v, want early return before %v", elapsed, echoDiscoveryTimeoutVar)
	}

	if got := fb.DiscoveryGeneration(); got != 4 {
		t.Fatalf("generation = %d, want 4", got)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	start = time.Now()
	waitForEchoDiscovery(canceled, fb)
	elapsed = time.Since(start)

	if elapsed >= echoDiscoveryTimeoutVar {
		t.Fatalf("canceled wait elapsed %v, want fast return before %v", elapsed, echoDiscoveryTimeoutVar)
	}

	echoDiscoveryTimeoutVar = 20 * time.Millisecond
	plain := &fakeEchoBackend{}

	start = time.Now()
	waitForEchoDiscovery(context.Background(), plain)
	elapsed = time.Since(start)

	if elapsed >= time.Second {
		t.Fatalf("viewerless wait elapsed %v, want bounded return", elapsed)
	}
}
