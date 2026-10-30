// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// fakeLossBackend is a Backend stub with a controllable lost counter.
type fakeLossBackend struct {
	lost uint64
}

// PublishBytes accepts one message without sending.
func (f *fakeLossBackend) PublishBytes(_ context.Context, _ transports.OutgoingMessage) error {
	return nil
}

// TakeBytes reports no message without blocking.
func (f *fakeLossBackend) TakeBytes(_ context.Context) (transports.IncomingMessage, error) {
	return transports.IncomingMessage{}, nil
}

// JoinDiscovery records a successful join without dialing.
func (f *fakeLossBackend) JoinDiscovery(_ context.Context, _ transports.DiscoveryInfo) error {
	return nil
}

// ReportLiveliness accepts one report without tracking.
func (f *fakeLossBackend) ReportLiveliness(_ context.Context, _ transports.LivelinessReport) error {
	return nil
}

// Close releases the stub without work.
func (f *fakeLossBackend) Close() error {
	return nil
}

// LostCount returns the controllable gap loss count.
func (f *fakeLossBackend) LostCount() uint64 {
	return f.lost
}

// TestWithEchoTimeoutZero checks zero means run until interrupt.
func TestWithEchoTimeoutZero(t *testing.T) {
	ctx := context.Background()

	got, cancel := withEchoTimeout(ctx, 0)
	defer cancel()

	_, ok := got.Deadline()
	if ok {
		t.Fatal("zero timeout must not set a deadline")
	}
}

// TestWithEchoTimeoutNegative checks negative means run until interrupt.
//
// The CLI still rejects negative values with exit 2; the helper treats
// them like zero so only the flag layer decides validity.
func TestWithEchoTimeoutNegative(t *testing.T) {
	ctx := context.Background()

	got, cancel := withEchoTimeout(ctx, -5*time.Second)
	defer cancel()

	_, ok := got.Deadline()
	if ok {
		t.Fatal("negative timeout must not set a deadline")
	}
}

// TestWithEchoTimeoutPositive checks a positive delay sets a deadline.
func TestWithEchoTimeoutPositive(t *testing.T) {
	ctx := context.Background()

	got, cancel := withEchoTimeout(ctx, time.Minute)
	defer cancel()

	deadline, ok := got.Deadline()
	if !ok {
		t.Fatal("positive timeout must set a deadline")
	}

	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > time.Minute {
		t.Fatalf("deadline remaining = %v, want within (0, 1m]", remaining)
	}
}

// TestResolvePubText checks flag versus positional message selection.
func TestResolvePubText(t *testing.T) {
	got, err := resolvePubText("hello-flag", nil)
	if err != nil {
		t.Fatalf("flag text: %v", err)
	}

	if got != "hello-flag" {
		t.Fatalf("flag text = %q, want %q", got, "hello-flag")
	}

	got, err = resolvePubText("", []string{"hello-pos"})
	if err != nil {
		t.Fatalf("positional text: %v", err)
	}

	if got != "hello-pos" {
		t.Fatalf("positional text = %q, want %q", got, "hello-pos")
	}

	got, err = resolvePubText("flag-wins", []string{"pos-ignored"})
	if err != nil {
		t.Fatalf("flag wins: %v", err)
	}

	if got != "flag-wins" {
		t.Fatalf("flag wins = %q, want %q", got, "flag-wins")
	}

	_, err = resolvePubText("", nil)
	if err == nil {
		t.Fatal("missing text must fail")
	}

	if !strings.HasPrefix(err.Error(), "weld:") {
		t.Fatalf("missing text error = %q, want weld: prefix", err.Error())
	}

	_, err = resolvePubText("", []string{"one", "two"})
	if err == nil {
		t.Fatal("two positionals must fail")
	}

	if !strings.HasPrefix(err.Error(), "weld:") {
		t.Fatalf("two positionals error = %q, want weld: prefix", err.Error())
	}
}

// TestValidateTopic checks empty and relative names fail.
func TestValidateTopic(t *testing.T) {
	err := validateTopic("")
	if err == nil {
		t.Fatal("empty topic must fail")
	}

	if !strings.HasPrefix(err.Error(), "weld:") {
		t.Fatalf("empty topic error = %q, want weld: prefix", err.Error())
	}

	err = validateTopic("chatter")
	if err == nil {
		t.Fatal("relative topic must fail")
	}

	if !strings.HasPrefix(err.Error(), "weld:") {
		t.Fatalf("relative topic error = %q, want weld: prefix", err.Error())
	}

	err = validateTopic("/chatter")
	if err != nil {
		t.Fatalf("absolute topic: %v", err)
	}
}

// TestOneLine checks errors collapse to the first trimmed line.
func TestOneLine(t *testing.T) {
	got := oneLine(errTestLines("first", "second"))
	if got != "first" {
		t.Fatalf("oneLine = %q, want %q", got, "first")
	}

	got = oneLine(errTestLines("padded  \t", "second"))
	if got != "padded" {
		t.Fatalf("oneLine = %q, want %q", got, "padded")
	}

	got = oneLine(errTestLines("single", ""))
	if got != "single" {
		t.Fatalf("oneLine = %q, want %q", got, "single")
	}
}

// errTestLines joins lines with a newline for oneLine cases.
func errTestLines(first, second string) error {
	if second == "" {
		return errTestString(first)
	}

	return errTestString(first + "\n" + second)
}

// errTestString wraps s as an error without extra context.
func errTestString(s string) error {
	return testStringError(s)
}

// testStringError is a string error for oneLine inputs.
type testStringError string

// Error returns the string error text.
func (e testStringError) Error() string {
	return string(e)
}

// TestLossTrackerThrottle checks counter lines emit at most once per second.
func TestLossTrackerThrottle(t *testing.T) {
	now := time.Now()

	fb := &fakeLossBackend{}

	var buf bytes.Buffer

	tr := newLossTracker(fb, &buf)
	tr.now = func() time.Time {
		return now
	}

	tr.countDrop()
	tr.countDrop()

	if got := strings.Count(buf.String(), "\n"); got != 1 {
		t.Fatalf("rapid drops logged %d lines, want 1", got)
	}

	if !strings.Contains(buf.String(), "weld: dropped") {
		t.Fatalf("drop line = %q, want weld: dropped prefix", buf.String())
	}

	now = now.Add(2 * time.Second)

	tr.countDrop()

	if got := strings.Count(buf.String(), "\n"); got != 2 {
		t.Fatalf("spaced drops logged %d lines, want 2", got)
	}

	var pollBuf bytes.Buffer

	pollBackend := &fakeLossBackend{lost: 5}

	pollTracker := newLossTracker(pollBackend, &pollBuf)
	pollTracker.now = func() time.Time {
		return now
	}

	pollTracker.poll()

	if pollBuf.Len() != 0 {
		t.Fatalf("baseline poll logged %q, want silence", pollBuf.String())
	}

	pollBackend.lost = 7

	pollTracker.poll()

	if got := strings.Count(pollBuf.String(), "\n"); got != 1 {
		t.Fatalf("growth poll logged %d lines, want 1", got)
	}

	pollTracker.poll()

	if got := strings.Count(pollBuf.String(), "\n"); got != 1 {
		t.Fatalf("repeat poll logged %d lines, want 1", got)
	}
}

// TestResolveEchoDurability checks volatile mapping and bad value rejection.
func TestResolveEchoDurability(t *testing.T) {
	cases := []struct {
		name         string
		input        string
		wantVolatile bool
		wantErr      bool
	}{
		{name: "volatile", input: "volatile", wantVolatile: true, wantErr: false},
		{name: "transient local", input: "transient_local", wantVolatile: false, wantErr: false},
		{name: "empty", input: "", wantVolatile: true, wantErr: true},
		{name: "uppercase", input: "VOLATILE", wantVolatile: true, wantErr: true},
		{name: "bogus", input: "bogus", wantVolatile: true, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveEchoDurability(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("resolveEchoDurability(%q) must fail", tc.input)
				}

				if !strings.HasPrefix(err.Error(), "weld:") {
					t.Fatalf("resolveEchoDurability(%q) error = %q, want weld: prefix", tc.input, err.Error())
				}

				return
			}

			if err != nil {
				t.Fatalf("resolveEchoDurability(%q): %v", tc.input, err)
			}

			if got != tc.wantVolatile {
				t.Fatalf("resolveEchoDurability(%q) = %v, want %v", tc.input, got, tc.wantVolatile)
			}
		})
	}
}

// TestEchoDurabilityFlagBogus checks a bad flag value exits 2 before dialing.
func TestEchoDurabilityFlagBogus(t *testing.T) {
	ctx := context.Background()

	var stdout bytes.Buffer

	var stderr bytes.Buffer

	code := runEchoCommand(ctx, []string{"--qos-durability", "bogus"}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("bogus durability exit = %d, want 2", code)
	}

	if !strings.Contains(stderr.String(), "qos-durability") {
		t.Fatalf("bogus durability stderr = %q, want qos-durability hint", stderr.String())
	}

	if stdout.Len() != 0 {
		t.Fatalf("bogus durability stdout = %q, want silence", stdout.String())
	}
}
