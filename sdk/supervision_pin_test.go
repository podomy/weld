// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/podomy/weld/sdk"
)

// TestSerialAttachGenerationChecked pins that a late Attach during
// Configure still runs the missed Configure before insertion.
func TestSerialAttachGenerationChecked(t *testing.T) {
	group, err := sdk.NewSerialGroup("attach-race")
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	configured := &atomic.Int32{}
	block := make(chan struct{})
	late := sdk.SerialEntity{
		Name: "late",
		Configure: func() error {
			configured.Add(1)
			return nil
		},
	}
	early := sdk.SerialEntity{
		Name: "early",
		Configure: func() error {
			<-block
			return nil
		},
	}
	err = group.Attach(early)
	if err != nil {
		t.Fatalf("attach early: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	var configureErr error
	go func() {
		defer wg.Done()
		configureErr = group.Configure()
	}()
	done := make(chan error, 1)
	go func() {
		close(block)
		done <- group.Attach(late)
	}()
	attachErr := <-done
	wg.Wait()
	if configureErr != nil {
		t.Fatalf("configure: %v", configureErr)
	}
	if attachErr != nil {
		t.Fatalf("late attach: %v", attachErr)
	}
	if configured.Load() != 1 {
		t.Fatalf("late configure runs = %d, want 1", configured.Load())
	}
	if !group.IsConfigured() {
		t.Fatalf("group not configured after race")
	}
	if group.Len() != 2 {
		t.Fatalf("len = %d, want 2", group.Len())
	}
}

// TestSerialDetachExactlyOnce pins that concurrent Detach runs Deactivate
// exactly once.
func TestSerialDetachExactlyOnce(t *testing.T) {
	group, err := sdk.NewSerialGroup("detach-once")
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	calls := &atomic.Int32{}
	err = group.Attach(sdk.SerialEntity{
		Name: "victim",
		Deactivate: func() error {
			calls.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	err = group.Configure()
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	err = group.Activate()
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	succeeded := runDetachRacers(group, "victim", 8)
	if succeeded != 1 {
		t.Fatalf("detach successes = %d, want exactly 1", succeeded)
	}
	if calls.Load() != 1 {
		t.Fatalf("deactivate runs = %d, want 1", calls.Load())
	}
	if group.Len() != 0 {
		t.Fatalf("len = %d, want 0", group.Len())
	}
}

// runDetachRacers runs racers concurrent Detach calls and counts successes.
func runDetachRacers(group *sdk.SerialGroup, name string, racers int) int {
	var wg sync.WaitGroup
	wg.Add(racers)
	errs := make([]error, racers)
	for i := range racers {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = group.Detach(name)
		}(i)
	}
	wg.Wait()
	return countDetachSuccesses(errs)
}

// countDetachSuccesses counts nil errors.
func countDetachSuccesses(errs []error) int {
	succeeded := 0
	for _, err := range errs {
		if err == nil {
			succeeded++
		}
	}
	return succeeded
}

// TestSerialConfigurePartialVisible pins the fail-fast partial contract:
// a failing Configure leaves the flag unset with earlier entities driven.
func TestSerialConfigurePartialVisible(t *testing.T) {
	group, err := sdk.NewSerialGroup("partial")
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	ran := map[string]int{}
	failOnce := true
	attachPartial := func(name string) {
		t.Helper()
		n := name
		err := group.Attach(sdk.SerialEntity{
			Name: n,
			Configure: func() error {
				ran[n]++
				if n == "middle" && failOnce {
					failOnce = false
					return errors.New("boom middle")
				}
				return nil
			},
		})
		if err != nil {
			t.Fatalf("attach %s: %v", n, err)
		}
	}
	attachPartial("first")
	attachPartial("middle")
	attachPartial("last")
	err = group.Configure()
	if err == nil {
		t.Fatalf("first configure must fail")
	}
	checkPartialRuns(t, ran, group, false)
	err = group.Configure()
	if err != nil {
		t.Fatalf("retry configure: %v", err)
	}
	checkPartialRuns(t, ran, group, true)
}

// checkPartialRuns pins visible partial state before retry and configured
// state after retry.
func checkPartialRuns(t *testing.T, ran map[string]int, group *sdk.SerialGroup, wantConfigured bool) {
	t.Helper()
	if wantConfigured {
		if !group.IsConfigured() {
			t.Fatalf("not configured after retry")
		}
		return
	}
	if group.IsConfigured() {
		t.Fatalf("configured after partial failure")
	}
	if ran["first"] != 1 {
		t.Fatalf("first runs = %d, want 1", ran["first"])
	}
	if ran["middle"] != 1 {
		t.Fatalf("middle runs = %d, want 1", ran["middle"])
	}
	if ran["last"] != 0 {
		t.Fatalf("last runs = %d, want 0", ran["last"])
	}
}

// waitSerialSignal waits for a start signal with a loud timeout.
func waitSerialSignal(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not start", label)
	}
}

// assertSerialBlocked pins that done stays pending for a short window,
// proving serialization instead of concurrent completion.
func assertSerialBlocked(t *testing.T, done <-chan error, label string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s completed concurrently: %v", label, err)
	case <-time.After(100 * time.Millisecond):
	}
}

// waitSerialDone waits for lifecycle completion with a loud timeout.
func waitSerialDone(t *testing.T, done <-chan error, label string) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not complete", label)
	}
}

// TestSerialActivateVsDetachSerialized pins that Activate and Detach
// serialize on opMu: Detach blocks while Activate runs callbacks instead
// of removing concurrently.
func TestSerialActivateVsDetachSerialized(t *testing.T) {
	group, err := sdk.NewSerialGroup("activate-detach")
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	err = group.Attach(sdk.SerialEntity{
		Name: "blocker",
		Activate: func() error {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatalf("attach blocker: %v", err)
	}
	err = group.Attach(sdk.SerialEntity{
		Name: "victim",
		Activate: func() error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("attach victim: %v", err)
	}
	err = group.Configure()
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	activateDone := make(chan error, 1)
	go func() {
		activateDone <- group.Activate()
	}()
	waitSerialSignal(t, started, "activate")
	detachDone := make(chan error, 1)
	go func() {
		detachDone <- group.Detach("victim")
	}()
	assertSerialBlocked(t, detachDone, "detach during activate")
	close(release)
	waitSerialDone(t, activateDone, "activate")
	waitSerialDone(t, detachDone, "detach after activate")
	if group.Len() != 1 {
		t.Fatalf("len = %d, want 1 after detach", group.Len())
	}
	if !group.IsActive() {
		t.Fatalf("group not active after serialized activate")
	}
}

// TestSerialShutdownDuringDetachSerialized pins that Shutdown serializes
// with Detach and excludes the detaching name: Shutdown blocks while
// Detach runs Deactivate, then completes without running Shutdown on the
// detached victim.
func TestSerialShutdownDuringDetachSerialized(t *testing.T) {
	group, err := sdk.NewSerialGroup("shutdown-detach")
	if err != nil {
		t.Fatalf("new group: %v", err)
	}
	release := make(chan struct{})
	deactivateStarted := make(chan struct{}, 1)
	victimShutdowns := &atomic.Int32{}
	keeperShutdowns := &atomic.Int32{}
	err = group.Attach(sdk.SerialEntity{
		Name: "keeper",
		Shutdown: func() error {
			keeperShutdowns.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("attach keeper: %v", err)
	}
	err = group.Attach(sdk.SerialEntity{
		Name: "victim",
		Deactivate: func() error {
			select {
			case deactivateStarted <- struct{}{}:
			default:
			}
			<-release
			return nil
		},
		Shutdown: func() error {
			victimShutdowns.Add(1)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("attach victim: %v", err)
	}
	err = group.Configure()
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	err = group.Activate()
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	detachDone := make(chan error, 1)
	go func() {
		detachDone <- group.Detach("victim")
	}()
	waitSerialSignal(t, deactivateStarted, "detach deactivate")
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownDone <- group.Shutdown()
	}()
	assertSerialBlocked(t, shutdownDone, "shutdown during detach")
	close(release)
	waitSerialDone(t, detachDone, "detach")
	waitSerialDone(t, shutdownDone, "shutdown after detach")
	if victimShutdowns.Load() != 0 {
		t.Fatalf("victim shutdown runs = %d, want 0 excluded", victimShutdowns.Load())
	}
	if keeperShutdowns.Load() != 1 {
		t.Fatalf("keeper shutdown runs = %d, want 1", keeperShutdowns.Load())
	}
	if group.Len() != 1 {
		t.Fatalf("len = %d, want 1 after detach", group.Len())
	}
}
