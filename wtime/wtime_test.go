// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package wtime_test

import (
	"testing"
	stdtime "time"

	"github.com/podomy/weld/wtime"
)

// newTestDomain builds a domain for tests and fails on error.
func newTestDomain(t *testing.T, opts wtime.Options) *wtime.Domain {
	t.Helper()

	domain, err := wtime.NewDomain(opts)
	if err != nil {
		t.Fatalf("new domain: %v", err)
	}

	return domain
}

// TestDomainNow checks a fixed wall source serves the active timeline.
func TestDomainNow(t *testing.T) {
	t.Parallel()

	domain := newTestDomain(t, wtime.Options{
		WallFn:   func() int64 { return 123 },
		SteadyFn: func() int64 { return 456 },
	})

	if got := domain.Now(); got != 123 {
		t.Fatalf("now = %d, want 123", got)
	}

	if got := domain.ActiveKind(); got != wtime.KindWall {
		t.Fatalf("active kind = %v, want wall", got)
	}

	// stdlib time stays usable next to wtime with no shadowing.
	_ = stdtime.Second
}

// TestFakeAdvance checks a fake drives manual sim time deterministically.
func TestFakeAdvance(t *testing.T) {
	t.Parallel()

	fake := wtime.NewFake(0)

	domain := newTestDomain(t, wtime.Options{
		WallFn:       func() int64 { return 10 },
		SteadyFn:     func() int64 { return 20 },
		UseSimTime:   true,
		FallbackKind: wtime.KindSteady,
	})

	if domain.SimAvailable() {
		t.Fatalf("sim available before first apply, want unavailable")
	}

	advanced, err := fake.AdvanceSim(1000000)
	if err != nil {
		t.Fatalf("advance sim: %v", err)
	}

	if advanced != 1000000 {
		t.Fatalf("advanced = %d, want 1000000", advanced)
	}

	_, _, err = fake.ApplyTo(domain)
	if err != nil {
		t.Fatalf("apply to domain: %v", err)
	}

	if got := domain.Now(); got != 1000000 {
		t.Fatalf("domain now = %d, want 1000000", got)
	}

	if got := domain.ActiveKind(); got != wtime.KindSim {
		t.Fatalf("active kind = %v, want sim", got)
	}
}
