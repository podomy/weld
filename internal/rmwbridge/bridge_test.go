// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"testing"

	"github.com/podomy/weld/internal/rmwbridge"
)

// TestIdentifier pins the RMW_IMPLEMENTATION name.
func TestIdentifier(t *testing.T) {
	if got := rmwbridge.Identifier(); got != "rmw_weld" {
		t.Fatalf("identifier = %q, want rmw_weld", got)
	}
}

// TestVersion pins the bridge version string is non-empty.
func TestVersion(t *testing.T) {
	if got := rmwbridge.Version(); got == "" {
		t.Fatalf("version is empty")
	}
}
