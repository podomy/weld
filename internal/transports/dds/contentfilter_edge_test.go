// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"testing"
)

// TestCompareFilterOpRejectsNonComparison pins error returns over panics.
func TestCompareFilterOpRejectsNonComparison(t *testing.T) {
	t.Parallel()

	if _, err := compareFilterOp(tokAnd, 0); err == nil {
		t.Fatalf("logic op succeeded, want comparison error")
	}

	if _, err := compareFilterOp(tokLike, 1); err == nil {
		t.Fatalf("like op succeeded, want comparison error")
	}

	matched, err := compareFilterOp(tokEq, 0)
	if err != nil {
		t.Fatalf("eq op: %v", err)
	}

	if !matched {
		t.Fatalf("eq zero compare = false, want true")
	}
}
