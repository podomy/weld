// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package tagging

import (
	"errors"
	"testing"
)

// TestAuditTagGating pins tag-gated allow and deny outcomes.
func TestAuditTagGating(t *testing.T) {
	t.Parallel()

	plugin := New()

	if !plugin.Allows([]string{"red"}, nil) {
		t.Fatalf("empty rule tags must allow any endpoint")
	}

	if !plugin.Allows([]string{"red"}, []string{"red", "blue"}) {
		t.Fatalf("shared tag must allow")
	}

	if plugin.Allows([]string{"green"}, []string{"red"}) {
		t.Fatalf("missed tags must deny")
	}

	if plugin.Allows(nil, []string{"red"}) {
		t.Fatalf("empty endpoint tags must deny a tagged rule")
	}

	err := plugin.Check([]string{"green"}, []string{"red"}, "id", "rt/chatter")
	if !errors.Is(err, ErrTagDenied) {
		t.Fatalf("missed tags must deny with ErrTagDenied, got %v", err)
	}

	err = plugin.Check([]string{"red"}, []string{"red"}, "id", "rt/chatter")
	if err != nil {
		t.Fatalf("shared tag must allow, got %v", err)
	}

	if got := plugin.Propagate([]string{"a", "b"}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("propagate must copy tags, got %v", got)
	}

	if plugin.Propagate(nil) != nil {
		t.Fatalf("propagate of empty must stay nil")
	}
}
