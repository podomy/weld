// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package tagging holds data-tag propagation and tag-gated decisions.
package tagging

import (
	"errors"
	"fmt"
	"slices"
)

// ErrTagDenied reports a tag-gated denial.
var ErrTagDenied = errors.New("tagging: denied by data tags")

// Plugin evaluates data tags against permissions rule tags.
type Plugin struct{}

// New returns a tagging plugin.
func New() *Plugin {
	return &Plugin{}
}

// Allows reports whether endpoint tags satisfy one rule.
//
// An empty rule set allows any endpoint. A non-empty rule set requires
// at least one common tag. Nil plugins allow, so tagging never grants
// what permissions deny; it only adds denials.
func (p *Plugin) Allows(endpointTags, ruleTags []string) bool {
	if len(ruleTags) == 0 {
		return true
	}

	for _, want := range ruleTags {
		if slices.Contains(endpointTags, want) {
			return true
		}
	}

	return false
}

// Check denies when endpoint tags miss every rule tag.
func (p *Plugin) Check(endpointTags, ruleTags []string, identity, topic string) error {
	if p.Allows(endpointTags, ruleTags) {
		return nil
	}

	return fmt.Errorf("tagging: identity=%s topic=%s endpoint tags %v miss rule tags %v: %w", identity, topic, endpointTags, ruleTags, ErrTagDenied)
}

// Propagate copies endpoint tags for storage on discovery records.
func (p *Plugin) Propagate(tags []string) []string {
	if len(tags) == 0 {
		return nil
	}

	out := make([]string, len(tags))
	copy(out, tags)

	return out
}
