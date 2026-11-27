// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package wtime

import (
	clock "github.com/podomy/weld/internal/time"
)

// Fake is a deterministic manual clock for chaos tests.
type Fake = clock.Fake

// ProgrammedJump is one one-shot fake time jump.
type ProgrammedJump = clock.ProgrammedJump

// NewFake builds a running fake at startNanos with rate one.
func NewFake(startNanos int64) *Fake {
	return clock.NewFake(startNanos)
}
