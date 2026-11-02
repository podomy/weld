// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package gates_test

import (
	"time"

	"github.com/podomy/weld/testbed/live/helpers"
)

// Gates allowlisted product skips.
//
// Static declarations for the two gate interop gaps. The skipping tests
// keep only NoteProductSkip plus AssertNoExcessSkips; registration lives
// here so a missing allowance fails loudly instead of self-approving.
// Reasons and 2027-05-01 expiry are verbatim from the original call sites.
func init() {
	expiry := time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC)
	helpers.RegisterAllowlistedSkip("TestGate2ForwardEchoSeesTalker", "weld echo misses user DATA but echo ok, known user DATA gap forward", expiry)
	helpers.RegisterAllowlistedSkip("TestGate2ReverseEchoSeesPub", "ros echo misses user DATA but pub ok, known user DATA gap reverse", expiry)
}
