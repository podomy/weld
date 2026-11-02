// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package live_cli

import (
	"time"

	"github.com/podomy/weld/testbed/live/helpers"
)

// CLI allowlisted product skips.
//
// Static declarations for the two CLI interop gaps. The skipping tests
// keep only NoteProductSkip plus AssertNoExcessSkips; registration lives
// here so a missing allowance fails loudly instead of self-approving.
// Reasons and 2027-05-01 expiry are verbatim from the original call sites.
func init() {
	expiry := time.Date(2027, 5, 1, 0, 0, 0, 0, time.UTC)
	helpers.RegisterAllowlistedSkip("TestGateCLIForwardRosTalkerSeenByWeldEcho", "weld echo misses ROS user DATA but echo ok, known user DATA gap forward cli", expiry)
	helpers.RegisterAllowlistedSkip("TestGateCLIReverseWeldPubSeenByRosEcho", "ros echo misses weld user DATA but pub ok, known user DATA gap reverse cli", expiry)
}
