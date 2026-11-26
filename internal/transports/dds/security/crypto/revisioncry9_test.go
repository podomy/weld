// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"testing"
)

// TestRevisioncry9StagedOnly pins staged-only receiver acceptance.
func TestRevisioncry9StagedOnly(t *testing.T) {
	t.Parallel()

	ends := auditExchangeSetup(t)

	// Retransmit of the staged revision 1 reinstalls identically.
	err := ends.exchB.SetRemoteTokens(ends.envelope, ends.remote)
	if err != nil {
		t.Fatalf("retransmit revision 1 must reinstall: %v", err)
	}

	if got := auditLatestRevision(t, ends.storeB, ends.sender, 9); got != 1 {
		t.Fatalf("retransmit must keep revision 1, got %d", got)
	}

	// Novel bytes with a different salt must refuse without bumping.
	novel := ends.envelope
	novel.Tokens[0].MasterSalt = bytes.Repeat([]byte{0x99}, 32)

	err = ends.exchB.SetRemoteTokens(novel, ends.remote)
	if err == nil {
		t.Fatalf("novel unsolicited bytes must refuse")
	}

	if got := auditLatestRevision(t, ends.storeB, ends.sender, 9); got != 1 {
		t.Fatalf("novel bytes must never bump latest, got %d", got)
	}
}
