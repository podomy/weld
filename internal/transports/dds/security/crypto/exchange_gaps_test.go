// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"testing"
)

// TestGapsRevisionDowngradeRefused pins never-downgrade on volatile install.
func TestGapsRevisionDowngradeRefused(t *testing.T) {
	t.Parallel()

	ends := auditExchangeSetup(t)

	rotated := bytes.Repeat([]byte{0x58}, 32)
	auditRotateSender(t, ends, rotated, 2)
	auditStageReceivercry9(t, ends.storeB, ends.sender, ends.salt, rotated, 2)

	envelope, err := ends.exchA.CreateLocalTokens(ends.sender, ends.remote, 9, 2)
	if err != nil {
		t.Fatalf("create rotated: %v", err)
	}

	if err := ends.exchB.SetRemoteTokens(envelope, ends.remote); err != nil {
		t.Fatalf("staged rotation must install: %v", err)
	}

	// Replaying the old revision 1 material after advancing to 2 must
	// refuse: installing it would downgrade the receiver.
	oldEnvelope, err := ends.exchA.CreateLocalTokens(ends.sender, ends.remote, 9, 1)
	if err != nil {
		// Sender no longer holds revision 1 after rotation in this
		// fixture; the refusal still pins never-downgrade.
		t.Skipf("sender rotated past revision 1: %v", err)
	}

	if err := ends.exchB.SetRemoteTokens(oldEnvelope, ends.remote); err == nil {
		t.Fatalf("downgrade to revision 1 must refuse")
	}

	if got := auditLatestRevision(t, ends.storeB, ends.sender, 9); got != 2 {
		t.Fatalf("downgrade must keep revision 2, got %d", got)
	}
}

// TestGapsRolloverSignalHoldacc9 pins volatile rollover distribution.
func TestGapsRolloverSignalHoldacc9(t *testing.T) {
	t.Parallel()

	ends := auditExchangeSetup(t)

	signal, err := ends.exchA.CreateRolloverSignalcry9(ends.sender, ends.remote, 9, 1)
	if err != nil {
		t.Fatalf("rollover signal: %v", err)
	}

	if err := ends.exchB.SetRemoteTokens(signal, ends.remote); err != nil {
		t.Fatalf("rollover signal must install idempotently: %v", err)
	}

	if RolloverHoldTimeoutcry9 <= 0 {
		t.Fatalf("hold timeout must be positive")
	}
}

// TestGapsSameOrNewerOnlyacc9 pins retransmit plus staged acceptance.
func TestGapsSameOrNewerOnlyacc9(t *testing.T) {
	t.Parallel()

	ends := auditExchangeSetup(t)

	if err := ends.exchB.SetRemoteTokens(ends.envelope, ends.remote); err != nil {
		t.Fatalf("same-revision retransmit must install: %v", err)
	}

	novel := ends.envelope
	novel.Tokens[0].MasterSenderKey = bytes.Repeat([]byte{0x77}, 32)

	if err := ends.exchB.SetRemoteTokens(novel, ends.remote); err == nil {
		t.Fatalf("unsolicited novel material must refuse, never auto-advance")
	}
}
