// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"testing"
)

// TestRollovercry9Pending pins the session rollover signal.
func TestRollovercry9Pending(t *testing.T) {
	t.Parallel()

	sender := auditSender(0x64)
	store := NewKeyStoreWithLimit(4)

	err := store.Store(MasterRecord{
		Sender: sender, KeyID: 13, Revision: 1, Kind: TransformAES256GCM,
		Salt: bytes.Repeat([]byte{0x41}, 32), SenderKey: bytes.Repeat([]byte{0x42}, 32), Session: 1,
	})
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	pending, err := store.RolloverPendingcry9(sender, 13, 1)
	if err != nil {
		t.Fatalf("rollover query: %v", err)
	}

	if pending {
		t.Fatalf("fresh key must not report pending rollover")
	}

	err = store.ObserveBlocks(sender, 13, 1, 4)
	if err != nil {
		t.Fatalf("observe: %v", err)
	}

	pending, err = store.RolloverPendingcry9(sender, 13, 1)
	if err != nil {
		t.Fatalf("rollover query after bound: %v", err)
	}

	if !pending {
		t.Fatalf("key at bound must report pending rollover")
	}

	session, _, err := store.AllocateSuffix(sender, 13, 1)
	if err != nil {
		t.Fatalf("allocate: %v", err)
	}

	if session != 2 {
		t.Fatalf("allocate at bound must roll to session 2, got %d", session)
	}

	pending, err = store.RolloverPendingcry9(sender, 13, 1)
	if err != nil {
		t.Fatalf("rollover query after roll: %v", err)
	}

	if pending {
		t.Fatalf("rolled session must clear pending")
	}
}
