// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"bytes"
	"testing"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// TestEnablecry9RevisionTrigger pins staged revision primitives.
func TestEnablecry9RevisionTrigger(t *testing.T) {
	t.Parallel()

	store := NewKeyStore()
	sender := auditSender(0x65)

	factory, err := NewKeyFactory(store)
	if err != nil {
		t.Fatalf("new factory: %v", err)
	}

	material := tokens.KeyMaterial{
		TransformationKind: TransformAES256GCM,
		MasterSalt:         bytes.Repeat([]byte{0x41}, 32),
		SenderKeyID:        14,
		MasterSenderKey:    bytes.Repeat([]byte{0x42}, 32),
	}

	err = factory.Revise(sender, 2, material)
	if err != nil {
		t.Fatalf("revise: %v", err)
	}

	if !factory.HasPendingcry9(sender, 14, 2) {
		t.Fatalf("pending must report staged revision")
	}

	if factory.HasPendingcry9(sender, 14, 3) {
		t.Fatalf("unstaged revision must not report pending")
	}

	err = factory.Activate(sender, 14, 2)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}

	if factory.HasPendingcry9(sender, 14, 2) {
		t.Fatalf("pending must clear after activate")
	}

	stored, err := store.Lookup(sender, 14, 2)
	if err != nil {
		t.Fatalf("lookup staged revision: %v", err)
	}

	if !bytes.Equal(stored.SenderKey, material.MasterSenderKey) {
		t.Fatalf("activated revision must carry staged material")
	}

	err = factory.Activate(sender, 14, 3)
	if err == nil {
		t.Fatalf("activate without revise must fail closed")
	}
}
