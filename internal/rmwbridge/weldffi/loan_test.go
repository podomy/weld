// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package main

import (
	"testing"
)

// loanTestPubSub creates one pub plus one sub on /loan.
func loanTestPubSub(t *testing.T) (int64, int64) {
	t.Helper()
	bh := weldTestBridge(t)
	pub, code := weldGoPubCreate(bh, "n", "/loan")
	requireWeldCode(t, code, 0, "pub_create")
	sub, code := weldGoSubCreate(bh, "n", "/loan")
	requireWeldCode(t, code, 0, "sub_create")
	return pub, sub
}

// TestWeldLoanSharesHandle pins FFI loan handoff moves handles with no copy.
func TestWeldLoanSharesHandle(t *testing.T) {
	pub, sub := loanTestPubSub(t)
	const ptr uint64 = 0xabcdef01
	const members uint64 = 0x10fedcba
	code := weldGoLoanPublish(pub, ptr, members)
	requireWeldCode(t, code, 0, "loan_publish")
	gotPtr, gotMembers, takeCode := weldGoLoanTakePoll(sub)
	requireWeldCode(t, takeCode, 0, "loan_take_poll")
	if gotPtr != ptr || gotMembers != members {
		t.Fatalf("loan take = (%x, %x), want (%x, %x)", gotPtr, gotMembers, ptr, members)
	}
	gid, seq, icode := weldGoSubTakeInfo(sub)
	requireWeldCode(t, icode, 0, "take_info")
	if seq != 1 {
		t.Fatalf("loan take seq = %d, want 1", seq)
	}
	if gid == ([16]byte{}) {
		t.Fatalf("loan take gid is zero, want writer identity")
	}
	_, _, takeCode = weldGoLoanTakePoll(sub)
	requireWeldCode(t, takeCode, 1, "second loan_take_poll (exactly once)")
}

// TestWeldLoanValidation pins handle plus zero-handle rejection.
func TestWeldLoanValidation(t *testing.T) {
	pub, sub := loanTestPubSub(t)
	code := weldGoLoanPublish(pub, 0, 1)
	requireWeldCode(t, code, -1, "zero loan addr")
	code = weldGoLoanPublish(pub, 1, 0)
	requireWeldCode(t, code, -1, "zero members addr")
	code = weldGoLoanPublish(999999, 1, 2)
	requireWeldCode(t, code, -2, "unknown pub")
	_, _, code = weldGoLoanTakePoll(999999)
	requireWeldCode(t, code, -2, "unknown sub")
	_, _, code = weldGoLoanTakePoll(sub)
	requireWeldCode(t, code, 1, "empty loan_take_poll")
}
