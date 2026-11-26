// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package main

/*
#include <stdlib.h>
*/
import "C"

// weld_ffi_loan_publish enqueues one in-process loan handle without copying.
//
// loanAddr is the opaque C message pointer and membersAddr is the opaque C
// introspection members pointer, both valid only in the publishing process.
// The bridge never dereferences either and never copies payload bytes;
// only the handle values move, so this path is zero-copy where the wire
// path copies (CDR encode plus channel copy plus CDR decode). It returns 0
// when queued (caller transfers ownership to the queue and must not free),
// 1 when not queued because no subscription matches or the per-topic queue
// is full (caller keeps the wire copy path and frees the buffer, so
// delivery still succeeds via copying without leaking), -1 for zero
// handles, -2 for an unknown publisher, and -3 for publish errors. It never
// panics.
//
//export weld_ffi_loan_publish
func weld_ffi_loan_publish(handle C.longlong, loanAddr C.ulonglong, membersAddr C.ulonglong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	if uint64(loanAddr) == 0 || uint64(membersAddr) == 0 {
		return C.int(weldCodeErrNull)
	}
	weldMu.Lock()
	e, ok := weldPubs[key]
	weldMu.Unlock()
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	queued, err := e.pub.PublishLoan(uint64(loanAddr), uint64(membersAddr))
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	if !queued {
		return C.int(weldCodeEmpty)
	}
	return C.int(weldCodeOK)
}

// weld_ffi_loan_take_poll dequeues one in-process loan handle without copying.
//
// loanAddrOut receives the opaque C message pointer and membersAddrOut
// receives the opaque C introspection members pointer, both to be cast back
// in C with no payload byte copy, so this path is zero-copy where the wire
// take copies (channel copy plus CDR decode into a fresh buffer). It returns
// 0 with handles on data, 1 when no loan is queued (caller falls back to
// the copying wire take), -1 for NULL out pointers, -2 for an unknown
// subscription, and -3 for take errors. A successful dequeue records the
// take-info side channel plus deadline delivery like a wire take. It never
// panics.
//
//export weld_ffi_loan_take_poll
func weld_ffi_loan_take_poll(handle C.longlong, loanAddrOut *C.ulonglong, membersAddrOut *C.ulonglong) (rc C.int) {
	defer func() {
		r := recover()
		if r != nil {
			rc = C.int(weldCodeErrInternal)
		}
	}()
	key := int64(handle)
	if key <= 0 {
		return C.int(weldCodeErrHandle)
	}
	if loanAddrOut == nil || membersAddrOut == nil {
		return C.int(weldCodeErrNull)
	}
	weldMu.Lock()
	e, ok := weldSubs[key]
	weldMu.Unlock()
	if !ok {
		return C.int(weldCodeErrHandle)
	}
	ptr, members, gid, seq, dequeued, err := e.sub.TakeLoan()
	if err != nil {
		return C.int(weldCodeErrInternal)
	}
	if !dequeued {
		return C.int(weldCodeEmpty)
	}
	*loanAddrOut = C.ulonglong(ptr)
	*membersAddrOut = C.ulonglong(members)
	weldRecordSubTakeInfo(key, gid, seq)
	weldNoteSubDelivery(key)
	return C.int(weldCodeOK)
}

// weldGoLoanPublish enqueues one loan through weld_ffi_loan_publish.
//
// It uses only Go builtin types so loan tests stay free of cgo while
// exercising the exact export code path, including handle validation.
func weldGoLoanPublish(handle int64, loanAddr uint64, membersAddr uint64) int32 {
	rc := weld_ffi_loan_publish(C.longlong(handle), C.ulonglong(loanAddr), C.ulonglong(membersAddr))
	return int32(rc)
}

// weldGoLoanTakePoll dequeues one loan through weld_ffi_loan_take_poll.
//
// It returns the loan plus members handles and the export code. Code 1
// reports no queued loan. It uses only Go builtin types so loan tests stay
// free of cgo while exercising the exact export code path.
func weldGoLoanTakePoll(handle int64) (uint64, uint64, int32) {
	var loanOut C.ulonglong
	var membersOut C.ulonglong
	rc := weld_ffi_loan_take_poll(C.longlong(handle), &loanOut, &membersOut)
	if rc != C.int(weldCodeOK) {
		return 0, 0, int32(rc)
	}
	return uint64(loanOut), uint64(membersOut), int32(weldCodeOK)
}
