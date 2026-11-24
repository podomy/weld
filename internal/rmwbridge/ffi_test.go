// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge

import (
	"context"
	"testing"
	"time"
)

// ffiTestBridge creates and joins one bridge for tests.
func ffiTestBridge(t *testing.T) int64 {
	t.Helper()
	bh, code := ffiGoBridgeNew()
	if code != 0 {
		t.Fatalf("bridge_new = %d, want 0", code)
	}
	code = ffiGoJoin(bh, "ffi-test", 0)
	if code != 0 {
		t.Fatalf("join = %d, want 0", code)
	}
	return bh
}

// requireFFICodeZero fails when code is not zero.
func requireFFICodeZero(t *testing.T, code int32, op string) {
	t.Helper()
	if code != 0 {
		t.Fatalf("%s = %d, want 0", op, code)
	}
}

// requireFFINegative fails when code is not negative.
func requireFFINegative(t *testing.T, code int32, op string) {
	t.Helper()
	if code >= 0 {
		t.Fatalf("%s = %d, want negative", op, code)
	}
}

// checkFFIVersion pins version round trip and null handling.
func checkFFIVersion(t *testing.T) {
	t.Helper()
	got, code := ffiGoVersion()
	requireFFICodeZero(t, code, "version code")
	if got != Version() || got == "" {
		t.Fatalf("version = %q, want %q", got, Version())
	}
	code = ffiGoVersionNull()
	requireFFINegative(t, code, "NULL version buffer")
	code = ffiGoVersionShort()
	requireFFINegative(t, code, "short version buffer")
}

// checkFFIIdentifier pins identifier round trip and null bridge handling.
func checkFFIIdentifier(t *testing.T) {
	t.Helper()
	got, code := ffiGoIdentifier()
	requireFFICodeZero(t, code, "identifier code")
	if got != Identifier() || got == "" {
		t.Fatalf("identifier = %q, want %q", got, Identifier())
	}
	code = ffiGoBridgeNewNull()
	requireFFINegative(t, code, "NULL bridge_new")
	code = ffiGoGuardCreateNull()
	requireFFINegative(t, code, "NULL guard_create")
}

// TestFFIVersionIdentifier pins version and identifier round trip.
func TestFFIVersionIdentifier(t *testing.T) {
	checkFFIVersion(t)
	checkFFIIdentifier(t)
}

// createFFIPubSub creates one publisher and one subscription.
func createFFIPubSub(t *testing.T, bh int64) (int64, int64) {
	t.Helper()
	pubH, code := ffiGoPubCreate(bh, "ffinode", "/ffichatter")
	requireFFICodeZero(t, code, "pub_create")
	subH, code := ffiGoSubCreate(bh, "ffinode", "/ffichatter")
	requireFFICodeZero(t, code, "sub_create")
	return pubH, subH
}

// checkFFIPublishTake pins publish plus non-blocking take_poll.
func checkFFIPublishTake(t *testing.T, pubH int64, subH int64) {
	t.Helper()
	code := ffiGoPubPublish(pubH, []byte("hello"))
	requireFFICodeZero(t, code, "publish")
	data, n, code := ffiGoSubTakePoll(subH, 256)
	requireFFICodeZero(t, code, "take_poll")
	if n != int32(len("hello")) || string(data) != "hello" {
		t.Fatalf("take_poll = %q n = %d, want hello n = 5", string(data), n)
	}
	_, _, code = ffiGoSubTakePoll(subH, 256)
	if code != 1 {
		t.Fatalf("empty take_poll = %d, want 1", code)
	}
}

// checkFFIEmptyPublish pins empty publish roundtrip.
func checkFFIEmptyPublish(t *testing.T, pubH int64, subH int64) {
	t.Helper()
	code := ffiGoPubPublish(pubH, nil)
	requireFFICodeZero(t, code, "empty publish")
	data, n, code := ffiGoSubTakePoll(subH, 256)
	requireFFICodeZero(t, code, "empty take")
	if code != 0 || n != 0 || len(data) != 0 {
		t.Fatalf("empty take = %d n = %d len = %d, want 0 0 0", code, n, len(data))
	}
}

// checkFFIStashedTake pins short buffer failure and stashed retry.
func checkFFIStashedTake(t *testing.T, pubH int64, subH int64) {
	t.Helper()
	code := ffiGoPubPublish(pubH, []byte("0123456789"))
	requireFFICodeZero(t, code, "publish 10 bytes")
	_, _, code = ffiGoSubTakePoll(subH, 2)
	requireFFINegative(t, code, "short take_poll")
	data, n, code := ffiGoSubTakePoll(subH, 256)
	requireFFICodeZero(t, code, "stashed take_poll")
	if string(data) != "0123456789" || n != 10 {
		t.Fatalf("stashed take = %q n = %d, want 0123456789 n = 10", string(data), n)
	}
}

// destroyFFIPubSub pins sub, pub, and bridge destroy.
func destroyFFIPubSub(t *testing.T, bh int64, pubH int64, subH int64) {
	t.Helper()
	code := ffiGoSubDestroy(subH)
	requireFFICodeZero(t, code, "sub_destroy")
	code = ffiGoPubDestroy(pubH)
	requireFFICodeZero(t, code, "pub_destroy")
	code = ffiGoBridgeFree(bh)
	requireFFICodeZero(t, code, "bridge_free")
}

// TestFFIBridgePubSubPoll pins publish plus non-blocking take_poll.
func TestFFIBridgePubSubPoll(t *testing.T) {
	bh := ffiTestBridge(t)
	pubH, subH := createFFIPubSub(t, bh)
	checkFFIPublishTake(t, pubH, subH)
	checkFFIEmptyPublish(t, pubH, subH)
	checkFFIStashedTake(t, pubH, subH)
	destroyFFIPubSub(t, bh, pubH, subH)
}

// TestFFIServiceCallNB pins bounded service call with a Serve responder.
func TestFFIServiceCallNB(t *testing.T) {
	bh := ffiTestBridge(t)
	sh, code := ffiGoServiceCreate(bh, "ffisvc")
	requireFFICodeZero(t, code, "service_create")
	ffiMu.Lock()
	entry, ok := ffiServices[sh]
	ffiMu.Unlock()
	if !ok {
		t.Fatalf("service handle missing from registry")
	}
	svc := entry.svc
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, respond, err := svc.Serve(ctx)
		if err != nil {
			done <- err
			return
		}
		if string(req) != "ping" {
			done <- context.Cause(ctx)
			return
		}
		err = respond([]byte("pong"))
		if err != nil {
			done <- err
			return
		}
		done <- nil
	}()
	resp, n, code := ffiGoServiceCallNB(sh, []byte("ping"), 256)
	requireFFICodeZero(t, code, "service_call_nb")
	if string(resp) != "pong" || n != 4 {
		t.Fatalf("service reply = %q n = %d, want pong n = 4", string(resp), n)
	}
	err := <-done
	if err != nil {
		t.Fatalf("responder failed: %v", err)
	}
	code = ffiGoServiceDestroy(sh)
	requireFFICodeZero(t, code, "service_destroy")
	code = ffiGoBridgeFree(bh)
	requireFFICodeZero(t, code, "bridge_free")
}

// TestFFIGuardTriggerReset pins guard create, trigger, and reset.
func TestFFIGuardTriggerReset(t *testing.T) {
	gh, code := ffiGoGuardCreate()
	requireFFICodeZero(t, code, "guard_create")
	code = ffiGoGuardTrigger(gh)
	requireFFICodeZero(t, code, "guard_trigger")
	ffiMu.Lock()
	g, ok := ffiGuards[gh]
	ffiMu.Unlock()
	if !ok {
		t.Fatalf("guard handle missing from registry")
	}
	if !g.IsTriggered() {
		t.Fatalf("guard should be triggered")
	}
	code = ffiGoGuardReset(gh)
	requireFFICodeZero(t, code, "guard_reset")
	if g.IsTriggered() {
		t.Fatalf("guard should be reset")
	}
}

// checkFFIBadBridgeJoin pins bad bridge and join handles.
func checkFFIBadBridgeJoin(t *testing.T, bad int64, bh int64) {
	t.Helper()
	code := ffiGoBridgeFree(bad)
	requireFFINegative(t, code, "bridge_free bad handle")
	code = ffiGoJoin(bad, "ffi-test", 0)
	requireFFINegative(t, code, "join bad handle")
	code = ffiGoJoinNullParticipant(bh)
	requireFFINegative(t, code, "join NULL participant")
}

// checkFFIBadPubSub pins bad publish and take handles.
func checkFFIBadPubSub(t *testing.T, bad int64) {
	t.Helper()
	code := ffiGoPubPublish(bad, []byte("x"))
	requireFFINegative(t, code, "publish bad handle")
	_, _, code = ffiGoSubTakePoll(bad, 256)
	requireFFINegative(t, code, "take_poll bad handle")
	code = ffiGoTakePollNullN(bad)
	requireFFINegative(t, code, "take_poll NULL outN")
	_, _, code = ffiGoServiceCallNB(bad, []byte("x"), 256)
	requireFFINegative(t, code, "service_call_nb bad handle")
}

// checkFFIBadDestroy pins bad destroy and guard handles.
func checkFFIBadDestroy(t *testing.T, bad int64) {
	t.Helper()
	code := ffiGoPubDestroy(bad)
	requireFFINegative(t, code, "pub_destroy bad handle")
	code = ffiGoSubDestroy(bad)
	requireFFINegative(t, code, "sub_destroy bad handle")
	code = ffiGoServiceDestroy(bad)
	requireFFINegative(t, code, "service_destroy bad handle")
	code = ffiGoGuardTrigger(bad)
	requireFFINegative(t, code, "guard_trigger bad handle")
	code = ffiGoGuardReset(bad)
	requireFFINegative(t, code, "guard_reset bad handle")
}

// TestFFIInvalidHandles pins negative codes for bad handles and NULLs.
func TestFFIInvalidHandles(t *testing.T) {
	const bad = int64(123456789)
	bh := ffiTestBridge(t)
	checkFFIBadBridgeJoin(t, bad, bh)
	checkFFIBadPubSub(t, bad)
	checkFFIBadDestroy(t, bad)
	code := ffiGoBridgeFree(bh)
	requireFFICodeZero(t, code, "bridge_free")
}
