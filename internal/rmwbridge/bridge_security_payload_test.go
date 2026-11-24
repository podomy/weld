// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// TestSecurityOversizedPayload documents the 32 MiB cap behavior.
//
// The bridge currently enforces no payload size cap: a 32 MiB sample is
// copied once per send and round-trips intact. If a cap is added later,
// this test must be updated to expect the cap error instead of success.
func TestSecurityOversizedPayload(t *testing.T) {
	t.Parallel()
	b := newSecurityBridge(t)
	defer func() {
		closeSecurityBridge(t, b)
	}()
	const mib32 = 32 << 20
	big := make([]byte, mib32)
	big[0] = 0xAB
	big[mib32-1] = 0xCD

	n := mustNode(t, b, "bigpayload")
	qos := rmwbridge.DefaultQoS()
	pub, err := b.CreatePublisher(n, "/big", qos)
	requireNoError(t, err, "create publisher")
	sub, err := b.CreateSubscription(n, "/big", qos)
	requireNoError(t, err, "create subscription")
	checkBigPayloadPubSub(t, b, pub, sub, big, mib32)
}

// checkBigPayloadPubSub pins 32MiB roundtrip on both datapaths.
func checkBigPayloadPubSub(t *testing.T, b *rmwbridge.Bridge, pub *rmwbridge.Publisher, sub *rmwbridge.Subscription, big []byte, mib32 int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := pub.Publish(ctx, big)
	requireNoError(t, err, "publish 32MiB")
	got, err := sub.Take(ctx)
	requireNoError(t, err, "take 32MiB")
	if len(got) != mib32 {
		t.Fatalf("32MiB roundtrip len = %d, want %d", len(got), mib32)
	}
	if got[0] != 0xAB || got[mib32-1] != 0xCD {
		t.Fatalf("32MiB payload corrupted at edges")
	}
	t.Logf("32 MiB payload round-trips intact: no size cap currently enforced")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	err = b.PublishBytes(ctx2, rmwbridge.OutgoingMessage{TopicName: "/big", Payload: big})
	requireNoError(t, err, "publish bytes 32MiB")
	msg, err := b.TakeBytes(ctx2)
	requireNoError(t, err, "take bytes 32MiB")
	if len(msg.Payload) != mib32 {
		t.Fatalf("backend 32MiB len = %d, want %d", len(msg.Payload), mib32)
	}
}

// checkCtxErr fails unless err is canceled or deadline exceeded.
func checkCtxErr(t *testing.T, name string, err error) {
	t.Helper()
	requireTimeout(t, err, name)
}

// setupCtxCancelBridge creates pub sub service and client for cancel test.
func setupCtxCancelBridge(t *testing.T) (*rmwbridge.Bridge, *rmwbridge.Publisher, *rmwbridge.Subscription, *rmwbridge.Service, *rmwbridge.Client) {
	t.Helper()
	b := newSecurityBridge(t)
	n := mustNode(t, b, "ctxcancel")
	qos := rmwbridge.DefaultQoS()
	pub, err := b.CreatePublisher(n, "/ctx", qos)
	requireNoError(t, err, "create publisher")
	sub, err := b.CreateSubscription(n, "/ctx", qos)
	requireNoError(t, err, "create subscription")
	svc, err := b.CreateService("ctxsvc")
	requireNoError(t, err, "create service")
	client, err := b.CreateClient("ctxsvc")
	requireNoError(t, err, "create client")
	return b, pub, sub, svc, client
}

// checkCanceledCalls pins canceled context on all blocking calls.
func checkCanceledCalls(t *testing.T, b *rmwbridge.Bridge, pub *rmwbridge.Publisher, sub *rmwbridge.Subscription, svc *rmwbridge.Service, client *rmwbridge.Client, canceled context.Context) {
	t.Helper()
	err := b.JoinDiscovery(canceled, rmwbridge.DiscoveryInfo{})
	requireError(t, err, "JoinDiscovery canceled")
	err = b.PublishBytes(canceled, rmwbridge.OutgoingMessage{})
	checkCtxErr(t, "PublishBytes", err)
	err = pub.Publish(canceled, []byte("x"))
	checkCtxErr(t, "Publisher.Publish", err)
	_, err = svc.Call(canceled, []byte("x"))
	checkCtxErr(t, "Service.Call", err)
	_, err = client.Call(canceled, []byte("x"))
	checkCtxErr(t, "Client.Call", err)
	_, _, err = svc.Serve(canceled)
	checkCtxErr(t, "Serve", err)
	_, err = sub.Take(canceled)
	checkCtxErr(t, "Take", err)
	_, err = b.TakeBytes(canceled)
	checkCtxErr(t, "TakeBytes", err)
	err = b.ReportLiveliness(canceled, rmwbridge.LivelinessReport{})
	requireError(t, err, "ReportLiveliness canceled")
	ws := rmwbridge.NewWaitSet()
	err = ws.Wait(canceled)
	checkCtxErr(t, "Wait", err)
}

// checkTimeoutVariants pins timeout on empty queues and wait sets.
func checkTimeoutVariants(t *testing.T, sub *rmwbridge.Subscription, svc *rmwbridge.Service) {
	t.Helper()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := sub.Take(timeoutCtx)
	requireError(t, err, "Take timeout on empty sub")
	ws := rmwbridge.NewWaitSet()
	timeoutCtx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	err = ws.Wait(timeoutCtx2)
	requireError(t, err, "Wait timeout on empty set")
	timeoutCtx3, cancel3 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel3()
	_, _, err = svc.Serve(timeoutCtx3)
	requireError(t, err, "Serve timeout with no request")
}

// TestSecurityContextCancel verifies every blocking call honors cancel.
func TestSecurityContextCancel(t *testing.T) {
	t.Parallel()
	b, pub, sub, svc, client := setupCtxCancelBridge(t)
	defer func() {
		closeSecurityBridge(t, b)
	}()
	canceled, stop := context.WithCancel(context.Background())
	stop()
	checkCanceledCalls(t, b, pub, sub, svc, client, canceled)
	checkTimeoutVariants(t, sub, svc)
}
