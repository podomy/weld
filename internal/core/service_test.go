// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

// TestService covers the transport free service call state machine: dual QoS
// fanout, GUID plus sequence routing, per call timeouts, gone peer paths,
// atomic take plus dispatch, and introspection hooks.
func TestService(t *testing.T) {
	t.Run("default timeout", checkServiceDefaultTimeout)
	t.Run("trace strings", checkServiceTraceStrings)
	t.Run("dual fanout", checkServiceDualFanout)
	t.Run("transient local warns", checkServiceTransientLocalWarns)
	t.Run("best available fanout", checkServiceBestAvailableFanout)
	t.Run("fanout errors", checkServiceFanoutErrors)
	t.Run("client routing", checkServiceClientRouting)
	t.Run("client introspection", checkServiceClientIntrospection)
	t.Run("client timeouts", checkServiceClientTimeouts)
	t.Run("client gone", checkServiceClientGone)
	t.Run("client cancelled", checkServiceClientCancelled)
	t.Run("client validation", checkServiceClientValidation)
	t.Run("server take dispatch", checkServiceServerTakeDispatch)
	t.Run("server introspection", checkServiceServerIntrospection)
	t.Run("server reply", checkServiceServerReply)
	t.Run("server gone", checkServiceServerGone)
	t.Run("server timeouts", checkServiceServerTimeouts)
	t.Run("server cancelled", checkServiceServerCancelled)
	t.Run("server validation", checkServiceServerValidation)
	t.Run("independent discovery", checkServiceIndependentDiscovery)
	t.Run("concurrent use", checkServiceConcurrentUse)
}

// serviceFakeClock is a deterministic nanos clock for timeout tests.
type serviceFakeClock struct {
	mu  sync.Mutex
	now int64
}

// Now returns the current fake time.
func (f *serviceFakeClock) Now() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance moves the fake time forward by delta.
func (f *serviceFakeClock) Advance(delta int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now += delta
}

// serviceTestGUID returns a fixed 8 byte client GUID.
func serviceTestGUID() [8]byte {
	return [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
}

// serviceOtherGUID returns a distinct 8 byte client GUID.
func serviceOtherGUID() [8]byte {
	return [8]byte{9, 9, 9, 9, 9, 9, 9, 9}
}

// checkServiceDefaultTimeout pins the five second default.
func checkServiceDefaultTimeout(t *testing.T) {
	t.Helper()
	if got := DefaultServiceTimeout(); got != NewDuration(5, 0) {
		t.Fatalf("default service timeout must be 5s, got %s", got)
	}
}

// checkServiceTraceStrings pins trace kind strings and trace rendering.
func checkServiceTraceStrings(t *testing.T) {
	t.Helper()
	assertTraceKindStrings(t)
	assertTraceStringCarries(t)
}

// assertTraceKindStrings pins all four kinds plus invalid.
func assertTraceKindStrings(t *testing.T) {
	t.Helper()
	cases := map[ServiceTraceKind]string{
		ServiceTraceRequestSent:      "request_sent",
		ServiceTraceResponseReceived: "response_received",
		ServiceTraceRequestReceived:  "request_received",
		ServiceTraceResponseSent:     "response_sent",
		ServiceTraceKind(99):         "invalid",
	}
	for kind, want := range cases {
		if got := kind.String(); got != want {
			t.Fatalf("trace kind %d must render %q, got %q", int(kind), want, got)
		}
	}
}

// assertTraceStringCarries pins trace strings carry service, kind, guid, seq.
func assertTraceStringCarries(t *testing.T) {
	t.Helper()
	trace := ServiceTrace{Service: "/add_two_ints", Client: serviceTestGUID(), Sequence: 7, Kind: ServiceTraceRequestSent}
	s := trace.String()
	if !strings.Contains(s, "/add_two_ints") || !strings.Contains(s, "request_sent") {
		t.Fatalf("trace must carry service and kind, got %q", s)
	}
	if !strings.Contains(s, "0102030405060708") || !strings.Contains(s, "7") {
		t.Fatalf("trace must carry guid and sequence, got %q", s)
	}
}

// checkServiceDualFanout pins one input fans out to two identical legs.
func checkServiceDualFanout(t *testing.T) {
	t.Helper()
	requested := ResolveSystemDefaults(DefaultQoS())
	got, warning, err := ResolveServiceQoS("/add_two_ints", requested)
	if err != nil {
		t.Fatalf("resolve service QoS: %v", err)
	}
	if warning != nil {
		t.Fatalf("volatile service must not warn, got %+v", warning)
	}
	if got.Service != "/add_two_ints" {
		t.Fatalf("fanout must carry service name, got %q", got.Service)
	}
	if got.Request != got.Response {
		t.Fatalf("request and response legs must match, got %+v vs %+v", got.Request, got.Response)
	}
	assertDualGetters(t, got, requested)
	assertAvoidPreserved(t)
}

// assertDualGetters pins client and server getters expose the legs.
func assertDualGetters(t *testing.T, got ServiceQoS, requested QoSProfile) {
	t.Helper()
	if got.ClientRequestPublisher() != requested || got.ClientResponseSubscription() != requested {
		t.Fatalf("client getters must expose resolved legs, got %+v", got)
	}
	if got.ServerRequestSubscription() != requested || got.ServerResponsePublisher() != requested {
		t.Fatalf("server getters must expose resolved legs, got %+v", got)
	}
}

// assertAvoidPreserved pins the avoid flag passes through unresolved.
func assertAvoidPreserved(t *testing.T) {
	t.Helper()
	requested := ResolveSystemDefaults(DefaultQoS())
	requested.AvoidROSNamespaceConventions = true
	got, _, err := ResolveServiceQoS("/svc", requested)
	if err != nil {
		t.Fatalf("resolve avoided QoS: %v", err)
	}
	if !got.Request.AvoidROSNamespaceConventions || !got.Response.AvoidROSNamespaceConventions {
		t.Fatalf("avoid flag must survive fanout, got %+v", got)
	}
	report := CheckCompatibility(got.Request, ResolveSystemDefaults(DefaultQoS()))
	if !report.Compatible {
		t.Fatalf("avoid flag must never affect matching: %q", report.Reason())
	}
}

// checkServiceTransientLocalWarns pins transient local warns with name and kind.
func checkServiceTransientLocalWarns(t *testing.T) {
	t.Helper()
	requested := ResolveSystemDefaults(DefaultQoS())
	requested.Durability = DurabilityTransientLocal
	got, warning, err := ResolveServiceQoS("/add_two_ints", requested)
	if err != nil {
		t.Fatalf("resolve transient local: %v", err)
	}
	if warning == nil {
		t.Fatal("transient local on services must warn")
	}
	if warning.Topic != "/add_two_ints" || warning.Policy != PolicyDurability {
		t.Fatalf("warning must carry service and policy, got %+v", warning)
	}
	if warning.Severity != SeverityWarning {
		t.Fatalf("service durability warning must be warning, got %+v", warning)
	}
	if !strings.Contains(warning.String(), "/add_two_ints") || !strings.Contains(warning.LogLine(), "durability") {
		t.Fatalf("warning shapes must carry names, got %+v", warning)
	}
	if got.Request.Durability != DurabilityTransientLocal {
		t.Fatalf("legs must keep the resolved value, got %+v", got)
	}
}

// checkServiceBestAvailableFanout pins markers become services default.
func checkServiceBestAvailableFanout(t *testing.T) {
	t.Helper()
	got, _, err := ResolveServiceQoS("/svc", BestAvailableQoS())
	if err != nil {
		t.Fatalf("resolve best available: %v", err)
	}
	want := ResolveSystemDefaults(ServicesDefaultQoS())
	if got.Request != want || got.Response != want {
		t.Fatalf("best available must become services default, got %+v want %+v", got, want)
	}
	partial := ResolveSystemDefaults(DefaultQoS())
	partial.Reliability = ReliabilityBestAvailable
	partial.Durability = DurabilityTransientLocal
	partialGot, _, err := ResolveServiceQoS("/svc", partial)
	if err != nil {
		t.Fatalf("resolve partial markers: %v", err)
	}
	if partialGot.Request.Reliability != want.Reliability {
		t.Fatalf("marker reliability must take services default, got %s", partialGot.Request.Reliability)
	}
	if partialGot.Request.Durability != DurabilityTransientLocal {
		t.Fatalf("explicit durability must survive, got %s", partialGot.Request.Durability)
	}
}

// checkServiceFanoutErrors pins empty service and invalid profiles fail,
// plus sentinel markers resolve instead of surviving.
func checkServiceFanoutErrors(t *testing.T) {
	t.Helper()
	if _, _, err := ResolveServiceQoS("", DefaultQoS()); err == nil {
		t.Fatal("empty service must fail fanout")
	}
	if _, _, err := ResolveServiceQoS("/svc", UnknownQoS()); err == nil {
		t.Fatal("unknown profile must fail fanout")
	}
	bad := ResolveSystemDefaults(DefaultQoS())
	bad.Reliability = ReliabilityPolicy(99)
	if _, _, err := ResolveServiceQoS("/svc", bad); err == nil {
		t.Fatal("invalid reliability must fail fanout")
	}
	assertSentinelResolves(t)
}

// assertSentinelResolves pins a lone sentinel deadline takes services default.
func assertSentinelResolves(t *testing.T) {
	t.Helper()
	bad := ResolveSystemDefaults(DefaultQoS())
	bad.Deadline = BestAvailableDuration()
	got, _, err := ResolveServiceQoS("/svc", bad)
	if err != nil {
		t.Fatalf("sentinel deadline must resolve, got %v", err)
	}
	if IsBestAvailableDuration(got.Request.Deadline) {
		t.Fatalf("sentinel must not survive fanout, got %s", got.Request.Deadline)
	}
}

// checkServiceClientRouting pins exact one to one routing by key.
func checkServiceClientRouting(t *testing.T) {
	t.Helper()
	clock := &serviceFakeClock{}
	client := NewServiceClient(clock.Now, false, nil)
	ctx := context.Background()
	guid := serviceTestGUID()
	err := client.BeginCall(ctx, "/add_two_ints", guid, 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("begin call: %v", err)
	}
	assertClientRoutingMismatch(t, client, guid)
	assertClientRoutingMatch(t, client, guid)
}

// assertClientRoutingMismatch pins wrong guid, seq, service miss.
func assertClientRoutingMismatch(t *testing.T, client *ServiceClient, guid [8]byte) {
	t.Helper()
	if !client.HasPending("/add_two_ints", guid, 1) {
		t.Fatal("pending key must exist after begin")
	}
	if client.HasPending("/add_two_ints", serviceOtherGUID(), 1) {
		t.Fatal("wrong guid must not match")
	}
	if client.HasPending("/add_two_ints", guid, 2) {
		t.Fatal("wrong sequence must not match")
	}
	if client.HasPending("/other", guid, 1) {
		t.Fatal("wrong service must not match")
	}
	err := client.CompleteCall("/add_two_ints", serviceOtherGUID(), 1)
	if !errors.Is(err, ErrServiceNoPending) {
		t.Fatalf("wrong guid complete must be no pending, got %v", err)
	}
	err = client.CompleteCall("/add_two_ints", guid, 2)
	if !errors.Is(err, ErrServiceNoPending) {
		t.Fatalf("wrong sequence complete must be no pending, got %v", err)
	}
	err = client.BeginCall(context.Background(), "/add_two_ints", guid, 1, UnspecifiedDuration())
	if !errors.Is(err, ErrServiceDuplicate) {
		t.Fatalf("duplicate begin must fail, got %v", err)
	}
}

// assertClientRoutingMatch pins the exact key completes.
func assertClientRoutingMatch(t *testing.T, client *ServiceClient, guid [8]byte) {
	t.Helper()
	err := client.CompleteCall("/add_two_ints", guid, 1)
	if err != nil {
		t.Fatalf("complete exact key: %v", err)
	}
	if client.PendingCount() != 0 {
		t.Fatalf("pending must be empty after complete, got %d", client.PendingCount())
	}
	err = client.CompleteCall("/add_two_ints", guid, 1)
	if !errors.Is(err, ErrServiceNoPending) {
		t.Fatalf("second complete must be no pending, got %v", err)
	}
}

// checkServiceClientIntrospection pins request sent plus response received.
func checkServiceClientIntrospection(t *testing.T) {
	t.Helper()
	var mu sync.Mutex
	var traces []ServiceTrace
	hook := func(tr ServiceTrace) {
		mu.Lock()
		defer mu.Unlock()
		traces = append(traces, tr)
	}
	clock := &serviceFakeClock{}
	client := NewServiceClient(clock.Now, true, hook)
	guid := serviceTestGUID()
	err := client.BeginCall(context.Background(), "/svc", guid, 11, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("begin call: %v", err)
	}
	err = client.CompleteCall("/svc", guid, 11)
	if err != nil {
		t.Fatalf("complete call: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	assertClientTraces(t, traces, guid)
	assertDisabledEmitsNothing(t)
}

// assertClientTraces pins two events carry sequence and client GUID.
func assertClientTraces(t *testing.T, traces []ServiceTrace, guid [8]byte) {
	t.Helper()
	if len(traces) != 2 {
		t.Fatalf("expected two client traces, got %+v", traces)
	}
	if traces[0].Kind != ServiceTraceRequestSent || traces[1].Kind != ServiceTraceResponseReceived {
		t.Fatalf("client must emit sent then received, got %+v", traces)
	}
	for _, tr := range traces {
		if tr.Client != guid || tr.Sequence != 11 || tr.Service != "/svc" {
			t.Fatalf("trace must carry key, got %+v", tr)
		}
	}
}

// assertDisabledEmitsNothing pins disabled trackers stay quiet.
func assertDisabledEmitsNothing(t *testing.T) {
	t.Helper()
	fired := false
	quiet := NewServiceClient(nil, false, func(ServiceTrace) { fired = true })
	err := quiet.BeginCall(context.Background(), "/svc", serviceTestGUID(), 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("quiet begin: %v", err)
	}
	err = quiet.CompleteCall("/svc", serviceTestGUID(), 1)
	if err != nil {
		t.Fatalf("quiet complete: %v", err)
	}
	if fired {
		t.Fatal("disabled client must not emit")
	}
	quietServer := NewServiceServer(nil, false, func(ServiceTrace) { fired = true })
	err = quietServer.EnqueueRequest(context.Background(), "/svc", serviceTestGUID(), 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("quiet enqueue: %v", err)
	}
	_, ok := quietServer.TakeRequest()
	if !ok {
		t.Fatal("quiet take must succeed")
	}
	err = quietServer.Reply("/svc", serviceTestGUID(), 1)
	if err != nil {
		t.Fatalf("quiet reply: %v", err)
	}
	if fired {
		t.Fatal("disabled server must not emit")
	}
}

// checkServiceClientTimeouts pins per call timeouts with a fake clock.
func checkServiceClientTimeouts(t *testing.T) {
	t.Helper()
	clock := &serviceFakeClock{}
	client := NewServiceClient(clock.Now, false, nil)
	ctx := context.Background()
	err := client.BeginCall(ctx, "/svc", serviceTestGUID(), 1, NewDuration(0, 100000000))
	if err != nil {
		t.Fatalf("begin finite: %v", err)
	}
	err = client.BeginCall(ctx, "/svc", serviceTestGUID(), 2, InfiniteDuration())
	if err != nil {
		t.Fatalf("begin infinite: %v", err)
	}
	if expired := client.ExpireCalls(50000000); len(expired) != 0 {
		t.Fatalf("fresh window must not expire, got %+v", expired)
	}
	expired := client.ExpireCalls(100000000)
	if len(expired) != 1 || expired[0].Sequence != 1 {
		t.Fatalf("lapsed call must expire once, got %+v", expired)
	}
	if got := client.TimeoutCount(); got != 1 {
		t.Fatalf("timeout count must be 1, got %d", got)
	}
	if expired := client.ExpireCalls(1 << 60); len(expired) != 0 {
		t.Fatalf("infinite must never expire, got %+v", expired)
	}
	assertDefaultTimeoutExpiry(t)
}

// assertDefaultTimeoutExpiry pins unspecified means five seconds.
func assertDefaultTimeoutExpiry(t *testing.T) {
	t.Helper()
	clock := &serviceFakeClock{}
	client := NewServiceClient(clock.Now, false, nil)
	err := client.BeginCall(context.Background(), "/svc", serviceTestGUID(), 9, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("begin default: %v", err)
	}
	if expired := client.ExpireCalls(4999999999); len(expired) != 0 {
		t.Fatalf("default must not expire early, got %+v", expired)
	}
	expired := client.ExpireCalls(5000000000)
	if len(expired) != 1 {
		t.Fatalf("default must expire at 5s, got %+v", expired)
	}
}

// checkServiceClientGone pins gone server errors one call.
//
//nolint:cyclop // client test sequences begin, gone, and error checks in one run, splitting would obscure intent
func checkServiceClientGone(t *testing.T) {
	t.Helper()
	clock := &serviceFakeClock{}
	client := NewServiceClient(clock.Now, false, nil)
	ctx := context.Background()
	err := client.BeginCall(ctx, "/svc", serviceTestGUID(), 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("begin 1: %v", err)
	}
	err = client.BeginCall(ctx, "/svc", serviceTestGUID(), 2, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("begin 2: %v", err)
	}
	err = client.BeginCall(ctx, "/other", serviceTestGUID(), 3, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("begin other: %v", err)
	}
	err = client.FailServerGone("/svc", serviceTestGUID(), 1)
	if !errors.Is(err, ErrServiceServerGone) {
		t.Fatalf("gone server must wrap gone error, got %v", err)
	}
	if !strings.Contains(err.Error(), "/svc") {
		t.Fatalf("gone error must carry service name, got %q", err.Error())
	}
	if client.PendingCount() != 2 {
		t.Fatalf("one gone must leave two, got %d", client.PendingCount())
	}
	if got := client.GoneCount(); got != 1 {
		t.Fatalf("gone count must be 1, got %d", got)
	}
	err = client.FailServerGone("/svc", serviceTestGUID(), 1)
	if !errors.Is(err, ErrServiceNoPending) {
		t.Fatalf("second gone on same key must be no pending, got %v", err)
	}
	if n := client.FailAllServerGone("/svc"); n != 1 {
		t.Fatalf("fail all must remove one, got %d", n)
	}
	if client.PendingCount() != 1 || !client.HasPending("/other", serviceTestGUID(), 3) {
		t.Fatal("fail all must keep other services")
	}
	if n := client.FailAllServerGone("/missing"); n != 0 {
		t.Fatalf("fail all missing must be zero, got %d", n)
	}
}

// checkServiceClientCancelled pins context close errors one call.
func checkServiceClientCancelled(t *testing.T) {
	t.Helper()
	clock := &serviceFakeClock{}
	client := NewServiceClient(clock.Now, false, nil)
	ctx, cancel := context.WithCancel(context.Background())
	err := client.BeginCall(ctx, "/svc", serviceTestGUID(), 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("begin cancellable: %v", err)
	}
	err = client.BeginCall(context.Background(), "/svc", serviceTestGUID(), 2, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("begin plain: %v", err)
	}
	cancel()
	cancelled := client.CollectCancelled()
	if len(cancelled) != 1 || cancelled[0].Sequence != 1 {
		t.Fatalf("cancel must collect exactly one, got %+v", cancelled)
	}
	if client.PendingCount() != 1 {
		t.Fatalf("cancel must leave one, got %d", client.PendingCount())
	}
	deadlineCtx, deadlineCancel := context.WithCancel(context.Background())
	deadlineCancel()
	err = client.BeginCall(deadlineCtx, "/svc", serviceTestGUID(), 3, UnspecifiedDuration())
	if err == nil {
		t.Fatal("begin on done context must fail")
	}
}

// checkServiceClientValidation pins empty, nil, malformed inputs fail.
func checkServiceClientValidation(t *testing.T) {
	t.Helper()
	client := NewServiceClient(nil, false, nil)
	if err := client.BeginCall(context.Background(), "", serviceTestGUID(), 1, UnspecifiedDuration()); err == nil {
		t.Fatal("empty service begin must fail")
	}
	if err := client.BeginCall(nil, "/svc", serviceTestGUID(), 1, UnspecifiedDuration()); err == nil { //nolint:staticcheck // SA1012 intentional nil to pin validation, callee returns error safely
		t.Fatal("nil context begin must fail")
	}
	if err := client.BeginCall(context.Background(), "/svc", serviceTestGUID(), 1, Duration{Seconds: -1}); err == nil {
		t.Fatal("negative timeout must fail")
	}
	if err := client.BeginCall(context.Background(), "/svc", serviceTestGUID(), 1, Duration{Seconds: 1, Nanoseconds: 2000000000}); err == nil {
		t.Fatal("oversized timeout must fail")
	}
	if err := client.CompleteCall("", serviceTestGUID(), 1); err == nil {
		t.Fatal("empty service complete must fail")
	}
	if err := client.FailServerGone("", serviceTestGUID(), 1); err == nil {
		t.Fatal("empty service gone must fail")
	}
	var nilClient *ServiceClient
	if err := nilClient.BeginCall(context.Background(), "/svc", serviceTestGUID(), 1, UnspecifiedDuration()); err == nil {
		t.Fatal("nil client begin must fail")
	}
	if err := nilClient.CompleteCall("/svc", serviceTestGUID(), 1); err == nil {
		t.Fatal("nil client complete must fail")
	}
	if err := nilClient.FailServerGone("/svc", serviceTestGUID(), 1); err == nil {
		t.Fatal("nil client gone must fail")
	}
	if got := nilClient.PendingCount(); got != 0 {
		t.Fatalf("nil pending count must be 0, got %d", got)
	}
}

// checkServiceServerTakeDispatch pins atomic take plus empty signals.
//
//nolint:cyclop // server test sequences take, empty, and dispatch checks in one run, splitting would obscure intent
func checkServiceServerTakeDispatch(t *testing.T) {
	t.Helper()
	clock := &serviceFakeClock{}
	server := NewServiceServer(clock.Now, false, nil)
	if _, ok := server.TakeRequest(); ok {
		t.Fatal("empty take must return false")
	}
	if got := server.EmptyTakeCount(); got != 1 {
		t.Fatalf("empty take count must be 1, got %d", got)
	}
	ctx := context.Background()
	err := server.EnqueueRequest(ctx, "/svc", serviceTestGUID(), 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("enqueue 1: %v", err)
	}
	err = server.EnqueueRequest(ctx, "/svc", serviceTestGUID(), 2, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("enqueue 2: %v", err)
	}
	if got := server.QueuedCount(); got != 2 {
		t.Fatalf("queued must be 2, got %d", got)
	}
	first, ok := server.TakeRequest()
	if !ok || first.Sequence != 1 {
		t.Fatalf("take must dispatch oldest first, got %+v %v", first, ok)
	}
	if got := server.QueuedCount(); got != 1 {
		t.Fatalf("queued must be 1 after take, got %d", got)
	}
	if got := server.ActiveCount(); got != 1 {
		t.Fatalf("active must be 1 after take, got %d", got)
	}
	second, ok := server.TakeRequest()
	if !ok || second.Sequence != 2 {
		t.Fatalf("second take must dispatch seq 2, got %+v %v", second, ok)
	}
	if _, ok := server.TakeRequest(); ok {
		t.Fatal("third take must be empty")
	}
	if got := server.EmptyTakeCount(); got != 2 {
		t.Fatalf("empty count must be 2, got %d", got)
	}
}

// checkServiceServerIntrospection pins request received plus response sent.
func checkServiceServerIntrospection(t *testing.T) {
	t.Helper()
	var mu sync.Mutex
	var traces []ServiceTrace
	hook := func(tr ServiceTrace) {
		mu.Lock()
		defer mu.Unlock()
		traces = append(traces, tr)
	}
	clock := &serviceFakeClock{}
	server := NewServiceServer(clock.Now, true, hook)
	guid := serviceTestGUID()
	err := server.EnqueueRequest(context.Background(), "/svc", guid, 5, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	_, ok := server.TakeRequest()
	if !ok {
		t.Fatal("take must succeed")
	}
	err = server.Reply("/svc", guid, 5)
	if err != nil {
		t.Fatalf("reply: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(traces) != 2 {
		t.Fatalf("expected two server traces, got %+v", traces)
	}
	if traces[0].Kind != ServiceTraceRequestReceived || traces[1].Kind != ServiceTraceResponseSent {
		t.Fatalf("server must emit received then sent, got %+v", traces)
	}
	for _, tr := range traces {
		if tr.Client != guid || tr.Sequence != 5 || tr.Service != "/svc" {
			t.Fatalf("server trace must carry key, got %+v", tr)
		}
	}
}

// checkServiceServerReply pins reply needs a taken request.
func checkServiceServerReply(t *testing.T) {
	t.Helper()
	server := NewServiceServer(nil, false, nil)
	guid := serviceTestGUID()
	err := server.EnqueueRequest(context.Background(), "/svc", guid, 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	err = server.Reply("/svc", guid, 1)
	if !errors.Is(err, ErrServiceTimeout) {
		t.Fatalf("reply before take must be reader not ready timeout, got %v", err)
	}
	if !strings.Contains(err.Error(), "/svc") {
		t.Fatalf("reply timeout must carry service name, got %q", err.Error())
	}
	_, ok := server.TakeRequest()
	if !ok {
		t.Fatal("take must succeed")
	}
	err = server.Reply("/svc", guid, 1)
	if err != nil {
		t.Fatalf("reply after take: %v", err)
	}
	err = server.Reply("/svc", guid, 1)
	if !errors.Is(err, ErrServiceTimeout) {
		t.Fatalf("second reply must be timeout, got %v", err)
	}
	err = server.EnqueueRequest(context.Background(), "/svc", guid, 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("sequence reuse after reply must succeed: %v", err)
	}
}

// checkServiceServerGone pins gone client errors one queued or active call.
func checkServiceServerGone(t *testing.T) {
	t.Helper()
	server := NewServiceServer(nil, false, nil)
	guid := serviceTestGUID()
	ctx := context.Background()
	err := server.EnqueueRequest(ctx, "/svc", guid, 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("enqueue 1: %v", err)
	}
	err = server.EnqueueRequest(ctx, "/svc", guid, 2, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("enqueue 2: %v", err)
	}
	_, ok := server.TakeRequest()
	if !ok {
		t.Fatal("take must succeed")
	}
	err = server.FailClientGone("/svc", guid, 2)
	if !errors.Is(err, ErrServiceClientGone) {
		t.Fatalf("queued gone must wrap client gone, got %v", err)
	}
	if got := server.QueuedCount(); got != 0 {
		t.Fatalf("queued must be empty after queued gone, got %d", got)
	}
	if got := server.ActiveCount(); got != 1 {
		t.Fatalf("active must stay 1, got %d", got)
	}
	err = server.FailClientGone("/svc", guid, 1)
	if !errors.Is(err, ErrServiceClientGone) {
		t.Fatalf("active gone must wrap client gone, got %v", err)
	}
	if got := server.GoneCount(); got != 2 {
		t.Fatalf("gone count must be 2, got %d", got)
	}
	err = server.FailClientGone("/svc", guid, 9)
	if !errors.Is(err, ErrServiceNoRequest) {
		t.Fatalf("missing gone must be no request, got %v", err)
	}
}

// checkServiceServerTimeouts pins expiry on queued and active legs.
func checkServiceServerTimeouts(t *testing.T) {
	t.Helper()
	clock := &serviceFakeClock{}
	server := NewServiceServer(clock.Now, false, nil)
	ctx := context.Background()
	err := server.EnqueueRequest(ctx, "/svc", serviceTestGUID(), 1, NewDuration(0, 100000000))
	if err != nil {
		t.Fatalf("enqueue finite: %v", err)
	}
	err = server.EnqueueRequest(ctx, "/svc", serviceTestGUID(), 2, InfiniteDuration())
	if err != nil {
		t.Fatalf("enqueue infinite: %v", err)
	}
	_, ok := server.TakeRequest()
	if !ok {
		t.Fatal("take must succeed")
	}
	if expired := server.ExpireRequests(50000000); len(expired) != 0 {
		t.Fatalf("fresh window must not expire, got %+v", expired)
	}
	expired := server.ExpireRequests(100000000)
	if len(expired) != 1 || expired[0].Sequence != 1 {
		t.Fatalf("lapsed active must expire once, got %+v", expired)
	}
	if got := server.TimeoutCount(); got != 1 {
		t.Fatalf("timeout count must be 1, got %d", got)
	}
	if expired := server.ExpireRequests(1 << 60); len(expired) != 0 {
		t.Fatalf("infinite queued must never expire, got %+v", expired)
	}
	assertServerQueuedExpiry(t)
}

// assertServerQueuedExpiry pins a queued request expires before take.
func assertServerQueuedExpiry(t *testing.T) {
	t.Helper()
	clock := &serviceFakeClock{}
	server := NewServiceServer(clock.Now, false, nil)
	err := server.EnqueueRequest(context.Background(), "/svc", serviceTestGUID(), 3, NewDuration(0, 50000000))
	if err != nil {
		t.Fatalf("enqueue queued: %v", err)
	}
	expired := server.ExpireRequests(60000000)
	if len(expired) != 1 || expired[0].Sequence != 3 {
		t.Fatalf("queued expiry must return seq 3, got %+v", expired)
	}
	if got := server.QueuedCount(); got != 0 {
		t.Fatalf("queued must be empty after expiry, got %d", got)
	}
}

// checkServiceServerCancelled pins done contexts collect from both sets.
func checkServiceServerCancelled(t *testing.T) {
	t.Helper()
	server := NewServiceServer(nil, false, nil)
	ctx, cancel := context.WithCancel(context.Background())
	err := server.EnqueueRequest(ctx, "/svc", serviceTestGUID(), 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("enqueue cancellable: %v", err)
	}
	err = server.EnqueueRequest(context.Background(), "/svc", serviceTestGUID(), 2, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("enqueue plain: %v", err)
	}
	_, ok := server.TakeRequest()
	if !ok {
		t.Fatal("take must succeed")
	}
	cancel()
	cancelled := server.CollectCancelled()
	if len(cancelled) != 1 || cancelled[0].Sequence != 1 {
		t.Fatalf("cancel must collect active seq 1, got %+v", cancelled)
	}
	queuedCtx, queuedCancel := context.WithCancel(context.Background())
	err = server.EnqueueRequest(queuedCtx, "/svc", serviceTestGUID(), 3, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("enqueue queued cancellable: %v", err)
	}
	queuedCancel()
	cancelled = server.CollectCancelled()
	found := false
	for _, key := range cancelled {
		if key.Sequence == 3 {
			found = true
		}
	}
	if !found {
		t.Fatalf("cancel must collect queued seq 3, got %+v", cancelled)
	}
}

// checkServiceServerValidation pins duplicate and malformed inputs fail.
//
//nolint:cyclop // server test sequences duplicate, malformed, and guard checks in one run, splitting would obscure intent
func checkServiceServerValidation(t *testing.T) {
	t.Helper()
	server := NewServiceServer(nil, false, nil)
	guid := serviceTestGUID()
	if err := server.EnqueueRequest(context.Background(), "", guid, 1, UnspecifiedDuration()); err == nil {
		t.Fatal("empty service enqueue must fail")
	}
	if err := server.EnqueueRequest(nil, "/svc", guid, 1, UnspecifiedDuration()); err == nil { //nolint:staticcheck // SA1012 intentional nil to pin validation, callee returns error safely
		t.Fatal("nil context enqueue must fail")
	}
	if err := server.EnqueueRequest(context.Background(), "/svc", guid, 1, Duration{Seconds: -2}); err == nil {
		t.Fatal("negative timeout must fail")
	}
	err := server.EnqueueRequest(context.Background(), "/svc", guid, 1, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	err = server.EnqueueRequest(context.Background(), "/svc", guid, 1, UnspecifiedDuration())
	if !errors.Is(err, ErrServiceDuplicate) {
		t.Fatalf("duplicate queued must fail, got %v", err)
	}
	_, ok := server.TakeRequest()
	if !ok {
		t.Fatal("take must succeed")
	}
	err = server.EnqueueRequest(context.Background(), "/svc", guid, 1, UnspecifiedDuration())
	if !errors.Is(err, ErrServiceDuplicate) {
		t.Fatalf("duplicate active must fail, got %v", err)
	}
	if err := server.Reply("", guid, 1); err == nil {
		t.Fatal("empty service reply must fail")
	}
	if err := server.FailClientGone("", guid, 1); err == nil {
		t.Fatal("empty service gone must fail")
	}
	var nilServer *ServiceServer
	if err := nilServer.EnqueueRequest(context.Background(), "/svc", guid, 1, UnspecifiedDuration()); err == nil {
		t.Fatal("nil server enqueue must fail")
	}
	if _, ok := nilServer.TakeRequest(); ok {
		t.Fatal("nil server take must be empty")
	}
	if err := nilServer.Reply("/svc", guid, 1); err == nil {
		t.Fatal("nil server reply must fail")
	}
	if err := nilServer.FailClientGone("/svc", guid, 1); err == nil {
		t.Fatal("nil server gone must fail")
	}
	if got := nilServer.QueuedCount(); got != 0 {
		t.Fatalf("nil queued must be 0, got %d", got)
	}
}

// checkServiceIndependentDiscovery pins client and server need no shared state.
func checkServiceIndependentDiscovery(t *testing.T) {
	t.Helper()
	clientClock := &serviceFakeClock{}
	serverClock := &serviceFakeClock{}
	client := NewServiceClient(clientClock.Now, false, nil)
	server := NewServiceServer(serverClock.Now, false, nil)
	guid := serviceTestGUID()
	err := client.BeginCall(context.Background(), "/svc", guid, 42, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("client begin without server: %v", err)
	}
	if server.QueuedCount() != 0 {
		t.Fatal("server must stay empty without enqueue")
	}
	err = server.EnqueueRequest(context.Background(), "/other", guid, 42, UnspecifiedDuration())
	if err != nil {
		t.Fatalf("server enqueue without client: %v", err)
	}
	if client.PendingCount() != 1 || server.QueuedCount() != 1 {
		t.Fatal("sides must track independently")
	}
	err = client.CompleteCall("/svc", guid, 42)
	if err != nil {
		t.Fatalf("client complete: %v", err)
	}
	req, ok := server.TakeRequest()
	if !ok || req.Service != "/other" || req.Sequence != 42 {
		t.Fatalf("server take must return its own key, got %+v %v", req, ok)
	}
	err = server.Reply("/other", guid, 42)
	if err != nil {
		t.Fatalf("server reply: %v", err)
	}
}

// checkServiceConcurrentUse pins concurrent distinct keys stay race free.
func checkServiceConcurrentUse(t *testing.T) {
	t.Helper()
	client := NewServiceClient(nil, false, nil)
	server := NewServiceServer(nil, false, nil)
	var wg sync.WaitGroup
	for i := range 16 {
		seq := uint64(i + 1)
		wg.Go(func() {
			err := client.BeginCall(context.Background(), "/svc", serviceTestGUID(), seq, UnspecifiedDuration())
			if err != nil {
				t.Errorf("concurrent begin %d: %v", seq, err)
			}
		})
		wg.Go(func() {
			err := server.EnqueueRequest(context.Background(), "/svc", serviceTestGUID(), seq, UnspecifiedDuration())
			if err != nil {
				t.Errorf("concurrent enqueue %d: %v", seq, err)
			}
		})
	}
	wg.Wait()
	if got := client.PendingCount(); got != 16 {
		t.Fatalf("concurrent pending must be 16, got %d", got)
	}
	if got := server.QueuedCount(); got != 16 {
		t.Fatalf("concurrent queued must be 16, got %d", got)
	}
	assertConcurrentDrain(t, client, server)
}

// assertConcurrentDrain pins concurrent take plus complete drain all.
func assertConcurrentDrain(t *testing.T, client *ServiceClient, server *ServiceServer) {
	t.Helper()
	for range 16 {
		_, ok := server.TakeRequest()
		if !ok {
			t.Fatal("drain take must succeed")
		}
	}
	if got := server.ActiveCount(); got != 16 {
		t.Fatalf("active must be 16 after drain, got %d", got)
	}
	var wg sync.WaitGroup
	for i := range 16 {
		seq := uint64(i + 1)
		wg.Go(func() {
			err := client.CompleteCall("/svc", serviceTestGUID(), seq)
			if err != nil {
				t.Errorf("concurrent complete %d: %v", seq, err)
			}
		})
		wg.Go(func() {
			err := server.Reply("/svc", serviceTestGUID(), seq)
			if err != nil {
				t.Errorf("concurrent reply %d: %v", seq, err)
			}
		})
	}
	wg.Wait()
	if got := client.PendingCount(); got != 0 {
		t.Fatalf("pending must drain to 0, got %d", got)
	}
	if got := server.ActiveCount(); got != 0 {
		t.Fatalf("active must drain to 0, got %d", got)
	}
}
