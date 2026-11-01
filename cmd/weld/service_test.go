// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// fakeServiceBackend is an in memory Backend stub with controllable behavior.
type fakeServiceBackend struct {
	mu             sync.Mutex
	joined         bool
	closed         bool
	publishErrs    []error
	publishCalls   int
	lastOut        transports.OutgoingMessage
	takeMsgs       []transports.IncomingMessage
	takeErrs       []error
	takeCalls      int
	subscribeCalls int
	subscribeTopic string
	subscribeType  string
	subscribeHash  string
	subscribeErr   error
}

// PublishBytes accepts one message or returns the queued error.
func (f *fakeServiceBackend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("fake service publish context: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	if !f.joined {
		return transports.ErrNotJoined
	}

	f.publishCalls++

	if len(f.publishErrs) > 0 {
		next := f.publishErrs[0]
		f.publishErrs = f.publishErrs[1:]

		return next
	}

	f.lastOut = msg

	return nil
}

// TakeBytes returns one queued message, queued error, or blocks for ctx.
func (f *fakeServiceBackend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("fake service take context: %w", err)
	}

	f.mu.Lock()

	if f.closed {
		f.mu.Unlock()

		return transports.IncomingMessage{}, transports.ErrBackendClosed
	}

	if !f.joined {
		f.mu.Unlock()

		return transports.IncomingMessage{}, transports.ErrNotJoined
	}

	f.takeCalls++

	if len(f.takeErrs) > 0 {
		next := f.takeErrs[0]
		f.takeErrs = f.takeErrs[1:]
		f.mu.Unlock()

		return transports.IncomingMessage{}, next
	}

	if len(f.takeMsgs) > 0 {
		next := f.takeMsgs[0]
		f.takeMsgs = f.takeMsgs[1:]
		f.mu.Unlock()

		return next, nil
	}

	f.mu.Unlock()

	<-ctx.Done()
	err = ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("fake service take context: %w", err)
	}

	return transports.IncomingMessage{}, context.DeadlineExceeded
}

// JoinDiscovery records a successful join.
func (f *fakeServiceBackend) JoinDiscovery(ctx context.Context, info transports.DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("fake service join context: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	f.joined = true

	return nil
}

// ReportLiveliness accepts one liveliness report after join.
func (f *fakeServiceBackend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("fake service liveliness context: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	if !f.joined {
		return transports.ErrNotJoined
	}

	return nil
}

// Close releases the stub.
func (f *fakeServiceBackend) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closed = true

	return nil
}

// Subscribe records a reader declaration.
func (f *fakeServiceBackend) Subscribe(ctx context.Context, topic, typeName, typeHash string) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("fake service subscribe context: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.subscribeCalls++
	f.subscribeTopic = topic
	f.subscribeType = typeName
	f.subscribeHash = typeHash

	if f.subscribeErr != nil {
		return f.subscribeErr
	}

	return nil
}

// plainServiceBackend implements Backend without Subscribe.
type plainServiceBackend struct {
	joined bool
}

// PublishBytes accepts one message without sending.
func (f *plainServiceBackend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("plain service publish context: %w", err)
	}

	_ = msg

	return nil
}

// TakeBytes reports no message without blocking.
func (f *plainServiceBackend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("plain service take context: %w", err)
	}

	return transports.IncomingMessage{}, context.DeadlineExceeded
}

// JoinDiscovery records a successful join.
func (f *plainServiceBackend) JoinDiscovery(ctx context.Context, info transports.DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("plain service join context: %w", err)
	}

	_ = info
	f.joined = true

	return nil
}

// ReportLiveliness accepts one report without tracking.
func (f *plainServiceBackend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("plain service liveliness context: %w", err)
	}

	_ = report

	return nil
}

// Close releases the stub.
func (f *plainServiceBackend) Close() error {
	return nil
}

// serviceTestGUID returns a fixed 8 byte GUID for deterministic tests.
func serviceTestGUID() [8]byte {
	return [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
}

// serviceOtherGUID returns a distinct 8 byte GUID for mismatch tests.
func serviceOtherGUID() [8]byte {
	return [8]byte{9, 9, 9, 9, 9, 9, 9, 9}
}

// mustEncodeServiceRequest encodes or fails the test.
func mustEncodeServiceRequest(t *testing.T, request string, guid [8]byte, seq uint64) []byte {
	t.Helper()

	wire, err := encodeServiceRequest(request, guid, seq)
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}

	return wire
}

// TestResolveServiceCallArgs checks flag wins plus positional fallback.
// checkServiceCallSuccess checks one successful args resolution.
func checkServiceCallSuccess(t *testing.T, flagService, flagRequest string, args []string, wantService, wantRequest, label string) {
	t.Helper()

	service, request, err := resolveServiceCallArgs(flagService, flagRequest, args)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if service != wantService || request != wantRequest {
		t.Fatalf("%s = (%q, %q), want (%q, %q)", label, service, request, wantService, wantRequest)
	}
}

// checkServiceCallFailure checks one failing args resolution keeps weld prefix.
func checkServiceCallFailure(t *testing.T, flagService, flagRequest string, args []string, label string) {
	t.Helper()

	_, _, err := resolveServiceCallArgs(flagService, flagRequest, args)
	if err == nil {
		t.Fatal(label + " must fail")
	}
	if !strings.HasPrefix(err.Error(), "weld:") {
		t.Fatalf("%s error = %q, want weld prefix", label, err.Error())
	}
}

func TestResolveServiceCallArgs(t *testing.T) {
	checkServiceCallSuccess(t, "/flag-svc", "flag-req", nil, "/flag-svc", "flag-req", "flag wins")
	checkServiceCallSuccess(t, "", "", []string{"/pos-svc", "pos-req"}, "/pos-svc", "pos-req", "two positionals")
	checkServiceCallFailure(t, "/flag-svc", "", nil, "flag service alone without request")
	checkServiceCallFailure(t, "", "", []string{"/only-svc"}, "one positional without request")
	checkServiceCallFailure(t, "/flag-svc", "", []string{""}, "empty request")
	checkServiceCallFailure(t, "", "", nil, "missing service")
	checkServiceCallFailure(t, "/svc", "flag-req", []string{"extra"}, "flag plus positional")
	checkServiceCallFailure(t, "", "", []string{"/svc", "one", "two"}, "three positionals")
}

// TestValidateServiceDeadline checks positive passes and zero plus negative fail.
func TestValidateServiceDeadline(t *testing.T) {
	err := validateServiceDeadline(5 * time.Second)
	if err != nil {
		t.Fatalf("positive deadline: %v", err)
	}

	err = validateServiceDeadline(0)
	if err == nil {
		t.Fatal("zero deadline must fail")
	}

	if !strings.HasPrefix(err.Error(), "weld:") {
		t.Fatalf("zero error = %q, want weld prefix", err.Error())
	}

	err = validateServiceDeadline(-time.Second)
	if err == nil {
		t.Fatal("negative deadline must fail")
	}

	if !strings.HasPrefix(err.Error(), "weld:") {
		t.Fatalf("negative error = %q, want weld prefix", err.Error())
	}
}

// TestCheckServiceEndpoints checks valid maps and bad names fail loudly.
func TestCheckServiceEndpoints(t *testing.T) {
	err := checkServiceEndpoints("/add_two_ints")
	if err != nil {
		t.Fatalf("valid service: %v", err)
	}

	err = checkServiceEndpoints("///")
	if err == nil {
		t.Fatal("bad service must fail")
	}

	if !strings.HasPrefix(oneLine(err), "weld:") {
		t.Fatalf("bad service error = %q, want weld prefix", err.Error())
	}

	err = checkServiceEndpoints("")
	if err == nil {
		t.Fatal("empty service must fail")
	}

	if !strings.HasPrefix(oneLine(err), "weld:") {
		t.Fatalf("empty error = %q, want weld prefix", err.Error())
	}
}

// TestEncodeDecodeServiceRoundTrip checks GUID plus sequence plus string survive.
func TestEncodeDecodeServiceRoundTrip(t *testing.T) {
	guid := serviceTestGUID()

	wire := mustEncodeServiceRequest(t, "hello service", guid, 7)

	got, matched, err := decodeServiceReply(wire, guid, 7)
	if err != nil {
		t.Fatalf("decode round trip: %v", err)
	}

	if !matched {
		t.Fatal("round trip must match")
	}

	if got != "hello service" {
		t.Fatalf("round trip = %q, want hello service", got)
	}
}

// TestDecodeServiceMismatch checks wrong GUID plus sequence plus service are ignored.
func TestDecodeServiceMismatch(t *testing.T) {
	guid := serviceTestGUID()
	other := serviceOtherGUID()

	wire := mustEncodeServiceRequest(t, "payload", guid, 1)

	_, matched, err := decodeServiceReply(wire, other, 1)
	if err != nil {
		t.Fatalf("wrong guid decode: %v", err)
	}

	if matched {
		t.Fatal("wrong guid must not match")
	}

	_, matched, err = decodeServiceReply(wire, guid, 2)
	if err != nil {
		t.Fatalf("wrong seq decode: %v", err)
	}

	if matched {
		t.Fatal("wrong sequence must not match")
	}
}

// TestDecodeServiceTrailing checks trailing bytes fail loudly.
func TestDecodeServiceTrailing(t *testing.T) {
	guid := serviceTestGUID()

	wire := mustEncodeServiceRequest(t, "ok", guid, 1)
	wire = append(wire, 0xFF)

	_, matched, err := decodeServiceReply(wire, guid, 1)
	if err == nil {
		t.Fatal("trailing bytes must fail")
	}

	if !matched {
		t.Fatal("trailing failure must still report matched")
	}

	if !strings.HasPrefix(err.Error(), "weld:") {
		t.Fatalf("trailing error = %q, want weld prefix", err.Error())
	}
}

// TestNewServiceGUID checks random GUID generation succeeds.
func TestNewServiceGUID(t *testing.T) {
	guid, err := newServiceGUID()
	if err != nil {
		t.Fatalf("new GUID: %v", err)
	}

	if len(guid[:]) != 8 {
		t.Fatalf("GUID length = %d, want 8", len(guid[:]))
	}
}

// TestSubscribeServiceReply checks Subscribe wiring plus plain fallback.
func TestSubscribeServiceReply(t *testing.T) {
	ctx := context.Background()

	fb := &fakeServiceBackend{joined: true}
	err := subscribeServiceReply(ctx, fb, "/svc")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if fb.subscribeCalls != 1 {
		t.Fatalf("subscribe calls = %d, want 1", fb.subscribeCalls)
	}

	if fb.subscribeTopic != "/svc" {
		t.Fatalf("subscribe topic = %q, want /svc", fb.subscribeTopic)
	}

	if fb.subscribeType != serviceResponseIDL {
		t.Fatalf("subscribe type = %q, want %q", fb.subscribeType, serviceResponseIDL)
	}

	plain := &plainServiceBackend{joined: true}
	err = subscribeServiceReply(ctx, plain, "/svc")
	if err != nil {
		t.Fatalf("plain subscribe must be no-op: %v", err)
	}

	fail := &fakeServiceBackend{joined: true, subscribeErr: errors.New("boom subscribe")}
	err = subscribeServiceReply(ctx, fail, "/svc")
	if err == nil {
		t.Fatal("failing subscribe must fail")
	}

	if !strings.HasPrefix(oneLine(err), "weld:") {
		t.Fatalf("subscribe error = %q, want weld prefix", err.Error())
	}

	if !strings.Contains(err.Error(), "/svc") {
		t.Fatalf("subscribe error misses service: %v", err)
	}
}

// TestPublishServiceRequestFlaky checks two no-route retries then success.
func TestPublishServiceRequestFlaky(t *testing.T) {
	ctx := context.Background()
	fb := &fakeServiceBackend{joined: true}

	noRoute := errors.Join(errors.New("dds: send without route"), dds.ErrNoRoute)
	fb.publishErrs = append(fb.publishErrs, noRoute, noRoute)

	out := transports.OutgoingMessage{TopicName: "/svc", TypeName: serviceRequestIDL, Payload: []byte{1, 2, 3}}

	err := publishServiceRequest(ctx, fb, out, 2*time.Second)
	if err != nil {
		t.Fatalf("flaky publish: %v", err)
	}

	if fb.publishCalls != 3 {
		t.Fatalf("flaky calls = %d, want 3", fb.publishCalls)
	}
}

// TestPublishServiceRequestTimeout checks deadline preserves no-route loudly.
func TestPublishServiceRequestTimeout(t *testing.T) {
	ctx := context.Background()
	fb := &fakeServiceBackend{joined: true}

	noRoute := errors.Join(errors.New("dds: send without route"), dds.ErrNoRoute)
	for range 100 {
		fb.publishErrs = append(fb.publishErrs, noRoute)
	}

	out := transports.OutgoingMessage{TopicName: "/svc", TypeName: serviceRequestIDL, Payload: []byte{9}}

	start := time.Now()
	err := publishServiceRequest(ctx, fb, out, 150*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("timeout publish must fail")
	}

	if !errors.Is(err, dds.ErrNoRoute) {
		t.Fatalf("timeout must preserve no-route, got %v", err)
	}

	if elapsed < 100*time.Millisecond {
		t.Fatalf("timeout elapsed %v too short", elapsed)
	}
}

// TestPublishServiceRequestCancel checks canceled context returns cancel fast.
func TestPublishServiceRequestCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fb := &fakeServiceBackend{joined: true}
	fb.publishErrs = append(fb.publishErrs, errors.Join(errors.New("x"), dds.ErrNoRoute))

	out := transports.OutgoingMessage{TopicName: "/svc", TypeName: serviceRequestIDL, Payload: []byte{9}}

	err := publishServiceRequest(ctx, fb, out, 2*time.Second)
	if err == nil {
		t.Fatal("canceled publish must fail")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publish = %v, want canceled", err)
	}
}

// TestServiceCallExactRouting checks mismatch plus topic filter plus match.
func TestServiceCallExactRouting(t *testing.T) {
	ctx := context.Background()
	fb := &fakeServiceBackend{joined: true}

	guid := serviceTestGUID()
	other := serviceOtherGUID()

	mismatchWire := mustEncodeServiceRequest(t, "wrong", other, 1)
	matchWire := mustEncodeServiceRequest(t, "right", guid, 1)

	fb.takeMsgs = append(fb.takeMsgs,
		transports.IncomingMessage{TopicName: "/other", TypeName: serviceResponseIDL, Payload: matchWire},
		transports.IncomingMessage{TopicName: "/svc", TypeName: serviceResponseIDL, Payload: mismatchWire},
		transports.IncomingMessage{TopicName: "/svc", TypeName: serviceResponseIDL, Payload: matchWire},
	)

	got, err := serviceCallWithBackend(ctx, fb, "/svc", "req", guid, 1, 2*time.Second)
	if err != nil {
		t.Fatalf("exact routing: %v", err)
	}

	if got != "right" {
		t.Fatalf("exact routing = %q, want right", got)
	}

	if fb.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", fb.publishCalls)
	}

	if fb.takeCalls != 3 {
		t.Fatalf("take calls = %d, want 3", fb.takeCalls)
	}

	if fb.lastOut.TopicName != "/svc" {
		t.Fatalf("publish topic = %q, want /svc", fb.lastOut.TopicName)
	}

	if fb.lastOut.TypeName != serviceRequestIDL {
		t.Fatalf("publish type = %q, want %q", fb.lastOut.TypeName, serviceRequestIDL)
	}
}

// TestServiceCallSkippable checks stale samples are skipped.
func TestServiceCallSkippable(t *testing.T) {
	ctx := context.Background()
	fb := &fakeServiceBackend{joined: true}

	guid := serviceTestGUID()
	matchWire := mustEncodeServiceRequest(t, "after-drop", guid, 1)

	fb.takeErrs = append(fb.takeErrs, errors.Join(errors.New("stale"), dds.ErrStaleSequence))
	fb.takeMsgs = append(fb.takeMsgs,
		transports.IncomingMessage{TopicName: "/svc", TypeName: serviceResponseIDL, Payload: matchWire},
	)

	got, err := serviceCallWithBackend(ctx, fb, "/svc", "req", guid, 1, 2*time.Second)
	if err != nil {
		t.Fatalf("skippable: %v", err)
	}

	if got != "after-drop" {
		t.Fatalf("skippable = %q, want after-drop", got)
	}
}

// TestServiceCallTimeout checks deadline without reply is loud with service name.
func TestServiceCallTimeout(t *testing.T) {
	ctx := context.Background()
	fb := &fakeServiceBackend{joined: true}

	guid := serviceTestGUID()

	_, err := serviceCallWithBackend(ctx, fb, "/gone-svc", "req", guid, 9, 120*time.Millisecond)
	if err == nil {
		t.Fatal("timeout must fail")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout must wrap deadline, got %v", err)
	}

	if !strings.Contains(err.Error(), "/gone-svc") {
		t.Fatalf("timeout misses service name: %v", err)
	}

	if !strings.HasPrefix(oneLine(err), "weld:") {
		t.Fatalf("timeout error = %q, want weld prefix", err.Error())
	}
}

// TestServiceCallNoServer checks publish no-route is loud with service name.
func TestServiceCallNoServer(t *testing.T) {
	ctx := context.Background()
	fb := &fakeServiceBackend{joined: true}

	noRoute := errors.Join(errors.New("dds: send without route"), dds.ErrNoRoute)
	for range 100 {
		fb.publishErrs = append(fb.publishErrs, noRoute)
	}

	guid := serviceTestGUID()

	_, err := serviceCallWithBackend(ctx, fb, "/missing-svc", "req", guid, 3, 150*time.Millisecond)
	if err == nil {
		t.Fatal("no server must fail")
	}

	if !errors.Is(err, dds.ErrNoRoute) {
		t.Fatalf("no server must preserve no-route, got %v", err)
	}

	if !strings.Contains(err.Error(), "/missing-svc") {
		t.Fatalf("no server misses service name: %v", err)
	}

	if !strings.Contains(err.Error(), "no server") {
		t.Fatalf("no server misses reason: %v", err)
	}

	if !strings.HasPrefix(oneLine(err), "weld:") {
		t.Fatalf("no server error = %q, want weld prefix", err.Error())
	}
}

// TestServiceCallCorruptReply checks matched but corrupt payload fails loudly.
func TestServiceCallCorruptReply(t *testing.T) {
	ctx := context.Background()
	fb := &fakeServiceBackend{joined: true}

	guid := serviceTestGUID()
	wire := mustEncodeServiceRequest(t, "ok", guid, 1)
	wire = append(wire, 0xFF)

	fb.takeMsgs = append(fb.takeMsgs,
		transports.IncomingMessage{TopicName: "/svc", TypeName: serviceResponseIDL, Payload: wire},
	)

	_, err := serviceCallWithBackend(ctx, fb, "/svc", "req", guid, 1, 2*time.Second)
	if err == nil {
		t.Fatal("corrupt reply must fail")
	}

	if !strings.HasPrefix(oneLine(err), "weld:") {
		t.Fatalf("corrupt error = %q, want weld prefix", err.Error())
	}
}

// TestRunServiceCallPrintsResponse checks wiring prints one reply line.
func TestRunServiceCallPrintsResponse(t *testing.T) {
	ctx := context.Background()

	fb := &echoServiceBackend{joined: true}

	oldOpen := openServiceBackendFunc
	openServiceBackendFunc = func(ctx context.Context, domain uint32, participant string) (transports.Backend, error) {
		err := fb.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: participant, DomainID: domain})
		if err != nil {
			return nil, err
		}

		return fb, nil
	}
	defer func() {
		openServiceBackendFunc = oldOpen
	}()

	var stdout bytes.Buffer

	err := runServiceCall(ctx, "/svc-weld", "2+3", 42, 2*time.Second, &stdout)
	if err != nil {
		t.Fatalf("run service call: %v", err)
	}

	if strings.TrimSpace(stdout.String()) != "echo:2+3" {
		t.Fatalf("stdout = %q, want echo:2+3", stdout.String())
	}

	if fb.subscribeCalls != 1 {
		t.Fatalf("subscribe calls = %d, want 1", fb.subscribeCalls)
	}

	if fb.publishCalls != 1 {
		t.Fatalf("publish calls = %d, want 1", fb.publishCalls)
	}
}

// echoServiceBackend echoes requests as replies with the same GUID plus sequence.
type echoServiceBackend struct {
	mu             sync.Mutex
	joined         bool
	closed         bool
	publishCalls   int
	subscribeCalls int
	takeQueue      []transports.IncomingMessage
}

// JoinDiscovery records a successful join.
func (f *echoServiceBackend) JoinDiscovery(ctx context.Context, info transports.DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("echo service join context: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	f.joined = true

	return nil
}

// PublishBytes decodes the request and queues an echo reply with the same key.
func (f *echoServiceBackend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("echo service publish context: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	if !f.joined {
		return transports.ErrNotJoined
	}

	f.publishCalls++

	dec, err := types.NewDecoder(msg.Payload)
	if err != nil {
		return fmt.Errorf("echo service decode header: %w", err)
	}

	guid, seq, err := dec.ReadServiceHeader()
	if err != nil {
		return fmt.Errorf("echo service read header: %w", err)
	}

	req, err := dec.ReadString()
	if err != nil {
		return fmt.Errorf("echo service read request: %w", err)
	}

	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("echo service finish decode: %w", err)
	}

	replyWire, err := encodeServiceRequest("echo:"+req, guid, seq)
	if err != nil {
		return err
	}

	f.takeQueue = append(f.takeQueue, transports.IncomingMessage{
		TopicName: msg.TopicName,
		TypeName:  serviceResponseIDL,
		Payload:   replyWire,
	})

	return nil
}

// TakeBytes returns one queued echo reply or blocks for ctx.
func (f *echoServiceBackend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("echo service take context: %w", err)
	}

	f.mu.Lock()

	if f.closed {
		f.mu.Unlock()

		return transports.IncomingMessage{}, transports.ErrBackendClosed
	}

	if !f.joined {
		f.mu.Unlock()

		return transports.IncomingMessage{}, transports.ErrNotJoined
	}

	if len(f.takeQueue) > 0 {
		next := f.takeQueue[0]
		f.takeQueue = f.takeQueue[1:]
		f.mu.Unlock()

		return next, nil
	}

	f.mu.Unlock()

	<-ctx.Done()
	err = ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("echo service take context: %w", err)
	}

	return transports.IncomingMessage{}, context.DeadlineExceeded
}

// ReportLiveliness accepts one liveliness report after join.
func (f *echoServiceBackend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("echo service liveliness context: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return transports.ErrBackendClosed
	}

	if !f.joined {
		return transports.ErrNotJoined
	}

	return nil
}

// Close releases the stub.
func (f *echoServiceBackend) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.closed = true

	return nil
}

// Subscribe records a reader declaration.
func (f *echoServiceBackend) Subscribe(ctx context.Context, _, _, _ string) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("echo service subscribe context: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.subscribeCalls++

	return nil
}
