// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// DefaultServiceTimeout returns the per call service timeout.
//
// The value is five seconds, matching the documented service timeout in
// wireless and design notes. Callers use it when the request carries no
// explicit timeout. Timeouts attach at the transition that starts the call,
// never recomputed later.
func DefaultServiceTimeout() Duration {
	return NewDuration(5, 0)
}

// ServiceTraceKind names one service introspection event.
type ServiceTraceKind int

// Service trace kinds.
const (
	// ServiceTraceRequestSent fires on clients when a request starts.
	ServiceTraceRequestSent ServiceTraceKind = iota
	// ServiceTraceResponseReceived fires on clients when a reply arrives.
	ServiceTraceResponseReceived
	// ServiceTraceRequestReceived fires on servers when a request arrives.
	ServiceTraceRequestReceived
	// ServiceTraceResponseSent fires on servers when a reply is sent.
	ServiceTraceResponseSent
)

// String renders the kind in lowercase form.
func (k ServiceTraceKind) String() string {
	switch k {
	case ServiceTraceRequestSent:
		return "request_sent"
	case ServiceTraceResponseReceived:
		return "response_received"
	case ServiceTraceRequestReceived:
		return "request_received"
	case ServiceTraceResponseSent:
		return "response_sent"
	default:
		return "invalid"
	}
}

// ServiceTrace is one service introspection event.
//
// It carries the service name plus the routing key (client GUID plus
// sequence) so operators can pair request and response legs. The GUID is the
// 8 byte wire service GUID from the CDR envelope, distinct from the 16 byte
// DDS participant GUID used on discovery.
type ServiceTrace struct {
	// Service is the ROS service name such as /add_two_ints.
	Service string
	// Client is the 8 byte wire client GUID.
	Client [8]byte
	// Sequence is the per client sequence number.
	Sequence uint64
	// Kind names which transition fired.
	Kind ServiceTraceKind
}

// String renders the trace as one log line.
func (t ServiceTrace) String() string {
	return fmt.Sprintf("service=%s kind=%s client=%s seq=%d",
		t.Service, t.Kind.String(), hex.EncodeToString(t.Client[:]), t.Sequence)
}

// ServiceTraceHook observes service transitions for introspection.
//
// Hooks run outside the tracker lock and must not call back into the tracker.
// A nil hook drops events. Hooks fire only when introspection is enabled.
type ServiceTraceHook func(ServiceTrace)

// ServiceQoS is one input QoS fanned out to request and response entities.
//
// One requested profile produces two wire entities: a request leg and a
// response leg. Both legs carry the same resolved profile because services
// are plain RPC with no history replay. Clients use the request leg as
// publisher and the response leg as subscription; servers use the reverse.
type ServiceQoS struct {
	// Service is the ROS service name.
	Service string
	// Request is the resolved request leg profile.
	Request QoSProfile
	// Response is the resolved response leg profile.
	Response QoSProfile
}

// ClientRequestPublisher returns the client request publisher profile.
func (q ServiceQoS) ClientRequestPublisher() QoSProfile {
	return q.Request
}

// ClientResponseSubscription returns the client response subscription profile.
func (q ServiceQoS) ClientResponseSubscription() QoSProfile {
	return q.Response
}

// ServerRequestSubscription returns the server request subscription profile.
func (q ServiceQoS) ServerRequestSubscription() QoSProfile {
	return q.Request
}

// ServerResponsePublisher returns the server response publisher profile.
func (q ServiceQoS) ServerResponsePublisher() QoSProfile {
	return q.Response
}

// ResolveServiceQoS fans one requested profile out to both service legs.
//
// Resolution order: reject an empty service name; resolve system defaults;
// when any best available marker is set, replace only the marker fields with
// the services default, preserving explicit fields; validate the stored
// profile. The avoidance flag passes through unresolved per the actual
// getter rule. When the resolved durability is transient local, the returned
// warning names the service and the durability policy because services do
// not use durability. A nil warning means no warning.
func ResolveServiceQoS(service string, requested QoSProfile) (ServiceQoS, *MismatchEvent, error) {
	if service == "" {
		return ServiceQoS{}, nil, errors.New("core: resolve service QoS: service name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	resolved := ResolveSystemDefaults(requested)
	if hasServiceMarker(requested) {
		resolved = resolveServiceProfile(requested, resolved)
	}
	err := Validate(resolved)
	if err != nil {
		return ServiceQoS{}, nil, fmt.Errorf("core: resolve service QoS on %q: %w", service, err)
	}
	out := ServiceQoS{Service: service, Request: resolved, Response: resolved}
	warning, ok := serviceDurabilityWarning(service, resolved)
	if !ok {
		return out, nil, nil
	}
	return out, &warning, nil
}

// hasServiceMarker reports whether p carries any best available marker,
// including the duration sentinel on deadline, lifespan, or lease duration.
func hasServiceMarker(p QoSProfile) bool {
	return p.Reliability == ReliabilityBestAvailable ||
		p.Durability == DurabilityBestAvailable ||
		p.Liveliness == LivelinessBestAvailable ||
		IsBestAvailableDuration(p.Deadline) ||
		IsBestAvailableDuration(p.Lifespan) ||
		IsBestAvailableDuration(p.LeaseDuration)
}

// resolveServiceProfile merges per field: every best available marker field
// in requested takes the resolved services default, every explicit field
// keeps the system default resolved value.
func resolveServiceProfile(requested, resolved QoSProfile) QoSProfile {
	return resolveServiceBestAvailable(requested, resolved)
}

// serviceDurabilityWarning returns a warning when the resolved service
// profile carries transient local durability, which services do not use.
func serviceDurabilityWarning(service string, p QoSProfile) (MismatchEvent, bool) {
	if p.Durability != DurabilityTransientLocal {
		return MismatchEvent{}, false
	}
	return MismatchEvent{
		Topic:      service,
		Publisher:  "server",
		Subscriber: "client",
		Policy:     PolicyDurability,
		Offered:    p.Durability.String(),
		Requested:  p.Durability.String(),
		Severity:   SeverityWarning,
		Detail:     "services do not use durability; prefer volatile",
	}, true
}

// ServiceCallKey is the exact one to one routing key for one call.
//
// The service name scopes the call so request and reply discovery stay
// independent: a reply matches only the same service plus client GUID plus
// sequence. The GUID is the 8 byte wire service GUID, never the 16 byte
// participant GUID.
type ServiceCallKey struct {
	// Service is the ROS service name.
	Service string
	// Client is the 8 byte wire client GUID.
	Client [8]byte
	// Sequence is the per client sequence number.
	Sequence uint64
}

// Service errors. Each wraps with service name plus GUID plus sequence at
// the call site so one gone or timed out peer errors exactly one call.
var (
	// ErrServiceTimeout reports a per call timeout with service name context.
	ErrServiceTimeout = errors.New("core: service call timeout")
	// ErrServiceServerGone reports a gone server for one pending call.
	ErrServiceServerGone = errors.New("core: service server gone")
	// ErrServiceClientGone reports a gone client for one pending request.
	ErrServiceClientGone = errors.New("core: service client gone")
	// ErrServiceDuplicate reports a duplicate sequence for one client.
	ErrServiceDuplicate = errors.New("core: service duplicate sequence")
	// ErrServiceNoPending reports a reply with no matching pending call.
	ErrServiceNoPending = errors.New("core: service no pending call")
	// ErrServiceNoRequest reports a server reply with no matching request.
	ErrServiceNoRequest = errors.New("core: service no pending request")
)

// validateServiceTimeout rejects malformed timeouts before they start a call.
//
// Negative seconds and nanoseconds at or above one second are malformed and
// must normalize with NewDuration first. Unspecified and infinite pass
// through: unspecified means use the default at the call site, infinite
// means no timeout.
func validateServiceTimeout(timeout Duration) error {
	if timeout.Seconds < 0 {
		return fmt.Errorf("core: service timeout: negative seconds are malformed: %w", ErrServiceTimeout)
	}
	if timeout.Nanoseconds >= 1000000000 {
		return fmt.Errorf("core: service timeout: nanoseconds >= 1e9 are malformed: %w", ErrServiceTimeout)
	}
	return nil
}

// effectiveServiceTimeout returns timeout unless unspecified, in which case
// it returns the five second default.
func effectiveServiceTimeout(timeout Duration) Duration {
	if timeout.IsUnspecified() {
		return DefaultServiceTimeout()
	}
	return timeout
}

// computeServiceDeadline returns the absolute expiry in the now domain.
//
// Infinite never expires and maps to max int64. Other totals add saturating
// at max int64 so a fake clock near the top still terminates cleanly.
func computeServiceDeadline(startNanos int64, timeout Duration) int64 {
	eff := effectiveServiceTimeout(timeout)
	if eff.IsInfinite() {
		return math.MaxInt64
	}
	total := eff.TotalNanoseconds()
	if total > math.MaxInt64-startNanos {
		return math.MaxInt64
	}
	return startNanos + total
}

// isServiceExpired reports whether now reached a finite deadline.
// Max int64 means infinite and never expires.
func isServiceExpired(nowNanos, deadlineNanos int64) bool {
	if deadlineNanos == math.MaxInt64 {
		return false
	}
	return nowNanos >= deadlineNanos
}

// isDoneClosed reports whether a context Done channel already closed.
// A nil channel never closes.
func isDoneClosed(done <-chan struct{}) bool {
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// defaultServiceNow returns wall nanos for trackers built without a clock.
func defaultServiceNow() int64 {
	return time.Now().UnixNano()
}

// normalizeServiceNow returns now unless nil, in which case it returns wall time.
func normalizeServiceNow(now func() int64) func() int64 {
	if now == nil {
		return defaultServiceNow
	}
	return now
}

// servicePendingCall is one outbound client call awaiting a reply.
type servicePendingCall struct {
	key           ServiceCallKey
	startNanos    int64
	deadlineNanos int64
	done          <-chan struct{}
}

// ServiceClient tracks outbound service calls by GUID plus sequence.
//
// The tracker is transport free: transports carry bytes, core carries keys.
// Client and server trackers are independent and never assume symmetric
// discovery. Do not copy a client after first use; build with
// NewServiceClient and use a pointer.
type ServiceClient struct {
	noCopy       noCopy
	mu           sync.Mutex
	now          func() int64
	pending      map[ServiceCallKey]servicePendingCall
	traceEnabled bool
	traceHook    ServiceTraceHook
	timeouts     uint64
	gone         uint64
}

// NewServiceClient returns an empty client tracker.
//
// The now function supplies nanoseconds in an arbitrary domain for timeout
// math; tests inject a fake clock, production passes nil for wall time.
// When enabled is true and hook is non nil, BeginCall emits request sent and
// CompleteCall emits response received with sequence and client GUID. When
// enabled is false nothing emits.
func NewServiceClient(now func() int64, enabled bool, hook ServiceTraceHook) *ServiceClient {
	return &ServiceClient{
		now:          normalizeServiceNow(now),
		pending:      make(map[ServiceCallKey]servicePendingCall),
		traceEnabled: enabled,
		traceHook:    hook,
	}
}

// SetIntrospection replaces the introspection gate and hook.
// When enabled is false no hook fires. A nil hook drops events.
func (c *ServiceClient) SetIntrospection(enabled bool, hook ServiceTraceHook) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traceEnabled = enabled
	c.traceHook = hook
}

// emitClientTrace calls the hook outside the lock when gated on.
func (c *ServiceClient) emitClientTrace(trace ServiceTrace, enabled bool, hook ServiceTraceHook) {
	if !enabled || hook == nil {
		return
	}
	hook(trace)
}

// BeginCall starts one call and captures its timeout at start.
//
// It validates the service name, rejects a nil context, returns the context
// error when already done, rejects malformed timeouts, and rejects a
// duplicate key for exact one to one routing. Unspecified timeout means the
// five second default; infinite means no timeout. On success it emits
// request sent with sequence and client GUID when introspection is enabled.
func (c *ServiceClient) BeginCall(ctx context.Context, service string, client [8]byte, seq uint64, timeout Duration) error {
	if c == nil {
		return errors.New("core: begin service call: nil client") //nolint:wrapcheck // new error, nothing to wrap
	}
	if service == "" {
		return errors.New("core: begin service call: service name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if ctx == nil {
		return fmt.Errorf("core: begin service call on %q seq %d: nil context: %w", service, seq, ErrServiceTimeout)
	}
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("core: begin service call on %q seq %d: %w", service, seq, err)
	}
	err = validateServiceTimeout(timeout)
	if err != nil {
		return fmt.Errorf("core: begin service call on %q seq %d: %w", service, seq, err)
	}
	key := ServiceCallKey{Service: service, Client: client, Sequence: seq}
	c.mu.Lock()
	if _, dup := c.pending[key]; dup {
		c.mu.Unlock()
		return fmt.Errorf("core: begin service call on %q seq %d: %w", service, seq, ErrServiceDuplicate)
	}
	start := c.now()
	deadline := computeServiceDeadline(start, timeout)
	c.pending[key] = servicePendingCall{key: key, startNanos: start, deadlineNanos: deadline, done: ctx.Done()}
	enabled := c.traceEnabled
	hook := c.traceHook
	c.mu.Unlock()
	c.emitClientTrace(ServiceTrace{Service: service, Client: client, Sequence: seq, Kind: ServiceTraceRequestSent}, enabled, hook)
	return nil
}

// takeServiceEntry removes key from m under mu when present, capturing the
// trace gate for an outside lock emit. It reports presence with the gate.
func takeServiceEntry[V any](mu *sync.Mutex, m map[ServiceCallKey]V, key ServiceCallKey, gate func() (bool, ServiceTraceHook)) (bool, bool, ServiceTraceHook) {
	mu.Lock()
	if _, ok := m[key]; !ok {
		mu.Unlock()
		return false, false, nil
	}
	delete(m, key)
	enabled, hook := gate()
	mu.Unlock()
	return true, enabled, hook
}

// finishServiceEntry builds the routing key, takes the entry under mu,
// maps absence with missing, and emits trace outside the lock for the
// removed entry. Callers check receiver and service name first because
// those errors differ per side.
func finishServiceEntry[V any](mu *sync.Mutex, m map[ServiceCallKey]V, service string, client [8]byte, seq uint64, gate func() (bool, ServiceTraceHook), missing func(ServiceCallKey) error, emit func(trace ServiceTrace, enabled bool, hook ServiceTraceHook), kind ServiceTraceKind) error {
	key := ServiceCallKey{Service: service, Client: client, Sequence: seq}
	present, enabled, hook := takeServiceEntry(mu, m, key, gate)
	if !present {
		return missing(key)
	}
	emit(ServiceTrace{Service: service, Client: client, Sequence: seq, Kind: kind}, enabled, hook)
	return nil
}

// CompleteCall routes one reply by GUID plus sequence to its pending call.
//
// The key must match service plus client GUID plus sequence exactly; any
// mismatch returns a no pending error and leaves state unchanged. On success
// the entry is removed and response received emits with sequence and client
// GUID when introspection is enabled.
func (c *ServiceClient) CompleteCall(service string, client [8]byte, seq uint64) error {
	if c == nil {
		return errors.New("core: complete service call: nil client") //nolint:wrapcheck // new error, nothing to wrap
	}
	if service == "" {
		return errors.New("core: complete service call: service name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	return finishServiceEntry(&c.mu, c.pending, service, client, seq, func() (bool, ServiceTraceHook) {
		return c.traceEnabled, c.traceHook
	}, func(ServiceCallKey) error {
		return fmt.Errorf("core: complete service call on %q seq %d: %w", service, seq, ErrServiceNoPending)
	}, c.emitClientTrace, ServiceTraceResponseReceived)
}

// FailServerGone errors exactly one pending call for a gone server.
//
// The key must match exactly. The entry is removed and the returned error
// wraps ErrServiceServerGone with service name plus sequence.
func (c *ServiceClient) FailServerGone(service string, client [8]byte, seq uint64) error {
	if c == nil {
		return errors.New("core: fail service server gone: nil client") //nolint:wrapcheck // new error, nothing to wrap
	}
	if service == "" {
		return errors.New("core: fail service server gone: service name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	key := ServiceCallKey{Service: service, Client: client, Sequence: seq}
	c.mu.Lock()
	if _, ok := c.pending[key]; !ok {
		c.mu.Unlock()
		return fmt.Errorf("core: fail service call on %q seq %d: %w", service, seq, ErrServiceNoPending)
	}
	delete(c.pending, key)
	c.gone++
	c.mu.Unlock()
	return fmt.Errorf("core: service call on %q seq %d: %w", service, seq, ErrServiceServerGone)
}

// FailAllServerGone errors every pending call for one gone server.
//
// Each call errors once with its own key. The return is the number of calls
// failed. An empty service name fails nothing and returns zero.
func (c *ServiceClient) FailAllServerGone(service string) int {
	if c == nil || service == "" {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for key := range c.pending {
		if key.Service != service {
			continue
		}
		delete(c.pending, key)
		count++
	}
	c.gone += uint64(count)
	return count
}

// ExpireCalls removes calls whose deadline passed in the now domain.
//
// The nowNanos argument is explicit so fake clocks drive determinism. Each
// expired key maps to a timeout error with service name context at the call
// site. Infinite deadlines never expire. The return holds expired keys.
func (c *ServiceClient) ExpireCalls(nowNanos int64) []ServiceCallKey {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []ServiceCallKey
	for key, call := range c.pending {
		if !isServiceExpired(nowNanos, call.deadlineNanos) {
			continue
		}
		delete(c.pending, key)
		out = append(out, key)
	}
	c.timeouts += uint64(len(out))
	return out
}

// CollectCancelled removes calls whose context already closed.
//
// Transports poll this alongside ExpireCalls so per request context
// deadlines error one call even when the fake timeout clock has not moved.
// A nil Done channel never cancels.
func (c *ServiceClient) CollectCancelled() []ServiceCallKey {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []ServiceCallKey
	for key, call := range c.pending {
		if !isDoneClosed(call.done) {
			continue
		}
		delete(c.pending, key)
		out = append(out, key)
	}
	return out
}

// PendingCount returns the number of calls awaiting replies.
func (c *ServiceClient) PendingCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// HasPending reports whether the exact key is awaiting a reply.
func (c *ServiceClient) HasPending(service string, client [8]byte, seq uint64) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.pending[ServiceCallKey{Service: service, Client: client, Sequence: seq}]
	return ok
}

// TimeoutCount returns the monotonic count of calls removed by ExpireCalls.
func (c *ServiceClient) TimeoutCount() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timeouts
}

// GoneCount returns the monotonic count of calls removed as gone server.
func (c *ServiceClient) GoneCount() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gone
}

// ServiceRequest is one server side request awaiting dispatch.
type ServiceRequest struct {
	// Service is the ROS service name.
	Service string
	// Client is the 8 byte wire client GUID.
	Client [8]byte
	// Sequence is the per client sequence number.
	Sequence uint64
	// DeadlineNanos is the absolute expiry in the server now domain.
	DeadlineNanos int64
}

// Key returns the exact routing key for the request.
func (r ServiceRequest) Key() ServiceCallKey {
	return ServiceCallKey{Service: r.Service, Client: r.Client, Sequence: r.Sequence}
}

// serviceQueuedRequest is one queued or active server request.
type serviceQueuedRequest struct {
	req   ServiceRequest
	done  <-chan struct{}
	start int64
}

// ServiceServer tracks inbound service requests with atomic take plus dispatch.
//
// Enqueue is the request arrival transition, Take is the atomic take plus
// dispatch step, and Reply is the response send transition. Take with nothing
// ready returns false and counts one empty signal with a metric instead of
// crashing. Do not copy a server after first use; build with
// NewServiceServer and use a pointer.
type ServiceServer struct {
	noCopy       noCopy
	mu           sync.Mutex
	now          func() int64
	queued       []serviceQueuedRequest
	queuedIndex  map[ServiceCallKey]struct{}
	active       map[ServiceCallKey]serviceQueuedRequest
	traceEnabled bool
	traceHook    ServiceTraceHook
	emptyTakes   uint64
	timeouts     uint64
	gone         uint64
}

// NewServiceServer returns an empty server tracker.
//
// The now function supplies nanoseconds for timeout math; tests inject a
// fake clock, production passes nil for wall time. When enabled is true and
// hook is non nil, EnqueueRequest emits request received and Reply emits
// response sent with sequence and client GUID. When enabled is false nothing
// emits.
func NewServiceServer(now func() int64, enabled bool, hook ServiceTraceHook) *ServiceServer {
	return &ServiceServer{
		now:          normalizeServiceNow(now),
		queuedIndex:  make(map[ServiceCallKey]struct{}),
		active:       make(map[ServiceCallKey]serviceQueuedRequest),
		traceEnabled: enabled,
		traceHook:    hook,
	}
}

// SetIntrospection replaces the introspection gate and hook.
// When enabled is false no hook fires. A nil hook drops events.
func (s *ServiceServer) SetIntrospection(enabled bool, hook ServiceTraceHook) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.traceEnabled = enabled
	s.traceHook = hook
}

// emitServerTrace calls the hook outside the lock when gated on.
func (s *ServiceServer) emitServerTrace(trace ServiceTrace, enabled bool, hook ServiceTraceHook) {
	if !enabled || hook == nil {
		return
	}
	hook(trace)
}

// EnqueueRequest records one inbound request and captures its timeout at start.
//
// It validates the service name, rejects a nil context, returns the context
// error when already done, rejects malformed timeouts, and rejects a
// duplicate key already queued or active. Unspecified timeout means the five
// second default; infinite means no timeout. On success it emits request
// received with sequence and client GUID when introspection is enabled.
func (s *ServiceServer) EnqueueRequest(ctx context.Context, service string, client [8]byte, seq uint64, timeout Duration) error {
	if s == nil {
		return errors.New("core: enqueue service request: nil server") //nolint:wrapcheck // new error, nothing to wrap
	}
	if service == "" {
		return errors.New("core: enqueue service request: service name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if ctx == nil {
		return fmt.Errorf("core: enqueue service request on %q seq %d: nil context: %w", service, seq, ErrServiceTimeout)
	}
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("core: enqueue service request on %q seq %d: %w", service, seq, err)
	}
	err = validateServiceTimeout(timeout)
	if err != nil {
		return fmt.Errorf("core: enqueue service request on %q seq %d: %w", service, seq, err)
	}
	key := ServiceCallKey{Service: service, Client: client, Sequence: seq}
	s.mu.Lock()
	if _, dup := s.queuedIndex[key]; dup {
		s.mu.Unlock()
		return fmt.Errorf("core: enqueue service request on %q seq %d: %w", service, seq, ErrServiceDuplicate)
	}
	if _, dup := s.active[key]; dup {
		s.mu.Unlock()
		return fmt.Errorf("core: enqueue service request on %q seq %d: %w", service, seq, ErrServiceDuplicate)
	}
	start := s.now()
	deadline := computeServiceDeadline(start, timeout)
	s.queued = append(s.queued, serviceQueuedRequest{
		req:   ServiceRequest{Service: service, Client: client, Sequence: seq, DeadlineNanos: deadline},
		done:  ctx.Done(),
		start: start,
	})
	if s.queuedIndex == nil {
		s.queuedIndex = make(map[ServiceCallKey]struct{})
	}
	s.queuedIndex[key] = struct{}{}
	enabled := s.traceEnabled
	hook := s.traceHook
	s.mu.Unlock()
	s.emitServerTrace(ServiceTrace{Service: service, Client: client, Sequence: seq, Kind: ServiceTraceRequestReceived}, enabled, hook)
	return nil
}

// TakeRequest atomically takes plus dispatches the oldest queued request.
//
// Take and dispatch are one step under the lock: the head moves from queued
// to active before return. With nothing ready it returns false and counts
// one empty signal; the counter is introspection for tests and metrics.
func (s *ServiceServer) TakeRequest() (ServiceRequest, bool) {
	if s == nil {
		return ServiceRequest{}, false
	}
	s.mu.Lock()
	if len(s.queued) == 0 {
		s.emptyTakes++
		s.mu.Unlock()
		return ServiceRequest{}, false
	}
	head := s.queued[0]
	var zero serviceQueuedRequest
	s.queued[0] = zero
	s.queued = s.queued[1:]
	delete(s.queuedIndex, head.req.Key())
	if s.active == nil {
		s.active = make(map[ServiceCallKey]serviceQueuedRequest)
	}
	s.active[head.req.Key()] = head
	req := head.req
	s.mu.Unlock()
	return req, true
}

// Reply sends one response for an active request.
//
// The key must match a taken request exactly; queued but not yet taken
// requests cannot reply. On success the entry leaves active and response
// sent emits with sequence and client GUID when introspection is enabled.
// A send to a missing request maps to a timeout style error with service
// name context because the reader is not ready.
func (s *ServiceServer) Reply(service string, client [8]byte, seq uint64) error {
	if s == nil {
		return errors.New("core: reply service request: nil server") //nolint:wrapcheck // new error, nothing to wrap
	}
	if service == "" {
		return errors.New("core: reply service request: service name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	return finishServiceEntry(&s.mu, s.active, service, client, seq, func() (bool, ServiceTraceHook) {
		return s.traceEnabled, s.traceHook
	}, func(ServiceCallKey) error {
		return fmt.Errorf("core: reply service request on %q seq %d: reader not ready: %w", service, seq, ErrServiceTimeout)
	}, s.emitServerTrace, ServiceTraceResponseSent)
}

// FailClientGone errors exactly one queued or active request for a gone client.
//
// The entry is removed from whichever set holds it and the returned error
// wraps ErrServiceClientGone with service name plus sequence. A missing key
// returns a no request error and changes nothing.
func (s *ServiceServer) FailClientGone(service string, client [8]byte, seq uint64) error {
	if s == nil {
		return errors.New("core: fail service client gone: nil server") //nolint:wrapcheck // new error, nothing to wrap
	}
	if service == "" {
		return errors.New("core: fail service client gone: service name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	key := ServiceCallKey{Service: service, Client: client, Sequence: seq}
	s.mu.Lock()
	if _, ok := s.active[key]; ok {
		delete(s.active, key)
		s.gone++
		s.mu.Unlock()
		return fmt.Errorf("core: service request on %q seq %d: %w", service, seq, ErrServiceClientGone)
	}
	if _, ok := s.queuedIndex[key]; !ok {
		s.mu.Unlock()
		return fmt.Errorf("core: fail service request on %q seq %d: %w", service, seq, ErrServiceNoRequest)
	}
	delete(s.queuedIndex, key)
	kept := s.queued[:0]
	for _, item := range s.queued {
		if item.req.Key() == key {
			continue
		}
		kept = append(kept, item)
	}
	for i := len(kept); i < len(s.queued); i++ {
		s.queued[i] = serviceQueuedRequest{}
	}
	s.queued = kept
	s.gone++
	s.mu.Unlock()
	return fmt.Errorf("core: service request on %q seq %d: %w", service, seq, ErrServiceClientGone)
}

// ExpireRequests removes queued and active requests past deadline.
//
// The nowNanos argument is explicit so fake clocks drive determinism.
// Infinite deadlines never expire. The return holds expired keys in queued
// then active order.
func (s *ServiceServer) ExpireRequests(nowNanos int64) []ServiceCallKey {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ServiceCallKey
	kept := s.queued[:0]
	for _, item := range s.queued {
		if !isServiceExpired(nowNanos, item.req.DeadlineNanos) {
			kept = append(kept, item)
			continue
		}
		delete(s.queuedIndex, item.req.Key())
		out = append(out, item.req.Key())
	}
	for i := len(kept); i < len(s.queued); i++ {
		s.queued[i] = serviceQueuedRequest{}
	}
	s.queued = kept
	for key, item := range s.active {
		if !isServiceExpired(nowNanos, item.req.DeadlineNanos) {
			continue
		}
		delete(s.active, key)
		out = append(out, key)
	}
	s.timeouts += uint64(len(out))
	return out
}

// CollectCancelled removes queued and active requests whose context closed.
//
// A nil Done channel never cancels. Transports poll this alongside
// ExpireRequests so gone clients with canceled contexts error one call.
func (s *ServiceServer) CollectCancelled() []ServiceCallKey {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []ServiceCallKey
	kept := s.queued[:0]
	for _, item := range s.queued {
		if !isDoneClosed(item.done) {
			kept = append(kept, item)
			continue
		}
		delete(s.queuedIndex, item.req.Key())
		out = append(out, item.req.Key())
	}
	for i := len(kept); i < len(s.queued); i++ {
		s.queued[i] = serviceQueuedRequest{}
	}
	s.queued = kept
	for key, item := range s.active {
		if !isDoneClosed(item.done) {
			continue
		}
		delete(s.active, key)
		out = append(out, key)
	}
	return out
}

// QueuedCount returns the number of requests awaiting Take.
func (s *ServiceServer) QueuedCount() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queued)
}

// ActiveCount returns the number of taken requests awaiting Reply.
func (s *ServiceServer) ActiveCount() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.active)
}

// EmptyTakeCount returns the monotonic count of Takes with nothing ready.
func (s *ServiceServer) EmptyTakeCount() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.emptyTakes
}

// TimeoutCount returns the monotonic count of requests removed by expiry.
func (s *ServiceServer) TimeoutCount() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.timeouts
}

// GoneCount returns the monotonic count of requests removed as gone client.
func (s *ServiceServer) GoneCount() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gone
}
