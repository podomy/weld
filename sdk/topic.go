// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// ErrLoanReturned reports use after return or a second return on a loaned message.
var ErrLoanReturned = errors.New("sdk: loan already returned")

// Reliability names the delivery retry behavior of a QoS profile.
type Reliability int

const (
	// ReliabilityReliable retries until delivery.
	ReliabilityReliable Reliability = iota
	// ReliabilityBestEffort sends once without retry.
	ReliabilityBestEffort
)

// String renders the policy in lowercase form.
func (r Reliability) String() string {
	switch r {
	case ReliabilityReliable:
		return "reliable"
	case ReliabilityBestEffort:
		return "best_effort"
	default:
		return "invalid"
	}
}

// Durability names the late joiner history behavior of a QoS profile.
type Durability int

const (
	// DurabilityVolatile keeps no history for late joiners.
	DurabilityVolatile Durability = iota
	// DurabilityTransientLocal keeps history for late joiners.
	DurabilityTransientLocal
)

// String renders the policy in lowercase form.
func (d Durability) String() string {
	switch d {
	case DurabilityVolatile:
		return "volatile"
	case DurabilityTransientLocal:
		return "transient_local"
	default:
		return "invalid"
	}
}

// QoS is the topic delivery contract: retry behavior, late joiner
// history, and per-subscriber queue depth.
//
// REVIEW: only reliability and durability gate delivery. Deadline,
// liveliness, lifespan, and history kind are deferred to user review;
// deadlines surface as alerts, not mismatches, in this SDK.
type QoS struct {
	// Reliability selects retry behavior.
	Reliability Reliability
	// Durability selects late joiner history.
	Durability Durability
	// Depth bounds each subscriber queue and must be positive.
	Depth int
}

// DefaultQoS returns reliable volatile with depth 10.
func DefaultQoS() QoS {
	return QoS{Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Depth: 10}
}

// SensorDataQoS returns best effort volatile with depth 5.
func SensorDataQoS() QoS {
	return QoS{Reliability: ReliabilityBestEffort, Durability: DurabilityVolatile, Depth: 5}
}

// Validate checks that depth is positive and policies are concrete.
func (q QoS) Validate() error {
	if q.Depth <= 0 {
		return fmt.Errorf("sdk: invalid QoS: depth %d must be positive", q.Depth)
	}
	switch q.Reliability {
	case ReliabilityReliable, ReliabilityBestEffort:
	default:
		return fmt.Errorf("sdk: invalid QoS: reliability %d is invalid", int(q.Reliability))
	}
	switch q.Durability {
	case DurabilityVolatile, DurabilityTransientLocal:
	default:
		return fmt.Errorf("sdk: invalid QoS: durability %d is invalid", int(q.Durability))
	}
	return nil
}

// Compatible reports whether offered bytes may flow to requested code.
//
// It returns false with a policy detail naming the loud mismatch instead
// of staying silent. Reliable offered satisfies any request; transient
// local offered satisfies any durability request.
func Compatible(offered, requested QoS) (bool, string) {
	if offered.Reliability == ReliabilityBestEffort && requested.Reliability == ReliabilityReliable {
		return false, "reliability: offered best_effort cannot satisfy requested reliable"
	}
	if offered.Durability == DurabilityVolatile && requested.Durability == DurabilityTransientLocal {
		return false, "durability: offered volatile keeps no history for requested transient_local"
	}
	return true, ""
}

// copyBytes returns an owned copy of payload with nil preserved.
func copyBytes(payload []byte) []byte {
	if payload == nil {
		return nil
	}
	out := make([]byte, len(payload))
	copy(out, payload)
	return out
}

// Publisher sends byte payloads on one topic.
//
// REVIEW: payloads are plain bytes; typed wrappers and type hash checks
// are deferred to user review. Publish copies once at ingress.
type Publisher struct {
	// mu guards closed.
	mu sync.Mutex
	// node owns this publisher for teardown checks.
	node *Node
	// topic is the fully qualified name such as /cmd_vel.
	topic string
	// typeName is the ROS IDL name such as geometry_msgs/msg/Twist.
	typeName string
	// qos is the validated offer profile.
	qos QoS
	// closed marks teardown; Close is idempotent.
	closed bool
}

// Topic returns the publisher topic name.
func (p *Publisher) Topic() string {
	if p == nil {
		return ""
	}
	return p.topic
}

// TypeName returns the publisher ROS type name.
func (p *Publisher) TypeName() string {
	if p == nil {
		return ""
	}
	return p.typeName
}

// QoS returns the publisher offer profile.
func (p *Publisher) QoS() QoS {
	if p == nil {
		return QoS{}
	}
	return p.qos
}

// Publish sends one sample; it copies the payload at ingress.
//
// It returns the context error when ctx already ended, or an error when
// the node or publisher is closed. Delivery never blocks: full
// subscribers drop oldest and count lost.
func (p *Publisher) Publish(ctx context.Context, payload []byte) error {
	if p == nil {
		return errors.New("sdk: publish on nil publisher") //nolint:wrapcheck // new error, nothing to wrap
	}
	if ctx == nil {
		return fmt.Errorf("sdk: publish on %q: nil context", p.topic)
	}
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("sdk: publish on %q: %w", p.topic, err)
	}
	p.mu.Lock()
	closed := p.closed
	nodeClosed := p.node.IsClosed()
	p.mu.Unlock()
	if closed {
		return fmt.Errorf("sdk: publish on %q: publisher closed", p.topic)
	}
	if nodeClosed {
		return fmt.Errorf("sdk: publish on %q: node closed", p.topic)
	}
	owned := copyBytes(payload)
	p.node.bus.publish(p.topic, owned)
	return nil
}

// Close marks the publisher closed and is idempotent.
func (p *Publisher) Close() error {
	if p == nil {
		return errors.New("sdk: close nil publisher") //nolint:wrapcheck // new error, nothing to wrap
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

// Subscription receives byte payloads on one topic.
//
// The channel is owned by the subscriber with depth from QoS and drop
// oldest overflow with a lost counter, never silent. Do not close the
// channel; call Close.
type Subscription struct {
	// mu guards closed during deliver and Close.
	mu sync.Mutex
	// node owns this subscription for teardown checks.
	node *Node
	// topic is the fully qualified name such as /cmd_vel.
	topic string
	// typeName is the ROS IDL name such as geometry_msgs/msg/Twist.
	typeName string
	// qos is the validated request profile.
	qos QoS
	// ch carries samples oldest first.
	ch chan []byte
	// lost counts overflow drops.
	lost atomic.Uint64
	// closed marks teardown; Close is idempotent.
	closed bool
}

// Topic returns the subscription topic name.
func (s *Subscription) Topic() string {
	if s == nil {
		return ""
	}
	return s.topic
}

// TypeName returns the subscription ROS type name.
func (s *Subscription) TypeName() string {
	if s == nil {
		return ""
	}
	return s.typeName
}

// QoS returns the subscription request profile.
func (s *Subscription) QoS() QoS {
	if s == nil {
		return QoS{}
	}
	return s.qos
}

// C returns the receive channel owned by the subscriber.
func (s *Subscription) C() <-chan []byte {
	if s == nil {
		return nil
	}
	return s.ch
}

// Lost returns the monotonic count of overflow drops.
func (s *Subscription) Lost() uint64 {
	if s == nil {
		return 0
	}
	return s.lost.Load()
}

// Len returns the queued sample count.
func (s *Subscription) Len() int {
	if s == nil {
		return 0
	}
	return len(s.ch)
}

// deliver queues one bus sample with drop oldest overflow.
func (s *Subscription) deliver(payload []byte) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	owned := copyBytes(payload)
	select {
	case s.ch <- owned:
		return
	default:
	}
	select {
	case <-s.ch:
	default:
	}
	s.lost.Add(1)
	s.ch <- owned
}

// Next waits for one sample or context end and returns an owned copy.
func (s *Subscription) Next(ctx context.Context) ([]byte, error) {
	if s == nil {
		return nil, errors.New("sdk: next on nil subscription") //nolint:wrapcheck // new error, nothing to wrap
	}
	if ctx == nil {
		return nil, fmt.Errorf("sdk: subscription next on %q: nil context", s.topic)
	}
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return nil, fmt.Errorf("sdk: subscription next on %q: %w", s.topic, err)
		}
		return nil, fmt.Errorf("sdk: subscription next on %q: context done", s.topic)
	case msg, ok := <-s.ch:
		if !ok {
			return nil, fmt.Errorf("sdk: subscription next on %q: subscription closed", s.topic)
		}
		return copyBytes(msg), nil
	}
}

// Close unsubscribes and drains; it is idempotent.
func (s *Subscription) Close() error {
	if s == nil {
		return errors.New("sdk: close nil subscription") //nolint:wrapcheck // new error, nothing to wrap
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	topic := s.topic
	node := s.node
	ch := s.ch
	s.mu.Unlock()
	if node != nil && node.bus != nil {
		node.bus.unsubscribe(topic, s)
	}
	close(ch)
	return nil
}

// NewPublisher creates a topic publisher with a validated QoS offer.
func (n *Node) NewPublisher(topic, typeName string, qos QoS) (*Publisher, error) {
	if n == nil {
		return nil, errors.New("sdk: new publisher on nil node") //nolint:wrapcheck // new error, nothing to wrap
	}
	if topic == "" {
		return nil, errors.New("sdk: publisher topic is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if typeName == "" {
		return nil, errors.New("sdk: publisher type name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := qos.Validate()
	if err != nil {
		return nil, fmt.Errorf("sdk: new publisher on %q: %w", topic, err)
	}
	if n.IsClosed() {
		return nil, fmt.Errorf("sdk: new publisher on %q: node closed", topic)
	}
	pub := &Publisher{node: n, topic: topic, typeName: typeName, qos: qos}
	n.trackPub(pub)
	if n.IsClosed() {
		err = pub.Close()
		if err != nil {
			return nil, fmt.Errorf("sdk: new publisher on %q: %w", topic, err)
		}
		return nil, fmt.Errorf("sdk: new publisher on %q: node closed", topic)
	}
	return pub, nil
}

// NewSubscription creates a topic subscriber with a validated QoS request.
func (n *Node) NewSubscription(topic, typeName string, qos QoS) (*Subscription, error) {
	if n == nil {
		return nil, errors.New("sdk: new subscription on nil node") //nolint:wrapcheck // new error, nothing to wrap
	}
	if topic == "" {
		return nil, errors.New("sdk: subscription topic is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if typeName == "" {
		return nil, errors.New("sdk: subscription type name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := qos.Validate()
	if err != nil {
		return nil, fmt.Errorf("sdk: new subscription on %q: %w", topic, err)
	}
	if n.IsClosed() {
		return nil, fmt.Errorf("sdk: new subscription on %q: node closed", topic)
	}
	sub := &Subscription{node: n, topic: topic, typeName: typeName, qos: qos, ch: make(chan []byte, qos.Depth)}
	n.bus.subscribe(topic, sub)
	n.trackSub(sub)
	if n.IsClosed() {
		err = sub.Close()
		if err != nil {
			return nil, fmt.Errorf("sdk: new subscription on %q: %w", topic, err)
		}
		return nil, fmt.Errorf("sdk: new subscription on %q: node closed", topic)
	}
	return sub, nil
}

// LoanPolicy selects zero-copy loan behavior for one publish or take.
//
// Loaning applies only within one process for eligible types; every other
// path copies. Use CanLoanOffer to check a concrete request before
// assuming a loan.
type LoanPolicy int

const (
	// LoanNever always copies at ingress and on take.
	LoanNever LoanPolicy = iota
	// LoanIfEligible permits a loan when the type is eligible and the
	// path is in-process; otherwise it falls back to copies.
	LoanIfEligible
)

// String renders the loan policy in lowercase form.
func (l LoanPolicy) String() string {
	switch l {
	case LoanNever:
		return "never"
	case LoanIfEligible:
		return "if_eligible"
	default:
		return "invalid"
	}
}

// LoanOptions carries the loan preference for one publish or take.
//
// InProcess must be true for a loan: cross-process and cross-vendor
// exchanges always copy because no shared-memory transport exists.
// Eligible reports whether the message type may loan, mirroring the
// transport CanLoan check for self-contained memcpy-safe types.
type LoanOptions struct {
	// Policy selects whether a loan may be used.
	Policy LoanPolicy
	// InProcess reports whether peer and local share one address space.
	InProcess bool
	// Eligible reports whether the message type may loan.
	Eligible bool
}

// DefaultLoanOptions returns the conservative default: never loan, so
// behavior matches plain Publish and Next copies exactly.
func DefaultLoanOptions() LoanOptions {
	return LoanOptions{Policy: LoanNever}
}

// CanLoanOffer answers whether a loan may be used for the given options.
//
// It is true only when the policy opts in, the path is in-process, and
// the type is eligible. Every other combination answers false with a
// reason naming the veto, so callers never assume a loan where a copy
// happens. It is pure with no I/O.
//
// Contract mirror: this SDK offer path stays stdlib only and must not
// import transport packages. The authorative eligibility checks live in
// types.CanLoan (introspection self containment) and dds.CanLoanType
// (local loan eligibility with reason). When loan rules change, update
// this comment plus types.CanLoan plus dds.CanLoanType together so the
// SDK offer and the transport checks keep identical veto semantics.
func CanLoanOffer(opts LoanOptions) (bool, string) {
	if opts.Policy != LoanIfEligible {
		return false, "sdk: loan policy never: exchange copies"
	}
	if !opts.InProcess {
		return false, "sdk: loan needs one shared process: exchange copies"
	}
	if !opts.Eligible {
		return false, "sdk: loan type not eligible: exchange copies"
	}
	return true, "sdk: in-process eligible loan"
}

// PublishWithLoan sends one sample with a loan preference.
//
// When the policy opts into loans it checks CanLoanOffer first and fails
// loudly with the ineligibility reason instead of silently copying, so a
// caller that asked for a loan never mistakes a copy for a loan. LoanNever
// always copies exactly like Publish. An eligible loan still copies once at
// ingress today because the in-memory bus has no shared-memory transport; a
// future in-process transport may loan eligible samples. It returns the
// context error when ctx already ended, or an error when the node or
// publisher is closed.
func (p *Publisher) PublishWithLoan(ctx context.Context, payload []byte, opts LoanOptions) error {
	if opts.Policy != LoanNever {
		ok, reason := CanLoanOffer(opts)
		if !ok {
			topic := ""
			if p != nil {
				topic = p.topic
			}
			return fmt.Errorf("sdk: publish with loan on %q refused: %s", topic, reason)
		}
	}
	return p.Publish(ctx, payload)
}

// LoanedMessage is one take with explicit return.
//
// Bytes stays valid until Return. Return consumes the handle: Bytes fails
// after return and a second Return is a loud error. The SDK bus path
// always copies, so NextLoaned documents the fallback and a future
// in-process transport may back this handle with a true loan.
type LoanedMessage struct {
	// mu guards valid across Bytes and Return.
	mu sync.Mutex
	// payload holds the owned sample bytes.
	payload []byte
	// valid is consumed by Return.
	valid bool
	// topic is the ROS topic the sample arrived on.
	topic string
	// typeName is the ROS IDL type of the sample.
	typeName string
}

// Topic returns the ROS topic the sample arrived on.
func (m *LoanedMessage) Topic() string {
	if m == nil {
		return ""
	}
	return m.topic
}

// TypeName returns the ROS IDL type of the sample.
func (m *LoanedMessage) TypeName() string {
	if m == nil {
		return ""
	}
	return m.typeName
}

// Valid reports whether the handle still owns its bytes.
func (m *LoanedMessage) Valid() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.valid
}

// Bytes exposes the sample bytes. It fails after Return.
func (m *LoanedMessage) Bytes() ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("sdk: loaned bytes on nil message: %w", ErrLoanReturned)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.valid {
		return nil, fmt.Errorf("sdk: loaned bytes after return: %w", ErrLoanReturned)
	}
	return m.payload, nil
}

// Return releases the handle exactly once.
func (m *LoanedMessage) Return() error {
	if m == nil {
		return fmt.Errorf("sdk: loaned return on nil message: %w", ErrLoanReturned)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.valid {
		return fmt.Errorf("sdk: loaned double return: %w", ErrLoanReturned)
	}
	m.valid = false
	m.payload = nil
	return nil
}

// NextLoaned waits for one sample or context end and returns a handle
// with explicit return.
//
// When the policy opts into loans it checks CanLoanOffer first and fails
// loudly with the ineligibility reason instead of silently copying. An
// eligible loan still copies once like Next today because the in-memory bus
// has no shared-memory transport; a future transport may back the handle
// with a true loan. The caller must call Return exactly once.
func (s *Subscription) NextLoaned(ctx context.Context, opts LoanOptions) (*LoanedMessage, error) {
	if opts.Policy != LoanNever {
		ok, reason := CanLoanOffer(opts)
		if !ok {
			topic := ""
			if s != nil {
				topic = s.topic
			}
			return nil, fmt.Errorf("sdk: subscription next loaned on %q refused: %s", topic, reason)
		}
	}
	msg, err := s.Next(ctx)
	if err != nil {
		return nil, err
	}
	topic := ""
	typeName := ""
	if s != nil {
		topic = s.topic
		typeName = s.typeName
	}
	return &LoanedMessage{payload: msg, valid: true, topic: topic, typeName: typeName}, nil
}
