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
	return &Publisher{node: n, topic: topic, typeName: typeName, qos: qos}, nil
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
