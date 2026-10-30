// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package sdk is the designed Go API for Weld robot programs.
//
// Decided pre-v1: where the design doc leaves a choice open, the SDK
// picks the smallest option. Topics carry byte payloads, services use
// context deadlines, config is a versioned store with schema and watch,
// time is one domain per context, supervision offers serial and pooled
// groups. Delivery runs over the in-memory bus so tests pass without
// live ROS; the SDK never imports transport packages.
package sdk

import (
	"errors"
	"sync"
)

// Node is one supervised participant in a fleet.
//
// Construction takes an explicit shared bus instead of an options
// struct; a nil bus means an isolated private bus. No global state exists:
// two nodes never share hidden singletons.
type Node struct {
	// mu guards closed plus the tracked entity lists below.
	mu sync.Mutex
	// name identifies the node for logs and error context.
	name string
	// bus carries topics and services between nodes sharing it.
	bus *Bus
	// config is the node versioned configuration store.
	config *Store
	// clock is the node single clock domain.
	clock *Clock
	// done closes when the node closes.
	done chan struct{}
	// closed marks teardown; Close is idempotent.
	closed bool
	// subs tracks subscriptions for teardown.
	subs []*Subscription
	// servers tracks service servers for teardown.
	servers []*ServiceServer
}

// NewNode returns a node with its own config store and wall clock.
//
// The node starts in the wall domain with zero wall and steady
// ticks; callers switch to sim explicitly. Name validation is minimal:
// non-empty only, namespace rules are deferred to tooling review.
func NewNode(name string, bus *Bus) (*Node, error) {
	if name == "" {
		return nil, errors.New("sdk: node name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if bus == nil {
		bus = NewBus()
	}
	n := &Node{
		name:   name,
		bus:    bus,
		config: NewStore(),
		clock:  NewClock(DomainWall, 0, 0),
		done:   make(chan struct{}),
	}
	return n, nil
}

// Name returns the node name. A nil node reports empty.
func (n *Node) Name() string {
	if n == nil {
		return ""
	}
	return n.name
}

// Bus returns the shared in-memory bus. A nil node returns nil.
func (n *Node) Bus() *Bus {
	if n == nil {
		return nil
	}
	return n.bus
}

// Config returns the node versioned configuration store. A nil node returns nil.
func (n *Node) Config() *Store {
	if n == nil {
		return nil
	}
	return n.config
}

// Clock returns the node single clock domain. A nil node returns nil.
func (n *Node) Clock() *Clock {
	if n == nil {
		return nil
	}
	return n.clock
}

// Done returns a channel closed when the node closes. A nil node returns nil.
func (n *Node) Done() <-chan struct{} {
	if n == nil {
		return nil
	}
	return n.done
}

// IsClosed reports whether Close ran. A nil node reports closed.
func (n *Node) IsClosed() bool {
	if n == nil {
		return true
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.closed
}

// trackSub records one subscription for teardown. A nil node drops it.
func (n *Node) trackSub(sub *Subscription) {
	if n == nil || sub == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	n.subs = append(n.subs, sub)
}

// trackServer records one service server for teardown. A nil node drops it.
func (n *Node) trackServer(srv *ServiceServer) {
	if n == nil || srv == nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closed {
		return
	}
	n.servers = append(n.servers, srv)
}

// Close releases node resources and is idempotent.
//
// Close marks the node closed, closes Done, then closes tracked
// subscriptions and unregisters tracked servers. Later Publish and Call
// fail loudly instead of flowing. A second call returns nil.
func (n *Node) Close() error {
	if n == nil {
		return errors.New("sdk: close nil node") //nolint:wrapcheck // new error, nothing to wrap
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	close(n.done)
	subs := make([]*Subscription, len(n.subs))
	copy(subs, n.subs)
	servers := make([]*ServiceServer, len(n.servers))
	copy(servers, n.servers)
	n.mu.Unlock()
	for _, sub := range subs {
		_ = sub.Close()
	}
	for _, srv := range servers {
		_ = srv.Close()
	}
	return nil
}
