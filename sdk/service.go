// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultServiceTimeout bounds a call when the context carries no deadline.
func defaultServiceTimeout() time.Duration {
	return 5 * time.Second
}

// Service errors. Each call site wraps them with service name and sequence
// so one gone or timed out peer errors exactly one call.
var (
	// ErrNoSuchService reports a call with no registered server.
	ErrNoSuchService = errors.New("sdk: no such service")
	// ErrServiceServerGone reports a call on a closed node or server.
	ErrServiceServerGone = errors.New("sdk: service server gone")
	// ErrServiceClientGone reports a call on a closed client or node.
	ErrServiceClientGone = errors.New("sdk: service client gone")
)

// ServiceHandler serves one request and returns one reply.
//
// Handlers receive a context derived from the call context, so deadlines
// propagate even to handlers that check ctx. Handlers must not retain the
// request slice; the caller copies at ingress.
type ServiceHandler func(ctx context.Context, req []byte) ([]byte, error)

// ServiceServer serves one service name with exact one to one routing.
//
// REVIEW: routing is a direct handler map on the shared bus; wire GUID
// plus sequence pairing is deferred to transports. Sequence numbers below
// exist only for error context.
type ServiceServer struct {
	// mu guards closed.
	mu sync.Mutex
	// node owns this server for teardown checks.
	node *Node
	// service is the ROS service name such as /add_two_ints.
	service string
	// handler serves calls outside the bus lock.
	handler ServiceHandler
	// closed marks teardown; Close is idempotent.
	closed bool
}

// Service returns the served service name.
func (s *ServiceServer) Service() string {
	if s == nil {
		return ""
	}
	return s.service
}

// Close unregisters the server and is idempotent.
func (s *ServiceServer) Close() error {
	if s == nil {
		return errors.New("sdk: close nil service server") //nolint:wrapcheck // new error, nothing to wrap
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	node := s.node
	service := s.service
	s.mu.Unlock()
	if node != nil && node.bus != nil {
		node.bus.unregisterService(service, s)
	}
	return nil
}

// isClosed reports whether Close ran.
func (s *ServiceServer) isClosed() bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// ServiceClient calls one service name with per call deadlines.
type ServiceClient struct {
	// node owns this client for teardown checks.
	node *Node
	// service is the ROS service name such as /add_two_ints.
	service string
	// seq numbers calls for error context only.
	seq atomic.Uint64
	// mu guards closed.
	mu sync.Mutex
	// closed marks teardown; Close is idempotent.
	closed bool
}

// Service returns the called service name.
func (c *ServiceClient) Service() string {
	if c == nil {
		return ""
	}
	return c.service
}

// Close marks the client closed and is idempotent.
func (c *ServiceClient) Close() error {
	if c == nil {
		return errors.New("sdk: close nil service client") //nolint:wrapcheck // new error, nothing to wrap
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// isClosed reports whether Close ran.
func (c *ServiceClient) isClosed() bool {
	if c == nil {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// serviceResult carries one handler outcome across the goroutine boundary.
type serviceResult struct {
	// reply holds the handler bytes on success.
	reply []byte
	// err holds the handler error on failure.
	err error
}

// Call routes one request to the server and returns one reply.
//
// It copies the request at ingress, applies the default timeout when ctx
// carries no deadline, and errors exactly one call when the server is
// gone or the context ends. Gone servers and canceled contexts never
// affect other calls.
func (c *ServiceClient) Call(ctx context.Context, req []byte) ([]byte, error) {
	if c == nil {
		return nil, errors.New("sdk: call on nil service client") //nolint:wrapcheck // new error, nothing to wrap
	}
	if ctx == nil {
		return nil, fmt.Errorf("sdk: service call on %q: nil context", c.service)
	}
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("sdk: service call on %q: %w", c.service, err)
	}
	if c.isClosed() {
		return nil, fmt.Errorf("sdk: service call on %q: %w", c.service, ErrServiceClientGone)
	}
	if c.node.IsClosed() {
		return nil, fmt.Errorf("sdk: service call on %q: %w", c.service, ErrServiceClientGone)
	}
	eff, cancel := withServiceTimeout(ctx)
	defer cancel()
	handler, ok := c.node.bus.lookupService(c.service)
	if !ok {
		return nil, fmt.Errorf("sdk: service call on %q: %w", c.service, ErrNoSuchService)
	}
	seq := c.seq.Add(1)
	owned := copyBytes(req)
	resCh := make(chan serviceResult, 1)
	go runServiceHandler(eff, handler, owned, resCh)
	select {
	case <-eff.Done():
		ctxErr := eff.Err()
		if ctxErr != nil {
			return nil, fmt.Errorf("sdk: service call on %q seq %d: %w", c.service, seq, ctxErr)
		}
		return nil, fmt.Errorf("sdk: service call on %q seq %d: context done", c.service, seq)
	case res := <-resCh:
		if res.err != nil {
			return nil, fmt.Errorf("sdk: service call on %q seq %d: %w", c.service, seq, res.err)
		}
		return res.reply, nil
	}
}

// withServiceTimeout applies the default timeout when ctx has none.
func withServiceTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	_, ok := ctx.Deadline()
	if !ok {
		eff, cancel := context.WithTimeout(ctx, defaultServiceTimeout())
		return eff, cancel
	}
	eff, cancel := context.WithCancel(ctx)
	return eff, cancel
}

// runServiceHandler invokes one handler and reports one result.
func runServiceHandler(ctx context.Context, handler ServiceHandler, req []byte, resCh chan<- serviceResult) {
	reply, err := handler(ctx, req)
	if err != nil {
		resCh <- serviceResult{err: err}
		return
	}
	resCh <- serviceResult{reply: copyBytes(reply)}
}

// NewServiceServer registers one handler for a service name.
func (n *Node) NewServiceServer(service string, handler ServiceHandler) (*ServiceServer, error) {
	if n == nil {
		return nil, errors.New("sdk: new service server on nil node") //nolint:wrapcheck // new error, nothing to wrap
	}
	if service == "" {
		return nil, errors.New("sdk: service name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if handler == nil {
		return nil, fmt.Errorf("sdk: new service server on %q: handler is nil", service)
	}
	if n.IsClosed() {
		return nil, fmt.Errorf("sdk: new service server on %q: node closed: %w", service, ErrServiceServerGone)
	}
	srv := &ServiceServer{node: n, service: service, handler: handler}
	err := n.bus.registerService(service, srv, handler)
	if err != nil {
		return nil, fmt.Errorf("sdk: new service server on %q: %w", service, err)
	}
	n.trackServer(srv)
	if n.IsClosed() {
		_ = srv.Close() //nolint:errcheck // teardown is best effort, node closed error below carries the failure
		return nil, fmt.Errorf("sdk: new service server on %q: node closed: %w", service, ErrServiceServerGone)
	}
	if srv.isClosed() {
		return nil, fmt.Errorf("sdk: new service server on %q: node closed: %w", service, ErrServiceServerGone)
	}
	return srv, nil
}

// NewServiceClient creates a client for a service name.
func (n *Node) NewServiceClient(service string) (*ServiceClient, error) {
	if n == nil {
		return nil, errors.New("sdk: new service client on nil node") //nolint:wrapcheck // new error, nothing to wrap
	}
	if service == "" {
		return nil, errors.New("sdk: service name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if n.IsClosed() {
		return nil, fmt.Errorf("sdk: new service client on %q: node closed", service)
	}
	return &ServiceClient{node: n, service: service}, nil
}
