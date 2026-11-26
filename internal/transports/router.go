// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package transports

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
)

// Route maps a topic prefix to a named backend.
type Route struct {
	// Prefix is the topic prefix matched with strings.HasPrefix.
	// Empty matches all topics. On equal-length matches the first
	// route in RouterConfig.Routes wins.
	Prefix string
	// BackendName names the backend receiving matching topics.
	BackendName string
}

// RouterConfig selects a backend per topic by longest prefix match.
type RouterConfig struct {
	// Default names the backend used when no route matches.
	Default string
	// Routes holds prefix to backend mappings.
	Routes []Route
}

// Validate checks structural config without backend existence.
func (c RouterConfig) Validate() error {
	if c.Default == "" {
		return errors.New("transports: router default backend name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	for i, route := range c.Routes {
		if route.BackendName == "" {
			return fmt.Errorf("transports: router route %d has empty backend name", i)
		}
	}

	return nil
}

// Router routes requests across named backends by topic prefix.
//
// TakeBytes races all backends concurrently and returns the first
// available message. Extra messages that win the race together are
// buffered in order for later takes so no sample is dropped.
// Use NewRouter to build one; the zero value is not usable.
type Router struct {
	mu       sync.Mutex
	takeMu   sync.Mutex
	backends map[string]Backend
	order    []string
	cfg      RouterConfig
	// pending buffers simultaneous race winners ahead of later takes.
	// It carries no cap or drop counter by design: every entry is a
	// consumed sample that NoDrops (proof/router.tla) requires be
	// delivered, and takes drain it first. Growth is bounded in
	// practice by simultaneous-win rate: each race adds at most
	// len(order)-1 while each take removes one.
	pending []IncomingMessage
	closed  bool
	joined  bool
}

// routerTakeResult carries one backend take outcome.
type routerTakeResult struct {
	msg  IncomingMessage
	err  error
	name string
}

// _ asserts Router implements Backend.
var _ Backend = (*Router)(nil)

// checkReady reports whether the router was built by NewRouter.
//
// The zero value is not usable: NewRouter wires the backend map,
// order, and config. Failing loudly here beats silently joining
// nothing and marking a zero router joined.
func (r *Router) checkReady() error {
	if r == nil || r.backends == nil {
		return errors.New("transports: router not initialized, use NewRouter") //nolint:wrapcheck // new error, nothing to wrap
	}

	return nil
}

// NewRouter builds a Router over explicit backends and config.
func NewRouter(backends map[string]Backend, cfg RouterConfig) (*Router, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("transports: router invalid config: %w", err)
	}

	if len(backends) == 0 {
		return nil, errors.New("transports: router needs at least one backend") //nolint:wrapcheck // new error, nothing to wrap
	}

	if _, ok := backends[cfg.Default]; !ok {
		return nil, fmt.Errorf("transports: router unknown default backend %q", cfg.Default)
	}

	for i, route := range cfg.Routes {
		if _, ok := backends[route.BackendName]; !ok {
			return nil, fmt.Errorf("transports: router route %d unknown backend %q", i, route.BackendName)
		}
	}

	for name, backend := range backends {
		if backend == nil {
			return nil, fmt.Errorf("transports: router backend %q is nil", name)
		}
	}

	copied := make(map[string]Backend, len(backends))
	maps.Copy(copied, backends)

	routes := make([]Route, len(cfg.Routes))
	copy(routes, cfg.Routes)

	order := []string{cfg.Default}
	seen := map[string]bool{cfg.Default: true}

	for _, route := range routes {
		if !seen[route.BackendName] {
			seen[route.BackendName] = true
			order = append(order, route.BackendName)
		}
	}

	var extra []string

	for name := range copied {
		if !seen[name] {
			extra = append(extra, name)
		}
	}

	sort.Strings(extra)
	order = append(order, extra...)

	return &Router{
		backends: copied,
		order:    order,
		cfg:      RouterConfig{Default: cfg.Default, Routes: routes},
	}, nil
}

// selectBackendName picks the backend name for a topic by longest prefix.
func selectBackendName(cfg RouterConfig, topic string) string {
	name := cfg.Default
	best := -1

	for _, route := range cfg.Routes {
		if strings.HasPrefix(topic, route.Prefix) {
			if len(route.Prefix) > best {
				best = len(route.Prefix)
				name = route.BackendName
			}
		}
	}

	return name
}

// PublishBytes publishes one message via the longest prefix match.
func (r *Router) PublishBytes(ctx context.Context, msg OutgoingMessage) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("transports: router publish context done: %w", err)
	}

	if err := r.checkReady(); err != nil {
		return err
	}

	r.mu.Lock()
	closed, joined := r.closed, r.joined
	cfg := r.cfg
	backends := r.backends
	r.mu.Unlock()

	if closed {
		return ErrBackendClosed
	}

	if !joined {
		return ErrNotJoined
	}

	name := selectBackendName(cfg, msg.TopicName)

	target, ok := backends[name]
	if !ok {
		return fmt.Errorf("transports: router missing backend %q", name)
	}

	err = target.PublishBytes(ctx, msg)
	if err != nil {
		return fmt.Errorf("transports: router publish via %q: %w", name, err)
	}

	return nil
}

// deliverDrained returns one buffered message unless a Close landed
// after the pre-check, in which case the message goes back to the
// head of the queue to preserve order and the take reports the close.
func (r *Router) deliverDrained(msg IncomingMessage) (IncomingMessage, error) {
	r.mu.Lock()
	closedAgain := r.closed
	r.mu.Unlock()

	if closedAgain {
		r.mu.Lock()
		r.pending = append([]IncomingMessage{msg}, r.pending...)
		r.mu.Unlock()

		return IncomingMessage{}, ErrBackendClosed
	}

	return msg, nil
}

// drainPending pops one buffered message when present.
func (r *Router) drainPending() (IncomingMessage, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.pending) == 0 {
		return IncomingMessage{}, false
	}

	msg := r.pending[0]
	r.pending = append(r.pending[:0], r.pending[1:]...)

	return msg, true
}

// bufferPending stores extra race winners for later takes.
func (r *Router) bufferPending(msgs []IncomingMessage) {
	if len(msgs) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.pending = append(r.pending, msgs...)
}

// snapshotTakes copies the take order while holding the lock.
//
// The returned backends map is shared, not copied: it is only ever
// populated by NewRouter and never mutated after, so concurrent takes
// read it safely under no lock. Callers must not mutate it.
func (r *Router) snapshotTakes() ([]string, map[string]Backend, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, nil, ErrBackendClosed
	}

	order := append([]string(nil), r.order...)

	return order, r.backends, nil
}

// raceTakes runs one TakeBytes per backend and returns first success.
//
// The caller context bounds the whole race. The first success wins
// immediately without waiting for quiet backends; later simultaneous
// successes drain in the background into pending so no sample is
// dropped. On context end every outstanding outcome is collected and
// successes are buffered before the error returns, so a popped sample
// is never stranded.
func raceTakes(ctx context.Context, r *Router, order []string, backends map[string]Backend) (IncomingMessage, error) {
	child, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan routerTakeResult, len(order))

	for _, name := range order {
		go func(target string) {
			msg, err := backends[target].TakeBytes(child)
			if err != nil {
				err = fmt.Errorf("transports: router take via %q: %w", target, err)
			}

			results <- routerTakeResult{msg: msg, err: err, name: target}
		}(name)
	}

	remaining := len(order)
	var lastErr error

	for remaining > 0 {
		select {
		case <-ctx.Done():
			return IncomingMessage{}, r.abortRace(ctx, results, remaining)
		case res := <-results:
			remaining--

			if res.err != nil {
				if lastErr == nil {
					lastErr = res.err
				}

				continue
			}

			go r.drainRaceResults(results, remaining)

			return res.msg, nil
		}
	}

	if lastErr != nil {
		return IncomingMessage{}, lastErr
	}

	return IncomingMessage{}, errors.New("transports: router has no backends to take from") //nolint:wrapcheck // new error, nothing to wrap
}

// abortRace harvests arrived outcomes, parks stragglers in the
// background, and reports the caller's context error. Successes are
// buffered for later takes so a popped sample is never stranded; the
// caller still gets its context error promptly without waiting for
// quiet backends. A backend error that arrived first does not mask a
// cancellation: the contract promises ctx.Err() on cancellation.
func (r *Router) abortRace(ctx context.Context, results <-chan routerTakeResult, remaining int) error {
	err := ctx.Err()

	for {
		select {
		case res := <-results:
			remaining--

			if res.err == nil {
				r.bufferPending([]IncomingMessage{res.msg})
			}
		default:
			go r.drainRaceResults(results, remaining)

			return fmt.Errorf("transports: router take context done: %w", err)
		}
	}
}

// drainRaceResults buffers late simultaneous successes after the
// winner returns. Errors are ignored: the winning take already
// succeeded. It terminates after count receives; every sender
// terminates because the race context is cancelled on return.
func (r *Router) drainRaceResults(results <-chan routerTakeResult, count int) {
	for range count {
		if res := <-results; res.err == nil {
			r.bufferPending([]IncomingMessage{res.msg})
		}
	}
}

// TakeBytes races backends concurrently and returns the first message.
//
// Takes serialize on takeMu; concurrent callers queue behind the
// in-flight race. The first winning backend returns immediately;
// late simultaneous successes buffer for later takes. Cancellation
// reports ctx.Err and buffers arrived successes instead of dropping
// them; a closed router reports ErrBackendClosed even with buffered
// samples waiting.
func (r *Router) TakeBytes(ctx context.Context) (IncomingMessage, error) {
	r.takeMu.Lock()
	defer r.takeMu.Unlock()

	err := ctx.Err()
	if err != nil {
		return IncomingMessage{}, fmt.Errorf("transports: router take context done: %w", err)
	}

	if err := r.checkReady(); err != nil {
		return IncomingMessage{}, err
	}

	r.mu.Lock()
	closed, joined := r.closed, r.joined
	r.mu.Unlock()

	if closed {
		return IncomingMessage{}, ErrBackendClosed
	}

	if !joined {
		return IncomingMessage{}, ErrNotJoined
	}

	if msg, ok := r.drainPending(); ok {
		return r.deliverDrained(msg)
	}

	order, backends, err := r.snapshotTakes()
	if err != nil {
		return IncomingMessage{}, err
	}

	return raceTakes(ctx, r, order, backends)
}

// fanOut runs one operation on every backend in order.
//
// It reports the first backend error wrapped with the operation name,
// or nil when every backend accepts. Context cancellation and a closed
// router fail before any backend runs.
func (r *Router) fanOut(ctx context.Context, op string, call func(Backend) error) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("transports: router %s context done: %w", op, err)
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrBackendClosed
	}

	order := append([]string(nil), r.order...)
	backends := r.backends
	r.mu.Unlock()

	var firstErr error

	for _, name := range order {
		err := call(backends[name])
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("transports: router %s backend %q: %w", op, name, err)
		}
	}

	return firstErr
}

// JoinDiscovery joins all backends and reports the first error.
//
// The router counts as joined only when every backend accepts;
// a partial join leaves it unjoined so later takes fail loudly
// instead of running on a subset.
func (r *Router) JoinDiscovery(ctx context.Context, info DiscoveryInfo) error {
	if err := r.checkReady(); err != nil {
		return err
	}

	if err := r.fanOut(ctx, "join", func(backend Backend) error {
		return backend.JoinDiscovery(ctx, info)
	}); err != nil {
		return err
	}

	// Re-check under the lock: a Close that landed mid-join must win
	// over marking joined, mirroring Backend.JoinDiscovery's second
	// gate. Takes check closed first so a closed+joined router still
	// reports ErrBackendClosed everywhere.
	return r.markJoined()
}

// markJoined marks the router joined unless a concurrent Close won.
func (r *Router) markJoined() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return ErrBackendClosed
	}

	r.joined = true

	return nil
}

// ReportLiveliness fans out one report to all backends.
func (r *Router) ReportLiveliness(ctx context.Context, report LivelinessReport) error {
	if err := r.checkReady(); err != nil {
		return err
	}

	r.mu.Lock()
	closed, joined := r.closed, r.joined
	r.mu.Unlock()

	if closed {
		return ErrBackendClosed
	}

	if !joined {
		return ErrNotJoined
	}

	return r.fanOut(ctx, "report", func(backend Backend) error {
		return backend.ReportLiveliness(ctx, report)
	})
}

// Close closes all backends once and stays idempotent.
func (r *Router) Close() error {
	if err := r.checkReady(); err != nil {
		return err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}

	r.closed = true
	order := append([]string(nil), r.order...)
	backends := r.backends
	r.mu.Unlock()

	var firstErr error

	for _, name := range order {
		err := backends[name].Close()
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("transports: router close backend %q: %w", name, err)
			}
		}
	}

	if firstErr != nil {
		return firstErr
	}

	return nil
}
