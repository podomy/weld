// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// Listener and callback dispatch, DDS 1.4 section 2.2.4.
//
// Listeners fan taken samples out to Go callbacks without touching the
// take path: recording calls Notify after history append, and the legacy
// path delivers exactly as before whether or not a listener is
// registered. ListenerPoller drives a poll function in the background and
// notifies per topic, which is the Go-side counterpart to the rmw
// on-new-message callbacks.

// pollerBackoff bounds the poll retry delay for persistent errors.
//
// The base delay doubles per consecutive failure and caps at the max so a
// persistently failing poll cannot hot-spin the loop.
const (
	pollerBaseBackoff = 5 * time.Millisecond
	pollerMaxBackoff  = 200 * time.Millisecond
)

// pollerRetryDelay returns the backoff for attempt starting at 1.
func pollerRetryDelay(attempt int) time.Duration {
	delay := pollerBaseBackoff
	for i := 1; i < attempt && delay < pollerMaxBackoff; i++ {
		delay *= 2
	}

	if delay > pollerMaxBackoff {
		return pollerMaxBackoff
	}

	return delay
}

// DataCallback receives one taken sample for its topic.
type DataCallback func(SampledMessage)

// LivelinessCallback receives one writer aliveness edge.
type LivelinessCallback func(handle [16]byte, alive bool)

// DeadlineCallback receives one deadline miss for a topic.
type DeadlineCallback func(topic string)

// DataListener groups the callbacks for one subscription.
type DataListener struct {
	// OnData fires for each taken sample, nil for none.
	OnData DataCallback
	// OnLiveliness fires for each aliveness edge, nil for none.
	OnLiveliness LivelinessCallback
	// OnDeadline fires for each deadline miss, nil for none.
	OnDeadline DeadlineCallback
}

// listenerRegistry fans samples out to per-topic listeners.
type listenerRegistry struct {
	mu      sync.RWMutex
	byTopic map[string][]*DataListener
}

// newListenerRegistry returns an empty registry.
func newListenerRegistry() *listenerRegistry {
	return &listenerRegistry{byTopic: make(map[string][]*DataListener)}
}

// AddListener registers one listener for a topic and returns its removal.
//
// Removal is idempotent: calling it twice removes once and never panics.
func (r *DataReceiver) AddListener(topic string, listener *DataListener) (remove func()) {
	if listener == nil {
		return func() {}
	}

	reg := r.readerStateFor().listeners
	reg.mu.Lock()
	reg.byTopic[topic] = append(reg.byTopic[topic], listener)
	reg.mu.Unlock()

	var once sync.Once

	return func() {
		once.Do(func() {
			reg.mu.Lock()
			defer reg.mu.Unlock()

			kept := reg.byTopic[topic][:0]
			for _, cand := range reg.byTopic[topic] {
				if cand != listener {
					kept = append(kept, cand)
				}
			}

			if len(kept) == 0 {
				delete(reg.byTopic, topic)

				return
			}

			reg.byTopic[topic] = kept
		})
	}
}

// ListenerCount returns the listener count for one topic.
func (r *DataReceiver) ListenerCount(topic string) int {
	reg := r.readerStateFor().listeners
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	return len(reg.byTopic[topic])
}

// NotifyData fans one sample out to the topic listeners.
//
// Listener panics never propagate: a failing listener is skipped after a
// loud log so one bad callback cannot break delivery. Nil listeners
// never fire.
func (r *DataReceiver) NotifyData(topic string, sample SampledMessage) {
	reg := r.readerStateFor().listeners
	reg.mu.RLock()
	live := append([]*DataListener(nil), reg.byTopic[topic]...)
	reg.mu.RUnlock()

	for _, listener := range live {
		if listener == nil || listener.OnData == nil {
			continue
		}

		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					log.Printf("dds: listener OnData panic isolated: %v", recovered)
				}
			}()

			listener.OnData(sample)
		}()
	}
}

// NotifyLiveliness fans one aliveness edge out to the topic listeners.
func (r *DataReceiver) NotifyLiveliness(topic string, handle [16]byte, alive bool) {
	reg := r.readerStateFor().listeners
	reg.mu.RLock()
	live := append([]*DataListener(nil), reg.byTopic[topic]...)
	reg.mu.RUnlock()

	for _, listener := range live {
		if listener == nil || listener.OnLiveliness == nil {
			continue
		}

		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					log.Printf("dds: listener OnLiveliness panic isolated: %v", recovered)
				}
			}()

			listener.OnLiveliness(handle, alive)
		}()
	}
}

// NotifyDeadline fans one deadline miss out to the topic listeners.
func (r *DataReceiver) NotifyDeadline(topic string) {
	reg := r.readerStateFor().listeners
	reg.mu.RLock()
	live := append([]*DataListener(nil), reg.byTopic[topic]...)
	reg.mu.RUnlock()

	for _, listener := range live {
		if listener == nil || listener.OnDeadline == nil {
			continue
		}

		func() {
			defer func() {
				if recovered := recover(); recovered != nil {
					log.Printf("dds: listener OnDeadline panic isolated: %v", recovered)
				}
			}()

			listener.OnDeadline(topic)
		}()
	}
}

// ListenerPoller invokes topic listeners from a background poll loop.
//
// The poll function returns one sample per call and blocks until ctx
// ends; the poller notifies the registry and continues. Errors from poll
// count and retry on the next iteration except for context cancellation,
// which stops the loop. Start is idempotent: a running poller ignores a
// second start. Stats self-lock and are safe for concurrent use.
type ListenerPoller struct {
	mu        sync.Mutex
	running   bool
	cancel    context.CancelFunc
	done      chan struct{}
	delivered uint64
	errors    uint64
}

// PollStats is one snapshot of poller counters.
type PollStats struct {
	// Delivered counts samples fanned out to listeners.
	Delivered uint64
	// Errors counts poll failures excluding cancellation.
	Errors uint64
	// Running is true while the loop runs.
	Running bool
}

// Start begins the background poll loop for one topic.
//
// Poll must block until a sample arrives or ctx ends and must return
// ctx.Err on cancellation. The receiver must already exist; samples
// notify under topic.
func (p *ListenerPoller) Start(ctx context.Context, receiver *DataReceiver, topic string, poll func(context.Context) (SampledMessage, error)) error {
	if receiver == nil {
		return fmt.Errorf("dds: listener poller without receiver: %w", ErrInvalidFrame)
	}

	if poll == nil {
		return fmt.Errorf("dds: listener poller without poll: %w", ErrInvalidFrame)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return nil
	}

	loopCtx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	p.done = make(chan struct{})
	p.running = true

	go p.loop(loopCtx, receiver, topic, poll)

	return nil
}

// loop polls until the context ends, then marks the poller stopped.
//
// Persistent poll errors back off with a capped exponential delay so a
// failing poll cannot hot-spin. The delay select also watches ctx so Stop
// still ends promptly.
func (p *ListenerPoller) loop(ctx context.Context, receiver *DataReceiver, topic string, poll func(context.Context) (SampledMessage, error)) {
	defer close(p.done)

	consecutive := 0

	for {
		sample, err := poll(ctx)
		if err != nil {
			p.mu.Lock()
			stopped := ctx.Err() != nil
			if !stopped {
				p.errors++
			}
			p.mu.Unlock()

			if stopped {
				p.mu.Lock()
				p.running = false
				p.mu.Unlock()

				return
			}

			consecutive++

			delay := pollerRetryDelay(consecutive)

			select {
			case <-ctx.Done():
				p.mu.Lock()
				p.running = false
				p.mu.Unlock()

				return
			case <-time.After(delay):
			}

			continue
		}

		consecutive = 0

		p.mu.Lock()
		p.delivered++
		p.mu.Unlock()

		receiver.NotifyData(topic, sample)
	}
}

// Stop ends the background poll loop and waits for it.
func (p *ListenerPoller) Stop() {
	p.mu.Lock()
	cancel := p.cancel
	done := p.done
	p.mu.Unlock()

	if cancel == nil {
		return
	}

	cancel()

	<-done
}

// Stats snapshots the poller counters.
func (p *ListenerPoller) Stats() PollStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return PollStats{Delivered: p.delivered, Errors: p.errors, Running: p.running}
}
