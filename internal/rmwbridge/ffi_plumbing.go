// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge

import (
	"errors"
	"fmt"
)

// ErrServiceBusy reports a non-blocking service send with a full queue.
var ErrServiceBusy = errors.New("rmwbridge: service busy")

// PendingRequests returns the number of requests queued for Serve.
//
// It reports the in-memory rendezvous depth only. Callers check handle
// liveness separately. It never blocks.
func (s *Service) PendingRequests() int {
	return len(s.requests)
}

// PendingMessages returns the number of samples queued for Take.
//
// It never blocks.
func (s *Subscription) PendingMessages() int {
	return len(s.ch)
}

// SetTypeName records the IDL type name on a service.
//
// It mirrors CreateServiceWithType for handles created without a type,
// so late type reports still reach graph queries. It never blocks.
func (s *Service) SetTypeName(name string) {
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	s.TypeName = name
}

// SetTypeName records the IDL type name on a subscription.
//
// It mirrors CreateSubscriptionWithType for handles created without a
// type. It never blocks.
func (s *Subscription) SetTypeName(name string) {
	s.bridge.mu.Lock()
	defer s.bridge.mu.Unlock()
	s.TypeName = name
}

// TrySendRequest enqueues one request without blocking and returns its reply waiter.
//
// The payload is copied, so the caller may reuse its buffer on return.
// The waiter receives exactly one reply through the normal Serve plus
// respond path; a second respond reports ErrDuplicateResponse to the
// responder instead. A full queue reports ErrServiceBusy without
// consuming the payload.
func (s *Service) TrySendRequest(payload []byte) (chan []byte, error) {
	if len(payload) > maxPayloadBytes {
		return nil, fmt.Errorf("rmwbridge: service try send: payload %d bytes exceeds %d bytes: %w", len(payload), maxPayloadBytes, ErrPayloadTooLarge)
	}
	s.bridge.mu.Lock()
	bridgeClosed := s.bridge.closed
	s.bridge.mu.Unlock()
	if bridgeClosed {
		return nil, fmt.Errorf("rmwbridge: service try send: %w", ErrBridgeClosed)
	}
	s.mu.Lock()
	svcClosed := s.closed
	s.mu.Unlock()
	if svcClosed {
		return nil, fmt.Errorf("rmwbridge: service try send: %w", ErrServiceClosed)
	}
	data := append([]byte(nil), payload...)
	reply := make(chan []byte, 1)
	select {
	case s.requests <- ServiceRequest{Payload: data, reply: reply}:
		return reply, nil
	default:
		return nil, fmt.Errorf("rmwbridge: service try send: %w", ErrServiceBusy)
	}
}
