// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"fmt"
	"sync"
)

// Bus is the in-memory hub carrying topics and services.
//
// REVIEW: the bus stands in for DDS, Zenoh, and MQTT transports so unit
// and fleet tests run without live ROS. Matching, routing, and timeouts
// live in this package; transports would only move these bytes later.
type Bus struct {
	// mu guards topics and services.
	mu sync.Mutex
	// topics maps a topic name to its live subscribers.
	topics map[string]map[*Subscription]struct{}
	// services maps a service name to its registered server.
	services map[string]*serviceRecord
}

// serviceRecord pairs one registered server with its handler.
type serviceRecord struct {
	// owner identifies the server for unregister.
	owner *ServiceServer
	// handler serves calls outside the bus lock.
	handler ServiceHandler
}

// NewBus returns an empty hub. Every fleet shares one bus explicitly.
func NewBus() *Bus {
	return &Bus{
		topics:   make(map[string]map[*Subscription]struct{}),
		services: make(map[string]*serviceRecord),
	}
}

// subscribe records one live subscriber on a topic.
func (b *Bus) subscribe(topic string, sub *Subscription) {
	if b == nil || sub == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	set, ok := b.topics[topic]
	if !ok {
		set = make(map[*Subscription]struct{})
		b.topics[topic] = set
	}
	set[sub] = struct{}{}
}

// unsubscribe removes one subscriber from a topic.
func (b *Bus) unsubscribe(topic string, sub *Subscription) {
	if b == nil || sub == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	set, ok := b.topics[topic]
	if !ok {
		return
	}
	delete(set, sub)
	if len(set) == 0 {
		delete(b.topics, topic)
	}
}

// publish delivers one ingress-copied sample to every subscriber of topic.
func (b *Bus) publish(topic string, payload []byte) {
	if b == nil {
		return
	}
	b.mu.Lock()
	set, ok := b.topics[topic]
	subs := make([]*Subscription, 0, len(set))
	if ok {
		for sub := range set {
			subs = append(subs, sub)
		}
	}
	b.mu.Unlock()
	for _, sub := range subs {
		sub.deliver(payload)
	}
}

// registerService records one handler for a service name.
func (b *Bus) registerService(service string, owner *ServiceServer, handler ServiceHandler) error {
	if b == nil {
		return fmt.Errorf("sdk: register service %q: nil bus", service)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, dup := b.services[service]; dup {
		return fmt.Errorf("sdk: register service %q: already registered", service)
	}
	b.services[service] = &serviceRecord{owner: owner, handler: handler}
	return nil
}

// unregisterService removes a service only when owner matches.
func (b *Bus) unregisterService(service string, owner *ServiceServer) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	rec, ok := b.services[service]
	if !ok {
		return
	}
	if rec.owner != owner {
		return
	}
	delete(b.services, service)
}

// lookupService returns the handler for a service name.
func (b *Bus) lookupService(service string) (ServiceHandler, bool) {
	if b == nil {
		return nil, false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	rec, ok := b.services[service]
	if !ok {
		return nil, false
	}
	return rec.handler, true
}
