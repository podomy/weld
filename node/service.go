// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"fmt"

	"github.com/podomy/weld/internal/core"
)

// ServiceClient tracks outbound service calls by GUID plus sequence. The
// tracker is transport free: transports carry bytes, core carries keys. Do
// not copy a client after first use; build with NewServiceClient.
type ServiceClient = core.ServiceClient

// ServiceServer tracks inbound service requests with atomic take plus
// dispatch. Do not copy a server after first use; build with NewServiceServer.
type ServiceServer = core.ServiceServer

// ServiceQoS is one input QoS fanned out to request and response entities.
// Clients use the request leg as publisher and the response leg as
// subscription; servers use the reverse.
type ServiceQoS = core.ServiceQoS

// ServiceCallKey is the exact one to one routing key for one call: service
// name plus 8 byte wire client GUID plus sequence.
type ServiceCallKey = core.ServiceCallKey

// ServiceRequest is one server side request awaiting dispatch.
type ServiceRequest = core.ServiceRequest

// ServiceTrace is one service introspection event carrying the service name
// plus the routing key so operators can pair request and response legs.
type ServiceTrace = core.ServiceTrace

// ServiceTraceHook observes service transitions for introspection. Hooks run
// outside the tracker lock and must not call back into the tracker.
type ServiceTraceHook = core.ServiceTraceHook

// ServiceTraceKind names one service introspection event.
type ServiceTraceKind = core.ServiceTraceKind

const (
	// ServiceTraceRequestSent fires on clients when a request starts.
	ServiceTraceRequestSent = core.ServiceTraceRequestSent
	// ServiceTraceResponseReceived fires on clients when a reply arrives.
	ServiceTraceResponseReceived = core.ServiceTraceResponseReceived
	// ServiceTraceRequestReceived fires on servers when a request arrives.
	ServiceTraceRequestReceived = core.ServiceTraceRequestReceived
	// ServiceTraceResponseSent fires on servers when a reply is sent.
	ServiceTraceResponseSent = core.ServiceTraceResponseSent
)

var (
	// ErrServiceTimeout reports a per call timeout with service name context.
	ErrServiceTimeout = core.ErrServiceTimeout
	// ErrServiceServerGone reports a gone server for one pending call.
	ErrServiceServerGone = core.ErrServiceServerGone
	// ErrServiceClientGone reports a gone client for one pending request.
	ErrServiceClientGone = core.ErrServiceClientGone
	// ErrServiceDuplicate reports a duplicate sequence for one client.
	ErrServiceDuplicate = core.ErrServiceDuplicate
	// ErrServiceNoPending reports a reply with no matching pending call.
	ErrServiceNoPending = core.ErrServiceNoPending
	// ErrServiceNoRequest reports a server reply with no matching request.
	ErrServiceNoRequest = core.ErrServiceNoRequest
)

// ResolveServiceQoS fans one requested profile out to both service legs.
// When any best available marker is set, only the marker fields take the
// services default and explicit fields survive. A transient local result
// returns a durability warning because services do not use durability.
func ResolveServiceQoS(service string, requested QoSProfile) (ServiceQoS, *MismatchEvent, error) {
	resolved, event, err := core.ResolveServiceQoS(service, requested)
	if err != nil {
		return ServiceQoS{}, nil, fmt.Errorf("node: resolve service qos: %w", err)
	}
	return resolved, event, nil
}

// DefaultServiceTimeout returns the per call service timeout of five seconds.
// Callers use it when the request carries no explicit timeout.
func DefaultServiceTimeout() Duration {
	return core.DefaultServiceTimeout()
}

// NewServiceClient returns an empty client tracker. The now function supplies
// nanoseconds for timeout math; tests inject a fake clock, production passes
// nil for wall time. When enabled is true and hook is non nil, transitions
// emit introspection events.
func NewServiceClient(now func() int64, enabled bool, hook ServiceTraceHook) *ServiceClient {
	return core.NewServiceClient(now, enabled, hook)
}

// NewServiceServer returns an empty server tracker. The now function supplies
// nanoseconds for timeout math; tests inject a fake clock, production passes
// nil for wall time. When enabled is true and hook is non nil, transitions
// emit introspection events.
func NewServiceServer(now func() int64, enabled bool, hook ServiceTraceHook) *ServiceServer {
	return core.NewServiceServer(now, enabled, hook)
}
