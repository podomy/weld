// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"context"
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/transports"
)

// Service legs for the MQTT backend.
//
// Services ride the rq and rr legs: a client publishes requests and
// takes replies, a server takes requests and publishes replies. The
// payload is opaque to this package; callers carry the service header
// plus GUID plus sequence exactly as on the DDS path (see
// internal/types/envelope.go MarshalServiceMessage). The take contract
// in transport.go only serves the data leg, so TakeService serves the
// request plus reply legs on this concrete type. Role selection mirrors
// the DDS backend ServiceRole names.

// ServiceRole selects the service direction for one call.
type ServiceRole int

const (
	// ServiceRoleServer takes requests and publishes replies.
	ServiceRoleServer ServiceRole = iota
	// ServiceRoleClient publishes requests and takes replies.
	ServiceRoleClient
)

// serviceSample is one decoded request or reply sample.
type serviceSample struct {
	// kind is KindRequest or KindReply.
	kind Kind
	// rosTopic is the fully qualified ROS service name.
	rosTopic string
	// typeName is the ROS IDL service base name.
	typeName string
	// payload holds the opaque service bytes.
	payload []byte
	// typeHash is the RIHS01 hash from user properties, empty if unset.
	typeHash string
}

// ServiceMessage is one taken service sample.
type ServiceMessage struct {
	// Service is the fully qualified ROS service name.
	Service string
	// TypeName is the ROS IDL service base name.
	TypeName string
	// Payload holds the opaque service bytes.
	Payload []byte
	// TypeHash is the RIHS01 hash, empty when unknown.
	TypeHash string
	// IsRequest is true for requests and false for replies.
	IsRequest bool
}

// serviceKindForPublish selects the publish leg for one role.
func serviceKindForPublish(role ServiceRole) (Kind, error) {
	switch role {
	case ServiceRoleClient:
		return KindRequest, nil
	case ServiceRoleServer:
		return KindReply, nil
	default:
		return "", fmt.Errorf("mqtt: unknown service role %d", int(role))
	}
}

// serviceKindForTake selects the take leg for one role.
func serviceKindForTake(role ServiceRole) (Kind, error) {
	switch role {
	case ServiceRoleServer:
		return KindRequest, nil
	case ServiceRoleClient:
		return KindReply, nil
	default:
		return "", fmt.Errorf("mqtt: unknown service role %d", int(role))
	}
}

// PublishService publishes one service request or reply sample.
//
// service is the ROS service name and typeName is the IDL base name.
// The client role publishes requests and the server role publishes
// replies. The payload is opaque service bytes. The caller type hash
// travels in user properties. Requires a prior JoinDiscovery; after
// Close it reports ErrBackendClosed.
func (b *Backend) PublishService(ctx context.Context, service, typeName string, role ServiceRole, payload []byte, typeHash string) error {
	snap, err := b.snapshotPublish(ctx, service, typeName, payload, typeHash, "publish service")
	if err != nil {
		return err
	}

	kind, err := serviceKindForPublish(role)
	if err != nil {
		return err
	}

	mqttTopic, err := EncodeTopic(snap.prefix, snap.domain, kind, service)
	if err != nil {
		return err
	}

	envelope := encodeEnvelope(typeName, payload)

	err = b.conn.Publish(ctx, mqttTopic, snap.qos, snap.retained, envelope, Properties{TypeHash: typeHash})
	if err != nil {
		if isClosedErr(err) {
			return fmt.Errorf("mqtt: publish service after close: %w", transports.ErrBackendClosed)
		}

		return fmt.Errorf("mqtt: publish service: %w", err)
	}

	return nil
}

// isClosedErr reports whether err wraps the closed sentinel.
func isClosedErr(err error) bool {
	return errors.Is(err, transports.ErrBackendClosed)
}

// TakeService takes one service sample for one role.
//
// The server role takes requests and the client role takes replies.
// Samples for the other leg are buffered for their take path up to
// maxLegPending each with oldest-evicted counting, so a consumer that
// only ever takes one leg cannot grow memory without bound; evictions
// are counted, never silent. Blocks until a matching sample arrives;
// ctx cancellation reports ctx.Err, Close reports ErrBackendClosed.
func (b *Backend) TakeService(ctx context.Context, role ServiceRole) (ServiceMessage, error) {
	var empty ServiceMessage

	err := ctx.Err()
	if err != nil {
		return empty, fmt.Errorf("mqtt: take service context: %w", err)
	}

	b.takeMu.Lock()
	defer b.takeMu.Unlock()

	prefix, domain, err := b.takeSnapshot(ctx, "take service")
	if err != nil {
		return empty, err
	}

	want, err := serviceKindForTake(role)
	if err != nil {
		return empty, err
	}

	if msg, ok := popServiceSample(b.svcPending, want); ok {
		b.svcPending = removeServiceSample(b.svcPending, want)

		b.mu.Lock()
		closedAgain := b.closed
		b.mu.Unlock()

		if closedAgain {
			b.svcPending = append([]serviceSample{{
				kind:     want,
				rosTopic: msg.Service,
				typeName: msg.TypeName,
				payload:  msg.Payload,
				typeHash: msg.TypeHash,
			}}, b.svcPending...)

			return empty, fmt.Errorf("mqtt: take service after close: %w", transports.ErrBackendClosed)
		}

		return msg, nil
	}

	for {
		svc, kind, err := b.takeServiceWire(ctx, prefix, domain)
		if err != nil {
			return empty, err
		}

		if kind == KindData {
			b.stageLegSample(incomingFromService(svc, kind))

			continue
		}

		if kind == want {
			return ServiceMessage{
				Service:   svc.rosTopic,
				TypeName:  svc.typeName,
				Payload:   svc.payload,
				TypeHash:  svc.typeHash,
				IsRequest: kind == KindRequest,
			}, nil
		}

		b.stageSvcSample(svc)
	}
}

// stageLegSample buffers one cross-leg sample, evicting the oldest
// buffered sample and counting one drop when the bound is reached.
// The caller holds takeMu; the drop counter takes mu, matching the
// takeMu-then-mu order used everywhere takes touch shared state.
func (b *Backend) stageLegSample(sample transports.IncomingMessage) {
	if len(b.dataPending) >= maxLegPending {
		copy(b.dataPending, b.dataPending[1:])
		b.dataPending[len(b.dataPending)-1] = sample
		b.mu.Lock()
		b.dropped++
		b.mu.Unlock()

		return
	}

	b.dataPending = append(b.dataPending, sample)
}

// stageSvcSample buffers one cross-leg service sample with the same
// evict-oldest plus counted-drop policy as stageLegSample.
func (b *Backend) stageSvcSample(sample serviceSample) {
	if len(b.svcPending) >= maxLegPending {
		copy(b.svcPending, b.svcPending[1:])
		b.svcPending[len(b.svcPending)-1] = sample
		b.mu.Lock()
		b.dropped++
		b.mu.Unlock()

		return
	}

	b.svcPending = append(b.svcPending, sample)
}

// takeServiceWire reads one inbox sample and decodes its kind.
//
// Discovery samples never reach here: the pump routes them to the
// endpoint cache before they enter the inbox. A decode failure counts
// one drop like the TakeBytes path and returns the error.
func (b *Backend) takeServiceWire(ctx context.Context, prefix string, domain uint32) (serviceSample, Kind, error) {
	var empty serviceSample

	var wire ConnMessage

	select {
	case <-ctx.Done():
		err := ctx.Err()

		return empty, "", fmt.Errorf("mqtt: take service context: %w", err)
	case <-b.done:
		return empty, "", fmt.Errorf("mqtt: take service after close: %w", transports.ErrBackendClosed)
	case wire = <-b.inbox:
	}

	kind, svc, err := b.decodeWireMessage(prefix, domain, wire)
	if err != nil {
		b.mu.Lock()
		b.dropped++
		b.mu.Unlock()

		return empty, "", fmt.Errorf("mqtt: take service decode: %w", err)
	}

	return svc, kind, nil
}

// popServiceSample returns the first buffered sample of one kind.
func popServiceSample(pending []serviceSample, want Kind) (ServiceMessage, bool) {
	for _, svc := range pending {
		if svc.kind == want {
			return ServiceMessage{
				Service:   svc.rosTopic,
				TypeName:  svc.typeName,
				Payload:   svc.payload,
				TypeHash:  svc.typeHash,
				IsRequest: svc.kind == KindRequest,
			}, true
		}
	}

	return ServiceMessage{}, false
}

// removeServiceSample drops the first buffered sample of one kind.
func removeServiceSample(pending []serviceSample, want Kind) []serviceSample {
	for i, svc := range pending {
		if svc.kind == want {
			return append(pending[:i], pending[i+1:]...)
		}
	}

	return pending
}
