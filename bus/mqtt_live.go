// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus

import (
	"context"
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/transports/mqtt"
)

// DialMQTT dials a live MQTT broker and wraps the session in a backend.
//
// The dial uses mqtt.Dial, whose return type is the unexported netConn handle.
// The handle is kept opaque by type inference and passed to NewWithConn as a
// Conn, so no unexported type leaks through this public signature. netConn
// implements Conn with Publish, Receive, and Close plus the subscriber
// interface used by Backend JoinDiscovery for live sessions, so the returned
// backend supports the same join, publish, take, and leave paths as any MQTT
// backend. The backend is not joined; callers must JoinDiscovery before
// publish or take, matching NewWithConn. On NewWithConn failure the dialed
// session is closed before returning.
func DialMQTT(ctx context.Context, cfg MQTTConfig) (Backend, error) {
	conn, err := mqtt.Dial(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("bus: dial mqtt: %w", err)
	}

	backend, err := NewWithConn(cfg, conn)
	if err != nil {
		closeErr := conn.Close()
		if closeErr != nil {
			joined := errors.Join(err, closeErr)
			return nil, fmt.Errorf("bus: new mqtt backend with live conn: %w", joined)
		}

		return nil, fmt.Errorf("bus: new mqtt backend with live conn: %w", err)
	}

	return backend, nil
}
