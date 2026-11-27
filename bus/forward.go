// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus

import (
	"context"
	"fmt"

	"github.com/podomy/weld/internal/transports"
)

// ForwardOnce takes one message from src and publishes it to dst.
//
// The ROS topic, type name, and payload carry over unchanged. Request legs
// stay requests and replies stay replies, so services bridge without a
// second path. The returned message is the sample that was forwarded.
// Context cancellation aborts either leg and reports ctx.Err.
func ForwardOnce(ctx context.Context, src Backend, dst Backend) (IncomingMessage, error) {
	msg, err := transports.ForwardOnce(ctx, src, dst)
	if err != nil {
		return IncomingMessage{}, fmt.Errorf("bus: forward once: %w", err)
	}

	return msg, nil
}

// ForwardLoop relays every message from src to dst until ctx ends.
//
// It calls ForwardOnce in a loop. A nil backend fails fast on the first
// iteration instead of hanging the loop. Context cancellation ends the
// loop and reports ctx.Err. Any other take or publish error ends the loop
// with a wrapped error naming the failed leg.
func ForwardLoop(ctx context.Context, src Backend, dst Backend) error {
	err := transports.ForwardLoop(ctx, src, dst)
	if err != nil {
		return fmt.Errorf("bus: forward loop: %w", err)
	}

	return nil
}
