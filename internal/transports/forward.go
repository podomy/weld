// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package transports

import (
	"context"
	"errors"
	"fmt"
)

// ForwardOnce takes one message from src and publishes it to dst.
//
// The ROS topic, type name, and payload carry over unchanged. Request legs
// stay requests and replies stay replies, so services bridge without a
// second path. Context cancellation aborts either leg and reports ctx.Err.
//
// Takes are destructive: when the take succeeds but the publish fails,
// the sample is consumed and lost. Callers needing at-least-once
// delivery must retry the take side themselves; the error names the
// failed leg.
func ForwardOnce(ctx context.Context, src Backend, dst Backend) (IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return IncomingMessage{}, fmt.Errorf("transports: forward context: %w", err)
	}

	if src == nil {
		return IncomingMessage{}, errors.New("transports: forward source is nil") //nolint:wrapcheck // new error, nothing to wrap
	}

	if dst == nil {
		return IncomingMessage{}, errors.New("transports: forward destination is nil") //nolint:wrapcheck // new error, nothing to wrap
	}

	msg, err := src.TakeBytes(ctx)
	if err != nil {
		return IncomingMessage{}, fmt.Errorf("transports: forward take: %w", err)
	}

	out := OutgoingMessage(msg)

	err = dst.PublishBytes(ctx, out)
	if err != nil {
		return IncomingMessage{}, fmt.Errorf("transports: forward publish: %w", err)
	}

	return msg, nil
}

// ForwardLoop relays every message from src to dst until ctx ends.
//
// It calls ForwardOnce in a loop. A nil backend fails fast on the
// first iteration instead of hanging the loop. Context cancellation
// ends the loop and reports ctx.Err. Any other take or publish error
// ends the loop with a wrapped error naming the failed leg. Close
// either backend to shut down a loop that has no traffic.
func ForwardLoop(ctx context.Context, src Backend, dst Backend) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("transports: forward loop context: %w", err)
	}

	if src == nil {
		return errors.New("transports: forward loop source is nil") //nolint:wrapcheck // new error, nothing to wrap
	}

	if dst == nil {
		return errors.New("transports: forward loop destination is nil") //nolint:wrapcheck // new error, nothing to wrap
	}

	for {
		err = ctx.Err()
		if err != nil {
			return fmt.Errorf("transports: forward loop context: %w", err)
		}

		_, err = ForwardOnce(ctx, src, dst)
		if err != nil {
			return err
		}
	}
}
