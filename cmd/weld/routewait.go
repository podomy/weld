// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
)

// publishWithRouteRetry retries PublishBytes on ErrNoRoute until timeout or ctx ends.
//
// SPDP exchange needs time after JoinDiscovery, so the first publish can
// see empty routes. Only ErrNoRoute retries with a short sleep; other
// errors return at once. The wait is bounded by timeout or ctx cancel,
// whichever ends first. A cancelled context reports cancel; a deadline
// wait that outlasts the timeout preserves no-route loudly.
func publishWithRouteRetry(ctx context.Context, backend transports.Backend, msg transports.OutgoingMessage, timeout time.Duration) error {
	start := time.Now()

	for {
		err := backend.PublishBytes(ctx, msg)
		if err == nil {
			return nil
		}

		if !errors.Is(err, dds.ErrNoRoute) {
			return fmt.Errorf("weld: publish: %w", err)
		}

		if time.Since(start) >= timeout {
			return fmt.Errorf("weld: publish no route: %w", err)
		}

		routeErr := checkRouteContext(ctx, start, timeout, err)
		if routeErr != nil {
			return routeErr
		}

		waitErr := awaitRouteRetry(ctx, err)
		if waitErr != nil {
			return waitErr
		}
	}
}

// checkRouteContext returns nil when retry should continue after a no-route
// publish, else a wrapped error carrying cancel or timeout context.
func checkRouteContext(ctx context.Context, start time.Time, timeout time.Duration, publishErr error) error {
	cerr := ctx.Err()
	if cerr == nil {
		return nil
	}

	if errors.Is(cerr, context.Canceled) {
		return fmt.Errorf("weld: publish canceled: %w", cerr)
	}

	if time.Since(start) >= timeout {
		return fmt.Errorf("weld: publish no route: %w", publishErr)
	}

	return fmt.Errorf("weld: publish context: %w", cerr)
}

// awaitRouteRetry waits one retry interval or ctx end.
//
// A timer fire returns nil so the caller retries publish. A ctx end returns
// a wrapped error preserving cancel or no-route loudly.
func awaitRouteRetry(ctx context.Context, publishErr error) error {
	timer := time.NewTimer(pubRouteRetryInterval)

	select {
	case <-ctx.Done():
		timer.Stop()

		cerr := ctx.Err()
		if cerr != nil {
			if errors.Is(cerr, context.Canceled) {
				return fmt.Errorf("weld: publish canceled: %w", cerr)
			}

			if errors.Is(publishErr, dds.ErrNoRoute) {
				return fmt.Errorf("weld: publish no route: %w", publishErr)
			}

			return fmt.Errorf("weld: publish context: %w", cerr)
		}

		return fmt.Errorf("weld: publish no route: %w", publishErr)
	case <-timer.C:
		return nil
	}
}
