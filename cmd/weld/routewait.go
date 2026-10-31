// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
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
			return err
		}

		if time.Since(start) >= timeout {
			return err
		}

		cerr := ctx.Err()
		if cerr != nil {
			if errors.Is(cerr, context.Canceled) {
				return cerr
			}

			if time.Since(start) >= timeout {
				return err
			}

			return cerr
		}

		timer := time.NewTimer(pubRouteRetryInterval)

		select {
		case <-ctx.Done():
			timer.Stop()

			cerr = ctx.Err()
			if cerr != nil {
				if errors.Is(cerr, context.Canceled) {
					return cerr
				}

				if errors.Is(err, dds.ErrNoRoute) {
					return err
				}

				return cerr
			}

			return err
		case <-timer.C:
		}
	}
}
