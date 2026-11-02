// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/podomy/weld/sdk"
)

// publishRaceFixture publishes count samples without blocking.
func publishRaceFixture(ctx context.Context, pub *sdk.Publisher, count int, done chan<- struct{}) {
	defer close(done)
	for i := range count {
		payload := fmt.Appendf(nil, "msg-%03d", i)
		err := pub.Publish(ctx, payload)
		if err != nil {
			return
		}
	}
}

// drainRaceAvailable receives until the publisher is done and the queue
// stays empty for one timeout.
func drainRaceAvailable(ctx context.Context, t *testing.T, sub *sdk.Subscription, done <-chan struct{}) int {
	t.Helper()
	received := 0
	for {
		callCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		msg, err := sub.Next(callCtx)
		cancel()
		if err != nil {
			select {
			case <-done:
				return received
			default:
				continue
			}
		}
		_ = msg
		received++
	}
}

// drainRaceRemainder receives the final queued samples after the publisher
// is done.
func drainRaceRemainder(ctx context.Context, sub *sdk.Subscription, received int) int {
	for {
		callCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		_, err := sub.Next(callCtx)
		cancel()
		if err != nil {
			return received
		}
		received++
	}
}

// checkRaceAccounting pins that lost plus received plus queued equals published.
func checkRaceAccounting(t *testing.T, received, queued, published int, lost uint64) {
	t.Helper()
	total := int64(received) + int64(queued) + int64(lost) //nolint:gosec // counts are small test totals fitting int64
	if total != int64(published) {
		t.Fatalf("accounting: received %d + lost %d + queued %d != published %d", received, lost, queued, published)
	}
}

// TestTopicDeliverNextLostRace pins that concurrent deliver and Next never
// overcount lost when Next frees space between the first send probe and
// the eviction. Lost plus received plus queued must equal published.
func TestTopicDeliverNextLostRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bus := sdk.NewBus()
	node, err := sdk.NewNode("race-node", bus)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	t.Cleanup(func() {
		cerr := node.Close()
		if cerr != nil {
			t.Errorf("close node: %v", cerr)
		}
	})
	qos := sdk.DefaultQoS()
	qos.Depth = 4
	pub, err := node.NewPublisher("/race", "std_msgs/msg/String", qos)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	sub, err := node.NewSubscription("/race", "std_msgs/msg/String", qos)
	if err != nil {
		t.Fatalf("new subscription: %v", err)
	}
	const published = 200
	done := make(chan struct{})
	go publishRaceFixture(ctx, pub, published, done)
	received := drainRaceAvailable(ctx, t, sub, done)
	received = drainRaceRemainder(ctx, sub, received)
	checkRaceAccounting(t, received, sub.Len(), published, sub.Lost())
}
