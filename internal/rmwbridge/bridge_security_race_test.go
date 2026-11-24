// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// hammerPublishLoop publishes until stop closes, ignoring expected race errors.
func hammerPublishLoop(pub *rmwbridge.Publisher, stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := pub.Publish(ctx, []byte("ping"))
		cancel()
		if err != nil {
			continue
		}
	}
}

// hammerTakeLoop takes until stop closes, ignoring expected race errors.
func hammerTakeLoop(sub *rmwbridge.Subscription, stop chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := sub.Take(ctx)
		cancel()
		if err != nil {
			continue
		}
	}
}

// hammerBridgeLoop publishes and takes at bridge level until stop closes.
func hammerBridgeLoop(t *testing.T, b *rmwbridge.Bridge, stop chan struct{}) {
	t.Helper()
	for {
		select {
		case <-stop:
			return
		default:
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := b.PublishBytes(ctx, rmwbridge.OutgoingMessage{TopicName: "/race"})
		cancel()
		if err != nil {
			continue
		}
		ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err = b.TakeBytes(ctx2)
		cancel2()
		if err != nil {
			continue
		}
	}
}

// hammerCloserLoop closes handles repeatedly and reports unexpected errors.
func hammerCloserLoop(t *testing.T, b *rmwbridge.Bridge, pub *rmwbridge.Publisher, sub *rmwbridge.Subscription) {
	t.Helper()
	for range 10 {
		err := pub.Close()
		if err != nil {
			t.Errorf("pub close: %v", err)
			return
		}
		err = sub.Close()
		if err != nil {
			t.Errorf("sub close: %v", err)
			return
		}
		err = b.Close()
		if err != nil {
			t.Errorf("bridge close: %v", err)
			return
		}
	}
}

// startRaceHammers launches publishers, takers, bridge loops, and closer.
func startRaceHammers(t *testing.T, b *rmwbridge.Bridge, pub *rmwbridge.Publisher, sub *rmwbridge.Subscription, stop chan struct{}, wg *sync.WaitGroup) {
	t.Helper()
	for range 4 {
		wg.Go(func() {
			hammerPublishLoop(pub, stop)
		})
	}
	for range 4 {
		wg.Go(func() {
			hammerTakeLoop(sub, stop)
		})
	}
	for range 2 {
		wg.Go(func() {
			hammerBridgeLoop(t, b, stop)
		})
	}
	wg.Go(func() {
		hammerCloserLoop(t, b, pub, sub)
	})
}

// TestSecurityConcurrentPublishTakeClose hammers publish, take, and close.
func TestSecurityConcurrentPublishTakeClose(t *testing.T) {
	t.Parallel()
	b := newSecurityBridge(t)
	n := mustNode(t, b, "race")
	qos := rmwbridge.DefaultQoS()
	pub, err := b.CreatePublisher(n, "/race", qos)
	requireNoError(t, err, "create publisher")
	sub, err := b.CreateSubscription(n, "/race", qos)
	requireNoError(t, err, "create subscription")

	var wg sync.WaitGroup
	stop := make(chan struct{})
	startRaceHammers(t, b, pub, sub, stop, &wg)

	time.Sleep(securityTimeout)
	close(stop)
	wg.Wait()
	err = pub.Close()
	requireNoError(t, err, "final pub close")
	err = sub.Close()
	requireNoError(t, err, "final sub close")
	err = b.Close()
	requireNoError(t, err, "final bridge close")
}
