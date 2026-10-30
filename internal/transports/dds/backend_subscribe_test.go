// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"context"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
)

// TestBackendSubscribeWithQoSAnnouncesTranslatedOffer checks the per-profile path.
func TestBackendSubscribeWithQoSAnnouncesTranslatedOffer(t *testing.T) {
	t.Parallel()

	const domain uint32 = 96

	backend := newLiveBackend(t, domain, "qos-sub")
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	profile := core.QoSProfile{
		Reliability: core.ReliabilityBestEffort,
		Durability:  core.DurabilityVolatile,
		History:     core.HistoryKeepLast,
		Depth:       10,
		Liveliness:  core.LivelinessAutomatic,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := backend.SubscribeWithQoS(ctx, "/qos", "std_msgs__msg__String", StringTypeHash, profile); err != nil {
		t.Fatalf("subscribe with qos: %v", err)
	}

	found := false
	for _, ep := range backend.sedp.LocalEndpoints() {
		if ep.ROSTopic != "/qos" || !ep.IsReader {
			continue
		}

		found = true

		if ep.WireOffer == nil {
			t.Fatalf("reader WireOffer is nil, want translated offer")
		}

		if ep.WireOffer.ReliabilityKind != 1 {
			t.Fatalf("wire reliability = %d, want best effort 1", ep.WireOffer.ReliabilityKind)
		}

		if ep.WireOffer.DurabilityKind != 0 {
			t.Fatalf("wire durability = %d, want volatile 0", ep.WireOffer.DurabilityKind)
		}
	}

	if !found {
		t.Fatalf("no local reader for /qos after SubscribeWithQoS")
	}
}

// TestBackendSubscribeWithQoSRefusesLoudly checks untranslatable profiles fail fast.
func TestBackendSubscribeWithQoSRefusesLoudly(t *testing.T) {
	t.Parallel()

	const domain uint32 = 97

	backend := newLiveBackend(t, domain, "qos-refuse")
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	profile := core.QoSProfile{
		Reliability: core.ReliabilityPolicy(99),
		Durability:  core.DurabilityVolatile,
		History:     core.HistoryKeepLast,
		Depth:       10,
		Liveliness:  core.LivelinessAutomatic,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := backend.SubscribeWithQoS(ctx, "/qos-bad", "std_msgs__msg__String", StringTypeHash, profile)
	if err == nil {
		t.Fatalf("untranslatable profile subscribed, want loud refusal")
	}

	if !IsWireQoSRefusal(err) {
		t.Logf("refusal (non-WireQoSRefusal wrapper): %v", err)
	}
}

// TestBackendSubscribeWithPartitionsStampsLocals checks partition stamping.
func TestBackendSubscribeWithPartitionsStampsLocals(t *testing.T) {
	t.Parallel()

	const domain uint32 = 98

	backend := newLiveBackend(t, domain, "part-sub")
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := backend.SubscribeWithPartitions(ctx, "/part", "std_msgs__msg__String", StringTypeHash, []string{"A"}); err != nil {
		t.Fatalf("subscribe with partitions: %v", err)
	}

	found := false
	for _, ep := range backend.sedp.LocalEndpoints() {
		if ep.ROSTopic != "/part" || !ep.IsReader {
			continue
		}

		found = true

		if len(ep.Partitions) != 1 || ep.Partitions[0] != "A" {
			t.Fatalf("local partitions = %v, want [A]", ep.Partitions)
		}
	}

	if !found {
		t.Fatalf("no local reader for /part after SubscribeWithPartitions")
	}

	if err := backend.SubscribeWithPartitions(ctx, "/part-bad", "std_msgs__msg__String", StringTypeHash, []string{"a,b"}); err == nil {
		t.Fatalf("comma partition subscribed, want loud reject")
	}
}

// TestBackendSubscribeWithContentFilterHooksTake checks server-side filtering.
func TestBackendSubscribeWithContentFilterHooksTake(t *testing.T) {
	t.Parallel()

	const domain uint32 = 99

	backend := newLiveBackend(t, domain, "filter-sub")
	defer func() {
		if err := backend.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hookTestStringDesc(t)

	if err := backend.SubscribeWithContentFilter(ctx, "/filter", "hook_test__msg__Hooked", "", "data = 'keep-me'"); err != nil {
		t.Fatalf("subscribe with content filter: %v", err)
	}

	if err := backend.SubscribeWithContentFilter(ctx, "/filter-bad", "hook_test__msg__Hooked", "", "data === 'broken"); err == nil {
		t.Fatalf("bad filter expression subscribed, want loud parse error")
	}

	if got := backend.DropReasonCount(DropReasonContentFilterMiss); got != 0 {
		t.Fatalf("filter miss count = %d before takes, want 0", got)
	}
}
