// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Content filtered topic live gate timeouts and sentinels.
//
// Each test allocates one exclusive domain from the parallel pool and one
// per-run topic, so concurrent runs never share samples. The talkers are
// unfiltered Cyclone `ros2 topic pub` procs; filtering happens in the Weld
// reader via dds.ContentFilter until the take-path hook lands (see the
// INTEGRATION notes at the end of this file). Timeouts are generous so
// discovery plus NACK repair complete before the take window ends.
const (
	contentFilterOverall time.Duration = 4 * time.Minute
	contentFilterSettle  time.Duration = 6 * time.Second
	contentFilterTake    time.Duration = 60 * time.Second
	contentFilterProbe   time.Duration = 5 * time.Second
)

// contentFilterStringDesc describes the std_msgs String wire shape.
func contentFilterStringDesc() *types.MessageMembers {
	return &types.MessageMembers{
		Namespace:   "std_msgs__msg",
		Name:        "String",
		MemberCount: 1,
		Members: []types.MessageMember{
			{Name: "data", TypeID: types.RosTypeString},
		},
	}
}

// contentFilterDecodeString reads the data field from one XCDR1 sample.
func contentFilterDecodeString(payload []byte) (string, error) {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return "", err //nolint:wrapcheck // test helper returns decoder error unwrapped
	}

	s, err := dec.ReadString()
	if err != nil {
		return "", err //nolint:wrapcheck // test helper returns decoder error unwrapped
	}

	return s, nil
}

// TestROSContentFilterMixedTalker checks a filtered Weld reader against an
// unfiltered Cyclone talker mix.
//
// Two talkers publish fixed keep and drop payloads on one per-run topic.
// The Weld reader subscribes normally, takes for a bounded window, and
// classifies every sample with the "data LIKE 'keep-%'" filter. It fails
// loudly unless both payload kinds arrived raw (proving the writer side
// is unfiltered), the filtered set equals exactly the keep payload, every
// drop sample classified as filter-miss, and no sample hit eval-error.
//
//nolint:gocognit,cyclop // live gate sequences two talkers plus take classify in one run, splitting would obscure intent
func TestROSContentFilterMixedTalker(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), contentFilterOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)

	runID := helpers.LiveID("ros-contentfilter")
	topic := helpers.LiveTopic(runID)
	keepPayload := "keep-" + runID
	dropPayload := "drop-" + runID

	keepID := runID + "-keep"
	dropID := runID + "-drop"
	keepPid := helpers.HostPidPath(t, keepID)
	dropPid := helpers.HostPidPath(t, dropID)

	err := helpers.StartPeerProc(ctx, helpers.Container, domain, keepID,
		helpers.RosBin, "topic", "pub", topic, helpers.RosType, "{data: "+keepPayload+"}", "--rate", "2")
	if err != nil {
		t.Fatalf("start keep talker: %v", err)
	}

	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(context.WithoutCancel(ctx), helpers.Container, domain, keepPid, topic)
	})

	err = helpers.StartPeerProc(ctx, helpers.Container, domain, dropID,
		helpers.RosBin, "topic", "pub", topic, helpers.RosType, "{data: "+dropPayload+"}", "--rate", "2")
	if err != nil {
		t.Fatalf("start drop talker: %v", err)
	}

	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(context.WithoutCancel(ctx), helpers.Container, domain, dropPid, topic)
	})

	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "ros-contentfilter-"+runID, "contentfilter") //nolint:gosec // G115: pooled domain fits uint32

	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic,
		helpers.ForwardStringHash, contentFilterProbe, "contentfilter")
	if err != nil {
		t.Fatalf("resolve topic hash: %v", err)
	}

	err = helpers.SubscribeLiveReader(ctx, backend, topic, helpers.ForwardStringIDL, typeHash, "contentfilter")
	if err != nil {
		t.Fatalf("subscribe filtered reader: %v", err)
	}

	helpers.Sleep(ctx, contentFilterSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before take: %v", ctx.Err())
	}

	filter, err := dds.CompileContentFilter("data LIKE 'keep-%'")
	if err != nil {
		t.Fatalf("compile mixed filter: %v", err)
	}

	desc := contentFilterStringDesc()
	raw := map[string]bool{}
	matched := map[string]bool{}
	misses := 0

	takeCtx, takeCancel := context.WithTimeout(ctx, contentFilterTake)
	defer takeCancel()

	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
				break
			}

			continue
		}

		// Cyclone discovery chatter (for example ros_discovery_info)
		// shares the domain and reaches takes; only the test topic
		// counts, mirroring the weld echo topic check. Undecodable
		// samples on the test topic itself still fail loudly below.
		if msg.TopicName != topic {
			continue
		}

		text, derr := contentFilterDecodeString(msg.Payload)
		if derr != nil {
			t.Fatalf("decode live string: %v", derr)
		}

		raw[text] = true

		ok, reason, aerr := filter.Apply(msg.Payload, desc)
		if aerr != nil {
			if errors.Is(aerr, dds.ErrContentFilterEvalError) {
				t.Fatalf("eval error on live sample %q: %v", text, aerr)
			}

			if !ok {
				misses++

				continue
			}

			t.Fatalf("filter error on live sample %q: %v", text, aerr)
		}

		if !ok {
			if reason != dds.DropReasonContentFilterMiss {
				t.Fatalf("miss on %q carried reason %q", text, reason)
			}

			misses++

			continue
		}

		matched[text] = true
	}

	if !raw[keepPayload] {
		t.Fatalf("reader never saw keep payload %q (raw %v)", keepPayload, raw)
	}

	if !raw[dropPayload] {
		t.Fatalf("reader never saw drop payload %q, writer mix missing (raw %v)", dropPayload, raw)
	}

	if len(matched) != 1 || !matched[keepPayload] {
		t.Fatalf("filtered set %v, want exactly {%q}", matched, keepPayload)
	}

	if misses == 0 {
		t.Fatalf("filtered-out count is 0, want at least one drop sample")
	}

	t.Logf("contentfilter mixed ok keep=%q drops=%d raw=%v", keepPayload, misses, raw)
}

// TestROSContentFilterAlwaysFalse checks an always-false filter receives
// nothing while the reader stays healthy.
//
// One talker publishes live samples on a per-run topic. The filter matches
// a value that never appears, so every sample must classify as
// filter-miss. It fails loudly unless at least one raw sample arrived
// (proving the reader is live), the matched set is empty, and no sample
// hit eval-error.
//
//nolint:gocognit,cyclop // live gate sequences talker plus take classify in one run, splitting would obscure intent
func TestROSContentFilterAlwaysFalse(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), contentFilterOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)

	runID := helpers.LiveID("ros-contentfilter-false")
	topic := helpers.LiveTopic(runID)
	livePayload := "live-" + runID
	talkID := runID + "-talk"
	talkPid := helpers.HostPidPath(t, talkID)

	err := helpers.StartPeerProc(ctx, helpers.Container, domain, talkID,
		helpers.RosBin, "topic", "pub", topic, helpers.RosType, "{data: "+livePayload+"}", "--rate", "2")
	if err != nil {
		t.Fatalf("start talker: %v", err)
	}

	t.Cleanup(func() {
		helpers.StopPeerProcAndWait(context.WithoutCancel(ctx), helpers.Container, domain, talkPid, topic)
	})

	backend := helpers.MustOpenLiveBackend(t, ctx, uint32(domain), "ros-contentfilter-false-"+runID, "contentfilter") //nolint:gosec // G115: pooled domain fits uint32

	typeHash, err := helpers.ResolveTopicHash(ctx, helpers.Container, domain, helpers.RosBin, topic,
		helpers.ForwardStringHash, contentFilterProbe, "contentfilter")
	if err != nil {
		t.Fatalf("resolve topic hash: %v", err)
	}

	err = helpers.SubscribeLiveReader(ctx, backend, topic, helpers.ForwardStringIDL, typeHash, "contentfilter")
	if err != nil {
		t.Fatalf("subscribe filtered reader: %v", err)
	}

	helpers.Sleep(ctx, contentFilterSettle)
	if ctx.Err() != nil {
		t.Fatalf("context ended before take: %v", ctx.Err())
	}

	filter, err := dds.CompileContentFilter("data = 'no-such-sample-" + runID + "-zzz'")
	if err != nil {
		t.Fatalf("compile always-false filter: %v", err)
	}

	desc := contentFilterStringDesc()
	raw := 0
	matched := 0

	takeCtx, takeCancel := context.WithTimeout(ctx, contentFilterTake)
	defer takeCancel()

	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
				break
			}

			continue
		}

		// Same discovery-chatter skip as the mixed gate above.
		if msg.TopicName != topic {
			continue
		}

		text, derr := contentFilterDecodeString(msg.Payload)
		if derr != nil {
			t.Fatalf("decode live string: %v", derr)
		}

		raw++

		ok, reason, aerr := filter.Apply(msg.Payload, desc)
		if aerr != nil && errors.Is(aerr, dds.ErrContentFilterEvalError) {
			t.Fatalf("eval error on live sample %q: %v", text, aerr)
		}

		if ok {
			matched++
		} else if reason != dds.DropReasonContentFilterMiss {
			t.Fatalf("non-match on %q carried reason %q, want filter-miss", text, reason)
		}
	}

	if raw == 0 {
		t.Fatalf("reader saw no samples, reader unhealthy")
	}

	if matched != 0 {
		t.Fatalf("always-false filter matched %d samples, want 0", matched)
	}

	t.Logf("contentfilter always-false ok raw=%d matched=%d", raw, matched)
}
