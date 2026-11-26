// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/podomy/weld/internal/transports"
)

// sliceSource serves fixed frames oldest first for sampled take tests.
type sliceSource struct {
	mu     sync.Mutex
	frames [][]byte
	guids  []WriterGUID
	next   int
}

// ReadFrame returns the next frame or ctx.Err when exhausted.
func (s *sliceSource) ReadFrame(ctx context.Context) (WriterGUID, uint64, []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := ctx.Err()
	if err != nil {
		return WriterGUID{}, 0, nil, err //nolint:wrapcheck // test source passes ctx through unchanged
	}

	if s.next >= len(s.frames) {
		return WriterGUID{}, 0, nil, ctx.Err() //nolint:wrapcheck // test source passes ctx through unchanged
	}

	guid := s.guids[s.next]
	frame := append([]byte(nil), s.frames[s.next]...)
	s.next++

	return guid, 0, frame, nil
}

// TestTakeSampledConcurrentPairing checks take-plus-info stays paired.
//
// Concurrent TakeSampled calls must each report the info for their own
// payload, never a neighbor racing through the shared side channel.
func TestTakeSampledConcurrentPairing(t *testing.T) {
	t.Parallel()

	const n = 16
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	for i := range n {
		mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/pair", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, fmt.Sprintf("msg-%d", i))})
	}

	frames := make([][]byte, 0, n)
	for i := range n {
		frames = append(frames, sink.frameAt(t, i))
	}

	recv := NewDataReceiver()
	var wg sync.WaitGroup
	errs := make([]error, n)
	got := make([]SampledMessage, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sample, err := recv.TakeSampled(frames[i])
			if err != nil {
				errs[i] = err

				return
			}
			got[i] = sample
		}(i)
	}
	wg.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Fatalf("take sampled %d: %v", i, errs[i])
		}
		if got[i].Info.SampleSequence != uint64(i+1) {
			t.Fatalf("sample %d sequence = %d, want %d", i, got[i].Info.SampleSequence, i+1)
		}
		want := encodeTestString(t, fmt.Sprintf("msg-%d", i))
		if string(got[i].Msg.Payload) != string(want) {
			t.Fatalf("sample %d payload mismatch", i)
		}
	}
}

// TestTakeFromSampledConcurrentPairing checks RTPS take-plus-info pairing.
func TestTakeFromSampledConcurrentPairing(t *testing.T) {
	t.Parallel()

	const n = 8
	guid := testWriterGUID()
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, guid, sink)
	for i := range n {
		mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/pairfrom", TypeName: "std_msgs__msg__String", Payload: encodeTestString(t, fmt.Sprintf("from-%d", i))})
	}

	src := &sliceSource{}
	for i := range n {
		payload, err := MarshalRTPSPayload(sink.frameAt(t, i))
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		src.frames = append(src.frames, payload)
		src.guids = append(src.guids, guid)
	}

	recv := NewDataReceiver()
	var wg sync.WaitGroup
	errs := make([]error, n)
	got := make([]SampledMessage, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sample, err := recv.TakeFromSampled(context.Background(), src)
			if err != nil {
				errs[i] = err

				return
			}
			got[i] = sample
		}(i)
	}
	wg.Wait()

	seen := make(map[uint64]string, n)
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("take from sampled %d: %v", i, errs[i])
		}
		seq := got[i].Info.SampleSequence
		seen[seq] = string(got[i].Msg.Payload)
	}
	if len(seen) != n {
		t.Fatalf("paired samples = %d, want %d", len(seen), n)
	}
	for seq, payload := range seen {
		want := encodeTestString(t, fmt.Sprintf("from-%d", seq-1))
		if payload != string(want) {
			t.Fatalf("seq %d payload mismatch", seq)
		}
	}
}
