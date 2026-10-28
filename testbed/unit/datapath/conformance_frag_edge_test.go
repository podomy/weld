// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_datapath holds DATA_FRAG edge conformance pins.
package unit_datapath

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/internal/types"
)

// fragEdgeMaxPayloadLen mirrors maxPayloadLen in datapath.go.
const fragEdgeMaxPayloadLen = 32 << 20

// fragEdgeMaxCount mirrors fragMaxCount in frag.go.
const fragEdgeMaxCount = 131072

// fragEdgeMaxEntries mirrors fragMaxEntries in frag.go.
const fragEdgeMaxEntries = 16

// fragEdgeReassemblyTimeout mirrors fragReassemblyTimeout in frag.go.
const fragEdgeReassemblyTimeout = 10 * time.Second

// fragEdgeUserWriter is the user DATA writer entity 00 00 02 03.
var fragEdgeUserWriter = rtps.EntityID{0x00, 0x00, 0x02, 0x03}

// fragEdgeKey names one fragmented sample.
type fragEdgeKey struct {
	guid dds.WriterGUID
	seq  uint64
}

// fragEdgeEntry holds one in progress sample.
type fragEdgeEntry struct {
	sampleSize    uint32
	fragmentSize  uint16
	total         uint32
	buf           []byte
	received      []bool
	receivedCount uint32
	firstSeen     time.Time
}

// fragEdgeReassembler collects fragments until complete.
type fragEdgeReassembler struct {
	mu         sync.Mutex
	entries    map[fragEdgeKey]*fragEdgeEntry
	maxEntries int
	timeout    time.Duration
	now        func() time.Time
}

// fragEdgeNewReassembler returns an empty edge reassembler.
func fragEdgeNewReassembler(timeout time.Duration, now func() time.Time) *fragEdgeReassembler {
	if now == nil {
		now = time.Now
	}
	return &fragEdgeReassembler{
		entries:    make(map[fragEdgeKey]*fragEdgeEntry),
		maxEntries: fragEdgeMaxEntries,
		timeout:    timeout,
		now:        now,
	}
}

// fragEdgeBuildBody builds one DATA_FRAG body in little endian order.
func fragEdgeBuildBody(t *testing.T, reader, writer rtps.EntityID, seq uint64, start uint32, count, fragSize uint16, sampleSize uint32, fragPayload []byte) []byte {
	t.Helper()
	return fragTestBuildBody(t, reader, writer, seq, start, count, fragSize, sampleSize, fragPayload)
}

// fragEdgeCheckBounds validates sample size and total count.
func fragEdgeCheckBounds(sampleSize uint32, fragmentSize uint16) (uint32, error) {
	if fragmentSize == 0 {
		err := fmt.Errorf("frag edge: fragment size 0: %w", dds.ErrDataFragDropped)
		return 0, err
	}
	if uint64(sampleSize) > uint64(fragEdgeMaxPayloadLen) {
		err := fmt.Errorf("frag edge: sample %d above cap %d: %w", sampleSize, fragEdgeMaxPayloadLen, dds.ErrDataFragDropped)
		return 0, err
	}
	total64 := (uint64(sampleSize) + uint64(fragmentSize) - 1) / uint64(fragmentSize)
	if total64 == 0 || total64 > fragEdgeMaxCount {
		err := fmt.Errorf("frag edge: total %d out of range: %w", total64, dds.ErrDataFragDropped)
		return 0, err
	}
	return uint32(total64), nil
}

// fragEdgeIsUserWriter reports whether id is a user writer.
func fragEdgeIsUserWriter(id rtps.EntityID) bool {
	if id == rtps.EntitySPDPWriter {
		return false
	}
	first := rtps.EntityID{0x00, 0x00, 0x02, 0xC2}
	cyclone := rtps.EntityID{0x00, 0x00, 0x03, 0xC2}
	second := rtps.EntityID{0x00, 0x00, 0x04, 0xC2}
	if id == first || id == cyclone || id == second {
		return false
	}
	return true
}

// fragEdgeSliceLen returns the octet length for fragment number fragNum.
func fragEdgeSliceLen(fragNum, total uint32, fragmentSize uint16, sampleSize uint32) uint64 {
	if fragNum == total {
		rem := uint64(sampleSize) - uint64(total-1)*uint64(fragmentSize)
		return rem
	}
	return uint64(fragmentSize)
}

// fragEdgeCopyRun copies one fragment run into the entry buffer.
func fragEdgeCopyRun(entry *fragEdgeEntry, frag rtps.DataFrag) error {
	offInPayload := uint64(0)
	for i := range uint64(frag.FragmentsInSubmessage) {
		fragNum := uint64(frag.FragmentStartingNum) + i
		idx := fragNum - 1
		wantLen := fragEdgeSliceLen(uint32(fragNum), entry.total, entry.fragmentSize, entry.sampleSize) //nolint:gosec // G115: fragNum bounds by total fragments above
		if offInPayload+wantLen > uint64(len(frag.Payload)) {
			err := fmt.Errorf("frag edge: run truncated: %w", dds.ErrDataFragDropped)
			return err
		}
		if entry.received[idx] {
			offInPayload += wantLen
			continue
		}
		dstOff := (fragNum - 1) * uint64(entry.fragmentSize)
		copy(entry.buf[dstOff:dstOff+wantLen], frag.Payload[offInPayload:offInPayload+wantLen])
		entry.received[idx] = true
		entry.receivedCount++
		offInPayload += wantLen
	}
	return nil
}

// fragEdgeSweep deletes expired entries and returns the dropped count.
func fragEdgeSweep(f *fragEdgeReassembler) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := f.now()
	dropped := 0
	for key, entry := range f.entries {
		if now.Sub(entry.firstSeen) < f.timeout {
			continue
		}
		delete(f.entries, key)
		dropped++
	}
	return dropped
}

// fragEdgeLen returns the number of in progress entries.
func fragEdgeLen(f *fragEdgeReassembler) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.entries)
}

// fragEdgeReceivedCount returns the received fragment count for guid and seq.
func fragEdgeReceivedCount(f *fragEdgeReassembler, guid dds.WriterGUID, seq uint64) uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	entry, seen := f.entries[fragEdgeKey{guid: guid, seq: seq}]
	if !seen {
		return 0
	}
	return entry.receivedCount
}

// fragEdgeAdd stores one fragment run and returns complete payload when done.
func fragEdgeAdd(f *fragEdgeReassembler, guid dds.WriterGUID, frag rtps.DataFrag) ([]byte, bool, error) {
	total, err := fragEdgeCheckBounds(frag.SampleSize, frag.FragmentSize)
	if err != nil {
		return nil, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fragEdgeKey{guid: guid, seq: frag.SequenceNumber}
	entry, seen := f.entries[key]
	if !seen {
		if len(f.entries) >= f.maxEntries {
			derr := fmt.Errorf("frag edge: table full: %w", dds.ErrDataFragDropped)
			return nil, false, derr
		}
		entry = &fragEdgeEntry{
			sampleSize:   frag.SampleSize,
			fragmentSize: frag.FragmentSize,
			total:        total,
			buf:          make([]byte, frag.SampleSize),
			received:     make([]bool, total),
			firstSeen:    f.now(),
		}
		f.entries[key] = entry
	} else if entry.sampleSize != frag.SampleSize || entry.fragmentSize != frag.FragmentSize {
		derr := fmt.Errorf("frag edge: inconsistent sample: %w", dds.ErrDataFragDropped)
		return nil, false, derr
	}
	cerr := fragEdgeCopyRun(entry, frag)
	if cerr != nil {
		return nil, false, cerr
	}
	if entry.receivedCount != entry.total {
		return nil, false, nil
	}
	out := append([]byte(nil), entry.buf...)
	delete(f.entries, key)
	return out, true, nil
}

// fragEdgeDecode decodes one DATA_FRAG body with drop accounting.
func fragEdgeDecode(t *testing.T, body []byte) rtps.DataFrag {
	t.Helper()
	var drops rtps.Counter
	decoded, err := rtps.DecodeDataFrag(body, rtps.FlagLittleEndian, &drops)
	if err != nil {
		t.Fatalf("decode frag: %v", err)
	}
	if drops.Count() != 0 {
		t.Fatalf("decode drops = %d want 0", drops.Count())
	}
	return decoded
}

// fragEdgeSplit builds one decoded fragment per piece for sample.
func fragEdgeSplit(t *testing.T, sample []byte, seq uint64, fragSize uint16, writer rtps.EntityID) []rtps.DataFrag {
	t.Helper()
	sampleLen := len(sample)
	if sampleLen == 0 {
		t.Fatalf("sample is empty")
	}
	sampleSize := uint32(sampleLen) //nolint:gosec // G115: test payload length is small and non-negative
	total, err := fragEdgeCheckBounds(sampleSize, fragSize)
	if err != nil {
		t.Fatalf("bounds: %v", err)
	}
	if total < 2 {
		t.Fatalf("total = %d want at least 2", total)
	}
	out := make([]rtps.DataFrag, 0, total)
	for n := uint32(1); n <= total; n++ {
		off := int(n-1) * int(fragSize)
		end := min(off+int(fragSize), len(sample))
		slice := sample[off:end]
		body := fragEdgeBuildBody(t, rtps.EntityUnknown, writer, seq, n, 1, fragSize, sampleSize, slice)
		decoded := fragEdgeDecode(t, body)
		if decoded.SequenceNumber != seq {
			t.Fatalf("frag %d seq = %d want %d", n, decoded.SequenceNumber, seq)
		}
		if decoded.FragmentStartingNum != n {
			t.Fatalf("frag %d start = %d want %d", n, decoded.FragmentStartingNum, n)
		}
		out = append(out, decoded)
	}
	return out
}

// fragEdgePollSweep polls sweep until one drop or context ends without spawning a goroutine.
func fragEdgePollSweep(ctx context.Context, f *fragEdgeReassembler) (int, error) {
	for {
		err := ctx.Err()
		if err != nil {
			return 0, fmt.Errorf("frag edge poll sweep: %w", err)
		}
		dropped := fragEdgeSweep(f)
		if dropped > 0 {
			return dropped, nil
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			cerr := ctx.Err()
			if cerr != nil {
				return 0, fmt.Errorf("frag edge poll sweep done: %w", cerr)
			}
			return 0, context.DeadlineExceeded
		case <-timer.C:
		}
	}
}

// fragEdgeAddReverse adds fragments in reverse order and returns the completed payload.
func fragEdgeAddReverse(t *testing.T, reasm *fragEdgeReassembler, guid dds.WriterGUID, frags []rtps.DataFrag) []byte {
	t.Helper()
	var complete []byte
	done := false
	for i, frag := range slices.Backward(frags) {
		out, finished, err := fragEdgeAdd(reasm, guid, frag)
		if err != nil {
			t.Fatalf("add frag %d: %v", frag.FragmentStartingNum, err)
		}
		if i > 0 && finished {
			t.Fatalf("frag %d finished early, want incomplete", frag.FragmentStartingNum)
		}
		if i == 0 && !finished {
			t.Fatalf("last add did not finish, want complete")
		}
		if finished {
			complete = out
			done = finished
		}
	}
	if !done {
		t.Fatalf("reassembly did not complete")
	}
	return complete
}

// fragEdgeCheckReassembled verifies a completed payload round trips through the receiver.
func fragEdgeCheckReassembled(t *testing.T, reasm *fragEdgeReassembler, complete, rtpsPayload, userPayload []byte) {
	t.Helper()
	if !bytes.Equal(complete, rtpsPayload) {
		t.Fatalf("reassembled payload differs from sample")
	}
	if fragEdgeLen(reasm) != 0 {
		t.Fatalf("entries = %d want 0 after complete", fragEdgeLen(reasm))
	}
	recv := dds.NewDataReceiver()
	msg, err := recv.Take(complete)
	if err != nil {
		t.Fatalf("take reassembled: %v", err)
	}
	if msg.TopicName != "/chatter" {
		t.Fatalf("topic = %q want /chatter", msg.TopicName)
	}
	if msg.TypeName != "std_msgs__msg__String" {
		t.Fatalf("type = %q want std_msgs__msg__String", msg.TypeName)
	}
	if !bytes.Equal(msg.Payload, userPayload) {
		t.Fatalf("payload = %v want %v", msg.Payload, userPayload)
	}
	if recv.LostCount() != 0 {
		t.Fatalf("lost = %d want 0", recv.LostCount())
	}
	if recv.DroppedCount() != 0 {
		t.Fatalf("drops = %d want 0", recv.DroppedCount())
	}
}

// TestFragEdgeOutOfOrderReassemble checks reverse order fragments reassemble to one payload.
func TestFragEdgeOutOfOrderReassemble(t *testing.T) {
	t.Parallel()
	guid := dataPathWriterGUID()
	userPayload := []byte("hello fragmented weld edge pin out of order")
	rtpsPayload := dataPathBuildUserPayload(t, guid, userPayload)
	fragSize := uint16(40)
	seq := uint64(31)
	frags := fragEdgeSplit(t, rtpsPayload, seq, fragSize, fragEdgeUserWriter)
	reasm := fragEdgeNewReassembler(fragEdgeReassemblyTimeout, nil)
	complete := fragEdgeAddReverse(t, reasm, guid, frags)
	fragEdgeCheckReassembled(t, reasm, complete, rtpsPayload, userPayload)
}

// fragEdgeAddFirstDuplicate adds the first fragment twice and checks the count stays at one.
func fragEdgeAddFirstDuplicate(t *testing.T, reasm *fragEdgeReassembler, guid dds.WriterGUID, frags []rtps.DataFrag, seq uint64) {
	t.Helper()
	_, finished, err := fragEdgeAdd(reasm, guid, frags[0])
	if err != nil {
		t.Fatalf("add first: %v", err)
	}
	if finished {
		t.Fatalf("first frag finished, want incomplete")
	}
	before := fragEdgeReceivedCount(reasm, guid, seq)
	if before != 1 {
		t.Fatalf("received before duplicate = %d want 1", before)
	}
	_, finished, err = fragEdgeAdd(reasm, guid, frags[0])
	if err != nil {
		t.Fatalf("add duplicate: %v", err)
	}
	if finished {
		t.Fatalf("duplicate finished, want incomplete")
	}
	after := fragEdgeReceivedCount(reasm, guid, seq)
	if after != 1 {
		t.Fatalf("received after duplicate = %d want 1", after)
	}
}

// fragEdgeDrainRemaining adds remaining fragments and returns the completed payload.
func fragEdgeDrainRemaining(t *testing.T, reasm *fragEdgeReassembler, guid dds.WriterGUID, frags []rtps.DataFrag) []byte {
	t.Helper()
	var complete []byte
	done := false
	for i := 1; i < len(frags); i++ {
		out, fin, aerr := fragEdgeAdd(reasm, guid, frags[i])
		if aerr != nil {
			t.Fatalf("add frag %d: %v", frags[i].FragmentStartingNum, aerr)
		}
		if fin {
			complete = out
			done = true
		}
	}
	if !done {
		t.Fatalf("reassembly did not complete after duplicate")
	}
	return complete
}

// TestFragEdgeDuplicateIgnored checks a repeated fragment does not corrupt reassembly.
func TestFragEdgeDuplicateIgnored(t *testing.T) {
	t.Parallel()
	guid := dataPathWriterGUID()
	userPayload := []byte("duplicate fragment must be ignored edge pin")
	rtpsPayload := dataPathBuildUserPayload(t, guid, userPayload)
	fragSize := uint16(32)
	seq := uint64(32)
	frags := fragEdgeSplit(t, rtpsPayload, seq, fragSize, fragEdgeUserWriter)
	if len(frags) < 3 {
		t.Fatalf("frags = %d want at least 3", len(frags))
	}
	reasm := fragEdgeNewReassembler(fragEdgeReassemblyTimeout, nil)
	fragEdgeAddFirstDuplicate(t, reasm, guid, frags, seq)
	complete := fragEdgeDrainRemaining(t, reasm, guid, frags)
	if !bytes.Equal(complete, rtpsPayload) {
		t.Fatalf("reassembled payload differs after duplicate")
	}
	if fragEdgeLen(reasm) != 0 {
		t.Fatalf("entries = %d want 0 after complete", fragEdgeLen(reasm))
	}
}

// TestFragEdgeOversizeDropped checks sample above cap drops with sentinel plus counter.
func TestFragEdgeOversizeDropped(t *testing.T) {
	t.Parallel()
	if fragEdgeMaxPayloadLen != 32<<20 {
		t.Fatalf("max payload = %d want %d", fragEdgeMaxPayloadLen, 32<<20)
	}
	fragSize := uint16(100)
	sampleSize := uint32(fragEdgeMaxPayloadLen + 1)
	fragPayload := make([]byte, 100)
	body := fragEdgeBuildBody(t, rtps.EntityUnknown, fragEdgeUserWriter, 1, 1, 1, fragSize, sampleSize, fragPayload)
	var drops rtps.Counter
	frag, err := rtps.DecodeDataFrag(body, rtps.FlagLittleEndian, &drops)
	if err != nil {
		t.Fatalf("decode oversize geometry: %v", err)
	}
	if drops.Count() != 0 {
		t.Fatalf("decode drops = %d want 0", drops.Count())
	}
	dropCount := uint64(0)
	_, berr := fragEdgeCheckBounds(frag.SampleSize, frag.FragmentSize)
	if berr != nil {
		dropCount++
	}
	if berr == nil {
		t.Fatalf("oversize bounds succeeded, want ErrDataFragDropped")
	}
	if !errors.Is(berr, dds.ErrDataFragDropped) {
		t.Fatalf("oversize error = %v want ErrDataFragDropped", berr)
	}
	if dropCount != 1 {
		t.Fatalf("drop counter = %d want 1", dropCount)
	}
	reasm := fragEdgeNewReassembler(fragEdgeReassemblyTimeout, nil)
	var guid dds.WriterGUID
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	_, _, aerr := fragEdgeAdd(reasm, guid, frag)
	if aerr == nil {
		t.Fatalf("oversize add succeeded, want ErrDataFragDropped")
	}
	if !errors.Is(aerr, dds.ErrDataFragDropped) {
		t.Fatalf("oversize add error = %v want ErrDataFragDropped", aerr)
	}
	if fragEdgeLen(reasm) != 0 {
		t.Fatalf("entries = %d want 0 after oversize drop", fragEdgeLen(reasm))
	}
}

// TestFragEdgeUnknownWriterDropped checks builtin writer fragments drop without delivery.
func TestFragEdgeUnknownWriterDropped(t *testing.T) {
	t.Parallel()
	guid := dataPathWriterGUID()
	userPayload := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	sample := dataPathBuildUserPayload(t, guid, userPayload)
	sampleSize := uint32(len(sample)) //nolint:gosec // G115: test payload length is small and non-negative
	fragSize := uint16(sampleSize)    //nolint:gosec // G115: test fragment size is small, fits uint16
	dropCount := 0
	dataCh := make(chan dataPathItem, 4)
	builtinWriters := []rtps.EntityID{
		rtps.EntitySPDPWriter,
		{0x00, 0x00, 0x02, 0xC2},
		{0x00, 0x00, 0x03, 0xC2},
		{0x00, 0x00, 0x04, 0xC2},
	}
	for _, w := range builtinWriters {
		body := fragEdgeBuildBody(t, rtps.EntityUnknown, w, 1, 1, 1, fragSize, sampleSize, sample)
		decoded, err := rtps.DecodeDataFrag(body, rtps.FlagLittleEndian, &rtps.Counter{})
		if err != nil {
			t.Fatalf("decode builtin frag %v: %v", w, err)
		}
		if fragEdgeIsUserWriter(decoded.WriterID) {
			t.Fatalf("builtin writer %v passed user filter, want drop", w)
		}
		dropCount++
	}
	if len(dataCh) != 0 {
		t.Fatalf("dataCh length = %d want 0 after builtin drops", len(dataCh))
	}
	if dropCount != len(builtinWriters) {
		t.Fatalf("drop counter = %d want %d", dropCount, len(builtinWriters))
	}
	if !fragEdgeIsUserWriter(fragEdgeUserWriter) {
		t.Fatalf("user writer failed filter, want pass")
	}
	enc := types.NewEncoder(true)
	werr := enc.WriteString("stray")
	if werr != nil {
		t.Fatalf("encode stray string: %v", werr)
	}
	wire := enc.Bytes()
	src := &dataPathSliceFrameSource{}
	var unknown dds.WriterGUID
	unknown[0] = 0xFF
	src.queue(unknown, wire)
	recv := dds.NewDataReceiver()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, terr := recv.TakeFrom(ctx, src)
	if terr == nil {
		t.Fatalf("unknown writer take succeeded, want error")
	}
	if recv.DroppedCount() != 1 {
		t.Fatalf("unknown writer drops = %d want 1", recv.DroppedCount())
	}
}

// fragEdgeSetupPartial adds the first fragment and checks one entry is pending.
func fragEdgeSetupPartial(t *testing.T, reasm *fragEdgeReassembler, guid dds.WriterGUID, frags []rtps.DataFrag) {
	t.Helper()
	_, finished, err := fragEdgeAdd(reasm, guid, frags[0])
	if err != nil {
		t.Fatalf("add first: %v", err)
	}
	if finished {
		t.Fatalf("first frag finished, want incomplete")
	}
	if fragEdgeLen(reasm) != 1 {
		t.Fatalf("entries = %d want 1 before sweep", fragEdgeLen(reasm))
	}
}

// fragEdgePollOneDrop polls the sweep and checks exactly one entry expires.
func fragEdgePollOneDrop(t *testing.T, ctx context.Context, reasm *fragEdgeReassembler) {
	t.Helper()
	dropped, perr := fragEdgePollSweep(ctx, reasm)
	if perr != nil {
		t.Fatalf("poll sweep: %v", perr)
	}
	if dropped != 1 {
		t.Fatalf("sweep dropped = %d want 1", dropped)
	}
	if fragEdgeLen(reasm) != 0 {
		t.Fatalf("entries = %d want 0 after sweep", fragEdgeLen(reasm))
	}
}

// fragEdgeAddFreshPartial adds two fragments after a sweep and checks a fresh partial remains.
func fragEdgeAddFreshPartial(t *testing.T, reasm *fragEdgeReassembler, guid dds.WriterGUID, frags []rtps.DataFrag) {
	t.Helper()
	_, finished, err := fragEdgeAdd(reasm, guid, frags[1])
	if err != nil {
		t.Fatalf("add second after sweep: %v", err)
	}
	if finished {
		t.Fatalf("second frag after sweep finished, want incomplete")
	}
	_, finished, err = fragEdgeAdd(reasm, guid, frags[2])
	if err != nil {
		t.Fatalf("add third after sweep: %v", err)
	}
	if finished {
		t.Fatalf("third frag without first finished, want incomplete")
	}
	if fragEdgeLen(reasm) != 1 {
		t.Fatalf("entries = %d want 1 for fresh partial", fragEdgeLen(reasm))
	}
}

// TestFragEdgeSweepTimeout checks incomplete entries expire by bounded poll without blocking.
func TestFragEdgeSweepTimeout(t *testing.T) {
	t.Parallel()
	current := time.Now()
	nowFn := func() time.Time {
		return current
	}
	timeout := 30 * time.Millisecond
	reasm := fragEdgeNewReassembler(timeout, nowFn)
	guid := dataPathWriterGUID()
	userPayload := []byte("sweep timeout must drop incomplete edge pin with bounded poll")
	rtpsPayload := dataPathBuildUserPayload(t, guid, userPayload)
	fragSize := uint16(32)
	seq := uint64(33)
	frags := fragEdgeSplit(t, rtpsPayload, seq, fragSize, fragEdgeUserWriter)
	if len(frags) < 3 {
		t.Fatalf("frags = %d want at least 3", len(frags))
	}
	fragEdgeSetupPartial(t, reasm, guid, frags)
	current = current.Add(timeout + 5*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fragEdgePollOneDrop(t, ctx, reasm)
	fragEdgeAddFreshPartial(t, reasm, guid, frags)
}
