// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package unit_datapath holds DATA_FRAG plus GAP plus ACKNACK conformance pins.
package unit_datapath

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// fragTestMaxPayloadLen mirrors maxPayloadLen in datapath.go.
const fragTestMaxPayloadLen = 32 << 20

// fragTestHistoryDepth mirrors userHistoryDepth in reliability.go.
const fragTestHistoryDepth = 64

// fragTestRepairWindowSize mirrors userRepairWindow in reliability.go.
const fragTestRepairWindowSize = 64

// fragTestUserWriter is the user DATA writer entity 00 00 02 03.
var fragTestUserWriter = rtps.EntityID{0x00, 0x00, 0x02, 0x03}

// fragTestBuildBody builds one DATA_FRAG body in little endian order.
func fragTestBuildBody(t *testing.T, reader, writer rtps.EntityID, seq uint64, start uint32, count, fragSize uint16, sampleSize uint32, fragPayload []byte) []byte {
	t.Helper()
	out := make([]byte, 0, 32+len(fragPayload))
	out = append(out, 0x00, 0x00)
	out = append(out, 0x10, 0x00)
	out = append(out, reader[:]...)
	out = append(out, writer[:]...)
	var seqBuf [8]byte
	binary.LittleEndian.PutUint32(seqBuf[0:4], uint32(seq>>32))
	binary.LittleEndian.PutUint32(seqBuf[4:8], uint32(seq)) //nolint:gosec // G115: test sequence numbers are small, fit uint32
	out = append(out, seqBuf[:]...)
	var u32 [4]byte
	binary.LittleEndian.PutUint32(u32[:], start)
	out = append(out, u32[:]...)
	var u16 [2]byte
	binary.LittleEndian.PutUint16(u16[:], count)
	out = append(out, u16[:]...)
	binary.LittleEndian.PutUint16(u16[:], fragSize)
	out = append(out, u16[:]...)
	binary.LittleEndian.PutUint32(u32[:], sampleSize)
	out = append(out, u32[:]...)
	out = append(out, fragPayload...)
	return out
}

// fragTestCheckBounds mirrors checkFragBounds in frag.go for pinning.
func fragTestCheckBounds(sampleSize uint32, fragmentSize uint16) (uint32, error) {
	if fragmentSize == 0 {
		return 0, errors.Join(errors.New("frag test: fragment size 0"), dds.ErrDataFragDropped) //nolint:wrapcheck // test sentinel combination, callers compare with errors.Is
	}
	if uint64(sampleSize) > uint64(fragTestMaxPayloadLen) {
		return 0, errors.Join(errors.New("frag test: sample above cap"), dds.ErrDataFragDropped) //nolint:wrapcheck // test sentinel combination, callers compare with errors.Is
	}
	total64 := (uint64(sampleSize) + uint64(fragmentSize) - 1) / uint64(fragmentSize)
	if total64 == 0 || total64 > 131072 {
		return 0, errors.Join(errors.New("frag test: total out of range"), dds.ErrDataFragDropped) //nolint:wrapcheck // test sentinel combination, callers compare with errors.Is
	}
	return uint32(total64), nil
}

// fragTestIsUserWriter mirrors isUserWriterEntity plus sedpBuiltinWriter.
func fragTestIsUserWriter(id rtps.EntityID) bool {
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

// fragTestEncodeGapBody renders one GAP body with gapStart and next base.
func fragTestEncodeGapBody(reader, writer rtps.EntityID, gapStart uint64) []byte {
	out := make([]byte, 0, 28)
	out = append(out, reader[:]...)
	out = append(out, writer[:]...)
	var start [8]byte
	binary.LittleEndian.PutUint32(start[0:4], uint32(gapStart>>32))
	binary.LittleEndian.PutUint32(start[4:8], uint32(gapStart)) //nolint:gosec // G115: test sequence numbers are small, fit uint32
	out = append(out, start[:]...)
	next := gapStart + 1
	if next == 0 {
		next = 1
	}
	var base [8]byte
	binary.LittleEndian.PutUint32(base[0:4], uint32(next>>32))
	binary.LittleEndian.PutUint32(base[4:8], uint32(next))
	out = append(out, base[:]...)
	var bits [4]byte
	binary.LittleEndian.PutUint32(bits[:], 0)
	out = append(out, bits[:]...)
	return out
}

// fragTestHistory stores sent payloads with depth 64 for repair pins.
type fragTestHistory struct {
	payloads map[dds.WriterGUID]map[uint64][]byte
	order    map[dds.WriterGUID][]uint64
}

// fragTestNewHistory returns an empty test history.
func fragTestNewHistory() *fragTestHistory {
	return &fragTestHistory{
		payloads: make(map[dds.WriterGUID]map[uint64][]byte),
		order:    make(map[dds.WriterGUID][]uint64),
	}
}

// fragTestStore copies payload for guid and seq with depth eviction.
func fragTestStore(h *fragTestHistory, guid dds.WriterGUID, seq uint64, payload []byte) {
	cp := append([]byte(nil), payload...)
	bySeq, seen := h.payloads[guid]
	if !seen {
		bySeq = make(map[uint64][]byte)
		h.payloads[guid] = bySeq
	}
	_, dup := bySeq[seq]
	if !dup {
		h.order[guid] = append(h.order[guid], seq)
	}
	bySeq[seq] = cp
	for len(h.order[guid]) > fragTestHistoryDepth {
		old := h.order[guid][0]
		h.order[guid] = h.order[guid][1:]
		delete(bySeq, old)
	}
}

// fragTestLoad returns a copy for guid and seq.
//
//nolint:unparam // test helper keeps payload return for symmetry with store
func fragTestLoad(h *fragTestHistory, guid dds.WriterGUID, seq uint64) ([]byte, bool) {
	bySeq, seen := h.payloads[guid]
	if !seen {
		return nil, false
	}
	raw, ok := bySeq[seq]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), raw...), true
}

// fragTestFirst returns the first kept seq or 0.
func fragTestFirst(h *fragTestHistory, guid dds.WriterGUID) uint64 {
	ord := h.order[guid]
	if len(ord) == 0 {
		return 0
	}
	return ord[0]
}

// fragTestLast returns the last kept seq or 0.
func fragTestLast(h *fragTestHistory, guid dds.WriterGUID) uint64 {
	ord := h.order[guid]
	if len(ord) == 0 {
		return 0
	}
	return ord[len(ord)-1]
}

// fragTestRepairRange mirrors Backend.repairWindow with window 64.
func fragTestRepairRange(h *fragTestHistory, guid dds.WriterGUID, base uint64) (uint64, uint64, uint64, uint64, bool) {
	first := fragTestFirst(h, guid)
	last := fragTestLast(h, guid)
	if last == 0 {
		return 0, 0, 0, 0, false
	}
	if base > last {
		return 0, 0, 0, 0, false
	}
	start := max(base, first)
	end := start + fragTestRepairWindowSize - 1
	if end > last || end < start {
		end = last
	}
	return start, first, last, end, true
}

// TestFragReassembleString checks fragmented String reassembles to one payload.
//
//nolint:gocognit,cyclop // reassembly test sequences build, decode, and verify in one run
func TestFragReassembleString(t *testing.T) {
	t.Parallel()
	guid := dataPathWriterGUID()
	userPayload := []byte("hello fragmented weld String pin")
	rtpsPayload := dataPathBuildUserPayload(t, guid, userPayload)
	sampleLen := len(rtpsPayload)
	if sampleLen == 0 {
		t.Fatalf("sample is empty")
	}
	sampleSize := uint32(sampleLen) //nolint:gosec // G115: test payload length is small and non-negative
	fragSize := uint16(40)
	total64 := (uint64(sampleSize) + uint64(fragSize) - 1) / uint64(fragSize)
	if total64 < 2 {
		t.Fatalf("sample %d with frag %d needs one fragment, want at least 2", sampleSize, fragSize)
	}
	total, err := fragTestCheckBounds(sampleSize, fragSize)
	if err != nil {
		t.Fatalf("bounds: %v", err)
	}
	if uint64(total) != total64 {
		t.Fatalf("total = %d want %d", total, total64)
	}
	outerSeq := uint64(9)
	type fragPiece struct {
		num  uint32
		body []byte
	}
	pieces := make([]fragPiece, 0, total)
	for n := uint32(1); n <= total; n++ {
		off := int(n-1) * int(fragSize)
		end := off + int(fragSize)
		end = min(end, len(rtpsPayload))
		slice := rtpsPayload[off:end]
		body := fragTestBuildBody(t, rtps.EntityUnknown, fragTestUserWriter, outerSeq, n, 1, fragSize, sampleSize, slice)
		var drops rtps.Counter
		decoded, derr := rtps.DecodeDataFrag(body, rtps.FlagLittleEndian, &drops)
		if derr != nil {
			t.Fatalf("decode frag %d: %v", n, derr)
		}
		if decoded.SequenceNumber != outerSeq {
			t.Fatalf("frag %d seq = %d want %d", n, decoded.SequenceNumber, outerSeq)
		}
		if decoded.FragmentStartingNum != n {
			t.Fatalf("frag %d start = %d want %d", n, decoded.FragmentStartingNum, n)
		}
		if decoded.SampleSize != sampleSize {
			t.Fatalf("frag %d sample = %d want %d", n, decoded.SampleSize, sampleSize)
		}
		if drops.Count() != 0 {
			t.Fatalf("frag %d drops = %d want 0", n, drops.Count())
		}
		pieces = append(pieces, fragPiece{num: n, body: body})
	}
	buf := make([]byte, sampleSize)
	received := make([]bool, total)
	for _, piece := range slices.Backward(pieces) {
		var drops rtps.Counter
		decoded, derr := rtps.DecodeDataFrag(piece.body, rtps.FlagLittleEndian, &drops)
		if derr != nil {
			t.Fatalf("re-decode frag %d: %v", piece.num, derr)
		}
		fragNum := uint64(decoded.FragmentStartingNum)
		idx := fragNum - 1
		if received[idx] {
			t.Fatalf("duplicate frag %d", fragNum)
		}
		off := (fragNum - 1) * uint64(fragSize)
		copy(buf[off:off+uint64(len(decoded.Payload))], decoded.Payload)
		received[idx] = true
	}
	for i, seen := range received {
		if !seen {
			t.Fatalf("frag %d missing after reassembly", i+1)
		}
	}
	if !bytes.Equal(buf, rtpsPayload) {
		t.Fatalf("reassembled payload differs from sample")
	}
	dataCh := make(chan dataPathItem, 4)
	select {
	case dataCh <- dataPathItem{guid: guid, payload: append([]byte(nil), buf...)}:
	default:
		t.Fatalf("dataCh blocked, no loss allowed")
	}
	if len(dataCh) != 1 {
		t.Fatalf("dataCh length = %d want 1", len(dataCh))
	}
	item := <-dataCh
	recv := dds.NewDataReceiver()
	msg, terr := recv.Take(item.payload)
	if terr != nil {
		t.Fatalf("take reassembled: %v", terr)
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

// TestFragOversizeDropped checks sample above cap drops with sentinel.
func TestFragOversizeDropped(t *testing.T) {
	t.Parallel()
	if fragTestMaxPayloadLen != 32<<20 {
		t.Fatalf("max payload = %d want %d", fragTestMaxPayloadLen, 32<<20)
	}
	fragSize := uint16(100)
	sampleSize := uint32(fragTestMaxPayloadLen + 1)
	fragPayload := make([]byte, 100)
	body := fragTestBuildBody(t, rtps.EntityUnknown, fragTestUserWriter, 1, 1, 1, fragSize, sampleSize, fragPayload)
	var drops rtps.Counter
	frag, err := rtps.DecodeDataFrag(body, rtps.FlagLittleEndian, &drops)
	if err != nil {
		t.Fatalf("decode oversize geometry: %v", err)
	}
	if drops.Count() != 0 {
		t.Fatalf("decode drops = %d want 0", drops.Count())
	}
	_, berr := fragTestCheckBounds(frag.SampleSize, frag.FragmentSize)
	if berr == nil {
		t.Fatalf("oversize bounds succeeded, want ErrDataFragDropped")
	}
	if !errors.Is(berr, dds.ErrDataFragDropped) {
		t.Fatalf("oversize error = %v want ErrDataFragDropped", berr)
	}
	dropCount := 0
	err = berr
	if err != nil {
		dropCount++
	}
	if dropCount != 1 {
		t.Fatalf("drop counter = %d want 1", dropCount)
	}
	_, zerr := fragTestCheckBounds(100, 0)
	if zerr == nil {
		t.Fatalf("zero frag size succeeded, want ErrDataFragDropped")
	}
	if !errors.Is(zerr, dds.ErrDataFragDropped) {
		t.Fatalf("zero frag error = %v want ErrDataFragDropped", zerr)
	}
}

// TestFragUnknownWriterDropped checks builtin writer fragments drop.
func TestFragUnknownWriterDropped(t *testing.T) {
	t.Parallel()
	userPayload := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	guid := dataPathWriterGUID()
	sample := dataPathBuildUserPayload(t, guid, userPayload)
	sampleSize := uint32(len(sample)) //nolint:gosec // G115: test payload length is small and non-negative
	fragSize := uint16(sampleSize)    //nolint:gosec // G115: test fragment size is small, fits uint16
	body := fragTestBuildBody(t, rtps.EntityUnknown, rtps.EntitySPDPWriter, 1, 1, 1, fragSize, sampleSize, sample)
	var drops rtps.Counter
	frag, err := rtps.DecodeDataFrag(body, rtps.FlagLittleEndian, &drops)
	if err != nil {
		t.Fatalf("decode builtin frag: %v", err)
	}
	if fragTestIsUserWriter(frag.WriterID) {
		t.Fatalf("SPDP writer passed user filter, want drop")
	}
	sedpWriters := []rtps.EntityID{
		{0x00, 0x00, 0x02, 0xC2},
		{0x00, 0x00, 0x03, 0xC2},
		{0x00, 0x00, 0x04, 0xC2},
	}
	for _, w := range sedpWriters {
		if fragTestIsUserWriter(w) {
			t.Fatalf("SEDP writer %v passed user filter, want drop", w)
		}
	}
	if !fragTestIsUserWriter(fragTestUserWriter) {
		t.Fatalf("user writer failed filter, want pass")
	}
	dataCh := make(chan dataPathItem, 4)
	if !fragTestIsUserWriter(frag.WriterID) {
		if len(dataCh) != 0 {
			t.Fatalf("dataCh length = %d want 0 after builtin drop", len(dataCh))
		}
	}
	enc := types.NewEncoder(true)
	encErr := enc.WriteString("stray")
	if encErr != nil {
		t.Fatalf("encode stray string: %v", encErr)
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

// TestGapSkipWithoutLost checks GAP advances without lost count.
//
//nolint:gocognit,cyclop // gap test sequences publish, take, and sweep in one run
func TestGapSkipWithoutLost(t *testing.T) {
	t.Parallel()
	guid := dataPathWriterGUID()
	sink := &dataPathRecordingWriter{}
	pub := helpers.MustNewPublisher(t, guid, sink)
	payloads := [][]byte{{10, 10, 10, 10}, {20, 20, 20, 20}, {30, 30, 30, 30}}
	for _, p := range payloads {
		perr := pub.Publish(transports.OutgoingMessage{TopicName: "/chatter", TypeName: "std_msgs__msg__String", Payload: p})
		if perr != nil {
			t.Fatalf("publish: %v", perr)
		}
	}
	if sink.count() != 3 {
		t.Fatalf("frames = %d want 3", sink.count())
	}
	first := sink.at(t, 0)
	third := sink.at(t, 2)
	recv := dds.NewDataReceiver()
	_, err := recv.Take(first)
	if err != nil {
		t.Fatalf("take first: %v", err)
	}
	if recv.LostCount() != 0 {
		t.Fatalf("lost after first = %d want 0", recv.LostCount())
	}
	gapBody := fragTestEncodeGapBody(rtps.EntityUnknown, fragTestUserWriter, 2)
	var drops rtps.Counter
	gap, gerr := rtps.DecodeGap(gapBody, rtps.FlagLittleEndian, &drops)
	if gerr != nil {
		t.Fatalf("decode gap: %v", gerr)
	}
	if gap.GapStart != 2 {
		t.Fatalf("gap start = %d want 2", gap.GapStart)
	}
	if drops.Count() != 0 {
		t.Fatalf("gap drops = %d want 0", drops.Count())
	}
	recv.ApplyGap(guid, gap.GapStart)
	msg, terr := recv.Take(third)
	if terr != nil {
		t.Fatalf("take after gap: %v", terr)
	}
	if msg.Payload[0] != 30 {
		t.Fatalf("gap payload first byte = %d want 30", msg.Payload[0])
	}
	if recv.LostCount() != 0 {
		t.Fatalf("lost after gap skip = %d want 0", recv.LostCount())
	}
	if recv.DroppedCount() != 0 {
		t.Fatalf("drops after gap skip = %d want 0", recv.DroppedCount())
	}
	recv.ApplyGap(guid, 2)
	if recv.LostCount() != 0 {
		t.Fatalf("lost after duplicate gap = %d want 0", recv.LostCount())
	}
	recv.ApplyGap(guid, 1)
	if recv.LostCount() != 0 {
		t.Fatalf("lost after old gap = %d want 0", recv.LostCount())
	}
	recv2 := dds.NewDataReceiver()
	_, err = recv2.Take(first)
	if err != nil {
		t.Fatalf("control take first: %v", err)
	}
	_, err = recv2.Take(third)
	if err != nil {
		t.Fatalf("control take third: %v", err)
	}
	if recv2.LostCount() != 1 {
		t.Fatalf("control lost without gap = %d want 1", recv2.LostCount())
	}
}

// TestAckRepairWindow checks history depth 64 plus window 64 repair choice.
//
//nolint:gocognit,cyclop // repair test sequences history, window, and ACK checks in one run
func TestAckRepairWindow(t *testing.T) {
	t.Parallel()
	if fragTestHistoryDepth != 64 {
		t.Fatalf("history depth = %d want 64", fragTestHistoryDepth)
	}
	if fragTestRepairWindowSize != 64 {
		t.Fatalf("repair window = %d want 64", fragTestRepairWindowSize)
	}
	var guid dds.WriterGUID
	for i := range guid {
		guid[i] = byte(i + 1)
	}
	hist := fragTestNewHistory()
	for seq := uint64(1); seq <= 70; seq++ {
		payload := []byte{byte(seq)}
		fragTestStore(hist, guid, seq, payload)
	}
	if len(hist.order[guid]) != 64 {
		t.Fatalf("history length = %d want 64", len(hist.order[guid]))
	}
	first := fragTestFirst(hist, guid)
	if first != 7 {
		t.Fatalf("history first = %d want 7", first)
	}
	last := fragTestLast(hist, guid)
	if last != 70 {
		t.Fatalf("history last = %d want 70", last)
	}
	start, rfirst, rlast, end, ok := fragTestRepairRange(hist, guid, 5)
	if !ok {
		t.Fatalf("repair window base 5 not ok, want ok")
	}
	if rfirst != 7 || rlast != 70 {
		t.Fatalf("repair first/last = %d/%d want 7/70", rfirst, rlast)
	}
	if start != 7 || end != 70 {
		t.Fatalf("repair start/end = %d/%d want 7/70", start, end)
	}
	_, _, _, _, ok = fragTestRepairRange(hist, guid, 71)
	if ok {
		t.Fatalf("repair window base 71 ok, want not ok")
	}
	_, _, _, endSingle, ok := fragTestRepairRange(hist, guid, 70)
	if !ok {
		t.Fatalf("repair window base 70 not ok, want ok")
	}
	if endSingle != 70 {
		t.Fatalf("repair single end = %d want 70", endSingle)
	}
	sparse := fragTestNewHistory()
	for seq := uint64(2); seq <= 64; seq++ {
		fragTestStore(sparse, guid, seq, []byte{byte(seq)})
	}
	fragTestStore(sparse, guid, 200, []byte{0xC8})
	sfirst := fragTestFirst(sparse, guid)
	if sfirst != 2 {
		t.Fatalf("sparse first = %d want 2", sfirst)
	}
	slast := fragTestLast(sparse, guid)
	if slast != 200 {
		t.Fatalf("sparse last = %d want 200", slast)
	}
	sstart, _, _, send, ok := fragTestRepairRange(sparse, guid, 2)
	if !ok {
		t.Fatalf("sparse window not ok, want ok")
	}
	if sstart != 2 {
		t.Fatalf("sparse start = %d want 2", sstart)
	}
	if send != 65 {
		t.Fatalf("sparse end = %d want 65", send)
	}
	_, present := fragTestLoad(sparse, guid, 2)
	if !present {
		t.Fatalf("seq 2 missing, want repair")
	}
	_, missing := fragTestLoad(sparse, guid, 65)
	if missing {
		t.Fatalf("seq 65 present, want gap")
	}
	receipt := rtps.AckNack{
		ReaderID: rtps.EntityUnknown,
		WriterID: fragTestUserWriter,
		Base:     7,
		NumBits:  8,
		Bitmap:   []uint32{0x00000001},
		Count:    1,
	}
	ackBody, aerr := receipt.Encode(true)
	if aerr != nil {
		t.Fatalf("ack encode: %v", aerr)
	}
	if len(ackBody) != 28 {
		t.Fatalf("ack body length = %d want 28", len(ackBody))
	}
	baseHigh := binary.LittleEndian.Uint32(ackBody[8:12])
	baseLow := binary.LittleEndian.Uint32(ackBody[12:16])
	base := uint64(baseHigh)<<32 | uint64(baseLow)
	if base != 7 {
		t.Fatalf("ack base = %d want 7", base)
	}
	numBits := binary.LittleEndian.Uint32(ackBody[16:20])
	if numBits != 8 {
		t.Fatalf("ack bits = %d want 8", numBits)
	}
	if fragTestIsUserWriter(rtps.EntitySPDPWriter) {
		t.Fatalf("SPDP ack writer passed filter, want ignore")
	}
	if !fragTestIsUserWriter(receipt.WriterID) {
		t.Fatalf("user ack writer failed filter, want repair path")
	}
}
