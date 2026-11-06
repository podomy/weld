// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// testNackGUID returns a distinct writer GUID for NACK tests.
func testNackGUID(prefix byte) WriterGUID {
	var guid WriterGUID
	for i := range guid {
		guid[i] = prefix + byte(i)
	}

	return guid
}

// TestSedpBaseNoStateFullRange checks first heartbeat with no local state
// NACKs the full advertised range starting at first.
func TestSedpBaseNoStateFullRange(t *testing.T) {
	t.Parallel()

	backend := &Backend{}
	guid := testNackGUID(0xA0)

	if got := backend.sedpBase(guid, 5); got != 5 {
		t.Fatalf("base = %d, want 5 for unseen first 5", got)
	}

	if got := backend.sedpBase(guid, 0); got != FirstSequenceNumber {
		t.Fatalf("base = %d, want %d for unseen first 0", got, FirstSequenceNumber)
	}
}

// TestSedpBaseSeenClampsToFirst checks a heartbeat first beyond our high
// clamps up so we never NACK seq the writer already skipped.
func TestSedpBaseSeenClampsToFirst(t *testing.T) {
	t.Parallel()

	backend := &Backend{}
	guid := testNackGUID(0xB0)

	backend.noteSedpSeq(guid, 3)

	if got := backend.sedpBase(guid, 10); got != 10 {
		t.Fatalf("base = %d, want 10 when first advances past high", got)
	}

	if got := backend.sedpBase(guid, 1); got != 4 {
		t.Fatalf("base = %d, want 4 for normal advance", got)
	}

	if got := backend.sedpBase(guid, 0); got != 4 {
		t.Fatalf("base = %d, want 4 when first is 0 and seen", got)
	}
}

// TestSedpBaseOverflowWraps checks max high wraps to first sequence.
func TestSedpBaseOverflowWraps(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		sedpHighSeq: map[WriterGUID]uint64{testNackGUID(0xC0): ^uint64(0)},
	}
	guid := testNackGUID(0xC0)

	if got := backend.sedpBase(guid, 7); got != FirstSequenceNumber {
		t.Fatalf("base = %d, want %d on overflow", got, FirstSequenceNumber)
	}
}

// requireSedpBase fails when the SEDP base differs from want.
func requireSedpBase(t *testing.T, backend *Backend, guid WriterGUID, want uint64, format string) {
	t.Helper()
	if got := backend.sedpBase(guid, 1); got != want {
		t.Fatalf(format, got)
	}
}

// TestNoteSedpSeqIdempotent checks duplicate and reordered DATA keep the
// same high so the next heartbeat re-NACKs the same range.
func TestNoteSedpSeqIdempotent(t *testing.T) {
	t.Parallel()

	backend := &Backend{}
	guid := testNackGUID(0xD0)

	backend.noteSedpSeq(guid, 0)

	requireSedpBase(t, backend, guid, 1, "base = %d, want 1 after zero ignored")

	backend.noteSedpSeq(guid, 5)

	requireSedpBase(t, backend, guid, 6, "base = %d, want 6 after seq 5")

	backend.noteSedpSeq(guid, 5)

	requireSedpBase(t, backend, guid, 6, "base = %d, want 6 after duplicate 5")

	backend.noteSedpSeq(guid, 3)

	requireSedpBase(t, backend, guid, 6, "base = %d, want 6 after reordered 3")
}

// TestApplySedpGapAdvances checks GAP advances tracking and stays monotonic.
func TestApplySedpGapAdvances(t *testing.T) {
	t.Parallel()

	backend := &Backend{}
	guid := testNackGUID(0xE0)

	backend.applySedpGap(guid, 0)

	requireSedpBase(t, backend, guid, 1, "base = %d, want 1 after zero gap")

	backend.applySedpGap(guid, 4)

	requireSedpBase(t, backend, guid, 5, "base = %d, want 5 after gap 4")

	backend.applySedpGap(guid, 4)

	requireSedpBase(t, backend, guid, 5, "base = %d, want 5 after duplicate gap")

	backend.applySedpGap(guid, 2)

	requireSedpBase(t, backend, guid, 5, "base = %d, want 5 after old gap")
}

// TestPlanSedpNackRanges checks pure versus NACK planning and bounded
// re-NACK on repeated heartbeats with the same range.
//
//nolint:cyclop // test pins pure, single, and huge NACK range breadth in one run, splitting would obscure intent
func TestPlanSedpNackRanges(t *testing.T) {
	t.Parallel()

	if _, _, need := planSedpNack(0, 5); need {
		t.Fatalf("need = true for base 0, want pure ACK")
	}

	if _, _, need := planSedpNack(6, 5); need {
		t.Fatalf("need = true for base past last, want pure ACK")
	}

	bitmap, numBits, need := planSedpNack(1, 5)
	if !need {
		t.Fatalf("need = false for base 1 last 5, want NACK")
	}

	if numBits != 5 {
		t.Fatalf("numBits = %d, want 5", numBits)
	}

	if len(bitmap) != 1 {
		t.Fatalf("bitmap words = %d, want 1", len(bitmap))
	}

	again, againBits, againNeed := planSedpNack(1, 5)
	if !againNeed || againBits != numBits || len(again) != len(bitmap) || again[0] != bitmap[0] {
		t.Fatalf("repeat NACK differs, want identical bounded re-NACK")
	}

	_, singleBits, singleNeed := planSedpNack(9, 9)
	if !singleNeed || singleBits != 1 {
		t.Fatalf("single need = %v bits = %d, want true 1", singleNeed, singleBits)
	}

	huge, hugeBits, hugeNeed := planSedpNack(1, uint64(1)<<40)
	if !hugeNeed {
		t.Fatalf("need = false for huge range, want NACK")
	}

	if hugeBits != userAckBitsMax {
		t.Fatalf("huge bits = %d, want cap %d", hugeBits, userAckBitsMax)
	}

	if uint64(len(huge)*32) < uint64(hugeBits) {
		t.Fatalf("huge bitmap too short for %d bits", hugeBits)
	}
}

// TestPlanSedpNackFinalDiscipline checks pure ACKs carry Final and NACKs do
// not, so repair keeps flowing without storms.
func TestPlanSedpNackFinalDiscipline(t *testing.T) {
	t.Parallel()

	local, err := NewLocalParticipant(0, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local participant: %v", err)
	}

	backend := &Backend{local: local}
	remote := testNackGUID(0xF0)
	reader := rtps.EntityID{0x00, 0x00, 0x02, 0xC7}

	_, _, need := planSedpNack(6, 5)
	if need {
		t.Fatalf("need = true for caught up, want pure")
	}

	pure, err := backend.buildAckSubmessage(remote, reader, 6, 0, nil, true)
	if err != nil {
		t.Fatalf("build pure ack: %v", err)
	}

	if pure.Flags&rtps.AckNackFlagFinal == 0 {
		t.Fatalf("pure ack flags %#02x missing Final", pure.Flags)
	}

	bitmap, numBits, need := planSedpNack(1, 4)
	if !need {
		t.Fatalf("need = false for missing range, want NACK")
	}

	nack, err := backend.buildAckSubmessage(remote, reader, 1, numBits, bitmap, false)
	if err != nil {
		t.Fatalf("build nack: %v", err)
	}

	if nack.Flags&rtps.AckNackFlagFinal != 0 {
		t.Fatalf("nack flags %#02x has Final, want without", nack.Flags)
	}
}

// TestSedpDuplicateRetransmitIdempotent checks duplicate endpoint DATA
// installs idempotently through UpsertRemote without bumping generation.
func TestSedpDuplicateRetransmitIdempotent(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0x51)
	peer := mustSEDPIndexWithPrefix(t, 0x61)

	writer := mustSEDPWriter(t, peer, "/chatter", "RIHS01_abc")

	err := local.UpsertRemote(writer)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	if got := local.Generation(); got != 1 {
		t.Fatalf("generation = %d, want 1", got)
	}

	err = local.UpsertRemote(writer)
	if err != nil {
		t.Fatalf("duplicate upsert: %v", err)
	}

	if got := local.Generation(); got != 1 {
		t.Fatalf("generation after duplicate = %d, want 1", got)
	}
}

// TestRouteBuiltinDiscoverySedpGapAdvances checks batched SEDP GAP plus
// DATA advances SEDP tracking while DATA still enqueues to SEDP.
func TestRouteBuiltinDiscoverySedpGapAdvances(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		routes: make(map[GUIDPrefix][]int),
		spdpCh: make(chan []byte, demuxQueueDepth),
		sedpCh: make(chan []byte, demuxQueueDepth),
		dataCh: make(chan dataItem, demuxQueueDepth),
	}

	var prefix [12]byte
	for i := range prefix {
		prefix[i] = byte(0x70 + i)
	}

	header := rtps.DefaultHeader(prefix)
	writer := rtps.EntityID{0x00, 0x00, sedpWriterEntityB14, 0xC2}

	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       writer,
		SequenceNumber: 1,
		Payload:        []byte{0x09, 0x08},
	}

	rawData, err := data.Submessage(true)
	if err != nil {
		t.Fatalf("data submessage: %v", err)
	}

	gapBody := encodeUserGapBody(rtps.EntityUnknown, writer, 4)
	gapRaw := rtps.RawSubmessage{
		ID:    rtps.SubmessageGap,
		Flags: rtps.FlagLittleEndian,
		Body:  gapBody,
	}

	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{rawData, gapRaw})
	if err != nil {
		t.Fatalf("encode batched: %v", err)
	}

	decodedHeader, subs, err := rtps.DecodeMessage(msg, nil)
	if err != nil {
		t.Fatalf("decode batched: %v", err)
	}

	if done := backend.routeBuiltinDiscovery(t.Context(), decodedHeader, msg, subs, backend.spdpCh, backend.sedpCh); done {
		t.Fatalf("route cancelled, want progress")
	}

	if len(backend.sedpCh) != 1 {
		t.Fatalf("sedp queue = %d, want 1", len(backend.sedpCh))
	}

	if len(backend.dataCh) != 0 {
		t.Fatalf("data queue = %d, want 0", len(backend.dataCh))
	}

	var guid WriterGUID
	copy(guid[:12], prefix[:])
	copy(guid[12:16], writer[:])

	if got := backend.sedpBase(guid, 1); got != 5 {
		t.Fatalf("base = %d, want 5 after gap 4", got)
	}

	if got := backend.demuxErrorCount(); got != 0 {
		t.Fatalf("demux errors = %d, want 0", got)
	}
}

// TestUserHistoryDepthPinsFleetChurn pins writer retention for fleet churn.
func TestUserHistoryDepthPinsFleetChurn(t *testing.T) {
	t.Parallel()

	if userHistoryDepth != 100 {
		t.Fatalf("history depth = %d, want 100 for fleet churn", userHistoryDepth)
	}

	if userRepairWindow != 64 {
		t.Fatalf("repair window = %d, want 64 bounded work", userRepairWindow)
	}

	if userPromptNackMaxAttempts != 3 {
		t.Fatalf("prompt attempts = %d, want 3 no storm", userPromptNackMaxAttempts)
	}
}

// TestWriterRetainsMissedFirstSample checks NACK base 1 repairs seq 1.
//
//nolint:gocognit,cyclop // test pins history, window, repair, and decode breadth in one run, splitting would obscure intent
func TestWriterRetainsMissedFirstSample(t *testing.T) {
	t.Parallel()

	local, err := NewLocalParticipant(0, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local participant: %v", err)
	}

	backend := &Backend{
		local:         local,
		repairHistory: newUserHistory(),
		routes:        make(map[GUIDPrefix][]int),
	}

	var guid WriterGUID
	lg := local.GUID()
	copy(guid[:12], lg.Prefix[:])
	guid[12] = writerEntityUserB12
	guid[13] = writerEntityUserB13
	guid[14] = writerEntityUserB14
	guid[15] = writerEntityUserB15

	for seq := uint64(1); seq <= 5; seq++ {
		backend.repairHistory.store(guid, seq, []byte{byte(seq)})
	}

	if got := backend.repairHistoryFirst(guid); got != 1 {
		t.Fatalf("first = %d, want 1 retained", got)
	}

	start, first, last, end, ok := backend.repairWindow(guid, 1)
	if !ok {
		t.Fatalf("repair window base 1 not ok, want ok")
	}

	if first != 1 || last != 5 {
		t.Fatalf("first/last = %d/%d, want 1/5", first, last)
	}

	if start != 1 || end != 5 {
		t.Fatalf("start/end = %d/%d, want 1/5", start, end)
	}

	payload, ok := backend.repairHistory.load(guid, 1)
	if !ok {
		t.Fatalf("seq 1 missing, want repair")
	}

	msg, err := backend.buildRepairMessage(guid, payload, 1, first, last)
	if err != nil {
		t.Fatalf("build repair 1: %v", err)
	}

	_, subs, err := rtps.DecodeMessage(msg, nil)
	if err != nil {
		t.Fatalf("decode repair: %v", err)
	}

	found := false
	for _, sub := range subs {
		if sub.ID != rtps.SubmessageData {
			continue
		}
		decoded, err := rtps.DecodeSubmessage(sub, nil)
		if err != nil {
			t.Fatalf("decode repair data: %v", err)
		}
		if decoded.Data == nil {
			continue
		}
		if decoded.Data.SequenceNumber == 1 {
			found = true
		}
	}

	if !found {
		t.Fatalf("repair message misses seq 1")
	}
}

// TestEvictedSeqYieldsGapWithLostAccounting checks evicted base gaps without lost.
//
//nolint:gocognit,cyclop // test pins evict, window, gap, and lost breadth in one run, splitting would obscure intent
func TestEvictedSeqYieldsGapWithLostAccounting(t *testing.T) {
	t.Parallel()

	local, err := NewLocalParticipant(0, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local participant: %v", err)
	}

	backend := &Backend{
		local:         local,
		repairHistory: newUserHistory(),
		routes:        make(map[GUIDPrefix][]int),
	}

	var guid WriterGUID
	lg := local.GUID()
	copy(guid[:12], lg.Prefix[:])
	guid[12] = writerEntityUserB12
	guid[13] = writerEntityUserB13
	guid[14] = writerEntityUserB14
	guid[15] = writerEntityUserB15

	total := int(userHistoryDepth) + 5
	for seq := uint64(1); seq <= uint64(total); seq++ {
		backend.repairHistory.store(guid, seq, []byte{byte(seq)})
	}

	first := backend.repairHistoryFirst(guid)
	if first != 6 {
		t.Fatalf("first = %d, want 6 after evict", first)
	}

	last := backend.repairHistoryLast(guid)
	if last != uint64(total) {
		t.Fatalf("last = %d, want %d", last, total)
	}

	start, rfirst, rlast, end, ok := backend.repairWindow(guid, 1)
	if !ok {
		t.Fatalf("repair window base 1 not ok, want ok for evicted")
	}

	if start != 1 {
		t.Fatalf("start = %d, want 1 evicted prefix not silent", start)
	}

	if rfirst != first || rlast != last {
		t.Fatalf("first/last = %d/%d, want %d/%d", rfirst, rlast, first, last)
	}

	if end != 1+uint64(userRepairWindow)-1 {
		t.Fatalf("end = %d, want %d bounded window", end, 1+uint64(userRepairWindow)-1)
	}

	_, ok = backend.repairHistory.load(guid, 1)
	if ok {
		t.Fatalf("seq 1 present after evict, want missing")
	}

	gapMsg, err := backend.buildGapMessage(guid, 1, rfirst, rlast)
	if err != nil {
		t.Fatalf("build gap 1: %v", err)
	}

	_, subs, err := rtps.DecodeMessage(gapMsg, nil)
	if err != nil {
		t.Fatalf("decode gap: %v", err)
	}

	foundGap := false
	for _, sub := range subs {
		if sub.ID != rtps.SubmessageGap {
			continue
		}
		var drops rtps.Counter
		gap, err := rtps.DecodeGap(sub.Body, sub.Flags, &drops)
		if err != nil {
			t.Fatalf("decode gap sub: %v", err)
		}
		if gap.GapStart == 1 {
			foundGap = true
		}
	}

	if !foundGap {
		t.Fatalf("gap message misses gap start 1")
	}

	recv := NewDataReceiver()
	recv.ApplyGap(guid, 1)
	if got := recv.LostCount(); got != 0 {
		t.Fatalf("lost after evicted gap = %d, want 0", got)
	}

	if got := recv.DroppedCount(); got != 0 {
		t.Fatalf("drops after evicted gap = %d, want 0", got)
	}
}

// TestPromptNackOnInitialGapBounded checks prompt NACK caps retries and bounds base.
//
//nolint:cyclop // test pins prompt, base, queue, and data breadth in one run, splitting would obscure intent
func TestPromptNackOnInitialGapBounded(t *testing.T) {
	t.Parallel()

	local, err := NewLocalParticipant(0, DefaultLeaseDuration)
	if err != nil {
		t.Fatalf("local participant: %v", err)
	}

	backend := &Backend{
		local:         local,
		repairHistory: newUserHistory(),
		routes:        make(map[GUIDPrefix][]int),
		remoteTrack:   make(map[WriterGUID]*remoteTracker),
		dataCh:        make(chan dataItem, demuxQueueDepth),
		repairQueue:   make(chan repairTask, repairQueueDepth),
		repairGen:     7,
	}

	var remotePrefix [12]byte
	for i := range remotePrefix {
		remotePrefix[i] = byte(0x90 + i)
	}

	var remote WriterGUID
	copy(remote[:12], remotePrefix[:])
	remote[12] = writerEntityUserB12
	remote[13] = writerEntityUserB13
	remote[14] = writerEntityUserB14
	remote[15] = writerEntityUserB15

	resetPromptNackForTest(backend, remote)

	ctx := t.Context()
	for range 10 {
		backend.maybePromptNackOnData(remote, 5)
	}

	if got := promptNackCountForTest(backend, remote); got != userPromptNackMaxAttempts {
		t.Fatalf("prompt count = %d, want cap %d no storm", got, userPromptNackMaxAttempts)
	}

	if got := len(backend.repairQueue); got != userPromptNackMaxAttempts {
		t.Fatalf("repair queue = %d, want %d queued ACKs, not swallowed", got, userPromptNackMaxAttempts)
	}

	queued := <-backend.repairQueue
	if queued.gen != backend.repairGen {
		t.Fatalf("queued gen = %d, want %d", queued.gen, backend.repairGen)
	}

	if queued.kind != repairTaskAck {
		t.Fatalf("queued kind = %d, want ack", queued.kind)
	}

	if queued.base != FirstSequenceNumber {
		t.Fatalf("queued base = %d, want %d", queued.base, FirstSequenceNumber)
	}

	// Drain remaining queued prompts so the later seen-writer check starts clean.
	for len(backend.repairQueue) > 0 {
		<-backend.repairQueue
	}

	if got := promptNackBase(5); got != FirstSequenceNumber {
		t.Fatalf("prompt base for 5 = %d, want %d", got, FirstSequenceNumber)
	}

	huge := FirstSequenceNumber + uint64(userHistoryDepth) + 10
	if got := promptNackBase(huge); got != huge-uint64(userHistoryDepth) {
		t.Fatalf("prompt base for huge = %d, want %d bounded window", got, huge-uint64(userHistoryDepth))
	}

	header := rtps.DefaultHeader(remotePrefix)
	userWriter := rtps.EntityID{writerEntityUserB12, writerEntityUserB13, writerEntityUserB14, writerEntityUserB15}
	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       userWriter,
		SequenceNumber: 5,
		Payload:        []byte{0x0A, 0x0B},
	}

	raw, err := data.Submessage(true)
	if err != nil {
		t.Fatalf("data submessage: %v", err)
	}

	before := promptNackCountForTest(backend, remote)
	backend.enqueueData(ctx, backend.dataCh, header, raw)

	if len(backend.dataCh) != 1 {
		t.Fatalf("data queue = %d, want 1 never block on repair", len(backend.dataCh))
	}

	after := promptNackCountForTest(backend, remote)
	if after != before {
		t.Fatalf("prompt count moved %d to %d on seen writer, want capped", before, after)
	}

	if got := backend.demuxErrorCount(); got != 0 {
		t.Fatalf("demux errors = %d, want 0 best effort", got)
	}
}

// TestRepairHistoryByteBudgetEvictsOldest checks byte overfill evicts plus accounting.
func TestRepairHistoryByteBudgetEvictsOldest(t *testing.T) {
	t.Parallel()

	history := newUserHistory()
	guid := testNackGUID(0xAA)

	payload := bytes.Repeat([]byte{0xAB}, 16<<20)
	const samples = 5

	for seq := uint64(1); seq <= samples; seq++ {
		history.store(guid, seq, payload)
	}

	if got := history.totalBytes(); got > userHistoryMaxBytes {
		t.Fatalf("history bytes = %d above budget %d", got, userHistoryMaxBytes)
	}

	if _, ok := history.load(guid, 1); ok {
		t.Fatalf("seq 1 present after byte overfill, want evicted")
	}

	stored, ok := history.load(guid, samples)
	if !ok {
		t.Fatalf("seq %d missing after byte overfill, want retained", samples)
	}

	if len(stored) != len(payload) {
		t.Fatalf("retained length = %d, want %d", len(stored), len(payload))
	}

	if first := history.first(guid); first <= 1 {
		t.Fatalf("first = %d after byte evict, want above 1", first)
	}
}

// TestRollbackSeqForMaxRestoresExactly checks Max claim rollback keeps global.
func TestRollbackSeqForMaxRestoresExactly(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		rtpsSeqByWriter: map[WriterGUID]uint64{},
		rtpsSeq:         ^uint64(0),
	}
	guid := testNackGUID(0xBB)
	backend.rtpsSeqByWriter[guid] = 0

	backend.rollbackSeqFor(guid, ^uint64(0))

	if got := backend.rtpsSeqByWriter[guid]; got != ^uint64(0) {
		t.Fatalf("per-writer = %d, want Max after rollback", got)
	}

	backend.mu.RLock()
	global := backend.rtpsSeq
	backend.mu.RUnlock()

	if global != ^uint64(0) {
		t.Fatalf("global = %d, want Max, not Max-1", global)
	}
}

// TestEnsureProvisionalUsesFakeClock checks FirstSeen follows the receiver clock.
func TestEnsureProvisionalUsesFakeClock(t *testing.T) {
	t.Parallel()

	fake := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	receiver := NewDataReceiver()
	receiver.now = func() time.Time {
		return fake
	}

	var guid WriterGUID
	guid[0] = 0xCC

	prov, ok := receiver.ensureProvisional(guid, ProvisionalQoS{}, false)
	if !ok {
		t.Fatalf("provisional not installed, want ok")
	}

	if !prov.FirstSeen.Equal(fake) {
		t.Fatalf("FirstSeen = %v, want %v fake clock", prov.FirstSeen, fake)
	}

	later := fake.Add(time.Hour)
	receiver.now = func() time.Time {
		return later
	}

	again, ok := receiver.ensureProvisional(guid, ProvisionalQoS{}, false)
	if !ok {
		t.Fatalf("provisional missing on second call, want ok")
	}

	if !again.FirstSeen.Equal(fake) {
		t.Fatalf("FirstSeen moved to %v, want stable %v", again.FirstSeen, fake)
	}
}

// TestSedpShouldSkipResendAtUsesFakeClock checks resend timing with explicit now.
func TestSedpShouldSkipResendAtUsesFakeClock(t *testing.T) {
	t.Parallel()

	fake := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)

	if !sedpShouldSkipResendAt(nil, nil, nil, nil, fake, fake.Add(500*time.Millisecond)) {
		t.Fatalf("skip within interval returned false, want true")
	}

	if sedpShouldSkipResendAt(nil, nil, nil, nil, fake, fake.Add(2*time.Second)) {
		t.Fatalf("skip past interval returned true, want false")
	}
}

// encodeTestNackFrag builds one little-endian NACKFRAG body: reader,
// writer, writer seq, fragment base, bit count, MSB-first bitmap words,
// then the reader count.
func encodeTestNackFrag(t *testing.T, reader, writer rtps.EntityID, seq uint64, fragBase uint32, frags []uint32, count int32) []byte {
	t.Helper()
	maxFrag := fragBase
	for _, f := range frags {
		if f > maxFrag {
			maxFrag = f
		}
	}
	var numBits uint32
	if len(frags) > 0 {
		numBits = maxFrag - fragBase + 1
	}
	words := (numBits + 31) / 32
	bitmap := make([]uint32, words)
	for _, f := range frags {
		i := f - fragBase
		bitmap[i/32] |= 1 << (31 - (i % 32))
	}
	out := make([]byte, 0, 28+4*len(bitmap))
	out = append(out, reader[:]...)
	out = append(out, writer[:]...)
	var u32 [4]byte
	putNackUint32(u32[:], uint32(seq>>32))
	out = append(out, u32[:]...)
	putNackUint32(u32[:], uint32(seq)) //nolint:gosec // G115: test seqs are small literals
	out = append(out, u32[:]...)
	putNackUint32(u32[:], fragBase)
	out = append(out, u32[:]...)
	putNackUint32(u32[:], numBits)
	out = append(out, u32[:]...)
	for _, w := range bitmap {
		putNackUint32(u32[:], w)
		out = append(out, u32[:]...)
	}
	putNackUint32(u32[:], uint32(count)) //nolint:gosec // G115: test reader count is a small literal
	out = append(out, u32[:]...)
	return out
}

// putNackUint32 stores one little-endian word for the NACKFRAG test body.
func putNackUint32(out []byte, v uint32) {
	binary.LittleEndian.PutUint32(out, v)
}

// TestDecodeUserNackFragRoundTrip pins the NACKFRAG codec: frags 5 and 7 of
// seq 9 decode back exactly, MSB first like the Gap bitmap order.
func TestDecodeUserNackFragRoundTrip(t *testing.T) {
	t.Parallel()
	var reader, writer rtps.EntityID
	reader[3] = 0x01
	writer[3] = 0x02
	body := encodeTestNackFrag(t, reader, writer, 9, 5, []uint32{5, 7}, 3)
	req, err := decodeUserNackFrag(body, rtps.FlagLittleEndian)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if req.seq != 9 {
		t.Fatalf("seq = %d, want 9", req.seq)
	}
	if len(req.fragNums) != 2 || req.fragNums[0] != 5 || req.fragNums[1] != 7 {
		t.Fatalf("frags = %v, want [5 7]", req.fragNums)
	}
	if req.count != 3 {
		t.Fatalf("count = %d, want 3", req.count)
	}
}

// TestDecodeUserNackFragRejectsBadBodies pins the loud bounds: short
// bodies, zero fragment bases, and oversized bit counts fail; a zero bit
// count decodes to an empty no-op request.
func TestDecodeUserNackFragRejectsBadBodies(t *testing.T) {
	t.Parallel()
	var reader, writer rtps.EntityID
	if _, err := decodeUserNackFrag(make([]byte, 10), rtps.FlagLittleEndian); err == nil {
		t.Fatalf("short body decoded without error")
	}
	zeroBase := encodeTestNackFrag(t, reader, writer, 9, 0, nil, 1)
	if _, err := decodeUserNackFrag(zeroBase, rtps.FlagLittleEndian); err == nil {
		t.Fatalf("zero frag base decoded without error")
	}
	huge := encodeTestNackFrag(t, reader, writer, 9, 1, nil, 1)
	// Patch numBits past the cap: body is reader+writer+seq+base+bits+count.
	huge[20] = 0xFF
	huge[21] = 0xFF
	if _, err := decodeUserNackFrag(huge, rtps.FlagLittleEndian); err == nil {
		t.Fatalf("oversized bits decoded without error")
	}
	empty := encodeTestNackFrag(t, reader, writer, 9, 5, nil, 1)
	req, err := decodeUserNackFrag(empty, rtps.FlagLittleEndian)
	if err != nil {
		t.Fatalf("empty request: %v", err)
	}
	if len(req.fragNums) != 0 {
		t.Fatalf("empty request frags = %v, want none", req.fragNums)
	}
	zeroSeq := encodeTestNackFrag(t, reader, writer, 0, 5, nil, 1)
	req, err = decodeUserNackFrag(zeroSeq, rtps.FlagLittleEndian)
	if err != nil {
		t.Fatalf("zero seq: %v", err)
	}
	if len(req.fragNums) != 0 {
		t.Fatalf("zero seq frags = %v, want none", req.fragNums)
	}
}

// TestSelectiveNackBitmapRequestsOnlyMissing pins selective repair.
//
// A tracker with contiguity plus out-of-order seen must request only the
// gaps in base through last, clearing bits for already received sequences
// so retransmits converge instead of filling the demux queue with duplicates.
// A nil tracker requests the full range like the legacy path. Empty ranges
// request nothing.
func TestSelectiveNackBitmapRequestsOnlyMissing(t *testing.T) {
	t.Parallel()

	tracker := &remoteTracker{contig: 5, seen: map[uint64]struct{}{7: {}, 8: {}}}

	bitmap, numBits := selectiveNackBitmap(6, 10, tracker)
	if numBits != 5 {
		t.Fatalf("numBits = %d, want 5 for base 6 last 10", numBits)
	}

	if len(bitmap) != 1 {
		t.Fatalf("bitmap words = %d, want 1", len(bitmap))
	}

	// Base 6 missing, 7 seen, 8 seen, 9 missing, 10 missing. MSB packing
	// puts base at bit 31: bits 31, 28, 27 set, bits 30 and 29 clear.
	// Top 5 bits 10011 is 0x98000000.
	want := uint32(0x98000000)
	if bitmap[0] != want {
		t.Fatalf("bitmap = %08X, want %08X", bitmap[0], want)
	}

	full, fullBits := selectiveNackBitmap(6, 10, nil)
	if fullBits != 5 {
		t.Fatalf("nil tracker bits = %d, want 5", fullBits)
	}

	if len(full) != 1 || full[0] != 0xF8000000 {
		t.Fatalf("nil tracker bitmap = % X, want F8000000", full)
	}

	empty, emptyBits := selectiveNackBitmap(11, 10, tracker)
	if emptyBits != 0 || empty != nil {
		t.Fatalf("empty range bits = %d bitmap = %v, want 0 nil", emptyBits, empty)
	}
}
