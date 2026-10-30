// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// User DATA reliability for first contact.
//
// Scope is minimal repair to unblock DDS conformance with CycloneDDS reliable peers. The writer keeps
// a bounded history per user writer GUID and answers ACKNACK with repair DATA plus GAP for unknown
// seq. The reader tracks the highest outer RTPS seq per remote writer, answers HEARTBEAT with ACKNACK,
// and skips GAP seq without lost count. Best effort fallback stays: Take and TakeFrom ordering is
// unchanged when no HEARTBEAT was seen. DATA_FRAG reassembles with bounds in frag.go and delivers one
// payload to dataCh; small samples still fit one DATA submessage under 64 KiB, which covers /chatter
// interop.

// userHistoryDepth caps kept samples per writer for repair. Oldest evicts first.
// Depth 100 matches FacadeHistoryDepth for late joiners and covers fleet churn
// where 64 evicted the first sample before NACK; window stays 64 to bound work.
const userHistoryDepth = 100

// userRepairWindow caps seq scanned per ACKNACK to bound work. Larger gaps converge over later ACKs.
const userRepairWindow = 64

// userAckBitsMax caps ACKNACK bits to bound decode work. Larger receipts are rejected as invalid.
const userAckBitsMax = 1024

// userPromptNackMaxAttempts caps prompt NACK sends per remote writer for an
// initial gap. Heartbeat driven ACKs continue for convergence while prompt
// sends stay capped so a late joiner cannot cause a storm.
const userPromptNackMaxAttempts = 3

// promptNackKey identifies one remote writer for prompt counting per Backend.
type promptNackKey struct {
	backend *Backend
	guid    WriterGUID
}

// promptNackMu guards promptNackCounts.
var promptNackMu sync.Mutex

// promptNackCounts tracks prompt NACK attempts per remote writer.
var promptNackCounts = make(map[promptNackKey]int)

// ErrDataFragDropped reports a DATA_FRAG sample rejected without complete reassembly.
var ErrDataFragDropped = errors.New("dds: fragmented sample dropped")

// userAckCount counts sent ACKNACKs process wide. Zero is never emitted.
var userAckCount atomic.Int32

// userHistory stores sent RTPS payloads per writer GUID for ACKNACK repair.
//
// It is safe for concurrent use. The mutex must not be copied after first use.
type userHistory struct {
	mu sync.Mutex
	// payloads maps writer to seq to copy.
	payloads map[WriterGUID]map[uint64][]byte
	// order maps writer to FIFO seqs.
	order map[WriterGUID][]uint64
}

// newUserHistory returns an empty history.
func newUserHistory() *userHistory {
	return &userHistory{
		payloads: make(
			map[WriterGUID]map[uint64][]byte,
		),
		order: make(map[WriterGUID][]uint64),
	}
}

// store copies payload for guid and seq.
//
// It evicts oldest when depth is passed. A nil receiver is a no-op for tests that build Backend
// without live state.
func (h *userHistory) store(
	guid WriterGUID,
	seq uint64,
	payload []byte,
) {
	if h == nil {
		return
	}

	if seq == 0 {
		return
	}

	cp := append([]byte(nil), payload...)

	h.mu.Lock()
	defer h.mu.Unlock()

	bySeq, seen := h.payloads[guid]
	if !seen {
		bySeq = make(map[uint64][]byte)
		h.payloads[guid] = bySeq
	}

	_, dup := bySeq[seq]
	if !dup {
		h.order[guid] = append(
			h.order[guid],
			seq,
		)
	}

	bySeq[seq] = cp

	for len(h.order[guid]) > userHistoryDepth {
		old := h.order[guid][0]
		h.order[guid] = h.order[guid][1:]
		delete(bySeq, old)
	}
}

// load returns a copy for guid and seq.
func (h *userHistory) load(
	guid WriterGUID,
	seq uint64,
) ([]byte, bool) {
	if h == nil {
		return nil, false
	}

	h.mu.Lock()
	defer h.mu.Unlock()

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

// first returns the first kept seq or 0.
func (h *userHistory) first(guid WriterGUID) uint64 {
	if h == nil {
		return 0
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	ord := h.order[guid]
	if len(ord) == 0 {
		return 0
	}

	return ord[0]
}

// last returns the last kept seq or 0.
func (h *userHistory) last(guid WriterGUID) uint64 {
	if h == nil {
		return 0
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	ord := h.order[guid]
	if len(ord) == 0 {
		return 0
	}

	return ord[len(ord)-1]
}

// ackRequest is one decoded user ACKNACK.
type ackRequest struct {
	// reader names the requesting reader.
	reader rtps.EntityID
	// writer names the targeted writer.
	writer rtps.EntityID
	// base is the first wanted sequence.
	base uint64
	// numBits counts bitmap bits.
	numBits uint32
	// bitmap holds repair bits.
	bitmap []uint32
	// count is the reader counter.
	count int32
}

// getAckUint32 loads 4 bytes in order.
func getAckUint32(
	raw []byte,
	little bool,
) uint32 {
	if little {
		return binary.LittleEndian.Uint32(raw)
	}

	return binary.BigEndian.Uint32(raw)
}

// decodeUserAckNack parses one ACKNACK body with its flags. It validates length and bounds numBits for
// bounded work. Base 0 decodes without error; callers treat it as a silent no-op.
func decodeUserAckNack(
	body []byte,
	flags uint8,
) (ackRequest, error) {
	var out ackRequest

	little := rtps.LittleEndian(flags)

	const need = 24

	if len(body) < need {
		return out, fmt.Errorf(
			"dds: ack need %d have %d: %w",
			need,
			len(body),
			ErrInvalidFrame,
		)
	}

	copy(out.reader[:], body[0:4])
	copy(out.writer[:], body[4:8])

	high := getAckUint32(body[8:12], little)
	low := getAckUint32(body[12:16], little)
	out.base = uint64(high)<<32 | uint64(low)

	out.numBits = getAckUint32(body[16:20], little)

	if out.numBits > userAckBitsMax {
		return out, fmt.Errorf(
			"dds: ack bits %d above %d: %w",
			out.numBits,
			userAckBitsMax,
			ErrInvalidFrame,
		)
	}

	words := (uint64(out.numBits) + 31) / 32
	needTotal := uint64(need) + words*4
	if uint64(len(body)) < needTotal {
		return out, fmt.Errorf(
			"dds: ack need %d have %d: %w",
			needTotal,
			len(body),
			ErrInvalidFrame,
		)
	}

	out.bitmap = make([]uint32, words)
	off := 20

	for i := range out.bitmap {
		out.bitmap[i] = getAckUint32(
			body[off:off+4],
			little,
		)
		off += 4
	}

	out.count = int32( //nolint:gosec // G115: count wrap is layout
		getAckUint32(body[off:off+4], little),
	)

	return out, nil
}

// nextUserAckCount returns the next ACKNACK count, never zero. Negatives wrap to 1.
func nextUserAckCount() int32 {
	for {
		count := userAckCount.Add(1)
		if count == 0 {
			continue
		}

		if count < 0 {
			userAckCount.Store(1)

			return 1
		}

		return count
	}
}

// isUserWriterEntity reports whether id is a user writer, not SPDP or SEDP builtin.
func isUserWriterEntity(id rtps.EntityID) bool {
	if id == rtps.EntitySPDPWriter {
		return false
	}

	if sedpBuiltinWriter(id) {
		return false
	}

	return true
}

// putSeqLE stores seq as high then low in little endian. Callers guarantee 8 bytes.
func putSeqLE(buf []byte, seq uint64) {
	binary.LittleEndian.PutUint32(
		buf[0:4],
		uint32(seq>>32),
	)
	binary.LittleEndian.PutUint32(
		buf[4:8],
		uint32(seq), //nolint:gosec // G115: low word wrap is layout
	)
}

// encodeUserGapBody renders one GAP body in little endian for a single skipped seq. The gap list is
// empty with base next, so only gapStart matters.
func encodeUserGapBody(
	reader rtps.EntityID,
	writer rtps.EntityID,
	gapStart uint64,
) []byte {
	out := make([]byte, 0, 32)
	out = append(out, reader[:]...)
	out = append(out, writer[:]...)

	var start [8]byte
	putSeqLE(start[:], gapStart)
	out = append(out, start[:]...)

	next := gapStart + 1
	if next == 0 {
		next = FirstSequenceNumber
	}

	var base [8]byte
	putSeqLE(base[:], next)
	out = append(out, base[:]...)

	var bits [4]byte
	binary.LittleEndian.PutUint32(bits[:], 0)
	out = append(out, bits[:]...)

	return out
}

// ApplyGap skips gapStart without lost.
//
// The caller is the demux GAP path. Gaps mean the writer will never send the seq, so the receiver
// advances past it with no lost count, unlike best effort gaps which count missing as lost. Duplicates
// and old gaps are ignored.
func (r *DataReceiver) ApplyGap(
	guid WriterGUID,
	gapStart uint64,
) {
	if r == nil {
		return
	}

	if gapStart == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.last == nil {
		return
	}

	last, seen := r.last[guid]
	if !seen {
		r.last[guid] = gapStart

		return
	}

	if gapStart <= last {
		return
	}

	r.last[guid] = gapStart
}

// repairHistoryFirst returns the first kept seq or 1.
func (b *Backend) repairHistoryFirst(
	guid WriterGUID,
) uint64 {
	if b.repairHistory == nil {
		return FirstSequenceNumber
	}

	first := b.repairHistory.first(guid)
	if first == 0 {
		return FirstSequenceNumber
	}

	return first
}

// repairHistoryLast returns the last kept seq or 0.
func (b *Backend) repairHistoryLast(
	guid WriterGUID,
) uint64 {
	if b.repairHistory == nil {
		return 0
	}

	return b.repairHistory.last(guid)
}

// noteRemoteData records one outer seq.
//
// It tracks the highest RTPS seq per remote writer for ACKNACK base. A nil map is allocated on first
// use under mu.
func (b *Backend) noteRemoteData(
	guid WriterGUID,
	seq uint64,
) {
	if seq == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.remoteHighSeq == nil {
		b.remoteHighSeq = make(map[WriterGUID]uint64)
	}

	if seq > b.remoteHighSeq[guid] {
		b.remoteHighSeq[guid] = seq
	}
}

// remoteBase returns next wanted seq for a remote writer: highest plus one, or first from heartbeat
// when never seen.
func (b *Backend) remoteBase(
	guid WriterGUID,
	first uint64,
) uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	last, seen := b.remoteHighSeq[guid]
	if !seen {
		if first == 0 {
			return FirstSequenceNumber
		}

		return first
	}

	next := last + 1
	if next == 0 {
		return FirstSequenceNumber
	}

	return next
}

// noteSedpSeq records one SEDP DATA seq per remote builtin writer.
//
// It tracks the highest sequence so heartbeats ack only missing samples
// instead of the full history. A nil map is allocated on first use under
// mu. Zero sequences are ignored. Updates are monotonic so duplicate
// retransmits keep the same high and the next heartbeat re-NACKs the same
// range instead of advancing past unseen samples.
func (b *Backend) noteSedpSeq(guid WriterGUID, seq uint64) {
	if seq == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sedpHighSeq == nil {
		b.sedpHighSeq = make(map[WriterGUID]uint64)
	}

	if seq > b.sedpHighSeq[guid] {
		b.sedpHighSeq[guid] = seq
	}
}

// sedpBase returns next wanted seq for a remote builtin writer: highest
// plus one, or first from heartbeat when never seen.
//
// Why: a first heartbeat before any local DATA must NACK the full
// advertised range, so unseen writers start at first instead of requiring
// prior state. When seen, the base clamps up to first so a writer that
// advanced past our high through GAP or restart does not get NACKs for
// seq it will never resend. Overflow wraps to FirstSequenceNumber to keep
// the base valid. Callers treat base past last as caught up and send a
// pure ACK.
func (b *Backend) sedpBase(guid WriterGUID, first uint64) uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	last, seen := b.sedpHighSeq[guid]
	if !seen {
		if first == 0 {
			return FirstSequenceNumber
		}

		return first
	}

	next := last + 1
	if next == 0 {
		return FirstSequenceNumber
	}

	// Why clamp: first beyond next means the writer already skipped everything
	// we would request, so asking for next would NACK dead seq forever.
	if first != 0 && first > next {
		return first
	}

	return next
}

// applySedpGap advances SEDP tracking for one GAP.
//
// Why: a GAP means the writer will never send the seq, so the next
// heartbeat must not NACK it or the peer retransmits nothing and the NACK
// repeats forever. Updates are monotonic and zero is ignored, which keeps
// re-NACKs idempotent and bounded when GAPs repeat.
func (b *Backend) applySedpGap(guid WriterGUID, gapStart uint64) {
	if gapStart == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sedpHighSeq == nil {
		b.sedpHighSeq = make(map[WriterGUID]uint64)
	}

	if gapStart > b.sedpHighSeq[guid] {
		b.sedpHighSeq[guid] = gapStart
	}
}

// planSedpNack computes a bounded NACK bitmap for base through last.
//
// Why: lost NACKs must re-send the identical bounded bitmap on the next
// heartbeat with the same range instead of growing. Base 0 and base past
// last mean caught up, so the caller sends a pure ACK with the Final flag
// and the peer slows to keepalive pace. Otherwise the bitmap sets every
// bit from base through last and the caller sends a NACK without Final so
// the peer keeps repairing. Length is capped by sedpNackBitmap at
// userAckBitsMax so huge ranges stay bounded.
func planSedpNack(base, last uint64) ([]uint32, uint32, bool) {
	if base == 0 {
		return nil, 0, false
	}

	if base > last {
		return nil, 0, false
	}

	bitmap, numBits := sedpNackBitmap(last - base + 1)

	return bitmap, numBits, true
}

// applyRemoteGap advances remote tracking plus DataReceiver skip for one GAP.
func (b *Backend) applyRemoteGap(
	guid WriterGUID,
	gapStart uint64,
) {
	if gapStart == 0 {
		return
	}

	b.mu.Lock()
	if b.remoteHighSeq == nil {
		b.remoteHighSeq = make(map[WriterGUID]uint64)
	}

	if gapStart > b.remoteHighSeq[guid] {
		b.remoteHighSeq[guid] = gapStart
	}

	receiver := b.receiver
	b.mu.Unlock()

	if receiver != nil {
		receiver.ApplyGap(guid, gapStart)
	}
}

// buildRepairMessage wraps one stored payload in DATA with its original seq plus one HEARTBEAT. First
// and last come from history so evicted prefix is not claimed.
func (b *Backend) buildRepairMessage(
	guid WriterGUID,
	payload []byte,
	seq uint64,
	first uint64,
	last uint64,
) ([]byte, error) {
	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()

	if local == nil {
		return nil, fmt.Errorf(
			"dds: repair without transport: %w",
			ErrDataPathNotWired,
		)
	}

	var prefix [12]byte

	lg := local.GUID()
	copy(prefix[:], lg.Prefix[:])

	header := rtps.DefaultHeader(prefix)

	var writerID rtps.EntityID
	copy(writerID[:], guid[12:16])

	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       writerID,
		SequenceNumber: seq,
		Payload:        payload,
	}

	raw, err := data.Submessage(true)
	if err != nil {
		return nil, fmt.Errorf(
			"dds: repair data: %w",
			err,
		)
	}

	beat := rtps.Heartbeat{
		ReaderID:      rtps.EntityUnknown,
		WriterID:      writerID,
		FirstSequence: first,
		LastSequence:  last,
		Count:         nextHeartbeatCount(),
	}

	hbRaw := rtps.RawSubmessage{
		ID:    rtps.SubmessageHeartbeat,
		Flags: rtps.FlagLittleEndian,
		Body:  beat.Encode(true),
	}

	msg, err := rtps.EncodeMessage(
		header,
		[]rtps.RawSubmessage{raw, hbRaw},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"dds: repair encode: %w",
			err,
		)
	}

	return msg, nil
}

// buildGapMessage wraps one GAP plus one HEARTBEAT for a missing seq. It tells a reliable reader to
// skip seq it will never receive because history evicted it.
func (b *Backend) buildGapMessage(
	guid WriterGUID,
	gapStart uint64,
	first uint64,
	last uint64,
) ([]byte, error) {
	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()

	if local == nil {
		return nil, fmt.Errorf(
			"dds: gap without transport: %w",
			ErrDataPathNotWired,
		)
	}

	var prefix [12]byte

	lg := local.GUID()
	copy(prefix[:], lg.Prefix[:])

	header := rtps.DefaultHeader(prefix)

	var writerID rtps.EntityID
	copy(writerID[:], guid[12:16])

	gapBody := encodeUserGapBody(
		rtps.EntityUnknown,
		writerID,
		gapStart,
	)

	gapRaw := rtps.RawSubmessage{
		ID:    rtps.SubmessageGap,
		Flags: rtps.FlagLittleEndian,
		Body:  gapBody,
	}

	beat := rtps.Heartbeat{
		ReaderID:      rtps.EntityUnknown,
		WriterID:      writerID,
		FirstSequence: first,
		LastSequence:  last,
		Count:         nextHeartbeatCount(),
	}

	hbRaw := rtps.RawSubmessage{
		ID:    rtps.SubmessageHeartbeat,
		Flags: rtps.FlagLittleEndian,
		Body:  beat.Encode(true),
	}

	msg, err := rtps.EncodeMessage(
		header,
		[]rtps.RawSubmessage{gapRaw, hbRaw},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"dds: gap encode: %w",
			err,
		)
	}

	return msg, nil
}

// buildAckSubmessage renders one ACKNACK submessage for a remote writer.
//
// Reader names the local reader the receipt belongs to and base is next
// wanted. Set bits in bitmap request repair of base plus bit index;
// NumBits 0 with no bitmap requests nothing, sets the Final flag, and only
// advances the writer.
//
// Why Final discipline matters: pure ACKs carry Final so the writer slows
// to keepalive pace, while NACKs never carry Final so the writer keeps
// repairing. Mixing this up either stalls repair or causes ACK storms.
func (b *Backend) buildAckSubmessage(
	remote WriterGUID,
	reader rtps.EntityID,
	base uint64,
	numBits uint32,
	bitmap []uint32,
) (rtps.RawSubmessage, error) {
	var empty rtps.RawSubmessage

	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()

	if local == nil {
		return empty, fmt.Errorf(
			"dds: ack without transport: %w",
			ErrDataPathNotWired,
		)
	}

	if base == 0 {
		return empty, fmt.Errorf(
			"dds: ack base 0: %w",
			ErrInvalidFrame,
		)
	}

	var writerID rtps.EntityID
	copy(writerID[:], remote[12:16])

	ack := rtps.AckNack{
		ReaderID: reader,
		WriterID: writerID,
		Base:     base,
		NumBits:  numBits,
		Bitmap:   bitmap,
		Count:    nextUserAckCount(),
	}

	body, err := ack.Encode(true)
	if err != nil {
		return empty, fmt.Errorf(
			"dds: ack encode: %w",
			err,
		)
	}

	flags := uint8(rtps.FlagLittleEndian)
	if numBits == 0 {
		flags |= rtps.AckNackFlagFinal
	}

	return rtps.RawSubmessage{
		ID:    rtps.SubmessageAckNack,
		Flags: flags,
		Body:  body,
	}, nil
}

// buildAckMessage wraps one ACKNACK for a remote writer. Reader names the
// local reader the receipt belongs to and base is next wanted. NumBits 0
// with no bitmap requests nothing and only advances the writer.
func (b *Backend) buildAckMessage(
	remote WriterGUID,
	reader rtps.EntityID,
	base uint64,
) ([]byte, error) {
	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()

	if local == nil {
		return nil, fmt.Errorf(
			"dds: ack without transport: %w",
			ErrDataPathNotWired,
		)
	}

	if base == 0 {
		return nil, fmt.Errorf(
			"dds: ack base 0: %w",
			ErrInvalidFrame,
		)
	}

	var prefix [12]byte

	lg := local.GUID()
	copy(prefix[:], lg.Prefix[:])

	header := rtps.DefaultHeader(prefix)

	raw, err := b.buildAckSubmessage(remote, reader, base, 0, nil)
	if err != nil {
		return nil, err
	}

	msg, err := rtps.EncodeMessage(
		header,
		[]rtps.RawSubmessage{raw},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"dds: ack encode message: %w",
			err,
		)
	}

	return msg, nil
}

// sendUserControl fanouts one control datagram to all routes best effort.
func (b *Backend) sendUserControl(
	ctx context.Context,
	msg []byte,
) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf(
			"dds: user control context: %w",
			err,
		)
	}

	ports, uni := b.snapshotRoutesAndUnicast()

	if len(ports) == 0 {
		return nil
	}

	if uni == nil {
		return fmt.Errorf(
			"dds: user control without transport: %w",
			ErrDataPathNotWired,
		)
	}

	sent, last := sendToPorts(ctx, uni, ports, msg)
	if sent > 0 {
		return nil
	}

	if last != nil {
		return fmt.Errorf(
			"dds: user control send: %w",
			last,
		)
	}

	return nil
}

// repairWindow bounds repair to window and returns start, first, last, end. It reports false when no
// repair is needed. Start is base even when base precedes history first so evicted prefix
// yields GAP instead of silent skip; each evicted seq keeps reasoned accounting through GAP
// while retained seq repairs from history. End caps to base plus window so per ACK work stays
// bounded and larger gaps converge over later ACKs as the reader advances on GAP.
func (b *Backend) repairWindow(
	guid WriterGUID,
	base uint64,
) (uint64, uint64, uint64, uint64, bool) {
	first := b.repairHistoryFirst(guid)
	last := b.repairHistoryLast(guid)

	if last == 0 {
		return 0, 0, 0, 0, false
	}

	if base > last {
		return 0, 0, 0, 0, false
	}

	start := base

	end := start + userRepairWindow - 1
	if end > last || end < start {
		end = last
	}

	return start, first, last, end, true
}

// repairOneSeq resends one seq or GAPs it when history misses it.
func (b *Backend) repairOneSeq(
	ctx context.Context,
	guid WriterGUID,
	seq uint64,
	first uint64,
	last uint64,
) error {
	payload, ok := b.repairHistory.load(guid, seq)
	if ok {
		msg, err := b.buildRepairMessage(
			guid,
			payload,
			seq,
			first,
			last,
		)
		if err != nil {
			return err
		}

		err = b.sendUserControl(ctx, msg)
		if err != nil {
			return err
		}

		return nil
	}

	msg, err := b.buildGapMessage(
		guid,
		seq,
		first,
		last,
	)
	if err != nil {
		return err
	}

	err = b.sendUserControl(ctx, msg)
	if err != nil {
		return err
	}

	return nil
}

// repairFromAck resends history from base plus GAP for missing in window. It bounds work to
// userRepairWindow seq. Base 0 and unknown writers are ignored without repair.
func (b *Backend) repairFromAck(
	ctx context.Context,
	writer rtps.EntityID,
	base uint64,
) error {
	if base == 0 {
		return nil
	}

	if !isUserWriterEntity(writer) {
		return nil
	}

	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()

	if local == nil {
		return nil
	}

	var guid WriterGUID

	lg := local.GUID()
	copy(guid[:12], lg.Prefix[:])
	copy(guid[12:16], writer[:])

	start, first, last, end, ok := b.repairWindow(
		guid,
		base,
	)
	if !ok {
		return nil
	}

	for seq := start; seq <= end; seq++ {
		err := ctx.Err()
		if err != nil {
			return fmt.Errorf(
				"dds: repair context: %w",
				err,
			)
		}

		err = b.repairOneSeq(
			ctx,
			guid,
			seq,
			first,
			last,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

// ackUserHeartbeat answers one user HEARTBEAT with ACKNACK. Base is next wanted from remote tracking.
func (b *Backend) ackUserHeartbeat(
	ctx context.Context,
	header rtps.Header,
	beat rtps.Heartbeat,
) {
	if !isUserWriterEntity(beat.WriterID) {
		return
	}

	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()

	if local == nil {
		return
	}

	var remote WriterGUID
	copy(remote[:12], header.Prefix[:])
	copy(remote[12:16], beat.WriterID[:])

	base := max(
		b.remoteBase(remote, beat.FirstSequence),
		beat.FirstSequence,
	)

	if base == 0 {
		return
	}

	msg, err := b.buildAckMessage(remote, rtps.EntityUnknown, base)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux ack build: %v", err)

		return
	}

	err = b.sendUserControl(ctx, msg)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux ack send: %v", err)
	}
}

// promptNackCountForTest returns prompt attempts for a remote writer.
func promptNackCountForTest(b *Backend, guid WriterGUID) int {
	promptNackMu.Lock()
	defer promptNackMu.Unlock()

	return promptNackCounts[promptNackKey{backend: b, guid: guid}]
}

// resetPromptNackForTest clears prompt attempts for a remote writer.
func resetPromptNackForTest(b *Backend, guid WriterGUID) {
	promptNackMu.Lock()
	defer promptNackMu.Unlock()

	delete(promptNackCounts, promptNackKey{backend: b, guid: guid})
}

// tryClaimPromptNack claims one prompt attempt when under cap.
func (b *Backend) tryClaimPromptNack(guid WriterGUID) bool {
	promptNackMu.Lock()
	defer promptNackMu.Unlock()

	key := promptNackKey{backend: b, guid: guid}
	if promptNackCounts[key] >= userPromptNackMaxAttempts {
		return false
	}

	promptNackCounts[key]++

	return true
}

// promptNackBase bounds a prompt request to the retention window.
func promptNackBase(seq uint64) uint64 {
	if seq <= FirstSequenceNumber {
		return FirstSequenceNumber
	}

	if seq > FirstSequenceNumber+userHistoryDepth {
		return seq - userHistoryDepth
	}

	return FirstSequenceNumber
}

// sendPromptNack emits one best effort ACKNACK for an initial gap.
func (b *Backend) sendPromptNack(
	ctx context.Context,
	remote WriterGUID,
	base uint64,
) {
	if base == 0 {
		return
	}

	msg, err := b.buildAckMessage(remote, rtps.EntityUnknown, base)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux prompt ack build: %v", err)

		return
	}

	err = b.sendUserControl(ctx, msg)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux prompt ack send: %v", err)
	}
}

// maybePromptNackOnData emits a bounded prompt NACK when the first DATA seq
// for an unseen writer is beyond first, which means 1 through N minus 1 were
// missed. Seen writers return without send so steady state stays on the
// heartbeat cycle and cannot storm. Capped claims bound retries while the
// repair window bounds per NACK work. Failures only bump demux errors so the
// data path never blocks on repair and lost counting stays in Take.
func (b *Backend) maybePromptNackOnData(
	ctx context.Context,
	remote WriterGUID,
	seq uint64,
) {
	if seq <= FirstSequenceNumber {
		return
	}

	var entity rtps.EntityID
	copy(entity[:], remote[12:16])
	if !isUserWriterEntity(entity) {
		return
	}

	b.mu.RLock()
	_, seen := b.remoteHighSeq[remote]
	b.mu.RUnlock()

	if seen {
		return
	}

	claimed := b.tryClaimPromptNack(remote)
	if !claimed {
		return
	}

	b.sendPromptNack(ctx, remote, promptNackBase(seq))
}

// handleUserAck routes one ACKNACK for a user writer to repair, not to dataCh.
func (b *Backend) handleUserAck(
	ctx context.Context,
	sub rtps.RawSubmessage,
) {
	req, err := decodeUserAckNack(sub.Body, sub.Flags)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux ack decode: %v", err)

		return
	}

	err = b.repairFromAck(ctx, req.writer, req.base)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux repair: %v", err)
	}
}

// handleUserHeartbeat routes one HEARTBEAT for a user writer to ACK, not to dataCh.
func (b *Backend) handleUserHeartbeat(
	ctx context.Context,
	header rtps.Header,
	sub rtps.RawSubmessage,
) {
	var drops rtps.Counter

	beat, err := rtps.DecodeHeartbeat(
		sub.Body,
		sub.Flags,
		&drops,
	)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux heartbeat: %v", err)

		return
	}

	b.ackUserHeartbeat(ctx, header, beat)
}

// handleUserGap routes one GAP for a user writer to skip, not to dataCh. Only GapStart is honored; the
// list is tolerated but not expanded, which fits single-seq GAPs used for repair.
func (b *Backend) handleUserGap(
	sub rtps.RawSubmessage,
	header rtps.Header,
) {
	var drops rtps.Counter

	gap, err := rtps.DecodeGap(
		sub.Body,
		sub.Flags,
		&drops,
	)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux gap: %v", err)

		return
	}

	if !isUserWriterEntity(gap.WriterID) {
		return
	}

	var remote WriterGUID
	copy(remote[:12], header.Prefix[:])
	copy(remote[12:16], gap.WriterID[:])

	b.applyRemoteGap(remote, gap.GapStart)
}

// handleUserControl processes user control subs before DATA queueing. It handles ACKNACK with repair,
// HEARTBEAT with ACK, GAP with skip, and DATA_FRAG with reassembly to dataCh. It returns true when sub
// was user control and must not reach dataCh through the DATA path.
func (b *Backend) handleUserControl(
	ctx context.Context,
	header rtps.Header,
	sub rtps.RawSubmessage,
	dataCh chan dataItem,
) bool {
	switch sub.ID {
	case rtps.SubmessageAckNack:
		rawReq, err := decodeUserAckNack(
			sub.Body,
			sub.Flags,
		)
		if err != nil {
			b.bumpDemuxError()
			log.Printf(
				"dds: demux ack decode: %v",
				err,
			)

			return true
		}

		if !isUserWriterEntity(rawReq.writer) {
			return false
		}

		b.handleUserAck(ctx, sub)

		return true
	case rtps.SubmessageHeartbeat:
		var drops rtps.Counter

		beat, err := rtps.DecodeHeartbeat(
			sub.Body,
			sub.Flags,
			&drops,
		)
		if err != nil {
			return false
		}

		if !isUserWriterEntity(beat.WriterID) {
			return false
		}

		b.handleUserHeartbeat(ctx, header, sub)

		return true
	case rtps.SubmessageGap:
		var drops rtps.Counter

		gap, err := rtps.DecodeGap(
			sub.Body,
			sub.Flags,
			&drops,
		)
		if err != nil {
			return false
		}

		if !isUserWriterEntity(gap.WriterID) {
			return false
		}

		b.handleUserGap(sub, header)

		return true
	case rtps.SubmessageDataFrag:
		return b.handleDataFrag(ctx, header, sub, dataCh)
	default:
		return false
	}
}
