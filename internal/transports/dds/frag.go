// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// Fragment reassembly for DATA_FRAG on the user path.
//
// Scope is minimal repair for large samples that do not fit one DATA
// submessage. Fragments key by writer GUID plus sequence number and collect
// by fragment number. Total size bounds by maxPayloadLen with explicit error
// plus drop counter. Incomplete entries expire by sweep with no goroutine.
// Complete payloads deliver to dataCh as one dataItem so ordering still uses
// DataReceiver. Incomplete entries drop plus count without blocking.
//
// Send side splits samples above the single DATA limit into ordered
// fragments with the same bounds. Each fragment carries its one based number
// plus the total so receivers reassemble in any arrival order.

// fragMaxEntries bounds concurrent in progress samples.
const fragMaxEntries = 16

// fragReassemblyTimeout bounds one in progress sample lifetime.
const fragReassemblyTimeout = 10 * time.Second

// fragMaxCount bounds total fragments for one sample.
const fragMaxCount = 131072

// fragSingleMax bounds one RTPS payload that fits in a single DATA submessage.
// DATA body is 20 bytes fixed plus payload, capped at 65535.
const fragSingleMax = 65535 - 20

// fragSendSize is the nominal fragment payload size for DATA_FRAG sends.
// It fits in one DATA_FRAG body with margin under the 65535 submessage cap
// and under the 65507 datagram cap, and keeps a 32MiB sample at 2048
// fragments, well under fragMaxCount.
const fragSendSize uint16 = 16384

// fragKey names one fragmented sample.
type fragKey struct {
	guid WriterGUID
	seq  uint64
}

// fragEntry holds one in progress sample.
type fragEntry struct {
	sampleSize    uint32
	fragmentSize  uint16
	total         uint32
	buf           []byte
	received      []bool
	receivedCount uint32
	firstSeen     time.Time
}

// fragReassembler collects fragments until complete.
//
// It is safe for concurrent use. The mutex must not be copied after first use.
type fragReassembler struct {
	mu         sync.Mutex
	entries    map[fragKey]*fragEntry
	maxEntries int
	timeout    time.Duration
	now        func() time.Time
}

// newFragReassembler returns an empty reassembler with the given clock.
func newFragReassembler(now func() time.Time) *fragReassembler {
	if now == nil {
		now = time.Now
	}
	return &fragReassembler{
		entries:    make(map[fragKey]*fragEntry),
		maxEntries: fragMaxEntries,
		timeout:    fragReassemblyTimeout,
		now:        now,
	}
}

// sweep deletes expired entries and returns the dropped count.
func (f *fragReassembler) sweep() int {
	if f == nil {
		return 0
	}
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

// checkFragBounds validates sample size and total count.
func checkFragBounds(sampleSize uint32, fragmentSize uint16) (uint32, error) {
	if fragmentSize == 0 {
		return 0, fmt.Errorf("dds: datafrag fragment size 0: %w", ErrDataFragDropped)
	}
	if uint64(sampleSize) > uint64(maxPayloadLen) {
		return 0, fmt.Errorf("dds: datafrag sample %d above cap %d: %w", sampleSize, maxPayloadLen, ErrDataFragDropped)
	}
	total64 := (uint64(sampleSize) + uint64(fragmentSize) - 1) / uint64(fragmentSize)
	if total64 == 0 || total64 > fragMaxCount {
		return 0, fmt.Errorf("dds: datafrag total %d out of range: %w", total64, ErrDataFragDropped)
	}
	return uint32(total64), nil
}

// fragNeedsSplit reports whether sampleSize needs fragmentation.
func fragNeedsSplit(sampleSize uint32) bool {
	return uint64(sampleSize) > uint64(fragSingleMax)
}

// planFragSend validates sampleSize and returns fragmentSize plus total.
func planFragSend(sampleSize uint32) (uint16, uint32, error) {
	total, err := checkFragBounds(sampleSize, fragSendSize)
	if err != nil {
		return 0, 0, err
	}
	return fragSendSize, total, nil
}

// fragSlice returns the subslice for fragNum without copying.
//
// fragNum is one based and must not exceed total. The returned slice aliases
// sample; callers must not mutate sample until fragments are sent and sinks
// must copy when retaining beyond the call.
func fragSlice(sample []byte, fragNum, total uint32, fragmentSize uint16) []byte {
	off := uint64(fragNum-1) * uint64(fragmentSize)
	if off >= uint64(len(sample)) {
		return nil
	}
	end := off + uint64(fragmentSize)
	if fragNum == total {
		end = uint64(len(sample))
	}
	if end > uint64(len(sample)) {
		end = uint64(len(sample))
	}
	return sample[off:end]
}

// encodeFragBody builds one DATA_FRAG body in little endian order.
//
// The body carries one fragment run with FragmentsInSubmessage set to 1,
// starting at start, with nominal fragmentSize and total sampleSize plus the
// fragment payload bytes. Reader and writer name the endpoints, seq is the
// sample sequence, and payload holds exactly the fragment octets.
// octetsToInlineQoS is 28 (4+4+8+4+2+2+4) for DATA_FRAG: reader plus writer
// plus sequence plus start plus count plus fragment size plus sample size.
// DATA uses 16, but reuse here shifts strict peers by 12 bytes.
func encodeFragBody(reader, writer rtps.EntityID, seq uint64, start uint32, fragmentSize uint16, sampleSize uint32, payload []byte) []byte {
	out := make([]byte, 0, 32+len(payload))
	out = append(out, 0x00, 0x00)
	var u16 [2]byte
	binary.LittleEndian.PutUint16(u16[:], 28)
	out = append(out, u16[:]...)
	out = append(out, reader[:]...)
	out = append(out, writer[:]...)
	var seqBuf [8]byte
	binary.LittleEndian.PutUint32(seqBuf[0:4], uint32(seq>>32))
	binary.LittleEndian.PutUint32(seqBuf[4:8], uint32(seq)) //nolint:gosec // G115: low word truncation is the RTPS layout
	out = append(out, seqBuf[:]...)
	var u32 [4]byte
	binary.LittleEndian.PutUint32(u32[:], start)
	out = append(out, u32[:]...)
	binary.LittleEndian.PutUint16(u16[:], 1)
	out = append(out, u16[:]...)
	binary.LittleEndian.PutUint16(u16[:], fragmentSize)
	out = append(out, u16[:]...)
	binary.LittleEndian.PutUint32(u32[:], sampleSize)
	out = append(out, u32[:]...)
	out = append(out, payload...)
	return out
}

// makeFragEntry allocates one entry for guid and seq.
func makeFragEntry(sampleSize uint32, fragmentSize uint16, total uint32, now time.Time) *fragEntry {
	return &fragEntry{
		sampleSize:   sampleSize,
		fragmentSize: fragmentSize,
		total:        total,
		buf:          make([]byte, sampleSize),
		received:     make([]bool, total),
		firstSeen:    now,
	}
}

// fragSliceLen returns the octet length for fragment number fragNum.
func fragSliceLen(fragNum, total uint32, fragmentSize uint16, sampleSize uint32) uint64 {
	if fragNum == total {
		rem := uint64(sampleSize) - uint64(total-1)*uint64(fragmentSize)
		return rem
	}
	return uint64(fragmentSize)
}

// copyFragRun copies one submessage run into the entry buffer.
func copyFragRun(entry *fragEntry, frag rtps.DataFrag) error {
	offInPayload := uint64(0)
	for i := range uint64(frag.FragmentsInSubmessage) {
		fragNum := uint64(frag.FragmentStartingNum) + i
		idx := fragNum - 1
		wantLen := fragSliceLen(uint32(fragNum), entry.total, entry.fragmentSize, entry.sampleSize) //nolint:gosec // G115: fragNum bounds by total fragments above
		if offInPayload+wantLen > uint64(len(frag.Payload)) {
			return fmt.Errorf("dds: datafrag run truncated: %w", ErrDataFragDropped)
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

// add stores one fragment run and returns the complete payload when done.
func (f *fragReassembler) add(guid WriterGUID, frag rtps.DataFrag) ([]byte, bool, error) {
	if f == nil {
		return nil, false, fmt.Errorf("dds: datafrag without reassembler: %w", ErrDataFragDropped)
	}
	total, err := checkFragBounds(frag.SampleSize, frag.FragmentSize)
	if err != nil {
		return nil, false, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := fragKey{guid: guid, seq: frag.SequenceNumber}
	entry, seen := f.entries[key]
	if !seen {
		if len(f.entries) >= f.maxEntries {
			return nil, false, fmt.Errorf("dds: datafrag table full: %w", ErrDataFragDropped)
		}
		entry = makeFragEntry(frag.SampleSize, frag.FragmentSize, total, f.now())
		f.entries[key] = entry
	} else if entry.sampleSize != frag.SampleSize || entry.fragmentSize != frag.FragmentSize {
		return nil, false, fmt.Errorf("dds: datafrag inconsistent sample: %w", ErrDataFragDropped)
	}
	err = copyFragRun(entry, frag)
	if err != nil {
		return nil, false, err
	}
	if entry.receivedCount != entry.total {
		return nil, false, nil
	}
	out := append([]byte(nil), entry.buf...)
	delete(f.entries, key)
	return out, true, nil
}

// handleDataFrag reassembles one DATA_FRAG and enqueues the complete payload.
func (b *Backend) handleDataFrag(ctx context.Context, header rtps.Header, sub rtps.RawSubmessage, dataCh chan dataItem) bool {
	err := ctx.Err()
	if err != nil {
		return true
	}
	frag, err := rtps.DecodeDataFrag(sub.Body, sub.Flags, nil)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux datafrag decode: %v", err)
		return true
	}
	if !isUserWriterEntity(frag.WriterID) {
		return true
	}
	if frag.SequenceNumber == 0 {
		b.bumpDemuxError()
		log.Printf("dds: demux datafrag sequence 0")
		return true
	}
	if b.fragReasm == nil {
		b.bumpDemuxError()
		log.Printf("dds: demux datafrag dropped %d bytes", len(sub.Body))
		return true
	}
	expired := b.fragReasm.sweep()
	for range expired {
		b.bumpDemuxError()
	}
	var guid WriterGUID
	copy(guid[:12], header.Prefix[:])
	copy(guid[12:16], frag.WriterID[:])
	complete, done, err := b.fragReasm.add(guid, frag)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux datafrag store: %v", err)
		return true
	}
	if !done {
		return true
	}
	b.noteRemoteData(guid, frag.SequenceNumber)
	cp := append([]byte(nil), complete...)
	enqueueBestEffort(ctx, dataCh, dataItem{guid: guid, seq: frag.SequenceNumber, payload: cp})
	return true
}
