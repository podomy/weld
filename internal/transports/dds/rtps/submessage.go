// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rtps

import (
	"errors"
	"fmt"
)

// Submessage ids carried on this wire. Only the kinds listed here have
// typed decoders; every other id skips by length in DecodeSubmessage and
// is never fatal.
const (
	// SubmessagePad is alignment with no meaning.
	SubmessagePad uint8 = 0x01
	// SubmessageAckNack is a reader to writer receipt. Decoded on the user
	// repair path and encoded for sent receipts.
	SubmessageAckNack uint8 = 0x06
	// SubmessageHeartbeat announces writer sequence bounds. Decoded for
	// ACK replies and encoded for history and repair heartbeats.
	SubmessageHeartbeat uint8 = 0x07
	// SubmessageGap describes skipped sequence numbers. Decoded for skip
	// handling and encoded for gap repairs.
	SubmessageGap uint8 = 0x08
	// SubmessageInfoTS carries a sample timestamp.
	SubmessageInfoTS uint8 = 0x09
	// SubmessageInfoSrc carries source version and flags. Decoded by
	// DecodeInfoSrc into version plus vendor; malformed bodies fail.
	SubmessageInfoSrc uint8 = 0x0c
	// SubmessageInfoReplyIP4 carries reply locators. Decoded by
	// DecodeInfoReplyIP4 with fixed width validation per 8.3.8.11;
	// malformed bodies fail.
	SubmessageInfoReplyIP4 uint8 = 0x0d
	// SubmessageInfoDst selects the message destination. Decoded by
	// DecodeInfoDst with 0 or 12 byte width validation per 8.3.8.13;
	// malformed bodies fail.
	SubmessageInfoDst uint8 = 0x0e
	// SubmessageInfoReply carries reply locators. Decoded by
	// DecodeInfoReply with locator multiple validation per 8.3.8.11;
	// malformed bodies fail.
	SubmessageInfoReply uint8 = 0x0f
	// SubmessageNackFrag is a fragment receipt. Decoded by DecodeNackFrag
	// into fragment repair bounds; see NackFrag for the caller handoff.
	SubmessageNackFrag uint8 = 0x12
	// SubmessageHeartbeatFrag announces fragment bounds. Decoded by
	// DecodeHeartbeatFrag into fragment availability bounds.
	SubmessageHeartbeatFrag uint8 = 0x13
	// SubmessageData carries a generic byte sample.
	SubmessageData uint8 = 0x15
	// SubmessageDataFrag carries a sample fragment. Decoded by DecodeDataFrag.
	SubmessageDataFrag uint8 = 0x16
)

// Submessage flag bits for the kinds decoded here.
const (
	// DataFlagInlineQoS marks inline QoS parameters present in DATA.
	DataFlagInlineQoS = 0x02
	// DataFlagDataPresent marks a serialized payload in DATA.
	DataFlagDataPresent = 0x04
	// DataFlagKeyPresent marks a serialized key in DATA.
	DataFlagKeyPresent = 0x08
	// DataFragFlagInlineQoS marks inline QoS parameters present in DATA_FRAG.
	DataFragFlagInlineQoS = 0x02
	// DataFragFlagKeyPresent marks a serialized key in DATA_FRAG.
	DataFragFlagKeyPresent = 0x04
	// InfoTSFlagInvalidate marks the timestamp absent in INFO_TS.
	InfoTSFlagInvalidate = 0x02
	// HeartbeatFlagFinal marks no further heartbeat expected.
	HeartbeatFlagFinal = 0x02
	// AckNackFlagFinal marks a pure ACK with no NACK needed.
	AckNackFlagFinal = 0x02
	// HeartbeatFlagLiveliness marks a manual liveliness assertion.
	HeartbeatFlagLiveliness = 0x04
	// GapFlagGroupInfo marks group sequence fields present in GAP per
	// DDSI-RTPS 2.5 8.3.8.4 table 24: value 0x02.
	GapFlagGroupInfo = 0x02
	// GapFlagFilteredCount marks filteredCount present in GAP per
	// DDSI-RTPS 2.5 8.3.8.4 table 24: value 0x04. The field is a
	// ChangeCount_t of 8 bytes per 8.3.5.11: 4 byte high plus 4 byte low.
	GapFlagFilteredCount = 0x04
	// InfoReplyIP4FlagMulticast marks a multicast locator present in
	// INFO_REPLY_IP4 per 8.3.8.11.
	InfoReplyIP4FlagMulticast = 0x02
	// InfoReplyFlagMulticast marks multicast locators present in
	// INFO_REPLY per 8.3.8.11.
	InfoReplyFlagMulticast = 0x02
)

// DATA content sizes on the wire.
const (
	// dataFixedSize is extraFlags plus offsets plus ids plus sequence.
	dataFixedSize = dataReservedSize + dataOffsetsSize + entityIDSize + entityIDSize + dataSeqSize
	// octetsToInlineQoS is the standard offset from readerId to inline QoS.
	// Real peers send 16 with and without inline QoS; decode ignores the
	// field and parses sequentially, so any peer value is tolerated.
	octetsToInlineQoS = 16
	// heartbeatSize is the fixed HEARTBEAT content size.
	heartbeatSize = entityIDSize + entityIDSize + dataSeqSize + dataSeqSize + wordSize
	// gapMinSize is readerId plus writerId plus gap start.
	gapMinSize = entityIDSize + entityIDSize + gapStartSize
	// infoTSSize is the fixed INFO_TS content size when valid.
	infoTSSize = 8
	// parameterHeaderSize is one parameter id plus length.
	parameterHeaderSize = 4
	// paramIDSize is one parameter id size.
	paramIDSize = 2
	// paramLenSize is one parameter length size.
	paramLenSize = 2
	// padByte fills parameter alignment.
	padByte = 0x00
	// dataReservedSize is the extra flags size in DATA.
	dataReservedSize = 2
	// dataOffsetsSize is the octets to inline QoS size in DATA.
	dataOffsetsSize = 2
	// dataReaderOff is the reader id offset in DATA.
	dataReaderOff = 4
	// dataWriterOff is the writer id offset in DATA.
	dataWriterOff = 8
	// dataSeqOff is the sequence number offset in DATA.
	dataSeqOff = 12
	// dataSeqSize is the sequence number size in DATA.
	dataSeqSize = 8
	// entityIDSize is one entity id size.
	entityIDSize = 4
	// infoTSSecondsOff is the seconds offset in INFO_TS.
	infoTSSecondsOff = 0
	// infoTSFractionOff is the fraction offset in INFO_TS.
	infoTSFractionOff = 4
	// infoSrcSize is the minimal INFO_SRC content size per 8.3.8.10:
	// reserved plus major plus minor plus 2 byte vendor id.
	infoSrcSize = 5
	// infoSrcReservedOff is the reserved offset in INFO_SRC.
	infoSrcReservedOff = 0
	// infoSrcMajorOff is the version major offset in INFO_SRC.
	infoSrcMajorOff = 1
	// infoSrcMinorOff is the version minor offset in INFO_SRC.
	infoSrcMinorOff = 2
	// infoSrcVendorOff is the vendor id offset in INFO_SRC.
	infoSrcVendorOff = 3
	// infoSrcVendorLen is the 2 byte vendor id length per 8.3.8.10.
	infoSrcVendorLen = 2
	// wordSize is one 4 byte word size.
	wordSize = 4
	// seqSetBaseSize is one sequence base size.
	seqSetBaseSize = 8
	// ackNackFixedSize is reader plus writer plus base plus bits plus count.
	ackNackFixedSize = entityIDSize + entityIDSize + seqSetBaseSize + wordSize + wordSize
	// gapReaderOff is the reader id offset in GAP and HEARTBEAT.
	gapReaderOff = 0
	// gapWriterOff is the writer id offset in GAP and HEARTBEAT.
	gapWriterOff = 4
	// gapStartOff is the gap start offset in GAP.
	gapStartOff = 8
	// gapStartSize is the gap start size in GAP.
	gapStartSize = 8
	// heartbeatFirstOff is the first sequence offset in HEARTBEAT.
	heartbeatFirstOff = 8
	// heartbeatLastOff is the last sequence offset in HEARTBEAT.
	heartbeatLastOff = 16
	// heartbeatCountOff is the count offset in HEARTBEAT.
	heartbeatCountOff = 24
	// dataFragFixedSize is extraFlags plus offsets plus ids plus seq plus frag fields.
	dataFragFixedSize = dataReservedSize + dataOffsetsSize + entityIDSize + entityIDSize + dataSeqSize + fragStartSize + fragCountSize + fragSizeFieldSize + sampleSizeFieldSize
	// dataFragReaderOff is the reader id offset in DATA_FRAG.
	dataFragReaderOff = 4
	// dataFragWriterOff is the writer id offset in DATA_FRAG.
	dataFragWriterOff = 8
	// dataFragSeqOff is the sequence number offset in DATA_FRAG.
	dataFragSeqOff = 12
	// dataFragStartOff is the fragment starting number offset in DATA_FRAG.
	dataFragStartOff = 20
	// dataFragCountOff is the fragments in submessage offset in DATA_FRAG.
	dataFragCountOff = 24
	// dataFragSizeOff is the fragment size offset in DATA_FRAG.
	dataFragSizeOff = 26
	// dataFragSampleOff is the sample size offset in DATA_FRAG.
	dataFragSampleOff = 28
	// fragStartSize is the fragment starting number size.
	fragStartSize = 4
	// fragCountSize is the fragments in submessage size.
	fragCountSize = 2
	// fragSizeFieldSize is the fragment size field size.
	fragSizeFieldSize = 2
	// sampleSizeFieldSize is the sample size field size.
	sampleSizeFieldSize = 4
	// heartbeatFragSize is the fixed HEARTBEAT_FRAG content size.
	heartbeatFragSize = entityIDSize + entityIDSize + dataSeqSize + fragStartSize + wordSize
	// nackFragMinSize is reader plus writer plus writerSN plus base plus bits plus count.
	nackFragMinSize = entityIDSize + entityIDSize + dataSeqSize + fragStartSize + wordSize + wordSize
	// nackFragBaseSize is one fragment base size.
	nackFragBaseSize = 4
	// gapGroupSize is gapStartGSN plus gapEndGSN when G is set.
	gapGroupSize = 16
	// gapFilteredSize is the ChangeCount filteredCount size when F is set.
	gapFilteredSize = 8
	// seqSetMaxBits caps one SequenceNumberSet and FragmentNumberSet.
	seqSetMaxBits = 256
)

// EntityID is the 4 octet endpoint suffix of a DDS GUID: 3 octet key plus
// 1 octet kind. Ids are opaque bytes copied verbatim, never byte swapped.
// Combined with the RTPS header GUID prefix they name one endpoint.
type EntityID [4]byte

// Well known entity ids used on first contact paths.
var (
	// EntityUnknown is the zero reader id for anonymous subscriptions.
	EntityUnknown = EntityID{0x00, 0x00, 0x00, 0x00}
	// EntityParticipant is the participant entity kind.
	EntityParticipant = EntityID{0x00, 0x00, 0x01, 0xC1}
	// EntitySPDPWriter is the builtin SPDP announcement writer.
	EntitySPDPWriter = EntityID{0x00, 0x01, 0x00, 0xC2}
	// EntitySPDPReader is the builtin SPDP announcement reader.
	EntitySPDPReader = EntityID{0x00, 0x01, 0x00, 0xC7}
)

// ParamSentinel terminates an inline QoS parameter list.
const ParamSentinel uint16 = 0x0001

// Parameter is one inline QoS entry: id plus opaque value bytes. Unknown
// ids are preserved on decode and skipped by readers that do not know
// them, so new QoS keys never break old peers.
type Parameter struct {
	// ID is the parameter key, for example a PID value.
	ID uint16
	// Value holds the parameter bytes without padding.
	Value []byte
}

// padTo4 returns the zero pad needed to reach a 4 byte boundary.
func padTo4(n int) int {
	return (4 - (n % 4)) % 4
}

// appendParamEntry appends one id plus length plus value plus zero pad.
// Callers validate length fits uint16 before calling.
func appendParamEntry(out []byte, id uint16, value []byte, littleEndian bool) []byte {
	var hdr [parameterHeaderSize]byte
	putUint16(hdr[0:paramIDSize], id, littleEndian)
	putUint16(hdr[paramIDSize:paramIDSize+paramLenSize], uint16(len(value)), littleEndian) //nolint:gosec // G115: callers guard length by maxSubmessageBody
	out = append(out, hdr[:]...)
	out = append(out, value...)
	for range padTo4(len(value)) {
		out = append(out, padByte)
	}
	return out
}

// appendParamSentinel appends a terminating sentinel with zero length.
func appendParamSentinel(out []byte, sentinel uint16, littleEndian bool) []byte {
	var term [parameterHeaderSize]byte
	putUint16(term[0:paramIDSize], sentinel, littleEndian)
	putUint16(term[paramIDSize:paramIDSize+paramLenSize], 0, littleEndian)
	return append(out, term[:]...)
}

// paramHeaderAt reads one id and length at off. It reports false when the
// header is truncated, leaving error text to the caller.
func paramHeaderAt(body []byte, off int, littleEndian bool) (uint16, int, bool) {
	if len(body)-off < parameterHeaderSize {
		return 0, 0, false
	}
	id := uint16Value(body[off:off+paramIDSize], littleEndian)
	length := int(uint16Value(body[off+paramIDSize:off+paramIDSize+paramLenSize], littleEndian))
	return id, length, true
}

// seqSigned reinterprets a wire sequence as signed per 8.3.5.4.
func seqSigned(v uint64) int64 {
	return int64(v) //nolint:gosec // G115: intentional two's-complement reinterpret from the wire
}

// encodeParameters renders params plus a terminating sentinel. Values are
// padded with zeros to 4 bytes per the RTPS parameter alignment.
func encodeParameters(params []Parameter, littleEndian bool) ([]byte, error) {
	out := make([]byte, 0, len(params)*(parameterHeaderSize+4)+parameterHeaderSize)
	for i := range params {
		if len(params[i].Value) > maxSubmessageBody {
			return nil, fmt.Errorf("rtps: parameter %#04x value %d above %d (8.3.5.9)", params[i].ID, len(params[i].Value), maxSubmessageBody)
		}
		out = appendParamEntry(out, params[i].ID, params[i].Value, littleEndian)
	}
	out = appendParamSentinel(out, ParamSentinel, littleEndian)
	return out, nil
}

// decodeParameters parses a parameter list ending at the sentinel and
// returns the entries with the consumed byte count per DDSI-RTPS 2.5
// 8.3.5.9. Unknown ids are kept so callers can skip what they do not
// understand. Entry count stays capped at maxParamsPerList so a crafted
// list cannot force unbounded allocation.
func decodeParameters(body []byte, littleEndian bool) ([]Parameter, int, error) {
	var params []Parameter
	off := 0
	for {
		if len(params) >= maxParamsPerList {
			return nil, 0, fmt.Errorf("rtps: parameter count above %d (8.3.5.9)", maxParamsPerList)
		}
		id, length, ok := paramHeaderAt(body, off, littleEndian)
		if !ok {
			return nil, 0, fmt.Errorf("rtps: parameter header need %d have %d (8.3.5.9)", parameterHeaderSize, len(body)-off)
		}
		off += parameterHeaderSize
		if id == ParamSentinel {
			return decodeParameterSentinel(params, off, length)
		}
		value, next, err := copyParameterValue(body, off, length, id)
		if err != nil {
			return nil, 0, err
		}
		params = append(params, Parameter{ID: id, Value: value})
		off = next
	}
}

// checkSentinelLength validates a terminating sentinel length.
func checkSentinelLength(length int) error {
	if length != 0 {
		return fmt.Errorf("rtps: sentinel carries %d bytes (8.3.5.9)", length)
	}
	return nil
}

// copyParamBytes copies length bytes at off and skips alignment pad.
// It returns the copied value and the offset past the pad.
func copyParamBytes(body []byte, off, length int) ([]byte, int, error) {
	if len(body)-off < length {
		return nil, 0, fmt.Errorf("rtps: param value need %d have %d (8.3.5.9)", length, len(body)-off)
	}
	value := make([]byte, length)
	copy(value, body[off:off+length])
	off += length
	pad := padTo4(length)
	if len(body)-off < pad {
		return nil, 0, fmt.Errorf("rtps: param pad need %d have %d (8.3.5.9)", pad, len(body)-off)
	}
	return value, off + pad, nil
}

// decodeParameterSentinel validates the terminating sentinel.
func decodeParameterSentinel(params []Parameter, off, length int) ([]Parameter, int, error) {
	err := checkSentinelLength(length)
	if err != nil {
		return nil, 0, err
	}
	return params, off, nil
}

// copyParameterValue copies one value plus its alignment pad.
func copyParameterValue(body []byte, off, length int, id uint16) ([]byte, int, error) {
	value, next, err := copyParamBytes(body, off, length)
	if err != nil {
		return nil, 0, fmt.Errorf("rtps: parameter %#04x (8.3.5.9): %w", id, err)
	}
	return value, next, nil
}

// Data is a decoded DATA submessage: the generic byte sample plus its
// routing metadata. Payload stays opaque XCDR1 bytes including the
// encapsulation header; v1 never interprets type specific content.
type Data struct {
	// ReaderID names the destination entity, zero for anonymous.
	ReaderID EntityID
	// WriterID names the source entity.
	WriterID EntityID
	// SequenceNumber is the writer sequence starting at 1.
	SequenceNumber uint64
	// InlineQoS carries skip tolerant endpoint parameters.
	InlineQoS []Parameter
	// Payload holds the opaque serialized sample, aliasing the decoded
	// body owned by this struct. Nil means no sample was present.
	Payload []byte
	// Key reports the Key flag instead of the Data flag. Either flag
	// stores its bytes in Payload; only the flag differs.
	Key bool
}

// dataFlags computes the DATA submessage flags for an endianness.
func (d Data) dataFlags(littleEndian bool) uint8 {
	var flags uint8
	if littleEndian {
		flags |= FlagLittleEndian
	}
	if len(d.InlineQoS) > 0 {
		flags |= DataFlagInlineQoS
	}
	if d.Key {
		flags |= DataFlagKeyPresent
	} else {
		flags |= DataFlagDataPresent
	}
	return flags
}

// encodeDataInline renders inline QoS when present, else nil.
func encodeDataInline(params []Parameter, littleEndian bool) ([]byte, error) {
	if len(params) == 0 {
		return nil, nil
	}
	inline, err := encodeParameters(params, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("rtps: encode data inline qos: %w (8.3.8.2)", err)
	}
	return inline, nil
}

// appendDataFixed appends the 20 byte DATA prefix without inline QoS.
func appendDataFixed(out []byte, d Data, littleEndian bool) []byte {
	var reserved [dataReservedSize]byte
	out = append(out, reserved[:]...)
	var offsets [dataOffsetsSize]byte
	putUint16(offsets[:], octetsToInlineQoS, littleEndian)
	out = append(out, offsets[:]...)
	out = append(out, d.ReaderID[:]...)
	out = append(out, d.WriterID[:]...)
	var seq [dataSeqSize]byte
	putSeqNumber(seq[:], d.SequenceNumber, littleEndian)
	return append(out, seq[:]...)
}

// Body renders the DATA content without the submessage header. Inline QoS
// parameters plus the terminating sentinel are present only when the
// struct carries entries; a bare sample encodes no parameter list.
func (d Data) Body(littleEndian bool) ([]byte, error) {
	inline, err := encodeDataInline(d.InlineQoS, littleEndian)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, dataFixedSize+len(inline)+len(d.Payload))
	out = appendDataFixed(out, d, littleEndian)
	out = append(out, inline...)
	out = append(out, d.Payload...)
	if len(out) > maxSubmessageBody {
		return nil, fmt.Errorf("rtps: data body %d above %d (8.3.8.2)", len(out), maxSubmessageBody)
	}
	return out, nil
}

// Submessage renders d as a framed DATA submessage with header.
func (d Data) Submessage(littleEndian bool) (RawSubmessage, error) {
	flags := d.dataFlags(littleEndian)
	body, err := d.Body(littleEndian)
	if err != nil {
		return RawSubmessage{}, fmt.Errorf("rtps: data submessage: %w (8.3.8.2)", err)
	}
	return RawSubmessage{ID: SubmessageData, Flags: flags, Body: body}, nil
}

// decodeDataFixed copies routing ids and sequence from a DATA body.
func decodeDataFixed(body []byte, littleEndian bool, out *Data) {
	copy(out.ReaderID[:], body[dataReaderOff:dataReaderOff+entityIDSize])
	copy(out.WriterID[:], body[dataWriterOff:dataWriterOff+entityIDSize])
	out.SequenceNumber = seqNumberValue(body[dataSeqOff:dataSeqOff+dataSeqSize], littleEndian)
}

// decodeDataInline parses inline QoS when the flag is set.
func decodeDataInline(rest []byte, littleEndian bool, drops *Counter) ([]Parameter, []byte, error) {
	params, consumed, err := decodeParameters(rest, littleEndian)
	if err != nil {
		drops.RecordDrop()
		return nil, nil, fmt.Errorf("rtps: decode data inline qos (8.3.8.2): %w", err)
	}
	return params, rest[consumed:], nil
}

// checkDataFlags rejects contradictory Data plus Key flags.
func checkDataFlags(flags uint8, drops *Counter) error {
	if flags&(DataFlagDataPresent|DataFlagKeyPresent) == DataFlagDataPresent|DataFlagKeyPresent {
		drops.RecordDrop()
		return errors.New("rtps: data sets both data and key flags (8.3.8.2)") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// checkDataSize rejects truncated DATA bodies.
func checkDataSize(body []byte, drops *Counter) error {
	if len(body) < dataFixedSize {
		drops.RecordDrop()
		return fmt.Errorf("rtps: data need %d have %d (8.3.8.2)", dataFixedSize, len(body))
	}
	return nil
}

// DecodeData parses a DATA body with its header flags per DDSI-RTPS 2.5
// 8.3.8.2. Extra flags and the inline QoS offset are ignored for
// tolerance; parsing is sequential. Contradictory Data plus Key flags
// drop loudly. A DATA with neither D nor K carries no payload: any
// trailing bytes past the fixed prefix and inline QoS drop loudly as
// malformed per 8.3.8.2. Payload aliases the owned body slice. Zero and
// negative writer sequences drop loudly as invalid per 8.3.8.2, matching
// DecodeDataFrag strictness.
func DecodeData(body []byte, flags uint8, drops *Counter) (Data, error) {
	var out Data
	err := checkDataSize(body, drops)
	if err != nil {
		return out, err
	}
	err = checkDataFlags(flags, drops)
	if err != nil {
		return out, err
	}
	littleEndian := LittleEndian(flags)
	decodeDataFixed(body, littleEndian, &out)
	if seqSigned(out.SequenceNumber) <= 0 {
		drops.RecordDrop()
		return Data{}, fmt.Errorf("rtps: data sequence %d not positive (8.3.8.2)", seqSigned(out.SequenceNumber))
	}
	rest := body[dataFixedSize:]
	if flags&DataFlagInlineQoS != 0 {
		params, remaining, err := decodeDataInline(rest, littleEndian, drops)
		if err != nil {
			return Data{}, err
		}
		out.InlineQoS = params
		rest = remaining
	}
	out.Key = flags&DataFlagKeyPresent != 0
	if flags&(DataFlagDataPresent|DataFlagKeyPresent) != 0 {
		out.Payload = rest
		return out, nil
	}
	if len(rest) != 0 {
		drops.RecordDrop()
		return Data{}, fmt.Errorf("rtps: data trailing %d without data or key flag (8.3.8.2)", len(rest))
	}
	return out, nil
}

// DataFrag is a decoded DATA_FRAG submessage: one run of consecutive
// fragments for a single sample sequence number.
type DataFrag struct {
	// ReaderID names the destination entity, zero for anonymous.
	ReaderID EntityID
	// WriterID names the source entity.
	WriterID EntityID
	// SequenceNumber is the writer sequence starting at 1.
	SequenceNumber uint64
	// FragmentStartingNum is the one based number of the first fragment in this submessage.
	FragmentStartingNum uint32
	// FragmentsInSubmessage counts consecutive fragments carried here.
	FragmentsInSubmessage uint16
	// FragmentSize is the octet size per fragment, except the last fragment of the sample.
	FragmentSize uint16
	// SampleSize is the total octet size of the reassembled serialized payload.
	SampleSize uint32
	// InlineQoS carries skip tolerant endpoint parameters.
	InlineQoS []Parameter
	// Payload holds the concatenated fragments, aliasing the decoded body.
	Payload []byte
	// Key reports the Key flag. Either form stores bytes in Payload.
	Key bool
}

// checkDataFragSize rejects truncated DATA_FRAG bodies.
func checkDataFragSize(body []byte, drops *Counter) error {
	if len(body) < dataFragFixedSize {
		drops.RecordDrop()
		return fmt.Errorf("rtps: datafrag need %d have %d (8.3.8.3)", dataFragFixedSize, len(body))
	}
	return nil
}

// decodeDataFragFixed copies routing ids, sequence, and fragment geometry from a DATA_FRAG body.
func decodeDataFragFixed(body []byte, littleEndian bool, out *DataFrag) {
	copy(out.ReaderID[:], body[dataFragReaderOff:dataFragReaderOff+entityIDSize])
	copy(out.WriterID[:], body[dataFragWriterOff:dataFragWriterOff+entityIDSize])
	out.SequenceNumber = seqNumberValue(body[dataFragSeqOff:dataFragSeqOff+dataSeqSize], littleEndian)
	out.FragmentStartingNum = uint32Value(body[dataFragStartOff:dataFragStartOff+fragStartSize], littleEndian)
	out.FragmentsInSubmessage = uint16Value(body[dataFragCountOff:dataFragCountOff+fragCountSize], littleEndian)
	out.FragmentSize = uint16Value(body[dataFragSizeOff:dataFragSizeOff+fragSizeFieldSize], littleEndian)
	out.SampleSize = uint32Value(body[dataFragSampleOff:dataFragSampleOff+sampleSizeFieldSize], littleEndian)
}

// dataFragTotal computes total fragments for sampleSize split by fragmentSize.
func dataFragTotal(sampleSize uint32, fragmentSize uint16) uint64 {
	frag := uint64(fragmentSize)
	sample := uint64(sampleSize)
	return (sample + frag - 1) / frag
}

// checkDataFragSequence rejects zero, negative, and UNKNOWN writer sequences per 8.3.8.3.
func checkDataFragSequence(out DataFrag, drops *Counter) error {
	if seqSigned(out.SequenceNumber) <= 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: datafrag sequence %d not positive (8.3.8.3)", seqSigned(out.SequenceNumber))
	}
	return nil
}

// checkDataFragGeometry validates fragment numbers and sizes with bounded work.
func checkDataFragGeometry(out DataFrag, drops *Counter) error {
	if out.FragmentStartingNum == 0 {
		drops.RecordDrop()
		return errors.New("rtps: datafrag starting number 0 (8.3.8.3)") //nolint:wrapcheck // new error, nothing to wrap
	}
	if out.FragmentsInSubmessage == 0 {
		drops.RecordDrop()
		return errors.New("rtps: datafrag fragments in submessage 0 (8.3.8.3)") //nolint:wrapcheck // new error, nothing to wrap
	}
	if out.FragmentSize == 0 {
		drops.RecordDrop()
		return errors.New("rtps: datafrag fragment size 0 (8.3.8.3)") //nolint:wrapcheck // new error, nothing to wrap
	}
	if out.SampleSize == 0 {
		drops.RecordDrop()
		return errors.New("rtps: datafrag sample size 0 (8.3.8.3)") //nolint:wrapcheck // new error, nothing to wrap
	}
	total := dataFragTotal(out.SampleSize, out.FragmentSize)
	start := uint64(out.FragmentStartingNum)
	count := uint64(out.FragmentsInSubmessage)
	if start+count-1 > total {
		drops.RecordDrop()
		return fmt.Errorf("rtps: datafrag range %d plus %d above total %d (8.3.8.3)", start, count, total)
	}
	return nil
}

// checkDataFragPayload validates the fragment payload length against geometry.
func checkDataFragPayload(out DataFrag, payload []byte, drops *Counter) error {
	offset := (uint64(out.FragmentStartingNum) - 1) * uint64(out.FragmentSize)
	if offset >= uint64(out.SampleSize) {
		drops.RecordDrop()
		return fmt.Errorf("rtps: datafrag offset %d above sample %d (8.3.8.3)", offset, out.SampleSize)
	}
	remaining := uint64(out.SampleSize) - offset
	want := uint64(out.FragmentsInSubmessage) * uint64(out.FragmentSize)
	want = min(want, remaining)
	if uint64(len(payload)) != want {
		drops.RecordDrop()
		return fmt.Errorf("rtps: datafrag payload need %d have %d (8.3.8.3)", want, len(payload))
	}
	return nil
}

// DecodeDataFrag parses a DATA_FRAG body with its header flags.
func DecodeDataFrag(body []byte, flags uint8, drops *Counter) (DataFrag, error) {
	var out DataFrag
	err := checkDataFragSize(body, drops)
	if err != nil {
		return out, err
	}
	littleEndian := LittleEndian(flags)
	decodeDataFragFixed(body, littleEndian, &out)
	err = checkDataFragSequence(out, drops)
	if err != nil {
		return DataFrag{}, err
	}
	err = checkDataFragGeometry(out, drops)
	if err != nil {
		return DataFrag{}, err
	}
	rest := body[dataFragFixedSize:]
	if flags&DataFragFlagInlineQoS != 0 {
		params, remaining, err := decodeDataInline(rest, littleEndian, drops)
		if err != nil {
			return DataFrag{}, err
		}
		out.InlineQoS = params
		rest = remaining
	}
	err = checkDataFragPayload(out, rest, drops)
	if err != nil {
		return DataFrag{}, err
	}
	out.Payload = rest
	out.Key = flags&DataFragFlagKeyPresent != 0
	return out, nil
}

// InfoTS is a decoded INFO_TS submessage. Invalid means the sender
// withdrew the timestamp; Seconds and Fraction are then unset.
type InfoTS struct {
	// Valid reports a timestamp is present.
	Valid bool
	// Seconds is NTP time seconds.
	Seconds uint32
	// Fraction is NTP time fraction.
	Fraction uint32
}

// Flags computes the INFO_TS submessage flags for an endianness.
func (t InfoTS) Flags(littleEndian bool) uint8 {
	if !t.Valid {
		return InfoTSFlagInvalidate
	}
	if littleEndian {
		return FlagLittleEndian
	}
	return 0
}

// Encode renders the INFO_TS content: 8 bytes when valid, empty when the
// timestamp is invalidated.
func (t InfoTS) Encode(littleEndian bool) []byte {
	if !t.Valid {
		return nil
	}
	out := make([]byte, infoTSSize)
	putUint32(out[infoTSSecondsOff:infoTSSecondsOff+wordSize], t.Seconds, littleEndian)
	putUint32(out[infoTSFractionOff:infoTSFractionOff+wordSize], t.Fraction, littleEndian)
	return out
}

// DecodeInfoTS parses an INFO_TS body. Invalidate accepts any body and
// reports invalid, since no fields follow. A valid stamp needs exactly 8
// bytes: short bodies and trailing bytes both drop, since the content is
// fixed width and extra bytes indicate a malformed sender or a framing
// error.
func DecodeInfoTS(body []byte, flags uint8, drops *Counter) (InfoTS, error) {
	var out InfoTS
	if flags&InfoTSFlagInvalidate != 0 {
		return out, nil
	}
	if len(body) != infoTSSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: info_ts need %d have %d (8.3.8.9)", infoTSSize, len(body))
	}
	littleEndian := LittleEndian(flags)
	out.Valid = true
	out.Seconds = uint32Value(body[infoTSSecondsOff:infoTSSecondsOff+wordSize], littleEndian)
	out.Fraction = uint32Value(body[infoTSFractionOff:infoTSFractionOff+wordSize], littleEndian)
	return out, nil
}

// InfoSrc is a decoded INFO_SRC submessage per DDSI-RTPS 2.5 8.3.8.10.
// It carries the source protocol version plus the 2 byte vendor id.
// The body is exactly 5 bytes: reserved, major, minor, vendor[2].
// Short bodies, trailing bytes, nonzero reserved bytes, and non-2 major
// versions drop fail-closed. The minimal
// form carries no GUID prefix, so dispatch validates and logs without
// overriding GUID identity.
type InfoSrc struct {
	// Major is the source protocol major version, always 2.
	Major uint8
	// Minor is the source protocol minor version.
	Minor uint8
	// Vendor is the 2 byte source vendor id, never byte swapped.
	Vendor [2]byte
}

// Encode renders the 5 byte INFO_SRC content per 8.3.8.10.
func (s InfoSrc) Encode() []byte {
	out := make([]byte, infoSrcSize)
	out[infoSrcReservedOff] = 0
	out[infoSrcMajorOff] = s.Major
	out[infoSrcMinorOff] = s.Minor
	copy(out[infoSrcVendorOff:infoSrcVendorOff+infoSrcVendorLen], s.Vendor[:])
	return out
}

// checkInfoSrcReserved rejects nonzero reserved bytes fail-closed per 8.3.8.10.
func checkInfoSrcReserved(body []byte, drops *Counter) error {
	if body[infoSrcReservedOff] != 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: info_src reserved %#02x (8.3.8.10)", body[infoSrcReservedOff])
	}
	return nil
}

// checkInfoSrcVersion rejects non-2 major versions fail-closed per 8.3.8.10.
func checkInfoSrcVersion(body []byte, drops *Counter) error {
	if body[infoSrcMajorOff] != ProtocolMajor {
		drops.RecordDrop()
		return fmt.Errorf("rtps: info_src major %d want %d (8.3.8.10)", body[infoSrcMajorOff], ProtocolMajor)
	}
	return nil
}

// DecodeInfoSrc parses an INFO_SRC body per 8.3.8.10 minimal form.
// The body must be exactly infoSrcSize bytes: short bodies and trailing
// bytes both drop loudly with a counter increment, since the content is
// fixed width. Nonzero reserved bytes and non-2 major versions drop
// fail-closed. Callers invalidate the rest of the message
// on error per 8.3.4.1 rule 6.
func DecodeInfoSrc(body []byte, flags uint8, drops *Counter) (InfoSrc, error) {
	var out InfoSrc
	_ = flags
	if len(body) != infoSrcSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: info_src need %d have %d (8.3.8.10)", infoSrcSize, len(body))
	}
	err := checkInfoSrcReserved(body, drops)
	if err != nil {
		return InfoSrc{}, err
	}
	err = checkInfoSrcVersion(body, drops)
	if err != nil {
		return InfoSrc{}, err
	}
	out.Major = body[infoSrcMajorOff]
	out.Minor = body[infoSrcMinorOff]
	copy(out.Vendor[:], body[infoSrcVendorOff:infoSrcVendorOff+infoSrcVendorLen])
	return out, nil
}

// InfoDst sizes on the wire per 8.3.8.13.
const (
	// infoDstPrefixLen is the 12 byte GUID prefix length.
	infoDstPrefixLen = 12
	// infoReplyIP4UnicastLen is one IPv4 port plus address length.
	infoReplyIP4UnicastLen = 8
	// infoReplyIP4MulticastLen is unicast plus multicast locator length.
	infoReplyIP4MulticastLen = 16
	// infoReplyPortLen is one locator port field length.
	infoReplyPortLen = 4
	// infoReplyIP4AddrLen is one IPv4 address length.
	infoReplyIP4AddrLen = 4
)

// InfoDst is a decoded INFO_DST submessage per DDSI-RTPS 2.5 8.3.8.13.
// An empty body resets the destination; a 12 byte body names a GUID
// prefix. Any other length is malformed and drops fail-closed.
type InfoDst struct {
	// HasPrefix reports a 12 byte prefix is present.
	HasPrefix bool
	// Prefix is the destination GUID prefix when present.
	Prefix [12]byte
}

// DecodeInfoDst parses an INFO_DST body per 8.3.8.13. Empty bodies
// report absent; 12 byte bodies report the prefix; any other length
// drops with a counter increment.
func DecodeInfoDst(body []byte, flags uint8, drops *Counter) (InfoDst, error) {
	var out InfoDst
	_ = flags
	if len(body) == 0 {
		return out, nil
	}
	if len(body) != infoDstPrefixLen {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: info_dst need 0 or %d have %d (8.3.8.13)", infoDstPrefixLen, len(body))
	}
	copy(out.Prefix[:], body)
	out.HasPrefix = true
	return out, nil
}

// InfoReplyIP4 is a decoded INFO_REPLY_IP4 submessage per DDSI-RTPS 2.5
// 8.3.8.11. The body carries one unicast IPv4 locator (4 byte port in
// submessage endianness plus 4 byte IPv4 address in network order) and,
// when the multicast flag is set, one multicast locator of the same
// shape. IPv6 locators ride the generic INFO_REPLY as 24 byte locators.
type InfoReplyIP4 struct {
	// HasMulticast reports a multicast locator is present.
	HasMulticast bool
	// UnicastPort is the unicast port in host order.
	UnicastPort uint32
	// UnicastAddr is the unicast IPv4 address in network order.
	UnicastAddr [4]byte
	// MulticastPort is the multicast port when present.
	MulticastPort uint32
	// MulticastAddr is the multicast IPv4 address when present.
	MulticastAddr [4]byte
}

// checkInfoReplyIP4Flags rejects reserved flag bits fail-closed per 8.3.8.11.
func checkInfoReplyIP4Flags(flags uint8, drops *Counter) error {
	if flags&^(FlagLittleEndian|InfoReplyIP4FlagMulticast) != 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: info_reply_ip4 flags %#02x reserved (8.3.8.11)", flags)
	}
	return nil
}

// DecodeInfoReplyIP4 parses an INFO_REPLY_IP4 body per 8.3.8.11. Without
// the multicast flag the body must be 8 bytes; with the flag it must be
// 16 bytes. Any other length or reserved flag drops fail-closed.
func DecodeInfoReplyIP4(body []byte, flags uint8, drops *Counter) (InfoReplyIP4, error) {
	var out InfoReplyIP4
	err := checkInfoReplyIP4Flags(flags, drops)
	if err != nil {
		return InfoReplyIP4{}, err
	}
	littleEndian := LittleEndian(flags)
	wantMulticast := flags&InfoReplyIP4FlagMulticast != 0
	if wantMulticast {
		if len(body) != infoReplyIP4MulticastLen {
			drops.RecordDrop()
			return out, fmt.Errorf("rtps: info_reply_ip4 need %d with multicast have %d (8.3.8.11)", infoReplyIP4MulticastLen, len(body))
		}
	} else {
		if len(body) != infoReplyIP4UnicastLen {
			drops.RecordDrop()
			return out, fmt.Errorf("rtps: info_reply_ip4 need %d have %d (8.3.8.11)", infoReplyIP4UnicastLen, len(body))
		}
	}
	out.HasMulticast = wantMulticast
	out.UnicastPort = uint32Value(body[0:infoReplyPortLen], littleEndian)
	copy(out.UnicastAddr[:], body[infoReplyPortLen:infoReplyPortLen+infoReplyIP4AddrLen])
	if wantMulticast {
		off := infoReplyIP4UnicastLen
		out.MulticastPort = uint32Value(body[off:off+infoReplyPortLen], littleEndian)
		copy(out.MulticastAddr[:], body[off+infoReplyPortLen:off+infoReplyPortLen+infoReplyIP4AddrLen])
	}
	return out, nil
}

// InfoReply is a decoded INFO_REPLY submessage per DDSI-RTPS 2.5
// 8.3.8.11. The body carries a list of 24 byte locators (int32 kind
// plus uint32 port in submessage endianness plus 16 byte address).
// IPv6 locators use this generic form. An empty body carries no reply
// locators; any non-empty body must be a multiple of 24 bytes.
type InfoReply struct {
	// HasMulticast reports the multicast flag was set.
	HasMulticast bool
	// Locators holds one 24 byte copy per locator in wire order.
	Locators [][]byte
}

// checkInfoReplyFlags rejects reserved flag bits fail-closed per 8.3.8.11.
func checkInfoReplyFlags(flags uint8, drops *Counter) error {
	if flags&^(FlagLittleEndian|InfoReplyFlagMulticast) != 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: info_reply flags %#02x reserved (8.3.8.11)", flags)
	}
	return nil
}

// DecodeInfoReply parses an INFO_REPLY body per 8.3.8.11. Reserved flags
// and lengths that are not a multiple of the 24 byte locator size drop
// fail-closed.
func DecodeInfoReply(body []byte, flags uint8, drops *Counter) (InfoReply, error) {
	var out InfoReply
	err := checkInfoReplyFlags(flags, drops)
	if err != nil {
		return InfoReply{}, err
	}
	if len(body)%locatorSize != 0 {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: info_reply need multiple of %d have %d (8.3.8.11)", locatorSize, len(body))
	}
	out.HasMulticast = flags&InfoReplyFlagMulticast != 0
	for off := 0; off < len(body); off += locatorSize {
		loc := make([]byte, locatorSize)
		copy(loc, body[off:off+locatorSize])
		out.Locators = append(out.Locators, loc)
	}
	return out, nil
}

// IsKnownSubmessageID reports whether id has a typed decoder per
// DDSI-RTPS 2.5 8.3.4. Known ids fail loudly on malformed bodies and
// invalidate the rest of the message per 8.3.4.1 rule 6. Unknown ids
// skip by length. SubmessageHeaderExtension (0x00) is known: its body
// validates via DecodeHeaderExtension, so malformed extensions fail
// instead of skipping as opaque bytes.
func IsKnownSubmessageID(id uint8) bool {
	switch id {
	case SubmessageHeaderExtension,
		SubmessagePad,
		SubmessageAckNack,
		SubmessageHeartbeat,
		SubmessageGap,
		SubmessageInfoTS,
		SubmessageInfoSrc,
		SubmessageInfoReplyIP4,
		SubmessageInfoDst,
		SubmessageInfoReply,
		SubmessageData,
		SubmessageDataFrag,
		SubmessageNackFrag,
		SubmessageHeartbeatFrag:
		return true
	default:
		return false
	}
}

// Heartbeat is a decoded HEARTBEAT submessage announcing writer sequence
// bounds. The body must be exactly 28 bytes per 8.3.8.6; short and long
// bodies drop. There is no
// reliability engine in v1: heartbeats inform but never trigger repair.
type Heartbeat struct {
	// ReaderID names the destination entity.
	ReaderID EntityID
	// WriterID names the announcing writer.
	WriterID EntityID
	// FirstSequence is the first available writer sequence.
	FirstSequence uint64
	// LastSequence is the last available writer sequence.
	LastSequence uint64
	// Count is the writer heartbeat counter for duplicate detection.
	Count int32
}

// Flags computes the HEARTBEAT submessage flags.
func (h Heartbeat) Flags(littleEndian, final, liveliness bool) uint8 {
	var flags uint8
	if littleEndian {
		flags |= FlagLittleEndian
	}
	if final {
		flags |= HeartbeatFlagFinal
	}
	if liveliness {
		flags |= HeartbeatFlagLiveliness
	}
	return flags
}

// Encode renders the 28 byte HEARTBEAT content.
func (h Heartbeat) Encode(littleEndian bool) []byte {
	out := make([]byte, heartbeatSize)
	copy(out[gapReaderOff:gapReaderOff+entityIDSize], h.ReaderID[:])
	copy(out[gapWriterOff:gapWriterOff+entityIDSize], h.WriterID[:])
	putSeqNumber(out[heartbeatFirstOff:heartbeatFirstOff+dataSeqSize], h.FirstSequence, littleEndian)
	putSeqNumber(out[heartbeatLastOff:heartbeatLastOff+dataSeqSize], h.LastSequence, littleEndian)
	putUint32(out[heartbeatCountOff:heartbeatCountOff+wordSize], uint32(h.Count), littleEndian) //nolint:gosec // G115: intentional two's-complement reinterpret for the wire
	return out
}

// checkHeartbeatBounds enforces signed sequence rules per 8.3.8.6.3.
func checkHeartbeatBounds(first, last int64, drops *Counter) error {
	if first <= 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: heartbeat first %d not positive (8.3.8.6.3)", first)
	}
	if last < 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: heartbeat last %d negative (8.3.8.6.3)", last)
	}
	if last < first-1 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: heartbeat first %d above last %d (8.3.8.6.3)", first, last)
	}
	return nil
}

// DecodeHeartbeat parses a HEARTBEAT body per 8.3.8.6. The body must be
// exactly 28 bytes: short bodies and trailing bytes both drop with a
// counter increment, since the content is fixed width and extra bytes
// indicate a malformed sender or a framing error. Signed invalid bounds
// per 8.3.8.6.3 drop as well. An empty range with first equal to last
// plus one is valid: CycloneDDS sends first last plus one when the
// writer holds no new sample, so it parses without error and callers
// treat it as a silent no-op.
func DecodeHeartbeat(body []byte, flags uint8, drops *Counter) (Heartbeat, error) {
	var out Heartbeat
	if len(body) != heartbeatSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: heartbeat need %d have %d (8.3.8.6)", heartbeatSize, len(body))
	}
	littleEndian := LittleEndian(flags)
	copy(out.ReaderID[:], body[gapReaderOff:gapReaderOff+entityIDSize])
	copy(out.WriterID[:], body[gapWriterOff:gapWriterOff+entityIDSize])
	out.FirstSequence = seqNumberValue(body[heartbeatFirstOff:heartbeatFirstOff+dataSeqSize], littleEndian)
	out.LastSequence = seqNumberValue(body[heartbeatLastOff:heartbeatLastOff+dataSeqSize], littleEndian)
	err := checkHeartbeatBounds(seqSigned(out.FirstSequence), seqSigned(out.LastSequence), drops)
	if err != nil {
		return Heartbeat{}, err
	}
	out.Count = int32(uint32Value(body[heartbeatCountOff:heartbeatCountOff+wordSize], littleEndian)) //nolint:gosec // G115: intentional two's-complement reinterpret from the wire
	return out, nil
}

// AckNack is a reader receipt: base sequence plus a bit map of missing
// samples. v1 sends receipts for future reliable paths; the receive path
// validates readerSNState per 8.3.5.5 and drops malformed bodies loudly so
// callers invalidate the rest of the message per 8.3.4.1 rule 6.
type AckNack struct {
	// ReaderID names the requesting reader.
	ReaderID EntityID
	// WriterID names the writer the receipt targets.
	WriterID EntityID
	// Base is the sequence number bit zero refers to.
	Base uint64
	// NumBits counts meaningful bits in Bitmap.
	NumBits uint32
	// Bitmap holds ceil(NumBits/32) words, set bits request repair.
	Bitmap []uint32
	// Count is the reader counter for duplicate detection.
	Count int32
}

// ackNackWords counts 32 bit words for numBits.
func ackNackWords(numBits uint32) uint64 {
	return (uint64(numBits) + 31) / 32
}

// checkAckNackBitmap validates bitmap length against numBits.
func checkAckNackBitmap(bitmap []uint32, numBits uint32) (uint64, error) {
	words := ackNackWords(numBits)
	if uint64(len(bitmap)) != words {
		return 0, fmt.Errorf("rtps: acknack bitmap %d words for %d bits, want %d (8.3.5.5)", len(bitmap), numBits, words)
	}
	return words, nil
}

// appendAckNackWords appends bitmap words in order.
func appendAckNackWords(out []byte, bitmap []uint32, littleEndian bool) []byte {
	for _, word := range bitmap {
		var slot [wordSize]byte
		putUint32(slot[:], word, littleEndian)
		out = append(out, slot[:]...)
	}
	return out
}

// appendAckNackPrefix appends reader, writer, base, and bit count.
func appendAckNackPrefix(out []byte, a AckNack, littleEndian bool) []byte {
	out = append(out, a.ReaderID[:]...)
	out = append(out, a.WriterID[:]...)
	var base [seqSetBaseSize]byte
	putSeqNumber(base[:], a.Base, littleEndian)
	out = append(out, base[:]...)
	var bits [wordSize]byte
	putUint32(bits[:], a.NumBits, littleEndian)
	return append(out, bits[:]...)
}

// appendAckNackSuffix appends the count word.
func appendAckNackSuffix(out []byte, a AckNack, littleEndian bool) []byte {
	var count [wordSize]byte
	putUint32(count[:], uint32(a.Count), littleEndian) //nolint:gosec // G115: intentional two's-complement reinterpret for the wire
	return append(out, count[:]...)
}

// checkAckNackSize rejects bodies above the submessage limit.
func checkAckNackSize(out []byte) error {
	if len(out) > maxSubmessageBody {
		return fmt.Errorf("rtps: acknack body %d above %d (8.3.8.5)", len(out), maxSubmessageBody)
	}
	return nil
}

// Encode renders the ACKNACK content. Bitmap length must equal
// ceil(NumBits/32) exactly; mismatches fail loudly instead of padding.
func (a AckNack) Encode(littleEndian bool) ([]byte, error) {
	_, err := checkAckNackBitmap(a.Bitmap, a.NumBits)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, ackNackFixedSize+wordSize*len(a.Bitmap))
	out = appendAckNackPrefix(out, a, littleEndian)
	out = appendAckNackWords(out, a.Bitmap, littleEndian)
	out = appendAckNackSuffix(out, a, littleEndian)
	err = checkAckNackSize(out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// checkAckNackState validates the readerSNState set per 8.3.5.5. An empty
// set allows any base including zero; a present set needs base at least 1
// with span at most 256 and no base plus numBits wrap.
func checkAckNackState(base uint64, numBits uint32, drops *Counter) error {
	if numBits > seqSetMaxBits {
		drops.RecordDrop()
		return fmt.Errorf("rtps: acknack bits %d above %d (8.3.5.5)", numBits, seqSetMaxBits)
	}
	if numBits == 0 {
		return nil
	}
	if seqSigned(base) < 1 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: acknack base %d below 1 (8.3.5.5)", seqSigned(base))
	}
	if uint64(numBits) > ^uint64(0)-base {
		drops.RecordDrop()
		return fmt.Errorf("rtps: acknack base %d plus bits %d overflows (8.3.5.5)", base, numBits)
	}
	return nil
}

// DecodeAckNack parses an ACKNACK body and validates readerSNState per
// 8.3.5.5. Short bodies, base below 1 for a present set, span above 256,
// truncated bitmaps, and trailing bytes past the count drop with a counter
// increment. Valid bodies return the receipt; callers invalidate the rest
// of the message on error per 8.3.4.1 rule 6.
func DecodeAckNack(body []byte, flags uint8, drops *Counter) (AckNack, error) {
	var out AckNack
	if len(body) < ackNackFixedSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: acknack need %d have %d (8.3.8.5)", ackNackFixedSize, len(body))
	}
	littleEndian := LittleEndian(flags)
	copy(out.ReaderID[:], body[gapReaderOff:gapReaderOff+entityIDSize])
	copy(out.WriterID[:], body[gapWriterOff:gapWriterOff+entityIDSize])
	out.Base = seqNumberValue(body[gapStartOff:gapStartOff+seqSetBaseSize], littleEndian)
	out.NumBits = uint32Value(body[gapStartOff+seqSetBaseSize:gapStartOff+seqSetBaseSize+wordSize], littleEndian)
	err := checkAckNackState(out.Base, out.NumBits, drops)
	if err != nil {
		return AckNack{}, err
	}
	words := ackNackWords(out.NumBits)
	need := uint64(ackNackFixedSize) + words*wordSize
	if uint64(len(body)) != need {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: acknack need %d have %d (8.3.8.5)", need, len(body))
	}
	off := gapStartOff + seqSetBaseSize + wordSize
	out.Bitmap = make([]uint32, words)
	for i := range out.Bitmap {
		out.Bitmap[i] = uint32Value(body[off:off+wordSize], littleEndian)
		off += wordSize
	}
	out.Count = int32(uint32Value(body[off:off+wordSize], littleEndian)) //nolint:gosec // G115: intentional two's-complement reinterpret from the wire
	return out, nil
}

// Gap is a decoded GAP submessage: the writer irrevocably skipped samples
// from GapStart through the skipped list. GapListBase names the sequence
// number set base that follows GapStart, NumBits counts meaningful bits in
// Bitmap, and Bitmap holds ceil(NumBits/32) words with MSB first per word.
// A set bit names an additional skipped sequence at base plus bit index.
// An empty list means only GapStart is skipped. Group fields and
// filteredCount follow the trailing set at PSM 9.4.5.6 offsets when the G
// and F flags are set.
type Gap struct {
	// ReaderID names the destination entity.
	ReaderID EntityID
	// WriterID names the skipping writer.
	WriterID EntityID
	// GapStart is the first skipped sequence number.
	GapStart uint64
	// GapListBase is the base of the trailing sequence set, zero when absent.
	GapListBase uint64
	// NumBits counts meaningful bits in Bitmap.
	NumBits uint32
	// Bitmap holds the trailing skipped set, MSB first.
	Bitmap []uint32
	// HasGroup reports group sequence fields present via the G flag.
	HasGroup bool
	// GapStartGSN is the group number for GapStart when HasGroup is set.
	GapStartGSN uint64
	// GapEndGSN is the group range end when HasGroup is set.
	GapEndGSN uint64
	// HasFiltered reports filteredCount present via the F flag.
	HasFiltered bool
	// FilteredCount names still cached but filtered changes.
	FilteredCount uint64
}

// gapListFixedSize is base plus numBits after GapStart.
const gapListFixedSize = 12

// checkGapStart rejects zero and negative gap starts per 8.3.8.4.
func checkGapStart(start uint64, drops *Counter) error {
	if seqSigned(start) <= 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: gap start %d not positive (8.3.8.4)", seqSigned(start))
	}
	return nil
}

// checkGapList validates the trailing set base and span per 8.3.5.5. An
// empty set allows any base including zero; a present set needs base at
// least 1 with span at most 256 and no base plus numBits wrap.
func checkGapList(base uint64, numBits uint32, gapStart uint64, drops *Counter) error {
	if numBits > seqSetMaxBits {
		drops.RecordDrop()
		return fmt.Errorf("rtps: gap bits %d above %d (8.3.5.5)", numBits, seqSetMaxBits)
	}
	if numBits == 0 {
		return nil
	}
	if seqSigned(base) < 1 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: gap base %d below 1 (8.3.5.5)", seqSigned(base))
	}
	if uint64(numBits) > ^uint64(0)-base {
		drops.RecordDrop()
		return fmt.Errorf("rtps: gap base %d plus bits %d overflows (8.3.5.5)", base, numBits)
	}
	if numBits > 0 && base <= gapStart {
		drops.RecordDrop()
		return fmt.Errorf("rtps: gap base %d at or below start %d (8.3.8.4)", base, gapStart)
	}
	return nil
}

// checkGapGroup validates group sequence bounds per 8.3.8.4.
func checkGapGroup(start, end uint64, drops *Counter) error {
	if seqSigned(start) <= 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: gap group start %d not positive (8.3.8.4)", seqSigned(start))
	}
	if seqSigned(end) <= 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: gap group end %d not positive (8.3.8.4)", seqSigned(end))
	}
	if seqSigned(end) < seqSigned(start)-1 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: gap group end %d below start %d (8.3.8.4)", seqSigned(end), seqSigned(start))
	}
	return nil
}

// decodeGapList parses the trailing set and returns the offset past it
// per DDSI-RTPS 2.5 8.3.5.5 and 8.3.8.4. When numBits is zero no bitmap
// words follow; only group or filtered fields gated by flags may follow.
func decodeGapList(body []byte, littleEndian bool, out *Gap, drops *Counter) (int, error) {
	baseOff := gapMinSize
	bitsOff := gapMinSize + 8
	out.GapListBase = seqNumberValue(body[baseOff:baseOff+8], littleEndian)
	out.NumBits = uint32Value(body[bitsOff:bitsOff+wordSize], littleEndian)
	err := checkGapList(out.GapListBase, out.NumBits, out.GapStart, drops)
	if err != nil {
		return 0, err
	}
	words := (uint64(out.NumBits) + 31) / 32
	need := uint64(gapMinSize) + uint64(gapListFixedSize) + words*4
	if uint64(len(body)) < need {
		drops.RecordDrop()
		return 0, fmt.Errorf("rtps: gap need %d have %d (8.3.8.4)", need, len(body))
	}
	out.Bitmap = make([]uint32, words)
	off := gapMinSize + gapListFixedSize
	for i := range out.Bitmap {
		out.Bitmap[i] = uint32Value(body[off:off+4], littleEndian)
		off += 4
	}
	return off, nil
}

// decodeGapGroup parses group fields at off when the G flag is set per 8.3.8.4.
func decodeGapGroup(body []byte, off int, littleEndian bool, out *Gap, drops *Counter) (int, error) {
	if len(body)-off < gapGroupSize {
		drops.RecordDrop()
		return 0, fmt.Errorf("rtps: gap group need %d have %d (8.3.8.4)", gapGroupSize, len(body)-off)
	}
	out.HasGroup = true
	out.GapStartGSN = seqNumberValue(body[off:off+8], littleEndian)
	out.GapEndGSN = seqNumberValue(body[off+8:off+16], littleEndian)
	err := checkGapGroup(out.GapStartGSN, out.GapEndGSN, drops)
	if err != nil {
		return 0, err
	}
	return off + gapGroupSize, nil
}

// decodeGapFiltered parses filteredCount at off when the F flag is set
// per 8.3.8.4. The field is a ChangeCount_t of 8 bytes per 8.3.5.11.
func decodeGapFiltered(body []byte, off int, littleEndian bool, out *Gap) (int, error) {
	if len(body)-off < gapFilteredSize {
		return 0, fmt.Errorf("rtps: gap filtered need %d have %d (8.3.8.4)", gapFilteredSize, len(body)-off)
	}
	out.HasFiltered = true
	high := uint32Value(body[off:off+4], littleEndian)
	low := uint32Value(body[off+4:off+8], littleEndian)
	out.FilteredCount = uint64(high)<<32 | uint64(low)
	return off + gapFilteredSize, nil
}

// DecodeGap parses a GAP body per DDSI-RTPS 2.5 8.3.8.4 and PSM 9.4.5.6.
// Short bodies and signed invalid starts drop with a counter increment.
// A present list must be fully covered with base at least 1 and span at
// most 256 per 8.3.5.5; when numBits is zero no bitmap words follow and
// only group or filtered fields gated by flags may follow. Group fields
// and filteredCount are parsed at correct offsets when the G and F flags
// are set. A present list with base at or below GapStart drops loudly so
// SkippedSequences stays sorted. Any trailing bytes past the expected
// group and filtered fields drop loudly as malformed per 8.3.8.4; the
// body length must match exactly like DecodeAckNack.
func DecodeGap(body []byte, flags uint8, drops *Counter) (Gap, error) {
	var out Gap
	if len(body) < gapMinSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: gap need %d have %d (8.3.8.4)", gapMinSize, len(body))
	}
	littleEndian := LittleEndian(flags)
	copy(out.ReaderID[:], body[gapReaderOff:gapReaderOff+entityIDSize])
	copy(out.WriterID[:], body[gapWriterOff:gapWriterOff+entityIDSize])
	out.GapStart = seqNumberValue(body[gapStartOff:gapStartOff+gapStartSize], littleEndian)
	err := checkGapStart(out.GapStart, drops)
	if err != nil {
		return Gap{}, err
	}
	if len(body) == gapMinSize {
		err = checkGapEmptyFlags(flags, drops)
		if err != nil {
			return Gap{}, err
		}
		return out, nil
	}
	if len(body) < gapMinSize+gapListFixedSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: gap list need %d have %d (8.3.8.4)", gapMinSize+gapListFixedSize, len(body))
	}
	off, err := decodeGapList(body, littleEndian, &out, drops)
	if err != nil {
		return Gap{}, err
	}
	if flags&GapFlagGroupInfo != 0 {
		off, err = decodeGapGroup(body, off, littleEndian, &out, drops)
		if err != nil {
			return Gap{}, err
		}
	}
	if flags&GapFlagFilteredCount != 0 {
		next, ferr := decodeGapFiltered(body, off, littleEndian, &out)
		if ferr != nil {
			drops.RecordDrop()
			return Gap{}, ferr
		}
		off = next
	}
	if off != len(body) {
		drops.RecordDrop()
		return Gap{}, fmt.Errorf("rtps: gap trailing %d (8.3.8.4)", len(body)-off)
	}
	return out, nil
}

// checkGapEmptyFlags rejects group or filtered flags on an empty list body per 8.3.8.4.
func checkGapEmptyFlags(flags uint8, drops *Counter) error {
	if flags&(GapFlagGroupInfo|GapFlagFilteredCount) != 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: gap flags %#02x need group or filtered fields (8.3.8.4)", flags&(GapFlagGroupInfo|GapFlagFilteredCount))
	}
	return nil
}

// SkippedSequences returns every skipped sequence in wire order.
//
// It always starts with GapStart, then appends base plus bit index for
// each set bit with MSB first per word. Invalid bit patterns beyond
// NumBits in the final word are ignored. Additions that would overflow
// are skipped so a crafted base near the top cannot wrap.
func (g Gap) SkippedSequences() []uint64 {
	out := make([]uint64, 0, 1+len(g.Bitmap))
	if seqSigned(g.GapStart) <= 0 {
		return out
	}
	out = append(out, g.GapStart)
	for n := range int(g.NumBits) {
		i := uint32(n)
		word := g.Bitmap[i/32]
		shift := 31 - (i % 32)
		if word&(1<<shift) == 0 {
			continue
		}
		if g.GapListBase > ^uint64(0)-uint64(i) {
			continue
		}
		out = append(out, g.GapListBase+uint64(i))
	}
	return out
}

// NackFrag is a decoded NACK_FRAG submessage: a reader fragment receipt
// for one writer sequence. Base names the first fragment in the set,
// NumBits counts meaningful bits in Bitmap, and Bitmap holds
// ceil(NumBits/32) words with MSB first per word. A set bit requests the
// fragment at base plus bit index. No reassembly engine exists
// in-package: callers combine WriterSN plus these bounds with
// DecodeDataFrag geometry to drive fragment repair.
type NackFrag struct {
	// ReaderID names the requesting reader.
	ReaderID EntityID
	// WriterID names the writer the receipt targets.
	WriterID EntityID
	// WriterSN is the sequence needing fragments, starting at 1.
	WriterSN uint64
	// Base is the first fragment number in the set, starting at 1.
	Base uint32
	// NumBits counts meaningful bits in Bitmap.
	NumBits uint32
	// Bitmap holds the missing fragment set, MSB first.
	Bitmap []uint32
	// Count is the reader counter for duplicate detection.
	Count int32
}

// HeartbeatFrag is a decoded HEARTBEAT_FRAG submessage: a writer
// announcement of available fragments for one sequence. No reassembly
// engine exists in-package: callers combine WriterSN plus LastFragmentNum
// with DecodeDataFrag geometry to decide which NackFrag to send.
type HeartbeatFrag struct {
	// ReaderID names the destination entity.
	ReaderID EntityID
	// WriterID names the announcing writer.
	WriterID EntityID
	// WriterSN is the sequence with available fragments, starting at 1.
	WriterSN uint64
	// LastFragmentNum is the highest available fragment, starting at 1.
	LastFragmentNum uint32
	// Count is the writer counter for duplicate detection.
	Count int32
}

// checkNackFragState validates writer and fragment set bounds per 8.3.8.12
// and 8.3.5.7. Fragment base plus numBits must not wrap the 32 bit
// fragment number space, and must not wrap uint64 arithmetic.
func checkNackFragState(writerSN uint64, base, numBits uint32, drops *Counter) error {
	if seqSigned(writerSN) <= 0 {
		drops.RecordDrop()
		return fmt.Errorf("rtps: nackfrag writer %d not positive (8.3.8.12)", seqSigned(writerSN))
	}
	if base == 0 {
		drops.RecordDrop()
		return errors.New("rtps: nackfrag base 0 (8.3.8.12)") //nolint:wrapcheck // new error, nothing to wrap
	}
	if numBits > seqSetMaxBits {
		drops.RecordDrop()
		return fmt.Errorf("rtps: nackfrag bits %d above %d (8.3.5.7)", numBits, seqSetMaxBits)
	}
	if uint64(numBits) > ^uint64(0)-uint64(base) {
		drops.RecordDrop()
		return fmt.Errorf("rtps: nackfrag base %d plus bits %d overflows (8.3.5.7)", base, numBits)
	}
	if uint64(base)+uint64(numBits)-1 > uint64(^uint32(0)) {
		drops.RecordDrop()
		return fmt.Errorf("rtps: nackfrag base %d plus bits %d above uint32 (8.3.8.12)", base, numBits)
	}
	return nil
}

// DecodeNackFrag parses a NACK_FRAG body per 8.3.8.12. Short bodies,
// non-positive writer sequences, zero bases, spans above 256, and
// truncated bitmaps drop with a counter increment. The body length must
// match exactly: trailing bytes past the count drop loudly like
// DecodeAckNack. Valid bodies return the repair bounds; callers
// invalidate the rest on error per 8.3.4.1 rule 6.
func DecodeNackFrag(body []byte, flags uint8, drops *Counter) (NackFrag, error) {
	var out NackFrag
	if len(body) < nackFragMinSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: nackfrag need %d have %d (8.3.8.12)", nackFragMinSize, len(body))
	}
	littleEndian := LittleEndian(flags)
	copy(out.ReaderID[:], body[gapReaderOff:gapReaderOff+entityIDSize])
	copy(out.WriterID[:], body[gapWriterOff:gapWriterOff+entityIDSize])
	out.WriterSN = seqNumberValue(body[gapStartOff:gapStartOff+gapStartSize], littleEndian)
	baseOff := gapMinSize
	bitsOff := gapMinSize + nackFragBaseSize
	out.Base = uint32Value(body[baseOff:baseOff+nackFragBaseSize], littleEndian)
	out.NumBits = uint32Value(body[bitsOff:bitsOff+wordSize], littleEndian)
	err := checkNackFragState(out.WriterSN, out.Base, out.NumBits, drops)
	if err != nil {
		return NackFrag{}, err
	}
	words := (uint64(out.NumBits) + 31) / 32
	need := uint64(nackFragMinSize) + words*wordSize
	if uint64(len(body)) != need {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: nackfrag need %d have %d (8.3.8.12)", need, len(body))
	}
	off := gapMinSize + nackFragBaseSize + wordSize
	out.Bitmap = make([]uint32, words)
	for i := range out.Bitmap {
		out.Bitmap[i] = uint32Value(body[off:off+wordSize], littleEndian)
		off += wordSize
	}
	out.Count = int32(uint32Value(body[off:off+wordSize], littleEndian)) //nolint:gosec // G115: intentional two's-complement reinterpret from the wire
	return out, nil
}

// DecodeHeartbeatFrag parses a HEARTBEAT_FRAG body per 8.3.8.7. Short
// bodies, non-positive writer sequences, and zero last fragments drop
// with a counter increment. The body must be exactly 24 bytes:
// trailing bytes past the count drop loudly like DecodeAckNack. Valid
// bodies return the availability bounds; callers invalidate the rest on
// error per 8.3.4.1 rule 6.
func DecodeHeartbeatFrag(body []byte, flags uint8, drops *Counter) (HeartbeatFrag, error) {
	var out HeartbeatFrag
	if len(body) != heartbeatFragSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: heartbeatfrag need %d have %d (8.3.8.7)", heartbeatFragSize, len(body))
	}
	littleEndian := LittleEndian(flags)
	copy(out.ReaderID[:], body[gapReaderOff:gapReaderOff+entityIDSize])
	copy(out.WriterID[:], body[gapWriterOff:gapWriterOff+entityIDSize])
	out.WriterSN = seqNumberValue(body[gapStartOff:gapStartOff+gapStartSize], littleEndian)
	if seqSigned(out.WriterSN) <= 0 {
		drops.RecordDrop()
		return HeartbeatFrag{}, fmt.Errorf("rtps: heartbeatfrag writer %d not positive (8.3.8.7)", seqSigned(out.WriterSN))
	}
	out.LastFragmentNum = uint32Value(body[gapMinSize:gapMinSize+fragStartSize], littleEndian)
	if out.LastFragmentNum == 0 {
		drops.RecordDrop()
		return HeartbeatFrag{}, errors.New("rtps: heartbeatfrag last fragment 0 (8.3.8.7)") //nolint:wrapcheck // new error, nothing to wrap
	}
	out.Count = int32(uint32Value(body[gapMinSize+fragStartSize:gapMinSize+fragStartSize+wordSize], littleEndian)) //nolint:gosec // G115: intentional two's-complement reinterpret from the wire
	return out, nil
}

// EncodePad returns length zero bytes for submessage alignment per DDSI-RTPS 2.5
// 8.3.4. Lengths above 64K are rejected loudly instead of truncated.
func EncodePad(length int) ([]byte, error) {
	if length < 0 || length > maxSubmessageBody {
		return nil, fmt.Errorf("rtps: pad length %d out of range (8.3.4)", length)
	}
	return make([]byte, length), nil
}

// Submessage is one decoded submessage. Exactly one of the typed pointers
// is set for known kinds, including the header extension; Raw carries
// skipped content for PAD, ACKNACK, and unknown ids, aliasing the owned
// raw body.
type Submessage struct {
	// ID selects the submessage kind.
	ID uint8
	// Flags carries the endianness bit plus kind specific bits.
	Flags uint8
	// HeaderExtension holds a decoded RTPS_HE submessage, nil otherwise.
	// Checksum verification against the full datagram needs the frame;
	// see ValidateHeaderExtensionPlacement.
	HeaderExtension *HeaderExtension
	// Data holds a decoded DATA submessage, nil otherwise.
	Data *Data
	// DataFrag holds a decoded DATA_FRAG submessage, nil otherwise.
	DataFrag *DataFrag
	// InfoTS holds a decoded INFO_TS submessage, nil otherwise.
	InfoTS *InfoTS
	// InfoSrc holds a decoded INFO_SRC submessage, nil otherwise.
	InfoSrc *InfoSrc
	// InfoDst holds a decoded INFO_DST submessage, nil otherwise.
	InfoDst *InfoDst
	// InfoReplyIP4 holds a decoded INFO_REPLY_IP4 submessage, nil otherwise.
	InfoReplyIP4 *InfoReplyIP4
	// InfoReply holds a decoded INFO_REPLY submessage, nil otherwise.
	InfoReply *InfoReply
	// Heartbeat holds a decoded HEARTBEAT submessage, nil otherwise.
	Heartbeat *Heartbeat
	// Gap holds a decoded GAP submessage, nil otherwise.
	Gap *Gap
	// NackFrag holds a decoded NACK_FRAG submessage, nil otherwise.
	NackFrag *NackFrag
	// HeartbeatFrag holds a decoded HEARTBEAT_FRAG submessage, nil otherwise.
	HeartbeatFrag *HeartbeatFrag
	// Raw holds skipped content for PAD, ACKNACK, and unknown ids.
	Raw []byte
}

// decodeDataSubmessage decodes one DATA body with drop accounting.
func decodeDataSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	data, err := DecodeData(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage data: %w (8.3.4.1)", err)
	}
	out.Data = &data
	return out, nil
}

// decodeInfoTSSubmessage decodes one INFO_TS body with drop accounting.
func decodeInfoTSSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	stamp, err := DecodeInfoTS(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage info_ts: %w (8.3.4.1)", err)
	}
	out.InfoTS = &stamp
	return out, nil
}

// decodeInfoSrcSubmessage decodes one INFO_SRC body with drop accounting.
func decodeInfoSrcSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	src, err := DecodeInfoSrc(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage info_src: %w (8.3.4.1)", err)
	}
	out.InfoSrc = &src
	return out, nil
}

// decodeHeartbeatSubmessage decodes one HEARTBEAT body with drop accounting.
func decodeHeartbeatSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	beaten, err := DecodeHeartbeat(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage heartbeat: %w (8.3.4.1)", err)
	}
	out.Heartbeat = &beaten
	return out, nil
}

// decodeGapSubmessage decodes one GAP body with drop accounting.
func decodeGapSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	skipped, err := DecodeGap(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage gap: %w (8.3.4.1)", err)
	}
	out.Gap = &skipped
	return out, nil
}

// decodeDataFragSubmessage decodes one DATA_FRAG body with drop accounting.
func decodeDataFragSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	frag, err := DecodeDataFrag(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage datafrag: %w (8.3.4.1)", err)
	}
	out.DataFrag = &frag
	return out, nil
}

// decodeAckNackSubmessage validates one ACKNACK body with drop accounting.
func decodeAckNackSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags, Raw: raw.Body}
	_, err := DecodeAckNack(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage acknack: %w (8.3.4.1)", err)
	}
	return out, nil
}

// decodeNackFragSubmessage decodes one NACK_FRAG body with drop accounting.
func decodeNackFragSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	nack, err := DecodeNackFrag(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage nackfrag: %w (8.3.4.1)", err)
	}
	out.NackFrag = &nack
	return out, nil
}

// decodeHeartbeatFragSubmessage decodes one HEARTBEAT_FRAG body with drop accounting.
func decodeHeartbeatFragSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	frag, err := DecodeHeartbeatFrag(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage heartbeatfrag: %w (8.3.4.1)", err)
	}
	out.HeartbeatFrag = &frag
	return out, nil
}

// decodeInfoDstSubmessage decodes one INFO_DST body with drop accounting per 8.3.8.13.
func decodeInfoDstSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	dst, err := DecodeInfoDst(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage info_dst: %w (8.3.4.1)", err)
	}
	out.InfoDst = &dst
	return out, nil
}

// decodeHeaderExtensionSubmessage validates one RTPS_HE body with drop accounting per 8.3.7.3.
//
// A malformed extension body fails loudly so callers invalidate the rest
// of the datagram per 8.3.4.1 rule 6. Body validity here covers flagged
// field fit plus parameter shape; checksum verification against the full
// datagram needs the frame and runs in ValidateHeaderExtensionPlacement,
// which datagram walks must also call to enforce first-position.
func decodeHeaderExtensionSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	var tmp TCPDropCounter
	ext, err := DecodeHeaderExtension(raw.Body, raw.Flags, &tmp)
	if err != nil {
		drops.RecordDrop()
		return Submessage{}, fmt.Errorf("rtps: submessage header extension: %w (8.3.7.3)", err)
	}
	out.HeaderExtension = &ext
	return out, nil
}

// decodeInfoReplyIP4Submessage decodes one INFO_REPLY_IP4 body per 8.3.8.11.
func decodeInfoReplyIP4Submessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	reply, err := DecodeInfoReplyIP4(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage info_reply_ip4: %w (8.3.4.1)", err)
	}
	out.InfoReplyIP4 = &reply
	return out, nil
}

// decodeInfoReplySubmessage decodes one INFO_REPLY body per 8.3.8.11.
func decodeInfoReplySubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	reply, err := DecodeInfoReply(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage info_reply: %w (8.3.4.1)", err)
	}
	out.InfoReply = &reply
	return out, nil
}

// DecodeSubmessage decodes one raw submessage by id per 8.3.4.1. PAD
// content is skipped without validation. ACKNACK bodies are validated per
// 8.3.5.5; valid receipts stay as Raw for the repair path while malformed
// bodies fail. INFO_SRC bodies are validated per 8.3.8.10 minimal form
// with exact 5 byte width. INFO_DST bodies are validated per 8.3.8.13
// with 0 or 12 byte widths.
// INFO_REPLY_IP4 and INFO_REPLY bodies are validated per 8.3.8.11 with
// fixed widths. RTPS_HE bodies are validated per 8.3.7.3; malformed
// extensions fail instead of skipping as opaque bytes. NACK_FRAG and
// HEARTBEAT_FRAG decode to typed repair
// bounds.
// Unknown ids are skipped by length and never fatal: Raw preserves the
// bytes for forward compatible readers. A known but invalid submessage
// returns an error; callers invalidate the rest of the message per
// 8.3.4.1 rule 6.
func DecodeSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	if sub, handled, err := decodeDataSubmessageKind(raw, drops); handled || err != nil {
		return sub, err
	}
	if sub, handled, err := decodeInfoSubmessageKind(raw, drops); handled || err != nil {
		return sub, err
	}
	return decodeControlSubmessageKind(raw, drops)
}

// decodeDataSubmessageKind decodes DATA plus DATA_FRAG kinds.
func decodeDataSubmessageKind(raw RawSubmessage, drops *Counter) (Submessage, bool, error) {
	switch raw.ID {
	case SubmessageData:
		sub, err := decodeDataSubmessage(raw, drops)
		return sub, true, err
	case SubmessageDataFrag:
		sub, err := decodeDataFragSubmessage(raw, drops)
		return sub, true, err
	default:
		return Submessage{}, false, nil
	}
}

// decodeInfoSubmessageKind decodes INFO_* kinds.
func decodeInfoSubmessageKind(raw RawSubmessage, drops *Counter) (Submessage, bool, error) {
	switch raw.ID {
	case SubmessageInfoTS:
		sub, err := decodeInfoTSSubmessage(raw, drops)
		return sub, true, err
	case SubmessageInfoSrc:
		sub, err := decodeInfoSrcSubmessage(raw, drops)
		return sub, true, err
	case SubmessageInfoReplyIP4:
		sub, err := decodeInfoReplyIP4Submessage(raw, drops)
		return sub, true, err
	case SubmessageInfoDst:
		sub, err := decodeInfoDstSubmessage(raw, drops)
		return sub, true, err
	case SubmessageInfoReply:
		sub, err := decodeInfoReplySubmessage(raw, drops)
		return sub, true, err
	default:
		return Submessage{}, false, nil
	}
}

// decodeControlSubmessageKind decodes control kinds plus PAD and unknown.
func decodeControlSubmessageKind(raw RawSubmessage, drops *Counter) (Submessage, error) {
	switch raw.ID {
	case SubmessageHeaderExtension:
		return decodeHeaderExtensionSubmessage(raw, drops)
	case SubmessageHeartbeat:
		return decodeHeartbeatSubmessage(raw, drops)
	case SubmessageGap:
		return decodeGapSubmessage(raw, drops)
	case SubmessageNackFrag:
		return decodeNackFragSubmessage(raw, drops)
	case SubmessageHeartbeatFrag:
		return decodeHeartbeatFragSubmessage(raw, drops)
	case SubmessagePad:
		// Alignment carries no meaning; skip without validation.
		return Submessage{ID: raw.ID, Flags: raw.Flags}, nil
	case SubmessageAckNack:
		return decodeAckNackSubmessage(raw, drops)
	default:
		// Unknown ids skip by length and never fail.
		return Submessage{ID: raw.ID, Flags: raw.Flags, Raw: raw.Body}, nil
	}
}

// InfoDstVerdict classifies unicast INFO_DST addressing for one datagram
// per 8.3.8.13.
//
// Absent means process normally: no INFO_DST, a zero-length INFO_DST
// (legal RTPS no-op), or an all-zero destination (unknown). Mismatch
// means a non-zero prefix names another participant: drop. Malformed
// means a short non-empty body that can be neither, an undecodable
// datagram, or a typed INFO_DST decode failure: drop loudly
// fail-closed. Undecodable datagrams report Malformed so the caller drops
// instead of flowing to the normal decode path. Multicast ingress must
// never use this: group sockets carry every sender, so per-sender
// identity has no meaning there.
type InfoDstVerdict int

const (
	// InfoDstAbsent processes the datagram normally.
	InfoDstAbsent InfoDstVerdict = iota
	// InfoDstMismatch drops a datagram addressed to another participant.
	InfoDstMismatch
	// InfoDstMalformed drops a datagram with an undecodable INFO_DST body.
	InfoDstMalformed
)

// ClassifyUnicastInfoDst walks one datagram for INFO_DST submessages per
// 8.3.8.13 and 8.3.4.1 rule 6. Each INFO_DST body is validated through
// the typed DecodeSubmessage path; any decode failure reports Malformed
// fail-closed. Undecodable datagrams report Malformed fail-closed.
//
// Non-INFO_DST submessages are skipped. The first decisive entry wins:
// absent and unknown entries never override a drop, and a drop returns
// immediately.
func ClassifyUnicastInfoDst(data []byte, local [12]byte) InfoDstVerdict {
	var drops Counter
	_, raws, err := DecodeMessage(data, &drops)
	if err != nil {
		return InfoDstMalformed
	}

	for _, raw := range raws {
		if raw.ID != SubmessageInfoDst {
			continue
		}
		decoded, err := DecodeSubmessage(raw, &drops)
		if err != nil {
			return InfoDstMalformed
		}
		if decoded.InfoDst == nil {
			return InfoDstMalformed
		}
		if !decoded.InfoDst.HasPrefix {
			continue
		}
		var zero [12]byte
		if decoded.InfoDst.Prefix == zero {
			continue
		}
		if decoded.InfoDst.Prefix != local {
			return InfoDstMismatch
		}
	}

	return InfoDstAbsent
}
