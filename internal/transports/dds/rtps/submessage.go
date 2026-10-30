// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

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
	// SubmessageAckNack is a reader to writer receipt. Encode only in v1.
	SubmessageAckNack uint8 = 0x06
	// SubmessageHeartbeat announces writer sequence bounds. Decode only;
	// v1 runs no reliability engine.
	SubmessageHeartbeat uint8 = 0x07
	// SubmessageGap describes skipped sequence numbers. Skip tolerant.
	SubmessageGap uint8 = 0x08
	// SubmessageInfoTS carries a sample timestamp.
	SubmessageInfoTS uint8 = 0x09
	// SubmessageInfoSrc carries source version and flags. Skipped by length.
	SubmessageInfoSrc uint8 = 0x0c
	// SubmessageInfoReplyIP4 carries reply locators. Skipped by length.
	SubmessageInfoReplyIP4 uint8 = 0x0d
	// SubmessageInfoDst selects the message destination. Skipped by length.
	SubmessageInfoDst uint8 = 0x0e
	// SubmessageInfoReply carries reply flags. Skipped by length.
	SubmessageInfoReply uint8 = 0x0f
	// SubmessageNackFrag is a fragment receipt. Skipped by length.
	SubmessageNackFrag uint8 = 0x12
	// SubmessageHeartbeatFrag announces fragment bounds. Skipped by length.
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

// encodeParameters renders params plus a terminating sentinel. Values are
// padded with zeros to 4 bytes per the RTPS parameter alignment.
func encodeParameters(params []Parameter, littleEndian bool) ([]byte, error) {
	out := make([]byte, 0, len(params)*(parameterHeaderSize+4)+parameterHeaderSize)
	for i := range params {
		if len(params[i].Value) > maxSubmessageBody {
			return nil, fmt.Errorf("rtps: parameter %#04x value %d above %d", params[i].ID, len(params[i].Value), maxSubmessageBody)
		}
		out = appendParamEntry(out, params[i].ID, params[i].Value, littleEndian)
	}
	out = appendParamSentinel(out, ParamSentinel, littleEndian)
	return out, nil
}

// decodeParameters parses a parameter list ending at the sentinel and
// returns the entries with the consumed byte count. Unknown ids are kept
// so callers can skip what they do not understand.
func decodeParameters(body []byte, littleEndian bool) ([]Parameter, int, error) {
	var params []Parameter
	off := 0
	for {
		id, length, ok := paramHeaderAt(body, off, littleEndian)
		if !ok {
			return nil, 0, fmt.Errorf("rtps: parameter header need %d have %d", parameterHeaderSize, len(body)-off)
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
		return fmt.Errorf("rtps: sentinel carries %d bytes", length)
	}
	return nil
}

// copyParamBytes copies length bytes at off and skips alignment pad.
// It returns the copied value and the offset past the pad.
func copyParamBytes(body []byte, off, length int) ([]byte, int, error) {
	if len(body)-off < length {
		return nil, 0, fmt.Errorf("rtps: param value need %d have %d", length, len(body)-off)
	}
	value := make([]byte, length)
	copy(value, body[off:off+length])
	off += length
	pad := padTo4(length)
	if len(body)-off < pad {
		return nil, 0, fmt.Errorf("rtps: param pad need %d have %d", pad, len(body)-off)
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
		return nil, 0, fmt.Errorf("rtps: parameter %#04x: %w", id, err)
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
		return nil, fmt.Errorf("rtps: encode data inline qos: %w", err)
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
		return nil, fmt.Errorf("rtps: data body %d above %d", len(out), maxSubmessageBody)
	}
	return out, nil
}

// Submessage renders d as a framed DATA submessage with header.
func (d Data) Submessage(littleEndian bool) (RawSubmessage, error) {
	flags := d.dataFlags(littleEndian)
	body, err := d.Body(littleEndian)
	if err != nil {
		return RawSubmessage{}, fmt.Errorf("rtps: data submessage: %w", err)
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
		return nil, nil, fmt.Errorf("rtps: decode data inline qos: %w", err)
	}
	return params, rest[consumed:], nil
}

// checkDataFlags rejects contradictory Data plus Key flags.
func checkDataFlags(flags uint8, drops *Counter) error {
	if flags&(DataFlagDataPresent|DataFlagKeyPresent) == DataFlagDataPresent|DataFlagKeyPresent {
		drops.RecordDrop()
		return errors.New("rtps: data sets both data and key flags") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// checkDataSize rejects truncated DATA bodies.
func checkDataSize(body []byte, drops *Counter) error {
	if len(body) < dataFixedSize {
		drops.RecordDrop()
		return fmt.Errorf("rtps: data need %d have %d", dataFixedSize, len(body))
	}
	return nil
}

// DecodeData parses a DATA body with its header flags. Extra flags and
// the inline QoS offset are ignored for tolerance; parsing is sequential.
// Contradictory Data plus Key flags drop loudly. Payload aliases the
// owned body slice.
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
		return fmt.Errorf("rtps: datafrag need %d have %d", dataFragFixedSize, len(body))
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

// checkDataFragGeometry validates fragment numbers and sizes with bounded work.
func checkDataFragGeometry(out DataFrag, drops *Counter) error {
	if out.FragmentStartingNum == 0 {
		drops.RecordDrop()
		return errors.New("rtps: datafrag starting number 0") //nolint:wrapcheck // new error, nothing to wrap
	}
	if out.FragmentsInSubmessage == 0 {
		drops.RecordDrop()
		return errors.New("rtps: datafrag fragments in submessage 0") //nolint:wrapcheck // new error, nothing to wrap
	}
	if out.FragmentSize == 0 {
		drops.RecordDrop()
		return errors.New("rtps: datafrag fragment size 0") //nolint:wrapcheck // new error, nothing to wrap
	}
	if out.SampleSize == 0 {
		drops.RecordDrop()
		return errors.New("rtps: datafrag sample size 0") //nolint:wrapcheck // new error, nothing to wrap
	}
	total := dataFragTotal(out.SampleSize, out.FragmentSize)
	start := uint64(out.FragmentStartingNum)
	count := uint64(out.FragmentsInSubmessage)
	if start+count-1 > total {
		drops.RecordDrop()
		return fmt.Errorf("rtps: datafrag range %d plus %d above total %d", start, count, total)
	}
	return nil
}

// checkDataFragPayload validates the fragment payload length against geometry.
func checkDataFragPayload(out DataFrag, payload []byte, drops *Counter) error {
	offset := (uint64(out.FragmentStartingNum) - 1) * uint64(out.FragmentSize)
	remaining := uint64(out.SampleSize) - offset
	want := uint64(out.FragmentsInSubmessage) * uint64(out.FragmentSize)
	want = min(want, remaining)
	if uint64(len(payload)) != want {
		drops.RecordDrop()
		return fmt.Errorf("rtps: datafrag payload need %d have %d", want, len(payload))
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
// reports invalid, since no fields follow. A valid stamp needs 8 bytes;
// trailing bytes are ignored for forward compatibility.
func DecodeInfoTS(body []byte, flags uint8, drops *Counter) (InfoTS, error) {
	var out InfoTS
	if flags&InfoTSFlagInvalidate != 0 {
		return out, nil
	}
	if len(body) < infoTSSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: info_ts need %d have %d", infoTSSize, len(body))
	}
	littleEndian := LittleEndian(flags)
	out.Valid = true
	out.Seconds = uint32Value(body[infoTSSecondsOff:infoTSSecondsOff+wordSize], littleEndian)
	out.Fraction = uint32Value(body[infoTSFractionOff:infoTSFractionOff+wordSize], littleEndian)
	return out, nil
}

// Heartbeat is a decoded HEARTBEAT submessage announcing writer sequence
// bounds. Decode is tolerant and trailing bytes are ignored. There is no
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

// DecodeHeartbeat parses a HEARTBEAT body, ignoring trailing bytes for
// tolerance. Short bodies drop with a counter increment.
func DecodeHeartbeat(body []byte, flags uint8, drops *Counter) (Heartbeat, error) {
	var out Heartbeat
	if len(body) < heartbeatSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: heartbeat need %d have %d", heartbeatSize, len(body))
	}
	littleEndian := LittleEndian(flags)
	copy(out.ReaderID[:], body[gapReaderOff:gapReaderOff+entityIDSize])
	copy(out.WriterID[:], body[gapWriterOff:gapWriterOff+entityIDSize])
	out.FirstSequence = seqNumberValue(body[heartbeatFirstOff:heartbeatFirstOff+dataSeqSize], littleEndian)
	out.LastSequence = seqNumberValue(body[heartbeatLastOff:heartbeatLastOff+dataSeqSize], littleEndian)
	out.Count = int32(uint32Value(body[heartbeatCountOff:heartbeatCountOff+wordSize], littleEndian)) //nolint:gosec // G115: intentional two's-complement reinterpret from the wire
	return out, nil
}

// AckNack is an encode only reader receipt: base sequence plus a bit map
// of missing samples. v1 sends receipts for future reliable paths; the
// receive path skips ACKNACK content by length without decoding.
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
		return 0, fmt.Errorf("rtps: acknack bitmap %d words for %d bits, want %d", len(bitmap), numBits, words)
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
		return fmt.Errorf("rtps: acknack body %d above %d", len(out), maxSubmessageBody)
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

// Gap is a decoded GAP submessage: the writer irrevocably skipped samples
// from GapStart through the skipped list. v1 keeps only the start; the
// sequence set that follows is skipped by length as tolerant input.
type Gap struct {
	// ReaderID names the destination entity.
	ReaderID EntityID
	// WriterID names the skipping writer.
	WriterID EntityID
	// GapStart is the first skipped sequence number.
	GapStart uint64
}

// DecodeGap parses a GAP body through gap start and skips the sequence
// set that follows. Short bodies drop with a counter increment.
func DecodeGap(body []byte, flags uint8, drops *Counter) (Gap, error) {
	var out Gap
	if len(body) < gapMinSize {
		drops.RecordDrop()
		return out, fmt.Errorf("rtps: gap need %d have %d", gapMinSize, len(body))
	}
	littleEndian := LittleEndian(flags)
	copy(out.ReaderID[:], body[gapReaderOff:gapReaderOff+entityIDSize])
	copy(out.WriterID[:], body[gapWriterOff:gapWriterOff+entityIDSize])
	out.GapStart = seqNumberValue(body[gapStartOff:gapStartOff+gapStartSize], littleEndian)
	return out, nil
}

// EncodePad returns length zero bytes for submessage alignment. Lengths
// above 64K are rejected loudly instead of truncated.
func EncodePad(length int) ([]byte, error) {
	if length < 0 || length > maxSubmessageBody {
		return nil, fmt.Errorf("rtps: pad length %d out of range", length)
	}
	return make([]byte, length), nil
}

// Submessage is one decoded submessage. Exactly one of the typed pointers
// is set for known kinds; Raw carries skipped content for PAD, ACKNACK,
// and unknown ids, aliasing the owned raw body.
type Submessage struct {
	// ID selects the submessage kind.
	ID uint8
	// Flags carries the endianness bit plus kind specific bits.
	Flags uint8
	// Data holds a decoded DATA submessage, nil otherwise.
	Data *Data
	// InfoTS holds a decoded INFO_TS submessage, nil otherwise.
	InfoTS *InfoTS
	// Heartbeat holds a decoded HEARTBEAT submessage, nil otherwise.
	Heartbeat *Heartbeat
	// Gap holds a decoded GAP submessage, nil otherwise.
	Gap *Gap
	// Raw holds skipped content for PAD, ACKNACK, and unknown ids.
	Raw []byte
}

// decodeDataSubmessage decodes one DATA body with drop accounting.
func decodeDataSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	data, err := DecodeData(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage data: %w", err)
	}
	out.Data = &data
	return out, nil
}

// decodeInfoTSSubmessage decodes one INFO_TS body with drop accounting.
func decodeInfoTSSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	stamp, err := DecodeInfoTS(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage info_ts: %w", err)
	}
	out.InfoTS = &stamp
	return out, nil
}

// decodeHeartbeatSubmessage decodes one HEARTBEAT body with drop accounting.
func decodeHeartbeatSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	beaten, err := DecodeHeartbeat(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage heartbeat: %w", err)
	}
	out.Heartbeat = &beaten
	return out, nil
}

// decodeGapSubmessage decodes one GAP body with drop accounting.
func decodeGapSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	out := Submessage{ID: raw.ID, Flags: raw.Flags}
	skipped, err := DecodeGap(raw.Body, raw.Flags, drops)
	if err != nil {
		return Submessage{}, fmt.Errorf("rtps: submessage gap: %w", err)
	}
	out.Gap = &skipped
	return out, nil
}

// DecodeSubmessage decodes one raw submessage by id. PAD content is
// skipped without validation. ACKNACK is encode only in v1, so its content
// is skipped by length. Unknown ids are skipped by length and never fatal:
// Raw preserves the bytes for forward compatible readers.
func DecodeSubmessage(raw RawSubmessage, drops *Counter) (Submessage, error) {
	switch raw.ID {
	case SubmessageData:
		return decodeDataSubmessage(raw, drops)
	case SubmessageInfoTS:
		return decodeInfoTSSubmessage(raw, drops)
	case SubmessageHeartbeat:
		return decodeHeartbeatSubmessage(raw, drops)
	case SubmessageGap:
		return decodeGapSubmessage(raw, drops)
	case SubmessagePad:
		// Alignment carries no meaning; skip without validation.
		return Submessage{ID: raw.ID, Flags: raw.Flags}, nil
	case SubmessageAckNack:
		// Encode only in v1; the receive path skips by length.
		return Submessage{ID: raw.ID, Flags: raw.Flags, Raw: raw.Body}, nil
	default:
		// Unknown ids skip by length and never fail.
		return Submessage{ID: raw.ID, Flags: raw.Flags, Raw: raw.Body}, nil
	}
}
