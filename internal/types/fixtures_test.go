// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later
package types

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"testing"
	"unsafe"
)

// IDL constants are Go consts only and never travel on the wire.
// Suffix mapping from IDL literals: L means int32, UL means uint32,
// F means float32, booleans stay bool. No constant is serialized.
// Values below are arbitrary fixtures, not IDL defaults. They pin the
// mapping with distinct signed, unsigned, fractional, and bool cases.
// The only real IDL default in the corpus is the identity quaternion.
const (
	ConstInt32L  CInt32  = -123
	ConstUint32L CUint32 = 456
	ConstFloatF  CFloat  = 1.5
	ConstBoolT   CBool   = true
	ConstBoolF   CBool   = false
)

// SolidPrimitive type enum values from shape_msgs.
const (
	SolidPrimitiveBOX      uint8 = 1
	SolidPrimitiveSPHERE   uint8 = 2
	SolidPrimitiveCYLINDER uint8 = 3
	SolidPrimitiveCONE     uint8 = 4
)

// SolidPrimitiveDimensionsMaxSize is the __MAX_SIZE bound for dimensions.
const SolidPrimitiveDimensionsMaxSize uint = 3

// CovarianceSize is the fixed element count for covariance matrices.
// It is used for the holder type, the introspection bound and member,
// and the zero value so the three sites cannot drift apart.
const CovarianceSize = 36

// SolidPrimitiveMaxStringSize is zero because the fixture has no bounded string.
const SolidPrimitiveMaxStringSize uint = 0

// Float64Sequence is the concrete sequence used by SolidPrimitive.
type Float64Sequence = Sequence[float64]

// Uint8Sequence is the concrete sequence used for byte buffers.
type Uint8Sequence = Sequence[uint8]

// SolidPrimitive mirrors shape_msgs/msg/SolidPrimitive in IDL order.
// IDL order is type then dimensions, and Go field order must match.
type SolidPrimitive struct {
	Type       uint8
	Dimensions Float64Sequence
}

// SolidPrimitiveBound returns the introspection max for dimensions.
// This is the IDL max only; serialize time merges it with the type
// description capacity via ValidateSequenceForSerialize.
func SolidPrimitiveBound() SequenceBound {
	return SequenceBound{
		ArraySize:    SolidPrimitiveDimensionsMaxSize,
		IsUpperBound: true,
		MaxSize:      SolidPrimitiveDimensionsMaxSize,
	}
}

// NewSolidPrimitive builds a primitive honoring the init mode.
// It delegates to InitSolidPrimitive so construction and init cannot diverge.
func NewSolidPrimitive(init MessageInit) (SolidPrimitive, error) {
	var out SolidPrimitive
	err := InitSolidPrimitive(&out, init)
	if err != nil {
		return SolidPrimitive{}, fmt.Errorf("types: new solid primitive: %w", err)
	}
	return out, nil
}

// InitSolidPrimitive applies the init mode to an existing primitive.
func InitSolidPrimitive(p *SolidPrimitive, init MessageInit) error {
	if p == nil {
		return errors.New("types: init nil solid primitive") //nolint:wrapcheck // fixture sentinel has no cause to wrap
	}
	err := init.Validate()
	if err != nil {
		return fmt.Errorf("types: init solid primitive: %w", err)
	}
	switch init {
	case MessageInitAll, MessageInitZero:
		p.Type = 0
		err = p.Dimensions.Fini()
		if err != nil {
			return fmt.Errorf("types: init solid primitive dimensions: %w", err)
		}
	case MessageInitDefaultsOnly:
		if p.Dimensions.Data == nil {
			err = p.Dimensions.Fini()
			if err != nil {
				return fmt.Errorf("types: init solid primitive dimensions: %w", err)
			}
		}
	case MessageInitSkip:
	default:
		return fmt.Errorf("types: unknown solid primitive init %s", init.String())
	}
	return nil
}

// FiniSolidPrimitive finalizes a primitive.
func FiniSolidPrimitive(p *SolidPrimitive) error {
	if p == nil {
		return errors.New("types: fini nil solid primitive") //nolint:wrapcheck // fixture sentinel has no cause to wrap
	}
	err := p.Dimensions.Fini()
	if err != nil {
		return fmt.Errorf("types: fini solid primitive dimensions: %w", err)
	}
	p.Type = 0
	return nil
}

// ResizeSolidPrimitiveDimensions resizes dimensions with fini plus init semantics.
func ResizeSolidPrimitiveDimensions(p *SolidPrimitive, size uint) error {
	if p == nil {
		return errors.New("types: resize nil solid primitive") //nolint:wrapcheck // fixture sentinel has no cause to wrap
	}
	err := p.Dimensions.Resize(size, SolidPrimitiveBound())
	if err != nil {
		return fmt.Errorf("types: resize solid primitive dimensions: %w", err)
	}
	return nil
}

// ValidateSolidPrimitive enforces dimensions bounds at serialize time.
func ValidateSolidPrimitive(p SolidPrimitive) error {
	err := p.Dimensions.Validate(SolidPrimitiveBound())
	if err != nil {
		return fmt.Errorf("types: validate solid primitive: %w", err)
	}
	return nil
}

// assertOffsetFitsUint32 rejects uintptr offsets that truncate on cast.
// Offsets below come from unsafe.Offsetof and are Go layout only.
// It panics when off exceeds math.MaxUint32; callers pass only struct
// offsets which fit on all supported builds, so a panic signals a
// programming error rather than a runtime condition to handle.
func assertOffsetFitsUint32(off uintptr, what string) {
	if off > uintptr(math.MaxUint32) {
		panic("types: offset exceeds uint32 for " + what)
	}
}

// SolidPrimitiveMessageMembers builds the introspection table with offsets.
// Offsets are Go struct layouts for reflection only, not C ABI, and differ
// by architecture. SizeOf is the Go struct size on this build.
func SolidPrimitiveMessageMembers() *MessageMembers {
	var zero SolidPrimitive
	assertOffsetFitsUint32(unsafe.Offsetof(zero.Type), "SolidPrimitive.Type")
	assertOffsetFitsUint32(unsafe.Offsetof(zero.Dimensions), "SolidPrimitive.Dimensions")
	offType := uint32(unsafe.Offsetof(zero.Type))
	offDims := uint32(unsafe.Offsetof(zero.Dimensions))
	members := []MessageMember{
		NewScalarMember("type", RosTypeUint8, offType),
		NewSequenceMember("dimensions", RosTypeDouble, SolidPrimitiveDimensionsMaxSize, offDims, "Dimensions"),
	}
	initFn := func(base any, init MessageInit) error {
		p, ok := base.(*SolidPrimitive)
		if !ok {
			return fmt.Errorf("types: init base is %T, want *SolidPrimitive", base)
		}
		err := InitSolidPrimitive(p, init)
		if err != nil {
			return fmt.Errorf("types: init solid primitive table: %w", err)
		}
		return nil
	}
	finiFn := func(base any) error {
		p, ok := base.(*SolidPrimitive)
		if !ok {
			return fmt.Errorf("types: fini base is %T, want *SolidPrimitive", base)
		}
		err := FiniSolidPrimitive(p)
		if err != nil {
			return fmt.Errorf("types: fini solid primitive table: %w", err)
		}
		return nil
	}
	return NewMessageMembers("shape_msgs__msg", "SolidPrimitive", uint(unsafe.Sizeof(zero)), members, initFn, finiFn)
}

// CovarianceHolder carries an inline fixed float64[CovarianceSize] covariance.
type CovarianceHolder struct {
	Covariance [CovarianceSize]float64
}

// CovarianceBound returns the fixed array bound for covariance.
// This is the IDL fixed size only; serialize time checks exact length
// with CheckSequenceLength so description caps cannot loosen it.
func CovarianceBound() SequenceBound {
	return SequenceBound{ArraySize: CovarianceSize, IsUpperBound: false}
}

// CovarianceMessageMembers builds the introspection table for the fixed array.
func CovarianceMessageMembers() *MessageMembers {
	var zero CovarianceHolder
	assertOffsetFitsUint32(unsafe.Offsetof(zero.Covariance), "CovarianceHolder.Covariance")
	off := uint32(unsafe.Offsetof(zero.Covariance))
	members := []MessageMember{
		NewFixedArrayMember("covariance", RosTypeDouble, CovarianceSize, off, "Covariance"),
	}
	initFn := func(base any, init MessageInit) error {
		h, ok := base.(*CovarianceHolder)
		if !ok {
			return fmt.Errorf("types: init base is %T, want *CovarianceHolder", base)
		}
		err := init.Validate()
		if err != nil {
			return fmt.Errorf("types: init covariance: %w", err)
		}
		switch init {
		case MessageInitAll, MessageInitZero:
			h.Covariance = [CovarianceSize]float64{}
		case MessageInitDefaultsOnly:
			// No defaults exist for covariance, so preserve caller data.
			// Zero would also be valid since the default is zero, but
			// preserving matches Sequence and SolidPrimitive behavior.
		case MessageInitSkip:
		default:
			return fmt.Errorf("types: unknown covariance init %s", init.String())
		}
		return nil
	}
	finiFn := func(base any) error {
		h, ok := base.(*CovarianceHolder)
		if !ok {
			return fmt.Errorf("types: fini base is %T, want *CovarianceHolder", base)
		}
		h.Covariance = [CovarianceSize]float64{}
		return nil
	}
	return NewMessageMembers("geometry_msgs__msg", "CovarianceHolder", uint(unsafe.Sizeof(zero)), members, initFn, finiFn)
}

// BufferHolder carries a uint8 sequence flagged as an rosidl buffer.
type BufferHolder struct {
	Data Uint8Sequence
}

// BufferHolderBound returns the unbounded buffer bound.
// This is the introspection max only; serialize time merges it with the
// type description capacity via ValidateSequenceForSerialize.
func BufferHolderBound() SequenceBound {
	return SequenceBound{IsUpperBound: true}
}

// BufferHolderMessageMembers builds the introspection table honoring the buffer flag.
func BufferHolderMessageMembers() *MessageMembers {
	var zero BufferHolder
	assertOffsetFitsUint32(unsafe.Offsetof(zero.Data), "BufferHolder.Data")
	off := uint32(unsafe.Offsetof(zero.Data))
	members := []MessageMember{
		NewBufferMember("data", 0, off, "Data"),
	}
	initFn := func(base any, init MessageInit) error {
		h, ok := base.(*BufferHolder)
		if !ok {
			return fmt.Errorf("types: init base is %T, want *BufferHolder", base)
		}
		err := h.Data.Init(init)
		if err != nil {
			return fmt.Errorf("types: init buffer holder: %w", err)
		}
		return nil
	}
	finiFn := func(base any) error {
		h, ok := base.(*BufferHolder)
		if !ok {
			return fmt.Errorf("types: fini base is %T, want *BufferHolder", base)
		}
		err := h.Data.Fini()
		if err != nil {
			return fmt.Errorf("types: fini buffer holder: %w", err)
		}
		return nil
	}
	return NewMessageMembers("std_msgs__msg", "BufferHolder", uint(unsafe.Sizeof(zero)), members, initFn, finiFn)
}

// DefaultedHolder exercises defaults only init with two defaulted fields.
type DefaultedHolder struct {
	Count int32
	Label RosString
	Plain int32
}

// Default values for the defaulted holder fixture.
const (
	DefaultedCount int32  = 5
	DefaultedLabel string = "hello"
)

// applyDefaultedHolderDefaults fills every defaulted field.
// Plain is never touched here; callers decide Skip versus fill.
func applyDefaultedHolderDefaults(h *DefaultedHolder) error {
	h.Count = DefaultedCount
	s, err := NewRosString(DefaultedLabel, StringBound{})
	if err != nil {
		return fmt.Errorf("types: init defaulted label: %w", err)
	}
	h.Label = s
	return nil
}

// NewDefaultedHolder builds the holder honoring the init mode.
// It delegates to InitDefaultedHolder so construction cannot diverge.
func NewDefaultedHolder(init MessageInit) (DefaultedHolder, error) {
	out := DefaultedHolder{}
	err := InitDefaultedHolder(&out, init)
	if err != nil {
		return DefaultedHolder{}, fmt.Errorf("types: new defaulted holder: %w", err)
	}
	return out, nil
}

// InitDefaultedHolder applies explicit zeroing or default fill per mode.
// DefaultsOnly fills defaulted fields (Count, Label) unconditionally and
// leaves non-defaulted Plain untouched, even when Plain already holds data.
func InitDefaultedHolder(h *DefaultedHolder, init MessageInit) error {
	if h == nil {
		return errors.New("types: init nil defaulted holder") //nolint:wrapcheck // fixture sentinel has no cause to wrap
	}
	err := init.Validate()
	if err != nil {
		return fmt.Errorf("types: init defaulted holder: %w", err)
	}
	switch init {
	case MessageInitAll:
		h.Plain = 0
		err = applyDefaultedHolderDefaults(h)
		if err != nil {
			return err
		}
	case MessageInitZero:
		h.Count = 0
		h.Plain = 0
		err = h.Label.Init(MessageInitZero)
		if err != nil {
			return fmt.Errorf("types: zero defaulted label: %w", err)
		}
	case MessageInitDefaultsOnly:
		err = applyDefaultedHolderDefaults(h)
		if err != nil {
			return err
		}
	case MessageInitSkip:
	default:
		return fmt.Errorf("types: unknown defaulted holder init %s", init.String())
	}
	return nil
}

// FiniDefaultedHolder finalizes the holder.
func FiniDefaultedHolder(h *DefaultedHolder) error {
	if h == nil {
		return errors.New("types: fini nil defaulted holder") //nolint:wrapcheck // fixture sentinel has no cause to wrap
	}
	err := h.Label.Fini()
	if err != nil {
		return fmt.Errorf("types: fini defaulted label: %w", err)
	}
	h.Count = 0
	h.Plain = 0
	return nil
}

// BoundedStringHolder proves bounded string plumbing with a length 10 cap.
// This fixture keeps the bounded string path covered instead of unproven.
type BoundedStringHolder struct {
	Name RosString
}

// BoundedStringBound returns the length 10 bound for the holder.
// This is the IDL max only; serialize time merges it with the type
// description capacity via ValidateStringForSerialize.
func BoundedStringBound() StringBound {
	return StringBound{StringUpperBound: 10}
}

// initBoundedStringHolder applies init mode to a bounded string holder.
func initBoundedStringHolder(base any, init MessageInit) error {
	h, ok := base.(*BoundedStringHolder)
	if !ok {
		return fmt.Errorf("types: init base is %T, want *BoundedStringHolder", base)
	}
	err := init.Validate()
	if err != nil {
		return fmt.Errorf("types: init bounded string: %w", err)
	}
	switch init {
	case MessageInitAll, MessageInitZero:
		err = h.Name.Init(MessageInitZero)
		if err != nil {
			return fmt.Errorf("types: init bounded name: %w", err)
		}
	case MessageInitDefaultsOnly:
		err = h.Name.Init(MessageInitDefaultsOnly)
		if err != nil {
			return fmt.Errorf("types: defaults bounded name: %w", err)
		}
	case MessageInitSkip:
	default:
		return fmt.Errorf("types: unknown bounded string init %s", init.String())
	}
	return nil
}

// finiBoundedStringHolder finalizes a bounded string holder.
func finiBoundedStringHolder(base any) error {
	h, ok := base.(*BoundedStringHolder)
	if !ok {
		return fmt.Errorf("types: fini base is %T, want *BoundedStringHolder", base)
	}
	err := h.Name.Fini()
	if err != nil {
		return fmt.Errorf("types: fini bounded name: %w", err)
	}
	return nil
}

// BoundedStringMessageMembers builds the introspection table for the holder.
func BoundedStringMessageMembers() *MessageMembers {
	var zero BoundedStringHolder
	assertOffsetFitsUint32(unsafe.Offsetof(zero.Name), "BoundedStringHolder.Name")
	off := uint32(unsafe.Offsetof(zero.Name))
	members := []MessageMember{
		NewStringMember("name", RosTypeString, 10, off),
	}
	return NewMessageMembers("std_msgs__msg", "BoundedStringHolder", uint(unsafe.Sizeof(zero)), members, initBoundedStringHolder, finiBoundedStringHolder)
}

// HeaderFixture mirrors std_msgs/msg/Header frame handling in miniature.
type HeaderFixture struct {
	Seq     uint32
	FrameID RosString
}

// HeaderStampedFixture nests HeaderFixture to exercise recursive patching.
type HeaderStampedFixture struct {
	Header HeaderFixture
	Value  int32
}

// initHeaderFixture applies init mode to a header fixture.
func initHeaderFixture(base any, init MessageInit) error {
	h, ok := base.(*HeaderFixture)
	if !ok {
		return fmt.Errorf("types: init base is %T, want *HeaderFixture", base)
	}
	err := init.Validate()
	if err != nil {
		return fmt.Errorf("types: init header: %w", err)
	}
	switch init {
	case MessageInitAll, MessageInitZero:
		h.Seq = 0
		err = h.FrameID.Init(MessageInitZero)
		if err != nil {
			return fmt.Errorf("types: init header frame: %w", err)
		}
	case MessageInitDefaultsOnly:
		err = h.FrameID.Init(MessageInitDefaultsOnly)
		if err != nil {
			return fmt.Errorf("types: defaults header frame: %w", err)
		}
	case MessageInitSkip:
	default:
		return fmt.Errorf("types: unknown header init %s", init.String())
	}
	return nil
}

// finiHeaderFixture finalizes a header fixture.
func finiHeaderFixture(base any) error {
	h, ok := base.(*HeaderFixture)
	if !ok {
		return fmt.Errorf("types: fini base is %T, want *HeaderFixture", base)
	}
	err := h.FrameID.Fini()
	if err != nil {
		return fmt.Errorf("types: fini header frame: %w", err)
	}
	h.Seq = 0
	return nil
}

// HeaderFixtureMessageMembers builds the header table.
func HeaderFixtureMessageMembers() *MessageMembers {
	var zero HeaderFixture
	assertOffsetFitsUint32(unsafe.Offsetof(zero.Seq), "HeaderFixture.Seq")
	assertOffsetFitsUint32(unsafe.Offsetof(zero.FrameID), "HeaderFixture.FrameID")
	offSeq := uint32(unsafe.Offsetof(zero.Seq))
	offFrame := uint32(unsafe.Offsetof(zero.FrameID))
	members := []MessageMember{
		NewScalarMember("seq", RosTypeUint32, offSeq),
		NewStringMember("frame_id", RosTypeString, 0, offFrame),
	}
	return NewMessageMembers("std_msgs__msg", "HeaderFixture", uint(unsafe.Sizeof(zero)), members, initHeaderFixture, finiHeaderFixture)
}

// initHeaderStamped applies init mode to a stamped fixture.
func initHeaderStamped(base any, init MessageInit) error {
	h, ok := base.(*HeaderStampedFixture)
	if !ok {
		return fmt.Errorf("types: init base is %T, want *HeaderStampedFixture", base)
	}
	err := init.Validate()
	if err != nil {
		return fmt.Errorf("types: init stamped: %w", err)
	}
	switch init {
	case MessageInitAll, MessageInitZero:
		h.Value = 0
		h.Header.Seq = 0
		err = h.Header.FrameID.Init(MessageInitZero)
		if err != nil {
			return fmt.Errorf("types: init stamped frame: %w", err)
		}
	case MessageInitDefaultsOnly:
		err = h.Header.FrameID.Init(MessageInitDefaultsOnly)
		if err != nil {
			return fmt.Errorf("types: defaults stamped frame: %w", err)
		}
	case MessageInitSkip:
	default:
		return fmt.Errorf("types: unknown stamped init %s", init.String())
	}
	return nil
}

// finiHeaderStamped finalizes a stamped fixture.
func finiHeaderStamped(base any) error {
	h, ok := base.(*HeaderStampedFixture)
	if !ok {
		return fmt.Errorf("types: fini base is %T, want *HeaderStampedFixture", base)
	}
	err := h.Header.FrameID.Fini()
	if err != nil {
		return fmt.Errorf("types: fini stamped frame: %w", err)
	}
	h.Header.Seq = 0
	h.Value = 0
	return nil
}

// HeaderStampedMessageMembers builds the stamped table with lazy nesting.
func HeaderStampedMessageMembers() *MessageMembers {
	var zero HeaderStampedFixture
	assertOffsetFitsUint32(unsafe.Offsetof(zero.Header), "HeaderStampedFixture.Header")
	assertOffsetFitsUint32(unsafe.Offsetof(zero.Value), "HeaderStampedFixture.Value")
	offHeader := uint32(unsafe.Offsetof(zero.Header))
	offValue := uint32(unsafe.Offsetof(zero.Value))
	members := []MessageMember{
		NewNestedMember("header", offHeader, "Header", "std_msgs__msg", "HeaderFixture"),
		NewScalarMember("value", RosTypeInt32, offValue),
	}
	return NewMessageMembers("std_msgs__msg", "HeaderStampedFixture", uint(unsafe.Sizeof(zero)), members, initHeaderStamped, finiHeaderStamped)
}

// ClearMessageRegistryForTest drops all registered tables for isolated tests.
func ClearMessageRegistryForTest() {
	introspectionMu.Lock()
	defer introspectionMu.Unlock()
	messageRegistry = map[string]*MessageMembers{}
}

// TestPointCloud2FragmentedRegression pins the 100KiB PointCloud2 XCDR1 layout.
//
// It mirrors the live large payload shape: 1 by 25600 cloud with one
// float32 x field and deterministic data. The header is zero stamp plus a
// fixed frame_id, then height plus width plus one field descriptor, then
// bigendian plus steps plus data plus dense. The message ends with bool
// is_dense, so Encoder.Bytes adds 3 zero pad bytes to reach a 4 byte
// boundary. Finish must accept that pad and report zero trailing.
//
//nolint:gocognit,cyclop // regression test mirrors large PointCloud2 wire shape end to end in one run, splitting would obscure intent
func TestPointCloud2FragmentedRegression(t *testing.T) {
	frame := "pc2-regression"
	data := make([]byte, 25600*4)
	for i := range data {
		data[i] = byte(i % 256)
	}
	enc := NewEncoder(true)
	enc.WriteInt32(0)
	enc.WriteUint32(0)
	err := enc.WriteString(frame)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	enc.WriteUint32(1)
	enc.WriteUint32(25600)
	err = enc.WriteSequenceLength(1)
	if err != nil {
		t.Fatalf("encode fields len: %v", err)
	}
	err = enc.WriteString("x")
	if err != nil {
		t.Fatalf("encode field name: %v", err)
	}
	enc.WriteUint32(0)
	enc.WriteUint8(7)
	enc.WriteUint32(1)
	enc.WriteBool(false)
	enc.WriteUint32(4)
	enc.WriteUint32(102400)
	err = enc.WriteOctetSequence(data)
	if err != nil {
		t.Fatalf("encode data: %v", err)
	}
	enc.WriteBool(true)
	wire := enc.Bytes()
	if len(wire)%4 != 0 {
		t.Fatalf("wire length %d not padded to 4", len(wire))
	}
	dec, err := NewDecoder(wire)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	sec, err := dec.ReadInt32()
	if err != nil {
		t.Fatalf("decode sec: %v", err)
	}
	if sec != 0 {
		t.Fatalf("sec %d wants 0", sec)
	}
	nsec, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode nanosec: %v", err)
	}
	if nsec != 0 {
		t.Fatalf("nanosec %d wants 0", nsec)
	}
	gotFrame, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if gotFrame != frame {
		t.Fatalf("frame %q wants %q", gotFrame, frame)
	}
	height, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode height: %v", err)
	}
	if height != 1 {
		t.Fatalf("height %d wants 1", height)
	}
	width, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode width: %v", err)
	}
	if width != 25600 {
		t.Fatalf("width %d wants 25600", width)
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		t.Fatalf("decode fields len: %v", err)
	}
	if n != 1 {
		t.Fatalf("fields len %d wants 1", n)
	}
	name, err := dec.ReadString()
	if err != nil {
		t.Fatalf("decode field name: %v", err)
	}
	if name != "x" {
		t.Fatalf("field name %q wants %q", name, "x")
	}
	off, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode offset: %v", err)
	}
	if off != 0 {
		t.Fatalf("offset %d wants 0", off)
	}
	dt, err := dec.ReadUint8()
	if err != nil {
		t.Fatalf("decode datatype: %v", err)
	}
	if dt != 7 {
		t.Fatalf("datatype %d wants 7", dt)
	}
	cnt, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode count: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("count %d wants 1", cnt)
	}
	big, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("decode bigendian: %v", err)
	}
	if big {
		t.Fatalf("bigendian true wants false")
	}
	step, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode point_step: %v", err)
	}
	if step != 4 {
		t.Fatalf("point_step %d wants 4", step)
	}
	row, err := dec.ReadUint32()
	if err != nil {
		t.Fatalf("decode row_step: %v", err)
	}
	if row != 102400 {
		t.Fatalf("row_step %d wants 102400", row)
	}
	got, err := dec.ReadOctetSequence()
	if err != nil {
		t.Fatalf("decode data: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("data bytes differ len %d wants %d", len(got), len(data))
	}
	dense, err := dec.ReadBool()
	if err != nil {
		t.Fatalf("decode dense: %v", err)
	}
	if !dense {
		t.Fatalf("dense false wants true")
	}
	err = dec.Finish()
	if err != nil {
		t.Fatalf("Finish found trailing bytes: %v", err)
	}
	if dec.Remaining() != 0 {
		t.Fatalf("remaining %d wants 0 after Finish", dec.Remaining())
	}
}
