// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later
package types

import (
	"reflect"
	"testing"
	"unsafe"
)

// TestBoundsStringCapacity checks NUL terminator accounting.
func TestBoundsStringCapacity(t *testing.T) {
	s, err := NewRosString("hi", StringBound{})
	if err != nil {
		t.Fatalf("new string: %v", err)
	}
	if s.Size != 2 || s.Capacity != 3 {
		t.Fatalf("size=%d capacity=%d, want 2 and 3", s.Size, s.Capacity)
	}
	err = CheckRosString(s, StringBound{})
	if err != nil {
		t.Fatalf("check ros string: %v", err)
	}
	bad := RosString{Data: []byte("hi"), Size: 2, Capacity: 2}
	err = CheckRosString(bad, StringBound{})
	if err == nil {
		t.Fatal("capacity missing terminator must be defective")
	}
	empty := RosString{}
	empty.Sync()
	if empty.Size != 0 || empty.Capacity != 1 {
		t.Fatalf("empty sync = %+v, want size 0 cap 1", empty)
	}
	mismatch := RosString{Data: []byte("hello"), Size: 2, Capacity: 6}
	if mismatch.GoString() != "he" {
		t.Fatalf("go string = %q, want he", mismatch.GoString())
	}
	over := RosString{Data: []byte("hi"), Size: 99, Capacity: 100}
	if over.GoString() != "hi" {
		t.Fatalf("over size go string = %q, want hi", over.GoString())
	}
	err = mismatch.Validate(StringBound{})
	if err == nil {
		t.Fatal("size mismatch must fail validate")
	}
}

// checkSequenceTripleBasic verifies new sequence triple and size mismatch.
func checkSequenceTripleBasic(t *testing.T) {
	t.Helper()
	seq, err := NewSequence([]float64{1, 2}, SequenceBound{IsUpperBound: true})
	if err != nil {
		t.Fatalf("new sequence: %v", err)
	}
	if seq.Size != 2 || uint(len(seq.Data)) != seq.Size {
		t.Fatalf("triple inconsistent: %+v", seq)
	}
	err = seq.Validate(SequenceBound{IsUpperBound: true})
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	bad := Sequence[float64]{Data: make([]float64, 2), Size: 1, Capacity: 2}
	err = bad.Validate(SequenceBound{IsUpperBound: true})
	if err == nil {
		t.Fatal("size mismatch must fail")
	}
}

// checkSequenceTripleSpare verifies spare sync and resize triple.
func checkSequenceTripleSpare(t *testing.T) {
	t.Helper()
	spare := Sequence[float64]{Data: make([]float64, 2, 8), Size: 0, Capacity: 0}
	spare.Sync()
	if spare.Size != 2 || spare.Capacity != 8 {
		t.Fatalf("spare sync = %+v, want size 2 cap 8", spare)
	}
	err := spare.Validate(SequenceBound{IsUpperBound: true})
	if err != nil {
		t.Fatalf("spare validate: %v", err)
	}
	lieCap := Sequence[float64]{Data: make([]float64, 2, 4), Size: 2, Capacity: 9}
	err = lieCap.Validate(SequenceBound{IsUpperBound: true})
	if err == nil {
		t.Fatal("slice cap mismatch must fail")
	}
	grown, err := NewSequence([]float64{1}, SequenceBound{IsUpperBound: true})
	if err != nil {
		t.Fatalf("new grown: %v", err)
	}
	err = grown.Resize(1, SequenceBound{IsUpperBound: true})
	if err != nil {
		t.Fatalf("resize same: %v", err)
	}
	if uint(cap(grown.Data)) != grown.Capacity || uint(len(grown.Data)) != grown.Size {
		t.Fatalf("resize triple = %+v", grown)
	}
}

// TestBoundsSequenceTriple checks data size capacity consistency.
func TestBoundsSequenceTriple(t *testing.T) {
	t.Run("basic", checkSequenceTripleBasic)
	t.Run("spare", checkSequenceTripleSpare)
}

// checkBoundsStringEnforcement verifies string bound sources.
func checkBoundsStringEnforcement(t *testing.T) {
	t.Helper()
	bound := StringBound{StringUpperBound: 4, MaxStringSize: 3, DescriptionStringCap: 5, DescriptionCapacity: 6}
	upper, ok := bound.EffectiveUpperBound()
	if !ok || upper != 3 {
		t.Fatalf("effective string bound = %d %v, want 3 true", upper, ok)
	}
	err := CheckStringLength("abcd", bound)
	if err == nil {
		t.Fatal("string above max size must fail")
	}
	err = CheckStringLength("abc", bound)
	if err != nil {
		t.Fatalf("string within bound: %v", err)
	}
	_, ok = StringBound{}.EffectiveUpperBound()
	if ok {
		t.Fatal("empty string bound must be unbounded")
	}
}

// checkBoundsSequenceEnforcement verifies sequence bound sources.
func checkBoundsSequenceEnforcement(t *testing.T) {
	t.Helper()
	seqBound := SequenceBound{ArraySize: 3, IsUpperBound: true, MaxSize: 3, DescriptionCapacity: 2}
	smax, ok := seqBound.EffectiveMax()
	if !ok || smax != 2 {
		t.Fatalf("effective seq bound = %d %v, want 2 true", smax, ok)
	}
	err := CheckSequenceLength(3, seqBound)
	if err == nil {
		t.Fatal("sequence above description capacity must fail")
	}
	fixed := SequenceBound{ArraySize: 36, IsUpperBound: false}
	err = CheckSequenceLength(35, fixed)
	if err == nil {
		t.Fatal("fixed array wrong length must fail")
	}
	fmax, ok := fixed.EffectiveMax()
	if ok {
		t.Fatal("fixed array effective max ok must be false for exact length")
	}
	if fmax != 36 {
		t.Fatalf("fixed max = %d, want 36", fmax)
	}
	err = CheckSequenceLength(36, fixed)
	if err != nil {
		t.Fatalf("fixed exact length: %v", err)
	}
}

// checkBoundsSerializeEnforcement verifies serialize time bound merging.
func checkBoundsSerializeEnforcement(t *testing.T) {
	t.Helper()
	err := ValidateStringForSerialize("toolong", StringBound{StringUpperBound: 4}, DescriptionField{StringCapacity: 4})
	if err == nil {
		t.Fatal("serialize string above bound must fail")
	}
	err = ValidateSequenceForSerialize(4, SolidPrimitiveBound(), DescriptionField{Capacity: 3})
	if err == nil {
		t.Fatal("serialize sequence above bound must fail")
	}
	loose := StringBound{StringUpperBound: 2}
	err = ValidateStringForSerialize("ab", loose, DescriptionField{StringCapacity: 99})
	if err != nil {
		t.Fatalf("looser description must keep prior min: %v", err)
	}
	err = ValidateStringForSerialize("abc", loose, DescriptionField{StringCapacity: 99})
	if err == nil {
		t.Fatal("prior min must survive looser description")
	}
	looseSeq := SequenceBound{ArraySize: 2, IsUpperBound: true}
	err = ValidateSequenceForSerialize(2, looseSeq, DescriptionField{Capacity: 99})
	if err != nil {
		t.Fatalf("looser seq description must keep prior min: %v", err)
	}
	merged := StringBound{StringUpperBound: 5}.Merge(DescriptionField{StringCapacity: 3, Capacity: 4})
	mmax, ok := merged.EffectiveUpperBound()
	if !ok || mmax != 3 {
		t.Fatalf("merged string bound = %d %v, want 3 true", mmax, ok)
	}
	smerged := SequenceBound{ArraySize: 5, IsUpperBound: true}.Merge(DescriptionField{Capacity: 3})
	msmax, ok := smerged.EffectiveMax()
	if !ok || msmax != 3 {
		t.Fatalf("merged seq bound = %d %v, want 3 true", msmax, ok)
	}
}

// checkBoundsDescriptionEnforcement verifies description cap checks.
func checkBoundsDescriptionEnforcement(t *testing.T) {
	t.Helper()
	err := CheckAgainstDescription(DescriptionField{StringCapacity: 2}, 3, 0, true)
	if err == nil {
		t.Fatal("description string cap must fail")
	}
	err = CheckAgainstDescription(DescriptionField{Capacity: 2}, 3, 0, true)
	if err == nil {
		t.Fatal("description capacity on string must fail")
	}
	err = CheckAgainstDescription(DescriptionField{Capacity: 2}, 0, 3, false)
	if err == nil {
		t.Fatal("description capacity on sequence must fail")
	}
	err = CheckAgainstDescription(DescriptionField{}, 99, 99, true)
	if err != nil {
		t.Fatalf("empty description string: %v", err)
	}
	err = CheckAgainstDescription(DescriptionField{}, 0, 99, false)
	if err != nil {
		t.Fatalf("empty description sequence: %v", err)
	}
}

// TestBoundsEnforcement checks every bound source at serialize time.
func TestBoundsEnforcement(t *testing.T) {
	t.Run("string", checkBoundsStringEnforcement)
	t.Run("sequence", checkBoundsSequenceEnforcement)
	t.Run("serialize", checkBoundsSerializeEnforcement)
	t.Run("description", checkBoundsDescriptionEnforcement)
}

// checkBoundsAllZero verifies All and Zero paths.
func checkBoundsAllZero(t *testing.T) {
	t.Helper()
	all, err := NewDefaultedHolder(MessageInitAll)
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if all.Count != DefaultedCount || all.Label.GoString() != DefaultedLabel || all.Plain != 0 {
		t.Fatalf("all = %+v", all)
	}
	zero, err := NewDefaultedHolder(MessageInitZero)
	if err != nil {
		t.Fatalf("zero: %v", err)
	}
	if zero.Count != 0 || zero.Label.GoString() != "" {
		t.Fatalf("zero must ignore defaults: %+v", zero)
	}
}

// checkBoundsDefaultsOnly verifies DefaultsOnly paths.
func checkBoundsDefaultsOnly(t *testing.T) {
	t.Helper()
	pre := DefaultedHolder{Plain: 99, Count: 0}
	err := InitDefaultedHolder(&pre, MessageInitDefaultsOnly)
	if err != nil {
		t.Fatalf("defaults only: %v", err)
	}
	if pre.Count != DefaultedCount || pre.Label.GoString() != DefaultedLabel {
		t.Fatalf("defaults only must fill defaults: %+v", pre)
	}
	if pre.Plain != 99 {
		t.Fatalf("defaults only must leave plain untouched, got %d", pre.Plain)
	}
	partial := DefaultedHolder{Count: 7, Plain: 8, Label: RosString{Data: []byte("old"), Size: 3, Capacity: 4}}
	err = InitDefaultedHolder(&partial, MessageInitDefaultsOnly)
	if err != nil {
		t.Fatalf("partial defaults: %v", err)
	}
	if partial.Count != DefaultedCount || partial.Label.GoString() != DefaultedLabel {
		t.Fatalf("defaults only must overwrite defaulted fields: %+v", partial)
	}
	if partial.Plain != 8 {
		t.Fatalf("defaults only must preserve plain, got %d", partial.Plain)
	}
}

// checkBoundsDefaultsModes verifies All Zero and DefaultsOnly paths.
func checkBoundsDefaultsModes(t *testing.T) {
	t.Helper()
	t.Run("allzero", checkBoundsAllZero)
	t.Run("defaultsonly", checkBoundsDefaultsOnly)
}

// checkBoundsSkipModes verifies Skip preservation for holders and primitives.
func checkBoundsSkipModes(t *testing.T) {
	t.Helper()
	keep := DefaultedHolder{Count: 7, Plain: 8}
	keep.Label = RosString{Data: []byte("k"), Size: 1, Capacity: 2}
	err := InitDefaultedHolder(&keep, MessageInitSkip)
	if err != nil {
		t.Fatalf("skip: %v", err)
	}
	if keep.Count != 7 || keep.Plain != 8 {
		t.Fatalf("skip must leave untouched: %+v", keep)
	}
	var sp SolidPrimitive
	sp.Type = 9
	err = InitSolidPrimitive(&sp, MessageInitSkip)
	if err != nil {
		t.Fatalf("skip solid: %v", err)
	}
	if sp.Type != 9 {
		t.Fatal("skip must preserve solid primitive")
	}
	err = InitSolidPrimitive(&sp, MessageInitZero)
	if err != nil {
		t.Fatalf("zero solid: %v", err)
	}
	if sp.Type != 0 {
		t.Fatal("zero must clear type")
	}
}

// checkBoundsNilFiniModes verifies nil errors and fini zeroing.
func checkBoundsNilFiniModes(t *testing.T) {
	t.Helper()
	err := InitSolidPrimitive(nil, MessageInitAll)
	if err == nil {
		t.Fatal("nil solid init must fail")
	}
	err = FiniSolidPrimitive(nil)
	if err == nil {
		t.Fatal("nil solid fini must fail")
	}
	err = InitDefaultedHolder(nil, MessageInitAll)
	if err == nil {
		t.Fatal("nil defaulted init must fail")
	}
	err = FiniDefaultedHolder(nil)
	if err == nil {
		t.Fatal("nil defaulted fini must fail")
	}
	filled, err := NewDefaultedHolder(MessageInitAll)
	if err != nil {
		t.Fatalf("filled: %v", err)
	}
	err = FiniDefaultedHolder(&filled)
	if err != nil {
		t.Fatalf("fini defaulted: %v", err)
	}
	if filled.Count != 0 || filled.Plain != 0 {
		t.Fatalf("fini must zero counts: %+v", filled)
	}
	err = filled.Label.Validate(StringBound{})
	if err == nil {
		t.Fatal("fini label must be empty invalid triple")
	}
}

// TestBoundsInitModes checks all skip zero and defaults only behavior.
func TestBoundsInitModes(t *testing.T) {
	t.Run("defaults", checkBoundsDefaultsModes)
	t.Run("skip", checkBoundsSkipModes)
	t.Run("nilfini", checkBoundsNilFiniModes)
}

// checkRosidlBufferFlags verifies buffer flag negatives.
func checkRosidlBufferFlags(t *testing.T, m *MessageMember) {
	t.Helper()
	if !m.IsRosidlBuffer || !m.IsBuffer() {
		t.Fatal("buffer member must honor is_rosidl_buffer flag")
	}
	if m.TypeID != RosTypeUint8 {
		t.Fatalf("buffer type = %s", m.TypeID.String())
	}
	plain := NewSequenceMember("data", RosTypeUint8, 0, 0, "Data")
	if plain.IsBuffer() {
		t.Fatal("non buffer uint8 sequence must not report buffer")
	}
	fixedBuf := NewFixedArrayMember("data", RosTypeUint8, 4, 0, "Data")
	fixedBuf.IsRosidlBuffer = true
	if fixedBuf.IsBuffer() {
		t.Fatal("fixed array buffer must be false")
	}
	intBuf := NewSequenceMember("data", RosTypeInt32, 0, 0, "Data")
	intBuf.IsRosidlBuffer = true
	if intBuf.IsBuffer() {
		t.Fatal("int32 buffer must be false")
	}
	scalarBuf := NewScalarMember("data", RosTypeUint8, 0)
	scalarBuf.IsRosidlBuffer = true
	if scalarBuf.IsBuffer() {
		t.Fatal("scalar buffer must be false")
	}
}

// checkRosidlBufferResize verifies buffer init and large resize.
func checkRosidlBufferResize(t *testing.T, table *MessageMembers) {
	t.Helper()
	var h BufferHolder
	err := table.Init(&h, MessageInitAll)
	if err != nil {
		t.Fatalf("init buffer: %v", err)
	}
	err = h.Data.Resize(4, BufferHolderBound())
	if err != nil {
		t.Fatalf("resize buffer: %v", err)
	}
	err = h.Data.Resize(1<<20, BufferHolderBound())
	if err != nil {
		t.Fatalf("large buffer resize: %v", err)
	}
	if h.Data.Size != 1<<20 {
		t.Fatalf("large size = %d", h.Data.Size)
	}
	err = h.Data.Validate(BufferHolderBound())
	if err != nil {
		t.Fatalf("large validate: %v", err)
	}
}

// TestBoundsRosidlBuffer checks the uint8 buffer flag path.
func TestBoundsRosidlBuffer(t *testing.T) {
	table := BufferHolderMessageMembers()
	err := table.Validate()
	if err != nil {
		t.Fatalf("validate buffer: %v", err)
	}
	t.Run("flags", func(t *testing.T) { checkRosidlBufferFlags(t, &table.Members[0]) })
	t.Run("resize", func(t *testing.T) { checkRosidlBufferResize(t, table) })
}

// checkSolidPrimitiveDirect verifies direct resize and validate.
func checkSolidPrimitiveDirect(t *testing.T) {
	t.Helper()
	p, err := NewSolidPrimitive(MessageInitAll)
	if err != nil {
		t.Fatalf("new solid primitive: %v", err)
	}
	err = ResizeSolidPrimitiveDimensions(&p, 3)
	if err != nil {
		t.Fatalf("resize to max 3: %v", err)
	}
	err = ValidateSolidPrimitive(p)
	if err != nil {
		t.Fatalf("validate max 3: %v", err)
	}
	err = ResizeSolidPrimitiveDimensions(&p, 4)
	if err == nil {
		t.Fatal("resize above 3 must fail")
	}
}

// checkSolidPrimitiveTable verifies table resize and nil handling.
func checkSolidPrimitiveTable(t *testing.T) {
	t.Helper()
	table := SolidPrimitiveMessageMembers()
	err := table.Validate()
	if err != nil {
		t.Fatalf("validate table: %v", err)
	}
	dims := &table.Members[1]
	if !dims.IsUpperBound || dims.ArraySize != 3 {
		t.Fatalf("dimensions array_size=%d upper=%v, want 3 true", dims.ArraySize, dims.IsUpperBound)
	}
	var live SolidPrimitive
	err = table.Init(&live, MessageInitAll)
	if err != nil {
		t.Fatalf("table init: %v", err)
	}
	err = dims.Resize(&live, 3)
	if err != nil {
		t.Fatalf("table resize 3: %v", err)
	}
	err = dims.Resize(&live, 4)
	if err == nil {
		t.Fatal("table resize 4 must fail")
	}
	err = ResizeSolidPrimitiveDimensions(nil, 1)
	if err == nil {
		t.Fatal("nil resize must fail")
	}
	delegated, err := NewSolidPrimitive(MessageInitZero)
	if err != nil {
		t.Fatalf("delegated zero: %v", err)
	}
	if delegated.Type != 0 {
		t.Fatal("delegated zero must clear type")
	}
}

// TestBoundsSolidPrimitiveDimensions exercises the single bounded sequence.
func TestBoundsSolidPrimitiveDimensions(t *testing.T) {
	t.Run("direct", checkSolidPrimitiveDirect)
	t.Run("table", checkSolidPrimitiveTable)
}

// checkCMappingScalars verifies scalar C sizes.
func checkCMappingScalars(t *testing.T) {
	t.Helper()
	if unsafe.Sizeof(CFloat(0)) != 4 {
		t.Fatal("float must be 4 bytes")
	}
	if unsafe.Sizeof(CDouble(0)) != 8 {
		t.Fatal("double must be 8 bytes")
	}
	if unsafe.Sizeof(CChar(0)) != 1 {
		t.Fatal("char must be signed 1 byte")
	}
	var c CChar = -1
	if c != -1 {
		t.Fatal("char must be signed")
	}
	if unsafe.Sizeof(CWChar(0)) != 2 {
		t.Fatal("wchar must be uint16 2 bytes")
	}
	if unsafe.Sizeof(CBool(false)) != 1 {
		t.Fatal("bool must be 1 byte")
	}
	if unsafe.Sizeof(COctet(0)) != 1 || unsafe.Sizeof(CUint8(0)) != 1 {
		t.Fatal("octet and uint8 must be 1 byte")
	}
}

// checkCMappingStructs verifies struct shapes and fixed arrays.
func checkCMappingStructs(t *testing.T) {
	t.Helper()
	var s RosString
	if unsafe.Sizeof(s) == 0 {
		t.Fatal("string must be a struct")
	}
	var seq Float64Sequence
	if unsafe.Sizeof(seq) == 0 {
		t.Fatal("sequence must be a struct")
	}
	var cov CovarianceHolder
	if unsafe.Sizeof(cov.Covariance) != 36*8 {
		t.Fatalf("fixed float64[36] must be 288 bytes inline, got %d", unsafe.Sizeof(cov.Covariance))
	}
	var sp SolidPrimitive
	if unsafe.Sizeof(sp) == 0 {
		t.Fatal("solid primitive size must be nonzero on this arch")
	}
	_ = reflect.TypeFor[SolidPrimitive]()
}

// TestBoundsCMapping checks byte for byte primitive sizes and struct shapes.
func TestBoundsCMapping(t *testing.T) {
	t.Run("scalars", checkCMappingScalars)
	t.Run("structs", checkCMappingStructs)
}

// TestBoundsFieldOrder checks IDL order is emitted without reorder.
func TestBoundsFieldOrder(t *testing.T) {
	rt := reflect.TypeFor[SolidPrimitive]()
	if rt.NumField() != 2 || rt.Field(0).Name != "Type" || rt.Field(1).Name != "Dimensions" {
		t.Fatalf("solid primitive field order = %v, want Type then Dimensions", rt)
	}
	table := SolidPrimitiveMessageMembers()
	if table.Members[0].Name != "type" || table.Members[1].Name != "dimensions" {
		t.Fatalf("introspection order = %q %q", table.Members[0].Name, table.Members[1].Name)
	}
	if table.Members[1].Offset <= table.Members[0].Offset {
		t.Fatal("offsets must increase in IDL order")
	}
}

// checkConstantsValues verifies constant values are unchanged.
func checkConstantsValues(t *testing.T) {
	t.Helper()
	if ConstInt32L != -123 || ConstUint32L != 456 || ConstFloatF != 1.5 || !ConstBoolT || ConstBoolF {
		t.Fatal("constant values changed")
	}
	if SolidPrimitiveBOX != 1 || SolidPrimitiveSPHERE != 2 || SolidPrimitiveCYLINDER != 3 || SolidPrimitiveCONE != 4 {
		t.Fatal("solid primitive enum values changed")
	}
	if SolidPrimitiveMaxStringSize != 0 {
		t.Fatal("solid primitive must have no bounded string")
	}
}

// checkConstantsNotOnWire verifies no constant leaks onto the wire.
func checkConstantsNotOnWire(t *testing.T, table *MessageMembers) {
	t.Helper()
	for i := range table.Members {
		switch table.Members[i].Name {
		case "l", "ul", "f", "true", "false", "BOX", "SPHERE":
			t.Fatalf("constant %q must never be on the wire", table.Members[i].Name)
		default:
		}
	}
}

// TestBoundsConstantsNotOnWire checks consts exist only as Go consts.
func TestBoundsConstantsNotOnWire(t *testing.T) {
	t.Run("values", checkConstantsValues)
	t.Run("wire", func(t *testing.T) { checkConstantsNotOnWire(t, SolidPrimitiveMessageMembers()) })
}

// TestBoundsCovariance checks fixed array zeroing and exact length.
func TestBoundsCovariance(t *testing.T) {
	table := CovarianceMessageMembers()
	var h CovarianceHolder
	h.Covariance[0] = 9.5
	err := table.Init(&h, MessageInitAll)
	if err != nil {
		t.Fatalf("cov init: %v", err)
	}
	if h.Covariance[0] != 0 {
		t.Fatal("cov init must zero fixed array")
	}
	h.Covariance[5] = 2.5
	err = table.Fini(&h)
	if err != nil {
		t.Fatalf("cov fini: %v", err)
	}
	if h.Covariance[5] != 0 {
		t.Fatal("cov fini must zero fixed array")
	}
	bound := CovarianceBound()
	err = CheckSequenceLength(36, bound)
	if err != nil {
		t.Fatalf("cov exact length: %v", err)
	}
	err = table.Init(&h, MessageInitSkip)
	if err != nil {
		t.Fatalf("cov skip: %v", err)
	}
}

// TestBoundsBoundedStringHolder checks the proven bounded string path.
func TestBoundsBoundedStringHolder(t *testing.T) {
	table := BoundedStringMessageMembers()
	err := table.Validate()
	if err != nil {
		t.Fatalf("bounded validate: %v", err)
	}
	if table.Members[0].StringUpperBound != 10 {
		t.Fatalf("upper bound = %d, want 10", table.Members[0].StringUpperBound)
	}
	s, err := NewRosString("hello", BoundedStringBound())
	if err != nil {
		t.Fatalf("new bounded: %v", err)
	}
	err = CheckRosString(s, BoundedStringBound())
	if err != nil {
		t.Fatalf("check bounded: %v", err)
	}
	_, err = NewRosString("0123456789X", BoundedStringBound())
	if err == nil {
		t.Fatal("string above 10 must fail")
	}
	var h BoundedStringHolder
	err = table.Init(&h, MessageInitAll)
	if err != nil {
		t.Fatalf("holder init: %v", err)
	}
	err = table.Fini(&h)
	if err != nil {
		t.Fatalf("holder fini: %v", err)
	}
}

// TestBoundsHeaderNested checks the header like nested fixture.
func TestBoundsHeaderNested(t *testing.T) {
	ClearMessageRegistryForTest()
	headerTable := HeaderFixtureMessageMembers()
	err := RegisterMessageMembers(headerTable)
	if err != nil {
		t.Fatalf("register header: %v", err)
	}
	stamped := HeaderStampedMessageMembers()
	err = stamped.Validate()
	if err != nil {
		t.Logf("stamped pre-patch validate: %v", err)
	}
	err = stamped.EnsureNestedPatched()
	if err != nil {
		t.Fatalf("stamped patch: %v", err)
	}
	err = stamped.Validate()
	if err != nil {
		t.Fatalf("stamped validate: %v", err)
	}
	var v HeaderStampedFixture
	err = stamped.Init(&v, MessageInitAll)
	if err != nil {
		t.Fatalf("stamped init: %v", err)
	}
	if v.Header.Seq != 0 || v.Value != 0 {
		t.Fatalf("stamped init = %+v", v)
	}
	ok, err := IsSelfContained(stamped)
	if err != nil {
		t.Fatalf("stamped self contained: %v", err)
	}
	if ok {
		t.Fatal("stamped with string must not be self contained")
	}
}

// TestBoundsRosStringSpareCap checks spare capacity preservation.
func TestBoundsRosStringSpareCap(t *testing.T) {
	spare := RosString{Data: make([]byte, 2, 8), Size: 2, Capacity: 9}
	spare.Sync()
	if spare.Size != 2 {
		t.Fatalf("spare size = %d, want 2", spare.Size)
	}
	if spare.Capacity != 9 {
		t.Fatalf("spare capacity = %d, want 9 preserved", spare.Capacity)
	}
	if err := spare.Validate(StringBound{}); err != nil {
		t.Fatalf("spare validate: %v", err)
	}
	tight := RosString{Data: make([]byte, 3), Size: 3, Capacity: 3}
	tight.Sync()
	if tight.Capacity != 4 {
		t.Fatalf("tight capacity = %d, want 4 with NUL", tight.Capacity)
	}
	empty := RosString{}
	if err := empty.Init(MessageInitAll); err != nil {
		t.Fatalf("empty init all: %v", err)
	}
	if empty.Capacity != 1 {
		t.Fatalf("empty init capacity = %d, want 1", empty.Capacity)
	}
}

// TestBoundsInitMatrix checks RosString init modes across all values.
func TestBoundsInitMatrix(t *testing.T) {
	modes := []MessageInit{MessageInitAll, MessageInitZero, MessageInitDefaultsOnly, MessageInitSkip}
	for _, mode := range modes {
		s := RosString{Data: []byte("hi"), Size: 2, Capacity: 3}
		if err := s.Init(mode); err != nil {
			t.Fatalf("string init %s: %v", mode.String(), err)
		}
	}
	filled := RosString{Data: []byte("hello"), Size: 5, Capacity: 6}
	if err := filled.Init(MessageInitSkip); err != nil {
		t.Fatalf("string skip: %v", err)
	}
	if filled.GoString() != "hello" {
		t.Fatalf("skip must preserve string, got %q", filled.GoString())
	}
	strBad := RosString{}
	if err := strBad.Init(MessageInit(99)); err == nil {
		t.Fatal("bad string init must fail")
	}
}

// TestBoundsSequenceInitMatrix checks Sequence init modes across all values.
func TestBoundsSequenceInitMatrix(t *testing.T) {
	modes := []MessageInit{MessageInitAll, MessageInitZero, MessageInitDefaultsOnly, MessageInitSkip}
	for _, mode := range modes {
		seq := Sequence[float64]{Data: []float64{1, 2}, Size: 2, Capacity: 2}
		if err := seq.Init(mode); err != nil {
			t.Fatalf("sequence init %s: %v", mode.String(), err)
		}
	}
	seqFilled := Sequence[float64]{Data: []float64{7}, Size: 1, Capacity: 1}
	if err := seqFilled.Init(MessageInitDefaultsOnly); err != nil {
		t.Fatalf("sequence defaults: %v", err)
	}
	if len(seqFilled.Data) != 1 || seqFilled.Data[0] != 7 {
		t.Fatalf("defaults must preserve sequence, got %+v", seqFilled)
	}
	if err := seqFilled.Init(MessageInit(99)); err == nil {
		t.Fatal("bad sequence init must fail")
	}
	seqEmpty := Sequence[float64]{}
	if err := seqEmpty.Init(MessageInitDefaultsOnly); err != nil {
		t.Fatalf("empty sequence defaults: %v", err)
	}
	if seqEmpty.Size != 0 || seqEmpty.Capacity != 0 {
		t.Fatalf("empty sequence defaults = %+v", seqEmpty)
	}
}

// TestBoundsCovarianceDefaults checks DefaultsOnly preservation.
func TestBoundsCovarianceDefaults(t *testing.T) {
	table := CovarianceMessageMembers()
	var h CovarianceHolder
	h.Covariance[0] = 4.5
	if err := table.Init(&h, MessageInitDefaultsOnly); err != nil {
		t.Fatalf("cov defaults: %v", err)
	}
	if h.Covariance[0] != 4.5 {
		t.Fatal("cov defaults must preserve caller data")
	}
	if CovarianceSize != 36 {
		t.Fatalf("covariance size = %d, want 36", CovarianceSize)
	}
	if err := CheckSequenceLength(36, CovarianceBound()); err != nil {
		t.Fatalf("cov exact: %v", err)
	}
	if err := CheckSequenceLength(0, SequenceBound{ArraySize: 0}); err == nil {
		t.Fatal("fixed zero size must fail")
	}
}
