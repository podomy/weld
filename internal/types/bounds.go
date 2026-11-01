// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later
package types

import (
	"errors"
	"fmt"
)

// IDL to C mapping aliases. Each alias is byte for byte identical to the
// C type used by the generator, so sizes must match the C ABI.
type (
	CFloat  = float32
	CDouble = float64
	CChar   = int8
	CWChar  = uint16
	CBool   = bool
	COctet  = uint8
	CByte   = uint8
	CInt8   = int8
	CUint8  = uint8
	CInt16  = int16
	CUint16 = uint16
	CInt32  = int32
	CUint32 = uint32
	CInt64  = int64
	CUint64 = uint64
)

// RosString models rosidl_runtime_c String as data plus size plus capacity.
// Capacity includes the NUL terminator: valid strings satisfy
// Capacity >= Size+1. The empty valid triple is Data nil, Size 0,
// Capacity 1 (NUL only). Fini releases to Data nil, Size 0, Capacity 0,
// which is not valid until Init runs again.
//
// Sequence by contrast carries no terminator: valid sequences satisfy
// Capacity >= Size, and the empty valid triple is Data nil, Size 0,
// Capacity 0. The two empty rules differ by design.
type RosString struct {
	Data     []byte
	Size     uint
	Capacity uint
}

// NewRosString builds a string with capacity including the terminator.
func NewRosString(s string, bound StringBound) (RosString, error) {
	err := CheckStringLength(s, bound)
	if err != nil {
		return RosString{}, fmt.Errorf("types: new string: %w", err)
	}
	data := make([]byte, len(s))
	copy(data, s)
	out := RosString{
		Data: data,
		Size: uint(len(s)),
	}
	out.Sync()
	err = out.Validate(bound)
	if err != nil {
		return RosString{}, fmt.Errorf("types: new string validate: %w", err)
	}
	return out, nil
}

// Sync refreshes Size from Data and enforces the NUL capacity rule.
// Spare capacity is preserved; Capacity below Size+1 is raised to Size+1.
func (s *RosString) Sync() {
	s.Size = uint(len(s.Data))
	if s.Capacity < s.Size+1 {
		s.Capacity = s.Size + 1
	}
}

// GoString returns the Go string contents up to Size.
// A Size beyond Data is treated as Data length to avoid a panic;
// Validate reports that mismatch separately.
func (s *RosString) GoString() string {
	if uint(len(s.Data)) < s.Size {
		return string(s.Data)
	}
	return string(s.Data[:s.Size])
}

// Init applies the init mode to a string.
func (s *RosString) Init(init MessageInit) error {
	err := init.Validate()
	if err != nil {
		return fmt.Errorf("types: init string: %w", err)
	}
	switch init {
	case MessageInitAll, MessageInitZero:
		s.Data = nil
		s.Size = 0
		s.Capacity = 1
	case MessageInitDefaultsOnly:
		if s.Data == nil && s.Size == 0 && s.Capacity == 0 {
			s.Data = nil
			s.Size = 0
			s.Capacity = 1
		}
	case MessageInitSkip:
	default:
		return fmt.Errorf("types: unknown string init mode %s", init.String())
	}
	return nil
}

// Fini clears a string, releasing its contents.
func (s *RosString) Fini() error {
	s.Data = nil
	s.Size = 0
	s.Capacity = 0
	return nil
}

// Validate checks size, capacity, and the NUL terminator rule.
func (s *RosString) Validate(bound StringBound) error {
	if uint(len(s.Data)) != s.Size {
		return fmt.Errorf("types: string size %d != data len %d", s.Size, len(s.Data))
	}
	if s.Capacity < s.Size+1 {
		return fmt.Errorf("types: string capacity %d misses NUL for size %d", s.Capacity, s.Size)
	}
	err := CheckStringLength(string(s.Data), bound)
	if err != nil {
		return fmt.Errorf("types: validate string: %w", err)
	}
	return nil
}

// Sequence models a CDR sequence as data plus size plus capacity.
// There is no terminator: valid triples satisfy Capacity >= Size and
// len(Data) == Size. The empty valid triple is Data nil, Size 0,
// Capacity 0, unlike RosString which reserves one NUL byte.
type Sequence[T any] struct {
	Data     []T
	Size     uint
	Capacity uint
}

// Sync refreshes Size and Capacity from the backing slice.
func (s *Sequence[T]) Sync() {
	s.Size = uint(len(s.Data))
	if cap(s.Data) == 0 && len(s.Data) == 0 {
		s.Capacity = 0
		return
	}
	s.Capacity = uint(cap(s.Data))
}

// NewSequence builds a sequence and syncs its triple.
func NewSequence[T any](vals []T, bound SequenceBound) (Sequence[T], error) {
	err := CheckSequenceLength(uint(len(vals)), bound)
	if err != nil {
		return Sequence[T]{}, fmt.Errorf("types: new sequence: %w", err)
	}
	data := make([]T, len(vals))
	copy(data, vals)
	out := Sequence[T]{Data: data}
	out.Sync()
	err = out.Validate(bound)
	if err != nil {
		return Sequence[T]{}, fmt.Errorf("types: new sequence validate: %w", err)
	}
	return out, nil
}

// Init applies the init mode to a sequence.
// DefaultsOnly is an explicit no-op for already initialized triples:
// it preserves existing Data and only leaves an empty triple empty.
func (s *Sequence[T]) Init(init MessageInit) error {
	err := init.Validate()
	if err != nil {
		return fmt.Errorf("types: init sequence: %w", err)
	}
	switch init {
	case MessageInitAll, MessageInitZero:
		s.Data = nil
		s.Size = 0
		s.Capacity = 0
	case MessageInitDefaultsOnly:
		// No defaults exist for plain sequences, so preserve callers data.
		if s.Data == nil && s.Size == 0 && s.Capacity == 0 {
			s.Data = nil
			s.Size = 0
			s.Capacity = 0
		}
	case MessageInitSkip:
	default:
		return fmt.Errorf("types: unknown sequence init mode %s", init.String())
	}
	return nil
}

// Fini clears a sequence with fini semantics.
func (s *Sequence[T]) Fini() error {
	var zero []T
	s.Data = zero
	s.Size = 0
	s.Capacity = 0
	return nil
}

// Resize applies fini plus init resize within the bound.
// Growth uses MakeSlice with len equal cap, so spare capacity is dropped.
// Unbounded sizes are capped by maxSequenceElements before allocation so a
// corrupt huge size returns an error instead of panicking or OOM on make.
func (s *Sequence[T]) Resize(size uint, bound SequenceBound) error {
	err := CheckSequenceLength(size, bound)
	if err != nil {
		return fmt.Errorf("types: resize sequence: %w", err)
	}
	if uint64(size) > uint64(maxSequenceElements) {
		return fmt.Errorf("types: sequence len %d exceeds cap %d", size, maxSequenceElements)
	}
	next := make([]T, size)
	limit := len(s.Data)
	if uint(limit) > size {
		limit = int(size)
	}
	copy(next, s.Data[:limit])
	s.Data = next
	s.Sync()
	return nil
}

// Validate checks triple consistency and bound enforcement.
func (s *Sequence[T]) Validate(bound SequenceBound) error {
	if uint(len(s.Data)) != s.Size {
		return fmt.Errorf("types: sequence size %d != data len %d", s.Size, len(s.Data))
	}
	if s.Capacity < s.Size {
		return fmt.Errorf("types: sequence capacity %d below size %d", s.Capacity, s.Size)
	}
	if uint(cap(s.Data)) != s.Capacity && (len(s.Data) != 0 || s.Capacity != 0) {
		return fmt.Errorf("types: sequence capacity %d != slice cap %d", s.Capacity, cap(s.Data))
	}
	err := CheckSequenceLength(s.Size, bound)
	if err != nil {
		return fmt.Errorf("types: validate sequence: %w", err)
	}
	return nil
}

// StringBound gathers every upper bound source for a string field.
// Sources are introspection StringUpperBound, IDL MaxStringSize, and
// description Capacity plus StringCapacity.
type StringBound struct {
	StringUpperBound     uint
	MaxStringSize        uint
	DescriptionCapacity  uint
	DescriptionStringCap uint
}

// EffectiveUpperBound returns the tightest bound and whether one exists.
// Ok false means unbounded: any length is accepted.
func (b StringBound) EffectiveUpperBound() (uint, bool) {
	best := uint(0)
	found := false
	for _, v := range []uint{b.StringUpperBound, b.MaxStringSize, b.DescriptionCapacity, b.DescriptionStringCap} {
		if v == 0 {
			continue
		}
		if !found || v < best {
			best = v
			found = true
		}
	}
	return best, found
}

// Merge returns the tightest combination of introspection and description caps.
// Each description source only tightens: a larger field value never loosens
// an existing smaller bound.
func (b StringBound) Merge(field DescriptionField) StringBound {
	out := b
	if field.StringCapacity != 0 {
		if out.DescriptionStringCap == 0 || field.StringCapacity < out.DescriptionStringCap {
			out.DescriptionStringCap = field.StringCapacity
		}
	}
	if field.Capacity != 0 {
		if out.DescriptionCapacity == 0 || field.Capacity < out.DescriptionCapacity {
			out.DescriptionCapacity = field.Capacity
		}
	}
	return out
}

// CheckStringLength enforces all bound sources at serialize time.
// Bounds count runes like the CDR wire bounds, not bytes, so multi-byte
// UTF-8 never over-rejects.
func CheckStringLength(s string, bound StringBound) error {
	upper, ok := bound.EffectiveUpperBound()
	if ok {
		n := countRunes(s)
		if uint(n) > upper {
			return fmt.Errorf("types: string len %d above bound %d", n, upper)
		}
	}
	return nil
}

// CheckRosString enforces bounds plus the NUL terminator rule.
func CheckRosString(s RosString, bound StringBound) error {
	err := s.Validate(bound)
	if err != nil {
		return fmt.Errorf("types: check ros string: %w", err)
	}
	return nil
}

// SequenceBound gathers every upper bound source for an array field.
type SequenceBound struct {
	ArraySize           uint
	IsUpperBound        bool
	MaxSize             uint
	DescriptionCapacity uint
}

// EffectiveMax returns the max length and whether the field is bounded.
// Ok false means the field is a fixed size array: length must equal
// ArraySize exactly. Ok true means bounded sequence with max length.
func (b SequenceBound) EffectiveMax() (uint, bool) {
	if !b.IsUpperBound {
		return b.ArraySize, false
	}
	best := b.ArraySize
	found := b.ArraySize != 0
	if b.MaxSize != 0 && (!found || b.MaxSize < best) {
		best = b.MaxSize
		found = true
	}
	if b.DescriptionCapacity != 0 && (!found || b.DescriptionCapacity < best) {
		best = b.DescriptionCapacity
		found = true
	}
	return best, found
}

// Merge returns the tightest combination of introspection and description caps.
// A larger field capacity never loosens an existing smaller bound.
func (b SequenceBound) Merge(field DescriptionField) SequenceBound {
	out := b
	if field.Capacity != 0 {
		if out.DescriptionCapacity == 0 || field.Capacity < out.DescriptionCapacity {
			out.DescriptionCapacity = field.Capacity
		}
	}
	return out
}

// CheckSequenceLength enforces fixed and bounded lengths at serialize time.
// A fixed bound with ArraySize zero is rejected as malformed: callers must
// supply a positive size for fixed arrays instead of relying on a zero
// that would otherwise accept any length.
func CheckSequenceLength(n uint, bound SequenceBound) error {
	if !bound.IsUpperBound {
		if bound.ArraySize == 0 {
			return errors.New("types: fixed array has zero size") //nolint:wrapcheck // new error, nothing to wrap
		}
		if n != bound.ArraySize {
			return fmt.Errorf("types: fixed array len %d != %d", n, bound.ArraySize)
		}
		return nil
	}
	upper, ok := bound.EffectiveMax()
	if ok {
		if n > upper {
			return fmt.Errorf("types: sequence len %d above bound %d", n, upper)
		}
	}
	return nil
}

// DescriptionField mirrors a type description field capacity pair.
type DescriptionField struct {
	Capacity       uint
	StringCapacity uint
}

// CheckAgainstDescription enforces description capacities at serialize time.
// StrLen counts runes like the CDR wire bounds, not bytes.
func CheckAgainstDescription(field DescriptionField, strLen, seqLen uint, isString bool) error {
	if isString {
		if field.StringCapacity != 0 && strLen > field.StringCapacity {
			return fmt.Errorf("types: string len %d above description %d", strLen, field.StringCapacity)
		}
		if field.Capacity != 0 && strLen > field.Capacity {
			return fmt.Errorf("types: string len %d above description capacity %d", strLen, field.Capacity)
		}
		return nil
	}
	if field.Capacity != 0 && seqLen > field.Capacity {
		return fmt.Errorf("types: sequence len %d above description capacity %d", seqLen, field.Capacity)
	}
	return nil
}

// ValidateStringForSerialize enforces introspection plus description bounds for strings.
// Introspection tables carry only the IDL max; the description cap is
// merged here so serialize time sees the tightest of both. Callers that
// already merged must not merge again with a looser cap.
func ValidateStringForSerialize(s string, bound StringBound, field DescriptionField) error {
	merged := bound.Merge(field)
	err := CheckStringLength(s, merged)
	if err != nil {
		return fmt.Errorf("types: serialize string: %w", err)
	}
	return nil
}

// ValidateSequenceForSerialize enforces introspection plus description bounds for sequences.
// Introspection tables carry only the IDL max; the description cap is
// merged here so serialize time sees the tightest of both. Callers that
// already merged must not merge again with a looser cap.
func ValidateSequenceForSerialize(n uint, bound SequenceBound, field DescriptionField) error {
	merged := bound.Merge(field)
	err := CheckSequenceLength(n, merged)
	if err != nil {
		return fmt.Errorf("types: serialize sequence: %w", err)
	}
	return nil
}
