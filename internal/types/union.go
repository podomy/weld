// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"fmt"
)

// UnionDescriptor describes a discriminated union for codec use.
//
// Labels maps each discriminator value to a member index in the IDL
// declaration order. DefaultMember holds the member index of the
// default branch, or -1 when the union has no default. Discriminator
// carries the IDL discriminator kind. See DDS-XTypes 1.4 7.2.3.2.4.3.
type UnionDescriptor struct {
	// Labels maps discriminator values to member indexes.
	Labels map[int64]uint
	// DefaultMember is the default branch index, or -1 for none.
	DefaultMember int
	// Discriminator is the RosTypeID of the discriminator.
	Discriminator RosTypeID
}

// Validate checks the discriminator kind and the default index range.
func (u *UnionDescriptor) Validate() error {
	if u == nil {
		return fmt.Errorf("types: nil union descriptor: %w", ErrInvalid)
	}
	if !isUnionDiscriminatorKind(u.Discriminator) {
		return fmt.Errorf("types: union discriminator %s is not integral: %w", u.Discriminator.String(), ErrInvalid)
	}
	if u.DefaultMember < -1 {
		return fmt.Errorf("types: union default %d below -1: %w", u.DefaultMember, ErrInvalid)
	}
	return nil
}

// isUnionDiscriminatorKind reports whether kind can discriminate a union.
//
// Discriminators are integer kinds plus bool, char, and wchar, per
// 7.2.3.2.4.3. Char16 (wchar) counts as a 16 bit discriminator like
// uint16. All other introspection kinds are listed explicitly so the
// exhaustive check sees the full type id set.
func isUnionDiscriminatorKind(kind RosTypeID) bool {
	switch kind {
	case RosTypeBoolean, RosTypeChar, RosTypeOctet, RosTypeUint8, RosTypeInt8,
		RosTypeUint16, RosTypeInt16, RosTypeWChar, RosTypeUint32, RosTypeInt32,
		RosTypeUint64, RosTypeInt64:
		return true
	case RosTypeFloat, RosTypeDouble, RosTypeLongDbl,
		RosTypeString, RosTypeWString, RosTypeMessage:
		return false
	default:
		return false
	}
}

// SelectBranch resolves a discriminator value to a member index.
//
// An exact label match wins. Otherwise the default branch is used when
// present. A value with no label and no default is an invalid sample.
func (u *UnionDescriptor) SelectBranch(discriminator int64) (uint, error) {
	if u == nil {
		return 0, fmt.Errorf("types: nil union descriptor: %w", ErrInvalid)
	}
	if u.Labels != nil {
		if index, ok := u.Labels[discriminator]; ok {
			return index, nil
		}
	}
	if u.DefaultMember >= 0 {
		return uint(u.DefaultMember), nil
	}
	return 0, fmt.Errorf("types: union discriminator %d matches no branch: %w", discriminator, ErrInvalid)
}

// WriteUnionDiscriminator writes the discriminator in its IDL kind.
func (e *Encoder) WriteUnionDiscriminator(kind RosTypeID, value int64) error {
	if !isUnionDiscriminatorKind(kind) {
		return fmt.Errorf("types: union discriminator %s is not integral: %w", kind.String(), ErrInvalid)
	}
	switch kind {
	case RosTypeBoolean:
		e.WriteBool(value != 0)
		return nil
	case RosTypeChar, RosTypeOctet, RosTypeUint8, RosTypeInt8:
		e.putByte(byte(value)) //nolint:gosec // G115: discriminator truncation is IDL defined
		return nil
	case RosTypeUint16, RosTypeInt16, RosTypeWChar:
		return e.putUint16(uint16(value)) //nolint:gosec // G115: discriminator truncation is IDL defined
	case RosTypeUint32, RosTypeInt32:
		err := e.putUint32(uint32(value)) //nolint:gosec // G115: discriminator truncation is IDL defined
		if err != nil {
			return fmt.Errorf("types: encode union discriminator: %w", err)
		}
		return nil
	case RosTypeUint64, RosTypeInt64:
		err := e.putUint64(uint64(value)) //nolint:gosec // G115: two's complement reinterpret is IDL defined
		if err != nil {
			return fmt.Errorf("types: encode union discriminator: %w", err)
		}
		return nil
	case RosTypeFloat, RosTypeDouble, RosTypeLongDbl,
		RosTypeString, RosTypeWString, RosTypeMessage:
		return fmt.Errorf("types: union discriminator %s is not integral: %w", kind.String(), ErrInvalid)
	default:
		return fmt.Errorf("types: union discriminator %s is not integral: %w", kind.String(), ErrInvalid)
	}
}

// ReadUnionDiscriminator reads the discriminator before branch select.
//
// The discriminator is always read first so decode can select the
// single active member, per 7.4.3.5.3 rules 26 to 28. Values are
// sign extended to int64 for uniform label lookup.
func (d *Decoder) ReadUnionDiscriminator(kind RosTypeID) (int64, error) {
	if !isUnionDiscriminatorKind(kind) {
		return 0, fmt.Errorf("types: union discriminator %s is not integral: %w", kind.String(), ErrInvalid)
	}
	switch kind {
	case RosTypeBoolean, RosTypeChar, RosTypeOctet, RosTypeUint8, RosTypeInt8:
		return d.readUnionDisc8(kind)
	case RosTypeUint16, RosTypeInt16, RosTypeWChar:
		return d.readUnionDisc16(kind)
	case RosTypeUint32, RosTypeInt32:
		return d.readUnionDisc32(kind)
	case RosTypeUint64, RosTypeInt64:
		return d.readUnionDisc64(kind)
	case RosTypeFloat, RosTypeDouble, RosTypeLongDbl,
		RosTypeString, RosTypeWString, RosTypeMessage:
		return 0, fmt.Errorf("types: union discriminator %s is not integral: %w", kind.String(), ErrInvalid)
	default:
		return 0, fmt.Errorf("types: union discriminator %s is not integral: %w", kind.String(), ErrInvalid)
	}
}

// readUnionDisc8 reads bool and 8 bit discriminator kinds.
func (d *Decoder) readUnionDisc8(kind RosTypeID) (int64, error) {
	if kind == RosTypeBoolean {
		v, err := d.ReadBool()
		if err != nil {
			return 0, err
		}
		if v {
			return 1, nil
		}
		return 0, nil
	}
	if kind == RosTypeChar || kind == RosTypeInt8 {
		v, err := d.ReadInt8()
		if err != nil {
			return 0, err
		}
		return int64(v), nil
	}
	v, err := d.ReadUint8()
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

// readUnionDisc16 reads 16 bit discriminator kinds.
func (d *Decoder) readUnionDisc16(kind RosTypeID) (int64, error) {
	if kind == RosTypeInt16 {
		v, err := d.ReadInt16()
		if err != nil {
			return 0, err
		}
		return int64(v), nil
	}
	v, err := d.ReadUint16()
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

// readUnionDisc32 reads 32 bit discriminator kinds.
func (d *Decoder) readUnionDisc32(kind RosTypeID) (int64, error) {
	if kind == RosTypeInt32 {
		v, err := d.ReadInt32()
		if err != nil {
			return 0, err
		}
		return int64(v), nil
	}
	v, err := d.ReadUint32()
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

// readUnionDisc64 reads 64 bit discriminator kinds.
func (d *Decoder) readUnionDisc64(kind RosTypeID) (int64, error) {
	if kind == RosTypeInt64 {
		v, err := d.ReadInt64()
		if err != nil {
			return 0, err
		}
		return v, nil
	}
	v, err := d.ReadUint64()
	if err != nil {
		return 0, err
	}
	if v > ^uint64(0)>>1 {
		d.drops++
		return 0, fmt.Errorf("types: union discriminator %d overflows int64: %w", v, ErrInvalid)
	}
	return int64(v), nil
}

// Unionxt6aShape is the model input for union assignability checks.
type Unionxt6aShape struct {
	Ext            Extensibility
	DiscKind       FieldTypeID
	Labels         map[int64]uint
	MemberNames    map[uint]string
	MustUnderstand map[uint]bool
	// MemberTypes maps member indexes to their type refs for recursive
	// assignability per 7.2.5.5.9. Nil maps skip the type check for
	// backward compatibility; when both sides carry entries, each common
	// label requires recursive IsAssignablext6aFrom.
	MemberTypes map[uint]TypeRefxt6a
}

// IsUnionxt6aAssignable reports whether T1 is assignable from T2.
//
// The check covers the 7.2.5.5.9 union rules in model form: equal
// extensibility, strongly assignable discriminators, and label coverage.
// Final types need identical label sets; other kinds need at least one
// common non-default label. Must understand members of T2 must appear
// in T1. When both sides carry MemberTypes, each common label also needs
// recursive member type assignability.
func IsUnionxt6aAssignable(t1, t2 Unionxt6aShape, ignoreMemberNames bool) error {
	if t1.Ext != t2.Ext {
		return fmt.Errorf("types: union extensibility %s vs %s: %w", t1.Ext.String(), t2.Ext.String(), ErrInvalid)
	}
	if !isStronglyxt6aAssignablePrimitive(t1.DiscKind, t2.DiscKind) {
		return fmt.Errorf("types: union discriminator %d not strongly assignable from %d: %w", uint8(t1.DiscKind), uint8(t2.DiscKind), ErrInvalid)
	}
	err := checkxt6aUnionLabels(t1, t2, ignoreMemberNames)
	if err != nil {
		return err
	}
	if err := checkxt6aUnionMustUnderstand(t1, t2); err != nil {
		return err
	}
	return checkxt6aUnionMemberTypes(t1, t2, ignoreMemberNames)
}

// checkxt6aUnionMemberTypes requires recursive assignability for common labels.
//
// Nil member type maps fail closed: without type detail the checker cannot
// verify recursive assignability, so it refuses instead of passing leniently.
// When both sides carry maps, every common label must have entries on both
// sides and those entries must assign.
func checkxt6aUnionMemberTypes(t1, t2 Unionxt6aShape, ignoreMemberNames bool) error {
	if t1.MemberTypes == nil || t2.MemberTypes == nil {
		return fmt.Errorf("types: union member types missing, cannot verify: %w", ErrInvalid)
	}
	policy := Defaultxt6aConsistency()
	policy.IgnoreMemberNames = ignoreMemberNames
	for label, m2 := range t2.Labels {
		m1, ok := t1.Labels[label]
		if !ok {
			continue
		}
		r1, ok1 := t1.MemberTypes[m1]
		r2, ok2 := t2.MemberTypes[m2]
		if !ok1 || !ok2 {
			return fmt.Errorf("types: union label %d misses member type detail: %w", label, ErrInvalid)
		}
		if err := IsAssignablext6aFrom(r1, r2, policy); err != nil {
			return fmt.Errorf("types: union label %d member not assignable: %w", label, err)
		}
	}
	return nil
}

// isStronglyxt6aAssignablePrimitive requires identical primitive kinds.
func isStronglyxt6aAssignablePrimitive(t1, t2 FieldTypeID) bool {
	if t1 == FieldTypeNotSet || t2 == FieldTypeNotSet {
		return false
	}
	return t1 == t2
}

// checkxt6aUnionLabels verifies label set coverage between two unions.
func checkxt6aUnionLabels(t1, t2 Unionxt6aShape, ignoreMemberNames bool) error {
	if t1.Ext == ExtFinal {
		return checkxt6aUnionFinalLabels(t1, t2, ignoreMemberNames)
	}
	common, err := countxt6aUnionCommon(t1, t2, ignoreMemberNames)
	if err != nil {
		return err
	}
	if common == 0 {
		return fmt.Errorf("types: union shares no common label: %w", ErrInvalid)
	}
	return nil
}

// checkxt6aUnionFinalLabels requires identical label sets for final types.
func checkxt6aUnionFinalLabels(t1, t2 Unionxt6aShape, ignoreMemberNames bool) error {
	if len(t1.Labels) != len(t2.Labels) {
		return fmt.Errorf("types: final union label count %d vs %d: %w", len(t1.Labels), len(t2.Labels), ErrInvalid)
	}
	for label := range t1.Labels {
		if _, ok := t2.Labels[label]; !ok {
			return fmt.Errorf("types: final union misses label %d: %w", label, ErrInvalid)
		}
	}
	_, err := countxt6aUnionCommon(t1, t2, ignoreMemberNames)
	if err != nil {
		return err
	}
	return nil
}

// countxt6aUnionCommon counts shared labels with name agreement.
func countxt6aUnionCommon(t1, t2 Unionxt6aShape, ignoreMemberNames bool) (int, error) {
	common := 0
	for label, m2 := range t2.Labels {
		m1, ok := t1.Labels[label]
		if !ok {
			continue
		}
		common++
		if !ignoreMemberNames {
			err := checkxt6aUnionLabelName(t1, t2, label, m1, m2)
			if err != nil {
				return 0, err
			}
		}
	}
	return common, nil
}

// checkxt6aUnionLabelName verifies one shared label keeps member names.
func checkxt6aUnionLabelName(t1, t2 Unionxt6aShape, label int64, m1, m2 uint) error {
	n1, ok1 := t1.MemberNames[m1]
	n2, ok2 := t2.MemberNames[m2]
	if ok1 && ok2 && n1 != n2 {
		return fmt.Errorf("types: union label %d names %q vs %q: %w", label, n1, n2, ErrInvalid)
	}
	return nil
}

// checkxt6aUnionMustUnderstand requires T2 must understand labels in T1.
func checkxt6aUnionMustUnderstand(t1, t2 Unionxt6aShape) error {
	for label, m2 := range t2.Labels {
		if !t2.MustUnderstand[m2] {
			continue
		}
		if _, ok := t1.Labels[label]; !ok {
			return fmt.Errorf("types: union must-understand label %d missing in T1: %w", label, ErrInvalid)
		}
	}
	return nil
}
