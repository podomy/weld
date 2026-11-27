// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package types

import (
	"testing"
)

// TestPrimitivext6aTable pins Table 14 same kind rules.
func TestPrimitivext6aTable(t *testing.T) {
	t.Run("identity", checkPrimitivext6aIdentity)
	t.Run("bitmask", checkPrimitivext6aBitmask)
}

// checkPrimitivext6aIdentity verifies identical kinds assign.
func checkPrimitivext6aIdentity(t *testing.T) {
	t.Helper()
	if !IsPrimitivext6aAssignable(FieldTypeInt32, FieldTypeInt32) {
		t.Fatal("int32 from int32 rejected")
	}
	if IsPrimitivext6aAssignable(FieldTypeInt32, FieldTypeInt64) {
		t.Fatal("int32 from int64 accepted, want rejection")
	}
	if IsPrimitivext6aAssignable(FieldTypeNotSet, FieldTypeInt32) {
		t.Fatal("unset base accepted, want rejection")
	}
}

// checkPrimitivext6aBitmask verifies the uint and bitmask cross rows.
func checkPrimitivext6aBitmask(t *testing.T) {
	t.Helper()
	t.Run("uint", checkPrimitivext6aUintRows)
	t.Run("bitmask", checkPrimitivext6aBitmaskRows)
}

// checkPrimitivext6aUintRows verifies UINT from BITMASK bound rows.
func checkPrimitivext6aUintRows(t *testing.T) {
	t.Helper()
	if !IsUintFromBitmaskxt6aAssignable(FieldTypeUInt8, 8) {
		t.Fatal("uint8 from bound 8 rejected")
	}
	if IsUintFromBitmaskxt6aAssignable(FieldTypeUInt8, 9) {
		t.Fatal("uint8 from bound 9 accepted, want rejection")
	}
	if !IsUintFromBitmaskxt6aAssignable(FieldTypeUInt16, 9) || !IsUintFromBitmaskxt6aAssignable(FieldTypeUInt16, 16) {
		t.Fatal("uint16 from 9-16 rejected")
	}
	if IsUintFromBitmaskxt6aAssignable(FieldTypeUInt16, 17) {
		t.Fatal("uint16 from bound 17 accepted, want rejection")
	}
	if !IsUintFromBitmaskxt6aAssignable(FieldTypeUInt32, 17) || !IsUintFromBitmaskxt6aAssignable(FieldTypeUInt32, 32) {
		t.Fatal("uint32 from 17-32 rejected")
	}
	if IsUintFromBitmaskxt6aAssignable(FieldTypeUInt32, 33) {
		t.Fatal("uint32 from bound 33 accepted, want rejection")
	}
	if !IsUintFromBitmaskxt6aAssignable(FieldTypeUInt64, 33) || !IsUintFromBitmaskxt6aAssignable(FieldTypeUInt64, 64) {
		t.Fatal("uint64 from 33-64 rejected")
	}
	if IsUintFromBitmaskxt6aAssignable(FieldTypeInt32, 8) {
		t.Fatal("int32 from bitmask accepted, want rejection")
	}
}

// checkPrimitivext6aBitmaskRows verifies BITMASK from UINT rows.
func checkPrimitivext6aBitmaskRows(t *testing.T) {
	t.Helper()
	if !IsBitmaskFromUintxt6aAssignable(4, FieldTypeUInt8) {
		t.Fatal("bitmask bound 4 from uint8 rejected")
	}
	if IsBitmaskFromUintxt6aAssignable(4, FieldTypeUInt16) {
		t.Fatal("bitmask bound 4 from uint16 accepted, want rejection")
	}
}

// TestStringxt6aTable pins Table 15 family and bound rules.
func TestStringxt6aTable(t *testing.T) {
	if !IsStringxt6aAssignable(Kindxt6aString8, Kindxt6aString8, 10, 5, false) {
		t.Fatal("bounded string 10 from 5 rejected")
	}
	if IsStringxt6aAssignable(Kindxt6aString8, Kindxt6aString8, 5, 10, false) {
		t.Fatal("bounded string 5 from 10 accepted, want rejection")
	}
	if !IsStringxt6aAssignable(Kindxt6aString8, Kindxt6aString8, 5, 10, true) {
		t.Fatal("ignored bounds rejected")
	}
	if !IsStringxt6aAssignable(Kindxt6aString8, Kindxt6aString8, 0, 99, false) {
		t.Fatal("unbounded from bounded rejected")
	}
	if IsStringxt6aAssignable(Kindxt6aString8, Kindxt6aString8, 10, 0, false) {
		t.Fatal("bounded from unbounded accepted, want rejection")
	}
	if IsStringxt6aAssignable(Kindxt6aString8, Kindxt6aString16, 0, 0, true) {
		t.Fatal("string8 from string16 accepted, want rejection")
	}
	if IsStringxt6aAssignable(Kindxt6aPrimitive, Kindxt6aPrimitive, 0, 0, true) {
		t.Fatal("primitive kinds accepted as strings, want rejection")
	}
}

// TestCollectionxt6aTable pins Tables 16 and 17 collection rules.
func TestCollectionxt6aTable(t *testing.T) {
	t.Run("array", checkCollectionxt6aArray)
	t.Run("sequence", checkCollectionxt6aSequence)
	t.Run("map", checkCollectionxt6aMap)
}

// checkCollectionxt6aArray verifies fixed array bound equality.
func checkCollectionxt6aArray(t *testing.T) {
	t.Helper()
	if !IsArrayxt6aAssignable(4, 4, true) {
		t.Fatal("equal arrays rejected")
	}
	if IsArrayxt6aAssignable(4, 5, true) {
		t.Fatal("unequal arrays accepted, want rejection")
	}
	if IsArrayxt6aAssignable(4, 4, false) {
		t.Fatal("array with bad element accepted, want rejection")
	}
}

// checkCollectionxt6aSequence verifies sequence bound compatibility.
func checkCollectionxt6aSequence(t *testing.T) {
	t.Helper()
	if !IsSequencext6aAssignable(0, 99, true, false) {
		t.Fatal("unbounded sequence rejected")
	}
	if !IsSequencext6aAssignable(10, 5, true, false) {
		t.Fatal("sequence 10 from 5 rejected")
	}
	if IsSequencext6aAssignable(5, 10, true, false) {
		t.Fatal("sequence 5 from 10 accepted, want rejection")
	}
	if !IsSequencext6aAssignable(5, 10, true, true) {
		t.Fatal("ignored sequence bounds rejected")
	}
	if IsSequencext6aAssignable(5, 5, false, true) {
		t.Fatal("sequence with bad element accepted, want rejection")
	}
}

// checkCollectionxt6aMap verifies map key and element compatibility.
func checkCollectionxt6aMap(t *testing.T) {
	t.Helper()
	if !IsMapxt6aAssignable(10, 5, true, true, false) {
		t.Fatal("map 10 from 5 rejected")
	}
	if IsMapxt6aAssignable(5, 10, true, true, false) {
		t.Fatal("map 5 from 10 accepted, want rejection")
	}
	if IsMapxt6aAssignable(10, 5, false, true, false) {
		t.Fatal("map with bad key accepted, want rejection")
	}
	if IsMapxt6aAssignable(10, 5, true, false, false) {
		t.Fatal("map with bad element accepted, want rejection")
	}
	if !IsMapxt6aAssignable(5, 10, true, true, true) {
		t.Fatal("ignored map bounds rejected")
	}
}

// TestEnumxt6aTable pins Table 18 enum rules.
func TestEnumxt6aTable(t *testing.T) {
	mkEnum := func(bound uint32) Enumxt6aType {
		return Enumxt6aType{BitBound: bound, Literals: []Enumxt6aLiteral{{Name: "A", Value: 0}, {Name: "B", Value: 1}}}
	}
	err := IsEnumxt6aAssignable(mkEnum(8), mkEnum(8), ExtFinal, ExtFinal, false)
	if err != nil {
		t.Fatalf("identical enums rejected: %v", err)
	}
	err = IsEnumxt6aAssignable(mkEnum(8), mkEnum(16), ExtFinal, ExtFinal, false)
	if err == nil {
		t.Fatal("cross set enums accepted, want rejection")
	}
	err = IsEnumxt6aAssignable(mkEnum(8), mkEnum(8), ExtFinal, ExtAppendable, false)
	if err == nil {
		t.Fatal("extensibility mismatch accepted, want rejection")
	}
	renamed := Enumxt6aType{BitBound: 8, Literals: []Enumxt6aLiteral{{Name: "A", Value: 0}, {Name: "C", Value: 5}}}
	err = IsEnumxt6aAssignable(renamed, mkEnum(8), ExtAppendable, ExtAppendable, false)
	if err != nil {
		t.Fatalf("disjoint names without overlap rejected: %v", err)
	}
	conflictVal := Enumxt6aType{BitBound: 8, Literals: []Enumxt6aLiteral{{Name: "A", Value: 0}, {Name: "B", Value: 2}}}
	err = IsEnumxt6aAssignable(conflictVal, mkEnum(8), ExtAppendable, ExtAppendable, false)
	if err == nil {
		t.Fatal("shared name differing value accepted, want rejection")
	}
	err = IsEnumxt6aAssignable(conflictVal, mkEnum(8), ExtAppendable, ExtAppendable, true)
	if err != nil {
		t.Fatalf("ignored literal names rejected: %v", err)
	}
	shorter := Enumxt6aType{BitBound: 8, Literals: []Enumxt6aLiteral{{Name: "A", Value: 0}}}
	err = IsEnumxt6aAssignable(shorter, mkEnum(8), ExtFinal, ExtFinal, false)
	if err == nil {
		t.Fatal("final literal count mismatch accepted, want rejection")
	}
	err = IsBitmaskxt6aAssignable(Bitmaskxt6aType{BitBound: 8}, Bitmaskxt6aType{BitBound: 8}, ExtFinal, ExtFinal)
	if err != nil {
		t.Fatalf("equal bitmasks rejected: %v", err)
	}
	err = IsBitmaskxt6aAssignable(Bitmaskxt6aType{BitBound: 8}, Bitmaskxt6aType{BitBound: 16}, ExtFinal, ExtFinal)
	if err == nil {
		t.Fatal("unequal bitmask bounds accepted, want rejection")
	}
	err = IsBitmaskxt6aAssignable(Bitmaskxt6aType{BitBound: 8}, Bitmaskxt6aType{BitBound: 8}, ExtFinal, ExtAppendable)
	if err == nil {
		t.Fatal("mismatched bitmask extensibility accepted, want rejection")
	}
}

// TestStructxt6aTable pins Table 19 struct rules.
func TestStructxt6aTable(t *testing.T) {
	mkShape := func() Structxt6aShape {
		return Structxt6aShape{Ext: ExtAppendable, Members: []Structxt6aMember{
			{Name: "x", MemberID: 0, Type: FieldTypeInt32},
			{Name: "label", MemberID: 1, Type: FieldTypeString, Units: "m"},
		}}
	}
	err := IsStructxt6aAssignable(mkShape(), mkShape(), Defaultxt6aConsistency(), IsPrimitivext6aAssignable)
	if err != nil {
		t.Fatalf("identical structs rejected: %v", err)
	}
	renamed := mkShape()
	renamed.Members[0].Name = "y"
	err = IsStructxt6aAssignable(renamed, mkShape(), Defaultxt6aConsistency(), IsPrimitivext6aAssignable)
	if err == nil {
		t.Fatal("renamed member accepted, want rejection")
	}
	ignore := Defaultxt6aConsistency()
	ignore.IgnoreMemberNames = true
	err = IsStructxt6aAssignable(renamed, mkShape(), ignore, IsPrimitivext6aAssignable)
	if err != nil {
		t.Fatalf("ignored member names rejected: %v", err)
	}
	disjoint := Structxt6aShape{Ext: ExtAppendable, Members: []Structxt6aMember{{Name: "z", MemberID: 9, Type: FieldTypeInt32}}}
	err = IsStructxt6aAssignable(disjoint, mkShape(), Defaultxt6aConsistency(), IsPrimitivext6aAssignable)
	if err == nil {
		t.Fatal("disjoint structs accepted, want rejection")
	}
	keyed1 := Structxt6aShape{Ext: ExtAppendable, Members: []Structxt6aMember{{Name: "k", MemberID: 0, Type: FieldTypeInt32, IsKey: true}}}
	keyed2 := Structxt6aShape{Ext: ExtAppendable, Members: []Structxt6aMember{{Name: "k", MemberID: 0, Type: FieldTypeInt32}}}
	err = IsStructxt6aAssignable(keyed1, keyed2, Defaultxt6aConsistency(), IsPrimitivext6aAssignable)
	if err == nil {
		t.Fatal("key mismatch accepted, want rejection")
	}
	units := mkShape()
	units.Members[1].Units = "s"
	err = IsStructxt6aAssignable(units, mkShape(), Defaultxt6aConsistency(), IsPrimitivext6aAssignable)
	if err == nil {
		t.Fatal("unit mismatch accepted, want rejection")
	}
	ignoreUnits := Defaultxt6aConsistency()
	ignoreUnits.IgnoreUnits = true
	err = IsStructxt6aAssignable(units, mkShape(), ignoreUnits, IsPrimitivext6aAssignable)
	if err != nil {
		t.Fatalf("ignored units rejected: %v", err)
	}
	ext := mkShape()
	ext.Ext = ExtFinal
	err = IsStructxt6aAssignable(ext, mkShape(), Defaultxt6aConsistency(), IsPrimitivext6aAssignable)
	if err == nil {
		t.Fatal("extensibility mismatch accepted, want rejection")
	}
}

// TestUnionxt6aTable pins 7.2.5.5.9 union rules.
func TestUnionxt6aTable(t *testing.T) {
	mkUnion := func(ext Extensibility) Unionxt6aShape {
		return Unionxt6aShape{
			Ext:            ext,
			DiscKind:       FieldTypeInt32,
			Labels:         map[int64]uint{1: 0, 2: 1},
			MemberNames:    map[uint]string{0: "x", 1: "y"},
			MustUnderstand: map[uint]bool{},
			MemberTypes: map[uint]TypeRefxt6a{
				0: {Kind: Kindxt6aPrimitive, Base: FieldTypeInt32},
				1: {Kind: Kindxt6aPrimitive, Base: FieldTypeInt32},
			},
		}
	}
	err := IsUnionxt6aAssignable(mkUnion(ExtAppendable), mkUnion(ExtAppendable), false)
	if err != nil {
		t.Fatalf("identical unions rejected: %v", err)
	}
	err = IsUnionxt6aAssignable(mkUnion(ExtFinal), mkUnion(ExtAppendable), false)
	if err == nil {
		t.Fatal("extensibility mismatch accepted, want rejection")
	}
	badDisc := mkUnion(ExtAppendable)
	badDisc.DiscKind = FieldTypeInt64
	err = IsUnionxt6aAssignable(badDisc, mkUnion(ExtAppendable), false)
	if err == nil {
		t.Fatal("discriminator mismatch accepted, want rejection")
	}
	disjoint := Unionxt6aShape{Ext: ExtAppendable, DiscKind: FieldTypeInt32, Labels: map[int64]uint{9: 0}, MemberTypes: map[uint]TypeRefxt6a{0: {Kind: Kindxt6aPrimitive, Base: FieldTypeInt32}}}
	err = IsUnionxt6aAssignable(disjoint, mkUnion(ExtAppendable), false)
	if err == nil {
		t.Fatal("label disjoint unions accepted, want rejection")
	}
	final := mkUnion(ExtFinal)
	final.Labels = map[int64]uint{1: 0}
	err = IsUnionxt6aAssignable(final, mkUnion(ExtFinal), false)
	if err == nil {
		t.Fatal("final label subset accepted, want rejection")
	}
	must := mkUnion(ExtAppendable)
	must.Labels[3] = 2
	must.MustUnderstand = map[uint]bool{2: true}
	err = IsUnionxt6aAssignable(mkUnion(ExtAppendable), must, false)
	if err == nil {
		t.Fatal("missing must-understand label accepted, want rejection")
	}
}

// TestDispatcherxt6aCases pins dispatcher routing plus cross kinds.
func TestDispatcherxt6aCases(t *testing.T) {
	policy := Defaultxt6aConsistency()
	err := IsAssignablext6aFrom(TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt32}, TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt32}, policy)
	if err != nil {
		t.Fatalf("primitive dispatch rejected: %v", err)
	}
	err = IsAssignablext6aFrom(TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt32}, TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt64}, policy)
	if err == nil {
		t.Fatal("primitive mismatch accepted, want rejection")
	}
	bitmask := &Bitmaskxt6aType{BitBound: 8}
	err = IsAssignablext6aFrom(TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeUInt8}, TypeRefxt6a{Kind: Kindxt6aBitmask, Bitmask: bitmask}, policy)
	if err != nil {
		t.Fatalf("uint8 from bitmask 8 rejected: %v", err)
	}
	err = IsAssignablext6aFrom(TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeUInt16}, TypeRefxt6a{Kind: Kindxt6aBitmask, Bitmask: bitmask}, policy)
	if err == nil {
		t.Fatal("uint16 from bitmask 8 accepted, want rejection")
	}
	err = IsAssignablext6aFrom(TypeRefxt6a{Kind: Kindxt6aString8, Bound: 10}, TypeRefxt6a{Kind: Kindxt6aString8, Bound: 5}, policy)
	if err != nil {
		t.Fatalf("string 10 from 5 rejected: %v", err)
	}
	elem1 := &TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt32}
	elem2 := &TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt32}
	err = IsAssignablext6aFrom(TypeRefxt6a{Kind: Kindxt6aSequence, Bound: 10, Elem: elem1}, TypeRefxt6a{Kind: Kindxt6aSequence, Bound: 5, Elem: elem2}, policy)
	if err != nil {
		t.Fatalf("sequence dispatch rejected: %v", err)
	}
	arrBad := &TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt64}
	err = IsAssignablext6aFrom(TypeRefxt6a{Kind: Kindxt6aSequence, Bound: 10, Elem: elem1}, TypeRefxt6a{Kind: Kindxt6aSequence, Bound: 5, Elem: arrBad}, policy)
	if err == nil {
		t.Fatal("sequence element mismatch accepted, want rejection")
	}
	err = IsAssignablext6aFrom(TypeRefxt6a{Kind: Kindxt6aPrimitive, Base: FieldTypeInt32}, TypeRefxt6a{Kind: Kindxt6aString8, Bound: 5}, policy)
	if err == nil {
		t.Fatal("primitive from string accepted, want rejection")
	}
	err = IsAssignablext6aFrom(TypeRefxt6a{Kind: Kindxt6a(99)}, TypeRefxt6a{Kind: Kindxt6a(99)}, policy)
	if err == nil {
		t.Fatal("unknown kind accepted, want rejection")
	}
}

// TestConsistencyxt6aPolicy pins 7.6.3.4.2 evaluation steps.
func TestConsistencyxt6aPolicy(t *testing.T) {
	if Defaultxt6aConsistency().Kind != Consistencyxt6aAllowCoercion {
		t.Fatal("default kind is not ALLOW_TYPE_COERCION")
	}
	if Consistencyxt6aDisallowCoercion.String() != "DISALLOW_TYPE_COERCION" || Consistencyxt6aAllowCoercion.String() != "ALLOW_TYPE_COERCION" {
		t.Fatal("kind strings wrong")
	}
	if TypeConsistencyxt6aKind(99).String() != "UNKNOWN" {
		t.Fatal("unknown kind string wrong")
	}
	disallow := TypeConsistencyxt6aPolicy{Kind: Consistencyxt6aDisallowCoercion}
	err := Evaluatext6aConsistency(disallow, true, false, true, "A", "A")
	if err != nil {
		t.Fatalf("disallow with equivalence rejected: %v", err)
	}
	err = Evaluatext6aConsistency(disallow, false, true, true, "A", "A")
	if err == nil {
		t.Fatal("disallow without equivalence accepted, want rejection")
	}
	allow := Defaultxt6aConsistency()
	err = Evaluatext6aConsistency(allow, false, true, true, "A", "A")
	if err != nil {
		t.Fatalf("allow with assignable rejected: %v", err)
	}
	err = Evaluatext6aConsistency(allow, false, false, true, "A", "A")
	if err == nil {
		t.Fatal("allow without assignable accepted, want rejection")
	}
	err = Evaluatext6aConsistency(allow, false, false, false, "pkg/A", "pkg/A")
	if err != nil {
		t.Fatalf("name fallback rejected: %v", err)
	}
	err = Evaluatext6aConsistency(allow, false, false, false, "pkg/A", "pkg/B")
	if err == nil {
		t.Fatal("name mismatch accepted, want rejection")
	}
	force := Defaultxt6aConsistency()
	force.ForceTypeValidation = true
	err = Evaluatext6aConsistency(force, false, false, false, "pkg/A", "pkg/A")
	if err == nil {
		t.Fatal("forced validation without info accepted, want rejection")
	}
}

// TestUnitsxt6aCompat pins wildcard and one-sided unit rules.
func TestUnitsxt6aCompat(t *testing.T) {
	allow := Defaultxt6aConsistency()
	if !AreUnitsxt6aCompatible("meter; m", "m", allow) {
		t.Fatal("shared unit names rejected")
	}
	if AreUnitsxt6aCompatible("meter", "second", allow) {
		t.Fatal("disjoint units accepted, want rejection")
	}
	if !AreUnitsxt6aCompatible("__ANY__", "meter", allow) {
		t.Fatal("wildcard rejected")
	}
	if !AreUnitsxt6aCompatible("", "", allow) {
		t.Fatal("both unspecified rejected")
	}
	if AreUnitsxt6aCompatible("", "meter", allow) {
		t.Fatal("one-sided units accepted without ignore, want rejection")
	}
	ignoreUnspec := allow
	ignoreUnspec.IgnoreUnspecifiedUnits = true
	if !AreUnitsxt6aCompatible("", "meter", ignoreUnspec) {
		t.Fatal("one-sided units rejected with ignore_unspecified_units")
	}
	ignoreAll := allow
	ignoreAll.IgnoreUnits = true
	if !AreUnitsxt6aCompatible("meter", "second", ignoreAll) {
		t.Fatal("disjoint units rejected with ignore_units")
	}
	if !AreRangesxt6aCompatible(Rangext6aLimit{HasMin: true, Min: -20, HasMax: true, Max: 30}, Rangext6aLimit{HasMin: true, Min: 5, HasMax: true, Max: 50}, false) {
		t.Fatal("overlapping ranges rejected")
	}
	if AreRangesxt6aCompatible(Rangext6aLimit{HasMin: true, Min: -20, HasMax: true, Max: 30}, Rangext6aLimit{HasMin: true, Min: 50}, false) {
		t.Fatal("disjoint ranges accepted, want rejection")
	}
	if !AreRangesxt6aCompatible(Rangext6aLimit{HasMin: true, Min: -20}, Rangext6aLimit{HasMax: true, Max: 30}, true) {
		t.Fatal("ranges rejected with ignore_ranges")
	}
}
