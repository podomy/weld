// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later
package types

import (
	"reflect"
	"strings"
	"sync"
	"testing"
	"unsafe"
)

// TestIntrospectionIdentifier checks the C introspection identifier.
func TestIntrospectionIdentifier(t *testing.T) {
	if IntrospectionIdentifier != "rosidl_typesupport_introspection_c" {
		t.Fatalf("identifier = %q, want rosidl_typesupport_introspection_c", IntrospectionIdentifier)
	}
}

// checkMemberRecordOffsets verifies offsets and counts.
func checkMemberRecordOffsets(t *testing.T, table *MessageMembers) {
	t.Helper()
	if len(table.Members) != 2 {
		t.Fatalf("member count = %d, want 2", len(table.Members))
	}
	var zero SolidPrimitive
	if table.Members[0].Offset != uint32(unsafe.Offsetof(zero.Type)) {
		t.Fatalf("type offset = %d, want %d", table.Members[0].Offset, unsafe.Offsetof(zero.Type))
	}
	if table.Members[1].Offset != uint32(unsafe.Offsetof(zero.Dimensions)) {
		t.Fatalf("dimensions offset = %d, want %d", table.Members[1].Offset, unsafe.Offsetof(zero.Dimensions))
	}
	if table.Members[0].DefaultValue != nil || table.Members[1].DefaultValue != nil {
		t.Fatal("default value must always be nil")
	}
}

// checkMemberRecordFuncs verifies resize and type rules.
func checkMemberRecordFuncs(t *testing.T, table *MessageMembers) {
	t.Helper()
	if table.Members[0].ResizeFunc != nil {
		t.Fatal("scalar member must have nil resize")
	}
	if table.Members[1].ResizeFunc == nil {
		t.Fatal("sequence member must have resize")
	}
	if table.Members[0].TypeID != RosTypeUint8 {
		t.Fatalf("type id = %s, want ROS_TYPE_UINT8", table.Members[0].TypeID.String())
	}
	if table.Members[1].StringUpperBound != 0 {
		t.Fatalf("sequence string upper bound = %d, want 0", table.Members[1].StringUpperBound)
	}
}

// checkMemberRecordCovariance verifies covariance fixed array rules.
func checkMemberRecordCovariance(t *testing.T) {
	t.Helper()
	cov := CovarianceMessageMembers()
	err := cov.Validate()
	if err != nil {
		t.Fatalf("validate covariance: %v", err)
	}
	if cov.Members[0].ResizeFunc != nil {
		t.Fatal("fixed array must have nil resize")
	}
	if cov.Members[0].SizeFunc == nil {
		t.Fatal("fixed array must have size function")
	}
}

// TestIntrospectionMemberRecord checks member fields, offsets, and nil rules.
func TestIntrospectionMemberRecord(t *testing.T) {
	ClearMessageRegistryForTest()
	table := SolidPrimitiveMessageMembers()
	err := table.Validate()
	if err != nil {
		t.Fatalf("validate solid primitive: %v", err)
	}
	t.Run("offsets", func(t *testing.T) { checkMemberRecordOffsets(t, table) })
	t.Run("funcs", func(t *testing.T) { checkMemberRecordFuncs(t, table) })
	t.Run("covariance", checkMemberRecordCovariance)
}

// TestIntrospectionContainer checks namespace, name, counts, size, and init fini.
func TestIntrospectionContainer(t *testing.T) {
	table := SolidPrimitiveMessageMembers()
	if table.Namespace != "shape_msgs__msg" || table.Name != "SolidPrimitive" {
		t.Fatalf("container = %s/%s", table.Namespace, table.Name)
	}
	if table.MemberCount != 2 {
		t.Fatalf("member count = %d, want 2", table.MemberCount)
	}
	var zero SolidPrimitive
	if table.SizeOf != uint(unsafe.Sizeof(zero)) {
		t.Fatalf("size_of = %d, want %d", table.SizeOf, unsafe.Sizeof(zero))
	}
	if table.HasAnyKey {
		t.Fatal("solid primitive must have no key")
	}
	var p SolidPrimitive
	err := table.Init(&p, MessageInitAll)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	err = table.Fini(&p)
	if err != nil {
		t.Fatalf("fini: %v", err)
	}
}

// TestIntrospectionServiceTables checks request response event tables.
func TestIntrospectionServiceTables(t *testing.T) {
	ClearMessageRegistryForTest()
	req := SolidPrimitiveMessageMembers()
	req.Namespace = "example__srv"
	req.Name = "AddTwoInts_Request"
	resp := CovarianceMessageMembers()
	resp.Namespace = "example__srv"
	resp.Name = "AddTwoInts_Response"
	evt := SolidPrimitiveMessageMembers()
	evt.Namespace = "example__srv"
	evt.Name = "AddTwoInts_Event"
	svc := &ServiceMembers{Namespace: "example__srv", Name: "AddTwoInts", Request: req, Response: resp, Event: evt}
	err := svc.Validate()
	if err != nil {
		t.Fatalf("validate service: %v", err)
	}
	err = svc.EnsurePatched()
	if err != nil {
		t.Fatalf("ensure patched: %v", err)
	}
	if svc.Request == nil || svc.Response == nil || svc.Event == nil {
		t.Fatal("service tables must all be present")
	}
}

// TestIntrospectionLazyNestedPatch checks pointers patch on first call.
func TestIntrospectionLazyNestedPatch(t *testing.T) {
	ClearMessageRegistryForTest()
	inner := NewMessageMembers("pkg__msg", "Inner", 4, []MessageMember{
		NewScalarMember("value", RosTypeInt32, 0),
	}, func(base any, init MessageInit) error { return nil }, func(base any) error { return nil })
	err := RegisterMessageMembers(inner)
	if err != nil {
		t.Fatalf("register inner: %v", err)
	}
	type Outer struct {
		Inner InnerFixture
	}
	_ = Outer{}
	member := NewNestedMember("inner", 0, "Inner", "pkg__msg", "Inner")
	if member.Members != nil {
		t.Fatal("nested pointer must start nil before first call")
	}
	got, err := member.NestedMembers()
	if err != nil {
		t.Fatalf("nested members: %v", err)
	}
	if got != inner {
		t.Fatal("lazy patch did not return registered table")
	}
	if member.Members == nil {
		t.Fatal("nested pointer was not patched on first call")
	}
}

// InnerFixture is a trivial nested payload for lazy patch tests.
type InnerFixture struct {
	Value int32
}

// checkValueKindBasics verifies basic kind mappings and returns bool vector.
func checkValueKindBasics(t *testing.T) MessageMember {
	t.Helper()
	str := NewStringMember("name", RosTypeString, 0, 0)
	if ValueKindFor(str) != ValueBoundedString {
		t.Fatalf("string kind = %s", ValueKindFor(str).String())
	}
	seq := NewSequenceMember("dims", RosTypeDouble, 3, 8, "Dimensions")
	if ValueKindFor(seq) != ValueSpanSequence {
		t.Fatalf("sequence kind = %s", ValueKindFor(seq).String())
	}
	bv := NewSequenceMember("flags", RosTypeBoolean, 0, 0, "Flags")
	if ValueKindFor(bv) != ValueBoolVector {
		t.Fatalf("bool vector kind = %s", ValueKindFor(bv).String())
	}
	prim := NewScalarMember("count", RosTypeInt32, 0)
	if ValueKindFor(prim) != ValuePrimitive {
		t.Fatalf("primitive kind = %s", ValueKindFor(prim).String())
	}
	return bv
}

// checkValueKindArrays verifies array kind mappings.
func checkValueKindArrays(t *testing.T) {
	t.Helper()
	fixedBool := NewFixedArrayMember("flags", RosTypeBoolean, 4, 0, "Flags")
	if fixedBool.IsBoolVector() {
		t.Fatal("fixed bool array must not be a bool vector")
	}
	if ValueKindFor(fixedBool) != ValueSpanSequence {
		t.Fatalf("fixed bool kind = %s, want span_sequence", ValueKindFor(fixedBool).String())
	}
	strSeq := NewSequenceMember("names", RosTypeString, 4, 0, "Names")
	if ValueKindFor(strSeq) != ValueStringSequence {
		t.Fatalf("string sequence kind = %s, want string_sequence", ValueKindFor(strSeq).String())
	}
	fixedStr := NewFixedArrayMember("names", RosTypeString, 2, 0, "Names")
	if ValueKindFor(fixedStr) != ValueStringSequence {
		t.Fatalf("fixed string kind = %s, want string_sequence", ValueKindFor(fixedStr).String())
	}
}

// checkValueKindContained verifies self containment and loan rules.
func checkValueKindContained(t *testing.T, bv MessageMember) {
	t.Helper()
	plain := SolidPrimitiveMessageMembers()
	plain.Members[0] = NewScalarMember("type", RosTypeUint8, 0)
	plain.Members[1] = NewScalarMember("extra", RosTypeInt32, 1)
	ok, err := IsSelfContained(plain)
	if err != nil {
		t.Fatalf("self contained: %v", err)
	}
	if !ok {
		t.Fatal("all primitive message must be self contained")
	}
	withString := SolidPrimitiveMessageMembers()
	withString.Members[0] = NewStringMember("name", RosTypeString, 0, 0)
	ok, err = IsSelfContained(withString)
	if err != nil {
		t.Fatalf("self contained string: %v", err)
	}
	if ok {
		t.Fatal("string member must clear self containment")
	}
	withSeq := SolidPrimitiveMessageMembers()
	ok, err = IsSelfContained(withSeq)
	if err != nil {
		t.Fatalf("self contained seq: %v", err)
	}
	if ok {
		t.Fatal("sequence member must clear self containment")
	}
	checkValueKindLoan(t, withSeq, bv)
}

// checkValueKindLoan verifies loan and memcpy rules.
func checkValueKindLoan(t *testing.T, withSeq *MessageMembers, bv MessageMember) {
	t.Helper()
	loan, err := CanLoan(withSeq)
	if err != nil {
		t.Fatalf("can loan: %v", err)
	}
	if loan {
		t.Fatal("sequence type must disable loan")
	}
	safe, err := IsMemcpySafe(withSeq)
	if err != nil {
		t.Fatalf("memcpy safe: %v", err)
	}
	if safe {
		t.Fatal("sequence type must disable trivial path")
	}
	boolVecTable := NewMessageMembers("pkg__msg", "BoolVec", 8, []MessageMember{bv}, func(base any, init MessageInit) error { return nil }, func(base any) error { return nil })
	ok, err := IsSelfContained(boolVecTable)
	if err != nil {
		t.Fatalf("self contained boolvec: %v", err)
	}
	if ok {
		t.Fatal("bool vector must clear self containment")
	}
}

// TestIntrospectionValueTypes checks string sequence and bool vector mapping.
func TestIntrospectionValueTypes(t *testing.T) {
	var bv MessageMember
	t.Run("basics", func(t *testing.T) { bv = checkValueKindBasics(t) })
	t.Run("arrays", checkValueKindArrays)
	t.Run("contained", func(t *testing.T) { checkValueKindContained(t, bv) })
}

// TestIntrospectionRosTypeNumbering checks introspection numbers stay distinct.
func TestIntrospectionRosTypeNumbering(t *testing.T) {
	if RosTypeFloat != 1 || RosTypeDouble != 2 || RosTypeMessage != 18 {
		t.Fatalf("ros type numbering changed: float=%d double=%d message=%d", RosTypeFloat, RosTypeDouble, RosTypeMessage)
	}
	if RosTypeBool != RosTypeBoolean || RosTypeByte != RosTypeOctet || RosTypeFloat32 != RosTypeFloat {
		t.Fatal("backward compatible aliases must match")
	}
}

// checkArrayAccessorResize verifies resize and size.
func checkArrayAccessorResize(t *testing.T, dims *MessageMember, p *SolidPrimitive) {
	t.Helper()
	err := dims.Resize(p, 2)
	if err != nil {
		t.Fatalf("resize: %v", err)
	}
	n, err := dims.Size(p)
	if err != nil {
		t.Fatalf("size: %v", err)
	}
	if n != 2 {
		t.Fatalf("size = %d, want 2", n)
	}
}

// checkArrayAccessorAssignFetch verifies assign and fetch.
func checkArrayAccessorAssignFetch(t *testing.T, dims *MessageMember, p *SolidPrimitive) {
	t.Helper()
	err := dims.Assign(p, 0, float64(1.5))
	if err != nil {
		t.Fatalf("assign: %v", err)
	}
	var out float64
	err = dims.Fetch(p, 0, &out)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if out != 1.5 {
		t.Fatalf("fetch = %v, want 1.5", out)
	}
}

// checkArrayAccessorGets verifies get variants.
func checkArrayAccessorGets(t *testing.T, dims *MessageMember, p *SolidPrimitive) {
	t.Helper()
	got, err := dims.GetConst(p, 0)
	if err != nil {
		t.Fatalf("get const: %v", err)
	}
	f, ok := got.(float64)
	if !ok || f != 1.5 {
		t.Fatalf("get const = %v", got)
	}
	copied, err := dims.GetCopy(p, 0)
	if err != nil {
		t.Fatalf("get copy: %v", err)
	}
	cf, ok := copied.(float64)
	if !ok || cf != 1.5 {
		t.Fatalf("get copy = %v", copied)
	}
	mut, err := dims.Get(p, 0)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	mf, ok := mut.(float64)
	if !ok || mf != 1.5 {
		t.Fatalf("get = %v", mut)
	}
}

// checkArrayAccessorBounds verifies bound and scalar errors.
func checkArrayAccessorBounds(t *testing.T, table *MessageMembers, dims *MessageMember, p *SolidPrimitive) {
	t.Helper()
	err := dims.Resize(p, SolidPrimitiveDimensionsMaxSize+1)
	if err == nil {
		t.Fatal("resize above bound must fail")
	}
	scalar := &table.Members[0]
	_, err = scalar.Size(p)
	if err == nil {
		t.Fatal("scalar size must fail")
	}
	err = scalar.Resize(p, 1)
	if err == nil {
		t.Fatal("scalar resize must fail")
	}
}

// TestIntrospectionArrayAccessors checks size get fetch assign and resize path.
func TestIntrospectionArrayAccessors(t *testing.T) {
	table := SolidPrimitiveMessageMembers()
	p := SolidPrimitive{}
	err := table.Init(&p, MessageInitAll)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	dims := &table.Members[1]
	t.Run("resize", func(t *testing.T) { checkArrayAccessorResize(t, dims, &p) })
	t.Run("assign", func(t *testing.T) { checkArrayAccessorAssignFetch(t, dims, &p) })
	t.Run("gets", func(t *testing.T) { checkArrayAccessorGets(t, dims, &p) })
	t.Run("bounds", func(t *testing.T) { checkArrayAccessorBounds(t, table, dims, &p) })
}

// checkMemberValidateBasics verifies name default and type checks.
func checkMemberValidateBasics(t *testing.T) {
	t.Helper()
	bad := NewScalarMember("", RosTypeInt32, 0)
	err := bad.Validate()
	if err == nil {
		t.Fatal("empty name must fail")
	}
	withDefault := NewScalarMember("x", RosTypeInt32, 0)
	withDefault.DefaultValue = int32(1)
	err = withDefault.Validate()
	if err == nil {
		t.Fatal("non nil default must fail")
	}
	unset := NewScalarMember("x", RosTypeID(0), 0)
	err = unset.Validate()
	if err == nil {
		t.Fatal("unset type id must fail")
	}
	nonStringBound := NewScalarMember("x", RosTypeInt32, 0)
	nonStringBound.StringUpperBound = 4
	err = nonStringBound.Validate()
	if err == nil {
		t.Fatal("string bound on non string must fail")
	}
}

// checkMemberValidateNested verifies nested reference checks.
func checkMemberValidateNested(t *testing.T) {
	t.Helper()
	stray := NewScalarMember("x", RosTypeInt32, 0)
	stray.Members = SolidPrimitiveMessageMembers()
	err := stray.Validate()
	if err == nil {
		t.Fatal("non message with nested table must fail")
	}
	strayRef := NewScalarMember("x", RosTypeInt32, 0)
	strayRef.NestedNamespace = "a"
	strayRef.NestedName = "b"
	err = strayRef.Validate()
	if err == nil {
		t.Fatal("non message with nested ref must fail")
	}
	halfRef := NewNestedMember("n", 0, "N", "pkg__msg", "")
	err = halfRef.Validate()
	if err == nil {
		t.Fatal("nested with half reference must fail")
	}
	noRef := MessageMember{Name: "n", TypeID: RosTypeMessage}
	err = noRef.Validate()
	if err == nil {
		t.Fatal("nested with no table and no ref must fail")
	}
	keySeq := NewSequenceMember("k", RosTypeUint8, 0, 0, "K")
	keySeq.IsKey = true
	err = keySeq.Validate()
	if err == nil {
		t.Fatal("sequence key must fail")
	}
}

// checkMemberValidateBuffer verifies buffer and field checks.
func checkMemberValidateBuffer(t *testing.T) {
	t.Helper()
	bufScalar := NewScalarMember("d", RosTypeUint8, 0)
	bufScalar.IsRosidlBuffer = true
	err := bufScalar.Validate()
	if err == nil {
		t.Fatal("scalar buffer must fail")
	}
	bufFixed := NewFixedArrayMember("d", RosTypeUint8, 4, 0, "Data")
	bufFixed.IsRosidlBuffer = true
	err = bufFixed.Validate()
	if err == nil {
		t.Fatal("fixed array buffer must fail")
	}
	bufWrong := NewSequenceMember("d", RosTypeInt32, 0, 0, "Data")
	bufWrong.IsRosidlBuffer = true
	err = bufWrong.Validate()
	if err == nil {
		t.Fatal("non uint8 buffer must fail")
	}
	noField := NewSequenceMember("d", RosTypeDouble, 0, 0, "")
	err = noField.Validate()
	if err == nil {
		t.Fatal("empty field wiring must fail")
	}
	fixedZero := NewFixedArrayMember("a", RosTypeDouble, 0, 0, "A")
	err = fixedZero.Validate()
	if err == nil {
		t.Fatal("zero fixed size must fail")
	}
}

// TestIntrospectionMemberValidate checks construction validation fails fast.
func TestIntrospectionMemberValidate(t *testing.T) {
	t.Run("basics", checkMemberValidateBasics)
	t.Run("nested", checkMemberValidateNested)
	t.Run("buffer", checkMemberValidateBuffer)
}

// TestIntrospectionContainerValidate checks count size key and offset mismatches.
func TestIntrospectionContainerValidate(t *testing.T) {
	base := SolidPrimitiveMessageMembers()
	badCount := *base
	badCount.MemberCount = 99
	err := badCount.Validate()
	if err == nil {
		t.Fatal("member count mismatch must fail")
	}
	badSize := *base
	badSize.SizeOf = 0
	err = badSize.Validate()
	if err == nil {
		t.Fatal("zero size must fail")
	}
	badKey := *base
	badKey.Members[0].IsKey = true
	badKey.HasAnyKey = false
	err = badKey.Validate()
	if err == nil {
		t.Fatal("key presence mismatch must fail")
	}
	badOrder := *base
	badOrder.Members[0].Offset = 100
	badOrder.Members[1].Offset = 10
	err = badOrder.Validate()
	if err == nil {
		t.Fatal("decreasing offsets must fail")
	}
	equalOff := *base
	equalOff.Members[1].Offset = equalOff.Members[0].Offset
	err = equalOff.Validate()
	if err == nil {
		t.Fatal("equal offsets for distinct fields must fail")
	}
	noInit := *base
	noInit.InitFunc = nil
	err = noInit.Validate()
	if err == nil {
		t.Fatal("nil init must fail")
	}
	empty := &MessageMembers{}
	err = empty.Validate()
	if err == nil {
		t.Fatal("empty namespace must fail")
	}
	_, err = base.MemberByName("missing_field")
	if err == nil {
		t.Fatal("member by name miss must fail")
	}
	err = base.Init(nil, MessageInitAll)
	if err == nil {
		t.Fatal("init nil base must fail")
	}
	err = base.Fini(nil)
	if err == nil {
		t.Fatal("fini nil base must fail")
	}
	err = base.Init(&SolidPrimitive{}, MessageInit(99))
	if err == nil {
		t.Fatal("bad init mode must fail")
	}
}

// TestIntrospectionNestedEdgeCases checks missing refs and recursive patch.
func TestIntrospectionNestedEdgeCases(t *testing.T) {
	ClearMessageRegistryForTest()
	scalar := NewScalarMember("x", RosTypeInt32, 0)
	_, err := scalar.NestedMembers()
	if err == nil {
		t.Fatal("nested on scalar must fail")
	}
	orphan := NewNestedMember("n", 0, "N", "no__pkg", "Nope")
	_, err = orphan.NestedMembers()
	if err == nil {
		t.Fatal("unregistered nested must fail")
	}
	ClearMessageRegistryForTest()
	headerTable := HeaderFixtureMessageMembers()
	err = RegisterMessageMembers(headerTable)
	if err != nil {
		t.Fatalf("register header: %v", err)
	}
	stamped := HeaderStampedMessageMembers()
	err = stamped.EnsureNestedPatched()
	if err != nil {
		t.Fatalf("recursive patch: %v", err)
	}
	if stamped.Members[0].Members == nil {
		t.Fatal("nested header was not patched")
	}
	if stamped.Members[0].Members.Name != "HeaderFixture" {
		t.Fatalf("nested name = %q", stamped.Members[0].Members.Name)
	}
}

// TestIntrospectionSelfContainedErrors checks unpatched and cyclic tables.
func TestIntrospectionSelfContainedErrors(t *testing.T) {
	ClearMessageRegistryForTest()
	orphan := NewMessageMembers("pkg__msg", "Orphan", 8, []MessageMember{
		NewNestedMember("n", 0, "N", "missing__pkg", "Missing"),
	}, func(base any, init MessageInit) error { return nil }, func(base any) error { return nil })
	_, err := IsSelfContained(orphan)
	if err == nil {
		t.Fatal("unpatched lookup must error")
	}
	_, err = CanLoan(orphan)
	if err == nil {
		t.Fatal("can loan lookup must error")
	}
	_, err = IsMemcpySafe(orphan)
	if err == nil {
		t.Fatal("memcpy lookup must error")
	}
	_, err = IsSelfContained(nil)
	if err == nil {
		t.Fatal("nil table must error")
	}
	ClearMessageRegistryForTest()
	cyc := NewMessageMembers("pkg__msg", "Cyc", 8, []MessageMember{
		NewNestedMember("self", 0, "Self", "pkg__msg", "Cyc"),
	}, func(base any, init MessageInit) error { return nil }, func(base any) error { return nil })
	err = RegisterMessageMembers(cyc)
	if err != nil {
		t.Fatalf("register cyc: %v", err)
	}
	m, err := cyc.Members[0].NestedMembers()
	if err != nil {
		t.Fatalf("self patch: %v", err)
	}
	if m != cyc {
		t.Fatal("self patch must return same table")
	}
	_, err = IsSelfContained(cyc)
	if err == nil {
		t.Fatal("cycle must error")
	}
}

// TestIntrospectionStructFieldErrors checks base shape validation.
func TestIntrospectionStructFieldErrors(t *testing.T) {
	_, err := structField(nil, "X")
	if err == nil {
		t.Fatal("nil base must fail")
	}
	x := 5
	_, err = structField(&x, "X")
	if err == nil {
		t.Fatal("non struct base must fail")
	}
	p := SolidPrimitive{}
	_, err = structField(&p, "Missing")
	if err == nil {
		t.Fatal("missing field must fail")
	}
	_, _, _, err = arrayDataSlice(&p, "Type")
	if err == nil {
		t.Fatal("scalar field as array must fail")
	}
	_, _, _, err = arrayDataSlice(&p, "Missing")
	if err == nil {
		t.Fatal("missing array field must fail")
	}
	bad := struct {
		W struct {
			Data int
		}
	}{}
	_, _, _, err = arrayDataSlice(&bad, "W")
	if err == nil {
		t.Fatal("malformed wrapper must fail")
	}
	parent := &SolidPrimitive{}
	dims, err := structField(parent, "Dimensions")
	if err != nil {
		t.Fatalf("dimensions field: %v", err)
	}
	err = syncSequenceTriple(dims)
	if err != nil {
		t.Fatalf("sync triple: %v", err)
	}
	empty := struct{ V int }{V: 1}
	ev := reflect.ValueOf(&empty)
	err = syncSequenceTriple(ev.Elem())
	if err == nil {
		t.Fatal("sync on non wrapper must fail")
	}
}

// checkAccessorGetErrors verifies get out of range errors.
func checkAccessorGetErrors(t *testing.T, dims *MessageMember, p *SolidPrimitive) {
	t.Helper()
	_, err := dims.GetConst(p, 5)
	if err == nil {
		t.Fatal("get const out of range must fail")
	}
	_, err = dims.Get(p, 5)
	if err == nil {
		t.Fatal("get out of range must fail")
	}
}

// checkAccessorFetchErrors verifies fetch type and range errors.
func checkAccessorFetchErrors(t *testing.T, dims *MessageMember, p *SolidPrimitive) {
	t.Helper()
	var out int32
	err := dims.Fetch(p, 0, &out)
	if err == nil {
		t.Fatal("fetch type mismatch must fail")
	}
	err = dims.Fetch(p, 0, nil)
	if err == nil {
		t.Fatal("fetch nil out must fail")
	}
	err = dims.Fetch(p, 9, &out)
	if err == nil {
		t.Fatal("fetch out of range must fail")
	}
}

// checkAccessorAssignErrors verifies assign type and range errors.
func checkAccessorAssignErrors(t *testing.T, dims *MessageMember, p *SolidPrimitive) {
	t.Helper()
	err := dims.Assign(p, 0, "not a float")
	if err == nil {
		t.Fatal("assign type mismatch must fail")
	}
	err = dims.Assign(p, 9, float64(1))
	if err == nil {
		t.Fatal("assign out of range must fail")
	}
	err = dims.Assign(p, 0, nil)
	if err == nil {
		t.Fatal("assign nil must fail")
	}
	err = dims.Assign(p, 0, int32(1))
	if err == nil {
		t.Fatal("assign int32 to float64 must fail without conversion")
	}
}

// checkAccessorFixedErrors verifies fixed array errors.
func checkAccessorFixedErrors(t *testing.T) {
	t.Helper()
	cov := CovarianceMessageMembers()
	var h CovarianceHolder
	err := cov.Init(&h, MessageInitAll)
	if err != nil {
		t.Fatalf("cov init: %v", err)
	}
	fixed := &cov.Members[0]
	err = fixed.Resize(&h, 2)
	if err == nil {
		t.Fatal("fixed array resize must fail")
	}
	n, err := fixed.Size(&h)
	if err != nil {
		t.Fatalf("fixed size: %v", err)
	}
	if n != 36 {
		t.Fatalf("fixed size = %d, want 36", n)
	}
}

// TestIntrospectionAccessorErrors checks out of range and type mismatches.
func TestIntrospectionAccessorErrors(t *testing.T) {
	table := SolidPrimitiveMessageMembers()
	p := SolidPrimitive{}
	err := table.Init(&p, MessageInitAll)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	dims := &table.Members[1]
	err = dims.Resize(&p, 1)
	if err != nil {
		t.Fatalf("resize: %v", err)
	}
	t.Run("gets", func(t *testing.T) { checkAccessorGetErrors(t, dims, &p) })
	t.Run("fetch", func(t *testing.T) { checkAccessorFetchErrors(t, dims, &p) })
	t.Run("assign", func(t *testing.T) { checkAccessorAssignErrors(t, dims, &p) })
	t.Run("fixed", checkAccessorFixedErrors)
}

// TestIntrospectionConcurrentPatch checks lazy patch under race.
func TestIntrospectionConcurrentPatch(t *testing.T) {
	ClearMessageRegistryForTest()
	inner := NewMessageMembers("race__msg", "Inner", 4, []MessageMember{
		NewScalarMember("value", RosTypeInt32, 0),
	}, func(base any, init MessageInit) error { return nil }, func(base any) error { return nil })
	err := RegisterMessageMembers(inner)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	outer := NewMessageMembers("race__msg", "Outer", 8, []MessageMember{
		NewNestedMember("inner", 0, "Inner", "race__msg", "Inner"),
	}, func(base any, init MessageInit) error { return nil }, func(base any) error { return nil })
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := outer.Members[0].NestedMembers(); err != nil {
				t.Errorf("concurrent NestedMembers failed: %v", err)
			}
			if err := outer.EnsureNestedPatched(); err != nil {
				t.Errorf("concurrent EnsureNestedPatched failed: %v", err)
			}
		})
	}
	wg.Wait()
	if outer.Members[0].Members == nil {
		t.Fatal("concurrent patch left nil table")
	}
}

// TestIntrospectionNestedSequenceResize checks nested sequence growth.
func TestIntrospectionNestedSequenceResize(t *testing.T) {
	ClearMessageRegistryForTest()
	inner := NewMessageMembers("pkg__msg", "Inner", 4, []MessageMember{
		NewScalarMember("value", RosTypeInt32, 0),
	}, func(base any, init MessageInit) error { return nil }, func(base any) error { return nil })
	err := RegisterMessageMembers(inner)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	type OuterSeq struct {
		Items []int32
	}
	outer := OuterSeq{}
	m := NewNestedSequenceMember("items", 4, 0, "Items", "pkg__msg", "Inner")
	err = m.Validate()
	if err != nil {
		t.Fatalf("nested seq validate: %v", err)
	}
	err = m.Resize(&outer, 3)
	if err != nil {
		t.Fatalf("nested seq resize: %v", err)
	}
	if len(outer.Items) != 3 {
		t.Fatalf("nested seq len = %d, want 3", len(outer.Items))
	}
	err = m.Resize(&outer, 5)
	if err == nil {
		t.Fatal("resize above max must fail")
	}
	got, err := m.NestedMembers()
	if err != nil {
		t.Fatalf("nested seq lookup: %v", err)
	}
	if got != inner {
		t.Fatal("nested seq did not resolve inner")
	}
}

// TestIntrospectionStringUnknowns covers String fallback branches.
func TestIntrospectionStringUnknowns(t *testing.T) {
	rosCases := []struct {
		id   RosTypeID
		want string
	}{
		{RosTypeFloat, "ROS_TYPE_FLOAT"},
		{RosTypeFloat32, "ROS_TYPE_FLOAT"},
		{RosTypeMessage, "ROS_TYPE_MESSAGE"},
		{RosTypeID(0), "ROS_TYPE_UNKNOWN"},
		{RosTypeID(99), "ROS_TYPE_UNKNOWN"},
	}
	for _, c := range rosCases {
		if got := c.id.String(); got != c.want {
			t.Fatalf("ros type %d = %q, want %q", uint8(c.id), got, c.want)
		}
	}
	initCases := []struct {
		mode MessageInit
		want string
	}{
		{MessageInitAll, "ALL"},
		{MessageInitSkip, "SKIP"},
		{MessageInitZero, "ZERO"},
		{MessageInitDefaultsOnly, "DEFAULTS_ONLY"},
		{MessageInit(99), "UNKNOWN"},
	}
	for _, c := range initCases {
		if got := c.mode.String(); got != c.want {
			t.Fatalf("init %d = %q, want %q", int(c.mode), got, c.want)
		}
	}
	kindCases := []struct {
		kind ValueKind
		want string
	}{
		{ValuePrimitive, "primitive"},
		{ValueBoundedString, "bounded_string"},
		{ValueStringSequence, "string_sequence"},
		{ValueSpanSequence, "span_sequence"},
		{ValueBoolVector, "bool_vector"},
		{ValueKind(99), "unknown"},
	}
	for _, c := range kindCases {
		if got := c.kind.String(); got != c.want {
			t.Fatalf("kind %d = %q, want %q", int(c.kind), got, c.want)
		}
	}
}

// TestIntrospectionMemberByNameSuccess checks lookup hit path.
func TestIntrospectionMemberByNameSuccess(t *testing.T) {
	table := SolidPrimitiveMessageMembers()
	got, err := table.MemberByName("dimensions")
	if err != nil {
		t.Fatalf("member by name: %v", err)
	}
	if got.Name != "dimensions" {
		t.Fatalf("member name = %q", got.Name)
	}
	if !got.IsSequence() {
		t.Fatal("dimensions must be a sequence")
	}
}

// TestIntrospectionServiceNilTables checks nil table error paths.
func TestIntrospectionServiceNilTables(t *testing.T) {
	ClearMessageRegistryForTest()
	base := SolidPrimitiveMessageMembers()
	svc := &ServiceMembers{Namespace: "x__srv", Name: "S", Request: nil, Response: base, Event: base}
	if err := svc.EnsurePatched(); err == nil {
		t.Fatal("nil request must fail ensure patched")
	}
	if err := svc.Validate(); err == nil {
		t.Fatal("nil request must fail validate")
	}
	svcNil := &ServiceMembers{Namespace: "", Name: ""}
	if err := svcNil.Validate(); err == nil {
		t.Fatal("empty service identity must fail")
	}
	empty := &ServiceMembers{
		Namespace: "x__srv",
		Name:      "S",
		Request:   base,
		Response:  nil,
		Event:     nil,
	}
	if err := empty.EnsurePatched(); err == nil {
		t.Fatal("nil response must fail ensure patched")
	}
}

// TestIntrospectionRegisterConflicting checks overwrite guard.
func TestIntrospectionRegisterConflicting(t *testing.T) {
	ClearMessageRegistryForTest()
	first := NewMessageMembers("pkg__msg", "Dup", 4, []MessageMember{
		NewScalarMember("value", RosTypeInt32, 0),
	}, func(base any, init MessageInit) error { return nil }, func(base any) error { return nil })
	if err := RegisterMessageMembers(first); err != nil {
		t.Fatalf("register first: %v", err)
	}
	if err := RegisterMessageMembers(first); err != nil {
		t.Fatalf("same pointer must be idempotent: %v", err)
	}
	second := NewMessageMembers("pkg__msg", "Dup", 4, []MessageMember{
		NewScalarMember("other", RosTypeInt32, 0),
	}, func(base any, init MessageInit) error { return nil }, func(base any) error { return nil })
	if err := RegisterMessageMembers(second); err == nil {
		t.Fatal("conflicting re-register must fail")
	}
}

// TestIntrospectionGetCopyPrefix checks GetCopy error wrapping.
func TestIntrospectionGetCopyPrefix(t *testing.T) {
	table := SolidPrimitiveMessageMembers()
	p := SolidPrimitive{}
	if err := table.Init(&p, MessageInitAll); err != nil {
		t.Fatalf("init: %v", err)
	}
	dims := &table.Members[1]
	if _, err := dims.GetCopy(&p, 9); err == nil {
		t.Fatal("get copy out of range must fail")
	} else if !strings.Contains(err.Error(), "get copy") {
		t.Fatalf("get copy error = %q, want get copy prefix", err.Error())
	}
	scalar := NewScalarMember("x", RosTypeString, 0)
	if !scalar.IsScalarStringField() || !scalar.IsStringField() {
		t.Fatal("scalar string must report true")
	}
	arr := NewFixedArrayMember("names", RosTypeString, 2, 0, "Names")
	if arr.IsStringField() || arr.IsScalarStringField() {
		t.Fatal("array string must not report scalar")
	}
}

// TestIntrospectionCyclePatch checks cycle detection in patch walk.
func TestIntrospectionCyclePatch(t *testing.T) {
	ClearMessageRegistryForTest()
	cyc := NewMessageMembers("pkg__msg", "Cyc", 8, []MessageMember{
		NewNestedMember("self", 0, "Self", "pkg__msg", "Cyc"),
	}, func(base any, init MessageInit) error { return nil }, func(base any) error { return nil })
	if err := RegisterMessageMembers(cyc); err != nil {
		t.Fatalf("register cyc: %v", err)
	}
	if _, err := cyc.Members[0].NestedMembers(); err != nil {
		t.Fatalf("self patch: %v", err)
	}
	if err := cyc.EnsureNestedPatched(); err == nil {
		t.Fatal("cycle patch must fail")
	}
}
