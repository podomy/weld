// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package types

import (
	"testing"
)

// registerLookupFixture registers one full description for lookup tests.
func registerLookupFixture(t *testing.T, r *Registry) TypeHash {
	t.Helper()
	full := TypeDescription{
		Description: IndividualTypeDescription{
			TypeName: "pkg/msg/A",
			Fields: []Field{
				{Name: "v", Type: FieldType{TypeID: FieldTypeInt32}, MemberID: 1},
			},
		},
		Referenced: []IndividualTypeDescription{},
	}
	h, err := r.RegisterFull(full, nil)
	if err != nil {
		t.Fatalf("RegisterFull failed: %v", err)
	}
	return h
}

// checkLookupDependencies verifies request plus reply round trip.
func checkLookupDependencies(t *testing.T, le bool) {
	t.Helper()
	r := NewRegistry()
	h := registerLookupFixture(t, r)
	srv := NewTypeLookupServer(r)
	cli := NewTypeLookupClient(func(req []byte) ([]byte, error) {
		return srv.HandleDependencies(req)
	})
	ids, err := cli.GetTypeDependencies([]TypeHash{h}, le)
	if err != nil {
		t.Fatalf("le=%v dependencies failed: %v", le, err)
	}
	if len(ids) != 1 || ids[0].Kind != TIStrongComp {
		t.Fatalf("le=%v ids %+v want one SCC", le, ids)
	}
	_, err = cli.GetTypeDependencies([]TypeHash{{Version: HashVersion1}}, le)
	if err == nil {
		t.Fatalf("le=%v unknown hash must fail", le)
	}
}

// checkLookupTypes verifies type fetch round trip plus raw framing.
func checkLookupTypes(t *testing.T, le bool) {
	t.Helper()
	cli, ind := newLookupTypesClient(t)
	checkLookupMinimal(t, cli, ind, le)
	checkLookupComplete(t, cli, ind, le)
	checkLookupUnknown(t, cli, ind, le)
}

// newLookupTypesClient builds a types client with fixture A registered.
func newLookupTypesClient(t *testing.T) (*TypeLookupClient, IndividualTypeDescription) {
	t.Helper()
	r := NewRegistry()
	registerLookupFixture(t, r)
	srv := NewTypeLookupServer(r)
	cli := NewTypeLookupClient(func(req []byte) ([]byte, error) {
		return srv.HandleTypes(req)
	})
	ind, ok := r.LookupType("pkg/msg/A")
	if !ok {
		t.Fatalf("lookup type missed")
	}
	return cli, ind
}

// checkLookupMinimal verifies minimal fetch round trip.
func checkLookupMinimal(t *testing.T, cli *TypeLookupClient, ind IndividualTypeDescription, le bool) {
	t.Helper()
	wireMin, err := wireHashForIndividual(ind, true, le)
	if err != nil {
		t.Fatalf("le=%v wire hash failed: %v", le, err)
	}
	ids := []TypeIdentifier{{Kind: EKMinimal, Hash: wireMin.Digest}}
	objects, err := cli.GetTypes(ids, le)
	if err != nil {
		t.Fatalf("le=%v get types failed: %v", le, err)
	}
	if len(objects) != 1 {
		t.Fatalf("le=%v objects %d want 1", le, len(objects))
	}
	obj, err := UnmarshalTypeObject(objects[0], le)
	if err != nil {
		t.Fatalf("le=%v unmarshal object failed: %v", le, err)
	}
	if obj.Minimal == nil || obj.Minimal.TypeName != "pkg/msg/A" {
		t.Fatalf("le=%v object %+v want minimal A", le, obj)
	}
}

// checkLookupComplete verifies complete fetch round trip.
func checkLookupComplete(t *testing.T, cli *TypeLookupClient, ind IndividualTypeDescription, le bool) {
	t.Helper()
	wireComplete, err := wireHashForIndividual(ind, false, le)
	if err != nil {
		t.Fatalf("le=%v wire complete hash failed: %v", le, err)
	}
	completeObjs, err := cli.GetTypes([]TypeIdentifier{{Kind: EKComplete, Hash: wireComplete.Digest}}, le)
	if err != nil {
		t.Fatalf("le=%v get complete failed: %v", le, err)
	}
	cobj, err := UnmarshalTypeObject(completeObjs[0], le)
	if err != nil {
		t.Fatalf("le=%v unmarshal complete failed: %v", le, err)
	}
	if cobj.Complete == nil || cobj.Complete.TypeName != "pkg/msg/A" {
		t.Fatalf("le=%v complete object %+v want complete A", le, cobj)
	}
}

// checkLookupUnknown verifies unknown kind and hash fail loudly.
func checkLookupUnknown(t *testing.T, cli *TypeLookupClient, ind IndividualTypeDescription, le bool) {
	t.Helper()
	wireMin, err := wireHashForIndividual(ind, true, le)
	if err != nil {
		t.Fatalf("le=%v wire hash failed: %v", le, err)
	}
	_, err = cli.GetTypes([]TypeIdentifier{{Kind: 0xC0, Hash: wireMin.Digest}}, le)
	if err == nil {
		t.Fatalf("le=%v unknown kind must fail", le)
	}
	var unknown [14]byte
	unknown[0] = 0xFF
	_, err = cli.GetTypes([]TypeIdentifier{{Kind: EKMinimal, Hash: unknown}}, le)
	if err == nil {
		t.Fatalf("le=%v unknown hash must fail", le)
	}
}

// checkLookupEmpty verifies empty registry fails loudly.
func checkLookupEmpty(t *testing.T, le bool) {
	t.Helper()
	empty := NewRegistry()
	srv := NewTypeLookupServer(empty)
	cli := NewTypeLookupClient(func(req []byte) ([]byte, error) {
		return srv.HandleTypes(req)
	})
	_, err := cli.GetTypes([]TypeIdentifier{{Kind: EKMinimal}}, le)
	if err == nil {
		t.Fatalf("le=%v empty registry must fail", le)
	}
}

// TestTypeLookupWire pins getTypeDependencies plus getTypes over CDR.
func TestTypeLookupWire(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkLookupDependencies(t, le)
		checkLookupTypes(t, le)
		checkLookupEmpty(t, le)
	}
}

// TestTypeLookupMultiType pins exact hash lookup with two types.
func TestTypeLookupMultiType(t *testing.T) {
	for _, le := range []bool{true, false} {
		checkMultiTypeRoundTrip(t, le)
	}
}

// checkMultiTypeRoundTrip registers A plus B and verifies each fetch.
func checkMultiTypeRoundTrip(t *testing.T, le bool) {
	t.Helper()
	r := registerMultiTypeFixture(t, le)
	srv := NewTypeLookupServer(r)
	cli := NewTypeLookupClient(func(req []byte) ([]byte, error) {
		return srv.HandleTypes(req)
	})
	for _, name := range []string{"pkg/msg/A", "pkg/msg/B"} {
		checkOneMultiType(t, cli, r, name, le)
	}
}

// registerMultiTypeFixture registers two single-field types.
func registerMultiTypeFixture(t *testing.T, le bool) *Registry {
	t.Helper()
	r := NewRegistry()
	for _, name := range []string{"pkg/msg/A", "pkg/msg/B"} {
		full := TypeDescription{
			Description: IndividualTypeDescription{
				TypeName: name,
				Fields:   []Field{{Name: "v", Type: FieldType{TypeID: FieldTypeInt32}, MemberID: 1}},
			},
		}
		_, err := r.RegisterFull(full, nil)
		if err != nil {
			t.Fatalf("le=%v register %s failed: %v", le, name, err)
		}
	}
	return r
}

// checkOneMultiType verifies one named type fetch round trip.
func checkOneMultiType(t *testing.T, cli *TypeLookupClient, r *Registry, name string, le bool) {
	t.Helper()
	ind, ok := r.LookupType(name)
	if !ok {
		t.Fatalf("le=%v lookup %s missed", le, name)
	}
	wire, err := wireHashForIndividual(ind, true, le)
	if err != nil {
		t.Fatalf("le=%v hash %s failed: %v", le, name, err)
	}
	objs, err := cli.GetTypes([]TypeIdentifier{{Kind: EKMinimal, Hash: wire.Digest}}, le)
	if err != nil {
		t.Fatalf("le=%v get %s failed: %v", le, name, err)
	}
	obj, err := UnmarshalTypeObject(objs[0], le)
	if err != nil {
		t.Fatalf("le=%v unmarshal %s failed: %v", le, name, err)
	}
	if obj.Minimal == nil || obj.Minimal.TypeName != name {
		t.Fatalf("le=%v want %s got %+v", le, name, obj)
	}
}
