// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Schema registry keyed by type name plus hash.
package types

import (
	"fmt"
	"sync"
)

// fullKey identifies one stored description by name plus versioned hash.
// The hash is the full TypeHash so version is significant: a v2 digest
// never matches a v1 entry even when digests collide, matching Equal.
type fullKey struct {
	name string
	hash TypeHash
}

// equivalencext6aKey identifies one stored description by name plus EQ14.
type equivalencext6aKey struct {
	name string
	eq   Equivalencext6aHash
}

// Registry stores individuals plus sources plus full descriptions by name and hash.
type Registry struct {
	mu             sync.RWMutex
	types          map[string]IndividualTypeDescription
	sources        map[string]TypeSource
	fullByNameHash map[fullKey]TypeDescription
	// equivalencext6a indexes the same descriptions by EQ14 hash.
	// This is the model-only JSON MD5 index; discovery uses wireByHash.
	equivalencext6a map[equivalencext6aKey]TypeDescription
	// wireByHash indexes individuals by wire EquivalenceHash (CDR MD5)
	// for exact discovery lookup. Both LE and BE hashes plus minimal
	// and complete variants point to the same individual.
	wireByHash map[[14]byte]IndividualTypeDescription
}

// NewRegistry returns an empty registry with no shared global state.
func NewRegistry() *Registry {
	return &Registry{
		types:           make(map[string]IndividualTypeDescription),
		sources:         make(map[string]TypeSource),
		fullByNameHash:  make(map[fullKey]TypeDescription),
		equivalencext6a: make(map[equivalencext6aKey]TypeDescription),
		wireByHash:      make(map[[14]byte]IndividualTypeDescription),
	}
}

// RegisterType stores one individual description after validation.
//
// A conflicting redefinition with different fields is rejected. A default
// dynamic source is recorded when no source exists for the name.
func (r *Registry) RegisterType(desc IndividualTypeDescription) error {
	err := desc.Validate()
	if err != nil {
		return fmt.Errorf("types: register type %q: %w", desc.TypeName, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.types[desc.TypeName]; ok {
		if !equalIndividual(existing, desc) {
			return fmt.Errorf("types: register type %q: conflicting redefinition", desc.TypeName)
		}
		return nil
	}
	r.types[desc.TypeName] = cloneIndividual(desc)
	indexWireIndividualLocked(r, desc)
	if _, ok := r.sources[desc.TypeName]; !ok {
		r.sources[desc.TypeName] = TypeSource{
			TypeName: desc.TypeName,
			Encoding: EncodingDynamic,
		}
	}
	return nil
}

// RegisterSource stores one type source after validation.
//
// Sources use overwrite-wins: a later RegisterSource replaces any existing
// entry for the same type name. This lets RegisterType record a default
// dynamic placeholder that a later file based source replaces, and makes
// re-registration of an identical source idempotent.
func (r *Registry) RegisterSource(src TypeSource) error {
	err := src.Validate()
	if err != nil {
		return fmt.Errorf("types: register source %q: %w", src.TypeName, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sources[src.TypeName] = src
	return nil
}

// LookupType returns a copy of one individual description by name.
func (r *Registry) LookupType(name string) (IndividualTypeDescription, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.types[name]
	if !ok {
		return IndividualTypeDescription{}, false
	}
	return cloneIndividual(d), true
}

// LookupSource returns a copy of one type source by name.
func (r *Registry) LookupSource(name string) (TypeSource, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sources[name]
	if !ok {
		return TypeSource{}, false
	}
	return s, true
}

// TransitiveClosure builds the full description with sorted references.
//
// Collection holds the read lock only while cloning map entries. Sorting
// and result assembly run lock free on those clones, so concurrent readers
// never block on sort work. Sorting under the read lock would also be
// correct, but the split keeps critical sections short.
func (r *Registry) TransitiveClosure(root string) (TypeDescription, error) {
	err := ValidateTypeName(root)
	if err != nil {
		return TypeDescription{}, fmt.Errorf("types: closure %q: %w", root, err)
	}
	r.mu.RLock()
	rootDesc, refs, err := r.collectNestedLocked(root)
	r.mu.RUnlock()
	if err != nil {
		return TypeDescription{}, err
	}
	sortIndividuals(refs)
	if refs == nil {
		refs = []IndividualTypeDescription{}
	}
	return TypeDescription{
		Description: rootDesc,
		Referenced:  refs,
	}, nil
}

// collectNestedLocked gathers the root plus transitive refs as clones.
//
// The caller must hold the read lock or write lock. Queue names were
// verified present before enqueue, and the held lock excludes writers, so
// direct map indexing cannot miss. The returned slices are clones that the
// caller may sort without holding the lock.
func (r *Registry) collectNestedLocked(root string) (IndividualTypeDescription, []IndividualTypeDescription, error) {
	rootDesc, ok := r.types[root]
	if !ok {
		return IndividualTypeDescription{}, nil, fmt.Errorf("types: closure: unknown type %q", root)
	}
	rootClone := cloneIndividual(rootDesc)
	visited := map[string]bool{root: true}
	queue := []string{root}
	var refs []IndividualTypeDescription
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		// Lock exclusion note: queue entries were checked present at
		// enqueue time and the lock bars concurrent writes, so indexing
		// directly is safe without a missing branch.
		cur := r.types[name]
		for i := range cur.Fields {
			nested := cur.Fields[i].Type.NestedTypeName
			if nested == "" {
				continue
			}
			if visited[nested] {
				continue
			}
			nestedDesc, ok := r.types[nested]
			if !ok {
				return IndividualTypeDescription{}, nil, fmt.Errorf("types: closure %q: missing nested type %q", root, nested)
			}
			visited[nested] = true
			refs = append(refs, cloneIndividual(nestedDesc))
			queue = append(queue, nested)
		}
	}
	return rootClone, refs, nil
}

// ComputeHash returns the RIHS01 hash of the transitive closure for root.
func (r *Registry) ComputeHash(root string) (TypeHash, error) {
	full, err := r.TransitiveClosure(root)
	if err != nil {
		return UnsetTypeHash, fmt.Errorf("types: compute hash %q: %w", root, err)
	}
	h, err := ComputeTypeHash(full)
	if err != nil {
		return UnsetTypeHash, fmt.Errorf("types: compute hash %q: %w", root, err)
	}
	return h, nil
}

// normalizeFull validates plus sorts a copy plus hashes one description.
//
// Sources are validated first. The description is copied, sorted by type
// name, validated, checked for closure completeness, then hashed.
func normalizeFull(desc TypeDescription, sources []TypeSource) (TypeDescription, TypeHash, error) {
	for i := range sources {
		err := sources[i].Validate()
		if err != nil {
			return TypeDescription{}, UnsetTypeHash, fmt.Errorf("types: normalize full %q source %q: %w", desc.Description.TypeName, sources[i].TypeName, err)
		}
	}
	sorted := TypeDescription{
		Description: cloneIndividual(desc.Description),
		Referenced:  make([]IndividualTypeDescription, len(desc.Referenced)),
	}
	for i := range desc.Referenced {
		sorted.Referenced[i] = cloneIndividual(desc.Referenced[i])
	}
	sortIndividuals(sorted.Referenced)
	err := sorted.Validate()
	if err != nil {
		return TypeDescription{}, UnsetTypeHash, fmt.Errorf("types: normalize full %q: %w", desc.Description.TypeName, err)
	}
	err = checkClosureComplete(sorted)
	if err != nil {
		return TypeDescription{}, UnsetTypeHash, fmt.Errorf("types: normalize full %q: %w", desc.Description.TypeName, err)
	}
	h, err := ComputeTypeHash(sorted)
	if err != nil {
		return TypeDescription{}, UnsetTypeHash, fmt.Errorf("types: normalize full %q: %w", desc.Description.TypeName, err)
	}
	return sorted, h, nil
}

// checkClosureComplete verifies every nested name appears in the closed set.
// It also rejects referenced-but-unreachable types: the closure must equal
// the reachable set from the root, not merely a superset.
func checkClosureComplete(sorted TypeDescription) error {
	present := make(map[string]bool, len(sorted.Referenced)+1)
	present[sorted.Description.TypeName] = true
	for i := range sorted.Referenced {
		present[sorted.Referenced[i].TypeName] = true
	}
	for i := range sorted.Description.Fields {
		nested := sorted.Description.Fields[i].Type.NestedTypeName
		if nested != "" && !present[nested] {
			return fmt.Errorf("types: incomplete closure %q misses nested %q", sorted.Description.TypeName, nested)
		}
	}
	for i := range sorted.Referenced {
		for j := range sorted.Referenced[i].Fields {
			nested := sorted.Referenced[i].Fields[j].Type.NestedTypeName
			if nested != "" && !present[nested] {
				return fmt.Errorf("types: incomplete closure %q misses nested %q", sorted.Referenced[i].TypeName, nested)
			}
		}
	}
	err := checkClosureReachable(sorted, present)
	if err != nil {
		return err
	}
	return nil
}

// checkClosureReachable rejects referenced types unreachable from the root.
// It walks nested names from the root over the closed set; every referenced
// entry must be visited or the closure carries unreachable types loudly.
func checkClosureReachable(sorted TypeDescription, present map[string]bool) error {
	lookup := make(map[string]IndividualTypeDescription, len(present))
	lookup[sorted.Description.TypeName] = sorted.Description
	for i := range sorted.Referenced {
		lookup[sorted.Referenced[i].TypeName] = sorted.Referenced[i]
	}
	reachable := map[string]bool{sorted.Description.TypeName: true}
	queue := []string{sorted.Description.TypeName}
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		cur, ok := lookup[name]
		if !ok {
			continue
		}
		queue = visitReachableFields(cur, present, reachable, queue)
	}
	for i := range sorted.Referenced {
		name := sorted.Referenced[i].TypeName
		if !reachable[name] {
			return fmt.Errorf("types: closure %q contains unreachable referenced %q", sorted.Description.TypeName, name)
		}
	}
	return nil
}

// visitReachableFields enqueues unvisited nested names from one individual.
// It keeps checkClosureReachable short by isolating the field loop.
func visitReachableFields(cur IndividualTypeDescription, present, reachable map[string]bool, queue []string) []string {
	for i := range cur.Fields {
		nested := cur.Fields[i].Type.NestedTypeName
		if nested == "" || reachable[nested] || !present[nested] {
			continue
		}
		reachable[nested] = true
		queue = append(queue, nested)
	}
	return queue
}

// storeFull stores one normalized description plus sources with conflict checks.
//
// Conflicting redefinitions with different fields are rejected. The caller
// must hold no lock; this helper locks internally. The model EQ14 index
// (JSON MD5) is kept for model-only use; the wire index (CDR MD5) is built
// for discovery via indexWireIndividualLocked.
func (r *Registry) storeFull(sorted TypeDescription, h TypeHash, sources []TypeSource) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	all := make([]IndividualTypeDescription, 0, len(sorted.Referenced)+1)
	all = append(all, sorted.Description)
	all = append(all, sorted.Referenced...)
	for i := range all {
		existing, ok := r.types[all[i].TypeName]
		if ok && !equalIndividual(existing, all[i]) {
			return fmt.Errorf("types: store full %q: conflicting redefinition of %q", sorted.Description.TypeName, all[i].TypeName)
		}
	}
	for i := range all {
		r.types[all[i].TypeName] = cloneIndividual(all[i])
		indexWireIndividualLocked(r, all[i])
	}
	for i := range sources {
		r.sources[sources[i].TypeName] = sources[i]
	}
	key := fullKey{name: sorted.Description.TypeName, hash: h}
	r.fullByNameHash[key] = sorted
	eq, err := ComputeEquivalencext6aHash(sorted)
	if err != nil {
		return fmt.Errorf("types: store full %q equivalence: %w", sorted.Description.TypeName, err)
	}
	r.equivalencext6a[equivalencext6aKey{name: sorted.Description.TypeName, eq: eq}] = sorted
	return nil
}

// RegisterFull validates plus sorts plus hashes plus indexes one description.
//
// The input may list references in any order; a sorted copy is stored and
// hashed. Conflicting redefinitions are rejected. All nested names must be
// closed in the input.
func (r *Registry) RegisterFull(desc TypeDescription, sources []TypeSource) (TypeHash, error) {
	sorted, h, err := normalizeFull(desc, sources)
	if err != nil {
		return UnsetTypeHash, fmt.Errorf("types: register full %q: %w", desc.Description.TypeName, err)
	}
	err = r.storeFull(sorted, h, sources)
	if err != nil {
		return UnsetTypeHash, fmt.Errorf("types: register full %q: %w", desc.Description.TypeName, err)
	}
	return h, nil
}

// individualToMinimalTypeObject converts one stored individual to a minimal body.
//
// The name hash is MD5(name) first 4 bytes per 7.3.3.5. Bounds, nested
// names, units, and extensibility are preserved so distinct types differing
// only in these produce distinct wire hashes.
func individualToMinimalTypeObject(ind IndividualTypeDescription) MinimalTypeObject {
	out := MinimalTypeObject{TypeName: ind.TypeName, Ext: ind.Extensibility}
	for i := range ind.Fields {
		h := ComputeMinimalNameHash(ind.Fields[i].Name)
		out.Members = append(out.Members, MinimalMember{
			MemberID:       ind.Fields[i].MemberID,
			Kind:           uint8(ind.Fields[i].Type.TypeID),
			NameHash:       h,
			Capacity:       ind.Fields[i].Type.Capacity,
			StringCapacity: ind.Fields[i].Type.StringCapacity,
			NestedTypeName: ind.Fields[i].Type.NestedTypeName,
			Units:          ind.Fields[i].Units,
		})
	}
	return out
}

// individualToCompleteTypeObject converts one stored individual to a complete body.
//
// MustUnderstand is carried from the field so the complete wire preserves
// it. Bounds, nested names, units, and extensibility are preserved like
// the minimal arm.
func individualToCompleteTypeObject(ind IndividualTypeDescription) CompleteTypeObject {
	out := CompleteTypeObject{TypeName: ind.TypeName, Ext: ind.Extensibility}
	for i := range ind.Fields {
		out.Members = append(out.Members, CompleteMember{
			MemberID:       ind.Fields[i].MemberID,
			Kind:           uint8(ind.Fields[i].Type.TypeID),
			Name:           ind.Fields[i].Name,
			Optional:       ind.Fields[i].Optional,
			MustUnderstand: ind.Fields[i].MustUnderstand,
			Capacity:       ind.Fields[i].Type.Capacity,
			StringCapacity: ind.Fields[i].Type.StringCapacity,
			NestedTypeName: ind.Fields[i].Type.NestedTypeName,
			Units:          ind.Fields[i].Units,
		})
	}
	return out
}

// wireHashForIndividual returns the wire EquivalenceHash for one individual.
//
// The hash is canonical per 7.3.3.10: it is MD5 over the TypeObjectSeq CDR
// containing the single DHEADER-framed TypeObject encoded little-endian,
// regardless of the littleEndian hint. The hint is retained for
// compatibility but ignored for hashing so LE and BE wires hash
// identically. Minimal selects the minimal arm, complete the complete arm.
func wireHashForIndividual(ind IndividualTypeDescription, minimal bool, littleEndian bool) (EquivalenceHash, error) {
	_ = littleEndian
	var raw []byte
	var err error
	if minimal {
		raw, err = MarshalTypeObject(TypeObject{Minimal: func() *MinimalTypeObject { m := individualToMinimalTypeObject(ind); return &m }()}, true)
	} else {
		raw, err = MarshalTypeObject(TypeObject{Complete: func() *CompleteTypeObject { c := individualToCompleteTypeObject(ind); return &c }()}, true)
	}
	if err != nil {
		return EquivalenceHash{}, err
	}
	return ComputeWireEquivalenceHash([][]byte{raw}, true)
}

// indexWireIndividualLocked stores minimal and complete canonical wire hashes.
//
// The caller must hold the write lock. One canonical (little-endian) hash
// per arm is indexed; LE and BE transports share the same entry so
// discovery is endian-independent. Colliding hashes from different type
// names are kept first-wins to preserve exact lookup determinism.
func indexWireIndividualLocked(r *Registry, ind IndividualTypeDescription) {
	clone := cloneIndividual(ind)
	for _, minimal := range []bool{true, false} {
		h, err := wireHashForIndividual(ind, minimal, true)
		if err != nil {
			continue
		}
		if _, ok := r.wireByHash[h.Digest]; !ok {
			r.wireByHash[h.Digest] = clone
		}
	}
}

// cloneFullxt6a returns a deep copy of one stored description.
func cloneFullxt6a(d TypeDescription) TypeDescription {
	out := TypeDescription{
		Description: cloneIndividual(d.Description),
		Referenced:  make([]IndividualTypeDescription, len(d.Referenced)),
	}
	for i := range d.Referenced {
		out.Referenced[i] = cloneIndividual(d.Referenced[i])
	}
	return out
}

// LookupFull returns one stored description by type name plus hash.
// The lookup is version-sensitive like Equal: version and digest must match.
func (r *Registry) LookupFull(name string, hash TypeHash) (TypeDescription, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := fullKey{name: name, hash: hash}
	d, ok := r.fullByNameHash[key]
	if !ok {
		return TypeDescription{}, false
	}
	return cloneFullxt6a(d), true
}

// LookupFullByEquivalencext6a returns one stored description by name plus EQ14.
//
// The RIHS01 index in LookupFull is unchanged and byte-identical; this
// is a second index for equivalence based discovery.
func (r *Registry) LookupFullByEquivalencext6a(name string, eq Equivalencext6aHash) (TypeDescription, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := equivalencext6aKey{name: name, eq: eq}
	d, ok := r.equivalencext6a[key]
	if !ok {
		return TypeDescription{}, false
	}
	return cloneFullxt6a(d), true
}

// ComputeEquivalencext6a returns the EQ14 hash of the closure for root.
func (r *Registry) ComputeEquivalencext6a(root string) (Equivalencext6aHash, error) {
	full, err := r.TransitiveClosure(root)
	if err != nil {
		return Equivalencext6aHash{}, fmt.Errorf("types: compute equivalence %q: %w", root, err)
	}
	eq, err := ComputeEquivalencext6aHash(full)
	if err != nil {
		return Equivalencext6aHash{}, fmt.Errorf("types: compute equivalence %q: %w", root, err)
	}
	return eq, nil
}

// LookupWithHash verifies the closure hash for name against hash.
//
// It returns unknown type errors, validation errors, and mismatch errors
// distinctly so callers log mismatches loudly with both digests.
func (r *Registry) LookupWithHash(name string, hash TypeHash) (TypeDescription, error) {
	full, err := r.TransitiveClosure(name)
	if err != nil {
		return TypeDescription{}, fmt.Errorf("types: lookup hash %q: %w", name, err)
	}
	h, err := ComputeTypeHash(full)
	if err != nil {
		return TypeDescription{}, fmt.Errorf("types: lookup hash %q: %w", name, err)
	}
	if !h.Equal(hash) {
		return TypeDescription{}, fmt.Errorf("types: lookup hash %q mismatch: have %s want %s", name, h.String(), hash.String())
	}
	return full, nil
}

// Len returns the count of registered individual types.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.types)
}

// TypeLookupServer answers getTypeDependencies and getTypes over CDR.
//
// The server wraps a Registry and speaks the DDS TypeLookup wire: the
// caller passes CDR request bytes (as they would arrive on the built-in
// TypeLookup endpoints) and receives CDR reply bytes. This keeps the
// transport out of this package while pinning the exact wire framing
// with round-trip tests.
type TypeLookupServer struct {
	reg *Registry
}

// NewTypeLookupServer binds a server to a registry.
func NewTypeLookupServer(reg *Registry) *TypeLookupServer {
	return &TypeLookupServer{reg: reg}
}

// MarshalDependenciesRequest encodes hashes as sequence plus 33 byte entries.
//
// Each entry is version byte plus 32 digest bytes with no per-entry pad.
// The sequence length uses 4 byte alignment; entries follow contiguously.
func MarshalDependenciesRequest(hashes []TypeHash, littleEndian bool) ([]byte, error) {
	enc := NewEncoder(littleEndian)
	err := enc.WriteSequenceLength(len(hashes))
	if err != nil {
		return nil, err
	}
	for i := range hashes {
		vb := hashes[i].VersionedBytes()
		enc.buf = append(enc.buf, vb[:]...)
	}
	return enc.Bytes(), nil
}

// ParseDependenciesRequest decodes a dependencies request.
func ParseDependenciesRequest(wire []byte) ([]TypeHash, error) {
	dec, err := NewDecoder(wire)
	if err != nil {
		return nil, err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, err
	}
	err = dec.CheckSequenceAllocation(n, 33)
	if err != nil {
		return nil, err
	}
	out := make([]TypeHash, 0, n)
	for range n {
		err = dec.need(33)
		if err != nil {
			return nil, err
		}
		var h TypeHash
		h.Version = dec.data[dec.off]
		copy(h.Digest[:], dec.data[dec.off+1:dec.off+33])
		dec.off += 33
		out = append(out, h)
	}
	err = dec.Finish()
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MarshalDependenciesReply encodes identifiers as octet sequences.
func MarshalDependenciesReply(ids []TypeIdentifier, littleEndian bool) ([]byte, error) {
	enc := NewEncoder(littleEndian)
	err := enc.WriteSequenceLength(len(ids))
	if err != nil {
		return nil, err
	}
	for i := range ids {
		raw, err := MarshalTypeIdentifier(ids[i], littleEndian)
		if err != nil {
			return nil, err
		}
		err = enc.WriteOctetSequence(raw)
		if err != nil {
			return nil, err
		}
	}
	return enc.Bytes(), nil
}

// ParseDependenciesReply decodes a dependencies reply.
func ParseDependenciesReply(wire []byte) ([]TypeIdentifier, error) {
	dec, err := NewDecoder(wire)
	if err != nil {
		return nil, err
	}
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return nil, err
	}
	err = dec.CheckSequenceAllocation(n, 4)
	if err != nil {
		return nil, err
	}
	out := make([]TypeIdentifier, 0, n)
	for range n {
		raw, err := dec.ReadOctetSequence()
		if err != nil {
			return nil, err
		}
		id, err := UnmarshalTypeIdentifier(raw, dec.LittleEndian())
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	err = dec.Finish()
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MarshalTypesRequest encodes identifiers as octet sequences.
func MarshalTypesRequest(ids []TypeIdentifier, littleEndian bool) ([]byte, error) {
	return MarshalDependenciesReply(ids, littleEndian)
}

// ParseTypesRequest decodes a getTypes request.
func ParseTypesRequest(wire []byte) ([]TypeIdentifier, error) {
	return ParseDependenciesReply(wire)
}

// MarshalTypesReply encodes type objects as octet sequences.
//
// Each object already carries its DHEADER per 7.3.3.5; the sequence
// adds only the element count plus 4 byte alignment pad between objects.
// Minimal and complete objects share one sequence in discriminator order
// as they appear on the wire. Framing is shared with
// MarshalTypeObjectSeqCDR via appendTypeObjectsSeq.
func MarshalTypesReply(objects [][]byte, littleEndian bool) ([]byte, error) {
	return MarshalTypeObjectSeqCDR(objects, littleEndian)
}

// ParseTypesReply decodes a getTypes reply into raw objects.
func ParseTypesReply(wire []byte) ([][]byte, error) {
	return ParseTypeObjectSeqCDR(wire)
}

// HandleDependencies answers one getTypeDependencies request wire.
//
// Each requested RIHS01 hash maps to a strongly connected hash
// identifier carrying the wire 14 byte equivalence digest (CDR MD5 per
// 7.3.3.10) when the registry holds the description; unknown hashes fail.
// The encapsulation header must parse; a bad header fails instead of
// defaulting to little-endian.
func (s *TypeLookupServer) HandleDependencies(wire []byte) ([]byte, error) {
	if s.reg == nil {
		return nil, fmt.Errorf("types: lookup server misses registry: %w", ErrInvalid)
	}
	if len(wire) < HeaderSize {
		return nil, fmt.Errorf("types: lookup dependencies wire too short %d: %w", len(wire), ErrTruncated)
	}
	var hdr [4]byte
	copy(hdr[:], wire[:HeaderSize])
	parsed, err := ParseEncapsulationxt6b(hdr)
	if err != nil {
		return nil, err
	}
	le := parsed.LittleEndian
	hashes, err := ParseDependenciesRequest(wire)
	if err != nil {
		return nil, err
	}
	ids := make([]TypeIdentifier, 0, len(hashes))
	for i := range hashes {
		id, err := s.dependencyFor(hashes[i], le)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return MarshalDependenciesReply(ids, le)
}

// dependencyFor maps one RIHS01 hash to a strongly connected identifier.
//
// The returned hash is the canonical wire EquivalenceHash (TypeObjectSeq
// CDR MD5 over little-endian canonical bytes) for the root individual,
// not the model JSON MD5. The RIHS01 index stays for ROS interop; the SCC
// digest is the wire form per 7.3.3.10. The le hint selects transport
// framing only and never affects the digest.
func (s *TypeLookupServer) dependencyFor(hash TypeHash, le bool) (TypeIdentifier, error) {
	_ = le
	s.reg.mu.RLock()
	defer s.reg.mu.RUnlock()
	for key, full := range s.reg.fullByNameHash {
		if key.hash.Equal(hash) {
			wire, err := wireHashForIndividual(full.Description, true, true)
			if err != nil {
				return TypeIdentifier{}, err
			}
			return TypeIdentifier{Kind: TIStrongComp, Hash: wire.Digest}, nil
		}
	}
	return TypeIdentifier{}, fmt.Errorf("types: lookup unknown hash %s: %w", hash.String(), ErrInvalid)
}

// HandleTypes answers one getTypes request wire.
//
// The reply carries one DHEADER framed TypeObject per known hash
// identifier in request order. Unknown kinds and unknown hashes fail.
// The encapsulation header must parse; a bad header fails instead of
// defaulting to little-endian.
func (s *TypeLookupServer) HandleTypes(wire []byte) ([]byte, error) {
	if s.reg == nil {
		return nil, fmt.Errorf("types: lookup server misses registry: %w", ErrInvalid)
	}
	if len(wire) < HeaderSize {
		return nil, fmt.Errorf("types: lookup types wire too short %d: %w", len(wire), ErrTruncated)
	}
	var hdr [4]byte
	copy(hdr[:], wire[:HeaderSize])
	parsed, err := ParseEncapsulationxt6b(hdr)
	if err != nil {
		return nil, err
	}
	le := parsed.LittleEndian
	ids, err := ParseTypesRequest(wire)
	if err != nil {
		return nil, err
	}
	objects := make([][]byte, 0, len(ids))
	for i := range ids {
		obj, err := s.objectFor(ids[i], le)
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	return MarshalTypesReply(objects, le)
}

// objectFor builds one TypeObject body for a requested identifier.
//
// The requested 14 byte hash must exactly match a stored wire hash (both
// LE and BE hashes are indexed); the lexicographically-first fallback is
// removed. Unknown hashes and unknown kinds fail. EKMinimal returns the
// minimal arm, EKComplete the complete arm, and TIStrongComp the minimal
// arm for the matched individual.
func (s *TypeLookupServer) objectFor(id TypeIdentifier, le bool) ([]byte, error) {
	s.reg.mu.RLock()
	defer s.reg.mu.RUnlock()
	if len(s.reg.wireByHash) == 0 && len(s.reg.fullByNameHash) == 0 {
		return nil, fmt.Errorf("types: lookup registry empty: %w", ErrInvalid)
	}
	ind, ok := s.reg.wireByHash[id.Hash]
	if !ok {
		return nil, fmt.Errorf("types: lookup unknown equivalence hash %x: %w", id.Hash, ErrInvalid)
	}
	var raw []byte
	var err error
	switch id.Kind {
	case EKMinimal, TIStrongComp:
		minimal := individualToMinimalTypeObject(ind)
		raw, err = MarshalTypeObject(TypeObject{Minimal: &minimal}, le)
	case EKComplete:
		complete := individualToCompleteTypeObject(ind)
		raw, err = MarshalTypeObject(TypeObject{Complete: &complete}, le)
	default:
		return nil, fmt.Errorf("types: lookup kind %#02x unsupported: %w", id.Kind, ErrInvalid)
	}
	if err != nil {
		return nil, err
	}
	// Aliasing guard: the returned object's recomputed canonical seq-MD5
	// must equal the requested digest. A transport-endian remarshal or a
	// colliding entry that no longer matches fails closed instead of
	// serving the wrong type.
	recomputed, err := ComputeWireEquivalenceHash([][]byte{raw}, true)
	if err != nil {
		return nil, fmt.Errorf("types: lookup recompute: %w", err)
	}
	if recomputed.Digest != id.Hash {
		return nil, fmt.Errorf("types: lookup hash mismatch: recomputed %x vs requested %x: %w", recomputed.Digest, id.Hash, ErrInvalid)
	}
	return raw, nil
}

// TypeLookupClient exchanges CDR request and reply bytes with a server.
//
// Transport simulates the DDS built-in endpoints: it receives request
// bytes and returns reply bytes. Round-trip tests pin the exact wire.
type TypeLookupClient struct {
	transport func(request []byte) ([]byte, error)
}

// NewTypeLookupClient binds a client to a transport function.
func NewTypeLookupClient(transport func(request []byte) ([]byte, error)) *TypeLookupClient {
	return &TypeLookupClient{transport: transport}
}

// GetTypeDependencies requests identifiers for hashes over the transport.
func (c *TypeLookupClient) GetTypeDependencies(hashes []TypeHash, littleEndian bool) ([]TypeIdentifier, error) {
	if c.transport == nil {
		return nil, fmt.Errorf("types: lookup client misses transport: %w", ErrInvalid)
	}
	wire, err := MarshalDependenciesRequest(hashes, littleEndian)
	if err != nil {
		return nil, err
	}
	reply, err := c.transport(wire)
	if err != nil {
		return nil, err
	}
	return ParseDependenciesReply(reply)
}

// GetTypes requests type objects for identifiers over the transport.
func (c *TypeLookupClient) GetTypes(ids []TypeIdentifier, littleEndian bool) ([][]byte, error) {
	if c.transport == nil {
		return nil, fmt.Errorf("types: lookup client misses transport: %w", ErrInvalid)
	}
	wire, err := MarshalTypesRequest(ids, littleEndian)
	if err != nil {
		return nil, err
	}
	reply, err := c.transport(wire)
	if err != nil {
		return nil, err
	}
	return ParseTypesReply(reply)
}
