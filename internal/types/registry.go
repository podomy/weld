// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Schema registry keyed by type name plus hash.
package types

import (
	"fmt"
	"sync"
)

// fullKey identifies one stored description by name plus hash string.
type fullKey struct {
	name string
	hash string
}

// Registry stores individuals plus sources plus full descriptions by name and hash.
type Registry struct {
	mu             sync.RWMutex
	types          map[string]IndividualTypeDescription
	sources        map[string]TypeSource
	fullByNameHash map[fullKey]TypeDescription
}

// NewRegistry returns an empty registry with no shared global state.
func NewRegistry() *Registry {
	return &Registry{
		types:          make(map[string]IndividualTypeDescription),
		sources:        make(map[string]TypeSource),
		fullByNameHash: make(map[fullKey]TypeDescription),
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
	return nil
}

// storeFull stores one normalized description plus sources with conflict checks.
//
// Conflicting redefinitions with different fields are rejected. The caller
// must hold no lock; this helper locks internally.
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
	}
	for i := range sources {
		r.sources[sources[i].TypeName] = sources[i]
	}
	key := fullKey{name: sorted.Description.TypeName, hash: h.String()}
	r.fullByNameHash[key] = sorted
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

// LookupFull returns one stored description by type name plus hash.
func (r *Registry) LookupFull(name string, hash TypeHash) (TypeDescription, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key := fullKey{name: name, hash: hash.String()}
	d, ok := r.fullByNameHash[key]
	if !ok {
		return TypeDescription{}, false
	}
	out := TypeDescription{
		Description: cloneIndividual(d.Description),
		Referenced:  make([]IndividualTypeDescription, len(d.Referenced)),
	}
	for i := range d.Referenced {
		out.Referenced[i] = cloneIndividual(d.Referenced[i])
	}
	return out, true
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
