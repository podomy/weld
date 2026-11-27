// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package msg

import (
	"fmt"

	"github.com/podomy/weld/internal/types"
)

// ParseTypeHash parses a RIHS01 string or empty unset encoding.
func ParseTypeHash(s string) (TypeHash, error) {
	h, err := types.ParseTypeHash(s)
	if err != nil {
		return TypeHash{}, fmt.Errorf("msg: ParseTypeHash: %w", err)
	}
	return h, nil
}

// ComputeTypeHash hashes the canonical JSON with defaults already omitted.
func ComputeTypeHash(desc TypeDescription) (TypeHash, error) {
	h, err := types.ComputeTypeHash(desc)
	if err != nil {
		return TypeHash{}, fmt.Errorf("msg: ComputeTypeHash: %w", err)
	}
	return h, nil
}

// ComputeEquivalenceHash returns the wire hash over a TypeObjectSeq CDR.
func ComputeEquivalenceHash(seqBytes []byte) EquivalenceHash {
	return types.ComputeEquivalenceHash(seqBytes)
}

// ComputeWireEquivalenceHash builds the TypeObjectSeq CDR then hashes it.
func ComputeWireEquivalenceHash(objects [][]byte, littleEndian bool) (EquivalenceHash, error) {
	h, err := types.ComputeWireEquivalenceHash(objects, littleEndian)
	if err != nil {
		return EquivalenceHash{}, fmt.Errorf("msg: ComputeWireEquivalenceHash: %w", err)
	}
	return h, nil
}

// ParseEquivalenceHash parses the EQ14 string form of the wire hash.
func ParseEquivalenceHash(s string) (EquivalenceHash, error) {
	h, err := types.ParseEquivalenceHash(s)
	if err != nil {
		return EquivalenceHash{}, fmt.Errorf("msg: ParseEquivalenceHash: %w", err)
	}

	return h, nil
}

// ComputeEquivalencext6aHash returns the 14 byte equivalence hash.
func ComputeEquivalencext6aHash(desc TypeDescription) (Equivalencext6aHash, error) {
	h, err := types.ComputeEquivalencext6aHash(desc)
	if err != nil {
		return Equivalencext6aHash{}, fmt.Errorf("msg: ComputeEquivalencext6aHash: %w", err)
	}

	return h, nil
}

// ParseEquivalencext6aHash parses the EQM14 string form.
func ParseEquivalencext6aHash(s string) (Equivalencext6aHash, error) {
	h, err := types.ParseEquivalencext6aHash(s)
	if err != nil {
		return Equivalencext6aHash{}, fmt.Errorf("msg: ParseEquivalencext6aHash: %w", err)
	}

	return h, nil
}
