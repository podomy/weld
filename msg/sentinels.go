// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package msg

import (
	"fmt"

	"github.com/podomy/weld/internal/types"
)

// KeyHashSize is the 16 byte RTPS KeyHash width.
const KeyHashSize = types.KeyHashSize

// Sentinel values re-exported so callers need not import internal paths.
var (
	// UnsetTypeHash is the zero hash with version 0 and empty string form.
	UnsetTypeHash = types.UnsetTypeHash
	// ErrInvalid reports malformed input that fails validation.
	ErrInvalid = types.ErrInvalid
	// ErrTruncated reports short input that needs more bytes.
	ErrTruncated = types.ErrTruncated
)

// ParseHeader validates 4 header bytes and returns the endian flag.
func ParseHeader(data []byte) (bool, error) {
	littleEndian, err := types.ParseHeader(data)
	if err != nil {
		return false, fmt.Errorf("msg: ParseHeader: %w", err)
	}

	return littleEndian, nil
}

// RegisterMessageMembers records a message table for lazy nested patching.
func RegisterMessageMembers(m *MessageMembers) error {
	err := types.RegisterMessageMembers(m)
	if err != nil {
		return fmt.Errorf("msg: RegisterMessageMembers: %w", err)
	}

	return nil
}

// ComputeKeyHash returns the RTPS KeyHash for serialized key bytes.
func ComputeKeyHash(key []byte) [KeyHashSize]byte {
	return types.ComputeKeyHash(key)
}
