// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package msg

import (
	"fmt"

	"github.com/podomy/weld/internal/types"
)

// Encoder builds one XCDR sample including its 4 byte header.
type Encoder = types.Encoder

// Decoder reads one XCDR sample including its 4 byte header.
type Decoder = types.Decoder

// Metrics counts dropped inputs for envelope parsing.
type Metrics = types.Metrics

// StringBound gathers every upper bound source for a string field.
type StringBound = types.StringBound

// SequenceBound gathers every upper bound source for an array field.
type SequenceBound = types.SequenceBound

// MapBound gathers the upper bound source for a map field.
type MapBound = types.MapBound

// DescriptionField mirrors a type description field capacity pair.
type DescriptionField = types.DescriptionField

// NewEncoder returns an XCDR1 encoder for the given byte order.
func NewEncoder(littleEndian bool) *Encoder {
	return types.NewEncoder(littleEndian)
}

// NewDecoder returns an XCDR decoder over one wire sample.
func NewDecoder(data []byte) (*Decoder, error) {
	dec, err := types.NewDecoder(data)
	if err != nil {
		return nil, fmt.Errorf("msg: NewDecoder: %w", err)
	}
	return dec, nil
}

// MarshalMessage wraps a raw payload with header plus zeroed pad.
func MarshalMessage(payload []byte, littleEndian bool) []byte {
	return types.MarshalMessage(payload, littleEndian)
}

// UnmarshalMessage validates the header plus length and returns payload.
func UnmarshalMessage(data []byte, m *Metrics) ([]byte, bool, error) {
	payload, endian, err := types.UnmarshalMessage(data, m)
	if err != nil {
		return nil, false, fmt.Errorf("msg: UnmarshalMessage: %w", err)
	}
	return payload, endian, nil
}

// MarshalServiceMessage wraps a payload with header plus GUID and sequence.
func MarshalServiceMessage(payload []byte, guid [8]byte, seq uint64, littleEndian bool) []byte {
	return types.MarshalServiceMessage(payload, guid, seq, littleEndian)
}

// UnmarshalServiceMessage validates a service sample and splits the parts.
func UnmarshalServiceMessage(data []byte, m *Metrics) ([]byte, [8]byte, uint64, bool, error) {
	payload, guid, seq, endian, err := types.UnmarshalServiceMessage(data, m)
	if err != nil {
		return nil, [8]byte{}, 0, false, fmt.Errorf("msg: UnmarshalServiceMessage: %w", err)
	}
	return payload, guid, seq, endian, nil
}

// CheckStringLength enforces all bound sources at serialize time.
func CheckStringLength(s string, bound StringBound) error {
	err := types.CheckStringLength(s, bound)
	if err != nil {
		return fmt.Errorf("msg: CheckStringLength: %w", err)
	}
	return nil
}

// CheckSequenceLength enforces fixed and bounded lengths at serialize time.
//
// Every sequence also stays under the shared CDR element cap of 33554432
// elements (32<<20) enforced on resize and wire decode paths; that global
// cap is reported separately from the IDL bound checked here.
func CheckSequenceLength(n uint, bound SequenceBound) error {
	err := types.CheckSequenceLength(n, bound)
	if err != nil {
		return fmt.Errorf("msg: CheckSequenceLength: %w", err)
	}
	return nil
}

// CheckMapLength enforces the bound at encode time.
func CheckMapLength(n int, bound MapBound) error {
	err := types.CheckMapLength(n, bound)
	if err != nil {
		return fmt.Errorf("msg: CheckMapLength: %w", err)
	}
	return nil
}

// ValidateStringForSerialize enforces introspection plus description bounds for strings.
func ValidateStringForSerialize(s string, bound StringBound, field DescriptionField) error {
	err := types.ValidateStringForSerialize(s, bound, field)
	if err != nil {
		return fmt.Errorf("msg: ValidateStringForSerialize: %w", err)
	}
	return nil
}

// ValidateSequenceForSerialize enforces introspection plus description bounds for sequences.
//
// The merged IDL and description bound is checked here; the shared CDR
// element cap of 33554432 elements (32<<20) is enforced on resize and
// wire decode paths and bounds every sequence including unbounded IDL.
func ValidateSequenceForSerialize(n uint, bound SequenceBound, field DescriptionField) error {
	err := types.ValidateSequenceForSerialize(n, bound, field)
	if err != nil {
		return fmt.Errorf("msg: ValidateSequenceForSerialize: %w", err)
	}
	return nil
}
