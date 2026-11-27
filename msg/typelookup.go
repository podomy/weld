// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package msg

import (
	"fmt"

	"github.com/podomy/weld/internal/types"
)

// TypeIdentifier is one spec TypeIdentifier union value.
type TypeIdentifier = types.TypeIdentifier

// TypeObject is one spec TypeObject union value.
type TypeObject = types.TypeObject

// TypeLookupClient exchanges CDR request and reply bytes with a server.
type TypeLookupClient = types.TypeLookupClient

// TypeLookupServer answers getTypeDependencies and getTypes over CDR.
type TypeLookupServer = types.TypeLookupServer

// NewTypeLookupClient binds a client to a transport function.
func NewTypeLookupClient(transport func(request []byte) ([]byte, error)) *TypeLookupClient {
	return types.NewTypeLookupClient(transport)
}

// NewTypeLookupServer binds a server to a registry.
func NewTypeLookupServer(reg *Registry) *TypeLookupServer {
	return types.NewTypeLookupServer(reg)
}

// CanonicalJSON returns the hashable JSON with separators comma space and colon space.
func CanonicalJSON(desc TypeDescription) (string, error) {
	out, err := types.CanonicalJSON(desc)
	if err != nil {
		return "", fmt.Errorf("msg: CanonicalJSON: %w", err)
	}

	return out, nil
}

// ValidateTypeName checks that a type name is non empty plus ASCII plus within cap.
func ValidateTypeName(name string) error {
	err := types.ValidateTypeName(name)
	if err != nil {
		return fmt.Errorf("msg: ValidateTypeName: %w", err)
	}

	return nil
}

// MarshalDependenciesRequest encodes hashes as sequence plus 33 byte entries.
func MarshalDependenciesRequest(hashes []TypeHash, littleEndian bool) ([]byte, error) {
	out, err := types.MarshalDependenciesRequest(hashes, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("msg: MarshalDependenciesRequest: %w", err)
	}

	return out, nil
}

// ParseDependenciesRequest decodes a dependencies request.
func ParseDependenciesRequest(wire []byte) ([]TypeHash, error) {
	out, err := types.ParseDependenciesRequest(wire)
	if err != nil {
		return nil, fmt.Errorf("msg: ParseDependenciesRequest: %w", err)
	}

	return out, nil
}

// MarshalDependenciesReply encodes identifiers as octet sequences.
func MarshalDependenciesReply(ids []TypeIdentifier, littleEndian bool) ([]byte, error) {
	out, err := types.MarshalDependenciesReply(ids, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("msg: MarshalDependenciesReply: %w", err)
	}

	return out, nil
}

// ParseDependenciesReply decodes a dependencies reply.
func ParseDependenciesReply(wire []byte) ([]TypeIdentifier, error) {
	out, err := types.ParseDependenciesReply(wire)
	if err != nil {
		return nil, fmt.Errorf("msg: ParseDependenciesReply: %w", err)
	}

	return out, nil
}

// MarshalTypesRequest encodes identifiers as octet sequences.
func MarshalTypesRequest(ids []TypeIdentifier, littleEndian bool) ([]byte, error) {
	out, err := types.MarshalTypesRequest(ids, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("msg: MarshalTypesRequest: %w", err)
	}

	return out, nil
}

// ParseTypesRequest decodes a getTypes request.
func ParseTypesRequest(wire []byte) ([]TypeIdentifier, error) {
	out, err := types.ParseTypesRequest(wire)
	if err != nil {
		return nil, fmt.Errorf("msg: ParseTypesRequest: %w", err)
	}

	return out, nil
}

// MarshalTypesReply encodes type objects as octet sequences.
func MarshalTypesReply(objects [][]byte, littleEndian bool) ([]byte, error) {
	out, err := types.MarshalTypesReply(objects, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("msg: MarshalTypesReply: %w", err)
	}

	return out, nil
}

// ParseTypesReply decodes a getTypes reply into raw objects.
func ParseTypesReply(wire []byte) ([][]byte, error) {
	out, err := types.ParseTypesReply(wire)
	if err != nil {
		return nil, fmt.Errorf("msg: ParseTypesReply: %w", err)
	}

	return out, nil
}

// MarshalTypeIdentifier encodes one identifier with CDR alignment.
func MarshalTypeIdentifier(id TypeIdentifier, littleEndian bool) ([]byte, error) {
	out, err := types.MarshalTypeIdentifier(id, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("msg: MarshalTypeIdentifier: %w", err)
	}

	return out, nil
}

// UnmarshalTypeIdentifier decodes one identifier from CDR bytes.
func UnmarshalTypeIdentifier(data []byte, littleEndian bool) (TypeIdentifier, error) {
	out, err := types.UnmarshalTypeIdentifier(data, littleEndian)
	if err != nil {
		return TypeIdentifier{}, fmt.Errorf("msg: UnmarshalTypeIdentifier: %w", err)
	}

	return out, nil
}

// MarshalTypeObject encodes one TypeObject with XCDR2 DHEADER framing.
func MarshalTypeObject(o TypeObject, littleEndian bool) ([]byte, error) {
	out, err := types.MarshalTypeObject(o, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("msg: MarshalTypeObject: %w", err)
	}

	return out, nil
}

// UnmarshalTypeObject decodes one DHEADER framed TypeObject.
func UnmarshalTypeObject(data []byte, littleEndian bool) (TypeObject, error) {
	out, err := types.UnmarshalTypeObject(data, littleEndian)
	if err != nil {
		return TypeObject{}, fmt.Errorf("msg: UnmarshalTypeObject: %w", err)
	}

	return out, nil
}

// MarshalTypeObjectSeqCDR encodes a TypeObjectSeq as sequence plus bodies.
func MarshalTypeObjectSeqCDR(objects [][]byte, littleEndian bool) ([]byte, error) {
	out, err := types.MarshalTypeObjectSeqCDR(objects, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("msg: MarshalTypeObjectSeqCDR: %w", err)
	}

	return out, nil
}

// ParseTypeObjectSeqCDR splits a TypeObjectSeq into its element bodies.
func ParseTypeObjectSeqCDR(wire []byte) ([][]byte, error) {
	out, err := types.ParseTypeObjectSeqCDR(wire)
	if err != nil {
		return nil, fmt.Errorf("msg: ParseTypeObjectSeqCDR: %w", err)
	}

	return out, nil
}
