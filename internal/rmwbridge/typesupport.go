// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1
package rmwbridge

import (
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/types"
)

// ErrTypeNotFound reports lookup of an unregistered type name.
var ErrTypeNotFound = errors.New("rmwbridge: type not found")

// ErrTypeHashMismatch reports a registered name with a different hash.
var ErrTypeHashMismatch = errors.New("rmwbridge: type hash mismatch")

// TypeSupport describes one ROS type known to the bridge.
//
// TypeName is the ROS IDL name such as std_msgs__msg__String.
// TypeHash is the RIHS01 string such as RIHS01_... Empty means unknown.
type TypeSupport struct {
	// TypeName is the ROS IDL name.
	TypeName string
	// TypeHash is the RIHS01 hash string, or empty when unknown.
	TypeHash string
}

// RegisterType records one type name plus RIHS01 hash.
//
// An empty hash records an unknown type. A non-empty hash must parse via
// types.ParseTypeHash. Re-registering the same name plus hash is idempotent.
// Re-registering the same name with a different hash reports
// ErrTypeHashMismatch. It reports ErrBridgeClosed after Close.
func (b *Bridge) RegisterType(name, hash string) error {
	if name == "" {
		return errors.New("rmwbridge: register type: empty name") //nolint:wrapcheck // new error, nothing to wrap
	}
	if hash != "" {
		_, err := types.ParseTypeHash(hash)
		if err != nil {
			return fmt.Errorf("rmwbridge: register type %q: %w", name, err)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return fmt.Errorf("rmwbridge: register type: %w", ErrBridgeClosed)
	}
	existing, ok := b.typeSupports[name]
	if ok {
		if existing.TypeHash == hash {
			return nil
		}
		return fmt.Errorf("rmwbridge: register type %q mismatch have %q want %q: %w", name, existing.TypeHash, hash, ErrTypeHashMismatch)
	}
	b.typeSupports[name] = TypeSupport{TypeName: name, TypeHash: hash}
	return nil
}

// LookupType returns one registered type by name.
//
// An empty wantHash skips the hash check and returns the stored entry.
// A non-empty wantHash must parse via types.ParseTypeHash. When both the
// stored hash and wantHash are non-empty and differ, LookupType reports
// ErrTypeHashMismatch with both digests. Unknown names report
// ErrTypeNotFound. It reports ErrBridgeClosed after Close.
func (b *Bridge) LookupType(name, wantHash string) (TypeSupport, error) {
	if name == "" {
		return TypeSupport{}, errors.New("rmwbridge: lookup type: empty name") //nolint:wrapcheck // new error, nothing to wrap
	}
	if wantHash != "" {
		_, err := types.ParseTypeHash(wantHash)
		if err != nil {
			return TypeSupport{}, fmt.Errorf("rmwbridge: lookup type %q: %w", name, err)
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return TypeSupport{}, fmt.Errorf("rmwbridge: lookup type: %w", ErrBridgeClosed)
	}
	stored, ok := b.typeSupports[name]
	if !ok {
		return TypeSupport{}, fmt.Errorf("rmwbridge: lookup type %q: %w", name, ErrTypeNotFound)
	}
	if wantHash != "" && stored.TypeHash != "" && wantHash != stored.TypeHash {
		return TypeSupport{}, fmt.Errorf("rmwbridge: lookup type %q mismatch have %q want %q: %w", name, stored.TypeHash, wantHash, ErrTypeHashMismatch)
	}
	return stored, nil
}

// EncodeCDR returns a copy of already serialized XCDR1 wire bytes.
//
// It is a passthrough: no CDR fields are interpreted or transformed. The
// caller may reuse the input after return. A nil input maps to nil.
//
// Size enforcement lives at the bridge entry points that accept encoded
// CDR, not here, because this signature returns only []byte and cannot
// report an error without breaking callers. PublishBytes,
// Publisher.Publish, Client.Call, Service.Call, and the Serve responder
// each reject payloads larger than maxPayloadBytes with ErrPayloadTooLarge
// before copying. The bound is 32 MiB, matching maxPayloadLen in
// internal/transports/dds/datapath.go so a corrupt or abusive length
// cannot force a larger single allocation.
func EncodeCDR(payload []byte) []byte {
	out := append([]byte(nil), payload...)
	return out
}

// DecodeCDR validates XCDR1 wire bytes and returns a copy.
//
// It validates the 4 byte encapsulation header via types.ParseHeader, then
// returns a copy of the full wire including header and pad tail. No CDR
// fields are interpreted. The caller owns the returned slice.
func DecodeCDR(wire []byte) ([]byte, error) {
	_, err := types.ParseHeader(wire)
	if err != nil {
		return nil, fmt.Errorf("rmwbridge: decode cdr: %w", err)
	}
	out := append([]byte(nil), wire...)
	return out, nil
}

// DecodeCDR validates XCDR1 wire bytes and returns a copy.
//
// It is the Bridge handle form of DecodeCDR for callers holding a bridge.
// It delegates to the package function and reports the same errors.
func (b *Bridge) DecodeCDR(wire []byte) ([]byte, error) {
	out, err := DecodeCDR(wire)
	if err != nil {
		return nil, err
	}
	return out, nil
}
