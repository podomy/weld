// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package tokens

import (
	"bytes"
	"testing"
)

// TestVolatileMustBeSealed pins 9.5.1.9 plus 10.5.2.1.2: raw volatile bytes
// carry key material and must ride inside a Kx seal, never clear.
func TestVolatileMustBeSealed(t *testing.T) {
	t.Parallel()
	var src, dst [16]byte
	src[0] = 0x51
	dst[0] = 0x52
	envelope := VolatileEnvelope{
		SourceGUID:      src,
		DestinationGUID: dst,
		Tokens: []KeyMaterial{{
			TransformationKind: 1,
			MasterSalt:         bytes.Repeat([]byte{0x56}, 32),
			SenderKeyID:        9,
			MasterSenderKey:    bytes.Repeat([]byte{0x57}, 32),
		}},
	}
	raw, err := EncodeVolatile(envelope)
	if err != nil {
		t.Fatalf("encode volatile: %v", err)
	}
	if !bytes.Contains(raw, bytes.Repeat([]byte{0x57}, 4)) {
		t.Fatalf("raw volatile hides key material, want visible (must be sealed)")
	}
	back, err := DecodeVolatile(raw)
	if err != nil {
		t.Fatalf("decode volatile: %v", err)
	}
	if back.SourceGUID != src || back.DestinationGUID != dst {
		t.Fatalf("volatile GUIDs mismatch")
	}
}
