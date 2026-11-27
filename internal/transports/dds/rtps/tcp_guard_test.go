// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rtps

import (
	"testing"
)

// TestVerifyTCPChecksumOverrunGuard pins the fail-closed bounds check in
// verifyTCPChecksum per 8.3.7.3: a crafted flagged length that pushes the
// checksum field past the frame drops loudly instead of panicking on the
// probe or frame slice.
func TestVerifyTCPChecksumOverrunGuard(t *testing.T) {
	t.Parallel()

	ext := HeaderExtension{
		LittleEndian:  true,
		HasLength:     true,
		HasTimestamp:  true,
		HasUExtension: true,
		HasWExtension: true,
		Checksum:      TCPChecksumMD5,
	}
	frame := make([]byte, HeaderSize+SubmessageHeaderSize)

	var drops TCPDropCounter
	if err := verifyTCPChecksum(frame, ext, HeaderSize, &drops); err == nil {
		t.Fatalf("overrun checksum succeeded, want error")
	}
	if drops.Count() != 1 {
		t.Fatalf("drops = %d, want 1", drops.Count())
	}
	if err := verifyTCPChecksum(frame, ext, HeaderSize, nil); err == nil {
		t.Fatalf("overrun checksum with nil drops succeeded, want error")
	}
}
