// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
)

// Sentinel errors for HKDF failures.
var (
	// ErrHKDFLength reports an invalid output length.
	ErrHKDFLength = errors.New("crypto: invalid hkdf length")
)

// hkdfHashLen is the SHA-256 output size.
const hkdfHashLen = 32

// HKDFExtract implements RFC 5869 extract with HMAC-SHA256.
//
// An empty salt behaves as HashLen zeros per the RFC.
func HKDFExtract(salt, ikm []byte) []byte {
	if len(salt) == 0 {
		salt = make([]byte, hkdfHashLen)
	}

	mac := hmac.New(sha256.New, salt)
	_, _ = mac.Write(ikm) //nolint:errcheck // hash writes never fail

	return mac.Sum(nil)
}

// HKDFExpand implements RFC 5869 expand with HMAC-SHA256.
func HKDFExpand(prk, info []byte, length int) ([]byte, error) {
	if length <= 0 || length > 255*hkdfHashLen {
		return nil, fmt.Errorf("crypto: hkdf expand length %d: %w", length, ErrHKDFLength)
	}

	var out []byte

	var previous []byte

	for counter := byte(1); len(out) < length; counter++ {
		if counter == 0 {
			return nil, fmt.Errorf("crypto: hkdf expand counter overflow: %w", ErrHKDFLength)
		}

		mac := hmac.New(sha256.New, prk)
		_, _ = mac.Write(previous)        //nolint:errcheck // hash writes never fail
		_, _ = mac.Write(info)            //nolint:errcheck // hash writes never fail
		_, _ = mac.Write([]byte{counter}) //nolint:errcheck // hash writes never fail
		previous = mac.Sum(nil)
		out = append(out, previous...)
	}

	return out[:length], nil
}

// HKDF composes extract plus expand per RFC 5869.
func HKDF(salt, ikm, info []byte, length int) ([]byte, error) {
	prk := HKDFExtract(salt, ikm)

	out, err := HKDFExpand(prk, info, length)
	if err != nil {
		return nil, fmt.Errorf("crypto: hkdf: %w", err)
	}

	return out, nil
}
