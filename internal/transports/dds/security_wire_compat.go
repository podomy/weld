// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/internal/transports/dds/security/crypto"
)

// DDS-SECURITY submessage wire compat: SEC_PREFIX/BODY/POSTFIX triple
// handling per section 9.5.3. Restored from the continuous-security
// implementation for wire-test compatibility; the crypto Transform
// in security/crypto handles payload protection while these helpers
// handle submessage framing.

// secSplitTriple splits one SEC triple payload into framed submessages.
func secSplitTriple(payload []byte) ([]byte, []byte, []byte, error) {
	first, rest, err := secSplitOne(payload)
	if err != nil {
		return nil, nil, nil, err
	}

	second, rest, err := secSplitOne(rest)
	if err != nil {
		return nil, nil, nil, err
	}

	third, rest, err := secSplitOne(rest)
	if err != nil {
		return nil, nil, nil, err
	}

	if len(rest) != 0 {
		return nil, nil, nil, fmt.Errorf("dds: submessage triple has %d trailing bytes: %w", len(rest), ErrSecuredTransformUnavailable)
	}

	return first, second, third, nil
}

// secSplitOne splits one framed submessage off the front.
func secSplitOne(raw []byte) ([]byte, []byte, error) {
	if len(raw) < 4 {
		return nil, nil, fmt.Errorf("dds: submessage need header have %d: %w", len(raw), ErrSecuredTransformUnavailable)
	}

	var length int

	if raw[1]&rtps.FlagLittleEndian != 0 {
		length = int(binary.LittleEndian.Uint16(raw[2:4]))
	} else {
		length = int(binary.BigEndian.Uint16(raw[2:4]))
	}

	if len(raw)-4 < length {
		return nil, nil, fmt.Errorf("dds: submessage need %d have %d: %w", length, len(raw)-4, ErrSecuredTransformUnavailable)
	}

	return append([]byte(nil), raw[:4+length]...), append([]byte(nil), raw[4+length:]...), nil
}

// DDS-Security submessage protection, section 9.5.3.
//
// Protected traffic on the wire wraps one inner submessage in a
// SEC_PREFIX plus body plus SEC_POSTFIX triple. The prefix carries
// the transform kind, the sender key id, and the 12 byte IV; the
// middle carries the ciphertext or the original submessage; the
// postfix carries the 16 byte tag. GMAC kinds authenticate the inner
// bytes while GCM kinds encrypt them. Session keys derive through
// crypto.DeriveSessionKey, matching the CycloneDDS computation.

// SEC submessage ids and framing sizes.
const (
	// secPrefixID opens one protected submessage.
	secPrefixID = 0x31
	// secBodyID carries one encrypted submessage body.
	secBodyID = 0x30
	// secPostfixID closes one protected submessage.
	secPostfixID = 0x32
	// secPrefixContentLen is the 20 byte SEC_PREFIX content.
	secPrefixContentLen = 20
	// secPostfixContentLen is the 20 byte SEC_POSTFIX content.
	secPostfixContentLen = 20
	// secTagLen is the 16 byte authentication tag.
	secTagLen = 16
	// secIVLen is the 12 byte initialization vector.
	secIVLen = 12
)

// OMG crypto transform kinds on the wire.
const (
	// secKindAES128GMAC authenticates with 128-bit keys.
	secKindAES128GMAC = 1
	// secKindAES128GCM encrypts with 128-bit keys.
	secKindAES128GCM = 2
	// secKindAES256GMAC authenticates with 256-bit keys.
	secKindAES256GMAC = 3
	// secKindAES256GCM encrypts with 256-bit keys.
	secKindAES256GCM = 4
)

// secKindKeySize returns the key size for one wire transform kind.
func secKindKeySize(kind uint32) (int, error) {
	switch kind {
	case secKindAES128GMAC, secKindAES128GCM:
		return 16, nil
	case secKindAES256GMAC, secKindAES256GCM:
		return 32, nil
	default:
		return 0, fmt.Errorf("dds: submessage transform kind %d: %w", kind, ErrSecuredTransformUnavailable)
	}
}

// secKindEncrypts reports whether one wire kind encrypts its body.
func secKindEncrypts(kind uint32) bool {
	return kind == secKindAES128GCM || kind == secKindAES256GCM
}

// gcmBlockMul multiplies two 128-bit blocks in the GCM field.
//
// It uses the reflected-bit shift-and-xor loop from the GCM
// specification, processing the multiplier most significant bit first
// with reduction polynomial x^128 + x^7 + x^2 + x + 1.
func gcmBlockMul(x, y [16]byte) [16]byte {
	var z, v [16]byte

	copy(v[:], y[:])

	for i := range 128 {
		if x[i>>3]>>(7-uint(i&7))&1 != 0 {
			for j := range 16 {
				z[j] ^= v[j]
			}
		}

		bit := v[15] & 1

		for j := 15; j > 0; j-- {
			v[j] = v[j]>>1 | v[j-1]<<7
		}

		v[0] >>= 1

		if bit != 0 {
			v[0] ^= 0xE1
		}
	}

	return z
}

// ghashWithAAD computes GHASH over aad plus data with zero padding.
//
// The input closes with the 16 byte length block carrying the aad bit
// length plus the data bit length, matching the GCM definition. An empty
// aad matches the legacy empty-AAD vectors.
func ghashWithAAD(h [16]byte, aad, data []byte) [16]byte {
	var y [16]byte

	for off := 0; off < len(aad); off += 16 {
		var block [16]byte

		copy(block[:], aad[off:min(off+16, len(aad))])

		for i := range 16 {
			block[i] ^= y[i]
		}

		y = gcmBlockMul(block, h)
	}

	for off := 0; off < len(data); off += 16 {
		var block [16]byte

		copy(block[:], data[off:min(off+16, len(data))])

		for i := range 16 {
			block[i] ^= y[i]
		}

		y = gcmBlockMul(block, h)
	}

	var lengths [16]byte

	binary.BigEndian.PutUint64(lengths[0:8], uint64(len(aad)*8))
	binary.BigEndian.PutUint64(lengths[8:16], uint64(len(data)*8))

	for i := range 16 {
		lengths[i] ^= y[i]
	}

	return gcmBlockMul(lengths, h)
}

// newAESCipher builds one AES cipher with a wrapped error.
func newAESCipher(key []byte) (cipher.Block, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("dds: submessage aes: %w", err)
	}

	return block, nil
}

// secGMACTag computes the GMAC tag for one inner submessage with prefix AAD.
//
// Per 10.5.3.3 the SEC_PREFIX header is additional authenticated data:
// GHASH covers aad plus the full inner submessage bytes, masked with AES
// of the counter zero block. An empty aad reproduces the legacy vector.
func secGMACTag(sessionKey, iv, body, aad []byte) ([]byte, error) {
	if len(iv) != secIVLen {
		return nil, fmt.Errorf("dds: GMAC iv need %d have %d: %w", secIVLen, len(iv), ErrSecuredTransformUnavailable)
	}

	block, err := newAESCipher(sessionKey)
	if err != nil {
		return nil, err
	}

	var hRaw [16]byte

	block.Encrypt(hRaw[:], make([]byte, 16))

	digest := ghashWithAAD(hRaw, aad, body)

	var y0 [16]byte

	copy(y0[:], iv)
	y0[15] = 0x01

	var mask [16]byte

	block.Encrypt(mask[:], y0[:])

	tag := make([]byte, secTagLen)
	for i := range secTagLen {
		tag[i] = digest[i] ^ mask[i]
	}

	return tag, nil
}

// secPrefix holds one parsed SEC_PREFIX content.
type secPrefix struct {
	// kind is the wire transform kind.
	kind uint32
	// transformID is the sender key id.
	transformID uint32
	// session is the session id from the IV prefix.
	session uint32
	// iv is the 12 byte initialization vector.
	iv [secIVLen]byte
}

// secParsePrefix parses one 20 byte SEC_PREFIX content big-endian.
func secParsePrefix(raw []byte) (secPrefix, error) {
	var out secPrefix

	if len(raw) != secPrefixContentLen {
		return out, fmt.Errorf("dds: submessage prefix need %d have %d: %w", secPrefixContentLen, len(raw), ErrSecuredTransformUnavailable)
	}

	out.kind = binary.BigEndian.Uint32(raw[0:4])
	out.transformID = binary.BigEndian.Uint32(raw[4:8])
	copy(out.iv[:], raw[8:20])
	out.session = binary.BigEndian.Uint32(raw[8:12])

	_, err := secKindKeySize(out.kind)
	if err != nil {
		return out, err
	}

	return out, nil
}

// secEncodePrefix renders one SEC_PREFIX submessage with native LE flags.
func secEncodePrefix(prefix secPrefix) rtps.RawSubmessage {
	var content [secPrefixContentLen]byte

	binary.BigEndian.PutUint32(content[0:4], prefix.kind)
	binary.BigEndian.PutUint32(content[4:8], prefix.transformID)
	copy(content[8:20], prefix.iv[:])

	return rtps.RawSubmessage{ID: secPrefixID, Flags: rtps.FlagLittleEndian, Body: append([]byte(nil), content[:]...)}
}

// secEncodePostfix renders one SEC_POSTFIX submessage with an empty MAC list.
func secEncodePostfix(tag []byte) (rtps.RawSubmessage, error) {
	if len(tag) != secTagLen {
		return rtps.RawSubmessage{}, fmt.Errorf("dds: submessage tag need %d have %d: %w", secTagLen, len(tag), ErrSecuredTransformUnavailable)
	}

	content := make([]byte, 0, secPostfixContentLen)
	content = append(content, tag...)
	content = append(content, 0x00, 0x00, 0x00, 0x00)

	return rtps.RawSubmessage{ID: secPostfixID, Flags: rtps.FlagLittleEndian, Body: content}, nil
}

// secEncodeBody renders one SEC_BODY submessage carrying ciphertext.
func secEncodeBody(ciphertext []byte) rtps.RawSubmessage {
	content := make([]byte, 0, 4+len(ciphertext))

	var raw [4]byte

	binary.BigEndian.PutUint32(raw[:], uint32(len(ciphertext))) //nolint:gosec // G115: ciphertext bounded by submessage cap
	content = append(content, raw[:]...)
	content = append(content, ciphertext...)

	return rtps.RawSubmessage{ID: secBodyID, Flags: rtps.FlagLittleEndian, Body: content}
}

// secWrapSubmessage wraps one inner submessage in a SEC triple.
//
// The inner bytes must be one complete framed submessage including
// its 4 byte header. GMAC kinds authenticate the inner bytes while
// GCM kinds encrypt them. Per 10.5.3.3 the framed SEC_PREFIX is the
// GCM additional authenticated data for both kinds, following the
// fullAAD pattern in security/crypto/transform.go. Suffixes must be
// unique per session.
//
//nolint:dupl // framing mirrors secWrapSubmessageWithOrigin in backend.go by design: same triple layout, prefix-only AAD vs origin-bound AAD.
func secWrapSubmessage(kind, transformID, session uint32, suffix uint64, sessionKey, inner []byte) ([]byte, error) {
	keySize, err := secKindKeySize(kind)
	if err != nil {
		return nil, err
	}

	if len(sessionKey) != keySize {
		return nil, fmt.Errorf("dds: submessage session key need %d have %d: %w", keySize, len(sessionKey), ErrSecuredTransformUnavailable)
	}

	var iv [secIVLen]byte

	binary.BigEndian.PutUint32(iv[0:4], session)
	binary.BigEndian.PutUint64(iv[4:12], suffix)

	prefix := secEncodePrefix(secPrefix{kind: kind, transformID: transformID, session: session, iv: iv})
	prefixAAD := secFrameSubmessage(prefix)

	var body rtps.RawSubmessage

	if secKindEncrypts(kind) {
		block, err := newAESCipher(sessionKey)
		if err != nil {
			return nil, err
		}

		gcm, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("dds: submessage gcm: %w", err)
		}

		ciphertext := gcm.Seal(nil, iv[:], inner, prefixAAD)
		overhead := gcm.Overhead()

		tag := append([]byte(nil), ciphertext[len(ciphertext)-overhead:]...)
		ciphertext = ciphertext[:len(ciphertext)-overhead]

		body = secEncodeBody(ciphertext)

		postfix, err := secEncodePostfix(tag)
		if err != nil {
			return nil, err
		}

		return secJoinTriple(prefix, body, postfix), nil
	}

	tag, err := secGMACTag(sessionKey, iv[:], inner, prefixAAD)
	if err != nil {
		return nil, err
	}

	if len(inner) < 4 {
		return nil, fmt.Errorf("dds: submessage inner truncated: %w", ErrSecuredTransformUnavailable)
	}

	body = rtps.RawSubmessage{ID: inner[0], Flags: inner[1], Body: append([]byte(nil), inner[4:]...)}

	postfix, err := secEncodePostfix(tag)
	if err != nil {
		return nil, err
	}

	return secJoinTriple(prefix, body, postfix), nil
}

// secUnwrapSplit verifies and opens one split SEC triple.
//
// prefixBody, bodySub, and postfixSub carry the framed submessages
// including their 4 byte headers. lookup resolves the master key and
// salt for one transform id and session.
func secUnwrapSplit(prefixRaw, bodyRaw, postfixRaw []byte, lookup func(transformID, session uint32) (key, salt []byte, err error)) ([]byte, error) {
	prefixBody, err := secSubmessageBody(prefixRaw)
	if err != nil {
		return nil, err
	}

	prefix, err := secParsePrefix(prefixBody)
	if err != nil {
		return nil, err
	}

	postfixBody, err := secSubmessageBody(postfixRaw)
	if err != nil {
		return nil, err
	}

	if len(postfixBody) != secPostfixContentLen {
		return nil, fmt.Errorf("dds: submessage postfix need %d have %d: %w", secPostfixContentLen, len(postfixBody), ErrSecuredTransformUnavailable)
	}

	if binary.BigEndian.Uint32(postfixBody[secTagLen:secTagLen+4]) != 0 {
		return nil, fmt.Errorf("dds: submessage receiver macs unsupported: %w", ErrSecuredTransformUnavailable)
	}

	sessionKey, tag, err := secSessionMaterials(prefix, postfixBody, lookup)
	if err != nil {
		return nil, err
	}

	if secKindEncrypts(prefix.kind) {
		return secOpenEncryptedBody(prefix, bodyRaw, sessionKey, tag, prefixRaw)
	}

	return secVerifySignedBody(prefix, bodyRaw, sessionKey, tag, prefixRaw)
}

// secSessionMaterials resolves master keys and derives the session key plus tag.
//
// The master salt is always 32 bytes per KeyMaterial_AES_GCM_GMAC while
// the master key is 16 or 32 bytes per the transform kind; the session
// key truncates to the kind size below, matching DeriveSessionKey.
func secSessionMaterials(prefix secPrefix, postfixBody []byte, lookup func(transformID, session uint32) (key, salt []byte, err error)) ([]byte, []byte, error) {
	masterKey, masterSalt, err := lookup(prefix.transformID, prefix.session)
	if err != nil {
		return nil, nil, err
	}

	keySize, err := secKindKeySize(prefix.kind)
	if err != nil {
		return nil, nil, err
	}

	if len(masterKey) != keySize || len(masterSalt) != 32 {
		return nil, nil, fmt.Errorf("dds: submessage master key need %d salt need 32: %w", keySize, ErrSecuredTransformUnavailable)
	}

	sessionKey, err := crypto.DeriveSessionKey(masterKey, masterSalt, prefix.session, "SessionKey")
	if err != nil {
		return nil, nil, fmt.Errorf("dds: submessage session key: %w", err)
	}

	if len(sessionKey) > keySize {
		sessionKey = sessionKey[:keySize]
	}

	tag := append([]byte(nil), postfixBody[:secTagLen]...)

	return sessionKey, tag, nil
}

// secOpenEncryptedBody decrypts one SEC_BODY submessage with prefix AAD.
func secOpenEncryptedBody(prefix secPrefix, bodyRaw, sessionKey, tag, aad []byte) ([]byte, error) {
	if len(bodyRaw) == 0 || bodyRaw[0] != secBodyID {
		return nil, fmt.Errorf("dds: submessage encrypted body id: %w", ErrSecuredTransformUnavailable)
	}

	body, err := secSubmessageBody(bodyRaw)
	if err != nil {
		return nil, err
	}

	if len(body) < 4 {
		return nil, fmt.Errorf("dds: submessage secure body need length: %w", ErrSecuredTransformUnavailable)
	}

	length := binary.BigEndian.Uint32(body[0:4])
	if int(length) > len(body)-4 {
		return nil, fmt.Errorf("dds: submessage ciphertext overruns: %w", ErrSecuredTransformUnavailable)
	}

	ciphertext := append([]byte(nil), body[4:4+length]...)
	ciphertext = append(ciphertext, tag...)

	return secGCMOpen(sessionKey, prefix.iv[:], ciphertext, aad)
}

// secVerifySignedBody verifies one SIGN body submessage with prefix AAD.
func secVerifySignedBody(prefix secPrefix, bodyRaw, sessionKey, tag, aad []byte) ([]byte, error) {
	inner, err := secSubmessageWhole(bodyRaw)
	if err != nil {
		return nil, err
	}

	err = secVerifyGMAC(sessionKey, prefix.iv[:], inner, tag, aad)
	if err != nil {
		return nil, err
	}

	return inner, nil
}

// secJoinTriple frames prefix plus body plus postfix submessages.
func secJoinTriple(prefix, body, postfix rtps.RawSubmessage) []byte {
	out := append([]byte(nil), secFrameSubmessage(prefix)...)
	out = append(out, secFrameSubmessage(body)...)
	out = append(out, secFrameSubmessage(postfix)...)

	return out
}

// secFrameSubmessage renders one submessage with its 4 byte header.
func secFrameSubmessage(sub rtps.RawSubmessage) []byte {
	out := []byte{sub.ID, sub.Flags, 0x00, 0x00}

	if sub.Flags&rtps.FlagLittleEndian != 0 {
		binary.LittleEndian.PutUint16(out[2:4], uint16(len(sub.Body))) //nolint:gosec // G115: bodies bounded by submessage cap
	} else {
		binary.BigEndian.PutUint16(out[2:4], uint16(len(sub.Body))) //nolint:gosec // G115: bodies bounded by submessage cap
	}

	return append(out, sub.Body...)
}

// secUnwrapTriple verifies and opens one SEC triple.
//
// lookup resolves the master key and salt for one transform id and
// session. GMAC kinds verify the tag over the body bytes while GCM
// kinds decrypt the SEC_BODY content. The returned bytes are the
// complete inner submessage including its header.
func secUnwrapTriple(prefixRaw, bodyRaw, postfixRaw []byte, lookup func(transformID, session uint32) (key, salt []byte, err error)) ([]byte, error) {
	if len(prefixRaw) < 4 || prefixRaw[0] != secPrefixID {
		return nil, fmt.Errorf("dds: submessage unwrap needs SEC_PREFIX: %w", ErrSecuredTransformUnavailable)
	}

	if len(postfixRaw) < 4 || postfixRaw[0] != secPostfixID {
		return nil, fmt.Errorf("dds: submessage unwrap needs SEC_POSTFIX: %w", ErrSecuredTransformUnavailable)
	}

	if len(bodyRaw) < 4 {
		return nil, fmt.Errorf("dds: submessage body truncated: %w", ErrSecuredTransformUnavailable)
	}

	return secUnwrapSplit(prefixRaw, bodyRaw, postfixRaw, lookup)
}

// secSubmessageBody returns the content of one framed submessage.
func secSubmessageBody(raw []byte) ([]byte, error) {
	if len(raw) < 4 {
		return nil, fmt.Errorf("dds: submessage need header have %d: %w", len(raw), ErrSecuredTransformUnavailable)
	}

	var length int

	if raw[1]&rtps.FlagLittleEndian != 0 {
		length = int(binary.LittleEndian.Uint16(raw[2:4]))
	} else {
		length = int(binary.BigEndian.Uint16(raw[2:4]))
	}

	if len(raw)-4 < length {
		return nil, fmt.Errorf("dds: submessage need %d have %d: %w", length, len(raw)-4, ErrSecuredTransformUnavailable)
	}

	return append([]byte(nil), raw[4:4+length]...), nil
}

// secSubmessageWhole returns one complete framed submessage.
func secSubmessageWhole(raw []byte) ([]byte, error) {
	body, err := secSubmessageBody(raw)
	if err != nil {
		return nil, err
	}

	out := append([]byte{raw[0], raw[1], raw[2], raw[3]}, body...)

	return out, nil
}

// secGCMOpen decrypts ciphertext plus tag with prefix header as AAD.
func secGCMOpen(sessionKey, iv, ciphertext, aad []byte) ([]byte, error) {
	block, err := newAESCipher(sessionKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("dds: submessage gcm open: %w", err)
	}

	plain, err := gcm.Open(nil, iv, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("dds: submessage gcm open: %w", errors.Join(err, ErrSecuredTransformUnavailable))
	}

	return plain, nil
}

// secVerifyGMAC verifies one GMAC tag with prefix header as AAD.
func secVerifyGMAC(sessionKey, iv, body, tag, aad []byte) error {
	block, err := newAESCipher(sessionKey)
	if err != nil {
		return err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return fmt.Errorf("dds: submessage gcm verify: %w", err)
	}

	signed := append(append([]byte(nil), body...), tag...)

	_, err = gcm.Open(nil, iv, signed, aad)
	if err != nil {
		return fmt.Errorf("dds: submessage tag mismatch: %w", errors.Join(err, ErrSecuredTransformUnavailable))
	}

	return nil
}

// secUnwrapSplitWithOrigin verifies and opens one split SEC triple with origin-bound AAD.
//
// It mirrors secUnwrapSplit framing but seals and tags with
// originAAD instead of the bare prefix header, per 10.5.3.3. The
// origin is the 16 byte sender GUID (header prefix plus writer
// entity), never from the body. Origin-bound seals verify only
// here; prefix-only seals verify only in secUnwrapSplit, so
// cross-verify fails closed on AAD mismatch. Nil-suite tolerance
// never calls this path, keeping plain flow byte-identical.
func secUnwrapSplitWithOrigin(prefixRaw, bodyRaw, postfixRaw []byte, origin [16]byte, lookup func(transformID, session uint32) (key, salt []byte, err error)) ([]byte, error) {
	prefixBody, err := secSubmessageBody(prefixRaw)
	if err != nil {
		return nil, err
	}

	prefix, err := secParsePrefix(prefixBody)
	if err != nil {
		return nil, err
	}

	postfixBody, err := secSubmessageBody(postfixRaw)
	if err != nil {
		return nil, err
	}

	if len(postfixBody) != secPostfixContentLen {
		return nil, fmt.Errorf("dds: submessage postfix need %d have %d: %w", secPostfixContentLen, len(postfixBody), ErrSecuredTransformUnavailable)
	}

	if binary.BigEndian.Uint32(postfixBody[secTagLen:secTagLen+4]) != 0 {
		return nil, fmt.Errorf("dds: submessage receiver macs unsupported: %w", ErrSecuredTransformUnavailable)
	}

	sessionKey, tag, err := secSessionMaterials(prefix, postfixBody, lookup)
	if err != nil {
		return nil, err
	}

	aad := originAAD(origin, prefixRaw, nil)

	if secKindEncrypts(prefix.kind) {
		return secOpenEncryptedBody(prefix, bodyRaw, sessionKey, tag, aad)
	}

	return secVerifySignedBody(prefix, bodyRaw, sessionKey, tag, aad)
}

// secUnwrapTripleWithOrigin verifies and opens one SEC triple with origin-bound AAD.
//
// It checks the SEC_PREFIX plus SEC_POSTFIX framing then delegates
// to secUnwrapSplitWithOrigin, so origin-bound seals verify only
// here per 10.5.3.3. lookup resolves the master key and salt for one
// transform id and session.
func secUnwrapTripleWithOrigin(prefixRaw, bodyRaw, postfixRaw []byte, origin [16]byte, lookup func(transformID, session uint32) (key, salt []byte, err error)) ([]byte, error) {
	if len(prefixRaw) < 4 || prefixRaw[0] != secPrefixID {
		return nil, fmt.Errorf("dds: submessage unwrap needs SEC_PREFIX: %w", ErrSecuredTransformUnavailable)
	}

	if len(postfixRaw) < 4 || postfixRaw[0] != secPostfixID {
		return nil, fmt.Errorf("dds: submessage unwrap needs SEC_POSTFIX: %w", ErrSecuredTransformUnavailable)
	}

	if len(bodyRaw) < 4 {
		return nil, fmt.Errorf("dds: submessage body truncated: %w", ErrSecuredTransformUnavailable)
	}

	return secUnwrapSplitWithOrigin(prefixRaw, bodyRaw, postfixRaw, origin, lookup)
}
