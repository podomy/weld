// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package tokens holds CDR big-endian codecs for handshake and crypto tokens.
package tokens

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Sentinel errors for token codec failures.
var (
	// ErrTokenTruncated reports a short buffer.
	ErrTokenTruncated = errors.New("tokens: truncated")
	// ErrTokenInvalid reports a malformed field.
	ErrTokenInvalid = errors.New("tokens: invalid")
	// ErrTokenTooLarge reports a field above its cap.
	ErrTokenTooLarge = errors.New("tokens: too large")
)

// Message class ids for the stateless channel.
const (
	// ClassAuthRequest names the bootstrap auth request class.
	ClassAuthRequest = "dds.sec.auth_request"
	// ClassAuth names the handshake message class.
	ClassAuth = "dds.sec.auth"
)

// Token class ids for PKI-DH handshake tokens.
//
//nolint:gosec // G101: protocol class ids are constants, not credentials.
const (
	// TokenAuthReq names the AuthRequestMessageToken class.
	TokenAuthReq = "DDS:Auth:PKI-DH:1.0+AuthReq"
	// TokenReq names the HandshakeRequestMessageToken class.
	TokenReq = "DDS:Auth:PKI-DH:1.0+Req"
	// TokenReply names the HandshakeReplyMessageToken class.
	TokenReply = "DDS:Auth:PKI-DH:1.0+Reply"
	// TokenFinal names the final handshake token class.
	TokenFinal = "DDS:Auth:PKI-DH:1.0+Final"
)

// Caps for bounded decoding.
//
// maxBytesLen matches spec.go maxSpecBytes: handshake binary properties
// such as c.perm and c.pdata routinely exceed 4 KiB, and the spec-side
// framing already bounds vectors at 16 KiB. Keeping two caps would fail
// closed on compliant peers on one path while admitting them on the
// other. maxStringLen matches spec.go maxSpecString for the same
// reason: identity subject names and algorithm ids decode on both
// paths. maxTokens matches spec.go maxSpecHolders so one envelope
// cannot carry more holders on the compact path than the spec path
// admits.
//
// INTERFACE REQUEST: caps are unified but the two codecs stay separate.
// A single-codec merge (one CDR path for volatile plus generic plus
// handshake) is out of scope for this batch; the owning batch should
// propose exact function signatures in tokens/codec.go plus spec.go
// after proving every existing test stays green with one shared
// string, bytes, holder, and prop cap.
const (
	// maxStringLen caps one decoded string without NUL.
	maxStringLen = 4096
	// maxBytesLen caps one decoded byte vector.
	maxBytesLen = 16384
	// maxTokens caps one decoded token sequence.
	maxTokens = 8
	// maxReceiverMACs caps one decoded footer list.
	maxReceiverMACs = 8
)

// encodeUint32 appends one big-endian uint32.
func encodeUint32(out []byte, value uint32) []byte {
	var raw [4]byte

	binary.BigEndian.PutUint32(raw[:], value)

	return append(out, raw[:]...)
}

// decodeUint32 reads one big-endian uint32 at off.
func decodeUint32(raw []byte, off int) (uint32, int, error) {
	if len(raw)-off < 4 {
		return 0, 0, fmt.Errorf("tokens: uint32 need 4 have %d: %w", len(raw)-off, ErrTokenTruncated)
	}

	return binary.BigEndian.Uint32(raw[off : off+4]), off + 4, nil
}

// encodeUint64 appends one uint64 as high word then low word, big-endian.
func encodeUint64(out []byte, value uint64) []byte {
	out = encodeUint32(out, uint32(value>>32))
	out = encodeUint32(out, uint32(value)) //nolint:gosec // G115: low word truncation is the layout

	return out
}

// decodeUint64 reads one high plus low word pair at off.
func decodeUint64(raw []byte, off int) (uint64, int, error) {
	high, next, err := decodeUint32(raw, off)
	if err != nil {
		return 0, 0, fmt.Errorf("tokens: uint64 high: %w", err)
	}

	low, next, err := decodeUint32(raw, next)
	if err != nil {
		return 0, 0, fmt.Errorf("tokens: uint64 low: %w", err)
	}

	return uint64(high)<<32 | uint64(low), next, nil
}

// padLen returns the zero pad needed to reach a 4 byte boundary.
func padLen(n int) int {
	return (4 - n%4) % 4
}

// encodeString appends one CDR string with NUL plus pad, big-endian.
func encodeString(out []byte, value string) ([]byte, error) {
	if len(value) > maxStringLen {
		return nil, fmt.Errorf("tokens: string length %d above %d: %w", len(value), maxStringLen, ErrTokenTooLarge)
	}

	length := uint32(len(value) + 1) //nolint:gosec // G115: length guarded by maxStringLen

	out = encodeUint32(out, length)
	out = append(out, value...)
	out = append(out, 0x00)

	for range padLen(len(value) + 1) {
		out = append(out, 0x00)
	}

	return out, nil
}

// decodeString reads one CDR string at off.
func decodeString(raw []byte, off int) (string, int, error) {
	length, next, err := decodeUint32(raw, off)
	if err != nil {
		return "", 0, fmt.Errorf("tokens: string header: %w", err)
	}

	if length < 1 || length > maxStringLen+1 {
		return "", 0, fmt.Errorf("tokens: string length %d: %w", length, ErrTokenInvalid)
	}

	need := int(length)
	if len(raw)-next < need {
		return "", 0, fmt.Errorf("tokens: string need %d have %d: %w", need, len(raw)-next, ErrTokenTruncated)
	}

	body := raw[next : next+need]
	if body[len(body)-1] != 0x00 {
		return "", 0, fmt.Errorf("tokens: string misses NUL: %w", ErrTokenInvalid)
	}

	out := string(body[:len(body)-1])
	next += need
	next += padLen(need)

	if next > len(raw) {
		return "", 0, fmt.Errorf("tokens: string pad overruns: %w", ErrTokenTruncated)
	}

	return out, next, nil
}

// encodeBytes appends one length prefixed byte vector with pad.
func encodeBytes(out []byte, value []byte) ([]byte, error) {
	if len(value) > maxBytesLen {
		return nil, fmt.Errorf("tokens: bytes length %d above %d: %w", len(value), maxBytesLen, ErrTokenTooLarge)
	}

	out = encodeUint32(out, uint32(len(value))) //nolint:gosec // G115: length guarded by maxBytesLen
	out = append(out, value...)

	for range padLen(len(value)) {
		out = append(out, 0x00)
	}

	return out, nil
}

// decodeBytes reads one length prefixed byte vector at off.
func decodeBytes(raw []byte, off int) ([]byte, int, error) {
	length, next, err := decodeUint32(raw, off)
	if err != nil {
		return nil, 0, fmt.Errorf("tokens: bytes header: %w", err)
	}

	if length > maxBytesLen {
		return nil, 0, fmt.Errorf("tokens: bytes length %d above %d: %w", length, maxBytesLen, ErrTokenTooLarge)
	}

	need := int(length)
	if len(raw)-next < need {
		return nil, 0, fmt.Errorf("tokens: bytes need %d have %d: %w", need, len(raw)-next, ErrTokenTruncated)
	}

	out := append([]byte(nil), raw[next:next+need]...)
	next += need
	next += padLen(need)

	if next > len(raw) {
		return nil, 0, fmt.Errorf("tokens: bytes pad overruns: %w", ErrTokenTruncated)
	}

	return out, next, nil
}

// AuthRequestToken carries a 256-bit future challenge nonce.
type AuthRequestToken struct {
	// Class is the token class id, normally TokenAuthReq.
	Class string
	// FutureChallenge is the 256-bit bootstrap nonce.
	FutureChallenge [32]byte
}

// EncodeAuthRequest renders one auth request token.
//
// The output is the DataHolder CDR serialization carried inside a
// ParticipantGenericMessage, per section 7.4.
func EncodeAuthRequest(token AuthRequestToken) ([]byte, error) {
	holder := BuildAuthRequestToken(token.FutureChallenge)

	out, err := EncodeDataHolder(holder)
	if err != nil {
		return nil, fmt.Errorf("tokens: auth request encode: %w", err)
	}

	return out, nil
}

// DecodeAuthRequest parses one auth request token.
func DecodeAuthRequest(raw []byte) (AuthRequestToken, error) {
	var out AuthRequestToken

	holder, err := DecodeDataHolder(raw, false)
	if err != nil {
		return out, fmt.Errorf("tokens: auth request holder: %w", err)
	}

	challenge, err := ParseAuthRequestToken(holder)
	if err != nil {
		return out, err
	}

	out.Class = TokenAuthReq
	out.FutureChallenge = challenge

	return out, nil
}

// HandshakeRequestToken carries challenge1 plus the ECDHE public key.
type HandshakeRequestToken struct {
	// Class is the token class id, normally TokenReq.
	Class string
	// Challenge1 is the 256-bit initiator nonce.
	Challenge1 [32]byte
	// DH1 holds the initiator ECDHE public key bytes.
	DH1 []byte
	// KeyAgreeAlgo names the agreement algorithm.
	KeyAgreeAlgo string
	// SignAlgo names the signature algorithm.
	SignAlgo string
	// IdentityCert holds the DER identity certificate for c.id.
	IdentityCert []byte
	// Permissions holds the permissions document for c.perm.
	Permissions []byte
	// ParticipantData holds the serialized participant data for c.pdata.
	ParticipantData []byte
}

// EncodeHandshakeRequest renders one handshake request token.
//
// The output is the DataHolder CDR serialization carried inside a
// ParticipantGenericMessage, per section 7.4.
func EncodeHandshakeRequest(token HandshakeRequestToken) ([]byte, error) {
	holder, err := BuildHandshakeRequest(token)
	if err != nil {
		return nil, err
	}

	out, err := EncodeDataHolder(holder)
	if err != nil {
		return nil, fmt.Errorf("tokens: handshake request encode: %w", err)
	}

	return out, nil
}

// DecodeHandshakeRequest parses one handshake request token.
func DecodeHandshakeRequest(raw []byte) (HandshakeRequestToken, error) {
	var out HandshakeRequestToken

	holder, err := DecodeDataHolder(raw, false)
	if err != nil {
		return out, fmt.Errorf("tokens: handshake request holder: %w", err)
	}

	parsed, err := ParseHandshakeRequest(holder)
	if err != nil {
		return out, err
	}

	return parsed, nil
}

// HandshakeReplyToken carries the echoed challenge plus responder key material.
type HandshakeReplyToken struct {
	// Class is the token class id, normally TokenReply.
	Class string
	// Challenge1Echo echoes the initiator nonce.
	Challenge1Echo [32]byte
	// Challenge2 is the 256-bit responder nonce.
	Challenge2 [32]byte
	// DH2 holds the responder ECDHE public key bytes.
	DH2 []byte
	// Signature carries the responder signature over the token hashes.
	Signature []byte
	// KeyAgreeAlgo names the agreed agreement algorithm.
	KeyAgreeAlgo string
	// SignAlgo names the responder signature algorithm.
	SignAlgo string
	// IdentityCert holds the DER identity certificate for c.id.
	IdentityCert []byte
	// Permissions holds the permissions document for c.perm.
	Permissions []byte
	// ParticipantData holds the serialized participant data for c.pdata.
	ParticipantData []byte
}

// EncodeHandshakeReply renders one handshake reply token.
//
// The output is the DataHolder CDR serialization carried inside a
// ParticipantGenericMessage, per section 7.4.
func EncodeHandshakeReply(token HandshakeReplyToken) ([]byte, error) {
	holder, err := BuildHandshakeReply(token)
	if err != nil {
		return nil, err
	}

	out, err := EncodeDataHolder(holder)
	if err != nil {
		return nil, fmt.Errorf("tokens: handshake reply encode: %w", err)
	}

	return out, nil
}

// DecodeHandshakeReply parses one handshake reply token.
func DecodeHandshakeReply(raw []byte) (HandshakeReplyToken, error) {
	var out HandshakeReplyToken

	holder, err := DecodeDataHolder(raw, false)
	if err != nil {
		return out, fmt.Errorf("tokens: handshake reply holder: %w", err)
	}

	parsed, err := ParseHandshakeReply(holder)
	if err != nil {
		return out, err
	}

	return parsed, nil
}

// HandshakeFinalToken proves possession of challenge2.
type HandshakeFinalToken struct {
	// Class is the token class id, normally TokenFinal.
	Class string
	// Challenge1Echo echoes the initiator nonce.
	Challenge1Echo [32]byte
	// Challenge2Echo echoes the responder nonce.
	Challenge2Echo [32]byte
	// Signature carries the initiator signature over the token hashes.
	Signature []byte
}

// EncodeHandshakeFinal renders one final token.
//
// The output is the DataHolder CDR serialization carried inside a
// ParticipantGenericMessage, per section 7.4.
func EncodeHandshakeFinal(token HandshakeFinalToken) ([]byte, error) {
	holder, err := BuildHandshakeFinal(token)
	if err != nil {
		return nil, err
	}

	out, err := EncodeDataHolder(holder)
	if err != nil {
		return nil, fmt.Errorf("tokens: handshake final encode: %w", err)
	}

	return out, nil
}

// DecodeHandshakeFinal parses one final token.
func DecodeHandshakeFinal(raw []byte) (HandshakeFinalToken, error) {
	var out HandshakeFinalToken

	holder, err := DecodeDataHolder(raw, false)
	if err != nil {
		return out, fmt.Errorf("tokens: handshake final holder: %w", err)
	}

	parsed, err := ParseHandshakeFinal(holder)
	if err != nil {
		return out, err
	}

	return parsed, nil
}

// KeyMaterial mirrors KeyMaterial_AES_GCM_GMAC per 10.5.2.1.1.
type KeyMaterial struct {
	// TransformationKind selects the crypto transform.
	TransformationKind uint32
	// MasterSalt is the 32 byte salt for session derivation.
	MasterSalt []byte
	// SenderKeyID identifies the sender key.
	SenderKeyID uint32
	// MasterSenderKey is the 16 or 32 byte sender master key.
	MasterSenderKey []byte
	// ReceiverKeyID identifies the receiver specific key.
	ReceiverKeyID uint32
	// MasterReceiverKey is the receiver specific master key, empty when unused.
	MasterReceiverKey []byte
}

// EncodeKeyMaterial renders one key material record, big-endian.
func EncodeKeyMaterial(material KeyMaterial) ([]byte, error) {
	out := encodeUint32(nil, material.TransformationKind)

	var err error

	out, err = encodeBytes(out, material.MasterSalt)
	if err != nil {
		return nil, fmt.Errorf("tokens: key material salt: %w", err)
	}

	out = encodeUint32(out, material.SenderKeyID)

	out, err = encodeBytes(out, material.MasterSenderKey)
	if err != nil {
		return nil, fmt.Errorf("tokens: key material sender key: %w", err)
	}

	out = encodeUint32(out, material.ReceiverKeyID)

	out, err = encodeBytes(out, material.MasterReceiverKey)
	if err != nil {
		return nil, fmt.Errorf("tokens: key material receiver key: %w", err)
	}

	return out, nil
}

// DecodeKeyMaterial parses one key material record.
func DecodeKeyMaterial(raw []byte) (KeyMaterial, error) {
	var out KeyMaterial

	kind, next, err := decodeUint32(raw, 0)
	if err != nil {
		return out, fmt.Errorf("tokens: key material kind: %w", err)
	}

	salt, next2, err := decodeBytes(raw, next)
	if err != nil {
		return out, fmt.Errorf("tokens: key material salt: %w", err)
	}

	next = next2

	senderID, next2, err := decodeUint32(raw, next)
	if err != nil {
		return out, fmt.Errorf("tokens: key material sender id: %w", err)
	}

	next = next2

	senderKey, next2, err := decodeBytes(raw, next)
	if err != nil {
		return out, fmt.Errorf("tokens: key material sender key: %w", err)
	}

	next = next2

	receiverID, next2, err := decodeUint32(raw, next)
	if err != nil {
		return out, fmt.Errorf("tokens: key material receiver id: %w", err)
	}

	next = next2

	receiverKey, next2, err := decodeBytes(raw, next)
	if err != nil {
		return out, fmt.Errorf("tokens: key material receiver key: %w", err)
	}

	next = next2

	if next != len(raw) {
		return out, fmt.Errorf("tokens: key material has %d trailing bytes: %w", len(raw)-next, ErrTokenInvalid)
	}

	if len(salt) != 32 {
		return out, fmt.Errorf("tokens: key material salt need 32 have %d: %w", len(salt), ErrTokenInvalid)
	}

	if len(senderKey) != 16 && len(senderKey) != 32 {
		return out, fmt.Errorf("tokens: key material sender key need 16 or 32 have %d: %w", len(senderKey), ErrTokenInvalid)
	}

	out.TransformationKind = kind
	out.MasterSalt = salt
	out.SenderKeyID = senderID
	out.MasterSenderKey = senderKey
	out.ReceiverKeyID = receiverID
	out.MasterReceiverKey = receiverKey

	return out, nil
}

// EncodeCryptoTokenSeq renders one token sequence, big-endian.
func EncodeCryptoTokenSeq(tokens []KeyMaterial) ([]byte, error) {
	if len(tokens) > maxTokens {
		return nil, fmt.Errorf("tokens: token seq length %d above %d: %w", len(tokens), maxTokens, ErrTokenTooLarge)
	}

	out := encodeUint32(nil, uint32(len(tokens))) //nolint:gosec // G115: length guarded by maxTokens

	for i := range tokens {
		encoded, err := EncodeKeyMaterial(tokens[i])
		if err != nil {
			return nil, fmt.Errorf("tokens: token seq entry %d: %w", i, err)
		}

		var err2 error

		out, err2 = encodeBytes(out, encoded)
		if err2 != nil {
			return nil, fmt.Errorf("tokens: token seq wrap %d: %w", i, err2)
		}
	}

	return out, nil
}

// DecodeCryptoTokenSeq parses one token sequence.
func DecodeCryptoTokenSeq(raw []byte) ([]KeyMaterial, error) {
	count, next, err := decodeUint32(raw, 0)
	if err != nil {
		return nil, fmt.Errorf("tokens: token seq header: %w", err)
	}

	if count > maxTokens {
		return nil, fmt.Errorf("tokens: token seq length %d above %d: %w", count, maxTokens, ErrTokenTooLarge)
	}

	out := make([]KeyMaterial, 0, count)

	for range count {
		wrapped, next2, err := decodeBytes(raw, next)
		if err != nil {
			return nil, fmt.Errorf("tokens: token seq wrap: %w", err)
		}

		next = next2

		material, err := DecodeKeyMaterial(wrapped)
		if err != nil {
			return nil, fmt.Errorf("tokens: token seq entry: %w", err)
		}

		out = append(out, material)
	}

	if next != len(raw) {
		return nil, fmt.Errorf("tokens: token seq has %d trailing bytes: %w", len(raw)-next, ErrTokenInvalid)
	}

	return out, nil
}

// CryptoHeader carries session routing for one protected submessage.
type CryptoHeader struct {
	// TransformationKind selects the crypto transform.
	TransformationKind uint32
	// SessionID selects the session key derivation input.
	SessionID uint32
	// IVSuffix is the 8 byte per-message suffix, big-endian on the wire.
	IVSuffix [8]byte
}

// EncodeCryptoHeader renders one 16 byte header, big-endian.
func EncodeCryptoHeader(header CryptoHeader) []byte {
	out := encodeUint32(nil, header.TransformationKind)
	out = encodeUint32(out, header.SessionID)
	out = append(out, header.IVSuffix[:]...)

	return out
}

// DecodeCryptoHeader parses one 16 byte header.
func DecodeCryptoHeader(raw []byte) (CryptoHeader, error) {
	var out CryptoHeader

	if len(raw) != 16 {
		return out, fmt.Errorf("tokens: crypto header need 16 have %d: %w", len(raw), ErrTokenInvalid)
	}

	kind, next, err := decodeUint32(raw, 0)
	if err != nil {
		return out, fmt.Errorf("tokens: crypto header kind: %w", err)
	}

	session, _, err := decodeUint32(raw, next)
	if err != nil {
		return out, fmt.Errorf("tokens: crypto header session: %w", err)
	}

	out.TransformationKind = kind
	out.SessionID = session
	copy(out.IVSuffix[:], raw[8:16])

	return out, nil
}

// CryptoFooter carries message authentication tags.
type CryptoFooter struct {
	// CommonMAC is the 16 byte GCM tag over header plus ciphertext.
	CommonMAC [16]byte
	// ReceiverMACs holds optional per-receiver tags.
	ReceiverMACs [][]byte
}

// EncodeCryptoFooter renders one footer, big-endian.
func EncodeCryptoFooter(footer CryptoFooter) ([]byte, error) {
	if len(footer.ReceiverMACs) > maxReceiverMACs {
		return nil, fmt.Errorf("tokens: footer macs %d above %d: %w", len(footer.ReceiverMACs), maxReceiverMACs, ErrTokenTooLarge)
	}

	out := append([]byte(nil), footer.CommonMAC[:]...)
	out = encodeUint32(out, uint32(len(footer.ReceiverMACs))) //nolint:gosec // G115: length guarded by maxReceiverMACs

	for i := range footer.ReceiverMACs {
		if len(footer.ReceiverMACs[i]) != 16 {
			return nil, fmt.Errorf("tokens: footer mac %d need 16 have %d: %w", i, len(footer.ReceiverMACs[i]), ErrTokenInvalid)
		}

		out = append(out, footer.ReceiverMACs[i]...)
	}

	return out, nil
}

// DecodeCryptoFooter parses one footer.
func DecodeCryptoFooter(raw []byte) (CryptoFooter, error) {
	var out CryptoFooter

	if len(raw) < 20 {
		return out, fmt.Errorf("tokens: footer need 20 have %d: %w", len(raw), ErrTokenTruncated)
	}

	copy(out.CommonMAC[:], raw[:16])

	count, _, err := decodeUint32(raw, 16)
	if err != nil {
		return out, fmt.Errorf("tokens: footer count: %w", err)
	}

	if count > maxReceiverMACs {
		return out, fmt.Errorf("tokens: footer macs %d above %d: %w", count, maxReceiverMACs, ErrTokenTooLarge)
	}

	want := 20 + int(count)*16
	if len(raw) != want {
		return out, fmt.Errorf("tokens: footer need %d have %d: %w", want, len(raw), ErrTokenInvalid)
	}

	for i := range count {
		start := 20 + int(i)*16
		out.ReceiverMACs = append(out.ReceiverMACs, append([]byte(nil), raw[start:start+16]...))
	}

	return out, nil
}

// StatelessMessage is the volatile envelope for one handshake sample.
type StatelessMessage struct {
	// ClassID selects the handshake class.
	ClassID string
	// MessageID is the sender sequence for duplicate suppression.
	MessageID uint64
	// RelatedID links replies to their request.
	RelatedID uint64
	// DestinationGUID routes the sample, zero means broadcast.
	DestinationGUID [16]byte
	// SourceGUID names the sender participant.
	SourceGUID [16]byte
	// Payload holds the encoded token bytes.
	Payload []byte
}

// EncodeStateless renders one stateless envelope, big-endian.
func EncodeStateless(message StatelessMessage) ([]byte, error) {
	out, err := encodeString(nil, message.ClassID)
	if err != nil {
		return nil, fmt.Errorf("tokens: stateless class: %w", err)
	}

	if message.ClassID != ClassAuthRequest && message.ClassID != ClassAuth {
		return nil, fmt.Errorf("tokens: stateless class %q: %w", message.ClassID, ErrTokenInvalid)
	}

	out = encodeUint64(out, message.MessageID)
	out = encodeUint64(out, message.RelatedID)
	out = append(out, message.DestinationGUID[:]...)
	out = append(out, message.SourceGUID[:]...)

	out, err = encodeBytes(out, message.Payload)
	if err != nil {
		return nil, fmt.Errorf("tokens: stateless payload: %w", err)
	}

	return out, nil
}

// DecodeStateless parses one stateless envelope.
func DecodeStateless(raw []byte) (StatelessMessage, error) {
	var out StatelessMessage

	class, next, err := decodeString(raw, 0)
	if err != nil {
		return out, fmt.Errorf("tokens: stateless class: %w", err)
	}

	if class != ClassAuthRequest && class != ClassAuth {
		return out, fmt.Errorf("tokens: stateless class %q: %w", class, ErrTokenInvalid)
	}

	msgID, next2, err := decodeUint64(raw, next)
	if err != nil {
		return out, fmt.Errorf("tokens: stateless message id: %w", err)
	}

	next = next2

	related, next2, err := decodeUint64(raw, next)
	if err != nil {
		return out, fmt.Errorf("tokens: stateless related id: %w", err)
	}

	next = next2

	if len(raw)-next < 32 {
		return out, fmt.Errorf("tokens: stateless guids need 32: %w", ErrTokenTruncated)
	}

	out.ClassID = class
	out.MessageID = msgID
	out.RelatedID = related
	copy(out.DestinationGUID[:], raw[next:next+16])
	copy(out.SourceGUID[:], raw[next+16:next+32])
	next += 32

	payload, next2, err := decodeBytes(raw, next)
	if err != nil {
		return out, fmt.Errorf("tokens: stateless payload: %w", err)
	}

	next = next2

	if next != len(raw) {
		return out, fmt.Errorf("tokens: stateless has %d trailing bytes: %w", len(raw)-next, ErrTokenInvalid)
	}

	out.Payload = payload

	return out, nil
}

// VolatileEnvelope carries key material over the volatile channel.
type VolatileEnvelope struct {
	// DestinationGUID names the intended participant.
	DestinationGUID [16]byte
	// SourceGUID names the sender participant.
	SourceGUID [16]byte
	// Tokens holds the key material sequence bytes.
	Tokens []KeyMaterial
}

// volatileSpecClasses accepts the three crypto token exchange classes.
func volatileSpecClasses(class string) bool {
	return class == ClassParticipantTokens || class == ClassWriterTokens || class == ClassReaderTokens
}

// EncodeVolatile renders one volatile envelope as a spec GenericMessage.
//
// Per DDS Security 7.4 the volatile channel carries a
// ParticipantGenericMessage whose Data holds crypto tokens
// (DDS:Crypto:AES_GCM_GMAC with dds.cryp.keymat). The destination
// participant routes the sample; endpoints stay zero for participant
// tokens. Decoding accepts both this spec form and the legacy compact
// envelope below for interop.
func EncodeVolatile(envelope VolatileEnvelope) ([]byte, error) {
	return EncodeVolatileSpec(envelope, ClassParticipantTokens)
}

// EncodeVolatileSpec renders one envelope under the given exchange class.
func EncodeVolatileSpec(envelope VolatileEnvelope, class string) ([]byte, error) {
	if !volatileSpecClasses(class) {
		return nil, fmt.Errorf("tokens: volatile class %q: %w", class, ErrTokenInvalid)
	}

	holders := make([]DataHolder, 0, len(envelope.Tokens))
	for i := range envelope.Tokens {
		holder, err := BuildCryptoToken(envelope.Tokens[i])
		if err != nil {
			return nil, fmt.Errorf("tokens: volatile token %d: %w", i, err)
		}

		holders = append(holders, holder)
	}

	msg := GenericMessage{
		SourceGUID:                 envelope.SourceGUID,
		DestinationParticipantGUID: envelope.DestinationGUID,
		ClassID:                    class,
		Data:                       holders,
	}

	out, err := EncodeGenericPayload(msg)
	if err != nil {
		return nil, fmt.Errorf("tokens: volatile generic: %w", err)
	}

	return out, nil
}

// DecodeVolatile parses one volatile envelope.
//
// It accepts the spec GenericMessage form first and falls back to the
// legacy compact envelope (destination plus source GUIDs plus wrapped
// token sequence) so old captures still parse.
func DecodeVolatile(raw []byte) (VolatileEnvelope, error) {
	if msg, err := DecodeGenericPayload(raw); err == nil && volatileSpecClasses(msg.ClassID) {
		return parseVolatileSpec(msg)
	}

	return decodeVolatileLegacy(raw)
}

// parseVolatileSpec converts one spec GenericMessage into an envelope.
func parseVolatileSpec(msg GenericMessage) (VolatileEnvelope, error) {
	var out VolatileEnvelope

	out.SourceGUID = msg.SourceGUID
	out.DestinationGUID = msg.DestinationParticipantGUID

	for i := range msg.Data {
		material, err := ParseCryptoToken(msg.Data[i])
		if err != nil {
			return out, fmt.Errorf("tokens: volatile token %d: %w", i, err)
		}

		out.Tokens = append(out.Tokens, material)
	}

	return out, nil
}

// decodeVolatileLegacy parses the pre-spec compact volatile envelope.
func decodeVolatileLegacy(raw []byte) (VolatileEnvelope, error) {
	var out VolatileEnvelope

	if len(raw) < 32 {
		return out, fmt.Errorf("tokens: volatile guids need 32: %w", ErrTokenTruncated)
	}

	copy(out.DestinationGUID[:], raw[:16])
	copy(out.SourceGUID[:], raw[16:32])

	wrapped, next, err := decodeBytes(raw, 32)
	if err != nil {
		return out, fmt.Errorf("tokens: volatile wrap: %w", err)
	}

	if next != len(raw) {
		return out, fmt.Errorf("tokens: volatile has %d trailing bytes: %w", len(raw)-next, ErrTokenInvalid)
	}

	tokens, err := DecodeCryptoTokenSeq(wrapped)
	if err != nil {
		return out, fmt.Errorf("tokens: volatile tokens: %w", err)
	}

	out.Tokens = tokens

	return out, nil
}
