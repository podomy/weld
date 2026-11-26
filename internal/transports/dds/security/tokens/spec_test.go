// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package tokens

import (
	"bytes"
	"testing"
)

// TestSpecDataHolderRoundtrip pins big-endian DataHolder CDR.
func TestSpecDataHolderRoundtrip(t *testing.T) {
	t.Parallel()

	holder := DataHolder{
		ClassID:    TokenReq,
		Properties: []Property{{Name: "a", Value: "b"}},
		BinaryProperties: []BinaryProperty{
			{Name: PropCID, Value: []byte{0x01, 0x02}},
			{Name: PropChallenge1, Value: bytes.Repeat([]byte{0x07}, 32)},
		},
	}

	raw, err := EncodeDataHolder(holder)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := DecodeDataHolder(raw, false)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.ClassID != holder.ClassID {
		t.Fatalf("class %q, want %q", decoded.ClassID, holder.ClassID)
	}

	if len(decoded.BinaryProperties) != 2 {
		t.Fatalf("binary props %d, want 2", len(decoded.BinaryProperties))
	}

	value, ok := FindBinaryProperty(decoded, PropChallenge1)
	if !ok || !bytes.Equal(value, bytes.Repeat([]byte{0x07}, 32)) {
		t.Fatalf("challenge1 mismatch")
	}
}

// TestSpecDataHolderRejects pins fail-closed DataHolder decoding.
func TestSpecDataHolderRejects(t *testing.T) {
	t.Parallel()

	holder := DataHolder{ClassID: TokenReq}

	raw, err := EncodeDataHolder(holder)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if _, err := DecodeDataHolder(raw[:len(raw)-1], false); err == nil {
		t.Fatalf("truncated holder must fail")
	}

	if _, err := DecodeDataHolder(append(raw, 0x00), false); err == nil {
		t.Fatalf("trailing holder must fail")
	}

	other, err := DecodeDataHolder(raw, true)
	if err == nil {
		_ = other

		t.Fatalf("little-endian misread must fail")
	}
}

// TestSpecGenericMessageRoundtrip pins ParticipantGenericMessage CDR.
func TestSpecGenericMessageRoundtrip(t *testing.T) {
	t.Parallel()

	msg := buildSpecGenericMessage()
	payload := encodeSpecGenericMessage(t, msg)
	decoded := decodeSpecGenericMessage(t, payload)
	checkSpecGenericDecoded(t, decoded)
}

// buildSpecGenericMessage returns one fixed GenericMessage fixture.
func buildSpecGenericMessage() GenericMessage {
	var source, dest, endpoint [16]byte

	for i := range source {
		source[i] = byte(i)
		dest[i] = byte(0x80 + i)
		endpoint[i] = byte(0x40 + i)
	}

	holder := DataHolder{
		ClassID:          TokenReply,
		BinaryProperties: []BinaryProperty{{Name: PropSignature, Value: []byte{0x09, 0x08}}},
	}

	return GenericMessage{
		SourceGUID:                 source,
		SourceSeq:                  7,
		RelatedSourceGUID:          dest,
		RelatedSeq:                 3,
		DestinationParticipantGUID: dest,
		DestinationEndpointGUID:    endpoint,
		SourceEndpointGUID:         endpoint,
		ClassID:                    ClassAuthMsg,
		Data:                       []DataHolder{holder},
	}
}

// encodeSpecGenericMessage encodes and pins the CDR header.
func encodeSpecGenericMessage(t *testing.T, msg GenericMessage) []byte {
	t.Helper()

	payload, err := EncodeGenericPayload(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if payload[0] != 0x00 || payload[1] != 0x00 || payload[2] != 0x00 || payload[3] != 0x00 {
		t.Fatalf("payload header % X, want big-endian CDR", payload[:4])
	}

	return payload
}

// decodeSpecGenericMessage decodes one payload, failing loudly.
func decodeSpecGenericMessage(t *testing.T, payload []byte) GenericMessage {
	t.Helper()

	decoded, err := DecodeGenericPayload(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	return decoded
}

// checkSpecGenericDecoded pins the decoded fixture shape.
func checkSpecGenericDecoded(t *testing.T, decoded GenericMessage) {
	t.Helper()

	var source, dest [16]byte

	for i := range source {
		source[i] = byte(i)
		dest[i] = byte(0x80 + i)
	}

	if decoded.SourceSeq != 7 || decoded.RelatedSeq != 3 {
		t.Fatalf("seq mismatch %d/%d", decoded.SourceSeq, decoded.RelatedSeq)
	}

	if decoded.SourceGUID != source || decoded.DestinationParticipantGUID != dest {
		t.Fatalf("guid mismatch")
	}

	if decoded.ClassID != ClassAuthMsg || len(decoded.Data) != 1 {
		t.Fatalf("class or data mismatch")
	}

	if decoded.Data[0].ClassID != TokenReply {
		t.Fatalf("token class %q", decoded.Data[0].ClassID)
	}
}

// TestSpecGenericMessageRejects pins fail-closed GenericMessage decoding.
func TestSpecGenericMessageRejects(t *testing.T) {
	t.Parallel()

	msg := GenericMessage{ClassID: ClassAuthMsg, Data: []DataHolder{{ClassID: TokenFinal}}}

	payload, err := EncodeGenericPayload(msg)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if _, err := DecodeGenericPayload(payload[:3]); err == nil {
		t.Fatalf("short header must fail")
	}

	bad := append([]byte(nil), payload...)
	bad[0], bad[1] = 0x00, 0x04

	if _, err := DecodeGenericPayload(bad); err == nil {
		t.Fatalf("unknown encapsulation must fail")
	}

	if _, err := DecodeGenericPayload(payload[:len(payload)-2]); err == nil {
		t.Fatalf("truncated body must fail")
	}

	empty := GenericMessage{ClassID: ClassAuthMsg}

	emptyPayload, err := EncodeGenericPayload(empty)
	if err != nil {
		t.Fatalf("encode empty: %v", err)
	}

	decoded, err := DecodeGenericPayload(emptyPayload)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}

	if len(decoded.Data) != 0 {
		t.Fatalf("empty data %d, want 0", len(decoded.Data))
	}
}

// TestSpecHandshakeTokensRoundtrip pins request reply final shapes.
func TestSpecHandshakeTokensRoundtrip(t *testing.T) {
	t.Parallel()

	var challenge [32]byte

	for i := range challenge {
		challenge[i] = byte(i)
	}

	checkHandshakeRequestRoundtrip(t, challenge)
	checkHandshakeReplyRoundtrip(t, challenge)
	checkHandshakeFinalRoundtrip(t, challenge)
}

// checkHandshakeRequestRoundtrip pins the request shape.
func checkHandshakeRequestRoundtrip(t *testing.T, challenge [32]byte) {
	t.Helper()

	req := HandshakeRequestToken{
		Challenge1:      challenge,
		DH1:             bytes.Repeat([]byte{0x04}, 65),
		KeyAgreeAlgo:    KAgreeECDH,
		SignAlgo:        DSignAlgoECDSA,
		IdentityCert:    []byte{0x30, 0x82},
		Permissions:     []byte("perms"),
		ParticipantData: EncodeParticipantData([12]byte{0x01}),
	}

	holder, err := BuildHandshakeRequest(req)
	if err != nil {
		t.Fatalf("build req: %v", err)
	}

	if len(holder.BinaryProperties) != 7 {
		t.Fatalf("req props %d, want 7", len(holder.BinaryProperties))
	}

	raw, err := EncodeDataHolder(holder)
	if err != nil {
		t.Fatalf("encode req: %v", err)
	}

	back, err := DecodeDataHolder(raw, false)
	if err != nil {
		t.Fatalf("decode req holder: %v", err)
	}

	parsed, err := ParseHandshakeRequest(back)
	if err != nil {
		t.Fatalf("parse req: %v", err)
	}

	if parsed.Challenge1 != challenge || parsed.KeyAgreeAlgo != KAgreeECDH {
		t.Fatalf("req mismatch")
	}

	hashC1, err := HashBinaryItems(RequestHashItems(parsed))
	if err != nil {
		t.Fatalf("hash c1: %v", err)
	}

	if hashC1 == ([32]byte{}) {
		t.Fatalf("hash c1 must not be zero")
	}
}

// checkHandshakeReplyRoundtrip pins the reply shape.
func checkHandshakeReplyRoundtrip(t *testing.T, challenge [32]byte) {
	t.Helper()

	reply := HandshakeReplyToken{
		Challenge1Echo:  challenge,
		Challenge2:      challenge,
		DH2:             bytes.Repeat([]byte{0x05}, 65),
		Signature:       bytes.Repeat([]byte{0x06}, 64),
		KeyAgreeAlgo:    KAgreeECDH,
		SignAlgo:        DSignAlgoECDSA,
		IdentityCert:    []byte{0x30, 0x83},
		Permissions:     []byte("perms2"),
		ParticipantData: EncodeParticipantData([12]byte{0x02}),
	}

	replyHolder, err := BuildHandshakeReply(reply)
	if err != nil {
		t.Fatalf("build reply: %v", err)
	}

	replyRaw, err := EncodeDataHolder(replyHolder)
	if err != nil {
		t.Fatalf("encode reply: %v", err)
	}

	replyBack, err := DecodeDataHolder(replyRaw, false)
	if err != nil {
		t.Fatalf("decode reply holder: %v", err)
	}

	parsedReply, err := ParseHandshakeReply(replyBack)
	if err != nil {
		t.Fatalf("parse reply: %v", err)
	}

	if parsedReply.Challenge1Echo != challenge || len(parsedReply.Signature) != 64 {
		t.Fatalf("reply mismatch")
	}
}

// checkHandshakeFinalRoundtrip pins the final shape.
func checkHandshakeFinalRoundtrip(t *testing.T, challenge [32]byte) {
	t.Helper()

	final := HandshakeFinalToken{
		Challenge1Echo: challenge,
		Challenge2Echo: challenge,
		Signature:      bytes.Repeat([]byte{0x07}, 64),
	}

	finalHolder, err := BuildHandshakeFinal(final)
	if err != nil {
		t.Fatalf("build final: %v", err)
	}

	finalRaw, err := EncodeDataHolder(finalHolder)
	if err != nil {
		t.Fatalf("encode final: %v", err)
	}

	finalBack, err := DecodeDataHolder(finalRaw, false)
	if err != nil {
		t.Fatalf("decode final holder: %v", err)
	}

	parsedFinal, err := ParseHandshakeFinal(finalBack)
	if err != nil {
		t.Fatalf("parse final: %v", err)
	}

	if parsedFinal.Challenge1Echo != challenge || parsedFinal.Challenge2Echo != challenge {
		t.Fatalf("final mismatch")
	}
}

// TestSpecHandshakeTokensReject pins fail-closed token parsing.
func TestSpecHandshakeTokensReject(t *testing.T) {
	t.Parallel()

	bad := DataHolder{ClassID: TokenReq}

	if _, err := ParseHandshakeRequest(bad); err == nil {
		t.Fatalf("empty req must fail")
	}

	missing := DataHolder{
		ClassID: TokenReq,
		BinaryProperties: []BinaryProperty{
			{Name: PropCID, Value: []byte{0x01}},
			{Name: PropCPerm, Value: nil},
			{Name: PropCPData, Value: nil},
			{Name: PropCDSignAlgo, Value: []byte(DSignAlgoECDSA)},
			{Name: PropCKAgreeAlgo, Value: []byte(KAgreeECDH)},
			{Name: PropDH1, Value: []byte{0x04}},
		},
	}

	if _, err := ParseHandshakeRequest(missing); err == nil {
		t.Fatalf("req without challenge must fail")
	}

	wrong := DataHolder{ClassID: TokenReply}

	if _, err := ParseHandshakeRequest(wrong); err == nil {
		t.Fatalf("wrong class must fail")
	}

	authReq := BuildAuthRequestToken([32]byte{0x09})

	fc, err := ParseAuthRequestToken(authReq)
	if err != nil {
		t.Fatalf("parse auth req: %v", err)
	}

	if fc != [32]byte{0x09} {
		t.Fatalf("future challenge mismatch")
	}

	badAuth := DataHolder{ClassID: TokenAuthReq}

	if _, err := ParseAuthRequestToken(badAuth); err == nil {
		t.Fatalf("auth req without challenge must fail")
	}
}

// TestSpecIdentityTokens pins identity plus permissions plus crypto tokens.
func TestSpecIdentityTokens(t *testing.T) {
	t.Parallel()

	checkIdentityRoundtrip(t)
	checkPermissionsRoundtrip(t)
	checkCryptoRoundtrip(t)
}

// checkIdentityRoundtrip pins the identity token shape.
func checkIdentityRoundtrip(t *testing.T) {
	t.Helper()

	identity := IdentityToken{
		CertSN: "CN=weld", CertAlgo: CertAlgoECPrime256v1, CaSN: "CN=ca", CaAlgo: CertAlgoECPrime256v1,
	}

	holder, err := BuildIdentityToken(identity)
	if err != nil {
		t.Fatalf("build identity: %v", err)
	}

	raw, err := EncodeDataHolder(holder)
	if err != nil {
		t.Fatalf("encode identity: %v", err)
	}

	back, err := DecodeDataHolder(raw, false)
	if err != nil {
		t.Fatalf("decode identity: %v", err)
	}

	parsed, err := ParseIdentityToken(back)
	if err != nil {
		t.Fatalf("parse identity: %v", err)
	}

	if parsed.CertSN != "CN=weld" || parsed.CaAlgo != CertAlgoECPrime256v1 {
		t.Fatalf("identity mismatch")
	}
}

// checkPermissionsRoundtrip pins the permissions token shape.
func checkPermissionsRoundtrip(t *testing.T) {
	t.Helper()

	permHolder := BuildPermissionsToken()

	permRaw, err := EncodeDataHolder(permHolder)
	if err != nil {
		t.Fatalf("encode permissions: %v", err)
	}

	permBack, err := DecodeDataHolder(permRaw, false)
	if err != nil {
		t.Fatalf("decode permissions: %v", err)
	}

	if err := ParsePermissionsToken(permBack); err != nil {
		t.Fatalf("parse permissions: %v", err)
	}
}

// checkCryptoRoundtrip pins the crypto token shape.
func checkCryptoRoundtrip(t *testing.T) {
	t.Helper()

	material := KeyMaterial{
		TransformationKind: 4,
		MasterSalt:         bytes.Repeat([]byte{0x11}, 32),
		SenderKeyID:        3,
		MasterSenderKey:    bytes.Repeat([]byte{0x12}, 32),
	}

	cryptoHolder, err := BuildCryptoToken(material)
	if err != nil {
		t.Fatalf("build crypto: %v", err)
	}

	cryptoRaw, err := EncodeDataHolder(cryptoHolder)
	if err != nil {
		t.Fatalf("encode crypto: %v", err)
	}

	cryptoBack, err := DecodeDataHolder(cryptoRaw, false)
	if err != nil {
		t.Fatalf("decode crypto: %v", err)
	}

	parsedMaterial, err := ParseCryptoToken(cryptoBack)
	if err != nil {
		t.Fatalf("parse crypto: %v", err)
	}

	if parsedMaterial.TransformationKind != 4 || parsedMaterial.SenderKeyID != 3 {
		t.Fatalf("crypto mismatch")
	}

	if len(parsedMaterial.MasterSalt) != 32 {
		t.Fatalf("crypto salt %d, want 32", len(parsedMaterial.MasterSalt))
	}
}

// TestSpecCryptoKindMapping pins local to wire transform mapping.
func TestSpecCryptoKindMapping(t *testing.T) {
	t.Parallel()

	wire, err := CryptoKindToWire(2)
	if err != nil {
		t.Fatalf("to wire: %v", err)
	}

	if wire != 4 {
		t.Fatalf("wire %d, want 4", wire)
	}

	local, err := CryptoKindFromWire(4)
	if err != nil {
		t.Fatalf("from wire: %v", err)
	}

	if local != 2 {
		t.Fatalf("local %d, want 2", local)
	}

	wire, err = CryptoKindToWire(1)
	if err != nil {
		t.Fatalf("to wire 128: %v", err)
	}

	if wire != 2 {
		t.Fatalf("wire %d, want 2", wire)
	}

	if _, err := CryptoKindToWire(9); err == nil {
		t.Fatalf("unknown local kind must fail")
	}

	if _, err := CryptoKindFromWire(3); err == nil {
		t.Fatalf("gmac only kind must fail")
	}
}

// TestSpecParticipantData pins the minimal c.pdata encoding.
func TestSpecParticipantData(t *testing.T) {
	t.Parallel()

	var prefix [12]byte

	for i := range prefix {
		prefix[i] = byte(0x80 + i)
	}

	raw := EncodeParticipantData(prefix)

	if len(raw) != 24 {
		t.Fatalf("pdata length %d, want 24", len(raw))
	}

	if raw[0] != 0x00 || raw[1] != 0x50 || raw[2] != 0x00 || raw[3] != 0x10 {
		t.Fatalf("pdata guid pid % X", raw[:4])
	}

	if !bytes.Equal(raw[4:16], prefix[:]) {
		t.Fatalf("pdata key mismatch")
	}
}
