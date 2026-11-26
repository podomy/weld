// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package access

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"testing"
)

// TestSecPrefixedMultiRefTamperFails pins every prefixed digest verifying.
func TestSecPrefixedMultiRefTamperFails(t *testing.T) {
	t.Parallel()

	payload := []byte(`<dds><data>prefixed-multi</data></dds>`)
	sum := sha256.Sum256(payload)
	goodB64 := base64.StdEncoding.EncodeToString(sum[:])

	badSum := sha256.Sum256([]byte("tampered"))
	badB64 := base64.StdEncoding.EncodeToString(badSum[:])

	goodRaw := []byte(`<ds:Signature><ds:SignedInfo>` +
		`<CanonicalizationMethod Algorithm="` + DSIGC14NExciseBytes + `"/>` +
		`<SignatureMethod Algorithm="` + DSIGSigRSASHA256 + `"/>` +
		`<ds:Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>` + goodB64 + `</DigestValue></ds:Reference>` +
		`<ds:Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>` + goodB64 + `</DigestValue></ds:Reference>` +
		`</ds:SignedInfo><SignatureValue>AAAA</SignatureValue>` +
		`<KeyInfo><X509Data><X509Certificate>AAAA</X509Certificate></X509Data></KeyInfo></ds:Signature>`)

	if got := countReferences(goodRaw); got != 2 {
		t.Fatalf("prefixed count must be 2, got %d", got)
	}

	var goodSig envelopeSignature
	if err := xml.Unmarshal(goodRaw, &goodSig); err != nil {
		t.Fatalf("unmarshal prefixed good: %v", err)
	}

	if err := verifyPayloadDigests(payload, goodSig, goodRaw); err != nil {
		t.Fatalf("valid prefixed two-ref must verify, got %v", err)
	}

	badRaw := []byte(`<ds:Signature><ds:SignedInfo>` +
		`<CanonicalizationMethod Algorithm="` + DSIGC14NExciseBytes + `"/>` +
		`<SignatureMethod Algorithm="` + DSIGSigRSASHA256 + `"/>` +
		`<ds:Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>` + goodB64 + `</DigestValue></ds:Reference>` +
		`<ds:Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>` + badB64 + `</DigestValue></ds:Reference>` +
		`</ds:SignedInfo><SignatureValue>AAAA</SignatureValue>` +
		`<KeyInfo><X509Data><X509Certificate>AAAA</X509Certificate></X509Data></KeyInfo></ds:Signature>`)

	var badSig envelopeSignature
	if err := xml.Unmarshal(badRaw, &badSig); err != nil {
		t.Fatalf("unmarshal prefixed bad: %v", err)
	}

	if err := verifyPayloadDigests(payload, badSig, badRaw); err == nil {
		t.Fatalf("tampered prefixed second reference must fail")
	}
}

// TestSecUnparseableMultiRefFails pins nil extras failing closed.
//
// A "<Reference" substring inside a comment counts toward the raw total
// but parses to zero extras; verification must fail closed instead of
// verifying only the first digest.
func TestSecUnparseableMultiRefFails(t *testing.T) {
	t.Parallel()

	payload := []byte(`<dds><data>unparseable</data></dds>`)
	sum := sha256.Sum256(payload)
	goodB64 := base64.StdEncoding.EncodeToString(sum[:])

	raw := []byte(`<Signature><SignedInfo>` +
		`<CanonicalizationMethod Algorithm="` + DSIGC14NExciseBytes + `"/>` +
		`<SignatureMethod Algorithm="` + DSIGSigRSASHA256 + `"/>` +
		`<Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>` + goodB64 + `</DigestValue></Reference>` +
		`<!-- <Reference -->` +
		`</SignedInfo><SignatureValue>AAAA</SignatureValue>` +
		`<KeyInfo><X509Data><X509Certificate>AAAA</X509Certificate></X509Data></KeyInfo></Signature>`)

	if got := countReferences(raw); got != 2 {
		t.Fatalf("unparseable count must be 2, got %d", got)
	}

	if extras := extraReferences(raw); len(extras) != 0 {
		t.Fatalf("comment reference must parse to zero extras, got %d", len(extras))
	}

	var sig envelopeSignature
	if err := xml.Unmarshal(raw, &sig); err != nil {
		t.Fatalf("unmarshal first ref: %v", err)
	}

	if err := verifyPayloadDigests(payload, sig, raw); err == nil {
		t.Fatalf("unparseable multi-reference must fail closed")
	}
}
