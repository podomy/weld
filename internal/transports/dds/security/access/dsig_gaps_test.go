// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package access

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/xml"
	"math/big"
	"strings"
	"testing"
	"time"
)

// certForRSAKey builds one self-signed RSA cert for verify tests.
func certForRSAKey(t *testing.T, key *rsa.PrivateKey) *x509.Certificate {
	t.Helper()

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "pss-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create RSA cert: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse RSA cert: %v", err)
	}

	return cert
}

// TestGapsPrefixedSignatureacc9 pins ds: prefix acceptance.
func TestGapsPrefixedSignatureacc9(t *testing.T) {
	t.Parallel()

	ca, caKey, _ := auditCA(t, "gaps-dsig-ca")
	signer, signerKey := auditLeaf(t, ca, caKey, "signer", 600, x509.KeyUsageDigitalSignature)

	// SignEnvelope uses ECDSA by default when the key is ECDSA.
	payload := []byte(`<dds><domain_access_rules><domain_rule><domains><id>0</id></domains>` +
		`<allow_unauthenticated_participants>false</allow_unauthenticated_participants>` +
		`<enable_join_access_control>true</enable_join_access_control>` +
		`<discovery_protection_kind>SIGN</discovery_protection_kind>` +
		`<liveliness_protection_kind>SIGN</liveliness_protection_kind>` +
		`<rtps_protection_kind>SIGN</rtps_protection_kind>` +
		`<topic_access_rules><topic_rule><topic_expression>t</topic_expression>` +
		`<enable_discovery_protection>false</enable_discovery_protection>` +
		`<enable_liveliness_protection>false</enable_liveliness_protection>` +
		`<enable_read_access_control>false</enable_read_access_control>` +
		`<enable_write_access_control>false</enable_write_access_control>` +
		`<metadata_protection_kind>NONE</metadata_protection_kind>` +
		`<data_protection_kind>NONE</data_protection_kind>` +
		`</topic_rule></topic_access_rules></domain_rule></domain_access_rules></dds>`)

	signed, err := SignEnvelope(payload, signer, signerKey)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	prefixed := bytes.Replace(signed, []byte("<Signature"), []byte("<ds:Signature"), 1)
	prefixed = bytes.Replace(prefixed, []byte("</Signature>"), []byte("</ds:Signature>"), 1)

	outPayload, raw, err := splitEnvelope(prefixed)
	if err != nil {
		t.Fatalf("prefixed split must pass, got %v", err)
	}

	if len(outPayload) == 0 || len(raw) == 0 {
		t.Fatalf("prefixed split must return parts")
	}

	if _, err := parseEnvelope(raw); err != nil {
		t.Fatalf("prefixed envelope must parse, got %v", err)
	}
}

// TestGapsPSSVerifyacc9 pins RSA-PSS verification paths.
func TestGapsPSSVerifyacc9(t *testing.T) {
	t.Parallel()

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA: %v", err)
	}

	digest := sha256.Sum256([]byte("pss-test"))
	sig, err := rsa.SignPSS(rand.Reader, rsaKey, crypto.SHA256, digest[:], &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthAuto, Hash: crypto.SHA256})
	if err != nil {
		t.Fatalf("sign PSS: %v", err)
	}

	cert := certForRSAKey(t, rsaKey)
	if err := verifyWithCert(cert, DSIGSigRSAPSSSHA256, crypto.SHA256, digest[:], sig); err != nil {
		t.Fatalf("PSS-SHA256 must verify: %v", err)
	}

	digest512 := sha512.Sum512([]byte("pss512-test"))
	sig512, err := rsa.SignPSS(rand.Reader, rsaKey, crypto.SHA512, digest512[:], &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthAuto, Hash: crypto.SHA512})
	if err != nil {
		t.Fatalf("sign PSS512: %v", err)
	}

	if err := verifyWithCert(cert, DSIGSigRSAPSSSHA512, crypto.SHA512, digest512[:], sig512); err != nil {
		t.Fatalf("PSS-SHA512 must verify: %v", err)
	}
}

// TestGapsSHA512Digestacc9 pins SHA-512 payload digests.
func TestGapsSHA512Digestacc9(t *testing.T) {
	t.Parallel()

	payload := []byte("sha512-payload")
	sum := sha512.Sum512(payload)

	if err := comparePayloadDigest(payload, DSIGDigestSHA512, sum[:]); err != nil {
		t.Fatalf("SHA-512 digest must match: %v", err)
	}

	bad := sha512.Sum512([]byte("other"))
	if err := comparePayloadDigest(payload, DSIGDigestSHA512, bad[:]); err == nil {
		t.Fatalf("mismatched SHA-512 must fail")
	}
}

// TestGapsC14NWaivedacc9 pins standard C14N refusal as waived.
func TestGapsC14NWaivedacc9(t *testing.T) {
	t.Parallel()

	raw := []byte(`<Signature><SignedInfo>` +
		`<CanonicalizationMethod Algorithm="http://www.w3.org/TR/2001/REC-xml-c14n-20010315"/>` +
		`<SignatureMethod Algorithm="` + DSIGSigRSASHA256 + `"/>` +
		`<Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/>` +
		`<DigestValue>AAAA</DigestValue></Reference></SignedInfo>` +
		`<SignatureValue>AAAA</SignatureValue>` +
		`<KeyInfo><X509Data><X509Certificate>AAAA</X509Certificate></X509Data></KeyInfo></Signature>`)

	_, err := parseEnvelope(raw)
	if err == nil {
		t.Fatalf("standard C14N must refuse")
	}

	if !strings.Contains(err.Error(), "waived") {
		t.Fatalf("C14N refusal must name waiver, got %v", err)
	}
}

// TestGapsMultiRefCountacc9 pins multi-reference detection.
func TestGapsMultiRefCountacc9(t *testing.T) {
	t.Parallel()

	twoRefs := []byte(`<Signature><SignedInfo>` +
		`<CanonicalizationMethod Algorithm="` + DSIGC14NExciseBytes + `"/>` +
		`<SignatureMethod Algorithm="` + DSIGSigRSASHA256 + `"/>` +
		`<Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>AAAA</DigestValue></Reference>` +
		`<Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>BBBB</DigestValue></Reference>` +
		`</SignedInfo></Signature>`)

	extras := extraReferences(twoRefs)
	if len(extras) != 1 {
		t.Fatalf("two references must yield one extra, got %d", len(extras))
	}
}

// TestGapsMultiRefDigestsVerifyacc9 pins every reference digest verifies.
func TestGapsMultiRefDigestsVerifyacc9(t *testing.T) {
	t.Parallel()

	payload := []byte(`<dds><data>multi-ref</data></dds>`)
	sum := sha256.Sum256(payload)
	goodB64 := base64.StdEncoding.EncodeToString(sum[:])

	badSum := sha256.Sum256([]byte("tampered"))
	badB64 := base64.StdEncoding.EncodeToString(badSum[:])

	goodRaw := []byte(`<Signature><SignedInfo>` +
		`<CanonicalizationMethod Algorithm="` + DSIGC14NExciseBytes + `"/>` +
		`<SignatureMethod Algorithm="` + DSIGSigRSASHA256 + `"/>` +
		`<Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>` + goodB64 + `</DigestValue></Reference>` +
		`<Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>` + goodB64 + `</DigestValue></Reference>` +
		`</SignedInfo><SignatureValue>AAAA</SignatureValue>` +
		`<KeyInfo><X509Data><X509Certificate>AAAA</X509Certificate></X509Data></KeyInfo></Signature>`)

	var goodSig envelopeSignature
	if err := xml.Unmarshal(goodRaw, &goodSig); err != nil {
		t.Fatalf("unmarshal good: %v", err)
	}

	if err := verifyPayloadDigests(payload, goodSig, goodRaw); err != nil {
		t.Fatalf("valid two-ref envelope must verify, got %v", err)
	}

	badRaw := []byte(`<Signature><SignedInfo>` +
		`<CanonicalizationMethod Algorithm="` + DSIGC14NExciseBytes + `"/>` +
		`<SignatureMethod Algorithm="` + DSIGSigRSASHA256 + `"/>` +
		`<Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>` + goodB64 + `</DigestValue></Reference>` +
		`<Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>` + badB64 + `</DigestValue></Reference>` +
		`</SignedInfo><SignatureValue>AAAA</SignatureValue>` +
		`<KeyInfo><X509Data><X509Certificate>AAAA</X509Certificate></X509Data></KeyInfo></Signature>`)

	var badSig envelopeSignature
	if err := xml.Unmarshal(badRaw, &badSig); err != nil {
		t.Fatalf("unmarshal bad: %v", err)
	}

	if err := verifyPayloadDigests(payload, badSig, badRaw); err == nil {
		t.Fatalf("tampered second reference must fail")
	}
}

// TestGapsMultiRefFragmentRefusesacc9 pins non-empty URIs refuse fail-closed.
func TestGapsMultiRefFragmentRefusesacc9(t *testing.T) {
	t.Parallel()

	fragRaw := []byte(`<Signature><SignedInfo>` +
		`<CanonicalizationMethod Algorithm="` + DSIGC14NExciseBytes + `"/>` +
		`<SignatureMethod Algorithm="` + DSIGSigRSASHA256 + `"/>` +
		`<Reference URI=""><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>AAAA</DigestValue></Reference>` +
		`<Reference URI="#frag"><Transforms><Transform Algorithm="` + DSIGTransformEnveloped + `"/></Transforms>` +
		`<DigestMethod Algorithm="` + DSIGDigestSHA256 + `"/><DigestValue>AAAA</DigestValue></Reference>` +
		`</SignedInfo></Signature>`)

	if err := checkExtraDigests(fragRaw); err == nil {
		t.Fatalf("fragment extra reference must refuse")
	}
}
