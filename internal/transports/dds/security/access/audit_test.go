// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package access

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"
)

// auditCA generates one self-signed CA for envelope tests.
func auditCA(t *testing.T, cn string) (*x509.Certificate, crypto.Signer, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}

	return cert, key, pemEncode(t, der)
}

// auditLeaf generates one leaf certificate for envelope tests.
func auditLeaf(t *testing.T, ca *x509.Certificate, caKey crypto.Signer, cn string, serial int64, usage x509.KeyUsage) (*x509.Certificate, crypto.Signer) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     usage,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}

	return cert, key
}

// pemEncode renders DER bytes as a PEM block for trust pools.
func pemEncode(t *testing.T, der []byte) []byte {
	t.Helper()

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// auditPermissionsPayload returns a minimal parseable permissions body.
func auditPermissionsPayload() []byte {
	return []byte(`<permissions><grant name="g"><subject_name>CN=sig</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<allow_rule><topics><topic>rt/chatter</topic></topics></allow_rule>` +
		`</grant></permissions>`)
}

// TestAuditDenyOnlyGrantCoversNothing pins deny rules conferring no domain.
func TestAuditDenyOnlyGrantCoversNothing(t *testing.T) {
	t.Parallel()

	doc, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="deny-only"><subject_name>CN=x</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<deny_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics></deny_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("parse deny-only grant: %v", err)
	}

	grant := GrantForSubject(doc, "CN=x", time.Now())
	if grant == nil {
		t.Fatalf("deny-only grant must resolve for its subject")
	}

	if CoversDomain(grant, 0) {
		t.Fatalf("deny-only grant must cover no domain")
	}

	if AllowsEndpoint(grant, 0, "rt/chatter", nil, nil) {
		t.Fatalf("deny-only grant must allow no endpoint")
	}

	// A default-ALLOW grant with zero allow rules still covers nothing.
	doc, err = ParsePermissions([]byte(`<permissions>` +
		`<grant name="default-allow" default="ALLOW"><subject_name>CN=y</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<deny_rule><topics><topic>rt/other</topic></topics></deny_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("parse default-allow grant: %v", err)
	}

	grant = GrantForSubject(doc, "CN=y", time.Now())
	if grant == nil {
		t.Fatalf("default-allow grant must resolve for its subject")
	}

	if CoversDomain(grant, 0) {
		t.Fatalf("default-ALLOW with zero allow rules must cover no domain")
	}

	if AllowsEndpoint(grant, 0, "rt/chatter", nil, nil) {
		t.Fatalf("default-ALLOW with zero allow rules must allow no endpoint")
	}
}

// TestAuditDomainsMatchConjunction pins domain AND tag conjunction.
func TestAuditDomainsMatchConjunction(t *testing.T) {
	t.Parallel()

	domains := &Domains{IDs: []uint32{5}, Tags: []string{"cell"}}

	if !DomainsMatch(domains, 5, "cell") {
		t.Fatalf("matching domain plus tag must match")
	}

	if DomainsMatch(domains, 5, "other") {
		t.Fatalf("tag-scoped rule must not open an untagged join")
	}

	if DomainsMatch(domains, 7, "cell") {
		t.Fatalf("tag-scoped rule must not open an unlisted domain")
	}

	if DomainsMatch(nil, 5, "cell") {
		t.Fatalf("nil scope must match nothing")
	}

	if DomainsMatch(&Domains{}, 5, "cell") {
		t.Fatalf("empty scope must match nothing")
	}

	numberOnly := &Domains{IDs: []uint32{5}}
	if !DomainsMatch(numberOnly, 5, "anything") {
		t.Fatalf("number-only scope must match any tag")
	}

	tagOnly := &Domains{Tags: []string{"cell"}}
	if !DomainsMatch(tagOnly, 9, "cell") {
		t.Fatalf("tag-only scope must match any domain")
	}

	if DomainsMatch(tagOnly, 9, "other") {
		t.Fatalf("tag-only scope must refuse a missed tag")
	}
}

// TestAuditDeniesEndpointPartitionsTags pins four-condition deny matching.
func TestAuditDeniesEndpointPartitionsTags(t *testing.T) {
	t.Parallel()

	doc, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="g"><subject_name>CN=x</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics></allow_rule>` +
		`<deny_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics>` +
		`<partitions><partition>secret</partition></partitions>` +
		`<data_tags><tag>red</tag></data_tags></deny_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("parse scoped deny grant: %v", err)
	}

	grant := GrantForSubject(doc, "CN=x", time.Now())
	if grant == nil {
		t.Fatalf("grant must resolve for its subject")
	}

	if !DeniesEndpoint(grant, 0, "rt/chatter", []string{"secret"}, []string{"red"}) {
		t.Fatalf("fully matching deny must hit")
	}

	if DeniesEndpoint(grant, 0, "rt/chatter", []string{"other"}, []string{"red"}) {
		t.Fatalf("deny with missed partition must miss")
	}

	if DeniesEndpoint(grant, 0, "rt/chatter", []string{"secret"}, []string{"blue"}) {
		t.Fatalf("deny with missed tag must miss")
	}

	if DeniesEndpoint(grant, 0, "rt/chatter", nil, nil) {
		t.Fatalf("deny with empty endpoint scope must miss a scoped rule")
	}

	if !AllowsEndpoint(grant, 0, "rt/chatter", []string{"other"}, nil) {
		t.Fatalf("endpoint outside the deny scope must stay allowed")
	}

	if AllowsEndpoint(grant, 0, "rt/chatter", []string{"secret"}, []string{"red"}) {
		t.Fatalf("endpoint inside the deny scope must refuse")
	}
}

// TestAuditBareDatetimeRefused pins RFC 3339 as the only window format.
func TestAuditBareDatetimeRefused(t *testing.T) {
	t.Parallel()

	_, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="g"><subject_name>CN=x</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<allow_rule><topics><topic>t</topic></topics></allow_rule>` +
		`</grant></permissions>`))
	if !errors.Is(err, ErrInvalidPermissions) {
		t.Fatalf("bare datetime must fail, got %v", err)
	}

	doc, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="g"><subject_name>CN=x</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<allow_rule><topics><topic>t</topic></topics></allow_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("RFC3339 window must parse: %v", err)
	}

	if GrantForSubject(doc, "CN=x", time.Now()) == nil {
		t.Fatalf("live RFC3339 grant must resolve")
	}
}

// auditSMIMEConfig selects one crafted envelope shape.
type auditSMIMEConfig struct {
	// signerKey signs the attribute digest.
	signerKey crypto.Signer
	// sidIssuer is the DER issuer named in the signer binding.
	sidIssuer []byte
	// sidSerial is the serial named in the signer binding.
	sidSerial *big.Int
	// sigAlgo selects the signature algorithm OID on the wire.
	sigAlgo asn1.ObjectIdentifier
	// chainDERs are the envelope certificates in order.
	chainDERs [][]byte
	// crlDERs are the stapled CRLs, empty for none.
	crlDERs [][]byte
}

// buildSMIME crafts one multipart/signed envelope with a PKCS7 body.
func buildSMIME(t *testing.T, payload []byte, cfg auditSMIMEConfig) []byte {
	t.Helper()

	canonical := canonicalSMIME(payload)
	sum := sha256.Sum256(canonical)

	digestAttr := pkcs7Attribute{Type: oidMessageDigest}
	digestDER, err := asn1.Marshal(sum[:])
	if err != nil {
		t.Fatalf("marshal digest: %v", err)
	}

	digestAttr.Values = []asn1.RawValue{{FullBytes: digestDER}}

	typeAttr := pkcs7Attribute{Type: oidContentType}
	typeDER, err := asn1.Marshal(oidData)
	if err != nil {
		t.Fatalf("marshal content type: %v", err)
	}

	typeAttr.Values = []asn1.RawValue{{FullBytes: typeDER}}

	var attrBytes []byte

	for _, attr := range []pkcs7Attribute{typeAttr, digestAttr} {
		encoded, merr := asn1.Marshal(attr)
		if merr != nil {
			t.Fatalf("marshal signed attr: %v", merr)
		}

		attrBytes = append(attrBytes, encoded...)
	}

	// The verifier flips the explicit tag to SET, so the signed input is
	// the SET encoding over the concatenated attributes, while the wire
	// carries the bare attribute bytes.
	signedInput := append([]byte{0x31}, appendDERLength(nil, len(attrBytes))...)
	signedInput = append(signedInput, attrBytes...)
	setDER := attrBytes

	attrsDigest := sha256.Sum256(signedInput)
	sig, err := cfg.signerKey.Sign(rand.Reader, attrsDigest[:], crypto.SHA256)
	if err != nil {
		t.Fatalf("sign attrs: %v", err)
	}

	signerInfo := pkcs7SignerInfo{
		Version:            1,
		SID:                pkcs7IssuerSerial{Issuer: asn1.RawValue{FullBytes: cfg.sidIssuer}, Serial: asn1.RawValue{Class: 0, Tag: 2, Bytes: cfg.sidSerial.Bytes()}},
		DigestAlgorithm:    pkcs7AlgoID{Algo: oidSHA256},
		SignedAttrs:        asn1.RawValue{FullBytes: explicitTag(0, setDER)},
		SignatureAlgorithm: pkcs7AlgoID{Algo: cfg.sigAlgo},
		Signature:          sig,
	}

	signedData := pkcs7SignedData{
		Version:          1,
		DigestAlgorithms: []pkcs7AlgoID{{Algo: oidSHA256}},
		EncapContent:     pkcs7EncapContent{ContentType: oidData},
		SignerInfos:      []pkcs7SignerInfo{signerInfo},
	}

	var chainBytes []byte
	for _, der := range cfg.chainDERs {
		chainBytes = append(chainBytes, der...)
	}

	signedData.Certs = asn1.RawValue{FullBytes: explicitTag(0, chainBytes)}

	if len(cfg.crlDERs) > 0 {
		var crlBytes []byte
		for _, der := range cfg.crlDERs {
			crlBytes = append(crlBytes, der...)
		}

		signedData.CRLs = asn1.RawValue{FullBytes: explicitTag(1, crlBytes)}
	}

	sdDER, err := asn1.Marshal(signedData)
	if err != nil {
		t.Fatalf("marshal signedData: %v", err)
	}

	outer := pkcs7ContentInfo{
		ContentType: oidSignedData,
		Content:     asn1.RawValue{FullBytes: explicitTag(0, sdDER)},
	}

	outerDER, err := asn1.Marshal(outer)
	if err != nil {
		t.Fatalf("marshal contentInfo: %v", err)
	}

	const boundary = "audit-boundary"

	message := "Content-Type: multipart/signed; protocol=\"application/pkcs7-signature\"; micalg=\"sha-256\"; boundary=\"" + boundary + "\"\n"
	message += "\n--" + boundary + "\n"
	message += string(payload)
	message += "\n--" + boundary + "\n"
	message += "Content-Type: application/pkcs7-signature\nContent-Transfer-Encoding: base64\n\n"
	message += base64.StdEncoding.EncodeToString(outerDER)
	message += "\n--" + boundary + "--\n"

	return []byte(message)
}

// explicitTag wraps DER contents in one context-specific explicit tag.
func explicitTag(tag byte, contents []byte) []byte {
	out := []byte{0xA0 | tag}
	out = appendDERLength(out, len(contents))

	return append(out, contents...)
}

// appendDERLength appends one DER length prefix.
func appendDERLength(out []byte, length int) []byte {
	if length < 128 {
		return append(out, byte(length)) //nolint:gosec // G115: length below 128 by the guard above.
	}

	var raw [8]byte

	count := 0
	remaining := length

	for remaining > 0 {
		raw[count] = byte(remaining & 0xFF)
		remaining >>= 8
		count++
	}

	out = append(out, byte(0x80|count)) //nolint:gosec // G115: at most 8 length octets by construction.

	for i := count - 1; i >= 0; i-- {
		out = append(out, raw[i])
	}

	return out
}

// auditTrustPool builds a trust pool from one PEM bundle.
func auditTrustPool(t *testing.T, caPEM []byte) *x509.CertPool {
	t.Helper()

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatalf("append CA to pool")
	}

	return pool
}

// TestAuditSMIMESignerBindingExact pins exact issuer plus serial binding.
func TestAuditSMIMESignerBindingExact(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-smime-ca")
	signer, signerKey := auditLeaf(t, ca, caKey, "signer", 100, x509.KeyUsageDigitalSignature)
	other, _ := auditLeaf(t, ca, caKey, "other", 101, x509.KeyUsageDigitalSignature)

	payload := auditPermissionsPayload()
	pool := auditTrustPool(t, caPEM)

	// Positive control: exact SID binding with a verifying key passes.
	clean := buildSMIME(t, payload, auditSMIMEConfig{
		signerKey: signerKey,
		sidIssuer: signer.RawIssuer,
		sidSerial: signer.SerialNumber,
		sigAlgo:   oidECDSASHA256,
		chainDERs: [][]byte{signer.Raw, ca.Raw},
	})

	_, err := VerifyPermissionsDocument(clean, pool)
	if err != nil {
		t.Fatalf("exact signer binding must verify: %v", err)
	}

	// Mismatched signer: the SID names an unchained certificate while a
	// chained key holds a valid signature. The removed any-chained-cert
	// fallback would have accepted this; exact binding refuses it.
	stray, err := x509.ParseCertificate(other.Raw)
	if err != nil {
		t.Fatalf("parse other: %v", err)
	}

	_ = stray

	mismatched := buildSMIME(t, payload, auditSMIMEConfig{
		signerKey: signerKey,
		sidIssuer: other.RawIssuer,
		sidSerial: other.SerialNumber,
		sigAlgo:   oidECDSASHA256,
		chainDERs: [][]byte{other.Raw, signer.Raw, ca.Raw},
	})

	_, err = VerifyPermissionsDocument(mismatched, pool)
	if !errors.Is(err, ErrSignatureUnverified) {
		t.Fatalf("mismatched signer must fail, got %v", err)
	}
}

// TestAuditSMIMERSAEncryptionOIDRefused pins sha256WithRSAEncryption only.
func TestAuditSMIMERSAEncryptionOIDRefused(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-rsa-ca")

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(200),
		Subject:      pkix.Name{CommonName: "rsa-signer"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca, &rsaKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create RSA leaf: %v", err)
	}

	signer, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse RSA leaf: %v", err)
	}

	payload := auditPermissionsPayload()
	pool := auditTrustPool(t, caPEM)

	// Positive control: RSA PKCS1v15 with SHA-256 verifies.
	rsaSHA256 := asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 1, 11}
	clean := buildSMIME(t, payload, auditSMIMEConfig{
		signerKey: rsaKey,
		sidIssuer: signer.RawIssuer,
		sidSerial: signer.SerialNumber,
		sigAlgo:   rsaSHA256,
		chainDERs: [][]byte{signer.Raw, ca.Raw},
	})

	_, err = VerifyPermissionsDocument(clean, pool)
	if err != nil {
		t.Fatalf("rsa-sha256 signature must verify: %v", err)
	}

	// Generic rsaEncryption OID carries no digest binding and is refused.
	rsaEncryption := asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 1, 1}
	weak := buildSMIME(t, payload, auditSMIMEConfig{
		signerKey: rsaKey,
		sidIssuer: signer.RawIssuer,
		sidSerial: signer.SerialNumber,
		sigAlgo:   rsaEncryption,
		chainDERs: [][]byte{signer.Raw, ca.Raw},
	})

	_, err = VerifyPermissionsDocument(weak, pool)
	if !errors.Is(err, ErrSignatureUnverified) {
		t.Fatalf("rsaEncryption OID must fail, got %v", err)
	}
}

// TestAuditSMIMESignerUsageRefused pins digitalSignature on S/MIME signers.
func TestAuditSMIMESignerUsageRefused(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-usage-ca")

	// Key encipherment without digitalSignature must not sign documents.
	signer, signerKey := auditLeaf(t, ca, caKey, "no-sign", 300, x509.KeyUsageKeyEncipherment)

	payload := auditPermissionsPayload()
	pool := auditTrustPool(t, caPEM)

	envelope := buildSMIME(t, payload, auditSMIMEConfig{
		signerKey: signerKey,
		sidIssuer: signer.RawIssuer,
		sidSerial: signer.SerialNumber,
		sigAlgo:   oidECDSASHA256,
		chainDERs: [][]byte{signer.Raw, ca.Raw},
	})

	_, err := VerifyPermissionsDocument(envelope, pool)
	if !errors.Is(err, ErrSignatureUnverified) {
		t.Fatalf("signer without digitalSignature must fail, got %v", err)
	}

	if !strings.Contains(err.Error(), "digitalSignature") {
		t.Fatalf("refusal must name digitalSignature, got %v", err)
	}
}

// TestAuditDSIGSignerUsageRefused pins digitalSignature on envelope signers.
func TestAuditDSIGSignerUsageRefused(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-dsig-ca")
	signer, signerKey := auditLeaf(t, ca, caKey, "no-sign", 400, x509.KeyUsageKeyEncipherment)

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
		t.Fatalf("sign envelope: %v", err)
	}

	pool := auditTrustPool(t, caPEM)

	_, err = VerifyGovernanceDocument(signed, pool)
	if !errors.Is(err, ErrSignatureUnverified) {
		t.Fatalf("envelope signer without digitalSignature must fail, got %v", err)
	}
}

// auditCRL builds one CRL revoking the given serials, signed by caKey.
func auditCRL(t *testing.T, ca *x509.Certificate, caKey crypto.Signer, serials ...int64) []byte {
	t.Helper()

	var entries []x509.RevocationListEntry

	for _, serial := range serials {
		entries = append(entries, x509.RevocationListEntry{
			SerialNumber:   big.NewInt(serial),
			RevocationTime: time.Now(),
		})
	}

	template := &x509.RevocationList{
		Number:     big.NewInt(1),
		ThisUpdate: time.Now().Add(-time.Hour),
		NextUpdate: time.Now().Add(24 * time.Hour),
	}
	template.RevokedCertificateEntries = entries

	der, err := x509.CreateRevocationList(rand.Reader, template, ca, caKey)
	if err != nil {
		t.Fatalf("create CRL: %v", err)
	}

	return der
}

// TestAuditSMIMEStapledCRLRefused pins stapled revocation enforcement.
func TestAuditSMIMEStapledCRLRefused(t *testing.T) {
	t.Parallel()

	ca, caKey, caPEM := auditCA(t, "audit-crl-ca")
	signer, signerKey := auditLeaf(t, ca, caKey, "signer", 500, x509.KeyUsageDigitalSignature)

	payload := auditPermissionsPayload()
	pool := auditTrustPool(t, caPEM)

	revokedDER := auditCRL(t, ca, caKey, 500)

	revoked := buildSMIME(t, payload, auditSMIMEConfig{
		signerKey: signerKey,
		sidIssuer: signer.RawIssuer,
		sidSerial: signer.SerialNumber,
		sigAlgo:   oidECDSASHA256,
		chainDERs: [][]byte{signer.Raw, ca.Raw},
		crlDERs:   [][]byte{revokedDER},
	})

	_, err := VerifyPermissionsDocument(revoked, pool)
	if !errors.Is(err, ErrSignatureUnverified) {
		t.Fatalf("revoked signer must fail, got %v", err)
	}

	// A CRL that verifies against no envelope certificate fails closed.
	otherCA, otherKey, _ := auditCA(t, "audit-crl-other")
	foreignDER := auditCRL(t, otherCA, otherKey, 500)

	foreign := buildSMIME(t, payload, auditSMIMEConfig{
		signerKey: signerKey,
		sidIssuer: signer.RawIssuer,
		sidSerial: signer.SerialNumber,
		sigAlgo:   oidECDSASHA256,
		chainDERs: [][]byte{signer.Raw, ca.Raw},
		crlDERs:   [][]byte{foreignDER},
	})

	_, err = VerifyPermissionsDocument(foreign, pool)
	if !errors.Is(err, ErrSignatureUnverified) {
		t.Fatalf("unverifiable CRL must fail, got %v", err)
	}

	// The same envelope without staples verifies.
	clean := buildSMIME(t, payload, auditSMIMEConfig{
		signerKey: signerKey,
		sidIssuer: signer.RawIssuer,
		sidSerial: signer.SerialNumber,
		sigAlgo:   oidECDSASHA256,
		chainDERs: [][]byte{signer.Raw, ca.Raw},
	})

	_, err = VerifyPermissionsDocument(clean, pool)
	if err != nil {
		t.Fatalf("clean envelope must verify: %v", err)
	}
}

// TestAuditRequiresOriginAuth pins origin-kind detection per channel.
func TestAuditRequiresOriginAuth(t *testing.T) {
	t.Parallel()

	plain := &DomainRule{}
	if RequiresOriginAuth(plain, nil) {
		t.Fatal("empty rule must not require origin auth")
	}

	if RequiresOriginAuth(nil, nil) {
		t.Fatal("nil rule must not require origin auth")
	}

	signed := &DomainRule{
		DiscoveryProtection:  ProtectionSign,
		LivelinessProtection: ProtectionSign,
		RTPSProtection:       ProtectionEncrypt,
	}
	if RequiresOriginAuth(signed, nil) {
		t.Fatal("plain sign and encrypt must not require origin auth")
	}

	if RequiresOriginAuth(signed, &TopicRule{MetadataProtection: ProtectionSign, DataProtection: ProtectionEncrypt}) {
		t.Fatal("plain topic kinds must not require origin auth")
	}

	channels := []struct {
		name string
		set  func(*DomainRule, *TopicRule)
	}{
		{"discovery", func(d *DomainRule, _ *TopicRule) { d.DiscoveryProtection = ProtectionSignWithOrigin }},
		{"liveliness", func(d *DomainRule, _ *TopicRule) { d.LivelinessProtection = ProtectionEncryptWithOrigin }},
		{"rtps", func(d *DomainRule, _ *TopicRule) { d.RTPSProtection = ProtectionSignWithOrigin }},
		{"metadata", func(_ *DomainRule, tr *TopicRule) { tr.MetadataProtection = ProtectionEncryptWithOrigin }},
		{"data", func(_ *DomainRule, tr *TopicRule) { tr.DataProtection = ProtectionSignWithOrigin }},
	}

	for _, channel := range channels {
		rule := &DomainRule{}
		topic := &TopicRule{}
		channel.set(rule, topic)

		if !RequiresOriginAuth(rule, topic) {
			t.Fatalf("%s origin kind must require origin auth", channel.name)
		}
	}
}

// TestAuditDirectionPreservationacc8 pins publish versus subscribe separation.
func TestAuditDirectionPreservationacc8(t *testing.T) {
	t.Parallel()

	doc, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="pub-only"><subject_name>CN=x</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains>` +
		`<publish><topics><topic>rt/chatter</topic></topics></publish>` +
		`</allow_rule></grant></permissions>`))
	if err != nil {
		t.Fatalf("parse pub-only grant: %v", err)
	}

	grant := GrantForSubject(doc, "CN=x", time.Now())
	if grant == nil {
		t.Fatalf("pub-only grant must resolve")
	}

	ok, _ := AllowsPublishacc8(grant, 0, "rt/chatter", nil, nil)
	if !ok {
		t.Fatalf("publish-only grant must allow publish")
	}

	ok, _ = AllowsSubscribeacc8(grant, 0, "rt/chatter", nil, nil)
	if ok {
		t.Fatalf("publish-only grant must deny subscribe")
	}

	ok, _ = AllowsRelayacc8(grant, 0, "rt/chatter", nil, nil)
	if ok {
		t.Fatalf("publish-only grant must deny relay")
	}

	doc, err = ParsePermissions([]byte(`<permissions>` +
		`<grant name="sub-only"><subject_name>CN=y</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains>` +
		`<subscribe><topics><topic>rt/chatter</topic></topics></subscribe>` +
		`</allow_rule></grant></permissions>`))
	if err != nil {
		t.Fatalf("parse sub-only grant: %v", err)
	}

	grant = GrantForSubject(doc, "CN=y", time.Now())
	if grant == nil {
		t.Fatalf("sub-only grant must resolve")
	}

	ok, _ = AllowsSubscribeacc8(grant, 0, "rt/chatter", nil, nil)
	if !ok {
		t.Fatalf("subscribe-only grant must allow subscribe")
	}

	ok, _ = AllowsPublishacc8(grant, 0, "rt/chatter", nil, nil)
	if ok {
		t.Fatalf("subscribe-only grant must deny publish")
	}
}

// TestAuditDefaultAllowScopingacc8 pins topic-level scoping for default ALLOW.
func TestAuditDefaultAllowScopingacc8(t *testing.T) {
	t.Parallel()

	doc, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="default-allow" default="ALLOW"><subject_name>CN=x</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/allowed</topic></topics></allow_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("parse default-allow grant: %v", err)
	}

	grant := GrantForSubject(doc, "CN=x", time.Now())
	if grant == nil {
		t.Fatalf("default-allow grant must resolve")
	}

	if !AllowsEndpoint(grant, 0, "rt/allowed", nil, nil) {
		t.Fatalf("covered topic must allow")
	}

	if AllowsEndpoint(grant, 0, "rt/other", nil, nil) {
		t.Fatalf("default-ALLOW with single-topic allow must deny other topics in a covered domain")
	}

	ok, _ := AllowsPublishacc8(grant, 0, "rt/other", nil, nil)
	if ok {
		t.Fatalf("directional default-ALLOW must deny uncovered topic")
	}
}

// TestAuditTagMediationSingleRuleacc8 pins same-rule tag mediation.
func TestAuditTagMediationSingleRuleacc8(t *testing.T) {
	t.Parallel()

	doc, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="g"><subject_name>CN=x</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics>` +
		`<data_tags><tag>blue</tag></data_tags></allow_rule>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/chatter</topic></topics>` +
		`<data_tags><tag>red</tag></data_tags></allow_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("parse two-rule grant: %v", err)
	}

	grant := GrantForSubject(doc, "CN=x", time.Now())
	if grant == nil {
		t.Fatalf("grant must resolve")
	}

	ok, tags := AllowsPublishacc8(grant, 0, "rt/chatter", nil, []string{"red"})
	if !ok {
		t.Fatalf("matching second rule must allow")
	}

	found := false
	for _, tag := range tags {
		if tag == "red" {
			found = true
		}
	}

	if !found {
		t.Fatalf("mediated tags %v must come from matching second rule, want red", tags)
	}

	ok, _ = AllowsPublishacc8(grant, 0, "rt/chatter", nil, []string{"green"})
	if ok {
		t.Fatalf("mismatched tags must deny")
	}

	ok, _ = AllowsPublishacc8(grant, 0, "rt/chatter", nil, nil)
	if ok {
		t.Fatalf("deny-scoped tags must deny nil scope")
	}
}
