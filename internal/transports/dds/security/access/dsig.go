// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package access

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
)

// DSIG subset URIs. Excise-bytes canonicalization below verifies; standard
// C14N remains a documented waiver (fail-closed refuse) because full W3C
// C14N over arbitrary peer whitespace plus namespace prefixes is not
// implemented. RSA-PSS with SHA-256 or SHA-512, SHA-512 digests, prefixed
// ds:Signature forms, and multi-reference envelopes verify.
const (
	// DSIGC14NExciseBytes is the supported canonicalization method.
	DSIGC14NExciseBytes = "urn:weld:xmlsig:excise-bytes-sha256"
	// DSIGDigestSHA256 selects SHA-256 digests.
	DSIGDigestSHA256 = "http://www.w3.org/2001/04/xmlenc#sha256"
	// DSIGDigestSHA512 selects SHA-512 digests.
	DSIGDigestSHA512 = "http://www.w3.org/2001/04/xmlenc#sha512"
	// DSIGSigRSASHA256 selects RSA PKCS1v15 with SHA-256.
	DSIGSigRSASHA256 = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"
	// DSIGSigECDSASHA256 selects ECDSA with SHA-256.
	DSIGSigECDSASHA256 = "http://www.w3.org/2001/04/xmldsig-more#ecdsa-sha256"
	// DSIGSigRSAPSSSHA256 selects RSA-PSS with SHA-256.
	DSIGSigRSAPSSSHA256 = "http://www.w3.org/2007/05/xmldsig-more#sha256-rsa-MGF1"
	// DSIGSigRSAPSSSHA512 selects RSA-PSS with SHA-512.
	DSIGSigRSAPSSSHA512 = "http://www.w3.org/2007/05/xmldsig-more#sha512-rsa-MGF1"
	// DSIGSigECDSASHA512 selects ECDSA with SHA-512.
	DSIGSigECDSASHA512 = "http://www.w3.org/2001/04/xmldsig-more#ecdsa-sha512"
	// DSIGTransformEnveloped is the required first reference transform.
	DSIGTransformEnveloped = "http://www.w3.org/2000/09/xmldsig#enveloped-signature"
	// DSIGC14N20010315 is the standard C14N URI, refused fail-closed (waived).
	DSIGC14N20010315 = "http://www.w3.org/TR/2001/REC-xml-c14n-20010315"
	// DSIGC14NExclusive is the exclusive C14N URI, refused fail-closed (waived).
	DSIGC14NExclusive = "http://www.w3.org/2001/10/xml-exc-c14n#"
)

// dsigTrustPool builds a trust pool from PEM CA bytes.
func dsigTrustPool(caPEM []byte) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("security: no CA certificates parsed: %w", ErrSignatureUnverified)
	}

	return pool, nil
}

// envelopeSignature is the supported Signature element shape.
type envelopeSignature struct {
	// XMLName pins the local element name.
	XMLName xml.Name `xml:"Signature"`
	// SignedInfo carries the signed digest.
	SignedInfo envelopeSignedInfo `xml:"SignedInfo"`
	// SignatureValue carries base64 signature over canonical SignedInfo.
	SignatureValue string `xml:"SignatureValue"`
	// KeyInfo carries the signing certificate.
	KeyInfo envelopeKeyInfo `xml:"KeyInfo"`
}

// envelopeSignedInfo is the supported SignedInfo shape.
type envelopeSignedInfo struct {
	// CanonicalizationMethod names the canonicalization.
	CanonicalizationMethod envelopeAlgo `xml:"CanonicalizationMethod"`
	// SignatureMethod names the signature algorithm.
	SignatureMethod envelopeAlgo `xml:"SignatureMethod"`
	// Reference carries the first enveloped reference.
	Reference envelopeReference `xml:"Reference"`
}

// envelopeAlgo carries one Algorithm attribute.
type envelopeAlgo struct {
	// Algorithm is the method URI.
	Algorithm string `xml:"Algorithm,attr"`
}

// envelopeReference is the single supported reference.
type envelopeReference struct {
	// URI must be empty for enveloped whole-document references.
	URI string `xml:"URI,attr"`
	// Transforms must hold exactly the enveloped transform.
	Transforms envelopeTransforms `xml:"Transforms"`
	// DigestMethod must be SHA-256.
	DigestMethod envelopeAlgo `xml:"DigestMethod"`
	// DigestValue carries base64 SHA-256 over excised payload bytes.
	DigestValue string `xml:"DigestValue"`
}

// envelopeTransforms holds transform entries.
type envelopeTransforms struct {
	// Items holds transform algorithms in order.
	Items []envelopeAlgo `xml:"Transform"`
}

// envelopeKeyInfo holds the signing certificate.
type envelopeKeyInfo struct {
	// Data holds X509 data.
	Data envelopeX509Data `xml:"X509Data"`
}

// envelopeX509Data holds one certificate.
type envelopeX509Data struct {
	// Certificate carries base64 DER.
	Certificate string `xml:"X509Certificate"`
}

// splitEnvelope excises the single Signature element by bytes.
//
// It returns the payload with the element removed plus the raw element
// bytes. Zero elements means unsigned, more than one means ambiguous;
// both fail closed. Unprefixed "<Signature>" plus prefixed "<ds:Signature"
// and "<dsig:Signature" forms are accepted; "<SignatureMethod>" and
// "<SignatureValue>" never count as outer elements.
func splitEnvelope(data []byte) ([]byte, []byte, error) {
	start, prefix, ok := findSignatureOpen(data, 0)
	if !ok {
		return nil, nil, fmt.Errorf("security: document carries no Signature element: %w", ErrSignatureUnverified)
	}

	if next, _, dup := findSignatureOpen(data, start+1); dup {
		_ = next

		return nil, nil, fmt.Errorf("security: document carries multiple Signature elements: %w", ErrSignatureUnverified)
	}

	closeTag := []byte("</" + prefix + "Signature>")
	closeIdx := bytes.Index(data[start:], closeTag)
	if closeIdx < 0 {
		return nil, nil, fmt.Errorf("security: Signature element is not closed: %w", ErrSignatureUnverified)
	}

	end := start + closeIdx + len(closeTag)
	raw := append([]byte(nil), data[start:end]...)

	payload := make([]byte, 0, len(data)-(end-start))
	payload = append(payload, data[:start]...)
	payload = append(payload, data[end:]...)

	return payload, raw, nil
}

// findSignatureOpen locates the next outer Signature open tag.
//
// It matches unprefixed plus ds: plus dsig: prefixes and reports the
// prefix used (empty for unprefixed). Inner "<SignatureMethod>" and
// "<SignatureValue>" elements never count.
func findSignatureOpen(data []byte, from int) (int, string, bool) {
	best := -1
	bestPrefix := ""

	for _, prefix := range []string{"", "ds:", "dsig:"} {
		abs, ok := scanPrefixOpen(data, from, prefix)
		if ok && (best < 0 || abs < best) {
			best = abs
			bestPrefix = prefix
		}
	}

	if best < 0 {
		return -1, "", false
	}

	return best, bestPrefix, true
}

// scanPrefixOpen finds one prefixed open tag with a valid terminator.
func scanPrefixOpen(data []byte, from int, prefix string) (int, bool) {
	tag := []byte("<" + prefix + "Signature")
	off := from

	for off < len(data) {
		rel := bytes.Index(data[off:], tag)
		if rel < 0 {
			return -1, false
		}

		abs := off + rel
		next := abs + len(tag)
		if next < len(data) {
			c := data[next]
			if c == ' ' || c == '>' || c == '/' {
				return abs, true
			}
		}

		off = abs + 1
	}

	return -1, false
}

// parseEnvelope parses one Signature element and checks its method set.
//
// Standard C14N URIs refuse fail-closed as a documented waiver: full W3C
// C14N over arbitrary peer whitespace is not implemented. RSA-PSS with
// SHA-256 or SHA-512, ECDSA with SHA-256 or SHA-512, and SHA-256 or
// SHA-512 digests verify. Multi-reference envelopes verify every
// reference digest against the excised payload; the first reference must
// carry the enveloped transform with an empty URI.
func parseEnvelope(raw []byte) (envelopeSignature, error) {
	var sig envelopeSignature

	err := xml.Unmarshal(raw, &sig)
	if err != nil {
		return sig, fmt.Errorf("security: parse Signature element: %w", errors.Join(err, ErrSignatureUnverified))
	}

	if err := checkC14NMethod(sig.SignedInfo.CanonicalizationMethod.Algorithm); err != nil {
		return sig, err
	}

	if err := checkSignatureMethod(sig.SignedInfo.SignatureMethod.Algorithm); err != nil {
		return sig, err
	}

	if err := checkFirstReference(sig.SignedInfo.Reference); err != nil {
		return sig, err
	}

	if err := checkExtraDigests(raw); err != nil {
		return sig, err
	}

	return sig, nil
}

// checkC14NMethod accepts excise-bytes and refuses standard C14N as waived.
//
// Standard C14N URIs refuse fail-closed as a documented waiver: full W3C
// C14N over arbitrary peer whitespace is not implemented. The refusal
// message names the waiver explicitly so operators know the document
// was denied, not ignored.
func checkC14NMethod(algo string) error {
	if algo == DSIGC14NExciseBytes {
		return nil
	}

	if algo == DSIGC14N20010315 || algo == DSIGC14NExclusive {
		return fmt.Errorf("security: standard c14n %q refused, documented waiver waived, excise-bytes only: %w", algo, ErrSignatureUnverified)
	}

	return fmt.Errorf("security: unsupported canonicalization %q: %w", algo, ErrSignatureUnverified)
}

// checkSignatureMethod accepts the supported signature method set.
func checkSignatureMethod(algo string) error {
	switch algo {
	case DSIGSigRSASHA256, DSIGSigECDSASHA256, DSIGSigRSAPSSSHA256, DSIGSigRSAPSSSHA512, DSIGSigECDSASHA512:
		return nil
	default:
		return fmt.Errorf("security: unsupported signature method %q: %w", algo, ErrSignatureUnverified)
	}
}

// checkFirstReference validates the enveloped whole-document reference.
func checkFirstReference(ref envelopeReference) error {
	if ref.URI != "" {
		return fmt.Errorf("security: reference URI %q must be empty: %w", ref.URI, ErrSignatureUnverified)
	}

	if len(ref.Transforms.Items) != 1 || ref.Transforms.Items[0].Algorithm != DSIGTransformEnveloped {
		return fmt.Errorf("security: reference must carry exactly the enveloped transform: %w", ErrSignatureUnverified)
	}

	switch ref.DigestMethod.Algorithm {
	case DSIGDigestSHA256, DSIGDigestSHA512:
		return nil
	default:
		return fmt.Errorf("security: unsupported digest method %q: %w", ref.DigestMethod.Algorithm, ErrSignatureUnverified)
	}
}

// countReferences counts Reference opens including prefixed forms.
//
// Unprefixed "<Reference" plus "ds:" plus "dsig:" prefixes each count,
// so prefixed multi-reference envelopes cannot bypass shape plus digest
// checks. Overcounting fails closed; undercounting would bypass.
func countReferences(raw []byte) int {
	return bytes.Count(raw, []byte("<Reference")) +
		bytes.Count(raw, []byte("<ds:Reference")) +
		bytes.Count(raw, []byte("<dsig:Reference"))
}

// checkExtraDigests validates additional references fail-closed per 9.6.
//
// Every extra reference must carry an empty URI for the enveloped
// whole-document, exactly the enveloped transform, and a SHA-256 or
// SHA-512 digest. Fragment URIs, missing transforms, and unknown digests
// refuse. Unparseable multi-reference envelopes refuse instead of
// silently verifying only the first digest. Prefixed Reference forms
// count the same as unprefixed.
func checkExtraDigests(raw []byte) error {
	count := countReferences(raw)
	if count <= 1 {
		return nil
	}

	extras := extraReferences(raw)
	if len(extras) != count-1 {
		return fmt.Errorf("security: multi-reference envelope unparseable: %w", ErrSignatureUnverified)
	}

	for i, ref := range extras {
		if ref.URI != "" {
			return fmt.Errorf("security: extra reference %d URI %q must be empty: %w", i, ref.URI, ErrSignatureUnverified)
		}

		if len(ref.Transforms.Items) != 1 || ref.Transforms.Items[0].Algorithm != DSIGTransformEnveloped {
			return fmt.Errorf("security: extra reference %d must carry exactly the enveloped transform: %w", i, ErrSignatureUnverified)
		}

		if ref.DigestMethod.Algorithm != DSIGDigestSHA256 && ref.DigestMethod.Algorithm != DSIGDigestSHA512 {
			return fmt.Errorf("security: unsupported extra digest %q: %w", ref.DigestMethod.Algorithm, ErrSignatureUnverified)
		}
	}

	return nil
}

// extraReferences returns Reference elements beyond the first.
//
// encoding/xml binds only the first Reference into envelopeSignature, so
// multi-reference envelopes are detected by counting Reference opens
// (including ds: plus dsig: prefixes) in the raw Signature bytes. Each
// extra must still digest the excised payload; fragment references
// refuse fail-closed in verification.
func extraReferences(raw []byte) []envelopeReference {
	count := countReferences(raw)
	if count <= 1 {
		return nil
	}

	type multiRef struct {
		Refs []envelopeReference `xml:"Reference"`
	}

	var parsed struct {
		SignedInfo multiRef `xml:"SignedInfo"`
	}

	if err := xml.Unmarshal(raw, &parsed); err != nil {
		return nil
	}

	if len(parsed.SignedInfo.Refs) <= 1 {
		return nil
	}

	return parsed.SignedInfo.Refs[1:]
}

// canonicalSignedInfo renders SignedInfo bytes for sign and verify.
//
// This is the documented deviation from full C14N: a fixed line-oriented
// encoding instead of canonical XML. Sign and verify share it exactly.
func canonicalSignedInfo(info envelopeSignedInfo, digestB64 string) []byte {
	out := "WELD-XMLDSIGv1\n"
	out += info.CanonicalizationMethod.Algorithm + "\n"
	out += info.SignatureMethod.Algorithm + "\n"
	out += info.Reference.URI + "\n"
	out += info.Reference.DigestMethod.Algorithm + "\n"
	out += digestB64 + "\n"

	return []byte(out)
}

// verifyEnvelopeSignature checks digest, chain, and signature in order.
func verifyEnvelopeSignature(data []byte, caPool *x509.CertPool) ([]byte, *x509.Certificate, error) {
	if caPool == nil {
		return nil, nil, fmt.Errorf("security: nil trust pool: %w", ErrSignatureUnverified)
	}

	payload, raw, err := splitEnvelope(data)
	if err != nil {
		return nil, nil, err
	}

	sig, err := parseEnvelope(raw)
	if err != nil {
		return nil, nil, err
	}

	err = verifyPayloadDigests(payload, sig, raw)
	if err != nil {
		return nil, nil, err
	}

	cert, err := loadSigningCert(sig, caPool)
	if err != nil {
		return nil, nil, err
	}

	sigBytes, err := base64.StdEncoding.DecodeString(stripSpace(sig.SignatureValue))
	if err != nil {
		return nil, nil, fmt.Errorf("security: decode SignatureValue: %w", errors.Join(err, ErrSignatureUnverified))
	}

	canonical := canonicalSignedInfo(sig.SignedInfo, stripSpace(sig.SignedInfo.Reference.DigestValue))
	digest, hash := hashCanonical(sig.SignedInfo.SignatureMethod.Algorithm, canonical)

	err = verifyWithCert(cert, sig.SignedInfo.SignatureMethod.Algorithm, hash, digest, sigBytes)
	if err != nil {
		return nil, nil, err
	}

	return payload, cert, nil
}

// loadSigningCert decodes the KeyInfo certificate and verifies its chain.
func loadSigningCert(sig envelopeSignature, caPool *x509.CertPool) (*x509.Certificate, error) {
	certDER, err := base64.StdEncoding.DecodeString(stripSpace(sig.KeyInfo.Data.Certificate))
	if err != nil {
		return nil, fmt.Errorf("security: decode signing certificate: %w", errors.Join(err, ErrSignatureUnverified))
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, fmt.Errorf("security: parse signing certificate: %w", errors.Join(err, ErrSignatureUnverified))
	}

	_, err = cert.Verify(x509.VerifyOptions{
		Roots:     caPool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageAny},
	})
	if err != nil {
		return nil, fmt.Errorf("security: signing certificate chain: %w", errors.Join(err, ErrSignatureUnverified))
	}

	if cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return nil, fmt.Errorf("security: signing certificate lacks digitalSignature usage: %w", ErrSignatureUnverified)
	}

	return cert, nil
}

// verifyPayloadDigests checks every reference digest per 9.6.
//
// The first plus each extra reference digest against the excised payload.
// Extra references were already shape-checked in parseEnvelope; here every
// digest value must match the payload, so a tampered second reference
// fails closed. The raw count (including prefixed forms) must match the
// parsed extras plus one; a nil return from unparseable extras therefore
// fails closed instead of verifying only the first digest.
func verifyPayloadDigests(payload []byte, sig envelopeSignature, raw []byte) error {
	want, err := base64.StdEncoding.DecodeString(stripSpace(sig.SignedInfo.Reference.DigestValue))
	if err != nil {
		return fmt.Errorf("security: decode DigestValue: %w", errors.Join(err, ErrSignatureUnverified))
	}

	err = comparePayloadDigest(payload, sig.SignedInfo.Reference.DigestMethod.Algorithm, want)
	if err != nil {
		return err
	}

	total := countReferences(raw)
	extras := extraReferencesForVerify(raw)

	if total > 1 && len(extras) != total-1 {
		return fmt.Errorf("security: multi-reference envelope unparseable: %w", ErrSignatureUnverified)
	}

	for i := range extras {
		extraWant, err := base64.StdEncoding.DecodeString(stripSpace(extras[i].DigestValue))
		if err != nil {
			return fmt.Errorf("security: decode extra DigestValue %d: %w", i, errors.Join(err, ErrSignatureUnverified))
		}

		err = comparePayloadDigest(payload, extras[i].DigestMethod.Algorithm, extraWant)
		if err != nil {
			return fmt.Errorf("security: extra reference %d: %w", i, err)
		}
	}

	return nil
}

// extraReferencesForVerify returns multi-reference digests for verification.
//
// encoding/xml binds only the first Reference into envelopeSignature, so
// extras re-parse from the raw Signature bytes. It returns
// extraReferences(raw) directly so every digest verifies.
func extraReferencesForVerify(raw []byte) []envelopeReference {
	return extraReferences(raw)
}

// comparePayloadDigest checks one payload digest in constant time.
func comparePayloadDigest(payload []byte, method string, want []byte) error {
	switch method {
	case DSIGDigestSHA512:
		sum := sha512.Sum512(payload)
		if subtle.ConstantTimeCompare(sum[:], want) != 1 {
			return fmt.Errorf("security: document digest mismatch: %w", ErrSignatureUnverified)
		}

		return nil
	default:
		sum := sha256.Sum256(payload)
		if subtle.ConstantTimeCompare(sum[:], want) != 1 {
			return fmt.Errorf("security: document digest mismatch: %w", ErrSignatureUnverified)
		}

		return nil
	}
}

// hashCanonical hashes canonical SignedInfo per signature method.
func hashCanonical(method string, canonical []byte) ([]byte, crypto.Hash) {
	switch method {
	case DSIGSigRSAPSSSHA512, DSIGSigECDSASHA512:
		sum := sha512.Sum512(canonical)

		return sum[:], crypto.SHA512
	default:
		sum := sha256.Sum256(canonical)

		return sum[:], crypto.SHA256
	}
}

// verifyWithCert verifies canonical digest with the cert public key.
func verifyWithCert(cert *x509.Certificate, method string, hash crypto.Hash, digest, sig []byte) error {
	switch method {
	case DSIGSigRSASHA256:
		return verifyRSAPKCS1(cert, digest, sig)
	case DSIGSigECDSASHA256, DSIGSigECDSASHA512:
		return verifyECDSA(cert, digest, sig, method)
	case DSIGSigRSAPSSSHA256, DSIGSigRSAPSSSHA512:
		return verifyRSAPSS(cert, method, hash, digest, sig)
	default:
		return fmt.Errorf("security: unsupported signature method %q: %w", method, ErrSignatureUnverified)
	}
}

// verifyRSAPKCS1 verifies RSA PKCS1v15 SHA-256.
func verifyRSAPKCS1(cert *x509.Certificate, digest, sig []byte) error {
	pub, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return fmt.Errorf("security: rsa method with non-rsa key: %w", ErrSignatureUnverified)
	}

	err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest, sig)
	if err != nil {
		return fmt.Errorf("security: rsa signature mismatch: %w", errors.Join(err, ErrSignatureUnverified))
	}

	return nil
}

// verifyECDSA verifies ECDSA ASN.1 signatures for SHA-256 and SHA-512.
func verifyECDSA(cert *x509.Certificate, digest, sig []byte, method string) error {
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("security: ecdsa method with non-ecdsa key: %w", ErrSignatureUnverified)
	}

	if !ecdsa.VerifyASN1(pub, digest, sig) {
		return fmt.Errorf("security: ecdsa %s mismatch: %w", method, ErrSignatureUnverified)
	}

	return nil
}

// verifyRSAPSS verifies RSA-PSS for SHA-256 and SHA-512.
func verifyRSAPSS(cert *x509.Certificate, method string, hash crypto.Hash, digest, sig []byte) error {
	pub, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return fmt.Errorf("security: rsa-pss method with non-rsa key: %w", ErrSignatureUnverified)
	}

	err := rsa.VerifyPSS(pub, hash, digest, sig, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthAuto, Hash: hash})
	if err != nil {
		return fmt.Errorf("security: %s mismatch: %w", pssLabel(method), errors.Join(err, ErrSignatureUnverified))
	}

	return nil
}

// pssLabel names the PSS variant for audit messages.
func pssLabel(method string) string {
	if method == DSIGSigRSAPSSSHA512 {
		return "rsa-pss-sha512"
	}

	return "rsa-pss-sha256"
}

// stripSpace removes whitespace from base64 fields.
func stripSpace(s string) string {
	out := make([]byte, 0, len(s))

	for i := range len(s) {
		c := s[i]
		if c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			continue
		}

		out = append(out, c)
	}

	return string(out)
}

// SignEnvelope signs payload bytes with the documented subset.
//
// Payload must carry no Signature element. The Signature element is
// inserted before the final closing root tag. RSA uses PKCS1v15 SHA-256,
// ECDSA uses ASN1 SHA-256. Only *rsa.PrivateKey and *ecdsa.PrivateKey
// signers are supported.
func SignEnvelope(payload []byte, cert *x509.Certificate, key crypto.Signer) ([]byte, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("security: empty payload cannot sign: %w", ErrSignatureUnverified)
	}

	if bytes.Contains(payload, []byte("<Signature")) {
		return nil, fmt.Errorf("security: payload already carries a Signature: %w", ErrSignatureUnverified)
	}

	if cert == nil || key == nil {
		return nil, fmt.Errorf("security: signing needs certificate and key: %w", ErrSignatureUnverified)
	}

	sum := sha256.Sum256(payload)
	digestB64 := base64.StdEncoding.EncodeToString(sum[:])

	var method string

	switch key.(type) {
	case *rsa.PrivateKey:
		method = DSIGSigRSASHA256
	case *ecdsa.PrivateKey:
		method = DSIGSigECDSASHA256
	default:
		return nil, fmt.Errorf("security: unsupported signing key type: %w", ErrSignatureUnverified)
	}

	info := envelopeSignedInfo{}
	info.CanonicalizationMethod.Algorithm = DSIGC14NExciseBytes
	info.SignatureMethod.Algorithm = method
	info.Reference.URI = ""
	info.Reference.Transforms.Items = []envelopeAlgo{{Algorithm: DSIGTransformEnveloped}}
	info.Reference.DigestMethod.Algorithm = DSIGDigestSHA256
	info.Reference.DigestValue = digestB64

	canonical := canonicalSignedInfo(info, digestB64)
	canonicalDigest := sha256.Sum256(canonical)

	sigBytes, err := key.Sign(nil, canonicalDigest[:], crypto.SHA256)
	if err != nil {
		return nil, fmt.Errorf("security: sign SignedInfo: %w", errors.Join(err, ErrSignatureUnverified))
	}

	certB64 := base64.StdEncoding.EncodeToString(cert.Raw)
	sigB64 := base64.StdEncoding.EncodeToString(sigBytes)

	element := "<Signature xmlns=\"http://www.w3.org/2000/09/xmldsig#\">" +
		"<SignedInfo>" +
		"<CanonicalizationMethod Algorithm=\"" + DSIGC14NExciseBytes + "\"/>" +
		"<SignatureMethod Algorithm=\"" + method + "\"/>" +
		"<Reference URI=\"\">" +
		"<Transforms><Transform Algorithm=\"" + DSIGTransformEnveloped + "\"/></Transforms>" +
		"<DigestMethod Algorithm=\"" + DSIGDigestSHA256 + "\"/>" +
		"<DigestValue>" + digestB64 + "</DigestValue>" +
		"</Reference></SignedInfo>" +
		"<SignatureValue>" + sigB64 + "</SignatureValue>" +
		"<KeyInfo><X509Data><X509Certificate>" + certB64 + "</X509Certificate></X509Data></KeyInfo>" +
		"</Signature>"

	closeIdx := bytes.LastIndex(payload, []byte("</"))
	if closeIdx < 0 {
		return nil, fmt.Errorf("security: payload misses closing root tag: %w", ErrSignatureUnverified)
	}

	out := make([]byte, 0, len(payload)+len(element))
	out = append(out, payload[:closeIdx]...)
	out = append(out, element...)
	out = append(out, payload[closeIdx:]...)

	return out, nil
}
