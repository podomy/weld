// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package access

import (
	"bytes"
	"crypto"
	"crypto/ecdsa"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"mime"
	"strings"
)

// Sentinel errors for S/MIME failures.
var (
	// ErrInvalidSMIME reports a malformed S/MIME envelope.
	ErrInvalidSMIME = errors.New("security: invalid smime envelope")
)

// S/MIME object identifiers for PKCS7 verification.
var (
	// oidSignedData selects the SignedData content type.
	oidSignedData = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 7, 2}
	// oidData selects the detached data content type.
	oidData = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 7, 1}
	// oidSHA256 selects SHA-256 digests.
	oidSHA256 = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 2, 1}
	// oidRSASHA256 selects RSA PKCS1v15 with SHA-256.
	oidRSASHA256 = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 1, 11}
	// oidECDSASHA256 selects ECDSA with SHA-256.
	oidECDSASHA256 = asn1.ObjectIdentifier{1, 2, 840, 10045, 4, 3, 2}
	// oidContentType selects the content-type signed attribute.
	oidContentType = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 9, 3}
	// oidMessageDigest selects the message-digest signed attribute.
	oidMessageDigest = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 9, 4}
)

// isSMIME reports whether data carries a multipart/signed envelope.
func isSMIME(data []byte) bool {
	return bytes.Contains(data, []byte("multipart/signed")) && bytes.Contains(data, []byte("boundary="))
}

// smimeParts holds the extracted content plus signature bytes.
type smimeParts struct {
	// content holds the full first part for digest verification.
	content []byte
	// payload holds the first part body for XML parsing.
	payload []byte
	// signature holds the DER PKCS7 signature bytes.
	signature []byte
}

// extractSMIME splits one S/MIME message into content plus signature.
func extractSMIME(data []byte) (smimeParts, error) {
	var out smimeParts

	boundary, err := smimeBoundary(data)
	if err != nil {
		return out, err
	}

	content, sigB64, err := splitSMIMEParts(data, boundary)
	if err != nil {
		return out, err
	}

	sigDER, err := base64.StdEncoding.DecodeString(stripSpace(string(sigB64)))
	if err != nil {
		return out, fmt.Errorf("security: decode pkcs7 signature: %w", errors.Join(err, ErrSignatureUnverified))
	}

	if len(content) == 0 || len(sigDER) == 0 {
		return out, fmt.Errorf("security: empty smime content or signature: %w", ErrSignatureUnverified)
	}

	out.content = content
	out.payload = contentBodyForDigest(content)
	out.signature = sigDER

	return out, nil
}

// smimeBoundary extracts the multipart boundary parameter.
func smimeBoundary(data []byte) (string, error) {
	head := data
	if len(head) > 4096 {
		head = head[:4096]
	}

	var ctype string

	for line := range strings.SplitSeq(string(head), "\n") {
		trimmed := strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(trimmed, "Content-Type:"); ok {
			ctype = strings.TrimSpace(after)
			break
		}
	}

	if ctype == "" {
		return "", fmt.Errorf("security: smime misses content type: %w", ErrSignatureUnverified)
	}

	mediatype, params, err := mime.ParseMediaType(ctype)
	if err != nil {
		return "", fmt.Errorf("security: parse smime content type: %w", errors.Join(err, ErrSignatureUnverified))
	}

	if !strings.Contains(mediatype, "multipart/signed") {
		return "", fmt.Errorf("security: smime media type %q is not multipart/signed: %w", mediatype, ErrSignatureUnverified)
	}

	protocol := strings.ToLower(params["protocol"])
	if !strings.Contains(protocol, "pkcs7-signature") {
		return "", fmt.Errorf("security: smime protocol %q is not pkcs7: %w", params["protocol"], ErrSignatureUnverified)
	}

	micalg := strings.ToLower(params["micalg"])
	if micalg != "" && !strings.Contains(micalg, "sha-256") && !strings.Contains(micalg, "sha256") {
		return "", fmt.Errorf("security: smime micalg %q is not sha-256: %w", params["micalg"], ErrSignatureUnverified)
	}

	boundary := params["boundary"]
	if boundary == "" {
		return "", fmt.Errorf("security: smime misses boundary: %w", ErrSignatureUnverified)
	}

	return boundary, nil
}

// splitSMIMEParts returns the first part plus second part base64.
func splitSMIMEParts(data []byte, boundary string) ([]byte, []byte, error) {
	marker := []byte("--" + boundary)
	pieces := bytes.Split(data, marker)
	if len(pieces) < 3 {
		return nil, nil, fmt.Errorf("security: smime needs two parts: %w", ErrSignatureUnverified)
	}

	content := stripFraming(pieces[1])
	sigPart := stripFraming(pieces[2])

	body, err := splitPartBody(sigPart)
	if err != nil {
		return nil, nil, err
	}

	return content, body, nil
}

// stripFraming removes one leading plus one trailing line break.
func stripFraming(part []byte) []byte {
	if bytes.HasPrefix(part, []byte("\r\n")) {
		part = part[2:]
	} else if bytes.HasPrefix(part, []byte("\n")) {
		part = part[1:]
	}

	if bytes.HasSuffix(part, []byte("\r\n")) {
		part = part[:len(part)-2]
	} else if bytes.HasSuffix(part, []byte("\n")) {
		part = part[:len(part)-1]
	}

	return part
}

// splitPartBody separates part headers from the body by blank line.
func splitPartBody(part []byte) ([]byte, error) {
	idx := bytes.Index(part, []byte("\r\n\r\n"))
	sep := 4

	if idx < 0 {
		idx = bytes.Index(part, []byte("\n\n"))
		sep = 2
	}

	if idx < 0 {
		return nil, fmt.Errorf("security: smime signature part misses headers: %w", ErrSignatureUnverified)
	}

	headers := part[:idx]
	body := part[idx+sep:]

	lower := bytes.ToLower(headers)
	if !bytes.Contains(lower, []byte("base64")) {
		return nil, fmt.Errorf("security: smime signature encoding is not base64: %w", ErrSignatureUnverified)
	}

	return bytes.TrimSpace(body), nil
}

// contentBodyForDigest strips optional part headers from first part.
func contentBodyForDigest(part []byte) []byte {
	idx := bytes.Index(part, []byte("\r\n\r\n"))
	sep := 4

	if idx < 0 {
		idx = bytes.Index(part, []byte("\n\n"))
		sep = 2
	}

	if idx < 0 {
		return part
	}

	head := part[:idx]
	if !looksLikeHeaders(head) {
		return part
	}

	return part[idx+sep:]
}

// looksLikeHeaders reports whether head carries colon headers.
func looksLikeHeaders(head []byte) bool {
	lines := bytes.Split(head, []byte("\n"))
	found := false

	for _, line := range lines {
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			continue
		}

		if !bytes.Contains(trimmed, []byte(":")) {
			return false
		}

		found = true
	}

	return found
}

// canonicalSMIME maps line endings to CRLF for digest computation.
func canonicalSMIME(content []byte) []byte {
	flat := make([]byte, 0, len(content))
	for i := 0; i < len(content); i++ {
		if content[i] == '\r' && i+1 < len(content) && content[i+1] == '\n' {
			flat = append(flat, '\n')
			i++
		} else {
			flat = append(flat, content[i])
		}
	}

	out := make([]byte, 0, len(flat)+8)
	for _, c := range flat {
		if c == '\n' {
			out = append(out, '\r', '\n')
		} else {
			out = append(out, c)
		}
	}

	return out
}

// pkcs7ContentInfo wraps one PKCS7 content.
type pkcs7ContentInfo struct {
	// ContentType selects signedData.
	ContentType asn1.ObjectIdentifier
	// Content holds the SignedData bytes.
	Content asn1.RawValue `asn1:"tag:0,explicit,optional"`
}

// pkcs7AlgoID carries one algorithm identifier.
type pkcs7AlgoID struct {
	// Algo names the algorithm.
	Algo asn1.ObjectIdentifier
	// Params holds optional parameters.
	Params asn1.RawValue `asn1:"optional"`
}

// pkcs7EncapContent carries the encapsulated content type.
type pkcs7EncapContent struct {
	// ContentType names the inner type.
	ContentType asn1.ObjectIdentifier
	// Content holds detached content when present.
	Content asn1.RawValue `asn1:"tag:0,explicit,optional"`
}

// pkcs7IssuerSerial binds a signer to one certificate.
type pkcs7IssuerSerial struct {
	// Issuer holds the DER issuer name.
	Issuer asn1.RawValue
	// Serial holds the DER serial integer.
	Serial asn1.RawValue
}

// pkcs7SignerInfo carries one signer block.
type pkcs7SignerInfo struct {
	// Version is the signer version.
	Version int
	// SID binds issuer plus serial.
	SID pkcs7IssuerSerial
	// DigestAlgorithm names the digest.
	DigestAlgorithm pkcs7AlgoID
	// SignedAttrs holds the signed attributes.
	SignedAttrs asn1.RawValue `asn1:"tag:0,optional"`
	// SignatureAlgorithm names the signature scheme.
	SignatureAlgorithm pkcs7AlgoID
	// Signature holds the signature bytes.
	Signature []byte
	// UnauthAttrs holds unauthenticated attributes.
	UnauthAttrs asn1.RawValue `asn1:"tag:1,optional"`
}

// pkcs7SignedData carries the SignedData set.
type pkcs7SignedData struct {
	// Version is the SignedData version.
	Version int
	// DigestAlgorithms lists digest methods.
	DigestAlgorithms []pkcs7AlgoID `asn1:"set"`
	// EncapContent names the inner content.
	EncapContent pkcs7EncapContent
	// Certs holds included certificates.
	Certs asn1.RawValue `asn1:"tag:0,optional"`
	// CRLs holds revocation lists.
	CRLs asn1.RawValue `asn1:"tag:1,optional"`
	// SignerInfos holds exactly one signer.
	SignerInfos []pkcs7SignerInfo `asn1:"set"`
}

// pkcs7Attribute carries one signed attribute.
type pkcs7Attribute struct {
	// Type names the attribute.
	Type asn1.ObjectIdentifier
	// Values holds the attribute set.
	Values []asn1.RawValue `asn1:"set"`
}

// verifySignedPayload verifies one signed document in either envelope.
//
// It accepts the peer-native S/MIME envelope plus the documented WELD
// excise-bytes subset. Full C14N, prefixed signatures, multiple
// references, and any other method refuse loudly. The returned payload
// still needs structural parsing by the caller.
func verifySignedPayload(data []byte, caPool *x509.CertPool) ([]byte, error) {
	if isSMIME(data) {
		payload, cert, err := verifySMIME(data, caPool)
		if err != nil {
			return nil, err
		}

		if cert == nil || len(cert.Raw) == 0 {
			return nil, fmt.Errorf("security: verified document misses signing certificate: %w", ErrSignatureUnverified)
		}

		return payload, nil
	}

	payload, cert, err := verifyEnvelopeSignature(data, caPool)
	if err != nil {
		return nil, err
	}

	if cert == nil || len(cert.Raw) == 0 {
		return nil, fmt.Errorf("security: verified document misses signing certificate: %w", ErrSignatureUnverified)
	}

	return payload, nil
}

// verifySMIME verifies one S/MIME envelope against a trust pool.
func verifySMIME(data []byte, caPool *x509.CertPool) ([]byte, *x509.Certificate, error) {
	if caPool == nil {
		return nil, nil, fmt.Errorf("security: nil trust pool: %w", ErrSignatureUnverified)
	}

	parts, err := extractSMIME(data)
	if err != nil {
		return nil, nil, err
	}

	cert, err := verifyPKCS7(parts.signature, parts.content, caPool)
	if err != nil {
		return nil, nil, err
	}

	return parts.payload, cert, nil
}

// verifyPKCS7 checks digest, chain, plus signature in order.
func verifyPKCS7(sigDER, content []byte, caPool *x509.CertPool) (*x509.Certificate, error) {
	var outer pkcs7ContentInfo

	_, err := asn1.Unmarshal(sigDER, &outer)
	if err != nil {
		return nil, fmt.Errorf("security: parse pkcs7 outer: %w", errors.Join(err, ErrSignatureUnverified))
	}

	if !outer.ContentType.Equal(oidSignedData) {
		return nil, fmt.Errorf("security: pkcs7 type %s is not signedData: %w", outer.ContentType.String(), ErrSignatureUnverified)
	}

	var sd pkcs7SignedData

	_, err = asn1.Unmarshal(outer.Content.Bytes, &sd)
	if err != nil {
		return nil, fmt.Errorf("security: parse signedData: %w", errors.Join(err, ErrSignatureUnverified))
	}

	err = checkPKCS7Shape(&sd)
	if err != nil {
		return nil, err
	}

	si := sd.SignerInfos[0]

	err = checkDigestAlgo(si.DigestAlgorithm.Algo)
	if err != nil {
		return nil, err
	}

	certs, err := parsePKCS7Certs(sd.Certs.Bytes)
	if err != nil {
		return nil, err
	}

	canonical := canonicalSMIME(content)
	sum := sha256.Sum256(canonical)

	attrsDER, digest, err := parseSignedAttrs(si.SignedAttrs.FullBytes)
	if err != nil {
		return nil, err
	}

	if sum != digest {
		return nil, fmt.Errorf("security: smime digest mismatch: %w", ErrSignatureUnverified)
	}

	cert, err := finishPKCS7Verify(certs, &si, &sd, attrsDER, caPool)
	if err != nil {
		return nil, err
	}

	return cert, nil
}

// finishPKCS7Verify selects the signer and checks usage, revocation,
// plus the attribute signature in order.
func finishPKCS7Verify(certs []*x509.Certificate, si *pkcs7SignerInfo, sd *pkcs7SignedData, attrsDER []byte, caPool *x509.CertPool) (*x509.Certificate, error) {
	cert, err := selectSignerCert(certs, si, caPool)
	if err != nil {
		return nil, err
	}

	err = checkSignerUsage(cert)
	if err != nil {
		return nil, err
	}

	err = checkSignerRevocation(sd, certs, cert)
	if err != nil {
		return nil, err
	}

	err = verifyAttrsSignature(cert, si, attrsDER)
	if err != nil {
		return nil, err
	}

	return cert, nil
}

// checkPKCS7Shape enforces detached single-signer shape.
func checkPKCS7Shape(sd *pkcs7SignedData) error {
	if !sd.EncapContent.ContentType.Equal(oidData) {
		return fmt.Errorf("security: pkcs7 inner type %s is not data: %w", sd.EncapContent.ContentType.String(), ErrSignatureUnverified)
	}

	if len(sd.EncapContent.Content.Bytes) != 0 || len(sd.EncapContent.Content.FullBytes) != 0 {
		return fmt.Errorf("security: pkcs7 must be detached: %w", ErrSignatureUnverified)
	}

	if len(sd.SignerInfos) != 1 {
		return fmt.Errorf("security: pkcs7 needs one signer have %d: %w", len(sd.SignerInfos), ErrSignatureUnverified)
	}

	return nil
}

// checkDigestAlgo accepts SHA-256 only.
func checkDigestAlgo(algo asn1.ObjectIdentifier) error {
	if !algo.Equal(oidSHA256) {
		return fmt.Errorf("security: pkcs7 digest %s is not sha-256: %w", algo.String(), ErrSignatureUnverified)
	}

	return nil
}

// parsePKCS7Certs splits concatenated certificate DER blocks.
func parsePKCS7Certs(raw []byte) ([]*x509.Certificate, error) {
	var out []*x509.Certificate

	rest := raw
	for len(rest) > 0 {
		var value asn1.RawValue

		next, err := asn1.Unmarshal(rest, &value)
		if err != nil {
			return nil, fmt.Errorf("security: parse pkcs7 certificate: %w", errors.Join(err, ErrSignatureUnverified))
		}

		cert, err := x509.ParseCertificate(value.FullBytes)
		if err != nil {
			return nil, fmt.Errorf("security: parse pkcs7 certificate: %w", errors.Join(err, ErrSignatureUnverified))
		}

		out = append(out, cert)
		rest = next
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("security: pkcs7 carries no certificates: %w", ErrSignatureUnverified)
	}

	return out, nil
}

// parseSignedAttrs returns DER SET plus message digest.
func parseSignedAttrs(full []byte) ([]byte, [32]byte, error) {
	var empty [32]byte

	if len(full) == 0 {
		return nil, empty, fmt.Errorf("security: pkcs7 misses signed attributes: %w", ErrSignatureUnverified)
	}

	der := append([]byte(nil), full...)
	der[0] = 0x31

	var attrs []pkcs7Attribute

	_, err := asn1.UnmarshalWithParams(der, &attrs, "set")
	if err != nil {
		return nil, empty, fmt.Errorf("security: parse signed attributes: %w", errors.Join(err, ErrSignatureUnverified))
	}

	var contentTypeOK bool

	var digest [32]byte

	var digestOK bool

	for i := range attrs {
		switch {
		case attrs[i].Type.Equal(oidContentType):
			contentTypeOK = checkContentTypeAttr(&attrs[i])
		case attrs[i].Type.Equal(oidMessageDigest):
			got, ok := messageDigestAttr(&attrs[i])
			if ok {
				digest = got
				digestOK = true
			}
		default:
		}
	}

	if !contentTypeOK {
		return nil, empty, fmt.Errorf("security: pkcs7 content-type attribute mismatch: %w", ErrSignatureUnverified)
	}

	if !digestOK {
		return nil, empty, fmt.Errorf("security: pkcs7 misses message digest: %w", ErrSignatureUnverified)
	}

	return der, digest, nil
}

// checkContentTypeAttr verifies the content-type attribute names data.
func checkContentTypeAttr(attr *pkcs7Attribute) bool {
	if attr == nil || len(attr.Values) != 1 {
		return false
	}

	var oid asn1.ObjectIdentifier

	_, err := asn1.Unmarshal(attr.Values[0].FullBytes, &oid)
	if err != nil {
		return false
	}

	return oid.Equal(oidData)
}

// messageDigestAttr extracts the 32 byte digest attribute.
func messageDigestAttr(attr *pkcs7Attribute) ([32]byte, bool) {
	var empty [32]byte

	if attr == nil || len(attr.Values) != 1 {
		return empty, false
	}

	var raw []byte

	_, err := asn1.Unmarshal(attr.Values[0].FullBytes, &raw)
	if err != nil {
		return empty, false
	}

	if len(raw) != 32 {
		return empty, false
	}

	var out [32]byte

	copy(out[:], raw)

	return out, true
}

// selectSignerCert finds the chained signer certificate.
//
// Only the certificate named by the exact issuer plus serial binding
// is accepted: a chained certificate whose key happens to verify is
// never substituted, so a mismatched signer fails closed instead of
// falling back to any chained certificate.
func selectSignerCert(certs []*x509.Certificate, si *pkcs7SignerInfo, caPool *x509.CertPool) (*x509.Certificate, error) {
	for _, cert := range certs {
		if !signerMatchesCert(si, cert) {
			continue
		}

		_, err := cert.Verify(x509.VerifyOptions{
			Roots:     caPool,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageAny},
		})
		if err != nil {
			continue
		}

		return cert, nil
	}

	return nil, fmt.Errorf("security: no pkcs7 signer chains to trust: %w", ErrSignatureUnverified)
}

// signerMatchesCert compares issuer plus serial bindings.
func signerMatchesCert(si *pkcs7SignerInfo, cert *x509.Certificate) bool {
	if si == nil || cert == nil {
		return false
	}

	serial := new(big.Int).SetBytes(si.SID.Serial.Bytes)
	if cert.SerialNumber == nil || serial.Cmp(cert.SerialNumber) != 0 {
		return false
	}

	if len(si.SID.Issuer.Bytes) == 0 || len(cert.RawIssuer) == 0 {
		return false
	}

	return bytes.Equal(si.SID.Issuer.Bytes, cert.RawIssuer) || bytes.Equal(si.SID.Issuer.FullBytes, cert.RawIssuer)
}

// checkSignerUsage requires digitalSignature on the signer certificate.
//
// Chain verification already constrains extended key usage to client,
// server, or any auth; key usage must still name signing, otherwise a
// key-agreement-only certificate could sign documents.
func checkSignerUsage(cert *x509.Certificate) error {
	if cert == nil {
		return fmt.Errorf("security: missing pkcs7 signer certificate: %w", ErrSignatureUnverified)
	}

	if cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return fmt.Errorf("security: pkcs7 signer lacks digitalSignature usage: %w", ErrSignatureUnverified)
	}

	return nil
}

// checkSignerRevocation refuses a signer listed in a stapled CRL.
//
// Each stapled CRL must carry a signature that verifies against one of
// the envelope certificates; a CRL that verifies against none of them
// refuses the document fail-closed instead of being ignored. Only
// entries from a verified CRL are enforced.
func checkSignerRevocation(sd *pkcs7SignedData, certs []*x509.Certificate, signer *x509.Certificate) error {
	if sd == nil || len(sd.CRLs.Bytes) == 0 {
		return nil
	}

	if signer == nil {
		return fmt.Errorf("security: missing pkcs7 signer for revocation check: %w", ErrSignatureUnverified)
	}

	lists, err := parseStapledCRLs(sd.CRLs.Bytes)
	if err != nil {
		return err
	}

	for i := range lists {
		err = enforceStapledCRL(lists[i], certs, signer)
		if err != nil {
			return err
		}
	}

	return nil
}

// enforceStapledCRL verifies one stapled CRL against the envelope
// certificates and refuses a listed signer.
//
// A CRL that verifies against none of the envelope certificates
// refuses the document fail-closed instead of being ignored.
func enforceStapledCRL(list *x509.RevocationList, certs []*x509.Certificate, signer *x509.Certificate) error {
	authority := false

	for _, cert := range certs {
		if list.CheckSignatureFrom(cert) == nil {
			authority = true
			break
		}
	}

	if !authority {
		return fmt.Errorf("security: pkcs7 CRL carries no verifiable signature: %w", ErrSignatureUnverified)
	}

	for _, revoked := range list.RevokedCertificateEntries {
		if revoked.SerialNumber != nil && signer.SerialNumber != nil && revoked.SerialNumber.Cmp(signer.SerialNumber) == 0 {
			return fmt.Errorf("security: pkcs7 signer is revoked: %w", ErrSignatureUnverified)
		}
	}

	return nil
}

// parseStapledCRLs splits concatenated DER CRLs from the SignedData set.
func parseStapledCRLs(raw []byte) ([]*x509.RevocationList, error) {
	unwrapped, err := unwrapDERSet(raw)
	if err != nil {
		return nil, err
	}

	elements, err := splitDERElements(unwrapped)
	if err != nil {
		return nil, err
	}

	var out []*x509.RevocationList

	for i := range elements {
		list, perr := x509.ParseRevocationList(elements[i])
		if perr != nil {
			return nil, fmt.Errorf("security: parse pkcs7 CRL %d: %w", i, errors.Join(perr, ErrSignatureUnverified))
		}

		out = append(out, list)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("security: pkcs7 CRL set is empty: %w", ErrSignatureUnverified)
	}

	return out, nil
}

// unwrapDERSet strips one leading SET wrapper from explicit CRL sets.
func unwrapDERSet(raw []byte) ([]byte, error) {
	if len(raw) == 0 || raw[0] != 0x31 {
		return raw, nil
	}

	length, size, err := derLength(raw[1:])
	if err != nil {
		return nil, err
	}

	if 1+size+length != len(raw) {
		return nil, fmt.Errorf("security: DER set bounds mismatch: %w", ErrSignatureUnverified)
	}

	return raw[1+size:], nil
}

// splitDERElements splits concatenated DER values at top-level bounds.
func splitDERElements(raw []byte) ([][]byte, error) {
	var out [][]byte

	rest := raw

	for len(rest) > 0 {
		if len(rest) < 2 {
			return nil, fmt.Errorf("security: truncated DER element: %w", ErrSignatureUnverified)
		}

		length, size, derr := derLength(rest[1:])
		if derr != nil {
			return nil, derr
		}

		total := 1 + size + length
		if total > len(rest) {
			return nil, fmt.Errorf("security: DER element overruns: %w", ErrSignatureUnverified)
		}

		out = append(out, rest[:total])
		rest = rest[total:]
	}

	return out, nil
}

// derLength parses one DER length prefix after the tag byte.
func derLength(raw []byte) (length, size int, err error) {
	if len(raw) == 0 {
		return 0, 0, fmt.Errorf("security: truncated DER length: %w", ErrSignatureUnverified)
	}

	first := raw[0]
	if first&0x80 == 0 {
		return int(first), 1, nil
	}

	count := int(first & 0x7f)
	if count == 0 || count > 4 || len(raw) < 1+count {
		return 0, 0, fmt.Errorf("security: bad DER length form: %w", ErrSignatureUnverified)
	}

	total := 0

	for i := range count {
		total = total*256 + int(raw[1+i])
	}

	return total, 1 + count, nil
}

// verifyAttrsSignature verifies the DER SET with the signer key.
func verifyAttrsSignature(cert *x509.Certificate, si *pkcs7SignerInfo, attrsDER []byte) error {
	if cert == nil || si == nil || len(attrsDER) == 0 {
		return fmt.Errorf("security: missing pkcs7 signature inputs: %w", ErrSignatureUnverified)
	}

	switch si.SignatureAlgorithm.Algo.String() {
	case oidRSASHA256.String():
		return verifyRSAAttrs(cert, si, attrsDER)
	case oidECDSASHA256.String():
		return verifyECDSAAttrs(cert, si, attrsDER)
	default:
		return fmt.Errorf("security: pkcs7 signature %s unsupported: %w", si.SignatureAlgorithm.Algo.String(), ErrSignatureUnverified)
	}
}

// verifyRSAAttrs verifies RSA PKCS1v15 over the attrs digest.
func verifyRSAAttrs(cert *x509.Certificate, si *pkcs7SignerInfo, attrsDER []byte) error {
	pub, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return fmt.Errorf("security: rsa signature with non-rsa key: %w", ErrSignatureUnverified)
	}

	digest := sha256.Sum256(attrsDER)
	err := rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], si.Signature)
	if err != nil {
		return fmt.Errorf("security: rsa pkcs7 mismatch: %w", errors.Join(err, ErrSignatureUnverified))
	}

	return nil
}

// verifyECDSAAttrs verifies ECDSA ASN1 over the attrs digest.
func verifyECDSAAttrs(cert *x509.Certificate, si *pkcs7SignerInfo, attrsDER []byte) error {
	pub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return fmt.Errorf("security: ecdsa signature with non-ecdsa key: %w", ErrSignatureUnverified)
	}

	digest := sha256.Sum256(attrsDER)
	if !ecdsa.VerifyASN1(pub, digest[:], si.Signature) {
		return fmt.Errorf("security: ecdsa pkcs7 mismatch: %w", ErrSignatureUnverified)
	}

	return nil
}
