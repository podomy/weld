// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package dds holds the CycloneDDS sidecar contract.
//
// SECURITY_GAP: this file is honest tolerance only. It is NOT security.
// Weld implements no DDS Security crypto and must never be described as
// secured. The following are explicitly NOT implemented here or anywhere
// else in this repo:
//   - Authentication handshake. No DDS:Auth:PKI-DH challenge or response,
//     no identity certificate verification, no shared secret establishment.
//   - Crypto transforms. No AES-GCM or other decryption of SEC_BODY or
//     SRTPS payloads, no session keys, no key exchange, no crypto tokens.
//   - Access-control enforcement. Governance and permissions files are
//     never read for decisions; topic allow or deny rules are not applied.
//   - Revocation. No CRL or OCSP checks, no expiry-driven eviction of
//     compromised identities.
//   - Secure discovery. Protected SPDP or SEDP builtin writers stay
//     unreadable; see ErrSecuredDiscoveryRequiresCrypto.
//
// What this file does instead: parse and tolerate every DDS Security
// discovery PID so secured peers never break unsecured discovery,
// classify peers from their dds.sec.* properties, drop protected RTPS
// submessages loudly with a distinct reason instead of attempting
// decryption theater, and carry configuration for future SDK and CLI
// wiring. Tolerance here grants zero access and decrypts nothing.
package dds

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// DDS Security discovery parameter ids.
//
// Values follow the OMG DDS Security specification parameter id table as
// observed in CycloneDDS and FastDDS peers. PIDPropertyList is the generic
// key-value carrier (PropertyQosPolicy) that transports dds.sec.* entries
// on both SPDP and SEDP announcements.
const (
	// PIDIdentityToken carries the opaque authentication identity token.
	PIDIdentityToken uint16 = 0x1001
	// PIDPermissionsToken carries the opaque permissions token.
	PIDPermissionsToken uint16 = 0x1002
	// PIDDataTags carries opaque data tagging information.
	PIDDataTags uint16 = 0x1003
	// PIDEndpointSecurityInfo carries endpoint security attributes.
	PIDEndpointSecurityInfo uint16 = 0x1004
	// PIDParticipantSecurityInfo carries participant security attributes.
	PIDParticipantSecurityInfo uint16 = 0x1005
	// PIDIdentityStatusToken carries the opaque identity status token.
	PIDIdentityStatusToken uint16 = 0x1006
	// PIDPropertyList carries generic key-value properties including dds.sec.*.
	PIDPropertyList uint16 = 0x0059
)

// Protected RTPS submessage kinds.
//
// Values follow the OMG DDS Security specification: SEC_BODY, SEC_PREFIX,
// and SEC_POSTFIX wrap a single protected submessage, while SRTPS_PREFIX
// and SRTPS_POSTFIX wrap a whole protected RTPS message. Arrival of any
// of these without an established crypto context is a loud drop, never a
// decode attempt.
const (
	// SecuredSubmessageSecBody wraps one protected submessage body.
	SecuredSubmessageSecBody byte = 0x30
	// SecuredSubmessageSecPrefix opens one protected submessage.
	SecuredSubmessageSecPrefix byte = 0x31
	// SecuredSubmessageSecPostfix closes one protected submessage.
	SecuredSubmessageSecPostfix byte = 0x32
	// SecuredSubmessageSRTPSPrefix opens one protected RTPS message.
	SecuredSubmessageSRTPSPrefix byte = 0x33
	// SecuredSubmessageSRTPSPostfix closes one protected RTPS message.
	SecuredSubmessageSRTPSPostfix byte = 0x34
)

// Sentinel errors for explicit security boundary failures.
var (
	// ErrSecuredTransformUnavailable reports a protected submessage with no crypto context.
	ErrSecuredTransformUnavailable = errors.New("dds: secured transform unavailable")
	// ErrSecuredDiscoveryRequiresCrypto reports a peer demanding protected discovery.
	ErrSecuredDiscoveryRequiresCrypto = errors.New("dds: secured discovery requires crypto")
	// ErrSecurityPropertyInvalid reports a malformed security property value.
	//
	// It is the warn-and-continue sentinel for ExtractSecurityInfo: malformed
	// values warn with this error while discovery of sibling params keeps
	// flowing. It never authorizes a must-reject decision; use
	// ErrSecuredDiscoveryRequiresCrypto for protected-discovery rejects and
	// ErrSecuredTransformUnavailable for protected-submessage drops so
	// errors.Is distinguishes warnings from rejects.
	ErrSecurityPropertyInvalid = errors.New("dds: invalid security property")
	// ErrSecurityConfigInvalid reports an invalid security configuration.
	ErrSecurityConfigInvalid = errors.New("dds: invalid security config")
)

// DropReasonSecuredTransformUnavailable counts protected submessages dropped with no crypto.
//
// It extends the datapath DropReason codes in datapath.go with a distinct
// machine readable cause so dashboards tell crypto-boundary drops apart
// from unknown-writer and decode-fail drops.
const DropReasonSecuredTransformUnavailable DropReason = "secured-transform-unavailable"

// SecurityClassification names the crypto posture of one remote peer.
type SecurityClassification string

// Peer security classifications.
const (
	// SecurityUnsecured marks a peer with no security markers at all.
	SecurityUnsecured SecurityClassification = "unsecured"
	// SecurityCompatibleUnknown marks a security-aware peer that does not demand crypto.
	SecurityCompatibleUnknown SecurityClassification = "secured-compatible-unknown"
	// SecurityRequiresCrypto marks a peer whose properties demand crypto transforms.
	SecurityRequiresCrypto SecurityClassification = "secured-requiring-crypto"
)

// Security property prefixes and hard-require keys.
const (
	// securityPropertyPrefix selects DDS Security QoS properties.
	securityPropertyPrefix = "dds.sec."
	// securityMaxProperties caps one property list for bounded work.
	securityMaxProperties = 64
	// securityMaxPropertyStringLen caps one property name or value.
	securityMaxPropertyStringLen = 1024
)

// securityHardRequireKeys demand crypto transforms when present in properties.
var securityHardRequireKeys = map[string]bool{
	"dds.sec.auth.identity_certificate": true,
	"dds.sec.auth.private_key":          true,
	"dds.sec.access.governance":         true,
	"dds.sec.access.permissions":        true,
	"dds.sec.access.permissions_ca":     true,
}

// SecurityInfo is the typed record for one peer's security posture.
//
// Masks are raw wire values read little endian, matching CycloneDDS wire
// order; exact bit semantics are out of scope and undocumented here on
// purpose. Classification is the only decision input. Presence flags record
// PID sightings even when the value is malformed so classification stays
// fail-closed instead of treating a malformed secured peer as unsecured.
// Presence of this record grants nothing and decrypts nothing.
type SecurityInfo struct {
	// Present reports any security PID or dds.sec.* property was seen.
	Present bool
	// HasParticipantSecurityInfo reports PID 0x1005 was seen.
	HasParticipantSecurityInfo bool
	// ParticipantSecurityAttributes holds the raw participant mask, zero when absent.
	ParticipantSecurityAttributes uint32
	// HasEndpointSecurityInfo reports PID 0x1004 was seen.
	HasEndpointSecurityInfo bool
	// EndpointSecurityAttributes holds the raw endpoint mask, zero when absent.
	EndpointSecurityAttributes uint32
	// IdentityTokenPresent reports PID 0x1001 was seen.
	IdentityTokenPresent bool
	// PermissionsTokenPresent reports PID 0x1002 was seen.
	PermissionsTokenPresent bool
	// DataTagsPresent reports PID 0x1003 was seen.
	DataTagsPresent bool
	// IdentityStatusPresent reports PID 0x1006 was seen.
	IdentityStatusPresent bool
	// PropertyListPresent reports PID 0x0059 was seen, even when malformed.
	PropertyListPresent bool
	// Properties holds decoded key-value properties, including dds.sec.* entries.
	Properties map[string]string
	// Classification is the peer posture derived from markers and properties.
	Classification SecurityClassification
}

// SecurityConfig carries DDS Security material for later SDK and CLI wiring.
//
// INTEGRATION: the SDK must construct this from flags or files, call
// Validate, and pass it to backend construction. The CLI must add one flag
// per path plus an enable flag. No call site exists yet; this type only
// reserves the shape so future wiring has a pinned contract. Governance
// and permissions documents are never read for access decisions: presence
// of paths here grants nothing and enforces no topic allow or deny rules,
// matching the honest-tolerance posture above.
type SecurityConfig struct {
	// Enabled selects secured operation once crypto exists. False means tolerate mode.
	Enabled bool
	// IdentityCertificatePath is the PEM identity certificate file.
	IdentityCertificatePath string
	// IdentityPrivateKeyPath is the PEM identity private key file.
	IdentityPrivateKeyPath string
	// PermissionsCAPath is the permissions authority certificate file.
	PermissionsCAPath string
	// GovernancePath is the signed governance file.
	GovernancePath string
}

// Validate checks path presence for secured operation.
//
// Disabled config always passes: tolerate mode needs no material. Enabled
// config requires every path to be non-empty; file contents are never
// parsed here because crypto is out of scope. Each missing field joins a
// distinct error so errors.Is still reports ErrSecurityConfigInvalid.
func (c SecurityConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	var errs []error

	if strings.TrimSpace(c.IdentityCertificatePath) == "" {
		errs = append(errs, fmt.Errorf("dds: security identity certificate path is empty: %w", ErrSecurityConfigInvalid))
	}

	if strings.TrimSpace(c.IdentityPrivateKeyPath) == "" {
		errs = append(errs, fmt.Errorf("dds: security identity private key path is empty: %w", ErrSecurityConfigInvalid))
	}

	if strings.TrimSpace(c.PermissionsCAPath) == "" {
		errs = append(errs, fmt.Errorf("dds: security permissions CA path is empty: %w", ErrSecurityConfigInvalid))
	}

	if strings.TrimSpace(c.GovernancePath) == "" {
		errs = append(errs, fmt.Errorf("dds: security governance path is empty: %w", ErrSecurityConfigInvalid))
	}

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("dds: security config: %w", errors.Join(errs...))
}

// IsProtectedSubmessageID reports whether id names a crypto-wrapped submessage.
func IsProtectedSubmessageID(id byte) bool {
	switch id {
	case SecuredSubmessageSecBody,
		SecuredSubmessageSecPrefix,
		SecuredSubmessageSecPostfix,
		SecuredSubmessageSRTPSPrefix,
		SecuredSubmessageSRTPSPostfix:
		return true
	default:
		return false
	}
}

// SecuredTransformName names one submessage kind for loud drop errors.
func SecuredTransformName(id byte) string {
	switch id {
	case SecuredSubmessageSecBody:
		return "SEC_BODY"
	case SecuredSubmessageSecPrefix:
		return "SEC_PREFIX"
	case SecuredSubmessageSecPostfix:
		return "SEC_POSTFIX"
	case SecuredSubmessageSRTPSPrefix:
		return "SRTPS_PREFIX"
	case SecuredSubmessageSRTPSPostfix:
		return "SRTPS_POSTFIX"
	default:
		return "UNKNOWN"
	}
}

// ClassifySecurity maps properties plus info-PID sightings to a posture.
//
// It is pure: the same inputs always yield the same class. No markers at
// all means unsecured. Any dds.sec.* property or any security info PID
// means secured-compatible-unknown unless a hard-require property (identity
// certificate or key, governance, permissions, or any dds.sec.crypto.*
// entry) demands crypto transforms.
func ClassifySecurity(properties map[string]string, securityInfoSeen bool) SecurityClassification {
	seen := securityInfoSeen

	for key := range properties {
		if !strings.HasPrefix(key, securityPropertyPrefix) {
			continue
		}

		seen = true

		if securityHardRequireKeys[key] || strings.HasPrefix(key, "dds.sec.crypto.") {
			return SecurityRequiresCrypto
		}
	}

	if seen {
		return SecurityCompatibleUnknown
	}

	return SecurityUnsecured
}

// ExtractSecurityInfo parses every security PID in params into a record.
//
// It never fails the parse loop: each malformed security value appends one
// loud warning and parsing continues with the next param, so secured peers
// can never kill discovery of unsecured peers. The returned warnings are
// empty when every security value parsed cleanly.
func ExtractSecurityInfo(params []rtps.Param) (SecurityInfo, []error) {
	info := SecurityInfo{Properties: make(map[string]string)}

	var warnings []error

	for _, p := range params {
		err := applySecurityParam(&info, p)
		if err != nil {
			warnings = append(warnings, err)
		}
	}

	finalizeSecurityInfo(&info)

	return info, warnings
}

// applySecurityParam folds one param into the record, returning a loud
// warning for malformed security values and nil for clean or ignored params.
//
// PID presence is recorded even when the value is malformed so
// classification stays fail-closed: a malformed secured peer never reads
// as unsecured. Malformed masks keep zero attributes but keep the presence
// flag; malformed property lists keep the list flag with no properties.
func applySecurityParam(info *SecurityInfo, p rtps.Param) error {
	switch p.ID {
	case PIDParticipantSecurityInfo:
		info.HasParticipantSecurityInfo = true
		mask, err := parseSecurityAttributes(p.Value)
		if err != nil {
			return fmt.Errorf("dds: security participant info: %w", err)
		}
		info.ParticipantSecurityAttributes = mask
	case PIDEndpointSecurityInfo:
		info.HasEndpointSecurityInfo = true
		mask, err := parseSecurityAttributes(p.Value)
		if err != nil {
			return fmt.Errorf("dds: security endpoint info: %w", err)
		}
		info.EndpointSecurityAttributes = mask
	case PIDIdentityToken:
		info.IdentityTokenPresent = true
	case PIDPermissionsToken:
		info.PermissionsTokenPresent = true
	case PIDDataTags:
		info.DataTagsPresent = true
	case PIDIdentityStatusToken:
		info.IdentityStatusPresent = true
	case PIDPropertyList:
		info.PropertyListPresent = true
		props, err := ParseSecurityPropertyList(p.Value)
		if err != nil {
			return fmt.Errorf("dds: security property list: %w", err)
		}
		mergeSecurityProperties(info.Properties, props)
	default:
		return nil
	}
	return nil
}

// mergeSecurityProperties keeps first-wins values like the SEDP endpoint parser.
func mergeSecurityProperties(dst, src map[string]string) {
	for key, value := range src {
		if _, seen := dst[key]; !seen {
			dst[key] = value
		}
	}
}

// securityMarkersSeen reports whether any info PID was sighted.
func securityMarkersSeen(info *SecurityInfo) bool {
	return info.HasParticipantSecurityInfo ||
		info.HasEndpointSecurityInfo ||
		info.IdentityTokenPresent ||
		info.PermissionsTokenPresent ||
		info.DataTagsPresent ||
		info.IdentityStatusPresent ||
		info.PropertyListPresent
}

// finalizeSecurityInfo derives presence and posture from markers and properties.
func finalizeSecurityInfo(info *SecurityInfo) {
	seen := securityMarkersSeen(info)
	info.Present = seen || hasSecurityProperty(info.Properties)
	info.Classification = ClassifySecurity(info.Properties, seen)
}

// hasSecurityProperty reports whether any stored key carries the dds.sec. prefix.
func hasSecurityProperty(properties map[string]string) bool {
	for key := range properties {
		if strings.HasPrefix(key, securityPropertyPrefix) {
			return true
		}
	}

	return false
}

// parseSecurityAttributes reads the leading attribute mask from one info value.
//
// The value holds a little endian uint32 attributes mask, optionally
// followed by a plugin mask that is ignored for posture decisions.
// Shorter values fail loudly with the warning sentinel so the caller can
// warn without killing the loop.
func parseSecurityAttributes(value []byte) (uint32, error) {
	if len(value) < 4 {
		return 0, fmt.Errorf("dds: security attributes need 4 have %d: %w", len(value), ErrSecurityPropertyInvalid)
	}

	mask := binary.LittleEndian.Uint32(value[:4])

	return mask, nil
}

// ParseSecurityPropertyList decodes a PropertyQosPolicy value into key-value pairs.
//
// The value is a CDR sequence of Property entries: a uint32 count then,
// per entry, two CDR strings (name, value) plus one boolean propagate
// byte. Only little endian decodes, matching CycloneDDS wire order;
// big-endian values fail loudly with the warning sentinel instead of a
// silent LE-then-BE fallback. Malformed input fails loudly; callers warn
// and continue the discovery loop. Duplicate names keep the first value,
// matching the SEDP first-wins convention.
func ParseSecurityPropertyList(value []byte) (map[string]string, error) {
	props, err := parseSecurityPropertyListEndian(value, true)
	if err != nil {
		return nil, fmt.Errorf("dds: security property list malformed: %w", errors.Join(ErrSecurityPropertyInvalid, err))
	}

	return props, nil
}

// parseSecurityPropertyListEndian decodes a property list in one byte order.
//
// The wire layout is a count followed by (name, value) string pairs with
// NO propagate boolean: CycloneDDS and FastDDS serialize PID 0x0059 as
// bare name/value pairs, proven by live captures. Some encoders append a
// single propagate byte plus pad after each value; the parser accepts
// that shape too by falling back to a propagate skip when the next bytes
// do not validate as a string header. Genuinely malformed entries still
// fail loudly. Trailing bytes past the declared entries fail loudly
// instead of decoding silently.
func parseSecurityPropertyListEndian(value []byte, littleEndian bool) (map[string]string, error) {
	if len(value) < 4 {
		return nil, fmt.Errorf("dds: security property count need 4 have %d: %w", len(value), ErrSecurityPropertyInvalid)
	}

	count := securityUint32(value[:4], littleEndian)
	if count > securityMaxProperties {
		return nil, fmt.Errorf("dds: security property count %d above %d: %w", count, securityMaxProperties, ErrSecurityPropertyInvalid)
	}

	props := make(map[string]string)

	off := 4

	for range count {
		name, next, err := securityReadString(value, off, littleEndian)
		if err != nil {
			return nil, fmt.Errorf("dds: security property name: %w", err)
		}

		val, next, err := securityReadString(value, next, littleEndian)
		if err != nil {
			return nil, fmt.Errorf("dds: security property value: %w", err)
		}

		next = securitySkipPropagateOptional(value, next, littleEndian)

		if _, seen := props[name]; !seen {
			props[name] = val
		}

		off = next
	}

	if off != len(value) {
		return nil, fmt.Errorf("dds: security property list has %d trailing bytes: %w", len(value)-off, ErrSecurityPropertyInvalid)
	}

	return props, nil
}

// securityUint32 reads one uint32 in the given order.
func securityUint32(raw []byte, littleEndian bool) uint32 {
	if littleEndian {
		return binary.LittleEndian.Uint32(raw)
	}

	return binary.BigEndian.Uint32(raw)
}

// securityReadString reads one CDR string (length with NUL plus bytes) at off.
func securityReadString(value []byte, off int, littleEndian bool) (string, int, error) {
	if len(value)-off < 4 {
		return "", 0, fmt.Errorf("dds: security string header need 4 have %d: %w", len(value)-off, ErrSecurityPropertyInvalid)
	}

	length := int(securityUint32(value[off:off+4], littleEndian))

	if length < 1 || length > securityMaxPropertyStringLen+1 {
		return "", 0, fmt.Errorf("dds: security string length %d: %w", length, ErrSecurityPropertyInvalid)
	}

	if len(value)-(off+4) < length {
		return "", 0, fmt.Errorf("dds: security string need %d have %d: %w", length, len(value)-(off+4), ErrSecurityPropertyInvalid)
	}

	raw := value[off+4 : off+4+length]
	if raw[length-1] != 0x00 {
		return "", 0, fmt.Errorf("dds: security string misses NUL terminator: %w", ErrSecurityPropertyInvalid)
	}

	out := string(raw[:length-1])
	if len(out) > securityMaxPropertyStringLen {
		return "", 0, fmt.Errorf("dds: security string length %d above cap: %w", len(out), ErrSecurityPropertyInvalid)
	}

	next := off + 4 + length
	next += (4 - next%4) % 4

	if next > len(value) {
		return "", 0, fmt.Errorf("dds: security string pad overruns input: %w", ErrSecurityPropertyInvalid)
	}

	return out, next, nil
}

// securitySkipPropagateOptional advances past an optional CDR boolean.
//
// The standard wire layout has no propagate byte, so when the next bytes
// already validate as a string header the offset passes through
// unchanged. Otherwise one boolean byte plus alignment pad is skipped,
// accepting encoders that append propagate. When neither shape validates,
// the offset passes through unchanged and the next read fails loudly at
// the true error site.
func securitySkipPropagateOptional(value []byte, off int, littleEndian bool) int {
	if off == len(value) {
		return off
	}

	if securityValidStringHeader(value, off, littleEndian) {
		return off
	}

	next := off + 1
	next += (4 - next%4) % 4

	if next > len(value) {
		return off
	}

	if next == len(value) || securityValidStringHeader(value, next, littleEndian) {
		return next
	}

	return off
}

// securityValidStringHeader reports whether a CDR string header at off is
// well formed: four length bytes available, length in range, the full
// NUL-terminated body present. It never fails, only probes.
func securityValidStringHeader(value []byte, off int, littleEndian bool) bool {
	_, _, err := securityReadString(value, off, littleEndian)

	return err == nil
}

// CheckProtectedDiscovery rejects peers that demand protected discovery.
//
// Unsecured and compatible-unknown peers pass with nil so their paths keep
// flowing. Peers classified as requiring crypto fail loudly with an error
// that errors.Is reports as ErrSecuredDiscoveryRequiresCrypto.
func CheckProtectedDiscovery(info SecurityInfo) error {
	if info.Classification != SecurityRequiresCrypto {
		return nil
	}

	return fmt.Errorf("dds: peer demands protected discovery (class %s): %w", info.Classification, ErrSecuredDiscoveryRequiresCrypto)
}

// DropProtectedSubmessage drops one protected submessage with no crypto context.
//
// It counts the drop under DropReasonSecuredTransformUnavailable on the
// receiver so dashboards see the exact cause, then returns a loud error
// naming the transform kind that errors.Is reports as
// ErrSecuredTransformUnavailable. It never attempts decryption. A nil
// receiver is an explicit error, not a silent skip.
func DropProtectedSubmessage(r *DataReceiver, id byte) error {
	if r == nil {
		return fmt.Errorf("dds: drop protected %s nil receiver: %w", SecuredTransformName(id), ErrNilReceiver)
	}

	r.countDropReason(DropReasonSecuredTransformUnavailable)

	return fmt.Errorf("dds: submessage %s requires crypto, dropping without decode: %w", SecuredTransformName(id), ErrSecuredTransformUnavailable)
}

// HandleProtectedSubmessage drops one raw submessage when it is crypto-wrapped.
//
// It returns handled false with nil error for ordinary submessages so the
// normal path keeps flowing untouched. Protected kinds count one
// secured-transform-unavailable drop and return a loud error. This is the
// one-line hook for the datapath demux.
//
// INTEGRATION state: pure tolerance helpers are done here, including PID
// presence recorded even when malformed for fail-closed classification and
// strict trailing-length checks on property lists. Wiring stays out:
//   - spdp.go announcementFromData: after collectParams, call
//     ExtractSecurityInfo and carry SecurityInfo on the participant record;
//     on CheckProtectedDiscovery error, log loudly and keep the unsecured path.
//   - sedp.go sedpEndpointFields: after byID assembly, call
//     ExtractSecurityInfo on inline plus payload params and carry
//     SecurityInfo on SEDPEndpoint.
//   - backend.go routeBuiltinSub/dispatchRTPS: before DecodeSubmessage,
//     call HandleProtectedSubmessage and continue when handled is true.
//   - sdk/cli config plumbing: construct SecurityConfig from flags or
//     files, call Validate, pass into backend construction. No call site
//     exists yet.
func HandleProtectedSubmessage(r *DataReceiver, sub rtps.RawSubmessage) (bool, error) {
	if !IsProtectedSubmessageID(sub.ID) {
		return false, nil
	}

	err := DropProtectedSubmessage(r, sub.ID)
	if err != nil {
		return true, err
	}

	return true, nil
}
