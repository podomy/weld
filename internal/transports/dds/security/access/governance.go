// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package access holds the Phase 1 governance parser and evaluator.
//
// It parses the signed governance XML shape from DDS Security 10.4.1.2
// into structural records, selects the first matching domain rule, and
// checks protection kind and algorithm vocabularies. Signature
// verification is stubbed to REJECT-ALL: stdlib has no XML-DSIG, the
// envelope verifier lands in Phase 4, and the exact signature envelope
// format is TBD against the specification at that time. No parse
// success here authorizes anything; the coordinator denies every
// protected operation until authentication and verification exist.
package access

import (
	"crypto/x509"
	"encoding/xml"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
)

// Sentinel errors for governance failures.
var (
	// ErrInvalidGovernance reports a malformed governance document.
	ErrInvalidGovernance = errors.New("security: invalid governance document")
	// ErrUnknownAlgorithm reports an algorithm outside the clause 8 vocabulary.
	ErrUnknownAlgorithm = errors.New("security: unknown algorithm")
	// ErrSignatureUnverified reports that no document signature verified.
	ErrSignatureUnverified = errors.New("security: signature unverified")
)

// Protection kind vocabulary from 10.4.1.2.
const (
	// ProtectionNone disables protection for the channel.
	ProtectionNone = "NONE"
	// ProtectionSign signs the channel without encrypting it.
	ProtectionSign = "SIGN"
	// ProtectionEncrypt encrypts the channel.
	ProtectionEncrypt = "ENCRYPT"
	// ProtectionSignWithOrigin signs with origin authentication.
	ProtectionSignWithOrigin = "SIGN_WITH_ORIGIN_AUTHENTICATION"
	// ProtectionEncryptWithOrigin encrypts with origin authentication.
	ProtectionEncryptWithOrigin = "ENCRYPT_WITH_ORIGIN_AUTHENTICATION"
)

// ValidProtectionKind reports whether kind is in the governance vocabulary.
func ValidProtectionKind(kind string) bool {
	switch kind {
	case ProtectionNone,
		ProtectionSign,
		ProtectionEncrypt,
		ProtectionSignWithOrigin,
		ProtectionEncryptWithOrigin:
		return true
	default:
		return false
	}
}

// RequiresOriginAuth reports whether governance demands origin authentication.
//
// SIGN_WITH_ORIGIN_AUTHENTICATION and ENCRYPT_WITH_ORIGIN_AUTHENTICATION
// need origin identity bound into the crypto AAD, which the transform and
// submessage paths do not wire yet. Callers fail closed on true instead
// of silently downgrading to plain SIGN or ENCRYPT. A nil topic rule
// checks domain channels only.
func RequiresOriginAuth(rule *DomainRule, topicRule *TopicRule) bool {
	if rule == nil {
		return false
	}

	if rule.DiscoveryProtection == ProtectionSignWithOrigin ||
		rule.DiscoveryProtection == ProtectionEncryptWithOrigin ||
		rule.LivelinessProtection == ProtectionSignWithOrigin ||
		rule.LivelinessProtection == ProtectionEncryptWithOrigin ||
		rule.RTPSProtection == ProtectionSignWithOrigin ||
		rule.RTPSProtection == ProtectionEncryptWithOrigin {
		return true
	}

	if topicRule == nil {
		return false
	}

	return topicRule.MetadataProtection == ProtectionSignWithOrigin ||
		topicRule.MetadataProtection == ProtectionEncryptWithOrigin ||
		topicRule.DataProtection == ProtectionSignWithOrigin ||
		topicRule.DataProtection == ProtectionEncryptWithOrigin
}

// KnownCryptoAlgorithm reports whether name is in the clause 8 vocabulary.
//
// It covers the signature, key agreement, and symmetric names quoted in
// the design: RSASSA and ECDSA suites, DHE and ECDHE-CEUM suites, and
// AES-GCM sizes. Anything else fails closed per INV-7 and INV-8.
func KnownCryptoAlgorithm(name string) bool {
	switch name {
	case "RSASSA-PSS-MGF1SHA256+2048+SHA256",
		"RSASSA-PKCS1-V1_5+2048+SHA256",
		"ECDSA+P256+SHA256",
		"ECDSA+P384+SHA384",
		"DHE+MODP-2048-256",
		"ECDHE-CEUM+P256",
		"ECDHE-CEUM+P384",
		"AES128+GCM",
		"AES256+GCM":
		return true
	default:
		return false
	}
}

// GovernanceDoc is the parsed structural form of one governance document.
type GovernanceDoc struct {
	// XMLName pins the root element to dds.
	XMLName xml.Name `xml:"dds"`
	// Rules holds domain rules in evaluation order. First match wins.
	Rules []DomainRule `xml:"domain_access_rules>domain_rule"`
}

// DomainRule holds one domain rule with its topic rules.
type DomainRule struct {
	// Domains selects the domains this rule governs.
	Domains Domains `xml:"domains"`
	// AllowUnauthenticated permits unauthenticated participants.
	AllowUnauthenticated bool `xml:"allow_unauthenticated_participants"`
	// EnableJoinAccessControl gates joining on access control.
	EnableJoinAccessControl bool `xml:"enable_join_access_control"`
	// EnableKeyRevision requests key revision on membership change.
	EnableKeyRevision bool `xml:"enable_key_revision"`
	// DiscoveryProtection guards discovery traffic.
	DiscoveryProtection string `xml:"discovery_protection_kind"`
	// LivelinessProtection guards liveliness traffic.
	LivelinessProtection string `xml:"liveliness_protection_kind"`
	// RTPSProtection guards whole RTPS messages.
	RTPSProtection string `xml:"rtps_protection_kind"`
	// AllowedAlgorithms lists permitted crypto algorithms.
	AllowedAlgorithms []string `xml:"allowed_crypto_algorithms>algorithm"`
	// TopicRules holds topic rules in evaluation order.
	TopicRules []TopicRule `xml:"topic_access_rules>topic_rule"`
}

// Domains selects domains by id, range, or tag.
type Domains struct {
	// IDs lists exact domain ids.
	IDs []uint32 `xml:"id"`
	// Ranges lists inclusive domain id ranges.
	Ranges []IDRange `xml:"id_range"`
	// Tags lists exact domain tags.
	Tags []string `xml:"tag"`
	// TagExpressions lists tag fnmatch expressions.
	TagExpressions []string `xml:"tag_expression"`
}

// IDRange is one inclusive domain id interval.
type IDRange struct {
	// Min is the first domain id in the range.
	Min uint32 `xml:"min"`
	// Max is the last domain id in the range.
	Max uint32 `xml:"max"`
}

// TopicRule holds one topic access rule.
type TopicRule struct {
	// Expression is the topic fnmatch expression.
	Expression string `xml:"topic_expression"`
	// EnableDiscovery guards discovery for matching topics.
	EnableDiscovery bool `xml:"enable_discovery_protection"`
	// EnableLiveliness guards liveliness for matching topics.
	EnableLiveliness bool `xml:"enable_liveliness_protection"`
	// EnableReadAccess gates read access for matching topics.
	EnableReadAccess bool `xml:"enable_read_access_control"`
	// EnableWriteAccess gates write access for matching topics.
	EnableWriteAccess bool `xml:"enable_write_access_control"`
	// MetadataProtection guards submessage metadata.
	MetadataProtection string `xml:"metadata_protection_kind"`
	// DataProtection guards payload data.
	DataProtection string `xml:"data_protection_kind"`
}

// ParseGovernance parses and structurally validates one governance document.
//
// It uses stdlib encoding/xml only. Unknown algorithms and unknown
// protection kinds fail closed. Signature verification is out of scope:
// see VerifyGovernanceSignature.
func ParseGovernance(data []byte) (*GovernanceDoc, error) {
	var doc GovernanceDoc

	err := xml.Unmarshal(data, &doc)
	if err != nil {
		return nil, fmt.Errorf("security: parse governance xml: %w", errors.Join(err, ErrInvalidGovernance))
	}

	err = validateGovernance(&doc)
	if err != nil {
		return nil, err
	}

	return &doc, nil
}

// validateGovernance checks structural rules on a parsed document.
func validateGovernance(doc *GovernanceDoc) error {
	if len(doc.Rules) == 0 {
		return fmt.Errorf("security: governance needs at least one domain rule: %w", ErrInvalidGovernance)
	}

	for i := range doc.Rules {
		err := validateDomainRule(&doc.Rules[i])
		if err != nil {
			return fmt.Errorf("security: governance rule %d: %w", i, err)
		}
	}

	return nil
}

// validateDomainRule checks one domain rule for scope and vocabulary.
func validateDomainRule(rule *DomainRule) error {
	err := validateDomainScope(&rule.Domains)
	if err != nil {
		return err
	}

	err = validateProtectionKinds(rule.DiscoveryProtection, rule.LivelinessProtection, rule.RTPSProtection)
	if err != nil {
		return err
	}

	for _, name := range rule.AllowedAlgorithms {
		if !KnownCryptoAlgorithm(name) {
			return fmt.Errorf("security: unknown allowed algorithm %q: %w", name, ErrUnknownAlgorithm)
		}
	}

	if len(rule.TopicRules) == 0 {
		return fmt.Errorf("security: domain rule needs at least one topic rule: %w", ErrInvalidGovernance)
	}

	for i := range rule.TopicRules {
		err := validateTopicRule(&rule.TopicRules[i])
		if err != nil {
			return fmt.Errorf("security: topic rule %d: %w", i, err)
		}
	}

	return nil
}

// validateDomainScope checks domain selectors for presence and shape.
func validateDomainScope(domains *Domains) error {
	if len(domains.IDs) == 0 && len(domains.Ranges) == 0 && len(domains.Tags) == 0 && len(domains.TagExpressions) == 0 {
		return fmt.Errorf("security: domain rule needs at least one domain selector: %w", ErrInvalidGovernance)
	}

	for _, r := range domains.Ranges {
		if r.Max != 0 && r.Min > r.Max {
			return fmt.Errorf("security: domain range %d above %d: %w", r.Min, r.Max, ErrInvalidGovernance)
		}
	}

	for _, expr := range domains.TagExpressions {
		_, err := path.Match(expr, "")
		if err != nil {
			return fmt.Errorf("security: bad tag expression %q: %w", expr, errors.Join(err, ErrInvalidGovernance))
		}
	}

	return nil
}

// validateProtectionKinds checks three channel protection kinds.
func validateProtectionKinds(discovery, liveliness, rtps string) error {
	for _, kind := range []string{discovery, liveliness, rtps} {
		if !ValidProtectionKind(kind) {
			return fmt.Errorf("security: unknown protection kind %q: %w", kind, ErrInvalidGovernance)
		}
	}

	return nil
}

// validateTopicRule checks one topic rule for scope and vocabulary.
func validateTopicRule(rule *TopicRule) error {
	if strings.TrimSpace(rule.Expression) == "" {
		return fmt.Errorf("security: topic rule needs an expression: %w", ErrInvalidGovernance)
	}

	_, err := path.Match(rule.Expression, "")
	if err != nil {
		return fmt.Errorf("security: bad topic expression %q: %w", rule.Expression, errors.Join(err, ErrInvalidGovernance))
	}

	err = validateProtectionKinds(rule.MetadataProtection, rule.DataProtection, ProtectionNone)
	if err != nil {
		return err
	}

	return nil
}

// DomainsMatch reports whether a domain rule scope covers a domain and tag.
//
// Exact ids and inclusive ranges match by number, tags match exactly,
// and tag expressions match by fnmatch. When a rule carries both
// number and tag selectors both must match: a tag-scoped rule never
// opens an unlisted domain, and a domain-scoped rule with tags never
// opens an untagged join. A scope with only number selectors matches
// any tag; a scope with only tag selectors matches any domain. A
// scope with no selectors matches nothing.
func DomainsMatch(domains *Domains, domainID uint32, tag string) bool {
	if domains == nil {
		return false
	}

	hasNumber := len(domains.IDs) != 0 || len(domains.Ranges) != 0
	hasTag := len(domains.Tags) != 0 || len(domains.TagExpressions) != 0

	switch {
	case hasNumber && hasTag:
		return matchDomainNumber(domains, domainID) && matchDomainTag(domains, tag)
	case hasNumber:
		return matchDomainNumber(domains, domainID)
	case hasTag:
		return matchDomainTag(domains, tag)
	default:
		return false
	}
}

// matchDomainNumber reports exact id or inclusive range coverage.
func matchDomainNumber(domains *Domains, domainID uint32) bool {
	if slices.Contains(domains.IDs, domainID) {
		return true
	}

	for _, r := range domains.Ranges {
		if r.Max == 0 {
			if r.Min <= domainID {
				return true
			}

			continue
		}

		if r.Min <= domainID && domainID <= r.Max {
			return true
		}
	}

	return false
}

// matchDomainTag reports tag or tag expression coverage.
func matchDomainTag(domains *Domains, tag string) bool {
	if len(domains.Tags) == 0 && len(domains.TagExpressions) == 0 {
		return len(domains.IDs) == 0 && len(domains.Ranges) == 0
	}

	if slices.Contains(domains.Tags, tag) {
		return true
	}

	for _, expr := range domains.TagExpressions {
		ok, err := path.Match(expr, tag)
		if err != nil {
			continue
		}

		if ok {
			return true
		}
	}

	return false
}

// FindDomainRule returns the first rule covering a domain and tag.
//
// Overlapping rules never merge: first match wins per 10.4.1.2.5. A nil
// result means deny per INV-1. Signature state plays no part here; the
// caller enforces verification before honoring the result.
func FindDomainRule(doc *GovernanceDoc, domainID uint32, tag string) *DomainRule {
	if doc == nil {
		return nil
	}

	for i := range doc.Rules {
		if DomainsMatch(&doc.Rules[i].Domains, domainID, tag) {
			return &doc.Rules[i]
		}
	}

	return nil
}

// FindTopicRule returns the first topic rule matching a topic name.
//
// Patterns use fnmatch; a lone star matches every topic for peer
// interop. A malformed pattern never matches so traffic fails closed.
// A nil result means deny per INV-1.
func FindTopicRule(rule *DomainRule, topic string) *TopicRule {
	if rule == nil {
		return nil
	}

	for i := range rule.TopicRules {
		expr := rule.TopicRules[i].Expression
		if expr == "*" {
			return &rule.TopicRules[i]
		}

		ok, err := path.Match(expr, topic)
		if err != nil {
			continue
		}

		if ok {
			return &rule.TopicRules[i]
		}
	}

	return nil
}

// VerifyGovernanceSignature rejects every document at Phase 1.
//
// There is no stdlib XML-DSIG, so the envelope verifier is new code
// that lands in Phase 4 with negative tests. The exact signature
// envelope format is TBD against the specification at that time. Until
// then every document is unverified per INV-1 and nothing may pass
// closed-loop on governance content.
func VerifyGovernanceSignature() error {
	return fmt.Errorf("security: governance signature cannot verify at Phase 1, envelope TBD: %w", ErrSignatureUnverified)
}

// VerifyGovernanceDocument verifies one signed governance document.
//
// It accepts the peer-native S/MIME envelope plus the documented
// excise-bytes subset in dsig.go. Full C14N, prefixed signatures,
// multiple references, and any other method refuse loudly.
func VerifyGovernanceDocument(data []byte, caPool *x509.CertPool) (*GovernanceDoc, error) {
	payload, err := verifySignedPayload(data, caPool)
	if err != nil {
		return nil, err
	}

	doc, err := ParseGovernance(payload)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// VerifyGovernancePEM verifies one signed document against PEM CA bytes.
func VerifyGovernancePEM(data, caPEM []byte) (*GovernanceDoc, error) {
	pool, err := dsigTrustPool(caPEM)
	if err != nil {
		return nil, err
	}

	doc, err := VerifyGovernanceDocument(data, pool)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// AuthorizeJoin composes domain selection, unauthenticated policy, and
// the signature stub for one join attempt.
//
// It always denies at Phase 1: either no rule matches, the peer is
// unauthenticated against a closed rule, or the signature stub rejects.
// Phase 4 keeps this shape and lets verified documents pass.
func AuthorizeJoin(doc *GovernanceDoc, domainID uint32, tag string, authenticated bool) error {
	rule := FindDomainRule(doc, domainID, tag)
	if rule == nil {
		return fmt.Errorf("security: join domain %d tag %q matches no rule: %w", domainID, tag, ErrInvalidGovernance)
	}

	if !authenticated && !rule.AllowUnauthenticated {
		return fmt.Errorf("security: join domain %d refuses unauthenticated peer: %w", domainID, ErrSignatureUnverified)
	}

	err := VerifyGovernanceSignature()
	if err != nil {
		return fmt.Errorf("security: join domain %d: %w", domainID, err)
	}

	return nil
}
