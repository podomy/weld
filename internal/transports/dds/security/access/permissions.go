// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package access

import (
	"crypto/x509"
	"encoding/xml"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/types"
)

// Sentinel errors for permissions failures.
var (
	// ErrInvalidPermissions reports a malformed permissions document.
	ErrInvalidPermissions = errors.New("security: invalid permissions document")
	// ErrNotAllowedBySecurity reports a denial under DDS Security 7.3.25.
	ErrNotAllowedBySecurity = errors.New("security: not allowed by security")
)

// PermissionsDoc is the parsed structural form of one permissions document.
//
// It accepts the legacy Weld shape with root permissions plus the
// peer-native OMG shape with root dds containing permissions. Grants
// bind subject names to scoped rules with validity windows.
type PermissionsDoc struct {
	// XMLName pins the root element to permissions.
	XMLName xml.Name `xml:"permissions"`
	// Grants holds permission grants in document order.
	Grants []Grant `xml:"grant"`
}

// Grant binds one subject name to allow and deny rules.
type Grant struct {
	// Name is the grant label, empty when the document omits it.
	Name string `xml:"name,attr"`
	// SubjectName is the certificate subject this grant applies to.
	SubjectName string `xml:"subject_name"`
	// Validity bounds the grant lifetime.
	Validity Validity `xml:"validity"`
	// AllowRules holds allow rules in evaluation order.
	AllowRules []Rule `xml:"allow_rule"`
	// DenyRules holds deny rules in evaluation order.
	DenyRules []Rule `xml:"deny_rule"`
	// Default selects ALLOW or DENY when no rule matches, empty means DENY.
	Default string `xml:"default"`
}

// Validity bounds one grant with not-before and not-after timestamps.
type Validity struct {
	// NotBefore is the RFC3339 start of validity.
	NotBefore string `xml:"not_before"`
	// NotAfter is the RFC3339 end of validity.
	NotAfter string `xml:"not_after"`
}

// Rule scopes one allow or deny rule over domains, topics, partitions,
// and data tags. Flat fields carry the legacy Weld shape; Publish,
// Subscribe, and Relay carry the peer-native OMG criteria which
// normalize into the flat fields at parse time.
type Rule struct {
	// Domains lists in-scope domain ids.
	Domains []uint32 `xml:"domains>id"`
	// DomainRanges lists in-scope domain ranges, Max zero is open.
	DomainRanges []IDRange `xml:"domains>id_range"`
	// Topics lists in-scope topic expressions.
	Topics []string `xml:"topics>topic"`
	// Partitions lists in-scope partition names.
	Partitions []string `xml:"partitions>partition"`
	// DataTags lists in-scope data tag values.
	DataTags []string `xml:"data_tags>tag"`
	// Publish holds publish criteria for the OMG shape.
	Publish []permCriteria `xml:"publish"`
	// Subscribe holds subscribe criteria for the OMG shape.
	Subscribe []permCriteria `xml:"subscribe"`
	// Relay holds relay criteria for the OMG shape.
	Relay []permCriteria `xml:"relay"`
}

// permCriteria scopes topics, partitions, plus tags in one OMG block.
type permCriteria struct {
	// Topics lists topic expressions.
	Topics []string `xml:"topics>topic"`
	// Partitions lists partition expressions.
	Partitions []string `xml:"partitions>partition"`
	// Tags lists data tag pairs.
	Tags []permTag `xml:"data_tags>tag"`
}

// permTag holds one data tag in plain or name plus value form.
type permTag struct {
	// Name holds the tag name for pair form.
	Name string `xml:"name"`
	// Value holds the tag value for pair form.
	Value string `xml:"value"`
	// Text holds chardata for plain form.
	Text string `xml:",chardata"`
}

// tagString renders one tag to a plain string for matching.
func (t permTag) tagString() string {
	name := strings.TrimSpace(t.Name)
	value := strings.TrimSpace(t.Value)
	text := strings.TrimSpace(t.Text)

	if name != "" && value != "" {
		return name + "=" + value
	}

	if value != "" {
		return value
	}

	if name != "" {
		return name
	}

	return text
}

// ParsePermissions parses and structurally validates one permissions document.
//
// It accepts root permissions plus root dds containing permissions.
// Time windows require RFC3339; bare timestamps without an offset are
// refused. OMG publish, subscribe, and relay criteria normalize into
// flat topic, partition, and tag lists with union semantics.
func ParsePermissions(data []byte) (*PermissionsDoc, error) {
	var doc PermissionsDoc

	err := xml.Unmarshal(data, &doc)
	if err != nil || len(doc.Grants) == 0 {
		wrapped, werr := parseDDSPermissions(data)
		if werr == nil {
			doc = *wrapped
		} else if err != nil {
			return nil, fmt.Errorf("security: parse permissions xml: %w", errors.Join(err, ErrInvalidPermissions))
		}
	}

	normalizePermissions(&doc)

	err = validatePermissions(&doc)
	if err != nil {
		return nil, err
	}

	return &doc, nil
}

// ddsPermissionsRoot accepts the peer-native dds wrapper root.
type ddsPermissionsRoot struct {
	// XMLName pins the root element to dds.
	XMLName xml.Name `xml:"dds"`
	// Permissions holds the inner permissions block.
	Permissions PermissionsDoc `xml:"permissions"`
}

// parseDDSPermissions parses the dds wrapped permissions shape.
func parseDDSPermissions(data []byte) (*PermissionsDoc, error) {
	var root ddsPermissionsRoot

	err := xml.Unmarshal(data, &root)
	if err != nil {
		return nil, fmt.Errorf("security: parse dds permissions xml: %w", errors.Join(err, ErrInvalidPermissions))
	}

	if len(root.Permissions.Grants) == 0 {
		return nil, fmt.Errorf("security: dds permissions hold no grants: %w", ErrInvalidPermissions)
	}

	return &root.Permissions, nil
}

// normalizePermissions keeps OMG criteria separate from flat rule fields.
//
// Publish, subscribe, and relay criteria stay directional per 9.4.2.5:
// flattening them into one topic list would let a subscribe-only grant
// authorize publish and vice versa. Legacy flat rules carry no OMG
// criteria and evaluate for all directions.
func normalizePermissions(doc *PermissionsDoc) {
	if doc == nil {
		return
	}
}

// ruleHasCriteriaacc8 reports whether one rule carries OMG direction scope.
func ruleHasCriteriaacc8(rule *Rule) bool {
	if rule == nil {
		return false
	}

	return len(rule.Publish) != 0 || len(rule.Subscribe) != 0 || len(rule.Relay) != 0
}

// criteriaTagsacc8 renders one OMG criteria tag list to plain strings.
func criteriaTagsacc8(criteria permCriteria) []string {
	var out []string

	for _, tag := range criteria.Tags {
		out = append(out, tag.tagString())
	}

	return out
}

// criteriaTopicMatchesacc8 reports fnmatch coverage with empty meaning all.
func criteriaTopicMatchesacc8(criteria permCriteria, topic string) bool {
	if len(criteria.Topics) == 0 {
		return true
	}

	for _, expr := range criteria.Topics {
		if expr == "*" {
			return true
		}

		ok, err := path.Match(expr, topic)
		if err != nil {
			continue
		}

		if ok {
			return true
		}
	}

	return false
}

// criteriaMatchesacc8 reports full topic plus partition plus tag coverage.
func criteriaMatchesacc8(criteria permCriteria, topic string, partitions, tags []string) bool {
	if !criteriaTopicMatchesacc8(criteria, topic) {
		return false
	}

	if len(criteria.Partitions) != 0 && !partitionsOverlap(criteria.Partitions, partitions) {
		return false
	}

	flat := criteriaTagsacc8(criteria)
	if len(flat) != 0 && !tagsOverlap(flat, tags) {
		return false
	}

	return true
}

// ruleMatchesPublishacc8 reports directional publish coverage.
func ruleMatchesPublishacc8(rule *Rule, topic string, partitions, tags []string) bool {
	for _, criteria := range rule.Publish {
		if criteriaMatchesacc8(criteria, topic, partitions, tags) {
			return true
		}
	}

	return false
}

// ruleMatchesSubscribeacc8 reports directional subscribe coverage.
func ruleMatchesSubscribeacc8(rule *Rule, topic string, partitions, tags []string) bool {
	for _, criteria := range rule.Subscribe {
		if criteriaMatchesacc8(criteria, topic, partitions, tags) {
			return true
		}
	}

	return false
}

// ruleMatchesRelayacc8 reports directional relay coverage.
func ruleMatchesRelayacc8(rule *Rule, topic string, partitions, tags []string) bool {
	for _, criteria := range rule.Relay {
		if criteriaMatchesacc8(criteria, topic, partitions, tags) {
			return true
		}
	}

	return false
}

// ruleMatchesAnyDirectionacc8 reports union coverage for legacy callers.
func ruleMatchesAnyDirectionacc8(rule *Rule, topic string, partitions, tags []string) bool {
	if ruleMatchesPublishacc8(rule, topic, partitions, tags) {
		return true
	}

	if ruleMatchesSubscribeacc8(rule, topic, partitions, tags) {
		return true
	}

	return ruleMatchesRelayacc8(rule, topic, partitions, tags)
}

// ruleTopicCoversPublishacc8 reports publish topic coverage ignoring partitions and tags.
func ruleTopicCoversPublishacc8(rule *Rule, topic string) bool {
	for _, criteria := range rule.Publish {
		if criteriaTopicMatchesacc8(criteria, topic) {
			return true
		}
	}

	return false
}

// ruleTopicCoversSubscribeacc8 reports subscribe topic coverage ignoring partitions and tags.
func ruleTopicCoversSubscribeacc8(rule *Rule, topic string) bool {
	for _, criteria := range rule.Subscribe {
		if criteriaTopicMatchesacc8(criteria, topic) {
			return true
		}
	}

	return false
}

// ruleTopicCoversRelayacc8 reports relay topic coverage ignoring partitions and tags.
func ruleTopicCoversRelayacc8(rule *Rule, topic string) bool {
	for _, criteria := range rule.Relay {
		if criteriaTopicMatchesacc8(criteria, topic) {
			return true
		}
	}

	return false
}

// allowPublishTopicCoveredacc8 reports publish topic coverage for default scoping.
func allowPublishTopicCoveredacc8(grant *Grant, domain uint32, topic string) bool {
	return allowTopicCoveredWith(grant, domain, topic, ruleTopicCoversPublishacc8)
}

// allowSubscribeTopicCoveredacc8 reports subscribe topic coverage for default scoping.
func allowSubscribeTopicCoveredacc8(grant *Grant, domain uint32, topic string) bool {
	return allowTopicCoveredWith(grant, domain, topic, ruleTopicCoversSubscribeacc8)
}

// allowRelayTopicCoveredacc8 reports relay topic coverage for default scoping.
func allowRelayTopicCoveredacc8(grant *Grant, domain uint32, topic string) bool {
	return allowTopicCoveredWith(grant, domain, topic, ruleTopicCoversRelayacc8)
}

// validatePermissions checks structural rules on a parsed document.
func validatePermissions(doc *PermissionsDoc) error {
	if len(doc.Grants) == 0 {
		return fmt.Errorf("security: permissions need at least one grant: %w", ErrInvalidPermissions)
	}

	for i := range doc.Grants {
		err := validateGrant(&doc.Grants[i])
		if err != nil {
			return fmt.Errorf("security: permissions grant %d: %w", i, err)
		}
	}

	return nil
}

// validateGrant checks subject, window, and rule scope for one grant.
func validateGrant(grant *Grant) error {
	if strings.TrimSpace(grant.SubjectName) == "" {
		return fmt.Errorf("security: grant needs a subject name: %w", ErrInvalidPermissions)
	}

	err := validateValidity(&grant.Validity)
	if err != nil {
		return err
	}

	if strings.TrimSpace(grant.Default) != "" && grant.Default != "ALLOW" && grant.Default != "DENY" {
		return fmt.Errorf("security: grant default %q must be ALLOW or DENY: %w", grant.Default, ErrInvalidPermissions)
	}

	if len(grant.AllowRules) == 0 && len(grant.DenyRules) == 0 {
		return fmt.Errorf("security: grant for %q needs at least one rule: %w", grant.SubjectName, ErrInvalidPermissions)
	}

	for i := range grant.AllowRules {
		err := validateRule(&grant.AllowRules[i])
		if err != nil {
			return fmt.Errorf("security: allow rule %d: %w", i, err)
		}
	}

	for i := range grant.DenyRules {
		err := validateRule(&grant.DenyRules[i])
		if err != nil {
			return fmt.Errorf("security: deny rule %d: %w", i, err)
		}
	}

	return nil
}

// validateValidity checks that a window parses and orders correctly.
func validateValidity(window *Validity) error {
	before, err := parsePermissionTime(window.NotBefore)
	if err != nil {
		return fmt.Errorf("security: bad not_before %q: %w", window.NotBefore, errors.Join(err, ErrInvalidPermissions))
	}

	after, err := parsePermissionTime(window.NotAfter)
	if err != nil {
		return fmt.Errorf("security: bad not_after %q: %w", window.NotAfter, errors.Join(err, ErrInvalidPermissions))
	}

	if after.Before(before) {
		return fmt.Errorf("security: validity window ends before it starts: %w", ErrInvalidPermissions)
	}

	return nil
}

// parsePermissionTime parses RFC3339 timestamps only.
//
// Bare local timestamps without an offset are refused: without zone
// information the window is ambiguous and fail-closed per INV-1
// requires rejecting the grant rather than guessing UTC.
func parsePermissionTime(raw string) (time.Time, error) {
	trimmed := strings.TrimSpace(raw)

	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return time.Time{}, fmt.Errorf("security: parse permission time %q: %w", trimmed, errors.Join(err, ErrInvalidPermissions))
	}

	return parsed, nil
}

// validateRule checks that one rule scopes at least one dimension.
func validateRule(rule *Rule) error {
	hasScope := len(rule.Domains) != 0 || len(rule.DomainRanges) != 0 || len(rule.Topics) != 0
	hasScope = hasScope || len(rule.Partitions) != 0 || len(rule.DataTags) != 0
	hasScope = hasScope || len(rule.Publish) != 0 || len(rule.Subscribe) != 0 || len(rule.Relay) != 0

	if !hasScope {
		return fmt.Errorf("security: rule needs at least one scope: %w", ErrInvalidPermissions)
	}

	for _, topic := range rule.Topics {
		if strings.TrimSpace(topic) == "" {
			return fmt.Errorf("security: rule topic is empty: %w", ErrInvalidPermissions)
		}
	}

	return nil
}

// VerifyPermissionsSignature rejects every document at Phase 1.
//
// Same stub rationale as VerifyGovernanceSignature: no stdlib XML-DSIG,
// envelope verifier lands in Phase 4, envelope format TBD. Until then
// every permissions document is unverified per INV-1.
func VerifyPermissionsSignature() error {
	return fmt.Errorf("security: permissions signature cannot verify at Phase 1, envelope TBD: %w", ErrSignatureUnverified)
}

// VerifyPermissionsDocument verifies one signed permissions document.
//
// It accepts the peer-native S/MIME envelope plus the documented
// excise-bytes subset with the same fail-closed boundary as governance.
func VerifyPermissionsDocument(data []byte, caPool *x509.CertPool) (*PermissionsDoc, error) {
	payload, err := verifySignedPayload(data, caPool)
	if err != nil {
		return nil, err
	}

	doc, err := ParsePermissions(payload)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// VerifyPermissionsPEM verifies one signed document against PEM CA bytes.
func VerifyPermissionsPEM(data, caPEM []byte) (*PermissionsDoc, error) {
	pool, err := dsigTrustPool(caPEM)
	if err != nil {
		return nil, err
	}

	doc, err := VerifyPermissionsDocument(data, pool)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// grantClockSkewacc9 bounds validity window tolerance for clock drift.
//
// Peers compare grant windows against local time; a small skew allowance
// prevents flapping when clocks differ by seconds. Windows outside the
// skew still deny fail-closed.
const grantClockSkewacc9 = 5 * time.Minute

// GrantForSubject returns the grant for one subject with a live window.
//
// Validity compares against now with clock-skew allowance. Expired or
// not-yet-valid grants outside the skew are skipped. A nil result means
// deny per INV-1.
func GrantForSubject(doc *PermissionsDoc, subject string, now time.Time) *Grant {
	if doc == nil {
		return nil
	}

	for i := range doc.Grants {
		grant := &doc.Grants[i]
		if grant.SubjectName != subject {
			continue
		}

		before, err := parsePermissionTime(grant.Validity.NotBefore)
		if err != nil {
			continue
		}

		after, err := parsePermissionTime(grant.Validity.NotAfter)
		if err != nil {
			continue
		}

		if now.Before(before.Add(-grantClockSkewacc9)) || now.After(after.Add(grantClockSkewacc9)) {
			continue
		}

		return grant
	}

	return nil
}

// CoversDomain reports whether a grant scopes one domain.
//
// Deprecated: legacy union tolerant. It ignores partitions and tags,
// so tag-gated grants appear covered even with nil scope. New code
// must use CoversDomainacc8Scoped with explicit scope; this shim
// passes empty scope explicitly and stays for backward compatibility.
// Only allow rules confer coverage: deny rules never grant access, so
// a deny-only grant covers nothing, and a grant with no allow rules
// covers nothing. An empty domain scope inside one allow rule still
// means all domains.
func CoversDomain(grant *Grant, domain uint32) bool {
	if grant == nil {
		return false
	}

	for i := range grant.AllowRules {
		if ruleCoversDomain(&grant.AllowRules[i], domain) {
			return true
		}
	}

	return false
}

// scopedPartitionsInList reports scoped plus matched for one criteria list.
func scopedPartitionsInList(criterias []permCriteria, partitions []string) (bool, bool) {
	scoped := false

	for _, criteria := range criterias {
		if len(criteria.Partitions) == 0 {
			continue
		}

		scoped = true

		if partitionsOverlap(criteria.Partitions, partitions) {
			return true, true
		}
	}

	return scoped, false
}

// scopedTagsInList reports scoped plus matched for one criteria list.
func scopedTagsInList(criterias []permCriteria, tags []string) (bool, bool) {
	scoped := false

	for _, criteria := range criterias {
		flat := criteriaTagsacc8(criteria)
		if len(flat) == 0 {
			continue
		}

		scoped = true

		if tagsOverlap(flat, tags) {
			return true, true
		}
	}

	return scoped, false
}

// domainScopedRuleCovers reports whether one allow rule covers scope.
//
// OMG rules need both partition and tag coverage; flat rules share
// the partition plus tag overlap check.
func domainScopedRuleCovers(rule *Rule, partitions, tags []string) bool {
	if ruleHasCriteriaacc8(rule) {
		return coversCriteriaPartitionsacc8(rule, partitions) && coversCriteriaTagsacc8(rule, tags)
	}

	if len(rule.Partitions) != 0 && !partitionsOverlap(rule.Partitions, partitions) {
		return false
	}

	if len(rule.DataTags) != 0 && !tagsOverlap(rule.DataTags, tags) {
		return false
	}

	return true
}

// CoversDomainacc8Scoped reports domain coverage with partition and tag scope.
//
// A rule with non-empty partitions requires overlap with the supplied
// partitions, and a rule with non-empty data tags requires overlap
// with the supplied tags, so tag-gated grants deny nil scope while
// allowing supplied scope. Empty rule scope still means all.
func CoversDomainacc8Scoped(grant *Grant, domain uint32, partitions, tags []string) bool {
	if grant == nil {
		return false
	}

	for i := range grant.AllowRules {
		rule := &grant.AllowRules[i]
		if !ruleCoversDomain(rule, domain) {
			continue
		}

		if domainScopedRuleCovers(rule, partitions, tags) {
			return true
		}
	}

	return false
}

// coversCriteriaPartitionsacc8 reports OMG partition coverage for domain scope.
func coversCriteriaPartitionsacc8(rule *Rule, partitions []string) bool {
	scoped, matched := scopedPartitionsInList(rule.Publish, partitions)
	if matched {
		return true
	}

	subScoped, subMatched := scopedPartitionsInList(rule.Subscribe, partitions)
	scoped = scoped || subScoped
	if subMatched {
		return true
	}

	relayScoped, relayMatched := scopedPartitionsInList(rule.Relay, partitions)
	scoped = scoped || relayScoped

	return relayMatched || !scoped
}

// coversCriteriaTagsacc8 reports OMG tag coverage for domain scope.
func coversCriteriaTagsacc8(rule *Rule, tags []string) bool {
	scoped, matched := scopedTagsInList(rule.Publish, tags)
	if matched {
		return true
	}

	subScoped, subMatched := scopedTagsInList(rule.Subscribe, tags)
	scoped = scoped || subScoped
	if subMatched {
		return true
	}

	relayScoped, relayMatched := scopedTagsInList(rule.Relay, tags)
	scoped = scoped || relayScoped

	return relayMatched || !scoped
}

// ruleCoversDomain reports domain scope with empty meaning all.
func ruleCoversDomain(rule *Rule, domain uint32) bool {
	if len(rule.Domains) == 0 && len(rule.DomainRanges) == 0 {
		return true
	}

	if slices.Contains(rule.Domains, domain) {
		return true
	}

	for _, r := range rule.DomainRanges {
		if r.Max == 0 {
			if r.Min <= domain {
				return true
			}

			continue
		}

		if r.Min <= domain && domain <= r.Max {
			return true
		}
	}

	return false
}

// ruleMatchesTopic reports fnmatch coverage with empty meaning all.
func ruleMatchesTopic(rule *Rule, topic string) bool {
	if len(rule.Topics) == 0 {
		return true
	}

	for _, expr := range rule.Topics {
		if expr == "*" {
			return true
		}

		ok, err := path.Match(expr, topic)
		if err != nil {
			continue
		}

		if ok {
			return true
		}
	}

	return false
}

// flatRuleMatches reports topic plus partition plus tag coverage.
//
// Empty topics match all, empty partitions or tags in the rule match any
// endpoint value. It is the shared flat check for deny, allow, and tag
// helpers so the four directional variants stay identical.
func flatRuleMatches(rule *Rule, topic string, partitions, tags []string) bool {
	if !ruleMatchesTopic(rule, topic) {
		return false
	}

	if len(rule.Partitions) != 0 && !partitionsOverlap(rule.Partitions, partitions) {
		return false
	}

	if len(rule.DataTags) != 0 && !tagsOverlap(rule.DataTags, tags) {
		return false
	}

	return true
}

// deniesWith reports whether any deny rule matches using one matcher.
//
// The matcher selects directional coverage for OMG criteria rules; flat
// rules share flatRuleMatches. It is the single loop behind the four
// Denies variants.
func deniesWith(grant *Grant, domain uint32, topic string, partitions, tags []string, match func(*Rule, string, []string, []string) bool) bool {
	if grant == nil {
		return true
	}

	for i := range grant.DenyRules {
		rule := &grant.DenyRules[i]
		if !ruleCoversDomain(rule, domain) {
			continue
		}

		if ruleHasCriteriaacc8(rule) {
			if match(rule, topic, partitions, tags) {
				return true
			}

			continue
		}

		if flatRuleMatches(rule, topic, partitions, tags) {
			return true
		}
	}

	return false
}

// criteriaAllowTags returns tags for one matching criteria list.
func criteriaAllowTags(criterias []permCriteria, topic string, partitions, tags []string) ([]string, bool) {
	for _, criteria := range criterias {
		if criteriaMatchesacc8(criteria, topic, partitions, tags) {
			return criteriaTagsacc8(criteria), true
		}
	}

	return nil, false
}

// allowTagsWith returns tags for one rule using its directional list.
//
// OMG rules scan only their own direction list; flat rules share the
// flat topic plus partition plus tag check and return rule tags.
func allowTagsWith(rule *Rule, topic string, partitions, tags []string, criterias []permCriteria) ([]string, bool) {
	if ruleHasCriteriaacc8(rule) {
		return criteriaAllowTags(criterias, topic, partitions, tags)
	}

	if !flatRuleMatches(rule, topic, partitions, tags) {
		return nil, false
	}

	return append([]string(nil), rule.DataTags...), true
}

// allowsWith reports publish style allow plus matched tags in one lookup.
//
// Deny runs first, then each allow rule consults tagsFor. Default-deny
// per DDS Security 9.4: no explicit allow match denies, even when the
// grant default names ALLOW. The default attribute only documents intent;
// authorization requires an explicit allow rule covering domain plus
// topic plus partitions plus tags. The covered hook is retained for
// callers to probe topic scoping without authorizing.
func allowsWith(grant *Grant, domain uint32, topic string, partitions, tags []string, denies func(*Grant, uint32, string, []string, []string) bool, tagsFor func(*Rule, string, []string, []string) ([]string, bool), covered func(*Grant, uint32, string) bool) (bool, []string) {
	if grant == nil {
		return false, nil
	}

	if denies(grant, domain, topic, partitions, tags) {
		return false, nil
	}

	for i := range grant.AllowRules {
		rule := &grant.AllowRules[i]
		if !ruleCoversDomain(rule, domain) {
			continue
		}

		matched, ok := tagsFor(rule, topic, partitions, tags)
		if ok {
			return true, matched
		}
	}

	_ = covered

	return false, nil
}

// allowTopicCoveredWith reports domain plus topic coverage in one loop.
//
// The cover selector picks union or directional topic coverage for OMG
// rules; flat rules share ruleMatchesTopic.
func allowTopicCoveredWith(grant *Grant, domain uint32, topic string, cover func(*Rule, string) bool) bool {
	if grant == nil {
		return false
	}

	for i := range grant.AllowRules {
		rule := &grant.AllowRules[i]
		if !ruleCoversDomain(rule, domain) {
			continue
		}

		if ruleHasCriteriaacc8(rule) {
			if cover(rule, topic) {
				return true
			}

			continue
		}

		if ruleMatchesTopic(rule, topic) {
			return true
		}
	}

	return false
}

// DeniesEndpoint reports whether any deny rule matches domain plus topic.
//
// Deprecated: legacy union across publish, subscribe, and relay. A
// subscribe-only deny still reports true here, which is fail-closed for
// deny, but callers needing direction must use DeniesPublishacc8,
// DeniesSubscribeacc8, or DeniesRelayacc8 with explicit scope. No
// internal callers use this union; it stays for backward compatibility
// only.
func DeniesEndpoint(grant *Grant, domain uint32, topic string, partitions, tags []string) bool {
	return deniesWith(grant, domain, topic, partitions, tags, ruleMatchesAnyDirectionacc8)
}

// DeniesPublishacc8 reports directional publish deny coverage.
func DeniesPublishacc8(grant *Grant, domain uint32, topic string, partitions, tags []string) bool {
	return deniesWith(grant, domain, topic, partitions, tags, ruleMatchesPublishacc8)
}

// DeniesSubscribeacc8 reports directional subscribe deny coverage.
func DeniesSubscribeacc8(grant *Grant, domain uint32, topic string, partitions, tags []string) bool {
	return deniesWith(grant, domain, topic, partitions, tags, ruleMatchesSubscribeacc8)
}

// DeniesRelayacc8 reports directional relay deny coverage.
func DeniesRelayacc8(grant *Grant, domain uint32, topic string, partitions, tags []string) bool {
	return deniesWith(grant, domain, topic, partitions, tags, ruleMatchesRelayacc8)
}

// publishAllowTagsacc8 returns publish tags for one matching rule.
func publishAllowTagsacc8(rule *Rule, topic string, partitions, tags []string) ([]string, bool) {
	return allowTagsWith(rule, topic, partitions, tags, rule.Publish)
}

// subscribeAllowTagsacc8 returns subscribe tags for one matching rule.
func subscribeAllowTagsacc8(rule *Rule, topic string, partitions, tags []string) ([]string, bool) {
	return allowTagsWith(rule, topic, partitions, tags, rule.Subscribe)
}

// relayAllowTagsacc8 returns relay tags for one matching rule.
func relayAllowTagsacc8(rule *Rule, topic string, partitions, tags []string) ([]string, bool) {
	return allowTagsWith(rule, topic, partitions, tags, rule.Relay)
}

// AllowsPublishacc8 reports publish allow plus matched tags in one lookup.
func AllowsPublishacc8(grant *Grant, domain uint32, topic string, partitions, tags []string) (bool, []string) {
	return allowsWith(grant, domain, topic, partitions, tags, DeniesPublishacc8, publishAllowTagsacc8, allowPublishTopicCoveredacc8)
}

// AllowsSubscribeacc8 reports subscribe allow plus matched tags in one lookup.
func AllowsSubscribeacc8(grant *Grant, domain uint32, topic string, partitions, tags []string) (bool, []string) {
	return allowsWith(grant, domain, topic, partitions, tags, DeniesSubscribeacc8, subscribeAllowTagsacc8, allowSubscribeTopicCoveredacc8)
}

// AllowsRelayacc8 reports relay allow plus matched tags in one lookup.
func AllowsRelayacc8(grant *Grant, domain uint32, topic string, partitions, tags []string) (bool, []string) {
	return allowsWith(grant, domain, topic, partitions, tags, DeniesRelayacc8, relayAllowTagsacc8, allowRelayTopicCoveredacc8)
}

// AllowsEndpoint reports whether an allow rule matches and no deny does.
//
// Deprecated: legacy union was permissive (either direction allowed the
// endpoint), so a subscribe-only grant could authorize publish. This
// shim now denies unless both publish and subscribe allow: it returns
// true only when AllowsPublishacc8 and AllowsSubscribeacc8 both allow.
// New code must use AllowsPublishacc8, AllowsSubscribeacc8, or
// AllowsRelayacc8 with explicit scope. No internal callers use this
// union; it stays for backward compatibility only. Deny rules override
// allow rules, and default-deny per 9.4 applies: no explicit allow
// match denies even when the grant default names ALLOW.
func AllowsEndpoint(grant *Grant, domain uint32, topic string, partitions, tags []string) bool {
	if grant == nil {
		return false
	}

	if DeniesEndpoint(grant, domain, topic, partitions, tags) {
		return false
	}

	okPub, _ := AllowsPublishacc8(grant, domain, topic, partitions, tags)
	if !okPub {
		return false
	}

	okSub, _ := AllowsSubscribeacc8(grant, domain, topic, partitions, tags)

	return okSub
}

// CheckTypeConsistencyacc9 reports whether writer type is assignable to reader type.
//
// It implements the 7.6.3.4.2 step 2 name fallback through the types
// engine: with type names only, identical names assign and anything
// else denies fail-closed, including when coercion is allowed, because
// coercion needs full type information to verify. Descriptor callers
// use CheckTypeConsistencyWithPolicy for the step 1 engine evaluation
// (7.2.5 assignability plus 7.6.3.4 enforcement).
func CheckTypeConsistencyacc9(readerType, writerType string, allowCoercion bool) error {
	policy := types.Defaultxt6aConsistency()
	if !allowCoercion {
		policy.Kind = types.Consistencyxt6aDisallowCoercion
	}

	err := CheckTypeConsistencyWithPolicy(policy, readerType, writerType, nil, nil)
	if err != nil {
		return err
	}

	return nil
}

// QoSConsistencyPolicy converts a TypeConsistencyEnforcementQosPolicy
// (7.6.3.4.1) into the types engine policy for 7.6.3.4.2 evaluation.
//
// The kind maps one to one: disallow requires minimal equivalence
// (EXACT_TYPE matching) while allow evaluates reader-from-writer
// assignability (MINIMAL matching). Bound, name, widening, and
// validation flags carry over unchanged.
func QoSConsistencyPolicy(qos core.TypeConsistencyEnforcementQosPolicy) types.TypeConsistencyxt6aPolicy {
	policy := types.TypeConsistencyxt6aPolicy{
		IgnoreSequenceBounds: qos.IgnoreSequenceBounds,
		IgnoreStringBounds:   qos.IgnoreStringBounds,
		IgnoreMemberNames:    qos.IgnoreMemberNames,
		IgnoreLiteralNames:   qos.IgnoreLiteralNames,
		PreventTypeWidening:  qos.PreventTypeWidening,
		ForceTypeValidation:  qos.ForceTypeValidation,
	}

	if qos.Kind == core.TypeConsistencyDisallowCoercion {
		policy.Kind = types.Consistencyxt6aDisallowCoercion
	} else {
		policy.Kind = types.Consistencyxt6aAllowCoercion
	}

	return policy
}

// CheckTypeConsistencyWithPolicy evaluates 7.6.3.4.2 with descriptors
// when present and type names otherwise.
//
// Nil reader or writer descriptors mean no type information: step 2
// compares type names and denies on mismatch, failing closed even when
// the policy allows coercion, and denies everything when force
// validation is set. Present descriptors run step 1 through the 7.2.5
// assignability tables. Every inconsistency denies with
// ErrNotAllowedBySecurity.
func CheckTypeConsistencyWithPolicy(policy types.TypeConsistencyxt6aPolicy, readerName, writerName string, reader, writer *types.TypeRefxt6a) error {
	if strings.TrimSpace(readerName) == "" || strings.TrimSpace(writerName) == "" {
		return fmt.Errorf("security: type consistency needs reader plus writer type names: %w", ErrNotAllowedBySecurity)
	}

	var err error

	if reader == nil || writer == nil {
		err = types.Evaluatext6aConsistency(policy, false, false, false, readerName, writerName)
	} else {
		err = types.CheckTypeConsistencyxt6a(policy, *reader, *writer, readerName, writerName)
	}

	if err != nil {
		return fmt.Errorf("security: type %q not assignable to %q: %w", writerName, readerName, errors.Join(err, ErrNotAllowedBySecurity))
	}

	return nil
}

// CheckLocalDataWriterMatchWithTypes gates a local writer match with
// type enforcement per 7.2.5 and 7.6.3.4.
//
// Consistency runs before the allow skeleton and fails closed on
// inconsistency unless the policy allows it, so mismatched endpoints
// never match even in tolerate mode. readerName and writerName carry
// the step 2 fallback names; nil descriptors mean names only.
func (c *AccessController) CheckLocalDataWriterMatchWithTypes(local, remote, topic string, policy types.TypeConsistencyxt6aPolicy, readerName, writerName string, reader, writer *types.TypeRefxt6a) error {
	err := CheckTypeConsistencyWithPolicy(policy, readerName, writerName, reader, writer)
	if err != nil {
		return err
	}

	return c.CheckLocalDataWriterMatch(local, remote, topic)
}

// CheckLocalDataReaderMatchWithTypes gates a local reader match with
// type enforcement per 7.2.5 and 7.6.3.4.
//
// Consistency runs before the allow skeleton and fails closed on
// inconsistency unless the policy allows it, so mismatched endpoints
// never match even in tolerate mode. readerName and writerName carry
// the step 2 fallback names; nil descriptors mean names only.
func (c *AccessController) CheckLocalDataReaderMatchWithTypes(local, remote, topic string, policy types.TypeConsistencyxt6aPolicy, readerName, writerName string, reader, writer *types.TypeRefxt6a) error {
	err := CheckTypeConsistencyWithPolicy(policy, readerName, writerName, reader, writer)
	if err != nil {
		return err
	}

	return c.CheckLocalDataReaderMatch(local, remote, topic)
}

// CheckRemoteDataWriterWithTypes gates a remote writer match with type
// enforcement per 7.2.5 and 7.6.3.4.
//
// Consistency runs before the allow skeleton and fails closed on
// inconsistency unless the policy allows it. readerName and writerName
// carry the step 2 fallback names; nil descriptors mean names only.
func (c *AccessController) CheckRemoteDataWriterWithTypes(identity, topic string, policy types.TypeConsistencyxt6aPolicy, readerName, writerName string, reader, writer *types.TypeRefxt6a) error {
	err := CheckTypeConsistencyWithPolicy(policy, readerName, writerName, reader, writer)
	if err != nil {
		return err
	}

	return c.CheckRemoteDataWriter(identity, topic)
}

// CheckRemoteDataReaderWithTypes gates a remote reader match with type
// enforcement per 7.2.5 and 7.6.3.4.
//
// Consistency runs before the allow skeleton and fails closed on
// inconsistency unless the policy allows it. readerName and writerName
// carry the step 2 fallback names; nil descriptors mean names only.
func (c *AccessController) CheckRemoteDataReaderWithTypes(identity, topic string, policy types.TypeConsistencyxt6aPolicy, readerName, writerName string, reader, writer *types.TypeRefxt6a) error {
	err := CheckTypeConsistencyWithPolicy(policy, readerName, writerName, reader, writer)
	if err != nil {
		return err
	}

	return c.CheckRemoteDataReader(identity, topic)
}

// partitionsOverlap reports a common partition with empty meaning default.
func partitionsOverlap(rule, endpoint []string) bool {
	if len(endpoint) == 0 {
		endpoint = []string{""}
	}

	ruleSet := rule
	if len(ruleSet) == 0 {
		ruleSet = []string{""}
	}

	for _, want := range ruleSet {
		if slices.Contains(endpoint, want) {
			return true
		}
	}

	return false
}

// tagsOverlap reports a common data tag.
func tagsOverlap(rule, endpoint []string) bool {
	for _, want := range rule {
		if slices.Contains(endpoint, want) {
			return true
		}
	}

	return false
}

// ParticipantSecurityConfig is the decided config for one participant.
//
// Per 9.4.2.4 it is the sole input to protection and matching. Phase 1
// carries identity only; Phase 4 fills protection kinds after the
// permissions document verifies.
type ParticipantSecurityConfig struct {
	// Identity names the participant the config was decided for.
	Identity string
	// DiscoveryProtection guards discovery traffic.
	DiscoveryProtection string
	// LivelinessProtection guards liveliness traffic.
	LivelinessProtection string
	// RTPSProtection guards whole RTPS messages.
	RTPSProtection string
	// AllowUnauthenticated permits unauthenticated peers.
	AllowUnauthenticated bool
}

// TopicSecurityConfig is the decided config for one topic.
//
// Per 9.4.2.6 it is the sole input to topic protection. Phase 1
// carries the topic only; Phase 4 fills rule outcomes.
type TopicSecurityConfig struct {
	// Topic names the topic the config was decided for.
	Topic string
	// MetadataProtection guards submessage metadata.
	MetadataProtection string
	// DataProtection guards payload data.
	DataProtection string
	// EnableDiscovery guards discovery for the topic.
	EnableDiscovery bool
	// EnableLiveliness guards liveliness for the topic.
	EnableLiveliness bool
	// EnableReadAccess gates read access for the topic.
	EnableReadAccess bool
	// EnableWriteAccess gates write access for the topic.
	EnableWriteAccess bool
}

// EndpointSecurityConfig is the decided config for one endpoint.
//
// Per 9.4.2.7 it is the sole input to endpoint protection and match
// gating. Phase 1 carries routing identity only; Phase 4 fills
// protection kinds and match outcomes.
type EndpointSecurityConfig struct {
	// Endpoint names the endpoint the config was decided for.
	Endpoint string
	// Topic names the endpoint topic.
	Topic string
	// IsReader is true for reader configs and false for writer configs.
	IsReader bool
	// Allowed is true when the endpoint may match.
	Allowed bool
	// DataProtection guards payload data for the endpoint topic.
	DataProtection string
	// MetadataProtection guards metadata for the endpoint topic.
	MetadataProtection string
}

// AccessController implements the 9.4.2.9 check skeleton for Phase 1.
//
// When disabled every check allows and every getter returns an empty
// config, preserving tolerance behavior. When enabled every check
// denies with ErrNotAllowedBySecurity and every getter denies the same
// way, because no handshake has validated any peer and no permissions
// signature verifies yet. Rule consultation against parsed documents
// lands in Phase 4; this skeleton only pins the operation shape.
type AccessController struct {
	// Enabled selects deny-default. False means tolerate mode.
	Enabled bool
}

// NewController returns a controller with the given posture.
func NewController(enabled bool) *AccessController {
	return &AccessController{Enabled: enabled}
}

// deny returns nil when disabled and a loud denial when enabled.
func (c *AccessController) deny(op, identity string) error {
	if c == nil || !c.Enabled {
		return nil
	}

	return fmt.Errorf("security: %s refused identity=%s: %w", op, identity, ErrNotAllowedBySecurity)
}

// ValidateLocalPermissions checks local permissions material.
func (c *AccessController) ValidateLocalPermissions(identity string) error {
	err := c.deny("validate_local_permissions", identity)
	if err != nil {
		return err
	}

	return nil
}

// ValidateRemotePermissions checks remote permissions material.
func (c *AccessController) ValidateRemotePermissions(identity string) error {
	err := c.deny("validate_remote_permissions", identity)
	if err != nil {
		return err
	}

	return nil
}

// CheckCreateParticipant gates local participant creation.
func (c *AccessController) CheckCreateParticipant(identity string) error {
	err := c.deny("check_create_participant", identity)
	if err != nil {
		return err
	}

	return nil
}

// CheckCreateTopic gates local topic creation.
func (c *AccessController) CheckCreateTopic(identity, topic string) error {
	err := c.deny("check_create_topic:"+topic, identity)
	if err != nil {
		return err
	}

	return nil
}

// CheckCreateDataWriter gates local writer creation.
func (c *AccessController) CheckCreateDataWriter(identity, topic string) error {
	err := c.deny("check_create_datawriter:"+topic, identity)
	if err != nil {
		return err
	}

	return nil
}

// CheckCreateDataReader gates local reader creation.
func (c *AccessController) CheckCreateDataReader(identity, topic string) error {
	err := c.deny("check_create_datareader:"+topic, identity)
	if err != nil {
		return err
	}

	return nil
}

// CheckLocalDataWriterRegisterInstance gates local writer instance registration.
func (c *AccessController) CheckLocalDataWriterRegisterInstance(identity, topic string) error {
	err := c.deny("check_local_datawriter_register_instance:"+topic, identity)
	if err != nil {
		return err
	}

	return nil
}

// CheckRemoteParticipant gates remote participant matching.
func (c *AccessController) CheckRemoteParticipant(identity string) error {
	err := c.deny("check_remote_participant", identity)
	if err != nil {
		return err
	}

	return nil
}

// CheckRemoteTopic gates remote topic matching.
func (c *AccessController) CheckRemoteTopic(identity, topic string) error {
	err := c.deny("check_remote_topic:"+topic, identity)
	if err != nil {
		return err
	}

	return nil
}

// CheckRemoteDataWriter gates remote writer matching.
func (c *AccessController) CheckRemoteDataWriter(identity, topic string) error {
	err := c.deny("check_remote_datawriter:"+topic, identity)
	if err != nil {
		return err
	}

	return nil
}

// CheckRemoteDataReader gates remote reader matching.
func (c *AccessController) CheckRemoteDataReader(identity, topic string) error {
	err := c.deny("check_remote_datareader:"+topic, identity)
	if err != nil {
		return err
	}

	return nil
}

// CheckLocalDataWriterMatch gates matching a local writer with a remote reader.
func (c *AccessController) CheckLocalDataWriterMatch(local, remote, topic string) error {
	err := c.deny("check_local_datawriter_match:"+topic, local+">"+remote)
	if err != nil {
		return err
	}

	return nil
}

// CheckLocalDataReaderMatch gates matching a local reader with a remote writer.
func (c *AccessController) CheckLocalDataReaderMatch(local, remote, topic string) error {
	err := c.deny("check_local_datareader_match:"+topic, local+">"+remote)
	if err != nil {
		return err
	}

	return nil
}

// GetParticipantSecurityConfig decides the participant config.
func (c *AccessController) GetParticipantSecurityConfig(identity string) (ParticipantSecurityConfig, error) {
	cfg := ParticipantSecurityConfig{Identity: identity}

	err := c.deny("get_participant_security_config", identity)
	if err != nil {
		return ParticipantSecurityConfig{}, err
	}

	return cfg, nil
}

// GetTopicSecurityConfig decides the topic config.
func (c *AccessController) GetTopicSecurityConfig(topic string) (TopicSecurityConfig, error) {
	cfg := TopicSecurityConfig{Topic: topic}

	err := c.deny("get_topic_security_config", topic)
	if err != nil {
		return TopicSecurityConfig{}, err
	}

	return cfg, nil
}

// GetEndpointSecurityConfig decides the endpoint config.
func (c *AccessController) GetEndpointSecurityConfig(endpoint, topic string, isReader bool) (EndpointSecurityConfig, error) {
	cfg := EndpointSecurityConfig{Endpoint: endpoint, Topic: topic, IsReader: isReader}

	err := c.deny("get_endpoint_security_config:"+topic, endpoint)
	if err != nil {
		return EndpointSecurityConfig{}, err
	}

	return cfg, nil
}
