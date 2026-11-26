// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package security holds the Phase 1 DDS Security decision scaffolding.
//
// It hardens DDS Security 7.3 property lists into typed records,
// validates governance and permissions file shape, and denies every
// protected operation behind an explicit opt-in flag. It performs no
// authentication, key exchange, or crypto: there is no handshake until
// Phase 2 and no signature verification until Phase 4, so an enabled
// suite denies every remote match and local create while recording a
// loud log plus a per-class counter for each refusal. A disabled suite
// allows everything silently, preserving tolerance behavior.
//
// Phase 1 cannot authenticate anyone. Enabling the suite therefore
// means an authenticated-only network that cannot form yet. Every
// denial says so in its reason string. Phase 2 adds the handshake and
// reuses these choke points; Phase 4 completes signature verification
// and rule consultation.
package security

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/access"
	"github.com/podomy/weld/internal/transports/dds/security/auth"
	"github.com/podomy/weld/internal/transports/dds/security/logging"
	"github.com/podomy/weld/internal/transports/dds/security/tagging"
	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// Sentinel errors for suite failures.
var (
	// ErrNotAllowedBySecurity denies an operation under 7.3.25.
	ErrNotAllowedBySecurity = access.ErrNotAllowedBySecurity
	// ErrInvalidSuiteConfig reports a bad suite configuration.
	ErrInvalidSuiteConfig = errors.New("security: invalid suite config")
)

// Property keys pinned by the tolerance layer for material references.
//
// Verified at Phase 5 bring-up against CycloneDDS
// dds_security_api_defs.h plus Fast DDS peers: dds.sec.auth.identity_ca,
// dds.sec.auth.private_key, dds.sec.auth.identity_certificate,
// dds.sec.auth.password, dds.sec.access.permissions_ca,
// dds.sec.access.governance, dds.sec.access.permissions,
// dds.sec.auth.trusted_ca_dir, dds.sec.builtin_endpoint_name,
// dds.sec.crypto.keysize, plus dds.sec.auth/crypto/access library path
// keys and org.eclipse.cyclonedds.sec.auth.crl. Parsing accepts both
// plugin and plugin_class spellings plus Fast DDS builtin aliases so
// no peer is misread either way.
const (
	// PropAuthPlugin names the authentication plugin.
	PropAuthPlugin = "dds.sec.auth.plugin"
	// PropAuthPluginClass is the alternate authentication plugin key.
	PropAuthPluginClass = "dds.sec.auth.plugin_class"
	// PropAccessPlugin names the access plugin.
	PropAccessPlugin = "dds.sec.access.plugin"
	// PropAccessPluginClass is the alternate access plugin key.
	PropAccessPluginClass = "dds.sec.access.plugin_class"
	// PropCryptoPlugin names the crypto plugin.
	PropCryptoPlugin = "dds.sec.crypto.plugin"
	// PropCryptoPluginClass is the alternate crypto plugin key.
	PropCryptoPluginClass = "dds.sec.crypto.plugin_class"
	// PropKeyExchangeAlgorithm carries the key agreement algorithm.
	PropKeyExchangeAlgorithm = "dds.sec.crypto.key_exchange_algorithm"
)

// Known plugin names for the three builtin plugins.
const (
	// AuthPluginPKIDH is the builtin PKI-DH authentication plugin.
	AuthPluginPKIDH = "DDS:Auth:PKI-DH"
	// AccessPluginPermissions is the builtin permissions access plugin.
	AccessPluginPermissions = "DDS:Access:Permissions"
	// CryptoPluginAESGCM is the builtin AES-GCM-GMAC crypto plugin.
	CryptoPluginAESGCM = "DDS:Crypto:AES-GCM-GMAC"
)

// PeerClass names the security posture of one peer.
type PeerClass string

// Peer posture values mirroring the tolerance classifier.
const (
	// PeerUnsecured marks a peer with no security markers.
	PeerUnsecured PeerClass = "unsecured"
	// PeerCompatible marks a security-aware peer that demands no crypto.
	PeerCompatible PeerClass = "secured-compatible-unknown"
	// PeerRequiresCrypto marks a peer whose properties demand crypto.
	PeerRequiresCrypto PeerClass = "secured-requiring-crypto"
)

// RefusalClass names one refusal category for counters and audit.
type RefusalClass string

// Refusal categories. Each has its own counter; see RefusalSnapshot.
const (
	// RefusalUnsecurePeer counts unsecure peers refused by an enabled suite.
	RefusalUnsecurePeer RefusalClass = "unsecure-peer"
	// RefusalIncompleteMaterial counts peers refused over unreadable local material.
	RefusalIncompleteMaterial RefusalClass = "incomplete-material"
	// RefusalNoHandshake counts complete peers refused for lack of handshake.
	RefusalNoHandshake RefusalClass = "no-handshake"
	// RefusalMalformed counts peers refused over malformed properties.
	RefusalMalformed RefusalClass = "malformed"
	// RefusalUnknownAlgorithm counts peers refused over unknown algorithms.
	RefusalUnknownAlgorithm RefusalClass = "unknown-algorithm"
)

// PeerDescriptor carries the decision inputs for one peer.
type PeerDescriptor struct {
	// Identity names the peer for audit logs, usually a hex GUID.
	Identity string
	// Class is the peer posture from the tolerance classifier.
	Class PeerClass
	// Properties holds decoded dds.sec.* properties. It is read only.
	Properties map[string]string
	// DomainID carries the participant domain for rule selection.
	DomainID uint32
	// Tag carries the domain tag for rule selection, empty for none.
	Tag string
	// Partitions lists endpoint partitions for grant matching.
	Partitions []string
	// DataTags lists endpoint data tags for tag-gated decisions.
	DataTags []string
}

// ParticipantProperties is the typed form of one 7.3 property list.
type ParticipantProperties struct {
	// AuthPlugin names the authentication plugin, empty when unannounced.
	AuthPlugin string
	// AccessPlugin names the access control plugin, empty when unannounced.
	AccessPlugin string
	// CryptoPlugin names the crypto plugin, empty when unannounced.
	CryptoPlugin string
	// IdentityCA references the identity CA material.
	IdentityCA string
	// IdentityCertificate references the identity certificate material.
	IdentityCertificate string
	// PrivateKey references the private key material.
	PrivateKey string
	// Governance references the governance document.
	Governance string
	// Permissions references the permissions document.
	Permissions string
	// PermissionsCA references the permissions CA material.
	PermissionsCA string
	// KeyExchangeAlgorithm names the key agreement algorithm.
	KeyExchangeAlgorithm string
	// Malformed is true when any warning fired during parsing.
	Malformed bool
	// UnknownAlgorithm is true when a plugin or algorithm name is unknown.
	UnknownAlgorithm bool
}

// ParseParticipantProperties hardens one property map into typed form.
//
// It never fails: malformed entries are loud-and-skip warnings, never
// fatal. First wins on duplicate keys, matching the tolerance layer.
// Unknown plugin or algorithm names warn and set UnknownAlgorithm so
// the decision fails closed per INV-7 and INV-8.
func ParseParticipantProperties(props map[string]string) (ParticipantProperties, []string) {
	var parsed ParticipantProperties

	var warnings []string

	for key, value := range props {
		if !strings.HasPrefix(key, "dds.sec.") {
			continue
		}

		warning := assignProperty(&parsed, key, value)
		if warning == "" {
			continue
		}

		warnings = append(warnings, warning)

		if strings.HasPrefix(warning, "empty value") {
			parsed.Malformed = true
		} else {
			parsed.UnknownAlgorithm = true
		}
	}

	return parsed, warnings
}

// assignProperty folds one dds.sec.* entry and reports a warning or empty.
func assignProperty(parsed *ParticipantProperties, key, value string) string {
	if strings.TrimSpace(value) == "" {
		return "empty value for " + key
	}

	switch key {
	case PropAuthPlugin, PropAuthPluginClass:
		return assignPlugin(&parsed.AuthPlugin, value, AuthPluginPKIDH, key)
	case PropAccessPlugin, PropAccessPluginClass:
		return assignPlugin(&parsed.AccessPlugin, value, AccessPluginPermissions, key)
	case PropCryptoPlugin, PropCryptoPluginClass:
		return assignPlugin(&parsed.CryptoPlugin, value, CryptoPluginAESGCM, key)
	case PropKeyExchangeAlgorithm:
		return assignAlgorithm(parsed, value, key)
	}

	assignMaterial(parsed, key, value)

	return ""
}

// assignPlugin records one plugin name, warning on unknown values.
func assignPlugin(field *string, value, known, key string) string {
	if *field == "" {
		*field = value
	}

	if value == known || isBuiltinPluginAlias(key, value) {
		return ""
	}

	return "unknown plugin " + value + " for " + key
}

// isBuiltinPluginAlias accepts Fast DDS builtin names for peer interop.
func isBuiltinPluginAlias(key, value string) bool {
	switch key {
	case PropAuthPlugin, PropAuthPluginClass:
		return value == "builtin.PKI-DH"
	case PropAccessPlugin, PropAccessPluginClass:
		return value == "builtin.Access-Permissions"
	case PropCryptoPlugin, PropCryptoPluginClass:
		return value == "builtin.AES-GCM-GMAC"
	default:
		return false
	}
}

// assignAlgorithm records one algorithm name, warning on unknown values.
func assignAlgorithm(parsed *ParticipantProperties, value, key string) string {
	if parsed.KeyExchangeAlgorithm == "" {
		parsed.KeyExchangeAlgorithm = value
	}

	if !access.KnownCryptoAlgorithm(value) {
		return "unknown algorithm " + value + " for " + key
	}

	return ""
}

// assignMaterial records one material reference by first-wins convention.
func assignMaterial(parsed *ParticipantProperties, key, value string) {
	assignAuthMaterial(parsed, key, value)
	assignAccessMaterial(parsed, key, value)
}

// assignAuthMaterial records identity material references.
func assignAuthMaterial(parsed *ParticipantProperties, key, value string) {
	switch key {
	case "dds.sec.auth.identity_ca":
		if parsed.IdentityCA == "" {
			parsed.IdentityCA = value
		}
	case "dds.sec.auth.identity_certificate":
		if parsed.IdentityCertificate == "" {
			parsed.IdentityCertificate = value
		}
	case "dds.sec.auth.private_key":
		if parsed.PrivateKey == "" {
			parsed.PrivateKey = value
		}
	}
}

// assignAccessMaterial records access material references.
func assignAccessMaterial(parsed *ParticipantProperties, key, value string) {
	switch key {
	case "dds.sec.access.governance":
		if parsed.Governance == "" {
			parsed.Governance = value
		}
	case "dds.sec.access.permissions":
		if parsed.Permissions == "" {
			parsed.Permissions = value
		}
	case "dds.sec.access.permissions_ca":
		if parsed.PermissionsCA == "" {
			parsed.PermissionsCA = value
		}
	}
}

// SuiteConfig carries the local material paths for one suite.
type SuiteConfig struct {
	// Enabled selects deny-default. False means tolerate mode.
	Enabled bool
	// IdentityCertificatePath is the PEM identity certificate file.
	IdentityCertificatePath string
	// IdentityPrivateKeyPath is the PEM identity private key file.
	IdentityPrivateKeyPath string
	// IdentityCAPath is the identity authority certificate file. Empty
	// falls back to PermissionsCAPath, so single-CA deployments keep
	// working while split-CA deployments separate identity trust from
	// permissions trust.
	IdentityCAPath string
	// PermissionsCAPath is the permissions authority certificate file.
	PermissionsCAPath string
	// GovernancePath is the signed governance file.
	GovernancePath string
	// PermissionsDocumentPath is the signed permissions file.
	PermissionsDocumentPath string
	// CRLPath is the optional revocation list file in PEM or DER form.
	// When set, every listed identity certificate refuses, and a CRL
	// that verifies against no configured identity CA fails the load
	// closed. Empty means no revocation data is enforced.
	CRLPath string
}

// Validate checks path presence for secured operation.
func (c SuiteConfig) Validate() error {
	if !c.Enabled {
		return nil
	}

	var errs []error

	if strings.TrimSpace(c.IdentityCertificatePath) == "" {
		errs = append(errs, fmt.Errorf("security: identity certificate path is empty: %w", ErrInvalidSuiteConfig))
	}

	if strings.TrimSpace(c.IdentityPrivateKeyPath) == "" {
		errs = append(errs, fmt.Errorf("security: identity private key path is empty: %w", ErrInvalidSuiteConfig))
	}

	if strings.TrimSpace(c.PermissionsCAPath) == "" {
		errs = append(errs, fmt.Errorf("security: permissions CA path is empty: %w", ErrInvalidSuiteConfig))
	}

	if strings.TrimSpace(c.GovernancePath) == "" {
		errs = append(errs, fmt.Errorf("security: governance path is empty: %w", ErrInvalidSuiteConfig))
	}

	if strings.TrimSpace(c.PermissionsDocumentPath) == "" {
		errs = append(errs, fmt.Errorf("security: permissions document path is empty: %w", ErrInvalidSuiteConfig))
	}

	if len(errs) == 0 {
		return nil
	}

	return fmt.Errorf("security: suite config: %w", errors.Join(errs...))
}

// verifiedStaleAfteracc8 bounds verified material caching.
//
// Verified governance plus permissions documents reload when older
// than this bound, so rotated files flip decisions without a process
// restart. The bound is deliberately short for tests and operators.
const verifiedStaleAfteracc8 = 5 * time.Minute

// PeerState records the last decision for one peer.
type PeerState struct {
	// Identity names the peer.
	Identity string
	// Class is the peer posture at decision time.
	Class PeerClass
	// Decision is allow or the refusal class.
	Decision string
	// Reason explains the decision for audit.
	Reason string
	// UpdatedAt is the last decision time.
	UpdatedAt time.Time
}

// RefusalSnapshot counts refusals by class for INV-10 observability.
type RefusalSnapshot struct {
	// UnsecurePeer counts unsecure peers refused while enabled.
	UnsecurePeer uint64
	// IncompleteMaterial counts refusals over unreadable local material.
	IncompleteMaterial uint64
	// NoHandshake counts complete peers refused for lack of handshake.
	NoHandshake uint64
	// Malformed counts refusals over malformed properties.
	Malformed uint64
	// UnknownAlgorithm counts refusals over unknown algorithms.
	UnknownAlgorithm uint64
}

// Suite is the Phase 4 decision choke point for SPDP and SEDP.
//
// The access handle pins the 9.4.2.9 operation shape. Verified governance
// plus permissions documents, the local identity certificate, and the
// authenticated peer set gate allow decisions when the suite is enabled.
// Material loads lazily on the first decision and caches: missing files
// refuse as incomplete material, malformed files refuse as malformed,
// unknown algorithms refuse closed, and complete but unauthenticated
// peers refuse for lack of handshake. Verified documents plus an
// authenticated peer plus matching domain and topic rules allow. There
// is no hot reload; restart to pick up new material.
type Suite struct {
	// enabled selects deny-default.
	enabled bool
	// config carries local material paths.
	config SuiteConfig
	// controller pins the check operation shape.
	controller *access.AccessController
	// mu guards registry and material cache.
	mu sync.Mutex
	// peers maps identity to last decision.
	peers map[string]PeerState
	// loaded is true once material load ran.
	loaded bool
	// governance caches the parsed governance document.
	governance *access.GovernanceDoc
	// permissions caches the parsed permissions document.
	permissions *access.PermissionsDoc
	// materialRefusal caches the load refusal class.
	materialRefusal RefusalClass
	// materialReason caches the load refusal reason.
	materialReason string
	// materialOK is true when local material parsed.
	materialOK bool
	// verifiedLoaded is true once verified load ran.
	verifiedLoaded bool
	// verifiedGov caches the signature-verified governance document.
	verifiedGov *access.GovernanceDoc
	// verifiedPerms caches the signature-verified permissions document.
	verifiedPerms *access.PermissionsDoc
	// identityPool caches the identity CA trust pool. It equals the
	// permissions pool in single-CA deployments.
	identityPool *x509.CertPool
	// identityCAs caches parsed identity CA certificates for CRL checks.
	identityCAs []*x509.Certificate
	// revoked caches revoked identity key serials from the CRL file.
	revoked map[string]struct{}
	// identity caches the local identity certificate.
	identity *x509.Certificate
	// verifiedOK is true when signatures plus trust verified.
	verifiedOK bool
	// verifiedReason explains a verified load failure for audit.
	verifiedReason string
	// verifiedAt records the last verified load time for staleness.
	verifiedAt time.Time
	// authenticated maps identity to validated peer certificate.
	authenticated map[string]*x509.Certificate
	// boundCert pins one identity to its first certificate fingerprint.
	boundCert map[string]string
	// boundIdentity pins one certificate fingerprint to its identity.
	boundIdentity map[string]string
	// handshake reports completed handshake state per 9.3.2 when set.
	// Suite decisions require both the handshake completion and the
	// local certificate validation; a nil handshake preserves the
	// validation-only path used by unit fixtures.
	handshake HandshakeReporter
	// logger records allow and deny events per INV-10.
	logger *logging.Plugin
	// tagger evaluates data-tag gates.
	tagger *tagging.Plugin
	// unsecure counts unsecure peer refusals.
	unsecure atomic.Uint64
	// incomplete counts incomplete material refusals.
	incomplete atomic.Uint64
	// noHandshake counts complete peer refusals.
	noHandshake atomic.Uint64
	// malformed counts malformed property refusals.
	malformed atomic.Uint64
	// unknownAlgorithm counts unknown algorithm refusals.
	unknownAlgorithm atomic.Uint64
	// negotiatedKind holds the OMG CryptoTransformKind for exact
	// governance checks per 9.5.3. Zero means unset and the
	// allow-list proxy applies.
	negotiatedKind uint32
}

// NewSuite builds a suite from configuration without reading files.
//
// Enabled configs must carry every path; files load lazily on the
// first decision so every refusal class stays reachable through the
// public decision API. Disabled configs always allow silently.
func NewSuite(cfg SuiteConfig) (*Suite, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	logger := logging.New()
	logger.SetLogOptions(cfg.Enabled)

	return &Suite{
		enabled:       cfg.Enabled,
		config:        cfg,
		controller:    access.NewController(cfg.Enabled),
		peers:         make(map[string]PeerState),
		authenticated: make(map[string]*x509.Certificate),
		boundCert:     make(map[string]string),
		boundIdentity: make(map[string]string),
		revoked:       make(map[string]struct{}),
		logger:        logger,
		tagger:        tagging.New(),
	}, nil
}

// HandshakeReporter reports completed handshake state per 9.3.2.
type HandshakeReporter interface {
	// Authenticated reports whether one peer completed the handshake
	// with verified identity.
	Authenticated(remote string) bool
}

// SetHandshakeManager wires handshake completion into Suite decisions.
//
// Decisions require both handshake Authenticated and certificate
// validation when a reporter is set; nil restores validation-only.
func (s *Suite) SetHandshakeManager(reporter HandshakeReporter) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.handshake = reporter
}

// SetNegotiatedTransformKind pins the negotiated crypto kind per 9.5.3.
//
// OMG CryptoTransformKind values are 1=AES128_GMAC, 2=AES128_GCM,
// 3=AES256_GMAC, 4=AES256_GCM. GMAC kinds satisfy SIGN only; GCM kinds
// satisfy SIGN and ENCRYPT. Zero clears the pin and restores the
// allow-list proxy.
func (s *Suite) SetNegotiatedTransformKind(kind uint32) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.negotiatedKind = kind
}

// IdentityTrustPool returns the verified identity CA pool for handshake use.
//
// It triggers the verified load so split-CA pools populate before the
// handshake validates peer chains. Nil means unverified; callers fail
// closed through verifiedState decisions.
func (s *Suite) IdentityTrustPool() *x509.CertPool {
	if s == nil || !s.Enabled() {
		return nil
	}

	_, _, _, ok, _ := s.verifiedState()
	if !ok {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.identityPool
}

// Enabled reports whether deny-default is active.
func (s *Suite) Enabled() bool {
	return s != nil && s.enabled
}

// IsAuthenticated reports whether one peer completed certificate validation.
func (s *Suite) IsAuthenticated(identity string) bool {
	return s.isAuthenticated(identity)
}

// ValidateDiscoveryTokens checks SPDP token presence per 7.4 and 9.3.2.
//
// Both the identity token (PID 0x1001) and the permissions token (PID
// 0x1002) must be present for a secured peer; either missing fails
// closed. Backend wiring TODO (backend-owned authorizeRemoteParticipant
// in backend.go currently checks only IdentityTokenPresent): the
// discovery gate should call this helper with both presence flags and
// deny when it errors, so token-less peers never gain routes.
//
// INTERFACE REQUEST for backend owners: replace the single
// IdentityTokenPresent check with
// if err := ddssecurity.ValidateDiscoveryTokens(ann.Security.IdentityTokenPresent, ann.Security.PermissionsTokenPresent); err != nil { deny }.
func ValidateDiscoveryTokens(identityPresent, permissionsPresent bool) error {
	if !identityPresent {
		return fmt.Errorf("security: discovery without identity token: %w", ErrNotAllowedBySecurity)
	}

	if !permissionsPresent {
		return fmt.Errorf("security: discovery without permissions token: %w", ErrNotAllowedBySecurity)
	}

	return nil
}

// ValidateDiscoveryTokenContents checks parsed token class IDs per 7.4.
//
// Identity holders must parse as IdentityToken and permissions holders
// as PermissionsToken; wrong classes or malformed holders fail closed.
// Backend wiring TODO: once SPDP carries parsed DataHolders, pass them
// here in addition to presence.
func ValidateDiscoveryTokenContents(identityTok, permTok tokens.DataHolder) error {
	if _, err := tokens.ParseIdentityToken(identityTok); err != nil {
		return fmt.Errorf("security: discovery identity token: %w", err)
	}

	if err := tokens.ParsePermissionsToken(permTok); err != nil {
		return fmt.Errorf("security: discovery permissions token: %w", err)
	}

	return nil
}

// DecideRemotePeer is the pure remote decision core.
//
// It allows only when disabled. Malformed properties and unknown
// algorithms refuse first per INV-1 and INV-7, then unsecure peers,
// then peers over unreadable local material, and finally complete
// peers because no handshake exists yet at Phase 1. The allow result
// is true only for the disabled case.
func DecideRemotePeer(enabled bool, class PeerClass, localComplete, malformed, unknownAlgorithm bool) (bool, RefusalClass, string) {
	if !enabled {
		return true, "", ""
	}

	if malformed {
		return false, RefusalMalformed, "peer announces malformed security material, fail closed per INV-1"
	}

	if unknownAlgorithm {
		return false, RefusalUnknownAlgorithm, "peer announces an unknown algorithm, fail closed per INV-7"
	}

	if class == PeerUnsecured {
		return false, RefusalUnsecurePeer, "unsecure peer refused, enabled suite admits authenticated peers only"
	}

	if !localComplete {
		return false, RefusalIncompleteMaterial, "secure peer refused, local material is incomplete"
	}

	return false, RefusalNoHandshake, "secure peer with complete material refused, Phase 1 cannot authenticate anyone so flag-on networks cannot form yet"
}

// CheckRemoteParticipant allows an authenticated peer when enabled.
//
// It parses peer properties for loud warnings, ensures local material,
// records the peer, logs with identity plus reason per INV-10, and
// bumps the refusal counter. Disabled suites return nil silently.
// Verified governance plus permissions documents with an authenticated
// peer and matching domain rule allow; all other enabled peers deny.
func (s *Suite) CheckRemoteParticipant(peer PeerDescriptor) error {
	if !s.Enabled() {
		return nil
	}

	props, warnings := ParseParticipantProperties(peer.Properties)

	for _, warning := range warnings {
		log.Printf("security: peer=%s property warn: %s", peer.Identity, warning)
	}

	refusal, reason := s.decideRemote(peer, props)

	if refusal == RefusalNoHandshake || refusal == RefusalUnsecurePeer {
		if s.tryAllowParticipant(peer, props) {
			return s.allow(peer.Identity, peer.Class, "check_remote_participant", "participant allowed by governance plus permissions")
		}

		if vRefusal, vReason, ok := s.verifiedDenyParticipant(peer, props); ok {
			refusal, reason = vRefusal, vReason
		}
	}

	err := s.controller.CheckRemoteParticipant(peer.Identity)
	if err == nil {
		err = ErrNotAllowedBySecurity
	}

	return s.refuse(peer.Identity, peer.Class, refusal, reason, "check_remote_participant", err)
}

// CheckRemoteEndpoint allows a matching endpoint when enabled.
//
// Topic and reader direction enter the audit reason and the endpoint
// check shape. Disabled suites return nil silently. Verified documents
// with an authenticated or explicitly unauthenticated-open peer plus
// matching domain and topic rules allow; all other enabled endpoints
// deny with identity plus reason plus counter.
func (s *Suite) CheckRemoteEndpoint(peer PeerDescriptor, topic string, isReader bool) error {
	if !s.Enabled() {
		return nil
	}

	props, warnings := ParseParticipantProperties(peer.Properties)

	for _, warning := range warnings {
		log.Printf("security: peer=%s property warn: %s", peer.Identity, warning)
	}

	refusal, reason := s.decideRemote(peer, props)

	if refusal == RefusalNoHandshake || refusal == RefusalUnsecurePeer {
		if s.tryAllowEndpoint(peer, topic, isReader, props) {
			return s.allow(peer.Identity, peer.Class, "check_remote_endpoint", "endpoint "+topic+" allowed by governance plus permissions")
		}

		if vRefusal, vReason, ok := s.verifiedDenyEndpoint(peer, topic, isReader, props); ok {
			refusal, reason = vRefusal, vReason
		}
	}

	direction := "writer"

	if isReader {
		direction = "reader"
	}

	full := "endpoint " + topic + " " + direction + ": " + reason

	var err error

	if isReader {
		err = s.controller.CheckRemoteDataReader(peer.Identity, topic)
	} else {
		err = s.controller.CheckRemoteDataWriter(peer.Identity, topic)
	}

	if err == nil {
		err = ErrNotAllowedBySecurity
	}

	return s.refuse(peer.Identity, peer.Class, refusal, full, "check_remote_endpoint", err)
}

// CheckCreateParticipant allows local creation with verified material.
//
// Disabled suites return nil silently. Verified local identity plus
// governance domain coverage allow; all other enabled creates deny.
// The domain defaults to zero; domain-aware callers use
// CheckCreateParticipantInDomain.
func (s *Suite) CheckCreateParticipant(identity string) error {
	return s.CheckCreateParticipantInDomain(identity, 0)
}

// CheckCreateParticipantInDomain allows local creation in one domain.
//
// Verified local identity plus a live grant covering the domain
// allow; all other enabled creates deny.
func (s *Suite) CheckCreateParticipantInDomain(identity string, domainID uint32) error {
	return s.CheckCreateParticipantInDomainacc8Scoped(identity, domainID, nil, nil)
}

// CheckCreateParticipantInDomainacc8Scoped allows local creation with scope.
//
// Partitions and tags gate tag-scoped grants: a tag-gated grant denies
// nil scope while allowing supplied scope. Disabled suites allow
// silently.
func (s *Suite) CheckCreateParticipantInDomainacc8Scoped(identity string, domainID uint32, partitions, tags []string) error {
	if !s.Enabled() {
		return nil
	}

	refusal, reason := s.decideLocal("participant " + identity)

	if refusal == RefusalNoHandshake {
		if s.tryAllowLocalCreateacc8Scoped(identity, "", domainID, partitions, tags) {
			return s.allow(identity, "", "check_create_participant", "local participant allowed by governance plus permissions")
		}
	}

	err := s.controller.CheckCreateParticipant(identity)
	if err == nil {
		err = ErrNotAllowedBySecurity
	}

	return s.refuse(identity, "", refusal, reason, "check_create_participant", err)
}

// CheckCreateEndpoint allows local endpoint creation with verified material.
//
// Disabled suites return nil silently. Verified local identity plus
// matching domain and topic rules allow; all other enabled declares deny.
// The domain defaults to zero; domain-aware callers use
// CheckCreateEndpointInDomain.
func (s *Suite) CheckCreateEndpoint(identity, topic string, isReader bool) error {
	return s.CheckCreateEndpointInDomain(identity, topic, isReader, 0)
}

// CheckCreateEndpointInDomain allows local endpoint creation in one domain.
//
// Verified local identity plus the grant for the local subject covering
// the domain and topic allow; all other enabled declares deny.
func (s *Suite) CheckCreateEndpointInDomain(identity, topic string, isReader bool, domainID uint32) error {
	return s.CheckCreateEndpointInDomainacc8Scoped(identity, topic, isReader, domainID, nil, nil)
}

// CheckCreateEndpointInDomainacc8Scoped allows local endpoint creation with scope.
//
// Direction follows isReader: readers need subscribe, writers need
// publish, so a subscribe-only grant denies a publish create. Partitions
// and tags gate tag-scoped grants: nil scope denies while supplied
// scope allows. Disabled suites allow silently.
func (s *Suite) CheckCreateEndpointInDomainacc8Scoped(identity, topic string, isReader bool, domainID uint32, partitions, tags []string) error {
	if !s.Enabled() {
		return nil
	}

	direction := "writer"

	if isReader {
		direction = "reader"
	}

	refusal, reason := s.decideLocal(direction + " " + topic)

	if refusal == RefusalNoHandshake {
		if s.tryAllowLocalEndpointacc8Scoped(identity, topic, isReader, domainID, partitions, tags) {
			return s.allow(identity, "", "check_create_endpoint", "local "+direction+" "+topic+" allowed by governance plus permissions")
		}
	}

	full := "local " + direction + " " + topic + ": " + reason

	var err error

	if isReader {
		err = s.controller.CheckCreateDataReader(identity, topic)
	} else {
		err = s.controller.CheckCreateDataWriter(identity, topic)
	}

	if err == nil {
		err = ErrNotAllowedBySecurity
	}

	return s.refuse(identity, "", refusal, full, "check_create_endpoint", err)
}

// CheckLocalDataWriterMatch gates matching a local writer with a remote reader.
//
// Disabled suites allow silently. Verified documents with matching
// domain and topic rules allow; otherwise deny per INV-1. The domain
// defaults to zero; domain-aware callers use
// CheckLocalDataWriterMatchInDomain.
func (s *Suite) CheckLocalDataWriterMatch(local, remote, topic string) error {
	return s.CheckLocalDataWriterMatchInDomain(local, remote, topic, 0)
}

// CheckLocalDataWriterMatchInDomain gates a local writer match in one domain.
func (s *Suite) CheckLocalDataWriterMatchInDomain(local, remote, topic string, domainID uint32) error {
	return s.CheckLocalDataWriterMatchInDomainacc8Scoped(local, remote, topic, domainID, nil, nil, "")
}

// CheckLocalDataWriterMatchInDomainacc8Scoped gates a local writer match
// with partitions, tags, and type consistency per 9.4.2.5 and 7.6.3.4.
//
// Partitions and tags thread into the grant lookup so tag-gated grants
// deny nil scope; readerType/writerType enforce minimal type assignability
// (exact name match, TODO engine for full XTypes).
func (s *Suite) CheckLocalDataWriterMatchInDomainacc8Scoped(local, remote, topic string, domainID uint32, partitions, tags []string, writerType string) error {
	return s.checkLocalMatchacc9(false, local, remote, topic, domainID, partitions, tags, writerType)
}

// CheckLocalDataReaderMatch gates matching a local reader with a remote writer.
//
// Disabled suites allow silently. Verified documents with matching
// domain and topic rules allow; otherwise deny per INV-1. The domain
// defaults to zero; domain-aware callers use
// CheckLocalDataReaderMatchInDomain.
func (s *Suite) CheckLocalDataReaderMatch(local, remote, topic string) error {
	return s.CheckLocalDataReaderMatchInDomain(local, remote, topic, 0)
}

// CheckLocalDataReaderMatchInDomain gates a local reader match in one domain.
func (s *Suite) CheckLocalDataReaderMatchInDomain(local, remote, topic string, domainID uint32) error {
	return s.CheckLocalDataReaderMatchInDomainacc8Scoped(local, remote, topic, domainID, nil, nil, "")
}

// CheckLocalDataReaderMatchInDomainacc8Scoped gates a local reader match
// with partitions, tags, and type consistency.
func (s *Suite) CheckLocalDataReaderMatchInDomainacc8Scoped(local, remote, topic string, domainID uint32, partitions, tags []string, readerType string) error {
	return s.checkLocalMatchacc9(true, local, remote, topic, domainID, partitions, tags, readerType)
}

// checkLocalMatchacc9 gates one local match with scope plus type checks.
func (s *Suite) checkLocalMatchacc9(isReader bool, local, remote, topic string, domainID uint32, partitions, tags []string, typeName string) error {
	if !s.Enabled() {
		return nil
	}

	kind := "writer"
	op := "check_local_datawriter_match"
	if isReader {
		kind = "reader"
		op = "check_local_datareader_match"
	}

	if typeName != "" {
		err := access.CheckTypeConsistencyacc9(typeName, typeName, true)
		if err != nil {
			return s.refuse(local, "", RefusalNoHandshake, "local "+kind+" "+topic+" type inconsistent", op, err)
		}
	}

	if s.tryAllowLocalEndpointacc8Scoped(local, topic, isReader, domainID, partitions, tags) {
		return s.allow(local, "", op, "local "+kind+" "+topic+" matches "+remote)
	}

	var err error
	if isReader {
		err = s.controller.CheckLocalDataReaderMatch(local, remote, topic)
	} else {
		err = s.controller.CheckLocalDataWriterMatch(local, remote, topic)
	}
	if err == nil {
		err = ErrNotAllowedBySecurity
	}

	return s.refuse(local, "", RefusalNoHandshake, "local "+kind+" "+topic+" refused", op, err)
}

// ValidateLocalPermissions checks local permissions material per 9.4.2.9.
//
// Disabled suites delegate to the controller. Enabled suites require
// verified documents plus a live grant for the local subject covering
// the domain; otherwise they deny fail-closed.
func (s *Suite) ValidateLocalPermissions(identity string, domainID uint32) error {
	if !s.Enabled() {
		err := s.controller.ValidateLocalPermissions(identity)
		if err != nil {
			return fmt.Errorf("security: validate local permissions: %w", err)
		}

		return nil
	}

	_, grant, ok := s.verifiedLocalRuleGrant(domainID)
	if !ok || grant == nil {
		err := s.controller.ValidateLocalPermissions(identity)
		if err == nil {
			err = ErrNotAllowedBySecurity
		}

		return s.refuse(identity, "", RefusalNoHandshake, "local permissions invalid for domain", "validate_local_permissions", err)
	}

	return s.allow(identity, "", "validate_local_permissions", "local permissions validated")
}

// ValidateRemotePermissions checks remote permissions material per 9.4.2.9.
func (s *Suite) ValidateRemotePermissions(identity string, domainID uint32) error {
	if !s.Enabled() {
		err := s.controller.ValidateRemotePermissions(identity)
		if err != nil {
			return fmt.Errorf("security: validate remote permissions: %w", err)
		}

		return nil
	}

	subject, refusal, reason := s.remoteSubject(identity)
	if subject == "" {
		err := s.controller.ValidateRemotePermissions(identity)
		if err == nil {
			err = ErrNotAllowedBySecurity
		}

		return s.refuse(identity, "", refusal, reason, "validate_remote_permissions", err)
	}

	grant, refusal, reason := s.remoteGrant(subject)
	if grant == nil {
		err := s.controller.ValidateRemotePermissions(identity)
		if err == nil {
			err = ErrNotAllowedBySecurity
		}

		return s.refuse(identity, "", refusal, reason, "validate_remote_permissions", err)
	}

	_ = domainID

	return s.allow(identity, "", "validate_remote_permissions", "remote permissions validated")
}

// remoteSubject returns the authenticated subject or the refusal reason.
func (s *Suite) remoteSubject(identity string) (string, RefusalClass, string) {
	if !s.isAuthenticated(identity) {
		return "", RefusalNoHandshake, "remote permissions unauthenticated"
	}

	subject := s.authenticatedSubject(identity)
	if subject == "" {
		return "", RefusalNoHandshake, "remote permissions subject missing"
	}

	return subject, "", ""
}

// remoteGrant returns the live grant for one subject or the refusal reason.
func (s *Suite) remoteGrant(subject string) (*access.Grant, RefusalClass, string) {
	gov, perms, _, ok, _ := s.verifiedState()
	if !ok || gov == nil || perms == nil {
		return nil, RefusalIncompleteMaterial, "remote permissions unverified"
	}

	grant := access.GrantForSubject(perms, subject, time.Now())
	if grant == nil {
		return nil, RefusalNoHandshake, "remote permissions grant missing or expired"
	}

	return grant, "", ""
}

// CheckCreateTopic gates local topic creation per 9.4.2.9.
func (s *Suite) CheckCreateTopic(identity, topic string, domainID uint32) error {
	if !s.Enabled() {
		err := s.controller.CheckCreateTopic(identity, topic)
		if err != nil {
			return fmt.Errorf("security: check create topic: %w", err)
		}

		return nil
	}

	rule, grant, ok := s.verifiedLocalRuleGrant(domainID)
	if !ok || rule == nil || grant == nil {
		err := s.controller.CheckCreateTopic(identity, topic)
		if err == nil {
			err = ErrNotAllowedBySecurity
		}

		return s.refuse(identity, "", RefusalNoHandshake, "local topic "+topic+" refused", "check_create_topic", err)
	}

	if access.FindTopicRule(rule, topic) == nil {
		err := s.controller.CheckCreateTopic(identity, topic)
		if err == nil {
			err = ErrNotAllowedBySecurity
		}

		return s.refuse(identity, "", RefusalNoHandshake, "local topic "+topic+" not covered", "check_create_topic", err)
	}

	return s.allow(identity, "", "check_create_topic", "local topic "+topic+" allowed")
}

// CheckLocalDataWriterRegisterInstance gates writer instance registration.
func (s *Suite) CheckLocalDataWriterRegisterInstance(identity, topic string, domainID uint32) error {
	if !s.Enabled() {
		err := s.controller.CheckLocalDataWriterRegisterInstance(identity, topic)
		if err != nil {
			return fmt.Errorf("security: check register instance: %w", err)
		}

		return nil
	}

	if s.tryAllowLocalEndpoint(identity, topic, false, domainID) {
		return s.allow(identity, "", "check_local_datawriter_register_instance", "local writer "+topic+" instance allowed")
	}

	err := s.controller.CheckLocalDataWriterRegisterInstance(identity, topic)
	if err == nil {
		err = ErrNotAllowedBySecurity
	}

	return s.refuse(identity, "", RefusalNoHandshake, "local writer "+topic+" instance refused", "check_local_datawriter_register_instance", err)
}

// CheckRemoteTopic gates remote topic matching per 9.4.2.9.
func (s *Suite) CheckRemoteTopic(peer PeerDescriptor, topic string) error {
	if !s.Enabled() {
		err := s.controller.CheckRemoteTopic(peer.Identity, topic)
		if err != nil {
			return fmt.Errorf("security: check remote topic: %w", err)
		}

		return nil
	}

	props, _ := ParseParticipantProperties(peer.Properties)

	if s.tryAllowEndpoint(peer, topic, false, props) {
		return s.allow(peer.Identity, peer.Class, "check_remote_topic", "remote topic "+topic+" allowed")
	}

	if s.tryAllowEndpoint(peer, topic, true, props) {
		return s.allow(peer.Identity, peer.Class, "check_remote_topic", "remote topic "+topic+" allowed")
	}

	err := s.controller.CheckRemoteTopic(peer.Identity, topic)
	if err == nil {
		err = ErrNotAllowedBySecurity
	}

	return s.refuse(peer.Identity, peer.Class, RefusalNoHandshake, "remote topic "+topic+" refused", "check_remote_topic", err)
}

// RekeyOnEnableKeyRevision triggers staged rekey when governance demands it.
//
// When the domain rule for domainID carries EnableKeyRevision, the caller
// must stage fresh material via Revise before use and Activate only after
// the volatile distribution acknowledges or times out. This helper reports
// whether a rekey is required; the crypto factory Revise plus Activate
// pair performs the hold. Domains without the flag need no rekey.
func RekeyOnEnableKeyRevision(gov *access.GovernanceDoc, domainID uint32, tag string) bool {
	if gov == nil {
		return false
	}

	rule := access.FindDomainRule(gov, domainID, tag)
	if rule == nil {
		return false
	}

	return rule.EnableKeyRevision
}

// GetParticipantSecurityConfig decides the participant config.
//
// Disabled suites delegate to the controller. Enabled suites return the
// decided config from verified governance when signatures verify;
// otherwise they deny as before. The domain defaults to zero; see
// GetParticipantSecurityConfigInDomain.
func (s *Suite) GetParticipantSecurityConfig(identity string) (access.ParticipantSecurityConfig, error) {
	return s.GetParticipantSecurityConfigInDomain(identity, 0)
}

// GetParticipantSecurityConfigInDomain decides the participant config.
//
// The domain rule for domainID supplies the protection kinds instead
// of the hardcoded domain zero rule.
func (s *Suite) GetParticipantSecurityConfigInDomain(identity string, domainID uint32) (access.ParticipantSecurityConfig, error) {
	if !s.Enabled() {
		cfg, err := s.controller.GetParticipantSecurityConfig(identity)
		if err != nil {
			return access.ParticipantSecurityConfig{}, fmt.Errorf("security: get participant config: %w", err)
		}

		return cfg, nil
	}

	gov, _, _, ok, _ := s.verifiedState()
	if !ok || gov == nil {
		return s.deniedParticipantConfig(identity)
	}

	rule := access.FindDomainRule(gov, domainID, "")
	if rule == nil {
		return s.deniedParticipantConfig(identity)
	}

	cfg := access.ParticipantSecurityConfig{
		Identity:             identity,
		DiscoveryProtection:  rule.DiscoveryProtection,
		LivelinessProtection: rule.LivelinessProtection,
		RTPSProtection:       rule.RTPSProtection,
		AllowUnauthenticated: rule.AllowUnauthenticated,
	}

	s.noteAllow(identity, "", "get_participant_security_config", "participant config decided")

	return cfg, nil
}

// deniedParticipantConfig denies one participant config getter.
func (s *Suite) deniedParticipantConfig(identity string) (access.ParticipantSecurityConfig, error) {
	cfg, err := s.controller.GetParticipantSecurityConfig(identity)

	denyErr := s.denyConfigGetter("get_participant_security_config", "participant "+identity, identity, err)
	if denyErr != nil {
		return access.ParticipantSecurityConfig{}, denyErr
	}

	if err != nil {
		return access.ParticipantSecurityConfig{}, fmt.Errorf("security: get participant config: %w", err)
	}

	return cfg, nil
}

// GetTopicSecurityConfig decides the topic config.
//
// Disabled suites delegate to the controller. Enabled suites return the
// decided config from the domain rule covering the domain; otherwise
// they deny as before. The domain defaults to zero; see
// GetTopicSecurityConfigInDomain.
func (s *Suite) GetTopicSecurityConfig(topic string) (access.TopicSecurityConfig, error) {
	return s.GetTopicSecurityConfigInDomain(topic, 0)
}

// GetTopicSecurityConfigInDomain decides the topic config in one domain.
func (s *Suite) GetTopicSecurityConfigInDomain(topic string, domainID uint32) (access.TopicSecurityConfig, error) {
	if !s.Enabled() {
		cfg, err := s.controller.GetTopicSecurityConfig(topic)
		if err != nil {
			return access.TopicSecurityConfig{}, fmt.Errorf("security: get topic config: %w", err)
		}

		return cfg, nil
	}

	gov, _, _, ok, _ := s.verifiedState()
	if !ok || gov == nil {
		return s.deniedTopicConfig(topic)
	}

	rule := access.FindDomainRule(gov, domainID, "")
	if rule == nil {
		return s.deniedTopicConfig(topic)
	}

	topicRule := access.FindTopicRule(rule, topic)
	if topicRule == nil {
		return s.deniedTopicConfig(topic)
	}

	cfg := access.TopicSecurityConfig{
		Topic:              topic,
		MetadataProtection: topicRule.MetadataProtection,
		DataProtection:     topicRule.DataProtection,
		EnableDiscovery:    topicRule.EnableDiscovery,
		EnableLiveliness:   topicRule.EnableLiveliness,
		EnableReadAccess:   topicRule.EnableReadAccess,
		EnableWriteAccess:  topicRule.EnableWriteAccess,
	}

	s.noteAllow(topic, "", "get_topic_security_config", "topic config decided")

	return cfg, nil
}

// deniedTopicConfig denies one topic config getter.
func (s *Suite) deniedTopicConfig(topic string) (access.TopicSecurityConfig, error) {
	cfg, err := s.controller.GetTopicSecurityConfig(topic)

	denyErr := s.denyConfigGetter("get_topic_security_config", "topic "+topic, topic, err)
	if denyErr != nil {
		return access.TopicSecurityConfig{}, denyErr
	}

	if err != nil {
		return access.TopicSecurityConfig{}, fmt.Errorf("security: get topic config: %w", err)
	}

	return cfg, nil
}

// GetEndpointSecurityConfig decides the endpoint config.
//
// Disabled suites delegate to the controller. Enabled suites return the
// decided config from verified governance when the topic matches and
// the grant for the local subject covers the domain and topic;
// otherwise they deny as before. The domain defaults to zero; see
// GetEndpointSecurityConfigInDomain.
func (s *Suite) GetEndpointSecurityConfig(endpoint, topic string, isReader bool) (access.EndpointSecurityConfig, error) {
	return s.GetEndpointSecurityConfigInDomain(endpoint, topic, isReader, 0)
}

// GetEndpointSecurityConfigInDomain decides the endpoint config.
//
// Like the Check paths it runs GrantForSubject for the local subject:
// a matching topic rule alone never allows without a live grant
// covering the domain and topic.
func (s *Suite) GetEndpointSecurityConfigInDomain(endpoint, topic string, isReader bool, domainID uint32) (access.EndpointSecurityConfig, error) {
	if !s.Enabled() {
		cfg, err := s.controller.GetEndpointSecurityConfig(endpoint, topic, isReader)
		if err != nil {
			return access.EndpointSecurityConfig{}, fmt.Errorf("security: get endpoint config: %w", err)
		}

		return cfg, nil
	}

	gov, perms, localCert, ok, _ := s.verifiedState()
	if !ok || gov == nil || perms == nil || localCert == nil {
		return s.deniedEndpointConfig(endpoint, topic, isReader)
	}

	rule := access.FindDomainRule(gov, domainID, "")
	if rule == nil {
		return s.deniedEndpointConfig(endpoint, topic, isReader)
	}

	topicRule := access.FindTopicRule(rule, topic)
	if topicRule == nil {
		return s.deniedEndpointConfig(endpoint, topic, isReader)
	}

	subject := auth.CertificateSubjectFullauth7(localCert)
	grant := access.GrantForSubject(perms, subject, time.Now())

	if grant == nil {
		return s.deniedEndpointConfig(endpoint, topic, isReader)
	}

	var allowed bool
	if isReader {
		allowed, _ = access.AllowsSubscribeacc8(grant, domainID, topic, nil, nil)
	} else {
		allowed, _ = access.AllowsPublishacc8(grant, domainID, topic, nil, nil)
	}

	if !allowed {
		return s.deniedEndpointConfig(endpoint, topic, isReader)
	}

	cfg := access.EndpointSecurityConfig{
		Endpoint:           endpoint,
		Topic:              topic,
		IsReader:           isReader,
		Allowed:            true,
		DataProtection:     topicRule.DataProtection,
		MetadataProtection: topicRule.MetadataProtection,
	}

	s.noteAllow(endpoint, "", "get_endpoint_security_config", "endpoint config decided")

	return cfg, nil
}

// deniedEndpointConfig denies one endpoint config getter.
func (s *Suite) deniedEndpointConfig(endpoint, topic string, isReader bool) (access.EndpointSecurityConfig, error) {
	cfg, err := s.controller.GetEndpointSecurityConfig(endpoint, topic, isReader)

	denyErr := s.denyConfigGetter("get_endpoint_security_config", "endpoint "+topic, endpoint, err)
	if denyErr != nil {
		return access.EndpointSecurityConfig{}, denyErr
	}

	if err != nil {
		return access.EndpointSecurityConfig{}, fmt.Errorf("security: get endpoint config: %w", err)
	}

	return cfg, nil
}

// denyConfigGetter denies one config getter when enabled.
//
// It returns nil while disabled so the caller falls through to the
// controller result. While enabled it records, counts, and logs the
// denial and returns the wrapped error.
func (s *Suite) denyConfigGetter(op, what, identity string, opErr error) error {
	if !s.Enabled() {
		return nil
	}

	refusal, reason := s.decideLocal(what)

	if opErr == nil {
		opErr = ErrNotAllowedBySecurity
	}

	return s.refuse(identity, "", refusal, "no config at Phase 1 for "+what+": "+reason, op, opErr)
}

// materialReport is the cached local material verdict.
type materialReport struct {
	// complete is true when both documents parsed.
	complete bool
	// refusal classifies a load failure.
	refusal RefusalClass
	// reason explains a load failure for audit.
	reason string
}

// decideRemote computes the remote verdict from peer flags and material.
//
// Peer-local causes outrank local files: malformed properties, unknown
// algorithms, and unsecure posture refuse without file IO, so the audit
// names the peer cause first and plain peers cost no reads. Secure
// peers with clean properties consult cached local material, then
// refuse for lack of handshake.
func (s *Suite) decideRemote(peer PeerDescriptor, props ParticipantProperties) (RefusalClass, string) {
	_, refusal, reason := DecideRemotePeer(true, peer.Class, true, props.Malformed, props.UnknownAlgorithm)

	if refusal != RefusalNoHandshake {
		return refusal, reason
	}

	report := s.material()
	if !report.complete {
		return report.refusal, report.reason
	}

	return refusal, reason
}

// decideLocal computes the local create verdict from material only.
func (s *Suite) decideLocal(what string) (RefusalClass, string) {
	report := s.material()

	if !report.complete {
		return report.refusal, report.reason
	}

	return RefusalNoHandshake, "local " + what + " refused, Phase 1 cannot authorize creates so flag-on networks cannot form yet"
}

// refuse records, counts, logs, and wraps one denial per INV-10.
//
// opErr carries the operation check failure; callers substitute the
// NOT_ALLOWED sentinel when the check unexpectedly allows so denial
// stays total.
func (s *Suite) refuse(identity string, class PeerClass, refusal RefusalClass, reason, op string, opErr error) error {
	s.count(refusal)
	s.record(identity, class, string(refusal), reason)
	log.Printf("security: deny identity=%s class=%s op=%s reason=%s", identity, class, op, reason)

	if s != nil && s.logger != nil {
		s.logger.Log(op, identity, false, reason)
	}

	return fmt.Errorf("security: identity=%s %s: %w", identity, reason, opErr)
}

// allow records, logs, and returns one grant per INV-10.
func (s *Suite) allow(identity string, class PeerClass, op, reason string) error {
	s.noteAllow(identity, class, op, reason)

	return nil
}

// noteAllow records one grant without a return for audit-only callers.
func (s *Suite) noteAllow(identity string, class PeerClass, op, reason string) {
	s.record(identity, class, "allow", reason)
	log.Printf("security: allow identity=%s class=%s op=%s reason=%s", identity, class, op, reason)

	if s != nil && s.logger != nil {
		s.logger.Log(op, identity, true, reason)
	}
}

// AuditLog returns recorded allow and deny events per INV-10.
func (s *Suite) AuditLog() []logging.Event {
	if s == nil || s.logger == nil {
		return nil
	}

	return s.logger.Entries()
}

// AuthenticatePeer validates one peer certificate and records it.
//
// Chain builds to the configured identity CA with stdlib x509, expiry
// and key usage enforced. The caller identity is then bound to the
// certificate: GUID-form identities must equal the adjusted GUID
// prefix derived from the certificate, so a peer cannot present
// another identity certificate under our GUID. Opaque labels are
// pinned bidirectionally instead: one identity keeps its first
// certificate and one certificate keeps its first identity, so a
// re-auth under a different certificate displacing a live binding is
// refused. A certificate listed in the configured CRL refuses.
// Failures deny loudly and never record.
func (s *Suite) AuthenticatePeer(identity string, certPEM []byte) error {
	if !s.Enabled() {
		return nil
	}

	if s == nil {
		return fmt.Errorf("security: nil suite cannot authenticate: %w", ErrNotAllowedBySecurity)
	}

	identityPool, revoked, ok, reason := s.identityTrust()
	if !ok || identityPool == nil {
		err := s.refuse(identity, "", RefusalIncompleteMaterial, "authenticate without verified material: "+reason, "authenticate_peer", ErrNotAllowedBySecurity)

		return err
	}

	cert, err := auth.ParseCertificatePEM(certPEM)
	if err != nil {
		return s.refuse(identity, "", RefusalMalformed, "authenticate bad certificate: "+err.Error(), "authenticate_peer", err)
	}

	err = auth.ValidateIdentity(cert, identityPool, time.Now(), "")
	if err != nil {
		return s.refuse(identity, "", RefusalMalformed, "authenticate identity unverified: "+err.Error(), "authenticate_peer", err)
	}

	if certRevoked(revoked, cert) {
		err = fmt.Errorf("security: authenticate revoked certificate serial %s: %w", cert.SerialNumber.String(), ErrNotAllowedBySecurity)

		return s.refuse(identity, "", RefusalMalformed, "authenticate revoked certificate", "authenticate_peer", err)
	}

	err = s.bindIdentity(identity, cert)
	if err != nil {
		return s.refuse(identity, "", RefusalMalformed, "authenticate binding refused: "+err.Error(), "authenticate_peer", err)
	}

	s.record(identity, "", "allow", "peer authenticated")
	log.Printf("security: allow identity=%s op=authenticate_peer reason=peer authenticated", identity)

	if s.logger != nil {
		s.logger.Log("authenticate_peer", identity, true, "peer authenticated")
	}

	return nil
}

// canonicalIdentity strips backend mangling per 9.3.2.
//
// The backend mangles authenticated peer identities as
// LocalID+">"+hexPrefix (see backend.go authorizeRemoteParticipant).
// Suite lookups must handle both mangled and unmangled keys, so the
// canonical form is the suffix after the last ">", or the identity
// itself when no ">" is present.
func canonicalIdentity(identity string) string {
	if idx := strings.LastIndex(identity, ">"); idx >= 0 {
		return identity[idx+1:]
	}

	return identity
}

// bindIdentity pins one caller identity to one certificate.
//
// A re-auth under a different certificate displacing a live binding
// is refused, and one certificate cannot bind a second identity, so a
// validated peer cannot squat another identity. GUID-form identities
// must equal the adjusted GUID prefix of the certificate; opaque
// labels carry no cryptographic meaning and are pinned as-is. Backend
// mangling (LocalID+">"+hex) stores both mangled and unmangled keys:
// identities sharing the same canonical base may share one
// certificate, but distinct bases never share.
func (s *Suite) bindIdentity(identity string, cert *x509.Certificate) error {
	sum := sha256.Sum256(cert.Raw)
	fingerprint := hex.EncodeToString(sum[:])
	base := canonicalIdentity(identity)

	if err := checkGUIDIdentityBinding(base, identity, cert); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	initBindIdentityMaps(s)
	if err := checkIdentityBindings(s, identity, base, fingerprint); err != nil {
		return err
	}
	storeIdentityBindings(s, identity, base, fingerprint, cert)
	return nil
}

// checkGUIDIdentityBinding validates GUID identities against the certificate.
func checkGUIDIdentityBinding(base, identity string, cert *x509.Certificate) error {
	if !isGUIDIdentity(base) {
		return nil
	}
	prefix, err := auth.AdjustedGUIDPrefix(cert)
	if err != nil {
		return fmt.Errorf("security: adjusted GUID without certificate: %w", err)
	}
	if !strings.EqualFold(base, hex.EncodeToString(prefix[:])) {
		return fmt.Errorf("security: identity %q does not match certificate GUID: %w", identity, ErrNotAllowedBySecurity)
	}
	return nil
}

// initBindIdentityMaps lazily creates binding maps.
func initBindIdentityMaps(s *Suite) {
	if s.authenticated == nil {
		s.authenticated = make(map[string]*x509.Certificate)
	}
	if s.boundCert == nil {
		s.boundCert = make(map[string]string)
	}
	if s.boundIdentity == nil {
		s.boundIdentity = make(map[string]string)
	}
}

// checkIdentityBindings rejects conflicting certificate bindings.
func checkIdentityBindings(s *Suite, identity, base, fingerprint string) error {
	if previous, ok := s.boundCert[identity]; ok && previous != fingerprint {
		return fmt.Errorf("security: identity %q already bound to another certificate: %w", identity, ErrNotAllowedBySecurity)
	}
	if base != identity {
		if previous, ok := s.boundCert[base]; ok && previous != fingerprint {
			return fmt.Errorf("security: identity %q already bound to another certificate: %w", base, ErrNotAllowedBySecurity)
		}
	}
	if previous, ok := s.boundIdentity[fingerprint]; ok && previous != identity && previous != base && canonicalIdentity(previous) != base {
		return fmt.Errorf("security: certificate already bound to identity %q: %w", previous, ErrNotAllowedBySecurity)
	}
	return nil
}

// storeIdentityBindings records the validated bindings.
func storeIdentityBindings(s *Suite, identity, base, fingerprint string, cert *x509.Certificate) {
	s.boundCert[identity] = fingerprint
	if base != identity {
		s.boundCert[base] = fingerprint
	}
	s.boundIdentity[fingerprint] = base
	s.authenticated[identity] = cert
	if base != identity {
		s.authenticated[base] = cert
	}
}

// isGUIDIdentity reports whether an identity carries a GUID prefix.
//
// Production callers pass the 12 byte participant GUID prefix as 24
// hex characters; opaque labels such as test names take the pinned
// path in bindIdentity instead.
func isGUIDIdentity(identity string) bool {
	if len(identity) != 24 {
		return false
	}

	for i := range len(identity) {
		c := identity[i]
		if c >= '0' && c <= '9' {
			continue
		}

		if c >= 'a' && c <= 'f' {
			continue
		}

		if c >= 'A' && c <= 'F' {
			continue
		}

		return false
	}

	return true
}

// isAuthenticated reports whether one peer completed validation.
//
// When a handshake reporter is wired, both the certificate validation
// map and the handshake Authenticated state must agree per 9.3.2; an
// unsigned or incomplete handshake never authorizes even with a
// validated certificate. Without a reporter the validation map alone
// decides for unit fixtures. Backend mangling (LocalID+">"+hex) is
// handled: direct, canonical-base, and reporter checks for both forms
// are tried so mangled and unmangled keys both authorize.
func (s *Suite) isAuthenticated(identity string) bool {
	if s == nil {
		return false
	}

	base := canonicalIdentity(identity)

	s.mu.Lock()
	_, ok := s.authenticated[identity]
	if !ok && base != identity {
		_, ok = s.authenticated[base]
	}
	reporter := s.handshake
	s.mu.Unlock()

	if !ok {
		return false
	}

	if reporter == nil {
		return true
	}

	if reporter.Authenticated(identity) {
		return true
	}

	if base != identity && reporter.Authenticated(base) {
		return true
	}

	return false
}

// authenticatedSubject returns the full DN bound to one peer.
//
// Full DN binding per 9.4 distinguishes same-CN certificates from
// different organizations; CN-only lookup would conflate them. Mangled
// identities resolve via the canonical base.
func (s *Suite) authenticatedSubject(identity string) string {
	if s == nil {
		return ""
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cert, ok := s.authenticated[identity]
	if (!ok || cert == nil) && canonicalIdentity(identity) != identity {
		cert, ok = s.authenticated[canonicalIdentity(identity)]
	}
	if !ok || cert == nil {
		return ""
	}

	return auth.CertificateSubjectFullauth7(cert)
}

// verifiedState loads and caches signature-verified material.
//
// Unsigned, mis-signed, expired, or unchained documents fail closed
// with verifiedOK false while leaving the Phase 1 parse cache
// untouched, so legacy unsigned fixtures keep their historical
// refusal classes. Only fully verified documents gate allow paths.
// Cached material older than verifiedStaleAfteracc8 reloads, and any
// file newer than the last load reloads immediately, so rotated
// governance, permissions, identity, CA, or CRL files flip decisions
// without a restart. Use Reloadacc8Cache for an explicit reload.
func (s *Suite) verifiedState() (*access.GovernanceDoc, *access.PermissionsDoc, *x509.Certificate, bool, string) {
	if s == nil {
		return nil, nil, nil, false, "nil suite"
	}

	s.mu.Lock()
	loaded := s.verifiedLoaded
	sinceLoad := time.Since(s.verifiedAt)
	gov := s.verifiedGov
	perms := s.verifiedPerms
	identity := s.identity
	ok := s.verifiedOK
	reason := s.verifiedReason
	cfg := s.config
	s.mu.Unlock()

	if loaded && sinceLoad < verifiedStaleAfteracc8 && !verifiedFilesChanged(cfg, s.verifiedAtTime()) {
		return gov, perms, identity, ok, reason
	}

	gov, perms, _, identityPool, identityCAs, identity, revoked, ok, reason := loadVerified(cfg)

	s.mu.Lock()
	s.verifiedLoaded = true
	s.verifiedGov = gov
	s.verifiedPerms = perms
	s.identityPool = identityPool
	s.identityCAs = identityCAs
	s.identity = identity
	if revoked != nil {
		s.revoked = revoked
	}
	s.verifiedOK = ok
	s.verifiedReason = reason
	s.verifiedAt = time.Now()
	s.mu.Unlock()

	return gov, perms, identity, ok, reason
}

// verifiedAtTime returns the last verified load time under lock.
func (s *Suite) verifiedAtTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.verifiedAt
}

// verifiedFilesChanged reports whether any material file changed after loadedAt.
//
// Stat errors fail closed to reload: a deleted or unreadable file forces
// a reload attempt, and the load itself fails closed when files are
// unreadable, so missing material denies instead of serving stale allows.
func verifiedFilesChanged(cfg SuiteConfig, loadedAt time.Time) bool {
	if loadedAt.IsZero() {
		return true
	}

	for _, path := range []string{cfg.GovernancePath, cfg.PermissionsDocumentPath, cfg.IdentityCertificatePath, cfg.PermissionsCAPath, cfg.IdentityCAPath, cfg.CRLPath} {
		if path == "" {
			continue
		}

		info, err := os.Stat(path)
		if err != nil {
			return true
		}

		if info.ModTime().After(loadedAt) {
			return true
		}
	}

	return false
}

// Reloadacc8Cache clears cached material and reloads verified documents.
//
// It resets both the legacy parse cache and the verified cache so the
// next decision reads rotated files. It returns the reload error, if
// any, while keeping fail-closed behavior: a failed reload leaves
// verifiedOK false so decisions deny until files verify.
func (s *Suite) Reloadacc8Cache() error {
	if s == nil {
		return fmt.Errorf("security: nil suite cannot reload: %w", ErrNotAllowedBySecurity)
	}

	s.mu.Lock()
	s.loaded = false
	s.governance = nil
	s.permissions = nil
	s.materialOK = false
	s.verifiedLoaded = false
	s.verifiedGov = nil
	s.verifiedPerms = nil
	s.verifiedOK = false
	s.mu.Unlock()

	_, _, _, ok, reason := s.verifiedState()
	if !ok {
		err := fmt.Errorf("security: reload: %s: %w", reason, ErrNotAllowedBySecurity)
		return err
	}

	return nil
}

// identityTrust returns the identity CA pool plus revoked serials.
//
// It forces the verified load first so split-CA pools and CRL entries
// are populated before authentication consults them.
func (s *Suite) identityTrust() (*x509.CertPool, map[string]struct{}, bool, string) {
	_, _, _, ok, reason := s.verifiedState()
	if !ok {
		return nil, nil, false, reason
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]struct{}, len(s.revoked))
	for key := range s.revoked {
		out[key] = struct{}{}
	}

	return s.identityPool, out, true, ""
}

// loadVerified reads CA plus identity plus signed documents and verifies.
//
// The permissions documents verify against the permissions CA while
// the local and peer identity certificates verify against the
// identity CA, which falls back to the permissions CA when no
// separate identity CA is configured. A configured CRL file must
// parse and verify against an identity CA; otherwise the load fails
// closed with verifiedOK false.
func loadVerified(cfg SuiteConfig) (*access.GovernanceDoc, *access.PermissionsDoc, *x509.CertPool, *x509.CertPool, []*x509.Certificate, *x509.Certificate, map[string]struct{}, bool, string) {
	fail := func(reason string) (*access.GovernanceDoc, *access.PermissionsDoc, *x509.CertPool, *x509.CertPool, []*x509.Certificate, *x509.Certificate, map[string]struct{}, bool, string) {
		return nil, nil, nil, nil, nil, nil, nil, false, reason
	}

	caPEM, err := os.ReadFile(cfg.PermissionsCAPath)
	if err != nil {
		return fail("permissions CA unreadable: " + err.Error())
	}

	pool, err := auth.BuildCAPool(caPEM)
	if err != nil {
		return fail("permissions CA invalid: " + err.Error())
	}

	identityPEM := caPEM

	if strings.TrimSpace(cfg.IdentityCAPath) != "" {
		identityPEM, err = os.ReadFile(cfg.IdentityCAPath)
		if err != nil {
			return fail("identity CA unreadable: " + err.Error())
		}
	}

	identityPool, err := auth.BuildCAPool(identityPEM)
	if err != nil {
		return fail("identity CA invalid: " + err.Error())
	}

	identityCAs, err := parseCACertificates(identityPEM)
	if err != nil {
		return fail("identity CA invalid: " + err.Error())
	}

	gov, perms, err := loadVerifiedDocs(cfg, pool)
	if err != nil {
		return fail(err.Error())
	}

	identity, err := loadVerifiedIdentity(cfg, identityPool)
	if err != nil {
		return fail(err.Error())
	}

	revoked, err := loadRevoked(cfg.CRLPath, identityCAs)
	if err != nil {
		return fail("revocation data invalid: " + err.Error())
	}

	return gov, perms, pool, identityPool, identityCAs, identity, revoked, true, ""
}

// loadVerifiedDocs reads plus verifies the governance and permissions files.
func loadVerifiedDocs(cfg SuiteConfig, pool *x509.CertPool) (*access.GovernanceDoc, *access.PermissionsDoc, error) {
	govRaw, err := os.ReadFile(cfg.GovernancePath)
	if err != nil {
		return nil, nil, fmt.Errorf("governance file unreadable: %w", err)
	}

	gov, err := access.VerifyGovernanceDocument(govRaw, pool)
	if err != nil {
		return nil, nil, fmt.Errorf("governance signature unverified: %w", err)
	}

	permsRaw, err := os.ReadFile(cfg.PermissionsDocumentPath)
	if err != nil {
		return nil, nil, fmt.Errorf("permissions file unreadable: %w", err)
	}

	perms, err := access.VerifyPermissionsDocument(permsRaw, pool)
	if err != nil {
		return nil, nil, fmt.Errorf("permissions signature unverified: %w", err)
	}

	return gov, perms, nil
}

// loadVerifiedIdentity reads plus validates the local identity certificate.
func loadVerifiedIdentity(cfg SuiteConfig, identityPool *x509.CertPool) (*x509.Certificate, error) {
	identityPEM, err := os.ReadFile(cfg.IdentityCertificatePath)
	if err != nil {
		return nil, fmt.Errorf("identity certificate unreadable: %w", err)
	}

	identity, err := auth.ParseCertificatePEM(identityPEM)
	if err != nil {
		return nil, fmt.Errorf("identity certificate invalid: %w", err)
	}

	err = auth.ValidateIdentity(identity, identityPool, time.Now(), "")
	if err != nil {
		return nil, fmt.Errorf("local identity unverified: %w", err)
	}

	return identity, nil
}

// parseCACertificates parses every CA certificate from PEM bytes.
func parseCACertificates(caPEM []byte) ([]*x509.Certificate, error) {
	var out []*x509.Certificate

	rest := caPEM

	for {
		var block *pem.Block

		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}

		if block.Type != "CERTIFICATE" {
			continue
		}

		cert, perr := x509.ParseCertificate(block.Bytes)
		if perr != nil {
			return nil, fmt.Errorf("security: parse CA certificate: %w", perr)
		}

		out = append(out, cert)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("security: no CA certificates parsed: %w", ErrInvalidSuiteConfig)
	}

	return out, nil
}

// loadRevoked reads one CRL file and collects revoked serials.
//
// An empty path enforces nothing. PEM blocks typed X509 CRL or CRL
// are accepted; a file with no PEM blocks parses as concatenated DER.
// Every CRL must verify against one configured identity CA, and a
// serial is keyed by issuer plus serial so distinct CAs never
// collide. Unverifiable CRLs fail the load closed.
func loadRevoked(crlPath string, cas []*x509.Certificate) (map[string]struct{}, error) {
	out := make(map[string]struct{})

	if strings.TrimSpace(crlPath) == "" {
		return out, nil
	}

	raw, err := os.ReadFile(crlPath) //nolint:gosec // G304: operator-owned suite config path, same trust as sibling material reads.
	if err != nil {
		return nil, fmt.Errorf("security: read CRL file: %w", err)
	}

	lists, err := parseCRLBytes(raw)
	if err != nil {
		return nil, err
	}

	for i := range lists {
		err = collectRevoked(out, lists[i], cas, i)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// collectRevoked verifies one CRL against the identity CAs and merges
// its entries into the revoked set.
func collectRevoked(out map[string]struct{}, list *x509.RevocationList, cas []*x509.Certificate, index int) error {
	authority := false

	for _, ca := range cas {
		if list.CheckSignatureFrom(ca) == nil {
			authority = true
			break
		}
	}

	if !authority {
		return fmt.Errorf("security: CRL %d carries no verifiable signature: %w", index, ErrInvalidSuiteConfig)
	}

	for _, revoked := range list.RevokedCertificateEntries {
		if revoked.SerialNumber == nil {
			continue
		}

		out[revokedKey(list.RawIssuer, revoked.SerialNumber.String())] = struct{}{}
	}

	return nil
}

// revokedKey names one revocation entry by issuer plus serial.
func revokedKey(issuer []byte, serial string) string {
	return string(issuer) + "\x00" + serial
}

// certRevoked reports whether one certificate is listed as revoked.
func certRevoked(revoked map[string]struct{}, cert *x509.Certificate) bool {
	if len(revoked) == 0 || cert == nil || cert.SerialNumber == nil {
		return false
	}

	_, ok := revoked[revokedKey(cert.RawIssuer, cert.SerialNumber.String())]

	return ok
}

// parseCRLBytes parses PEM CRL blocks or concatenated DER CRLs.
func parseCRLBytes(raw []byte) ([]*x509.RevocationList, error) {
	var ders [][]byte

	rest := raw

	for {
		var block *pem.Block

		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}

		if block.Type != "X509 CRL" && block.Type != "CRL" {
			continue
		}

		ders = append(ders, block.Bytes)
	}

	if len(ders) == 0 {
		elements, err := splitCRLDER(raw)
		if err != nil {
			return nil, err
		}

		ders = elements
	}

	var out []*x509.RevocationList

	for i := range ders {
		list, err := x509.ParseRevocationList(ders[i])
		if err != nil {
			return nil, fmt.Errorf("security: parse CRL %d: %w", i, err)
		}

		out = append(out, list)
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("security: CRL file holds no revocation lists: %w", ErrInvalidSuiteConfig)
	}

	return out, nil
}

// splitCRLDER splits concatenated DER values at top-level bounds.
func splitCRLDER(raw []byte) ([][]byte, error) {
	var out [][]byte

	rest := raw

	for len(rest) > 0 {
		if len(rest) < 2 {
			return nil, fmt.Errorf("security: truncated CRL element: %w", ErrInvalidSuiteConfig)
		}

		length, size, err := crlDERLength(rest[1:])
		if err != nil {
			return nil, err
		}

		total := 1 + size + length
		if total > len(rest) {
			return nil, fmt.Errorf("security: CRL element overruns: %w", ErrInvalidSuiteConfig)
		}

		out = append(out, rest[:total])
		rest = rest[total:]
	}

	return out, nil
}

// crlDERLength parses one DER length prefix after the tag byte.
func crlDERLength(raw []byte) (length, size int, err error) {
	if len(raw) == 0 {
		return 0, 0, fmt.Errorf("security: truncated CRL length: %w", ErrInvalidSuiteConfig)
	}

	first := raw[0]
	if first&0x80 == 0 {
		return int(first), 1, nil
	}

	count := int(first & 0x7f)
	if count == 0 || count > 4 || len(raw) < 1+count {
		return 0, 0, fmt.Errorf("security: bad CRL length form: %w", ErrInvalidSuiteConfig)
	}

	total := 0

	for i := range count {
		total = total*256 + int(raw[1+i])
	}

	return total, 1 + count, nil
}

// algorithmAllowed reports governance algorithm coverage.
//
// An empty rule allow list admits nothing: the design vocabulary
// (docs/security-design.md section 5) names required algorithms per
// rule, and INV-8 fails closed on unknown or absent algorithms, so a
// rule that names no algorithm cannot authorize a peer announcing
// one, and a peer announcing none cannot satisfy a rule that names
// algorithms. Unauthenticated plain peers are exempt at the call
// sites, not here: open topics under INV-4 negotiate no crypto, so
// no algorithm compatibility applies to them.
func algorithmAllowed(rule *access.DomainRule, algo string) bool {
	if rule == nil || len(rule.AllowedAlgorithms) == 0 {
		return false
	}

	if algo == "" {
		return false
	}

	return slices.Contains(rule.AllowedAlgorithms, algo)
}

// algorithmGated reports whether one peer must satisfy algorithm cover.
//
// Plain unsecured peers negotiate no crypto on INV-4 open topics, so
// the allow list does not apply to them. Every other peer announces
// security posture and must satisfy it.
func algorithmGated(peer PeerDescriptor) bool {
	return peer.Class != PeerUnsecured
}

// OMG CryptoTransformKind values per DDS Security 10.5.3 and 9.5.3.
//
// AES128_GMAC (1) and AES256_GMAC (3) provide authentication only and
// satisfy SIGN kinds but never ENCRYPT. AES128_GCM (2) and AES256_GCM
// (4) provide authenticated encryption and satisfy both SIGN and
// ENCRYPT. Local stores carry GCM only; GMAC kinds have no local
// equivalent and deny.
const (
	transformKindGMAC128 uint32 = 1
	transformKindGCM128  uint32 = 2
	transformKindGMAC256 uint32 = 3
	transformKindGCM256  uint32 = 4
)

// transformKindSatisfiesGovernanceacc8 enforces protection kinds fail-closed.
//
// Governance NONE needs no transform and allows. Any SIGN, ENCRYPT, or
// origin-authentication kind needs a negotiated transform kind. When no
// negotiated kind is pinned, the check uses the governance algorithm
// allow-list as a proxy: protected kinds allow only when the rule lists
// an AES-GCM transform and does not require origin authentication
// (which the transform never wires). This still fails closed per
// 10.5.3.1 - unknown or origin kinds deny, and an empty allow-list
// denies protected traffic - without guessing a transform decision.
// Pinned-kind enforcement lives in
// transformKindSatisfiesGovernanceWithKindacc8; Suite methods consult
// the pinned kind when set.
func transformKindSatisfiesGovernanceacc8(rule *access.DomainRule, topicRule *access.TopicRule) bool {
	if rule == nil {
		return false
	}

	if access.RequiresOriginAuth(rule, topicRule) {
		return false
	}

	needsTransform := protectionRequiresTransform(rule.DiscoveryProtection) ||
		protectionRequiresTransform(rule.LivelinessProtection) ||
		protectionRequiresTransform(rule.RTPSProtection)

	if topicRule != nil {
		needsTransform = needsTransform ||
			protectionRequiresTransform(topicRule.MetadataProtection) ||
			protectionRequiresTransform(topicRule.DataProtection)
	}

	if !needsTransform {
		return true
	}

	return ruleListsGCMTransform(rule)
}

// transformKindSatisfiesGovernanceWithKindacc8 enforces exact kind mapping.
//
// Per 9.5.3 and 10.5.3, SIGN needs a sign-capable kind (GMAC or GCM)
// and ENCRYPT needs an encrypt-capable kind (GCM only). Origin kinds
// always deny. NONE needs no kind. Zero kind falls back to the
// allow-list proxy so callers without negotiation keep fail-closed
// behavior.
func transformKindSatisfiesGovernanceWithKindacc8(rule *access.DomainRule, topicRule *access.TopicRule, kind uint32) bool {
	if rule == nil {
		return false
	}
	if access.RequiresOriginAuth(rule, topicRule) {
		return false
	}
	needsSign, needsEncrypt := governanceNeeds(rule, topicRule)
	if !needsSign && !needsEncrypt {
		return true
	}
	if kind == 0 {
		return ruleListsGCMTransform(rule)
	}
	if !transformKindCoversNeeds(kind, needsSign, needsEncrypt) {
		return false
	}
	return ruleListsGCMTransform(rule)
}

// transformKindCoversNeeds checks sign plus encrypt capability.
func transformKindCoversNeeds(kind uint32, needsSign, needsEncrypt bool) bool {
	if needsEncrypt && !isEncryptCapableKind(kind) {
		return false
	}
	return !needsSign || isSignCapableKind(kind)
}

// isSignCapableKind reports GMAC or GCM kinds.
func isSignCapableKind(kind uint32) bool {
	return kind == transformKindGMAC128 || kind == transformKindGCM128 ||
		kind == transformKindGMAC256 || kind == transformKindGCM256
}

// isEncryptCapableKind reports GCM-only kinds.
func isEncryptCapableKind(kind uint32) bool {
	return kind == transformKindGCM128 || kind == transformKindGCM256
}

// governanceNeeds reports whether any channel needs SIGN or ENCRYPT.
func governanceNeeds(rule *access.DomainRule, topicRule *access.TopicRule) (needSign, needEncrypt bool) {
	kinds := []string{rule.DiscoveryProtection, rule.LivelinessProtection, rule.RTPSProtection}
	if topicRule != nil {
		kinds = append(kinds, topicRule.MetadataProtection, topicRule.DataProtection)
	}

	for _, kind := range kinds {
		switch kind {
		case access.ProtectionSign:
			needSign = true
		case access.ProtectionEncrypt:
			needSign = true
			needEncrypt = true
		case access.ProtectionNone, "":
		default:
			// Unknown kinds are handled by RequiresOriginAuth or proxy
			// deny paths; treat any other non-NONE kind as needing both
			// to stay fail-closed.
			needSign = true
			needEncrypt = true
		}
	}

	return needSign, needEncrypt
}

// ruleListsGCMTransform reports whether the rule allows an AES-GCM transform.
func ruleListsGCMTransform(rule *access.DomainRule) bool {
	for _, name := range rule.AllowedAlgorithms {
		if name == "AES128+GCM" || name == "AES256+GCM" {
			return true
		}
	}

	return false
}

// protectionRequiresTransform reports whether one governance kind needs crypto.
func protectionRequiresTransform(kind string) bool {
	if kind == "" {
		return false
	}

	return kind != access.ProtectionNone
}

// transformSatisfies checks governance against the pinned negotiated kind.
//
// When a negotiated kind is pinned via SetNegotiatedTransformKind it
// enforces exact SIGN versus ENCRYPT mapping; otherwise the allow-list
// proxy applies.
func (s *Suite) transformSatisfies(rule *access.DomainRule, topicRule *access.TopicRule) bool {
	var kind uint32
	if s != nil {
		s.mu.Lock()
		kind = s.negotiatedKind
		s.mu.Unlock()
	}

	if kind != 0 {
		return transformKindSatisfiesGovernanceWithKindacc8(rule, topicRule, kind)
	}

	return transformKindSatisfiesGovernanceacc8(rule, topicRule)
}

// tryAllowParticipant checks verified domain coverage for one peer.
func (s *Suite) tryAllowParticipant(peer PeerDescriptor, props ParticipantProperties) bool {
	gov, perms, _, ok, _ := s.verifiedState()
	if !ok || gov == nil || perms == nil {
		return false
	}

	rule := access.FindDomainRule(gov, peer.DomainID, peer.Tag)
	if rule == nil {
		return false
	}

	if access.RequiresOriginAuth(rule, nil) {
		return false
	}

	if !s.transformSatisfies(rule, nil) {
		return false
	}

	if algorithmGated(peer) && !algorithmAllowed(rule, props.KeyExchangeAlgorithm) {
		return false
	}

	if peer.Class == PeerUnsecured {
		return rule.AllowUnauthenticated
	}

	if !s.isAuthenticated(peer.Identity) {
		return rule.AllowUnauthenticated
	}

	return s.allowAuthenticatedParticipant(rule, peer, perms)
}

// allowAuthenticatedParticipant checks grants for one authenticated peer.
//
// Join control disabled admits without grant checks: per governance,
// joining is not access-controlled in that domain. Otherwise a blanket
// domain deny in any direction refuses even when an allow rule covers
// the domain, and domain coverage by an allow rule admits.
// Topic-scoped denies never match the topicless join check. Scoped
// coverage requires partition and tag overlap, so tag-gated grants
// deny nil scope.
func (s *Suite) allowAuthenticatedParticipant(rule *access.DomainRule, peer PeerDescriptor, perms *access.PermissionsDoc) bool {
	if !rule.EnableJoinAccessControl {
		return true
	}

	subject := s.authenticatedSubject(peer.Identity)
	if subject == "" {
		return false
	}

	grant := access.GrantForSubject(perms, subject, time.Now())
	if grant == nil {
		return false
	}

	if access.DeniesPublishacc8(grant, peer.DomainID, "", peer.Partitions, peer.DataTags) ||
		access.DeniesSubscribeacc8(grant, peer.DomainID, "", peer.Partitions, peer.DataTags) ||
		access.DeniesRelayacc8(grant, peer.DomainID, "", peer.Partitions, peer.DataTags) {
		return false
	}

	return access.CoversDomainacc8Scoped(grant, peer.DomainID, peer.Partitions, peer.DataTags)
}

// tryAllowEndpoint checks verified domain plus topic rules for one endpoint.
func (s *Suite) tryAllowEndpoint(peer PeerDescriptor, topic string, isReader bool, props ParticipantProperties) bool {
	gov, perms, _, ok, _ := s.verifiedState()
	if !ok || gov == nil || perms == nil {
		return false
	}

	rule := access.FindDomainRule(gov, peer.DomainID, peer.Tag)
	if rule == nil {
		return false
	}

	if algorithmGated(peer) && !algorithmAllowed(rule, props.KeyExchangeAlgorithm) {
		return false
	}

	topicRule := access.FindTopicRule(rule, topic)
	if topicRule == nil {
		return false
	}

	if access.RequiresOriginAuth(rule, topicRule) {
		return false
	}

	if !s.transformSatisfies(rule, topicRule) {
		return false
	}

	if !s.isAuthenticated(peer.Identity) {
		return unauthenticatedEndpointOpen(rule, topicRule, isReader)
	}

	return authenticatedEndpointOpen(s, perms, peer, topic, isReader)
}

// unauthenticatedEndpointOpen checks open-topic admission without auth.
func unauthenticatedEndpointOpen(rule *access.DomainRule, topicRule *access.TopicRule, isReader bool) bool {
	if !rule.AllowUnauthenticated {
		return false
	}

	if isReader && topicRule.EnableReadAccess {
		return false
	}

	if !isReader && topicRule.EnableWriteAccess {
		return false
	}

	if topicRule.MetadataProtection != access.ProtectionNone || topicRule.DataProtection != access.ProtectionNone {
		return false
	}

	return true
}

// authenticatedEndpointOpen checks grant plus tag coverage with auth.
//
// It uses one directional lookup: the tags checked are those of the
// same allow rule that authorized the endpoint, so a mismatched first
// rule never shadows a matching second rule.
func authenticatedEndpointOpen(s *Suite, perms *access.PermissionsDoc, peer PeerDescriptor, topic string, isReader bool) bool {
	subject := s.authenticatedSubject(peer.Identity)
	if subject == "" {
		return false
	}

	grant := access.GrantForSubject(perms, subject, time.Now())
	if grant == nil {
		return false
	}

	var ok bool

	var ruleTags []string

	if isReader {
		ok, ruleTags = access.AllowsSubscribeacc8(grant, peer.DomainID, topic, peer.Partitions, peer.DataTags)
		if !ok {
			return false
		}
	} else {
		ok, ruleTags = access.AllowsPublishacc8(grant, peer.DomainID, topic, peer.Partitions, peer.DataTags)
		if !ok {
			return false
		}
	}

	if s.tagger != nil {
		if !s.tagger.Allows(peer.DataTags, ruleTags) {
			return false
		}
	}

	return true
}

// verifiedLocalRuleGrant resolves the governance rule and grant for local use.
func (s *Suite) verifiedLocalRuleGrant(domainID uint32) (*access.DomainRule, *access.Grant, bool) {
	gov, perms, localCert, ok, _ := s.verifiedState()
	if !ok || gov == nil || perms == nil || localCert == nil {
		return nil, nil, false
	}

	rule := access.FindDomainRule(gov, domainID, "")
	if rule == nil {
		return nil, nil, false
	}

	if access.RequiresOriginAuth(rule, nil) {
		return nil, nil, false
	}

	if !s.transformSatisfies(rule, nil) {
		return nil, nil, false
	}

	subject := auth.CertificateSubjectFullauth7(localCert)
	if subject == "" {
		return nil, nil, false
	}

	grant := access.GrantForSubject(perms, subject, time.Now())
	if grant == nil {
		return nil, nil, false
	}

	return rule, grant, true
}

// tryAllowLocalCreateacc8Scoped checks verified coverage with scope.
func (s *Suite) tryAllowLocalCreateacc8Scoped(identity, topic string, domainID uint32, partitions, tags []string) bool {
	if identity == "" {
		return false
	}

	rule, grant, ok := s.verifiedLocalRuleGrant(domainID)
	if !ok {
		return false
	}

	if topic == "" {
		if access.DeniesPublishacc8(grant, domainID, "", partitions, tags) ||
			access.DeniesSubscribeacc8(grant, domainID, "", partitions, tags) ||
			access.DeniesRelayacc8(grant, domainID, "", partitions, tags) {
			return false
		}

		return access.CoversDomainacc8Scoped(grant, domainID, partitions, tags)
	}

	return s.allowLocalTopicRuleacc8Scoped(rule, grant, topic, domainID, false, partitions, tags)
}

// allowLocalTopicRuleacc8Scoped checks one topic rule with scope and direction.
func (s *Suite) allowLocalTopicRuleacc8Scoped(rule *access.DomainRule, grant *access.Grant, topic string, domainID uint32, isReader bool, partitions, tags []string) bool {
	topicRule := access.FindTopicRule(rule, topic)
	if topicRule == nil {
		return false
	}

	if access.RequiresOriginAuth(rule, topicRule) {
		return false
	}

	if !s.transformSatisfies(rule, topicRule) {
		return false
	}

	var ok bool

	if isReader {
		ok, _ = access.AllowsSubscribeacc8(grant, domainID, topic, partitions, tags)
		return ok
	}

	ok, _ = access.AllowsPublishacc8(grant, domainID, topic, partitions, tags)
	return ok
}

// tryAllowLocalEndpoint checks verified topic rules for local declares.
func (s *Suite) tryAllowLocalEndpoint(identity, topic string, isReader bool, domainID uint32) bool {
	return s.tryAllowLocalEndpointacc8Scoped(identity, topic, isReader, domainID, nil, nil)
}

// tryAllowLocalEndpointacc8Scoped checks verified rules with scope and direction.
func (s *Suite) tryAllowLocalEndpointacc8Scoped(identity, topic string, isReader bool, domainID uint32, partitions, tags []string) bool {
	if identity == "" {
		return false
	}

	rule, grant, ok := s.verifiedLocalRuleGrant(domainID)
	if !ok {
		return false
	}

	return s.allowLocalTopicRuleacc8Scoped(rule, grant, topic, domainID, isReader, partitions, tags)
}

// verifiedDenyParticipant explains a verified participant denial.
//
// It returns false when documents are unverified so legacy refusal
// classes stay untouched. When verified, it names the missing domain
// rule, algorithm mismatch, authentication gap, or grant gap with the
// matching refusal class per INV-1, INV-4, and INV-8.
func (s *Suite) verifiedDenyParticipant(peer PeerDescriptor, props ParticipantProperties) (RefusalClass, string, bool) {
	gov, perms, _, ok, _ := s.verifiedState()
	if !ok || gov == nil || perms == nil {
		return "", "", false
	}

	rule := access.FindDomainRule(gov, peer.DomainID, peer.Tag)
	if rule == nil {
		return RefusalNoHandshake, "participant domain has no governance rule", true
	}

	if access.RequiresOriginAuth(rule, nil) {
		return RefusalIncompleteMaterial, "participant domain requires origin authentication, unsupported", true
	}

	if algorithmGated(peer) && !algorithmAllowed(rule, props.KeyExchangeAlgorithm) {
		return RefusalUnknownAlgorithm, "participant algorithm not in governance allow list", true
	}

	if peer.Class == PeerUnsecured {
		return RefusalUnsecurePeer, "unsecure participant refused by closed domain rule", true
	}

	if !s.isAuthenticated(peer.Identity) {
		return RefusalNoHandshake, "participant authentication missing", true
	}

	subject := s.authenticatedSubject(peer.Identity)
	if subject == "" {
		return RefusalNoHandshake, "participant subject missing", true
	}

	grant := access.GrantForSubject(perms, subject, time.Now())
	if grant == nil {
		return RefusalNoHandshake, "participant permissions grant missing or expired", true
	}

	return RefusalNoHandshake, "participant domain not covered by permissions grant", true
}

// verifiedDenyEndpoint explains a verified endpoint denial.
func (s *Suite) verifiedDenyEndpoint(peer PeerDescriptor, topic string, isReader bool, props ParticipantProperties) (RefusalClass, string, bool) {
	gov, perms, _, ok, _ := s.verifiedState()
	if !ok || gov == nil || perms == nil {
		return "", "", false
	}

	rule := access.FindDomainRule(gov, peer.DomainID, peer.Tag)
	if rule == nil {
		return RefusalNoHandshake, "endpoint domain has no governance rule", true
	}

	if algorithmGated(peer) && !algorithmAllowed(rule, props.KeyExchangeAlgorithm) {
		return RefusalUnknownAlgorithm, "endpoint algorithm not in governance allow list", true
	}

	if class, reason, decided := denyEndpointOrigin(rule, topic); decided {
		return class, reason, true
	}

	direction := "writer"
	if isReader {
		direction = "reader"
	}

	if !s.isAuthenticated(peer.Identity) {
		return RefusalNoHandshake, "endpoint " + direction + " authentication missing or topic not open to unauthenticated peers", true
	}

	subject := s.authenticatedSubject(peer.Identity)
	if subject == "" {
		return RefusalNoHandshake, "endpoint " + direction + " subject missing", true
	}

	grant := access.GrantForSubject(perms, subject, time.Now())
	if grant == nil {
		return RefusalNoHandshake, "endpoint " + direction + " permissions grant missing or expired", true
	}

	return RefusalNoHandshake, "endpoint " + direction + " topic not covered by permissions grant", true
}

// denyEndpointOrigin refuses origin-auth topics before auth checks.
//
// It returns decided false when the topic rule is present and needs no
// origin authentication, letting the caller continue to authentication
// and grant checks.
func denyEndpointOrigin(rule *access.DomainRule, topic string) (RefusalClass, string, bool) {
	topicRule := access.FindTopicRule(rule, topic)
	if topicRule == nil {
		return RefusalNoHandshake, "endpoint topic has no governance rule", true
	}

	if access.RequiresOriginAuth(rule, topicRule) {
		return RefusalIncompleteMaterial, "endpoint topic requires origin authentication, unsupported", true
	}

	return "", "", false
}

// material loads and caches material state under lock.
func (s *Suite) material() materialReport {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.loaded {
		return materialReport{complete: s.materialOK, refusal: s.materialRefusal, reason: s.materialReason}
	}

	s.loaded = true

	governance, permissions, refusal, reason, ok := loadMaterial(s.config)
	s.governance = governance
	s.permissions = permissions
	s.materialRefusal = refusal
	s.materialReason = reason
	s.materialOK = ok

	return materialReport{complete: ok, refusal: refusal, reason: reason}
}

// loadMaterial reads and structurally validates both documents.
func loadMaterial(cfg SuiteConfig) (*access.GovernanceDoc, *access.PermissionsDoc, RefusalClass, string, bool) {
	raw, err := os.ReadFile(cfg.GovernancePath)
	if err != nil {
		return nil, nil, RefusalIncompleteMaterial, "governance file unreadable: " + err.Error(), false
	}

	governance, err := access.ParseGovernance(raw)
	if err != nil {
		return nil, nil, classifyLoadError(err), "governance invalid: " + err.Error(), false
	}

	raw, err = os.ReadFile(cfg.PermissionsDocumentPath)
	if err != nil {
		return nil, nil, RefusalIncompleteMaterial, "permissions file unreadable: " + err.Error(), false
	}

	permissions, err := access.ParsePermissions(raw)
	if err != nil {
		return nil, nil, classifyLoadError(err), "permissions invalid: " + err.Error(), false
	}

	return governance, permissions, "", "", true
}

// classifyLoadError maps a parse failure to a refusal class.
func classifyLoadError(err error) RefusalClass {
	if errors.Is(err, access.ErrUnknownAlgorithm) {
		return RefusalUnknownAlgorithm
	}

	return RefusalMalformed
}

// count bumps the counter for one refusal class.
func (s *Suite) count(refusal RefusalClass) {
	switch refusal {
	case RefusalUnsecurePeer:
		s.unsecure.Add(1)
	case RefusalIncompleteMaterial:
		s.incomplete.Add(1)
	case RefusalNoHandshake:
		s.noHandshake.Add(1)
	case RefusalMalformed:
		s.malformed.Add(1)
	case RefusalUnknownAlgorithm:
		s.unknownAlgorithm.Add(1)
	default:
	}
}

// record stores the last decision for one identity.
func (s *Suite) record(identity string, class PeerClass, decision, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.peers == nil {
		s.peers = make(map[string]PeerState)
	}

	s.peers[identity] = PeerState{
		Identity:  identity,
		Class:     class,
		Decision:  decision,
		Reason:    reason,
		UpdatedAt: time.Now(),
	}
}

// Peer returns the last decision for one identity.
func (s *Suite) Peer(identity string) (PeerState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.peers[identity]

	return state, ok
}

// Peers returns a sorted snapshot of recorded peer decisions.
func (s *Suite) Peers() []PeerState {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]PeerState, 0, len(s.peers))
	for _, state := range s.peers {
		out = append(out, state)
	}

	slices.SortFunc(out, func(a, b PeerState) int {
		return strings.Compare(a.Identity, b.Identity)
	})

	return out
}

// Refusals returns the per-class refusal counters.
func (s *Suite) Refusals() RefusalSnapshot {
	return RefusalSnapshot{
		UnsecurePeer:       s.unsecure.Load(),
		IncompleteMaterial: s.incomplete.Load(),
		NoHandshake:        s.noHandshake.Load(),
		Malformed:          s.malformed.Load(),
		UnknownAlgorithm:   s.unknownAlgorithm.Load(),
	}
}
