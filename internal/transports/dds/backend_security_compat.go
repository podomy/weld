// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"crypto/x509"
	"fmt"

	ddssecurity "github.com/podomy/weld/internal/transports/dds/security"
	"github.com/podomy/weld/internal/transports/dds/security/access"
	"github.com/podomy/weld/internal/transports/dds/security/auth"
	"github.com/podomy/weld/internal/transports/dds/security/crypto"
	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// secureIdentityMaterial holds local identity for secure announcements.
//
// Restored for live-gate compatibility; full SPDP param wiring follows in
// the security audit. The manager uses the material to sign handshake
// replies and finals; the SPDP provider advertises identity plus
// permissions tokens whenever the suite is enabled.
type secureIdentityMaterial struct {
	// cert is the local identity certificate.
	cert *x509.Certificate
	// certDER holds the DER identity certificate for c.id.
	certDER []byte
	// caSubject names the identity CA for the identity token.
	caSubject string
	// permDoc holds the permissions document bytes for c.perm.
	permDoc []byte
	// pdata holds the serialized participant data for c.pdata.
	pdata []byte
}

// SetSecuritySuite installs the Phase 1 decision suite plus handshake state.
//
// A nil suite keeps tolerance mode. An enabled suite denies handshake
// completion until signatures verify, so flag-on full-auth networks
// cannot complete authentication. It forwards to the SPDP plus SEDP
// tables when they exist and stores the suite for demux. When both suite
// and manager are present the suite consults Manager.Authenticated for
// decisions per 9.3.2, and the manager validates remote c.id plus c.perm
// plus c.pdata chains against the suite identity pool per 9.4.2.8.
// The receiver provisional gate closes here as well as on Join plus
// subscribeReader plus publish, so enabled suites fail closed on unknown
// writers from the moment the suite installs.
func (b *Backend) SetSecuritySuite(suite *ddssecurity.Suite, manager *auth.Manager, store *crypto.KeyStore) {
	b.mu.Lock()

	b.secSuite = suite
	b.handshake = manager
	b.cryptoStore = store

	if suite != nil && manager != nil {
		suite.SetHandshakeManager(manager)

		if pool := suite.IdentityTrustPool(); pool != nil {
			manager.SetTrustPool(pool)
		}
	}

	if store != nil {
		transform, err := crypto.NewTransform(store)
		if err == nil {
			b.cryptoTransform = transform
		}
	}

	if b.remotes != nil {
		b.remotes.SetSecuritySuite(suite)
	}

	if b.sedp != nil {
		b.sedp.SetSecuritySuite(suite)
	}

	receiver := b.receiver
	enabled := suite != nil && suite.Enabled()
	b.mu.Unlock()

	if receiver != nil {
		receiver.SetSecureProvisionalGate(enabled)
	}
}

// InstallSecureIdentity installs local identity for handshakes and announces.
//
// certDER carries the DER identity certificate, permDoc the permissions
// document bytes, and privPEM the EC identity private key. caSubject
// names the identity CA for the SPDP identity token. The manager uses
// the material to sign handshake replies and finals; the SPDP provider
// advertises identity plus permissions tokens whenever the suite is
// enabled. Errors fail closed without touching existing material.
func (b *Backend) InstallSecureIdentity(certDER, permDoc, privPEM []byte, caSubject string) error {
	if len(certDER) == 0 {
		return fmt.Errorf("dds: secure identity without certificate: %w", ErrSecuredTransformUnavailable)
	}

	if len(privPEM) == 0 {
		return fmt.Errorf("dds: secure identity without private key: %w", ErrSecuredTransformUnavailable)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return fmt.Errorf("dds: secure identity certificate: %w", err)
	}

	prefix, err := auth.AdjustedGUIDPrefix(cert)
	if err != nil {
		return fmt.Errorf("dds: secure identity adjusted guid: %w", err)
	}

	b.mu.RLock()
	manager := b.handshake
	b.mu.RUnlock()

	if manager == nil {
		return fmt.Errorf("dds: secure identity without handshake: %w", ErrDataPathNotWired)
	}

	pdata := tokens.EncodeParticipantData(prefix)

	err = manager.SetIdentityMaterial(certDER, permDoc, pdata, privPEM)
	if err != nil {
		return fmt.Errorf("dds: secure identity material: %w", err)
	}

	identity := &secureIdentityMaterial{
		cert:      cert,
		certDER:   append([]byte(nil), certDER...),
		caSubject: caSubject,
		permDoc:   append([]byte(nil), permDoc...),
		pdata:     append([]byte(nil), pdata...),
	}

	b.mu.Lock()
	b.secureIdentity = identity
	b.mu.Unlock()

	return nil
}

// checkPlainEgressAllowed fails closed when governance protects the topic.
//
// Per 10.5.3.1, SIGN/ENCRYPT protection kinds must drive transform, never
// plain DATA. Full SRTPS/SUBMESSAGE egress is an interface request (no
// session keys are negotiated on the plain publish path), so protected
// topics deny plain egress loudly with ErrSecuredTransformUnavailable
// instead of sending plain. Open topics with NONE/NONE allow plain.
// Unverified governance also denies plain: unknown protection must not
// downgrade to plaintext. Discovery, liveliness, and RTPS protections
// also deny plain DATA: an RTPS message carrying plain DATA leaks even
// when topic data plus metadata are NONE, so the DATA path checks the
// participant domain rule as well as the topic rule by design.
func (b *Backend) checkPlainEgressAllowed(topic string, domain uint32) error {
	if b == nil {
		return nil
	}

	suite := b.plainEgressSuite()
	if suite == nil || !suite.Enabled() {
		return nil
	}

	cfg, err := suite.GetTopicSecurityConfigInDomain(topic, domain)
	if err != nil {
		return fmt.Errorf("dds: plain egress without verified governance for %q: %w", topic, err)
	}
	if err := checkTopicProtection(topic, cfg); err != nil {
		return err
	}

	pcfg, err := suite.GetParticipantSecurityConfigInDomain("", domain)
	if err != nil {
		return fmt.Errorf("dds: plain egress without verified participant governance for %q: %w", topic, err)
	}
	return checkParticipantProtection(topic, pcfg)
}

// plainEgressSuite returns the security suite for egress checks.
func (b *Backend) plainEgressSuite() *ddssecurity.Suite {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.secSuite
}

// checkTopicProtection denies plain when topic data or metadata need transform.
func checkTopicProtection(topic string, cfg access.TopicSecurityConfig) error {
	if protectionRequiresTransform(cfg.DataProtection) {
		return fmt.Errorf("dds: plain egress refused for %q, data %s needs transform: %w", topic, cfg.DataProtection, ErrSecuredTransformUnavailable)
	}
	if protectionRequiresTransform(cfg.MetadataProtection) {
		return fmt.Errorf("dds: plain egress refused for %q, metadata %s needs transform: %w", topic, cfg.MetadataProtection, ErrSecuredTransformUnavailable)
	}
	return nil
}

// checkParticipantProtection denies plain when discovery, liveliness, or RTPS need transform.
func checkParticipantProtection(topic string, pcfg access.ParticipantSecurityConfig) error {
	if protectionRequiresTransform(pcfg.DiscoveryProtection) {
		return fmt.Errorf("dds: plain egress refused for %q, discovery %s needs transform: %w", topic, pcfg.DiscoveryProtection, ErrSecuredTransformUnavailable)
	}
	if protectionRequiresTransform(pcfg.LivelinessProtection) {
		return fmt.Errorf("dds: plain egress refused for %q, liveliness %s needs transform: %w", topic, pcfg.LivelinessProtection, ErrSecuredTransformUnavailable)
	}
	if protectionRequiresTransform(pcfg.RTPSProtection) {
		return fmt.Errorf("dds: plain egress refused for %q, rtps %s needs transform: %w", topic, pcfg.RTPSProtection, ErrSecuredTransformUnavailable)
	}
	return nil
}

// protectionRequiresTransform reports whether a protection kind denies plain.
func protectionRequiresTransform(kind string) bool {
	return kind != "" && kind != access.ProtectionNone
}

// checkDiscoveryEgressAllowed fails closed when discovery, liveliness, or
// RTPS protection requires a transform per 10.5.3.1 and 9.4.2.4.
//
// Heartbeat, SEDP announce, and SPDP announce paths call this before
// sending plain: protected discovery denies plain with
// ErrSecuredTransformUnavailable instead of leaking. Open participant
// governance allows plain so tolerance flow stays byte-identical.
func (b *Backend) checkDiscoveryEgressAllowed(domain uint32) error {
	if b == nil {
		return nil
	}
	suite := b.plainEgressSuite()
	if suite == nil || !suite.Enabled() {
		return nil
	}
	pcfg, err := suite.GetParticipantSecurityConfigInDomain("", domain)
	if err != nil {
		return fmt.Errorf("dds: discovery egress without verified governance: %w", err)
	}
	if protectionRequiresTransform(pcfg.DiscoveryProtection) {
		return fmt.Errorf("dds: plain discovery egress refused, discovery %s needs transform: %w", pcfg.DiscoveryProtection, ErrSecuredTransformUnavailable)
	}
	if protectionRequiresTransform(pcfg.LivelinessProtection) {
		return fmt.Errorf("dds: plain discovery egress refused, liveliness %s needs transform: %w", pcfg.LivelinessProtection, ErrSecuredTransformUnavailable)
	}
	if protectionRequiresTransform(pcfg.RTPSProtection) {
		return fmt.Errorf("dds: plain discovery egress refused, rtps %s needs transform: %w", pcfg.RTPSProtection, ErrSecuredTransformUnavailable)
	}
	return nil
}

// checkHeartbeatEgressAllowed gates plain HEARTBEAT per 10.5.3.1.
//
// Liveliness plus RTPS protection kinds drive the decision: protected
// liveliness denies plain heartbeat fail-closed.
func (b *Backend) checkHeartbeatEgressAllowed(domain uint32) error {
	return b.checkDiscoveryEgressAllowed(domain)
}

// rekeyRequiredForDomain reports whether governance demands a staged rekey.
//
// It wraps ddssecurity.RekeyOnEnableKeyRevision per 9.5.1.9: when the
// domain rule carries EnableKeyRevision the caller must stage fresh
// material via crypto factory Revise, distribute via the volatile
// rollover signal, then Activate on ack. The factory Revise plus Activate
// pair lives in security/crypto (foreign to this file); this helper owns
// the publish-path trigger decision. Nil governance needs no rekey.
func rekeyRequiredForDomain(gov *access.GovernanceDoc, domain uint32, tag string) bool {
	return ddssecurity.RekeyOnEnableKeyRevision(gov, domain, tag)
}

// SetNegotiatedTransformKind pins the negotiated crypto kind into the suite.
//
// Per 9.5.3 the handshake completion selects one CryptoTransformKind and
// the data path seals with it. The stateless handshake manager lives in
// security/auth (foreign); backend handshake completion paths you own
// should call this when they observe a completed handshake. Until the
// foreign manager calls back, this setter plus unit test pins the wiring.
// TODO(handshake-owner): call Backend.SetNegotiatedTransformKind from the
// auth.Manager completion callback with the negotiated kind.
func (b *Backend) SetNegotiatedTransformKind(kind uint32) {
	b.mu.RLock()
	suite := b.secSuite
	b.mu.RUnlock()
	if suite == nil {
		return
	}
	suite.SetNegotiatedTransformKind(kind)
}
