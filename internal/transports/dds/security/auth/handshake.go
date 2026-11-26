// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package auth

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// Sentinel errors for manager failures.
var (
	// ErrNoSession reports a handshake step without a session.
	ErrNoSession = errors.New("auth: no session")
	// ErrWrongState reports a handshake step in the wrong state.
	ErrWrongState = errors.New("auth: wrong state")
	// ErrSelfauth7 reports a handshake message from the local identity.
	ErrSelfauth7 = errors.New("auth: self handshake")
)

// SessionState names one per-peer handshake state.
type SessionState int

// Per-peer handshake states per section 3.3.
const (
	// StateDiscovered waits for the first handshake step.
	StateDiscovered SessionState = iota
	// StateReqSent waits for a reply after sending a request.
	StateReqSent
	// StateRepSent waits for a final after sending a reply.
	StateRepSent
	// StateAuthenticated holds a derived shared secret.
	StateAuthenticated
	// StateFailed is a terminal failure; the peer must be evicted.
	StateFailed
)

// Session holds one peer handshake transcript.
type Session struct {
	// Remote names the peer.
	Remote string
	// State is the current handshake state.
	State SessionState
	// Challenge1 is the initiator nonce we sent or received.
	Challenge1 [32]byte
	// Challenge2 is the responder nonce we sent or received.
	Challenge2 [32]byte
	// PeerPublic holds the peer ECDHE public key bytes.
	PeerPublic []byte
	// PeerCertDER holds the peer DER identity certificate from c.id.
	PeerCertDER []byte
	// ReplyPerm holds the c.perm bytes received in the peer reply.
	ReplyPerm []byte
	// ReplyPData holds the c.pdata bytes received in the peer reply.
	ReplyPData []byte
	// ReplyDSign holds the c.dsign_algo received in the peer reply.
	ReplyDSign string
	// ReplyKAgree holds the c.kagree_algo received in the peer reply.
	ReplyKAgree string
	// Secret holds the derived ECDHE secret once available.
	Secret []byte
	// LocalCert holds the c.id bytes we sent in our request.
	LocalCert []byte
	// LocalPerm holds the c.perm bytes we sent in our request.
	LocalPerm []byte
	// LocalPData holds the c.pdata bytes we sent in our request.
	LocalPData []byte
	// LocalDSign holds the c.dsign_algo we sent in our request.
	LocalDSign string
	// LocalKAgree holds the c.kagree_algo we sent in our request.
	LocalKAgree string
	// ReqCert holds the c.id bytes received in the peer request.
	ReqCert []byte
	// ReqPerm holds the c.perm bytes received in the peer request.
	ReqPerm []byte
	// ReqPData holds the c.pdata bytes received in the peer request.
	ReqPData []byte
	// ReqDSign holds the c.dsign_algo received in the peer request.
	ReqDSign string
	// ReqKAgree holds the c.kagree_algo received in the peer request.
	ReqKAgree string
	// SentFC is the future challenge we announced in our auth request.
	SentFC [32]byte
	// SentFCPresent reports SentFC is set and unconsumed.
	SentFCPresent bool
	// RecvFC is the future challenge announced by the peer auth request.
	RecvFC [32]byte
	// RecvFCPresent reports RecvFC is set.
	RecvFCPresent bool
	// Result is the last validation outcome.
	Result ValidationResult
	// LastOutboundID is the last sent message identity for retransmit.
	LastOutboundID uint64
	// LastSendAt is when the last outbound was sent.
	LastSendAt time.Time
	// Seen tracks inbound message identities for duplicate suppression.
	Seen map[uint64]struct{}
	// FailReason records why the session failed.
	FailReason string
}

// Manager drives one participant handshake side.
type Manager struct {
	// local names this participant.
	local string
	// auth holds the local ECDHE identity.
	auth *Authenticator
	// mu guards sessions plus counters.
	mu sync.Mutex
	// sessions maps remote id to handshake state.
	sessions map[string]*Session
	// nextID assigns outbound message identities.
	nextID uint64
	// certDER holds the local DER identity certificate for c.id.
	certDER []byte
	// permDoc holds the local permissions document for c.perm.
	permDoc []byte
	// pdata holds the local serialized participant data for c.pdata.
	pdata []byte
	// signer holds the local EC identity key for reply and final.
	signer *ecdsa.PrivateKey
	// haveIdentity reports SetIdentityMaterial installed material.
	haveIdentity bool
	// caPool verifies remote identity certificates per 9.4.2.8 when set.
	// Nil skips chain verification only for explicitly unauthenticated
	// loopback (see allowUnauthenticated); all other nil-pool paths fail
	// closed in verifyRemoteIdentityauth7.
	caPool *x509.CertPool
	// allowUnauthenticated permits explicitly unauthenticated loopback
	// with empty c.id, c.perm, and c.pdata and no trust pool. Default
	// false (deny): unsigned emission plus nil-pool skips fail closed
	// unless the caller opts in via SetAllowUnauthenticated for tests.
	allowUnauthenticated bool
}

// NewManager returns a handshake manager for one participant.
func NewManager(localID string, auth *Authenticator) (*Manager, error) {
	if auth == nil {
		return nil, fmt.Errorf("auth: manager without authenticator: %w", ErrHandshakeFailed)
	}

	return &Manager{
		local:    localID,
		auth:     auth,
		sessions: make(map[string]*Session),
	}, nil
}

// LocalID returns the manager participant name.
func (m *Manager) LocalID() string {
	return m.local
}

// SetIdentityMaterial installs the local identity for signed tokens.
//
// certDER carries the DER identity certificate for c.id, permDoc the
// permissions document for c.perm, pdata the serialized participant
// data for c.pdata, and privPEM the EC identity private key. RSA keys
// fail closed because CycloneDDS signs RSA with PSS. Without material
// the manager emits unsigned legacy tokens for loopback tests.
func (m *Manager) SetIdentityMaterial(certDER, permDoc, pdata, privPEM []byte) error {
	if len(certDER) == 0 {
		return fmt.Errorf("auth: identity certificate must not be empty: %w", ErrHandshakeFailed)
	}

	signer, err := ParseECPrivateKey(privPEM)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.certDER = append([]byte(nil), certDER...)
	m.permDoc = append([]byte(nil), permDoc...)
	m.pdata = append([]byte(nil), pdata...)
	m.signer = signer
	m.haveIdentity = true

	return nil
}

// SetAllowUnauthenticated opts into explicitly unauthenticated loopback.
//
// Default false (deny): StartInitiator without identity material and
// nil-pool verification with empty material fail closed. Tests needing
// legacy unsigned loopback must call SetAllowUnauthenticated(true);
// production handshakes must leave it false and install identity plus
// trust material instead.
func (m *Manager) SetAllowUnauthenticated(allow bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.allowUnauthenticated = allow
}

// SetTrustPool installs the identity CA pool for remote verification.
//
// When set, each handshake step validates the peer certificate chain
// plus validity windows per 9.4.2.8 and requires non-empty c.perm and
// c.pdata; failures reject the session fail-closed. Nil restores the
// legacy skip, which verifyRemoteIdentityauth7 grants only for
// explicitly unauthenticated loopback (empty c.id, c.perm, c.pdata
// plus SetAllowUnauthenticated(true)); all other nil-pool paths fail
// closed.
func (m *Manager) SetTrustPool(pool *x509.CertPool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.caPool = pool
}

// verifyRemoteIdentityauth7 validates one peer certificate plus material.
//
// A nil pool fails closed per 9.4.2.8 whenever the peer presents any
// identity material: chain, validity, c.perm, and c.pdata cannot verify
// without trust. The only nil-pool pass is explicitly unauthenticated
// loopback with empty c.id, c.perm, and c.pdata plus
// SetAllowUnauthenticated(true). Otherwise the peer certificate must
// parse, chain to the pool with validity windows enforced, and both
// c.perm and c.pdata must be non-empty. Pdata GUID binding is checked
// by the caller. The caller holds m.mu in handshake paths; direct test
// calls read m.allowUnauthenticated without the lock on a quiescent
// manager.
func verifyRemoteIdentityauth7(m *Manager, peerCertDER, peerPerm, peerPData []byte) error {
	if m == nil {
		return fmt.Errorf("auth: remote identity without manager: %w", ErrHandshakeFailed)
	}

	if m.caPool == nil {
		if len(peerCertDER) == 0 && len(peerPerm) == 0 && len(peerPData) == 0 {
			if m.allowUnauthenticated {
				return nil
			}

			return fmt.Errorf("auth: unauthenticated handshake requires explicit loopback opt-in: %w", ErrHandshakeFailed)
		}

		return fmt.Errorf("auth: remote identity without trust pool: %w", ErrHandshakeFailed)
	}

	if len(peerCertDER) == 0 {
		return fmt.Errorf("auth: remote c.id missing with trust pool: %w", ErrHandshakeFailed)
	}

	if len(peerPerm) == 0 {
		return fmt.Errorf("auth: remote c.perm missing with trust pool: %w", ErrHandshakeFailed)
	}

	if len(peerPData) == 0 {
		return fmt.Errorf("auth: remote c.pdata missing with trust pool: %w", ErrHandshakeFailed)
	}

	peerCert, err := ParseCertificatePEM(peerCertDER)
	if err != nil {
		return fmt.Errorf("auth: remote c.id parse: %w", err)
	}

	err = ValidateIdentity(peerCert, m.caPool, time.Now(), "")
	if err != nil {
		return fmt.Errorf("auth: remote identity unverified: %w", err)
	}

	return nil
}

// identitySnapshot copies the identity material under lock.
func (m *Manager) identitySnapshot() ([]byte, []byte, []byte, *ecdsa.PrivateKey, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]byte(nil), m.certDER...),
		append([]byte(nil), m.permDoc...),
		append([]byte(nil), m.pdata...),
		m.signer, m.haveIdentity
}

// MintAuthRequest mints one future challenge for the bootstrap token.
func (m *Manager) MintAuthRequest(remote string, now time.Time) (tokens.DataHolder, uint64, error) {
	challenge, err := NewChallenge()
	if err != nil {
		return tokens.DataHolder{}, 0, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.sessionLocked(remote)
	if session.State == StateFailed {
		return tokens.DataHolder{}, 0, fmt.Errorf("auth: auth request against failed peer %s: %w", remote, ErrWrongState)
	}

	msgID := m.nextMessageIDLocked()
	session.SentFC = challenge
	session.SentFCPresent = true
	session.LastOutboundID = msgID
	session.LastSendAt = now

	return tokens.BuildAuthRequestToken(challenge), msgID, nil
}

// NoteRemoteAuthRequest records the peer future challenge.
func (m *Manager) NoteRemoteAuthRequest(remote string, challenge [32]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.sessionLocked(remote)
	session.RecvFC = challenge
	session.RecvFCPresent = true
}

// sessionLocked returns the session for remote, creating it when missing.
func (m *Manager) sessionLocked(remote string) *Session {
	session, ok := m.sessions[remote]
	if !ok {
		session = &Session{Remote: remote, State: StateDiscovered, Seen: make(map[uint64]struct{})}
		m.sessions[remote] = session
	}

	if session.Seen == nil {
		session.Seen = make(map[uint64]struct{})
	}

	return session
}

// nextMessageIDLocked assigns the next outbound identity, never zero.
func (m *Manager) nextMessageIDLocked() uint64 {
	m.nextID++
	if m.nextID == 0 {
		m.nextID = 1
	}

	return m.nextID
}

// NoteInbound records one inbound identity, reporting false on replay.
//
// Replays drop loudly with ErrStaleHandshake per the section 6 negative
// matrix. The first sight records and reports true.
func (m *Manager) NoteInbound(remote string, messageID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.sessionLocked(remote)

	if _, seen := session.Seen[messageID]; seen {
		return fmt.Errorf("auth: replay message %d from %s: %w", messageID, remote, ErrStaleHandshake)
	}

	session.Seen[messageID] = struct{}{}

	return nil
}

// decodePDataPrefixauth7 extracts the GUID prefix from minimal c.pdata.
//
// It scans the parameter list for PID 0x0050 and returns the first 12
// bytes of its value. Undecodable inputs report false for legacy skip.
func decodePDataPrefixauth7(pdata []byte) ([12]byte, bool) {
	var out [12]byte

	off := 0
	for off+4 <= len(pdata) {
		pid := int(pdata[off])<<8 | int(pdata[off+1])
		length := int(pdata[off+2])<<8 | int(pdata[off+3])
		off += 4
		if pid == 0x0001 && length == 0 {
			break
		}
		if off+length > len(pdata) {
			return out, false
		}
		if pid == 0x0050 && length >= 12 {
			copy(out[:], pdata[off:off+12])
			return out, true
		}
		off += length
	}

	return out, false
}

// localPrefixForSelfauth7 derives the local GUID prefix for self checks.
func localPrefixForSelfauth7(localCertDER []byte) ([12]byte, bool) {
	var out [12]byte

	if len(localCertDER) == 0 {
		return out, false
	}

	localCert, err := ParseCertificatePEM(localCertDER)
	if err != nil {
		return out, false
	}

	prefix, err := AdjustedGUIDPrefix(localCert)
	if err != nil {
		return out, false
	}

	return prefix, true
}

// isSelfCertPrefixauth7 reports whether a remote cert carries the local prefix.
func isSelfCertPrefixauth7(remoteCertDER []byte, localPrefix [12]byte) bool {
	if len(remoteCertDER) == 0 {
		return false
	}

	remoteCert, err := ParseCertificatePEM(remoteCertDER)
	if err != nil {
		return false
	}

	remotePrefix, err := AdjustedGUIDPrefix(remoteCert)
	if err != nil {
		return false
	}

	return remotePrefix == localPrefix
}

// isSelfMessageauth7 reports whether remote material carries the local GUID.
//
// Raw certificate equality, certificate prefix equality, and pdata prefix
// equality each count as self per 9.3. Empty or undecodable inputs report
// false so legacy loopback fixtures with distinct IDs keep completing.
func isSelfMessageauth7(localCertDER, remoteCertDER, remotePData []byte) bool {
	if len(localCertDER) == 0 {
		return false
	}

	if len(remoteCertDER) != 0 && bytes.Equal(localCertDER, remoteCertDER) {
		return true
	}

	localPrefix, ok := localPrefixForSelfauth7(localCertDER)
	if !ok {
		return false
	}

	if isSelfCertPrefixauth7(remoteCertDER, localPrefix) {
		return true
	}

	if len(remotePData) == 0 {
		return false
	}

	prefix, ok := decodePDataPrefixauth7(remotePData)
	if !ok {
		return false
	}

	return prefix == localPrefix
}

// verifyPeerPDataauth7 checks pdata GUID prefix against the peer certificate.
//
// Per DDS Security 10.5, an authenticated handshake binds c.pdata to
// c.id: when the peer presents a certificate, c.pdata must be present,
// decodable, and carry the adjusted GUID prefix of the peer certificate.
// Empty pdata with an identity, or undecodable pdata with an identity,
// fails closed. The only skip is fully unauthenticated loopback with no
// peer certificate. Chain verification of the permissions envelope needs
// a CA pool and is tracked as an interface request, so this helper covers
// only the GUID binding that local material already supports.
func verifyPeerPDataauth7(peerCertDER, peerPData []byte) error {
	if len(peerCertDER) == 0 {
		return nil
	}

	if len(peerPData) == 0 {
		return fmt.Errorf("auth: peer pdata missing with identity: %w", ErrHandshakeFailed)
	}

	peerCert, err := ParseCertificatePEM(peerCertDER)
	if err != nil {
		return fmt.Errorf("auth: peer pdata cert parse: %w", err)
	}

	want, err := AdjustedGUIDPrefix(peerCert)
	if err != nil {
		return fmt.Errorf("auth: peer pdata prefix: %w", err)
	}

	got, ok := decodePDataPrefixauth7(peerPData)
	if !ok {
		return fmt.Errorf("auth: peer pdata undecodable with identity: %w", ErrHandshakeFailed)
	}

	if got != want {
		return fmt.Errorf("auth: pdata prefix mismatches certificate: %w", ErrHandshakeFailed)
	}

	return nil
}

// checkPeerBindingauth7 rejects a changed peer certificate for one session.
//
// The first sight with empty stored state passes and the caller pins it.
// Once a non-empty certificate is pinned, a later empty or different
// certificate fails closed per 9.3.2 and INV-5, so a cert swap cannot ride
// the session. Both empty stays allowed only as explicitly
// unauthenticated loopback: verifyRemoteIdentityauth7 gates empty-empty
// behind SetAllowUnauthenticated(true), and later sign, verify, final,
// plus SharedSecret and Authenticated gates fail closed without a peer
// certificate, so an empty binding alone never authenticates.
func checkPeerBindingauth7(stored, incoming []byte) error {
	if len(stored) == 0 {
		return nil
	}

	if len(incoming) == 0 {
		return fmt.Errorf("auth: peer certificate dropped for session: %w", ErrHandshakeFailed)
	}

	if !bytes.Equal(stored, incoming) {
		return fmt.Errorf("auth: peer certificate changed for session: %w", ErrHandshakeFailed)
	}

	return nil
}

// StartInitiator begins the initiator side against remote.
//
// Without identity material it fails closed by default per 9.3.2 and
// INV-5: an unsigned HandshakeRequest must never emit outside explicitly
// unauthenticated loopback. Callers needing legacy loopback must opt in
// via SetAllowUnauthenticated(true); production callers install
// identity via SetIdentityMaterial instead.
func (m *Manager) StartInitiator(remote string, now time.Time) (tokens.HandshakeRequestToken, uint64, error) {
	var out tokens.HandshakeRequestToken

	certDER, permDoc, pdata, _, _ := m.identitySnapshot()

	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.haveIdentity && !m.allowUnauthenticated {
		return out, 0, fmt.Errorf("auth: initiator without identity requires explicit unauthenticated loopback: %w", ErrHandshakeFailed)
	}

	session := m.sessionLocked(remote)
	if session.State == StateFailed {
		return out, 0, fmt.Errorf("auth: start against failed peer %s: %w", remote, ErrWrongState)
	}

	var challenge [32]byte

	var err error

	if session.SentFCPresent {
		challenge = session.SentFC
		session.SentFCPresent = false
	} else {
		challenge, err = NewChallenge()
		if err != nil {
			return out, 0, err
		}
	}

	public, err := m.auth.PublicKey()
	if err != nil {
		return out, 0, err
	}

	ours, err := kagreeCurveID(m.auth.KAgreeName())
	if err != nil {
		return out, 0, err
	}

	offered, err := kagreeCurveID(tokens.KAgreeECDH)
	if err != nil {
		return out, 0, err
	}

	if ours != offered {
		return out, 0, fmt.Errorf("auth: manager group %s cannot offer %q: %w", ours, tokens.KAgreeECDH, ErrHandshakeFailed)
	}

	err = CheckAlgorithm(tokens.KAgreeECDH)
	if err != nil {
		return out, 0, err
	}

	msgID := m.nextMessageIDLocked()

	session.State = StateReqSent
	session.Challenge1 = challenge
	session.Result = ValidationPendingMessage
	session.LastOutboundID = msgID
	session.LastSendAt = now
	session.LocalCert = append([]byte(nil), certDER...)
	session.LocalPerm = append([]byte(nil), permDoc...)
	session.LocalPData = append([]byte(nil), pdata...)
	session.LocalDSign = tokens.DSignAlgoECDSA
	session.LocalKAgree = tokens.KAgreeECDH

	out = tokens.HandshakeRequestToken{
		Class:           tokens.TokenReq,
		Challenge1:      challenge,
		DH1:             append([]byte(nil), public...),
		KeyAgreeAlgo:    tokens.KAgreeECDH,
		SignAlgo:        tokens.DSignAlgoECDSA,
		IdentityCert:    append([]byte(nil), certDER...),
		Permissions:     append([]byte(nil), permDoc...),
		ParticipantData: append([]byte(nil), pdata...),
	}

	return out, msgID, nil
}

// HandleRequest processes one initiator request on the replier side.
func (m *Manager) HandleRequest(remote string, request tokens.HandshakeRequestToken, inboundID uint64, now time.Time) (tokens.HandshakeReplyToken, uint64, error) {
	var out tokens.HandshakeReplyToken

	m.mu.Lock()
	defer m.mu.Unlock()

	if isSelfMessageauth7(m.certDER, request.IdentityCert, request.ParticipantData) {
		return out, 0, fmt.Errorf("auth: self request dropped: %w", errors.Join(ErrSelfauth7, ErrHandshakeFailed))
	}

	session := m.sessionLocked(remote)

	if _, seen := session.Seen[inboundID]; seen {
		return out, 0, fmt.Errorf("auth: replay request %d from %s: %w", inboundID, remote, ErrStaleHandshake)
	}

	session.Seen[inboundID] = struct{}{}

	if session.State == StateFailed {
		return out, 0, fmt.Errorf("auth: request from failed peer %s: %w", remote, ErrWrongState)
	}

	err := m.checkRequestLocked(session, request)
	if err != nil {
		return out, 0, err
	}

	challenge2, err := NewChallenge()
	if err != nil {
		return out, 0, err
	}

	public, err := m.auth.PublicKey()
	if err != nil {
		return out, 0, err
	}

	secret, err := m.auth.SharedSecret(request.DH1)
	if err != nil {
		session.State = StateFailed
		session.Result = ValidationFailed
		session.FailReason = err.Error()

		return out, 0, err
	}

	msgID := m.nextMessageIDLocked()

	session.State = StateRepSent
	session.Challenge1 = request.Challenge1
	session.Challenge2 = challenge2
	session.PeerPublic = append([]byte(nil), request.DH1...)
	session.PeerCertDER = append([]byte(nil), request.IdentityCert...)
	session.Secret = secret
	session.Result = ValidationPendingMessage
	session.LastOutboundID = msgID
	session.LastSendAt = now
	session.ReqCert = append([]byte(nil), request.IdentityCert...)
	session.ReqPerm = append([]byte(nil), request.Permissions...)
	session.ReqPData = append([]byte(nil), request.ParticipantData...)
	session.ReqDSign = request.SignAlgo
	session.ReqKAgree = request.KeyAgreeAlgo

	var certDER []byte

	if m.haveIdentity {
		certDER = m.certDER
	}

	signature, err := m.signReplyLocked(session, request, public)
	if err != nil {
		session.State = StateFailed
		session.Result = ValidationFailed
		session.FailReason = err.Error()

		return out, 0, err
	}

	out = tokens.HandshakeReplyToken{
		Class:           tokens.TokenReply,
		Challenge1Echo:  request.Challenge1,
		Challenge2:      challenge2,
		DH2:             append([]byte(nil), public...),
		Signature:       signature,
		KeyAgreeAlgo:    request.KeyAgreeAlgo,
		SignAlgo:        tokens.DSignAlgoECDSA,
		IdentityCert:    append([]byte(nil), certDER...),
		Permissions:     append([]byte(nil), m.permDoc...),
		ParticipantData: append([]byte(nil), m.pdata...),
	}

	return out, msgID, nil
}

// checkRequestPeerauth7 validates peer binding plus pdata for one request.
//
// The caller holds m.mu. It fails the session loudly on mismatch.
func checkRequestPeerauth7(m *Manager, session *Session, request tokens.HandshakeRequestToken) error {
	err := checkPeerBindingauth7(session.PeerCertDER, request.IdentityCert)
	if err != nil {
		return m.failLocked(session, err)
	}

	err = checkPeerBindingauth7(session.ReqCert, request.IdentityCert)
	if err != nil {
		return m.failLocked(session, err)
	}

	err = verifyPeerPDataauth7(request.IdentityCert, request.ParticipantData)
	if err != nil {
		return m.failLocked(session, err)
	}

	return nil
}

// checkRequestLocked validates one initiator request, failing the session.
//
// The caller holds m.mu.
func (m *Manager) checkRequestLocked(session *Session, request tokens.HandshakeRequestToken) error {
	if session.RecvFCPresent {
		err := VerifyChallengeEcho(session.RecvFC, request.Challenge1)
		if err != nil {
			return m.failLocked(session, err)
		}
	}

	err := CheckAlgorithm(request.KeyAgreeAlgo)
	if err != nil {
		return m.failLocked(session, err)
	}

	want, werr := kagreeCurveID(m.auth.KAgreeName())
	got, gerr := kagreeCurveID(request.KeyAgreeAlgo)
	if werr != nil || gerr != nil || want != got {
		err := fmt.Errorf("auth: request kagree %q not our group: %w", request.KeyAgreeAlgo, ErrHandshakeFailed)

		return m.failLocked(session, err)
	}

	if len(request.DH1) == 0 {
		err := fmt.Errorf("auth: request without dh1: %w", ErrHandshakeFailed)

		return m.failLocked(session, err)
	}

	if m.haveIdentity && len(request.IdentityCert) == 0 {
		err := fmt.Errorf("auth: secure request without c.id: %w", ErrHandshakeFailed)

		return m.failLocked(session, err)
	}

	err = verifyRemoteIdentityauth7(m, request.IdentityCert, request.Permissions, request.ParticipantData)
	if err != nil {
		return m.failLocked(session, err)
	}

	return checkRequestPeerauth7(m, session, request)
}

// failLocked marks one session failed with a loud reason.
//
// The caller holds m.mu.
func (m *Manager) failLocked(session *Session, err error) error {
	session.State = StateFailed
	session.Result = ValidationFailed
	session.FailReason = err.Error()

	return err
}

// signReplyLocked signs the reply content when identity is installed.
//
// The caller holds m.mu. Without identity it fails closed per 9.3.2:
// HandshakeReply must carry a signature, and an unsigned reply must
// never complete as authenticated.
func (m *Manager) signReplyLocked(session *Session, request tokens.HandshakeRequestToken, dh2 []byte) ([]byte, error) {
	if !m.haveIdentity {
		return nil, fmt.Errorf("auth: reply without identity material: %w", ErrHandshakeFailed)
	}

	hashC1, err := tokens.HashBinaryItems(tokens.RequestHashItems(request))
	if err != nil {
		return nil, fmt.Errorf("auth: reply hash: %w", err)
	}

	replyProps := []tokens.BinaryProperty{
		{Name: tokens.PropCID, Value: m.certDER},
		{Name: tokens.PropCPerm, Value: m.permDoc},
		{Name: tokens.PropCPData, Value: m.pdata},
		{Name: tokens.PropCDSignAlgo, Value: []byte(tokens.DSignAlgoECDSA)},
		{Name: tokens.PropCKAgreeAlgo, Value: []byte(request.KeyAgreeAlgo)},
	}

	hashC2, err := tokens.HashBinaryItems(replyProps)
	if err != nil {
		return nil, fmt.Errorf("auth: reply hash: %w", err)
	}

	content, err := ReplySignContent(hashC1, hashC2, request.Challenge1, session.Challenge2, request.DH1, dh2)
	if err != nil {
		return nil, err
	}

	signature, err := SignHandshakeData(m.signer, content)
	if err != nil {
		return nil, err
	}

	return signature, nil
}

// checkReplyBasicsauth7 validates challenge echo, kagree echo, and dh2.
//
// It fails the session loudly on mismatch. The caller holds m.mu.
func checkReplyBasicsauth7(m *Manager, session *Session, reply tokens.HandshakeReplyToken) error {
	err := VerifyChallengeEcho(session.Challenge1, reply.Challenge1Echo)
	if err != nil {
		return m.failLocked(session, err)
	}

	if reply.KeyAgreeAlgo != session.LocalKAgree {
		err := fmt.Errorf("auth: reply kagree %q mismatches offer %q: %w", reply.KeyAgreeAlgo, session.LocalKAgree, ErrHandshakeFailed)

		return m.failLocked(session, err)
	}

	if len(reply.DH2) == 0 {
		err := fmt.Errorf("auth: reply without dh2: %w", ErrHandshakeFailed)

		return m.failLocked(session, err)
	}

	return nil
}

// HandleReply processes one reply on the initiator side.
func (m *Manager) HandleReply(remote string, reply tokens.HandshakeReplyToken, inboundID uint64, now time.Time) (tokens.HandshakeFinalToken, uint64, error) {
	var out tokens.HandshakeFinalToken

	m.mu.Lock()
	defer m.mu.Unlock()

	if isSelfMessageauth7(m.certDER, reply.IdentityCert, reply.ParticipantData) {
		return out, 0, fmt.Errorf("auth: self reply dropped: %w", errors.Join(ErrSelfauth7, ErrHandshakeFailed))
	}

	session, ok := m.sessions[remote]
	if !ok {
		return out, 0, fmt.Errorf("auth: reply without session for %s: %w", remote, ErrNoSession)
	}

	if _, seen := session.Seen[inboundID]; seen {
		return out, 0, fmt.Errorf("auth: replay reply %d from %s: %w", inboundID, remote, ErrStaleHandshake)
	}

	session.Seen[inboundID] = struct{}{}

	if session.State != StateReqSent {
		return out, 0, fmt.Errorf("auth: reply in state %d: %w", int(session.State), ErrWrongState)
	}

	err := checkReplyBasicsauth7(m, session, reply)
	if err != nil {
		return out, 0, err
	}

	secret, err := m.auth.SharedSecret(reply.DH2)
	if err != nil {
		session.State = StateFailed
		session.Result = ValidationFailed
		session.FailReason = err.Error()

		return out, 0, err
	}

	public, err := m.auth.PublicKey()
	if err != nil {
		return out, 0, err
	}

	err = m.verifyReplyLocked(session, reply, public)
	if err != nil {
		session.State = StateFailed
		session.Result = ValidationFailed
		session.FailReason = err.Error()

		return out, 0, err
	}

	msgID := m.nextMessageIDLocked()

	session.Challenge2 = reply.Challenge2
	session.PeerPublic = append([]byte(nil), reply.DH2...)
	session.PeerCertDER = append([]byte(nil), reply.IdentityCert...)
	session.ReplyPerm = append([]byte(nil), reply.Permissions...)
	session.ReplyPData = append([]byte(nil), reply.ParticipantData...)
	session.ReplyDSign = reply.SignAlgo
	session.ReplyKAgree = reply.KeyAgreeAlgo
	session.Secret = secret
	session.State = StateAuthenticated
	session.Result = ValidationOKFinalMessage
	session.LastOutboundID = msgID
	session.LastSendAt = now

	var signature []byte

	if m.haveIdentity {
		signature, err = m.signFinalLocked(session, public)
		if err != nil {
			session.State = StateFailed
			session.Result = ValidationFailed
			session.FailReason = err.Error()

			return out, 0, err
		}
	}

	out = tokens.HandshakeFinalToken{
		Class:          tokens.TokenFinal,
		Challenge1Echo: session.Challenge1,
		Challenge2Echo: reply.Challenge2,
		Signature:      signature,
	}

	return out, msgID, nil
}

// verifyReplyLocked verifies the responder signature when identity is installed.
//
// The caller holds m.mu. Without local identity it fails closed per
// 9.3.2: an unsigned HandshakeReply must never complete as authenticated.
func (m *Manager) verifyReplyLocked(session *Session, reply tokens.HandshakeReplyToken, dh1 []byte) error {
	if !m.haveIdentity {
		return fmt.Errorf("auth: reply without local identity material: %w", ErrHandshakeFailed)
	}

	if len(reply.IdentityCert) == 0 {
		return fmt.Errorf("auth: reply without c.id: %w", ErrHandshakeFailed)
	}

	err := checkPeerBindingauth7(session.PeerCertDER, reply.IdentityCert)
	if err != nil {
		return err
	}

	err = verifyPeerPDataauth7(reply.IdentityCert, reply.ParticipantData)
	if err != nil {
		return err
	}

	err = verifyRemoteIdentityauth7(m, reply.IdentityCert, reply.Permissions, reply.ParticipantData)
	if err != nil {
		return err
	}

	reqItems := []tokens.BinaryProperty{
		{Name: tokens.PropCID, Value: session.LocalCert},
		{Name: tokens.PropCPerm, Value: session.LocalPerm},
		{Name: tokens.PropCPData, Value: session.LocalPData},
		{Name: tokens.PropCDSignAlgo, Value: []byte(session.LocalDSign)},
		{Name: tokens.PropCKAgreeAlgo, Value: []byte(session.LocalKAgree)},
	}

	hashC1, err := tokens.HashBinaryItems(reqItems)
	if err != nil {
		return fmt.Errorf("auth: verify hash: %w", err)
	}

	hashC2, err := tokens.HashBinaryItems(tokens.ReplyHashItems(reply))
	if err != nil {
		return fmt.Errorf("auth: verify hash: %w", err)
	}

	content, err := ReplySignContent(hashC1, hashC2, session.Challenge1, reply.Challenge2, dh1, reply.DH2)
	if err != nil {
		return fmt.Errorf("auth: verify content: %w", err)
	}

	err = VerifySignature(reply.IdentityCert, content, reply.Signature)
	if err != nil {
		return fmt.Errorf("auth: reply signature: %w", err)
	}

	return nil
}

// signFinalLocked signs the final content when identity is installed.
//
// The caller holds m.mu.
func (m *Manager) signFinalLocked(session *Session, dh1 []byte) ([]byte, error) {
	reqItems := []tokens.BinaryProperty{
		{Name: tokens.PropCID, Value: session.LocalCert},
		{Name: tokens.PropCPerm, Value: session.LocalPerm},
		{Name: tokens.PropCPData, Value: session.LocalPData},
		{Name: tokens.PropCDSignAlgo, Value: []byte(session.LocalDSign)},
		{Name: tokens.PropCKAgreeAlgo, Value: []byte(session.LocalKAgree)},
	}

	hashC1, err := tokens.HashBinaryItems(reqItems)
	if err != nil {
		return nil, fmt.Errorf("auth: final hash: %w", err)
	}

	peerItems := []tokens.BinaryProperty{
		{Name: tokens.PropCID, Value: session.PeerCertDER},
		{Name: tokens.PropCPerm, Value: session.ReplyPerm},
		{Name: tokens.PropCPData, Value: session.ReplyPData},
		{Name: tokens.PropCDSignAlgo, Value: []byte(session.ReplyDSign)},
		{Name: tokens.PropCKAgreeAlgo, Value: []byte(session.ReplyKAgree)},
	}

	hashC2, err := tokens.HashBinaryItems(peerItems)
	if err != nil {
		return nil, fmt.Errorf("auth: final hash: %w", err)
	}

	content, err := FinalSignContent(hashC1, hashC2, session.Challenge1, session.Challenge2, dh1, session.PeerPublic)
	if err != nil {
		return nil, err
	}

	signature, err := SignHandshakeData(m.signer, content)
	if err != nil {
		return nil, err
	}

	return signature, nil
}

// HandleFinal processes one final token on the replier side.
func (m *Manager) HandleFinal(remote string, final tokens.HandshakeFinalToken, inboundID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[remote]
	if !ok {
		return fmt.Errorf("auth: final without session for %s: %w", remote, ErrNoSession)
	}

	if _, seen := session.Seen[inboundID]; seen {
		return fmt.Errorf("auth: replay final %d from %s: %w", inboundID, remote, ErrStaleHandshake)
	}

	session.Seen[inboundID] = struct{}{}

	if session.State != StateRepSent {
		return fmt.Errorf("auth: final in state %d: %w", int(session.State), ErrWrongState)
	}

	err := VerifyChallengeEcho(session.Challenge2, final.Challenge2Echo)
	if err != nil {
		session.State = StateFailed
		session.Result = ValidationFailed
		session.FailReason = err.Error()

		return err
	}

	err = m.verifyFinalLocked(session, final)
	if err != nil {
		session.State = StateFailed
		session.Result = ValidationFailed
		session.FailReason = err.Error()

		return err
	}

	session.State = StateAuthenticated
	session.Result = ValidationOK

	return nil
}

// verifyFinalLocked verifies the initiator signature when a peer cert is known.
//
// The caller holds m.mu. Without a peer certificate it fails closed per
// 9.3.2: an unsigned HandshakeFinal must never complete as authenticated.
// When a trust pool is installed the stored peer certificate chain plus
// validity windows re-verify per 9.4.2.8 before the signature check.
func (m *Manager) verifyFinalLocked(session *Session, final tokens.HandshakeFinalToken) error {
	if len(session.PeerCertDER) == 0 {
		return fmt.Errorf("auth: final without peer c.id: %w", ErrHandshakeFailed)
	}

	if len(final.Signature) == 0 {
		return fmt.Errorf("auth: final without signature: %w", ErrHandshakeFailed)
	}

	err := verifyRemoteIdentityauth7(m, session.ReqCert, session.ReqPerm, session.ReqPData)
	if err != nil {
		return err
	}

	reqItems := []tokens.BinaryProperty{
		{Name: tokens.PropCID, Value: session.ReqCert},
		{Name: tokens.PropCPerm, Value: session.ReqPerm},
		{Name: tokens.PropCPData, Value: session.ReqPData},
		{Name: tokens.PropCDSignAlgo, Value: []byte(session.ReqDSign)},
		{Name: tokens.PropCKAgreeAlgo, Value: []byte(session.ReqKAgree)},
	}

	hashC1, err := tokens.HashBinaryItems(reqItems)
	if err != nil {
		return fmt.Errorf("auth: final hash: %w", err)
	}

	replyItems := []tokens.BinaryProperty{
		{Name: tokens.PropCID, Value: m.certDER},
		{Name: tokens.PropCPerm, Value: m.permDoc},
		{Name: tokens.PropCPData, Value: m.pdata},
		{Name: tokens.PropCDSignAlgo, Value: []byte(tokens.DSignAlgoECDSA)},
		{Name: tokens.PropCKAgreeAlgo, Value: []byte(session.ReqKAgree)},
	}

	hashC2, err := tokens.HashBinaryItems(replyItems)
	if err != nil {
		return fmt.Errorf("auth: verify hash: %w", err)
	}

	public, err := m.auth.PublicKey()
	if err != nil {
		return err
	}

	content, err := FinalSignContent(hashC1, hashC2, session.Challenge1, session.Challenge2, session.PeerPublic, public)
	if err != nil {
		return err
	}

	err = VerifySignature(session.PeerCertDER, content, final.Signature)
	if err != nil {
		return fmt.Errorf("auth: final signature: %w", err)
	}

	return nil
}

// PeerCertificate returns the peer DER identity certificate once received.
func (m *Manager) PeerCertificate(remote string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[remote]
	if !ok {
		return nil, fmt.Errorf("auth: certificate without session for %s: %w", remote, ErrNoSession)
	}

	if len(session.PeerCertDER) == 0 {
		return nil, fmt.Errorf("auth: no peer certificate for %s: %w", remote, ErrHandshakeFailed)
	}

	return append([]byte(nil), session.PeerCertDER...), nil
}

// SharedSecret returns the derived ECDHE secret for an authenticated peer.
// It fails closed when no peer identity certificate was verified, so a
// ValidationOK without identity can never release key material.
func (m *Manager) SharedSecret(remote string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[remote]
	if !ok {
		return nil, fmt.Errorf("auth: secret without session for %s: %w", remote, ErrNoSession)
	}

	if session.State != StateAuthenticated {
		return nil, fmt.Errorf("auth: secret before authentication for %s: %w", remote, ErrWrongState)
	}

	if len(session.PeerCertDER) == 0 {
		return nil, fmt.Errorf("auth: secret without peer identity for %s: %w", remote, ErrHandshakeFailed)
	}

	return append([]byte(nil), session.Secret...), nil
}

// Validation returns the last validation result for one peer.
func (m *Manager) Validation(remote string) ValidationResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[remote]
	if !ok {
		return ValidationFailed
	}

	return session.Result
}

// Authenticated reports whether one peer completed with verified identity.
//
// It requires the authenticated state plus a verified peer certificate
// plus an OK validation result, so callers never mistake an unsigned
// ValidationOK for authentication.
func (m *Manager) Authenticated(remote string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[remote]
	if !ok {
		return false
	}

	if session.State != StateAuthenticated {
		return false
	}

	if len(session.PeerCertDER) == 0 {
		return false
	}

	return session.Result == ValidationOK || session.Result == ValidationOKFinalMessage
}

// NeedsResend reports whether the last outbound needs retransmit.
//
// The stateless channel assumes no reliability, so the caller resends
// the stored message identity after timeout with the same identity per
// 9.3.2.9.1. Authenticated and failed sessions never resend.
func (m *Manager) NeedsResend(remote string, now time.Time, timeout time.Duration) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[remote]
	if !ok {
		return 0, false
	}

	if session.State != StateReqSent && session.State != StateRepSent {
		return 0, false
	}

	if now.Sub(session.LastSendAt) < timeout {
		return 0, false
	}

	return session.LastOutboundID, true
}

// TouchSend refreshes the last send time after a retransmit.
func (m *Manager) TouchSend(remote string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session, ok := m.sessions[remote]
	if !ok {
		return
	}

	session.LastSendAt = now
}

// Fail marks one peer failed with a loud reason for eviction and backoff.
func (m *Manager) Fail(remote, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	session := m.sessionLocked(remote)
	session.State = StateFailed
	session.Result = ValidationFailed
	session.FailReason = reason
}
