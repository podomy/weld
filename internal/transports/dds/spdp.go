// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package dds implements the DDS/RTPS wire transport.
//
// This file implements SPDP participant discovery for first contact as
// genuine RTPS: announcements are RTPS messages with our 12 byte GUID
// prefix in the header and one DATA submessage to the builtin SPDP
// writer. It is pure Go with no cgo, LAN reachable with loopback fallback, and decoupled
// from any socket layer through small local Sender and Receiver
// interfaces. The backend wires those interfaces to the real transport.
package dds

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	ddssecurity "github.com/podomy/weld/internal/transports/dds/security"
	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// DefaultLeaseDuration is the default SPDP participant lease.
const DefaultLeaseDuration = 10 * time.Second

// Protocol version announced in SPDP params.
const (
	protocolMajor = 2
	protocolMinor = 5
)

// Vendor id announced as Eclipse CycloneDDS.
//
// Both the RTPS header vendor and the PID 0x0016 value carry 01 10 as
// observed on a live domain 42 Cyclone talker in ros-jazzy.
const (
	vendorID0 = 0x01
	vendorID1 = 0x10
)

// Participant entity id for the standard DDS participant.
var participantEntityID = [4]byte{0x00, 0x00, 0x01, 0xC1}

// pidDomainID carries the domain tag in SPDP params.
//
// It is absent from the rtps PID table but present on the live wire: a
// domain 42 Cyclone announcement carries PID 0x000f with value 42, so
// announcements include it and the parser reads it back to preserve the
// DomainID field across the RTPS rewrite.
const pidDomainID uint16 = 0x000f

// spdp2PIDDomainTag carries the DDS domain tag string per Table 9.19.
const spdp2PIDDomainTag uint16 = 0x4014

// spdp2BestEffortParticipantMessageBit selects best-effort reader QoS.
//
// It is bit 0 of BuiltinEndpointQos per 9.3.2. Absent QoS means reliable
// per 8.4.13.3.
const spdp2BestEffortParticipantMessageBit uint32 = 1 << 0

// Sentinel errors for SPDP handling.
var (
	// ErrInvalidSPDP reports a malformed SPDP packet.
	ErrInvalidSPDP = errors.New("dds: invalid SPDP packet")
	// ErrInvalidLease reports a non positive lease duration.
	ErrInvalidLease = errors.New("dds: invalid lease duration")
	// ErrNilSender reports a nil Sender.
	ErrNilSender = errors.New("dds: nil sender")
	// ErrNilReceiver reports a nil Receiver.
	ErrNilReceiver = errors.New("dds: nil receiver")
	// ErrNilContext reports a nil context.
	ErrNilContext = errors.New("dds: nil context")
	// ErrInvalidTime reports a zero time where a real clock is required.
	ErrInvalidTime = errors.New("dds: invalid time")
	// ErrInvalidPeriod reports a non positive loop period.
	ErrInvalidPeriod = errors.New("dds: non positive period")
	// ErrStaleSPDP reports a replayed sequence at or below the last accepted value.
	ErrStaleSPDP = errors.New("dds: stale SPDP sequence")
	// ErrSpdp2UnnegotiatedAlgo reports that no secure announce algorithm
	// could be negotiated from the governance allow-list.
	ErrSpdp2UnnegotiatedAlgo = errors.New("dds: unnegotiated secure algorithm")
)

// errSpdp2DomainMismatch reports a domain or tag gate reject.
var errSpdp2DomainMismatch = errors.New("dds: spdp domain mismatch")

// ErrSpdp2DomainMismatch reports a domain or tag gate reject.
//
// It aliases the internal gate sentinel so external callers distinguish
// domain rejects from stale sequence rejects.
var ErrSpdp2DomainMismatch = errSpdp2DomainMismatch

// spdpParseDrops counts SPDP DATA entries skipped after a parse failure.
var spdpParseDrops atomic.Uint64

// GUIDPrefix is the 12 byte RTPS GUID prefix.
type GUIDPrefix [12]byte

// GUID is an RTPS GUID with prefix and entity id.
type GUID struct {
	// Prefix is the random participant prefix.
	Prefix GUIDPrefix
	// EntityID is the entity within the participant.
	EntityID [4]byte
}

// ParticipantInfo is the remote record kept per GUID prefix.
type ParticipantInfo struct {
	// GUID is the remote participant GUID.
	GUID GUID
	// LeaseExpiry is the time the remote lease ends.
	LeaseExpiry time.Time
	// LastSeq is the last accepted SPDP sequence.
	LastSeq uint64
	// DomainID is the accepted DDS domain for the prefix.
	DomainID uint32
	// DomainTag is the accepted DDS domain tag, empty for default.
	DomainTag string
	// AvailableBuiltinEndpoints is the parsed 0x0058 mask.
	AvailableBuiltinEndpoints uint32
	// BuiltinEndpointQos is the parsed 0x0077 mask.
	BuiltinEndpointQos uint32
	// ManualLivelinessCount is the high-water manual count.
	ManualLivelinessCount uint32
	// ManualLeaseExpiry is the manual liveliness deadline, zero when the
	// peer never asserted manual liveliness. Only a higher manual count
	// renews it, so silence past the lease expires the peer per 8.5.3.2
	// even while periodic automatic re-announces continue.
	ManualLeaseExpiry time.Time
	// Security carries the parsed DDS-Security attributes for the
	// participant, zero when the peer announced none.
	Security SecurityInfo
}

// Announcement is a parsed SPDP packet.
type Announcement struct {
	// GUID is the announcing participant GUID.
	GUID GUID
	// DomainID is the DDS domain from the domain tag param.
	DomainID uint32
	// DomainTag is the DDS domain tag from PID 0x4014, empty when absent.
	DomainTag string
	// Sequence is the SPDP sequence number.
	Sequence uint64
	// Alive is true for DATA and false for dispose.
	Alive bool
	// Lease is the announced lease, zero for dispose.
	Lease time.Duration
	// Ports holds all UDPv4 unicast locator ports in wire order, empty for dispose.
	Ports []uint16
	// Hosts holds the UDPv4 unicast locator IPs in wire order, empty for dispose.
	Hosts []string
	// AvailableBuiltinEndpoints is the parsed 0x0058 mask.
	AvailableBuiltinEndpoints uint32
	// ManualLivelinessCount is the participant manual liveliness count, zero when absent.
	ManualLivelinessCount uint32
	// BuiltinEndpointQos is the builtin endpoint QoS mask, zero when absent.
	BuiltinEndpointQos uint32
	// Security carries the parsed DDS-Security attributes, zero when the
	// peer announced none. Secured peers are detected, never decrypted.
	Security SecurityInfo
}

// Sender transmits SPDP payloads to the transport.
//
// The slice is owned by the caller. Implementations must copy when
// retained beyond the call. The backend wires this to RTPS later.
type Sender interface {
	// Send transmits one SPDP payload.
	Send(ctx context.Context, data []byte) error
}

// Receiver obtains SPDP payloads from the transport.
//
// The returned slice is newly owned by the caller. The backend wires
// this to RTPS later.
type Receiver interface {
	// Receive blocks until one SPDP payload arrives or ctx ends.
	Receive(ctx context.Context) ([]byte, error)
}

// LocalParticipant is the local SPDP identity.
//
// GUID prefix is random per run. Domain is resolved through
// transports.ResolveDomainID. Lease is fixed at construction.
// Sequence starts at zero and increments per announce.
// Announce ports default to zero, meaning legacy participant-zero ports;
// SetAnnouncePorts pins the actually bound unicast ports so a second
// participant on one host never advertises the first participant's port.
type LocalParticipant struct {
	mu     sync.Mutex
	guid   GUID
	domain uint32
	lease  time.Duration
	seq    uint64
	// manual is the MANUAL_BY_PARTICIPANT count for PID 0x0034.
	manual uint32
	// domainTag is the local DDS domain tag, empty for default.
	domainTag string
	// metaPort is the bound metatraffic unicast port, zero for legacy.
	metaPort int
	// defaultPort is the bound user-traffic unicast port, zero for legacy.
	defaultPort int
	// secureAllowed is the governance allow-list for secure announces, nil for tolerance mode.
	secureAllowed []string
	// secureCert is the local identity certificate for secure announces, nil for tolerance mode.
	secureCert *x509.Certificate
	// secureCASubject is the CA subject for the identity token, valid when secureCert is set.
	secureCASubject string
}

// NewLocalParticipant creates a local participant identity.
//
// It resolves callerDomain through transports.ResolveDomainID so
// ROS_DOMAIN_ID overrides the caller value, generates a random GUID
// prefix, and pins the participant entity id. Lease must be positive.
func NewLocalParticipant(callerDomain uint32, lease time.Duration) (*LocalParticipant, error) {
	if lease <= 0 {
		return nil, fmt.Errorf("dds: lease %v must be positive: %w", lease, ErrInvalidLease)
	}

	resolved, err := transports.ResolveDomainID(callerDomain)
	if err != nil {
		return nil, fmt.Errorf("dds: resolve domain: %w", err)
	}

	var prefix GUIDPrefix

	n, err := rand.Read(prefix[:])
	if err != nil {
		return nil, fmt.Errorf("dds: random prefix: %w", err)
	}

	if n != len(prefix) {
		return nil, fmt.Errorf("dds: short random read %d: %w", n, ErrInvalidSPDP)
	}

	if isZeroPrefix(prefix) {
		return nil, fmt.Errorf("dds: random prefix is zero: %w", ErrInvalidSPDP)
	}

	return &LocalParticipant{
		guid:   GUID{Prefix: prefix, EntityID: participantEntityID},
		domain: resolved,
		lease:  lease,
	}, nil
}

// GUID returns the local participant GUID.
func (p *LocalParticipant) GUID() GUID {
	return p.guid
}

// DomainID returns the resolved domain id.
func (p *LocalParticipant) DomainID() uint32 {
	return p.domain
}

// LeaseDuration returns the configured lease duration.
func (p *LocalParticipant) LeaseDuration() time.Duration {
	return p.lease
}

// Sequence returns the last assigned sequence number.
func (p *LocalParticipant) Sequence() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.seq
}

// Announce builds an alive DATA packet and sends it.
func (p *LocalParticipant) Announce(ctx context.Context, sender Sender) error {
	return p.send(ctx, sender, true)
}

// Goodbye builds a dispose packet and sends it.
func (p *LocalParticipant) Goodbye(ctx context.Context, sender Sender) error {
	return p.send(ctx, sender, false)
}

// AnnounceLoop sends alive announcements immediately and on every period until ctx ends.
//
// It sends one announcement before waiting, then ticks with a timer. It stops
// when ctx ends and returns ctx.Err wrapped so errors.Is reports
// context.Canceled or context.DeadlineExceeded. A nil sender or a non
// positive period is an explicit error. Sender failures are returned
// immediately unless ctx is done, in which case ctx.Err wins. It spawns no
// goroutine and stops its timer on return, so it is stop-clean. It is safe
// for concurrent use with other LocalParticipant methods.
func (p *LocalParticipant) AnnounceLoop(ctx context.Context, sender Sender, period time.Duration) error {
	if ctx == nil {
		return fmt.Errorf("dds: announce loop nil context: %w", ErrNilContext)
	}

	if sender == nil {
		return fmt.Errorf("dds: announce loop nil sender: %w", ErrNilSender)
	}

	if period <= 0 {
		return fmt.Errorf("dds: announce loop period %v must be positive: %w", period, ErrInvalidPeriod)
	}

	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: announce loop context: %w", err)
	}

	err = p.Announce(ctx, sender)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("dds: announce loop stopped: %w", ctx.Err())
		case <-ticker.C:
			err = p.Announce(ctx, sender)
			if err != nil {
				if ctx.Err() != nil {
					return fmt.Errorf("dds: announce loop stopped: %w", ctx.Err())
				}

				return err
			}
		}
	}
}

// SetAnnouncePorts pins the actually bound unicast ports for SPDP locators.
//
// Both ports are normally the single bound unicast socket port: the backend
// listens on one wildcard socket for metatraffic and user traffic alike.
// Zero values keep legacy participant-zero ports for unit goldens. It is
// safe for concurrent use.
func (p *LocalParticipant) SetAnnouncePorts(metaPort, defaultPort int) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.metaPort = metaPort
	p.defaultPort = defaultPort
}

// SetSpdp2DomainTag pins the local DDS domain tag for SPDP announces.
//
// Empty keeps the baseline wire shape with no PID 0x4014. Non-empty adds
// the tag param so peers gate per 8.5.5.1. It is safe for concurrent use.
func (p *LocalParticipant) SetSpdp2DomainTag(tag string) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.domainTag = tag
}

// SetSpdp2SecurityMaterial arms secure SPDP announces per DDS-SECURITY 7.4.
//
// Allowed must be the governance allow-list of algorithm names (for
// example ECDSA+P256+SHA256 plus ECDHE-CEUM+P256); cert is the local
// identity certificate and caSubject names its CA. When set, alive
// announces negotiate via Spdp2NegotiatedSecureAlgos (fail-closed on
// unnegotiated curves or allow-list misses), set the honest
// Spdp2SecureParticipantMask, and emit PID 0x1001 plus 0x1002 identity
// and permissions tokens carrying only the negotiated local algorithm
// names, never mirrored peer input. When unset (nil cert), announces use
// the tolerance baseline with byte-identical output. Goodbye packets are
// unaffected. It is safe for concurrent use.
func (p *LocalParticipant) SetSpdp2SecurityMaterial(allowed []string, cert *x509.Certificate, caSubject string) {
	if p == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.secureAllowed = append([]string(nil), allowed...)
	p.secureCert = cert
	p.secureCASubject = caSubject
}

// Spdp2ManualCount returns the current manual liveliness count.
func (p *LocalParticipant) Spdp2ManualCount() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.manual
}

// AssertSpdp2ManualLiveliness asserts MANUAL_BY_PARTICIPANT liveliness.
//
// It increments the manual count per Table 8.78 and sends a fresh alive
// DATA with the new count so peers renew the manual lease.
func (p *LocalParticipant) AssertSpdp2ManualLiveliness(ctx context.Context, sender Sender) error {
	if ctx == nil {
		return fmt.Errorf("dds: assert nil context: %w", ErrNilContext)
	}

	if sender == nil {
		return fmt.Errorf("dds: assert nil sender: %w", ErrNilSender)
	}

	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: assert context: %w", err)
	}

	p.mu.Lock()
	p.manual++
	p.mu.Unlock()

	err = p.Announce(ctx, sender)
	if err != nil {
		return err
	}

	return nil
}

// spdpAnnounce carries the next SPDP sequence plus immutable participant fields.
//
// It groups the snapshot taken under lock so send builds one packet from one value.
type spdpAnnounce struct {
	// seq is the next SPDP sequence number.
	seq uint64
	// guid is the local participant GUID.
	guid GUID
	// domain is the resolved domain id.
	domain uint32
	// lease is the configured lease duration.
	lease time.Duration
	// manual is the MANUAL_BY_PARTICIPANT count for PID 0x0034.
	manual uint32
	// domainTag is the local domain tag, empty for default.
	domainTag string
	// metaPort is the bound metatraffic unicast port, zero for legacy.
	metaPort int
	// defaultPort is the bound user-traffic unicast port, zero for legacy.
	defaultPort int
	// secureAllowed is the governance allow-list snapshot for this ticket.
	secureAllowed []string
	// secureCert is the local identity certificate snapshot, nil for tolerance mode.
	secureCert *x509.Certificate
	// secureCASubject is the CA subject snapshot for the identity token.
	secureCASubject string
}

// send assigns the next sequence and transmits one packet.
func (p *LocalParticipant) send(ctx context.Context, sender Sender, alive bool) error {
	if ctx == nil {
		return fmt.Errorf("dds: send nil context: %w", ErrNilContext)
	}

	if sender == nil {
		return fmt.Errorf("dds: send nil sender: %w", ErrNilSender)
	}

	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: send context: %w", err)
	}

	ticket, err := p.nextPacket()
	if err != nil {
		return err
	}

	var packet []byte

	if alive {
		packet, err = marshalSpdp2TicketAlive(ticket)
	} else {
		packet, err = MarshalDisposePacket(ticket.guid, ticket.domain, ticket.seq)
	}

	if err != nil {
		return err
	}

	err = sender.Send(ctx, packet)
	if err != nil {
		return fmt.Errorf("dds: send SPDP: %w", err)
	}

	return nil
}

// nextPacket increments the sequence and snapshots immutable fields.
func (p *LocalParticipant) nextPacket() (spdpAnnounce, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.seq == math.MaxUint64 {
		return spdpAnnounce{}, fmt.Errorf("dds: sequence overflow: %w", ErrInvalidSPDP)
	}

	p.seq++

	return spdpAnnounce{seq: p.seq, guid: p.guid, domain: p.domain, lease: p.lease, manual: p.manual, domainTag: p.domainTag, metaPort: p.metaPort, defaultPort: p.defaultPort, secureAllowed: append([]string(nil), p.secureAllowed...), secureCert: p.secureCert, secureCASubject: p.secureCASubject}, nil
}

// marshalSpdp2TicketAlive builds one alive packet from a ticket.
//
// Baseline tickets with zero manual, empty tag, and no security material
// use the exported wrappers so goldens stay byte-identical. Tickets
// carrying manual or tag use the detail builder. Tickets carrying
// security material use the secure builder, which negotiates from the
// governance allow-list plus the local certificate fail-closed and emits
// the honest secure mask plus identity and permissions tokens.
func marshalSpdp2TicketAlive(ticket spdpAnnounce) ([]byte, error) {
	if ticket.secureCert != nil {
		return marshalSpdp2SecureTicketAlive(ticket)
	}
	if ticket.manual == 0 && ticket.domainTag == "" {
		if ticket.metaPort > 0 && ticket.defaultPort > 0 {
			return MarshalAlivePacketWithPorts(ticket.guid, ticket.domain, ticket.seq, ticket.lease, ticket.metaPort, ticket.defaultPort)
		}

		return MarshalAlivePacket(ticket.guid, ticket.domain, ticket.seq, ticket.lease)
	}

	metaPort := ticket.metaPort
	defaultPort := ticket.defaultPort

	metaMCPort, err := rtps.MetatrafficMulticastPort(ticket.domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP meta multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	defaultMCPort, err := rtps.UsertrafficMulticastPort(ticket.domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP default multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	if metaPort <= 0 || defaultPort <= 0 {
		metaPort, err = rtps.MetatrafficUnicastPort(ticket.domain, 0)
		if err != nil {
			return nil, fmt.Errorf("dds: SPDP meta port: %w", errors.Join(err, ErrInvalidSPDP))
		}

		defaultPort, err = rtps.UsertrafficUnicastPort(ticket.domain, 0)
		if err != nil {
			return nil, fmt.Errorf("dds: SPDP default port: %w", errors.Join(err, ErrInvalidSPDP))
		}
	}

	return marshalSpdp2AliveWithDetail(ticket.guid, ticket.domain, ticket.seq, ticket.lease, metaPort, defaultPort, metaMCPort, defaultMCPort, ticket.manual, ticket.domainTag)
}

// marshalSpdp2SecureTicketAlive builds one secure alive packet from a ticket.
//
// It negotiates the announce algorithms from the ticket governance
// allow-list plus the local certificate via Spdp2NegotiatedSecureAlgos
// (fail-closed on unnegotiated curves or allow-list misses, never
// mirroring peer input), resolves the ticket unicast ports with legacy
// fallback like the baseline path, and emits the honest secure mask plus
// PID 0x1001 and 0x1002 tokens with the manual count and domain tag.
func marshalSpdp2SecureTicketAlive(ticket spdpAnnounce) ([]byte, error) {
	dsignToken, kagreeToken, err := Spdp2NegotiatedSecureAlgos(ticket.secureAllowed, ticket.secureCert)
	if err != nil {
		return nil, err
	}

	metaPort := ticket.metaPort
	defaultPort := ticket.defaultPort

	metaMCPort, err := rtps.MetatrafficMulticastPort(ticket.domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP meta multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	defaultMCPort, err := rtps.UsertrafficMulticastPort(ticket.domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP default multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	if metaPort <= 0 || defaultPort <= 0 {
		metaPort, err = rtps.MetatrafficUnicastPort(ticket.domain, 0)
		if err != nil {
			return nil, fmt.Errorf("dds: SPDP meta port: %w", errors.Join(err, ErrInvalidSPDP))
		}

		defaultPort, err = rtps.UsertrafficUnicastPort(ticket.domain, 0)
		if err != nil {
			return nil, fmt.Errorf("dds: SPDP default port: %w", errors.Join(err, ErrInvalidSPDP))
		}
	}

	return marshalAliveSecureWithDetail(ticket.guid, ticket.domain, ticket.seq, ticket.lease, metaPort, defaultPort, metaMCPort, defaultMCPort, dsignToken, kagreeToken, ticket.secureCert, ticket.secureCASubject, ticket.manual, ticket.domainTag)
}

// marshalSpdp2AliveWithDetail builds an alive packet with manual and tag.
//
// It mirrors marshalAliveWithPorts but carries the ticket manual count
// in PID 0x0034 and appends PID 0x4014 when the tag is non-empty.
func marshalSpdp2AliveWithDetail(guid GUID, domain uint32, seq uint64, lease time.Duration, metaPort, defaultPort, metaMCPort, defaultMCPort int, manual uint32, domainTag string) ([]byte, error) {
	err := validatePacketHeader(guid, domain, seq)
	if err != nil {
		return nil, err
	}

	sec, nanos, err := durationToParts(lease)
	if err != nil {
		return nil, err
	}

	advertise := rtps.AdvertiseIPv4()

	group := net.ParseIP(rtps.SPDPMulticastGroup)

	domainRaw := domainRawBytes(domain)

	var leaseRaw [8]byte

	binary.LittleEndian.PutUint32(leaseRaw[0:4], sec)
	binary.LittleEndian.PutUint32(leaseRaw[4:8], nanos)

	var besRaw [4]byte

	binary.LittleEndian.PutUint32(besRaw[:], spdpBuiltinEndpointMask)

	var builtinQosRaw [4]byte

	binary.LittleEndian.PutUint32(builtinQosRaw[:], 0)

	var manualRaw [4]byte

	binary.LittleEndian.PutUint32(manualRaw[:], manual)

	userdata := []byte(spdpUserDataEnclave)

	var userdataRaw []byte

	userdataRaw = binary.LittleEndian.AppendUint32(userdataRaw, uint32(len(userdata))) //nolint:gosec // G115: enclave string is a short constant
	userdataRaw = append(userdataRaw, userdata...)
	for len(userdataRaw)%4 != 0 {
		userdataRaw = append(userdataRaw, 0x00)
	}

	params := spdpAliveParams(guid, domainRaw, leaseRaw, besRaw, builtinQosRaw, manualRaw, userdataRaw, advertise, group, metaPort, defaultPort, metaMCPort, defaultMCPort)

	if domainTag != "" {
		params = append(params, rtps.Param{ID: spdp2PIDDomainTag, Value: spdp2EncodeDomainTag(domainTag)})
	}

	encoded, err := encodeSPDPParams(params)
	if err != nil {
		return nil, err
	}

	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: seq,
		Payload:        encoded,
	}

	return frameSPDP(guid.Prefix, data)
}

// Spdp2MarshalAliveWithManual builds an alive packet with manual and tag.
//
// It uses participant-zero ports and is intended for tests pinning
// same-sequence manual refresh and domain tag gating.
func Spdp2MarshalAliveWithManual(guid GUID, domain uint32, seq uint64, lease time.Duration, manual uint32, domainTag string) ([]byte, error) {
	metaPort, err := rtps.MetatrafficUnicastPort(domain, 0)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP meta port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	defaultPort, err := rtps.UsertrafficUnicastPort(domain, 0)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP default port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	metaMCPort, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP meta multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	defaultMCPort, err := rtps.UsertrafficMulticastPort(domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP default multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	return marshalSpdp2AliveWithDetail(guid, domain, seq, lease, metaPort, defaultPort, metaMCPort, defaultMCPort, manual, domainTag)
}

// spdp2EncodeDomainTag renders a CDR string for PID 0x4014.
//
// Layout is 4 byte LE length including NUL plus pad, then bytes plus
// NUL plus zero pad to 4 bytes. Length includes pad to match Cyclone.
func spdp2EncodeDomainTag(tag string) []byte {
	raw := make([]byte, 0, 4+len(tag)+4)
	raw = append(raw, 0, 0, 0, 0)
	raw = append(raw, tag...)
	raw = append(raw, 0x00)
	for len(raw)%4 != 0 {
		raw = append(raw, 0x00)
	}
	binary.LittleEndian.PutUint32(raw[:4], uint32(len(raw)-4)) //nolint:gosec // G115: tags are short.

	return raw
}

// SPDPUnicastPort returns the SPDP unicast port for a domain.
//
// It delegates to rtps.MetatrafficUnicastPort with participant zero, so
// the port follows rtps.BasePort plus rtps.DomainGain times domain and
// matches the discovery unicast assignment. It fits uint16 for valid domains.
func SPDPUnicastPort(domain uint32) (uint16, error) {
	port, err := rtps.MetatrafficUnicastPort(domain, 0)
	if err != nil {
		return 0, fmt.Errorf("dds: SPDP unicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	return uint16(port), nil //nolint:gosec // G115: standard ports fit uint16.
}

// spdpBuiltinEndpointMask advertises participant, publication, and
// subscription announcer plus detector endpoints. It matches the
// participant, publications, and subscriptions bits CycloneDDS sends,
// without claiming participant message or type lookup support.
const spdpBuiltinEndpointMask uint32 = 0x0000003F

// spdpMaxSubmessages caps one datagram submessage walk for bounded work.
//
// The bound matches rtps maxSubmessagesPerDatagram (4096) so the
// standalone patch walk and the rtps decode path agree; over-cap
// datagrams drop loudly instead of trusting an unbounded header chain.
const spdpMaxSubmessages = 4096

// spdpEncapSize is the 4 byte parameter list encapsulation header.
const spdpEncapSize = 4

// spdpEncapPLCDRLE marks a little endian parameter list payload.
var spdpEncapPLCDRLE = [4]byte{0x00, 0x03, 0x00, 0x00}

// spdpEncapPLCDRBE marks a big endian parameter list payload.
var spdpEncapPLCDRBE = [4]byte{0x00, 0x02, 0x00, 0x00}

// spdpUserDataEnclave is the default ROS enclave carried in USER_DATA so
// peers that key enrollment on enclave accept the participant.
const spdpUserDataEnclave = "enclave=/;"

// encodeSPDPParams renders params with a PL_CDR little endian header plus
// a terminating sentinel for DATA payloads.
//
// The encapsulation header is prepended here so callers do not duplicate
// it. The header bytes are copied explicitly so the shared array backing
// spdpEncapPLCDRLE is never mutated by append. Oversize values fail loudly
// instead of silent truncation.
func encodeSPDPParams(params []rtps.Param) ([]byte, error) {
	out := append([]byte(nil), spdpEncapPLCDRLE[:]...)

	encoded, err := rtps.EncodeParams(params)
	if err != nil {
		return nil, fmt.Errorf("dds: encode spdp params: %w", err)
	}

	return append(out, encoded...), nil
}

// validatePacketHeader checks domain range, sequence, and GUID prefix.
func validatePacketHeader(guid GUID, domain uint32, seq uint64) error {
	if domain > transports.MaxDomainID {
		return fmt.Errorf("dds: domain %d above maximum %d: %w", domain, transports.MaxDomainID, ErrInvalidSPDP)
	}

	if seq == 0 {
		return fmt.Errorf("dds: sequence must be non-zero: %w", ErrInvalidSPDP)
	}

	if isZeroPrefix(guid.Prefix) {
		return fmt.Errorf("dds: GUID prefix is zero: %w", ErrInvalidSPDP)
	}

	return nil
}

// domainRawBytes renders domain as 4 little endian bytes.
func domainRawBytes(domain uint32) [4]byte {
	var raw [4]byte

	binary.LittleEndian.PutUint32(raw[:], domain)

	return raw
}

// frameSPDP frames one DATA submessage in an RTPS message.
//
// It carries the SPDP header plus DATA body used by both alive and
// dispose packets so the two marshal paths share one framing step.
func frameSPDP(prefix GUIDPrefix, data rtps.Data) ([]byte, error) {
	msg, err := encodeSPDPMessage(prefix, data)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// MarshalAlivePacketWithPorts builds an alive packet announcing explicit
// unicast locator ports, normally the actually bound socket ports. Peers
// send metatraffic and user DATA to these ports, so advertising a port
// owned by another participant would divert that participant's traffic.
// Zero or negative ports fall back to legacy participant-zero ports.
func MarshalAlivePacketWithPorts(guid GUID, domain uint32, seq uint64, lease time.Duration, metaPort, defaultPort int) ([]byte, error) {
	if metaPort <= 0 || defaultPort <= 0 {
		return MarshalAlivePacket(guid, domain, seq, lease)
	}

	if metaPort > rtps.MaxPort || defaultPort > rtps.MaxPort {
		return nil, fmt.Errorf("dds: SPDP announce ports above max: %w", ErrInvalidSPDP)
	}

	metaMCPort, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP meta multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	defaultMCPort, err := rtps.UsertrafficMulticastPort(domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP default multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	return marshalAliveWithPorts(guid, domain, seq, lease, metaPort, defaultPort, metaMCPort, defaultMCPort)
}

// MarshalAlivePacket builds an alive RTPS SPDP DATA packet.
//
// The datagram is an RTPS header carrying our 12 byte GUID prefix plus
// one little endian DATA submessage to builtin writer 0x000100c2 addressed
// to the unknown reader with the given sequence. The parameter list
// travels in the payload behind a PL_CDR header, matching what CycloneDDS
// sends and accepts; the payload carries the participant GUID, protocol
// version 2.1, CycloneDDS vendor id, domain tag, lease duration,
// builtin endpoint set, builtin endpoint QoS, manual liveliness count,
// enclave user data, metatraffic plus default
// unicast locators on the LAN address with participant-zero unicast ports
// for the domain, plus metatraffic plus default multicast locators on the
// SPDP group. Participants that bound a later slot must use
// MarshalAlivePacketWithPorts with their actual ports instead, or peers
// send metatraffic and user DATA to the wrong socket. Manual count is
// zero because this package never asserts manual liveliness. Builtin QoS
// is zero for the non secure baseline. Tolerance path stays byte-identical:
// secure announces must use MarshalAlivePacketSecure instead, which emits
// identity plus permissions tokens with negotiated algorithms per 7.4.
//
// Unknown vendor PIDs such as 0x8003, 0x8007, 0x8019, and 0x800c are
// tolerated on receive and survive an rtps round trip.
func MarshalAlivePacket(guid GUID, domain uint32, seq uint64, lease time.Duration) ([]byte, error) {
	metaPort, err := rtps.MetatrafficUnicastPort(domain, 0)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP meta port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	defaultPort, err := rtps.UsertrafficUnicastPort(domain, 0)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP default port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	metaMCPort, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP meta multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	defaultMCPort, err := rtps.UsertrafficMulticastPort(domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP default multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}

	return marshalAliveWithPorts(guid, domain, seq, lease, metaPort, defaultPort, metaMCPort, defaultMCPort)
}

// spdpAliveParams builds the shared SPDP alive parameter list.
//
// It carries GUID, version, vendor, domain, lease, builtin set and QoS,
// manual count, enclave user data, plus unicast and multicast locators
// so both alive marshal paths share one layout.
func spdpAliveParams(guid GUID, domainRaw [4]byte, leaseRaw [8]byte, besRaw [4]byte, builtinQosRaw [4]byte, manualRaw [4]byte, userdataRaw []byte, advertise, group net.IP, metaPort, defaultPort, metaMCPort, defaultMCPort int) []rtps.Param {
	return []rtps.Param{
		{ID: rtps.PIDParticipantGUID, Value: guidBytes(guid)},
		{ID: rtps.PIDProtocolVersion, Value: []byte{protocolMajor, protocolMinor, 0x00, 0x00}},
		{ID: rtps.PIDVendorID, Value: []byte{vendorID0, vendorID1, 0x00, 0x00}},
		{ID: pidDomainID, Value: append([]byte(nil), domainRaw[:]...)},
		{ID: rtps.PIDParticipantLeaseDuration, Value: append([]byte(nil), leaseRaw[:]...)},
		{ID: rtps.PIDBuiltinEndpointSet, Value: append([]byte(nil), besRaw[:]...)},
		{ID: rtps.PIDBuiltinEndpointQos, Value: append([]byte(nil), builtinQosRaw[:]...)},
		{ID: rtps.PIDParticipantManualLivelinessCount, Value: append([]byte(nil), manualRaw[:]...)},
		{ID: rtps.PIDUserData, Value: userdataRaw},
		{ID: rtps.PIDMetatrafficUnicastLocator, Value: rtps.EncodeIPv4Locator(rtps.LocatorKindUDPv4, uint32(metaPort), advertise)},        //nolint:gosec // G115: ports are bounded by MaxPort 65535.
		{ID: rtps.PIDDefaultUnicastLocatorStandard, Value: rtps.EncodeIPv4Locator(rtps.LocatorKindUDPv4, uint32(defaultPort), advertise)}, //nolint:gosec // G115: ports are bounded by MaxPort 65535.
		{ID: rtps.PIDMetatrafficMulticastLocator, Value: rtps.EncodeIPv4Locator(rtps.LocatorKindUDPv4, uint32(metaMCPort), group)},        //nolint:gosec // G115: ports are bounded by MaxPort 65535.
		{ID: rtps.PIDDefaultMulticastLocator, Value: rtps.EncodeIPv4Locator(rtps.LocatorKindUDPv4, uint32(defaultMCPort), group)},         //nolint:gosec // G115: ports are bounded by MaxPort 65535.
	}
}

// marshalAliveWithPorts builds an alive packet from explicit locator ports.
//
// It validates the header and lease, then encodes the shared parameter
// list; see MarshalAlivePacket for the wire layout.
func marshalAliveWithPorts(guid GUID, domain uint32, seq uint64, lease time.Duration, metaPort, defaultPort, metaMCPort, defaultMCPort int) ([]byte, error) {
	err := validatePacketHeader(guid, domain, seq)
	if err != nil {
		return nil, err
	}

	sec, nanos, err := durationToParts(lease)
	if err != nil {
		return nil, err
	}

	advertise := rtps.AdvertiseIPv4()

	group := net.ParseIP(rtps.SPDPMulticastGroup)

	domainRaw := domainRawBytes(domain)

	var leaseRaw [8]byte

	binary.LittleEndian.PutUint32(leaseRaw[0:4], sec)
	binary.LittleEndian.PutUint32(leaseRaw[4:8], nanos)

	var besRaw [4]byte

	binary.LittleEndian.PutUint32(besRaw[:], spdpBuiltinEndpointMask)

	var builtinQosRaw [4]byte

	binary.LittleEndian.PutUint32(builtinQosRaw[:], 0)

	var manualRaw [4]byte

	binary.LittleEndian.PutUint32(manualRaw[:], 0)

	userdata := []byte(spdpUserDataEnclave)

	var userdataRaw []byte

	userdataRaw = binary.LittleEndian.AppendUint32(userdataRaw, uint32(len(userdata))) //nolint:gosec // G115: enclave string is a short constant
	userdataRaw = append(userdataRaw, userdata...)
	// Pad the value itself to 4 bytes so the PID length includes pad,
	// matching CycloneDDS on the wire. Cyclone sends UserData length 16
	// for a 14 byte value plus 2 pad, and its parser rejects the standard
	// length-excludes-pad form with deserialization failed.
	for len(userdataRaw)%4 != 0 {
		userdataRaw = append(userdataRaw, 0x00)
	}

	params := spdpAliveParams(guid, domainRaw, leaseRaw, besRaw, builtinQosRaw, manualRaw, userdataRaw, advertise, group, metaPort, defaultPort, metaMCPort, defaultMCPort)

	encoded, err := encodeSPDPParams(params)
	if err != nil {
		return nil, err
	}

	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: seq,
		Payload:        encoded,
	}

	return frameSPDP(guid.Prefix, data)
}

// MarshalAlivePacketSecure builds an alive RTPS SPDP DATA packet with
// DDS Security identity plus permissions tokens per 7.4 and 9.3.2.
//
// It negotiates the announce algorithms from the governance allow-list
// plus the local certificate via Spdp2NegotiatedSecureAlgos (fail-closed
// on unnegotiated curves or allow-list misses), sets the secure
// participant mask via Spdp2SecureParticipantMask, and emits PID 0x1001
// plus 0x1002 token params carrying the negotiated algorithm names. The
// tolerance MarshalAlivePacket path stays byte-identical; this secure
// path is only for suite-enabled announces.
func MarshalAlivePacketSecure(guid GUID, domain uint32, seq uint64, lease time.Duration, allowed []string, cert *x509.Certificate, caSubject string) ([]byte, error) {
	dsignToken, kagreeToken, err := Spdp2NegotiatedSecureAlgos(allowed, cert)
	if err != nil {
		return nil, err
	}
	metaPort, err := rtps.MetatrafficUnicastPort(domain, 0)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP meta port: %w", errors.Join(err, ErrInvalidSPDP))
	}
	defaultPort, err := rtps.UsertrafficUnicastPort(domain, 0)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP default port: %w", errors.Join(err, ErrInvalidSPDP))
	}
	metaMCPort, err := rtps.MetatrafficMulticastPort(domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP meta multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}
	defaultMCPort, err := rtps.UsertrafficMulticastPort(domain)
	if err != nil {
		return nil, fmt.Errorf("dds: SPDP default multicast port: %w", errors.Join(err, ErrInvalidSPDP))
	}
	return marshalAliveSecureWithPorts(guid, domain, seq, lease, metaPort, defaultPort, metaMCPort, defaultMCPort, dsignToken, kagreeToken, cert, caSubject)
}

// marshalAliveSecureWithPorts builds a secure alive packet with zero
// manual count and no domain tag.
func marshalAliveSecureWithPorts(guid GUID, domain uint32, seq uint64, lease time.Duration, metaPort, defaultPort, metaMCPort, defaultMCPort int, dsignToken, kagreeToken string, cert *x509.Certificate, caSubject string) ([]byte, error) {
	return marshalAliveSecureWithDetail(guid, domain, seq, lease, metaPort, defaultPort, metaMCPort, defaultMCPort, dsignToken, kagreeToken, cert, caSubject, 0, "")
}

// marshalAliveSecureWithDetail builds a secure alive packet with tokens.
//
// The identity token carries the certificate subject, CA subject, and the
// negotiated algorithm names; the permissions token is empty per spec.
// Both encode via tokens.EncodeDataHolder into PID 0x1001 plus 0x1002.
// The builtin set carries the honest Spdp2SecureParticipantMask and the
// manual count plus optional domain tag ride alongside like the
// non-secure detail builder.
func marshalAliveSecureWithDetail(guid GUID, domain uint32, seq uint64, lease time.Duration, metaPort, defaultPort, metaMCPort, defaultMCPort int, dsignToken, kagreeToken string, cert *x509.Certificate, caSubject string, manual uint32, domainTag string) ([]byte, error) {
	err := validatePacketHeader(guid, domain, seq)
	if err != nil {
		return nil, err
	}
	sec, nanos, err := durationToParts(lease)
	if err != nil {
		return nil, err
	}
	certSN := ""
	if cert != nil {
		certSN = cert.Subject.String()
	}
	identity, err := tokens.BuildIdentityToken(tokens.IdentityToken{
		Class:    tokens.TokenIdentity,
		CertSN:   certSN,
		CertAlgo: dsignToken,
		CaSN:     caSubject,
		CaAlgo:   kagreeToken,
	})
	if err != nil {
		return nil, fmt.Errorf("dds: secure announce identity token: %w", err)
	}
	identityRaw, err := tokens.EncodeDataHolder(identity)
	if err != nil {
		return nil, fmt.Errorf("dds: secure announce identity encode: %w", err)
	}
	permissions := tokens.BuildPermissionsToken()
	permissionsRaw, err := tokens.EncodeDataHolder(permissions)
	if err != nil {
		return nil, fmt.Errorf("dds: secure announce permissions encode: %w", err)
	}
	advertise := rtps.AdvertiseIPv4()
	group := net.ParseIP(rtps.SPDPMulticastGroup)
	domainRaw := domainRawBytes(domain)
	var leaseRaw [8]byte
	binary.LittleEndian.PutUint32(leaseRaw[0:4], sec)
	binary.LittleEndian.PutUint32(leaseRaw[4:8], nanos)
	var besRaw [4]byte
	binary.LittleEndian.PutUint32(besRaw[:], Spdp2SecureParticipantMask())
	var builtinQosRaw [4]byte
	binary.LittleEndian.PutUint32(builtinQosRaw[:], 0)
	var manualRaw [4]byte
	binary.LittleEndian.PutUint32(manualRaw[:], manual)
	userdata := []byte(spdpUserDataEnclave)
	var userdataRaw []byte
	userdataRaw = binary.LittleEndian.AppendUint32(userdataRaw, uint32(len(userdata))) //nolint:gosec // G115: enclave string is a short constant
	userdataRaw = append(userdataRaw, userdata...)
	for len(userdataRaw)%4 != 0 {
		userdataRaw = append(userdataRaw, 0x00)
	}
	params := spdpAliveParams(guid, domainRaw, leaseRaw, besRaw, builtinQosRaw, manualRaw, userdataRaw, advertise, group, metaPort, defaultPort, metaMCPort, defaultMCPort)
	if domainTag != "" {
		params = append(params, rtps.Param{ID: spdp2PIDDomainTag, Value: spdp2EncodeDomainTag(domainTag)})
	}
	params = append(params,
		rtps.Param{ID: PIDIdentityToken, Value: identityRaw},
		rtps.Param{ID: PIDPermissionsToken, Value: permissionsRaw},
	)
	encoded, err := encodeSPDPParams(params)
	if err != nil {
		return nil, err
	}
	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: seq,
		Payload:        encoded,
	}
	return frameSPDP(guid.Prefix, data)
}

// MarshalDisposePacket builds an RTPS SPDP dispose packet.
//
// It carries the same RTPS header and DATA endpoints as an alive packet
// but sets the key flag and carries only the GUID plus domain tag, so a
// peer removes the participant instead of refreshing its lease. It also
// carries inline status info marking dispose plus unregister, matching
// CycloneDDS on the wire: without it Cyclone logs no/invalid payload
// and never removes the participant.
func MarshalDisposePacket(guid GUID, domain uint32, seq uint64) ([]byte, error) {
	err := validatePacketHeader(guid, domain, seq)
	if err != nil {
		return nil, err
	}

	domainRaw := domainRawBytes(domain)

	params := []rtps.Param{
		{ID: rtps.PIDParticipantGUID, Value: guidBytes(guid)},
		{ID: pidDomainID, Value: append([]byte(nil), domainRaw[:]...)},
	}

	encoded, err := encodeSPDPParams(params)
	if err != nil {
		return nil, err
	}

	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: seq,
		InlineQoS: []rtps.Parameter{
			{ID: rtps.PIDStatusInfo, Value: []byte{0x00, 0x00, 0x00, 0x03}},
		},
		Payload: encoded,
		Key:     true,
	}

	return frameSPDP(guid.Prefix, data)
}

// maxSPDPLease caps one announced lease for bounded expiry work.
//
// Leases above this bound clamp loudly instead of trusting the peer for
// hours; the default is 10s and Cyclone peers stay far below the cap.
const maxSPDPLease = 5 * time.Minute

// ParseAnnouncement parses one RTPS SPDP packet into fields.
//
// It decodes the RTPS message, scans all submessages for DATA addressed
// from builtin writer 0x000100c2, and selects the newest valid entry by
// sequence: every DATA is validated, prefix mismatches and parse failures
// drop loudly. A known invalid submessage drops the rest of the datagram
// per 8.3.4.1 rule 6, preserving entries before the invalid one. Only when
// no entry validates does it fail with ErrInvalidSPDP. Payload and inline
// QoS params merge so live Cyclone payloads with a PL_CDR encapsulation
// header parse under the same rules as inline QoS announcements. Non SPDP
// DATA is ignored. Key flagged DATA is a dispose with zero lease and no
// ports. The GUID prefix must match the RTPS header prefix; mismatches
// drop loudly instead of installing a spoofed route. Leases above
// maxSPDPLease clamp loudly to the cap. Zero sequences and dispose
// anomalies fail loudly.
// considerSPDPData validates one SPDP DATA submessage and keeps the newest.
//
// Prefix mismatches and parse failures drop loudly and the scan continues.
// Caller holds the best candidate seen so far. It never mutates header.
func considerSPDPData(header rtps.Header, data *rtps.Data, best *Announcement) *Announcement {
	ann, err := announcementFromData(data)
	if err != nil {
		n := spdpParseDrops.Add(1)
		log.Printf("dds: spdp parse data dropped %d: %v", n, err)

		return best
	}

	if GUIDPrefix(header.Prefix) != ann.GUID.Prefix {
		n := spdpParseDrops.Add(1)
		log.Printf("dds: spdp guid prefix %x mismatches header %x, dropping %d", ann.GUID.Prefix, header.Prefix, n)

		return best
	}

	if ann.Alive && ann.Lease > maxSPDPLease {
		log.Printf("dds: spdp lease %v above cap %v, clamping", ann.Lease, maxSPDPLease)
		ann.Lease = maxSPDPLease
	}

	if best == nil || ann.Sequence > best.Sequence {
		candidate := ann
		return &candidate
	}

	return best
}

func ParseAnnouncement(data []byte) (Announcement, error) {
	var out Announcement
	var drops rtps.Counter

	header, raws, err := rtps.DecodeMessage(data, &drops)
	if err != nil {
		return out, fmt.Errorf("dds: decode RTPS message: %w", errors.Join(err, ErrInvalidSPDP))
	}

	var best *Announcement

	for i := range raws {
		sub, err := rtps.DecodeSubmessage(raws[i], &drops)
		if err != nil {
			n := spdpParseDrops.Add(1)
			log.Printf("dds: spdp invalid submessage %#02x drops rest %d: %v", raws[i].ID, n, err)
			break
		}

		if sub.Data == nil {
			continue
		}

		if sub.Data.WriterID != rtps.EntitySPDPWriter {
			continue
		}

		best = considerSPDPData(header, sub.Data, best)
	}

	if best == nil {
		return out, fmt.Errorf("dds: no SPDP DATA: %w", ErrInvalidSPDP)
	}

	return *best, nil
}

// announcementAliveFields fills lease, ports, hosts, manual count, and QoS.
func announcementAliveFields(params []rtps.Param, out *Announcement) error {
	lease, err := findLeaseParam(params)
	if err != nil {
		return err
	}

	ports, hosts, err := findUnicastPortsParam(params)
	if err != nil {
		return err
	}

	manual, err := findManualCountParam(params)
	if err != nil {
		return err
	}

	qos, err := findBuiltinQosParam(params)
	if err != nil {
		return err
	}

	set, err := findSpdp2BuiltinSetParam(params)
	if err != nil {
		return err
	}

	out.Lease = lease
	out.Ports = ports
	out.Hosts = hosts
	out.ManualLivelinessCount = manual
	out.BuiltinEndpointQos = qos
	out.AvailableBuiltinEndpoints = set

	return nil
}

// announcementSecurityFields fills security info with skip logging.
func announcementSecurityFields(params []rtps.Param, out *Announcement) {
	secInfo, secErrs := ExtractSecurityInfo(params)
	for _, secErr := range secErrs {
		log.Printf("dds: spdp security param skipped: %v", secErr)
	}

	out.Security = secInfo

	err := CheckProtectedDiscovery(secInfo)
	if err != nil {
		log.Printf("dds: spdp secured peer detected, crypto unavailable: %v", err)
	}
}

// announcementIdentity extracts GUID and domain from params.
func announcementIdentity(params []rtps.Param) (GUID, uint32, error) {
	guid, err := findGUIDParam(params)
	if err != nil {
		return GUID{}, 0, err
	}

	domain, err := findDomainParam(params)
	if err != nil {
		return GUID{}, 0, err
	}

	if domain > transports.MaxDomainID {
		return GUID{}, 0, fmt.Errorf("dds: domain %d above maximum %d: %w", domain, transports.MaxDomainID, ErrInvalidSPDP)
	}

	return guid, domain, nil
}

// announcementFromData extracts fields from one SPDP DATA submessage.
//
// Key flagged DATA is a dispose carrying GUID plus domain. Other DATA
// is alive and requires a positive lease plus at least one UDPv4 unicast
// locator. Missing GUID, zero sequence, over range domain, or absent
// alive params are explicit errors. Dispose packets with a positive lease
// or unicast locators are anomalies and fail loudly instead of silently
// removing. Manual liveliness count and builtin endpoint QoS are optional
// and default to zero when absent; present values must be 4 bytes. Vendor
// PIDs such as 0x8003, 0x8007, 0x8019, and 0x800c are ignored for matching
// but preserved by the codec.
func announcementFromData(d *rtps.Data) (Announcement, error) {
	var out Announcement

	if d.SequenceNumber == 0 {
		return out, fmt.Errorf("dds: sequence must be non-zero: %w", ErrInvalidSPDP)
	}

	params, err := collectParams(d)
	if err != nil {
		return out, err
	}

	guid, domain, err := announcementIdentity(params)
	if err != nil {
		return out, err
	}

	tag, err := findSpdp2DomainTagParam(params)
	if err != nil {
		return out, err
	}

	out.GUID = guid
	out.DomainID = domain
	out.DomainTag = tag
	out.Sequence = d.SequenceNumber

	if d.Key {
		if hasLeaseParam(params) {
			return out, fmt.Errorf("dds: dispose carries lease: %w", ErrInvalidSPDP)
		}
		if hasUnicastLocatorParam(params) {
			return out, fmt.Errorf("dds: dispose carries locator: %w", ErrInvalidSPDP)
		}
		return out, nil
	}

	out.Alive = true

	err = announcementAliveFields(params, &out)
	if err != nil {
		return out, err
	}

	announcementSecurityFields(params, &out)

	return out, nil
}

// hasLeaseParam reports whether a lease param is present.
func hasLeaseParam(params []rtps.Param) bool {
	for _, p := range params {
		if p.ID == rtps.PIDParticipantLeaseDuration {
			return true
		}
	}
	return false
}

// hasUnicastLocatorParam reports whether a unicast locator param is present.
func hasUnicastLocatorParam(params []rtps.Param) bool {
	for _, p := range params {
		if p.ID == rtps.PIDMetatrafficUnicastLocator ||
			p.ID == rtps.PIDDefaultUnicastLocatorStandard ||
			p.ID == rtps.PIDDefaultUnicastLocator {
			return true
		}
	}
	return false
}

// collectParams merges inline QoS and payload params.
//
// Inline QoS entries are already decoded by the DATA path. Payload
// params decode through the PID codec so live payloads parse under the
// same rules. Unknown ids, including vendor 0x8003, 0x8007, 0x8019, and
// 0x800c, are preserved by the codec and ignored by
// the finders below. PIDPad entries from inline QoS are skipped because
// the DATA inline path does not filter PAD.
func collectParams(d *rtps.Data) ([]rtps.Param, error) {
	var out []rtps.Param

	for _, p := range d.InlineQoS {
		if p.ID == rtps.PIDPad {
			continue
		}
		out = append(out, rtps.Param(p))
	}

	if len(d.Payload) == 0 {
		return out, nil
	}

	if len(d.Payload) < spdpEncapSize {
		return nil, fmt.Errorf("dds: SPDP payload need %d have %d: %w", spdpEncapSize, len(d.Payload), ErrInvalidSPDP)
	}

	var decoded []rtps.Param

	var err error

	switch {
	case bytes.Equal(d.Payload[:4], spdpEncapPLCDRLE[:]):
		decoded, err = rtps.DecodeParams(d.Payload[spdpEncapSize:])
	case bytes.Equal(d.Payload[:4], spdpEncapPLCDRBE[:]):
		decoded, _, err = rtps.DecodeParamsEndian(d.Payload[spdpEncapSize:], false)
	default:
		err = fmt.Errorf("dds: SPDP payload encap % X is not a parameter list: %w", d.Payload[:4], ErrInvalidSPDP)
	}

	if err != nil {
		return nil, fmt.Errorf("dds: decode SPDP payload: %w", errors.Join(err, ErrInvalidSPDP))
	}

	return append(out, decoded...), nil
}

// findGUIDParam returns the participant GUID param.
//
// The value must be 16 bytes with a non zero prefix.
func findGUIDParam(params []rtps.Param) (GUID, error) {
	for _, p := range params {
		if p.ID != rtps.PIDParticipantGUID {
			continue
		}

		if len(p.Value) != 16 {
			return GUID{}, fmt.Errorf("dds: GUID length %d: %w", len(p.Value), ErrInvalidSPDP)
		}

		var guid GUID

		copy(guid.Prefix[:], p.Value[:12])
		copy(guid.EntityID[:], p.Value[12:16])

		if isZeroPrefix(guid.Prefix) {
			return GUID{}, fmt.Errorf("dds: GUID prefix is zero: %w", ErrInvalidSPDP)
		}

		return guid, nil
	}

	return GUID{}, fmt.Errorf("dds: missing GUID: %w", ErrInvalidSPDP)
}

// findDomainParam returns the domain tag param.
//
// A missing tag means domain zero. A present tag must be 4 bytes.
func findDomainParam(params []rtps.Param) (uint32, error) {
	for _, p := range params {
		if p.ID != pidDomainID {
			continue
		}

		if len(p.Value) != 4 {
			return 0, fmt.Errorf("dds: domain length %d: %w", len(p.Value), ErrInvalidSPDP)
		}

		return binary.LittleEndian.Uint32(p.Value), nil
	}

	return 0, nil
}

// findLeaseParam returns the lease duration param.
//
// The value must be 8 bytes with a positive duration.
func findLeaseParam(params []rtps.Param) (time.Duration, error) {
	for _, p := range params {
		if p.ID != rtps.PIDParticipantLeaseDuration {
			continue
		}

		if len(p.Value) != 8 {
			return 0, fmt.Errorf("dds: lease length %d: %w", len(p.Value), ErrInvalidSPDP)
		}

		sec := binary.LittleEndian.Uint32(p.Value[0:4])
		nanos := binary.LittleEndian.Uint32(p.Value[4:8])

		dur, err := partsToDuration(sec, nanos)
		if err != nil {
			return 0, err
		}

		return dur, nil
	}

	return 0, fmt.Errorf("dds: missing lease: %w", ErrInvalidSPDP)
}

// findManualCountParam returns the manual liveliness count param.
//
// A missing param means zero. A present param must be 4 bytes.
func findManualCountParam(params []rtps.Param) (uint32, error) {
	for _, p := range params {
		if p.ID != rtps.PIDParticipantManualLivelinessCount {
			continue
		}

		if len(p.Value) != 4 {
			return 0, fmt.Errorf("dds: manual count length %d: %w", len(p.Value), ErrInvalidSPDP)
		}

		return binary.LittleEndian.Uint32(p.Value), nil
	}

	return 0, nil
}

// findBuiltinQosParam returns the builtin endpoint QoS param.
//
// A missing param means zero. A present param must be 4 bytes.
func findBuiltinQosParam(params []rtps.Param) (uint32, error) {
	for _, p := range params {
		if p.ID != rtps.PIDBuiltinEndpointQos {
			continue
		}

		if len(p.Value) != 4 {
			return 0, fmt.Errorf("dds: builtin QoS length %d: %w", len(p.Value), ErrInvalidSPDP)
		}

		return binary.LittleEndian.Uint32(p.Value), nil
	}

	return 0, nil
}

// findSpdp2BuiltinSetParam returns the 0x0058 builtin endpoint mask.
//
// Missing means zero so peers announcing no endpoints create no proxies.
func findSpdp2BuiltinSetParam(params []rtps.Param) (uint32, error) {
	for _, p := range params {
		if p.ID != rtps.PIDBuiltinEndpointSet {
			continue
		}

		if len(p.Value) != 4 {
			return 0, fmt.Errorf("dds: builtin set length %d: %w", len(p.Value), ErrInvalidSPDP)
		}

		return binary.LittleEndian.Uint32(p.Value), nil
	}

	return 0, nil
}

// findSpdp2DomainTagParam returns the 0x4014 domain tag string.
//
// Missing means empty, the default tag. Present values decode as CDR
// strings in either endian, tolerating pad in the length.
func findSpdp2DomainTagParam(params []rtps.Param) (string, error) {
	for _, p := range params {
		if p.ID != spdp2PIDDomainTag {
			continue
		}

		tag, err := spdp2DecodeDomainTag(p.Value)
		if err != nil {
			return "", err
		}

		return tag, nil
	}

	return "", nil
}

// spdp2TagLength reads one length prefix in the given byte order.
func spdp2TagLength(value []byte, little bool) int {
	if little {
		return int(binary.LittleEndian.Uint32(value[:4]))
	}

	return int(binary.BigEndian.Uint32(value[:4]))
}

// spdp2TagString extracts the NUL terminated string from one raw field.
func spdp2TagString(raw []byte) (string, bool) {
	nul := -1

	for i, b := range raw {
		if b == 0x00 {
			nul = i

			break
		}
	}

	if nul < 0 {
		return "", false
	}

	out := string(raw[:nul])

	if len(out) > 256 {
		return "", false
	}

	return out, true
}

// spdp2DecodeEndian attempts one byte order for a domain tag value.
func spdp2DecodeEndian(value []byte, little bool) (string, bool) {
	length := spdp2TagLength(value, little)

	if length < 1 || length > 260 {
		return "", false
	}

	if len(value) < 4+length {
		return "", false
	}

	return spdp2TagString(value[4 : 4+length])
}

// spdp2DecodeDomainTag decodes one CDR string value for PID 0x4014.
func spdp2DecodeDomainTag(value []byte) (string, error) {
	if len(value) < 4 {
		return "", fmt.Errorf("dds: domain tag need 4 have %d: %w", len(value), ErrInvalidSPDP)
	}

	for _, little := range []bool{true, false} {
		out, ok := spdp2DecodeEndian(value, little)
		if ok {
			return out, nil
		}
	}

	return "", fmt.Errorf("dds: domain tag misses NUL terminator: %w", ErrInvalidSPDP)
}

// findUnicastPortsParam returns all UDPv4 unicast locator ports and hosts.
//
// Metatraffic, standard default, and discovery brief unicast ids all
// count in wire order. Multicast locators never count. Only the length
// is validated; any IPv4 address is accepted so off host captures
// parse, and the ports are the routes stored on the announcement.
// Hosts carry the locator IPs in the same wire order for cross host
// fanout; loopback entries stay and filter at send time.
func findUnicastPortsParam(params []rtps.Param) ([]uint16, []string, error) {
	var out []uint16
	var hosts []string

	for _, p := range params {
		if p.ID != rtps.PIDMetatrafficUnicastLocator &&
			p.ID != rtps.PIDDefaultUnicastLocatorStandard &&
			p.ID != rtps.PIDDefaultUnicastLocator {
			continue
		}

		kind, port, ip, err := rtps.DecodeLocator(p.Value)
		if err != nil {
			return nil, nil, fmt.Errorf("dds: decode locator: %w", errors.Join(err, ErrInvalidSPDP))
		}

		if kind != rtps.LocatorKindUDPv4 {
			continue
		}

		if port > math.MaxUint16 {
			return nil, nil, fmt.Errorf("dds: locator port %d above maximum: %w", port, ErrInvalidSPDP)
		}

		out = append(out, uint16(port))
		if ip != nil {
			hosts = append(hosts, ip.String())
		}
	}

	if len(out) == 0 {
		return nil, nil, fmt.Errorf("dds: missing unicast locator: %w", ErrInvalidSPDP)
	}

	return out, hosts, nil
}

// encodeSPDPMessage frames one DATA submessage in an RTPS message.
//
// The header carries our GUID prefix with the default CycloneDDS vendor
// id. The DATA body is little endian.
func encodeSPDPMessage(prefix GUIDPrefix, data rtps.Data) ([]byte, error) {
	sub, err := data.Submessage(true)
	if err != nil {
		return nil, fmt.Errorf("dds: encode SPDP DATA: %w", err)
	}

	hdr := rtps.DefaultHeader([12]byte(prefix))

	msg, err := rtps.EncodeMessage(hdr, []rtps.RawSubmessage{sub})
	if err != nil {
		return nil, fmt.Errorf("dds: encode SPDP message: %w", err)
	}

	return msg, nil
}

// patchSPDPLocatorPorts rewrites UDPv4 unicast locator ports to port.
//
// It copies packet and stores port in the 4 byte port field of every
// 24 byte UDPv4 unicast locator (metatraffic, standard default, and
// discovery brief) inside SPDP DATA payloads and inline QoS. It is the
// RTPS successor to the old 6 byte locator patch: the backend calls it
// with the live unicast port before sending a template announcement.
// Packets without SPDP DATA or without unicast locators come back
// unchanged. It never panics on short or malformed input.
//
// Byte patching is intentional: decoding through rtps.DecodeParams and
// re-encoding through rtps.EncodeParams would normalize vendor padding,
// parameter order, and unknown PIDs and drift from live captures. The
// patch therefore edits only the port field in place and leaves every
// other byte identical. TestPatchSPDPLocatorPortsRewritesUnicastLocators
// cross references the production parser by feeding patched packets back
// through ParseAnnouncement and the PID codec.
func patchSPDPLocatorPorts(packet []byte, port uint16) []byte {
	out := append([]byte(nil), packet...)

	if len(out) < rtps.HeaderSize {
		return out
	}

	if !bytes.Equal(out[:4], []byte(rtps.Magic)) {
		return out
	}

	off := rtps.HeaderSize

	count := 0

	for len(out)-off >= rtps.SubmessageHeaderSize {
		count++
		if count > spdpMaxSubmessages {
			n := spdpParseDrops.Add(1)
			log.Printf("dds: spdp patch submessages above %d, leaving %d", spdpMaxSubmessages, n)

			return out
		}
		id := out[off]
		flags := out[off+1]
		littleEndian := flags&rtps.FlagLittleEndian != 0

		var length int
		if littleEndian {
			length = int(binary.LittleEndian.Uint16(out[off+2 : off+4]))
		} else {
			length = int(binary.BigEndian.Uint16(out[off+2 : off+4]))
		}

		if len(out)-off-rtps.SubmessageHeaderSize < length {
			return out
		}

		if id == rtps.SubmessageData {
			patchSPDPDataLocators(out[off+rtps.SubmessageHeaderSize:off+rtps.SubmessageHeaderSize+length], flags, littleEndian, port)
		}

		off += rtps.SubmessageHeaderSize + length
	}

	return out
}

// patchSPDPDataLocators rewrites unicast locator ports in one DATA body.
//
// It requires the SPDP writer id, then patches inline QoS locators when
// present plus payload locators when DataPresent carries a parameter list,
// so datagrams with both sections patch both. Payload starts after the
// inline list when both are present. Bounds failures leave the body
// unchanged.
func patchSPDPDataLocators(body []byte, flags uint8, littleEndian bool, port uint16) {
	if len(body) < 20 {
		return
	}

	if !isSPDPWriterID(body[8:12]) {
		return
	}

	payloadOff := 20
	if flags&rtps.DataFlagInlineQoS != 0 {
		patchInlineLocators(body[20:], littleEndian, port)
		end := inlineListEnd(body[20:], littleEndian)
		if end < 0 {
			return
		}
		payloadOff = 20 + end
	}

	if flags&rtps.DataFlagDataPresent == 0 {
		return
	}

	if len(body) <= payloadOff {
		return
	}

	payload := body[payloadOff:]
	if len(payload) < spdpEncapSize || !isParameterListEncap(payload[:spdpEncapSize]) {
		return
	}

	patchInlineLocators(payload[spdpEncapSize:], littleEndian, port)
}

// inlineListEnd returns the length of one inline QoS list including its sentinel.
//
// It walks ID plus length headers in the given order with 4 byte padding.
// Truncation returns -1 so the caller leaves the body unchanged.
func inlineListEnd(list []byte, littleEndian bool) int {
	off := 0

	for len(list)-off >= 4 {
		id, length := readParamHeader(list[off:], littleEndian)
		if id == rtps.ParamSentinel {
			return off + 4
		}

		off += 4

		if len(list)-off < length {
			return -1
		}

		off += length

		pad := padLen(length)
		if len(list)-off < pad {
			return -1
		}

		off += pad
	}

	return -1
}

// isParameterListEncap reports whether raw is a PL_CDR parameter list
// encapsulation header in either byte order.
func isParameterListEncap(raw []byte) bool {
	if len(raw) < spdpEncapSize {
		return false
	}

	return bytes.Equal(raw, spdpEncapPLCDRLE[:]) || bytes.Equal(raw, spdpEncapPLCDRBE[:])
}

// isSPDPWriterID reports whether id is the builtin SPDP writer entity.
func isSPDPWriterID(id []byte) bool {
	return slices.Equal(id, rtps.EntitySPDPWriter[:])
}

// isUnicastLocatorID reports whether id names a UDPv4 unicast locator.
func isUnicastLocatorID(id uint16) bool {
	return id == rtps.PIDMetatrafficUnicastLocator ||
		id == rtps.PIDDefaultUnicastLocatorStandard ||
		id == rtps.PIDDefaultUnicastLocator
}

// patchInlineLocators walks one inline QoS list and rewrites locator ports.
func patchInlineLocators(list []byte, littleEndian bool, port uint16) {
	off := 0

	for len(list)-off >= 4 {
		id, length := readParamHeader(list[off:], littleEndian)
		if id == rtps.ParamSentinel {
			return
		}

		off += 4

		if len(list)-off < length {
			return
		}

		if length == 24 && isUnicastLocatorID(id) {
			patchLocatorPort(list[off:off+length], littleEndian, port)
		}

		off += length

		pad := padLen(length)
		if len(list)-off < pad {
			return
		}

		off += pad
	}
}

// readParamHeader loads one parameter id and length in the given order.
func readParamHeader(buf []byte, littleEndian bool) (uint16, int) {
	if littleEndian {
		return binary.LittleEndian.Uint16(buf[0:2]), int(binary.LittleEndian.Uint16(buf[2:4]))
	}

	return binary.BigEndian.Uint16(buf[0:2]), int(binary.BigEndian.Uint16(buf[2:4]))
}

// padLen returns the zero pad needed to reach a 4 byte boundary.
func padLen(n int) int {
	return (4 - n%4) % 4
}

// patchLocatorPort stores port in one 24 byte locator value when it
// names UDPv4. Other kinds are left unchanged.
func patchLocatorPort(value []byte, littleEndian bool, port uint16) {
	var kind int32
	if littleEndian {
		kind = int32(binary.LittleEndian.Uint32(value[0:4])) //nolint:gosec // G115: intentional two's complement reinterpret from the wire.
	} else {
		kind = int32(binary.BigEndian.Uint32(value[0:4])) //nolint:gosec // G115: intentional two's complement reinterpret from the wire.
	}

	if kind != rtps.LocatorKindUDPv4 {
		return
	}

	if littleEndian {
		binary.LittleEndian.PutUint32(value[4:8], uint32(port))
	} else {
		binary.BigEndian.PutUint32(value[4:8], uint32(port))
	}
}

// durationToParts splits a lease into seconds and nanos.
func durationToParts(d time.Duration) (uint32, uint32, error) {
	if d <= 0 {
		return 0, 0, fmt.Errorf("dds: lease %v must be positive: %w", d, ErrInvalidLease)
	}

	sec := int64(d / time.Second)
	nanos := int64(d % time.Second)

	if sec > int64(math.MaxUint32) {
		return 0, 0, fmt.Errorf("dds: lease %v overflows uint32: %w", d, ErrInvalidLease)
	}

	return uint32(sec), uint32(nanos), nil //nolint:gosec // sec checked above, nanos sub second.
}

// partsToDuration joins seconds and nanos into a lease.
func partsToDuration(sec, nanos uint32) (time.Duration, error) {
	if nanos >= uint32(time.Second) {
		return 0, fmt.Errorf("dds: nanos %d out of range: %w", nanos, ErrInvalidSPDP)
	}

	if sec == 0 && nanos == 0 {
		return 0, fmt.Errorf("dds: lease must be positive: %w", ErrInvalidSPDP)
	}

	return time.Duration(sec)*time.Second + time.Duration(nanos), nil
}

// isZeroPrefix reports whether a GUID prefix is all zero.
func isZeroPrefix(prefix GUIDPrefix) bool {
	for _, b := range prefix {
		if b != 0 {
			return false
		}
	}

	return true
}

// guidBytes flattens a GUID into 16 bytes.
func guidBytes(guid GUID) []byte {
	out := make([]byte, 0, 16)
	out = append(out, guid.Prefix[:]...)
	out = append(out, guid.EntityID[:]...)

	return out
}

// RemoteTable tracks remote participants by GUID prefix.
//
// It is the partition local index for SPDP: entries expire by lease,
// sequence orders updates, generation counts mutations, and onChange
// observes each generation. Tombstones retain the high-water sequence
// for removed prefixes until lease sweep expires them, so a stale alive
// at or below the tombstone never reinstalls. It is safe for concurrent use.
type RemoteTable struct {
	mu         sync.Mutex
	entries    map[GUIDPrefix]ParticipantInfo
	tombstones map[GUIDPrefix]spdpTombstone
	generation uint64
	onChange   func(uint64)
	// localDomain is the gate domain, valid when localSet is true.
	localDomain uint32
	// localTag is the gate domain tag, valid when localSet is true.
	localTag string
	// localSet enables the 8.5.5.1 domain and tag gate.
	localSet bool
	// sec holds the Phase 1 decision suite, nil for tolerance mode.
	sec *ddssecurity.Suite
}

// spdpTombstone retains the removal high-water for one prefix.
//
// Seq is the last accepted dispose, remove, or sweep sequence. ExpiresAt
// bounds the entry lifetime so the map stays bounded: Sweep deletes the
// tombstone once now reaches ExpiresAt.
type spdpTombstone struct {
	// seq is the high-water sequence for the removed prefix.
	seq uint64
	// expiresAt is creation plus maxSPDPLease for every path so the
	// distrust window stays predictable; Sweep deletes the tombstone
	// once now reaches it.
	expiresAt time.Time
}

// NewRemoteTable creates an empty remote table.
//
// onChange may be nil. It is called with the new generation after
// each mutation, outside the table lock, so it must not block.
func NewRemoteTable(onChange func(uint64)) *RemoteTable {
	return &RemoteTable{
		entries:    make(map[GUIDPrefix]ParticipantInfo),
		tombstones: make(map[GUIDPrefix]spdpTombstone),
		onChange:   onChange,
	}
}

// SetOnChange replaces the change hook.
func (t *RemoteTable) SetOnChange(fn func(uint64)) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.onChange = fn
}

// SetSecuritySuite installs the Phase 1 decision suite.
//
// A nil suite keeps tolerance mode: alive peers install exactly as
// today. A non-nil enabled suite refuses every alive peer before it
// enters the participant graph. It is safe for concurrent use.
func (t *RemoteTable) SetSecuritySuite(s *ddssecurity.Suite) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.sec = s
}

// SetSpdp2LocalDomain pins the local domain and tag gate per 8.5.5.1.
//
// Once set, the table never holds a prefix whose domain or tag differs;
// mismatched announces reject loudly with errSpdp2DomainMismatch.
// Empty tag selects the default domain tag. It is safe for concurrent use.
func (t *RemoteTable) SetSpdp2LocalDomain(domain uint32, tag string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.localDomain = domain
	t.localTag = tag
	t.localSet = true
}

// Spdp2LocalDomain returns the gate domain, tag, and whether set.
func (t *RemoteTable) Spdp2LocalDomain() (uint32, string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.localDomain, t.localTag, t.localSet
}

// spdp2CheckDomainLocked rejects foreign domains or tags. Caller holds the lock.
func (t *RemoteTable) spdp2CheckDomainLocked(ann Announcement) error {
	if !t.localSet {
		return nil
	}

	if ann.DomainID != t.localDomain || ann.DomainTag != t.localTag {
		return fmt.Errorf("dds: spdp domain %d tag %q mismatches local %d %q: %w", ann.DomainID, ann.DomainTag, t.localDomain, t.localTag, errSpdp2DomainMismatch)
	}

	return nil
}

// Spdp2BuiltinIntersection returns the proxy endpoint set per 8.5.5.1.
//
// Only endpoints in both local and remote create proxies.
func Spdp2BuiltinIntersection(local, remote uint32) uint32 {
	return local & remote
}

// Spdp2IsBestEffortParticipantMessage reports best-effort reader QoS.
//
// Bit 0 of builtinEndpointQos selects best-effort per 9.3.2. Absent or
// zero QoS means reliable per 8.4.13.3.
func Spdp2IsBestEffortParticipantMessage(qos uint32) bool {
	return qos&spdp2BestEffortParticipantMessageBit != 0
}

// Spdp2SecureParticipantMask returns the honest secure announce mask.
//
// It is always valid-only and never mirrors remote masks, so attacker
// influenced peer state cannot shape our own announcement.
func Spdp2SecureParticipantMask() uint32 {
	return 1 << 31
}

// spdp2CertAlgoForCert labels the identity algorithm from the key type.
func spdp2CertAlgoForCert(cert *x509.Certificate) string {
	if cert == nil {
		return "unknown"
	}

	switch pub := cert.PublicKey.(type) {
	case *ecdsa.PublicKey:
		if pub.Curve == elliptic.P384() {
			return "EC-prime384v1"
		}

		return "EC-prime256v1"
	case *rsa.PublicKey:
		return "RSA-2048"
	default:
		return "unknown"
	}
}

// Spdp2CertAlgoForCert labels the identity algorithm from the key type.
//
// It reports EC-prime256v1, EC-prime384v1, RSA-2048, or unknown from the
// actual public key, never a hardcoded constant. It is the identity label
// for logging and diagnostics only; secure announce properties must use
// Spdp2NegotiatedSecureAlgos instead, which sources the token names from
// the governance allow-list.
func Spdp2CertAlgoForCert(cert *x509.Certificate) string {
	return spdp2CertAlgoForCert(cert)
}

// Spdp2NegotiatedSecureAlgos selects the secure announce algorithms.
//
// It intersects the local certificate capability with the governance
// allow-list for the domain and returns the token wire names for
// c.dsign_algo and c.kagree_algo. Only the P-256 suite maps to token
// names today; RSA, P-384, and unknown keys fail closed because no token
// name exists for them. An empty allow-list or a missing overlap fails
// closed with ErrSpdp2UnnegotiatedAlgo. Peer-supplied values never enter:
// the inputs are only the local allow-list and the local certificate,
// so attacker masks or labels cannot shape our own announcement.
// Announce builders must use this instead of Spdp2CertAlgoForCert.
func Spdp2NegotiatedSecureAlgos(allowed []string, cert *x509.Certificate) (string, string, error) {
	if cert == nil {
		return "", "", fmt.Errorf("dds: secure announce without identity: %w", ErrSpdp2UnnegotiatedAlgo)
	}

	var dsignGovernance, kagreeGovernance, dsignToken, kagreeToken string

	switch pub := cert.PublicKey.(type) {
	case *ecdsa.PublicKey:
		if pub.Curve != elliptic.P256() {
			return "", "", fmt.Errorf("dds: secure announce curve unsupported: %w", ErrSpdp2UnnegotiatedAlgo)
		}

		dsignGovernance = "ECDSA+P256+SHA256"
		kagreeGovernance = "ECDHE-CEUM+P256"
		dsignToken = tokens.DSignAlgoECDSA
		kagreeToken = tokens.KAgreeECDH
	default:
		return "", "", fmt.Errorf("dds: secure announce key type %T unsupported: %w", cert.PublicKey, ErrSpdp2UnnegotiatedAlgo)
	}

	if !slices.Contains(allowed, dsignGovernance) || !slices.Contains(allowed, kagreeGovernance) {
		return "", "", fmt.Errorf("dds: secure announce algorithms %q and %q miss governance allow-list: %w", dsignGovernance, kagreeGovernance, ErrSpdp2UnnegotiatedAlgo)
	}

	return dsignToken, kagreeToken, nil
}

// Generation returns the current generation counter.
func (t *RemoteTable) Generation() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.generation
}

// Get returns the participant for a prefix.
func (t *RemoteTable) Get(prefix GUIDPrefix) (ParticipantInfo, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	info, ok := t.entries[prefix]

	return info, ok
}

// List returns a sorted copy of all participants.
func (t *RemoteTable) List() []ParticipantInfo {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]ParticipantInfo, 0, len(t.entries))
	for _, info := range t.entries {
		out = append(out, info)
	}

	slices.SortFunc(out, compareInfo)

	return out
}

// Len returns the number of tracked participants.
func (t *RemoteTable) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	return len(t.entries)
}

// notifyChange invokes the hook outside the lock when changed.
func notifyChange(changed bool, gen uint64, hook func(uint64)) {
	if !changed {
		return
	}

	if hook == nil {
		return
	}

	hook(gen)
}

// TombstoneSeq returns the removal high-water for one prefix.
//
// The second result reports whether a tombstone exists. The backend route
// gate uses it so a stale alive at or below the tombstone never reinstalls
// routes the table would reject.
func (t *RemoteTable) TombstoneSeq(prefix GUIDPrefix) (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	tomb, ok := t.tombstones[prefix]
	if !ok {
		return 0, false
	}

	return tomb.seq, true
}

// handleDisposeLocked removes one prefix. Caller holds the lock.
//
// A dispose at the stored sequence is a routine multicast duplicate and
// returns silently without touching generation: senders repeat packets
// on lossy groups. Only a strictly older sequence fails loudly with
// ErrStaleSPDP, since rollbacks never occur legitimately. Only a newer
// sequence deletes the entry and its routes. The delete retains a
// tombstone with the dispose sequence until now plus maxSPDPLease so the
// window stays predictable and a stale alive cannot reinstall; Sweep
// drops the tombstone after expiry so the map stays bounded. The caller
// supplies now, matching Sweep and the alive path, so fake clocks stay
// deterministic. A missing entry consults the tombstone with the same
// duplicate versus stale discipline and never creates a new tombstone,
// so spoofed disposes cannot grow the map.
func (t *RemoteTable) handleDisposeLocked(ann Announcement, now time.Time) (bool, uint64, func(uint64), error) {
	err := t.spdp2CheckDomainLocked(ann)
	if err != nil {
		return false, 0, nil, err
	}

	existing, ok := t.entries[ann.GUID.Prefix]
	if !ok {
		tomb, seen := t.tombstones[ann.GUID.Prefix]
		if !seen {
			return false, 0, nil, nil
		}
		if ann.Sequence == tomb.seq {
			return false, 0, nil, nil
		}
		if ann.Sequence < tomb.seq {
			n := spdpParseDrops.Add(1)
			log.Printf("dds: spdp stale dispose seq=%d last=%d dropping %d", ann.Sequence, tomb.seq, n)

			return false, 0, nil, fmt.Errorf("dds: spdp stale dispose seq=%d last=%d: %w", ann.Sequence, tomb.seq, ErrStaleSPDP)
		}
		tomb.seq = ann.Sequence
		tomb.expiresAt = now.Add(maxSPDPLease)
		t.tombstones[ann.GUID.Prefix] = tomb

		return false, 0, nil, nil
	}

	if ann.Sequence == existing.LastSeq {
		return false, 0, nil, nil
	}

	if ann.Sequence < existing.LastSeq {
		n := spdpParseDrops.Add(1)
		log.Printf("dds: spdp stale dispose seq=%d last=%d dropping %d", ann.Sequence, existing.LastSeq, n)

		return false, 0, nil, fmt.Errorf("dds: spdp stale dispose seq=%d last=%d: %w", ann.Sequence, existing.LastSeq, ErrStaleSPDP)
	}

	delete(t.entries, ann.GUID.Prefix)
	t.tombstones[ann.GUID.Prefix] = spdpTombstone{seq: ann.Sequence, expiresAt: now.Add(maxSPDPLease)}
	t.generation++

	return true, t.generation, t.onChange, nil
}

// handleAliveLocked adds or refreshes one entry. Caller holds the lock.
//
// An alive at the stored sequence is a routine periodic re-announcement
// (same sample resent on lossy groups or on the announce tick) and refreshes
// the lease without touching generation or the entry: liveness is renewed,
// content is unchanged. Only a strictly older sequence fails loudly with
// ErrStaleSPDP. Only a newer sequence installs or refreshes the entry plus
// generation. A missing entry consults the tombstone: an alive at the
// tombstone sequence is a duplicate and returns silently, a strictly older
// alive fails loudly, and only a newer alive deletes the tombstone and
// installs.
// spdpManualExpiry returns the manual lease deadline for one announcement.
//
// A zero manual count means the peer never asserted manual liveliness,
// so no manual lease engages and only the automatic lease expires the
// entry. A positive count engages the manual lease at the announced
// expiry: only a higher count renews it, so silence past the lease
// expires the peer even while periodic automatic re-announces continue.
func spdpManualExpiry(ann Announcement, expiry time.Time) time.Time {
	if ann.ManualLivelinessCount == 0 {
		return time.Time{}
	}

	return expiry
}

// installAliveLocked installs one alive announcement for a missing prefix.
//
// A tombstone sequence match is a duplicate and returns silently; a
// strictly older sequence fails loudly; only a newer sequence deletes
// the tombstone and installs. Caller holds the lock.
func (t *RemoteTable) installAliveLocked(ann Announcement, expiry time.Time) (bool, uint64, func(uint64), error) {
	err := t.spdp2CheckDomainLocked(ann)
	if err != nil {
		return false, 0, nil, err
	}

	err = t.spdpCheckSecurityLocked(ann)
	if err != nil {
		return false, 0, nil, err
	}

	if tomb, seen := t.tombstones[ann.GUID.Prefix]; seen {
		if ann.Sequence == tomb.seq {
			return false, 0, nil, nil
		}
		if ann.Sequence < tomb.seq {
			n := spdpParseDrops.Add(1)
			log.Printf("dds: spdp stale alive seq=%d last=%d dropping %d", ann.Sequence, tomb.seq, n)

			return false, 0, nil, fmt.Errorf("dds: spdp stale alive seq=%d last=%d: %w", ann.Sequence, tomb.seq, ErrStaleSPDP)
		}
		delete(t.tombstones, ann.GUID.Prefix)
	}
	t.entries[ann.GUID.Prefix] = ParticipantInfo{
		GUID:                      ann.GUID,
		LeaseExpiry:               expiry,
		LastSeq:                   ann.Sequence,
		DomainID:                  ann.DomainID,
		DomainTag:                 ann.DomainTag,
		AvailableBuiltinEndpoints: ann.AvailableBuiltinEndpoints,
		BuiltinEndpointQos:        ann.BuiltinEndpointQos,
		ManualLivelinessCount:     ann.ManualLivelinessCount,
		ManualLeaseExpiry:         spdpManualExpiry(ann, expiry),
		Security:                  ann.Security,
	}
	t.generation++

	return true, t.generation, t.onChange, nil
}

// spdpCheckSecurityLocked enforces the Phase 1 decision suite on install
// and refresh.
//
// A nil or disabled suite keeps tolerance mode and allows exactly as
// today, preserving table behavior. An enabled suite refuses peers that
// fail CheckRemoteParticipant, fail-closed, so the discovery,
// liveliness, and RTPS protection kinds decided by governance per 9.4.2.4
// are enforced at the participant graph instead of only decided. Install
// and every refresh (newer sequence and same-sequence lease renewal)
// re-check; dispose needs no check. Caller holds the lock.
func (t *RemoteTable) spdpCheckSecurityLocked(ann Announcement) error {
	suite := t.sec
	if suite == nil || !suite.Enabled() {
		return nil
	}

	err := suite.CheckRemoteParticipant(spdpPeerDescriptor(ann))
	if err != nil {
		return fmt.Errorf("dds: spdp refuse peer: %w", err)
	}

	return nil
}

// spdpPeerDescriptor builds the Phase 1 decision input for one announcement.
//
// Identity is the hex participant prefix. Class follows the parsed
// security posture: no markers means unsecured, hard-require properties
// mean crypto, and anything else means compatible-unknown. Properties,
// domain, and tag pass through untouched; the suite decides.
func spdpPeerDescriptor(ann Announcement) ddssecurity.PeerDescriptor {
	var class ddssecurity.PeerClass

	switch ann.Security.Classification {
	case SecurityUnsecured:
		class = ddssecurity.PeerUnsecured
	case SecurityRequiresCrypto:
		class = ddssecurity.PeerRequiresCrypto
	case SecurityCompatibleUnknown:
		class = ddssecurity.PeerCompatible
	default:
		class = ddssecurity.PeerUnsecured
	}

	return ddssecurity.PeerDescriptor{
		Identity:   hex.EncodeToString(ann.GUID.Prefix[:]),
		Class:      class,
		Properties: ann.Security.Properties,
		DomainID:   ann.DomainID,
		Tag:        ann.DomainTag,
	}
}

// evictHostileLocked removes one entry that failed the refresh security
// re-check. Caller holds the lock.
//
// The entry deletes at once and leaves a tombstone at the hostile
// sequence until now plus maxSPDPLease, so identical re-announces stay
// quiet duplicates instead of reinstalling the refused peer. Generation
// bumps once and the change hook fires through the returned values even
// though a refusal error also returns: the caller notifies on changed
// before surfacing the error. Dispose needs no check and never calls
// this path.
func (t *RemoteTable) evictHostileLocked(ann Announcement, expiry time.Time, secErr error) (bool, uint64, func(uint64), error) {
	now := expiry.Add(-ann.Lease)
	delete(t.entries, ann.GUID.Prefix)
	t.tombstones[ann.GUID.Prefix] = spdpTombstone{seq: ann.Sequence, expiresAt: now.Add(maxSPDPLease)}
	t.generation++

	return true, t.generation, t.onChange, fmt.Errorf("dds: spdp refuse hostile refresh: %w", secErr)
}

func (t *RemoteTable) handleAliveLocked(ann Announcement, expiry time.Time) (bool, uint64, func(uint64), error) {
	err := t.spdp2CheckDomainLocked(ann)
	if err != nil {
		return false, 0, nil, err
	}

	existing, ok := t.entries[ann.GUID.Prefix]
	if !ok {
		return t.installAliveLocked(ann, expiry)
	}

	if ann.Sequence == existing.LastSeq {
		// Same-sequence re-announcements re-check security: a peer
		// admitted before the suite enabled must not renew its lease
		// once its posture refuses. A hostile refresh evicts at once
		// instead of renewing.
		if err := t.spdpCheckSecurityLocked(ann); err != nil {
			return t.evictHostileLocked(ann, expiry, err)
		}

		existing.LeaseExpiry = expiry
		if ann.ManualLivelinessCount > existing.ManualLivelinessCount {
			existing.ManualLivelinessCount = ann.ManualLivelinessCount
			existing.ManualLeaseExpiry = spdpManualExpiry(ann, expiry)
		}
		existing.AvailableBuiltinEndpoints = ann.AvailableBuiltinEndpoints
		existing.BuiltinEndpointQos = ann.BuiltinEndpointQos
		existing.Security = ann.Security
		t.entries[ann.GUID.Prefix] = existing
		return false, 0, nil, nil
	}

	if ann.Sequence < existing.LastSeq {
		n := spdpParseDrops.Add(1)
		log.Printf("dds: spdp stale alive seq=%d last=%d dropping %d", ann.Sequence, existing.LastSeq, n)

		return false, 0, nil, fmt.Errorf("dds: spdp stale alive seq=%d last=%d: %w", ann.Sequence, existing.LastSeq, ErrStaleSPDP)
	}

	// Newer-sequence updates re-check security for the same reason: the
	// announced posture may have turned hostile since install. A hostile
	// update evicts instead of installing.
	if err := t.spdpCheckSecurityLocked(ann); err != nil {
		return t.evictHostileLocked(ann, expiry, err)
	}

	existing.GUID = ann.GUID
	existing.LeaseExpiry = expiry
	existing.LastSeq = ann.Sequence
	existing.DomainID = ann.DomainID
	existing.DomainTag = ann.DomainTag
	existing.AvailableBuiltinEndpoints = ann.AvailableBuiltinEndpoints
	existing.BuiltinEndpointQos = ann.BuiltinEndpointQos
	existing.ManualLivelinessCount = ann.ManualLivelinessCount
	existing.ManualLeaseExpiry = spdpManualExpiry(ann, expiry)
	existing.Security = ann.Security
	t.entries[ann.GUID.Prefix] = existing
	t.generation++

	return true, t.generation, t.onChange, nil
}

// HandlePacket parses one SPDP packet and updates the table.
//
// Alive packets add or refresh the entry with expiry now plus lease.
// Same-sequence alive re-announcements refresh the lease quietly without
// bumping generation: senders repeat the same sample on the announce tick,
// so a duplicate is liveness, not an error. Same-sequence with a higher
// manual count refreshes both expiries and stores the high-water count
// without bumping generation. Same-sequence without a count advance
// refreshes only the automatic lease, so manual silence still expires
// the entry through Sweep. Strictly older sequences fail loudly with
// ErrStaleSPDP without bumping generation. Dispose packets remove the
// entry only when their sequence is newer than the stored LastSeq; older
// disposes fail loudly like stale alive packets, while same-sequence
// disposes are routine duplicates. A missing entry for a dispose succeeds
// quietly with no generation bump and no tombstone, subject to the
// tombstone discipline which rejects at-or-below high-water. When the
// local domain gate is set via SetSpdp2LocalDomain, foreign domain or tag
// announces reject loudly with errSpdp2DomainMismatch and never install.
// Unknown params are ignored. Generation bumps once per effective change.
func (t *RemoteTable) HandlePacket(data []byte, now time.Time) error {
	if now.IsZero() {
		return fmt.Errorf("dds: handle zero time: %w", ErrInvalidTime)
	}

	ann, err := ParseAnnouncement(data)
	if err != nil {
		return fmt.Errorf("dds: handle packet: %w", err)
	}

	if !ann.Alive {
		t.mu.Lock()
		changed, gen, hook, herr := t.handleDisposeLocked(ann, now)
		t.mu.Unlock()
		if herr != nil {
			return fmt.Errorf("dds: handle packet: %w", herr)
		}
		notifyChange(changed, gen, hook)

		return nil
	}

	expiry := now.Add(ann.Lease)

	t.mu.Lock()
	changed, gen, hook, herr := t.handleAliveLocked(ann, expiry)
	t.mu.Unlock()
	// A hostile refresh evicts with both changed and an error: fire the
	// hook before surfacing the refusal so the removal is visible.
	// Every other error path returns unchanged and notifies as a no-op.
	if changed {
		notifyChange(changed, gen, hook)
	}
	if herr != nil {
		return fmt.Errorf("dds: handle packet: %w", herr)
	}

	return nil
}

// checkSPDPInfoDstUnicast enforces INFO_DST on unicast ingress.
//
// It shares the walk with the SEDP helper and the backend demux through
// rtps.ClassifyUnicastInfoDst so all three agree: absent, zero-length, or
// all-zero INFO_DST processes normally, mismatch drops loudly with
// ErrInvalidSPDP, and short non-empty or undecodable INFO_DST bodies drop
// loudly fail-closed with ErrInvalidSPDP. Multicast ingress must never
// call this helper: group sockets carry every sender, so per-sender
// identity has no meaning there and enforcement would drop valid peers.
func checkSPDPInfoDstUnicast(data []byte, local GUIDPrefix) error {
	var want [12]byte
	copy(want[:], local[:])

	switch rtps.ClassifyUnicastInfoDst(data, want) {
	case rtps.InfoDstAbsent:
		return nil
	case rtps.InfoDstMismatch:
		n := spdpParseDrops.Add(1)
		log.Printf("dds: spdp unicast INFO_DST mismatch, dropping %d", n)

		return fmt.Errorf("dds: spdp INFO_DST mismatch: %w", ErrInvalidSPDP)
	case rtps.InfoDstMalformed:
		n := spdpParseDrops.Add(1)
		log.Printf("dds: spdp unicast INFO_DST malformed, dropping %d", n)

		return fmt.Errorf("dds: spdp INFO_DST malformed body: %w", ErrInvalidSPDP)
	default:
		return nil
	}
}

// HandlePacketUnicast enforces INFO_DST for unicast ingress then handles the packet.
//
// It drops loudly when INFO_DST addresses another participant, then
// delegates to HandlePacket for prefix binding plus monotonic sequence
// gating. Multicast ingress must call HandlePacket directly without this
// wrapper because group sockets carry every sender.
func (t *RemoteTable) HandlePacketUnicast(data []byte, now time.Time, local GUIDPrefix) error {
	err := checkSPDPInfoDstUnicast(data, local)
	if err != nil {
		return err
	}

	err = t.HandlePacket(data, now)
	if err != nil {
		return err
	}

	return nil
}

// Remove deletes one prefix and bumps generation when present.
//
// The delete retains a tombstone with the removed LastSeq until the
// stored lease expiry plus maxSPDPLease so a stale alive cannot
// reinstall without wall clock use; Sweep drops the tombstone after
// expiry so the map stays bounded. A missing prefix returns false
// without touching the tombstone. Prefer RemoveSpdp2At with caller now.
func (t *RemoteTable) Remove(prefix GUIDPrefix) bool {
	t.mu.Lock()

	existing, ok := t.entries[prefix]
	if !ok {
		t.mu.Unlock()

		return false
	}

	delete(t.entries, prefix)
	t.tombstones[prefix] = spdpTombstone{seq: existing.LastSeq, expiresAt: existing.LeaseExpiry.Add(maxSPDPLease)}
	t.generation++

	gen := t.generation
	hook := t.onChange
	t.mu.Unlock()

	notifyChange(true, gen, hook)

	return true
}

// RemoveSpdp2At deletes one prefix using caller supplied now.
//
// It retains a tombstone until now plus maxSPDPLease so the distrust
// window stays predictable with fake clocks. Missing prefixes return
// false without creating tombstones.
func (t *RemoteTable) RemoveSpdp2At(prefix GUIDPrefix, now time.Time) bool {
	t.mu.Lock()

	existing, ok := t.entries[prefix]
	if !ok {
		t.mu.Unlock()

		return false
	}

	delete(t.entries, prefix)
	t.tombstones[prefix] = spdpTombstone{seq: existing.LastSeq, expiresAt: now.Add(maxSPDPLease)}
	t.generation++

	gen := t.generation
	hook := t.onChange
	t.mu.Unlock()

	notifyChange(true, gen, hook)

	return true
}

// spdpManualDue reports whether the manual liveliness lease elapsed.
//
// A zero ManualLeaseExpiry means the peer never engaged manual
// liveliness, so only the automatic lease expires the entry. Otherwise
// the entry is due once now reaches the manual deadline, even while the
// automatic lease stays fresh from periodic re-announces.
func spdpManualDue(info ParticipantInfo, now time.Time) bool {
	if info.ManualLeaseExpiry.IsZero() {
		return false
	}

	return !now.Before(info.ManualLeaseExpiry)
}

// Sweep removes expired entries and bumps generation once when any expire.
//
// An entry expires when now is at or past LeaseExpiry, or when its manual
// liveliness lease elapsed while engaged: spdpManualDue stays false for
// peers that never asserted manual liveliness, so their periodic
// re-announces keep renewing only the automatic lease. Each expired entry
// retains a tombstone until now plus maxSPDPLease so a stale alive cannot
// reinstall after sweep. Tombstones at or past expiry are dropped without
// a generation bump so the map stays bounded. It returns the number of
// removed entries.
//
// Sweep is table-only and self-contained: it takes only the table lock,
// touches only table state, and is safe to call standalone or under the
// backend joint lock without ordering concerns. Joint paths that also
// touch backend store or route state live in backend-owned code and own
// their lock order (backend mu then table mu); this helper never takes
// the backend lock itself.
func (t *RemoteTable) Sweep(now time.Time) int {
	t.mu.Lock()

	removed := 0

	for prefix, info := range t.entries {
		if now.Before(info.LeaseExpiry) && !spdpManualDue(info, now) {
			continue
		}

		delete(t.entries, prefix)
		t.tombstones[prefix] = spdpTombstone{seq: info.LastSeq, expiresAt: now.Add(maxSPDPLease)}
		removed++
	}

	for prefix, tomb := range t.tombstones {
		if now.Before(tomb.expiresAt) {
			continue
		}

		delete(t.tombstones, prefix)
	}

	if removed == 0 {
		t.mu.Unlock()

		return 0
	}

	t.generation++

	gen := t.generation
	hook := t.onChange
	t.mu.Unlock()

	notifyChange(removed > 0, gen, hook)

	return removed
}

// ListenLoop receives SPDP packets, feeds HandlePacket, and sweeps expired entries until ctx ends.
//
// It blocks in Receiver.Receive with a child timeout capped at the sweep
// interval, feeds each payload to HandlePacket with the current time, and
// sweeps entries whose lease expired when the interval elapses. Malformed
// packets and transient receive errors are ignored so discovery stays alive;
// only ctx termination stops the loop. It returns ctx.Err wrapped so
// errors.Is reports context.Canceled or context.DeadlineExceeded. A nil
// receiver or a non positive sweep interval is an explicit error. It spawns
// no goroutine and holds no timer beyond the child timeout, so it is
// stop-clean. It is safe for concurrent use with other RemoteTable methods.
//
// The loop takes one time.Now per iteration and reuses that instant for
// both HandlePacket and the sweep check, so expiry and sweep share one
// clock reading and cannot skew within an iteration.
func (t *RemoteTable) ListenLoop(ctx context.Context, receiver Receiver, sweep time.Duration) error {
	if ctx == nil {
		return fmt.Errorf("dds: listen loop nil context: %w", ErrNilContext)
	}

	if receiver == nil {
		return fmt.Errorf("dds: listen loop nil receiver: %w", ErrNilReceiver)
	}

	if sweep <= 0 {
		return fmt.Errorf("dds: listen loop sweep %v must be positive: %w", sweep, ErrInvalidPeriod)
	}

	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: listen loop context: %w", err)
	}

	lastSweep := time.Now()

	for {
		err = ctx.Err()
		if err != nil {
			return fmt.Errorf("dds: listen loop stopped: %w", err)
		}

		child, cancel := context.WithTimeout(ctx, sweep)
		data, err := receiver.Receive(child)
		cancel()

		now := time.Now()

		if err == nil {
			// Malformed and stale packets must not kill discovery; errors
			// count loudly here and the sweep check below still runs.
			herr := t.HandlePacket(data, now)
			if herr != nil {
				n := spdpParseDrops.Add(1)
				log.Printf("dds: spdp listen dropped %d: %v", n, herr)
			}
		} else if ctx.Err() != nil {
			return fmt.Errorf("dds: listen loop stopped: %w", ctx.Err())
		}

		if now.Sub(lastSweep) >= sweep {
			t.Sweep(now)
			lastSweep = now
		}
	}
}

// compareInfo orders participant info by GUID prefix then entity.
func compareInfo(a, b ParticipantInfo) int {
	if c := bytes.Compare(a.GUID.Prefix[:], b.GUID.Prefix[:]); c != 0 {
		return c
	}

	return bytes.Compare(a.GUID.EntityID[:], b.GUID.EntityID[:])
}

// INTEGRATION state for SPDP discovery hardening.
//
// Done in this file: endpoint identity binds to the RTPS header prefix in
// ParseAnnouncement with a loud drop on mismatch, so spoofed routes never
// install; monotonic sequence gates both alive and dispose paths with a
// loud ErrStaleSPDP reject that leaves generation unchanged; dispose
// deletes only on a newer sequence, and a missing entry succeeds quietly
// because there is no route to delete; the patch walk caps at
// spdpMaxSubmessages (4096, matching rtps) with a loud leave-unchanged
// drop; unicast INFO_DST enforcement lives in checkSPDPInfoDstUnicast
// plus HandlePacketUnicast, which unicast ingress must call; secure
// announces negotiate from the governance allow-list plus the local
// certificate via Spdp2NegotiatedSecureAlgos and emit the honest secure
// mask plus identity and permissions tokens only when security material
// is armed, with the tolerance baseline staying byte-identical; table
// Sweep is table-only and self-locked, safe standalone or under joint
// locks.
//
// Done elsewhere: backend shouldDropUnicastForInfoDst plus
// storeAnnouncementRoutes enforce the same unicast INFO_DST and
// per-prefix route sequence checks on the live socket path, including
// lease expiry sweep and port filtering; rtps DecodeMessage caps
// submessages at 4096 before dispatch.
//
// Explicitly open and unenforced: multicast ingress never calls the
// INFO_DST helper because group sockets carry every sender and have no
// per-sender identity; standalone RemoteTable.HandlePacket stays
// transport agnostic and relies on the unicast wrapper or the backend
// demux for INFO_DST. No rejected scaffolding remains: the legacy custom
// locator path is gone and all framing flows through rtps.
//
// Lock order for joint paths is always b.mu then t.mu: backend
// storeAnnouncementWithTableAdvancewir10 takes b.mu before remotes.mu while
// standalone RemoteTable methods take only t.mu, so no path inverts the
// order and every table path in this file stays atomic-safe under joint
// backend locks. Table helpers never take b.mu themselves.
