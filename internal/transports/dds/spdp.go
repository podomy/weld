// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package dds holds the CycloneDDS sidecar contract.
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
	"crypto/rand"
	"encoding/binary"
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
)

// DefaultLeaseDuration is the default SPDP participant lease.
const DefaultLeaseDuration = 10 * time.Second

// Protocol version announced in SPDP params.
const (
	protocolMajor = 2
	protocolMinor = 1
)

// Vendor id announced as Eclipse CycloneDDS.
//
// Both the RTPS header vendor and the PID 0x0016 value carry 01 10 as
// observed on a live domain 42 Cyclone talker in weld-jazzy.
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
)

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
	// Sequence is the SPDP sequence number.
	Sequence uint64
	// Alive is true for DATA and false for dispose.
	Alive bool
	// Lease is the announced lease, zero for dispose.
	Lease time.Duration
	// Ports holds all UDPv4 unicast locator ports in wire order, empty for dispose.
	Ports []uint16
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
	// metaPort is the bound metatraffic unicast port, zero for legacy.
	metaPort int
	// defaultPort is the bound user-traffic unicast port, zero for legacy.
	defaultPort int
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
	// metaPort is the bound metatraffic unicast port, zero for legacy.
	metaPort int
	// defaultPort is the bound user-traffic unicast port, zero for legacy.
	defaultPort int
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
		if ticket.metaPort > 0 && ticket.defaultPort > 0 {
			packet, err = MarshalAlivePacketWithPorts(ticket.guid, ticket.domain, ticket.seq, ticket.lease, ticket.metaPort, ticket.defaultPort)
		} else {
			packet, err = MarshalAlivePacket(ticket.guid, ticket.domain, ticket.seq, ticket.lease)
		}
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

	return spdpAnnounce{seq: p.seq, guid: p.guid, domain: p.domain, lease: p.lease, metaPort: p.metaPort, defaultPort: p.defaultPort}, nil
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
// spdpEncapPLCDRLE is never mutated by append.
func encodeSPDPParams(params []rtps.Param) []byte {
	out := append([]byte(nil), spdpEncapPLCDRLE[:]...)

	return append(out, rtps.EncodeParams(params)...)
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
// is zero for the non secure baseline.
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

	params := []rtps.Param{
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

	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: seq,
		Payload:        encodeSPDPParams(params),
	}

	return frameSPDP(guid.Prefix, data)
}

// MarshalDisposePacket builds an RTPS SPDP dispose packet.
//
// It carries the same RTPS header and DATA endpoints as an alive packet
// but sets the key flag and carries only the GUID plus domain tag, so a
// peer removes the participant instead of refreshing its lease.
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

	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       rtps.EntitySPDPWriter,
		SequenceNumber: seq,
		Payload:        encodeSPDPParams(params),
		Key:            true,
	}

	return frameSPDP(guid.Prefix, data)
}

// ParseAnnouncement parses one RTPS SPDP packet into fields.
//
// It decodes the RTPS message, scans submessages for the first DATA
// addressed from builtin writer 0x000100c2, and extracts the GUID,
// domain tag, lease, and all UDPv4 unicast locator ports. Payload and
// inline QoS params merge so live Cyclone payloads with a PL_CDR
// encapsulation header parse under the same rules as inline QoS
// announcements. Non SPDP DATA is ignored. Key flagged DATA is a
// dispose with zero lease and no ports.
func ParseAnnouncement(data []byte) (Announcement, error) {
	var out Announcement
	var drops rtps.Counter

	_, raws, err := rtps.DecodeMessage(data, &drops)
	if err != nil {
		return out, fmt.Errorf("dds: decode RTPS message: %w", errors.Join(err, ErrInvalidSPDP))
	}

	for i := range raws {
		sub, err := rtps.DecodeSubmessage(raws[i], &drops)
		if err != nil {
			continue
		}

		if sub.Data == nil {
			continue
		}

		if sub.Data.WriterID != rtps.EntitySPDPWriter {
			continue
		}

		ann, err := announcementFromData(sub.Data)
		if err != nil {
			n := spdpParseDrops.Add(1)
			log.Printf("dds: spdp parse data dropped %d: %v", n, err)

			continue
		}

		return ann, nil
	}

	return out, fmt.Errorf("dds: no SPDP DATA: %w", ErrInvalidSPDP)
}

// announcementAliveFields fills lease, ports, manual count, and QoS.
func announcementAliveFields(params []rtps.Param, out *Announcement) error {
	lease, err := findLeaseParam(params)
	if err != nil {
		return err
	}

	ports, err := findUnicastPortsParam(params)
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

	out.Lease = lease
	out.Ports = ports
	out.ManualLivelinessCount = manual
	out.BuiltinEndpointQos = qos

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
// alive params are explicit errors. Manual liveliness count and builtin
// endpoint QoS are optional and default to zero when absent; present
// values must be 4 bytes. Vendor PIDs such as 0x8003, 0x8007, 0x8019,
// and 0x800c are ignored for matching but preserved by the codec.
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

	out.GUID = guid
	out.DomainID = domain
	out.Sequence = d.SequenceNumber

	if d.Key {
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

// findUnicastPortsParam returns all UDPv4 unicast locator ports in wire order.
//
// Metatraffic, standard default, and discovery brief unicast ids all
// count in wire order. Multicast locators never count. Only the length
// is validated; any IPv4 address is accepted so off host captures
// parse, and the ports are the routes stored on the announcement.
func findUnicastPortsParam(params []rtps.Param) ([]uint16, error) {
	var out []uint16

	for _, p := range params {
		if p.ID != rtps.PIDMetatrafficUnicastLocator &&
			p.ID != rtps.PIDDefaultUnicastLocatorStandard &&
			p.ID != rtps.PIDDefaultUnicastLocator {
			continue
		}

		kind, port, _, err := rtps.DecodeLocator(p.Value)
		if err != nil {
			return nil, fmt.Errorf("dds: decode locator: %w", errors.Join(err, ErrInvalidSPDP))
		}

		if kind != rtps.LocatorKindUDPv4 {
			continue
		}

		if port > math.MaxUint16 {
			return nil, fmt.Errorf("dds: locator port %d above maximum: %w", port, ErrInvalidSPDP)
		}

		out = append(out, uint16(port))
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("dds: missing unicast locator: %w", ErrInvalidSPDP)
	}

	return out, nil
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

	for len(out)-off >= rtps.SubmessageHeaderSize {
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
// It requires the SPDP writer id and the inline QoS flag, then walks
// the parameter list and stores port in each UDPv4 unicast locator
// port field. Bounds failures leave the body unchanged.
func patchSPDPDataLocators(body []byte, flags uint8, littleEndian bool, port uint16) {
	if len(body) < 20 {
		return
	}

	if !isSPDPWriterID(body[8:12]) {
		return
	}

	if flags&rtps.DataFlagInlineQoS != 0 {
		patchInlineLocators(body[20:], littleEndian, port)

		return
	}

	if flags&rtps.DataFlagDataPresent == 0 {
		return
	}

	payload := body[20:]
	if len(payload) < spdpEncapSize || !isParameterListEncap(payload[:spdpEncapSize]) {
		return
	}

	patchInlineLocators(payload[spdpEncapSize:], littleEndian, port)
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
// observes each generation. It is safe for concurrent use.
type RemoteTable struct {
	mu         sync.Mutex
	entries    map[GUIDPrefix]ParticipantInfo
	generation uint64
	onChange   func(uint64)
}

// NewRemoteTable creates an empty remote table.
//
// onChange may be nil. It is called with the new generation after
// each mutation, outside the table lock, so it must not block.
func NewRemoteTable(onChange func(uint64)) *RemoteTable {
	return &RemoteTable{
		entries:  make(map[GUIDPrefix]ParticipantInfo),
		onChange: onChange,
	}
}

// SetOnChange replaces the change hook.
func (t *RemoteTable) SetOnChange(fn func(uint64)) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.onChange = fn
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

// handleDisposeLocked removes one prefix. Caller holds the lock.
//
// Stale dispose sequences at or below the stored LastSeq are ignored
// without bumping generation, matching the alive path ordering.
func (t *RemoteTable) handleDisposeLocked(ann Announcement) (bool, uint64, func(uint64)) {
	existing, ok := t.entries[ann.GUID.Prefix]
	if !ok {
		return false, 0, nil
	}

	if ann.Sequence <= existing.LastSeq {
		return false, 0, nil
	}

	delete(t.entries, ann.GUID.Prefix)
	t.generation++

	return true, t.generation, t.onChange
}

// handleAliveLocked adds or refreshes one entry. Caller holds the lock.
func (t *RemoteTable) handleAliveLocked(ann Announcement, expiry time.Time) (bool, uint64, func(uint64)) {
	existing, ok := t.entries[ann.GUID.Prefix]
	if !ok {
		t.entries[ann.GUID.Prefix] = ParticipantInfo{
			GUID:        ann.GUID,
			LeaseExpiry: expiry,
			LastSeq:     ann.Sequence,
			Security:    ann.Security,
		}
		t.generation++

		return true, t.generation, t.onChange
	}

	if ann.Sequence <= existing.LastSeq {
		return false, 0, nil
	}

	existing.GUID = ann.GUID
	existing.LeaseExpiry = expiry
	existing.LastSeq = ann.Sequence
	existing.Security = ann.Security
	t.entries[ann.GUID.Prefix] = existing
	t.generation++

	return true, t.generation, t.onChange
}

// HandlePacket parses one SPDP packet and updates the table.
//
// Alive packets add or refresh the entry with expiry now plus lease.
// Stale sequences at or below the stored LastSeq are ignored without
// bumping generation. Dispose packets remove the entry when their
// sequence is newer than the stored LastSeq; stale disposes are ignored
// like stale alive packets. Unknown params are ignored. Generation
// bumps once per effective change.
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
		changed, gen, hook := t.handleDisposeLocked(ann)
		t.mu.Unlock()
		notifyChange(changed, gen, hook)

		return nil
	}

	expiry := now.Add(ann.Lease)

	t.mu.Lock()
	changed, gen, hook := t.handleAliveLocked(ann, expiry)
	t.mu.Unlock()
	notifyChange(changed, gen, hook)

	return nil
}

// Remove deletes one prefix and bumps generation when present.
func (t *RemoteTable) Remove(prefix GUIDPrefix) bool {
	t.mu.Lock()

	_, ok := t.entries[prefix]
	if !ok {
		t.mu.Unlock()

		return false
	}

	delete(t.entries, prefix)
	t.generation++

	gen := t.generation
	hook := t.onChange
	t.mu.Unlock()

	notifyChange(true, gen, hook)

	return true
}

// Sweep removes expired entries and bumps generation once when any expire.
//
// An entry expires when now is at or past LeaseExpiry. It returns the
// number of removed entries.
func (t *RemoteTable) Sweep(now time.Time) int {
	t.mu.Lock()

	removed := 0

	for prefix, info := range t.entries {
		if now.Before(info.LeaseExpiry) {
			continue
		}

		delete(t.entries, prefix)
		removed++
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
			// Malformed packets must not kill discovery; the error is
			// intentionally ignored and the sweep check below still runs.
			herr := t.HandlePacket(data, now)
			if herr != nil {
				_ = herr
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
