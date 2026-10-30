// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package rtps

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// Port mapping constants per DDSI-RTPS section 9.6.1 as used by CycloneDDS.
// PB is the base port, DG the per domain gain, PG the per participant gain.
// Multicast well-known ports are B+d0 and B+d1; unicast ports are
// B+PG*PI+d10 and B+PG*PI+d11, where B is PB+DG*domain and the d offsets are
// the Metatraffic constants below.
const (
	// BasePort is the RTPS well known base port PB.
	BasePort = 7400
	// DomainGain is the per domain port stride DG.
	DomainGain = 250
	// ParticipantGain is the per participant port stride PG.
	ParticipantGain = 2
	// MetatrafficUnicastOffset is d10 for participant discovery unicast.
	MetatrafficUnicastOffset = 10
	// UsertrafficUnicastOffset is d11 for user data unicast.
	UsertrafficUnicastOffset = 11
	// MetatrafficMulticastOffset is d0 for SPDP multicast.
	MetatrafficMulticastOffset = 0
	// UsertrafficMulticastOffset is d1 for user data multicast.
	UsertrafficMulticastOffset = 1
	// MaxPort is the largest valid UDP port.
	MaxPort = 65535
	// SPDPMulticastGroup is the standard IPv4 SPDP multicast group.
	SPDPMulticastGroup = "239.255.0.1"
	// LoopbackIPv4 is the fallback address when no LAN interface exists.
	LoopbackIPv4 = "127.0.0.1"
	// AnyIPv4 is the wildcard bind for unicast sockets so LAN-directed
	// packets arrive alongside loopback traffic.
	AnyIPv4 = "0.0.0.0"
)

// Socket and validation bounds shared by send and bind paths.
const (
	// minPort is the smallest valid UDP port for sends.
	minPort = 1
	// ephemeralPort binds an ephemeral port for tests.
	ephemeralPort = 0
	// socketOptOn enables a socket option.
	socketOptOn = 1
)

// ErrClosed reports use of a Transport after Close.
var ErrClosed = errors.New("rtps: transport closed")

// errReuseUnavailable reports the reuse-port bind path is not available
// on this platform. Callers fall back to ListenMulticastUDP.
var errReuseUnavailable = errors.New("rtps: reuse port bind unavailable")

// closeGuard owns the closed flag shared by every socket type.
// Each Transport, UnicastConn, MulticastConn, and MulticastSender embeds
// one guard instead of repeating a mutex plus bool pair.
type closeGuard struct {
	// mu guards closed for concurrent use.
	mu sync.Mutex
	// closed reports whether Close has run.
	closed bool
}

// isClosed reports whether Close has run.
func (g *closeGuard) isClosed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closed
}

// tryClose marks closed and reports true on the first call.
// Later calls report false so Close stays idempotent.
func (g *closeGuard) tryClose() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return false
	}
	g.closed = true
	return true
}

// MetatrafficMulticastPort maps a domain to its SPDP multicast port:
// 7400 plus 250 per domain plus the metatraffic multicast offset.
func MetatrafficMulticastPort(domain uint32) (int, error) {
	return multicastPort(domain, MetatrafficMulticastOffset)
}

// UsertrafficMulticastPort maps a domain to its user multicast port.
func UsertrafficMulticastPort(domain uint32) (int, error) {
	return multicastPort(domain, UsertrafficMulticastOffset)
}

// MetatrafficUnicastPort maps a domain and participant to the discovery
// unicast port.
func MetatrafficUnicastPort(domain, participant uint32) (int, error) {
	return unicastPort(domain, participant, MetatrafficUnicastOffset)
}

// UsertrafficUnicastPort maps a domain and participant to the user data
// unicast port.
func UsertrafficUnicastPort(domain, participant uint32) (int, error) {
	return unicastPort(domain, participant, UsertrafficUnicastOffset)
}

// checkDomain rejects domains above the shared transports limit.
func checkDomain(domain uint32) error {
	if domain > transports.MaxDomainID {
		return fmt.Errorf("rtps: domain %d above maximum %d", domain, transports.MaxDomainID)
	}
	return nil
}

// checkContext reports a canceled context with operation context.
func checkContext(ctx context.Context, op string) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("rtps: %s: %w", op, err)
	}
	return nil
}

// multicastPort adds the domain gain and offset, rejecting domains above
// the shared transports limit and ports above the UDP maximum.
func multicastPort(domain uint32, offset uint64) (int, error) {
	err := checkDomain(domain)
	if err != nil {
		return 0, err
	}
	port := uint64(BasePort) + uint64(DomainGain)*uint64(domain) + offset
	if port > MaxPort {
		return 0, fmt.Errorf("rtps: multicast port %d above maximum %d", port, MaxPort)
	}
	return int(port), nil
}

// unicastPort adds the domain gain, participant gain, and offset,
// rejecting out of range domains and overflowing ports.
func unicastPort(domain, participant uint32, offset uint64) (int, error) {
	err := checkDomain(domain)
	if err != nil {
		return 0, err
	}
	port := uint64(BasePort) + uint64(DomainGain)*uint64(domain) + uint64(ParticipantGain)*uint64(participant) + offset
	if port > MaxPort {
		return 0, fmt.Errorf("rtps: unicast port %d above maximum %d", port, MaxPort)
	}
	return int(port), nil
}

// Config carries join parameters for the wildcard UDP transport. Zero
// MetaPort or UnicastPort binds an ephemeral port, which suits tests;
// JoinStandard fills standard mapped ports for real domains.
type Config struct {
	// DomainID is the DDS domain to join, at most transports.MaxDomainID.
	DomainID uint32
	// ParticipantID is the local participant index for unicast mapping.
	ParticipantID uint32
	// MetaPort is the SPDP multicast port, or zero for ephemeral.
	MetaPort int
	// UnicastPort is the unicast data port, or zero for ephemeral.
	UnicastPort int
}

// Validate checks the domain range and the explicit port range.
func (c Config) Validate() error {
	if c.DomainID > transports.MaxDomainID {
		return fmt.Errorf("rtps: domain %d above maximum %d", c.DomainID, transports.MaxDomainID)
	}
	if c.MetaPort < 0 || c.MetaPort > MaxPort {
		return fmt.Errorf("rtps: meta port %d out of range", c.MetaPort)
	}
	if c.UnicastPort < 0 || c.UnicastPort > MaxPort {
		return fmt.Errorf("rtps: unicast port %d out of range", c.UnicastPort)
	}
	return nil
}

// Transport is a wildcard UDP transport for first contact: SPDP multicast
// send plus receive on the metatraffic socket, and unicast data on the
// loopback socket. Delivery is best effort with no retransmission and no
// ordering; payloads stay generic bytes. It is safe for concurrent use.
type Transport struct {
	// guard owns the closed flag for concurrent use.
	guard closeGuard
	// meta carries SPDP multicast traffic.
	meta *net.UDPConn
	// uni carries unicast data on the wildcard socket.
	uni *net.UDPConn
	// target is the SPDP multicast destination.
	target *net.UDPAddr
	// domain is the joined DDS domain.
	domain uint32
	// participant is the local participant index.
	participant uint32
	// metaPort is the bound SPDP multicast port.
	metaPort int
	// uniPort is the bound unicast port.
	uniPort int
}

// Join binds the SPDP multicast receive socket and the wildcard unicast
// socket, enables multicast loopback on the sender, and returns a ready
// Transport. Zero ports bind ephemeral ports for tests.
func Join(ctx context.Context, cfg Config) (*Transport, error) {
	err := cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("rtps: join: %w", err)
	}
	err = checkContext(ctx, "join")
	if err != nil {
		return nil, err
	}
	meta, uni, err := openTransportSockets(ctx, cfg)
	if err != nil {
		return nil, err
	}
	out, err := newTransport(meta, uni, cfg)
	if err != nil {
		cerr := closeBoth(meta, uni)
		return nil, fmt.Errorf("rtps: join: %w", errors.Join(err, cerr))
	}
	return out, nil
}

// openTransportSockets binds the meta and unicast sockets for Join.
// The caller owns both sockets on success; on failure the bound socket
// is closed before returning.
func openTransportSockets(ctx context.Context, cfg Config) (*net.UDPConn, *net.UDPConn, error) {
	meta, err := listenMeta(ctx, cfg.MetaPort)
	if err != nil {
		return nil, nil, err
	}
	uni, err := listenUnicast(cfg.UnicastPort)
	if err != nil {
		cerr := meta.Close()
		return nil, nil, fmt.Errorf("rtps: join: %w", errors.Join(err, cerr))
	}
	return meta, uni, nil
}

// JoinStandard joins a domain on standard mapped ports: SPDP multicast
// for discovery plus per participant user unicast for data.
func JoinStandard(ctx context.Context, domainID, participantID uint32) (*Transport, error) {
	meta, err := MetatrafficMulticastPort(domainID)
	if err != nil {
		return nil, fmt.Errorf("rtps: join standard: %w", err)
	}
	uni, err := UsertrafficUnicastPort(domainID, participantID)
	if err != nil {
		return nil, fmt.Errorf("rtps: join standard: %w", err)
	}
	return Join(ctx, Config{DomainID: domainID, ParticipantID: participantID, MetaPort: meta, UnicastPort: uni})
}

// enableLoopback enables IP multicast loop on one socket.
//
// It controls multicast self delivery, not the bind address.
func enableLoopback(conn *net.UDPConn, op string) error {
	err := enableMulticastLoopback(conn, true)
	if err != nil {
		return fmt.Errorf("rtps: enable %s loopback: %w", op, err)
	}
	return nil
}

// boundPorts returns the bound ports for the meta and unicast sockets.
func boundPorts(meta, uni *net.UDPConn) (int, int, error) {
	metaPort, err := boundPort(meta)
	if err != nil {
		return 0, 0, fmt.Errorf("rtps: meta port: %w", err)
	}
	uniPort, err := boundPort(uni)
	if err != nil {
		return 0, 0, fmt.Errorf("rtps: unicast port: %w", err)
	}
	return metaPort, uniPort, nil
}

// enableTransportLoopback enables IP multicast loop on both sockets.
func enableTransportLoopback(meta, uni *net.UDPConn) error {
	err := enableLoopback(meta, "meta")
	if err != nil {
		return err
	}
	err = enableLoopback(uni, "unicast")
	if err != nil {
		return err
	}
	return nil
}

// newTransport enables multicast loopback on both sockets and fills the
// SPDP target from the bound meta port. Sockets stay open on success and
// are left to the caller on failure.
//
// Multicast loopback is enabled on the meta socket because SPDP sends go out that
// socket: without it local group members miss self sends. It is enabled
// on the unicast socket for symmetry; unicast sends never need it.
func newTransport(meta, uni *net.UDPConn, cfg Config) (*Transport, error) {
	err := enableTransportLoopback(meta, uni)
	if err != nil {
		return nil, err
	}
	metaPort, uniPort, err := boundPorts(meta, uni)
	if err != nil {
		return nil, err
	}
	group, err := spdpGroup()
	if err != nil {
		return nil, err
	}
	return &Transport{
		meta:        meta,
		uni:         uni,
		target:      &net.UDPAddr{IP: group, Port: metaPort},
		domain:      cfg.DomainID,
		participant: cfg.ParticipantID,
		metaPort:    metaPort,
		uniPort:     uniPort,
	}, nil
}

// DomainID returns the joined domain.
func (t *Transport) DomainID() uint32 {
	return t.domain
}

// ParticipantID returns the local participant index.
func (t *Transport) ParticipantID() uint32 {
	return t.participant
}

// MetaPort returns the bound SPDP multicast port.
func (t *Transport) MetaPort() int {
	return t.metaPort
}

// UnicastPort returns the bound unicast port.
func (t *Transport) UnicastPort() int {
	return t.uniPort
}

// SPDPTarget returns the SPDP multicast destination: group plus bound
// meta port. The result is a copy owned by the caller.
func (t *Transport) SPDPTarget() *net.UDPAddr {
	return &net.UDPAddr{IP: append(net.IP(nil), t.target.IP...), Port: t.target.Port}
}

// LocalUnicastAddr returns the advertised LAN address of the unicast socket.
//
// It rescans interfaces on each call and falls back to loopback when no LAN exists.
func (t *Transport) LocalUnicastAddr() *net.UDPAddr {
	return &net.UDPAddr{IP: AdvertiseIPv4(), Port: t.uniPort}
}

// SendTo sends msg to addr, choosing the socket by destination. Multicast
// goes out the meta socket so group traffic never takes the unicast socket.
// Unicast goes out the unicast
// socket with its stable source port. Empty and oversize payloads fail
// loudly; short writes fail instead of truncating.
func (t *Transport) SendTo(addr *net.UDPAddr, msg []byte) error {
	if addr == nil {
		return errors.New("rtps: send to nil address") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := checkSend(msg)
	if err != nil {
		return err
	}
	if t.isClosed() {
		return fmt.Errorf("rtps: send to: %w", ErrClosed)
	}
	conn := t.uni
	op := "unicast"
	if addr.IP.IsMulticast() {
		conn = t.meta
		op = "multicast"
	}
	err = writeDatagram(conn, addr, msg, "send "+op, op, t.isClosed, nil)
	if err != nil {
		return err
	}
	return nil
}

// SendUnicast sends msg to a unicast addr over the unicast socket.
// Multicast destinations are rejected so group traffic cannot take the
// unicast socket and vanish; use SendTo for group destinations.
func (t *Transport) SendUnicast(addr *net.UDPAddr, msg []byte) error {
	if addr != nil && addr.IP.IsMulticast() {
		return fmt.Errorf("rtps: send unicast to multicast group %s", addr.IP)
	}
	err := t.SendTo(addr, msg)
	if err != nil {
		return fmt.Errorf("rtps: send unicast: %w", err)
	}
	return nil
}

// SendSPDP multicasts msg to the SPDP group for this transport over the
// meta socket. Standard deployments share one meta port per domain, so
// every peer on the domain receives it. Ephemeral test ports receive only
// self sends, which still proves local delivery; cross transport multicast tests
// use SendTo with the peer SPDPTarget.
func (t *Transport) SendSPDP(msg []byte) error {
	err := t.SendTo(t.SPDPTarget(), msg)
	if err != nil {
		return fmt.Errorf("rtps: send spdp: %w", err)
	}
	return nil
}

// ReceiveMeta reads one datagram from the SPDP multicast socket. It blocks
// until a datagram arrives or the read deadline fires; use SetReadDeadline
// to bound the wait.
func (t *Transport) ReceiveMeta(buf []byte) (int, *net.UDPAddr, error) {
	if t.isClosed() {
		return 0, nil, fmt.Errorf("rtps: receive meta: %w", ErrClosed)
	}
	return receive(t.meta, "meta", buf)
}

// ReceiveUnicast reads one datagram from the unicast socket. It blocks
// until a datagram arrives or the read deadline fires; use SetReadDeadline
// to bound the wait.
func (t *Transport) ReceiveUnicast(buf []byte) (int, *net.UDPAddr, error) {
	if t.isClosed() {
		return 0, nil, fmt.Errorf("rtps: receive unicast: %w", ErrClosed)
	}
	return receive(t.uni, "unicast", buf)
}

// SetReadDeadline bounds ReceiveMeta and ReceiveUnicast. A zero time
// clears the deadline.
func (t *Transport) SetReadDeadline(tm time.Time) error {
	err := t.meta.SetReadDeadline(tm)
	if err != nil {
		return fmt.Errorf("rtps: meta read deadline: %w", err)
	}
	err = t.uni.SetReadDeadline(tm)
	if err != nil {
		return fmt.Errorf("rtps: unicast read deadline: %w", err)
	}
	return nil
}

// Close shuts both sockets. It is idempotent and safe for concurrent use;
// later data path calls report ErrClosed.
func (t *Transport) Close() error {
	if !t.guard.tryClose() {
		return nil
	}
	err := errors.Join(t.meta.Close(), t.uni.Close())
	if err != nil {
		return fmt.Errorf("rtps: close: %w", err)
	}
	return nil
}

// isClosed reports whether Close has run.
func (t *Transport) isClosed() bool {
	return t.guard.isClosed()
}

// checkSend rejects empty and oversize payloads before any socket use.
func checkSend(msg []byte) error {
	if len(msg) == 0 {
		return errors.New("rtps: send drops empty payload") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(msg) > MaxDatagramSize {
		return fmt.Errorf("rtps: send %d above max datagram %d", len(msg), MaxDatagramSize)
	}
	return nil
}

// mapSendError maps a write failure to closed, canceled, or wrapped IO.
func mapSendError(op string, err error, closed func() bool, ctx context.Context) error {
	if closed() {
		return fmt.Errorf("rtps: %s: %w", op, ErrClosed)
	}
	if ctx != nil {
		cerr := ctx.Err()
		if cerr != nil {
			return fmt.Errorf("rtps: %s: %w", op, cerr)
		}
	}
	return fmt.Errorf("rtps: %s: %w", op, err)
}

// writeDatagram writes payload to addr and checks the short write. The op
// names the wrapped error context and shortOp names the short write. A nil
// ctx skips context mapping for non unicast sends.
func writeDatagram(conn *net.UDPConn, addr *net.UDPAddr, payload []byte, op, shortOp string, closed func() bool, ctx context.Context) error {
	sent, err := conn.WriteToUDP(payload, addr)
	if err != nil {
		return mapSendError(op, err, closed, ctx)
	}
	if sent != len(payload) {
		return fmt.Errorf("rtps: short %s write %d of %d", shortOp, sent, len(payload))
	}
	return nil
}

// receive reads one datagram from conn, tagging errors with the socket op.
func receive(conn *net.UDPConn, op string, buf []byte) (int, *net.UDPAddr, error) {
	sent, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return 0, nil, fmt.Errorf("rtps: receive %s: %w", op, err)
	}
	return sent, addr, nil
}

// listenMeta binds the SPDP multicast receive socket and joins the group.
// A zero port binds an ephemeral port for tests. It prefers the
// reuse-port path so Weld coexists with vendor participants on one host,
// and falls back to ListenMulticastUDP where that path is unavailable.
func listenMeta(ctx context.Context, port int) (*net.UDPConn, error) {
	group, err := spdpGroup()
	if err != nil {
		return nil, err
	}
	conn, err := listenMetaReuse(ctx, port, group)
	if err == nil {
		return conn, nil
	}
	if !errors.Is(err, errReuseUnavailable) {
		return nil, err
	}
	conn, err = net.ListenMulticastUDP("udp4", nil, &net.UDPAddr{IP: group, Port: port})
	if err != nil {
		return nil, fmt.Errorf("rtps: listen spdp multicast: %w", err)
	}
	return conn, nil
}

// listenUnicast binds the wildcard unicast socket. A zero port binds an
// ephemeral port for tests.
func listenUnicast(port int) (*net.UDPConn, error) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP(AnyIPv4), Port: port})
	if err != nil {
		return nil, fmt.Errorf("rtps: listen unicast: %w", err)
	}
	return conn, nil
}

// spdpGroup parses the SPDP multicast group address.
func spdpGroup() (net.IP, error) {
	group := net.ParseIP(SPDPMulticastGroup)
	if group == nil {
		return nil, fmt.Errorf("rtps: bad spdp group %q", SPDPMulticastGroup)
	}
	return group, nil
}

// boundPort returns the local UDP port of a bound socket.
func boundPort(conn *net.UDPConn) (int, error) {
	addr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return 0, errors.New("rtps: local addr is not udp") //nolint:wrapcheck // new error, nothing to wrap
	}
	return addr.Port, nil
}

// closeBoth closes two sockets, joining close errors for failure paths.
func closeBoth(first, second *net.UDPConn) error {
	err := errors.Join(first.Close(), second.Close())
	if err != nil {
		return fmt.Errorf("rtps: close sockets: %w", err)
	}
	return nil
}

// unicastProbeLimit caps participant slot probes in DialUnicast. The first
// free user unicast port wins, so two callers on one domain land on
// successive participants instead of colliding on participant zero.
const unicastProbeLimit = 64

// socketPollInterval bounds context cancel latency for socket receives.
// Each receive waits at most this long before rechecking context state.
const socketPollInterval = 20 * time.Millisecond

// UnicastConn is a bound wildcard UDP socket for DDSI user data. The local
// port is the first free UsertrafficUnicastPort for the domain, starting at
// participant zero. It is safe for concurrent use.
type UnicastConn struct {
	// guard owns the closed flag for concurrent use.
	guard closeGuard
	// conn carries unicast datagrams.
	conn *net.UDPConn
	// domain is the joined DDS domain.
	domain uint32
	// participant is the probed participant index.
	participant uint32
	// port is the bound unicast port.
	port int
}

// MulticastConn is an SPDP multicast socket for one domain. Sends go to the
// MetatrafficMulticastPort group and receives read from the joined group.
// Loopback is enabled so local sends are visible locally. It is safe for
// concurrent use.
type MulticastConn struct {
	// guard owns the closed flag for concurrent use.
	guard closeGuard
	// conn carries multicast datagrams.
	conn *net.UDPConn
	// target is the SPDP multicast destination.
	target *net.UDPAddr
	// domain is the joined DDS domain.
	domain uint32
	// port is the bound SPDP multicast port.
	port int
}

// bindUnicastSlot maps one participant slot to a bound socket.
func bindUnicastSlot(domain, participant uint32) (int, *net.UDPConn, error) {
	port, err := UsertrafficUnicastPort(domain, participant)
	if err != nil {
		return 0, nil, err
	}
	conn, err := listenUnicast(port)
	if err != nil {
		return 0, nil, err
	}
	return port, conn, nil
}

// dialSlotError maps a bind failure to retry or fatal.
// Address in use retries on the next participant; other failures abort,
// joining any earlier in-use error for context.
func dialSlotError(err, lastErr error) (bool, error) {
	if isAddrInUse(err) {
		return true, err
	}
	if lastErr != nil {
		joined := errors.Join(lastErr, err)
		return false, fmt.Errorf("rtps: dial unicast: %w", joined)
	}
	return false, fmt.Errorf("rtps: dial unicast: %w", err)
}

// tryUnicastSlot binds one participant slot.
// It returns the bound connection, or whether to try the next slot.
func tryUnicastSlot(domain, participant uint32, lastErr error) (*UnicastConn, bool, error) {
	port, conn, err := bindUnicastSlot(domain, participant)
	if err != nil {
		retry, mapped := dialSlotError(err, lastErr)
		if retry {
			return nil, true, mapped
		}
		return nil, false, mapped
	}
	return &UnicastConn{
		conn:        conn,
		domain:      domain,
		participant: participant,
		port:        port,
	}, false, nil
}

// noFreeSlotError maps an exhausted probe to its final error.
func noFreeSlotError(lastErr error) error {
	if lastErr == nil {
		return errors.New("rtps: dial unicast: no free participant slot") //nolint:wrapcheck // new error, nothing to wrap
	}
	return fmt.Errorf("rtps: dial unicast: no free participant slot: %w", lastErr)
}

// probeUnicastSlots scans participant slots until one binds.
func probeUnicastSlots(ctx context.Context, domain uint32) (*UnicastConn, error) {
	var lastErr error
	for i := range unicastProbeLimit {
		participant := uint32(i)
		err := checkContext(ctx, "dial unicast")
		if err != nil {
			return nil, err
		}
		conn, retry, err := tryUnicastSlot(domain, participant, lastErr)
		if err != nil {
			if retry {
				lastErr = err
				continue
			}
			return nil, err
		}
		return conn, nil
	}
	return nil, noFreeSlotError(lastErr)
}

// DialUnicast binds 0.0.0.0 to the DDSI user data unicast port for domain.
// The port comes from UsertrafficUnicastPort, starting at participant zero
// and advancing to the first free slot, so two dials on one domain succeed
// on successive participants. The context only gates startup; a canceled
// context fails the dial. The caller owns the result and must Close it.
func DialUnicast(ctx context.Context, domain uint32) (*UnicastConn, error) {
	err := checkDomain(domain)
	if err != nil {
		return nil, fmt.Errorf("rtps: dial unicast: %w", err)
	}
	err = checkContext(ctx, "dial unicast")
	if err != nil {
		return nil, err
	}
	return probeUnicastSlots(ctx, domain)
}

// multicastEndpoint resolves the SPDP port and group for domain.
func multicastEndpoint(domain uint32) (int, net.IP, error) {
	return multicastEndpointFor(domain, MetatrafficMulticastPort)
}

// userMulticastEndpoint resolves the user-traffic port and group for domain.
//
// CycloneDDS multicasts user DATA and heartbeats here; joining it is what
// lets every local participant receive samples by fanout instead of
// depending on per-reader unicast.
func userMulticastEndpoint(domain uint32) (int, net.IP, error) {
	return multicastEndpointFor(domain, UsertrafficMulticastPort)
}

// multicastEndpointFor resolves one domain port plus the SPDP group.
func multicastEndpointFor(domain uint32, portFn func(uint32) (int, error)) (int, net.IP, error) {
	port, err := portFn(domain)
	if err != nil {
		return 0, nil, err
	}
	group, err := spdpGroup()
	if err != nil {
		return 0, nil, err
	}
	return port, group, nil
}

// enableJoinLoopback enables IP multicast loop on a joined socket with cleanup.
func enableJoinLoopback(conn *net.UDPConn) error {
	err := enableMulticastLoopback(conn, true)
	if err != nil {
		cerr := conn.Close()
		joined := errors.Join(err, cerr)
		return fmt.Errorf("rtps: join multicast: %w", joined)
	}
	return nil
}

// JoinMulticast joins the SPDP multicast group for domain on the standard
// MetatrafficMulticastPort and enables loopback so local sends are visible
// locally. The caller owns the result and must Close it.
func JoinMulticast(ctx context.Context, domain uint32) (*MulticastConn, error) {
	return joinMulticastEndpoint(ctx, domain, multicastEndpoint)
}

// JoinUserMulticast joins the user-traffic multicast group for domain on the
// standard UsertrafficMulticastPort. CycloneDDS multicasts user DATA and
// heartbeats here; without this join a participant depends entirely on
// per-reader unicast, and a second local participant on one domain
// starves while the first receives everything. The caller owns the result
// and must Close it.
func JoinUserMulticast(ctx context.Context, domain uint32) (*MulticastConn, error) {
	return joinMulticastEndpoint(ctx, domain, userMulticastEndpoint)
}

// joinMulticastEndpoint joins one domain group endpoint with loopback on.
func joinMulticastEndpoint(ctx context.Context, domain uint32, endpoint func(uint32) (int, net.IP, error)) (*MulticastConn, error) {
	err := checkDomain(domain)
	if err != nil {
		return nil, fmt.Errorf("rtps: join multicast: %w", err)
	}
	err = checkContext(ctx, "join multicast")
	if err != nil {
		return nil, err
	}
	port, group, err := endpoint(domain)
	if err != nil {
		return nil, fmt.Errorf("rtps: join multicast: %w", err)
	}
	conn, err := listenMeta(ctx, port)
	if err != nil {
		return nil, err
	}
	err = enableJoinLoopback(conn)
	if err != nil {
		return nil, err
	}
	return &MulticastConn{
		conn:   conn,
		target: &net.UDPAddr{IP: group, Port: port},
		domain: domain,
		port:   port,
	}, nil
}

// Domain returns the joined domain for a unicast socket.
func (c *UnicastConn) Domain() uint32 {
	return c.domain
}

// Participant returns the probed participant index for a unicast socket.
func (c *UnicastConn) Participant() uint32 {
	return c.participant
}

// LocalPort returns the bound unicast port.
func (c *UnicastConn) LocalPort() int {
	return c.port
}

// LocalAddr returns the advertised LAN address of a unicast socket.
//
// It rescans interfaces on each call and falls back to loopback when no LAN exists.
func (c *UnicastConn) LocalAddr() *net.UDPAddr {
	return &net.UDPAddr{IP: AdvertiseIPv4(), Port: c.port}
}

// Domain returns the joined domain for a multicast socket.
func (m *MulticastConn) Domain() uint32 {
	return m.domain
}

// Port returns the bound SPDP multicast port.
func (m *MulticastConn) Port() int {
	return m.port
}

// Target returns the SPDP multicast destination. The result is a copy owned
// by the caller.
func (m *MulticastConn) Target() *net.UDPAddr {
	return &net.UDPAddr{IP: append(net.IP(nil), m.target.IP...), Port: m.target.Port}
}

// validateUnicastTarget parses ip and checks port range.
func validateUnicastTarget(ip string, port int) (net.IP, error) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return nil, fmt.Errorf("rtps: unicast send to bad ip %q", ip)
	}
	if port < minPort || port > MaxPort {
		return nil, fmt.Errorf("rtps: unicast send to bad port %d", port)
	}
	return parsed, nil
}

// checkUnicastSend validates context, payload, target, and closed state.
func checkUnicastSend(ctx context.Context, closed func() bool, ip string, port int, b []byte) (net.IP, error) {
	err := checkContext(ctx, "unicast send")
	if err != nil {
		return nil, err
	}
	err = checkSend(b)
	if err != nil {
		return nil, fmt.Errorf("rtps: unicast send: %w", err)
	}
	parsed, err := validateUnicastTarget(ip, port)
	if err != nil {
		return nil, err
	}
	if closed() {
		return nil, fmt.Errorf("rtps: unicast send: %w", ErrClosed)
	}
	err = checkContext(ctx, "unicast send")
	if err != nil {
		return nil, err
	}
	return parsed, nil
}

// copyDatagram returns a fresh copy of the first n bytes.
func copyDatagram(buf []byte, n int) []byte {
	out := make([]byte, n)
	copy(out, buf[:n])
	return out
}

// readSingleDatagram performs one poll plus read.
// It returns the byte count and sender, or whether to retry the poll.
func readSingleDatagram(conn *net.UDPConn, buf []byte, ctx context.Context, op string, closed func() bool) (int, *net.UDPAddr, bool, error) {
	err := setPollDeadline(conn, op, closed)
	if err != nil {
		return 0, nil, false, err
	}
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		retry, mapped := mapReadError(err, ctx, op, closed)
		if retry {
			return 0, nil, true, nil
		}
		return 0, nil, false, mapped
	}
	return n, addr, false, nil
}

// receiveDatagram polls one datagram with context cancel support. It owns
// the buffer and returns a fresh copy plus the sender.
func receiveDatagram(conn *net.UDPConn, ctx context.Context, op string, closed func() bool) ([]byte, *net.UDPAddr, error) {
	buf := make([]byte, MaxDatagramSize)
	for {
		n, addr, retry, err := readSingleDatagram(conn, buf, ctx, op, closed)
		if err != nil {
			return nil, nil, err
		}
		if retry {
			continue
		}
		return copyDatagram(buf, n), addr, nil
	}
}

// SendTo sends b to ip and port over a unicast socket. A canceled context
// fails with the context error. Empty and oversize payloads fail loudly.
// Use after Close reports ErrClosed.
func (c *UnicastConn) SendTo(ctx context.Context, ip string, port int, b []byte) error {
	parsed, err := checkUnicastSend(ctx, c.isClosed, ip, port, b)
	if err != nil {
		return err
	}
	addr := &net.UDPAddr{IP: parsed, Port: port}
	err = writeDatagram(c.conn, addr, b, "unicast send", "unicast", c.isClosed, ctx)
	if err != nil {
		return err
	}
	return nil
}

// Receive reads one datagram from a unicast socket. It polls with a short
// read deadline so context cancel unblocks the wait with the context error.
// The returned bytes are a fresh copy owned by the caller. Use after Close
// reports ErrClosed.
func (c *UnicastConn) Receive(ctx context.Context) (string, int, []byte, error) {
	if c.isClosed() {
		return "", 0, nil, fmt.Errorf("rtps: unicast receive: %w", ErrClosed)
	}
	err := checkContext(ctx, "unicast receive")
	if err != nil {
		return "", 0, nil, err
	}
	out, addr, err := receiveDatagram(c.conn, ctx, "unicast", c.isClosed)
	if err != nil {
		return "", 0, nil, err
	}
	if addr == nil {
		return "", 0, out, nil
	}
	return addr.IP.String(), addr.Port, out, nil
}

// Close shuts a unicast socket. It is idempotent and safe for concurrent
// use; later data path calls report ErrClosed.
func (c *UnicastConn) Close() error {
	if !c.guard.tryClose() {
		return nil
	}
	err := c.conn.Close()
	if err != nil {
		return fmt.Errorf("rtps: unicast close: %w", err)
	}
	return nil
}

// isClosed reports whether Close has run for a unicast socket.
func (c *UnicastConn) isClosed() bool {
	return c.guard.isClosed()
}

// Send multicasts b to the SPDP group for the joined domain. Empty and
// oversize payloads fail loudly. Use after Close reports ErrClosed.
func (m *MulticastConn) Send(b []byte) error {
	err := checkSend(b)
	if err != nil {
		return fmt.Errorf("rtps: multicast send: %w", err)
	}
	if m.isClosed() {
		return fmt.Errorf("rtps: multicast send: %w", ErrClosed)
	}
	err = writeDatagram(m.conn, m.target, b, "multicast send", "multicast", m.isClosed, nil)
	if err != nil {
		return err
	}
	return nil
}

// Receive reads one datagram from a multicast socket. It polls with a short
// read deadline so context cancel unblocks the wait with the context error.
// The returned bytes are a fresh copy owned by the caller. Use after Close
// reports ErrClosed.
func (m *MulticastConn) Receive(ctx context.Context) ([]byte, error) {
	if m.isClosed() {
		return nil, fmt.Errorf("rtps: multicast receive: %w", ErrClosed)
	}
	err := checkContext(ctx, "multicast receive")
	if err != nil {
		return nil, err
	}
	out, _, err := receiveDatagram(m.conn, ctx, "multicast", m.isClosed)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Close shuts a multicast socket. It is idempotent and safe for concurrent
// use; later data path calls report ErrClosed.
func (m *MulticastConn) Close() error {
	if !m.guard.tryClose() {
		return nil
	}
	err := m.conn.Close()
	if err != nil {
		return fmt.Errorf("rtps: multicast close: %w", err)
	}
	return nil
}

// isClosed reports whether Close has run for a multicast socket.
func (m *MulticastConn) isClosed() bool {
	return m.guard.isClosed()
}

// MulticastSender is a send-only SPDP multicast socket for one domain.
//
// It holds an unbound wildcard UDP socket that sends to the
// MetatrafficMulticastPort group without binding the group port, so any
// number of senders coexist without address conflicts and EADDRINUSE is
// impossible. Reception runs over unicast: CycloneDDS itself coexists this
// way with exactly one multicast receiver per host, and directed SPDP
// arrives on the advertised unicast metatraffic locator. It is safe for
// concurrent use.
type MulticastSender struct {
	// guard owns the closed flag for concurrent use.
	guard closeGuard
	// conn carries outbound multicast datagrams.
	conn *net.UDPConn
	// target is the SPDP multicast destination.
	target *net.UDPAddr
	// domain is the joined DDS domain.
	domain uint32
	// port is the SPDP multicast port.
	port int
}

// closeSetupConn closes conn after a failed setup step and wraps both errors.
func closeSetupConn(conn *net.UDPConn, err error, op string) error {
	cerr := conn.Close()
	return fmt.Errorf("rtps: %s: %w", op, errors.Join(err, cerr))
}

// setupSenderEgress pins egress on a sender socket with cleanup.
func setupSenderEgress(conn *net.UDPConn) error {
	err := setMulticastEgress(conn)
	if err != nil {
		return closeSetupConn(conn, err, "multicast sender egress")
	}
	return nil
}

// setupSenderLoopback enables loopback on a sender socket with cleanup.
func setupSenderLoopback(conn *net.UDPConn) error {
	err := enableMulticastLoopback(conn, true)
	if err != nil {
		return closeSetupConn(conn, err, "multicast sender loopback")
	}
	return nil
}

// setupSenderSocket configures egress and loopback on a sender socket.
func setupSenderSocket(conn *net.UDPConn) error {
	err := setupSenderEgress(conn)
	if err != nil {
		return err
	}
	err = setupSenderLoopback(conn)
	if err != nil {
		return err
	}
	return nil
}

// DialMulticastSender returns a send-only multicast socket for domain.
//
// It binds an ephemeral wildcard port and targets the SPDP multicast
// group, so it never conflicts with multicast receivers. The caller owns
// the result and must Close it.
func DialMulticastSender(domain uint32) (*MulticastSender, error) {
	err := checkDomain(domain)
	if err != nil {
		return nil, err
	}
	port, err := MetatrafficMulticastPort(domain)
	if err != nil {
		return nil, fmt.Errorf("rtps: multicast sender port: %w", err)
	}
	group, err := spdpGroup()
	if err != nil {
		return nil, fmt.Errorf("rtps: multicast sender group: %w", err)
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: ephemeralPort})
	if err != nil {
		return nil, fmt.Errorf("rtps: multicast sender listen: %w", err)
	}
	err = setupSenderSocket(conn)
	if err != nil {
		return nil, err
	}
	return &MulticastSender{
		conn:   conn,
		target: &net.UDPAddr{IP: group, Port: port},
		domain: domain,
		port:   port,
	}, nil
}

// Domain returns the joined domain for a multicast sender.
func (m *MulticastSender) Domain() uint32 {
	return m.domain
}

// Port returns the SPDP multicast port for a multicast sender.
func (m *MulticastSender) Port() int {
	return m.port
}

// Target returns the SPDP multicast destination. The result is a copy
// owned by the caller.
func (m *MulticastSender) Target() *net.UDPAddr {
	return &net.UDPAddr{IP: append(net.IP(nil), m.target.IP...), Port: m.target.Port}
}

// Send multicasts b to the SPDP group for the joined domain. Empty and
// oversize payloads fail loudly. Use after Close reports ErrClosed.
func (m *MulticastSender) Send(b []byte) error {
	err := checkSend(b)
	if err != nil {
		return fmt.Errorf("rtps: multicast sender send: %w", err)
	}
	if m.isClosed() {
		return fmt.Errorf("rtps: multicast sender send: %w", ErrClosed)
	}
	err = writeDatagram(m.conn, m.target, b, "multicast sender send", "multicast sender", m.isClosed, nil)
	if err != nil {
		return err
	}
	return nil
}

// Close shuts a multicast sender. It is idempotent and safe for
// concurrent use; later data path calls report ErrClosed.
func (m *MulticastSender) Close() error {
	if !m.guard.tryClose() {
		return nil
	}
	err := m.conn.Close()
	if err != nil {
		return fmt.Errorf("rtps: multicast sender close: %w", err)
	}
	return nil
}

// isClosed reports whether Close has run for a multicast sender.
func (m *MulticastSender) isClosed() bool {
	return m.guard.isClosed()
}

// isAddrInUse reports whether err wraps address already in use.
func isAddrInUse(err error) bool {
	return errors.Is(err, syscall.EADDRINUSE)
}

// isTimeout reports whether err is a network timeout.
func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// setPollDeadline sets a short read deadline for context polling. A closed
// socket maps to ErrClosed so callers do not mistake teardown for IO.
func setPollDeadline(conn *net.UDPConn, op string, closed func() bool) error {
	err := conn.SetReadDeadline(time.Now().Add(socketPollInterval))
	if err != nil {
		if closed() {
			return fmt.Errorf("rtps: %s receive: %w", op, ErrClosed)
		}
		return fmt.Errorf("rtps: %s receive deadline: %w", op, err)
	}
	return nil
}

// mapReadError maps a datagram read failure to retry or fatal. Timeouts
// retry so the poll loop rechecks context state. A canceled context or a
// closed socket maps to its sentinel; other failures stay fatal.
func mapReadError(err error, ctx context.Context, op string, closed func() bool) (bool, error) {
	if closed() {
		return false, fmt.Errorf("rtps: %s receive: %w", op, ErrClosed)
	}
	cerr := ctx.Err()
	if cerr != nil {
		return false, fmt.Errorf("rtps: %s receive: %w", op, cerr)
	}
	if isTimeout(err) {
		return true, nil
	}
	return false, fmt.Errorf("rtps: %s receive: %w", op, err)
}

// interfaceAddrs carries one interface with its addresses for egress choice.
type interfaceAddrs struct {
	// name is the interface name for debugging.
	name string
	// flags carries up, multicast, and loopback bits.
	flags net.Flags
	// addrs holds the interface addresses.
	addrs []net.Addr
}

// pickMulticastEgress returns the IPv4 of the first up multicast-capable
// non-loopback interface, or nil when only loopback qualifies. Vendor
// participants join SPDP on the LAN interface, so multicast must leave
// through it to be seen.
func pickMulticastEgress(list []interfaceAddrs) net.IP {
	for _, ifc := range list {
		if ifc.flags&net.FlagUp == 0 || ifc.flags&net.FlagMulticast == 0 || ifc.flags&net.FlagLoopback != 0 {
			continue
		}

		for _, addr := range ifc.addrs {
			if ipNet, ok := addr.(*net.IPNet); ok {
				if v4 := ipNet.IP.To4(); v4 != nil {
					return v4
				}
			}
		}
	}

	return nil
}

// collectInterfaceAddrs lists interfaces with their addresses for egress.
func collectInterfaceAddrs() ([]interfaceAddrs, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("rtps: list interfaces: %w", err)
	}
	list := make([]interfaceAddrs, 0, len(ifs))
	for _, ifc := range ifs {
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		list = append(list, interfaceAddrs{name: ifc.Name, flags: ifc.Flags, addrs: addrs})
	}
	return list, nil
}

// applyEgressInterface pins conn multicast output to ip.
func applyEgressInterface(conn *net.UDPConn, ip net.IP) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("rtps: egress syscall conn: %w", err)
	}
	var opErr error
	ctrlErr := raw.Control(func(fd uintptr) {
		var addr [ipv4TailSize]byte
		copy(addr[:], ip.To4())
		opErr = syscall.SetsockoptInet4Addr(int(fd), syscall.IPPROTO_IP, syscall.IP_MULTICAST_IF, addr)
	})
	if ctrlErr != nil {
		return fmt.Errorf("rtps: egress control: %w", ctrlErr)
	}
	if opErr != nil {
		return fmt.Errorf("rtps: set egress interface: %w", opErr)
	}
	return nil
}

// setMulticastEgress pins multicast sends to the LAN interface when one
// exists. Without this the socket follows the host multicast route,
// which may point at a tunnel device the vendor participants ignore.
func setMulticastEgress(conn *net.UDPConn) error {
	list, err := collectInterfaceAddrs()
	if err != nil {
		return err
	}
	ip := pickMulticastEgress(list)
	if ip == nil {
		return nil
	}
	err = applyEgressInterface(conn, ip)
	if err != nil {
		return err
	}
	return nil
}

// AdvertiseIPv4 returns the LAN IPv4 to advertise in SPDP locators.
//
// It returns the first up multicast-capable non-loopback IPv4, matching
// the multicast egress choice, so peers route back to a reachable
// address. It falls back to loopback when no LAN interface exists.
func AdvertiseIPv4() net.IP {
	list, err := collectInterfaceAddrs()
	if err != nil {
		return net.ParseIP(LoopbackIPv4)
	}
	ip := pickMulticastEgress(list)
	if ip == nil {
		return net.ParseIP(LoopbackIPv4)
	}
	return ip
}
