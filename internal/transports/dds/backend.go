// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// Default announcement intervals for discovery loops.
const (
	// defaultSPDPInterval is the period between SPDP announcements.
	defaultSPDPInterval = 50 * time.Millisecond
	// defaultSEDPInterval is the period between SEDP announcements.
	defaultSEDPInterval = 50 * time.Millisecond
)

// Demux and transport constants shared by the backend loops.
const (
	// demuxQueueDepth bounds each demux channel without blocking senders.
	demuxQueueDepth = 128
	// loopbackHost is the unicast bootstrap host for same host discovery.
	// DATA and SEDP fanout send to discovered ports via loopback; SPDP
	// locators advertise the LAN address for future remote routes.
	loopbackHost = "127.0.0.1"
	// maxUDPPort is the largest valid UDP port, equal to MaxUint16.
	maxUDPPort = 0xFFFF
	// legacySPDPrefix routes legacy custom SPDP datagrams in the demux.
	legacySPDPrefix = "SPDP"
	// legacySEDPrefix routes legacy custom SEDP datagrams in the demux.
	legacySEDPrefix = "SEDP"
)

// Writer entity bytes for user DATA on the wire.
const (
	// writerEntityUserB12 is the user writer entity first byte.
	writerEntityUserB12 = 0x00
	// writerEntityUserB13 is the user writer entity second byte.
	writerEntityUserB13 = 0x00
	// writerEntityUserB14 is the user writer entity third byte.
	writerEntityUserB14 = 0x02
	// writerEntityUserB15 is the user writer entity fourth byte.
	writerEntityUserB15 = 0x03
	// sedpWriterEntityB14 is the SEDP builtin third byte for publications.
	sedpWriterEntityB14 = 0x02
	// sedpWriterEntityB14Cyclone is the SEDP builtin third byte CycloneDDS uses for publications.
	sedpWriterEntityB14Cyclone = 0x03
	// sedpWriterEntityB14Alt is the SEDP builtin third byte for subscriptions.
	sedpWriterEntityB14Alt = 0x04
)

// ErrDataPathNotWired reports that the publish and take path is not yet wired.
var ErrDataPathNotWired = errors.New("dds: data path not wired, see datapath.go")

// ErrNoRoute reports that publish has no discovered peer for the topic.
var ErrNoRoute = errors.New("dds: no route to peers")

// Clock abstracts time for discovery leases and announcement ticks.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// After waits for the given duration and reports the wake time.
	After(d time.Duration) <-chan time.Time
}

// systemClock is the standard library Clock implementation.
type systemClock struct{}

// SystemClock returns the standard library Clock.
func SystemClock() Clock {
	return systemClock{}
}

// Now returns the current wall clock time.
func (systemClock) Now() time.Time {
	return time.Now()
}

// After waits for the given duration using the standard library.
//
// It uses time.After for simplicity because discovery ticks use short
// intervals and each tick consumes its timer. When the run context ends
// before the timer fires, the timer lingers until it fires one interval
// later, which bounds the leak to one short timer per loop. Loops that
// need early timer release should use time.NewTimer plus Stop directly
// instead of this Clock method.
func (systemClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// UDPPeer abstracts a wildcard UDP peer for discovery transport.
type UDPPeer interface {
	// Write sends bytes to the peer.
	Write(b []byte) (int, error)
	// Close releases the peer handle.
	Close() error
}

// transportPeer wraps live unicast plus multicast sender.
type transportPeer struct {
	uni    *rtps.UnicastConn
	sender *rtps.MulticastSender
}

// Write sends bytes over the multicast discovery path.
func (p *transportPeer) Write(b []byte) (int, error) {
	if p.sender == nil {
		return 0, fmt.Errorf("dds: peer write without multicast: %w", ErrDataPathNotWired)
	}

	err := p.sender.Send(b)
	if err != nil {
		return 0, fmt.Errorf("dds: peer write: %w", err)
	}

	return len(b), nil
}

// Close releases both live sockets.
func (p *transportPeer) Close() error {
	var errs []error

	if p.uni != nil {
		err := p.uni.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if p.sender != nil {
		err := p.sender.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("dds: peer close: %w", errors.Join(errs...))
	}

	return nil
}

// makeDial returns a dial func bound to the resolved domain.
//
// The peer binds wildcard unicast plus a send-only multicast sender.
func makeDial(domain uint32) func(context.Context, string) (UDPPeer, error) {
	return func(ctx context.Context, addr string) (UDPPeer, error) {
		err := ctx.Err()
		if err != nil {
			return nil, fmt.Errorf("dds: dial peer %s: %w", addr, err)
		}

		uni, err := rtps.DialUnicast(ctx, domain)
		if err != nil {
			return nil, fmt.Errorf("dds: dial discovery peer: %w", err)
		}

		sender, err := rtps.DialMulticastSender(domain)
		if err != nil {
			cerr := uni.Close()
			if cerr != nil {
				err = errors.Join(err, cerr)
			}

			return nil, fmt.Errorf("dds: dial discovery peer: %w", err)
		}

		return &transportPeer{uni: uni, sender: sender}, nil
	}
}

// discardWriter satisfies NewPublisher without carrying traffic.
type discardWriter struct{}

// WriteFrame discards one frame and reports success.
func (discardWriter) WriteFrame(_ []byte) error {
	return nil
}

// dataItem carries one RTPS DATA payload with its writer GUID.
type dataItem struct {
	guid    WriterGUID
	seq     uint64
	payload []byte
}

// Backend is the DDS lifecycle and discovery plumbing.
//
// Lifecycle order follows the Backend contract: for sidecar based buses
// the process level Connect dials first, then JoinDiscovery, then the
// data path, then Close. JoinDiscovery dials unicast plus a send-only
// multicast sender on the resolved domain, so no receive bind exists and
// EADDRINUSE is impossible. Reception runs over unicast only: directed
// SPDP from peers such as CycloneDDS arrives on the advertised unicast
// metatraffic locator, and a unicast demultiplexer splits RTPS DATA by
// writer entity into SPDP builtin, SEDP builtin, and user paths. Close cancels the run context, shuts sockets, and waits.
// PublishBytes and TakeBytes use datapath Publisher per writer GUID with
// MarshalRTPSPayload embedded in rtps DATA sent to discovered unicast
// locators, and TakeFrom over a FrameSource fed by the demultiplexer.
// The struct is safe for concurrent use. The mutex must not be copied
// after first use; all exported getters self-lock with RLock while
// writers hold Lock.
type Backend struct {
	mu sync.RWMutex
	// cfg is the normalized sidecar configuration.
	cfg SidecarConfig
	// domainID is the resolved domain from DomainConfig.
	domainID uint32
	// joined is true after a successful JoinDiscovery.
	joined bool
	// closed is true after the first Close.
	closed bool
	// lastGen holds the liveliness watermark per participant.
	lastGen map[string]uint64
	// lastAlive holds the last aliveness per participant.
	lastAlive map[string]bool
	// leases holds the last report time per participant.
	leases map[string]time.Time
	// discoveryGen counts local SPDP announcements and remote changes.
	discoveryGen uint64
	// spdpCount counts SPDP loop ticks and sends.
	spdpCount uint64
	// sedpCount counts SEDP loop ticks and sends.
	sedpCount uint64
	// demuxErrors counts demux receive and decode failures for observability.
	demuxErrors uint64
	// typeHashMisses counts publishes with no known type hash for observability.
	typeHashMisses uint64
	// clock supplies time for leases and ticks.
	clock Clock
	// dial opens the loopback UDP peer plus multicast sender for discovery.
	dial func(ctx context.Context, addr string) (UDPPeer, error)
	// peer is the dialed discovery peer.
	peer UDPPeer
	// runCancel stops the announcement loops.
	runCancel context.CancelFunc
	// done closes on Close to unblock takes.
	done chan struct{}
	// runWG waits for the announcement loops.
	runWG sync.WaitGroup
	// spdpInterval is the SPDP tick period.
	spdpInterval time.Duration
	// sedpInterval is the SEDP tick period.
	sedpInterval time.Duration
	// uni carries SPDP plus SEDP plus DATA unicast.
	uni *rtps.UnicastConn
	// mcast carries send-only SPDP multicast.
	mcast *rtps.MulticastSender
	// mcastRx carries receive-only SPDP multicast.
	// It joins the standard group with reuse so vendor coexistence keeps
	// working and Close releases it alongside the unicast path.
	mcastRx *rtps.MulticastConn
	// mcastUserRx carries receive-only user-traffic multicast.
	// CycloneDDS multicasts user DATA and heartbeats on the domain user
	// port; without this join a participant depends entirely on
	// per-reader unicast, and a second local participant starves while
	// the first receives everything. Close releases it with mcastRx.
	mcastUserRx *rtps.MulticastConn
	// local is the local SPDP identity.
	local *LocalParticipant
	// remotes tracks remote SPDP participants.
	remotes *RemoteTable
	// sedp tracks local plus remote endpoints.
	sedp *SEDPIndex
	// receiver strips DATA envelopes on take.
	receiver *DataReceiver
	// publisher emits one DATA frame per publish.
	publisher *Publisher
	// pubGUID is the single writer GUID for publishes.
	pubGUID WriterGUID
	// rtpsSeq counts total publishes for observability and unit-test compat.
	// Actual DATA sequence numbers are per writer in rtpsSeqByWriter.
	rtpsSeq uint64
	// rtpsSeqByWriter is the next RTPS DATA sequence per writer GUID.
	rtpsSeqByWriter map[WriterGUID]uint64
	// routes maps remote prefix to unicast ports from SPDP.
	routes map[GUIDPrefix][]int
	// spdpCh carries demultiplexed SPDP bytes.
	spdpCh chan []byte
	// sedpCh carries demultiplexed SEDP bytes.
	sedpCh chan []byte
	// dataCh carries demultiplexed DATA frames.
	dataCh chan dataItem
	// repairHistory keeps sent user payloads for ACKNACK repair.
	repairHistory *userHistory
	// remoteTrack holds per-writer contiguity state for NACK base.
	// The base must be the first missing sequence, not highest seen
	// plus one: a cumulative ACK advances the peer past everything
	// below it, so an overshooting base abandons the gap forever.
	remoteTrack map[WriterGUID]*remoteTracker
	// sedpHighSeq tracks the highest SEDP DATA seq per remote builtin writer.
	sedpHighSeq map[WriterGUID]uint64
	// fragReasm reassembles DATA_FRAG samples.
	fragReasm *fragReassembler
}

// publishSnapshot carries publisher state for one publish without holding mu.
//
// It replaces the prior 5-tuple return so callers pass one value and the
// locking contract stays inside snapshotPublish.
type publishSnapshot struct {
	publisher  *Publisher
	sedp       *SEDPIndex
	domain     uint32
	live       bool
	routeCount int
}

// NewBackend validates the config and returns a lifecycle Backend.
func NewBackend(cfg SidecarConfig) (*Backend, error) {
	backend, err := NewBackendWithClock(cfg, SystemClock())
	if err != nil {
		return nil, err
	}

	return backend, nil
}

// NewBackendWithClock validates the config and returns a Backend with a clock.
func NewBackendWithClock(cfg SidecarConfig, clock Clock) (*Backend, error) {
	if clock == nil {
		return nil, errors.New("dds: clock is nil") //nolint:wrapcheck // new error, nothing to wrap
	}

	domainCfg := transports.DomainConfig{DomainID: cfg.DomainID}

	_, err := domainCfg.Resolve()
	if err != nil {
		return nil, fmt.Errorf("dds: resolve domain: %w", err)
	}

	side, err := NewSidecar(cfg)
	if err != nil {
		return nil, fmt.Errorf("dds: invalid backend config: %w", err)
	}

	normed := side.Config()

	return &Backend{
		cfg:             normed,
		domainID:        normed.DomainID,
		lastGen:         make(map[string]uint64),
		lastAlive:       make(map[string]bool),
		leases:          make(map[string]time.Time),
		clock:           clock,
		dial:            makeDial(normed.DomainID),
		spdpInterval:    defaultSPDPInterval,
		sedpInterval:    defaultSEDPInterval,
		routes:          make(map[GUIDPrefix][]int),
		rtpsSeq:         FirstSequenceNumber,
		rtpsSeqByWriter: make(map[WriterGUID]uint64),
	}, nil
}

// Config returns a copy of the normalized sidecar configuration.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) Config() SidecarConfig {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.cfg
}

// DomainID returns the resolved domain id.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) DomainID() uint32 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.domainID
}

// Joined reports whether JoinDiscovery has succeeded.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) Joined() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.joined
}

// Closed reports whether Close has been called.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) Closed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.closed
}

// DiscoveryGeneration returns the local SPDP announcement generation.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) DiscoveryGeneration() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.discoveryGen
}

// demuxErrorCount returns the demux failure count for observability.
//
// It self-locks with RLock and is safe for concurrent use. It stays
// unexported so the Backend contract keeps zero API change.
func (b *Backend) demuxErrorCount() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.demuxErrors
}

// checkJoinLocked reports whether join may proceed.
//
// The caller must hold b.mu (read or write). It checks the closed flag
// without locking. All other join and closed checks self-lock with RLock;
// this is the only helper that requires the caller to hold the lock, and
// its Locked suffix marks that contract.
func (b *Backend) checkJoinLocked() error {
	if b.closed {
		return fmt.Errorf("dds: join discovery after close: %w", transports.ErrBackendClosed)
	}

	return nil
}

// bumpDiscovery adds one to the discovery generation.
func (b *Backend) bumpDiscovery() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.discoveryGen++
}

// bumpSPDP counts one SPDP send plus one discovery tick.
func (b *Backend) bumpSPDP() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.spdpCount++
	b.discoveryGen++
}

// bumpSEDP counts one SEDP send.
func (b *Backend) bumpSEDP() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.sedpCount++
}

// bumpDemuxError counts one demux failure for observability.
func (b *Backend) bumpDemuxError() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.demuxErrors++
}

// bumpTypeHashMiss counts one publish with no known type hash.
func (b *Backend) bumpTypeHashMiss() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.typeHashMisses++
}

// typeHashMissCount returns the unknown type hash count for observability.
func (b *Backend) typeHashMissCount() uint64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.typeHashMisses
}

// isClosed reports whether Close has run.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) isClosed() bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.closed
}

// startStubLoopsLocked starts stub loops with the caller holding mu.
//
// It reads clock and intervals directly without locking and adds one
// WaitGroup count immediately before each go statement. The caller must
// hold mu so Close blocks until Adds complete. See startLiveLoopsLocked
// for the live counterpart.
func (b *Backend) startStubLoopsLocked(runCtx context.Context) {
	clock := b.clock
	spdpInterval := b.spdpInterval
	sedpInterval := b.sedpInterval

	b.runWG.Add(1)
	go b.spdpLoop(runCtx, clock, spdpInterval)
	b.runWG.Add(1)
	go b.sedpLoop(runCtx, clock, sedpInterval)
}

// snapshotIntervals returns SPDP and SEDP intervals under lock.
//
// It self-locks with RLock and is safe for concurrent use. The two
// intervals are independent so tests can tune SEDP without moving SPDP.
func (b *Backend) snapshotIntervals() (time.Duration, time.Duration) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.spdpInterval, b.sedpInterval
}

// startLiveLoopsLocked starts live loops with the caller holding mu.
//
// It adds one WaitGroup count immediately before each go statement so
// Close cannot Wait until all Adds complete.
func (b *Backend) startLiveLoopsLocked(runCtx context.Context) {
	b.runWG.Add(1)
	go b.spdpAnnounceLoop(runCtx)
	b.runWG.Add(1)
	go b.spdpListenLoop(runCtx)
	b.runWG.Add(1)
	go b.sedpAnnounceLoop(runCtx)
	b.runWG.Add(1)
	go b.sedpListenLoop(runCtx)
	b.runWG.Add(1)
	go b.demuxLoop(runCtx)
	b.runWG.Add(1)
	go b.userHeartbeatLoop(runCtx)
}

// userHeartbeatInterval spaces silent-writer availability heartbeats.
//
// A writer that published and went silent emits no DATA, so late joiners
// never learn its retained history without a heartbeat trigger. The loop
// advertises every local writer's [first-kept, last] range once per
// interval; matched readers NACK the gap and repair serves from history.
// The interval matches the SEDP resend cadence: prompt enough for tests,
// quiet enough for the fleet.
const userHeartbeatInterval = 1 * time.Second

// userHeartbeatLoop advertises retained writer history until context ends.
//
// It shares the run WaitGroup with the other live loops so Close waits
// for it. Each tick is best effort: a send failure logs and the next tick
// retries, so a transient send error never kills availability.
func (b *Backend) userHeartbeatLoop(ctx context.Context) {
	defer b.runWG.Done()

	ticker := time.NewTicker(userHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.advertiseWriterHistory(ctx)
		}
	}
}

// advertiseWriterHistory heartbeats every local writer's retained range.
//
// Writers with no retained samples are skipped: First 1 Last 0 is an
// invalid range and there is nothing to repair. Failures log loudly and
// skip the writer; the next tick retries.
func (b *Backend) advertiseWriterHistory(ctx context.Context) {
	err := ctx.Err()
	if err != nil {
		return
	}

	b.mu.RLock()
	sedpIdx := b.sedp
	joined := b.joined
	closed := b.closed
	b.mu.RUnlock()

	if sedpIdx == nil || !joined || closed {
		return
	}

	for _, ep := range sedpIdx.LocalEndpoints() {
		if ep.IsReader {
			continue
		}

		guid := writerGUIDFromEndpoint(ep.ID)
		first := b.repairHistoryFirst(guid)
		last := b.repairHistoryLast(guid)

		if last == 0 {
			continue
		}

		msg, err := b.buildHistoryHeartbeatMessage(guid, first, last)
		if err != nil {
			log.Printf("dds: user heartbeat build %x: %v", guid[:], err)
			continue
		}

		if err := b.sendUserControl(ctx, msg); err != nil {
			log.Printf("dds: user heartbeat send %x: %v", guid[:], err)
		}
	}
}

// buildHistoryHeartbeatMessage wraps one HEARTBEAT naming a retained range.
//
// First names the earliest kept seq so a late reader NACKing from 1 gets
// GAP for the evicted prefix plus repair for retained samples. Last names
// the newest seq so the reader learns everything available without
// waiting for the next DATA sample.
func (b *Backend) buildHistoryHeartbeatMessage(guid WriterGUID, first, last uint64) ([]byte, error) {
	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()

	if local == nil {
		return nil, fmt.Errorf("dds: history heartbeat without transport: %w", ErrDataPathNotWired)
	}

	var prefix [12]byte
	lg := local.GUID()
	copy(prefix[:], lg.Prefix[:])

	header := rtps.DefaultHeader(prefix)

	var writerID rtps.EntityID
	copy(writerID[:], guid[12:16])

	heartbeat := rtps.Heartbeat{
		ReaderID:      rtps.EntityUnknown,
		WriterID:      writerID,
		FirstSequence: first,
		LastSequence:  last,
		Count:         nextHeartbeatCount(),
	}

	hbRaw := rtps.RawSubmessage{
		ID:    rtps.SubmessageHeartbeat,
		Flags: rtps.FlagLittleEndian,
		Body:  heartbeat.Encode(true),
	}

	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{hbRaw})
	if err != nil {
		return nil, fmt.Errorf("dds: history heartbeat encode: %w", err)
	}

	return msg, nil
}

// closePeerBestEffort closes a dialed peer and logs failures.
//
// Join paths already fail for another reason, so peer close stays best
// effort with a log instead of a silent drop.
func (b *Backend) closePeerBestEffort(peer UDPPeer, reason string) {
	err := peer.Close()
	if err != nil {
		log.Printf("dds: join peer close after %s: %v", reason, err)
	}
}

// JoinDiscovery joins discovery and starts SPDP and SEDP announcement loops.
//
// For sidecar buses the process level Connect dials first, then
// JoinDiscovery dials unicast plus a send-only multicast sender on the
// resolved domain, then the data path, then Close. No multicast receive
// bind exists, so EADDRINUSE is impossible and reception runs over
// unicast only. The announcement plus listen loops outlive the
// JoinDiscovery call, so the run context detaches cancel from ctx but
// keeps its values. Close cancels the loops and shuts sockets.
// PublishBytes sends to discovered unicast locators and TakeBytes receives
// over the demultiplexer.
func (b *Backend) JoinDiscovery(ctx context.Context, _ transports.DiscoveryInfo) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: join discovery: %w", err)
	}

	b.mu.Lock()

	err = b.checkJoinLocked()
	if err != nil {
		b.mu.Unlock()

		return err
	}

	if b.joined {
		b.mu.Unlock()

		return nil
	}

	dial := b.dial
	addr := b.cfg.PeerAddress

	b.mu.Unlock()

	peer, err := dial(ctx, addr)
	if err != nil {
		return fmt.Errorf("dds: dial discovery peer: %w", err)
	}

	err = ctx.Err()
	if err != nil {
		b.closePeerBestEffort(peer, "cancel")

		return fmt.Errorf("dds: join discovery: %w", err)
	}

	return b.completeJoin(ctx, peer)
}

// completeJoin wires a dialed peer with the caller holding no lock.
//
// It locks to check closed and joined, installs stub or live state, adds
// WaitGroup counts next to goroutine starts under the same lock that
// Close uses, then unlocks. Wire bytes stay unchanged.
func (b *Backend) completeJoin(ctx context.Context, peer UDPPeer) error {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		b.closePeerBestEffort(peer, "close")

		return fmt.Errorf("dds: join discovery after close: %w", transports.ErrBackendClosed)
	}

	if b.joined {
		b.mu.Unlock()
		b.closePeerBestEffort(peer, "duplicate join")

		return nil
	}

	live, ok := peer.(*transportPeer)
	if !ok {
		runCtx := b.installStubLocked(peer, ctx)
		b.startStubLoopsLocked(runCtx)
		b.mu.Unlock()

		return nil
	}

	runCtx, err := b.installLiveLocked(peer, live, ctx)
	if err != nil {
		b.mu.Unlock()
		b.closePeerBestEffort(peer, "install failure")

		return err
	}

	b.startStubLoopsLocked(runCtx)
	b.startLiveLoopsLocked(runCtx)
	b.startMcastIngressLocked(runCtx)
	b.mu.Unlock()

	return nil
}

// installStubLocked wires a fake peer and returns the run context.
//
// The caller must hold b.mu and must call startStubLoopsLocked before
// unlocking so WaitGroup Adds happen under the same lock that Close uses.
func (b *Backend) installStubLocked(peer UDPPeer, ctx context.Context) context.Context {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	b.peer = peer
	b.joined = true
	b.done = make(chan struct{})
	b.runCancel = cancel

	return runCtx
}

// installLiveLocked wires live sockets plus tables and returns run context.
//
// The caller must hold b.mu and must call startStubLoopsLocked plus
// startLiveLoopsLocked before unlocking so WaitGroup Adds happen under
// the same lock that Close uses. Wire bytes are unchanged: the user
// writer entity suffix stays 00 00 02 03 and channels stay bounded.
func (b *Backend) installLiveLocked(
	peer UDPPeer,
	live *transportPeer,
	ctx context.Context,
) (context.Context, error) {
	domain := b.domainID

	local, err := NewLocalParticipant(domain, DefaultLeaseDuration)
	if err != nil {
		return nil, fmt.Errorf("dds: join local participant: %w", err)
	}

	// Announce the actually bound unicast ports, not participant-zero
	// ports: a second participant on one host binds a later slot, and
	// advertising slot zero would divert its metatraffic and user DATA
	// to the first participant's socket while this one starves.
	local.SetAnnouncePorts(live.uni.LocalPort(), live.uni.LocalPort())

	var sedpPrefix SEDPGUIDPrefix

	lguid := local.GUID()
	copy(sedpPrefix[:], lguid.Prefix[:])

	sedpIdx, err := NewSEDPIndex(sedpPrefix, domain)
	if err != nil {
		return nil, fmt.Errorf("dds: join sedp index: %w", err)
	}

	remotes := NewRemoteTable(nil)
	onChange := func(uint64) {
		b.bumpDiscovery()
	}
	remotes.SetOnChange(onChange)
	sedpIdx.SetOnChange(onChange)

	var pubGUID WriterGUID

	guid := local.GUID()
	copy(pubGUID[:12], guid.Prefix[:])
	pubGUID[12] = writerEntityUserB12
	pubGUID[13] = writerEntityUserB13
	pubGUID[14] = writerEntityUserB14
	pubGUID[15] = writerEntityUserB15

	publisher, err := NewPublisher(pubGUID, discardWriter{})
	if err != nil {
		return nil, fmt.Errorf("dds: join publisher: %w", err)
	}

	err = ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("dds: join discovery: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

	mcastRx := joinMulticastBestEffort(ctx, domain)
	mcastUserRx := joinUserMulticastBestEffort(ctx, domain)

	b.peer = peer
	b.uni = live.uni
	b.mcast = live.sender
	b.mcastRx = mcastRx
	b.mcastUserRx = mcastUserRx
	b.local = local
	b.remotes = remotes
	b.sedp = sedpIdx
	b.receiver = NewDataReceiver()
	b.receiver.SetSEDPIndex(sedpIdx)
	b.receiver.SetNackSender(b.sendPlainNack)
	b.publisher = publisher
	b.pubGUID = pubGUID
	b.spdpCh = make(chan []byte, demuxQueueDepth)
	b.sedpCh = make(chan []byte, demuxQueueDepth)
	b.dataCh = make(chan dataItem, demuxQueueDepth)
	b.repairHistory = newUserHistory()
	b.remoteTrack = make(map[WriterGUID]*remoteTracker)
	if b.rtpsSeqByWriter == nil {
		b.rtpsSeqByWriter = make(map[WriterGUID]uint64)
	}
	b.fragReasm = newFragReassembler(b.clock.Now)
	b.joined = true
	b.done = make(chan struct{})
	b.runCancel = cancel

	if b.routes == nil {
		b.routes = make(map[GUIDPrefix][]int)
	}

	if b.rtpsSeq == 0 {
		b.rtpsSeq = FirstSequenceNumber
	}

	if mcastRx != nil {
		log.Printf(
			"dds: multicast receive joined on domain %d group port %d user port %d unicast port %d",
			domain,
			mcastRx.Port(),
			userMulticastPortOrZero(mcastUserRx),
			live.uni.LocalPort(),
		)
	} else {
		log.Printf(
			"dds: multicast receive unavailable, running in send-only unicast-bootstrap mode on domain %d unicast port %d",
			domain,
			live.uni.LocalPort(),
		)
	}

	return runCtx, nil
}

// counterLoop runs one discovery counter until the run context ends.
//
// It shares the select plus closed check used by spdpLoop and sedpLoop so
// the two loops cannot drift. The onTick callback runs under mu and
// returns false to exit when closed.
func (b *Backend) counterLoop(ctx context.Context, clock Clock, interval time.Duration, onTick func() bool) {
	defer b.runWG.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-clock.After(interval):
			b.mu.Lock()

			keep := onTick()

			b.mu.Unlock()

			if !keep {
				return
			}
		}
	}
}

// spdpLoop announces the local participant until the run context ends.
func (b *Backend) spdpLoop(ctx context.Context, clock Clock, interval time.Duration) {
	b.counterLoop(ctx, clock, interval, func() bool {
		if b.closed {
			return false
		}

		b.discoveryGen++
		b.spdpCount++

		return true
	})
}

// sedpLoop asserts local endpoints until the run context ends.
func (b *Backend) sedpLoop(ctx context.Context, clock Clock, interval time.Duration) {
	b.counterLoop(ctx, clock, interval, func() bool {
		if b.closed {
			return false
		}

		b.sedpCount++

		return true
	})
}

// spdpBootstrapParticipants caps participant slots probed per SPDP announce.
// spdpBootstrapProbes caps total unicast bootstrap targets per SPDP announce.
// Each announce probes both metatraffic and user unicast for the first
// participant slots for the domain so two send-only peers discover each
// other without any multicast receive, and Weld reaches Cyclone whether it
// listens SPDP on meta or user unicast.
const spdpBootstrapProbes = 32

// spdpBootstrapParticipants is the participant range probed for bootstrap.
const spdpBootstrapParticipants = 16

// spdpSender sends SPDP packets over multicast plus unicast bootstrap.
type spdpSender struct {
	backend *Backend
}

// Send transmits one SPDP payload over multicast and unicast bootstrap.
func (s *spdpSender) Send(ctx context.Context, data []byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: spdp send context: %w", err)
	}

	sender, uni, domain := s.backend.snapshotSPDPSend()

	if sender == nil || uni == nil {
		return fmt.Errorf("dds: spdp send without transport: %w", ErrDataPathNotWired)
	}

	patched := patchSPDPPort(data, uni.LocalPort())

	multicastOK, last := sendSPDPMulticast(ctx, sender, patched)
	ports := s.backend.snapshotRoutes()

	routeSent, routeErr := sendSPDPRoutes(ctx, uni, patched, ports)
	if routeErr != nil {
		return routeErr
	}

	probeErr := sendSPDPProbe(ctx, uni, patched, domain, ports)
	if probeErr != nil {
		return probeErr
	}

	sent := routeSent
	if multicastOK {
		sent++
	}

	if sent == 0 && last != nil {
		return fmt.Errorf("dds: spdp send: %w", last)
	}

	s.backend.bumpSPDP()

	return nil
}

// sendSPDPMulticast sends one patched packet over multicast.
//
// It takes ctx so contextcheck sees the caller context propagate, even
// though the multicast sender is not context aware. Cancellation is
// checked before sending so a canceled announce does not block.
func sendSPDPMulticast(
	ctx context.Context,
	sender *rtps.MulticastSender,
	patched []byte,
) (bool, error) {
	err := ctx.Err()
	if err != nil {
		return false, fmt.Errorf("dds: spdp send context: %w", err)
	}

	err = sender.Send(patched) //nolint:contextcheck // multicast sender is fire-and-forget without context
	if err != nil {
		return false, fmt.Errorf("dds: spdp send multicast: %w", err)
	}

	return true, nil
}

// sendOneUnicastBestEffort sends one datagram and maps context cancel.
//
// It returns true when the datagram was sent. Context cancellation
// returns an error; other send failures return false plus nil so the
// caller continues best effort to remaining ports.
func sendOneUnicastBestEffort(
	ctx context.Context,
	uni *rtps.UnicastConn,
	port int,
	data []byte,
) (bool, error) {
	err := uni.SendTo(ctx, loopbackHost, port, data)
	if err != nil {
		cerr := ctx.Err()
		if cerr != nil {
			return false, fmt.Errorf("dds: spdp send context: %w", cerr)
		}

		return false, nil
	}

	return true, nil
}

// sendSPDPRoutes unicasts one packet to each known route.
func sendSPDPRoutes(
	ctx context.Context,
	uni *rtps.UnicastConn,
	patched []byte,
	ports []int,
) (int, error) {
	sent := 0

	for _, port := range ports {
		err := ctx.Err()
		if err != nil {
			return sent, fmt.Errorf("dds: spdp send context: %w", err)
		}

		ok, err := sendOneUnicastBestEffort(ctx, uni, port, patched)
		if err != nil {
			return sent, err
		}

		if ok {
			sent++
		}
	}

	return sent, nil
}

// sendSPDPProbe unicasts one packet to bootstrap participant ports.
func sendSPDPProbe(
	ctx context.Context,
	uni *rtps.UnicastConn,
	patched []byte,
	domain uint32,
	known []int,
) error {
	for _, port := range spdpBootstrapPorts(domain, uni.LocalPort()) {
		err := ctx.Err()
		if err != nil {
			return fmt.Errorf("dds: spdp send context: %w", err)
		}

		if slices.Contains(known, port) {
			continue
		}

		_, err = sendOneUnicastBestEffort(ctx, uni, port, patched)
		if err != nil {
			return err
		}
	}

	return nil
}

// snapshotSPDPSend returns sender, unicast, and domain under lock.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) snapshotSPDPSend() (*rtps.MulticastSender, *rtps.UnicastConn, uint32) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.mcast, b.uni, b.domainID
}

// spdpBootstrapPorts returns metatraffic plus user unicast ports to probe.
func spdpBootstrapPorts(domain uint32, selfPort int) []int {
	out := make([]int, 0, spdpBootstrapProbes)

	appendProbe := func(port int) {
		if port == selfPort {
			return
		}

		if slices.Contains(out, port) {
			return
		}

		out = append(out, port)
	}

	for i := range spdpBootstrapParticipants {
		meta, err := rtps.MetatrafficUnicastPort(domain, uint32(i))
		if err == nil {
			appendProbe(meta)
		}

		user, err := rtps.UsertrafficUnicastPort(domain, uint32(i))
		if err == nil {
			appendProbe(user)
		}
	}

	return out
}

// patchSPDPPort rewrites the locator ports to the live unicast port.
//
// SPDP templates carry participant zero ports; the backend patches them
// with the bound unicast port before each send so peers route back to
// the live socket. Out of range ports leave the packet unchanged.
func patchSPDPPort(packet []byte, uniPort int) []byte {
	if uniPort < 0 || uniPort > maxUDPPort {
		return append([]byte(nil), packet...)
	}

	return patchSPDPLocatorPorts(packet, uint16(uniPort))
}

// spdpReceiver receives SPDP packets over unicast and records routes.
type spdpReceiver struct {
	backend *Backend
}

// Receive blocks until one SPDP payload arrives or ctx ends.
func (r *spdpReceiver) Receive(ctx context.Context) ([]byte, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("dds: spdp receive context: %w", err)
	}

	ch, domain, local := r.backend.snapshotSPDP()

	if ch == nil {
		return nil, fmt.Errorf("dds: spdp receive without transport: %w", ErrDataPathNotWired)
	}

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			return nil, fmt.Errorf("dds: spdp receive context: %w", err)
		case data := <-ch:
			parsed, keep := r.backend.snoopAnnouncement(data, domain, local)
			if !keep {
				continue
			}

			if parsed == nil {
				return append([]byte(nil), data...), nil
			}

			return append([]byte(nil), data...), nil
		}
	}
}

// snapshotSPDP returns the SPDP unicast queue, domain, and local under lock.
//
// It self-locks with RLock and is safe for concurrent use. The returned
// channel is shared with the demux; callers must not close it.
func (b *Backend) snapshotSPDP() (chan []byte, uint32, *LocalParticipant) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.spdpCh, b.domainID, b.local
}

// snoopAnnouncement records the unicast routes from one SPDP packet.
//
// It returns the parsed announcement when the packet is usable and keep
// true when the caller should return the bytes to the listen loop. Parse
// failures return nil plus true so the table can ignore them. Domain
// mismatches and self packets return nil plus false so the receiver keeps
// waiting. Disposals delete the route.
func (b *Backend) snoopAnnouncement(
	data []byte,
	domain uint32,
	local *LocalParticipant,
) (*Announcement, bool) {
	ann, err := ParseAnnouncement(data)
	if err != nil {
		return nil, true
	}

	if ann.DomainID != domain {
		return nil, false
	}

	if local != nil && ann.GUID.Prefix == local.GUID().Prefix {
		return nil, false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil, true
	}

	if b.routes == nil {
		b.routes = make(map[GUIDPrefix][]int)
	}

	if !ann.Alive {
		delete(b.routes, ann.GUID.Prefix)
	} else if len(ann.Ports) != 0 {
		ports := make([]int, 0, len(ann.Ports))
		for _, port := range ann.Ports {
			ports = append(ports, int(port))
		}

		b.routes[ann.GUID.Prefix] = ports
	}

	out := ann

	return &out, true
}

// sedpSender sends SEDP DATA bytes over unicast to discovered peers.
type sedpSender struct {
	backend *Backend
}

// SendSEDP sends one serialized DATA sample to all discovered peers.
func (s *sedpSender) SendSEDP(ctx context.Context, data []byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: sedp send context: %w", err)
	}

	ports, uni := s.backend.snapshotRoutesAndUnicast()

	if len(ports) == 0 {
		return nil
	}

	if uni == nil {
		return fmt.Errorf("dds: sedp send without transport: %w", ErrDataPathNotWired)
	}

	cp := append([]byte(nil), data...)

	sent, last := sendToPorts(ctx, uni, ports, cp)
	if sent == 0 && last != nil {
		return fmt.Errorf("dds: sedp send: %w", last)
	}

	s.backend.bumpSEDP()

	return nil
}

// sedpReceiver receives SEDP DATA bytes from the demultiplexer.
type sedpReceiver struct {
	backend *Backend
}

// ReceiveSEDP blocks until one serialized DATA sample arrives.
func (r *sedpReceiver) ReceiveSEDP(ctx context.Context) ([]byte, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("dds: sedp receive context: %w", err)
	}

	r.backend.mu.RLock()
	ch := r.backend.sedpCh
	domain := r.backend.domainID
	r.backend.mu.RUnlock()

	if ch == nil {
		return nil, fmt.Errorf("dds: sedp receive without transport: %w", ErrDataPathNotWired)
	}

	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			return nil, fmt.Errorf("dds: sedp receive context: %w", err)
		case data := <-ch:
			sedpTracef("receiver recv wire len=%d local-domain=%d", len(data), domain)
			eps, err := ParseSEDPDATAs(data)
			if err != nil {
				sedpTracef("receiver parse reject len=%d err=%v", len(data), err)
				continue
			}

			forward, firstGUID, firstROS := sedpReceiveForward(eps, domain)
			if !forward {
				continue
			}

			sedpTracef("receiver forward guid=%s ros=%q count=%d len=%d", firstGUID, firstROS, len(eps), len(data))
			return append([]byte(nil), data...), nil
		}
	}
}

// sedpReceiveForward filters parsed endpoints by domain and names the first.
func sedpReceiveForward(eps []SEDPEndpoint, domain uint32) (bool, string, string) {
	forward := false
	var firstGUID string
	var firstROS string
	for _, ep := range eps {
		sedpTracef("receiver parse accept guid=%s ros=%q dds=%q typeDDS=%q hash=%q domain=%d reader=%v", ep.ID.String(), ep.ROSTopic, ep.DDSTopic, ep.TypeDDS, ep.TypeHash, ep.DomainID, ep.IsReader)
		if firstGUID == "" {
			firstGUID = ep.ID.String()
			firstROS = ep.ROSTopic
		}
		if ep.DomainID == 0 || ep.DomainID == domain {
			forward = true
		} else {
			sedpTracef("receiver domain skip guid=%s ep-domain=%d local-domain=%d", ep.ID.String(), ep.DomainID, domain)
		}
	}
	return forward, firstGUID, firstROS
}

// frameSink embeds RTPS payloads in DATA submessages addressed to peers.
type frameSink struct {
	backend *Backend
}

// WriteFrame accepts one RTPS payload for the given writer GUID.
func (s *frameSink) WriteFrame(ctx context.Context, guid WriterGUID, payload []byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: frame sink context: %w", err)
	}

	if guid != s.backend.publisherGUID() {
		return fmt.Errorf("dds: frame sink unknown writer: %w", ErrInvalidFrame)
	}

	ports := s.backend.snapshotRoutes()

	if len(ports) == 0 {
		return fmt.Errorf("dds: frame sink without route: %w", ErrNoRoute)
	}

	err = s.backend.sendData(ctx, guid, payload, ports)
	if err != nil {
		return err
	}

	return nil
}

// WriteFrag accepts one fragment and forwards to the Backend.
func (s *frameSink) WriteFrag(ctx context.Context, guid WriterGUID, seq uint64, fragNum, total uint32, fragmentSize uint16, sampleSize uint32, data []byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: frame sink context: %w", err)
	}

	if guid != s.backend.publisherGUID() {
		return fmt.Errorf("dds: frame sink unknown writer: %w", ErrInvalidFrame)
	}

	err = s.backend.WriteFrag(ctx, guid, seq, fragNum, total, fragmentSize, sampleSize, data)
	if err != nil {
		return err
	}

	return nil
}

// frameSource supplies RTPS DATA payloads from the demultiplexer.
type frameSource struct {
	backend *Backend
}

// ReadFrame returns the next RTPS payload for its writer GUID.
//
// The sequence is the RTPS DATA submessage number, zero when the queue
// predates sequence tracking. Plain vendor samples need it for gap
// NACKs; framed takes ignore it in favor of the frame header.
func (s *frameSource) ReadFrame(ctx context.Context) (WriterGUID, uint64, []byte, error) {
	var empty WriterGUID

	err := ctx.Err()
	if err != nil {
		return empty, 0, nil, fmt.Errorf("dds: frame source context: %w", err)
	}

	s.backend.mu.RLock()
	ch := s.backend.dataCh
	done := s.backend.done
	s.backend.mu.RUnlock()

	if ch == nil {
		return empty, 0, nil, fmt.Errorf("dds: frame source without transport: %w", ErrDataPathNotWired)
	}

	if done == nil {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			return empty, 0, nil, fmt.Errorf("dds: frame source context: %w", err)
		case item := <-ch:
			return item.guid, item.seq, item.payload, nil
		}
	}

	select {
	case <-ctx.Done():
		err := ctx.Err()
		return empty, 0, nil, fmt.Errorf("dds: frame source context: %w", err)
	case <-done:
		return empty, 0, nil, fmt.Errorf("dds: frame source stopped: %w", transports.ErrBackendClosed)
	case item := <-ch:
		return item.guid, item.seq, item.payload, nil
	}
}

// publisherGUID returns the writer GUID under lock.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) publisherGUID() WriterGUID {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.pubGUID
}

// snapshotRoutes returns a copy of discovered unicast ports.
//
// It self-locks with RLock and returns a fresh slice on each call. The
// copy protects the internal routes map, matching the VisiblePeers copy
// rule: callers own the returned slice and must not retain internal
// storage. Ports from all peers flatten into one deduped slice so SEDP
// and DATA fan out to both metatraffic and default unicast.
func (b *Backend) snapshotRoutes() []int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return flattenRoutes(b.routes)
}

// snapshotRoutesAndUnicast returns routes plus unicast under one lock.
//
// It self-locks with RLock and is safe for concurrent use. Returning
// both together avoids a TOCTOU gap between separate snapshots.
func (b *Backend) snapshotRoutesAndUnicast() ([]int, *rtps.UnicastConn) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return flattenRoutes(b.routes), b.uni
}

// flattenRoutes flattens per peer port lists into one deduped slice.
//
// It is a pure helper with no locking; callers pass the routes map while
// holding b.mu. First sight order wins and duplicates collapse so each UDP
// port is probed once.
func flattenRoutes(routes map[GUIDPrefix][]int) []int {
	seen := make(map[int]struct{})
	ports := make([]int, 0, len(routes)*2)

	for _, list := range routes {
		for _, port := range list {
			if _, ok := seen[port]; ok {
				continue
			}

			seen[port] = struct{}{}
			ports = append(ports, port)
		}
	}

	return ports
}

// snapshotUnicast returns the unicast connection under lock.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) snapshotUnicast() *rtps.UnicastConn {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.uni
}

// sendToPorts unicasts bytes to each port best effort without holding mu.
//
// It is the single shared helper for SEDP and DATA unicast fanout.
// Context cancellation aborts with an error; other send failures are
// recorded as last and skipped so remaining ports still receive.
func sendToPorts(ctx context.Context, uni *rtps.UnicastConn, ports []int, data []byte) (int, error) {
	sent := 0
	var last error

	for _, port := range ports {
		err := uni.SendTo(ctx, loopbackHost, port, data)
		if err != nil {
			cerr := ctx.Err()
			if cerr != nil {
				return sent, fmt.Errorf("dds: send ports context: %w", cerr)
			}

			last = err

			continue
		}

		sent++
	}

	return sent, last
}

// buildDataMessage wraps payload in one RTPS DATA datagram.
//
// The datagram carries one DATA submessage plus one HEARTBEAT so reliable
// readers such as CycloneDDS learn the sequence bounds and can deliver
// the sample. Without a heartbeat a single best-effort send from a
// reliable writer arrives as out-of-sync with no readers and is dropped.
// First comes from history when available, else 1, so evicted prefix is
// not claimed. Sequence encoding stays high word then low word per RTPS.
func (b *Backend) buildDataMessage(
	guid WriterGUID,
	payload []byte,
	seq uint64,
) ([]byte, error) {
	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()

	if local == nil {
		return nil, fmt.Errorf("dds: send data without transport: %w", ErrDataPathNotWired)
	}

	first := b.repairHistoryFirst(guid)
	if first == 0 {
		first = FirstSequenceNumber
	}

	var prefix [12]byte

	lg := local.GUID()
	copy(prefix[:], lg.Prefix[:])

	header := rtps.DefaultHeader(prefix)

	var writerID rtps.EntityID

	copy(writerID[:], guid[12:16])

	data := rtps.Data{
		ReaderID:       rtps.EntityUnknown,
		WriterID:       writerID,
		SequenceNumber: seq,
		Payload:        payload,
	}

	raw, err := data.Submessage(true)
	if err != nil {
		return nil, fmt.Errorf("dds: send data submessage: %w", err)
	}

	heartbeat := rtps.Heartbeat{
		ReaderID:      rtps.EntityUnknown,
		WriterID:      writerID,
		FirstSequence: first,
		LastSequence:  seq,
		Count:         nextHeartbeatCount(),
	}

	hbBody := heartbeat.Encode(true)

	hbRaw := rtps.RawSubmessage{
		ID:    rtps.SubmessageHeartbeat,
		Flags: rtps.FlagLittleEndian,
		Body:  hbBody,
	}

	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{raw, hbRaw})
	if err != nil {
		return nil, fmt.Errorf("dds: send data encode: %w", err)
	}

	return msg, nil
}

// buildHeartbeatMessage wraps one HEARTBEAT in an RTPS datagram.
//
// It mirrors the trailing heartbeat that buildDataMessage appends after
// DATA so reliable readers complete samples instead of waiting for a
// heartbeat that never comes. First and Last both name seq: the heartbeat
// advertises exactly the sample just sent as available.
func (b *Backend) buildHeartbeatMessage(guid WriterGUID, seq uint64) ([]byte, error) {
	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()

	if local == nil {
		return nil, fmt.Errorf("dds: send heartbeat without transport: %w", ErrDataPathNotWired)
	}

	var prefix [12]byte
	lg := local.GUID()
	copy(prefix[:], lg.Prefix[:])

	header := rtps.DefaultHeader(prefix)

	var writerID rtps.EntityID
	copy(writerID[:], guid[12:16])

	heartbeat := rtps.Heartbeat{
		ReaderID:      rtps.EntityUnknown,
		WriterID:      writerID,
		FirstSequence: seq,
		LastSequence:  seq,
		Count:         nextHeartbeatCount(),
	}

	hbBody := heartbeat.Encode(true)

	hbRaw := rtps.RawSubmessage{
		ID:    rtps.SubmessageHeartbeat,
		Flags: rtps.FlagLittleEndian,
		Body:  hbBody,
	}

	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{hbRaw})
	if err != nil {
		return nil, fmt.Errorf("dds: send heartbeat encode: %w", err)
	}

	return msg, nil
}

// buildFragMessage wraps one fragment in one RTPS DATA_FRAG datagram.
//
//nolint:unparam // total documents the fragment run for callers though the wire derives from sample size
func (b *Backend) buildFragMessage(guid WriterGUID, seq uint64, fragNum, total uint32, fragmentSize uint16, sampleSize uint32, fragData []byte) ([]byte, error) {
	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()

	if local == nil {
		return nil, fmt.Errorf("dds: send frag without transport: %w", ErrDataPathNotWired)
	}

	var prefix [12]byte
	lg := local.GUID()
	copy(prefix[:], lg.Prefix[:])

	header := rtps.DefaultHeader(prefix)

	var writerID rtps.EntityID
	copy(writerID[:], guid[12:16])

	body := encodeFragBody(rtps.EntityUnknown, writerID, seq, fragNum, fragmentSize, sampleSize, fragData)

	raw := rtps.RawSubmessage{
		ID:    rtps.SubmessageDataFrag,
		Flags: rtps.FlagLittleEndian,
		Body:  body,
	}

	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{raw})
	if err != nil {
		return nil, fmt.Errorf("dds: send frag encode: %w", err)
	}

	return msg, nil
}

// nextRTPSSeqFor claims the next RTPS DATA sequence for one writer.
//
// Each DDS writer needs its own contiguous sequence starting at 1.
// Sharing one global counter across writers leaves per-writer gaps that
// reliable ROS readers NACK while waiting, so mixed service callers
// stall. The map keeps contiguity per writer; rtpsSeq still counts
// total publishes for observability and unit-test compat.
func (b *Backend) nextRTPSSeqFor(guid WriterGUID) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rtpsSeqByWriter == nil {
		b.rtpsSeqByWriter = make(map[WriterGUID]uint64)
	}
	seq, ok := b.rtpsSeqByWriter[guid]
	if !ok || seq == 0 {
		seq = FirstSequenceNumber
	}
	b.rtpsSeqByWriter[guid] = seq + 1
	if b.rtpsSeqByWriter[guid] == 0 {
		b.rtpsSeqByWriter[guid] = FirstSequenceNumber
	}
	// Compat: keep the global counting for tests that assert it.
	b.rtpsSeq++
	if b.rtpsSeq == 0 {
		b.rtpsSeq = FirstSequenceNumber
	}
	return seq
}

// sendData embeds payload in RTPS DATA and unicasts to each port.
//
// It stores the payload in history for ACKNACK repair before sending, so
// a NACK arriving right after the send still finds the sample.
func (b *Backend) sendData(
	ctx context.Context,
	guid WriterGUID,
	payload []byte,
	ports []int,
) error {
	uni := b.snapshotUnicast()

	if uni == nil {
		return fmt.Errorf("dds: send data without transport: %w", ErrDataPathNotWired)
	}

	seq := b.nextRTPSSeqFor(guid)

	if b.repairHistory != nil {
		b.repairHistory.store(guid, seq, payload)
	}

	msg, err := b.buildDataMessage(guid, payload, seq)
	if err != nil {
		return err
	}

	sent, last := sendToPorts(ctx, uni, ports, msg)
	if sent > 0 {
		return nil
	}

	if last != nil {
		closed := b.isClosed()
		if errors.Is(last, rtps.ErrClosed) || closed {
			return fmt.Errorf("dds: send data after close: %w", transports.ErrBackendClosed)
		}

		return fmt.Errorf("dds: send data: %w", last)
	}

	return fmt.Errorf("dds: send data without route: %w", ErrNoRoute)
}

// WriteFrag accepts one fragment for the given writer GUID and seq.
func (b *Backend) WriteFrag(ctx context.Context, guid WriterGUID, seq uint64, fragNum, total uint32, fragmentSize uint16, sampleSize uint32, data []byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: frag sink context: %w", err)
	}

	if err = b.validateWriteFrag(guid, seq, fragNum, total, fragmentSize, sampleSize, data); err != nil {
		return err
	}

	ports := b.snapshotRoutes()
	if len(ports) == 0 {
		return fmt.Errorf("dds: frag sink without route: %w", ErrNoRoute)
	}

	uni := b.snapshotUnicast()
	if uni == nil {
		return fmt.Errorf("dds: frag sink without transport: %w", ErrDataPathNotWired)
	}

	msg, err := b.buildFragMessage(guid, seq, fragNum, total, fragmentSize, sampleSize, data)
	if err != nil {
		return err
	}

	return reportFragSend(ctx, b, uni, ports, msg)
}

// validateWriteFrag checks one fragment request without sending.
func (b *Backend) validateWriteFrag(guid WriterGUID, seq uint64, fragNum, total uint32, fragmentSize uint16, sampleSize uint32, data []byte) error {
	if seq == 0 {
		return fmt.Errorf("dds: frag sink sequence 0: %w", ErrInvalidFrame)
	}
	if guid != b.publisherGUID() {
		return fmt.Errorf("dds: frag sink unknown writer: %w", ErrInvalidFrame)
	}
	wantTotal, err := checkFragBounds(sampleSize, fragmentSize)
	if err != nil {
		return err
	}
	if wantTotal != total {
		return fmt.Errorf("dds: frag sink total %d mismatch want %d: %w", total, wantTotal, ErrInvalidFrame)
	}
	if fragNum == 0 || fragNum > total {
		return fmt.Errorf("dds: frag sink frag %d out of %d: %w", fragNum, total, ErrInvalidFrame)
	}
	if len(data) == 0 {
		return fmt.Errorf("dds: frag sink empty data: %w", ErrInvalidFrame)
	}
	wantLen := uint64(fragmentSize)
	if fragNum == total {
		rem := uint64(sampleSize) - uint64(total-1)*uint64(fragmentSize)
		wantLen = rem
	}
	if uint64(len(data)) != wantLen {
		return fmt.Errorf("dds: frag sink data %d want %d: %w", len(data), wantLen, ErrInvalidFrame)
	}
	return nil
}

// reportFragSend sends one fragment message and maps send failures.
func reportFragSend(ctx context.Context, b *Backend, uni *rtps.UnicastConn, ports []int, msg []byte) error {
	sent, last := sendToPorts(ctx, uni, ports, msg)
	if sent > 0 {
		return nil
	}
	if last != nil {
		closed := b.isClosed()
		if errors.Is(last, rtps.ErrClosed) || closed {
			return fmt.Errorf("dds: send frag after close: %w", transports.ErrBackendClosed)
		}
		return fmt.Errorf("dds: send frag: %w", last)
	}
	return fmt.Errorf("dds: send frag without route: %w", ErrNoRoute)
}

// sendPlainFrags emits one large plain sample as ordered DATA_FRAG.
func (b *Backend) sendPlainFrags(ctx context.Context, guid WriterGUID, sample []byte, sampleSize uint32, fragmentSize uint16, total uint32, ports []int) error {
	uni := b.snapshotUnicast()
	if uni == nil {
		return fmt.Errorf("dds: send frag without transport: %w", ErrDataPathNotWired)
	}

	seq := b.claimPlainFragSeq(guid)

	if b.repairHistory != nil {
		b.repairHistory.store(guid, seq, sample)
	}

	err := b.sendPlainFragRun(ctx, uni, ports, guid, seq, sample, sampleSize, fragmentSize, total)
	if err != nil {
		return err
	}

	b.commitPlainFragSeq(guid, seq)

	return b.sendPlainFragHeartbeat(ctx, uni, ports, guid, seq)
}

// claimPlainFragSeq peeks the next DATA sequence for one writer.
func (b *Backend) claimPlainFragSeq(guid WriterGUID) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.rtpsSeqByWriter == nil {
		b.rtpsSeqByWriter = make(map[WriterGUID]uint64)
	}
	seq, ok := b.rtpsSeqByWriter[guid]
	if !ok || seq == 0 {
		seq = FirstSequenceNumber
	}
	return seq
}

// commitPlainFragSeq advances the DATA sequence after a successful run.
func (b *Backend) commitPlainFragSeq(guid WriterGUID, seq uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if cur, ok := b.rtpsSeqByWriter[guid]; !ok || cur == seq {
		b.rtpsSeqByWriter[guid] = seq + 1
		if b.rtpsSeqByWriter[guid] == 0 {
			b.rtpsSeqByWriter[guid] = FirstSequenceNumber
		}
	}
	// Compat: keep global counting.
	b.rtpsSeq++
	if b.rtpsSeq == 0 {
		b.rtpsSeq = FirstSequenceNumber
	}
}

// sendPlainFragRun emits each DATA_FRAG in order.
func (b *Backend) sendPlainFragRun(ctx context.Context, uni *rtps.UnicastConn, ports []int, guid WriterGUID, seq uint64, sample []byte, sampleSize uint32, fragmentSize uint16, total uint32) error {
	for fragNum := uint32(1); fragNum <= total; fragNum++ {
		err := ctx.Err()
		if err != nil {
			return fmt.Errorf("dds: send frag context: %w", err)
		}
		fragData := fragSlice(sample, fragNum, total, fragmentSize)
		msg, err := b.buildFragMessage(guid, seq, fragNum, total, fragmentSize, sampleSize, fragData)
		if err != nil {
			return err
		}
		err = reportFragSend(ctx, b, uni, ports, msg)
		if err != nil {
			return err
		}
	}
	return nil
}

// sendPlainFragHeartbeat emits the terminal heartbeat for one sample.
func (b *Backend) sendPlainFragHeartbeat(ctx context.Context, uni *rtps.UnicastConn, ports []int, guid WriterGUID, seq uint64) error {
	hb, err := b.buildHeartbeatMessage(guid, seq)
	if err != nil {
		return err
	}
	sent, last := sendToPorts(ctx, uni, ports, hb)
	if sent > 0 {
		return nil
	}
	if last != nil {
		closed := b.isClosed()
		if errors.Is(last, rtps.ErrClosed) || closed {
			return fmt.Errorf("dds: send frag heartbeat after close: %w", transports.ErrBackendClosed)
		}
		return fmt.Errorf("dds: send frag heartbeat: %w", last)
	}
	return fmt.Errorf("dds: send frag heartbeat without route: %w", ErrNoRoute)
}

// logLoopError logs a loop exit unless it was caused by context cancel or Close.
//
// It shares the error mapping used by all announce and listen loops so
// silent drops become visible logs without changing wire behavior.
func (b *Backend) logLoopError(ctx context.Context, err error, name string) {
	if err == nil {
		return
	}

	cerr := ctx.Err()
	if cerr != nil && errors.Is(err, cerr) {
		return
	}

	if b.isClosed() {
		return
	}

	log.Printf("dds: %s loop ended: %v", name, err)
}

// snapshotLocalInterval returns the local participant plus SPDP interval.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) snapshotLocalInterval() (*LocalParticipant, time.Duration) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.local, b.spdpInterval
}

// snapshotRemotesInterval returns the remote table plus SPDP interval.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) snapshotRemotesInterval() (*RemoteTable, time.Duration) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.remotes, b.spdpInterval
}

// snapshotSEDPIndex returns the SEDP index under lock.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) snapshotSEDPIndex() *SEDPIndex {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.sedp
}

// runSEDPLoop runs one SEDP listen loop with shared snapshot plus logging.
func (b *Backend) runSEDPLoop(ctx context.Context, name string, fn func(idx *SEDPIndex) error) {
	defer b.runWG.Done()

	idx := b.snapshotSEDPIndex()
	if idx == nil {
		return
	}

	err := fn(idx)
	b.logLoopError(ctx, err, name)
}

// spdpAnnounceLoop sends SPDP announces until the run context ends.
func (b *Backend) spdpAnnounceLoop(ctx context.Context) {
	defer b.runWG.Done()

	local, interval := b.snapshotLocalInterval()

	if local == nil {
		return
	}

	err := local.AnnounceLoop(ctx, &spdpSender{backend: b}, interval)
	b.logLoopError(ctx, err, "spdp announce")
}

// spdpListenLoop receives SPDP packets until the run context ends.
func (b *Backend) spdpListenLoop(ctx context.Context) {
	defer b.runWG.Done()

	remotes, interval := b.snapshotRemotesInterval()

	if remotes == nil {
		return
	}

	err := remotes.ListenLoop(ctx, &spdpReceiver{backend: b}, interval)
	b.logLoopError(ctx, err, "spdp listen")
}

// sedpAnnounceLoop publishes local endpoints until the run context ends.
func (b *Backend) sedpAnnounceLoop(ctx context.Context) {
	defer b.runWG.Done()

	idx := b.snapshotSEDPIndex()
	if idx == nil {
		return
	}

	err := b.announceWithRoutes(ctx, idx)
	b.logLoopError(ctx, err, "sedp announce")
}

// sortedRoutesSnapshot returns sorted discovered unicast ports for change detection.
func (b *Backend) sortedRoutesSnapshot() []int {
	ports := b.snapshotRoutes()
	slices.Sort(ports)

	return ports
}

// announceWithRoutes re-announces locals when locals or routes change.
//
// It snapshots locals plus sorted routes, sends the initial locals when a
// route exists, then polls every sedpAnnouncePollInterval and re-sends when
// either snapshot differs. It also resends every second with fresh DATA
// sequences and HEARTBEATs so reliable peers that dropped the first sample
// still match. Route tracking matters because sedpSender drops announcements
// while no SPDP route exists: endpoints declared before discovery would
// otherwise never reach late peers. Sends run outside the index lock.
// Skipping sends while routes are empty preserves per-writer SEDP DATA
// sequence continuity: SerializeSEDPDATA consumes one sequence per
// sample from the writer stream, and CycloneDDS holds seq 2 until seq 1
// arrives, so a seq 1 dropped for no route would stall matching until
// expiry.
func (b *Backend) announceWithRoutes(ctx context.Context, idx *SEDPIndex) error {
	if idx == nil {
		return fmt.Errorf("dds: sedp announce nil index: %w", ErrSEDPInvalidEndpoint)
	}

	if ctx == nil {
		return fmt.Errorf("dds: sedp announce nil context: %w", ErrSEDPInvalidEndpoint)
	}

	err := ctx.Err()
	if err != nil {
		return err //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
	}

	sender := &sedpSender{backend: b}

	lastLocals := idx.LocalEndpoints()
	lastRoutes := b.sortedRoutesSnapshot()

	if len(lastRoutes) != 0 {
		err = sedpAnnounceSnapshot(ctx, sender, lastLocals)
		if err != nil {
			return sedpMapLoopError(ctx, fmt.Errorf("dds: sedp announce snapshot: %w", err))
		}
	}

	lastSend := time.Now()

	ticker := time.NewTicker(sedpAnnouncePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err() //nolint:wrapcheck // cancel semantics require unwrapped ctx.Err
		case <-ticker.C:
			var err error
			lastLocals, lastRoutes, lastSend, err = b.announceTick(ctx, sender, idx, lastLocals, lastRoutes, lastSend)
			if err != nil {
				return err
			}
		}
	}
}

// announceTick checks routes and announces when endpoints or routes change.
func (b *Backend) announceTick(ctx context.Context, sender *sedpSender, idx *SEDPIndex, lastLocals []SEDPEndpoint, lastRoutes []int, lastSend time.Time) ([]SEDPEndpoint, []int, time.Time, error) {
	currentLocals := idx.LocalEndpoints()
	currentRoutes := b.sortedRoutesSnapshot()
	if len(currentRoutes) == 0 {
		return currentLocals, currentRoutes, lastSend, nil
	}
	if sedpShouldSkipResend(lastLocals, currentLocals, lastRoutes, currentRoutes, lastSend) {
		return lastLocals, lastRoutes, lastSend, nil
	}
	err := sedpAnnounceSnapshot(ctx, sender, currentLocals)
	if err != nil {
		return lastLocals, lastRoutes, lastSend, sedpMapLoopError(ctx, fmt.Errorf("dds: sedp announce snapshot: %w", err))
	}
	return currentLocals, currentRoutes, time.Now(), nil
}

// sedpShouldSkipResend reports whether the periodic resend can be skipped.
func sedpShouldSkipResend(lastLocals, currentLocals []SEDPEndpoint, lastRoutes, currentRoutes []int, lastSend time.Time) bool {
	return sedpEndpointsSlicesEqual(lastLocals, currentLocals) && slices.Equal(lastRoutes, currentRoutes) && time.Since(lastSend) < sedpAnnounceResendInterval
}

// sedpListenLoop receives SEDP samples until the run context ends.
func (b *Backend) sedpListenLoop(ctx context.Context) {
	b.runSEDPLoop(ctx, "sedp listen", func(idx *SEDPIndex) error {
		return idx.ListenLoop(ctx, &sedpReceiver{backend: b})
	})
}

// demuxLoop splits unicast datagrams into SPDP versus SEDP versus DATA queues.
//
// Legacy custom SPDP and SEDP prefixes route directly for compatibility
// with the current wire. RTPS DATA routes by writer entity: the SPDP
// builtin writer 0x000100c2 goes to the SPDP path, the SEDP builtin
// writers 0x000002c2, 0x000003c2, and 0x000004c2 go to the SEDP path,
// and all other writers go to the user data path.
func (b *Backend) demuxLoop(ctx context.Context) {
	defer b.runWG.Done()

	for {
		err := ctx.Err()
		if err != nil {
			return
		}

		uni, spdpCh, sedpCh, dataCh, ok := b.snapshotDemux()
		if !ok {
			return
		}

		data, ok := b.receiveDemux(ctx, uni)
		if !ok {
			return
		}

		if data == nil {
			continue
		}

		if bytes.HasPrefix(data, []byte(legacySEDPrefix)) {
			b.enqueueSEDP(ctx, sedpCh, data)

			continue
		}

		if bytes.HasPrefix(data, []byte(legacySPDPrefix)) {
			b.enqueueSPDP(ctx, spdpCh, data)

			continue
		}

		if !bytes.HasPrefix(data, []byte(rtps.Magic)) {
			b.bumpDemuxError()
			log.Printf("dds: demux dropped %d non-RTPS bytes", len(data))

			continue
		}

		b.dispatchRTPS(ctx, data, spdpCh, sedpCh, dataCh)
	}
}

// snapshotDemux returns unicast plus queues under lock.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) snapshotDemux() (*rtps.UnicastConn, chan []byte, chan []byte, chan dataItem, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.uni == nil || b.spdpCh == nil || b.sedpCh == nil || b.dataCh == nil {
		return nil, nil, nil, nil, false
	}

	return b.uni, b.spdpCh, b.sedpCh, b.dataCh, true
}

// receiveDemux reads one unicast datagram. False means exit the loop.
func (b *Backend) receiveDemux(ctx context.Context, uni *rtps.UnicastConn) ([]byte, bool) {
	_, _, data, err := uni.Receive(ctx)
	if err != nil {
		cerr := ctx.Err()
		if cerr != nil {
			return nil, false
		}

		if b.isClosed() {
			return nil, false
		}

		b.bumpDemuxError()
		log.Printf("dds: demux receive failed: %v", err)

		return nil, true
	}

	if len(data) < 4 {
		b.bumpDemuxError()
		log.Printf("dds: demux dropped short %d byte datagram", len(data))

		return nil, true
	}

	return data, true
}

// enqueueBestEffort queues one value without blocking the demux.
//
// It copies bytes before queueing so the caller may reuse the input.
// Drops on full queue are counted by the caller when needed.
func enqueueBestEffort[T any](ctx context.Context, ch chan T, v T) {
	select {
	case <-ctx.Done():
	case ch <- v:
	default:
	}
}

// enqueueSEDP queues SEDP bytes best effort.
func (b *Backend) enqueueSEDP(ctx context.Context, sedpCh chan []byte, data []byte) {
	cp := append([]byte(nil), data...)
	enqueueBestEffort(ctx, sedpCh, cp)
}

// enqueueSPDP queues SPDP bytes best effort.
func (b *Backend) enqueueSPDP(ctx context.Context, spdpCh chan []byte, data []byte) {
	cp := append([]byte(nil), data...)
	enqueueBestEffort(ctx, spdpCh, cp)
}

// sedpBuiltinWriter reports whether id is a SEDP builtin writer entity.
func sedpBuiltinWriter(id rtps.EntityID) bool {
	first := rtps.EntityID{0x00, 0x00, sedpWriterEntityB14, 0xC2}
	cyclone := rtps.EntityID{0x00, 0x00, sedpWriterEntityB14Cyclone, 0xC2}
	second := rtps.EntityID{0x00, 0x00, sedpWriterEntityB14Alt, 0xC2}

	return id == first || id == cyclone || id == second
}

// dispatchRTPS routes one RTPS datagram by writer entity and queues DATA payloads.
//
// Batched datagrams may carry builtin discovery plus user DATA in one
// message. Builtin DATA enqueues the whole datagram for the SPDP and SEDP
// listen loops, which scan for their own writer, while user DATA payloads
// still enqueue to the data path. Cancellation aborts before queueing
// users.
func (b *Backend) dispatchRTPS(
	ctx context.Context,
	data []byte,
	spdpCh chan []byte,
	sedpCh chan []byte,
	dataCh chan dataItem,
) {
	header, subs, err := rtps.DecodeMessage(data, nil)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux decode failed: %v", err)

		return
	}

	b.mu.RLock()
	recv := b.receiver
	b.mu.RUnlock()

	kept := subs[:0]
	for _, sub := range subs {
		handled, herr := HandleProtectedSubmessage(recv, sub)
		if herr != nil {
			b.bumpDemuxError()
			log.Printf("dds: demux secured submessage dropped: %v", herr)
		}
		if handled {
			continue
		}
		kept = append(kept, sub)
	}
	subs = kept

	if b.routeBuiltinDiscovery(ctx, header, data, subs, spdpCh, sedpCh) {
		return
	}

	b.enqueueUserSubs(ctx, header, subs, dataCh)
}

// routeBuiltinDiscovery enqueues SPDP and SEDP datagrams and reports cancellation.
//
// It scans all subs and enqueues the whole datagram once to SPDP when an
// SPDP builtin writer is present and once to SEDP when an SEDP builtin
// writer is present. HEARTBEAT from an SEDP builtin writer is answered
// with an ACKNACK requesting from its first sequence so reliable peers
// such as CycloneDDS send their endpoint DATA to this participant;
// without the ACK the peer heartbeats forever and its endpoints stay
// unknown. ACKNACK for SEDP writers is answered here and never reaches
// the user path, as are SPDP HEARTBEATs, which need no reply because
// periodic announces already drive matching. SEDP GAP advances tracking
// so gapped seq is not NACKed forever. User subs are left for
// enqueueUserSubs, so batched discovery plus user DATA in one datagram
// no longer drops the user sample. It returns true only when ctx is
// cancelled.
func (b *Backend) routeBuiltinDiscovery(
	ctx context.Context,
	header rtps.Header,
	data []byte,
	subs []rtps.RawSubmessage,
	spdpCh chan []byte,
	sedpCh chan []byte,
) bool {
	spdpFound := false
	sedpFound := false

	for _, sub := range subs {
		if ctx.Err() != nil {
			return true
		}
		if b.routeBuiltinSub(ctx, header, sub) {
			continue
		}
		if sub.ID != rtps.SubmessageData {
			continue
		}
		spdpFound, sedpFound = b.routeDiscoveryData(ctx, header, sub, data, spdpCh, sedpCh, spdpFound, sedpFound)
	}

	return false
}

// sendPlainNack transmits one ACKNACK for a plain-path sequence gap.
//
// It answers late-join and loss gaps on vendor samples that bypass framed
// sequencing. Failures return loudly so the receiver counts them under a
// distinct reason instead of failing silently.
func (b *Backend) sendPlainNack(ctx context.Context, guid WriterGUID, base uint64) error {
	if base == 0 {
		return nil
	}

	if err := b.sendUserAckNack(ctx, guid, b.readerEntityForWriter(guid), base, 0, nil); err != nil {
		return fmt.Errorf("dds: plain nack send: %w", err)
	}

	return nil
}

// routeBuiltinSub routes one discovery control submessage.
//
// It reports true when the submessage was fully handled and the caller
// must continue to the next submessage.
func (b *Backend) routeBuiltinSub(ctx context.Context, header rtps.Header, sub rtps.RawSubmessage) bool {
	if sub.ID == rtps.SubmessageHeartbeat {
		b.ackSedpHeartbeat(ctx, header, sub)
		return true
	}
	if skipDiscoveryControl(sub) {
		return true
	}
	if sub.ID == rtps.SubmessageGap {
		b.handleDiscoveryGap(header, sub)
		return true
	}
	return false
}

// handleDiscoveryGap advances SEDP tracking for one GAP submessage.
func (b *Backend) handleDiscoveryGap(header rtps.Header, sub rtps.RawSubmessage) {
	gap, ok := b.decodeDiscoveryGap(sub)
	if !ok {
		return
	}
	if !sedpBuiltinWriter(gap.WriterID) {
		return
	}
	var guid WriterGUID
	copy(guid[:12], header.Prefix[:])
	copy(guid[12:16], gap.WriterID[:])
	b.applySedpGap(guid, gap.GapStart)
}

// decodeDiscoveryGap decodes one SEDP GAP and reports decode failures.
func (b *Backend) decodeDiscoveryGap(sub rtps.RawSubmessage) (rtps.Gap, bool) {
	var drops rtps.Counter
	gap, err := rtps.DecodeGap(sub.Body, sub.Flags, &drops)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux sedp gap: %v", err)
		return rtps.Gap{}, false
	}
	return gap, true
}

// skipDiscoveryControl reports whether a submessage is ignorable discovery control.
//
// ACKNACK never routes to SPDP or SEDP data queues. SEDP HEARTBEAT is
// answered with an ACKNACK in routeBuiltinDiscovery before this check, so
// reaching here means a non-HEARTBEAT control submessage.
func skipDiscoveryControl(sub rtps.RawSubmessage) bool {
	return sub.ID == rtps.SubmessageHeartbeat || sub.ID == rtps.SubmessageAckNack
}

// snapshotRoutePorts returns a copy of unicast ports for one participant.
//
// It self-locks with RLock and is safe for concurrent use.
func (b *Backend) snapshotRoutePorts(prefix GUIDPrefix) []int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return append([]int(nil), b.routes[prefix]...)
}

// sendSedpAck unicasts one INFO_DST plus ACKNACK to the heartbeat sender.
//
// INFO_DST names the sender participant so the peer resolves the target
// writer; without it CycloneDDS logs not a connection and never sends
// endpoint DATA. The receipt fans out to every known route port because the
// route table stores bare ports and the peer may listen on metatraffic.
// Failures count a demux error and log without failing
// the loop.
func (b *Backend) sendSedpAck(ctx context.Context, sender GUIDPrefix, ack rtps.RawSubmessage) {
	if ctx.Err() != nil {
		return
	}
	ports := b.snapshotRoutePorts(sender)
	if len(ports) == 0 {
		return
	}
	uni := b.snapshotUnicast()
	if uni == nil {
		b.bumpDemuxError()
		log.Printf("dds: demux sedp ack without transport")
		return
	}
	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()
	if local == nil {
		return
	}
	var prefix [12]byte
	lg := local.GUID()
	copy(prefix[:], lg.Prefix[:])
	header := rtps.DefaultHeader(prefix)
	dst := rtps.RawSubmessage{
		ID:    rtps.SubmessageInfoDst,
		Flags: rtps.FlagLittleEndian,
		Body:  append([]byte(nil), sender[:]...),
	}
	msg, err := rtps.EncodeMessage(header, []rtps.RawSubmessage{dst, ack})
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux sedp ack encode: %v", err)
		return
	}
	sent, last := sendToPorts(ctx, uni, ports, msg)
	if sent == 0 && last != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux sedp ack send: %v", last)
	}
}

// ackSedpHeartbeat answers one SEDP HEARTBEAT with an ACKNACK.
//
// It decodes the heartbeat and ignores non-SED writers, self packets, and
// empty ranges. The ACK sets every bit from the next wanted sequence
// through last so the peer sends only missing endpoint DATA; once caught
// up the ACK is pure with the Final flag and the peer slows to keepalive
// pace. Without it a reliable peer heartbeats forever and its endpoints
// stay unknown. The ACK carries the matching builtin reader entity and
// fans out to every known route port with an INFO_DST naming the sender
// participant. Failures count a demux error and log without failing the loop.
func (b *Backend) ackSedpHeartbeat(ctx context.Context, header rtps.Header, sub rtps.RawSubmessage) {
	var drops rtps.Counter
	beat, err := rtps.DecodeHeartbeat(sub.Body, sub.Flags, &drops)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux sedp heartbeat: %v", err)
		return
	}
	reader, ok := sedpBuiltinReaderForWriter(beat.WriterID)
	if !ok {
		return
	}
	b.mu.RLock()
	local := b.local
	b.mu.RUnlock()
	if local == nil {
		return
	}
	sender := GUIDPrefix(header.Prefix)
	if sender == local.GUID().Prefix {
		return
	}
	if beat.FirstSequence == 0 || beat.LastSequence < beat.FirstSequence {
		return
	}
	var remote WriterGUID
	copy(remote[:12], header.Prefix[:])
	copy(remote[12:16], beat.WriterID[:])
	base := b.sedpBase(remote, beat.FirstSequence)
	bitmap, numBits, _ := planSedpNack(base, beat.LastSequence)
	ack, err := b.buildAckSubmessage(remote, reader, base, numBits, bitmap, numBits == 0)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux sedp ack build: %v", err)
		return
	}
	b.sendSedpAck(ctx, sender, ack)
}

// routeDiscoveryData enqueues one DATA submessage to SPDP or SEDP when builtin.
//
// It returns updated found flags so the whole datagram enqueues at most
// once per path. Non-builtin DATA is left for the user path.
func (b *Backend) routeDiscoveryData(
	ctx context.Context,
	header rtps.Header,
	sub rtps.RawSubmessage,
	data []byte,
	spdpCh chan []byte,
	sedpCh chan []byte,
	spdpFound bool,
	sedpFound bool,
) (bool, bool) {
	writer, ok := builtinWriterOf(sub)
	if !ok {
		if sub.ID == rtps.SubmessageData {
			sedpTracef("demux data non-builtin sub-id=%#02x len=%d", sub.ID, len(data))
		}
		return spdpFound, sedpFound
	}

	if writer == rtps.EntitySPDPWriter {
		sedpTracef("demux spdp-data writer=%02x%02x%02x%02x len=%d", writer[0], writer[1], writer[2], writer[3], len(data))
		if !spdpFound {
			b.enqueueSPDP(ctx, spdpCh, data)
			spdpFound = true
		}

		return spdpFound, sedpFound
	}

	if sedpBuiltinWriter(writer) {
		sedpTracef("demux sedp-data writer=%02x%02x%02x%02x len=%d", writer[0], writer[1], writer[2], writer[3], len(data))
		b.noteSedpData(header, sub, writer)
		if !sedpFound {
			b.enqueueSEDP(ctx, sedpCh, data)
			sedpFound = true
		}
	} else if sub.ID == rtps.SubmessageData {
		sedpTracef("demux user-data writer=%02x%02x%02x%02x len=%d", writer[0], writer[1], writer[2], writer[3], len(data))
	}

	return spdpFound, sedpFound
}

// noteSedpData records the DATA sequence for one remote SEDP writer.
//
// It decodes the submessage sequence and stores the max under the writer
// GUID built from the datagram header prefix, so later heartbeats ack
// only missing samples. Decode failures are ignored: the listen loop
// reports them when it parses the enqueued datagram.
func (b *Backend) noteSedpData(header rtps.Header, sub rtps.RawSubmessage, writer rtps.EntityID) {
	var drops rtps.Counter
	decoded, err := rtps.DecodeSubmessage(sub, &drops)
	if err != nil || decoded.Data == nil {
		return
	}
	var guid WriterGUID
	copy(guid[:12], header.Prefix[:])
	copy(guid[12:16], writer[:])
	b.noteSedpSeq(guid, decoded.Data.SequenceNumber)
}

// sedpNackBitmap returns an all-ones bitmap for n samples.
//
// Bits run from the most significant bit per the RTPS sequence number set
// layout, so CycloneDDS reads the first samples as requested; LSB packing
// reads as all zero and never triggers a retransmit. Bits are capped at
// userAckBitsMax to bound work; SEDP sets stay far below the cap. The
// returned count fits the bitmap words exactly so the encoder accepts it.
func sedpNackBitmap(n uint64) ([]uint32, uint32) {
	if n > userAckBitsMax {
		n = userAckBitsMax
	}
	bits := uint32(n)
	words := (bits + 31) / 32
	out := make([]uint32, words)
	for i := range out {
		out[i] = 0xFFFFFFFF
	}
	if rem := bits % 32; rem != 0 {
		out[len(out)-1] = ^uint32(0) << (32 - rem)
	}
	return out, bits
}

// sedpBuiltinReaderForWriter returns the builtin reader pairing with a writer.
//
// Publications writers pair with 0x000002c7, the Cyclone publications
// writer pairs with 0x000003c7, and the subscriptions writer
// pairs with 0x000004c7. It reports false for non SEDP writers so callers
// keep one validation point with sedpBuiltinWriter.
func sedpBuiltinReaderForWriter(writer rtps.EntityID) (rtps.EntityID, bool) {
	if !sedpBuiltinWriter(writer) {
		return rtps.EntityID{}, false
	}
	if writer[2] == sedpWriterEntityB14 {
		return rtps.EntityID{0x00, 0x00, sedpWriterEntityB14, 0xC7}, true
	}
	if writer[2] == sedpWriterEntityB14Alt {
		return rtps.EntityID{0x00, 0x00, sedpWriterEntityB14Alt, 0xC7}, true
	}
	return rtps.EntityID{0x00, 0x00, sedpWriterEntityB14Cyclone, 0xC7}, true
}

// builtinWriterOf returns the writer entity for one DATA submessage.
func builtinWriterOf(sub rtps.RawSubmessage) (rtps.EntityID, bool) {
	decoded, err := rtps.DecodeSubmessage(sub, nil)
	if err != nil || decoded.Data == nil {
		return rtps.EntityID{}, false
	}

	return decoded.Data.WriterID, true
}

// enqueueUserSubs queues each user DATA payload from decoded subs.
//
// Builtin SPDP and SEDP writers are skipped here because routeBuiltinDiscovery
// already enqueued the whole datagram for those paths. This keeps batched
// discovery plus user DATA from delivering builtin samples to TakeFrom.
// User control (ACKNACK, HEARTBEAT, GAP) routes to the reliability
// handler, not to dataCh, before DATA queueing. DATA_FRAG reassembles to
// one payload and queues to dataCh when complete.
func (b *Backend) enqueueUserSubs(
	ctx context.Context,
	header rtps.Header,
	subs []rtps.RawSubmessage,
	dataCh chan dataItem,
) {
	for _, sub := range subs {
		err := ctx.Err()
		if err != nil {
			return
		}

		if b.handleUserControl(ctx, header, sub, dataCh) {
			continue
		}

		if sub.ID != rtps.SubmessageData {
			continue
		}

		if writer, ok := builtinWriterOf(sub); ok {
			if writer == rtps.EntitySPDPWriter || sedpBuiltinWriter(writer) {
				continue
			}
		}

		b.enqueueData(ctx, dataCh, header, sub)
	}
}

// enqueueData decodes one submessage and queues its DATA payload.
//
// It records the outer RTPS seq for ACK base so later HEARTBEATs can be
// answered without changing best effort Take ordering.
func (b *Backend) enqueueData(
	ctx context.Context,
	dataCh chan dataItem,
	header rtps.Header,
	sub rtps.RawSubmessage,
) {
	decoded, err := rtps.DecodeSubmessage(sub, nil)
	if err != nil {
		b.bumpDemuxError()
		log.Printf("dds: demux submessage decode failed: %v", err)

		return
	}

	if decoded.Data == nil || len(decoded.Data.Payload) == 0 {
		return
	}

	var guid WriterGUID

	copy(guid[:12], header.Prefix[:])
	copy(guid[12:16], decoded.Data.WriterID[:])

	b.maybePromptNackOnData(ctx, guid, decoded.Data.SequenceNumber)
	b.noteRemoteData(guid, decoded.Data.SequenceNumber)

	payload := append([]byte(nil), decoded.Data.Payload...)

	enqueueBestEffort(ctx, dataCh, dataItem{guid: guid, seq: decoded.Data.SequenceNumber, payload: payload})
}

// ReportLiveliness reports local endpoint liveliness on the SEDP path.
func (b *Backend) ReportLiveliness(ctx context.Context, report transports.LivelinessReport) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: report liveliness: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return fmt.Errorf("dds: report liveliness after close: %w", transports.ErrBackendClosed)
	}

	if !b.joined {
		return fmt.Errorf("dds: report liveliness before join: %w", transports.ErrNotJoined)
	}

	last, seen := b.lastGen[report.ParticipantName]
	if !seen {
		b.lastGen[report.ParticipantName] = report.Generation
		b.lastAlive[report.ParticipantName] = report.Alive
		b.leases[report.ParticipantName] = b.clock.Now()

		return nil
	}

	if report.Generation > last {
		b.lastGen[report.ParticipantName] = report.Generation
		b.lastAlive[report.ParticipantName] = report.Alive
		b.leases[report.ParticipantName] = b.clock.Now()

		return nil
	}

	if report.Generation == last && report.Alive == b.lastAlive[report.ParticipantName] {
		return nil
	}

	return fmt.Errorf(
		"dds: stale generation %d for %q: %w",
		report.Generation,
		report.ParticipantName,
		transports.ErrStaleGeneration,
	)
}

// snapshotPublish returns publisher state under lock as one struct.
//
// It self-locks with RLock and is safe for concurrent use. SEDPEndpoint
// values from the index are clones; callers must treat them as must-not-
// copy snapshots and never retain internal storage.
func (b *Backend) snapshotPublish() publishSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return publishSnapshot{
		publisher:  b.publisher,
		sedp:       b.sedp,
		domain:     b.domainID,
		live:       b.uni != nil && b.mcast != nil && b.local != nil,
		routeCount: len(b.routes),
	}
}

// sedpTypeHashStatic maps IDL names to known Jazzy type hashes.
//
// The table backs sedpTypeHashForIDL so the lookup stays a map fetch
// instead of a wide switch.
var sedpTypeHashStatic = map[string]string{
	"std_msgs__msg__Int32":                                  "RIHS01_b6578ded3c58c626cfe8d1a6fb6e04f706f97e9f03d2727c9ff4e74b1cef0deb",
	"std_msgs__msg__Float64":                                "RIHS01_705ba9c3d1a09df43737eb67095534de36fd426c0587779bda2bc51fe790182a",
	"std_msgs__msg__Bool":                                   "RIHS01_feb91e995ff9ebd09c0cb3d2aed18b11077585839fb5db80193b62d74528f6c9",
	"std_msgs__msg__Int16":                                  "RIHS01_1dcc3464e47c288a55f943a389d337cdb06804de3f5cd7a266b0de718eee17e5",
	"std_msgs__msg__Int64":                                  "RIHS01_8cd1048c2f186b6bd9a92472dc1ce51723c0833a221e2b7aecfff111774f4b49",
	"std_msgs__msg__UInt8":                                  "RIHS01_6138bd83d8c3569cb80a667db03cfc1629f529fee79d944c39c34e352e72f010",
	"std_msgs__msg__Int8":                                   "RIHS01_26525065a403d972cb672f0777e333f0c799ad444ae5fcd79e43d1e73bd0f440",
	"std_msgs__msg__UInt16":                                 "RIHS01_08a406e4b022bc22e907f985d6a9e9dd1d4fbecae573549cf49350113e7757b1",
	"std_msgs__msg__UInt32":                                 "RIHS01_a5c874829b752bc5fa190024b0ad76f578cc278271e855c7d02a818b3516fb4a",
	"std_msgs__msg__UInt64":                                 "RIHS01_fbdc52018fc13755dce18024d1a671c856aa8b4aaf63adfb095b608f98e8c943",
	"std_msgs__msg__Float32":                                "RIHS01_7170d3d8f841f7be3172ce5f4f59f3a4d7f63b0447e8b33327601ad64d83d6e2",
	"std_msgs__msg__Char":                                   "RIHS01_3ad2d04dd29ba19d04b16659afa3ccaedd691914b02a64e82e252f2fa6a586a9",
	"std_msgs__msg__Byte":                                   "RIHS01_41e1a3345f73fe93ede006da826a6ee274af23dd4653976ff249b0f44e3e798f",
	"std_msgs__msg__Empty":                                  "RIHS01_20b625256f32d5dbc0d04fee44f43c41e51c70d3502f84b4a08e7a9c26a96312",
	"std_msgs__msg__Header":                                 "RIHS01_f49fb3ae2cf070f793645ff749683ac6b06203e41c891e17701b1cb597ce6a01",
	"std_msgs__msg__ColorRGBA":                              "RIHS01_77a7a5b9ae477306097665106e0413ba74440245b1f3d0c6d6405fe5c7813fe8",
	"std_msgs__msg__MultiArrayDimension":                    "RIHS01_5e773a60a4c7fc8a54985f307c7837aa2994252a126c301957a24e31282c9cbe",
	"std_msgs__msg__MultiArrayLayout":                       "RIHS01_4c66e6f78e740ac103a94cf63259f968e48c617e7699e829b63c21a5cb50dac6",
	"std_msgs__msg__ByteMultiArray":                         "RIHS01_972fec7f50ab3c1d06783c228e79e8a9a509021708c511c059926261ada901d4",
	"std_msgs__msg__Float32MultiArray":                      "RIHS01_0599f6f85b4bfca379873a0b4375a0aca022156bd2d7021275d116ed1fa8bfe0",
	"std_msgs__msg__Float64MultiArray":                      "RIHS01_1025ddc6b9552d191f89ef1a8d2f60f3d373e28b283d8891ddcc974e8c55397f",
	"std_msgs__msg__Int16MultiArray":                        "RIHS01_b58810e8e5b90fb19a5062469eb8409f5ab11a446d60de7157a1457e52a076ce",
	"std_msgs__msg__Int32MultiArray":                        "RIHS01_84a7346323525d1b4dfca899df3820f245e54009dac5a6b69217d14fdefd1701",
	"std_msgs__msg__Int64MultiArray":                        "RIHS01_e60f9fe34d697f0939ad49d33158693c1277fbac0e2f04b7c2995dc21c89b422",
	"std_msgs__msg__Int8MultiArray":                         "RIHS01_f21998d4b492abd63330765d75d5831238d400740386f651f13a872a4d2188db",
	"std_msgs__msg__UInt16MultiArray":                       "RIHS01_94fe73428ec63baecc774f8fb82406123e9291cf728f1b7c91caf5335129492b",
	"std_msgs__msg__UInt32MultiArray":                       "RIHS01_6c2577c7ad3cbdcc2164a41c12f1d5ad314ea320f3fb1ee47e78019fe16bb5b0",
	"std_msgs__msg__UInt64MultiArray":                       "RIHS01_fc1c685c2f76bdc6983da025cb25d2db5fb5157b059e300f6d957d86f981b366",
	"std_msgs__msg__UInt8MultiArray":                        "RIHS01_5687e861b8d307a5e48b7515467ae7a5fc2daf805bd0ce6d8e9e604bade9f385",
	"builtin_interfaces__msg__Time":                         "RIHS01_b106235e25a4c5ed35098aa0a61a3ee9c9b18d197f398b0e4206cea9acf9c197",
	"builtin_interfaces__msg__Duration":                     "RIHS01_e8d009f659816f758b75334ee1a9ca5b5c0b859843261f14c7f937349599d93b",
	"geometry_msgs__msg__Vector3":                           "RIHS01_cc12fe83e4c02719f1ce8070bfd14aecd40f75a96696a67a2a1f37f7dbb0765d",
	"geometry_msgs__msg__PointStamped":                      "RIHS01_4c0296af86e01e562e9e0405d138a01537247580076c58ea38d7923ac1045897",
	"geometry_msgs__msg__Point":                             "RIHS01_6963084842a9b04494d6b2941d11444708d892da2f4b09843b9c43f42a7f6881",
	"geometry_msgs__msg__Point32":                           "RIHS01_2fc4db7cae16a4582c79a56b66173a8d48d52c7dc520ddc55a0d4bcf2a4bfdbc",
	"geometry_msgs__msg__Accel":                             "RIHS01_dc448243ded9b1fcbcca24aba0c22f013dae06c354ba2d849571c0a2a3f57ca0",
	"geometry_msgs__msg__Inertia":                           "RIHS01_2ddd5dab5c347825ba2e56c895ddccfd0b8efe53ae931bf67f905529930b4bd7",
	"geometry_msgs__msg__Polygon":                           "RIHS01_3782f9f0bf044964d692d6c017d705e37611afb1f0bf6a9dee248a7dda0f784a",
	"geometry_msgs__msg__Transform":                         "RIHS01_beb83fbe698636351461f6f35d1abb20010c43d55374d81bd041f1ba2581fddc",
	"geometry_msgs__msg__Wrench":                            "RIHS01_018e8519d57c16adbe97c9fe1460ef21fec7e31bc541de3d653a35895677ce52",
	"geometry_msgs__msg__AccelWithCovariance":               "RIHS01_230d51bd53bc36f260574e73b42941cefe44684753480b6fc330c032c5db5997",
	"geometry_msgs__msg__PoseWithCovariance":                "RIHS01_9a7c0fd234b7f45c6098745ecccd773ca1085670e64107135397aee31c02e1bb",
	"geometry_msgs__msg__TwistWithCovariance":               "RIHS01_49f574f033f095d8b6cd1beaca5ca7925e296e84af1716d16c89d38b059c8c18",
	"geometry_msgs__msg__AccelStamped":                      "RIHS01_ef1df9eabae0a708cc049a061ebcddc4e2a5f745730100ba680e086a9698b165",
	"geometry_msgs__msg__AccelWithCovarianceStamped":        "RIHS01_61c9ad8928e71dd95ce791b2f02809ee2a0bbcc42cd0e4047fd00a822a08e444",
	"geometry_msgs__msg__InertiaStamped":                    "RIHS01_766be45976252babf7f9d8ac4ae7c912a7ceccf71035622529f27518b695aa09",
	"geometry_msgs__msg__PolygonStamped":                    "RIHS01_b7cf07932f1523d4b4088075945c1a0141f7cd21da87cc940fc61652e9138b46",
	"geometry_msgs__msg__PoseArray":                         "RIHS01_af0cc36d190e104d546d168d6b39df04fa4b4ccecf59cb4c9ed328d3d5004aa0",
	"geometry_msgs__msg__PoseStamped":                       "RIHS01_10f3786d7d40fd2b54367835614bff85d4ad3b5dab62bf8bca0cc232d73b4cd8",
	"geometry_msgs__msg__PoseWithCovarianceStamped":         "RIHS01_26432f9803e43727d3c8f668d1fdb3c630f548af631e2f4e31382371bfea3b6e",
	"geometry_msgs__msg__QuaternionStamped":                 "RIHS01_381add86c6c3160644d228ca342182c7fd6c7fab11c7a85ad817a9cc22dbac6e",
	"geometry_msgs__msg__TransformStamped":                  "RIHS01_0a241f87d04668d94099cbb5ba11691d5ad32c2f29682e4eb5653424bd275206",
	"geometry_msgs__msg__TwistStamped":                      "RIHS01_5f0fcd4f81d5d06ad9b4c4c63e3ea51b82d6ae4d0558f1d475229b1121db6f64",
	"geometry_msgs__msg__TwistWithCovarianceStamped":        "RIHS01_77b67434531e6529b7a0091357b186b6ebdb17fd9ffd3e0c7ce9d3fb11a44563",
	"geometry_msgs__msg__Vector3Stamped":                    "RIHS01_d4829622288cbb443886e7ea94ea5671a3b1be6bab4ad04224432a65f7d7887a",
	"geometry_msgs__msg__WrenchStamped":                     "RIHS01_8dc3deaf06b2ab281f9f9a742a8961c328ca7cec16e3fd6586d3a5c83fa78f77",
	"geometry_msgs__msg__Pose":                              "RIHS01_d501954e9476cea2996984e812054b68026ae0bfae789d9a10b23daf35cc90fa",
	"geometry_msgs__msg__Quaternion":                        "RIHS01_8a765f66778c8ff7c8ab94afcc590a2ed5325a1d9a076ffff38fbce36f458684",
	"geometry_msgs__msg__Twist":                             "RIHS01_9c45bf16fe0983d80e3cfe750d6835843d265a9a6c46bd2e609fcddde6fb8d2a",
	"sensor_msgs__msg__Temperature":                         "RIHS01_72514a14126ab9f8a9abec974c78e5610a367b59db5da355ff1fb982d5bad4b8",
	"sensor_msgs__msg__FluidPressure":                       "RIHS01_22dfb2b145a0bd5a31a1ac3882a1b32148b51d9b2f3bab250290d66f3595bc32",
	"sensor_msgs__msg__CameraInfo":                          "RIHS01_b3dfd68ff46c9d56c80fd3bd4ed22c7a4ddce8c8348f2f59c299e73118e7e275",
	"sensor_msgs__msg__CompressedImage":                     "RIHS01_15640771531571185e2efc8a100baf923961a4d15d5569652e6cb6691e8e371a",
	"sensor_msgs__msg__PointCloud":                          "RIHS01_614593df71d3c2b9bd4604a71b750fd218f0d65c045ea988b713719455a65b3b",
	"sensor_msgs__msg__PointField":                          "RIHS01_5c6a4750728c2bcfbbf7037225b20b02d4429634732146b742dee1726637ef01",
	"sensor_msgs__msg__Range":                               "RIHS01_b42b62562e93cbfe9d42b82fe5994dfa3d63d7d5c90a317981703f7388adff3a",
	"sensor_msgs__msg__RegionOfInterest":                    "RIHS01_ad16bcba5f9131dcdba6fbded19f726f5440e3c513b4fb586dd3027eeed8abb1",
	"sensor_msgs__msg__RelativeHumidity":                    "RIHS01_8687c99b4fb393cb2e545e407b5ea7fd0b5d8960bcd849a0f86c544740138839",
	"sensor_msgs__msg__TimeReference":                       "RIHS01_dd66e84cf40bbb5d5a40472e6ecf2675a031334d4c426abdb2ad41801a8efc99",
	"sensor_msgs__msg__MagneticField":                       "RIHS01_e80f32f56a20486c9923008fc1a1db07bbb273cbbf6a5b3bfa00835ee00e4dff",
	"sensor_msgs__msg__Joy":                                 "RIHS01_0d356c79cad3401e35ffeb75a96a96e08be3ef896b8b83841d73e890989372c5",
	"sensor_msgs__msg__JointState":                          "RIHS01_a13ee3a330e346c9d87b5aa18d24e11690752bd33a0350f11c5882bc9179260e",
	"sensor_msgs__msg__BatteryState":                        "RIHS01_4bee5dfce981c98faa6828b868307a0a73f992ed0789f374ee96c8f840e69741",
	"sensor_msgs__msg__Illuminance":                         "RIHS01_b954b25f452fcf81a91c9c2a7e3b3fd85c4c873d452aecb3cfd8fd1da732a22d",
	"sensor_msgs__msg__MultiEchoLaserScan":                  "RIHS01_ba5eac341cd5bbb2701527aa4568e8baec172b69cadb9a1945d6f149d087ee48",
	"sensor_msgs__msg__LaserEcho":                           "RIHS01_0fbc05a0db7d37fe52c0f0375356db55da0046f7ef5bd27ca6b34bd0582bc952",
	"sensor_msgs__msg__NavSatStatus":                        "RIHS01_d1ed3befa628e09571bd273b888ba1c1fd187c9a5e0006b385d7e5e9095a3204",
	"sensor_msgs__msg__JoyFeedback":                         "RIHS01_231dd362f71d6fc08272770d07120ad5fe5874ce2dbac70109b28986834290cd",
	"sensor_msgs__msg__PointCloud2":                         "RIHS01_9198cabf7da3796ae6fe19c4cb3bdd3525492988c70522628af5daa124bae2b5",
	"sensor_msgs__msg__Imu":                                 "RIHS01_7d9a00ff131080897a5ec7e26e315954b8eae3353c3f995c55faf71574000b5b",
	"sensor_msgs__msg__LaserScan":                           "RIHS01_64c191398013af96509d518dac71d5164f9382553fce5c1f8cca5be7924bd828",
	"sensor_msgs__msg__NavSatFix":                           "RIHS01_62223ab3fe210a15976021da7afddc9e200dc9ec75231c1b6a557fc598a65404",
	"sensor_msgs__msg__Image":                               "RIHS01_d31d41a9a4c4bc8eae9be757b0beed306564f7526c88ea6a4588fb9582527d47",
	"nav_msgs__msg__Odometry":                               "RIHS01_3cc97dc7fb7502f8714462c526d369e35b603cfc34d946e3f2eda2766dfec6e0",
	"nav_msgs__msg__Path":                                   "RIHS01_1957a5bb3cee5da65c4e52e52b65a93df227efce4c20f8458b36e73066ca334b",
	"nav_msgs__msg__OccupancyGrid":                          "RIHS01_8d348150c12913a31ee0ec170fbf25089e4745d17035792a1ba94d6f0bc0cfc7",
	"nav_msgs__msg__MapMetaData":                            "RIHS01_2772d4b2000ef2b35dbaeb80fd3946c1369f817fb4f75677d916d27c17d763c8",
	"nav_msgs__msg__GridCells":                              "RIHS01_bb99c2f5d0a04750745a81ec6a8147aa373cce5bd17c8cd6507f2413354a6933",
	"visualization_msgs__msg__Marker":                       "RIHS01_45b13ccf791f225962bf74e746f9644518855d783a6f42ba0cc14fde2b4f3ce0",
	"visualization_msgs__msg__MarkerArray":                  "RIHS01_86cb8800b6fb05b5eff1abd7a56f62a5641d3ae9a1c29e78e67e704f1d067dcf",
	"visualization_msgs__msg__MenuEntry":                    "RIHS01_22170c387c70fd4236232ec902de8604e72ff027342c7c0f28ad9f68c64c51d6",
	"visualization_msgs__msg__ImageMarker":                  "RIHS01_603152491ef2331c200a5305230d31f6e8704875944b388da0f547c415d11836",
	"visualization_msgs__msg__InteractiveMarkerControl":     "RIHS01_60e2fa36344f5f4791b24a809542a18bffd555f563550d4b22b3bbfc31ec0ed5",
	"visualization_msgs__msg__InteractiveMarker":            "RIHS01_3d5b51448b51d73b0f395b94d259edd3a5d269ae9f7d9fd5cceb9ae4b72be346",
	"visualization_msgs__msg__InteractiveMarkerPose":        "RIHS01_c60e9a4407d5f709a63e0fe9caea324aee08fe717cd090209ebe35012ce7cb66",
	"visualization_msgs__msg__InteractiveMarkerFeedback":    "RIHS01_6cc48741df9f05d19ba7d9ea3101e9fcd1309c9d6bda3c55668ba607492f725e",
	"visualization_msgs__msg__InteractiveMarkerInit":        "RIHS01_23fda1b3373b154d9d6408dd7b9f8129b2a2b76b905ee421e8c4109d8bf71f78",
	"visualization_msgs__msg__InteractiveMarkerUpdate":      "RIHS01_0a8b000c4fd4d50876ac716a7de018911599a2f015795388e956a2ca8b0c54f0",
	"diagnostic_msgs__msg__KeyValue":                        "RIHS01_d68081eaa540288c5440753baecef0c4e16e81a5f78ad68902ded5100413bb42",
	"diagnostic_msgs__msg__DiagnosticStatus":                "RIHS01_b0e3e692ea2d54a8af2f4ef1930e81556a2db55216b771f8a7d2724ed47bf0e4",
	"diagnostic_msgs__msg__DiagnosticArray":                 "RIHS01_5a8a36efb05fb25070fa0fb3810290c0e6cd4862b54a8fb975a1ee8dc55a333e",
	"stereo_msgs__msg__DisparityImage":                      "RIHS01_1ec1ff6b5bace919e4544a37f2d96ead9f81783701b7b7a4d97a09325ecf2711",
	"trajectory_msgs__msg__JointTrajectoryPoint":            "RIHS01_de8907036d8bd45aac6f30cc9044a3d4a329c42cbf719aff7d95a584cfa532d7",
	"trajectory_msgs__msg__JointTrajectory":                 "RIHS01_179b33eba59d676f6d967ac71fe35e7ca2f64b2f3928f4a018cec115e213796e",
	"trajectory_msgs__msg__MultiDOFJointTrajectory":         "RIHS01_3a18fd095292a65cfde8833c72985a30af981f3ec44494655c6267262b443a4a",
	"trajectory_msgs__msg__MultiDOFJointTrajectoryPoint":    "RIHS01_6ada1085b5ee64eaa069b074968e69f0e27c8c5e6f5bb0586dd1c834ef0e32b8",
	"shape_msgs__msg__Plane":                                "RIHS01_dfbfe8314689c850615d4a727af017e9aa86c10e369a606c8c851ef8f16c58c8",
	"shape_msgs__msg__MeshTriangle":                         "RIHS01_618e5c073eeb729e433ef6226e72c01d995c459fb7d76348c9700409a5020bd0",
	"shape_msgs__msg__Mesh":                                 "RIHS01_f2150b82d8ee7e8bc3f396a2b158aefb4b9a5510a474be271ba1268aebb55289",
	"shape_msgs__msg__SolidPrimitive":                       "RIHS01_2802a15190aadc3f496584df4b0b4c5824d8a0b31aaef839faa75bc34dda38ac",
	"std_srvs__srv__Trigger_Request":                        "RIHS01_d010825374ce8918e72bfd826c82603e60f45419e932ea976f807b74a863a199",
	"std_srvs__srv__Trigger_Response":                       "RIHS01_2d946c21e2fc3f1e9ca6986a8191d85fcc70097a8bb7771a053564bc47009cdf",
	"rcl_interfaces__srv__GetParameters_Request":            "RIHS01_135a9e1c3d7cebef792f6687d7ddf99949b21a0bd9a320d863a9110a3f5cf4d6",
	"rcl_interfaces__srv__GetParameters_Response":           "RIHS01_9fa773b46e39cc5d02db6224d8fd520bb9caeeffc2b27c96c1e59431baba9f2a",
	"rcl_interfaces__srv__GetParameterTypes_Request":        "RIHS01_87d7ec5ef545d4daa289d500025d31fb2437bc8b8365ca367161c4dac4be33fd",
	"rcl_interfaces__srv__GetParameterTypes_Response":       "RIHS01_ac1335dce67c478e669434f6ac2fbff34fcd529580f272d3006785ba8b54e47f",
	"rcl_interfaces__srv__SetParameters_Request":            "RIHS01_130d46386b7e84ea9ecf60608a4d83cbe5e5cb7c6cc5b712b7a4e3a6c254e932",
	"rcl_interfaces__srv__SetParameters_Response":           "RIHS01_7a0e8140a0b8d98308fa81a5403a766e610af29a239caf54b7553227906fc266",
	"rcl_interfaces__srv__SetParametersAtomically_Request":  "RIHS01_db0bd950ed5ed867a0e90b225e44dea382d762c0762ff3e0ee842190042a59e5",
	"rcl_interfaces__srv__SetParametersAtomically_Response": "RIHS01_31b1a38c5b0b9d893ee24da45f69638f4561c78461d82d52750f429b534502b8",
	"rcl_interfaces__srv__DescribeParameters_Request":       "RIHS01_05dd1c2bc87dec2627105c8070d6f9725c781a4f2ab852975bf105462b82b644",
	"rcl_interfaces__srv__DescribeParameters_Response":      "RIHS01_ad6747319613c6131589e062e095b469a46cc764f7d0f12a001a49e6cd893995",
	"rcl_interfaces__srv__ListParameters_Request":           "RIHS01_a1b0b5d6b967a5b8ac5bc8563c3ea678f349e312a6a4967227c5a96bc5ce38df",
	"rcl_interfaces__srv__ListParameters_Response":          "RIHS01_31b937b82de3612426be79f231e386259f97e8dc843e3f2845288b4bd296bdd",
}

// sedpTypeHashForIDL returns the given hash or the known static hash.
//
// The live hash from ros2 topic info -v wins when present so distro
// changes are picked up on the Subscribe and Publish paths. The static
// fallback covers probes that fail on older CLIs and callers that pass
// empty TypeHash in OutgoingMessage. Values match weld-jazzy Jazzy.
func sedpTypeHashForIDL(idlName, given string) string {
	if given != "" {
		return given
	}
	if idlName == "std_msgs__msg__String" {
		return StringTypeHash
	}
	if hash, ok := sedpTypeHashStatic[idlName]; ok {
		return hash
	}
	return ""
}

// declarePublishWriter asserts the SEDP writer for a publish.
//
// It reuses a matching local writer when one exists so repeated publishes
// to the same topic keep one stable writer GUID. The returned endpoint
// names the GUID that DATA must use so CycloneDDS can match DATA to the
// SEDP announcement. An empty type name declares nothing and returns a
// zero endpoint for callers to fall back to the static publisher GUID.
// The caller hash in OutgoingMessage wins when present; the static table
// covers callers that pass empty. A final empty hash keeps legacy behavior
// for truly unknown types and counts a loud miss metric.
func (b *Backend) declarePublishWriter(sedpIdx *SEDPIndex, msg transports.OutgoingMessage) (SEDPEndpoint, error) {
	if msg.TypeName == "" {
		return SEDPEndpoint{}, nil
	}

	hash := sedpTypeHashForIDL(msg.TypeName, msg.TypeHash)

	if hash == "" {
		b.bumpTypeHashMiss()
		log.Printf("dds: publish type %q topic %q has no known type hash, announcing with empty hash", msg.TypeName, msg.TopicName)
	}

	if ep, ok := sedpIdx.FindLocalWriter(msg.TopicName, msg.TypeName, hash); ok {
		return ep, nil
	}

	ep, err := sedpIdx.DeclareWriter(msg.TopicName, msg.TypeName, hash, nil)
	if err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: publish declare writer: %w", err)
	}

	return ep, nil
}

// writerGUIDFromEndpoint maps one SEDP endpoint ID to a DATA writer GUID.
//
// The 12 byte participant prefix plus the 4 byte user entity name the
// writer on the user DATA path. It lets plain and framed DATA use the
// announced entity such as 00 10 00 03 instead of the legacy static
// 00 00 02 03.
func writerGUIDFromEndpoint(id SEDPGUID) WriterGUID {
	var guid WriterGUID

	copy(guid[:12], id.Prefix[:])
	copy(guid[12:16], id.Entity[:])

	return guid
}

// announceWriterOnce sends one SEDP announcement for ep before DATA.
//
// Single shot publishers exit right after PublishBytes, so waiting for the
// periodic announce loop would leave CycloneDDS with DATA from an unknown
// writer. Sending once synchronously here keeps SEDP ahead of the first
// DATA sample on the same route set. Empty routes report ErrNoRoute so the
// CLI route wait retries instead of sending DATA unmatched.
func (b *Backend) announceWriterOnce(ctx context.Context, ep SEDPEndpoint) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: sedp announce context: %w", err)
	}

	ports := b.snapshotRoutes()
	if len(ports) == 0 {
		return fmt.Errorf("dds: sedp announce without route: %w", ErrNoRoute)
	}

	data, err := SerializeSEDPDATA(ep)
	if err != nil {
		return fmt.Errorf("dds: sedp announce serialize: %w", err)
	}

	uni := b.snapshotUnicast()
	if uni == nil {
		return fmt.Errorf("dds: sedp announce without transport: %w", ErrDataPathNotWired)
	}

	sent, last := sendToPorts(ctx, uni, ports, data)
	if sent == 0 && last != nil {
		return fmt.Errorf("dds: sedp announce send: %w", last)
	}

	if sent == 0 {
		return fmt.Errorf("dds: sedp announce without route: %w", ErrNoRoute)
	}

	return nil
}

// hasMatchingRemoteReader reports whether a remote reader matches ep.
//
// It scans remote endpoints for a reader with equal ROS topic, DDS topic,
// kind, DDS type, hash, and domain. A match means CycloneDDS already
// announced its echo reader, so user DATA sent now has readers and will
// not arrive as no readers out-of-sync.
func (b *Backend) hasMatchingRemoteReader(ep SEDPEndpoint) bool {
	b.mu.RLock()
	sedpIdx := b.sedp
	b.mu.RUnlock()

	if sedpIdx == nil {
		return false
	}

	for _, remote := range sedpIdx.RemoteEndpoints() {
		if SEDPEndpointsMatch(remote, ep) {
			return true
		}
	}

	return false
}

// HasRemoteEndpoint reports whether a remote endpoint for topic exists.
//
// It scans remote SEDP endpoints for equal ROS topic and returns true on
// the first match. It self-locks with RLock through the SEDP snapshot and
// is safe for concurrent use. A nil SEDP index reports false so callers
// before JoinDiscovery see no endpoint without a nil panic.
func (b *Backend) HasRemoteEndpoint(topic string) bool {
	b.mu.RLock()
	sedpIdx := b.sedp
	b.mu.RUnlock()

	if sedpIdx == nil {
		sedpTracef("hasRemote topic=%q result=false reason=nil-index", topic)
		return false
	}

	remotes := sedpIdx.RemoteEndpoints()
	for _, remote := range remotes {
		if remote.ROSTopic == topic {
			sedpTracef("hasRemote topic=%q result=true remotes=%d match-guid=%s match-type=%q match-hash=%q match-domain=%d", topic, len(remotes), remote.ID.String(), remote.TypeDDS, remote.TypeHash, remote.DomainID)
			return true
		}
	}

	sedpTracef("hasRemote topic=%q result=false remotes=%d", topic, len(remotes))
	for _, remote := range remotes {
		sedpTracef("hasRemote topic=%q remote guid=%s ros=%q dds=%q typeDDS=%q hash=%q domain=%d reader=%v", topic, remote.ID.String(), remote.ROSTopic, remote.DDSTopic, remote.TypeDDS, remote.TypeHash, remote.DomainID, remote.IsReader)
	}

	return false
}

// waitForMatchingReader polls for a remote reader matching ep.
//
// CycloneDDS drops user DATA that arrives before its reader matches as
// no readers out-of-sync. Waiting here keeps SEDP ahead of DATA: after
// the synchronous announce above, the peer still needs one announce
// interval to process SEDP and match. It polls every 20ms up to 1s and
// returns true when a match appears. On timeout it returns false so the
// caller still sends best-effort for Weld-to-Weld peers that never
// announce readers. Context cancellation aborts early with false.
func (b *Backend) waitForMatchingReader(ctx context.Context, ep SEDPEndpoint) bool {
	if b.hasMatchingRemoteReader(ep) {
		return true
	}

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		err := ctx.Err()
		if err != nil {
			return false
		}

		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}

		if b.hasMatchingRemoteReader(ep) {
			return true
		}
	}

	return b.hasMatchingRemoteReader(ep)
}

// pauseForPeerProcessing pauses briefly so the peer processes our SEDP.
//
// CycloneDDS creates its proxy writer and matches readers asynchronously
// after receiving SEDP DATA. Sending user DATA immediately arrives as no
// readers out-of-sync and is dropped. This fixed 200ms pause keeps SEDP
// ahead of DATA for single shot publishers; use waitForMatchingReader when
// polling for a reader match is needed. Context cancellation aborts early.
func (b *Backend) pauseForPeerProcessing(ctx context.Context) error {
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return fmt.Errorf("dds: publish: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

// Subscribe declares a local SEDP reader so remote writers send DATA.
func (b *Backend) Subscribe(ctx context.Context, topic, typeName, typeHash string) error {
	return b.SubscribeWithDurability(ctx, topic, typeName, typeHash, true)
}

// SubscribeWithDurability declares a local SEDP reader with explicit durability.
//
// A volatile reader matches ros2 topic echo and flows against any offer.
// A transient local reader refuses volatile writers through peer side RxO
// matching, which is how QoS gates prove silence for incompatible pairs.
func (b *Backend) SubscribeWithDurability(ctx context.Context, topic, typeName, typeHash string, volatile bool) error {
	return b.subscribeReader(ctx, topic, typeName, typeHash, volatile, nil)
}

// SubscribeWithQoS declares a local SEDP reader announcing the translated
// per-profile wire QoS instead of the facade default.
//
// System defaults resolve exactly like the core matcher, so a zero
// profile behaves like Subscribe. Refusal to translate is loud and aborts
// the subscribe. The reader also records liveliness and deadline
// trackers so the take path enforces the requested lease and deadline.
func (b *Backend) SubscribeWithQoS(ctx context.Context, topic, typeName, typeHash string, profile core.QoSProfile) error {
	return b.subscribeReader(ctx, topic, typeName, typeHash, false, &profile)
}

// SubscribeWithPartitions declares a local SEDP reader joining explicit
// DDS partitions, empty for the default partition.
//
// Partition names validate loudly at declare. Matching requires a common
// partition with writers, otherwise the pair stays silent without an RxO
// error per the DDS rule.
func (b *Backend) SubscribeWithPartitions(ctx context.Context, topic, typeName, typeHash string, partitions []string) error {
	if err := ValidatePartitionList(partitions); err != nil {
		return fmt.Errorf("dds: subscribe partitions: %w", err)
	}

	err := b.Subscribe(ctx, topic, typeName, typeHash)
	if err != nil {
		return err
	}

	return b.stampLocalPartitions(topic, typeName, partitions)
}

// stampLocalPartitions records partitions on matching local readers.
//
// SEDP declare stores no partitions by default; this backfills the field
// on locals for one topic and type so matching and announce carry the
// requested set. It bumps generation once when any record changes.
func (b *Backend) stampLocalPartitions(topic, typeName string, partitions []string) error {
	b.mu.RLock()
	sedpIdx := b.sedp
	b.mu.RUnlock()

	if sedpIdx == nil {
		return fmt.Errorf("dds: stamp partitions data path not wired yet: %w", ErrDataPathNotWired)
	}

	sedpIdx.StampLocalPartitions(topic, typeName, partitions)

	return nil
}

// SubscribeWithContentFilter declares a local SEDP reader and attaches a
// compiled content filter evaluated on every take.
//
// The expression compiles now and parses loudly on error; nothing
// subscribes when compilation fails. Matching samples flow; misses and
// eval errors drop under distinct reasons countable via DropReasonCount.
func (b *Backend) SubscribeWithContentFilter(ctx context.Context, topic, typeName, typeHash, filterExpr string) error {
	filter, err := CompileContentFilter(filterExpr)
	if err != nil {
		return fmt.Errorf("dds: subscribe content filter: %w", err)
	}

	desc, err := lookupDescriptorForIDL(typeName)
	if err != nil {
		return fmt.Errorf("dds: subscribe content filter descriptor: %w", err)
	}

	err = b.Subscribe(ctx, topic, typeName, typeHash)
	if err != nil {
		return err
	}

	b.mu.RLock()
	receiver := b.receiver
	b.mu.RUnlock()

	if receiver == nil {
		return fmt.Errorf("dds: subscribe content filter data path not wired yet: %w", ErrDataPathNotWired)
	}

	receiver.SetContentFilter(topic, filter, desc)

	return nil
}

// DropReasonCount forwards the per-reason drop tally for dashboards.
func (b *Backend) DropReasonCount(reason DropReason) uint64 {
	b.mu.RLock()
	receiver := b.receiver
	b.mu.RUnlock()

	if receiver == nil {
		return 0
	}

	return receiver.DropReasonCount(reason)
}

// subscribeReader declares a local SEDP reader, optionally with a profile.
//
// A nil profile keeps the facade path with the volatile default. A
// non-nil profile resolves system defaults, derives the volatile flag
// from requested durability, and announces translated wire QoS loudly
// refusing untranslatable settings. Liveliness and deadline trackers
// record from the resolved profile so takes enforce them.
func (b *Backend) subscribeReader(ctx context.Context, topic, typeName, typeHash string, volatile bool, profile *core.QoSProfile) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("dds: subscribe: %w", err)
	}

	b.mu.RLock()
	closed := b.closed
	joined := b.joined
	sedpIdx := b.sedp
	receiver := b.receiver
	b.mu.RUnlock()

	if closed {
		return fmt.Errorf("dds: subscribe after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return fmt.Errorf("dds: subscribe before join: %w", transports.ErrNotJoined)
	}

	if sedpIdx == nil {
		return fmt.Errorf("dds: subscribe data path not wired yet: %w", ErrDataPathNotWired)
	}

	hash := sedpTypeHashForIDL(typeName, typeHash)

	if profile == nil {
		_, err = sedpIdx.DeclareReaderWithDurability(topic, typeName, hash, nil, volatile)
		if err != nil {
			return fmt.Errorf("dds: subscribe declare reader: %w", err)
		}

		return nil
	}

	resolved := core.ResolveSystemDefaults(*profile)
	readerVolatile := resolved.Durability == core.DurabilityVolatile
	local, err := sedpIdx.DeclareReaderWithQoS(topic, typeName, hash, nil, readerVolatile, &resolved)
	if err != nil {
		return fmt.Errorf("dds: subscribe declare reader with qos: %w", err)
	}

	if receiver != nil {
		var writers []WriterGUID
		if matched, merr := sedpIdx.MatchingWriters(local.ID); merr == nil {
			for _, writer := range matched {
				writers = append(writers, writerGUIDFromEndpoint(writer.ID))
			}
		}

		receiver.SetLivelinessTrackerForTopic(topic, resolved.Liveliness, resolved.LeaseDuration, writers)
		receiver.SetDeadlineTrackerForTopic(topic, resolved.Deadline)
	}

	return nil
}

// routeError formats the unknown route error with topic and domain.
func routeError(topic string, domain uint32, err error) error {
	return fmt.Errorf("dds: publish %q domain %d: no route to peers: %w", topic, domain, err)
}

// publishPlain sends one XCDR1 sample as plain RTPS DATA.
//
// It bypasses Weld framing and sends msg.Payload directly via sendData,
// so CycloneDDS accepts the sample as XCDR1 for any ROS type. Only the RTPS
// sequence advances. The writer GUID must be the SEDP announced entity
// for the topic, otherwise CycloneDDS drops DATA from an unknown writer.
// SEDP declaration stays with the caller. Empty routes map to ErrNoRoute
// for route wait retry. Payloads above fragSingleMax fragment via
// planFragSend into ordered DATA_FRAG with one sequence.
func (b *Backend) publishPlain(
	ctx context.Context,
	msg transports.OutgoingMessage,
	snap publishSnapshot,
	writer WriterGUID,
) error {
	if len(msg.Payload) > maxPayloadLen {
		return fmt.Errorf(
			"dds: publish payload length %d above cap %d: %w",
			len(msg.Payload),
			maxPayloadLen,
			ErrInvalidFrame,
		)
	}

	guid := writer

	ports := b.snapshotRoutes()
	if len(ports) == 0 {
		return routeError(msg.TopicName, snap.domain, ErrNoRoute)
	}

	cp := append([]byte(nil), msg.Payload...)

	if !fragNeedsSplit(uint32(len(cp))) { //nolint:gosec // G115: cp length capped by maxPayloadLen above, fits uint32
		err := b.sendData(ctx, guid, cp, ports)
		if err != nil {
			if errors.Is(err, ErrNoRoute) {
				return routeError(msg.TopicName, snap.domain, err)
			}

			return fmt.Errorf("dds: publish: %w", err)
		}

		return nil
	}

	sampleSize := uint32(len(cp)) //nolint:gosec // G115: cp length capped by maxPayloadLen above, fits uint32
	fragmentSize, total, err := planFragSend(sampleSize)
	if err != nil {
		return fmt.Errorf("dds: publish fragment: %w", err)
	}

	err = b.sendPlainFrags(ctx, guid, cp, sampleSize, fragmentSize, total, ports)
	if err != nil {
		if errors.Is(err, ErrNoRoute) {
			return routeError(msg.TopicName, snap.domain, err)
		}

		return fmt.Errorf("dds: publish: %w", err)
	}

	return nil
}

// loadPublishSnapshot validates context and lifecycle for one publish.
//
// It checks context cancellation plus closed and joined flags, then
// snapshots publisher state without holding the lock. A missing live
// path maps to ErrDataPathNotWired so callers keep one wiring check.
func (b *Backend) loadPublishSnapshot(ctx context.Context) (publishSnapshot, error) {
	err := ctx.Err()
	if err != nil {
		return publishSnapshot{}, fmt.Errorf("dds: publish: %w", err)
	}

	b.mu.RLock()

	closed := b.closed
	joined := b.joined

	b.mu.RUnlock()

	if closed {
		return publishSnapshot{}, fmt.Errorf("dds: publish after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return publishSnapshot{}, fmt.Errorf("dds: publish before join: %w", transports.ErrNotJoined)
	}

	snap := b.snapshotPublish()

	if !snap.live || snap.publisher == nil || snap.sedp == nil {
		return publishSnapshot{}, fmt.Errorf("dds: publish data path not wired yet: %w", ErrDataPathNotWired)
	}

	return snap, nil
}

// alignPublisherGUID moves the static publisher to the SEDP writer entity.
//
// DATA must carry the announced user entity such as 00 10 00 03. The first
// publish declares that entity while the publisher still holds the legacy
// 00 00 02 03, so without alignment CycloneDDS sees DATA from an unknown
// writer. Reusing one writer per topic keeps the GUID stable after the
// first alignment and preserves sequence continuity. Lock order is
// publisher then backend, matching PublishTo through frameSink, so
// concurrent publishes cannot deadlock.
func (b *Backend) alignPublisherGUID(snapPublisher *Publisher, writer WriterGUID) {
	if snapPublisher == nil {
		return
	}

	if snapPublisher.GUID() == writer {
		b.mu.Lock()
		needsBackend := b.pubGUID != writer
		if needsBackend {
			b.pubGUID = writer
		}
		b.mu.Unlock()

		return
	}

	snapPublisher.setGUID(writer)

	b.mu.Lock()
	b.pubGUID = writer
	b.mu.Unlock()
}

// dispatchPublish declares the writer and sends through the selected path.
//
// It checks routes before sending, uses plain DATA for interop samples, and
// falls back to framed PublishTo otherwise. The SEDP writer is announced
// synchronously before DATA so single shot publishers do not exit before the
// periodic loop sends it. Route misses map to routeError so the CLI can
// retry with backoff.
func (b *Backend) dispatchPublish(
	ctx context.Context,
	msg transports.OutgoingMessage,
	snap publishSnapshot,
) error {
	ep, err := b.declarePublishWriter(snap.sedp, msg)
	if err != nil {
		return err
	}

	if snap.routeCount == 0 && len(b.snapshotRoutes()) == 0 {
		return routeError(msg.TopicName, snap.domain, ErrNoRoute)
	}

	writer := b.writerForPublish(snap, ep)

	err = b.announceWriterForPublish(ctx, msg, ep, writer, snap)
	if err != nil {
		return err
	}

	if isPlainCDRSample(msg.Payload, msg.TypeName) {
		err = b.publishPlain(ctx, msg, snap, writer)
		if err != nil {
			return err
		}

		return nil
	}

	sink := &frameSink{backend: b}

	err = snap.publisher.PublishTo(ctx, sink, msg)
	if err != nil {
		if errors.Is(err, ErrNoRoute) {
			return routeError(msg.TopicName, snap.domain, err)
		}

		return fmt.Errorf("dds: publish: %w", err)
	}

	return nil
}

// writerForPublish returns the writer GUID for one publish.
func (b *Backend) writerForPublish(snap publishSnapshot, ep SEDPEndpoint) WriterGUID {
	if ep.ID != (SEDPGUID{}) {
		return writerGUIDFromEndpoint(ep.ID)
	}

	return snap.publisher.GUID()
}

// announceWriterForPublish sends synchronous SEDP plus matching wait for interop writers.
func (b *Backend) announceWriterForPublish(
	ctx context.Context,
	msg transports.OutgoingMessage,
	ep SEDPEndpoint,
	writer WriterGUID,
	snap publishSnapshot,
) error {
	if ep.ID == (SEDPGUID{}) {
		return nil
	}

	err := b.announceWriterOnce(ctx, ep)
	if err != nil {
		if errors.Is(err, ErrNoRoute) {
			return routeError(msg.TopicName, snap.domain, err)
		}

		return err
	}

	b.alignPublisherGUID(snap.publisher, writer)

	if !isPlainCDRSample(msg.Payload, msg.TypeName) {
		return nil
	}

	b.waitForMatchingReader(ctx, ep)

	err = b.pauseForPeerProcessing(ctx)
	if err != nil {
		return err
	}

	return nil
}

// PublishBytes publishes one serialized message through the data path.
func (b *Backend) PublishBytes(ctx context.Context, msg transports.OutgoingMessage) error {
	snap, err := b.loadPublishSnapshot(ctx)
	if err != nil {
		return err
	}

	err = b.dispatchPublish(ctx, msg, snap)
	if err != nil {
		return err
	}

	return nil
}

// TakeBytes receives one serialized message through the data path.
func (b *Backend) TakeBytes(ctx context.Context) (transports.IncomingMessage, error) {
	err := ctx.Err()
	if err != nil {
		return transports.IncomingMessage{}, fmt.Errorf("dds: take: %w", err)
	}

	b.mu.RLock()

	closed := b.closed
	joined := b.joined
	receiver := b.receiver
	live := b.uni != nil && b.mcast != nil && b.local != nil

	b.mu.RUnlock()

	if closed {
		return transports.IncomingMessage{}, fmt.Errorf("dds: take after close: %w", transports.ErrBackendClosed)
	}

	if !joined {
		return transports.IncomingMessage{}, fmt.Errorf("dds: take before join: %w", transports.ErrNotJoined)
	}

	if !live || receiver == nil {
		return transports.IncomingMessage{}, fmt.Errorf("dds: take data path not wired yet: %w", ErrDataPathNotWired)
	}

	source := &frameSource{backend: b}

	msg, err := receiver.TakeFrom(ctx, source)
	if err != nil {
		return b.mapTakeError(ctx, err)
	}

	return msg, nil
}

// mapTakeError converts take failures to closed or wrapped errors.
func (b *Backend) mapTakeError(
	ctx context.Context,
	err error,
) (transports.IncomingMessage, error) {
	cerr := ctx.Err()
	if cerr != nil {
		return transports.IncomingMessage{}, fmt.Errorf("dds: take: %w", cerr)
	}

	if b.isClosed() {
		return transports.IncomingMessage{}, fmt.Errorf("dds: take after close: %w", transports.ErrBackendClosed)
	}

	return transports.IncomingMessage{}, fmt.Errorf("dds: take: %w", err)
}

// sendGoodbyeBestEffort sends one SPDP dispose before teardown.
func (b *Backend) sendGoodbyeBestEffort(local *LocalParticipant) {
	if local == nil {
		return
	}
	b.mu.RLock()
	live := b.uni != nil && b.mcast != nil
	b.mu.RUnlock()
	if !live {
		return
	}
	goodbyeCtx, goodbyeCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer goodbyeCancel()
	sender := &spdpSender{backend: b}
	err := local.Goodbye(goodbyeCtx, sender)
	if err != nil {
		log.Printf("dds: goodbye send failed: %v", err)
	}
}

// Close releases the backend handle and stops discovery loops.
func (b *Backend) Close() error {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()

		return nil
	}

	b.closed = true
	local := b.local
	b.mu.Unlock()

	b.sendGoodbyeBestEffort(local)

	b.mu.Lock()
	cancel := b.runCancel
	peer := b.peer
	done := b.done
	mcastRx := b.mcastRx
	mcastUserRx := b.mcastUserRx
	b.peer = nil
	b.runCancel = nil
	b.uni = nil
	b.mcast = nil
	b.mcastRx = nil
	b.mcastUserRx = nil
	b.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	if done != nil {
		close(done)
	}

	if peer != nil {
		err := peer.Close()
		if err != nil {
			log.Printf("dds: peer close failed: %v", err)
		}
	}

	if mcastRx != nil {
		err := mcastRx.Close()
		if err != nil {
			log.Printf("dds: multicast receive close failed: %v", err)
		}
	}

	if mcastUserRx != nil {
		err := mcastUserRx.Close()
		if err != nil {
			log.Printf("dds: user multicast receive close failed: %v", err)
		}
	}

	b.runWG.Wait()

	return nil
}

// Compile time check that Backend exposes the Backend contract.
var _ transports.Backend = (*Backend)(nil)

// Compile time checks for live adapters.
var (
	_ Sender       = (*spdpSender)(nil)
	_ Receiver     = (*spdpReceiver)(nil)
	_ SEDPSender   = (*sedpSender)(nil)
	_ SEDPReceiver = (*sedpReceiver)(nil)
	_ FrameSink    = (*frameSink)(nil)
	_ FragSink     = (*frameSink)(nil)
	_ FragSink     = (*Backend)(nil)
	_ FrameSource  = (*frameSource)(nil)
)
