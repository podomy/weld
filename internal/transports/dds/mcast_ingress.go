// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"context"
	"log"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// datagramSource names the socket a datagram arrived on.
// It keeps multicast discovery distinguishable from directed unicast in logs
// so vendor interop issues can be traced to one path without guessing.
type datagramSource string

const (
	// sourceMulticast marks datagrams read from the SPDP multicast group.
	// It separates shared group traffic from directed traffic in demux logs.
	sourceMulticast datagramSource = "multicast"
	// sourceUnicast marks datagrams read from the unicast socket.
	// It is kept beside multicast so shared dispatch logs stay comparable.
	sourceUnicast datagramSource = "unicast"
)

// joinMulticastBestEffort joins the SPDP group without failing discovery.
// It returns nil when the standard port is held exclusively so unicast
// bootstrap keeps working and no EADDRINUSE regression appears.
func joinMulticastBestEffort(ctx context.Context, domain uint32) *rtps.MulticastConn {
	conn, err := rtps.JoinMulticast(ctx, domain)
	if err != nil {
		log.Printf("dds: multicast join failed, continuing unicast-only on domain %d: %v", domain, err)
		return nil
	}

	return conn
}

// joinUserMulticastBestEffort joins the user-traffic group without failing
// discovery. A nil return keeps SPDP-only behavior when the user port is
// held exclusively.
func joinUserMulticastBestEffort(ctx context.Context, domain uint32) *rtps.MulticastConn {
	conn, err := rtps.JoinUserMulticast(ctx, domain)
	if err != nil {
		log.Printf("dds: user multicast join failed, continuing without user fanout on domain %d: %v", domain, err)
		return nil
	}

	return conn
}

// userMulticastPortOrZero reports the joined user port or zero when absent.
func userMulticastPortOrZero(conn *rtps.MulticastConn) int {
	if conn == nil {
		return 0
	}

	return conn.Port()
}

// isSelfDatagram reports whether data carries the local participant prefix.
// It filters our own multicast loopback before dispatch so self SPDP never
// re-enters discovery and self SEDP never installs self as a remote.
func isSelfDatagram(data []byte, local *LocalParticipant) bool {
	if local == nil {
		return false
	}

	if len(data) == 0 {
		return false
	}

	header, err := rtps.DecodeHeader(data)
	if err != nil {
		return false
	}

	guid := local.GUID()

	return bytes.Equal(header.Prefix[:], guid.Prefix[:])
}

// snapshotMcastIngress captures the group socket plus shared queues under lock.
// It shares the single dispatch channels with the unicast demux so both paths
// feed one ingest and teardown stays consistent under Close.
func (b *Backend) snapshotMcastIngress() (*rtps.MulticastConn, chan []byte, chan []byte, chan dataItem, *LocalParticipant, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.mcastRx == nil || b.spdpCh == nil || b.sedpCh == nil || b.dataCh == nil {
		return nil, nil, nil, nil, nil, false
	}

	return b.mcastRx, b.spdpCh, b.sedpCh, b.dataCh, b.local, true
}

// ingestMulticastDatagram feeds one group datagram through the shared dispatch.
// It drops loopback self sends first, then follows the unicast demux order for
// legacy prefixes and RTPS magic so multicast SEDP installs writers that
// unicast-only mode missed without changing unicast behavior.
func (b *Backend) ingestMulticastDatagram(ctx context.Context, data []byte, local *LocalParticipant, spdpCh, sedpCh chan []byte, dataCh chan dataItem) {
	err := ctx.Err()
	if err != nil {
		return
	}

	if isSelfDatagram(data, local) {
		return
	}

	if bytes.HasPrefix(data, []byte(legacySEDPrefix)) {
		b.enqueueSEDP(ctx, sedpCh, data)

		return
	}

	if bytes.HasPrefix(data, []byte(legacySPDPrefix)) {
		b.enqueueSPDP(ctx, spdpCh, data)

		return
	}

	if !bytes.HasPrefix(data, []byte(rtps.Magic)) {
		b.bumpDemuxError()
		sampleDropLog("dds: %s demux dropped %d non-RTPS bytes", sourceLabel(true), len(data))

		return
	}

	b.dispatchRTPS(ctx, data, spdpCh, sedpCh, dataCh)
}

// mcastIngressLoop polls one group socket and reuses the shared dispatch.
// It runs beside the unicast demux so one logical demux feeds every socket
// and reception stays additive; Close cancels the context and closes the
// socket so the loop exits idempotently. The snapshot func selects the
// SPDP or user-traffic socket; both loops share all queues.
func (b *Backend) mcastIngressLoop(ctx context.Context, snapshot func() (*rtps.MulticastConn, chan []byte, chan []byte, chan dataItem, *LocalParticipant, bool)) {
	defer b.runWG.Done()

	for {
		err := ctx.Err()
		if err != nil {
			return
		}

		mcast, spdpCh, sedpCh, dataCh, local, ok := snapshot()
		if !ok {
			return
		}

		if !b.handleMcastReceive(ctx, mcast, spdpCh, sedpCh, dataCh, local) {
			return
		}
	}
}

// handleMcastReceive reads one group datagram and reports whether to continue.
func (b *Backend) handleMcastReceive(ctx context.Context, mcast *rtps.MulticastConn, spdpCh, sedpCh chan []byte, dataCh chan dataItem, local *LocalParticipant) bool {
	data, err := mcast.Receive(ctx)
	if err != nil {
		cerr := ctx.Err()
		if cerr != nil {
			return false
		}

		if b.isClosed() {
			return false
		}

		b.bumpDemuxError()
		log.Printf("dds: %s demux receive failed: %v", sourceLabel(true), err)

		return true
	}

	if len(data) < 4 {
		b.bumpDemuxError()
		sampleDropLog("dds: %s demux dropped short %d byte datagram", sourceLabel(true), len(data))

		return true
	}

	b.ingestMulticastDatagram(ctx, data, local, spdpCh, sedpCh, dataCh)

	return true
}

// snapshotMcastUserIngress captures the user-traffic socket plus queues.
func (b *Backend) snapshotMcastUserIngress() (*rtps.MulticastConn, chan []byte, chan []byte, chan dataItem, *LocalParticipant, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.mcastUserRx == nil || b.spdpCh == nil || b.sedpCh == nil || b.dataCh == nil {
		return nil, nil, nil, nil, nil, false
	}

	return b.mcastUserRx, b.spdpCh, b.sedpCh, b.dataCh, b.local, true
}

// startMcastIngressLocked starts the group loops with the caller holding mu.
// It shares the same lock that Close uses so WaitGroup Adds cannot race
// teardown and a missing group socket simply means unicast-only mode.
// The user-traffic loop is independent: SPDP still works when the user
// port is held exclusively, and vice versa.
func (b *Backend) startMcastIngressLocked(ctx context.Context) {
	if b.mcastRx != nil {
		b.runWG.Add(1)
		go b.mcastIngressLoop(ctx, b.snapshotMcastIngress)
	}

	if b.mcastUserRx != nil {
		b.runWG.Add(1)
		go b.mcastIngressLoop(ctx, b.snapshotMcastUserIngress)
	}
}

// sourceLabel returns the log label for one ingress path.
// It keeps both path constants referenced so shared dispatch logs stay
// comparable when unicast and multicast drops are compared.
func sourceLabel(multicast bool) datagramSource {
	if multicast {
		return sourceMulticast
	}

	return sourceUnicast
}
