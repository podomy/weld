// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"context"
	"fmt"
	"log"
	"maps"
	"net"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// Reconnect for the live MQTT session.
//
// A socket drop redials with backoff, resubscribes the stored filters,
// and replays unacked samples: QoS 1 republishes with DUP set, QoS 2
// replays PUBLISH or PUBREL per handshake phase. The receive queue
// survives across redials, so takes keep blocking across the outage
// unless the context ends. Every successful redial bumps the
// reconnect counter for observability. QoS 0 samples in flight during
// the drop are lost by protocol design; callers that need them use
// QoS 1.

// reconnect redials until success, close, or context end.
//
// It reports true after a live session resumes and false when Close
// or the lifetime context ends the loop. Only the read loop calls
// it, so attempts never overlap.
func (c *netConn) reconnect(ctx context.Context) bool {
	c.reconMu.Lock()
	defer c.reconMu.Unlock()

	if c.snapshotClosed() {
		return false
	}

	delay := reconnectInitialDelay

	for {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
		}

		if c.snapshotClosed() {
			return false
		}

		if c.tryReconnect(ctx) {
			return true
		}

		delay = backoffNext(delay)
	}
}

// tryReconnect performs one redial plus handshake plus resume round.
//
// Handshake failures log loudly: unlike dial failures they usually
// mean a config problem such as bad credentials or a version the
// broker refuses, not a transient outage.
func (c *netConn) tryReconnect(ctx context.Context) bool {
	raw, err := c.dialOnce(ctx)
	if err != nil {
		return false
	}

	if err := c.handshake(ctx, raw); err != nil {
		log.Printf("mqtt: handshake retry: %v", err)
		c.closeQuietly(raw, "handshake conn")

		return false
	}

	if err := c.resume(ctx, raw); err != nil {
		c.closeQuietly(raw, "resume conn")

		return false
	}

	return true
}

// closeQuietly closes one socket, logging failures.
func (c *netConn) closeQuietly(raw net.Conn, what string) {
	if err := raw.Close(); err != nil {
		log.Printf("mqtt: close failed %s: %v", what, err)
	}
}

// backoffNext doubles one delay up to the reconnect cap.
func backoffNext(delay time.Duration) time.Duration {
	doubled := delay * 2
	if doubled > reconnectMaxDelay {
		return reconnectMaxDelay
	}

	return doubled
}

// resume swaps in one live socket and restores the session.
//
// It resubscribes every stored filter, replays every unacked sample,
// then swaps the socket so the read loop resumes on the live
// connection. QoS 1 samples replay with DUP set; QoS 2 samples replay
// per phase, PUBLISH with payload while PUBREC is awaited and bare
// PUBREL while PUBCOMP is awaited. PUBLISH packets arriving mid
// resubscribe are buffered and dispatched after the swap so retained
// or in-flight samples are never dropped. Inbound QoS 2 ids survive
// resume so redelivery still dedups under clean start. Any failure
// leaves the old state for the next backoff round.
func (c *netConn) resume(life context.Context, raw net.Conn) error {
	c.mu.Lock()
	filters := make(map[string]byte, len(c.filters))
	maps.Copy(filters, c.filters)

	pending := make(map[uint16]pendingPublish, len(c.inflight))
	maps.Copy(pending, c.inflight)
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(life, 10*time.Second)
	defer cancel()

	if deadline, ok := ctx.Deadline(); ok {
		if err := raw.SetDeadline(deadline); err != nil {
			return fmt.Errorf("mqtt: resume deadline: %w", err)
		}

		defer resetDeadline(raw)
	}

	old := c.currentConn()

	buffered, overrun, err := c.resubscribeAll(ctx, raw, filters)
	if err != nil {
		c.absorbResumeLeftovers(life, buffered, overrun)

		return err
	}

	c.absorbResumeLeftovers(life, nil, overrun)

	if err := republishPending(c.version, raw, pending); err != nil {
		c.absorbResumeLeftovers(life, buffered, 0)

		return err
	}

	// The closed check and the socket swap run under writeMu while the
	// whole resume holds reconMu, which Close also holds across its
	// goodbye plus socket close. So a concurrent Close either closes
	// the swapped socket here or this path discards the fresh socket.
	// No live socket survives Close.
	c.writeMu.Lock()
	c.mu.Lock()
	closed := c.closed
	if !closed {
		c.conn = raw
	}
	c.mu.Unlock()

	if closed {
		c.writeMu.Unlock()
		c.closeQuietly(raw, "resume after close")

		return fmt.Errorf("mqtt: resume after close: %w", transports.ErrBackendClosed)
	}

	c.writeMu.Unlock()

	if old != nil {
		if cerr := old.Close(); cerr != nil {
			log.Printf("mqtt: close replaced conn: %v", cerr)
		}
	}

	c.mu.Lock()
	c.reconnects++
	c.readErr = nil
	c.mu.Unlock()

	for _, held := range buffered {
		c.dispatchPacket(life, held)
	}

	c.noteRecv()
	c.broadcast()

	return nil
}

// absorbResumeLeftovers counts dropped mid-resume samples and queues
// the buffered ones for dispatch, even on the resume error path where
// the fresh socket is discarded. Queued samples are never silently
// lost: the broker redelivers unacked QoS 1 and QoS 2, and QoS 0 loss
// stays by design.
func (c *netConn) absorbResumeLeftovers(ctx context.Context, buffered [][]byte, overrun int) {
	if overrun > 0 {
		c.mu.Lock()
		c.dropped += uint64(overrun)
		c.mu.Unlock()

		logResumeDropped(overrun)
	}

	for _, held := range buffered {
		// Only PUBLISH is ever buffered, so this cannot reach the
		// DISCONNECT path and its reconnect, which would deadlock on
		// the reconMu this resume already holds. The guard keeps that
		// true if buffering ever widens.
		if len(held) > 0 && held[0]>>4 == packetPublish {
			c.dispatchPacket(ctx, held)
		}
	}
}

// resubscribeAll restores every stored filter on the fresh socket and
// returns PUBLISH packets that arrived mid resubscribe for dispatch
// after the socket swap.
func (c *netConn) resubscribeAll(ctx context.Context, raw net.Conn, filters map[string]byte) ([][]byte, int, error) {
	var buffered [][]byte
	dropped := 0

	for filter, qos := range filters {
		held, lost, err := c.subscribeOn(ctx, raw, filter, qos)
		dropped += lost
		buffered = append(buffered, held...)

		if err != nil {
			return buffered, dropped, fmt.Errorf("mqtt: resume subscribe %q: %w", filter, err)
		}
	}

	return buffered, dropped, nil
}

// republishPending replays every unacked sample: QoS 1 PUBLISH with
// DUP set, QoS 2 PUBLISH with DUP while PUBREC is awaited, and bare
// PUBREL while PUBCOMP is awaited.
func republishPending(version Version, raw net.Conn, pending map[uint16]pendingPublish) error {
	for id, sample := range pending {
		if sample.qos == 2 && sample.phase == phasePubRelSent {
			if err := republishPubRel(raw, id); err != nil {
				return err
			}

			continue
		}

		packet, err := EncodePublishDup(version, sample.topic, id, sample.qos, sample.retained, true, sample.props, sample.payload)
		if err != nil {
			return err
		}

		if err := writePacket(raw, packet); err != nil {
			return fmt.Errorf("mqtt: resume republish: %w", err)
		}
	}

	return nil
}

// republishPubRel replays one bare PUBREL for a QoS 2 sample awaiting
// PUBCOMP. PUBREL carries no DUP flag, so the replay is byte identical.
func republishPubRel(raw net.Conn, id uint16) error {
	packet, err := EncodePubRel(id)
	if err != nil {
		return err
	}

	if err := writePacket(raw, packet); err != nil {
		return fmt.Errorf("mqtt: resume republish pubrel: %w", err)
	}

	return nil
}

// currentConn returns the live socket under writeMu.
//
// It briefly blocks writers holding writeMu; that is the price of
// reading the socket the swap mutates. Callers on the read path
// accept the wait because writes are deadline-bounded.
func (c *netConn) currentConn() net.Conn {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return c.conn
}

// subscribeOn sends one SUBSCRIBE over an explicit socket and returns
// PUBLISH packets that arrived while awaiting its SUBACK, plus the
// count dropped past the resume buffer bound.
func (c *netConn) subscribeOn(ctx context.Context, raw net.Conn, filter string, qos byte) ([][]byte, int, error) {
	id := c.nextPacketID()

	packet, err := EncodeSubscribe(c.version, id, filter, qos)
	if err != nil {
		return nil, 0, err
	}

	err = writePacket(raw, packet)
	if err != nil {
		return nil, 0, fmt.Errorf("mqtt: resume subscribe write: %w", err)
	}

	rawPacket, buffered, dropped, err := readSubscribeAck(ctx, raw, c.version, id)
	if err != nil {
		return buffered, dropped, err
	}

	if rawPacket != id {
		return nil, 0, fmt.Errorf("mqtt: resume suback id %d mismatches %d", rawPacket, id)
	}

	return buffered, dropped, nil
}

// readSubscribeAck reads packets until the matching SUBACK arrives,
// buffering PUBLISH packets for dispatch after the socket swap.
//
// The caller context bounds the wait through a socket deadline.
// PUBLISH packets arriving mid resubscribe are held up to maxNetQueue:
// clean start sessions may deliver retained samples here, and dropping
// a QoS 2 sample without answering PUBREC would stall the broker.
// Beyond the bound samples drop loudly and senders replay unacked
// QoS 1 and QoS 2; QoS 0 mid-resume loss is by design. Other packet
// types are still ignored; broker retries cover those.
func readSubscribeAck(ctx context.Context, raw net.Conn, version Version, id uint16) (uint16, [][]byte, int, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, 0, fmt.Errorf("mqtt: resume subscribe context: %w", err)
	}

	if deadline, ok := ctx.Deadline(); ok {
		if err := raw.SetDeadline(deadline); err != nil {
			return 0, nil, 0, fmt.Errorf("mqtt: resume subscribe deadline: %w", err)
		}

		// No reset here: the deadline belongs to the outer resume
		// envelope, which clears it when the whole resume (resubscribe
		// plus replay plus swap) finishes. Resetting per SUBACK would
		// leave later subscribe writes and all replay writes with no
		// deadline, hanging Close on reconMu against a stuck broker.
	}

	var buffered [][]byte
	dropped := 0

	for {
		if err := ctx.Err(); err != nil {
			return 0, buffered, dropped, fmt.Errorf("mqtt: resume subscribe context: %w", err)
		}

		got, matched, held, err := awaitSubAck(raw, version, id)
		if err != nil {
			return 0, buffered, dropped, err
		}

		buffered, dropped = admitResumePacket(buffered, held, dropped)

		if matched {
			return got, buffered, dropped, nil
		}
	}
}

// admitResumePacket holds mid-resume packets within the bound,
// counting drops past it for the loud summary at swap time.
func admitResumePacket(buffered, held [][]byte, dropped int) ([][]byte, int) {
	if len(buffered)+len(held) > maxNetQueue {
		return buffered, dropped + len(held)
	}

	return append(buffered, held...), dropped
}

// logResumeDropped reports one resume buffer overrun loudly.
func logResumeDropped(dropped int) {
	if dropped > 0 {
		log.Printf("mqtt: resume dropped %d mid-resubscribe publishes over buffer bound", dropped)
	}
}

// awaitSubAck reads one packet, reporting the SUBACK match plus any
// PUBLISH packet to hold for dispatch after the socket swap.
func awaitSubAck(raw net.Conn, version Version, id uint16) (uint16, bool, [][]byte, error) {
	packet, err := readPacket(raw)
	if err != nil {
		return 0, false, nil, err
	}

	if len(packet) == 0 {
		return 0, false, nil, nil
	}

	if packet[0]>>4 == packetPublish {
		return 0, false, [][]byte{packet}, nil
	}

	if packet[0]>>4 != packetSubAck {
		return 0, false, nil, nil
	}

	got, err := decodeAckID(packet, packetSubAck)
	if err != nil {
		return 0, false, nil, err
	}

	if got != id {
		return 0, false, nil, nil
	}

	if err := checkAckResult(version, packet); err != nil {
		return 0, false, nil, err
	}

	return got, true, nil, nil
}

// resetDeadline clears one socket deadline, logging failures.
func resetDeadline(raw net.Conn) {
	if err := raw.SetDeadline(time.Time{}); err != nil {
		log.Printf("mqtt: reset deadline: %v", err)
	}
}
