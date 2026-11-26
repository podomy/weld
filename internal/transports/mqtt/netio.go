// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// Socket IO for the live MQTT v5 session.
//
// writePacket writes one whole packet. readPacket reads one whole
// packet framed by the fixed header plus remaining length. The read
// loop demultiplexes PUBLISH into the receive queue, PUBACK plus
// SUBACK plus PUBREC plus PUBCOMP into their waiters, inbound PUBREL
// into PUBCOMP answers, and PINGRESP into the keepalive path. QoS 2
// inbound PUBLISH answers PUBREC and enqueues exactly once. A socket
// error triggers reconnect; takes keep blocking across the outage and
// resume on the live socket, and receivers fail loudly only after
// Close.

// writePacket writes one packet without retaining it.
func writePacket(conn net.Conn, packet []byte) error {
	total := 0

	for total < len(packet) {
		n, err := conn.Write(packet[total:])
		if err != nil {
			return fmt.Errorf("mqtt: write packet: %w", err)
		}

		total += n
	}

	return nil
}

// maxInboundPacket caps one framed packet from the wire.
//
// The remaining length field is attacker controlled up to 256 MiB,
// so an uncapped read allocates that much before the first body byte
// arrives. The cap covers the largest sample weld itself can send: a
// 32 MiB payload plus a 64 KiB topic plus 32 max-size user properties
// (about 38 MiB all told), with headroom. Anything larger is a
// corrupt or hostile peer and fails loudly here.
const maxInboundPacket = 40 << 20

// readPacket reads one framed packet from the socket.
func readPacket(conn net.Conn) ([]byte, error) {
	header := make([]byte, 1)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		return nil, fmt.Errorf("mqtt: read fixed header: %w", err)
	}

	length, err := readVarint(conn)
	if err != nil {
		return nil, err
	}

	if length.value > maxInboundPacket {
		return nil, fmt.Errorf("mqtt: remaining length %d above maximum %d", length.value, maxInboundPacket)
	}

	out := make([]byte, 0, 1+len(length.prefix)+int(length.value))
	out = append(out, header[0])
	out = append(out, length.prefix...)

	body := make([]byte, length.value)
	_, err = io.ReadFull(conn, body)
	if err != nil {
		return nil, fmt.Errorf("mqtt: read body: %w", err)
	}

	return append(out, body...), nil
}

// varintRead carries one decoded length plus its wire bytes.
type varintRead struct {
	value  uint32
	prefix []byte
}

// readVarint reads one variable byte integer from the socket.
func readVarint(conn net.Conn) (varintRead, error) {
	var out varintRead
	var shift uint

	for range 4 {
		one := make([]byte, 1)
		_, err := io.ReadFull(conn, one)
		if err != nil {
			return out, fmt.Errorf("mqtt: read varint byte: %w", err)
		}

		out.prefix = append(out.prefix, one[0])
		out.value |= uint32(one[0]&0x7F) << shift

		if one[0]&0x80 == 0 {
			return out, nil
		}

		shift += 7
	}

	return out, errors.New("mqtt: varint exceeds 4 bytes") //nolint:wrapcheck // new error, nothing to wrap
}

// decodeAckID parses one PUBACK or SUBACK packet id plus reason.
func decodeAckID(packet []byte, wantType byte) (uint16, error) {
	if len(packet) < 2 {
		return 0, errors.New("mqtt: ack shorter than header") //nolint:wrapcheck // new error, nothing to wrap
	}

	if packet[0]>>4 != wantType {
		return 0, fmt.Errorf("mqtt: ack type %d is not %d", packet[0]>>4, wantType)
	}

	length, used, err := decodeVarint(packet[1:])
	if err != nil {
		return 0, err
	}

	body := packet[1+used:]

	if len(body) != int(length) {
		return 0, fmt.Errorf("mqtt: ack length %d mismatches %d", length, len(body))
	}

	if len(body) < 2 {
		return 0, errors.New("mqtt: ack body shorter than packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	id := uint16(body[0])<<8 | uint16(body[1])

	if id == 0 {
		return 0, errors.New("mqtt: ack carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	return id, nil
}

// completeWaiter delivers one ack result to its waiter without blocking.
func (c *netConn) completeWaiter(id uint16, err error) {
	c.mu.Lock()
	waiter, ok := c.waiters[id]
	delete(c.waiters, id)
	c.mu.Unlock()

	if !ok {
		return
	}

	select {
	case waiter <- err:
	default:
	}
}

// maxNetQueue bounds received PUBLISH samples waiting for Receive.
//
// The backend inbox behind this queue is bounded with evict-oldest, so
// an unbounded front queue would let a fast broker grow memory without
// bound. A full queue evicts the oldest sample and counts one drop.
const maxNetQueue = 1024

// pushPublish queues one decoded publish and acks QoS 1.
func (c *netConn) pushPublish(packet PublishPacket) {
	if packet.QoS == 1 {
		c.ackPublishPacket(packet.PacketID)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	if len(c.queue) >= maxNetQueue {
		copy(c.queue, c.queue[1:])
		c.queue[len(c.queue)-1] = ConnMessage{
			Topic:    packet.Topic,
			Payload:  packet.Payload,
			QoS:      packet.QoS,
			Retained: packet.Retained,
			Props:    packet.Props,
		}
		c.dropped++
		c.wake.Broadcast()

		return
	}

	c.queue = append(c.queue, ConnMessage{
		Topic:    packet.Topic,
		Payload:  packet.Payload,
		QoS:      packet.QoS,
		Retained: packet.Retained,
		Props:    packet.Props,
	})
	c.wake.Broadcast()
}

// Dropped returns the count of evicted receive samples plus best-effort
// ack write failures after the first per connection (see noteAckDrop).
func (c *netConn) Dropped() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.dropped
}

// ackPublishPacket encodes and sends one PUBACK best effort.
func (c *netConn) ackPublishPacket(id uint16) {
	ack, err := EncodePubAck(id)
	if err != nil {
		log.Printf("mqtt: puback encode: %v", err)

		return
	}

	c.sendAckBestEffort(ack, "puback")
}

// sendPubRec encodes and sends one PUBREC best effort for inbound QoS 2.
func (c *netConn) sendPubRec(id uint16) {
	rec, err := EncodePubRec(id)
	if err != nil {
		log.Printf("mqtt: pubrec encode: %v", err)

		return
	}

	c.sendAckBestEffort(rec, "pubrec")
}

// sendPubRel encodes and sends one outbound PUBREL best effort.
//
// PUBREL carries no DUP flag, so every replay sends identical bytes.
func (c *netConn) sendPubRel(id uint16) {
	rel, err := EncodePubRel(id)
	if err != nil {
		log.Printf("mqtt: pubrel encode: %v", err)

		return
	}

	c.sendAckBestEffort(rel, "pubrel")
}

// sendPubComp encodes and sends one PUBCOMP best effort for inbound QoS 2.
func (c *netConn) sendPubComp(id uint16) {
	comp, err := EncodePubComp(id)
	if err != nil {
		log.Printf("mqtt: pubcomp encode: %v", err)

		return
	}

	c.sendAckBestEffort(comp, "pubcomp")
}

// sendAckBestEffort writes one ack packet without failing the read loop.
func (c *netConn) sendAckBestEffort(packet []byte, op string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil {
		c.noteAckDrop(op, err)

		return
	}

	defer func() {
		if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
			c.noteAckDrop("clear "+op, err)
		}
	}()

	if err := writePacket(c.conn, packet); err != nil {
		c.noteAckDrop(op, err)
	}
}

// noteAckDrop counts one best-effort ack failure and logs only the
// first per connection. A hostile broker controls inbound publish
// rate, so logging every failure would hand it a log-fill dial;
// counting the rest in dropped (exposed via Dropped) keeps the signal
// without the spam. Lock order follows writeMu then mu.
func (c *netConn) noteAckDrop(op string, err error) {
	c.mu.Lock()
	first := !c.ackDropLogged
	c.ackDropLogged = true
	c.dropped++
	c.mu.Unlock()

	if first {
		log.Printf("mqtt: %s ack drop, further drops counted not logged: %v", op, err)
	}
}

// readLoop demultiplexes broker packets until close or context end.
//
// A socket drop triggers reconnect; takes keep blocking across the
// outage and resume on the live socket. Identity checks keep a stale
// reader from redialing after another path already resumed.
func (c *netConn) readLoop(ctx context.Context) {
	for {
		if !c.readOne(ctx) {
			return
		}
	}
}

// readOne reads and dispatches one packet, reporting whether to continue.
func (c *netConn) readOne(ctx context.Context) bool {
	failed := c.currentConn()

	packet, err := readPacket(failed)
	if err != nil {
		return c.handleReadError(ctx, failed)
	}

	c.noteRecv()

	if len(packet) == 0 {
		return true
	}

	c.dispatchPacket(ctx, packet)

	return true
}

// handleReadError reconnects after a socket drop.
//
// Stale readers yield when another path already resumed, and a closed
// connection ends the loop.
func (c *netConn) handleReadError(ctx context.Context, failed net.Conn) bool {
	if c.snapshotClosed() {
		return false
	}

	if c.currentConn() != failed {
		return true
	}

	c.broadcast()

	return c.reconnect(ctx)
}

// dispatchPacket routes one decoded packet to its handler.
//
// Only broker-to-client packets arrive here (PUBLISH, the ack family,
// PINGRESP, DISCONNECT). There are no decoders for CONNECT, SUBSCRIBE,
// PINGREQ, or client-sent AUTH by design: weld is the client and never
// receives them. Unknown types are ignored without effect.
func (c *netConn) dispatchPacket(ctx context.Context, packet []byte) {
	switch packet[0] >> 4 {
	case packetPublish:
		c.dispatchPublish(packet)
	case packetPubAck, packetSubAck, packetPubRec, packetPubComp, packetUnsubAck:
		c.dispatchAck(packet)
	case packetPubRel:
		c.dispatchPubRel(packet)
	case packetPingResp:
		// No-op: noteRecv already stamped this packet before dispatch,
		// which is all the keepalive needs.
	case packetDisconnect:
		// A broker DISCONNECT carries no state weld keeps, so any
		// reason triggers the same reconnect as a socket drop. Only
		// framing was validated on the way in; the reason code and
		// properties are intentionally not inspected.
		c.handleDisconnect(ctx)
	default:
	}
}

// dispatchPublish decodes and queues one PUBLISH, dropping malformed input.
//
// QoS 2 takes the receiver path: the first sighting of an id enqueues
// exactly once, and every sighting answers PUBREC.
func (c *netConn) dispatchPublish(packet []byte) {
	decoded, _, err := DecodePublish(c.version, packet)
	if err != nil {
		return
	}

	if decoded.QoS == 2 {
		c.receiveQoS2(decoded)

		return
	}

	c.pushPublish(decoded)
}

// receiveQoS2 records one inbound QoS 2 sample and answers PUBREC.
//
// An unseen id is stored, acked, then enqueued once through the same
// pushPublish path that counts loss. A redelivered id resends PUBREC
// without a second enqueue. Inbound PUBREL later releases the id.
func (c *netConn) receiveQoS2(packet PublishPacket) {
	c.mu.Lock()

	if _, seen := c.qos2recv[packet.PacketID]; seen {
		c.mu.Unlock()
		c.sendPubRec(packet.PacketID)

		return
	}

	c.qos2recv[packet.PacketID] = struct{}{}
	c.mu.Unlock()

	c.sendPubRec(packet.PacketID)
	c.pushPublish(packet)
}

// dispatchPubRel answers one inbound PUBREL with PUBCOMP.
//
// Framing is validated first: a malformed PUBREL is dropped without
// answering, since PUBCOMP to garbage would confirm nothing. A
// well-formed PUBREL always gets PUBCOMP, whatever its reason code,
// which is informational on this path; the sender side must converge.
// A stored id is released; an unknown id still gets PUBCOMP so a lost
// PUBCOMP converges without a second enqueue. PUBREL never enqueues.
func (c *netConn) dispatchPubRel(packet []byte) {
	if _, err := decodeAckBody(packet); err != nil {
		return
	}

	id, err := decodeAckID(packet, packetPubRel)
	if err != nil {
		return
	}

	c.mu.Lock()
	delete(c.qos2recv, id)
	c.mu.Unlock()

	c.sendPubComp(id)
}

// dispatchAck completes one PUBACK, SUBACK, PUBREC, or PUBCOMP waiter.
func (c *netConn) dispatchAck(packet []byte) {
	id, err := decodeAckID(packet, packet[0]>>4)
	if err != nil {
		return
	}

	result := checkAckResult(c.version, packet)

	switch packet[0] >> 4 {
	case packetPubAck:
		c.ackPublish(id, result)
	case packetPubComp:
		c.ackPubComp(id, result)
	case packetPubRec:
		c.handlePubRec(id, result)
	default:
		c.completeWaiter(id, result)
	}
}

// handleDisconnect reconnects on a broker sent disconnect.
func (c *netConn) handleDisconnect(ctx context.Context) {
	if c.snapshotClosed() {
		return
	}

	c.broadcast()

	if !c.reconnect(ctx) {
		return
	}
}

// ackPublish completes one PUBACK waiter and clears its inflight sample.
//
// Only a tracked QoS 1 sample is completed: a PUBACK for any other id
// (a subscribe waiter, a QoS 2 sample, or a stale id) is ignored
// instead of completing the wrong waiter, matching the spec's
// consume-without-effect branch. A refused publish fails the waiter
// instead of resending, since it would fail the same way on resend.
func (c *netConn) ackPublish(id uint16, result error) {
	c.mu.Lock()
	sample, ok := c.inflight[id]

	if !ok || sample.qos != 1 {
		c.mu.Unlock()

		return
	}

	delete(c.inflight, id)
	c.mu.Unlock()

	c.completeWaiter(id, result)
}

// ackPubComp completes one QoS 2 sample on its PUBCOMP.
//
// A PUBCOMP for an id that is not awaiting PUBCOMP is bogus or
// duplicated past completion and is ignored instead of completing
// a stale waiter, matching the spec's consume-without-effect branch.
func (c *netConn) ackPubComp(id uint16, result error) {
	c.mu.Lock()
	sample, ok := c.inflight[id]

	if !ok || sample.qos != 2 || sample.phase != phasePubRelSent {
		c.mu.Unlock()

		return
	}

	delete(c.inflight, id)
	c.mu.Unlock()

	c.completeWaiter(id, result)
}

// handlePubRec advances one QoS 2 sample past its PUBREC.
//
// On success it sends PUBREL best effort, marks the phase, and frees
// the stored payload plus props while keeping the topic and id for
// resume replay; the waiter stays until PUBCOMP. On a refused reason
// it clears the entry and fails the waiter like a refused PUBACK.
// PUBREC for an unknown or QoS 1 id is ignored.
func (c *netConn) handlePubRec(id uint16, result error) {
	c.mu.Lock()
	sample, ok := c.inflight[id]

	if !ok || sample.qos != 2 {
		c.mu.Unlock()

		return
	}

	if result != nil {
		delete(c.inflight, id)
		c.mu.Unlock()
		c.completeWaiter(id, result)

		return
	}

	sample.phase = phasePubRelSent
	sample.payload = nil
	sample.props = Properties{}
	c.inflight[id] = sample
	c.mu.Unlock()

	c.sendPubRel(id)
}

// pingLoop sends keepalive probes and watches for half-open links.
//
// A silent link trips the watchdog: with no inbound packet for half
// again the keepalive interval, the socket is closed so the read loop
// redials instead of stalling forever on a dead peer. A failed ping
// write never ends the loop; the watchdog or the read loop owns
// recovery so a transient write error cannot permanently disable
// keepalive after a successful redial.
func (c *netConn) pingLoop(interval time.Duration) {
	if interval <= 0 {
		interval = DefaultKeepAliveSecs * time.Second / 2
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.pingStop:
			return
		case <-ticker.C:
			if c.watchdogFired() {
				continue
			}

			if err := c.writePacketLocked(EncodePingReq(), "ping"); err != nil {
				log.Printf("mqtt: ping write: %v", err)
			}
		}
	}
}

// watchdogFired closes a silent socket and reports whether it did.
func (c *netConn) watchdogFired() bool {
	keepAlive := c.keepAlive
	if keepAlive <= 0 {
		// Hand-built connections bypassing Dial carry no keepalive;
		// fall back to the default instead of disabling death
		// detection while pingLoop still probes. Live sessions always
		// set a nonzero keepalive via keepAliveOf.
		keepAlive = DefaultKeepAliveSecs
	}

	threshold := time.Duration(keepAlive) * 3 * time.Second / 2
	if threshold <= 0 {
		return false
	}

	if time.Since(time.Unix(0, c.lastRecv.Load())) < threshold {
		return false
	}

	failed := c.currentConn()
	if c.snapshotClosed() || c.currentConn() != failed || failed == nil {
		return false
	}

	log.Printf("mqtt: watchdog closed silent connection")

	if err := failed.Close(); err != nil {
		log.Printf("mqtt: watchdog close: %v", err)
	}

	return true
}
