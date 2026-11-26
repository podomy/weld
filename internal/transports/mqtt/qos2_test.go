// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

// errQoS2Handshake marks a failed scripted handshake in QoS 2 tests.
var errQoS2Handshake = errors.New("mqtt: test qos2 handshake failed")

// serveQoS2Listener starts one TCP listener for a scripted QoS 2 broker.
func serveQoS2Listener(t *testing.T) net.Listener {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	return listener
}

// closeQoS2Listener releases one scripted listener, logging failures.
func closeQoS2Listener(t *testing.T, listener net.Listener) {
	t.Helper()

	if err := listener.Close(); err != nil {
		t.Logf("listener close: %v", err)
	}
}

// serveQoS2 runs one scripted session in the background and reports its error.
func serveQoS2(t *testing.T, listener net.Listener, script func(raw net.Conn) error) chan error {
	t.Helper()

	out := make(chan error, 1)

	go func() {
		raw, err := listener.Accept()
		if err != nil {
			out <- err

			return
		}

		defer func() {
			if cerr := raw.Close(); cerr != nil {
				t.Logf("script close: %v", cerr)
			}
		}()

		if !serveHandshake(raw, Version5, make(chan error, 1)) {
			out <- errQoS2Handshake

			return
		}

		out <- script(raw)
	}()

	return out
}

// dialQoS2Conn dials one live session against a scripted listener.
func dialQoS2Conn(t *testing.T, ctx context.Context, listener net.Listener, clientID string) *netConn {
	t.Helper()

	conn, err := Dial(ctx, MqttConfig{BrokerURL: "tcp://" + listener.Addr().String(), ClientID: clientID})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("conn close: %v", cerr)
		}
	})

	return conn
}

// awaitQoS2Script fails unless one scripted session ends cleanly.
func awaitQoS2Script(t *testing.T, ctx context.Context, done <-chan error, what string) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-ctx.Done():
		t.Fatalf("%s never finished", what)
	}
}

// readQoS2Publish reads one PUBLISH and decodes it for a script.
func readQoS2Publish(raw net.Conn) (PublishPacket, error) {
	packet, err := readPacket(raw)
	if err != nil {
		return PublishPacket{}, err
	}

	decoded, _, err := DecodePublish(Version5, packet)
	if err != nil {
		return PublishPacket{}, err
	}

	return decoded, nil
}

// finishQoS2Handshake runs one broker side exchange: it reads the client
// PUBLISH, checks its id plus DUP flag, then runs PUBREC plus PUBREL
// plus PUBCOMP to completion.
func finishQoS2Handshake(raw net.Conn, wantID uint16, wantDup bool) error {
	decoded, err := readQoS2Publish(raw)
	if err != nil {
		return err
	}

	if decoded.PacketID != wantID {
		return errors.New("mqtt: test qos2 replay id mismatch") //nolint:wrapcheck // test helper sentinel
	}

	if decoded.Dup != wantDup {
		return errors.New("mqtt: test qos2 replay dup mismatch") //nolint:wrapcheck // test helper sentinel
	}

	rec, err := EncodePubRec(decoded.PacketID)
	if err != nil {
		return err
	}

	if err := writePacket(raw, rec); err != nil {
		return err
	}

	rel, err := readPacket(raw)
	if err != nil {
		return err
	}

	relID, err := decodeAckID(rel, packetPubRel)
	if err != nil {
		return err
	}

	if relID != wantID {
		return errors.New("mqtt: test qos2 pubrel id mismatch") //nolint:wrapcheck // test helper sentinel
	}

	comp, err := EncodePubComp(relID)
	if err != nil {
		return err
	}

	return writePacket(raw, comp)
}

// awaitQoS2ID receives one broker side packet id or fails the test.
func awaitQoS2ID(t *testing.T, ctx context.Context, ch <-chan uint16, what string) uint16 {
	t.Helper()

	select {
	case id := <-ch:
		return id
	case <-ctx.Done():
		t.Fatalf("broker sent no %s", what)

		return 0
	}
}

// awaitQoS2Publish waits for one client publish result or fails the test.
func awaitQoS2Publish(t *testing.T, ctx context.Context, ch <-chan error, what string) {
	t.Helper()

	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	case <-ctx.Done():
		t.Fatalf("%s never acked", what)
	}
}

// inflightQoS2Len reports the count of retained unacked samples.
func inflightQoS2Len(conn *netConn) int {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	return len(conn.inflight)
}

// seenQoS2 carries one observed broker side QoS 2 exchange.
type seenQoS2 struct {
	publish PublishPacket
	pubrel  uint16
}

// runQoS2HappyScript runs one broker side PUBLISH plus PUBREC plus
// PUBREL plus PUBCOMP exchange and reports what it observed.
func runQoS2HappyScript(raw net.Conn, observed chan seenQoS2) error {
	decoded, err := readQoS2Publish(raw)
	if err != nil {
		return err
	}

	relID, err := answerQoS2Publish(raw, decoded.PacketID)
	if err != nil {
		return err
	}

	observed <- seenQoS2{publish: decoded, pubrel: relID}

	return nil
}

// answerQoS2Publish sends PUBREC for one PUBLISH, reads the PUBREL,
// answers PUBCOMP, and reports the PUBREL id.
func answerQoS2Publish(raw net.Conn, id uint16) (uint16, error) {
	rec, err := EncodePubRec(id)
	if err != nil {
		return 0, err
	}

	if err := writePacket(raw, rec); err != nil {
		return 0, err
	}

	rel, err := readPacket(raw)
	if err != nil {
		return 0, err
	}

	relID, err := decodeAckID(rel, packetPubRel)
	if err != nil {
		return 0, err
	}

	comp, err := EncodePubComp(relID)
	if err != nil {
		return 0, err
	}

	if err := writePacket(raw, comp); err != nil {
		return 0, err
	}

	return relID, nil
}

// TestQoS2HappyPath proves PUBLISH plus PUBREC plus PUBREL plus PUBCOMP.
func TestQoS2HappyPath(t *testing.T) {
	t.Parallel()

	listener := serveQoS2Listener(t)
	defer closeQoS2Listener(t, listener)

	observed := make(chan seenQoS2, 1)

	done := serveQoS2(t, listener, func(raw net.Conn) error {
		return runQoS2HappyScript(raw, observed)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dialQoS2Conn(t, ctx, listener, "weld-qos2-happy")

	err := conn.Publish(ctx, "weld/0/rt/chatter", 2, false, []byte{0x05}, Properties{TypeHash: "RIHS01_qos2"})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	awaitQoS2Script(t, ctx, done, "qos2 happy path")

	var got seenQoS2

	select {
	case got = <-observed:
	case <-ctx.Done():
		t.Fatalf("broker saw no publish")
	}

	if got.publish.Topic != "weld/0/rt/chatter" || got.publish.QoS != 2 {
		t.Fatalf("broker publish = %+v, want qos 2 chatter", got.publish)
	}

	if got.publish.Dup {
		t.Fatalf("broker publish dup is true, want false")
	}

	if string(got.publish.Payload) != "\x05" || got.publish.Props.TypeHash != "RIHS01_qos2" {
		t.Fatalf("broker publish payload or hash = %+v, want 0x05 with hash", got.publish)
	}

	if got.pubrel != got.publish.PacketID {
		t.Fatalf("pubrel id = %d, want %d", got.pubrel, got.publish.PacketID)
	}
}

// runQoS2DedupScript sends one QoS 2 PUBLISH twice and collects PUBRECs.
func runQoS2DedupScript(raw net.Conn, recs chan uint16) error {
	packet, err := EncodePublish(Version5, "weld/0/rt/a", 7, 2, false, Properties{}, []byte{0x09})
	if err != nil {
		return err
	}

	for range 2 {
		if err := writePacket(raw, packet); err != nil {
			return err
		}
	}

	return collectQoS2IDs(raw, packetPubRec, recs, 2)
}

// collectQoS2IDs reads count acks of one type and reports their ids.
func collectQoS2IDs(raw net.Conn, ptype byte, out chan uint16, count int) error {
	for range count {
		ack, err := readPacket(raw)
		if err != nil {
			return err
		}

		id, err := decodeAckID(ack, ptype)
		if err != nil {
			return err
		}

		out <- id
	}

	return nil
}

// TestQoS2DuplicatePublishDedups proves a redelivered PUBLISH enqueues once.
func TestQoS2DuplicatePublishDedups(t *testing.T) {
	t.Parallel()

	listener := serveQoS2Listener(t)
	defer closeQoS2Listener(t, listener)

	recs := make(chan uint16, 4)

	done := serveQoS2(t, listener, func(raw net.Conn) error {
		return runQoS2DedupScript(raw, recs)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dialQoS2Conn(t, ctx, listener, "weld-qos2-dedup")

	msg, err := conn.Receive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	if msg.Topic != "weld/0/rt/a" || msg.QoS != 2 || string(msg.Payload) != "\x09" {
		t.Fatalf("receive = %+v, want qos 2 sample", msg)
	}

	short, cancelShort := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelShort()

	_, err = conn.Receive(short)
	if err == nil {
		t.Fatalf("second receive succeeded, want timeout for deduped publish")
	}

	awaitQoS2Script(t, ctx, done, "qos2 duplicate publish")

	for range 2 {
		select {
		case id := <-recs:
			if id != 7 {
				t.Fatalf("pubrec id = %d, want 7", id)
			}
		case <-ctx.Done():
			t.Fatalf("broker saw no pubrec pair")
		}
	}
}

// runQoS2PubRelScript publishes once, then sends PUBREL twice and
// collects both PUBCOMPs.
func runQoS2PubRelScript(raw net.Conn, comps chan uint16) error {
	packet, err := EncodePublish(Version5, "weld/0/rt/b", 9, 2, false, Properties{}, []byte{0x0B})
	if err != nil {
		return err
	}

	if err := writePacket(raw, packet); err != nil {
		return err
	}

	ack, err := readPacket(raw)
	if err != nil {
		return err
	}

	id, err := decodeAckID(ack, packetPubRec)
	if err != nil {
		return err
	}

	rel, err := EncodePubRel(id)
	if err != nil {
		return err
	}

	for range 2 {
		if err := writePacket(raw, rel); err != nil {
			return err
		}

		comp, err := readPacket(raw)
		if err != nil {
			return err
		}

		compID, err := decodeAckID(comp, packetPubComp)
		if err != nil {
			return err
		}

		comps <- compID
	}

	return nil
}

// TestQoS2DuplicatePubRelCompResends proves a redelivered PUBREL answers PUBCOMP.
func TestQoS2DuplicatePubRelCompResends(t *testing.T) {
	t.Parallel()

	listener := serveQoS2Listener(t)
	defer closeQoS2Listener(t, listener)

	comps := make(chan uint16, 4)

	done := serveQoS2(t, listener, func(raw net.Conn) error {
		return runQoS2PubRelScript(raw, comps)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dialQoS2Conn(t, ctx, listener, "weld-qos2-pubrel")

	msg, err := conn.Receive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	if msg.Topic != "weld/0/rt/b" || msg.QoS != 2 {
		t.Fatalf("receive = %+v, want qos 2 sample", msg)
	}

	short, cancelShort := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelShort()

	_, err = conn.Receive(short)
	if err == nil {
		t.Fatalf("second receive succeeded, want timeout after pubrel pair")
	}

	awaitQoS2Script(t, ctx, done, "qos2 duplicate pubrel")

	for range 2 {
		select {
		case id := <-comps:
			if id != 9 {
				t.Fatalf("pubcomp id = %d, want 9", id)
			}
		case <-ctx.Done():
			t.Fatalf("broker saw no pubcomp pair")
		}
	}
}

// TestQoS2RefusedPubRec proves a refused PUBREC fails the publish.
func TestQoS2RefusedPubRec(t *testing.T) {
	t.Parallel()

	listener := serveQoS2Listener(t)
	defer closeQoS2Listener(t, listener)

	done := serveQoS2(t, listener, func(raw net.Conn) error {
		decoded, err := readQoS2Publish(raw)
		if err != nil {
			return err
		}

		hi := byte(decoded.PacketID >> 8)
		lo := byte(decoded.PacketID) //nolint:gosec // G115: test splits the id into halves that fit in one byte
		refused := []byte{packetPubRec << 4, 0x03, hi, lo, 0x80}

		return writePacket(raw, refused)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dialQoS2Conn(t, ctx, listener, "weld-qos2-refrec")

	err := conn.Publish(ctx, "weld/0/rt/a", 2, false, []byte{0x01}, Properties{})
	if err == nil {
		t.Fatalf("refused pubrec publish succeeded, want error")
	}

	awaitQoS2Script(t, ctx, done, "qos2 refused pubrec")

	if left := inflightQoS2Len(conn); left != 0 {
		t.Fatalf("inflight = %d, want empty after refused pubrec", left)
	}
}

// TestQoS2RefusedPubComp proves a refused PUBCOMP fails the publish.
func TestQoS2RefusedPubComp(t *testing.T) {
	t.Parallel()

	listener := serveQoS2Listener(t)
	defer closeQoS2Listener(t, listener)

	done := serveQoS2(t, listener, func(raw net.Conn) error {
		decoded, err := readQoS2Publish(raw)
		if err != nil {
			return err
		}

		rec, err := EncodePubRec(decoded.PacketID)
		if err != nil {
			return err
		}

		if err := writePacket(raw, rec); err != nil {
			return err
		}

		rel, err := readPacket(raw)
		if err != nil {
			return err
		}

		relID, err := decodeAckID(rel, packetPubRel)
		if err != nil {
			return err
		}

		hi := byte(relID >> 8)
		lo := byte(relID) //nolint:gosec // G115: test splits the id into halves that fit in one byte
		refused := []byte{packetPubComp << 4, 0x03, hi, lo, 0x87}

		return writePacket(raw, refused)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dialQoS2Conn(t, ctx, listener, "weld-qos2-refcomp")

	err := conn.Publish(ctx, "weld/0/rt/a", 2, false, []byte{0x01}, Properties{})
	if err == nil {
		t.Fatalf("refused pubcomp publish succeeded, want error")
	}

	awaitQoS2Script(t, ctx, done, "qos2 refused pubcomp")

	if left := inflightQoS2Len(conn); left != 0 {
		t.Fatalf("inflight = %d, want empty after refused pubcomp", left)
	}
}

// TestQoS2ResumeReplaysPublish proves a drop before PUBREC replays PUBLISH.
func TestQoS2ResumeReplaysPublish(t *testing.T) {
	t.Parallel()

	listener := serveQoS2Listener(t)
	defer closeQoS2Listener(t, listener)

	firstID := make(chan uint16, 1)

	firstDone := serveQoS2(t, listener, func(raw net.Conn) error {
		decoded, err := readQoS2Publish(raw)
		if err != nil {
			return err
		}

		firstID <- decoded.PacketID

		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn := dialQoS2Conn(t, ctx, listener, "weld-qos2-redial-pub")

	publishErr := make(chan error, 1)

	go func() {
		publishErr <- conn.Publish(ctx, "weld/0/rt/a", 2, false, []byte{0x05}, Properties{})
	}()

	wantID := awaitQoS2ID(t, ctx, firstID, "first publish")
	awaitQoS2Script(t, ctx, firstDone, "qos2 drop first session")

	secondDone := serveQoS2(t, listener, func(raw net.Conn) error {
		return finishQoS2Handshake(raw, wantID, true)
	})

	awaitQoS2Publish(t, ctx, publishErr, "publish after replay")
	awaitQoS2Script(t, ctx, secondDone, "qos2 replay second session")

	if got := conn.Reconnects(); got != 1 {
		t.Fatalf("reconnects = %d, want 1", got)
	}
}

// TestQoS2ResumeReplaysPubRel proves a drop after PUBREC replays PUBREL.
func TestQoS2ResumeReplaysPubRel(t *testing.T) {
	t.Parallel()

	listener := serveQoS2Listener(t)
	defer closeQoS2Listener(t, listener)

	firstID := make(chan uint16, 1)

	firstDone := serveQoS2(t, listener, func(raw net.Conn) error {
		decoded, err := readQoS2Publish(raw)
		if err != nil {
			return err
		}

		firstID <- decoded.PacketID

		rec, err := EncodePubRec(decoded.PacketID)
		if err != nil {
			return err
		}

		return writePacket(raw, rec)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn := dialQoS2Conn(t, ctx, listener, "weld-qos2-redial-rel")

	publishErr := make(chan error, 1)

	go func() {
		publishErr <- conn.Publish(ctx, "weld/0/rt/a", 2, false, []byte{0x05}, Properties{})
	}()

	wantID := awaitQoS2ID(t, ctx, firstID, "first publish")
	awaitQoS2Script(t, ctx, firstDone, "qos2 pubrel drop first session")

	secondDone := serveQoS2(t, listener, func(raw net.Conn) error {
		rel, err := readPacket(raw)
		if err != nil {
			return err
		}

		if rel[0]>>4 != packetPubRel {
			return errors.New("mqtt: test qos2 replay is not pubrel")
		}

		relID, err := decodeAckID(rel, packetPubRel)
		if err != nil {
			return err
		}

		if relID != wantID {
			return errors.New("mqtt: test qos2 pubrel id mismatch")
		}

		comp, err := EncodePubComp(relID)
		if err != nil {
			return err
		}

		return writePacket(raw, comp)
	})

	awaitQoS2Publish(t, ctx, publishErr, "publish after pubrel replay")
	awaitQoS2Script(t, ctx, secondDone, "qos2 pubrel replay second session")

	if got := conn.Reconnects(); got != 1 {
		t.Fatalf("reconnects = %d, want 1", got)
	}
}

// TestQoS2CancelThenResumeConverges proves a cancelled publish still replays.
func TestQoS2CancelThenResumeConverges(t *testing.T) {
	t.Parallel()

	listener := serveQoS2Listener(t)
	defer closeQoS2Listener(t, listener)

	firstID := make(chan uint16, 1)

	firstDone := serveQoS2(t, listener, func(raw net.Conn) error {
		decoded, err := readQoS2Publish(raw)
		if err != nil {
			return err
		}

		firstID <- decoded.PacketID

		time.Sleep(1500 * time.Millisecond)

		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn := dialQoS2Conn(t, ctx, listener, "weld-qos2-cancel")

	short, cancelShort := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancelShort()

	publishErr := make(chan error, 1)

	go func() {
		publishErr <- conn.Publish(short, "weld/0/rt/a", 2, false, []byte{0x05}, Properties{})
	}()

	wantID := awaitQoS2ID(t, ctx, firstID, "first publish")

	select {
	case err := <-publishErr:
		if err == nil {
			t.Fatalf("cancelled publish succeeded, want context error")
		}
	case <-ctx.Done():
		t.Fatalf("cancelled publish never returned")
	}

	if kept := inflightQoS2Len(conn); kept != 1 {
		t.Fatalf("inflight = %d, want 1 retained after cancel", kept)
	}

	awaitQoS2Script(t, ctx, firstDone, "qos2 cancel first session")

	secondDone := serveQoS2(t, listener, func(raw net.Conn) error {
		return finishQoS2Handshake(raw, wantID, true)
	})

	awaitQoS2Script(t, ctx, secondDone, "qos2 cancel second session")

	deadline := time.Now().Add(10 * time.Second)

	for {
		left := inflightQoS2Len(conn)
		if left == 0 {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("inflight = %d, want empty after converged replay", left)
		}

		time.Sleep(50 * time.Millisecond)
	}

	if got := conn.Reconnects(); got != 1 {
		t.Fatalf("reconnects = %d, want 1", got)
	}
}

// runMalformedPubRelScript proves a misframed PUBREL gets no reply
// while a well-formed one still completes the handshake.
func runMalformedPubRelScript(raw net.Conn) error {
	id, err := exchangeQoS2PubRec(raw)
	if err != nil {
		return err
	}

	if err := probeMalformedPubRel(raw); err != nil {
		return err
	}

	return finishQoS2PubRel(raw, id)
}

// exchangeQoS2PubRec publishes one QoS 2 sample and reads its PUBREC.
func exchangeQoS2PubRec(raw net.Conn) (uint16, error) {
	packet, err := EncodePublish(Version5, "weld/0/rt/c", 11, 2, false, Properties{}, []byte{0x0D})
	if err != nil {
		return 0, err
	}

	if err := writePacket(raw, packet); err != nil {
		return 0, err
	}

	ack, err := readPacket(raw)
	if err != nil {
		return 0, err
	}

	id, err := decodeAckID(ack, packetPubRec)
	if err != nil {
		return 0, err
	}

	if id != 11 {
		return 0, errors.New("mqtt: test malformed pubrel wrong id") //nolint:wrapcheck // test helper sentinel
	}

	return id, nil
}

// probeMalformedPubRel sends a flags-zero PUBREL and requires silence.
func probeMalformedPubRel(raw net.Conn) error {
	bad := []byte{packetPubRel << 4, 0x02, 0x00, 0x0B}

	if err := writePacket(raw, bad); err != nil {
		return fmt.Errorf("mqtt: test bad pubrel write: %w", err)
	}

	if err := raw.SetReadDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		return fmt.Errorf("mqtt: test set probe deadline: %w", err)
	}

	probe, err := readPacket(raw)
	if err == nil || probe != nil {
		return errors.New("mqtt: test malformed pubrel got reply") //nolint:wrapcheck // test helper sentinel
	}

	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		return fmt.Errorf("mqtt: test probe read: %w", err)
	}

	if err := raw.SetReadDeadline(time.Time{}); err != nil {
		return fmt.Errorf("mqtt: test clear probe deadline: %w", err)
	}

	return nil
}

// finishQoS2PubRel sends one well-formed PUBREL and reads its PUBCOMP.
func finishQoS2PubRel(raw net.Conn, id uint16) error {
	rel, err := EncodePubRel(id)
	if err != nil {
		return err
	}

	if err := writePacket(raw, rel); err != nil {
		return err
	}

	comp, err := readPacket(raw)
	if err != nil {
		return err
	}

	compID, err := decodeAckID(comp, packetPubComp)
	if err != nil {
		return err
	}

	if compID != 11 {
		return errors.New("mqtt: test valid pubrel wrong comp id") //nolint:wrapcheck // test helper sentinel
	}

	return nil
}

// TestQoS2MalformedPubRelGetsNoComp proves a misframed PUBREL is dropped.
func TestQoS2MalformedPubRelGetsNoComp(t *testing.T) {
	t.Parallel()

	listener := serveQoS2Listener(t)
	defer closeQoS2Listener(t, listener)

	done := serveQoS2(t, listener, runMalformedPubRelScript)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dialQoS2Conn(t, ctx, listener, "weld-qos2-badrel")

	msg, err := conn.Receive(ctx)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}

	if msg.Topic != "weld/0/rt/c" || msg.QoS != 2 {
		t.Fatalf("receive = %+v, want qos 2 sample", msg)
	}

	awaitQoS2Script(t, ctx, done, "qos2 malformed pubrel")
}
