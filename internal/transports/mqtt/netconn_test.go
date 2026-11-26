// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// fakeBroker is a scripted TCP broker for wire level tests.
//
// It performs the CONNECT plus CONNACK handshake, acks every
// SUBSCRIBE, records client PUBLISH packets, and lets the test push
// broker PUBLISH packets back. All assertions on raw wire bytes pass
// through EncodePublish plus DecodePublish, so the test pins the
// exact bytes a real broker such as mosquitto would see.
type fakeBroker struct {
	listener net.Listener
	conn     net.Conn
	version  Version
	received chan PublishPacket
	sendErr  chan error
}

// serveFakeBroker starts one scripted session and returns its handle.
func serveFakeBroker(t *testing.T, version Version) *fakeBroker {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	broker := &fakeBroker{
		listener: listener,
		version:  version,
		received: make(chan PublishPacket, 32),
		sendErr:  make(chan error, 1),
	}

	go broker.serve()

	return broker
}

// serve runs the handshake plus packet loop for one client.
func (b *fakeBroker) serve() {
	conn, err := b.listener.Accept()
	if err != nil {
		b.sendErr <- fmt.Errorf("accept: %w", err)

		return
	}

	b.conn = conn

	if !serveHandshake(conn, b.version, b.sendErr) {
		return
	}

	for {
		packet, ok := nextServePacket(conn)
		if !ok {
			return
		}

		if packet == nil {
			continue
		}

		if packet[0]>>4 == packetPublish {
			if !servePublish(b, conn, packet) {
				return
			}

			continue
		}

		if !serveCommon(conn, b.version, packet) {
			return
		}
	}
}

// servePublish decodes one PUBLISH and queues it; false ends the session.
func servePublish(b *fakeBroker, conn net.Conn, packet []byte) bool {
	decoded, _, err := DecodePublish(b.version, packet)
	if err != nil {
		return true
	}

	if decoded.QoS == 1 {
		if !servePubAck(conn, decoded.PacketID) {
			return false
		}
	}

	b.received <- decoded

	return true
}

// serveCommon handles handshake packets shared by every script.
//
// It returns false when the session ends. PUBLISH packets return true
// for the caller to handle with its own decoder.
func serveCommon(raw net.Conn, version Version, packet []byte) bool {
	switch packet[0] >> 4 {
	case packetSubscribe:
		return serveSubscribe(raw, version, packet)
	case packetPingReq:
		return servePing(raw)
	case packetDisconnect:
		return false
	default:
		return true
	}
}

// writeConnAck sends one session accepted CONNACK for one version.
func writeConnAck(raw net.Conn, version Version) bool {
	var packet []byte

	if version == Version311 {
		packet = []byte{packetConnAck << 4, 0x02, 0x00, connackAccepted}
	} else {
		packet = []byte{packetConnAck << 4, 0x03, 0x00, connackAccepted, 0x00}
	}

	return writePacket(raw, packet) == nil
}

// serveHandshake runs the CONNECT plus CONNACK exchange.
func serveHandshake(conn net.Conn, version Version, sendErr chan error) bool {
	hello, err := readPacket(conn)
	if err != nil {
		sendErr <- fmt.Errorf("read connect: %w", err)

		return false
	}

	if len(hello) == 0 || hello[0]>>4 != packetConnect {
		sendErr <- fmt.Errorf("first packet type %d is not connect", hello[0]>>4)

		return false
	}

	if !writeConnAck(conn, version) {
		sendErr <- errors.New("mqtt: test write connack failed")

		return false
	}

	return true
}

// nextServePacket reads one packet; false ends the session loop.
func nextServePacket(raw net.Conn) ([]byte, bool) {
	packet, err := readPacket(raw)
	if err != nil {
		return nil, false
	}

	if len(packet) == 0 {
		return nil, true
	}

	return packet, true
}

// serveSubscribe acks one SUBSCRIBE with granted QoS 0.
func serveSubscribe(raw net.Conn, version Version, packet []byte) bool {
	return serveSubAck(raw, version, packet, 0x00)
}

// serveSubAck acks one SUBSCRIBE with one granted code.
func serveSubAck(raw net.Conn, version Version, packet []byte, granted byte) bool {
	id, err := subscribeID(packet)
	if err != nil {
		return true
	}

	var ack []byte

	if version == Version311 {
		ack = []byte{packetSubAck << 4, 0x03, byte(id >> 8), byte(id), granted} //nolint:gosec // G115: packet id halves always fit in one byte
	} else {
		ack = []byte{packetSubAck << 4, 0x04, byte(id >> 8), byte(id), 0x00, granted} //nolint:gosec // G115: packet id halves always fit in one byte
	}

	return writePacket(raw, ack) == nil
}

// servePubAck sends one PUBACK; false ends the session loop.
func servePubAck(raw net.Conn, id uint16) bool {
	ack, err := EncodePubAck(id)
	if err != nil {
		return false
	}

	return writePacket(raw, ack) == nil
}

// servePing answers one PINGREQ; false ends the session loop.
func servePing(raw net.Conn) bool {
	return writePacket(raw, []byte{packetPingResp << 4, 0x00}) == nil
}

// subscribeID extracts the packet id from one SUBSCRIBE.
func subscribeID(packet []byte) (uint16, error) {
	if len(packet) < 2 {
		return 0, errors.New("mqtt: test subscribe shorter than header") //nolint:wrapcheck // test helper sentinel
	}

	length, used, err := decodeVarint(packet[1:])
	if err != nil {
		return 0, err
	}

	body := packet[1+used:]

	if len(body) != int(length) || len(body) < 2 {
		return 0, errors.New("mqtt: test subscribe body mismatch") //nolint:wrapcheck // test helper sentinel
	}

	return uint16(body[0])<<8 | uint16(body[1]), nil
}

// push sends one broker PUBLISH to the client.
func (b *fakeBroker) push(topic string, props Properties, payload []byte) error {
	packet, err := EncodePublish(b.version, topic, 0, 0, false, props, payload)
	if err != nil {
		return err
	}

	return writePacket(b.conn, packet)
}

// close releases the listener plus the session.
func (b *fakeBroker) close(t *testing.T) {
	t.Helper()

	if b.conn != nil {
		if err := b.conn.Close(); err != nil {
			t.Logf("broker conn close: %v", err)
		}
	}

	if err := b.listener.Close(); err != nil {
		t.Logf("broker listener close: %v", err)
	}
}

// dialJoinedBackend dials the scripted broker and joins discovery.
func dialJoinedBackend(t *testing.T, ctx context.Context, broker *fakeBroker, cfg MqttConfig, participant string) *Backend {
	t.Helper()

	cfg.BrokerURL = "tcp://" + broker.listener.Addr().String()

	conn, err := Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	backend, err := NewWithConn(MqttConfig{TopicPrefix: "weld", DomainID: cfg.DomainID, Version: cfg.Version}, conn)
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	t.Cleanup(func() {
		if cerr := backend.Close(); cerr != nil {
			t.Logf("backend close: %v", cerr)
		}
	})

	err = backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: participant, DomainID: cfg.DomainID})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	return backend
}

// awaitBrokerPublish receives one broker sample or fails the test.
func awaitBrokerPublish(t *testing.T, ctx context.Context, broker *fakeBroker, what string) PublishPacket {
	t.Helper()

	select {
	case seen := <-broker.received:
		return seen
	case <-ctx.Done():
		t.Fatalf("broker saw no %s", what)

		return PublishPacket{}
	}
}

// TestLiveBrokerRoundTrip proves client bytes against a scripted broker.
func TestLiveBrokerRoundTrip(t *testing.T) {
	t.Parallel()

	broker := serveFakeBroker(t, Version5)
	defer broker.close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	join := MqttConfig{BrokerURL: "tcp://" + broker.listener.Addr().String(), ClientID: "weld-wire"}
	backend := dialJoinedBackend(t, ctx, broker, join, "wire")

	err := backend.PublishBytes(ctx, transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   []byte{0x05},
		TypeHash:  "RIHS01_wire",
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	seen := awaitBrokerPublish(t, ctx, broker, "publish")

	assertBrokerData(t, seen, "weld/0/rt/chatter", "RIHS01_wire")

	err = backend.AnnounceEndpoint(ctx, "wire", KindData, "/chatter", "std_msgs__msg__String", "RIHS01_wire")
	if err != nil {
		t.Fatalf("announce: %v", err)
	}

	seen = awaitBrokerPublish(t, ctx, broker, "announce")
	assertBrokerAnnounce(t, seen, "weld/0/sdp/wire/rt/chatter")

	takeServiceReply(t, ctx, backend, broker)
	takeBrokerData(t, ctx, backend, broker)
}

// assertBrokerData checks one data publish arrived intact.
func assertBrokerData(t *testing.T, seen PublishPacket, topic, hash string) {
	t.Helper()

	if seen.Topic != topic {
		t.Fatalf("broker topic = %q, want %s", seen.Topic, topic)
	}

	if seen.Props.TypeHash != hash {
		t.Fatalf("broker hash = %q, want %q", seen.Props.TypeHash, hash)
	}
}

// assertBrokerAnnounce checks one retained announce arrived intact.
func assertBrokerAnnounce(t *testing.T, seen PublishPacket, topic string) {
	t.Helper()

	if seen.Topic != topic {
		t.Fatalf("announce topic = %q, want %s", seen.Topic, topic)
	}

	if !seen.Retained {
		t.Fatalf("announce retained is false, want true")
	}
}

// takeServiceReply pushes one broker reply and takes it as a client.
func takeServiceReply(t *testing.T, ctx context.Context, backend *Backend, broker *fakeBroker) {
	t.Helper()

	reply, err := EncodePublish(Version5, "weld/0/rr/add_two_ints", 0, 0, false, Properties{TypeHash: "RIHS01_svc"}, encodeEnvelope("my_pkg__srv__AddTwoInts", []byte{0x09}))
	if err != nil {
		t.Fatalf("encode reply: %v", err)
	}

	if err := writePacket(broker.conn, reply); err != nil {
		t.Fatalf("broker push: %v", err)
	}

	taken, err := backend.TakeService(ctx, ServiceRoleClient)
	if err != nil {
		t.Fatalf("take service: %v", err)
	}

	if taken.Service != "/add_two_ints" || taken.IsRequest {
		t.Fatalf("service = %+v, want /add_two_ints reply", taken)
	}

	if taken.TypeHash != "RIHS01_svc" {
		t.Fatalf("service hash = %q, want RIHS01_svc", taken.TypeHash)
	}
}

// takeBrokerData pushes one broker sample and takes it as data.
func takeBrokerData(t *testing.T, ctx context.Context, backend *Backend, broker *fakeBroker) {
	t.Helper()

	data, err := EncodePublish(Version5, "weld/0/rt/chatter", 0, 0, false, Properties{TypeHash: "RIHS01_wire"}, encodeEnvelope("std_msgs__msg__String", []byte{0x05}))
	if err != nil {
		t.Fatalf("encode data: %v", err)
	}

	if err := writePacket(broker.conn, data); err != nil {
		t.Fatalf("broker push data: %v", err)
	}

	msg, err := backend.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	if msg.TopicName != "/chatter" || msg.TypeHash != "RIHS01_wire" {
		t.Fatalf("take = %+v, want /chatter with hash", msg)
	}
}

// TestV311BrokerRoundTrip proves a 3.1.1 session drops the hash loudly.
func TestV311BrokerRoundTrip(t *testing.T) {
	t.Parallel()

	broker := serveFakeBroker(t, Version311)
	defer broker.close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	backend := dialJoinedBackend(t, ctx, broker, MqttConfig{ClientID: "weld-v311", Version: Version311}, "wire311")

	err := backend.PublishBytes(ctx, transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   []byte{0x05},
		TypeHash:  "RIHS01_wire",
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	seen := awaitBrokerPublish(t, ctx, broker, "publish")
	assertBrokerData(t, seen, "weld/0/rt/chatter", "")

	pushBrokerData(t, broker, "weld/0/rt/chatter")

	msg, err := backend.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	if msg.TopicName != "/chatter" || msg.TypeHash != "" {
		t.Fatalf("take = %+v, want /chatter with empty hash", msg)
	}

	if msg.Leg != transports.LegData {
		t.Fatalf("leg = %q, want data", string(msg.Leg))
	}
}

// pushBrokerData pushes one envelope to the client through the broker.
func pushBrokerData(t *testing.T, broker *fakeBroker, topic string) {
	t.Helper()

	err := broker.push(topic, Properties{}, encodeEnvelope("std_msgs__msg__String", []byte{0x05}))
	if err != nil {
		t.Fatalf("broker push: %v", err)
	}
}

// serveRefusingBroker serves one session that refuses the subscribe.
func serveRefusingBroker(t *testing.T, listener net.Listener) {
	t.Helper()

	raw, err := listener.Accept()
	if err != nil {
		return
	}

	defer func() {
		if cerr := raw.Close(); cerr != nil {
			t.Logf("refused broker close: %v", cerr)
		}
	}()

	hello, err := readPacket(raw)
	if err != nil || len(hello) == 0 || hello[0]>>4 != packetConnect {
		return
	}

	if !writeConnAck(raw, Version5) {
		return
	}

	sub, err := readPacket(raw)
	if err != nil || len(sub) == 0 || sub[0]>>4 != packetSubscribe {
		return
	}

	id, err := subscribeID(sub)
	if err != nil {
		return
	}

	// Refuse with granted code 0x80 instead of a QoS grant.
	if err := writePacket(raw, []byte{packetSubAck << 4, 0x04, byte(id >> 8), byte(id), 0x00, 0x80}); err != nil { //nolint:gosec // G115: packet id halves always fit in one byte
		return
	}

	// Hold the session briefly so the client reads the refusal
	// instead of a socket error.
	time.Sleep(2 * time.Second)
}

// TestJoinRefusedSubscribe checks a refused SUBACK fails the join.
func TestJoinRefusedSubscribe(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	defer func() {
		if cerr := listener.Close(); cerr != nil {
			t.Logf("listener close: %v", cerr)
		}
	}()

	go serveRefusingBroker(t, listener)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := MqttConfig{BrokerURL: "tcp://" + listener.Addr().String(), ClientID: "weld-refused"}

	conn, err := Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	backend, err := NewWithConn(cfg, conn)
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		if cerr := backend.Close(); cerr != nil {
			t.Logf("backend close: %v", cerr)
		}
	}()

	err = backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "refused", DomainID: 0})
	if err == nil {
		t.Fatalf("join with refused subscribe succeeded, want error")
	}
}

func TestDialRejectsScheme(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	_, err := Dial(ctx, MqttConfig{BrokerURL: "ws://broker:80", ClientID: "x"})
	if err == nil {
		t.Fatalf("ws dial succeeded, want error")
	}

	_, err = parseBrokerAddr("  ")
	if err == nil {
		t.Fatalf("empty broker succeeded, want error")
	}

	endpoint, err := parseBrokerAddr("broker.local")
	if err != nil {
		t.Fatalf("bare host: %v", err)
	}

	if endpoint.addr != "broker.local:1883" {
		t.Fatalf("addr = %q, want broker.local:1883", endpoint.addr)
	}

	if endpoint.useTLS {
		t.Fatalf("plain host selected TLS, want plain")
	}

	if _, uerr := parseBrokerAddr("mqtt://user:pass@broker.local"); uerr == nil {
		t.Fatalf("userinfo broker succeeded, want error")
	}

	endpoint, err = parseBrokerAddr("mqtts://broker.local")
	if err != nil {
		t.Fatalf("mqtts host: %v", err)
	}

	if endpoint.addr != "broker.local:8883" {
		t.Fatalf("tls addr = %q, want broker.local:8883", endpoint.addr)
	}

	if !endpoint.useTLS {
		t.Fatalf("mqtts scheme missed TLS, want TLS")
	}
}

// dropBroker is a scripted broker that drops the first session mid sample.
//
// It completes the handshake, acks subscribes, records one QoS 1
// publish without acking, then closes the socket to force a client
// reconnect. The second session is served until the test ends.
type dropBroker struct {
	listener net.Listener
	version  Version
	firstID  chan uint16
	secondID chan uint16
	redup    chan bool
	subs     chan int
}

// serveDropBroker starts the dropping broker for one version.
func serveDropBroker(t *testing.T, version Version) *dropBroker {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	broker := &dropBroker{
		listener: listener,
		version:  version,
		firstID:  make(chan uint16, 1),
		secondID: make(chan uint16, 1),
		redup:    make(chan bool, 1),
		subs:     make(chan int, 8),
	}

	go broker.serve()

	return broker
}

// closeDropSession closes one scripted socket, logging failures.
func closeDropSession(raw net.Conn, what string) {
	if err := raw.Close(); err != nil {
		log.Printf("mqtt test: close failed %s: %v", what, err)
	}
}

// serve runs the drop script for exactly two sessions.
func (b *dropBroker) serve() {
	raw, err := b.listener.Accept()
	if err != nil {
		return
	}

	if !dropHandshake(raw, b.version) {
		closeDropSession(raw, "handshake")

		return
	}

	for {
		packet, ok := nextServePacket(raw)
		if !ok {
			closeDropSession(raw, "drop session")

			return
		}

		if packet == nil {
			continue
		}

		if packet[0]>>4 == packetPublish {
			decoded, _, err := DecodePublish(b.version, packet)
			if err != nil {
				continue
			}

			b.firstID <- decoded.PacketID
			// Drop without PUBACK to force a client reconnect.
			closeDropSession(raw, "drop")

			b.serveSecond()

			return
		}

		if !serveCommon(raw, b.version, packet) {
			return
		}
	}
}

// serveSecond serves the reconnected session until the test closes it.
func (b *dropBroker) serveSecond() {
	raw, err := b.listener.Accept()
	if err != nil {
		return
	}

	defer func() {
		closeDropSession(raw, "second session")
	}()

	if !dropHandshake(raw, b.version) {
		return
	}

	subs := 0

	for {
		packet, ok := nextServePacket(raw)
		if !ok {
			return
		}

		if packet == nil {
			continue
		}

		if packet[0]>>4 == packetPublish {
			if !serveResend(raw, b, packet) {
				return
			}

			return
		}

		if !serveCommon(raw, b.version, packet) {
			return
		}

		subs++
		b.subs <- subs
	}
}

// serveResend records one republish and acks it.
func serveResend(raw net.Conn, b *dropBroker, packet []byte) bool {
	decoded, _, err := DecodePublish(b.version, packet)
	if err != nil {
		return true
	}

	b.secondID <- decoded.PacketID
	b.redup <- decoded.Dup

	ack, err := EncodePubAck(decoded.PacketID)
	if err != nil {
		return false
	}

	return writePacket(raw, ack) == nil
}

// dropHandshake runs the CONNECT plus CONNACK exchange.
func dropHandshake(raw net.Conn, version Version) bool {
	hello, err := readPacket(raw)
	if err != nil {
		return false
	}

	if len(hello) == 0 || hello[0]>>4 != packetConnect {
		return false
	}

	return writeConnAck(raw, version)
}

// TestReconnectRepublishesUnacked checks a v5 drop redials with DUP.
func TestReconnectRepublishesUnacked(t *testing.T) {
	t.Parallel()

	reconnectDropRoundTrip(t, Version5)
}

// TestReconnectV311RepublishesUnacked checks a 3.1.1 drop redials with DUP.
func TestReconnectV311RepublishesUnacked(t *testing.T) {
	t.Parallel()

	reconnectDropRoundTrip(t, Version311)
}

// awaitBrokerUint16 receives one broker id or fails the test.
func awaitBrokerUint16(t *testing.T, ctx context.Context, ch <-chan uint16, what string) uint16 {
	t.Helper()

	select {
	case id := <-ch:
		return id
	case <-ctx.Done():
		t.Fatalf("broker sent no %s", what)

		return 0
	}
}

// awaitBrokerBool receives one broker flag or fails the test.
func awaitBrokerBool(t *testing.T, ctx context.Context, ch <-chan bool, what string) bool {
	t.Helper()

	select {
	case flag := <-ch:
		return flag
	case <-ctx.Done():
		t.Fatalf("broker sent no %s", what)

		return false
	}
}

// awaitPublishAck waits for one publish result or fails the test.
func awaitPublishAck(t *testing.T, ctx context.Context, ch <-chan error) {
	t.Helper()

	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("publish never acked after reconnect")
	}
}

// reconnectDropRoundTrip runs the drop script for one version.
func reconnectDropRoundTrip(t *testing.T, version Version) {
	t.Helper()

	broker := serveDropBroker(t, version)

	defer func() {
		if err := broker.listener.Close(); err != nil {
			t.Logf("drop listener close: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	conn, err := Dial(ctx, MqttConfig{BrokerURL: "tcp://" + broker.listener.Addr().String(), ClientID: "weld-redial", QoS: 1, Version: version})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	backend, err := NewWithConn(MqttConfig{TopicPrefix: "weld", QoS: 1, DomainID: 0, Version: version}, conn)
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	}()

	err = backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "redial", DomainID: 0})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	publishErr := make(chan error, 1)

	go func() {
		publishErr <- backend.PublishBytes(ctx, transports.OutgoingMessage{
			TopicName: "/chatter",
			TypeName:  "std_msgs__msg__String",
			Payload:   []byte{0x05},
		})
	}()

	first := awaitBrokerUint16(t, ctx, broker.firstID, "first publish")
	second := awaitBrokerUint16(t, ctx, broker.secondID, "republish after drop")
	redup := awaitBrokerBool(t, ctx, broker.redup, "dup flag")
	awaitPublishAck(t, ctx, publishErr)

	if first != second {
		t.Fatalf("republish id = %d, want %d", second, first)
	}

	if !redup {
		t.Fatalf("republish dup is false, want true")
	}

	if got := conn.Reconnects(); got != 1 {
		t.Fatalf("reconnects = %d, want 1", got)
	}
}

// serveSilentConn serves one session that never answers publishes or pings.
func serveSilentConn(t *testing.T, raw net.Conn) {
	t.Helper()

	defer func() {
		if err := raw.Close(); err != nil {
			t.Logf("silent session close: %v", err)
		}
	}()

	if !serveSilentHandshake(raw) {
		return
	}

	for {
		if !serveSilentPacket(t, raw) {
			return
		}
	}
}

// serveSilentHandshake runs the CONNECT plus CONNACK exchange.
func serveSilentHandshake(raw net.Conn) bool {
	hello, err := readPacket(raw)
	if err != nil || len(hello) == 0 || hello[0]>>4 != packetConnect {
		return false
	}

	return writeConnAck(raw, Version5)
}

// serveSilentPacket acks one SUBSCRIBE and drops everything else.
func serveSilentPacket(t *testing.T, raw net.Conn) bool {
	t.Helper()

	packet, err := readPacket(raw)
	if err != nil {
		return false
	}

	if len(packet) == 0 || packet[0]>>4 != packetSubscribe {
		return true
	}

	id, err := subscribeID(packet)
	if err != nil {
		return true
	}

	ack := []byte{packetSubAck << 4, 0x04, byte(id >> 8), byte(id), 0x00, 0x00} //nolint:gosec // G115: packet id halves always fit in one byte

	return writePacket(raw, ack) == nil
}

// startSilentBroker serves silent sessions until the test ends.
func startSilentBroker(t *testing.T) (string, func()) {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	go func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}

			go serveSilentConn(t, raw)
		}
	}()

	return "tcp://" + listener.Addr().String(), func() {
		if err := listener.Close(); err != nil {
			t.Logf("listener close: %v", err)
		}
	}
}

// waitForReconnect fails unless the backend redials before the timeout.
func waitForReconnect(t *testing.T, ctx context.Context, conn *netConn, what string) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)

	for conn.Reconnects() < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("no reconnect for %s", what)
		}

		select {
		case <-ctx.Done():
			t.Fatalf("reconnect wait: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// TestWatchdogReconnectsSilentBroker checks half-open links redial.
func TestWatchdogReconnectsSilentBroker(t *testing.T) {
	t.Parallel()

	addr, stop := startSilentBroker(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg := MqttConfig{BrokerURL: addr, ClientID: "weld-watchdog", QoS: 0, KeepAliveSecs: 1}
	conn, err := Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	backend, err := NewWithConn(MqttConfig{TopicPrefix: "weld", DomainID: 0, QoS: 0, KeepAliveSecs: 1}, conn)
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		if cerr := backend.Close(); cerr != nil {
			t.Logf("backend close: %v", cerr)
		}
	}()

	if err := backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "watchdog", DomainID: 0}); err != nil {
		t.Fatalf("join: %v", err)
	}

	waitForReconnect(t, ctx, conn, "silent broker")
}

// TestInflightCapRejectsOverflow checks the unacked bounded publish queue.
func TestInflightCapRejectsOverflow(t *testing.T) {
	t.Parallel()

	conn := &netConn{
		version:  Version5,
		waiters:  make(map[uint16]chan error),
		inflight: make(map[uint16]pendingPublish),
	}

	var err error

	for i := range maxInflight {
		_, _, err = conn.registerInflight(uint16(i+1), "/chatter", 1, false, []byte{0x01}, Properties{})
		if err != nil {
			t.Fatalf("register %d: %v", i, err)
		}
	}

	if _, _, err = conn.registerInflight(maxInflight+1, "/chatter", 1, false, []byte{0x01}, Properties{}); err == nil {
		t.Fatalf("overflowed inflight accepted, want error")
	}
}

// TestReceiveQueueEvictsOldest checks a fast broker cannot grow memory.
func TestReceiveQueueEvictsOldest(t *testing.T) {
	t.Parallel()

	conn := &netConn{
		version:  Version5,
		waiters:  make(map[uint16]chan error),
		inflight: make(map[uint16]pendingPublish),
	}
	conn.wake = sync.NewCond(&conn.mu)

	for range maxNetQueue + 10 {
		conn.pushPublish(PublishPacket{Topic: "weld/0/rt/a", QoS: 0})
	}

	conn.mu.Lock()
	length := len(conn.queue)
	dropped := conn.dropped
	conn.mu.Unlock()

	if length != maxNetQueue {
		t.Fatalf("queue = %d, want %d", length, maxNetQueue)
	}

	if dropped != 10 {
		t.Fatalf("dropped = %d, want 10", dropped)
	}

	if got := conn.Dropped(); got != 10 {
		t.Fatalf("Dropped() = %d, want 10", got)
	}
}

// TestNextPacketIDSkipsLive checks IDs in use are never returned.
func TestNextPacketIDSkipsLive(t *testing.T) {
	t.Parallel()

	conn := &netConn{
		version:  Version5,
		waiters:  make(map[uint16]chan error),
		inflight: make(map[uint16]pendingPublish),
	}

	first := conn.nextPacketID()
	conn.waiters[first] = make(chan error, 1)
	conn.inflight[first+1] = pendingPublish{}

	second := conn.nextPacketID()
	if second == first || second == first+1 {
		t.Fatalf("nextPacketID = %d reuses live %d or %d", second, first, first+1)
	}

	if _, _, err := conn.registerInflight(first, "/a", 1, false, []byte{0x01}, Properties{}); err == nil {
		t.Fatalf("register collided id succeeded, want error")
	}
}

// serveUnsubAckScript answers one UNSUBSCRIBE with a coded UNSUBACK.
func serveUnsubAckScript(raw net.Conn, wantFilter string, code byte) error {
	packet, err := readPacket(raw)
	if err != nil {
		return err
	}

	id, filter, err := DecodeUnsubscribe(Version5, packet)
	if err != nil {
		return err
	}

	if filter != wantFilter {
		return errors.New("mqtt: test unsubscribe filter mismatch") //nolint:wrapcheck // test helper sentinel
	}

	ack, err := EncodeUnsubAck(Version5, id, code)
	if err != nil {
		return err
	}

	return writePacket(raw, ack)
}

// serveUnsubBroker runs one scripted session answering UNSUBSCRIBE.
func serveUnsubBroker(t *testing.T, code byte) (string, <-chan error) {
	t.Helper()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	t.Cleanup(func() {
		if cerr := listener.Close(); cerr != nil {
			t.Logf("listener close: %v", cerr)
		}
	})

	done := make(chan error, 1)

	go func() {
		raw, err := listener.Accept()
		if err != nil {
			done <- err

			return
		}
		defer func() {
			if cerr := raw.Close(); cerr != nil {
				t.Logf("script close: %v", cerr)
			}
		}()

		if !serveHandshake(raw, Version5, make(chan error, 1)) {
			done <- errQoS2Handshake

			return
		}

		done <- serveUnsubAckScript(raw, "weld/0/rt/#", code)
	}()

	return "tcp://" + listener.Addr().String(), done
}

// dialUnsubConn dials one live session against a scripted listener.
func dialUnsubConn(t *testing.T, ctx context.Context, addr, clientID string) *netConn {
	t.Helper()

	conn, err := Dial(ctx, MqttConfig{BrokerURL: addr, ClientID: clientID})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("conn close: %v", cerr)
		}
	})

	conn.mu.Lock()
	conn.filters["weld/0/rt/#"] = 0
	conn.mu.Unlock()

	return conn
}

// awaitUnsubScript fails unless one scripted session ends cleanly.
func awaitUnsubScript(t *testing.T, ctx context.Context, done <-chan error, what string) {
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

// checkFilterGone fails if the filter survived the unsubscribe.
func checkFilterGone(t *testing.T, conn *netConn, filter string) {
	t.Helper()

	conn.mu.Lock()
	_, kept := conn.filters[filter]
	conn.mu.Unlock()

	if kept {
		t.Fatalf("filter survived unsubscribe, want removed")
	}
}

// TestNetConnUnsubscribe proves UNSUBSCRIBE plus UNSUBACK over TCP.
func TestNetConnUnsubscribe(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr, done := serveUnsubBroker(t, 0x00)
	conn := dialUnsubConn(t, ctx, addr, "weld-unsub")

	if err := conn.Unsubscribe(ctx, "weld/0/rt/#"); err != nil {
		t.Fatalf("unsubscribe: %v", err)
	}

	awaitUnsubScript(t, ctx, done, "unsubscribe script")
	checkFilterGone(t, conn, "weld/0/rt/#")
}

// TestNetConnUnsubscribeRefused proves a refused UNSUBACK fails loudly.
func TestNetConnUnsubscribeRefused(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	addr, done := serveUnsubBroker(t, 0x80)
	conn := dialUnsubConn(t, ctx, addr, "weld-unsub-ref")

	if err := conn.Unsubscribe(ctx, "weld/0/rt/#"); err == nil {
		t.Fatalf("refused unsubscribe succeeded, want error")
	}

	awaitUnsubScript(t, ctx, done, "unsubscribe script")
}

// TestReadSubscribeAckBoundsBuffer proves mid-resume floods stay capped.
func TestReadSubscribeAckBoundsBuffer(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("client close: %v", err)
		}

		if err := server.Close(); err != nil {
			t.Logf("server close: %v", err)
		}
	})

	const total = maxNetQueue + 100

	go floodResumePipe(t, server, total)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got, buffered, dropped, err := readSubscribeAck(ctx, client, Version5, 9)
	if err != nil {
		t.Fatalf("read subscribe ack: %v", err)
	}

	if got != 9 {
		t.Fatalf("suback id = %d, want 9", got)
	}

	if len(buffered) != maxNetQueue {
		t.Fatalf("buffered = %d, want capped %d", len(buffered), maxNetQueue)
	}

	if dropped != 100 {
		t.Fatalf("dropped = %d, want 100 counted", dropped)
	}
}

// floodResumePipe writes count tiny publishes plus one SUBACK.
func floodResumePipe(t *testing.T, server net.Conn, count int) {
	t.Helper()

	for range count {
		packet, err := EncodePublish(Version5, "weld/0/rt/a", 0, 0, false, Properties{}, []byte{0x01})
		if err != nil {
			t.Logf("encode: %v", err)

			return
		}

		if err := writePacket(server, packet); err != nil {
			return
		}
	}

	suback := []byte{packetSubAck << 4, 0x04, 0x00, 0x09, 0x00, 0x00}

	if err := writePacket(server, suback); err != nil {
		t.Logf("suback write: %v", err)
	}
}

// serveSilentPublish reads one PUBLISH then holds the session open
// without ever acking it, so the client publish blocks.
func serveSilentPublish(t *testing.T, raw net.Conn, firstID chan<- uint16) error {
	t.Helper()

	decoded, err := readQoS2Publish(raw)
	if err != nil {
		return err
	}

	firstID <- decoded.PacketID
	time.Sleep(30 * time.Second)

	return nil
}

// serveSilentPublishConn runs one scripted session that reads a PUBLISH
// and then holds the connection open without ever acking it.
func serveSilentPublishConn(t *testing.T, listener net.Listener, firstID chan<- uint16, done chan<- error) {
	t.Helper()

	raw, err := listener.Accept()
	if err != nil {
		done <- err

		return
	}
	defer func() {
		if cerr := raw.Close(); cerr != nil {
			t.Logf("script close: %v", cerr)
		}
	}()

	if !serveHandshake(raw, Version5, make(chan error, 1)) {
		done <- errQoS2Handshake

		return
	}

	done <- serveSilentPublish(t, raw, firstID)
}

// awaitSilentPublishID fails unless the broker saw the client publish.
func awaitSilentPublishID(t *testing.T, ctx context.Context, firstID <-chan uint16) {
	t.Helper()

	select {
	case id := <-firstID:
		if id == 0 {
			t.Fatalf("broker saw zero packet id")
		}
	case <-ctx.Done():
		t.Fatalf("broker never saw the publish")
	}
}

// awaitBlockedPublishFails fails unless Close unblocks the publish.
func awaitBlockedPublishFails(t *testing.T, ctx context.Context, publishErr <-chan error) {
	t.Helper()

	select {
	case err := <-publishErr:
		if err == nil {
			t.Fatalf("blocked publish succeeded across close, want error")
		}

		if !errors.Is(err, transports.ErrBackendClosed) {
			t.Fatalf("blocked publish err = %v, want %v", err, transports.ErrBackendClosed)
		}
	case <-ctx.Done():
		t.Fatalf("blocked publish never returned after close")
	}
}

// TestCloseFailsBlockedPublish proves Close unblocks waiting publishes.
func TestCloseFailsBlockedPublish(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	defer func() {
		if cerr := listener.Close(); cerr != nil {
			t.Logf("listener close: %v", cerr)
		}
	}()

	firstID := make(chan uint16, 1)
	done := make(chan error, 1)

	go serveSilentPublishConn(t, listener, firstID, done)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := Dial(ctx, MqttConfig{BrokerURL: "tcp://" + listener.Addr().String(), ClientID: "weld-close-wait"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	publishErr := make(chan error, 1)

	go func() {
		publishErr <- conn.Publish(ctx, "weld/0/rt/a", 1, false, []byte{0x01}, Properties{})
	}()

	awaitSilentPublishID(t, ctx, firstID)

	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	awaitBlockedPublishFails(t, ctx, publishErr)

	select {
	case err := <-done:
		if err != nil {
			t.Logf("script ended: %v", err)
		}
	case <-time.After(time.Second):
	}
}

// TestSubscribeRefusedDropsFilter proves a refused SUBSCRIBE leaves no
// resubscribe entry behind to retry forever.
func TestSubscribeRefusedDropsFilter(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	defer func() {
		if cerr := listener.Close(); cerr != nil {
			t.Logf("listener close: %v", cerr)
		}
	}()

	go serveRefusingBroker(t, listener)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := Dial(ctx, MqttConfig{BrokerURL: "tcp://" + listener.Addr().String(), ClientID: "weld-refused-sub"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	defer func() {
		if cerr := conn.Close(); cerr != nil {
			t.Logf("conn close: %v", cerr)
		}
	}()

	if err := conn.Subscribe(ctx, "weld/0/rt/#", 0); err == nil {
		t.Fatalf("refused subscribe succeeded, want error")
	}

	conn.mu.Lock()
	_, kept := conn.filters["weld/0/rt/#"]
	conn.mu.Unlock()

	if kept {
		t.Fatalf("refused filter kept, want dropped")
	}
}

// TestAckPublishIgnoresForeignID proves a PUBACK only completes a
// tracked QoS 1 waiter, never a subscribe or unknown waiter.
func TestAckPublishIgnoresForeignID(t *testing.T) {
	t.Parallel()

	conn := &netConn{
		version:  Version5,
		waiters:  make(map[uint16]chan error),
		inflight: make(map[uint16]pendingPublish),
	}

	foreign := make(chan error, 1)
	conn.waiters[9] = foreign
	cleanup := func() {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		delete(conn.waiters, 9)
	}
	defer cleanup()

	conn.ackPublish(9, nil)

	select {
	case err := <-foreign:
		t.Fatalf("foreign waiter completed with %v, want silence", err)
	default:
	}

	ack, release, err := conn.registerInflight(7, "/a", 1, false, []byte{0x01}, Properties{})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	defer release()

	conn.ackPublish(7, nil)

	select {
	case err := <-ack:
		if err != nil {
			t.Fatalf("own waiter failed: %v", err)
		}
	default:
		t.Fatalf("own waiter not completed")
	}
}

// TestLegBufferCapsOverflow proves cross-leg buffers evict oldest
// with counting instead of growing without bound.
func TestLegBufferCapsOverflow(t *testing.T) {
	t.Parallel()

	backend := &Backend{}

	for range maxLegPending + 10 {
		backend.stageLegSample(transports.IncomingMessage{TopicName: "/a"})
	}

	if len(backend.dataPending) != maxLegPending {
		t.Fatalf("data pending = %d, want capped %d", len(backend.dataPending), maxLegPending)
	}

	if backend.dropped != 10 {
		t.Fatalf("dropped = %d, want 10 counted", backend.dropped)
	}

	for range maxLegPending + 4 {
		backend.stageSvcSample(serviceSample{rosTopic: "/s"})
	}

	if len(backend.svcPending) != maxLegPending {
		t.Fatalf("svc pending = %d, want capped %d", len(backend.svcPending), maxLegPending)
	}

	if backend.dropped != 14 {
		t.Fatalf("dropped = %d, want 14 counted", backend.dropped)
	}
}

// TestAbsorbResumeLeftoversDispatches proves buffered publishes survive
// a failed resume instead of being dropped with the fresh socket.
func TestAbsorbResumeLeftoversDispatches(t *testing.T) {
	t.Parallel()

	conn := &netConn{version: Version5}
	conn.wake = sync.NewCond(&conn.mu)

	packet, err := EncodePublish(Version5, "weld/0/rt/a", 0, 0, false, Properties{}, []byte{0x01})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	ctx := context.Background()
	conn.absorbResumeLeftovers(ctx, [][]byte{packet, {0x01}}, 2)

	if got := conn.Dropped(); got != 2 {
		t.Fatalf("dropped = %d, want counted overrun 2", got)
	}

	msg, err := conn.Receive(ctx)
	if err != nil {
		t.Fatalf("receive buffered: %v", err)
	}

	if msg.Topic != "weld/0/rt/a" {
		t.Fatalf("topic = %q, want buffered publish", msg.Topic)
	}
}

// TestAbandonInflightKeepsNewerEntry proves a stale cancel cannot drop
// a newer sample that reused the packet id.
func TestAbandonInflightKeepsNewerEntry(t *testing.T) {
	t.Parallel()

	conn := &netConn{
		version:  Version5,
		waiters:  make(map[uint16]chan error),
		inflight: make(map[uint16]pendingPublish),
	}

	ackOld, cleanupOld, err := conn.registerInflight(7, "/a", 1, false, []byte{0x01}, Properties{})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	defer cleanupOld()

	conn.ackPublish(7, nil)

	select {
	case err := <-ackOld:
		if err != nil {
			t.Fatalf("old waiter failed: %v", err)
		}
	default:
		t.Fatalf("old waiter not completed")
	}

	ackNew, cleanupNew, err := conn.registerInflight(7, "/b", 1, false, []byte{0x02}, Properties{})
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}

	defer cleanupNew()
	_ = ackNew

	conn.abandonInflight(7, ackOld)

	conn.mu.Lock()
	sample, ok := conn.inflight[7]
	conn.mu.Unlock()

	if !ok {
		t.Fatalf("stale abandon deleted the newer inflight entry")
	}

	if sample.topic != "/b" {
		t.Fatalf("inflight topic = %q, want /b", sample.topic)
	}
}

// TestAckPubCompIgnoresWrongPhase proves a PUBCOMP only completes a
// QoS 2 sample that is actually awaiting it.
func TestAckPubCompIgnoresWrongPhase(t *testing.T) {
	t.Parallel()

	conn := &netConn{
		version:  Version5,
		waiters:  make(map[uint16]chan error),
		inflight: make(map[uint16]pendingPublish),
	}

	ack, cleanup, err := conn.registerInflight(9, "/a", 1, false, []byte{0x01}, Properties{})
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	defer cleanup()

	conn.ackPubComp(9, nil)

	select {
	case err := <-ack:
		t.Fatalf("qos1 waiter completed by pubcomp: %v", err)
	default:
	}

	conn.ackPubComp(10, nil)
}

// TestQoS2WriteFailureKeepsInflight proves a failed QoS 2 write keeps
// the slot for resume replay instead of stranding the broker half-state.
func TestQoS2WriteFailureKeepsInflight(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()

	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Logf("client close: %v", err)
		}

		if err := server.Close(); err != nil {
			t.Logf("server close: %v", err)
		}
	})

	// Close the server end so writes fail.
	if err := server.Close(); err != nil {
		t.Fatalf("close server: %v", err)
	}

	conn := &netConn{
		version:  Version5,
		conn:     client,
		waiters:  make(map[uint16]chan error),
		inflight: make(map[uint16]pendingPublish),
		qos2recv: make(map[uint16]struct{}),
		filters:  make(map[string]byte),
	}
	conn.wake = sync.NewCond(&conn.mu)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := conn.publishQoS2(ctx, "weld/0/rt/a", 2, false, []byte{0x01}, Properties{})
	if err == nil {
		t.Fatalf("publish over broken pipe succeeded, want error")
	}

	conn.mu.Lock()
	_, kept := conn.inflight[1]
	conn.mu.Unlock()

	if !kept {
		t.Fatalf("qos2 write failure abandoned the slot, want kept for resume")
	}
}
