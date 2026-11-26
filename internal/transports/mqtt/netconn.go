// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/podomy/weld/internal/transports"
)

// Real MQTT v5 broker connection over TCP with TLS and reconnect.
//
// netConn implements Conn against a live broker such as mosquitto. It
// owns one net.Conn plus a read loop: incoming PUBLISH packets land in
// a queue for Receive, PUBACK and SUBACK complete their waiters, and
// PINGRESP answers keepalive. A dropped session redials with backoff,
// resubscribes its filters, republishes unacked QoS 1 samples with
// DUP set, and replays QoS 2 handshakes per phase, so a broker restart
// surfaces as a reconnect counter bump instead of a silent state loss.
// TLS uses the host trust store plus an optional CA bundle and client
// pair from config.

// defaultBrokerPort is the plain TCP port when the URL names none.
const defaultBrokerPort = "1883"

// defaultTLSPort is the TLS port when the URL names none.
const defaultTLSPort = "8883"

// reconnectInitialDelay is the first redial wait after a drop.
const reconnectInitialDelay = 100 * time.Millisecond

// reconnectMaxDelay caps the redial backoff.
const reconnectMaxDelay = 5 * time.Second

// brokerEndpoint splits one broker URL into address plus TLS selection.
type brokerEndpoint struct {
	// addr is the host plus port dial target.
	addr string
	// useTLS selects a TLS session.
	useTLS bool
	// serverName is the TLS server name from config or host.
	serverName string
}

// pendingPublish is one unacked sample awaiting its ack: PUBACK for
// QoS 1, PUBREC plus PUBCOMP for QoS 2.
type pendingPublish struct {
	// topic is the publish topic.
	topic string
	// qos is the publish quality of service.
	qos byte
	// retained echoes the publish retained flag.
	retained bool
	// payload holds the envelope bytes.
	payload []byte
	// props carries the user properties.
	props Properties
	// phase tracks the QoS 2 handshake: PUBLISH sent or PUBREL sent.
	// QoS 1 entries stay in phasePublishSent until their PUBACK.
	phase byte
}

// QoS 2 sender phases for pendingPublish.
const (
	// phasePublishSent means PUBLISH is written and PUBREC is awaited.
	phasePublishSent byte = iota
	// phasePubRelSent means PUBREC arrived, PUBREL is written, and
	// PUBCOMP is awaited. Payload and props are freed in this phase.
	phasePubRelSent
)

// netConn is a live MQTT v5 session over one TCP connection.
type netConn struct {
	// conn carries the MQTT byte stream. Reconnect swaps it.
	conn net.Conn
	// mu guards the queue, waiters, filters, inflight, and closed flag.
	mu sync.Mutex
	// writeMu serializes packet writes and connection swaps.
	writeMu sync.Mutex
	// reconMu serializes reconnect attempts.
	reconMu sync.Mutex
	// queue holds received PUBLISH samples in arrival order.
	queue []ConnMessage
	// dropped counts receive queue evictions when takes fall behind.
	// Best-effort ack write failures after the first per connection
	// count here too (see noteAckDrop) instead of logging every one.
	dropped uint64
	// ackDropLogged marks the first best-effort ack failure already
	// logged for this connection; later ones only bump dropped.
	ackDropLogged bool
	// waiters maps packet ids to ack channels for QoS 1, QoS 2, and subscribe.
	waiters map[uint16]chan error
	// inflight maps packet ids to unacked QoS 1 and QoS 2 samples for republish.
	inflight map[uint16]pendingPublish
	// qos2recv tracks inbound QoS 2 packet ids awaiting PUBREL so a
	// redelivered PUBLISH resends PUBREC without a second enqueue.
	qos2recv map[uint16]struct{}
	// lastRecv holds the last inbound packet time as unix nanos for
	// the half-open watchdog.
	lastRecv atomic.Int64
	// filters maps subscribed topic filters to their QoS for resubscribe.
	filters map[string]byte
	// version selects the packet framing. Zero means Version5.
	version Version
	// nextID sources packet identifiers, never zero on the wire.
	nextID atomic.Uint32
	// closed marks teardown.
	closed bool
	// readErr records the latest read loop failure for receivers.
	readErr error
	// reconnects counts successful redials after drops.
	reconnects uint64
	// wake signals receivers on queue, error, or close.
	wake *sync.Cond
	// stop ends the lifetime context on Close.
	stop context.CancelFunc
	// pingStop ends the keepalive loop.
	pingStop chan struct{}
	// pingOnce guards keepalive shutdown.
	pingOnce sync.Once
	// endpoint is the redial target.
	endpoint brokerEndpoint
	// tlsConfig secures TLS sessions. Nil means plain TCP.
	tlsConfig *tls.Config
	// connect carries the CONNECT parameters for redial.
	connect ConnectOptions
	// keepAlive paces keepalive in seconds.
	keepAlive uint16
}

// keepAliveOf returns the effective keepalive, defaulting when zero.
//
// A zero keepalive, which would disable the mechanism per spec, maps
// to the default instead: weld always runs the keepalive watchdog.
func keepAliveOf(cfg MqttConfig) uint16 {
	if cfg.KeepAliveSecs == 0 {
		return DefaultKeepAliveSecs
	}

	return cfg.KeepAliveSecs
}

// parseBrokerAddr validates one broker URL into address plus TLS use.
func parseBrokerAddr(raw string) (brokerEndpoint, error) {
	var out brokerEndpoint

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return out, errors.New("mqtt: broker url is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if !strings.Contains(trimmed, "://") {
		trimmed = "tcp://" + trimmed
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return out, fmt.Errorf("mqtt: parse broker url: %w", err)
	}

	if parsed.User != nil {
		return out, errors.New("mqtt: broker url carries userinfo; set username plus password in config") //nolint:wrapcheck // new error, nothing to wrap
	}

	port := defaultBrokerPort

	switch parsed.Scheme {
	case "tcp", "mqtt":
	case "tls", "mqtts", "ssl":
		out.useTLS = true
		port = defaultTLSPort
	default:
		return out, fmt.Errorf("mqtt: broker scheme %q unsupported, want tcp, mqtt, tls, mqtts, or ssl", parsed.Scheme)
	}

	// Path, query, and fragment are ignored: the broker address is
	// host plus port only. A URL carrying them is almost certainly a
	// copy-paste error, but rejecting it would break no-scheme
	// shorthand handling above, so the extra parts are dropped.
	host := parsed.Host
	if host == "" {
		return out, fmt.Errorf("mqtt: broker url %q misses host", raw)
	}

	if _, _, err := net.SplitHostPort(host); err != nil {
		host = net.JoinHostPort(host, port)
	}

	out.addr = host

	if hostname, _, err := net.SplitHostPort(host); err == nil {
		out.serverName = hostname
	} else {
		out.serverName = host
	}

	return out, nil
}

// tlsConfigOf builds one TLS config from the backend config.
//
// A lone cert or key file is rejected. File contents never appear in
// errors. A nil result means plain TCP.
func tlsConfigOf(endpoint brokerEndpoint, cfg MqttTLS) (*tls.Config, error) {
	if !endpoint.useTLS {
		return nil, nil
	}

	out := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if cfg.ServerName != "" {
		out.ServerName = cfg.ServerName
	} else {
		out.ServerName = endpoint.serverName
	}

	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("mqtt: read tls ca file: %w", err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("mqtt: tls ca file carries no certificates") //nolint:wrapcheck // new error, nothing to wrap
		}

		out.RootCAs = pool
	}

	if cfg.CertFile != "" || cfg.KeyFile != "" {
		pair, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("mqtt: load tls client pair: %w", err)
		}

		out.Certificates = []tls.Certificate{pair}
	}

	return out, nil
}

// Dial opens one MQTT v5 session against a broker.
//
// The context bounds dial plus the CONNECT plus CONNACK handshake;
// without a caller deadline Dial applies a 30 s bound so a silent
// broker fails loudly instead of hanging forever. Empty ClientID
// means the broker assigns one. A nonzero CONNACK reason fails with
// the reason code. TLS, username, password, and the last will come
// from cfg. The caller owns the handle and must Close it. Drops
// after Dial redial automatically; see reconnect.
//
// The return type is unexported on purpose: use type inference
// (conn, err := Dial(...)) and keep the handle opaque. The handle
// implements transports-independent Publish, Subscribe, Unsubscribe,
// Receive, and Close for live sessions; most callers should prefer
// Backend New or NewWithConn over driving Dial directly.
func Dial(ctx context.Context, cfg MqttConfig) (*netConn, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("mqtt: dial context: %w", err)
	}

	err = cfg.Validate()
	if err != nil {
		return nil, err
	}

	endpoint, err := parseBrokerAddr(cfg.BrokerURL)
	if err != nil {
		return nil, err
	}

	tlsConfig, err := tlsConfigOf(endpoint, cfg.TLS)
	if err != nil {
		return nil, err
	}

	keepAlive := keepAliveOf(cfg)

	conn := &netConn{
		waiters:   make(map[uint16]chan error),
		inflight:  make(map[uint16]pendingPublish),
		qos2recv:  make(map[uint16]struct{}),
		filters:   make(map[string]byte),
		pingStop:  make(chan struct{}),
		endpoint:  endpoint,
		tlsConfig: tlsConfig,
		keepAlive: keepAlive,
		version:   cfg.Version,
		connect: ConnectOptions{
			Version:      cfg.Version,
			ClientID:     cfg.ClientID,
			KeepAlive:    keepAlive,
			Username:     cfg.Username,
			Password:     []byte(cfg.Password),
			WillTopic:    cfg.Will.Topic,
			WillPayload:  append([]byte(nil), cfg.Will.Payload...),
			WillQoS:      cfg.Will.QoS,
			WillRetained: cfg.Will.Retained,
		},
	}
	conn.wake = sync.NewCond(&conn.mu)

	// The lifetime detaches cancel from the dial context but keeps its
	// values, so the session outlives the handshake bound. Close ends it.
	life, stop := context.WithCancel(context.WithoutCancel(ctx))
	conn.stop = stop

	raw, err := conn.dialOnce(ctx)
	if err != nil {
		stop()

		return nil, err
	}

	conn.conn = raw
	conn.noteRecv()

	err = conn.handshake(ctx, raw)
	if err != nil {
		stop()

		if cerr := raw.Close(); cerr != nil {
			return nil, fmt.Errorf("mqtt: handshake: %w", errors.Join(err, cerr))
		}

		return nil, err
	}

	go conn.readLoop(life)
	go conn.pingLoop(time.Duration(keepAlive) * time.Second / 2)

	return conn, nil
}

// dialTimeout bounds one redial attempt when the caller sets no deadline.
//
// The initial Dial carries the caller deadline (or a 30 s default), but
// the reconnect loop runs on the detached lifetime context which has no
// deadline. Without this bound a silent SYN would hang the reconnect
// loop forever instead of backing off to the next round.
const dialTimeout = 10 * time.Second

// dialOnce opens one TCP or TLS socket bounded by ctx.
func (c *netConn) dialOnce(ctx context.Context) (net.Conn, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dialTimeout)
		defer cancel()

		return c.dialOnce(ctx)
	}

	dialer := net.Dialer{}

	if c.tlsConfig == nil {
		raw, err := dialer.DialContext(ctx, "tcp", c.endpoint.addr)
		if err != nil {
			return nil, fmt.Errorf("mqtt: dial broker %q: %w", c.endpoint.addr, err)
		}

		return raw, nil
	}

	tlsDialer := tls.Dialer{NetDialer: &dialer, Config: c.tlsConfig}

	raw, err := tlsDialer.DialContext(ctx, "tcp", c.endpoint.addr)
	if err != nil {
		return nil, fmt.Errorf("mqtt: dial tls broker %q: %w", c.endpoint.addr, err)
	}

	return raw, nil
}

// handshake sends CONNECT and validates CONNACK.
//
// The caller context bounds the exchange through a socket deadline:
// without it a silent broker would hang Dial forever. When the caller
// sets no deadline a 10 s bound applies so redials fail loudly into
// the backoff loop.
func (c *netConn) handshake(ctx context.Context, raw net.Conn) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(dialTimeout)
	}

	if err := raw.SetDeadline(deadline); err != nil {
		return fmt.Errorf("mqtt: handshake deadline: %w", err)
	}

	defer func() {
		if err := raw.SetDeadline(time.Time{}); err != nil {
			log.Printf("mqtt: reset handshake deadline: %v", err)
		}
	}()

	hello, err := EncodeConnect(c.connect)
	if err != nil {
		return err
	}

	err = writePacket(raw, hello)
	if err != nil {
		return fmt.Errorf("mqtt: send connect: %w", err)
	}

	rawPacket, err := readPacket(raw)
	if err != nil {
		return fmt.Errorf("mqtt: read connack: %w", err)
	}

	reason, err := DecodeConnAck(c.version, rawPacket)
	if err != nil {
		return err
	}

	if reason != connackAccepted {
		return fmt.Errorf("mqtt: broker refused session with reason %d", reason)
	}

	return nil
}

// nextPacketID returns the next nonzero packet identifier not currently
// in use by a waiter or an unacked publish. IDs wrap the 16 bit space;
// skipping live IDs prevents a new SUBSCRIBE or PUBLISH from completing
// a stale waiter with the wrong ack type.
func (c *netConn) nextPacketID() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()

	for range 65535 {
		id := uint16(c.nextID.Add(1) % 65536)
		if id == 0 {
			continue
		}

		if _, ok := c.waiters[id]; ok {
			continue
		}

		if _, ok := c.inflight[id]; ok {
			continue
		}

		return id
	}

	id := uint16(c.nextID.Add(1) % 65536)
	if id == 0 {
		id = 1
	}

	return id
}

// failWaitersLocked completes every waiter with a close error so no
// blocked publish or subscribe waits forever after teardown. Channels
// are buffered size one, so delivery here never blocks while holding mu.
// The caller holds mu.
func (c *netConn) failWaitersLocked(err error) {
	for id, waiter := range c.waiters {
		delete(c.waiters, id)

		select {
		case waiter <- err:
		default:
		}
	}
}

// snapshotClosed reports teardown without blocking writers.
func (c *netConn) snapshotClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.closed
}

// broadcast wakes blocked receivers while holding the cond mutex so a
// check-then-wait in Receive cannot miss the signal.
func (c *netConn) broadcast() {
	c.mu.Lock()
	c.wake.Broadcast()
	c.mu.Unlock()
}

// noteRecv stamps one inbound packet for the half-open watchdog.
func (c *netConn) noteRecv() {
	c.lastRecv.Store(time.Now().UnixNano())
}

// writeTimeout bounds one socket write, scaling with packet size.
//
// The floor covers handshakes and keepalives on slow links; bulk
// samples earn about 64 KiB per second so a 32 MiB publish can still
// drain instead of failing loudly on a slow radio.
func writeTimeoutFor(n int) time.Duration {
	const perByte = int64(time.Second) / 65536

	timeout := 15*time.Second + time.Duration(int64(n)*perByte)
	if timeout < 15*time.Second {
		return 15 * time.Second
	}

	return timeout
}

// Publish sends one sample, waiting for PUBACK on QoS 1 and for
// PUBREC plus PUBCOMP on QoS 2.
func (c *netConn) Publish(ctx context.Context, topic string, qos byte, retained bool, payload []byte, props Properties) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: publish context: %w", err)
	}

	if c.snapshotClosed() {
		return fmt.Errorf("mqtt: publish after close: %w", transports.ErrBackendClosed)
	}

	if qos > maxQoS {
		return fmt.Errorf("mqtt: publish qos %d above supported %d", qos, maxQoS)
	}

	if qos == 0 {
		packet, err := EncodePublish(c.version, topic, 0, qos, retained, props, payload)
		if err != nil {
			return err
		}

		return c.writePacketLocked(packet, "publish")
	}

	if qos == 2 {
		return c.publishQoS2(ctx, topic, qos, retained, payload, props)
	}

	return c.publishQoS1(ctx, topic, qos, retained, payload, props)
}

// publishQoS2 sends one sample through the exactly-once handshake and
// waits for its PUBCOMP.
//
// A caller context cancel drops only the waiter: the inflight entry
// plus its phase stay for resume replay so the sample still converges.
// A failed write keeps the entry for the same reason: the write may
// have reached the broker before the error surfaced, so dropping it
// would strand the broker half-state while the app retries with a new
// id (at-least-once instead of exactly-once). This diverges from
// publishQoS1, where abandonInflight drops the entry because the
// caller owns the retry. The deferred registerInflight cleanup removes
// the waiter identity checked.
func (c *netConn) publishQoS2(ctx context.Context, topic string, qos byte, retained bool, payload []byte, props Properties) error {
	packetID := c.nextPacketID()

	packet, err := EncodePublish(c.version, topic, packetID, qos, retained, props, payload)
	if err != nil {
		return err
	}

	ack, cleanup, err := c.registerInflight(packetID, topic, qos, retained, payload, props)
	if err != nil {
		return err
	}

	defer cleanup()

	// No abandon on write failure: the entry stays for resume replay
	// (see above). The waiter is already removed by the deferred
	// cleanup; the phase stays phasePublishSent so the next resume
	// replays PUBLISH with DUP set.
	if err := c.writePacketLocked(packet, "publish"); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		err := ctx.Err()

		return fmt.Errorf("mqtt: publish ack context: %w", err)
	case err := <-ack:
		if err != nil {
			return fmt.Errorf("mqtt: publish refused: %w", err)
		}

		return nil
	}
}

// publishQoS1 sends one sample and waits for its PUBACK.
func (c *netConn) publishQoS1(ctx context.Context, topic string, qos byte, retained bool, payload []byte, props Properties) error {
	packetID := c.nextPacketID()

	packet, err := EncodePublish(c.version, topic, packetID, qos, retained, props, payload)
	if err != nil {
		return err
	}

	ack, cleanup, err := c.registerInflight(packetID, topic, qos, retained, payload, props)
	if err != nil {
		return err
	}

	defer cleanup()

	if err := c.writePacketLocked(packet, "publish"); err != nil {
		c.abandonInflight(packetID, ack)

		return err
	}

	select {
	case <-ctx.Done():
		err := ctx.Err()
		c.abandonInflight(packetID, ack)

		return fmt.Errorf("mqtt: publish ack context: %w", err)
	case err := <-ack:
		if err != nil {
			return fmt.Errorf("mqtt: publish refused: %w", err)
		}

		return nil
	}
}

// maxInflight bounds unacked QoS 1 plus QoS 2 samples per connection.
//
// The cap keeps the resend set bounded. Packet id reuse while unacked
// is prevented by nextPacketID skipping live waiter and inflight ids
// plus registerInflight rejecting collisions.
const maxInflight = 1024

// registerInflight tracks one unacked QoS 1 or QoS 2 sample and its waiter.
func (c *netConn) registerInflight(id uint16, topic string, qos byte, retained bool, payload []byte, props Properties) (chan error, func(), error) {
	ack := make(chan error, 1)

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, nil, fmt.Errorf("mqtt: publish after close: %w", transports.ErrBackendClosed)
	}

	if len(c.inflight) >= maxInflight {
		return nil, nil, errors.New("mqtt: too many unacked publishes") //nolint:wrapcheck // new error, nothing to wrap
	}

	if _, ok := c.waiters[id]; ok {
		return nil, nil, fmt.Errorf("mqtt: packet id %d already in use", id)
	}

	if _, ok := c.inflight[id]; ok {
		return nil, nil, fmt.Errorf("mqtt: packet id %d already in use", id)
	}

	copied := props
	if props.Extra != nil {
		copied.Extra = make(map[string]string, len(props.Extra))
		maps.Copy(copied.Extra, props.Extra)
	}

	c.waiters[id] = ack
	c.inflight[id] = pendingPublish{topic: topic, qos: qos, retained: retained, payload: append([]byte(nil), payload...), props: copied, phase: phasePublishSent}

	return ack, func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		if current, ok := c.waiters[id]; ok && current == ack {
			delete(c.waiters, id)
		}
	}, nil
}

// abandonInflight drops one unacked QoS 1 sample that will never be
// awaited again, such as after a caller context cancel or a failed
// write where the caller owns the retry. QoS 2 never abandons: its
// entry stays for resume replay on both cancel and write failure.
// The drop is identity checked: if the id was already reused by a
// newer publish, the newer sample is left alone. The waiter cleanup
// runs through the registered closer.
func (c *netConn) abandonInflight(id uint16, ack chan error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if current, ok := c.waiters[id]; !ok || current != ack {
		return
	}

	delete(c.inflight, id)
}

// writePacketLocked writes one packet with an operation label.
func (c *netConn) writePacketLocked(packet []byte, op string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if err := c.writeBounded(packet); err != nil {
		if c.snapshotClosed() {
			return fmt.Errorf("mqtt: %s after close: %w", op, transports.ErrBackendClosed)
		}

		// One retry on the same socket: the write holds writeMu, so no
		// reconnect swap can interleave, and the retry only papers over
		// transient write errors. QoS 2 keeps the sample inflight
		// either way so a later reconnect republishes it with DUP set;
		// QoS 1 abandons on failure and leaves the retry to the caller
		// (see publishQoS2 vs publishQoS1).
		if err := c.writeBounded(packet); err != nil {
			return fmt.Errorf("mqtt: %s write: %w", op, err)
		}
	}

	return nil
}

// writeBounded writes one packet with a size scaled deadline.
func (c *netConn) writeBounded(packet []byte) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(writeTimeoutFor(len(packet)))); err != nil {
		return fmt.Errorf("mqtt: write deadline: %w", err)
	}

	defer func() {
		if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
			log.Printf("mqtt: clear write deadline: %v", err)
		}
	}()

	if err := writePacket(c.conn, packet); err != nil {
		return fmt.Errorf("mqtt: write packet: %w", err)
	}

	return nil
}

// Subscribe registers one topic filter with the broker.
//
// The filter joins the resubscribe set only after the SUBACK accepts
// it, so a refused or failed subscribe is never retried on reconnect.
func (c *netConn) Subscribe(ctx context.Context, filter string, qos byte) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: subscribe context: %w", err)
	}

	if c.snapshotClosed() {
		return fmt.Errorf("mqtt: subscribe after close: %w", transports.ErrBackendClosed)
	}

	if qos > maxQoS {
		return fmt.Errorf("mqtt: subscribe qos %d above supported %d", qos, maxQoS)
	}

	if err := c.subscribeOnce(ctx, filter, qos); err != nil {
		return err
	}

	c.mu.Lock()
	c.filters[filter] = qos
	c.mu.Unlock()

	return nil
}

// subscribeOnce sends one SUBSCRIBE and waits for its SUBACK.
func (c *netConn) subscribeOnce(ctx context.Context, filter string, qos byte) error {
	id := c.nextPacketID()

	packet, err := EncodeSubscribe(c.version, id, filter, qos)
	if err != nil {
		return err
	}

	ack := make(chan error, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()

		return fmt.Errorf("mqtt: subscribe after close: %w", transports.ErrBackendClosed)
	}

	if _, ok := c.waiters[id]; ok {
		c.mu.Unlock()

		return fmt.Errorf("mqtt: packet id %d already in use", id)
	}
	c.waiters[id] = ack
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		if current, ok := c.waiters[id]; ok && current == ack {
			delete(c.waiters, id)
		}
	}()

	if err := c.writePacketLocked(packet, "subscribe"); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		err := ctx.Err()

		return fmt.Errorf("mqtt: subscribe ack context: %w", err)
	case err := <-ack:
		if err != nil {
			return fmt.Errorf("mqtt: subscribe refused: %w", err)
		}

		return nil
	}
}

// Unsubscribe removes one topic filter from the broker.
//
// The filter is dropped from the resubscribe set first, so a
// reconnect racing the UNSUBACK cannot resurrect it. The UNSUBACK
// result is validated like SUBACK: a refused code fails loudly.
func (c *netConn) Unsubscribe(ctx context.Context, filter string) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("mqtt: unsubscribe context: %w", err)
	}

	if c.snapshotClosed() {
		return fmt.Errorf("mqtt: unsubscribe after close: %w", transports.ErrBackendClosed)
	}

	c.mu.Lock()
	delete(c.filters, filter)
	c.mu.Unlock()

	return c.unsubscribeOnce(ctx, filter)
}

// unsubscribeOnce sends one UNSUBSCRIBE and waits for its UNSUBACK.
func (c *netConn) unsubscribeOnce(ctx context.Context, filter string) error {
	id := c.nextPacketID()

	packet, err := EncodeUnsubscribe(c.version, id, filter)
	if err != nil {
		return err
	}

	ack := make(chan error, 1)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()

		return fmt.Errorf("mqtt: unsubscribe after close: %w", transports.ErrBackendClosed)
	}

	if _, ok := c.waiters[id]; ok {
		c.mu.Unlock()

		return fmt.Errorf("mqtt: packet id %d already in use", id)
	}
	c.waiters[id] = ack
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()

		if current, ok := c.waiters[id]; ok && current == ack {
			delete(c.waiters, id)
		}
	}()

	if err := c.writePacketLocked(packet, "unsubscribe"); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		err := ctx.Err()

		return fmt.Errorf("mqtt: unsubscribe ack context: %w", err)
	case err := <-ack:
		if err != nil {
			return fmt.Errorf("mqtt: unsubscribe refused: %w", err)
		}

		return nil
	}
}

// Receive returns the next broker sample in arrival order.
func (c *netConn) Receive(ctx context.Context) (ConnMessage, error) {
	err := ctx.Err()
	if err != nil {
		return ConnMessage{}, fmt.Errorf("mqtt: receive context: %w", err)
	}

	stop := context.AfterFunc(ctx, func() {
		c.broadcast()
	})
	defer stop()

	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		err = ctx.Err()
		if err != nil {
			return ConnMessage{}, fmt.Errorf("mqtt: receive context: %w", err)
		}

		if len(c.queue) > 0 {
			msg := c.queue[0]
			c.queue = append(c.queue[:0], c.queue[1:]...)

			return msg, nil
		}

		if c.closed {
			return ConnMessage{}, fmt.Errorf("mqtt: receive after close: %w", transports.ErrBackendClosed)
		}

		if c.readErr != nil {
			return ConnMessage{}, fmt.Errorf("mqtt: receive: %w", c.readErr)
		}

		c.wake.Wait()
	}
}

// Close ends the session with DISCONNECT and releases the socket.
//
// Close takes reconMu so an in-progress redial either finishes its
// swap before the close (and gets closed here) or sees the closed flag
// and discards its fresh socket instead of leaking it. Swap-vs-close
// exclusion is via reconMu: resume holds reconMu across check-then-swap
// while Close holds reconMu across goodbye plus socket close (the swap
// itself additionally takes writeMu, but the socket close here does
// not, so writeMu is not the exclusion lock).
func (c *netConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()

		return nil
	}

	c.closed = true
	c.failWaitersLocked(fmt.Errorf("mqtt: close: %w", transports.ErrBackendClosed))
	c.mu.Unlock()

	c.stop()

	c.pingOnce.Do(func() {
		close(c.pingStop)
	})

	c.reconMu.Lock()
	defer c.reconMu.Unlock()

	c.sendGoodbye()

	err := c.conn.Close()
	if err != nil {
		c.broadcast()

		return fmt.Errorf("mqtt: close conn: %w", err)
	}

	c.broadcast()

	return nil
}

// sendGoodbye writes DISCONNECT under the write lock with a short bound.
func (c *netConn) sendGoodbye() {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return
	}

	if err := c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return
	}

	if err := writePacket(c.conn, EncodeDisconnect(c.version)); err != nil {
		log.Printf("mqtt: goodbye write: %v", err)
	}

	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		log.Printf("mqtt: clear goodbye deadline: %v", err)
	}
}

// Reconnects returns the count of successful redials after drops.
func (c *netConn) Reconnects() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.reconnects
}
