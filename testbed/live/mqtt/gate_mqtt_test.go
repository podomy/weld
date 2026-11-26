// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/mqtt"
)

// Live gate constants for the mosquitto sidecar.
const (
	mqttGateContainer = "weld-mosquitto"
	mqttGateDomain    = uint32(100)
	mqttGateTimeout   = 90 * time.Second
	mqttGateClientPre = "weld-gate"
)

// brokerAddr returns the plain MQTT address, defaulting to localhost.
func brokerAddr() string {
	if addr := strings.TrimSpace(os.Getenv("WELD_MQTT_BROKER")); addr != "" {
		return addr
	}

	return "127.0.0.1:1883"
}

// brokerTLSAddr returns the TLS MQTT address, defaulting to localhost.
func brokerTLSAddr() string {
	if addr := strings.TrimSpace(os.Getenv("WELD_MQTT_TLS_BROKER")); addr != "" {
		return addr
	}

	return "127.0.0.1:8883"
}

// brokerAuthAddr returns the authenticated MQTT address.
func brokerAuthAddr() string {
	if addr := strings.TrimSpace(os.Getenv("WELD_MQTT_AUTH_BROKER")); addr != "" {
		return addr
	}

	return "127.0.0.1:1884"
}

// certFile returns the path of one committed test certificate.
func certFile(name string) string {
	return "certs/" + name
}

// SkipIfNoBroker skips when the mosquitto sidecar is absent.
//
// It probes the docker daemon plus the container running state with no
// shell. A missing sidecar skips with the compose command instead of
// failing the tree.
func SkipIfNoBroker(t *testing.T, ctx context.Context) {
	t.Helper()

	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cmd := exec.CommandContext(probeCtx, "docker", "inspect", "-f", "{{.State.Running}}", mqttGateContainer)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("mosquitto sidecar absent, skipping live gate: %v output %s (run: cd testbed && docker compose up -d mosquitto)", err, strings.TrimSpace(string(out)))
	}

	if strings.TrimSpace(string(out)) != "true" {
		t.Skipf("mosquitto sidecar not running, skipping live gate (run: cd testbed && docker compose up -d mosquitto)")
	}
}

// openGateBackend dials the live broker and joins discovery.
func openGateBackend(t *testing.T, ctx context.Context, cfg mqtt.MqttConfig, participant string) *mqtt.Backend {
	t.Helper()

	raw, err := mqtt.Dial(ctx, cfg)
	if err != nil {
		t.Fatalf("dial %s: %v", cfg.BrokerURL, err)
	}

	backend, err := mqtt.NewWithConn(cfg, raw)
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	err = backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: participant, DomainID: mqttGateDomain})
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			t.Fatalf("join %s: %v (close: %v)", participant, err, cerr)
		}

		t.Fatalf("join %s: %v", participant, err)
	}

	t.Cleanup(func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("backend close: %v", cerr)
		}
	})

	return backend
}

// gateClientID returns a per test client id.
func gateClientID(t *testing.T, role string) string {
	name := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}

		return '-'
	}, t.Name())

	return fmt.Sprintf("%s-%s", mqttGateClientPre, strings.ToLower(role)+"-"+name)
}

// gateDialTCP checks one address accepts TCP before the test proceeds.
func gateDialTCP(t *testing.T, addr string) {
	t.Helper()

	dialer := net.Dialer{Timeout: 5 * time.Second}

	conn, err := dialer.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("broker tcp %s unreachable: %v", addr, err)
	}

	if err := conn.Close(); err != nil {
		t.Logf("probe close: %v", err)
	}
}

// waitForGate polls one condition until timeout or context end.
func waitForGate(t *testing.T, ctx context.Context, what string, timeout time.Duration, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("gate timed out waiting for %s", what)
		}

		select {
		case <-ctx.Done():
			t.Fatalf("gate wait for %s: %v", what, ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// TestMQTTGateDataRoundTrip proves data with hash against mosquitto.
func TestMQTTGateDataRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), mqttGateTimeout)
	defer cancel()

	SkipIfNoBroker(t, ctx)
	gateDialTCP(t, brokerAddr())

	const topic = "/gate/chatter"
	const typeName = "std_msgs__msg__String"
	const typeHash = "RIHS01_gatehash00000000000000000000000000000000000000000000000000"

	payload := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	talker := openGateBackend(t, ctx, mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "talker"),
		DomainID:  mqttGateDomain,
	}, "gate-talker")

	listener := openGateBackend(t, ctx, mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "listener"),
		DomainID:  mqttGateDomain,
	}, "gate-listener")

	err := talker.PublishBytes(ctx, transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  typeName,
		Payload:   payload,
		TypeHash:  typeHash,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, err := listener.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	if msg.TopicName != topic {
		t.Fatalf("topic = %q, want %q", msg.TopicName, topic)
	}

	if msg.TypeName != typeName {
		t.Fatalf("type = %q, want %q", msg.TypeName, typeName)
	}

	if msg.TypeHash != typeHash {
		t.Fatalf("hash = %q, want %q", msg.TypeHash, typeHash)
	}

	if msg.Leg != transports.LegData {
		t.Fatalf("leg = %q, want data", string(msg.Leg))
	}

	if len(msg.Payload) != len(payload) || string(msg.Payload) != string(payload) {
		t.Fatalf("payload = %v, want %v", msg.Payload, payload)
	}
}

// TestMQTTGateRetainedDiscovery proves a late joiner sees announcements.
func TestMQTTGateRetainedDiscovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), mqttGateTimeout)
	defer cancel()

	SkipIfNoBroker(t, ctx)
	gateDialTCP(t, brokerAddr())

	const topic = "/gate/discovery"
	const typeName = "std_msgs__msg__String"

	announcer := openGateBackend(t, ctx, mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "announcer"),
		DomainID:  mqttGateDomain,
	}, "gate-announcer")

	err := announcer.AnnounceEndpoint(ctx, "gate-announcer", mqtt.KindData, topic, typeName, "")
	if err != nil {
		t.Fatalf("announce: %v", err)
	}

	late := openGateBackend(t, ctx, mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "late"),
		DomainID:  mqttGateDomain,
	}, "gate-late")

	waitForGate(t, ctx, "late joiner announce", 30*time.Second, func() bool {
		return late.HasRemoteEndpoint(topic)
	})

	err = announcer.RetractEndpoint(ctx, "gate-announcer", mqtt.KindData, topic)
	if err != nil {
		t.Fatalf("retract: %v", err)
	}

	fresh := openGateBackend(t, ctx, mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "fresh"),
		DomainID:  mqttGateDomain,
	}, "gate-fresh")

	waitForGate(t, ctx, "fresh joiner retract", 30*time.Second, func() bool {
		return !fresh.HasRemoteEndpoint(topic)
	})
}

// TestMQTTGateServiceRoundTrip proves rq plus rr legs on mosquitto.
func TestMQTTGateServiceRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), mqttGateTimeout)
	defer cancel()

	SkipIfNoBroker(t, ctx)
	gateDialTCP(t, brokerAddr())

	const service = "/gate/add_two_ints"
	const typeName = "std_srvs__srv__AddTwoInts"

	client := openGateBackend(t, ctx, mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "client"),
		DomainID:  mqttGateDomain,
	}, "gate-client")

	server := openGateBackend(t, ctx, mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "server"),
		DomainID:  mqttGateDomain,
	}, "gate-server")

	err := client.PublishService(ctx, service, typeName, mqtt.ServiceRoleClient, []byte{0x02, 0x03}, "")
	if err != nil {
		t.Fatalf("publish request: %v", err)
	}

	taken, err := server.TakeService(ctx, mqtt.ServiceRoleServer)
	if err != nil {
		t.Fatalf("take request: %v", err)
	}

	if taken.Service != service || !taken.IsRequest {
		t.Fatalf("request = %+v, want %s request", taken, service)
	}

	err = server.PublishService(ctx, service, typeName, mqtt.ServiceRoleServer, []byte{0x05}, "")
	if err != nil {
		t.Fatalf("publish reply: %v", err)
	}

	reply, err := client.TakeService(ctx, mqtt.ServiceRoleClient)
	if err != nil {
		t.Fatalf("take reply: %v", err)
	}

	if reply.Service != service || reply.IsRequest {
		t.Fatalf("reply = %+v, want %s reply", reply, service)
	}

	if len(reply.Payload) != 1 || reply.Payload[0] != 0x05 {
		t.Fatalf("reply payload = %v, want [5]", reply.Payload)
	}
}

// TestMQTTGateTLSRoundTrip proves TLS sessions on both versions.
func TestMQTTGateTLSRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), mqttGateTimeout)
	defer cancel()

	SkipIfNoBroker(t, ctx)

	if _, err := os.Stat(certFile("ca.pem")); err != nil {
		t.Skipf("test CA absent, skipping TLS gate (run gen-certs.sh): %v", err)
	}

	gateDialTCP(t, brokerTLSAddr())

	for _, version := range []mqtt.Version{mqtt.Version5, mqtt.Version311} {
		roundTripTLS(t, ctx, version)
	}
}

// roundTripTLS runs one TLS data round trip for one version.
func roundTripTLS(t *testing.T, ctx context.Context, version mqtt.Version) {
	t.Helper()

	const topic = "/gate/tls"

	cfg := mqtt.MqttConfig{
		BrokerURL: "mqtts://" + brokerTLSAddr(),
		ClientID:  gateClientID(t, "tls-"+version.String()),
		DomainID:  mqttGateDomain,
		Version:   version,
	}
	cfg.TLS.CAFile = certFile("ca.pem")

	talker := openGateBackend(t, ctx, cfg, "gate-tls-talker-"+version.String())

	cfg.ClientID = gateClientID(t, "tls-listen-"+version.String())
	listener := openGateBackend(t, ctx, cfg, "gate-tls-listener-"+version.String())

	err := talker.PublishBytes(ctx, transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  "std_msgs__msg__String",
		Payload:   []byte{0x0A},
	})
	if err != nil {
		t.Fatalf("publish %s: %v", version, err)
	}

	msg, err := listener.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take %s: %v", version, err)
	}

	if msg.TopicName != topic || len(msg.Payload) != 1 || msg.Payload[0] != 0x0A {
		t.Fatalf("take %s = %+v, want %s with [10]", version, msg, topic)
	}
}

// TestMQTTGateAuth proves password authentication plus refusal.
func TestMQTTGateAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), mqttGateTimeout)
	defer cancel()

	SkipIfNoBroker(t, ctx)
	gateDialTCP(t, brokerAuthAddr())

	badCfg := mqtt.MqttConfig{
		BrokerURL: "tcp://" + brokerAuthAddr(),
		ClientID:  gateClientID(t, "bad"),
		DomainID:  mqttGateDomain,
		Username:  "fleet",
		Password:  "wrong-pass",
	}

	_, err := mqtt.Dial(ctx, badCfg)
	if err == nil {
		t.Fatalf("wrong password dial succeeded, want refusal")
	}

	cfg := mqtt.MqttConfig{
		BrokerURL: "tcp://" + brokerAuthAddr(),
		ClientID:  gateClientID(t, "auth"),
		DomainID:  mqttGateDomain,
		Username:  "fleet",
		Password:  "weld-test-pass",
	}

	authed := openGateBackend(t, ctx, cfg, "gate-authed")

	err = authed.PublishBytes(ctx, transports.OutgoingMessage{
		TopicName: "/gate/auth",
		TypeName:  "std_msgs__msg__String",
		Payload:   []byte{0x0B},
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, err := authed.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	if msg.TopicName != "/gate/auth" {
		t.Fatalf("topic = %q, want /gate/auth", msg.TopicName)
	}
}

// TestMQTTGateV311RoundTrip proves 3.1.1 interop on the same broker.
func TestMQTTGateV311RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), mqttGateTimeout)
	defer cancel()

	SkipIfNoBroker(t, ctx)
	gateDialTCP(t, brokerAddr())

	const topic = "/gate/v311"
	const typeName = "std_msgs__msg__String"

	cfg := mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "v311"),
		DomainID:  mqttGateDomain,
		Version:   mqtt.Version311,
	}

	talker := openGateBackend(t, ctx, cfg, "gate-v311-talker")

	cfg.ClientID = gateClientID(t, "v311-listen")
	listener := openGateBackend(t, ctx, cfg, "gate-v311-listener")

	msg := publishGateData(t, ctx, talker, listener, topic, typeName, []byte{0x0D}, "RIHS01_gatehash00000000000000000000000000000000000000000000000000")
	assertGateV311Take(t, msg, topic)

	gateServiceExchange(t, ctx, talker, listener, "/gate/v311svc")
}

// assertGateV311Take checks one v3 take has a leg and no hash.
func assertGateV311Take(t *testing.T, msg transports.IncomingMessage, topic string) {
	t.Helper()

	if msg.TopicName != topic || msg.Leg != transports.LegData {
		t.Fatalf("take = %+v, want %s data leg", msg, topic)
	}

	if msg.TypeHash != "" {
		t.Fatalf("hash = %q, want empty on 3.1.1", msg.TypeHash)
	}

	if len(msg.Payload) != 1 || msg.Payload[0] != 0x0D {
		t.Fatalf("payload = %v, want [13]", msg.Payload)
	}
}

// publishGateData publishes one sample and takes it on the peer.
func publishGateData(t *testing.T, ctx context.Context, talker, listener *mqtt.Backend, topic, typeName string, payload []byte, hash string) transports.IncomingMessage {
	t.Helper()

	err := talker.PublishBytes(ctx, transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  typeName,
		Payload:   payload,
		TypeHash:  hash,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg, err := listener.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}

	return msg
}

// TestMQTTGateQoS2RoundTrip proves exactly-once delivery on mosquitto.
func TestMQTTGateQoS2RoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), mqttGateTimeout)
	defer cancel()

	SkipIfNoBroker(t, ctx)
	gateDialTCP(t, brokerAddr())

	const topic = "/gate/qos2"
	const typeName = "std_msgs__msg__String"
	const typeHash = "RIHS01_gatehash00000000000000000000000000000000000000000000000000"

	talker := openGateBackend(t, ctx, mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "qos2-talker"),
		DomainID:  mqttGateDomain,
		QoS:       2,
	}, "gate-qos2-talker")

	listener := openGateBackend(t, ctx, mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "qos2-listen"),
		DomainID:  mqttGateDomain,
		QoS:       2,
	}, "gate-qos2-listener")

	msg := publishGateData(t, ctx, talker, listener, topic, typeName, []byte{0x0E}, typeHash)

	if msg.TopicName != topic || msg.Leg != transports.LegData {
		t.Fatalf("take = %+v, want %s data leg", msg, topic)
	}

	if msg.TypeHash != typeHash {
		t.Fatalf("hash = %q, want %q", msg.TypeHash, typeHash)
	}

	if len(msg.Payload) != 1 || msg.Payload[0] != 0x0E {
		t.Fatalf("payload = %v, want [14]", msg.Payload)
	}
}

// gateServiceExchange runs one request plus reply exchange over a broker.
func gateServiceExchange(t *testing.T, ctx context.Context, talker, listener *mqtt.Backend, service string) {
	t.Helper()

	err := talker.PublishService(ctx, service, "my_pkg__srv__AddTwoInts", mqtt.ServiceRoleClient, []byte{0x02}, "RIHS01_svc")
	if err != nil {
		t.Fatalf("publish request: %v", err)
	}

	taken, err := listener.TakeService(ctx, mqtt.ServiceRoleServer)
	if err != nil {
		t.Fatalf("take request: %v", err)
	}

	if taken.Service != service || !taken.IsRequest {
		t.Fatalf("request = %+v, want %s request", taken, service)
	}

	if taken.TypeHash != "" {
		t.Fatalf("service hash = %q, want empty on 3.1.1", taken.TypeHash)
	}

	err = listener.PublishService(ctx, service, "my_pkg__srv__AddTwoInts", mqtt.ServiceRoleServer, []byte{0x05}, "")
	if err != nil {
		t.Fatalf("publish reply: %v", err)
	}

	reply, err := talker.TakeService(ctx, mqtt.ServiceRoleClient)
	if err != nil {
		t.Fatalf("take reply: %v", err)
	}

	if reply.Service != service || reply.IsRequest {
		t.Fatalf("reply = %+v, want %s reply", reply, service)
	}
}

// TestMQTTGateReconnect proves the session redials across a restart.
func TestMQTTGateReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	SkipIfNoBroker(t, ctx)
	gateDialTCP(t, brokerAddr())

	cfg := mqtt.MqttConfig{
		BrokerURL: brokerAddr(),
		ClientID:  gateClientID(t, "resilient"),
		DomainID:  mqttGateDomain,
		QoS:       1,
	}

	backend := openGateBackend(t, ctx, cfg, "gate-resilient")

	restartCtx, restartCancel := context.WithTimeout(ctx, 60*time.Second)
	defer restartCancel()

	restart := exec.CommandContext(restartCtx, "docker", "restart", mqttGateContainer)
	out, err := restart.CombinedOutput()
	if err != nil {
		t.Fatalf("docker restart: %v output %s", err, strings.TrimSpace(string(out)))
	}

	waitForGate(t, ctx, "reconnect after broker restart", 90*time.Second, func() bool {
		return backend.Reconnects() >= 1
	})

	err = backend.PublishBytes(ctx, transports.OutgoingMessage{
		TopicName: "/gate/resilient",
		TypeName:  "std_msgs__msg__String",
		Payload:   []byte{0x0C},
	})
	if err != nil {
		t.Fatalf("publish after restart: %v", err)
	}

	msg, err := backend.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take after restart: %v", err)
	}

	if msg.TopicName != "/gate/resilient" {
		t.Fatalf("topic = %q, want /gate/resilient", msg.TopicName)
	}
}
