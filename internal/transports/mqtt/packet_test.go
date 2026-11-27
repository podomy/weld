// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"bytes"
	"net"
	"testing"
)

// TestConnectRoundTrip checks CONNECT encodes and CONNACK decodes accept.
func TestConnectRoundTrip(t *testing.T) {
	t.Parallel()

	packet, err := EncodeConnect(ConnectOptions{ClientID: "weld-test", KeepAlive: 30})
	if err != nil {
		t.Fatalf("encode connect: %v", err)
	}

	if packet[0]>>4 != packetConnect {
		t.Fatalf("connect type = %d, want %d", packet[0]>>4, packetConnect)
	}

	// Minimal accepted v5 CONNACK: flags plus reason 0 plus empty properties.
	ack := []byte{packetConnAck << 4, 0x03, 0x00, 0x00, 0x00}

	reason, err := DecodeConnAck(Version5, ack)
	if err != nil {
		t.Fatalf("decode connack: %v", err)
	}

	if reason != connackAccepted {
		t.Fatalf("reason = %d, want %d", reason, connackAccepted)
	}
}

// TestConnectAuthWill checks username, password, and will flags encode.
func TestConnectAuthWill(t *testing.T) {
	t.Parallel()

	packet, err := EncodeConnect(ConnectOptions{
		ClientID:     "c",
		KeepAlive:    30,
		Username:     "u",
		Password:     []byte("p"),
		WillTopic:    "w/t",
		WillPayload:  []byte{0x01},
		WillQoS:      1,
		WillRetained: true,
	})
	if err != nil {
		t.Fatalf("encode connect: %v", err)
	}

	// Body starts after the 2 byte fixed header: 6 byte proto name,
	// level, then flags. Flags must carry username, password, will,
	// will QoS 1, will retain, and clean start: 0xEE.
	if len(packet) < 11 {
		t.Fatalf("connect too short: %d", len(packet))
	}

	if packet[9] != 0xEE {
		t.Fatalf("connect flags = %#02x, want 0xee", packet[9])
	}

	_, err = EncodeConnect(ConnectOptions{ClientID: "c", WillQoS: 3, WillTopic: "w/t"})
	if err == nil {
		t.Fatalf("will qos 3 succeeded, want error")
	}
}

// TestConnAckRejectsRefusal checks nonzero reasons decode for the caller.
func TestConnAckRejectsRefusal(t *testing.T) {
	t.Parallel()

	ack := []byte{packetConnAck << 4, 0x03, 0x00, 0x05, 0x00}

	reason, err := DecodeConnAck(Version5, ack)
	if err != nil {
		t.Fatalf("decode connack: %v", err)
	}

	if reason == connackAccepted {
		t.Fatalf("refused reason reads accepted")
	}

	if _, err := DecodeConnAck(Version5, []byte{packetConnAck << 4, 0x02, 0x00, 0x00}); err == nil {
		t.Fatalf("v3 shaped v5 connack succeeded, want error")
	}

	_, err = DecodeConnAck(Version5, []byte{packetConnect << 4, 0x02, 0x00, 0x00})
	if err == nil {
		t.Fatalf("wrong type connack succeeded, want error")
	}
}

// TestPublishQoS0RoundTrip checks a QoS 0 publish with hash survives.
func TestPublishQoS0RoundTrip(t *testing.T) {
	t.Parallel()

	props := Properties{TypeHash: "RIHS01_abc"}
	payload := []byte{0x00, 0x01, 0x02}

	packet, err := EncodePublish(Version5, "weld/0/rt/chatter", 0, 0, false, props, payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, used, err := DecodePublish(Version5, packet)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if used != len(packet) {
		t.Fatalf("used = %d, want %d", used, len(packet))
	}

	if decoded.Topic != "weld/0/rt/chatter" {
		t.Fatalf("topic = %q", decoded.Topic)
	}

	if decoded.Props.TypeHash != "RIHS01_abc" {
		t.Fatalf("hash = %q", decoded.Props.TypeHash)
	}

	if !bytes.Equal(decoded.Payload, payload) {
		t.Fatalf("payload = %v, want %v", decoded.Payload, payload)
	}

	if decoded.QoS != 0 || decoded.PacketID != 0 || decoded.Retained {
		t.Fatalf("qos path = %+v, want qos 0 no id no retain", decoded)
	}
}

// TestPublishQoS1RoundTrip checks packet id plus retained survive.
func TestPublishQoS1RoundTrip(t *testing.T) {
	t.Parallel()

	packet, err := EncodePublish(Version5, "weld/0/rq/svc", 41, 1, true, Properties{}, []byte{0x09})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, _, err := DecodePublish(Version5, packet)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.PacketID != 41 {
		t.Fatalf("packet id = %d, want 41", decoded.PacketID)
	}

	if decoded.QoS != 1 || !decoded.Retained {
		t.Fatalf("qos path = %+v, want qos 1 retained", decoded)
	}

	if decoded.Dup {
		t.Fatalf("fresh publish dup is true, want false")
	}

	dup, err := EncodePublishDup(Version5, "weld/0/rq/svc", 41, 1, true, true, Properties{}, []byte{0x09})
	if err != nil {
		t.Fatalf("encode dup: %v", err)
	}

	redelivered, _, err := DecodePublish(Version5, dup)
	if err != nil {
		t.Fatalf("decode dup: %v", err)
	}

	if !redelivered.Dup || redelivered.PacketID != 41 {
		t.Fatalf("dup republish = %+v, want dup id 41", redelivered)
	}
}

// TestPublishQoS2RoundTrip checks QoS 2 carries a packet id like QoS 1.
func TestPublishQoS2RoundTrip(t *testing.T) {
	t.Parallel()

	checkQoS2Version(t, Version5, "v5")
	checkQoS2Version(t, Version311, "v3")
	checkQoS2Rejects(t)
}

// checkQoS2Version round-trips one QoS 2 publish on one version.
func checkQoS2Version(t *testing.T, version Version, name string) {
	t.Helper()

	packet, err := EncodePublishDup(version, "weld/0/rt/a", 41, 2, false, true, Properties{TypeHash: "RIHS01_qos2"}, []byte{0x01})
	if err != nil {
		t.Fatalf("encode %s: %v", name, err)
	}

	decoded, used, err := DecodePublish(version, packet)
	if err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}

	if used != len(packet) {
		t.Fatalf("%s used = %d, want %d", name, used, len(packet))
	}

	if decoded.QoS != 2 || decoded.PacketID != 41 || !decoded.Dup {
		t.Fatalf("%s decoded = %+v, want qos 2 id 41 dup", name, decoded)
	}

	if version == Version5 && decoded.Props.TypeHash != "RIHS01_qos2" {
		t.Fatalf("%s hash = %q, want RIHS01_qos2", name, decoded.Props.TypeHash)
	}
}

// checkQoS2Rejects checks QoS 3 fails on encode and decode.
func checkQoS2Rejects(t *testing.T) {
	t.Helper()

	if _, err := EncodePublish(Version5, "weld/0/rt/a", 1, 3, false, Properties{}, nil); err == nil {
		t.Fatalf("encode qos 3 succeeded, want error")
	}

	packet, err := EncodePublish(Version5, "weld/0/rt/a", 0, 0, false, Properties{}, []byte{0x01})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	packet[0] |= 0x06

	_, _, err = DecodePublish(Version5, packet)
	if err == nil {
		t.Fatalf("decode qos 3 succeeded, want error")
	}
}

// TestPublishRejectsDupQoS0 checks DUP with QoS 0 fails decode.
func TestPublishRejectsDupQoS0(t *testing.T) {
	t.Parallel()

	packet, err := EncodePublish(Version5, "weld/0/rt/a", 0, 0, false, Properties{}, []byte{0x01})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	packet[0] |= 0x08

	_, _, err = DecodePublish(Version5, packet)
	if err == nil {
		t.Fatalf("decode dup qos 0 succeeded, want error")
	}
}

// TestPubAckSubscribeEncode checks ack plus subscribe framing.
func TestPubAckSubscribeEncode(t *testing.T) {
	t.Parallel()

	ack, err := EncodePubAck(7)
	if err != nil {
		t.Fatalf("encode puback: %v", err)
	}

	id, err := decodeAckID(ack, packetPubAck)
	if err != nil {
		t.Fatalf("decode puback: %v", err)
	}

	if id != 7 {
		t.Fatalf("puback id = %d, want 7", id)
	}

	sub, err := EncodeSubscribe(Version5, 9, "weld/0/rt/#", 0)
	if err != nil {
		t.Fatalf("encode subscribe: %v", err)
	}

	if sub[0]>>4 != packetSubscribe {
		t.Fatalf("subscribe type = %d, want %d", sub[0]>>4, packetSubscribe)
	}

	id, err = decodeAckID([]byte{packetSubAck << 4, 0x02, 0x00, 0x09}, packetSubAck)
	if err != nil {
		t.Fatalf("decode suback: %v", err)
	}

	if id != 9 {
		t.Fatalf("suback id = %d, want 9", id)
	}
}

// TestConnectV311 checks 3.1.1 framing omits properties.
func TestConnectV311(t *testing.T) {
	t.Parallel()

	packet, err := EncodeConnect(ConnectOptions{Version: Version311, ClientID: "c", KeepAlive: 30, Username: "u"})
	if err != nil {
		t.Fatalf("encode connect: %v", err)
	}

	// Fixed header 2 bytes, body: 6 proto name, level, flags, 2 keepalive,
	// 3 client id field, 3 username field. v5 would add one property byte.
	const wantLen = 2 + 6 + 1 + 1 + 2 + 3 + 3

	if len(packet) != wantLen {
		t.Fatalf("connect len = %d, want %d", len(packet), wantLen)
	}

	if packet[8] != protocolLevel311 {
		t.Fatalf("protocol level = %#02x, want %#02x", packet[8], protocolLevel311)
	}

	if packet[9] != connectCleanStart|connectUsernameFlag {
		t.Fatalf("connect flags = %#02x, want %#02x", packet[9], connectCleanStart|connectUsernameFlag)
	}

	will, err := EncodeConnect(ConnectOptions{
		Version:      Version311,
		ClientID:     "c",
		KeepAlive:    30,
		WillTopic:    "w/t",
		WillPayload:  []byte{0x01},
		WillQoS:      1,
		WillRetained: true,
	})
	if err != nil {
		t.Fatalf("encode will connect: %v", err)
	}

	// Flags carry will, will QoS 1, and will retain on the v3 path.
	const wantWill = connectCleanStart | connectWillFlag | connectWillRetain | 0x08
	if will[9] != wantWill {
		t.Fatalf("will connect flags = %#02x, want %#02x", will[9], wantWill)
	}

	// v3 body carries no property lengths: 2 header plus 6 proto name,
	// level, flags, 2 keepalive, 3 client id, 5 will topic, 3 will
	// payload. v5 would add 2 property bytes.
	const wantWillLen = 2 + 6 + 1 + 1 + 2 + 3 + 5 + 3

	if len(will) != wantWillLen {
		t.Fatalf("will connect len = %d, want %d", len(will), wantWillLen)
	}

	// The will topic length follows the client id directly at [15].
	if will[15] != 0x00 || will[16] != 0x03 {
		t.Fatalf("will topic misframed: %v", will[12:18])
	}

	_, err = EncodeConnect(ConnectOptions{Version: Version(99), ClientID: "c"})
	if err == nil {
		t.Fatalf("unknown version connect succeeded, want error")
	}
}

// TestPublishV311RoundTrip checks hash drops and payload survives.
func TestPublishV311RoundTrip(t *testing.T) {
	t.Parallel()

	payload := []byte{0x0A, 0x0B}

	packet, err := EncodePublish(Version311, "weld/0/rt/chatter", 0, 0, false, Properties{TypeHash: "RIHS01_abc"}, payload)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, used, err := DecodePublish(Version311, packet)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if used != len(packet) {
		t.Fatalf("used = %d, want %d", used, len(packet))
	}

	if decoded.Topic != "weld/0/rt/chatter" {
		t.Fatalf("topic = %q", decoded.Topic)
	}

	if decoded.Props.TypeHash != "" {
		t.Fatalf("hash = %q, want empty on 3.1.1", decoded.Props.TypeHash)
	}

	if len(decoded.Payload) != len(payload) || string(decoded.Payload) != string(payload) {
		t.Fatalf("payload = %v, want %v", decoded.Payload, payload)
	}

	if decoded.QoS != 0 || decoded.Retained {
		t.Fatalf("qos path = %+v, want plain qos 0", decoded)
	}
}

// TestConnAckV311 checks the 2 byte 3.1.1 form decodes.
func TestConnAckV311(t *testing.T) {
	t.Parallel()

	ack := []byte{packetConnAck << 4, 0x02, 0x00, 0x00}

	reason, err := DecodeConnAck(Version311, ack)
	if err != nil {
		t.Fatalf("decode connack: %v", err)
	}

	if reason != connackAccepted {
		t.Fatalf("reason = %d, want accepted", reason)
	}

	bad := []byte{packetConnAck << 4, 0x02, 0x00, 0x04}

	reason, err = DecodeConnAck(Version311, bad)
	if err != nil {
		t.Fatalf("decode refused: %v", err)
	}

	if reason == connackAccepted {
		t.Fatalf("refused reason reads accepted")
	}

	_, err = DecodeConnAck(Version(99), ack)
	if err == nil {
		t.Fatalf("unknown version connack succeeded, want error")
	}
}

// TestSubscribeV311 checks 3.1.1 subscribe omits properties.
func TestSubscribeV311(t *testing.T) {
	t.Parallel()

	v5, err := EncodeSubscribe(Version5, 9, "weld/0/rt/#", 0)
	if err != nil {
		t.Fatalf("encode v5: %v", err)
	}

	v3, err := EncodeSubscribe(Version311, 9, "weld/0/rt/#", 0)
	if err != nil {
		t.Fatalf("encode v3: %v", err)
	}

	if len(v3) != len(v5)-1 {
		t.Fatalf("v3 len = %d, want %d", len(v3), len(v5)-1)
	}

	if v3[0]>>4 != packetSubscribe {
		t.Fatalf("subscribe type = %d", v3[0]>>4)
	}

	_, err = EncodeSubscribe(Version(99), 9, "weld/0/rt/#", 0)
	if err == nil {
		t.Fatalf("unknown version subscribe succeeded, want error")
	}
}

// TestDisconnectVersions checks the 2 versus 3 byte forms.
func TestDisconnectVersions(t *testing.T) {
	t.Parallel()

	v3 := EncodeDisconnect(Version311)
	if len(v3) != 2 || v3[0] != packetDisconnect<<4 || v3[1] != 0x00 {
		t.Fatalf("v3 disconnect = %v, want [0xe0 0x00]", v3)
	}

	v5 := EncodeDisconnect(Version5)
	if len(v5) != 3 || v5[0] != packetDisconnect<<4 || v5[1] != 0x01 || v5[2] != 0x00 {
		t.Fatalf("v5 disconnect = %v, want [0xe0 0x01 0x00]", v5)
	}
}

// TestPingReqFraming pins PINGREQ encoding on both versions.
//
// PINGREQ carries no version-dependent fields: one fixed two-byte
// packet serves keepalive for 3.1.1 and v5, decoded as PINGRESP on the
// wire. MQTT-3.12.1-1 requires the 0xC0 header with zero remaining
// length.
func TestPingReqFraming(t *testing.T) {
	t.Parallel()

	got := EncodePingReq()
	if len(got) != 2 || got[0] != packetPingReq<<4 || got[1] != 0x00 {
		t.Fatalf("pingreq = %v, want [0xc0 0x00]", got)
	}
}

// TestPublishVersionReject checks unknown versions fail loudly.
func TestPublishVersionReject(t *testing.T) {
	t.Parallel()

	_, err := EncodePublish(Version(99), "weld/0/rt/a", 0, 0, false, Properties{}, nil)
	if err == nil {
		t.Fatalf("unknown version encode succeeded, want error")
	}

	_, err = EncodePublishDup(Version(99), "weld/0/rt/a", 0, 0, false, false, Properties{}, nil)
	if err == nil {
		t.Fatalf("unknown version dup encode succeeded, want error")
	}

	good, err := EncodePublish(Version5, "weld/0/rt/a", 0, 0, false, Properties{}, []byte{0x01})
	if err != nil {
		t.Fatalf("encode v5: %v", err)
	}

	_, _, err = DecodePublish(Version(99), good)
	if err == nil {
		t.Fatalf("unknown version decode succeeded, want error")
	}
}

// TestCheckAckResult checks PUBACK and SUBACK validation on both versions.
func TestCheckAckResult(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		version Version
		packet  []byte
		wantErr bool
	}{
		{name: "v5 puback ok", version: Version5, packet: []byte{packetPubAck << 4, 0x02, 0x00, 0x07}},
		{name: "v5 puback reason ok", version: Version5, packet: []byte{packetPubAck << 4, 0x03, 0x00, 0x07, 0x00}},
		{name: "v5 puback refused", version: Version5, packet: []byte{packetPubAck << 4, 0x03, 0x00, 0x07, 0x87}, wantErr: true},
		{name: "v3 puback ok", version: Version311, packet: []byte{packetPubAck << 4, 0x02, 0x00, 0x07}},
		{name: "v3 puback trailing", version: Version311, packet: []byte{packetPubAck << 4, 0x03, 0x00, 0x07, 0x00}, wantErr: true},
		{name: "v5 suback ok", version: Version5, packet: []byte{packetSubAck << 4, 0x04, 0x00, 0x09, 0x00, 0x00}},
		{name: "v5 suback refused", version: Version5, packet: []byte{packetSubAck << 4, 0x04, 0x00, 0x09, 0x00, 0x80}, wantErr: true},
		{name: "v5 suback with props", version: Version5, packet: []byte{packetSubAck << 4, 0x06, 0x00, 0x09, 0x02, 0x26, 0x00, 0x00}},
		{name: "v3 suback ok", version: Version311, packet: []byte{packetSubAck << 4, 0x03, 0x00, 0x09, 0x01}},
		{name: "v3 suback refused", version: Version311, packet: []byte{packetSubAck << 4, 0x03, 0x00, 0x09, 0x80}, wantErr: true},
		{name: "v3 suback no grant", version: Version311, packet: []byte{packetSubAck << 4, 0x02, 0x00, 0x09}, wantErr: true},
	}

	for _, tc := range cases {
		err := checkAckResult(tc.version, tc.packet)
		if tc.wantErr && err == nil {
			t.Errorf("%s succeeded, want error", tc.name)
		}

		if !tc.wantErr && err != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
	}

	if err := checkAckResult(Version5, []byte{packetPublish << 4, 0x00}); err == nil {
		t.Errorf("publish as ack succeeded, want error")
	}
}

// TestReadPacketRejectsOversize checks huge remaining lengths fail fast.
func TestReadPacketRejectsOversize(t *testing.T) {
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

	go func() {
		if _, err := server.Write([]byte{packetPublish << 4, 0xFF, 0xFF, 0xFF, 0x7F}); err != nil {
			t.Logf("oversize probe write: %v", err)
		}
	}()

	if _, err := readPacket(client); err == nil {
		t.Fatalf("oversize read succeeded, want error")
	}
}

// TestConnAckStrict checks fixed flags, shapes, and property lengths.
func TestConnAckStrict(t *testing.T) {
	t.Parallel()

	if _, err := DecodeConnAck(Version5, []byte{packetConnAck<<4 | 0x01, 0x03, 0x00, 0x00, 0x00}); err == nil {
		t.Errorf("connack flags nonzero succeeded, want error")
	}

	if _, err := DecodeConnAck(Version5, []byte{packetConnAck << 4, 0x03, 0x02, 0x00, 0x00}); err == nil {
		t.Errorf("connack session flags 2 succeeded, want error")
	}

	if _, err := DecodeConnAck(Version311, []byte{packetConnAck << 4, 0x03, 0x00, 0x00, 0x00}); err == nil {
		t.Errorf("v3 connack 3 byte body succeeded, want error")
	}

	if _, err := DecodeConnAck(Version5, []byte{packetConnAck << 4, 0x03, 0x00, 0x00, 0x01, 0x00}); err == nil {
		t.Errorf("connack property overrun succeeded, want error")
	}

	if _, err := DecodeConnAck(Version5, []byte{packetConnAck << 4, 0x03, 0x00, 0x00, 0x00}); err != nil {
		t.Errorf("minimal v5 connack: %v", err)
	}
}

// TestConnectRejectsPasswordWithoutUsername checks encode parity with config.
func TestConnectRejectsPasswordWithoutUsername(t *testing.T) {
	t.Parallel()

	if _, err := EncodeConnect(ConnectOptions{ClientID: "c", Password: []byte("p")}); err == nil {
		t.Fatalf("password without username succeeded, want error")
	}
}

// TestDecodeStringRejectsBadUTF8 checks NUL and invalid sequences fail.
func TestDecodeStringRejectsBadUTF8(t *testing.T) {
	t.Parallel()

	if _, _, err := decodeString([]byte{0x00, 0x02, 0x61, 0x00}); err == nil {
		t.Errorf("NUL string succeeded, want error")
	}

	if _, _, err := decodeString([]byte{0x00, 0x02, 0xFF, 0xFE}); err == nil {
		t.Errorf("invalid utf-8 string succeeded, want error")
	}

	if _, err := encodeString("a\x00b"); err == nil {
		t.Errorf("NUL encode succeeded, want error")
	}
}

// TestExtraPropsCap checks the entry bound both directions.
func TestExtraPropsCap(t *testing.T) {
	t.Parallel()

	big := Properties{Extra: make(map[string]string, maxExtraProps+1)}
	for i := 0; i <= maxExtraProps; i++ {
		big.Extra[string(rune('a'+i%26))+string(rune('0'+i/26))] = "v"
	}

	if _, err := encodeProperties(big); err == nil {
		t.Fatalf("oversize extra encode succeeded, want error")
	}
}

// TestAckStrictness checks extended refusal codes and flag validation.
func TestAckStrictness(t *testing.T) {
	t.Parallel()

	err := checkAckResult(Version5, []byte{packetSubAck << 4, 0x04, 0x00, 0x09, 0x00, 0x87})
	if err == nil {
		t.Errorf("v5 suback 0x87 succeeded, want refusal")
	}

	err = checkAckResult(Version5, []byte{packetPubAck<<4 | 0x01, 0x02, 0x00, 0x07})
	if err == nil {
		t.Errorf("puback flags nonzero succeeded, want error")
	}

	err = checkAckResult(Version5, []byte{packetPubAck << 4, 0x04, 0x00, 0x07, 0x00, 0x02, 0xAA, 0xBB})
	if err == nil {
		t.Errorf("puback property overrun succeeded, want error")
	}
}

// TestQoS2AcksRoundTrip checks PUBREC plus PUBREL plus PUBCOMP.
func TestQoS2AcksRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		encode func(uint16) ([]byte, error)
		ptype  byte
		flags  byte
	}{
		{name: "pubrec", encode: EncodePubRec, ptype: packetPubRec},
		{name: "pubrel", encode: EncodePubRel, ptype: packetPubRel, flags: 0x02},
		{name: "pubcomp", encode: EncodePubComp, ptype: packetPubComp},
	}

	for _, tc := range cases {
		packet, err := tc.encode(21)
		if err != nil {
			t.Fatalf("%s encode: %v", tc.name, err)
		}

		if packet[0]>>4 != tc.ptype {
			t.Fatalf("%s type = %d, want %d", tc.name, packet[0]>>4, tc.ptype)
		}

		if packet[0]&0x0F != tc.flags {
			t.Fatalf("%s flags = %#02x, want %#02x", tc.name, packet[0]&0x0F, tc.flags)
		}

		id, err := decodeAckID(packet, tc.ptype)
		if err != nil {
			t.Fatalf("%s decode id: %v", tc.name, err)
		}

		if id != 21 {
			t.Fatalf("%s id = %d, want 21", tc.name, id)
		}

		err = checkAckResult(Version5, packet)
		if err != nil {
			t.Fatalf("%s v5 check: %v", tc.name, err)
		}

		err = checkAckResult(Version311, packet)
		if err != nil {
			t.Fatalf("%s v3 check: %v", tc.name, err)
		}
	}
}

// encodeUnsubscribePair encodes the same filter on both versions.
func encodeUnsubscribePair(t *testing.T) ([]byte, []byte) {
	t.Helper()

	v5, err := EncodeUnsubscribe(Version5, 33, "weld/0/rt/#")
	if err != nil {
		t.Fatalf("encode v5: %v", err)
	}

	v3, err := EncodeUnsubscribe(Version311, 33, "weld/0/rt/#")
	if err != nil {
		t.Fatalf("encode v3: %v", err)
	}

	return v5, v3
}

// checkUnsubscribeShapes checks the header flags and v3 length delta.
func checkUnsubscribeShapes(t *testing.T, v5 []byte, v3 []byte) {
	t.Helper()

	if len(v3) != len(v5)-1 {
		t.Fatalf("v3 len = %d, want %d", len(v3), len(v5)-1)
	}

	if v5[0]>>4 != packetUnsubscribe {
		t.Fatalf("type = %d, want %d", v5[0]>>4, packetUnsubscribe)
	}

	if v5[0]&0x0F != 0x02 {
		t.Fatalf("flags = %#02x, want 0x02", v5[0]&0x0F)
	}
}

// checkUnsubscribeDecode checks both versions decode to id 33.
func checkUnsubscribeDecode(t *testing.T, v5 []byte, v3 []byte) {
	t.Helper()

	id, filter, err := DecodeUnsubscribe(Version5, v5)
	if err != nil {
		t.Fatalf("decode v5: %v", err)
	}

	if id != 33 || filter != "weld/0/rt/#" {
		t.Fatalf("v5 = %d %q, want 33 filter", id, filter)
	}

	id, filter, err = DecodeUnsubscribe(Version311, v3)
	if err != nil {
		t.Fatalf("decode v3: %v", err)
	}

	if id != 33 || filter != "weld/0/rt/#" {
		t.Fatalf("v3 = %d %q, want 33 filter", id, filter)
	}
}

// checkUnsubscribeVersionReject checks unknown versions fail loudly.
func checkUnsubscribeVersionReject(t *testing.T, v5 []byte) {
	t.Helper()

	_, err := EncodeUnsubscribe(Version(99), 33, "weld/0/rt/#")
	if err == nil {
		t.Fatalf("unknown version encode succeeded, want error")
	}

	_, _, err = DecodeUnsubscribe(Version(99), v5)
	if err == nil {
		t.Fatalf("unknown version decode succeeded, want error")
	}
}

// TestUnsubscribeRoundTrip checks UNSUBSCRIBE on both versions.
func TestUnsubscribeRoundTrip(t *testing.T) {
	t.Parallel()

	v5, v3 := encodeUnsubscribePair(t)
	checkUnsubscribeShapes(t, v5, v3)
	checkUnsubscribeDecode(t, v5, v3)
	checkUnsubscribeVersionReject(t, v5)
}

// TestUnsubAckRoundTrip checks UNSUBACK single code on both versions.
func TestUnsubAckRoundTrip(t *testing.T) {
	t.Parallel()

	v5, err := EncodeUnsubAck(Version5, 33, 0x00)
	if err != nil {
		t.Fatalf("encode v5: %v", err)
	}

	v3, err := EncodeUnsubAck(Version311, 33, 0x00)
	if err != nil {
		t.Fatalf("encode v3: %v", err)
	}

	if len(v3) != len(v5)-1 {
		t.Fatalf("v3 len = %d, want %d", len(v3), len(v5)-1)
	}

	id, err := decodeAckID(v5, packetUnsubAck)
	if err != nil {
		t.Fatalf("decode v5 id: %v", err)
	}

	if id != 33 {
		t.Fatalf("v5 id = %d, want 33", id)
	}

	err = checkAckResult(Version5, v5)
	if err != nil {
		t.Fatalf("v5 check: %v", err)
	}

	err = checkAckResult(Version311, v3)
	if err != nil {
		t.Fatalf("v3 check: %v", err)
	}

	alt, err := EncodeUnsubAck(Version5, 34, 0x11)
	if err != nil {
		t.Fatalf("encode 0x11: %v", err)
	}

	err = checkAckResult(Version5, alt)
	if err != nil {
		t.Fatalf("0x11 check: %v", err)
	}

	_, err = EncodeUnsubAck(Version(99), 33, 0x00)
	if err == nil {
		t.Fatalf("unknown version encode succeeded, want error")
	}
}

// TestAuthRoundTrip checks AUTH v5 framing plus v3 rejection.
func TestAuthRoundTrip(t *testing.T) {
	t.Parallel()

	packet, err := EncodeAuth(Version5, 0x00)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if packet[0]>>4 != packetAuth {
		t.Fatalf("type = %d, want %d", packet[0]>>4, packetAuth)
	}

	if packet[0]&0x0F != 0 {
		t.Fatalf("flags = %#02x, want zero", packet[0]&0x0F)
	}

	reason, err := DecodeAuth(Version5, packet)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if reason != 0x00 {
		t.Fatalf("reason = %#02x, want 0x00", reason)
	}

	err = checkAckResult(Version5, packet)
	if err != nil {
		t.Fatalf("check: %v", err)
	}

	_, err = EncodeAuth(Version311, 0x00)
	if err == nil {
		t.Fatalf("v3 encode succeeded, want error")
	}

	_, err = DecodeAuth(Version311, packet)
	if err == nil {
		t.Fatalf("v3 decode succeeded, want error")
	}
}

// TestNewAckRejectRules checks flags, v3 extras, PUBLISH props.
func TestNewAckRejectRules(t *testing.T) {
	t.Parallel()

	rec, err := EncodePubRec(7)
	if err != nil {
		t.Fatalf("encode pubrec: %v", err)
	}

	rec[0] |= 0x01

	err = checkAckResult(Version5, rec)
	if err == nil {
		t.Errorf("pubrec flags nonzero succeeded, want error")
	}

	err = checkAckResult(Version311, []byte{packetPubRec << 4, 0x03, 0x00, 0x07, 0x00})
	if err == nil {
		t.Errorf("v3 pubrec extras succeeded, want error")
	}

	err = checkAckResult(Version311, []byte{packetPubRel << 4, 0x03, 0x00, 0x07, 0x00})
	if err == nil {
		t.Errorf("v3 pubrel extras succeeded, want error")
	}

	err = checkAckResult(Version311, []byte{packetPubComp << 4, 0x03, 0x00, 0x07, 0x00})
	if err == nil {
		t.Errorf("v3 pubcomp extras succeeded, want error")
	}

	err = checkAckResult(Version5, []byte{packetUnsubAck<<4 | 0x01, 0x03, 0x00, 0x07, 0x00})
	if err == nil {
		t.Errorf("unsuback flags nonzero succeeded, want error")
	}

	err = checkAckResult(Version311, []byte{packetUnsubAck << 4, 0x04, 0x00, 0x07, 0x00, 0x00})
	if err == nil {
		t.Errorf("v3 unsuback extras succeeded, want error")
	}

	err = checkAckResult(Version5, []byte{packetAuth<<4 | 0x01, 0x02, 0x00, 0x00})
	if err == nil {
		t.Errorf("auth flags nonzero succeeded, want error")
	}

	prefix, err := encodeVarint(2)
	if err != nil {
		t.Fatalf("encode length: %v", err)
	}

	prefix = append(prefix, propPayloadFormatIndicator, 0x00)

	_, _, err = decodeProperties(prefix)
	if err == nil {
		t.Errorf("publish 0x01 property succeeded, want error")
	}

	_, _, err = DecodeUnsubscribe(Version5, []byte{packetUnsubscribe << 4, 0x02, 0x00, 0x07})
	if err == nil {
		t.Errorf("unsubscribe short body succeeded, want error")
	}
}

// TestDecodeRejectsZeroPacketID checks QoS 1 and 2 reject id zero.
func TestDecodeRejectsZeroPacketID(t *testing.T) {
	t.Parallel()

	for _, qos := range []byte{1, 2} {
		packet, err := EncodePublish(Version5, "weld/0/rt/a", 7, qos, false, Properties{}, []byte{0x01})
		if err != nil {
			t.Fatalf("encode qos %d: %v", qos, err)
		}

		// Patch the packet id to zero: header is 2 bytes here
		// (remaining length under 128), then topic length plus topic.
		patched := append([]byte(nil), packet...)
		idOff := 2 + 2 + len("weld/0/rt/a")
		patched[idOff] = 0
		patched[idOff+1] = 0

		_, _, err = DecodePublish(Version5, patched)
		if err == nil {
			t.Fatalf("decode qos %d id zero succeeded, want error", qos)
		}
	}
}

// TestDecodeRejectsEmptyTopic checks empty PUBLISH topics fail both ways.
func TestDecodeRejectsEmptyTopic(t *testing.T) {
	t.Parallel()

	if _, err := EncodePublish(Version5, "", 0, 0, false, Properties{}, []byte{0x01}); err == nil {
		t.Fatalf("encode empty topic succeeded, want error")
	}

	raw := []byte{packetPublish << 4, 0x04, 0x00, 0x00, 0x00, 0x01}

	if _, _, err := DecodePublish(Version5, raw); err == nil {
		t.Fatalf("decode empty topic succeeded, want error")
	}
}

// TestSubscribeRejectsEmptyFilter checks empty SUBSCRIBE filters fail.
func TestSubscribeRejectsEmptyFilter(t *testing.T) {
	t.Parallel()

	if _, err := EncodeSubscribe(Version5, 9, "", 0); err == nil {
		t.Fatalf("encode empty subscribe filter succeeded, want error")
	}

	if _, err := EncodeUnsubscribe(Version5, 9, ""); err == nil {
		t.Fatalf("encode empty unsubscribe filter succeeded, want error")
	}

	// Empty UNSUBSCRIBE filter on the wire: id plus no props plus zero length.
	raw := []byte{packetUnsubscribe<<4 | 0x02, 0x05, 0x00, 0x09, 0x00, 0x00, 0x00}

	if _, _, err := DecodeUnsubscribe(Version5, raw); err == nil {
		t.Fatalf("decode empty unsubscribe filter succeeded, want error")
	}
}

// TestDecodeRejectsZeroUnsubAckID checks zero ids fail on UNSUBACK.
func TestDecodeRejectsZeroUnsubAckID(t *testing.T) {
	t.Parallel()

	raw := []byte{packetUnsubAck << 4, 0x04, 0x00, 0x00, 0x00, 0x00}

	id, err := decodeAckID(raw, packetUnsubAck)
	if err == nil {
		t.Fatalf("decode zero unsuback id = %d, want error", id)
	}
}

// TestDecodeRejectsZeroAckID checks zero ids fail on PUBACK and SUBACK.
func TestDecodeRejectsZeroAckID(t *testing.T) {
	t.Parallel()

	puback := []byte{packetPubAck << 4, 0x02, 0x00, 0x00}

	if _, err := decodeAckID(puback, packetPubAck); err == nil {
		t.Fatalf("decode zero puback id succeeded, want error")
	}

	suback := []byte{packetSubAck << 4, 0x04, 0x00, 0x00, 0x00, 0x00}

	if _, err := decodeAckID(suback, packetSubAck); err == nil {
		t.Fatalf("decode zero suback id succeeded, want error")
	}
}

// TestEncodeRejectsZeroPacketID checks encode rejects id zero where
// decode does, so malformed packets never reach the wire.
func TestEncodeRejectsZeroPacketID(t *testing.T) {
	t.Parallel()

	if _, err := EncodeSubscribe(Version5, 0, "weld/0/rt/#", 0); err == nil {
		t.Fatalf("encode subscribe id zero succeeded, want error")
	}

	if _, err := EncodeUnsubscribe(Version5, 0, "weld/0/rt/#"); err == nil {
		t.Fatalf("encode unsubscribe id zero succeeded, want error")
	}

	if _, err := EncodePublishDup(Version5, "weld/0/rt/a", 0, 1, false, false, Properties{}, []byte{0x01}); err == nil {
		t.Fatalf("encode qos1 publish id zero succeeded, want error")
	}

	if _, err := EncodePublishDup(Version5, "weld/0/rt/a", 0, 2, false, false, Properties{}, []byte{0x01}); err == nil {
		t.Fatalf("encode qos2 publish id zero succeeded, want error")
	}

	// QoS 0 carries no id, so zero stays legal there.
	if _, err := EncodePublishDup(Version5, "weld/0/rt/a", 0, 0, false, false, Properties{}, []byte{0x01}); err != nil {
		t.Fatalf("encode qos0 publish id zero failed: %v", err)
	}

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{"puback", func() error { _, err := EncodePubAck(0); return err }},
		{"pubrec", func() error { _, err := EncodePubRec(0); return err }},
		{"pubrel", func() error { _, err := EncodePubRel(0); return err }},
		{"pubcomp", func() error { _, err := EncodePubComp(0); return err }},
		{"unsuback", func() error { _, err := EncodeUnsubAck(Version5, 0, 0); return err }},
	} {
		if err := tc.call(); err == nil {
			t.Fatalf("encode %s id zero succeeded, want error", tc.name)
		}
	}
}

// TestEncodeRejectsDupQoS0 checks the encoder refuses DUP on QoS 0
// like the decoder does, per MQTT DUP-must-be-zero on QoS 0.
func TestEncodeRejectsDupQoS0(t *testing.T) {
	t.Parallel()

	if _, err := EncodePublishDup(Version5, "weld/0/rt/a", 0, 0, false, true, Properties{}, []byte{0x01}); err == nil {
		t.Fatalf("encode dup qos0 succeeded, want error")
	}
}

// TestEncodeRejectsV311ExtraProps checks 3.1.1 drops no caller data
// silently: extra user properties fail loudly instead.
func TestEncodeRejectsV311ExtraProps(t *testing.T) {
	t.Parallel()

	props := Properties{Extra: map[string]string{"k": "v"}}

	if _, err := EncodePublishDup(Version311, "weld/0/rt/a", 0, 0, false, false, props, []byte{0x01}); err == nil {
		t.Fatalf("encode v311 extra props succeeded, want error")
	}
}

// TestCheckAckResultRejectsZeroID checks the ack validator stands
// alone: zero ids fail even without the decodeAckID pre-check.
func TestCheckAckResultRejectsZeroID(t *testing.T) {
	t.Parallel()

	puback := []byte{packetPubAck << 4, 0x02, 0x00, 0x00}

	if err := checkAckResult(Version5, puback); err == nil {
		t.Fatalf("checkAckResult zero puback id succeeded, want error")
	}

	suback := []byte{packetSubAck << 4, 0x04, 0x00, 0x00, 0x00, 0x00}

	if err := checkAckResult(Version5, suback); err == nil {
		t.Fatalf("checkAckResult zero suback id succeeded, want error")
	}
}
