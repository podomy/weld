// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"testing"
)

// TestVarintRoundTrip checks boundary values encode and decode exactly.
func TestVarintRoundTrip(t *testing.T) {
	t.Parallel()

	values := []uint32{0, 1, 127, 128, 16383, 16384, 2097151, 268435455}

	for _, value := range values {
		encoded, err := encodeVarint(value)
		if err != nil {
			t.Fatalf("encode %d: %v", value, err)
		}

		decoded, used, err := decodeVarint(encoded)
		if err != nil {
			t.Fatalf("decode %d: %v", value, err)
		}

		if decoded != value {
			t.Fatalf("varint = %d, want %d", decoded, value)
		}

		if used != len(encoded) {
			t.Fatalf("varint %d used = %d, want %d", value, used, len(encoded))
		}
	}
}

// TestVarintRejectsOverflow checks values above the maximum fail.
func TestVarintRejectsOverflow(t *testing.T) {
	t.Parallel()

	_, err := encodeVarint(maxVarint + 1)
	if err == nil {
		t.Fatalf("encode above maximum succeeded, want error")
	}

	_, _, err = decodeVarint([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x01})
	if err == nil {
		t.Fatalf("decode 5 byte varint succeeded, want error")
	}

	_, _, err = decodeVarint([]byte{0x80})
	if err == nil {
		t.Fatalf("decode truncated varint succeeded, want error")
	}
}

// TestStringRoundTrip checks length prefixed strings survive the trip.
func TestStringRoundTrip(t *testing.T) {
	t.Parallel()

	encoded, err := encodeString("type-hash")
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, used, err := decodeString(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded != "type-hash" {
		t.Fatalf("string = %q, want type-hash", decoded)
	}

	if used != len(encoded) {
		t.Fatalf("used = %d, want %d", used, len(encoded))
	}
}

// TestStringRejectsBounds checks oversize and truncated strings fail.
func TestStringRejectsBounds(t *testing.T) {
	t.Parallel()

	big := make([]byte, maxUTF8+1)
	_, err := encodeString(string(big))
	if err == nil {
		t.Fatalf("encode oversize succeeded, want error")
	}

	_, _, err = decodeString([]byte{0x00})
	if err == nil {
		t.Fatalf("decode short header succeeded, want error")
	}

	_, _, err = decodeString([]byte{0x00, 0x05, 0x41, 0x42})
	if err == nil {
		t.Fatalf("decode truncated body succeeded, want error")
	}
}

// TestPropertiesRoundTrip checks hash plus extras survive the trip.
func TestPropertiesRoundTrip(t *testing.T) {
	t.Parallel()

	empty, err := encodeProperties(Properties{})
	if err != nil {
		t.Fatalf("encode empty: %v", err)
	}

	if len(empty) != 1 || empty[0] != 0 {
		t.Fatalf("empty properties = %v, want [0]", empty)
	}

	props := Properties{TypeHash: "RIHS01_abc", Extra: map[string]string{"frame": "map"}}

	encoded, err := encodeProperties(props)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, used, err := decodeProperties(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if used != len(encoded) {
		t.Fatalf("used = %d, want %d", used, len(encoded))
	}

	if decoded.TypeHash != "RIHS01_abc" {
		t.Fatalf("hash = %q, want RIHS01_abc", decoded.TypeHash)
	}

	if decoded.Extra["frame"] != "map" {
		t.Fatalf("extra = %v, want frame map", decoded.Extra)
	}
}

// TestPropertiesRejectsUnknownID checks foreign properties fail loudly.
func TestPropertiesRejectsUnknownID(t *testing.T) {
	t.Parallel()

	encoded, err := encodeVarint(3)
	if err != nil {
		t.Fatalf("encode length: %v", err)
	}

	encoded = append(encoded, 0x01, 0x00)

	_, _, err = decodeProperties(encoded)
	if err == nil {
		t.Fatalf("decode unknown property succeeded, want error")
	}
}

// TestPropertiesKeepsFirstHash checks duplicate hashes keep the first.
func TestPropertiesKeepsFirstHash(t *testing.T) {
	t.Parallel()

	props := Properties{TypeHash: "RIHS01_first"}
	encoded, err := encodeProperties(props)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Append a second type-hash pair manually.
	second, err := encodeString(typeHashProperty)
	if err != nil {
		t.Fatalf("encode key: %v", err)
	}

	value, err := encodeString("RIHS01_second")
	if err != nil {
		t.Fatalf("encode value: %v", err)
	}

	body := encoded[1:]
	body = append(body, propUserProperty)
	body = append(body, second...)
	body = append(body, value...)

	prefix, err := encodeVarint(uint32(len(body))) //nolint:gosec // G115: test appends one small pair, far below the varint ceiling
	if err != nil {
		t.Fatalf("encode prefix: %v", err)
	}

	decoded, _, err := decodeProperties(append(prefix, body...))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.TypeHash != "RIHS01_first" {
		t.Fatalf("hash = %q, want RIHS01_first", decoded.TypeHash)
	}
}

// checkPropConstant fails when one property constant drifts.
func checkPropConstant(t *testing.T, name string, got byte, want byte) {
	t.Helper()

	if got != want {
		t.Fatalf("%s = %#02x, want %#02x", name, got, want)
	}
}

// TestPropertyTableValues checks wire values match section 2.2.2.
func TestPropertyTableValues(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		got  byte
		want byte
	}{
		{name: "format", got: propPayloadFormatIndicator, want: 0x01},
		{name: "expiry", got: propMessageExpiry, want: 0x02},
		{name: "content", got: propContentType, want: 0x03},
		{name: "sub id", got: propSubscriptionID, want: 0x0B},
		{name: "session", got: propSessionExpiry, want: 0x11},
		{name: "client id", got: propAssignedClientID, want: 0x12},
		{name: "keep alive", got: propServerKeepAlive, want: 0x13},
		{name: "auth method", got: propAuthMethod, want: 0x15},
		{name: "auth data", got: propAuthData, want: 0x16},
		{name: "reason", got: propReasonString, want: 0x1F},
		{name: "receive", got: propReceiveMaximum, want: 0x21},
		{name: "alias", got: propTopicAlias, want: 0x23},
		{name: "max qos", got: propMaximumQoS, want: 0x24},
		{name: "user", got: propUserProperty, want: 0x26},
		{name: "max packet", got: propMaximumPacketSize, want: 0x27},
		{name: "shared", got: propSharedSubAvailable, want: 0x2A},
	}

	for _, tc := range cases {
		checkPropConstant(t, tc.name, tc.got, tc.want)
	}
}

// checkPublishAllowedSet checks the PUBLISH allow list.
func checkPublishAllowedSet(t *testing.T) {
	t.Helper()

	if !isPublishProp(propUserProperty) {
		t.Fatalf("publish must allow user property")
	}

	if isPublishProp(propReasonString) {
		t.Fatalf("publish must reject reason string")
	}

	if isPublishProp(propPayloadFormatIndicator) {
		t.Fatalf("publish must reject format indicator")
	}
}

// checkPubAckAllowedSet checks the PUBACK family allow list.
func checkPubAckAllowedSet(t *testing.T) {
	t.Helper()

	if !isPubAckProp(propReasonString) {
		t.Fatalf("puback must allow reason string")
	}

	if !isAllowedProp(packetPubRec, propReasonString) {
		t.Fatalf("pubrec must allow reason string")
	}

	if isAllowedProp(packetPublish, propReasonString) {
		t.Fatalf("publish must reject reason via dispatch")
	}
}

// checkSubscribeAllowedSet checks the SUBSCRIBE allow list.
func checkSubscribeAllowedSet(t *testing.T) {
	t.Helper()

	if !isSubscribeProp(propSubscriptionID) {
		t.Fatalf("subscribe must allow sub id")
	}

	if isSubscribeProp(propReasonString) {
		t.Fatalf("subscribe must reject reason string")
	}
}

// checkUnsubscribeAllowedSet checks the UNSUBSCRIBE allow list.
func checkUnsubscribeAllowedSet(t *testing.T) {
	t.Helper()

	if !isUnsubscribeProp(propUserProperty) {
		t.Fatalf("unsubscribe must allow user property")
	}

	if isUnsubscribeProp(propReasonString) {
		t.Fatalf("unsubscribe must reject reason string")
	}
}

// checkUnsubAckAllowedSet checks the UNSUBACK allow list.
func checkUnsubAckAllowedSet(t *testing.T) {
	t.Helper()

	if !isUnsubAckProp(propReasonString) {
		t.Fatalf("unsuback must allow reason string")
	}
}

// checkAuthAllowedSet checks the AUTH allow list.
func checkAuthAllowedSet(t *testing.T) {
	t.Helper()

	if !isAuthProp(propAuthMethod) {
		t.Fatalf("auth must allow method")
	}

	if !isAuthProp(propAuthData) {
		t.Fatalf("auth must allow data")
	}

	if isAuthProp(propTopicAlias) {
		t.Fatalf("auth must reject topic alias")
	}

	if isAllowedProp(packetAuth, propTopicAlias) {
		t.Fatalf("auth must reject alias via dispatch")
	}
}

// TestAllowedSets checks per packet property allow lists.
func TestAllowedSets(t *testing.T) {
	t.Parallel()

	checkPublishAllowedSet(t)
	checkPubAckAllowedSet(t)
	checkSubscribeAllowedSet(t)
	checkUnsubscribeAllowedSet(t)
	checkUnsubAckAllowedSet(t)
	checkAuthAllowedSet(t)
}

// TestCheckPropsAllowed checks raw props validation per packet.
func TestCheckPropsAllowed(t *testing.T) {
	t.Parallel()

	err := checkPropsAllowed(packetPublish, []byte{})
	if err != nil {
		t.Fatalf("empty publish props: %v", err)
	}

	err = checkPropsAllowed(packetUnsubAck, []byte{})
	if err != nil {
		t.Fatalf("empty unsuback props: %v", err)
	}

	err = checkPropsAllowed(packetAuth, []byte{})
	if err != nil {
		t.Fatalf("empty auth props: %v", err)
	}

	err = checkPropsAllowed(packetPublish, []byte{propReasonString, 0x00, 0x01, 0x41})
	if err == nil {
		t.Fatalf("publish reason string succeeded, want error")
	}

	reason, err := encodeString("oops")
	if err != nil {
		t.Fatalf("encode reason: %v", err)
	}

	props := append([]byte{propReasonString}, reason...)

	err = checkPropsAllowed(packetUnsubAck, props)
	if err != nil {
		t.Fatalf("unsuback reason props: %v", err)
	}

	err = checkPropsAllowed(packetUnsubscribe, props)
	if err == nil {
		t.Fatalf("unsubscribe reason props succeeded, want error")
	}
}

// TestPublishStillRejectsTableIDs checks the table did not widen PUBLISH.
func TestPublishStillRejectsTableIDs(t *testing.T) {
	t.Parallel()

	ids := []byte{
		propPayloadFormatIndicator,
		propMessageExpiry,
		propSubscriptionID,
		propSessionExpiry,
		propReasonString,
	}

	for _, id := range ids {
		prefix, err := encodeVarint(2)
		if err != nil {
			t.Fatalf("encode length: %v", err)
		}

		prefix = append(prefix, id, 0x00)

		_, _, err = decodeProperties(prefix)
		if err == nil {
			t.Fatalf("publish id %#02x succeeded, want error", id)
		}
	}
}
