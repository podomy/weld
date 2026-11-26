// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

// MQTT v5 wire primitives used by the weld backend.
//
// Scope is deliberately narrow: variable byte integers, length prefixed
// UTF-8 strings, and the property set the backend sends on PUBLISH
// (user properties only). The full broker session in netconn.go reuses
// these helpers so packet bytes stay identical between tests and live
// traffic. Limits follow the specification: a variable byte integer is
// at most 4 bytes and 268435455, a UTF-8 string is at most 65535 bytes.

// Single byte properties carry one byte values.
const (
	propPayloadFormatIndicator = 0x01
	propRequestProblemInfo     = 0x17
	propRequestResponseInfo    = 0x19
	propMaximumQoS             = 0x24
	propRetainAvailable        = 0x25
	propWildcardSubAvailable   = 0x28
	propSubIDAvailable         = 0x29
	propSharedSubAvailable     = 0x2A
)

// Two byte properties carry two byte integers.
const (
	propServerKeepAlive   = 0x13
	propReceiveMaximum    = 0x21
	propTopicAliasMaximum = 0x22
	propTopicAlias        = 0x23
)

// Four byte properties carry four byte integers.
const (
	propMessageExpiry     = 0x02
	propSessionExpiry     = 0x11
	propWillDelay         = 0x18
	propMaximumPacketSize = 0x27
)

// Variable byte integer properties carry a varint value.
const propSubscriptionID = 0x0B

// UTF-8 string properties carry one length prefixed string.
const (
	propContentType      = 0x03
	propResponseTopic    = 0x08
	propAssignedClientID = 0x12
	propAuthMethod       = 0x15
	propResponseInfo     = 0x1A
	propServerReference  = 0x1C
	propReasonString     = 0x1F
)

// Binary properties carry one length prefixed blob.
const (
	propCorrelationData = 0x09
	propAuthData        = 0x16
)

// String pair properties carry two length prefixed strings.
//
// propUserProperty is the MQTT v5 user property identifier. It is
// the only property the backend sends on PUBLISH.
const propUserProperty = 0x26

// maxVarint is the largest encodable variable byte integer.
const maxVarint = 268435455

// maxUTF8 is the largest encodable UTF-8 string in bytes.
const maxUTF8 = 65535

// typeHashProperty names the weld type hash in user properties.
const typeHashProperty = "type-hash"

// encodeVarint renders one variable byte integer.
func encodeVarint(value uint32) ([]byte, error) {
	if uint64(value) > maxVarint {
		return nil, fmt.Errorf("mqtt: varint %d above maximum %d", value, maxVarint)
	}

	out := make([]byte, 0, 4)
	remaining := value

	for {
		enc := byte(remaining % 128)
		remaining /= 128

		if remaining > 0 {
			enc |= 0x80
		}

		out = append(out, enc)

		if remaining == 0 {
			return out, nil
		}
	}
}

// decodeVarint parses one variable byte integer and reports bytes used.
func decodeVarint(buf []byte) (uint32, int, error) {
	var value uint32
	var shift uint

	for i, b := range buf {
		if i >= 4 {
			return 0, 0, errors.New("mqtt: varint exceeds 4 bytes") //nolint:wrapcheck // new error, nothing to wrap
		}

		value |= uint32(b&0x7F) << shift

		if b&0x80 == 0 {
			return value, i + 1, nil
		}

		shift += 7
	}

	return 0, 0, errors.New("mqtt: truncated varint") //nolint:wrapcheck // new error, nothing to wrap
}

// encodeString renders one length prefixed UTF-8 string.
func encodeString(value string) ([]byte, error) {
	if !utf8.ValidString(value) {
		return nil, errors.New("mqtt: string carries invalid utf-8") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsRune(value, 0) {
		return nil, errors.New("mqtt: string carries null character") //nolint:wrapcheck // new error, nothing to wrap
	}

	raw := []byte(value)

	if len(raw) > maxUTF8 {
		return nil, fmt.Errorf("mqtt: string %d bytes above maximum %d", len(raw), maxUTF8)
	}

	out := make([]byte, 2+len(raw))
	out[0] = byte(len(raw) >> 8) //nolint:gosec // G115: length checked against maxUTF8 above
	out[1] = byte(len(raw))      //nolint:gosec // G115: length checked against maxUTF8 above

	copy(out[2:], raw)

	return out, nil
}

// decodeString parses one length prefixed UTF-8 string.
func decodeString(buf []byte) (string, int, error) {
	if len(buf) < 2 {
		return "", 0, fmt.Errorf("mqtt: string shorter than header: %d", len(buf))
	}

	length := int(buf[0])<<8 | int(buf[1])

	if len(buf)-2 < length {
		return "", 0, fmt.Errorf("mqtt: string length %d exceeds %d", length, len(buf)-2)
	}

	raw := buf[2 : 2+length]
	if !utf8.Valid(raw) {
		return "", 0, errors.New("mqtt: string carries invalid utf-8") //nolint:wrapcheck // new error, nothing to wrap
	}

	if slices.Contains(raw, 0) {
		return "", 0, errors.New("mqtt: string carries null character") //nolint:wrapcheck // new error, nothing to wrap
	}

	return string(append([]byte(nil), raw...)), 2 + length, nil
}

// Properties carries MQTT v5 user properties for one sample.
//
// TypeHash holds the RIHS01 hash when known and empty when unknown.
// Extra carries caller defined pairs; keys and values must be valid
// UTF-8 without NUL characters (MQTT places no other restriction on
// user property content, and weld never interprets these as topics).
// The backend sends TypeHash as the type-hash user property and keeps
// the envelope type name as is.
type Properties struct {
	// TypeHash is the optional RIHS01 hash, empty when unknown.
	TypeHash string
	// Extra holds additional user property pairs, at most maxExtraProps;
	// more is rejected loudly on encode and decode. Nil means none.
	Extra map[string]string
}

// maxExtraProps bounds caller defined user property pairs per sample.
//
// The wire property length already caps total bytes, but each pair costs
// a map entry plus two strings. Without an entry cap a 40 MiB property
// section of tiny pairs allocates millions of entries and OOMs.
const maxExtraProps = 32

// encodeProperties renders user properties with a leading length varint.
//
// An empty property set encodes as a single zero byte. Each pair
// encodes as identifier 0x26 plus two UTF-8 strings.
func encodeProperties(props Properties) ([]byte, error) {
	if len(props.Extra) > maxExtraProps {
		return nil, fmt.Errorf("mqtt: extra properties %d above maximum %d", len(props.Extra), maxExtraProps)
	}

	body := make([]byte, 0, 32)

	if props.TypeHash != "" {
		encoded, err := appendUserProperty(body, typeHashProperty, props.TypeHash)
		if err != nil {
			return nil, err
		}

		body = encoded
	}

	for key, value := range props.Extra {
		encoded, err := appendUserProperty(body, key, value)
		if err != nil {
			return nil, err
		}

		body = encoded
	}

	prefix, err := encodeVarint(uint32(len(body))) //nolint:gosec // G115: strings are length capped; encodeVarint rejects oversize bodies
	if err != nil {
		return nil, err
	}

	return append(prefix, body...), nil
}

// appendUserProperty validates and appends one user property pair.
func appendUserProperty(body []byte, key, value string) ([]byte, error) {
	if !utf8.ValidString(key) || !utf8.ValidString(value) {
		return nil, errors.New("mqtt: property carries invalid utf-8") //nolint:wrapcheck // new error, nothing to wrap
	}

	if strings.ContainsRune(key, 0) || strings.ContainsRune(value, 0) {
		return nil, errors.New("mqtt: property carries null character") //nolint:wrapcheck // new error, nothing to wrap
	}

	keyBytes, err := encodeString(key)
	if err != nil {
		return nil, err
	}

	valueBytes, err := encodeString(value)
	if err != nil {
		return nil, err
	}

	body = append(body, propUserProperty)
	body = append(body, keyBytes...)
	body = append(body, valueBytes...)

	return body, nil
}

// decodeProperties parses the leading length varint plus user properties.
//
// Unknown property identifiers report an error so a broker or peer
// sending a property the backend cannot interpret fails loudly instead
// of silently dropping metadata. Duplicate type-hash keys keep the
// first value.
func decodeProperties(buf []byte) (Properties, int, error) {
	length, used, err := decodeVarint(buf)
	if err != nil {
		return Properties{}, 0, err
	}

	rest := buf[used:]

	if uint64(len(rest)) < uint64(length) {
		return Properties{}, 0, fmt.Errorf("mqtt: properties length %d exceeds %d", length, len(rest))
	}

	body := rest[:length]
	var props Properties

	for len(body) > 0 {
		consumed, err := decodePropertyPair(body, &props)
		if err != nil {
			return Properties{}, 0, err
		}

		body = body[consumed:]

		if len(props.Extra) > maxExtraProps {
			return Properties{}, 0, fmt.Errorf("mqtt: extra properties above maximum %d", maxExtraProps)
		}
	}

	return props, used + int(length), nil
}

// decodePropertyPair decodes one user property into props.
//
// Unknown identifiers fail loudly instead of silently dropping
// metadata. Duplicate type-hash keys keep the first value. It
// returns the bytes consumed by the pair.
func decodePropertyPair(body []byte, props *Properties) (int, error) {
	id := body[0]
	rest := body[1:]

	if !isPublishProp(uint32(id)) {
		return 0, fmt.Errorf("mqtt: unsupported property %d", id)
	}

	key, consumed, err := decodeString(rest)
	if err != nil {
		return 0, err
	}

	rest = rest[consumed:]

	value, consumed, err := decodeString(rest)
	if err != nil {
		return 0, err
	}

	rest = rest[consumed:]

	if key == typeHashProperty {
		if props.TypeHash == "" {
			props.TypeHash = value
		}

		return len(body) - len(rest), nil
	}

	if props.Extra == nil {
		props.Extra = make(map[string]string)
	}

	props.Extra[key] = value

	return len(body) - len(rest), nil
}

// isPublishProp reports whether id may appear on PUBLISH.
func isPublishProp(id uint32) bool {
	return id == propUserProperty
}

// isPubAckProp reports whether id may appear on PUBACK family.
func isPubAckProp(id uint32) bool {
	return id == propReasonString || id == propUserProperty
}

// isSubscribeProp reports whether id may appear on SUBSCRIBE.
func isSubscribeProp(id uint32) bool {
	return id == propSubscriptionID || id == propUserProperty
}

// isSubAckProp reports whether id may appear on SUBACK.
func isSubAckProp(id uint32) bool {
	return id == propReasonString || id == propUserProperty
}

// isUnsubscribeProp reports whether id may appear on UNSUBSCRIBE.
func isUnsubscribeProp(id uint32) bool {
	return id == propUserProperty
}

// isUnsubAckProp reports whether id may appear on UNSUBACK.
func isUnsubAckProp(id uint32) bool {
	return id == propReasonString || id == propUserProperty
}

// isAuthProp reports whether id may appear on AUTH.
func isAuthProp(id uint32) bool {
	return id == propAuthMethod || id == propAuthData ||
		id == propReasonString || id == propUserProperty
}

// isAllowedProp reports whether id may appear on packetType.
func isAllowedProp(packetType byte, id uint32) bool {
	switch packetType {
	case packetPublish:
		return isPublishProp(id)
	case packetPubAck, packetPubRec, packetPubRel, packetPubComp:
		return isPubAckProp(id)
	case packetSubscribe:
		return isSubscribeProp(id)
	case packetSubAck:
		return isSubAckProp(id)
	case packetUnsubscribe:
		return isUnsubscribeProp(id)
	case packetUnsubAck:
		return isUnsubAckProp(id)
	case packetAuth:
		return isAuthProp(id)
	default:
		return false
	}
}

// skipBytePropValue skips one single byte property value.
func skipBytePropValue(buf []byte) (int, error) {
	if len(buf) < 1 {
		return 0, errors.New("mqtt: byte property truncated") //nolint:wrapcheck // new error, nothing to wrap
	}

	return 1, nil
}

// skipTwoBytePropValue skips one two byte property value.
func skipTwoBytePropValue(buf []byte) (int, error) {
	if len(buf) < 2 {
		return 0, errors.New("mqtt: two byte property truncated") //nolint:wrapcheck // new error, nothing to wrap
	}

	return 2, nil
}

// skipFourBytePropValue skips one four byte property value.
func skipFourBytePropValue(buf []byte) (int, error) {
	if len(buf) < 4 {
		return 0, errors.New("mqtt: four byte property truncated") //nolint:wrapcheck // new error, nothing to wrap
	}

	return 4, nil
}

// skipVarintPropValue skips one variable byte integer property value.
func skipVarintPropValue(buf []byte) (int, error) {
	_, used, err := decodeVarint(buf)
	if err != nil {
		return 0, err
	}

	return used, nil
}

// skipStringPropValue skips one UTF-8 string property value.
func skipStringPropValue(buf []byte) (int, error) {
	_, used, err := decodeString(buf)
	if err != nil {
		return 0, err
	}

	return used, nil
}

// skipBinaryPropValue skips one binary blob property value.
func skipBinaryPropValue(buf []byte) (int, error) {
	if len(buf) < 2 {
		return 0, fmt.Errorf("mqtt: binary shorter than header: %d", len(buf))
	}

	length := int(buf[0])<<8 | int(buf[1])

	if len(buf)-2 < length {
		return 0, fmt.Errorf("mqtt: binary length %d exceeds %d", length, len(buf)-2)
	}

	return 2 + length, nil
}

// skipUserPropValue skips one string pair property value.
func skipUserPropValue(buf []byte) (int, error) {
	_, used, err := decodeString(buf)
	if err != nil {
		return 0, err
	}

	rest := buf[used:]

	_, used2, err := decodeString(rest)
	if err != nil {
		return 0, err
	}

	return used + used2, nil
}

// skipPropValue returns bytes consumed by the value of id in buf.
func skipPropValue(id uint32, buf []byte) (int, error) {
	switch id {
	case propPayloadFormatIndicator, propRequestProblemInfo,
		propRequestResponseInfo, propMaximumQoS,
		propRetainAvailable, propWildcardSubAvailable,
		propSubIDAvailable, propSharedSubAvailable:
		return skipBytePropValue(buf)
	case propServerKeepAlive, propReceiveMaximum,
		propTopicAliasMaximum, propTopicAlias:
		return skipTwoBytePropValue(buf)
	case propMessageExpiry, propSessionExpiry,
		propWillDelay, propMaximumPacketSize:
		return skipFourBytePropValue(buf)
	case propSubscriptionID:
		return skipVarintPropValue(buf)
	case propContentType, propResponseTopic,
		propAssignedClientID, propAuthMethod,
		propResponseInfo, propServerReference,
		propReasonString:
		return skipStringPropValue(buf)
	case propCorrelationData, propAuthData:
		return skipBinaryPropValue(buf)
	case propUserProperty:
		return skipUserPropValue(buf)
	default:
		return 0, fmt.Errorf("mqtt: unsupported property %d", id)
	}
}

// checkPropsAllowed validates raw props bytes for packetType.
func checkPropsAllowed(packetType byte, props []byte) error {
	rest := props

	for len(rest) > 0 {
		id, used, err := decodeVarint(rest)
		if err != nil {
			return err
		}

		rest = rest[used:]

		if !isAllowedProp(packetType, id) {
			return fmt.Errorf("mqtt: property %d not allowed on %d", id, packetType)
		}

		consumed, err := skipPropValue(id, rest)
		if err != nil {
			return err
		}

		rest = rest[consumed:]
	}

	return nil
}
