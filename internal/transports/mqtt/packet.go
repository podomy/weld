// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"errors"
	"fmt"
)

// MQTT v5 packet framing for the weld data path.
//
// Only the packets the backend exchanges with a broker are modeled:
// CONNECT plus CONNACK for session start, SUBSCRIBE plus SUBACK for the
// domain wildcard, PUBLISH plus PUBACK for QoS 0 and QoS 1 samples,
// PINGREQ plus PINGRESP for keepalive, DISCONNECT for clean teardown,
// plus PUBREC plus PUBREL plus PUBCOMP for QoS 2 exactly-once plus
// UNSUBSCRIBE plus UNSUBACK plus AUTH framing. Packet type nibbles
// and flag bits follow the specification exactly so captures from
// mosquitto decode byte for byte.

// Packet type nibbles for the fixed header high half byte.
const (
	packetConnect     = 0x1
	packetConnAck     = 0x2
	packetPublish     = 0x3
	packetPubAck      = 0x4
	packetPubRec      = 0x5
	packetPubRel      = 0x6
	packetPubComp     = 0x7
	packetSubscribe   = 0x8
	packetSubAck      = 0x9
	packetUnsubscribe = 0xA
	packetUnsubAck    = 0xB
	packetPingReq     = 0xC
	packetPingResp    = 0xD
	packetDisconnect  = 0xE
	packetAuth        = 0xF
)

// Protocol constants for session start.
const (
	protocolName = "MQTT"
	// protocolLevelV5 is the v5 protocol level.
	protocolLevelV5 = 0x05
	// protocolLevel311 is the 3.1.1 protocol level.
	protocolLevel311 = 0x04
)

// Connect flag bits for session start.
const (
	connectUsernameFlag = 0x80
	connectPasswordFlag = 0x40
	connectWillRetain   = 0x20
	connectWillFlag     = 0x04
	connectCleanStart   = 0x02
)

// connackAccepted marks a successful session in a CONNACK body.
const connackAccepted = 0x00

// ConnectOptions carries one MQTT CONNECT.
//
// Password never appears in errors; callers must keep it out of logs.
// A WillQoS above 2 is rejected. An empty WillTopic disables the will
// even when other will fields are set. Version selects the framing:
// v5 sends property lengths, 3.1.1 omits them.
type ConnectOptions struct {
	// Version selects the protocol version. Zero means Version5.
	Version Version
	// ClientID is the MQTT client identifier.
	ClientID string
	// KeepAlive paces keepalive in seconds.
	KeepAlive uint16
	// Username is the MQTT username. Empty means anonymous.
	Username string
	// Password is the MQTT password. Nil means none.
	Password []byte
	// WillTopic is the last will topic. Empty disables the will.
	WillTopic string
	// WillPayload is the last will body.
	WillPayload []byte
	// WillQoS is the last will quality of service, 0 through 2.
	WillQoS byte
	// WillRetained selects a retained will.
	WillRetained bool
}

// encodeBinary renders one length prefixed binary blob.
func encodeBinary(data []byte) ([]byte, error) {
	if len(data) > maxUTF8 {
		return nil, fmt.Errorf("mqtt: binary %d bytes above maximum %d", len(data), maxUTF8)
	}

	out := make([]byte, 2+len(data))
	out[0] = byte(len(data) >> 8) //nolint:gosec // G115: length checked against maxUTF8 above
	out[1] = byte(len(data))      //nolint:gosec // G115: length checked against maxUTF8 above

	copy(out[2:], data)

	return out, nil
}

// encodeFixedHeader renders one fixed header plus remaining length.
func encodeFixedHeader(packetType byte, flags byte, remaining int) ([]byte, error) {
	if remaining < 0 || uint64(remaining) > maxVarint {
		return nil, fmt.Errorf("mqtt: remaining length %d out of range", remaining)
	}

	length, err := encodeVarint(uint32(remaining))
	if err != nil {
		return nil, err
	}

	out := []byte{packetType<<4 | (flags & 0x0F)}
	out = append(out, length...)

	return out, nil
}

// EncodeConnect renders one MQTT CONNECT with clean start or session.
//
// An empty will topic masks the will fields: non-empty will payload,
// QoS, or retained flag with no topic are discarded here instead of
// reaching the wire. Config validation rejects that combination
// loudly; the codec masks defensively so an unvalidated ConnectOptions
// cannot emit a half-will.
func EncodeConnect(opts ConnectOptions) ([]byte, error) {
	if err := checkConnectVersion(opts.Version); err != nil {
		return nil, err
	}

	if len(opts.Password) > 0 && opts.Username == "" {
		return nil, errors.New("mqtt: password without username") //nolint:wrapcheck // new error, nothing to wrap
	}

	if opts.WillQoS > maxQoS {
		return nil, fmt.Errorf("mqtt: will qos %d above supported %d", opts.WillQoS, maxQoS)
	}

	flags := connectFlags(opts)
	useWill := opts.WillTopic != ""

	name, err := encodeString(protocolName)
	if err != nil {
		return nil, err
	}

	id, err := encodeString(opts.ClientID)
	if err != nil {
		return nil, err
	}

	body := make([]byte, 0, len(name)+10+len(id))
	body = append(body, name...)
	body = append(body, connectLevel(opts.Version), flags, byte(opts.KeepAlive>>8), byte(opts.KeepAlive)) //nolint:gosec // G115: keepalive halves always fit in one byte

	if opts.Version == Version5 {
		props, err := encodeVarint(0)
		if err != nil {
			return nil, err
		}

		body = append(body, props...)
	}

	body = append(body, id...)

	body, err = appendConnectWill(body, opts, useWill)
	if err != nil {
		return nil, err
	}

	body, err = appendConnectCredentials(body, opts)
	if err != nil {
		return nil, err
	}

	header, err := encodeFixedHeader(packetConnect, 0, len(body))
	if err != nil {
		return nil, err
	}

	return append(header, body...), nil
}

// checkConnectVersion rejects unknown protocol versions.
func checkConnectVersion(version Version) error {
	switch version {
	case Version5, Version311:
		return nil
	default:
		return fmt.Errorf("mqtt: connect version %d unknown", int(version))
	}
}

// connectLevel returns the protocol level byte for one version.
func connectLevel(version Version) byte {
	if version == Version311 {
		return protocolLevel311
	}

	return protocolLevelV5
}

// connectFlags renders the CONNECT flag byte for one option set.
//
// Clean start is always set: weld keeps no persistent sessions, so
// every redial resubscribes and replays instead of resuming broker
// state (see reconnect.go). Password without username is rejected at
// EncodeConnect time as well as config time (MQTT-3.1.2-22).
func connectFlags(opts ConnectOptions) byte {
	flags := byte(connectCleanStart)

	if opts.Username != "" {
		flags |= connectUsernameFlag
	}

	if len(opts.Password) > 0 {
		flags |= connectPasswordFlag
	}

	if opts.WillTopic != "" {
		flags |= connectWillFlag

		if opts.WillRetained {
			flags |= connectWillRetain
		}

		flags |= opts.WillQoS << 3
	}

	return flags
}

// appendConnectWill appends the will section when configured.
func appendConnectWill(body []byte, opts ConnectOptions, useWill bool) ([]byte, error) {
	if !useWill {
		return body, nil
	}

	if opts.Version == Version5 {
		willProps, err := encodeVarint(0)
		if err != nil {
			return nil, err
		}

		body = append(body, willProps...)
	}

	willTopic, err := encodeString(opts.WillTopic)
	if err != nil {
		return nil, err
	}

	body = append(body, willTopic...)

	willPayload, err := encodeBinary(opts.WillPayload)
	if err != nil {
		return nil, err
	}

	return append(body, willPayload...), nil
}

// appendConnectCredentials appends username and password when set.
func appendConnectCredentials(body []byte, opts ConnectOptions) ([]byte, error) {
	if opts.Username != "" {
		user, err := encodeString(opts.Username)
		if err != nil {
			return nil, err
		}

		body = append(body, user...)
	}

	if len(opts.Password) > 0 {
		password, err := encodeBinary(opts.Password)
		if err != nil {
			return nil, err
		}

		body = append(body, password...)
	}

	return body, nil
}

// DecodeConnAck validates one CONNACK packet and reports the reason code.
//
// Version selects the framing: v5 carries session present plus reason
// plus properties, 3.1.1 carries session present plus return code only.
// Reason 0 accepts on both; anything else fails the dial. Fixed header
// flags must be zero and the session present byte must be 0 or 1 with
// reserved bits clear on both versions.
func DecodeConnAck(version Version, packet []byte) (byte, error) {
	body, err := decodeConnAckBody(version, packet)
	if err != nil {
		return 0, err
	}

	if version == Version311 {
		return body[1], nil
	}

	if err := checkConnAckProps(body); err != nil {
		return 0, err
	}

	return body[1], nil
}

// decodeConnAckBody validates framing and returns the CONNACK body.
func decodeConnAckBody(version Version, packet []byte) ([]byte, error) {
	if len(packet) < 2 {
		return nil, fmt.Errorf("mqtt: connack shorter than header: %d", len(packet))
	}

	if packet[0]>>4 != packetConnAck {
		return nil, fmt.Errorf("mqtt: connack type %d is not %d", packet[0]>>4, packetConnAck)
	}

	if packet[0]&0x0F != 0 {
		return nil, fmt.Errorf("mqtt: connack flags %#02x must be zero", packet[0]&0x0F)
	}

	switch version {
	case Version5, Version311:
	default:
		return nil, fmt.Errorf("mqtt: connack version %d unknown", int(version))
	}

	length, used, err := decodeVarint(packet[1:])
	if err != nil {
		return nil, err
	}

	body := packet[1+used:]

	if len(body) != int(length) {
		return nil, fmt.Errorf("mqtt: connack length %d mismatches %d", length, len(body))
	}

	if len(body) < 2 {
		return nil, fmt.Errorf("mqtt: connack body shorter than flags plus reason: %d", len(body))
	}

	if body[0]&0xFE != 0 {
		return nil, fmt.Errorf("mqtt: connack session flags %#02x must be 0 or 1", body[0])
	}

	if version == Version311 {
		if len(body) != 2 {
			return nil, fmt.Errorf("mqtt: v3 connack length %d must be 2", len(body))
		}
	}

	return body, nil
}

// checkConnAckProps validates the v5 CONNACK property section.
//
// Only framing is checked: weld acts on nothing inside CONNACK
// properties, so unknown identifiers are skipped rather than
// rejected. A malformed length still fails loudly.
func checkConnAckProps(body []byte) error {
	if len(body) < 3 {
		return fmt.Errorf("mqtt: v5 connack body %d shorter than flags plus reason plus properties", len(body))
	}

	propsLen, propsUsed, err := decodeVarint(body[2:])
	if err != nil {
		return err
	}

	if 2+propsUsed+int(propsLen) != len(body) {
		return fmt.Errorf("mqtt: v5 connack properties %d mismatch body %d", propsLen, len(body))
	}

	return nil
}

// EncodePublish renders one MQTT PUBLISH packet with user properties.
func EncodePublish(version Version, topic string, packetID uint16, qos byte, retained bool, props Properties, payload []byte) ([]byte, error) {
	return EncodePublishDup(version, topic, packetID, qos, retained, false, props, payload)
}

// checkPublishArgs validates the PUBLISH envelope arguments.
func checkPublishArgs(version Version, topic string, packetID uint16, qos byte, dup bool) error {
	if qos > maxQoS {
		return fmt.Errorf("mqtt: publish qos %d above supported %d", qos, maxQoS)
	}

	switch version {
	case Version5, Version311:
	default:
		return fmt.Errorf("mqtt: publish version %d unknown", int(version))
	}

	if topic == "" {
		return errors.New("mqtt: publish carries empty topic") //nolint:wrapcheck // new error, nothing to wrap
	}

	if (qos == 1 || qos == 2) && packetID == 0 {
		return errors.New("mqtt: publish carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	if dup && qos == 0 {
		return errors.New("mqtt: publish dup with qos 0") //nolint:wrapcheck // new error, nothing to wrap
	}

	return nil
}

// EncodePublishDup renders one MQTT PUBLISH packet with an explicit DUP flag.
//
// Version311 omits the property section; the hash then rides nowhere
// and takes report it empty. QoS 2 carries a packet id like QoS 1 and
// runs the exactly-once handshake; QoS 0 carries none. Rejected up
// front: unknown versions, QoS above 2, empty topics, zero packet ids
// on QoS 1 and 2, DUP on QoS 0, and extra user properties on 3.1.1
// (which has nowhere to carry them).
func EncodePublishDup(version Version, topic string, packetID uint16, qos byte, retained bool, dup bool, props Properties, payload []byte) ([]byte, error) {
	if err := checkPublishArgs(version, topic, packetID, qos, dup); err != nil {
		return nil, err
	}

	if version == Version311 && len(props.Extra) > 0 {
		return nil, fmt.Errorf("mqtt: publish carries %d properties on 3.1.1 without properties", len(props.Extra))
	}

	name, err := encodeString(topic)
	if err != nil {
		return nil, err
	}

	body := make([]byte, 0, len(name)+4+len(payload))
	body = append(body, name...)

	if qos == 1 || qos == 2 {
		body = append(body, byte(packetID>>8), byte(packetID)) //nolint:gosec // G115: packet id halves always fit in one byte
	}

	if version == Version5 {
		encodedProps, err := encodeProperties(props)
		if err != nil {
			return nil, err
		}

		body = append(body, encodedProps...)
	}

	body = append(body, payload...)

	var flags byte
	if retained {
		flags |= 0x01
	}

	if dup {
		flags |= 0x08
	}

	flags |= qos << 1

	header, err := encodeFixedHeader(packetPublish, flags, len(body))
	if err != nil {
		return nil, err
	}

	return append(header, body...), nil
}

// PublishPacket is one decoded MQTT v5 PUBLISH from the wire.
type PublishPacket struct {
	// Topic is the full MQTT topic the sample arrived on.
	Topic string
	// PacketID carries the QoS 1 or QoS 2 identifier, zero for QoS 0.
	PacketID uint16
	// QoS is the sample quality of service, 0, 1, or 2.
	QoS byte
	// Retained reports the broker retained flag.
	Retained bool
	// Dup reports the redelivery flag for QoS 1 and QoS 2 republishes.
	Dup bool
	// Props carries the user properties including the type hash.
	Props Properties
	// Payload holds the envelope bytes.
	Payload []byte
}

// DecodePublish parses one raw MQTT PUBLISH packet.
//
// Version selects the framing: v5 reads the property section into
// Props, 3.1.1 has none and leaves Props empty.
func DecodePublish(version Version, packet []byte) (PublishPacket, int, error) {
	var out PublishPacket

	header, rest, err := decodePublishHead(version, packet)
	if err != nil {
		return out, 0, err
	}

	sample, consumed, err := decodePublishBody(version, header, rest)
	if err != nil {
		return out, 0, err
	}

	out.Topic = header.topic
	out.PacketID = sample.packetID
	out.QoS = header.qos
	out.Retained = header.retained
	out.Dup = header.dup
	out.Props = sample.props
	out.Payload = append([]byte(nil), sample.payload...)

	return out, consumed, nil
}

// publishHead carries one decoded PUBLISH fixed header plus topic.
type publishHead struct {
	// topic is the full MQTT topic.
	topic string
	// qos is the sample quality of service, 0, 1, or 2.
	qos byte
	// retained reports the broker retained flag.
	retained bool
	// dup reports the redelivery flag.
	dup bool
	// length is the framed body length.
	length uint32
	// used counts the fixed header bytes before the body.
	used int
}

// publishTail carries one decoded PUBLISH body.
type publishTail struct {
	// packetID carries the QoS 1 or QoS 2 identifier, zero for QoS 0.
	packetID uint16
	// props carries the user properties.
	props Properties
	// payload holds the envelope bytes.
	payload []byte
}

// decodePublishHead validates the fixed header plus topic.
func decodePublishHead(version Version, packet []byte) (publishHead, []byte, error) {
	var head publishHead

	if len(packet) < 2 {
		return head, nil, fmt.Errorf("mqtt: publish shorter than header: %d", len(packet))
	}

	if packet[0]>>4 != packetPublish {
		return head, nil, fmt.Errorf("mqtt: publish type %d is not %d", packet[0]>>4, packetPublish)
	}

	switch version {
	case Version5, Version311:
	default:
		return head, nil, fmt.Errorf("mqtt: publish version %d unknown", int(version))
	}

	flags := packet[0] & 0x0F
	head.qos = (flags >> 1) & 0x03
	head.dup = flags&0x08 != 0
	head.retained = flags&0x01 != 0

	if head.qos > maxQoS {
		return head, nil, fmt.Errorf("mqtt: publish qos %d above supported %d", head.qos, maxQoS)
	}

	if head.dup && head.qos == 0 {
		return head, nil, errors.New("mqtt: publish dup with qos 0") //nolint:wrapcheck // new error, nothing to wrap
	}

	length, used, err := decodeVarint(packet[1:])
	if err != nil {
		return head, nil, err
	}

	head.length = length
	head.used = used

	body := packet[1+used:]

	if len(body) != int(length) {
		return head, nil, fmt.Errorf("mqtt: publish length %d mismatches %d", length, len(body))
	}

	topic, consumed, err := decodeString(body)
	if err != nil {
		return head, nil, err
	}

	if topic == "" {
		return head, nil, errors.New("mqtt: publish carries empty topic") //nolint:wrapcheck // new error, nothing to wrap
	}

	head.topic = topic

	return head, body[consumed:], nil
}

// decodePublishBody validates the packet id plus properties plus payload.
func decodePublishBody(version Version, head publishHead, body []byte) (publishTail, int, error) {
	var tail publishTail

	if head.qos == 1 || head.qos == 2 {
		if len(body) < 2 {
			return tail, 0, fmt.Errorf("mqtt: publish qos %d shorter than packet id", head.qos)
		}

		tail.packetID = uint16(body[0])<<8 | uint16(body[1])

		if tail.packetID == 0 {
			return tail, 0, errors.New("mqtt: publish carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
		}

		body = body[2:]
	}

	if version == Version5 {
		props, consumed, err := decodeProperties(body)
		if err != nil {
			return tail, 0, err
		}

		tail.props = props
		body = body[consumed:]
	}

	tail.payload = body

	return tail, 1 + head.used + int(head.length), nil
}

// checkAckResult validates one ack result code.
//
// v5 PUBACK family carries an optional reason after the packet id;
// any code at or above 0x80 is a failed publish. v3 bodies are
// always bare. SUBACK carries one granted code per filter on both
// versions; 0x80 is a refused subscription. UNSUBACK carries a
// single code. AUTH carries a reason plus properties. Unknown codes
// fail loudly instead of silently masking a broker refusal.
// expectedAckFlags returns the required fixed header flags nibble.
// PUBREL alone carries 0010 per MQTT-3.6.1-1; every other ack is 0000.
func expectedAckFlags(packetType byte) byte {
	if packetType == packetPubRel {
		return 0x02
	}

	return 0x00
}

// decodeAckBody validates the fixed header plus remaining length
// and returns the ack body.
func decodeAckBody(packet []byte) ([]byte, error) {
	if len(packet) < 2 {
		return nil, errors.New("mqtt: ack shorter than header") //nolint:wrapcheck // new error, nothing to wrap
	}

	if want := expectedAckFlags(packet[0] >> 4); packet[0]&0x0F != want {
		return nil, fmt.Errorf("mqtt: ack flags %#02x must be %#02x", packet[0]&0x0F, want)
	}

	length, used, err := decodeVarint(packet[1:])
	if err != nil {
		return nil, err
	}

	body := packet[1+used:]

	if len(body) != int(length) || len(body) < 2 {
		return nil, fmt.Errorf("mqtt: ack body mismatch: have %d want %d", len(body), int(length))
	}

	return body, nil
}

// dispatchAckResult routes one ack body to the per-type checker.
func dispatchAckResult(version Version, packetType byte, body []byte) error {
	switch packetType {
	case packetPubAck:
		return checkPubAckResult(version, body)
	case packetPubRec:
		return checkPubRecResult(version, body)
	case packetPubRel:
		return checkPubRelResult(version, body)
	case packetPubComp:
		return checkPubCompResult(version, body)
	case packetSubAck:
		return checkSubAckGrants(version, body)
	case packetUnsubAck:
		return checkUnsubAckGrants(version, body)
	case packetAuth:
		return checkAuthResult(version, body)
	default:
		return fmt.Errorf("mqtt: ack type %d is not ack", packetType)
	}
}

func checkAckResult(version Version, packet []byte) error {
	body, err := decodeAckBody(packet)
	if err != nil {
		return err
	}

	return dispatchAckResult(version, packet[0]>>4, body)
}

// checkIdReasonProps validates one PUBACK family body with an id
// plus an optional reason plus an optional property section.
// opName names the operation in errors; packetType gates properties.
// The packet id is nonzero: live callers check it in decodeAckID
// first, and this repeats the check so the validator is safe alone.
func checkIdReasonProps(version Version, body []byte, opName string, packetType byte) error {
	if len(body) < 2 {
		return fmt.Errorf("mqtt: %s body shorter than packet id: %d", opName, len(body))
	}

	if body[0] == 0 && body[1] == 0 {
		return errors.New("mqtt: ack carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	rest := body[2:]

	if len(rest) == 0 {
		return nil
	}

	if version == Version311 {
		return fmt.Errorf("mqtt: v3 %s carries %d extra bytes", opName, len(rest))
	}

	if rest[0] >= 0x80 {
		return fmt.Errorf("mqtt: publish refused with reason %#02x", rest[0])
	}

	if len(rest) == 1 {
		return nil
	}

	propsLen, propsUsed, err := decodeVarint(rest[1:])
	if err != nil {
		return err
	}

	if 1+propsUsed+int(propsLen) != len(rest) {
		return fmt.Errorf("mqtt: %s properties %d mismatch body %d", opName, propsLen, len(rest))
	}

	if propsLen > 0 {
		props := rest[1+propsUsed : 1+propsUsed+int(propsLen)]
		err = checkPropsAllowed(packetType, props)
		if err != nil {
			return err
		}
	}

	return nil
}

// checkPubAckResult validates one PUBACK reason code.
func checkPubAckResult(version Version, body []byte) error {
	return checkIdReasonProps(version, body, "puback", packetPubAck)
}

// checkPubRecResult validates one PUBREC reason code.
func checkPubRecResult(version Version, body []byte) error {
	return checkIdReasonProps(version, body, "pubrec", packetPubRec)
}

// checkPubRelResult validates one PUBREL reason code.
func checkPubRelResult(version Version, body []byte) error {
	return checkIdReasonProps(version, body, "pubrel", packetPubRel)
}

// checkPubCompResult validates one PUBCOMP reason code.
func checkPubCompResult(version Version, body []byte) error {
	return checkIdReasonProps(version, body, "pubcomp", packetPubComp)
}

// checkSubAckGrants validates one SUBACK granted list.
//
// The v5 property section is framing-checked and skipped: weld acts
// on nothing inside it, so unknown identifiers are skipped rather
// than rejected. Grant count is not tied to the filter count because
// weld only ever sends single-filter subscribes.
func checkSubAckGrants(version Version, body []byte) error {
	if len(body) < 2 {
		return fmt.Errorf("mqtt: suback body shorter than packet id: %d", len(body))
	}

	if body[0] == 0 && body[1] == 0 {
		return errors.New("mqtt: ack carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	rest := body[2:]

	if version == Version5 {
		propsLen, used, err := decodeVarint(rest)
		if err != nil {
			return err
		}

		if len(rest)-used < int(propsLen) {
			return errors.New("mqtt: suback properties exceed body") //nolint:wrapcheck // new error, nothing to wrap
		}

		rest = rest[used+int(propsLen):]
	}

	if len(rest) == 0 {
		return errors.New("mqtt: suback carries no granted code") //nolint:wrapcheck // new error, nothing to wrap
	}

	for _, granted := range rest {
		if granted >= 0x80 {
			return fmt.Errorf("mqtt: subscription refused by broker with reason %#02x", granted)
		}

		if granted > 0x02 {
			return fmt.Errorf("mqtt: suback granted code %#02x unknown", granted)
		}
	}

	return nil
}

// stripUnsubAckProps validates the v5 property section and
// returns the remaining grant bytes.
func stripUnsubAckProps(rest []byte) ([]byte, error) {
	propsLen, used, err := decodeVarint(rest)
	if err != nil {
		return nil, err
	}

	if len(rest)-used < int(propsLen) {
		return nil, errors.New("mqtt: unsuback properties exceed body") //nolint:wrapcheck // new error, nothing to wrap
	}

	if propsLen > 0 {
		props := rest[used : used+int(propsLen)]
		err = checkPropsAllowed(packetUnsubAck, props)
		if err != nil {
			return nil, err
		}
	}

	return rest[used+int(propsLen):], nil
}

// checkUnsubAckGrants validates one UNSUBACK single code.
func checkUnsubAckGrants(version Version, body []byte) error {
	if len(body) < 2 {
		return fmt.Errorf("mqtt: unsuback body shorter than packet id: %d", len(body))
	}

	if body[0] == 0 && body[1] == 0 {
		return errors.New("mqtt: ack carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	rest := body[2:]

	if version == Version5 {
		stripped, err := stripUnsubAckProps(rest)
		if err != nil {
			return err
		}

		rest = stripped
	}

	if len(rest) != 1 {
		return fmt.Errorf("mqtt: unsuback carries %d codes, want 1", len(rest))
	}

	if rest[0] >= 0x80 {
		return fmt.Errorf("mqtt: unsubscribe refused with reason %#02x", rest[0])
	}

	if rest[0] != 0x00 && rest[0] != 0x11 {
		return fmt.Errorf("mqtt: unsuback code %#02x unknown", rest[0])
	}

	return nil
}

// checkAuthResult validates one AUTH reason plus properties.
func checkAuthResult(version Version, body []byte) error {
	if version != Version5 {
		return fmt.Errorf("mqtt: auth version %d requires v5", int(version))
	}

	if len(body) == 0 {
		return nil
	}

	if body[0] >= 0x80 {
		return fmt.Errorf("mqtt: auth refused with reason %#02x", body[0])
	}

	if len(body) == 1 {
		return nil
	}

	propsLen, propsUsed, err := decodeVarint(body[1:])
	if err != nil {
		return err
	}

	if 1+propsUsed+int(propsLen) != len(body) {
		return fmt.Errorf("mqtt: auth properties %d mismatch body %d", propsLen, len(body))
	}

	if propsLen > 0 {
		props := body[1+propsUsed : 1+propsUsed+int(propsLen)]
		err = checkPropsAllowed(packetAuth, props)
		if err != nil {
			return err
		}
	}

	return nil
}

// EncodePubAck renders one PUBACK for a QoS 1 publish.
func EncodePubAck(packetID uint16) ([]byte, error) {
	if packetID == 0 {
		return nil, errors.New("mqtt: puback carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	header, err := encodeFixedHeader(packetPubAck, 0, 2)
	if err != nil {
		return nil, err
	}

	return append(header, byte(packetID>>8), byte(packetID)), nil //nolint:gosec // G115: packet id halves always fit in one byte
}

// EncodeSubscribe renders one MQTT SUBSCRIBE for a single filter.
//
// Version311 omits the property section. An empty filter is malformed
// on the wire (MQTT-4.7.3 requires at least one character), so it is
// rejected here instead of sending a subscription the broker must
// refuse.
func EncodeSubscribe(version Version, packetID uint16, filter string, qos byte) ([]byte, error) {
	if qos > maxQoS {
		return nil, fmt.Errorf("mqtt: subscribe qos %d above supported %d", qos, maxQoS)
	}

	if packetID == 0 {
		return nil, errors.New("mqtt: subscribe carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	if filter == "" {
		return nil, errors.New("mqtt: subscribe carries empty filter") //nolint:wrapcheck // new error, nothing to wrap
	}

	switch version {
	case Version5, Version311:
	default:
		return nil, fmt.Errorf("mqtt: subscribe version %d unknown", int(version))
	}

	name, err := encodeString(filter)
	if err != nil {
		return nil, err
	}

	body := []byte{byte(packetID >> 8), byte(packetID)} //nolint:gosec // G115: packet id halves always fit in one byte

	if version == Version5 {
		props, err := encodeVarint(0)
		if err != nil {
			return nil, err
		}

		body = append(body, props...)
	}

	// The trailing byte is the Subscription Options byte with QoS in
	// the low nibble; weld always subscribes plain (No Local 0,
	// Retain As Published 0, Retain Handling 0), so the upper bits
	// stay zero.
	body = append(body, name...)
	body = append(body, qos)

	header, err := encodeFixedHeader(packetSubscribe, 0x02, len(body))
	if err != nil {
		return nil, err
	}

	return append(header, body...), nil
}

// EncodePubRec renders one PUBREC with a 2 byte packet id.
func EncodePubRec(packetID uint16) ([]byte, error) {
	if packetID == 0 {
		return nil, errors.New("mqtt: pubrec carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	header, err := encodeFixedHeader(packetPubRec, 0, 2)
	if err != nil {
		return nil, err
	}

	return append(header, byte(packetID>>8), byte(packetID)), nil //nolint:gosec // G115: packet id halves always fit in one byte
}

// EncodePubRel renders one PUBREL with a 2 byte packet id.
//
// The fixed header flags are 0010 as MQTT-3.6.1-1 requires; the wire
// has no DUP flag, so every replay sends identical bytes.
func EncodePubRel(packetID uint16) ([]byte, error) {
	if packetID == 0 {
		return nil, errors.New("mqtt: pubrel carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	header, err := encodeFixedHeader(packetPubRel, 0x02, 2)
	if err != nil {
		return nil, err
	}

	return append(header, byte(packetID>>8), byte(packetID)), nil //nolint:gosec // G115: packet id halves always fit in one byte
}

// EncodePubComp renders one PUBCOMP with a 2 byte packet id.
func EncodePubComp(packetID uint16) ([]byte, error) {
	if packetID == 0 {
		return nil, errors.New("mqtt: pubcomp carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	header, err := encodeFixedHeader(packetPubComp, 0, 2)
	if err != nil {
		return nil, err
	}

	return append(header, byte(packetID>>8), byte(packetID)), nil //nolint:gosec // G115: packet id halves always fit in one byte
}

// EncodeUnsubscribe renders one MQTT UNSUBSCRIBE for a single filter.
//
// Version311 omits the property section. The payload carries one
// filter with no options byte, matching the specification.
func EncodeUnsubscribe(version Version, packetID uint16, filter string) ([]byte, error) {
	if packetID == 0 {
		return nil, errors.New("mqtt: unsubscribe carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	if filter == "" {
		return nil, errors.New("mqtt: unsubscribe carries empty filter") //nolint:wrapcheck // new error, nothing to wrap
	}

	switch version {
	case Version5, Version311:
	default:
		return nil, fmt.Errorf("mqtt: unsubscribe version %d unknown", int(version))
	}

	name, err := encodeString(filter)
	if err != nil {
		return nil, err
	}

	body := []byte{byte(packetID >> 8), byte(packetID)} //nolint:gosec // G115: packet id halves always fit in one byte

	if version == Version5 {
		props, err := encodeVarint(0)
		if err != nil {
			return nil, err
		}

		body = append(body, props...)
	}

	body = append(body, name...)

	header, err := encodeFixedHeader(packetUnsubscribe, 0x02, len(body))
	if err != nil {
		return nil, err
	}

	return append(header, body...), nil
}

// decodeUnsubscribeHead validates framing and returns the body.
func decodeUnsubscribeHead(version Version, packet []byte) ([]byte, error) {
	if len(packet) < 2 {
		return nil, fmt.Errorf("mqtt: unsubscribe shorter than header: %d", len(packet))
	}

	if packet[0]>>4 != packetUnsubscribe {
		return nil, fmt.Errorf("mqtt: unsubscribe type %d is not %d", packet[0]>>4, packetUnsubscribe)
	}

	if packet[0]&0x0F != 0x02 {
		return nil, fmt.Errorf("mqtt: unsubscribe flags %#02x must be 0x02", packet[0]&0x0F)
	}

	switch version {
	case Version5, Version311:
	default:
		return nil, fmt.Errorf("mqtt: unsubscribe version %d unknown", int(version))
	}

	length, used, err := decodeVarint(packet[1:])
	if err != nil {
		return nil, err
	}

	body := packet[1+used:]

	if len(body) != int(length) || len(body) < 2 {
		return nil, fmt.Errorf("mqtt: unsubscribe body mismatch: have %d want %d", len(body), int(length))
	}

	return body, nil
}

// splitUnsubscribeID splits the packet id from the body.
func splitUnsubscribeID(body []byte) (uint16, []byte, error) {
	id := uint16(body[0])<<8 | uint16(body[1])

	if id == 0 {
		return 0, nil, errors.New("mqtt: unsubscribe carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	return id, body[2:], nil
}

// stripUnsubscribeProps validates the v5 property section and
// returns the remaining filter bytes.
func stripUnsubscribeProps(rest []byte) ([]byte, error) {
	propsLen, propsUsed, err := decodeVarint(rest)
	if err != nil {
		return nil, err
	}

	if len(rest)-propsUsed < int(propsLen) {
		return nil, errors.New("mqtt: unsubscribe properties exceed body") //nolint:wrapcheck // new error, nothing to wrap
	}

	if propsLen > 0 {
		props := rest[propsUsed : propsUsed+int(propsLen)]
		err = checkPropsAllowed(packetUnsubscribe, props)
		if err != nil {
			return nil, err
		}
	}

	return rest[propsUsed+int(propsLen):], nil
}

// checkUnsubscribeFilter decodes the filter and rejects trailing bytes.
func checkUnsubscribeFilter(rest []byte) (string, error) {
	filter, consumed, err := decodeString(rest)
	if err != nil {
		return "", err
	}

	if filter == "" {
		return "", errors.New("mqtt: unsubscribe carries empty filter") //nolint:wrapcheck // new error, nothing to wrap
	}

	if consumed != len(rest) {
		return "", fmt.Errorf("mqtt: unsubscribe carries %d trailing bytes", len(rest)-consumed)
	}

	return filter, nil
}

// DecodeUnsubscribe parses one UNSUBSCRIBE and reports id plus filter.
//
// Version selects the framing: v5 reads the property section,
// 3.1.1 has none. Fixed header flags must be 0x02.
func DecodeUnsubscribe(version Version, packet []byte) (uint16, string, error) {
	body, err := decodeUnsubscribeHead(version, packet)
	if err != nil {
		return 0, "", err
	}

	id, rest, err := splitUnsubscribeID(body)
	if err != nil {
		return 0, "", err
	}

	if version == Version5 {
		stripped, err := stripUnsubscribeProps(rest)
		if err != nil {
			return 0, "", err
		}

		rest = stripped
	}

	filter, err := checkUnsubscribeFilter(rest)
	if err != nil {
		return 0, "", err
	}

	return id, filter, nil
}

// EncodeUnsubAck renders one UNSUBACK with a single result code.
//
// Version311 omits the property section.
func EncodeUnsubAck(version Version, packetID uint16, reason byte) ([]byte, error) {
	if packetID == 0 {
		return nil, errors.New("mqtt: unsuback carries zero packet id") //nolint:wrapcheck // new error, nothing to wrap
	}

	switch version {
	case Version5, Version311:
	default:
		return nil, fmt.Errorf("mqtt: unsuback version %d unknown", int(version))
	}

	body := []byte{byte(packetID >> 8), byte(packetID)} //nolint:gosec // G115: packet id halves always fit in one byte

	if version == Version5 {
		props, err := encodeVarint(0)
		if err != nil {
			return nil, err
		}

		body = append(body, props...)
	}

	body = append(body, reason)

	header, err := encodeFixedHeader(packetUnsubAck, 0, len(body))
	if err != nil {
		return nil, err
	}

	return append(header, body...), nil
}

// EncodeAuth renders one AUTH with reason plus properties length.
//
// AUTH exists only on v5; Version311 fails loudly.
func EncodeAuth(version Version, reason byte) ([]byte, error) {
	if version != Version5 {
		return nil, fmt.Errorf("mqtt: auth version %d requires v5", int(version))
	}

	body := []byte{reason}

	props, err := encodeVarint(0)
	if err != nil {
		return nil, err
	}

	body = append(body, props...)

	header, err := encodeFixedHeader(packetAuth, 0, len(body))
	if err != nil {
		return nil, err
	}

	return append(header, body...), nil
}

// DecodeAuth parses one AUTH and reports the reason code.
//
// AUTH exists only on v5; Version311 fails loudly. Fixed header
// flags must be zero.
func DecodeAuth(version Version, packet []byte) (byte, error) {
	if len(packet) < 2 {
		return 0, fmt.Errorf("mqtt: auth shorter than header: %d", len(packet))
	}

	if packet[0]>>4 != packetAuth {
		return 0, fmt.Errorf("mqtt: auth type %d is not %d", packet[0]>>4, packetAuth)
	}

	if packet[0]&0x0F != 0 {
		return 0, fmt.Errorf("mqtt: auth flags %#02x must be zero", packet[0]&0x0F)
	}

	if version != Version5 {
		return 0, fmt.Errorf("mqtt: auth version %d requires v5", int(version))
	}

	length, used, err := decodeVarint(packet[1:])
	if err != nil {
		return 0, err
	}

	body := packet[1+used:]

	if len(body) != int(length) {
		return 0, fmt.Errorf("mqtt: auth length %d mismatches %d", length, len(body))
	}

	if len(body) == 0 {
		return 0, nil
	}

	err = checkAuthResult(version, body)
	if err != nil {
		return 0, err
	}

	return body[0], nil
}

// EncodePingReq renders one PINGREQ packet.
func EncodePingReq() []byte {
	return []byte{packetPingReq << 4, 0x00}
}

// EncodeDisconnect renders one DISCONNECT.
//
// Version311 emits the 2 byte form; v5 carries a zero reason. Unknown
// versions emit the v5 form: the version comes from validated config
// (Validate rejects anything else before dial), so this stays infallible
// for the goodbye path, which must never fail Close.
func EncodeDisconnect(version Version) []byte {
	if version == Version311 {
		return []byte{packetDisconnect << 4, 0x00}
	}

	return []byte{packetDisconnect << 4, 0x01, 0x00}
}
