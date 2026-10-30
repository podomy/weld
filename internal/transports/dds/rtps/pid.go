// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package rtps

import (
	"errors"
	"fmt"
	"net"
)

// ErrInvalidParam reports a malformed PID parameter list.
var ErrInvalidParam = errors.New("rtps: invalid param")

// DDSI-RTPS parameter ids for discovery and endpoint QoS.
//
// Values match the DDSI-RTPS spec and the CycloneDDS q_protocol.h and
// FastDDS ParameterTypes.hpp headers. Two aliases below are intentional:
// PIDDefaultUnicastLocator follows the discovery brief mapping to 0x002f
// which is PIDUnicastLocator on the wire, and PIDEndpointGUID follows the
// brief mapping to 0x005b while Cyclone and FastDDS send 0x005a on the
// wire (see PIDEndpointGUIDWire). Decode is tolerant and keeps both.
//
// PAD entries carry no meaning and are skipped on decode so alignment
// fill never surfaces as a param. Unknown ids, including vendor specific
// 0x8003, 0x8007, 0x8019, and 0x800c, are preserved on decode and survive
// an encode round trip so new keys never break old peers. XTypes
// TypeObject (0x0072) is parsed as opaque bytes only; full TypeObject
// lookup with dependency resolution is out of scope for v1.
const (
	// PIDPad is alignment with no meaning. Decoders skip it.
	PIDPad uint16 = 0x0000
	// PIDSentinel terminates a parameter list with zero length.
	PIDSentinel uint16 = 0x0001
	// PIDParticipantLeaseDuration carries the 8 byte lease duration.
	PIDParticipantLeaseDuration uint16 = 0x0002
	// PIDTopicName carries the null terminated topic name.
	PIDTopicName uint16 = 0x0005
	// PIDTypeName carries the null terminated type name.
	PIDTypeName uint16 = 0x0007
	// PIDProtocolVersion carries the 2 byte RTPS version.
	PIDProtocolVersion uint16 = 0x0015
	// PIDVendorID carries the 2 byte vendor id.
	PIDVendorID uint16 = 0x0016
	// PIDReliability carries the reliability QoS.
	PIDReliability uint16 = 0x001a
	// PIDDurability carries the durability QoS.
	PIDDurability uint16 = 0x001d
	// PIDParticipantManualLivelinessCount carries the 4 byte manual liveliness count.
	PIDParticipantManualLivelinessCount uint16 = 0x0034
	// PIDHistory carries the history QoS kind plus depth.
	PIDHistory uint16 = 0x0040
	// PIDUnicastLocator carries one 24 byte locator.
	PIDUnicastLocator uint16 = 0x002f
	// PIDDefaultUnicastLocator is the discovery brief name for 0x002f.
	// It aliases PIDUnicastLocator on the wire for participant discovery.
	PIDDefaultUnicastLocator uint16 = 0x002f
	// PIDMulticastLocator carries one 24 byte multicast locator.
	PIDMulticastLocator uint16 = 0x0030
	// PIDDefaultUnicastLocatorStandard is the spec 0x0031 value.
	PIDDefaultUnicastLocatorStandard uint16 = 0x0031
	// PIDMetatrafficUnicastLocator carries one discovery unicast locator.
	PIDMetatrafficUnicastLocator uint16 = 0x0032
	// PIDMetatrafficMulticastLocator carries one discovery multicast locator.
	PIDMetatrafficMulticastLocator uint16 = 0x0033
	// PIDDefaultMulticastLocator carries the default multicast locator.
	PIDDefaultMulticastLocator uint16 = 0x0048
	// PIDParticipantGUID carries the 16 byte participant GUID.
	PIDParticipantGUID uint16 = 0x0050
	// PIDBuiltinEndpointSet carries the 4 byte builtin endpoint mask.
	PIDBuiltinEndpointSet uint16 = 0x0058
	// PIDEndpointGUID carries the endpoint GUID per the discovery brief.
	PIDEndpointGUID uint16 = 0x005b
	// PIDEndpointGUIDWire is the 0x005a value sent by CycloneDDS and
	// FastDDS for the endpoint GUID. Decoders must accept both.
	PIDEndpointGUIDWire uint16 = 0x005a
	// PIDUserData carries the CDR octet sequence userdata blob.
	PIDUserData uint16 = 0x002c
	// PIDTypeObject carries the XTypes TypeObject bytes. V1 parses and
	// stores the bytes opaquely; full lookup is out of scope.
	PIDTypeObject uint16 = 0x0072
	// PIDTypeInformation carries the XTypes TypeInformation bytes.
	PIDTypeInformation uint16 = 0x0075
	// PIDBuiltinEndpointQos carries the 4 byte builtin endpoint QoS mask.
	PIDBuiltinEndpointQos uint16 = 0x0077
	// PIDVendorAdlinkWriterDataLifecycle is vendor 0x8003.
	PIDVendorAdlinkWriterDataLifecycle uint16 = 0x8003
	// PIDVendorAdlinkParticipantVersionInfo is vendor 0x8007.
	PIDVendorAdlinkParticipantVersionInfo uint16 = 0x8007
	// PIDVendorAdlinkEntityFactory is vendor 0x800c.
	PIDVendorAdlinkEntityFactory uint16 = 0x800c
	// PIDVendorCycloneReceiveBufferSize is vendor 0x8019.
	PIDVendorCycloneReceiveBufferSize uint16 = 0x8019
)

// Locator kinds for the 24 byte locator address.
const (
	// LocatorKindInvalid marks an unusable locator.
	LocatorKindInvalid int32 = -1
	// LocatorKindUDPv4 selects IPv4 UDP transport.
	LocatorKindUDPv4 int32 = 1
	// LocatorKindUDPv6 selects IPv6 UDP transport.
	LocatorKindUDPv6 int32 = 2
)

// Locator wire sizes and offsets.
const (
	// locatorSize is kind plus port plus 16 byte address.
	locatorSize = 24
	// ipv4TailSize is the 4 byte IPv4 tail inside the address.
	ipv4TailSize = 4
	// locatorKindOff is the kind offset in a locator.
	locatorKindOff = 0
	// locatorPortOff is the port offset in a locator.
	locatorPortOff = 4
	// locatorAddrSize is the 16 byte address size in a locator.
	locatorAddrSize = 16
	// locatorAddrOff is the address offset in a locator.
	locatorAddrOff = 8
)

// Param is one RTPS PID parameter: id plus opaque value bytes without
// padding. Unknown ids are preserved on decode so new keys never break
// old peers.
type Param struct {
	// ID is the parameter key, for example a PID value.
	ID uint16
	// Value holds the parameter bytes without padding.
	Value []byte
}

// EncodeParams renders params in little endian with a terminating
// sentinel. Each entry is id plus length plus value plus zero pad to a
// 4 byte boundary. Values longer than the uint16 range are truncated to
// fit the length; callers should keep values small.
func EncodeParams(params []Param) []byte {
	const littleEndian = true
	out := make([]byte, 0, len(params)*8+4)
	for i := range params {
		value := params[i].Value
		if len(value) > maxSubmessageBody {
			value = value[:maxSubmessageBody]
		}
		out = appendParamEntry(out, params[i].ID, value, littleEndian)
	}
	return appendParamSentinel(out, PIDSentinel, littleEndian)
}

// DecodeParams parses a little endian PID list requiring sentinel
// termination. Truncated headers, truncated values, truncated pad,
// nonzero sentinel length, missing sentinel, and trailing bytes after
// the sentinel all fail. Unknown ids are kept so callers can skip what
// they do not understand, including vendor 0x8003, 0x8007, 0x800c, and
// 0x8019. PIDPad entries are skipped. The input buffer is never mutated and the
// returned values are copies. Decode never panics.
func DecodeParams(b []byte) ([]Param, error) {
	const littleEndian = true
	out, consumed, err := DecodeParamsEndian(b, littleEndian)
	if err != nil {
		return nil, err
	}
	if consumed != len(b) {
		return nil, fmt.Errorf("rtps: trailing %d bytes after sentinel: %w", len(b)-consumed, ErrInvalidParam)
	}
	return out, nil
}

// EncodeIPv4Locator renders a 24 byte locator: int32 kind little endian
// where 1 selects UDPv4, uint32 port little endian, then 16 bytes with
// 12 zero plus 4 IPv4 bytes in network order. A nil or non IPv4 address
// encodes as 4 zero bytes; the kind and port are still stored.
func EncodeIPv4Locator(kind int32, port uint32, ip net.IP) []byte {
	const littleEndian = true
	out := make([]byte, locatorSize)
	putUint32(out[locatorKindOff:locatorKindOff+wordSize], uint32(kind), littleEndian) //nolint:gosec // G115: intentional two's complement reinterpret for the wire
	putUint32(out[locatorPortOff:locatorPortOff+wordSize], port, littleEndian)
	v4 := ip.To4()
	if v4 == nil {
		return out
	}
	copy(out[locatorAddrOff+locatorAddrSize-ipv4TailSize:], v4)
	return out
}

// DecodeIPv4Locator parses the inverse of EncodeIPv4Locator: 24 bytes
// into kind, port, and IPv4 address. The first 12 address bytes are
// ignored for tolerance; the last 4 bytes are the IPv4 address in
// network order. Only the length is validated.
func DecodeIPv4Locator(b []byte) (int32, uint32, net.IP, error) {
	if len(b) != locatorSize {
		return 0, 0, nil, fmt.Errorf("rtps: locator need %d have %d: %w", locatorSize, len(b), ErrInvalidParam)
	}
	const littleEndian = true
	kind := int32(uint32Value(b[locatorKindOff:locatorKindOff+wordSize], littleEndian)) //nolint:gosec // G115: intentional two's complement reinterpret from the wire
	port := uint32Value(b[locatorPortOff:locatorPortOff+wordSize], littleEndian)
	addr := make(net.IP, ipv4TailSize)
	copy(addr, b[locatorAddrOff+locatorAddrSize-ipv4TailSize:])
	return kind, port, addr, nil
}

// DecodeLocator parses any 24 byte locator into kind, port, and IP.
// It accepts the EncodeIPv4Locator layout and ignores the first 12
// address bytes for tolerance with future address families.
func DecodeLocator(b []byte) (int32, uint32, net.IP, error) {
	kind, port, ip, err := DecodeIPv4Locator(b)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("rtps: decode locator: %w", err)
	}
	return kind, port, ip, nil
}

// copyPIDValue copies one PID value plus its alignment pad.
func copyPIDValue(body []byte, off, length int, id uint16) ([]byte, int, error) {
	value, next, err := copyParamBytes(body, off, length)
	if err != nil {
		return nil, 0, fmt.Errorf("rtps: param %#04x: %w: %w", id, err, ErrInvalidParam)
	}
	return value, next, nil
}

// decodePIDSentinel validates the terminating sentinel for endian lists.
func decodePIDSentinel(length int) error {
	err := checkSentinelLength(length)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidParam, err)
	}
	return nil
}

// DecodeParamsEndian decodes a sentinel terminated parameter list in the
// given byte order and returns the params plus consumed bytes. Trailing
// bytes after the sentinel are left for the caller, which suits payloads
// that follow the list. Unknown ids are preserved, including vendor
// 0x8003, 0x8007, 0x800c, and 0x8019. PIDPad entries are skipped so
// alignment fill never surfaces. Lengths that include
// trailing pad are tolerated because senders vary: the value is read and
// the offset advances to the next 4 byte boundary either way.
func DecodeParamsEndian(b []byte, littleEndian bool) ([]Param, int, error) {
	var out []Param
	off := 0
	for {
		id, length, ok := paramHeaderAt(b, off, littleEndian)
		if !ok {
			return nil, 0, fmt.Errorf("rtps: param header need %d have %d: %w", parameterHeaderSize, len(b)-off, ErrInvalidParam)
		}
		off += parameterHeaderSize
		if id == PIDSentinel {
			err := decodePIDSentinel(length)
			if err != nil {
				return nil, 0, err
			}
			return out, off, nil
		}
		if id == PIDPad {
			value, next, err := copyPIDValue(b, off, length, id)
			if err != nil {
				return nil, 0, err
			}
			_ = value
			off = next
			continue
		}
		value, next, err := copyPIDValue(b, off, length, id)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, Param{ID: id, Value: value})
		off = next
	}
}
