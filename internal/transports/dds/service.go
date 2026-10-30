// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/types"
)

// Service wire mapping for DDS request plus reply topics.
//
// Scope is translator only. No core logic lives here: matching, QoS,
// routing, timeouts, retries, and state machines stay in core. This file
// maps ROS service names to DDS topics and types, encodes service packets
// as header plus data with GUID plus sequence, splits response writer IDs,
// waits for matched readers, filters replies on the client handle, and
// checks server availability from graph counts plus userdata service IDs.
//
// Topic naming follows the sidecar rules. Normal names use rq plus rr
// prefixes with Request plus Reply suffixes: rq/<name>Request plus
// rr/<name>Reply. Bare names drop the prefix when avoid-namespace
// conventions is set. Type names use Request_ plus Response_ suffixes:
// <ns>::dds_::<Name>_Request_ plus <ns>::dds_::<Name>_Response_. Topics use
// Reply while types use Response.
//
// Packet shape is 4 byte XCDR1 header plus 8 byte GUID plus 8 byte
// sequence plus XCDR1 user fields. The 8 byte service GUID is the opaque
// rmw request id, copied verbatim and never swapped. It is distinct from
// the 16 byte DDS participant GUID. The 16 byte header variant sertype
// carries the 8 plus 8 service header outside the CDR alignment domain.
// Empty payloads stay empty: ROS sends no dummy byte for empty service
// structs.
//
// Roles are fixed. The server subscribes to the request topic and
// publishes to the reply topic. The client publishes to the request topic
// and subscribes to the reply topic. Availability uses graph counts for
// request readers plus reply writers, then matches on userdata service ID
// with fallback to non empty sets.

// Service wire constants.
const (
	// ServiceMatchedReaderWait bounds one response reader wait.
	ServiceMatchedReaderWait = 100 * time.Millisecond
	// ServiceSertypeHeaderSize is the 16 byte service header width.
	ServiceSertypeHeaderSize = types.ServiceHeaderSize
	// ServiceGUIDSize is the 8 byte service GUID width.
	ServiceGUIDSize = types.ServiceGUIDSize
	// ServiceSequenceSize is the 8 byte service sequence width.
	ServiceSequenceSize = types.ServiceSequenceSize
	// serviceIDHexLen is the hex length of one 8 byte service ID.
	serviceIDHexLen = 16
	// serviceUserdataKey carries the hex service ID in USER_DATA.
	serviceUserdataKey = "serviceid"
	// clientUserdataKey carries the hex client ID in USER_DATA.
	clientUserdataKey = "clientid"
)

// Sentinel errors for explicit service failures.
var (
	// ErrServiceInvalidName reports a bad service or topic name.
	ErrServiceInvalidName = errors.New("dds: invalid service name")
	// ErrServiceInvalidType reports a bad service type name.
	ErrServiceInvalidType = errors.New("dds: invalid service type")
	// ErrServiceInvalidPacket reports a malformed service packet.
	ErrServiceInvalidPacket = errors.New("dds: invalid service packet")
	// ErrServiceNoReader reports no matched response reader after the wait.
	ErrServiceNoReader = errors.New("dds: no matched response reader")
	// ErrServiceNotAvailable reports no server for the service.
	ErrServiceNotAvailable = errors.New("dds: service not available")
	// ErrServiceHandleMismatch reports a reply for another client handle.
	ErrServiceHandleMismatch = errors.New("dds: service handle mismatch")
)

// ServiceRole selects server or client topic direction.
type ServiceRole int

const (
	// ServiceRoleServer subscribes to requests and publishes replies.
	ServiceRoleServer ServiceRole = iota
	// ServiceRoleClient publishes requests and subscribes to replies.
	ServiceRoleClient
)

// String returns the role name for logs.
func (r ServiceRole) String() string {
	switch r {
	case ServiceRoleServer:
		return "server"
	case ServiceRoleClient:
		return "client"
	default:
		return fmt.Sprintf("ServiceRole(%d)", int(r))
	}
}

// ServiceEndpoints holds the mapped DDS topics and types for one service.
//
// RequestTopic is rq/<name>Request and ReplyTopic is rr/<name>Reply,
// or bare names when AvoidConventions is set. RequestType carries the
// Request_ suffix and ResponseType carries the Response_ suffix.
type ServiceEndpoints struct {
	// ServiceName is the ROS service name such as /add_two_ints.
	ServiceName string
	// RequestTopic is the DDS request topic.
	RequestTopic string
	// ReplyTopic is the DDS reply topic.
	ReplyTopic string
	// RequestType is the DDS request type name.
	RequestType string
	// ResponseType is the DDS response type name.
	ResponseType string
	// AvoidConventions is true when bare names are used.
	AvoidConventions bool
}

// NewServiceEndpoints maps one service name plus IDL base to DDS names.
//
// serviceName is the ROS name such as /add_two_ints. idlBase is the IDL
// base without suffix such as my_pkg__srv__AddTwoInts. avoid selects bare
// names with no rq or rr prefix. It returns ErrServiceInvalidName for bad
// service names and ErrServiceInvalidType for bad IDL names.
func NewServiceEndpoints(serviceName, idlBase string, avoid bool) (ServiceEndpoints, error) {
	reqTopic, err := RequestTopic(serviceName, avoid)
	if err != nil {
		return ServiceEndpoints{}, fmt.Errorf("dds: service request topic %q: %w", serviceName, err)
	}

	repTopic, err := ReplyTopic(serviceName, avoid)
	if err != nil {
		return ServiceEndpoints{}, fmt.Errorf("dds: service reply topic %q: %w", serviceName, err)
	}

	reqType, err := ServiceRequestTypeName(idlBase)
	if err != nil {
		return ServiceEndpoints{}, fmt.Errorf("dds: service request type %q: %w", idlBase, err)
	}

	rspType, err := ServiceResponseTypeName(idlBase)
	if err != nil {
		return ServiceEndpoints{}, fmt.Errorf("dds: service response type %q: %w", idlBase, err)
	}

	return ServiceEndpoints{
		ServiceName:      serviceName,
		RequestTopic:     reqTopic,
		ReplyTopic:       repTopic,
		RequestType:      reqType,
		ResponseType:     rspType,
		AvoidConventions: avoid,
	}, nil
}

// SubscribeTopic returns the DDS topic this role subscribes to.
func (e ServiceEndpoints) SubscribeTopic(role ServiceRole) string {
	if role == ServiceRoleServer {
		return e.RequestTopic
	}

	return e.ReplyTopic
}

// PublishTopic returns the DDS topic this role publishes to.
func (e ServiceEndpoints) PublishTopic(role ServiceRole) string {
	if role == ServiceRoleServer {
		return e.ReplyTopic
	}

	return e.RequestTopic
}

// SubscribeType returns the DDS type this role subscribes to.
func (e ServiceEndpoints) SubscribeType(role ServiceRole) string {
	if role == ServiceRoleServer {
		return e.RequestType
	}

	return e.ResponseType
}

// PublishType returns the DDS type this role publishes.
func (e ServiceEndpoints) PublishType(role ServiceRole) string {
	if role == ServiceRoleServer {
		return e.ResponseType
	}

	return e.RequestType
}

// MarshalServiceRequest wraps payload with header plus GUID plus sequence.
//
// Layout is 4 byte CDR header plus 8 byte GUID plus 8 byte sequence in
// the negotiated order plus XCDR1 user fields. Empty payload stays empty.
// Payloads above the DDS sample cap report ErrServiceInvalidPacket.
func MarshalServiceRequest(payload []byte, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	if seq == 0 {
		return nil, fmt.Errorf("dds: service request sequence 0: %w", ErrServiceInvalidPacket)
	}

	if len(payload) > maxPayloadLen {
		return nil, fmt.Errorf(
			"dds: service request payload %d above cap %d: %w",
			len(payload),
			maxPayloadLen,
			ErrServiceInvalidPacket,
		)
	}

	out := types.MarshalServiceMessage(payload, guid, seq, littleEndian)

	return out, nil
}

// MarshalServiceReply wraps a reply payload the same way as a request.
//
// Requests and replies share one packet shape. The separate name keeps
// server reply paths distinct from client request paths in review.
func MarshalServiceReply(payload []byte, guid [8]byte, seq uint64, littleEndian bool) ([]byte, error) {
	out, err := MarshalServiceRequest(payload, guid, seq, littleEndian)
	if err != nil {
		return nil, fmt.Errorf("dds: service reply marshal: %w", err)
	}

	return out, nil
}

// UnmarshalServiceRequest validates one request packet and splits the parts.
//
// It checks the 4 byte header plus 20 byte prefix length before parsing.
// The returned payload aliases the input after the prefix, including pad
// tail. Short, empty, and bad header input reports ErrServiceInvalidPacket
// or ErrTruncated. The GUID is opaque and never swapped.
func UnmarshalServiceRequest(data []byte, m *types.Metrics) ([]byte, [8]byte, uint64, bool, error) {
	var guid [8]byte

	payload, gotGUID, seq, le, err := types.UnmarshalServiceMessage(data, m)
	if err != nil {
		if errors.Is(err, ErrTruncated) {
			return nil, guid, 0, false, fmt.Errorf("dds: service request parse: %w", err)
		}

		return nil, guid, 0, false, fmt.Errorf("dds: service request parse: %w", errors.Join(err, ErrServiceInvalidPacket))
	}

	return payload, gotGUID, seq, le, nil
}

// UnmarshalServiceReply validates one reply packet and splits the parts.
//
// It shares validation with UnmarshalServiceRequest. The separate name
// keeps client reply paths distinct from server request paths in review.
func UnmarshalServiceReply(data []byte, m *types.Metrics) ([]byte, [8]byte, uint64, bool, error) {
	payload, guid, seq, le, err := UnmarshalServiceRequest(data, m)
	if err != nil {
		return nil, guid, 0, false, fmt.Errorf("dds: service reply parse: %w", err)
	}

	return payload, guid, seq, le, nil
}

// EncodeServiceIDHex renders one 8 byte service ID as 16 lowercase hex digits.
func EncodeServiceIDHex(id [8]byte) string {
	return hex.EncodeToString(id[:])
}

// ParseServiceIDHex parses 16 hex digits back to 8 bytes.
func ParseServiceIDHex(s string) ([8]byte, error) {
	var out [8]byte

	trimmed := strings.TrimSpace(s)
	if len(trimmed) != serviceIDHexLen {
		return out, fmt.Errorf("dds: service id %q length %d want %d: %w", s, len(trimmed), serviceIDHexLen, ErrServiceInvalidName)
	}

	raw, err := hex.DecodeString(trimmed)
	if err != nil {
		return out, fmt.Errorf("dds: service id %q: %w", s, errors.Join(err, ErrServiceInvalidName))
	}

	copy(out[:], raw)

	return out, nil
}

// parseServiceIDROS validates a dot-separated ROS service ID.
//
// ROS formats 16 bytes as dot-separated lowercase hex pairs such as
// 1.10.9a.9c... The dots are stripped and the 32 hex digits must decode
// to 16 bytes. Plain 16 digit hex (the legacy 8 byte form) is rejected
// here; use ParseServiceIDHex for that form.
func parseServiceIDROS(s string) error {
	trimmed := strings.TrimSpace(s)
	stripped := strings.ReplaceAll(trimmed, ".", "")
	if len(stripped) != 32 {
		return fmt.Errorf("dds: service id %q hex length %d want 32: %w", s, len(stripped), ErrServiceInvalidName)
	}
	_, err := hex.DecodeString(stripped)
	if err != nil {
		return fmt.Errorf("dds: service id %q: %w", s, errors.Join(err, ErrServiceInvalidName))
	}
	return nil
}

// validateServiceID checks one trimmed service ID in either wire form.
func validateServiceID(trimmed string) error {
	if strings.Contains(trimmed, ".") {
		if err := parseServiceIDROS(trimmed); err != nil {
			return fmt.Errorf("dds: service userdata id: %w", err)
		}
		return nil
	}
	if _, err := ParseServiceIDHex(trimmed); err != nil {
		return fmt.Errorf("dds: service userdata id: %w", err)
	}
	return nil
}

// normalizeServiceID trims one service ID and validates its wire form.
func normalizeServiceID(serviceIDHex string) (string, error) {
	if serviceIDHex == "" {
		return "", nil
	}
	trimmed := strings.TrimSpace(serviceIDHex)
	if strings.ContainsAny(trimmed, ";=\n\r") {
		return "", fmt.Errorf("dds: service userdata id %q has separator: %w", serviceIDHex, ErrServiceInvalidName)
	}
	if err := validateServiceID(trimmed); err != nil {
		return "", err
	}
	return trimmed, nil
}

// EncodeServiceUserdata builds USER_DATA content carrying the service ID.
//
// Layout is serviceid plus typehash plus DDS topic plus domain in fixed
// order, split on semicolons when parsing. Values must not carry
// separators. An empty service ID omits the serviceid pair so pre service
// peers keep the old three field form.
func EncodeServiceUserdata(serviceIDHex, typeHash, ddsTopic string, domain uint32) (string, error) {
	err := sedpCheckWireValues(ddsTopic, typeHash)
	if err != nil {
		return "", fmt.Errorf("dds: service userdata values: %w", err)
	}

	if domain > sedpMaxDomainID {
		return "", fmt.Errorf("dds: service userdata domain %d above maximum %d: %w", domain, sedpMaxDomainID, ErrServiceInvalidName)
	}

	serviceIDHex, err = normalizeServiceID(serviceIDHex)
	if err != nil {
		return "", err
	}

	base, err := sedpEncodeEndpointUserdata(typeHash, ddsTopic, domain)
	if err != nil {
		return "", fmt.Errorf("dds: service userdata base: %w", err)
	}

	if serviceIDHex == "" {
		return base, nil
	}

	return serviceUserdataKey + "=" + serviceIDHex + ";" + base, nil
}

// ServiceUserdata holds parsed USER_DATA pairs including the service ID.
type ServiceUserdata struct {
	// ServiceID is the hex service ID, empty when absent.
	ServiceID string
	// HasServiceID is true when the serviceid pair was present.
	HasServiceID bool
	// TypeHash is the type hash passthrough.
	TypeHash string
	// DDSTopic echoes the DDS topic when present.
	DDSTopic string
	// HasDDSTopic is true when the DDS topic echo was present.
	HasDDSTopic bool
	// Domain is the Weld domain ID when present.
	Domain uint32
	// HasDomain is true when the domain was present.
	HasDomain bool
}

// ParseServiceUserdata parses USER_DATA pairs and tolerates unknown keys.
//
// It accepts any order, empty input, and trailing semicolons. Missing
// equals reports ErrServiceInvalidPacket. Unknown keys are ignored so
// older and newer peers interoperate. Present service IDs must be
// dot-format ROS IDs or 16 hex digits after trimming spaces.
func ParseServiceUserdata(raw string) (ServiceUserdata, error) {
	var out ServiceUserdata

	for chunk := range strings.SplitSeq(raw, ";") {
		if err := applyServiceUserdataPair(&out, chunk); err != nil {
			return out, err
		}
	}

	return out, nil
}

// applyServiceUserdataPair applies one semicolon-separated pair to out.
func applyServiceUserdataPair(out *ServiceUserdata, chunk string) error {
	if chunk == "" {
		return nil
	}
	key, value, found := strings.Cut(chunk, "=")
	if !found {
		return fmt.Errorf("dds: service userdata %q misses equals: %w", chunk, ErrServiceInvalidPacket)
	}
	switch key {
	case serviceUserdataKey:
		return setServiceUserdataID(out, value)
	case sedpUserdataKeyTypeHash:
		out.TypeHash = value
		return nil
	case sedpUserdataKeyDDSTopic:
		out.DDSTopic = value
		out.HasDDSTopic = true
		return nil
	case sedpUserdataKeyDomain:
		return setServiceUserdataDomain(out, value)
	default:
		return nil
	}
}

// setServiceUserdataID validates one service ID value into out.
func setServiceUserdataID(out *ServiceUserdata, value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fmt.Errorf("dds: service userdata id is empty: %w", ErrServiceInvalidName)
	}
	if err := validateServiceID(trimmed); err != nil {
		return err
	}
	out.ServiceID = trimmed
	out.HasServiceID = true
	return nil
}

// setServiceUserdataDomain parses one domain value into out.
func setServiceUserdataDomain(out *ServiceUserdata, value string) error {
	domain, err := parseServiceDomain(value)
	if err != nil {
		return err
	}
	out.Domain = domain
	out.HasDomain = true
	return nil
}

// parseServiceDomain parses one domain value for service userdata.
func parseServiceDomain(value string) (uint32, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, fmt.Errorf("dds: service userdata domain is empty: %w", ErrServiceInvalidName)
	}

	fields, err := sedpParseEndpointUserdata(sedpUserdataKeyDomain + "=" + trimmed)
	if err != nil {
		return 0, fmt.Errorf("dds: service userdata domain: %w", err)
	}

	return fields.domain, nil
}

// CheckServiceAvailable reports whether a server is available for a client.
//
// requestReaders counts remote readers on the request topic and
// replyWriters counts remote writers on the reply topic. Zero in either
// count means no server. wantServiceID is the hex service ID the client
// wants, empty when the client does not filter. readerIDs and writerIDs
// carry the advertised hex service IDs for each side. A want match on
// either side means available. When both ID lists are empty the check
// falls back to non empty counts so pre service ID peers still match.
// Otherwise a present but unmatched ID set means not available.
func CheckServiceAvailable(
	requestReaders int,
	replyWriters int,
	wantServiceID string,
	readerIDs []string,
	writerIDs []string,
) bool {
	if requestReaders <= 0 || replyWriters <= 0 {
		return false
	}

	want := strings.TrimSpace(wantServiceID)
	if want == "" {
		return true
	}

	for _, id := range readerIDs {
		if strings.TrimSpace(id) == want {
			return true
		}
	}

	for _, id := range writerIDs {
		if strings.TrimSpace(id) == want {
			return true
		}
	}

	if len(readerIDs) == 0 && len(writerIDs) == 0 {
		return true
	}

	return false
}

// serviceIDForName derives a ROS-format service ID from a prefix.
//
// The ID matches the rmw_cyclonedds_cpp wire form: the 12 byte
// participant GUID prefix plus 4 bytes derived from the service name,
// formatted as dot-separated hex pairs like
// 1.10.9a.9c.bc.6.94.20.76.2d.69.93.0.0.0.8. Reader plus writer for one
// service share the ID so rmw pairs both halves to one service;
// distinct services map to distinct IDs.
func serviceIDForName(prefix SEDPGUIDPrefix, service string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(service)) //nolint:errcheck // hash writes never fail
	var raw [16]byte
	copy(raw[:12], prefix[:])
	binary.BigEndian.PutUint32(raw[12:], h.Sum32())
	parts := make([]string, 0, 16)
	for _, b := range raw {
		parts = append(parts, strings.ToLower(hex.EncodeToString([]byte{b})))
	}
	return strings.Join(parts, ".")
}

// serviceResolveTopic maps one service name to its DDS topic and checks the round trip.
func serviceResolveTopic(serviceName string, isRequest bool) (ddsTopic, rosBack string, kind TopicKind, err error) {
	if isRequest {
		ddsTopic, err = RequestTopic(serviceName, false)
		if err != nil {
			return "", "", "", fmt.Errorf("dds: service declare request topic %q: %w", serviceName, err)
		}
	} else {
		ddsTopic, err = ReplyTopic(serviceName, false)
		if err != nil {
			return "", "", "", fmt.Errorf("dds: service declare reply topic %q: %w", serviceName, err)
		}
	}
	rosBack, kind, err = ParseTopicName(ddsTopic)
	if err != nil {
		return "", "", "", fmt.Errorf("dds: service declare parse topic %q: %w", ddsTopic, err)
	}
	wantKind := TopicKindReply
	if isRequest {
		wantKind = TopicKindRequest
	}
	if kind != wantKind {
		return "", "", "", fmt.Errorf("dds: service declare kind %q, want %q: %w", kind, wantKind, ErrSEDPInvalidEndpoint)
	}
	trimmedService := strings.TrimSuffix(strings.TrimSpace(serviceName), "/")
	if rosBack != trimmedService {
		return "", "", "", fmt.Errorf("dds: service declare service %q, want %q: %w", rosBack, trimmedService, ErrSEDPInvalidEndpoint)
	}
	return ddsTopic, rosBack, kind, nil
}

// serviceResolveType maps one IDL name to its DDS type and checks the round trip.
func serviceResolveType(idlName string) (ddsType string, err error) {
	ddsType, err = MessageTypeName(idlName)
	if err != nil {
		return "", fmt.Errorf("dds: service declare type %q: %w", idlName, err)
	}
	backIDL, err := DemangleTypeName(ddsType)
	if err != nil {
		return "", fmt.Errorf("dds: service declare demangle %q: %w", ddsType, err)
	}
	if backIDL != idlName {
		return "", fmt.Errorf("dds: service declare type round trip %q vs %q: %w", backIDL, idlName, ErrSEDPInvalidEndpoint)
	}
	return ddsType, nil
}

// DeclareServiceEndpoint declares a local service reader or writer.
//
// serviceName is the ROS service name such as /add_two_ints. idlName is
// the IDL request or response name such as std_srvs__srv__Trigger_Request.
// isRequest selects the rq request topic when true and the rr reply topic
// when false. The DDS topic derives from RequestTopic or ReplyTopic and
// the DDS type derives from MessageTypeName. Both mappings round trip
// through ParseTopicName and DemangleTypeName so a malformed service name
// or type fails before any entity is allocated. The stored ROSTopic is
// the ROS service name and Kind is request or reply, so TakeBytes
// reports the service name for both directions.
func (idx *SEDPIndex) DeclareServiceEndpoint(isReader bool, serviceName, idlName string, isRequest bool, typeHash string, qos []byte) (SEDPEndpoint, error) {
	ddsTopic, rosBack, kind, err := serviceResolveTopic(serviceName, isRequest)
	if err != nil {
		return SEDPEndpoint{}, err
	}
	ddsType, err := serviceResolveType(idlName)
	if err != nil {
		return SEDPEndpoint{}, err
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.next > sedpUserEntityMax {
		return SEDPEndpoint{}, fmt.Errorf("dds: service declare entity space exhausted: %w", ErrSEDPInvalidEndpoint)
	}
	entity := sedpEntityFor(idx.next, isReader)
	idx.next++
	id := SEDPGUID{Prefix: idx.prefix, Entity: entity}
	if _, exists := idx.locals[id]; exists {
		return SEDPEndpoint{}, fmt.Errorf("dds: service declare duplicate entity: %w", ErrSEDPInvalidEndpoint)
	}
	ep := SEDPEndpoint{
		ID:                 id,
		ROSTopic:           rosBack,
		DDSTopic:           ddsTopic,
		Kind:               kind,
		TypeDDS:            ddsType,
		TypeIDL:            idlName,
		TypeHash:           typeHash,
		QoS:                sedpCloneBytes(qos),
		IsReader:           isReader,
		ServiceID:          serviceIDForName(idx.prefix, rosBack),
		VolatileDurability: true,
		DomainID:           idx.domain,
	}
	if err := sedpCheckWireValues(ep.DDSTopic, ep.TypeHash); err != nil {
		return SEDPEndpoint{}, fmt.Errorf("dds: service declare wire values: %w", err)
	}
	idx.locals[id] = sedpCloneEndpoint(ep)
	return sedpCloneEndpoint(ep), nil
}

// findLocalServiceWriter reuses a local service writer for one DDS topic and type.
//
// It scans locals for a writer with equal DDS topic, DDS type, IDL, and
// hash so repeated service publishes keep one stable writer GUID and the
// DATA writer entity matches the SEDP announcement.
func (idx *SEDPIndex) findLocalServiceWriter(ddsTopic, ddsType, idlName, typeHash string) (SEDPEndpoint, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	for _, ep := range idx.locals {
		if ep.IsReader {
			continue
		}
		if ep.DDSTopic != ddsTopic {
			continue
		}
		if ep.TypeDDS != ddsType {
			continue
		}
		if ep.TypeIDL != idlName {
			continue
		}
		if ep.TypeHash != typeHash {
			continue
		}
		return sedpCloneEndpoint(ep), true
	}
	return SEDPEndpoint{}, false
}

// outgoingForService builds the publish message view for service DATA.
//
// The topic name is the ROS service name for error text and the type
// name is the IDL request or response form for plain sample selection.
// The wire topic and type ride on the declared writer GUID, not on this
// view.
func outgoingForService(service, idlName string, payload []byte) transports.OutgoingMessage {
	return transports.OutgoingMessage{TopicName: service, TypeName: idlName, Payload: payload}
}

// DeclareServiceWriter pre-declares a local service writer without sending DATA.
//
// The server role pre-declares its reply writer at startup so ROS
// clients waiting for service availability see the reply writer before
// any request arrives. Without this the server only announces the writer
// on its first reply publish, which deadlocks against clients that wait
// for availability before sending. The client role pre-declares its
// request writer for the same reason when it must be visible before the
// first publish. Declaration rides the periodic SEDP announce loop, so
// no routes are required and no DATA is sent. A best-effort synchronous
// announce runs when routes exist so ROS peers waiting for availability
// see the writer without waiting for the next periodic tick; a missing
// route is not an error because the periodic loop still delivers later.
func (b *Backend) DeclareServiceWriter(ctx context.Context, service, idlBase string, role ServiceRole, typeHash string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("dds: declare service writer: %w", err)
	}
	b.mu.RLock()
	closed := b.closed
	joined := b.joined
	sedpIdx := b.sedp
	b.mu.RUnlock()
	if closed {
		return fmt.Errorf("dds: declare service writer after close: %w", transports.ErrBackendClosed)
	}
	if !joined {
		return fmt.Errorf("dds: declare service writer before join: %w", transports.ErrNotJoined)
	}
	if sedpIdx == nil {
		return fmt.Errorf("dds: declare service writer data path not wired yet: %w", ErrDataPathNotWired)
	}
	eps, err := NewServiceEndpoints(service, idlBase, false)
	if err != nil {
		return err
	}
	var ddsTopic, ddsType string
	var isRequest bool
	if role == ServiceRoleClient {
		ddsTopic = eps.RequestTopic
		ddsType = eps.RequestType
		isRequest = true
	} else {
		ddsTopic = eps.ReplyTopic
		ddsType = eps.ResponseType
		isRequest = false
	}
	idlName, err := DemangleTypeName(ddsType)
	if err != nil {
		return fmt.Errorf("dds: declare service writer demangle: %w", err)
	}
	hash := sedpTypeHashForIDL(idlName, typeHash)
	if _, ok := sedpIdx.findLocalServiceWriter(ddsTopic, ddsType, idlName, hash); ok {
		return nil
	}
	ep, err := sedpIdx.DeclareServiceEndpoint(false, service, idlName, isRequest, hash, nil)
	if err != nil {
		return fmt.Errorf("dds: declare service writer: %w", err)
	}
	err = b.announceWriterOnce(ctx, ep)
	if err != nil {
		if errors.Is(err, ErrNoRoute) {
			return nil
		}
		return err
	}
	return nil
}

// SubscribeService declares a local service reader for one role.
//
// service is the ROS service name and idlBase is the IDL base without
// suffix such as std_srvs__srv__Trigger. The client role subscribes to
// the reply topic with the Response type and the server role subscribes
// to the request topic with the Request type. The type hash is empty for
// v1 service interop. A missing backend or unwired data path fails
// loudly. A best-effort synchronous announce runs when routes exist so
// peers discover the reader without waiting for the next periodic tick;
// a missing route is not an error because the periodic loop still
// delivers later.
func (b *Backend) SubscribeService(ctx context.Context, service, idlBase string, role ServiceRole, typeHash string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("dds: subscribe service: %w", err)
	}
	b.mu.RLock()
	closed := b.closed
	joined := b.joined
	sedpIdx := b.sedp
	b.mu.RUnlock()
	if closed {
		return fmt.Errorf("dds: subscribe service after close: %w", transports.ErrBackendClosed)
	}
	if !joined {
		return fmt.Errorf("dds: subscribe service before join: %w", transports.ErrNotJoined)
	}
	if sedpIdx == nil {
		return fmt.Errorf("dds: subscribe service data path not wired yet: %w", ErrDataPathNotWired)
	}
	eps, err := NewServiceEndpoints(service, idlBase, false)
	if err != nil {
		return err
	}
	var idlName string
	var isRequest bool
	if role == ServiceRoleServer {
		// RequestType is DDS form; demangle to IDL request form.
		idlName, err = DemangleTypeName(eps.RequestType)
		if err != nil {
			return fmt.Errorf("dds: subscribe service demangle request: %w", err)
		}
		isRequest = true
	} else {
		idlName, err = DemangleTypeName(eps.ResponseType)
		if err != nil {
			return fmt.Errorf("dds: subscribe service demangle response: %w", err)
		}
		isRequest = false
	}
	hash := sedpTypeHashForIDL(idlName, typeHash)
	ep, err := sedpIdx.DeclareServiceEndpoint(true, service, idlName, isRequest, hash, nil)
	if err != nil {
		return fmt.Errorf("dds: subscribe service declare reader: %w", err)
	}
	err = b.announceWriterOnce(ctx, ep)
	if err != nil {
		if errors.Is(err, ErrNoRoute) {
			return nil
		}
		return err
	}
	return nil
}

// PublishService publishes one service request or reply sample.
//
// service is the ROS service name and idlBase is the IDL base without
// suffix. The client role publishes requests and the server role
// publishes replies. The payload must already carry the service header
// plus GUID plus sequence from the Encode helpers and a valid XCDR1
// header so CycloneDDS accepts it as plain DATA. The writer is declared
// through the service endpoint path and announced synchronously before
// DATA so single shot callers do not exit before matching. Empty routes
// report ErrNoRoute for retry loops. Non plain payloads fail loudly
// instead of falling back to framed topics.
func (b *Backend) PublishService(ctx context.Context, service, idlBase string, role ServiceRole, payload []byte, typeHash string) error {
	snap, err := b.loadPublishSnapshot(ctx)
	if err != nil {
		return err
	}
	eps, err := NewServiceEndpoints(service, idlBase, false)
	if err != nil {
		return err
	}
	var ddsTopic, ddsType, idlName string
	var isRequest bool
	if role == ServiceRoleClient {
		ddsTopic = eps.RequestTopic
		ddsType = eps.RequestType
		isRequest = true
	} else {
		ddsTopic = eps.ReplyTopic
		ddsType = eps.ResponseType
		isRequest = false
	}
	idlName, err = DemangleTypeName(ddsType)
	if err != nil {
		return fmt.Errorf("dds: publish service demangle: %w", err)
	}
	hash := sedpTypeHashForIDL(idlName, typeHash)
	if !isPlainCDRSample(payload, idlName) {
		return fmt.Errorf("dds: publish service payload not plain for %q: %w", idlName, ErrServiceInvalidPacket)
	}
	ep, ok := snap.sedp.findLocalServiceWriter(ddsTopic, ddsType, idlName, hash)
	if !ok {
		ep, err = snap.sedp.DeclareServiceEndpoint(false, service, idlName, isRequest, hash, nil)
		if err != nil {
			return fmt.Errorf("dds: publish service declare writer: %w", err)
		}
	}
	if snap.routeCount == 0 && len(b.snapshotRoutes()) == 0 {
		return routeError(service, snap.domain, ErrNoRoute)
	}
	writer := b.writerForPublish(snap, ep)
	msg := outgoingForService(service, idlName, payload)
	if err := b.announceWriterForPublish(ctx, msg, ep, writer, snap); err != nil {
		return err
	}
	if err := b.publishPlain(ctx, msg, snap, writer); err != nil {
		return err
	}
	return nil
}
