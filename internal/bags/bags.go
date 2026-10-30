// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package bags carries the v1 bag exchange for info and replay.
//
// There is no storage engine in v1. This package reads and writes the
// exchange formats only: topic plus type plus hash metadata with XCDR1
// payloads preserved byte for byte. Schemas come from the type registry,
// payloads stay as XCDR1 envelope bytes, iteration order is deterministic,
// and truncated input reports bounds errors with metrics and never panics.
package bags

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/podomy/weld/internal/types"
)

// Exchange identifiers and wire constants.
const (
	// StorageSQLite3 is the sqlite3 storage id from the bag contract.
	StorageSQLite3 = "sqlite3"
	// StorageMCAP is the mcap storage id from the bag contract.
	StorageMCAP = "mcap"
	// FormatCDR is the only serialization format supported in v1.
	FormatCDR = "cdr"
	// wireVersion is the exchange version written after the magic.
	wireVersion uint16 = 1
)

// Wire framing and allocation caps.
//
// Caps bound corrupt length prefixes before allocation so a truncated or
// hostile buffer fails closed with a drop instead of forcing a huge
// allocation. The data cap matches the CDR single sample cap so one bag
// sample can never force more than one CDR sample.
const (
	magic0 = 0x57
	magic1 = 0x42
	magic2 = 0x41
	magic3 = 0x47

	maxStorageLen  = 32
	maxTopics      = 65536
	maxMessages    = 1 << 20
	maxNameLen     = 1024
	maxTypeLen     = 1024
	maxFormatLen   = 32
	maxHashLen     = 128
	maxQoSEntries  = 32
	maxQoSEntryLen = 256
	maxStringField = 4096
	maxDataBytes   = 32 << 20
	maxDecodeDepth = 32
)

// Metrics counts dropped inputs for bag parsing.
//
// Drops increments for truncated, empty, and malformed buffers. The struct
// is plain data owned by the caller. There is no global state.
type Metrics struct {
	// Drops counts inputs dropped instead of parsed.
	Drops uint64
}

// RecordDrop adds one drop to the counter.
func (m *Metrics) RecordDrop() {
	if m == nil {
		return
	}
	m.Drops++
}

// Count returns the drop total, treating nil as zero.
func (m *Metrics) Count() uint64 {
	if m == nil {
		return 0
	}
	return m.Drops
}

// Topic carries one topic record with id plus name plus type plus format
// plus offered QoS vector plus type description hash.
type Topic struct {
	// ID is the numeric topic id carried in bag metadata.
	ID uint32
	// Name is the fully qualified ROS name such as /chatter.
	Name string
	// Type is the slash IDL name such as std_msgs/msg/String.
	Type string
	// Format is the serialization format, always cdr in v1.
	Format string
	// OfferedQoS is the recorded offered QoS vector in declared order.
	OfferedQoS []string
	// TypeHash is the RIHS01 hash string or empty for unset.
	TypeHash string
}

// Validate checks one topic record without touching the registry.
func (t Topic) Validate() error {
	err := validateTopicHeader(t)
	if err != nil {
		return err
	}
	err = validateTopicQoS(t)
	if err != nil {
		return err
	}
	err = validateTopicHash(t)
	if err != nil {
		return err
	}
	return nil
}

// validateTopicHeader checks name plus type plus format.
func validateTopicHeader(t Topic) error {
	if t.Name == "" {
		return errors.New("bags: topic name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if t.Name[0] != '/' {
		return fmt.Errorf("bags: topic %q must start with slash", t.Name)
	}
	if len(t.Name) > maxNameLen {
		return fmt.Errorf("bags: topic name length %d above cap %d", len(t.Name), maxNameLen)
	}
	if t.Type == "" {
		return errors.New("bags: topic type is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := types.ValidateTypeName(t.Type)
	if err != nil {
		return fmt.Errorf("bags: topic %q type: %w", t.Name, err)
	}
	if t.Format != FormatCDR {
		return fmt.Errorf("bags: topic %q format %q must be %q", t.Name, t.Format, FormatCDR)
	}
	return nil
}

// validateTopicQoS checks the offered QoS vector entries.
func validateTopicQoS(t Topic) error {
	if len(t.OfferedQoS) > maxQoSEntries {
		return fmt.Errorf("bags: topic %q qos entries %d above cap %d", t.Name, len(t.OfferedQoS), maxQoSEntries)
	}
	for i := range t.OfferedQoS {
		if t.OfferedQoS[i] == "" {
			return fmt.Errorf("bags: topic %q qos entry %d is empty", t.Name, i)
		}
		if len(t.OfferedQoS[i]) > maxQoSEntryLen {
			return fmt.Errorf("bags: topic %q qos entry %d above cap", t.Name, i)
		}
	}
	return nil
}

// validateTopicHash checks the RIHS01 hash string when present.
func validateTopicHash(t Topic) error {
	if t.TypeHash == "" {
		return nil
	}
	_, err := types.ParseTypeHash(t.TypeHash)
	if err != nil {
		return fmt.Errorf("bags: topic %q hash: %w", t.Name, err)
	}
	return nil
}

// Message carries one serialized bag message.
//
// Data holds the full XCDR1 envelope bytes including the 4 byte header,
// preserved verbatim for byte identical replay. Sequence defaults to 0
// when the recorder did not set one.
type Message struct {
	// Topic is the fully qualified ROS name the bytes arrived on.
	Topic string
	// Data holds the XCDR1 envelope bytes. The slice is copied on append
	// and on parse so the caller keeps ownership of the input.
	Data []byte
	// ReceiveNs is the receive timestamp in nanoseconds.
	ReceiveNs int64
	// SendNs is the send timestamp in nanoseconds.
	SendNs int64
	// Sequence is the writer sequence, defaulting to 0 when unset.
	Sequence uint64
	// Format is the serialization format, always cdr in v1.
	Format string
}

// Validate checks one message record including the XCDR1 envelope header.
func (m Message) Validate() error {
	err := validateMessageHeader(m)
	if err != nil {
		return err
	}
	err = validateMessageTimes(m)
	if err != nil {
		return err
	}
	if len(m.Data) == 0 {
		return fmt.Errorf("bags: message on %q has empty data", m.Topic)
	}
	if len(m.Data) > maxDataBytes {
		return fmt.Errorf("bags: message on %q data length %d above cap %d", m.Topic, len(m.Data), maxDataBytes)
	}
	err = validateEnvelope(m.Data)
	if err != nil {
		return fmt.Errorf("bags: message on %q: %w", m.Topic, err)
	}
	return nil
}

// validateMessageHeader checks topic plus format identity.
func validateMessageHeader(m Message) error {
	if m.Topic == "" {
		return errors.New("bags: message topic is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if m.Topic[0] != '/' {
		return fmt.Errorf("bags: message topic %q must start with slash", m.Topic)
	}
	if len(m.Topic) > maxNameLen {
		return fmt.Errorf("bags: message topic length %d above cap", len(m.Topic))
	}
	if m.Format != FormatCDR {
		return fmt.Errorf("bags: message on %q format %q must be %q", m.Topic, m.Format, FormatCDR)
	}
	return nil
}

// validateMessageTimes checks receive plus send timestamps.
func validateMessageTimes(m Message) error {
	if m.ReceiveNs < 0 {
		return fmt.Errorf("bags: message on %q receive timestamp %d is negative", m.Topic, m.ReceiveNs)
	}
	if m.SendNs < 0 {
		return fmt.Errorf("bags: message on %q send timestamp %d is negative", m.Topic, m.SendNs)
	}
	return nil
}

// validateEnvelope checks the XCDR1 header plus 4 byte pad rule.
func validateEnvelope(data []byte) error {
	_, err := types.ParseHeader(data)
	if err != nil {
		return fmt.Errorf("bags: envelope: %w", err)
	}
	if len(data)%4 != 0 {
		return fmt.Errorf("bags: envelope length %d is not a multiple of 4", len(data))
	}
	return nil
}

// TopicInfo summarizes one topic for info output.
type TopicInfo struct {
	// ID is the topic id from metadata.
	ID uint32
	// Name is the topic name.
	Name string
	// Type is the slash IDL type name.
	Type string
	// Format is the serialization format.
	Format string
	// TypeHash is the RIHS01 hash string or empty.
	TypeHash string
	// Count is the number of messages recorded on this topic.
	Count int
}

// Info summarizes one bag in deterministic topic order.
type Info struct {
	// Storage is the storage id such as sqlite3 or mcap.
	Storage string
	// TopicCount is the number of topics.
	TopicCount int
	// MessageCount is the number of messages.
	MessageCount int
	// StartNs is the smallest receive timestamp, zero when empty.
	StartNs int64
	// EndNs is the largest receive timestamp, zero when empty.
	EndNs int64
	// Topics lists per topic counts sorted by topic name.
	Topics []TopicInfo
}

// Bag holds the in memory exchange for info and replay.
//
// Topics are indexed by name and by id. Messages keep raw XCDR1 bytes
// verbatim. Iteration through Topics, OrderedMessages, Info, and Marshal
// is deterministic: topics sort by name and messages sort by receive
// timestamp then topic then sequence.
type Bag struct {
	storage  string
	topics   map[string]Topic
	ids      map[uint32]string
	messages []Message
	drops    uint64
}

// New returns an empty bag for the given storage id.
func New(storage string) (*Bag, error) {
	if storage == "" {
		storage = StorageSQLite3
	}
	if storage != StorageSQLite3 && storage != StorageMCAP {
		return nil, fmt.Errorf("bags: unknown storage id %q", storage)
	}
	return &Bag{
		storage: storage,
		topics:  make(map[string]Topic),
		ids:     make(map[uint32]string),
	}, nil
}

// Storage returns the storage id for this bag.
func (b *Bag) Storage() string {
	if b == nil {
		return ""
	}
	return b.storage
}

// Drops returns the count of failed append plus verify inputs.
func (b *Bag) Drops() uint64 {
	if b == nil {
		return 0
	}
	return b.drops
}

// recordDrop adds one drop to the bag counter.
func (b *Bag) recordDrop() {
	if b == nil {
		return
	}
	b.drops++
}

// AddTopic records one topic record.
//
// A zero ID is assigned as one plus the current maximum so callers that
// add topics in sorted name order get deterministic IDs. A nonzero ID must
// be unique across topics. Names must be unique. The QoS vector is copied.
func (b *Bag) AddTopic(t Topic) error {
	if b == nil {
		return errors.New("bags: nil bag") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := t.Validate()
	if err != nil {
		b.recordDrop()
		return fmt.Errorf("bags: add topic %q: %w", t.Name, err)
	}
	if _, ok := b.topics[t.Name]; ok {
		b.recordDrop()
		return fmt.Errorf("bags: duplicate topic %q", t.Name)
	}
	id := t.ID
	if id == 0 {
		id = b.nextTopicID()
	}
	if _, ok := b.ids[id]; ok {
		b.recordDrop()
		return fmt.Errorf("bags: duplicate topic id %d", id)
	}
	cp := Topic{
		ID:       id,
		Name:     t.Name,
		Type:     t.Type,
		Format:   t.Format,
		TypeHash: t.TypeHash,
	}
	if t.OfferedQoS != nil {
		cp.OfferedQoS = make([]string, len(t.OfferedQoS))
		copy(cp.OfferedQoS, t.OfferedQoS)
	} else {
		cp.OfferedQoS = []string{}
	}
	b.topics[t.Name] = cp
	b.ids[id] = t.Name
	return nil
}

// nextTopicID returns one plus the current maximum id, or 1 when empty.
func (b *Bag) nextTopicID() uint32 {
	var top uint32
	for id := range b.ids {
		if id > top {
			top = id
		}
	}
	return top + 1
}

// Append records one message after envelope plus schema checks.
//
// The topic must exist and the message format must match the topic format.
// The XCDR1 envelope header plus pad rule is always checked. When reg is
// non nil and the topic type is known to the registry, the type hash is
// verified when present and the payload fields are decoded against the
// registry schema with trailing garbage rejected. Unknown types stay first
// class: they pass with envelope checks only.
func (b *Bag) Append(m Message, reg *types.Registry) error {
	if b == nil {
		return errors.New("bags: nil bag") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := m.Validate()
	if err != nil {
		b.recordDrop()
		return fmt.Errorf("bags: append: %w", err)
	}
	topic, ok := b.topics[m.Topic]
	if !ok {
		b.recordDrop()
		return fmt.Errorf("bags: append: unknown topic %q", m.Topic)
	}
	if m.Format != topic.Format {
		b.recordDrop()
		return fmt.Errorf("bags: append on %q format %q mismatches topic format %q", m.Topic, m.Format, topic.Format)
	}
	err = checkSchema(m, topic, reg)
	if err != nil {
		b.recordDrop()
		return err
	}
	cp := Message{
		Topic:     m.Topic,
		ReceiveNs: m.ReceiveNs,
		SendNs:    m.SendNs,
		Sequence:  m.Sequence,
		Format:    m.Format,
	}
	if len(m.Data) > 0 {
		cp.Data = make([]byte, len(m.Data))
		copy(cp.Data, m.Data)
	}
	b.messages = append(b.messages, cp)
	return nil
}

// checkSchema verifies hash plus payload fields when the registry knows the type.
func checkSchema(m Message, topic Topic, reg *types.Registry) error {
	if reg == nil {
		return nil
	}
	if topic.Type == "" {
		return nil
	}
	_, ok := reg.LookupType(topic.Type)
	if !ok {
		return nil
	}
	if topic.TypeHash != "" {
		h, err := types.ParseTypeHash(topic.TypeHash)
		if err != nil {
			return fmt.Errorf("bags: append on %q: %w", m.Topic, err)
		}
		_, err = reg.LookupWithHash(topic.Type, h)
		if err != nil {
			return fmt.Errorf("bags: append on %q: %w", m.Topic, err)
		}
	}
	err := validatePayload(m.Data, topic.Type, reg)
	if err != nil {
		return fmt.Errorf("bags: append on %q: %w", m.Topic, err)
	}
	return nil
}

// Topics returns the topic records sorted by name.
func (b *Bag) Topics() []Topic {
	if b == nil {
		return nil
	}
	out := make([]Topic, 0, len(b.topics))
	for _, topic := range b.topics {
		if topic.OfferedQoS != nil {
			cpQoS := make([]string, len(topic.OfferedQoS))
			copy(cpQoS, topic.OfferedQoS)
			topic.OfferedQoS = cpQoS
		}
		out = append(out, topic)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

// OrderedMessages returns the messages in replay order.
//
// Replay order sorts by receive timestamp then topic name then sequence
// number with a stable tiebreak on send timestamp. Data slices are copied
// so the caller may mutate the result without affecting the bag.
func (b *Bag) OrderedMessages() []Message {
	if b == nil {
		return nil
	}
	out := make([]Message, len(b.messages))
	for i := range b.messages {
		out[i] = b.messages[i]
		if len(b.messages[i].Data) > 0 {
			cp := make([]byte, len(b.messages[i].Data))
			copy(cp, b.messages[i].Data)
			out[i].Data = cp
		}
	}
	sortMessages(out)
	return out
}

// sortMessages orders messages for deterministic replay.
func sortMessages(ms []Message) {
	sort.SliceStable(ms, func(i, j int) bool {
		if ms[i].ReceiveNs != ms[j].ReceiveNs {
			return ms[i].ReceiveNs < ms[j].ReceiveNs
		}
		if ms[i].Topic != ms[j].Topic {
			return ms[i].Topic < ms[j].Topic
		}
		if ms[i].Sequence != ms[j].Sequence {
			return ms[i].Sequence < ms[j].Sequence
		}
		return ms[i].SendNs < ms[j].SendNs
	})
}

// Info summarizes the bag with per topic counts in name order.
func (b *Bag) Info() Info {
	if b == nil {
		return Info{}
	}
	topics := b.Topics()
	counts := make(map[string]int, len(topics))
	for i := range b.messages {
		counts[b.messages[i].Topic]++
	}
	infos := make([]TopicInfo, 0, len(topics))
	for i := range topics {
		infos = append(infos, TopicInfo{
			ID:       topics[i].ID,
			Name:     topics[i].Name,
			Type:     topics[i].Type,
			Format:   topics[i].Format,
			TypeHash: topics[i].TypeHash,
			Count:    counts[topics[i].Name],
		})
	}
	out := Info{
		Storage:      b.storage,
		TopicCount:   len(topics),
		MessageCount: len(b.messages),
		Topics:       infos,
	}
	rangeStart, rangeEnd, ok := messageRange(b.messages)
	if ok {
		out.StartNs = rangeStart
		out.EndNs = rangeEnd
	}
	return out
}

// messageRange returns the smallest plus largest receive timestamps.
func messageRange(ms []Message) (int64, int64, bool) {
	if len(ms) == 0 {
		return 0, 0, false
	}
	first := ms[0].ReceiveNs
	last := ms[0].ReceiveNs
	for i := range ms[1:] {
		v := ms[i+1].ReceiveNs
		if v < first {
			first = v
		}
		if v > last {
			last = v
		}
	}
	return first, last, true
}

// Verify checks every topic hash plus payload against the registry.
//
// Unknown types stay first class and pass with envelope checks only. Known
// types verify the hash when present and decode the payload fields with
// trailing garbage rejected. A nil registry checks envelopes only.
func (b *Bag) Verify(reg *types.Registry) error {
	if b == nil {
		return errors.New("bags: nil bag") //nolint:wrapcheck // new error, nothing to wrap
	}
	err := b.verifyTopics(reg)
	if err != nil {
		return err
	}
	err = b.verifyMessages(reg)
	if err != nil {
		return err
	}
	return nil
}

// verifyTopics checks topic records plus known hashes.
func (b *Bag) verifyTopics(reg *types.Registry) error {
	for _, topic := range b.Topics() {
		err := topic.Validate()
		if err != nil {
			b.recordDrop()
			return fmt.Errorf("bags: verify topic %q: %w", topic.Name, err)
		}
		err = b.verifyTopicHash(topic, reg)
		if err != nil {
			return err
		}
	}
	return nil
}

// verifyTopicHash checks one topic hash when the registry knows the type.
func (b *Bag) verifyTopicHash(topic Topic, reg *types.Registry) error {
	if reg == nil {
		return nil
	}
	if topic.Type == "" || topic.TypeHash == "" {
		return nil
	}
	_, ok := reg.LookupType(topic.Type)
	if !ok {
		return nil
	}
	h, err := types.ParseTypeHash(topic.TypeHash)
	if err != nil {
		b.recordDrop()
		return fmt.Errorf("bags: verify topic %q: %w", topic.Name, err)
	}
	_, err = reg.LookupWithHash(topic.Type, h)
	if err != nil {
		b.recordDrop()
		return fmt.Errorf("bags: verify topic %q: %w", topic.Name, err)
	}
	return nil
}

// verifyMessages checks message envelopes plus known payloads.
func (b *Bag) verifyMessages(reg *types.Registry) error {
	for i := range b.messages {
		msg := b.messages[i]
		err := msg.Validate()
		if err != nil {
			b.recordDrop()
			return fmt.Errorf("bags: verify message %d: %w", i, err)
		}
		topic, ok := b.topics[msg.Topic]
		if !ok {
			b.recordDrop()
			return fmt.Errorf("bags: verify message %d: unknown topic %q", i, msg.Topic)
		}
		err = b.verifyMessagePayload(msg, topic, reg)
		if err != nil {
			return err
		}
	}
	return nil
}

// verifyMessagePayload decodes one payload when the registry knows the type.
func (b *Bag) verifyMessagePayload(msg Message, topic Topic, reg *types.Registry) error {
	if reg == nil {
		return nil
	}
	if topic.Type == "" {
		return nil
	}
	_, ok := reg.LookupType(topic.Type)
	if !ok {
		return nil
	}
	err := validatePayload(msg.Data, topic.Type, reg)
	if err != nil {
		b.recordDrop()
		return fmt.Errorf("bags: verify message on %q: %w", msg.Topic, err)
	}
	return nil
}

// validatePayload decodes one XCDR1 payload against the registry schema.
func validatePayload(payload []byte, typeName string, reg *types.Registry) error {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return fmt.Errorf("bags: header: %w", err)
	}
	full, err := reg.TransitiveClosure(typeName)
	if err != nil {
		return fmt.Errorf("bags: closure %q: %w", typeName, err)
	}
	lookup := make(map[string]types.IndividualTypeDescription, len(full.Referenced)+1)
	lookup[full.Description.TypeName] = full.Description
	for i := range full.Referenced {
		lookup[full.Referenced[i].TypeName] = full.Referenced[i]
	}
	err = decodeIndividual(dec, full.Description, lookup, 0)
	if err != nil {
		return fmt.Errorf("bags: decode %q: %w", typeName, err)
	}
	err = dec.Finish()
	if err != nil {
		return fmt.Errorf("bags: trailing bytes: %w", err)
	}
	return nil
}

// decodeIndividual decodes one struct in IDL field order.
func decodeIndividual(dec *types.Decoder, desc types.IndividualTypeDescription, lookup map[string]types.IndividualTypeDescription, depth int) error {
	if dec == nil {
		return errors.New("bags: nil decoder") //nolint:wrapcheck // new error, nothing to wrap
	}
	if depth > maxDecodeDepth {
		return fmt.Errorf("bags: decode depth above cap at %q", desc.TypeName)
	}
	for i := range desc.Fields {
		err := decodeField(dec, desc.Fields[i], lookup, depth)
		if err != nil {
			return fmt.Errorf("bags: field %q: %w", desc.Fields[i].Name, err)
		}
	}
	return nil
}

// decodeField decodes one registry field including array wrappers.
func decodeField(dec *types.Decoder, field types.Field, lookup map[string]types.IndividualTypeDescription, depth int) error {
	id := field.Type.TypeID
	if id.IsArray() {
		return decodeFixedArray(dec, field, lookup, depth)
	}
	if id.IsBoundedSequence() {
		return decodeBoundedSequence(dec, field, lookup, depth)
	}
	if id.IsUnboundedSequence() {
		return decodeUnboundedSequence(dec, field, lookup, depth)
	}
	return decodeScalar(dec, id.Base(), field, lookup, depth)
}

// decodeFixedArray decodes a fixed array with exactly capacity elements.
func decodeFixedArray(dec *types.Decoder, field types.Field, lookup map[string]types.IndividualTypeDescription, depth int) error {
	n, err := checkedCapacity(field.Type.Capacity)
	if err != nil {
		return err
	}
	base := field.Type.TypeID.Base()
	for i := range n {
		err = decodeScalar(dec, base, field, lookup, depth)
		if err != nil {
			return fmt.Errorf("bags: index %d: %w", i, err)
		}
	}
	return nil
}

// decodeBoundedSequence decodes a bounded sequence within capacity.
func decodeBoundedSequence(dec *types.Decoder, field types.Field, lookup map[string]types.IndividualTypeDescription, depth int) error {
	bound, err := checkedCapacity(field.Type.Capacity)
	if err != nil {
		return err
	}
	n, err := dec.ReadBoundedSequenceLength(bound)
	if err != nil {
		return fmt.Errorf("bags: sequence length: %w", err)
	}
	base := field.Type.TypeID.Base()
	for i := range n {
		err = decodeScalar(dec, base, field, lookup, depth)
		if err != nil {
			return fmt.Errorf("bags: index %d: %w", i, err)
		}
	}
	return nil
}

// decodeUnboundedSequence decodes an unbounded sequence.
func decodeUnboundedSequence(dec *types.Decoder, field types.Field, lookup map[string]types.IndividualTypeDescription, depth int) error {
	n, err := dec.ReadSequenceLength()
	if err != nil {
		return fmt.Errorf("bags: sequence length: %w", err)
	}
	base := field.Type.TypeID.Base()
	for i := range n {
		err = decodeScalar(dec, base, field, lookup, depth)
		if err != nil {
			return fmt.Errorf("bags: index %d: %w", i, err)
		}
	}
	return nil
}

// checkedCapacity converts a registry capacity to an int for loops.
func checkedCapacity(capacity uint64) (int, error) {
	if capacity == 0 {
		return 0, errors.New("bags: zero array capacity") //nolint:wrapcheck // new error, nothing to wrap
	}
	if capacity > uint64(maxDataBytes) {
		return 0, fmt.Errorf("bags: capacity %d above cap", capacity)
	}
	return int(capacity), nil
}

// checkedStringBound converts a registry string capacity to an int bound.
func checkedStringBound(capacity uint64) (int, error) {
	if capacity > uint64(maxDataBytes) {
		return 0, fmt.Errorf("bags: string capacity %d above cap", capacity)
	}
	return int(capacity), nil
}

// decodeScalar decodes one scalar base value.
func decodeScalar(dec *types.Decoder, base types.FieldTypeID, field types.Field, lookup map[string]types.IndividualTypeDescription, depth int) error {
	if base == types.FieldTypeNestedType {
		return decodeNested(dec, field, lookup, depth)
	}
	if isStringBase(base) {
		return decodeStringBase(dec, base, field)
	}
	if isSmallBase(base) {
		return decodeSmallBase(dec, base)
	}
	if isNumericBase(base) {
		return decodeNumericBase(dec, base)
	}
	return fmt.Errorf("bags: unsupported type id %d", uint8(base))
}

// isStringBase reports whether the base id is a string family kind.
func isStringBase(base types.FieldTypeID) bool {
	return base == types.FieldTypeString ||
		base == types.FieldTypeWString ||
		base == types.FieldTypeFixedString ||
		base == types.FieldTypeFixedWString ||
		base == types.FieldTypeBoundedString ||
		base == types.FieldTypeBoundedWString
}

// isSmallBase reports whether the base id is bool plus char plus wchar.
func isSmallBase(base types.FieldTypeID) bool {
	return base == types.FieldTypeBoolean ||
		base == types.FieldTypeChar ||
		base == types.FieldTypeWChar
}

// isNumericBase reports whether the base id is an integer plus float kind.
func isNumericBase(base types.FieldTypeID) bool {
	return base == types.FieldTypeInt8 ||
		base == types.FieldTypeUInt8 ||
		base == types.FieldTypeByte ||
		base == types.FieldTypeInt16 ||
		base == types.FieldTypeUInt16 ||
		base == types.FieldTypeInt32 ||
		base == types.FieldTypeUInt32 ||
		base == types.FieldTypeInt64 ||
		base == types.FieldTypeUInt64 ||
		base == types.FieldTypeFloat ||
		base == types.FieldTypeDouble
}

// decodeStringBase decodes one string family value.
func decodeStringBase(dec *types.Decoder, base types.FieldTypeID, field types.Field) error {
	if base == types.FieldTypeString ||
		base == types.FieldTypeFixedString ||
		base == types.FieldTypeBoundedString {
		return decodeStringScalar(dec, field)
	}
	if base == types.FieldTypeWString ||
		base == types.FieldTypeFixedWString ||
		base == types.FieldTypeBoundedWString {
		return decodeWStringScalar(dec, field)
	}
	return fmt.Errorf("bags: unsupported string id %d", uint8(base))
}

// decodeSmallBase decodes one bool plus char plus wchar value.
func decodeSmallBase(dec *types.Decoder, base types.FieldTypeID) error {
	if base == types.FieldTypeBoolean {
		_, err := dec.ReadBool()
		if err != nil {
			return fmt.Errorf("bags: bool: %w", err)
		}
		return nil
	}
	if base == types.FieldTypeChar {
		_, err := dec.ReadChar()
		if err != nil {
			return fmt.Errorf("bags: char: %w", err)
		}
		return nil
	}
	if base == types.FieldTypeWChar {
		_, err := dec.ReadWchar()
		if err != nil {
			return fmt.Errorf("bags: wchar: %w", err)
		}
		return nil
	}
	return fmt.Errorf("bags: unsupported small id %d", uint8(base))
}

// decodeNumericBase decodes one integer plus float value.
func decodeNumericBase(dec *types.Decoder, base types.FieldTypeID) error {
	if base == types.FieldTypeInt8 {
		return decodeInt8(dec)
	}
	if base == types.FieldTypeUInt8 || base == types.FieldTypeByte {
		return decodeUint8(dec)
	}
	if base == types.FieldTypeInt16 {
		return decodeInt16(dec)
	}
	if base == types.FieldTypeUInt16 {
		return decodeUint16(dec)
	}
	if base == types.FieldTypeInt32 {
		return decodeInt32(dec)
	}
	if base == types.FieldTypeUInt32 {
		return decodeUint32(dec)
	}
	if base == types.FieldTypeInt64 {
		return decodeInt64(dec)
	}
	if base == types.FieldTypeUInt64 {
		return decodeUint64(dec)
	}
	if base == types.FieldTypeFloat {
		return decodeFloat32(dec)
	}
	if base == types.FieldTypeDouble {
		return decodeFloat64(dec)
	}
	return fmt.Errorf("bags: unsupported numeric id %d", uint8(base))
}

// decodeInt8 decodes one int8 value.
func decodeInt8(dec *types.Decoder) error {
	_, err := dec.ReadInt8()
	if err != nil {
		return fmt.Errorf("bags: int8: %w", err)
	}
	return nil
}

// decodeUint8 decodes one uint8 value.
func decodeUint8(dec *types.Decoder) error {
	_, err := dec.ReadUint8()
	if err != nil {
		return fmt.Errorf("bags: uint8: %w", err)
	}
	return nil
}

// decodeInt16 decodes one int16 value.
func decodeInt16(dec *types.Decoder) error {
	_, err := dec.ReadInt16()
	if err != nil {
		return fmt.Errorf("bags: int16: %w", err)
	}
	return nil
}

// decodeUint16 decodes one uint16 value.
func decodeUint16(dec *types.Decoder) error {
	_, err := dec.ReadUint16()
	if err != nil {
		return fmt.Errorf("bags: uint16: %w", err)
	}
	return nil
}

// decodeInt32 decodes one int32 value.
func decodeInt32(dec *types.Decoder) error {
	_, err := dec.ReadInt32()
	if err != nil {
		return fmt.Errorf("bags: int32: %w", err)
	}
	return nil
}

// decodeUint32 decodes one uint32 value.
func decodeUint32(dec *types.Decoder) error {
	_, err := dec.ReadUint32()
	if err != nil {
		return fmt.Errorf("bags: uint32: %w", err)
	}
	return nil
}

// decodeInt64 decodes one int64 value.
func decodeInt64(dec *types.Decoder) error {
	_, err := dec.ReadInt64()
	if err != nil {
		return fmt.Errorf("bags: int64: %w", err)
	}
	return nil
}

// decodeUint64 decodes one uint64 value.
func decodeUint64(dec *types.Decoder) error {
	_, err := dec.ReadUint64()
	if err != nil {
		return fmt.Errorf("bags: uint64: %w", err)
	}
	return nil
}

// decodeFloat32 decodes one float32 value.
func decodeFloat32(dec *types.Decoder) error {
	_, err := dec.ReadFloat32()
	if err != nil {
		return fmt.Errorf("bags: float32: %w", err)
	}
	return nil
}

// decodeFloat64 decodes one float64 value.
func decodeFloat64(dec *types.Decoder) error {
	_, err := dec.ReadFloat64()
	if err != nil {
		return fmt.Errorf("bags: float64: %w", err)
	}
	return nil
}

// decodeNested decodes one nested message value.
func decodeNested(dec *types.Decoder, field types.Field, lookup map[string]types.IndividualTypeDescription, depth int) error {
	nested, ok := lookup[field.Type.NestedTypeName]
	if !ok {
		return fmt.Errorf("bags: missing nested type %q", field.Type.NestedTypeName)
	}
	err := decodeIndividual(dec, nested, lookup, depth+1)
	if err != nil {
		return err
	}
	return nil
}

// decodeStringScalar decodes one string with the registry bound when present.
func decodeStringScalar(dec *types.Decoder, field types.Field) error {
	if field.Type.StringCapacity == 0 {
		_, err := dec.ReadString()
		if err != nil {
			return fmt.Errorf("bags: string: %w", err)
		}
		return nil
	}
	bound, err := checkedStringBound(field.Type.StringCapacity)
	if err != nil {
		return err
	}
	_, err = dec.ReadBoundedString(bound)
	if err != nil {
		return fmt.Errorf("bags: string: %w", err)
	}
	return nil
}

// decodeWStringScalar decodes one wstring with the registry bound when present.
func decodeWStringScalar(dec *types.Decoder, field types.Field) error {
	if field.Type.StringCapacity == 0 {
		_, err := dec.ReadWString()
		if err != nil {
			return fmt.Errorf("bags: wstring: %w", err)
		}
		return nil
	}
	bound, err := checkedStringBound(field.Type.StringCapacity)
	if err != nil {
		return err
	}
	_, err = dec.ReadBoundedWString(bound)
	if err != nil {
		return fmt.Errorf("bags: wstring: %w", err)
	}
	return nil
}

// Marshal encodes the bag in deterministic replay order.
//
// Topics sort by name and messages sort by receive timestamp then topic
// then sequence. Raw XCDR1 bytes are copied verbatim so a parse plus
// marshal round trip is byte identical.
func (b *Bag) Marshal() ([]byte, error) {
	if b == nil {
		return nil, errors.New("bags: nil bag") //nolint:wrapcheck // new error, nothing to wrap
	}
	if b.storage != StorageSQLite3 && b.storage != StorageMCAP {
		return nil, fmt.Errorf("bags: unknown storage id %q", b.storage)
	}
	topics := b.Topics()
	err := validateMarshalTopics(topics)
	if err != nil {
		return nil, err
	}
	msgs := b.OrderedMessages()
	err = b.validateMarshalMessages(msgs)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 64)
	out = append(out, magic0, magic1, magic2, magic3)
	var tmp [8]byte
	binary.LittleEndian.PutUint16(tmp[0:2], wireVersion)
	binary.LittleEndian.PutUint16(tmp[2:4], 0)
	out = append(out, tmp[0:4]...)
	out = appendString(out, b.storage)
	out = appendU32(out, uint32(len(topics))) //nolint:gosec // G115: length checked against maxTopics below
	for i := range topics {
		out = appendTopic(out, topics[i])
	}
	out = appendU32(out, uint32(len(msgs))) //nolint:gosec // G115: length checked against maxMessages below
	for i := range msgs {
		out = appendMessage(out, msgs[i])
	}
	return out, nil
}

// validateMarshalTopics checks topics for marshal.
func validateMarshalTopics(topics []Topic) error {
	for i := range topics {
		err := topics[i].Validate()
		if err != nil {
			return fmt.Errorf("bags: marshal topic %q: %w", topics[i].Name, err)
		}
	}
	return nil
}

// validateMarshalMessages checks messages for marshal.
func (b *Bag) validateMarshalMessages(msgs []Message) error {
	for i := range msgs {
		err := msgs[i].Validate()
		if err != nil {
			return fmt.Errorf("bags: marshal message %d: %w", i, err)
		}
		if _, ok := b.topics[msgs[i].Topic]; !ok {
			return fmt.Errorf("bags: marshal message %d: unknown topic %q", i, msgs[i].Topic)
		}
	}
	return nil
}

// appendU32 appends one little endian u32.
func appendU32(out []byte, v uint32) []byte {
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], v)
	return append(out, tmp[:]...)
}

// appendU64 appends one little endian u64.
func appendU64(out []byte, v uint64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], v)
	return append(out, tmp[:]...)
}

// appendI64 appends one little endian int64.
func appendI64(out []byte, v int64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], uint64(v)) //nolint:gosec // G115: bit preserving reinterpret for wire encoding
	return append(out, tmp[:]...)
}

// appendString appends a u32 length plus raw bytes.
func appendString(out []byte, s string) []byte {
	out = appendU32(out, uint32(len(s))) //nolint:gosec // G115: lengths validated against caps before marshal
	return append(out, s...)
}

// appendBytes appends a u32 length plus raw bytes.
func appendBytes(out, p []byte) []byte {
	out = appendU32(out, uint32(len(p))) //nolint:gosec // G115: lengths validated against caps before marshal
	return append(out, p...)
}

// appendTopic appends one topic record.
func appendTopic(out []byte, t Topic) []byte {
	out = appendU32(out, t.ID)
	out = appendString(out, t.Name)
	out = appendString(out, t.Type)
	out = appendString(out, t.Format)
	out = appendU32(out, uint32(len(t.OfferedQoS))) //nolint:gosec // G115: length checked by Validate
	for i := range t.OfferedQoS {
		out = appendString(out, t.OfferedQoS[i])
	}
	out = appendString(out, t.TypeHash)
	return out
}

// appendMessage appends one message record.
func appendMessage(out []byte, m Message) []byte {
	out = appendString(out, m.Topic)
	out = appendU64(out, m.Sequence)
	out = appendI64(out, m.ReceiveNs)
	out = appendI64(out, m.SendNs)
	out = appendString(out, m.Format)
	out = appendBytes(out, m.Data)
	return out
}

// Parse decodes one exchange buffer with bounds checks.
//
// Every length prefix is checked against the remaining bytes and against
// allocation caps before any slice is taken. Truncated, empty, and
// malformed input records one drop in m and returns an error. It never
// panics for any input bytes.
func Parse(data []byte, m *Metrics) (*Bag, error) {
	if len(data) == 0 {
		m.RecordDrop()
		return nil, errors.New("bags: empty buffer") //nolint:wrapcheck // new error, nothing to wrap
	}
	off := 0
	storage, err := parsePrefix(data, &off, m)
	if err != nil {
		return nil, err
	}
	bag, err := New(storage)
	if err != nil {
		m.RecordDrop()
		return nil, err
	}
	err = parseTopics(data, &off, bag, m)
	if err != nil {
		return nil, err
	}
	err = parseMessages(data, &off, bag, m)
	if err != nil {
		return nil, err
	}
	if off != len(data) {
		m.RecordDrop()
		return nil, fmt.Errorf("bags: %d trailing bytes", len(data)-off)
	}
	return bag, nil
}

// parsePrefix reads magic plus version plus storage id.
func parsePrefix(data []byte, off *int, m *Metrics) (string, error) {
	err := checkMagic(data, off, m)
	if err != nil {
		return "", err
	}
	err = checkVersion(data, off, m)
	if err != nil {
		return "", err
	}
	storage, err := readString(data, off, maxStorageLen, m)
	if err != nil {
		return "", fmt.Errorf("bags: storage: %w", err)
	}
	if storage != StorageSQLite3 && storage != StorageMCAP {
		m.RecordDrop()
		return "", fmt.Errorf("bags: unknown storage id %q", storage)
	}
	return storage, nil
}

// checkMagic validates the 4 byte magic.
func checkMagic(data []byte, off *int, m *Metrics) error {
	magic, err := takeBytes(data, off, 4, m)
	if err != nil {
		return err
	}
	if magic[0] != magic0 || magic[1] != magic1 || magic[2] != magic2 || magic[3] != magic3 {
		m.RecordDrop()
		return fmt.Errorf("bags: bad magic 0x%02x%02x%02x%02x", magic[0], magic[1], magic[2], magic[3])
	}
	return nil
}

// checkVersion validates version plus reserved fields.
func checkVersion(data []byte, off *int, m *Metrics) error {
	ver, err := readU16(data, off, m)
	if err != nil {
		return err
	}
	if ver != wireVersion {
		m.RecordDrop()
		return fmt.Errorf("bags: unsupported version %d", ver)
	}
	reserved, err := readU16(data, off, m)
	if err != nil {
		return err
	}
	if reserved != 0 {
		m.RecordDrop()
		return fmt.Errorf("bags: nonzero reserved 0x%04x", reserved)
	}
	return nil
}

// parseTopics reads the topic table into the bag.
func parseTopics(data []byte, off *int, bag *Bag, m *Metrics) error {
	count, err := readU32(data, off, m)
	if err != nil {
		return fmt.Errorf("bags: topic count: %w", err)
	}
	if count > maxTopics {
		m.RecordDrop()
		return fmt.Errorf("bags: topic count %d above cap %d", count, maxTopics)
	}
	for range count {
		err = parseOneTopic(data, off, bag, m)
		if err != nil {
			return err
		}
	}
	return nil
}

// parseOneTopic reads plus validates plus stores one topic record.
func parseOneTopic(data []byte, off *int, bag *Bag, m *Metrics) error {
	id, err := readU32(data, off, m)
	if err != nil {
		return fmt.Errorf("bags: id: %w", err)
	}
	name, err := readString(data, off, maxStringField, m)
	if err != nil {
		return fmt.Errorf("bags: name: %w", err)
	}
	typ, err := readString(data, off, maxStringField, m)
	if err != nil {
		return fmt.Errorf("bags: type: %w", err)
	}
	format, err := readString(data, off, maxFormatLen, m)
	if err != nil {
		return fmt.Errorf("bags: format: %w", err)
	}
	qos, err := parseQoS(data, off, m)
	if err != nil {
		return err
	}
	hash, err := readString(data, off, maxHashLen, m)
	if err != nil {
		return fmt.Errorf("bags: hash: %w", err)
	}
	topic := Topic{
		ID:         id,
		Name:       name,
		Type:       typ,
		Format:     format,
		OfferedQoS: qos,
		TypeHash:   hash,
	}
	err = bag.AddTopic(topic)
	if err != nil {
		m.RecordDrop()
		return err
	}
	return nil
}

// parseQoS reads the QoS vector for one topic.
func parseQoS(data []byte, off *int, m *Metrics) ([]string, error) {
	qosCount, err := readU32(data, off, m)
	if err != nil {
		return nil, fmt.Errorf("bags: qos count: %w", err)
	}
	if qosCount > maxQoSEntries {
		m.RecordDrop()
		return nil, fmt.Errorf("bags: qos count %d above cap", qosCount)
	}
	qos := make([]string, 0, qosCount)
	for range qosCount {
		s, err := readString(data, off, maxQoSEntryLen, m)
		if err != nil {
			return nil, fmt.Errorf("bags: qos entry: %w", err)
		}
		qos = append(qos, s)
	}
	return qos, nil
}

// parseMessages reads the message table into the bag.
func parseMessages(data []byte, off *int, bag *Bag, m *Metrics) error {
	count, err := readU32(data, off, m)
	if err != nil {
		return fmt.Errorf("bags: message count: %w", err)
	}
	if count > maxMessages {
		m.RecordDrop()
		return fmt.Errorf("bags: message count %d above cap %d", count, maxMessages)
	}
	for idx := range count {
		err = parseOneMessage(data, off, bag, m)
		if err != nil {
			return fmt.Errorf("bags: message %d: %w", idx, err)
		}
	}
	return nil
}

// parseOneMessage reads plus validates plus appends one message record.
func parseOneMessage(data []byte, off *int, bag *Bag, m *Metrics) error {
	topic, err := readString(data, off, maxStringField, m)
	if err != nil {
		return fmt.Errorf("bags: topic: %w", err)
	}
	seq, err := readU64(data, off, m)
	if err != nil {
		return fmt.Errorf("bags: sequence: %w", err)
	}
	recv, err := readI64(data, off, m)
	if err != nil {
		return fmt.Errorf("bags: receive timestamp: %w", err)
	}
	send, err := readI64(data, off, m)
	if err != nil {
		return fmt.Errorf("bags: send timestamp: %w", err)
	}
	format, err := readString(data, off, maxFormatLen, m)
	if err != nil {
		return fmt.Errorf("bags: format: %w", err)
	}
	payload, err := readBytes(data, off, maxDataBytes, m)
	if err != nil {
		return fmt.Errorf("bags: data: %w", err)
	}
	msg := Message{
		Topic:     topic,
		Data:      payload,
		ReceiveNs: recv,
		SendNs:    send,
		Sequence:  seq,
		Format:    format,
	}
	err = msg.Validate()
	if err != nil {
		m.RecordDrop()
		return err
	}
	if _, ok := bag.topics[topic]; !ok {
		m.RecordDrop()
		return fmt.Errorf("bags: unknown topic %q", topic)
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	bag.messages = append(bag.messages, Message{
		Topic:     topic,
		Data:      cp,
		ReceiveNs: recv,
		SendNs:    send,
		Sequence:  seq,
		Format:    format,
	})
	return nil
}

// readU16 reads one little endian u16 with bounds checks.
func readU16(data []byte, off *int, m *Metrics) (uint16, error) {
	if *off < 0 || *off+2 > len(data) {
		m.RecordDrop()
		return 0, fmt.Errorf("bags: truncated need 2 have %d", len(data)-*off)
	}
	v := binary.LittleEndian.Uint16(data[*off : *off+2])
	*off += 2
	return v, nil
}

// readU32 reads one little endian u32 with bounds checks.
func readU32(data []byte, off *int, m *Metrics) (uint32, error) {
	if *off < 0 || *off+4 > len(data) {
		m.RecordDrop()
		return 0, fmt.Errorf("bags: truncated need 4 have %d", len(data)-*off)
	}
	v := binary.LittleEndian.Uint32(data[*off : *off+4])
	*off += 4
	return v, nil
}

// readU64 reads one little endian u64 with bounds checks.
func readU64(data []byte, off *int, m *Metrics) (uint64, error) {
	if *off < 0 || *off+8 > len(data) {
		m.RecordDrop()
		return 0, fmt.Errorf("bags: truncated need 8 have %d", len(data)-*off)
	}
	v := binary.LittleEndian.Uint64(data[*off : *off+8])
	*off += 8
	return v, nil
}

// readI64 reads one little endian int64 with bounds checks.
func readI64(data []byte, off *int, m *Metrics) (int64, error) {
	v, err := readU64(data, off, m)
	if err != nil {
		return 0, err
	}
	return int64(v), nil //nolint:gosec // G115: bit preserving reinterpret for wire decoding
}

// takeBytes takes n raw bytes with bounds checks.
func takeBytes(data []byte, off *int, n int, m *Metrics) ([]byte, error) {
	if n < 0 {
		m.RecordDrop()
		return nil, fmt.Errorf("bags: negative read size %d", n)
	}
	if *off < 0 || *off+n > len(data) {
		m.RecordDrop()
		return nil, fmt.Errorf("bags: truncated need %d have %d", n, len(data)-*off)
	}
	out := data[*off : *off+n]
	*off += n
	return out, nil
}

// readString reads a u32 length plus bytes with caps.
func readString(data []byte, off *int, capLen int, m *Metrics) (string, error) {
	n, err := readU32(data, off, m)
	if err != nil {
		return "", err
	}
	if n > uint32(capLen) { //nolint:gosec // G115: capLen is a small non-negative constant
		m.RecordDrop()
		return "", fmt.Errorf("bags: string length %d above cap %d", n, capLen)
	}
	raw, err := takeBytes(data, off, int(n), m)
	if err != nil {
		return "", err
	}
	cp := make([]byte, len(raw))
	copy(cp, raw)
	return string(cp), nil
}

// readBytes reads a u32 length plus bytes with caps.
func readBytes(data []byte, off *int, capLen int, m *Metrics) ([]byte, error) {
	n, err := readU32(data, off, m)
	if err != nil {
		return nil, err
	}
	if n > uint32(capLen) { //nolint:gosec // G115: capLen is a small non-negative constant
		m.RecordDrop()
		return nil, fmt.Errorf("bags: bytes length %d above cap %d", n, capLen)
	}
	raw, err := takeBytes(data, off, int(n), m)
	if err != nil {
		return nil, err
	}
	cp := make([]byte, len(raw))
	copy(cp, raw)
	return cp, nil
}
