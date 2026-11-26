// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package tokens

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

// Spec message class ids for the stateless and volatile channels.
//
// Values follow OMG DDS-Security 1.2 section 7.4 and match the
// ddsi_security_msg.h header verified against the fleet container.
//
//nolint:gosec // G101: protocol class ids are constants, not credentials.
const (
	// ClassAuthRequest names the bootstrap auth request class.
	ClassAuthRequestMsg = "dds.sec.auth_request"
	// ClassAuthMsg names the handshake message class.
	ClassAuthMsg = "dds.sec.auth"
	// ClassParticipantTokens names participant crypto token exchange.
	ClassParticipantTokens = "dds.sec.participant_crypto_tokens"
	// ClassWriterTokens names datawriter crypto token exchange.
	ClassWriterTokens = "dds.sec.datawriter_crypto_tokens"
	// ClassReaderTokens names datareader crypto token exchange.
	ClassReaderTokens = "dds.sec.datareader_crypto_tokens"
)

// Spec token class ids for PKI-DH and crypto tokens.
//
// Values follow OMG DDS-Security 1.2 section 10.5 and match the
// auth_tokens.h, ac_tokens.h, and crypto_tokens.h headers verified
// against the fleet container.
//
//nolint:gosec // G101: protocol class ids are constants, not credentials.
const (
	// TokenIdentity names the IdentityToken class.
	TokenIdentity = "DDS:Auth:PKI-DH:1.0"
	// TokenPermissions names the PermissionsToken class.
	TokenPermissions = "DDS:Access:Permissions:1.0"
	// TokenCrypto names the crypto token class.
	TokenCrypto = "DDS:Crypto:AES_GCM_GMAC"
)

// Spec handshake property names for DataHolder binary properties.
//
// Values match auth_tokens.h verified against the fleet container.
const (
	// PropCID carries the DER identity certificate.
	PropCID = "c.id"
	// PropCPerm carries the permissions document.
	PropCPerm = "c.perm"
	// PropCPData carries the serialized participant data.
	PropCPData = "c.pdata"
	// PropCDSignAlgo carries the signature algorithm name.
	PropCDSignAlgo = "c.dsign_algo"
	// PropCKAgreeAlgo carries the key agreement algorithm name.
	PropCKAgreeAlgo = "c.kagree_algo"
	// PropDH1 carries the initiator ECDHE public key.
	PropDH1 = "dh1"
	// PropDH2 carries the responder ECDHE public key.
	PropDH2 = "dh2"
	// PropChallenge1 carries the initiator nonce.
	PropChallenge1 = "challenge1"
	// PropChallenge2 carries the responder nonce.
	PropChallenge2 = "challenge2"
	// PropSignature carries the handshake signature.
	PropSignature = "signature"
	// PropFutureChallenge carries the bootstrap nonce.
	PropFutureChallenge = "future_challenge"
	// PropKeymat carries serialized key material in crypto tokens.
	PropKeymat = "dds.cryp.keymat"
)

// Spec identity token property names for text properties.
const (
	// PropCertSN carries the identity certificate subject name.
	PropCertSN = "dds.cert.sn"
	// PropCertAlgo carries the identity certificate algorithm.
	PropCertAlgo = "dds.cert.algo"
	// PropCaSN carries the identity CA subject name.
	PropCaSN = "dds.ca.sn"
	// PropCaAlgo carries the identity CA algorithm.
	PropCaAlgo = "dds.ca.algo"
)

// Spec algorithm names for handshake tokens.
//
// Values match the CycloneDDS authentication plugin verified against
// the fleet container.
const (
	// DSignAlgoECDSA selects ECDSA with SHA-256 signatures.
	DSignAlgoECDSA = "ECDSA-SHA256"
	// KAgreeECDH selects ECDH over prime256v1 key agreement.
	KAgreeECDH = "ECDH+prime256v1-CEUM"
	// CertAlgoECPrime256v1 names EC prime256v1 identity certificates.
	CertAlgoECPrime256v1 = "EC-prime256v1"
)

// Caps for spec-exact decoding.
const (
	// maxSpecString caps one decoded spec string without NUL.
	maxSpecString = 4096
	// maxSpecBytes caps one decoded spec binary value.
	maxSpecBytes = 16384
	// maxSpecHolders caps one decoded DataHolder sequence.
	maxSpecHolders = 8
	// maxSpecProps caps one decoded property sequence.
	maxSpecProps = 64
)

// Property is one DDS-Security text property.
type Property struct {
	// Name is the property name.
	Name string
	// Value is the property value.
	Value string
}

// BinaryProperty is one DDS-Security binary property.
type BinaryProperty struct {
	// Name is the property name.
	Name string
	// Value is the property octets.
	Value []byte
}

// DataHolder is one DDS-Security DataHolder per section 7.4.
//
// Properties carry text and binary properties carry octets. The
// propagate flag is transport local and never appears on the wire:
// CycloneDDS omits it during serialization and assumes true when
// parsing, so this type has no propagate field.
type DataHolder struct {
	// ClassID is the token class id.
	ClassID string
	// Properties holds text properties, usually empty for handshake tokens.
	Properties []Property
	// BinaryProperties holds binary properties.
	BinaryProperties []BinaryProperty
}

// specWriter renders big-endian CDR per section 7.4.
type specWriter struct {
	// out holds the encoded bytes.
	out []byte
}

// align pads the output to a 4 byte boundary.
func (w *specWriter) align() {
	for len(w.out)%4 != 0 {
		w.out = append(w.out, 0x00)
	}
}

// align8 pads the output to an 8 byte boundary.
func (w *specWriter) align8() {
	for len(w.out)%8 != 0 {
		w.out = append(w.out, 0x00)
	}
}

// u32 appends one big-endian uint32.
func (w *specWriter) u32(value uint32) {
	w.align()

	var raw [4]byte

	binary.BigEndian.PutUint32(raw[:], value)
	w.out = append(w.out, raw[:]...)
}

// u64 appends one big-endian uint64.
func (w *specWriter) u64(value uint64) {
	w.align8()

	var raw [8]byte

	binary.BigEndian.PutUint64(raw[:], value)
	w.out = append(w.out, raw[:]...)
}

// str appends one CDR string with NUL and no trailing pad.
func (w *specWriter) str(value string) error {
	if len(value) > maxSpecString {
		return fmt.Errorf("tokens: spec string length %d above %d: %w", len(value), maxSpecString, ErrTokenTooLarge)
	}

	length := uint32(len(value) + 1) //nolint:gosec // G115: length guarded by maxSpecString

	w.u32(length)
	w.out = append(w.out, value...)
	w.out = append(w.out, 0x00)

	return nil
}

// bytes appends one length prefixed octet vector with no trailing pad.
func (w *specWriter) bytes(value []byte) error {
	if len(value) > maxSpecBytes {
		return fmt.Errorf("tokens: spec bytes length %d above %d: %w", len(value), maxSpecBytes, ErrTokenTooLarge)
	}

	w.u32(uint32(len(value))) //nolint:gosec // G115: length guarded by maxSpecBytes
	w.out = append(w.out, value...)

	return nil
}

// guid appends one 16 byte participant or endpoint GUID.
func (w *specWriter) guid(value [16]byte) {
	w.out = append(w.out, value[:]...)
}

// specReader parses big-endian or little-endian CDR fail-closed.
type specReader struct {
	// raw holds the input bytes.
	raw []byte
	// off is the current offset.
	off int
	// little selects little-endian field order.
	little bool
}

// align advances past pad bytes to a 4 byte boundary.
func (r *specReader) align() error {
	next := (r.off + 3) & ^3
	if next > len(r.raw) {
		return fmt.Errorf("tokens: spec align overruns: %w", ErrTokenTruncated)
	}

	r.off = next

	return nil
}

// align8 advances past pad bytes to an 8 byte boundary.
func (r *specReader) align8() error {
	next := (r.off + 7) & ^7
	if next > len(r.raw) {
		return fmt.Errorf("tokens: spec align8 overruns: %w", ErrTokenTruncated)
	}

	r.off = next

	return nil
}

// readUint reads n bytes as uint64 after aligning.
func (r *specReader) readUint(n int, use8 bool) (uint64, error) {
	var alignErr error
	if use8 {
		alignErr = r.align8()
	} else {
		alignErr = r.align()
	}
	if alignErr != nil {
		return 0, alignErr
	}

	if len(r.raw)-r.off < n {
		return 0, fmt.Errorf("tokens: spec uint need %d have %d: %w", n, len(r.raw)-r.off, ErrTokenTruncated)
	}

	value := decodeUint(r.raw[r.off:r.off+n], r.little, use8)
	r.off += n

	return value, nil
}

// decodeUint decodes n bytes in the given order without nesting.
func decodeUint(raw []byte, little, use8 bool) uint64 {
	if little && use8 {
		return binary.LittleEndian.Uint64(raw)
	}

	if little {
		return uint64(binary.LittleEndian.Uint32(raw))
	}

	if use8 {
		return binary.BigEndian.Uint64(raw)
	}

	return uint64(binary.BigEndian.Uint32(raw))
}

// u32 reads one uint32 in the reader order.
func (r *specReader) u32() (uint32, error) {
	value, err := r.readUint(4, false)
	if err != nil {
		return 0, err
	}

	return uint32(value), nil //nolint:gosec // G115: readUint with n=4 never exceeds uint32
}

// u64 reads one uint64 in the reader order.
func (r *specReader) u64() (uint64, error) {
	value, err := r.readUint(8, true)
	if err != nil {
		return 0, err
	}

	return value, nil
}

// str reads one CDR string in the reader order.
func (r *specReader) str() (string, error) {
	length, err := r.u32()
	if err != nil {
		return "", err
	}

	if length < 1 || length > maxSpecString+1 {
		return "", fmt.Errorf("tokens: spec string length %d: %w", length, ErrTokenInvalid)
	}

	need := int(length)
	if len(r.raw)-r.off < need {
		return "", fmt.Errorf("tokens: spec string need %d have %d: %w", need, len(r.raw)-r.off, ErrTokenTruncated)
	}

	body := r.raw[r.off : r.off+need]
	if body[len(body)-1] != 0x00 {
		return "", fmt.Errorf("tokens: spec string misses NUL: %w", ErrTokenInvalid)
	}

	out := string(body[:len(body)-1])
	r.off += need

	return out, nil
}

// bytes reads one length prefixed octet vector in the reader order.
func (r *specReader) bytes() ([]byte, error) {
	length, err := r.u32()
	if err != nil {
		return nil, err
	}

	if length > maxSpecBytes {
		return nil, fmt.Errorf("tokens: spec bytes length %d above %d: %w", length, maxSpecBytes, ErrTokenTooLarge)
	}

	need := int(length)
	if len(r.raw)-r.off < need {
		return nil, fmt.Errorf("tokens: spec bytes need %d have %d: %w", need, len(r.raw)-r.off, ErrTokenTruncated)
	}

	out := append([]byte(nil), r.raw[r.off:r.off+need]...)
	r.off += need

	return out, nil
}

// guid reads one 16 byte GUID.
func (r *specReader) guid() ([16]byte, error) {
	var out [16]byte

	if len(r.raw)-r.off < 16 {
		return out, fmt.Errorf("tokens: spec guid need 16 have %d: %w", len(r.raw)-r.off, ErrTokenTruncated)
	}

	copy(out[:], r.raw[r.off:r.off+16])
	r.off += 16

	return out, nil
}

// EncodeDataHolder renders one DataHolder in big-endian CDR.
func EncodeDataHolder(holder DataHolder) ([]byte, error) {
	if len(holder.Properties) > maxSpecProps {
		return nil, fmt.Errorf("tokens: dataholder properties %d above %d: %w", len(holder.Properties), maxSpecProps, ErrTokenTooLarge)
	}

	if len(holder.BinaryProperties) > maxSpecProps {
		return nil, fmt.Errorf("tokens: dataholder binary properties %d above %d: %w", len(holder.BinaryProperties), maxSpecProps, ErrTokenTooLarge)
	}

	var w specWriter

	err := w.str(holder.ClassID)
	if err != nil {
		return nil, fmt.Errorf("tokens: dataholder class: %w", err)
	}

	w.u32(uint32(len(holder.Properties))) //nolint:gosec // G115: length guarded by maxSpecProps

	for i := range holder.Properties {
		err = w.str(holder.Properties[i].Name)
		if err != nil {
			return nil, fmt.Errorf("tokens: dataholder property %d name: %w", i, err)
		}

		err = w.str(holder.Properties[i].Value)
		if err != nil {
			return nil, fmt.Errorf("tokens: dataholder property %d value: %w", i, err)
		}
	}

	w.u32(uint32(len(holder.BinaryProperties))) //nolint:gosec // G115: length guarded by maxSpecProps

	for i := range holder.BinaryProperties {
		err = w.str(holder.BinaryProperties[i].Name)
		if err != nil {
			return nil, fmt.Errorf("tokens: dataholder binary property %d name: %w", i, err)
		}

		err = w.bytes(holder.BinaryProperties[i].Value)
		if err != nil {
			return nil, fmt.Errorf("tokens: dataholder binary property %d value: %w", i, err)
		}
	}

	return w.out, nil
}

// DecodeDataHolder parses one DataHolder in either byte order.
func DecodeDataHolder(raw []byte, little bool) (DataHolder, error) {
	var out DataHolder

	r := &specReader{raw: raw, little: little}

	class, err := r.str()
	if err != nil {
		return out, fmt.Errorf("tokens: dataholder class: %w", err)
	}

	propCount, err := r.u32()
	if err != nil {
		return out, fmt.Errorf("tokens: dataholder property count: %w", err)
	}

	if propCount > maxSpecProps {
		return out, fmt.Errorf("tokens: dataholder properties %d above %d: %w", propCount, maxSpecProps, ErrTokenTooLarge)
	}

	out.ClassID = class
	out.Properties = make([]Property, 0, propCount)

	err = decodeTextProperties(r, &out, propCount)
	if err != nil {
		return out, err
	}

	binCount, err := r.u32()
	if err != nil {
		return out, fmt.Errorf("tokens: dataholder binary count: %w", err)
	}

	if binCount > maxSpecProps {
		return out, fmt.Errorf("tokens: dataholder binary properties %d above %d: %w", binCount, maxSpecProps, ErrTokenTooLarge)
	}

	out.BinaryProperties = make([]BinaryProperty, 0, binCount)

	err = decodeBinaryProperties(r, &out, binCount)
	if err != nil {
		return out, err
	}

	if r.off != len(raw) {
		return out, fmt.Errorf("tokens: dataholder has %d trailing bytes: %w", len(raw)-r.off, ErrTokenInvalid)
	}

	return out, nil
}

// decodeTextProperties appends propCount text properties to out.
func decodeTextProperties(r *specReader, out *DataHolder, propCount uint32) error {
	for range propCount {
		name, err := r.str()
		if err != nil {
			return fmt.Errorf("tokens: dataholder property name: %w", err)
		}

		value, err := r.str()
		if err != nil {
			return fmt.Errorf("tokens: dataholder property value: %w", err)
		}

		out.Properties = append(out.Properties, Property{Name: name, Value: value})
	}

	return nil
}

// decodeBinaryProperties appends binCount binary properties to out.
func decodeBinaryProperties(r *specReader, out *DataHolder, binCount uint32) error {
	for range binCount {
		name, err := r.str()
		if err != nil {
			return fmt.Errorf("tokens: dataholder binary name: %w", err)
		}

		value, err := r.bytes()
		if err != nil {
			return fmt.Errorf("tokens: dataholder binary value: %w", err)
		}

		out.BinaryProperties = append(out.BinaryProperties, BinaryProperty{Name: name, Value: value})
	}

	return nil
}

// FindBinaryProperty returns the first binary property with the given name.
func FindBinaryProperty(holder DataHolder, name string) ([]byte, bool) {
	for i := range holder.BinaryProperties {
		if holder.BinaryProperties[i].Name == name {
			return holder.BinaryProperties[i].Value, true
		}
	}

	return nil, false
}

// FindProperty returns the first text property with the given name.
func FindProperty(holder DataHolder, name string) (string, bool) {
	for i := range holder.Properties {
		if holder.Properties[i].Name == name {
			return holder.Properties[i].Value, true
		}
	}

	return "", false
}

// SerializeBinaryItems renders name plus octet pairs for hashing and signing.
//
// The layout matches DDS_Security_Serialize_BinaryPropertyArray: a
// big-endian count followed by per-item name string plus octet
// sequence, with 4 byte alignment before each length word.
func SerializeBinaryItems(items []BinaryProperty) ([]byte, error) {
	if len(items) > maxSpecProps {
		return nil, fmt.Errorf("tokens: binary items %d above %d: %w", len(items), maxSpecProps, ErrTokenTooLarge)
	}

	var w specWriter

	w.u32(uint32(len(items))) //nolint:gosec // G115: length guarded by maxSpecProps

	for i := range items {
		err := w.str(items[i].Name)
		if err != nil {
			return nil, fmt.Errorf("tokens: binary item %d name: %w", i, err)
		}

		err = w.bytes(items[i].Value)
		if err != nil {
			return nil, fmt.Errorf("tokens: binary item %d value: %w", i, err)
		}
	}

	return w.out, nil
}

// HashBinaryItems returns SHA-256 over the serialized binary items.
//
// Hash_c1 and hash_c2 in handshake signatures cover the five c.*
// properties in token order, matching the CycloneDDS computation.
func HashBinaryItems(items []BinaryProperty) ([32]byte, error) {
	var out [32]byte

	raw, err := SerializeBinaryItems(items)
	if err != nil {
		return out, err
	}

	return sha256.Sum256(raw), nil
}

// GenericMessage is one ParticipantGenericMessage per section 7.4.
type GenericMessage struct {
	// SourceGUID names the sender stateless or volatile writer.
	SourceGUID [16]byte
	// SourceSeq is the sender writer sequence for duplicate suppression.
	SourceSeq int64
	// RelatedSourceGUID echoes the inbound source for replies.
	RelatedSourceGUID [16]byte
	// RelatedSeq echoes the inbound sequence for replies.
	RelatedSeq int64
	// DestinationParticipantGUID routes the sample, zero means broadcast.
	DestinationParticipantGUID [16]byte
	// DestinationEndpointGUID routes volatile tokens, zero for handshake.
	DestinationEndpointGUID [16]byte
	// SourceEndpointGUID names the sender endpoint, zero for handshake.
	SourceEndpointGUID [16]byte
	// ClassID selects the auth or crypto exchange class.
	ClassID string
	// Data holds the token sequence, normally one token.
	Data []DataHolder
}

// EncodeGenericMessage renders one GenericMessage in big-endian CDR.
func EncodeGenericMessage(message GenericMessage) ([]byte, error) {
	if len(message.Data) > maxSpecHolders {
		return nil, fmt.Errorf("tokens: generic message holders %d above %d: %w", len(message.Data), maxSpecHolders, ErrTokenTooLarge)
	}

	var w specWriter

	w.guid(message.SourceGUID)
	w.u64(uint64(message.SourceSeq)) //nolint:gosec // G115: sequence guarded non-negative by construction
	w.guid(message.RelatedSourceGUID)
	w.u64(uint64(message.RelatedSeq)) //nolint:gosec // G115: sequence guarded non-negative by construction
	w.guid(message.DestinationParticipantGUID)
	w.guid(message.DestinationEndpointGUID)
	w.guid(message.SourceEndpointGUID)

	err := w.str(message.ClassID)
	if err != nil {
		return nil, fmt.Errorf("tokens: generic message class: %w", err)
	}

	w.u32(uint32(len(message.Data))) //nolint:gosec // G115: length guarded by maxSpecHolders

	out := w.out

	for i := range message.Data {
		encoded, err := EncodeDataHolder(message.Data[i])
		if err != nil {
			return nil, fmt.Errorf("tokens: generic message holder %d: %w", i, err)
		}

		out = append(out, encoded...)
	}

	return out, nil
}

// DecodeGenericMessage parses one GenericMessage body in either byte order.
func DecodeGenericMessage(raw []byte, little bool) (GenericMessage, error) {
	var out GenericMessage

	r := &specReader{raw: raw, little: little}

	head, err := readGenericHeaders(r)
	if err != nil {
		return out, err
	}

	out.SourceGUID = head.source
	out.SourceSeq = int64(head.sourceSeq) //nolint:gosec // G115: sequence round-trips through uint64
	out.RelatedSourceGUID = head.relatedSource
	out.RelatedSeq = int64(head.relatedSeq) //nolint:gosec // G115: sequence round-trips through uint64
	out.DestinationParticipantGUID = head.destPart
	out.DestinationEndpointGUID = head.destEndp
	out.SourceEndpointGUID = head.sourceEndp
	out.ClassID = head.class
	out.Data = make([]DataHolder, 0, head.count)

	err = decodeGenericHolders(r, raw, &out, head.count)
	if err != nil {
		return out, err
	}

	return out, nil
}

// genericHeaders holds the decoded GenericMessage fixed header.
type genericHeaders struct {
	source        [16]byte
	sourceSeq     uint64
	relatedSource [16]byte
	relatedSeq    uint64
	destPart      [16]byte
	destEndp      [16]byte
	sourceEndp    [16]byte
	class         string
	count         uint32
}

// decodeGenericHolders appends count holders and checks trailing bytes.
func decodeGenericHolders(r *specReader, raw []byte, out *GenericMessage, count uint32) error {
	for range count {
		holder, next, err := decodeDataHolderAt(r)
		if err != nil {
			return err
		}

		_ = next
		out.Data = append(out.Data, holder)
	}

	if r.off != len(raw) {
		return fmt.Errorf("tokens: generic message has %d trailing bytes: %w", len(raw)-r.off, ErrTokenInvalid)
	}

	return nil
}

// readGenericHeaders reads the fixed GenericMessage header fields.
func readGenericHeaders(r *specReader) (genericHeaders, error) {
	var head genericHeaders

	var err error

	head.source, err = r.guid()
	if err != nil {
		return head, fmt.Errorf("tokens: generic message source: %w", err)
	}

	head.sourceSeq, err = r.u64()
	if err != nil {
		return head, fmt.Errorf("tokens: generic message source seq: %w", err)
	}

	head.relatedSource, err = r.guid()
	if err != nil {
		return head, fmt.Errorf("tokens: generic message related source: %w", err)
	}

	head.relatedSeq, err = r.u64()
	if err != nil {
		return head, fmt.Errorf("tokens: generic message related seq: %w", err)
	}

	head.destPart, head.destEndp, head.sourceEndp, head.class, head.count, err = readGenericTail(r)
	if err != nil {
		return head, err
	}

	return head, nil
}

// readGenericTail reads destination guids, class, and count.
func readGenericTail(r *specReader) ([16]byte, [16]byte, [16]byte, string, uint32, error) {
	var destPart, destEndp, sourceEndp [16]byte

	var class string

	var count uint32

	var err error

	destPart, err = r.guid()
	if err != nil {
		return destPart, destEndp, sourceEndp, class, count, fmt.Errorf("tokens: generic message destination: %w", err)
	}

	destEndp, err = r.guid()
	if err != nil {
		return destPart, destEndp, sourceEndp, class, count, fmt.Errorf("tokens: generic message destination endpoint: %w", err)
	}

	sourceEndp, err = r.guid()
	if err != nil {
		return destPart, destEndp, sourceEndp, class, count, fmt.Errorf("tokens: generic message source endpoint: %w", err)
	}

	class, err = r.str()
	if err != nil {
		return destPart, destEndp, sourceEndp, class, count, fmt.Errorf("tokens: generic message class: %w", err)
	}

	count, err = r.u32()
	if err != nil {
		return destPart, destEndp, sourceEndp, class, count, fmt.Errorf("tokens: generic message count: %w", err)
	}

	if count > maxSpecHolders {
		return destPart, destEndp, sourceEndp, class, count, fmt.Errorf("tokens: generic message holders %d above %d: %w", count, maxSpecHolders, ErrTokenTooLarge)
	}

	return destPart, destEndp, sourceEndp, class, count, nil
}

// decodeDataHolderAt parses one DataHolder at the reader offset.
func decodeDataHolderAt(r *specReader) (DataHolder, int, error) {
	var out DataHolder

	start := r.off

	class, err := r.str()
	if err != nil {
		return out, 0, fmt.Errorf("tokens: dataholder class: %w", err)
	}

	propCount, err := r.u32()
	if err != nil {
		return out, 0, fmt.Errorf("tokens: dataholder property count: %w", err)
	}

	if propCount > maxSpecProps {
		return out, 0, fmt.Errorf("tokens: dataholder properties %d above %d: %w", propCount, maxSpecProps, ErrTokenTooLarge)
	}

	out.ClassID = class
	out.Properties = make([]Property, 0, propCount)

	for range propCount {
		name, err := r.str()
		if err != nil {
			return out, 0, fmt.Errorf("tokens: dataholder property name: %w", err)
		}

		value, err := r.str()
		if err != nil {
			return out, 0, fmt.Errorf("tokens: dataholder property value: %w", err)
		}

		out.Properties = append(out.Properties, Property{Name: name, Value: value})
	}

	binCount, err := r.u32()
	if err != nil {
		return out, 0, fmt.Errorf("tokens: dataholder binary count: %w", err)
	}

	if binCount > maxSpecProps {
		return out, 0, fmt.Errorf("tokens: dataholder binary properties %d above %d: %w", binCount, maxSpecProps, ErrTokenTooLarge)
	}

	out.BinaryProperties = make([]BinaryProperty, 0, binCount)

	for range binCount {
		name, err := r.str()
		if err != nil {
			return out, 0, fmt.Errorf("tokens: dataholder binary name: %w", err)
		}

		value, err := r.bytes()
		if err != nil {
			return out, 0, fmt.Errorf("tokens: dataholder binary value: %w", err)
		}

		out.BinaryProperties = append(out.BinaryProperties, BinaryProperty{Name: name, Value: value})
	}

	return out, r.off - start, nil
}

// CDR encapsulation identifiers for GenericMessage payloads.
const (
	// cdrHeaderSize is the 4 byte encapsulation prefix.
	cdrHeaderSize = 4
)

// EncodeGenericPayload frames one GenericMessage with a big-endian CDR header.
func EncodeGenericPayload(message GenericMessage) ([]byte, error) {
	body, err := EncodeGenericMessage(message)
	if err != nil {
		return nil, err
	}

	out := []byte{0x00, 0x00, 0x00, 0x00}

	return append(out, body...), nil
}

// DecodeGenericPayload parses one framed GenericMessage.
//
// Big-endian and little-endian encapsulations are both accepted so
// native-order peers such as CycloneDDS parse; any other identifier
// fails closed.
func DecodeGenericPayload(raw []byte) (GenericMessage, error) {
	var out GenericMessage

	if len(raw) < cdrHeaderSize {
		return out, fmt.Errorf("tokens: generic payload need %d have %d: %w", cdrHeaderSize, len(raw), ErrTokenTruncated)
	}

	var little bool

	switch {
	case raw[0] == 0x00 && raw[1] == 0x00 && raw[2] == 0x00 && raw[3] == 0x00:
		little = false
	case raw[0] == 0x00 && raw[1] == 0x01 && raw[2] == 0x00 && raw[3] == 0x00:
		little = true
	default:
		return out, fmt.Errorf("tokens: generic payload encapsulation % X is not CDR: %w", raw[:4], ErrTokenInvalid)
	}

	message, err := DecodeGenericMessage(raw[cdrHeaderSize:], little)
	if err != nil {
		return out, fmt.Errorf("tokens: generic payload body: %w", err)
	}

	return message, nil
}

// BuildAuthRequestToken renders an AuthRequestMessageToken DataHolder.
func BuildAuthRequestToken(futureChallenge [32]byte) DataHolder {
	return DataHolder{
		ClassID: TokenAuthReq,
		BinaryProperties: []BinaryProperty{
			{Name: PropFutureChallenge, Value: append([]byte(nil), futureChallenge[:]...)},
		},
	}
}

// ParseAuthRequestToken extracts the future challenge from a DataHolder.
func ParseAuthRequestToken(holder DataHolder) ([32]byte, error) {
	var out [32]byte

	if holder.ClassID != TokenAuthReq {
		return out, fmt.Errorf("tokens: auth request class %q: %w", holder.ClassID, ErrTokenInvalid)
	}

	value, ok := FindBinaryProperty(holder, PropFutureChallenge)
	if !ok {
		return out, fmt.Errorf("tokens: auth request misses future_challenge: %w", ErrTokenInvalid)
	}

	if len(value) != 32 {
		return out, fmt.Errorf("tokens: auth request challenge need 32 have %d: %w", len(value), ErrTokenInvalid)
	}

	copy(out[:], value)

	return out, nil
}

// BuildHandshakeRequest renders a HandshakeRequestMessageToken DataHolder.
//
// Properties follow the CycloneDDS order: c.id, c.perm, c.pdata,
// c.dsign_algo, c.kagree_algo, dh1, challenge1. The digest and hash
// inputs must use this order.
func BuildHandshakeRequest(token HandshakeRequestToken) (DataHolder, error) {
	if len(token.Challenge1) != 32 {
		return DataHolder{}, fmt.Errorf("tokens: handshake request challenge need 32: %w", ErrTokenInvalid)
	}

	return DataHolder{
		ClassID: TokenReq,
		BinaryProperties: []BinaryProperty{
			{Name: PropCID, Value: append([]byte(nil), token.IdentityCert...)},
			{Name: PropCPerm, Value: append([]byte(nil), token.Permissions...)},
			{Name: PropCPData, Value: append([]byte(nil), token.ParticipantData...)},
			{Name: PropCDSignAlgo, Value: []byte(token.SignAlgo)},
			{Name: PropCKAgreeAlgo, Value: []byte(token.KeyAgreeAlgo)},
			{Name: PropDH1, Value: append([]byte(nil), token.DH1...)},
			{Name: PropChallenge1, Value: append([]byte(nil), token.Challenge1[:]...)},
		},
	}, nil
}

// requiredBinary returns the named binary property or a missing error.
func requiredBinary(holder DataHolder, name, what string) ([]byte, error) {
	value, ok := FindBinaryProperty(holder, name)
	if !ok {
		return nil, fmt.Errorf("tokens: %s misses %s: %w", what, name, ErrTokenInvalid)
	}

	return value, nil
}

// requiredNonEmptyBinary returns the named property, refusing empty values.
func requiredNonEmptyBinary(holder DataHolder, name, what string) ([]byte, error) {
	value, err := requiredBinary(holder, name, what)
	if err != nil {
		return nil, err
	}

	if len(value) == 0 {
		return nil, fmt.Errorf("tokens: %s misses %s: %w", what, name, ErrTokenInvalid)
	}

	return value, nil
}

// requiredChallenge32 returns the named 32 byte challenge.
func requiredChallenge32(holder DataHolder, name, what string) ([]byte, error) {
	value, err := requiredBinary(holder, name, what)
	if err != nil {
		return nil, err
	}

	if len(value) != 32 {
		return nil, fmt.Errorf("tokens: %s challenge need 32: %w", what, ErrTokenInvalid)
	}

	return value, nil
}

// ParseHandshakeRequest extracts a request token from a DataHolder.
//
// The seven properties must be present; both algorithm names, dh1,
// and a 32 byte challenge1 must be non-empty. Identity, permissions,
// and participant data may be empty for legacy loopback peers; secure
// mode refuses those at the manager layer. Extra properties such as
// hash_c1 or ocsp_status are ignored for forward compatibility.
func ParseHandshakeRequest(holder DataHolder) (HandshakeRequestToken, error) {
	var out HandshakeRequestToken

	if holder.ClassID != TokenReq {
		return out, fmt.Errorf("tokens: handshake request class %q: %w", holder.ClassID, ErrTokenInvalid)
	}

	cert, err := requiredBinary(holder, PropCID, "handshake request")
	if err != nil {
		return out, err
	}

	perm, err := requiredBinary(holder, PropCPerm, "handshake request")
	if err != nil {
		return out, err
	}

	pdata, err := requiredBinary(holder, PropCPData, "handshake request")
	if err != nil {
		return out, err
	}

	dsign, err := requiredNonEmptyBinary(holder, PropCDSignAlgo, "handshake request")
	if err != nil {
		return out, err
	}

	kagree, err := requiredNonEmptyBinary(holder, PropCKAgreeAlgo, "handshake request")
	if err != nil {
		return out, err
	}

	dh1, err := requiredNonEmptyBinary(holder, PropDH1, "handshake request")
	if err != nil {
		return out, err
	}

	challenge, err := requiredChallenge32(holder, PropChallenge1, "handshake request")
	if err != nil {
		return out, err
	}

	out.Class = TokenReq
	copy(out.Challenge1[:], challenge)
	out.DH1 = dh1
	out.KeyAgreeAlgo = string(kagree)
	out.SignAlgo = string(dsign)
	out.IdentityCert = cert
	out.Permissions = perm
	out.ParticipantData = pdata

	return out, nil
}

// RequestHashItems returns the five c.* properties in hash order.
func RequestHashItems(token HandshakeRequestToken) []BinaryProperty {
	return []BinaryProperty{
		{Name: PropCID, Value: token.IdentityCert},
		{Name: PropCPerm, Value: token.Permissions},
		{Name: PropCPData, Value: token.ParticipantData},
		{Name: PropCDSignAlgo, Value: []byte(token.SignAlgo)},
		{Name: PropCKAgreeAlgo, Value: []byte(token.KeyAgreeAlgo)},
	}
}

// BuildHandshakeReply renders a HandshakeReplyMessageToken DataHolder.
//
// Property order matches CycloneDDS: c.id, c.perm, c.pdata,
// c.dsign_algo, c.kagree_algo, dh2, challenge1, challenge2,
// signature.
func BuildHandshakeReply(token HandshakeReplyToken) (DataHolder, error) {
	return DataHolder{
		ClassID: TokenReply,
		BinaryProperties: []BinaryProperty{
			{Name: PropCID, Value: append([]byte(nil), token.IdentityCert...)},
			{Name: PropCPerm, Value: append([]byte(nil), token.Permissions...)},
			{Name: PropCPData, Value: append([]byte(nil), token.ParticipantData...)},
			{Name: PropCDSignAlgo, Value: []byte(token.SignAlgo)},
			{Name: PropCKAgreeAlgo, Value: []byte(token.KeyAgreeAlgo)},
			{Name: PropDH2, Value: append([]byte(nil), token.DH2...)},
			{Name: PropChallenge1, Value: append([]byte(nil), token.Challenge1Echo[:]...)},
			{Name: PropChallenge2, Value: append([]byte(nil), token.Challenge2[:]...)},
			{Name: PropSignature, Value: append([]byte(nil), token.Signature...)},
		},
	}, nil
}

// ParseHandshakeReply extracts a reply token from a DataHolder.
//
// The nine properties must be present; both algorithm names, dh2,
// and both 32 byte challenges must be non-empty. Identity,
// permissions, participant data, and the signature may be empty for
// legacy loopback peers; secure mode refuses those at the manager
// layer. Extra properties are ignored for forward compatibility.
func ParseHandshakeReply(holder DataHolder) (HandshakeReplyToken, error) {
	var out HandshakeReplyToken

	if holder.ClassID != TokenReply {
		return out, fmt.Errorf("tokens: handshake reply class %q: %w", holder.ClassID, ErrTokenInvalid)
	}

	cert, err := requiredBinary(holder, PropCID, "handshake reply")
	if err != nil {
		return out, err
	}

	perm, err := requiredBinary(holder, PropCPerm, "handshake reply")
	if err != nil {
		return out, err
	}

	pdata, err := requiredBinary(holder, PropCPData, "handshake reply")
	if err != nil {
		return out, err
	}

	dsign, err := requiredNonEmptyBinary(holder, PropCDSignAlgo, "handshake reply")
	if err != nil {
		return out, err
	}

	kagree, err := requiredNonEmptyBinary(holder, PropCKAgreeAlgo, "handshake reply")
	if err != nil {
		return out, err
	}

	dh2, err := requiredNonEmptyBinary(holder, PropDH2, "handshake reply")
	if err != nil {
		return out, err
	}

	challenge1, err := requiredChallenge32(holder, PropChallenge1, "handshake reply")
	if err != nil {
		return out, err
	}

	challenge2, err := requiredChallenge32(holder, PropChallenge2, "handshake reply")
	if err != nil {
		return out, err
	}

	sig, err := requiredBinary(holder, PropSignature, "handshake reply")
	if err != nil {
		return out, err
	}

	out.Class = TokenReply
	copy(out.Challenge1Echo[:], challenge1)
	copy(out.Challenge2[:], challenge2)
	out.DH2 = dh2
	out.Signature = sig
	out.KeyAgreeAlgo = string(kagree)
	out.SignAlgo = string(dsign)
	out.IdentityCert = cert
	out.Permissions = perm
	out.ParticipantData = pdata

	return out, nil
}

// ReplyHashItems returns the five c.* properties in hash order.
func ReplyHashItems(token HandshakeReplyToken) []BinaryProperty {
	return []BinaryProperty{
		{Name: PropCID, Value: token.IdentityCert},
		{Name: PropCPerm, Value: token.Permissions},
		{Name: PropCPData, Value: token.ParticipantData},
		{Name: PropCDSignAlgo, Value: []byte(token.SignAlgo)},
		{Name: PropCKAgreeAlgo, Value: []byte(token.KeyAgreeAlgo)},
	}
}

// BuildHandshakeFinal renders a HandshakeFinalMessageToken DataHolder.
func BuildHandshakeFinal(token HandshakeFinalToken) (DataHolder, error) {
	return DataHolder{
		ClassID: TokenFinal,
		BinaryProperties: []BinaryProperty{
			{Name: PropChallenge1, Value: append([]byte(nil), token.Challenge1Echo[:]...)},
			{Name: PropChallenge2, Value: append([]byte(nil), token.Challenge2Echo[:]...)},
			{Name: PropSignature, Value: append([]byte(nil), token.Signature...)},
		},
	}, nil
}

// ParseHandshakeFinal extracts a final token from a DataHolder.
func ParseHandshakeFinal(holder DataHolder) (HandshakeFinalToken, error) {
	var out HandshakeFinalToken

	if holder.ClassID != TokenFinal {
		return out, fmt.Errorf("tokens: handshake final class %q: %w", holder.ClassID, ErrTokenInvalid)
	}

	challenge1, ok := FindBinaryProperty(holder, PropChallenge1)
	if !ok || len(challenge1) != 32 {
		return out, fmt.Errorf("tokens: handshake final challenge1 need 32: %w", ErrTokenInvalid)
	}

	challenge2, ok := FindBinaryProperty(holder, PropChallenge2)
	if !ok || len(challenge2) != 32 {
		return out, fmt.Errorf("tokens: handshake final challenge2 need 32: %w", ErrTokenInvalid)
	}

	sig, ok := FindBinaryProperty(holder, PropSignature)
	if !ok {
		return out, fmt.Errorf("tokens: handshake final misses signature: %w", ErrTokenInvalid)
	}

	out.Class = TokenFinal
	copy(out.Challenge1Echo[:], challenge1)
	copy(out.Challenge2Echo[:], challenge2)
	out.Signature = sig

	return out, nil
}

// IdentityToken carries the SPDP identity announcement per section 10.5.
type IdentityToken struct {
	// Class is the token class id, normally TokenIdentity.
	Class string
	// CertSN is the identity certificate subject name.
	CertSN string
	// CertAlgo is the identity certificate algorithm.
	CertAlgo string
	// CaSN is the identity CA subject name.
	CaSN string
	// CaAlgo is the identity CA algorithm.
	CaAlgo string
}

// BuildIdentityToken renders an IdentityToken DataHolder with text properties.
func BuildIdentityToken(token IdentityToken) (DataHolder, error) {
	class := token.Class
	if class == "" {
		class = TokenIdentity
	}

	return DataHolder{
		ClassID: class,
		Properties: []Property{
			{Name: PropCertSN, Value: token.CertSN},
			{Name: PropCertAlgo, Value: token.CertAlgo},
			{Name: PropCaSN, Value: token.CaSN},
			{Name: PropCaAlgo, Value: token.CaAlgo},
		},
	}, nil
}

// ParseIdentityToken extracts an identity token from a DataHolder.
func ParseIdentityToken(holder DataHolder) (IdentityToken, error) {
	var out IdentityToken

	if holder.ClassID != TokenIdentity {
		return out, fmt.Errorf("tokens: identity class %q: %w", holder.ClassID, ErrTokenInvalid)
	}

	certSN, ok := FindProperty(holder, PropCertSN)
	if !ok {
		return out, fmt.Errorf("tokens: identity misses cert sn: %w", ErrTokenInvalid)
	}

	certAlgo, ok := FindProperty(holder, PropCertAlgo)
	if !ok {
		return out, fmt.Errorf("tokens: identity misses cert algo: %w", ErrTokenInvalid)
	}

	caSN, ok := FindProperty(holder, PropCaSN)
	if !ok {
		return out, fmt.Errorf("tokens: identity misses ca sn: %w", ErrTokenInvalid)
	}

	caAlgo, ok := FindProperty(holder, PropCaAlgo)
	if !ok {
		return out, fmt.Errorf("tokens: identity misses ca algo: %w", ErrTokenInvalid)
	}

	out.Class = holder.ClassID
	out.CertSN = certSN
	out.CertAlgo = certAlgo
	out.CaSN = caSN
	out.CaAlgo = caAlgo

	return out, nil
}

// BuildPermissionsToken renders an empty PermissionsToken DataHolder.
func BuildPermissionsToken() DataHolder {
	return DataHolder{ClassID: TokenPermissions}
}

// Participant data PIDs for the minimal c.pdata announcement.
const (
	// pdataPIDGUID carries the participant key in c.pdata.
	pdataPIDGUID = 0x0050
	// pdataPIDSentinel terminates the c.pdata parameter list.
	pdataPIDSentinel = 0x0001
)

// EncodeParticipantData renders minimal participant data for c.pdata.
//
// The layout is a big-endian parameter list with the participant GUID
// PID carrying the 12 byte key plus 4 zero bytes, followed by the
// sentinel. It deserializes in the CycloneDDS participant data parser
// and satisfies the adjusted GUID key check.
func EncodeParticipantData(keyPrefix [12]byte) []byte {
	out := make([]byte, 0, 28)
	out = append(out, 0x00, pdataPIDGUID, 0x00, 0x10)
	out = append(out, keyPrefix[:]...)
	out = append(out, 0x00, 0x00, 0x00, 0x00)
	out = append(out, 0x00, pdataPIDSentinel, 0x00, 0x00)

	return out
}

// ParsePermissionsToken checks a permissions token class id.
func ParsePermissionsToken(holder DataHolder) error {
	if holder.ClassID != TokenPermissions {
		return fmt.Errorf("tokens: permissions class %q: %w", holder.ClassID, ErrTokenInvalid)
	}

	return nil
}

// BuildCryptoToken renders one KeyMaterial as a crypto token DataHolder.
func BuildCryptoToken(material KeyMaterial) (DataHolder, error) {
	encoded, err := EncodeKeyMaterial(material)
	if err != nil {
		return DataHolder{}, fmt.Errorf("tokens: crypto token key material: %w", err)
	}

	return DataHolder{
		ClassID: TokenCrypto,
		BinaryProperties: []BinaryProperty{
			{Name: PropKeymat, Value: encoded},
		},
	}, nil
}

// ParseCryptoToken extracts KeyMaterial from a crypto token DataHolder.
func ParseCryptoToken(holder DataHolder) (KeyMaterial, error) {
	var out KeyMaterial

	if holder.ClassID != TokenCrypto {
		return out, fmt.Errorf("tokens: crypto token class %q: %w", holder.ClassID, ErrTokenInvalid)
	}

	raw, ok := FindBinaryProperty(holder, PropKeymat)
	if !ok || len(raw) == 0 {
		return out, fmt.Errorf("tokens: crypto token misses keymat: %w", ErrTokenInvalid)
	}

	material, err := DecodeKeyMaterial(raw)
	if err != nil {
		return out, fmt.Errorf("tokens: crypto token key material: %w", err)
	}

	return material, nil
}

// Crypto kind mapping between local ids and OMG wire values.
//
// The local store numbers AES-128 as 1 and AES-256 as 2, while the
// OMG CryptoTransformKind enum numbers AES128_GMAC as 1, AES128_GCM
// as 2, AES256_GMAC as 3, and AES256_GCM as 4. GMAC-only kinds have
// no local equivalent and fail closed.
var (
	// ErrUnknownTransform reports an unmappable transform kind.
	ErrUnknownTransform = errors.New("tokens: unknown transform")
)

// CryptoKindToWire maps a local transform kind to its OMG wire value.
func CryptoKindToWire(local uint32) (uint32, error) {
	switch local {
	case 1:
		return 2, nil
	case 2:
		return 4, nil
	default:
		return 0, fmt.Errorf("tokens: local transform kind %d: %w", local, ErrUnknownTransform)
	}
}

// CryptoKindFromWire maps an OMG wire transform kind to the local id.
func CryptoKindFromWire(wire uint32) (uint32, error) {
	switch wire {
	case 2:
		return 1, nil
	case 4:
		return 2, nil
	default:
		return 0, fmt.Errorf("tokens: wire transform kind %d: %w", wire, ErrUnknownTransform)
	}
}

// DSIG subset manifest per 9.6 and access/dsig.go, documented here
// because governance and permissions envelopes gate every token this
// package parses.
//
// Supported and verified: excise-bytes canonicalization
// (urn:weld:xmlsig:excise-bytes-sha256), SHA-256 and SHA-512 digests,
// RSA PKCS1v15 with SHA-256, ECDSA with SHA-256 or SHA-512, RSA-PSS
// with SHA-256 or SHA-512, prefixed ds:/dsig: Signature forms, and
// multi-reference envelopes whose first reference carries the
// enveloped-signature transform on an empty-URI whole-document
// reference. Enforcement lives in access/dsig.go parseEnvelope; this
// package never verifies XML itself.
//
// Explicitly refused fail-closed: standard XML C14N
// (http://www.w3.org/TR/2001/REC-xml-c14n-20010315, exclusive C14N,
// and any other c14n URI) as a documented waiver, plus multiple
// Signature elements. Full W3C C14N stays out of scope explicitly: the
// line-oriented excise-bytes encoding in access/dsig.go is the
// documented deviation, and sign plus verify share it exactly.
