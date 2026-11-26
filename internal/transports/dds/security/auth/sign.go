// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package auth

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"

	"github.com/podomy/weld/internal/transports/dds/security/tokens"
)

// ParseECPrivateKey decodes one PEM or DER EC private key.
func ParseECPrivateKey(data []byte) (*ecdsa.PrivateKey, error) {
	raw := data
	if block, _ := pem.Decode(data); block != nil {
		raw = block.Bytes
	}

	key, err := x509.ParseECPrivateKey(raw)
	if err == nil {
		return key, nil
	}

	parsed, err2 := x509.ParsePKCS8PrivateKey(raw)
	if err2 != nil {
		return nil, fmt.Errorf("auth: parse EC private key: %w", errors.Join(err, err2, ErrHandshakeFailed))
	}

	ecKey, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("auth: private key is not EC: %w", ErrHandshakeFailed)
	}

	return ecKey, nil
}

// SignHandshakeData signs handshake content with an EC identity key.
//
// RSA identity keys fail closed: CycloneDDS signs RSA with PSS, which
// this path never fabricates.
func SignHandshakeData(privateKey *ecdsa.PrivateKey, content []byte) ([]byte, error) {
	if privateKey == nil {
		return nil, fmt.Errorf("auth: sign without private key: %w", ErrHandshakeFailed)
	}

	if len(content) == 0 {
		return nil, fmt.Errorf("auth: sign empty content: %w", ErrHandshakeFailed)
	}

	digest := sha256.Sum256(content)

	sig, err := ecdsa.SignASN1(rand.Reader, privateKey, digest[:])
	if err != nil {
		return nil, fmt.Errorf("auth: ecdsa sign: %w", errors.Join(err, ErrHandshakeFailed))
	}

	return sig, nil
}

// signItems orders one six-item signature content per section 10.5.
func signItems(first, second, third, fourth, fifth, sixth tokens.BinaryProperty) ([]byte, error) {
	raw, err := tokens.SerializeBinaryItems([]tokens.BinaryProperty{first, second, third, fourth, fifth, sixth})
	if err != nil {
		return nil, fmt.Errorf("auth: signature content: %w", err)
	}

	return raw, nil
}

// binProp builds one named binary property copying the value.
func binProp(name string, value []byte) tokens.BinaryProperty {
	return tokens.BinaryProperty{Name: name, Value: append([]byte(nil), value...)}
}

// ReplySignContent builds the responder signature content.
//
// Order matches the CycloneDDS authentication plugin: hash_c2,
// challenge2, dh2, challenge1, dh1, hash_c1.
func ReplySignContent(hashC1, hashC2, challenge1, challenge2 [32]byte, dh1, dh2 []byte) ([]byte, error) {
	content, err := signItems(
		binProp("hash_c2", hashC2[:]),
		binProp(tokens.PropChallenge2, challenge2[:]),
		binProp(tokens.PropDH2, dh2),
		binProp(tokens.PropChallenge1, challenge1[:]),
		binProp(tokens.PropDH1, dh1),
		binProp("hash_c1", hashC1[:]),
	)
	if err != nil {
		return nil, err
	}

	return content, nil
}

// FinalSignContent builds the initiator final signature content.
//
// Order matches the CycloneDDS authentication plugin: hash_c1,
// challenge1, dh1, challenge2, dh2, hash_c2.
func FinalSignContent(hashC1, hashC2, challenge1, challenge2 [32]byte, dh1, dh2 []byte) ([]byte, error) {
	content, err := signItems(
		binProp("hash_c1", hashC1[:]),
		binProp(tokens.PropChallenge1, challenge1[:]),
		binProp(tokens.PropDH1, dh1),
		binProp(tokens.PropChallenge2, challenge2[:]),
		binProp(tokens.PropDH2, dh2),
		binProp("hash_c2", hashC2[:]),
	)
	if err != nil {
		return nil, err
	}

	return content, nil
}
