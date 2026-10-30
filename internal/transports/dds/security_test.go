// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"errors"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// secTestPropList encodes a CDR little endian property list for tests.
//
// Pairs hold alternating keys and values. Each entry is two CDR strings
// plus one false propagate byte with CDR alignment, matching
// securityReadString expectations.
func secTestPropList(t *testing.T, pairs ...string) []byte {
	t.Helper()

	if len(pairs)%2 != 0 {
		t.Fatalf("pairs need even count have %d", len(pairs))
	}

	out := appendUint32LE(nil, uint32(len(pairs)/2)) //nolint:gosec // G115: test pair count is tiny

	for i := 0; i < len(pairs); i += 2 {
		out = appendCDRStringLE(out, pairs[i])
		out = appendCDRStringLE(out, pairs[i+1]) //nolint:gosec // G602: pairs length is even and checked above, i+1 stays in bounds
		out = append(out, 0x00)

		for len(out)%4 != 0 {
			out = append(out, 0x00)
		}
	}

	return out
}

// appendUint32LE appends one little endian uint32.
func appendUint32LE(out []byte, v uint32) []byte {
	return append(out, byte(v), byte(v>>8), byte(v>>16), byte(v>>24)) //nolint:gosec // G115: intentional truncation for little endian encoding
}

// appendCDRStringLE appends one CDR string with NUL plus pad.
func appendCDRStringLE(out []byte, s string) []byte {
	out = appendUint32LE(out, uint32(len(s)+1)) //nolint:gosec // G115: test strings are tiny
	out = append(out, s...)
	out = append(out, 0x00)

	for len(out)%4 != 0 {
		out = append(out, 0x00)
	}

	return out
}

// TestSecurityParseTolerateMatrix checks every security PID parses and tolerates.
//
//nolint:gocognit // test pins all security PID shapes and classes in one matrix, splitting would obscure intent
func TestSecurityParseTolerateMatrix(t *testing.T) {
	cases := []struct {
		name       string
		params     []rtps.Param
		wantClass  SecurityClassification
		wantTokens bool
	}{
		{
			name:      "empty is unsecured",
			params:    nil,
			wantClass: SecurityUnsecured,
		},
		{
			name:      "unknown vendor pid is ignored",
			params:    []rtps.Param{{ID: rtps.PIDVendorAdlinkWriterDataLifecycle, Value: []byte{0x01}}},
			wantClass: SecurityUnsecured,
		},
		{
			name: "participant info eight bytes",
			params: []rtps.Param{
				{ID: PIDParticipantSecurityInfo, Value: []byte{0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}},
			},
			wantClass: SecurityCompatibleUnknown,
		},
		{
			name: "endpoint info four bytes",
			params: []rtps.Param{
				{ID: PIDEndpointSecurityInfo, Value: []byte{0x01, 0x00, 0x00, 0x00}},
			},
			wantClass: SecurityCompatibleUnknown,
		},
		{
			name: "identity and permissions tokens",
			params: []rtps.Param{
				{ID: PIDIdentityToken, Value: []byte{0xAA}},
				{ID: PIDPermissionsToken, Value: []byte{0xBB}},
			},
			wantClass:  SecurityCompatibleUnknown,
			wantTokens: true,
		},
		{
			name: "identity ca alone stays compatible",
			params: []rtps.Param{
				{ID: PIDPropertyList, Value: secTestPropList(t, "dds.sec.auth.identity_ca", "file:ca.pem")},
			},
			wantClass: SecurityCompatibleUnknown,
		},
		{
			name: "governance demands crypto",
			params: []rtps.Param{
				{ID: PIDPropertyList, Value: secTestPropList(t, "dds.sec.access.governance", "file:gov.p7s")},
			},
			wantClass: SecurityRequiresCrypto,
		},
		{
			name: "crypto property demands crypto",
			params: []rtps.Param{
				{ID: PIDPropertyList, Value: secTestPropList(t, "dds.sec.crypto.key_exchange_algorithm", "ECDH")},
			},
			wantClass: SecurityRequiresCrypto,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info, warnings := ExtractSecurityInfo(tc.params)
			if len(warnings) != 0 {
				t.Fatalf("warnings %v, want none", warnings)
			}

			if info.Classification != tc.wantClass {
				t.Fatalf("class %q, want %q", info.Classification, tc.wantClass)
			}

			if tc.wantClass == SecurityUnsecured && info.Present {
				t.Fatalf("unsecured peer must not mark present")
			}

			if tc.wantClass != SecurityUnsecured && !info.Present {
				t.Fatalf("secured peer must mark present")
			}

			if tc.wantTokens && (!info.IdentityTokenPresent || !info.PermissionsTokenPresent) {
				t.Fatalf("tokens not recorded: %+v", info)
			}
		})
	}
}

// TestSecurityMalformedLoudWithoutKillingLoop checks bad values warn but never stop the loop.
func TestSecurityMalformedLoudWithoutKillingLoop(t *testing.T) {
	goodEndpoint := rtps.Param{ID: PIDEndpointSecurityInfo, Value: []byte{0x01, 0x00, 0x00, 0x00}}
	goodToken := rtps.Param{ID: PIDIdentityToken, Value: []byte{0xAA}}

	cases := []struct {
		name   string
		params []rtps.Param
	}{
		{
			name: "short participant info beside good endpoint",
			params: []rtps.Param{
				{ID: PIDParticipantSecurityInfo, Value: []byte{0x01, 0x02}},
				goodEndpoint,
				goodToken,
			},
		},
		{
			name: "truncated property list beside good token",
			params: []rtps.Param{
				{ID: PIDPropertyList, Value: []byte{0x01}},
				goodToken,
			},
		},
		{
			name: "property list with overrun string beside good endpoint",
			params: []rtps.Param{
				{ID: PIDPropertyList, Value: []byte{0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x00, 0x00, 'a', 0x00}},
				goodEndpoint,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info, warnings := ExtractSecurityInfo(tc.params)
			if len(warnings) == 0 {
				t.Fatalf("malformed security value must warn loudly")
			}

			if !info.Present {
				t.Fatalf("good sibling params must still record presence: %+v", info)
			}

			if info.Classification == SecurityUnsecured {
				t.Fatalf("good sibling markers must still classify secured: %+v", info)
			}
		})
	}
}

// TestSecurityClassifyMatrix pins the pure posture function.
func TestSecurityClassifyMatrix(t *testing.T) {
	cases := []struct {
		name       string
		properties map[string]string
		seen       bool
		want       SecurityClassification
	}{
		{name: "nil means unsecured", properties: nil, seen: false, want: SecurityUnsecured},
		{name: "empty means unsecured", properties: map[string]string{}, seen: false, want: SecurityUnsecured},
		{name: "non sec keys alone mean unsecured", properties: map[string]string{"foo": "bar"}, seen: false, want: SecurityUnsecured},
		{name: "info pid alone means compatible", properties: nil, seen: true, want: SecurityCompatibleUnknown},
		{name: "identity ca means compatible", properties: map[string]string{"dds.sec.auth.identity_ca": "x"}, seen: false, want: SecurityCompatibleUnknown},
		{name: "identity cert demands crypto", properties: map[string]string{"dds.sec.auth.identity_certificate": "x"}, seen: false, want: SecurityRequiresCrypto},
		{name: "private key demands crypto", properties: map[string]string{"dds.sec.auth.private_key": "x"}, seen: false, want: SecurityRequiresCrypto},
		{name: "governance demands crypto", properties: map[string]string{"dds.sec.access.governance": "x"}, seen: false, want: SecurityRequiresCrypto},
		{name: "permissions demand crypto", properties: map[string]string{"dds.sec.access.permissions": "x"}, seen: false, want: SecurityRequiresCrypto},
		{name: "permissions ca demands crypto", properties: map[string]string{"dds.sec.access.permissions_ca": "x"}, seen: false, want: SecurityRequiresCrypto},
		{name: "crypto prefix demands crypto", properties: map[string]string{"dds.sec.crypto.foo": "x"}, seen: false, want: SecurityRequiresCrypto},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifySecurity(tc.properties, tc.seen)
			if got != tc.want {
				t.Fatalf("class %q, want %q", got, tc.want)
			}
		})
	}
}

// TestSecuredTransformDropExactReason checks every protected kind drops with the exact reason.
func TestSecuredTransformDropExactReason(t *testing.T) {
	ids := []struct {
		id   byte
		name string
	}{
		{SecuredSubmessageSecBody, "SEC_BODY"},
		{SecuredSubmessageSecPrefix, "SEC_PREFIX"},
		{SecuredSubmessageSecPostfix, "SEC_POSTFIX"},
		{SecuredSubmessageSRTPSPrefix, "SRTPS_PREFIX"},
		{SecuredSubmessageSRTPSPostfix, "SRTPS_POSTFIX"},
	}

	recv := NewDataReceiver()

	for _, kind := range ids {
		handled, err := HandleProtectedSubmessage(recv, rtps.RawSubmessage{ID: kind.id, Flags: 0x01, Body: []byte{0x00}})
		if !handled {
			t.Fatalf("protected id %#02x must report handled", kind.id)
		}

		if !errors.Is(err, ErrSecuredTransformUnavailable) {
			t.Fatalf("id %#02x error %v must wrap secured-transform-unavailable", kind.id, err)
		}

		if !strings.Contains(err.Error(), kind.name) {
			t.Fatalf("id %#02x error %q must name transform %s", kind.id, err, kind.name)
		}
	}

	if got := recv.DropReasonCount(DropReasonSecuredTransformUnavailable); got != uint64(len(ids)) {
		t.Fatalf("secured-transform-unavailable count %d, want %d", got, len(ids))
	}

	if got := recv.DroppedCount(); got != uint64(len(ids)) {
		t.Fatalf("total drops %d, want %d", got, len(ids))
	}

	handled, err := HandleProtectedSubmessage(recv, rtps.RawSubmessage{ID: rtps.SubmessageData, Flags: 0x01, Body: []byte{0x00}})
	if handled || err != nil {
		t.Fatalf("plain DATA must pass through untouched: handled=%v err=%v", handled, err)
	}

	if got := recv.DropReasonCount(DropReasonSecuredTransformUnavailable); got != uint64(len(ids)) {
		t.Fatalf("plain DATA must not count secured drops, have %d", got)
	}

	if err := DropProtectedSubmessage(nil, SecuredSubmessageSecBody); !errors.Is(err, ErrNilReceiver) {
		t.Fatalf("nil receiver must fail loudly, got %v", err)
	}
}

// TestSecuredDiscoveryLoud checks crypto-demanding peers fail while others flow.
func TestSecuredDiscoveryLoud(t *testing.T) {
	crypto := SecurityInfo{Present: true, Classification: SecurityRequiresCrypto}
	if err := CheckProtectedDiscovery(crypto); !errors.Is(err, ErrSecuredDiscoveryRequiresCrypto) {
		t.Fatalf("crypto peer must fail loudly, got %v", err)
	}

	for _, info := range []SecurityInfo{
		{Classification: SecurityUnsecured},
		{Present: true, Classification: SecurityCompatibleUnknown},
	} {
		if err := CheckProtectedDiscovery(info); err != nil {
			t.Fatalf("class %s must keep flowing, got %v", info.Classification, err)
		}
	}
}

// TestSecurityConfigMatrix pins validation of path presence.
func TestSecurityConfigMatrix(t *testing.T) {
	full := SecurityConfig{
		Enabled:                 true,
		IdentityCertificatePath: "cert.pem",
		IdentityPrivateKeyPath:  "key.pem",
		PermissionsCAPath:       "ca.pem",
		GovernancePath:          "gov.p7s",
	}

	if err := (SecurityConfig{}).Validate(); err != nil {
		t.Fatalf("disabled empty config must pass, got %v", err)
	}

	if err := full.Validate(); err != nil {
		t.Fatalf("enabled full config must pass, got %v", err)
	}

	missing := []struct {
		name   string
		modify func(*SecurityConfig)
	}{
		{name: "cert", modify: func(c *SecurityConfig) { c.IdentityCertificatePath = "" }},
		{name: "key", modify: func(c *SecurityConfig) { c.IdentityPrivateKeyPath = " " }},
		{name: "ca", modify: func(c *SecurityConfig) { c.PermissionsCAPath = "" }},
		{name: "governance", modify: func(c *SecurityConfig) { c.GovernancePath = "" }},
	}

	for _, tc := range missing {
		t.Run(tc.name, func(t *testing.T) {
			cfg := full
			tc.modify(&cfg)

			err := cfg.Validate()
			if !errors.Is(err, ErrSecurityConfigInvalid) {
				t.Fatalf("missing %s must fail validation, got %v", tc.name, err)
			}
		})
	}
}

// TestSecuredReceiverStaysHealthy checks drops never stall plain samples.
func TestSecuredReceiverStaysHealthy(t *testing.T) {
	sink := &recordingWriter{}
	pub := mustNewPublisher(t, testWriterGUID(), sink)
	mustPublish(t, pub, transports.OutgoingMessage{TopicName: "/sec_health", TypeName: "std_msgs__msg__String", Payload: []byte("ok")})

	recv := NewDataReceiver()

	if _, err := HandleProtectedSubmessage(recv, rtps.RawSubmessage{ID: SecuredSubmessageSRTPSPrefix, Body: []byte{0x01}}); err == nil {
		t.Fatalf("protected submessage must drop loudly")
	}

	msg := mustTake(t, recv, sink.lastFrame(t))
	if string(msg.Payload) != "ok" {
		t.Fatalf("payload %q, want %q", msg.Payload, "ok")
	}

	if msg.TopicName != "/sec_health" {
		t.Fatalf("topic %q, want %q", msg.TopicName, "/sec_health")
	}
}
