// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"bytes"
	"strings"
	"testing"
)

// TestDurabilityServiceGolden pins the exact wire bytes for Cyclone defaults.
func TestDurabilityServiceGolden(t *testing.T) {
	t.Parallel()

	value, err := EncodeDurabilityService(DefaultDurabilityServiceQoS())
	if err != nil {
		t.Fatalf("encode defaults: %v", err)
	}

	want := []byte{
		0x00, 0x00, 0x00, 0x00, // cleanup delay seconds 0
		0x00, 0x00, 0x00, 0x00, // cleanup delay fraction 0
		0x00, 0x00, 0x00, 0x00, // history kind keep last
		0x01, 0x00, 0x00, 0x00, // history depth 1
		0xFF, 0xFF, 0xFF, 0xFF, // max samples unlimited
		0xFF, 0xFF, 0xFF, 0xFF, // max instances unlimited
		0xFF, 0xFF, 0xFF, 0xFF, // max samples per instance unlimited
	}

	if !bytes.Equal(value, want) {
		t.Fatalf("golden bytes = %x, want %x", value, want)
	}

	decoded, err := DecodeDurabilityService(value, true)
	if err != nil {
		t.Fatalf("decode golden: %v", err)
	}

	if decoded != DefaultDurabilityServiceQoS() {
		t.Fatalf("decoded = %+v, want %+v", decoded, DefaultDurabilityServiceQoS())
	}
}

// TestDurabilityServiceGoldenWeld pins the facade depth 100 wire bytes.
func TestDurabilityServiceGoldenWeld(t *testing.T) {
	t.Parallel()

	value, err := EncodeDurabilityService(WeldDurabilityServiceQoS())
	if err != nil {
		t.Fatalf("encode weld offer: %v", err)
	}

	if len(value) != durabilityServiceValueLen {
		t.Fatalf("length = %d, want %d", len(value), durabilityServiceValueLen)
	}

	decoded, err := DecodeDurabilityService(value, true)
	if err != nil {
		t.Fatalf("decode weld offer: %v", err)
	}

	if decoded.HistoryDepth != 100 {
		t.Fatalf("history depth = %d, want 100", decoded.HistoryDepth)
	}

	if decoded.MaxSamples != durabilityServiceUnlimited {
		t.Fatalf("max samples = %d, want unlimited", decoded.MaxSamples)
	}
}

// TestDurabilityServiceRoundTrip covers non-default values both endians.
//
//nolint:gocognit // test pins endian, depth, and caps breadth in one round trip, splitting would obscure intent
func TestDurabilityServiceRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []DurabilityServiceQoS{
		DefaultDurabilityServiceQoS(),
		WeldDurabilityServiceQoS(),
		{
			CleanupDelaySec:       5,
			CleanupDelayFrac:      100,
			HistoryKind:           durabilityServiceHistoryKeepAll,
			HistoryDepth:          7,
			MaxSamples:            100,
			MaxInstances:          10,
			MaxSamplesPerInstance: 5,
		},
	}

	for _, littleEndian := range []bool{true, false} {
		for _, q := range cases {
			value, err := EncodeDurabilityService(q)
			if err != nil {
				t.Fatalf("encode %+v: %v", q, err)
			}

			if littleEndian {
				decoded, err := DecodeDurabilityService(value, true)
				if err != nil {
					t.Fatalf("decode %+v: %v", q, err)
				}

				if decoded != q {
					t.Fatalf("round trip = %+v, want %+v", decoded, q)
				}

				continue
			}

			be := convertToBigEndian(t, value)
			decoded, err := DecodeDurabilityService(be, false)
			if err != nil {
				t.Fatalf("decode big endian %+v: %v", q, err)
			}

			if decoded != q {
				t.Fatalf("big endian round trip = %+v, want %+v", decoded, q)
			}
		}
	}
}

// convertToBigEndian swaps each 4 byte word for the endian test path.
func convertToBigEndian(t *testing.T, value []byte) []byte {
	t.Helper()

	if len(value)%4 != 0 {
		t.Fatalf("value length %d is not word aligned", len(value))
	}

	out := make([]byte, len(value))
	for off := 0; off < len(value); off += 4 {
		out[off] = value[off+3]
		out[off+1] = value[off+2]
		out[off+2] = value[off+1]
		out[off+3] = value[off]
	}

	return out
}

// TestDurabilityServiceDecodeRejects pins strict length checks.
func TestDurabilityServiceDecodeRejects(t *testing.T) {
	t.Parallel()

	for _, length := range []int{0, 4, 27, 29, 56} {
		_, err := DecodeDurabilityService(make([]byte, length), true)
		if err == nil {
			t.Fatalf("length %d decoded without error", length)
		}
	}
}

// TestDurabilityServiceValidate pins offer validation.
func TestDurabilityServiceValidate(t *testing.T) {
	t.Parallel()

	err := DefaultDurabilityServiceQoS().Validate()
	if err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}

	err = WeldDurabilityServiceQoS().Validate()
	if err != nil {
		t.Fatalf("weld offer invalid: %v", err)
	}

	badKind := DefaultDurabilityServiceQoS()
	badKind.HistoryKind = 2
	err = badKind.Validate()
	if err == nil {
		t.Fatalf("history kind 2 validated without error")
	}

	zeroDepth := DefaultDurabilityServiceQoS()
	zeroDepth.HistoryDepth = 0
	err = zeroDepth.Validate()
	if err == nil {
		t.Fatalf("keep last depth 0 validated without error")
	}

	negativeDelay := DefaultDurabilityServiceQoS()
	negativeDelay.CleanupDelaySec = -1
	err = negativeDelay.Validate()
	if err == nil {
		t.Fatalf("negative cleanup delay validated without error")
	}

	badLimit := DefaultDurabilityServiceQoS()
	badLimit.MaxSamples = -2
	err = badLimit.Validate()
	if err == nil {
		t.Fatalf("max samples -2 validated without error")
	}

	_, err = EncodeDurabilityService(badKind)
	if err == nil {
		t.Fatalf("encode of bad kind succeeded without error")
	}
}

// TestDepthNormMatrix pins unset plus set depths across all vendors.
func TestDepthNormMatrix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		vendor    PeerVendor
		wantUnset uint64
	}{
		{VendorCyclone, 1},
		{VendorWeldCore, 10},
		{VendorWeldFacade, 100},
		{VendorZenoh, 42},
		{VendorUnknown, 10},
	}

	for _, c := range cases {
		if got := NormalizeDepthForVendor(0, c.vendor); got != c.wantUnset {
			t.Fatalf("vendor %s unset = %d, want %d", c.vendor, got, c.wantUnset)
		}

		for _, requested := range []uint64{1, 5, 42, 100, 1000} {
			if got := NormalizeDepthForVendor(requested, c.vendor); got != requested {
				t.Fatalf("vendor %s requested %d = %d, want passthrough", c.vendor, requested, got)
			}
		}
	}
}

// TestDepthNormUnsetEdges pins the zero vendor and zero depth edges.
func TestDepthNormUnsetEdges(t *testing.T) {
	t.Parallel()

	if got := NormalizeDepthForVendor(0, PeerVendor(99)); got != UnknownVendorUnsetDepth {
		t.Fatalf("out of range vendor unset = %d, want %d", got, UnknownVendorUnsetDepth)
	}

	if UnknownVendorUnsetDepth != WeldCoreUnsetDepth {
		t.Fatalf("unknown default %d diverges from core %d", UnknownVendorUnsetDepth, WeldCoreUnsetDepth)
	}

	report := NegotiateDepth(0, 0, VendorCyclone)
	if report.ResolvedRequested != WeldCoreUnsetDepth {
		t.Fatalf("resolved requested = %d, want core %d", report.ResolvedRequested, WeldCoreUnsetDepth)
	}

	if report.ResolvedPeer != CycloneUnsetDepth {
		t.Fatalf("resolved peer = %d, want cyclone %d", report.ResolvedPeer, CycloneUnsetDepth)
	}

	if report.EffectiveDepth != CycloneUnsetDepth {
		t.Fatalf("effective = %d, want %d", report.EffectiveDepth, CycloneUnsetDepth)
	}

	if report.Reduced {
		t.Fatalf("unset request must not report reduced")
	}
}

// TestDepthNormNegotiate pins the never-silently-reduce rule.
func TestDepthNormNegotiate(t *testing.T) {
	t.Parallel()

	cycloneShort := NegotiateDepth(100, 1, VendorCyclone)
	if cycloneShort.EffectiveDepth != 1 {
		t.Fatalf("effective = %d, want 1", cycloneShort.EffectiveDepth)
	}

	if !cycloneShort.Reduced {
		t.Fatalf("request 100 against peer 1 must report reduced")
	}

	weldGenerous := NegotiateDepth(1, 100, VendorWeldFacade)
	if weldGenerous.EffectiveDepth != 1 {
		t.Fatalf("effective = %d, want 1", weldGenerous.EffectiveDepth)
	}

	if weldGenerous.Reduced {
		t.Fatalf("request 1 against peer 100 must not report reduced")
	}

	unsetLocal := NegotiateDepth(0, 5, VendorCyclone)
	if unsetLocal.EffectiveDepth != 5 {
		t.Fatalf("effective = %d, want 5", unsetLocal.EffectiveDepth)
	}

	if unsetLocal.Reduced {
		t.Fatalf("unset request must not report reduced")
	}

	unsetPeer := NegotiateDepth(5, 0, VendorCyclone)
	if unsetPeer.ResolvedPeer != CycloneUnsetDepth {
		t.Fatalf("resolved peer = %d, want cyclone %d", unsetPeer.ResolvedPeer, CycloneUnsetDepth)
	}

	if unsetPeer.EffectiveDepth != 1 {
		t.Fatalf("effective = %d, want 1", unsetPeer.EffectiveDepth)
	}

	if !unsetPeer.Reduced {
		t.Fatalf("request 5 against unset cyclone peer must report reduced")
	}
}

// TestReplayBoundVolatile pins volatile keeps nothing.
func TestReplayBoundVolatile(t *testing.T) {
	t.Parallel()

	caps := WeldDurabilityServiceQoS()
	for _, published := range []uint64{0, 1, 50, 1000} {
		if got := ReplayBound(published, 100, sedpDurabilityVolatile, caps); got != 0 {
			t.Fatalf("volatile published %d = %d, want 0", published, got)
		}
	}
}

// TestReplayBoundWriterDepth pins transient-local writer depth bounds.
func TestReplayBoundWriterDepth(t *testing.T) {
	t.Parallel()

	caps := DurabilityServiceQoS{}

	cases := []struct {
		published uint64
		depth     uint64
		want      uint64
	}{
		{8, 5, 5},
		{3, 5, 3},
		{150, 100, 100},
		{10, 0, 1},
		{0, 100, 0},
	}

	for _, c := range cases {
		got := ReplayBound(c.published, c.depth, sedpDurabilityTransientLocal, caps)
		if got != c.want {
			t.Fatalf("published %d depth %d = %d, want %d", c.published, c.depth, got, c.want)
		}
	}
}

// TestReplayBoundCaps pins caps constrain transient only, never transient-local.
func TestReplayBoundCaps(t *testing.T) {
	t.Parallel()

	cycloneCaps := DefaultDurabilityServiceQoS()

	// Transient-local replay ignores durability-service caps per spec.
	if got := ReplayBound(50, 100, sedpDurabilityTransientLocal, cycloneCaps); got != 50 {
		t.Fatalf("transient-local with cyclone caps = %d, want 50", got)
	}

	// Transient durability applies the caps history depth of 1.
	if got := ReplayBound(50, 100, 2, cycloneCaps); got != 1 {
		t.Fatalf("transient with cyclone caps = %d, want 1", got)
	}

	limited := DurabilityServiceQoS{
		HistoryKind:           durabilityServiceHistoryKeepLast,
		HistoryDepth:          20,
		MaxSamples:            3,
		MaxInstances:          durabilityServiceUnlimited,
		MaxSamplesPerInstance: durabilityServiceUnlimited,
	}

	if got := ReplayBound(50, 100, 2, limited); got != 3 {
		t.Fatalf("transient with max samples 3 = %d, want 3", got)
	}

	keepAll := DurabilityServiceQoS{
		HistoryKind:           durabilityServiceHistoryKeepAll,
		MaxSamples:            7,
		MaxInstances:          durabilityServiceUnlimited,
		MaxSamplesPerInstance: durabilityServiceUnlimited,
	}

	if got := ReplayBound(20, 100, 2, keepAll); got != 7 {
		t.Fatalf("transient keep-all with max samples 7 = %d, want 7", got)
	}

	if got := ReplayBound(50, 100, 99, cycloneCaps); got != 0 {
		t.Fatalf("unknown durability kind = %d, want fail-closed 0", got)
	}
}

// TestDurabilityServiceCompatMatch pins compatible pairs stay silent.
func TestDurabilityServiceCompatMatch(t *testing.T) {
	t.Parallel()

	equal := CheckDurabilityServiceCompat(DefaultDurabilityServiceQoS(), DefaultDurabilityServiceQoS())
	if !equal.Compatible {
		t.Fatalf("equal pair incompatible: %s", equal.Reason)
	}

	if equal.Reason != "" {
		t.Fatalf("compatible reason = %q, want empty", equal.Reason)
	}

	offer := WeldDurabilityServiceQoS()
	request := DefaultDurabilityServiceQoS()
	smaller := CheckDurabilityServiceCompat(offer, request)
	if !smaller.Compatible {
		t.Fatalf("smaller request incompatible: %s", smaller.Reason)
	}

	unstated := DurabilityServiceQoS{}
	unstatedCaps := CheckDurabilityServiceCompat(unstated, DefaultDurabilityServiceQoS())
	if !unstatedCaps.Compatible {
		t.Fatalf("unstated offer incompatible: %s", unstatedCaps.Reason)
	}
}

// TestDurabilityServiceCompatMismatch pins distinct mismatch reasons.
func TestDurabilityServiceCompatMismatch(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		offered   DurabilityServiceQoS
		requested DurabilityServiceQoS
		wantFrag  string
	}{
		{
			name:      "cleanup delay",
			offered:   DefaultDurabilityServiceQoS(),
			requested: DurabilityServiceQoS{CleanupDelaySec: 5, HistoryKind: durabilityServiceHistoryKeepLast, HistoryDepth: 1, MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: -1},
			wantFrag:  "service_cleanup_delay",
		},
		{
			name:      "history kind",
			offered:   DefaultDurabilityServiceQoS(),
			requested: DurabilityServiceQoS{HistoryKind: durabilityServiceHistoryKeepAll, HistoryDepth: 1, MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: -1},
			wantFrag:  "keep_all history",
		},
		{
			name:      "history depth",
			offered:   DefaultDurabilityServiceQoS(),
			requested: DurabilityServiceQoS{HistoryKind: durabilityServiceHistoryKeepLast, HistoryDepth: 10, MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: -1},
			wantFrag:  "history depth",
		},
		{
			name:      "max samples",
			offered:   DurabilityServiceQoS{HistoryKind: durabilityServiceHistoryKeepLast, HistoryDepth: 5, MaxSamples: 10, MaxInstances: -1, MaxSamplesPerInstance: -1},
			requested: DurabilityServiceQoS{HistoryKind: durabilityServiceHistoryKeepLast, HistoryDepth: 5, MaxSamples: 20, MaxInstances: -1, MaxSamplesPerInstance: -1},
			wantFrag:  "max_samples",
		},
		{
			name:      "max samples unlimited",
			offered:   DurabilityServiceQoS{HistoryKind: durabilityServiceHistoryKeepLast, HistoryDepth: 5, MaxSamples: 10, MaxInstances: -1, MaxSamplesPerInstance: -1},
			requested: DurabilityServiceQoS{HistoryKind: durabilityServiceHistoryKeepLast, HistoryDepth: 5, MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: -1},
			wantFrag:  "max_samples unlimited",
		},
		{
			name:      "max instances",
			offered:   DurabilityServiceQoS{HistoryKind: durabilityServiceHistoryKeepLast, HistoryDepth: 5, MaxSamples: -1, MaxInstances: 2, MaxSamplesPerInstance: -1},
			requested: DurabilityServiceQoS{HistoryKind: durabilityServiceHistoryKeepLast, HistoryDepth: 5, MaxSamples: -1, MaxInstances: 4, MaxSamplesPerInstance: -1},
			wantFrag:  "max_instances",
		},
		{
			name:      "max samples per instance",
			offered:   DurabilityServiceQoS{HistoryKind: durabilityServiceHistoryKeepLast, HistoryDepth: 5, MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: 2},
			requested: DurabilityServiceQoS{HistoryKind: durabilityServiceHistoryKeepLast, HistoryDepth: 5, MaxSamples: -1, MaxInstances: -1, MaxSamplesPerInstance: 9},
			wantFrag:  "max_samples_per_instance",
		},
	}

	for _, c := range cases {
		result := CheckDurabilityServiceCompat(c.offered, c.requested)
		if result.Compatible {
			t.Fatalf("%s: compatible, want mismatch", c.name)
		}

		if !strings.Contains(result.Reason, c.wantFrag) {
			t.Fatalf("%s: reason %q misses %q", c.name, result.Reason, c.wantFrag)
		}
	}
}

// TestDurabilityServiceParam pins the SEDP parameter wrapper.
func TestDurabilityServiceParam(t *testing.T) {
	t.Parallel()

	param, err := DurabilityServiceParam(WeldDurabilityServiceQoS())
	if err != nil {
		t.Fatalf("param: %v", err)
	}

	if param.ID != PIDDurabilityService {
		t.Fatalf("param id = %#04x, want %#04x", param.ID, PIDDurabilityService)
	}

	if PIDDurabilityService != 0x001e {
		t.Fatalf("param id = %#04x, want spec 0x001e", PIDDurabilityService)
	}

	decoded, err := DecodeDurabilityService(param.Value, true)
	if err != nil {
		t.Fatalf("decode param value: %v", err)
	}

	if decoded != WeldDurabilityServiceQoS() {
		t.Fatalf("param round trip = %+v, want %+v", decoded, WeldDurabilityServiceQoS())
	}
}
