// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"testing"

	"github.com/podomy/weld/internal/transports"
)

// TestSedpTypeHashNavSatStatus checks the rest fallback for NavSatStatus.
func TestSedpTypeHashNavSatStatus(t *testing.T) {
	t.Parallel()

	want := "RIHS01_d1ed3befa628e09571bd273b888ba1c1fd187c9a5e0006b385d7e5e9095a3204"

	if got := sedpTypeHashForIDL("sensor_msgs__msg__NavSatStatus", ""); got != want {
		t.Fatalf("NavSatStatus hash = %q, want %q", got, want)
	}
}

// TestSedpTypeHashLiveWins checks caller hash beats the static table.
func TestSedpTypeHashLiveWins(t *testing.T) {
	t.Parallel()

	live := "RIHS01_ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

	if got := sedpTypeHashForIDL("sensor_msgs__msg__NavSatStatus", live); got != live {
		t.Fatalf("live hash = %q, want %q", got, live)
	}

	if got := sedpTypeHashForIDL("std_msgs__msg__String", live); got != live {
		t.Fatalf("live string hash = %q, want %q", got, live)
	}
}

// TestSedpTypeHashRestTable checks a sample of rest fallbacks plus unknown empty.
func TestSedpTypeHashRestTable(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"sensor_msgs__msg__NavSatFix":      "RIHS01_62223ab3fe210a15976021da7afddc9e200dc9ec75231c1b6a557fc598a65404",
		"sensor_msgs__msg__Imu":            "RIHS01_7d9a00ff131080897a5ec7e26e315954b8eae3353c3f995c55faf71574000b5b",
		"sensor_msgs__msg__LaserScan":      "RIHS01_64c191398013af96509d518dac71d5164f9382553fce5c1f8cca5be7924bd828",
		"sensor_msgs__msg__Image":          "RIHS01_d31d41a9a4c4bc8eae9be757b0beed306564f7526c88ea6a4588fb9582527d47",
		"geometry_msgs__msg__Pose":         "RIHS01_d501954e9476cea2996984e812054b68026ae0bfae789d9a10b23daf35cc90fa",
		"nav_msgs__msg__Odometry":          "RIHS01_3cc97dc7fb7502f8714462c526d369e35b603cfc34d946e3f2eda2766dfec6e0",
		"builtin_interfaces__msg__Time":    "RIHS01_b106235e25a4c5ed35098aa0a61a3ee9c9b18d197f398b0e4206cea9acf9c197",
		"std_msgs__msg__Header":            "RIHS01_f49fb3ae2cf070f793645ff749683ac6b06203e41c891e17701b1cb597ce6a01",
		"shape_msgs__msg__SolidPrimitive":  "RIHS01_2802a15190aadc3f496584df4b0b4c5824d8a0b31aaef839faa75bc34dda38ac",
		"stereo_msgs__msg__DisparityImage": "RIHS01_1ec1ff6b5bace919e4544a37f2d96ead9f81783701b7b7a4d97a09325ecf2711",
	}

	for idl, want := range cases {
		if got := sedpTypeHashForIDL(idl, ""); got != want {
			t.Fatalf("%s hash = %q, want %q", idl, got, want)
		}
	}

	if got := sedpTypeHashForIDL("sensor_msgs__msg__NotAType", ""); got != "" {
		t.Fatalf("unknown hash = %q, want empty", got)
	}
}

// TestDeclarePublishWriterThreadsHash checks publish carries live hash to SEDP.
func TestDeclarePublishWriterThreadsHash(t *testing.T) {
	var prefix SEDPGUIDPrefix

	for i := range prefix {
		prefix[i] = byte(i + 1)
	}

	idx, err := NewSEDPIndex(prefix, 42)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}

	backend := &Backend{}

	live := "RIHS01_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

	msg := transports.OutgoingMessage{
		TopicName: "/fix",
		TypeName:  "sensor_msgs__msg__NavSatStatus",
		Payload:   []byte{1, 2, 3},
		TypeHash:  live,
	}

	ep, err := backend.declarePublishWriter(idx, msg)
	if err != nil {
		t.Fatalf("declare with live hash: %v", err)
	}

	if ep.TypeHash != live {
		t.Fatalf("endpoint hash = %q, want live %q", ep.TypeHash, live)
	}

	found, ok := idx.FindLocalWriter("/fix", "sensor_msgs__msg__NavSatStatus", live)
	if !ok {
		t.Fatalf("live writer not found after declare")
	}

	if found.ID != ep.ID {
		t.Fatalf("found ID %v differs from declared %v", found.ID, ep.ID)
	}

	again, err := backend.declarePublishWriter(idx, msg)
	if err != nil {
		t.Fatalf("second declare: %v", err)
	}

	if again.ID != ep.ID {
		t.Fatalf("second declare ID %v differs, want stable %v", again.ID, ep.ID)
	}
}

// TestDeclarePublishWriterStaticFallback checks empty caller hash uses the table.
func TestDeclarePublishWriterStaticFallback(t *testing.T) {
	var prefix SEDPGUIDPrefix

	for i := range prefix {
		prefix[i] = byte(0xA0 + i)
	}

	idx, err := NewSEDPIndex(prefix, 42)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}

	backend := &Backend{}

	msg := transports.OutgoingMessage{
		TopicName: "/status",
		TypeName:  "sensor_msgs__msg__NavSatStatus",
		Payload:   []byte{4, 5},
	}

	ep, err := backend.declarePublishWriter(idx, msg)
	if err != nil {
		t.Fatalf("declare static fallback: %v", err)
	}

	want := "RIHS01_d1ed3befa628e09571bd273b888ba1c1fd187c9a5e0006b385d7e5e9095a3204"
	if ep.TypeHash != want {
		t.Fatalf("endpoint hash = %q, want static %q", ep.TypeHash, want)
	}

	if n := backend.typeHashMissCount(); n != 0 {
		t.Fatalf("miss count = %d, want 0 for known type", n)
	}
}

// TestDeclarePublishWriterUnknownMiss checks empty hash keeps legacy plus loud metric.
func TestDeclarePublishWriterUnknownMiss(t *testing.T) {
	var prefix SEDPGUIDPrefix

	for i := range prefix {
		prefix[i] = byte(0xC0 + i)
	}

	idx, err := NewSEDPIndex(prefix, 42)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}

	backend := &Backend{}

	msg := transports.OutgoingMessage{
		TopicName: "/unknown",
		TypeName:  "sensor_msgs__msg__NotAType",
		Payload:   []byte{6},
	}

	ep, err := backend.declarePublishWriter(idx, msg)
	if err != nil {
		t.Fatalf("declare unknown: %v", err)
	}

	if ep.TypeHash != "" {
		t.Fatalf("unknown hash = %q, want empty", ep.TypeHash)
	}

	if n := backend.typeHashMissCount(); n != 1 {
		t.Fatalf("miss count = %d, want 1", n)
	}

	emptyReader := SEDPEndpoint{
		IsReader: true,
		DomainID: 42,
		ROSTopic: ep.ROSTopic,
		DDSTopic: ep.DDSTopic,
		Kind:     ep.Kind,
		TypeDDS:  ep.TypeDDS,
		TypeHash: "",
	}
	emptyWriter := ep

	if !SEDPEndpointsMatch(emptyReader, emptyWriter) {
		t.Fatalf("empty reader must match empty writer for legacy peers")
	}
}
