// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package dds

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// mustHEFramed returns one framed empty RTPS_HE for placement tests.
func mustHEFramed(t *testing.T) []byte {
	t.Helper()

	framed, err := rtps.EncodeHeaderExtension(rtps.HeaderExtension{LittleEndian: true})
	if err != nil {
		t.Fatalf("encode HE: %v", err)
	}

	return framed
}

// mustEndpointDatagram serializes one endpoint DATA datagram for tests.
func mustEndpointDatagram(t *testing.T, ep SEDPEndpoint) []byte {
	t.Helper()

	raw, err := SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize endpoint: %v", err)
	}

	return raw
}

// TestSEDPHeaderExtensionValidFirstPasses pins HE-first acceptance per 8.3.7.3.
func TestSEDPHeaderExtensionValidFirstPasses(t *testing.T) {
	t.Parallel()

	producer := mustSEDPIndexWithPrefix(t, 0xD1)
	ep := mustSEDPWriter(t, producer, "/chatter", "RIHS01_he")
	raw := mustEndpointDatagram(t, ep)

	local := mustSEDPIndexWithPrefix(t, 0xD2)
	withHE := append(append([]byte(nil), raw[:sedpRTPSHeaderLen]...), mustHEFramed(t)...)
	withHE = append(withHE, raw[sedpRTPSHeaderLen:]...)

	if err := local.ingestSample(withHE); err != nil {
		t.Fatalf("ingest HE-first: %v", err)
	}
	if got := local.Generation(); got != 1 {
		t.Fatalf("generation = %d, want 1", got)
	}
	if got := len(local.RemoteEndpoints()); got != 1 {
		t.Fatalf("remotes = %d, want 1", got)
	}
}

// TestSEDPHeaderExtensionMispositionedDrops pins fail-closed on later HE per 8.3.7.3.
func TestSEDPHeaderExtensionMispositionedDrops(t *testing.T) {
	t.Parallel()

	producer := mustSEDPIndexWithPrefix(t, 0xD3)
	ep := mustSEDPWriter(t, producer, "/chatter", "RIHS01_he")
	raw := mustEndpointDatagram(t, ep)

	local := mustSEDPIndexWithPrefix(t, 0xD4)
	misplaced := append(append([]byte(nil), raw...), mustHEFramed(t)...)

	if err := local.ingestSample(misplaced); err == nil {
		t.Fatalf("ingest mispositioned HE succeeded, want loud error")
	}
	if got := local.Generation(); got != 0 {
		t.Fatalf("generation = %d, want 0 after HE drop", got)
	}
	if got := len(local.RemoteEndpoints()); got != 0 {
		t.Fatalf("remotes = %d, want 0 after HE drop", got)
	}
}

// TestSEDPHeaderExtensionMalformedDropsLoud pins loud fail-closed on short HE per 8.3.7.3.
func TestSEDPHeaderExtensionMalformedDropsLoud(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xD5)
	hdr := []byte(sedpRTPSMagic)
	hdr = append(hdr, sedpRTPSMajor, sedpRTPSMinor, sedpVendorID0, sedpVendorID1)
	var prefix SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = 0xE0 + byte(i)
	}
	hdr = append(hdr, prefix[:]...)
	badHE, err := rtps.WrapSubmessage(rtps.SubmessageHeaderExtension, 0x03, []byte{0x01})
	if err != nil {
		t.Fatalf("wrap bad HE: %v", err)
	}
	pad, err := rtps.WrapSubmessage(rtps.SubmessagePad, 0x01, nil)
	if err != nil {
		t.Fatalf("wrap pad: %v", err)
	}
	raw := append(append(hdr, badHE...), pad...)

	if err := local.ingestSample(raw); err == nil {
		t.Fatalf("ingest malformed HE succeeded, want error")
	}
	if got := local.Generation(); got != 0 {
		t.Fatalf("generation = %d, want 0 after malformed HE", got)
	}
}

// TestSEDPScanControlDropsMispositionedHE pins the control scan HE gate per 8.3.7.3.
func TestSEDPScanControlDropsMispositionedHE(t *testing.T) {
	t.Parallel()

	producer := mustSEDPIndexWithPrefix(t, 0xD6)
	ep := mustSEDPWriter(t, producer, "/chatter", "RIHS01_he")
	raw := mustEndpointDatagram(t, ep)
	misplaced := append(append([]byte(nil), raw...), mustHEFramed(t)...)

	called := false
	scanSedpControl(misplaced, sedpSubmessageHeartbeat, func(_ []byte, _ byte) bool {
		called = true

		return true
	})
	if called {
		t.Fatalf("scan ran handle past mispositioned HE, want drop")
	}

	handled := false
	scanSedpControl(raw, sedpSubmessageHeartbeat, func(body []byte, _ byte) bool {
		handled = true

		return true
	})
	if !handled {
		t.Fatalf("scan skipped valid heartbeat, want handle")
	}
}

// TestSEDPUpsertRemoteCheckedBinding pins prefix enforcement fail-closed.
func TestSEDPUpsertRemoteCheckedBinding(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xD7)
	peer := mustSEDPIndexWithPrefix(t, 0xD8)
	ep := mustSEDPWriter(t, peer, "/chatter", "RIHS01_bind")

	var foreign SEDPGUIDPrefix
	for i := range foreign {
		foreign[i] = 0xF0 + byte(i)
	}
	if err := local.UpsertRemoteChecked(foreign, ep); !errors.Is(err, ErrSEDPInvalidDATA) {
		t.Fatalf("checked upsert foreign prefix err = %v, want ErrSEDPInvalidDATA", err)
	}
	if got := len(local.RemoteEndpoints()); got != 0 {
		t.Fatalf("remotes = %d, want 0 after refused bind", got)
	}
	if got := local.Generation(); got != 0 {
		t.Fatalf("generation = %d, want 0 after refused bind", got)
	}

	if err := local.UpsertRemoteChecked(ep.ID.Prefix, ep); err != nil {
		t.Fatalf("checked upsert bound prefix: %v", err)
	}
	if got := len(local.RemoteEndpoints()); got != 1 {
		t.Fatalf("remotes = %d, want 1 after bound install", got)
	}
}

// TestSEDPProvisionalInstallSeqGate pins DATA sequence gating on the fast path.
func TestSEDPProvisionalInstallSeqGate(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xD9)
	peer := mustSEDPIndexWithPrefix(t, 0xDA)
	ep := mustSEDPWriter(t, peer, "/chatter", "RIHS01_seq")

	if err := local.ProvisionalInstall(ep, 0); !errors.Is(err, ErrSEDPInvalidDATA) {
		t.Fatalf("seq-zero install err = %v, want ErrSEDPInvalidDATA", err)
	}
	if got := len(local.RemoteEndpoints()); got != 0 {
		t.Fatalf("remotes = %d, want 0 after seq-zero refuse", got)
	}

	if err := local.ProvisionalInstall(ep, 3); err != nil {
		t.Fatalf("install seq 3: %v", err)
	}
	stale := ep
	stale.TypeHash = "RIHS01_stale"
	if err := local.ProvisionalInstall(stale, 2); !errors.Is(err, ErrSEDPStale) {
		t.Fatalf("stale install err = %v, want ErrSEDPStale", err)
	}
	snap := local.RemoteEndpoints()
	if len(snap) != 1 || snap[0].TypeHash != ep.TypeHash {
		t.Fatalf("stale install clobbered placeholder: %+v", snap)
	}
}

// TestSEDPProvisionalCapEvicts pins the bounded remotes table.
func TestSEDPProvisionalCapEvicts(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xDB)
	peer := mustSEDPIndexWithPrefix(t, 0xDC)
	base := mustSEDPWriter(t, peer, "/chatter", "RIHS01_cap")

	for i := range sedpAuxCap + 5 {
		ep := base
		ep.ID.Entity = sedpEntityFor(sedpUserEntityBase+uint32(i), false)
		if err := local.ProvisionalInstall(ep, uint64(i+1)); err != nil {
			t.Fatalf("install %d: %v", i, err)
		}
	}
	if got := len(local.RemoteEndpoints()); got > sedpAuxCap {
		t.Fatalf("remotes = %d, want at most %d", got, sedpAuxCap)
	}
}

// TestSEDPUpsertRemoteCapEvicts pins the confirmed-path remotes bound.
func TestSEDPUpsertRemoteCapEvicts(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xDD)
	peer := mustSEDPIndexWithPrefix(t, 0xDE)
	base := mustSEDPWriter(t, peer, "/chatter", "RIHS01_upcap")

	for i := range sedpAuxCap + 5 {
		ep := base
		ep.ID.Entity = sedpEntityFor(sedpUserEntityBase+uint32(i), false)
		if err := local.UpsertRemote(ep); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
	}
	if got := len(local.RemoteEndpoints()); got > sedpAuxCap {
		t.Fatalf("remotes = %d, want at most %d", got, sedpAuxCap)
	}
}

// TestSEDPDomainTagExactMatch pins fail-closed tag equality per 8.5.5.1.
func TestSEDPDomainTagExactMatch(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xDF)
	local.SetSEDPDomainTag("prod")
	peer := mustSEDPIndexWithPrefix(t, 0xE0)
	ep := mustSEDPWriter(t, peer, "/chatter", "RIHS01_tag")

	if err := local.UpsertRemote(ep); !errors.Is(err, ErrSEDPInvalidDomain) {
		t.Fatalf("untagged upsert err = %v, want ErrSEDPInvalidDomain", err)
	}
	if got := local.Generation(); got != 0 {
		t.Fatalf("generation = %d, want 0 after tag refuse", got)
	}

	tagged := ep
	tagged.DomainTag = "prod"
	if err := local.UpsertRemote(tagged); err != nil {
		t.Fatalf("tagged upsert: %v", err)
	}

	foreign := ep
	foreign.ID.Entity = sedpEntityFor(sedpUserEntityBase+1, false)
	foreign.DomainTag = "other"
	if err := local.UpsertRemote(foreign); !errors.Is(err, ErrSEDPInvalidDomain) {
		t.Fatalf("foreign upsert err = %v, want ErrSEDPInvalidDomain", err)
	}

	plain := mustSEDPIndexWithPrefix(t, 0xE1)
	if err := plain.ProvisionalInstall(ep, 1); err != nil {
		t.Fatalf("default tag provisional: %v", err)
	}
	if err := local.ProvisionalInstall(ep, 1); !errors.Is(err, ErrSEDPInvalidDomain) {
		t.Fatalf("tagged provisional untagged err = %v, want ErrSEDPInvalidDomain", err)
	}
}

// TestSEDPRemoteTopicsInstall pins Topics-only install without endpoints.
func TestSEDPRemoteTopicsInstall(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xE2)
	peer := mustSEDPIndexWithPrefix(t, 0xE3)
	ep := mustSEDPWriter(t, peer, "/chatter", "RIHS01_topics_install")

	topicsRaw, err := SerializeSEDPTopicsDATA(ep.ID.Prefix, SEDPTopic{DDSTopic: "rt/chatter", TypeDDS: "std_msgs::msg::dds_::String_"})
	if err != nil {
		t.Fatalf("serialize topics: %v", err)
	}

	if err := local.ingestSample(topicsRaw); err != nil {
		t.Fatalf("ingest topics-only: %v", err)
	}
	topics := local.RemoteTopics()
	if len(topics) != 1 {
		t.Fatalf("remote topics = %d, want 1", len(topics))
	}
	if topics[0].DDSTopic != "rt/chatter" || topics[0].TypeDDS != "std_msgs::msg::dds_::String_" {
		t.Fatalf("remote topic = %+v, want chatter String", topics[0])
	}
	if got := len(local.RemoteEndpoints()); got != 0 {
		t.Fatalf("remotes = %d, want 0 for topics-only", got)
	}
	if got := local.Generation(); got != 0 {
		t.Fatalf("generation = %d, want 0 for topics-only", got)
	}

	endpointRaw := mustEndpointDatagram(t, ep)
	mixed := append(append([]byte(nil), topicsRaw...), endpointRaw[sedpRTPSHeaderLen:]...)
	if err := local.ingestSample(mixed); err != nil {
		t.Fatalf("ingest mixed: %v", err)
	}
	if got := len(local.RemoteEndpoints()); got != 1 {
		t.Fatalf("mixed remotes = %d, want 1", got)
	}
	if got := len(local.RemoteTopics()); got != 1 {
		t.Fatalf("mixed topics = %d, want 1", got)
	}
}

// TestSEDPOverCapDatagramDrops pins the 4096 submessage bound.
func TestSEDPOverCapDatagramDrops(t *testing.T) {
	t.Parallel()

	var prefix SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = 0xE4 + byte(i)
	}
	hdr := append([]byte(nil), sedpRTPSMagic...)
	hdr = append(hdr, sedpRTPSMajor, sedpRTPSMinor, sedpVendorID0, sedpVendorID1)
	hdr = append(hdr, prefix[:]...)

	pad, err := rtps.WrapSubmessage(rtps.SubmessagePad, 0x01, nil)
	if err != nil {
		t.Fatalf("wrap pad: %v", err)
	}
	over := append([]byte(nil), hdr...)
	for range sedpMaxSubmessages + 1 {
		over = append(over, pad...)
	}
	if _, _, err := sedpFindAllEndpointDATA(over); err == nil {
		t.Fatalf("over-cap walk succeeded, want error")
	} else if !errors.Is(err, ErrSEDPInvalidDATA) {
		t.Fatalf("over-cap err = %v, want ErrSEDPInvalidDATA", err)
	}

	// A 100-pad datagram walks fine (old 64 bound is gone) and reports no
	// endpoint DATA rather than over-cap.
	under := append([]byte(nil), hdr...)
	for range 100 {
		under = append(under, pad...)
	}
	if _, _, err := sedpFindAllEndpointDATA(under); err == nil {
		t.Fatalf("pad-only walk succeeded, want no-endpoint error")
	} else if !errors.Is(err, ErrSEDPInvalidDATA) {
		t.Fatalf("pad-only err = %v, want ErrSEDPInvalidDATA", err)
	}
}

// TestSEDPSweepConcurrentSafe pins table-only Sweep under concurrency.
func TestSEDPSweepConcurrentSafe(t *testing.T) {
	t.Parallel()

	local := mustSEDPIndexWithPrefix(t, 0xE5)
	peer := mustSEDPIndexWithPrefix(t, 0xE6)
	base := mustSEDPWriter(t, peer, "/chatter", "RIHS01_race")

	var wg sync.WaitGroup
	for n := range uint32(8) {
		wg.Go(func() {
			ep := base
			ep.ID.Entity = sedpEntityFor(sedpUserEntityBase+n, false)
			if err := local.UpsertRemote(ep); err != nil {
				t.Errorf("concurrent upsert: %v", err)
			}
			now := time.Now()
			_ = local.Sweep(now)
			_ = local.ExpireProvisional(now)
			_ = local.RemoteEndpoints()
		})
	}
	wg.Wait()
}

// TestSEDPFindingsTopicsRoutesToSEDPQueue pins Topics demux routing.
//
// A Topics-only datagram (writer 0x000002c2) must enqueue to the SEDP
// queue and never to the user data queue: the Topics writer is an SEDP
// builtin, not endpoint DATA and not user DATA.
func TestSEDPFindingsTopicsRoutesToSEDPQueue(t *testing.T) {
	t.Parallel()

	backend := &Backend{
		routes: make(map[GUIDPrefix][]int),
		spdpCh: make(chan []byte, demuxQueueDepth),
		sedpCh: make(chan []byte, demuxQueueDepth),
		dataCh: make(chan dataItem, demuxQueueDepth),
	}

	var prefix SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = 0xEA + byte(i)
	}
	topicsRaw, err := SerializeSEDPTopicsDATA(prefix, SEDPTopic{DDSTopic: "rt/chatter", TypeDDS: "std_msgs::msg::dds_::String_"})
	if err != nil {
		t.Fatalf("serialize topics: %v", err)
	}

	var drops rtps.Counter
	header, subs, err := rtps.DecodeMessage(topicsRaw, &drops)
	if err != nil {
		t.Fatalf("decode topics datagram: %v", err)
	}
	if done := backend.routeBuiltinDiscovery(context.Background(), header, topicsRaw, subs, backend.spdpCh, backend.sedpCh); done {
		t.Fatalf("route cancelled, want progress")
	}
	if len(backend.sedpCh) != 1 {
		t.Fatalf("sedp queue = %d, want 1 for Topics DATA", len(backend.sedpCh))
	}
	if len(backend.spdpCh) != 0 {
		t.Fatalf("spdp queue = %d, want 0 for Topics DATA", len(backend.spdpCh))
	}
	if len(backend.dataCh) != 0 {
		t.Fatalf("data queue = %d, want 0 for Topics DATA", len(backend.dataCh))
	}
}
