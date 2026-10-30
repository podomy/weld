// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package fastdds_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/fastdds"
)

// resolvedProfile returns a concrete profile for forward tests.
func resolvedProfile() core.QoSProfile {
	return core.QoSProfile{
		History:       core.HistoryKeepLast,
		Depth:         10,
		Reliability:   core.ReliabilityReliable,
		Durability:    core.DurabilityVolatile,
		Deadline:      core.NewDuration(1, 0),
		Lifespan:      core.NewDuration(2, 0),
		Liveliness:    core.LivelinessAutomatic,
		LeaseDuration: core.NewDuration(9, 0),
	}
}

// mustForward maps a profile and fails the test on error.
func mustForward(t *testing.T, p core.QoSProfile, hash string) fastdds.FastDDSQoS {
	t.Helper()

	out, err := fastdds.ForwardQoS(p, hash)
	if err != nil {
		t.Fatalf("ForwardQoS: %v", err)
	}

	return out
}

// TestMapDiscoveryRangeOff checks off uses minimal allocation with dummy unicast.
func TestMapDiscoveryRangeOff(t *testing.T) {
	t.Parallel()

	got, err := fastdds.MapDiscoveryRange(transports.DiscoveryRangeOff)
	if err != nil {
		t.Fatalf("MapDiscoveryRange off: %v", err)
	}

	if !got.MinimalAllocation {
		t.Fatalf("off MinimalAllocation is false")
	}

	if got.UseMulticast {
		t.Fatalf("off UseMulticast is true")
	}

	if !got.UseUDP {
		t.Fatalf("off UseUDP is false")
	}

	if len(got.Peers) != 1 || got.Peers[0] != fastdds.DummyUnicastPeer {
		t.Fatalf("off Peers = %v, want [%q]", got.Peers, fastdds.DummyUnicastPeer)
	}
}

// TestMapDiscoveryRangeLocalhost checks localhost clears multicast with SHM plus loopback.
func TestMapDiscoveryRangeLocalhost(t *testing.T) {
	t.Parallel()

	got, err := fastdds.MapDiscoveryRange(transports.DiscoveryRangeLocalhost)
	if err != nil {
		t.Fatalf("MapDiscoveryRange localhost: %v", err)
	}

	if got.UseMulticast {
		t.Fatalf("localhost UseMulticast is true")
	}

	if !got.UseSharedMemory {
		t.Fatalf("localhost UseSharedMemory is false")
	}

	if !got.UseUDP {
		t.Fatalf("localhost UseUDP is false")
	}

	if len(got.Peers) != 1 || got.Peers[0] != fastdds.LoopbackPeer {
		t.Fatalf("localhost Peers = %v, want [%q]", got.Peers, fastdds.LoopbackPeer)
	}

	if !fastdds.RequiresCycloneInteropCheck(got) {
		t.Fatalf("localhost needs Cyclone interop check")
	}
}

// TestMapDiscoveryRangeSubnet checks subnet keeps peers plus multicast group.
func TestMapDiscoveryRangeSubnet(t *testing.T) {
	t.Parallel()

	got, err := fastdds.MapDiscoveryRange(transports.DiscoveryRangeSubnet)
	if err != nil {
		t.Fatalf("MapDiscoveryRange subnet: %v", err)
	}

	if !got.UseMulticast {
		t.Fatalf("subnet UseMulticast is false")
	}

	if got.MinimalAllocation {
		t.Fatalf("subnet MinimalAllocation is true")
	}

	if len(got.Peers) != 1 || got.Peers[0] != fastdds.MulticastGroup {
		t.Fatalf("subnet Peers = %v, want [%q]", got.Peers, fastdds.MulticastGroup)
	}

	if fastdds.RequiresCycloneInteropCheck(got) {
		t.Fatalf("subnet needs no Cyclone interop check")
	}
}

// TestMapDiscoveryRangeSystemDefault checks system default defers to middleware.
func TestMapDiscoveryRangeSystemDefault(t *testing.T) {
	t.Parallel()

	got, err := fastdds.MapDiscoveryRange(transports.DiscoveryRangeSystemDefault)
	if err != nil {
		t.Fatalf("MapDiscoveryRange system_default: %v", err)
	}

	if !got.UseMulticast || !got.UseSharedMemory || !got.UseUDP {
		t.Fatalf("system_default = %+v, want all transports on", got)
	}

	if len(got.Peers) != 0 {
		t.Fatalf("system_default Peers = %v, want empty", got.Peers)
	}

	if got.MinimalAllocation {
		t.Fatalf("system_default MinimalAllocation is true")
	}
}

// TestMapDiscoveryRangeNotSet checks NotSet needs explicit normalization.
func TestMapDiscoveryRangeNotSet(t *testing.T) {
	t.Parallel()

	_, err := fastdds.MapDiscoveryRange(transports.DiscoveryRangeNotSet)
	if err == nil {
		t.Fatalf("MapDiscoveryRange not_set succeeded, want error")
	}

	normalized := fastdds.NormalizeDiscoveryRange(transports.DiscoveryRangeNotSet)
	if normalized != transports.DiscoveryRangeSystemDefault {
		t.Fatalf("NormalizeDiscoveryRange not_set = %v, want system_default", normalized)
	}

	got, err := fastdds.MapDiscoveryRange(normalized)
	if err != nil {
		t.Fatalf("MapDiscoveryRange normalized: %v", err)
	}

	if !got.UseMulticast {
		t.Fatalf("normalized UseMulticast is false")
	}
}

// TestMapDiscoveryRangeUnknown checks unknown ranges fail loudly.
func TestMapDiscoveryRangeUnknown(t *testing.T) {
	t.Parallel()

	_, err := fastdds.MapDiscoveryRange(transports.DiscoveryRange(99))
	if err == nil {
		t.Fatalf("MapDiscoveryRange 99 succeeded, want error")
	}
}

// TestDiscoveryTransportForConfig checks config normalization maps to system default.
func TestDiscoveryTransportForConfig(t *testing.T) {
	t.Parallel()

	cfg := fastdds.Config{Range: transports.DiscoveryRangeNotSet}

	got, err := cfg.DiscoveryTransportForConfig()
	if err != nil {
		t.Fatalf("DiscoveryTransportForConfig: %v", err)
	}

	if !got.UseMulticast {
		t.Fatalf("normalized config UseMulticast is false")
	}
}

// TestForwardQoSReliable checks reliable maps with infinite blocking.
func TestForwardQoSReliable(t *testing.T) {
	t.Parallel()

	out := mustForward(t, resolvedProfile(), "")

	if out.Reliability != fastdds.ReliabilityReliable {
		t.Fatalf("reliability = %v, want reliable", out.Reliability)
	}

	if !out.MaxBlockingTime.IsInfinite() {
		t.Fatalf("reliable blocking = %+v, want infinite", out.MaxBlockingTime)
	}
}

// TestForwardQoSBestEffort checks best effort maps with zero blocking.
func TestForwardQoSBestEffort(t *testing.T) {
	t.Parallel()

	profile := resolvedProfile()
	profile.Reliability = core.ReliabilityBestEffort

	out := mustForward(t, profile, "")

	if out.Reliability != fastdds.ReliabilityBestEffort {
		t.Fatalf("reliability = %v, want best_effort", out.Reliability)
	}

	if !out.MaxBlockingTime.IsZero() {
		t.Fatalf("best effort blocking = %+v, want zero", out.MaxBlockingTime)
	}
}

// TestForwardQoSKeepLastDepth checks keep last preserves depth.
func TestForwardQoSKeepLastDepth(t *testing.T) {
	t.Parallel()

	profile := resolvedProfile()
	profile.Depth = 7

	out := mustForward(t, profile, "")

	if out.History != fastdds.HistoryKeepLast {
		t.Fatalf("history = %v, want keep_last", out.History)
	}

	if out.Depth != 7 {
		t.Fatalf("depth = %d, want 7", out.Depth)
	}
}

// TestForwardQoSKeepAll checks keep all maps with ignored depth.
func TestForwardQoSKeepAll(t *testing.T) {
	t.Parallel()

	profile := resolvedProfile()
	profile.History = core.HistoryKeepAll
	profile.Depth = 0

	out := mustForward(t, profile, "")

	if out.History != fastdds.HistoryKeepAll {
		t.Fatalf("history = %v, want keep_all", out.History)
	}

	back, err := fastdds.ReverseQoS(out)
	if err != nil {
		t.Fatalf("ReverseQoS keep all: %v", err)
	}

	if back.History != core.HistoryKeepAll {
		t.Fatalf("reverse history = %v, want keep_all", back.History)
	}
}

// TestForwardQoSDurabilityKindOnly checks durability sets kind only.
func TestForwardQoSDurabilityKindOnly(t *testing.T) {
	t.Parallel()

	profile := resolvedProfile()
	profile.Durability = core.DurabilityTransientLocal

	out := mustForward(t, profile, "")

	if out.Durability != fastdds.DurabilityTransientLocal {
		t.Fatalf("durability = %v, want transient_local", out.Durability)
	}

	if uint64(out.Depth) != profile.Depth {
		t.Fatalf("transient local changed depth to %d", out.Depth)
	}
}

// TestForwardQoSLiveliness checks liveliness mapping plus two thirds period.
func TestForwardQoSLiveliness(t *testing.T) {
	t.Parallel()

	profile := resolvedProfile()
	profile.Liveliness = core.LivelinessManualByTopic
	profile.LeaseDuration = core.NewDuration(9, 0)

	out := mustForward(t, profile, "")

	if out.Liveliness != fastdds.LivelinessManualByTopic {
		t.Fatalf("liveliness = %v, want manual_by_topic", out.Liveliness)
	}

	if !out.AnnouncementIsSet {
		t.Fatalf("announcement is not set for 9s lease")
	}

	period, set, err := fastdds.AnnouncementPeriodForLease(profile.LeaseDuration)
	if err != nil {
		t.Fatalf("AnnouncementPeriodForLease: %v", err)
	}

	if !set {
		t.Fatalf("period set is false for 9s lease")
	}

	if period != 6*time.Second {
		t.Fatalf("period = %v, want 6s", period)
	}
}

// TestForwardQoSFacade checks ignorelocal, autodispose, XCDR1, and hash passthrough.
func TestForwardQoSFacade(t *testing.T) {
	t.Parallel()

	out := mustForward(t, resolvedProfile(), "RIHS01_abc")

	if !out.IgnoreLocalParticipant {
		t.Fatalf("IgnoreLocalParticipant is false")
	}

	if out.AutodisposeUnregisteredInstances {
		t.Fatalf("Autodispose is true, want false")
	}

	if len(out.DataRepresentation) != 1 || out.DataRepresentation[0] != fastdds.DataRepresentationXCDR1 {
		t.Fatalf("representation = %v, want [XCDR1]", out.DataRepresentation)
	}

	if out.TypeHash != "RIHS01_abc" {
		t.Fatalf("TypeHash = %q, want passthrough", out.TypeHash)
	}
}

// TestForwardQoSRejectsUnresolved checks FastDDS late failure becomes early error.
func TestForwardQoSRejectsUnresolved(t *testing.T) {
	t.Parallel()

	cases := []core.QoSProfile{
		func() core.QoSProfile {
			p := resolvedProfile()
			p.History = core.HistorySystemDefault
			return p
		}(),
		func() core.QoSProfile {
			p := resolvedProfile()
			p.Reliability = core.ReliabilitySystemDefault
			return p
		}(),
		func() core.QoSProfile {
			p := resolvedProfile()
			p.Durability = core.DurabilitySystemDefault
			return p
		}(),
		func() core.QoSProfile {
			p := resolvedProfile()
			p.Liveliness = core.LivelinessSystemDefault
			return p
		}(),
		func() core.QoSProfile {
			p := resolvedProfile()
			p.Reliability = core.ReliabilityUnknown
			return p
		}(),
		func() core.QoSProfile {
			p := resolvedProfile()
			p.History = core.HistoryKeepLast
			p.Depth = 0
			return p
		}(),
	}

	for i, profile := range cases {
		_, err := fastdds.ForwardQoS(profile, "")
		if err == nil {
			t.Fatalf("case %d succeeded, want resolution error", i)
		}
	}
}

// TestForwardLeaseUnspecifiedIsDefault checks unspecified lease maps to DDS default.
func TestForwardLeaseUnspecifiedIsDefault(t *testing.T) {
	t.Parallel()

	lease := core.UnspecifiedDuration()

	got, isDefault, err := fastdds.ForwardLeaseDuration(lease)
	if err != nil {
		t.Fatalf("ForwardLeaseDuration unspecified: %v", err)
	}

	if !isDefault {
		t.Fatalf("unspecified IsDefault is false, want true")
	}

	if !got.IsInfinite() {
		t.Fatalf("unspecified encoding = %+v, want infinite DDS default", got)
	}

	profile := resolvedProfile()
	profile.LeaseDuration = core.UnspecifiedDuration()

	out := mustForward(t, profile, "")

	if !out.LeaseIsDefault {
		t.Fatalf("ForwardQoS unspecified LeaseIsDefault is false")
	}

	back, err := fastdds.ReverseQoS(out)
	if err != nil {
		t.Fatalf("ReverseQoS default lease: %v", err)
	}

	if !back.LeaseDuration.IsUnspecified() {
		t.Fatalf("reverse default lease = %v, want unspecified", back.LeaseDuration)
	}
}

// TestForwardLeaseInfiniteIsNotDefault checks resolved infinite stays explicit.
func TestForwardLeaseInfiniteIsNotDefault(t *testing.T) {
	t.Parallel()

	got, isDefault, err := fastdds.ForwardLeaseDuration(core.InfiniteDuration())
	if err != nil {
		t.Fatalf("ForwardLeaseDuration infinite: %v", err)
	}

	if isDefault {
		t.Fatalf("infinite IsDefault is true, want false")
	}

	if !got.IsInfinite() {
		t.Fatalf("infinite encoding = %+v, want infinite", got)
	}
}

// TestAnnouncementPeriodTwoThirds checks finite leases synthesize two thirds.
func TestAnnouncementPeriodTwoThirds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		lease core.Duration
		want  time.Duration
	}{
		{lease: core.NewDuration(9, 0), want: 6 * time.Second},
		{lease: core.NewDuration(3, 0), want: 2 * time.Second},
		{lease: core.NewDuration(1, 500000000), want: time.Second},
	}

	for _, tc := range cases {
		got, set, err := fastdds.AnnouncementPeriodForLease(tc.lease)
		if err != nil {
			t.Fatalf("lease %v: %v", tc.lease, err)
		}

		if !set {
			t.Fatalf("lease %v set is false", tc.lease)
		}

		if got != tc.want {
			t.Fatalf("lease %v period = %v, want %v", tc.lease, got, tc.want)
		}
	}
}

// TestAnnouncementPeriodUnset checks default and infinite leases carry no period.
func TestAnnouncementPeriodUnset(t *testing.T) {
	t.Parallel()

	leases := []core.Duration{
		core.UnspecifiedDuration(),
		core.InfiniteDuration(),
		core.BestAvailableDuration(),
	}

	for _, lease := range leases {
		_, set, err := fastdds.AnnouncementPeriodForLease(lease)
		if err != nil {
			t.Fatalf("lease %v: %v", lease, err)
		}

		if set {
			t.Fatalf("lease %v set is true, want false", lease)
		}
	}
}

// TestAnnouncementPeriodRejectsMalformed checks negative leases fail loudly.
func TestAnnouncementPeriodRejectsMalformed(t *testing.T) {
	t.Parallel()

	_, _, err := fastdds.AnnouncementPeriodForLease(core.Duration{Seconds: -1})
	if err == nil {
		t.Fatalf("negative lease succeeded, want error")
	}
}

// TestReverseTransientBecomesUnknown checks transient and persistent reverse to unknown.
func TestReverseTransientBecomesUnknown(t *testing.T) {
	t.Parallel()

	base := mustForward(t, resolvedProfile(), "")

	for _, kind := range []fastdds.DurabilityKind{fastdds.DurabilityTransient, fastdds.DurabilityPersistent, fastdds.DurabilityUnknown} {
		base.Durability = kind

		back, err := fastdds.ReverseQoS(base)
		if err != nil {
			t.Fatalf("durability %v: %v", kind, err)
		}

		if back.Durability != core.DurabilityUnknown {
			t.Fatalf("durability %v reverse = %v, want unknown", kind, back.Durability)
		}
	}
}

// TestReverseRoundTrip checks forward then reverse preserves the profile.
func TestReverseRoundTrip(t *testing.T) {
	t.Parallel()

	profile := resolvedProfile()

	out := mustForward(t, profile, "RIHS01_abc")

	back, err := fastdds.ReverseQoS(out)
	if err != nil {
		t.Fatalf("ReverseQoS: %v", err)
	}

	if back.History != profile.History || back.Depth != profile.Depth {
		t.Fatalf("history round trip = %v depth %d", back.History, back.Depth)
	}

	if back.Reliability != profile.Reliability {
		t.Fatalf("reliability round trip = %v", back.Reliability)
	}

	if back.Durability != profile.Durability {
		t.Fatalf("durability round trip = %v", back.Durability)
	}

	if back.Liveliness != profile.Liveliness {
		t.Fatalf("liveliness round trip = %v", back.Liveliness)
	}

	if back.Deadline.Compare(profile.Deadline) != 0 {
		t.Fatalf("deadline round trip = %v", back.Deadline)
	}

	if back.LeaseDuration.Compare(profile.LeaseDuration) != 0 {
		t.Fatalf("lease round trip = %v", back.LeaseDuration)
	}
}

// TestReverseRejectsNonXCDR1 checks representation drift fails loudly.
func TestReverseRejectsNonXCDR1(t *testing.T) {
	t.Parallel()

	out := mustForward(t, resolvedProfile(), "")
	out.DataRepresentation = []fastdds.DataRepresentation{"XCDR2"}

	_, err := fastdds.ReverseQoS(out)
	if err == nil {
		t.Fatalf("reverse XCDR2 succeeded, want error")
	}
}

// TestCoreDurationClamp checks large durations saturate to infinite.
func TestCoreDurationClamp(t *testing.T) {
	t.Parallel()

	big := core.NewDuration(3000000000, 0)

	got, err := fastdds.CoreDurationToFastDDS(big)
	if err != nil {
		t.Fatalf("CoreDurationToFastDDS large: %v", err)
	}

	if !got.IsInfinite() {
		t.Fatalf("large duration = %+v, want infinite", got)
	}

	back, err := fastdds.FastDDSDurationToCore(got)
	if err != nil {
		t.Fatalf("FastDDSDurationToCore infinite: %v", err)
	}

	if !back.IsInfinite() {
		t.Fatalf("back = %v, want infinite", back)
	}
}

// TestLookupXMLProfile checks per topic selection with client fallback.
func TestLookupXMLProfile(t *testing.T) {
	t.Parallel()

	profiles := map[string]core.QoSProfile{
		"/chatter":                        resolvedProfile(),
		fastdds.ClientFallbackProfileName: core.SensorDataQoS(),
	}

	got, ok := fastdds.LookupXMLProfile(profiles, "/chatter", false)
	if !ok {
		t.Fatalf("exact topic not found")
	}

	if got.Depth != 10 {
		t.Fatalf("exact depth = %d", got.Depth)
	}

	fallback, ok := fastdds.LookupXMLProfile(profiles, "/missing", true)
	if !ok {
		t.Fatalf("client fallback not found")
	}

	if fallback.Reliability != core.ReliabilityBestEffort {
		t.Fatalf("fallback reliability = %v", fallback.Reliability)
	}

	_, ok = fastdds.LookupXMLProfile(profiles, "/missing", false)
	if ok {
		t.Fatalf("non client fallback succeeded, want not found")
	}

	_, ok = fastdds.LookupXMLProfile(profiles, "   ", true)
	if ok {
		t.Fatalf("empty topic succeeded, want not found")
	}
}

// TestEffectiveQoSCodeWins checks code QoS wins over XML.
func TestEffectiveQoSCodeWins(t *testing.T) {
	t.Parallel()

	code := resolvedProfile()
	xml := core.SensorDataQoS()

	effective, ignored := fastdds.EffectiveQoS(code, xml, true)
	if !ignored {
		t.Fatalf("ignored is false with XML present")
	}

	if effective.Reliability != core.ReliabilityReliable {
		t.Fatalf("effective reliability = %v, want code reliable", effective.Reliability)
	}

	effective, ignored = fastdds.EffectiveQoS(code, core.QoSProfile{}, false)
	if ignored {
		t.Fatalf("ignored is true without XML")
	}

	if effective.Depth != code.Depth {
		t.Fatalf("effective depth = %d, want code %d", effective.Depth, code.Depth)
	}
}

// TestDefaultPublicationMode checks sync default for cross vendor paths.
func TestDefaultPublicationMode(t *testing.T) {
	t.Parallel()

	if fastdds.DefaultPublicationMode() != fastdds.PublicationModeSync {
		t.Fatalf("default mode is not sync")
	}

	cfg := fastdds.Config{}

	if cfg.EffectivePublicationMode() != fastdds.PublicationModeSync {
		t.Fatalf("empty mode effective is not sync")
	}
}

// TestForcedDataRepresentation checks XCDR1 is forced.
func TestForcedDataRepresentation(t *testing.T) {
	t.Parallel()

	reps := fastdds.ForcedDataRepresentation()
	if len(reps) != 1 || reps[0] != fastdds.DataRepresentationXCDR1 {
		t.Fatalf("reps = %v, want [XCDR1]", reps)
	}
}

// TestBackendStubLifecycle checks stub join, wire stub, and close.
func TestBackendStubLifecycle(t *testing.T) {
	t.Parallel()

	backend, err := fastdds.NewBackend(fastdds.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("JoinDiscovery: %v", err)
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, fastdds.ErrFastDDSNotImplemented) {
		t.Fatalf("publish = %v, want ErrFastDDSNotImplemented", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, fastdds.ErrFastDDSNotImplemented) {
		t.Fatalf("take = %v, want ErrFastDDSNotImplemented", err)
	}

	reps := backend.DataRepresentation()
	if len(reps) != 1 || reps[0] != fastdds.DataRepresentationXCDR1 {
		t.Fatalf("backend reps = %v, want [XCDR1]", reps)
	}

	transport, err := backend.DiscoveryTransport()
	if err != nil {
		t.Fatalf("DiscoveryTransport: %v", err)
	}

	if !transport.UseMulticast {
		t.Fatalf("default transport multicast is off")
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("second Close: %v", err)
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("publish after close = %v, want ErrBackendClosed", err)
	}
}

// TestBackendRequiresJoin checks data path before join reports not joined.
func TestBackendRequiresJoin(t *testing.T) {
	t.Parallel()

	backend, err := fastdds.NewBackend(fastdds.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("publish before join = %v, want ErrNotJoined", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("take before join = %v, want ErrNotJoined", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p", Generation: 1, Alive: true})
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("report before join = %v, want ErrNotJoined", err)
	}
}

// TestBackendLivelinessWatermark checks generation ordering on the stub.
func TestBackendLivelinessWatermark(t *testing.T) {
	t.Parallel()

	backend, err := fastdds.NewBackend(fastdds.DefaultConfig())
	if err != nil {
		t.Fatalf("NewBackend: %v", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if err != nil {
		t.Fatalf("JoinDiscovery: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: true})
	if err != nil {
		t.Fatalf("gen 7: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: true})
	if err != nil {
		t.Fatalf("repeat same gen: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 7, Alive: false})
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("conflicting reuse = %v, want ErrStaleGeneration", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "talker", Generation: 6, Alive: true})
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("backward gen = %v, want ErrStaleGeneration", err)
	}
}
