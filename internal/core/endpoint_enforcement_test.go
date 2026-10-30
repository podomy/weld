// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"strings"
	"testing"
)

// TestEndpointEnforcement pins compatibility gating on creation plus live pairing.
//
// Volatile offered against transient requested stays silent with a loud
// durability event. Reverse transient local offered flows to both transient
// and volatile requests. History never drives matching but compatible
// transient local keeps retention for late joiners. Deadline lapses never
// unmatch: runtime alerts stay per outage with backward rebase and MarkSeen
// clears while deliver and retain stay true. Best available keeps
// resolve once semantics.
func TestEndpointEnforcement(t *testing.T) {
	t.Run("volatile offered stays silent", checkVolatileOfferedStaysSilent)
	t.Run("reverse transient flows", checkReverseTransientFlows)
	t.Run("history never drives matching", checkHistoryNeverDrivesMatching)
	t.Run("compatible transient keeps history", checkCompatibleTransientKeepsHistory)
	t.Run("deadline lapse keeps delivering", checkDeadlineLapseKeepsDelivering)
	t.Run("best available resolve once", checkEnforcementResolveOnce)
}

// enforcementBase returns the resolved default profile used by the gate.
func enforcementBase() QoSProfile {
	return ResolveSystemDefaults(DefaultQoS())
}

// enforcementTransient returns base with transient local durability.
func enforcementTransient(base QoSProfile) QoSProfile {
	out := base
	out.Durability = DurabilityTransientLocal
	return out
}

// checkVolatileOfferedStaysSilent pins volatile versus transient as silent.
func checkVolatileOfferedStaysSilent(t *testing.T) {
	t.Helper()
	base := enforcementBase()
	offered := base
	requested := enforcementTransient(base)
	assertPairSilent(t, offered, requested)
	assertEndpointsSilent(t, offered, requested)
	assertLivePairSilent(t, offered, requested)
}

// assertPairSilent pins profile gates report incompatible with durability.
func assertPairSilent(t *testing.T, offered, requested QoSProfile) {
	t.Helper()
	if PairShouldDeliver(offered, requested) {
		t.Fatal("volatile offered must not deliver to transient requested")
	}
	if ShouldDeliver(offered, requested) {
		t.Fatal("live gate must not deliver volatile to transient")
	}
	report := CheckCompatibility(offered, requested)
	if report.Compatible {
		t.Fatalf("volatile versus transient must be incompatible, got %q", report.Reason())
	}
	found := false
	for _, issue := range report.Errors {
		if issue.Kind == PolicyDurability {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("errors must name durability, got %q", report.ErrorText())
	}
	if ShouldRetainForLateJoiner(offered, requested) {
		t.Fatal("incompatible pair must keep no servable history")
	}
}

// assertEndpointsSilent pins endpoint gates stay silent with no retention.
func assertEndpointsSilent(t *testing.T, offered, requested QoSProfile) {
	t.Helper()
	pub, err := CreateEndpoint("/scan", "talker", EndpointPublisher, offered, nil, false)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	sub, err := CreateEndpoint("/scan", "listener", EndpointSubscription, requested, nil, false)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	report := CheckEndpointPair(&pub, &sub)
	if report.Compatible {
		t.Fatalf("endpoint pair must be incompatible, got %q", report.Reason())
	}
	if EndpointsCanDeliver(&pub, &sub) {
		t.Fatal("endpoints must not deliver volatile to transient")
	}
	if EndpointsRetainForLateJoiner(&pub, &sub) {
		t.Fatal("incompatible endpoints must keep no servable history")
	}
	if EndpointsCanDeliver(nil, &sub) || EndpointsCanDeliver(&pub, nil) {
		t.Fatal("nil endpoints must never deliver")
	}
	if EndpointsRetainForLateJoiner(nil, &sub) {
		t.Fatal("nil endpoints must never retain")
	}
}

// assertLivePairSilent pins live pairing stays silent but loud.
//
//nolint:gocognit,cyclop // pairing test sequences deliver, event, counter, and endpoint checks in one run, splitting would obscure intent
func assertLivePairSilent(t *testing.T, offered, requested QoSProfile) {
	t.Helper()
	var lines []string
	r := NewLiveMismatchReporter(nil, func(line string) {
		lines = append(lines, line)
	})
	deliver, events := r.CheckPair("/scan", "talker", "listener", offered, requested)
	if deliver {
		t.Fatal("live pair must stay silent for volatile to transient")
	}
	if len(events) != 1 {
		t.Fatalf("incompatible pair must yield one event, got %+v", events)
	}
	e := events[0]
	if e.Policy != PolicyDurability {
		t.Fatalf("policy = %q, want %q", e.Policy, PolicyDurability)
	}
	if e.Severity != SeverityError {
		t.Fatalf("severity = %q, want error", e.Severity)
	}
	if e.Topic != "/scan" || e.Publisher != "talker" || e.Subscriber != "listener" {
		t.Fatalf("event must carry topic plus endpoint pair, got %+v", e)
	}
	if !strings.Contains(e.String(), string(PolicyDurability)) || !strings.Contains(e.LogLine(), string(PolicyDurability)) {
		t.Fatalf("event shapes must carry policy kind, got %+v", e)
	}
	if !strings.Contains(e.String(), "/scan") || !strings.Contains(e.LogLine(), "talker") {
		t.Fatalf("event shapes must carry names, got %+v", e)
	}
	if got := r.Counter().Count(PolicyDurability); got != 1 {
		t.Fatalf("durability count = %d, want 1", got)
	}
	if len(lines) != 1 {
		t.Fatalf("log lines = %d, want 1", len(lines))
	}
	for _, want := range []string{"/scan", "talker", "listener", string(PolicyDurability)} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("log line misses %q: %q", want, lines[0])
		}
	}
	pub, err := CreateEndpoint("/scan", "talker", EndpointPublisher, offered, nil, false)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	sub, err := CreateEndpoint("/scan", "listener", EndpointSubscription, requested, nil, false)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	deliver, endpointEvents := r.CheckEndpoints(&pub, &sub)
	if deliver {
		t.Fatal("endpoint live pair must stay silent")
	}
	if len(endpointEvents) != 1 || endpointEvents[0].Policy != PolicyDurability {
		t.Fatalf("endpoint live pair must yield durability, got %+v", endpointEvents)
	}
	if deliverNil, _ := r.CheckEndpoints(nil, &sub); deliverNil {
		t.Fatal("nil publisher must never deliver")
	}
}

// checkReverseTransientFlows pins facade transient to both requests flows.
func checkReverseTransientFlows(t *testing.T) {
	t.Helper()
	offered := FacadeAdvertisedQoS()
	base := enforcementBase()
	transientRequested := enforcementTransient(base)
	assertReverseFlows(t, offered, transientRequested, "transient")
	assertReverseFlows(t, offered, base, "volatile")
}

// assertReverseFlows pins one reverse pair flows with no durability error.
//
//nolint:cyclop // reverse test sequences deliver, gate, and compatibility checks in one run, splitting would obscure intent
func assertReverseFlows(t *testing.T, offered, requested QoSProfile, label string) {
	t.Helper()
	if !PairShouldDeliver(offered, requested) {
		t.Fatalf("%s reverse must deliver, got %q", label, CheckCompatibility(offered, requested).Reason())
	}
	if !ShouldDeliver(offered, requested) {
		t.Fatalf("%s live gate must deliver", label)
	}
	report := CheckCompatibility(offered, requested)
	if !report.Compatible {
		t.Fatalf("%s reverse must be compatible, got %q", label, report.Reason())
	}
	for _, issue := range report.Errors {
		if issue.Kind == PolicyDurability {
			t.Fatalf("%s reverse must not raise durability, got %q", label, report.ErrorText())
		}
	}
	if !ShouldRetainForLateJoiner(offered, requested) {
		t.Fatalf("%s reverse with transient offer must retain", label)
	}
	pub, err := CreateEndpoint("/scan", "weld-pub", EndpointPublisher, offered, nil, false)
	if err != nil {
		t.Fatalf("%s create publisher: %v", label, err)
	}
	sub, err := CreateEndpoint("/scan", "ros-echo", EndpointSubscription, requested, nil, false)
	if err != nil {
		t.Fatalf("%s create subscription: %v", label, err)
	}
	if !EndpointsCanDeliver(&pub, &sub) {
		t.Fatalf("%s endpoints must deliver", label)
	}
	if !EndpointsRetainForLateJoiner(&pub, &sub) {
		t.Fatalf("%s endpoints must retain", label)
	}
	r := NewLiveMismatchReporter(nil, nil)
	deliver, events := r.CheckPair("/scan", "weld-pub", "ros-echo", offered, requested)
	if !deliver {
		t.Fatalf("%s live pair must deliver, got %+v", label, events)
	}
	if len(events) != 0 {
		t.Fatalf("%s compatible pair must emit nothing, got %+v", label, events)
	}
	deliver, events = r.CheckEndpoints(&pub, &sub)
	if !deliver || len(events) != 0 {
		t.Fatalf("%s endpoint live pair must deliver cleanly, got %v %+v", label, deliver, events)
	}
}

// checkHistoryNeverDrivesMatching pins history and depth never change verdict.
func checkHistoryNeverDrivesMatching(t *testing.T) {
	t.Helper()
	base := enforcementBase()
	histories := []HistoryPolicy{HistoryKeepLast, HistoryKeepAll}
	depths := []uint64{1, 10, 100}
	for _, history := range histories {
		for _, depth := range depths {
			assertHistoryPairVerdict(t, base, history, depth)
		}
	}
}

// assertHistoryPairVerdict pins one history and depth keeps verdict stable.
func assertHistoryPairVerdict(t *testing.T, base QoSProfile, history HistoryPolicy, depth uint64) {
	t.Helper()
	volatileOffered := base
	volatileOffered.History = history
	volatileOffered.Depth = depth
	transientRequested := enforcementTransient(base)
	transientRequested.History = history
	transientRequested.Depth = depth
	if PairShouldDeliver(volatileOffered, transientRequested) {
		t.Fatalf("history %s depth %d must not rescue volatile to transient", history, depth)
	}
	report := CheckCompatibility(volatileOffered, transientRequested)
	found := false
	for _, issue := range report.Errors {
		if issue.Kind == PolicyDurability {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("history %s depth %d must keep durability error, got %q", history, depth, report.ErrorText())
	}
	facade := FacadeAdvertisedQoS()
	volatileRequested := base
	volatileRequested.History = history
	volatileRequested.Depth = depth
	if !PairShouldDeliver(facade, volatileRequested) {
		t.Fatalf("history %s depth %d must not break transient to volatile", history, depth)
	}
	if !PairShouldDeliver(facade, transientRequested) {
		t.Fatalf("history %s depth %d must not break transient to transient", history, depth)
	}
}

// checkCompatibleTransientKeepsHistory pins retention serves late joiners.
func checkCompatibleTransientKeepsHistory(t *testing.T) {
	t.Helper()
	offered := FacadeAdvertisedQoS()
	base := enforcementBase()
	requested := base
	assertRetentionServesLateJoiner(t, offered, requested)
	transientRequested := enforcementTransient(base)
	assertRetentionServesLateJoiner(t, offered, transientRequested)
	assertIncompatibleServesNothing(t)
}

// assertRetentionServesLateJoiner pins compatible history snapshot serves.
func assertRetentionServesLateJoiner(t *testing.T, offered, requested QoSProfile) {
	t.Helper()
	if !ShouldRetainForLateJoiner(offered, requested) {
		t.Fatal("compatible transient offer must retain")
	}
	capacity, err := RetentionCapacity(offered)
	if err != nil {
		t.Fatalf("retention capacity: %v", err)
	}
	cache := NewHistoryCache[int](capacity)
	samples := []int{1, 2, 3}
	for _, v := range samples {
		cache.Push(v)
	}
	snap := cache.Snapshot()
	if len(snap) != len(samples) {
		t.Fatalf("late joiner snapshot must hold %d samples, got %v", len(samples), snap)
	}
	for i, want := range samples {
		if snap[i] != want {
			t.Fatalf("snapshot must be oldest first, got %v", snap)
		}
	}
	snap[0] = 99
	if again := cache.Snapshot(); again[0] != samples[0] {
		t.Fatalf("snapshot must copy, got %v", again)
	}
}

// assertIncompatibleServesNothing pins incompatible stays silent on history.
func assertIncompatibleServesNothing(t *testing.T) {
	t.Helper()
	base := enforcementBase()
	offered := base
	requested := enforcementTransient(base)
	if ShouldDeliver(offered, requested) {
		t.Fatal("incompatible pair must not deliver")
	}
	if ShouldRetainForLateJoiner(offered, requested) {
		t.Fatal("incompatible pair must not retain")
	}
	capacity, err := RetentionCapacity(offered)
	if err != nil {
		t.Fatalf("retention capacity: %v", err)
	}
	cache := NewHistoryCache[int](capacity)
	deliver := ShouldDeliver(offered, requested)
	if deliver {
		t.Fatal("incompatible deliver flag must be false")
	}
	snap := cache.Snapshot()
	if snap == nil || len(snap) != 0 {
		t.Fatalf("silent pair must serve empty history, got %+v", snap)
	}
}

// checkDeadlineLapseKeepsDelivering pins runtime lapse never unmatches.
//
// A compatible transient pair keeps delivering and retaining across a
// deadline lapse while the tracker stays per outage: first lapse alerts,
// repeat stays quiet, MarkSeen clears for the next outage, and backward
// rebase clears without unmatching.
func checkDeadlineLapseKeepsDelivering(t *testing.T) {
	t.Helper()
	offered := FacadeAdvertisedQoS()
	base := enforcementBase()
	pub, err := CreateEndpoint("/scan", "weld-pub", EndpointPublisher, offered, nil, false)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	sub, err := CreateEndpoint("/scan", "ros-echo", EndpointSubscription, base, nil, false)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	assertDeadlinePairKeepsServing(t, &pub, &sub)
	tracker := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 0)
	if _, ok := tracker.Check(50000000); ok {
		t.Fatal("fresh window must not alert")
	}
	if _, ok := tracker.Check(100000000); !ok {
		t.Fatal("first lapse must alert")
	}
	assertDeadlinePairKeepsServing(t, &pub, &sub)
	if _, ok := tracker.Check(150000000); ok {
		t.Fatal("repeat in same outage must stay quiet")
	}
	assertDeadlinePairKeepsServing(t, &pub, &sub)
	tracker.MarkSeen(500000000)
	if _, ok := tracker.Check(550000000); ok {
		t.Fatal("fresh window after seen must not alert")
	}
	if _, ok := tracker.Check(600000000); !ok {
		t.Fatal("next outage must alert once")
	}
	assertDeadlinePairKeepsServing(t, &pub, &sub)
	rebased := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 200000000)
	if _, ok := rebased.Check(100000000); ok {
		t.Fatal("backward jump must not alert")
	}
	if _, ok := rebased.Check(200000000); !ok {
		t.Fatal("rebased window must alert after a full deadline")
	}
	assertDeadlinePairKeepsServing(t, &pub, &sub)
}

// assertDeadlinePairKeepsServing pins deliver and retain stay true across lapse.
func assertDeadlinePairKeepsServing(t *testing.T, pub, sub *Endpoint) {
	t.Helper()
	if !EndpointsCanDeliver(pub, sub) {
		t.Fatalf("deadline lapse must not unmatch, got %q", CheckEndpointPair(pub, sub).Reason())
	}
	if !EndpointsRetainForLateJoiner(pub, sub) {
		t.Fatal("deadline lapse must not drop retention")
	}
}

// checkEnforcementResolveOnce pins stored profile never moves on discovery.
func checkEnforcementResolveOnce(t *testing.T) {
	t.Helper()
	first := []Peer{{Name: "pub-a", Profile: FacadeAdvertisedQoS()}}
	endpoint, err := CreateEndpoint("/odom", "sub", EndpointSubscription, BestAvailableQoS(), first, false)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	before := endpoint.ActualQoS()
	beforePeers := endpoint.VisiblePeers()
	endpoint.ObserveDiscovery([]Peer{{Name: "pub-b", Profile: SensorDataQoS()}})
	endpoint.ObserveDiscovery(nil)
	if after := endpoint.ActualQoS(); after != before {
		t.Fatalf("later discovery changed actual: before %+v after %+v", before, after)
	}
	afterPeers := endpoint.VisiblePeers()
	if len(afterPeers) != len(beforePeers) || afterPeers[0] != beforePeers[0] {
		t.Fatalf("later discovery changed peer set: %+v", afterPeers)
	}
	if got := endpoint.LaterDiscoveryCount(); got != 2 {
		t.Fatalf("later discovery count must be 2, got %d", got)
	}
	pub, err := CreateEndpoint("/odom", "pub", EndpointPublisher, BestAvailableQoS(), first, false)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	if !EndpointsCanDeliver(&pub, &endpoint) {
		t.Fatalf("resolved pair must deliver, got %q", CheckEndpointPair(&pub, &endpoint).Reason())
	}
}
