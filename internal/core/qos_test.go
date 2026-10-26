// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"strings"
	"sync"
	"testing"
)

// TestQoS covers the facade model: the advertised profile, best available
// resolution, endpoint stability, loud mismatch, deadline monitoring, and
// bounded retention.
func TestQoS(t *testing.T) {
	t.Run("advertised facade", checkAdvertisedFacade)
	t.Run("subscriber best available", checkSubscriberBestAvailable)
	t.Run("publisher best available", checkPublisherBestAvailable)
	t.Run("zero peers", checkZeroPeers)
	t.Run("services replace best available", checkServicesReplaceBestAvailable)
	t.Run("services partial markers", checkServicesPartialMarkers)
	t.Run("lifespan sentinel", checkLifespanSentinel)
	t.Run("unresolved peers", checkUnresolvedPeers)
	t.Run("mixed durations", checkMixedDurations)
	t.Run("negative deadline peer", checkNegativeDeadlinePeer)
	t.Run("endpoint stability", checkEndpointStability)
	t.Run("discovery zero value", checkDiscoveryZeroValue)
	t.Run("actual getters expose resolved", checkActualGetters)
	t.Run("avoid flag round trip", checkAvoidFlagRoundTrip)
	t.Run("avoid best available service", checkAvoidBestAvailableService)
	t.Run("input visible aliasing", checkInputVisibleAliasing)
	t.Run("mismatch is loud", checkMismatchIsLoud)
	t.Run("loudness table", checkLoudnessTable)
	t.Run("counter zero value and concurrency", checkCounterZeroValueConcurrency)
	t.Run("nil record", checkNilRecord)
	t.Run("deadline monitoring", checkDeadlineMonitoring)
	t.Run("deadline infinite negative backward", checkDeadlineInfiniteNegativeBackward)
	t.Run("deadline race", checkDeadlineRace)
	t.Run("bounded retention", checkBoundedRetention)
	t.Run("per drop loud", checkPerDropLoud)
	t.Run("slow consumer large", checkSlowConsumerLarge)
	t.Run("clear preserves lost", checkClearPreservesLost)
	t.Run("producer slowdown", checkProducerSlowdown)
	t.Run("snapshot nil versus empty", checkSnapshotNilVersusEmpty)
	t.Run("retention capacity", checkRetentionCapacity)
	t.Run("retention round trip", checkRetentionRoundTrip)
	t.Run("sentinel survival", checkSentinelSurvival)
	t.Run("string invalid kinds", checkStringInvalidKinds)
	t.Run("delivery behavior", checkDeliveryBehavior)
}

// checkAdvertisedFacade pins reliable plus transient local bounded keep last
// and its compatibility with every standard subscription.
func checkAdvertisedFacade(t *testing.T) {
	t.Helper()
	got := FacadeAdvertisedQoS()
	if got.Reliability != ReliabilityReliable || got.Durability != DurabilityTransientLocal {
		t.Fatalf("facade must advertise reliable transient local, got %+v", got)
	}
	if got.History != HistoryKeepLast || got.Depth != FacadeHistoryDepth {
		t.Fatalf("facade must advertise bounded keep last, got %+v", got)
	}
	err := Validate(got)
	if err != nil {
		t.Fatalf("facade profile must be creation ready: %v", err)
	}
	subscriptions := []struct {
		name string
		want QoSProfile
	}{
		{"default", DefaultQoS()},
		{"sensor data", SensorDataQoS()},
		{"parameters", ParametersQoS()},
		{"services default", ServicesDefaultQoS()},
		{"parameter events", ParameterEventsQoS()},
		{"rosout", RosoutQoS()},
	}
	for _, tc := range subscriptions {
		requested := ResolveSystemDefaults(tc.want)
		report := CheckCompatibility(got, requested)
		if !report.Compatible {
			t.Fatalf("facade must satisfy %s subscription: %q", tc.name, report.Reason())
		}
	}
}

// checkSubscriberBestAvailable pins subscriber side resolution from peers.
func checkSubscriberBestAvailable(t *testing.T) {
	t.Helper()
	peers := []Peer{
		{Name: "pub-a", Profile: QoSProfile{Reliability: ReliabilityReliable, Durability: DurabilityTransientLocal, Liveliness: LivelinessManualByTopic, Deadline: NewDuration(0, 20000000), LeaseDuration: NewDuration(4, 0)}},
		{Name: "pub-b", Profile: QoSProfile{Reliability: ReliabilityReliable, Durability: DurabilityTransientLocal, Liveliness: LivelinessManualByTopic, Deadline: NewDuration(0, 40000000), LeaseDuration: NewDuration(8, 0)}},
	}
	endpoint, err := CreateEndpoint("/scan", "sub", EndpointSubscription, BestAvailableQoS(), peers, false)
	if err != nil {
		t.Fatalf("create subscriber: %v", err)
	}
	actual := endpoint.ActualQoS()
	if actual.Reliability != ReliabilityReliable {
		t.Fatalf("all reliable peers must resolve reliable, got %s", actual.Reliability)
	}
	if actual.Durability != DurabilityTransientLocal {
		t.Fatalf("all transient local peers must resolve transient local, got %s", actual.Durability)
	}
	if actual.Liveliness != LivelinessManualByTopic {
		t.Fatalf("all manual peers must resolve manual, got %s", actual.Liveliness)
	}
	if actual.Deadline != NewDuration(0, 40000000) {
		t.Fatalf("subscriber deadline must take the maximum, got %s", actual.Deadline)
	}
	if actual.LeaseDuration != NewDuration(8, 0) {
		t.Fatalf("subscriber lease must take the maximum, got %s", actual.LeaseDuration)
	}
	if !actual.Lifespan.IsUnspecified() {
		t.Fatalf("subscriber lifespan must resolve unspecified, got %s", actual.Lifespan)
	}

	mixed := []Peer{
		{Name: "reliable", Profile: QoSProfile{Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}},
		{Name: "best effort", Profile: QoSProfile{Reliability: ReliabilityBestEffort, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}},
	}
	endpoint, err = CreateEndpoint("/scan", "sub", EndpointSubscription, BestAvailableQoS(), mixed, false)
	if err != nil {
		t.Fatalf("create mixed subscriber: %v", err)
	}
	actual = endpoint.ActualQoS()
	if actual.Reliability != ReliabilityBestEffort {
		t.Fatalf("mixed reliability must resolve best effort, got %s", actual.Reliability)
	}
	if actual.Durability != DurabilityVolatile {
		t.Fatalf("volatile peer must resolve volatile, got %s", actual.Durability)
	}
	if actual.Liveliness != LivelinessAutomatic {
		t.Fatalf("automatic peer must resolve automatic, got %s", actual.Liveliness)
	}
}

// checkPublisherBestAvailable pins publisher side resolution from peers.
func checkPublisherBestAvailable(t *testing.T) {
	t.Helper()
	peers := []Peer{
		{Name: "sub-a", Profile: QoSProfile{Reliability: ReliabilityBestEffort, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic, Deadline: NewDuration(0, 20000000), LeaseDuration: NewDuration(4, 0)}},
		{Name: "sub-b", Profile: QoSProfile{Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessManualByTopic, Deadline: NewDuration(0, 40000000), LeaseDuration: NewDuration(8, 0)}},
	}
	endpoint, err := CreateEndpoint("/cmd_vel", "pub", EndpointPublisher, BestAvailableQoS(), peers, false)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	actual := endpoint.ActualQoS()
	if actual.Reliability != ReliabilityReliable || actual.Durability != DurabilityTransientLocal {
		t.Fatalf("publisher must always offer reliable transient local, got %+v", actual)
	}
	if actual.Liveliness != LivelinessManualByTopic {
		t.Fatalf("manual subscriber must resolve manual, got %s", actual.Liveliness)
	}
	if actual.Deadline != NewDuration(0, 20000000) {
		t.Fatalf("publisher deadline must take the minimum, got %s", actual.Deadline)
	}
	if actual.LeaseDuration != NewDuration(4, 0) {
		t.Fatalf("publisher lease must take the minimum, got %s", actual.LeaseDuration)
	}
	if !actual.Lifespan.IsUnspecified() {
		t.Fatalf("publisher lifespan must resolve unspecified, got %s", actual.Lifespan)
	}

	automatic := []Peer{{Name: "sub", Profile: ResolveSystemDefaults(DefaultQoS())}}
	endpoint, err = CreateEndpoint("/cmd_vel", "pub", EndpointPublisher, BestAvailableQoS(), automatic, false)
	if err != nil {
		t.Fatalf("create automatic publisher: %v", err)
	}
	if got := endpoint.ActualQoS().Liveliness; got != LivelinessAutomatic {
		t.Fatalf("automatic subscribers must resolve automatic, got %s", got)
	}
}

// checkZeroPeers pins the documented empty peer set answers: volatile,
// automatic, and best effort, matching the non empty else branches.
func checkZeroPeers(t *testing.T) {
	t.Helper()
	assertPeerlessSubscriber(t)
	assertPeerlessPublisher(t)
}

// assertPeerlessSubscriber pins peerless subscriber resolves to defaults.
func assertPeerlessSubscriber(t *testing.T) {
	t.Helper()
	endpoint, err := CreateEndpoint("/scan", "sub", EndpointSubscription, BestAvailableQoS(), nil, false)
	if err != nil {
		t.Fatalf("create peerless subscriber: %v", err)
	}
	assertPeerlessSubPolicies(t, endpoint.ActualQoS())
}

// assertPeerlessSubPolicies pins subscriber reliability, durability, liveliness.
func assertPeerlessSubPolicies(t *testing.T, actual QoSProfile) {
	t.Helper()
	if actual.Reliability != ReliabilityBestEffort {
		t.Fatalf("no peers must resolve best effort, got %s", actual.Reliability)
	}
	if actual.Durability != DurabilityVolatile {
		t.Fatalf("no peers must resolve volatile, got %s", actual.Durability)
	}
	if actual.Liveliness != LivelinessAutomatic {
		t.Fatalf("no peers must resolve automatic, got %s", actual.Liveliness)
	}
	assertPeerlessSubDurations(t, actual)
}

// assertPeerlessSubDurations pins subscriber durations resolve unspecified.
func assertPeerlessSubDurations(t *testing.T, actual QoSProfile) {
	t.Helper()
	if !actual.Deadline.IsUnspecified() || !actual.LeaseDuration.IsUnspecified() {
		t.Fatalf("no peers must resolve default durations, got %+v", actual)
	}
	if !actual.Lifespan.IsUnspecified() {
		t.Fatalf("no peers must resolve unspecified lifespan, got %+v", actual)
	}
}

// assertPeerlessPublisher pins peerless publisher resolves to offer defaults.
func assertPeerlessPublisher(t *testing.T) {
	t.Helper()
	pub, err := CreateEndpoint("/scan", "pub", EndpointPublisher, BestAvailableQoS(), nil, false)
	if err != nil {
		t.Fatalf("create peerless publisher: %v", err)
	}
	assertPeerlessPubPolicies(t, pub.ActualQoS())
}

// assertPeerlessPubPolicies pins publisher offer policies and durations.
func assertPeerlessPubPolicies(t *testing.T, pubActual QoSProfile) {
	t.Helper()
	if pubActual.Reliability != ReliabilityReliable || pubActual.Durability != DurabilityTransientLocal {
		t.Fatalf("peerless publisher must offer reliable transient local, got %+v", pubActual)
	}
	if pubActual.Liveliness != LivelinessAutomatic {
		t.Fatalf("no peers must resolve automatic on offer, got %s", pubActual.Liveliness)
	}
	if !pubActual.Deadline.IsUnspecified() || !pubActual.LeaseDuration.IsUnspecified() || !pubActual.Lifespan.IsUnspecified() {
		t.Fatalf("no peers must resolve default durations on offer, got %+v", pubActual)
	}
}

// checkServicesReplaceBestAvailable pins the services default substitution
// for a fully marked profile.
func checkServicesReplaceBestAvailable(t *testing.T) {
	t.Helper()
	peers := []Peer{{Name: "client", Profile: FacadeAdvertisedQoS()}}
	endpoint, err := CreateEndpoint("/add_two_ints", "server", EndpointSubscription, BestAvailableQoS(), peers, true)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	if actual := endpoint.ActualQoS(); actual != ResolveSystemDefaults(ServicesDefaultQoS()) {
		t.Fatalf("service best available must become services default, got %+v", actual)
	}
	if !endpoint.IsService() {
		t.Fatal("service flag must survive creation")
	}

	explicit, err := CreateEndpoint("/add_two_ints", "server", EndpointSubscription, SensorDataQoS(), peers, true)
	if err != nil {
		t.Fatalf("create explicit service: %v", err)
	}
	actual, want := explicit.ActualQoS(), ResolveSystemDefaults(SensorDataQoS())
	if actual != want {
		t.Fatalf("explicit service profile must survive: got %+v want %+v", actual, want)
	}
}

// checkServicesPartialMarkers pins per field service substitution: only
// marker fields take the services default, explicit fields survive.
func checkServicesPartialMarkers(t *testing.T) {
	t.Helper()
	sentinel := BestAvailableDuration()
	requested := ResolveSystemDefaults(DefaultQoS())
	requested.Reliability = ReliabilityBestAvailable
	requested.Liveliness = LivelinessBestAvailable
	requested.Deadline = sentinel
	requested.Durability = DurabilityTransientLocal
	requested.LeaseDuration = NewDuration(9, 0)
	requested.Lifespan = NewDuration(5, 0)
	peers := []Peer{{Name: "client", Profile: FacadeAdvertisedQoS()}}
	_, err := CreateEndpoint("/svc", "server", EndpointSubscription, requested, peers, false)
	if err != nil {
		t.Fatalf("create non service baseline: %v", err)
	}
	svc, err := CreateEndpoint("/svc", "server", EndpointSubscription, requested, peers, true)
	if err != nil {
		t.Fatalf("create partial marker service: %v", err)
	}
	actual := svc.ActualQoS()
	svcDefault := ResolveSystemDefaults(ServicesDefaultQoS())
	if actual.Reliability != svcDefault.Reliability {
		t.Fatalf("marker reliability must take services default, got %s", actual.Reliability)
	}
	if actual.Liveliness != svcDefault.Liveliness {
		t.Fatalf("marker liveliness must take services default, got %s", actual.Liveliness)
	}
	if actual.Deadline != svcDefault.Deadline {
		t.Fatalf("marker deadline must take services default, got %s", actual.Deadline)
	}
	if actual.Durability != DurabilityTransientLocal {
		t.Fatalf("explicit durability must survive, got %s", actual.Durability)
	}
	if actual.LeaseDuration != NewDuration(9, 0) {
		t.Fatalf("explicit lease must survive, got %s", actual.LeaseDuration)
	}
	if actual.Lifespan != NewDuration(5, 0) {
		t.Fatalf("explicit lifespan must survive, got %s", actual.Lifespan)
	}
}

// checkLifespanSentinel pins that best available lifespan resolves to
// unspecified in both directions and never reaches the wire.
func checkLifespanSentinel(t *testing.T) {
	t.Helper()
	if !IsBestAvailableDuration(BestAvailableQoS().Lifespan) {
		t.Fatal("best available preset must carry the lifespan sentinel")
	}
	if !hasBestAvailable(BestAvailableQoS()) {
		t.Fatal("lifespan sentinel must count as best available")
	}
	peers := []Peer{{Name: "peer", Profile: FacadeAdvertisedQoS()}}
	sub, err := CreateEndpoint("/scan", "sub", EndpointSubscription, BestAvailableQoS(), peers, false)
	if err != nil {
		t.Fatalf("create subscriber: %v", err)
	}
	if !sub.ActualQoS().Lifespan.IsUnspecified() {
		t.Fatalf("subscriber lifespan must resolve unspecified, got %s", sub.ActualQoS().Lifespan)
	}
	pub, err := CreateEndpoint("/scan", "pub", EndpointPublisher, BestAvailableQoS(), peers, false)
	if err != nil {
		t.Fatalf("create publisher: %v", err)
	}
	if !pub.ActualQoS().Lifespan.IsUnspecified() {
		t.Fatalf("publisher lifespan must resolve unspecified, got %s", pub.ActualQoS().Lifespan)
	}
}

// checkUnresolvedPeers pins that system default, best available, and unknown
// peers count as non concrete and fall to the else branch.
func checkUnresolvedPeers(t *testing.T) {
	t.Helper()
	cases := []struct {
		name    string
		profile QoSProfile
	}{
		{"system default", SystemDefaultQoS()},
		{"best available", BestAvailableQoS()},
		{"unknown", UnknownQoS()},
	}
	for _, tc := range cases {
		peers := []Peer{{Name: tc.name, Profile: tc.profile}}
		endpoint, err := CreateEndpoint("/scan", "sub", EndpointSubscription, BestAvailableQoS(), peers, false)
		if err != nil {
			t.Fatalf("%s peer: create subscriber: %v", tc.name, err)
		}
		actual := endpoint.ActualQoS()
		if actual.Reliability != ReliabilityBestEffort {
			t.Fatalf("%s peer must resolve best effort, got %s", tc.name, actual.Reliability)
		}
		if actual.Durability != DurabilityVolatile {
			t.Fatalf("%s peer must resolve volatile, got %s", tc.name, actual.Durability)
		}
		if actual.Liveliness != LivelinessAutomatic {
			t.Fatalf("%s peer must resolve automatic, got %s", tc.name, actual.Liveliness)
		}
		if !actual.Deadline.IsUnspecified() || !actual.LeaseDuration.IsUnspecified() {
			t.Fatalf("%s peer must resolve default durations, got %+v", tc.name, actual)
		}
	}
}

// checkMixedDurations pins duration folding: all default to unspecified,
// mixed default plus finite to default on request and minimum on offer,
// all finite to the extremum, and zero peers to unspecified.
func checkMixedDurations(t *testing.T) {
	t.Helper()
	def := ResolveSystemDefaults(DefaultQoS())
	finite20 := NewDuration(0, 20000000)
	finite40 := NewDuration(0, 40000000)
	assertAllDefaultDurations(t, def)
	assertMixedDurations(t, def, finite40)
	assertAllFiniteDurations(t, def, finite20, finite40)
	assertZeroPeersDuration(t)
	assertSentinelPeersDuration(t, def, finite20)
}

// withFiniteProfile returns def with deadline and lease set to d.
func withFiniteProfile(def QoSProfile, d Duration) QoSProfile {
	p := def
	p.Deadline = d
	p.LeaseDuration = d
	return p
}

// assertAllDefaultDurations pins all default peers resolve unspecified.
func assertAllDefaultDurations(t *testing.T, def QoSProfile) {
	t.Helper()
	allDefault := []Peer{
		{Name: "a", Profile: def},
		{Name: "b", Profile: def},
	}
	sub, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), allDefault, false)
	if err != nil {
		t.Fatalf("all default sub: %v", err)
	}
	if !sub.ActualQoS().Deadline.IsUnspecified() {
		t.Fatalf("all default request must resolve unspecified, got %s", sub.ActualQoS().Deadline)
	}
	pub, err := CreateEndpoint("/t", "pub", EndpointPublisher, BestAvailableQoS(), allDefault, false)
	if err != nil {
		t.Fatalf("all default pub: %v", err)
	}
	if !pub.ActualQoS().Deadline.IsUnspecified() {
		t.Fatalf("all default offer must resolve unspecified, got %s", pub.ActualQoS().Deadline)
	}
}

// assertMixedDurations pins mixed default plus finite folding.
func assertMixedDurations(t *testing.T, def QoSProfile, finite40 Duration) {
	t.Helper()
	mixed := []Peer{
		{Name: "default", Profile: def},
		{Name: "finite", Profile: withFiniteProfile(def, finite40)},
	}
	sub, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), mixed, false)
	if err != nil {
		t.Fatalf("mixed sub: %v", err)
	}
	if !sub.ActualQoS().Deadline.IsUnspecified() {
		t.Fatalf("mixed default plus finite request must collapse to default, got %s", sub.ActualQoS().Deadline)
	}
	pub, err := CreateEndpoint("/t", "pub", EndpointPublisher, BestAvailableQoS(), mixed, false)
	if err != nil {
		t.Fatalf("mixed pub: %v", err)
	}
	if pub.ActualQoS().Deadline != finite40 {
		t.Fatalf("mixed default plus finite offer must resolve finite minimum, got %s", pub.ActualQoS().Deadline)
	}
}

// assertAllFiniteDurations pins all finite peers take extremum.
func assertAllFiniteDurations(t *testing.T, def QoSProfile, finite20, finite40 Duration) {
	t.Helper()
	allFinite := []Peer{
		{Name: "a", Profile: withFiniteProfile(def, finite20)},
		{Name: "b", Profile: withFiniteProfile(def, finite40)},
	}
	sub, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), allFinite, false)
	if err != nil {
		t.Fatalf("all finite sub: %v", err)
	}
	if sub.ActualQoS().Deadline != finite40 {
		t.Fatalf("all finite request must take maximum, got %s", sub.ActualQoS().Deadline)
	}
	pub, err := CreateEndpoint("/t", "pub", EndpointPublisher, BestAvailableQoS(), allFinite, false)
	if err != nil {
		t.Fatalf("all finite pub: %v", err)
	}
	if pub.ActualQoS().Deadline != finite20 {
		t.Fatalf("all finite offer must take minimum, got %s", pub.ActualQoS().Deadline)
	}
}

// assertZeroPeersDuration pins zero peers resolve unspecified.
func assertZeroPeersDuration(t *testing.T) {
	t.Helper()
	zeroSub, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), nil, false)
	if err != nil {
		t.Fatalf("zero sub: %v", err)
	}
	if !zeroSub.ActualQoS().Deadline.IsUnspecified() {
		t.Fatalf("zero peers must resolve unspecified, got %s", zeroSub.ActualQoS().Deadline)
	}
}

// assertSentinelPeersDuration pins sentinel peers count as default.
func assertSentinelPeersDuration(t *testing.T, def QoSProfile, finite20 Duration) {
	t.Helper()
	sentinelPeers := []Peer{
		{Name: "sentinel", Profile: BestAvailableQoS()},
		{Name: "finite", Profile: withFiniteProfile(def, finite20)},
	}
	sub, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), sentinelPeers, false)
	if err != nil {
		t.Fatalf("sentinel peer sub: %v", err)
	}
	if !sub.ActualQoS().Deadline.IsUnspecified() {
		t.Fatalf("sentinel peer must count as default on request, got %s", sub.ActualQoS().Deadline)
	}
	pub, err := CreateEndpoint("/t", "pub", EndpointPublisher, BestAvailableQoS(), sentinelPeers, false)
	if err != nil {
		t.Fatalf("sentinel peer pub: %v", err)
	}
	if pub.ActualQoS().Deadline != finite20 {
		t.Fatalf("sentinel peer must count as default on offer, got %s", pub.ActualQoS().Deadline)
	}
}

// checkEndpointStability proves later discovery never updates the endpoint.
func checkEndpointStability(t *testing.T) {
	t.Helper()
	first := []Peer{{Name: "pub-a", Profile: FacadeAdvertisedQoS()}}
	endpoint, err := CreateEndpoint("/odom", "sub", EndpointSubscription, BestAvailableQoS(), first, false)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	before := endpoint.ActualQoS()
	beforePeers := endpoint.VisiblePeers()
	assertDiscoveryStable(t, &endpoint, before, beforePeers)
	assertEndpointIdentity(t, &endpoint)
	assertConcurrentDiscoveryStable(t, &endpoint, before)
}

// assertDiscoveryStable pins two observations leave QoS and peers unchanged.
func assertDiscoveryStable(t *testing.T, endpoint *Endpoint, before QoSProfile, beforePeers []Peer) {
	t.Helper()
	endpoint.ObserveDiscovery([]Peer{{Name: "pub-b", Profile: SensorDataQoS()}})
	endpoint.ObserveDiscovery(nil)
	if after := endpoint.ActualQoS(); after != before {
		t.Fatalf("later discovery changed actual QoS: before %+v after %+v", before, after)
	}
	afterPeers := endpoint.VisiblePeers()
	if len(afterPeers) != len(beforePeers) || afterPeers[0] != beforePeers[0] {
		t.Fatalf("later discovery changed the recorded peer set: %+v", afterPeers)
	}
	if got := endpoint.LaterDiscoveryCount(); got != 2 {
		t.Fatalf("later discovery count must be 2, got %d", got)
	}
}

// assertEndpointIdentity pins topic, name, kind, requested getters.
func assertEndpointIdentity(t *testing.T, endpoint *Endpoint) {
	t.Helper()
	if endpoint.Topic() != "/odom" || endpoint.Name() != "sub" || endpoint.Kind() != EndpointSubscription {
		t.Fatal("endpoint identity getters must round trip")
	}
	if endpoint.RequestedQoS() != BestAvailableQoS() {
		t.Fatal("requested getter must return the configured profile")
	}
}

// assertConcurrentDiscoveryStable pins concurrent observations total 10.
func assertConcurrentDiscoveryStable(t *testing.T, endpoint *Endpoint, before QoSProfile) {
	t.Helper()
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			endpoint.ObserveDiscovery(nil)
		})
	}
	wg.Wait()
	if got := endpoint.LaterDiscoveryCount(); got != 10 {
		t.Fatalf("concurrent observations must total 10, got %d", got)
	}
	if after := endpoint.ActualQoS(); after != before {
		t.Fatalf("concurrent discovery changed actual QoS: %+v", after)
	}
}

// checkActualGetters asserts stored profiles carry no unresolved markers.
func checkActualGetters(t *testing.T) {
	t.Helper()
	peers := []Peer{{Name: "pub", Profile: FacadeAdvertisedQoS()}}
	endpoint, err := CreateEndpoint("/tf", "sub", EndpointSubscription, BestAvailableQoS(), peers, false)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	err = Validate(endpoint.ActualQoS())
	if err != nil {
		t.Fatalf("actual profile must validate: %v", err)
	}
	if got := endpoint.VisiblePeers(); len(got) != 1 || got[0].Name != "pub" {
		t.Fatalf("visible peers must round trip, got %+v", got)
	}
	if _, err := CreateEndpoint("", "sub", EndpointSubscription, DefaultQoS(), nil, false); err == nil {
		t.Fatal("empty topic must fail creation")
	}
	if _, err := CreateEndpoint("/tf", "", EndpointSubscription, DefaultQoS(), nil, false); err == nil {
		t.Fatal("empty name must fail creation")
	}
	if _, err := CreateEndpoint("/tf", "sub", EndpointKind(99), DefaultQoS(), nil, false); err == nil {
		t.Fatal("invalid kind must fail creation")
	}
	if _, err := CreateEndpoint("/tf", "sub", EndpointSubscription, UnknownQoS(), nil, false); err == nil {
		t.Fatal("unknown must fail creation")
	}
}

// checkAvoidFlagRoundTrip pins that the avoidance flag passes through
// resolution and endpoint creation unchanged without affecting matching.
func checkAvoidFlagRoundTrip(t *testing.T) {
	t.Helper()
	base := ResolveSystemDefaults(DefaultQoS())
	base.AvoidROSNamespaceConventions = true
	resolved := ResolveSystemDefaults(base)
	if !resolved.AvoidROSNamespaceConventions {
		t.Fatal("avoid flag must survive system default resolution")
	}
	endpoint, err := CreateEndpoint("/tf", "sub", EndpointSubscription, base, nil, false)
	if err != nil {
		t.Fatalf("create avoided endpoint: %v", err)
	}
	if !endpoint.ActualQoS().AvoidROSNamespaceConventions {
		t.Fatal("avoid flag must survive endpoint creation")
	}
	if !endpoint.RequestedQoS().AvoidROSNamespaceConventions {
		t.Fatal("requested getter must preserve avoid flag")
	}
	plain := ResolveSystemDefaults(DefaultQoS())
	report := CheckCompatibility(base, plain)
	if !report.Compatible {
		t.Fatalf("avoid flag must never affect matching: %q", report.Reason())
	}
}

// checkInputVisibleAliasing pins that input mutation cannot rewrite history
// and output mutation cannot alias internal state.
func checkInputVisibleAliasing(t *testing.T) {
	t.Helper()
	input := []Peer{{Name: "a", Profile: FacadeAdvertisedQoS()}}
	endpoint, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), input, false)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	input[0].Name = "mutated"
	input[0].Profile.Reliability = ReliabilityBestEffort
	visible := endpoint.VisiblePeers()
	if visible[0].Name != "a" {
		t.Fatalf("input mutation must not rewrite history, got %+v", visible)
	}
	visible[0].Name = "output-mutated"
	again := endpoint.VisiblePeers()
	if again[0].Name != "a" {
		t.Fatalf("output mutation must not alias internal state, got %+v", again)
	}
}

// checkMismatchIsLoud asserts events carry topic, endpoint pair, and policy
// kind across logs, metrics, and CLI shapes.
func checkMismatchIsLoud(t *testing.T) {
	t.Helper()
	base := ResolveSystemDefaults(DefaultQoS())
	events := mismatchErrorEvents(t, base)
	assertMismatchEventsLoud(t, events)
	assertMismatchCounter(t, events)
	assertMismatchWarnings(t, base)
}

// mismatchErrorEvents builds two error events for reliability plus durability.
func mismatchErrorEvents(t *testing.T, base QoSProfile) []MismatchEvent {
	t.Helper()
	offered := base
	offered.Reliability = ReliabilityBestEffort
	offered.Durability = DurabilityVolatile
	requested := base
	requested.Reliability = ReliabilityReliable
	requested.Durability = DurabilityTransientLocal
	events := CompatibilityEvents("/scan", "talker", "listener", offered, requested)
	if len(events) != 2 {
		t.Fatalf("expected two error events, got %+v", events)
	}
	return events
}

// assertMismatchEventsLoud pins events carry names, policy, severity.
func assertMismatchEventsLoud(t *testing.T, events []MismatchEvent) {
	t.Helper()
	kinds := map[PolicyKind]bool{}
	for _, e := range events {
		assertMismatchEventLoud(t, e)
		kinds[e.Policy] = true
	}
	if !kinds[PolicyReliability] || !kinds[PolicyDurability] {
		t.Fatalf("expected reliability plus durability events, got %+v", events)
	}
}

// assertMismatchEventLoud pins one event carries topic, pair, policy.
func assertMismatchEventLoud(t *testing.T, e MismatchEvent) {
	t.Helper()
	if e.Topic != "/scan" || e.Publisher != "talker" || e.Subscriber != "listener" {
		t.Fatalf("event must carry topic plus endpoint pair, got %+v", e)
	}
	if e.Severity != SeverityError {
		t.Fatalf("incompatibility must be error severity, got %+v", e)
	}
	assertMismatchShapesCarry(t, e)
}

// assertMismatchShapesCarry pins string and log line carry policy and names.
func assertMismatchShapesCarry(t *testing.T, e MismatchEvent) {
	t.Helper()
	if !strings.Contains(e.String(), string(e.Policy)) || !strings.Contains(e.LogLine(), string(e.Policy)) {
		t.Fatalf("event shapes must carry policy kind, got %+v", e)
	}
	if !strings.Contains(e.LogLine(), "/scan") || !strings.Contains(e.String(), "talker") {
		t.Fatalf("event shapes must carry names, got %+v", e)
	}
}

// assertMismatchCounter pins reliability count 1 and total 2.
func assertMismatchCounter(t *testing.T, events []MismatchEvent) {
	t.Helper()
	counter := NewMismatchCounter()
	for _, e := range events {
		counter.Record(e)
	}
	if got := counter.Count(PolicyReliability); got != 1 {
		t.Fatalf("reliability count must be 1, got %d", got)
	}
	if got := counter.Total(); got != 2 {
		t.Fatalf("total must be 2, got %d", got)
	}
}

// assertMismatchWarnings pins unresolved warn and clean emits nothing.
func assertMismatchWarnings(t *testing.T, base QoSProfile) {
	t.Helper()
	warnEvents := CompatibilityEvents("/scan", "talker", "listener", UnknownQoS(), base)
	if len(warnEvents) == 0 || warnEvents[0].Severity != SeverityWarning {
		t.Fatalf("unresolved markers must warn, got %+v", warnEvents)
	}
	if clean := CompatibilityEvents("/scan", "talker", "listener", base, base); len(clean) != 0 {
		t.Fatalf("clean pairs must emit nothing, got %+v", clean)
	}
}

// checkCounterZeroValueConcurrency pins that the zero value counter works
// and that concurrent Record calls stay race free.
func checkCounterZeroValueConcurrency(t *testing.T) {
	t.Helper()
	var zero MismatchCounter
	event := MismatchEvent{Topic: "/t", Publisher: "p", Subscriber: "s", Policy: PolicyReliability, Severity: SeverityError}
	assertZeroCounter(t, &zero, event)
	assertNilCounter(t)
	assertConcurrentCounter(t, event, &zero)
}

// assertZeroCounter pins zero value counts one record.
func assertZeroCounter(t *testing.T, zero *MismatchCounter, event MismatchEvent) {
	t.Helper()
	zero.Record(event)
	if got := zero.Count(PolicyReliability); got != 1 {
		t.Fatalf("zero value count must be 1, got %d", got)
	}
	if got := zero.Total(); got != 1 {
		t.Fatalf("zero value total must be 1, got %d", got)
	}
}

// assertNilCounter pins nil counter reports zero.
func assertNilCounter(t *testing.T) {
	t.Helper()
	var nilCounter *MismatchCounter
	if got := nilCounter.Count(PolicyReliability); got != 0 {
		t.Fatalf("nil count must be 0, got %d", got)
	}
	if got := nilCounter.Total(); got != 0 {
		t.Fatalf("nil total must be 0, got %d", got)
	}
}

// assertConcurrentCounter pins concurrent records total 16 and 17.
func assertConcurrentCounter(t *testing.T, event MismatchEvent, zero *MismatchCounter) {
	t.Helper()
	counter := NewMismatchCounter()
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			counter.Record(event)
			zero.Record(event)
		})
	}
	wg.Wait()
	if got := counter.Total(); got != 16 {
		t.Fatalf("concurrent total must be 16, got %d", got)
	}
	if got := zero.Total(); got != 17 {
		t.Fatalf("zero concurrent total must be 17, got %d", got)
	}
}

// checkDeadlineMonitoring asserts alerts fire with names and never unmatch.
func checkDeadlineMonitoring(t *testing.T) {
	t.Helper()
	tracker := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 0)
	assertFreshWindowQuiet(t, &tracker)
	assertLapsedWindowAlerts(t, &tracker)
	assertSeenRestartsWindow(t, &tracker)
	assertQuietTracker(t)
	assertBackwardJumpQuiet(t)
}

// assertFreshWindowQuiet pins fresh sample does not alert.
func assertFreshWindowQuiet(t *testing.T, tracker *DeadlineTracker) {
	t.Helper()
	if _, ok := tracker.Check(50000000); ok {
		t.Fatal("fresh sample must not alert")
	}
}

// assertLapsedWindowAlerts pins lapsed window alerts with names.
func assertLapsedWindowAlerts(t *testing.T, tracker *DeadlineTracker) {
	t.Helper()
	alert, ok := tracker.Check(100000000)
	if !ok {
		t.Fatal("lapsed window must alert")
	}
	if alert.Topic != "/scan" || alert.Endpoint != "listener" {
		t.Fatalf("alert must carry topic plus endpoint, got %+v", alert)
	}
	if !strings.Contains(alert.String(), "/scan") || !strings.Contains(alert.String(), "listener") {
		t.Fatalf("alert string must carry names, got %q", alert.String())
	}
}

// assertSeenRestartsWindow pins seen sample restarts window.
func assertSeenRestartsWindow(t *testing.T, tracker *DeadlineTracker) {
	t.Helper()
	tracker.MarkSeen(100000000)
	if _, ok := tracker.Check(150000000); ok {
		t.Fatal("seen sample must restart the window")
	}
}

// assertQuietTracker pins unspecified never alerts and getters round trip.
func assertQuietTracker(t *testing.T) {
	t.Helper()
	quiet := NewDeadlineTracker("/scan", "listener", UnspecifiedDuration(), 0)
	if _, ok := quiet.Check(1 << 60); ok {
		t.Fatal("unspecified deadline must never alert")
	}
	if quiet.Topic() != "/scan" || quiet.Endpoint() != "listener" {
		t.Fatal("tracker getters must round trip")
	}
	if got := quiet.Deadline(); !got.IsUnspecified() {
		t.Fatalf("tracker deadline getter must round trip, got %s", got)
	}
}

// assertBackwardJumpQuiet pins backward jump does not alert.
func assertBackwardJumpQuiet(t *testing.T) {
	t.Helper()
	backward := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 200000000)
	if _, ok := backward.Check(100000000); ok {
		t.Fatal("backward jump must not fire a false alert")
	}
}

// checkDeadlineInfiniteNegativeBackward pins infinite and negative quiet
// plus backward reset with recovery and MarkSeen guarding.
func checkDeadlineInfiniteNegativeBackward(t *testing.T) {
	t.Helper()
	infinite := NewDeadlineTracker("/scan", "listener", InfiniteDuration(), 0)
	if _, ok := infinite.Check(1 << 60); ok {
		t.Fatal("infinite deadline must never alert")
	}

	negative := NewDeadlineTracker("/scan", "listener", Duration{Seconds: -1}, 0)
	if _, ok := negative.Check(1 << 60); ok {
		t.Fatal("negative deadline must never alert")
	}

	backward := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 200000000)
	if _, ok := backward.Check(100000000); ok {
		t.Fatal("backward jump must not alert")
	}
	if _, ok := backward.Check(150000000); ok {
		t.Fatal("recovered window must not alert early")
	}
	if _, ok := backward.Check(200000000); !ok {
		t.Fatal("recovered window must alert after a full deadline")
	}

	guard := NewDeadlineTracker("/scan", "listener", NewDuration(0, 100000000), 0)
	guard.MarkSeen(200000000)
	guard.MarkSeen(100000000)
	if _, ok := guard.Check(250000000); ok {
		t.Fatal("backward MarkSeen must not rewind the window")
	}
	if _, ok := guard.Check(300000000); !ok {
		t.Fatal("forward window must still alert after guarded MarkSeen")
	}
}

// checkBoundedRetention asserts drop oldest with lost counters and late
// joiner snapshots.
func checkBoundedRetention(t *testing.T) {
	t.Helper()
	cache := NewHistoryCache[int](3)
	assertFillNoEvict(t, cache)
	assertOverflowEvicts(t, cache)
	assertSnapshotOldestFirst(t, cache)
	assertZeroCapacityPanics(t)
	assertConcurrentPushes(t)
}

// assertFillNoEvict pins three pushes fill without eviction and report full.
func assertFillNoEvict(t *testing.T, cache *HistoryCache[int]) {
	t.Helper()
	for _, v := range []int{1, 2, 3} {
		if evicted := cache.Push(v); evicted {
			t.Fatalf("push %d must not evict", v)
		}
	}
	if !cache.Full() {
		t.Fatal("cache must report full at capacity")
	}
}

// assertOverflowEvicts pins overflow reports eviction and counts lost.
func assertOverflowEvicts(t *testing.T, cache *HistoryCache[int]) {
	t.Helper()
	if evicted := cache.Push(4); !evicted {
		t.Fatal("overflow push must report eviction")
	}
	if got := cache.Lost(); got != 1 {
		t.Fatalf("lost counter must be 1, got %d", got)
	}
	if got := cache.Len(); got != 3 {
		t.Fatalf("length must stay at capacity, got %d", got)
	}
}

// assertSnapshotOldestFirst pins snapshot oldest first and copy isolation.
func assertSnapshotOldestFirst(t *testing.T, cache *HistoryCache[int]) {
	t.Helper()
	snap := cache.Snapshot()
	for i, want := range []int{2, 3, 4} {
		if snap[i] != want {
			t.Fatalf("snapshot must be oldest first, got %v", snap)
		}
	}
	snap[0] = 99
	if again := cache.Snapshot(); again[0] != 2 {
		t.Fatalf("snapshot must be a copy, got %v", again)
	}
}

// assertZeroCapacityPanics pins zero capacity panics.
func assertZeroCapacityPanics(t *testing.T) {
	t.Helper()
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("zero capacity must panic")
			}
		}()
		NewHistoryCache[int](0)
	}()
}

// assertConcurrentPushes pins concurrent pushes retain 8.
func assertConcurrentPushes(t *testing.T) {
	t.Helper()
	var wg sync.WaitGroup
	raceCache := NewHistoryCache[int](16)
	for i := range 8 {
		wg.Go(func() {
			raceCache.Push(i)
			raceCache.Snapshot()
			raceCache.Full()
			raceCache.Len()
			raceCache.Lost()
		})
	}
	wg.Wait()
	if got := raceCache.Len(); got != 8 {
		t.Fatalf("concurrent pushes must retain 8, got %d", got)
	}
}

// checkSnapshotNilVersusEmpty pins empty snapshots are non nil and shallow
// copies share pointees but not the slice header.
func checkSnapshotNilVersusEmpty(t *testing.T) {
	t.Helper()
	cache := NewHistoryCache[int](4)
	snap := cache.Snapshot()
	if snap == nil {
		t.Fatal("empty snapshot must be non nil")
	}
	if len(snap) != 0 {
		t.Fatalf("empty snapshot must be empty, got %v", snap)
	}

	endpoint, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), nil, false)
	if err != nil {
		t.Fatalf("create endpoint: %v", err)
	}
	visible := endpoint.VisiblePeers()
	if visible == nil {
		t.Fatal("empty visible peers must be non nil")
	}
	if len(visible) != 0 {
		t.Fatalf("empty visible peers must be empty, got %+v", visible)
	}

	ptrCache := NewHistoryCache[*int](2)
	a, b := 1, 2
	ptrCache.Push(&a)
	ptrCache.Push(&b)
	ptrSnap := ptrCache.Snapshot()
	if len(ptrSnap) != 2 || ptrSnap[0] != &a {
		t.Fatalf("pointer snapshot must share pointees, got %+v", ptrSnap)
	}
	a = 99
	if *ptrCache.Snapshot()[0] != 99 {
		t.Fatal("shallow copy must observe pointee mutation")
	}
	c := 3
	ptrSnap[0] = &c
	if ptrCache.Snapshot()[0] == &c {
		t.Fatal("slice header mutation must not alias the cache")
	}
}

// checkRetentionCapacity pins the keep last depth and keep all bound mapping.
func checkRetentionCapacity(t *testing.T) {
	t.Helper()
	profile := ResolveSystemDefaults(DefaultQoS())
	got, err := RetentionCapacity(profile)
	if err != nil {
		t.Fatalf("retention capacity: %v", err)
	}
	if got != DefaultHistoryDepth {
		t.Fatalf("keep last capacity must be depth, got %d", got)
	}
	keepAll := QoSProfile{History: HistoryKeepAll, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}
	got, err = RetentionCapacity(keepAll)
	if err != nil {
		t.Fatalf("keep all capacity: %v", err)
	}
	if got != KeepAllRetentionBound {
		t.Fatalf("keep all must use the explicit bound, got %d", got)
	}
	if _, err := RetentionCapacity(SystemDefaultQoS()); err == nil {
		t.Fatal("unresolved history must fail retention capacity")
	}
	if _, err := RetentionCapacity(UnknownQoS()); err == nil {
		t.Fatal("unknown history must fail retention capacity")
	}
	zeroDepth := QoSProfile{History: HistoryKeepLast, Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}
	if _, err := RetentionCapacity(zeroDepth); err == nil {
		t.Fatal("keep last depth zero must fail retention capacity")
	}
	invalid := QoSProfile{History: HistoryPolicy(99), Reliability: ReliabilityReliable, Durability: DurabilityVolatile, Liveliness: LivelinessAutomatic}
	if _, err := RetentionCapacity(invalid); err == nil {
		t.Fatal("invalid history must fail retention capacity")
	}
}

// checkStringInvalidKinds pins that invalid enum values render as invalid.
func checkStringInvalidKinds(t *testing.T) {
	t.Helper()
	if got := OverflowPolicy(99).String(); got != "invalid" {
		t.Fatalf("invalid overflow must render invalid, got %q", got)
	}
	if got := EndpointKind(99).String(); got != "invalid" {
		t.Fatalf("invalid endpoint kind must render invalid, got %q", got)
	}
	if got := HistoryPolicy(99).String(); got != "invalid" {
		t.Fatalf("invalid history must render invalid, got %q", got)
	}
	if got := ReliabilityPolicy(99).String(); got != "invalid" {
		t.Fatalf("invalid reliability must render invalid, got %q", got)
	}
	if got := DurabilityPolicy(99).String(); got != "invalid" {
		t.Fatalf("invalid durability must render invalid, got %q", got)
	}
	if got := LivelinessPolicy(99).String(); got != "invalid" {
		t.Fatalf("invalid liveliness must render invalid, got %q", got)
	}
	if got := policyValue(PolicyKind("nope"), ResolveSystemDefaults(DefaultQoS())); got != "invalid" {
		t.Fatalf("invalid policy kind must render invalid, got %q", got)
	}
}

// checkDeliveryBehavior pins the core behavior behind the facade.
func checkDeliveryBehavior(t *testing.T) {
	t.Helper()
	behavior := FacadeDeliveryBehavior()
	if !behavior.Backpressure || !behavior.BoundedRetention || !behavior.DeadlineMonitoring {
		t.Fatalf("core must own behavior, got %+v", behavior)
	}
	if behavior.Overflow != OverflowDropOldest || behavior.Overflow.String() != "drop_oldest" {
		t.Fatalf("overflow must be drop oldest, got %+v", behavior)
	}
}

// checkNegativeDeadlinePeer pins that malformed negative peer durations map
// to no constraint and resolve unspecified.
func checkNegativeDeadlinePeer(t *testing.T) {
	t.Helper()
	def := ResolveSystemDefaults(DefaultQoS())
	bad := def
	bad.Deadline = Duration{Seconds: -1}
	bad.LeaseDuration = Duration{Seconds: -1}
	peers := []Peer{{Name: "bad", Profile: bad}}
	sub, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), peers, false)
	if err != nil {
		t.Fatalf("create sub with negative peer: %v", err)
	}
	if !sub.ActualQoS().Deadline.IsUnspecified() {
		t.Fatalf("negative deadline peer must resolve unspecified, got %s", sub.ActualQoS().Deadline)
	}
	if !sub.ActualQoS().LeaseDuration.IsUnspecified() {
		t.Fatalf("negative lease peer must resolve unspecified, got %s", sub.ActualQoS().LeaseDuration)
	}
	pub, err := CreateEndpoint("/t", "pub", EndpointPublisher, BestAvailableQoS(), peers, false)
	if err != nil {
		t.Fatalf("create pub with negative peer: %v", err)
	}
	if !pub.ActualQoS().Deadline.IsUnspecified() {
		t.Fatalf("negative deadline peer must resolve unspecified on offer, got %s", pub.ActualQoS().Deadline)
	}
	finite := def
	finite.Deadline = NewDuration(0, 20000000)
	finite.LeaseDuration = NewDuration(4, 0)
	mixed := []Peer{{Name: "bad", Profile: bad}, {Name: "finite", Profile: finite}}
	mixSub, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), mixed, false)
	if err != nil {
		t.Fatalf("create mixed sub: %v", err)
	}
	if !mixSub.ActualQoS().Deadline.IsUnspecified() {
		t.Fatalf("negative plus finite must collapse to default on request, got %s", mixSub.ActualQoS().Deadline)
	}
	mixPub, err := CreateEndpoint("/t", "pub", EndpointPublisher, BestAvailableQoS(), mixed, false)
	if err != nil {
		t.Fatalf("create mixed pub: %v", err)
	}
	if mixPub.ActualQoS().Deadline != NewDuration(0, 20000000) {
		t.Fatalf("negative plus finite must resolve finite minimum on offer, got %s", mixPub.ActualQoS().Deadline)
	}
}

// checkDiscoveryZeroValue pins nil receiver safety and concurrent zero value use.
func checkDiscoveryZeroValue(t *testing.T) {
	t.Helper()
	var nilEndpoint *Endpoint
	nilEndpoint.ObserveDiscovery(nil)
	if got := nilEndpoint.LaterDiscoveryCount(); got != 0 {
		t.Fatalf("nil endpoint count must be 0, got %d", got)
	}
	var zero Endpoint
	assertZeroDiscoveryInitial(t, &zero)
	assertZeroDiscoveryConcurrent(t, &zero)
}

// assertZeroDiscoveryInitial pins zero value count and peers.
func assertZeroDiscoveryInitial(t *testing.T, zero *Endpoint) {
	t.Helper()
	if got := zero.LaterDiscoveryCount(); got != 0 {
		t.Fatalf("zero value count must be 0, got %d", got)
	}
	if got := zero.VisiblePeers(); got == nil || len(got) != 0 {
		t.Fatalf("zero visible peers must be empty non nil, got %+v", got)
	}
}

// assertZeroDiscoveryConcurrent pins concurrent zero value totals 16.
func assertZeroDiscoveryConcurrent(t *testing.T, zero *Endpoint) {
	t.Helper()
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			zero.ObserveDiscovery(nil)
		})
	}
	wg.Wait()
	if got := zero.LaterDiscoveryCount(); got != 16 {
		t.Fatalf("concurrent zero value total must be 16, got %d", got)
	}
}

// checkDeadlineRace pins concurrent MarkSeen plus Check stays race free.
func checkDeadlineRace(t *testing.T) {
	t.Helper()
	tracker := NewDeadlineTracker("/scan", "listener", NewDuration(0, 50000000), 0)
	assertDeadlineRaceConcurrent(&tracker)
	assertDeadlineRaceRecovery(t, &tracker)
}

// assertDeadlineRaceConcurrent pins concurrent mark and check race free.
func assertDeadlineRaceConcurrent(tracker *DeadlineTracker) {
	var wg sync.WaitGroup
	for i := range 8 {
		v := int64(i * 10000000)
		wg.Go(func() {
			tracker.MarkSeen(v)
		})
		wg.Go(func() {
			tracker.Check(v)
		})
	}
	wg.Wait()
}

// assertDeadlineRaceRecovery pins fresh quiet then lapsed alerts after race.
func assertDeadlineRaceRecovery(t *testing.T, tracker *DeadlineTracker) {
	t.Helper()
	tracker.MarkSeen(100000000)
	if _, ok := tracker.Check(120000000); ok {
		t.Fatal("fresh window must not alert after race")
	}
	if _, ok := tracker.Check(200000000); !ok {
		t.Fatal("lapsed window must alert after race")
	}
}

// checkSlowConsumerLarge pins count bounds hold for 100 one megabyte samples.
func checkSlowConsumerLarge(t *testing.T) {
	t.Helper()
	cache := NewHistoryCache[[]byte](100)
	for i := range 100 {
		buf := make([]byte, 1<<20)
		buf[0] = byte(i)
		if evicted := cache.Push(buf); evicted {
			t.Fatalf("push %d must not evict before capacity", i)
		}
	}
	if !cache.Full() {
		t.Fatal("cache must report full at 100 large samples")
	}
	if got := cache.Len(); got != 100 {
		t.Fatalf("length must be 100, got %d", got)
	}
	if got := cache.Lost(); got != 0 {
		t.Fatalf("lost must be 0 before overflow, got %d", got)
	}
	extra := make([]byte, 1<<20)
	if evicted := cache.Push(extra); !evicted {
		t.Fatal("overflow of large samples must report eviction")
	}
	if got := cache.Lost(); got != 1 {
		t.Fatalf("lost must be 1 after large overflow, got %d", got)
	}
	if got := cache.Len(); got != 100 {
		t.Fatalf("length must stay at capacity, got %d", got)
	}
}

// checkClearPreservesLost pins Clear drops retention but keeps drop history.
func checkClearPreservesLost(t *testing.T) {
	t.Helper()
	cache := NewHistoryCache[int](3)
	assertClearOverflow(t, cache)
	assertClearResets(t, cache)
	assertClearReuse(t, cache)
}

// assertClearOverflow pins overflow counts one before clear.
func assertClearOverflow(t *testing.T, cache *HistoryCache[int]) {
	t.Helper()
	for _, v := range []int{1, 2, 3} {
		if evicted := cache.Push(v); evicted {
			t.Fatalf("push %d must not evict", v)
		}
	}
	if evicted := cache.Push(4); !evicted {
		t.Fatal("overflow must evict")
	}
	if got := cache.Lost(); got != 1 {
		t.Fatalf("lost must be 1 before clear, got %d", got)
	}
}

// assertClearResets pins clear empties but preserves lost.
func assertClearResets(t *testing.T, cache *HistoryCache[int]) {
	t.Helper()
	cache.Clear()
	if got := cache.Len(); got != 0 {
		t.Fatalf("len after clear must be 0, got %d", got)
	}
	if got := cache.Lost(); got != 1 {
		t.Fatalf("lost after clear must stay 1, got %d", got)
	}
	if cache.Full() {
		t.Fatal("cleared cache must not report full")
	}
	snap := cache.Snapshot()
	if snap == nil || len(snap) != 0 {
		t.Fatalf("cleared snapshot must be empty non nil, got %+v", snap)
	}
}

// assertClearReuse pins reuse after clear holds new samples.
func assertClearReuse(t *testing.T, cache *HistoryCache[int]) {
	t.Helper()
	for _, v := range []int{5, 6} {
		if evicted := cache.Push(v); evicted {
			t.Fatalf("push %d after clear must not evict", v)
		}
	}
	if got := cache.Len(); got != 2 {
		t.Fatalf("len after jump must be 2, got %d", got)
	}
	if got := cache.Lost(); got != 1 {
		t.Fatalf("lost after jump must stay 1, got %d", got)
	}
	jump := cache.Snapshot()
	if len(jump) != 2 || jump[0] != 5 || jump[1] != 6 {
		t.Fatalf("jump snapshot must hold new samples, got %v", jump)
	}
}

// checkProducerSlowdown pins the backpressure caller contract: Full hints,
// Push reports authoritatively, and the producer slows instead of growing.
func checkProducerSlowdown(t *testing.T) {
	t.Helper()
	cache := NewHistoryCache[int](2)
	assertProducerFill(t, cache)
	slowdowns := countProducerSlowdowns(t, cache)
	assertProducerSlowdowns(t, cache, slowdowns)
}

// assertProducerFill pins two pushes fill and hint full.
func assertProducerFill(t *testing.T, cache *HistoryCache[int]) {
	t.Helper()
	for _, v := range []int{1, 2} {
		if evicted := cache.Push(v); evicted {
			t.Fatalf("push %d must not evict", v)
		}
	}
	if !cache.Full() {
		t.Fatal("cache must hint full at capacity")
	}
}

// countProducerSlowdowns pins three overflows each hint and report.
func countProducerSlowdowns(t *testing.T, cache *HistoryCache[int]) int {
	t.Helper()
	slowdowns := 0
	for _, v := range []int{3, 4, 5} {
		if cache.Full() {
			slowdowns++
		}
		if evicted := cache.Push(v); evicted {
			slowdowns++
		}
	}
	return slowdowns
}

// assertProducerSlowdowns pins slowdown signals and bounded retention.
func assertProducerSlowdowns(t *testing.T, cache *HistoryCache[int], slowdowns int) {
	t.Helper()
	if slowdowns == 0 {
		t.Fatal("producer must observe slowdown signals")
	}
	if got := cache.Len(); got != 2 {
		t.Fatalf("length must stay bounded, got %d", got)
	}
	if got := cache.Lost(); got != 3 {
		t.Fatalf("lost must count three overflows, got %d", got)
	}
	assertProducerSnapshot(t, cache)
}

// assertProducerSnapshot pins newest two retained.
func assertProducerSnapshot(t *testing.T, cache *HistoryCache[int]) {
	t.Helper()
	snap := cache.Snapshot()
	if len(snap) != 2 || snap[0] != 4 || snap[1] != 5 {
		t.Fatalf("retention must hold newest two, got %v", snap)
	}
}

// checkLoudnessTable pins all five policy errors plus sentinel warnings
// across topic, endpoints, policy, severity, log, string, and counter.
func checkLoudnessTable(t *testing.T) {
	t.Helper()
	base := ResolveSystemDefaults(DefaultQoS())
	cases := loudnessCases(base)
	counter := NewMismatchCounter()
	assertLoudnessErrors(t, cases, counter)
	assertLoudnessCounts(t, cases, counter)
	assertLoudnessWarnings(t, base, counter)
	assertSentinelDeadlineWarns(t, base)
}

// loudnessCase is one policy error expectation.
type loudnessCase struct {
	name      string
	offered   QoSProfile
	requested QoSProfile
	policy    PolicyKind
}

// loudnessCases returns reliability, durability, deadline, liveliness, lease.
func loudnessCases(base QoSProfile) []loudnessCase {
	withReliabilityOffer := base
	withReliabilityOffer.Reliability = ReliabilityBestEffort
	withReliabilityRequest := base
	withReliabilityRequest.Reliability = ReliabilityReliable
	withDurabilityOffer := base
	withDurabilityOffer.Durability = DurabilityVolatile
	withDurabilityRequest := base
	withDurabilityRequest.Durability = DurabilityTransientLocal
	withDeadlineRequest := base
	withDeadlineRequest.Deadline = NewDuration(0, 20000000)
	withLivelinessOffer := base
	withLivelinessOffer.Liveliness = LivelinessAutomatic
	withLivelinessRequest := base
	withLivelinessRequest.Liveliness = LivelinessManualByTopic
	withLeaseRequest := base
	withLeaseRequest.LeaseDuration = NewDuration(4, 0)
	return []loudnessCase{
		{"reliability", withReliabilityOffer, withReliabilityRequest, PolicyReliability},
		{"durability", withDurabilityOffer, withDurabilityRequest, PolicyDurability},
		{"deadline", base, withDeadlineRequest, PolicyDeadline},
		{"liveliness", withLivelinessOffer, withLivelinessRequest, PolicyLiveliness},
		{"lease", base, withLeaseRequest, PolicyLeaseDuration},
	}
}

// assertLoudnessErrors pins each case yields one error event.
func assertLoudnessErrors(t *testing.T, cases []loudnessCase, counter *MismatchCounter) {
	t.Helper()
	for _, tc := range cases {
		events := CompatibilityEvents("/scan", "talker", "listener", tc.offered, tc.requested)
		if len(events) != 1 {
			t.Fatalf("%s must yield one event, got %+v", tc.name, events)
		}
		assertLoudnessEvent(t, tc, events[0])
		counter.Record(events[0])
	}
}

// assertLoudnessEvent pins one event carries topic, policy, severity.
func assertLoudnessEvent(t *testing.T, tc loudnessCase, e MismatchEvent) {
	t.Helper()
	if e.Topic != "/scan" || e.Publisher != "talker" || e.Subscriber != "listener" {
		t.Fatalf("%s must carry topic plus pair, got %+v", tc.name, e)
	}
	if e.Policy != tc.policy {
		t.Fatalf("%s policy must be %s, got %s", tc.name, tc.policy, e.Policy)
	}
	if e.Severity != SeverityError {
		t.Fatalf("%s severity must be error, got %s", tc.name, e.Severity)
	}
	assertLoudnessShapes(t, tc.name, e)
}

// assertLoudnessShapes pins string and log line carry policy and names.
func assertLoudnessShapes(t *testing.T, name string, e MismatchEvent) {
	t.Helper()
	if !strings.Contains(e.String(), string(e.Policy)) || !strings.Contains(e.LogLine(), string(e.Policy)) {
		t.Fatalf("%s shapes must carry policy, got %+v", name, e)
	}
	if !strings.Contains(e.String(), "/scan") || !strings.Contains(e.LogLine(), "talker") {
		t.Fatalf("%s shapes must carry names, got %+v", name, e)
	}
}

// assertLoudnessCounts pins each policy counted once and total five.
func assertLoudnessCounts(t *testing.T, cases []loudnessCase, counter *MismatchCounter) {
	t.Helper()
	for _, tc := range cases {
		if got := counter.Count(tc.policy); got != 1 {
			t.Fatalf("%s counter must be 1, got %d", tc.name, got)
		}
	}
	if got := counter.Total(); got != 5 {
		t.Fatalf("total must be 5, got %d", got)
	}
}

// assertLoudnessWarnings pins unknown markers warn and advance counter.
func assertLoudnessWarnings(t *testing.T, base QoSProfile, counter *MismatchCounter) {
	t.Helper()
	warns := CompatibilityEvents("/scan", "talker", "listener", UnknownQoS(), base)
	if len(warns) == 0 {
		t.Fatal("unresolved markers must warn")
	}
	for _, e := range warns {
		assertLoudnessWarning(t, e)
		counter.Record(e)
	}
	if got := counter.Total(); got <= 5 {
		t.Fatalf("warnings must advance the counter past 5, got %d", got)
	}
}

// assertLoudnessWarning pins one warning carries topic, pair, policy.
func assertLoudnessWarning(t *testing.T, e MismatchEvent) {
	t.Helper()
	if e.Severity != SeverityWarning {
		t.Fatalf("marker severity must be warning, got %+v", e)
	}
	if e.Topic != "/scan" || e.Publisher != "talker" || e.Subscriber != "listener" {
		t.Fatalf("warning must carry topic plus pair, got %+v", e)
	}
	if !strings.Contains(e.String(), string(e.Policy)) || !strings.Contains(e.LogLine(), string(e.Policy)) {
		t.Fatalf("warning shapes must carry policy, got %+v", e)
	}
}

// assertSentinelDeadlineWarns pins sentinel deadline warns.
func assertSentinelDeadlineWarns(t *testing.T, base QoSProfile) {
	t.Helper()
	sentinelWarns := CompatibilityEvents("/scan", "talker", "listener", BestAvailableQoS(), base)
	if len(sentinelWarns) == 0 {
		t.Fatal("sentinel durations must warn")
	}
	if !hasDeadlineWarning(sentinelWarns) {
		t.Fatalf("sentinel deadline must warn, got %+v", sentinelWarns)
	}
}

// hasDeadlineWarning reports whether any event warns on deadline.
func hasDeadlineWarning(events []MismatchEvent) bool {
	for _, e := range events {
		if e.Policy == PolicyDeadline && e.Severity == SeverityWarning {
			return true
		}
	}
	return false
}

// checkNilRecord pins nil counter Record stays quiet without panic.
func checkNilRecord(t *testing.T) {
	t.Helper()
	var nilCounter *MismatchCounter
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("nil Record must not panic, got %v", r)
			}
		}()
		nilCounter.Record(MismatchEvent{Topic: "/t", Policy: PolicyReliability, Severity: SeverityError})
	}()
	if got := nilCounter.Count(PolicyReliability); got != 0 {
		t.Fatalf("nil count must be 0, got %d", got)
	}
	if got := nilCounter.Total(); got != 0 {
		t.Fatalf("nil total must be 0, got %d", got)
	}
}

// checkPerDropLoud pins every overflow increments Lost by one on the loud channel.
func checkPerDropLoud(t *testing.T) {
	t.Helper()
	cache := NewHistoryCache[int](3)
	for _, v := range []int{1, 2, 3} {
		if evicted := cache.Push(v); evicted {
			t.Fatalf("push %d must not evict", v)
		}
	}
	for i, v := range []int{4, 5, 6, 7} {
		if evicted := cache.Push(v); !evicted {
			t.Fatalf("push %d must evict", v)
		}
		if got, want := cache.Lost(), uint64(i+1); got != want {
			t.Fatalf("lost after push %d must be %d, got %d", v, want, got)
		}
	}
	if got := cache.Lost(); got != 4 {
		t.Fatalf("four overflows must count four, got %d", got)
	}
	if got := cache.Len(); got != 3 {
		t.Fatalf("length must stay at capacity, got %d", got)
	}
}

// checkRetentionRoundTrip pins ActualQoS feeds RetentionCapacity for
// default, keep all, and best available endpoints.
func checkRetentionRoundTrip(t *testing.T) {
	t.Helper()
	assertDefaultRoundTrip(t)
	assertKeepAllRoundTrip(t)
	assertBestAvailableRoundTrip(t)
}

// assertDefaultRoundTrip pins default actual feeds capacity and fills.
func assertDefaultRoundTrip(t *testing.T) {
	t.Helper()
	defEndpoint, err := CreateEndpoint("/t", "sub", EndpointSubscription, DefaultQoS(), nil, false)
	if err != nil {
		t.Fatalf("create default endpoint: %v", err)
	}
	capacity, err := RetentionCapacity(defEndpoint.ActualQoS())
	if err != nil {
		t.Fatalf("retention capacity from actual: %v", err)
	}
	if capacity != DefaultHistoryDepth {
		t.Fatalf("default round trip must be depth, got %d", capacity)
	}
	fillCacheNoEvict(t, capacity)
}

// fillCacheNoEvict pins filling capacity samples does not evict.
func fillCacheNoEvict(t *testing.T, capacity uint64) {
	t.Helper()
	cache := NewHistoryCache[int](capacity)
	count := 0
	for range capacity {
		if evicted := cache.Push(count); evicted {
			t.Fatalf("push %d must not evict", count)
		}
		count++
	}
}

// assertKeepAllRoundTrip pins keep all actual uses bound.
func assertKeepAllRoundTrip(t *testing.T) {
	t.Helper()
	keepAllProfile := QoSProfile{
		History:     HistoryKeepAll,
		Reliability: ReliabilityReliable,
		Durability:  DurabilityVolatile,
		Liveliness:  LivelinessAutomatic,
	}
	keepEndpoint, err := CreateEndpoint("/t", "sub", EndpointSubscription, keepAllProfile, nil, false)
	if err != nil {
		t.Fatalf("create keep all endpoint: %v", err)
	}
	keepCap, err := RetentionCapacity(keepEndpoint.ActualQoS())
	if err != nil {
		t.Fatalf("keep all capacity: %v", err)
	}
	if keepCap != KeepAllRetentionBound {
		t.Fatalf("keep all round trip must be bound, got %d", keepCap)
	}
}

// assertBestAvailableRoundTrip pins best available actual yields capacity.
func assertBestAvailableRoundTrip(t *testing.T) {
	t.Helper()
	peers := []Peer{{Name: "pub", Profile: FacadeAdvertisedQoS()}}
	baEndpoint, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), peers, false)
	if err != nil {
		t.Fatalf("create best available endpoint: %v", err)
	}
	baCap, err := RetentionCapacity(baEndpoint.ActualQoS())
	if err != nil {
		t.Fatalf("best available capacity: %v", err)
	}
	if baCap == 0 {
		t.Fatal("best available round trip must yield non zero capacity")
	}
	fillBestAvailableNoEvict(t, baCap)
}

// fillBestAvailableNoEvict pins best available cache fills then overflows.
func fillBestAvailableNoEvict(t *testing.T, baCap uint64) {
	t.Helper()
	baCache := NewHistoryCache[int](baCap)
	count := 0
	for range baCap {
		if evicted := baCache.Push(count); evicted {
			t.Fatalf("best available push %d must not evict", count)
		}
		count++
	}
	if evicted := baCache.Push(999); !evicted {
		t.Fatal("best available overflow must evict")
	}
}

// checkSentinelSurvival pins no best available sentinel reaches ActualQoS.
func checkSentinelSurvival(t *testing.T) {
	t.Helper()
	peers := []Peer{{Name: "peer", Profile: FacadeAdvertisedQoS()}}
	sub, err := CreateEndpoint("/t", "sub", EndpointSubscription, BestAvailableQoS(), peers, false)
	if err != nil {
		t.Fatalf("create sub: %v", err)
	}
	for _, d := range []Duration{sub.ActualQoS().Deadline, sub.ActualQoS().Lifespan, sub.ActualQoS().LeaseDuration} {
		if IsBestAvailableDuration(d) {
			t.Fatalf("subscriber sentinel must not survive, got %s", d)
		}
	}
	pub, err := CreateEndpoint("/t", "pub", EndpointPublisher, BestAvailableQoS(), peers, false)
	if err != nil {
		t.Fatalf("create pub: %v", err)
	}
	for _, d := range []Duration{pub.ActualQoS().Deadline, pub.ActualQoS().Lifespan, pub.ActualQoS().LeaseDuration} {
		if IsBestAvailableDuration(d) {
			t.Fatalf("publisher sentinel must not survive, got %s", d)
		}
	}
	svc, err := CreateEndpoint("/svc", "server", EndpointSubscription, BestAvailableQoS(), peers, true)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	for _, d := range []Duration{svc.ActualQoS().Deadline, svc.ActualQoS().Lifespan, svc.ActualQoS().LeaseDuration} {
		if IsBestAvailableDuration(d) {
			t.Fatalf("service sentinel must not survive, got %s", d)
		}
	}
	err = Validate(sub.ActualQoS())
	if err != nil {
		t.Fatalf("resolved actual must validate: %v", err)
	}
}

// checkAvoidBestAvailableService pins the avoid flag passes through best
// available resolution and service substitution without affecting matching.
func checkAvoidBestAvailableService(t *testing.T) {
	t.Helper()
	avoided := BestAvailableQoS()
	avoided.AvoidROSNamespaceConventions = true
	peers := []Peer{{Name: "pub", Profile: FacadeAdvertisedQoS()}}
	sub, err := CreateEndpoint("/t", "sub", EndpointSubscription, avoided, peers, false)
	if err != nil {
		t.Fatalf("create avoided best available: %v", err)
	}
	if !sub.ActualQoS().AvoidROSNamespaceConventions {
		t.Fatal("avoid flag must survive best available resolution")
	}
	if !sub.RequestedQoS().AvoidROSNamespaceConventions {
		t.Fatal("requested getter must preserve avoid flag")
	}
	svc, err := CreateEndpoint("/svc", "server", EndpointSubscription, avoided, peers, true)
	if err != nil {
		t.Fatalf("create avoided service: %v", err)
	}
	if !svc.ActualQoS().AvoidROSNamespaceConventions {
		t.Fatal("avoid flag must survive service substitution")
	}
	wantService := ResolveSystemDefaults(ServicesDefaultQoS())
	wantService.AvoidROSNamespaceConventions = true
	if svc.ActualQoS() != wantService {
		t.Fatalf("avoided service must equal services default plus avoid, got %+v want %+v", svc.ActualQoS(), wantService)
	}
	plain := ResolveSystemDefaults(DefaultQoS())
	report := CheckCompatibility(sub.ActualQoS(), plain)
	if !report.Compatible {
		t.Fatalf("avoid flag must never affect matching: %q", report.Reason())
	}
}
