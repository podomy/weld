// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package graph

import (
	"strings"
	"testing"

	"github.com/podomy/weld/internal/core"
)

// testGID returns a deterministic GID with the last byte set to seed.
func testGID(seed byte) GID {
	var out GID
	out[0] = 0xAA
	out[15] = seed
	return out
}

// mustUpsertParticipant inserts a participant or fails the test.
func mustUpsertParticipant(t *testing.T, idx *Index, info ParticipantInfo) {
	t.Helper()
	err := idx.UpsertParticipant(info)
	if err != nil {
		t.Fatalf("upsert participant: %v", err)
	}
}

// mustUpsertWriter inserts a writer or fails the test.
func mustUpsertWriter(t *testing.T, idx *Index, info EntityInfo) {
	t.Helper()
	err := idx.UpsertWriter(info)
	if err != nil {
		t.Fatalf("upsert writer: %v", err)
	}
}

// mustUpsertReader inserts a reader or fails the test.
func mustUpsertReader(t *testing.T, idx *Index, info EntityInfo) {
	t.Helper()
	err := idx.UpsertReader(info)
	if err != nil {
		t.Fatalf("upsert reader: %v", err)
	}
}

// mustUpsertService inserts a service or fails the test.
func mustUpsertService(t *testing.T, idx *Index, info ServiceInfo) {
	t.Helper()
	err := idx.UpsertService(info)
	if err != nil {
		t.Fatalf("upsert service: %v", err)
	}
}

// seedIndex builds an index with two nodes plus one topic pair.
func seedIndex(t *testing.T) *Index {
	t.Helper()
	idx := NewIndex()
	partA := testGID(0x01)
	partB := testGID(0x02)
	mustUpsertParticipant(t, idx, ParticipantInfo{
		GID:     partA,
		Enclave: "/",
		Nodes:   []NodeRef{{Name: "talker", Namespace: "/", Enclave: "/"}},
	})
	mustUpsertParticipant(t, idx, ParticipantInfo{
		GID:     partB,
		Enclave: "/",
		Nodes:   []NodeRef{{Name: "listener", Namespace: "/ns", Enclave: "/"}},
	})
	mustUpsertWriter(t, idx, EntityInfo{
		GID:           testGID(0x10),
		TopicName:     "/chatter",
		TopicType:     "std_msgs/msg/String",
		TopicTypeHash: "RIHS01_abc",
		Participant:   partA,
		QoS:           DefaultQoS(),
	})
	mustUpsertReader(t, idx, EntityInfo{
		GID:           testGID(0x11),
		TopicName:     "/chatter",
		TopicType:     "std_msgs/msg/String",
		TopicTypeHash: "RIHS01_abc",
		Participant:   partB,
		QoS:           DefaultQoS(),
	})
	return idx
}

// addForeignEndpoints adds one unknown plus one bare DDS endpoint.
func addForeignEndpoints(t *testing.T, idx *Index) {
	t.Helper()
	mustUpsertWriter(t, idx, EntityInfo{
		GID:         testGID(0x20),
		TopicName:   "/chatter",
		TopicType:   "std_msgs/msg/String",
		Participant: testGID(0x7F),
		QoS:         DefaultQoS(),
	})
	barePart := testGID(0x03)
	mustUpsertParticipant(t, idx, ParticipantInfo{GID: barePart, Enclave: "/", Nodes: []NodeRef{}})
	mustUpsertReader(t, idx, EntityInfo{
		GID:         testGID(0x21),
		TopicName:   "/chatter",
		TopicType:   "std_msgs/msg/String",
		Participant: barePart,
		QoS:         DefaultQoS(),
	})
}

// checkPublisherSentinels verifies talker plus unknown entries.
func checkPublisherSentinels(t *testing.T, pubs []TopicEndpointInfo) {
	t.Helper()
	if len(pubs) != 2 {
		t.Fatalf("publishers info len = %d, want 2", len(pubs))
	}
	seen := map[string]bool{}
	for _, info := range pubs {
		seen[info.NodeName] = true
		if info.TopicType != "std_msgs/msg/String" {
			t.Fatalf("topic type = %q, want std_msgs/msg/String", info.TopicType)
		}
		if info.GID.IsZero() {
			t.Fatalf("endpoint GID is zero")
		}
	}
	if !seen["talker"] {
		t.Fatalf("missing talker in %v", seen)
	}
	if !seen[UnknownNodeName] {
		t.Fatalf("missing unknown sentinel in %v", seen)
	}
}

// checkBareReader verifies the bare DDS sentinel appears in subscriptions.
func checkBareReader(t *testing.T, subs []TopicEndpointInfo) {
	t.Helper()
	if len(subs) != 2 {
		t.Fatalf("subscriptions info len = %d, want 2", len(subs))
	}
	for _, info := range subs {
		if info.NodeName == BareDDSAppName && info.NodeNamespace == BareDDSAppNamespace {
			return
		}
	}
	t.Fatalf("missing bare DDS sentinel in %+v", subs)
}

// seedServiceIndex builds an index with one known plus one foreign service.
func seedServiceIndex(t *testing.T) *Index {
	t.Helper()
	idx := NewIndex()
	partA := testGID(0x01)
	mustUpsertParticipant(t, idx, ParticipantInfo{
		GID:     partA,
		Enclave: "/",
		Nodes:   []NodeRef{{Name: "server", Namespace: "/", Enclave: "/"}},
	})
	mustUpsertService(t, idx, ServiceInfo{
		GID:             testGID(0x30),
		ServiceName:     "/add_two_ints",
		ServiceType:     "example_interfaces/srv/AddTwoInts",
		ServiceTypeHash: "RIHS01_svc",
		Participant:     partA,
		QoS:             ServicesDefaultQoS(),
	})
	mustUpsertService(t, idx, ServiceInfo{
		GID:             testGID(0x31),
		ServiceName:     "/add_two_ints",
		ServiceType:     "example_interfaces/srv/AddTwoInts",
		ServiceTypeHash: "RIHS01_svc",
		Participant:     testGID(0x7E),
		QoS:             ServicesDefaultQoS(),
	})
	return idx
}

// checkServiceInfos verifies node plus type fields and the unknown sentinel.
func checkServiceInfos(t *testing.T, infos []ServiceEndpointInfo) {
	t.Helper()
	if len(infos) != 2 {
		t.Fatalf("service infos len = %d, want 2", len(infos))
	}
	if infos[0].NodeNamespace != "/" || infos[0].NodeName != "server" {
		t.Fatalf("first service node = %s/%s, want //server", infos[0].NodeNamespace, infos[0].NodeName)
	}
	if infos[0].ServiceType != "example_interfaces/srv/AddTwoInts" {
		t.Fatalf("service type = %q", infos[0].ServiceType)
	}
	if infos[0].ServiceTypeHash != "RIHS01_svc" {
		t.Fatalf("service hash = %q", infos[0].ServiceTypeHash)
	}
	for _, info := range infos {
		if info.NodeName == UnknownNodeName {
			return
		}
	}
	t.Fatalf("missing unknown sentinel in %+v", infos)
}

// seedHiddenIndex builds an index with hidden plus visible names.
func seedHiddenIndex(t *testing.T) *Index {
	t.Helper()
	idx := NewIndex()
	partA := testGID(0x01)
	mustUpsertWriter(t, idx, EntityInfo{
		GID:         testGID(0x10),
		TopicName:   "/_hidden",
		TopicType:   "std_msgs/msg/String",
		Participant: partA,
		QoS:         DefaultQoS(),
	})
	mustUpsertWriter(t, idx, EntityInfo{
		GID:         testGID(0x11),
		TopicName:   "/visible",
		TopicType:   "std_msgs/msg/String",
		Participant: partA,
		QoS:         DefaultQoS(),
	})
	mustUpsertService(t, idx, ServiceInfo{
		GID:         testGID(0x30),
		ServiceName: "/_hidden_svc",
		ServiceType: "example_interfaces/srv/AddTwoInts",
		Participant: partA,
		QoS:         ServicesDefaultQoS(),
	})
	return idx
}

// checkHiddenTopics verifies topic hidden filtering.
func checkHiddenTopics(t *testing.T, snap Snapshot) {
	t.Helper()
	plain := snap.TopicNamesAndTypes(false)
	if len(plain) != 1 || plain[0].Name != "/visible" {
		t.Fatalf("filtered topics = %+v, want only /visible", plain)
	}
	withHidden := snap.TopicNamesAndTypes(true)
	if len(withHidden) != 2 {
		t.Fatalf("unfiltered topics len = %d, want 2", len(withHidden))
	}
}

// checkHiddenServices verifies service hidden filtering.
func checkHiddenServices(t *testing.T, snap Snapshot) {
	t.Helper()
	svcPlain := snap.ServiceNamesAndTypes(false)
	if len(svcPlain) != 0 {
		t.Fatalf("filtered services = %+v, want empty", svcPlain)
	}
	svcHidden := snap.ServiceNamesAndTypes(true)
	if len(svcHidden) != 1 {
		t.Fatalf("unfiltered services len = %d, want 1", len(svcHidden))
	}
}

// checkHiddenPredicates verifies the hidden predicate helpers.
func checkHiddenPredicates(t *testing.T) {
	t.Helper()
	if !IsHiddenTopic("/foo/_bar") {
		t.Fatalf("expected /foo/_bar hidden")
	}
	if IsHiddenTopic("/chatter") {
		t.Fatalf("expected /chatter visible")
	}
	if !IsHiddenService("/_svc") {
		t.Fatalf("expected /_svc hidden")
	}
	if !IsHiddenNode("_hidden") {
		t.Fatalf("expected _hidden node hidden")
	}
	if IsHiddenNode("talker") {
		t.Fatalf("expected talker visible")
	}
}

// TestCountsByTopic checks writer plus reader counts by topic.
func TestCountsByTopic(t *testing.T) {
	idx := seedIndex(t)
	snap := idx.Snapshot()
	if got := snap.PublisherCount("/chatter"); got != 1 {
		t.Fatalf("publisher count = %d, want 1", got)
	}
	if got := snap.SubscriptionCount("/chatter"); got != 1 {
		t.Fatalf("subscription count = %d, want 1", got)
	}
	if got := snap.PublisherCount("/missing"); got != 0 {
		t.Fatalf("missing publisher count = %d, want 0", got)
	}
	writers := snap.PublisherCounts()
	if writers["/chatter"] != 1 {
		t.Fatalf("writer map = %v, want /chatter 1", writers)
	}
	readers := snap.SubscriptionCounts()
	if readers["/chatter"] != 1 {
		t.Fatalf("reader map = %v, want /chatter 1", readers)
	}
}

// TestEntityInfoByTopic checks node lookup plus sentinels for foreign endpoints.
func TestEntityInfoByTopic(t *testing.T) {
	idx := seedIndex(t)
	addForeignEndpoints(t, idx)
	snap := idx.Snapshot()
	checkPublisherSentinels(t, snap.PublishersInfoByTopic("/chatter"))
	checkBareReader(t, snap.SubscriptionsInfoByTopic("/chatter"))
	if got := snap.PublishersInfoByTopic("/missing"); len(got) != 0 {
		t.Fatalf("missing topic info len = %d, want 0", len(got))
	}
}

// TestServiceEntityInfo checks service info with node plus type matching.
func TestServiceEntityInfo(t *testing.T) {
	idx := seedServiceIndex(t)
	snap := idx.Snapshot()
	checkServiceInfos(t, snap.ServiceInfosByService("/add_two_ints"))
	if got := snap.ServiceInfosByService("/missing"); len(got) != 0 {
		t.Fatalf("missing service len = %d, want 0", len(got))
	}
}

// TestNamesTypesMerged checks names plus types merged across readers plus writers.
func TestNamesTypesMerged(t *testing.T) {
	idx := NewIndex()
	partA := testGID(0x01)
	mustUpsertWriter(t, idx, EntityInfo{
		GID:         testGID(0x10),
		TopicName:   "/chatter",
		TopicType:   "std_msgs/msg/String",
		Participant: partA,
		QoS:         DefaultQoS(),
	})
	mustUpsertReader(t, idx, EntityInfo{
		GID:         testGID(0x11),
		TopicName:   "/chatter",
		TopicType:   "std_msgs/msg/Other",
		Participant: partA,
		QoS:         DefaultQoS(),
	})
	mustUpsertService(t, idx, ServiceInfo{
		GID:         testGID(0x30),
		ServiceName: "/add_two_ints",
		ServiceType: "example_interfaces/srv/AddTwoInts",
		Participant: partA,
		QoS:         ServicesDefaultQoS(),
	})
	snap := idx.Snapshot()
	topics := snap.TopicNamesAndTypes(false)
	if len(topics) != 1 {
		t.Fatalf("topics len = %d, want 1", len(topics))
	}
	if topics[0].Name != "/chatter" {
		t.Fatalf("topic name = %q, want /chatter", topics[0].Name)
	}
	if len(topics[0].Types) != 2 || topics[0].Types[0] != "std_msgs/msg/Other" || topics[0].Types[1] != "std_msgs/msg/String" {
		t.Fatalf("merged types = %v", topics[0].Types)
	}
	services := snap.ServiceNamesAndTypes(false)
	if len(services) != 1 || services[0].Name != "/add_two_ints" {
		t.Fatalf("services = %+v", services)
	}
}

// TestByNodeLists checks by-node lists through node lookup.
func TestByNodeLists(t *testing.T) {
	idx := seedIndex(t)
	snap := idx.Snapshot()
	pubs := snap.PublishersByNode("talker", "/", false)
	if len(pubs) != 1 || pubs[0].Name != "/chatter" {
		t.Fatalf("talker pubs = %+v", pubs)
	}
	subs := snap.SubscriptionsByNode("listener", "/ns", false)
	if len(subs) != 1 || subs[0].Name != "/chatter" {
		t.Fatalf("listener subs = %+v", subs)
	}
	if got := snap.PublishersByNode("missing", "/", false); len(got) != 0 {
		t.Fatalf("missing node pubs len = %d, want 0", len(got))
	}
	mustUpsertService(t, idx, ServiceInfo{
		GID:         testGID(0x30),
		ServiceName: "/add_two_ints",
		ServiceType: "example_interfaces/srv/AddTwoInts",
		Participant: testGID(0x01),
		QoS:         ServicesDefaultQoS(),
	})
	snap = idx.Snapshot()
	svcs := snap.ServicesByNode("talker", "/", false)
	if len(svcs) != 1 || svcs[0].Name != "/add_two_ints" {
		t.Fatalf("talker services = %+v", svcs)
	}
}

// TestHiddenFiltering checks hidden filtering for topics, services, and nodes.
func TestHiddenFiltering(t *testing.T) {
	idx := seedHiddenIndex(t)
	snap := idx.Snapshot()
	checkHiddenTopics(t, snap)
	checkHiddenServices(t, snap)
	checkHiddenPredicates(t)
}

// TestSameNameDisambiguation checks duplicate node names gain suffixes.
func TestSameNameDisambiguation(t *testing.T) {
	idx := NewIndex()
	for _, seed := range []byte{0x01, 0x02} {
		part := testGID(seed)
		mustUpsertParticipant(t, idx, ParticipantInfo{
			GID:     part,
			Enclave: "/",
			Nodes:   []NodeRef{{Name: "talker", Namespace: "/", Enclave: "/"}},
		})
		mustUpsertWriter(t, idx, EntityInfo{
			GID:         testGID(0x10 + seed),
			TopicName:   "/chatter",
			TopicType:   "std_msgs/msg/String",
			Participant: part,
			QoS:         DefaultQoS(),
		})
	}
	snap := idx.Snapshot()
	names := snap.DisambiguatedNodeNames(false)
	if len(names) != 1 {
		t.Fatalf("disambiguated len = %d, want 1 unique node key, got %v", len(names), names)
	}
	partC := testGID(0x03)
	mustUpsertParticipant(t, idx, ParticipantInfo{
		GID:     partC,
		Enclave: "/other",
		Nodes:   []NodeRef{{Name: "talker", Namespace: "/", Enclave: "/other"}},
	})
	snap = idx.Snapshot()
	names = snap.DisambiguatedNodeNames(false)
	if len(names) != 2 {
		t.Fatalf("disambiguated len = %d, want 2, got %v", len(names), names)
	}
	if names[0] != "/talker" {
		t.Fatalf("first name = %q, want /talker", names[0])
	}
	if names[1] != "/talker (2)" {
		t.Fatalf("second name = %q, want /talker (2)", names[1])
	}
	refs := snap.NodeNames(false)
	if len(refs) != 2 {
		t.Fatalf("node names len = %d, want 2, got %+v", len(refs), refs)
	}
}

// TestGenerationOnChange checks generation plus hook on every mutation.
func TestGenerationOnChange(t *testing.T) {
	idx := NewIndex()
	var gens []uint64
	idx.SetOnChange(func(gen uint64) {
		gens = append(gens, gen)
	})
	if got := idx.Generation(); got != 0 {
		t.Fatalf("initial generation = %d, want 0", got)
	}
	partA := testGID(0x01)
	mustUpsertParticipant(t, idx, ParticipantInfo{GID: partA, Enclave: "/", Nodes: []NodeRef{}})
	if got := idx.Generation(); got != 1 {
		t.Fatalf("generation = %d, want 1", got)
	}
	info := EntityInfo{GID: testGID(0x10), TopicName: "/a", TopicType: "t", Participant: partA, QoS: DefaultQoS()}
	mustUpsertWriter(t, idx, info)
	mustUpsertWriter(t, idx, info)
	if got := idx.Generation(); got != 2 {
		t.Fatalf("generation after dup = %d, want 2", got)
	}
	if !idx.RemoveWriter(info.GID) {
		t.Fatalf("remove writer reported false")
	}
	if idx.RemoveWriter(info.GID) {
		t.Fatalf("second remove reported true")
	}
	if len(gens) != 3 || gens[0] != 1 || gens[1] != 2 || gens[2] != 3 {
		t.Fatalf("hook gens = %v, want [1 2 3]", gens)
	}
}

// TestSnapshotIsolation checks snapshots do not observe later mutations.
func TestSnapshotIsolation(t *testing.T) {
	idx := seedIndex(t)
	before := idx.Snapshot()
	mustUpsertWriter(t, idx, EntityInfo{
		GID:         testGID(0x40),
		TopicName:   "/late",
		TopicType:   "std_msgs/msg/String",
		Participant: testGID(0x01),
		QoS:         DefaultQoS(),
	})
	if got := before.PublisherCount("/late"); got != 0 {
		t.Fatalf("old snapshot sees /late count %d, want 0", got)
	}
	after := idx.Snapshot()
	if got := after.PublisherCount("/late"); got != 1 {
		t.Fatalf("new snapshot /late count %d, want 1", got)
	}
	if before.Generation == after.Generation {
		t.Fatalf("generations equal %d", before.Generation)
	}
}

// TestPartitionLocalDescribe checks the partition-local description.
func TestPartitionLocalDescribe(t *testing.T) {
	idx := seedIndex(t)
	snap := idx.Snapshot()
	desc := snap.Describe()
	if !strings.Contains(desc, "partition local") {
		t.Fatalf("describe misses partition local: %q", desc)
	}
	if !strings.Contains(desc, "never a global truth") && !strings.Contains(desc, "never global truth") {
		t.Fatalf("describe misses global truth note: %q", desc)
	}
}

// TestNodeCounts checks node names with counts.
func TestNodeCounts(t *testing.T) {
	idx := seedIndex(t)
	snap := idx.Snapshot()
	counts := snap.NodeCounts(false)
	total := 0
	for _, c := range counts {
		total += c.Count
	}
	if total != 2 {
		t.Fatalf("total endpoints = %d, want 2, got %+v", total, counts)
	}
	for _, c := range counts {
		if c.Name == "talker" && c.Namespace == "/" && c.Count == 1 {
			return
		}
	}
	t.Fatalf("missing talker count in %+v", counts)
}

// TestStaticTopology checks presized construction with no thread.
func TestStaticTopology(t *testing.T) {
	idx := NewIndexWithCapacity(4, 4, 2, 2)
	partA := testGID(0x01)
	mustUpsertParticipant(t, idx, ParticipantInfo{
		GID:     partA,
		Enclave: "/",
		Nodes:   []NodeRef{{Name: "n", Namespace: "/", Enclave: "/"}},
	})
	mustUpsertWriter(t, idx, EntityInfo{
		GID:         testGID(0x10),
		TopicName:   "/t",
		TopicType:   "x",
		Participant: partA,
		QoS:         DefaultQoS(),
	})
	snap := idx.Snapshot()
	if got := snap.PublisherCount("/t"); got != 1 {
		t.Fatalf("static count = %d, want 1", got)
	}
	empty := NewIndexWithCapacity(0, -1, 0, 0)
	if empty == nil {
		t.Fatalf("static zero constructor returned nil")
	}
}

// TestValidation checks invalid records return errors.
func TestValidation(t *testing.T) {
	idx := NewIndex()
	err := idx.UpsertWriter(EntityInfo{})
	if err == nil {
		t.Fatalf("empty writer accepted")
	}
	err = idx.UpsertWriter(EntityInfo{GID: testGID(0x10), TopicName: "no-slash", TopicType: "t"})
	if err == nil {
		t.Fatalf("relative topic accepted")
	}
	err = idx.UpsertService(ServiceInfo{})
	if err == nil {
		t.Fatalf("empty service accepted")
	}
	err = idx.UpsertParticipant(ParticipantInfo{})
	if err == nil {
		t.Fatalf("empty participant accepted")
	}
	err = idx.UpsertReader(EntityInfo{GID: testGID(0x11), TopicName: "/t", TopicType: ""})
	if err == nil {
		t.Fatalf("typeless reader accepted")
	}
}

// TestGIDRoundTrip checks GID parse plus string form.
func TestGIDRoundTrip(t *testing.T) {
	gid := testGID(0x5A)
	parsed, err := GIDFromString(gid.String())
	if err != nil {
		t.Fatalf("parse GID: %v", err)
	}
	if parsed != gid {
		t.Fatalf("round trip = %v, want %v", parsed, gid)
	}
	_, err = GIDFromString("short")
	if err == nil {
		t.Fatalf("short GID accepted")
	}
	var zero GID
	if !zero.IsZero() {
		t.Fatalf("zero GID not zero")
	}
	if gid.IsZero() {
		t.Fatalf("nonzero GID reports zero")
	}
}

// coreTestGID returns a deterministic core GID with the last byte set to seed.
func coreTestGID(seed byte) core.GID {
	var out core.GID
	out[0] = 0xAA
	out[15] = seed
	return out
}

// seedCoreGraph builds a core graph with one participant plus writer plus service.
func seedCoreGraph(t *testing.T) *core.Graph {
	t.Helper()
	g := core.NewGraph()
	part := coreTestGID(0x01)
	err := g.AddParticipant(part, core.ParticipantInfo{
		Enclave:       "/",
		NodeName:      "talker",
		NodeNamespace: "/",
		Sequence:      7,
	})
	if err != nil {
		t.Fatalf("add core participant: %v", err)
	}
	err = g.AddWriter(coreTestGID(0x10), core.EntityInfo{
		TopicName:      "/chatter",
		TopicType:      "std_msgs__msg__String",
		TypeHash:       "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18",
		ParticipantGID: part,
		QoS:            core.DefaultQoS(),
	})
	if err != nil {
		t.Fatalf("add core writer: %v", err)
	}
	err = g.AddReader(coreTestGID(0x11), core.EntityInfo{
		TopicName:      "/chatter",
		TopicType:      "std_msgs__msg__String",
		TypeHash:       "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18",
		ParticipantGID: part,
		QoS:            core.SensorDataQoS(),
	})
	if err != nil {
		t.Fatalf("add core reader: %v", err)
	}
	err = g.AddService(coreTestGID(0x30), core.ServiceInfo{
		ServiceName:     "/add_two_ints",
		ServiceType:     "example__srv__AddTwoInts",
		ServiceTypeHash: "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18",
		ParticipantGID:  part,
	})
	if err != nil {
		t.Fatalf("add core service: %v", err)
	}
	return g
}

// TestGIDFromCore checks core GID bytes survive the view conversion.
func TestGIDFromCore(t *testing.T) {
	cgid := coreTestGID(0x5A)
	converted := GIDFromCore(cgid)
	if converted.String() != cgid.String() {
		t.Fatalf("converted GID = %s, want %s", converted.String(), cgid.String())
	}
}

// TestQoSFromCore checks the core profile maps to the display mirror.
func TestQoSFromCore(t *testing.T) {
	got := QoSFromCore(core.DefaultQoS())
	if got.History != "keep_last" || got.Depth != 10 {
		t.Fatalf("history mirror = %+v", got)
	}
	if got.Reliability != "reliable" || got.Durability != "volatile" || got.Liveliness != "automatic" {
		t.Fatalf("policy mirror = %+v", got)
	}
	sensor := QoSFromCore(core.SensorDataQoS())
	if sensor.Reliability != "best_effort" || sensor.Depth != 5 {
		t.Fatalf("sensor mirror = %+v", sensor)
	}
}

// TestEntityFromCore checks topic plus type plus hash plus participant mapping.
func TestEntityFromCore(t *testing.T) {
	part := coreTestGID(0x01)
	info := EntityFromCore(coreTestGID(0x10), core.EntityInfo{
		TopicName:      "/chatter",
		TopicType:      "std_msgs__msg__String",
		TypeHash:       "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18",
		ParticipantGID: part,
		QoS:            core.DefaultQoS(),
	})
	if info.TopicName != "/chatter" || info.TopicType != "std_msgs__msg__String" {
		t.Fatalf("entity mirror = %+v", info)
	}
	if info.TopicTypeHash != "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18" {
		t.Fatalf("entity hash = %q", info.TopicTypeHash)
	}
	if info.Participant != GIDFromCore(part) {
		t.Fatalf("entity participant = %s", info.Participant.String())
	}
	if info.QoS.Reliability != "reliable" {
		t.Fatalf("entity QoS = %+v", info.QoS)
	}
}

// TestServiceFromCore checks service mapping carries the display default QoS.
func TestServiceFromCore(t *testing.T) {
	part := coreTestGID(0x01)
	info := ServiceFromCore(coreTestGID(0x30), core.ServiceInfo{
		ServiceName:     "/add_two_ints",
		ServiceType:     "example__srv__AddTwoInts",
		ServiceTypeHash: "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18",
		ParticipantGID:  part,
	})
	if info.ServiceName != "/add_two_ints" || info.ServiceType != "example__srv__AddTwoInts" {
		t.Fatalf("service mirror = %+v", info)
	}
	if info.ServiceTypeHash != "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18" {
		t.Fatalf("service hash = %q", info.ServiceTypeHash)
	}
	wantQoS := ServicesDefaultQoS()
	if info.QoS != wantQoS {
		t.Fatalf("service QoS = %+v, want %+v", info.QoS, wantQoS)
	}
}

// TestParticipantFromCore checks node identity maps to a single node ref.
func TestParticipantFromCore(t *testing.T) {
	converted := ParticipantFromCore(coreTestGID(0x01), core.ParticipantInfo{
		Enclave:       "/",
		NodeName:      "talker",
		NodeNamespace: "/",
		Sequence:      7,
	})
	if converted.Enclave != "/" {
		t.Fatalf("enclave = %q", converted.Enclave)
	}
	if len(converted.Nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(converted.Nodes))
	}
	if converted.Nodes[0].Name != "talker" || converted.Nodes[0].Namespace != "/" {
		t.Fatalf("node ref = %+v", converted.Nodes[0])
	}
}

// TestSnapshotFromCore checks the view snapshot mirrors core counts plus generation.
func TestSnapshotFromCore(t *testing.T) {
	g := seedCoreGraph(t)
	snap, err := SnapshotFromCore(g)
	if err != nil {
		t.Fatalf("SnapshotFromCore: %v", err)
	}
	if snap.Generation != g.Generation() {
		t.Fatalf("generation = %d, want %d", snap.Generation, g.Generation())
	}
	if got := snap.PublisherCount("/chatter"); got != 1 {
		t.Fatalf("publisher count = %d, want 1", got)
	}
	if got := snap.SubscriptionCount("/chatter"); got != 1 {
		t.Fatalf("subscription count = %d, want 1", got)
	}
	pubs := snap.PublishersInfoByTopic("/chatter")
	if len(pubs) != 1 || pubs[0].NodeName != "talker" {
		t.Fatalf("publishers info = %+v", pubs)
	}
	svcs := snap.ServiceInfosByService("/add_two_ints")
	if len(svcs) != 1 || svcs[0].NodeName != "talker" {
		t.Fatalf("service infos = %+v", svcs)
	}
}

// TestIndexFromCoreNil checks nil core graphs report an error.
func TestIndexFromCoreNil(t *testing.T) {
	_, err := IndexFromCore(nil)
	if err == nil {
		t.Fatalf("nil core index accepted")
	}
	_, err = SnapshotFromCore(nil)
	if err == nil {
		t.Fatalf("nil core snapshot accepted")
	}
}
