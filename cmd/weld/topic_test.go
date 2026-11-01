// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
)

// fakeTopicBackend is a Backend stub with an injectable graph snapshot.
type fakeTopicBackend struct {
	joined   bool
	closed   bool
	snapshot graphSnapshot
	hasSnap  bool
}

// PublishBytes accepts one message without sending.
func (f *fakeTopicBackend) PublishBytes(_ context.Context, _ transports.OutgoingMessage) error {
	return nil
}

// TakeBytes reports no message without blocking.
func (f *fakeTopicBackend) TakeBytes(_ context.Context) (transports.IncomingMessage, error) {
	return transports.IncomingMessage{}, context.DeadlineExceeded
}

// JoinDiscovery records a successful join without dialing.
func (f *fakeTopicBackend) JoinDiscovery(_ context.Context, _ transports.DiscoveryInfo) error {
	f.joined = true

	return nil
}

// ReportLiveliness accepts one report without tracking.
func (f *fakeTopicBackend) ReportLiveliness(_ context.Context, _ transports.LivelinessReport) error {
	return nil
}

// Close releases the stub without work.
func (f *fakeTopicBackend) Close() error {
	f.closed = true

	return nil
}

// SnapshotGraph returns the injected snapshot copy.
func (f *fakeTopicBackend) SnapshotGraph() graphSnapshot {
	if !f.hasSnap {
		return graphSnapshot{}
	}

	out := make([]topicEndpoint, len(f.snapshot.Endpoints))
	copy(out, f.snapshot.Endpoints)

	return graphSnapshot{Generation: f.snapshot.Generation, Endpoints: out}
}

// sedpOnlyBackend exposes only SEDP viewers for snapshot building.
type sedpOnlyBackend struct {
	locals  []dds.SEDPEndpoint
	remotes []dds.SEDPEndpoint
	gen     uint64
}

// PublishBytes accepts one message without sending.
func (f *sedpOnlyBackend) PublishBytes(_ context.Context, _ transports.OutgoingMessage) error {
	return nil
}

// TakeBytes reports no message without blocking.
func (f *sedpOnlyBackend) TakeBytes(_ context.Context) (transports.IncomingMessage, error) {
	return transports.IncomingMessage{}, context.DeadlineExceeded
}

// JoinDiscovery records a successful join without dialing.
func (f *sedpOnlyBackend) JoinDiscovery(_ context.Context, _ transports.DiscoveryInfo) error {
	return nil
}

// ReportLiveliness accepts one report without tracking.
func (f *sedpOnlyBackend) ReportLiveliness(_ context.Context, _ transports.LivelinessReport) error {
	return nil
}

// Close releases the stub without work.
func (f *sedpOnlyBackend) Close() error {
	return nil
}

// LocalEndpoints returns a copy of locals.
func (f *sedpOnlyBackend) LocalEndpoints() []dds.SEDPEndpoint {
	out := make([]dds.SEDPEndpoint, len(f.locals))
	copy(out, f.locals)

	return out
}

// RemoteEndpoints returns a copy of remotes.
func (f *sedpOnlyBackend) RemoteEndpoints() []dds.SEDPEndpoint {
	out := make([]dds.SEDPEndpoint, len(f.remotes))
	copy(out, f.remotes)

	return out
}

// DiscoveryGeneration returns the stub generation.
func (f *sedpOnlyBackend) DiscoveryGeneration() uint64 {
	return f.gen
}

// topicTestQoS returns a reliable volatile profile for matching tests.
func topicTestQoS() core.QoSProfile {
	return core.QoSProfile{
		History:     core.HistoryKeepLast,
		Depth:       10,
		Reliability: core.ReliabilityReliable,
		Durability:  core.DurabilityVolatile,
		Liveliness:  core.LivelinessAutomatic,
	}
}

// topicTestBestEffort returns a best effort volatile profile.
func topicTestBestEffort() core.QoSProfile {
	p := topicTestQoS()
	p.Reliability = core.ReliabilityBestEffort

	return p
}

// TestIsHiddenTopic checks underscore segments hide topics.
func TestIsHiddenTopic(t *testing.T) {
	hidden := []string{"/_hidden", "/foo/_bar", "/foo/_bar/baz", "/_/x"}
	for _, topic := range hidden {
		if !isHiddenTopic(topic) {
			t.Fatalf("hidden %q not detected", topic)
		}
	}

	visible := []string{"/chatter", "/foo/bar", "/foo_bar", "/"}
	for _, topic := range visible {
		if isHiddenTopic(topic) {
			t.Fatalf("visible %q marked hidden", topic)
		}
	}
}

// TestBuildGraphSnapshotCopies checks snapshot copies input.
func TestBuildGraphSnapshotCopies(t *testing.T) {
	eps := []topicEndpoint{
		{GID: "a", Topic: "/chatter", Type: "std_msgs__msg__String", TypeHash: "h1"},
	}

	snap := buildGraphSnapshot(eps, 7)
	if snap.Generation != 7 {
		t.Fatalf("generation = %d, want 7", snap.Generation)
	}

	if len(snap.Endpoints) != 1 {
		t.Fatalf("endpoints = %d, want 1", len(snap.Endpoints))
	}

	eps[0].GID = "mutated"
	if snap.Endpoints[0].GID != "a" {
		t.Fatalf("snapshot aliases input, got %q", snap.Endpoints[0].GID)
	}
}

// TestSummarizeTopicsCounts checks grouping plus counts plus hashes.
func TestSummarizeTopicsCounts(t *testing.T) {
	snap := buildGraphSnapshot([]topicEndpoint{
		{GID: "pub1", Topic: "/chatter", Type: "std_msgs__msg__String", TypeHash: "h1"},
		{GID: "pub2", Topic: "/chatter", Type: "std_msgs__msg__String", TypeHash: "h1"},
		{GID: "sub1", Topic: "/chatter", Type: "std_msgs__msg__String", TypeHash: "h1", IsReader: true},
		{GID: "pub3", Topic: "/other", Type: "std_msgs__msg__String", TypeHash: "h2"},
	}, 0)

	got := summarizeTopics(snap, false)
	if len(got) != 2 {
		t.Fatalf("summaries = %d, want 2", len(got))
	}

	if got[0].Name != "/chatter" {
		t.Fatalf("first = %q, want /chatter sorted", got[0].Name)
	}

	if got[0].PubCount != 2 || got[0].SubCount != 1 {
		t.Fatalf("chatter counts = %d pubs %d subs, want 2 and 1", got[0].PubCount, got[0].SubCount)
	}

	if len(got[0].Types) != 1 || got[0].Types[0] != "std_msgs__msg__String" {
		t.Fatalf("chatter types = %v, want one String", got[0].Types)
	}

	if len(got[0].TypeHashes) != 1 || got[0].TypeHashes[0] != "h1" {
		t.Fatalf("chatter hashes = %v, want [h1]", got[0].TypeHashes)
	}
}

// TestSummarizeTopicsHiddenFiltering checks hidden topics filter.
func TestSummarizeTopicsHiddenFiltering(t *testing.T) {
	snap := buildGraphSnapshot([]topicEndpoint{
		{GID: "a", Topic: "/chatter", Type: "t", TypeHash: "h"},
		{GID: "b", Topic: "/_hidden", Type: "t", TypeHash: "h"},
	}, 0)

	filtered := summarizeTopics(snap, false)
	if len(filtered) != 1 || filtered[0].Name != "/chatter" {
		t.Fatalf("filtered = %v, want only /chatter", filtered)
	}

	shown := summarizeTopics(snap, true)
	if len(shown) != 2 {
		t.Fatalf("include hidden = %d, want 2", len(shown))
	}
}

// TestSummarizeTopicsSameNameDisambiguation checks multi type topics.
func TestSummarizeTopicsSameNameDisambiguation(t *testing.T) {
	snap := buildGraphSnapshot([]topicEndpoint{
		{GID: "a", Topic: "/same", Type: "a__msg__A", TypeHash: "h1"},
		{GID: "b", Topic: "/same", Type: "b__msg__B", TypeHash: "h2", IsReader: true},
	}, 0)

	got := summarizeTopics(snap, false)
	if len(got) != 1 {
		t.Fatalf("summaries = %d, want 1", len(got))
	}

	if len(got[0].Types) != 2 {
		t.Fatalf("types = %v, want 2 for disambiguation", got[0].Types)
	}

	if len(got[0].TypeHashes) != 2 {
		t.Fatalf("hashes = %v, want 2 for disambiguation", got[0].TypeHashes)
	}
}

// TestEndpointFromSEDP checks SEDP conversion keeps identity.
func TestEndpointFromSEDP(t *testing.T) {
	var prefix dds.SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = byte(i)
	}

	idx, err := dds.NewSEDPIndex(prefix, 42)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}

	ep, err := idx.DeclareWriter("/chatter", "std_msgs__msg__String", "RIHS01_abc", nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}

	got := endpointFromSEDP(ep)
	if got.Topic != "/chatter" {
		t.Fatalf("topic = %q, want /chatter", got.Topic)
	}

	if got.Type != "std_msgs__msg__String" {
		t.Fatalf("type = %q, want String", got.Type)
	}

	if got.TypeHash != "RIHS01_abc" {
		t.Fatalf("hash = %q, want RIHS01_abc", got.TypeHash)
	}

	if got.IsReader {
		t.Fatalf("writer marked as reader")
	}

	if got.GID == "" {
		t.Fatalf("empty GID from SEDP")
	}

	if got.HasQoS {
		t.Fatalf("SEDP record must not carry QoS")
	}
}

// TestDetectTypeMismatches checks differing hashes report loudly.
func TestDetectTypeMismatches(t *testing.T) {
	eps := []topicEndpoint{
		newTopicEndpointWithQoS("pub", "/chatter", "std_msgs__msg__String", "h1", false, topicTestQoS()),
		newTopicEndpointWithQoS("sub", "/chatter", "std_msgs__msg__String", "h2", true, topicTestQoS()),
	}

	got := detectTypeMismatches("/chatter", eps)
	if len(got) != 1 {
		t.Fatalf("type mismatches = %d, want 1", len(got))
	}

	m := got[0]
	if m.Topic != "/chatter" || m.Publisher != "pub" || m.Subscriber != "sub" {
		t.Fatalf("mismatch identity = %+v, want topic plus pair", m)
	}

	if m.Policy != topicTypeHashPolicy {
		t.Fatalf("policy = %q, want %q", m.Policy, topicTypeHashPolicy)
	}

	same := []topicEndpoint{
		newTopicEndpointWithQoS("pub", "/chatter", "std_msgs__msg__String", "h1", false, topicTestQoS()),
		newTopicEndpointWithQoS("sub", "/chatter", "std_msgs__msg__String", "h1", true, topicTestQoS()),
	}

	if got := detectTypeMismatches("/chatter", same); len(got) != 0 {
		t.Fatalf("same hash mismatches = %d, want 0", len(got))
	}
}

// TestDetectQoSMismatches checks best effort versus reliable reports policy.
func TestDetectQoSMismatches(t *testing.T) {
	eps := []topicEndpoint{
		newTopicEndpointWithQoS("pub", "/chatter", "t", "h", false, topicTestBestEffort()),
		newTopicEndpointWithQoS("sub", "/chatter", "t", "h", true, topicTestQoS()),
	}

	got := detectQoSMismatches("/chatter", eps)
	if len(got) == 0 {
		t.Fatalf("qos mismatch missing for best effort versus reliable")
	}

	found := false
	for _, m := range got {
		if m.Policy == core.PolicyReliability && m.Topic == "/chatter" && m.Publisher == "pub" && m.Subscriber == "sub" {
			found = true
		}
	}

	if !found {
		t.Fatalf("reliability mismatch with topic plus pair missing: %+v", got)
	}

	compatible := []topicEndpoint{
		newTopicEndpointWithQoS("pub", "/chatter", "t", "h", false, topicTestQoS()),
		newTopicEndpointWithQoS("sub", "/chatter", "t", "h", true, topicTestQoS()),
	}

	if got := detectQoSMismatches("/chatter", compatible); len(got) != 0 {
		t.Fatalf("compatible mismatches = %d, want 0", len(got))
	}
}

// TestDetectMismatchesSorted checks deterministic order.
func TestDetectMismatchesSorted(t *testing.T) {
	eps := []topicEndpoint{
		newTopicEndpointWithQoS("pub-b", "/chatter", "t", "h1", false, topicTestBestEffort()),
		newTopicEndpointWithQoS("pub-a", "/chatter", "t", "h2", false, topicTestBestEffort()),
		newTopicEndpointWithQoS("sub", "/chatter", "t", "h3", true, topicTestQoS()),
	}

	got := detectMismatches("/chatter", eps)
	if len(got) == 0 {
		t.Fatalf("mismatches missing")
	}

	for i := 1; i < len(got); i++ {
		prev := got[i-1].Publisher + got[i-1].Subscriber + string(got[i-1].Policy)
		curr := got[i].Publisher + got[i].Subscriber + string(got[i].Policy)
		if prev > curr {
			t.Fatalf("mismatches not sorted: %q before %q", prev, curr)
		}
	}
}

// TestFormatTopicList checks counts plus types plus hashes appear.
func TestFormatTopicList(t *testing.T) {
	summaries := []topicSummary{
		{Name: "/chatter", Types: []string{"std_msgs__msg__String"}, TypeHashes: []string{"h1"}, PubCount: 2, SubCount: 1},
	}

	out := formatTopicList(summaries)
	if !strings.Contains(out, "/chatter") {
		t.Fatalf("list misses topic:\n%s", out)
	}

	if !strings.Contains(out, "2 publishers") || !strings.Contains(out, "1 subscriber") {
		t.Fatalf("list misses counts:\n%s", out)
	}

	if !strings.Contains(out, "std_msgs__msg__String") || !strings.Contains(out, "h1") {
		t.Fatalf("list misses type or hash:\n%s", out)
	}

	if out := formatTopicList(nil); out != "" {
		t.Fatalf("empty list = %q, want empty", out)
	}
}

// TestFormatTopicInfo checks endpoint info plus loud mismatches.
func TestFormatTopicInfo(t *testing.T) {
	summary := topicSummary{
		Name:       "/chatter",
		Types:      []string{"std_msgs__msg__String"},
		TypeHashes: []string{"h1"},
		PubCount:   1,
		SubCount:   1,
		Endpoints: []topicEndpoint{
			newTopicEndpointWithQoS("pub", "/chatter", "std_msgs__msg__String", "h1", false, topicTestBestEffort()),
			newTopicEndpointWithQoS("sub", "/chatter", "std_msgs__msg__String", "h1", true, topicTestQoS()),
		},
	}

	mismatches := detectMismatches(summary.Name, summary.Endpoints)
	out := formatTopicInfo(summary, mismatches)

	for _, want := range []string{"/chatter", "gid=pub", "gid=sub", "h1", "reliability", "Mismatches"} {
		if !strings.Contains(out, want) {
			t.Fatalf("info misses %q:\n%s", want, out)
		}
	}

	if !strings.Contains(out, "topic=/chatter") || !strings.Contains(out, "publisher=pub") || !strings.Contains(out, "subscriber=sub") {
		t.Fatalf("info mismatch misses topic plus pair:\n%s", out)
	}

	if !strings.Contains(out, string(core.PolicyReliability)) {
		t.Fatalf("info mismatch misses policy kind:\n%s", out)
	}

	none := formatTopicInfo(summary, nil)
	if !strings.Contains(none, "Mismatches: none") {
		t.Fatalf("no mismatch info misses none line:\n%s", none)
	}
}

// TestFormatMismatch checks loud line carries topic pair kind.
func TestFormatMismatch(t *testing.T) {
	event := core.MismatchEvent{
		Topic:      "/chatter",
		Publisher:  "pub",
		Subscriber: "sub",
		Policy:     core.PolicyReliability,
		Offered:    "best_effort",
		Requested:  "reliable",
		Severity:   core.SeverityError,
		Detail:     "offered best_effort cannot satisfy requested reliable",
	}

	got := formatMismatch(event)
	if !strings.HasPrefix(got, "weld:") {
		t.Fatalf("mismatch = %q, want weld prefix", got)
	}

	for _, want := range []string{"/chatter", "pub", "sub", string(core.PolicyReliability)} {
		if !strings.Contains(got, want) {
			t.Fatalf("mismatch misses %q: %q", want, got)
		}
	}
}

// TestResolveInfoTopic checks flag versus positional selection.
// checkInfoTopicSuccess checks one successful topic resolution.
func checkInfoTopicSuccess(t *testing.T, flagText string, explicit bool, args []string, want, label string) {
	t.Helper()

	got, err := resolveInfoTopic(flagText, explicit, args)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if got != want {
		t.Fatalf("%s = %q, want %q", label, got, want)
	}
}

// checkInfoTopicFailure checks one failing topic resolution keeps weld prefix.
func checkInfoTopicFailure(t *testing.T, flagText string, explicit bool, args []string, label string) {
	t.Helper()

	_, err := resolveInfoTopic(flagText, explicit, args)
	if err == nil {
		t.Fatalf("%s accepted", label)
	}
	if !strings.HasPrefix(err.Error(), "weld:") {
		t.Fatalf("%s error = %q, want weld prefix", label, err.Error())
	}
}

func TestResolveInfoTopic(t *testing.T) {
	checkInfoTopicSuccess(t, "/explicit", true, nil, "/explicit", "flag topic")
	checkInfoTopicSuccess(t, defaultTopic, false, []string{"/pos"}, "/pos", "positional")
	checkInfoTopicSuccess(t, defaultTopic, true, []string{"/pos"}, defaultTopic, "explicit default topic")
	checkInfoTopicSuccess(t, defaultTopic, false, nil, defaultTopic, "default topic")
	checkInfoTopicFailure(t, "", false, nil, "missing topic")
	checkInfoTopicFailure(t, "/a", true, []string{"/b", "/c"}, "two args")
}

// TestFetchTopicSnapshotEmpty checks backend without graph yields empty.
func TestFetchTopicSnapshotEmpty(t *testing.T) {
	plain := &fakeTopicBackendNoGraph{}
	snap, err := fetchTopicSnapshot(plain)
	if err != nil {
		t.Fatalf("fetch empty: %v", err)
	}

	if len(snap.Endpoints) != 0 {
		t.Fatalf("empty endpoints = %d, want 0", len(snap.Endpoints))
	}

	_, err = fetchTopicSnapshot(nil)
	if err == nil {
		t.Fatalf("nil backend accepted")
	}

	if !strings.HasPrefix(err.Error(), "weld:") {
		t.Fatalf("nil error = %q, want weld prefix", err.Error())
	}
}

// fakeTopicBackendNoGraph implements Backend without snapshot methods.
type fakeTopicBackendNoGraph struct{}

// PublishBytes accepts one message without sending.
func (f *fakeTopicBackendNoGraph) PublishBytes(_ context.Context, _ transports.OutgoingMessage) error {
	return nil
}

// TakeBytes reports no message without blocking.
func (f *fakeTopicBackendNoGraph) TakeBytes(_ context.Context) (transports.IncomingMessage, error) {
	return transports.IncomingMessage{}, context.DeadlineExceeded
}

// JoinDiscovery records a successful join without dialing.
func (f *fakeTopicBackendNoGraph) JoinDiscovery(_ context.Context, _ transports.DiscoveryInfo) error {
	return nil
}

// ReportLiveliness accepts one report without tracking.
func (f *fakeTopicBackendNoGraph) ReportLiveliness(_ context.Context, _ transports.LivelinessReport) error {
	return nil
}

// Close releases the stub without work.
func (f *fakeTopicBackendNoGraph) Close() error {
	return nil
}

// TestFetchTopicSnapshotWithSnapshotter checks SnapshotGraph path copies.
func TestFetchTopicSnapshotWithSnapshotter(t *testing.T) {
	fb := &fakeTopicBackend{
		hasSnap: true,
		snapshot: graphSnapshot{
			Generation: 9,
			Endpoints: []topicEndpoint{
				{GID: "a", Topic: "/chatter", Type: "t", TypeHash: "h"},
			},
		},
	}

	snap, err := fetchTopicSnapshot(fb)
	if err != nil {
		t.Fatalf("fetch snapshotter: %v", err)
	}

	if snap.Generation != 9 || len(snap.Endpoints) != 1 {
		t.Fatalf("snapshot = %+v, want generation 9 with 1 endpoint", snap)
	}

	fb.snapshot.Endpoints[0].GID = "mutated"
	if snap.Endpoints[0].GID != "a" {
		t.Fatalf("snapshot aliases backend storage")
	}
}

// TestFetchTopicSnapshotFromSEDP checks SEDP viewer path merges.
func TestFetchTopicSnapshotFromSEDP(t *testing.T) {
	var prefix dds.SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = byte(i)
	}

	idx, err := dds.NewSEDPIndex(prefix, 42)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}

	local, err := idx.DeclareWriter("/chatter", "std_msgs__msg__String", "h1", nil)
	if err != nil {
		t.Fatalf("declare writer: %v", err)
	}

	remote := local
	remote.IsReader = true

	fb := &sedpOnlyBackend{locals: []dds.SEDPEndpoint{local}, remotes: []dds.SEDPEndpoint{remote}, gen: 3}

	snap, err := fetchTopicSnapshot(fb)
	if err != nil {
		t.Fatalf("fetch sedp: %v", err)
	}

	if snap.Generation != 3 {
		t.Fatalf("generation = %d, want 3", snap.Generation)
	}

	if len(snap.Endpoints) != 2 {
		t.Fatalf("endpoints = %d, want 2", len(snap.Endpoints))
	}
}

// stubTopicBackend installs a fake opener and restores it after the test.
func stubTopicBackend(t *testing.T, backend transports.Backend) {
	t.Helper()

	prev := openTopicBackendFunc
	prevWait := topicDiscoveryWaitVar
	openTopicBackendFunc = func(_ context.Context, _ uint32, _ string) (transports.Backend, error) {
		return backend, nil
	}
	topicDiscoveryWaitVar = 0

	t.Cleanup(func() {
		openTopicBackendFunc = prev
		topicDiscoveryWaitVar = prevWait
	})
}

// TestRunTopicListCommand checks list prints snapshot and help paths.
func TestRunTopicListCommand(t *testing.T) {
	fb := &fakeTopicBackend{
		hasSnap: true,
		snapshot: graphSnapshot{Endpoints: []topicEndpoint{
			{GID: "pub", Topic: "/chatter", Type: "std_msgs__msg__String", TypeHash: "h1"},
			{GID: "sub", Topic: "/chatter", Type: "std_msgs__msg__String", TypeHash: "h1", IsReader: true},
		}},
	}
	stubTopicBackend(t, fb)

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := runTopicListCommand(context.Background(), []string{"--domain", "42"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("list code = %d, want 0\nstderr:\n%s", code, stderr.String())
	}

	if !strings.Contains(stdout.String(), "/chatter") || !strings.Contains(stdout.String(), "h1") {
		t.Fatalf("list stdout misses topic or hash:\n%s", stdout.String())
	}

	if !strings.Contains(stdout.String(), "1 publisher") || !strings.Contains(stdout.String(), "1 subscriber") {
		t.Fatalf("list stdout misses counts:\n%s", stdout.String())
	}

	var helpOut bytes.Buffer
	var helpErr bytes.Buffer

	code = runTopicListCommand(context.Background(), []string{"-h"}, &helpOut, &helpErr)
	if code != 0 {
		t.Fatalf("list help code = %d, want 0", code)
	}

	if !strings.Contains(helpOut.String(), "Usage:") {
		t.Fatalf("list help misses Usage")
	}

	var badOut bytes.Buffer
	var badErr bytes.Buffer

	code = runTopicListCommand(context.Background(), []string{"/extra"}, &badOut, &badErr)
	if code != 2 {
		t.Fatalf("list positional code = %d, want 2", code)
	}
}

// TestRunTopicInfoCommand checks info prints endpoints plus mismatches.
func TestRunTopicInfoCommand(t *testing.T) {
	fb := &fakeTopicBackend{
		hasSnap: true,
		snapshot: graphSnapshot{Endpoints: []topicEndpoint{
			newTopicEndpointWithQoS("pub", "/chatter", "std_msgs__msg__String", "h1", false, topicTestBestEffort()),
			newTopicEndpointWithQoS("sub", "/chatter", "std_msgs__msg__String", "h1", true, topicTestQoS()),
		}},
	}
	stubTopicBackend(t, fb)

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := runTopicInfoCommand(context.Background(), []string{"--topic", "/chatter", "--domain", "42"}, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("info code = %d, want 1 for mismatch\nstderr:\n%s", code, stderr.String())
	}

	if !strings.Contains(stdout.String(), "Topic: /chatter") {
		t.Fatalf("info misses topic header:\n%s", stdout.String())
	}

	if !strings.Contains(stdout.String(), "gid=pub") || !strings.Contains(stdout.String(), "gid=sub") {
		t.Fatalf("info misses endpoint info:\n%s", stdout.String())
	}

	if !strings.Contains(stderr.String(), "topic=/chatter") || !strings.Contains(stderr.String(), string(core.PolicyReliability)) {
		t.Fatalf("info stderr misses loud mismatch:\n%s", stderr.String())
	}

	var missingOut bytes.Buffer
	var missingErr bytes.Buffer

	code = runTopicInfoCommand(context.Background(), []string{"--topic", "/missing", "--domain", "42"}, &missingOut, &missingErr)
	if code != 1 {
		t.Fatalf("missing topic code = %d, want 1", code)
	}

	if !strings.Contains(missingErr.String(), "unknown topic") {
		t.Fatalf("missing topic misses error:\n%s", missingErr.String())
	}
}

// TestRunTopicCommandDispatch checks topic dispatch and help.
func TestRunTopicCommandDispatch(t *testing.T) {
	fb := &fakeTopicBackend{hasSnap: true}
	stubTopicBackend(t, fb)

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := runTopicCommand(context.Background(), []string{"list", "--domain", "42"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("topic list dispatch code = %d, want 0", code)
	}

	var helpOut bytes.Buffer
	var helpErr bytes.Buffer

	code = runTopicCommand(context.Background(), []string{"-h"}, &helpOut, &helpErr)
	if code != 0 || !strings.Contains(helpOut.String(), "Usage:") {
		t.Fatalf("topic help code = %d, want 0 with Usage", code)
	}

	var unknownOut bytes.Buffer
	var unknownErr bytes.Buffer

	code = runTopicCommand(context.Background(), []string{"bogus"}, &unknownOut, &unknownErr)
	if code != 2 {
		t.Fatalf("unknown topic code = %d, want 2", code)
	}

	var emptyOut bytes.Buffer
	var emptyErr bytes.Buffer

	code = runTopicCommand(context.Background(), nil, &emptyOut, &emptyErr)
	if code != 2 {
		t.Fatalf("empty topic code = %d, want 2", code)
	}
}

// TestRunDispatchesTopic checks top level run routes topic.
func TestRunDispatchesTopic(t *testing.T) {
	fb := &fakeTopicBackend{hasSnap: true}
	stubTopicBackend(t, fb)

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	code := run(context.Background(), []string{"topic", "list", "--domain", "42"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run topic code = %d, want 0\nstderr:\n%s", code, stderr.String())
	}
}
