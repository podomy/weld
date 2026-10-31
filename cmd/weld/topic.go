// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
)

// Topic discovery defaults for list and info verbs.
const (
	// topicParticipant names the topic verb side in discovery.
	topicParticipant = "weld-topic"
	// topicDiscoveryWait bounds the SPDP and SEDP settle wait before snapshot.
	topicDiscoveryWait = 500 * time.Millisecond
)

// TopicTypeHashPolicy marks type hash mismatches in loud CLI output.
//
// Core PolicyKind covers QoS only, so type mismatches use this extra kind
// to keep topic plus endpoint pair plus policy kind in one line.
const TopicTypeHashPolicy core.PolicyKind = "type_hash"

// TopicEndpoint is one endpoint in a graph snapshot for list and info.
type TopicEndpoint struct {
	// GID identifies the endpoint, from SEDP GUID string form.
	GID string
	// Topic is the ROS name such as /chatter.
	Topic string
	// Type is the IDL name such as std_msgs__msg__String.
	Type string
	// TypeHash is the RIHS01 hash, empty for pre hash peers.
	TypeHash string
	// IsReader is true for subscriptions and false for publishers.
	IsReader bool
	// QoS is the resolved core profile for matching.
	QoS core.QoSProfile
	// HasQoS is true when QoS carries a profile for matching.
	HasQoS bool
}

// GraphSnapshot is a point in time copy of discovered endpoints.
//
// It is the CLI view of the graph index: endpoints are copied so later
// discovery never mutates a printed list. Generation counts remote
// mutations when known, else zero.
type GraphSnapshot struct {
	// Generation versions the snapshot for stale detection.
	Generation uint64
	// Endpoints holds one record per discovered endpoint.
	Endpoints []TopicEndpoint
}

// TopicSummary groups one topic with counts plus types plus hashes.
type TopicSummary struct {
	// Name is the ROS topic name.
	Name string
	// Types holds distinct IDL names sorted.
	Types []string
	// TypeHashes holds distinct non empty hashes sorted.
	TypeHashes []string
	// PubCount counts publishers on the topic.
	PubCount int
	// SubCount counts subscriptions on the topic.
	SubCount int
	// Endpoints holds the topic endpoints sorted by GID.
	Endpoints []TopicEndpoint
}

// EndpointFromSEDP converts one SEDP endpoint to a snapshot record.
//
// QoS bytes stay opaque passthrough in v1, so HasQoS is false and QoS
// mismatch checks skip the record. Type mismatches still flow from Type
// and TypeHash.
func EndpointFromSEDP(ep dds.SEDPEndpoint) TopicEndpoint {
	return TopicEndpoint{
		GID:      ep.ID.String(),
		Topic:    ep.ROSTopic,
		Type:     ep.TypeIDL,
		TypeHash: ep.TypeHash,
		IsReader: ep.IsReader,
		HasQoS:   false,
	}
}

// NewTopicEndpointWithQoS returns a snapshot record with a core profile.
//
// Tests and future translators use it to attach a resolved profile so
// QoS matching flows through core.Indicate whether the record carries QoS.
func NewTopicEndpointWithQoS(gid, topic, typ, hash string, isReader bool, qos core.QoSProfile) TopicEndpoint {
	return TopicEndpoint{
		GID:      gid,
		Topic:    topic,
		Type:     typ,
		TypeHash: hash,
		IsReader: isReader,
		QoS:      qos,
		HasQoS:   true,
	}
}

// BuildGraphSnapshot copies endpoints into a snapshot with generation.
func BuildGraphSnapshot(endpoints []TopicEndpoint, generation uint64) GraphSnapshot {
	out := make([]TopicEndpoint, len(endpoints))
	copy(out, endpoints)

	return GraphSnapshot{Generation: generation, Endpoints: out}
}

// isHiddenTopic reports whether topic is hidden from default list output.
//
// A topic is hidden when any slash separated segment starts with an
// underscore, matching the ros2 hidden convention. The leading empty
// segment from the leading slash never counts.
func isHiddenTopic(topic string) bool {
	for _, part := range strings.Split(topic, "/") {
		if part == "" {
			continue
		}

		if strings.HasPrefix(part, "_") {
			return true
		}
	}

	return false
}

// distinctStrings returns sorted distinct non empty strings from in.
func distinctStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))

	for _, s := range in {
		if s == "" {
			continue
		}

		seen[s] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for s := range seen {
		out = append(out, s)
	}

	slices.Sort(out)

	return out
}

// SummarizeTopics groups a snapshot by topic with counts plus hashes.
//
// Hidden topics are skipped unless includeHidden is true. Summaries sort
// by name and endpoints sort by GID so output is deterministic. Types and
// hashes collect distinct non empty values to disambiguate same name use
// with different types.
func SummarizeTopics(snap GraphSnapshot, includeHidden bool) []TopicSummary {
	byTopic := make(map[string][]TopicEndpoint)

	for _, ep := range snap.Endpoints {
		if ep.Topic == "" {
			continue
		}

		if !includeHidden && isHiddenTopic(ep.Topic) {
			continue
		}

		byTopic[ep.Topic] = append(byTopic[ep.Topic], ep)
	}

	out := make([]TopicSummary, 0, len(byTopic))

	for name, eps := range byTopic {
		slices.SortFunc(eps, func(a, b TopicEndpoint) int {
			return strings.Compare(a.GID, b.GID)
		})

		types := make([]string, 0, len(eps))
		hashes := make([]string, 0, len(eps))
		pubs := 0
		subs := 0

		for _, ep := range eps {
			types = append(types, ep.Type)
			hashes = append(hashes, ep.TypeHash)

			if ep.IsReader {
				subs++
			} else {
				pubs++
			}
		}

		out = append(out, TopicSummary{
			Name:       name,
			Types:      distinctStrings(types),
			TypeHashes: distinctStrings(hashes),
			PubCount:   pubs,
			SubCount:   subs,
			Endpoints:  eps,
		})
	}

	slices.SortFunc(out, func(a, b TopicSummary) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out
}

// pluralize renders n plus noun with singular for one.
func pluralize(n int, singular, plural string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, singular)
	}

	return fmt.Sprintf("%d %s", n, plural)
}

// FormatTopicList renders summaries as list lines with counts plus hashes.
//
// Each line holds topic plus publisher and subscriber counts plus distinct
// types plus distinct hashes. Empty summaries render as empty string.
func FormatTopicList(summaries []TopicSummary) string {
	if len(summaries) == 0 {
		return ""
	}

	var b strings.Builder

	for _, s := range summaries {
		types := "(none)"
		if len(s.Types) != 0 {
			types = "[" + strings.Join(s.Types, ", ") + "]"
		}

		hashes := "(none)"
		if len(s.TypeHashes) != 0 {
			hashes = strings.Join(s.TypeHashes, ", ")
		}

		_, _ = fmt.Fprintf(&b, "%s (%s, %s) %s %s\n",
			s.Name,
			pluralize(s.PubCount, "publisher", "publishers"),
			pluralize(s.SubCount, "subscriber", "subscribers"),
			types,
			hashes,
		)
	}

	return b.String()
}

// FormatTopicInfo renders one topic with endpoint info plus type hashes.
//
// The report holds types, hashes, counts, per endpoint GID plus type plus
// hash plus QoS, and loud mismatch lines with topic plus endpoint pair
// plus policy kind. Empty mismatches render as an explicit none line.
func FormatTopicInfo(summary TopicSummary, mismatches []core.MismatchEvent) string {
	var b strings.Builder

	_, _ = fmt.Fprintf(&b, "Topic: %s\n", summary.Name)

	types := "(none)"
	if len(summary.Types) != 0 {
		types = strings.Join(summary.Types, ", ")
	}

	_, _ = fmt.Fprintf(&b, "Types: %s\n", types)

	hashes := "(none)"
	if len(summary.TypeHashes) != 0 {
		hashes = strings.Join(summary.TypeHashes, ", ")
	}

	_, _ = fmt.Fprintf(&b, "Type hashes: %s\n", hashes)
	_, _ = fmt.Fprintf(&b, "Publishers: %d\n", summary.PubCount)
	_, _ = fmt.Fprintf(&b, "Subscribers: %d\n", summary.SubCount)

	_, _ = fmt.Fprintln(&b, "Endpoints:")
	for _, ep := range summary.Endpoints {
		role := "publisher"
		if ep.IsReader {
			role = "subscriber"
		}

		qos := "unknown"
		if ep.HasQoS {
			qos = fmt.Sprintf("reliability=%s durability=%s liveliness=%s",
				ep.QoS.Reliability.String(), ep.QoS.Durability.String(), ep.QoS.Liveliness.String())
		}

		hash := ep.TypeHash
		if hash == "" {
			hash = "(none)"
		}

		typ := ep.Type
		if typ == "" {
			typ = "(none)"
		}

		_, _ = fmt.Fprintf(&b, "  - gid=%s role=%s type=%s hash=%s %s\n", ep.GID, role, typ, hash, qos)
	}

	if len(mismatches) == 0 {
		_, _ = fmt.Fprintln(&b, "Mismatches: none")

		return b.String()
	}

	_, _ = fmt.Fprintf(&b, "Mismatches (%d):\n", len(mismatches))

	for _, m := range mismatches {
		_, _ = fmt.Fprintf(&b, "  - %s\n", FormatMismatch(m))
	}

	return b.String()
}

// FormatMismatch renders one mismatch loudly with topic plus pair plus kind.
//
// The weld prefix keeps CLI mismatch lines greppable across logs, metrics,
// and CLI. The core LogLine already carries topic, publisher, subscriber,
// and policy kind.
func FormatMismatch(event core.MismatchEvent) string {
	return "weld: " + event.LogLine()
}

// DetectQoSMismatches checks each publisher versus subscriber pair with core.
//
// Pairs without QoS on both sides are skipped. Compatible pairs yield no
// events. Incompatible pairs yield one event per policy kind through
// core.CompatibilityEvents, each loud with topic plus endpoint pair plus
// policy kind.
func DetectQoSMismatches(topic string, endpoints []TopicEndpoint) []core.MismatchEvent {
	var pubs []TopicEndpoint
	var subs []TopicEndpoint

	for _, ep := range endpoints {
		if ep.IsReader {
			subs = append(subs, ep)
		} else {
			pubs = append(pubs, ep)
		}
	}

	slices.SortFunc(pubs, func(a, b TopicEndpoint) int {
		return strings.Compare(a.GID, b.GID)
	})
	slices.SortFunc(subs, func(a, b TopicEndpoint) int {
		return strings.Compare(a.GID, b.GID)
	})

	var out []core.MismatchEvent

	for _, pub := range pubs {
		if !pub.HasQoS {
			continue
		}

		for _, sub := range subs {
			if !sub.HasQoS {
				continue
			}

			events := core.CompatibilityEvents(topic, pub.GID, sub.GID, pub.QoS, sub.QoS)
			out = append(out, events...)
		}
	}

	return out
}

// DetectTypeMismatches reports publisher versus subscriber type differences.
//
// Each pair with differing Type or TypeHash yields one error event with
// TopicTypeHashPolicy kind so output keeps topic plus endpoint pair plus
// policy kind. Pairs with equal type and hash flow silently.
func DetectTypeMismatches(topic string, endpoints []TopicEndpoint) []core.MismatchEvent {
	var pubs []TopicEndpoint
	var subs []TopicEndpoint

	for _, ep := range endpoints {
		if ep.IsReader {
			subs = append(subs, ep)
		} else {
			pubs = append(pubs, ep)
		}
	}

	slices.SortFunc(pubs, func(a, b TopicEndpoint) int {
		return strings.Compare(a.GID, b.GID)
	})
	slices.SortFunc(subs, func(a, b TopicEndpoint) int {
		return strings.Compare(a.GID, b.GID)
	})

	var out []core.MismatchEvent

	for _, pub := range pubs {
		for _, sub := range subs {
			if pub.Type == sub.Type && pub.TypeHash == sub.TypeHash {
				continue
			}

			out = append(out, core.MismatchEvent{
				Topic:      topic,
				Publisher:  pub.GID,
				Subscriber: sub.GID,
				Policy:     TopicTypeHashPolicy,
				Offered:    pub.TypeHash,
				Requested:  sub.TypeHash,
				Severity:   core.SeverityError,
				Detail:     "type mismatch publisher type " + pub.Type + " subscriber type " + sub.Type,
			})
		}
	}

	return out
}

// DetectMismatches combines type plus QoS mismatches sorted deterministically.
func DetectMismatches(topic string, endpoints []TopicEndpoint) []core.MismatchEvent {
	out := append(DetectTypeMismatches(topic, endpoints), DetectQoSMismatches(topic, endpoints)...)

	slices.SortFunc(out, func(a, b core.MismatchEvent) int {
		if c := strings.Compare(a.Publisher, b.Publisher); c != 0 {
			return c
		}

		if c := strings.Compare(a.Subscriber, b.Subscriber); c != 0 {
			return c
		}

		return strings.Compare(string(a.Policy), string(b.Policy))
	})

	return out
}

// resolveInfoTopic picks the info topic from flag or positional arg.
//
// An explicit --topic wins when it differs from default. Otherwise one
// positional supplies the topic when the flag was left at default. The
// shared --topic flag defaults to /chatter like echo and pub, so no
// positional with default flag reports the default topic.
func resolveInfoTopic(flagText string, args []string) (string, error) {
	if len(args) > 1 {
		return "", fmt.Errorf("weld: topic info takes one topic, have %d args", len(args))
	}

	if flagText != "" && flagText != defaultTopic {
		return flagText, nil
	}

	if len(args) == 1 {
		return args[0], nil
	}

	if flagText != "" {
		return flagText, nil
	}

	return "", fmt.Errorf("weld: topic info needs --topic or one positional topic")
}

// topicSnapshotter is implemented by backends that expose a graph snapshot.
type topicSnapshotter interface {
	// SnapshotGraph returns a copy of the current graph snapshot.
	SnapshotGraph() GraphSnapshot
}

// sedpLocalViewer exposes local SEDP endpoints for snapshot building.
type sedpLocalViewer interface {
	// LocalEndpoints returns a sorted copy of local endpoints.
	LocalEndpoints() []dds.SEDPEndpoint
}

// sedpRemoteViewer exposes remote SEDP endpoints for snapshot building.
type sedpRemoteViewer interface {
	// RemoteEndpoints returns a sorted copy of remote endpoints.
	RemoteEndpoints() []dds.SEDPEndpoint
}

// discoveryGenerationViewer exposes the discovery generation.
type discoveryGenerationViewer interface {
	// DiscoveryGeneration returns the local announcement generation.
	DiscoveryGeneration() uint64
}

// openTopicBackendFunc opens the live backend for topic verbs.
//
// It is a variable so tests inject a fake without sockets.
var openTopicBackendFunc = openBackend

// topicDiscoveryWaitVar bounds the settle wait and is stubbed in tests.
var topicDiscoveryWaitVar = topicDiscoveryWait

// fetchTopicSnapshot reads a graph snapshot from backend without blocking.
//
// It prefers SnapshotGraph when the backend implements it, else builds
// from LocalEndpoints plus RemoteEndpoints when both exist, else returns
// an empty snapshot. The empty case keeps CLI usable before the backend
// exposes graph state and still exercises list plus info formatting.
func fetchTopicSnapshot(backend transports.Backend) (GraphSnapshot, error) {
	if backend == nil {
		return GraphSnapshot{}, fmt.Errorf("weld: topic snapshot needs a backend")
	}

	if snapper, ok := backend.(topicSnapshotter); ok {
		snap := snapper.SnapshotGraph()
		out := make([]TopicEndpoint, len(snap.Endpoints))
		copy(out, snap.Endpoints)

		return GraphSnapshot{Generation: snap.Generation, Endpoints: out}, nil
	}

	localViewer, hasLocal := backend.(sedpLocalViewer)
	remoteViewer, hasRemote := backend.(sedpRemoteViewer)

	if hasLocal && hasRemote {
		locals := localViewer.LocalEndpoints()
		remotes := remoteViewer.RemoteEndpoints()

		merged := make([]TopicEndpoint, 0, len(locals)+len(remotes))
		for _, ep := range locals {
			merged = append(merged, EndpointFromSEDP(ep))
		}

		for _, ep := range remotes {
			merged = append(merged, EndpointFromSEDP(ep))
		}

		var gen uint64
		if genViewer, ok := backend.(discoveryGenerationViewer); ok {
			gen = genViewer.DiscoveryGeneration()
		}

		return GraphSnapshot{Generation: gen, Endpoints: merged}, nil
	}

	return GraphSnapshot{}, nil
}

// waitForTopicDiscovery pauses for SPDP and SEDP to settle or ctx to end.
func waitForTopicDiscovery(ctx context.Context) error {
	wait := topicDiscoveryWaitVar
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil {
			return fmt.Errorf("weld: topic discovery wait: %w", err)
		}

		return nil
	case <-timer.C:
		return nil
	}
}

// openAndSnapshotTopic opens the backend, joins discovery, waits, and snapshots.
//
// It closes the backend before returning so list and info share one path.
func openAndSnapshotTopic(ctx context.Context, domain uint32) (GraphSnapshot, error) {
	backend, err := openTopicBackendFunc(ctx, domain, topicParticipant)
	if err != nil {
		return GraphSnapshot{}, fmt.Errorf("weld: open backend: %w", err)
	}

	snap, ferr := func() (GraphSnapshot, error) {
		err := waitForTopicDiscovery(ctx)
		if err != nil {
			return GraphSnapshot{}, err
		}

		snap, err := fetchTopicSnapshot(backend)
		if err != nil {
			return GraphSnapshot{}, fmt.Errorf("weld: snapshot graph on domain %d: %w", domain, err)
		}

		return snap, nil
	}()

	cerr := backend.Close()
	if ferr != nil && cerr != nil {
		return GraphSnapshot{}, fmt.Errorf("weld: topic snapshot on domain %d: %w", domain, errors.Join(ferr, cerr))
	}

	if cerr != nil {
		return GraphSnapshot{}, fmt.Errorf("weld: close topic backend: %w", cerr)
	}

	if ferr != nil {
		return GraphSnapshot{}, ferr
	}

	return snap, nil
}

// findTopicSummary returns the summary for topic or false when absent.
func findTopicSummary(summaries []TopicSummary, topic string) (TopicSummary, bool) {
	for _, s := range summaries {
		if s.Name == topic {
			return s, true
		}
	}

	return TopicSummary{}, false
}

// printTopicMismatches emits loud mismatch lines to stderr.
func printTopicMismatches(stderr io.Writer, mismatches []core.MismatchEvent) {
	for _, m := range mismatches {
		_, _ = fmt.Fprintln(stderr, FormatMismatch(m)) //nolint:errcheck // final error output is best effort
	}
}

// runTopicCommand dispatches topic list and info verbs.
func runTopicCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		_, _ = fmt.Fprintln(stderr, "weld: topic needs one of list, info") //nolint:errcheck // final error output is best effort

		return 2
	}

	switch args[0] {
	case "list":
		return runTopicListCommand(ctx, args[1:], stdout, stderr)
	case "info":
		return runTopicInfoCommand(ctx, args[1:], stdout, stderr)
	case "-h", "-help", "--help", "help":
		printUsage(stdout)

		return 0
	default:
		printUsage(stderr)
		_, _ = fmt.Fprintf(stderr, "weld: unknown topic subcommand %q\n", args[0]) //nolint:errcheck // final error output is best effort

		return 2
	}
}

// runTopicListCommand parses list flags and prints topics with counts.
func runTopicListCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("topic list", flag.ContinueOnError)
	fs.SetOutput(stderr)

	if isHelpRequest(args) {
		printUsage(stdout)

		return 0
	}

	domainFlag := fs.Uint("domain", uint(defaultDomainID()), "DDS domain id, default from ROS_DOMAIN_ID")
	includeHidden := fs.Bool("include-hidden", false, "show hidden topics with underscore segments")

	err := fs.Parse(args)
	if err != nil {
		return 2
	}

	if fs.NArg() != 0 {
		_, _ = fmt.Fprintln(stderr, "weld: topic list takes no positional args") //nolint:errcheck // final error output is best effort

		return 2
	}

	domain, err := resolveEffectiveDomain(*domainFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	snap, err := openAndSnapshotTopic(ctx, domain)
	if err != nil {
		return handleRunError(err, stderr)
	}

	summaries := SummarizeTopics(snap, *includeHidden)

	_, _ = fmt.Fprint(stdout, FormatTopicList(summaries)) //nolint:errcheck // help output failure is not actionable

	for _, s := range summaries {
		printTopicMismatches(stderr, DetectMismatches(s.Name, s.Endpoints))
	}

	return 0
}

// runTopicInfoCommand parses info flags and prints one topic with hashes.
func runTopicInfoCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("topic info", flag.ContinueOnError)
	fs.SetOutput(stderr)

	if isHelpRequest(args) {
		printUsage(stdout)

		return 0
	}

	topicFlag, domainFlag := topicDomainFlags(fs)

	err := fs.Parse(args)
	if err != nil {
		return 2
	}

	topic, err := resolveInfoTopic(*topicFlag, fs.Args())
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	err = validateTopic(topic)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	domain, err := resolveEffectiveDomain(*domainFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	snap, err := openAndSnapshotTopic(ctx, domain)
	if err != nil {
		return handleRunError(err, stderr)
	}

	summaries := SummarizeTopics(snap, true)

	summary, ok := findTopicSummary(summaries, topic)
	if !ok {
		_, _ = fmt.Fprintf(stderr, "weld: unknown topic %q\n", topic) //nolint:errcheck // final error output is best effort

		return 1
	}

	mismatches := DetectMismatches(summary.Name, summary.Endpoints)

	_, _ = fmt.Fprint(stdout, FormatTopicInfo(summary, mismatches)) //nolint:errcheck // help output failure is not actionable
	printTopicMismatches(stderr, mismatches)

	return 0
}
