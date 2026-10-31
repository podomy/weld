// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/podomy/weld/internal/core"
)

// DoctorReport is the result of checking every topic in a snapshot.
//
// Summaries holds per topic endpoint info in sorted order, including hidden
// underscore topics so nothing stays silent. Mismatches holds every detected
// event sorted by topic plus publisher plus subscriber plus policy kind,
// each carrying topic plus endpoint pair plus policy kind. Counter holds
// monotonically increasing per policy counts plus a total; it is the counter
// passed to BuildDoctorReport, or a fresh one when nil was passed.
type DoctorReport struct {
	// Summaries holds per topic endpoint info for topic info display.
	Summaries []TopicSummary
	// Mismatches holds every loud mismatch event across topics.
	Mismatches []core.MismatchEvent
	// Counter holds per policy counts plus a total for metrics display.
	Counter *core.MismatchCounter
}

// BuildDoctorReport detects mismatches for every topic in snap, records each
// event into counter, and returns the sorted report.
//
// Type plus QoS mismatches flow through DetectMismatches, so reliable versus
// best effort and volatile versus transient local pairs raise incompatible
// error events with their policy kind instead of staying silent. A nil
// counter allocates a fresh one so events always count.
func BuildDoctorReport(snap GraphSnapshot, counter *core.MismatchCounter) DoctorReport {
	if counter == nil {
		counter = core.NewMismatchCounter()
	}
	summaries := SummarizeTopics(snap, true)
	var mismatches []core.MismatchEvent
	for _, s := range summaries {
		found := DetectMismatches(s.Name, s.Endpoints)
		for _, m := range found {
			counter.Record(m)
			mismatches = append(mismatches, m)
		}
	}
	slices.SortFunc(mismatches, func(a, b core.MismatchEvent) int {
		if c := strings.Compare(a.Topic, b.Topic); c != 0 {
			return c
		}
		if c := strings.Compare(a.Publisher, b.Publisher); c != 0 {
			return c
		}
		if c := strings.Compare(a.Subscriber, b.Subscriber); c != 0 {
			return c
		}
		return strings.Compare(string(a.Policy), string(b.Policy))
	})
	return DoctorReport{Summaries: summaries, Mismatches: mismatches, Counter: counter}
}

// DoctorHasErrors reports whether report holds any error severity event.
//
// Incompatible pairs (for example reliable versus best effort) surface as
// errors; unresolved markers surface as warnings. Both break DoctorIsClean,
// but this helper lets callers tell hard incompatibilities apart.
func DoctorHasErrors(report DoctorReport) bool {
	for _, m := range report.Mismatches {
		if m.Severity == core.SeverityError {
			return true
		}
	}
	return false
}

// DoctorIsClean reports whether report holds no mismatch events at all.
//
// Clean means zero events: warnings also break clean because unresolved
// markers must resolve before entity creation. Silence is a defect, so only
// an empty mismatch list counts as clean.
func DoctorIsClean(report DoctorReport) bool {
	return len(report.Mismatches) == 0
}

// FormatDoctorMetrics renders metrics counters exposition for the report.
//
// It delegates to core.FormatMismatchMetrics so CLI metrics share the single
// agreed shape with logs: one line per policy kind plus a total.
func FormatDoctorMetrics(counter *core.MismatchCounter) string {
	return core.FormatMismatchMetrics(counter)
}

// FormatDoctorReport renders topic info display plus mismatch lines plus
// metrics counters.
//
// Each topic prints through FormatTopicInfo (endpoints plus type hashes plus
// per topic mismatches), then a Metrics section prints the counter
// exposition. Compatible topics show their explicit none line; loud events
// show topic plus endpoint pair plus policy kind.
func FormatDoctorReport(report DoctorReport) string {
	var b strings.Builder
	_, _ = fmt.Fprintf(&b, "Doctor: %s across %s\n",
		pluralize(len(report.Mismatches), "mismatch", "mismatches"),
		pluralize(len(report.Summaries), "topic", "topics"))
	for _, s := range report.Summaries {
		var per []core.MismatchEvent
		for _, m := range report.Mismatches {
			if m.Topic == s.Name {
				per = append(per, m)
			}
		}
		_, _ = fmt.Fprint(&b, FormatTopicInfo(s, per))
	}
	_, _ = fmt.Fprint(&b, "Metrics:\n")
	_, _ = fmt.Fprint(&b, FormatDoctorMetrics(report.Counter))
	return b.String()
}

// printDoctorUsage writes doctor help.
func printDoctorUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, "Usage: weld doctor [--check] [flags]\n\ndoctor: list Weld entities with per topic QoS compatibility\n  weld doctor --domain 42\n  weld doctor --check --domain 42\n\nFlags:\n  --check exit 1 when any mismatch exists, 0 when clean\n  --domain uint DDS domain id, default from ROS_DOMAIN_ID\n") //nolint:errcheck // help output failure is not actionable
}

// runDoctorCommand parses doctor flags, snapshots discovery, prints topic
// info display plus metrics, and checks compatibility loudly.
//
// The full report always prints to stdout and loud mismatch lines go to
// stderr, so --check gates on the same output it verifies:
// --check exits 1 when any mismatch exists and 0 when clean. Compatible
// Weld pairs (facade reliable plus transient local against standard ROS
// subscriptions) print explicit none lines with zero metrics.
//
// This verb implementation lives here; expose it by adding a doctor case
// to run in main.go.
func runDoctorCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if isHelpRequest(args) {
		printDoctorUsage(stdout)
		return 0
	}
	domainFlag := fs.Uint("domain", uint(defaultDomainID()), "DDS domain id, default from ROS_DOMAIN_ID")
	checkFlag := fs.Bool("check", false, "exit 1 when any mismatch exists, 0 when clean")
	err := fs.Parse(args)
	if err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		_, _ = fmt.Fprintln(stderr, "weld: doctor takes no positional args") //nolint:errcheck // final error output is best effort
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
	report := BuildDoctorReport(snap, core.NewMismatchCounter())
	_, _ = fmt.Fprint(stdout, FormatDoctorReport(report)) //nolint:errcheck // report output failure is not actionable
	printTopicMismatches(stderr, report.Mismatches)
	if *checkFlag && !DoctorIsClean(report) {
		_, _ = fmt.Fprintf(stderr, "weld: doctor check failed with %d mismatches\n", len(report.Mismatches)) //nolint:errcheck // final error output is best effort
		return 1
	}
	return 0
}
