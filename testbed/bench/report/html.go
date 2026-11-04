// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package report

import (
	"fmt"
	"strconv"
	"strings"
)

// HTML renders a self-contained report page with the summary table and
// inline SVG charts.
//
// The page inlines the throughput, latency marker, latency by scenario,
// and latency-vs-throughput sweep charts and links no external assets,
// fonts, scripts, or stylesheets.
func HTML(rs []Result, title string) string {
	if strings.TrimSpace(title) == "" {
		title = "Bench Report"
	}
	out := "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n<title>" +
		svgEscape(title) + "</title>\n</head>\n<body>\n<h1>" + svgEscape(title) + "</h1>\n"
	out += htmlTable(rs)
	out += htmlGaps(rs)
	out += "<p>" + svgEscape(percentileFooter) + "</p>\n"
	out += "<h2>Throughput</h2>\n" + BarThroughput(rs)
	out += "\n<h2>Latency markers</h2>\n" + CDFLatency(rs)
	out += "\n<h2>Latency by scenario</h2>\n" + BarLatencyGrouped(rs)
	out += "\n<h2>Latency vs throughput (sweep)</h2>\n" + SweepLatencyVsThroughput(rs)
	return out + "\n</body>\n</html>\n"
}

// htmlTable renders the summary table shared with the Markdown output.
func htmlTable(rs []Result) string {
	ths := make([]string, 0, 13)
	for _, h := range []string{
		"Scenario", "Leg", "Payload", "Sent", "Recv", "Loss%",
		"Msg/s", "MiB/s", "p50", "p95", "p99", "Max", "Notes",
	} {
		ths = append(ths, "<th>"+svgEscape(h)+"</th>")
	}
	rows := make([]string, 0, len(rs))
	for _, r := range rs {
		rows = append(rows, htmlRow(r))
	}
	return "<table border=\"1\" cellpadding=\"4\" cellspacing=\"0\">\n<thead>\n<tr>" +
		strings.Join(ths, "") + "</tr>\n</thead>\n<tbody>\n" +
		strings.Join(rows, "") + "</tbody>\n</table>\n"
}

// htmlRow renders one result as an HTML table row.
func htmlRow(r Result) string {
	cells := []string{
		r.Scenario,
		r.Leg,
		strconv.Itoa(r.PayloadBytes),
		strconv.Itoa(r.Sent),
		strconv.Itoa(r.Recv),
		strconv.FormatFloat(r.LossPct, 'f', 2, 64),
		strconv.FormatFloat(r.ThrMsgs, 'f', 1, 64),
		strconv.FormatFloat(r.ThrMiBs, 'f', 3, 64),
		strconv.FormatFloat(r.LatNs.P50, 'f', 0, 64),
		strconv.FormatFloat(r.LatNs.P95, 'f', 0, 64),
		strconv.FormatFloat(r.LatNs.P99, 'f', 0, 64),
		strconv.FormatFloat(r.LatNs.Max, 'f', 0, 64),
		resultNote(r),
	}
	tds := make([]string, 0, len(cells))
	for _, c := range cells {
		tds = append(tds, "<td>"+svgEscape(c)+"</td>")
	}
	return "<tr>" + strings.Join(tds, "") + "</tr>\n"
}

// resultNote renders gaps, duplicates, and free-form notes for one result.
func resultNote(r Result) string {
	parts := make([]string, 0, 3)
	if len(r.Gaps) > 0 {
		parts = append(parts, fmt.Sprintf("gaps=%v", r.Gaps))
	}
	if r.Dups > 0 {
		parts = append(parts, "dups="+strconv.Itoa(r.Dups))
	}
	if r.Notes != "" {
		parts = append(parts, r.Notes)
	}
	return strings.Join(parts, " ")
}

// htmlGaps renders gap and duplicate signals as a list, or None.
func htmlGaps(rs []Result) string {
	items := []string{}
	for _, r := range rs {
		if len(r.Gaps) == 0 && r.Dups == 0 {
			continue
		}
		items = append(items, "<li>"+svgEscape(barLabel(r))+": "+svgEscape(resultNote(r))+"</li>\n")
	}
	if len(items) == 0 {
		items = append(items, "<li>None.</li>\n")
	}
	return "<h2>Gaps and duplicates</h2>\n<ul>\n" + strings.Join(items, "") + "</ul>\n"
}
