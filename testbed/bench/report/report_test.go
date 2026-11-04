// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package report

import (
	"encoding/xml"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"testing"
)

// sampleResults returns two fixed results for rendering tests.
func sampleResults() []Result {
	return []Result{
		{
			Scenario:     "alpha",
			Leg:          "intra",
			PayloadBytes: 64,
			RateHz:       100,
			WarmupN:      10,
			MeasuredN:    100,
			Sent:         100,
			Recv:         98,
			Dups:         1,
			Gaps:         []int{3, 7},
			LossPct:      2,
			LatNs:        Summary{Min: 100, P50: 200, P95: 300, P99: 400, Max: 500, Mean: 220, Stddev: 60},
			ThrMsgs:      1000,
			ThrMiBs:      0.5,
			CPUS:         0.2,
			RSSMiB:       12.5,
			DiscoveryMs:  3.1,
			Notes:        "first",
		},
		{
			Scenario:     "beta",
			Leg:          "inter",
			PayloadBytes: 1024,
			RateHz:       50,
			WarmupN:      5,
			MeasuredN:    50,
			Sent:         50,
			Recv:         50,
			LossPct:      0,
			LatNs:        Summary{Min: 1000, P50: 2000, P95: 3000, P99: 4000, Max: 5000, Mean: 2100, Stddev: 400},
			ThrMsgs:      500,
			ThrMiBs:      1.25,
			CPUS:         0.4,
			RSSMiB:       14,
			DiscoveryMs:  4.2,
		},
	}
}

// TestAccPercentilesPinsKnownSeries checks nearest-rank pins on 1..1000.
func TestAccPercentilesPinsKnownSeries(t *testing.T) {
	t.Parallel()
	var a Acc
	for i := range 1000 {
		a.Add(float64(i + 1))
	}
	got := a.Summary()
	if got.Min != 1 {
		t.Errorf("min = %v, want 1", got.Min)
	}
	if got.P50 != 500 {
		t.Errorf("p50 = %v, want 500", got.P50)
	}
	if got.P95 != 950 {
		t.Errorf("p95 = %v, want 950", got.P95)
	}
	if got.P99 != 990 {
		t.Errorf("p99 = %v, want 990", got.P99)
	}
	if got.Max != 1000 {
		t.Errorf("max = %v, want 1000", got.Max)
	}
	if got.Mean != 500.5 {
		t.Errorf("mean = %v, want 500.5", got.Mean)
	}
	wantStddev := math.Sqrt(83333.25)
	if math.Abs(got.Stddev-wantStddev) > 1e-9 {
		t.Errorf("stddev = %v, want %v", got.Stddev, wantStddev)
	}
}

// TestAccEmptySummaryIsZero checks empty Summary defines zeros without NaN.
func TestAccEmptySummaryIsZero(t *testing.T) {
	t.Parallel()
	var a Acc
	got := a.Summary()
	for _, v := range []float64{got.Min, got.P50, got.P95, got.P99, got.Max, got.Mean, got.Stddev} {
		if math.IsNaN(v) {
			t.Fatal("empty summary contains NaN")
		}
		if v != 0 {
			t.Errorf("empty summary field = %v, want 0", v)
		}
	}
	var nilAcc *Acc
	nilAcc.Add(1)
	ngot := nilAcc.Summary()
	if ngot != (Summary{}) {
		t.Errorf("nil summary = %+v, want zeros", ngot)
	}
}

// TestHTMLCoversScenariosAndMethod checks names, footer, and inline SVG.
func TestHTMLCoversScenariosAndMethod(t *testing.T) {
	t.Parallel()
	html := HTML(sampleResults(), "Demo Title")
	for _, want := range []string{"Demo Title", "alpha", "beta", "nearest-rank", "<svg"} {
		if !strings.Contains(html, want) {
			t.Errorf("html lacks %q", want)
		}
	}
	if strings.Contains(html, "<script") || strings.Contains(html, "stylesheet") {
		t.Errorf("html must not reference external assets or scripts")
	}
	assertWellFormed(t, BarThroughput(sampleResults()))
	assertWellFormed(t, CDFLatency(sampleResults()))
	assertWellFormed(t, BarLatencyGrouped(sampleResults()))
}

// TestSVGEscapesScenarioNames checks special chars escape and parse safely.
func TestSVGEscapesScenarioNames(t *testing.T) {
	t.Parallel()
	name := `a<b>&"c`
	rs := []Result{
		{
			Scenario:     name,
			Leg:          `x<y>&"z`,
			PayloadBytes: 64,
			ThrMsgs:      10,
			LatNs:        Summary{Min: 1, P50: 2, P95: 3, P99: 4, Max: 5, Mean: 2, Stddev: 1},
		},
	}
	svgs := []string{BarThroughput(rs), CDFLatency(rs), BarLatencyGrouped(rs)}
	for _, svg := range svgs {
		if strings.Contains(svg, name) {
			t.Errorf("raw scenario name leaked into svg")
		}
		for _, esc := range []string{"&lt;", "&gt;", "&amp;", "&#34;"} {
			if !strings.Contains(svg, esc) {
				t.Errorf("svg lacks escape %q", esc)
			}
		}
		if !strings.Contains(svg, `viewBox="0 0 960 420"`) {
			t.Errorf("svg lacks fixed viewBox")
		}
		if !strings.Contains(svg, `font-family="sans-serif"`) {
			t.Errorf("svg lacks sans-serif family")
		}
		assertWellFormed(t, svg)
	}
}

// TestEmptyInputsStayValid checks empty renders keep shape without panic.
func TestEmptyInputsStayValid(t *testing.T) {
	t.Parallel()
	if !strings.Contains(HTML(nil, ""), "Bench Report") {
		t.Errorf("empty html lacks default title")
	}
	for _, svg := range []string{BarThroughput(nil), CDFLatency(nil), BarLatencyGrouped(nil)} {
		if !strings.Contains(svg, "no data") {
			t.Errorf("empty svg lacks placeholder")
		}
		assertWellFormed(t, svg)
	}
}

// assertWellFormed fails the test when s is not well-formed XML.
func assertWellFormed(t *testing.T, s string) {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(s))
	for {
		_, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatalf("svg is not well-formed XML: %v", err)
		}
	}
}

// TestLegColorPinsStackColors checks fixed Weld blue versus ROS orange.
func TestLegColorPinsStackColors(t *testing.T) {
	t.Parallel()

	if got := legColor("L1"); got != "#2563eb" {
		t.Fatalf("weld-weld color = %q, want weld blue", got)
	}
	if got := legColor("weld-weld"); got != legColor("L1") {
		t.Fatalf("weld-weld alias color = %q, want L1 color", got)
	}
	if got := legColor("L2"); got != "#ea580c" {
		t.Fatalf("ros-ros color = %q, want ros orange", got)
	}
	if got := legColor("L1c"); got != "#60a5fa" {
		t.Fatalf("L1c color = %q, want light weld blue", got)
	}
	if legColor("L1c") == legColor("L1") || legColor("L1c") == legColor("L2") {
		t.Fatal("L1c must not reuse the L1 or L2 color")
	}
	if got := legDisplay("L1c"); got != "weld-weld-container" {
		t.Fatalf("L1c display = %q, want weld-weld-container", got)
	}
	if legColor("L2") == legColor("L1") {
		t.Fatal("ros and weld legs share a color, want distinct stacks")
	}
	if got := legColor("custom"); got != "#6b7280" {
		t.Fatalf("unknown leg color = %q, want gray", got)
	}
	if got := legDisplay("L3"); got != "weld-ros" {
		t.Fatalf("L3 display = %q, want weld-ros", got)
	}
	if got := legDisplay("custom"); got != "custom" {
		t.Fatalf("unknown leg display = %q, want passthrough", got)
	}
}

// TestBarLatencyGroupedPinsGroupedBars checks one scenario renders three
// opacity-graded bars on a microsecond axis with a leg plus percentile legend.
func TestBarLatencyGroupedPinsGroupedBars(t *testing.T) {
	t.Parallel()
	rs := []Result{
		{Scenario: "p0", Leg: "L1", PayloadBytes: 256, LatNs: Summary{Min: 100000, P50: 200000, P95: 300000, P99: 400000, Max: 500000, Mean: 250000, Stddev: 100000}},
		{Scenario: "p0", Leg: "L2", PayloadBytes: 256, LatNs: Summary{Min: 200000, P50: 300000, P95: 400000, P99: 500000, Max: 600000, Mean: 350000, Stddev: 100000}},
	}
	svg := BarLatencyGrouped(rs)
	for _, want := range []string{"weld-weld", "ros-ros", "p50 solid", "p95 medium", "p99 faint", "latency us", `fill-opacity="0.55"`, `fill-opacity="0.3"`} {
		if !strings.Contains(svg, want) {
			t.Errorf("grouped latency svg lacks %q", want)
		}
	}
	if strings.Contains(svg, "payload bytes") {
		t.Errorf("grouped latency svg must not carry the size axis")
	}
	assertWellFormed(t, svg)
}

// TestBarLatencyGroupedLegendClearsData pins the outside legend: the white
// legend backing rect must start at or right of the plot edge, never over
// data, and every bar must end left of it.
func TestBarLatencyGroupedLegendClearsData(t *testing.T) {
	t.Parallel()
	rs := []Result{
		{Scenario: "p0", Leg: "L1", LatNs: Summary{P50: 256000, P95: 388000, P99: 694000}},
		{Scenario: "p0", Leg: "L1c", LatNs: Summary{P50: 251000, P95: 417000, P99: 1148000}},
		{Scenario: "p0", Leg: "L3", LatNs: Summary{P50: 460000, P95: 646000, P99: 1045000}},
		{Scenario: "p0", Leg: "L4", LatNs: Summary{P50: 333000, P95: 600000, P99: 610000}},
		{Scenario: "p0", Leg: "L2", LatNs: Summary{P50: 431000, P95: 605000, P99: 610000}},
	}
	svg := BarLatencyGrouped(rs)
	legendX := -1.0
	maxBarRight := 0.0
	for _, m := range parseRects(svg) {
		if m.fill == "#ffffff" {
			legendX = m.x
			continue
		}
		if m.fill == "none" || !strings.HasPrefix(m.fill, "#") {
			continue
		}
		if r := m.x + m.w; r > maxBarRight {
			maxBarRight = r
		}
	}
	if legendX < 0 {
		t.Fatalf("no white legend rect found")
	}
	if legendX < float64(svgPlotX+svgPlotW) {
		t.Fatalf("legend x=%.1f sits inside the plot ending at %d", legendX, svgPlotX+svgPlotW)
	}
	if legendX < maxBarRight+4 {
		t.Fatalf("legend x=%.1f overlaps bars ending at %.1f", legendX, maxBarRight)
	}
}

type svgRect struct {
	x, w float64
	fill string
}

func parseRects(svg string) []svgRect {
	var out []svgRect
	for seg := range strings.SplitSeq(svg, "<rect") {
		x := rectAttr(seg, `x="`)
		w := rectAttr(seg, `width="`)
		fill := rectStrAttr(seg, `fill="`)
		if x < 0 || w < 0 || fill == "" {
			continue
		}
		out = append(out, svgRect{x: x, w: w, fill: fill})
	}
	return out
}

func rectAttr(seg, key string) float64 {
	_, rest, ok := strings.Cut(seg, key)
	if !ok {
		return -1
	}
	val, _, ok := strings.Cut(rest, `"`)
	if !ok {
		return -1
	}
	v, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return -1
	}
	return v
}

func rectStrAttr(seg, key string) string {
	_, rest, ok := strings.Cut(seg, key)
	if !ok {
		return ""
	}
	val, _, ok := strings.Cut(rest, `"`)
	if !ok {
		return ""
	}
	return val
}

// TestCDFLatencyLegendClearsData pins the outside legend for marker
// curves: the white box sits right of the plot edge and every polyline
// point ends left of it.
func TestCDFLatencyLegendClearsData(t *testing.T) {
	t.Parallel()
	rs := []Result{
		{Scenario: "p0", Leg: "L3", LatNs: Summary{P50: 445000, P95: 700000, P99: 900000}},
		{Scenario: "p1", Leg: "L3", LatNs: Summary{P50: 452000, P95: 750000, P99: 950000}},
		{Scenario: "p5", Leg: "L3", LatNs: Summary{P50: 11291000, P95: 14000000, P99: 17000000}},
	}
	svg := CDFLatency(rs)
	legendX := sweepLegendX(svg)
	if legendX < 0 {
		t.Fatalf("no white legend rect found")
	}
	if legendX < float64(svgPlotX+svgPlotW) {
		t.Fatalf("legend x=%.1f sits inside the plot ending at %d", legendX, svgPlotX+svgPlotW)
	}
	maxPX := maxPolylineX(svg)
	if maxPX == 0 {
		t.Fatalf("no polyline points found")
	}
	if legendX < maxPX+4 {
		t.Fatalf("legend x=%.1f overlaps curves ending at %.1f", legendX, maxPX)
	}
}

// TestSweepLatencyVsThroughputPinsCurveAndLossRing checks the sweep chart:
// one p50 curve per leg in leg colors, red rings on lossy points, and a
// clean note with no sweep rows.
func TestSweepLatencyVsThroughputPinsCurveAndLossRing(t *testing.T) {
	t.Parallel()
	rs := []Result{
		{Scenario: "sw50", Leg: "L1", ThrMsgs: 50, LossPct: 0, LatNs: Summary{P50: 250000, P99: 400000}},
		{Scenario: "sw400", Leg: "L1", ThrMsgs: 390, LossPct: 2.5, Gaps: []int{7}, LatNs: Summary{P50: 900000, P99: 5000000}},
		{Scenario: "sw50", Leg: "L2", ThrMsgs: 50, LossPct: 0, LatNs: Summary{P50: 450000, P99: 700000}},
		{Scenario: "p0", Leg: "L1", ThrMsgs: 100, LossPct: 0, LatNs: Summary{P50: 250000, P99: 400000}},
	}
	svg := SweepLatencyVsThroughput(rs)
	for _, want := range []string{"weld-weld", "ros-ros", "p50 solid+circles (leg color)", "p99 dashed+triangles (leg color)", "red ring marks loss", "achieved throughput", "latency (us", "#dc2626", `stroke-dasharray="5,3"`} {
		if !strings.Contains(svg, want) {
			t.Errorf("sweep svg lacks %q", want)
		}
	}
	if strings.Contains(svg, "sw50 (weld-weld)") {
		t.Errorf("sweep svg must label legs, not scenarios")
	}
	assertWellFormed(t, svg)
	empty := SweepLatencyVsThroughput([]Result{{Scenario: "p0", Leg: "L1"}})
	if !strings.Contains(empty, "no sweep data") {
		t.Errorf("empty sweep svg lacks note")
	}
	assertWellFormed(t, empty)
}

// TestSweepLatencyVsThroughputLegendClearsData pins the outside legend
// for the sweep chart: the white box sits right of the plot edge, curves
// and triangles end left of it.
func TestSweepLatencyVsThroughputLegendClearsData(t *testing.T) {
	t.Parallel()
	rs := []Result{
		{Scenario: "sw50", Leg: "L3", ThrMsgs: 50, LatNs: Summary{P50: 445000, P99: 900000}},
		{Scenario: "sw800", Leg: "L3", ThrMsgs: 780, LatNs: Summary{P50: 2000000, P99: 17000000}},
		{Scenario: "sw50", Leg: "L4", ThrMsgs: 50, LatNs: Summary{P50: 333000, P99: 610000}},
		{Scenario: "sw800", Leg: "L4", ThrMsgs: 700, LossPct: 1.0, Gaps: []int{9}, LatNs: Summary{P50: 3000000, P99: 20000000}},
	}
	svg := SweepLatencyVsThroughput(rs)
	if legendX, maxPX := sweepLegendX(svg), maxPolylineX(svg); legendX < float64(svgPlotX+svgPlotW) || legendX < maxPX+4 {
		t.Fatalf("legend x=%.1f must clear plot edge %d and curves ending at %.1f", legendX, svgPlotX+svgPlotW, maxPX)
	}
	triMax := maxTriangleX(svg)
	if legendX(svg) < triMax+4 {
		t.Fatalf("legend overlaps p99 triangles ending at %.1f", triMax)
	}
}

// maxPointsX returns the largest x across every points list for one SVG
// element tag ("<polyline" or "<polygon"). Both marker shapes store x,y
// pairs the same way, so one walker covers curves and triangles.
func maxPointsX(svg, tag string) float64 {
	maxPX := 0.0
	for seg := range strings.SplitSeq(svg, tag) {
		_, after, ok := strings.Cut(seg, `points="`)
		if !ok {
			continue
		}
		val, _, ok := strings.Cut(after, `"`)
		if !ok {
			continue
		}
		for pair := range strings.SplitSeq(val, " ") {
			xstr, _, ok := strings.Cut(pair, ",")
			if !ok {
				continue
			}
			x, err := strconv.ParseFloat(xstr, 64)
			if err != nil {
				continue
			}
			maxPX = max(maxPX, x)
		}
	}
	return maxPX
}

// maxPolylineX returns the largest x across all polyline point lists.
func maxPolylineX(svg string) float64 {
	return maxPointsX(svg, "<polyline")
}

func sweepLegendX(svg string) float64 {
	for _, m := range parseRects(svg) {
		if m.fill == "#ffffff" {
			return m.x
		}
	}
	return -1
}

func legendX(svg string) float64 {
	return sweepLegendX(svg)
}

// maxTriangleX returns the largest polygon point x in svg.
func maxTriangleX(svg string) float64 {
	return maxPointsX(svg, "<polygon")
}
