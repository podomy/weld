// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package report

import (
	"bytes"
	"encoding/xml"
	"math"
	"slices"
	"strconv"
	"strings"
)

// SVG canvas and plot geometry. Every chart uses the fixed 960x420 viewBox
// below with the sans-serif family on the root and no CSS or JS. The plot
// occupies the left 672 px; the right margin holds the legend outside the
// data area so swatches never cover bars or curves.
const (
	svgWidth  = 960
	svgHeight = 420
	svgPlotX  = 72
	svgPlotY  = 30
	svgPlotW  = 600
	svgPlotH  = 320
)

// svgLegendX is the left edge of the outside legend panel.
const svgLegendX = svgPlotX + svgPlotW + 12

// svgPoint is a plot coordinate pair in SVG pixels.
type svgPoint struct {
	x float64
	y float64
}

// svgLegendEntry maps one color and dash style to a label.
type svgLegendEntry struct {
	color  string
	label  string
	dashed bool
}

// legColor returns the fixed chart color for one bench leg so the Weld and
// ROS stacks read apart at a glance in every chart: Weld-only legs are
// blue, ROS-only legs are orange, interop legs get their own hues.
// Unknown legs map to gray so a new leg never silently reuses a stack
// color and its presence shows up as unassigned in review.
func legColor(leg string) string {
	switch leg {
	case "L1", "weld-weld":
		return "#2563eb"
	case "L1c", "weld-weld-container":
		return "#60a5fa"
	case "L2", "ros-ros":
		return "#ea580c"
	case "L3", "weld-ros":
		return "#0d9488"
	case "L4", "ros-weld":
		return "#7c3aed"
	default:
		return "#6b7280"
	}
}

// legDisplay renders a leg code as a readable chart label, passing unknown
// legs through unchanged so custom legs still label themselves.
func legDisplay(leg string) string {
	switch leg {
	case "L1":
		return "weld-weld"
	case "L1c":
		return "weld-weld-container"
	case "L2":
		return "ros-ros"
	case "L3":
		return "weld-ros"
	case "L4":
		return "ros-weld"
	default:
		return leg
	}
}

// svgEscape escapes s for XML text and attribute content via encoding/xml.
func svgEscape(s string) string {
	var buf bytes.Buffer
	err := xml.EscapeText(&buf, []byte(s))
	if err != nil {
		return ""
	}
	return buf.String()
}

// svgCoord formats a coordinate with one decimal place.
func svgCoord(v float64) string {
	return strconv.FormatFloat(v, 'f', 1, 64)
}

// svgNum formats a tick value compactly.
func svgNum(v float64) string {
	av := math.Abs(v)
	switch {
	case av != 0 && (av >= 1000000 || av < 0.001):
		return strconv.FormatFloat(v, 'e', 1, 64)
	case av >= 100:
		return strconv.FormatFloat(v, 'f', 0, 64)
	case av >= 1:
		return strconv.FormatFloat(v, 'f', 1, 64)
	default:
		return strconv.FormatFloat(v, 'f', 3, 64)
	}
}

// svgY maps v in [vmin,vmax] to a plot y coordinate.
func svgY(v, vmin, vmax float64) float64 {
	if vmax <= vmin {
		return float64(svgPlotY + svgPlotH/2)
	}
	return float64(svgPlotY+svgPlotH) - (v-vmin)/(vmax-vmin)*float64(svgPlotH)
}

// svgHead opens an SVG document with a fixed viewBox and an escaped title.
func svgHead(title string) string {
	return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 960 420" width="960" height="420" ` +
		`font-family="sans-serif" role="img"><title>` + svgEscape(title) + `</title>`
}

// svgTail closes an SVG document.
func svgTail() string {
	return `</svg>`
}

// svgHeading renders the chart heading centered above the plot.
func svgHeading(text string) string {
	return `<text x="480.0" y="18.0" font-size="13" text-anchor="middle">` + svgEscape(text) + `</text>`
}

// svgFrame renders the plot border rectangle.
func svgFrame() string {
	return `<rect x="` + svgCoord(float64(svgPlotX)) + `" y="` + svgCoord(float64(svgPlotY)) +
		`" width="` + strconv.Itoa(svgPlotW) + `" height="` + strconv.Itoa(svgPlotH) +
		`" fill="none" stroke="#333333" stroke-width="1"/>`
}

// svgEmptyNote renders the placeholder text for charts with no data.
func svgEmptyNote() string {
	return `<text x="372.0" y="190.0" font-size="12" text-anchor="middle">no data</text>`
}

// svgX maps v in [vmin,vmax] to a plot x coordinate across the full plot.
func svgX(v, vmin, vmax float64) float64 {
	if vmax <= vmin {
		return float64(svgPlotX + svgPlotW/2)
	}
	return float64(svgPlotX) + (v-vmin)/(vmax-vmin)*float64(svgPlotW)
}

// svgXCaption renders the x axis caption below the plot.
func svgXCaption(text string) string {
	return `<text x="372.0" y="400.0" font-size="10" text-anchor="middle">` + svgEscape(text) + `</text>`
}

// svgYCaption renders the y axis caption rotated beside the plot.
func svgYCaption(text string) string {
	return `<text x="16.0" y="190.0" font-size="10" text-anchor="middle" ` +
		`transform="rotate(-90 16 190)">` + svgEscape(text) + `</text>`
}

// svgTicks returns five evenly spaced ticks spanning [vmin,vmax].
//
//nolint:unparam // every chart starts its axis at zero today; the helper spans arbitrary ranges by design.
func svgTicks(vmin, vmax float64) []float64 {
	if vmax <= vmin {
		vmax = vmin + 1
	}
	const count = 5
	out := make([]float64, count)
	for i := range out {
		out[i] = vmin + (vmax-vmin)*float64(i)/float64(count-1)
	}
	return out
}

// svgYGrid renders horizontal gridlines plus value labels for ticks.
func svgYGrid(ticks []float64, vmin, vmax float64) string {
	parts := make([]string, 0, 2*len(ticks))
	for _, t := range ticks {
		y := svgCoord(svgY(t, vmin, vmax))
		parts = append(parts,
			`<line x1="`+svgCoord(float64(svgPlotX))+`" y1="`+y+
				`" x2="`+svgCoord(float64(svgPlotX+svgPlotW))+`" y2="`+y+
				`" stroke="#cccccc" stroke-width="1"/>`,
			`<text x="`+svgCoord(float64(svgPlotX)-6)+`" y="`+svgCoord(svgY(t, vmin, vmax)+3.5)+
				`" font-size="9" text-anchor="end">`+svgEscape(svgNum(t))+`</text>`,
		)
	}
	return strings.Join(parts, "")
}

// svgXGrid renders vertical gridlines plus value labels for ticks.
func svgXGrid(ticks []float64, vmin, vmax, bottom float64) string {
	parts := make([]string, 0, 2*len(ticks))
	for _, t := range ticks {
		x := svgCoord(svgX(t, vmin, vmax))
		parts = append(parts,
			`<line x1="`+x+`" y1="`+svgCoord(float64(svgPlotY))+
				`" x2="`+x+`" y2="`+svgCoord(bottom)+`" stroke="#cccccc" stroke-width="1"/>`,
			`<text x="`+x+`" y="`+svgCoord(bottom+14)+
				`" font-size="9" text-anchor="middle">`+svgEscape(svgNum(t))+`</text>`,
		)
	}
	return strings.Join(parts, "")
}

// svgCircle renders a filled circle marker with an escaped tooltip.
func svgCircle(x, y, r float64, color, tip string) string {
	return `<circle cx="` + svgCoord(x) + `" cy="` + svgCoord(y) + `" r="` + svgCoord(r) +
		`" fill="` + color + `"><title>` + svgEscape(tip) + `</title></circle>`
}

// svgSquare renders a filled square marker centered at x,y.
func svgSquare(x, y, half float64, color, tip string) string {
	return `<rect x="` + svgCoord(x-half) + `" y="` + svgCoord(y-half) +
		`" width="` + svgCoord(half*2) + `" height="` + svgCoord(half*2) +
		`" fill="` + color + `"><title>` + svgEscape(tip) + `</title></rect>`
}

// svgTriangle renders a filled triangle marker centered at x,y.
func svgTriangle(x, y, half float64, color, tip string) string {
	p1 := svgCoord(x) + `,` + svgCoord(y-half)
	p2 := svgCoord(x-half) + `,` + svgCoord(y+half)
	p3 := svgCoord(x+half) + `,` + svgCoord(y+half)
	return `<polygon points="` + p1 + ` ` + p2 + ` ` + p3 +
		`" fill="` + color + `"><title>` + svgEscape(tip) + `</title></polygon>`
}

// svgPolyline renders a polyline through pts in one color.
func svgPolyline(pts []svgPoint, color string, dashed bool) string {
	if len(pts) == 0 {
		return ""
	}
	coords := make([]string, 0, len(pts))
	for _, p := range pts {
		coords = append(coords, svgCoord(p.x)+","+svgCoord(p.y))
	}
	dash := ""
	if dashed {
		dash = ` stroke-dasharray="5,3"`
	}
	return `<polyline points="` + strings.Join(coords, " ") +
		`" fill="none" stroke="` + color + `" stroke-width="2"` + dash + `/>`
}

// svgLegend renders color swatches with escaped labels at x,y.
func svgLegend(entries []svgLegendEntry, x, y float64) string {
	parts := make([]string, 0, 2*len(entries))
	yy := y
	for _, e := range entries {
		dash := ""
		if e.dashed {
			dash = ` stroke-dasharray="5,3"`
		}
		parts = append(parts,
			`<line x1="`+svgCoord(x)+`" y1="`+svgCoord(yy)+
				`" x2="`+svgCoord(x+22)+`" y2="`+svgCoord(yy)+
				`" stroke="`+e.color+`" stroke-width="3"`+dash+`/>`,
			`<text x="`+svgCoord(x+26)+`" y="`+svgCoord(yy+3.5)+
				`" font-size="10" text-anchor="start">`+svgEscape(e.label)+`</text>`,
		)
		yy += 15
	}
	return strings.Join(parts, "")
}

// svgLegendBox renders legend entries over a white backing rect at x,y.
func svgLegendBox(entries []svgLegendEntry, x, y float64) string {
	widest := 0
	for _, e := range entries {
		widest = max(widest, len(e.label))
	}
	w := 38 + widest*6
	h := 10 + 15*len(entries)
	out := `<rect x="` + svgCoord(x) + `" y="` + svgCoord(y) +
		`" width="` + strconv.Itoa(w) + `" height="` + strconv.Itoa(h) +
		`" fill="#ffffff" stroke="#333333" stroke-width="1"/>`
	return out + svgLegend(entries, x+6, y+13)
}

// barLabel returns the chart label for a result, adding the leg when set.
func barLabel(r Result) string {
	if r.Leg == "" {
		return r.Scenario
	}
	return r.Scenario + " (" + legDisplay(r.Leg) + ")"
}

// barMax returns the largest throughput, floored at 1.
func barMax(rs []Result) float64 {
	top := 0.0
	for _, r := range rs {
		top = max(top, r.ThrMsgs)
	}
	if top <= 0 {
		return 1
	}
	return top
}

// BarThroughput renders message throughput per scenario as an SVG bar chart.
func BarThroughput(rs []Result) string {
	const title = "Throughput messages per second by scenario"
	out := svgHead(title) + svgHeading("Throughput (messages/s by scenario)")
	if len(rs) == 0 {
		return out + svgFrame() + svgEmptyNote() + svgTail()
	}
	top := barMax(rs)
	out += svgYGrid(svgTicks(0, top), 0, top) + svgFrame()
	out += barRects(rs, top)
	out += svgXCaption("scenario") + svgYCaption("messages/s")
	return out + svgTail()
}

// barRects renders one bar per result with rotated labels and value tags.
func barRects(rs []Result, top float64) string {
	parts := make([]string, 0, 3*len(rs))
	slot := float64(svgPlotW) / float64(len(rs))
	base := svgY(0, 0, top)
	bottom := float64(svgPlotY + svgPlotH)
	for i, r := range rs {
		val := max(r.ThrMsgs, 0)
		y := svgY(val, 0, top)
		left := float64(svgPlotX) + slot*float64(i) + slot*0.2
		width := slot * 0.6
		color := legColor(r.Leg)
		tip := barLabel(r) + ": " + svgNum(val) + " messages/s"
		cx := left + width/2
		parts = append(parts,
			`<rect x="`+svgCoord(left)+`" y="`+svgCoord(y)+
				`" width="`+svgCoord(width)+`" height="`+svgCoord(base-y)+
				`" fill="`+color+`"><title>`+svgEscape(tip)+`</title></rect>`,
			`<text x="`+svgCoord(cx)+`" y="`+svgCoord(bottom+14)+
				`" font-size="9" text-anchor="end" transform="rotate(-25 `+
				svgCoord(cx)+` `+svgCoord(bottom+14)+`)">`+svgEscape(barLabel(r))+`</text>`,
			`<text x="`+svgCoord(cx)+`" y="`+svgCoord(y-5)+
				`" font-size="9" text-anchor="middle">`+svgEscape(svgNum(val))+`</text>`,
		)
	}
	return strings.Join(parts, "")
}

// cdfMax returns the largest p99 latency, floored at 1.
func cdfMax(rs []Result) float64 {
	top := 0.0
	for _, r := range rs {
		top = max(top, r.LatNs.P50, r.LatNs.P95, r.LatNs.P99)
	}
	if top <= 0 {
		return 1
	}
	return top
}

// CDFLatency renders per-scenario p50, p95, and p99 latency markers as
// grouped SVG marks with a legend.
//
// Each scenario contributes one polyline through (p50, 0.5), (p95, 0.95),
// and (p99, 0.99) with circle, square, and triangle marks in the scenario
// color. The x range spans the largest p99 and the y range is [0,1].
func CDFLatency(rs []Result) string {
	const title = "Latency cumulative markers by scenario"
	out := svgHead(title) + svgHeading("Latency markers (p50 circles, p95 squares, p99 triangles)")
	if len(rs) == 0 {
		return out + svgFrame() + svgEmptyNote() + svgTail()
	}
	xmax := cdfMax(rs)
	bottom := float64(svgPlotY + svgPlotH)
	out += svgXGrid(svgTicks(0, xmax), 0, xmax, bottom)
	out += cdfReferenceLines()
	out += svgFrame()
	body, entries := cdfBody(rs, xmax)
	out += body
	out += svgLegendBox(entries, float64(svgLegendX), float64(svgPlotY))
	out += svgXCaption("latency ns") + svgYCaption("cumulative probability")
	return out + svgTail()
}

// cdfReferenceLines renders dashed probability guides at 0.5, 0.95, and 0.99.
func cdfReferenceLines() string {
	parts := make([]string, 0, 6)
	for _, p := range []float64{0.5, 0.95, 0.99} {
		y := svgCoord(svgY(p, 0, 1))
		parts = append(parts,
			`<line x1="`+svgCoord(float64(svgPlotX))+`" y1="`+y+
				`" x2="`+svgCoord(float64(svgPlotX+svgPlotW))+`" y2="`+y+
				`" stroke="#999999" stroke-width="1" stroke-dasharray="4,3"/>`,
			`<text x="`+svgCoord(float64(svgPlotX)+4)+`" y="`+svgCoord(svgY(p, 0, 1)-4)+
				`" font-size="9" text-anchor="start">`+svgEscape(svgNum(p))+`</text>`,
		)
	}
	return strings.Join(parts, "")
}

// cdfBody renders one marker polyline per scenario plus a per-leg legend.
// Color encodes the leg, not the scenario, so the legend lists each leg
// once plus the marker key; scenarios stay distinguishable by position
// and tooltip. The legend lives outside the plot, so markers use the full
// width and never pass under the swatches.
func cdfBody(rs []Result, xmax float64) (string, []svgLegendEntry) {
	parts := make([]string, 0, 4*len(rs))
	for _, r := range rs {
		color := legColor(r.Leg)
		name := barLabel(r)
		p50 := max(r.LatNs.P50, 0)
		p95 := max(r.LatNs.P95, 0)
		p99 := max(r.LatNs.P99, 0)
		pts := []svgPoint{
			{svgX(p50, 0, xmax), svgY(0.5, 0, 1)},
			{svgX(p95, 0, xmax), svgY(0.95, 0, 1)},
			{svgX(p99, 0, xmax), svgY(0.99, 0, 1)},
		}
		parts = append(parts,
			svgPolyline(pts, color, false),
			svgCircle(pts[0].x, pts[0].y, 4, color, name+" p50="+svgNum(p50)+" ns"),
			svgSquare(pts[1].x, pts[1].y, 4, color, name+" p95="+svgNum(p95)+" ns"),
			svgTriangle(pts[2].x, pts[2].y, 5, color, name+" p99="+svgNum(p99)+" ns"),
		)
	}
	entries := make([]svgLegendEntry, 0, 8)
	for _, leg := range sortedLegs(rs) {
		entries = append(entries, svgLegendEntry{color: legColor(leg), label: legDisplay(leg)})
	}
	entries = append(entries,
		svgLegendEntry{color: "#333333", label: "p50 circles"},
		svgLegendEntry{color: "#888888", label: "p95 squares"},
		svgLegendEntry{color: "#cccccc", label: "p99 triangles"},
	)
	return strings.Join(parts, ""), entries
}

// sortedLegs returns the distinct legs in rs in sorted order so legends
// and per-leg curves render deterministically.
func sortedLegs(rs []Result) []string {
	seen := make(map[string]bool)
	legs := make([]string, 0, 5)
	for _, r := range rs {
		if !seen[r.Leg] {
			seen[r.Leg] = true
			legs = append(legs, r.Leg)
		}
	}
	slices.Sort(legs)
	return legs
}

// groupedLatencyMax returns the largest p50, p95, or p99 latency in
// microseconds, floored at 1 so an empty or zero series still frames.
func groupedLatencyMax(rs []Result) float64 {
	top := 0.0
	for _, r := range rs {
		top = max(top, r.LatNs.P50, r.LatNs.P95, r.LatNs.P99)
	}
	top /= 1000
	if top <= 0 {
		return 1
	}
	return top
}

// BarLatencyGrouped renders p50, p95, and p99 latency per scenario as
// grouped bars in microseconds.
//
// Each scenario contributes three bars in its leg color: p50 solid, p95
// at reduced opacity, p99 faint. Tooltips carry exact nanosecond values.
// The legend maps colors to scenarios and grays to percentiles.
func BarLatencyGrouped(rs []Result) string {
	const title = "Latency by scenario (p50, p95, p99)"
	out := svgHead(title) + svgHeading(title+" - solid p50, medium p95, faint p99 (us)")
	if len(rs) == 0 {
		return out + svgFrame() + svgEmptyNote() + svgTail()
	}
	top := groupedLatencyMax(rs) * 1.1
	bottom := float64(svgPlotY + svgPlotH)
	out += svgYGrid(svgTicks(0, top), 0, top) + svgFrame()
	out += groupedLatencyBars(rs, top, bottom)
	entries := groupedLatencyLegend(rs)
	out += svgLegendBox(entries, float64(svgLegendX), float64(svgPlotY))
	out += svgXCaption("scenario") + svgYCaption("latency us")
	return out + svgTail()
}

// groupedLatencyBars renders three opacity-graded bars per result across
// the full plot width. The legend lives outside the plot, so bars never
// pass under the swatches.
func groupedLatencyBars(rs []Result, top, bottom float64) string {
	parts := make([]string, 0, 6*len(rs))
	slot := float64(svgPlotW) / float64(len(rs))
	base := svgY(0, 0, top)
	width := slot * 0.22
	for i, r := range rs {
		left := float64(svgPlotX) + slot*float64(i)
		vals := []struct {
			v       float64
			opacity string
			name    string
		}{
			{max(r.LatNs.P50, 0) / 1000, "1", "p50"},
			{max(r.LatNs.P95, 0) / 1000, "0.55", "p95"},
			{max(r.LatNs.P99, 0) / 1000, "0.3", "p99"},
		}
		for k, bar := range vals {
			x := left + slot*(0.12+float64(k)*0.27)
			y := svgY(bar.v, 0, top)
			color := legColor(r.Leg)
			tip := barLabel(r) + " " + bar.name + "=" + svgNum(bar.v*1000) + " ns"
			parts = append(parts,
				`<rect x="`+svgCoord(x)+`" y="`+svgCoord(y)+
					`" width="`+svgCoord(width)+`" height="`+svgCoord(base-y)+
					`" fill="`+color+`" fill-opacity="`+bar.opacity+`"><title>`+svgEscape(tip)+`</title></rect>`,
			)
		}
		cx := left + slot/2
		name := barLabel(r)
		parts = append(parts,
			`<text x="`+svgCoord(cx)+`" y="`+svgCoord(bottom+14)+
				`" font-size="9" text-anchor="end" transform="rotate(-25 `+
				svgCoord(cx)+` `+svgCoord(bottom+14)+`)">`+svgEscape(name)+`</text>`,
			`<text x="`+svgCoord(left+slot*0.12+width/2)+`" y="`+svgCoord(svgY(max(r.LatNs.P50, 0)/1000, 0, top)-5)+
				`" font-size="9" text-anchor="middle">`+svgEscape(svgNum(max(r.LatNs.P50, 0)/1000))+`</text>`,
		)
	}
	return strings.Join(parts, "")
}

// groupedLatencyLegend maps leg colors to legs plus gray percentile keys.
// Color encodes the leg, not the scenario, so each leg appears once no
// matter how many scenarios share it; scenarios stay readable from the
// bar labels and tooltips.
func groupedLatencyLegend(rs []Result) []svgLegendEntry {
	entries := make([]svgLegendEntry, 0, 8)
	for _, leg := range sortedLegs(rs) {
		entries = append(entries, svgLegendEntry{color: legColor(leg), label: legDisplay(leg)})
	}
	entries = append(entries,
		svgLegendEntry{color: "#333333", label: "p50 solid"},
		svgLegendEntry{color: "#888888", label: "p95 medium"},
		svgLegendEntry{color: "#cccccc", label: "p99 faint"},
	)
	return entries
}

// svgRing renders a hollow red ring centered at x,y to flag a lossy sweep
// point: the stack choked there, so the latency marker alone would mislead.
func svgRing(x, y, r float64, tip string) string {
	return `<circle cx="` + svgCoord(x) + `" cy="` + svgCoord(y) + `" r="` + svgCoord(r+3) +
		`" fill="none" stroke="#dc2626" stroke-width="2"><title>` + svgEscape(tip) + `</title></circle>`
}

// isSweepRow reports whether r is a load-latency sweep point. Sweep tiers
// are named sw<rate> at fixed P1 payloads; every other scenario belongs to
// the paced matrix, fanout, fanin, or saturation charts.
func isSweepRow(r Result) bool {
	return strings.HasPrefix(r.Scenario, "sw")
}

// sweepMax returns the largest achieved throughput and p99 latency over
// sweep rows, floored at 1 so an empty or zero series still frames.
func sweepMax(rs []Result) (float64, float64) {
	topX, topY := 0.0, 0.0
	for _, r := range rs {
		topX = max(topX, r.ThrMsgs)
		topY = max(topY, r.LatNs.P99/1000)
	}
	if topX <= 0 {
		topX = 1
	}
	if topY <= 0 {
		topY = 1
	}
	return topX, topY
}

// sweepLegBody renders one leg's curves with markers and red loss rings.
// Points sort by achieved throughput so curves read left to right even
// when steps complete out of order. The p50 line is solid with circles;
// p99 rides a dashed line with triangles so the choke climb reads as a
// line, not scattered points. A ring sits on the p50 marker but flags the
// whole rate step, p99 included.
func sweepLegBody(leg string, pts []Result, xmax, ymax float64) string {
	slices.SortFunc(pts, func(a, b Result) int {
		switch {
		case a.ThrMsgs < b.ThrMsgs:
			return -1
		case a.ThrMsgs > b.ThrMsgs:
			return 1
		default:
			return 0
		}
	})
	parts := make([]string, 0, 5*len(pts)+2)
	color := legColor(leg)
	p50 := make([]svgPoint, 0, len(pts))
	p99 := make([]svgPoint, 0, len(pts))
	for _, r := range pts {
		p50 = append(p50, svgPoint{svgX(r.ThrMsgs, 0, xmax), svgY(max(r.LatNs.P50, 0)/1000, 0, ymax)})
		p99 = append(p99, svgPoint{svgX(r.ThrMsgs, 0, xmax), svgY(max(r.LatNs.P99, 0)/1000, 0, ymax)})
	}
	parts = append(parts, svgPolyline(p50, color, false), svgPolyline(p99, color, true))
	for i, r := range pts {
		name := legDisplay(leg) + " " + r.Scenario
		parts = append(parts,
			svgCircle(p50[i].x, p50[i].y, 4, color, name+": "+svgNum(r.ThrMsgs)+" messages/s p50="+svgNum(r.LatNs.P50)+" ns"),
			svgTriangle(p99[i].x, p99[i].y, 5, color, name+": p99="+svgNum(r.LatNs.P99)+" ns"),
		)
		if r.LossPct > 0 || len(r.Gaps) > 0 {
			parts = append(parts, svgRing(p50[i].x, p50[i].y, 4, name+": loss "+svgNum(r.LossPct)+"% gaps="+strconv.Itoa(len(r.Gaps))))
		}
	}
	return strings.Join(parts, "")
}

// SweepLatencyVsThroughput renders latency against achieved throughput for
// sweep rows: one p50 solid curve per leg in its leg color with circles,
// plus a p99 dashed curve with triangles. A red ring marks any lossy
// point, which is the choke signal: latency without the loss marker would
// hide the choke. Non-sweep rows never appear here; with no sweep rows the
// chart renders a note instead of an empty frame so default smoke reports
// stay clean.
func SweepLatencyVsThroughput(rs []Result) string {
	const title = "Latency vs throughput (1024 B sweep)"
	sweep := make([]Result, 0, len(rs))
	for _, r := range rs {
		if isSweepRow(r) {
			sweep = append(sweep, r)
		}
	}
	head := svgHead(title) + svgHeading(title+" - circles p50, triangles p99, red ring marks loss (us, achieved throughput x)")
	if len(sweep) == 0 {
		return head + svgFrame() + `<text x="372.0" y="190.0" font-size="12" text-anchor="middle">no sweep data (run WELD_BENCH_FULL=1 go test ./testbed/bench/ -run TestBenchSweep -count=1 -timeout 60m with fleet up)</text>` + svgTail()
	}
	xmax, ymax := sweepMax(sweep)
	xmax *= 1.1
	ymax *= 1.1
	byLeg := make(map[string][]Result)
	for _, r := range sweep {
		byLeg[r.Leg] = append(byLeg[r.Leg], r)
	}
	legs := make([]string, 0, len(byLeg))
	for leg := range byLeg {
		legs = append(legs, leg)
	}
	slices.Sort(legs)
	entries := make([]svgLegendEntry, 0, len(legs)+3)
	for _, leg := range legs {
		entries = append(entries, svgLegendEntry{color: legColor(leg), label: legDisplay(leg)})
	}
	entries = append(entries,
		svgLegendEntry{color: "#333333", label: "p50 solid+circles (leg color)"},
		svgLegendEntry{color: "#888888", label: "p99 dashed+triangles (leg color)", dashed: true},
		svgLegendEntry{color: "#dc2626", label: "red ring marks loss"},
	)
	bottom := float64(svgPlotY + svgPlotH)
	parts := []string{
		svgXGrid(svgTicks(0, xmax), 0, xmax, bottom),
		svgYGrid(svgTicks(0, ymax), 0, ymax) + svgFrame(),
	}
	for _, leg := range legs {
		parts = append(parts, sweepLegBody(leg, byLeg[leg], xmax, ymax))
	}
	parts = append(parts,
		svgLegendBox(entries, float64(svgLegendX), float64(svgPlotY)),
		svgXCaption("achieved throughput (messages/s)")+svgYCaption("latency (us, microseconds)"),
	)
	return head + strings.Join(parts, "") + svgTail()
}
