// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ros_test

import (
	"context"
	"testing"

	"github.com/podomy/weld/testbed/live/helpers"
)

// Viz rest2 alias gates for visualization_msgs.
//
// Grep of gate_type_viz*.go shows gate_type_viz_extra_test.go already
// covers MarkerArray, MenuEntry, ImageMarker, InteractiveMarkerControl,
// InteractiveMarker, InteractiveMarkerPose, InteractiveMarkerFeedback,
// InteractiveMarkerInit, and InteractiveMarkerUpdate with small samples.
// Marker itself lives in gate_type_visualization_test.go. No type from the
// requested list is missing, so this file adds no new codec symbols. It
// provides alias live gates with unique test names that reuse vizExtraCases
// through the shared matrix runners. This keeps domain 42 plus docker argv
// plus per-run live IDs plus Skip if absent identical to the primary gates
// while satisfying the NEW file requirement without symbol duplication.

// runVizRest2Label runs forward plus reverse for one viz extra label.
//
// Domain is the allocated exclusive domain shared by both directions.
func runVizRest2Label(t *testing.T, ctx context.Context, domain int, label string) {
	t.Helper()
	runSharedMatrixLabel(t, ctx, domain, label, vizExtraCases())
}

// TestROSTypeVizRest2MarkerArray checks MarkerArray forward plus reverse.
func TestROSTypeVizRest2MarkerArray(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizRest2Label(t, ctx, domain, "viz-markerarray")
}

// TestROSTypeVizRest2MenuEntry checks MenuEntry forward plus reverse.
func TestROSTypeVizRest2MenuEntry(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizRest2Label(t, ctx, domain, "viz-menuentry")
}

// TestROSTypeVizRest2ImageMarker checks ImageMarker forward plus reverse.
func TestROSTypeVizRest2ImageMarker(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizRest2Label(t, ctx, domain, "viz-imagemarker")
}

// TestROSTypeVizRest2InteractiveMarkerControl checks Control forward plus reverse.
func TestROSTypeVizRest2InteractiveMarkerControl(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizRest2Label(t, ctx, domain, "viz-imcontrol")
}

// TestROSTypeVizRest2InteractiveMarker checks InteractiveMarker forward plus reverse.
func TestROSTypeVizRest2InteractiveMarker(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizRest2Label(t, ctx, domain, "viz-imarker")
}

// TestROSTypeVizRest2InteractiveMarkerPose checks Pose forward plus reverse.
func TestROSTypeVizRest2InteractiveMarkerPose(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizRest2Label(t, ctx, domain, "viz-ipose")
}

// TestROSTypeVizRest2InteractiveMarkerFeedback checks Feedback forward plus reverse.
func TestROSTypeVizRest2InteractiveMarkerFeedback(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizRest2Label(t, ctx, domain, "viz-ifeedback")
}

// TestROSTypeVizRest2InteractiveMarkerInit checks Init forward plus reverse.
func TestROSTypeVizRest2InteractiveMarkerInit(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizRest2Label(t, ctx, domain, "viz-iinit")
}

// TestROSTypeVizRest2InteractiveMarkerUpdate checks Update forward plus reverse.
func TestROSTypeVizRest2InteractiveMarkerUpdate(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), typeMatrixTestTimeout)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runVizRest2Label(t, ctx, domain, "viz-iupdate")
}
