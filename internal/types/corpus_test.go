// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package types

import (
	"slices"
	"strings"
	"testing"
)

// TestCorpus checks the fixture table shape, ordering, and semantic notes.
func TestCorpus(t *testing.T) {
	entries := Corpus()
	if len(entries) == 0 {
		t.Error("Corpus is empty")
	}

	seen := make(map[string]bool, len(entries))
	for i, e := range entries {
		if e.Package == "" || e.Name == "" || e.Summary == "" {
			t.Errorf("entry %d has empty package, name, or summary: %+v", i, e)
		}
		switch e.Kind {
		case KindMessage, KindService:
		default:
			t.Errorf("entry %d has invalid kind %q", i, e.Kind)
		}
		key := e.Package + "/" + e.Name
		if seen[key] {
			t.Errorf("duplicate corpus entry %s", key)
		}
		seen[key] = true
	}

	t.Run("topological rank", func(t *testing.T) {
		checkTopologicalRank(t, entries)
	})

	t.Run("dependency DAG", func(t *testing.T) {
		checkDependencyDAG(t, entries)
	})

	t.Run("dependency order", func(t *testing.T) {
		checkDependencyOrder(t, entries)
	})

	t.Run("header semantics", func(t *testing.T) {
		checkHeaderSemantics(t)
	})

	t.Run("time layout", func(t *testing.T) {
		checkTimeLayout(t)
	})

	t.Run("bounded sequence", func(t *testing.T) {
		checkBoundedSequence(t)
	})

	t.Run("inline arrays", func(t *testing.T) {
		checkInlineArrays(t, entries)
	})

	t.Run("type hash", func(t *testing.T) {
		checkTypeHashEmpty(t, entries)
	})

	t.Run("service coverage", func(t *testing.T) {
		checkServiceCoverage(t)
	})
}

// checkTopologicalRank asserts package tiers never run backward. Packages
// sharing a tier may appear in any order, so only the rank is checked.
func checkTopologicalRank(t *testing.T, entries []CorpusEntry) {
	t.Helper()
	rank := map[string]int{
		"builtin_interfaces": 0,
		"std_msgs":           1,
		"geometry_msgs":      2,
		"sensor_msgs":        3,
		"trajectory_msgs":    3,
		"nav_msgs":           3,
		"stereo_msgs":        4,
		"shape_msgs":         4,
		"visualization_msgs": 4,
		"diagnostic_msgs":    4,
		"std_srvs":           5,
	}
	prev := -1
	for i, e := range entries {
		r, ok := rank[e.Package]
		if !ok {
			t.Errorf("entry %d has unknown package %q", i, e.Package)
			continue
		}
		if r < prev {
			t.Errorf("topological order violated at index %d: %s/%s rank %d after rank %d", i, e.Package, e.Name, r, prev)
		}
		prev = r
	}
}

// checkDependencyDAG asserts every listed dependency appears earlier in the
// table and that header flags agree with the dependency lists.
// checkDAGOrder verifies dependency ordering and unknown refs.
func checkDAGOrder(t *testing.T, entries []CorpusEntry, index map[string]int) {
	t.Helper()
	for i, e := range entries {
		for _, dep := range e.Dependencies {
			if !strings.Contains(dep, "/") {
				t.Errorf("entry %s/%s has malformed dependency %q", e.Package, e.Name, dep)
				continue
			}
			j, ok := index[dep]
			if !ok {
				t.Errorf("entry %s/%s depends on unknown %s", e.Package, e.Name, dep)
				continue
			}
			if j >= i {
				t.Errorf("dependency order violated: %s/%s at index %d needs %s at index %d", e.Package, e.Name, i, dep, j)
			}
		}
	}
}

// checkDAGHasHeader verifies HasHeader entries carry dependency and note.
func checkDAGHasHeader(t *testing.T, e CorpusEntry) {
	t.Helper()
	if e.HasHeader {
		if !slices.Contains(e.Dependencies, "std_msgs/Header") {
			t.Errorf("entry %s/%s sets HasHeader without a std_msgs/Header dependency", e.Package, e.Name)
		}
		if e.HeaderNote == "" {
			t.Errorf("entry %s/%s sets HasHeader with empty HeaderNote", e.Package, e.Name)
		}
	}
}

// checkDAGHeaderDep verifies Header dependency implies HasHeader.
func checkDAGHeaderDep(t *testing.T, e CorpusEntry, allowNoHeader map[string]bool) {
	t.Helper()
	for _, dep := range e.Dependencies {
		if dep == "std_msgs/Header" && !e.HasHeader && !allowNoHeader[e.Package+"/"+e.Name] {
			t.Errorf("entry %s/%s lists std_msgs/Header without HasHeader", e.Package, e.Name)
		}
	}
	if e.HeaderNote != "" && !e.HasHeader && (e.Package != "std_msgs" || e.Name != "Header") {
		t.Errorf("entry %s/%s has a HeaderNote without HasHeader", e.Package, e.Name)
	}
}

// checkDAGHeaderFlags verifies header flag consistency.
func checkDAGHeaderFlags(t *testing.T, entries []CorpusEntry) {
	t.Helper()
	allowNoHeader := map[string]bool{
		"std_msgs/Header": true,
	}
	for _, e := range entries {
		checkDAGHasHeader(t, e)
		checkDAGHeaderDep(t, e, allowNoHeader)
	}
}

func checkDependencyDAG(t *testing.T, entries []CorpusEntry) {
	t.Helper()
	index := make(map[string]int, len(entries))
	for i, e := range entries {
		index[e.Package+"/"+e.Name] = i
	}
	checkDAGOrder(t, entries, index)
	checkDAGHeaderFlags(t, entries)
}

// checkHeaderSemantics spot checks header notes for key types.
func checkHeaderSemantics(t *testing.T) {
	t.Helper()
	cases := []struct {
		pkg  string
		name string
		want []string
	}{
		{"std_msgs", "Header", []string{"header carries stamp plus frame_id"}},
		{"geometry_msgs", "TransformStamped", []string{"frame_id is reference", "child_frame_id is target"}},
		{"nav_msgs", "Odometry", []string{"odometry header frame_id is pose parent", "child_frame_id"}},
		{"sensor_msgs", "LaserScan", []string{"first ray", "+z", "+x"}},
		{"sensor_msgs", "MultiEchoLaserScan", []string{"first ray", "+z", "+x"}},
		{"sensor_msgs", "Image", []string{"image stamp is acquisition", "optical frame"}},
		{"stereo_msgs", "DisparityImage", []string{"left image acquisition", "optical frame"}},
		{"nav_msgs", "Path", []string{"path header is path frame", "per-pose", "fallback"}},
		{"sensor_msgs", "NavSatFix", []string{"navsatfix stamp is ros time", "antenna frame"}},
		{"sensor_msgs", "JointState", []string{"joint stamp is recording time"}},
		{"sensor_msgs", "MultiDOFJointState", []string{"joint stamp is recording time"}},
		{"trajectory_msgs", "JointTrajectory", []string{"trajectory stamp is reference time"}},
		{"trajectory_msgs", "MultiDOFJointTrajectory", []string{"trajectory stamp is reference time"}},
		{"sensor_msgs", "TimeReference", []string{"header carries stamp plus frame_id"}},
		{"visualization_msgs", "Marker", []string{"header carries stamp plus frame_id"}},
		{"nav_msgs", "OccupancyGrid", []string{"header carries stamp plus frame_id"}},
	}
	for _, c := range cases {
		e, ok := Lookup(c.pkg, c.name)
		if !ok {
			t.Errorf("Lookup(%q, %q) missing", c.pkg, c.name)
			continue
		}
		if !e.HasHeader && (c.pkg != "std_msgs" || c.name != "Header") {
			t.Errorf("Lookup(%q, %q) wants HasHeader set", c.pkg, c.name)
		}
		note := strings.ToLower(e.HeaderNote)
		for _, w := range c.want {
			if !strings.Contains(note, strings.ToLower(w)) {
				t.Errorf("Lookup(%q, %q) HeaderNote %q missing %q", c.pkg, c.name, e.HeaderNote, w)
			}
		}
	}
}

// checkTimeLayout asserts Time and Duration carry layout in Summary only.
func checkTimeLayout(t *testing.T) {
	t.Helper()
	cases := []struct {
		pkg  string
		name string
	}{
		{"builtin_interfaces", "Time"},
		{"builtin_interfaces", "Duration"},
	}
	for _, c := range cases {
		e, ok := Lookup(c.pkg, c.name)
		if !ok {
			t.Errorf("Lookup(%q, %q) missing", c.pkg, c.name)
			continue
		}
		if e.HeaderNote != "" {
			t.Errorf("Lookup(%q, %q) HeaderNote = %q, want empty", c.pkg, c.name, e.HeaderNote)
		}
		if e.HasHeader {
			t.Errorf("Lookup(%q, %q) HasHeader = true, want false", c.pkg, c.name)
		}
		summary := strings.ToLower(e.Summary)
		for _, w := range []string{"int32 sec", "uint32 nanosec"} {
			if !strings.Contains(summary, w) {
				t.Errorf("Lookup(%q, %q) Summary %q missing %q", c.pkg, c.name, e.Summary, w)
			}
		}
	}
}

// checkBoundedSequence asserts the single bounded sequence fixture.
func checkBoundedSequence(t *testing.T) {
	t.Helper()
	e, ok := Lookup("shape_msgs", "SolidPrimitive")
	if !ok {
		t.Error("Lookup(shape_msgs, SolidPrimitive) missing")
		return
	}
	if e.Kind != KindMessage {
		t.Errorf("SolidPrimitive kind = %q, want message", e.Kind)
	}
	summary := strings.ToLower(e.Summary)
	for _, w := range []string{"max 3", "bounded", "dimensions"} {
		if !strings.Contains(summary, w) {
			t.Errorf("SolidPrimitive Summary %q missing %q", e.Summary, w)
		}
	}
	entries := Corpus()
	for _, other := range entries {
		if other.Package == "shape_msgs" && other.Name == "SolidPrimitive" {
			continue
		}
		if strings.Contains(strings.ToLower(other.Summary), "bounded") {
			t.Errorf("entry %s/%s Summary %q mentions bounded, want only SolidPrimitive to mention it", other.Package, other.Name, other.Summary)
		}
		if strings.Contains(strings.ToLower(other.HeaderNote), "bounded") {
			t.Errorf("entry %s/%s HeaderNote %q mentions bounded, want only SolidPrimitive Summary to mention it", other.Package, other.Name, other.HeaderNote)
		}
	}
}

// checkDependencyOrder asserts Header-first field order convention.
func checkDependencyOrder(t *testing.T, entries []CorpusEntry) {
	t.Helper()
	for _, e := range entries {
		for i, dep := range e.Dependencies {
			if dep == "std_msgs/Header" && i != 0 {
				t.Errorf("entry %s/%s lists std_msgs/Header at index %d, want index 0", e.Package, e.Name, i)
			}
		}
	}
}

// checkInlineArrays asserts fixed size arrays name inline storage.
func checkInlineArrays(t *testing.T, entries []CorpusEntry) {
	t.Helper()
	fixed := []string{"[36]", "[9]", "[12]", "[4]", "[3]"}
	for _, e := range entries {
		lower := strings.ToLower(e.Summary)
		needsInline := false
		for _, f := range fixed {
			if strings.Contains(lower, f) {
				needsInline = true
				break
			}
		}
		if needsInline && !strings.Contains(lower, "inline") {
			t.Errorf("entry %s/%s Summary %q has fixed array without inline", e.Package, e.Name, e.Summary)
		}
	}
}

// checkTypeHashEmpty asserts Phase 1 TypeHash stays empty.
// TODO(hash): populate RIHS01 hashes, then require exact format for every entry.
func checkTypeHashEmpty(t *testing.T, entries []CorpusEntry) {
	t.Helper()
	for _, e := range entries {
		if e.TypeHash != "" && !isTypeHashFormat(e.TypeHash) {
			t.Errorf("entry %s/%s TypeHash %q has bad RIHS01 format", e.Package, e.Name, e.TypeHash)
		}
		if e.TypeHash != "" {
			t.Errorf("entry %s/%s TypeHash = %q, want empty in Phase 1", e.Package, e.Name, e.TypeHash)
		}
	}
}

// isTypeHashFormat reports RIHS01 plus 64 lowercase hex for 71 chars total.
func isTypeHashFormat(s string) bool {
	const prefix = "RIHS01_"
	if len(s) != len(prefix)+64 {
		return false
	}
	if !strings.HasPrefix(s, prefix) {
		return false
	}
	for _, c := range s[len(prefix):] {
		isDigit := c >= '0' && c <= '9'
		isHex := c >= 'a' && c <= 'f'
		if !isDigit && !isHex {
			return false
		}
	}
	return true
}

// checkServiceCoverage asserts required service fixtures exist.
func checkServiceCoverage(t *testing.T) {
	t.Helper()
	services := [][2]string{
		{"sensor_msgs", "SetCameraInfo"},
		{"nav_msgs", "GetMap"},
		{"nav_msgs", "GetPlan"},
		{"nav_msgs", "LoadMap"},
		{"nav_msgs", "SetMap"},
		{"diagnostic_msgs", "AddDiagnostics"},
		{"diagnostic_msgs", "SelfTest"},
		{"std_srvs", "Empty"},
		{"std_srvs", "SetBool"},
		{"std_srvs", "Trigger"},
	}
	for _, s := range services {
		e, ok := Lookup(s[0], s[1])
		if !ok {
			t.Errorf("Lookup(%q, %q) missing", s[0], s[1])
			continue
		}
		if e.Kind != KindService {
			t.Errorf("Lookup(%q, %q) kind = %q, want service", s[0], s[1], e.Kind)
		}
	}
}

// TestFloatSentinels checks float sentinel wording stays in Summary only
// while HeaderNote stays header-only.
// checkFloatSentinelCase verifies one sentinel entry.
func checkFloatSentinelCase(t *testing.T, pkg, name string, summaryWant, forbidden []string) {
	t.Helper()
	e, ok := Lookup(pkg, name)
	if !ok {
		t.Errorf("Lookup(%q, %q) missing", pkg, name)
		return
	}
	summary := strings.ToLower(e.Summary)
	for _, w := range summaryWant {
		if !strings.Contains(summary, strings.ToLower(w)) {
			t.Errorf("Lookup(%q, %q) Summary %q missing %q", pkg, name, e.Summary, w)
		}
	}
	note := strings.ToLower(e.HeaderNote)
	for _, w := range forbidden {
		if strings.Contains(note, w) {
			t.Errorf("Lookup(%q, %q) HeaderNote %q must stay header-only, found %q", pkg, name, e.HeaderNote, w)
		}
	}
	if e.HasHeader && !strings.Contains(note, "header") && !strings.Contains(note, "stamp") && !strings.Contains(note, "frame") {
		t.Errorf("Lookup(%q, %q) HeaderNote %q must mention header, stamp, or frame", pkg, name, e.HeaderNote)
	}
}

// floatSentinelCases returns the sentinel table.
func floatSentinelCases() []struct {
	pkg         string
	name        string
	summaryWant []string
} {
	return []struct {
		pkg         string
		name        string
		summaryWant []string
	}{
		{"sensor_msgs", "FluidPressure", []string{"nan means unmeasured"}},
		{"sensor_msgs", "Illuminance", []string{"nan means unmeasured"}},
		{"sensor_msgs", "RelativeHumidity", []string{"nan means unmeasured"}},
		{"sensor_msgs", "Temperature", []string{"nan means unmeasured"}},
		{"sensor_msgs", "BatteryState", []string{"nan when unknown"}},
		{"sensor_msgs", "Range", []string{"discard outside min_range/max_range"}},
		{"sensor_msgs", "LaserScan", []string{"discard outside range_min/range_max"}},
		{"sensor_msgs", "MultiEchoLaserScan", []string{"discard outside range_min/range_max"}},
		{"sensor_msgs", "MagneticField", []string{"-1 means disregard"}},
		{"sensor_msgs", "Imu", []string{"-1 means disregard", "all zero means unknown"}},
		{"sensor_msgs", "CameraInfo", []string{"zeroed k r p mean uncalibrated", "inline"}},
		{"nav_msgs", "OccupancyGrid", []string{"row major", "resolution plus origin"}},
		{"visualization_msgs", "Marker", []string{"lifetime zero means forever"}},
		{"visualization_msgs", "MarkerArray", []string{"per-element lifetime zero means forever"}},
	}
}

func TestFloatSentinels(t *testing.T) {
	forbidden := []string{"nan", "discard", "disregard", "unknown", "uncalibrated", "row major", "lifetime", "bounded", "enum", "zeroed", "resolution", "outside", "all zero", "-1 means"}
	for _, c := range floatSentinelCases() {
		checkFloatSentinelCase(t, c.pkg, c.name, c.summaryWant, forbidden)
	}

	e, ok := Lookup("visualization_msgs", "MarkerArray")
	if !ok {
		t.Error("Lookup(visualization_msgs, MarkerArray) missing")
		return
	}
	if e.HeaderNote != "" {
		t.Errorf("MarkerArray HeaderNote = %q, want empty (no direct Header field)", e.HeaderNote)
	}
	if e.HasHeader {
		t.Error("MarkerArray HasHeader = true, want false (no direct Header field)")
	}
}

// TestCorpusCompleteness asserts the table matches the manifest allow-list.
// The manifest is synced by hand from ROS IDL at
// https://github.com/ros2/common_interfaces/tree/jazzy for the Jazzy
// distro. To sync, diff Corpus against the linked IDL, keep topological
// order and Header-first field order, then update the allow-list below.
func TestCorpusCompleteness(t *testing.T) {
	manifest := []string{
		"builtin_interfaces/Duration",
		"builtin_interfaces/Time",
		"std_msgs/Bool",
		"std_msgs/Byte",
		"std_msgs/Char",
		"std_msgs/ColorRGBA",
		"std_msgs/Empty",
		"std_msgs/Float32",
		"std_msgs/Float64",
		"std_msgs/Header",
		"std_msgs/Int16",
		"std_msgs/Int32",
		"std_msgs/Int64",
		"std_msgs/Int8",
		"std_msgs/String",
		"std_msgs/UInt16",
		"std_msgs/UInt32",
		"std_msgs/UInt64",
		"std_msgs/UInt8",
		"std_msgs/MultiArrayDimension",
		"std_msgs/MultiArrayLayout",
		"std_msgs/ByteMultiArray",
		"std_msgs/Float32MultiArray",
		"std_msgs/Float64MultiArray",
		"std_msgs/Int16MultiArray",
		"std_msgs/Int32MultiArray",
		"std_msgs/Int64MultiArray",
		"std_msgs/Int8MultiArray",
		"std_msgs/UInt16MultiArray",
		"std_msgs/UInt32MultiArray",
		"std_msgs/UInt64MultiArray",
		"std_msgs/UInt8MultiArray",
		"geometry_msgs/Point",
		"geometry_msgs/Point32",
		"geometry_msgs/Quaternion",
		"geometry_msgs/Vector3",
		"geometry_msgs/Accel",
		"geometry_msgs/Inertia",
		"geometry_msgs/Polygon",
		"geometry_msgs/Pose",
		"geometry_msgs/Transform",
		"geometry_msgs/Twist",
		"geometry_msgs/Wrench",
		"geometry_msgs/AccelWithCovariance",
		"geometry_msgs/PoseWithCovariance",
		"geometry_msgs/TwistWithCovariance",
		"geometry_msgs/AccelStamped",
		"geometry_msgs/AccelWithCovarianceStamped",
		"geometry_msgs/InertiaStamped",
		"geometry_msgs/PointStamped",
		"geometry_msgs/PolygonStamped",
		"geometry_msgs/PoseArray",
		"geometry_msgs/PoseStamped",
		"geometry_msgs/PoseWithCovarianceStamped",
		"geometry_msgs/QuaternionStamped",
		"geometry_msgs/TransformStamped",
		"geometry_msgs/TwistStamped",
		"geometry_msgs/TwistWithCovarianceStamped",
		"geometry_msgs/Vector3Stamped",
		"geometry_msgs/WrenchStamped",
		"sensor_msgs/BatteryState",
		"sensor_msgs/ChannelFloat32",
		"sensor_msgs/CompressedImage",
		"sensor_msgs/FluidPressure",
		"sensor_msgs/Illuminance",
		"sensor_msgs/Image",
		"sensor_msgs/Imu",
		"sensor_msgs/JointState",
		"sensor_msgs/Joy",
		"sensor_msgs/JoyFeedback",
		"sensor_msgs/JoyFeedbackArray",
		"sensor_msgs/LaserEcho",
		"sensor_msgs/LaserScan",
		"sensor_msgs/MagneticField",
		"sensor_msgs/MultiDOFJointState",
		"sensor_msgs/MultiEchoLaserScan",
		"sensor_msgs/NavSatStatus",
		"sensor_msgs/NavSatFix",
		"sensor_msgs/PointCloud",
		"sensor_msgs/PointField",
		"sensor_msgs/PointCloud2",
		"sensor_msgs/Range",
		"sensor_msgs/RegionOfInterest",
		"sensor_msgs/CameraInfo",
		"sensor_msgs/RelativeHumidity",
		"sensor_msgs/Temperature",
		"sensor_msgs/TimeReference",
		"sensor_msgs/SetCameraInfo",
		"trajectory_msgs/JointTrajectoryPoint",
		"trajectory_msgs/JointTrajectory",
		"trajectory_msgs/MultiDOFJointTrajectoryPoint",
		"trajectory_msgs/MultiDOFJointTrajectory",
		"nav_msgs/GridCells",
		"nav_msgs/MapMetaData",
		"nav_msgs/OccupancyGrid",
		"nav_msgs/Odometry",
		"nav_msgs/Path",
		"nav_msgs/GetMap",
		"nav_msgs/GetPlan",
		"nav_msgs/LoadMap",
		"nav_msgs/SetMap",
		"stereo_msgs/DisparityImage",
		"shape_msgs/MeshTriangle",
		"shape_msgs/Mesh",
		"shape_msgs/Plane",
		"shape_msgs/SolidPrimitive",
		"visualization_msgs/ImageMarker",
		"visualization_msgs/Marker",
		"visualization_msgs/MarkerArray",
		"visualization_msgs/MenuEntry",
		"visualization_msgs/InteractiveMarkerControl",
		"visualization_msgs/InteractiveMarker",
		"visualization_msgs/InteractiveMarkerInit",
		"visualization_msgs/InteractiveMarkerPose",
		"visualization_msgs/InteractiveMarkerFeedback",
		"visualization_msgs/InteractiveMarkerUpdate",
		"diagnostic_msgs/KeyValue",
		"diagnostic_msgs/DiagnosticStatus",
		"diagnostic_msgs/DiagnosticArray",
		"diagnostic_msgs/AddDiagnostics",
		"diagnostic_msgs/SelfTest",
		"std_srvs/Empty",
		"std_srvs/SetBool",
		"std_srvs/Trigger",
	}
	entries := Corpus()
	if len(entries) != len(manifest) {
		t.Errorf("Corpus length = %d, manifest length = %d", len(entries), len(manifest))
	}
	got := make(map[string]bool, len(entries))
	for _, e := range entries {
		got[e.Package+"/"+e.Name] = true
	}
	for _, m := range manifest {
		if !got[m] {
			t.Errorf("manifest entry %s missing from Corpus", m)
		}
	}
	want := make(map[string]bool, len(manifest))
	for _, m := range manifest {
		if want[m] {
			t.Errorf("manifest lists duplicate entry %s", m)
		}
		want[m] = true
	}
	for _, e := range entries {
		key := e.Package + "/" + e.Name
		if !want[key] {
			t.Errorf("Corpus entry %s not in manifest allow-list", key)
		}
	}
}

// TestCorpusCopyIsolated asserts Corpus copies stay isolated from mutation.
func TestCorpusCopyIsolated(t *testing.T) {
	first := Corpus()
	if len(first) == 0 {
		t.Error("Corpus is empty")
		return
	}
	first[0].Summary = "mutated summary"
	depIdx := -1
	for i, e := range first {
		if len(e.Dependencies) > 0 {
			depIdx = i
			break
		}
	}
	if depIdx < 0 {
		t.Error("no Corpus entry carries Dependencies")
		return
	}
	first[depIdx].Dependencies[0] = "mutated/dep"

	second := Corpus()
	if second[0].Summary == "mutated summary" {
		t.Error("Corpus shares entry storage: mutating the copy changed the table")
	}
	if second[depIdx].Dependencies[0] == "mutated/dep" {
		t.Error("Corpus shares dependency storage: mutating the copy changed the table")
	}
	e, ok := Lookup(first[depIdx].Package, first[depIdx].Name)
	if !ok {
		t.Errorf("Lookup(%q, %q) missing after copy mutation", first[depIdx].Package, first[depIdx].Name)
		return
	}
	if e.Dependencies[0] == "mutated/dep" {
		t.Error("Lookup reflects copy mutation: table storage is shared")
	}
}

// TestLookupCopyIsolated asserts Lookup copies stay isolated from mutation.
func TestLookupCopyIsolated(t *testing.T) {
	e, ok := Lookup("sensor_msgs", "Imu")
	if !ok {
		t.Error("Lookup(sensor_msgs, Imu) missing")
		return
	}
	if len(e.Dependencies) == 0 {
		t.Error("Imu carries no Dependencies to mutate")
		return
	}
	e.Dependencies[0] = "mutated/dep"
	e.Summary = "mutated summary"

	again, ok := Lookup("sensor_msgs", "Imu")
	if !ok {
		t.Error("Lookup(sensor_msgs, Imu) missing on second call")
		return
	}
	if len(again.Dependencies) > 0 && again.Dependencies[0] == "mutated/dep" {
		t.Error("Lookup shares dependency storage: mutating the copy changed the table")
	}
	if again.Summary == "mutated summary" {
		t.Error("Lookup shares entry storage: mutating the copy changed the table")
	}
}

// TestLookupMiss asserts Lookup misses are exact and case-sensitive.
func TestLookupMiss(t *testing.T) {
	cases := []struct {
		pkg  string
		name string
	}{
		{"nope", "Nothing"},
		{"std_msgs", "string"},
		{"STD_MSGS", "String"},
		{"sensor_msgs", "image"},
		{"std_msgs", ""},
		{"", "String"},
		{"std_msgs", "String "},
		{"std_msgs", "HeaderExtra"},
	}
	for _, c := range cases {
		if e, ok := Lookup(c.pkg, c.name); ok {
			t.Errorf("Lookup(%q, %q) = %+v, want miss", c.pkg, c.name, e)
		}
	}
}
