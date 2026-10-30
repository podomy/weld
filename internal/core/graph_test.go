// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"sync"
	"testing"
)

// TestGraph covers the partition local index: GID helpers, demangling,
// add plus upsert plus remove paths, generation plus hook behavior, and
// validation of demangled names plus translated QoS plus type hashes.
func TestGraph(t *testing.T) {
	t.Run("gid helpers", checkGraphGID)
	t.Run("demangle topic", checkGraphDemangleTopic)
	t.Run("demangle type", checkGraphDemangleType)
	t.Run("writer paths", checkGraphWriterPaths)
	t.Run("reader paths", checkGraphReaderPaths)
	t.Run("service paths", checkGraphServicePaths)
	t.Run("participant paths", checkGraphParticipantPaths)
	t.Run("generation debounce", checkGraphGenerationDebounce)
	t.Run("hook fires", checkGraphHook)
	t.Run("validation rejects wire form", checkGraphValidation)
	t.Run("concurrent use", checkGraphConcurrent)
}

// checkGraphGID pins GID string plus parse plus byte round trip.
func checkGraphGID(t *testing.T) {
	t.Helper()
	var zero GID
	if !zero.IsZero() {
		t.Fatalf("zero GID must report IsZero")
	}
	if zero.String() != "00000000000000000000000000000000" {
		t.Fatalf("zero GID string = %q", zero.String())
	}
	raw := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	gid, err := GIDFromBytes(raw)
	if err != nil {
		t.Fatalf("GIDFromBytes: %v", err)
	}
	if gid.IsZero() {
		t.Fatalf("nonzero GID must not report IsZero")
	}
	back, err := ParseGID(gid.String())
	if err != nil {
		t.Fatalf("ParseGID: %v", err)
	}
	if !gid.Equal(back) {
		t.Fatalf("GID round trip mismatch %s vs %s", gid.String(), back.String())
	}
	_, err = GIDFromBytes([]byte{1, 2, 3})
	if err == nil {
		t.Fatalf("short GID bytes must fail")
	}
	_, err = ParseGID("nothex")
	if err == nil {
		t.Fatalf("bad GID hex must fail")
	}
	_, err = ParseGID("00")
	if err == nil {
		t.Fatalf("short GID hex must fail")
	}
}

// checkGraphDemangleTopic pins DDS to ROS topic mapping.
func checkGraphDemangleTopic(t *testing.T) {
	t.Helper()
	cases := []struct {
		dds  string
		want string
	}{
		{"rt/chatter", "/chatter"},
		{"rt/a/b", "/a/b"},
		{"rq/add_two_intsRequest", "/add_two_ints"},
		{"rr/add_two_intsReply", "/add_two_ints"},
		{"chatter", "/chatter"},
	}
	for _, tc := range cases {
		got, err := DemangleTopicName(tc.dds)
		if err != nil {
			t.Fatalf("DemangleTopicName(%q): %v", tc.dds, err)
		}
		if got != tc.want {
			t.Fatalf("DemangleTopicName(%q) = %q, want %q", tc.dds, got, tc.want)
		}
	}
	for _, bad := range []string{"", "/rt/chatter", "rt/", "rt//a", "rq/add_two_ints", "rr/add_two_ints"} {
		_, err := DemangleTopicName(bad)
		if err == nil {
			t.Fatalf("DemangleTopicName(%q) must fail", bad)
		}
	}
}

// checkGraphDemangleType pins DDS to IDL type mapping.
func checkGraphDemangleType(t *testing.T) {
	t.Helper()
	got, err := DemangleTypeName("std_msgs::msg::dds_::String_")
	if err != nil {
		t.Fatalf("DemangleTypeName: %v", err)
	}
	if got != "std_msgs__msg__String" {
		t.Fatalf("demangled = %q, want std_msgs__msg__String", got)
	}
	req, err := DemangleTypeName("my_pkg::srv::dds_::AddTwoInts_Request_")
	if err != nil {
		t.Fatalf("DemangleTypeName request: %v", err)
	}
	if req != "my_pkg__srv__AddTwoInts_Request" {
		t.Fatalf("demangled request = %q", req)
	}
	for _, bad := range []string{"", "String_", "std_msgs::msg::String_", "a::dds_::b::dds_::c_"} {
		_, err := DemangleTypeName(bad)
		if err == nil {
			t.Fatalf("DemangleTypeName(%q) must fail", bad)
		}
	}
}

// sampleEntityInfo returns a valid writer or reader record.
func sampleEntityInfo() EntityInfo {
	return EntityInfo{
		TopicName: "/chatter",
		TopicType: "std_msgs__msg__String",
		TypeHash:  "",
		QoS:       DefaultQoS(),
	}
}

// sampleGID returns a deterministic GID from one trailing byte.
func sampleGID(last byte) GID {
	var gid GID
	gid[15] = last
	return gid
}

// checkGraphWriterPaths pins writer add plus upsert plus remove plus getters.
//
//nolint:cyclop // writer test sequences add, upsert, and remove checks in one run, splitting would obscure intent
func checkGraphWriterPaths(t *testing.T) {
	t.Helper()
	g := NewGraph()
	gid := sampleGID(1)
	info := sampleEntityInfo()
	err := g.AddWriter(gid, info)
	if err != nil {
		t.Fatalf("AddWriter: %v", err)
	}
	if g.WriterCount() != 1 {
		t.Fatalf("WriterCount = %d, want 1", g.WriterCount())
	}
	err = g.AddWriter(gid, info)
	if err == nil {
		t.Fatalf("duplicate AddWriter must fail")
	}
	got, ok := g.Writer(gid)
	if !ok {
		t.Fatalf("Writer must be found")
	}
	if got.TopicName != "/chatter" {
		t.Fatalf("Writer topic = %q", got.TopicName)
	}
	changed := info
	changed.TopicType = "std_msgs__msg__String"
	err = g.UpsertWriter(gid, changed)
	if err != nil {
		t.Fatalf("UpsertWriter identical: %v", err)
	}
	updated := info
	updated.TypeHash = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"
	err = g.UpsertWriter(gid, updated)
	if err != nil {
		t.Fatalf("UpsertWriter update: %v", err)
	}
	got, _ = g.Writer(gid)
	if got.TypeHash != updated.TypeHash {
		t.Fatalf("TypeHash not stored, got %q", got.TypeHash)
	}
	if got.QoS.Reliability != ReliabilityReliable {
		t.Fatalf("translated QoS not stored")
	}
	if !g.RemoveWriter(gid) {
		t.Fatalf("RemoveWriter must report true")
	}
	if g.RemoveWriter(gid) {
		t.Fatalf("second RemoveWriter must report false")
	}
	_, ok = g.Writer(gid)
	if ok {
		t.Fatalf("removed writer must be absent")
	}
}

// checkGraphReaderPaths pins reader add plus upsert plus remove paths.
func checkGraphReaderPaths(t *testing.T) {
	t.Helper()
	g := NewGraph()
	gid := sampleGID(2)
	info := sampleEntityInfo()
	info.TopicName = "/scan"
	err := g.AddReader(gid, info)
	if err != nil {
		t.Fatalf("AddReader: %v", err)
	}
	if g.ReaderCount() != 1 {
		t.Fatalf("ReaderCount = %d, want 1", g.ReaderCount())
	}
	if len(g.Readers()) != 1 {
		t.Fatalf("Readers snapshot must hold one entry")
	}
	err = g.UpsertReader(gid, info)
	if err != nil {
		t.Fatalf("UpsertReader identical: %v", err)
	}
	if !g.RemoveReader(gid) {
		t.Fatalf("RemoveReader must report true")
	}
	if g.ReaderCount() != 0 {
		t.Fatalf("ReaderCount after remove = %d", g.ReaderCount())
	}
}

// sampleServiceInfo returns a valid service record.
func sampleServiceInfo() ServiceInfo {
	return ServiceInfo{
		ServiceName:     "/add_two_ints",
		ServiceType:     "example__srv__AddTwoInts",
		ServiceTypeHash: "",
	}
}

// checkGraphServicePaths pins service add plus upsert plus remove with hash only.
func checkGraphServicePaths(t *testing.T) {
	t.Helper()
	g := NewGraph()
	gid := sampleGID(3)
	info := sampleServiceInfo()
	err := g.AddService(gid, info)
	if err != nil {
		t.Fatalf("AddService: %v", err)
	}
	if g.ServiceCount() != 1 {
		t.Fatalf("ServiceCount = %d, want 1", g.ServiceCount())
	}
	err = g.AddService(gid, info)
	if err == nil {
		t.Fatalf("duplicate AddService must fail")
	}
	updated := info
	updated.ServiceTypeHash = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"
	err = g.UpsertService(gid, updated)
	if err != nil {
		t.Fatalf("UpsertService: %v", err)
	}
	got, ok := g.Service(gid)
	if !ok {
		t.Fatalf("Service must be found")
	}
	if got.ServiceTypeHash != updated.ServiceTypeHash {
		t.Fatalf("service type hash not stored")
	}
	if !g.RemoveService(gid) {
		t.Fatalf("RemoveService must report true")
	}
	if g.RemoveService(gid) {
		t.Fatalf("second RemoveService must report false")
	}
}

// sampleParticipantInfo returns a valid participant record.
func sampleParticipantInfo() ParticipantInfo {
	return ParticipantInfo{
		Enclave:       "/",
		NodeName:      "talker",
		NodeNamespace: "/",
		Sequence:      7,
	}
}

// checkGraphParticipantPaths pins participant add plus upsert plus remove.
func checkGraphParticipantPaths(t *testing.T) {
	t.Helper()
	g := NewGraph()
	gid := sampleGID(4)
	info := sampleParticipantInfo()
	err := g.AddParticipant(gid, info)
	if err != nil {
		t.Fatalf("AddParticipant: %v", err)
	}
	if g.ParticipantCount() != 1 {
		t.Fatalf("ParticipantCount = %d, want 1", g.ParticipantCount())
	}
	err = g.AddParticipant(gid, info)
	if err == nil {
		t.Fatalf("duplicate AddParticipant must fail")
	}
	updated := info
	updated.Sequence = 8
	err = g.UpsertParticipant(gid, updated)
	if err != nil {
		t.Fatalf("UpsertParticipant: %v", err)
	}
	got, ok := g.Participant(gid)
	if !ok {
		t.Fatalf("Participant must be found")
	}
	if got.Sequence != 8 {
		t.Fatalf("sequence not stored, got %d", got.Sequence)
	}
	if got.Enclave != "/" {
		t.Fatalf("enclave not stored, got %q", got.Enclave)
	}
	if !g.RemoveParticipant(gid) {
		t.Fatalf("RemoveParticipant must report true")
	}
	if g.RemoveParticipant(gid) {
		t.Fatalf("second RemoveParticipant must report false")
	}
}

// checkGraphGenerationDebounce pins no tick on identical upsert.
func checkGraphGenerationDebounce(t *testing.T) {
	t.Helper()
	g := NewGraph()
	if g.Generation() != 0 {
		t.Fatalf("initial generation must be 0")
	}
	gid := sampleGID(5)
	info := sampleEntityInfo()
	err := g.AddWriter(gid, info)
	if err != nil {
		t.Fatalf("AddWriter: %v", err)
	}
	if g.Generation() != 1 {
		t.Fatalf("generation after add = %d, want 1", g.Generation())
	}
	err = g.UpsertWriter(gid, info)
	if err != nil {
		t.Fatalf("UpsertWriter identical: %v", err)
	}
	if g.Generation() != 1 {
		t.Fatalf("identical upsert must not tick generation, got %d", g.Generation())
	}
	changed := info
	changed.TopicName = "/other"
	err = g.UpsertWriter(gid, changed)
	if err != nil {
		t.Fatalf("UpsertWriter changed: %v", err)
	}
	if g.Generation() != 2 {
		t.Fatalf("changed upsert must tick, got %d", g.Generation())
	}
	if g.RemoveWriter(GID{}) {
		t.Fatalf("remove absent must report false")
	}
	if g.Generation() != 2 {
		t.Fatalf("failed remove must not tick, got %d", g.Generation())
	}
}

// checkGraphHook pins on change callback with generation increment.
func checkGraphHook(t *testing.T) {
	t.Helper()
	g := NewGraph()
	var mu sync.Mutex
	var seen []uint64
	g.SetOnChange(func(gen uint64) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, gen)
	})
	err := g.AddWriter(sampleGID(11), sampleEntityInfo())
	if err != nil {
		t.Fatalf("AddWriter: %v", err)
	}
	err = g.AddReader(sampleGID(12), sampleEntityInfo())
	if err != nil {
		t.Fatalf("AddReader: %v", err)
	}
	err = g.AddService(sampleGID(13), sampleServiceInfo())
	if err != nil {
		t.Fatalf("AddService: %v", err)
	}
	err = g.AddParticipant(sampleGID(14), sampleParticipantInfo())
	if err != nil {
		t.Fatalf("AddParticipant: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 4 {
		t.Fatalf("hook fired %d times, want 4", len(seen))
	}
	for i, gen := range seen {
		if gen != uint64(i+1) {
			t.Fatalf("hook %d got generation %d", i, gen)
		}
	}
	g.SetOnChange(nil)
	if !g.RemoveWriter(sampleGID(11)) {
		t.Fatalf("RemoveWriter must succeed without hook")
	}
}

// checkGraphValidation pins rejection of wire forms and bad hashes.
func checkGraphValidation(t *testing.T) {
	t.Helper()
	g := NewGraph()
	wire := sampleEntityInfo()
	wire.TopicName = "rt/chatter"
	err := g.AddWriter(sampleGID(21), wire)
	if err == nil {
		t.Fatalf("DDS wire topic must be rejected")
	}
	badType := sampleEntityInfo()
	badType.TopicType = "std_msgs::msg::dds_::String_"
	err = g.AddWriter(sampleGID(22), badType)
	if err == nil {
		t.Fatalf("DDS wire type must be rejected")
	}
	badHash := sampleEntityInfo()
	badHash.TypeHash = "bogus"
	err = g.AddWriter(sampleGID(23), badHash)
	if err == nil {
		t.Fatalf("malformed hash must be rejected")
	}
	upper := sampleEntityInfo()
	upper.TypeHash = "RIHS01_DF668C740482BBD48FB39D76A70DFD4BD59DB1288021743503259E948F6B1A18"
	err = g.AddWriter(sampleGID(24), upper)
	if err == nil {
		t.Fatalf("uppercase hash must be rejected")
	}
	badSvc := sampleServiceInfo()
	badSvc.ServiceTypeHash = "bogus"
	err = g.AddService(sampleGID(25), badSvc)
	if err == nil {
		t.Fatalf("malformed service hash must be rejected")
	}
	badPart := sampleParticipantInfo()
	badPart.Enclave = ""
	err = g.AddParticipant(sampleGID(26), badPart)
	if err == nil {
		t.Fatalf("empty enclave must be rejected")
	}
	if g.Generation() != 0 {
		t.Fatalf("failed adds must not tick generation, got %d", g.Generation())
	}
}

// checkGraphConcurrent pins safe concurrent use of the index.
func checkGraphConcurrent(t *testing.T) {
	t.Helper()
	g := NewGraph()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			gid := sampleGID(byte(40 + n)) //nolint:gosec // G115 safe: n is 0..7 so 40+n is 40..47 fits byte
			info := sampleEntityInfo()
			_ = g.UpsertWriter(gid, info) //nolint:errcheck // valid sample info never fails, final drain assertion verifies
			_, _ = g.Writer(gid)
			_ = g.WriterCount()
			_ = g.Writers()
			_ = g.Generation()
			_ = g.RemoveWriter(gid)
		}(i)
	}
	wg.Wait()
	if g.WriterCount() != 0 {
		t.Fatalf("concurrent writers must drain, got %d", g.WriterCount())
	}
}
