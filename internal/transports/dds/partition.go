// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package dds holds the CycloneDDS sidecar contract.
//
// This file implements DDS partition QoS for discovery: name validation,
// POSIX fnmatch style wildcard matching, the PID_PARTITION parameter codec,
// the partition-aware match outcome, and the partition-aware discovery key.
// Weld currently announces no partition QoS (fixed userdata enclave, no
// partition params), so without this matching a peer on another partition
// would either leak traffic in or starve traffic out. Everything here is
// pure: no socket, no index, no matcher edits. Wiring into SPDP announce,
// SEDP params, the matcher, and backend route keys is listed under the
// INTEGRATION notes at the end of this file.
package dds

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// Partition QoS wire and validation limits.
//
// PIDPartition is the DDSI-RTPS PID_PARTITION value from Table 9.9 of the
// spec: the parameter id for the PartitionQosPolicy sequence<string>. It
// lives here instead of rtps/pid.go because that table is frozen for this
// change; see the sedp.go INTEGRATION note for the merge step.
// MaxPartitionCount caps one partition list at 64 names and
// MaxPartitionChars caps the summed name bytes at 256, matching the RTI
// Connext DDS_PartitionQosPolicy resource limits (up to 64 names, at most
// 256 characters summed across all names). MaxPartitionNameLen caps one
// name at 256 bytes so a single name can never exceed the cumulative
// budget on its own.
const (
	// PIDPartition carries the partition name sequence<string>.
	PIDPartition uint16 = 0x0029
	// MaxPartitionCount caps names in one partition list.
	MaxPartitionCount = 64
	// MaxPartitionNameLen caps bytes in one partition name.
	MaxPartitionNameLen = 256
	// MaxPartitionChars caps summed name bytes in one partition list.
	MaxPartitionChars = 256
)

// ErrInvalidPartition reports a malformed partition name or list.
var ErrInvalidPartition = errors.New("dds: invalid partition")

// PartitionMismatchReason names why a partition match failed.
type PartitionMismatchReason string

// Partition match outcomes: none means matched.
const (
	// PartitionMismatchNone means offered and requested share a partition.
	PartitionMismatchNone PartitionMismatchReason = ""
	// PartitionMismatchNoCommonPartition means no requested name matches any offered name.
	PartitionMismatchNoCommonPartition PartitionMismatchReason = "no_common_partition"
)

// PartitionMatchOutcome is the partition-aware match result.
//
// Matched is true when the reader and writer share a partition. Reason is
// PartitionMismatchNone when matched and names the failure otherwise, so a
// later matcher hook can report partitions without re-deriving the cause.
// Detail carries the normalized lists for logs.
type PartitionMatchOutcome struct {
	// Matched is true when a requested name matches an offered name.
	Matched bool
	// Reason names the mismatch, empty when matched.
	Reason PartitionMismatchReason
	// Detail lists the normalized offered and requested names.
	Detail string
}

// ValidatePartitionName checks one DDS partition name.
//
// The empty string is valid: it names the default partition, and an empty
// list normalizes to it. Otherwise every byte must be printable ASCII
// (0x20 through 0x7E) except comma: NUL terminates CDR strings, controls,
// DEL, and high bytes break logs and cross-vendor comparison, and comma is
// reserved by RTI Connext partition rules and collides with comma-joined
// tooling. Wildcards * ? [ ] and backslash stay allowed so patterns pass
// validation; only * and ? act as wildcards in matching, brackets and
// backslash match literally. Length is bytes, capped at
// MaxPartitionNameLen.
func ValidatePartitionName(name string) error {
	if len(name) > MaxPartitionNameLen {
		return fmt.Errorf("dds: partition name %q longer than %d bytes: %w", name, MaxPartitionNameLen, ErrInvalidPartition)
	}

	for i := range len(name) {
		c := name[i]
		if c == ',' {
			return fmt.Errorf("dds: partition name %q carries reserved comma: %w", name, ErrInvalidPartition)
		}
		if c < 0x20 || c > 0x7E {
			return fmt.Errorf("dds: partition name %q has byte %#02x outside printable ASCII: %w", name, c, ErrInvalidPartition)
		}
	}

	return nil
}

// ValidatePartitionList checks a full offered or requested partition list.
func ValidatePartitionList(names []string) error {
	if len(names) > MaxPartitionCount {
		return fmt.Errorf("dds: partition list holds %d names above %d: %w", len(names), MaxPartitionCount, ErrInvalidPartition)
	}

	total := 0
	for _, name := range names {
		err := ValidatePartitionName(name)
		if err != nil {
			return err
		}
		total += len(name)
	}

	if total > MaxPartitionChars {
		return fmt.Errorf("dds: partition list sums %d chars above %d: %w", total, MaxPartitionChars, ErrInvalidPartition)
	}

	return nil
}

// PartitionPatternMatch reports whether fnmatch pattern matches name.
//
// Only * (any byte sequence including empty) and ? (exactly one byte) are
// wildcards, per the POSIX fnmatch subset the DDS spec cites for partition
// names. Brackets and backslash are literal. Matching is case sensitive:
// partition names are case sensitive per RTI Connext partition rules.
func PartitionPatternMatch(pattern, name string) bool {
	return partitionGlob([]byte(pattern), []byte(name))
}

// partitionGlob matches glob pattern against name with * and ? wildcards.
func partitionGlob(pattern, name []byte) bool {
	px, nx := 0, 0
	star, mark := -1, 0

	for nx < len(name) {
		if px < len(pattern) && (pattern[px] == '?' || pattern[px] == name[nx]) {
			px++
			nx++
			continue
		}
		if px < len(pattern) && pattern[px] == '*' {
			star = px
			mark = nx
			px++
			continue
		}
		if star >= 0 {
			px = star + 1
			mark++
			nx = mark
			continue
		}
		return false
	}

	for px < len(pattern) && pattern[px] == '*' {
		px++
	}

	return px == len(pattern)
}

// normalizePartitions maps an empty list to the default partition.
//
// An empty partition list is equivalent to a list holding the single empty
// string per the DDS spec and the Fast-DDS partition rules, so endpoints
// that announce nothing meet only on the default partition.
func normalizePartitions(names []string) []string {
	if len(names) == 0 {
		return []string{""}
	}

	return append([]string(nil), names...)
}

// partitionPairMatches reports whether two names share a partition.
//
// Either side may hold the pattern: the check runs both ways like
// Fast-DDS, so offered "part*" meets requested "partition*" and requested
// "sensor_*" meets offered "sensor_data". With no wildcards on either side
// this reduces to byte equality. A bare "*" matches the default partition
// "" because it matches the empty sequence; Fast-DDS 3.x changed this to
// exclude default, Weld keeps POSIX fnmatch semantics.
func partitionPairMatches(a, b string) bool {
	return PartitionPatternMatch(a, b) || PartitionPatternMatch(b, a)
}

// PartitionsMatch reports whether requested and offered lists share one.
//
// Both lists normalize empty to the default partition first, then any
// requested name matching any offered name (either direction for patterns)
// is a match. Failure to match partitions is not an incompatible QoS per
// the DDS spec: endpoints stay unmatched with no listener, which is why the
// named outcome below exists instead of an error.
func PartitionsMatch(requested, offered []string) bool {
	return MatchPartitions(offered, requested).Matched
}

// MatchPartitions computes the partition-aware match outcome.
//
// offered is the writer list and requested is the reader list, matching the
// CheckCompatibility offered then requested order in internal/core
// matcher.go. Either list may be nil or empty for the default partition.
// The result is pure: it validates nothing and treats every element as
// opaque bytes, so callers validate at declare and parse time.
func MatchPartitions(offered, requested []string) PartitionMatchOutcome {
	want := normalizePartitions(requested)
	have := normalizePartitions(offered)

	for _, r := range want {
		for _, o := range have {
			if partitionPairMatches(r, o) {
				return PartitionMatchOutcome{Matched: true, Reason: PartitionMismatchNone}
			}
		}
	}

	return PartitionMatchOutcome{
		Matched: false,
		Reason:  PartitionMismatchNoCommonPartition,
		Detail:  "requested=" + partitionListString(want) + " offered=" + partitionListString(have),
	}
}

// partitionListString renders a name list for the mismatch detail.
func partitionListString(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, "\""+name+"\"")
	}

	return "[" + strings.Join(quoted, ",") + "]"
}

// EncodePartitionQoS renders a partition list as a PID_PARTITION value.
//
// Layout is little endian CDR sequence<string> matching the CycloneDDS
// per-string convention used for TOPIC_NAME in sedp.go: a 4 byte name
// count, then per name a 4 byte length including the NUL terminator plus
// zero pad to 4 bytes, the bytes, the NUL, and the pad. The stored length
// therefore always has mod 4 == 0, which Cyclone quickscan requires. An
// empty or nil list encodes as a zero count for the default partition.
func EncodePartitionQoS(names []string) ([]byte, error) {
	err := ValidatePartitionList(names)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 0, 4+len(names)*8)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(names))) //nolint:gosec // G115: count capped by MaxPartitionCount.

	for _, name := range names {
		out = appendPartitionString(out, name)
	}

	return out, nil
}

// appendPartitionString appends one CDR string with Cyclone pad-in-length.
func appendPartitionString(out []byte, name string) []byte {
	raw := len(name) + 1
	pad := (4 - raw%4) % 4
	out = binary.LittleEndian.AppendUint32(out, uint32(raw+pad)) //nolint:gosec // G115: names capped by MaxPartitionNameLen.
	out = append(out, name...)
	out = append(out, 0x00)
	for range pad {
		out = append(out, 0x00)
	}

	return out
}

// DecodePartitionQoS parses a PID_PARTITION value back into names.
//
// It reads the little endian count then each CDR string, tolerating both
// Cyclone pad-in-length and standard excludes-pad lengths like the SEDP
// param decoder does: the value is consumed to the next 4 byte boundary
// either way. A zero count decodes to an empty list for the default
// partition. Counts above MaxPartitionCount, short buffers, missing NUL
// terminators, and names failing ValidatePartitionName all fail.
func DecodePartitionQoS(value []byte) ([]string, error) {
	if len(value) < 4 {
		return nil, fmt.Errorf("dds: partition value need 4 have %d: %w", len(value), ErrInvalidPartition)
	}

	count := int(binary.LittleEndian.Uint32(value[:4]))
	if count < 0 || count > MaxPartitionCount {
		return nil, fmt.Errorf("dds: partition count %d above %d: %w", count, MaxPartitionCount, ErrInvalidPartition)
	}

	out := make([]string, 0, count)
	off := 4

	for range count {
		name, next, err := decodePartitionString(value, off)
		if err != nil {
			return nil, err
		}
		out = append(out, name)
		off = next
	}

	if off != len(value) {
		return nil, fmt.Errorf("dds: partition value has %d trailing bytes: %w", len(value)-off, ErrInvalidPartition)
	}

	err := ValidatePartitionList(out)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// decodePartitionString reads one CDR string at off and returns the next offset.
func decodePartitionString(value []byte, off int) (string, int, error) {
	if len(value)-off < 4 {
		return "", 0, fmt.Errorf("dds: partition string need 4 have %d: %w", len(value)-off, ErrInvalidPartition)
	}

	length := int(binary.LittleEndian.Uint32(value[off : off+4]))
	if length < 1 {
		return "", 0, fmt.Errorf("dds: partition string length %d misses NUL: %w", length, ErrInvalidPartition)
	}
	if length > MaxPartitionNameLen+4 {
		return "", 0, fmt.Errorf("dds: partition string length %d above cap: %w", length, ErrInvalidPartition)
	}
	if len(value)-off-4 < length {
		return "", 0, fmt.Errorf("dds: partition string need %d have %d: %w", length, len(value)-off-4, ErrInvalidPartition)
	}

	raw := value[off+4 : off+4+length]
	nul := -1
	for i, b := range raw {
		if b == 0x00 {
			nul = i
			break
		}
	}
	if nul < 0 {
		return "", 0, fmt.Errorf("dds: partition string misses NUL terminator: %w", ErrInvalidPartition)
	}

	name := string(raw[:nul])
	err := ValidatePartitionName(name)
	if err != nil {
		return "", 0, err
	}

	next := off + 4 + length + (4-length%4)%4
	if next > len(value) {
		return "", 0, fmt.Errorf("dds: partition string pad overruns value: %w", ErrInvalidPartition)
	}

	return name, next, nil
}

// PartitionParam wraps an encoded partition list as an RTPS param.
//
// The returned param carries PIDPartition so callers can append it to SEDP
// inline or payload lists with rtps.EncodeParams. See the sedp.go
// INTEGRATION note for the native SerializeSEDPDATA hook.
func PartitionParam(names []string) (rtps.Param, error) {
	value, err := EncodePartitionQoS(names)
	if err != nil {
		return rtps.Param{}, err
	}

	return rtps.Param{ID: PIDPartition, Value: value}, nil
}

// FindPartitionParam returns the partition list from decoded params.
//
// It returns the first PIDPartition entry decoded with DecodePartitionQoS,
// or nil when no entry is present (the default partition). Later
// duplicates are ignored: identity stays first-wins like the SEDP
// endpoint parser.
func FindPartitionParam(params []rtps.Param) ([]string, error) {
	for _, p := range params {
		if p.ID != PIDPartition {
			continue
		}

		names, err := DecodePartitionQoS(p.Value)
		if err != nil {
			return nil, fmt.Errorf("dds: partition param decode: %w", err)
		}

		return names, nil
	}

	return nil, nil
}

// PartitionRouteKey returns the effective discovery key for topic plus partitions.
//
// The key canonicalizes the partition set (empty maps to default, sorted,
// deduplicated) and joins it to the DDS topic with NUL separators, which
// neither DDS topics nor validated partition names can hold. The same topic
// on different partitions therefore never shares a key, and the same set in
// different orders shares one. Backend route and data maps keyed by this
// string stay isolated across partitions; see the backend.go INTEGRATION
// note for the wiring step.
func PartitionRouteKey(ddsTopic string, partitions []string) string {
	names := normalizePartitions(partitions)
	sorted := append([]string(nil), names...)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)

	return ddsTopic + "\x00" + strings.Join(sorted, "\x00")
}

// INTEGRATION hooks needing new signatures in existing files (read only here):
//
// INTEGRATION: spdp.go: announce participant partitions with
//   MarshalAlivePacketWithPartitions(guid GUID, domain uint32, seq uint64, lease time.Duration, partitions []string)
//   carrying a participant-level PIDPartition param, and extend Announcement with Partitions []string parsed back.
// INTEGRATION: sedp.go: add Partitions []string to SEDPEndpoint, append
//   PartitionParam(ep.Partitions) in SerializeSEDPDATA, decode PID_PARTITION
//   via FindPartitionParam in sedpEndpointFields, and validate with
//   ValidatePartitionList in sedpValidateRemote and sedpValidateMarshalEndpoint.
// INTEGRATION: matcher.go: gate SEDPEndpointsMatch on
//   MatchPartitions(writer.Partitions, reader.Partitions) so partition
//   mismatch stays a quiet non-match (no_common_partition) instead of an
//   RxO error, per the DDS rule that partition failure is not incompatible QoS.
// INTEGRATION: backend.go: key discovery routes and DATA demux by
//   PartitionRouteKey(ddsTopic, partitions) so the same DDS topic on
//   different partitions never shares endpoints, routes, or retention.
