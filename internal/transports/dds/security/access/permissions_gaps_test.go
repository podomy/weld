// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package access

import (
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/types"
)

// TestGapsDefaultDenyacc9 pins default-deny for uncovered topics.
func TestGapsDefaultDenyacc9(t *testing.T) {
	t.Parallel()

	doc, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="default-allow" default="ALLOW"><subject_name>CN=x</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before>` +
		`<not_after>2040-01-01T00:00:00Z</not_after></validity>` +
		`<allow_rule><domains><id>0</id></domains><topics><topic>rt/allowed</topic></topics></allow_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	grant := GrantForSubject(doc, "CN=x", time.Now())
	if grant == nil {
		t.Fatalf("grant must resolve")
	}

	if !AllowsEndpoint(grant, 0, "rt/allowed", nil, nil) {
		t.Fatalf("explicit allow must grant")
	}

	if AllowsEndpoint(grant, 0, "rt/other", nil, nil) {
		t.Fatalf("default ALLOW must not grant uncovered topic")
	}

	if ok, _ := AllowsPublishacc8(grant, 0, "rt/other", nil, nil); ok {
		t.Fatalf("directional default ALLOW must not grant uncovered topic")
	}

	if ok, _ := AllowsSubscribeacc8(grant, 0, "rt/other", nil, nil); ok {
		t.Fatalf("directional default ALLOW must not grant uncovered subscribe")
	}
}

// TestGapsGrantSkewacc9 pins clock-skew allowance in validity windows.
func TestGapsGrantSkewacc9(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	before := now.Add(4 * time.Minute).Format("2006-01-02T15:04:05Z")
	after := now.Add(24 * time.Hour).Format("2006-01-02T15:04:05Z")

	doc, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="skew"><subject_name>CN=s</subject_name>` +
		`<validity><not_before>` + before + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><topics><topic>t</topic></topics></allow_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if GrantForSubject(doc, "CN=s", now) == nil {
		t.Fatalf("grant 4min in future must allow within skew")
	}

	farBefore := now.Add(10 * time.Minute).Format("2006-01-02T15:04:05Z")
	farDoc, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="far"><subject_name>CN=s</subject_name>` +
		`<validity><not_before>` + farBefore + `</not_before><not_after>` + after + `</not_after></validity>` +
		`<allow_rule><topics><topic>t</topic></topics></allow_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("parse far: %v", err)
	}

	if GrantForSubject(farDoc, "CN=s", now) != nil {
		t.Fatalf("grant 10min in future must deny outside skew")
	}

	expired := now.Add(-4 * time.Minute).Format("2006-01-02T15:04:05Z")
	pastDoc, err := ParsePermissions([]byte(`<permissions>` +
		`<grant name="past"><subject_name>CN=s</subject_name>` +
		`<validity><not_before>2020-01-01T00:00:00Z</not_before><not_after>` + expired + `</not_after></validity>` +
		`<allow_rule><topics><topic>t</topic></topics></allow_rule>` +
		`</grant></permissions>`))
	if err != nil {
		t.Fatalf("parse past: %v", err)
	}

	if GrantForSubject(pastDoc, "CN=s", now) == nil {
		t.Fatalf("grant expired 4min ago must allow within skew")
	}
}

// TestGapsTypeConsistencyacc9 pins minimal assignability checks.
func TestGapsTypeConsistencyacc9(t *testing.T) {
	t.Parallel()

	if err := CheckTypeConsistencyacc9("Shape", "Shape", false); err != nil {
		t.Fatalf("identical types must assign, got %v", err)
	}

	if err := CheckTypeConsistencyacc9("A", "B", false); err == nil {
		t.Fatalf("mismatched types must deny without coercion")
	}

	if err := CheckTypeConsistencyacc9("A", "B", true); err == nil {
		t.Fatalf("mismatched types must deny even with coercion in minimal check")
	}

	if err := CheckTypeConsistencyacc9("", "B", false); err == nil {
		t.Fatalf("empty reader type must deny")
	}
}

// gapsSeqxt6a builds one int32 sequence descriptor for engine tests.
func gapsSeqxt6a(bound uint32) *types.TypeRefxt6a {
	return &types.TypeRefxt6a{
		Kind:  types.Kindxt6aSequence,
		Bound: bound,
		Elem:  &types.TypeRefxt6a{Kind: types.Kindxt6aPrimitive, Base: types.FieldTypeInt32},
	}
}

// gapsStructxt6a builds one appendable struct descriptor for engine tests.
func gapsStructxt6a(second string) *types.TypeRefxt6a {
	return &types.TypeRefxt6a{
		Kind: types.Kindxt6aStruct,
		Struct: &types.Structxt6aShape{Ext: types.ExtAppendable, Members: []types.Structxt6aMember{
			{Name: "x", MemberID: 0, Type: types.FieldTypeInt32},
			{Name: second, MemberID: 1, Type: types.FieldTypeInt32},
		}},
	}
}

// checkGapsConsistencyNames pins the step 2 name fallback fail-closed.
func checkGapsConsistencyNames(t *testing.T) {
	t.Helper()

	allow := types.Defaultxt6aConsistency()

	err := CheckTypeConsistencyWithPolicy(allow, "Shape", "Shape", nil, nil)
	if err != nil {
		t.Fatalf("identical names must assign, got %v", err)
	}

	err = CheckTypeConsistencyWithPolicy(allow, "A", "B", nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("mismatched names must deny fail-closed even when coercion is allowed, got %v", err)
	}

	disallow := types.TypeConsistencyxt6aPolicy{Kind: types.Consistencyxt6aDisallowCoercion}

	err = CheckTypeConsistencyWithPolicy(disallow, "A", "B", nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("mismatched names must deny without coercion, got %v", err)
	}

	err = CheckTypeConsistencyWithPolicy(disallow, "Shape", "Shape", nil, nil)
	if err != nil {
		t.Fatalf("identical names must assign without coercion, got %v", err)
	}

	forced := allow
	forced.ForceTypeValidation = true

	err = CheckTypeConsistencyWithPolicy(forced, "Shape", "Shape", nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("forced validation without type info must deny, got %v", err)
	}

	err = CheckTypeConsistencyWithPolicy(allow, "", "Shape", nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("empty reader name must deny, got %v", err)
	}
}

// checkGapsConsistencyDescriptors pins exact versus minimal assignability.
func checkGapsConsistencyDescriptors(t *testing.T) {
	t.Helper()

	allow := types.Defaultxt6aConsistency()
	disallow := types.TypeConsistencyxt6aPolicy{Kind: types.Consistencyxt6aDisallowCoercion}

	err := CheckTypeConsistencyWithPolicy(allow, "Seq10", "Seq5", gapsSeqxt6a(10), gapsSeqxt6a(5))
	if err != nil {
		t.Fatalf("minimal widened reader rejected: %v", err)
	}

	err = CheckTypeConsistencyWithPolicy(disallow, "Seq10", "Seq5", gapsSeqxt6a(10), gapsSeqxt6a(5))
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("exact widened sequence must deny, got %v", err)
	}

	err = CheckTypeConsistencyWithPolicy(allow, "Seq5", "Seq10", gapsSeqxt6a(5), gapsSeqxt6a(10))
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("minimal narrowed reader must deny, got %v", err)
	}

	relaxed := allow
	relaxed.IgnoreSequenceBounds = true

	err = CheckTypeConsistencyWithPolicy(relaxed, "Seq5", "Seq10", gapsSeqxt6a(5), gapsSeqxt6a(10))
	if err != nil {
		t.Fatalf("minimal ignored sequence bounds rejected: %v", err)
	}

	plain := &types.TypeRefxt6a{Kind: types.Kindxt6aPrimitive, Base: types.FieldTypeInt32}
	wide := &types.TypeRefxt6a{Kind: types.Kindxt6aPrimitive, Base: types.FieldTypeInt64}

	err = CheckTypeConsistencyWithPolicy(allow, "I32", "I64", plain, wide)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("primitive mismatch must deny, got %v", err)
	}

	err = CheckTypeConsistencyWithPolicy(disallow, "S", "S", gapsStructxt6a("y"), gapsStructxt6a("y"))
	if err != nil {
		t.Fatalf("exact identical structs rejected: %v", err)
	}

	err = CheckTypeConsistencyWithPolicy(disallow, "S", "S", gapsStructxt6a("z"), gapsStructxt6a("y"))
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("exact renamed struct must deny, got %v", err)
	}

	err = CheckTypeConsistencyWithPolicy(allow, "Seq10", "Shape", gapsSeqxt6a(10), nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("one-sided descriptor with mismatched names must deny, got %v", err)
	}
}

// checkGapsConsistencyQoS pins QoS to engine policy conversion.
func checkGapsConsistencyQoS(t *testing.T) {
	t.Helper()

	converted := QoSConsistencyPolicy(core.DefaultTypeConsistencyEnforcement())
	if converted.Kind != types.Consistencyxt6aAllowCoercion {
		t.Fatalf("default QoS must convert to allow, got %v", converted.Kind)
	}

	enforce := core.TypeConsistencyEnforcementQosPolicy{
		Kind:                 core.TypeConsistencyDisallowCoercion,
		IgnoreSequenceBounds: true,
		IgnoreStringBounds:   true,
		IgnoreMemberNames:    true,
		IgnoreLiteralNames:   true,
		PreventTypeWidening:  true,
		ForceTypeValidation:  true,
	}

	strict := QoSConsistencyPolicy(enforce)
	if strict.Kind != types.Consistencyxt6aDisallowCoercion {
		t.Fatalf("disallow QoS must convert to disallow, got %v", strict.Kind)
	}

	if !strict.IgnoreSequenceBounds || !strict.IgnoreStringBounds || !strict.IgnoreMemberNames ||
		!strict.IgnoreLiteralNames || !strict.PreventTypeWidening || !strict.ForceTypeValidation {
		t.Fatalf("QoS flags must carry over, got %+v", strict)
	}

	err := CheckTypeConsistencyWithPolicy(converted, "Shape", "Shape", nil, nil)
	if err != nil {
		t.Fatalf("converted default must assign identical names, got %v", err)
	}
}

// checkGapsConsistencyThreading pins type gating ahead of the skeletons.
func checkGapsConsistencyThreading(t *testing.T) {
	t.Helper()

	allow := types.Defaultxt6aConsistency()
	tolerate := NewController(false)

	err := tolerate.CheckLocalDataWriterMatchWithTypes("local", "remote", "t", allow, "T", "T", nil, nil)
	if err != nil {
		t.Fatalf("tolerate consistent writer must allow, got %v", err)
	}

	err = tolerate.CheckLocalDataWriterMatchWithTypes("local", "remote", "t", allow, "A", "B", nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("tolerate inconsistent writer must deny fail-closed, got %v", err)
	}

	err = tolerate.CheckLocalDataReaderMatchWithTypes("local", "remote", "t", allow, "T", "T", gapsSeqxt6a(10), gapsSeqxt6a(5))
	if err != nil {
		t.Fatalf("tolerate consistent reader must allow, got %v", err)
	}

	err = tolerate.CheckRemoteDataWriterWithTypes("peer", "t", allow, "A", "B", nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("tolerate inconsistent remote writer must deny, got %v", err)
	}

	err = tolerate.CheckRemoteDataReaderWithTypes("peer", "t", allow, "T", "T", nil, nil)
	if err != nil {
		t.Fatalf("tolerate consistent remote reader must allow, got %v", err)
	}

	enforced := NewController(true)

	err = enforced.CheckLocalDataWriterMatchWithTypes("local", "remote", "t", allow, "T", "T", nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("enabled consistent writer must deny on skeleton, got %v", err)
	}

	err = enforced.CheckLocalDataReaderMatchWithTypes("local", "remote", "t", allow, "A", "B", nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("enabled inconsistent reader must deny, got %v", err)
	}

	err = enforced.CheckRemoteDataWriterWithTypes("peer", "t", allow, "A", "B", nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("enabled inconsistent remote writer must deny, got %v", err)
	}

	err = enforced.CheckRemoteDataReaderWithTypes("peer", "t", allow, "T", "T", nil, nil)
	if !errors.Is(err, ErrNotAllowedBySecurity) {
		t.Fatalf("enabled consistent remote reader must deny on skeleton, got %v", err)
	}
}

// TestGapsTypeConsistencyEngineacc9 pins 7.2.5 assignability with 7.6.3.4 enforcement.
func TestGapsTypeConsistencyEngineacc9(t *testing.T) {
	t.Parallel()

	t.Run("name fallback", checkGapsConsistencyNames)
	t.Run("descriptors", checkGapsConsistencyDescriptors)
	t.Run("qos conversion", checkGapsConsistencyQoS)
	t.Run("match threading", checkGapsConsistencyThreading)
}
