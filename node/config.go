// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"fmt"

	"github.com/podomy/weld/internal/core"
)

// ConfigStore is a versioned parameter store with schema, validation, atomic
// batches, watches, and bounded history. The zero value is unusable; build
// with NewConfigStore. Do not copy after first use.
type ConfigStore = core.ConfigStore

// ConfigStoreOptions selects store behavior at creation. Remap tables are
// fixed here and cannot update after creation.
type ConfigStoreOptions = core.ConfigStoreOptions

// RemapRule rewrites one parameter name. Global remaps apply before local
// remaps; first match wins.
type RemapRule = core.RemapRule

// ParamValue is one typed parameter value. The zero value carries
// ParamTypeNotSet. Slices are owned: the store clones on entry and on exit.
type ParamValue = core.ParamValue

// ParamType names the value kind held by a ParamValue.
type ParamType = core.ParamType

const (
	// ParamTypeNotSet means no value.
	ParamTypeNotSet = core.ParamTypeNotSet
	// ParamTypeBool holds a boolean.
	ParamTypeBool = core.ParamTypeBool
	// ParamTypeInteger holds an int64.
	ParamTypeInteger = core.ParamTypeInteger
	// ParamTypeDouble holds a float64.
	ParamTypeDouble = core.ParamTypeDouble
	// ParamTypeString holds a string.
	ParamTypeString = core.ParamTypeString
	// ParamTypeByteArray holds opaque bytes.
	ParamTypeByteArray = core.ParamTypeByteArray
	// ParamTypeBoolArray holds booleans with uniform typing.
	ParamTypeBoolArray = core.ParamTypeBoolArray
	// ParamTypeIntegerArray holds int64 values with uniform typing.
	ParamTypeIntegerArray = core.ParamTypeIntegerArray
	// ParamTypeDoubleArray holds float64 values with uniform typing.
	ParamTypeDoubleArray = core.ParamTypeDoubleArray
	// ParamTypeStringArray holds strings with uniform typing.
	ParamTypeStringArray = core.ParamTypeStringArray
)

// ParamDescriptor describes one parameter: its type, documentation, and
// optional numeric range.
type ParamDescriptor = core.ParamDescriptor

// ParamSchema maps parameter names to their descriptors for pre-launch
// validation. Keys are final post-remap dotted names.
type ParamSchema = core.ParamSchema

// NewBoolParam returns a boolean parameter value.
func NewBoolParam(v bool) ParamValue {
	return core.NewBoolParam(v)
}

// NewIntegerParam returns an integer parameter value.
func NewIntegerParam(v int64) ParamValue {
	return core.NewIntegerParam(v)
}

// NewDoubleParam returns a double parameter value.
func NewDoubleParam(v float64) ParamValue {
	return core.NewDoubleParam(v)
}

// NewStringParam returns a string parameter value.
func NewStringParam(v string) ParamValue {
	return core.NewStringParam(v)
}

// NewByteArrayParam returns a byte array parameter value.
func NewByteArrayParam(v []byte) ParamValue {
	return core.NewByteArrayParam(v)
}

// NewBoolArrayParam returns a boolean array parameter value.
func NewBoolArrayParam(v []bool) ParamValue {
	return core.NewBoolArrayParam(v)
}

// NewIntegerArrayParam returns an integer array parameter value.
func NewIntegerArrayParam(v []int64) ParamValue {
	return core.NewIntegerArrayParam(v)
}

// NewDoubleArrayParam returns a double array parameter value.
func NewDoubleArrayParam(v []float64) ParamValue {
	return core.NewDoubleArrayParam(v)
}

// NewStringArrayParam returns a string array parameter value.
func NewStringArrayParam(v []string) ParamValue {
	return core.NewStringArrayParam(v)
}

// NewConfigStore returns an empty store with fixed remap tables. Rules are
// validated eagerly; an invalid rule fails creation. Tables are copied so
// caller mutation cannot rewrite them.
func NewConfigStore(opts ConfigStoreOptions) (*ConfigStore, error) {
	store, err := core.NewConfigStore(opts)
	if err != nil {
		return nil, fmt.Errorf("node: new config store: %w", err)
	}
	return store, nil
}

// ConfigApplyHook runs after a validated batch commits. See core.ConfigApplyHook.
type ConfigApplyHook = core.ConfigApplyHook

// ConfigChange is one applied parameter delta. See core.ConfigChange.
type ConfigChange = core.ConfigChange

// ConfigDumpEntry is one stored parameter with its descriptor. See core.ConfigDumpEntry.
type ConfigDumpEntry = core.ConfigDumpEntry

// ConfigEvent is one committed batch with its changes. See core.ConfigEvent.
type ConfigEvent = core.ConfigEvent

// ConfigHistoryEntry is one versioned parameter record. See core.ConfigHistoryEntry.
type ConfigHistoryEntry = core.ConfigHistoryEntry

// ConfigValidator validates a pending set and may reject it. See core.ConfigValidator.
type ConfigValidator = core.ConfigValidator

// ApplyRemap rewrites name through the creation-time table.
func ApplyRemap(name string, global, local []RemapRule) string {
	return core.ApplyRemap(name, global, local)
}

// CollapseParamFiles merges wildcard and node-specific file sections.
func CollapseParamFiles(files map[string]map[string]ParamValue, nodeFQN string) map[string]ParamValue {
	return core.CollapseParamFiles(files, nodeFQN)
}

// CollapseParamFilesChecked merges file sections and reports duplicate keys loudly.
func CollapseParamFilesChecked(files map[string]map[string]ParamValue, nodeFQN string) (map[string]ParamValue, error) {
	merged, err := core.CollapseParamFilesChecked(files, nodeFQN)
	if err != nil {
		return nil, fmt.Errorf("node: collapse param files: %w", err)
	}
	return merged, nil
}

// ExpandNodeScopedParam expands a relative parameter name against a namespace.
func ExpandNodeScopedParam(name, namespace string) (string, error) {
	expanded, err := core.ExpandNodeScopedParam(name, namespace)
	if err != nil {
		return "", fmt.Errorf("node: expand node scoped param: %w", err)
	}
	return expanded, nil
}

// FlattenNestedParams flattens nested YAML-like maps to dotted parameter names.
func FlattenNestedParams(nested map[string]any, prefix string) (map[string]ParamValue, error) {
	flat, err := core.FlattenNestedParams(nested, prefix)
	if err != nil {
		return nil, fmt.Errorf("node: flatten nested params: %w", err)
	}
	return flat, nil
}

// ParamValueFromAny converts a decoded YAML-like scalar or uniform array to a ParamValue.
func ParamValueFromAny(v any) (ParamValue, error) {
	value, err := core.ParamValueFromAny(v)
	if err != nil {
		return ParamValue{}, fmt.Errorf("node: param value from any: %w", err)
	}
	return value, nil
}

// ParseOverrideLiteral parses one YAML scalar or array literal for -p overrides.
func ParseOverrideLiteral(text string) (ParamValue, error) {
	value, err := core.ParseOverrideLiteral(text)
	if err != nil {
		return ParamValue{}, fmt.Errorf("node: parse override literal: %w", err)
	}
	return value, nil
}

// ParseParamOverride splits name:=literal for -p overrides.
func ParseParamOverride(text string) (string, ParamValue, error) {
	name, value, err := core.ParseParamOverride(text)
	if err != nil {
		return "", ParamValue{}, fmt.Errorf("node: parse param override: %w", err)
	}
	return name, value, nil
}

// ValidateParamName checks dotted parameter name shape.
func ValidateParamName(name string) error {
	err := core.ValidateParamName(name)
	if err != nil {
		return fmt.Errorf("node: validate param name: %w", err)
	}
	return nil
}

// ValidateRemapRule checks that both ends are valid parameter names.
func ValidateRemapRule(r RemapRule) error {
	err := core.ValidateRemapRule(r)
	if err != nil {
		return fmt.Errorf("node: validate remap rule: %w", err)
	}
	return nil
}
