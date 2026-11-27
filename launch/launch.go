// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package launch is the public launch argument surface.
package launch

import (
	"fmt"

	"github.com/podomy/weld/internal/core"
	ilaunch "github.com/podomy/weld/internal/launch"
)

// ROSArgs is the parsed result of one --ros-args section.
type ROSArgs = ilaunch.ROSArgs

// NodeOptions holds one node launch options before startup.
type NodeOptions = ilaunch.NodeOptions

// LaunchPlan holds one bringup with global remaps plus per node options.
type LaunchPlan = ilaunch.LaunchPlan

// RemapRule rewrites one graph name to another at creation time.
type RemapRule = ilaunch.RemapRule

// ParamValue is one typed parameter value shared with core.
type ParamValue = core.ParamValue

// Launch parameter names and environment keys.
const (
	// SimTimeParam is the parameter forced true when simulation time is used.
	SimTimeParam = ilaunch.SimTimeParam
	// AutostartParam is the lifecycle autostart parameter forced by rewrites.
	AutostartParam = ilaunch.AutostartParam
	// ROSLogDirEnv names the environment variable redirecting logs.
	ROSLogDirEnv = ilaunch.ROSLogDirEnv
)

// ParseROSArgs parses one --ros-args section with last wins ordering.
func ParseROSArgs(argv []string) (ROSArgs, error) {
	out, err := ilaunch.ParseROSArgs(argv)
	if err != nil {
		return ROSArgs{}, fmt.Errorf("launch: parse ros args: %w", err)
	}

	return out, nil
}

// ForwardArgs appends CLI argv to node options.
func ForwardArgs(base NodeOptions, argv []string) (NodeOptions, error) {
	out, err := ilaunch.ForwardArgs(base, argv)
	if err != nil {
		return NodeOptions{}, fmt.Errorf("launch: forward args: %w", err)
	}

	return out, nil
}

// AppendSDFParams appends an SDF params file with sim time forced on.
func AppendSDFParams(opts NodeOptions, sdfPath string) (NodeOptions, error) {
	out, err := ilaunch.AppendSDFParams(opts, sdfPath)
	if err != nil {
		return NodeOptions{}, fmt.Errorf("launch: append sdf params: %w", err)
	}

	return out, nil
}

// NewNodeOptions returns validated options with empty maps.
func NewNodeOptions(node string, namespace string, allowEmpty bool) (NodeOptions, error) {
	out, err := ilaunch.NewNodeOptions(node, namespace, allowEmpty)
	if err != nil {
		return NodeOptions{}, fmt.Errorf("launch: new node options: %w", err)
	}

	return out, nil
}

// ValidateNodeName checks one base node name.
func ValidateNodeName(name string) error {
	err := ilaunch.ValidateNodeName(name)
	if err != nil {
		return fmt.Errorf("launch: validate node name: %w", err)
	}

	return nil
}

// ValidateRemapName checks one graph name for remap rules.
func ValidateRemapName(name string) error {
	err := ilaunch.ValidateRemapName(name)
	if err != nil {
		return fmt.Errorf("launch: validate remap name: %w", err)
	}

	return nil
}

// ValidateRemapRule checks both ends of one graph remap rule.
func ValidateRemapRule(r RemapRule) error {
	err := ilaunch.ValidateRemapRule(r)
	if err != nil {
		return fmt.Errorf("launch: validate remap rule: %w", err)
	}

	return nil
}

// NormalizeNamespace returns the absolute form of ns.
func NormalizeNamespace(ns string) (string, error) {
	out, err := ilaunch.NormalizeNamespace(ns)
	if err != nil {
		return "", fmt.Errorf("launch: normalize namespace: %w", err)
	}

	return out, nil
}

// ResolveNodeFQN returns the fully qualified node name.
func ResolveNodeFQN(node string, namespace string) (string, error) {
	out, err := ilaunch.ResolveNodeFQN(node, namespace)
	if err != nil {
		return "", fmt.Errorf("launch: resolve node fqn: %w", err)
	}

	return out, nil
}

// ApplyNamespacePush joins a base namespace with a push fragment.
func ApplyNamespacePush(base string, push string) (string, error) {
	out, err := ilaunch.ApplyNamespacePush(base, push)
	if err != nil {
		return "", fmt.Errorf("launch: apply namespace push: %w", err)
	}

	return out, nil
}

// ResolveRemap rewrites name through the creation time table.
func ResolveRemap(name string, global []RemapRule, local []RemapRule) (string, error) {
	out, err := ilaunch.ResolveRemap(name, global, local)
	if err != nil {
		return "", fmt.Errorf("launch: resolve remap: %w", err)
	}

	return out, nil
}

// ResolvePlanFQNs validates a launch plan and returns node FQNs in order.
func ResolvePlanFQNs(plan LaunchPlan) ([]string, error) {
	out, err := ilaunch.ResolvePlanFQNs(plan)
	if err != nil {
		return nil, fmt.Errorf("launch: resolve plan fqns: %w", err)
	}

	return out, nil
}

// ResolveLogDir returns ROS_LOG_DIR when set and non blank.
func ResolveLogDir() (string, bool) {
	return ilaunch.ResolveLogDir()
}

// LoadParamsFile reads one YAML params file with substitutions evaluated.
func LoadParamsFile(path string) (map[string]map[string]ParamValue, error) {
	out, err := ilaunch.LoadParamsFile(path)
	if err != nil {
		return nil, fmt.Errorf("launch: load params file: %w", err)
	}

	return out, nil
}

// ParseParamsContent parses params YAML with dotted flattening.
func ParseParamsContent(content []byte) (map[string]map[string]ParamValue, error) {
	out, err := ilaunch.ParseParamsContent(content)
	if err != nil {
		return nil, fmt.Errorf("launch: parse params content: %w", err)
	}

	return out, nil
}

// MergeFileSections merges file sections in order with later files winning.
func MergeFileSections(all []map[string]map[string]ParamValue) map[string]map[string]ParamValue {
	return ilaunch.MergeFileSections(all)
}

// ResolveNodeParams merges files plus CLI plus code for one node.
func ResolveNodeParams(nodeFQN string, files map[string]map[string]ParamValue, cli map[string]ParamValue, code map[string]ParamValue) map[string]ParamValue {
	return ilaunch.ResolveNodeParams(nodeFQN, files, cli, code)
}

// RewriteParamsForNamespace applies per namespace rewrites plus autostart.
func RewriteParamsForNamespace(params map[string]ParamValue, namespace string, rewrites map[string]string, autostart bool) (map[string]ParamValue, error) {
	out, err := ilaunch.RewriteParamsForNamespace(params, namespace, rewrites, autostart)
	if err != nil {
		return nil, fmt.Errorf("launch: rewrite params for namespace: %w", err)
	}

	return out, nil
}

// CanonicalDump renders params in ros2 param dump form.
func CanonicalDump(nodeFQN string, params map[string]ParamValue) (string, error) {
	out, err := ilaunch.CanonicalDump(nodeFQN, params)
	if err != nil {
		return "", fmt.Errorf("launch: canonical dump: %w", err)
	}

	return out, nil
}

// UndeclaredParams returns sorted override names missing from declared.
func UndeclaredParams(declared map[string]ParamValue, overrides map[string]ParamValue) []string {
	return ilaunch.UndeclaredParams(declared, overrides)
}

// AutoDeclareUndeclared merges undeclared overrides into a copy.
func AutoDeclareUndeclared(declared map[string]ParamValue, overrides map[string]ParamValue) map[string]ParamValue {
	return ilaunch.AutoDeclareUndeclared(declared, overrides)
}
