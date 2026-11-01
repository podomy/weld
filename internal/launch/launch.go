// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package launch implements launch argument acceptance with full fidelity.
//
// Startup applies files plus CLI plus code in a fixed precedence with one
// batched event, and remap applies as a pure first class rewrite table at
// creation. This package owns the argv side of that contract: it parses
// --ros-args, forwards CLI argv into node options, resolves per node
// namespaces, rewrites YAML per namespace, appends SDF params as node args
// with sim time forced on, and auto declares undeclared parameters from
// overrides.
//
// The parameter value model is shared with internal/core: values are
// core.ParamValue with uniform array typing enforced by core. Topic and
// service remaps use a separate table from parameter remaps because the
// validated name shapes differ (slash separated graph names versus dotted
// param names). Remap lookup is pure with first match across global plus
// local arrays winning. File top level collapse gives fully qualified names
// priority over wildcards, and across sources files lose to CLI which loses
// to code, with last wins inside each class.
package launch

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/podomy/weld/internal/core"
)

// SimTimeParam is the parameter forced true when simulation time is used.
const SimTimeParam = "use_sim_time"

// AutostartParam is the lifecycle autostart parameter forced by rewrites.
const AutostartParam = "autostart"

// ROSLogDirEnv names the environment variable redirecting logs.
const ROSLogDirEnv = "ROS_LOG_DIR"

// RemapRule rewrites one graph name to another at creation time.
//
// From and To are ROS topic or service names such as /cmd_vel. The table
// is fixed at creation: callers validate with ValidateRemapRule and resolve
// with ResolveRemap, which checks global rules in order first and then
// local rules in order with the first exact match winning.
type RemapRule struct {
	// From is the input graph name.
	From string
	// To is the rewritten graph name.
	To string
}

// ROSArgs is the parsed result of one --ros-args section.
//
// NodeName and Namespace carry the last __node and __ns values when
// present. Remaps holds topic remaps deduped with last wins. Params holds
// current node overrides with last wins applied. ParamsFiles lists params
// files in order. The zero value is empty with no overrides.
type ROSArgs struct {
	// NodeName is the last __node value.
	NodeName string
	// HasNodeName reports whether __node was present.
	HasNodeName bool
	// Namespace is the last normalized absolute __ns value.
	Namespace string
	// HasNamespace reports whether __ns was present.
	HasNamespace bool
	// Remaps holds deduped topic remaps.
	Remaps []RemapRule
	// Params holds current node overrides.
	Params map[string]core.ParamValue
	// ParamsFiles lists params files in order.
	ParamsFiles []string
}

// NodeOptions holds one node's launch options before startup.
//
// NodeName and Namespace select the node. Remaps holds local graph remaps.
// Params holds code level overrides for the current node. ParamsFiles lists
// params files in order. AllowUndeclared permits Set to auto declare
// unknown names; startup and override batches always auto declare from
// overrides regardless of this flag to match the store.
type NodeOptions struct {
	// NodeName is the base node name without namespace.
	NodeName string
	// Namespace is the absolute namespace such as /ns.
	Namespace string
	// Remaps holds local graph remaps.
	Remaps []RemapRule
	// Params holds code level overrides for the current node.
	Params map[string]core.ParamValue
	// ParamsFiles lists params files in order.
	ParamsFiles []string
	// AllowUndeclared permits Set to auto declare unknown names.
	AllowUndeclared bool
}

// LaunchPlan holds one bringup with global remaps plus per node options.
//
// GlobalRemaps apply to every node and are searched before each node's
// local remaps with first match winning. Nodes carry per node namespaces
// and private remaps for multiple nodes per process.
type LaunchPlan struct {
	// GlobalRemaps apply to every node.
	GlobalRemaps []RemapRule
	// Nodes holds one entry per node.
	Nodes []NodeOptions
}

// NewNodeOptions returns validated options with empty maps.
//
// Node may be empty only when allowEmpty is true for plan assembly before
// CLI forwarding fills the name; callers that need a runnable node must
// pass a concrete name or forward argv carrying __node.
func NewNodeOptions(node, namespace string, allowEmpty bool) (NodeOptions, error) {
	if node == "" {
		if !allowEmpty {
			return NodeOptions{}, errors.New("launch: node name is empty") //nolint:wrapcheck // new error, nothing to wrap
		}
	} else {
		err := ValidateNodeName(node)
		if err != nil {
			return NodeOptions{}, err
		}
	}
	ns, err := NormalizeNamespace(namespace)
	if err != nil {
		return NodeOptions{}, err
	}
	return NodeOptions{
		NodeName:    node,
		Namespace:   ns,
		Params:      make(map[string]core.ParamValue),
		ParamsFiles: []string{},
		Remaps:      []RemapRule{},
	}, nil
}

// isNameStartChar reports whether c starts a node or namespace segment.
func isNameStartChar(c byte) bool {
	return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

// isNameChar reports whether c continues a node or namespace segment.
func isNameChar(c byte) bool {
	return isNameStartChar(c) || (c >= '0' && c <= '9')
}

// ValidateNodeName checks one base node name.
func ValidateNodeName(name string) error {
	if name == "" {
		return errors.New("launch: node name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if len(name) > 255 {
		return fmt.Errorf("launch: node name %q exceeds 255 bytes", name)
	}
	if !isNameStartChar(name[0]) {
		return fmt.Errorf("launch: node name %q must start with letter or underscore", name)
	}
	for i := range len(name) {
		if !isNameChar(name[i]) {
			return fmt.Errorf("launch: node name %q has bad character %q", name, string([]byte{name[i]}))
		}
	}
	return nil
}

// NormalizeNamespace returns the absolute form of ns.
//
// Empty maps to /. Relative input resolves to absolute by prefixing /.
// Redundant slashes collapse and a trailing slash is trimmed except for
// root. Segments must start with a letter or underscore.
func NormalizeNamespace(ns string) (string, error) {
	trimmed := strings.TrimSpace(ns)
	if trimmed == "" {
		return "/", nil
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	parts := strings.Split(trimmed, "/")
	var segs []string
	for _, p := range parts {
		if p == "" {
			continue
		}
		err := validateNamespaceSegment(p)
		if err != nil {
			return "", err
		}
		segs = append(segs, p)
	}
	if len(segs) == 0 {
		return "/", nil
	}
	return "/" + strings.Join(segs, "/"), nil
}

// validateNamespaceSegment checks one namespace segment.
func validateNamespaceSegment(seg string) error {
	if seg == "" {
		return errors.New("launch: namespace has empty segment") //nolint:wrapcheck // new error, nothing to wrap
	}
	if !isNameStartChar(seg[0]) {
		return fmt.Errorf("launch: namespace segment %q must start with letter or underscore", seg)
	}
	for i := range len(seg) {
		if !isNameChar(seg[i]) {
			return fmt.Errorf("launch: namespace segment %q has bad character %q", seg, string([]byte{seg[i]}))
		}
	}
	return nil
}

// ResolveNodeFQN returns the fully qualified node name.
//
// The namespace is normalized to absolute form first, so a relative __ns
// value resolves absolutely. Root namespace yields /node, otherwise
// /ns/node. Both parts are validated.
func ResolveNodeFQN(node, namespace string) (string, error) {
	err := ValidateNodeName(node)
	if err != nil {
		return "", err
	}
	ns, err := NormalizeNamespace(namespace)
	if err != nil {
		return "", err
	}
	if ns == "/" {
		return "/" + node, nil
	}
	return ns + "/" + node, nil
}

// ApplyNamespacePush joins a base namespace with a push fragment.
//
// An empty push returns the normalized base. An absolute push replaces the
// base. A relative push appends under the base. The result is normalized
// to absolute form for per node namespaces and namespace push actions.
func ApplyNamespacePush(base, push string) (string, error) {
	bns, err := NormalizeNamespace(base)
	if err != nil {
		return "", err
	}
	trimmed := strings.TrimSpace(push)
	if trimmed == "" {
		return bns, nil
	}
	if strings.HasPrefix(trimmed, "/") {
		out, err := NormalizeNamespace(trimmed)
		if err != nil {
			return "", err
		}
		return out, nil
	}
	joined := bns
	if joined == "/" {
		joined = "/" + trimmed
	} else {
		joined = joined + "/" + trimmed
	}
	out, err := NormalizeNamespace(joined)
	if err != nil {
		return "", err
	}
	return out, nil
}

// ValidateRemapName checks one graph name for remap rules.
func ValidateRemapName(name string) error {
	if name == "" {
		return errors.New("launch: remap name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if strings.Contains(name, " ") || strings.Contains(name, "\t") {
		return fmt.Errorf("launch: remap name %q contains whitespace", name)
	}
	if strings.Contains(name, ":=") {
		return fmt.Errorf("launch: remap name %q must not contain :=", name)
	}
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return errors.New("launch: remap name is blank") //nolint:wrapcheck // new error, nothing to wrap
	}
	parts := strings.Split(trimmed, "/")
	segs := 0
	for _, p := range parts {
		if p == "" {
			continue
		}
		err := validateNamespaceSegment(p)
		if err != nil {
			return fmt.Errorf("launch: remap name %q: %w", name, err)
		}
		segs++
	}
	if segs == 0 {
		return fmt.Errorf("launch: remap name %q has no segments", name)
	}
	return nil
}

// ValidateRemapRule checks both ends of one graph remap rule.
func ValidateRemapRule(r RemapRule) error {
	err := ValidateRemapName(r.From)
	if err != nil {
		return fmt.Errorf("launch: remap from: %w", err)
	}
	err = ValidateRemapName(r.To)
	if err != nil {
		return fmt.Errorf("launch: remap to: %w", err)
	}
	return nil
}

// ResolveRemap rewrites name through the creation time table.
//
// Global rules are searched in order first, then local rules in order,
// with the first exact match winning. The function is pure and never
// mutates its inputs. The name is fully validated.
func ResolveRemap(name string, global, local []RemapRule) (string, error) {
	err := ValidateRemapName(name)
	if err != nil {
		return "", err
	}
	for _, r := range global {
		if r.From == name {
			return r.To, nil
		}
	}
	for _, r := range local {
		if r.From == name {
			return r.To, nil
		}
	}
	return name, nil
}

// ParseROSArgs parses one --ros-args section with last wins ordering.
//
// When argv contains --ros-args, only tokens after its first occurrence
// are parsed, stopping before the first bare -- terminator. Without the
// marker the whole slice is parsed. Supported tokens in any order are -r
// src:=dst (with __node and __ns intercepted), -p name:=value, and
// --params-file path (or --params-file=path). Dict literals for -p are
// flattened with dotted keys, matching temp file materialization. Unknown
// dash flags and bare positionals fail loudly.
// sliceROSATokens returns the tokens ParseROSArgs must parse.
func sliceROSATokens(argv []string) []string {
	start := 0
	found := false
	for i, a := range argv {
		if a == "--ros-args" {
			start = i + 1
			found = true
			break
		}
	}
	var tokens []string
	if found {
		tokens = argv[start:]
	} else {
		tokens = argv
	}
	end := len(tokens)
	for i, t := range tokens {
		if t == "--" {
			end = i
			break
		}
	}
	return tokens[:end]
}

// consumeDashArg handles -flag value and -flag=value at position i.
func consumeDashArg(out *ROSArgs, tok string, tokens []string, i int, flag, prefix, missingMsg, emptyMsg string, apply func(*ROSArgs, string) error) (int, bool, error) {
	if tok == flag {
		if i+1 >= len(tokens) {
			return 0, true, errors.New(missingMsg) //nolint:wrapcheck // new error, nothing to wrap
		}
		if err := apply(out, tokens[i+1]); err != nil {
			return 0, true, err
		}
		return i + 2, true, nil
	}
	if rest, ok := strings.CutPrefix(tok, prefix); ok {
		if rest == "" {
			return 0, true, errors.New(emptyMsg) //nolint:wrapcheck // new error, nothing to wrap
		}
		if err := apply(out, rest); err != nil {
			return 0, true, err
		}
		return i + 1, true, nil
	}
	return 0, false, nil
}

// consumeRemapArg handles -r and -r= at position i.
func consumeRemapArg(out *ROSArgs, tok string, tokens []string, i int) (int, bool, error) {
	return consumeDashArg(out, tok, tokens, i, "-r", "-r=", "launch: -r needs a src:=dst argument", "launch: -r= needs a src:=dst argument", applyRemapToken)
}

// consumeParamArg handles -p and -p= at position i.
func consumeParamArg(out *ROSArgs, tok string, tokens []string, i int) (int, bool, error) {
	return consumeDashArg(out, tok, tokens, i, "-p", "-p=", "launch: -p needs a name:=value argument", "launch: -p= needs a name:=value argument", applyParamToken)
}

// consumeParamsFileArg handles --params-file forms at position i.
func consumeParamsFileArg(out *ROSArgs, tok string, tokens []string, i int) (int, bool, error) {
	if tok == "--params-file" {
		if i+1 >= len(tokens) {
			return 0, true, errors.New("launch: --params-file needs a path") //nolint:wrapcheck // new error, nothing to wrap
		}
		next := strings.TrimSpace(tokens[i+1])
		if next == "" {
			return 0, true, errors.New("launch: --params-file path is empty") //nolint:wrapcheck // new error, nothing to wrap
		}
		out.ParamsFiles = append(out.ParamsFiles, next)
		return i + 2, true, nil
	}
	if raw, ok := strings.CutPrefix(tok, "--params-file="); ok {
		rest := strings.TrimSpace(raw)
		if rest == "" {
			return 0, true, errors.New("launch: --params-file path is empty") //nolint:wrapcheck // new error, nothing to wrap
		}
		out.ParamsFiles = append(out.ParamsFiles, rest)
		return i + 1, true, nil
	}
	return 0, false, nil
}

// consumeROSArg handles one token at position i, returning the next index.
func consumeROSArg(out *ROSArgs, tokens []string, i int) (int, error) {
	tok := tokens[i]
	if next, handled, err := consumeRemapArg(out, tok, tokens, i); handled {
		return next, err
	}
	if next, handled, err := consumeParamArg(out, tok, tokens, i); handled {
		return next, err
	}
	if next, handled, err := consumeParamsFileArg(out, tok, tokens, i); handled {
		return next, err
	}
	if strings.HasPrefix(tok, "-") {
		return 0, fmt.Errorf("launch: unknown ros arg %q", tok)
	}
	return 0, fmt.Errorf("launch: unexpected positional %q after --ros-args", tok)
}

func ParseROSArgs(argv []string) (ROSArgs, error) {
	out := ROSArgs{
		Params:      make(map[string]core.ParamValue),
		ParamsFiles: []string{},
		Remaps:      []RemapRule{},
	}
	tokens := sliceROSATokens(argv)
	i := 0
	for i < len(tokens) {
		next, err := consumeROSArg(&out, tokens, i)
		if err != nil {
			return ROSArgs{}, err
		}
		i = next
	}
	out.Remaps = dedupeRemapsLastWins(out.Remaps)
	return out, nil
}

// applyRemapToken handles one src:=dst token for ParseROSArgs.
func applyRemapToken(out *ROSArgs, token string) error {
	from, to, ok := strings.Cut(token, ":=")
	if !ok {
		return fmt.Errorf("launch: remap %q misses :=", token)
	}
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	if from == "" || to == "" {
		return fmt.Errorf("launch: remap %q needs non-empty src and dst", token)
	}
	if from == "__node" {
		err := ValidateNodeName(to)
		if err != nil {
			return fmt.Errorf("launch: __node: %w", err)
		}
		out.NodeName = to
		out.HasNodeName = true
		return nil
	}
	if from == "__ns" {
		ns, err := NormalizeNamespace(to)
		if err != nil {
			return fmt.Errorf("launch: __ns: %w", err)
		}
		out.Namespace = ns
		out.HasNamespace = true
		return nil
	}
	if strings.HasPrefix(from, "__") {
		return fmt.Errorf("launch: unsupported special remap %q", from)
	}
	rule := RemapRule{From: from, To: to}
	err := ValidateRemapRule(rule)
	if err != nil {
		return err
	}
	out.Remaps = append(out.Remaps, rule)
	return nil
}

// applyParamToken handles one name:=value token for ParseROSArgs.
func applyParamToken(out *ROSArgs, token string) error {
	name, literal, ok := strings.Cut(token, ":=")
	if !ok {
		return fmt.Errorf("launch: param %q misses :=", token)
	}
	name = strings.TrimSpace(name)
	literal = strings.TrimSpace(literal)
	if name == "" {
		return fmt.Errorf("launch: param %q needs a name", token)
	}
	err := core.ValidateParamName(name)
	if err != nil {
		return fmt.Errorf("launch: param: %w", err)
	}
	if literal == "" {
		return fmt.Errorf("launch: param %q needs a value", token)
	}
	if strings.HasPrefix(literal, "{") {
		expanded, err := expandDictLiteral(name, literal)
		if err != nil {
			return err
		}
		maps.Copy(out.Params, expanded)
		return nil
	}
	value, err := core.ParseOverrideLiteral(literal)
	if err != nil {
		return fmt.Errorf("launch: param %q: %w", name, err)
	}
	out.Params[name] = value
	return nil
}

// expandDictLiteral flattens a {k: v, ...} literal under prefix.
func expandDictLiteral(prefix, literal string) (map[string]core.ParamValue, error) {
	parsed, err := parseFlowMap(literal)
	if err != nil {
		return nil, fmt.Errorf("launch: param %q: %w", prefix, err)
	}
	flat, err := core.FlattenNestedParams(parsed, prefix)
	if err != nil {
		return nil, fmt.Errorf("launch: param %q: %w", prefix, err)
	}
	return flat, nil
}

// dedupeRemapsLastWins keeps the last value per From.
//
// Later CLI entries win over earlier ones so last wins ordering holds
// while ResolveRemap keeps first match semantics on the deduped table.
func dedupeRemapsLastWins(rules []RemapRule) []RemapRule {
	last := make(map[string]string)
	order := []string{}
	seen := make(map[string]bool)
	for _, r := range rules {
		if _, ok := last[r.From]; !ok {
			order = append(order, r.From)
		} else if !seen[r.From] {
			order = append(order, r.From)
			seen[r.From] = true
		}
		last[r.From] = r.To
	}
	// Rebuild in first occurrence order with last values, dropping
	// duplicate positions so each From appears once.
	placed := make(map[string]bool)
	var out []RemapRule
	for _, from := range order {
		if placed[from] {
			continue
		}
		placed[from] = true
		out = append(out, RemapRule{From: from, To: last[from]})
	}
	if out == nil {
		return []RemapRule{}
	}
	return out
}

// cloneNodeOptions deep copies options so forwarding never aliases input.
func cloneNodeOptions(in NodeOptions) NodeOptions {
	remaps := make([]RemapRule, len(in.Remaps))
	copy(remaps, in.Remaps)
	params := make(map[string]core.ParamValue, len(in.Params))
	maps.Copy(params, in.Params)
	files := make([]string, len(in.ParamsFiles))
	copy(files, in.ParamsFiles)
	return NodeOptions{
		NodeName:        in.NodeName,
		Namespace:       in.Namespace,
		Remaps:          remaps,
		Params:          params,
		ParamsFiles:     files,
		AllowUndeclared: in.AllowUndeclared,
	}
}

// mergeRemapsLastWins appends CLI remaps with CLI winning per From.
func mergeRemapsLastWins(base, cli []RemapRule) []RemapRule {
	combined := make([]RemapRule, 0, len(base)+len(cli))
	combined = append(combined, base...)
	combined = append(combined, cli...)
	return dedupeRemapsLastWins(combined)
}

// ForwardArgs appends CLI argv to node options.
//
// The argv is parsed with ParseROSArgs. Node name and namespace from CLI
// replace the base values when present. Remaps merge with CLI winning per
// From. Params merge with CLI winning per key. Params files append in
// order. The base value is never mutated.
func ForwardArgs(base NodeOptions, argv []string) (NodeOptions, error) {
	parsed, err := ParseROSArgs(argv)
	if err != nil {
		return NodeOptions{}, err
	}
	out := cloneNodeOptions(base)
	if out.Params == nil {
		out.Params = make(map[string]core.ParamValue)
	}
	if parsed.HasNodeName {
		out.NodeName = parsed.NodeName
	}
	if parsed.HasNamespace {
		out.Namespace = parsed.Namespace
	}
	if out.Namespace == "" {
		out.Namespace = "/"
	} else {
		ns, err := NormalizeNamespace(out.Namespace)
		if err != nil {
			return NodeOptions{}, err
		}
		out.Namespace = ns
	}
	if out.NodeName != "" {
		err = ValidateNodeName(out.NodeName)
		if err != nil {
			return NodeOptions{}, err
		}
	}
	out.Remaps = mergeRemapsLastWins(out.Remaps, parsed.Remaps)
	maps.Copy(out.Params, parsed.Params)
	out.ParamsFiles = append(out.ParamsFiles, parsed.ParamsFiles...)
	return out, nil
}

// RewriteParamsForNamespace applies per namespace rewrites plus autostart.
//
// Params are copied and never mutated. Each rewrite value is parsed as a
// YAML literal with core.ParseOverrideLiteral, so zone rewrites such as
// zone clearing flags stay typed. When autostart is true the autostart
// key is forced to boolean true. The namespace is validated absolute.
func RewriteParamsForNamespace(params map[string]core.ParamValue, namespace string, rewrites map[string]string, autostart bool) (map[string]core.ParamValue, error) {
	_, err := NormalizeNamespace(namespace)
	if err != nil {
		return nil, err
	}
	out := make(map[string]core.ParamValue, len(params)+len(rewrites)+1)
	maps.Copy(out, params)
	keys := make([]string, 0, len(rewrites))
	for k := range rewrites {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		err = core.ValidateParamName(k)
		if err != nil {
			return nil, fmt.Errorf("launch: rewrite: %w", err)
		}
		raw := strings.TrimSpace(rewrites[k])
		if raw == "" {
			return nil, fmt.Errorf("launch: rewrite %q has empty value", k)
		}
		value, err := core.ParseOverrideLiteral(raw)
		if err != nil {
			return nil, fmt.Errorf("launch: rewrite %q: %w", k, err)
		}
		out[k] = value
	}
	if autostart {
		out[AutostartParam] = core.NewBoolParam(true)
	}
	return out, nil
}

// AppendSDFParams appends an SDF params file with sim time forced on.
//
// The path is recorded as a node argument without parsing: SDF is XML for
// the simulation bridge, opaque here. The use_sim_time override is set to
// boolean true after any existing value so the forced value wins. The
// input options are never mutated.
func AppendSDFParams(opts NodeOptions, sdfPath string) (NodeOptions, error) {
	trimmed := strings.TrimSpace(sdfPath)
	if trimmed == "" {
		return NodeOptions{}, errors.New("launch: sdf path is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	out := cloneNodeOptions(opts)
	if out.Params == nil {
		out.Params = make(map[string]core.ParamValue)
	}
	out.ParamsFiles = append(out.ParamsFiles, trimmed)
	out.Params[SimTimeParam] = core.NewBoolParam(true)
	return out, nil
}

// UndeclaredParams returns sorted override names missing from declared.
func UndeclaredParams(declared, overrides map[string]core.ParamValue) []string {
	var out []string
	for k := range overrides {
		if _, ok := declared[k]; !ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// AutoDeclareUndeclared merges undeclared overrides into a copy.
//
// Entries already declared keep their declared values. Every override
// name missing from declared is added. The inputs are never mutated.
func AutoDeclareUndeclared(declared, overrides map[string]core.ParamValue) map[string]core.ParamValue {
	out := make(map[string]core.ParamValue, len(declared)+len(overrides))
	maps.Copy(out, declared)
	keys := make([]string, 0, len(overrides))
	for k := range overrides {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, ok := out[k]; !ok {
			out[k] = overrides[k]
		}
	}
	return out
}

// ResolveLogDir returns ROS_LOG_DIR when set and non blank.
func ResolveLogDir() (string, bool) {
	raw, ok := os.LookupEnv(ROSLogDirEnv)
	if !ok {
		return "", false
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false
	}
	return trimmed, true
}

// ResolvePlanFQNs validates a launch plan and returns node FQNs in order.
//
// Global and per node remaps are validated. Each node name and namespace
// is validated with absolute namespace resolution. Duplicate FQNs fail
// because two nodes in one process must not share one fully qualified
// name.
func ResolvePlanFQNs(plan LaunchPlan) ([]string, error) {
	for _, r := range plan.GlobalRemaps {
		err := ValidateRemapRule(r)
		if err != nil {
			return nil, err
		}
	}
	seen := make(map[string]bool)
	out := make([]string, 0, len(plan.Nodes))
	for i, n := range plan.Nodes {
		for _, r := range n.Remaps {
			err := ValidateRemapRule(r)
			if err != nil {
				return nil, fmt.Errorf("launch: plan node %d: %w", i, err)
			}
		}
		fqn, err := ResolveNodeFQN(n.NodeName, n.Namespace)
		if err != nil {
			return nil, fmt.Errorf("launch: plan node %d: %w", i, err)
		}
		if seen[fqn] {
			return nil, fmt.Errorf("launch: plan duplicate node %q", fqn)
		}
		seen[fqn] = true
		out = append(out, fqn)
	}
	return out, nil
}

// LoadParamsFile reads one YAML params file with substitutions evaluated.
func LoadParamsFile(path string) (map[string]map[string]core.ParamValue, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil, errors.New("launch: params file path is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	raw, err := os.ReadFile(trimmed) //nolint:gosec // G304: launch reads user requested params file path
	if err != nil {
		return nil, fmt.Errorf("launch: read params file %q: %w", trimmed, err)
	}
	out, err := ParseParamsContent(raw)
	if err != nil {
		return nil, fmt.Errorf("launch: parse params file %q: %w", trimmed, err)
	}
	return out, nil
}

// ParseParamsContent parses params YAML with dotted flattening.
//
// The top level maps node patterns to mappings holding ros__parameters.
// Wildcard spellings *, /*, **, and /** all collapse to the wildcard.
// Dict keys flatten with dots, array typing stays uniform through core,
// and $(env VAR [default]) substitutions evaluate before YAML parsing so
// substituted scalars keep their YAML type. The result maps normalized
// node keys to flattened params.
// fileSectionForKey resolves one file top key to its ros__parameters mapping.
func fileSectionForKey(k string, mapping map[string]any) (map[string]any, error) {
	rawSection, ok := mapping[k].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("launch: params top key %q must map to ros__parameters", k)
	}
	if inner, ok := rawSection["ros__parameters"]; ok {
		innerMap, ok := inner.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("launch: params %q ros__parameters must be a mapping", k)
		}
		return innerMap, nil
	}
	return rawSection, nil
}

// flattenFileSection expands substitutions and flattens one file section.
func flattenFileSection(k string, section map[string]any) (map[string]core.ParamValue, error) {
	expanded, err := expandAnySubstitutions(section)
	if err != nil {
		return nil, fmt.Errorf("launch: params %q: %w", k, err)
	}
	expandedMap, ok := expanded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("launch: params %q did not expand to a mapping", k)
	}
	flat, err := core.FlattenNestedParams(expandedMap, "")
	if err != nil {
		return nil, fmt.Errorf("launch: params %q: %w", k, err)
	}
	return flat, nil
}

func ParseParamsContent(content []byte) (map[string]map[string]core.ParamValue, error) {
	expandedRaw, err := expandSubstitutions(string(content))
	if err != nil {
		return nil, err
	}
	mapping, err := parseYAMLMapping(expandedRaw)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]core.ParamValue)
	keys := make([]string, 0, len(mapping))
	for k := range mapping {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		norm := normalizeFileNodeKey(k)
		section, err := fileSectionForKey(k, mapping)
		if err != nil {
			return nil, err
		}
		flat, err := flattenFileSection(k, section)
		if err != nil {
			return nil, err
		}
		if existing, ok := out[norm]; ok {
			maps.Copy(existing, flat)
		} else {
			out[norm] = flat
		}
	}
	return out, nil
}

// normalizeFileNodeKey maps file top keys to core collapse form.
func normalizeFileNodeKey(key string) string {
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return "*"
	}
	if trimmed == "*" || trimmed == "/*" || trimmed == "**" || trimmed == "/**" {
		return "*"
	}
	inner := strings.Trim(trimmed, "/")
	inner = strings.ReplaceAll(inner, "/", ".")
	inner = strings.Trim(inner, ".")
	if inner == "" || inner == "*" || inner == "**" {
		return "*"
	}
	return inner
}

// toCoreFiles converts launch file sections to core collapse input.
func toCoreFiles(sections map[string]map[string]core.ParamValue) map[string]map[string]core.ParamValue {
	out := make(map[string]map[string]core.ParamValue, len(sections))
	for k, v := range sections {
		out[normalizeFileNodeKey(k)] = v
	}
	return out
}

// MergeFileSections merges file sections in order with later files winning.
func MergeFileSections(all []map[string]map[string]core.ParamValue) map[string]map[string]core.ParamValue {
	out := make(map[string]map[string]core.ParamValue)
	for _, sections := range all {
		for nodeKey, params := range sections {
			norm := normalizeFileNodeKey(nodeKey)
			existing, ok := out[norm]
			if !ok {
				existing = make(map[string]core.ParamValue)
				out[norm] = existing
			}
			maps.Copy(existing, params)
		}
	}
	return out
}

// coreNodeKey converts an FQN to core collapse lookup form.
func coreNodeKey(fqn string) string {
	trimmed := strings.TrimSpace(fqn)
	trimmed = strings.Trim(trimmed, "/")
	trimmed = strings.ReplaceAll(trimmed, "/", ".")
	trimmed = strings.Trim(trimmed, ".")
	if trimmed == "" {
		return "*"
	}
	return trimmed
}

// ResolveNodeParams merges files plus CLI plus code for one node.
//
// File sections collapse with fully qualified names beating wildcards.
// The collapsed file map loses to CLI which loses to code, with last wins
// already applied inside each class before the call. The result is meant
// for a single batched store event, not N events.
func ResolveNodeParams(nodeFQN string, files map[string]map[string]core.ParamValue, cli, code map[string]core.ParamValue) map[string]core.ParamValue {
	collapsed := core.CollapseParamFiles(toCoreFiles(files), coreNodeKey(nodeFQN))
	out := make(map[string]core.ParamValue, len(collapsed)+len(cli)+len(code))
	maps.Copy(out, collapsed)
	maps.Copy(out, cli)
	maps.Copy(out, code)
	return out
}

// CanonicalDump renders params in ros2 param dump form.
func CanonicalDump(nodeFQN string, params map[string]core.ParamValue) (string, error) {
	trimmed := strings.TrimSpace(nodeFQN)
	if trimmed == "" {
		return "", errors.New("launch: dump node name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(trimmed)                //nolint:errcheck // strings.Builder writes never fail
	b.WriteString(":\n")                  //nolint:errcheck // strings.Builder writes never fail
	b.WriteString("  ros__parameters:\n") //nolint:errcheck // strings.Builder writes never fail
	if len(keys) == 0 {
		b.WriteString("    {}\n") //nolint:errcheck // strings.Builder writes never fail
		return b.String(), nil
	}
	for _, k := range keys {
		err := core.ValidateParamName(k)
		if err != nil {
			return "", fmt.Errorf("launch: dump: %w", err)
		}
		rendered, err := canonicalValue(params[k])
		if err != nil {
			return "", fmt.Errorf("launch: dump %q: %w", k, err)
		}
		b.WriteString("    ")   //nolint:errcheck // strings.Builder writes never fail
		b.WriteString(k)        //nolint:errcheck // strings.Builder writes never fail
		b.WriteString(": ")     //nolint:errcheck // strings.Builder writes never fail
		b.WriteString(rendered) //nolint:errcheck // strings.Builder writes never fail
		b.WriteString("\n")     //nolint:errcheck // strings.Builder writes never fail
	}
	return b.String(), nil
}

// canonicalValue renders one param value in canonical YAML form.
func canonicalValue(v core.ParamValue) (string, error) {
	switch v.Type {
	case core.ParamTypeBool:
		if v.Bool {
			return "true", nil
		}
		return "false", nil
	case core.ParamTypeInteger:
		return strconv.FormatInt(v.Integer, 10), nil
	case core.ParamTypeDouble:
		return formatDouble(v.Double), nil
	case core.ParamTypeString:
		return quoteString(v.String), nil
	case core.ParamTypeByteArray, core.ParamTypeBoolArray, core.ParamTypeIntegerArray, core.ParamTypeDoubleArray, core.ParamTypeStringArray:
		return canonicalArrayValue(v)
	case core.ParamTypeNotSet:
		return "", errors.New("launch: dump not_set value has no canonical form") //nolint:wrapcheck // new error, nothing to wrap
	default:
		return "", fmt.Errorf("launch: dump unknown param type %s", v.Type)
	}
}

// canonicalArrayValue renders one array param value in canonical YAML form.
func canonicalArrayValue(v core.ParamValue) (string, error) {
	switch v.Type {
	case core.ParamTypeBool, core.ParamTypeInteger, core.ParamTypeDouble, core.ParamTypeString, core.ParamTypeNotSet:
		return "", fmt.Errorf("launch: dump unknown param type %s", v.Type)
	case core.ParamTypeByteArray:
		return formatByteArray(v.Bytes), nil
	case core.ParamTypeBoolArray:
		return formatBoolArray(v.Bools), nil
	case core.ParamTypeIntegerArray:
		return formatIntegerArray(v.Integers), nil
	case core.ParamTypeDoubleArray:
		return formatDoubleArray(v.Doubles), nil
	case core.ParamTypeStringArray:
		return formatStringArray(v.Strings), nil
	default:
		return "", fmt.Errorf("launch: dump unknown param type %s", v.Type)
	}
}

// formatDouble renders a double keeping double typing for whole numbers.
func formatDouble(f float64) string {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	if !strings.ContainsAny(s, "./eE") {
		s += ".0"
	}
	return s
}

// quoteString renders a string plain when safe, quoted otherwise.
func quoteString(s string) string {
	if s == "" {
		return "''"
	}
	if isPlainString(s) {
		return s
	}
	if strings.Contains(s, "\n") || strings.Contains(s, "\"") || strings.Contains(s, "\\") {
		escaped := strings.ReplaceAll(s, "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "\"", "\\\"")
		escaped = strings.ReplaceAll(escaped, "\n", "\\n")
		escaped = strings.ReplaceAll(escaped, "\t", "\\t")
		return "\"" + escaped + "\""
	}
	escaped := strings.ReplaceAll(s, "'", "''")
	return "'" + escaped + "'"
}

// isPlainReserved reports whether s is a YAML keyword needing quotes.
func isPlainReserved(s string) bool {
	return s == "true" || s == "false" || s == "null" || s == "~"
}

// isPlainChar reports whether c is allowed at position i in a plain string.
func isPlainChar(c byte, i int) bool {
	if c == '_' || c == '-' || c == '/' {
		return true
	}
	if c >= 'A' && c <= 'Z' {
		return true
	}
	if c >= 'a' && c <= 'z' {
		return true
	}
	if i > 0 && c >= '0' && c <= '9' {
		return true
	}
	return false
}

// isPlainChars reports whether every byte of s is a plain string char.
func isPlainChars(s string) bool {
	for i := range len(s) {
		if !isPlainChar(s[i], i) {
			return false
		}
	}
	return true
}

// isPlainString reports whether s renders without quotes.
func isPlainString(s string) bool {
	if isPlainReserved(s) {
		return false
	}
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return false
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return false
	}
	if !isPlainChars(s) {
		return false
	}
	first := s[0]
	if first >= '0' && first <= '9' {
		return false
	}
	return true
}

// formatByteArray renders opaque bytes as an integer list.
func formatByteArray(b []byte) string {
	var sb strings.Builder
	sb.WriteString("[") //nolint:errcheck // strings.Builder writes never fail
	for i, e := range b {
		if i > 0 {
			sb.WriteString(", ") //nolint:errcheck // strings.Builder writes never fail
		}
		sb.WriteString(strconv.FormatInt(int64(e), 10)) //nolint:errcheck // strings.Builder writes never fail
	}
	sb.WriteString("]") //nolint:errcheck // strings.Builder writes never fail
	return sb.String()
}

// formatBoolArray renders a boolean array.
func formatBoolArray(v []bool) string {
	var sb strings.Builder
	sb.WriteString("[") //nolint:errcheck // strings.Builder writes never fail
	for i, e := range v {
		if i > 0 {
			sb.WriteString(", ") //nolint:errcheck // strings.Builder writes never fail
		}
		if e {
			sb.WriteString("true") //nolint:errcheck // strings.Builder writes never fail
		} else {
			sb.WriteString("false") //nolint:errcheck // strings.Builder writes never fail
		}
	}
	sb.WriteString("]") //nolint:errcheck // strings.Builder writes never fail
	return sb.String()
}

// formatIntegerArray renders an integer array.
func formatIntegerArray(v []int64) string {
	var sb strings.Builder
	sb.WriteString("[") //nolint:errcheck // strings.Builder writes never fail
	for i, e := range v {
		if i > 0 {
			sb.WriteString(", ") //nolint:errcheck // strings.Builder writes never fail
		}
		sb.WriteString(strconv.FormatInt(e, 10)) //nolint:errcheck // strings.Builder writes never fail
	}
	sb.WriteString("]") //nolint:errcheck // strings.Builder writes never fail
	return sb.String()
}

// formatDoubleArray renders a double array.
func formatDoubleArray(v []float64) string {
	var sb strings.Builder
	sb.WriteString("[") //nolint:errcheck // strings.Builder writes never fail
	for i, e := range v {
		if i > 0 {
			sb.WriteString(", ") //nolint:errcheck // strings.Builder writes never fail
		}
		sb.WriteString(formatDouble(e)) //nolint:errcheck // strings.Builder writes never fail
	}
	sb.WriteString("]") //nolint:errcheck // strings.Builder writes never fail
	return sb.String()
}

// formatStringArray renders a string array.
func formatStringArray(v []string) string {
	var sb strings.Builder
	sb.WriteString("[") //nolint:errcheck // strings.Builder writes never fail
	for i, e := range v {
		if i > 0 {
			sb.WriteString(", ") //nolint:errcheck // strings.Builder writes never fail
		}
		sb.WriteString(quoteString(e)) //nolint:errcheck // strings.Builder writes never fail
	}
	sb.WriteString("]") //nolint:errcheck // strings.Builder writes never fail
	return sb.String()
}

// yamlLine is one parsed line with indent and content.
type yamlLine struct {
	indent int
	text   string
}

// validateYAMLKey checks one mapping key has no whitespace.
func validateYAMLKey(key string) error {
	if key == "" {
		return errors.New("launch: yaml has empty key") //nolint:wrapcheck // new error, nothing to wrap
	}
	if strings.Contains(key, " ") || strings.Contains(key, "\t") {
		return fmt.Errorf("launch: yaml key %q contains whitespace", key)
	}
	return nil
}

// parseYAMLMapping parses minimal indentation YAML to nested maps.
func parseYAMLMapping(content string) (map[string]any, error) {
	lines, err := splitYAMLLines(content)
	if err != nil {
		return nil, err
	}
	root := make(map[string]any)
	type frame struct {
		indent int
		m      map[string]any
	}
	stack := []frame{{indent: -1, m: root}}
	for _, l := range lines {
		for len(stack) > 1 && l.indent <= stack[len(stack)-1].indent {
			stack = stack[:len(stack)-1]
		}
		parent := stack[len(stack)-1].m
		key, value, ok := cutYAMLKey(l.text)
		if !ok {
			return nil, fmt.Errorf("launch: yaml line %q misses colon", l.text)
		}
		key = strings.TrimSpace(key)
		if err := validateYAMLKey(key); err != nil {
			return nil, err
		}
		value = strings.TrimSpace(value)
		if value == "" {
			child := make(map[string]any)
			parent[key] = child
			stack = append(stack, frame{indent: l.indent, m: child})
			continue
		}
		parsed, err := parseYAMLValue(value)
		if err != nil {
			return nil, fmt.Errorf("launch: yaml key %q: %w", key, err)
		}
		parent[key] = parsed
	}
	return root, nil
}

// splitYAMLLines splits content into indent annotated lines.
func splitYAMLLines(content string) ([]yamlLine, error) {
	raw := strings.ReplaceAll(content, "\r\n", "\n")
	parts := strings.Split(raw, "\n")
	var out []yamlLine
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		indent := 0
		for indent < len(p) && p[indent] == ' ' {
			indent++
		}
		if indent < len(p) && p[indent] == '\t' {
			return nil, errors.New("launch: yaml tabs are not allowed, use spaces") //nolint:wrapcheck // new error, nothing to wrap
		}
		text := strings.TrimSpace(stripYAMLComment(p[indent:]))
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		out = append(out, yamlLine{indent: indent, text: text})
	}
	return out, nil
}

// trackCommentQuote updates quote state for one byte, reporting if consumed.
func trackCommentQuote(c, prev byte, inSingle, inDouble bool) (bool, bool, bool) {
	if c == '\'' && !inDouble {
		return !inSingle, inDouble, true
	}
	if c == '"' && !inSingle {
		if prev == '\\' {
			return inSingle, inDouble, true
		}
		return inSingle, !inDouble, true
	}
	return inSingle, inDouble, false
}

// isCommentStart reports whether # at i starts a comment.
func isCommentStart(s string, i int, inSingle, inDouble bool) bool {
	if inSingle || inDouble {
		return false
	}
	if i == 0 {
		return true
	}
	return s[i-1] == ' ' || s[i-1] == '\t'
}

// stripYAMLComment removes a trailing # comment outside quotes.
func stripYAMLComment(s string) string {
	inSingle := false
	inDouble := false
	for i := range len(s) {
		c := s[i]
		var prev byte
		if i > 0 {
			prev = s[i-1]
		}
		nextSingle, nextDouble, consumed := trackCommentQuote(c, prev, inSingle, inDouble)
		inSingle, inDouble = nextSingle, nextDouble
		if consumed {
			continue
		}
		if c == '#' && isCommentStart(s, i, inSingle, inDouble) {
			return strings.TrimRight(s[:i], " \t")
		}
	}
	return s
}

// trackKeyQuote updates quote state for one key line byte.
func trackKeyQuote(c, prev byte, inSingle, inDouble bool) (bool, bool, bool) {
	if c == '\'' && !inDouble {
		return !inSingle, inDouble, true
	}
	if c == '"' && !inSingle {
		if prev == '\\' {
			return inSingle, inDouble, true
		}
		return inSingle, !inDouble, true
	}
	return inSingle, inDouble, false
}

// isKeyColon reports whether colon at i ends the key.
func isKeyColon(line string, i int, inSingle, inDouble bool) bool {
	if inSingle || inDouble {
		return false
	}
	if i+1 == len(line) {
		return true
	}
	return line[i+1] == ' ' || line[i+1] == '\t'
}

// cutYAMLKey splits one key: value line on the first colon.
func cutYAMLKey(line string) (string, string, bool) {
	inSingle := false
	inDouble := false
	for i := range len(line) {
		c := line[i]
		var prev byte
		if i > 0 {
			prev = line[i-1]
		}
		nextSingle, nextDouble, consumed := trackKeyQuote(c, prev, inSingle, inDouble)
		inSingle, inDouble = nextSingle, nextDouble
		if consumed {
			continue
		}
		if c == ':' && isKeyColon(line, i, inSingle, inDouble) {
			return line[:i], line[i+1:], true
		}
	}
	return "", "", false
}

// parseYAMLValue parses one scalar, flow array, or flow map.
func parseYAMLValue(s string) (any, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil, errors.New("launch: yaml value is empty") //nolint:wrapcheck // new error, nothing to wrap
	}
	if strings.HasPrefix(trimmed, "[") {
		return parseFlowArray(trimmed)
	}
	if strings.HasPrefix(trimmed, "{") {
		return parseFlowMap(trimmed)
	}
	return parseYAMLScalar(trimmed), nil
}

// parseYAMLScalar parses one plain or quoted scalar.
func parseYAMLScalar(s string) any {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		inner := trimmed[1 : len(trimmed)-1]
		inner = strings.ReplaceAll(inner, "\\\"", "\"")
		inner = strings.ReplaceAll(inner, "\\\\", "\\")
		inner = strings.ReplaceAll(inner, "\\n", "\n")
		inner = strings.ReplaceAll(inner, "\\t", "\t")
		return inner
	}
	if len(trimmed) >= 2 && trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'' {
		inner := trimmed[1 : len(trimmed)-1]
		inner = strings.ReplaceAll(inner, "''", "'")
		return inner
	}
	if trimmed == "true" {
		return true
	}
	if trimmed == "false" {
		return false
	}
	if iv, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return iv
	}
	if fv, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return fv
	}
	return trimmed
}

// parseFlowArray parses [a, b] with scalar elements only.
func parseFlowArray(s string) (any, error) {
	trimmed := strings.TrimSpace(s)
	if !strings.HasSuffix(trimmed, "]") {
		return nil, fmt.Errorf("launch: yaml array %q misses closing bracket", s)
	}
	inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	if inner == "" {
		return []any{}, nil
	}
	parts, err := splitTopLevel(inner, ',')
	if err != nil {
		return nil, err
	}
	out := make([]any, 0, len(parts))
	for _, p := range parts {
		item := strings.TrimSpace(p)
		if item == "" {
			return nil, fmt.Errorf("launch: yaml array %q has empty element", s)
		}
		if strings.HasPrefix(item, "[") || strings.HasPrefix(item, "{") {
			return nil, fmt.Errorf("launch: yaml array %q must hold scalars for params", s)
		}
		out = append(out, parseYAMLScalar(item))
	}
	return out, nil
}

// parseFlowMap parses {k: v, ...} with nested values.
func parseFlowMap(s string) (map[string]any, error) {
	trimmed := strings.TrimSpace(s)
	if !strings.HasSuffix(trimmed, "}") {
		return nil, fmt.Errorf("launch: yaml dict %q misses closing brace", s)
	}
	inner := strings.TrimSpace(trimmed[1 : len(trimmed)-1])
	out := make(map[string]any)
	if inner == "" {
		return out, nil
	}
	parts, err := splitTopLevel(inner, ',')
	if err != nil {
		return nil, err
	}
	for _, p := range parts {
		key, value, ok := cutFlowPair(p)
		if !ok {
			return nil, fmt.Errorf("launch: yaml dict entry %q misses colon", p)
		}
		key = strings.TrimSpace(strings.Trim(strings.TrimSpace(key), "\"'"))
		if key == "" {
			return nil, errors.New("launch: yaml dict has empty key") //nolint:wrapcheck // new error, nothing to wrap
		}
		parsed, err := parseYAMLValue(strings.TrimSpace(value))
		if err != nil {
			return nil, err
		}
		out[key] = parsed
	}
	return out, nil
}

// trackFlowQuote updates quote state for flow parsing.
func trackFlowQuote(c, prev byte, inSingle, inDouble bool) (bool, bool, bool) {
	if c == '\'' && !inDouble {
		return !inSingle, inDouble, true
	}
	if c == '"' && !inSingle {
		if prev == '\\' {
			return inSingle, inDouble, true
		}
		return inSingle, !inDouble, true
	}
	return inSingle, inDouble, false
}

// updateFlowDepth updates curly and square depth for one byte.
func updateFlowDepth(c byte, curly, square int) (int, int, bool) {
	switch c {
	case '{':
		return curly + 1, square, true
	case '}':
		return curly - 1, square, true
	case '[':
		return curly, square + 1, true
	case ']':
		return curly, square - 1, true
	default:
		return curly, square, false
	}
}

// cutFlowPair splits one k: v pair respecting quotes.
func cutFlowPair(s string) (string, string, bool) {
	inSingle := false
	inDouble := false
	depthCurly := 0
	depthSquare := 0
	for i := range len(s) {
		c := s[i]
		var prev byte
		if i > 0 {
			prev = s[i-1]
		}
		nextSingle, nextDouble, consumed := trackFlowQuote(c, prev, inSingle, inDouble)
		inSingle, inDouble = nextSingle, nextDouble
		if consumed {
			continue
		}
		if inSingle || inDouble {
			continue
		}
		nextCurly, nextSquare, isBracket := updateFlowDepth(c, depthCurly, depthSquare)
		depthCurly, depthSquare = nextCurly, nextSquare
		if isBracket {
			continue
		}
		if c == ':' && depthCurly == 0 && depthSquare == 0 {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

// checkFlowClose validates a closing bracket depth.
func checkFlowClose(c byte, depth int, s string) error {
	if depth >= 0 {
		return nil
	}
	if c == '}' {
		return fmt.Errorf("launch: unbalanced %q in %q", "}", s)
	}
	return fmt.Errorf("launch: unbalanced %q in %q", "]", s)
}

// validateTopLevel checks quotes and brackets after a split scan.
func validateTopLevel(s string, inSingle, inDouble bool, curly, square int) error {
	if inSingle || inDouble {
		return fmt.Errorf("launch: unterminated quote in %q", s)
	}
	if curly != 0 || square != 0 {
		return fmt.Errorf("launch: unbalanced brackets in %q", s)
	}
	return nil
}

// prevByte returns the previous byte or zero at the start.
func prevByte(s string, i int) byte {
	if i > 0 {
		return s[i-1]
	}
	return 0
}

// updateSplitDepths updates depths for one bracket byte in splitTopLevel.
func updateSplitDepths(c byte, curly, square int, s string) (int, int, error, bool) {
	if c == '{' || c == '[' {
		nextCurly, nextSquare, _ := updateFlowDepth(c, curly, square)
		return nextCurly, nextSquare, nil, true
	}
	if c == '}' {
		curly--
		if err := checkFlowClose(c, curly, s); err != nil {
			return 0, 0, err, true
		}
		return curly, square, nil, true
	}
	if c == ']' {
		square--
		if err := checkFlowClose(c, square, s); err != nil {
			return 0, 0, err, true
		}
		return curly, square, nil, true
	}
	return curly, square, nil, false
}

// splitTopLevel splits on sep respecting quotes and nesting.
func splitTopLevel(s string, sep byte) ([]string, error) {
	var out []string
	inSingle := false
	inDouble := false
	depthCurly := 0
	depthSquare := 0
	start := 0
	for i := range len(s) {
		c := s[i]
		prev := prevByte(s, i)
		nextSingle, nextDouble, consumed := trackFlowQuote(c, prev, inSingle, inDouble)
		inSingle, inDouble = nextSingle, nextDouble
		if consumed {
			continue
		}
		if inSingle || inDouble {
			continue
		}
		nextCurly, nextSquare, err, handled := updateSplitDepths(c, depthCurly, depthSquare, s)
		if err != nil {
			return nil, err
		}
		if handled {
			depthCurly, depthSquare = nextCurly, nextSquare
			continue
		}
		if c == sep && depthCurly == 0 && depthSquare == 0 {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if err := validateTopLevel(s, inSingle, inDouble, depthCurly, depthSquare); err != nil {
		return nil, err
	}
	out = append(out, s[start:])
	return out, nil
}

// expandAnySubstitutions expands $(env ...) in every string leaf.
func expandAnySubstitutions(v any) (any, error) {
	switch t := v.(type) {
	case string:
		out, err := expandSubstitutions(t)
		if err != nil {
			return nil, err
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			expanded, err := expandAnySubstitutions(e)
			if err != nil {
				return nil, err
			}
			out[k] = expanded
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			expanded, err := expandAnySubstitutions(e)
			if err != nil {
				return nil, err
			}
			out[i] = expanded
		}
		return out, nil
	default:
		return v, nil
	}
}

// expandSubstitutions evaluates $(env NAME [default]) in s.
func expandSubstitutions(s string) (string, error) {
	out := s
	for {
		start := strings.Index(out, "$(")
		if start < 0 {
			return out, nil
		}
		end := strings.Index(out[start:], ")")
		if end < 0 {
			return "", fmt.Errorf("launch: substitution in %q misses closing paren", s)
		}
		end += start
		expr := strings.TrimSpace(out[start+2 : end])
		replacement, err := evalSubstitutionExpr(expr)
		if err != nil {
			return "", fmt.Errorf("launch: substitution $(%s): %w", expr, err)
		}
		out = out[:start] + replacement + out[end+1:]
	}
}

// evalSubstitutionExpr evaluates one substitution expression.
func evalSubstitutionExpr(expr string) (string, error) {
	fields := strings.Fields(expr)
	if len(fields) == 0 {
		return "", errors.New("empty substitution") //nolint:wrapcheck // new error, nothing to wrap
	}
	if fields[0] != "env" {
		return "", fmt.Errorf("unsupported substitution %q, only $(env NAME [default]) is supported", fields[0])
	}
	if len(fields) < 2 {
		return "", errors.New("$(env) needs a variable name") //nolint:wrapcheck // new error, nothing to wrap
	}
	name := fields[1]
	value, ok := os.LookupEnv(name)
	if ok {
		return value, nil
	}
	if len(fields) > 2 {
		return strings.Join(fields[2:], " "), nil
	}
	return "", fmt.Errorf("environment variable %q is not set and has no default", name)
}
