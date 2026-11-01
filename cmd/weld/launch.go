// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/launch"
)

// stringSliceFlag collects repeatable string flags in order.
type stringSliceFlag struct {
	values []string
}

// String returns the joined values for flag display.
func (s *stringSliceFlag) String() string {
	return strings.Join(s.values, ",")
}

// Set appends one flag occurrence.
func (s *stringSliceFlag) Set(v string) error {
	s.values = append(s.values, v)
	return nil
}

// prepareLaunchForwarded parses flags and forwards args, resolving the FQN.
func prepareLaunchForwarded(left, right []string, stderr io.Writer) (launch.NodeOptions, launchFlagConfig, string, int, bool) {
	cfg, cfgErr := parseLaunchFlags(left)
	if cfgErr != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(cfgErr)) //nolint:errcheck // final error output is best effort
		return launch.NodeOptions{}, launchFlagConfig{}, "", 2, false
	}
	base, err := launch.NewNodeOptions(cfg.node, cfg.namespace, cfg.node == "")
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return launch.NodeOptions{}, launchFlagConfig{}, "", 2, false
	}
	base.ParamsFiles = append([]string{}, cfg.paramsFiles...)
	forwarded, err := launch.ForwardArgs(base, right)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return launch.NodeOptions{}, launchFlagConfig{}, "", 2, false
	}
	if strings.TrimSpace(forwarded.NodeName) == "" {
		_, _ = fmt.Fprintln(stderr, "weld: launch needs --node or -r __node:=name") //nolint:errcheck // final error output is best effort
		return launch.NodeOptions{}, launchFlagConfig{}, "", 2, false
	}
	fqn, err := launch.ResolveNodeFQN(forwarded.NodeName, forwarded.Namespace)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return launch.NodeOptions{}, launchFlagConfig{}, "", 2, false
	}
	return forwarded, cfg, fqn, 0, true
}

// buildLaunchFinal resolves params, rewrites, and appends SDF.
func buildLaunchFinal(forwarded launch.NodeOptions, cfg launchFlagConfig, stderr io.Writer) (map[string]core.ParamValue, string, int, bool) {
	rewrites, err := parseRewriteFlags(cfg.rewrites)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return nil, "", 2, false
	}
	resolved, err := resolveLaunchParams(forwarded)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return nil, "", 1, false
	}
	final, err := launch.RewriteParamsForNamespace(resolved, forwarded.Namespace, rewrites, cfg.autostart)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return nil, "", 2, false
	}
	sdfPath := strings.TrimSpace(cfg.sdf)
	if sdfPath != "" {
		validated, err := launch.AppendSDFParams(forwarded, sdfPath)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
			return nil, "", 2, false
		}
		final[launch.SimTimeParam] = validated.Params[launch.SimTimeParam]
	}
	return final, sdfPath, 0, true
}

// runLaunchCommand resolves launch arguments and prints node config.
//
// Left of --ros-args carries launch flags (--node, --namespace, --sdf,
// --autostart, repeatable --params-file and --rewrite as k:=v). Right of
// --ros-args forwards with launch.ForwardArgs where CLI argv appends to
// node options with last wins. Files plus CLI resolve with FQN over
// wildcard, rewrites apply per namespace, SDF appends with sim time forced
// on, and the canonical dump shares form with ros2 param dump.
// A cancelled context terminates with code 1 like params; the 1 here is
// pinned by TestRunLaunchUsage so launch cancel stays loud.
func runLaunchCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	err := ctx.Err()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	if isHelpRequest(args) {
		printLaunchUsage(stdout)
		return 0
	}
	left, right := splitRosArgs(args)
	forwarded, cfg, fqn, code, ok := prepareLaunchForwarded(left, right, stderr)
	if !ok {
		return code
	}
	final, sdfPath, code, ok := buildLaunchFinal(forwarded, cfg, stderr)
	if !ok {
		return code
	}
	dump, err := launch.CanonicalDump(fqn, final)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	err = ctx.Err()
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}
	printLaunchResult(stdout, fqn, forwarded.Remaps, dump, sdfPath)
	return 0
}

// launchFlagConfig holds parsed left side launch flags.
type launchFlagConfig struct {
	node        string
	namespace   string
	sdf         string
	autostart   bool
	paramsFiles []string
	rewrites    []string
}

// parseLaunchFlags parses flags left of --ros-args.
func parseLaunchFlags(left []string) (launchFlagConfig, error) {
	fs := flag.NewFlagSet("launch", flag.ContinueOnError)
	var cfg launchFlagConfig
	var files stringSliceFlag
	var rewrites stringSliceFlag
	fs.StringVar(&cfg.node, "node", "", "node base name, or supply -r __node:=name after --ros-args")
	fs.StringVar(&cfg.namespace, "namespace", "", "node namespace, absolute resolution, default /")
	fs.StringVar(&cfg.sdf, "sdf", "", "SDF file appended as node args with sim time forced on")
	fs.BoolVar(&cfg.autostart, "autostart", false, "force autostart true through namespace rewrites")
	fs.Var(&files, "params-file", "params YAML file, repeatable")
	fs.Var(&rewrites, "rewrite", "param rewrite k:=v, repeatable")
	err := fs.Parse(left)
	if err != nil {
		return launchFlagConfig{}, fmt.Errorf("weld: launch flags: %w", err)
	}
	if fs.NArg() != 0 {
		return launchFlagConfig{}, fmt.Errorf("weld: launch takes no positional args, have %d", fs.NArg())
	}
	cfg.paramsFiles = append([]string{}, files.values...)
	cfg.rewrites = append([]string{}, rewrites.values...)
	return cfg, nil
}

// splitRosArgs splits args at the first --ros-args marker.
func splitRosArgs(args []string) ([]string, []string) {
	for i, a := range args {
		if a == "--ros-args" {
			return args[:i], args[i:]
		}
	}
	return args, []string{}
}

// parseRewriteFlags splits repeatable k:=v rewrite flags.
func parseRewriteFlags(flags []string) (map[string]string, error) {
	out := make(map[string]string, len(flags))
	for _, f := range flags {
		key, value, ok := strings.Cut(f, ":=")
		if !ok {
			return nil, fmt.Errorf("weld: rewrite %q misses :=", f)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			return nil, fmt.Errorf("weld: rewrite %q needs non-empty key and value", f)
		}
		out[key] = value
	}
	return out, nil
}

// resolveLaunchParams loads YAML files and merges files plus CLI.
func resolveLaunchParams(opts launch.NodeOptions) (map[string]core.ParamValue, error) {
	fqn, err := launch.ResolveNodeFQN(opts.NodeName, opts.Namespace)
	if err != nil {
		return nil, fmt.Errorf("weld: resolve node %q: %w", opts.NodeName, err)
	}
	var sections []map[string]map[string]core.ParamValue
	for _, path := range opts.ParamsFiles {
		loaded, err := launch.LoadParamsFile(path)
		if err != nil {
			return nil, fmt.Errorf("weld: load params file %q: %w", path, err)
		}
		sections = append(sections, loaded)
	}
	merged := launch.MergeFileSections(sections)
	resolved := launch.ResolveNodeParams(fqn, merged, opts.Params, nil)
	return resolved, nil
}

// printLaunchResult writes FQN, remaps, dump, SDF note, and log dir.
func printLaunchResult(stdout io.Writer, fqn string, remaps []launch.RemapRule, dump, sdfPath string) {
	_, _ = fmt.Fprintln(stdout, "node: "+fqn)               //nolint:errcheck // config output failure is not actionable
	_, _ = fmt.Fprintf(stdout, "remaps: %d\n", len(remaps)) //nolint:errcheck // config output failure is not actionable
	sorted := append([]launch.RemapRule{}, remaps...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].From == sorted[j].From {
			return sorted[i].To < sorted[j].To
		}
		return sorted[i].From < sorted[j].From
	})
	for _, r := range sorted {
		_, _ = fmt.Fprintln(stdout, "  "+r.From+" -> "+r.To) //nolint:errcheck // config output failure is not actionable
	}
	_, _ = fmt.Fprint(stdout, dump) //nolint:errcheck // config output failure is not actionable
	if strings.TrimSpace(sdfPath) != "" {
		_, _ = fmt.Fprintln(stdout, "sdf: "+strings.TrimSpace(sdfPath)) //nolint:errcheck // config output failure is not actionable
	}
	if dir, ok := launch.ResolveLogDir(); ok {
		_, _ = fmt.Fprintln(stdout, "log_dir: "+dir) //nolint:errcheck // config output failure is not actionable
	}
}

// printLaunchUsage writes launch help.
func printLaunchUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, "Usage: weld launch --node NAME [--namespace /ns] [--params-file f.yaml] [--sdf robot.sdf] [--rewrite k:=v] [--autostart] [--ros-args -r __node:=n -r __ns:=/ns -r from:=to -p k:=v --params-file g.yaml --]\n") //nolint:errcheck // help output failure is not actionable
}
