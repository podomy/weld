// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Command hook runs fast verification by default with a full live gate on request.
//
// Default (pre-commit): tidy, format, lint, vet, plus go test on all
// packages except testbed/live. Live suites need the fleet and dominate
// runtime, so they stay out of the commit path. Full gate runs the same
// steps plus the live matrix: go run ./check full or WELD_CHECK_FULL=1.
// The full test timeout matches the documented live budget because the ros
// live package exceeds the 10 minute go test default with the peer up.
// Bench stays smoke by default behind WELD_BENCH_FULL=1 in both modes.
//
// go mod tidy rewrites go.mod and go.sum in place, so the tidy step
// does modify the working tree; the git diff check right after it
// fails the run when tidy produces changes, keeping the tree
// reproducible. Lint, vet, and test scan the whole tree of each
// module (root, sdk, and cmd/weld), not just staged files, so the
// hook is safe with unrelated work in progress and with partial
// commits only in the sense that nothing is auto-fixed: formatters
// report diffs instead of rewriting and the header check fixes
// nothing. cmd/weld is its own module outside the workspace and runs
// with GOWORK=off. CI plus hook stay offline and live runs are manual via serial.sh with fleet up.
package main

import (
	"bytes"
	"context"
	"log"
	"os"
	"os/exec"
	"strings"
)

// runIn runs a fixed repo command, optionally in dir with extra env.
//
// The command is fixed by the caller, never user input.
func runIn(ctx context.Context, dir string, env []string, name string, arg ...string) {
	// #nosec G204 - hook runs fixed commands from the repo, not user input.
	cmd := exec.CommandContext(ctx, arg[0], arg[1:]...)
	if dir != "" {
		cmd.Dir = dir
	}

	if len(env) != 0 {
		cmd.Env = append(os.Environ(), env...)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("%s: %v", name, err)
	}
}

// step is one fixed repo command in the verification sequence.
type step struct {
	dir  string
	env  []string
	name string
	arg  []string
}

// isFullGate reports whether the full live matrix was requested.
//
// It returns true when argv holds full, --full, or -full, or when
// WELD_CHECK_FULL is 1. Default is the fast gate without testbed/live.
func isFullGate() bool {
	for _, arg := range os.Args[1:] {
		if arg == "full" || arg == "--full" || arg == "-full" {
			return true
		}
	}
	val := os.Getenv("WELD_CHECK_FULL")
	return val == "1"
}

// isLivePackage reports whether a package path belongs to the fleet live suites.
//
// Empty paths count as live so the fast filter drops them without a second branch.
func isLivePackage(pkg string) bool {
	if pkg == "" {
		return true
	}
	return strings.Contains(pkg, "testbed/live")
}

// filterLivePackages drops fleet live suites while keeping bench smoke and unit coverage.
func filterLivePackages(raw []string) []string {
	pkgs := make([]string, 0, len(raw))
	for _, pkg := range raw {
		pkg = strings.TrimSpace(pkg)
		if isLivePackage(pkg) {
			continue
		}
		pkgs = append(pkgs, pkg)
	}
	return pkgs
}

// runGoList returns the root module package list for the fast gate.
//
// It runs a fixed go list command and fails loudly on error.
func runGoList(ctx context.Context) string {
	// #nosec G204 - hook runs fixed commands from the repo, not user input.
	cmd := exec.CommandContext(ctx, "go", "list", "./...")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	if err != nil {
		log.Fatalf("go list fast packages: %v", err)
	}
	return out.String()
}

// listFastPackages returns root module packages excluding testbed/live.
//
// It lists via runGoList and filters live suites so the fast gate skips
// fleet suites while keeping bench smoke and unit coverage. It fails
// loudly when listing yields no packages.
func listFastPackages(ctx context.Context) []string {
	raw := strings.Split(strings.TrimSpace(runGoList(ctx)), "\n")
	pkgs := filterLivePackages(raw)
	if len(pkgs) == 0 {
		log.Fatalf("go list fast packages: no packages after live filter")
	}
	return pkgs
}

// testTimeoutArgs returns the go test timeout flag for the selected gate.
//
// Full keeps the 60m live budget. Fast uses 10m for smoke plus units.
func testTimeoutArgs(full bool) []string {
	if full {
		return []string{"-timeout", "60m"}
	}
	return []string{"-timeout", "10m"}
}

// moduleTestArgs builds a go test argv for one module without live filtering.
//
// Sdk and cmd/weld hold no live suites, so fast and full differ only in timeout.
func moduleTestArgs(full bool) []string {
	arg := []string{"go", "test"}
	arg = append(arg, testTimeoutArgs(full)...)
	return append(arg, "./...")
}

// rootTestArgs builds the root go test argv with live filtering in fast mode.
func rootTestArgs(ctx context.Context, full bool) []string {
	if full {
		return moduleTestArgs(true)
	}
	arg := []string{"go", "test"}
	arg = append(arg, testTimeoutArgs(false)...)
	return append(arg, listFastPackages(ctx)...)
}

// main runs the pre-commit verification steps.
func main() {
	ctx := context.Background()

	goworkOff := []string{"GOWORK=off"}
	full := isFullGate()

	rootTestArg := rootTestArgs(ctx, full)
	sdkTestArg := moduleTestArgs(full)
	weldTestArg := moduleTestArgs(full)

	steps := []step{
		{dir: "", env: nil, name: "go mod tidy (root)", arg: []string{"go", "mod", "tidy"}},
		{dir: "sdk", env: nil, name: "go mod tidy (sdk)", arg: []string{"go", "mod", "tidy"}},
		{dir: "cmd/weld", env: goworkOff, name: "go mod tidy (cmd/weld)", arg: []string{"go", "mod", "tidy"}},
		{dir: "", env: nil, name: "git diff --exit-code modules", arg: []string{"git", "diff", "--exit-code", "--", "go.mod", "go.sum", "sdk/go.mod", "sdk/go.sum", "cmd/weld/go.mod", "cmd/weld/go.sum", "go.work", "go.work.sum"}},
		{dir: "", env: nil, name: "golangci-lint fmt --diff", arg: []string{"golangci-lint", "fmt", "--diff", "./..."}},
		{dir: "sdk", env: nil, name: "golangci-lint fmt --diff (sdk)", arg: []string{"golangci-lint", "fmt", "--diff", "./..."}},
		{dir: "cmd/weld", env: goworkOff, name: "golangci-lint fmt --diff (cmd/weld)", arg: []string{"golangci-lint", "fmt", "--diff", "./..."}},
		{dir: "", env: nil, name: "golangci-lint header check", arg: []string{"golangci-lint", "run", "--enable-only=goheader", "./..."}},
		{dir: "sdk", env: nil, name: "golangci-lint header check (sdk)", arg: []string{"golangci-lint", "run", "--enable-only=goheader", "./..."}},
		{dir: "cmd/weld", env: goworkOff, name: "golangci-lint header check (cmd/weld)", arg: []string{"golangci-lint", "run", "--enable-only=goheader", "./..."}},
		{dir: "", env: nil, name: "golangci-lint", arg: []string{"golangci-lint", "run", "./..."}},
		{dir: "sdk", env: nil, name: "golangci-lint (sdk)", arg: []string{"golangci-lint", "run", "./..."}},
		{dir: "cmd/weld", env: goworkOff, name: "golangci-lint (cmd/weld)", arg: []string{"golangci-lint", "run", "./..."}},
		{dir: "", env: nil, name: "go vet", arg: []string{"go", "vet", "./..."}},
		{dir: "sdk", env: nil, name: "go vet (sdk)", arg: []string{"go", "vet", "./..."}},
		{dir: "cmd/weld", env: goworkOff, name: "go vet (cmd/weld)", arg: []string{"go", "vet", "./..."}},
		{dir: "", env: nil, name: "go test", arg: rootTestArg},
		{dir: "sdk", env: nil, name: "go test (sdk)", arg: sdkTestArg},
		{dir: "cmd/weld", env: goworkOff, name: "go test (cmd/weld)", arg: weldTestArg},
	}

	for i := range steps {
		runIn(ctx, steps[i].dir, steps[i].env, steps[i].name, steps[i].arg...)
	}
}
