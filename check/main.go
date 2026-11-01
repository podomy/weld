// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Command hook runs the pre-commit verification: tidy, format, lint, and test.
//
// go mod tidy rewrites go.mod and go.sum in place, so the tidy step
// does modify the working tree; the git diff check right after it
// fails the run when tidy produces changes, keeping the tree
// reproducible. Lint, vet, and test scan the whole tree of each
// module (root, sdk, and cmd/weld), not just staged files, so the
// hook is safe with unrelated work in progress and with partial
// commits only in the sense that nothing is auto-fixed: formatters
// report diffs instead of rewriting and the header check fixes
// nothing. The test timeout matches the documented live budget because
// the ros live package exceeds the 10 minute go test default with the
// peer up. cmd/weld is its own module outside the workspace and runs
// with GOWORK=off.
package main

import (
	"context"
	"log"
	"os"
	"os/exec"
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

// main runs the pre-commit verification steps.
func main() {
	ctx := context.Background()

	goworkOff := []string{"GOWORK=off"}
	testTimeout := []string{"-timeout", "60m"}

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
		{dir: "", env: nil, name: "go test", arg: append([]string{"go", "test"}, append(testTimeout, "./...")...)},
		{dir: "sdk", env: nil, name: "go test (sdk)", arg: append([]string{"go", "test"}, append(testTimeout, "./...")...)},
		{dir: "cmd/weld", env: goworkOff, name: "go test (cmd/weld)", arg: append([]string{"go", "test"}, append(testTimeout, "./...")...)},
	}

	for i := range steps {
		runIn(ctx, steps[i].dir, steps[i].env, steps[i].name, steps[i].arg...)
	}
}
