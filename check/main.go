// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Command hook runs the pre-commit verification: tidy, format, lint, and test.
//
// Every step is check-only and never modifies the working tree, so the
// hook is safe with unrelated work in progress and with partial commits:
// formatters report diffs instead of rewriting, the header check fixes
// nothing, and go mod tidy must leave go.mod, go.sum, and go.work clean.
// The test timeout matches the documented live budget because the ros
// live package exceeds the 10 minute go test default with the peer up.
package main

import (
	"context"
	"log"
	"os"
	"os/exec"
)

func run(ctx context.Context, name string, arg ...string) {
	// #nosec G204 - hook runs fixed commands from the repo, not user input.
	cmd := exec.CommandContext(ctx, arg[0], arg[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("%s: %v", name, err)
	}
}

// main runs the pre-commit verification steps.
func main() {
	ctx := context.Background()

	run(ctx, "go mod tidy", "go", "mod", "tidy")
	run(ctx, "git diff --exit-code go.mod go.sum go.work go.work.sum", "git", "diff", "--exit-code", "--", "go.mod", "go.sum", "go.work", "go.work.sum")
	run(ctx, "golangci-lint fmt --diff", "golangci-lint", "fmt", "--diff", "./...")
	run(ctx, "golangci-lint header check", "golangci-lint", "run", "--enable-only=goheader", "./...")
	run(ctx, "golangci-lint", "golangci-lint", "run", "./...")
	run(ctx, "go test", "go", "test", "-timeout", "60m", "./...")
}
