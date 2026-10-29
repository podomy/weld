// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop // test breadth
package gates_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/podomy/weld/testbed/live/helpers"
)

// TestGateFilesPresent reports missing Go gate files.
//
// It checks the canonical Go gate files plus the compose file. Paths resolve
// from the repo root so the test passes regardless of working directory.
// It uses no DDS domain and no process env, so it is safe for future
// t.Parallel workers alongside allocated exclusive domains.
func TestGateFilesPresent(t *testing.T) {
	root := helpers.RepoRoot(t)
	files := []string{
		filepath.Join(root, "testbed", "live", "gates", "gate0_test.go"),
		filepath.Join(root, "testbed", "live", "gates", "gate1_test.go"),
		filepath.Join(root, "testbed", "live", "gates", "gate2_test.go"),
		filepath.Join(root, "testbed", "docker-compose.yml"),
	}

	for _, path := range files {
		_, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
	}

	ignore := filepath.Join(root, "testbed", "logs", ".gitignore")
	_, err := os.Stat(ignore)
	if err != nil {
		t.Fatalf("stat logs .gitignore: %v", err)
	}
}
