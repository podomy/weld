// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

import (
	"fmt"
	"os"
)

// sedpTraceEnabled reports whether SEDP ingest tracing is enabled.
//
// Tracing is gated behind WELD_SEDP_TRACE and is off by default. When
// unset or empty this returns false and all trace call sites are no-ops
// with no behavior change.
func sedpTraceEnabled() bool {
	return os.Getenv("WELD_SEDP_TRACE") != ""
}

// sedpTracef emits one SEDP ingest trace line to stderr when enabled.
//
// It is a no-op when WELD_SEDP_TRACE is unset. All ingest decision points
// log through here so live interop runs can pinpoint the exact drop point
// without changing behavior when the variable is unset.
func sedpTracef(format string, args ...any) {
	if !sedpTraceEnabled() {
		return
	}

	_, _ = fmt.Fprintf(os.Stderr, "weld-sedp-trace: "+format+"\n", args...) //nolint:errcheck // diagnostics are best effort
}
