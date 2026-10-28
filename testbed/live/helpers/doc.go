// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package helpers holds shared docker argv and live test helpers.
//
// Tests use only os/exec with docker argv and no shell, with environment
// passed via -e flags and container checks via docker inspect. The peer
// container is assumed already running and never started here. Every
// docker and weld invocation uses a context timeout. Tests skip cleanly
// when no daemon or peer is present.
//
// Canonical shared helpers live here and nowhere else: LiveID and LiveTopic
// for per-run isolation, DockerArgs and DockerDetachedArgs for the Jazzy
// peer argv, DistroDockerArgs and DistroDetachedArgs for the distro matrix
// argv, SkipIfNoPeer and SkipIfNoDistroPeer for gate entry, and
// MustNewPublisher for the shared publisher harness.
// Skip budget in skip_budget.go tracks allowlisted product skips by test
// name with reason and expiry. SkipIfNoPeer stays exempt as environment.
package helpers
