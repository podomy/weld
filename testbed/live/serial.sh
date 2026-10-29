#!/usr/bin/env bash
# Copyright (C) 2026 Podomy.
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# Serial runner for domain-42 ROS suites.
#
# Runs the testbed/live/ros package one step at a time to avoid the
# cross-talk seen when parallel shards share domain 42. Only one run per
# domain and topic at a time is supported, so this script never overlaps
# two go test invocations. Order is roundtrip, then type matrix splits,
# then QoS, then liveliness, then distro, then payload.
#
# The script never starts or stops the fleet. The fleet must already
# be running. Each live test calls SkipIfNoPeer first, so a stopped peer
# skips cleanly instead of failing. Each step prints the package plus the
# PASS or FAIL result for that step. The script runs all steps and exits
# non-zero when any step fails.
#
# Usage from the repo root:
#   ./testbed/live/serial.sh
#
# Coverage for testbed/live/ros (175 tests):
#   roundtrip covers Forward plus Reverse plus offline sanity
#     (BadDomainRejected, EmptyPayloadAllowed, DuplicateStaleDroppedNotFatal).
#   type splits cover all 148 type tests by message family.
#   QoS covers all TestROSQoS tests.
#   liveliness covers all TestROSLiveliness tests.
#   distro covers all TestROSDistroMatrix tests.
#   payload covers all TestROSLargePayload tests.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${ROOT}"

PKG="./testbed/live/ros"
GO_TAGS="live"
GO_TIMEOUT="60m"
SETTLE_SECS="2"

fails=0
total=0

run() {
  label="$1"
  pattern="$2"
  total=$((total + 1))
  echo "=== ${label}: ${PKG} -run ${pattern}"
  if go test -tags "${GO_TAGS}" -count=1 -parallel 1 -p 1 -timeout "${GO_TIMEOUT}" -run "${pattern}" "${PKG}"; then
    echo "PASS ${label}: ${PKG}"
  else
    echo "FAIL ${label}: ${PKG}"
    fails=$((fails + 1))
  fi
  sleep "${SETTLE_SECS}"
}

# 1. Roundtrip on domain 42 plus offline sanity (no peer needed for offline).
run "roundtrip" "^(TestROSForwardWeldEchoSeesROSTalker|TestROSReverseROSEchoSeesWeldPub|TestROSBadDomainRejected|TestROSEmptyPayloadAllowed|TestROSDuplicateStaleDroppedNotFatal)$"

# 2. Type matrix splits on domain 42, one family at a time.
run "type-std" "^(TestROSTypeStd)"
run "type-builtin" "^(TestROSTypeBuiltin)"
run "type-geometry" "^(TestROSTypeGeometry)"
run "type-sensor" "^(TestROSTypeSensor|TestROSPureGoSensorSetCameraInfo)"
run "type-nav" "^(TestROSTypeNav|TestROSNavExtraServices)"
run "type-shape" "^(TestROSTypeShape)"
run "type-trajectory" "^(TestROSTypeTrajectory)"
run "type-viz" "^(TestROSTypeVisualization|TestROSTypeViz|TestROSVizExtra)"
run "type-diag" "^(TestROSTypeDiag|TestROSDiag)"
run "type-stereo-misc" "^(TestROSTypeStereo|TestROSTypeMisc)"

# 3. QoS on domain 42.
run "qos" "^(TestROSQoS)"

# 4. Liveliness on ephemeral domains 65 through 69.
run "liveliness" "^(TestROSLiveliness)"

# 5. Distro matrix on domains 42 plus 43 plus 44.
run "distro" "^(TestROSDistroMatrix)"

# 6. Payload fragmentation on isolated domain 46.
run "payload" "^(TestROSLargePayload)"

echo "serial done: $((total - fails))/${total} steps passed, ${fails} failed"
if [ "${fails}" -ne 0 ]; then
  exit 1
fi
