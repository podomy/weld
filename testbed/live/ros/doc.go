// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package ros holds the ROS String round trip proof on domain 42.
//
// Tests drive the already running weld-jazzy peer with os/exec docker
// argv only and no shell. Forward checks weld echo sees ros2 topic pub
// on /chatter with std_msgs/String. Reverse checks ros2 topic echo sees
// weld pub on the same topic. Every payload carries a per-run live ID so
// stale runs never match. This package is separate from Cyclone SPDP
// checks: it asserts only String payloads and never decodes SPDP wire
// data and never parses trace files. Only one run per domain and topic
// at a time is supported.
package ros
