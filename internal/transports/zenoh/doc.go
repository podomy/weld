// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package zenoh holds the rmw_zenoh backend skeleton.
//
// One Zenoh session runs per ROS context. Domains isolate by key prefix
// and types plus hashes isolate within a domain. The reserved prefix
// @ros2_lv stays hermetic and wildcards never match it. Liveliness
// tokens track entities and attachments carry sequence plus timestamp
// plus source GID.
//
// Network I/O is stubbed until the Zenoh client library lands. Pure
// key, naming, and attachment helpers work now. Session, publish, and
// take paths report ErrZenohNotImplemented after lifecycle checks so
// interface conformance plus unit tests stay green.
package zenoh
