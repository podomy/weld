// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package mqtt implements an MQTT backend for transports.Backend.
//
// The package hides the real broker behind the small Conn interface. The
// default in-memory channel implementation carries traffic without a broker
// so contract tests run with no external dependencies; Dial plus netConn
// provide the live broker session. No cgo is used.
package mqtt
