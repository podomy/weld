// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package spdp holds SPDP conformance tests for Weld first contact.
//
// Tests run pure Go against public rtps and dds APIs with no Docker.
// They pin port mapping, LAN advertisement, RTPS framing, UserData
// padding, locators, domain tag, lease, builtin endpoints, GUID order,
// and multi port routes across domains 0, 1, 42, 48, and 101.
package spdp
