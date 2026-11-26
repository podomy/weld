// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package mqtt_test holds the always compatible MQTT live gate.
//
// The gate proves the backend against a real mosquitto broker instead
// of the scripted shims in internal unit tests: data with type hash,
// retained discovery for late joiners, services on rq plus rr, TLS,
// authentication, and a broker restart under a QoS 1 publish. The
// broker ships in the testbed compose file beside the ROS fleet
// without touching it:
//
//	cd testbed && docker compose up -d mosquitto
//	go test -tags live -count=1 -timeout 10m ./testbed/live/mqtt/
//
// Every test skips cleanly when the broker container is absent, so the
// gate never fails a tree without the sidecar. Tests never run in
// parallel and each uses its own topics on one fixed domain, so one
// run per topic holds while the ROS suites run alongside.
package mqtt_test
