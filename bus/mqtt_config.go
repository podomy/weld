// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package bus

import (
	"github.com/podomy/weld/internal/transports/mqtt"
)

// MqttTLS carries TLS settings for mqtts sessions.
//
// Empty means plain TCP. It is the type of MQTTConfig TLS so external
// callers can construct secure configs without importing internal paths.
type MqttTLS = mqtt.MqttTLS

// MqttWill carries the last will for ungraceful disconnects.
//
// Empty Topic disables the will. It is the type of MQTTConfig Will so
// external callers can construct liveliness wills without internal imports.
type MqttWill = mqtt.MqttWill
