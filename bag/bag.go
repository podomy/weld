// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package bag is the public bag exchange for info and replay.
package bag

import (
	"fmt"

	ibags "github.com/podomy/weld/internal/bags"
	"github.com/podomy/weld/internal/types"
)

// Bag holds the in memory exchange for info and replay.
type Bag = ibags.Bag

// Topic carries one topic record with id plus name plus type plus format.
type Topic = ibags.Topic

// Message carries one serialized bag message.
type Message = ibags.Message

// Info summarizes one bag in deterministic topic order.
type Info = ibags.Info

// TopicInfo summarizes one topic for info output.
type TopicInfo = ibags.TopicInfo

// Metrics counts dropped inputs for bag parsing.
type Metrics = ibags.Metrics

// Registry stores individuals plus sources plus full descriptions by name and hash.
//
// It is identical to msg.Registry. Bag.Append and Bag.Verify accept this
// type, so external callers name it through bag.Registry or msg.Registry
// and obtain a value from msg.NewRegistry. No internal import is required.
type Registry = types.Registry

// Storage ids for the v1 bag exchange contract.
const (
	// StorageSQLite3 is the sqlite3 storage id from the bag contract.
	StorageSQLite3 = ibags.StorageSQLite3
	// StorageMCAP is the mcap storage id from the bag contract.
	StorageMCAP = ibags.StorageMCAP
)

// New returns an empty bag for the given storage id.
func New(storage string) (*Bag, error) {
	b, err := ibags.New(storage)
	if err != nil {
		return nil, fmt.Errorf("bag: new: %w", err)
	}

	return b, nil
}

// Parse decodes one exchange buffer with bounds checks.
//
// Bounds are enforced inside internal/bags and are documented here for
// discoverability: topic count at most 65536, message count at most
// 1048576 (1<<20), and each message payload at most 32 MiB (32<<20).
// Smaller framing caps also apply to storage id, topic and type names,
// format, hash, QoS entries, string fields, and decode depth. Over cap
// or truncated input records one drop in m and returns an error. There
// is no public tuning knob for these caps in v1.
func Parse(data []byte, m *Metrics) (*Bag, error) {
	b, err := ibags.Parse(data, m)
	if err != nil {
		return nil, fmt.Errorf("bag: parse: %w", err)
	}

	return b, nil
}

// Marshal encodes the bag in deterministic replay order.
func Marshal(b *Bag) ([]byte, error) {
	raw, err := b.Marshal()
	if err != nil {
		return nil, fmt.Errorf("bag: marshal: %w", err)
	}

	return raw, nil
}
