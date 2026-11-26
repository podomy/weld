// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package mqtt

import (
	"testing"
)

// fuzzSeeds covers empty, every fixed header nibble, valid v5
// PUBLISH, CONNACK, SUBACK, UNSUBACK, overlong varints,
// truncated bodies, plus string and property shapes.
var fuzzSeeds = [][]byte{
	{},
	{0x10},
	{0x20},
	{0x30},
	{0x40},
	{0x50},
	{0x60},
	{0x70},
	{0x80},
	{0x90},
	{0xA0},
	{0xB0},
	{0xC0},
	{0xD0},
	{0xE0},
	{0xF0},
	{0x30, 0x04, 0x00, 0x01, 0x61, 0x00},
	{0x20, 0x03, 0x00, 0x00, 0x00},
	{0x90, 0x04, 0x00, 0x09, 0x00, 0x00},
	{0xB0, 0x04, 0x00, 0x21, 0x00, 0x00},
	{0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x01},
	{0x20, 0x80, 0x80, 0x80, 0x80, 0x00},
	{0xFF, 0xFF, 0xFF, 0xFF, 0x01},
	{0x30, 0x05, 0x00},
	{0x20, 0x03, 0x00},
	{0x90, 0x02, 0x00},
	{0x00, 0x03, 0x61, 0x62, 0x63},
	{0x00, 0x02, 0x61, 0x00},
	{0x00, 0x02, 0xFF, 0xFE},
	{0x00, 0x05, 0x41, 0x42},
	{0x00},
	{0x07, 0x26, 0x00, 0x01, 0x61, 0x00, 0x01, 0x62},
	{0x02, 0x01, 0x00},
	{0x05, 0x26, 0x00},
}

// FuzzDecodePublish checks DecodePublish never panics.
func FuzzDecodePublish(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, d []byte) {
		if len(d) < 2 {
			return
		}
		v := Version5
		if d[0]&1 == 1 {
			v = Version311
		}
		if _, _, err := DecodePublish(v, d); err != nil {
			return
		}
	})
}

// FuzzDecodeConnAck checks DecodeConnAck never panics.
func FuzzDecodeConnAck(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, d []byte) {
		if len(d) < 2 {
			return
		}
		v := Version5
		if d[0]&1 == 1 {
			v = Version311
		}
		if _, err := DecodeConnAck(v, d); err != nil {
			return
		}
	})
}

// FuzzDecodeUnsubscribe checks DecodeUnsubscribe never panics.
func FuzzDecodeUnsubscribe(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, d []byte) {
		if len(d) < 2 {
			return
		}
		v := Version5
		if d[0]&1 == 1 {
			v = Version311
		}
		if _, _, err := DecodeUnsubscribe(v, d); err != nil {
			return
		}
	})
}

// FuzzCheckAckResult checks checkAckResult never panics.
func FuzzCheckAckResult(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, d []byte) {
		if len(d) < 2 {
			return
		}
		v := Version5
		if d[0]&1 == 1 {
			v = Version311
		}
		if err := checkAckResult(v, d); err != nil {
			return
		}
	})
}

// FuzzDecodeString checks decodeString never panics.
func FuzzDecodeString(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, d []byte) {
		if len(d) < 2 {
			return
		}
		if _, _, err := decodeString(d); err != nil {
			return
		}
	})
}

// FuzzDecodeProperties checks decodeProperties never panics.
func FuzzDecodeProperties(f *testing.F) {
	for _, s := range fuzzSeeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, d []byte) {
		if len(d) < 2 {
			return
		}
		if _, _, err := decodeProperties(d); err != nil {
			return
		}
	})
}
