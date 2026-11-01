// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package types

// Synthetic keyed descriptors for keyed topic testing.
//
// These are test helpers compiled into the non-test package so the
// cross-package keyed_test in internal/transports/dds can use them: Go
// test files cannot be imported across packages, so the helpers live here
// instead of a _test file by design. They are not ROS standard types, they
// never enter the global registry, and each constructor returns fresh
// pointers so tests never share mutable tables.
//
// Shapes:
//   - Keyed flat: weld_test/msg/Keyed with uint32 seq plus string label
//     plus int32 count, where seq and count are keys.
//   - Keyed nested: weld_test/msg/KeyedNested with uint8 tag plus an inner
//     struct plus uint16 tail, where tag, tail, and the inner code leaf
//     are keys. The inner member itself is not flagged: key scope for
//     nested types comes from the nested key leaves.
func KeyedTestMembers() *MessageMembers {
	members := []MessageMember{
		NewScalarMember("seq", RosTypeUint32, 0),
		NewStringMember("label", RosTypeString, 0, 4),
		NewScalarMember("count", RosTypeInt32, 8),
	}
	members[0].IsKey = true
	members[2].IsKey = true
	return NewMessageMembers("weld_test", "Keyed", 16, members, keyedTestInit, keyedTestFini)
}

// KeyedTestInnerMembers returns the nested leaf table for KeyedNested.
//
// The int16 code field is the only key. The table is attached directly to
// the outer nested member, so no registry lookup is needed.
func KeyedTestInnerMembers() *MessageMembers {
	members := []MessageMember{
		NewScalarMember("code", RosTypeInt16, 0),
		NewScalarMember("value", RosTypeInt32, 4),
	}
	members[0].IsKey = true
	return NewMessageMembers("weld_test", "KeyedInner", 8, members, keyedTestInit, keyedTestFini)
}

// KeyedTestNestedMembers returns the outer table for the nested key shape.
//
// Tag and tail are scalar keys. The inner member carries the nested table
// by pointer and stays unflagged, proving leaf scoped key collection.
func KeyedTestNestedMembers() *MessageMembers {
	inner := NewNestedMember("inner", 4, "Inner", "weld_test", "KeyedInner")
	inner.Members = KeyedTestInnerMembers()
	members := []MessageMember{
		NewScalarMember("tag", RosTypeUint8, 0),
		inner,
		NewScalarMember("tail", RosTypeUint16, 8),
	}
	members[0].IsKey = true
	members[2].IsKey = true
	return NewMessageMembers("weld_test", "KeyedNested", 12, members, keyedTestInit, keyedTestFini)
}

// keyedTestInit is the no-op init for synthetic keyed descriptors.
func keyedTestInit(_ any, _ MessageInit) error {
	return nil
}

// keyedTestFini is the no-op fini for synthetic keyed descriptors.
func keyedTestFini(_ any) error {
	return nil
}
