// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"errors"
	"testing"
)

// TestRunClosersFirstError pins that teardown returns the first error while
// still running every closer.
func TestRunClosersFirstError(t *testing.T) {
	firstErr := newTestBoomError("first")
	secondErr := newTestBoomError("second")
	ran := 0
	err := runClosers([]func() error{
		func() error {
			ran++
			return firstErr
		},
		func() error {
			ran++
			return secondErr
		},
		func() error {
			ran++
			return nil
		},
	})
	if err == nil {
		t.Fatalf("runClosers must fail")
	}
	if !errors.Is(err, firstErr) {
		t.Fatalf("first error = %v, want first", err)
	}
	if ran != 3 {
		t.Fatalf("ran = %d, want 3", ran)
	}
	err = runClosers(nil)
	if err != nil {
		t.Fatalf("empty closers: %v", err)
	}
}

// newTestBoomError returns a test error.
func newTestBoomError(label string) error {
	return testBoomError("sdk: test " + label + " boom")
}

// testBoomError is a string error for closer tests.
type testBoomError string

// Error returns the string error text.
func (e testBoomError) Error() string {
	return string(e)
}
