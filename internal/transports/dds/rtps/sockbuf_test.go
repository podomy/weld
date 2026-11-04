// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package rtps

import (
	"net"
	"testing"
)

// TestTuneReceiveBufferRequestsLargeBuffers pins the burst-loss guard: a
// fresh socket gets an 8 MiB request and the verified effective size reads
// back at least the megabyte OS default, so 1 MiB frag bursts fit without
// kernel drops. tuneReceiveBuffer itself must never fail the bind path.
func TestTuneReceiveBufferRequestsLargeBuffers(t *testing.T) {
	t.Parallel()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	tuneReceiveBuffer(conn, "test")
	got, err := readReceiveBuffer(conn)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got/2 < 1<<20 {
		t.Fatalf("effective buffer %d below 1 MiB default", got/2)
	}
	tuneReceiveBuffer(nil, "test-nil")
}
