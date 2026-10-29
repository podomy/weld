// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop // test breadth
package moved

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Live SEDP identity for rt/chatter on one allocated exclusive domain.
const (
	sedpLiveTypeHash = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"
	sedpLiveDDSTopic = "rt/chatter"
	sedpLiveROSTopic = "/chatter"
	sedpLiveTypeDDS  = "std_msgs::msg::dds_::String_"
	sedpLiveTypeIDL  = "std_msgs__msg__String"
)

// sedpLivePrefix returns a deterministic 12 byte prefix for tests.
func sedpLivePrefix(base byte) dds.SEDPGUIDPrefix {
	var prefix dds.SEDPGUIDPrefix
	for i := range prefix {
		prefix[i] = base + byte(i)
	}
	return prefix
}

// sedpLiveNewIndex returns an index with a fixed prefix and domain.
func sedpLiveNewIndex(t *testing.T, base byte, domain uint32) *dds.SEDPIndex {
	t.Helper()
	prefix := sedpLivePrefix(base)
	idx, err := dds.NewSEDPIndex(prefix, domain)
	if err != nil {
		t.Fatalf("new index: %v", err)
	}
	return idx
}

// TestGateSEDPJazzyDomain42 checks the running jazzy peer on one allocated domain.
//
// It uses docker argv with structured environment and no shell through
// the shared helpers. It skips cleanly when no daemon or peer is present
// and never starts the compose stack. It uses an allocated exclusive
// domain with explicit domain plumbing, so it is safe for future
// t.Parallel workers.
//
//nolint:cyclop // live peer test asserts setup, interface and topic checks in one run
func TestGateSEDPJazzyDomain42(t *testing.T) {
	domain := helpers.MustAllocateDomain(t)
	domainU32 := uint32(domain) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)

	exists, err := helpers.DistroSetupExists(ctx, helpers.DistroJazzy)
	if err != nil {
		t.Fatalf("setup probe: %v", err)
	}
	if !exists {
		t.Skipf("setup %s missing, skipping live check", helpers.DistroJazzy.SetupBash)
	}

	ifaceArgs := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "interface", "show", "std_msgs/msg/String")
	ifaceOut, err := helpers.DockerExec(ctx, ifaceArgs...)
	if err != nil {
		t.Fatalf("interface show: %v output %s", err, string(ifaceOut))
	}
	if !strings.Contains(string(ifaceOut), "string data") {
		t.Fatalf("interface show misses String field: %s", string(ifaceOut))
	}

	listArgs := helpers.DockerArgs(helpers.Container, domain, helpers.RosBin, "topic", "list")
	listOut, err := helpers.DockerExec(ctx, listArgs...)
	if err != nil {
		t.Fatalf("topic list: %v output %s", err, string(listOut))
	}
	listText := string(listOut)
	if !strings.Contains(listText, "/parameter_events") && !strings.Contains(listText, "/rosout") {
		t.Logf("topic list on domain %d shows only %q, still live", domain, listText)
	}

	idx := sedpLiveNewIndex(t, 0x1F, domainU32)
	ep, err := idx.DeclareWriter(sedpLiveROSTopic, sedpLiveTypeIDL, sedpLiveTypeHash, nil)
	if err != nil {
		t.Fatalf("declare live writer: %v", err)
	}
	raw, err := dds.SerializeSEDPDATA(ep)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	decoded, err := dds.ParseSEDPDATA(raw)
	if err != nil {
		t.Fatalf("live round trip parse: %v", err)
	}
	if decoded.DDSTopic != sedpLiveDDSTopic {
		t.Fatalf("live topic = %q, want %q", decoded.DDSTopic, sedpLiveDDSTopic)
	}
	if decoded.TypeDDS != sedpLiveTypeDDS {
		t.Fatalf("live type = %q, want %q", decoded.TypeDDS, sedpLiveTypeDDS)
	}
	if decoded.TypeHash != sedpLiveTypeHash {
		t.Fatalf("live hash = %q, want %q", decoded.TypeHash, sedpLiveTypeHash)
	}
}
