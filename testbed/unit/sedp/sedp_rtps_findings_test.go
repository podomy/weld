// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

//nolint:cyclop // findings file holds many sequential fail-closed assertions, high average reflects breadth
package unit_sedp

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// sedpFindingsFeedTopics feeds one datagram through ListenLoop and stops.
func sedpFindingsFeedTopics(t *testing.T, local *dds.SEDPIndex, datagram []byte) {
	t.Helper()

	rx := &sedpConformQueueReceiver{queue: [][]byte{datagram}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- local.ListenLoop(ctx, rx)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for local.Generation() == 0 && len(local.RemoteTopics()) == 0 && time.Now().Before(deadline) {
	}
	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("listen loop err = %v, want cancel", err)
	}
}

// TestSEDPFindingsTopicsOnlyInstallsTopic pins Topics-only install without endpoints.
func TestSEDPFindingsTopicsOnlyInstallsTopic(t *testing.T) {
	t.Parallel()

	peer := sedpConformNewIndex(t, 0x41, sedpConformDomain)
	writer := sedpConformDeclareWriter(t, peer, sedpConformTypeHash)
	topicsRaw, err := dds.SerializeSEDPTopicsDATA(writer.ID.Prefix, dds.SEDPTopic{
		DDSTopic: sedpConformDDSTopic,
		TypeDDS:  sedpConformTypeDDS,
	})
	if err != nil {
		t.Fatalf("serialize topics: %v", err)
	}

	local := sedpConformNewIndex(t, 0x42, sedpConformDomain)
	sedpFindingsFeedTopics(t, local, topicsRaw)

	topics := local.RemoteTopics()
	if len(topics) != 1 {
		t.Fatalf("remote topics = %d, want 1", len(topics))
	}
	if topics[0].DDSTopic != sedpConformDDSTopic || topics[0].TypeDDS != sedpConformTypeDDS {
		t.Fatalf("remote topic = %+v", topics[0])
	}
	if got := len(local.RemoteEndpoints()); got != 0 {
		t.Fatalf("remotes = %d, want 0 for topics-only", got)
	}
	if got := local.Generation(); got != 0 {
		t.Fatalf("generation = %d, want 0 for topics-only", got)
	}
}

// TestSEDPFindingsMispositionedHEDrops pins fail-closed on later HE end to end.
func TestSEDPFindingsMispositionedHEDrops(t *testing.T) {
	t.Parallel()

	peer := sedpConformNewIndex(t, 0x43, sedpConformDomain)
	writer := sedpConformDeclareWriter(t, peer, sedpConformTypeHash)
	raw := sedpConformMustSerialize(t, writer)

	he, err := rtps.EncodeHeaderExtension(rtps.HeaderExtension{LittleEndian: true})
	if err != nil {
		t.Fatalf("encode HE: %v", err)
	}
	misplaced := append(append([]byte(nil), raw...), he...)

	local := sedpConformNewIndex(t, 0x44, sedpConformDomain)
	rx := &sedpConformQueueReceiver{queue: [][]byte{misplaced}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- local.ListenLoop(ctx, rx)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("listen loop err = %v, want cancel", err)
	}
	if got := len(local.RemoteEndpoints()); got != 0 {
		t.Fatalf("remotes = %d, want 0 after HE drop", got)
	}
	if got := local.Generation(); got != 0 {
		t.Fatalf("generation = %d, want 0 after HE drop", got)
	}
}

// TestSEDPFindingsCheckedUpsertBinding pins prefix enforcement through the public index.
func TestSEDPFindingsCheckedUpsertBinding(t *testing.T) {
	t.Parallel()

	local := sedpConformNewIndex(t, 0x45, sedpConformDomain)
	peer := sedpConformNewIndex(t, 0x46, sedpConformDomain)
	writer := sedpConformDeclareWriter(t, peer, sedpConformTypeHash)

	var foreign dds.SEDPGUIDPrefix
	for i := range foreign {
		foreign[i] = 0xA0 + byte(i)
	}
	if err := local.UpsertRemoteChecked(foreign, writer); err == nil {
		t.Fatalf("foreign prefix upsert succeeded, want error")
	}
	if got := len(local.RemoteEndpoints()); got != 0 {
		t.Fatalf("remotes = %d, want 0 after refuse", got)
	}
	if err := local.UpsertRemoteChecked(writer.ID.Prefix, writer); err != nil {
		t.Fatalf("bound upsert: %v", err)
	}
	if got := len(local.RemoteEndpoints()); got != 1 {
		t.Fatalf("remotes = %d, want 1", got)
	}
}

// TestSEDPFindingsDomainTagRefusesForeign pins tag equality through the public index.
func TestSEDPFindingsDomainTagRefusesForeign(t *testing.T) {
	t.Parallel()

	local := sedpConformNewIndex(t, 0x47, sedpConformDomain)
	local.SetSEDPDomainTag("prod")
	peer := sedpConformNewIndex(t, 0x48, sedpConformDomain)
	writer := sedpConformDeclareWriter(t, peer, sedpConformTypeHash)

	if err := local.UpsertRemote(writer); err == nil {
		t.Fatalf("untagged upsert succeeded, want refuse")
	}
	tagged := writer
	tagged.DomainTag = "prod"
	if err := local.UpsertRemote(tagged); err != nil {
		t.Fatalf("tagged upsert: %v", err)
	}
	if got := len(local.RemoteEndpoints()); got != 1 {
		t.Fatalf("remotes = %d, want 1", got)
	}
}
