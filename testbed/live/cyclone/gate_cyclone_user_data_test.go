// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cyclone

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Cyclone user DATA receipt timeouts on one allocated exclusive domain.
//
// The allocated domain plus the per-run topic plus per-run payload ID
// isolate concurrent or stale runs on the same wire. Timeouts bound every
// docker and backend call through context.
const (
	cycloneDataOverall time.Duration = 2 * time.Minute
	cycloneDataTake    time.Duration = 45 * time.Second
	cycloneDataSettle  time.Duration = 5 * time.Second
	cycloneDataClean   time.Duration = 10 * time.Second
	cycloneDataType                  = "std_msgs__msg__String"
	cycloneDataHash                  = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"
)

// TestCycloneUserDataReceiptToWeld checks Weld backend Take sees Cyclone pub.
//
// It starts ros2 topic pub detached via docker exec -d writing to
// /testbed/logs/<run-id>.out in the already running weld-jazzy peer and
// never starts the compose stack. It joins the Weld backend on the
// allocated domain, declares an SEDP reader with Subscribe, then loops
// TakeBytes (the DataReceiver TakeFrom path) until a sample arrives on the
// per-run topic. It asserts the IncomingMessage TopicName plus TypeName and
// asserts the decoded String payload holds the per-run live ID. It skips
// cleanly when no daemon or peer container is present. It uses an allocated
// exclusive domain with explicit domain plumbing, so it is safe for future
// t.Parallel workers.
//
//nolint:gocognit,cyclop // live take loop filters topic, type, and payload skips in one run
func TestCycloneUserDataReceiptToWeld(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	ctx, cancel := context.WithTimeout(context.Background(), cycloneDataOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	runID := helpers.LiveID("cyclone-data")
	topic := helpers.LiveTopic(runID)
	payload := "hello cyclone " + runID
	pidPath := helpers.HostPidPath(t, runID)
	pubArg := "{data: " + payload + "}"
	err := helpers.StartPeerProc(ctx, helpers.Container, int(domain), runID, helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2", "--qos-reliability", "reliable", "--qos-durability", "transient_local", "--qos-history", "keep_last", "--qos-depth", "10")
	if err != nil {
		t.Fatalf("start ROS talker: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cycloneDataClean)
		defer cleanupCancel()
		helpers.StopPeerProc(cleanupCtx, helpers.Container, int(domain), pidPath, runID)
	})
	helpers.Sleep(ctx, cycloneDataSettle)
	err = ctx.Err()
	if err != nil {
		t.Fatalf("context ended before join: %v", err)
	}
	cfg := dds.SidecarConfig{
		DomainID:    domain,
		PeerAddress: dds.DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	}
	backend, err := dds.NewBackend(cfg)
	if err != nil {
		t.Fatalf("new backend domain %d: %v", domain, err)
	}
	t.Cleanup(func() {
		cerr := backend.Close()
		if cerr != nil {
			t.Logf("backend close: %v", cerr)
		}
	})
	info := transports.DiscoveryInfo{
		ParticipantName: "cyclone-data-" + runID,
		DomainID:        domain,
		Range:           transports.DiscoveryRangeSystemDefault,
	}
	err = backend.JoinDiscovery(ctx, info)
	if err != nil {
		t.Fatalf("join discovery domain %d: %v", domain, err)
	}
	err = backend.Subscribe(ctx, topic, cycloneDataType, cycloneDataHash)
	if err != nil {
		t.Fatalf("subscribe %q: %v", topic, err)
	}
	takeCtx, takeCancel := context.WithTimeout(ctx, cycloneDataTake)
	defer takeCancel()
	skips := 0
	for takeCtx.Err() == nil {
		msg, terr := backend.TakeBytes(takeCtx)
		if terr != nil {
			if errors.Is(terr, context.DeadlineExceeded) || errors.Is(terr, context.Canceled) {
				break
			}
			if errors.Is(terr, dds.ErrStaleSequence) || errors.Is(terr, dds.ErrTruncated) || errors.Is(terr, dds.ErrInvalidFrame) {
				skips++
				t.Logf("skip take drop %d: %v", skips, terr)
				continue
			}
			skips++
			t.Logf("skip take error %d: %v", skips, terr)
			continue
		}
		if msg.TopicName != topic {
			skips++
			t.Logf("skip take arrived on %q want %q skips %d", msg.TopicName, topic, skips)
			continue
		}
		if msg.TypeName != cycloneDataType {
			t.Fatalf("take type = %q want %q on %q", msg.TypeName, cycloneDataType, topic)
		}
		text, derr := decodeCycloneDataString(msg.Payload)
		if derr != nil {
			skips++
			t.Logf("skip take decode on %q skips %d: %v", msg.TopicName, skips, derr)
			continue
		}
		if !strings.Contains(text, runID) {
			skips++
			t.Logf("skip take payload without run ID on %q skips %d got %q", msg.TopicName, skips, text)
			continue
		}
		t.Logf("cyclone to Weld ok on %q skips %d id %s", topic, skips, runID)
		return
	}
	tcerr := takeCtx.Err()
	if tcerr == nil {
		tcerr = ctx.Err()
	}
	t.Fatalf("weld take did not see Cyclone payload %q on %q after %d skips: %v", payload, topic, skips, tcerr)
}

// decodeCycloneDataString decodes XCDR1 std_msgs/String bytes to text.
func decodeCycloneDataString(wire []byte) (string, error) {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return "", fmt.Errorf("cyclone decode header: %w", err)
	}
	got, err := dec.ReadString()
	if err != nil {
		return "", fmt.Errorf("cyclone decode string: %w", err)
	}
	err = dec.Finish()
	if err != nil {
		return "", fmt.Errorf("cyclone decode finish: %w", err)
	}
	return got, nil
}
