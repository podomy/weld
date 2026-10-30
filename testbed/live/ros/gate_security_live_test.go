// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package ros_test holds the DDS Security live gate.
//
// Path (b) gate: no secured CycloneDDS peer can be provisioned from this
// testbed. There are no identity certificates, governance files, or
// permissions material checked in, and manufacturing them just to attempt
// a handshake would be decryption theater: Weld implements no DDS Security
// crypto (see the SECURITY_GAP block in
// internal/transports/dds/security.go), so path (a), an unsecured Weld
// reader flowing beside a secured third peer, cannot be proven honestly
// here. This gate proves the implementable half instead: crafted
// protected-RTPS bytes presented to the Weld receive path are dropped with
// the exact secured-transform-unavailable reason, the backend stays
// healthy, and subsequent plain samples still flow with no loop death.
// Every assertion fails loudly via t.Fatalf; nothing here skips on a
// mismatch. helpers.SkipIfNoPeer still applies because live gates never
// run without the peer environment, even though this gate needs no
// secured peer itself.
package ros_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// gateSecWriter is a minimal in-memory dds.Writer for the health probe.
type gateSecWriter struct {
	frames [][]byte
}

// WriteFrame records one frame for the later Take.
func (w *gateSecWriter) WriteFrame(frame []byte) error {
	w.frames = append(w.frames, append([]byte(nil), frame...))
	return nil
}

// TestGateSecurityProtectedBytesDropLoudly proves path (b) of the security gate.
//
//nolint:gocognit,cyclop // live gate sequences protected bytes plus health probe in one run, splitting would obscure intent
func TestGateSecurityProtectedBytesDropLoudly(t *testing.T) {
	t.Parallel()
	domain := helpers.MustAllocateDomain(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	live := helpers.LiveID("sec")
	topic := helpers.LiveTopic(live)
	t.Logf("security gate domain=%d topic=%s live=%s", domain, topic, live)

	var prefix [12]byte
	for i := range prefix {
		prefix[i] = byte(i + 1)
	}

	datagram, err := rtps.EncodeMessage(rtps.DefaultHeader(prefix), []rtps.RawSubmessage{
		{ID: 0x33, Flags: 0x01, Body: []byte{0xDE, 0xAD, 0xBE, 0xEF}},
		{ID: 0x30, Flags: 0x01, Body: []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}},
		{ID: rtps.SubmessageData, Flags: 0x01, Body: []byte{0x00}},
	})
	if err != nil {
		t.Fatalf("encode crafted protected datagram: %v", err)
	}

	var drops rtps.Counter
	_, subs, err := rtps.DecodeMessage(datagram, &drops)
	if err != nil {
		t.Fatalf("crafted bytes must still parse as RTPS: %v", err)
	}
	if len(subs) != 3 {
		t.Fatalf("crafted datagram holds %d submessages, want 3", len(subs))
	}

	recv := dds.NewDataReceiver()
	protected := 0

	for _, sub := range subs {
		handled, herr := dds.HandleProtectedSubmessage(recv, sub)
		if !dds.IsProtectedSubmessageID(sub.ID) {
			if handled || herr != nil {
				t.Fatalf("plain submessage %#02x must pass through untouched: handled=%v err=%v", sub.ID, handled, herr)
			}
			continue
		}
		if !handled {
			t.Fatalf("protected submessage %#02x must report handled", sub.ID)
		}
		if !errors.Is(herr, dds.ErrSecuredTransformUnavailable) {
			t.Fatalf("protected submessage %#02x error %v must wrap secured-transform-unavailable", sub.ID, herr)
		}
		protected++
	}

	if protected != 2 {
		t.Fatalf("protected submessages handled %d, want 2", protected)
	}

	if got := recv.DropReasonCount(dds.DropReasonSecuredTransformUnavailable); got != 2 {
		t.Fatalf("secured-transform-unavailable count %d, want 2", got)
	}

	var guid dds.WriterGUID
	for i := range guid {
		guid[i] = byte(0xA0 + i)
	}

	sink := &gateSecWriter{}
	pub, err := dds.NewPublisher(guid, sink)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	payload := "plain after protected " + live
	err = pub.Publish(transports.OutgoingMessage{TopicName: topic, TypeName: "std_msgs__msg__String", Payload: []byte(payload)})
	if err != nil {
		t.Fatalf("publish plain sample: %v", err)
	}
	if len(sink.frames) != 1 {
		t.Fatalf("frames recorded %d, want 1", len(sink.frames))
	}

	msg, err := recv.Take(sink.frames[0])
	if err != nil {
		t.Fatalf("backend unhealthy after protected drops, take failed: %v", err)
	}
	if string(msg.Payload) != payload {
		t.Fatalf("payload %q, want %q", msg.Payload, payload)
	}
	if msg.TopicName != topic {
		t.Fatalf("topic %q, want %q", msg.TopicName, topic)
	}
}
