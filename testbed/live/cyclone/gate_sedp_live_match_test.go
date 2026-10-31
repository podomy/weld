// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:cyclop // test breadth
package cyclone

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Live SEDP match timeouts on one allocated exclusive domain.
//
// The per-run topic plus the allocated domain keep this test off gates
// sharing the wire at the same time. Timeouts bound every socket and
// docker call. The suite never starts the compose stack.
const (
	sedpLiveOverall time.Duration = 30 * time.Second
	sedpLiveDial    time.Duration = 3 * time.Second
	sedpLiveRecv    time.Duration = 3 * time.Second
	sedpLiveLease   time.Duration = 10 * time.Second
	sedpLiveCleanup time.Duration = 5 * time.Second
)

// patchSEDPLocatorPorts rewrites participant zero unicast ports to one live port.
//
// MarshalAlivePacket advertises participant zero meta and user ports. The
// bound unicast socket usually holds a later participant, so Cyclone would
// route SEDP away from the listener. This mirrors the backend patch step
// with public wire layout only: a 24 byte IPv4 locator holds LE kind plus
// LE port plus 12 zero bytes plus 4 IPv4 bytes. Only locators carrying a
// participant zero unicast port on the advertise address change. Multicast
// locators use the group address and stay untouched.
//
//nolint:gocognit // locator patch checks sequential wire shapes in one pass
func patchSEDPLocatorPorts(packet []byte, metaZero, userZero, port int) []byte {
	out := append([]byte(nil), packet...)
	if port < 1 || port > 65535 {
		return out
	}
	adv := rtps.AdvertiseIPv4()
	adv4 := adv.To4()
	if adv4 == nil {
		return out
	}
	var want [4]byte
	binary.LittleEndian.PutUint32(want[:], uint32(port))
	var m0 [4]byte
	binary.LittleEndian.PutUint32(m0[:], uint32(metaZero)) //nolint:gosec // G115: DDS ports fit uint32
	var u0 [4]byte
	binary.LittleEndian.PutUint32(u0[:], uint32(userZero)) //nolint:gosec // G115: DDS ports fit uint32
	for i := 0; i+24 <= len(out); i++ {
		if out[i] != 0x01 || out[i+1] != 0x00 || out[i+2] != 0x00 || out[i+3] != 0x00 {
			continue
		}
		var pb [4]byte
		copy(pb[:], out[i+4:i+8])
		if pb != m0 && pb != u0 {
			continue
		}
		zero := true
		for j := i + 8; j < i+20; j++ {
			if out[j] != 0x00 {
				zero = false
				break
			}
		}
		if !zero {
			continue
		}
		if !bytes.Equal(out[i+20:i+24], adv4) {
			continue
		}
		copy(out[i+4:i+8], want[:])
	}
	return out
}

// sedpBuiltinReader maps a SEDP endpoint writer to its reader entity.
//
// Publications writers pair with reader 03C7 and the subscriptions writer
// pairs with reader 04C7, matching the backend pairing. Anything else
// reports false.
func sedpBuiltinReader(writer rtps.EntityID) (rtps.EntityID, bool) {
	if writer == (rtps.EntityID{0x00, 0x00, 0x04, 0xC2}) {
		return rtps.EntityID{0x00, 0x00, 0x04, 0xC7}, true
	}
	if writer == (rtps.EntityID{0x00, 0x00, 0x02, 0xC2}) {
		return rtps.EntityID{0x00, 0x00, 0x03, 0xC7}, true
	}
	if writer == (rtps.EntityID{0x00, 0x00, 0x03, 0xC2}) {
		return rtps.EntityID{0x00, 0x00, 0x03, 0xC7}, true
	}
	return rtps.EntityID{}, false
}

// sedpAckBitmap returns an all-ones MSB packed bitmap for n samples.
//
// Bits run from the most significant bit per the RTPS sequence number set
// layout, so CycloneDDS reads the first samples as requested. The count is
// capped to bound one ACKNACK datagram; live SEDP sets stay far below it.
func sedpAckBitmap(n uint64) ([]uint32, uint32) {
	const maxBits = 256
	if n > maxBits {
		n = maxBits
	}
	bits := uint32(n)
	words := (bits + 31) / 32
	out := make([]uint32, words)
	for i := range out {
		out[i] = 0xFFFFFFFF
	}
	if rem := bits % 32; rem != 0 {
		out[len(out)-1] = ^uint32(0) << (32 - rem)
	}
	return out, bits
}

// answerSEDPHeartbeat answers one SEDP HEARTBEAT with an ACKNACK receipt.
//
// A reliable peer heartbeats forever and never sends endpoint DATA until
// the reader asks. The ACK carries the matching builtin reader entity and
// an INFO_DST naming the sender participant, and goes to the sender first
// route port. Failures only log because heartbeats repeat. It returns the
// next ACK count.
func answerSEDPHeartbeat(t *testing.T, ctx context.Context, uni *rtps.UnicastConn, header [12]byte, sender dds.GUIDPrefix, beat rtps.Heartbeat, count int32, port int) int32 {
	t.Helper()
	reader, ok := sedpBuiltinReader(beat.WriterID)
	if !ok {
		return count
	}
	if beat.FirstSequence == 0 || beat.LastSequence < beat.FirstSequence {
		return count
	}
	n := beat.LastSequence - beat.FirstSequence + 1
	bitmap, numBits := sedpAckBitmap(n)
	if numBits == 0 {
		return count
	}
	ack := rtps.AckNack{
		ReaderID: reader,
		WriterID: beat.WriterID,
		Base:     beat.FirstSequence,
		NumBits:  numBits,
		Bitmap:   bitmap,
		Count:    count,
	}
	body, err := ack.Encode(true)
	if err != nil {
		t.Logf("sedp ack encode: %v", err)
		return count
	}
	dst := rtps.RawSubmessage{
		ID:    rtps.SubmessageInfoDst,
		Flags: rtps.FlagLittleEndian,
		Body:  append([]byte(nil), sender[:]...),
	}
	ackRaw := rtps.RawSubmessage{
		ID:    rtps.SubmessageAckNack,
		Flags: rtps.FlagLittleEndian,
		Body:  body,
	}
	msg, err := rtps.EncodeMessage(rtps.DefaultHeader(header), []rtps.RawSubmessage{dst, ackRaw})
	if err != nil {
		t.Logf("sedp ack frame: %v", err)
		return count
	}
	err = uni.SendTo(ctx, "127.0.0.1", port, msg)
	if err != nil {
		t.Logf("sedp ack send: %v", err)
		return count
	}
	return count + 1
}

// captureSEDPWriter captures one Cyclone writer endpoint for topic on one allocated domain.
//
// It announces patched SPDP on multicast and reads unicast until one SEDP
// DATA writer sample parses for the per-run topic. It answers SEDP
// HEARTBEATs with ACKNACK receipts because a reliable peer heartbeats
// forever without them and never sends endpoint DATA. HEARTBEAT-only,
// SPDP, and user datagrams do not parse as endpoint DATA and are skipped.
// Reader samples are skipped because the match step needs a remote writer.
// A missing Cyclone domain stamps to the test domain like ListenLoop.
//
//nolint:gocognit // live capture sequences announce, receive, ACK, and parse in one loop
func captureSEDPWriter(t *testing.T, ctx context.Context, local *dds.LocalParticipant, sender *rtps.MulticastSender, uni *rtps.UnicastConn, metaZero, userZero int, topic string, domain uint32) dds.SEDPEndpoint {
	t.Helper()
	lg := local.GUID()
	var header [12]byte
	copy(header[:], lg.Prefix[:])
	routes := make(map[dds.GUIDPrefix][]int)
	var ackCount int32 = 1
	seq := uint64(0)
	for ctx.Err() == nil {
		seq++
		packet, err := dds.MarshalAlivePacket(local.GUID(), domain, seq, local.LeaseDuration())
		if err != nil {
			t.Fatalf("marshal alive: %v", err)
		}
		patched := patchSEDPLocatorPorts(packet, metaZero, userZero, uni.LocalPort())
		err = sender.Send(patched) //nolint:contextcheck // multicast Send has no context param, socket uses timeouts
		if err != nil {
			t.Fatalf("multicast send: %v", err)
		}
		recvCtx, recvCancel := context.WithTimeout(ctx, sedpLiveRecv)
		_, _, raw, rerr := uni.Receive(recvCtx)
		recvCancel()
		if rerr != nil {
			continue
		}
		var drops rtps.Counter
		hdr, raws, err := rtps.DecodeMessage(raw, &drops)
		if err != nil {
			continue
		}
		if ann, err := dds.ParseAnnouncement(raw); err == nil {
			if ann.DomainID == domain && len(ann.Ports) > 0 && ann.GUID.Prefix != lg.Prefix {
				learned := make([]int, 0, len(ann.Ports))
				for _, p := range ann.Ports {
					learned = append(learned, int(p))
				}
				routes[ann.GUID.Prefix] = learned
			}
		}
		dataWriter, hasData := sedpEndpointDataWriter(raws)
		for i := range raws {
			sub, err := rtps.DecodeSubmessage(raws[i], &drops)
			if err != nil {
				continue
			}
			if sub.Heartbeat == nil {
				continue
			}
			if _, ok := sedpBuiltinReader(sub.Heartbeat.WriterID); !ok {
				continue
			}
			var senderPrefix dds.GUIDPrefix
			copy(senderPrefix[:], hdr.Prefix[:])
			if senderPrefix == lg.Prefix {
				continue
			}
			senderPorts := routes[senderPrefix]
			if len(senderPorts) == 0 {
				continue
			}
			ackCount = answerSEDPHeartbeat(t, ctx, uni, header, senderPrefix, *sub.Heartbeat, ackCount, senderPorts[0])
		}
		if !hasData {
			continue
		}
		ep, err := dds.ParseSEDPDATA(raw)
		if err != nil {
			continue
		}
		if ep.IsReader {
			continue
		}
		if ep.ROSTopic != topic {
			continue
		}
		if ep.DomainID == 0 {
			ep.DomainID = domain
		}
		if dataWriter != (rtps.EntityID{0x00, 0x00, 0x03, 0xC2}) {
			t.Fatalf("sedp wire writer = %v, want Cyclone publications 03C2", dataWriter)
		}
		return ep
	}
	t.Fatalf("no Cyclone SEDP writer on %q", topic)
	return dds.SEDPEndpoint{}
}

// sedpEndpointDataWriter reports the first SEDP endpoint DATA writer.
//
// It scans decoded submessages and returns the writer entity when a DATA
// submessage carries publications writer 03C2 or subscriptions writer 04C2.
// Anything else reports false.
func sedpEndpointDataWriter(raws []rtps.RawSubmessage) (rtps.EntityID, bool) {
	var drops rtps.Counter
	for i := range raws {
		sub, err := rtps.DecodeSubmessage(raws[i], &drops)
		if err != nil {
			continue
		}
		if sub.Data == nil {
			continue
		}
		if sub.Data.WriterID == (rtps.EntityID{0x00, 0x00, 0x03, 0xC2}) {
			return sub.Data.WriterID, true
		}
		if sub.Data.WriterID == (rtps.EntityID{0x00, 0x00, 0x04, 0xC2}) {
			return sub.Data.WriterID, true
		}
	}
	return rtps.EntityID{}, false
}

// TestCycloneSEDPLiveParseMatch pins live Cyclone SEDP parse plus match.
//
// It dials unicast on one allocated domain, starts a Cyclone talker on a per-run
// topic, announces patched SPDP, and captures one SEDP DATA writer sample
// with dds.ParseSEDPDATA. It asserts topic, type, and hash are non-empty
// with the allocated domain, upserts the remote into a fresh SEDP index, and requires
// SEDPEndpointsMatch with a local reader on the same topic, type, and hash.
// Only SPDP non-local receipt is pinned live today; the user DATA path
// depends on this match. It skips when no daemon or peer container is
// present.
//
//nolint:gocognit,cyclop // live SEDP test sequences dial, talker, capture, and match in one run
func TestCycloneSEDPLiveParseMatch(t *testing.T) {
	domain := helpers.MustAllocateDomainU32(t)
	ctx, cancel := context.WithTimeout(context.Background(), sedpLiveOverall)
	defer cancel()
	helpers.SkipIfNoPeer(t, ctx, helpers.Container)
	local, err := dds.NewLocalParticipant(domain, sedpLiveLease)
	if err != nil {
		t.Fatalf("new local participant: %v", err)
	}
	dialCtx, dialCancel := context.WithTimeout(ctx, sedpLiveDial)
	uni, err := rtps.DialUnicast(dialCtx, domain)
	dialCancel()
	if err != nil {
		t.Fatalf("dial unicast: %v", err)
	}
	t.Cleanup(func() {
		cerr := uni.Close()
		if cerr != nil {
			t.Logf("unicast close: %v", cerr)
		}
	})
	metaZero, err := rtps.MetatrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("meta unicast port: %v", err)
	}
	userZero, err := rtps.UsertrafficUnicastPort(domain, 0)
	if err != nil {
		t.Fatalf("user unicast port: %v", err)
	}
	sender, err := rtps.DialMulticastSender(domain)
	if err != nil {
		t.Fatalf("dial multicast sender: %v", err)
	}
	t.Cleanup(func() {
		cerr := sender.Close()
		if cerr != nil {
			t.Logf("sender close: %v", cerr)
		}
	})
	runID := helpers.LiveID("cyclone-sedp")
	topic := helpers.LiveTopic(runID)
	pubArg := "{data: hello sedp " + runID + "}"
	pidPath := helpers.HostPidPath(t, runID)
	err = helpers.StartPeerProc(ctx, helpers.Container, int(domain), runID, helpers.RosBin, "topic", "pub", topic, helpers.RosType, pubArg, "--rate", "2", "--qos-reliability", "reliable", "--qos-durability", "transient_local", "--qos-history", "keep_last", "--qos-depth", "10")
	if err != nil {
		t.Fatalf("start talker: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), sedpLiveCleanup)
		defer cleanupCancel()
		helpers.StopPeerProcAndWait(cleanupCtx, helpers.Container, int(domain), pidPath, runID)
	})
	ep := captureSEDPWriter(t, ctx, local, sender, uni, metaZero, userZero, topic, domain)
	if ep.DDSTopic == "" {
		t.Fatalf("sedp dds topic empty on %q", topic)
	}
	if ep.TypeDDS == "" {
		t.Fatalf("sedp type empty on %q", topic)
	}
	if ep.TypeHash == "" {
		t.Fatalf("sedp type hash empty on %q", topic)
	}
	if ep.DomainID != domain {
		t.Fatalf("sedp domain = %d, want %d", ep.DomainID, domain)
	}
	lg := local.GUID()
	var prefix dds.SEDPGUIDPrefix
	copy(prefix[:], lg.Prefix[:])
	if ep.ID.Prefix == prefix {
		t.Fatalf("sedp prefix matches local, want non-local Cyclone")
	}
	idx, err := dds.NewSEDPIndex(prefix, domain)
	if err != nil {
		t.Fatalf("new sedp index: %v", err)
	}
	err = idx.UpsertRemote(ep)
	if err != nil {
		t.Fatalf("upsert remote: %v", err)
	}
	reader, err := idx.DeclareReader(ep.ROSTopic, ep.TypeIDL, ep.TypeHash, nil)
	if err != nil {
		t.Fatalf("declare reader: %v", err)
	}
	if !dds.SEDPEndpointsMatch(reader, ep) {
		t.Fatalf("sedp endpoints do not match on %q", topic)
	}
	t.Logf("live SEDP match on %q type %q", topic, ep.TypeDDS)
}
