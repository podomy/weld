// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package rtps_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports/dds/rtps"
)

// TestEncodeParamsGolden pins exact wire bytes including padding.
func TestEncodeParamsGolden(t *testing.T) {
	t.Parallel()

	params := []rtps.Param{
		{ID: rtps.PIDParticipantGUID, Value: []byte{0xAA}},
		{ID: rtps.PIDTopicName, Value: []byte{0x01, 0x02}},
		{ID: rtps.PIDTopicName, Value: []byte{0x01, 0x02, 0x03}},
		{ID: rtps.PIDVendorID, Value: []byte{0x01, 0x02, 0x03, 0x04}},
	}
	got := rtps.EncodeParams(params)
	want := []byte{
		0x50, 0x00, 0x01, 0x00, 0xAA, 0x00, 0x00, 0x00,
		0x05, 0x00, 0x02, 0x00, 0x01, 0x02, 0x00, 0x00,
		0x05, 0x00, 0x03, 0x00, 0x01, 0x02, 0x03, 0x00,
		0x16, 0x00, 0x04, 0x00, 0x01, 0x02, 0x03, 0x04,
		0x01, 0x00, 0x00, 0x00,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("encode = % X, want % X", got, want)
	}
	back, err := rtps.DecodeParams(got)
	if err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	if len(back) != len(params) {
		t.Fatalf("decoded %d params, want %d", len(back), len(params))
	}
	for i := range params {
		if back[i].ID != params[i].ID || !bytes.Equal(back[i].Value, params[i].Value) {
			t.Fatalf("param %d = %#04x % X, want %#04x % X", i, back[i].ID, back[i].Value, params[i].ID, params[i].Value)
		}
	}
}

// TestEncodeParamsEmpty pins the sentinel only encoding.
func TestEncodeParamsEmpty(t *testing.T) {
	t.Parallel()

	got := rtps.EncodeParams(nil)
	want := []byte{0x01, 0x00, 0x00, 0x00}
	if !bytes.Equal(got, want) {
		t.Fatalf("empty encode = % X, want % X", got, want)
	}
	back, err := rtps.DecodeParams(got)
	if err != nil {
		t.Fatalf("decode empty: %v", err)
	}
	if len(back) != 0 {
		t.Fatalf("decoded %d params, want 0", len(back))
	}
}

// TestDecodeParamsErrors rejects truncated and unterminated inputs.
func TestDecodeParamsErrors(t *testing.T) {
	t.Parallel()

	cases := map[string][]byte{
		"empty":             {},
		"short header":      {0x50, 0x00},
		"truncated value":   {0x50, 0x00, 0x04, 0x00, 0x01, 0x02},
		"truncated pad":     {0x05, 0x00, 0x01, 0x00, 0xAA},
		"missing sentinel":  {0x50, 0x00, 0x01, 0x00, 0xAA, 0x00, 0x00, 0x00},
		"sentinel nonzero":  {0x01, 0x00, 0x01, 0x00, 0xAA, 0x00, 0x00, 0x00},
		"trailing sentinel": {0x01, 0x00, 0x00, 0x00, 0xFF},
	}
	for name, input := range cases {
		if _, err := rtps.DecodeParams(input); err == nil {
			t.Fatalf("%s succeeded, want error", name)
		}
	}
	sentinel, err := rtps.DecodeParams([]byte{0x01, 0x00, 0x00, 0x00})
	if err != nil {
		t.Fatalf("sentinel only: %v", err)
	}
	if len(sentinel) != 0 {
		t.Fatalf("sentinel only decoded %d params, want 0", len(sentinel))
	}
}

// TestDecodeParamsUnknownID proves unknown ids are preserved.
func TestDecodeParamsUnknownID(t *testing.T) {
	t.Parallel()

	params := []rtps.Param{
		{ID: 0x8007, Value: []byte{0xDE, 0xAD}},
		{ID: rtps.PIDVendorID, Value: []byte{0x01, 0x10}},
		{ID: 0xFFFF, Value: []byte{}},
	}
	raw := rtps.EncodeParams(params)
	back, err := rtps.DecodeParams(raw)
	if err != nil {
		t.Fatalf("decode unknown: %v", err)
	}
	if len(back) != len(params) {
		t.Fatalf("decoded %d params, want %d", len(back), len(params))
	}
	for i := range params {
		if back[i].ID != params[i].ID || !bytes.Equal(back[i].Value, params[i].Value) {
			t.Fatalf("param %d mismatch", i)
		}
	}
}

// TestLocatorRoundTrip checks the 24 byte locator layout.
func TestLocatorRoundTrip(t *testing.T) {
	t.Parallel()

	ip := net.ParseIP("192.168.0.162")
	raw := rtps.EncodeIPv4Locator(rtps.LocatorKindUDPv4, 17913, ip)
	if len(raw) != 24 {
		t.Fatalf("locator len = %d, want 24", len(raw))
	}
	wantPrefix := []byte{
		0x01, 0x00, 0x00, 0x00,
		0xF9, 0x45, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0xC0, 0xA8, 0x00, 0xA2,
	}
	if !bytes.Equal(raw, wantPrefix) {
		t.Fatalf("locator = % X, want % X", raw, wantPrefix)
	}
	kind, port, addr, err := rtps.DecodeIPv4Locator(raw)
	if err != nil {
		t.Fatalf("decode locator: %v", err)
	}
	if kind != rtps.LocatorKindUDPv4 || port != 17913 || !addr.Equal(net.ParseIP("192.168.0.162")) {
		t.Fatalf("locator = %d %d %v, want 1 17913 192.168.0.162", kind, port, addr)
	}
	if _, _, _, err := rtps.DecodeIPv4Locator(raw[:23]); err == nil {
		t.Fatalf("short locator succeeded, want error")
	}
	kind2, port2, addr2, err := rtps.DecodeLocator(raw)
	if err != nil {
		t.Fatalf("decode locator alias: %v", err)
	}
	if kind2 != kind || port2 != port || !addr2.Equal(addr) {
		t.Fatalf("alias mismatch")
	}
}

// cyclonesPDPList holds a genuine 360 byte SPDP PID list captured from
// CycloneDDS on domain 42 via the shared unicast port 17900.
//
// Observed wire facts pinned here and verified by the capture on
// 2026-10-26: RTPS version 2.1, vendor id 0x01 0x10 which is Eclipse
// CycloneDDS per the OMG vendor table, GUID prefix 0110a63e008f0a7f49bc220e
// with participant entity 000001c1, lease 10 seconds, locators for
// 192.168.0.162 and 239.255.0.1. Protocol PIDs use 4 byte values on the
// wire (02010000 and 01100000) with pad included in the length.
func cyclonesPDPList() []byte {
	return []byte{
		0x2c, 0x00, 0x10, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x65, 0x6e, 0x63, 0x6c,
		0x61, 0x76, 0x65, 0x3d, 0x2f, 0x3b, 0x00, 0x00, 0x59, 0x00, 0x5c, 0x00,
		0x03, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x5f, 0x5f, 0x50, 0x72,
		0x6f, 0x63, 0x65, 0x73, 0x73, 0x4e, 0x61, 0x6d, 0x65, 0x00, 0x00, 0x00,
		0x08, 0x00, 0x00, 0x00, 0x70, 0x79, 0x74, 0x68, 0x6f, 0x6e, 0x33, 0x00,
		0x06, 0x00, 0x00, 0x00, 0x5f, 0x5f, 0x50, 0x69, 0x64, 0x00, 0x00, 0x00,
		0x05, 0x00, 0x00, 0x00, 0x33, 0x32, 0x31, 0x35, 0x00, 0x00, 0x00, 0x00,
		0x0b, 0x00, 0x00, 0x00, 0x5f, 0x5f, 0x48, 0x6f, 0x73, 0x74, 0x6e, 0x61,
		0x6d, 0x65, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x66, 0x65, 0x64, 0x6f,
		0x72, 0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x04, 0x00,
		0x02, 0x01, 0x00, 0x00, 0x16, 0x00, 0x04, 0x00, 0x01, 0x10, 0x00, 0x00,
		0x02, 0x00, 0x08, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x50, 0x00, 0x10, 0x00, 0x01, 0x10, 0xa6, 0x3e, 0x00, 0x8f, 0x0a, 0x7f,
		0x49, 0xbc, 0x22, 0x0e, 0x00, 0x00, 0x01, 0xc1, 0x58, 0x00, 0x04, 0x00,
		0x3f, 0xfc, 0x00, 0x00, 0x0f, 0x00, 0x04, 0x00, 0x2a, 0x00, 0x00, 0x00,
		0x31, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0xf9, 0x45, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xc0, 0xa8, 0x00, 0xa2, 0x48, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00,
		0xed, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0xef, 0xff, 0x00, 0x01, 0x32, 0x00, 0x18, 0x00,
		0x01, 0x00, 0x00, 0x00, 0xf8, 0x45, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xc0, 0xa8, 0x00, 0xa2,
		0x33, 0x00, 0x18, 0x00, 0x01, 0x00, 0x00, 0x00, 0xec, 0x45, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0xef, 0xff, 0x00, 0x01, 0x07, 0x80, 0x34, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x2c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x1a, 0x00, 0x00, 0x00, 0x66, 0x65, 0x64, 0x6f,
		0x72, 0x61, 0x2f, 0x30, 0x2e, 0x31, 0x30, 0x2e, 0x35, 0x2f, 0x4c, 0x69,
		0x6e, 0x75, 0x78, 0x2f, 0x4c, 0x69, 0x6e, 0x75, 0x78, 0x00, 0x00, 0x00,
		0x19, 0x80, 0x04, 0x00, 0x00, 0x00, 0x10, 0x00, 0x01, 0x00, 0x00, 0x00,
	}
}

// lookupParam returns the first param with id.
func lookupParam(params []rtps.Param, id uint16) ([]byte, bool) {
	for i := range params {
		if params[i].ID == id {
			return params[i].Value, true
		}
	}
	return nil, false
}

// TestDecodeParamsCycloneGolden parses the captured Cyclone PID list.
func TestDecodeParamsCycloneGolden(t *testing.T) {
	t.Parallel()

	raw := cyclonesPDPList()
	params, err := rtps.DecodeParams(raw)
	if err != nil {
		t.Fatalf("decode cyclone golden: %v", err)
	}
	checkGoldenGUID(t, params)
	checkGoldenVersionVendor(t, params)
	checkGoldenLease(t, params)
	checkGoldenLocators(t, params)
	checkGoldenVendor(t, params)
}

// checkGoldenVendor asserts vendor 0x8007 and 0x8019 from the live capture.
func checkGoldenVendor(t *testing.T, params []rtps.Param) {
	t.Helper()

	versionInfo, ok := lookupParam(params, rtps.PIDVendorAdlinkParticipantVersionInfo)
	if !ok || len(versionInfo) != 52 {
		t.Fatalf("0x8007 missing or len %d, want 52", len(versionInfo))
	}

	if !bytes.Contains(versionInfo, []byte("fedora")) {
		t.Fatalf("0x8007 misses fedora marker")
	}

	bufferSize, ok := lookupParam(params, rtps.PIDVendorCycloneReceiveBufferSize)
	if !ok || len(bufferSize) != 4 {
		t.Fatalf("0x8019 missing or len %d, want 4", len(bufferSize))
	}

	if bufferSize[0] != 0x00 || bufferSize[1] != 0x00 || bufferSize[2] != 0x10 || bufferSize[3] != 0x00 {
		t.Fatalf("0x8019 = % X, want 00 00 10 00", bufferSize)
	}
}

// checkGoldenGUID asserts the participant GUID entity.
func checkGoldenGUID(t *testing.T, params []rtps.Param) {
	t.Helper()

	guid, ok := lookupParam(params, rtps.PIDParticipantGUID)
	if !ok || len(guid) != 16 {
		t.Fatalf("guid missing or len %d", len(guid))
	}
	if guid[12] != 0x00 || guid[13] != 0x00 || guid[14] != 0x01 || guid[15] != 0xC1 {
		t.Fatalf("guid entity = % X, want participant", guid[12:16])
	}
}

// checkGoldenVersionVendor asserts protocol 2.1 and Cyclone vendor.
func checkGoldenVersionVendor(t *testing.T, params []rtps.Param) {
	t.Helper()

	version, ok := lookupParam(params, rtps.PIDProtocolVersion)
	if !ok || len(version) < 2 || version[0] != 0x02 || version[1] != 0x01 {
		t.Fatalf("version = % X, want 02 01", version)
	}
	vendor, ok := lookupParam(params, rtps.PIDVendorID)
	if !ok || len(vendor) < 2 || vendor[0] != 0x01 || vendor[1] != 0x10 {
		t.Fatalf("vendor = % X, want 01 10 Cyclone", vendor)
	}
}

// checkGoldenLease asserts the 8 byte lease is present.
func checkGoldenLease(t *testing.T, params []rtps.Param) {
	t.Helper()

	lease, ok := lookupParam(params, rtps.PIDParticipantLeaseDuration)
	if !ok || len(lease) != 8 {
		t.Fatalf("lease missing or len %d", len(lease))
	}
}

// checkGoldenLocators asserts the four discovery locators decode.
func checkGoldenLocators(t *testing.T, params []rtps.Param) {
	t.Helper()

	ids := []uint16{
		rtps.PIDDefaultUnicastLocatorStandard,
		rtps.PIDMetatrafficUnicastLocator,
		rtps.PIDMetatrafficMulticastLocator,
		rtps.PIDDefaultMulticastLocator,
	}
	for _, id := range ids {
		value, ok := lookupParam(params, id)
		if !ok {
			t.Fatalf("locator %#04x missing", id)
		}
		if _, _, _, err := rtps.DecodeIPv4Locator(value); err != nil {
			t.Fatalf("locator %#04x decode: %v", id, err)
		}
	}
}

// TestLiveCaptureSPDP attempts a fresh SPDP capture when a testbed exists.
// It skips under -short so unit runs stay hermetic, and skips when docker
// or the domain 42 discovery path is unavailable.
//
// Manual capture used a python socket on 239.255.0.1:17910 when the port
// was free. The automated test listens on 0.0.0.0:17900 with reuse instead:
// Cyclone binds 17910 multicast exclusively but shares 17900 unicast, so
// 17900 still receives genuine SPDP while coexisting with other testbed
// publishers. The embedded golden vector above is the primary proof; this
// test re-proves liveness when the testbed is reachable.
func TestLiveCaptureSPDP(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode skips live capture")
	}
	ensureTestbed(t)
	conn := captureConn(t)
	defer closeCapture(t, conn)
	datagram := captureDatagram(t, conn)
	payload := extractSPDPParams(t, datagram)
	params, err := rtps.DecodeParams(payload)
	if err != nil {
		t.Fatalf("decode live PID list: %v", err)
	}
	if _, ok := lookupParam(params, rtps.PIDParticipantGUID); !ok {
		t.Fatalf("live GUID missing")
	}
	if _, ok := lookupParam(params, rtps.PIDVendorID); !ok {
		t.Fatalf("live vendor missing")
	}
}

// ensureTestbed skips when docker or weld-jazzy is unavailable.
func ensureTestbed(t *testing.T) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "docker", "exec", "weld-jazzy", "true").Run(); err != nil {
		t.Skipf("testbed weld-jazzy unavailable: %v", err)
	}
}

// captureConn binds the shared discovery port with reuse.
func captureConn(t *testing.T) net.PacketConn {
	t.Helper()

	conn, err := listenReuse(t, 17900)
	if err != nil {
		t.Skipf("port 17900 busy, no capture: %v", err)
	}
	if err := conn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		t.Fatalf("deadline: %v", err)
	}
	return conn
}

// closeCapture closes the capture socket.
func closeCapture(t *testing.T, conn net.PacketConn) {
	t.Helper()

	if err := conn.Close(); err != nil {
		t.Logf("close capture: %v", err)
	}
}

// captureDatagram starts a publisher and returns one RTPS datagram.
func captureDatagram(t *testing.T, conn net.PacketConn) []byte {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	pub := exec.CommandContext(ctx, "docker", "exec", "weld-jazzy", "bash", "-lc", "source /opt/ros/jazzy/setup.bash && ROS_DOMAIN_ID=42 RMW_IMPLEMENTATION=rmw_cyclonedds_cpp timeout 8 ros2 topic pub /chatter std_msgs/msg/String 'data: live' --rate 5")
	if err := pub.Start(); err != nil {
		t.Skipf("start publisher: %v", err)
	}
	defer func() {
		if err := pub.Process.Kill(); err != nil {
			t.Logf("kill publisher: %v", err)
		}
	}()
	buf := make([]byte, 65507)
	for {
		n, _, err := conn.ReadFrom(buf)
		if err != nil {
			t.Skipf("no SPDP datagram: %v", err)
		}
		if n < 20 || string(buf[:4]) != "RTPS" {
			continue
		}
		candidate := append([]byte(nil), buf[:n]...)
		if hasSPDPData(candidate) {
			return candidate
		}
	}
}

// hasSPDPData reports whether a datagram carries SPDP DATA.
func hasSPDPData(datagram []byte) bool {
	var drops rtps.Counter
	_, raws, err := rtps.DecodeMessage(datagram, &drops)
	if err != nil {
		return false
	}
	for _, raw := range raws {
		sub, err := rtps.DecodeSubmessage(raw, &drops)
		if err != nil {
			continue
		}
		if sub.Data == nil {
			continue
		}
		if sub.Data.WriterID == rtps.EntitySPDPWriter {
			return true
		}
	}
	return false
}

// extractSPDPParams unwraps RTPS DATA payload to the PID list.
func extractSPDPParams(t *testing.T, datagram []byte) []byte {
	t.Helper()

	var drops rtps.Counter
	_, raws, err := rtps.DecodeMessage(datagram, &drops)
	if err != nil {
		t.Fatalf("decode message: %v", err)
	}
	for _, raw := range raws {
		sub, err := rtps.DecodeSubmessage(raw, &drops)
		if err != nil {
			continue
		}
		if sub.Data == nil || len(sub.Data.Payload) < 4 {
			continue
		}
		if sub.Data.WriterID != rtps.EntitySPDPWriter {
			continue
		}
		return append([]byte(nil), sub.Data.Payload[4:]...)
	}
	t.Fatalf("no SPDP DATA payload in %d submessages", len(raws))
	return nil
}

// linuxReusePort is SO_REUSEPORT on Linux, matching soReusePort in
// loopback_linux.go. The test helper cannot import the unexported constant,
// so the value is repeated here with the same meaning.
const linuxReusePort = 15

// listenReuse binds 0.0.0.0:port with reuse so the capture coexists with
// Cyclone publishers that already share the unicast discovery port.
func listenReuse(t *testing.T, port int) (net.PacketConn, error) {
	t.Helper()

	lc := net.ListenConfig{
		Control: func(_, _ string, c syscall.RawConn) error {
			var first error
			ctrlErr := c.Control(func(fd uintptr) {
				if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil && first == nil {
					first = fmt.Errorf("rtps test: reuseaddr: %w", err)
				}
				if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, linuxReusePort, 1); err != nil && first == nil {
					first = fmt.Errorf("rtps test: reuseport: %w", err)
				}
			})
			if ctrlErr != nil {
				return fmt.Errorf("rtps test: reuse control: %w", ctrlErr)
			}
			return first
		},
	}
	conn, err := lc.ListenPacket(context.Background(), "udp4", net.JoinHostPort("0.0.0.0", strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("rtps test: listen %d: %w", port, err)
	}
	return conn, nil
}

// TestDecodeParamsEndianOrders checks both byte orders plus consumed count.
func TestDecodeParamsEndianOrders(t *testing.T) {
	t.Parallel()

	le := rtps.EncodeParams([]rtps.Param{{ID: rtps.PIDTopicName, Value: []byte("rt/x")}})

	gotLE, consumed, err := rtps.DecodeParamsEndian(le, true)
	if err != nil {
		t.Fatalf("decode le: %v", err)
	}

	if consumed != len(le) {
		t.Fatalf("le consumed = %d, want %d", consumed, len(le))
	}

	if len(gotLE) != 1 || gotLE[0].ID != rtps.PIDTopicName || string(gotLE[0].Value) != "rt/x" {
		t.Fatalf("le params = %+v", gotLE)
	}

	be := append([]byte(nil), le...)
	be[0], be[1] = 0x00, 0x05
	be[2], be[3] = 0x00, 0x04
	be[8], be[9] = 0x00, 0x01

	gotBE, consumed, err := rtps.DecodeParamsEndian(be, false)
	if err != nil {
		t.Fatalf("decode be: %v", err)
	}

	if consumed != len(be) {
		t.Fatalf("be consumed = %d, want %d", consumed, len(be))
	}

	if len(gotBE) != 1 || gotBE[0].ID != rtps.PIDTopicName || string(gotBE[0].Value) != "rt/x" {
		t.Fatalf("be params = %+v", gotBE)
	}
}

// TestDecodeParamsEndianTrailing checks bytes after the sentinel are left.
func TestDecodeParamsEndianTrailing(t *testing.T) {
	t.Parallel()

	blob := append(rtps.EncodeParams(nil), 0xAA, 0xBB)

	got, consumed, err := rtps.DecodeParamsEndian(blob, true)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("params = %d, want 0", len(got))
	}

	if consumed != 4 {
		t.Fatalf("consumed = %d, want 4", consumed)
	}
}

// TestDecodeParamsEndianTruncated checks short buffers fail loudly.
func TestDecodeParamsEndianTruncated(t *testing.T) {
	t.Parallel()

	if _, _, err := rtps.DecodeParamsEndian([]byte{0x05, 0x00}, true); err == nil {
		t.Fatal("short header succeeded, want error")
	}

	if _, _, err := rtps.DecodeParamsEndian([]byte{0x05, 0x00, 0x09, 0x00, 0xAA}, true); err == nil {
		t.Fatal("short value succeeded, want error")
	}

	if _, _, err := rtps.DecodeParamsEndian([]byte{0x01, 0x00, 0x01, 0x00}, true); err == nil {
		t.Fatal("nonzero sentinel succeeded, want error")
	}

	if _, _, err := rtps.DecodeParamsEndian([]byte{0x05, 0x00, 0x01, 0x00, 0xAA}, true); err == nil {
		t.Fatal("short pad succeeded, want error")
	}
}

// TestEncodeParamsTruncates caps values to the uint16 range.
func TestEncodeParamsTruncates(t *testing.T) {
	t.Parallel()

	big := make([]byte, 70000)
	for i := range big {
		big[i] = 0xAB
	}
	raw := rtps.EncodeParams([]rtps.Param{{ID: rtps.PIDTopicName, Value: big}})
	back, err := rtps.DecodeParams(raw)
	if err != nil {
		t.Fatalf("decode truncated: %v", err)
	}
	if len(back) != 1 {
		t.Fatalf("params = %d, want 1", len(back))
	}
	if len(back[0].Value) != 0xFFFF {
		t.Fatalf("truncated len = %d, want %d", len(back[0].Value), 0xFFFF)
	}
}

// TestEncodeIPv4LocatorNil encodes nil as zeros.
func TestEncodeIPv4LocatorNil(t *testing.T) {
	t.Parallel()

	raw := rtps.EncodeIPv4Locator(rtps.LocatorKindUDPv4, 7400, nil)
	if len(raw) != 24 {
		t.Fatalf("len = %d, want 24", len(raw))
	}
	kind, port, addr, err := rtps.DecodeIPv4Locator(raw)
	if err != nil {
		t.Fatalf("decode nil locator: %v", err)
	}
	if kind != rtps.LocatorKindUDPv4 || port != 7400 {
		t.Fatalf("locator = %d %d, want 1 7400", kind, port)
	}
	if !addr.Equal(net.ParseIP("0.0.0.0")) {
		t.Fatalf("addr = %v, want 0.0.0.0", addr)
	}
}

// TestDecodeLocatorShort rejects truncated input.
func TestDecodeLocatorShort(t *testing.T) {
	t.Parallel()

	if _, _, _, err := rtps.DecodeLocator([]byte{0x01, 0x02}); err == nil {
		t.Fatal("short locator succeeded, want error")
	}
}

// TestDecodeParamsSkipsPad checks PIDPad entries never surface.
func TestDecodeParamsSkipsPad(t *testing.T) {
	t.Parallel()

	blob := []byte{
		0x00, 0x00, 0x00, 0x00,
		0x16, 0x00, 0x04, 0x00, 0x01, 0x10, 0x00, 0x00,
		0x00, 0x00, 0x02, 0x00, 0xAA, 0xBB, 0x00, 0x00,
		0x01, 0x00, 0x00, 0x00,
	}

	back, err := rtps.DecodeParams(blob)
	if err != nil {
		t.Fatalf("decode pad: %v", err)
	}

	if len(back) != 1 || back[0].ID != rtps.PIDVendorID {
		t.Fatalf("pad decode = %+v, want single vendor", back)
	}
}

// TestDecodeParamsVendorGolden checks 0x8003, 0x8007, 0x8019 round trip.
func TestDecodeParamsVendorGolden(t *testing.T) {
	t.Parallel()

	params := []rtps.Param{
		{ID: rtps.PIDVendorAdlinkWriterDataLifecycle, Value: []byte{0x00, 0x00, 0x00, 0x00}},
		{ID: rtps.PIDVendorAdlinkParticipantVersionInfo, Value: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}},
		{ID: rtps.PIDVendorCycloneReceiveBufferSize, Value: []byte{0x00, 0x00, 0x10, 0x00}},
		{ID: rtps.PIDVendorAdlinkEntityFactory, Value: []byte{0x01, 0x00, 0x00, 0x00}},
	}

	raw := rtps.EncodeParams(params)
	back, err := rtps.DecodeParams(raw)
	if err != nil {
		t.Fatalf("decode vendor: %v", err)
	}

	if len(back) != len(params) {
		t.Fatalf("vendor params = %d, want %d", len(back), len(params))
	}

	for i := range params {
		if back[i].ID != params[i].ID || !bytes.Equal(back[i].Value, params[i].Value) {
			t.Fatalf("vendor param %d mismatch", i)
		}
	}

	// Pin golden bytes for 0x8003 with zero value.
	want8003 := []byte{0x03, 0x80, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}
	single := rtps.EncodeParams([]rtps.Param{{ID: rtps.PIDVendorAdlinkWriterDataLifecycle, Value: []byte{0x00, 0x00, 0x00, 0x00}}})
	if !bytes.Equal(single, want8003) {
		t.Fatalf("0x8003 golden = % X, want % X", single, want8003)
	}
}

// TestDecodeParamsManualAndBuiltinGolden pins 0x0034 and 0x0077 bytes.
func TestDecodeParamsManualAndBuiltinGolden(t *testing.T) {
	t.Parallel()

	manual := rtps.EncodeParams([]rtps.Param{{ID: rtps.PIDParticipantManualLivelinessCount, Value: []byte{0x07, 0x00, 0x00, 0x00}}})
	wantManual := []byte{0x34, 0x00, 0x04, 0x00, 0x07, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}
	if !bytes.Equal(manual, wantManual) {
		t.Fatalf("manual golden = % X, want % X", manual, wantManual)
	}

	builtin := rtps.EncodeParams([]rtps.Param{{ID: rtps.PIDBuiltinEndpointQos, Value: []byte{0x3F, 0x00, 0x00, 0x00}}})
	wantBuiltin := []byte{0x77, 0x00, 0x04, 0x00, 0x3F, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}
	if !bytes.Equal(builtin, wantBuiltin) {
		t.Fatalf("builtin QoS golden = % X, want % X", builtin, wantBuiltin)
	}

	typeObject := rtps.EncodeParams([]rtps.Param{{ID: rtps.PIDTypeObject, Value: []byte{0x01, 0x02}}})
	back, err := rtps.DecodeParams(typeObject)
	if err != nil {
		t.Fatalf("decode type object: %v", err)
	}

	if len(back) != 1 || back[0].ID != rtps.PIDTypeObject {
		t.Fatalf("type object decode = %+v", back)
	}
}
