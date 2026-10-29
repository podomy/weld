// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cyclone

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/transports/dds/rtps"
	"github.com/podomy/weld/testbed/live/helpers"
)

// Shared bounds for discovery live cases on allocated exclusive domains.
const (
	discoveryLease       time.Duration = 10 * time.Second
	discoveryDialTimeout time.Duration = 3 * time.Second
	discoveryRecvTimeout time.Duration = 6 * time.Second
	discoveryOverall     time.Duration = 20 * time.Second
	discoveryDockerProbe time.Duration = 5 * time.Second
	discoveryShortLease  time.Duration = 1 * time.Second
	discoverySweepSlack  time.Duration = 300 * time.Millisecond
)

// discoveryPinDomain validates one allocated domain for a test.
//
// Isolation uses explicit domain arguments for sockets and backends plus
// DockerArgs -e for peers, never process env, so tests stay parallel safe.
func discoveryPinDomain(t *testing.T, domain uint32) {
	t.Helper()
	if domain > transports.MaxDomainID {
		t.Fatalf("domain %d above maximum %d", domain, transports.MaxDomainID)
	}
}

// discoveryCloseMulticast closes a multicast conn and logs failures.
func discoveryCloseMulticast(t *testing.T, mc *rtps.MulticastConn) {
	t.Helper()
	err := mc.Close()
	if err != nil {
		t.Logf("multicast close: %v", err)
	}
}

// discoveryCloseUnicast closes a unicast conn and logs failures.
func discoveryCloseUnicast(t *testing.T, uc *rtps.UnicastConn) {
	t.Helper()
	err := uc.Close()
	if err != nil {
		t.Logf("unicast close: %v", err)
	}
}

// discoveryCloseSender closes a multicast sender and logs failures.
func discoveryCloseSender(t *testing.T, s *rtps.MulticastSender) {
	t.Helper()
	err := s.Close()
	if err != nil {
		t.Logf("sender close: %v", err)
	}
}

// discoverySetup validates domain, bounds context, and cross-checks docker.
//
// The domain travels explicitly to sockets and backends plus DockerArgs -e
// for peers, never via process env.
func discoverySetup(t *testing.T, domain uint32) (context.Context, context.CancelFunc) {
	t.Helper()
	discoveryPinDomain(t, domain)
	ctx, cancel := context.WithTimeout(context.Background(), discoveryOverall)
	discoveryDockerCrossCheck(t, ctx)
	return ctx, cancel
}

// discoveryJoinAndSender joins multicast and dials sender with cleanup.
func discoveryJoinAndSender(t *testing.T, ctx context.Context, domain uint32) (*rtps.MulticastConn, *rtps.MulticastSender) {
	t.Helper()
	joinCtx, joinCancel := context.WithTimeout(ctx, discoveryDialTimeout)
	mc, err := rtps.JoinMulticast(joinCtx, domain)
	joinCancel()
	if err != nil {
		t.Fatalf("join multicast: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseMulticast(t, mc)
	})
	sender, err := rtps.DialMulticastSender(domain)
	if err != nil {
		t.Fatalf("dial sender: %v", err)
	}
	t.Cleanup(func() {
		discoveryCloseSender(t, sender)
	})
	return mc, sender
}

// discoverySendTwice sends one packet twice for late joiner coverage.
func discoverySendTwice(t *testing.T, sender *rtps.MulticastSender, packet []byte) {
	t.Helper()
	err := sender.Send(packet)
	if err != nil {
		t.Fatalf("multicast send: %v", err)
	}
	err = sender.Send(packet)
	if err != nil {
		t.Fatalf("multicast resend: %v", err)
	}
}

// discoveryDockerCrossCheck logs peer container presence with docker argv only.
//
// It uses os/exec with fixed docker argv and context timeouts and never
// starts the compose stack. It never fails the test and never parses trace
// output. It only records whether weld-jazzy is running beside the live run.
func discoveryDockerCrossCheck(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := exec.LookPath("docker")
	if err != nil {
		t.Logf("docker not found, skipping container cross-check: %v", err)
		return
	}
	probeCtx, cancel := context.WithTimeout(ctx, discoveryDockerProbe)
	defer cancel()
	probe := exec.CommandContext(probeCtx, "docker", "inspect", "--format", "{{.State.Running}}", helpers.Container) //nolint:gosec // live harness inspects fixed test container with docker argv and no shell
	out, err := probe.CombinedOutput()
	if err != nil {
		t.Logf("docker inspect unavailable: %v output %q", err, string(out))
		return
	}
	t.Logf("container %s inspect output bytes %d", helpers.Container, len(out))
	logCtx, logCancel := context.WithTimeout(ctx, discoveryDockerProbe)
	defer logCancel()
	raw, err := exec.CommandContext(logCtx, "docker", "logs", "--tail", "5", helpers.Container).CombinedOutput() //nolint:gosec // live harness reads fixed test container logs with docker argv and no shell
	if err != nil {
		t.Logf("docker logs unavailable: %v", err)
		return
	}
	t.Logf("container %s logs tail bytes %d", helpers.Container, len(raw))
}

// isDiscoveryAnnouncerPacket reports whether raw is alive from announcer not observer.
func isDiscoveryAnnouncerPacket(raw []byte, observer, announcer *dds.LocalParticipant, domain uint32) (dds.Announcement, bool) {
	if !containsGateSPDP(raw) {
		return dds.Announcement{}, false
	}
	ann, perr := dds.ParseAnnouncement(raw)
	if perr != nil {
		return dds.Announcement{}, false
	}
	if ann.DomainID != domain {
		return dds.Announcement{}, false
	}
	if !ann.Alive {
		return dds.Announcement{}, false
	}
	if ann.GUID.Prefix == observer.GUID().Prefix {
		return dds.Announcement{}, false
	}
	if ann.GUID.Prefix != announcer.GUID().Prefix {
		return dds.Announcement{}, false
	}
	return ann, true
}
