// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package helpers

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"testing"
	"time"
)

// DistroPeer describes one ROS distro peer for the live matrix.
//
// Container is the compose container name. SetupBash is the setup file
// that the structured environment replicates. Domain isolates DDS traffic
// so distros never hear each other and never hear canonical 47 or 48 or
// ephemeral 60-69. RosBin is the ros2 binary in the peer.
type DistroPeer struct {
	Distro    string
	Container string
	SetupBash string
	Domain    int
	RosBin    string
}

// Canonical distro peers for the ROS String matrix.
//
// Jazzy on domain 42 is the canonical gate peer. Humble on domain 43 and
// Rolling on domain 44 extend coverage with no cross talk. Domains are
// distinct so one run never hears another distro run, never hears live
// domain 47 or regression domain 48, and never hears ephemeral domains
// 60 through 69 used by Cyclone and ROS liveliness plus CLI isolation.
var (
	// DistroJazzy is the canonical Jazzy peer on domain 42.
	DistroJazzy = DistroPeer{
		Distro:    "jazzy",
		Container: "weld-jazzy",
		SetupBash: "/opt/ros/jazzy/setup.bash",
		Domain:    42,
		RosBin:    "/opt/ros/jazzy/bin/ros2",
	}
	// DistroHumble is the Humble peer on domain 43.
	DistroHumble = DistroPeer{
		Distro:    "humble",
		Container: "weld-humble",
		SetupBash: "/opt/ros/humble/setup.bash",
		Domain:    43,
		RosBin:    "/opt/ros/humble/bin/ros2",
	}
	// DistroRolling is the Rolling peer on domain 44.
	DistroRolling = DistroPeer{
		Distro:    "rolling",
		Container: "weld-rolling",
		SetupBash: "/opt/ros/rolling/setup.bash",
		Domain:    44,
		RosBin:    "/opt/ros/rolling/bin/ros2",
	}
)

// DistroPeers returns the full matrix in canonical order.
func DistroPeers() []DistroPeer {
	return []DistroPeer{DistroJazzy, DistroHumble, DistroRolling}
}

// DistroEnv returns the structured environment for a distro.
//
// Values replicate what the distro setup.sh exports so docker exec can
// run the ros2 argv directly with -e flags and no shell. Jazzy values
// match ImageEnv. Humble uses python 3.10 with an extra local
// dist-packages entry for Ubuntu 22.04. Jazzy uses python 3.12 and
// Rolling uses python 3.14. Values verified against setup.sh output in
// each running peer. If a future Rolling base moves Ubuntu releases,
// update the PYTHONPATH here to match the new setup.sh output.
func DistroEnv(distro string) [][2]string {
	prefix := "/opt/ros/" + distro
	python := "python3.12"
	if distro == "humble" {
		python = "python3.10"
	}
	if distro == "rolling" {
		python = "python3.14"
	}
	pythonPath := prefix + "/lib/" + python + "/site-packages"
	if distro == "humble" {
		pythonPath += ":" + prefix + "/local/lib/python3.10/dist-packages"
	}
	return [][2]string{
		{"AMENT_PREFIX_PATH", prefix},
		{"LD_LIBRARY_PATH", prefix + "/opt/zenoh_cpp_vendor/lib:" + prefix + "/lib/x86_64-linux-gnu:" + prefix + "/lib"},
		{"PATH", prefix + "/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
		{"PYTHONPATH", pythonPath},
		{"ROS_DISTRO", distro},
		{"ROS_VERSION", "2"},
		{"ROS_PYTHON_VERSION", "3"},
	}
}

// DistroDockerArgs builds a docker exec argv for a distro peer.
//
// The argv starts with exec and carries ROS_DOMAIN_ID plus the RMW
// implementation and the distro setup as -e flags, then the container name
// and the command. No shell is used at any point. RMW is always
// rmw_cyclonedds_cpp. FastDDS stays out of the gates because v1 scope is
// Cyclone only: FastDDS uses distinct discovery and type negotiation that
// the v1 backend does not claim, so gating on it would pin behavior Weld
// does not implement.
func DistroDockerArgs(peer DistroPeer, cmd ...string) []string {
	return dockerExecArgs([]string{"exec"}, peer.Domain, DistroEnv(peer.Distro), peer.Container, cmd...)
}

// DistroDetachedArgs builds a docker exec -d argv for a distro peer.
//
// It carries the same ROS_DOMAIN_ID plus RMW and distro setup -e flags as
// DistroDockerArgs, then the container name and the command. The -d flag
// detaches, so the caller never holds a host pipe for the peer proc.
// Output redirection must happen inside the container to the logs volume.
func DistroDetachedArgs(peer DistroPeer, cmd ...string) []string {
	return dockerExecArgs([]string{"exec", "-d"}, peer.Domain, DistroEnv(peer.Distro), peer.Container, cmd...)
}

// StartDistroPeerProc starts a detached distro peer proc writing to the logs volume.
//
// rosArgs is the container argv, typically peer RosBin plus topic pub or
// echo. The proc runs via docker exec -d with sh -c only for pidfile plus
// redirection inside the container. Host side uses argv with no shell.
// Output lands in ContainerOutPath(runID), visible on the host at
// HostOutPath. The pidfile at ContainerPidPath holds the peer PID.
func StartDistroPeerProc(ctx context.Context, peer DistroPeer, runID string, rosArgs ...string) error {
	if runID == "" {
		return errors.New("helpers: empty run ID for detached distro peer") //nolint:wrapcheck // new sentinel error, nothing to wrap
	}
	err := validateRunIDPath(runID)
	if err != nil {
		return err
	}
	if len(rosArgs) == 0 {
		return fmt.Errorf("helpers: no ros args for detached distro peer %q", runID)
	}
	outPath := ContainerOutPath(runID)
	pidPath := ContainerPidPath(runID)
	script := buildDetachedScript(outPath, pidPath, rosArgs)
	args := DistroDetachedArgs(peer, "sh", "-c", script)
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness invokes docker with fixed args from test tables
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("helpers: docker exec -d %s: %w output %s", runID, err, string(out))
	}
	return nil
}

// SkipIfNoDistroPeer skips the test when the distro peer is absent.
//
// It never starts the compose stack. It probes with docker inspect through
// ContainerRunning and skips cleanly when the daemon is unavailable or the
// container is missing or stopped. Unexpected probe failures also skip
// with context. It never skips for assertion failures: distro setup,
// wire, and payload checks must fail so flakes surface.
func SkipIfNoDistroPeer(t *testing.T, ctx context.Context, peer DistroPeer) {
	t.Helper()
	SkipIfNoPeer(t, ctx, peer.Container)
}

// MustHaveDistroPeer fails the test when the distro peer is absent.
//
// It runs the same probes as SkipIfNoDistroPeer but calls t.Fatalf
// instead of t.Skipf. Use it for strict distro gates that require the
// peer; a missing daemon or container is a setup failure, not a skip.
func MustHaveDistroPeer(t *testing.T, ctx context.Context, peer DistroPeer) {
	t.Helper()
	MustHavePeer(t, ctx, peer.Container)
}

// SkipIfNoDistroTypeInterface skips when the distro peer lacks the ROS type.
//
// It runs ros2 interface show via DistroDockerArgs with no shell using the
// per distro env from DistroEnv. A non-zero exit means the peer does not
// carry the package, so the caller skips cleanly instead of failing the
// matrix. Per distro env keeps Humble and Rolling probes from running under
// the Jazzy ImageEnv and false skipping.
func SkipIfNoDistroTypeInterface(t *testing.T, ctx context.Context, peer DistroPeer, rosType string, probeWait time.Duration) {
	t.Helper()
	probeCtx, cancel := context.WithTimeout(ctx, probeWait)
	defer cancel()
	args := DistroDockerArgs(peer, peer.RosBin, "interface", "show", rosType)
	cmd := exec.CommandContext(probeCtx, "docker", args...) //nolint:gosec // live harness runs docker with controlled args
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Skipf("ros2 interface show %s absent in %s, skipping: %v output %s", rosType, peer.Distro, err, string(out))
	}
}

// DistroSetupExists reports whether the distro setup file exists in the peer.
//
// It runs docker exec test -f with argv only and no shell. The caller
// decides skip versus fail. A missing container returns false with no
// error so the caller can skip cleanly. Other failures return an error
// with context for the caller to report.
func DistroSetupExists(ctx context.Context, peer DistroPeer) (bool, error) {
	probeCtx, cancel := context.WithTimeout(ctx, DockerProbeTimeout)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "docker", "exec", peer.Container, "test", "-f", peer.SetupBash) //nolint:gosec // live harness probes fixed container path with docker argv and no shell
	err := cmd.Run()
	if err == nil {
		return true, nil
	}
	if probeCtx.Err() != nil {
		return false, fmt.Errorf("helpers: setup probe %s timed out: %w", peer.Container, probeCtx.Err())
	}
	running, rerr := ContainerRunning(ctx, peer.Container)
	if rerr != nil {
		return false, rerr
	}
	if !running {
		return false, nil
	}
	return false, nil
}
