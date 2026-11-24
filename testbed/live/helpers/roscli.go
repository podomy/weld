// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

//nolint:dupl // publish retry helpers share shape across weld and ros2 paths, splitting would obscure live gate flow
package helpers

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"time"
)

// RunRosEcho runs stock ros2 topic echo in the peer container and returns captured output.
//
// It mirrors RunWeldEcho but targets the ros-jazzy peer in both directions
// (ROS talker to ROS echo) with no weld binary. The echo runs with an
// explicit --timeout derived from echoTimeout so the probe always exits,
// matching the datapath gate pattern. The outer context adds EchoMargin so
// ros2 exits on its own timeout and flushes before the outer fires. Docker
// argv uses structured env with no shell. The caller passes a per-run topic
// from LiveTopic so concurrent runs stay isolated.
func RunRosEcho(ctx context.Context, container, topic string, domain int, echoTimeout time.Duration) (string, string, error) {
	if container == "" {
		container = Container
	}
	if echoTimeout <= 0 {
		echoTimeout = EchoTimeout
	}
	secs := int(echoTimeout / time.Second)
	if secs <= 0 {
		secs = 1
	}
	runCtx, cancel := context.WithTimeout(ctx, echoTimeout+EchoMargin)
	defer cancel()
	args := DockerArgs(container, domain, RosBin, "topic", "echo", topic, RosType, "--timeout", strconv.Itoa(secs))
	cmd := exec.CommandContext(runCtx, "docker", args...) //nolint:gosec // live harness runs docker with controlled test args and no shell
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String(), stderr.String(), err
	}
	return stdout.String(), stderr.String(), nil
}

// runRosPubOnce runs one stock ros2 topic pub --once in the peer container.
//
// The payload carries the per-run live ID inside a String YAML arg. Docker
// argv uses structured env with no shell. PubTimeout bounds the run.
func runRosPubOnce(ctx context.Context, container, topic string, domain int, payload string) (string, string, error) {
	if container == "" {
		container = Container
	}
	runCtx, cancel := context.WithTimeout(ctx, PubTimeout)
	defer cancel()
	pubArg := fmt.Sprintf("{data: %s}", payload)
	args := DockerArgs(container, domain, RosBin, "topic", "pub", topic, RosType, pubArg, "--once")
	cmd := exec.CommandContext(runCtx, "docker", args...) //nolint:gosec // live harness runs docker with controlled test args and no shell
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return stdout.String(), stderr.String(), err
	}
	return stdout.String(), stderr.String(), nil
}

// RosPubUntilSeen publishes via stock ros2 topic pub and polls the ROS echo log until it holds the payload.
//
// It mirrors PublishUntilSeen but targets the ros-jazzy peer in both
// directions (ROS pub to ROS echo) with no weld binary. It calls
// runRosPubOnce then WaitForFileContains for payload in echoPath, retrying
// the publish on miss up to maxPubs times. A pub error returns immediately
// as err with seen false and the last stdout and stderr. A miss after
// exhausting attempts returns seen false with nil err so the caller fails
// on the missing payload. Every attempt is bounded by ctx: the remaining
// time until the caller deadline is split across attempts left and caps
// pollWait, so the helper never overruns ctx. Non-positive maxPubs means
// one attempt and non-positive pollWait means FilePollTimeout. Topic and
// payload carry the per-run live ID so concurrent runs stay isolated.
func RosPubUntilSeen(ctx context.Context, container, topic string, domain int, payload, echoPath string, pollWait time.Duration, maxPubs int) (seen bool, stdout, stderr string, err error) {
	maxPubs, pollWait = normalizePublishBounds(maxPubs, pollWait)
	for attempt := range maxPubs {
		err = checkPublishCtx(ctx)
		if err != nil {
			return false, stdout, stderr, err
		}
		attemptsLeft := maxPubs - attempt
		var attemptPoll time.Duration
		attemptPoll, err = attemptPollWait(ctx, pollWait, attemptsLeft)
		if err != nil {
			return false, stdout, stderr, err
		}
		stdout, stderr, err = runRosPubOnce(ctx, container, topic, domain, payload)
		if err != nil {
			return false, stdout, stderr, err
		}
		if WaitForFileContains(ctx, echoPath, payload, attemptPoll) {
			return true, stdout, stderr, nil
		}
	}
	return false, stdout, stderr, nil
}
