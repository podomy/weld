// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
)

// Action send defaults for the composition root CLI verb.
const (
	// actionParticipant names the action client side in discovery.
	actionParticipant = "weld-action-client"
	// defaultActionTimeout bounds one action send besides SIGINT cancel.
	defaultActionTimeout = 60 * time.Second
)

// openActionBackendFunc opens the live backend for action sends.
//
// It is a variable so tests inject a fake without sockets.
var openActionBackendFunc = openBackend

// runActionCommand dispatches action send and maps errors to exit codes.
func runActionCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		_, _ = fmt.Fprintln(stderr, "weld: action needs one of send") //nolint:errcheck // final error output is best effort

		return 2
	}

	switch args[0] {
	case "send":
		return runActionSendCommand(ctx, args[1:], stdout, stderr)
	case "-h", "-help", "--help", "help":
		printUsage(stdout)

		return 0
	default:
		printUsage(stderr)
		_, _ = fmt.Fprintf(stderr, "weld: unknown action subcommand %q\n", args[0]) //nolint:errcheck // final error output is best effort

		return 2
	}
}

// runActionSendCommand parses action send flags and runs one goal then exits.
func runActionSendCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("action send", flag.ContinueOnError)
	fs.SetOutput(stderr)

	if isHelpRequest(args) {
		printUsage(stdout)

		return 0
	}

	nameFlag := fs.String("name", "", "ROS action name such as /navigate_to_pose")
	actionAlias := fs.String("action", "", "alias for --name")
	goalFlag := fs.String("goal", "", "action goal data")
	timeoutFlag := fs.Duration("timeout", defaultActionTimeout, "overall goal timeout")
	cancelAfterFlag := fs.Duration("cancel-after", 0, "cancel goal after this delay, 0 disables cancel")
	domainFlag := fs.Uint("domain", uint(defaultDomainID()), "DDS domain id, default from ROS_DOMAIN_ID")

	err := fs.Parse(args)
	if err != nil {
		return 2
	}

	name, goal, err := resolveActionSendArgs(*nameFlag, *actionAlias, *goalFlag, fs.Args())
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	err = validateActionTiming(*timeoutFlag, *cancelAfterFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	domain, err := resolveEffectiveDomain(*domainFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	err = validateTopic(name)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	err = runActionSend(ctx, name, goal, domain, *timeoutFlag, *cancelAfterFlag, stdout)
	if err != nil {
		return handleRunError(err, stderr)
	}

	return 0
}

// resolveActionSendArgs picks action name and goal from flags or positionals.
func resolveActionSendArgs(flagName, flagAlias, flagGoal string, args []string) (string, string, error) {
	name := flagName
	if name == "" {
		name = flagAlias
	}

	rest := args

	if name == "" {
		if len(rest) == 0 {
			return "", "", errors.New("weld: action send needs --name or one positional action") //nolint:wrapcheck // new error, nothing to wrap
		}

		name = rest[0]
		rest = rest[1:]
	}

	if flagGoal != "" {
		if len(rest) != 0 {
			return "", "", errors.New("weld: action send takes one goal, have flag plus positionals") //nolint:wrapcheck // new error, nothing to wrap
		}

		return name, flagGoal, nil
	}

	if len(rest) == 0 {
		return name, "", nil
	}

	if len(rest) == 1 {
		return name, rest[0], nil
	}

	return "", "", errors.New("weld: action send takes one goal, have too many args") //nolint:wrapcheck // new error, nothing to wrap
}

// validateActionTiming rejects non positive timeout and negative cancel delay.
func validateActionTiming(timeout, cancelAfter time.Duration) error {
	if timeout <= 0 {
		return errors.New("weld: action timeout must be positive") //nolint:wrapcheck // new error, nothing to wrap
	}

	if cancelAfter < 0 {
		return errors.New("weld: action cancel-after must not be negative") //nolint:wrapcheck // new error, nothing to wrap
	}

	return nil
}

// checkActionEndpoints validates action ROS mapping plus core admission.
func checkActionEndpoints(action string) error {
	_, err := dds.ActionDDSTopicsFor(action, false)
	if err != nil {
		return fmt.Errorf("weld: map action endpoints: %w", err)
	}

	_, err = core.NewActionServer(action, core.ClockWall, 0)
	if err != nil {
		return fmt.Errorf("weld: create action endpoint: %w", err)
	}

	return nil
}

// newActionGoalID returns one random hex goal id for the client side.
func newActionGoalID() (string, error) {
	var raw [16]byte

	_, err := rand.Read(raw[:])
	if err != nil {
		return "", fmt.Errorf("weld: new action goal id: %w", err)
	}

	return hex.EncodeToString(raw[:]), nil
}

// subscribeActionFeedback declares the feedback reader when supported.
func subscribeActionFeedback(ctx context.Context, backend transports.Backend, action string) error {
	sub, ok := backend.(serviceSubscriber)
	if !ok {
		return nil
	}

	feedback, err := dds.FeedbackTopicName(action)
	if err != nil {
		return fmt.Errorf("weld: map action feedback: %w", err)
	}

	err = sub.Subscribe(ctx, feedback, stringTypeName, stringTypeHash)
	if err != nil {
		return fmt.Errorf("weld: subscribe %q: %w", feedback, err)
	}

	return nil
}

// publishActionGoal retries PublishBytes on ErrNoRoute until timeout or ctx ends.
func publishActionGoal(ctx context.Context, backend transports.Backend, out transports.OutgoingMessage, timeout time.Duration) error {
	return publishWithRouteRetry(ctx, backend, out, timeout)
}

// actionSendWithBackend publishes one goal and streams feedback then result.
func actionSendWithBackend(ctx context.Context, backend transports.Backend, server *core.ActionServer, goalID, action, goal string, timeout, cancelAfter time.Duration, stdout io.Writer) (string, error) {
	err := validateActionTiming(timeout, cancelAfter)
	if err != nil {
		return "", err
	}

	sendGoal, err := dds.SendGoalServiceName(action)
	if err != nil {
		return "", fmt.Errorf("weld: map action send goal: %w", err)
	}

	feedbackROS, err := dds.FeedbackTopicName(action)
	if err != nil {
		return "", fmt.Errorf("weld: map action feedback: %w", err)
	}

	getResult, err := dds.GetResultServiceName(action)
	if err != nil {
		return "", fmt.Errorf("weld: map action get result: %w", err)
	}

	cancelROS, err := dds.CancelGoalServiceName(action)
	if err != nil {
		return "", fmt.Errorf("weld: map action cancel: %w", err)
	}

	statusROS, err := dds.StatusTopicName(action)
	if err != nil {
		return "", fmt.Errorf("weld: map action status: %w", err)
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	wire, err := encodeStringPayload(goal)
	if err != nil {
		return "", err
	}

	out := transports.OutgoingMessage{
		TopicName: sendGoal,
		TypeName:  stringTypeName,
		TypeHash:  stringTypeHash,
		Payload:   wire,
	}

	err = publishActionGoal(callCtx, backend, out, timeout)
	if err != nil {
		if errors.Is(err, dds.ErrNoRoute) {
			return "", fmt.Errorf("weld: action %q: no server: %w", action, err)
		}

		if errors.Is(err, context.Canceled) {
			return "", fmt.Errorf("weld: action %q: canceled: %w", action, context.Canceled)
		}

		if callCtx.Err() != nil {
			return "", fmt.Errorf("weld: action %q: timeout %v exceeded: %w", action, timeout, context.DeadlineExceeded)
		}

		return "", fmt.Errorf("weld: action %q: publish goal: %w", action, err)
	}

	start := time.Now()

	for {
		if cancelAfter > 0 && time.Since(start) >= cancelAfter {
			nowNanos := time.Now().UnixNano()
			_ = server.CancelGoal(goalID, nowNanos) //nolint:errcheck // cancel is best effort, loud error below carries the failure

			return "", fmt.Errorf("weld: action %q: canceled after %v: %w", action, cancelAfter, context.Canceled)
		}

		if callCtx.Err() != nil {
			if errors.Is(callCtx.Err(), context.Canceled) && ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
				nowNanos := time.Now().UnixNano()
				_ = server.CancelGoal(goalID, nowNanos) //nolint:errcheck // cancel is best effort, loud error below carries the failure

				return "", fmt.Errorf("weld: action %q: canceled: %w", action, context.Canceled)
			}

			return "", fmt.Errorf("weld: action %q: timeout %v exceeded: %w", action, timeout, context.DeadlineExceeded)
		}

		remaining := timeout - time.Since(start)
		if remaining <= 0 {
			return "", fmt.Errorf("weld: action %q: timeout %v exceeded: %w", action, timeout, context.DeadlineExceeded)
		}

		takeTimeout := remaining
		if cancelAfter > 0 {
			cancelRemaining := cancelAfter - time.Since(start)
			if cancelRemaining <= 0 {
				nowNanos := time.Now().UnixNano()
				_ = server.CancelGoal(goalID, nowNanos) //nolint:errcheck // cancel is best effort, loud error below carries the failure

				return "", fmt.Errorf("weld: action %q: canceled after %v: %w", action, cancelAfter, context.Canceled)
			}

			if cancelRemaining < takeTimeout {
				takeTimeout = cancelRemaining
			}
		}

		takeCtx, takeCancel := context.WithTimeout(callCtx, takeTimeout)
		msg, takeErr := backend.TakeBytes(takeCtx)
		takeCancel()

		if takeErr != nil {
			if errors.Is(callCtx.Err(), context.Canceled) && ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
				nowNanos := time.Now().UnixNano()
				_ = server.CancelGoal(goalID, nowNanos) //nolint:errcheck // cancel is best effort, loud error below carries the failure

				return "", fmt.Errorf("weld: action %q: canceled: %w", action, context.Canceled)
			}

			if cancelAfter > 0 && time.Since(start) >= cancelAfter {
				nowNanos := time.Now().UnixNano()
				_ = server.CancelGoal(goalID, nowNanos) //nolint:errcheck // cancel is best effort, loud error below carries the failure

				return "", fmt.Errorf("weld: action %q: canceled after %v: %w", action, cancelAfter, context.Canceled)
			}

			if callCtx.Err() != nil || time.Since(start) >= timeout {
				return "", fmt.Errorf("weld: action %q: timeout %v exceeded: %w", action, timeout, context.DeadlineExceeded)
			}

			if isSkippableSample(takeErr) {
				continue
			}

			if errors.Is(takeErr, context.DeadlineExceeded) || errors.Is(takeErr, context.Canceled) {
				continue
			}

			return "", fmt.Errorf("weld: action %q: take feedback: %w", action, takeErr)
		}

		if msg.TopicName == statusROS {
			continue
		}

		allowed := msg.TopicName == "" || msg.TopicName == action || msg.TopicName == sendGoal || msg.TopicName == feedbackROS || msg.TopicName == getResult || msg.TopicName == cancelROS
		if !allowed {
			continue
		}

		text, err := decodeStringPayload(msg.Payload)
		if err != nil {
			return "", fmt.Errorf("weld: action %q: decode message: %w", action, err)
		}

		if strings.HasPrefix(text, "result:") {
			trimmed := strings.TrimSpace(strings.TrimPrefix(text, "result:"))
			nowNanos := time.Now().UnixNano()

			err = server.CompleteGoal(goalID, core.GoalSucceeded, []byte(trimmed), nowNanos)
			if err != nil {
				return "", fmt.Errorf("weld: action %q: complete goal: %w", action, err)
			}

			return trimmed, nil
		}

		if strings.HasPrefix(text, "feedback:") {
			trimmed := strings.TrimSpace(strings.TrimPrefix(text, "feedback:"))
			nowNanos := time.Now().UnixNano()

			err = server.PublishFeedback(goalID, []byte(trimmed), nowNanos)
			if err != nil {
				return "", fmt.Errorf("weld: action %q: feedback: %w", action, err)
			}

			_, err = fmt.Fprintln(stdout, "feedback: "+trimmed)
			if err != nil {
				return "", fmt.Errorf("weld: print action feedback: %w", err)
			}

			continue
		}

		if msg.TopicName == feedbackROS {
			nowNanos := time.Now().UnixNano()

			err = server.PublishFeedback(goalID, []byte(text), nowNanos)
			if err != nil {
				return "", fmt.Errorf("weld: action %q: feedback: %w", action, err)
			}

			_, err = fmt.Fprintln(stdout, "feedback: "+text)
			if err != nil {
				return "", fmt.Errorf("weld: print action feedback: %w", err)
			}

			continue
		}

		nowNanos := time.Now().UnixNano()

		err = server.CompleteGoal(goalID, core.GoalSucceeded, []byte(text), nowNanos)
		if err != nil {
			return "", fmt.Errorf("weld: action %q: complete goal: %w", action, err)
		}

		return text, nil
	}
}

// openSubscribedActionBackend opens the backend and declares the feedback reader.
func openSubscribedActionBackend(ctx context.Context, domain uint32, action string) (transports.Backend, error) {
	backend, err := openActionBackendFunc(ctx, domain, actionParticipant)
	if err != nil {
		return nil, fmt.Errorf("weld: open backend for action %q: %w", action, err)
	}

	err = subscribeActionFeedback(ctx, backend, action)
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return nil, fmt.Errorf("weld: action send: %w", errors.Join(err, cerr))
		}

		return nil, err
	}

	return backend, nil
}

// runActionSend validates, opens the backend, sends one goal, and prints the result.
func runActionSend(ctx context.Context, action, goal string, domain uint32, timeout, cancelAfter time.Duration, stdout io.Writer) error {
	err := validateTopic(action)
	if err != nil {
		return err
	}

	err = validateActionTiming(timeout, cancelAfter)
	if err != nil {
		return err
	}

	err = checkActionEndpoints(action)
	if err != nil {
		return err
	}

	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	backend, err := openSubscribedActionBackend(callCtx, domain, action)
	if err != nil {
		return err
	}

	goalID, err := newActionGoalID()
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return fmt.Errorf("weld: action send: %w", errors.Join(err, cerr))
		}

		return err
	}

	nowNanos := time.Now().UnixNano()
	server, err := core.NewActionServer(action, core.ClockWall, 0)
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return fmt.Errorf("weld: action send: %w", errors.Join(err, cerr))
		}

		return fmt.Errorf("weld: create action workflow: %w", err)
	}

	server.Activate(nowNanos)

	err = server.AcceptGoal(goalID, nowNanos, core.ClockWall, nowNanos)
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return fmt.Errorf("weld: action send: %w", errors.Join(err, cerr))
		}

		return fmt.Errorf("weld: action %q: accept goal: %w", action, err)
	}

	resp, callErr := actionSendWithBackend(callCtx, backend, server, goalID, action, goal, timeout, cancelAfter, stdout)

	cerr := backend.Close()
	if callErr != nil && cerr != nil {
		return fmt.Errorf("weld: action send: %w", errors.Join(callErr, cerr))
	}

	if cerr != nil {
		return fmt.Errorf("weld: close action backend: %w", cerr)
	}

	if callErr != nil {
		return callErr
	}

	_, err = fmt.Fprintln(stdout, "result: "+resp)
	if err != nil {
		return fmt.Errorf("weld: print action result: %w", err)
	}

	return nil
}
