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
	if flagName != "" && flagAlias != "" && flagName != flagAlias {
		return "", "", errors.New("weld: action send takes one name, have conflicting --name and --action") //nolint:wrapcheck // new error, nothing to wrap
	}

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
		return "", "", errors.New("weld: action send needs --goal or one positional goal") //nolint:wrapcheck // new error, nothing to wrap
	}

	if len(rest) == 1 {
		if rest[0] == "" {
			return "", "", errors.New("weld: action send goal must not be empty") //nolint:wrapcheck // new error, nothing to wrap
		}

		return name, rest[0], nil
	}

	return "", "", errors.New("weld: action send takes one goal, have too many args") //nolint:wrapcheck // new error, nothing to wrap
}

// validateActionTiming rejects non positive timeout, negative cancel delay,
// and cancel delay beyond timeout.
func validateActionTiming(timeout, cancelAfter time.Duration) error {
	if timeout <= 0 {
		return errors.New("weld: action timeout must be positive") //nolint:wrapcheck // new error, nothing to wrap
	}

	if cancelAfter < 0 {
		return errors.New("weld: action cancel-after must not be negative") //nolint:wrapcheck // new error, nothing to wrap
	}

	if cancelAfter > timeout {
		return errors.New("weld: action cancel-after must not exceed timeout") //nolint:wrapcheck // new error, nothing to wrap
	}

	return nil
}

// checkActionEndpoints validates action ROS mapping plus core admission.
func checkActionEndpoints(action string) error {
	_, err := dds.ActionDDSTopicsFor(action, false)
	if err != nil {
		return fmt.Errorf("weld: map action %q endpoints: %w", action, err)
	}

	_, err = core.NewActionServer(action, core.ClockWall, 0)
	if err != nil {
		return fmt.Errorf("weld: create action %q endpoint: %w", action, err)
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
		return fmt.Errorf("weld: map action %q feedback: %w", action, err)
	}

	err = sub.Subscribe(ctx, feedback, stringTypeName, stringTypeHash)
	if err != nil {
		return fmt.Errorf("weld: subscribe action %q feedback %q: %w", action, feedback, err)
	}

	return nil
}

// publishActionGoal retries PublishBytes on ErrNoRoute until timeout or ctx ends.
func publishActionGoal(ctx context.Context, backend transports.Backend, out transports.OutgoingMessage, timeout time.Duration) error {
	return publishWithRouteRetry(ctx, backend, out, timeout)
}

// actionEndpoints holds resolved ROS names for one action.
type actionEndpoints struct {
	sendGoal    string
	feedbackROS string
	getResult   string
	cancelROS   string
	statusROS   string
}

// resolveActionEndpoints maps action ROS names for send, feedback, result, cancel, and status.
func resolveActionEndpoints(action string) (actionEndpoints, error) {
	var out actionEndpoints
	sendGoal, err := dds.SendGoalServiceName(action)
	if err != nil {
		return out, fmt.Errorf("weld: map action send goal: %w", err)
	}
	feedbackROS, err := dds.FeedbackTopicName(action)
	if err != nil {
		return out, fmt.Errorf("weld: map action feedback: %w", err)
	}
	getResult, err := dds.GetResultServiceName(action)
	if err != nil {
		return out, fmt.Errorf("weld: map action get result: %w", err)
	}
	cancelROS, err := dds.CancelGoalServiceName(action)
	if err != nil {
		return out, fmt.Errorf("weld: map action cancel: %w", err)
	}
	statusROS, err := dds.StatusTopicName(action)
	if err != nil {
		return out, fmt.Errorf("weld: map action status: %w", err)
	}
	out.sendGoal = sendGoal
	out.feedbackROS = feedbackROS
	out.getResult = getResult
	out.cancelROS = cancelROS
	out.statusROS = statusROS
	return out, nil
}

// checkPublishActionError maps publish errors to loud action errors.
func checkPublishActionError(action string, err error, callCtx context.Context, timeout time.Duration) error {
	if errors.Is(err, dds.ErrNoRoute) {
		return fmt.Errorf("weld: action %q: no server: %w", action, err)
	}
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("weld: action %q: canceled: %w", action, context.Canceled)
	}
	if callCtx.Err() != nil {
		return fmt.Errorf("weld: action %q: timeout %v exceeded: %w", action, timeout, context.DeadlineExceeded)
	}
	return fmt.Errorf("weld: action %q: publish goal: %w", action, err)
}

// cancelActionGoal best effort cancels one goal.
func cancelActionGoal(server *core.ActionServer, goalID string) {
	nowNanos := time.Now().UnixNano()
	_ = server.CancelGoal(goalID, nowNanos) //nolint:errcheck // cancel is best effort, loud error below carries the failure
}

// checkActionLoopCancel returns a terminal error when cancel or timeout hit.
func checkActionLoopCancel(ctx, callCtx context.Context, server *core.ActionServer, goalID, action string, start time.Time, timeout, cancelAfter time.Duration) error {
	if cancelAfter > 0 && time.Since(start) >= cancelAfter {
		cancelActionGoal(server, goalID)
		return fmt.Errorf("weld: action %q: canceled after %v: %w", action, cancelAfter, context.Canceled)
	}
	if callCtx.Err() != nil {
		if errors.Is(callCtx.Err(), context.Canceled) && ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
			cancelActionGoal(server, goalID)
			return fmt.Errorf("weld: action %q: canceled: %w", action, context.Canceled)
		}
		return fmt.Errorf("weld: action %q: timeout %v exceeded: %w", action, timeout, context.DeadlineExceeded)
	}
	return nil
}

// computeActionTakeTimeout returns the take timeout or a terminal cancel error.
func computeActionTakeTimeout(server *core.ActionServer, goalID, action string, start time.Time, cancelAfter, remaining time.Duration) (time.Duration, error) {
	takeTimeout := remaining
	if cancelAfter <= 0 {
		return takeTimeout, nil
	}
	cancelRemaining := cancelAfter - time.Since(start)
	if cancelRemaining <= 0 {
		cancelActionGoal(server, goalID)
		return 0, fmt.Errorf("weld: action %q: canceled after %v: %w", action, cancelAfter, context.Canceled)
	}
	if cancelRemaining < takeTimeout {
		takeTimeout = cancelRemaining
	}
	return takeTimeout, nil
}

// checkTakeCanceled returns a terminal error when the take failed due to cancel.
func checkTakeCanceled(ctx, callCtx context.Context, server *core.ActionServer, goalID, action string) error {
	if errors.Is(callCtx.Err(), context.Canceled) && ctx.Err() != nil && errors.Is(ctx.Err(), context.Canceled) {
		cancelActionGoal(server, goalID)
		return fmt.Errorf("weld: action %q: canceled: %w", action, context.Canceled)
	}
	return nil
}

// checkTakeTimeout returns a terminal error when cancel or timeout hit after take.
func checkTakeTimeout(callCtx context.Context, server *core.ActionServer, goalID, action string, start time.Time, timeout, cancelAfter time.Duration) error {
	if cancelAfter > 0 && time.Since(start) >= cancelAfter {
		cancelActionGoal(server, goalID)
		return fmt.Errorf("weld: action %q: canceled after %v: %w", action, cancelAfter, context.Canceled)
	}
	if callCtx.Err() != nil || time.Since(start) >= timeout {
		return fmt.Errorf("weld: action %q: timeout %v exceeded: %w", action, timeout, context.DeadlineExceeded)
	}
	return nil
}

// handleActionTakeError maps take errors to continue or terminal error.
func handleActionTakeError(ctx, callCtx context.Context, server *core.ActionServer, goalID, action string, start time.Time, timeout, cancelAfter time.Duration, takeErr error) error {
	err := checkTakeCanceled(ctx, callCtx, server, goalID, action)
	if err != nil {
		return err
	}
	err = checkTakeTimeout(callCtx, server, goalID, action, start, timeout, cancelAfter)
	if err != nil {
		return err
	}
	if isSkippableSample(takeErr) {
		return nil
	}
	if errors.Is(takeErr, context.DeadlineExceeded) || errors.Is(takeErr, context.Canceled) {
		return nil
	}
	return fmt.Errorf("weld: action %q: take feedback: %w", action, takeErr)
}

// completeActionResult completes one goal with text.
func completeActionResult(server *core.ActionServer, goalID, action, text string) (string, error) {
	nowNanos := time.Now().UnixNano()
	err := server.CompleteGoal(goalID, core.GoalSucceeded, []byte(text), nowNanos)
	if err != nil {
		return "", fmt.Errorf("weld: action %q: complete goal: %w", action, err)
	}
	return text, nil
}

// publishActionFeedback publishes feedback and prints one line.
func publishActionFeedback(server *core.ActionServer, goalID, action, text string, stdout io.Writer) error {
	nowNanos := time.Now().UnixNano()
	err := server.PublishFeedback(goalID, []byte(text), nowNanos)
	if err != nil {
		return fmt.Errorf("weld: action %q: feedback: %w", action, err)
	}
	_, err = fmt.Fprintln(stdout, "feedback: "+text)
	if err != nil {
		return fmt.Errorf("weld: print action feedback: %w", err)
	}
	return nil
}

// handlePrefixedActionText handles result: and feedback: prefixed payloads.
func handlePrefixedActionText(server *core.ActionServer, goalID, action, text string, stdout io.Writer) (string, bool, bool, error) {
	if rest, ok := strings.CutPrefix(text, "result:"); ok {
		trimmed := strings.TrimSpace(rest)
		got, err := completeActionResult(server, goalID, action, trimmed)
		if err != nil {
			return "", true, true, err
		}
		return got, true, true, nil
	}
	if rest, ok := strings.CutPrefix(text, "feedback:"); ok {
		trimmed := strings.TrimSpace(rest)
		err := publishActionFeedback(server, goalID, action, trimmed, stdout)
		if err != nil {
			return "", true, true, err
		}
		return "", false, true, nil
	}
	return "", false, false, nil
}

// isAllowedActionTopic reports whether msg topic belongs to this action.
func isAllowedActionTopic(topic, action string, endpoints actionEndpoints, feedbackROS string) bool {
	if topic == "" {
		return true
	}
	if topic == action {
		return true
	}
	if topic == endpoints.sendGoal {
		return true
	}
	if topic == feedbackROS {
		return true
	}
	if topic == endpoints.getResult {
		return true
	}
	return topic == endpoints.cancelROS
}

// handleActionSample processes one take sample, returning done plus result.
func handleActionSample(server *core.ActionServer, goalID, action, feedbackROS string, msg transports.IncomingMessage, stdout io.Writer, endpoints actionEndpoints) (string, bool, error) {
	if msg.TopicName == endpoints.statusROS {
		return "", false, nil
	}
	if !isAllowedActionTopic(msg.TopicName, action, endpoints, feedbackROS) {
		return "", false, nil
	}
	text, err := decodeStringPayload(msg.Payload)
	if err != nil {
		return "", true, fmt.Errorf("weld: action %q: decode message: %w", action, err)
	}
	got, done, handled, err := handlePrefixedActionText(server, goalID, action, text, stdout)
	if err != nil {
		return "", true, err
	}
	if handled {
		if done {
			return got, true, nil
		}
		return "", false, nil
	}
	if msg.TopicName == feedbackROS {
		err := publishActionFeedback(server, goalID, action, text, stdout)
		if err != nil {
			return "", true, err
		}
		return "", false, nil
	}
	got, err = completeActionResult(server, goalID, action, text)
	if err != nil {
		return "", true, err
	}
	return got, true, nil
}

// actionLoopOnce runs one take plus sample step, returning done plus result.
func actionLoopOnce(ctx, callCtx context.Context, backend transports.Backend, server *core.ActionServer, goalID, action string, start time.Time, timeout, cancelAfter time.Duration, stdout io.Writer, endpoints actionEndpoints) (string, bool, error) {
	err := checkActionLoopCancel(ctx, callCtx, server, goalID, action, start, timeout, cancelAfter)
	if err != nil {
		return "", true, err
	}
	remaining := timeout - time.Since(start)
	if remaining <= 0 {
		return "", true, fmt.Errorf("weld: action %q: timeout %v exceeded: %w", action, timeout, context.DeadlineExceeded)
	}
	takeTimeout, err := computeActionTakeTimeout(server, goalID, action, start, cancelAfter, remaining)
	if err != nil {
		return "", true, err
	}
	takeCtx, takeCancel := context.WithTimeout(callCtx, takeTimeout)
	msg, takeErr := backend.TakeBytes(takeCtx)
	takeCancel()
	if takeErr != nil {
		err := handleActionTakeError(ctx, callCtx, server, goalID, action, start, timeout, cancelAfter, takeErr)
		if err != nil {
			return "", true, err
		}
		return "", false, nil
	}
	got, done, err := handleActionSample(server, goalID, action, endpoints.feedbackROS, msg, stdout, endpoints)
	if err != nil {
		return "", true, err
	}
	if done {
		return got, true, nil
	}
	return "", false, nil
}

// actionSendWithBackend publishes one goal and streams feedback then result.
func actionSendWithBackend(ctx context.Context, backend transports.Backend, server *core.ActionServer, goalID, action, goal string, timeout, cancelAfter time.Duration, stdout io.Writer) (string, error) {
	err := validateActionTiming(timeout, cancelAfter)
	if err != nil {
		return "", err
	}
	endpoints, err := resolveActionEndpoints(action)
	if err != nil {
		return "", err
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	wire, err := encodeStringPayload(action, goal)
	if err != nil {
		return "", fmt.Errorf("weld: action %q: %w", action, err)
	}
	out := transports.OutgoingMessage{
		TopicName: endpoints.sendGoal,
		TypeName:  stringTypeName,
		TypeHash:  stringTypeHash,
		Payload:   wire,
	}
	err = publishActionGoal(callCtx, backend, out, timeout)
	if err != nil {
		return "", checkPublishActionError(action, err, callCtx, timeout)
	}
	start := time.Now()
	for {
		got, done, err := actionLoopOnce(ctx, callCtx, backend, server, goalID, action, start, timeout, cancelAfter, stdout, endpoints)
		if err != nil {
			return "", err
		}
		if done {
			return got, nil
		}
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

// createActionGoalID returns one goal id, closing backend on failure.
func createActionGoalID(backend transports.Backend) (string, error) {
	goalID, err := newActionGoalID()
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return "", fmt.Errorf("weld: action send: %w", errors.Join(err, cerr))
		}
		return "", err
	}
	return goalID, nil
}

// createActionWorkflow creates and activates one action server, closing backend on failure.
func createActionWorkflow(backend transports.Backend, action string, nowNanos int64) (*core.ActionServer, error) {
	server, err := core.NewActionServer(action, core.ClockWall, 0)
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return nil, fmt.Errorf("weld: action send: %w", errors.Join(err, cerr))
		}
		return nil, fmt.Errorf("weld: create action workflow: %w", err)
	}
	server.Activate(nowNanos)
	return server, nil
}

// acceptActionGoal accepts one goal, closing backend on failure.
func acceptActionGoal(backend transports.Backend, server *core.ActionServer, goalID, action string, nowNanos int64) error {
	err := server.AcceptGoal(goalID, nowNanos, core.ClockWall, nowNanos)
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return fmt.Errorf("weld: action send: %w", errors.Join(err, cerr))
		}
		return fmt.Errorf("weld: action %q: accept goal: %w", action, err)
	}
	return nil
}

// finishActionSend closes backend, joins errors, and prints the result.
func finishActionSend(backend transports.Backend, callErr error, resp string, stdout io.Writer) error {
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
	_, err := fmt.Fprintln(stdout, "result: "+resp)
	if err != nil {
		return fmt.Errorf("weld: print action result: %w", err)
	}
	return nil
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

	goalID, err := createActionGoalID(backend)
	if err != nil {
		return err
	}

	nowNanos := time.Now().UnixNano()
	server, err := createActionWorkflow(backend, action, nowNanos)
	if err != nil {
		return err
	}

	err = acceptActionGoal(backend, server, goalID, action, nowNanos)
	if err != nil {
		return err
	}

	resp, callErr := actionSendWithBackend(callCtx, backend, server, goalID, action, goal, timeout, cancelAfter, stdout)

	return finishActionSend(backend, callErr, resp, stdout)
}
