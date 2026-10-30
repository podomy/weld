// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// Service call defaults for the composition root CLI verb.
const (
	// serviceParticipant names the service client side in discovery.
	serviceParticipant = "weld-service-client"
	// defaultServiceDeadline bounds one service call besides SIGINT cancel.
	defaultServiceDeadline = 5 * time.Second
	// serviceIDLBase is the v1 generic string service base.
	serviceIDLBase = "weld__srv__Call"
	// serviceRequestIDL is the v1 generic request IDL with one string field.
	serviceRequestIDL = "weld__srv__Call_Request"
	// serviceResponseIDL is the v1 generic response IDL with one string field.
	serviceResponseIDL = "weld__srv__Call_Response"
)

// openServiceBackendFunc opens the live backend for service calls.
//
// It is a variable so tests inject a fake without sockets.
var openServiceBackendFunc = openBackend

// serviceSubscriber announces a reply reader on backends that support it.
type serviceSubscriber interface {
	// Subscribe declares a local reader for topic with the given type.
	Subscribe(ctx context.Context, topic, typeName, typeHash string) error
}

// runServiceCommand dispatches service call and maps errors to exit codes.
func runServiceCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		_, _ = fmt.Fprintln(stderr, "weld: service needs one of call") //nolint:errcheck // final error output is best effort

		return 2
	}

	switch args[0] {
	case "call":
		return runServiceCallCommand(ctx, args[1:], stdout, stderr)
	case "-h", "-help", "--help", "help":
		printUsage(stdout)

		return 0
	default:
		printUsage(stderr)
		_, _ = fmt.Fprintf(stderr, "weld: unknown service subcommand %q\n", args[0]) //nolint:errcheck // final error output is best effort

		return 2
	}
}

// runServiceCallCommand parses service call flags and runs one call then exits.
func runServiceCallCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("service call", flag.ContinueOnError)
	fs.SetOutput(stderr)

	if isHelpRequest(args) {
		printUsage(stdout)

		return 0
	}

	serviceFlag := fs.String("service", "", "ROS service name such as /add_two_ints")
	domainFlag := fs.Uint("domain", uint(defaultDomainID()), "DDS domain id, default from ROS_DOMAIN_ID")
	requestFlag := fs.String("request", "", "service request data")
	deadlineFlag := fs.Duration("deadline", defaultServiceDeadline, "per-call timeout")

	err := fs.Parse(args)
	if err != nil {
		return 2
	}

	service, request, err := resolveServiceCallArgs(*serviceFlag, *requestFlag, fs.Args())
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	err = validateServiceDeadline(*deadlineFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	domain, err := resolveEffectiveDomain(*domainFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	err = validateTopic(service)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

		return 2
	}

	err = runServiceCall(ctx, service, request, domain, *deadlineFlag, stdout)
	if err != nil {
		return handleRunError(err, stderr)
	}

	return 0
}

// resolveServiceCallArgs picks service and request from flags or positionals.
func resolveServiceCallArgs(flagService, flagRequest string, args []string) (string, string, error) {
	service := flagService

	rest := args

	if service == "" {
		if len(rest) == 0 {
			return "", "", errors.New("weld: service call needs --service or one positional service") //nolint:wrapcheck // new error, nothing to wrap
		}

		service = rest[0]
		rest = rest[1:]
	}

	if flagRequest != "" {
		if len(rest) != 0 {
			return "", "", errors.New("weld: service call takes one request, have flag plus positionals") //nolint:wrapcheck // new error, nothing to wrap
		}

		return service, flagRequest, nil
	}

	if len(rest) == 0 {
		return service, "", nil
	}

	if len(rest) == 1 {
		return service, rest[0], nil
	}

	return "", "", errors.New("weld: service call takes one request, have too many args") //nolint:wrapcheck // new error, nothing to wrap
}

// validateServiceDeadline rejects non positive per-call timeouts.
func validateServiceDeadline(deadline time.Duration) error {
	if deadline <= 0 {
		return errors.New("weld: service deadline must be positive") //nolint:wrapcheck // new error, nothing to wrap
	}

	return nil
}

// checkServiceEndpoints validates service QoS fanout plus rq and rr mapping.
func checkServiceEndpoints(service string) error {
	_, _, err := core.ResolveServiceQoS(service, core.BestAvailableQoS())
	if err != nil {
		return fmt.Errorf("weld: create service endpoint: %w", err)
	}

	_, err = dds.NewServiceEndpoints(service, serviceIDLBase, false)
	if err != nil {
		return fmt.Errorf("weld: map service endpoints: %w", err)
	}

	return nil
}

// newServiceGUID returns one random 8 byte service client GUID.
func newServiceGUID() ([8]byte, error) {
	var guid [8]byte

	_, err := rand.Read(guid[:])
	if err != nil {
		return guid, fmt.Errorf("weld: new service GUID: %w", err)
	}

	return guid, nil
}

// encodeServiceRequest encodes request text with header plus GUID plus sequence.
func encodeServiceRequest(request string, guid [8]byte, seq uint64) ([]byte, error) {
	enc := types.NewEncoder(true)

	err := enc.WriteServiceHeader(guid, seq)
	if err != nil {
		return nil, fmt.Errorf("weld: encode service header: %w", err)
	}

	err = enc.WriteString(request)
	if err != nil {
		return nil, fmt.Errorf("weld: encode service request: %w", err)
	}

	return enc.Bytes(), nil
}

// decodeServiceReply decodes one reply and reports whether GUID plus sequence match.
func decodeServiceReply(wire []byte, wantGUID [8]byte, wantSeq uint64) (string, bool, error) {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return "", false, fmt.Errorf("weld: decode service header: %w", err)
	}

	gotGUID, gotSeq, err := dec.ReadServiceHeader()
	if err != nil {
		return "", false, fmt.Errorf("weld: decode service header: %w", err)
	}

	if gotGUID != wantGUID || gotSeq != wantSeq {
		return "", false, nil
	}

	text, err := dec.ReadString()
	if err != nil {
		return "", true, fmt.Errorf("weld: decode service response: %w", err)
	}

	err = dec.Finish()
	if err != nil {
		return "", true, fmt.Errorf("weld: decode service trailing: %w", err)
	}

	return text, true, nil
}

// subscribeServiceReply declares the client reply reader when supported.
func subscribeServiceReply(ctx context.Context, backend transports.Backend, service string) error {
	sub, ok := backend.(serviceSubscriber)
	if !ok {
		return nil
	}

	err := sub.Subscribe(ctx, service, serviceResponseIDL, "")
	if err != nil {
		return fmt.Errorf("weld: subscribe %q: %w", service, err)
	}

	return nil
}

// publishServiceRequest retries PublishBytes on ErrNoRoute until deadline or ctx ends.
func publishServiceRequest(ctx context.Context, backend transports.Backend, out transports.OutgoingMessage, deadline time.Duration) error {
	start := time.Now()

	for {
		err := backend.PublishBytes(ctx, out)
		if err == nil {
			return nil
		}

		if !errors.Is(err, dds.ErrNoRoute) {
			return err
		}

		if time.Since(start) >= deadline {
			return err
		}

		cerr := ctx.Err()
		if cerr != nil {
			if errors.Is(cerr, context.Canceled) {
				return cerr
			}

			if time.Since(start) >= deadline {
				return err
			}

			return cerr
		}

		timer := time.NewTimer(pubRouteRetryInterval)

		select {
		case <-ctx.Done():
			timer.Stop()

			cerr = ctx.Err()
			if cerr != nil {
				if errors.Is(cerr, context.Canceled) {
					return cerr
				}

				if errors.Is(err, dds.ErrNoRoute) {
					return err
				}

				return cerr
			}

			return err
		case <-timer.C:
		}
	}
}

// serviceCallWithBackend publishes one request and waits for the matching reply.
func serviceCallWithBackend(ctx context.Context, backend transports.Backend, service, request string, guid [8]byte, seq uint64, deadline time.Duration) (string, error) {
	err := validateServiceDeadline(deadline)
	if err != nil {
		return "", err
	}

	callCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	client := core.NewServiceClient(nil, false, nil)

	timeout := core.DurationFromNanoseconds(int64(deadline))

	err = client.BeginCall(callCtx, service, guid, seq, timeout)
	if err != nil {
		return "", fmt.Errorf("weld: service %q: begin call: %w", service, err)
	}

	wire, err := encodeServiceRequest(request, guid, seq)
	if err != nil {
		return "", err
	}

	out := transports.OutgoingMessage{
		TopicName: service,
		TypeName:  serviceRequestIDL,
		Payload:   wire,
	}

	err = publishServiceRequest(callCtx, backend, out, deadline)
	if err != nil {
		if errors.Is(err, dds.ErrNoRoute) {
			_ = client.FailServerGone(service, guid, seq) //nolint:errcheck // gone count is best effort, loud error below carries the failure

			return "", fmt.Errorf("weld: service %q: no server: %w", service, err)
		}

		return "", fmt.Errorf("weld: service %q: publish request: %w", service, err)
	}

	for {
		msg, err := backend.TakeBytes(callCtx)
		if err != nil {
			if errors.Is(callCtx.Err(), context.Canceled) {
				return "", fmt.Errorf("weld: service %q: canceled: %w", service, context.Canceled)
			}

			if callCtx.Err() != nil {
				return "", fmt.Errorf("weld: service %q: deadline %v exceeded: %w", service, deadline, context.DeadlineExceeded)
			}

			if isSkippableSample(err) {
				continue
			}

			return "", fmt.Errorf("weld: service %q: take reply: %w", service, err)
		}

		if msg.TopicName != "" && msg.TopicName != service {
			continue
		}

		resp, matched, err := decodeServiceReply(msg.Payload, guid, seq)
		if err != nil {
			return "", fmt.Errorf("weld: service %q: %w", service, err)
		}

		if !matched {
			continue
		}

		err = client.CompleteCall(service, guid, seq)
		if err != nil {
			return "", fmt.Errorf("weld: service %q: complete call: %w", service, err)
		}

		return resp, nil
	}
}

// openSubscribedServiceBackend opens the backend and declares the reply reader.
func openSubscribedServiceBackend(ctx context.Context, domain uint32, service string) (transports.Backend, error) {
	backend, err := openServiceBackendFunc(ctx, domain, serviceParticipant)
	if err != nil {
		return nil, fmt.Errorf("weld: open backend: %w", err)
	}

	err = subscribeServiceReply(ctx, backend, service)
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return nil, fmt.Errorf("weld: service call: %w", errors.Join(err, cerr))
		}

		return nil, err
	}

	return backend, nil
}

// runServiceCall validates, opens the backend, calls once, and prints the reply.
func runServiceCall(ctx context.Context, service, request string, domain uint32, deadline time.Duration, stdout io.Writer) error {
	err := validateTopic(service)
	if err != nil {
		return err
	}

	err = validateServiceDeadline(deadline)
	if err != nil {
		return err
	}

	err = checkServiceEndpoints(service)
	if err != nil {
		return err
	}

	callCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	backend, err := openSubscribedServiceBackend(callCtx, domain, service)
	if err != nil {
		return err
	}

	guid, err := newServiceGUID()
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return fmt.Errorf("weld: service call: %w", errors.Join(err, cerr))
		}

		return err
	}

	seq := uint64(1)

	resp, callErr := serviceCallWithBackend(callCtx, backend, service, request, guid, seq, deadline)

	cerr := backend.Close()
	if callErr != nil && cerr != nil {
		return fmt.Errorf("weld: service call: %w", errors.Join(callErr, cerr))
	}

	if cerr != nil {
		return fmt.Errorf("weld: close service backend: %w", cerr)
	}

	if callErr != nil {
		return callErr
	}

	_, err = fmt.Fprintln(stdout, resp)
	if err != nil {
		return fmt.Errorf("weld: print service response: %w", err)
	}

	return nil
}
