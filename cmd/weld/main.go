// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Command weld provides the first-contact conformance CLI.
//
// It publishes and echoes std_msgs/String generic bytes on a topic.
// The CLI is the composition root: the only place allowed to join
// internal/core with internal/transports plus internal/types.
// Core owns matching and QoS rules, transports own the wire, and this
// package only wires them together for the echo and pub verbs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/podomy/weld/internal/core"
	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// First-contact defaults for topic, type, and participants.
const (
	// defaultTopic is the ROS topic used when no flag is given.
	defaultTopic = "/chatter"
	// stringTypeName is the IDL type name for std_msgs/String generic bytes.
	stringTypeName = "std_msgs__msg__String"
	// stringTypeHash is the interop type hash for std_msgs/String.
	stringTypeHash = "RIHS01_df668c740482bbd48fb39d76a70dfd4bd59db1288021743503259e948f6b1a18"
	// echoParticipant names the echo side in discovery.
	echoParticipant = "weld-echo"
	// pubParticipant names the pub side in discovery.
	pubParticipant = "weld-pub"
	// defaultEchoTimeout bounds one echo run besides SIGINT cancel.
	defaultEchoTimeout = 30 * time.Second
	// lostLogInterval throttles gap and drop counter lines to 1Hz.
	lostLogInterval = time.Second
	// pubRouteRetryInterval spaces pub retries while waiting for discovery.
	pubRouteRetryInterval = 50 * time.Millisecond
	// pubRouteRetryTimeout bounds pub waits for SPDP routes to appear.
	pubRouteRetryTimeout = 5 * time.Second
	// echoDiscoveryInterval spaces echo polls while waiting for discovery.
	echoDiscoveryInterval = 50 * time.Millisecond
	// echoDiscoveryTimeout bounds echo waits for SPDP routes to appear.
	echoDiscoveryTimeout = 5 * time.Second
)

// echoDiscoveryIntervalVar spaces echo discovery polls and is stubbed in tests.
var echoDiscoveryIntervalVar = echoDiscoveryInterval

// echoDiscoveryTimeoutVar bounds echo discovery waits and is stubbed in tests.
var echoDiscoveryTimeoutVar = echoDiscoveryTimeout

// main runs the CLI with signal cancel and exits with the run code.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()

	os.Exit(code)
}

// run dispatches echo, pub, topic, service, doctor, params, and launch and maps errors to exit codes.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "echo":
		return runEchoCommand(ctx, args[1:], stdout, stderr)
	case "pub":
		return runPubCommand(ctx, args[1:], stdout, stderr)
	case "topic":
		return runTopicCommand(ctx, args[1:], stdout, stderr)
	case "service":
		return runServiceCommand(ctx, args[1:], stdout, stderr)
	case "action":
		return runActionCommand(ctx, args[1:], stdout, stderr)
	case "doctor":
		return runDoctorCommand(ctx, args[1:], stdout, stderr)
	case "params":
		store := dds.NewMemoryStore(defaultParamsNode)
		return runParamsCommand(ctx, args[1:], stdout, stderr, store)
	case "launch":
		return runLaunchCommand(ctx, args[1:], stdout, stderr)
	case "-h", "-help", "--help", "help":
		printUsage(stdout)
		return 0
	default:
		printUsage(stderr)
		_, _ = fmt.Fprintf(stderr, "weld: unknown subcommand %q\n", args[0]) //nolint:errcheck // final error output is best effort
		return 2
	}
}

// topicDomainFlags registers topic and domain flags and returns pointers.
func topicDomainFlags(fs *flag.FlagSet) (*string, *uint) {
	topic := fs.String("topic", defaultTopic, "ROS topic name such as /chatter")
	domain := fs.Uint("domain", uint(defaultDomainID()), "DDS domain id, default from ROS_DOMAIN_ID")
	return topic, domain
}

// isHelpRequest reports whether args ask for help before flag parsing.
//
// Flag sets print help to their configured output, which is stderr for
// usage errors. Pre-scanning lets -h help go to stdout with exit 0 while
// parse failures still go to stderr with exit 2.
func isHelpRequest(args []string) bool {
	for _, a := range args {
		if a == "-h" || a == "-help" || a == "--help" {
			return true
		}
	}

	return false
}

// runEchoCommand parses echo flags and prints messages until ctx ends.
func runEchoCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("echo", flag.ContinueOnError)
	fs.SetOutput(stderr)

	if isHelpRequest(args) {
		printUsage(stdout)
		return 0
	}

	topicFlag, domainFlag := topicDomainFlags(fs)
	timeout := fs.Duration("timeout", defaultEchoTimeout, "stop echo after this delay, 0 runs until interrupt")
	durabilityFlag := fs.String("qos-durability", "volatile", "requested durability: volatile or transient_local")

	err := fs.Parse(args)
	if err != nil {
		return 2
	}

	if fs.NArg() != 0 {
		_, _ = fmt.Fprintln(stderr, "weld: echo takes no positional args") //nolint:errcheck // final error output is best effort
		return 2
	}

	if *timeout < 0 {
		_, _ = fmt.Fprintf(stderr, "weld: echo timeout %v must not be negative\n", *timeout) //nolint:errcheck // final error output is best effort
		return 2
	}

	volatile, err := resolveEchoDurability(*durabilityFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 2
	}

	domain, err := resolveEffectiveDomain(*domainFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 2
	}

	err = validateTopic(*topicFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 2
	}

	ctx, cancel := withEchoTimeout(ctx, *timeout)
	defer cancel()

	err = runEcho(ctx, *topicFlag, domain, volatile, stdout, stderr)
	if err != nil {
		return handleRunError(err, stderr)
	}

	return 0
}

// runPubCommand parses pub flags and publishes one message then exits.
func runPubCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("pub", flag.ContinueOnError)
	fs.SetOutput(stderr)

	if isHelpRequest(args) {
		printUsage(stdout)
		return 0
	}

	topicFlag, domainFlag := topicDomainFlags(fs)
	text := fs.String("text", "", "std_msgs/String data to publish")

	err := fs.Parse(args)
	if err != nil {
		return 2
	}

	domain, err := resolveEffectiveDomain(*domainFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 2
	}

	err = validateTopic(*topicFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 2
	}

	msg, err := resolvePubText(*text, fs.Args())
	if err != nil {
		_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort
		return 2
	}

	err = runPub(ctx, *topicFlag, msg, domain, stdout)
	if err != nil {
		return handleRunError(err, stderr)
	}

	return 0
}

// resolvePubText picks the pub message from flag or positional arg.
func resolvePubText(flagText string, args []string) (string, error) {
	if flagText != "" {
		if len(args) != 0 {
			return "", errors.New("weld: pub takes one message, have flag plus positionals") //nolint:wrapcheck // new error, nothing to wrap
		}

		return flagText, nil
	}

	if len(args) == 1 {
		return args[0], nil
	}

	if len(args) == 0 {
		return "", errors.New("weld: pub needs --text or one positional message") //nolint:wrapcheck // new error, nothing to wrap
	}

	return "", fmt.Errorf("weld: pub takes one message, have %d args", len(args))
}

// resolveEffectiveDomain resolves the flag domain against ROS_DOMAIN_ID.
func resolveEffectiveDomain(flagValue uint) (uint32, error) {
	if flagValue > uint(transports.MaxDomainID) {
		return 0, fmt.Errorf("weld: domain %d above maximum %d", flagValue, transports.MaxDomainID)
	}

	resolved, err := transports.ResolveDomainID(uint32(flagValue))
	if err != nil {
		return 0, fmt.Errorf("weld: resolve domain: %w", err)
	}

	return resolved, nil
}

// withEchoTimeout bounds echo by timeout, or until ctx ends when timeout is 0.
func withEchoTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, timeout)
}

// echoSubscriber announces an SEDP reader on backends that support it.
type echoSubscriber interface {
	// Subscribe declares a local reader for topic with the given type.
	Subscribe(ctx context.Context, topic, typeName, typeHash string) error
	// SubscribeWithDurability declares a local reader with explicit durability.
	SubscribeWithDurability(ctx context.Context, topic, typeName, typeHash string, volatile bool) error
}

// resolveEchoDurability maps the echo --qos-durability flag to volatility.
//
// Volatile is the default and matches ros2 topic echo: it flows against
// any offer. Transient local refuses volatile writers through peer side
// RxO matching, which is how QoS gates prove silence. Anything else is a
// usage error before any socket opens.
func resolveEchoDurability(flagValue string) (bool, error) {
	switch flagValue {
	case "volatile":
		return true, nil
	case "transient_local":
		return false, nil
	default:
		return true, fmt.Errorf("weld: echo --qos-durability must be volatile or transient_local, got %q", flagValue)
	}
}

// subscribeEchoReader declares the echo reader when the backend supports it.
func subscribeEchoReader(ctx context.Context, backend transports.Backend, topic string, volatile bool) error {
	sub, ok := backend.(echoSubscriber)
	if !ok {
		return nil
	}

	err := sub.SubscribeWithDurability(ctx, topic, stringTypeName, stringTypeHash, volatile)
	if err != nil {
		return fmt.Errorf("weld: subscribe %q: %w", topic, err)
	}

	return nil
}

// waitForEchoDiscovery waits for SPDP routes and first SEDP generation after Subscribe before taking.
//
// It polls DiscoveryGeneration when the backend exposes it and returns early on change, mirroring
// publishWithRouteWait polling. Backends without a generation signal still wait the bounded timeout
// so late joiner SEDP still settles. The wait is best effort and never fails: timeout or context
// end returns normally so the take loop still runs with authoritative drop counters and exit codes unchanged.
func waitForEchoDiscovery(ctx context.Context, backend transports.Backend) {
	timeout := echoDiscoveryTimeoutVar
	if timeout <= 0 {
		return
	}

	interval := echoDiscoveryIntervalVar
	if interval <= 0 {
		interval = echoDiscoveryInterval
	}

	var viewer discoveryGenerationViewer
	var start uint64
	hasViewer := false

	if v, ok := backend.(discoveryGenerationViewer); ok {
		viewer = v
		start = v.DiscoveryGeneration()
		hasViewer = true
	}

	begin := time.Now()

	for {
		if hasViewer && viewer.DiscoveryGeneration() != start {
			return
		}

		if time.Since(begin) >= timeout {
			return
		}

		if ctx.Err() != nil {
			return
		}

		timer := time.NewTimer(interval)

		select {
		case <-ctx.Done():
			timer.Stop()

			return
		case <-timer.C:
		}
	}
}

// runEcho subscribes to topic and prints std_msgs/String payloads until ctx ends.
func runEcho(ctx context.Context, topic string, domain uint32, volatile bool, stdout, stderr io.Writer) error {
	err := validateTopic(topic)
	if err != nil {
		return err
	}

	err = checkEndpoint(topic, echoParticipant, core.EndpointSubscription)
	if err != nil {
		return err
	}

	backend, err := openBackend(ctx, domain, echoParticipant)
	if err != nil {
		return fmt.Errorf("weld: open backend for topic %q: %w", topic, err)
	}

	err = subscribeEchoReader(ctx, backend, topic, volatile)
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return fmt.Errorf("weld: echo: %w", errors.Join(err, cerr))
		}

		return err
	}

	waitForEchoDiscovery(ctx, backend)

	loopErr := echoLoop(ctx, backend, topic, stdout, stderr)

	cerr := backend.Close()
	if loopErr != nil && cerr != nil {
		return fmt.Errorf("weld: echo: %w", errors.Join(loopErr, cerr))
	}

	if cerr != nil {
		return fmt.Errorf("weld: close echo backend: %w", cerr)
	}

	if loopErr != nil {
		return loopErr
	}

	return nil
}

// runPub publishes one std_msgs/String message then exits.
func runPub(ctx context.Context, topic, text string, domain uint32, stdout io.Writer) error {
	err := validateTopic(topic)
	if err != nil {
		return err
	}

	err = checkEndpoint(topic, pubParticipant, core.EndpointPublisher)
	if err != nil {
		return err
	}

	backend, err := openBackend(ctx, domain, pubParticipant)
	if err != nil {
		return fmt.Errorf("weld: open backend for topic %q: %w", topic, err)
	}

	payload, err := encodeStringPayload(text)
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return fmt.Errorf("weld: %w", errors.Join(err, cerr))
		}

		return fmt.Errorf("weld: %w", err)
	}

	out := transports.OutgoingMessage{
		TopicName: topic,
		TypeName:  stringTypeName,
		Payload:   payload,
	}

	err = publishWithRouteWait(ctx, backend, out)
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return fmt.Errorf("weld: publish bytes: %w", errors.Join(err, cerr))
		}

		return fmt.Errorf("weld: publish bytes: %w", err)
	}

	cerr := backend.Close()
	if cerr != nil {
		return fmt.Errorf("weld: close pub backend: %w", cerr)
	}

	_, err = fmt.Fprintln(stdout, "sent: "+text)
	if err != nil {
		return fmt.Errorf("weld: print confirmation: %w", err)
	}

	return nil
}

// publishWithRouteWait retries PublishBytes on ErrNoRoute until a route appears or the wait ends.
//
// It shares publishWithRouteRetry with service and action verbs so retry,
// timeout, and cancel semantics stay identical. The wait is bounded by
// pubRouteRetryTimeout or ctx cancel, whichever ends first.
func publishWithRouteWait(ctx context.Context, backend transports.Backend, msg transports.OutgoingMessage) error {
	return publishWithRouteRetry(ctx, backend, msg, pubRouteRetryTimeout)
}

// lossCounter is implemented by backends that count gap loss.
type lossCounter interface {
	// LostCount returns the count of samples declared lost across gaps.
	LostCount() uint64
}

// lossTracker logs gap and drop counters at most once per second.
type lossTracker struct {
	backend  transports.Backend
	stderr   io.Writer
	baseline bool
	lastLost uint64
	drops    uint64
	lastLog  time.Time
	now      func() time.Time
}

// newLossTracker returns a loss tracker for backend logging to stderr.
func newLossTracker(backend transports.Backend, stderr io.Writer) *lossTracker {
	return &lossTracker{backend: backend, stderr: stderr, now: time.Now}
}

// poll checks the backend lost counter and logs growth throttled to 1Hz.
func (t *lossTracker) poll() {
	rc, ok := t.backend.(lossCounter)
	if !ok {
		return
	}

	lost := rc.LostCount()
	if !t.baseline {
		t.baseline = true
		t.lastLost = lost

		return
	}

	if lost <= t.lastLost {
		return
	}

	t.lastLost = lost
	t.emit(fmt.Sprintf("weld: lost %d messages", lost))
}

// countDrop records one skipped corrupt or duplicate sample and logs throttled.
func (t *lossTracker) countDrop() {
	t.drops++

	t.emit(fmt.Sprintf("weld: dropped %d messages", t.drops))
}

// emit prints one counter line at most once per second to avoid log spam.
func (t *lossTracker) emit(line string) {
	nowFn := t.now
	if nowFn == nil {
		nowFn = time.Now
	}

	now := nowFn()
	if !t.lastLog.IsZero() && now.Sub(t.lastLog) < lostLogInterval {
		return
	}

	t.lastLog = now
	_, _ = fmt.Fprintln(t.stderr, line) //nolint:errcheck // counter output is best effort
}

// summary prints final drop and loss totals on clean exit when nonzero.
func (t *lossTracker) summary() {
	var lost uint64
	rc, ok := t.backend.(lossCounter)
	if ok {
		lost = rc.LostCount()
	}

	if t.drops == 0 && lost == 0 {
		return
	}

	_, _ = fmt.Fprintf(t.stderr, "weld: echo summary: dropped %d messages, lost %d messages\n", t.drops, lost) //nolint:errcheck // summary output is best effort
}

// isSkippableSample reports whether a TakeBytes error is a per-sample drop.
func isSkippableSample(err error) bool {
	return errors.Is(err, dds.ErrStaleSequence) ||
		errors.Is(err, dds.ErrTruncated) ||
		errors.Is(err, dds.ErrInvalidFrame)
}

// echoLoop takes messages on topic until ctx ends and prints decoded text.
//
// Messages on other topics are skipped and counted as drops so parallel
// shards sharing one domain do not cross-talk. Messages with a mismatched
// type name are also skipped as drops, including decode failures, so a
// foreign type on the subscribed topic never fails the run.
func echoLoop(ctx context.Context, backend transports.Backend, topic string, stdout, stderr io.Writer) error {
	tracker := newLossTracker(backend, stderr)

	for {
		msg, err := backend.TakeBytes(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				tracker.summary()

				return nil
			}

			if isSkippableSample(err) {
				tracker.countDrop()

				continue
			}

			return fmt.Errorf("weld: take bytes: %w", err)
		}

		tracker.poll()

		if msg.TopicName != topic {
			tracker.countDrop()

			continue
		}

		if msg.TypeName != "" && msg.TypeName != stringTypeName {
			tracker.countDrop()

			continue
		}

		text, err := decodeStringPayload(msg.Payload)
		if err != nil {
			if msg.TypeName != "" && msg.TypeName != stringTypeName {
				tracker.countDrop()

				continue
			}

			return fmt.Errorf("weld: decode message on %q: %w", msg.TopicName, err)
		}

		_, err = fmt.Fprintln(stdout, text)
		if err != nil {
			return fmt.Errorf("weld: print message: %w", err)
		}
	}
}

// openBackend dials the live DDS backend and joins discovery.
func openBackend(ctx context.Context, domain uint32, participant string) (transports.Backend, error) {
	cfg := dds.SidecarConfig{
		DomainID:    domain,
		PeerAddress: dds.DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	}

	backend, err := dds.NewBackend(cfg)
	if err != nil {
		return nil, fmt.Errorf("new backend on domain %d: %w", domain, err)
	}

	stored := backend.Config()
	info := transports.DiscoveryInfo{
		ParticipantName: participant,
		DomainID:        stored.DomainID,
		Range:           stored.Range,
	}

	err = backend.JoinDiscovery(ctx, info)
	if err != nil {
		cerr := backend.Close()
		if cerr != nil {
			return nil, fmt.Errorf("join discovery on domain %d as %q: %w", domain, participant, errors.Join(err, cerr))
		}

		return nil, fmt.Errorf("join discovery on domain %d as %q: %w", domain, participant, err)
	}

	return backend, nil
}

// checkEndpoint validates topic creation through core.
func checkEndpoint(topic, name string, kind core.EndpointKind) error {
	_, err := core.CreateEndpoint(topic, name, kind, core.BestAvailableQoS(), nil, false)
	if err != nil {
		return fmt.Errorf("weld: create endpoint: %w", err)
	}

	return nil
}

// encodeStringPayload encodes text as XCDR1 std_msgs/String bytes.
func encodeStringPayload(text string) ([]byte, error) {
	enc := types.NewEncoder(true)

	err := enc.WriteString(text)
	if err != nil {
		return nil, fmt.Errorf("encode string: %w", err)
	}

	return enc.Bytes(), nil
}

// decodeStringPayload decodes XCDR1 std_msgs/String bytes to text.
func decodeStringPayload(payload []byte) (string, error) {
	dec, err := types.NewDecoder(payload)
	if err != nil {
		return "", fmt.Errorf("decode header: %w", err)
	}

	text, err := dec.ReadString()
	if err != nil {
		return "", fmt.Errorf("decode string: %w", err)
	}

	err = dec.Finish()
	if err != nil {
		return "", fmt.Errorf("trailing bytes: %w", err)
	}

	return text, nil
}

// validateTopic rejects empty topics and topics not starting with slash.
func validateTopic(topic string) error {
	if topic == "" {
		return errors.New("weld: topic name is empty") //nolint:wrapcheck // new error, nothing to wrap
	}

	if topic[0] != '/' {
		return fmt.Errorf("weld: topic %q must start with slash", topic)
	}

	return nil
}

// defaultDomainID returns the flag default for --domain.
//
// It resolves ROS_DOMAIN_ID through transports.ResolveDomainID with caller
// 0. An invalid ROS_DOMAIN_ID does not fail here: this returns 0 and the
// failure is deferred to resolveEffectiveDomain during command setup,
// where it is reported with the weld: prefix and exit code 2.
func defaultDomainID() uint32 {
	id, err := transports.ResolveDomainID(0)
	if err != nil {
		return 0
	}

	return id
}

// oneLine collapses an error to its first line for friendly output.
func oneLine(err error) string {
	line, _, _ := strings.Cut(err.Error(), "\n")

	return strings.TrimRight(line, " \r\t")
}

// printUsage writes CLI help.
func printUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, "Usage: weld <echo|pub|topic|service|action|doctor|params|launch> [flags]\n\necho: print std_msgs/String payloads until interrupt\n  weld echo --topic /chatter --domain 42 --timeout 30s\n\npub: publish one std_msgs/String message then exit\n  weld pub --topic /chatter --domain 42 --text hello\n\ntopic list: list topics with counts plus types plus hashes\n  weld topic list --domain 42\n\ntopic info: show endpoints plus type hashes plus mismatches\n  weld topic info --topic /chatter --domain 42\n\nservice call: call one service with per-call deadline then exit\n  weld service call --service /add_two_ints --request hello --deadline 5s --domain 42\n\naction send: send one action goal with feedback then result\n  weld action send --name /navigate_to_pose --goal hello --timeout 60s\n\ndoctor: list entities with topic info plus mismatches plus metrics\n  weld doctor --domain 42\n\nparams: read and write node parameters\n  weld params get my_param --node /weld\n\nlaunch: resolve node config and print canonical dump\n  weld launch --node talker --namespace /ns\n\nFlags:\n  --topic string   ROS topic name such as /chatter (default /chatter)\n  --domain uint    DDS domain id, default from ROS_DOMAIN_ID\n  --text string    pub message data (or one positional arg)\n  --timeout duration echo stop delay, 0 runs until interrupt (default 30s)\n  --qos-durability string echo requested durability: volatile or transient_local (default volatile)\n  --include-hidden show hidden underscore topics in topic list\n  --service string ROS service name such as /add_two_ints\n  --request string service request data (or one positional arg)\n  --deadline duration service per-call timeout (default 5s)\n") //nolint:errcheck // help output failure is not actionable
}

// handleRunError maps run errors to exit codes with friendly text.
func handleRunError(err error, stderr io.Writer) int {
	if err == nil {
		return 0
	}

	if errors.Is(err, dds.ErrSidecarNotImplemented) {
		_, _ = fmt.Fprintln(stderr, "weld: DDS sidecar stub returned not-implemented (unexpected with the live backend): "+oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}

	if errors.Is(err, dds.ErrDataPathNotWired) {
		_, _ = fmt.Fprintln(stderr, "weld: DDS data path is not wired yet: "+oneLine(err)) //nolint:errcheck // final error output is best effort
		return 1
	}

	_, _ = fmt.Fprintln(stderr, oneLine(err)) //nolint:errcheck // final error output is best effort

	return 1
}
