// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/podomy/weld/internal/transports"
	"github.com/podomy/weld/internal/transports/dds"
	"github.com/podomy/weld/internal/types"
)

// Exit codes mirror the probe contract: 0 completes even with gaps
// (gaps are data), 2 marks bad args or invalid-data runs, 1 marks
// crashes, and 130 marks keyboard interrupt.
const (
	exitOK        = 0
	exitCrash     = 1
	exitBadArgs   = 2
	exitInterrupt = 130
)

// Wire constants mirror the harness bench constants: the String payload
// holds exactly size chars, the first 32 are lowercase hex of the 16
// header bytes (little endian send_ns plus seq), and the rest is NUL-free
// printable fill.
const (
	agentTypeIDL     = "std_msgs__msg__String"
	agentHeaderBytes = 16
	agentHeaderChars = 32
	agentFillBase    = 33
	agentFillSpan    = 94
	agentMinSize     = agentHeaderChars + 1
	agentPinnedDepth = 100
)

// Timing constants mirror the probe and harness: the default timeout pads
// the paced send duration, match polls space discovery waits, publish
// retries bound route settle, and the post-match settle keeps the first
// DATA behind peer SEDP processing.
const (
	agentTimeoutPad  = 30 * time.Second
	agentMatchPoll   = 50 * time.Millisecond
	agentPubRetry    = 10 * time.Second
	agentPubRetryGap = 50 * time.Millisecond
	// agentPostMatchSettle is the one-shot pause between an observed
	// match and the first publish. waitForReader proves the local side
	// saw the peer's reader SEDP, not that the peer processed ours; a
	// fragmented first sample fired into that window loses its head
	// frags on the peer with no reassembly state to NACK from, a
	// permanent gap no repair can close. Steady-state publishes send
	// without pause; only the run's first sample waits.
	agentPostMatchSettle = 1 * time.Second
)

// qosDoc mirrors the probe qos object: reliability, durability, history,
// depth in that order.
type qosDoc struct {
	Reliability string `json:"reliability"`
	Durability  string `json:"durability"`
	History     string `json:"history"`
	Depth       int    `json:"depth"`
}

// resultDoc mirrors the probe stdout JSON document field for field and in
// order. Timestamp pointers stay nil until the first stamp so missing
// stamps encode as null exactly like the probe.
type resultDoc struct {
	Mode        string  `json:"mode"`
	Topic       string  `json:"topic"`
	QoS         qosDoc  `json:"qos"`
	Count       int     `json:"count"`
	Sent        int     `json:"sent"`
	Received    int     `json:"received"`
	Gaps        []int   `json:"gaps"`
	Dups        int     `json:"dups"`
	SendFirstNs *int64  `json:"send_first_ns"`
	SendLastNs  *int64  `json:"send_last_ns"`
	RecvFirstNs *int64  `json:"recv_first_ns"`
	RecvLastNs  *int64  `json:"recv_last_ns"`
	LatNs       []int64 `json:"lat_ns"`
	Note        string  `json:"note"`
}

// args holds the parsed CLI flags, mirroring probe parse_args.
type args struct {
	mode         string
	topic        string
	count        int
	seqBase      int
	rateHz       float64
	size         int
	reliability  string
	durability   string
	history      string
	depth        int
	timeout      time.Duration
	runID        string
	waitForMatch time.Duration
}

// markSent folds one publish stamp into the result.
func markSent(res *resultDoc, sendNs int64) {
	if res.SendFirstNs == nil {
		res.SendFirstNs = &sendNs
	}
	res.SendLastNs = &sendNs
	res.Sent++
}

// markRecv folds one reception stamp into the result.
func markRecv(res *resultDoc, recvNs int64) {
	if res.RecvFirstNs == nil {
		res.RecvFirstNs = &recvNs
	}
	res.RecvLastNs = &recvNs
	res.Received++
}

// newResult builds the completion JSON skeleton with empty gaps and lat.
func newResult(cfg args) *resultDoc {
	return &resultDoc{
		Mode:  cfg.mode,
		Topic: cfg.topic,
		QoS: qosDoc{
			Reliability: cfg.reliability,
			Durability:  cfg.durability,
			History:     cfg.history,
			Depth:       cfg.depth,
		},
		Count: cfg.count,
		Gaps:  []int{},
		LatNs: []int64{},
	}
}

// normalizeQoS folds dashes to underscores and lowercases, mirroring probe.
func normalizeQoS(s string) string {
	return strings.ReplaceAll(strings.ToLower(s), "-", "_")
}

// sanitize folds text into a valid ROS node name fragment, mirroring probe.
func sanitize(value string) string {
	lower := strings.ToLower(value)
	buf := make([]byte, 0, len(lower))
	for i := range lower {
		c := lower[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			buf = append(buf, c)
		} else {
			buf = append(buf, '_')
		}
	}
	text := strings.Trim(string(buf), "_")
	if text == "" || text[0] < 'a' || text[0] > 'z' {
		text = "n" + text
	}
	return text
}

// formatSecs renders seconds with one decimal, mirroring probe %.1f notes.
func formatSecs(d time.Duration) string {
	return strconv.FormatFloat(d.Seconds(), 'f', 1, 64)
}

// parseFlags parses the CLI flags without validating them. Timeout stays
// in seconds so an explicit -1 keeps its unset meaning for fillDefaults.
func parseFlags(argv []string) (args, float64, float64, error) {
	var cfg args
	var timeoutSecs float64
	var waitSecs float64
	fs := flag.NewFlagSet("bench-agent", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.mode, "mode", "", "pub|sub")
	fs.StringVar(&cfg.topic, "topic", "", "topic name")
	fs.IntVar(&cfg.count, "count", 100, "message count")
	fs.IntVar(&cfg.seqBase, "seq-base", 1, "first sequence number (fanin publishers tile disjoint spaces)")
	fs.Float64Var(&cfg.rateHz, "rate", 10.0, "publish rate in Hz, 0 uncapped")
	fs.IntVar(&cfg.size, "size", 256, "payload size in chars")
	fs.StringVar(&cfg.reliability, "qos-reliability", "reliable", "reliable|best_effort")
	fs.StringVar(&cfg.durability, "qos-durability", "volatile", "volatile|transient_local")
	fs.StringVar(&cfg.history, "qos-history", "keep_last", "keep_last|keep_all")
	fs.IntVar(&cfg.depth, "qos-depth", 10, "history depth")
	fs.Float64Var(&timeoutSecs, "timeout", -1, "overall timeout in seconds")
	fs.StringVar(&cfg.runID, "run-id", "", "run id for participant naming")
	fs.Float64Var(&waitSecs, "wait-for-match", 0.0, "match wait in seconds before first publish")
	err := fs.Parse(argv)
	if err != nil {
		return cfg, 0, 0, fmt.Errorf("bench-agent: parse args: %w", err)
	}
	if fs.NArg() > 0 {
		return cfg, 0, 0, fmt.Errorf("bench-agent: unexpected positional args: %v", fs.Args())
	}
	return cfg, timeoutSecs, waitSecs, nil
}

// checkModeTopic validates the mode choice and topic presence.
func checkModeTopic(cfg args) error {
	switch cfg.mode {
	case "pub", "sub":
	default:
		return errors.New("--mode must be pub or sub") //nolint:wrapcheck // new error, nothing to wrap
	}
	if cfg.topic == "" {
		return errors.New("--topic is required") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// checkQoSValues validates each QoS flag against the probe value sets.
func checkQoSValues(cfg args) error {
	switch cfg.reliability {
	case "reliable", "best_effort":
	default:
		return errors.New("qos-reliability must be reliable or best_effort") //nolint:wrapcheck // new error, nothing to wrap
	}
	switch cfg.durability {
	case "volatile", "transient_local":
	default:
		return errors.New("qos-durability must be volatile or transient_local") //nolint:wrapcheck // new error, nothing to wrap
	}
	switch cfg.history {
	case "keep_last", "keep_all":
	default:
		return errors.New("qos-history must be keep_last or keep_all") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// checkCounts validates the numeric shape flags.
func checkCounts(cfg args) error {
	if cfg.count < 1 {
		return errors.New("--count must be >= 1") //nolint:wrapcheck // new error, nothing to wrap
	}
	if cfg.seqBase < 1 {
		return errors.New("--seq-base must be >= 1") //nolint:wrapcheck // new error, nothing to wrap
	}
	if cfg.rateHz < 0 {
		return errors.New("--rate must be >= 0, 0 uncapped") //nolint:wrapcheck // new error, nothing to wrap
	}
	if cfg.size < agentMinSize {
		return errors.New("--size must be >= 33 (hex header plus fill)") //nolint:wrapcheck // new error, nothing to wrap
	}
	if cfg.depth < 1 {
		return errors.New("--qos-depth must be >= 1") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// checkTimeout validates the timeout and match wait.
func checkTimeout(timeoutSecs, waitSecs float64) error {
	if timeoutSecs != -1 && timeoutSecs <= 0 {
		return errors.New("--timeout must be > 0") //nolint:wrapcheck // new error, nothing to wrap
	}
	if waitSecs < 0 {
		return errors.New("--wait-for-match must be >= 0") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// checkPinnedQoS rejects every QoS but the pinned bench set, keeping the
// matrix honest.
func checkPinnedQoS(cfg args) error {
	if cfg.reliability != "reliable" || cfg.durability != "transient_local" ||
		cfg.history != "keep_last" || cfg.depth != agentPinnedDepth {
		return errors.New("qos must be reliable/transient_local/keep_last/100") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// fillDefaults applies the probe run-id and timeout defaults.
func fillDefaults(cfg *args, timeoutSecs, waitSecs float64) {
	if cfg.runID == "" {
		cfg.runID = "run-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	if timeoutSecs == -1 {
		cfg.timeout = time.Duration(float64(cfg.count)/cfg.rateHz*float64(time.Second)) + agentTimeoutPad
	} else {
		cfg.timeout = time.Duration(timeoutSecs * float64(time.Second))
	}
	cfg.waitForMatch = time.Duration(waitSecs * float64(time.Second))
}

// parseArgs parses and validates the CLI flags; failures exit 2 like probe
// parser.error, before any JSON is printed.
func parseArgs(argv []string) (args, error) {
	cfg, timeoutSecs, waitSecs, err := parseFlags(argv)
	if err != nil {
		return cfg, err
	}
	err = checkModeTopic(cfg)
	if err != nil {
		return cfg, err
	}
	cfg.reliability = normalizeQoS(cfg.reliability)
	cfg.durability = normalizeQoS(cfg.durability)
	cfg.history = normalizeQoS(cfg.history)
	err = checkQoSValues(cfg)
	if err != nil {
		return cfg, err
	}
	err = checkCounts(cfg)
	if err != nil {
		return cfg, err
	}
	err = checkTimeout(timeoutSecs, waitSecs)
	if err != nil {
		return cfg, err
	}
	err = checkPinnedQoS(cfg)
	if err != nil {
		return cfg, err
	}
	fillDefaults(&cfg, timeoutSecs, waitSecs)
	return cfg, nil
}

// encodePayload builds the XCDR String sample for one seq, mirroring
// harness encodeBenchPayload: 32 ASCII hex chars of the 16 header bytes
// plus deterministic fill.
func encodePayload(seq, size int, sendNs int64) ([]byte, error) {
	if size < agentMinSize {
		return nil, fmt.Errorf("bench-agent: payload size %d below minimum %d", size, agentMinSize)
	}
	if seq < 1 {
		return nil, fmt.Errorf("bench-agent: seq %d below 1", seq)
	}
	var hdr [agentHeaderBytes]byte
	binary.LittleEndian.PutUint64(hdr[0:8], uint64(sendNs)) //nolint:gosec // G115: stamp is a non-negative UnixNano
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(seq))
	text := make([]byte, 0, size)
	text = append(text, hex.EncodeToString(hdr[:])...)
	for i := agentHeaderChars; i < size; i++ {
		text = append(text, byte(agentFillBase+(seq+i-agentHeaderChars)%agentFillSpan))
	}
	enc := types.NewEncoder(true)
	err := enc.WriteString(string(text))
	if err != nil {
		return nil, fmt.Errorf("bench-agent: encode payload seq %d: %w", seq, err)
	}
	return enc.Bytes(), nil
}

// decodePayload decodes one XCDR String sample into send_ns and seq and
// verifies the fill, symmetric with encodePayload.
func decodePayload(wire []byte) (int64, int, error) {
	dec, err := types.NewDecoder(wire)
	if err != nil {
		return 0, 0, fmt.Errorf("bench-agent: decode header: %w", err)
	}
	text, err := dec.ReadString()
	if err != nil {
		return 0, 0, fmt.Errorf("bench-agent: decode string: %w", err)
	}
	err = dec.Finish()
	if err != nil {
		return 0, 0, fmt.Errorf("bench-agent: decode finish: %w", err)
	}
	if len(text) < agentMinSize {
		return 0, 0, fmt.Errorf("bench-agent: payload %d chars below minimum %d", len(text), agentMinSize)
	}
	hdr, err := hex.DecodeString(text[:agentHeaderChars])
	if err != nil {
		return 0, 0, fmt.Errorf("bench-agent: decode hex header: %w", err)
	}
	sendNs := int64(binary.LittleEndian.Uint64(hdr[0:8])) //nolint:gosec // G115: header round trip stays in int64 range
	seq := int(binary.LittleEndian.Uint64(hdr[8:16]))     //nolint:gosec // G115: bench seq space fits int
	if seq < 1 {
		return 0, 0, fmt.Errorf("bench-agent: seq %d below 1", seq)
	}
	for i := agentHeaderChars; i < len(text); i++ {
		want := byte(agentFillBase + (seq+i-agentHeaderChars)%agentFillSpan)
		if text[i] != want {
			return 0, 0, fmt.Errorf("bench-agent: fill mismatch at char %d seq %d", i, seq)
		}
	}
	return sendNs, seq, nil
}

// computeGaps returns the sorted missing seqs in [1, count].
func computeGaps(seen map[int]bool, count int) []int {
	gaps := []int{}
	for seq := 1; seq <= count; seq++ {
		if !seen[seq] {
			gaps = append(gaps, seq)
		}
	}
	return gaps
}

// publishWithRetry publishes once, retrying ErrNoRoute while the route
// settles. Only the missing route retries; other errors return at once.
// The wait is bounded so a readerless publish fails instead of hanging.
func publishWithRetry(ctx context.Context, backend *dds.Backend, msg transports.OutgoingMessage, label string) error {
	start := time.Now()
	for {
		err := backend.PublishBytes(ctx, msg)
		if err == nil {
			return nil
		}
		if !errors.Is(err, dds.ErrNoRoute) {
			return fmt.Errorf("bench-agent: %s publish: %w", label, err)
		}
		if time.Since(start) >= agentPubRetry {
			return fmt.Errorf("bench-agent: %s publish without route: %w", label, err)
		}
		cerr := ctx.Err()
		if cerr != nil {
			return fmt.Errorf("bench-agent: %s publish context: %w", label, cerr)
		}
		timer := time.NewTimer(agentPubRetryGap)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("bench-agent: %s publish context: %w", label, ctx.Err())
		case <-timer.C:
		}
	}
}

// publishPaced publishes count payloads on topic paced at rateHz. The
// first publish goes out immediately and a ticker paces the rest, so the
// schedule stays deadline aligned without a busy loop. A non-positive
// rateHz disables pacing for open-loop saturation runs. Seq numbers start
// at seqBase so fanin publishers hold disjoint spaces. onSent folds one
// dispatch stamp; its error aborts the series.
func publishPaced(ctx context.Context, backend *dds.Backend, topic string, count, seqBase int, rateHz float64, size int, onSent func(seq int, sendNs int64) error) error {
	var ticker *time.Ticker
	var ch <-chan time.Time
	if rateHz > 0 {
		period := time.Duration(float64(time.Second) / rateHz)
		ticker = time.NewTicker(period)
		defer ticker.Stop()
		ch = ticker.C
	}
	for k := range count {
		seq := seqBase + k
		if k > 0 && ch != nil {
			select {
			case <-ctx.Done():
				return fmt.Errorf("bench-agent: publish pacing ended: %w", ctx.Err())
			case <-ch:
			}
		}
		sendNs := time.Now().UnixNano()
		wire, err := encodePayload(seq, size, sendNs)
		if err != nil {
			return err
		}
		msg := transports.OutgoingMessage{
			TopicName: topic,
			TypeName:  agentTypeIDL,
			Payload:   wire,
			TypeHash:  dds.StringTypeHash,
		}
		err = publishWithRetry(ctx, backend, msg, "pub")
		if err != nil {
			return err
		}
		err = onSent(seq, sendNs)
		if err != nil {
			return err
		}
	}
	return nil
}

// waitForReader polls for a matching remote endpoint until wait elapses,
// mirroring the harness discovery wait. It reports false on timeout.
func waitForReader(backend *dds.Backend, topic string, wait time.Duration) bool {
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		if backend.HasRemoteEndpoint(topic) {
			return true
		}
		remaining := min(time.Until(deadline), agentMatchPoll)
		if remaining > 0 {
			time.Sleep(remaining)
		}
	}
	return backend.HasRemoteEndpoint(topic)
}

// finishRecv logs the outcome; a timeout with zero received exits 2 since
// that marks discovery failure, partial receipt still exits 0.
func finishRecv(cfg args, res *resultDoc, done bool) int {
	if done {
		log.Printf("done %d/%d", res.Received, cfg.count) //nolint:gosec // G706: counts are integers, no newline injection
		return exitOK
	}
	if res.Received == 0 {
		res.Note = "timeout with zero received"
	} else if res.Note == "" {
		res.Note = "timeout after " + formatSecs(cfg.timeout) + "s"
	}
	log.Printf("partial %d/%d", res.Received, cfg.count) //nolint:gosec // G706: counts are integers, no newline injection
	if res.Received == 0 {
		return exitBadArgs
	}
	return exitOK
}

// agentFragWarmMin is the smallest payload that travels fragmented: the
// 65535 byte DATA submessage cap minus its 20 byte fixed header. Only
// fragmented first samples get the redundancy below; unfragmented rows
// keep dups at zero.
const agentFragWarmMin = 65535 - 20

// agentWarmGap spaces the redundant first sample from the measured
// stream, so a transient shorter than the gap cannot take both copies.
const agentWarmGap = 200 * time.Millisecond

// sendWarmSample publishes one redundant copy of the first sequence
// outside the measured accounting. A fragmented first sample can land
// before the peer finishes matching, and a fully missed first burst
// leaves no reassembly state to NACK from: a permanent gap no repair
// can close, while later samples flow fine. The warm copy is sent
// uncounted (no markSent, so Sent still equals the measured count) and
// the measured stream follows after agentWarmGap: a transient shorter
// than the gap takes at most one copy, and the survivor lands in dups,
// never in Recv. A sustained outage still gaps loudly.
func sendWarmSample(backend *dds.Backend, cfg args) int {
	wire, err := encodePayload(cfg.seqBase, cfg.size, time.Now().UnixNano())
	if err != nil {
		log.Printf("warm encode: %v", err)
		return exitCrash
	}
	msg := transports.OutgoingMessage{
		TopicName: cfg.topic,
		TypeName:  agentTypeIDL,
		Payload:   wire,
		TypeHash:  dds.StringTypeHash,
	}
	err = publishWithRetry(context.Background(), backend, msg, "warm")
	if err != nil {
		log.Printf("warm publish: %v", err)
		if errors.Is(err, dds.ErrNoRoute) {
			return exitBadArgs
		}
		return exitCrash
	}
	log.Printf("warm sent seq=%d size=%d", cfg.seqBase, cfg.size)
	return exitOK
}

// matchAndWarm gates the first publish on a matched reader, settles past
// peer SEDP processing, and sends one uncounted warm copy of the first
// fragmented sample. It returns exitOK to continue publishing, or the
// exit code to stop with.
func matchAndWarm(backend *dds.Backend, cfg args, res *resultDoc) int {
	if cfg.waitForMatch == 0 {
		return exitOK
	}
	if !waitForReader(backend, cfg.topic, cfg.waitForMatch) {
		res.Note = "wait for match timed out after " + formatSecs(cfg.waitForMatch) + "s"
		log.Printf("pub wait for match timed out after %s", formatSecs(cfg.waitForMatch))
		return exitBadArgs
	}
	log.Printf("pub matched reader, settling %s before %d", agentPostMatchSettle, cfg.count)
	time.Sleep(agentPostMatchSettle)
	if cfg.size <= agentFragWarmMin {
		return exitOK
	}
	if code := sendWarmSample(backend, cfg); code != exitOK {
		return code
	}
	time.Sleep(agentWarmGap)
	return exitOK
}

// runPub publishes count payloads on topic at rate and always exits 0,
// unless the match wait times out. Publish phases carry no timeout, like
// the probe; only the match wait is bounded. Fragmented first samples get
// one uncounted warm copy plus a gap before the measured stream; see
// sendWarmSample for why that preserves fail-loud accounting.
func runPub(backend *dds.Backend, cfg args, res *resultDoc) int {
	if code := matchAndWarm(backend, cfg, res); code != exitOK {
		return code
	}
	pubCtx, stop := context.WithCancel(context.Background())
	defer stop()
	err := publishPaced(pubCtx, backend, cfg.topic, cfg.count, cfg.seqBase, cfg.rateHz, cfg.size, func(_ int, sendNs int64) error {
		markSent(res, sendNs)
		return nil
	})
	if err != nil {
		log.Printf("pub publish: %v", err)
		if errors.Is(err, dds.ErrNoRoute) {
			res.Note = "publish without route"
			return exitBadArgs
		}
		return exitCrash
	}
	log.Printf("pub sent %d/%d", res.Sent, cfg.count)
	return exitOK
}

// foldSub folds one taken payload into the sub result. It always counts
// the arrival; only parseable payloads advance seq stats.
func foldSub(res *resultDoc, seen map[int]bool, payload []byte, recvNs int64) {
	markRecv(res, recvNs)
	sendNs, seq, err := decodePayload(payload)
	if err != nil {
		// Record a hex head of the first undecodable payload so a
		// future corrupt-frame investigation has the actual bytes;
		// later ones only bump the note count to bound note size.
		head := payload
		if len(head) > 48 {
			head = head[:48]
		}
		if res.Note == "" {
			res.Note = fmt.Sprintf("unparseable payload ignored for seq stats head=%x len=%d", head, len(payload))
		}
		return
	}
	markSent(res, sendNs)
	if seen[seq] {
		res.Dups++
	} else {
		seen[seq] = true
	}
	res.LatNs = append(res.LatNs, recvNs-sendNs)
}

// takeSubOnce takes one sub sample; it reports false when the loop must
// stop after context end or a fatal backend error. One Take maps to exactly
// one accounting event: stale redeliveries count as dups and return before
// folding, so they never inflate Received nor pin a spurious empty-head
// note in foldSub.
func takeSubOnce(ctx context.Context, backend *dds.Backend, res *resultDoc, seen map[int]bool) (bool, error) {
	msg, err := backend.TakeBytes(ctx)
	if ctx.Err() != nil {
		return false, nil //nolint:nilerr // context end is a clean stop, not a take error
	}
	if errors.Is(err, dds.ErrStaleSequence) {
		res.Dups++
		return true, nil
	}
	if err != nil {
		// Undecodable datagrams are skipped and noted, never fatal: one
		// corrupt or misrouted frame must not kill the subscriber. Like
		// the probe, only the deadline ends the run; a skipped frame
		// that carried a real seq surfaces as a gap and fails loudly.
		if errors.Is(err, dds.ErrProvisionalDecodeFail) ||
			errors.Is(err, dds.ErrUnknownWriter) ||
			errors.Is(err, dds.ErrTruncated) ||
			errors.Is(err, dds.ErrInvalidFrame) {
			log.Printf("sub take: skipping undecodable frame: %v", err)
			if res.Note == "" {
				res.Note = "undecodable frames skipped"
			}
			return true, nil
		}
		return false, fmt.Errorf("bench-agent: sub take: %w", err)
	}
	foldSub(res, seen, msg.Payload, time.Now().UnixNano())
	return true, nil
}

// runSub subscribes until count unique seqs arrive or timeout. Stale backend
// sequences count as dups and continue. The loop gates on unique seqs, not
// raw arrivals, so duplicates can never end the run early with the
// publisher still sending: an early exit would drop the pub's route and
// fail its remaining publishes. With --wait-for-match the sub waits for
// the writer first, so the first take never races SEDP discovery down the
// provisional path: a DATA that arrives before its writer is known fails
// provisional decode, and after a later seq sets the ordering baseline
// such a redelivery is stale forever.
func runSub(ctx context.Context, backend *dds.Backend, cfg args, res *resultDoc) int {
	err := backend.SubscribeWithDurability(ctx, cfg.topic, agentTypeIDL, dds.StringTypeHash, false)
	if err != nil {
		log.Printf("sub subscribe: %v", err)
		return exitCrash
	}
	if cfg.waitForMatch > 0 {
		if !waitForReader(backend, cfg.topic, cfg.waitForMatch) {
			res.Note = "sub wait for match timed out after " + formatSecs(cfg.waitForMatch) + "s"
			log.Printf("sub wait for match timed out after %s", formatSecs(cfg.waitForMatch))
			return exitBadArgs
		}
		log.Printf("sub matched writer, taking %d", cfg.count)
	}
	seen := make(map[int]bool)
	for len(seen) < cfg.count {
		cont, terr := takeSubOnce(ctx, backend, res, seen)
		if terr != nil {
			log.Printf("sub take: %v", terr)
			return exitCrash
		}
		if !cont {
			break
		}
	}
	res.Gaps = computeGaps(seen, cfg.count)
	return finishRecv(cfg, res, len(seen) >= cfg.count)
}

// foldPingReply folds one reply payload into ping state; corrupt and
// unmatched replies are ignored, mirroring the probe.
// dispatch runs one mode; sub runs under a timeout context while pub
// runs unbounded like the probe.
func dispatch(cfg args, backend *dds.Backend, res *resultDoc) int {
	if cfg.mode == "pub" {
		return runPub(backend, cfg, res)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	return runSub(ctx, backend, cfg, res)
}

// execute resolves the domain, joins one backend, runs the mode, and
// closes the backend. It asserts the resolved domain equals the requested
// domain so a ROS_DOMAIN_ID override fails loud instead of leaking into
// another domain.
func execute(cfg args, res *resultDoc) int {
	requested, err := transports.ResolveDomainID(0)
	if err != nil {
		res.Note = "resolve domain failed"
		log.Printf("resolve domain: %v", err)
		return exitBadArgs
	}
	backend, err := dds.NewBackend(dds.SidecarConfig{
		DomainID:    requested,
		PeerAddress: dds.DefaultPeerAddress,
		Range:       transports.DiscoveryRangeSystemDefault,
	})
	if err != nil {
		log.Printf("new backend: %v", err)
		return exitCrash
	}
	participant := sanitize("bench_"+cfg.mode+"_"+cfg.runID) + "_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{
		ParticipantName: participant,
		DomainID:        requested,
		Range:           transports.DiscoveryRangeSystemDefault,
	})
	if err != nil {
		log.Printf("join discovery: %v", err)
		cerr := backend.Close()
		if cerr != nil {
			log.Printf("close: %v", cerr)
		}
		return exitCrash
	}
	if backend.DomainID() != requested {
		log.Printf("domain id = %d want %d (check %s override)", backend.DomainID(), requested, transports.DomainEnvVar)
		res.Note = "domain mismatch"
		cerr := backend.Close()
		if cerr != nil {
			log.Printf("close: %v", cerr)
		}
		return exitBadArgs
	}
	code := dispatch(cfg, backend, res)
	cerr := backend.Close()
	if cerr != nil {
		log.Printf("close: %v", cerr)
	}
	return code
}

// writeDoc prints exactly one JSON document to stdout with a trailing
// newline; all logging stays on stderr.
func writeDoc(res *resultDoc) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	err := enc.Encode(res)
	if err != nil {
		return fmt.Errorf("bench-agent: encode doc: %w", err)
	}
	_, err = os.Stdout.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("bench-agent: write doc: %w", err)
	}
	return nil
}

// installInterrupt exits 130 on SIGINT without printing JSON, mirroring
// probe KeyboardInterrupt.
func installInterrupt() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT)
	go func() {
		<-sigCh
		os.Exit(exitInterrupt)
	}()
}

// run parses, executes, prints the single JSON document, and returns the
// exit code. Arg parse failures print no JSON, mirroring probe
// parser.error; every later path prints exactly one document.
func run(argv []string) int {
	log.SetPrefix("bench-agent: ")
	log.SetFlags(0)
	installInterrupt()
	cfg, err := parseArgs(argv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		log.Printf("%v", err)
		return exitBadArgs
	}
	res := newResult(cfg)
	code := execute(cfg, res)
	if werr := writeDoc(res); werr != nil {
		log.Printf("write doc: %v", werr)
		return exitCrash
	}
	return code
}

// main runs the agent and exits with the probe-compatible exit code.
func main() {
	os.Exit(run(os.Args[1:]))
}
