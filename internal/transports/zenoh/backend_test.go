// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package zenoh

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/podomy/weld/internal/transports"
)

// clearZenohEnv unsets the domain variable for deterministic tests.
func clearZenohEnv(t *testing.T) {
	t.Helper()

	old, had := os.LookupEnv(transports.DomainEnvVar)
	if had {
		t.Setenv(transports.DomainEnvVar, old)
	}

	err := os.Unsetenv(transports.DomainEnvVar)
	if err != nil {
		t.Fatalf("unset env: %v", err)
	}
}

// newJoinedBackend returns a joined backend for data path tests.
func newJoinedBackend(t *testing.T, cfg Config, participant string) *Backend {
	t.Helper()

	backend, err := NewBackend(cfg)
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: participant})
	if err != nil {
		t.Fatalf("join: %v", err)
	}

	return backend
}

// TestBackendImplementsContract checks Backend satisfies transports.Backend.
func TestBackendImplementsContract(t *testing.T) {
	t.Parallel()

	var _ transports.Backend = (*Backend)(nil)
}

// TestNewBackendDefaultsDomain checks unset env keeps caller value.
func TestNewBackendDefaultsDomain(t *testing.T) {
	clearZenohEnv(t)

	backend, err := NewBackend(Config{DomainID: 7})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	if got := backend.DomainID(); got != 7 {
		t.Fatalf("domain = %d, want 7", got)
	}

	if backend.Joined() {
		t.Fatalf("joined is true before JoinDiscovery")
	}

	if backend.Closed() {
		t.Fatalf("closed is true before Close")
	}
}

// TestNewBackendResolvesEnvOverride checks env wins over caller value.
func TestNewBackendResolvesEnvOverride(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "42")

	backend, err := NewBackend(Config{DomainID: 7})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	if got := backend.DomainID(); got != 42 {
		t.Fatalf("domain = %d, want 42", got)
	}

	if got := backend.Config().DomainID; got != 42 {
		t.Fatalf("config domain = %d, want 42", got)
	}
}

// TestNewBackendRejectsBadDomain checks domain range enforcement.
func TestNewBackendRejectsBadDomain(t *testing.T) {
	clearZenohEnv(t)

	_, err := NewBackend(Config{DomainID: 999})
	if err == nil {
		t.Fatalf("NewBackend with domain 999 succeeded, want error")
	}
}

// TestDefaultConfig checks the default joins domain zero.
func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()
	if cfg.DomainID != 0 {
		t.Fatalf("default domain = %d, want 0", cfg.DomainID)
	}
}

// TestStripFQN checks leading plus trailing slash stripping only.
func TestStripFQN(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw  string
		want string
	}{
		{raw: "/chatter", want: "chatter"},
		{raw: "chatter", want: "chatter"},
		{raw: "/a/b", want: "a/b"},
		{raw: "/a/b/", want: "a/b"},
		{raw: "  /chatter  ", want: "chatter"},
	}

	for _, tc := range cases {
		got, err := StripFQN(tc.raw)
		if err != nil {
			t.Fatalf("strip %q: %v", tc.raw, err)
		}

		if got != tc.want {
			t.Fatalf("strip %q = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

// TestStripFQNRejects checks empty plus double slash plus space fail.
func TestStripFQNRejects(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", "   ", "/", "///", "/a//b", "/a b"} {
		_, err := StripFQN(raw)
		if err == nil {
			t.Fatalf("strip %q succeeded, want error", raw)
		}
	}
}

// TestIDLToDDSType checks IDL to DDS mapping.
func TestIDLToDDSType(t *testing.T) {
	t.Parallel()

	got, err := IDLToDDSType("std_msgs__msg__String")
	if err != nil {
		t.Fatalf("convert: %v", err)
	}

	want := "std_msgs::msg::dds_::String_"
	if got != want {
		t.Fatalf("dds type = %q, want %q", got, want)
	}
}

// TestIDLToDDSTypeRejects checks short names fail.
func TestIDLToDDSTypeRejects(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", "String", "a__b", "pkg____Name"} {
		_, err := IDLToDDSType(raw)
		if err == nil {
			t.Fatalf("convert %q succeeded, want error", raw)
		}
	}
}

// TestDataKey checks the four segment key form.
func TestDataKey(t *testing.T) {
	t.Parallel()

	got, err := DataKey(0, "/chatter", "std_msgs::msg::dds_::String_", "RIHS01_abc")
	if err != nil {
		t.Fatalf("data key: %v", err)
	}

	want := "0/chatter/std_msgs::msg::dds_::String_/RIHS01_abc"
	if got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
}

// TestDataKeyHashless checks empty hash yields three segments.
func TestDataKeyHashless(t *testing.T) {
	t.Parallel()

	got, err := DataKey(7, "/a/b", "std_msgs::msg::dds_::String_", "")
	if err != nil {
		t.Fatalf("data key: %v", err)
	}

	want := "7/a/b/std_msgs::msg::dds_::String_"
	if got != want {
		t.Fatalf("key = %q, want %q", got, want)
	}
}

// TestDataKeyRejects checks bad domain plus topic plus type fail.
func TestDataKeyRejects(t *testing.T) {
	t.Parallel()

	_, err := DataKey(999, "/chatter", "std_msgs::msg::dds_::String_", "RIHS01_abc")
	if err == nil {
		t.Fatalf("data key with domain 999 succeeded, want error")
	}

	_, err = DataKey(0, "", "std_msgs::msg::dds_::String_", "RIHS01_abc")
	if err == nil {
		t.Fatalf("data key with empty topic succeeded, want error")
	}

	_, err = DataKey(0, "/chatter", "", "RIHS01_abc")
	if err == nil {
		t.Fatalf("data key with empty type succeeded, want error")
	}

	_, err = DataKey(0, "/chatter", "std_msgs::msg::dds_::String_", "bad hash/withslash")
	if err == nil {
		t.Fatalf("data key with slash hash succeeded, want error")
	}
}

// TestMangleRoundTrip checks mangle plus unmangle invert.
func TestMangleRoundTrip(t *testing.T) {
	t.Parallel()

	if got := MangleLiveliness(""); got != "%" {
		t.Fatalf("mangle empty = %q, want %%", got)
	}

	if got := UnmangleLiveliness("%"); got != "" {
		t.Fatalf("unmangle %% = %q, want empty", got)
	}

	cases := []string{"talker", "ns/node", "a/b/c"}
	for _, raw := range cases {
		mangled := MangleLiveliness(raw)
		if strings.Contains(mangled, "/") {
			t.Fatalf("mangled %q keeps slash", mangled)
		}

		back := UnmangleLiveliness(mangled)
		if back != raw {
			t.Fatalf("round trip %q = %q", raw, back)
		}
	}
}

// TestLivelinessKeys checks prefix plus filter plus token forms.
func TestLivelinessKeys(t *testing.T) {
	t.Parallel()

	if LivelinessPrefix != "@ros2_lv" {
		t.Fatalf("prefix = %q, want @ros2_lv", LivelinessPrefix)
	}

	if got := LivelinessDomainPrefix(3); got != "@ros2_lv/3" {
		t.Fatalf("domain prefix = %q, want @ros2_lv/3", got)
	}

	if got := LivelinessFilter(3); got != "@ros2_lv/3/**" {
		t.Fatalf("filter = %q, want @ros2_lv/3/**", got)
	}

	got, err := LivelinessKey(3, "talker")
	if err != nil {
		t.Fatalf("token: %v", err)
	}

	if got != "@ros2_lv/3/talker" {
		t.Fatalf("token = %q, want @ros2_lv/3/talker", got)
	}

	empty, err := LivelinessKey(3, "")
	if err != nil {
		t.Fatalf("empty token: %v", err)
	}

	if empty != "@ros2_lv/3/%" {
		t.Fatalf("empty token = %q, want @ros2_lv/3/%%", empty)
	}
}

// TestAttachmentRoundTrip checks marshal plus parse preserve fields.
func TestAttachmentRoundTrip(t *testing.T) {
	t.Parallel()

	var gid [16]byte
	for i := range gid {
		gid[i] = byte(i + 1)
	}

	original := Attachment{Sequence: 1, TimestampNs: 123456789, SourceGID: gid}

	raw, err := original.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if len(raw) != AttachmentSize {
		t.Fatalf("encoded length = %d, want %d", len(raw), AttachmentSize)
	}

	parsed, err := ParseAttachment(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if parsed.Sequence != original.Sequence {
		t.Fatalf("sequence = %d, want %d", parsed.Sequence, original.Sequence)
	}

	if parsed.TimestampNs != original.TimestampNs {
		t.Fatalf("timestamp = %d, want %d", parsed.TimestampNs, original.TimestampNs)
	}

	if parsed.SourceGID != original.SourceGID {
		t.Fatalf("gid mismatch")
	}
}

// TestAttachmentRejects checks zero sequence plus bad length fail.
func TestAttachmentRejects(t *testing.T) {
	t.Parallel()

	_, err := Attachment{Sequence: 0}.Marshal()
	if err == nil {
		t.Fatalf("marshal zero sequence succeeded, want error")
	}

	_, err = ParseAttachment([]byte{1, 2, 3})
	if err == nil {
		t.Fatalf("parse short buffer succeeded, want error")
	}

	zeroSeq := make([]byte, AttachmentSize)
	_, err = ParseAttachment(zeroSeq)
	if err == nil {
		t.Fatalf("parse zero sequence succeeded, want error")
	}
}

// TestBackendRequiresJoin checks data path before join reports ErrNotJoined.
func TestBackendRequiresJoin(t *testing.T) {
	clearZenohEnv(t)

	backend, err := NewBackend(DefaultConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
	})
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("publish before join = %v, want ErrNotJoined", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("take before join = %v, want ErrNotJoined", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("report before join = %v, want ErrNotJoined", err)
	}
}

// TestBackendJoinCanceledContext checks canceled join honors ctx.
func TestBackendJoinCanceledContext(t *testing.T) {
	clearZenohEnv(t)

	backend, err := NewBackend(DefaultConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = backend.JoinDiscovery(ctx, transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("join canceled = %v, want context.Canceled", err)
	}

	if backend.Joined() {
		t.Fatalf("joined is true after canceled join")
	}
}

// TestBackendJoinIsIdempotent checks repeat joins return nil.
func TestBackendJoinIsIdempotent(t *testing.T) {
	clearZenohEnv(t)

	backend := newJoinedBackend(t, DefaultConfig(), "talker")

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err := backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "talker"})
	if err != nil {
		t.Fatalf("second join: %v", err)
	}

	if !backend.Joined() {
		t.Fatalf("joined is false after join")
	}

	if got := backend.Participant(); got != "talker" {
		t.Fatalf("participant = %q, want talker", got)
	}

	token, err := backend.Token()
	if err != nil {
		t.Fatalf("token: %v", err)
	}

	if token != "@ros2_lv/0/talker" { //nolint:gosec // test liveliness token, not credentials
		t.Fatalf("token = %q, want @ros2_lv/0/talker", token)
	}
}

// TestBackendPublishTakeStub checks joined paths report not implemented.
func TestBackendPublishTakeStub(t *testing.T) {
	clearZenohEnv(t)

	backend := newJoinedBackend(t, DefaultConfig(), "talker")

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "std_msgs__msg__String",
		Payload:   []byte{0, 1, 2},
	})
	if !errors.Is(err, ErrZenohNotImplemented) {
		t.Fatalf("publish = %v, want ErrZenohNotImplemented", err)
	}

	if errors.Is(err, transports.ErrNotJoined) {
		t.Fatalf("publish = %v, must not be ErrNotJoined after join", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, ErrZenohNotImplemented) {
		t.Fatalf("take = %v, want ErrZenohNotImplemented", err)
	}
}

// TestBackendPublishValidates checks bad topic plus type fail before stub.
func TestBackendPublishValidates(t *testing.T) {
	clearZenohEnv(t)

	backend := newJoinedBackend(t, DefaultConfig(), "talker")

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{
		TopicName: "",
		TypeName:  "std_msgs__msg__String",
	})
	if err == nil {
		t.Fatalf("publish empty topic succeeded, want error")
	}

	if errors.Is(err, ErrZenohNotImplemented) {
		t.Fatalf("publish empty topic = %v, must not be not implemented", err)
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{
		TopicName: "/chatter",
		TypeName:  "bad",
	})
	if err == nil {
		t.Fatalf("publish bad type succeeded, want error")
	}
}

// TestBackendReportGenerationMatrix checks stale and idempotent cases.
func TestBackendReportGenerationMatrix(t *testing.T) {
	clearZenohEnv(t)

	backend := newJoinedBackend(t, DefaultConfig(), "p")

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	err := backend.ReportLiveliness(context.Background(), transports.LivelinessReport{
		ParticipantName: "talker",
		Generation:      7,
		Alive:           true,
	})
	if err != nil {
		t.Fatalf("gen 7 live: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{
		ParticipantName: "talker",
		Generation:      7,
		Alive:           true,
	})
	if err != nil {
		t.Fatalf("repeat same gen same alive is not idempotent: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{
		ParticipantName: "talker",
		Generation:      7,
		Alive:           false,
	})
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("same gen different alive = %v, want ErrStaleGeneration", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{
		ParticipantName: "talker",
		Generation:      6,
		Alive:           true,
	})
	if !errors.Is(err, transports.ErrStaleGeneration) {
		t.Fatalf("backward gen = %v, want ErrStaleGeneration", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{
		ParticipantName: "talker",
		Generation:      8,
		Alive:           false,
	})
	if err != nil {
		t.Fatalf("gen 8 dispose: %v", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{
		ParticipantName: "talker",
		Generation:      8,
		Alive:           false,
	})
	if err != nil {
		t.Fatalf("repeat dispose is not idempotent: %v", err)
	}
}

// TestBackendSequenceStartsAtOne checks first claimed sequence is one.
func TestBackendSequenceStartsAtOne(t *testing.T) {
	clearZenohEnv(t)

	backend, err := NewBackend(DefaultConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	if got := backend.nextSequence(); got != FirstSequenceNumber {
		t.Fatalf("first sequence = %d, want %d", got, FirstSequenceNumber)
	}

	if got := backend.nextSequence(); got != FirstSequenceNumber+1 {
		t.Fatalf("second sequence = %d, want %d", got, FirstSequenceNumber+1)
	}
}

// TestBackendKeyForUsesDomain checks KeyFor carries backend domain.
func TestBackendKeyForUsesDomain(t *testing.T) {
	t.Setenv(transports.DomainEnvVar, "9")

	backend, err := NewBackend(Config{DomainID: 0})
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	got, err := backend.KeyFor("/chatter", "std_msgs__msg__String")
	if err != nil {
		t.Fatalf("key for: %v", err)
	}

	if !strings.HasPrefix(got, "9/chatter/") {
		t.Fatalf("key = %q, want prefix 9/chatter/", got)
	}
}

// TestBackendSourceGIDDiffers checks distinct participants get distinct GIDs.
func TestBackendSourceGIDDiffers(t *testing.T) {
	clearZenohEnv(t)

	first := newJoinedBackend(t, DefaultConfig(), "alpha")

	defer func() {
		err := first.Close()
		if err != nil {
			t.Errorf("close first: %v", err)
		}
	}()

	clearZenohEnv(t)

	second, err := NewBackend(DefaultConfig())
	if err != nil {
		t.Fatalf("new second: %v", err)
	}

	defer func() {
		err := second.Close()
		if err != nil {
			t.Errorf("close second: %v", err)
		}
	}()

	err = second.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "beta"})
	if err != nil {
		t.Fatalf("join second: %v", err)
	}

	if first.SourceGID() == second.SourceGID() {
		t.Fatalf("distinct participants share GID")
	}
}

// TestBackendCloseIdempotent checks Close returns nil twice.
func TestBackendCloseIdempotent(t *testing.T) {
	clearZenohEnv(t)

	backend, err := NewBackend(DefaultConfig())
	if err != nil {
		t.Fatalf("new backend: %v", err)
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	if !backend.Closed() {
		t.Fatalf("closed is false after Close")
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("second close: %v", err)
	}
}

// TestBackendPostCloseErrors checks all methods fail after Close.
func TestBackendPostCloseErrors(t *testing.T) {
	clearZenohEnv(t)

	backend := newJoinedBackend(t, DefaultConfig(), "p")

	err := backend.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}

	err = backend.Close()
	if err != nil {
		t.Fatalf("second close: %v", err)
	}

	err = backend.JoinDiscovery(context.Background(), transports.DiscoveryInfo{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("join after close = %v, want ErrBackendClosed", err)
	}

	err = backend.PublishBytes(context.Background(), transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("publish after close = %v, want ErrBackendClosed", err)
	}

	_, err = backend.TakeBytes(context.Background())
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("take after close = %v, want ErrBackendClosed", err)
	}

	err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{ParticipantName: "p"})
	if !errors.Is(err, transports.ErrBackendClosed) {
		t.Fatalf("report after close = %v, want ErrBackendClosed", err)
	}
}

// TestBackendContextCancel checks canceled contexts return context errors.
func TestBackendContextCancel(t *testing.T) {
	clearZenohEnv(t)

	backend := newJoinedBackend(t, DefaultConfig(), "p")

	defer func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := backend.PublishBytes(ctx, transports.OutgoingMessage{TopicName: "/chatter"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("publish canceled = %v, want context.Canceled", err)
	}

	_, err = backend.TakeBytes(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("take canceled = %v, want context.Canceled", err)
	}

	err = backend.ReportLiveliness(ctx, transports.LivelinessReport{ParticipantName: "p"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("report canceled = %v, want context.Canceled", err)
	}
}

// checkAllowedZenohErr fails unless err is an allowed skeleton error.
func checkAllowedZenohErr(t *testing.T, err error, op string) {
	t.Helper()

	if err == nil {
		return
	}

	if errors.Is(err, transports.ErrBackendClosed) {
		return
	}

	if errors.Is(err, ErrZenohNotImplemented) {
		return
	}

	if errors.Is(err, transports.ErrStaleGeneration) {
		return
	}

	t.Errorf("%s: %v", op, err)
}

// TestBackendConcurrentUse checks concurrent data path and close stay safe.
func TestBackendConcurrentUse(t *testing.T) {
	clearZenohEnv(t)

	backend := newJoinedBackend(t, DefaultConfig(), "p")

	var wg sync.WaitGroup

	for range 8 {
		wg.Go(func() {
			err := backend.PublishBytes(context.Background(), transports.OutgoingMessage{
				TopicName: "/chatter",
				TypeName:  "std_msgs__msg__String",
			})
			checkAllowedZenohErr(t, err, "publish")

			_, err = backend.TakeBytes(context.Background())
			checkAllowedZenohErr(t, err, "take")

			err = backend.ReportLiveliness(context.Background(), transports.LivelinessReport{
				ParticipantName: "p",
				Generation:      1,
				Alive:           true,
			})
			checkAllowedZenohErr(t, err, "report")
		})
	}

	wg.Go(func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close: %v", err)
		}
	})

	wg.Wait()

	err := backend.Close()
	if err != nil {
		t.Fatalf("final close: %v", err)
	}
}
