// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package helpers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Parallel domain pool invariants.
//
// The pool hands out exclusive DDS domain IDs for parallel live tests so
// t.Parallel workers never share a domain. The invariants are:
//
//   - Disjoint: pool 70 through 101 never overlaps fixed uses 42, 43, 44,
//     45, 47, 48, 55, or 60 through 69. Fixed uses stay reserved for
//     canonical, distro, action, large payload, and ephemeral cases.
//     Unit port probes live at 46, 49 through 54, and 56, and the
//     collision gate pins 58, so pooled live traffic never shares
//     numbers with slot successiveness assertions.
//   - Bounded: every pooled domain sits in 0 through 101 (the ROS
//     transports.MaxDomainID range enforced by ValidateDomain,
//     JoinMulticast, and NewLocalParticipant) and therefore also in the
//     DDS wire range 0 through 232 set by the UDP port limit. RTPS maps
//     SPDP to 7400 plus 250 per domain (see
//     internal/transports/dds/rtps/udp.go), so domain 101 uses port
//     32650 and domain 232 would use 65400; the pool stays far below the
//     65535 UDP ceiling. The 101 cap is structural: it is pinned by
//     transports.MaxDomainID (see internal/transports/transport.go) and
//     rechecked by every backend (mqtt, zenoh, fastdds, dds sidecar,
//     SPDP, SEDP), so the pool cannot expand past 101 without a wire
//     change and stays at 32 domains.
//   - Exclusive: each domain is handed out at most once concurrently,
//     across threads and across separate go test binaries. A
//     process-wide mutex guards the used set for same-binary speed, and
//     one flocked lock file per domain (see domainLockPath) guards
//     cross-binary exclusion. A domain is handed out only after its
//     non-blocking exclusive flock succeeds. Go opens lock files
//     close-on-exec, so spawned weld and docker children never inherit
//     the hold.
//   - Crash safe: the lock lives in the open file description, so the
//     kernel releases it when the holder exits for any reason. There is
//     no PID tracking and no stale state: leftover lock files are empty
//     placeholders reused by the next run.
//   - Fail closed: lock directory creation, lock file open, and any
//     flock failure other than contention return an error instead of
//     handing out an unguarded domain. Contention on one domain moves on
//     to the next candidate. Exhaustion returns an error and never wraps
//     around to a live domain.
//   - Shared TMPDIR: the lock directory is fixed under os.TempDir so
//     concurrent binaries agree on it without configuration. Binaries
//     with disjoint TMPDIR values cannot see each other locks; the
//     default shared /tmp keeps them coordinated.
//   - Linux scope: cross-process exclusion uses flock(2) through
//     syscall.Flock, which the Linux docker hosts of the live harness
//     always provide. On a platform or filesystem where flock is
//     unavailable, AllocateDomain fails closed with an error and never
//     hands out an unguarded domain.
//   - Serial unchanged: the pool never returns LiveDomain 47, so a test
//     that never calls Allocate keeps using 47 exactly as before.
//   - Explicit plumbing: allocation never touches process state. Callers
//     pass the domain explicitly through DockerArgs domain params,
//     weld --domain flags, and EnvWithDomain slices. No t.Setenv use
//     means parallel workers never race on the shared environment.
//   - No reuse while live: a domain stays marked used and its flock held
//     until its test releases it through t.Cleanup, so sequential reuse
//     is safe but concurrent overlap is impossible.
//   - Ownership tokens: each lease mints a nonzero token stored with the
//     domain. ReleaseDomainLease frees only on token match, so an early
//     manual release plus a later stale t.Cleanup cannot free another
//     test domain that reused the number.
const (
	// ParallelDomainMin is the first pooled exclusive domain.
	ParallelDomainMin = 70
	// ParallelDomainMax is the last pooled exclusive domain.
	ParallelDomainMax = 101
)

// errDomainHeld reports one pooled domain is locked by another process.
// AllocateDomain maps it to trying the next candidate; it never reaches
// the caller unwrapped.
var errDomainHeld = errors.New("helpers: domain held by another process")

// parallelPoolUsed tracks pooled domains currently held by live tests.
var parallelPoolUsed = map[int]bool{}

// parallelPoolTokens holds the ownership token per held pooled domain.
// Each AllocateDomainLease mints a fresh nonzero token; ReleaseDomainLease
// releases only when the caller token matches the current holder, so a
// stale t.Cleanup after an early manual release cannot free another test
// domain that reused the number.
var parallelPoolTokens = map[int]uint64{}

// parallelPoolNextToken mints ownership tokens. Zero means no holder.
var parallelPoolNextToken uint64 = 1

// parallelPoolLocks holds the open lock file per allocated domain. Each
// file stays open with its exclusive flock until ReleaseDomainLease, so the
// kernel releases the domain even when the test binary crashes.
var parallelPoolLocks = map[int]*os.File{}

// parallelPoolMu guards parallelPoolUsed, parallelPoolTokens, and parallelPoolLocks for concurrent use.
var parallelPoolMu sync.Mutex

// domainLockDir returns the fixed directory holding per-domain lock files.
//
// The path sits under os.TempDir so concurrent go test binaries agree on
// it without configuration. Binaries with disjoint TMPDIR values cannot
// see each other locks; the default shared /tmp keeps them coordinated.
func domainLockDir() string {
	return filepath.Join(os.TempDir(), "weld-live-domainpool")
}

// domainLockPath returns the fixed lock file path for one pooled domain.
//
// One file per domain keeps contention per domain: a held domain blocks
// only itself while the scan moves on to the next candidate. Files are
// never unlinked, so a release cannot race a concurrent open by path;
// empty leftovers are harmless placeholders.
func domainLockPath(domain int) string {
	return filepath.Join(domainLockDir(), fmt.Sprintf("domain-%d.lock", domain))
}

// acquireDomainLock opens the lock file for domain and takes a
// non-blocking exclusive flock on it.
//
// The caller must hold parallelPoolMu. Success returns the open file,
// which the caller stores in parallelPoolLocks and keeps open until
// releaseDomainLock: the open description owns the lock, and closing it
// releases the domain, including on crash through the kernel. Contention
// returns errDomainHeld so the caller tries the next candidate. Any
// other failure (lock directory, open, or unexpected flock error)
// returns a wrapped error so the caller fails closed without handing
// out the domain.
func acquireDomainLock(domain int) (*os.File, error) {
	err := os.MkdirAll(domainLockDir(), 0o750)
	if err != nil {
		return nil, fmt.Errorf("helpers: domain lock dir: %w", err)
	}
	fh, err := os.OpenFile(domainLockPath(domain), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("helpers: open domain %d lock: %w", domain, err)
	}
	err = syscall.Flock(int(fh.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		cerr := fh.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("helpers: domain %d held: %w", domain, errors.Join(errDomainHeld, cerr))
		}
		return nil, fmt.Errorf("helpers: flock domain %d: %w", domain, errors.Join(err, cerr))
	}
	return fh, nil
}

// releaseDomainLock unlocks and closes the lock file for domain.
//
// The caller must hold parallelPoolMu. It is idempotent: a domain with
// no held lock is a no-op. Unlock and close are best effort: the close
// itself releases the flock, so a failed unlock never wedges the domain
// for other binaries.
func releaseDomainLock(domain int) {
	fh, ok := parallelPoolLocks[domain]
	if !ok {
		return
	}
	delete(parallelPoolLocks, domain)
	_ = syscall.Flock(int(fh.Fd()), syscall.LOCK_UN) //nolint:errcheck // best-effort unlock, the close below releases the lock
	_ = fh.Close()                                   //nolint:errcheck // best-effort cleanup, closing releases the flock
}

// AllocateDomainLease returns one exclusive pooled domain plus an ownership token.
//
// It scans from ParallelDomainMin and marks the first free domain used.
// A domain counts as free only when it is absent from the in-process
// used set and its cross-process lock file flocks on the first try, so
// two concurrent go test binaries never hold the same domain.
// Contention moves on to the next candidate. Exhaustion returns an
// error; the caller must not proceed without a domain. The token is a
// nonzero owner ID; ReleaseDomainLease frees only on token match so a
// stale cleanup after an early manual release cannot free a reused
// domain. Allocation purges stale ros2 daemons for the domain so every
// test starts clean even after killed runs left participants behind.
func AllocateDomainLease() (int, uint64, error) {
	domain, token, err := claimPooledDomain()
	if err != nil {
		return 0, 0, err
	}
	purgeStaleRosDaemon(domain)
	return domain, token, nil
}

// claimPooledDomain marks the first free pooled domain under the pool lock.
//
// It returns the domain plus ownership token with the lock released; the
// caller purges residue after return so slow docker exec never serializes
// other allocators behind the pool mutex.
func claimPooledDomain() (int, uint64, error) {
	parallelPoolMu.Lock()
	defer parallelPoolMu.Unlock()
	for domain := ParallelDomainMin; domain <= ParallelDomainMax; domain++ {
		if parallelPoolUsed[domain] {
			continue
		}
		err := ValidateDomain(domain)
		if err != nil {
			return 0, 0, fmt.Errorf("helpers: allocate domain %d invalid: %w", domain, err)
		}
		fh, err := acquireDomainLock(domain)
		if err != nil {
			if errors.Is(err, errDomainHeld) {
				continue
			}
			return 0, 0, err
		}
		token := parallelPoolNextToken
		if token == 0 {
			token = 1
		}
		parallelPoolNextToken = token + 1
		if parallelPoolNextToken == 0 {
			parallelPoolNextToken = 1
		}
		parallelPoolUsed[domain] = true
		parallelPoolTokens[domain] = token
		parallelPoolLocks[domain] = fh
		return domain, token, nil
	}
	return 0, 0, fmt.Errorf("helpers: domain pool %d through %d exhausted", ParallelDomainMin, ParallelDomainMax)
}

// AllocateDomain returns one exclusive pooled domain.
//
// It wraps AllocateDomainLease and discards the ownership token. Prefer
// AllocateDomainLease plus ReleaseDomainLease for manual early releases;
// the token form keeps a stale t.Cleanup from freeing a reused domain.
// Direct callers that use AllocateDomain must call ReleaseDomain when the
// domain is no longer needed.
func AllocateDomain() (int, error) {
	domain, _, err := AllocateDomainLease()
	if err != nil {
		return 0, err
	}
	return domain, nil
}

// fleetContainers lists every fleet peer that can host DDS participants.
//
// Pooled allocations are exclusive per domain, but ros2 CLI daemons and
// leaked peer procs linger per container. Purging only the Jazzy peer
// leaves humble and rolling residue visible on the shared multicast
// groups, so isolation purges must cover the whole fleet.
func fleetContainers() []string {
	seen := map[string]bool{Container: true}
	out := []string{Container}
	for _, peer := range DistroPeers() {
		if peer.Container == "" || seen[peer.Container] {
			continue
		}
		seen[peer.Container] = true
		out = append(out, peer.Container)
	}
	return out
}

// containerProcGone reports whether no process matches the pkill pattern
// in one container. pgrep runs directly (no shell wrapper) so the probe
// command itself can never match the pattern. Exec failures report
// not-gone so callers retry instead of assuming a clean container.
func containerProcGone(ctx context.Context, container, pattern string) bool {
	args := DockerArgs(container, 0, "pgrep", "-f", pattern)
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness invokes docker with fixed test args
	out, err := cmd.Output()
	if len(bytes.TrimSpace(out)) != 0 {
		return false
	}
	if err == nil {
		return true
	}
	// pgrep exits 1 on no match, which still means gone. Any other error
	// (docker unreachable, container gone) reports not-gone so the caller
	// retries instead of assuming a clean container.
	var exitErr *exec.ExitError
	return errors.As(err, &exitErr)
}

// WaitForContainerProcGone polls until no process matches the pkill pattern
// in one container or the bound expires. It returns true when the container
// is clean. Callers that require death fail loudly on false instead of
// racing teardown against a lingering peer.
func WaitForContainerProcGone(ctx context.Context, container, pattern string, bound time.Duration) bool {
	deadline := time.Now().Add(bound)
	for {
		if containerProcGone(ctx, container, pattern) {
			return true
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return containerProcGone(context.WithoutCancel(ctx), container, pattern)
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}

// stalePeerAgeSeconds bounds legitimate peer lifetime for the purge.
//
// Test talkers and echoes live seconds to about a minute; the longest
// suites hold peers under two minutes. Anything older than fifteen
// minutes is residue from a killed run, never a live test, so the purge
// may reap it without risking parallel or sequential suites.
const stalePeerAgeSeconds = "900"

// purgeStaleRosDaemon stops ros2 CLI daemon procs for one domain on every
// fleet peer and reaps ancient topic peers fleet-wide, verifying daemons.
//
// Daemons linger after killed runs and join discovery on reused domains,
// breaking isolation assertions and slot probes. Live daemons cannot
// exist yet at allocate time because no test on this domain runs before
// its allocator returns, so the purge only removes residue. Ancient
// topic procs (older than stalePeerAgeSeconds) are crash residue too:
// no live test holds a peer that long, and parallel suites hold other
// domains, so reaping them cannot signal a live test. Survivors fail
// loudly instead of handing out a dirty domain. Failures are ignored
// only when docker itself is unreachable in every attempt; a surviving
// daemon is loud.

//nolint:contextcheck,nolintlint // cleanup contexts intentionally outlive test cancellation, the purge runs at allocate time with a bounded Background timeout
func purgeStaleRosDaemon(domain int) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pattern := "daemonize.*ros-domain-id " + strconv.Itoa(domain) + " "
	for _, container := range fleetContainers() {
		KillRosDaemon(ctx, container, domain)
		if !WaitForContainerProcGone(ctx, container, pattern, 10*time.Second) {
			log.Printf("helpers: purge domain %d daemon survives on %s pattern %q", domain, container, pattern)
		}
		reapStaleTopicPeers(ctx, container)
	}
}

// reapStaleTopicPeers kills ros2 topic procs older than the stale bound.
//
// Crash residue accumulates across suites in one fleet and poisons later
// isolation assertions. The age bound keeps the reap clear of live tests:
// parallel suites hold other domains and no live peer approaches the bound.
// One extended pattern covers both verbs so the reap costs one exec.
func reapStaleTopicPeers(ctx context.Context, container string) {
	args := DockerArgs(container, 0, "pkill", "--older", stalePeerAgeSeconds, "-f", "topic (pub|echo)")
	cmd := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // live harness kills only stale test peers
	_ = cmd.Run()                                      //nolint:errcheck // best effort; absence of stale peers is not an error
}

// ReleaseDomainLease frees a pooled domain only on token match for later sequential reuse.
//
// It drops the in-process mark and releases the cross-process flock when
// the caller token matches the current holder, so the domain becomes
// available to later tests in this binary and in concurrent binaries. It
// is idempotent: a token mismatch, a free domain, or an out of pool
// domain is a no-op, so a stale t.Cleanup after an early manual release
// cannot free another test domain that reused the number.
func ReleaseDomainLease(domain int, token uint64) {
	parallelPoolMu.Lock()
	defer parallelPoolMu.Unlock()
	if token == 0 {
		return
	}
	current, ok := parallelPoolTokens[domain]
	if !ok || current != token {
		return
	}
	releaseDomainLock(domain)
	delete(parallelPoolUsed, domain)
	delete(parallelPoolTokens, domain)
}

// ReleaseDomain frees a pooled domain for later sequential reuse.
//
// Legacy form without ownership checking: it drops the in-process mark
// and releases the cross-process flock unconditionally. Prefer
// ReleaseDomainLease with the AllocateDomainLease token; the token form
// keeps a stale cleanup from freeing a reused domain. MustAllocateDomain
// no longer uses this; it registers the token form in t.Cleanup.
func ReleaseDomain(domain int) {
	parallelPoolMu.Lock()
	defer parallelPoolMu.Unlock()
	releaseDomainLock(domain)
	delete(parallelPoolUsed, domain)
	delete(parallelPoolTokens, domain)
}

// MustAllocateDomainLease returns one exclusive pooled domain plus token or fails the test.
//
// It wraps AllocateDomainLease with t.Fatalf on exhaustion and registers
// ReleaseDomainLease in t.Cleanup so sequential tests reuse domains while
// concurrent workers hold distinct ones. Use the returned token for an
// early manual ReleaseDomainLease; the later Cleanup with the same token
// is then a safe no-op, and a reused domain with a new token is never
// freed by the stale Cleanup. It never touches process state, so it is
// safe to call before or after t.Parallel.
func MustAllocateDomainLease(t *testing.T) (int, uint64) {
	t.Helper()
	domain, token, err := AllocateDomainLease()
	if err != nil {
		t.Fatalf("allocate domain: %v", err)
	}
	t.Cleanup(func() {
		ReleaseDomainLease(domain, token)
	})
	return domain, token
}

// MustAllocateDomain returns one exclusive pooled domain or fails the test.
//
// It wraps AllocateDomainLease with t.Fatalf on exhaustion and registers
// ReleaseDomainLease in t.Cleanup so sequential tests reuse domains while
// concurrent workers hold distinct ones. The token stays in the Cleanup
// closure, so a manual early release must use MustAllocateDomainLease to
// get its token; otherwise the automatic Cleanup still owns the domain.
// It never touches process state, so it is safe to call before or after
// t.Parallel.
func MustAllocateDomain(t *testing.T) int {
	t.Helper()
	domain, token, err := AllocateDomainLease()
	if err != nil {
		t.Fatalf("allocate domain: %v", err)
	}
	t.Cleanup(func() {
		ReleaseDomainLease(domain, token)
	})
	return domain
}

// MustAllocateDomainU32 returns one exclusive pooled domain as uint32 or fails the test.
//
// It wraps MustAllocateDomain with a range check before narrowing to
// uint32. Pooled domains 70 through 101 always fit uint32 and sit in
// 0 through 101, so the conversion below is provably safe. Use it at
// allocator call sites that feed dds and rtps APIs taking uint32 so
// those sites carry no inline conversion.
func MustAllocateDomainU32(t *testing.T) uint32 {
	t.Helper()
	domain := MustAllocateDomain(t)
	if domain < 0 || domain > 101 {
		t.Fatalf("allocated domain %d out of range 0 through 101", domain)
	}
	return uint32(domain) //nolint:gosec // G115: pooled domain 70 through 101 fits uint32
}

// IsParallelDomain reports whether domain sits in the pooled range.
//
// Pooled domains 70 through 101 never overlap fixed 42, 43, 44, 45, 47,
// 48, 55, or ephemeral 60 through 69, nor unit probes at 46, 49 through
// 54, and 56 or the collision gate at 58, so parallel workers stay
// isolated from canonical, distro, action, payload, and liveliness runs
// and from slot successiveness assertions.
func IsParallelDomain(domain int) bool {
	return domain >= ParallelDomainMin && domain <= ParallelDomainMax
}
