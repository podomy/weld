// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package helpers

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

// parallelPoolLocks holds the open lock file per allocated domain. Each
// file stays open with its exclusive flock until ReleaseDomain, so the
// kernel releases the domain even when the test binary crashes.
var parallelPoolLocks = map[int]*os.File{}

// parallelPoolMu guards parallelPoolUsed and parallelPoolLocks for concurrent use.
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

// AllocateDomain returns one exclusive pooled domain.
//
// It scans from ParallelDomainMin and marks the first free domain used.
// A domain counts as free only when it is absent from the in-process
// used set and its cross-process lock file flocks on the first try, so
// two concurrent go test binaries never hold the same domain.
// Contention moves on to the next candidate. Exhaustion returns an
// error; the caller must not proceed without a domain. Use
// MustAllocateDomain in tests for t.Fatalf handling plus automatic
// release. Direct callers that use AllocateDomain must call
// ReleaseDomain when the domain is no longer needed. Allocation purges
// stale ros2 daemons for the domain so every test starts clean even
// after killed runs left participants behind.
func AllocateDomain() (int, error) {
	parallelPoolMu.Lock()
	defer parallelPoolMu.Unlock()
	for domain := ParallelDomainMin; domain <= ParallelDomainMax; domain++ {
		if parallelPoolUsed[domain] {
			continue
		}
		err := ValidateDomain(domain)
		if err != nil {
			return 0, fmt.Errorf("helpers: allocate domain %d invalid: %w", domain, err)
		}
		fh, err := acquireDomainLock(domain)
		if err != nil {
			if errors.Is(err, errDomainHeld) {
				continue
			}
			return 0, err
		}
		parallelPoolUsed[domain] = true
		parallelPoolLocks[domain] = fh
		purgeStaleRosDaemon(domain)
		return domain, nil
	}
	return 0, fmt.Errorf("helpers: domain pool %d through %d exhausted", ParallelDomainMin, ParallelDomainMax)
}

// purgeStaleRosDaemon stops ros2 CLI daemon procs for one domain best effort.
//
// Daemons linger after killed runs and join discovery on reused domains,
// breaking isolation assertions and slot probes. Live daemons cannot
// exist yet at allocate time because no test on this domain runs before
// its allocator returns, so the purge only removes residue. It targets
// the Jazzy fleet peer that serves pooled allocations; distro peers
// need explicit kills at their call sites. Failures are ignored.

//nolint:contextcheck,nolintlint // cleanup contexts intentionally outlive test cancellation, the purge runs at allocate time with a bounded Background timeout
func purgeStaleRosDaemon(domain int) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	KillRosDaemon(ctx, Container, domain)
}

// ReleaseDomain frees a pooled domain for later sequential reuse.
//
// It drops the in-process mark and releases the cross-process flock, so
// the domain becomes available to later tests in this binary and in
// concurrent binaries. It is idempotent: releasing a free or out of
// pool domain is a no-op. MustAllocateDomain registers this through
// t.Cleanup, so most tests never call it directly. Direct AllocateDomain
// callers must call it when the test no longer needs exclusivity.
func ReleaseDomain(domain int) {
	parallelPoolMu.Lock()
	defer parallelPoolMu.Unlock()
	releaseDomainLock(domain)
	delete(parallelPoolUsed, domain)
}

// MustAllocateDomain returns one exclusive pooled domain or fails the test.
//
// It wraps AllocateDomain with t.Fatalf on exhaustion and registers
// ReleaseDomain in t.Cleanup so sequential tests reuse domains while
// concurrent workers hold distinct ones. It never touches process
// state, so it is safe to call before or after t.Parallel.
func MustAllocateDomain(t *testing.T) int {
	t.Helper()
	domain, err := AllocateDomain()
	if err != nil {
		t.Fatalf("allocate domain: %v", err)
	}
	t.Cleanup(func() {
		ReleaseDomain(domain)
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
