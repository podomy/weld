# Proofs: transports router

TLA+ spec for `../router.go` (plus the `Backend.TakeBytes`
contract in `../transport.go` and intended semantics in
`../router_test.go`). Run from this directory, e.g.:

```sh
java -XX:+UseParallelGC -cp /tmp/opencode/tla2tools.jar \
  tlc2.TLC -workers 8 -config router_2.cfg router.tla
```

Config notes: constants use explicit sets (this TLC build rejects
`..` ranges). `states/` and `*_TTrace*` are TLC working files and
are git-ignored, not proof artifacts.

## router.tla — TakeBytes multiplexing

Models R backends with depth-bounded FIFOs, the router racing all
backends with per-backend resolve (ties pick nondeterministically
among ready), a pending-extras buffer, sticky cancel, and one
mid-race backend close.

- `router_1.cfg`: 2 backends, 3 samples, FIFO depth 2.
- `router_2.cfg`: 3 backends, 4 samples, FIFO depth 2.

Proven: type safety, conservation (every staged sample in exactly
one place), at-most-once takes, no drops (consumed extras always
land in pending, on success, abort, or late arrival), cancel freezes
takes and returns an error, no loss or starvation absent cancel plus
close, a close strands only that backend's queued samples, deadlock
freedom. The race returns the first win immediately while stragglers
drain late; concurrent TakeBytes callers serialize on takeMu.
