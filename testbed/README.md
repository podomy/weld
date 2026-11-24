# Testbed

Live fleet and gates. Canonical doc is `docs/testbed.md`.

```sh
go test ./testbed/unit/... -count=1
go test -timeout 60m ./testbed/live/... -count=1
```

Bring the fleet up once with `cd testbed && docker compose up -d`. Tests skip without a peer. Detail: `docs/testbed.md`, `docs/conformance.md`.
