# Weld Makefile: thin conveniences only. The pre-commit check is the
# source of truth (see check/main.go); these targets delegate to it.

.PHONY: setup verify test build

# setup points git at the repo hooks so the verify script runs on
# every commit via .githooks/pre-commit. Run once per fresh clone.
setup:
	git config core.hooksPath .githooks

# verify runs the full pre-commit check: tidy plus format diff plus
# header check plus lint plus vet plus test, per module.
verify:
	go run ./check/

# test runs the root module unit tests. verify covers sdk and cmd/weld.
test:
	go test ./...

# build compiles the CLI. cmd/weld is its own module outside the
# workspace, so it builds with GOWORK=off.
build:
	cd cmd/weld && GOWORK=off go build -o /tmp/weld .
