# Weld bench endpoint image.
#
# Build (from the repo root):
#   docker compose -f testbed/docker-compose.yml build weld
#
# Build context is the repo root (this Dockerfile lives at ./Dockerfile,
# the compose default) because the agent package is built as
# ./testbed/bench/agent. No build args are defined; none are needed.
#
# Runtime pick: debian:bookworm-slim, NOT distroless-static. The compose
# service idles on `command: ["sleep", "infinity"]`, exactly like the ROS
# peers (ros-humble/ros-jazzy/ros-rolling), and distroless-static ships
# no /bin/sleep, so the container would exit. bookworm-slim matches the
# builder distro and provides sleep via coreutils with no extra packages.
# No CA certs are added: the agent speaks plain UDP/SPDP only, no TLS.
# No shell is used at runtime: control is `docker exec` argv, same as the
# ROS peers.

# Builder matches the repo toolchain (go.mod: go 1.26).
FROM golang:1.26-bookworm AS builder
WORKDIR /src
# No go.sum in the repo (stdlib only); copy the tree and build static.
# CGO_ENABLED=0 is safe: the bench agent pulls no cgo package, and cgo
# stays confined to internal/rmwbridge for the future rmw_weld shim.
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /bench-agent ./testbed/bench/agent

FROM debian:bookworm-slim
COPY --from=builder /bench-agent /bench-agent
CMD ["sleep", "infinity"]
