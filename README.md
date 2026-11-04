Weld

License: GNU Affero General Public License v3.0 or later (/LICENSE)

Weld is robotics middleware written in Go. One unified core moves
topics, services, actions, and parameters across transports, with
structured delivery, copy once handling, and a leaderless data plane
that stays correct under partition. Version one ships the DDS backend
against CycloneDDS; FastDDS, Zenoh, and MQTT transports follow after
v1 behind the same interface.

Weld speaks ROS 2 on the wire, so Go programs join ROS 2 systems as
first-class nodes, verified by mixed fleet behavior.

**Weld is faster than ROS 2 on the same host: up to 68% lower latency.**

**It also scales further: zero packet loss where ROS starts dropping,
with about 25% more throughput.**

Benchmarks

  Weld versus ROS 2 (ros-jazzy/CycloneDDS), same host and bytes, full
  matrix green (110 rows, exit 0; reports in testbed/logs/bench-*.html).
  Weld-to-weld latency is lower than ros-to-ros at every payload size
  (16 to 68% lower p50 from 64 B to 1 MiB) and every lossless rate
  (about 50% lower through 100 to 800 Hz at 1 KiB). Weld legs hold 0%
  loss from 50 Hz to 100 kHz offered; ros-to-ros first drops at 10 kHz.
  Full tables, the latency-vs-throughput chart, and caveats live in
  testbed/bench/README.md.

![Latency vs achieved throughput, 1 KiB sweep: one p50 curve per leg in
leg colors with circle markers, p99 dashed with triangles; red rings
mark lossy rates.](testbed/bench/latency-vs-throughput.svg)

Documentation

  Weld design - /docs/design: architecture, invariants, and build order.
  Weld invariants - /docs/invariants: 250+ testable promises the system
    holds, drawn from 6000+ upstream ROS issues.
  Overview - /docs/overview: what Weld is and what v1 ships.
  Types - /docs/types: IDL mapping, registry, constants.
  Type hashes - /docs/hash: RIHS01 computation and negotiation.
  CDR - /docs/cdr: XCDR1 wire encoding.
  Introspection and bounds - /docs/introspection: reflection tables
    and limits.
  Corpus - /docs/corpus: message families and fixtures.
  Actions - /docs/actions: goals, feedback, results, timeouts.
  Services - /docs/services: calls, deadlines, discovery.
  Config - /docs/config: versioned parameter store.
  Supervision - /docs/supervision: serial and pooled groups.
  Time - /docs/time: clock domains, jumps, overload.
  QoS - /docs/qos: matching, best-available, facade, retention.
  Transports - /docs/transports: backend interface and DDS sidecar.
  Interop - /docs/interop: field notes from CycloneDDS interop.
  Testbed - /docs/testbed: Docker peers and first interop.
  Testbed Go - /docs/testbed-go: live harness, domains, file split.
  Conformance - /docs/conformance: wire end state and gate record.
  Wireless - /docs/wireless: Rappaport distilled for WiFi fleets.
  CLI - /docs/cli: the weld echo/pub commands plus weld action send.
  Glossary - /docs/glossary: terms in plain words.
  FAQ - /docs/faq: constants, identity quaternion, one binary, sidecar.

Contributing

Discuss your change with the engineering team at contact@podomy.com
before opening a PR in order not to waste anybody's effort or time.

Announcements and engineering updates on distributed systems, consensus
algorithms, and robotic fleet coordination are shared via our newsletter
at https://podomy.com.

  Commit message format - /COMMITS
  Contributor license agreement - /CLA
  Contributing - /CONTRIBUTING

License

Weld is distributed under the GNU Affero General Public License v3.0 or
later. See /LICENSE.
