<h1 align="center">Weld</h1>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-BUSL--1.1-blue" alt="License: BUSL-1.1"></a>
</p>

Up to 68% lower latency than ROS 2 on the same host, with zero loss where ROS drops.

Weld is Go robotics middleware, wire-compatible with ROS 2, for fleets that must coordinate where links die. One leaderless data plane carries topics, services, actions, and parameters; version one ships the CycloneDDS backend.

Proven by a 110-row benchmark matrix, green end to end: lower p50 latency at every payload size, zero loss to 100 kHz where ROS first drops at 10 kHz.

<p align="center">
  <img src="testbed/bench/latency-vs-throughput.svg" alt="Latency vs throughput">
</p>

Full docs live in [docs/](/docs).

## Protocol support

| Transport | Spec | Status |
|-----------|------|--------|
| DDS | DDSI-RTPS 2.5, DDS 1.4, DDS-XTypes 1.4, DDS-SECURITY 1.2 | Fully supported |
| MQTT | MQTT v5, MQTT v3.1.1 | Fully supported |

Weld can bridge topics and services across transports on request, so DDS data can go out as MQTT and back.

## ROS 2

Weld runs ROS 2 nodes through `rmw_weld` (`RMW_IMPLEMENTATION=rmw_weld`): init, node, pub, sub, QoS, services, guard conditions, and waitsets are live against the Weld backend. Loans are owned-heap with tracking, not zero-copy shared memory. The following are explicitly out, with reasons: network flow endpoints (per-endpoint socket data not exposed); event types other than message-lost (no DDS source wired); publisher GID and sequence numbers in take info (takes carry no writer identity, sequences untracked); remote endpoint QoS and node names (unavailable from SEDP); N servers on one service name (requests race, use one server with N clients).

## Commercial Licensing

Commercial licenses are available. See [podomy.com/projects](https://podomy.com/projects/) for pricing, or contact [contact@podomy.com](mailto:contact@podomy.com).

## Contributing

Weld takes no pull requests or GitHub Issues. Propose first, patch second, both by email to [contact@podomy.com](mailto:contact@podomy.com): an RFC describing the problem, then a `git format-patch` series.

## License

Business Source License 1.1, with Change License GNU Affero General Public License v3.0 or later. See [LICENSE](/LICENSE). Prior releases remain AGPLv3-or-later, see [LICENSE.AGPL](/LICENSE.AGPL). Production use requires a commercial license. See [podomy.com/projects](https://podomy.com/projects/) or contact [contact@podomy.com](mailto:contact@podomy.com).
