<h1 align="center">Weld</h1>

<p align="center">
  <a href="https://github.com/podomy/weld/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/podomy/weld/ci.yml?label=linux" alt="Linux"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/podomy/weld" alt="License"></a>
</p>

Weld is robotics middleware written in Go. One unified core moves topics, services, actions, and parameters across DDS, Zenoh, and MQTT transports, with structured delivery, copy once handling, and a leaderless data plane that stays correct under partition.

Weld speaks ROS 2 on the wire, so Go programs join ROS 2 systems as first-class nodes, verified by mixed fleet behavior.

### Documentation

- [Weld design](./docs/design.md): architecture, invariants, and build order.
- [Weld invariants](./docs/invariants.md): 250+ testable promises the system holds, drawn from 6000+ upstream ROS issues.

### Contributing

Discuss your change with the engineering team at [contact@podomy.com](mailto:contact@podomy.com) before opening a PR in order not to waste anybody's effort or time.

Announcements and engineering updates on distributed systems, consensus algorithms, and robotic fleet coordination are shared via our newsletter at [podomy.com](https://podomy.com).

- [Commit message format](./COMMITS)
- [Contributor license agreement](./CLA)
- [Contributing](./CONTRIBUTING)

### License

Weld is distributed under the GNU Affero General Public License v3.0 or
later. See [LICENSE](./LICENSE).
