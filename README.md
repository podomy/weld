<h1 align="center">Weld</h1>

<p align="center">
  <a href="https://github.com/podomy/weld/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/podomy/weld/ci.yml?label=linux" alt="Linux"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/podomy/weld" alt="License"></a>
</p>

Weld is a ROS 2 compatible interface in Go. It talks to DDS and Zenoh
so Go programs can join a ROS 2 system as first-class nodes, covering
what ROS covers: topics, services, actions, and parameters.

### Documentation

- [Weld design](./docs/weld-design.md): architecture, invariants, and build order.
- [ROS reference](./docs/ros/): nineteen chapters mapping the ROS 2 stack Weld must stay compatible with, from wire protocols to Nav2, plus a failure analysis of 6000+ upstream issues that the design eliminates. Start here to understand what Weld mirrors and what it replaces.

### Contributing

Discuss your change with the engineering team at [contact@podomy.com](mailto:contact@podomy.com) before opening a PR in order not to waste anybody's effort or time.

Announcements and engineering updates on distributed systems, consensus algorithms, and robotic fleet coordination are shared via our newsletter at [podomy.com](https://podomy.com).

- [Commit message format](./COMMITS)
- [Contributor license agreement](./CLA)
- [Contributing](./CONTRIBUTING)

### License

Weld is distributed under the GNU Affero General Public License v3.0 or
later. See [LICENSE](./LICENSE).
