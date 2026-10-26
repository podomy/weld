# Testbed

Reproducible ROS peers for the conformance gates: one container per supported distro, each able to speak every RMW Weld must interoperate with.

## Images

- `weld-humble`: `ros:humble-ros-base` plus `rmw-cyclonedds-cpp` plus `rmw-zenoh-cpp`
- `weld-jazzy`: `ros:jazzy-ros-base` plus `rmw-cyclonedds-cpp` plus `rmw-zenoh-cpp`
- `weld-rolling`: `ros:rolling-ros-base` plus `rmw-cyclonedds-cpp` plus `rmw-zenoh-cpp`

FastDDS ships in each base image. Dockerfiles pin nothing beyond the ROS apt snapshots.

## Runtime

Each container idles on `sleep infinity` with host networking, so DDS multicast discovery works without extra configuration. Give each test run its own domain id to keep runs isolated.

```bash
cd testbed
docker compose build
docker compose up -d
docker compose ps
```

```bash
docker exec weld-jazzy bash -c "source /opt/ros/jazzy/setup.bash && ROS_DOMAIN_ID=42 RMW_IMPLEMENTATION=rmw_cyclonedds_cpp ros2 run demo_nodes_cpp talker"
docker exec weld-jazzy bash -c "source /opt/ros/jazzy/setup.bash && ROS_DOMAIN_ID=42 RMW_IMPLEMENTATION=rmw_cyclonedds_cpp ros2 topic list"
```

## Conflict

A stale container name fails `up` with `Conflict. The container name "/weld-rolling" is already in use`. Remove it first:

```bash
docker rm -f weld-rolling weld-humble weld-jazzy
docker compose up -d
```

## Teardown

```bash
docker compose down
```

`docker compose down --rmi all` frees about 8 GB.

## Current status

Testbed is the ROS peer half only. Weld has no `cmd/weld` publisher or subscriber yet and the DDS sidecar returns `ErrSidecarNotImplemented`, so no wire interop runs at this stage. Pure-Go gates (`go test ./...`) are green. First interop needs the sidecar client plus first-contact round trip on `/chatter`.
