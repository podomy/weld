FROM ros:humble-ros-base

# CycloneDDS RMW is the reference peer for Weld interop gates.
# FastDDS ships in the base image. Zenoh is released for Humble as well.
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ros-humble-rmw-cyclonedds-cpp ros-humble-rmw-zenoh-cpp \
    && rm -rf /var/lib/apt/lists/*

CMD ["sleep", "infinity"]
