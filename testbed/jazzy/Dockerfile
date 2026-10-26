FROM ros:jazzy-ros-base

# CycloneDDS RMW is the reference peer for Weld interop gates.
# FastDDS ships in the base image. Zenoh is available from Jazzy on.
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ros-jazzy-rmw-cyclonedds-cpp ros-jazzy-rmw-zenoh-cpp \
    && rm -rf /var/lib/apt/lists/*

CMD ["sleep", "infinity"]
