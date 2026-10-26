FROM ros:rolling-ros-base

# CycloneDDS RMW is the reference peer for Weld interop gates.
# FastDDS ships in the base image.
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ros-rolling-rmw-cyclonedds-cpp ros-rolling-rmw-zenoh-cpp \
    && rm -rf /var/lib/apt/lists/*

CMD ["sleep", "infinity"]
