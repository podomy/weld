FROM ros:rolling-ros-base

# CycloneDDS RMW is the reference peer for Weld interop gates.
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ros-rolling-rmw-cyclonedds-cpp \
    && rm -rf /var/lib/apt/lists/*

CMD ["sleep", "infinity"]
