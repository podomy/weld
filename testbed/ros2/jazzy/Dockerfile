FROM golang:1.26-bookworm AS gobuilder

# Go FFI shared library (module github.com/podomy/weld, stdlib only).
# No go.sum exists in the repo because the module has no external deps.
WORKDIR /src
COPY go.mod ./
COPY internal/ ./internal/
ENV CGO_ENABLED=1
RUN go build -buildmode=c-shared -o /out/libweld_ffi.so ./internal/rmwbridge/weldffi

FROM ros:jazzy-ros-base

# CycloneDDS RMW is the reference peer for Weld interop gates.
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq ros-jazzy-rmw-cyclonedds-cpp \
    && rm -rf /var/lib/apt/lists/*

# Go FFI shared library from the builder stage.
COPY --from=gobuilder /out/libweld_ffi.so /opt/ros/jazzy/lib/libweld_ffi.so

# rmw_weld shim sources (build context is the repo root).
COPY rmw_weld/src/rmw_weld.c /tmp/rmw_weld_src/rmw_weld.c
COPY rmw_weld/src/weld_cdr.c /tmp/rmw_weld_src/weld_cdr.c
COPY rmw_weld/src/weld_cdr.h /tmp/rmw_weld_src/weld_cdr.h

# Build plus install the shim so RMW_IMPLEMENTATION=rmw_weld resolves.
# Links the Go FFI with rpath $ORIGIN so librmw_weld.so finds
# libweld_ffi.so in the same lib dir with no extra env.
RUN . /opt/ros/jazzy/setup.sh \
    && gcc -std=c17 -fPIC -shared -Wall -Wextra -Wpedantic \
        -o /opt/ros/jazzy/lib/librmw_weld.so \
        /tmp/rmw_weld_src/rmw_weld.c /tmp/rmw_weld_src/weld_cdr.c \
        -I/tmp/rmw_weld_src -I/opt/ros/jazzy/include \
        $(for d in /opt/ros/jazzy/include/*/; do case "$d" in */dds/|*/idl/) continue;; *) printf -- "-I%s " "$d";; esac; done) \
        -L/opt/ros/jazzy/lib -lweld_ffi -lrmw -lrcl -lrcutils \
        -lrosidl_runtime_c -lrosidl_typesupport_c \
        -lrosidl_typesupport_introspection_c \
        -Wl,-rpath,'$ORIGIN' \
        -pthread \
    && mkdir -p /opt/ros/jazzy/share/ament_index/resource_index/rmw_implementation \
    && touch /opt/ros/jazzy/share/ament_index/resource_index/rmw_implementation/rmw_weld

CMD ["sleep", "infinity"]
