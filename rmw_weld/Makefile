# SPDX-License-Identifier: Apache-2.0
# rmw_weld Makefile: plain C build, no cmake.
#
# Build (needs a sourced ROS distro for rmw headers):
#   make -C rmw_weld
# Install (marker plus shared lib, no colcon):
#   make -C rmw_weld install PREFIX=/opt/weld
# Run with:
#   RMW_IMPLEMENTATION=rmw_weld LD_LIBRARY_PATH=/opt/weld/lib \
#     AMENT_PREFIX_PATH=/opt/weld:$AMENT_PREFIX_PATH ros2 run <pkg> <node>
#
# Humble needs C17 (Ubuntu 22.04 GCC 11); C23 stays out so all three
# distros build with one standard.

CC ?= cc
CSTD ?= c17
WARN ?= -Wall -Wextra -Wpedantic
# Every shim translation unit plus the private header they share.
# weld_private.h travels with the sources; there is no rmw_weld.c left.
SHIM_TUS ?= common.c init.c node.c publish.c subscription.c loan.c \
  service.c waitset.c graph.c event.c qos.c cft_dynamic.c
SHIM_HDR ?= weld_private.h
SHIM_SRCS ?= $(addprefix src/,$(SHIM_TUS)) src/weld_cdr.c
SHIM_FILES ?= $(SHIM_TUS) $(SHIM_HDR) weld_cdr.c weld_cdr.h
LIB ?= librmw_weld.so
PREFIX ?= /opt/weld

# Container build settings for the Jazzy peer.
#
# The testbed compose file bind mounts only ./logs, so the source is
# copied into the container before the gcc build. Install targets the
# stock Jazzy lib dir plus the ament index marker so plain ros2 argv
# with RMW_IMPLEMENTATION=rmw_weld picks the shim up with no extra env.
CONTAINER ?= ros-jazzy
JAZZY_SRC_DIR ?= /tmp/rmw_weld_src
JAZZY_SO ?= /tmp/librmw_weld.so
JAZZY_LIBDIR ?= /opt/ros/jazzy/lib
JAZZY_INDEX ?= /opt/ros/jazzy/share/ament_index/resource_index/rmw_implementation
SMOKE_DOMAIN ?= 59
SMOKE_TOPIC ?= /weld_rmw_smoke
TEST_DRIVER ?= linktest.c

# pkg-config flags stay best effort: empty outside a sourced ROS env,
# and the compile still fails loudly on the missing rmw/rmw.h include.
CFLAGS ?= -std=$(CSTD) $(WARN) -fPIC \
  $(shell pkg-config --cflags rmw rcl rosidl_runtime_c rcutils 2>/dev/null)
LDLIBS ?= $(shell pkg-config --libs rmw rcl rosidl_runtime_c rcutils 2>/dev/null)

.PHONY: all check install clean build-in-jazzy install-in-jazzy smoke-in-jazzy linktest-in-jazzy go-ffi-shared

all: $(LIB)

$(LIB): $(SHIM_SRCS) src/$(SHIM_HDR)
	$(CC) $(CFLAGS) -shared -o $@ $(SHIM_SRCS) $(LDLIBS)

check:
	$(CC) -std=$(CSTD) $(WARN) -fsyntax-only $(SHIM_SRCS) $(shell pkg-config --cflags rmw rcl rosidl_runtime_c rosidl_typesupport_c rosidl_typesupport_introspection_c rcutils 2>/dev/null)

install: all
	mkdir -p $(DESTDIR)$(PREFIX)/lib $(DESTDIR)$(PREFIX)/share/ament_index/resource_index/rmw_implementation
	cp $(LIB) $(DESTDIR)$(PREFIX)/lib/
	touch $(DESTDIR)$(PREFIX)/share/ament_index/resource_index/rmw_implementation/rmw_weld

# build-in-jazzy compiles the shim inside the running Jazzy peer.
#
# It copies every TU plus both headers into the container, then runs gcc
# with the same header and library set the linktest-in-jazzy probe uses:
# the shim needs weld_cdr.c plus rosidl_typesupport introspection libs.
# Output lands at JAZZY_SO inside the container.
build-in-jazzy: go-ffi-shared
	docker exec $(CONTAINER) mkdir -p $(JAZZY_SRC_DIR)
	for f in $(SHIM_FILES); do docker cp $(CURDIR)/src/$$f $(CONTAINER):$(JAZZY_SRC_DIR)/$$f || exit 1; done
	docker cp $(FFI_REAL_SO) $(CONTAINER):$(JAZZY_SRC_DIR)/libweld_ffi.so
	docker cp $(FFI_REAL_HDR) $(CONTAINER):$(JAZZY_SRC_DIR)/libweld_ffi.h
	docker exec $(CONTAINER) bash -lc 'source /opt/ros/jazzy/setup.bash && gcc -std=$(CSTD) $(WARN) -fPIC -shared -o $(JAZZY_SO) $(addprefix $(JAZZY_SRC_DIR)/,$(SHIM_TUS)) $(JAZZY_SRC_DIR)/weld_cdr.c -I$(JAZZY_SRC_DIR) -I/opt/ros/jazzy/include $$(for d in /opt/ros/jazzy/include/*/; do case "$$d" in */dds/|*/idl/) continue;; *) printf -- "-I%s " "$$d";; esac; done) $$(pkg-config --cflags rmw rcl rcutils rosidl_runtime_c rosidl_typesupport_c rosidl_typesupport_introspection_c 2>/dev/null) -L$(JAZZY_SRC_DIR) -L/opt/ros/jazzy/lib -lweld_ffi -lrmw -lrcl -lrcutils -lrosidl_runtime_c -lrosidl_typesupport_c -lrosidl_typesupport_introspection_c -Wl,-rpath,$(JAZZY_LIBDIR):$(JAZZY_SRC_DIR) -pthread'

# install-in-jazzy publishes the container build to a ROS-visible path.
#
# It copies JAZZY_SO plus the Go FFI shared library into the stock
# Jazzy lib dir (the rpath baked at link time already points there) and
# records the ament index marker, so RMW_IMPLEMENTATION=rmw_weld resolves
# with the stock Jazzy env and no extra prefix.
install-in-jazzy: build-in-jazzy
	docker exec $(CONTAINER) bash -lc "source /opt/ros/jazzy/setup.bash && cp $(JAZZY_SO) $(JAZZY_LIBDIR)/librmw_weld.so && cp $(JAZZY_SRC_DIR)/libweld_ffi.so $(JAZZY_LIBDIR)/libweld_ffi.so && mkdir -p $(JAZZY_INDEX) && touch $(JAZZY_INDEX)/rmw_weld"

# smoke-in-jazzy probes the installed shim with a tiny pub attempt.
#
# It expects the publish to succeed with a plain zero exit: the shim is
# a working RMW now, not a refusal stub. -w 0 skips the wait for a
# matching subscription (stock ros2 topic pub --once waits for one and
# would time out with no listener, even on Cyclone). A silent pass with
# no output fails, a signal exit (crash) fails, and a hang past timeout
# fails.
smoke-in-jazzy: install-in-jazzy
	docker exec $(CONTAINER) bash -lc 'source /opt/ros/jazzy/setup.bash && ROS_DOMAIN_ID=$(SMOKE_DOMAIN) RMW_IMPLEMENTATION=rmw_weld LD_LIBRARY_PATH=$(JAZZY_LIBDIR):$$LD_LIBRARY_PATH timeout 30 ros2 topic pub -w 0 $(SMOKE_TOPIC) std_msgs/msg/String "{data: smoke}" --once > /tmp/rmw_weld_smoke.log 2>&1; rc=$$?; cat /tmp/rmw_weld_smoke.log; if [ $$rc -eq 124 ]; then echo "smoke: rmw_weld probe timed out, failing"; exit 1; fi; if [ $$rc -ge 128 ]; then echo "smoke: rmw_weld crashed with exit $$rc, failing"; exit 1; fi; if [ $$rc -ne 0 ]; then echo "smoke: rmw_weld pub exited non-zero, failing"; exit 1; fi; echo "smoke: rmw_weld pub ok"'

FFI_REAL_DIR ?= /tmp/weld_ffi_real
FFI_REAL_SO ?= $(FFI_REAL_DIR)/libweld_ffi.so
FFI_REAL_HDR ?= $(FFI_REAL_DIR)/libweld_ffi.h
LINKTEST_SRC ?= linktest.c
JAZZY_LINKTEST_BIN ?= /tmp/weld_linktest

# linktest-in-jazzy links the shim against the real Go FFI inside ros-jazzy.
#
# It copies libweld_ffi.so plus libweld_ffi.h from the host c-shared
# build (see go-ffi-shared) plus linktest.c plus the shim TUs plus weld_cdr.c
# into the Jazzy peer, links with gcc -std=c17 against the ROS libs plus
# -lweld_ffi with rpath and LD_LIBRARY_PATH covering both the staged dir
# and the stock ROS lib dir, and runs the probe under timeout 60.
# It expects the real version plus identifier plus PASS lines for the
# RMW chain. A missing .so, crash, hang, or nonzero exit fails. No stubs.
linktest-in-jazzy: go-ffi-shared
	docker exec $(CONTAINER) mkdir -p $(JAZZY_SRC_DIR)
	docker cp $(CURDIR)/$(LINKTEST_SRC) $(CONTAINER):$(JAZZY_SRC_DIR)/linktest.c
	for f in $(SHIM_FILES); do docker cp $(CURDIR)/src/$$f $(CONTAINER):$(JAZZY_SRC_DIR)/$$f || exit 1; done
	docker cp $(FFI_REAL_SO) $(CONTAINER):$(JAZZY_SRC_DIR)/libweld_ffi.so
	docker cp $(FFI_REAL_HDR) $(CONTAINER):$(JAZZY_SRC_DIR)/libweld_ffi.h
	docker exec $(CONTAINER) bash -lc 'source /opt/ros/jazzy/setup.bash && echo "linktest: linking against REAL Go shared lib" && gcc -std=$(CSTD) $(WARN) -o $(JAZZY_LINKTEST_BIN) $(JAZZY_SRC_DIR)/linktest.c $(addprefix $(JAZZY_SRC_DIR)/,$(SHIM_TUS)) $(JAZZY_SRC_DIR)/weld_cdr.c -I$(JAZZY_SRC_DIR) $$(for d in /opt/ros/jazzy/include/*; do printf -- "-I%s " "$$d"; done) -L$(JAZZY_SRC_DIR) -L/opt/ros/jazzy/lib -lweld_ffi -lrmw_implementation -lrmw -lrcl -lrcutils -lrosidl_runtime_c -lrosidl_typesupport_c -lrosidl_typesupport_introspection_c -lstd_msgs__rosidl_generator_c -lstd_msgs__rosidl_typesupport_c -lstd_msgs__rosidl_typesupport_introspection_c -pthread -Wl,-rpath,$(JAZZY_SRC_DIR):/opt/ros/jazzy/lib && LD_LIBRARY_PATH=$(JAZZY_SRC_DIR):/opt/ros/jazzy/lib:$$LD_LIBRARY_PATH timeout 60 $(JAZZY_LINKTEST_BIN) > /tmp/weld_linktest.log 2>&1; rc=$$?; cat /tmp/weld_linktest.log; if [ $$rc -eq 124 ]; then echo "linktest: probe timed out, failing"; exit 1; fi; if [ $$rc -ge 128 ]; then echo "linktest: probe crashed with exit $$rc, failing"; exit 1; fi; if [ $$rc -ne 0 ]; then echo "linktest: probe exited $$rc, failing"; exit 1; fi; grep -E "^version=.+" /tmp/weld_linktest.log && grep -E "^identifier=rmw_weld$$" /tmp/weld_linktest.log && grep -E "^ffi=REAL$$" /tmp/weld_linktest.log && grep -E "^PASS publish$$" /tmp/weld_linktest.log && grep -E "^PASS take payload=hello$$" /tmp/weld_linktest.log && grep -E "^PASS qos compatible OK$$" /tmp/weld_linktest.log && grep -E "^PASS weld_linktest_real$$" /tmp/weld_linktest.log'

# go-ffi-shared builds the real Go FFI shared library on the host.
#
# internal/rmwbridge/weldffi is package main, so c-shared works and emits
# libweld_ffi.so plus libweld_ffi.h into FFI_REAL_DIR for linktest-in-jazzy.
go-ffi-shared:
	mkdir -p $(FFI_REAL_DIR)
	cd $(CURDIR)/.. && go build -buildmode=c-shared -o $(FFI_REAL_SO) ./internal/rmwbridge/weldffi
	ls -l $(FFI_REAL_SO) $(FFI_REAL_HDR)

clean:
	rm -f $(LIB)

INTEROP_SRC ?= interop_test.c
JAZZY_INTEROP_BIN ?= /tmp/weld_interop
INTEROP_DOMAIN ?= 59
INTEROP_TOPIC ?= /weld_interop

.PHONY: interop-in-jazzy

# interop-in-jazzy proves live RTPS exchange with a real ROS peer.
#
# It links interop_test.c plus the shim TUs plus weld_cdr.c against the real
# Go shared lib inside ros-jazzy, then runs the probe under timeout 60.
# Direction one checks rmw pub reaches ros echo on INTEROP_TOPIC in
# INTEROP_DOMAIN. Direction two checks ros pub reaches rmw sub. Any
# missing .so, crash, hang, or nonzero exit fails with the probe log.
interop-in-jazzy: go-ffi-shared
	docker exec $(CONTAINER) mkdir -p $(JAZZY_SRC_DIR)
	docker cp $(CURDIR)/$(INTEROP_SRC) $(CONTAINER):$(JAZZY_SRC_DIR)/interop_test.c
	for f in $(SHIM_FILES); do docker cp $(CURDIR)/src/$$f $(CONTAINER):$(JAZZY_SRC_DIR)/$$f || exit 1; done
	docker cp $(FFI_REAL_SO) $(CONTAINER):$(JAZZY_SRC_DIR)/libweld_ffi.so
	docker cp $(FFI_REAL_HDR) $(CONTAINER):$(JAZZY_SRC_DIR)/libweld_ffi.h
	docker exec $(CONTAINER) bash -lc 'source /opt/ros/jazzy/setup.bash && echo "interop: linking against REAL Go shared lib" && gcc -std=$(CSTD) $(WARN) -o $(JAZZY_INTEROP_BIN) $(JAZZY_SRC_DIR)/interop_test.c $(addprefix $(JAZZY_SRC_DIR)/,$(SHIM_TUS)) $(JAZZY_SRC_DIR)/weld_cdr.c -I$(JAZZY_SRC_DIR) $$(for d in /opt/ros/jazzy/include/*; do printf -- "-I%s " "$$d"; done) -L$(JAZZY_SRC_DIR) -L/opt/ros/jazzy/lib -lweld_ffi -lrmw_implementation -lrmw -lrcl -lrcutils -lrosidl_runtime_c -lrosidl_typesupport_c -lrosidl_typesupport_introspection_c -lstd_msgs__rosidl_generator_c -lstd_msgs__rosidl_typesupport_c -lstd_msgs__rosidl_typesupport_introspection_c -pthread -Wl,-rpath,$(JAZZY_SRC_DIR):/opt/ros/jazzy/lib && LD_LIBRARY_PATH=$(JAZZY_SRC_DIR):/opt/ros/jazzy/lib:$$LD_LIBRARY_PATH timeout 60 $(JAZZY_INTEROP_BIN) > /tmp/weld_interop.log 2>&1; rc=$$?; cat /tmp/weld_interop.log; if [ $$rc -eq 124 ]; then echo "interop: probe timed out, failing"; exit 1; fi; if [ $$rc -ge 128 ]; then echo "interop: probe crashed with exit $$rc, failing"; exit 1; fi; if [ $$rc -ne 0 ]; then echo "interop: probe exited $$rc, failing"; exit 1; fi; grep -E "^PASS weld_to_ros$$" /tmp/weld_interop.log && grep -E "^PASS ros_to_weld payload=hello$$" /tmp/weld_interop.log && grep -E "^PASS weld_interop_real$$" /tmp/weld_interop.log'

SVCWAIT_SRC ?= svcwait_test.c
JAZZY_SVCWAIT_BIN ?= /tmp/weld_svcwait

.PHONY: svcwait-in-jazzy

# svcwait-in-jazzy proves RMW service/client plus guard/waitset roundtrips
# through the real Go FFI inside ros-jazzy.
#
# It copies libweld_ffi.so plus libweld_ffi.h from the host c-shared
# build (see go-ffi-shared) plus svcwait_test.c plus the shim TUs plus
# weld_cdr.c into the Jazzy peer, links with gcc -std=c17 against the
# ROS libs plus -lweld_ffi with rpath and LD_LIBRARY_PATH covering both
# the staged dir and the stock ROS lib dir, and runs the probe under
# timeout 60. The probe uses std_srvs Empty introspection on one service
# name: a helper thread polls server take_request plus send_response
# while the main thread blocks in client take_response, because the call
# only reaches the Go service once take_response runs, so a single
# thread cannot serve its own request. Guard checks stay single
# threaded: trigger plus rmw_wait with 5s timeout expects OK, then wait
# with 1s timeout and no trigger expects TIMEOUT. It expects the real
# version plus identifier plus PASS lines for the RMW chain. A missing
# .so, crash, hang, or nonzero exit fails. No stubs.
svcwait-in-jazzy: go-ffi-shared
	docker exec $(CONTAINER) mkdir -p $(JAZZY_SRC_DIR)
	docker cp $(CURDIR)/$(SVCWAIT_SRC) $(CONTAINER):$(JAZZY_SRC_DIR)/svcwait_test.c
	for f in $(SHIM_FILES); do docker cp $(CURDIR)/src/$$f $(CONTAINER):$(JAZZY_SRC_DIR)/$$f || exit 1; done
	docker cp $(FFI_REAL_SO) $(CONTAINER):$(JAZZY_SRC_DIR)/libweld_ffi.so
	docker cp $(FFI_REAL_HDR) $(CONTAINER):$(JAZZY_SRC_DIR)/libweld_ffi.h
	docker exec $(CONTAINER) bash -lc 'source /opt/ros/jazzy/setup.bash && echo "svcwait: linking against REAL Go shared lib" && gcc -std=$(CSTD) $(WARN) -o $(JAZZY_SVCWAIT_BIN) $(JAZZY_SRC_DIR)/svcwait_test.c $(addprefix $(JAZZY_SRC_DIR)/,$(SHIM_TUS)) $(JAZZY_SRC_DIR)/weld_cdr.c -I$(JAZZY_SRC_DIR) $$(for d in /opt/ros/jazzy/include/*; do printf -- "-I%s " "$$d"; done) -L$(JAZZY_SRC_DIR) -L/opt/ros/jazzy/lib -lweld_ffi -lrmw_implementation -lrmw -lrcl -lrcutils -lrosidl_runtime_c -lrosidl_typesupport_c -lrosidl_typesupport_introspection_c -lstd_msgs__rosidl_generator_c -lstd_msgs__rosidl_typesupport_c -lstd_msgs__rosidl_typesupport_introspection_c -lstd_srvs__rosidl_generator_c -lstd_srvs__rosidl_typesupport_c -lstd_srvs__rosidl_typesupport_introspection_c -pthread -Wl,-rpath,$(JAZZY_SRC_DIR):/opt/ros/jazzy/lib && LD_LIBRARY_PATH=$(JAZZY_SRC_DIR):/opt/ros/jazzy/lib:$$LD_LIBRARY_PATH timeout 60 $(JAZZY_SVCWAIT_BIN) > /tmp/weld_svcwait.log 2>&1; rc=$$?; cat /tmp/weld_svcwait.log; if [ $$rc -eq 124 ]; then echo "svcwait: probe timed out, failing"; exit 1; fi; if [ $$rc -ge 128 ]; then echo "svcwait: probe crashed with exit $$rc, failing"; exit 1; fi; if [ $$rc -ne 0 ]; then echo "svcwait: probe exited $$rc, failing"; exit 1; fi; grep -E "^version=.+" /tmp/weld_svcwait.log && grep -E "^identifier=rmw_weld$$" /tmp/weld_svcwait.log && grep -E "^ffi=REAL$$" /tmp/weld_svcwait.log && grep -E "^PASS svc payload match seq=" /tmp/weld_svcwait.log && grep -E "^PASS wait triggered OK$$" /tmp/weld_svcwait.log && grep -E "^PASS wait timeout$$" /tmp/weld_svcwait.log && grep -E "^PASS weld_svcwait_real$$" /tmp/weld_svcwait.log'
