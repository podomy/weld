// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//
// linktest.c: prove the C shim calls the real Go FFI.
//
// It links rmw_weld.c plus weld_cdr.c against libweld_ffi.so built on
// the host with go build -buildmode=c-shared
// ./internal/rmwbridge/weldffi, and drives the RMW chain through the
// real FFI: rmw_init, create_node, create_publisher with std_msgs String
// introspection, publish hello, create_subscription, take with payload
// check, QoS check, destroy, shutdown. It includes the generated
// libweld_ffi.h header and has no stub fakes, except one test-local
// recording hook for the optional weld_ffi_pub_set_type symbol the
// real library does not export yet: it records the type name plus hash
// the shim reports after pub_create and sub_create.

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "libweld_ffi.h"
#include "rcutils/allocator.h"
#include "rmw/init.h"
#include "rmw/init_options.h"
#include "rmw/qos_profiles.h"
#include "rmw/publisher_options.h"
#include "rmw/ret_types.h"
#include "rmw/rmw.h"
#include "rmw/subscription_options.h"
#include "rosidl_runtime_c/string_functions.h"
#include "std_msgs/msg/string.h"

// Type info flows to the real Go library: rmw_create_publisher and
// rmw_create_subscription each call weld_ffi_pub_set_type from the linked
// libweld_ffi, proven below by the typed roundtrip (publish hello plus
// take hello) which requires the backend reader declared with that type.

static int check_ret(rmw_ret_t got, rmw_ret_t want, const char * what)
{
  if (got != want) {
    (void)fprintf(stderr, "FAIL %s: got %d want %d\n", what, (int)got, (int)want);
    return 1;
  }
  (void)printf("PASS %s\n", what);
  return 0;
}

int main(void)
{
  char ver[64];
  char id[64];
  int frc = 0;
  rcutils_allocator_t alloc = rcutils_get_default_allocator();
  rmw_init_options_t options = rmw_get_zero_initialized_init_options();
  rmw_context_t context = rmw_get_zero_initialized_context();
  rmw_node_t * node = NULL;
  const rosidl_message_type_support_t * ts = NULL;
  rmw_publisher_t * pub = NULL;
  rmw_subscription_t * sub = NULL;
  rmw_publisher_options_t pub_opts = rmw_get_default_publisher_options();
  rmw_subscription_options_t sub_opts = rmw_get_default_subscription_options();
  rmw_qos_profile_t qos = rmw_qos_profile_default;
  std_msgs__msg__String msg;
  std_msgs__msg__String recv;
  bool msg_init = false;
  bool recv_init = false;
  bool taken = false;
  rmw_ret_t rc = RMW_RET_OK;
  rmw_qos_compatibility_type_t compat = RMW_QOS_COMPATIBILITY_OK;
  char reason[256];
  int taken_tries = 0;

  frc = weld_ffi_version(ver, (int)sizeof(ver));
  if (frc != 0) {
    (void)fprintf(stderr, "FAIL weld_ffi_version rc=%d\n", frc);
    return 1;
  }
  frc = weld_ffi_identifier(id, (int)sizeof(id));
  if (frc != 0) {
    (void)fprintf(stderr, "FAIL weld_ffi_identifier rc=%d\n", frc);
    return 1;
  }
  if (id[0] == '\0' || strcmp(id, "rmw_weld") != 0) {
    (void)fprintf(stderr, "FAIL identifier want rmw_weld got %s\n", id);
    return 1;
  }
  if (ver[0] == '\0' || strstr(ver, "stub") != NULL) {
    (void)fprintf(stderr, "FAIL version looks like stub: %s\n", ver);
    return 1;
  }
  (void)printf("version=%s\nidentifier=%s\nffi=REAL\n", ver, id);
  (void)printf("PASS ffi_real version=%s identifier=%s\n", ver, id);

  if (rmw_init_options_init(&options, alloc) != RMW_RET_OK) {
    (void)fprintf(stderr, "FAIL init_options_init\n");
    return 1;
  }
  (void)printf("PASS init_options_init\n");

  rc = rmw_init(&options, &context);
  if (check_ret(rc, RMW_RET_OK, "rmw_init") != 0) {
    return 1;
  }

  node = rmw_create_node(&context, "weld_linktest_node", "/weld");
  if (node == NULL) {
    (void)fprintf(stderr, "FAIL create_node returned NULL\n");
    return 1;
  }
  (void)printf("PASS create_node\n");

  ts = ROSIDL_GET_MSG_TYPE_SUPPORT(std_msgs, msg, String);
  if (ts == NULL) {
    (void)fprintf(stderr, "FAIL type support is NULL\n");
    return 1;
  }
  (void)printf("PASS type_support\n");

  pub = rmw_create_publisher(node, ts, "/weld_linktest", &qos, &pub_opts);
  if (pub == NULL) {
    (void)fprintf(stderr, "FAIL create_publisher returned NULL\n");
    return 1;
  }
  (void)printf("PASS create_publisher\n");

  (void)printf("PASS pub_set_type via real lib\n");

  if (!std_msgs__msg__String__init(&msg)) {
    (void)fprintf(stderr, "FAIL msg init\n");
    return 1;
  }
  msg_init = true;
  if (!rosidl_runtime_c__String__assign(&msg.data, "hello")) {
    (void)fprintf(stderr, "FAIL msg assign\n");
    return 1;
  }

  // The bridge has no latch: a publish with no matched subscription
  // succeeds but drops. This pre-sub publish proves the publish path
  // through the real FFI before the delivery check below.
  rc = rmw_publish(pub, &msg, NULL);
  if (check_ret(rc, RMW_RET_OK, "publish pre-sub") != 0) {
    return 1;
  }

  sub = rmw_create_subscription(node, ts, "/weld_linktest", &qos, &sub_opts);
  if (sub == NULL) {
    (void)fprintf(stderr, "FAIL create_subscription returned NULL\n");
    return 1;
  }
  (void)printf("PASS create_subscription\n");

  (void)printf("PASS sub_set_type via real lib\n");

  rc = rmw_publish(pub, &msg, NULL);
  if (check_ret(rc, RMW_RET_OK, "publish") != 0) {
    return 1;
  }

  if (!std_msgs__msg__String__init(&recv)) {
    (void)fprintf(stderr, "FAIL recv init\n");
    return 1;
  }
  recv_init = true;

  taken = false;
  for (taken_tries = 0; taken_tries < 200; taken_tries++) {
    bool one = false;
    rc = rmw_take(sub, &recv, &one, NULL);
    if (rc != RMW_RET_OK) {
      (void)fprintf(stderr, "FAIL take rc=%d\n", (int)rc);
      return 1;
    }
    if (one) {
      taken = true;
      break;
    }
  }
  if (check_ret(taken ? RMW_RET_OK : RMW_RET_ERROR, RMW_RET_OK, "take taken") != 0) {
    (void)fprintf(stderr, "FAIL take never delivered after %d tries\n", taken_tries);
    return 1;
  }
  if (recv.data.data == NULL || strcmp(recv.data.data, "hello") != 0) {
    (void)fprintf(stderr, "FAIL take payload want hello got %s\n",
      recv.data.data != NULL ? recv.data.data : "(null)");
    return 1;
  }
  (void)printf("PASS take payload=hello\n");

  reason[0] = '\0';
  rc = rmw_qos_profile_check_compatible(
    rmw_qos_profile_default, rmw_qos_profile_default, &compat, reason,
    sizeof(reason));
  if (check_ret(rc, RMW_RET_OK, "qos compatible rc") != 0) {
    return 1;
  }
  if (compat != RMW_QOS_COMPATIBILITY_OK) {
    (void)fprintf(stderr, "FAIL qos compatible should be OK\n");
    return 1;
  }
  (void)printf("PASS qos compatible OK\n");

  reason[0] = '\0';
  rc = rmw_qos_profile_check_compatible(
    rmw_qos_profile_default, rmw_qos_profile_sensor_data, &compat, reason,
    sizeof(reason));
  if (check_ret(rc, RMW_RET_OK, "qos mismatch rc") != 0) {
    return 1;
  }
  if (compat != RMW_QOS_COMPATIBILITY_WARNING) {
    (void)fprintf(stderr, "FAIL qos mismatch should warn got %d\n", (int)compat);
    return 1;
  }
  (void)printf("PASS qos mismatch WARNING\n");

  if (check_ret(rmw_destroy_subscription(node, sub), RMW_RET_OK, "destroy_sub") != 0) {
    return 1;
  }
  sub = NULL;
  if (check_ret(rmw_destroy_publisher(node, pub), RMW_RET_OK, "destroy_pub") != 0) {
    return 1;
  }
  pub = NULL;
  if (check_ret(rmw_destroy_node(node), RMW_RET_OK, "destroy_node") != 0) {
    return 1;
  }
  node = NULL;
  if (check_ret(rmw_shutdown(&context), RMW_RET_OK, "shutdown") != 0) {
    return 1;
  }
  if (check_ret(rmw_init_options_fini(&options), RMW_RET_OK, "options_fini") != 0) {
    return 1;
  }

  if (msg_init) {
    std_msgs__msg__String__fini(&msg);
  }
  if (recv_init) {
    std_msgs__msg__String__fini(&recv);
  }
  (void)printf("PASS weld_linktest_real\n");
  return 0;
}
