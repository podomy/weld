// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//
// interop_test.c: prove rmw_weld exchanges live traffic with a ROS peer.
//
// It links rmw_weld.c plus weld_cdr.c against the real libweld_ffi.so and
// drives two directions on one topic in domain 59. Direction one publishes
// hello through rmw_weld while a background ros2 topic echo writes to a
// file; the test polls the file for hello. Direction two creates an rmw
// subscription while a background ros2 topic pub emits hello; the test
// takes until hello arrives. ROS peers use rmw_cyclonedds_cpp because the
// Weld RTPS backend targets CycloneDDS discovery. No threads, only system
// plus files plus polling with bounded snprintf and explicit FAIL lines.

#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "libweld_ffi.h"
#include "rcutils/allocator.h"
#include "rmw/init.h"
#include "rmw/init_options.h"
#include "rmw/publisher_options.h"
#include "rmw/qos_profiles.h"
#include "rmw/ret_types.h"
#include "rmw/rmw.h"
#include "rmw/subscription_options.h"
#include "rosidl_runtime_c/string_functions.h"
#include "std_msgs/msg/string.h"

#define INTEROP_DOMAIN 59
#define INTEROP_TOPIC "/weld_interop"
#define INTEROP_NODE "weld_interop_node"
#define INTEROP_NS "/weld"
#define INTEROP_HELLO "hello"
#define INTEROP_ECHO_LOG "/tmp/weld_interop_echo.log"
#define INTEROP_ECHO_PID "/tmp/weld_interop_echo.pid"
#define INTEROP_PUB_LOG "/tmp/weld_interop_pub.log"
#define INTEROP_PUB_PID "/tmp/weld_interop_pub.pid"
#define INTEROP_TRIES 300
#define INTEROP_SLEEP_MS 100

static int check_ret(rmw_ret_t got, rmw_ret_t want, const char *what)
{
  if (got != want) {
    (void)fprintf(stderr, "FAIL %s: got %d want %d\n", what, (int)got, (int)want);
    return 1;
  }
  (void)printf("PASS %s\n", what);
  return 0;
}

static void sleep_ms(long ms)
{
  struct timespec ts;
  ts.tv_sec = ms / 1000;
  ts.tv_nsec = (ms % 1000) * 1000000L;
  (void)nanosleep(&ts, NULL);
}

static int file_contains(const char *path, const char *needle)
{
  FILE *fp = NULL;
  long size = 0;
  size_t got = 0;
  char *buf = NULL;
  int found = 0;
  if (path == NULL || needle == NULL) {
    return -1;
  }
  fp = fopen(path, "rb");
  if (fp == NULL) {
    return 0;
  }
  if (fseek(fp, 0, SEEK_END) != 0) {
    (void)fclose(fp);
    return -1;
  }
  size = ftell(fp);
  if (size < 0) {
    (void)fclose(fp);
    return -1;
  }
  if (size == 0) {
    (void)fclose(fp);
    return 0;
  }
  if (size > 262144) {
    size = 262144;
  }
  if (fseek(fp, 0, SEEK_SET) != 0) {
    (void)fclose(fp);
    return -1;
  }
  buf = (char *)malloc((size_t)size + 1);
  if (buf == NULL) {
    (void)fclose(fp);
    return -1;
  }
  got = fread(buf, 1, (size_t)size, fp);
  (void)fclose(fp);
  buf[got] = '\0';
  found = (strstr(buf, needle) != NULL) ? 1 : 0;
  free(buf);
  return found;
}

static int run_cmd(const char *cmd)
{
  int rc = 0;
  if (cmd == NULL) {
    return -1;
  }
  rc = system(cmd);
  return rc;
}

int main(void)
{
  char ver[64];
  char id[64];
  int frc = 0;
  int n = 0;
  char cmd[1024];
  rcutils_allocator_t alloc = rcutils_get_default_allocator();
  rmw_init_options_t options = rmw_get_zero_initialized_init_options();
  rmw_context_t context = rmw_get_zero_initialized_context();
  rmw_node_t *node = NULL;
  const rosidl_message_type_support_t *ts = NULL;
  rmw_publisher_t *pub = NULL;
  rmw_subscription_t *sub = NULL;
  rmw_publisher_options_t pub_opts = rmw_get_default_publisher_options();
  rmw_subscription_options_t sub_opts = rmw_get_default_subscription_options();
  rmw_qos_profile_t qos = rmw_qos_profile_default;
  std_msgs__msg__String msg;
  std_msgs__msg__String recv;
  bool msg_init = false;
  bool recv_init = false;
  bool weld_to_ros = false;
  bool ros_to_weld = false;
  rmw_ret_t rc = RMW_RET_OK;
  int i = 0;

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
  options.domain_id = (size_t)INTEROP_DOMAIN;
  (void)printf("PASS domain=%d\n", INTEROP_DOMAIN);

  rc = rmw_init(&options, &context);
  if (check_ret(rc, RMW_RET_OK, "rmw_init") != 0) {
    return 1;
  }

  node = rmw_create_node(&context, INTEROP_NODE, INTEROP_NS);
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

  pub = rmw_create_publisher(node, ts, INTEROP_TOPIC, &qos, &pub_opts);
  if (pub == NULL) {
    (void)fprintf(stderr, "FAIL create_publisher returned NULL\n");
    return 1;
  }
  (void)printf("PASS create_publisher\n");

  if (!std_msgs__msg__String__init(&msg)) {
    (void)fprintf(stderr, "FAIL msg init\n");
    return 1;
  }
  msg_init = true;
  if (!rosidl_runtime_c__String__assign(&msg.data, INTEROP_HELLO)) {
    (void)fprintf(stderr, "FAIL msg assign\n");
    return 1;
  }

  n = snprintf(cmd, sizeof(cmd), "rm -f %s %s %s %s",
    INTEROP_ECHO_LOG, INTEROP_ECHO_PID, INTEROP_PUB_LOG, INTEROP_PUB_PID);
  if (n < 0 || (size_t)n >= sizeof(cmd)) {
    (void)fprintf(stderr, "FAIL cleanup snprintf\n");
    return 1;
  }
  (void)run_cmd(cmd);

  n = snprintf(cmd, sizeof(cmd),
    "RMW_IMPLEMENTATION=rmw_cyclonedds_cpp ROS_DOMAIN_ID=%d ros2 topic echo %s std_msgs/msg/String > %s 2>&1 & echo $! > %s",
    INTEROP_DOMAIN, INTEROP_TOPIC, INTEROP_ECHO_LOG, INTEROP_ECHO_PID);
  if (n < 0 || (size_t)n >= sizeof(cmd)) {
    (void)fprintf(stderr, "FAIL echo spawn snprintf\n");
    return 1;
  }
  if (run_cmd(cmd) != 0) {
    (void)fprintf(stderr, "FAIL echo spawn system rc\n");
    return 1;
  }
  (void)printf("PASS echo_spawn\n");
  sleep_ms(2000);

  for (i = 0; i < INTEROP_TRIES; i++) {
    int hit = 0;
    rc = rmw_publish(pub, &msg, NULL);
    if (rc != RMW_RET_OK) {
      (void)fprintf(stderr, "FAIL publish rc=%d try=%d\n", (int)rc, i);
      n = snprintf(cmd, sizeof(cmd),
        "kill \"$(cat %s 2>/dev/null)\" 2>/dev/null; pkill -f \"ros2 topic echo.*%s\" 2>/dev/null; true",
        INTEROP_ECHO_PID, INTEROP_TOPIC);
      if (n > 0 && (size_t)n < sizeof(cmd)) {
        (void)run_cmd(cmd);
      }
      return 1;
    }
    sleep_ms(INTEROP_SLEEP_MS);
    hit = file_contains(INTEROP_ECHO_LOG, INTEROP_HELLO);
    if (hit < 0) {
      (void)fprintf(stderr, "FAIL echo log read try=%d\n", i);
      n = snprintf(cmd, sizeof(cmd),
        "kill \"$(cat %s 2>/dev/null)\" 2>/dev/null; pkill -f \"ros2 topic echo.*%s\" 2>/dev/null; true",
        INTEROP_ECHO_PID, INTEROP_TOPIC);
      if (n > 0 && (size_t)n < sizeof(cmd)) {
        (void)run_cmd(cmd);
      }
      return 1;
    }
    if (hit == 1) {
      weld_to_ros = true;
      break;
    }
  }
  if (!weld_to_ros) {
    (void)fprintf(stderr, "FAIL weld_to_ros: echo never saw hello after %d tries\n",
      INTEROP_TRIES);
    n = snprintf(cmd, sizeof(cmd),
      "kill \"$(cat %s 2>/dev/null)\" 2>/dev/null; pkill -f \"ros2 topic echo.*%s\" 2>/dev/null; true",
      INTEROP_ECHO_PID, INTEROP_TOPIC);
    if (n > 0 && (size_t)n < sizeof(cmd)) {
      (void)run_cmd(cmd);
    }
    return 1;
  }
  (void)printf("PASS weld_to_ros\n");

  n = snprintf(cmd, sizeof(cmd),
    "kill \"$(cat %s 2>/dev/null)\" 2>/dev/null; pkill -f \"ros2 topic echo.*%s\" 2>/dev/null; true",
    INTEROP_ECHO_PID, INTEROP_TOPIC);
  if (n < 0 || (size_t)n >= sizeof(cmd)) {
    (void)fprintf(stderr, "FAIL echo kill snprintf\n");
    return 1;
  }
  (void)run_cmd(cmd);
  sleep_ms(500);

  sub = rmw_create_subscription(node, ts, INTEROP_TOPIC, &qos, &sub_opts);
  if (sub == NULL) {
    (void)fprintf(stderr, "FAIL create_subscription returned NULL\n");
    return 1;
  }
  (void)printf("PASS create_subscription\n");

  n = snprintf(cmd, sizeof(cmd),
    "RMW_IMPLEMENTATION=rmw_cyclonedds_cpp ROS_DOMAIN_ID=%d ros2 topic pub %s std_msgs/msg/String \"data: %s\" --rate 10 > %s 2>&1 & echo $! > %s",
    INTEROP_DOMAIN, INTEROP_TOPIC, INTEROP_HELLO, INTEROP_PUB_LOG, INTEROP_PUB_PID);
  if (n < 0 || (size_t)n >= sizeof(cmd)) {
    (void)fprintf(stderr, "FAIL pub spawn snprintf\n");
    return 1;
  }
  if (run_cmd(cmd) != 0) {
    (void)fprintf(stderr, "FAIL pub spawn system rc\n");
    return 1;
  }
  (void)printf("PASS ros_pub_spawn\n");
  sleep_ms(2000);

  if (!std_msgs__msg__String__init(&recv)) {
    (void)fprintf(stderr, "FAIL recv init\n");
    return 1;
  }
  recv_init = true;

  for (i = 0; i < INTEROP_TRIES; i++) {
    bool taken = false;
    rc = rmw_take(sub, &recv, &taken, NULL);
    if (rc != RMW_RET_OK) {
      (void)fprintf(stderr, "FAIL take rc=%d try=%d\n", (int)rc, i);
      n = snprintf(cmd, sizeof(cmd),
        "kill \"$(cat %s 2>/dev/null)\" 2>/dev/null; pkill -f \"ros2 topic pub.*%s\" 2>/dev/null; true",
        INTEROP_PUB_PID, INTEROP_TOPIC);
      if (n > 0 && (size_t)n < sizeof(cmd)) {
        (void)run_cmd(cmd);
      }
      return 1;
    }
    if (taken) {
      if (recv.data.data != NULL && strcmp(recv.data.data, INTEROP_HELLO) == 0) {
        ros_to_weld = true;
        break;
      }
    }
    sleep_ms(INTEROP_SLEEP_MS);
  }
  if (!ros_to_weld) {
    (void)fprintf(stderr, "FAIL ros_to_weld: take never saw hello after %d tries\n",
      INTEROP_TRIES);
    n = snprintf(cmd, sizeof(cmd),
      "kill \"$(cat %s 2>/dev/null)\" 2>/dev/null; pkill -f \"ros2 topic pub.*%s\" 2>/dev/null; true",
      INTEROP_PUB_PID, INTEROP_TOPIC);
    if (n > 0 && (size_t)n < sizeof(cmd)) {
      (void)run_cmd(cmd);
    }
    return 1;
  }
  (void)printf("PASS ros_to_weld payload=%s\n", INTEROP_HELLO);

  n = snprintf(cmd, sizeof(cmd),
    "kill \"$(cat %s 2>/dev/null)\" 2>/dev/null; pkill -f \"ros2 topic pub.*%s\" 2>/dev/null; true",
    INTEROP_PUB_PID, INTEROP_TOPIC);
  if (n < 0 || (size_t)n >= sizeof(cmd)) {
    (void)fprintf(stderr, "FAIL pub kill snprintf\n");
    return 1;
  }
  (void)run_cmd(cmd);

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
  (void)printf("PASS weld_interop_real\n");
  return 0;
}
