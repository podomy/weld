// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//
// svcwait_test.c: prove RMW service/client plus guard/waitset roundtrips
// through the real Go FFI.
//
// It links rmw_weld.c plus weld_cdr.c against libweld_ffi.so built on
// the host with go build -buildmode=c-shared
// ./internal/rmwbridge/weldffi, and drives the RMW chain through the
// real FFI: rmw_init, create_node, create_service plus create_client on
// one service name with std_srvs Empty introspection, client
// send_request, server take_request, server send_response, client
// take_response with sequence echo match, plus guard create plus trigger
// plus rmw_wait with 5s timeout expecting OK, plus wait with 1s timeout
// and no trigger expecting TIMEOUT. It includes the generated
// libweld_ffi.h header and has no stub fakes.
//
// Concurrency note: the service exchange needs one helper thread. The
// client take_response blocks inside weld_ffi_service_call_nb (bounded
// 5s) and the request only reaches the Go service once that call runs,
// so a single thread cannot serve its own request. The helper runs a
// bounded take_request plus send_response polling loop while the main
// thread blocks in take_response. This mirrors interop_test.c, which
// uses background peer processes for the same reason: with two
// endpoints, one side must move while the other waits. The guard plus
// waitset part stays single threaded with no polling sleep.

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <pthread.h>

#include "libweld_ffi.h"
#include "rcutils/allocator.h"
#include "rmw/init.h"
#include "rmw/init_options.h"
#include "rmw/qos_profiles.h"
#include "rmw/ret_types.h"
#include "rmw/rmw.h"
#include "rmw/types.h"
#include "std_srvs/srv/empty.h"

#define SVCWAIT_NODE "weld_svcwait_node"
#define SVCWAIT_NS "/weld"
#define SVCWAIT_SERVICE "/weld_svcwait"
#define SVCWAIT_TRIES 4000
#define SVCWAIT_SLEEP_MS 5

struct svcwait_server_args
{
  rmw_service_t * service;
  volatile int stop;
  volatile int served;
  volatile int failed;
};

static void sleep_ms(long ms)
{
  struct timespec ts;
  ts.tv_sec = ms / 1000;
  ts.tv_nsec = (ms % 1000) * 1000000L;
  (void)nanosleep(&ts, NULL);
}

static int check_ret(rmw_ret_t got, rmw_ret_t want, const char * what)
{
  if (got != want) {
    (void)fprintf(stderr, "FAIL %s: got %d want %d\n", what, (int)got, (int)want);
    return 1;
  }
  (void)printf("PASS %s\n", what);
  return 0;
}

static void svcwait_join(pthread_t * thread, int * on, struct svcwait_server_args * args)
{
  if (on != NULL && *on != 0) {
    args->stop = 1;
    (void)pthread_join(*thread, NULL);
    *on = 0;
  }
}

static void * svcwait_server_fn(void * arg)
{
  struct svcwait_server_args * args = (struct svcwait_server_args *)arg;
  std_srvs__srv__Empty_Request req;
  std_srvs__srv__Empty_Response resp;
  int i = 0;
  (void)memset(&req, 0, sizeof(req));
  (void)memset(&resp, 0, sizeof(resp));
  for (i = 0; i < SVCWAIT_TRIES; i++) {
    rmw_service_info_t hdr;
    rmw_request_id_t rid;
    bool taken = false;
    rmw_ret_t rc = RMW_RET_OK;
    if (args->stop != 0) {
      return NULL;
    }
    (void)memset(&hdr, 0, sizeof(hdr));
    rc = rmw_take_request(args->service, &hdr, &req, &taken);
    if (rc != RMW_RET_OK) {
      args->failed = 1;
      return NULL;
    }
    if (taken) {
      rid = hdr.request_id;
      rc = rmw_send_response(args->service, &rid, &resp);
      if (rc != RMW_RET_OK) {
        args->failed = 1;
        return NULL;
      }
      args->served = 1;
      return NULL;
    }
    sleep_ms(SVCWAIT_SLEEP_MS);
  }
  args->failed = 1;
  return NULL;
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
  const rosidl_service_type_support_t * ts = NULL;
  rmw_qos_profile_t qos = rmw_qos_profile_default;
  rmw_service_t * svc = NULL;
  rmw_client_t * cli = NULL;
  std_srvs__srv__Empty_Request cli_req;
  std_srvs__srv__Empty_Response cli_resp;
  int64_t seq = 0;
  rmw_service_info_t resp_hdr;
  bool taken = false;
  rmw_ret_t rc = RMW_RET_OK;
  struct svcwait_server_args sargs;
  pthread_t server;
  int thread_on = 0;
  rmw_wait_set_t * ws = NULL;
  rmw_guard_condition_t * guard = NULL;
  void * garr[1] = {NULL};
  rmw_guard_conditions_t guards = {0, NULL};
  rmw_time_t t5 = {0, 0};
  rmw_time_t t1 = {0, 0};

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

  node = rmw_create_node(&context, SVCWAIT_NODE, SVCWAIT_NS);
  if (node == NULL) {
    (void)fprintf(stderr, "FAIL create_node returned NULL\n");
    return 1;
  }
  (void)printf("PASS create_node\n");

  ts = ROSIDL_GET_SRV_TYPE_SUPPORT(std_srvs, srv, Empty);
  if (ts == NULL) {
    (void)fprintf(stderr, "FAIL type support is NULL\n");
    return 1;
  }
  (void)printf("PASS type_support\n");

  svc = rmw_create_service(node, ts, SVCWAIT_SERVICE, &qos);
  if (svc == NULL) {
    (void)fprintf(stderr, "FAIL create_service returned NULL\n");
    return 1;
  }
  (void)printf("PASS create_service\n");

  cli = rmw_create_client(node, ts, SVCWAIT_SERVICE, &qos);
  if (cli == NULL) {
    (void)fprintf(stderr, "FAIL create_client returned NULL\n");
    if (rmw_destroy_service(node, svc) != RMW_RET_OK) {
      (void)fprintf(stderr, "FAIL destroy_service cleanup\n");
    }
    return 1;
  }
  (void)printf("PASS create_client\n");

  (void)memset(&sargs, 0, sizeof(sargs));
  sargs.service = svc;
  (void)memset(&cli_req, 0, sizeof(cli_req));
  (void)memset(&cli_resp, 0, sizeof(cli_resp));
  if (pthread_create(&server, NULL, svcwait_server_fn, &sargs) != 0) {
    (void)fprintf(stderr, "FAIL server thread spawn\n");
    if (rmw_destroy_client(node, cli) != RMW_RET_OK) {
      (void)fprintf(stderr, "FAIL destroy_client cleanup\n");
    }
    if (rmw_destroy_service(node, svc) != RMW_RET_OK) {
      (void)fprintf(stderr, "FAIL destroy_service cleanup\n");
    }
    return 1;
  }
  thread_on = 1;

  rc = rmw_send_request(cli, &cli_req, &seq);
  if (check_ret(rc, RMW_RET_OK, "send_request") != 0) {
    svcwait_join(&server, &thread_on, &sargs);
    return 1;
  }

  (void)memset(&resp_hdr, 0, sizeof(resp_hdr));
  rc = rmw_take_response(cli, &resp_hdr, &cli_resp, &taken);
  if (check_ret(rc, RMW_RET_OK, "take_response rc") != 0) {
    svcwait_join(&server, &thread_on, &sargs);
    return 1;
  }
  svcwait_join(&server, &thread_on, &sargs);
  if (sargs.failed != 0 || sargs.served == 0) {
    (void)fprintf(stderr, "FAIL server never served failed=%d served=%d\n",
      sargs.failed, sargs.served);
    return 1;
  }
  (void)printf("PASS server served\n");
  if (!taken) {
    (void)fprintf(stderr, "FAIL take_response never delivered\n");
    return 1;
  }
  if (resp_hdr.request_id.sequence_number != seq) {
    (void)fprintf(stderr, "FAIL svc echo want %lld got %lld\n",
      (long long)seq, (long long)resp_hdr.request_id.sequence_number);
    return 1;
  }
  (void)printf("PASS svc payload match seq=%lld\n", (long long)seq);

  if (check_ret(rmw_destroy_client(node, cli), RMW_RET_OK, "destroy_client") != 0) {
    return 1;
  }
  cli = NULL;
  if (check_ret(rmw_destroy_service(node, svc), RMW_RET_OK, "destroy_service") != 0) {
    return 1;
  }
  svc = NULL;

  ws = rmw_create_wait_set(&context, 4);
  if (ws == NULL) {
    (void)fprintf(stderr, "FAIL create_wait_set returned NULL\n");
    return 1;
  }
  (void)printf("PASS create_wait_set\n");

  guard = rmw_create_guard_condition(&context);
  if (guard == NULL) {
    (void)fprintf(stderr, "FAIL create_guard_condition returned NULL\n");
    return 1;
  }
  (void)printf("PASS create_guard_condition\n");

  if (check_ret(rmw_trigger_guard_condition(guard), RMW_RET_OK, "trigger_guard") != 0) {
    return 1;
  }

  garr[0] = guard;
  guards.guard_condition_count = 1;
  guards.guard_conditions = garr;
  t5.sec = 5;
  t5.nsec = 0;
  rc = rmw_wait(NULL, &guards, NULL, NULL, NULL, ws, &t5);
  if (check_ret(rc, RMW_RET_OK, "wait triggered rc") != 0) {
    return 1;
  }
  if (guards.guard_conditions[0] != guard) {
    (void)fprintf(stderr, "FAIL wait triggered dropped ready guard\n");
    return 1;
  }
  (void)printf("PASS wait triggered OK\n");

  if (check_ret(rmw_destroy_guard_condition(guard), RMW_RET_OK, "destroy_guard") != 0) {
    return 1;
  }
  guard = NULL;

  guard = rmw_create_guard_condition(&context);
  if (guard == NULL) {
    (void)fprintf(stderr, "FAIL recreate guard returned NULL\n");
    return 1;
  }
  garr[0] = guard;
  guards.guard_condition_count = 1;
  guards.guard_conditions = garr;
  t1.sec = 1;
  t1.nsec = 0;
  rc = rmw_wait(NULL, &guards, NULL, NULL, NULL, ws, &t1);
  if (check_ret(rc, RMW_RET_TIMEOUT, "wait timeout rc") != 0) {
    return 1;
  }
  if (guards.guard_conditions[0] != NULL) {
    (void)fprintf(stderr, "FAIL wait timeout kept idle guard set\n");
    return 1;
  }
  (void)printf("PASS wait timeout\n");

  if (check_ret(rmw_destroy_guard_condition(guard), RMW_RET_OK, "destroy_guard2") != 0) {
    return 1;
  }
  guard = NULL;
  if (check_ret(rmw_destroy_wait_set(ws), RMW_RET_OK, "destroy_wait_set") != 0) {
    return 1;
  }
  ws = NULL;

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

  (void)printf("PASS weld_svcwait_real\n");
  return 0;
}
