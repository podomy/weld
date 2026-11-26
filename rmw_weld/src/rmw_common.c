// SPDX-License-Identifier: Apache-2.0
// rmw_common.c: process bridge plus fail and check helpers.
#include "rmw_common.h"

const char kImplementationIdentifier[] = "rmw_weld";

// Process-global FFI bridge handle.
//
// The bridge is created on the first rmw_init and joined with the
// participant plus domain from the init options. It is reference counted
// across contexts and freed on the last rmw_shutdown. Node data stores a
// copy of the handle plus the node name so pub and sub creation can pass
// both to the FFI layer.
pthread_mutex_t g_weld_ffi_mutex = PTHREAD_MUTEX_INITIALIZER;

long long g_weld_bridge = 0;

int g_weld_bridge_refs = 0;

rmw_ret_t weld_fail_ffi(const char * what, int code)
{
  char buf[256];
  rmw_reset_error();
  if (what == NULL) {
    what = "rmw_weld: backend call failed";
  }
  (void)snprintf(buf, sizeof(buf), "%s: ffi rc=%d", what, code);
  buf[sizeof(buf) - 1] = '\0';
  RCUTILS_SET_ERROR_MSG(buf);
  return RMW_RET_ERROR;
}

// Loud refusal for configurations without the needed support.
//
// Most call sites compiled out once their feature landed; the remaining
// uses sit in fallback branches for headers a distro might lack, so the
// definition stays even when no live caller exists on this distro.
rmw_ret_t weld_unimplemented(const char * what) __attribute__((unused));

rmw_ret_t weld_unimplemented(const char * what)
{
  rmw_reset_error();
  if (what == NULL) {
    RCUTILS_SET_ERROR_MSG("rmw_weld: unimplemented");
  } else {
    char buf[256];
    (void)snprintf(buf, sizeof(buf), "rmw_weld: %s not implemented yet", what);
    buf[sizeof(buf) - 1] = '\0';
    RCUTILS_SET_ERROR_MSG(buf);
  }
  return RMW_RET_UNSUPPORTED;
}

static pthread_mutex_t g_weld_graph_mutex = PTHREAD_MUTEX_INITIALIZER;

bool weld_context_ok(const rmw_context_t * context)
{
  return context != NULL &&
    context->implementation_identifier == kImplementationIdentifier;
}

bool weld_impl_eq(const char * got)
{
  return got == kImplementationIdentifier;
}

bool weld_node_ok(const rmw_node_t * node)
{
  return node != NULL && weld_impl_eq(node->implementation_identifier);
}

bool weld_pub_ok(const rmw_publisher_t * pub)
{
  return pub != NULL && weld_impl_eq(pub->implementation_identifier);
}

bool weld_sub_ok(const rmw_subscription_t * sub)
{
  return sub != NULL && weld_impl_eq(sub->implementation_identifier);
}

bool weld_svc_ok(const rmw_service_t * svc)
{
  return svc != NULL && weld_impl_eq(svc->implementation_identifier);
}

bool weld_cli_ok(const rmw_client_t * cli)
{
  return cli != NULL && weld_impl_eq(cli->implementation_identifier);
}

bool weld_waitset_ok(const rmw_wait_set_t * ws)
{
  return ws != NULL && weld_impl_eq(ws->implementation_identifier);
}

// Wait wakeups: rmw_wait polls subscriptions, guards, services, and
// clients with a 10 ms sleep instead of blocking on g_weld_wait_cond,
// so trigger wakes it early only by shortening the next poll, never
// instantly. The cond plus broadcast below stay as a wakeup hook for
// a future blocking wait; today no waiter sleeps on it. Polling
// cannot miss a wakeup, at the cost of latency up to the sleep plus
// CPU burn on infinite waits.
static pthread_once_t g_weld_wait_once = PTHREAD_ONCE_INIT;

static pthread_mutex_t g_weld_wait_mutex = PTHREAD_MUTEX_INITIALIZER;

static pthread_cond_t g_weld_wait_cond;

static bool g_weld_wait_cond_ready = false;

static void weld_wait_cond_init(void)
{
  pthread_condattr_t attr;
  if (pthread_condattr_init(&attr) == 0) {
#if defined(CLOCK_MONOTONIC)
    (void)pthread_condattr_setclock(&attr, CLOCK_MONOTONIC);
#endif
    if (pthread_cond_init(&g_weld_wait_cond, &attr) == 0) {
      g_weld_wait_cond_ready = true;
    }
    (void)pthread_condattr_destroy(&attr);
  }
  if (!g_weld_wait_cond_ready) {
    (void)pthread_cond_init(&g_weld_wait_cond, NULL);
    g_weld_wait_cond_ready = true;
  }
}

void weld_wait_wake(void)
{
  (void)pthread_once(&g_weld_wait_once, weld_wait_cond_init);
  (void)pthread_mutex_lock(&g_weld_wait_mutex);
  (void)pthread_cond_broadcast(&g_weld_wait_cond);
  (void)pthread_mutex_unlock(&g_weld_wait_mutex);
}

void weld_graph_lock(void)
{
  (void)pthread_mutex_lock(&g_weld_graph_mutex);
}

void weld_graph_unlock(void)
{
  (void)pthread_mutex_unlock(&g_weld_graph_mutex);
}

rmw_ret_t weld_fail_invalid(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_INVALID_ARGUMENT;
}

rmw_ret_t weld_fail_impl(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_INCORRECT_RMW_IMPLEMENTATION;
}

rmw_ret_t weld_fail_error(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_ERROR;
}

rmw_ret_t weld_fail_unsupported(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_UNSUPPORTED;
}

char * weld_dup_str(const char * s)
{
  if (s == NULL) {
    return NULL;
  }
  size_t n = strlen(s) + 1;
  char * out = (char *)malloc(n);
  if (out == NULL) {
    return NULL;
  }
  (void)memcpy(out, s, n);
  return out;
}
