// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//
// rmw_weld: ROS middleware shim forwarding RMW calls to the Weld Go core.
// Backend contract: internal/rmwbridge/bridge.go (identifier "rmw_weld")
// plus internal/transports/transport.go Backend (PublishBytes, TakeBytes,
// JoinDiscovery, ReportLiveliness, Close). No Go mapping exists yet, so every
// entry point below validates its arguments and returns RMW_RET_ERROR via
// rmw_reset_error plus RCUTILS_SET_ERROR_MSG instead of crashing.

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <limits.h>

#include "rcutils/allocator.h"
#include "rcutils/error_handling.h"
#include "rcutils/types/string_array.h"
#include "rmw/domain_id.h"
#include "rmw/error_handling.h"
#include "rmw/rmw.h"
#include "rosidl_typesupport_introspection_c/identifier.h"
#include "weld_cdr.h"
#if defined(__has_include)
#if __has_include("rmw/event.h")
#include "rmw/event.h"
#endif
#if __has_include("rmw/names_and_types.h")
#include "rmw/names_and_types.h"
#define WELD_HAVE_NAMES_AND_TYPES 1
#endif
#if __has_include("rmw/get_topic_names_and_types.h")
#include "rmw/get_topic_names_and_types.h"
#endif
#if __has_include("rmw/get_service_names_and_types.h")
#include "rmw/get_service_names_and_types.h"
#endif
#if __has_include("rmw/get_node_info_and_types.h")
#include "rmw/get_node_info_and_types.h"
#endif
#if __has_include("rmw/get_topic_endpoint_info.h")
#include "rmw/get_topic_endpoint_info.h"
#endif
#if __has_include("rmw/get_network_flow_endpoints.h")
#include "rmw/get_network_flow_endpoints.h"
#endif
#if __has_include("rmw/features.h")
#include "rmw/features.h"
#endif
#if __has_include("rmw/dynamic_message_type_support.h")
#include "rmw/dynamic_message_type_support.h"
#endif
#if __has_include("rmw/event_callback_type.h")
#include "rmw/event_callback_type.h"
#endif
#if __has_include("rmw/time.h")
#include "rmw/time.h"
#endif
#endif

// Humble declares wait in rmw/wait.h, newer distros in rmw/wait_set.h.
#if defined(__has_include)
#if __has_include("rmw/wait_set.h")
#include "rmw/wait_set.h"
#elif __has_include("rmw/wait.h")
#include "rmw/wait.h"
#endif
#endif

// Bounds handle for rmw_get_serialized_message_size (Jazzy spelling).
#include <rosidl_runtime_c/sequence_bound.h>

static const char kImplementationIdentifier[] = "rmw_weld";

// FFI imports from the Go core (internal/rmwbridge/ffi.go).
extern int weld_ffi_bridge_new(long long * out_handle);
extern int weld_ffi_bridge_free(long long handle);
extern int weld_ffi_join(long long handle, const char * participant, int domain);
extern int weld_ffi_pub_create(
  long long bridge, const char * node, const char * topic, long long * out);
extern int weld_ffi_pub_destroy(long long handle);
extern int weld_ffi_pub_publish(long long handle, const char * data, int len);
extern int weld_ffi_sub_create(
  long long bridge, const char * node, const char * topic, long long * out);
extern int weld_ffi_sub_destroy(long long handle);
extern int weld_ffi_sub_take_poll(
  long long handle, char * buf, int buflen, int * out_n);

// Optional type annotation for graph interop.
//
// Older Go libraries lack this symbol, so it stays weak: call sites
// check for NULL and skip silently when it is absent. The type name
// is the ROS IDL name (such as std_msgs__msg__String) and the hash is
// the RIHS01 string, or empty for hashless wildcard interop.
extern int weld_ffi_pub_set_type(
  long long handle, const char * type_name, const char * type_hash)
  __attribute__((weak));

// Subscription type report matches the publisher path: the create
// signature carries no type, so the introspection handle follows
// separately right after sub create. Weak for the same old-library
// reason as the publisher form.
extern int weld_ffi_sub_set_type(
  long long handle, const char * type_name, const char * type_hash)
  __attribute__((weak));

// Process-global FFI bridge handle.
//
// The bridge is created on the first rmw_init and joined with the
// participant plus domain from the init options. It is reference counted
// across contexts and freed on the last rmw_shutdown. Node data stores a
// copy of the handle plus the node name so pub and sub creation can pass
// both to the FFI layer.
static pthread_mutex_t g_weld_ffi_mutex = PTHREAD_MUTEX_INITIALIZER;
static long long g_weld_bridge = 0;
static int g_weld_bridge_refs = 0;

struct weld_node_data
{
  long long bridge;
  char * node_name;
  size_t domain;
  rmw_guard_condition_t * graph_guard;
};

struct weld_pub_data
{
  long long handle;
  const rosidl_message_type_support_t * type_support;
  char * topic;
  size_t domain;
  rmw_qos_profile_t qos;
};

struct weld_sub_data
{
  long long handle;
  const rosidl_message_type_support_t * type_support;
  char * topic;
  size_t domain;
  rmw_qos_profile_t qos;
  rmw_event_callback_t on_new_callback;
  const void * on_new_user_data;
  // Wait stash holds one wire sample consumed by rmw_wait so the next
  // rmw_take returns it instead of polling again. Wait and take run on
  // the same thread in rcl executors, so no lock is needed.
  uint8_t * wait_buf;
  size_t wait_len;
  bool has_wait;
};

static rmw_ret_t weld_fail_ffi(const char * what, int code)
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

static const rosidl_typesupport_introspection_c__MessageMembers *
weld_resolve_members(const rosidl_message_type_support_t * type_support)
{
  const rosidl_message_type_support_t * h = NULL;
  if (type_support == NULL) {
    return NULL;
  }
  h = get_message_typesupport_handle(
    type_support, rosidl_typesupport_introspection_c__identifier);
  if (h == NULL || h->data == NULL) {
    return NULL;
  }
  return (const rosidl_typesupport_introspection_c__MessageMembers *)h->data;
}

// Build the ROS IDL type name for FFI type reports.
//
// The outer type support handle carries only a typesupport identifier
// (such as rosidl_typesupport_c), so the name comes from the
// introspection members: message_namespace_ holds the package plus
// subfolder (such as std_msgs__msg) and message_name_ holds the base
// name (such as String). On success buf holds e.g.
// std_msgs__msg__String. Returns 0 on success, -1 when a field is
// missing or the name does not fit.
static int weld_type_name(
  const rosidl_typesupport_introspection_c__MessageMembers * members,
  char * buf, size_t buflen)
{
  int n = 0;
  if (members == NULL || buf == NULL || buflen == 0) {
    return -1;
  }
  if (members->message_namespace_ == NULL ||
    members->message_name_ == NULL)
  {
    return -1;
  }
  n = snprintf(
    buf, buflen, "%s__%s",
    members->message_namespace_, members->message_name_);
  if (n < 0 || (size_t)n >= buflen) {
    return -1;
  }
  return 0;
}

// Report the ROS type name for a fresh pub or sub handle.
//
// The hash stays empty for hashless wildcard interop until type hash
// pinning lands. A missing symbol (old Go library) or an unusable
// name skips silently so creation still succeeds.
static void weld_report_type(
  long long handle,
  const rosidl_message_type_support_t * type_support)
{
  char name[512];
  if (weld_ffi_pub_set_type == NULL) {
    return;
  }
  if (weld_type_name(weld_resolve_members(type_support), name,
    sizeof(name)) != 0)
  {
    return;
  }
  (void)weld_ffi_pub_set_type(handle, name, "");
}

// weld_report_sub_type reports a subscription type the same way the
// publisher form does. Call sites check for NULL and skip silently when
// the Go library predates the symbol.
static void weld_report_sub_type(
  long long handle,
  const rosidl_message_type_support_t * type_support)
{
  char name[512];
  if (weld_ffi_sub_set_type == NULL) {
    return;
  }
  if (weld_type_name(weld_resolve_members(type_support), name,
    sizeof(name)) != 0)
  {
    return;
  }
  (void)weld_ffi_sub_set_type(handle, name, "");
}

// Poll one payload without blocking.
//
// Returns 0 with malloc owned bytes on data, 1 when no data is ready, and
// -1 with the rmw error set on failure. A short buffer is grown and
// retried internally because the FFI layer stashes the oversized sample.
static int weld_poll_payload(long long handle, uint8_t ** out, size_t * out_len)
{
  size_t cap = 64 * 1024;
  const size_t cap_max = (size_t)(32 << 20);
  if (out == NULL || out_len == NULL) {
    return -1;
  }
  *out = NULL;
  *out_len = 0;
  for (;;) {
    uint8_t * buf = NULL;
    int n = 0;
    int rc = 0;
    if (cap > (size_t)INT_MAX) {
      cap = (size_t)INT_MAX;
    }
    buf = (uint8_t *)malloc(cap > 0 ? cap : 1);
    if (buf == NULL) {
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: take out of memory");
      return -1;
    }
    rc = weld_ffi_sub_take_poll(
      handle, (char *)buf, (int)cap, &n);
    if (rc == 1) {
      free(buf);
      return 1;
    }
    if (rc == -4) {
      free(buf);
      if (cap >= cap_max) {
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: take payload exceeds limit");
        return -1;
      }
      cap *= 2;
      if (cap > cap_max) {
        cap = cap_max;
      }
      continue;
    }
    if (rc != 0) {
      free(buf);
      (void)weld_fail_ffi("rmw_weld: take poll failed", rc);
      return -1;
    }
    if (n < 0) {
      free(buf);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: take poll bad length");
      return -1;
    }
    if (n == 0) {
      free(buf);
      *out = NULL;
      *out_len = 0;
      return 0;
    }
    *out = buf;
    *out_len = (size_t)n;
    return 0;
  }
}

// Take a stashed wait sample if present.
//
// Wait consumes one sample to prove readiness and parks it here so the
// next take returns the same bytes instead of polling again. Returns
// true with malloc owned bytes on hit, false when no stash exists.
static bool weld_sub_consume_wait(struct weld_sub_data * sd, uint8_t ** out, size_t * out_len)
{
  if (sd == NULL || out == NULL || out_len == NULL) {
    return false;
  }
  if (!sd->has_wait) {
    return false;
  }
  *out = sd->wait_buf;
  *out_len = sd->wait_len;
  sd->wait_buf = NULL;
  sd->wait_len = 0;
  sd->has_wait = false;
  return true;
}

// Take one wire sample, preferring the wait stash.
//
// Wait parks one sample per subscription so readiness survives until
// take. This helper returns the stashed sample first and polls only
// when no stash exists, keeping wait plus take paired. Returns 0 with
// malloc owned bytes on data (zero bytes allowed as empty sample),
// 1 when no data is ready, and -1 with the rmw error set on failure.
static int weld_take_wire(struct weld_sub_data * sd, uint8_t ** out, size_t * out_len)
{
  if (sd == NULL || out == NULL || out_len == NULL) {
    return -1;
  }
  if (weld_sub_consume_wait(sd, out, out_len)) {
    return 0;
  }
  return weld_poll_payload(sd->handle, out, out_len);
}

// Poll one subscription for wait readiness without losing data.
//
// A stash hit reports ready immediately. Otherwise one non-blocking
// poll runs; data is parked in the subscription stash and reports
// ready, empty reports not ready, and poll errors report not ready so
// wait keeps polling until the budget expires and take can report the
// error later.
static bool weld_sub_wait_poll(struct weld_sub_data * sd)
{
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  int empty = 0;
  if (sd == NULL) {
    return false;
  }
  if (sd->has_wait) {
    return true;
  }
  empty = weld_poll_payload(sd->handle, &wire, &wire_len);
  if (empty != 0) {
    return false;
  }
  // Empty sample (zero bytes) still counts as data ready: the take
  // path delivers zero bytes with taken true.
  sd->wait_buf = wire;
  sd->wait_len = wire_len;
  sd->has_wait = true;
  return true;
}

static rmw_ret_t weld_unimplemented(const char * what)
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
  return RMW_RET_ERROR;
}

const char * rmw_get_implementation_identifier(void)
{
  return kImplementationIdentifier;
}

// Global node registry plus FFI graph snapshots plus counts.
//
// Nodes are tracked by name plus namespace plus domain in this file and
// merged with the Go bridge registry, which carries the same nodes once
// publishers or subscriptions use them. Topics and services merge the Go
// bridge registries (local publishers, subscriptions, services, clients
// with publisher type reports) with the live RTPS backend remote view,
// so ROS peers discovered over the wire list beside local endpoints.
// An absent backend or an older Go library without the graph symbols
// contributes nothing and every query still returns RMW_RET_OK with the
// local or empty snapshot.
//
// Thread-safety: g_weld_nodes is shared mutable state guarded by
// g_weld_graph_mutex. Register, unregister, and snapshot paths must hold
// the mutex while touching the list. Node handles themselves are not
// thread-safe for concurrent finalization. FFI graph queries run without
// holding it.
struct weld_node_entry
{
  char * name;
  char * namespace_;
  size_t domain;
  struct weld_node_entry * next;
};

static pthread_mutex_t g_weld_graph_mutex = PTHREAD_MUTEX_INITIALIZER;
static struct weld_node_entry * g_weld_nodes = NULL;

// Graph generation plus per-node graph guard list.
//
// g_weld_graph_generation counts node join plus leave events in this
// process. Each node owns at most one graph guard (see weld_node_data)
// returned by rmw_node_get_graph_guard_condition. The guard list holds
// those live guards so a generation bump can wake every waiter. The
// list and the counter share g_weld_graph_mutex with the node list.
static unsigned long long g_weld_graph_generation = 0;

struct weld_graph_guard_entry
{
  const rmw_guard_condition_t * guard;
  struct weld_graph_guard_entry * next;
};

static struct weld_graph_guard_entry * g_weld_graph_guards = NULL;

static void weld_graph_bump_and_trigger(void);

static bool weld_context_ok(const rmw_context_t * context)
{
  return context != NULL &&
    context->implementation_identifier == kImplementationIdentifier;
}

static bool weld_impl_eq(const char * got)
{
  return got == kImplementationIdentifier;
}

static bool weld_node_ok(const rmw_node_t * node)
{
  return node != NULL && weld_impl_eq(node->implementation_identifier);
}

static bool weld_pub_ok(const rmw_publisher_t * pub)
{
  return pub != NULL && weld_impl_eq(pub->implementation_identifier);
}

static bool weld_sub_ok(const rmw_subscription_t * sub)
{
  return sub != NULL && weld_impl_eq(sub->implementation_identifier);
}

static bool weld_svc_ok(const rmw_service_t * svc)
{
  return svc != NULL && weld_impl_eq(svc->implementation_identifier);
}

static bool weld_cli_ok(const rmw_client_t * cli)
{
  return cli != NULL && weld_impl_eq(cli->implementation_identifier);
}

static bool weld_waitset_ok(const rmw_wait_set_t * ws)
{
  return ws != NULL && weld_impl_eq(ws->implementation_identifier);
}

// Service take header compat: Humble, Jazzy, and Rolling all declare
// rmw_take_request and rmw_take_response with rmw_service_info_t and
// rmw_send_response with rmw_request_id_t. Very old Foxy-style headers
// used rmw_request_id_t for take as well. Define
// WELD_RMW_LEGACY_REQUEST_ID at compile time to build against those
// headers; otherwise the Jazzy and newer spelling is used.
#if defined(WELD_RMW_LEGACY_REQUEST_ID)
typedef rmw_request_id_t weld_service_info_t;
#else
typedef rmw_service_info_t weld_service_info_t;
#endif

// Wait wakeups: rmw_wait blocks on g_weld_wait_cond with an absolute
// deadline so trigger wakes it early instead of busy polling.
// The cond uses CLOCK_MONOTONIC when available. No 5ms sleep poll
// remains; the wait uses only pthread cond plus clock_gettime, so it
// stays C17 with no new dependencies.
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

static void weld_wait_wake(void)
{
  (void)pthread_once(&g_weld_wait_once, weld_wait_cond_init);
  (void)pthread_mutex_lock(&g_weld_wait_mutex);
  (void)pthread_cond_broadcast(&g_weld_wait_cond);
  (void)pthread_mutex_unlock(&g_weld_wait_mutex);
}

static void weld_graph_lock(void)
{
  (void)pthread_mutex_lock(&g_weld_graph_mutex);
}

static void weld_graph_unlock(void)
{
  (void)pthread_mutex_unlock(&g_weld_graph_mutex);
}

static rmw_ret_t weld_fail_invalid(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_INVALID_ARGUMENT;
}

static rmw_ret_t weld_fail_impl(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_INCORRECT_RMW_IMPLEMENTATION;
}

static rmw_ret_t weld_fail_error(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_ERROR;
}

static char * weld_dup_str(const char * s)
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

static void weld_node_register(
  const char * name, const char * namespace_, size_t domain)
{
  struct weld_node_entry * e =
    (struct weld_node_entry *)malloc(sizeof(*e));
  if (e == NULL) {
    return;
  }
  e->name = weld_dup_str(name);
  e->namespace_ = weld_dup_str(namespace_);
  e->domain = domain;
  if (e->name == NULL || e->namespace_ == NULL) {
    free(e->name);
    free(e->namespace_);
    free(e);
    return;
  }
  weld_graph_lock();
  e->next = g_weld_nodes;
  g_weld_nodes = e;
  weld_graph_unlock();
  weld_graph_bump_and_trigger();
}

static void weld_node_unregister(
  const char * name, const char * namespace_, size_t domain)
{
  if (name == NULL || namespace_ == NULL) {
    return;
  }
  weld_graph_lock();
  struct weld_node_entry ** link = &g_weld_nodes;
  while (*link != NULL) {
    const char * e_name = (*link)->name;
    const char * e_ns = (*link)->namespace_;
    bool match = false;
    if (e_name != NULL && e_ns != NULL) {
      match = strcmp(e_name, name) == 0 &&
        strcmp(e_ns, namespace_) == 0 &&
        (*link)->domain == domain;
    }
    if (match) {
      struct weld_node_entry * dead = *link;
      *link = dead->next;
      free(dead->name);
      free(dead->namespace_);
      free(dead);
      break;
    } else {
      link = &(*link)->next;
    }
  }
  weld_graph_unlock();
  weld_graph_bump_and_trigger();
}

// Bump the graph generation and wake every live graph guard.
//
// Call after the node list changes with the list already updated. The
// trigger runs outside the trigger path error handling: a failed FFI
// mirror keeps the local wakeup. Guards are triggered under the graph
// mutex so add plus remove plus bump stay atomic.
static void weld_graph_bump_and_trigger(void)
{
  weld_graph_lock();
  g_weld_graph_generation++;
  for (struct weld_graph_guard_entry * e = g_weld_graph_guards;
    e != NULL; e = e->next)
  {
    if (e->guard != NULL) {
      rmw_ret_t bump_rc = rmw_trigger_guard_condition(e->guard);
      (void)bump_rc;
    }
  }
  weld_graph_unlock();
}

// Track one per-node graph guard. The caller holds no locks. Returns 0
// on success and -1 on alloc failure.
static int weld_graph_guard_add(const rmw_guard_condition_t * guard)
{
  struct weld_graph_guard_entry * e = NULL;
  if (guard == NULL) {
    return -1;
  }
  e = (struct weld_graph_guard_entry *)malloc(sizeof(*e));
  if (e == NULL) {
    return -1;
  }
  e->guard = guard;
  weld_graph_lock();
  e->next = g_weld_graph_guards;
  g_weld_graph_guards = e;
  weld_graph_unlock();
  return 0;
}

// Drop one graph guard from the wake list. Missing entries are ignored.
static void weld_graph_guard_remove(const rmw_guard_condition_t * guard)
{
  if (guard == NULL) {
    return;
  }
  weld_graph_lock();
  struct weld_graph_guard_entry ** link = &g_weld_graph_guards;
  while (*link != NULL) {
    if ((*link)->guard == guard) {
      struct weld_graph_guard_entry * dead = *link;
      *link = dead->next;
      free(dead);
      break;
    } else {
      link = &(*link)->next;
    }
  }
  weld_graph_unlock();
}

// FFI graph imports from the Go core (internal/rmwbridge/weldffi/graph.go).
//
// Every symbol stays weak: a NULL address means an older Go library is
// linked and the query falls back to the local snapshot for nodes or the
// empty snapshot for topics, services, and counts, always with RMW_RET_OK.
extern int weld_ffi_graph_node_count(long long bridge, long long * out)
  __attribute__((weak));
extern int weld_ffi_graph_node_at(
  long long bridge, long long index, char * name_buf, int name_len,
  char * ns_buf, int ns_len, long long * domain_out)
  __attribute__((weak));
extern int weld_ffi_graph_topic_entry_count(long long bridge, long long * out)
  __attribute__((weak));
extern int weld_ffi_graph_topic_entry_at(
  long long bridge, long long index, char * topic_buf, int topic_len,
  char * type_buf, int type_len)
  __attribute__((weak));
extern int weld_ffi_graph_service_entry_count(
  long long bridge, long long * out)
  __attribute__((weak));
extern int weld_ffi_graph_service_entry_at(
  long long bridge, long long index, char * service_buf, int service_len,
  char * type_buf, int type_len)
  __attribute__((weak));
extern int weld_ffi_graph_count_publishers(
  long long bridge, const char * topic, long long * out)
  __attribute__((weak));
extern int weld_ffi_graph_count_subscribers(
  long long bridge, const char * topic, long long * out)
  __attribute__((weak));
extern int weld_ffi_graph_count_services(
  long long bridge, const char * service, long long * out)
  __attribute__((weak));
extern int weld_ffi_graph_count_clients(
  long long bridge, const char * service, long long * out)
  __attribute__((weak));

// Graph entry buffer sizes for one FFI string copy.
enum
{
  WELD_GRAPH_NAME_MAX = 1024,
  WELD_GRAPH_NAME_BIG = 16384
};

// Borrow the process bridge handle for graph queries, or 0 when uninit.
static long long weld_graph_bridge(void)
{
  long long bridge = 0;
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  bridge = g_weld_bridge;
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  return bridge;
}

// Growable node list for merged graph snapshots. Names and namespaces
// hold malloc owned strings freed by weld_graph_nodes_fini.
struct weld_graph_nodes
{
  char ** names;
  char ** spaces;
  size_t n;
  size_t cap;
};

static void weld_graph_nodes_fini(struct weld_graph_nodes * v)
{
  size_t i = 0;
  if (v == NULL) {
    return;
  }
  for (i = 0; i < v->n; i++) {
    free(v->names[i]);
    free(v->spaces[i]);
  }
  free(v->names);
  free(v->spaces);
  v->names = NULL;
  v->spaces = NULL;
  v->n = 0;
  v->cap = 0;
}

// Append one node unless the name plus namespace is already present.
// Returns 0 on success (including a dedup skip) and -1 on alloc failure.
static int weld_graph_nodes_push(
  struct weld_graph_nodes * v, const char * name, const char * ns)
{
  size_t i = 0;
  char * name_copy = NULL;
  char * ns_copy = NULL;
  char ** next_names = NULL;
  char ** next_spaces = NULL;
  if (v == NULL || name == NULL || ns == NULL) {
    return -1;
  }
  for (i = 0; i < v->n; i++) {
    if (strcmp(v->names[i], name) == 0 && strcmp(v->spaces[i], ns) == 0) {
      return 0;
    }
  }
  if (v->n == v->cap) {
    size_t ncap = v->cap == 0 ? 8 : v->cap * 2;
    next_names = (char **)realloc(v->names, ncap * sizeof(*next_names));
    if (next_names == NULL) {
      return -1;
    }
    v->names = next_names;
    next_spaces = (char **)realloc(
      v->spaces, ncap * sizeof(*next_spaces));
    if (next_spaces == NULL) {
      return -1;
    }
    v->spaces = next_spaces;
    v->cap = ncap;
  }
  name_copy = weld_dup_str(name);
  ns_copy = weld_dup_str(ns);
  if (name_copy == NULL || ns_copy == NULL) {
    free(name_copy);
    free(ns_copy);
    return -1;
  }
  v->names[v->n] = name_copy;
  v->spaces[v->n] = ns_copy;
  v->n++;
  return 0;
}

// One aggregated name with its distinct type list.
struct weld_graph_entry
{
  char * name;
  char ** types;
  size_t ntypes;
  size_t captypes;
};

// Growable names plus types list for merged topic and service snapshots.
struct weld_graph_nt
{
  struct weld_graph_entry * items;
  size_t n;
  size_t cap;
};

static void weld_graph_nt_fini(struct weld_graph_nt * v)
{
  size_t i = 0;
  size_t j = 0;
  if (v == NULL) {
    return;
  }
  for (i = 0; i < v->n; i++) {
    free(v->items[i].name);
    for (j = 0; j < v->items[i].ntypes; j++) {
      free(v->items[i].types[j]);
    }
    free(v->items[i].types);
  }
  free(v->items);
  v->items = NULL;
  v->n = 0;
  v->cap = 0;
}

// Append one (name, type) pair, adding the name or type only when new.
// An empty type lists the name with no types. Returns 0 on success and
// -1 on alloc failure.
static int weld_graph_nt_add(
  struct weld_graph_nt * v, const char * name, const char * type)
{
  size_t i = 0;
  struct weld_graph_entry * e = NULL;
  char ** next_types = NULL;
  char * type_copy = NULL;
  if (v == NULL || name == NULL || type == NULL) {
    return -1;
  }
  for (i = 0; i < v->n; i++) {
    if (strcmp(v->items[i].name, name) == 0) {
      e = &v->items[i];
      break;
    }
  }
  if (e == NULL) {
    struct weld_graph_entry * next = NULL;
    char * name_copy = NULL;
    size_t ncap = 0;
    if (v->n == v->cap) {
      ncap = v->cap == 0 ? 8 : v->cap * 2;
      next = (struct weld_graph_entry *)realloc(
        v->items, ncap * sizeof(*next));
      if (next == NULL) {
        return -1;
      }
      v->items = next;
      v->cap = ncap;
    }
    name_copy = weld_dup_str(name);
    if (name_copy == NULL) {
      return -1;
    }
    e = &v->items[v->n];
    e->name = name_copy;
    e->types = NULL;
    e->ntypes = 0;
    e->captypes = 0;
    v->n++;
  }
  if (type[0] == '\0') {
    return 0;
  }
  for (i = 0; i < e->ntypes; i++) {
    if (strcmp(e->types[i], type) == 0) {
      return 0;
    }
  }
  if (e->ntypes == e->captypes) {
    size_t ncap = e->captypes == 0 ? 4 : e->captypes * 2;
    next_types = (char **)realloc(e->types, ncap * sizeof(*next_types));
    if (next_types == NULL) {
      return -1;
    }
    e->types = next_types;
    e->captypes = ncap;
  }
  type_copy = weld_dup_str(type);
  if (type_copy == NULL) {
    return -1;
  }
  e->types[e->ntypes] = type_copy;
  e->ntypes++;
  return 0;
}

// Report whether any listed node already carries name.
//
// The Go registry normalizes namespaces to / because the FFI create
// signature carries the bare node name, so a name hit means the FFI
// entry duplicates a local node even when namespaces differ.
static bool weld_graph_nodes_has(
  const struct weld_graph_nodes * v, const char * name)
{
  size_t i = 0;
  if (v == NULL || name == NULL) {
    return false;
  }
  for (i = 0; i < v->n; i++) {
    if (strcmp(v->names[i], name) == 0) {
      return true;
    }
  }
  return false;
}

// Copy one FFI entry into malloc owned buffers, growing once on short.
//
// at_fn fills a_buf plus b_buf; on -4 the buffers grow to BIG and the
// call retries once. Returns 0 with *a_out plus *b_out owned by the
// caller, 1 to skip a torn entry, and -1 on alloc failure.
static int weld_graph_entry_copy(
  int (* at_fn)(long long, long long, char *, int, char *, int),
  long long bridge, long long index, char ** a_out, char ** b_out)
{
  char * a_buf = NULL;
  char * b_buf = NULL;
  char * a_big = NULL;
  char * b_big = NULL;
  int rc = 0;
  if (at_fn == NULL || a_out == NULL || b_out == NULL) {
    return -1;
  }
  *a_out = NULL;
  *b_out = NULL;
  a_buf = (char *)malloc(WELD_GRAPH_NAME_MAX);
  b_buf = (char *)malloc(WELD_GRAPH_NAME_MAX);
  if (a_buf == NULL || b_buf == NULL) {
    free(a_buf);
    free(b_buf);
    return -1;
  }
  rc = at_fn(
    bridge, index, a_buf, WELD_GRAPH_NAME_MAX, b_buf,
    WELD_GRAPH_NAME_MAX);
  if (rc == -4) {
    a_big = (char *)realloc(a_buf, WELD_GRAPH_NAME_BIG);
    if (a_big == NULL) {
      free(a_buf);
      free(b_buf);
      return -1;
    }
    a_buf = a_big;
    b_big = (char *)realloc(b_buf, WELD_GRAPH_NAME_BIG);
    if (b_big == NULL) {
      free(a_buf);
      free(b_buf);
      return -1;
    }
    b_buf = b_big;
    rc = at_fn(
      bridge, index, a_buf, WELD_GRAPH_NAME_BIG, b_buf,
      WELD_GRAPH_NAME_BIG);
  }
  if (rc != 0) {
    free(a_buf);
    free(b_buf);
    return 1;
  }
  *a_out = a_buf;
  *b_out = b_buf;
  return 0;
}

// Fetch every flat (name, type) pair from one FFI entry source.
//
// count_fn reports the entry count and at_fn copies one entry. FFI
// errors fall back to the pairs fetched so far so a torn snapshot stays
// best effort. Returns 0 on success and -1 on alloc failure.
static int weld_graph_fetch_pairs(
  int (* count_fn)(long long, long long *),
  int (* at_fn)(long long, long long, char *, int, char *, int),
  long long bridge, struct weld_graph_nt * out)
{
  long long n = 0;
  long long i = 0;
  if (count_fn == NULL || at_fn == NULL || out == NULL) {
    return -1;
  }
  if (count_fn(bridge, &n) != 0 || n <= 0) {
    return 0;
  }
  for (i = 0; i < n; i++) {
    char * a = NULL;
    char * b = NULL;
    int rc = weld_graph_entry_copy(at_fn, bridge, i, &a, &b);
    if (rc < 0) {
      return -1;
    }
    if (rc > 0) {
      continue;
    }
    rc = weld_graph_nt_add(out, a, b);
    free(a);
    free(b);
    if (rc != 0) {
      return -1;
    }
  }
  return 0;
}

// Duplicate one string with the caller allocator for names_and_types.
static char * weld_graph_alloc_dup(
  const rcutils_allocator_t * allocator, const char * s)
{
  size_t n = 0;
  char * out = NULL;
  if (allocator == NULL || allocator->allocate == NULL || s == NULL) {
    return NULL;
  }
  n = strlen(s) + 1;
  out = (char *)allocator->allocate(n, allocator->state);
  if (out == NULL) {
    return NULL;
  }
  (void)memcpy(out, s, n);
  return out;
}

// Fill a names_and_types snapshot from aggregated pairs.
//
// The output takes allocator owned strings so fini releases them. On
// failure the output is finalized and BAD_ALLOC returns.
static rmw_ret_t weld_graph_fill_names_and_types(
  const struct weld_graph_nt * v, rcutils_allocator_t * allocator,
  rmw_names_and_types_t * out, const char * what)
{
  size_t i = 0;
  size_t j = 0;
  rmw_ret_t rc;
  if (v == NULL || allocator == NULL || out == NULL || what == NULL) {
    return weld_fail_invalid("rmw_weld: graph fill null argument");
  }
  rc = rmw_names_and_types_init(out, v->n, allocator);
  if (rc != RMW_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(what);
    return RMW_RET_BAD_ALLOC;
  }
  for (i = 0; i < v->n; i++) {
    out->names.data[i] = weld_graph_alloc_dup(allocator, v->items[i].name);
    if (out->names.data[i] == NULL) {
      goto fail;
    }
    if (rcutils_string_array_init(
        &out->types[i], v->items[i].ntypes, allocator) !=
      RCUTILS_RET_OK)
    {
      goto fail;
    }
    for (j = 0; j < v->items[i].ntypes; j++) {
      out->types[i].data[j] = weld_graph_alloc_dup(
        allocator, v->items[i].types[j]);
      if (out->types[i].data[j] == NULL) {
        goto fail;
      }
    }
  }
  return RMW_RET_OK;
fail:
  {
    rmw_ret_t fini_rc = rmw_names_and_types_fini(out);
    (void)fini_rc;
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_BAD_ALLOC;
}

// Implementation-defined init options marker.
//
// A non-NULL impl means this init_options was set up by rmw_init_options_init.
// All other fields stay in plain C memory owned here: fini frees the enclave
// copy and the marker, then re-zeroes the struct.
struct rmw_init_options_impl_s
{
  int initialized;
};

rmw_init_options_t rmw_get_zero_initialized_init_options(void)
{
  rmw_init_options_t opts;
  (void)memset(&opts, 0, sizeof(opts));
  return opts;
}

static bool weld_init_options_is_init(const rmw_init_options_t * options)
{
  return options != NULL && options->impl != NULL &&
    options->implementation_identifier == kImplementationIdentifier;
}

rmw_ret_t rmw_init_options_init(
  rmw_init_options_t * init_options, rcutils_allocator_t allocator)
{
  if (init_options == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_init null argument");
  }
  if (weld_init_options_is_init(init_options)) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_init already init");
  }
  (void)memset(init_options, 0, sizeof(*init_options));
  init_options->implementation_identifier = kImplementationIdentifier;
  init_options->domain_id = RMW_DEFAULT_DOMAIN_ID;
  /* No localhost_only assignment: Humble and Jazzy define it with
     RMW_LOCALHOST_ONLY_DEFAULT equal to zero, so memset already yields the
     default; Rolling removed the field, so naming it breaks that build. */
  init_options->enclave = NULL;
  init_options->allocator = allocator;
  init_options->impl =
    (rmw_init_options_impl_t *)malloc(sizeof(rmw_init_options_impl_t));
  if (init_options->impl == NULL) {
    (void)memset(init_options, 0, sizeof(*init_options));
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_init_options_init no memory");
    return RMW_RET_BAD_ALLOC;
  }
  init_options->impl->initialized = 1;
  return RMW_RET_OK;
}

rmw_ret_t rmw_init_options_copy(
  const rmw_init_options_t * src, rmw_init_options_t * dst)
{
  char * enclave_copy = NULL;
  if (src == NULL || dst == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_copy null argument");
  }
  if (!weld_init_options_is_init(src)) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_copy bad source");
  }
  if (weld_init_options_is_init(dst)) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_copy dst taken");
  }
  if (src->implementation_identifier != kImplementationIdentifier) {
    return weld_fail_impl("rmw_weld: rmw_init_options_copy wrong implementation");
  }
  if (src->enclave != NULL) {
    enclave_copy = weld_dup_str(src->enclave);
    if (enclave_copy == NULL) {
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_init_options_copy no memory");
      return RMW_RET_BAD_ALLOC;
    }
  }
  *dst = *src;
  dst->enclave = enclave_copy;
  dst->impl = (rmw_init_options_impl_t *)malloc(sizeof(rmw_init_options_impl_t));
  if (dst->impl == NULL) {
    free(enclave_copy);
    (void)memset(dst, 0, sizeof(*dst));
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_init_options_copy no memory");
    return RMW_RET_BAD_ALLOC;
  }
  dst->impl->initialized = 1;
  return RMW_RET_OK;
}

rmw_ret_t rmw_init_options_fini(rmw_init_options_t * init_options)
{
  if (init_options == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_fini null argument");
  }
  if (!weld_init_options_is_init(init_options)) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_fini not init");
  }
  free(init_options->enclave);
  free(init_options->impl);
  (void)memset(init_options, 0, sizeof(*init_options));
  return RMW_RET_OK;
}

rmw_ret_t rmw_init(const rmw_init_options_t * options, rmw_context_t * context)
{
  size_t domain = 0;
  const char * participant = NULL;
  long long bridge = 0;
  int rc = 0;
  bool fresh = false;
  if (options == NULL || context == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init null argument");
  }
  if (context->implementation_identifier == kImplementationIdentifier) {
    return weld_fail_invalid("rmw_weld: rmw_init context already init");
  }
  domain = options->domain_id;
  if (domain == RMW_DEFAULT_DOMAIN_ID) {
    domain = 0;
  }
  if (domain > (size_t)INT_MAX) {
    return weld_fail_invalid("rmw_weld: rmw_init domain too large");
  }
  if (options->enclave != NULL && options->enclave[0] != '\0') {
    participant = options->enclave;
  } else {
    participant = "weld_rmw";
  }
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  if (g_weld_bridge == 0) {
    rc = weld_ffi_bridge_new(&bridge);
    if (rc != 0) {
      (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
      return weld_fail_ffi("rmw_weld: rmw_init bridge_new failed", rc);
    }
    g_weld_bridge = bridge;
    fresh = true;
  }
  rc = weld_ffi_join(g_weld_bridge, participant, (int)domain);
  if (rc != 0) {
    if (fresh) {
      long long dead = g_weld_bridge;
      g_weld_bridge = 0;
      (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
      (void)weld_ffi_bridge_free(dead);
      return weld_fail_ffi("rmw_weld: rmw_init join failed", rc);
    }
    (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
    return weld_fail_ffi("rmw_weld: rmw_init join failed", rc);
  }
  g_weld_bridge_refs++;
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  context->implementation_identifier = kImplementationIdentifier;
  context->actual_domain_id = domain;
  context->impl = NULL;
  return RMW_RET_OK;
}

rmw_ret_t rmw_shutdown(rmw_context_t * context)
{
  long long dead = 0;
  if (context == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_shutdown null argument");
  }
  if (!weld_context_ok(context)) {
    return weld_fail_impl("rmw_weld: rmw_shutdown wrong implementation");
  }
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  if (g_weld_bridge_refs > 0) {
    g_weld_bridge_refs--;
  }
  if (g_weld_bridge_refs == 0 && g_weld_bridge != 0) {
    dead = g_weld_bridge;
    g_weld_bridge = 0;
  }
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  if (dead != 0) {
    int rc = weld_ffi_bridge_free(dead);
    if (rc != 0) {
      return weld_fail_ffi("rmw_weld: rmw_shutdown bridge_free failed", rc);
    }
  }
  return RMW_RET_OK;
}

rmw_node_t * rmw_create_node(
  rmw_context_t * context, const char * name, const char * namespace_)
{
  rmw_node_t * node = NULL;
  char * name_copy = NULL;
  char * ns_copy = NULL;
  char * ffi_name = NULL;
  struct weld_node_data * nd = NULL;
  long long bridge = 0;
  size_t eff_domain = 0;
  if (context == NULL || name == NULL || namespace_ == NULL) {
    (void)weld_fail_invalid("rmw_weld: rmw_create_node null argument");
    return NULL;
  }
  if (!weld_context_ok(context)) {
    (void)weld_fail_impl("rmw_weld: rmw_create_node wrong implementation");
    return NULL;
  }
  if (name[0] == '\0' || namespace_[0] == '\0') {
    (void)weld_fail_invalid("rmw_weld: rmw_create_node empty name");
    return NULL;
  }
  eff_domain = context->actual_domain_id;
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  bridge = g_weld_bridge;
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  if (bridge == 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_node backend not init");
    return NULL;
  }
  node = (rmw_node_t *)malloc(sizeof(*node));
  if (node == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_node out of memory");
    return NULL;
  }
  name_copy = weld_dup_str(name);
  ns_copy = weld_dup_str(namespace_);
  ffi_name = weld_dup_str(name);
  nd = (struct weld_node_data *)malloc(sizeof(*nd));
  if (name_copy == NULL || ns_copy == NULL || ffi_name == NULL || nd == NULL) {
    free(name_copy);
    free(ns_copy);
    free(ffi_name);
    free(nd);
    free(node);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_node out of memory");
    return NULL;
  }
  nd->bridge = bridge;
  nd->node_name = ffi_name;
  nd->domain = eff_domain;
  nd->graph_guard = NULL;
  node->implementation_identifier = kImplementationIdentifier;
  node->data = nd;
  node->name = name_copy;
  node->namespace_ = ns_copy;
  node->context = context;
  weld_node_register(name, namespace_, eff_domain);
  return node;
}

rmw_ret_t rmw_destroy_node(rmw_node_t * node)
{
  struct weld_node_data * nd = NULL;
  if (node == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_node null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: rmw_destroy_node wrong implementation");
  }
  nd = (struct weld_node_data *)node->data;
  if (node->name != NULL && node->namespace_ != NULL) {
    size_t domain = 0;
    bool have_domain = false;
    if (nd != NULL) {
      domain = nd->domain;
      have_domain = true;
    } else if (node->context != NULL) {
      domain = node->context->actual_domain_id;
      have_domain = true;
    }
    if (have_domain) {
      weld_node_unregister(node->name, node->namespace_, domain);
    }
  }
  if (nd != NULL) {
    if (nd->graph_guard != NULL) {
      rmw_guard_condition_t * doomed = nd->graph_guard;
      nd->graph_guard = NULL;
      weld_graph_guard_remove(doomed);
      {
        rmw_ret_t fini_rc = rmw_destroy_guard_condition(doomed);
        (void)fini_rc;
      }
    }
    free(nd->node_name);
    free(nd);
  }
  node->data = NULL;
  free((void *)node->name);
  free((void *)node->namespace_);
  free(node);
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_node_names(
  const rmw_node_t * node, rcutils_string_array_t * node_names,
  rcutils_string_array_t * node_namespaces)
{
  if (node == NULL || node_names == NULL || node_namespaces == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_get_node_names null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: rmw_get_node_names wrong implementation");
  }
  if (node_names->size != 0 || node_names->data != NULL) {
    return weld_fail_invalid("rmw_weld: rmw_get_node_names names not zero");
  }
  if (node_namespaces->size != 0 || node_namespaces->data != NULL) {
    return weld_fail_invalid("rmw_weld: rmw_get_node_names spaces not zero");
  }
  // Snapshot the local registry plus the Go bridge registry so nodes
  // created through either path list. The backend contributes no nodes:
  // remote participants are not ROS nodes.
  struct weld_graph_nodes merged;
  long long bridge = 0;
  merged.names = NULL;
  merged.spaces = NULL;
  merged.n = 0;
  merged.cap = 0;
  weld_graph_lock();
  for (struct weld_node_entry * e = g_weld_nodes; e != NULL; e = e->next) {
    const char * src_name = e->name != NULL ? e->name : "";
    const char * src_ns = e->namespace_ != NULL ? e->namespace_ : "";
    if (weld_graph_nodes_push(&merged, src_name, src_ns) != 0) {
      weld_graph_unlock();
      weld_graph_nodes_fini(&merged);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_node_names out of memory");
      return RMW_RET_BAD_ALLOC;
    }
  }
  weld_graph_unlock();
  bridge = weld_graph_bridge();
  if (bridge != 0 && weld_ffi_graph_node_count != NULL &&
    weld_ffi_graph_node_at != NULL)
  {
    long long n = 0;
    if (weld_ffi_graph_node_count(bridge, &n) == 0 && n > 0) {
      long long i = 0;
      for (i = 0; i < n; i++) {
        char name_buf[WELD_GRAPH_NAME_MAX];
        char ns_buf[WELD_GRAPH_NAME_MAX];
        long long domain = 0;
        int rc = weld_ffi_graph_node_at(
          bridge, i, name_buf, (int)sizeof(name_buf), ns_buf,
          (int)sizeof(ns_buf), &domain);
        if (rc == -4) {
          char * big_name = (char *)malloc(WELD_GRAPH_NAME_BIG);
          char * big_ns = (char *)malloc(WELD_GRAPH_NAME_BIG);
          if (big_name == NULL || big_ns == NULL) {
            free(big_name);
            free(big_ns);
            weld_graph_nodes_fini(&merged);
            rmw_reset_error();
            RCUTILS_SET_ERROR_MSG(
              "rmw_weld: rmw_get_node_names out of memory");
            return RMW_RET_BAD_ALLOC;
          }
          rc = weld_ffi_graph_node_at(
            bridge, i, big_name, WELD_GRAPH_NAME_BIG, big_ns,
            WELD_GRAPH_NAME_BIG, &domain);
          if (rc == 0 && !weld_graph_nodes_has(&merged, big_name)) {
            if (weld_graph_nodes_push(&merged, big_name, big_ns) != 0) {
              free(big_name);
              free(big_ns);
              weld_graph_nodes_fini(&merged);
              rmw_reset_error();
              RCUTILS_SET_ERROR_MSG(
                "rmw_weld: rmw_get_node_names out of memory");
              return RMW_RET_BAD_ALLOC;
            }
          }
          free(big_name);
          free(big_ns);
          continue;
        }
        if (rc != 0) {
          continue;
        }
        if (weld_graph_nodes_has(&merged, name_buf)) {
          continue;
        }
        if (weld_graph_nodes_push(&merged, name_buf, ns_buf) != 0) {
          weld_graph_nodes_fini(&merged);
          rmw_reset_error();
          RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_node_names out of memory");
          return RMW_RET_BAD_ALLOC;
        }
      }
    }
  }
  if (merged.n == 0) {
    weld_graph_nodes_fini(&merged);
    return RMW_RET_OK;
  }
  rcutils_allocator_t allocator = rcutils_get_default_allocator();
  rmw_ret_t rc = (rmw_ret_t)rcutils_string_array_init(
    node_names, merged.n, &allocator);
  if (rc != RCUTILS_RET_OK) {
    weld_graph_nodes_fini(&merged);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_node_names alloc failed");
    return RMW_RET_BAD_ALLOC;
  }
  rc = (rmw_ret_t)rcutils_string_array_init(
    node_namespaces, merged.n, &allocator);
  if (rc != RCUTILS_RET_OK) {
    rcutils_ret_t fini_rc = rcutils_string_array_fini(node_names);
    (void)fini_rc;
    weld_graph_nodes_fini(&merged);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_node_names alloc failed");
    return RMW_RET_BAD_ALLOC;
  }
  for (size_t i = 0; i < merged.n; i++) {
    node_names->data[i] = merged.names[i];
    node_namespaces->data[i] = merged.spaces[i];
  }
  free(merged.names);
  free(merged.spaces);
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_topic_names_and_types(
  const rmw_node_t * node, rcutils_allocator_t * allocator, bool no_demangle,
  rmw_names_and_types_t * topic_names_and_types)
{
  (void)no_demangle;
  if (node == NULL || allocator == NULL || topic_names_and_types == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_get_topic_names_and_types null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl(
      "rmw_weld: rmw_get_topic_names_and_types wrong implementation");
  }
#ifdef WELD_HAVE_NAMES_AND_TYPES
  if (rmw_names_and_types_check_zero(topic_names_and_types) != RMW_RET_OK) {
    return weld_fail_invalid(
      "rmw_weld: rmw_get_topic_names_and_types not zero");
  }
  // Merge local topics with publisher type reports plus the live RTPS
  // backend remote view. A missing bridge or missing FFI symbols keeps
  // the empty snapshot with RMW_RET_OK.
  {
    struct weld_graph_nt merged;
    long long bridge = 0;
    rmw_ret_t fill_rc = RMW_RET_OK;
    merged.items = NULL;
    merged.n = 0;
    merged.cap = 0;
    bridge = weld_graph_bridge();
    if (bridge != 0 && weld_ffi_graph_topic_entry_count != NULL &&
      weld_ffi_graph_topic_entry_at != NULL)
    {
      if (weld_graph_fetch_pairs(
          weld_ffi_graph_topic_entry_count,
          weld_ffi_graph_topic_entry_at, bridge, &merged) != 0)
      {
        weld_graph_nt_fini(&merged);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG(
          "rmw_weld: rmw_get_topic_names_and_types out of memory");
        return RMW_RET_BAD_ALLOC;
      }
    }
    fill_rc = weld_graph_fill_names_and_types(
      &merged, allocator, topic_names_and_types,
      "rmw_weld: rmw_get_topic_names_and_types init failed");
    weld_graph_nt_fini(&merged);
    return fill_rc;
  }
#else
  (void)allocator;
  (void)topic_names_and_types;
  return weld_unimplemented(
    "rmw_get_topic_names_and_types: names_and_types header unavailable");
#endif
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_service_names_and_types(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  rmw_names_and_types_t * service_names_and_types)
{
  if (node == NULL || allocator == NULL || service_names_and_types == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_get_service_names_and_types null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl(
      "rmw_weld: rmw_get_service_names_and_types wrong implementation");
  }
#ifdef WELD_HAVE_NAMES_AND_TYPES
  if (rmw_names_and_types_check_zero(service_names_and_types) != RMW_RET_OK) {
    return weld_fail_invalid(
      "rmw_weld: rmw_get_service_names_and_types not zero");
  }
  // Merge local services with the live RTPS backend remote view. A
  // missing bridge or missing FFI symbols keeps the empty snapshot with
  // RMW_RET_OK.
  {
    struct weld_graph_nt merged;
    long long bridge = 0;
    rmw_ret_t fill_rc = RMW_RET_OK;
    merged.items = NULL;
    merged.n = 0;
    merged.cap = 0;
    bridge = weld_graph_bridge();
    if (bridge != 0 && weld_ffi_graph_service_entry_count != NULL &&
      weld_ffi_graph_service_entry_at != NULL)
    {
      if (weld_graph_fetch_pairs(
          weld_ffi_graph_service_entry_count,
          weld_ffi_graph_service_entry_at, bridge, &merged) != 0)
      {
        weld_graph_nt_fini(&merged);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG(
          "rmw_weld: rmw_get_service_names_and_types out of memory");
        return RMW_RET_BAD_ALLOC;
      }
    }
    fill_rc = weld_graph_fill_names_and_types(
      &merged, allocator, service_names_and_types,
      "rmw_weld: rmw_get_service_names_and_types init failed");
    weld_graph_nt_fini(&merged);
    return fill_rc;
  }
#else
  (void)allocator;
  (void)service_names_and_types;
  return weld_unimplemented(
    "rmw_get_service_names_and_types: names_and_types header unavailable");
#endif
  return RMW_RET_OK;
}

rmw_ret_t rmw_count_publishers(
  const rmw_node_t * node, const char * topic_name, size_t * count)
{
  long long bridge = 0;
  if (node == NULL || topic_name == NULL || count == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_count_publishers null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: rmw_count_publishers wrong impl");
  }
  // Merge local publishers with live RTPS remote writers. A missing
  // bridge, missing symbol, or FFI error keeps zero with RMW_RET_OK.
  *count = 0;
  bridge = weld_graph_bridge();
  if (bridge != 0 && weld_ffi_graph_count_publishers != NULL) {
    long long n = 0;
    if (weld_ffi_graph_count_publishers(bridge, topic_name, &n) == 0 &&
      n > 0)
    {
      *count = (size_t)n;
    }
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_count_subscribers(
  const rmw_node_t * node, const char * topic_name, size_t * count)
{
  long long bridge = 0;
  if (node == NULL || topic_name == NULL || count == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_count_subscribers null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: rmw_count_subscribers wrong impl");
  }
  // Merge local subscriptions with live RTPS remote readers. A missing
  // bridge, missing symbol, or FFI error keeps zero with RMW_RET_OK.
  *count = 0;
  bridge = weld_graph_bridge();
  if (bridge != 0 && weld_ffi_graph_count_subscribers != NULL) {
    long long n = 0;
    if (weld_ffi_graph_count_subscribers(bridge, topic_name, &n) == 0 &&
      n > 0)
    {
      *count = (size_t)n;
    }
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_count_services(
  const rmw_node_t * node, const char * service_name, size_t * count)
{
  long long bridge = 0;
  if (node == NULL || service_name == NULL || count == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_count_services null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: rmw_count_services wrong impl");
  }
  // Merge local servers with live RTPS remote servers. A missing bridge,
  // missing symbol, or FFI error keeps zero with RMW_RET_OK.
  *count = 0;
  bridge = weld_graph_bridge();
  if (bridge != 0 && weld_ffi_graph_count_services != NULL) {
    long long n = 0;
    if (weld_ffi_graph_count_services(bridge, service_name, &n) == 0 &&
      n > 0)
    {
      *count = (size_t)n;
    }
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_count_clients(
  const rmw_node_t * node, const char * service_name, size_t * count)
{
  long long bridge = 0;
  if (node == NULL || service_name == NULL || count == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_count_clients null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: rmw_count_clients wrong impl");
  }
  // Merge local clients with live RTPS remote clients. A missing bridge,
  // missing symbol, or FFI error keeps zero with RMW_RET_OK.
  *count = 0;
  bridge = weld_graph_bridge();
  if (bridge != 0 && weld_ffi_graph_count_clients != NULL) {
    long long n = 0;
    if (weld_ffi_graph_count_clients(bridge, service_name, &n) == 0 &&
      n > 0)
    {
      *count = (size_t)n;
    }
  }
  return RMW_RET_OK;
}

rmw_publisher_t * rmw_create_publisher(
  const rmw_node_t * node, const rosidl_message_type_support_t * type_support,
  const char * topic_name, const rmw_qos_profile_t * qos_profile,
  const rmw_publisher_options_t * publisher_options)
{
  // Bridge: weld_ffi_pub_create stores passthrough QoS and registers in
  // per-domain pubs keyed by domain plus topic. The node name comes from
  // rmw_node_t data so the Go layer reuses the cached bridge node.
  struct weld_node_data * nd = NULL;
  struct weld_pub_data * pd = NULL;
  rmw_publisher_t * pub = NULL;
  char * topic_copy = NULL;
  char * data_topic = NULL;
  long long handle = 0;
  int rc = 0;
  if (node == NULL || type_support == NULL || topic_name == NULL ||
    qos_profile == NULL)
  {
    (void)weld_fail_invalid("rmw_weld: rmw_create_publisher null argument");
    return NULL;
  }
  if (!weld_node_ok(node)) {
    (void)weld_fail_impl("rmw_weld: rmw_create_publisher wrong implementation");
    return NULL;
  }
  if (topic_name[0] == '\0') {
    (void)weld_fail_invalid("rmw_weld: rmw_create_publisher empty topic");
    return NULL;
  }
  nd = (struct weld_node_data *)node->data;
  if (nd == NULL || nd->node_name == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_publisher no node data");
    return NULL;
  }
  if (weld_resolve_members(type_support) == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_create_publisher type support is not introspection");
    return NULL;
  }
  pub = (rmw_publisher_t *)malloc(sizeof(*pub));
  pd = (struct weld_pub_data *)malloc(sizeof(*pd));
  topic_copy = weld_dup_str(topic_name);
  data_topic = weld_dup_str(topic_name);
  if (pub == NULL || pd == NULL || topic_copy == NULL || data_topic == NULL) {
    free(pub);
    free(pd);
    free(topic_copy);
    free(data_topic);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_publisher out of memory");
    return NULL;
  }
  rc = weld_ffi_pub_create(nd->bridge, nd->node_name, topic_name, &handle);
  if (rc != 0) {
    free(pub);
    free(pd);
    free(topic_copy);
    free(data_topic);
    (void)weld_fail_ffi("rmw_weld: rmw_create_publisher backend failed", rc);
    return NULL;
  }
  pd->handle = handle;
  pd->type_support = type_support;
  pd->topic = data_topic;
  pd->domain = nd->domain;
  pd->qos = *qos_profile;
  // Report the ROS type for graph interop. Best effort: a missing
  // symbol or bad name skips silently and creation still succeeds.
  weld_report_type(handle, type_support);
  pub->implementation_identifier = kImplementationIdentifier;
  pub->data = pd;
  pub->topic_name = topic_copy;
  if (publisher_options != NULL) {
    pub->options = *publisher_options;
  } else {
    (void)memset(&pub->options, 0, sizeof(pub->options));
  }
  pub->can_loan_messages = false;
  return pub;
}

rmw_ret_t rmw_destroy_publisher(rmw_node_t * node, rmw_publisher_t * publisher)
{
  // Bridge: weld_ffi_pub_destroy unregisters from per-domain pubs and marks
  // closed so CountMatchedPublishers drops and Publish reports
  // ErrPublisherClosed. Channels stay open to avoid send panics.
  struct weld_pub_data * pd = NULL;
  int rc = 0;
  if (node == NULL || publisher == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_publisher null argument");
  }
  if (!weld_node_ok(node) || !weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_destroy_publisher wrong implementation");
  }
  pd = (struct weld_pub_data *)publisher->data;
  if (pd != NULL) {
    rc = weld_ffi_pub_destroy(pd->handle);
    free(pd->topic);
    free(pd);
  }
  publisher->data = NULL;
  free((void *)publisher->topic_name);
  free(publisher);
  if (rc != 0) {
    return weld_fail_ffi("rmw_weld: rmw_destroy_publisher backend failed", rc);
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_publish(
  const rmw_publisher_t * publisher, const void * ros_message,
  rmw_publisher_allocation_t * allocation)
{
  // Bridge: Publisher.Publish delivers a copy-once payload to every
  // Subscription in the same domain on the same topic. The ROS message is
  // serialized with weld_cdr_encode_members through the passed introspection
  // handle and the wire bytes go out via weld_ffi_pub_publish.
  const struct weld_pub_data * pd = NULL;
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  weld_cdr_status_t st;
  int rc = 0;
  (void)allocation;
  if (publisher == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publish null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publish wrong implementation");
  }
  pd = (const struct weld_pub_data *)publisher->data;
  if (pd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publish no publisher data");
    return RMW_RET_ERROR;
  }
  members = weld_resolve_members(pd->type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_publish type support is not introspection");
    return RMW_RET_ERROR;
  }
  errbuf[0] = '\0';
  st = weld_cdr_encode_members(
    members, ros_message, true, &wire, &wire_len, errbuf, sizeof(errbuf));
  if (st != WELD_CDR_OK) {
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publish encode failed");
    }
    free(wire);
    return RMW_RET_ERROR;
  }
  if (wire_len > (size_t)INT_MAX) {
    free(wire);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publish payload too large");
    return RMW_RET_ERROR;
  }
  rc = weld_ffi_pub_publish(
    pd->handle, (const char *)wire, (int)wire_len);
  free(wire);
  if (rc != 0) {
    return weld_fail_ffi("rmw_weld: rmw_publish backend failed", rc);
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_publish_serialized_message(
  const rmw_publisher_t * publisher,
  const rmw_serialized_message_t * serialized_message,
  rmw_publisher_allocation_t * allocation)
{
  // The CDR wire bytes pass through with copy semantics. No fields are
  // read here.
  const struct weld_pub_data * pd = NULL;
  int rc = 0;
  (void)allocation;
  if (publisher == NULL || serialized_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publish_serialized_message null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publish_serialized_message wrong implementation");
  }
  pd = (const struct weld_pub_data *)publisher->data;
  if (pd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publish_serialized_message no data");
    return RMW_RET_ERROR;
  }
  if (serialized_message->buffer_length > 0 &&
    serialized_message->buffer == NULL)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_publish_serialized_message null buffer");
  }
  if (serialized_message->buffer_length > (size_t)INT_MAX) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_publish_serialized_message payload too large");
    return RMW_RET_ERROR;
  }
  rc = weld_ffi_pub_publish(
    pd->handle, (const char *)serialized_message->buffer,
    (int)serialized_message->buffer_length);
  if (rc != 0) {
    return weld_fail_ffi(
      "rmw_weld: rmw_publish_serialized_message backend failed", rc);
  }
  return RMW_RET_OK;
}

rmw_subscription_t * rmw_create_subscription(
  const rmw_node_t * node, const rosidl_message_type_support_t * type_support,
  const char * topic_name, const rmw_qos_profile_t * qos_profile,
  const rmw_subscription_options_t * subscription_options)
{
  // Bridge: weld_ffi_sub_create stores passthrough QoS and registers in
  // per-domain subs keyed by domain plus topic. Delivery only happens
  // within the same domain.
  struct weld_node_data * nd = NULL;
  struct weld_sub_data * sd = NULL;
  rmw_subscription_t * sub = NULL;
  char * topic_copy = NULL;
  char * data_topic = NULL;
  long long handle = 0;
  int rc = 0;
  if (node == NULL || type_support == NULL || topic_name == NULL ||
    qos_profile == NULL)
  {
    (void)weld_fail_invalid("rmw_weld: rmw_create_subscription null argument");
    return NULL;
  }
  if (!weld_node_ok(node)) {
    (void)weld_fail_impl("rmw_weld: rmw_create_subscription wrong implementation");
    return NULL;
  }
  if (topic_name[0] == '\0') {
    (void)weld_fail_invalid("rmw_weld: rmw_create_subscription empty topic");
    return NULL;
  }
  nd = (struct weld_node_data *)node->data;
  if (nd == NULL || nd->node_name == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_subscription no node data");
    return NULL;
  }
  if (weld_resolve_members(type_support) == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_create_subscription type support is not introspection");
    return NULL;
  }
  sub = (rmw_subscription_t *)malloc(sizeof(*sub));
  sd = (struct weld_sub_data *)malloc(sizeof(*sd));
  topic_copy = weld_dup_str(topic_name);
  data_topic = weld_dup_str(topic_name);
  if (sub == NULL || sd == NULL || topic_copy == NULL || data_topic == NULL) {
    free(sub);
    free(sd);
    free(topic_copy);
    free(data_topic);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_subscription out of memory");
    return NULL;
  }
  rc = weld_ffi_sub_create(nd->bridge, nd->node_name, topic_name, &handle);
  if (rc != 0) {
    free(sub);
    free(sd);
    free(topic_copy);
    free(data_topic);
    (void)weld_fail_ffi(
      "rmw_weld: rmw_create_subscription backend failed", rc);
    return NULL;
  }
  sd->handle = handle;
  sd->type_support = type_support;
  sd->topic = data_topic;
  sd->domain = nd->domain;
  sd->qos = *qos_profile;
  sd->on_new_callback = NULL;
  sd->on_new_user_data = NULL;
  sd->wait_buf = NULL;
  sd->wait_len = 0;
  sd->has_wait = false;
  // Report the ROS type for graph interop. Best effort: a missing
  // symbol or bad name skips silently and creation still succeeds.
  weld_report_sub_type(handle, type_support);
  sub->implementation_identifier = kImplementationIdentifier;
  sub->data = sd;
  sub->topic_name = topic_copy;
  if (subscription_options != NULL) {
    sub->options = *subscription_options;
  } else {
    (void)memset(&sub->options, 0, sizeof(sub->options));
  }
  sub->can_loan_messages = false;
  sub->is_cft_enabled = false;
  return sub;
}

rmw_ret_t rmw_destroy_subscription(rmw_node_t * node, rmw_subscription_t * subscription)
{
  // Bridge: weld_ffi_sub_destroy unregisters from per-domain subs and marks
  // closed so CountMatchedSubscriptions drops and Take reports
  // ErrSubscriptionClosed. The delivery channel stays open.
  struct weld_sub_data * sd = NULL;
  int rc = 0;
  if (node == NULL || subscription == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_subscription null argument");
  }
  if (!weld_node_ok(node) || !weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_destroy_subscription wrong implementation");
  }
  sd = (struct weld_sub_data *)subscription->data;
  if (sd != NULL) {
    rc = weld_ffi_sub_destroy(sd->handle);
    free(sd->topic);
    free(sd->wait_buf);
    sd->wait_buf = NULL;
    sd->wait_len = 0;
    sd->has_wait = false;
    free(sd);
  }
  subscription->data = NULL;
  free((void *)subscription->topic_name);
  free(subscription);
  if (rc != 0) {
    return weld_fail_ffi(
      "rmw_weld: rmw_destroy_subscription backend failed", rc);
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_take(
  const rmw_subscription_t * subscription, void * ros_message, bool * taken,
  rmw_subscription_allocation_t * allocation)
{
  // Bridge: poll once without blocking and decode the copy-once payload
  // with weld_cdr_decode_members through the subscription introspection
  // handle. An empty queue reports taken false with RMW_RET_OK.
  struct weld_sub_data * sd = NULL;
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  weld_cdr_status_t st;
  int empty = 0;
  (void)allocation;
  if (subscription == NULL || ros_message == NULL || taken == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_take wrong implementation");
  }
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take no subscription data");
    return RMW_RET_ERROR;
  }
  members = weld_resolve_members(sd->type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take type support is not introspection");
    return RMW_RET_ERROR;
  }
  empty = weld_take_wire(sd, &wire, &wire_len);
  if (empty == 1) {
    *taken = false;
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  errbuf[0] = '\0';
  st = weld_cdr_decode_members(
    members, wire, wire_len, ros_message, errbuf, sizeof(errbuf));
  free(wire);
  if (st != WELD_CDR_OK) {
    *taken = false;
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take decode failed");
    }
    return RMW_RET_ERROR;
  }
  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_with_info(
  const rmw_subscription_t * subscription, void * ros_message, bool * taken,
  rmw_message_info_t * message_info, rmw_subscription_allocation_t * allocation)
{
  // Same poll plus decode path as rmw_take, with source info filled from
  // local metadata. The FFI layer carries no source timestamp or sequence,
  // so those report zero and unsupported while the receive stamp uses the
  // local clock.
  struct weld_sub_data * sd = NULL;
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  weld_cdr_status_t st;
  int empty = 0;
  struct timespec now;
  uint64_t now_ns = 0;
  (void)allocation;
  if (subscription == NULL || ros_message == NULL || taken == NULL ||
    message_info == NULL)
  {
    return weld_fail_invalid("rmw_weld: rmw_take_with_info null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_take_with_info wrong implementation");
  }
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_with_info no subscription data");
    return RMW_RET_ERROR;
  }
  members = weld_resolve_members(sd->type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_with_info type support is not introspection");
    return RMW_RET_ERROR;
  }
  empty = weld_take_wire(sd, &wire, &wire_len);
  if (empty == 1) {
    *taken = false;
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  errbuf[0] = '\0';
  st = weld_cdr_decode_members(
    members, wire, wire_len, ros_message, errbuf, sizeof(errbuf));
  free(wire);
  if (st != WELD_CDR_OK) {
    *taken = false;
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_with_info decode failed");
    }
    return RMW_RET_ERROR;
  }
  (void)clock_gettime(CLOCK_REALTIME, &now);
  now_ns = (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
  message_info->source_timestamp = 0;
  message_info->received_timestamp = (rmw_time_point_value_t)now_ns;
  message_info->publication_sequence_number =
    RMW_MESSAGE_INFO_SEQUENCE_NUMBER_UNSUPPORTED;
  message_info->reception_sequence_number =
    RMW_MESSAGE_INFO_SEQUENCE_NUMBER_UNSUPPORTED;
  message_info->publisher_gid.implementation_identifier =
    kImplementationIdentifier;
  (void)memset(
    message_info->publisher_gid.data, 0,
    sizeof(message_info->publisher_gid.data));
  message_info->from_intra_process = false;
  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_serialized_message(
  const rmw_subscription_t * subscription,
  rmw_serialized_message_t * serialized_message, bool * taken,
  rmw_subscription_allocation_t * allocation)
{
  // The CDR wire bytes pass through with copy semantics into the caller
  // buffer, growing it when the sample does not fit.
  struct weld_sub_data * sd = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  int empty = 0;
  (void)allocation;
  if (subscription == NULL || serialized_message == NULL || taken == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_serialized_message null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_take_serialized_message wrong implementation");
  }
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_serialized_message no data");
    return RMW_RET_ERROR;
  }
  empty = weld_take_wire(sd, &wire, &wire_len);
  if (empty == 1) {
    *taken = false;
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  if (wire_len > serialized_message->buffer_capacity) {
    rcutils_ret_t rc = rmw_serialized_message_resize(
      serialized_message, wire_len);
    if (rc != RCUTILS_RET_OK) {
      free(wire);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_take_serialized_message resize failed");
      return RMW_RET_BAD_ALLOC;
    }
  }
  if (wire_len > 0) {
    if (serialized_message->buffer == NULL) {
      free(wire);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_take_serialized_message null buffer");
      return RMW_RET_ERROR;
    }
    (void)memcpy(serialized_message->buffer, wire, wire_len);
  }
  free(wire);
  serialized_message->buffer_length = wire_len;
  *taken = true;
  return RMW_RET_OK;
}

// Service, client, guard, and waitset FFI wiring.
//
// The Go core (internal/rmwbridge/ffi.go) exports the bridge plus
// service and guard calls. The bridge handle itself is owned by
// rmw_init and rmw_shutdown (see g_weld_bridge); service and client
// creation only borrows it. The service and guard symbols below have
// no declarations elsewhere in this file, so they stay weak here: a
// NULL address means the Go core is not linked and the path fails
// loudly instead of crashing. Guards keep a local triggered flag as
// the source of truth for rmw_wait and mirror it into the FFI guard
// when one exists.
extern int weld_ffi_service_create(
  long long bridge, const char * name, long long * out_handle)
  __attribute__((weak));
extern int weld_ffi_service_destroy(long long handle)
  __attribute__((weak));
extern int weld_ffi_service_call_nb(
  long long handle, const char * req, int req_len, char * resp_buf,
  int resp_buf_len, int * out_n) __attribute__((weak));
// Serve and respond match the internal/rmwbridge/ffi.go exports. They
// stay weak so rmw_take_request and rmw_send_response fail loudly when
// the Go core is not linked instead of crashing: non-blocking poll
// with 0 for data, 1 for empty, and negative codes for errors,
// following the existing call_nb shape.
extern int weld_ffi_service_serve_nb(
  long long handle, char * req_buf, int req_buf_len, int * out_n,
  long long * out_seq) __attribute__((weak));
extern int weld_ffi_service_respond(
  long long handle, long long seq, const char * resp,
  int resp_len) __attribute__((weak));
// Matched-entity queries match the internal/rmwbridge/weldffi/matched.go
// exports. They stay weak so counts fail loudly when the Go core is not
// linked instead of crashing. Counts sum local same-bridge endpoints
// with remote SEDP peers; absence of a linked core yields an error,
// never a silent zero.
extern int weld_ffi_pub_matched_subs(long long handle, int * out_n)
  __attribute__((weak));
extern int weld_ffi_sub_matched_pubs(long long handle, int * out_n)
  __attribute__((weak));
extern int weld_ffi_service_remote_available(
  long long bridge, const char * name, int * out_n)
  __attribute__((weak));
extern int weld_ffi_guard_create(long long * out_handle)
  __attribute__((weak));
extern int weld_ffi_guard_trigger(long long handle)
  __attribute__((weak));
extern int weld_ffi_guard_reset(long long handle)
  __attribute__((weak));
// Guard destroy matches the internal/rmwbridge/ffi.go export. It stays
// weak so destroy clears the FFI state best effort when the Go core is
// not linked and still frees local state.
extern int weld_ffi_guard_destroy(long long handle)
  __attribute__((weak));

// Cap on in-flight service sequences per endpoint.
enum
{
  WELD_RPC_PENDING_MAX = 16
};

// One endpoint for the RMW service and client pairs.
//
// Servers and clients use the same layout: the FFI handle is shared
// per service name (see weld_svc_acquire), name keys the shared entry
// for release, next_seq hands out client sequence ids starting at 1,
// and pending tracks live ids. Server pending entries are take ids
// waiting for a response; client pending entries are send ids waiting
// for a reply. Request and response bodies stay untouched until the
// CDR link lands; only the 8 byte little endian sequence echo
// travels. n_pending never exceeds WELD_RPC_PENDING_MAX.
struct weld_rpc_state
{
  long long ffi_handle;
  rmw_qos_profile_t qos;
  char * name;
  pthread_mutex_t mutex;
  int64_t next_seq;
  int64_t pending[WELD_RPC_PENDING_MAX];
  size_t n_pending;
  rmw_event_callback_t on_new_callback;
  const void * on_new_user_data;
};

// Write one sequence id as 8 little endian bytes.
static void weld_seq_write(int64_t v, char out[8])
{
  uint64_t u = (uint64_t)v;
  for (int i = 0; i < 8; i++) {
    out[i] = (char)((u >> (unsigned)(i * 8)) & 0xFFu);
  }
}

// Read one sequence id from 8 little endian bytes.
static int64_t weld_seq_read(const char in[8])
{
  uint64_t u = 0;
  for (int i = 0; i < 8; i++) {
    u |= ((uint64_t)(unsigned char)in[i]) << (unsigned)(i * 8);
  }
  return (int64_t)u;
}

// Drop one sequence id from the pending set. The caller holds the
// endpoint mutex. Missing ids are ignored so double removal is safe.
static void weld_rpc_remove_locked(struct weld_rpc_state * st, int64_t seq)
{
  for (size_t i = 0; i < st->n_pending; i++) {
    if (st->pending[i] == seq) {
      size_t tail = st->n_pending - i - 1;
      if (tail > 0) {
        (void)memmove(
          &st->pending[i], &st->pending[i + 1],
          tail * sizeof(st->pending[0]));
      }
      st->n_pending--;
      return;
    }
  }
}

// Borrow the process bridge created by rmw_init.
//
// Service and client handles share the process bridge (see
// g_weld_bridge); a zero handle means no context is initialized and
// creation fails loudly.
static rmw_ret_t weld_ffi_bridge_borrow(long long * out)
{
  if (out == NULL) {
    return weld_fail_invalid("rmw_weld: bridge null argument");
  }
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  long long bridge = g_weld_bridge;
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  if (bridge == 0) {
    return weld_fail_error("rmw_weld: bridge not initialized");
  }
  *out = bridge;
  return RMW_RET_OK;
}

// Shared FFI service handles keyed by service name.
//
// Servers and clients for one name share a single FFI service handle
// so a client call_nb rendezvous with a server serve_nb through the
// same Go Service requests channel. The entry lives while at least one
// server or client references it; the last release unregisters the FFI
// handle. g_weld_svc_mutex guards the list and the ref counts. Weld
// FFI calls that can block run without holding it.
struct weld_svc_shared
{
  char * name;
  long long ffi_handle;
  size_t refs;
  size_t servers;
  struct weld_svc_shared * next;
};

static pthread_mutex_t g_weld_svc_mutex = PTHREAD_MUTEX_INITIALIZER;
static struct weld_svc_shared * g_weld_svc_shared = NULL;

// Take a reference on the shared FFI handle for name, creating it on
// first use. The caller passes a validated non-empty name.
static rmw_ret_t weld_svc_acquire(
  long long bridge, const char * name, long long * out)
{
  if (name == NULL || out == NULL) {
    return weld_fail_invalid("rmw_weld: service registry null argument");
  }
  if (bridge == 0) {
    return weld_fail_invalid("rmw_weld: service registry null bridge");
  }
  if (weld_ffi_service_create == NULL) {
    return weld_fail_error("rmw_weld: service FFI not linked yet");
  }
  (void)pthread_mutex_lock(&g_weld_svc_mutex);
  for (struct weld_svc_shared * e = g_weld_svc_shared; e != NULL;
    e = e->next)
  {
    if (strcmp(e->name, name) == 0) {
      e->refs++;
      *out = e->ffi_handle;
      (void)pthread_mutex_unlock(&g_weld_svc_mutex);
      return RMW_RET_OK;
    }
  }
  long long fresh = 0;
  // Registry insert on the Go side never blocks, so creation stays
  // under the mutex and check plus insert are atomic.
  int rc = weld_ffi_service_create(bridge, name, &fresh);
  if (rc != 0 || fresh == 0) {
    (void)pthread_mutex_unlock(&g_weld_svc_mutex);
    return weld_fail_error("rmw_weld: service create failed");
  }
  struct weld_svc_shared * e =
    (struct weld_svc_shared *)malloc(sizeof(*e));
  if (e == NULL) {
    if (weld_ffi_service_destroy != NULL) {
      (void)weld_ffi_service_destroy(fresh);
    }
    (void)pthread_mutex_unlock(&g_weld_svc_mutex);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: service registry out of memory");
    return RMW_RET_BAD_ALLOC;
  }
  e->name = weld_dup_str(name);
  if (e->name == NULL) {
    free(e);
    if (weld_ffi_service_destroy != NULL) {
      (void)weld_ffi_service_destroy(fresh);
    }
    (void)pthread_mutex_unlock(&g_weld_svc_mutex);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: service registry out of memory");
    return RMW_RET_BAD_ALLOC;
  }
  e->ffi_handle = fresh;
  e->refs = 1;
  e->servers = 0;
  e->next = g_weld_svc_shared;
  g_weld_svc_shared = e;
  *out = fresh;
  (void)pthread_mutex_unlock(&g_weld_svc_mutex);
  return RMW_RET_OK;
}

// Drop one reference for name, unregistering the FFI handle on the
// last release. Unknown names fail loudly.
static rmw_ret_t weld_svc_release(const char * name)
{
  if (name == NULL) {
    return weld_fail_invalid("rmw_weld: service registry null argument");
  }
  (void)pthread_mutex_lock(&g_weld_svc_mutex);
  struct weld_svc_shared ** link = &g_weld_svc_shared;
  while (*link != NULL) {
    if (strcmp((*link)->name, name) == 0) {
      struct weld_svc_shared * dead = *link;
      if (dead->refs > 0) {
        dead->refs--;
      }
      long long doomed = 0;
      if (dead->refs == 0) {
        *link = dead->next;
        doomed = dead->ffi_handle;
        free(dead->name);
        free(dead);
      }
      (void)pthread_mutex_unlock(&g_weld_svc_mutex);
      if (doomed != 0 && weld_ffi_service_destroy != NULL) {
        int rc = weld_ffi_service_destroy(doomed);
        if (rc != 0) {
          return weld_fail_error("rmw_weld: service destroy failed");
        }
      }
      return RMW_RET_OK;
    }
    link = &(*link)->next;
  }
  (void)pthread_mutex_unlock(&g_weld_svc_mutex);
  return weld_fail_error("rmw_weld: service handle unknown");
}

rmw_service_t * rmw_create_service(
  const rmw_node_t * node, const rosidl_service_type_support_t * type_support,
  const char * service_name, const rmw_qos_profile_t * qos_profile)
{
  // Bridge: servers and clients for one name share a single FFI service
  // handle from the process bridge (see weld_svc_acquire). Only the
  // sequence echo travels for now; the type support and QoS are
  // validated but otherwise unused until the CDR link lands. The
  // standalone build has no FFI symbols, so creation fails loudly there
  // instead of returning a dead handle.
  (void)type_support;
  (void)qos_profile;
  if (node == NULL || type_support == NULL || service_name == NULL ||
    qos_profile == NULL)
  {
    (void)weld_fail_invalid("rmw_weld: rmw_create_service null argument");
    return NULL;
  }
  if (!weld_node_ok(node)) {
    (void)weld_fail_impl("rmw_weld: rmw_create_service wrong implementation");
    return NULL;
  }
  if (service_name[0] == '\0') {
    (void)weld_fail_invalid("rmw_weld: rmw_create_service empty service name");
    return NULL;
  }
  long long bridge = 0;
  if (weld_ffi_bridge_borrow(&bridge) != RMW_RET_OK) {
    return NULL;
  }
  struct weld_rpc_state * st =
    (struct weld_rpc_state *)calloc(1, sizeof(*st));
  if (st == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_service out of memory");
    return NULL;
  }
  if (pthread_mutex_init(&st->mutex, NULL) != 0) {
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_service no mutex");
    return NULL;
  }
  st->name = weld_dup_str(service_name);
  if (st->name == NULL) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_service out of memory");
    return NULL;
  }
  st->next_seq = 1;
  st->qos = *qos_profile;
  rmw_service_t * svc = (rmw_service_t *)malloc(sizeof(*svc));
  if (svc == NULL) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st->name);
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_service out of memory");
    return NULL;
  }
  if (weld_svc_acquire(bridge, service_name, &st->ffi_handle) != RMW_RET_OK) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st->name);
    free(st);
    free(svc);
    return NULL;
  }
  // Record the local server so rmw_service_server_is_available answers
  // from local state plus remote SEDP peers. Clients share the entry
  // without touching this count.
  (void)pthread_mutex_lock(&g_weld_svc_mutex);
  for (struct weld_svc_shared * e = g_weld_svc_shared; e != NULL;
    e = e->next)
  {
    if (strcmp(e->name, service_name) == 0) {
      e->servers++;
      break;
    }
  }
  (void)pthread_mutex_unlock(&g_weld_svc_mutex);
  svc->implementation_identifier = kImplementationIdentifier;
  svc->data = st;
  // service_name aliases the endpoint name; destroy frees it once
  // through st->name.
  svc->service_name = st->name;
  return svc;
}

rmw_ret_t rmw_destroy_service(rmw_node_t * node, rmw_service_t * service)
{
  if (node == NULL || service == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_service null argument");
  }
  if (!weld_node_ok(node) || !weld_svc_ok(service)) {
    return weld_fail_impl("rmw_weld: rmw_destroy_service wrong implementation");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)service->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_service no data");
  }
  // Release the shared FFI handle first so the last destroy unregisters
  // the Go service. Local state is freed on every path.
  rmw_ret_t rc = RMW_RET_OK;
  if (st->name != NULL) {
    (void)pthread_mutex_lock(&g_weld_svc_mutex);
    for (struct weld_svc_shared * e = g_weld_svc_shared; e != NULL;
      e = e->next)
    {
      if (strcmp(e->name, st->name) == 0) {
        if (e->servers > 0) {
          e->servers--;
        }
        break;
      }
    }
    (void)pthread_mutex_unlock(&g_weld_svc_mutex);
    rc = weld_svc_release(st->name);
  } else {
    rc = weld_fail_invalid("rmw_weld: rmw_destroy_service no name");
  }
  (void)pthread_mutex_destroy(&st->mutex);
  free(st->name);
  st->name = NULL;
  free(st);
  service->data = NULL;
  service->service_name = NULL;
  free(service);
  return rc;
}

rmw_ret_t rmw_take_request(
  const rmw_service_t * service, weld_service_info_t * request_header,
  void * ros_request, bool * taken)
{
  // Bridge: a pending weld_ffi_service_serve_nb dequeues one Go Service
  // request and reports its sequence echo. Only the echo is mapped into
  // the header; ros_request stays untouched until the CDR link lands.
  (void)ros_request;
  if (service == NULL || request_header == NULL || ros_request == NULL ||
    taken == NULL)
  {
    return weld_fail_invalid("rmw_weld: rmw_take_request null argument");
  }
  if (!weld_svc_ok(service)) {
    return weld_fail_impl("rmw_weld: rmw_take_request wrong implementation");
  }
  if (weld_ffi_service_serve_nb == NULL) {
    return weld_fail_error("rmw_weld: rmw_take_request serve not linked yet");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)service->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_request no data");
  }
  char req[8];
  int out_n = 0;
  long long seq = 0;
  (void)pthread_mutex_lock(&st->mutex);
  long long handle = st->ffi_handle;
  // The Go poll never blocks, so the serve stays under the endpoint
  // mutex and take plus pending record are atomic.
  int rc = weld_ffi_service_serve_nb(
    handle, req, (int)sizeof(req), &out_n, &seq);
  if (rc == 1) {
    *taken = false;
    (void)pthread_mutex_unlock(&st->mutex);
    return RMW_RET_OK;
  }
  if (rc != 0) {
    (void)pthread_mutex_unlock(&st->mutex);
    return weld_fail_error("rmw_weld: rmw_take_request serve failed");
  }
  if (out_n != (int)sizeof(req)) {
    (void)pthread_mutex_unlock(&st->mutex);
    return weld_fail_error("rmw_weld: rmw_take_request bad echo size");
  }
  if (st->n_pending >= WELD_RPC_PENDING_MAX) {
    (void)pthread_mutex_unlock(&st->mutex);
    return weld_fail_error("rmw_weld: rmw_take_request too many pending");
  }
  st->pending[st->n_pending++] = (int64_t)seq;
  request_header->source_timestamp = 0;
  request_header->received_timestamp = 0;
  (void)memset(
    request_header->request_id.writer_guid, 0,
    sizeof(request_header->request_id.writer_guid));
  request_header->request_id.sequence_number = (int64_t)seq;
  *taken = true;
  (void)pthread_mutex_unlock(&st->mutex);
  return RMW_RET_OK;
}

// rmw_send_response keeps rmw_request_id_t on Humble, Jazzy, and Rolling;
// only take_request and take_response moved to rmw_service_info_t.
rmw_ret_t rmw_send_response(
  const rmw_service_t * service, rmw_request_id_t * request_header, void * ros_response)
{
  // Bridge: the sequence echo recorded by rmw_take_request selects the
  // Go responder. Only the echo travels; ros_response stays untouched
  // until the CDR link lands.
  (void)ros_response;
  if (service == NULL || request_header == NULL || ros_response == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_send_response null argument");
  }
  if (!weld_svc_ok(service)) {
    return weld_fail_impl("rmw_weld: rmw_send_response wrong implementation");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)service->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_send_response no data");
  }
  if (weld_ffi_service_respond == NULL) {
    return weld_fail_error("rmw_weld: rmw_send_response respond not linked yet");
  }
  int64_t seq = request_header->sequence_number;
  (void)pthread_mutex_lock(&st->mutex);
  bool known = false;
  for (size_t i = 0; i < st->n_pending; i++) {
    if (st->pending[i] == seq) {
      known = true;
      break;
    }
  }
  if (!known) {
    (void)pthread_mutex_unlock(&st->mutex);
    return weld_fail_error("rmw_weld: rmw_send_response unknown sequence");
  }
  char payload[8];
  weld_seq_write(seq, payload);
  // The Go respond never blocks, so it stays under the endpoint mutex
  // and respond plus pending removal are atomic.
  int rc = weld_ffi_service_respond(
    st->ffi_handle, (long long)seq, payload, (int)sizeof(payload));
  if (rc != 0) {
    (void)pthread_mutex_unlock(&st->mutex);
    return weld_fail_error("rmw_weld: rmw_send_response respond failed");
  }
  weld_rpc_remove_locked(st, seq);
  (void)pthread_mutex_unlock(&st->mutex);
  return RMW_RET_OK;
}

rmw_client_t * rmw_create_client(
  const rmw_node_t * node, const rosidl_service_type_support_t * type_support,
  const char * service_name, const rmw_qos_profile_t * qos_profile)
{
  // Bridge: clients share the FFI service handle for their service name
  // (see weld_svc_acquire) so a client call_nb rendezvous with the
  // server serve_nb on the same Go Service. Only the sequence echo
  // travels for now; the type support and QoS are validated but
  // otherwise unused until the CDR link lands.
  (void)type_support;
  (void)qos_profile;
  if (node == NULL || type_support == NULL || service_name == NULL ||
    qos_profile == NULL)
  {
    (void)weld_fail_invalid("rmw_weld: rmw_create_client null argument");
    return NULL;
  }
  if (!weld_node_ok(node)) {
    (void)weld_fail_impl("rmw_weld: rmw_create_client wrong implementation");
    return NULL;
  }
  if (service_name[0] == '\0') {
    (void)weld_fail_invalid("rmw_weld: rmw_create_client empty service name");
    return NULL;
  }
  long long bridge = 0;
  if (weld_ffi_bridge_borrow(&bridge) != RMW_RET_OK) {
    return NULL;
  }
  struct weld_rpc_state * st =
    (struct weld_rpc_state *)calloc(1, sizeof(*st));
  if (st == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_client out of memory");
    return NULL;
  }
  if (pthread_mutex_init(&st->mutex, NULL) != 0) {
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_client no mutex");
    return NULL;
  }
  st->name = weld_dup_str(service_name);
  if (st->name == NULL) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_client out of memory");
    return NULL;
  }
  st->next_seq = 1;
  st->qos = *qos_profile;
  rmw_client_t * cli = (rmw_client_t *)malloc(sizeof(*cli));
  if (cli == NULL) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st->name);
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_client out of memory");
    return NULL;
  }
  if (weld_svc_acquire(bridge, service_name, &st->ffi_handle) != RMW_RET_OK) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st->name);
    free(st);
    free(cli);
    return NULL;
  }
  cli->implementation_identifier = kImplementationIdentifier;
  cli->data = st;
  // service_name aliases the endpoint name; destroy frees it once
  // through st->name.
  cli->service_name = st->name;
  return cli;
}

rmw_ret_t rmw_destroy_client(rmw_node_t * node, rmw_client_t * client)
{
  if (node == NULL || client == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_client null argument");
  }
  if (!weld_node_ok(node) || !weld_cli_ok(client)) {
    return weld_fail_impl("rmw_weld: rmw_destroy_client wrong implementation");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)client->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_client no data");
  }
  // Release the shared FFI handle first so the last destroy unregisters
  // the Go service. Local state is freed on every path.
  rmw_ret_t rc = RMW_RET_OK;
  if (st->name != NULL) {
    rc = weld_svc_release(st->name);
  } else {
    rc = weld_fail_invalid("rmw_weld: rmw_destroy_client no name");
  }
  (void)pthread_mutex_destroy(&st->mutex);
  free(st->name);
  st->name = NULL;
  free(st);
  client->data = NULL;
  client->service_name = NULL;
  free(client);
  return rc;
}

rmw_ret_t rmw_send_request(
  const rmw_client_t * client, const void * ros_request, int64_t * sequence_id)
{
  // Bridge: send records a fresh sequence id and returns at once; the
  // matching rmw_take_response carries the 8 byte echo through
  // weld_ffi_service_call_nb. ros_request stays untouched until the CDR
  // link lands.
  (void)ros_request;
  if (client == NULL || ros_request == NULL || sequence_id == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_send_request null argument");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl("rmw_weld: rmw_send_request wrong implementation");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)client->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_send_request no data");
  }
  (void)pthread_mutex_lock(&st->mutex);
  if (st->n_pending >= WELD_RPC_PENDING_MAX) {
    (void)pthread_mutex_unlock(&st->mutex);
    return weld_fail_error("rmw_weld: rmw_send_request too many pending");
  }
  if (st->next_seq == INT64_MAX) {
    (void)pthread_mutex_unlock(&st->mutex);
    return weld_fail_error("rmw_weld: rmw_send_request sequence exhausted");
  }
  int64_t seq = st->next_seq++;
  st->pending[st->n_pending++] = seq;
  *sequence_id = seq;
  (void)pthread_mutex_unlock(&st->mutex);
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_response(
  const rmw_client_t * client, weld_service_info_t * request_header,
  void * ros_response, bool * taken)
{
  // Bridge: the oldest pending sequence echo goes out through
  // weld_ffi_service_call_nb on the shared handle and the reply echo is
  // matched back to it. ros_response stays untouched until the CDR link
  // lands. The call blocks up to the FFI internal bound when no server
  // serves; the pending entry is kept so a later take can retry.
  (void)ros_response;
  if (client == NULL || request_header == NULL || ros_response == NULL ||
    taken == NULL)
  {
    return weld_fail_invalid("rmw_weld: rmw_take_response null argument");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl("rmw_weld: rmw_take_response wrong implementation");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)client->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_response no data");
  }
  if (weld_ffi_service_call_nb == NULL) {
    return weld_fail_error("rmw_weld: rmw_take_response call not linked yet");
  }
  (void)pthread_mutex_lock(&st->mutex);
  if (st->n_pending == 0) {
    *taken = false;
    (void)pthread_mutex_unlock(&st->mutex);
    return RMW_RET_OK;
  }
  int64_t seq = st->pending[0];
  long long handle = st->ffi_handle;
  // The Go call can block, so it runs without the endpoint mutex.
  (void)pthread_mutex_unlock(&st->mutex);
  char payload[8];
  weld_seq_write(seq, payload);
  char reply[8];
  int out_n = 0;
  int rc = weld_ffi_service_call_nb(
    handle, payload, (int)sizeof(payload), reply, (int)sizeof(reply),
    &out_n);
  if (rc == 1) {
    *taken = false;
    return RMW_RET_OK;
  }
  if (rc != 0) {
    return weld_fail_error("rmw_weld: rmw_take_response call failed");
  }
  if (out_n != (int)sizeof(reply)) {
    return weld_fail_error("rmw_weld: rmw_take_response bad echo size");
  }
  int64_t echo = weld_seq_read(reply);
  if (echo != seq) {
    return weld_fail_error("rmw_weld: rmw_take_response sequence mismatch");
  }
  (void)pthread_mutex_lock(&st->mutex);
  weld_rpc_remove_locked(st, seq);
  (void)pthread_mutex_unlock(&st->mutex);
  request_header->source_timestamp = 0;
  request_header->received_timestamp = 0;
  (void)memset(
    request_header->request_id.writer_guid, 0,
    sizeof(request_header->request_id.writer_guid));
  request_header->request_id.sequence_number = echo;
  *taken = true;
  return RMW_RET_OK;
}

struct weld_guard_state
{
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  bool triggered;
  // FFI mirror handle, or 0 when the Go core is not linked. The local
  // triggered flag stays the source of truth for rmw_wait.
  long long ffi_handle;
};

struct weld_wait_state
{
  size_t max_conditions;
};

static bool weld_guard_state_triggered(const struct weld_guard_state * st)
{
  bool out = false;
  struct weld_guard_state * mut = (struct weld_guard_state *)st;
  if (mut != NULL) {
    (void)pthread_mutex_lock(&mut->mutex);
    out = mut->triggered;
    (void)pthread_mutex_unlock(&mut->mutex);
  }
  return out;
}

// Resolve one wait subscription entry to its data.
//
// rcl stores rmw_handle->data per SET_ADD_RMW in rcl/wait.c, so entries
// arriving through rcl are already weld_sub_data. Direct C callers pass
// the rmw wrapper instead. Wrappers carry the implementation identifier
// at offset zero while data carries a small FFI handle there, so pointer
// equality against the identifier string tells them apart with no string
// reads and no address guessing.
static struct weld_sub_data * weld_wait_sub_entry(void * entry)
{
  const rmw_subscription_t * sub = NULL;
  if (entry == NULL) {
    return NULL;
  }
  sub = (const rmw_subscription_t *)entry;
  if (weld_impl_eq(sub->implementation_identifier)) {
    return (struct weld_sub_data *)sub->data;
  }
  return (struct weld_sub_data *)entry;
}

// Resolve one wait guard entry to its state.
//
// Same dual convention as subscriptions: rcl passes weld_guard_state
// while direct C callers pass the rmw wrapper.
static struct weld_guard_state * weld_wait_guard_entry(void * entry)
{
  const rmw_guard_condition_t * guard = NULL;
  if (entry == NULL) {
    return NULL;
  }
  guard = (const rmw_guard_condition_t *)entry;
  if (weld_impl_eq(guard->implementation_identifier)) {
    return (struct weld_guard_state *)guard->data;
  }
  return (struct weld_guard_state *)entry;
}

static uint64_t weld_timeout_ns(const rmw_time_t * timeout)
{
  if (timeout == NULL) {
    return UINT64_MAX;
  }
  if (timeout->sec > (UINT64_MAX - timeout->nsec) / 1000000000ULL) {
    return UINT64_MAX;
  }
  return timeout->sec * 1000000000ULL + timeout->nsec;
}

rmw_guard_condition_t * rmw_create_guard_condition(rmw_context_t * context)
{
  if (context == NULL) {
    (void)weld_fail_invalid(
      "rmw_weld: rmw_create_guard_condition null argument");
    return NULL;
  }
  if (!weld_context_ok(context)) {
    (void)weld_fail_impl(
      "rmw_weld: rmw_create_guard_condition wrong implementation");
    return NULL;
  }
  rmw_guard_condition_t * guard =
    (rmw_guard_condition_t *)malloc(sizeof(*guard));
  if (guard == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_guard_condition no mem");
    return NULL;
  }
  struct weld_guard_state * st =
    (struct weld_guard_state *)malloc(sizeof(*st));
  if (st == NULL) {
    free(guard);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_guard_condition no mem");
    return NULL;
  }
  if (pthread_mutex_init(&st->mutex, NULL) != 0) {
    free(st);
    free(guard);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_guard_condition no mutex");
    return NULL;
  }
  if (pthread_cond_init(&st->cond, NULL) != 0) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st);
    free(guard);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_guard_condition no cond");
    return NULL;
  }
  st->triggered = false;
  st->ffi_handle = 0;
  // Best effort FFI mirror: local wait works with or without the Go
  // core, so a failed FFI create never fails guard creation.
  if (weld_ffi_guard_create != NULL) {
    long long fresh = 0;
    if (weld_ffi_guard_create(&fresh) == 0 && fresh != 0) {
      st->ffi_handle = fresh;
    }
  }
  guard->implementation_identifier = kImplementationIdentifier;
  guard->data = st;
  guard->context = context;
  return guard;
}

rmw_ret_t rmw_destroy_guard_condition(rmw_guard_condition_t * guard_condition)
{
  if (guard_condition == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_destroy_guard_condition null argument");
  }
  if (guard_condition->implementation_identifier !=
    kImplementationIdentifier)
  {
    return weld_fail_impl(
      "rmw_weld: rmw_destroy_guard_condition wrong implementation");
  }
  struct weld_guard_state * st =
    (struct weld_guard_state *)guard_condition->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_guard_condition no data");
  }
  // The Go handle is released best effort when the destroy export is
  // missing; a reset clears its triggered state and the handle stays
  // valid until process end. Local state is freed on every path.
  long long doomed = st->ffi_handle;
  (void)pthread_mutex_destroy(&st->mutex);
  (void)pthread_cond_destroy(&st->cond);
  free(st);
  free(guard_condition);
  if (doomed != 0) {
    if (weld_ffi_guard_destroy != NULL) {
      if (weld_ffi_guard_destroy(doomed) != 0) {
        return weld_fail_error("rmw_weld: rmw_destroy_guard_condition ffi failed");
      }
    } else if (weld_ffi_guard_reset != NULL) {
      (void)weld_ffi_guard_reset(doomed);
    }
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_trigger_guard_condition(const rmw_guard_condition_t * guard_condition)
{
  if (guard_condition == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_trigger_guard_condition null argument");
  }
  if (guard_condition->implementation_identifier !=
    kImplementationIdentifier)
  {
    return weld_fail_impl(
      "rmw_weld: rmw_trigger_guard_condition wrong implementation");
  }
  struct weld_guard_state * st =
    (struct weld_guard_state *)guard_condition->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_trigger_guard_condition no data");
  }
  (void)pthread_mutex_lock(&st->mutex);
  st->triggered = true;
  (void)pthread_cond_broadcast(&st->cond);
  (void)pthread_mutex_unlock(&st->mutex);
  weld_wait_wake();
  // The local flag is already set, so a failed FFI mirror reports
  // loudly but keeps the local wakeup.
  if (st->ffi_handle != 0 && weld_ffi_guard_trigger != NULL) {
    int rc = weld_ffi_guard_trigger(st->ffi_handle);
    if (rc != 0) {
      return weld_fail_error("rmw_weld: rmw_trigger_guard_condition ffi failed");
    }
  }
  return RMW_RET_OK;
}

rmw_wait_set_t * rmw_create_wait_set(rmw_context_t * context, size_t max_conditions)
{
  if (context == NULL) {
    (void)weld_fail_invalid("rmw_weld: rmw_create_wait_set null argument");
    return NULL;
  }
  if (!weld_context_ok(context)) {
    (void)weld_fail_impl(
      "rmw_weld: rmw_create_wait_set wrong implementation");
    return NULL;
  }
  rmw_wait_set_t * ws = (rmw_wait_set_t *)calloc(1, sizeof(*ws));
  if (ws == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_wait_set no mem");
    return NULL;
  }
  struct weld_wait_state * st =
    (struct weld_wait_state *)calloc(1, sizeof(*st));
  if (st == NULL) {
    free(ws);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_wait_set no mem");
    return NULL;
  }
  st->max_conditions = max_conditions;
  ws->implementation_identifier = kImplementationIdentifier;
  ws->guard_conditions = NULL;
  ws->data = st;
  return ws;
}

rmw_ret_t rmw_destroy_wait_set(rmw_wait_set_t * wait_set)
{
  if (wait_set == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_wait_set null argument");
  }
  if (!weld_waitset_ok(wait_set)) {
    return weld_fail_impl("rmw_weld: rmw_destroy_wait_set wrong impl");
  }
  free(wait_set->data);
  wait_set->data = NULL;
  free(wait_set);
  return RMW_RET_OK;
}

rmw_ret_t rmw_wait(
  rmw_subscriptions_t * subscriptions, rmw_guard_conditions_t * guard_conditions,
  rmw_services_t * services, rmw_clients_t * clients, rmw_events_t * events,
  rmw_wait_set_t * wait_set, const rmw_time_t * wait_timeout)
{
  // Wait reports readiness by leaving only ready entries set: every
  // non-ready subscription, guard, service, client, and event is
  // replaced with NULL. Entries are implementation data pointers, not
  // rmw wrappers: rcl stores rmw_handle->data per SET_ADD_RMW in
  // rcl/wait.c, so subscriptions carry weld_sub_data and guards carry
  // weld_guard_state. Subscriptions are ready when a wire or in-memory
  // sample is available (polled into the wait stash so the next take
  // returns it); guards are ready when triggered. Services, clients,
  // and events are not ready yet: service traffic stays in-memory
  // single-process and cross-process service wait is out of scope, so
  // they always filter to NULL. NULL slots are empty from a previous
  // wait filter and skip. This is what lets ros2 topic echo wake on
  // weld pub: the old stub ignored subscriptions and always timed out
  // even when data had arrived.
  uint64_t budget = 0;
  uint64_t start_ns = 0;
  uint64_t deadline_ns = 0;
  bool infinite = false;
  struct timespec ts;
  if (wait_set == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_wait null argument");
  }
  if (!weld_waitset_ok(wait_set)) {
    return weld_fail_impl("rmw_weld: rmw_wait wrong implementation");
  }
  if (subscriptions != NULL) {
    if (subscriptions->subscriber_count > 0 && subscriptions->subscribers == NULL) {
      return weld_fail_invalid("rmw_weld: rmw_wait null subscribers");
    }
  }
  if (guard_conditions != NULL) {
    if (guard_conditions->guard_condition_count > 0 &&
      guard_conditions->guard_conditions == NULL)
    {
      return weld_fail_invalid("rmw_weld: rmw_wait null guard conditions");
    }
  }
  if (services != NULL) {
    if (services->service_count > 0 && services->services == NULL) {
      return weld_fail_invalid("rmw_weld: rmw_wait null services");
    }
  }
  if (clients != NULL) {
    if (clients->client_count > 0 && clients->clients == NULL) {
      return weld_fail_invalid("rmw_weld: rmw_wait null clients");
    }
  }
  if (events != NULL) {
    if (events->event_count > 0 && events->events == NULL) {
      return weld_fail_invalid("rmw_weld: rmw_wait null events");
    }
  }
  budget = weld_timeout_ns(wait_timeout);
  infinite = (wait_timeout == NULL);
  (void)clock_gettime(CLOCK_MONOTONIC, &ts);
  start_ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
  if (infinite) {
    deadline_ns = UINT64_MAX;
  } else if (budget > UINT64_MAX - start_ns) {
    deadline_ns = UINT64_MAX;
  } else {
    deadline_ns = start_ns + budget;
  }
  for (;;) {
    size_t ready = 0;
    if (guard_conditions != NULL) {
      for (size_t i = 0; i < guard_conditions->guard_condition_count; i++) {
        struct weld_guard_state * st =
          weld_wait_guard_entry(guard_conditions->guard_conditions[i]);
        if (st != NULL && weld_guard_state_triggered(st)) {
          ready++;
        }
      }
    }
    if (subscriptions != NULL) {
      for (size_t i = 0; i < subscriptions->subscriber_count; i++) {
        struct weld_sub_data * sd =
          weld_wait_sub_entry(subscriptions->subscribers[i]);
        if (sd == NULL) {
          continue;
        }
        if (weld_sub_wait_poll(sd)) {
          ready++;
        }
      }
    }
    if (ready > 0) {
      if (subscriptions != NULL) {
        for (size_t i = 0; i < subscriptions->subscriber_count; i++) {
          struct weld_sub_data * sd =
            weld_wait_sub_entry(subscriptions->subscribers[i]);
          if (sd == NULL || !sd->has_wait) {
            subscriptions->subscribers[i] = NULL;
          }
        }
      }
      if (guard_conditions != NULL) {
        for (size_t i = 0; i < guard_conditions->guard_condition_count; i++) {
          struct weld_guard_state * st = weld_wait_guard_entry(
            guard_conditions->guard_conditions[i]);
          if (st == NULL || !weld_guard_state_triggered(st)) {
            guard_conditions->guard_conditions[i] = NULL;
          }
        }
      }
      if (services != NULL) {
        for (size_t i = 0; i < services->service_count; i++) {
          services->services[i] = NULL;
        }
      }
      if (clients != NULL) {
        for (size_t i = 0; i < clients->client_count; i++) {
          clients->clients[i] = NULL;
        }
      }
      if (events != NULL) {
        for (size_t i = 0; i < events->event_count; i++) {
          events->events[i] = NULL;
        }
      }
      return RMW_RET_OK;
    }
    (void)clock_gettime(CLOCK_MONOTONIC, &ts);
    {
      uint64_t now_ns =
        (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
      if (!infinite && now_ns >= deadline_ns) {
        if (subscriptions != NULL) {
          for (size_t i = 0; i < subscriptions->subscriber_count; i++) {
            subscriptions->subscribers[i] = NULL;
          }
        }
        if (guard_conditions != NULL) {
          for (size_t i = 0; i < guard_conditions->guard_condition_count; i++) {
            guard_conditions->guard_conditions[i] = NULL;
          }
        }
        if (services != NULL) {
          for (size_t i = 0; i < services->service_count; i++) {
            services->services[i] = NULL;
          }
        }
        if (clients != NULL) {
          for (size_t i = 0; i < clients->client_count; i++) {
            clients->clients[i] = NULL;
          }
        }
        if (events != NULL) {
          for (size_t i = 0; i < events->event_count; i++) {
            events->events[i] = NULL;
          }
        }
        return RMW_RET_TIMEOUT;
      }
    }
    {
      struct timespec sleep_ts;
      sleep_ts.tv_sec = 0;
      sleep_ts.tv_nsec = 10 * 1000 * 1000L;
      (void)nanosleep(&sleep_ts, NULL);
    }
  }
}

rmw_ret_t rmw_publisher_event_init(
  rmw_event_t * rmw_event, const rmw_publisher_t * publisher,
  rmw_event_type_t event_type)
{
  (void)event_type;
  if (rmw_event == NULL || publisher == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publisher_event_init null arg");
  }
  // Unsupported: leave rmw_event untouched per RMW error-path rules.
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: events not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_subscription_event_init(
  rmw_event_t * rmw_event, const rmw_subscription_t * subscription,
  rmw_event_type_t event_type)
{
  (void)event_type;
  if (rmw_event == NULL || subscription == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_event_init null arg");
  }
  // Unsupported: leave rmw_event untouched per RMW error-path rules.
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: events not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_event_fini(rmw_event_t * event)
{
  if (event == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_event_fini null argument");
  }
  event->implementation_identifier = NULL;
  event->data = NULL;
  return RMW_RET_OK;
}

rmw_wait_set_t * rmw_create_waitset(rmw_context_t * context, size_t max_conditions)
{
  return rmw_create_wait_set(context, max_conditions);
}

rmw_ret_t rmw_destroy_waitset(rmw_wait_set_t * wait_set)
{
  return rmw_destroy_wait_set(wait_set);
}

rmw_ret_t rmw_get_gid_for_publisher(const rmw_publisher_t * publisher, rmw_gid_t * gid)
{
  // Bridge: 16 byte writer identity with domain in the first 4 bytes
  // little endian and the FFI publisher handle in the next 8 bytes,
  // mirroring dds WriterGUID width. Unique within one domain.
  const struct weld_pub_data * pd = NULL;
  uint64_t handle_u = 0;
  uint32_t domain_u = 0;
  size_t i = 0;
  if (publisher == NULL || gid == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_get_gid_for_publisher null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_get_gid_for_publisher wrong implementation");
  }
  pd = (const struct weld_pub_data *)publisher->data;
  if (pd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_gid_for_publisher no data");
    return RMW_RET_ERROR;
  }
  if (pd->domain > (size_t)UINT32_MAX) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_gid_for_publisher domain range");
    return RMW_RET_ERROR;
  }
  domain_u = (uint32_t)pd->domain;
  handle_u = (uint64_t)pd->handle;
  (void)memset(gid->data, 0, sizeof(gid->data));
  gid->data[0] = (uint8_t)(domain_u & 0xFFU);
  gid->data[1] = (uint8_t)((domain_u >> 8) & 0xFFU);
  gid->data[2] = (uint8_t)((domain_u >> 16) & 0xFFU);
  gid->data[3] = (uint8_t)((domain_u >> 24) & 0xFFU);
  for (i = 0; i < 8; i++) {
    gid->data[4 + i] = (uint8_t)((handle_u >> (8 * i)) & 0xFFU);
  }
  gid->implementation_identifier = kImplementationIdentifier;
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_count_matched_subscriptions(
  const rmw_publisher_t * publisher, size_t * subscription_count)
{
  // Bridge: Publisher.CountMatchedSubscriptions counts live subscriptions
  // in the same per-domain pubs plus subs registry on the same topic.
  // Closed subscriptions are excluded. No Go mapping exists yet.
  if (publisher == NULL || subscription_count == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_publisher_count_matched_subscriptions null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl(
      "rmw_weld: rmw_publisher_count_matched_subscriptions wrong implementation");
  }
  {
    const struct weld_pub_data * pd =
      (const struct weld_pub_data *)publisher->data;
    long long handle = 0;
    int n = 0;
    if (pd == NULL) {
      return weld_fail_invalid(
        "rmw_weld: rmw_publisher_count_matched_subscriptions no data");
    }
    handle = pd->handle;
    if (weld_ffi_pub_matched_subs == NULL) {
      return weld_fail_error(
        "rmw_weld: rmw_publisher_count_matched_subscriptions FFI not linked");
    }
    if (weld_ffi_pub_matched_subs(handle, &n) != 0) {
      return weld_fail_error(
        "rmw_weld: rmw_publisher_count_matched_subscriptions backend failed");
    }
    *subscription_count = (size_t)n;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_count_matched_publishers(
  const rmw_subscription_t * subscription, size_t * publisher_count)
{
  // Bridge: Subscription.CountMatchedPublishers counts live publishers in
  // the same per-domain registry on the same topic. Closed publishers are
  // excluded. No Go mapping exists yet.
  if (subscription == NULL || publisher_count == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_count_matched_publishers null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_count_matched_publishers wrong implementation");
  }
  {
    const struct weld_sub_data * sd =
      (const struct weld_sub_data *)subscription->data;
    long long handle = 0;
    int n = 0;
    if (sd == NULL) {
      return weld_fail_invalid(
        "rmw_weld: rmw_subscription_count_matched_publishers no data");
    }
    handle = sd->handle;
    if (weld_ffi_sub_matched_pubs == NULL) {
      return weld_fail_error(
        "rmw_weld: rmw_subscription_count_matched_publishers FFI not linked");
    }
    if (weld_ffi_sub_matched_pubs(handle, &n) != 0) {
      return weld_fail_error(
        "rmw_weld: rmw_subscription_count_matched_publishers backend failed");
    }
    *publisher_count = (size_t)n;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_get_actual_qos(
  const rmw_publisher_t * publisher, rmw_qos_profile_t * qos)
{
  // Actual QoS is the profile stored at creation. No Go mapping is
  // needed: the profile never changes after creation.
  if (publisher == NULL || qos == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publisher_get_actual_qos null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publisher_get_actual_qos wrong implementation");
  }
  {
    const struct weld_pub_data * pd =
      (const struct weld_pub_data *)publisher->data;
    if (pd == NULL) {
      return weld_fail_invalid("rmw_weld: rmw_publisher_get_actual_qos no data");
    }
    *qos = pd->qos;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_get_actual_qos(
  const rmw_subscription_t * subscription, rmw_qos_profile_t * qos)
{
  // Actual QoS is the profile stored at creation. No Go mapping is
  // needed: the profile never changes after creation.
  if (subscription == NULL || qos == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_subscription_get_actual_qos null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_subscription_get_actual_qos wrong implementation");
  }
  {
    const struct weld_sub_data * sd =
      (const struct weld_sub_data *)subscription->data;
    if (sd == NULL) {
      return weld_fail_invalid("rmw_weld: rmw_subscription_get_actual_qos no data");
    }
    *qos = sd->qos;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_service_server_is_available(
  const rmw_node_t * node, const rmw_client_t * client, bool * is_available)
{
  if (node == NULL || client == NULL || is_available == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_service_server_is_available null argument");
  }
  if (!weld_node_ok(node) || !weld_cli_ok(client)) {
    return weld_fail_impl("rmw_weld: rmw_service_server_is_available wrong implementation");
  }
  {
    const struct weld_rpc_state * st =
      (const struct weld_rpc_state *)client->data;
    long long bridge = 0;
    size_t local = 0;
    int remote = 0;
    if (st == NULL || st->name == NULL) {
      return weld_fail_invalid("rmw_weld: rmw_service_server_is_available no data");
    }
    // Local servers come from this process registry; clients share the
    // entry without touching the count.
    (void)pthread_mutex_lock(&g_weld_svc_mutex);
    for (struct weld_svc_shared * e = g_weld_svc_shared; e != NULL;
      e = e->next)
    {
      if (strcmp(e->name, st->name) == 0) {
        local = e->servers;
        break;
      }
    }
    (void)pthread_mutex_unlock(&g_weld_svc_mutex);
    if (weld_ffi_service_remote_available == NULL) {
      return weld_fail_error(
        "rmw_weld: rmw_service_server_is_available FFI not linked");
    }
    if (weld_ffi_bridge_borrow(&bridge) != RMW_RET_OK) {
      return weld_fail_error("rmw_weld: rmw_service_server_is_available no bridge");
    }
    if (weld_ffi_service_remote_available(bridge, st->name, &remote) != 0) {
      return weld_fail_error(
        "rmw_weld: rmw_service_server_is_available backend failed");
    }
    *is_available = (local > 0 || remote > 0);
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_service_request_subscription_get_actual_qos(
  const rmw_service_t * service, rmw_qos_profile_t * qos)
{
  if (service == NULL || qos == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_service_request_subscription_get_actual_qos null argument");
  }
  if (!weld_svc_ok(service)) {
    return weld_fail_impl(
      "rmw_weld: rmw_service_request_subscription_get_actual_qos wrong implementation");
  }
  {
    const struct weld_rpc_state * st =
      (const struct weld_rpc_state *)service->data;
    if (st == NULL) {
      return weld_fail_invalid(
        "rmw_weld: rmw_service_request_subscription_get_actual_qos no data");
    }
    *qos = st->qos;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_service_response_publisher_get_actual_qos(
  const rmw_service_t * service, rmw_qos_profile_t * qos)
{
  if (service == NULL || qos == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_service_response_publisher_get_actual_qos null argument");
  }
  if (!weld_svc_ok(service)) {
    return weld_fail_impl(
      "rmw_weld: rmw_service_response_publisher_get_actual_qos wrong implementation");
  }
  {
    const struct weld_rpc_state * st =
      (const struct weld_rpc_state *)service->data;
    if (st == NULL) {
      return weld_fail_invalid(
        "rmw_weld: rmw_service_response_publisher_get_actual_qos no data");
    }
    *qos = st->qos;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_client_request_publisher_get_actual_qos(
  const rmw_client_t * client, rmw_qos_profile_t * qos)
{
  if (client == NULL || qos == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_client_request_publisher_get_actual_qos null argument");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl(
      "rmw_weld: rmw_client_request_publisher_get_actual_qos wrong implementation");
  }
  {
    const struct weld_rpc_state * st =
      (const struct weld_rpc_state *)client->data;
    if (st == NULL) {
      return weld_fail_invalid(
        "rmw_weld: rmw_client_request_publisher_get_actual_qos no data");
    }
    *qos = st->qos;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_client_response_subscription_get_actual_qos(
  const rmw_client_t * client, rmw_qos_profile_t * qos)
{
  if (client == NULL || qos == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_client_response_subscription_get_actual_qos null argument");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl(
      "rmw_weld: rmw_client_response_subscription_get_actual_qos wrong implementation");
  }
  {
    const struct weld_rpc_state * st =
      (const struct weld_rpc_state *)client->data;
    if (st == NULL) {
      return weld_fail_invalid(
        "rmw_weld: rmw_client_response_subscription_get_actual_qos no data");
    }
    *qos = st->qos;
  }
  return RMW_RET_OK;
}

// QoS stub scope: this shim stores passthrough QoS and never contacts the
// Go core, so compatibility is decided locally on reliability plus
// durability only. Deadline, liveliness, history, and depth are ignored.
// Matching publishers and subscriptions always report zero counts, and
// get_actual_qos paths above return ERROR until the bridge is linked.
rmw_ret_t rmw_qos_profile_check_compatible(
  const rmw_qos_profile_t publisher_profile,
  const rmw_qos_profile_t subscription_profile,
  rmw_qos_compatibility_type_t * compatibility, char * reason, size_t reason_size)
{
  if (compatibility == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_qos_profile_check_compatible null argument");
  }
  if (publisher_profile.reliability != subscription_profile.reliability ||
    publisher_profile.durability != subscription_profile.durability)
  {
    *compatibility = RMW_QOS_COMPATIBILITY_WARNING;
    if (reason != NULL && reason_size > 0) {
      (void)snprintf(
        reason, reason_size,
        "rmw_weld: publisher/subscriber QoS reliability or durability differ");
      reason[reason_size - 1] = '\0';
    }
    return RMW_RET_OK;
  }
  *compatibility = RMW_QOS_COMPATIBILITY_OK;
  if (reason != NULL && reason_size > 0) {
    reason[0] = '\0';
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_serialize(
  const void * ros_message, const rosidl_message_type_support_t * type_support,
  rmw_serialized_message_t * serialized_message)
{
  // Registry concept: LookupType checks the message hash for type_support,
  // then CDR encode copies the serialized wire bytes. No output is written
  // until the Go bridge is linked.
  if (ros_message == NULL || type_support == NULL || serialized_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_serialize null argument");
  }
  return weld_unimplemented("rmw_serialize: type lookup plus CDR encode passthrough not linked yet");
}

rmw_ret_t rmw_deserialize(
  const rmw_serialized_message_t * serialized_message,
  const rosidl_message_type_support_t * type_support, void * ros_message)
{
  // Registry concept: LookupType checks the message hash for type_support,
  // then CDR decode copies the wire bytes into ros_message. No output is
  // written until the Go bridge is linked.
  if (serialized_message == NULL || type_support == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_deserialize null argument");
  }
  return weld_unimplemented("rmw_deserialize: type lookup plus CDR decode passthrough not linked yet");
}

rmw_ret_t rmw_get_serialized_message_size(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds, size_t * size)
{
  // Registry concept: the bound comes from the registered type description;
  // full_bounded plus is_plain select the fixed CDR size. No size is written
  // until the Go bridge is linked.
  if (type_support == NULL || message_bounds == NULL || size == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_get_serialized_message_size null argument");
  }
  return weld_unimplemented("rmw_get_serialized_message_size: type lookup not linked yet");
}

// Context finalize plus serialization format plus GID compare plus log
// severity plus liveliness plus ack plus allocation plus take variants
// plus client GID. Single process in memory shim, so every path validates
// only and never contacts the Go core except where the take helpers poll.

rmw_ret_t rmw_context_fini(rmw_context_t * context)
{
  if (context == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_context_fini null argument");
  }
  if (weld_context_ok(context)) {
    (void)memset(context, 0, sizeof(*context));
    return RMW_RET_OK;
  }
  if (context->implementation_identifier == NULL) {
    (void)memset(context, 0, sizeof(*context));
    return RMW_RET_OK;
  }
  return weld_fail_impl("rmw_weld: rmw_context_fini wrong implementation");
}

const char * rmw_get_serialization_format(void)
{
  return "cdr";
}

rmw_ret_t rmw_compare_gids_equal(
  const rmw_gid_t * gid1, const rmw_gid_t * gid2, bool * result)
{
  if (gid1 == NULL || gid2 == NULL || result == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_compare_gids_equal null argument");
  }
  if (gid1->implementation_identifier != kImplementationIdentifier ||
    gid2->implementation_identifier != kImplementationIdentifier)
  {
    return weld_fail_impl("rmw_weld: rmw_compare_gids_equal wrong implementation");
  }
  *result = memcmp(gid1->data, gid2->data, sizeof(gid1->data)) == 0;
  return RMW_RET_OK;
}

static rmw_log_severity_t g_weld_log_severity = RMW_LOG_SEVERITY_INFO;

rmw_ret_t rmw_set_log_severity(rmw_log_severity_t severity)
{
  g_weld_log_severity = severity;
  return RMW_RET_OK;
}

rmw_ret_t rmw_node_assert_liveliness(const rmw_node_t * node)
{
  if (node == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_node_assert_liveliness null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: rmw_node_assert_liveliness wrong implementation");
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_assert_liveliness(const rmw_publisher_t * publisher)
{
  if (publisher == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publisher_assert_liveliness null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publisher_assert_liveliness wrong implementation");
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_wait_for_all_acked(
  const rmw_publisher_t * publisher, rmw_time_t wait_timeout)
{
  if (publisher == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publisher_wait_for_all_acked null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publisher_wait_for_all_acked wrong implementation");
  }
  (void)wait_timeout;
  return RMW_RET_OK;
}

rmw_ret_t rmw_init_publisher_allocation(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds,
  rmw_publisher_allocation_t * allocation)
{
  if (type_support == NULL || message_bounds == NULL || allocation == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init_publisher_allocation null argument");
  }
  (void)type_support;
  (void)message_bounds;
  (void)memset(allocation, 0, sizeof(*allocation));
  return RMW_RET_OK;
}

rmw_ret_t rmw_fini_publisher_allocation(rmw_publisher_allocation_t * allocation)
{
  if (allocation == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_fini_publisher_allocation null argument");
  }
  (void)memset(allocation, 0, sizeof(*allocation));
  return RMW_RET_OK;
}

rmw_ret_t rmw_init_subscription_allocation(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds,
  rmw_subscription_allocation_t * allocation)
{
  if (type_support == NULL || message_bounds == NULL || allocation == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init_subscription_allocation null argument");
  }
  (void)type_support;
  (void)message_bounds;
  (void)memset(allocation, 0, sizeof(*allocation));
  return RMW_RET_OK;
}

rmw_ret_t rmw_fini_subscription_allocation(rmw_subscription_allocation_t * allocation)
{
  if (allocation == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_fini_subscription_allocation null argument");
  }
  (void)memset(allocation, 0, sizeof(*allocation));
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_allocation_init(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds,
  rmw_publisher_allocation_t * allocation)
{
  if (type_support == NULL || message_bounds == NULL || allocation == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publisher_allocation_init null argument");
  }
  (void)type_support;
  (void)message_bounds;
  (void)memset(allocation, 0, sizeof(*allocation));
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_allocation_init(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds,
  rmw_subscription_allocation_t * allocation)
{
  if (type_support == NULL || message_bounds == NULL || allocation == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_subscription_allocation_init null argument");
  }
  (void)type_support;
  (void)message_bounds;
  (void)memset(allocation, 0, sizeof(*allocation));
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_sequence(
  const rmw_subscription_t * subscription, size_t count,
  rmw_message_sequence_t * message_sequence,
  rmw_message_info_sequence_t * message_info_sequence, size_t * taken,
  rmw_subscription_allocation_t * allocation)
{
  struct weld_sub_data * sd = NULL;
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  size_t n = 0;
  size_t i = 0;
  (void)allocation;
  if (subscription == NULL || message_sequence == NULL ||
    message_info_sequence == NULL || taken == NULL)
  {
    return weld_fail_invalid("rmw_weld: rmw_take_sequence null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_take_sequence wrong implementation");
  }
  if (count == 0) {
    return weld_fail_invalid("rmw_weld: rmw_take_sequence zero count");
  }
  if (message_sequence->capacity < count ||
    message_info_sequence->capacity < count)
  {
    return weld_fail_invalid("rmw_weld: rmw_take_sequence capacity too small");
  }
  if (message_sequence->data == NULL || message_info_sequence->data == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_sequence null sequence data");
  }
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_sequence no subscription data");
    return RMW_RET_ERROR;
  }
  members = weld_resolve_members(sd->type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_sequence type support is not introspection");
    return RMW_RET_ERROR;
  }
  *taken = 0;
  message_sequence->size = 0;
  message_info_sequence->size = 0;
  for (i = 0; i < count; i++) {
    uint8_t * wire = NULL;
    size_t wire_len = 0;
    char errbuf[256];
    weld_cdr_status_t st;
    struct timespec now;
    uint64_t now_ns = 0;
    rmw_message_info_t * info = NULL;
    int empty = 0;
    if (message_sequence->data[i] == NULL) {
      *taken = n;
      message_sequence->size = n;
      message_info_sequence->size = n;
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_sequence null message slot");
      return RMW_RET_ERROR;
    }
    empty = weld_take_wire(sd, &wire, &wire_len);
    if (empty == 1) {
      break;
    }
    if (empty != 0) {
      *taken = n;
      message_sequence->size = n;
      message_info_sequence->size = n;
      return RMW_RET_ERROR;
    }
    errbuf[0] = '\0';
    st = weld_cdr_decode_members(
      members, wire, wire_len, message_sequence->data[i], errbuf, sizeof(errbuf));
    free(wire);
    if (st != WELD_CDR_OK) {
      *taken = n;
      message_sequence->size = n;
      message_info_sequence->size = n;
      rmw_reset_error();
      if (errbuf[0] != '\0') {
        RCUTILS_SET_ERROR_MSG(errbuf);
      } else {
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_sequence decode failed");
      }
      return RMW_RET_ERROR;
    }
    (void)clock_gettime(CLOCK_REALTIME, &now);
    now_ns = (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
    info = &message_info_sequence->data[n];
    info->source_timestamp = 0;
    info->received_timestamp = (rmw_time_point_value_t)now_ns;
    info->publication_sequence_number =
      RMW_MESSAGE_INFO_SEQUENCE_NUMBER_UNSUPPORTED;
    info->reception_sequence_number =
      RMW_MESSAGE_INFO_SEQUENCE_NUMBER_UNSUPPORTED;
    info->publisher_gid.implementation_identifier = kImplementationIdentifier;
    (void)memset(
      info->publisher_gid.data, 0, sizeof(info->publisher_gid.data));
    info->from_intra_process = false;
    n++;
  }
  *taken = n;
  message_sequence->size = n;
  message_info_sequence->size = n;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_serialized_message_with_info(
  const rmw_subscription_t * subscription,
  rmw_serialized_message_t * serialized_message, bool * taken,
  rmw_message_info_t * message_info, rmw_subscription_allocation_t * allocation)
{
  struct weld_sub_data * sd = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  int empty = 0;
  (void)allocation;
  if (subscription == NULL || serialized_message == NULL || taken == NULL ||
    message_info == NULL)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_take_serialized_message_with_info null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_take_serialized_message_with_info wrong implementation");
  }
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_serialized_message_with_info no data");
    return RMW_RET_ERROR;
  }
  empty = weld_take_wire(sd, &wire, &wire_len);
  if (empty == 1) {
    *taken = false;
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  if (wire_len > serialized_message->buffer_capacity) {
    rcutils_ret_t rc = rmw_serialized_message_resize(
      serialized_message, wire_len);
    if (rc != RCUTILS_RET_OK) {
      free(wire);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_take_serialized_message_with_info resize failed");
      return RMW_RET_BAD_ALLOC;
    }
  }
  if (wire_len > 0) {
    if (serialized_message->buffer == NULL) {
      free(wire);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_take_serialized_message_with_info null buffer");
      return RMW_RET_ERROR;
    }
    (void)memcpy(serialized_message->buffer, wire, wire_len);
  }
  free(wire);
  serialized_message->buffer_length = wire_len;
  (void)memset(message_info, 0, sizeof(*message_info));
  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_gid_for_client(const rmw_client_t * client, rmw_gid_t * gid)
{
  const struct weld_rpc_state * st = NULL;
  uint64_t handle_u = 0;
  uint32_t domain_u = 0;
  size_t i = 0;
  if (client == NULL || gid == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_get_gid_for_client null argument");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl("rmw_weld: rmw_get_gid_for_client wrong implementation");
  }
  st = (const struct weld_rpc_state *)client->data;
  if (st == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_gid_for_client no data");
    return RMW_RET_ERROR;
  }
  domain_u = 0;
  handle_u = (uint64_t)st->ffi_handle;
  (void)memset(gid->data, 0, sizeof(gid->data));
  gid->data[0] = (uint8_t)(domain_u & 0xFFU);
  gid->data[1] = (uint8_t)((domain_u >> 8) & 0xFFU);
  gid->data[2] = (uint8_t)((domain_u >> 16) & 0xFFU);
  gid->data[3] = (uint8_t)((domain_u >> 24) & 0xFFU);
  for (i = 0; i < 8; i++) {
    gid->data[4 + i] = (uint8_t)((handle_u >> (8 * i)) & 0xFFU);
  }
  gid->implementation_identifier = kImplementationIdentifier;
  return RMW_RET_OK;
}

// Graph extended plus loan plus event plus content filter plus network
// flow plus feature query.
//
// Graph snapshots merge the local node registry with the Go bridge
// registry. Per-node endpoint maps plus per-endpoint QoS plus GID are
// not tracked by this shim, so by-node queries return an empty
// names_and_types with RMW_RET_OK when the node exists and
// RMW_RET_NODE_NAME_NON_EXISTENT otherwise, and info-by-topic queries
// return an empty array with RMW_RET_OK. The empty snapshot documents
// the unknown choice: the transport copies bytes and never reports a
// best-known QoS, so callers must not infer absence of peers from size.
// Loans are never claimed: the transport copies once at ingress, so
// every loan path reports RMW_RET_UNSUPPORTED with NULL plus false
// outputs. Events are not produced, content filter C wiring is out of
// scope although the transport can filter, and network flow endpoints
// are not reported.

// Report whether a node name plus namespace is known locally or via FFI.
static bool weld_node_known(const char * name, const char * ns)
{
  long long bridge = 0;
  if (name == NULL || ns == NULL) {
    return false;
  }
  weld_graph_lock();
  for (struct weld_node_entry * e = g_weld_nodes; e != NULL; e = e->next) {
    const char * e_name = e->name != NULL ? e->name : "";
    const char * e_ns = e->namespace_ != NULL ? e->namespace_ : "";
    if (strcmp(e_name, name) == 0 && strcmp(e_ns, ns) == 0) {
      weld_graph_unlock();
      return true;
    }
  }
  weld_graph_unlock();
  bridge = weld_graph_bridge();
  if (bridge != 0 && weld_ffi_graph_node_count != NULL &&
    weld_ffi_graph_node_at != NULL)
  {
    long long n = 0;
    if (weld_ffi_graph_node_count(bridge, &n) == 0 && n > 0) {
      long long i = 0;
      for (i = 0; i < n; i++) {
        char name_buf[WELD_GRAPH_NAME_MAX];
        char ns_buf[WELD_GRAPH_NAME_MAX];
        long long domain = 0;
        int rc = weld_ffi_graph_node_at(
          bridge, i, name_buf, (int)sizeof(name_buf), ns_buf,
          (int)sizeof(ns_buf), &domain);
        if (rc != 0) {
          continue;
        }
        if (strcmp(name_buf, name) == 0 && strcmp(ns_buf, ns) == 0) {
          return true;
        }
      }
    }
  }
  return false;
}

// Fill an empty names_and_types snapshot for a known node.
static rmw_ret_t weld_by_node_empty(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace,
  rmw_names_and_types_t * out, const char * what)
{
  rmw_ret_t rc = RMW_RET_OK;
  if (node == NULL || allocator == NULL || node_name == NULL ||
    node_namespace == NULL || out == NULL)
  {
    return weld_fail_invalid("rmw_weld: by_node null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: by_node wrong implementation");
  }
  if (node_name[0] == '\0' || node_namespace[0] == '\0') {
    return weld_fail_invalid("rmw_weld: by_node empty name");
  }
#ifdef WELD_HAVE_NAMES_AND_TYPES
  if (rmw_names_and_types_check_zero(out) != RMW_RET_OK) {
    return weld_fail_invalid("rmw_weld: by_node not zero");
  }
  if (!weld_node_known(node_name, node_namespace)) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(what);
    return RMW_RET_NODE_NAME_NON_EXISTENT;
  }
  rc = rmw_names_and_types_init(out, 0, allocator);
  if (rc != RMW_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(what);
    return RMW_RET_BAD_ALLOC;
  }
  return RMW_RET_OK;
#else
  (void)rc;
  (void)what;
  (void)allocator;
  (void)out;
  return weld_unimplemented("rmw_weld: by_node names_and_types unavailable");
#endif
}

rmw_ret_t rmw_get_node_names_with_enclaves(
  const rmw_node_t * node, rcutils_string_array_t * node_names,
  rcutils_string_array_t * node_namespaces, rcutils_string_array_t * enclaves)
{
  rmw_ret_t rc = RMW_RET_OK;
  rcutils_allocator_t allocator = rcutils_get_default_allocator();
  if (node == NULL || node_names == NULL || node_namespaces == NULL ||
    enclaves == NULL)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_get_node_names_with_enclaves null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl(
      "rmw_weld: rmw_get_node_names_with_enclaves wrong implementation");
  }
  if (node_names->size != 0 || node_names->data != NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_get_node_names_with_enclaves names not zero");
  }
  if (node_namespaces->size != 0 || node_namespaces->data != NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_get_node_names_with_enclaves spaces not zero");
  }
  if (enclaves->size != 0 || enclaves->data != NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_get_node_names_with_enclaves enclaves not zero");
  }
  rc = rmw_get_node_names(node, node_names, node_namespaces);
  if (rc != RMW_RET_OK) {
    return rc;
  }
  if (node_names->size == 0) {
    return RMW_RET_OK;
  }
  if (node_names->size != node_namespaces->size) {
    rcutils_ret_t fini_rc = rcutils_string_array_fini(node_names);
    (void)fini_rc;
    fini_rc = rcutils_string_array_fini(node_namespaces);
    (void)fini_rc;
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_get_node_names_with_enclaves size mismatch");
    return RMW_RET_ERROR;
  }
  if (rcutils_string_array_init(
      enclaves, node_names->size, &allocator) != RCUTILS_RET_OK)
  {
    rcutils_ret_t fini_rc = rcutils_string_array_fini(node_names);
    (void)fini_rc;
    fini_rc = rcutils_string_array_fini(node_namespaces);
    (void)fini_rc;
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_get_node_names_with_enclaves alloc failed");
    return RMW_RET_BAD_ALLOC;
  }
  for (size_t i = 0; i < enclaves->size; i++) {
    enclaves->data[i] = weld_dup_str("/");
    if (enclaves->data[i] == NULL) {
      rcutils_ret_t fini_rc = rcutils_string_array_fini(node_names);
      (void)fini_rc;
      fini_rc = rcutils_string_array_fini(node_namespaces);
      (void)fini_rc;
      fini_rc = rcutils_string_array_fini(enclaves);
      (void)fini_rc;
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_get_node_names_with_enclaves alloc failed");
      return RMW_RET_BAD_ALLOC;
    }
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_publisher_names_and_types_by_node(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace, bool no_demangle,
  rmw_names_and_types_t * topic_names_and_types)
{
  (void)no_demangle;
  return weld_by_node_empty(
    node, allocator, node_name, node_namespace, topic_names_and_types,
    "rmw_weld: rmw_get_publisher_names_and_types_by_node unknown node");
}

rmw_ret_t rmw_get_subscriber_names_and_types_by_node(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace, bool no_demangle,
  rmw_names_and_types_t * topic_names_and_types)
{
  (void)no_demangle;
  return weld_by_node_empty(
    node, allocator, node_name, node_namespace, topic_names_and_types,
    "rmw_weld: rmw_get_subscriber_names_and_types_by_node unknown node");
}

rmw_ret_t rmw_get_service_names_and_types_by_node(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace,
  rmw_names_and_types_t * service_names_and_types)
{
  return weld_by_node_empty(
    node, allocator, node_name, node_namespace, service_names_and_types,
    "rmw_weld: rmw_get_service_names_and_types_by_node unknown node");
}

rmw_ret_t rmw_get_client_names_and_types_by_node(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace,
  rmw_names_and_types_t * service_names_and_types)
{
  return weld_by_node_empty(
    node, allocator, node_name, node_namespace, service_names_and_types,
    "rmw_weld: rmw_get_client_names_and_types_by_node unknown node");
}

// Fill an empty topic endpoint array. Unknown topics are allowed and
// report empty with RMW_RET_OK. Known topics also report empty because
// per-endpoint QoS plus GID are not tracked by this shim.
static rmw_ret_t weld_info_by_topic_empty(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * topic_name,
  rmw_topic_endpoint_info_array_t * out, const char * what)
{
  rmw_ret_t rc = RMW_RET_OK;
  if (node == NULL || allocator == NULL || topic_name == NULL || out == NULL) {
    return weld_fail_invalid("rmw_weld: info_by_topic null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: info_by_topic wrong implementation");
  }
  if (topic_name[0] == '\0') {
    return weld_fail_invalid("rmw_weld: info_by_topic empty topic");
  }
  if (rmw_topic_endpoint_info_array_check_zero(out) != RMW_RET_OK) {
    return weld_fail_invalid("rmw_weld: info_by_topic not zero");
  }
  (void)what;
  rc = rmw_topic_endpoint_info_array_init_with_size(out, 0, allocator);
  if (rc != RMW_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: info_by_topic init failed");
    return RMW_RET_BAD_ALLOC;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_publishers_info_by_topic(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * topic_name, bool no_mangle,
  rmw_topic_endpoint_info_array_t * publishers_info)
{
  (void)no_mangle;
  return weld_info_by_topic_empty(
    node, allocator, topic_name, publishers_info,
    "rmw_weld: rmw_get_publishers_info_by_topic");
}

rmw_ret_t rmw_get_subscriptions_info_by_topic(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * topic_name, bool no_mangle,
  rmw_topic_endpoint_info_array_t * subscriptions_info)
{
  (void)no_mangle;
  return weld_info_by_topic_empty(
    node, allocator, topic_name, subscriptions_info,
    "rmw_weld: rmw_get_subscriptions_info_by_topic");
}

// Weld forward-compat service endpoint queries.
//
// Jazzy RMW has no rmw_get_servers_info_by_service or
// rmw_get_clients_info_by_service, so these symbols are weld extensions
// with the topic info array shape. They validate like info-by-topic and
// return an empty array with RMW_RET_OK because per-endpoint service
// QoS plus GID are not tracked.
rmw_ret_t rmw_get_servers_info_by_service(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * service_name, rmw_topic_endpoint_info_array_t * servers_info)
{
  return weld_info_by_topic_empty(
    node, allocator, service_name, servers_info,
    "rmw_weld: rmw_get_servers_info_by_service");
}

rmw_ret_t rmw_get_clients_info_by_service(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * service_name, rmw_topic_endpoint_info_array_t * clients_info)
{
  return weld_info_by_topic_empty(
    node, allocator, service_name, clients_info,
    "rmw_weld: rmw_get_clients_info_by_service");
}

const rmw_guard_condition_t * rmw_node_get_graph_guard_condition(
  const rmw_node_t * node)
{
  struct weld_node_data * nd = NULL;
  rmw_guard_condition_t * guard = NULL;
  if (node == NULL) {
    (void)weld_fail_invalid(
      "rmw_weld: rmw_node_get_graph_guard_condition null argument");
    return NULL;
  }
  if (!weld_node_ok(node)) {
    (void)weld_fail_impl(
      "rmw_weld: rmw_node_get_graph_guard_condition wrong implementation");
    return NULL;
  }
  nd = (struct weld_node_data *)node->data;
  if (nd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_node_get_graph_guard_condition no node data");
    return NULL;
  }
  if (node->context == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_node_get_graph_guard_condition no context");
    return NULL;
  }
  weld_graph_lock();
  guard = nd->graph_guard;
  weld_graph_unlock();
  if (guard != NULL) {
    return guard;
  }
  guard = rmw_create_guard_condition(node->context);
  if (guard == NULL) {
    return NULL;
  }
  if (weld_graph_guard_add(guard) != 0) {
    {
      rmw_ret_t fini_rc = rmw_destroy_guard_condition(guard);
      (void)fini_rc;
    }
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_node_get_graph_guard_condition no memory");
    return NULL;
  }
  weld_graph_lock();
  if (nd->graph_guard == NULL) {
    nd->graph_guard = guard;
  } else {
    weld_graph_unlock();
    weld_graph_guard_remove(guard);
    {
      rmw_ret_t fini_rc = rmw_destroy_guard_condition(guard);
      (void)fini_rc;
    }
    weld_graph_lock();
    guard = nd->graph_guard;
    weld_graph_unlock();
    return guard;
  }
  weld_graph_unlock();
  return guard;
}

// Loan family: the transport copies once at ingress and shares by
// reference internally, so no middleware loan is ever handed out.
// Every path validates per the RMW header and reports
// RMW_RET_UNSUPPORTED without claiming a loan.

rmw_ret_t rmw_borrow_loaned_message(
  const rmw_publisher_t * publisher,
  const rosidl_message_type_support_t * type_support, void ** ros_message)
{
  if (publisher == NULL || type_support == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_borrow_loaned_message null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_borrow_loaned_message wrong impl");
  }
  if (*ros_message != NULL) {
    return weld_fail_invalid("rmw_weld: rmw_borrow_loaned_message not null");
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: loaned messages not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_publish_loaned_message(
  const rmw_publisher_t * publisher, void * ros_message,
  rmw_publisher_allocation_t * allocation)
{
  (void)allocation;
  if (publisher == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publish_loaned_message null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publish_loaned_message wrong impl");
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: loaned messages not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_take_loaned_message(
  const rmw_subscription_t * subscription, void ** loaned_message,
  bool * taken, rmw_subscription_allocation_t * allocation)
{
  (void)allocation;
  if (subscription == NULL || loaned_message == NULL || taken == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_loaned_message null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_take_loaned_message wrong impl");
  }
  if (*loaned_message != NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_loaned_message not null");
  }
  *loaned_message = NULL;
  *taken = false;
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: loaned messages not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_take_loaned_message_with_info(
  const rmw_subscription_t * subscription, void ** loaned_message,
  bool * taken, rmw_message_info_t * message_info,
  rmw_subscription_allocation_t * allocation)
{
  (void)allocation;
  if (subscription == NULL || loaned_message == NULL || taken == NULL ||
    message_info == NULL)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_take_loaned_message_with_info null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_take_loaned_message_with_info wrong impl");
  }
  if (*loaned_message != NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_take_loaned_message_with_info not null");
  }
  *loaned_message = NULL;
  *taken = false;
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: loaned messages not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_return_loaned_message_from_publisher(
  const rmw_publisher_t * publisher, void * loaned_message)
{
  if (publisher == NULL || loaned_message == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_return_loaned_message_from_publisher null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl(
      "rmw_weld: rmw_return_loaned_message_from_publisher wrong impl");
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: loaned messages not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_return_loaned_message_from_subscription(
  const rmw_subscription_t * subscription, void * loaned_message)
{
  if (subscription == NULL || loaned_message == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_return_loaned_message_from_subscription null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_return_loaned_message_from_subscription wrong impl");
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: loaned messages not supported");
  return RMW_RET_UNSUPPORTED;
}

// On-new callbacks: stored on the endpoint for polling callers. The shim
// has no background dispatch thread, so a stored callback is never
// invoked by the middleware. Clearing with NULL is allowed.

rmw_ret_t rmw_subscription_set_on_new_message_callback(
  rmw_subscription_t * subscription, rmw_event_callback_t callback,
  const void * user_data)
{
  struct weld_sub_data * sd = NULL;
  if (subscription == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_set_on_new_message_callback null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_set_on_new_message_callback wrong impl");
  }
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_on_new_message_callback no data");
    return RMW_RET_ERROR;
  }
  sd->on_new_callback = callback;
  sd->on_new_user_data = user_data;
  return RMW_RET_OK;
}

rmw_ret_t rmw_service_set_on_new_request_callback(
  rmw_service_t * service, rmw_event_callback_t callback,
  const void * user_data)
{
  struct weld_rpc_state * st = NULL;
  if (service == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_service_set_on_new_request_callback null arg");
  }
  if (!weld_svc_ok(service)) {
    return weld_fail_impl(
      "rmw_weld: rmw_service_set_on_new_request_callback wrong impl");
  }
  st = (struct weld_rpc_state *)service->data;
  if (st == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_service_set_on_new_request_callback no data");
    return RMW_RET_ERROR;
  }
  (void)pthread_mutex_lock(&st->mutex);
  st->on_new_callback = callback;
  st->on_new_user_data = user_data;
  (void)pthread_mutex_unlock(&st->mutex);
  return RMW_RET_OK;
}

rmw_ret_t rmw_client_set_on_new_response_callback(
  rmw_client_t * client, rmw_event_callback_t callback,
  const void * user_data)
{
  struct weld_rpc_state * st = NULL;
  if (client == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_client_set_on_new_response_callback null arg");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl(
      "rmw_weld: rmw_client_set_on_new_response_callback wrong impl");
  }
  st = (struct weld_rpc_state *)client->data;
  if (st == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_client_set_on_new_response_callback no data");
    return RMW_RET_ERROR;
  }
  (void)pthread_mutex_lock(&st->mutex);
  st->on_new_callback = callback;
  st->on_new_user_data = user_data;
  (void)pthread_mutex_unlock(&st->mutex);
  return RMW_RET_OK;
}

// Events: no event source exists in this shim, so init reports
// UNSUPPORTED, take reports UNSUPPORTED with taken false, set_callback
// reports UNSUPPORTED, and no type is ever supported. Fini clears the
// handle and reports OK so a zero-initialized event stays finalizable.

rmw_ret_t rmw_take_event(
  const rmw_event_t * event_handle, void * event_info, bool * taken)
{
  if (event_handle == NULL || event_info == NULL || taken == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_event null argument");
  }
  if (event_handle->implementation_identifier != kImplementationIdentifier) {
    return weld_fail_impl("rmw_weld: rmw_take_event wrong implementation");
  }
  *taken = false;
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: events not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_event_set_callback(
  rmw_event_t * event, rmw_event_callback_t callback, const void * user_data)
{
  (void)callback;
  (void)user_data;
  if (event == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_event_set_callback null argument");
  }
  if (event->implementation_identifier != kImplementationIdentifier) {
    return weld_fail_impl("rmw_weld: rmw_event_set_callback wrong impl");
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: events not supported");
  return RMW_RET_UNSUPPORTED;
}

bool rmw_event_type_is_supported(rmw_event_type_t rmw_event_type)
{
  (void)rmw_event_type;
  return false;
}

// Dynamic message take: the CDR path requires static introspection
// members, so deferred dynamic types are out of scope. Humble predates
// the dynamic typesupport API entirely, so these entry points exist only
// where rmw/dynamic_message_type_support.h does.
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")

rmw_ret_t rmw_take_dynamic_message(
  const rmw_subscription_t * subscription,
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message, bool * taken,
  rmw_subscription_allocation_t * allocation)
{
  (void)allocation;
  if (subscription == NULL || dynamic_message == NULL || taken == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_dynamic_message null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_take_dynamic_message wrong impl");
  }
  *taken = false;
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: dynamic messages not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_take_dynamic_message_with_info(
  const rmw_subscription_t * subscription,
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message, bool * taken,
  rmw_message_info_t * message_info, rmw_subscription_allocation_t * allocation)
{
  (void)allocation;
  if (subscription == NULL || dynamic_message == NULL || taken == NULL ||
    message_info == NULL)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_take_dynamic_message_with_info null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_take_dynamic_message_with_info wrong impl");
  }
  *taken = false;
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: dynamic messages not supported");
  return RMW_RET_UNSUPPORTED;
}

#endif /* dynamic message typesupport available */

// Content filters: the transport can filter, but no C wiring exists yet
// to attach an expression to a subscription, so both paths report
// UNSUPPORTED loudly and leave outputs untouched.

rmw_ret_t rmw_subscription_set_content_filter(
  rmw_subscription_t * subscription,
  const rmw_subscription_content_filter_options_t * options)
{
  if (subscription == NULL || options == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_set_content_filter null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_set_content_filter wrong impl");
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(
    "rmw_weld: content filters not supported in C wiring yet");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_subscription_get_content_filter(
  const rmw_subscription_t * subscription, rcutils_allocator_t * allocator,
  rmw_subscription_content_filter_options_t * options)
{
  if (subscription == NULL || allocator == NULL || options == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_get_content_filter null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_get_content_filter wrong impl");
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(
    "rmw_weld: content filters not supported in C wiring yet");
  return RMW_RET_UNSUPPORTED;
}

// Network flow endpoints are not reported by this transport.

rmw_ret_t rmw_publisher_get_network_flow_endpoints(
  const rmw_publisher_t * publisher, rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * network_flow_endpoint_array)
{
  if (publisher == NULL || allocator == NULL ||
    network_flow_endpoint_array == NULL)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_publisher_get_network_flow_endpoints null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl(
      "rmw_weld: rmw_publisher_get_network_flow_endpoints wrong impl");
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: network flow endpoints not supported");
  return RMW_RET_UNSUPPORTED;
}

rmw_ret_t rmw_subscription_get_network_flow_endpoints(
  const rmw_subscription_t * subscription, rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * network_flow_endpoint_array)
{
  if (subscription == NULL || allocator == NULL ||
    network_flow_endpoint_array == NULL)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_get_network_flow_endpoints null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_get_network_flow_endpoints wrong impl");
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: network flow endpoints not supported");
  return RMW_RET_UNSUPPORTED;
}

// Feature query: no middleware optional feature is supported. QoS
// compatibility itself is real through rmw_qos_profile_check_compatible,
// but rmw_feature_supported only covers the rmw_feature_t enum and
// stays false for every member.
bool rmw_feature_supported(rmw_feature_t feature)
{
  (void)feature;
  return false;
}
