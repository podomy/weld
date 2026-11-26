// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//
// rmw_weld: ROS middleware shim forwarding RMW calls to the Weld Go core.
// Backend contract: internal/rmwbridge/bridge.go (identifier "rmw_weld")
// plus internal/transports/transport.go Backend (PublishBytes, TakeBytes,
// JoinDiscovery, ReportLiveliness, Close). Every entry point below
// forwards to the Go core over the weld_ffi bridge, validates its
// arguments, and fails loudly with RMW_RET_ERROR, RMW_RET_INVALID_ARGUMENT,
// RMW_RET_INCORRECT_RMW_IMPLEMENTATION, or RMW_RET_UNSUPPORTED through
// rmw_reset_error plus RCUTILS_SET_ERROR_MSG instead of crashing.
// Loans are single-owner heap buffers: zero copy in process by pointer,
// copying over the wire by serialization.

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#if defined(__has_include) && __has_include(<dlfcn.h>)
#include <dlfcn.h>
#define WELD_HAVE_DLOPEN 1
#endif

#include "rcutils/allocator.h"
#include "rcutils/error_handling.h"
#include "rcutils/types/string_array.h"
#include "rcutils/types/uint8_array.h"
#include "rmw/domain_id.h"
#include "rmw/error_handling.h"
#include "rmw/rmw.h"
#include "rosidl_typesupport_introspection_c/identifier.h"
#include "rosidl_typesupport_introspection_c/service_introspection.h"
#include "rosidl_runtime_c/string.h"
#include "rosidl_runtime_c/message_initialization.h"
#if defined(__has_include)
#if __has_include("rosidl_runtime_c/type_hash.h")
#include "rosidl_runtime_c/type_hash.h"
#define WELD_HAVE_TYPE_HASH 1
#endif
#endif
#if defined(__has_include)
#if __has_include("rosidl_dynamic_typesupport/dynamic_message_type_support_struct.h")
#include "rosidl_dynamic_typesupport/dynamic_message_type_support_struct.h"
#include "rosidl_dynamic_typesupport/identifier.h"
#endif
#if __has_include("rosidl_dynamic_typesupport/api/dynamic_data.h")
#include "rosidl_dynamic_typesupport/api/dynamic_data.h"
#endif
#endif
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
#if __has_include("rmw/events_statuses/liveliness_changed.h")
#include "rmw/events_statuses/liveliness_changed.h"
#define WELD_HAVE_LIVELINESS_CHANGED 1
#endif
#if __has_include("rmw/events_statuses/liveliness_lost.h")
#include "rmw/events_statuses/liveliness_lost.h"
#define WELD_HAVE_LIVELINESS_LOST 1
#endif
#if __has_include("rmw/events_statuses/message_lost.h")
#include "rmw/events_statuses/message_lost.h"
#define WELD_HAVE_MESSAGE_LOST 1
#endif
#if __has_include("rmw/events_statuses/offered_deadline_missed.h")
#include "rmw/events_statuses/offered_deadline_missed.h"
#define WELD_HAVE_OFFERED_DEADLINE 1
#endif
#if __has_include("rmw/events_statuses/requested_deadline_missed.h")
#include "rmw/events_statuses/requested_deadline_missed.h"
#define WELD_HAVE_REQUESTED_DEADLINE 1
#endif
#if __has_include("rmw/events_statuses/incompatible_qos.h")
#include "rmw/events_statuses/incompatible_qos.h"
#define WELD_HAVE_INCOMPAT_QOS 1
#endif
#if __has_include("rmw/qos_policy_kind.h")
#include "rmw/qos_policy_kind.h"
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

// In-process loan handoff for zero-copy delivery (see the loan family).
//
// loan_addr carries the opaque C message pointer and members_addr carries
// the opaque C introspection members pointer; the Go bridge never
// dereferences either and moves only the handle values. Both stay weak so
// an older Go library without the loan queue falls back to the copying
// wire path instead of crashing: a NULL address means no loan support.
extern int weld_ffi_loan_publish(
  long long handle, unsigned long long loan_addr,
  unsigned long long members_addr) __attribute__((weak));
extern int weld_ffi_loan_take_poll(
  long long handle, unsigned long long * loan_addr_out,
  unsigned long long * members_addr_out) __attribute__((weak));

// Take attribution side channel from the Go core (weld_ffi_sub_take_info).
//
// The poll ABI stays frozen, so the sequence plus writer GID ride beside
// it: the C take path polls this right after a successful take_poll for
// the same handle. out_seq receives the per-subscription receive
// sequence starting at 1; gid_buf receives the 16 byte writer GID
// without a terminator. It stays weak so an older Go library degrades to
// unsupported sequences plus a zero GID instead of crashing.
extern int weld_ffi_sub_take_info(
  long long handle, unsigned long long * out_seq, char * gid_buf, int gid_len)
  __attribute__((weak));

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

extern int weld_ffi_sub_has_data(long long handle)
  __attribute__((weak));

// Subscription dynamic type report in slash form (such as
// std_msgs/msg/String) plus the RIHS hash. The Go side normalizes the
// name to IDL form and declares the reader. Weak for the same
// old-library reason as the static form.
extern int weld_ffi_sub_set_dynamic_type(
  long long handle, const char * type_name, const char * type_hash)
  __attribute__((weak));

// Content filter wiring matches the
// internal/rmwbridge/weldffi/filter.go export. It stays weak so the
// filter path fails loudly when the Go core is not linked instead of
// crashing. params carries the NUL joined parameter blob with nparams
// entries.
extern int weld_ffi_sub_set_content_filter(
  long long handle, const char * expr, const char * params, int nparams)
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
  // Owning node name for liveliness asserts. The publisher carries no
  // other node reference, so this copy names the asserting participant.
  char * node_name;
  size_t domain;
  rmw_qos_profile_t qos;
  // Local incompatible QoS count ever observed at match time against
  // same-topic subscriptions. Bumped at endpoint creation on both sides
  // of a mismatched pair via rmw_qos_profile_check_compatible; wire
  // mismatches add through the FFI incompat totals at take time.
  size_t incompat_total;
};

struct weld_sub_data
{
  long long handle;
  const rosidl_message_type_support_t * type_support;
  char * topic;
  size_t domain;
  rmw_qos_profile_t qos;
  // Local incompatible QoS count ever observed at match time, mirroring
  // the publisher side. Bumped at creation on both sides of a mismatched
  // local pair; wire mismatches add through FFI totals at take time.
  size_t incompat_total;
  rmw_event_callback_t on_new_callback;
  const void * on_new_user_data;
  // Callback dispatch thread started on the first non-NULL on-new
  // callback. It polls weld_ffi_sub_has_data and invokes the stored
  // callback on empty to non-empty edges without consuming, so takes
  // still deliver every sample. The wait stash stays executor-thread
  // owned and is never touched here; only these fields are shared,
  // always under dispatch_mutex.
  pthread_t dispatch_thread;
  pthread_mutex_t dispatch_mutex;
  bool dispatch_run;
  bool dispatch_started;
  bool dispatch_fired;
  // Content filter expression plus parameters stored by
  // rmw_subscription_set_content_filter for
  // rmw_subscription_get_content_filter. NULL means no filter is set.
  // The take path applies it inside the Go backend.
  char * filter_expr;
  char ** filter_params;
  size_t filter_nparams;
  // Wait stash holds one wire sample consumed by rmw_wait so the next
  // rmw_take returns it instead of polling again. Wait and take run on
  // the same thread in rcl executors, so no lock is needed. wait_seq
  // plus wait_gid park the attribution observed at poll time so the
  // consuming take fills the same info.
  uint8_t * wait_buf;
  size_t wait_len;
  bool has_wait;
  unsigned long long wait_seq;
  uint8_t wait_gid[16];
  // Dynamic type identity resolved at create time from the runtime
  // type description (no static introspection handle). The slash form
  // name plus RIHS hash stay alive for the subscription lifetime and
  // are freed in rmw_destroy_subscription. NULL for static subs.
  bool is_dynamic;
  char * dyn_type_name;
  char * dyn_type_hash;
};

// Size of one RIHS type hash string with the NUL terminator.
enum
{
  WELD_TYPE_HASH_MAX = 128
};

// Dispatch polling period in nanoseconds. One millisecond keeps
// callback latency low without hot spinning while no data arrives.
#define WELD_DISPATCH_POLL_NS 1000000L

// Local endpoint registry kinds for by-node plus info-by-topic queries:
// 0 publisher, 1 subscription, 2 service server, 3 service client.
enum
{
  WELD_EP_PUB = 0,
  WELD_EP_SUB = 1,
  WELD_EP_SVC = 2,
  WELD_EP_CLI = 3
};

// Remote endpoint roles mirror the weldRole constants in
// internal/rmwbridge/weldffi/missing.go. Writers feed publishers info,
// readers feed subscriptions info, servers plus clients feed the
// service info queries.
enum
{
  WELD_FILTER_ROLE_WRITER = 0,
  WELD_FILTER_ROLE_READER = 1,
  WELD_FILTER_ROLE_SERVER = 2,
  WELD_FILTER_ROLE_CLIENT = 3
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

// Look up the RIHS type hash for a message type support handle.
//
// Uses the handle get_type_hash_func hook when the rosidl runtime
// provides it (Jazzy and newer) and formats the result as RIHS01 hex.
// Returns 0 with the hash in buf, -1 when unknown: Humble has no type
// hash support, or the handle or hook is missing, or the hash has no
// version. An unknown hash keeps the empty hashless wildcard announce.
static int weld_type_hash(
  const rosidl_message_type_support_t * type_support,
  char * buf, size_t buflen)
{
#if defined(WELD_HAVE_TYPE_HASH)
  const rosidl_type_hash_t * hash = NULL;
  size_t pos = 0;
  size_t i = 0;
  int n = 0;
  if (type_support == NULL || buf == NULL || buflen == 0) {
    return -1;
  }
  if (type_support->get_type_hash_func == NULL) {
    return -1;
  }
  hash = type_support->get_type_hash_func(type_support);
  if (hash == NULL || hash->version == 0) {
    return -1;
  }
  n = snprintf(buf, buflen, "RIHS%02u_", (unsigned)hash->version);
  if (n < 0 || (size_t)n >= buflen) {
    return -1;
  }
  pos = (size_t)n;
  for (i = 0; i < sizeof(hash->value); i++) {
    if (pos + 3 > buflen) {
      buf[0] = '\0';
      return -1;
    }
    n = snprintf(buf + pos, buflen - pos, "%02x", (unsigned)hash->value[i]);
    if (n != 2) {
      buf[0] = '\0';
      return -1;
    }
    pos += 2;
  }
  return 0;
#else
  (void)type_support;
  (void)buf;
  (void)buflen;
  return -1;
#endif
}

// Report the ROS type name for a fresh pub handle.
//
// The hash carries the RIHS string when the rosidl runtime provides it
// and stays empty for hashless wildcard interop otherwise. A missing
// symbol (old Go library) or an unusable name skips silently so
// creation still succeeds.
static void weld_report_type(
  long long handle,
  const rosidl_message_type_support_t * type_support)
{
  char name[512];
  char hash[WELD_TYPE_HASH_MAX];
  const char * hasharg = "";
  if (weld_ffi_pub_set_type == NULL) {
    return;
  }
  if (weld_type_name(weld_resolve_members(type_support), name,
    sizeof(name)) != 0)
  {
    return;
  }
  if (weld_type_hash(type_support, hash, sizeof(hash)) == 0) {
    hasharg = hash;
  }
  (void)weld_ffi_pub_set_type(handle, name, hasharg);
}

// weld_report_sub_type reports a subscription type the same way the
// publisher form does. Call sites check for NULL and skip silently when
// the Go library predates the symbol.
static void weld_report_sub_type(
  long long handle,
  const rosidl_message_type_support_t * type_support)
{
  char name[512];
  char hash[WELD_TYPE_HASH_MAX];
  const char * hasharg = "";
  if (weld_ffi_sub_set_type == NULL) {
    return;
  }
  if (weld_type_name(weld_resolve_members(type_support), name,
    sizeof(name)) != 0)
  {
    return;
  }
  if (weld_type_hash(type_support, hash, sizeof(hash)) == 0) {
    hasharg = hash;
  }
  (void)weld_ffi_sub_set_type(handle, name, hasharg);
}

// Report the runtime dynamic type to the Go backend.
// A missing symbol (old Go library) skips silently. Only defined where
// the dynamic typesupport API exists; otherwise the report has no
// caller and a static definition would warn as unused.
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")
static void weld_report_dynamic_sub_type(
  long long handle, const char * type_name, const char * type_hash)
{
  if (weld_ffi_sub_set_dynamic_type == NULL) {
    return;
  }
  if (type_name == NULL || type_hash == NULL) {
    return;
  }
  (void)weld_ffi_sub_set_dynamic_type(handle, type_name, type_hash);
}
#endif

// Read the take attribution for the last delivered sample.
//
// The C take path calls this right after a successful take poll for the
// same handle. Returns 0 with the sequence plus GID, or -1 with both
// zeroed when the side channel is missing, fails, or reports no take
// yet. A zero sequence means unsupported to the fill below.
static int weld_take_info(
  long long handle, unsigned long long * seq_out, uint8_t gid_out[16])
{
  unsigned long long seq = 0;
  int rc = 0;
  if (seq_out == NULL || gid_out == NULL) {
    return -1;
  }
  *seq_out = 0;
  (void)memset(gid_out, 0, 16);
  if (weld_ffi_sub_take_info == NULL) {
    return -1;
  }
  rc = weld_ffi_sub_take_info(handle, &seq, (char *)gid_out, 16);
  if (rc != 0 || seq == 0) {
    *seq_out = 0;
    (void)memset(gid_out, 0, 16);
    return -1;
  }
  *seq_out = seq;
  return 0;
}

// Poll one payload without blocking.
//
// Returns 0 with malloc owned bytes on data, 1 when no data is ready, and
// -1 with the rmw error set on failure. A short buffer is grown and
// retried internally because the FFI layer stashes the oversized sample.
// On success seq_out plus gid_out carry the delivered attribution when
// non-NULL (zeroed when the side channel is missing); they are untouched
// on empty or failure.
static int weld_poll_payload(
  long long handle, uint8_t ** out, size_t * out_len,
  unsigned long long * seq_out, uint8_t gid_out[16])
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
      if (seq_out != NULL) {
        uint8_t gid[16];
        unsigned long long seq = 0;
        (void)weld_take_info(handle, &seq, gid);
        *seq_out = seq;
        if (gid_out != NULL) {
          (void)memcpy(gid_out, gid, sizeof(gid));
        }
      }
      return 0;
    }
    if (seq_out != NULL) {
      uint8_t gid[16];
      unsigned long long seq = 0;
      (void)weld_take_info(handle, &seq, gid);
      *seq_out = seq;
      if (gid_out != NULL) {
        (void)memcpy(gid_out, gid, sizeof(gid));
      }
    }
    *out = buf;
    *out_len = (size_t)n;
    return 0;
  }
}

// Take a stashed wait sample if present.
//
// Wait consumes one sample to prove readiness and parks it here so the
// next take returns the same bytes plus the same attribution instead of
// polling again. Returns true with malloc owned bytes on hit, false
// when no stash exists.
static bool weld_sub_consume_wait(
  struct weld_sub_data * sd, uint8_t ** out, size_t * out_len,
  unsigned long long * seq_out, uint8_t gid_out[16])
{
  if (sd == NULL || out == NULL || out_len == NULL) {
    return false;
  }
  if (!sd->has_wait) {
    return false;
  }
  *out = sd->wait_buf;
  *out_len = sd->wait_len;
  if (seq_out != NULL) {
    *seq_out = sd->wait_seq;
  }
  if (gid_out != NULL) {
    (void)memcpy(gid_out, sd->wait_gid, sizeof(sd->wait_gid));
  }
  sd->wait_buf = NULL;
  sd->wait_len = 0;
  sd->has_wait = false;
  sd->wait_seq = 0;
  (void)memset(sd->wait_gid, 0, sizeof(sd->wait_gid));
  return true;
}

// Take one wire sample, preferring the wait stash.
//
// Wait parks one sample per subscription so readiness survives until
// take. This helper returns the stashed sample first and polls only
// when no stash exists, keeping wait plus take paired. Returns 0 with
// malloc owned bytes on data (zero bytes allowed as empty sample),
// 1 when no data is ready, and -1 with the rmw error set on failure.
// On success seq_out plus gid_out carry the delivered attribution when
// non-NULL.
static int weld_take_wire(
  struct weld_sub_data * sd, uint8_t ** out, size_t * out_len,
  unsigned long long * seq_out, uint8_t gid_out[16])
{
  if (sd == NULL || out == NULL || out_len == NULL) {
    return -1;
  }
  if (weld_sub_consume_wait(sd, out, out_len, seq_out, gid_out)) {
    return 0;
  }
  return weld_poll_payload(sd->handle, out, out_len, seq_out, gid_out);
}

// Poll one subscription for wait readiness without losing data.
//
// A stash hit reports ready immediately. Otherwise one non-blocking
// poll runs; data plus its attribution is parked in the subscription
// stash and reports ready, empty reports not ready, and poll errors
// report not ready so wait keeps polling until the budget expires and
// take can report the error later.
static bool weld_sub_wait_poll(struct weld_sub_data * sd)
{
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  unsigned long long seq = 0;
  uint8_t gid[16];
  int empty = 0;
  if (sd == NULL) {
    return false;
  }
  if (sd->has_wait) {
    return true;
  }
  (void)memset(gid, 0, sizeof(gid));
  empty = weld_poll_payload(sd->handle, &wire, &wire_len, &seq, gid);
  if (empty != 0) {
    return false;
  }
  // Empty sample (zero bytes) still counts as data ready: the take
  // path delivers zero bytes with taken true.
  sd->wait_buf = wire;
  sd->wait_len = wire_len;
  sd->has_wait = true;
  sd->wait_seq = seq;
  (void)memcpy(sd->wait_gid, gid, sizeof(sd->wait_gid));
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

static rmw_ret_t weld_fail_unsupported(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_UNSUPPORTED;
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

// Forward declarations: the endpoint registry is defined beside the
// graph queries below, but creation paths above register into it.
struct weld_rpc_state;
static void weld_ep_register(
  int kind, const char * node_name, const char * node_ns,
  size_t domain, const void * data);
static void weld_ep_unregister(const void * data);
// Snapshot helpers for match-time incompat counting, defined beside the
// registry. The scan compares the new endpoint QoS against existing
// opposite endpoints and bumps both sides on mismatch.
struct weld_ep_snap;
static size_t weld_incompat_scan(
  int new_kind, const rmw_qos_profile_t * new_qos, const char * topic,
  size_t domain);
static rmw_ret_t weld_ep_snapshot(
  const char * name, int kind, size_t domain, bool use_domain,
  struct weld_ep_snap ** out_items, size_t * out_n);
static void weld_ep_snapshot_fini(struct weld_ep_snap * items, size_t n);

// Liveliness generation: a process-wide monotonic counter. Repeating a
// generation with the same alive value is idempotent; any other reuse
// is stale, so every assert consumes a fresh value.
static _Atomic unsigned long long g_weld_liveliness_gen = 1;

// Loan family: single-owner in-process zero copy with a copying wire
// fallback. True cross-language shared memory would need an shm arena,
// which is out of scope, so zero copy is kept where the architecture
// allows it: same-process pub to sub via the bridge without the wire.
//
// Borrow allocates plus inits a message and records it per publisher.
// Publish encodes plus publishes the wire bytes first (fan out to every
// matched subscription, copying), then enqueues the loaned buffer by
// pointer for one zero-copy handoff; the first loaned take dequeues the
// same pointer with no encode, decode, or payload copy. Take prefers the
// queued handle and falls back to pulling one wire sample (wait stash
// first via weld_take_wire) plus decoding it into a fresh buffer, which
// copies. Return releases the buffer. Publishing or returning a foreign
// or stale pointer fails loudly instead of freeing wild memory. Released
// buffers are fini plus freed, so strings and sequences inside do not
// leak. The queued handle is freed exactly once by its taker's return;
// a second return fails because the record is already unlinked. Callers
// must publish or return every loan before destroying the endpoint;
// entries for a destroyed endpoint are never reclaimed. Loans are single
// owner and not thread safe for concurrent use of the same buffer. Do not
// mix loaned and regular takes on one subscription for one loaned
// publish: the loaned take discards its own wire duplicate, but a regular
// take first leaves the queued handle for a later loaned take to consume
// as a second delivery.

struct weld_loan_entry
{
  const void * endpoint;
  void * message;
  const rosidl_typesupport_introspection_c__MessageMembers * members;
  struct weld_loan_entry * next;
};

static pthread_mutex_t g_weld_loan_mutex = PTHREAD_MUTEX_INITIALIZER;

static struct weld_loan_entry * g_weld_pub_loans = NULL;

static struct weld_loan_entry * g_weld_sub_loans = NULL;

static size_t g_weld_decode_drops = 0;

// Add one outstanding loan to the list. Returns 0, or -1 on bad argument
// or alloc failure.
static int weld_loan_add(
  struct weld_loan_entry ** list, const void * endpoint, void * message,
  const rosidl_typesupport_introspection_c__MessageMembers * members)
{
  struct weld_loan_entry * e = NULL;
  if (list == NULL || endpoint == NULL || message == NULL ||
    members == NULL)
  {
    return -1;
  }
  e = (struct weld_loan_entry *)malloc(sizeof(*e));
  if (e == NULL) {
    return -1;
  }
  e->endpoint = endpoint;
  e->message = message;
  e->members = members;
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  e->next = *list;
  *list = e;
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return 0;
}

// Unlink one outstanding loan. On success stores the members for the
// caller to fini plus free and returns 0, else returns -1.
static int weld_loan_unlink(
  struct weld_loan_entry ** list, const void * endpoint, void * message,
  const rosidl_typesupport_introspection_c__MessageMembers ** members_out)
{
  int found = -1;
  if (list == NULL || endpoint == NULL || message == NULL ||
    members_out == NULL)
  {
    return -1;
  }
  *members_out = NULL;
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  {
    struct weld_loan_entry ** link = list;
    while (*link != NULL) {
      if ((*link)->endpoint == endpoint && (*link)->message == message) {
        struct weld_loan_entry * dead = *link;
        *link = dead->next;
        *members_out = dead->members;
        free(dead);
        found = 0;
        break;
      }
      link = &(*link)->next;
    }
  }
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return found;
}

// Restore a loan unlinked for publish after a failure. Fini plus frees
// the message when the record itself cannot be restored.
static void weld_loan_restore_pub(
  const void * endpoint, void * message,
  const rosidl_typesupport_introspection_c__MessageMembers * members)
{
  if (endpoint == NULL || message == NULL || members == NULL) {
    return;
  }
  if (weld_loan_add(&g_weld_pub_loans, endpoint, message, members) != 0) {
    members->fini_function(message);
    free(message);
  }
}

// Record one consumed but unusable wire sample for MESSAGE_LOST.
static void weld_drop_add(void)
{
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  if (g_weld_decode_drops < SIZE_MAX) {
    g_weld_decode_drops++;
  }
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
}

// Fill take info with a local receive stamp plus delivered attribution.
//
// The source stamp reports the local receive time (never 0) while the
// sequence is the per-subscription receive count from the Go core with
// intra-process equal to network because the bridge is a single
// transport. A zero sequence means the core predates attribution and
// both sequences stay unsupported. The GID is the publishing writer
// identity, or zeroed when the take carried none (wire takes, old
// core, or a NULL gid).
static void weld_fill_take_info(
  rmw_message_info_t * message_info, unsigned long long seq,
  const uint8_t gid[16])
{
  struct timespec now;
  uint64_t now_ns = 0;
  if (message_info == NULL) {
    return;
  }
  (void)clock_gettime(CLOCK_REALTIME, &now);
  now_ns = (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
  message_info->source_timestamp = (rmw_time_point_value_t)now_ns;
  message_info->received_timestamp = (rmw_time_point_value_t)now_ns;
  if (seq == 0 || seq > (unsigned long long)INT64_MAX) {
    message_info->publication_sequence_number =
      RMW_MESSAGE_INFO_SEQUENCE_NUMBER_UNSUPPORTED;
    message_info->reception_sequence_number =
      RMW_MESSAGE_INFO_SEQUENCE_NUMBER_UNSUPPORTED;
  } else {
    message_info->publication_sequence_number = (int64_t)seq;
    message_info->reception_sequence_number = (int64_t)seq;
  }
  message_info->publisher_gid.implementation_identifier =
    kImplementationIdentifier;
  if (gid == NULL) {
    (void)memset(
      message_info->publisher_gid.data, 0,
      sizeof(message_info->publisher_gid.data));
  } else {
    size_t copy_len = sizeof(message_info->publisher_gid.data);
    if (copy_len > 16) {
      copy_len = 16;
    }
    (void)memcpy(message_info->publisher_gid.data, gid, copy_len);
    if (copy_len < sizeof(message_info->publisher_gid.data)) {
      (void)memset(
        message_info->publisher_gid.data + copy_len, 0,
        sizeof(message_info->publisher_gid.data) - copy_len);
    }
  }
  message_info->from_intra_process = false;
}

// Take plus decode one sample into a fresh owned buffer recorded as a
// subscription loan, with an in-process zero-copy fast path first.
// Returns 0 with *message_out owned on data, 1 when no data is ready,
// and -1 with the rmw error set on failure. The fast path dequeues the
// same buffer pointer enqueued by publish with no encode, decode, or
// payload copy; the fallback pulls one wire sample (wait stash first via
// weld_take_wire) plus decodes it into a fresh buffer, which copies.
// Decode failures count as dropped samples for MESSAGE_LOST. On success
// seq_out plus gid_out carry the delivered attribution when non-NULL.
static int weld_take_loaned_wire(
  const rmw_subscription_t * subscription, void ** message_out,
  unsigned long long * seq_out, uint8_t gid_out[16])
{
  struct weld_sub_data * sd = NULL;
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  weld_cdr_status_t st;
  void * buf = NULL;
  int empty = 0;
  if (subscription == NULL || message_out == NULL) {
    return -1;
  }
  *message_out = NULL;
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take no subscription data");
    return -1;
  }
  if (weld_ffi_loan_take_poll != NULL) {
    unsigned long long loan_addr = 0;
    unsigned long long members_addr = 0;
    int lrc = weld_ffi_loan_take_poll(
      sd->handle, &loan_addr, &members_addr);
    if (lrc == 0 && loan_addr != 0 && members_addr != 0) {
      const rosidl_typesupport_introspection_c__MessageMembers * loan_members =
        (const rosidl_typesupport_introspection_c__MessageMembers *)
        (uintptr_t)members_addr;
      void * loan_buf = (void *)(uintptr_t)loan_addr;
      uint8_t * dup = NULL;
      size_t dup_len = 0;
      if (weld_loan_add(&g_weld_sub_loans, subscription, loan_buf,
        loan_members) != 0)
      {
        loan_members->fini_function(loan_buf);
        free(loan_buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take no memory");
        return -1;
      }
      if (weld_take_wire(sd, &dup, &dup_len, NULL, NULL) == 0) {
        free(dup);
      }
      if (seq_out != NULL || gid_out != NULL) {
        uint8_t gid[16];
        unsigned long long seq = 0;
        (void)weld_take_info(sd->handle, &seq, gid);
        if (seq_out != NULL) {
          *seq_out = seq;
        }
        if (gid_out != NULL) {
          (void)memcpy(gid_out, gid, sizeof(gid));
        }
      }
      *message_out = loan_buf;
      return 0;
    }
  }
  members = weld_resolve_members(sd->type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: loaned take type support is not introspection");
    return -1;
  }
  if (members->init_function == NULL || members->fini_function == NULL ||
    members->size_of_ == 0)
  {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take type support has no init");
    return -1;
  }
  empty = weld_take_wire(sd, &wire, &wire_len, seq_out, gid_out);
  if (empty == 1) {
    return 1;
  }
  if (empty != 0) {
    return -1;
  }
  buf = calloc(1, members->size_of_);
  if (buf == NULL) {
    free(wire);
    weld_drop_add();
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take no memory");
    return -1;
  }
  members->init_function(buf, ROSIDL_RUNTIME_C_MSG_INIT_ALL);
  errbuf[0] = '\0';
  st = weld_cdr_decode_members(
    members, wire, wire_len, buf, errbuf, sizeof(errbuf));
  free(wire);
  if (st != WELD_CDR_OK) {
    members->fini_function(buf);
    free(buf);
    weld_drop_add();
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take decode failed");
    }
    return -1;
  }
  if (weld_loan_add(&g_weld_sub_loans, subscription, buf, members) != 0) {
    members->fini_function(buf);
    free(buf);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take no memory");
    return -1;
  }
  *message_out = buf;
  return 0;
}

// Event slots keyed by event handle for per-type baselines plus
// on-event callbacks. Guarded by g_weld_loan_mutex with the loan lists.
// Each slot binds one rmw_event_t to its endpoint at init: handle is the
// FFI pub or sub handle, endpoint points at the live weld_pub_data or
// weld_sub_data, and is_pub selects the side. Message lost stays global
// with no endpoint. Baselines mirror the take path so wait peeks without
// consuming: lost, alive, not-alive, deadline, wire incompat, and local
// incompat totals last reported. Callback fires on take when new data
// exists; wait wakes for polling without firing.
struct weld_event_slot
{
  const rmw_event_t * handle;
  rmw_event_callback_t callback;
  const void * user_data;
  rmw_event_type_t event_type;
  long long endpoint_handle;
  const void * endpoint;
  bool is_pub;
  size_t lost_reported;
  long long alive_reported;
  long long not_alive_reported;
  unsigned long long deadline_reported;
  unsigned long long wire_incompat_reported;
  size_t local_incompat_reported;
  long long last_matched;
  unsigned long long lost_total;
  bool used;
};

enum
{
  WELD_EVENT_SLOTS_MAX = 16
};

static struct weld_event_slot g_weld_event_slots[WELD_EVENT_SLOTS_MAX];

// Find or claim a slot for handle. The caller holds g_weld_loan_mutex.
// Returns the slot, or NULL when the table is full.
static struct weld_event_slot * weld_event_slot_locked(
  const rmw_event_t * handle)
{
  struct weld_event_slot * free_slot = NULL;
  size_t i = 0;
  if (handle == NULL) {
    return NULL;
  }
  for (i = 0; i < (size_t)WELD_EVENT_SLOTS_MAX; i++) {
    if (g_weld_event_slots[i].used &&
      g_weld_event_slots[i].handle == handle)
    {
      return &g_weld_event_slots[i];
    }
    if (!g_weld_event_slots[i].used && free_slot == NULL) {
      free_slot = &g_weld_event_slots[i];
    }
  }
  if (free_slot == NULL) {
    return NULL;
  }
  (void)memset(free_slot, 0, sizeof(*free_slot));
  free_slot->handle = handle;
  free_slot->used = true;
  return free_slot;
}

// Release the slot for a finalized event handle.
//
// The caller must hold g_weld_loan_mutex. Slots are keyed by handle
// pointer, so finalizing twice is a silent no-op.
static void weld_event_slot_release_locked(const rmw_event_t * handle)
{
  size_t i = 0;
  if (handle == NULL) {
    return;
  }
  for (i = 0; i < (size_t)WELD_EVENT_SLOTS_MAX; i++) {
    if (g_weld_event_slots[i].used &&
      g_weld_event_slots[i].handle == handle)
    {
      (void)memset(&g_weld_event_slots[i], 0, sizeof(g_weld_event_slots[i]));
      return;
    }
  }
}

// Read the current matched count for a bound endpoint without consuming.
//
// Uses the existing matched FFI which sums local same-bridge endpoints
// with remote SEDP peers. Returns 0 with the count, -1 when the FFI is
// not linked or the endpoint is gone. Never fails loudly: callers treat
// unknown as not ready.
static int weld_event_matched_current(
  const struct weld_event_slot * slot, long long * out)
{
  int n = 0;
  if (slot == NULL || out == NULL) {
    return -1;
  }
  *out = 0;
  if (slot->is_pub) {
    if (weld_ffi_pub_matched_subs == NULL) {
      return -1;
    }
    if (weld_ffi_pub_matched_subs(slot->endpoint_handle, &n) != 0) {
      return -1;
    }
  } else {
    if (weld_ffi_sub_matched_pubs == NULL) {
      return -1;
    }
    if (weld_ffi_sub_matched_pubs(slot->endpoint_handle, &n) != 0) {
      return -1;
    }
  }
  if (n < 0) {
    return -1;
  }
  *out = (long long)n;
  return 0;
}

// Read the cumulative deadline total for a bound endpoint.
//
// Returns 0 with the total, -1 when the FFI is not linked. A zero total
// means no misses yet, which is a real source (deadline tracked from the
// stored QoS profile plus take and publish timing in the Go core).
static int weld_event_deadline_current(
  const struct weld_event_slot * slot, unsigned long long * out)
{
  unsigned long long total = 0;
  if (slot == NULL || out == NULL) {
    return -1;
  }
  *out = 0;
  if (slot->is_pub) {
    if (weld_ffi_pub_offered_deadline_total == NULL) {
      return -1;
    }
    if (weld_ffi_pub_offered_deadline_total(
        slot->endpoint_handle, &total) != 0)
    {
      return -1;
    }
  } else {
    if (weld_ffi_sub_requested_deadline_total == NULL) {
      return -1;
    }
    if (weld_ffi_sub_requested_deadline_total(
        slot->endpoint_handle, &total) != 0)
    {
      return -1;
    }
  }
  *out = total;
  return 0;
}

// Read the cumulative wire incompat total for a bound endpoint.
//
// Returns 0 with the monotonic ever-seen wire mismatch count, -1 when
// the FFI is not linked. Local profile mismatches counted at creation
// add separately from endpoint state.
static int weld_event_wire_incompat_current(
  const struct weld_event_slot * slot, unsigned long long * out)
{
  unsigned long long total = 0;
  if (slot == NULL || out == NULL) {
    return -1;
  }
  *out = 0;
  if (slot->is_pub) {
    if (weld_ffi_pub_incompatible_total == NULL) {
      return -1;
    }
    if (weld_ffi_pub_incompatible_total(
        slot->endpoint_handle, &total) != 0)
    {
      return -1;
    }
  } else {
    if (weld_ffi_sub_incompatible_total == NULL) {
      return -1;
    }
    if (weld_ffi_sub_incompatible_total(
        slot->endpoint_handle, &total) != 0)
    {
      return -1;
    }
  }
  *out = total;
  return 0;
}

// Read the local incompat total stored on the endpoint.
//
// Local mismatches are counted at creation via
// rmw_qos_profile_check_compatible on both sides of a mismatched pair.
// Returns 0 with the count, -1 when the endpoint is gone.
static int weld_event_local_incompat_current(
  const struct weld_event_slot * slot, size_t * out)
{
  if (slot == NULL || out == NULL || slot->endpoint == NULL) {
    return -1;
  }
  *out = 0;
  if (slot->is_pub) {
    const struct weld_pub_data * pd =
      (const struct weld_pub_data *)slot->endpoint;
    *out = pd->incompat_total;
  } else {
    const struct weld_sub_data * sd =
      (const struct weld_sub_data *)slot->endpoint;
    *out = sd->incompat_total;
  }
  return 0;
}

// Report whether one bound event has untaken status without consuming.
//
// Switches on the slot event type and compares the live source against
// the take-owned baseline. Unknown sources and missing FFI report not
// ready so wait keeps polling instead of firing spuriously.
static bool weld_event_bound_ready(const rmw_event_t * handle)
{
  bool ready = false;
  long long matched = 0;
  unsigned long long total = 0;
  unsigned long long wire = 0;
  size_t local = 0;
  if (handle == NULL) {
    return false;
  }
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  {
    struct weld_event_slot * slot = weld_event_slot_locked(handle);
    if (slot == NULL || !slot->used) {
      (void)pthread_mutex_unlock(&g_weld_loan_mutex);
      return false;
    }
    switch (slot->event_type) {
      case RMW_EVENT_MESSAGE_LOST:
        ready = g_weld_decode_drops > slot->lost_reported;
        break;
      case RMW_EVENT_LIVELINESS_CHANGED:
        if (weld_event_matched_current(slot, &matched) == 0) {
          ready = matched != slot->alive_reported;
        }
        break;
      case RMW_EVENT_LIVELINESS_LOST:
        if (weld_event_matched_current(slot, &matched) == 0) {
          unsigned long long cur = slot->lost_total;
          if (matched < slot->last_matched) {
            cur += (unsigned long long)(slot->last_matched - matched);
          }
          ready = cur > (unsigned long long)slot->lost_reported;
        }
        break;
      case RMW_EVENT_REQUESTED_DEADLINE_MISSED:
      case RMW_EVENT_OFFERED_DEADLINE_MISSED:
        if (!slot->is_pub &&
          slot->event_type == RMW_EVENT_OFFERED_DEADLINE_MISSED)
        {
          break;
        }
        if (slot->is_pub &&
          slot->event_type == RMW_EVENT_REQUESTED_DEADLINE_MISSED)
        {
          break;
        }
        if (weld_event_deadline_current(slot, &total) == 0) {
          ready = total > slot->deadline_reported;
        }
        break;
      case RMW_EVENT_REQUESTED_QOS_INCOMPATIBLE:
      case RMW_EVENT_OFFERED_QOS_INCOMPATIBLE:
        if (!slot->is_pub &&
          slot->event_type == RMW_EVENT_OFFERED_QOS_INCOMPATIBLE)
        {
          break;
        }
        if (slot->is_pub &&
          slot->event_type == RMW_EVENT_REQUESTED_QOS_INCOMPATIBLE)
        {
          break;
        }
        if (weld_event_wire_incompat_current(slot, &wire) == 0 &&
          weld_event_local_incompat_current(slot, &local) == 0)
        {
          ready = wire + (unsigned long long)local >
            slot->wire_incompat_reported +
            (unsigned long long)slot->local_incompat_reported;
        }
        break;
      default:
        break;
    }
  }
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return ready;
}

// Sleep the dispatch poll period. EINTR retries so signals never turn
// the dispatcher into a hot loop.
static void weld_dispatch_sleep(void)
{
  struct timespec req;
  req.tv_sec = 0;
  req.tv_nsec = WELD_DISPATCH_POLL_NS;
  while (nanosleep(&req, &req) != 0) {
    if (errno != EINTR) {
      return;
    }
  }
}

// Subscription dispatch thread: fire the stored on-new callback once
// per data arrival edge.
//
// The loop polls weld_ffi_sub_has_data, which never consumes, so takes
// still deliver every sample and the executor-owned wait stash stays
// untouched. The callback runs without holding the dispatch mutex
// because callbacks may call back into rmw. A missing FFI symbol ends
// the thread, restoring stored-never-fired behavior on old libraries.
static void * weld_sub_dispatch_thread(void * arg)
{
  struct weld_sub_data * sd = (struct weld_sub_data *)arg;
  long long handle = 0;
  if (sd == NULL) {
    return NULL;
  }
  (void)pthread_mutex_lock(&sd->dispatch_mutex);
  handle = sd->handle;
  (void)pthread_mutex_unlock(&sd->dispatch_mutex);
  for (;;) {
    bool run = false;
    rmw_event_callback_t cb = NULL;
    const void * user_data = NULL;
    int ready = 0;
    (void)pthread_mutex_lock(&sd->dispatch_mutex);
    run = sd->dispatch_run;
    cb = sd->on_new_callback;
    user_data = sd->on_new_user_data;
    (void)pthread_mutex_unlock(&sd->dispatch_mutex);
    if (!run) {
      return NULL;
    }
    if (weld_ffi_sub_has_data == NULL) {
      return NULL;
    }
    ready = weld_ffi_sub_has_data(handle);
    if (ready > 0 && cb != NULL) {
      bool fire = false;
      (void)pthread_mutex_lock(&sd->dispatch_mutex);
      fire = sd->dispatch_run && !sd->dispatch_fired;
      if (fire) {
        sd->dispatch_fired = true;
      }
      (void)pthread_mutex_unlock(&sd->dispatch_mutex);
      if (fire) {
        cb(user_data, 1);
      }
    } else if (ready == 0) {
      (void)pthread_mutex_lock(&sd->dispatch_mutex);
      sd->dispatch_fired = false;
      (void)pthread_mutex_unlock(&sd->dispatch_mutex);
    }
    weld_dispatch_sleep();
  }
}

// Start the subscription dispatch thread on the first callback.
//
// Called with no locks held. A thread start failure keeps the stored
// callback without dispatch, matching the old behavior loudly through
// the normal take and wait paths.
static void weld_sub_dispatch_start(struct weld_sub_data * sd)
{
  int rc = 0;
  if (sd == NULL) {
    return;
  }
  (void)pthread_mutex_lock(&sd->dispatch_mutex);
  if (sd->dispatch_started || sd->on_new_callback == NULL) {
    (void)pthread_mutex_unlock(&sd->dispatch_mutex);
    return;
  }
  sd->dispatch_run = true;
  sd->dispatch_fired = false;
  rc = pthread_create(
    &sd->dispatch_thread, NULL, weld_sub_dispatch_thread, sd);
  if (rc != 0) {
    sd->dispatch_run = false;
    (void)pthread_mutex_unlock(&sd->dispatch_mutex);
    return;
  }
  sd->dispatch_started = true;
  (void)pthread_mutex_unlock(&sd->dispatch_mutex);
}

// Stop the subscription dispatch thread and wait for its exit.
//
// Called with no locks held. Joining before teardown guarantees the
// thread never touches freed endpoint state.
static void weld_sub_dispatch_stop(struct weld_sub_data * sd)
{
  bool started = false;
  if (sd == NULL) {
    return;
  }
  (void)pthread_mutex_lock(&sd->dispatch_mutex);
  sd->dispatch_run = false;
  started = sd->dispatch_started;
  (void)pthread_mutex_unlock(&sd->dispatch_mutex);
  if (started) {
    (void)pthread_join(sd->dispatch_thread, NULL);
  }
}

// Dynamic subscription type identity, resolved at runtime.
//
// Static subscriptions carry an introspection handle, but dynamic ones
// arrive with the rosidl_dynamic_typesupport_c identifier and only a
// type description plus hash in the impl. These helpers detect that
// handle, copy the slash form type name, and format the RIHS hash for
// the Go declare path. All dlopen resolution reuses the no link time
// dependency pattern from weld_dyn_lookup. Only present where
// rmw/dynamic_message_type_support.h exists; Humble builds skip it.
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")

typedef const char * (* weld_dyn_ident_fn)(void);

static weld_dyn_ident_fn g_weld_dyn_ident = NULL;

static bool g_weld_dyn_ident_resolved = false;

// Look up the dynamic identifier string, resolving once per process.
// Returns NULL when the runtime has no dynamic support.
static const char * weld_dyn_identifier(void)
{
  const char * ident = NULL;
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  if (!g_weld_dyn_ident_resolved) {
#ifdef WELD_HAVE_DLOPEN
    void * lib = dlopen(
      "librosidl_dynamic_typesupport.so", RTLD_LAZY | RTLD_NOLOAD);
    if (lib == NULL) {
      lib = dlopen("librosidl_dynamic_typesupport.so", RTLD_LAZY);
    }
    if (lib != NULL) {
      void * sym = dlsym(lib, "rosidl_get_dynamic_typesupport_identifier");
      if (sym != NULL) {
        weld_dyn_ident_fn fn = NULL;
        (void)memcpy(&fn, &sym, sizeof(fn));
        g_weld_dyn_ident = fn;
      }
    }
#endif
    g_weld_dyn_ident_resolved = true;
  }
  if (g_weld_dyn_ident != NULL) {
    ident = g_weld_dyn_ident();
  }
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return ident;
}

// Report whether type_support is a runtime dynamic handle.
// Anything else, including NULL, reports false.
static bool weld_dynamic_is_ts(const rosidl_message_type_support_t * type_support)
{
  const char * ident = NULL;
  if (type_support == NULL || type_support->typesupport_identifier == NULL) {
    return false;
  }
  ident = weld_dyn_identifier();
  if (ident == NULL) {
    return false;
  }
  return strcmp(type_support->typesupport_identifier, ident) == 0;
}

// Copy the slash form type name from a dynamic handle impl.
// Returns 0 with the name in buf, -1 when the impl or name is missing
// or does not fit.
static int weld_dynamic_type_name(
  const rosidl_message_type_support_t * type_support,
  char * buf, size_t buflen)
{
  const rosidl_dynamic_message_type_support_impl_t * dimpl = NULL;
  const rosidl_runtime_c__String * name = NULL;
  size_t n = 0;
  if (type_support == NULL || buf == NULL || buflen == 0) {
    return -1;
  }
  dimpl = (const rosidl_dynamic_message_type_support_impl_t *)type_support->data;
  if (dimpl == NULL) {
    return -1;
  }
  name = &dimpl->type_description.type_description.type_name;
  if (name->data == NULL || name->data[0] == '\0') {
    return -1;
  }
  n = strlen(name->data) + 1;
  if (n > buflen) {
    return -1;
  }
  (void)memcpy(buf, name->data, n);
  return 0;
}

// Fill the keep alive dynamic identity for a subscription.
// The slash form name plus RIHS hash are heap owned by the caller. The
// hash stays empty when the runtime provides none, matching the
// hashless wildcard report. Returns 0 on success and -1 with no side
// effects on failure.
static int weld_dynamic_fill(
  const rosidl_message_type_support_t * type_support,
  char ** name_out, char ** hash_out)
{
  char name[512];
  char hash[WELD_TYPE_HASH_MAX];
  char * name_copy = NULL;
  char * hash_copy = NULL;
  if (type_support == NULL || name_out == NULL || hash_out == NULL) {
    return -1;
  }
  *name_out = NULL;
  *hash_out = NULL;
  if (weld_dynamic_type_name(type_support, name, sizeof(name)) != 0) {
    return -1;
  }
  name_copy = weld_dup_str(name);
  if (name_copy == NULL) {
    return -1;
  }
  if (weld_type_hash(type_support, hash, sizeof(hash)) == 0) {
    hash_copy = weld_dup_str(hash);
  } else {
    hash_copy = weld_dup_str("");
  }
  if (hash_copy == NULL) {
    free(name_copy);
    return -1;
  }
  *name_out = name_copy;
  *hash_out = hash_copy;
  return 0;
}

// Dynamic deserialize entry resolved without a link time dependency.
//
// The probe link lines carry no -lrosidl_dynamic_typesupport, so a direct
// call breaks the link. The symbol is resolved once from the loaded
// library (rcl already depends on it) via dlopen plus dlsym, falling back
// to loading by soname. A NULL result means the runtime has no dynamic
// support and the take reports UNSUPPORTED. dlopen lives in libc on the
// supported platforms, so no extra link flag is needed. The handle is
// never closed so the resolved entry stays valid.
typedef rcutils_ret_t (* weld_dyn_deserialize_fn)(
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message,
  rcutils_uint8_array_t * buffer);

static weld_dyn_deserialize_fn g_weld_dyn_deserialize = NULL;

static bool g_weld_dyn_resolved = false;

// Look up the dynamic deserialize entry, resolving once per process.
// Returns NULL when the runtime has no dynamic support.
static weld_dyn_deserialize_fn weld_dyn_lookup(void)
{
  weld_dyn_deserialize_fn fn = NULL;
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  if (!g_weld_dyn_resolved) {
#ifdef WELD_HAVE_DLOPEN
    void * lib = dlopen(
      "librosidl_dynamic_typesupport.so", RTLD_LAZY | RTLD_NOLOAD);
    if (lib == NULL) {
      lib = dlopen("librosidl_dynamic_typesupport.so", RTLD_LAZY);
    }
    if (lib != NULL) {
      void * sym = dlsym(
        lib, "rosidl_dynamic_typesupport_dynamic_data_deserialize");
      if (sym != NULL) {
        (void)memcpy(&fn, &sym, sizeof(fn));
        g_weld_dyn_deserialize = fn;
      }
    }
#endif
    g_weld_dyn_resolved = true;
  }
  fn = g_weld_dyn_deserialize;
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return fn;
}

#endif /* dynamic message typesupport available */

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

// Remote endpoint queries match the
// internal/rmwbridge/weldffi/filter.go exports. They stay weak so the
// info-by-topic paths treat remotes as absent when the Go core is not
// linked instead of crashing.
extern int weld_ffi_remote_ep_count(
  long long bridge, const char * topic, int role, long long * out)
  __attribute__((weak));
extern int weld_ffi_remote_ep_at(
  long long bridge, const char * topic, int role, long long index,
  char * type_buf, int type_len, char * gid_buf, int gid_len)
  __attribute__((weak));

// Network flow endpoint queries match the
// internal/rmwbridge/weldffi/netflow.go exports. They stay weak so the
// flow queries report UNSUPPORTED when the Go core is not linked instead
// of crashing. local reports the advertised IPv4 plus the bound unicast
// port, count/at report discovered unicast ports per remote endpoint,
// and a missing backend (in-memory only) reports -3 with no socket.
extern int weld_ffi_netflow_local(
  long long bridge, char * ip_buf, int ip_len, int * port_out)
  __attribute__((weak));
extern int weld_ffi_netflow_remote_count(
  long long bridge, const char * topic, int role, long long * out)
  __attribute__((weak));
extern int weld_ffi_netflow_remote_at(
  long long bridge, const char * topic, int role, long long index,
  char * ip_buf, int ip_len, int * port_out)
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
  char * node_copy = NULL;
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
  node_copy = weld_dup_str(node->name != NULL ? node->name : "");
  if (pub == NULL || pd == NULL || topic_copy == NULL || data_topic == NULL ||
    node_copy == NULL)
  {
    free(pub);
    free(pd);
    free(topic_copy);
    free(data_topic);
    free(node_copy);
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
    free(node_copy);
    (void)weld_fail_ffi("rmw_weld: rmw_create_publisher backend failed", rc);
    return NULL;
  }
  pd->handle = handle;
  pd->type_support = type_support;
  pd->topic = data_topic;
  pd->node_name = node_copy;
  pd->domain = nd->domain;
  pd->qos = *qos_profile;
  pd->incompat_total = 0;
  // Report the ROS type for graph interop. Best effort: a missing
  // symbol or bad name skips silently and creation still succeeds.
  weld_report_type(handle, type_support);
  // Report the deadline for offered-miss tracking. Best effort: an older
  // Go library without the symbol keeps local-only timing with no miss
  // counts instead of failing creation.
  if (weld_ffi_pub_set_deadline != NULL) {
    long long dsec = 0;
    long long dnsec = 0;
    if (qos_profile->deadline.sec <= (uint64_t)INT64_MAX) {
      dsec = (long long)qos_profile->deadline.sec;
    }
    if (qos_profile->deadline.nsec <= (uint64_t)INT64_MAX) {
      dnsec = (long long)qos_profile->deadline.nsec;
    }
    (void)weld_ffi_pub_set_deadline(handle, dsec, dnsec);
  }
  // Count local incompatibilities at match time against existing
  // subscriptions on the same topic. Both sides bump so take totals stay
  // real without a second pass.
  pd->incompat_total = weld_incompat_scan(
    WELD_EP_PUB, qos_profile, topic_name, nd->domain);
  pub->implementation_identifier = kImplementationIdentifier;
  pub->data = pd;
  pub->topic_name = topic_copy;
  if (publisher_options != NULL) {
    pub->options = *publisher_options;
  } else {
    (void)memset(&pub->options, 0, sizeof(pub->options));
  }
  // Loans are owned heap buffers, never zero copy shared memory: see
  // the loan family above.
  pub->can_loan_messages = true;
  // Record the endpoint for by-node plus info-by-topic queries. Best
  // effort: a missed record only shrinks info snapshots.
  weld_ep_register(
    WELD_EP_PUB, node->name, node->namespace_, nd->domain, pd);
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
    weld_ep_unregister(pd);
    // Check the backend result before freeing: on failure the
    // handle stays valid so the caller may retry the destroy
    // instead of using freed memory.
    rc = weld_ffi_pub_destroy(pd->handle);
    if (rc != 0) {
      return weld_fail_ffi(
        "rmw_weld: rmw_destroy_publisher backend failed", rc);
    }
    free(pd->topic);
    free(pd->node_name);
    free(pd);
  }
  publisher->data = NULL;
  free((void *)publisher->topic_name);
  free(publisher);
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
  bool is_dynamic = false;
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
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")
    // No static introspection handle: accept a runtime dynamic handle
    // and resolve the type from its description plus hash instead.
    // Anything else still fails loudly below.
    if (!weld_dynamic_is_ts(type_support)) {
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_create_subscription type support is not introspection");
      return NULL;
    }
    is_dynamic = true;
#else
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_create_subscription type support is not introspection");
    return NULL;
#endif
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
  sd->incompat_total = 0;
  sd->on_new_callback = NULL;
  sd->on_new_user_data = NULL;
  if (pthread_mutex_init(&sd->dispatch_mutex, NULL) != 0) {
    (void)weld_ffi_sub_destroy(handle);
    free(sub);
    free(sd);
    free(topic_copy);
    free(data_topic);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_subscription no mutex");
    return NULL;
  }
  sd->dispatch_run = false;
  sd->dispatch_started = false;
  sd->dispatch_fired = false;
  sd->filter_expr = NULL;
  sd->filter_params = NULL;
  sd->filter_nparams = 0;
  sd->wait_buf = NULL;
  sd->wait_len = 0;
  sd->has_wait = false;
  sd->wait_seq = 0;
  (void)memset(sd->wait_gid, 0, sizeof(sd->wait_gid));
  sd->is_dynamic = is_dynamic;
  sd->dyn_type_name = NULL;
  sd->dyn_type_hash = NULL;
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")
  if (is_dynamic) {
    // The runtime type has no static introspection handle, so the
    // slash form name plus hash resolve from the dynamic impl and stay
    // alive on the subscription. Derivation failure fails creation
    // loudly; only the backend report itself is best effort.
    if (weld_dynamic_fill(
        type_support, &sd->dyn_type_name, &sd->dyn_type_hash) != 0)
    {
      (void)weld_ffi_sub_destroy(handle);
      (void)pthread_mutex_destroy(&sd->dispatch_mutex);
      free(sub);
      free(sd);
      free(topic_copy);
      free(data_topic);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_create_subscription dynamic type has no description");
      return NULL;
    }
    weld_report_dynamic_sub_type(handle, sd->dyn_type_name, sd->dyn_type_hash);
  } else {
    // Report the ROS type for graph interop. Best effort: a missing
    // symbol or bad name skips silently and creation still succeeds.
    weld_report_sub_type(handle, type_support);
  }
#else
  // Report the ROS type for graph interop. Best effort: a missing
  // symbol or bad name skips silently and creation still succeeds.
  weld_report_sub_type(handle, type_support);
#endif
  sub->implementation_identifier = kImplementationIdentifier;
  sub->data = sd;
  sub->topic_name = topic_copy;
  if (subscription_options != NULL) {
    sub->options = *subscription_options;
  } else {
    (void)memset(&sub->options, 0, sizeof(sub->options));
  }
  // Loans are owned heap buffers, never zero copy shared memory: see
  // the loan family above.
  sub->can_loan_messages = true;
  sub->is_cft_enabled = false;
  // Report the deadline for requested-miss tracking. Best effort like
  // the publisher side.
  if (weld_ffi_sub_set_deadline != NULL) {
    long long dsec = 0;
    long long dnsec = 0;
    if (qos_profile->deadline.sec <= (uint64_t)INT64_MAX) {
      dsec = (long long)qos_profile->deadline.sec;
    }
    if (qos_profile->deadline.nsec <= (uint64_t)INT64_MAX) {
      dnsec = (long long)qos_profile->deadline.nsec;
    }
    (void)weld_ffi_sub_set_deadline(handle, dsec, dnsec);
  }
  // Count local incompatibilities at match time against existing
  // publishers on the same topic. Both sides bump so take totals stay
  // real without a second pass.
  sd->incompat_total = weld_incompat_scan(
    WELD_EP_SUB, qos_profile, topic_name, nd->domain);
  // Record the endpoint for by-node plus info-by-topic queries. Best
  // effort: a missed record only shrinks info snapshots.
  weld_ep_register(
    WELD_EP_SUB, node->name, node->namespace_, nd->domain, sd);
  return sub;
}

rmw_ret_t rmw_destroy_subscription(rmw_node_t * node, rmw_subscription_t * subscription)
{
  // Bridge: weld_ffi_sub_destroy unregisters from per-domain subs and marks
  // closed so CountMatchedPublishers drops and Take reports
  // ErrSubscriptionClosed. The delivery channel stays open.
  struct weld_sub_data * sd = NULL;
  size_t i = 0;
  int rc = 0;
  if (node == NULL || subscription == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_subscription null argument");
  }
  if (!weld_node_ok(node) || !weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_destroy_subscription wrong implementation");
  }
  sd = (struct weld_sub_data *)subscription->data;
  if (sd != NULL) {
    weld_ep_unregister(sd);
    // Stop plus join the dispatch thread before the FFI destroy so the
    // thread never polls a torn down handle or touches freed state.
    weld_sub_dispatch_stop(sd);
    // Check the backend result before freeing: on failure the
    // handle stays valid so the caller may retry the destroy
    // instead of using freed memory.
    rc = weld_ffi_sub_destroy(sd->handle);
    if (rc != 0) {
      return weld_fail_ffi(
        "rmw_weld: rmw_destroy_subscription backend failed", rc);
    }
    // Release the keep alive runtime dynamic type identity.
    if (sd->is_dynamic) {
      free(sd->dyn_type_name);
      sd->dyn_type_name = NULL;
      free(sd->dyn_type_hash);
      sd->dyn_type_hash = NULL;
    }
    free(sd->topic);
    free(sd->filter_expr);
    for (i = 0; i < sd->filter_nparams; i++) {
      free(sd->filter_params[i]);
    }
    free(sd->filter_params);
    free(sd->wait_buf);
    sd->wait_buf = NULL;
    sd->wait_len = 0;
    sd->has_wait = false;
    (void)pthread_mutex_destroy(&sd->dispatch_mutex);
    free(sd);
  }
  subscription->data = NULL;
  free((void *)subscription->topic_name);
  free(subscription);
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
  empty = weld_take_wire(sd, &wire, &wire_len, NULL, NULL);
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
    weld_drop_add();
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
  // local metadata through weld_fill_take_info. The source stamp reports
  // the local receive time while the sequence plus GID ride the take
  // info side channel.
  struct weld_sub_data * sd = NULL;
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  unsigned long long seq = 0;
  uint8_t gid[16];
  char errbuf[256];
  weld_cdr_status_t st;
  int empty = 0;
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
  (void)memset(gid, 0, sizeof(gid));
  empty = weld_take_wire(sd, &wire, &wire_len, &seq, gid);
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
    weld_drop_add();
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_with_info decode failed");
    }
    return RMW_RET_ERROR;
  }
  weld_fill_take_info(message_info, seq, gid);
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
  empty = weld_take_wire(sd, &wire, &wire_len, NULL, NULL);
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
// Serve and respond match the internal/rmwbridge/ffi.go exports. They
// stay weak so rmw_take_request and rmw_send_response fail loudly when
// the Go core is not linked instead of crashing: non-blocking poll
// with 0 for data, 1 for empty, and negative codes for errors,
// following the same shape as the other non-blocking call sites.
extern int weld_ffi_service_serve_nb(
  long long handle, char * req_buf, int req_buf_len, int * out_n,
  long long * out_seq) __attribute__((weak));
extern int weld_ffi_service_respond(
  long long handle, long long seq, const char * resp,
  int resp_len) __attribute__((weak));

// Service RTPS wiring matches the internal/rmwbridge/weldffi service
// exports. Every symbol stays weak: a NULL address means an older Go
// library is linked and the path falls back to in-memory only instead
// of crashing. set_type records the IDL service base from the C
// introspection handle for RTPS endpoint declares. send_nb enqueues an
// in-memory request and parks the reply waiter without blocking;
// poll_reply waits bounded for that reply and consumes it.
// send_rtps publishes an RTPS request immediately; take_reply polls the
// reply topic with the same bound. has_request and has_response peek
// without consuming for rmw_wait.
extern int weld_ffi_service_set_type(
  long long handle, const char * type_name, const char * type_hash)
  __attribute__((weak));
extern int weld_ffi_service_has_request(long long handle, int * out_n)
  __attribute__((weak));
extern int weld_ffi_service_has_response(
  long long handle, long long seq, const char * guid, const char * idl_base,
  int * out_n) __attribute__((weak));
extern int weld_ffi_service_send_nb(
  long long handle, long long seq, const char * guid,
  const char * req, int req_len) __attribute__((weak));
extern int weld_ffi_service_poll_reply(
  long long handle, long long seq, const char * guid,
  char * resp_buf, int resp_buf_len, int * out_n) __attribute__((weak));
extern int weld_ffi_service_send_rtps(
  long long handle, long long seq, const char * guid, const char * idl_base,
  const char * req, int req_len) __attribute__((weak));
extern int weld_ffi_service_take_reply(
  long long handle, long long seq, const char * guid, const char * idl_base,
  char * resp_buf, int resp_buf_len, int * out_n) __attribute__((weak));
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
// Event deadline plus incompatibility plus remote QoS wiring matches the
// internal/rmwbridge/weldffi/events.go exports. Every symbol stays weak:
// a NULL address means an older Go library is linked and the path falls
// back to local-only sources (matched counts for liveliness, local QoS
// mismatches for incompatibility, first-local QoS for remotes) instead
// of crashing. Deadline setters carry the RMW deadline once at endpoint
// creation; totals report cumulative missed periods.
extern int weld_ffi_sub_set_deadline(
  long long handle, long long sec, long long nsec)
  __attribute__((weak));
extern int weld_ffi_pub_set_deadline(
  long long handle, long long sec, long long nsec)
  __attribute__((weak));
extern int weld_ffi_sub_requested_deadline_total(
  long long handle, unsigned long long * out)
  __attribute__((weak));
extern int weld_ffi_pub_offered_deadline_total(
  long long handle, unsigned long long * out)
  __attribute__((weak));
extern int weld_ffi_sub_incompatible_total(
  long long handle, unsigned long long * out)
  __attribute__((weak));
extern int weld_ffi_pub_incompatible_total(
  long long handle, unsigned long long * out)
  __attribute__((weak));
// Remote QoS offer for info-by-topic queries. Out params carry
// reliability, durability, history, depth, deadline sec/nsec,
// liveliness, and lease sec/nsec with infinite mapped to zero. Unknown
// or malformed offers report -2 so the caller skips fail-closed.
extern int weld_ffi_remote_ep_qos(
  long long bridge, const char * topic, int role, long long index,
  long long * reliability, long long * durability, long long * history,
  long long * depth, long long * deadline_sec, long long * deadline_nsec,
  long long * liveliness, long long * lease_sec, long long * lease_nsec)
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

// Liveliness report matches the internal/rmwbridge/weldffi export.
// participant names the asserting participant, domain carries the
// context domain, alive is 1 for live, and generation is caller owned
// and must increase per call. Weak so validation-only asserts still
// pass when the Go core predates the symbol.
extern int weld_ffi_report_liveliness(
  long long bridge, const char * participant, int domain, int alive,
  unsigned long long generation) __attribute__((weak));

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
// for a reply. type_support drives CDR encode plus decode through the
// service introspection request plus response members. The Go core keeps
// one request queue per service name that every server handle pops
// work-queue style, so N servers on one name share load with
// first-available-server-wins FIFO fairness and each request is served
// exactly once, beside 1-server N-clients.
// pending_is_echo parallels pending: a true slot means the take
// arrived as an 8 byte sequence echo with no encoded bytes.
// pending_local parallels pending: a true slot means the take sent
// while a local server existed, so the take waits on the same leg the
// send used instead of re-deciding after a topology change.
// n_pending never exceeds WELD_RPC_PENDING_MAX.
struct weld_rpc_state
{
  long long ffi_handle;
  rmw_qos_profile_t qos;
  char * name;
  pthread_mutex_t mutex;
  int64_t next_seq;
  // Per-endpoint guid salt, assigned once at create from a process
  // monotonic. It keeps service guids stable across recompute while
  // distinguishing endpoint incarnations that reuse an address.
  uint64_t guid_salt;
  int64_t pending[WELD_RPC_PENDING_MAX];
  size_t n_pending;
  size_t domain;
  rmw_event_callback_t on_new_callback;
  const void * on_new_user_data;
  // Callback dispatch thread, same edge-trigger contract as
  // subscriptions. Services poll has_request, clients poll has_response
  // for the oldest pending sequence; both probes never consume. Shared
  // under mutex with the rest of the endpoint state.
  pthread_t dispatch_thread;
  bool dispatch_run;
  bool dispatch_started;
  bool dispatch_fired;
  // True for services, false for clients. The dispatch thread uses it
  // to pick the request probe versus the reply probe.
  bool is_service;
  const rosidl_service_type_support_t * type_support;
  bool pending_is_echo[WELD_RPC_PENDING_MAX];
  bool pending_local[WELD_RPC_PENDING_MAX];
};

// Forward declarations: the RPC dispatch thread helpers are defined
// below, but the destroy paths that stop them come first.
static void weld_rpc_dispatch_start(struct weld_rpc_state * st);
static void weld_rpc_dispatch_stop(struct weld_rpc_state * st);

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
// The echo flag and the leg flag parallel the id and drop with it.
static void weld_rpc_remove_locked(struct weld_rpc_state * st, int64_t seq)
{
  for (size_t i = 0; i < st->n_pending; i++) {
    if (st->pending[i] == seq) {
      size_t tail = st->n_pending - i - 1;
      if (tail > 0) {
        (void)memmove(
          &st->pending[i], &st->pending[i + 1],
          tail * sizeof(st->pending[0]));
        (void)memmove(
          &st->pending_is_echo[i], &st->pending_is_echo[i + 1],
          tail * sizeof(st->pending_is_echo[0]));
        (void)memmove(
          &st->pending_local[i], &st->pending_local[i + 1],
          tail * sizeof(st->pending_local[0]));
      }
      st->n_pending--;
      return;
    }
  }
}

// Drop every pending id. The caller holds no locks; the endpoint is
// already unreachable by the time destroy runs.
static void weld_rpc_free_stash(struct weld_rpc_state * st)
{
  st->n_pending = 0;
}

// Resolve the request or response introspection members for a service.
//
// want_request selects the request members and false selects the
// response members. Returns NULL when the type support misses
// introspection or the side is absent.
static const rosidl_typesupport_introspection_c__MessageMembers *
weld_service_side_members(
  const rosidl_service_type_support_t * type_support, bool want_request)
{
  const rosidl_service_type_support_t * h = NULL;
  const rosidl_typesupport_introspection_c__ServiceMembers * svc = NULL;
  if (type_support == NULL) {
    return NULL;
  }
  h = get_service_typesupport_handle(
    type_support, rosidl_typesupport_introspection_c__identifier);
  if (h == NULL || h->data == NULL) {
    return NULL;
  }
  svc = (const rosidl_typesupport_introspection_c__ServiceMembers *)h->data;
  if (svc == NULL) {
    return NULL;
  }
  if (want_request) {
    return svc->request_members_;
  }
  return svc->response_members_;
}

// Build one deterministic 8 byte service guid for a pending sequence.
//
// The guid mixes the endpoint address, the process id, the sequence
// id, and a per-endpoint salt assigned once at create time, so wait
// peeks and take calls recompute the same value while two clients in
// one process and two processes never share a header. The salt defeats
// address reuse: malloc may hand a freed endpoint address to a later
// endpoint with the sequence restarted at 1, so address plus pid plus
// seq alone could recompute a stale in-flight guid. The salt never
// repeats within a process, so a new endpoint never aliases a
// destroyed one.
static _Atomic uint64_t g_weld_guid_salt = 1;

static void weld_svc_guid(
  struct weld_rpc_state * st, int64_t seq, uint8_t guid[8])
{
  uint64_t h = (uint64_t)(uintptr_t)(const void *)st;
  uint64_t pid = (uint64_t)getpid();
  h ^= pid + 0x9E3779B97F4A7C15ULL + (h << 6) + (h >> 2);
  h ^= (uint64_t)seq * 11400714819323198549ULL + (h << 6) + (h >> 2);
  h ^= st->guid_salt * 14029466866897019727ULL + (h << 6) + (h >> 2);
  for (int i = 0; i < 8; i++) {
    guid[i] = (uint8_t)((h >> (unsigned)(i * 8)) & 0xFFu);
  }
}

// Report the IDL service base for RTPS declares.
//
// The base joins the service namespace and name, e.g.
// std_srvs__srv__Trigger. Returns 0 on success and -1 when the type
// support misses introspection.
static int weld_service_idl_base(
  const rosidl_service_type_support_t * type_support,
  char * buf,
  size_t buflen)
{
  const rosidl_service_type_support_t * h = NULL;
  const rosidl_typesupport_introspection_c__ServiceMembers * svc = NULL;
  int n = 0;
  if (type_support == NULL || buf == NULL || buflen == 0) {
    return -1;
  }
  h = get_service_typesupport_handle(
    type_support, rosidl_typesupport_introspection_c__identifier);
  if (h == NULL || h->data == NULL) {
    return -1;
  }
  svc = (const rosidl_typesupport_introspection_c__ServiceMembers *)h->data;
  if (svc == NULL || svc->service_namespace_ == NULL ||
    svc->service_name_ == NULL)
  {
    return -1;
  }
  n = snprintf(
    buf, buflen, "%s__%s", svc->service_namespace_, svc->service_name_);
  if (n < 0 || (size_t)n >= buflen) {
    return -1;
  }
  return 0;
}

// Read the stored FFI handle under the endpoint mutex.
static long long weld_rpc_handle(struct weld_rpc_state * st)
{
  long long handle = 0;
  (void)pthread_mutex_lock(&st->mutex);
  handle = st->ffi_handle;
  (void)pthread_mutex_unlock(&st->mutex);
  return handle;
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

// Report whether this process hosts a server for the service.
//
// The shared registry counts servers separately from clients, so a lone
// client never mistakes its own shared handle for a server. No FFI call
// is needed: the registry is process-local state. An unknown name keeps
// the historical true default.
static bool weld_svc_has_local_server(const struct weld_rpc_state * st)
{
  bool local = true;
  if (st == NULL) {
    return false;
  }
  if (st->name == NULL) {
    return true;
  }
  (void)pthread_mutex_lock(&g_weld_svc_mutex);
  for (struct weld_svc_shared * e = g_weld_svc_shared; e != NULL;
    e = e->next)
  {
    if (strcmp(e->name, st->name) == 0) {
      local = e->servers > 0;
      break;
    }
  }
  (void)pthread_mutex_unlock(&g_weld_svc_mutex);
  return local;
}

rmw_service_t * rmw_create_service(
  const rmw_node_t * node, const rosidl_service_type_support_t * type_support,
  const char * service_name, const rmw_qos_profile_t * qos_profile)
{
  // Bridge: servers and clients for one name share a single FFI service
  // handle from the process bridge (see weld_svc_acquire). The type
  // support drives CDR encode plus decode and the IDL base reported to
  // the FFI service set_type for RTPS endpoint declares; the QoS is
  // stored but otherwise unused. The standalone build has no FFI
  // symbols, so creation fails loudly there instead of returning a
  // dead handle.
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
  if (weld_service_side_members(type_support, true) == NULL ||
    weld_service_side_members(type_support, false) == NULL)
  {
    (void)weld_fail_invalid(
      "rmw_weld: rmw_create_service type support is not introspection");
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
  st->guid_salt =
    (uint64_t)atomic_fetch_add(&g_weld_guid_salt, 1);
  st->qos = *qos_profile;
  st->type_support = type_support;
  st->is_service = true;
  st->domain = node->context != NULL ? node->context->actual_domain_id : 0;
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
  // without touching this count. The IDL base is reported best effort
  // for RTPS endpoint declares; a missing symbol keeps in-memory only.
  if (weld_ffi_service_set_type != NULL) {
    char base[512];
    if (weld_service_idl_base(type_support, base, sizeof(base)) == 0) {
      (void)weld_ffi_service_set_type(st->ffi_handle, base, "");
    }
  }
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
  // Record the endpoint for info-by-service queries. Best effort: a
  // missed record only shrinks info snapshots.
  weld_ep_register(
    WELD_EP_SVC, node->name, node->namespace_,
    node->context != NULL ? node->context->actual_domain_id : 0, st);
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
  weld_ep_unregister(st);
  weld_rpc_dispatch_stop(st);
  // Release the shared FFI handle before touching local state: on
  // failure nothing is freed, so the handle stays valid and the
  // caller may retry the destroy instead of using freed memory.
  rmw_ret_t rc = RMW_RET_OK;
  if (st->name != NULL) {
    rc = weld_svc_release(st->name);
  } else {
    rc = weld_fail_invalid("rmw_weld: rmw_destroy_service no name");
  }
  if (rc != RMW_RET_OK) {
    return rc;
  }
  weld_rpc_free_stash(st);
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
  // request from the in-memory rendezvous or the RTPS request topic and
  // reports its sequence id. CDR payloads decode through the
  // introspection request members; a leftover 8 byte sequence echo from
  // the old in-memory path keeps working by mapping only the header
  // while ros_request stays untouched.
  if (service == NULL || request_header == NULL || ros_request == NULL ||
    taken == NULL)
  {
    return weld_fail_invalid("rmw_weld: rmw_take_request null argument");
  }
  if (!weld_svc_ok(service)) {
    return weld_fail_impl("rmw_weld: rmw_take_request wrong implementation");
  }
  // Default to not taken: every error return below then leaves a
  // defined value instead of untouched caller memory.
  *taken = false;
  if (weld_ffi_service_serve_nb == NULL) {
    return weld_fail_unsupported("rmw_weld: rmw_take_request serve not linked yet");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)service->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_request no data");
  }
  const rosidl_typesupport_introspection_c__MessageMembers * req_members =
    weld_service_side_members(st->type_support, true);
  if (req_members == NULL) {
    return weld_fail_error(
      "rmw_weld: rmw_take_request type support is not introspection");
  }
  long long handle = weld_rpc_handle(st);
  size_t cap = 64 * 1024;
  const size_t cap_max = (size_t)(32 << 20);
  uint8_t * buf = (uint8_t *)malloc(cap > 0 ? cap : 1);
  if (buf == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_request out of memory");
    return RMW_RET_BAD_ALLOC;
  }
  int out_n = 0;
  long long seq = 0;
  for (;;) {
    int rc = 0;
    if (cap > (size_t)INT_MAX) {
      cap = (size_t)INT_MAX;
    }
    rc = weld_ffi_service_serve_nb(
      handle, (char *)buf, (int)cap, &out_n, &seq);
    if (rc == 1) {
      free(buf);
      *taken = false;
      return RMW_RET_OK;
    }
    if (rc == -4) {
      uint8_t * next = NULL;
      if (cap >= cap_max) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_request payload too large");
        return RMW_RET_ERROR;
      }
      cap *= 2;
      if (cap > cap_max) {
        cap = cap_max;
      }
      next = (uint8_t *)realloc(buf, cap);
      if (next == NULL) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_request out of memory");
        return RMW_RET_BAD_ALLOC;
      }
      buf = next;
      continue;
    }
    if (rc != 0) {
      free(buf);
      return weld_fail_error("rmw_weld: rmw_take_request serve failed");
    }
    break;
  }
  if (out_n < 0) {
    free(buf);
    return weld_fail_error("rmw_weld: rmw_take_request bad length");
  }
  char errbuf[256];
  errbuf[0] = '\0';
  weld_cdr_status_t dec = weld_cdr_decode_members(
    req_members, buf, (size_t)out_n, ros_request, errbuf, sizeof(errbuf));
  if (dec == WELD_CDR_OK) {
    (void)pthread_mutex_lock(&st->mutex);
    if (st->n_pending >= WELD_RPC_PENDING_MAX) {
      (void)pthread_mutex_unlock(&st->mutex);
      free(buf);
      return weld_fail_error("rmw_weld: rmw_take_request too many pending");
    }
    st->pending[st->n_pending] = (int64_t)seq;
    st->pending_is_echo[st->n_pending] = false;
    st->pending_local[st->n_pending] = true;
    st->n_pending++;
    (void)pthread_mutex_unlock(&st->mutex);
    request_header->source_timestamp = 0;
    request_header->received_timestamp = 0;
    (void)memset(
      request_header->request_id.writer_guid, 0,
      sizeof(request_header->request_id.writer_guid));
    request_header->request_id.sequence_number = (int64_t)seq;
    *taken = true;
    free(buf);
    return RMW_RET_OK;
  }
  // Old in-memory echo: exactly 8 bytes with no CDR header. Only the
  // header sequence travels; ros_request stays untouched.
  if (out_n == 8 && buf[0] != 0x00) {
    (void)pthread_mutex_lock(&st->mutex);
    if (st->n_pending >= WELD_RPC_PENDING_MAX) {
      (void)pthread_mutex_unlock(&st->mutex);
      free(buf);
      return weld_fail_error("rmw_weld: rmw_take_request too many pending");
    }
    st->pending[st->n_pending] = (int64_t)seq;
    st->pending_is_echo[st->n_pending] = true;
    st->pending_local[st->n_pending] = true;
    st->n_pending++;
    (void)pthread_mutex_unlock(&st->mutex);
    request_header->source_timestamp = 0;
    request_header->received_timestamp = 0;
    (void)memset(
      request_header->request_id.writer_guid, 0,
      sizeof(request_header->request_id.writer_guid));
    request_header->request_id.sequence_number = (int64_t)seq;
    *taken = true;
    free(buf);
    return RMW_RET_OK;
  }
  free(buf);
  rmw_reset_error();
  if (errbuf[0] != '\0') {
    RCUTILS_SET_ERROR_MSG(errbuf);
  } else {
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_request decode failed");
  }
  return RMW_RET_ERROR;
}

// rmw_send_response keeps rmw_request_id_t on Humble, Jazzy, and Rolling;
// only take_request and take_response moved to rmw_service_info_t.
rmw_ret_t rmw_send_response(
  const rmw_service_t * service, rmw_request_id_t * request_header, void * ros_response)
{
  // Bridge: the sequence recorded by rmw_take_request selects the Go
  // responder, which is the in-memory channel or the RTPS reply publish
  // depending on which leg served the request. CDR takes encode the
  // response through the introspection response members; echo takes
  // send the 8 byte sequence echo.
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
    return weld_fail_unsupported("rmw_weld: rmw_send_response respond not linked yet");
  }
  int64_t seq = request_header->sequence_number;
  bool want_echo = false;
  long long handle = 0;
  // Peek the echo flag first: encoding needs no shared state, so it
  // runs without the mutex. The answer below re-verifies pending under
  // one lock hold, so a racing second responder finds the id gone and
  // fails instead of answering twice.
  (void)pthread_mutex_lock(&st->mutex);
  {
    bool found = false;
    for (size_t i = 0; i < st->n_pending; i++) {
      if (st->pending[i] == seq) {
        want_echo = st->pending_is_echo[i];
        found = true;
        break;
      }
    }
    handle = st->ffi_handle;
    if (!found) {
      (void)pthread_mutex_unlock(&st->mutex);
      return weld_fail_invalid("rmw_weld: rmw_send_response unknown sequence");
    }
  }
  (void)pthread_mutex_unlock(&st->mutex);
  uint8_t echo[8];
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  if (want_echo) {
    weld_seq_write(seq, (char *)echo);
    wire = echo;
    wire_len = sizeof(echo);
  } else {
    const rosidl_typesupport_introspection_c__MessageMembers * resp_members =
      weld_service_side_members(st->type_support, false);
    if (resp_members == NULL) {
      return weld_fail_error(
        "rmw_weld: rmw_send_response type support is not introspection");
    }
    errbuf[0] = '\0';
    weld_cdr_status_t enc = weld_cdr_encode_members(
      resp_members, ros_response, true, &wire, &wire_len, errbuf,
      sizeof(errbuf));
    if (enc != WELD_CDR_OK) {
      rmw_reset_error();
      if (errbuf[0] != '\0') {
        RCUTILS_SET_ERROR_MSG(errbuf);
      } else {
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_send_response encode failed");
      }
      free(wire);
      return RMW_RET_ERROR;
    }
    if (wire_len > (size_t)INT_MAX) {
      free(wire);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_send_response payload too large");
      return RMW_RET_ERROR;
    }
  }
  // The Go respond never blocks for the in-memory leg; the RTPS leg
  // publishes with its own bound. Lookup plus respond plus removal stay
  // under one endpoint mutex hold so two racing responders cannot
  // answer the same sequence twice: the loser finds the id gone. The
  // FFI call under lock is safe only because the Go respond path
  // never calls back into C; keep it that way.
  (void)pthread_mutex_lock(&st->mutex);
  {
    size_t at = st->n_pending;
    for (size_t i = 0; i < st->n_pending; i++) {
      if (st->pending[i] == seq) {
        at = i;
        break;
      }
    }
    if (at == st->n_pending) {
      (void)pthread_mutex_unlock(&st->mutex);
      if (!want_echo) {
        free(wire);
      }
      return weld_fail_invalid("rmw_weld: rmw_send_response unknown sequence");
    }
    int rc = weld_ffi_service_respond(
      handle, (long long)seq, (const char *)wire, (int)wire_len);
    if (!want_echo) {
      free(wire);
      wire = NULL;
    }
    if (rc != 0) {
      (void)pthread_mutex_unlock(&st->mutex);
      return weld_fail_error("rmw_weld: rmw_send_response respond failed");
    }
    weld_rpc_remove_locked(st, seq);
  }
  (void)pthread_mutex_unlock(&st->mutex);
  return RMW_RET_OK;
}

rmw_client_t * rmw_create_client(
  const rmw_node_t * node, const rosidl_service_type_support_t * type_support,
  const char * service_name, const rmw_qos_profile_t * qos_profile)
{
  // Bridge: clients share the FFI service handle for their service name
  // (see weld_svc_acquire) so a client send rendezvous with the
  // server serve_nb on the same Go Service. The type support drives CDR
  // encode through the introspection request members; the QoS is stored
  // but otherwise unused.
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
  if (weld_service_side_members(type_support, true) == NULL ||
    weld_service_side_members(type_support, false) == NULL)
  {
    (void)weld_fail_invalid(
      "rmw_weld: rmw_create_client type support is not introspection");
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
  st->guid_salt =
    (uint64_t)atomic_fetch_add(&g_weld_guid_salt, 1);
  st->qos = *qos_profile;
  st->type_support = type_support;
  st->domain = node->context != NULL ? node->context->actual_domain_id : 0;
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
  // Record the endpoint for info-by-service queries. Best effort: a
  // missed record only shrinks info snapshots.
  weld_ep_register(
    WELD_EP_CLI, node->name, node->namespace_,
    node->context != NULL ? node->context->actual_domain_id : 0, st);
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
  weld_ep_unregister(st);
  weld_rpc_dispatch_stop(st);
  // Release the shared FFI handle before touching local state: on
  // failure nothing is freed, so the handle stays valid and the
  // caller may retry the destroy instead of using freed memory.
  rmw_ret_t rc = RMW_RET_OK;
  if (st->name != NULL) {
    rc = weld_svc_release(st->name);
  } else {
    rc = weld_fail_invalid("rmw_weld: rmw_destroy_client no name");
  }
  if (rc != RMW_RET_OK) {
    return rc;
  }
  weld_rpc_free_stash(st);
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
  // Bridge: send encodes the request as CDR, records a fresh sequence id,
  // and delivers immediately: in-memory servers rendezvous through the
  // Go core while remotes receive an RTPS publish. Eager delivery lets
  // a waiting server take the request even when this client never
  // takes; the matching rmw_take_response only waits for the reply.
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
  if (weld_service_side_members(st->type_support, true) == NULL) {
    return weld_fail_error(
      "rmw_weld: rmw_send_request type support is not introspection");
  }
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  errbuf[0] = '\0';
  weld_cdr_status_t enc = weld_cdr_encode_members(
    weld_service_side_members(st->type_support, true), ros_request, true,
    &wire, &wire_len, errbuf, sizeof(errbuf));
  if (enc != WELD_CDR_OK) {
    free(wire);
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_send_request encode failed");
    }
    return RMW_RET_ERROR;
  }
  (void)pthread_mutex_lock(&st->mutex);
  if (st->n_pending >= WELD_RPC_PENDING_MAX) {
    (void)pthread_mutex_unlock(&st->mutex);
    free(wire);
    return weld_fail_error("rmw_weld: rmw_send_request too many pending");
  }
  if (st->next_seq == INT64_MAX) {
    (void)pthread_mutex_unlock(&st->mutex);
    free(wire);
    return weld_fail_error("rmw_weld: rmw_send_request sequence exhausted");
  }
  int64_t seq = st->next_seq++;
  long long handle = st->ffi_handle;
  // Record the send leg with the id: the take waits on this leg
  // instead of re-deciding, so a server created or destroyed between
  // send and take cannot strand the reply on the other leg.
  bool local = weld_svc_has_local_server(st);
  st->pending[st->n_pending] = seq;
  st->pending_is_echo[st->n_pending] = false;
  st->pending_local[st->n_pending] = local;
  st->n_pending++;
  (void)pthread_mutex_unlock(&st->mutex);
  // Deliver eagerly so a polling server wakes even when this client
  // never takes: the old deferred send deadlocked rcl executors, where
  // the server waits for a request the client only emits from take.
  // Local servers receive through the in-memory rendezvous, remotes
  // through an immediate RTPS publish; delivery runs without the
  // endpoint mutex and failures remove the pending entry loudly. The
  // FFI send copies the bytes, so the wire buffer is freed here on
  // every path and no request stash is kept.
  {
    uint8_t guid[8];
    int rc = 0;
    if (wire_len > (size_t)INT_MAX) {
      (void)pthread_mutex_lock(&st->mutex);
      weld_rpc_remove_locked(st, seq);
      (void)pthread_mutex_unlock(&st->mutex);
      free(wire);
      return weld_fail_error("rmw_weld: rmw_send_request payload too large");
    }
    weld_svc_guid(st, seq, guid);
    (void)pthread_mutex_lock(&st->mutex);
    {
      bool found = false;
      for (size_t i = 0; i < st->n_pending; i++) {
        if (st->pending[i] == seq) {
          local = st->pending_local[i];
          found = true;
          break;
        }
      }
      if (!found) {
        local = weld_svc_has_local_server(st);
      }
    }
    (void)pthread_mutex_unlock(&st->mutex);
    if (local) {
      if (weld_ffi_service_send_nb == NULL) {
        (void)pthread_mutex_lock(&st->mutex);
        weld_rpc_remove_locked(st, seq);
        (void)pthread_mutex_unlock(&st->mutex);
        free(wire);
        return weld_fail_unsupported(
          "rmw_weld: rmw_send_request send not linked yet");
      }
      rc = weld_ffi_service_send_nb(
        handle, (long long)seq, (const char *)guid,
        (const char *)wire, (int)wire_len);
      if (rc != 0) {
        (void)pthread_mutex_lock(&st->mutex);
        weld_rpc_remove_locked(st, seq);
        (void)pthread_mutex_unlock(&st->mutex);
        free(wire);
        return weld_fail_ffi("rmw_weld: rmw_send_request send failed", rc);
      }
    } else {
      char base[512];
      if (weld_ffi_service_send_rtps == NULL) {
        (void)pthread_mutex_lock(&st->mutex);
        weld_rpc_remove_locked(st, seq);
        (void)pthread_mutex_unlock(&st->mutex);
        free(wire);
        return weld_fail_unsupported(
          "rmw_weld: rmw_send_request rtps send not linked yet");
      }
      if (weld_service_idl_base(st->type_support, base, sizeof(base)) != 0) {
        (void)pthread_mutex_lock(&st->mutex);
        weld_rpc_remove_locked(st, seq);
        (void)pthread_mutex_unlock(&st->mutex);
        free(wire);
        return weld_fail_invalid(
          "rmw_weld: rmw_send_request type support has no service name");
      }
      rc = weld_ffi_service_send_rtps(
        handle, (long long)seq, (const char *)guid, base,
        (const char *)wire, (int)wire_len);
      if (rc != 0) {
        (void)pthread_mutex_lock(&st->mutex);
        weld_rpc_remove_locked(st, seq);
        (void)pthread_mutex_unlock(&st->mutex);
        free(wire);
        return weld_fail_ffi(
          "rmw_weld: rmw_send_request rtps send failed", rc);
      }
    }
    free(wire);
  }
  *sequence_id = seq;
  return RMW_RET_OK;
}

// Forward declarations: the take splits into local plus RTPS legs below.
static rmw_ret_t weld_take_response_local(
  struct weld_rpc_state * st, long long handle, int64_t seq,
  const uint8_t guid[8],
  const rosidl_typesupport_introspection_c__MessageMembers * resp_members,
  weld_service_info_t * request_header, void * ros_response, bool * taken);

static rmw_ret_t weld_take_response_rtps(
  struct weld_rpc_state * st, long long handle, int64_t seq,
  const uint8_t guid[8],
  const rosidl_typesupport_introspection_c__MessageMembers * resp_members,
  weld_service_info_t * request_header, void * ros_response, bool * taken);

rmw_ret_t rmw_take_response(
  const rmw_client_t * client, weld_service_info_t * request_header,
  void * ros_response, bool * taken)
{
  // Bridge: the request already left in rmw_send_request, so take only
  // waits for the reply. Local servers answer through the in-memory
  // rendezvous parked by the eager send; remotes answer over RTPS. The
  // reply decodes through the introspection response members; a leftover
  // 8 byte sequence echo from the old in-memory path matches back to the
  // pending id instead. Each leg blocks up to the FFI internal bound;
  // the pending entry is kept so a later take can retry.
  if (client == NULL || request_header == NULL || ros_response == NULL ||
    taken == NULL)
  {
    return weld_fail_invalid("rmw_weld: rmw_take_response null argument");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl("rmw_weld: rmw_take_response wrong implementation");
  }
  // Default to not taken: every error return below then leaves a
  // defined value instead of untouched caller memory.
  *taken = false;
  struct weld_rpc_state * st = (struct weld_rpc_state *)client->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_response no data");
  }
  if (weld_ffi_service_poll_reply == NULL ||
    weld_ffi_service_take_reply == NULL)
  {
    return weld_fail_unsupported("rmw_weld: rmw_take_response take not linked yet");
  }
  const rosidl_typesupport_introspection_c__MessageMembers * resp_members =
    weld_service_side_members(st->type_support, false);
  if (resp_members == NULL) {
    return weld_fail_error(
      "rmw_weld: rmw_take_response type support is not introspection");
  }
  (void)pthread_mutex_lock(&st->mutex);
  if (st->n_pending == 0) {
    *taken = false;
    (void)pthread_mutex_unlock(&st->mutex);
    return RMW_RET_OK;
  }
  // Snapshot the pending ids and try each in order: waiting only on
  // the head would stall when a later reply arrives first. Polls are
  // short, and depth is capped, so the scan stays bounded. Each id
  // waits on the leg recorded at send time, never a fresh decision.
  int64_t seqs[WELD_RPC_PENDING_MAX];
  bool legs[WELD_RPC_PENDING_MAX];
  size_t n_seqs = st->n_pending;
  for (size_t i = 0; i < n_seqs; i++) {
    seqs[i] = st->pending[i];
    legs[i] = st->pending_local[i];
  }
  long long handle = st->ffi_handle;
  (void)pthread_mutex_unlock(&st->mutex);
  // Local means a server lived in this process at send time: the
  // shared entry counts servers separately from clients, so a lone
  // client never mistakes its own shared handle for a server.
  for (size_t i = 0; i < n_seqs; i++) {
    uint8_t guid[8];
    rmw_ret_t try_rc = RMW_RET_OK;
    weld_svc_guid(st, seqs[i], guid);
    *taken = false;
    if (legs[i]) {
      try_rc = weld_take_response_local(
        st, handle, seqs[i], guid, resp_members, request_header,
        ros_response, taken);
    } else {
      try_rc = weld_take_response_rtps(
        st, handle, seqs[i], guid, resp_members, request_header,
        ros_response, taken);
    }
    if (try_rc != RMW_RET_OK) {
      return try_rc;
    }
    if (*taken) {
      return RMW_RET_OK;
    }
  }
  *taken = false;
  return RMW_RET_OK;
}

// Take one reply over the in-memory fast path.
//
// The reply was parked by the eager send; this leg only waits bounded
// for it and consumes it. The reply decodes as CDR with an 8 byte echo
// fallback for old servers. Short buffers grow and retry up to the
// same 32MB cap as the RTPS leg because the FFI layer reports short
// without consuming. Returns with the pending entry removed on success
// and kept otherwise.
static rmw_ret_t weld_take_response_local(
  struct weld_rpc_state * st, long long handle, int64_t seq,
  const uint8_t guid[8],
  const rosidl_typesupport_introspection_c__MessageMembers * resp_members,
  weld_service_info_t * request_header, void * ros_response, bool * taken)
{
  size_t cap = 64 * 1024;
  const size_t cap_max = (size_t)(32 << 20);
  uint8_t * buf = NULL;
  int out_n = 0;
  int rc = 0;
  char errbuf[256];
  buf = (uint8_t *)malloc(cap > 0 ? cap : 1);
  if (buf == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response out of memory");
    return RMW_RET_BAD_ALLOC;
  }
  if (weld_ffi_service_poll_reply == NULL) {
    free(buf);
    return weld_fail_unsupported("rmw_weld: rmw_take_response poll not linked yet");
  }
  for (;;) {
    uint8_t * next = NULL;
    if (cap > (size_t)INT_MAX) {
      cap = (size_t)INT_MAX;
    }
    rc = weld_ffi_service_poll_reply(
      handle, (long long)seq, (const char *)guid, (char *)buf, (int)cap,
      &out_n);
    if (rc == 1) {
      free(buf);
      *taken = false;
      return RMW_RET_OK;
    }
    if (rc == -4) {
      if (cap >= cap_max) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response reply too large");
        return RMW_RET_ERROR;
      }
      cap *= 2;
      if (cap > cap_max) {
        cap = cap_max;
      }
      next = (uint8_t *)realloc(buf, cap);
      if (next == NULL) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response out of memory");
        return RMW_RET_BAD_ALLOC;
      }
      buf = next;
      continue;
    }
    if (rc != 0) {
      free(buf);
      return weld_fail_error("rmw_weld: rmw_take_response poll failed");
    }
    break;
  }
  if (out_n < 0) {
    free(buf);
    return weld_fail_error("rmw_weld: rmw_take_response bad length");
  }
  errbuf[0] = '\0';
  weld_cdr_status_t dec = weld_cdr_decode_members(
    resp_members, buf, (size_t)out_n, ros_response, errbuf, sizeof(errbuf));
  if (dec == WELD_CDR_OK) {
    (void)pthread_mutex_lock(&st->mutex);
    weld_rpc_remove_locked(st, seq);
    (void)pthread_mutex_unlock(&st->mutex);
    request_header->source_timestamp = 0;
    request_header->received_timestamp = 0;
    (void)memset(
      request_header->request_id.writer_guid, 0,
      sizeof(request_header->request_id.writer_guid));
    request_header->request_id.sequence_number = seq;
    *taken = true;
    free(buf);
    return RMW_RET_OK;
  }
  // Old in-memory echo: exactly 8 bytes with no CDR header.
  if (out_n == 8 && buf[0] != 0x00) {
    int64_t echo = weld_seq_read((const char *)buf);
    if (echo == seq) {
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
      free(buf);
      return RMW_RET_OK;
    }
  }
  free(buf);
  rmw_reset_error();
  if (errbuf[0] != '\0') {
    RCUTILS_SET_ERROR_MSG(errbuf);
  } else {
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response decode failed");
  }
  return RMW_RET_ERROR;
}

// Take one reply over the RTPS leg.
//
// The request already published at send time; this leg only polls for
// the matching reply with a bounded wait. A missing RTPS symbol means
// the peer library predates cross-process services, so the take fails
// loudly instead of reporting not ready and hiding the mismatch.
// Short buffers grow and retry without republishing because the FFI
// layer stashes the consumed reply.
static rmw_ret_t weld_take_response_rtps(
  struct weld_rpc_state * st, long long handle, int64_t seq,
  const uint8_t guid[8],
  const rosidl_typesupport_introspection_c__MessageMembers * resp_members,
  weld_service_info_t * request_header, void * ros_response, bool * taken)
{
  size_t cap = 64 * 1024;
  const size_t cap_max = (size_t)(32 << 20);
  char base[512];
  uint8_t * buf = NULL;
  int out_n = 0;
  char errbuf[256];
  weld_cdr_status_t dec;
  if (weld_ffi_service_take_reply == NULL) {
    return weld_fail_unsupported("rmw_weld: rmw_take_response take not linked yet");
  }
  if (weld_service_idl_base(st->type_support, base, sizeof(base)) != 0) {
    return weld_fail_invalid(
      "rmw_weld: rmw_take_response type support has no service name");
  }
  buf = (uint8_t *)malloc(cap > 0 ? cap : 1);
  if (buf == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response out of memory");
    return RMW_RET_BAD_ALLOC;
  }
  for (;;) {
    int rc = 0;
    if (cap > (size_t)INT_MAX) {
      cap = (size_t)INT_MAX;
    }
    rc = weld_ffi_service_take_reply(
      handle, (long long)seq, (const char *)guid, base,
      (char *)buf, (int)cap, &out_n);
    if (rc == 1) {
      free(buf);
      *taken = false;
      return RMW_RET_OK;
    }
    if (rc == -4) {
      uint8_t * next = NULL;
      if (cap >= cap_max) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response reply too large");
        return RMW_RET_ERROR;
      }
      cap *= 2;
      if (cap > cap_max) {
        cap = cap_max;
      }
      next = (uint8_t *)realloc(buf, cap);
      if (next == NULL) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response out of memory");
        return RMW_RET_BAD_ALLOC;
      }
      buf = next;
      continue;
    }
    if (rc != 0) {
      free(buf);
      return weld_fail_error("rmw_weld: rmw_take_response take failed");
    }
    break;
  }
  if (out_n < 0) {
    free(buf);
    return weld_fail_error("rmw_weld: rmw_take_response bad length");
  }
  errbuf[0] = '\0';
  dec = weld_cdr_decode_members(
    resp_members, buf, (size_t)out_n, ros_response, errbuf, sizeof(errbuf));
  free(buf);
  if (dec != WELD_CDR_OK) {
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response decode failed");
    }
    return RMW_RET_ERROR;
  }
  (void)pthread_mutex_lock(&st->mutex);
  weld_rpc_remove_locked(st, seq);
  (void)pthread_mutex_unlock(&st->mutex);
  request_header->source_timestamp = 0;
  request_header->received_timestamp = 0;
  (void)memset(
    request_header->request_id.writer_guid, 0,
    sizeof(request_header->request_id.writer_guid));
  request_header->request_id.sequence_number = seq;
  *taken = true;
  return RMW_RET_OK;
}

// Probe one service for a pending request without consuming.
//
// has_request only reports a count, so this never disturbs takes.
static bool weld_svc_wait_probe(struct weld_rpc_state * st)
{
  long long handle = 0;
  int n = 0;
  if (st == NULL || weld_ffi_service_has_request == NULL) {
    return false;
  }
  handle = weld_rpc_handle(st);
  if (handle == 0) {
    return false;
  }
  if (weld_ffi_service_has_request(handle, &n) != 0) {
    return false;
  }
  return n > 0;
}

// Probe one client for a matching reply without consuming.
//
// Only the oldest pending sequence is checked; a reply for it means
// take_response will succeed. Missing type support or FFI reports not
// ready so the dispatcher never blocks creation paths.
static bool weld_cli_wait_probe(struct weld_rpc_state * st)
{
  int64_t seq = 0;
  const rosidl_service_type_support_t * ts = NULL;
  long long handle = 0;
  uint8_t guid[8];
  char base[512];
  int n = 0;
  if (st == NULL || weld_ffi_service_has_response == NULL) {
    return false;
  }
  (void)pthread_mutex_lock(&st->mutex);
  if (st->n_pending == 0) {
    (void)pthread_mutex_unlock(&st->mutex);
    return false;
  }
  seq = st->pending[0];
  ts = st->type_support;
  handle = st->ffi_handle;
  (void)pthread_mutex_unlock(&st->mutex);
  if (handle == 0 || ts == NULL) {
    return false;
  }
  weld_svc_guid(st, seq, guid);
  if (weld_service_idl_base(ts, base, sizeof(base)) != 0) {
    return false;
  }
  if (weld_ffi_service_has_response(
      handle, (long long)seq, (const char *)guid, base, &n) != 0)
  {
    return false;
  }
  return n > 0;
}

// RPC dispatch thread: fire the stored on-new callback once per
// readiness edge. The is_service flag selects the request probe
// (services) or the reply probe (clients). Same contract as
// subscriptions: no consumption, no locked invocation, FFI absence
// ends the thread.
static void * weld_rpc_dispatch_thread(void * arg)
{
  struct weld_rpc_state * st = (struct weld_rpc_state *)arg;
  if (st == NULL) {
    return NULL;
  }
  for (;;) {
    bool run = false;
    bool is_service = false;
    rmw_event_callback_t cb = NULL;
    const void * user_data = NULL;
    bool ready = false;
    (void)pthread_mutex_lock(&st->mutex);
    run = st->dispatch_run;
    is_service = st->is_service;
    cb = st->on_new_callback;
    user_data = st->on_new_user_data;
    (void)pthread_mutex_unlock(&st->mutex);
    if (!run) {
      return NULL;
    }
    if (weld_ffi_service_has_request == NULL ||
      weld_ffi_service_has_response == NULL)
    {
      return NULL;
    }
    if (is_service) {
      ready = weld_svc_wait_probe(st);
    } else {
      ready = weld_cli_wait_probe(st);
    }
    if (ready && cb != NULL) {
      bool fire = false;
      (void)pthread_mutex_lock(&st->mutex);
      fire = st->dispatch_run && !st->dispatch_fired;
      if (fire) {
        st->dispatch_fired = true;
      }
      (void)pthread_mutex_unlock(&st->mutex);
      if (fire) {
        cb(user_data, 1);
      }
    } else if (!ready) {
      (void)pthread_mutex_lock(&st->mutex);
      st->dispatch_fired = false;
      (void)pthread_mutex_unlock(&st->mutex);
    }
    weld_dispatch_sleep();
  }
}

// Start the endpoint dispatch thread on the first callback.
//
// Called with no locks held. Shares the subscription contract: start
// failures keep the stored callback without dispatch.
static void weld_rpc_dispatch_start(struct weld_rpc_state * st)
{
  int rc = 0;
  bool start = false;
  if (st == NULL) {
    return;
  }
  (void)pthread_mutex_lock(&st->mutex);
  start = !st->dispatch_started && st->on_new_callback != NULL;
  if (start) {
    st->dispatch_run = true;
    st->dispatch_fired = false;
  }
  (void)pthread_mutex_unlock(&st->mutex);
  if (!start) {
    return;
  }
  rc = pthread_create(
    &st->dispatch_thread, NULL, weld_rpc_dispatch_thread, st);
  (void)pthread_mutex_lock(&st->mutex);
  if (rc != 0) {
    st->dispatch_run = false;
  } else {
    st->dispatch_started = true;
  }
  (void)pthread_mutex_unlock(&st->mutex);
}

// Stop the endpoint dispatch thread and wait for its exit.
//
// Called with no locks held. Joining before teardown guarantees the
// thread never touches freed endpoint state.
static void weld_rpc_dispatch_stop(struct weld_rpc_state * st)
{
  bool started = false;
  if (st == NULL) {
    return;
  }
  (void)pthread_mutex_lock(&st->mutex);
  st->dispatch_run = false;
  started = st->dispatch_started;
  (void)pthread_mutex_unlock(&st->mutex);
  if (started) {
    (void)pthread_join(st->dispatch_thread, NULL);
  }
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

// Resolve one wait service entry to its data.
//
// Same dual convention as subscriptions: rcl passes weld_rpc_state
// while direct C callers pass the rmw wrapper.
static struct weld_rpc_state * weld_wait_svc_entry(void * entry)
{
  const rmw_service_t * svc = NULL;
  if (entry == NULL) {
    return NULL;
  }
  svc = (const rmw_service_t *)entry;
  if (weld_impl_eq(svc->implementation_identifier)) {
    return (struct weld_rpc_state *)svc->data;
  }
  return (struct weld_rpc_state *)entry;
}

// Resolve one wait client entry to its data.
//
// Same dual convention as services.
static struct weld_rpc_state * weld_wait_cli_entry(void * entry)
{
  const rmw_client_t * cli = NULL;
  if (entry == NULL) {
    return NULL;
  }
  cli = (const rmw_client_t *)entry;
  if (weld_impl_eq(cli->implementation_identifier)) {
    return (struct weld_rpc_state *)cli->data;
  }
  return (struct weld_rpc_state *)entry;
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
  // rcl/wait.c, so subscriptions carry weld_sub_data, guards carry
  // weld_guard_state, and services plus clients carry weld_rpc_state.
  // Subscriptions are ready when a wire or in-memory sample is
  // available (polled into the wait stash so the next take returns
  // it); guards are ready when triggered. A service is ready when a
  // request is pending: the peek consumes into the FFI stash so the
  // next take returns it. A client is ready when its reply is
  // available: the reply peek reports a parked in-memory reply as
  // well as an RTPS wire reply, so local clients fire here too.
  // MESSAGE_LOST events are ready while untaken decode drops exist;
  // liveliness events fire on matched-count edges, deadline events on
  // cumulative miss totals, and incompatible-QoS events on local plus
  // wire mismatch totals. Unknown event values fail loudly with
  // UNSUPPORTED. NULL slots are empty from a previous wait filter and
  // skip.
  //
  // The poll loop sleeps on g_weld_wait_cond with a 10 ms quantum
  // instead of busy polling: guard triggers broadcast the cond for an
  // immediate wakeup, and expiry or arrival is re-polled after every
  // wakeup, so spurious wakeups stay correct by construction.
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
  if (subscriptions == NULL && guard_conditions == NULL &&
    services == NULL && clients == NULL && events == NULL)
  {
    return weld_fail_invalid("rmw_weld: rmw_wait no sets to wait on");
  }
  {
    // Reject sourceless events up front: every known event type below has
    // a live source (decode drops, matched counts, deadline totals, or
    // incompat totals). Unknown event values report UNSUPPORTED per type
    // instead of silently never firing.
    if (events != NULL) {
      for (size_t i = 0; i < events->event_count; i++) {
        const rmw_event_t * ev =
          (const rmw_event_t *)events->events[i];
        if (ev == NULL) {
          continue;
        }
        if (ev->implementation_identifier != kImplementationIdentifier) {
          rmw_reset_error();
          RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_wait foreign event");
          return RMW_RET_UNSUPPORTED;
        }
        switch (ev->event_type) {
          case RMW_EVENT_MESSAGE_LOST:
          case RMW_EVENT_LIVELINESS_CHANGED:
          case RMW_EVENT_LIVELINESS_LOST:
          case RMW_EVENT_REQUESTED_DEADLINE_MISSED:
          case RMW_EVENT_OFFERED_DEADLINE_MISSED:
          case RMW_EVENT_REQUESTED_QOS_INCOMPATIBLE:
          case RMW_EVENT_OFFERED_QOS_INCOMPATIBLE:
            break;
          default: {
            char buf[128];
            (void)snprintf(
              buf, sizeof(buf),
              "rmw_weld: rmw_wait event type %d has no source",
              (int)ev->event_type);
            buf[sizeof(buf) - 1] = '\0';
            rmw_reset_error();
            RCUTILS_SET_ERROR_MSG(buf);
            return RMW_RET_UNSUPPORTED;
          }
        }
      }
    }
  }
  {
    // Enforce the wait set bound: more attached conditions than the
    // set was created for fails loudly instead of polling them.
    struct weld_wait_state * st = (struct weld_wait_state *)wait_set->data;
    size_t total = 0;
    if (st != NULL && st->max_conditions > 0) {
      if (subscriptions != NULL) {
        total += subscriptions->subscriber_count;
      }
      if (guard_conditions != NULL) {
        total += guard_conditions->guard_condition_count;
      }
      if (services != NULL) {
        total += services->service_count;
      }
      if (clients != NULL) {
        total += clients->client_count;
      }
      if (events != NULL) {
        total += events->event_count;
      }
      if (total > st->max_conditions) {
        return weld_fail_invalid("rmw_weld: rmw_wait too many conditions");
      }
    }
  }
  (void)pthread_once(&g_weld_wait_once, weld_wait_cond_init);
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
    // Count and filter in one pass: a second poll after counting
    // could flap on an FFI error and drop a ready entry (or keep a
    // dead one), so each entry is polled once and filtered at once.
    if (guard_conditions != NULL) {
      for (size_t i = 0; i < guard_conditions->guard_condition_count; i++) {
        struct weld_guard_state * st =
          weld_wait_guard_entry(guard_conditions->guard_conditions[i]);
        if (st != NULL && weld_guard_state_triggered(st)) {
          ready++;
        } else {
          guard_conditions->guard_conditions[i] = NULL;
        }
      }
    }
    if (subscriptions != NULL) {
      for (size_t i = 0; i < subscriptions->subscriber_count; i++) {
        struct weld_sub_data * sd =
          weld_wait_sub_entry(subscriptions->subscribers[i]);
        if (sd != NULL && weld_sub_wait_poll(sd)) {
          ready++;
        } else {
          subscriptions->subscribers[i] = NULL;
        }
      }
    }
    if (services != NULL) {
      for (size_t i = 0; i < services->service_count; i++) {
        struct weld_rpc_state * st =
          weld_wait_svc_entry(services->services[i]);
        if (st != NULL && weld_svc_wait_probe(st)) {
          ready++;
        } else {
          services->services[i] = NULL;
        }
      }
    }
    if (clients != NULL) {
      for (size_t i = 0; i < clients->client_count; i++) {
        struct weld_rpc_state * st =
          weld_wait_cli_entry(clients->clients[i]);
        if (st != NULL && weld_cli_wait_probe(st)) {
          ready++;
        } else {
          clients->clients[i] = NULL;
        }
      }
    }
    if (events != NULL) {
      for (size_t i = 0; i < events->event_count; i++) {
        const rmw_event_t * ev =
          (const rmw_event_t *)events->events[i];
        if (ev != NULL && weld_event_bound_ready(ev)) {
          ready++;
        } else {
          events->events[i] = NULL;
        }
      }
    }
    if (ready > 0) {
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
      // Sleep on the wake cond with a 10 ms quantum so guard triggers
      // wake the wait early while data arrival is re-polled promptly.
      // Re-polling after every wakeup keeps spurious wakeups correct.
      const uint64_t quantum_ns = 10 * 1000 * 1000ULL;
      uint64_t now_ns =
        (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
      uint64_t wake_ns = now_ns + quantum_ns;
      struct timespec abstime;
      if (!infinite && deadline_ns - now_ns < quantum_ns) {
        wake_ns = deadline_ns;
      }
      abstime.tv_sec = (time_t)(wake_ns / 1000000000ULL);
      abstime.tv_nsec = (long)(wake_ns % 1000000000ULL);
      (void)pthread_mutex_lock(&g_weld_wait_mutex);
      (void)pthread_cond_timedwait(
        &g_weld_wait_cond, &g_weld_wait_mutex, &abstime);
      (void)pthread_mutex_unlock(&g_weld_wait_mutex);
    }
  }
}

// Forward declarations: event binding snapshots live in the take section
// below, but init paths above bind before take runs.
static void weld_event_snapshot_locked(struct weld_event_slot * slot);
static int weld_event_bind(
  const rmw_event_t * handle, rmw_event_type_t event_type,
  long long endpoint_handle, const void * endpoint, bool is_pub);
static bool weld_event_is_pub_type(rmw_event_type_t event_type);
static bool weld_event_is_sub_type(rmw_event_type_t event_type);

rmw_ret_t rmw_publisher_event_init(
  rmw_event_t * rmw_event, const rmw_publisher_t * publisher,
  rmw_event_type_t event_type)
{
  const struct weld_pub_data * pd = NULL;
  if (rmw_event == NULL || publisher == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publisher_event_init null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publisher_event_init wrong impl");
  }
  // Publisher side types carry live sources; anything else stays
  // unsupported with the event left untouched per RMW error-path rules.
  // Unknown future values report unsupported with their numeric type.
  if (!weld_event_is_pub_type(event_type)) {
    char buf[128];
    (void)snprintf(
      buf, sizeof(buf),
      "rmw_weld: rmw_publisher_event_init type %d unsupported",
      (int)event_type);
    buf[sizeof(buf) - 1] = '\0';
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(buf);
    return RMW_RET_UNSUPPORTED;
  }
  pd = (const struct weld_pub_data *)publisher->data;
  if (pd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publisher_event_init no data");
    return RMW_RET_ERROR;
  }
  rmw_event->implementation_identifier = kImplementationIdentifier;
  rmw_event->data = NULL;
  rmw_event->event_type = event_type;
  if (weld_event_bind(
      rmw_event, event_type, pd->handle, pd, true) != 0)
  {
    rmw_event->implementation_identifier = NULL;
    rmw_event->data = NULL;
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_publisher_event_init too many events");
    return RMW_RET_ERROR;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_event_init(
  rmw_event_t * rmw_event, const rmw_subscription_t * subscription,
  rmw_event_type_t event_type)
{
  const struct weld_sub_data * sd = NULL;
  if (rmw_event == NULL || subscription == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_event_init null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_event_init wrong impl");
  }
  // Subscription side types carry live sources; anything else stays
  // unsupported with the event left untouched per RMW error-path rules.
  if (!weld_event_is_sub_type(event_type)) {
    char buf[128];
    (void)snprintf(
      buf, sizeof(buf),
      "rmw_weld: rmw_subscription_event_init type %d unsupported",
      (int)event_type);
    buf[sizeof(buf) - 1] = '\0';
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(buf);
    return RMW_RET_UNSUPPORTED;
  }
  sd = (const struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_subscription_event_init no data");
    return RMW_RET_ERROR;
  }
  rmw_event->implementation_identifier = kImplementationIdentifier;
  rmw_event->data = NULL;
  rmw_event->event_type = event_type;
  if (weld_event_bind(
      rmw_event, event_type, sd->handle, sd, false) != 0)
  {
    rmw_event->implementation_identifier = NULL;
    rmw_event->data = NULL;
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_event_init too many events");
    return RMW_RET_ERROR;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_event_fini(rmw_event_t * event)
{
  if (event == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_event_fini null argument");
  }
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  weld_event_slot_release_locked(event);
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
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
    // entry without touching the count. Without the FFI symbol the
    // answer stays local-only instead of failing: an older Go library
    // cannot see RTPS peers, but local servers are still exact.
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
      *is_available = (local > 0);
      return RMW_RET_OK;
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

// QoS scope: this shim stores passthrough QoS and never contacts the
// Go core, so compatibility is decided locally on every policy:
// reliability, durability, deadline, liveliness (plus lease duration),
// history, and depth. Any difference reports WARNING with the differing
// policies named in reason; the transport is best effort in memory, so
// no mismatch is ever a hard error.
// Append one policy label to a comma separated mismatch list.
// Caps the write position so a truncated snprintf never pushes the next
// write past the buffer. Always marks the comparison mismatched.
static void weld_qos_flag(
  char * details, size_t cap, size_t * pos, bool * mismatch,
  const char * label)
{
  int n = 0;
  if (details == NULL || pos == NULL || mismatch == NULL || label == NULL) {
    return;
  }
  if (*pos < cap) {
    n = snprintf(
      details + *pos, cap - *pos, "%s%s", *pos > 0 ? ", " : "", label);
    if (n > 0) {
      *pos += (size_t)n;
      if (*pos >= cap) {
        *pos = cap - 1;
      }
    }
  }
  *mismatch = true;
}

rmw_ret_t rmw_qos_profile_check_compatible(
  const rmw_qos_profile_t publisher_profile,
  const rmw_qos_profile_t subscription_profile,
  rmw_qos_compatibility_type_t * compatibility, char * reason, size_t reason_size)
{
  if (compatibility == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_qos_profile_check_compatible null argument");
  }
  {
    char details[256];
    size_t pos = 0;
    bool mismatch = false;
    details[0] = '\0';
    if (publisher_profile.reliability != subscription_profile.reliability) {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "reliability");
    }
    if (publisher_profile.durability != subscription_profile.durability) {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "durability");
    }
    if (publisher_profile.deadline.sec != subscription_profile.deadline.sec ||
      publisher_profile.deadline.nsec != subscription_profile.deadline.nsec)
    {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "deadline");
    }
    if (publisher_profile.liveliness != subscription_profile.liveliness ||
      publisher_profile.liveliness_lease_duration.sec !=
      subscription_profile.liveliness_lease_duration.sec ||
      publisher_profile.liveliness_lease_duration.nsec !=
      subscription_profile.liveliness_lease_duration.nsec)
    {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "liveliness");
    }
    if (publisher_profile.history != subscription_profile.history) {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "history");
    }
    if (publisher_profile.depth != subscription_profile.depth) {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "depth");
    }
    if (mismatch) {
      *compatibility = RMW_QOS_COMPATIBILITY_WARNING;
      if (reason != NULL && reason_size > 0) {
        (void)snprintf(
          reason, reason_size,
          "rmw_weld: publisher/subscriber QoS differ: %s", details);
        reason[reason_size - 1] = '\0';
      }
      return RMW_RET_OK;
    }
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
  // CDR encode copies the wire bytes into the caller buffer, growing it
  // when the sample does not fit. The message must already be
  // initialized; encoding an uninitialized message is undefined, same as
  // the encoder contract.
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  weld_cdr_status_t st;
  rcutils_ret_t rc = RCUTILS_RET_OK;
  if (ros_message == NULL || type_support == NULL || serialized_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_serialize null argument");
  }
  members = weld_resolve_members(type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_serialize type support is not introspection");
    return RMW_RET_ERROR;
  }
  errbuf[0] = '\0';
  st = weld_cdr_encode_members(
    members, ros_message, true, &wire, &wire_len, errbuf, sizeof(errbuf));
  if (st != WELD_CDR_OK) {
    free(wire);
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_serialize encode failed");
    }
    return RMW_RET_ERROR;
  }
  if (wire_len > serialized_message->buffer_capacity) {
    rc = rmw_serialized_message_resize(serialized_message, wire_len);
    if (rc != RCUTILS_RET_OK) {
      free(wire);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_serialize resize failed");
      return RMW_RET_BAD_ALLOC;
    }
  }
  if (wire_len > 0) {
    if (serialized_message->buffer == NULL) {
      free(wire);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_serialize null buffer");
      return RMW_RET_INVALID_ARGUMENT;
    }
    (void)memcpy(serialized_message->buffer, wire, wire_len);
  }
  free(wire);
  serialized_message->buffer_length = wire_len;
  return RMW_RET_OK;
}

rmw_ret_t rmw_deserialize(
  const rmw_serialized_message_t * serialized_message,
  const rosidl_message_type_support_t * type_support, void * ros_message)
{
  // CDR decode copies the wire bytes into ros_message, which the caller
  // must have initialized with the generated init function. Extra zero
  // pad is accepted; any other trailing bytes fail loudly.
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  char errbuf[256];
  weld_cdr_status_t st;
  if (serialized_message == NULL || type_support == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_deserialize null argument");
  }
  members = weld_resolve_members(type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_deserialize type support is not introspection");
    return RMW_RET_ERROR;
  }
  if (serialized_message->buffer_length > 0 && serialized_message->buffer == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_deserialize null buffer");
  }
  errbuf[0] = '\0';
  st = weld_cdr_decode_members(
    members, serialized_message->buffer, serialized_message->buffer_length,
    ros_message, errbuf, sizeof(errbuf));
  if (st != WELD_CDR_OK) {
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_deserialize decode failed");
    }
    return RMW_RET_ERROR;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_serialized_message_size(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds, size_t * size)
{
  // The maximum encoding size comes from a member walk over upper
  // bounds, mirroring the encoder byte for byte. Unbounded members
  // have no finite size and fail loudly with UNSUPPORTED instead of
  // guessing. The bounds handle is required by the API but carries no
  // additional data the walk needs.
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  char errbuf[256];
  weld_cdr_status_t st;
  (void)message_bounds;
  if (type_support == NULL || message_bounds == NULL || size == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_get_serialized_message_size null argument");
  }
  members = weld_resolve_members(type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_get_serialized_message_size type support is not introspection");
    return RMW_RET_ERROR;
  }
  errbuf[0] = '\0';
  st = weld_cdr_max_serialized_size(members, size, errbuf, sizeof(errbuf));
  if (st != WELD_CDR_OK) {
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_serialized_message_size failed");
    }
    return st == WELD_CDR_UNSUPPORTED ? RMW_RET_UNSUPPORTED : RMW_RET_ERROR;
  }
  return RMW_RET_OK;
}

// Context finalize plus serialization format plus GID compare plus log
// severity plus liveliness plus ack plus allocation plus take variants
// plus client GID. Liveliness asserts report to the Go core when the
// FFI symbol is linked and validate only otherwise; acks return
// immediately because the transport sends best effort with no
// acknowledgments.

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
  const struct weld_node_data * nd = NULL;
  long long bridge = 0;
  unsigned long long gen = 0;
  int rc = 0;
  if (node == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_node_assert_liveliness null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: rmw_node_assert_liveliness wrong implementation");
  }
  nd = (const struct weld_node_data *)node->data;
  if (nd == NULL || nd->node_name == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_node_assert_liveliness no data");
  }
  // Without the FFI symbol the assert stays validation only: SPDP
  // announcements already carry liveliness continuously.
  if (weld_ffi_report_liveliness == NULL) {
    return RMW_RET_OK;
  }
  bridge = nd->bridge;
  if (bridge == 0 || nd->domain > (size_t)INT_MAX) {
    return weld_fail_error("rmw_weld: rmw_node_assert_liveliness no bridge");
  }
  gen = atomic_fetch_add(&g_weld_liveliness_gen, 1);
  rc = weld_ffi_report_liveliness(
    bridge, nd->node_name, (int)nd->domain, 1, gen);
  if (rc != 0) {
    return weld_fail_ffi("rmw_weld: rmw_node_assert_liveliness backend failed", rc);
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_assert_liveliness(const rmw_publisher_t * publisher)
{
  const struct weld_pub_data * pd = NULL;
  long long bridge = 0;
  unsigned long long gen = 0;
  int rc = 0;
  if (publisher == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publisher_assert_liveliness null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publisher_assert_liveliness wrong implementation");
  }
  pd = (const struct weld_pub_data *)publisher->data;
  if (pd == NULL || pd->node_name == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publisher_assert_liveliness no data");
  }
  if (weld_ffi_report_liveliness == NULL) {
    return RMW_RET_OK;
  }
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  bridge = g_weld_bridge;
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  if (bridge == 0 || pd->domain > (size_t)INT_MAX) {
    return weld_fail_error("rmw_weld: rmw_publisher_assert_liveliness no bridge");
  }
  gen = atomic_fetch_add(&g_weld_liveliness_gen, 1);
  rc = weld_ffi_report_liveliness(
    bridge, pd->node_name, (int)pd->domain, 1, gen);
  if (rc != 0) {
    return weld_fail_ffi(
      "rmw_weld: rmw_publisher_assert_liveliness backend failed", rc);
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_wait_for_all_acked(
  const rmw_publisher_t * publisher, rmw_time_t wait_timeout)
{
  // The transport is best effort with no per-sample acknowledgments,
  // so there is nothing to wait for and the call returns immediately
  // without consuming the timeout. Validation only.
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
    unsigned long long seq = 0;
    uint8_t gid[16];
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
    (void)memset(gid, 0, sizeof(gid));
    empty = weld_take_wire(sd, &wire, &wire_len, &seq, gid);
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
    info = &message_info_sequence->data[n];
    weld_fill_take_info(info, seq, gid);
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
  unsigned long long seq = 0;
  uint8_t gid[16];
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
  (void)memset(gid, 0, sizeof(gid));
  empty = weld_take_wire(sd, &wire, &wire_len, &seq, gid);
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
  weld_fill_take_info(message_info, seq, gid);
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
  // Same 16 byte layout as rmw_get_gid_for_publisher: domain from the
  // creating node in the first 4 bytes little endian and the FFI
  // service handle in the next 8, unique within one domain. Synthetic,
  // not a DDS GUID: the FFI layer exposes local endpoint GUIDs
  // nowhere, so remote entries in info queries carry the raw SEDP
  // GUID while local endpoints use this.
  if (st->domain > (size_t)UINT32_MAX) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_gid_for_client domain range");
    return RMW_RET_ERROR;
  }
  domain_u = (uint32_t)st->domain;
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

// Fill a names_and_types snapshot for a known node.
//
// Graph snapshots merge the local node registry with the Go bridge
// registry. Per-node endpoint maps come from the local endpoint
// registry keyed by owning node name plus namespace: by-node queries
// return real entries for locally created endpoints, plus an empty
// snapshot with RMW_RET_OK when the node exists but owns nothing here,
// and RMW_RET_NODE_NAME_NON_EXISTENT otherwise. Info-by-topic queries
// merge local endpoints (stored actual QoS, IDL type, synthetic GID,
// real node names) with remote SEDP peers (announced type, raw 16 byte
// GUID, SEDP QoS where visible, empty node names fail-closed). The
// transport copies bytes and never reports a best-known QoS, so callers
// must not infer absence of peers from size. Loans are owned heap
// buffers, never zero copy: see the loan family above. All event types
// are backed by live sources; unknown event values report UNSUPPORTED
// per type. Content filters attach through the Go backend with the
// expression plus parameters stored per subscription, and network flow
// endpoints are not reported.

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

// Local endpoint registry for by-node plus info-by-topic queries.
//
// Creation records the owning node plus domain with the live endpoint
// data pointer; destruction unregisters before freeing. Queries snapshot
// under the mutex and read only the snapshot, so destruction can never
// race a fill. Kinds are WELD_EP_PUB through WELD_EP_CLI declared near
// the top of this file.

struct weld_ep_entry
{
  int kind;
  char * node_name;
  char * node_ns;
  size_t domain;
  const void * data;
  struct weld_ep_entry * next;
};

static pthread_mutex_t g_weld_ep_mutex = PTHREAD_MUTEX_INITIALIZER;

static struct weld_ep_entry * g_weld_ep_list = NULL;

// Record one live local endpoint. Best effort: alloc failure skips
// silently and info queries report fewer locals.
static void weld_ep_register(
  int kind, const char * node_name, const char * node_ns,
  size_t domain, const void * data)
{
  struct weld_ep_entry * e = NULL;
  if (node_name == NULL || node_ns == NULL || data == NULL) {
    return;
  }
  e = (struct weld_ep_entry *)malloc(sizeof(*e));
  if (e == NULL) {
    return;
  }
  e->node_name = weld_dup_str(node_name);
  e->node_ns = weld_dup_str(node_ns);
  if (e->node_name == NULL || e->node_ns == NULL) {
    free(e->node_name);
    free(e->node_ns);
    free(e);
    return;
  }
  e->kind = kind;
  e->domain = domain;
  e->data = data;
  (void)pthread_mutex_lock(&g_weld_ep_mutex);
  e->next = g_weld_ep_list;
  g_weld_ep_list = e;
  (void)pthread_mutex_unlock(&g_weld_ep_mutex);
}

// Drop one endpoint by data pointer. Missing entries are ignored so
// double destroy stays safe.
static void weld_ep_unregister(const void * data)
{
  if (data == NULL) {
    return;
  }
  (void)pthread_mutex_lock(&g_weld_ep_mutex);
  {
    struct weld_ep_entry ** link = &g_weld_ep_list;
    while (*link != NULL) {
      if ((*link)->data == data) {
        struct weld_ep_entry * dead = *link;
        *link = dead->next;
        free(dead->node_name);
        free(dead->node_ns);
        free(dead);
        break;
      } else {
        link = &(*link)->next;
      }
    }
  }
  (void)pthread_mutex_unlock(&g_weld_ep_mutex);
}

// One snapshotted local endpoint with owned strings plus copied QoS, so
// the fill path never touches live endpoint state.
struct weld_ep_snap
{
  int kind;
  char * node_name;
  char * node_ns;
  char * name;
  size_t domain;
  long long handle;
  rmw_qos_profile_t qos;
  const rosidl_message_type_support_t * type_support;
  const rosidl_service_type_support_t * svc_type_support;
};

static void weld_ep_snapshot_fini(struct weld_ep_snap * items, size_t n)
{
  size_t i = 0;
  if (items == NULL) {
    return;
  }
  for (i = 0; i < n; i++) {
    free(items[i].node_name);
    free(items[i].node_ns);
    free(items[i].name);
  }
  free(items);
}

// Snapshot local endpoints, optionally filtered by topic or service
// name and kind. A NULL name snapshots every endpoint of the kind.
//
// The snapshot copies every field the fill needs, so later destruction
// cannot invalidate it. Returns RMW_RET_OK with a malloc owned array
// (possibly empty) or RMW_RET_BAD_ALLOC with nothing stored.
static rmw_ret_t weld_ep_snapshot(
  const char * name, int kind, size_t domain, bool use_domain,
  struct weld_ep_snap ** out_items, size_t * out_n)
{
  struct weld_ep_snap * items = NULL;
  size_t n = 0;
  size_t cap = 0;
  if (out_items == NULL || out_n == NULL) {
    return weld_fail_invalid("rmw_weld: ep_snapshot null argument");
  }
  if (kind != WELD_EP_PUB && kind != WELD_EP_SUB && kind != WELD_EP_SVC &&
    kind != WELD_EP_CLI)
  {
    return weld_fail_invalid("rmw_weld: ep_snapshot bad kind");
  }
  *out_items = NULL;
  *out_n = 0;
  (void)pthread_mutex_lock(&g_weld_ep_mutex);
  for (struct weld_ep_entry * e = g_weld_ep_list; e != NULL; e = e->next) {
    const char * ep_name = NULL;
    struct weld_ep_snap item;
    struct weld_ep_snap * next = NULL;
    if (e->kind != kind) {
      continue;
    }
    if (use_domain && e->domain != domain) {
      continue;
    }
    if (e->data == NULL) {
      continue;
    }
    (void)memset(&item, 0, sizeof(item));
    if (kind == WELD_EP_PUB) {
      const struct weld_pub_data * pd =
        (const struct weld_pub_data *)e->data;
      if (pd->topic == NULL || (name != NULL && strcmp(pd->topic, name) != 0)) {
        continue;
      }
      ep_name = pd->topic;
      item.handle = pd->handle;
      item.qos = pd->qos;
      item.type_support = pd->type_support;
    } else if (kind == WELD_EP_SUB) {
      const struct weld_sub_data * sd =
        (const struct weld_sub_data *)e->data;
      if (sd->topic == NULL || (name != NULL && strcmp(sd->topic, name) != 0)) {
        continue;
      }
      ep_name = sd->topic;
      item.handle = sd->handle;
      item.qos = sd->qos;
      item.type_support = sd->type_support;
    } else {
      const struct weld_rpc_state * st =
        (const struct weld_rpc_state *)e->data;
      if (st->name == NULL || (name != NULL && strcmp(st->name, name) != 0)) {
        continue;
      }
      ep_name = st->name;
      item.handle = st->ffi_handle;
      item.qos = st->qos;
      item.svc_type_support = st->type_support;
    }
    item.kind = kind;
    item.domain = e->domain;
    item.node_name = weld_dup_str(e->node_name);
    item.node_ns = weld_dup_str(e->node_ns);
    item.name = weld_dup_str(ep_name);
    if (item.node_name == NULL || item.node_ns == NULL ||
      item.name == NULL)
    {
      free(item.node_name);
      free(item.node_ns);
      free(item.name);
      (void)pthread_mutex_unlock(&g_weld_ep_mutex);
      weld_ep_snapshot_fini(items, n);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: ep_snapshot out of memory");
      return RMW_RET_BAD_ALLOC;
    }
    if (n == cap) {
      size_t ncap = cap == 0 ? 4 : cap * 2;
      next = (struct weld_ep_snap *)realloc(
        items, ncap * sizeof(*next));
      if (next == NULL) {
        free(item.node_name);
        free(item.node_ns);
        free(item.name);
        (void)pthread_mutex_unlock(&g_weld_ep_mutex);
        weld_ep_snapshot_fini(items, n);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: ep_snapshot out of memory");
        return RMW_RET_BAD_ALLOC;
      }
      items = next;
      cap = ncap;
    }
    items[n] = item;
    n++;
  }
  (void)pthread_mutex_unlock(&g_weld_ep_mutex);
  *out_items = items;
  *out_n = n;
  return RMW_RET_OK;
}

// Count local incompatibilities at match time for a new endpoint.
//
// Snapshots existing opposite-kind endpoints on the same topic plus
// domain and runs rmw_qos_profile_check_compatible for each pair. Every
// mismatched pair bumps both sides: the existing endpoint counter is
// updated in place under no locks (creation runs single-threaded before
// the new endpoint registers) and the new count returns for the caller
// to store. Compatible pairs and check errors never count, so the total
// stays a real mismatch count instead of a probe failure count.
static size_t weld_incompat_scan(
  int new_kind, const rmw_qos_profile_t * new_qos, const char * topic,
  size_t domain)
{
  struct weld_ep_snap * snaps = NULL;
  size_t n_snaps = 0;
  size_t mismatched = 0;
  int opp_kind = WELD_EP_SUB;
  size_t i = 0;
  if (new_qos == NULL || topic == NULL) {
    return 0;
  }
  if (new_kind == WELD_EP_PUB) {
    opp_kind = WELD_EP_SUB;
  } else if (new_kind == WELD_EP_SUB) {
    opp_kind = WELD_EP_PUB;
  } else {
    return 0;
  }
  if (weld_ep_snapshot(topic, opp_kind, domain, true, &snaps, &n_snaps) !=
    RMW_RET_OK)
  {
    return 0;
  }
  for (i = 0; i < n_snaps; i++) {
    rmw_qos_compatibility_type_t compat = RMW_QOS_COMPATIBILITY_OK;
    rmw_ret_t rc = RMW_RET_OK;
    if (new_kind == WELD_EP_PUB) {
      rc = rmw_qos_profile_check_compatible(
        *new_qos, snaps[i].qos, &compat, NULL, 0);
    } else {
      rc = rmw_qos_profile_check_compatible(
        snaps[i].qos, *new_qos, &compat, NULL, 0);
    }
    if (rc != RMW_RET_OK || compat == RMW_QOS_COMPATIBILITY_OK) {
      continue;
    }
    mismatched++;
  }
  weld_ep_snapshot_fini(snaps, n_snaps);
  // Bumping existing endpoints needs live pointers, so rescan the
  // registry and bump each mismatched endpoint in place. The second scan
  // repeats the same check; creation is rare so the double walk is fine.
  if (mismatched > 0) {
    (void)pthread_mutex_lock(&g_weld_ep_mutex);
    for (struct weld_ep_entry * e = g_weld_ep_list; e != NULL;
      e = e->next)
    {
      rmw_qos_compatibility_type_t compat = RMW_QOS_COMPATIBILITY_OK;
      rmw_ret_t rc = RMW_RET_OK;
      if (e->kind != opp_kind || e->data == NULL) {
        continue;
      }
      if (e->domain != domain) {
        continue;
      }
      if (opp_kind == WELD_EP_PUB) {
        const struct weld_pub_data * pd =
          (const struct weld_pub_data *)e->data;
        if (pd->topic == NULL || strcmp(pd->topic, topic) != 0) {
          continue;
        }
        rc = rmw_qos_profile_check_compatible(
          pd->qos, *new_qos, &compat, NULL, 0);
        if (rc == RMW_RET_OK && compat != RMW_QOS_COMPATIBILITY_OK) {
          ((struct weld_pub_data *)e->data)->incompat_total++;
        }
      } else {
        const struct weld_sub_data * sd =
          (const struct weld_sub_data *)e->data;
        if (sd->topic == NULL || strcmp(sd->topic, topic) != 0) {
          continue;
        }
        rc = rmw_qos_profile_check_compatible(
          *new_qos, sd->qos, &compat, NULL, 0);
        if (rc == RMW_RET_OK && compat != RMW_QOS_COMPATIBILITY_OK) {
          ((struct weld_sub_data *)e->data)->incompat_total++;
        }
      }
    }
    (void)pthread_mutex_unlock(&g_weld_ep_mutex);
  }
  return mismatched;
}

// Fill a names_and_types snapshot from local endpoints owned by one
// node plus namespace.
//
// Endpoints match on owning node name plus namespace (never the bare
// name) and on the querying domain when the node carries a context.
// Each topic or service name aggregates its IDL types: message type
// for publishers plus subscriptions, service base for servers plus
// clients. Unknown nodes report RMW_RET_NODE_NAME_NON_EXISTENT; known
// nodes with no local endpoints report an empty snapshot with OK.
static rmw_ret_t weld_by_node_fill(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace, int kind,
  rmw_names_and_types_t * out, const char * what)
{
  struct weld_ep_snap * snaps = NULL;
  size_t n_snaps = 0;
  struct weld_graph_nt merged;
  size_t domain = 0;
  bool use_domain = false;
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
  if (node->context != NULL) {
    domain = node->context->actual_domain_id;
    use_domain = true;
  }
  merged.items = NULL;
  merged.n = 0;
  merged.cap = 0;
  rc = weld_ep_snapshot(NULL, kind, domain, use_domain, &snaps, &n_snaps);
  if (rc != RMW_RET_OK) {
    return rc;
  }
  for (size_t i = 0; i < n_snaps; i++) {
    char typebuf[512];
    const char * typetext = "";
    if (strcmp(snaps[i].node_name, node_name) != 0 ||
      strcmp(snaps[i].node_ns, node_namespace) != 0)
    {
      continue;
    }
    if (kind == WELD_EP_PUB || kind == WELD_EP_SUB) {
      if (weld_type_name(
          weld_resolve_members(snaps[i].type_support), typebuf,
          sizeof(typebuf)) == 0)
      {
        typetext = typebuf;
      }
    } else {
      if (snaps[i].svc_type_support != NULL &&
        weld_service_idl_base(
          snaps[i].svc_type_support, typebuf, sizeof(typebuf)) == 0)
      {
        typetext = typebuf;
      }
    }
    if (weld_graph_nt_add(&merged, snaps[i].name, typetext) != 0) {
      weld_ep_snapshot_fini(snaps, n_snaps);
      weld_graph_nt_fini(&merged);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: by_node out of memory");
      return RMW_RET_BAD_ALLOC;
    }
  }
  weld_ep_snapshot_fini(snaps, n_snaps);
  rc = weld_graph_fill_names_and_types(
    &merged, allocator, out, what);
  weld_graph_nt_fini(&merged);
  return rc;
#else
  (void)node_name;
  (void)node_namespace;
  (void)kind;
  (void)what;
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
  return weld_by_node_fill(
    node, allocator, node_name, node_namespace, WELD_EP_PUB,
    topic_names_and_types,
    "rmw_weld: rmw_get_publisher_names_and_types_by_node unknown node");
}

rmw_ret_t rmw_get_subscriber_names_and_types_by_node(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace, bool no_demangle,
  rmw_names_and_types_t * topic_names_and_types)
{
  (void)no_demangle;
  return weld_by_node_fill(
    node, allocator, node_name, node_namespace, WELD_EP_SUB,
    topic_names_and_types,
    "rmw_weld: rmw_get_subscriber_names_and_types_by_node unknown node");
}

rmw_ret_t rmw_get_service_names_and_types_by_node(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace,
  rmw_names_and_types_t * service_names_and_types)
{
  return weld_by_node_fill(
    node, allocator, node_name, node_namespace, WELD_EP_SVC,
    service_names_and_types,
    "rmw_weld: rmw_get_service_names_and_types_by_node unknown node");
}

rmw_ret_t rmw_get_client_names_and_types_by_node(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace,
  rmw_names_and_types_t * service_names_and_types)
{
  return weld_by_node_fill(
    node, allocator, node_name, node_namespace, WELD_EP_CLI,
    service_names_and_types,
    "rmw_weld: rmw_get_client_names_and_types_by_node unknown node");
}

// Set one endpoint info entry from plain fields.
//
// Strings copy through the rmw setters with the caller allocator. Gid
// must hold RMW_GID_STORAGE_SIZE bytes. Returns the setter result.
// Remote roles are the WELD_FILTER_ROLE_* constants near the top.
static rmw_ret_t weld_info_set_one(
  rmw_topic_endpoint_info_t * info, const char * node_name,
  const char * node_ns, const char * type, const uint8_t * gid,
  const rmw_qos_profile_t * qos, rmw_endpoint_type_t ep_type,
  rcutils_allocator_t * allocator)
{
  rmw_ret_t rc = RMW_RET_OK;
  rc = rmw_topic_endpoint_info_set_node_name(info, node_name, allocator);
  if (rc != RMW_RET_OK) {
    return rc;
  }
  rc = rmw_topic_endpoint_info_set_node_namespace(info, node_ns, allocator);
  if (rc != RMW_RET_OK) {
    return rc;
  }
  rc = rmw_topic_endpoint_info_set_topic_type(info, type, allocator);
  if (rc != RMW_RET_OK) {
    return rc;
  }
  rc = rmw_topic_endpoint_info_set_endpoint_type(info, ep_type);
  if (rc != RMW_RET_OK) {
    return rc;
  }
  rc = rmw_topic_endpoint_info_set_gid(
    info, gid, (size_t)RMW_GID_STORAGE_SIZE);
  if (rc != RMW_RET_OK) {
    return rc;
  }
  return rmw_topic_endpoint_info_set_qos_profile(info, qos);
}

// Fill the 16 byte info GID from domain plus handle.
//
// The layout mirrors rmw_get_gid_for_publisher: domain little endian in
// the first 4 bytes and the FFI handle in the next 8, zero padded. It
// is a synthetic identity unique within one domain, not a DDS GUID:
// the FFI layer exposes local endpoint GUIDs nowhere, so local entries
// use this while remote entries carry the raw 16 byte SEDP GUID from
// weld_ffi_remote_ep_at.
static void weld_info_gid(uint8_t gid[16], size_t domain, long long handle)
{
  uint64_t handle_u = 0;
  uint32_t domain_u = 0;
  size_t i = 0;
  (void)memset(gid, 0, 16);
  if (domain > (size_t)UINT32_MAX) {
    return;
  }
  domain_u = (uint32_t)domain;
  handle_u = (uint64_t)handle;
  gid[0] = (uint8_t)(domain_u & 0xFFU);
  gid[1] = (uint8_t)((domain_u >> 8) & 0xFFU);
  gid[2] = (uint8_t)((domain_u >> 16) & 0xFFU);
  gid[3] = (uint8_t)((domain_u >> 24) & 0xFFU);
  for (i = 0; i < 8; i++) {
    gid[4 + i] = (uint8_t)((handle_u >> (8 * i)) & 0xFFU);
  }
}

// Fill one remote QoS profile from SEDP params with fallback.
//
// Starts from fallback (first local QoS or qos_default) for the policies
// the wire does not carry, then overrides reliability, durability,
// history, depth, deadline, liveliness, and lease from the FFI offer.
// Returns 0 with a real remote offer in out, 1 when the offer is
// genuinely unknown (caller uses fallback), and -1 when the entry must
// skip fail-closed (torn read or a policy with no RMW equivalent, such
// as transient or persistent durability). It never fabricates QoS.
static int weld_info_remote_qos(
  long long bridge, const char * name, int remote_role, long long index,
  const rmw_qos_profile_t * fallback, rmw_qos_profile_t * out)
{
  long long reliability = 0;
  long long durability = 0;
  long long history = 0;
  long long depth = 0;
  long long deadline_sec = 0;
  long long deadline_nsec = 0;
  long long liveliness = 0;
  long long lease_sec = 0;
  long long lease_nsec = 0;
  int rc = 0;
  if (name == NULL || fallback == NULL || out == NULL) {
    return -1;
  }
  *out = *fallback;
  if (weld_ffi_remote_ep_qos == NULL) {
    return 1;
  }
  rc = weld_ffi_remote_ep_qos(
    bridge, name, remote_role, index, &reliability, &durability,
    &history, &depth, &deadline_sec, &deadline_nsec, &liveliness,
    &lease_sec, &lease_nsec);
  if (rc != 0) {
    return rc == -1 ? 1 : -1;
  }
  if (reliability == 1) {
    out->reliability = RMW_QOS_POLICY_RELIABILITY_BEST_EFFORT;
  } else if (reliability == 2) {
    out->reliability = RMW_QOS_POLICY_RELIABILITY_RELIABLE;
  } else {
    return -1;
  }
  if (durability == 0) {
    out->durability = RMW_QOS_POLICY_DURABILITY_VOLATILE;
  } else if (durability == 1) {
    out->durability = RMW_QOS_POLICY_DURABILITY_TRANSIENT_LOCAL;
  } else {
    return -1;
  }
  if (history == 0) {
    out->history = RMW_QOS_POLICY_HISTORY_KEEP_LAST;
    if (depth <= 0 || depth > (long long)SIZE_MAX) {
      return -1;
    }
    out->depth = (size_t)depth;
  } else if (history == 1) {
    out->history = RMW_QOS_POLICY_HISTORY_KEEP_ALL;
    out->depth = 0;
  } else {
    return -1;
  }
  if (deadline_sec < 0 || deadline_nsec < 0) {
    return -1;
  }
  out->deadline.sec = (uint64_t)deadline_sec;
  out->deadline.nsec = (uint64_t)deadline_nsec;
  if (liveliness == 0) {
    out->liveliness = RMW_QOS_POLICY_LIVELINESS_AUTOMATIC;
  } else if (liveliness == 1) {
    out->liveliness = RMW_QOS_POLICY_LIVELINESS_MANUAL_BY_NODE;
  } else if (liveliness == 2) {
    out->liveliness = RMW_QOS_POLICY_LIVELINESS_MANUAL_BY_TOPIC;
  } else {
    return -1;
  }
  if (lease_sec < 0 || lease_nsec < 0) {
    return -1;
  }
  out->liveliness_lease_duration.sec = (uint64_t)lease_sec;
  out->liveliness_lease_duration.nsec = (uint64_t)lease_nsec;
  return 0;
}

// Fill an info array from local registry endpoints plus remote peers.
//
// Locals carry their stored actual QoS, type, and synthetic GID with real
// node names from the bridge registry. Remotes carry their SEDP GUID,
// announced type, and QoS decoded from SEDP params through
// weld_ffi_remote_ep_qos where the bridge can see them; the first local
// QoS on the same name (or qos_default when no local exists) fills only
// the policies the wire does not carry and backs entries whose offer is
// genuinely unknown. Remote node names stay empty with namespace "/"
// because SEDP carries no ROS node identity (USER_DATA holds only
// typehash, DDS topic, domain, serviceid, and clientid; SPDP carries
// only enclave=/ and Weld publishes no ros_discovery_info topic), so no
// peer node-name map exists; the empty fallback is fail-closed, never a
// fabricated name. Unknown names still return OK with an empty array per
// spec. Torn or malformed remote entries skip quietly instead of
// zero-filling.
static rmw_ret_t weld_info_fill(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * name, int local_kind, int remote_role,
  rmw_endpoint_type_t ep_type, rmw_topic_endpoint_info_array_t * out)
{
  struct weld_ep_snap * snaps = NULL;
  size_t n_local = 0;
  size_t n_remote = 0;
  size_t i = 0;
  size_t w = 0;
  long long bridge = 0;
  size_t domain = 0;
  bool use_domain = false;
  rmw_ret_t rc = RMW_RET_OK;
  if (node == NULL || allocator == NULL || name == NULL || out == NULL) {
    return weld_fail_invalid("rmw_weld: info_by_topic null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: info_by_topic wrong implementation");
  }
  if (name[0] == '\0') {
    return weld_fail_invalid("rmw_weld: info_by_topic empty topic");
  }
  if (rmw_topic_endpoint_info_array_check_zero(out) != RMW_RET_OK) {
    return weld_fail_invalid("rmw_weld: info_by_topic not zero");
  }
  if (node->context != NULL) {
    domain = node->context->actual_domain_id;
    use_domain = true;
  }
  rc = weld_ep_snapshot(
    name, local_kind, domain, use_domain, &snaps, &n_local);
  if (rc != RMW_RET_OK) {
    return rc;
  }
  {
    const struct weld_node_data * nd =
      (const struct weld_node_data *)node->data;
    if (nd != NULL) {
      bridge = nd->bridge;
    }
  }
  if (bridge != 0 && weld_ffi_remote_ep_count != NULL &&
    weld_ffi_remote_ep_at != NULL)
  {
    long long n = 0;
    if (weld_ffi_remote_ep_count(bridge, name, remote_role, &n) == 0 &&
      n > 0)
    {
      n_remote = (size_t)n;
    }
  }
  rc = rmw_topic_endpoint_info_array_init_with_size(
    out, n_local + n_remote, allocator);
  if (rc != RMW_RET_OK) {
    weld_ep_snapshot_fini(snaps, n_local);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: info_by_topic init failed");
    return RMW_RET_BAD_ALLOC;
  }
  for (i = 0; i < n_local; i++) {
    rmw_topic_endpoint_info_t * info = &out->info_array[w];
    char typebuf[512];
    const char * typetext = "";
    uint8_t gid[16];
    if (local_kind == WELD_EP_PUB || local_kind == WELD_EP_SUB) {
      if (weld_type_name(
          weld_resolve_members(snaps[i].type_support), typebuf,
          sizeof(typebuf)) == 0)
      {
        typetext = typebuf;
      }
    } else {
      if (snaps[i].svc_type_support != NULL &&
        weld_service_idl_base(
          snaps[i].svc_type_support, typebuf, sizeof(typebuf)) == 0)
      {
        typetext = typebuf;
      }
    }
    weld_info_gid(gid, snaps[i].domain, snaps[i].handle);
    rc = weld_info_set_one(
      info, snaps[i].node_name, snaps[i].node_ns, typetext, gid,
      &snaps[i].qos, ep_type, allocator);
    if (rc != RMW_RET_OK) {
      goto fail;
    }
    w++;
  }
  for (i = 0; i < n_remote; i++) {
    rmw_topic_endpoint_info_t * info = NULL;
    char typebuf[1024];
    uint8_t gid[16];
    rmw_qos_profile_t rqos;
    const rmw_qos_profile_t * qos = &rmw_qos_profile_default;
    const rmw_qos_profile_t * use_qos = NULL;
    int frc = 0;
    int qrc = 0;
    if (n_local > 0) {
      qos = &snaps[0].qos;
    }
    frc = weld_ffi_remote_ep_at(
      bridge, name, remote_role, (long long)i, typebuf,
      (int)sizeof(typebuf), (char *)gid, (int)sizeof(gid));
    if (frc != 0) {
      continue;
    }
    // Decode the SEDP offer where the bridge can see it; fall back to
    // the first local QoS only when genuinely unknown, and skip torn or
    // inexpressible offers fail-closed instead of zero-filling.
    qrc = weld_info_remote_qos(
      bridge, name, remote_role, (long long)i, qos, &rqos);
    if (qrc < 0) {
      continue;
    }
    if (qrc == 0) {
      use_qos = &rqos;
    } else {
      use_qos = qos;
    }
    info = &out->info_array[w];
    rc = weld_info_set_one(
      info, "", "/", typebuf, gid, use_qos, ep_type, allocator);
    if (rc != RMW_RET_OK) {
      goto fail;
    }
    w++;
  }
  out->size = w;
  weld_ep_snapshot_fini(snaps, n_local);
  return RMW_RET_OK;
fail:
  {
    rmw_ret_t fini_rc = rmw_topic_endpoint_info_array_fini(out, allocator);
    (void)fini_rc;
  }
  weld_ep_snapshot_fini(snaps, n_local);
  if (rc == RMW_RET_BAD_ALLOC) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: info_by_topic alloc failed");
    return RMW_RET_BAD_ALLOC;
  }
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: info_by_topic fill failed");
  return RMW_RET_ERROR;
}

rmw_ret_t rmw_get_publishers_info_by_topic(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * topic_name, bool no_mangle,
  rmw_topic_endpoint_info_array_t * publishers_info)
{
  (void)no_mangle;
  return weld_info_fill(
    node, allocator, topic_name, WELD_EP_PUB, WELD_FILTER_ROLE_WRITER,
    RMW_ENDPOINT_PUBLISHER, publishers_info);
}

rmw_ret_t rmw_get_subscriptions_info_by_topic(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * topic_name, bool no_mangle,
  rmw_topic_endpoint_info_array_t * subscriptions_info)
{
  (void)no_mangle;
  return weld_info_fill(
    node, allocator, topic_name, WELD_EP_SUB, WELD_FILTER_ROLE_READER,
    RMW_ENDPOINT_SUBSCRIPTION, subscriptions_info);
}

// Weld forward-compat service endpoint queries.
//
// Jazzy RMW has no rmw_get_servers_info_by_service or
// rmw_get_clients_info_by_service, so these symbols are weld extensions
// with the topic info array shape. Servers publish replies, so they
// report RMW_ENDPOINT_PUBLISHER; clients subscribe to replies, so they
// report RMW_ENDPOINT_SUBSCRIPTION. Local service entries carry the
// IDL service base when introspection is stored, else an empty type.
rmw_ret_t rmw_get_servers_info_by_service(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * service_name, rmw_topic_endpoint_info_array_t * servers_info)
{
  return weld_info_fill(
    node, allocator, service_name, WELD_EP_SVC, WELD_FILTER_ROLE_SERVER,
    RMW_ENDPOINT_PUBLISHER, servers_info);
}

rmw_ret_t rmw_get_clients_info_by_service(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * service_name, rmw_topic_endpoint_info_array_t * clients_info)
{
  return weld_info_fill(
    node, allocator, service_name, WELD_EP_CLI, WELD_FILTER_ROLE_CLIENT,
    RMW_ENDPOINT_SUBSCRIPTION, clients_info);
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

// Loan family: single-owner in-process zero copy with a copying wire
// fallback. The wire path copies (CDR encode plus channel copy plus CDR
// decode), which serialization inherently requires; the in-process path
// shares the loaned buffer by pointer with no copy. See the loan infra
// above for the borrow, publish, take, and return bookkeeping.

rmw_ret_t rmw_borrow_loaned_message(
  const rmw_publisher_t * publisher,
  const rosidl_message_type_support_t * type_support, void ** ros_message)
{
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  void * buf = NULL;
  if (publisher == NULL || type_support == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_borrow_loaned_message null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_borrow_loaned_message wrong impl");
  }
  if (*ros_message != NULL) {
    return weld_fail_invalid("rmw_weld: rmw_borrow_loaned_message not null");
  }
  members = weld_resolve_members(type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_borrow_loaned_message type support is not introspection");
    return RMW_RET_ERROR;
  }
  if (members->init_function == NULL || members->fini_function == NULL ||
    members->size_of_ == 0)
  {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_borrow_loaned_message type support has no init");
    return RMW_RET_ERROR;
  }
  buf = calloc(1, members->size_of_);
  if (buf == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_borrow_loaned_message no memory");
    return RMW_RET_BAD_ALLOC;
  }
  members->init_function(buf, ROSIDL_RUNTIME_C_MSG_INIT_ALL);
  if (weld_loan_add(&g_weld_pub_loans, publisher, buf, members) != 0) {
    members->fini_function(buf);
    free(buf);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_borrow_loaned_message no memory");
    return RMW_RET_BAD_ALLOC;
  }
  *ros_message = buf;
  return RMW_RET_OK;
}

rmw_ret_t rmw_publish_loaned_message(
  const rmw_publisher_t * publisher, void * ros_message,
  rmw_publisher_allocation_t * allocation)
{
  // The loan is unlinked first so a concurrent publish or return of the
  // same buffer fails loudly instead of encoding or freeing twice. On
  // failure the record is restored so the caller still owns the loan.
  // The wire bytes publish first for every subscriber (copying); the
  // loaned buffer is then queued by pointer for one zero-copy take, and
  // freed here when the queue is unavailable so delivery still succeeds
  // via the wire copy without leaking.
  const struct weld_pub_data * pd = NULL;
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  weld_cdr_status_t st;
  int rc = 0;
  (void)allocation;
  if (publisher == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publish_loaned_message null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publish_loaned_message wrong impl");
  }
  pd = (const struct weld_pub_data *)publisher->data;
  if (pd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publish_loaned_message no data");
    return RMW_RET_INVALID_ARGUMENT;
  }
  if (weld_loan_unlink(&g_weld_pub_loans, publisher, ros_message, &members) != 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_publish_loaned_message unknown loaned message");
    return RMW_RET_INVALID_ARGUMENT;
  }
  errbuf[0] = '\0';
  st = weld_cdr_encode_members(
    members, ros_message, true, &wire, &wire_len, errbuf, sizeof(errbuf));
  if (st != WELD_CDR_OK) {
    weld_loan_restore_pub(publisher, ros_message, members);
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_publish_loaned_message encode failed");
    }
    free(wire);
    return RMW_RET_ERROR;
  }
  if (wire_len > (size_t)INT_MAX) {
    free(wire);
    weld_loan_restore_pub(publisher, ros_message, members);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_publish_loaned_message payload too large");
    return RMW_RET_ERROR;
  }
  rc = weld_ffi_pub_publish(
    pd->handle, (const char *)wire, (int)wire_len);
  free(wire);
  if (rc != 0) {
    weld_loan_restore_pub(publisher, ros_message, members);
    return weld_fail_ffi(
      "rmw_weld: rmw_publish_loaned_message backend failed", rc);
  }
  if (weld_ffi_loan_publish != NULL) {
    int lrc = weld_ffi_loan_publish(
      pd->handle, (unsigned long long)(uintptr_t)ros_message,
      (unsigned long long)(uintptr_t)members);
    if (lrc == 0) {
      return RMW_RET_OK;
    }
  }
  members->fini_function(ros_message);
  free(ros_message);
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_loaned_message(
  const rmw_subscription_t * subscription, void ** loaned_message,
  bool * taken, rmw_subscription_allocation_t * allocation)
{
  void * buf = NULL;
  int empty = 0;
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
  empty = weld_take_loaned_wire(subscription, &buf, NULL, NULL);
  if (empty == 1) {
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  *loaned_message = buf;
  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_loaned_message_with_info(
  const rmw_subscription_t * subscription, void ** loaned_message,
  bool * taken, rmw_message_info_t * message_info,
  rmw_subscription_allocation_t * allocation)
{
  void * buf = NULL;
  unsigned long long seq = 0;
  uint8_t gid[16];
  int empty = 0;
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
  (void)memset(gid, 0, sizeof(gid));
  empty = weld_take_loaned_wire(subscription, &buf, &seq, gid);
  if (empty == 1) {
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  weld_fill_take_info(message_info, seq, gid);
  *loaned_message = buf;
  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_return_loaned_message_from_publisher(
  const rmw_publisher_t * publisher, void * loaned_message)
{
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  if (publisher == NULL || loaned_message == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_return_loaned_message_from_publisher null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl(
      "rmw_weld: rmw_return_loaned_message_from_publisher wrong impl");
  }
  if (weld_loan_unlink(&g_weld_pub_loans, publisher, loaned_message, &members) != 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_return_loaned_message_from_publisher unknown loan");
    return RMW_RET_INVALID_ARGUMENT;
  }
  members->fini_function(loaned_message);
  free(loaned_message);
  return RMW_RET_OK;
}

rmw_ret_t rmw_return_loaned_message_from_subscription(
  const rmw_subscription_t * subscription, void * loaned_message)
{
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  if (subscription == NULL || loaned_message == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_return_loaned_message_from_subscription null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_return_loaned_message_from_subscription wrong impl");
  }
  if (weld_loan_unlink(&g_weld_sub_loans, subscription, loaned_message, &members) != 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_return_loaned_message_from_subscription unknown loan");
    return RMW_RET_INVALID_ARGUMENT;
  }
  members->fini_function(loaned_message);
  free(loaned_message);
  return RMW_RET_OK;
}

// On-new callbacks: stored on the endpoint plus dispatched. A per
// endpoint background thread polls a non-consuming readiness probe
// and invokes the stored callback once per arrival edge, so custom
// loops learn about data without polling wait. Takes still deliver
// every sample; the probes never consume. Clearing with NULL stops
// firing but keeps the thread parked until destroy. Start failures
// keep the stored callback without dispatch.

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
  (void)pthread_mutex_lock(&sd->dispatch_mutex);
  sd->on_new_callback = callback;
  sd->on_new_user_data = user_data;
  (void)pthread_mutex_unlock(&sd->dispatch_mutex);
  if (callback != NULL) {
    weld_sub_dispatch_start(sd);
  }
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
  if (callback != NULL) {
    weld_rpc_dispatch_start(st);
  }
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
  if (callback != NULL) {
    weld_rpc_dispatch_start(st);
  }
  return RMW_RET_OK;
}

// Events: every RMW event type is backed by a live source.
//
// MESSAGE_LOST counts decode drops in the take paths (see
// g_weld_decode_drops). LIVELINESS_CHANGED reports matched-publisher
// edges for subscriptions and LIVELINESS_LOST reports matched-subscriber
// drops for publishers, both via the existing matched FFI which sums
// local same-bridge endpoints with remote SEDP peers. Deadline misses
// accumulate in the Go core from the stored QoS deadline plus take and
// publish timing. Incompatible QoS totals sum local profile mismatches
// counted at endpoint creation with wire mismatches from
// CheckWireCompatibility in the Go core; the last policy kind is
// unknown so it reports RMW_QOS_POLICY_INVALID fail-closed. rmw_wait
// fires on every type while untaken status exists and rejects unknown
// event values up front. Callbacks stored by rmw_event_set_callback
// fire on rmw_take_event when new status exists.
//
// Remote node names stay empty with namespace "/" because SEDP carries
// no ROS node identity: USER_DATA holds only typehash, DDS topic,
// domain, serviceid, and clientid, and SPDP carries only enclave=/.
// Weld publishes no ros_discovery_info topic, so no peer node-name map
// exists to consult; the empty fallback is fail-closed, never a
// fabricated name.

// Clamp an unsigned total into int32_t status range.
//
// Totals grow monotonically but status fields are int32_t, so values
// past INT32_MAX saturate instead of wrapping. Returns the clamped
// value.
static int32_t weld_event_clamp32(unsigned long long v)
{
  if (v > (unsigned long long)INT32_MAX) {
    return INT32_MAX;
  }
  return (int32_t)v;
}

// Snapshot the live source into a fresh slot at init without consuming.
//
// The slot binding (endpoint handle plus pointer plus side) is already
// stored; this fills the take-owned baselines so the first take reports
// only changes since init. Missing FFI leaves zero baselines so take
// reports not ready until the source appears.
static void weld_event_snapshot_locked(struct weld_event_slot * slot)
{
  long long matched = 0;
  unsigned long long total = 0;
  unsigned long long wire = 0;
  size_t local = 0;
  if (slot == NULL) {
    return;
  }
  switch (slot->event_type) {
    case RMW_EVENT_MESSAGE_LOST:
      slot->lost_reported = g_weld_decode_drops;
      break;
    case RMW_EVENT_LIVELINESS_CHANGED:
      if (weld_event_matched_current(slot, &matched) == 0) {
        slot->alive_reported = matched;
        slot->not_alive_reported = 0;
      }
      break;
    case RMW_EVENT_LIVELINESS_LOST:
      if (weld_event_matched_current(slot, &matched) == 0) {
        slot->last_matched = matched;
        slot->lost_total = 0;
        slot->lost_reported = 0;
      }
      break;
    case RMW_EVENT_REQUESTED_DEADLINE_MISSED:
    case RMW_EVENT_OFFERED_DEADLINE_MISSED:
      if (weld_event_deadline_current(slot, &total) == 0) {
        slot->deadline_reported = total;
      }
      break;
    case RMW_EVENT_REQUESTED_QOS_INCOMPATIBLE:
    case RMW_EVENT_OFFERED_QOS_INCOMPATIBLE:
      if (weld_event_wire_incompat_current(slot, &wire) == 0) {
        slot->wire_incompat_reported = wire;
      }
      if (weld_event_local_incompat_current(slot, &local) == 0) {
        slot->local_incompat_reported = local;
      }
      break;
    default:
      break;
  }
}

// Bind one event handle to its endpoint at init.
//
// Claims the slot, stores the type plus endpoint association, and
// snapshots baselines. The caller holds no locks. Returns 0 on success
// and -1 when the slot table is full.
static int weld_event_bind(
  const rmw_event_t * handle, rmw_event_type_t event_type,
  long long endpoint_handle, const void * endpoint, bool is_pub)
{
  int ok = -1;
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  {
    struct weld_event_slot * slot = weld_event_slot_locked(handle);
    if (slot != NULL) {
      slot->event_type = event_type;
      slot->endpoint_handle = endpoint_handle;
      slot->endpoint = endpoint;
      slot->is_pub = is_pub;
      weld_event_snapshot_locked(slot);
      ok = 0;
    }
  }
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return ok;
}

// Report whether a publisher event type belongs on publishers.
//
// Returns true for the publisher side types, false otherwise.
static bool weld_event_is_pub_type(rmw_event_type_t event_type)
{
  return event_type == RMW_EVENT_MESSAGE_LOST ||
    event_type == RMW_EVENT_LIVELINESS_LOST ||
    event_type == RMW_EVENT_OFFERED_DEADLINE_MISSED ||
    event_type == RMW_EVENT_OFFERED_QOS_INCOMPATIBLE;
}

// Report whether a subscription event type belongs on subscriptions.
//
// Returns true for the subscription side types, false otherwise.
static bool weld_event_is_sub_type(rmw_event_type_t event_type)
{
  return event_type == RMW_EVENT_MESSAGE_LOST ||
    event_type == RMW_EVENT_LIVELINESS_CHANGED ||
    event_type == RMW_EVENT_REQUESTED_DEADLINE_MISSED ||
    event_type == RMW_EVENT_REQUESTED_QOS_INCOMPATIBLE;
}

rmw_ret_t rmw_take_event(
  const rmw_event_t * event_handle, void * event_info, bool * taken)
{
  rmw_event_callback_t cb = NULL;
  const void * user_data = NULL;
  bool fire = false;
  if (event_handle == NULL || event_info == NULL || taken == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_event null argument");
  }
  if (event_handle->implementation_identifier != kImplementationIdentifier) {
    return weld_fail_impl("rmw_weld: rmw_take_event wrong implementation");
  }
  *taken = false;
  switch (event_handle->event_type) {
    case RMW_EVENT_MESSAGE_LOST: {
#ifdef WELD_HAVE_MESSAGE_LOST
      rmw_message_lost_status_t * status = (rmw_message_lost_status_t *)event_info;
      struct weld_event_slot * slot = NULL;
      size_t total = 0;
      size_t last = 0;
      size_t change = 0;
      (void)pthread_mutex_lock(&g_weld_loan_mutex);
      total = g_weld_decode_drops;
      slot = weld_event_slot_locked(event_handle);
      if (slot != NULL) {
        last = slot->lost_reported;
        slot->lost_reported = total;
        cb = slot->callback;
        user_data = slot->user_data;
      }
      (void)pthread_mutex_unlock(&g_weld_loan_mutex);
      change = total - last;
      status->total_count = total;
      status->total_count_change = change;
      *taken = change > 0;
      fire = *taken && cb != NULL;
      if (fire) {
        cb(user_data, 1);
      }
      return RMW_RET_OK;
#else
      *taken = false;
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_event message lost unavailable");
      return RMW_RET_UNSUPPORTED;
#endif
    }
    case RMW_EVENT_LIVELINESS_CHANGED: {
#ifdef WELD_HAVE_LIVELINESS_CHANGED
      rmw_liveliness_changed_status_t * status =
        (rmw_liveliness_changed_status_t *)event_info;
      long long matched = 0;
      long long last = 0;
      int rc = -1;
      (void)pthread_mutex_lock(&g_weld_loan_mutex);
      {
        struct weld_event_slot * slot = NULL;
        for (size_t i = 0; i < (size_t)WELD_EVENT_SLOTS_MAX; i++) {
          if (g_weld_event_slots[i].used &&
            g_weld_event_slots[i].handle == event_handle)
          {
            slot = &g_weld_event_slots[i];
            break;
          }
        }
        if (slot != NULL && !slot->is_pub) {
          rc = weld_event_matched_current(slot, &matched);
          if (rc == 0) {
            last = slot->alive_reported;
            slot->alive_reported = matched;
            cb = slot->callback;
            user_data = slot->user_data;
          }
        }
      }
      (void)pthread_mutex_unlock(&g_weld_loan_mutex);
      if (rc != 0) {
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG(
          "rmw_weld: rmw_take_event liveliness changed no source");
        return RMW_RET_UNSUPPORTED;
      }
      status->alive_count = weld_event_clamp32(
        matched < 0 ? 0 : (unsigned long long)matched);
      status->not_alive_count = 0;
      status->alive_count_change = weld_event_clamp32(
        matched >= last ? (unsigned long long)(matched - last) : 0);
      if (matched < last) {
        long long drop = last - matched;
        status->alive_count_change = (int32_t)-weld_event_clamp32(
          (unsigned long long)drop);
      }
      status->not_alive_count_change = 0;
      *taken = matched != last;
      fire = *taken && cb != NULL;
      if (fire) {
        cb(user_data, 1);
      }
      return RMW_RET_OK;
#else
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_take_event liveliness changed unavailable");
      return RMW_RET_UNSUPPORTED;
#endif
    }
    case RMW_EVENT_LIVELINESS_LOST: {
#ifdef WELD_HAVE_LIVELINESS_LOST
      rmw_liveliness_lost_status_t * status =
        (rmw_liveliness_lost_status_t *)event_info;
      long long matched = 0;
      unsigned long long last_total = 0;
      unsigned long long cur = 0;
      int rc = -1;
      (void)pthread_mutex_lock(&g_weld_loan_mutex);
      {
        struct weld_event_slot * slot = NULL;
        for (size_t i = 0; i < (size_t)WELD_EVENT_SLOTS_MAX; i++) {
          if (g_weld_event_slots[i].used &&
            g_weld_event_slots[i].handle == event_handle)
          {
            slot = &g_weld_event_slots[i];
            break;
          }
        }
        if (slot != NULL && slot->is_pub) {
          rc = weld_event_matched_current(slot, &matched);
          if (rc == 0) {
            if (matched < slot->last_matched) {
              slot->lost_total +=
                (unsigned long long)(slot->last_matched - matched);
            }
            slot->last_matched = matched;
            last_total = (unsigned long long)slot->lost_reported;
            cur = slot->lost_total;
            slot->lost_reported = (size_t)cur;
            cb = slot->callback;
            user_data = slot->user_data;
          }
        }
      }
      (void)pthread_mutex_unlock(&g_weld_loan_mutex);
      if (rc != 0) {
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG(
          "rmw_weld: rmw_take_event liveliness lost no source");
        return RMW_RET_UNSUPPORTED;
      }
      status->total_count = weld_event_clamp32(cur);
      status->total_count_change = weld_event_clamp32(
        cur >= last_total ? cur - last_total : 0);
      *taken = cur != last_total;
      fire = *taken && cb != NULL;
      if (fire) {
        cb(user_data, 1);
      }
      return RMW_RET_OK;
#else
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_take_event liveliness lost unavailable");
      return RMW_RET_UNSUPPORTED;
#endif
    }
    case RMW_EVENT_OFFERED_DEADLINE_MISSED:
    case RMW_EVENT_REQUESTED_DEADLINE_MISSED: {
#if defined(WELD_HAVE_OFFERED_DEADLINE) || defined(WELD_HAVE_REQUESTED_DEADLINE)
      bool offered = event_handle->event_type ==
        RMW_EVENT_OFFERED_DEADLINE_MISSED;
      unsigned long long total = 0;
      unsigned long long last = 0;
      int rc = -1;
      (void)pthread_mutex_lock(&g_weld_loan_mutex);
      {
        struct weld_event_slot * slot = NULL;
        for (size_t i = 0; i < (size_t)WELD_EVENT_SLOTS_MAX; i++) {
          if (g_weld_event_slots[i].used &&
            g_weld_event_slots[i].handle == event_handle)
          {
            slot = &g_weld_event_slots[i];
            break;
          }
        }
        if (slot != NULL && slot->is_pub == offered) {
          rc = weld_event_deadline_current(slot, &total);
          if (rc == 0) {
            last = slot->deadline_reported;
            slot->deadline_reported = total;
            cb = slot->callback;
            user_data = slot->user_data;
          }
        }
      }
      (void)pthread_mutex_unlock(&g_weld_loan_mutex);
      if (rc != 0) {
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_event deadline no source");
        return RMW_RET_UNSUPPORTED;
      }
      if (offered) {
#ifdef WELD_HAVE_OFFERED_DEADLINE
        rmw_offered_deadline_missed_status_t * status =
          (rmw_offered_deadline_missed_status_t *)event_info;
        status->total_count = weld_event_clamp32(total);
        status->total_count_change = weld_event_clamp32(
          total >= last ? total - last : 0);
#else
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG(
          "rmw_weld: rmw_take_event offered deadline unavailable");
        return RMW_RET_UNSUPPORTED;
#endif
      } else {
#ifdef WELD_HAVE_REQUESTED_DEADLINE
        rmw_requested_deadline_missed_status_t * status =
          (rmw_requested_deadline_missed_status_t *)event_info;
        status->total_count = weld_event_clamp32(total);
        status->total_count_change = weld_event_clamp32(
          total >= last ? total - last : 0);
#else
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG(
          "rmw_weld: rmw_take_event requested deadline unavailable");
        return RMW_RET_UNSUPPORTED;
#endif
      }
      *taken = total != last;
      fire = *taken && cb != NULL;
      if (fire) {
        cb(user_data, 1);
      }
      return RMW_RET_OK;
#else
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_event deadline unavailable");
      return RMW_RET_UNSUPPORTED;
#endif
    }
    case RMW_EVENT_OFFERED_QOS_INCOMPATIBLE:
    case RMW_EVENT_REQUESTED_QOS_INCOMPATIBLE: {
#ifdef WELD_HAVE_INCOMPAT_QOS
      bool offered = event_handle->event_type ==
        RMW_EVENT_OFFERED_QOS_INCOMPATIBLE;
      rmw_qos_incompatible_event_status_t * status =
        (rmw_qos_incompatible_event_status_t *)event_info;
      unsigned long long wire = 0;
      unsigned long long wire_last = 0;
      size_t local = 0;
      size_t local_last = 0;
      unsigned long long total = 0;
      unsigned long long last = 0;
      int rc = -1;
      (void)pthread_mutex_lock(&g_weld_loan_mutex);
      {
        struct weld_event_slot * slot = NULL;
        for (size_t i = 0; i < (size_t)WELD_EVENT_SLOTS_MAX; i++) {
          if (g_weld_event_slots[i].used &&
            g_weld_event_slots[i].handle == event_handle)
          {
            slot = &g_weld_event_slots[i];
            break;
          }
        }
        if (slot != NULL && slot->is_pub == offered) {
          int wrc = weld_event_wire_incompat_current(slot, &wire);
          int lrc = weld_event_local_incompat_current(slot, &local);
          if (wrc == 0 && lrc == 0) {
            wire_last = slot->wire_incompat_reported;
            local_last = slot->local_incompat_reported;
            slot->wire_incompat_reported = wire;
            slot->local_incompat_reported = local;
            cb = slot->callback;
            user_data = slot->user_data;
            rc = 0;
          }
        }
      }
      (void)pthread_mutex_unlock(&g_weld_loan_mutex);
      if (rc != 0) {
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_event incompat no source");
        return RMW_RET_UNSUPPORTED;
      }
      total = wire + (unsigned long long)local;
      last = wire_last + (unsigned long long)local_last;
      status->total_count = weld_event_clamp32(total);
      status->total_count_change = weld_event_clamp32(
        total >= last ? total - last : 0);
      // The policy that refused last is not tracked through the FFI
      // totals, so it reports invalid fail-closed while counts stay
      // real.
      status->last_policy_kind = RMW_QOS_POLICY_INVALID;
      *taken = total != last;
      fire = *taken && cb != NULL;
      if (fire) {
        cb(user_data, 1);
      }
      return RMW_RET_OK;
#else
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_event incompat unavailable");
      return RMW_RET_UNSUPPORTED;
#endif
    }
    default: {
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_event event type not supported");
      return RMW_RET_UNSUPPORTED;
    }
  }
}

rmw_ret_t rmw_event_set_callback(
  rmw_event_t * event, rmw_event_callback_t callback, const void * user_data)
{
  struct weld_event_slot * slot = NULL;
  if (event == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_event_set_callback null argument");
  }
  if (event->implementation_identifier != kImplementationIdentifier) {
    return weld_fail_impl("rmw_weld: rmw_event_set_callback wrong impl");
  }
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  slot = weld_event_slot_locked(event);
  if (slot == NULL) {
    (void)pthread_mutex_unlock(&g_weld_loan_mutex);
    return weld_fail_error("rmw_weld: rmw_event_set_callback too many events");
  }
  slot->callback = callback;
  slot->user_data = user_data;
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return RMW_RET_OK;
}

bool rmw_event_type_is_supported(rmw_event_type_t rmw_event_type)
{
  switch (rmw_event_type) {
    case RMW_EVENT_MESSAGE_LOST:
    case RMW_EVENT_LIVELINESS_CHANGED:
    case RMW_EVENT_LIVELINESS_LOST:
    case RMW_EVENT_REQUESTED_DEADLINE_MISSED:
    case RMW_EVENT_OFFERED_DEADLINE_MISSED:
    case RMW_EVENT_REQUESTED_QOS_INCOMPATIBLE:
    case RMW_EVENT_OFFERED_QOS_INCOMPATIBLE:
      return true;
    default:
      return false;
  }
}

// Dynamic message take: the wire sample is taken the same way as the
// static path (wait stash first via weld_take_wire) and populated into
// the caller provided dynamic data through
// rosidl_dynamic_typesupport_dynamic_data_deserialize. The caller binds
// the dynamic data to its type plus serialization support; this shim
// only moves the taken CDR bytes (including the 4 byte encapsulation
// header, which matches what the serialization libraries expect) into
// it. The deserialize entry is resolved at runtime (see
// weld_dyn_lookup) so no link time dependency is needed. Humble predates
// the dynamic typesupport API entirely, so these entry points exist only
// where rmw/dynamic_message_type_support.h does.
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")

typedef rcutils_ret_t (* weld_dyn_ss_impl_fn)(
  rcutils_allocator_t *,
  rosidl_dynamic_typesupport_serialization_support_impl_t *);

typedef rcutils_ret_t (* weld_dyn_ss_iface_fn)(
  rcutils_allocator_t *,
  rosidl_dynamic_typesupport_serialization_support_interface_t *);

typedef rcutils_ret_t (* weld_dyn_ss_init_fn)(
  rosidl_dynamic_typesupport_serialization_support_impl_t *,
  rosidl_dynamic_typesupport_serialization_support_interface_t *,
  rcutils_allocator_t *,
  rosidl_dynamic_typesupport_serialization_support_t *);

// Resolve one dynamic serialization support symbol without a link time
// dependency. Returns NULL when the library or symbol is absent.
static void * weld_dyn_sym(const char * soname, const char * symname)
{
  void * sym = NULL;
#ifdef WELD_HAVE_DLOPEN
  void * lib = NULL;
  if (soname == NULL || symname == NULL) {
    return NULL;
  }
  lib = dlopen(soname, RTLD_LAZY | RTLD_NOLOAD);
  if (lib == NULL) {
    lib = dlopen(soname, RTLD_LAZY);
  }
  if (lib != NULL) {
    sym = dlsym(lib, symname);
  }
#else
  (void)soname;
  (void)symname;
#endif
  return sym;
}

rmw_ret_t rmw_serialization_support_init(
  const char * serialization_lib_name,
  rcutils_allocator_t * allocator,
  rosidl_dynamic_typesupport_serialization_support_t * serialization_support)
{
  // Initialize the polymorphic serialization support for a named
  // dynamic typesupport library (such as
  // rosidl_dynamic_typesupport_fastrtps). The library soname plus the
  // impl and interface init symbols derive from the name, and the core
  // init entry comes from librosidl_dynamic_typesupport, all resolved
  // by dlopen so the probe link line carries no extra library. Only
  // plain leaf names are accepted so the derived soname and symbol
  // strings cannot traverse paths. The interface init runs first
  // because it only fills the function-pointer table and allocates
  // nothing; the impl init allocates the library handle and runs
  // second, immediately before core assembly, so an impl success is
  // never abandoned by a later interface failure. On any failure the
  // output is untouched and the error is loud.
  char soname[512];
  char impl_sym[512];
  char iface_sym[512];
  size_t name_len = 0;
  int n = 0;
  void * impl_addr = NULL;
  void * iface_addr = NULL;
  void * core_addr = NULL;
  weld_dyn_ss_impl_fn impl_fn = NULL;
  weld_dyn_ss_iface_fn iface_fn = NULL;
  weld_dyn_ss_init_fn core_fn = NULL;
  rosidl_dynamic_typesupport_serialization_support_impl_t impl;
  rosidl_dynamic_typesupport_serialization_support_interface_t methods;
  rcutils_ret_t rc = RCUTILS_RET_OK;
  if (serialization_lib_name == NULL || serialization_lib_name[0] == '\0') {
    return weld_fail_invalid(
      "rmw_weld: rmw_serialization_support_init null library name");
  }
  if (allocator == NULL || !rcutils_allocator_is_valid(allocator)) {
    return weld_fail_invalid(
      "rmw_weld: rmw_serialization_support_init bad allocator");
  }
  if (serialization_support == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_serialization_support_init null output");
  }
  name_len = strlen(serialization_lib_name);
  if (name_len > 200) {
    return weld_fail_invalid(
      "rmw_weld: rmw_serialization_support_init library name too long");
  }
  // Only letters, digits, and underscores are accepted: slashes, dots,
  // and separators would turn the derived soname and symbol strings
  // into a path traversal past the intended library.
  for (size_t i = 0; i < name_len; i++) {
    char c = serialization_lib_name[i];
    bool plain = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9') || c == '_';
    if (!plain) {
      return weld_fail_invalid(
        "rmw_weld: rmw_serialization_support_init bad library name");
    }
  }
  n = snprintf(soname, sizeof(soname), "lib%s.so", serialization_lib_name);
  if (n < 0 || (size_t)n >= sizeof(soname)) {
    return weld_fail_error(
      "rmw_weld: rmw_serialization_support_init soname overflow");
  }
  n = snprintf(
    impl_sym, sizeof(impl_sym), "%s_init_serialization_support_impl",
    serialization_lib_name);
  if (n < 0 || (size_t)n >= sizeof(impl_sym)) {
    return weld_fail_error(
      "rmw_weld: rmw_serialization_support_init impl symbol overflow");
  }
  n = snprintf(
    iface_sym, sizeof(iface_sym),
    "%s_init_serialization_support_interface", serialization_lib_name);
  if (n < 0 || (size_t)n >= sizeof(iface_sym)) {
    return weld_fail_error(
      "rmw_weld: rmw_serialization_support_init iface symbol overflow");
  }
  impl_addr = weld_dyn_sym(soname, impl_sym);
  iface_addr = weld_dyn_sym(soname, iface_sym);
  core_addr = weld_dyn_sym(
    "librosidl_dynamic_typesupport.so",
    "rosidl_dynamic_typesupport_serialization_support_init");
  if (impl_addr == NULL || iface_addr == NULL || core_addr == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_serialization_support_init runtime missing symbols");
    return RMW_RET_ERROR;
  }
  (void)memcpy(&impl_fn, &impl_addr, sizeof(impl_fn));
  (void)memcpy(&iface_fn, &iface_addr, sizeof(iface_fn));
  (void)memcpy(&core_fn, &core_addr, sizeof(core_fn));
  (void)memset(&impl, 0, sizeof(impl));
  (void)memset(&methods, 0, sizeof(methods));
  // Interface first: it only fills the function-pointer table, so a
  // failure leaves no partial allocation behind.
  rc = iface_fn(allocator, &methods);
  if (rc != RCUTILS_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_serialization_support_init iface init failed");
    return RMW_RET_ERROR;
  }
  rc = impl_fn(allocator, &impl);
  if (rc != RCUTILS_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_serialization_support_init impl init failed");
    return RMW_RET_ERROR;
  }
  rc = core_fn(&impl, &methods, allocator, serialization_support);
  if (rc != RCUTILS_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_serialization_support_init core init failed");
    return RMW_RET_ERROR;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_dynamic_message(
  const rmw_subscription_t * subscription,
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message, bool * taken,
  rmw_subscription_allocation_t * allocation)
{
  struct weld_sub_data * sd = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  rcutils_uint8_array_t buf;
  weld_dyn_deserialize_fn dyn_fn = NULL;
  rcutils_ret_t dyn_rc = RCUTILS_RET_OK;
  int empty = 0;
  (void)allocation;
  if (subscription == NULL || dynamic_message == NULL || taken == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_dynamic_message null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_take_dynamic_message wrong impl");
  }
  *taken = false;
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_dynamic_message no data");
    return RMW_RET_INVALID_ARGUMENT;
  }
  dyn_fn = weld_dyn_lookup();
  if (dyn_fn == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_dynamic_message no dynamic runtime loaded");
    return RMW_RET_UNSUPPORTED;
  }
  empty = weld_take_wire(sd, &wire, &wire_len, NULL, NULL);
  if (empty == 1) {
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  buf.buffer = wire;
  buf.buffer_length = wire_len;
  buf.buffer_capacity = wire_len;
  buf.allocator = rcutils_get_default_allocator();
  dyn_rc = dyn_fn(dynamic_message, &buf);
  free(wire);
  if (dyn_rc != RCUTILS_RET_OK) {
    weld_drop_add();
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_dynamic_message deserialize failed");
    return RMW_RET_ERROR;
  }
  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_dynamic_message_with_info(
  const rmw_subscription_t * subscription,
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message, bool * taken,
  rmw_message_info_t * message_info, rmw_subscription_allocation_t * allocation)
{
  struct weld_sub_data * sd = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  rcutils_uint8_array_t buf;
  weld_dyn_deserialize_fn dyn_fn = NULL;
  rcutils_ret_t dyn_rc = RCUTILS_RET_OK;
  unsigned long long seq = 0;
  uint8_t gid[16];
  int empty = 0;
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
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_dynamic_message_with_info no data");
    return RMW_RET_INVALID_ARGUMENT;
  }
  dyn_fn = weld_dyn_lookup();
  if (dyn_fn == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_dynamic_message_with_info no dynamic runtime");
    return RMW_RET_UNSUPPORTED;
  }
  (void)memset(gid, 0, sizeof(gid));
  empty = weld_take_wire(sd, &wire, &wire_len, &seq, gid);
  if (empty == 1) {
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  buf.buffer = wire;
  buf.buffer_length = wire_len;
  buf.buffer_capacity = wire_len;
  buf.allocator = rcutils_get_default_allocator();
  dyn_rc = dyn_fn(dynamic_message, &buf);
  free(wire);
  if (dyn_rc != RCUTILS_RET_OK) {
    weld_drop_add();
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_dynamic_message_with_info deserialize failed");
    return RMW_RET_ERROR;
  }
  weld_fill_take_info(message_info, seq, gid);
  *taken = true;
  return RMW_RET_OK;
}

#endif /* dynamic message typesupport available */

// Content filters: expressions compile in the Go backend and attach to
// the shared data receiver by topic, so wire takes apply them
// automatically. Same-process in-memory takes re-evaluate the stashed
// compiled filter in the FFI take path, so both legs enforce the same
// predicate and misses are never delivered. Set stores the expression
// plus parameters on the subscription for the get round trip and
// re-declares the backend reader; get returns the stored copies. A
// missing FFI symbol reports UNSUPPORTED and stores nothing.

rmw_ret_t rmw_subscription_set_content_filter(
  rmw_subscription_t * subscription,
  const rmw_subscription_content_filter_options_t * options)
{
  struct weld_sub_data * sd = NULL;
  char * copy = NULL;
  char * blob = NULL;
  char ** params = NULL;
  size_t nparams = 0;
  size_t total = 0;
  size_t i = 0;
  int rc = 0;
  if (subscription == NULL || options == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_set_content_filter null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_set_content_filter wrong impl");
  }
  if (options->filter_expression == NULL ||
    options->filter_expression[0] == '\0')
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_set_content_filter empty expression");
  }
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_content_filter no data");
    return RMW_RET_INVALID_ARGUMENT;
  }
  if (weld_ffi_sub_set_content_filter == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_content_filter FFI not linked");
    return RMW_RET_UNSUPPORTED;
  }
  nparams = options->expression_parameters.size;
  if (nparams > (size_t)INT_MAX) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_content_filter too many parameters");
    return RMW_RET_INVALID_ARGUMENT;
  }
  if (nparams > 0) {
    if (options->expression_parameters.data == NULL) {
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_subscription_set_content_filter null parameters");
      return RMW_RET_INVALID_ARGUMENT;
    }
    for (i = 0; i < nparams; i++) {
      const char * s = options->expression_parameters.data[i];
      size_t need = 0;
      if (s == NULL) {
        s = "";
      }
      need = strlen(s) + 1;
      if (total + need < total) {
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG(
          "rmw_weld: rmw_subscription_set_content_filter parameters too large");
        return RMW_RET_INVALID_ARGUMENT;
      }
      total += need;
    }
    blob = (char *)malloc(total > 0 ? total : 1);
    if (blob == NULL) {
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_subscription_set_content_filter no memory");
      return RMW_RET_BAD_ALLOC;
    }
    total = 0;
    for (i = 0; i < nparams; i++) {
      const char * s = options->expression_parameters.data[i];
      size_t need = 0;
      if (s == NULL) {
        s = "";
      }
      need = strlen(s) + 1;
      (void)memcpy(blob + total, s, need);
      total += need;
    }
  }
  rc = weld_ffi_sub_set_content_filter(
    sd->handle, options->filter_expression, blob, (int)nparams);
  free(blob);
  if (rc != 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_content_filter backend failed");
    return RMW_RET_ERROR;
  }
  copy = weld_dup_str(options->filter_expression);
  if (copy == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_content_filter no memory");
    return RMW_RET_BAD_ALLOC;
  }
  if (nparams > 0) {
    params = (char **)calloc(nparams, sizeof(*params));
    if (params == NULL) {
      free(copy);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_subscription_set_content_filter no memory");
      return RMW_RET_BAD_ALLOC;
    }
    for (i = 0; i < nparams; i++) {
      const char * s = options->expression_parameters.data[i];
      params[i] = weld_dup_str(s != NULL ? s : "");
      if (params[i] == NULL) {
        for (size_t j = 0; j < i; j++) {
          free(params[j]);
        }
        free(params);
        free(copy);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG(
          "rmw_weld: rmw_subscription_set_content_filter no memory");
        return RMW_RET_BAD_ALLOC;
      }
    }
  }
  free(sd->filter_expr);
  for (i = 0; i < sd->filter_nparams; i++) {
    free(sd->filter_params[i]);
  }
  free(sd->filter_params);
  sd->filter_expr = copy;
  sd->filter_params = params;
  sd->filter_nparams = nparams;
  subscription->is_cft_enabled = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_get_content_filter(
  const rmw_subscription_t * subscription, rcutils_allocator_t * allocator,
  rmw_subscription_content_filter_options_t * options)
{
  const struct weld_sub_data * sd = NULL;
  size_t n = 0;
  size_t i = 0;
  if (subscription == NULL || allocator == NULL || options == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_get_content_filter null argument");
  }
  if (allocator->allocate == NULL || allocator->deallocate == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_get_content_filter bad allocator");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_get_content_filter wrong impl");
  }
  sd = (const struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_get_content_filter no data");
    return RMW_RET_INVALID_ARGUMENT;
  }
  if (sd->filter_expr == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_get_content_filter no filter set");
    return RMW_RET_ERROR;
  }
  // The caller passes a zero-initialized options struct: the expression
  // is allocated fresh here and the stored parameters round trip back
  // in order.
  n = strlen(sd->filter_expr) + 1;
  options->filter_expression =
    (char *)allocator->allocate(n, allocator->state);
  if (options->filter_expression == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_get_content_filter no memory");
    return RMW_RET_BAD_ALLOC;
  }
  (void)memcpy(options->filter_expression, sd->filter_expr, n);
  if (rcutils_string_array_init(
      &options->expression_parameters, sd->filter_nparams,
      allocator) != RCUTILS_RET_OK)
  {
    allocator->deallocate(options->filter_expression, allocator->state);
    options->filter_expression = NULL;
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_get_content_filter no memory");
    return RMW_RET_BAD_ALLOC;
  }
  for (i = 0; i < sd->filter_nparams; i++) {
    const char * s = sd->filter_params[i] != NULL ? sd->filter_params[i] : "";
    size_t m = strlen(s) + 1;
    char * dup = (char *)allocator->allocate(m, allocator->state);
    if (dup == NULL) {
      rcutils_ret_t fini_rc = rcutils_string_array_fini(
        &options->expression_parameters);
      (void)fini_rc;
      allocator->deallocate(options->filter_expression, allocator->state);
      options->filter_expression = NULL;
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_subscription_get_content_filter no memory");
      return RMW_RET_BAD_ALLOC;
    }
    (void)memcpy(dup, s, m);
    options->expression_parameters.data[i] = dup;
  }
  return RMW_RET_OK;
}

// Network flow endpoints carry real transport socket data.
//
// The local entry is the node's advertised IPv4 (the same source the
// backend uses for SPDP locators) with the bound unicast port. Remote
// entries are discovered unicast ports from routes/SED at the loopback
// destination this transport sends peer DATA to; entries with an
// unknown port are skipped rather than zero-filled, and torn discovery
// reads skip quietly. The transport applies no per-flow QoS marking,
// so flow label and DSCP stay 0.

// Fill one network flow entry with socket data.
//
// is_v6 selects the internet protocol for addr. A bad address or a port
// outside 1..65535 fails with -1 and stores nothing.
static int weld_netflow_set(
  rmw_network_flow_endpoint_t * e, bool is_v6, const char * addr, int port)
{
  if (e == NULL || addr == NULL || addr[0] == '\0') {
    return -1;
  }
  if (port <= 0 || port > 65535) {
    return -1;
  }
  e->transport_protocol = RMW_TRANSPORT_PROTOCOL_UDP;
  if (is_v6) {
    e->internet_protocol = RMW_INTERNET_PROTOCOL_IPV6;
  } else {
    e->internet_protocol = RMW_INTERNET_PROTOCOL_IPV4;
  }
  e->transport_port = (uint16_t)port;
  // No per-flow QoS marking in this transport.
  e->flow_label = 0;
  e->dscp = 0;
  (void)snprintf(
    e->internet_address, sizeof(e->internet_address), "%s",
    addr);
  return 0;
}

// Fill a network flow array: one local socket entry plus best-effort remotes.
//
// topic selects the remote endpoints with remote_role (reader peers for
// a publisher, writer peers for a subscription). The local entry must
// succeed; remote failures keep the local entry only. Returns RMW_RET_OK
// with the filled array, UNSUPPORTED when the FFI is not linked, the
// bridge is missing, or no transport socket exists (in-memory only),
// and ERROR or BAD_ALLOC otherwise.
static rmw_ret_t weld_netflow_fill(
  const char * topic, int remote_role,
  rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * out)
{
  long long bridge = 0;
  char ip[RMW_INET_ADDRSTRLEN];
  int port = 0;
  long long n_remote = 0;
  size_t w = 0;
  long long i = 0;
  int rc = 0;
  if (topic == NULL) {
    topic = "";
  }
  if (weld_ffi_netflow_local == NULL ||
    weld_ffi_netflow_remote_count == NULL ||
    weld_ffi_netflow_remote_at == NULL)
  {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: netflow FFI not linked");
    return RMW_RET_UNSUPPORTED;
  }
  bridge = weld_graph_bridge();
  if (bridge == 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: netflow bridge not initialized");
    return RMW_RET_UNSUPPORTED;
  }
  (void)memset(ip, 0, sizeof(ip));
  rc = weld_ffi_netflow_local(
    bridge, ip, (int)sizeof(ip), &port);
  if (rc == -3) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: netflow no transport socket");
    return RMW_RET_UNSUPPORTED;
  }
  if (rc != 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: netflow backend failed");
    return RMW_RET_ERROR;
  }
  if (weld_ffi_netflow_remote_count(
      bridge, topic, remote_role, &n_remote) != 0 ||
    n_remote < 0)
  {
    n_remote = 0;
  }
  if (rmw_network_flow_endpoint_array_init(
      out, (size_t)(1 + n_remote), allocator) != RMW_RET_OK)
  {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: netflow init failed");
    return RMW_RET_BAD_ALLOC;
  }
  if (weld_netflow_set(
      &out->network_flow_endpoint[0],
      strchr(ip, ':') != NULL, ip, port) != 0)
  {
    rmw_ret_t fini_rc = rmw_network_flow_endpoint_array_fini(out);
    (void)fini_rc;
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: netflow backend failed");
    return RMW_RET_ERROR;
  }
  w = 1;
  for (i = 0; i < n_remote; i++) {
    char rip[RMW_INET_ADDRSTRLEN];
    int rport = 0;
    rmw_network_flow_endpoint_t * e = NULL;
    (void)memset(rip, 0, sizeof(rip));
    if (weld_ffi_netflow_remote_at(
        bridge, topic, remote_role, i, rip, (int)sizeof(rip),
        &rport) != 0)
    {
      continue;
    }
    e = &out->network_flow_endpoint[w];
    if (weld_netflow_set(
        e, strchr(rip, ':') != NULL, rip, rport) != 0) {
      continue;
    }
    w++;
  }
  out->size = w;
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_get_network_flow_endpoints(
  const rmw_publisher_t * publisher, rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * network_flow_endpoint_array)
{
  const struct weld_pub_data * pd = NULL;
  const char * topic = "";
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
  if (rmw_network_flow_endpoint_array_check_zero(
      network_flow_endpoint_array) != RMW_RET_OK)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_publisher_get_network_flow_endpoints not zero");
  }
  pd = (const struct weld_pub_data *)publisher->data;
  if (pd != NULL && pd->topic != NULL) {
    topic = pd->topic;
  }
  return weld_netflow_fill(
    topic, WELD_FILTER_ROLE_READER, allocator,
    network_flow_endpoint_array);
}

rmw_ret_t rmw_subscription_get_network_flow_endpoints(
  const rmw_subscription_t * subscription, rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * network_flow_endpoint_array)
{
  const struct weld_sub_data * sd = NULL;
  const char * topic = "";
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
  if (rmw_network_flow_endpoint_array_check_zero(
      network_flow_endpoint_array) != RMW_RET_OK)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_get_network_flow_endpoints not zero");
  }
  sd = (const struct weld_sub_data *)subscription->data;
  if (sd != NULL && sd->topic != NULL) {
    topic = sd->topic;
  }
  return weld_netflow_fill(
    topic, WELD_FILTER_ROLE_WRITER, allocator,
    network_flow_endpoint_array);
}

// Feature query: only dynamic message takes are real. The take
// dynamic paths move the wire sample into caller bound dynamic data
// through the runtime deserializer when it is linked, which Jazzy and
// newer name RMW_MIDDLEWARE_CAN_TAKE_DYNAMIC_MESSAGE (value 3).
// Sequence numbers stay unsupported (both MESSAGE_INFO values stay
// false) and no discovery population exists (TYPE_DISCOVERY stays
// false). Humble predates dynamic takes entirely and stays false for
// every value.
bool rmw_feature_supported(rmw_feature_t feature)
{
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")
  return feature == (rmw_feature_t)3;
#else
  (void)feature;
  return false;
#endif
}
