// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//
// rmw_common.h: shared decls for the rmw_weld split modules.
//
// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//
// rmw_weld: ROS middleware shim forwarding RMW calls to the Weld Go core.
// Backend contract: internal/rmwbridge/bridge.go (identifier "rmw_weld")
// plus internal/transports/transport.go Backend (PublishBytes, TakeBytes,
// JoinDiscovery, ReportLiveliness, Close). Every entry point below forwards
// to the Go core over the weld_ffi bridge, validates its arguments, and
// fails loudly with RMW_RET_ERROR or RMW_RET_UNSUPPORTED through
// rmw_reset_error plus RCUTILS_SET_ERROR_MSG instead of crashing.
//
// Threading contract: entity handles are not reference counted. Do not
// destroy a subscription, service, client, guard, node, or wait set
// concurrently with a wait, take, trigger, or callback using it; the
// destroy paths join their own threads but do not fence concurrent API
// users. Single-threaded executors satisfy this by construction;
// anything else must serialize destroy against use externally.
// Destroying an already-destroyed handle is caller undefined behavior:
// freed memory cannot be revalidated, so every destroy fails loudly on
// NULL data or a wrong implementation instead of on a second free.


#ifndef RMW_WELD_COMMON_H
#define RMW_WELD_COMMON_H

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

// _DEFAULT_SOURCE exposes getifaddrs plus IFF_UP for the network flow
// endpoint queries below. It coexists with _POSIX_C_SOURCE and only
// affects the BSD socket declarations.
#ifndef _DEFAULT_SOURCE
#define _DEFAULT_SOURCE
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
#include <stdint.h>
#include <unistd.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
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
#include "rosidl_runtime_c/service_type_support_struct.h"
#if defined(__has_include)
#if __has_include("rosidl_runtime_c/type_hash.h")
#include "rosidl_runtime_c/type_hash.h"
#define WELD_HAVE_TYPE_HASH 1
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
#include "rosidl_dynamic_typesupport/dynamic_message_type_support_struct.h"
#include "rosidl_dynamic_typesupport/identifier.h"
#endif
#if __has_include("rmw/event_callback_type.h")
#include "rmw/event_callback_type.h"
#endif
#if __has_include("rmw/events_statuses/message_lost.h")
#include "rmw/events_statuses/message_lost.h"
#define WELD_HAVE_MESSAGE_LOST 1
#endif
#if __has_include("rosidl_dynamic_typesupport/api/dynamic_data.h")
#include "rosidl_dynamic_typesupport/api/dynamic_data.h"
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
#include <rosidl_runtime_c/message_initialization.h>


// Shared FFI imports, constants, structs, and cross-file helpers.

// FFI imports from the Go core (internal/rmwbridge/weldffi).
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

extern int weld_ffi_sub_has_data(long long handle)
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

// Dynamic subscription type report in slash form (such as
// std_msgs/msg/String) plus the RIHS hash. The Go side normalizes the
// name to IDL form and declares the reader. Weak for the same
// old-library reason as the static form.
extern int weld_ffi_sub_set_dynamic_type(
  long long handle, const char * type_name, const char * type_hash)
  __attribute__((weak));

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
  // Content filter expression stored by
  // rmw_subscription_set_content_filter for
  // rmw_subscription_get_content_filter. NULL means no filter is set.
  // The take path applies it inside the Go backend.
  char * filter_expr;
  // Wait stash holds one wire sample consumed by rmw_wait so the next
  // rmw_take returns it instead of polling again. Wait and take run on
  // the same thread in rcl executors, so no lock is needed.
  uint8_t * wait_buf;
  size_t wait_len;
  bool has_wait;
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

// Local endpoint registry kinds for info-by-topic plus service queries:
// 0 publisher, 1 subscription, 2 service server, 3 service client.
// The registry itself lives beside the info queries below; these early
// declarations let the create plus destroy paths record endpoints.
enum
{
  WELD_EP_PUB = 0,
  WELD_EP_SUB = 1,
  WELD_EP_SVC = 2,
  WELD_EP_CLI = 3
};

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

// Service, client, guard, and waitset FFI wiring.
//
// The Go core (internal/rmwbridge/weldffi) exports the bridge plus
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

// Service RTPS wiring matches the internal/rmwbridge/weldffi service
// exports. Every symbol stays weak: a NULL address means an older Go
// library is linked and the path falls back to in-memory only instead
// of crashing. set_type records the IDL service base from the C
// introspection handle for RTPS endpoint declares. call_rtps publishes
// a CDR request with the sequence id and blocks for the reply.
// has_request and has_response peek without consuming for rmw_wait.
extern int weld_ffi_service_set_type(
  long long handle, const char * type_name, const char * type_hash)
  __attribute__((weak));

extern int weld_ffi_service_has_request(long long handle, int * out_n)
  __attribute__((weak));

extern int weld_ffi_service_has_response(
  long long handle, long long seq, const char * guid, const char * idl_base,
  int * out_n) __attribute__((weak));

// Eager service delivery splits the old combined call in two. send_nb
// enqueues an in-memory request and parks the reply waiter without
// blocking; poll_reply waits bounded for that reply and consumes it.
// send_rtps publishes an RTPS request immediately; take_reply polls the
// reply topic with the same bound. All four stay weak like the rest:
// a NULL address means an older Go library and the send fails loudly
// instead of silently dropping.
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

// Content filter plus remote endpoint queries match the
// internal/rmwbridge/weldffi/filter.go exports. They stay weak so the
// info plus filter paths fail loudly when the Go core is not linked
// instead of crashing.
extern int weld_ffi_sub_set_content_filter(
  long long handle, const char * expr, const char * params, int nparams)
  __attribute__((weak));

extern int weld_ffi_remote_ep_count(
  long long bridge, const char * topic, int role, long long * out)
  __attribute__((weak));

extern int weld_ffi_remote_ep_at(
  long long bridge, const char * topic, int role, long long index,
  char * type_buf, int type_len, char * gid_buf, int gid_len)
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
// for a reply. type_support drives CDR encode plus decode through the
// service introspection request plus response members. Sharing one Go
// service per name means N servers on one name steal each other's
// requests (1 server x N clients is the supported shape); give each
// server its own service name instead.
// pending_is_echo parallels pending: a true slot means the take
// arrived as an 8 byte sequence echo with no encoded bytes.
// pending_local parallels pending: a true slot sent while a local
// server existed, so the take waits on the same leg the send used
// instead of re-deciding after a topology change.
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

// Process bridge, node registry head, loan registry head,
// and log severity shared across modules.
extern pthread_mutex_t g_weld_ffi_mutex;
extern long long g_weld_bridge;
extern int g_weld_bridge_refs;
extern const char kImplementationIdentifier[];
extern struct weld_node_entry * g_weld_nodes;
extern pthread_mutex_t g_weld_loan_mutex;
extern size_t g_weld_decode_drops;

// Cross-file helpers (definitions live in the noted modules).
rmw_ret_t weld_fail_invalid(const char * what);
rmw_ret_t weld_fail_impl(const char * what);
rmw_ret_t weld_fail_error(const char * what);
rmw_ret_t weld_fail_unsupported(const char * what);
char * weld_dup_str(const char * s);
rmw_ret_t weld_fail_ffi(const char * what, int code);
const rosidl_typesupport_introspection_c__MessageMembers *
weld_resolve_members(const rosidl_message_type_support_t * type_support);
int weld_type_name(
  const rosidl_typesupport_introspection_c__MessageMembers * members,
  char * buf, size_t buflen);
int weld_type_hash(
  const rosidl_message_type_support_t * type_support,
  char * buf, size_t buflen);
void weld_report_type(
  long long handle,
  const rosidl_message_type_support_t * type_support);
int weld_take_wire(struct weld_sub_data * sd, uint8_t ** out, size_t * out_len);
bool weld_sub_wait_poll(struct weld_sub_data * sd);
void weld_dispatch_sleep(void);
void weld_sub_dispatch_start(struct weld_sub_data * sd);
rmw_ret_t weld_unimplemented(const char * what);
bool weld_context_ok(const rmw_context_t * context);
bool weld_impl_eq(const char * got);
bool weld_node_ok(const rmw_node_t * node);
bool weld_pub_ok(const rmw_publisher_t * pub);
bool weld_sub_ok(const rmw_subscription_t * sub);
bool weld_svc_ok(const rmw_service_t * svc);
bool weld_cli_ok(const rmw_client_t * cli);
bool weld_waitset_ok(const rmw_wait_set_t * ws);
void weld_wait_wake(void);
void weld_graph_lock(void);
void weld_graph_unlock(void);
bool weld_dynamic_is_ts(const rosidl_message_type_support_t * type_support);
int weld_dynamic_fill(
  const rosidl_message_type_support_t * type_support,
  char ** name_out, char ** hash_out);
void weld_report_dynamic_sub_type(
  long long handle, const char * type_name, const char * type_hash);
void weld_graph_bump_and_trigger(void);
void weld_graph_guard_remove(const rmw_guard_condition_t * guard);
void weld_svc_guid(
  struct weld_rpc_state * st, int64_t seq, uint8_t guid[8]);
int weld_service_idl_base(
  const rosidl_service_type_support_t * type_support,
  char * buf,
  size_t buflen);
long long weld_rpc_handle(struct weld_rpc_state * st);
rmw_ret_t weld_ffi_bridge_borrow(long long * out);
bool weld_svc_dispatch_probe(struct weld_rpc_state * st);
bool weld_cli_dispatch_probe(struct weld_rpc_state * st);
void weld_rpc_dispatch_start(struct weld_rpc_state * st);
void weld_ep_register(
  int kind, const char * node_name, const char * node_ns,
  size_t domain, const void * data);
void weld_ep_unregister(const void * data);
void weld_drop_add(void);
void weld_fill_take_info(rmw_message_info_t * message_info);
bool weld_dispatch_has_stashed(const rmw_subscription_t * subscription);

#endif
