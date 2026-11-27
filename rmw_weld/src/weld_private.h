// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

#ifndef WELD_PRIVATE_H
#define WELD_PRIVATE_H

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

// Publisher GID authority from the Go core (weld_ffi_pub_get_gid).
//
// The bridge counter assigns the 16 byte writer identity shared with the
// take-info side channel, so rmw_compare_gids_equal holds in-process.
// Weak so an older Go library degrades to an error instead of crashing;
// synthesis is never used because it would diverge from take GIDs.
extern int weld_ffi_pub_get_gid(
  long long handle, char * gid_buf, int gid_len) __attribute__((weak));

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

// Publisher send stamp side channel (weld_ffi_sub_take_source).
//
// OutNs receives unix nanos captured at publish time, or zero when the
// leg carried none (wire takes). Callers fall back to local receive
// time on zero. Weak for the same old-library reason as take_info.
extern int weld_ffi_sub_take_source(
  long long handle, unsigned long long * out_ns) __attribute__((weak));

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

// Matched-entity queries match the internal/rmwbridge/weldffi/matched.go
// exports. They stay weak so counts fail loudly when the Go core is not
// linked instead of crashing.
extern int weld_ffi_pub_matched_subs(long long handle, int * out_n)
  __attribute__((weak));
extern int weld_ffi_sub_matched_pubs(long long handle, int * out_n)
  __attribute__((weak));

// Event deadline plus incompatibility wiring matches the
// internal/rmwbridge/weldffi/events.go exports. Every symbol stays
// weak so the paths fall back to local-only sources when an older Go
// library is linked instead of crashing.
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
// Event slots keyed by event handle for per-type baselines plus
// on-event callbacks. Guarded by g_weld_loan_mutex with the loan lists.
// Each slot binds one rmw_event_t to its endpoint at init: handle is the
// FFI pub or sub handle, endpoint points at the live weld_pub_data or
// weld_sub_data, and is_pub selects the side. Message lost tracks
// per-side totals selected by is_pub. Baselines mirror the take path so wait peeks without
// consuming: lost, alive, not-alive, deadline, wire incompat, and local
// incompat totals last reported. Callback fires on take when new data
// exists; wait wakes for polling without firing.
enum
{
  WELD_EVENT_SLOTS_MAX = 16
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
// RTPS server leg matches the weldffi serve_rtps exports: serve_rtps
// dequeues one wire request with its client guid plus seq, respond_rtps
// publishes the matching reply, and has_request_rtps peeks without
// consuming. Weak for the same old-library reason; a NULL address keeps
// the server leg in-memory only instead of crashing.
extern int weld_ffi_service_serve_rtps(
  long long handle, const char * idl_base, char * req_buf, int req_buf_len,
  int * out_n, long long * out_seq, char * out_guid) __attribute__((weak));
extern int weld_ffi_service_respond_rtps(
  long long handle, const char * guid, long long seq, const char * idl_base,
  const char * resp, int resp_len) __attribute__((weak));
extern int weld_ffi_service_has_request_rtps(
  long long handle, const char * idl_base, int * out_n)
  __attribute__((weak));
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
// send used instead of re-deciding after a topology change. For
// services, a false pending_local slot arrived via the RTPS wire leg
// and carries its client guid in pending_guid for the matching
// respond_rtps; a true slot arrived via the in-memory rendezvous and
// answers through respond.
// n_pending never exceeds WELD_RPC_PENDING_MAX.
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
struct weld_node_entry
{
  char * name;
  char * namespace_;
  size_t domain;
  struct weld_node_entry * next;
};
struct weld_graph_guard_entry
{
  const rmw_guard_condition_t * guard;
  struct weld_graph_guard_entry * next;
};
struct weld_loan_entry
{
  const void * endpoint;
  void * message;
  const rosidl_typesupport_introspection_c__MessageMembers * members;
  struct weld_loan_entry * next;
};
// Event slots keyed by event handle for per-type baselines plus
// on-event callbacks. Guarded by g_weld_loan_mutex with the loan lists.
// Each slot binds one rmw_event_t to its endpoint at init: handle is the
// FFI pub or sub handle, endpoint points at the live weld_pub_data or
// weld_sub_data, and is_pub selects the side. Message lost tracks
// per-side totals selected by is_pub. Baselines mirror the take path so wait peeks without
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
struct weld_graph_nodes
{
  char ** names;
  char ** spaces;
  size_t n;
  size_t cap;
};
struct weld_graph_entry
{
  char * name;
  char ** types;
  size_t ntypes;
  size_t captypes;
};
struct weld_graph_nt
{
  struct weld_graph_entry * items;
  size_t n;
  size_t cap;
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
// send used instead of re-deciding after a topology change. For
// services, a false pending_local slot arrived via the RTPS wire leg
// and carries its client guid in pending_guid for the matching
// respond_rtps; a true slot arrived via the in-memory rendezvous and
// answers through respond.
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
  uint8_t pending_guid[WELD_RPC_PENDING_MAX][8];
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
struct weld_svc_shared
{
  char * name;
  long long ffi_handle;
  size_t refs;
  size_t servers;
  struct weld_svc_shared * next;
};
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
struct weld_ep_entry
{
  int kind;
  char * node_name;
  char * node_ns;
  size_t domain;
  const void * data;
  struct weld_ep_entry * next;
};
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
typedef const char * (* weld_dyn_ident_fn)(void);
typedef rcutils_ret_t (* weld_dyn_deserialize_fn)(
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message,
  rcutils_uint8_array_t * buffer);


// Shared globals owned by their TUs; declared here for cross-TU use.
extern const char kImplementationIdentifier[];
extern pthread_mutex_t g_weld_ffi_mutex;
extern long long g_weld_bridge;
extern int g_weld_bridge_refs;
extern pthread_mutex_t g_weld_graph_mutex;
extern struct weld_node_entry * g_weld_nodes;
extern unsigned long long g_weld_graph_generation;
extern struct weld_graph_guard_entry * g_weld_graph_guards;
extern pthread_mutex_t g_weld_loan_mutex;
extern size_t g_weld_sub_drops;
extern size_t g_weld_pub_drops;
extern pthread_mutex_t g_weld_ep_mutex;
extern struct weld_ep_entry * g_weld_ep_list;
extern pthread_mutex_t g_weld_svc_mutex;
extern struct weld_svc_shared * g_weld_svc_shared;

// Cross-TU helpers. Definitions live in the owning TU;
// static-only helpers stay in their TU and are not declared here.
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
void weld_report_sub_type(
  long long handle,
  const rosidl_message_type_support_t * type_support);
void weld_report_dynamic_sub_type(
  long long handle, const char * type_name, const char * type_hash);
int weld_take_info(
  long long handle, unsigned long long * seq_out, uint8_t gid_out[16]);
int weld_take_source(long long handle, unsigned long long * ns_out);
int weld_poll_payload(
  long long handle, uint8_t ** out, size_t * out_len,
  unsigned long long * seq_out, uint8_t gid_out[16]);
bool weld_sub_consume_wait(
  struct weld_sub_data * sd, uint8_t ** out, size_t * out_len,
  unsigned long long * seq_out, uint8_t gid_out[16]);
int weld_take_wire(
  struct weld_sub_data * sd, uint8_t ** out, size_t * out_len,
  unsigned long long * seq_out, uint8_t gid_out[16]);
bool weld_sub_wait_poll(struct weld_sub_data * sd);
rmw_ret_t weld_unimplemented(const char * what);
bool weld_context_ok(const rmw_context_t * context);
bool weld_impl_eq(const char * got);
bool weld_node_ok(const rmw_node_t * node);
bool weld_pub_ok(const rmw_publisher_t * pub);
bool weld_sub_ok(const rmw_subscription_t * sub);
bool weld_svc_ok(const rmw_service_t * svc);
bool weld_cli_ok(const rmw_client_t * cli);
bool weld_waitset_ok(const rmw_wait_set_t * ws);
void weld_graph_lock(void);
void weld_graph_unlock(void);
rmw_ret_t weld_fail_invalid(const char * what);
rmw_ret_t weld_fail_impl(const char * what);
rmw_ret_t weld_fail_error(const char * what);
rmw_ret_t weld_fail_unsupported(const char * what);
char * weld_dup_str(const char * s);
void weld_node_register(
  const char * name, const char * namespace_, size_t domain);
void weld_node_unregister(
  const char * name, const char * namespace_, size_t domain);
void weld_graph_bump_and_trigger(void);
int weld_graph_guard_add(const rmw_guard_condition_t * guard);
void weld_graph_guard_remove(const rmw_guard_condition_t * guard);
long long weld_graph_bridge(void);
int weld_graph_nodes_push(
  struct weld_graph_nodes * v, const char * name, const char * ns);
void weld_graph_nodes_fini(struct weld_graph_nodes * v);
void weld_graph_nt_fini(struct weld_graph_nt * v);
int weld_graph_nt_add(
  struct weld_graph_nt * v, const char * name, const char * type);
bool weld_graph_nodes_has(
  const struct weld_graph_nodes * v, const char * name);
int weld_graph_fetch_pairs(
  int (* count_fn)(long long, long long *),
  int (* at_fn)(long long, long long, char *, int, char *, int),
  long long bridge, struct weld_graph_nt * out);
rmw_ret_t weld_graph_fill_names_and_types(
  const struct weld_graph_nt * v, rcutils_allocator_t * allocator,
  rmw_names_and_types_t * out, const char * what);
char * weld_graph_alloc_dup(
  const rcutils_allocator_t * allocator, const char * s);
int weld_loan_add(
  struct weld_loan_entry ** list, const void * endpoint, void * message,
  const rosidl_typesupport_introspection_c__MessageMembers * members);
int weld_loan_unlink(
  struct weld_loan_entry ** list, const void * endpoint, void * message,
  const rosidl_typesupport_introspection_c__MessageMembers ** members_out);
void weld_loan_restore_pub(
  const void * endpoint, void * message,
  const rosidl_typesupport_introspection_c__MessageMembers * members);
void weld_drop_add(void);
void weld_pub_drop_add(void);
void weld_fill_take_info(
  rmw_message_info_t * message_info, long long handle,
  unsigned long long seq, const uint8_t gid[16]);
int weld_take_loaned_wire(
  const rmw_subscription_t * subscription, void ** message_out,
  unsigned long long * seq_out, uint8_t gid_out[16]);
struct weld_event_slot * weld_event_slot_locked(
  const rmw_event_t * handle);
void weld_event_slot_release_locked(const rmw_event_t * handle);
int weld_event_matched_current(
  const struct weld_event_slot * slot, long long * out);
int weld_event_deadline_current(
  const struct weld_event_slot * slot, unsigned long long * out);
int weld_event_wire_incompat_current(
  const struct weld_event_slot * slot, unsigned long long * out);
int weld_event_local_incompat_current(
  const struct weld_event_slot * slot, size_t * out);
bool weld_event_bound_ready(const rmw_event_t * handle);
void weld_event_snapshot_locked(struct weld_event_slot * slot);
void * weld_sub_dispatch_thread(void * arg);
void weld_sub_dispatch_start(struct weld_sub_data * sd);
void weld_sub_dispatch_stop(struct weld_sub_data * sd);
const char * weld_dyn_identifier(void);
bool weld_dynamic_is_ts(const rosidl_message_type_support_t * type_support);
int weld_dynamic_type_name(
  const rosidl_message_type_support_t * type_support,
  char * buf, size_t buflen);
int weld_dynamic_fill(
  const rosidl_message_type_support_t * type_support,
  char ** name_out, char ** hash_out);
weld_dyn_deserialize_fn weld_dyn_lookup(void);
void weld_ep_register(
  int kind, const char * node_name, const char * node_ns,
  size_t domain, const void * data);
void weld_ep_unregister(const void * data);
rmw_ret_t weld_ep_snapshot(
  const char * name, int kind, size_t domain, bool use_domain,
  struct weld_ep_snap ** out_items, size_t * out_n);
void weld_ep_snapshot_fini(struct weld_ep_snap * items, size_t n);
size_t weld_incompat_scan(
  int new_kind, const rmw_qos_profile_t * new_qos, const char * topic,
  size_t domain);
size_t weld_matched_incompat_count(
  int new_kind, const rmw_qos_profile_t * new_qos, const char * topic,
  size_t domain);
bool weld_node_known(const char * name, const char * ns);
rmw_ret_t weld_by_node_fill(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace, int kind,
  rmw_names_and_types_t * out, const char * what);
rmw_ret_t weld_info_set_one(
  rmw_topic_endpoint_info_t * info, const char * node_name,
  const char * node_ns, const char * type, const uint8_t * gid,
  const rmw_qos_profile_t * qos, rmw_endpoint_type_t ep_type,
  rcutils_allocator_t * allocator);
void weld_info_gid(uint8_t gid[16], size_t domain, long long handle);
int weld_info_remote_qos(
  long long bridge, const char * name, int remote_role, long long index,
  const rmw_qos_profile_t * fallback, rmw_qos_profile_t * out);
rmw_ret_t weld_info_fill(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * name, int local_kind, int remote_role,
  rmw_endpoint_type_t ep_type, rmw_topic_endpoint_info_array_t * out);
int weld_netflow_set(
  rmw_network_flow_endpoint_t * e, bool is_v6, const char * addr, int port);
rmw_ret_t weld_netflow_fill(
  const char * topic, int remote_role,
  rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * out);
rmw_ret_t weld_ffi_bridge_borrow(long long * out);
void weld_svc_guid(
  struct weld_rpc_state * st, int64_t seq, uint8_t guid[8]);
int weld_service_idl_base(
  const rosidl_service_type_support_t * type_support,
  char * buf,
  size_t buflen);
long long weld_rpc_handle(struct weld_rpc_state * st);
rmw_ret_t weld_svc_acquire(
  long long bridge, const char * name, long long * out);
rmw_ret_t weld_svc_release(const char * name);
bool weld_svc_has_local_server(const struct weld_rpc_state * st);
bool weld_svc_wait_probe(struct weld_rpc_state * st);
bool weld_cli_wait_probe(struct weld_rpc_state * st);
void weld_wait_wake(void);
void weld_dispatch_sleep(void);
void weld_rpc_dispatch_start(struct weld_rpc_state * st);
void weld_rpc_dispatch_stop(struct weld_rpc_state * st);

#endif // WELD_PRIVATE_H
