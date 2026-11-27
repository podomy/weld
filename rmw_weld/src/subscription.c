// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

// subscription: subscriptions, takes, dispatch, type reports.

#include "weld_private.h"
const rosidl_typesupport_introspection_c__MessageMembers *
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
int weld_type_name(
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
int weld_type_hash(
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
void weld_report_type(
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
void weld_report_sub_type(
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
void weld_report_dynamic_sub_type(
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
int weld_take_info(
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

// Read the publisher send stamp for the last delivered sample.
//
// Returns 0 with the unix nanos on success, or -1 with zero when the
// side channel is missing, fails, or carried none (wire takes). A zero
// stamp means the caller falls back to local receive time.
int weld_take_source(long long handle, unsigned long long * ns_out)
{
  unsigned long long ns = 0;
  int rc = 0;
  if (ns_out == NULL) {
    return -1;
  }
  *ns_out = 0;
  if (weld_ffi_sub_take_source == NULL) {
    return -1;
  }
  rc = weld_ffi_sub_take_source(handle, &ns);
  if (rc != 0 || ns == 0) {
    *ns_out = 0;
    return -1;
  }
  *ns_out = ns;
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
int weld_poll_payload(
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
bool weld_sub_consume_wait(
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
int weld_take_wire(
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
bool weld_sub_wait_poll(struct weld_sub_data * sd)
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
// Sleep the dispatch poll period. EINTR retries so signals never turn
// the dispatcher into a hot loop.
void weld_dispatch_sleep(void)
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
void * weld_sub_dispatch_thread(void * arg)
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
void weld_sub_dispatch_start(struct weld_sub_data * sd)
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
void weld_sub_dispatch_stop(struct weld_sub_data * sd)
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
  // local metadata through weld_fill_take_info. The source stamp prefers
  // the publisher-side send stamp when the in-process leg carried one
  // and falls back to local receive time for wire takes, while the
  // sequence plus GID ride the take info side channel.
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
  weld_fill_take_info(message_info, sd->handle, seq, gid);
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
    weld_fill_take_info(info, sd->handle, seq, gid);
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
  weld_fill_take_info(message_info, sd->handle, seq, gid);
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
