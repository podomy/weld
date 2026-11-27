// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

// event: events, callbacks, take-event baselines.

#include "weld_private.h"
static struct weld_event_slot g_weld_event_slots[WELD_EVENT_SLOTS_MAX];

// Find or claim a slot for handle. The caller holds g_weld_loan_mutex.
// Returns the slot, or NULL when the table is full.
struct weld_event_slot * weld_event_slot_locked(
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
void weld_event_slot_release_locked(const rmw_event_t * handle)
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
int weld_event_matched_current(
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
int weld_event_deadline_current(
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
int weld_event_wire_incompat_current(
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
int weld_event_local_incompat_current(
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
bool weld_event_bound_ready(const rmw_event_t * handle)
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
        ready = (slot->is_pub ? g_weld_pub_drops : g_weld_sub_drops) >
          slot->lost_reported;
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
// Forward declarations: event binding snapshots live in the take section
// below, but init paths above bind before take runs.
void weld_event_snapshot_locked(struct weld_event_slot * slot);
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
// MESSAGE_LOST counts per-side drops: sub takes count decode drops
// (see g_weld_sub_drops) while pubs count publish-side encode or send
// losses (see g_weld_pub_drops). LIVELINESS_CHANGED reports matched-publisher
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
void weld_event_snapshot_locked(struct weld_event_slot * slot)
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
      slot->lost_reported = slot->is_pub ? g_weld_pub_drops :
        g_weld_sub_drops;
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
      slot = weld_event_slot_locked(event_handle);
      if (slot != NULL) {
        total = slot->is_pub ? g_weld_pub_drops : g_weld_sub_drops;
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
      long long not_alive = 0;
      long long not_last = 0;
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
            not_last = slot->not_alive_reported;
            not_alive = not_last;
            // Drops count as not-alive transitions fail-closed: without
            // per-writer lease tracking, an unmatch (dispose or close)
            // moves alive to not-alive. Lease expiry without unmatch
            // stays untracked, documented above.
            if (matched < last) {
              not_alive = not_last + (last - matched);
            }
            slot->alive_reported = matched;
            slot->not_alive_reported = not_alive;
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
      status->not_alive_count = weld_event_clamp32(
        not_alive < 0 ? 0 : (unsigned long long)not_alive);
      status->alive_count_change = weld_event_clamp32(
        matched >= last ? (unsigned long long)(matched - last) : 0);
      if (matched < last) {
        long long drop = last - matched;
        status->alive_count_change = (int32_t)-weld_event_clamp32(
          (unsigned long long)drop);
      }
      status->not_alive_count_change = weld_event_clamp32(
        not_alive >= not_last ?
        (unsigned long long)(not_alive - not_last) : 0);
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
