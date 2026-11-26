// SPDX-License-Identifier: Apache-2.0
// rmw_event.c: events, callbacks, plus on-new hooks.
#include "rmw_common.h"

// Forward declaration: fini releases the
// slot defined below.
static void weld_event_slot_release_locked(
  const rmw_event_t * handle);

rmw_ret_t rmw_publisher_event_init(
  rmw_event_t * rmw_event, const rmw_publisher_t * publisher,
  rmw_event_type_t event_type)
{
  if (rmw_event == NULL || publisher == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publisher_event_init null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publisher_event_init wrong impl");
  }
  // Only message lost is tracked; other types stay unsupported with the
  // event left untouched per RMW error-path rules.
  if (event_type != RMW_EVENT_MESSAGE_LOST) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publisher_event_init type unsupported");
    return RMW_RET_UNSUPPORTED;
  }
  rmw_event->implementation_identifier = kImplementationIdentifier;
  rmw_event->data = NULL;
  rmw_event->event_type = event_type;
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_event_init(
  rmw_event_t * rmw_event, const rmw_subscription_t * subscription,
  rmw_event_type_t event_type)
{
  if (rmw_event == NULL || subscription == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_event_init null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_event_init wrong impl");
  }
  // Only message lost is tracked; other types stay unsupported with the
  // event left untouched per RMW error-path rules.
  if (event_type != RMW_EVENT_MESSAGE_LOST) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_subscription_event_init type unsupported");
    return RMW_RET_UNSUPPORTED;
  }
  rmw_event->implementation_identifier = kImplementationIdentifier;
  rmw_event->data = NULL;
  rmw_event->event_type = event_type;
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

struct weld_event_slot
{
  const rmw_event_t * handle;
  rmw_event_callback_t callback;
  const void * user_data;
  size_t lost_reported;
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

// Events: only MESSAGE_LOST is backed, by the decode drop counter in
// the loaned and dynamic take paths (see g_weld_decode_drops). Other
// event types report RMW_RET_UNSUPPORTED loudly. Event init stays
// unsupported elsewhere in this file, so handles carrying this identifier
// cannot be created yet; take and set_callback still validate and behave
// per the RMW header. set_callback stores the registration and starts a
// per-endpoint dispatch thread that invokes it on arrival edges;
// rmw_wait still clears event entries.

rmw_ret_t rmw_take_event(
  const rmw_event_t * event_handle, void * event_info, bool * taken)
{
  if (event_handle == NULL || event_info == NULL || taken == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_event null argument");
  }
  if (event_handle->implementation_identifier != kImplementationIdentifier) {
    return weld_fail_impl("rmw_weld: rmw_take_event wrong implementation");
  }
  if (event_handle->event_type != RMW_EVENT_MESSAGE_LOST) {
    *taken = false;
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_event event type not supported");
    return RMW_RET_UNSUPPORTED;
  }
#ifdef WELD_HAVE_MESSAGE_LOST
  {
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
    }
    (void)pthread_mutex_unlock(&g_weld_loan_mutex);
    change = total - last;
    status->total_count = total;
    status->total_count_change = change;
    *taken = change > 0;
    return RMW_RET_OK;
  }
#else
  *taken = false;
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_event message lost unavailable");
  return RMW_RET_UNSUPPORTED;
#endif
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
  return rmw_event_type == RMW_EVENT_MESSAGE_LOST;
}
