// SPDX-License-Identifier: Apache-2.0
// rmw_waitset.c: guards, wait sets, plus rmw_wait.
#include "rmw_common.h"

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

// Poll one service for wait readiness without losing data.
//
// A pending request is consumed into the FFI stash so the next take
// returns it; the stash hit reports ready on later polls. In-memory
// and RTPS requests both report ready. Poll errors report not ready so
// wait keeps polling until the budget expires.
static bool weld_svc_wait_poll(struct weld_rpc_state * st)
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

// Poll one client for wait readiness without losing data.
//
// Both reply legs report ready here without consuming, so executors
// waiting on clients fire for local servers as well as remotes. The
// has_response peek reports a parked in-memory reply (the
// weld_take_response_local route) together with an RTPS wire reply
// (the weld_take_response_rtps route), so one peek observes both legs.
// Without the peek symbol an old library leaves local replies
// wait-invisible and only take_response delivers them. Peek errors
// report not ready so wait keeps polling until the budget expires.
static bool weld_cli_wait_poll(struct weld_rpc_state * st)
{
  int64_t seq = 0;
  uint8_t guid[8];
  char base[512];
  long long handle = 0;
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
  handle = st->ffi_handle;
  (void)pthread_mutex_unlock(&st->mutex);
  if (handle == 0) {
    return false;
  }
  weld_svc_guid(st, seq, guid);
  if (weld_service_idl_base(st->type_support, base, sizeof(base)) != 0) {
    return false;
  }
  if (weld_ffi_service_has_response(
      handle, (long long)seq, (const char *)guid, base, &n) != 0)
  {
    return false;
  }
  return n > 0;
}

// Probe one service for a pending request without consuming.
//
// has_request only reports a count, so this never disturbs takes.
bool weld_svc_dispatch_probe(struct weld_rpc_state * st)
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
bool weld_cli_dispatch_probe(struct weld_rpc_state * st)
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
  // Drop the graph wake entry first: node graph guards live in the
  // shared list until destroyed, and a stale pointer would wake freed
  // memory on the next graph bump. Missing entries are ignored, so the
  // node destroy path that already removed its guard stays safe.
  weld_graph_guard_remove(guard_condition);
  // Release the Go handle before freeing local state: on failure the
  // guard stays valid so the caller may retry the destroy instead
  // of using freed memory. Best effort when the destroy export is
  // missing; a reset clears its triggered state and the handle stays
  // valid until process end.
  long long doomed = st->ffi_handle;
  if (doomed != 0) {
    if (weld_ffi_guard_destroy != NULL) {
      if (weld_ffi_guard_destroy(doomed) != 0) {
        return weld_fail_error("rmw_weld: rmw_destroy_guard_condition ffi failed");
      }
    } else if (weld_ffi_guard_reset != NULL) {
      (void)weld_ffi_guard_reset(doomed);
    }
  }
  (void)pthread_mutex_destroy(&st->mutex);
  (void)pthread_cond_destroy(&st->cond);
  free(st);
  free(guard_condition);
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
  // Events are unsupported and fail loudly when any are attached.
  // NULL slots are empty from a previous wait filter and skip.
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
  if (events != NULL && events->event_count > 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_wait events unsupported");
    return RMW_RET_UNSUPPORTED;
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
      if (total > st->max_conditions) {
        return weld_fail_invalid("rmw_weld: rmw_wait too many conditions");
      }
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
        if (st != NULL && weld_svc_wait_poll(st)) {
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
        if (st != NULL && weld_cli_wait_poll(st)) {
          ready++;
        } else {
          clients->clients[i] = NULL;
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
      struct timespec sleep_ts;
      sleep_ts.tv_sec = 0;
      sleep_ts.tv_nsec = 10 * 1000 * 1000L;
      (void)nanosleep(&sleep_ts, NULL);
    }
  }
}

rmw_wait_set_t * rmw_create_waitset(rmw_context_t * context, size_t max_conditions)
{
  return rmw_create_wait_set(context, max_conditions);
}

rmw_ret_t rmw_destroy_waitset(rmw_wait_set_t * wait_set)
{
  return rmw_destroy_wait_set(wait_set);
}
