// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

// waitset: guard conditions, wait sets, rmw_wait.

#include "weld_private.h"
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

void weld_wait_wake(void)
{
  (void)pthread_once(&g_weld_wait_once, weld_wait_cond_init);
  (void)pthread_mutex_lock(&g_weld_wait_mutex);
  (void)pthread_cond_broadcast(&g_weld_wait_cond);
  (void)pthread_mutex_unlock(&g_weld_wait_mutex);
}
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
    // Track the wait set bound: more attached conditions than the
    // set was created for grows the bound instead of failing. rcl
    // sizes the set at init but may add an interrupt guard or resize
    // after creation, and Cyclone plus FastDDS accept the larger wait;
    // failing here breaks stock ros2 CLI verbs.
    struct weld_wait_state * st = (struct weld_wait_state *)wait_set->data;
    size_t total = 0;
    if (st != NULL) {
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
        st->max_conditions = total;
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
rmw_wait_set_t * rmw_create_waitset(rmw_context_t * context, size_t max_conditions)
{
  return rmw_create_wait_set(context, max_conditions);
}

rmw_ret_t rmw_destroy_waitset(rmw_wait_set_t * wait_set)
{
  return rmw_destroy_wait_set(wait_set);
}
