// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

// node: node lifecycle, graph guards, liveliness asserts.

#include "weld_private.h"
// Liveliness generation: a process-wide monotonic counter. Repeating a
// generation with the same alive value is idempotent; any other reuse
// is stale, so every assert consumes a fresh value.
static _Atomic unsigned long long g_weld_liveliness_gen = 1;
void weld_node_register(
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

void weld_node_unregister(
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
void weld_graph_bump_and_trigger(void)
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
int weld_graph_guard_add(const rmw_guard_condition_t * guard)
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
void weld_graph_guard_remove(const rmw_guard_condition_t * guard)
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
