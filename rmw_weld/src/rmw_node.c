// SPDX-License-Identifier: Apache-2.0
// rmw_node.c: node create, destroy, plus liveliness.
#include "rmw_common.h"

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
  if (namespace_[0] != '/') {
    (void)weld_fail_invalid("rmw_weld: rmw_create_node namespace not absolute");
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
      rmw_guard_condition_t * doomed = NULL;
      // Take the graph lock like the getter does: a concurrent
      // get would otherwise read the pointer while we clear it.
      weld_graph_lock();
      doomed = nd->graph_guard;
      nd->graph_guard = NULL;
      weld_graph_unlock();
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
  // Liveliness is asserted continuously by SPDP announcements, so an
  // explicit assert is already satisfied. Validation only.
  if (node == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_node_assert_liveliness null argument");
  }
  if (!weld_node_ok(node)) {
    return weld_fail_impl("rmw_weld: rmw_node_assert_liveliness wrong implementation");
  }
  return RMW_RET_OK;
}
