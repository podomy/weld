// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

// graph: node registry, graph queries, endpoint registry, info.

#include "weld_private.h"
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

pthread_mutex_t g_weld_graph_mutex = PTHREAD_MUTEX_INITIALIZER;
struct weld_node_entry * g_weld_nodes = NULL;

// Graph generation plus per-node graph guard list.
//
// g_weld_graph_generation counts node join plus leave events in this
// process. Each node owns at most one graph guard (see weld_node_data)
// returned by rmw_node_get_graph_guard_condition. The guard list holds
// those live guards so a generation bump can wake every waiter. The
// list and the counter share g_weld_graph_mutex with the node list.
unsigned long long g_weld_graph_generation = 0;


struct weld_graph_guard_entry * g_weld_graph_guards = NULL;
// Borrow the process bridge handle for graph queries, or 0 when uninit.
long long weld_graph_bridge(void)
{
  long long bridge = 0;
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  bridge = g_weld_bridge;
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  return bridge;
}

// Growable node list for merged graph snapshots. Names and namespaces
// hold malloc owned strings freed by weld_graph_nodes_fini.

void weld_graph_nodes_fini(struct weld_graph_nodes * v)
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
int weld_graph_nodes_push(
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

// Growable names plus types list for merged topic and service snapshots.

void weld_graph_nt_fini(struct weld_graph_nt * v)
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
int weld_graph_nt_add(
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
bool weld_graph_nodes_has(
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
int weld_graph_fetch_pairs(
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
char * weld_graph_alloc_dup(
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
rmw_ret_t weld_graph_fill_names_and_types(
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

rmw_ret_t rmw_get_gid_for_publisher(const rmw_publisher_t * publisher, rmw_gid_t * gid)
{
  // Bridge counter authority: the 16 byte writer identity comes from the
  // Go bridge (domain in the first 4 bytes, counter in the next 8),
  // the same authority backing the take-info side channel, so
  // rmw_compare_gids_equal holds in-process between publisher GIDs and
  // take GIDs. Wire takes with no writer identity keep the zero-GID
  // documented fallback. Synthesis is never used.
  const struct weld_pub_data * pd = NULL;
  uint8_t raw[16];
  size_t copy_len = 0;
  int rc = 0;
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
  if (weld_ffi_pub_get_gid == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_gid_for_publisher FFI not linked");
    return RMW_RET_ERROR;
  }
  (void)memset(raw, 0, sizeof(raw));
  rc = weld_ffi_pub_get_gid(pd->handle, (char *)raw, 16);
  if (rc != 0) {
    return weld_fail_ffi("rmw_weld: rmw_get_gid_for_publisher backend failed", rc);
  }
  (void)memset(gid->data, 0, sizeof(gid->data));
  copy_len = sizeof(raw);
  if (copy_len > sizeof(gid->data)) {
    copy_len = sizeof(gid->data);
  }
  (void)memcpy(gid->data, raw, copy_len);
  gid->implementation_identifier = kImplementationIdentifier;
  return RMW_RET_OK;
}

rmw_ret_t rmw_publisher_count_matched_subscriptions(
  const rmw_publisher_t * publisher, size_t * subscription_count)
{
  // Bridge: Publisher.CountMatchedSubscriptions counts live subscriptions
  // in the same per-domain pubs plus subs registry on the same topic.
  // Closed subscriptions are excluded. Local endpoints filter by QoS
  // compatibility (WARNING excluded); remote endpoints keep passthrough
  // because remote QoS may be unknown, documented here fail-closed.
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
    {
      size_t incompat = 0;
      if (pd->topic != NULL) {
        incompat = weld_matched_incompat_count(
          WELD_EP_PUB, &pd->qos, pd->topic, pd->domain);
      }
      if ((size_t)n > incompat) {
        *subscription_count = (size_t)n - incompat;
      } else {
        *subscription_count = 0;
      }
    }
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_count_matched_publishers(
  const rmw_subscription_t * subscription, size_t * publisher_count)
{
  // Bridge: Subscription.CountMatchedPublishers counts live publishers in
  // the same per-domain registry on the same topic. Closed publishers are
  // excluded. Local endpoints filter by QoS compatibility (WARNING
  // excluded); remotes keep passthrough where QoS is unknown.
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
    {
      size_t incompat = 0;
      if (sd->topic != NULL) {
        incompat = weld_matched_incompat_count(
          WELD_EP_SUB, &sd->qos, sd->topic, sd->domain);
      }
      if ((size_t)n > incompat) {
        *publisher_count = (size_t)n - incompat;
      } else {
        *publisher_count = 0;
      }
    }
  }
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
bool weld_node_known(const char * name, const char * ns)
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


pthread_mutex_t g_weld_ep_mutex = PTHREAD_MUTEX_INITIALIZER;

struct weld_ep_entry * g_weld_ep_list = NULL;

// Record one live local endpoint. Best effort: alloc failure skips
// silently and info queries report fewer locals.
void weld_ep_register(
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
void weld_ep_unregister(const void * data)
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

void weld_ep_snapshot_fini(struct weld_ep_snap * items, size_t n)
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
rmw_ret_t weld_ep_snapshot(
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
size_t weld_incompat_scan(
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

// Count QoS-incompatible local opposite endpoints without mutating.
//
// Snapshots opposite-kind endpoints on the same topic plus domain and
// runs rmw_qos_profile_check_compatible for each pair. WARNING counts as
// incompatible and is excluded from matched counts; OK counts as
// matched. Check errors never count (fail-closed passthrough) so a probe
// failure does not shrink the matched set. Remote endpoints keep
// passthrough: unknown remote QoS counts as matched, documented at the
// call sites.
size_t weld_matched_incompat_count(
  int new_kind, const rmw_qos_profile_t * new_qos, const char * topic,
  size_t domain)
{
  struct weld_ep_snap * snaps = NULL;
  size_t n_snaps = 0;
  size_t mismatched = 0;
  size_t i = 0;
  int opp_kind = WELD_EP_SUB;
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
    if (rc != RMW_RET_OK) {
      continue;
    }
    if (compat != RMW_QOS_COMPATIBILITY_OK) {
      mismatched++;
    }
  }
  weld_ep_snapshot_fini(snaps, n_snaps);
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
rmw_ret_t weld_by_node_fill(
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
rmw_ret_t weld_info_set_one(
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
void weld_info_gid(uint8_t gid[16], size_t domain, long long handle)
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
int weld_info_remote_qos(
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
  } else if (liveliness == 1 || liveliness == 2) {
    // DDS 1.4 section 2.2.3.11 has automatic 0, manual by participant 1,
    // and manual by topic 2. RMW has no participant kind and Jazzy
    // deprecates MANUAL_BY_NODE, so both manual kinds report TOPIC.
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
rmw_ret_t weld_info_fill(
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
int weld_netflow_set(
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
rmw_ret_t weld_netflow_fill(
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
