// SPDX-License-Identifier: Apache-2.0
// rmw_graph.c: graph, endpoints, GIDs, plus netflow.
#include "rmw_common.h"

struct weld_node_entry * g_weld_nodes = NULL;

// Graph generation plus per-node graph guard list.
//
// g_weld_graph_generation counts node join plus leave events in this
// process. Each node owns at most one graph guard (see weld_node_data)
// returned by rmw_node_get_graph_guard_condition. The guard list holds
// those live guards so a generation bump can wake every waiter. The
// list and the counter share g_weld_graph_mutex with the node list.
static unsigned long long g_weld_graph_generation = 0;

struct weld_graph_guard_entry
{
  const rmw_guard_condition_t * guard;
  struct weld_graph_guard_entry * next;
};

static struct weld_graph_guard_entry * g_weld_graph_guards = NULL;

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
static int weld_graph_guard_add(const rmw_guard_condition_t * guard)
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

// Graph entry buffer sizes for one FFI string copy.
enum
{
  WELD_GRAPH_NAME_MAX = 1024,
  WELD_GRAPH_NAME_BIG = 16384
};

// Borrow the process bridge handle for graph queries, or 0 when uninit.
static long long weld_graph_bridge(void)
{
  long long bridge = 0;
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  bridge = g_weld_bridge;
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  return bridge;
}

// Growable node list for merged graph snapshots. Names and namespaces
// hold malloc owned strings freed by weld_graph_nodes_fini.
struct weld_graph_nodes
{
  char ** names;
  char ** spaces;
  size_t n;
  size_t cap;
};

static void weld_graph_nodes_fini(struct weld_graph_nodes * v)
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
static int weld_graph_nodes_push(
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
struct weld_graph_entry
{
  char * name;
  char ** types;
  size_t ntypes;
  size_t captypes;
};

// Growable names plus types list for merged topic and service snapshots.
struct weld_graph_nt
{
  struct weld_graph_entry * items;
  size_t n;
  size_t cap;
};

static void weld_graph_nt_fini(struct weld_graph_nt * v)
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
static int weld_graph_nt_add(
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
static bool weld_graph_nodes_has(
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
static int weld_graph_fetch_pairs(
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
static char * weld_graph_alloc_dup(
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
static rmw_ret_t weld_graph_fill_names_and_types(
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

// Remote endpoint roles mirror the weldFilterRole constants in
// internal/rmwbridge/weldffi/filter.go. Writers feed publishers info,
// readers feed subscriptions info, servers plus clients feed the
// service info queries.
enum
{
  WELD_FILTER_ROLE_WRITER = 0,
  WELD_FILTER_ROLE_READER = 1,
  WELD_FILTER_ROLE_SERVER = 2,
  WELD_FILTER_ROLE_CLIENT = 3
};

rmw_ret_t rmw_get_gid_for_publisher(const rmw_publisher_t * publisher, rmw_gid_t * gid)
{
  // Bridge: 16 byte writer identity with domain in the first 4 bytes
  // little endian and the FFI publisher handle in the next 8 bytes,
  // mirroring dds WriterGUID width. Unique within one domain.
  const struct weld_pub_data * pd = NULL;
  uint64_t handle_u = 0;
  uint32_t domain_u = 0;
  size_t i = 0;
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
    return RMW_RET_INVALID_ARGUMENT;
  }
  if (pd->domain > (size_t)UINT32_MAX) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_gid_for_publisher domain range");
    return RMW_RET_ERROR;
  }
  domain_u = (uint32_t)pd->domain;
  handle_u = (uint64_t)pd->handle;
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

rmw_ret_t rmw_publisher_count_matched_subscriptions(
  const rmw_publisher_t * publisher, size_t * subscription_count)
{
  // Bridge: Publisher.CountMatchedSubscriptions counts live subscriptions
  // in the same per-domain pubs plus subs registry on the same topic.
  // Closed subscriptions are excluded. No Go mapping exists yet.
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
      return weld_fail_unsupported(
        "rmw_weld: rmw_publisher_count_matched_subscriptions FFI not linked");
    }
    if (weld_ffi_pub_matched_subs(handle, &n) != 0) {
      return weld_fail_error(
        "rmw_weld: rmw_publisher_count_matched_subscriptions backend failed");
    }
    *subscription_count = (size_t)n;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_count_matched_publishers(
  const rmw_subscription_t * subscription, size_t * publisher_count)
{
  // Bridge: Subscription.CountMatchedPublishers counts live publishers in
  // the same per-domain registry on the same topic. Closed publishers are
  // excluded. No Go mapping exists yet.
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
      return weld_fail_unsupported(
        "rmw_weld: rmw_subscription_count_matched_publishers FFI not linked");
    }
    if (weld_ffi_sub_matched_pubs(handle, &n) != 0) {
      return weld_fail_error(
        "rmw_weld: rmw_subscription_count_matched_publishers backend failed");
    }
    *publisher_count = (size_t)n;
  }
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
    return RMW_RET_INVALID_ARGUMENT;
  }
  // Same 16 byte layout as rmw_get_gid_for_publisher: domain from the
  // creating node in the first 4 bytes little endian and the FFI
  // service handle in the next 8, unique within one domain.
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

// Local endpoint registry for info-by-topic plus info-by-service queries.
//
// Creation records the owning node plus domain with the live endpoint data
// pointer; destruction unregisters before freeing. Queries snapshot under
// the mutex and read only the snapshot, so destruction can never race a
// fill. Kinds are WELD_EP_PUB through WELD_EP_CLI declared above.

struct weld_ep_entry
{
  int kind;
  char * node_name;
  char * node_ns;
  size_t domain;
  const void * data;
  struct weld_ep_entry * next;
};

static pthread_mutex_t g_weld_ep_mutex = PTHREAD_MUTEX_INITIALIZER;

static struct weld_ep_entry * g_weld_ep_list = NULL;

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
};

static void weld_ep_snapshot_fini(struct weld_ep_snap * items, size_t n)
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

// Snapshot local endpoints of one kind on one name.
//
// The snapshot copies every field the fill needs, so later destruction
// cannot invalidate it. Returns RMW_RET_OK with a malloc owned array
// (possibly empty) or RMW_RET_BAD_ALLOC with nothing stored.
static rmw_ret_t weld_ep_snapshot(
  const char * name, int kind, size_t domain, bool use_domain,
  struct weld_ep_snap ** out_items, size_t * out_n)
{
  struct weld_ep_snap * items = NULL;
  size_t n = 0;
  size_t cap = 0;
  if (name == NULL || out_items == NULL || out_n == NULL) {
    return weld_fail_invalid("rmw_weld: ep_snapshot null argument");
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
      if (pd->topic == NULL || strcmp(pd->topic, name) != 0) {
        continue;
      }
      ep_name = pd->topic;
      item.handle = pd->handle;
      item.qos = pd->qos;
      item.type_support = pd->type_support;
    } else if (kind == WELD_EP_SUB) {
      const struct weld_sub_data * sd =
        (const struct weld_sub_data *)e->data;
      if (sd->topic == NULL || strcmp(sd->topic, name) != 0) {
        continue;
      }
      ep_name = sd->topic;
      item.handle = sd->handle;
      item.qos = sd->qos;
      item.type_support = sd->type_support;
    } else {
      const struct weld_rpc_state * st =
        (const struct weld_rpc_state *)e->data;
      if (st->name == NULL || strcmp(st->name, name) != 0) {
        continue;
      }
      ep_name = st->name;
      item.handle = st->ffi_handle;
      item.qos = st->qos;
      item.type_support = NULL;
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

// Set one endpoint info entry from plain fields.
//
// Strings copy through the rmw setters with the caller allocator. Gid
// must hold RMW_GID_STORAGE_SIZE bytes. Returns the setter result.
static rmw_ret_t weld_info_set_one(
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
// the first 4 bytes and the FFI handle in the next 8, zero padded. It is
// a synthetic identity unique within one domain, not a DDS GUID.
static void weld_info_gid(uint8_t gid[16], size_t domain, long long handle)
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

// Fill an info array from local registry endpoints plus remote peers.
//
// Locals carry their stored actual QoS, type, and synthetic GID.
// Remotes carry their SEDP GUID, announced type, and the first local
// QoS on the same name (or qos_default when no local exists), because
// stored offer bytes do not decode to rmw_qos_profile_t. Remote node
// names are unavailable from SEDP and report empty with namespace "/".
// Unknown names still return OK with an empty array per spec. Torn
// remote entries from discovery churn skip quietly.
static rmw_ret_t weld_info_fill(
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
    const rmw_qos_profile_t * qos = &rmw_qos_profile_default;
    int frc = 0;
    if (n_local > 0) {
      qos = &snaps[0].qos;
    }
    frc = weld_ffi_remote_ep_at(
      bridge, name, remote_role, (long long)i, typebuf,
      (int)sizeof(typebuf), (char *)gid, (int)sizeof(gid));
    if (frc != 0) {
      continue;
    }
    info = &out->info_array[w];
    rc = weld_info_set_one(
      info, "", "/", typebuf, gid, qos, ep_type, allocator);
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

// One collected flow address with its family for the fill path.
struct weld_flow_addr
{
  bool is_v6;
  char text[RMW_INET_ADDRSTRLEN];
};

// Append one address unless already listed. Returns true when stored.
static bool weld_flow_add(
  struct weld_flow_addr * out, size_t * n, size_t cap, bool is_v6,
  const char * text)
{
  size_t i = 0;
  if (out == NULL || n == NULL || text == NULL || text[0] == '\0') {
    return false;
  }
  for (i = 0; i < *n; i++) {
    if (out[i].is_v6 == is_v6 && strcmp(out[i].text, text) == 0) {
      return false;
    }
  }
  if (*n >= cap) {
    return false;
  }
  out[*n].is_v6 = is_v6;
  (void)snprintf(out[*n].text, sizeof(out[*n].text), "%s", text);
  (*n)++;
  return true;
}

// Collect local interface addresses for the network flow queries.
//
// Up interfaces come first through getifaddrs. When that yields nothing,
// /proc/net/fib_trie host LOCAL lines back it up, and loopback is the
// last resort so the query never reports an empty host. Returns the
// stored count.
static size_t weld_flow_collect(struct weld_flow_addr * out, size_t cap)
{
  size_t n = 0;
  struct ifaddrs * head = NULL;
  if (out == NULL || cap == 0) {
    return 0;
  }
  if (getifaddrs(&head) == 0) {
    struct ifaddrs * ifa = NULL;
    for (ifa = head; ifa != NULL; ifa = ifa->ifa_next) {
      char buf[INET6_ADDRSTRLEN];
      if (ifa->ifa_addr == NULL) {
        continue;
      }
      if ((ifa->ifa_flags & (unsigned int)IFF_UP) == 0) {
        continue;
      }
      if (ifa->ifa_addr->sa_family == AF_INET) {
        const struct sockaddr_in * a =
          (const struct sockaddr_in *)(const void *)ifa->ifa_addr;
        if (inet_ntop(AF_INET, &a->sin_addr, buf, sizeof(buf)) == NULL) {
          continue;
        }
        (void)weld_flow_add(out, &n, cap, false, buf);
      } else if (ifa->ifa_addr->sa_family == AF_INET6) {
        const struct sockaddr_in6 * a6 =
          (const struct sockaddr_in6 *)(const void *)ifa->ifa_addr;
        if (inet_ntop(AF_INET6, &a6->sin6_addr, buf, sizeof(buf)) == NULL) {
          continue;
        }
        (void)weld_flow_add(out, &n, cap, true, buf);
      }
    }
    freeifaddrs(head);
  }
  if (n == 0) {
    FILE * f = fopen("/proc/net/fib_trie", "r");
    if (f != NULL) {
      char line[256];
      char leaf[64];
      leaf[0] = '\0';
      while (fgets(line, sizeof(line), f) != NULL) {
        const char * mark = strstr(line, "|--");
        if (mark != NULL) {
          size_t k = 0;
          mark += 3;
          while (*mark == ' ' || *mark == '\t') {
            mark++;
          }
          while (k + 1 < sizeof(leaf) && mark[k] != '\0' &&
            mark[k] != '\n' && mark[k] != ' ' && mark[k] != '\t' &&
            mark[k] != '/')
          {
            leaf[k] = mark[k];
            k++;
          }
          leaf[k] = '\0';
        } else if (
          strstr(line, "host LOCAL") != NULL && leaf[0] != '\0')
        {
          unsigned a = 0;
          unsigned b = 0;
          unsigned c = 0;
          unsigned d = 0;
          char extra = '\0';
          if (sscanf(
              leaf, "%u.%u.%u.%u%c", &a, &b, &c, &d,
              &extra) == 4 && a < 256 && b < 256 && c < 256 && d < 256)
          {
            (void)weld_flow_add(out, &n, cap, false, leaf);
          }
          leaf[0] = '\0';
        }
      }
      (void)fclose(f);
    }
  }
  if (n == 0) {
    (void)weld_flow_add(out, &n, cap, false, "127.0.0.1");
  }
  return n;
}

// Fill a network flow array from local interface addresses.
//
// The transport reports UDP with unknown ports: the shim owns no socket
// per endpoint, so port, flow label, and DSCP stay zero while the
// internet address is real.
static rmw_ret_t weld_netflow_fill(
  rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * out, const char * what)
{
  struct weld_flow_addr addrs[64];
  size_t n = 0;
  size_t i = 0;
  rmw_ret_t rc = RMW_RET_OK;
  (void)what;
  n = weld_flow_collect(addrs, sizeof(addrs) / sizeof(addrs[0]));
  rc = rmw_network_flow_endpoint_array_init(out, n, allocator);
  if (rc != RMW_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: netflow init failed");
    return RMW_RET_BAD_ALLOC;
  }
  for (i = 0; i < n; i++) {
    rmw_network_flow_endpoint_t * e = &out->network_flow_endpoint[i];
    e->transport_protocol = RMW_TRANSPORT_PROTOCOL_UDP;
    if (addrs[i].is_v6) {
      e->internet_protocol = RMW_INTERNET_PROTOCOL_IPV6;
    } else {
      e->internet_protocol = RMW_INTERNET_PROTOCOL_IPV4;
    }
    e->transport_port = 0;
    e->flow_label = 0;
    e->dscp = 0;
    (void)snprintf(
      e->internet_address, sizeof(e->internet_address), "%s",
      addrs[i].text);
  }
  return RMW_RET_OK;
}

// Graph extended plus loan plus event plus content filter plus network
// flow plus feature query.
//
// Graph snapshots merge the local node registry with the Go bridge
// registry. Per-node endpoint maps plus per-endpoint QoS plus GID are
// not tracked by this shim, so by-node queries return an empty
// names_and_types with RMW_RET_OK when the node exists and
// RMW_RET_NODE_NAME_NON_EXISTENT otherwise, and info-by-topic queries
// return an empty array with RMW_RET_OK. The empty snapshot documents
// the unknown choice: the transport copies bytes and never reports a
// best-known QoS, so callers must not infer absence of peers from size.
// Loans are owned heap buffers, never zero copy: see the loan family
// below. Only MESSAGE_LOST events are backed, content filters attach
// through the Go backend with the expression stored per subscription,
// and network flow endpoints report local interface addresses.

// Report whether a node name plus namespace is known locally or via FFI.
static bool weld_node_known(const char * name, const char * ns)
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

// Fill an empty names_and_types snapshot for a known node.
static rmw_ret_t weld_by_node_empty(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace,
  rmw_names_and_types_t * out, const char * what)
{
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
  rc = rmw_names_and_types_init(out, 0, allocator);
  if (rc != RMW_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(what);
    return RMW_RET_BAD_ALLOC;
  }
  return RMW_RET_OK;
#else
  (void)rc;
  (void)what;
  (void)allocator;
  (void)out;
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
  return weld_by_node_empty(
    node, allocator, node_name, node_namespace, topic_names_and_types,
    "rmw_weld: rmw_get_publisher_names_and_types_by_node unknown node");
}

rmw_ret_t rmw_get_subscriber_names_and_types_by_node(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace, bool no_demangle,
  rmw_names_and_types_t * topic_names_and_types)
{
  (void)no_demangle;
  return weld_by_node_empty(
    node, allocator, node_name, node_namespace, topic_names_and_types,
    "rmw_weld: rmw_get_subscriber_names_and_types_by_node unknown node");
}

rmw_ret_t rmw_get_service_names_and_types_by_node(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace,
  rmw_names_and_types_t * service_names_and_types)
{
  return weld_by_node_empty(
    node, allocator, node_name, node_namespace, service_names_and_types,
    "rmw_weld: rmw_get_service_names_and_types_by_node unknown node");
}

rmw_ret_t rmw_get_client_names_and_types_by_node(
  const rmw_node_t * node, rcutils_allocator_t * allocator,
  const char * node_name, const char * node_namespace,
  rmw_names_and_types_t * service_names_and_types)
{
  return weld_by_node_empty(
    node, allocator, node_name, node_namespace, service_names_and_types,
    "rmw_weld: rmw_get_client_names_and_types_by_node unknown node");
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
// report RMW_ENDPOINT_SUBSCRIPTION. Service IDL types are not tracked,
// so local entries report an empty type.
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

// Network flow endpoints report local interface addresses from this
// process. The shim owns no socket per endpoint, so the transport is
// UDP with unknown ports: port, flow label, and DSCP stay zero while
// the internet address is real.

rmw_ret_t rmw_publisher_get_network_flow_endpoints(
  const rmw_publisher_t * publisher, rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * network_flow_endpoint_array)
{
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
  return weld_netflow_fill(
    allocator, network_flow_endpoint_array,
    "rmw_weld: rmw_publisher_get_network_flow_endpoints");
}

rmw_ret_t rmw_subscription_get_network_flow_endpoints(
  const rmw_subscription_t * subscription, rcutils_allocator_t * allocator,
  rmw_network_flow_endpoint_array_t * network_flow_endpoint_array)
{
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
  return weld_netflow_fill(
    allocator, network_flow_endpoint_array,
    "rmw_weld: rmw_subscription_get_network_flow_endpoints");
}
