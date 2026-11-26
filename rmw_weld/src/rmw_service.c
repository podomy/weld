// SPDX-License-Identifier: Apache-2.0
// rmw_service.c: services, clients, RPC, plus dispatch.
#include "rmw_common.h"

// Forward declaration: the destroy paths
// stop the dispatch thread defined below.
static void weld_rpc_dispatch_stop(
  struct weld_rpc_state * st);

// Write one sequence id as 8 little endian bytes.
static void weld_seq_write(int64_t v, char out[8])
{
  uint64_t u = (uint64_t)v;
  for (int i = 0; i < 8; i++) {
    out[i] = (char)((u >> (unsigned)(i * 8)) & 0xFFu);
  }
}

// Read one sequence id from 8 little endian bytes.
static int64_t weld_seq_read(const char in[8])
{
  uint64_t u = 0;
  for (int i = 0; i < 8; i++) {
    u |= ((uint64_t)(unsigned char)in[i]) << (unsigned)(i * 8);
  }
  return (int64_t)u;
}

// Drop one sequence id from the pending set. The caller holds the
// endpoint mutex. Missing ids are ignored so double removal is safe.
// The echo flag and the leg flag parallel the id and drop with it.
static void weld_rpc_remove_locked(struct weld_rpc_state * st, int64_t seq)
{
  for (size_t i = 0; i < st->n_pending; i++) {
    if (st->pending[i] == seq) {
      size_t tail = st->n_pending - i - 1;
      if (tail > 0) {
        (void)memmove(
          &st->pending[i], &st->pending[i + 1],
          tail * sizeof(st->pending[0]));
        (void)memmove(
          &st->pending_is_echo[i], &st->pending_is_echo[i + 1],
          tail * sizeof(st->pending_is_echo[0]));
        (void)memmove(
          &st->pending_local[i], &st->pending_local[i + 1],
          tail * sizeof(st->pending_local[0]));
      }
      st->n_pending--;
      return;
    }
  }
}

// Drop every pending id. The caller holds no locks; the endpoint is
// already unreachable by the time destroy runs.
static void weld_rpc_free_stash(struct weld_rpc_state * st)
{
  st->n_pending = 0;
}

// Resolve the request or response introspection members for a service.
//
// want_request selects the request members and false selects the
// response members. Returns NULL when the type support misses
// introspection or the side is absent.
static const rosidl_typesupport_introspection_c__MessageMembers *
weld_service_side_members(
  const rosidl_service_type_support_t * type_support, bool want_request)
{
  const rosidl_service_type_support_t * h = NULL;
  const rosidl_typesupport_introspection_c__ServiceMembers * svc = NULL;
  if (type_support == NULL) {
    return NULL;
  }
  h = get_service_typesupport_handle(
    type_support, rosidl_typesupport_introspection_c__identifier);
  if (h == NULL || h->data == NULL) {
    return NULL;
  }
  svc = (const rosidl_typesupport_introspection_c__ServiceMembers *)h->data;
  if (svc == NULL) {
    return NULL;
  }
  if (want_request) {
    return svc->request_members_;
  }
  return svc->response_members_;
}

// Build one deterministic 8 byte service guid for a pending sequence.
//
// The guid mixes the endpoint address, the process id, the sequence
// id, and a per-endpoint salt assigned once at create time, so wait
// peeks and take calls recompute the same value while two clients in
// one process and two processes never share a header. The salt defeats
// address reuse: malloc may hand a freed endpoint address to a later
// endpoint with the sequence restarted at 1, so address plus pid plus
// seq alone could recompute a stale in-flight guid. The salt never
// repeats within a process, so a new endpoint never aliases a
// destroyed one.
static _Atomic uint64_t g_weld_guid_salt = 1;

void weld_svc_guid(
  struct weld_rpc_state * st, int64_t seq, uint8_t guid[8])
{
  uint64_t h = (uint64_t)(uintptr_t)(const void *)st;
  uint64_t pid = (uint64_t)getpid();
  h ^= pid + 0x9E3779B97F4A7C15ULL + (h << 6) + (h >> 2);
  h ^= (uint64_t)seq * 11400714819323198549ULL + (h << 6) + (h >> 2);
  h ^= st->guid_salt * 14029467366897019727ULL + (h << 6) + (h >> 2);
  for (int i = 0; i < 8; i++) {
    guid[i] = (uint8_t)((h >> (unsigned)(i * 8)) & 0xFFu);
  }
}

// Report the IDL service base for RTPS declares.
//
// The base joins the service namespace and name, e.g.
// std_srvs__srv__Trigger. Returns 0 on success and -1 when the type
// support misses introspection.
int weld_service_idl_base(
  const rosidl_service_type_support_t * type_support,
  char * buf,
  size_t buflen)
{
  if (weld_cdr_service_type_name(type_support, buf, buflen) != 0) {
    return -1;
  }
  return 0;
}

// Read the stored FFI handle under the endpoint mutex.
long long weld_rpc_handle(struct weld_rpc_state * st)
{
  long long handle = 0;
  (void)pthread_mutex_lock(&st->mutex);
  handle = st->ffi_handle;
  (void)pthread_mutex_unlock(&st->mutex);
  return handle;
}

// Borrow the process bridge created by rmw_init.
//
// Service and client handles share the process bridge (see
// g_weld_bridge); a zero handle means no context is initialized and
// creation fails loudly.
rmw_ret_t weld_ffi_bridge_borrow(long long * out)
{
  if (out == NULL) {
    return weld_fail_invalid("rmw_weld: bridge null argument");
  }
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  long long bridge = g_weld_bridge;
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  if (bridge == 0) {
    return weld_fail_error("rmw_weld: bridge not initialized");
  }
  *out = bridge;
  return RMW_RET_OK;
}

// Shared FFI service handles keyed by service name.
//
// Servers and clients for one name share a single FFI service handle
// so a client call_nb rendezvous with a server serve_nb through the
// same Go Service requests channel. The entry lives while at least one
// server or client references it; the last release unregisters the FFI
// handle. g_weld_svc_mutex guards the list and the ref counts. Weld
// FFI calls that can block run without holding it.
struct weld_svc_shared
{
  char * name;
  long long ffi_handle;
  size_t refs;
  size_t servers;
  struct weld_svc_shared * next;
};

static pthread_mutex_t g_weld_svc_mutex = PTHREAD_MUTEX_INITIALIZER;

static struct weld_svc_shared * g_weld_svc_shared = NULL;

// Take a reference on the shared FFI handle for name, creating it on
// first use. The caller passes a validated non-empty name.
static rmw_ret_t weld_svc_acquire(
  long long bridge, const char * name, long long * out)
{
  if (name == NULL || out == NULL) {
    return weld_fail_invalid("rmw_weld: service registry null argument");
  }
  if (bridge == 0) {
    return weld_fail_invalid("rmw_weld: service registry null bridge");
  }
  if (weld_ffi_service_create == NULL) {
    return weld_fail_unsupported("rmw_weld: service FFI not linked yet");
  }
  (void)pthread_mutex_lock(&g_weld_svc_mutex);
  for (struct weld_svc_shared * e = g_weld_svc_shared; e != NULL;
    e = e->next)
  {
    if (strcmp(e->name, name) == 0) {
      e->refs++;
      *out = e->ffi_handle;
      (void)pthread_mutex_unlock(&g_weld_svc_mutex);
      return RMW_RET_OK;
    }
  }
  long long fresh = 0;
  // Registry insert on the Go side never blocks, so creation stays
  // under the mutex and check plus insert are atomic.
  int rc = weld_ffi_service_create(bridge, name, &fresh);
  if (rc != 0 || fresh == 0) {
    (void)pthread_mutex_unlock(&g_weld_svc_mutex);
    return weld_fail_error("rmw_weld: service create failed");
  }
  struct weld_svc_shared * e =
    (struct weld_svc_shared *)malloc(sizeof(*e));
  if (e == NULL) {
    if (weld_ffi_service_destroy != NULL) {
      (void)weld_ffi_service_destroy(fresh);
    }
    (void)pthread_mutex_unlock(&g_weld_svc_mutex);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: service registry out of memory");
    return RMW_RET_BAD_ALLOC;
  }
  e->name = weld_dup_str(name);
  if (e->name == NULL) {
    free(e);
    if (weld_ffi_service_destroy != NULL) {
      (void)weld_ffi_service_destroy(fresh);
    }
    (void)pthread_mutex_unlock(&g_weld_svc_mutex);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: service registry out of memory");
    return RMW_RET_BAD_ALLOC;
  }
  e->ffi_handle = fresh;
  e->refs = 1;
  e->servers = 0;
  e->next = g_weld_svc_shared;
  g_weld_svc_shared = e;
  *out = fresh;
  (void)pthread_mutex_unlock(&g_weld_svc_mutex);
  return RMW_RET_OK;
}

// Drop one reference for name, unregistering the FFI handle on the
// last release. Unknown names fail loudly.
static rmw_ret_t weld_svc_release(const char * name)
{
  if (name == NULL) {
    return weld_fail_invalid("rmw_weld: service registry null argument");
  }
  (void)pthread_mutex_lock(&g_weld_svc_mutex);
  struct weld_svc_shared ** link = &g_weld_svc_shared;
  while (*link != NULL) {
    if (strcmp((*link)->name, name) == 0) {
      struct weld_svc_shared * dead = *link;
      if (dead->refs > 0) {
        dead->refs--;
      }
      long long doomed = 0;
      if (dead->refs == 0) {
        *link = dead->next;
        doomed = dead->ffi_handle;
        free(dead->name);
        free(dead);
      }
      (void)pthread_mutex_unlock(&g_weld_svc_mutex);
      if (doomed != 0 && weld_ffi_service_destroy != NULL) {
        int rc = weld_ffi_service_destroy(doomed);
        if (rc != 0) {
          return weld_fail_error("rmw_weld: service destroy failed");
        }
      }
      return RMW_RET_OK;
    }
    link = &(*link)->next;
  }
  (void)pthread_mutex_unlock(&g_weld_svc_mutex);
  return weld_fail_invalid("rmw_weld: service handle unknown");
}

// Report whether this process hosts a server for the service.
//
// The shared registry counts servers separately from clients, so a lone
// client never mistakes its own shared handle for a server. No FFI call
// is needed: the registry is process-local state. An unknown name keeps
// the historical true default.
static bool weld_svc_has_local_server(const struct weld_rpc_state * st)
{
  bool local = true;
  if (st == NULL) {
    return false;
  }
  if (st->name == NULL) {
    return true;
  }
  (void)pthread_mutex_lock(&g_weld_svc_mutex);
  for (struct weld_svc_shared * e = g_weld_svc_shared; e != NULL;
    e = e->next)
  {
    if (strcmp(e->name, st->name) == 0) {
      local = e->servers > 0;
      break;
    }
  }
  (void)pthread_mutex_unlock(&g_weld_svc_mutex);
  return local;
}

rmw_service_t * rmw_create_service(
  const rmw_node_t * node, const rosidl_service_type_support_t * type_support,
  const char * service_name, const rmw_qos_profile_t * qos_profile)
{
  // Bridge: servers and clients for one name share a single FFI service
  // handle from the process bridge (see weld_svc_acquire). The type
  // support drives CDR encode plus decode and the IDL base reported to
  // the FFI service set_type for RTPS endpoint declares; the QoS is
  // stored but otherwise unused. The standalone build has no FFI
  // symbols, so creation fails loudly there instead of returning a
  // dead handle.
  if (node == NULL || type_support == NULL || service_name == NULL ||
    qos_profile == NULL)
  {
    (void)weld_fail_invalid("rmw_weld: rmw_create_service null argument");
    return NULL;
  }
  if (!weld_node_ok(node)) {
    (void)weld_fail_impl("rmw_weld: rmw_create_service wrong implementation");
    return NULL;
  }
  if (service_name[0] == '\0') {
    (void)weld_fail_invalid("rmw_weld: rmw_create_service empty service name");
    return NULL;
  }
  if (weld_service_side_members(type_support, true) == NULL ||
    weld_service_side_members(type_support, false) == NULL)
  {
    (void)weld_fail_invalid(
      "rmw_weld: rmw_create_service type support is not introspection");
    return NULL;
  }
  long long bridge = 0;
  if (weld_ffi_bridge_borrow(&bridge) != RMW_RET_OK) {
    return NULL;
  }
  struct weld_rpc_state * st =
    (struct weld_rpc_state *)calloc(1, sizeof(*st));
  if (st == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_service out of memory");
    return NULL;
  }
  if (pthread_mutex_init(&st->mutex, NULL) != 0) {
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_service no mutex");
    return NULL;
  }
  st->name = weld_dup_str(service_name);
  if (st->name == NULL) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_service out of memory");
    return NULL;
  }
  st->next_seq = 1;
  st->guid_salt =
    (uint64_t)atomic_fetch_add(&g_weld_guid_salt, 1);
  st->qos = *qos_profile;
  st->type_support = type_support;
  st->is_service = true;
  st->domain = node->context != NULL ? node->context->actual_domain_id : 0;
  rmw_service_t * svc = (rmw_service_t *)malloc(sizeof(*svc));
  if (svc == NULL) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st->name);
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_service out of memory");
    return NULL;
  }
  if (weld_svc_acquire(bridge, service_name, &st->ffi_handle) != RMW_RET_OK) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st->name);
    free(st);
    free(svc);
    return NULL;
  }
  // Record the local server so rmw_service_server_is_available answers
  // from local state plus remote SEDP peers. Clients share the entry
  // without touching this count. The IDL base is reported best effort
  // for RTPS endpoint declares; a missing symbol keeps in-memory only.
  if (weld_ffi_service_set_type != NULL) {
    char base[512];
    if (weld_service_idl_base(type_support, base, sizeof(base)) == 0) {
      (void)weld_ffi_service_set_type(st->ffi_handle, base, "");
    }
  }
  (void)pthread_mutex_lock(&g_weld_svc_mutex);
  for (struct weld_svc_shared * e = g_weld_svc_shared; e != NULL;
    e = e->next)
  {
    if (strcmp(e->name, service_name) == 0) {
      e->servers++;
      break;
    }
  }
  (void)pthread_mutex_unlock(&g_weld_svc_mutex);
  svc->implementation_identifier = kImplementationIdentifier;
  svc->data = st;
  // service_name aliases the endpoint name; destroy frees it once
  // through st->name.
  svc->service_name = st->name;
  // Record the endpoint for info-by-service queries. Best effort: a
  // missed record only shrinks info snapshots.
  weld_ep_register(
    WELD_EP_SVC, node->name, node->namespace_,
    node->context != NULL ? node->context->actual_domain_id : 0, st);
  return svc;
}

rmw_ret_t rmw_destroy_service(rmw_node_t * node, rmw_service_t * service)
{
  if (node == NULL || service == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_service null argument");
  }
  if (!weld_node_ok(node) || !weld_svc_ok(service)) {
    return weld_fail_impl("rmw_weld: rmw_destroy_service wrong implementation");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)service->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_service no data");
  }
  weld_ep_unregister(st);
  weld_rpc_dispatch_stop(st);
  // Release the shared FFI handle before touching local state: on
  // failure nothing is freed, so the handle stays valid and the
  // caller may retry the destroy instead of using freed memory.
  rmw_ret_t rc = RMW_RET_OK;
  if (st->name != NULL) {
    rc = weld_svc_release(st->name);
  } else {
    rc = weld_fail_invalid("rmw_weld: rmw_destroy_service no name");
  }
  if (rc != RMW_RET_OK) {
    return rc;
  }
  weld_rpc_free_stash(st);
  if (st->name != NULL) {
    (void)pthread_mutex_lock(&g_weld_svc_mutex);
    for (struct weld_svc_shared * e = g_weld_svc_shared; e != NULL;
      e = e->next)
    {
      if (strcmp(e->name, st->name) == 0) {
        if (e->servers > 0) {
          e->servers--;
        }
        break;
      }
    }
    (void)pthread_mutex_unlock(&g_weld_svc_mutex);
  }
  (void)pthread_mutex_destroy(&st->mutex);
  free(st->name);
  st->name = NULL;
  free(st);
  service->data = NULL;
  service->service_name = NULL;
  free(service);
  return rc;
}

rmw_ret_t rmw_take_request(
  const rmw_service_t * service, weld_service_info_t * request_header,
  void * ros_request, bool * taken)
{
  // Bridge: a pending weld_ffi_service_serve_nb dequeues one Go Service
  // request from the in-memory rendezvous or the RTPS request topic and
  // reports its sequence id. CDR payloads decode through the
  // introspection request members; a leftover 8 byte sequence echo from
  // the old in-memory path keeps working by mapping only the header
  // while ros_request stays untouched.
  if (service == NULL || request_header == NULL || ros_request == NULL ||
    taken == NULL)
  {
    return weld_fail_invalid("rmw_weld: rmw_take_request null argument");
  }
  if (!weld_svc_ok(service)) {
    return weld_fail_impl("rmw_weld: rmw_take_request wrong implementation");
  }
  // Default to not taken: every error return below then leaves a
  // defined value instead of untouched caller memory.
  *taken = false;
  if (weld_ffi_service_serve_nb == NULL) {
    return weld_fail_unsupported("rmw_weld: rmw_take_request serve not linked yet");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)service->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_request no data");
  }
  const rosidl_typesupport_introspection_c__MessageMembers * req_members =
    weld_service_side_members(st->type_support, true);
  if (req_members == NULL) {
    return weld_fail_error(
      "rmw_weld: rmw_take_request type support is not introspection");
  }
  long long handle = weld_rpc_handle(st);
  size_t cap = 64 * 1024;
  const size_t cap_max = (size_t)(32 << 20);
  uint8_t * buf = (uint8_t *)malloc(cap > 0 ? cap : 1);
  if (buf == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_request out of memory");
    return RMW_RET_BAD_ALLOC;
  }
  int out_n = 0;
  long long seq = 0;
  for (;;) {
    int rc = 0;
    if (cap > (size_t)INT_MAX) {
      cap = (size_t)INT_MAX;
    }
    rc = weld_ffi_service_serve_nb(
      handle, (char *)buf, (int)cap, &out_n, &seq);
    if (rc == 1) {
      free(buf);
      *taken = false;
      return RMW_RET_OK;
    }
    if (rc == -4) {
      uint8_t * next = NULL;
      if (cap >= cap_max) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_request payload too large");
        return RMW_RET_ERROR;
      }
      cap *= 2;
      if (cap > cap_max) {
        cap = cap_max;
      }
      next = (uint8_t *)realloc(buf, cap);
      if (next == NULL) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_request out of memory");
        return RMW_RET_BAD_ALLOC;
      }
      buf = next;
      continue;
    }
    if (rc != 0) {
      free(buf);
      return weld_fail_error("rmw_weld: rmw_take_request serve failed");
    }
    break;
  }
  if (out_n < 0) {
    free(buf);
    return weld_fail_error("rmw_weld: rmw_take_request bad length");
  }
  char errbuf[256];
  errbuf[0] = '\0';
  weld_cdr_status_t dec = weld_cdr_decode_members(
    req_members, buf, (size_t)out_n, ros_request, errbuf, sizeof(errbuf));
  if (dec == WELD_CDR_OK) {
    (void)pthread_mutex_lock(&st->mutex);
    if (st->n_pending >= WELD_RPC_PENDING_MAX) {
      (void)pthread_mutex_unlock(&st->mutex);
      free(buf);
      return weld_fail_error("rmw_weld: rmw_take_request too many pending");
    }
    st->pending[st->n_pending] = (int64_t)seq;
    st->pending_is_echo[st->n_pending] = false;
    st->pending_local[st->n_pending] = true;
    st->n_pending++;
    (void)pthread_mutex_unlock(&st->mutex);
    request_header->source_timestamp = 0;
    request_header->received_timestamp = 0;
    (void)memset(
      request_header->request_id.writer_guid, 0,
      sizeof(request_header->request_id.writer_guid));
    request_header->request_id.sequence_number = (int64_t)seq;
    *taken = true;
    free(buf);
    return RMW_RET_OK;
  }
  // Old in-memory echo: exactly 8 bytes with no CDR header. Only the
  // header sequence travels; ros_request stays untouched.
  if (out_n == 8 && buf[0] != 0x00) {
    (void)pthread_mutex_lock(&st->mutex);
    if (st->n_pending >= WELD_RPC_PENDING_MAX) {
      (void)pthread_mutex_unlock(&st->mutex);
      free(buf);
      return weld_fail_error("rmw_weld: rmw_take_request too many pending");
    }
    st->pending[st->n_pending] = (int64_t)seq;
    st->pending_is_echo[st->n_pending] = true;
    st->pending_local[st->n_pending] = true;
    st->n_pending++;
    (void)pthread_mutex_unlock(&st->mutex);
    request_header->source_timestamp = 0;
    request_header->received_timestamp = 0;
    (void)memset(
      request_header->request_id.writer_guid, 0,
      sizeof(request_header->request_id.writer_guid));
    request_header->request_id.sequence_number = (int64_t)seq;
    *taken = true;
    free(buf);
    return RMW_RET_OK;
  }
  free(buf);
  rmw_reset_error();
  if (errbuf[0] != '\0') {
    RCUTILS_SET_ERROR_MSG(errbuf);
  } else {
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_request decode failed");
  }
  return RMW_RET_ERROR;
}

// rmw_send_response keeps rmw_request_id_t on Humble, Jazzy, and Rolling;
// only take_request and take_response moved to rmw_service_info_t.
rmw_ret_t rmw_send_response(
  const rmw_service_t * service, rmw_request_id_t * request_header, void * ros_response)
{
  // Bridge: the sequence recorded by rmw_take_request selects the Go
  // responder, which is the in-memory channel or the RTPS reply publish
  // depending on which leg served the request. CDR takes encode the
  // response through the introspection response members; echo takes
  // send the 8 byte sequence echo.
  if (service == NULL || request_header == NULL || ros_response == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_send_response null argument");
  }
  if (!weld_svc_ok(service)) {
    return weld_fail_impl("rmw_weld: rmw_send_response wrong implementation");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)service->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_send_response no data");
  }
  if (weld_ffi_service_respond == NULL) {
    return weld_fail_unsupported("rmw_weld: rmw_send_response respond not linked yet");
  }
  int64_t seq = request_header->sequence_number;
  bool want_echo = false;
  long long handle = 0;
  // Peek the echo flag first: encoding needs no shared state, so it
  // runs without the mutex. The answer below re-verifies pending under
  // one lock hold, so a racing second responder finds the id gone and
  // fails instead of answering twice.
  (void)pthread_mutex_lock(&st->mutex);
  {
    bool found = false;
    for (size_t i = 0; i < st->n_pending; i++) {
      if (st->pending[i] == seq) {
        want_echo = st->pending_is_echo[i];
        found = true;
        break;
      }
    }
    handle = st->ffi_handle;
    if (!found) {
      (void)pthread_mutex_unlock(&st->mutex);
      return weld_fail_invalid("rmw_weld: rmw_send_response unknown sequence");
    }
  }
  (void)pthread_mutex_unlock(&st->mutex);
  uint8_t echo[8];
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  if (want_echo) {
    weld_seq_write(seq, (char *)echo);
    wire = echo;
    wire_len = sizeof(echo);
  } else {
    const rosidl_typesupport_introspection_c__MessageMembers * resp_members =
      weld_service_side_members(st->type_support, false);
    if (resp_members == NULL) {
      return weld_fail_error(
        "rmw_weld: rmw_send_response type support is not introspection");
    }
    errbuf[0] = '\0';
    weld_cdr_status_t enc = weld_cdr_encode_members(
      resp_members, ros_response, true, &wire, &wire_len, errbuf,
      sizeof(errbuf));
    if (enc != WELD_CDR_OK) {
      free(wire);
      rmw_reset_error();
      if (errbuf[0] != '\0') {
        RCUTILS_SET_ERROR_MSG(errbuf);
      } else {
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_send_response encode failed");
      }
      return RMW_RET_ERROR;
    }
    if (wire_len > (size_t)INT_MAX) {
      free(wire);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_send_response payload too large");
      return RMW_RET_ERROR;
    }
  }
  // The Go respond never blocks for the in-memory leg; the RTPS leg
  // publishes with its own bound. Lookup plus respond plus removal stay
  // under one endpoint mutex hold so two racing responders cannot
  // answer the same sequence twice: the loser finds the id gone. The
  // FFI call under lock is safe only because the Go respond path
  // never calls back into C; keep it that way.
  (void)pthread_mutex_lock(&st->mutex);
  {
    size_t at = st->n_pending;
    for (size_t i = 0; i < st->n_pending; i++) {
      if (st->pending[i] == seq) {
        at = i;
        break;
      }
    }
    if (at == st->n_pending) {
      (void)pthread_mutex_unlock(&st->mutex);
      if (!want_echo) {
        free(wire);
      }
      return weld_fail_invalid("rmw_weld: rmw_send_response unknown sequence");
    }
    int rc = weld_ffi_service_respond(
      handle, (long long)seq, (const char *)wire, (int)wire_len);
    if (!want_echo) {
      free(wire);
      wire = NULL;
    }
    if (rc != 0) {
      (void)pthread_mutex_unlock(&st->mutex);
      return weld_fail_error("rmw_weld: rmw_send_response respond failed");
    }
    weld_rpc_remove_locked(st, seq);
  }
  (void)pthread_mutex_unlock(&st->mutex);
  return RMW_RET_OK;
}

rmw_client_t * rmw_create_client(
  const rmw_node_t * node, const rosidl_service_type_support_t * type_support,
  const char * service_name, const rmw_qos_profile_t * qos_profile)
{
  // Bridge: clients share the FFI service handle for their service name
  // (see weld_svc_acquire) so a client call_nb rendezvous with the
  // server serve_nb on the same Go Service. The type support drives CDR
  // encode through the introspection request members; the QoS is stored
  // but otherwise unused.
  (void)qos_profile;
  if (node == NULL || type_support == NULL || service_name == NULL ||
    qos_profile == NULL)
  {
    (void)weld_fail_invalid("rmw_weld: rmw_create_client null argument");
    return NULL;
  }
  if (!weld_node_ok(node)) {
    (void)weld_fail_impl("rmw_weld: rmw_create_client wrong implementation");
    return NULL;
  }
  if (service_name[0] == '\0') {
    (void)weld_fail_invalid("rmw_weld: rmw_create_client empty service name");
    return NULL;
  }
  if (weld_service_side_members(type_support, true) == NULL ||
    weld_service_side_members(type_support, false) == NULL)
  {
    (void)weld_fail_invalid(
      "rmw_weld: rmw_create_client type support is not introspection");
    return NULL;
  }
  long long bridge = 0;
  if (weld_ffi_bridge_borrow(&bridge) != RMW_RET_OK) {
    return NULL;
  }
  struct weld_rpc_state * st =
    (struct weld_rpc_state *)calloc(1, sizeof(*st));
  if (st == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_client out of memory");
    return NULL;
  }
  if (pthread_mutex_init(&st->mutex, NULL) != 0) {
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_client no mutex");
    return NULL;
  }
  st->name = weld_dup_str(service_name);
  if (st->name == NULL) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_client out of memory");
    return NULL;
  }
  st->next_seq = 1;
  st->guid_salt =
    (uint64_t)atomic_fetch_add(&g_weld_guid_salt, 1);
  st->qos = *qos_profile;
  st->type_support = type_support;
  st->domain = node->context != NULL ? node->context->actual_domain_id : 0;
  rmw_client_t * cli = (rmw_client_t *)malloc(sizeof(*cli));
  if (cli == NULL) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st->name);
    free(st);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_client out of memory");
    return NULL;
  }
  if (weld_svc_acquire(bridge, service_name, &st->ffi_handle) != RMW_RET_OK) {
    (void)pthread_mutex_destroy(&st->mutex);
    free(st->name);
    free(st);
    free(cli);
    return NULL;
  }
  cli->implementation_identifier = kImplementationIdentifier;
  cli->data = st;
  // service_name aliases the endpoint name; destroy frees it once
  // through st->name.
  cli->service_name = st->name;
  // Record the endpoint for info-by-service queries. Best effort: a
  // missed record only shrinks info snapshots.
  weld_ep_register(
    WELD_EP_CLI, node->name, node->namespace_,
    node->context != NULL ? node->context->actual_domain_id : 0, st);
  return cli;
}

rmw_ret_t rmw_destroy_client(rmw_node_t * node, rmw_client_t * client)
{
  if (node == NULL || client == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_client null argument");
  }
  if (!weld_node_ok(node) || !weld_cli_ok(client)) {
    return weld_fail_impl("rmw_weld: rmw_destroy_client wrong implementation");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)client->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_client no data");
  }
  weld_ep_unregister(st);
  weld_rpc_dispatch_stop(st);
  // Release the shared FFI handle before touching local state: on
  // failure nothing is freed, so the handle stays valid and the
  // caller may retry the destroy instead of using freed memory.
  rmw_ret_t rc = RMW_RET_OK;
  if (st->name != NULL) {
    rc = weld_svc_release(st->name);
  } else {
    rc = weld_fail_invalid("rmw_weld: rmw_destroy_client no name");
  }
  if (rc != RMW_RET_OK) {
    return rc;
  }
  weld_rpc_free_stash(st);
  (void)pthread_mutex_destroy(&st->mutex);
  free(st->name);
  st->name = NULL;
  free(st);
  client->data = NULL;
  client->service_name = NULL;
  free(client);
  return rc;
}

rmw_ret_t rmw_send_request(
  const rmw_client_t * client, const void * ros_request, int64_t * sequence_id)
{
  // Bridge: send encodes the request as CDR, records a fresh sequence id,
  // and delivers immediately: in-memory servers rendezvous through the
  // Go core while remotes receive an RTPS publish. Eager delivery lets
  // a waiting server take the request even when this client never
  // takes; the matching rmw_take_response only waits for the reply.
  if (client == NULL || ros_request == NULL || sequence_id == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_send_request null argument");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl("rmw_weld: rmw_send_request wrong implementation");
  }
  struct weld_rpc_state * st = (struct weld_rpc_state *)client->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_send_request no data");
  }
  if (weld_service_side_members(st->type_support, true) == NULL) {
    return weld_fail_error(
      "rmw_weld: rmw_send_request type support is not introspection");
  }
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  errbuf[0] = '\0';
  weld_cdr_status_t enc = weld_cdr_encode_service_request(
    st->type_support, ros_request, true, &wire, &wire_len, errbuf,
    sizeof(errbuf));
  if (enc != WELD_CDR_OK) {
    free(wire);
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_send_request encode failed");
    }
    return RMW_RET_ERROR;
  }
  (void)pthread_mutex_lock(&st->mutex);
  if (st->n_pending >= WELD_RPC_PENDING_MAX) {
    (void)pthread_mutex_unlock(&st->mutex);
    free(wire);
    return weld_fail_error("rmw_weld: rmw_send_request too many pending");
  }
  if (st->next_seq == INT64_MAX) {
    (void)pthread_mutex_unlock(&st->mutex);
    free(wire);
    return weld_fail_error("rmw_weld: rmw_send_request sequence exhausted");
  }
  int64_t seq = st->next_seq++;
  long long handle = st->ffi_handle;
  // Record the send leg with the id: the take waits on this leg
  // instead of re-deciding, so a server created or destroyed between
  // send and take cannot strand the reply on the other leg.
  bool local = weld_svc_has_local_server(st);
  st->pending[st->n_pending] = seq;
  st->pending_is_echo[st->n_pending] = false;
  st->pending_local[st->n_pending] = local;
  st->n_pending++;
  (void)pthread_mutex_unlock(&st->mutex);
  // Deliver eagerly so a polling server wakes even when this client
  // never takes: the old deferred send deadlocked rcl executors, where
  // the server waits for a request the client only emits from take.
  // Local servers receive through the in-memory rendezvous, remotes
  // through an immediate RTPS publish; delivery runs without the
  // endpoint mutex and failures remove the pending entry loudly. The
  // FFI send copies the bytes, so the wire buffer is freed here on
  // every path and no request stash is kept.
  {
    uint8_t guid[8];
    int rc = 0;
    if (wire_len > (size_t)INT_MAX) {
      (void)pthread_mutex_lock(&st->mutex);
      weld_rpc_remove_locked(st, seq);
      (void)pthread_mutex_unlock(&st->mutex);
      free(wire);
      return weld_fail_error("rmw_weld: rmw_send_request payload too large");
    }
    weld_svc_guid(st, seq, guid);
    (void)pthread_mutex_lock(&st->mutex);
    {
      bool found = false;
      for (size_t i = 0; i < st->n_pending; i++) {
        if (st->pending[i] == seq) {
          local = st->pending_local[i];
          found = true;
          break;
        }
      }
      if (!found) {
        local = weld_svc_has_local_server(st);
      }
    }
    (void)pthread_mutex_unlock(&st->mutex);
    if (local) {
      if (weld_ffi_service_send_nb == NULL) {
        (void)pthread_mutex_lock(&st->mutex);
        weld_rpc_remove_locked(st, seq);
        (void)pthread_mutex_unlock(&st->mutex);
        free(wire);
        return weld_fail_unsupported(
          "rmw_weld: rmw_send_request send not linked yet");
      }
      rc = weld_ffi_service_send_nb(
        handle, (long long)seq, (const char *)guid,
        (const char *)wire, (int)wire_len);
      if (rc != 0) {
        (void)pthread_mutex_lock(&st->mutex);
        weld_rpc_remove_locked(st, seq);
        (void)pthread_mutex_unlock(&st->mutex);
        free(wire);
        return weld_fail_ffi("rmw_weld: rmw_send_request send failed", rc);
      }
    } else {
      char base[512];
      if (weld_ffi_service_send_rtps == NULL) {
        (void)pthread_mutex_lock(&st->mutex);
        weld_rpc_remove_locked(st, seq);
        (void)pthread_mutex_unlock(&st->mutex);
        free(wire);
        return weld_fail_unsupported(
          "rmw_weld: rmw_send_request rtps send not linked yet");
      }
      if (weld_service_idl_base(st->type_support, base, sizeof(base)) != 0) {
        (void)pthread_mutex_lock(&st->mutex);
        weld_rpc_remove_locked(st, seq);
        (void)pthread_mutex_unlock(&st->mutex);
        free(wire);
        return weld_fail_invalid(
          "rmw_weld: rmw_send_request type support has no service name");
      }
      rc = weld_ffi_service_send_rtps(
        handle, (long long)seq, (const char *)guid, base,
        (const char *)wire, (int)wire_len);
      if (rc != 0) {
        (void)pthread_mutex_lock(&st->mutex);
        weld_rpc_remove_locked(st, seq);
        (void)pthread_mutex_unlock(&st->mutex);
        free(wire);
        return weld_fail_ffi(
          "rmw_weld: rmw_send_request rtps send failed", rc);
      }
    }
    free(wire);
  }
  *sequence_id = seq;
  return RMW_RET_OK;
}

// Forward declarations: the take splits into local plus RTPS legs below.
static rmw_ret_t weld_take_response_local(
  struct weld_rpc_state * st, long long handle, int64_t seq,
  const uint8_t guid[8],
  const rosidl_typesupport_introspection_c__MessageMembers * resp_members,
  weld_service_info_t * request_header, void * ros_response, bool * taken);

static rmw_ret_t weld_take_response_rtps(
  struct weld_rpc_state * st, long long handle, int64_t seq,
  const uint8_t guid[8],
  const rosidl_typesupport_introspection_c__MessageMembers * resp_members,
  weld_service_info_t * request_header, void * ros_response, bool * taken);

rmw_ret_t rmw_take_response(
  const rmw_client_t * client, weld_service_info_t * request_header,
  void * ros_response, bool * taken)
{
  // Bridge: the request already left in rmw_send_request, so take only
  // waits for the reply. Local servers answer through the in-memory
  // rendezvous parked by the eager send; remotes answer over RTPS. The
  // reply decodes through the introspection response members; a leftover
  // 8 byte sequence echo from the old in-memory path matches back to the
  // pending id instead. Each leg blocks up to the FFI internal bound;
  // the pending entry is kept so a later take can retry.
  if (client == NULL || request_header == NULL || ros_response == NULL ||
    taken == NULL)
  {
    return weld_fail_invalid("rmw_weld: rmw_take_response null argument");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl("rmw_weld: rmw_take_response wrong implementation");
  }
  // Default to not taken: every error return below then leaves a
  // defined value instead of untouched caller memory.
  *taken = false;
  struct weld_rpc_state * st = (struct weld_rpc_state *)client->data;
  if (st == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_response no data");
  }
  if (weld_ffi_service_poll_reply == NULL ||
    weld_ffi_service_take_reply == NULL)
  {
    return weld_fail_unsupported("rmw_weld: rmw_take_response take not linked yet");
  }
  const rosidl_typesupport_introspection_c__MessageMembers * resp_members =
    weld_service_side_members(st->type_support, false);
  if (resp_members == NULL) {
    return weld_fail_error(
      "rmw_weld: rmw_take_response type support is not introspection");
  }
  (void)pthread_mutex_lock(&st->mutex);
  if (st->n_pending == 0) {
    *taken = false;
    (void)pthread_mutex_unlock(&st->mutex);
    return RMW_RET_OK;
  }
  // Snapshot the pending ids and try each in order: waiting only on
  // the head would stall when a later reply arrives first. Polls are
  // short, and depth is capped, so the scan stays bounded. Each id
  // waits on the leg recorded at send time, never a fresh decision.
  int64_t seqs[WELD_RPC_PENDING_MAX];
  bool legs[WELD_RPC_PENDING_MAX];
  size_t n_seqs = st->n_pending;
  for (size_t i = 0; i < n_seqs; i++) {
    seqs[i] = st->pending[i];
    legs[i] = st->pending_local[i];
  }
  long long handle = st->ffi_handle;
  (void)pthread_mutex_unlock(&st->mutex);
  // Local means a server lived in this process at send time: the
  // shared entry counts servers separately from clients, so a lone
  // client never mistakes its own shared handle for a server.
  for (size_t i = 0; i < n_seqs; i++) {
    uint8_t guid[8];
    rmw_ret_t try_rc = RMW_RET_OK;
    weld_svc_guid(st, seqs[i], guid);
    *taken = false;
    if (legs[i]) {
      try_rc = weld_take_response_local(
        st, handle, seqs[i], guid, resp_members, request_header,
        ros_response, taken);
    } else {
      try_rc = weld_take_response_rtps(
        st, handle, seqs[i], guid, resp_members, request_header,
        ros_response, taken);
    }
    if (try_rc != RMW_RET_OK) {
      return try_rc;
    }
    if (*taken) {
      return RMW_RET_OK;
    }
  }
  *taken = false;
  return RMW_RET_OK;
}

// Take one reply over the in-memory fast path.
//
// The reply was parked by the eager send; this leg only waits bounded
// for it and consumes it. The reply decodes as CDR with an 8 byte echo
// fallback for old servers. Short buffers grow and retry up to the
// same 32MB cap as the RTPS leg because the FFI layer reports short
// without consuming. Returns with the pending entry removed on success
// and kept otherwise.
static rmw_ret_t weld_take_response_local(
  struct weld_rpc_state * st, long long handle, int64_t seq,
  const uint8_t guid[8],
  const rosidl_typesupport_introspection_c__MessageMembers * resp_members,
  weld_service_info_t * request_header, void * ros_response, bool * taken)
{
  size_t cap = 64 * 1024;
  const size_t cap_max = (size_t)(32 << 20);
  uint8_t * buf = NULL;
  int out_n = 0;
  int rc = 0;
  char errbuf[256];
  buf = (uint8_t *)malloc(cap > 0 ? cap : 1);
  if (buf == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response out of memory");
    return RMW_RET_BAD_ALLOC;
  }
  if (weld_ffi_service_poll_reply == NULL) {
    free(buf);
    return weld_fail_unsupported("rmw_weld: rmw_take_response poll not linked yet");
  }
  for (;;) {
    uint8_t * next = NULL;
    if (cap > (size_t)INT_MAX) {
      cap = (size_t)INT_MAX;
    }
    rc = weld_ffi_service_poll_reply(
      handle, (long long)seq, (const char *)guid, (char *)buf, (int)cap,
      &out_n);
    if (rc == 1) {
      free(buf);
      *taken = false;
      return RMW_RET_OK;
    }
    if (rc == -4) {
      if (cap >= cap_max) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response reply too large");
        return RMW_RET_ERROR;
      }
      cap *= 2;
      if (cap > cap_max) {
        cap = cap_max;
      }
      next = (uint8_t *)realloc(buf, cap);
      if (next == NULL) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response out of memory");
        return RMW_RET_BAD_ALLOC;
      }
      buf = next;
      continue;
    }
    if (rc != 0) {
      free(buf);
      return weld_fail_error("rmw_weld: rmw_take_response poll failed");
    }
    break;
  }
  if (out_n < 0) {
    free(buf);
    return weld_fail_error("rmw_weld: rmw_take_response bad length");
  }
  errbuf[0] = '\0';
  weld_cdr_status_t dec = weld_cdr_decode_members(
    resp_members, buf, (size_t)out_n, ros_response, errbuf, sizeof(errbuf));
  if (dec == WELD_CDR_OK) {
    (void)pthread_mutex_lock(&st->mutex);
    weld_rpc_remove_locked(st, seq);
    (void)pthread_mutex_unlock(&st->mutex);
    request_header->source_timestamp = 0;
    request_header->received_timestamp = 0;
    (void)memset(
      request_header->request_id.writer_guid, 0,
      sizeof(request_header->request_id.writer_guid));
    request_header->request_id.sequence_number = seq;
    *taken = true;
    free(buf);
    return RMW_RET_OK;
  }
  // Old in-memory echo: exactly 8 bytes with no CDR header.
  if (out_n == 8 && buf[0] != 0x00) {
    int64_t echo = weld_seq_read((const char *)buf);
    if (echo == seq) {
      (void)pthread_mutex_lock(&st->mutex);
      weld_rpc_remove_locked(st, seq);
      (void)pthread_mutex_unlock(&st->mutex);
      request_header->source_timestamp = 0;
      request_header->received_timestamp = 0;
      (void)memset(
        request_header->request_id.writer_guid, 0,
        sizeof(request_header->request_id.writer_guid));
      request_header->request_id.sequence_number = echo;
      *taken = true;
      free(buf);
      return RMW_RET_OK;
    }
  }
  free(buf);
  rmw_reset_error();
  if (errbuf[0] != '\0') {
    RCUTILS_SET_ERROR_MSG(errbuf);
  } else {
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response decode failed");
  }
  return RMW_RET_ERROR;
}

// Take one reply over the RTPS leg.
//
// The request already published at send time; this leg only polls for
// the matching reply with a bounded wait. A missing RTPS symbol means
// the peer library predates cross-process services, so the take fails
// loudly instead of reporting not ready and hiding the mismatch.
// Short buffers grow and retry without republishing because the FFI
// layer stashes the consumed reply.
static rmw_ret_t weld_take_response_rtps(
  struct weld_rpc_state * st, long long handle, int64_t seq,
  const uint8_t guid[8],
  const rosidl_typesupport_introspection_c__MessageMembers * resp_members,
  weld_service_info_t * request_header, void * ros_response, bool * taken)
{
  size_t cap = 64 * 1024;
  const size_t cap_max = (size_t)(32 << 20);
  char base[512];
  uint8_t * buf = NULL;
  int out_n = 0;
  char errbuf[256];
  weld_cdr_status_t dec;
  if (weld_ffi_service_take_reply == NULL) {
    return weld_fail_unsupported("rmw_weld: rmw_take_response take not linked yet");
  }
  if (weld_service_idl_base(st->type_support, base, sizeof(base)) != 0) {
    return weld_fail_invalid(
      "rmw_weld: rmw_take_response type support has no service name");
  }
  buf = (uint8_t *)malloc(cap > 0 ? cap : 1);
  if (buf == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response out of memory");
    return RMW_RET_BAD_ALLOC;
  }
  for (;;) {
    int rc = 0;
    if (cap > (size_t)INT_MAX) {
      cap = (size_t)INT_MAX;
    }
    rc = weld_ffi_service_take_reply(
      handle, (long long)seq, (const char *)guid, base,
      (char *)buf, (int)cap, &out_n);
    if (rc == 1) {
      free(buf);
      *taken = false;
      return RMW_RET_OK;
    }
    if (rc == -4) {
      uint8_t * next = NULL;
      if (cap >= cap_max) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response reply too large");
        return RMW_RET_ERROR;
      }
      cap *= 2;
      if (cap > cap_max) {
        cap = cap_max;
      }
      next = (uint8_t *)realloc(buf, cap);
      if (next == NULL) {
        free(buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response out of memory");
        return RMW_RET_BAD_ALLOC;
      }
      buf = next;
      continue;
    }
    if (rc != 0) {
      free(buf);
      return weld_fail_error("rmw_weld: rmw_take_response take failed");
    }
    break;
  }
  if (out_n < 0) {
    free(buf);
    return weld_fail_error("rmw_weld: rmw_take_response bad length");
  }
  errbuf[0] = '\0';
  dec = weld_cdr_decode_members(
    resp_members, buf, (size_t)out_n, ros_response, errbuf, sizeof(errbuf));
  free(buf);
  if (dec != WELD_CDR_OK) {
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_response decode failed");
    }
    return RMW_RET_ERROR;
  }
  (void)pthread_mutex_lock(&st->mutex);
  weld_rpc_remove_locked(st, seq);
  (void)pthread_mutex_unlock(&st->mutex);
  request_header->source_timestamp = 0;
  request_header->received_timestamp = 0;
  (void)memset(
    request_header->request_id.writer_guid, 0,
    sizeof(request_header->request_id.writer_guid));
  request_header->request_id.sequence_number = seq;
  *taken = true;
  return RMW_RET_OK;
}

// RPC dispatch thread: fire the stored on-new callback once per
// readiness edge. The is_service flag selects the request probe
// (services) or the reply probe (clients). Same contract as
// subscriptions: no consumption, no locked invocation, FFI absence
// ends the thread.
static void * weld_rpc_dispatch_thread(void * arg)
{
  struct weld_rpc_state * st = (struct weld_rpc_state *)arg;
  if (st == NULL) {
    return NULL;
  }
  for (;;) {
    bool run = false;
    bool is_service = false;
    rmw_event_callback_t cb = NULL;
    const void * user_data = NULL;
    bool ready = false;
    (void)pthread_mutex_lock(&st->mutex);
    run = st->dispatch_run;
    is_service = st->is_service;
    cb = st->on_new_callback;
    user_data = st->on_new_user_data;
    (void)pthread_mutex_unlock(&st->mutex);
    if (!run) {
      return NULL;
    }
    if (weld_ffi_service_has_request == NULL ||
      weld_ffi_service_has_response == NULL)
    {
      return NULL;
    }
    if (is_service) {
      ready = weld_svc_dispatch_probe(st);
    } else {
      ready = weld_cli_dispatch_probe(st);
    }
    if (ready && cb != NULL) {
      bool fire = false;
      (void)pthread_mutex_lock(&st->mutex);
      fire = st->dispatch_run && !st->dispatch_fired;
      if (fire) {
        st->dispatch_fired = true;
      }
      (void)pthread_mutex_unlock(&st->mutex);
      if (fire) {
        cb(user_data, 1);
      }
    } else if (!ready) {
      (void)pthread_mutex_lock(&st->mutex);
      st->dispatch_fired = false;
      (void)pthread_mutex_unlock(&st->mutex);
    }
    weld_dispatch_sleep();
  }
}

// Start the endpoint dispatch thread on the first callback.
//
// Called with no locks held. Shares the subscription contract: start
// failures keep the stored callback without dispatch.
void weld_rpc_dispatch_start(struct weld_rpc_state * st)
{
  int rc = 0;
  bool start = false;
  if (st == NULL) {
    return;
  }
  (void)pthread_mutex_lock(&st->mutex);
  start = !st->dispatch_started && st->on_new_callback != NULL;
  if (start) {
    st->dispatch_run = true;
    st->dispatch_fired = false;
  }
  (void)pthread_mutex_unlock(&st->mutex);
  if (!start) {
    return;
  }
  rc = pthread_create(
    &st->dispatch_thread, NULL, weld_rpc_dispatch_thread, st);
  (void)pthread_mutex_lock(&st->mutex);
  if (rc != 0) {
    st->dispatch_run = false;
  } else {
    st->dispatch_started = true;
  }
  (void)pthread_mutex_unlock(&st->mutex);
}

// Stop the endpoint dispatch thread and wait for its exit.
//
// Called with no locks held. Joining before teardown guarantees the
// thread never touches freed endpoint state.
static void weld_rpc_dispatch_stop(struct weld_rpc_state * st)
{
  bool started = false;
  if (st == NULL) {
    return;
  }
  (void)pthread_mutex_lock(&st->mutex);
  st->dispatch_run = false;
  started = st->dispatch_started;
  (void)pthread_mutex_unlock(&st->mutex);
  if (started) {
    (void)pthread_join(st->dispatch_thread, NULL);
  }
}

rmw_ret_t rmw_service_server_is_available(
  const rmw_node_t * node, const rmw_client_t * client, bool * is_available)
{
  if (node == NULL || client == NULL || is_available == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_service_server_is_available null argument");
  }
  if (!weld_node_ok(node) || !weld_cli_ok(client)) {
    return weld_fail_impl("rmw_weld: rmw_service_server_is_available wrong implementation");
  }
  {
    const struct weld_rpc_state * st =
      (const struct weld_rpc_state *)client->data;
    long long bridge = 0;
    size_t local = 0;
    int remote = 0;
    if (st == NULL || st->name == NULL) {
      return weld_fail_invalid("rmw_weld: rmw_service_server_is_available no data");
    }
    // Local servers come from this process registry; clients share the
    // entry without touching the count.
    (void)pthread_mutex_lock(&g_weld_svc_mutex);
    for (struct weld_svc_shared * e = g_weld_svc_shared; e != NULL;
      e = e->next)
    {
      if (strcmp(e->name, st->name) == 0) {
        local = e->servers;
        break;
      }
    }
    (void)pthread_mutex_unlock(&g_weld_svc_mutex);
    if (weld_ffi_service_remote_available == NULL) {
      return weld_fail_unsupported(
        "rmw_weld: rmw_service_server_is_available FFI not linked");
    }
    if (weld_ffi_bridge_borrow(&bridge) != RMW_RET_OK) {
      return weld_fail_error("rmw_weld: rmw_service_server_is_available no bridge");
    }
    if (weld_ffi_service_remote_available(bridge, st->name, &remote) != 0) {
      return weld_fail_error(
        "rmw_weld: rmw_service_server_is_available backend failed");
    }
    *is_available = (local > 0 || remote > 0);
  }
  return RMW_RET_OK;
}
