// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

// init: init, shutdown, context, serialization format, log severity.

#include "weld_private.h"
// Process-global FFI bridge handle.
//
// The bridge is created on the first rmw_init and joined with the
// participant plus domain from the init options. It is reference counted
// across contexts and freed on the last rmw_shutdown. Node data stores a
// copy of the handle plus the node name so pub and sub creation can pass
// both to the FFI layer.
pthread_mutex_t g_weld_ffi_mutex = PTHREAD_MUTEX_INITIALIZER;
long long g_weld_bridge = 0;
int g_weld_bridge_refs = 0;
const char * rmw_get_implementation_identifier(void)
{
  return kImplementationIdentifier;
}
// Implementation-defined init options marker.
//
// A non-NULL impl means this init_options was set up by rmw_init_options_init.
// All other fields stay in plain C memory owned here: fini frees the enclave
// copy and the marker, then re-zeroes the struct.
struct rmw_init_options_impl_s
{
  int initialized;
};
rmw_init_options_t rmw_get_zero_initialized_init_options(void)
{
  rmw_init_options_t opts;
  (void)memset(&opts, 0, sizeof(opts));
  return opts;
}

static bool weld_init_options_is_init(const rmw_init_options_t * options)
{
  return options != NULL && options->impl != NULL &&
    options->implementation_identifier == kImplementationIdentifier;
}

rmw_ret_t rmw_init_options_init(
  rmw_init_options_t * init_options, rcutils_allocator_t allocator)
{
  if (init_options == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_init null argument");
  }
  if (weld_init_options_is_init(init_options)) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_init already init");
  }
  (void)memset(init_options, 0, sizeof(*init_options));
  init_options->implementation_identifier = kImplementationIdentifier;
  init_options->domain_id = RMW_DEFAULT_DOMAIN_ID;
  /* No localhost_only assignment: Humble and Jazzy define it with
     RMW_LOCALHOST_ONLY_DEFAULT equal to zero, so memset already yields the
     default; Rolling removed the field, so naming it breaks that build. */
  init_options->enclave = NULL;
  init_options->allocator = allocator;
  init_options->impl =
    (rmw_init_options_impl_t *)malloc(sizeof(rmw_init_options_impl_t));
  if (init_options->impl == NULL) {
    (void)memset(init_options, 0, sizeof(*init_options));
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_init_options_init no memory");
    return RMW_RET_BAD_ALLOC;
  }
  init_options->impl->initialized = 1;
  return RMW_RET_OK;
}

rmw_ret_t rmw_init_options_copy(
  const rmw_init_options_t * src, rmw_init_options_t * dst)
{
  char * enclave_copy = NULL;
  if (src == NULL || dst == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_copy null argument");
  }
  if (!weld_init_options_is_init(src)) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_copy bad source");
  }
  if (weld_init_options_is_init(dst)) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_copy dst taken");
  }
  if (src->implementation_identifier != kImplementationIdentifier) {
    return weld_fail_impl("rmw_weld: rmw_init_options_copy wrong implementation");
  }
  if (src->enclave != NULL) {
    enclave_copy = weld_dup_str(src->enclave);
    if (enclave_copy == NULL) {
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_init_options_copy no memory");
      return RMW_RET_BAD_ALLOC;
    }
  }
  *dst = *src;
  dst->enclave = enclave_copy;
  dst->impl = (rmw_init_options_impl_t *)malloc(sizeof(rmw_init_options_impl_t));
  if (dst->impl == NULL) {
    free(enclave_copy);
    (void)memset(dst, 0, sizeof(*dst));
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_init_options_copy no memory");
    return RMW_RET_BAD_ALLOC;
  }
  dst->impl->initialized = 1;
  return RMW_RET_OK;
}

rmw_ret_t rmw_init_options_fini(rmw_init_options_t * init_options)
{
  if (init_options == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_fini null argument");
  }
  if (!weld_init_options_is_init(init_options)) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_fini not init");
  }
  free(init_options->enclave);
  free(init_options->impl);
  (void)memset(init_options, 0, sizeof(*init_options));
  return RMW_RET_OK;
}

rmw_ret_t rmw_init(const rmw_init_options_t * options, rmw_context_t * context)
{
  size_t domain = 0;
  const char * participant = NULL;
  long long bridge = 0;
  int rc = 0;
  bool fresh = false;
  if (options == NULL || context == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init null argument");
  }
  if (context->implementation_identifier == kImplementationIdentifier) {
    return weld_fail_invalid("rmw_weld: rmw_init context already init");
  }
  domain = options->domain_id;
  if (domain == RMW_DEFAULT_DOMAIN_ID) {
    domain = 0;
  }
  if (domain > (size_t)INT_MAX) {
    return weld_fail_invalid("rmw_weld: rmw_init domain too large");
  }
  if (options->enclave != NULL && options->enclave[0] != '\0') {
    participant = options->enclave;
  } else {
    participant = "weld_rmw";
  }
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  if (g_weld_bridge == 0) {
    rc = weld_ffi_bridge_new(&bridge);
    if (rc != 0) {
      (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
      return weld_fail_ffi("rmw_weld: rmw_init bridge_new failed", rc);
    }
    g_weld_bridge = bridge;
    fresh = true;
  }
  rc = weld_ffi_join(g_weld_bridge, participant, (int)domain);
  if (rc != 0) {
    if (fresh) {
      long long dead = g_weld_bridge;
      g_weld_bridge = 0;
      (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
      (void)weld_ffi_bridge_free(dead);
      return weld_fail_ffi("rmw_weld: rmw_init join failed", rc);
    }
    (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
    return weld_fail_ffi("rmw_weld: rmw_init join failed", rc);
  }
  g_weld_bridge_refs++;
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  context->implementation_identifier = kImplementationIdentifier;
  context->actual_domain_id = domain;
  context->impl = NULL;
  return RMW_RET_OK;
}

rmw_ret_t rmw_shutdown(rmw_context_t * context)
{
  long long dead = 0;
  if (context == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_shutdown null argument");
  }
  if (!weld_context_ok(context)) {
    return weld_fail_impl("rmw_weld: rmw_shutdown wrong implementation");
  }
  (void)pthread_mutex_lock(&g_weld_ffi_mutex);
  if (g_weld_bridge_refs > 0) {
    g_weld_bridge_refs--;
  }
  if (g_weld_bridge_refs == 0 && g_weld_bridge != 0) {
    dead = g_weld_bridge;
    g_weld_bridge = 0;
  }
  (void)pthread_mutex_unlock(&g_weld_ffi_mutex);
  if (dead != 0) {
    int rc = weld_ffi_bridge_free(dead);
    if (rc != 0) {
      return weld_fail_ffi("rmw_weld: rmw_shutdown bridge_free failed", rc);
    }
  }
  return RMW_RET_OK;
}
rmw_ret_t rmw_context_fini(rmw_context_t * context)
{
  if (context == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_context_fini null argument");
  }
  if (weld_context_ok(context)) {
    (void)memset(context, 0, sizeof(*context));
    return RMW_RET_OK;
  }
  if (context->implementation_identifier == NULL) {
    (void)memset(context, 0, sizeof(*context));
    return RMW_RET_OK;
  }
  return weld_fail_impl("rmw_weld: rmw_context_fini wrong implementation");
}

const char * rmw_get_serialization_format(void)
{
  return "cdr";
}

rmw_ret_t rmw_compare_gids_equal(
  const rmw_gid_t * gid1, const rmw_gid_t * gid2, bool * result)
{
  if (gid1 == NULL || gid2 == NULL || result == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_compare_gids_equal null argument");
  }
  if (gid1->implementation_identifier != kImplementationIdentifier ||
    gid2->implementation_identifier != kImplementationIdentifier)
  {
    return weld_fail_impl("rmw_weld: rmw_compare_gids_equal wrong implementation");
  }
  *result = memcmp(gid1->data, gid2->data, sizeof(gid1->data)) == 0;
  return RMW_RET_OK;
}

static rmw_log_severity_t g_weld_log_severity = RMW_LOG_SEVERITY_INFO;

rmw_ret_t rmw_set_log_severity(rmw_log_severity_t severity)
{
  g_weld_log_severity = severity;
  return RMW_RET_OK;
}
// Feature query: only dynamic message takes are real. The take
// dynamic paths move the wire sample into caller bound dynamic data
// through the runtime deserializer when it is linked, which Jazzy and
// newer name RMW_MIDDLEWARE_CAN_TAKE_DYNAMIC_MESSAGE. Sequence numbers
// stay unsupported (both MESSAGE_INFO values stay false) and no
// discovery population exists (TYPE_DISCOVERY stays false). Humble
// predates dynamic takes entirely and stays false for every value.
bool rmw_feature_supported(rmw_feature_t feature)
{
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")
#if defined(RMW_MIDDLEWARE_CAN_TAKE_DYNAMIC_MESSAGE)
  return feature == RMW_MIDDLEWARE_CAN_TAKE_DYNAMIC_MESSAGE;
#else
  return feature == (rmw_feature_t)3;
#endif
#else
  (void)feature;
  return false;
#endif
}
