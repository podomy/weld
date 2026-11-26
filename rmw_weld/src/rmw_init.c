// SPDX-License-Identifier: Apache-2.0
// rmw_init.c: init options, context init and shutdown.
#include "rmw_common.h"

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
  // The allocator is stored for API compatibility. Internal
  // allocations below use malloc plus free in matched pairs instead:
  // threading one allocator through every entry point would touch
  // the whole file for no behavioral gain, and the pairs never cross
  // into caller-owned memory.
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
  if (src->impl == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init_options_copy bad source");
  }
  if (src->implementation_identifier != kImplementationIdentifier) {
    return weld_fail_impl("rmw_weld: rmw_init_options_copy wrong implementation");
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
  if (options->impl == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init options not init");
  }
  if (options->implementation_identifier != kImplementationIdentifier) {
    return weld_fail_impl("rmw_weld: rmw_init wrong implementation");
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
  if (context->implementation_identifier == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_shutdown context not init");
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
  // Mark this context shut down: a second shutdown fails instead of
  // silently succeeding, and a later re-init starts clean.
  context->implementation_identifier = NULL;
  return RMW_RET_OK;
}

// Context finalize plus serialization format plus GID compare plus log
// severity plus liveliness plus ack plus allocation plus take variants
// plus client GID. Single process in memory shim, so every path validates
// only and never contacts the Go core except where the take helpers poll.

rmw_ret_t rmw_context_fini(rmw_context_t * context)
{
  if (context == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_context_fini null argument");
  }
  if (weld_context_ok(context)) {
    return weld_fail_invalid("rmw_weld: rmw_context_fini live context");
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

static _Atomic rmw_log_severity_t g_weld_log_severity =
  RMW_LOG_SEVERITY_INFO;

rmw_ret_t rmw_set_log_severity(rmw_log_severity_t severity)
{
  (void)atomic_store(&g_weld_log_severity, severity);
  return RMW_RET_OK;
}

// Feature query: no middleware optional feature is supported. QoS
// compatibility itself is real through rmw_qos_profile_check_compatible,
// but rmw_feature_supported only covers the rmw_feature_t enum and
// stays false for every member.
bool rmw_feature_supported(rmw_feature_t feature)
{
  (void)feature;
  return false;
}
