// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

// common: shared fail/ok helpers, graph lock, wait wake.

#include "weld_private.h"
const char kImplementationIdentifier[] = "rmw_weld";
rmw_ret_t weld_fail_ffi(const char * what, int code)
{
  char buf[256];
  rmw_reset_error();
  if (what == NULL) {
    what = "rmw_weld: backend call failed";
  }
  (void)snprintf(buf, sizeof(buf), "%s: ffi rc=%d", what, code);
  buf[sizeof(buf) - 1] = '\0';
  RCUTILS_SET_ERROR_MSG(buf);
  return RMW_RET_ERROR;
}
#ifndef WELD_HAVE_NAMES_AND_TYPES
rmw_ret_t weld_unimplemented(const char * what)
{
  rmw_reset_error();
  if (what == NULL) {
    RCUTILS_SET_ERROR_MSG("rmw_weld: unimplemented");
  } else {
    char buf[256];
    (void)snprintf(buf, sizeof(buf), "rmw_weld: %s not implemented yet", what);
    buf[sizeof(buf) - 1] = '\0';
    RCUTILS_SET_ERROR_MSG(buf);
  }
  return RMW_RET_ERROR;
}
#endif
bool weld_context_ok(const rmw_context_t * context)
{
  return context != NULL &&
    context->implementation_identifier == kImplementationIdentifier;
}

bool weld_impl_eq(const char * got)
{
  return got == kImplementationIdentifier;
}

bool weld_node_ok(const rmw_node_t * node)
{
  return node != NULL && weld_impl_eq(node->implementation_identifier);
}

bool weld_pub_ok(const rmw_publisher_t * pub)
{
  return pub != NULL && weld_impl_eq(pub->implementation_identifier);
}

bool weld_sub_ok(const rmw_subscription_t * sub)
{
  return sub != NULL && weld_impl_eq(sub->implementation_identifier);
}

bool weld_svc_ok(const rmw_service_t * svc)
{
  return svc != NULL && weld_impl_eq(svc->implementation_identifier);
}

bool weld_cli_ok(const rmw_client_t * cli)
{
  return cli != NULL && weld_impl_eq(cli->implementation_identifier);
}

bool weld_waitset_ok(const rmw_wait_set_t * ws)
{
  return ws != NULL && weld_impl_eq(ws->implementation_identifier);
}
void weld_graph_lock(void)
{
  (void)pthread_mutex_lock(&g_weld_graph_mutex);
}

void weld_graph_unlock(void)
{
  (void)pthread_mutex_unlock(&g_weld_graph_mutex);
}

rmw_ret_t weld_fail_invalid(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_INVALID_ARGUMENT;
}

rmw_ret_t weld_fail_impl(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_INCORRECT_RMW_IMPLEMENTATION;
}

rmw_ret_t weld_fail_error(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_ERROR;
}

rmw_ret_t weld_fail_unsupported(const char * what)
{
  rmw_reset_error();
  RCUTILS_SET_ERROR_MSG(what);
  return RMW_RET_UNSUPPORTED;
}

char * weld_dup_str(const char * s)
{
  if (s == NULL) {
    return NULL;
  }
  size_t n = strlen(s) + 1;
  char * out = (char *)malloc(n);
  if (out == NULL) {
    return NULL;
  }
  (void)memcpy(out, s, n);
  return out;
}
