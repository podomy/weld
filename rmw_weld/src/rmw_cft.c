// SPDX-License-Identifier: Apache-2.0
// rmw_cft.c: subscription content filters.
#include "rmw_common.h"

// Content filters: expressions compile in the Go backend and attach to
// the shared data receiver by topic, so wire takes apply them
// automatically. Same-process in-memory takes re-evaluate the stashed
// compiled filter in the FFI take path, so both legs enforce the same
// predicate and misses are never delivered. The DDS SQL subset supports
// comparisons, BETWEEN, IN, LIKE, AND, OR, NOT, and dotted scalar field
// paths; "%n" parameters bind in Go before parsing with numeric-looking
// values verbatim and all other values single-quoted, while leading-dot
// numerals and array or sequence fields fail compile. The backend
// resolves the filter descriptor from the well-known std_msgs tables, so
// a type outside that set fails here loudly and stores nothing. Setting
// re-declares the backend reader and announces one extra SEDP reader;
// delivery still fans out once per take.

rmw_ret_t rmw_subscription_set_content_filter(
  rmw_subscription_t * subscription,
  const rmw_subscription_content_filter_options_t * options)
{
  struct weld_sub_data * sd = NULL;
  char * copy = NULL;
  char * blob = NULL;
  size_t nparams = 0;
  size_t total = 0;
  size_t i = 0;
  int rc = 0;
  if (subscription == NULL || options == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_set_content_filter null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_set_content_filter wrong impl");
  }
  if (options->filter_expression == NULL ||
    options->filter_expression[0] == '\0')
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_set_content_filter empty expression");
  }
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_content_filter no data");
    return RMW_RET_INVALID_ARGUMENT;
  }
  if (weld_ffi_sub_set_content_filter == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_content_filter FFI not linked");
    return RMW_RET_UNSUPPORTED;
  }
  nparams = options->expression_parameters.size;
  if (nparams > (size_t)INT_MAX) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_content_filter too many parameters");
    return RMW_RET_INVALID_ARGUMENT;
  }
  if (nparams > 0) {
    if (options->expression_parameters.data == NULL) {
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_subscription_set_content_filter null parameters");
      return RMW_RET_INVALID_ARGUMENT;
    }
    for (i = 0; i < nparams; i++) {
      const char * s = options->expression_parameters.data[i];
      size_t need = 0;
      if (s == NULL) {
        s = "";
      }
      need = strlen(s) + 1;
      if (total + need < total) {
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG(
          "rmw_weld: rmw_subscription_set_content_filter parameters too large");
        return RMW_RET_INVALID_ARGUMENT;
      }
      total += need;
    }
    blob = (char *)malloc(total > 0 ? total : 1);
    if (blob == NULL) {
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_subscription_set_content_filter no memory");
      return RMW_RET_BAD_ALLOC;
    }
    total = 0;
    for (i = 0; i < nparams; i++) {
      const char * s = options->expression_parameters.data[i];
      size_t need = 0;
      if (s == NULL) {
        s = "";
      }
      need = strlen(s) + 1;
      (void)memcpy(blob + total, s, need);
      total += need;
    }
  }
  rc = weld_ffi_sub_set_content_filter(
    sd->handle, options->filter_expression, blob, (int)nparams);
  free(blob);
  if (rc != 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_content_filter backend failed");
    return RMW_RET_ERROR;
  }
  copy = weld_dup_str(options->filter_expression);
  if (copy == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_set_content_filter no memory");
    return RMW_RET_BAD_ALLOC;
  }
  free(sd->filter_expr);
  sd->filter_expr = copy;
  subscription->is_cft_enabled = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_get_content_filter(
  const rmw_subscription_t * subscription, rcutils_allocator_t * allocator,
  rmw_subscription_content_filter_options_t * options)
{
  const struct weld_sub_data * sd = NULL;
  size_t n = 0;
  if (subscription == NULL || allocator == NULL || options == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_get_content_filter null argument");
  }
  if (allocator->allocate == NULL || allocator->deallocate == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_subscription_get_content_filter bad allocator");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_subscription_get_content_filter wrong impl");
  }
  sd = (const struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_get_content_filter no data");
    return RMW_RET_INVALID_ARGUMENT;
  }
  if (sd->filter_expr == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_get_content_filter no filter set");
    return RMW_RET_ERROR;
  }
  // The caller passes a zero-initialized options struct: the expression
  // is allocated fresh here and the parameter array inits to empty.
  n = strlen(sd->filter_expr) + 1;
  options->filter_expression =
    (char *)allocator->allocate(n, allocator->state);
  if (options->filter_expression == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_get_content_filter no memory");
    return RMW_RET_BAD_ALLOC;
  }
  (void)memcpy(options->filter_expression, sd->filter_expr, n);
  // The backend grammar has no "%n" parameters, so set always stores a
  // literal expression and get reports zero parameters.
  if (rcutils_string_array_init(
      &options->expression_parameters, 0, allocator) != RCUTILS_RET_OK)
  {
    allocator->deallocate(options->filter_expression, allocator->state);
    options->filter_expression = NULL;
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_subscription_get_content_filter no memory");
    return RMW_RET_BAD_ALLOC;
  }
  return RMW_RET_OK;
}
