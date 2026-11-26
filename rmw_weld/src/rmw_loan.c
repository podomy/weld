// SPDX-License-Identifier: Apache-2.0
// rmw_loan.c: loaned messages plus MESSAGE_LOST drops.
#include "rmw_common.h"

// Loan family: owned heap buffers handed out as loans, never zero copy.
//
// The transport copies bytes at ingress, so no middleware loan is ever
// shared. Borrow allocates plus inits a message and records it per
// publisher, publish encodes plus publishes the buffer then releases the
// record, take pulls one wire sample (wait stash first via
// weld_take_wire) plus decodes it into a fresh buffer recorded per
// subscription, and return releases the buffer. Publishing or returning
// a foreign or stale pointer fails loudly instead of freeing wild
// memory. Released buffers are fini plus freed, so strings and sequences
// inside do not leak. Callers must publish or return every loan before
// destroying the endpoint; entries for a destroyed endpoint are never
// reclaimed. Loans are single owner and not thread safe for concurrent
// use of the same buffer.

struct weld_loan_entry
{
  const void * endpoint;
  void * message;
  const rosidl_typesupport_introspection_c__MessageMembers * members;
  struct weld_loan_entry * next;
};

pthread_mutex_t g_weld_loan_mutex = PTHREAD_MUTEX_INITIALIZER;

static struct weld_loan_entry * g_weld_pub_loans = NULL;

static struct weld_loan_entry * g_weld_sub_loans = NULL;

size_t g_weld_decode_drops = 0;

// Add one outstanding loan to the list. Returns 0, or -1 on bad argument
// or alloc failure.
static int weld_loan_add(
  struct weld_loan_entry ** list, const void * endpoint, void * message,
  const rosidl_typesupport_introspection_c__MessageMembers * members)
{
  struct weld_loan_entry * e = NULL;
  if (list == NULL || endpoint == NULL || message == NULL ||
    members == NULL)
  {
    return -1;
  }
  e = (struct weld_loan_entry *)malloc(sizeof(*e));
  if (e == NULL) {
    return -1;
  }
  e->endpoint = endpoint;
  e->message = message;
  e->members = members;
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  e->next = *list;
  *list = e;
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return 0;
}

// Unlink one outstanding loan. On success stores the members for the
// caller to fini plus free and returns 0, else returns -1.
static int weld_loan_unlink(
  struct weld_loan_entry ** list, const void * endpoint, void * message,
  const rosidl_typesupport_introspection_c__MessageMembers ** members_out)
{
  int found = -1;
  if (list == NULL || endpoint == NULL || message == NULL ||
    members_out == NULL)
  {
    return -1;
  }
  *members_out = NULL;
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  {
    struct weld_loan_entry ** link = list;
    while (*link != NULL) {
      if ((*link)->endpoint == endpoint && (*link)->message == message) {
        struct weld_loan_entry * dead = *link;
        *link = dead->next;
        *members_out = dead->members;
        free(dead);
        found = 0;
        break;
      }
      link = &(*link)->next;
    }
  }
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return found;
}

// Restore a loan unlinked for publish after a failure. Fini plus frees
// the message when the record itself cannot be restored.
static void weld_loan_restore_pub(
  const void * endpoint, void * message,
  const rosidl_typesupport_introspection_c__MessageMembers * members)
{
  if (endpoint == NULL || message == NULL || members == NULL) {
    return;
  }
  if (weld_loan_add(&g_weld_pub_loans, endpoint, message, members) != 0) {
    members->fini_function(message);
    free(message);
  }
}

// Record one consumed but unusable wire sample for MESSAGE_LOST.
void weld_drop_add(void)
{
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  if (g_weld_decode_drops < SIZE_MAX) {
    g_weld_decode_drops++;
  }
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
}

// Fill take info with a local receive stamp and unsupported source data.
//
// The FFI layer carries no source timestamp or sequence, so those report
// zero and unsupported while the receive stamp uses the local clock.
// This mirrors the rmw_take_with_info path, which is owned elsewhere.
void weld_fill_take_info(rmw_message_info_t * message_info)
{
  struct timespec now;
  uint64_t now_ns = 0;
  if (message_info == NULL) {
    return;
  }
  (void)clock_gettime(CLOCK_REALTIME, &now);
  now_ns = (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
  message_info->source_timestamp = 0;
  message_info->received_timestamp = (rmw_time_point_value_t)now_ns;
  message_info->publication_sequence_number =
    RMW_MESSAGE_INFO_SEQUENCE_NUMBER_UNSUPPORTED;
  message_info->reception_sequence_number =
    RMW_MESSAGE_INFO_SEQUENCE_NUMBER_UNSUPPORTED;
  message_info->publisher_gid.implementation_identifier =
    kImplementationIdentifier;
  (void)memset(
    message_info->publisher_gid.data, 0,
    sizeof(message_info->publisher_gid.data));
  message_info->from_intra_process = false;
}

rmw_ret_t rmw_borrow_loaned_message(
  const rmw_publisher_t * publisher,
  const rosidl_message_type_support_t * type_support, void ** ros_message)
{
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  void * buf = NULL;
  if (publisher == NULL || type_support == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_borrow_loaned_message null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_borrow_loaned_message wrong impl");
  }
  if (*ros_message != NULL) {
    return weld_fail_invalid("rmw_weld: rmw_borrow_loaned_message not null");
  }
  members = weld_resolve_members(type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_borrow_loaned_message type support is not introspection");
    return RMW_RET_ERROR;
  }
  if (members->init_function == NULL || members->fini_function == NULL ||
    members->size_of_ == 0)
  {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_borrow_loaned_message type support has no init");
    return RMW_RET_ERROR;
  }
  buf = calloc(1, members->size_of_);
  if (buf == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_borrow_loaned_message no memory");
    return RMW_RET_BAD_ALLOC;
  }
  members->init_function(buf, ROSIDL_RUNTIME_C_MSG_INIT_ALL);
  if (weld_loan_add(&g_weld_pub_loans, publisher, buf, members) != 0) {
    members->fini_function(buf);
    free(buf);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_borrow_loaned_message no memory");
    return RMW_RET_BAD_ALLOC;
  }
  *ros_message = buf;
  return RMW_RET_OK;
}

rmw_ret_t rmw_publish_loaned_message(
  const rmw_publisher_t * publisher, void * ros_message,
  rmw_publisher_allocation_t * allocation)
{
  // The loan is unlinked first so a concurrent publish or return of the
  // same buffer fails loudly instead of encoding or freeing twice. On
  // failure the record is restored so the caller still owns the loan.
  const struct weld_pub_data * pd = NULL;
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  weld_cdr_status_t st;
  int rc = 0;
  (void)allocation;
  if (publisher == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publish_loaned_message null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publish_loaned_message wrong impl");
  }
  pd = (const struct weld_pub_data *)publisher->data;
  if (pd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publish_loaned_message no data");
    return RMW_RET_INVALID_ARGUMENT;
  }
  if (weld_loan_unlink(&g_weld_pub_loans, publisher, ros_message, &members) != 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_publish_loaned_message unknown loaned message");
    return RMW_RET_INVALID_ARGUMENT;
  }
  errbuf[0] = '\0';
  st = weld_cdr_encode_members(
    members, ros_message, true, &wire, &wire_len, errbuf, sizeof(errbuf));
  if (st != WELD_CDR_OK) {
    weld_loan_restore_pub(publisher, ros_message, members);
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG(
        "rmw_weld: rmw_publish_loaned_message encode failed");
    }
    free(wire);
    return RMW_RET_ERROR;
  }
  if (wire_len > (size_t)INT_MAX) {
    free(wire);
    weld_loan_restore_pub(publisher, ros_message, members);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_publish_loaned_message payload too large");
    return RMW_RET_ERROR;
  }
  rc = weld_ffi_pub_publish(
    pd->handle, (const char *)wire, (int)wire_len);
  free(wire);
  if (rc != 0) {
    weld_loan_restore_pub(publisher, ros_message, members);
    return weld_fail_ffi(
      "rmw_weld: rmw_publish_loaned_message backend failed", rc);
  }
  members->fini_function(ros_message);
  free(ros_message);
  return RMW_RET_OK;
}

// Take plus decode one sample into a fresh owned buffer recorded as a
// subscription loan. Returns 0 with *message_out owned on data, 1 when
// no data is ready, and -1 with the rmw error set on failure. Decode
// failures count as dropped samples for MESSAGE_LOST.
static int weld_take_loaned_wire(
  const rmw_subscription_t * subscription, void ** message_out)
{
  struct weld_sub_data * sd = NULL;
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  weld_cdr_status_t st;
  void * buf = NULL;
  int empty = 0;
  if (subscription == NULL || message_out == NULL) {
    return -1;
  }
  *message_out = NULL;
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take no subscription data");
    return -1;
  }
  members = weld_resolve_members(sd->type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: loaned take type support is not introspection");
    return -1;
  }
  if (members->init_function == NULL || members->fini_function == NULL ||
    members->size_of_ == 0)
  {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take type support has no init");
    return -1;
  }
  empty = weld_take_wire(sd, &wire, &wire_len);
  if (empty == 1) {
    return 1;
  }
  if (empty != 0) {
    return -1;
  }
  buf = calloc(1, members->size_of_);
  if (buf == NULL) {
    free(wire);
    weld_drop_add();
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take no memory");
    return -1;
  }
  members->init_function(buf, ROSIDL_RUNTIME_C_MSG_INIT_ALL);
  errbuf[0] = '\0';
  st = weld_cdr_decode_members(
    members, wire, wire_len, buf, errbuf, sizeof(errbuf));
  free(wire);
  if (st != WELD_CDR_OK) {
    members->fini_function(buf);
    free(buf);
    weld_drop_add();
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take decode failed");
    }
    return -1;
  }
  if (weld_loan_add(&g_weld_sub_loans, subscription, buf, members) != 0) {
    members->fini_function(buf);
    free(buf);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take no memory");
    return -1;
  }
  *message_out = buf;
  return 0;
}

rmw_ret_t rmw_take_loaned_message(
  const rmw_subscription_t * subscription, void ** loaned_message,
  bool * taken, rmw_subscription_allocation_t * allocation)
{
  void * buf = NULL;
  int empty = 0;
  (void)allocation;
  if (subscription == NULL || loaned_message == NULL || taken == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_loaned_message null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_take_loaned_message wrong impl");
  }
  if (*loaned_message != NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_loaned_message not null");
  }
  *loaned_message = NULL;
  *taken = false;
  empty = weld_take_loaned_wire(subscription, &buf);
  if (empty == 1) {
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  *loaned_message = buf;
  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_loaned_message_with_info(
  const rmw_subscription_t * subscription, void ** loaned_message,
  bool * taken, rmw_message_info_t * message_info,
  rmw_subscription_allocation_t * allocation)
{
  void * buf = NULL;
  int empty = 0;
  (void)allocation;
  if (subscription == NULL || loaned_message == NULL || taken == NULL ||
    message_info == NULL)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_take_loaned_message_with_info null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_take_loaned_message_with_info wrong impl");
  }
  if (*loaned_message != NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_take_loaned_message_with_info not null");
  }
  *loaned_message = NULL;
  *taken = false;
  empty = weld_take_loaned_wire(subscription, &buf);
  if (empty == 1) {
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  weld_fill_take_info(message_info);
  *loaned_message = buf;
  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_return_loaned_message_from_publisher(
  const rmw_publisher_t * publisher, void * loaned_message)
{
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  if (publisher == NULL || loaned_message == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_return_loaned_message_from_publisher null arg");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl(
      "rmw_weld: rmw_return_loaned_message_from_publisher wrong impl");
  }
  if (weld_loan_unlink(&g_weld_pub_loans, publisher, loaned_message, &members) != 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_return_loaned_message_from_publisher unknown loan");
    return RMW_RET_INVALID_ARGUMENT;
  }
  members->fini_function(loaned_message);
  free(loaned_message);
  return RMW_RET_OK;
}

rmw_ret_t rmw_return_loaned_message_from_subscription(
  const rmw_subscription_t * subscription, void * loaned_message)
{
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  if (subscription == NULL || loaned_message == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_return_loaned_message_from_subscription null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_return_loaned_message_from_subscription wrong impl");
  }
  if (weld_loan_unlink(&g_weld_sub_loans, subscription, loaned_message, &members) != 0) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_return_loaned_message_from_subscription unknown loan");
    return RMW_RET_INVALID_ARGUMENT;
  }
  members->fini_function(loaned_message);
  free(loaned_message);
  return RMW_RET_OK;
}
