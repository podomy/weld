// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

// loan: loans, drop counters, take info, loaned wire takes.

#include "weld_private.h"
// Loan family: single-owner in-process zero copy with a copying wire
// fallback. True cross-language shared memory would need an shm arena,
// which is out of scope, so zero copy is kept where the architecture
// allows it: same-process pub to sub via the bridge without the wire.
//
// Borrow allocates plus inits a message and records it per publisher.
// Publish encodes plus publishes the wire bytes first (fan out to every
// matched subscription, copying), then enqueues the loaned buffer by
// pointer for one zero-copy handoff; the first loaned take dequeues the
// same pointer with no encode, decode, or payload copy. Take prefers the
// queued handle and falls back to pulling one wire sample (wait stash
// first via weld_take_wire) plus decoding it into a fresh buffer, which
// copies. Return releases the buffer. Publishing or returning a foreign
// or stale pointer fails loudly instead of freeing wild memory. Released
// buffers are fini plus freed, so strings and sequences inside do not
// leak. The queued handle is freed exactly once by its taker's return;
// a second return fails because the record is already unlinked. Callers
// must publish or return every loan before destroying the endpoint;
// entries for a destroyed endpoint are never reclaimed. Loans are single
// owner and not thread safe for concurrent use of the same buffer. Do not
// mix loaned and regular takes on one subscription for one loaned
// publish: the loaned take discards its own wire duplicate, but a regular
// take first leaves the queued handle for a later loaned take to consume
// as a second delivery.


pthread_mutex_t g_weld_loan_mutex = PTHREAD_MUTEX_INITIALIZER;

static struct weld_loan_entry * g_weld_pub_loans = NULL;

static struct weld_loan_entry * g_weld_sub_loans = NULL;

// MESSAGE_LOST drop counters split by side (DDS 1.4 section 2.2.4: lost
// counts are per-reader; publisher losses are separate publish failures).
// Sub drops count consumed but undecodable wire samples on the take
// paths. Pub drops count publish-side encode or send failures that lost
// a sample before delivery. Sharing one counter misattributes sub
// losses to pubs, so takes and waits select by slot side.
size_t g_weld_sub_drops = 0;

size_t g_weld_pub_drops = 0;
// Add one outstanding loan to the list. Returns 0, or -1 on bad argument
// or alloc failure.
int weld_loan_add(
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
int weld_loan_unlink(
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
void weld_loan_restore_pub(
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
  if (g_weld_sub_drops < SIZE_MAX) {
    g_weld_sub_drops++;
  }
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
}

// Record one publish-side lost sample for MESSAGE_LOST.
void weld_pub_drop_add(void)
{
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  if (g_weld_pub_drops < SIZE_MAX) {
    g_weld_pub_drops++;
  }
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
}

// Fill take info with a local receive stamp plus delivered attribution.
//
// The source stamp prefers the publisher-side send stamp from the Go
// core when available (in-process takes carry the publish-time unix
// nanos); wire takes carry none, so the source falls back to the local
// receive time with the documented divergence. The received stamp is
// always the local receive time (never 0). The sequence is the
// per-subscription receive count with intra-process equal to network
// because the bridge is a single transport. A zero sequence means the
// core predates attribution and both sequences stay unsupported. The
// GID is the publishing writer identity, or zeroed when the take
// carried none (wire takes, old core, or a NULL gid).
void weld_fill_take_info(
  rmw_message_info_t * message_info, long long handle,
  unsigned long long seq, const uint8_t gid[16])
{
  struct timespec now;
  uint64_t now_ns = 0;
  unsigned long long source_ns = 0;
  if (message_info == NULL) {
    return;
  }
  (void)clock_gettime(CLOCK_REALTIME, &now);
  now_ns = (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
  if (handle != 0) {
    (void)weld_take_source(handle, &source_ns);
  }
  if (source_ns != 0) {
    message_info->source_timestamp = (rmw_time_point_value_t)source_ns;
  } else {
    message_info->source_timestamp = (rmw_time_point_value_t)now_ns;
  }
  message_info->received_timestamp = (rmw_time_point_value_t)now_ns;
  if (seq == 0 || seq > (unsigned long long)INT64_MAX) {
    message_info->publication_sequence_number =
      RMW_MESSAGE_INFO_SEQUENCE_NUMBER_UNSUPPORTED;
    message_info->reception_sequence_number =
      RMW_MESSAGE_INFO_SEQUENCE_NUMBER_UNSUPPORTED;
  } else {
    message_info->publication_sequence_number = (int64_t)seq;
    message_info->reception_sequence_number = (int64_t)seq;
  }
  message_info->publisher_gid.implementation_identifier =
    kImplementationIdentifier;
  if (gid == NULL) {
    (void)memset(
      message_info->publisher_gid.data, 0,
      sizeof(message_info->publisher_gid.data));
  } else {
    size_t copy_len = sizeof(message_info->publisher_gid.data);
    if (copy_len > 16) {
      copy_len = 16;
    }
    (void)memcpy(message_info->publisher_gid.data, gid, copy_len);
    if (copy_len < sizeof(message_info->publisher_gid.data)) {
      (void)memset(
        message_info->publisher_gid.data + copy_len, 0,
        sizeof(message_info->publisher_gid.data) - copy_len);
    }
  }
  message_info->from_intra_process = false;
}

// Take plus decode one sample into a fresh owned buffer recorded as a
// subscription loan, with an in-process zero-copy fast path first.
// Returns 0 with *message_out owned on data, 1 when no data is ready,
// and -1 with the rmw error set on failure. The fast path dequeues the
// same buffer pointer enqueued by publish with no encode, decode, or
// payload copy; the fallback pulls one wire sample (wait stash first via
// weld_take_wire) plus decodes it into a fresh buffer, which copies.
// The loan fast path discards its wire duplicate without consuming the
// wait stash: wait parks one sample for the next regular take, so the
// duplicate is polled directly and the stash stays intact for a later
// wait to take_loaned delivery. Decode failures count as dropped samples
// for MESSAGE_LOST. On success seq_out plus gid_out carry the delivered
// attribution when non-NULL.
int weld_take_loaned_wire(
  const rmw_subscription_t * subscription, void ** message_out,
  unsigned long long * seq_out, uint8_t gid_out[16])
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
  if (weld_ffi_loan_take_poll != NULL) {
    unsigned long long loan_addr = 0;
    unsigned long long members_addr = 0;
    int lrc = weld_ffi_loan_take_poll(
      sd->handle, &loan_addr, &members_addr);
    if (lrc == 0 && loan_addr != 0 && members_addr != 0) {
      const rosidl_typesupport_introspection_c__MessageMembers * loan_members =
        (const rosidl_typesupport_introspection_c__MessageMembers *)
        (uintptr_t)members_addr;
      void * loan_buf = (void *)(uintptr_t)loan_addr;
      if (weld_loan_add(&g_weld_sub_loans, subscription, loan_buf,
        loan_members) != 0)
      {
        loan_members->fini_function(loan_buf);
        free(loan_buf);
        rmw_reset_error();
        RCUTILS_SET_ERROR_MSG("rmw_weld: loaned take no memory");
        return -1;
      }
      // Discard the wire duplicate without touching the wait stash so a
      // wait-parked sample survives for the next regular take. The stash
      // stays executor-thread owned and is never consumed here.
      {
        uint8_t * dup_direct = NULL;
        size_t dup_direct_len = 0;
        if (weld_poll_payload(
          sd->handle, &dup_direct, &dup_direct_len, NULL, NULL) == 0)
        {
          free(dup_direct);
        }
      }
      if (seq_out != NULL || gid_out != NULL) {
        uint8_t gid[16];
        unsigned long long seq = 0;
        (void)weld_take_info(sd->handle, &seq, gid);
        if (seq_out != NULL) {
          *seq_out = seq;
        }
        if (gid_out != NULL) {
          (void)memcpy(gid_out, gid, sizeof(gid));
        }
      }
      *message_out = loan_buf;
      return 0;
    }
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
  empty = weld_take_wire(sd, &wire, &wire_len, seq_out, gid_out);
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
// Loan family: single-owner in-process zero copy with a copying wire
// fallback. The wire path copies (CDR encode plus channel copy plus CDR
// decode), which serialization inherently requires; the in-process path
// shares the loaned buffer by pointer with no copy. See the loan infra
// above for the borrow, publish, take, and return bookkeeping.

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
  // The wire bytes publish first for every subscriber (copying); the
  // loaned buffer is then queued by pointer for one zero-copy take, and
  // freed here when the queue is unavailable so delivery still succeeds
  // via the wire copy without leaking.
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
  if (weld_ffi_loan_publish != NULL) {
    int lrc = weld_ffi_loan_publish(
      pd->handle, (unsigned long long)(uintptr_t)ros_message,
      (unsigned long long)(uintptr_t)members);
    if (lrc == 0) {
      return RMW_RET_OK;
    }
  }
  members->fini_function(ros_message);
  free(ros_message);
  return RMW_RET_OK;
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
  empty = weld_take_loaned_wire(subscription, &buf, NULL, NULL);
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
  unsigned long long seq = 0;
  uint8_t gid[16];
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
  (void)memset(gid, 0, sizeof(gid));
  empty = weld_take_loaned_wire(subscription, &buf, &seq, gid);
  if (empty == 1) {
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  {
    struct weld_sub_data * fill_sd =
      (struct weld_sub_data *)subscription->data;
    long long fill_handle = fill_sd != NULL ? fill_sd->handle : 0;
    weld_fill_take_info(message_info, fill_handle, seq, gid);
  }
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

// On-new callbacks: stored on the endpoint plus dispatched. A per
// endpoint background thread polls a non-consuming readiness probe
// and invokes the stored callback once per arrival edge, so custom
// loops learn about data without polling wait. Takes still deliver
// every sample; the probes never consume. Clearing with NULL stops
// firing but keeps the thread parked until destroy. Start failures
// keep the stored callback without dispatch.
