// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

// publish: publishers.

#include "weld_private.h"
rmw_publisher_t * rmw_create_publisher(
  const rmw_node_t * node, const rosidl_message_type_support_t * type_support,
  const char * topic_name, const rmw_qos_profile_t * qos_profile,
  const rmw_publisher_options_t * publisher_options)
{
  // Bridge: weld_ffi_pub_create stores passthrough QoS and registers in
  // per-domain pubs keyed by domain plus topic. The node name comes from
  // rmw_node_t data so the Go layer reuses the cached bridge node.
  struct weld_node_data * nd = NULL;
  struct weld_pub_data * pd = NULL;
  rmw_publisher_t * pub = NULL;
  char * topic_copy = NULL;
  char * data_topic = NULL;
  char * node_copy = NULL;
  long long handle = 0;
  int rc = 0;
  if (node == NULL || type_support == NULL || topic_name == NULL ||
    qos_profile == NULL)
  {
    (void)weld_fail_invalid("rmw_weld: rmw_create_publisher null argument");
    return NULL;
  }
  if (!weld_node_ok(node)) {
    (void)weld_fail_impl("rmw_weld: rmw_create_publisher wrong implementation");
    return NULL;
  }
  if (topic_name[0] == '\0') {
    (void)weld_fail_invalid("rmw_weld: rmw_create_publisher empty topic");
    return NULL;
  }
  nd = (struct weld_node_data *)node->data;
  if (nd == NULL || nd->node_name == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_publisher no node data");
    return NULL;
  }
  if (weld_resolve_members(type_support) == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_create_publisher type support is not introspection");
    return NULL;
  }
  pub = (rmw_publisher_t *)malloc(sizeof(*pub));
  pd = (struct weld_pub_data *)malloc(sizeof(*pd));
  topic_copy = weld_dup_str(topic_name);
  data_topic = weld_dup_str(topic_name);
  node_copy = weld_dup_str(node->name != NULL ? node->name : "");
  if (pub == NULL || pd == NULL || topic_copy == NULL || data_topic == NULL ||
    node_copy == NULL)
  {
    free(pub);
    free(pd);
    free(topic_copy);
    free(data_topic);
    free(node_copy);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_create_publisher out of memory");
    return NULL;
  }
  rc = weld_ffi_pub_create(nd->bridge, nd->node_name, topic_name, &handle);
  if (rc != 0) {
    free(pub);
    free(pd);
    free(topic_copy);
    free(data_topic);
    free(node_copy);
    (void)weld_fail_ffi("rmw_weld: rmw_create_publisher backend failed", rc);
    return NULL;
  }
  pd->handle = handle;
  pd->type_support = type_support;
  pd->topic = data_topic;
  pd->node_name = node_copy;
  pd->domain = nd->domain;
  pd->qos = *qos_profile;
  pd->incompat_total = 0;
  // Report the ROS type for graph interop. Best effort: a missing
  // symbol or bad name skips silently and creation still succeeds.
  weld_report_type(handle, type_support);
  // Report the deadline for offered-miss tracking. Best effort: an older
  // Go library without the symbol keeps local-only timing with no miss
  // counts instead of failing creation.
  if (weld_ffi_pub_set_deadline != NULL) {
    long long dsec = 0;
    long long dnsec = 0;
    if (qos_profile->deadline.sec <= (uint64_t)INT64_MAX) {
      dsec = (long long)qos_profile->deadline.sec;
    }
    if (qos_profile->deadline.nsec <= (uint64_t)INT64_MAX) {
      dnsec = (long long)qos_profile->deadline.nsec;
    }
    (void)weld_ffi_pub_set_deadline(handle, dsec, dnsec);
  }
  // Count local incompatibilities at match time against existing
  // subscriptions on the same topic. Both sides bump so take totals stay
  // real without a second pass.
  pd->incompat_total = weld_incompat_scan(
    WELD_EP_PUB, qos_profile, topic_name, nd->domain);
  pub->implementation_identifier = kImplementationIdentifier;
  pub->data = pd;
  pub->topic_name = topic_copy;
  if (publisher_options != NULL) {
    pub->options = *publisher_options;
  } else {
    (void)memset(&pub->options, 0, sizeof(pub->options));
  }
  // Loans are owned heap buffers, never zero copy shared memory: see
  // the loan family above.
  pub->can_loan_messages = true;
  // Record the endpoint for by-node plus info-by-topic queries. Best
  // effort: a missed record only shrinks info snapshots.
  weld_ep_register(
    WELD_EP_PUB, node->name, node->namespace_, nd->domain, pd);
  return pub;
}

rmw_ret_t rmw_destroy_publisher(rmw_node_t * node, rmw_publisher_t * publisher)
{
  // Bridge: weld_ffi_pub_destroy unregisters from per-domain pubs and marks
  // closed so CountMatchedPublishers drops and Publish reports
  // ErrPublisherClosed. Channels stay open to avoid send panics.
  struct weld_pub_data * pd = NULL;
  int rc = 0;
  if (node == NULL || publisher == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_destroy_publisher null argument");
  }
  if (!weld_node_ok(node) || !weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_destroy_publisher wrong implementation");
  }
  pd = (struct weld_pub_data *)publisher->data;
  if (pd != NULL) {
    weld_ep_unregister(pd);
    // Check the backend result before freeing: on failure the
    // handle stays valid so the caller may retry the destroy
    // instead of using freed memory.
    rc = weld_ffi_pub_destroy(pd->handle);
    if (rc != 0) {
      return weld_fail_ffi(
        "rmw_weld: rmw_destroy_publisher backend failed", rc);
    }
    free(pd->topic);
    free(pd->node_name);
    free(pd);
  }
  publisher->data = NULL;
  free((void *)publisher->topic_name);
  free(publisher);
  return RMW_RET_OK;
}

rmw_ret_t rmw_publish(
  const rmw_publisher_t * publisher, const void * ros_message,
  rmw_publisher_allocation_t * allocation)
{
  // Bridge: Publisher.Publish delivers a copy-once payload to every
  // Subscription in the same domain on the same topic. The ROS message is
  // serialized with weld_cdr_encode_members through the passed introspection
  // handle and the wire bytes go out via weld_ffi_pub_publish.
  const struct weld_pub_data * pd = NULL;
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  weld_cdr_status_t st;
  int rc = 0;
  (void)allocation;
  if (publisher == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publish null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publish wrong implementation");
  }
  pd = (const struct weld_pub_data *)publisher->data;
  if (pd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publish no publisher data");
    return RMW_RET_ERROR;
  }
  members = weld_resolve_members(pd->type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_publish type support is not introspection");
    return RMW_RET_ERROR;
  }
  errbuf[0] = '\0';
  st = weld_cdr_encode_members(
    members, ros_message, true, &wire, &wire_len, errbuf, sizeof(errbuf));
  if (st != WELD_CDR_OK) {
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publish encode failed");
    }
    free(wire);
    weld_pub_drop_add();
    return RMW_RET_ERROR;
  }
  if (wire_len > (size_t)INT_MAX) {
    free(wire);
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publish payload too large");
    weld_pub_drop_add();
    return RMW_RET_ERROR;
  }
  rc = weld_ffi_pub_publish(
    pd->handle, (const char *)wire, (int)wire_len);
  free(wire);
  if (rc != 0) {
    weld_pub_drop_add();
    return weld_fail_ffi("rmw_weld: rmw_publish backend failed", rc);
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_publish_serialized_message(
  const rmw_publisher_t * publisher,
  const rmw_serialized_message_t * serialized_message,
  rmw_publisher_allocation_t * allocation)
{
  // The CDR wire bytes pass through with copy semantics. No fields are
  // read here.
  const struct weld_pub_data * pd = NULL;
  int rc = 0;
  (void)allocation;
  if (publisher == NULL || serialized_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publish_serialized_message null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publish_serialized_message wrong implementation");
  }
  pd = (const struct weld_pub_data *)publisher->data;
  if (pd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_publish_serialized_message no data");
    return RMW_RET_ERROR;
  }
  if (serialized_message->buffer_length > 0 &&
    serialized_message->buffer == NULL)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_publish_serialized_message null buffer");
  }
  if (serialized_message->buffer_length > (size_t)INT_MAX) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_publish_serialized_message payload too large");
    return RMW_RET_ERROR;
  }
  rc = weld_ffi_pub_publish(
    pd->handle, (const char *)serialized_message->buffer,
    (int)serialized_message->buffer_length);
  if (rc != 0) {
    return weld_fail_ffi(
      "rmw_weld: rmw_publish_serialized_message backend failed", rc);
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_init_publisher_allocation(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds,
  rmw_publisher_allocation_t * allocation)
{
  if (type_support == NULL || message_bounds == NULL || allocation == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_init_publisher_allocation null argument");
  }
  (void)type_support;
  (void)message_bounds;
  (void)memset(allocation, 0, sizeof(*allocation));
  return RMW_RET_OK;
}

rmw_ret_t rmw_fini_publisher_allocation(rmw_publisher_allocation_t * allocation)
{
  if (allocation == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_fini_publisher_allocation null argument");
  }
  (void)memset(allocation, 0, sizeof(*allocation));
  return RMW_RET_OK;
}
