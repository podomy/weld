// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//

// qos: actual QoS, compatibility check, serialize support.

#include "weld_private.h"
rmw_ret_t rmw_publisher_get_actual_qos(
  const rmw_publisher_t * publisher, rmw_qos_profile_t * qos)
{
  // Actual QoS is the profile stored at creation. No Go mapping is
  // needed: the profile never changes after creation.
  if (publisher == NULL || qos == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_publisher_get_actual_qos null argument");
  }
  if (!weld_pub_ok(publisher)) {
    return weld_fail_impl("rmw_weld: rmw_publisher_get_actual_qos wrong implementation");
  }
  {
    const struct weld_pub_data * pd =
      (const struct weld_pub_data *)publisher->data;
    if (pd == NULL) {
      return weld_fail_invalid("rmw_weld: rmw_publisher_get_actual_qos no data");
    }
    *qos = pd->qos;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_subscription_get_actual_qos(
  const rmw_subscription_t * subscription, rmw_qos_profile_t * qos)
{
  // Actual QoS is the profile stored at creation. No Go mapping is
  // needed: the profile never changes after creation.
  if (subscription == NULL || qos == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_subscription_get_actual_qos null argument");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_subscription_get_actual_qos wrong implementation");
  }
  {
    const struct weld_sub_data * sd =
      (const struct weld_sub_data *)subscription->data;
    if (sd == NULL) {
      return weld_fail_invalid("rmw_weld: rmw_subscription_get_actual_qos no data");
    }
    *qos = sd->qos;
  }
  return RMW_RET_OK;
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
    // entry without touching the count. Without the FFI symbol the
    // answer stays local-only instead of failing: an older Go library
    // cannot see RTPS peers, but local servers are still exact.
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
      *is_available = (local > 0);
      return RMW_RET_OK;
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

rmw_ret_t rmw_service_request_subscription_get_actual_qos(
  const rmw_service_t * service, rmw_qos_profile_t * qos)
{
  if (service == NULL || qos == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_service_request_subscription_get_actual_qos null argument");
  }
  if (!weld_svc_ok(service)) {
    return weld_fail_impl(
      "rmw_weld: rmw_service_request_subscription_get_actual_qos wrong implementation");
  }
  {
    const struct weld_rpc_state * st =
      (const struct weld_rpc_state *)service->data;
    if (st == NULL) {
      return weld_fail_invalid(
        "rmw_weld: rmw_service_request_subscription_get_actual_qos no data");
    }
    *qos = st->qos;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_service_response_publisher_get_actual_qos(
  const rmw_service_t * service, rmw_qos_profile_t * qos)
{
  if (service == NULL || qos == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_service_response_publisher_get_actual_qos null argument");
  }
  if (!weld_svc_ok(service)) {
    return weld_fail_impl(
      "rmw_weld: rmw_service_response_publisher_get_actual_qos wrong implementation");
  }
  {
    const struct weld_rpc_state * st =
      (const struct weld_rpc_state *)service->data;
    if (st == NULL) {
      return weld_fail_invalid(
        "rmw_weld: rmw_service_response_publisher_get_actual_qos no data");
    }
    *qos = st->qos;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_client_request_publisher_get_actual_qos(
  const rmw_client_t * client, rmw_qos_profile_t * qos)
{
  if (client == NULL || qos == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_client_request_publisher_get_actual_qos null argument");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl(
      "rmw_weld: rmw_client_request_publisher_get_actual_qos wrong implementation");
  }
  {
    const struct weld_rpc_state * st =
      (const struct weld_rpc_state *)client->data;
    if (st == NULL) {
      return weld_fail_invalid(
        "rmw_weld: rmw_client_request_publisher_get_actual_qos no data");
    }
    *qos = st->qos;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_client_response_subscription_get_actual_qos(
  const rmw_client_t * client, rmw_qos_profile_t * qos)
{
  if (client == NULL || qos == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_client_response_subscription_get_actual_qos null argument");
  }
  if (!weld_cli_ok(client)) {
    return weld_fail_impl(
      "rmw_weld: rmw_client_response_subscription_get_actual_qos wrong implementation");
  }
  {
    const struct weld_rpc_state * st =
      (const struct weld_rpc_state *)client->data;
    if (st == NULL) {
      return weld_fail_invalid(
        "rmw_weld: rmw_client_response_subscription_get_actual_qos no data");
    }
    *qos = st->qos;
  }
  return RMW_RET_OK;
}

// QoS scope: this shim stores passthrough QoS and never contacts the
// Go core, so compatibility is decided locally on every policy:
// reliability, durability, deadline, lifespan, liveliness (plus lease
// duration), history, and depth. Any difference reports WARNING with
// the differing policies named in reason; the transport is best effort
// in memory, so no mismatch is ever a hard error. Lifespan is local
// per DDS 1.4 section 2.2.3 (data disposed when lifespan expires) and
// never blocks matching, hence WARNING not ERROR, mirroring the other
// policies here.
// Append one policy label to a comma separated mismatch list.
// Caps the write position so a truncated snprintf never pushes the next
// write past the buffer. Always marks the comparison mismatched.
static void weld_qos_flag(
  char * details, size_t cap, size_t * pos, bool * mismatch,
  const char * label)
{
  int n = 0;
  if (details == NULL || pos == NULL || mismatch == NULL || label == NULL) {
    return;
  }
  if (*pos < cap) {
    n = snprintf(
      details + *pos, cap - *pos, "%s%s", *pos > 0 ? ", " : "", label);
    if (n > 0) {
      *pos += (size_t)n;
      if (*pos >= cap) {
        *pos = cap - 1;
      }
    }
  }
  *mismatch = true;
}

rmw_ret_t rmw_qos_profile_check_compatible(
  const rmw_qos_profile_t publisher_profile,
  const rmw_qos_profile_t subscription_profile,
  rmw_qos_compatibility_type_t * compatibility, char * reason, size_t reason_size)
{
  if (compatibility == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_qos_profile_check_compatible null argument");
  }
  {
    char details[256];
    size_t pos = 0;
    bool mismatch = false;
    details[0] = '\0';
    if (publisher_profile.reliability != subscription_profile.reliability) {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "reliability");
    }
    if (publisher_profile.durability != subscription_profile.durability) {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "durability");
    }
    if (publisher_profile.deadline.sec != subscription_profile.deadline.sec ||
      publisher_profile.deadline.nsec != subscription_profile.deadline.nsec)
    {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "deadline");
    }
    if (publisher_profile.lifespan.sec != subscription_profile.lifespan.sec ||
      publisher_profile.lifespan.nsec != subscription_profile.lifespan.nsec)
    {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "lifespan");
    }
    if (publisher_profile.liveliness != subscription_profile.liveliness ||
      publisher_profile.liveliness_lease_duration.sec !=
      subscription_profile.liveliness_lease_duration.sec ||
      publisher_profile.liveliness_lease_duration.nsec !=
      subscription_profile.liveliness_lease_duration.nsec)
    {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "liveliness");
    }
    if (publisher_profile.history != subscription_profile.history) {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "history");
    }
    if (publisher_profile.depth != subscription_profile.depth) {
      weld_qos_flag(details, sizeof(details), &pos, &mismatch, "depth");
    }
    if (mismatch) {
      *compatibility = RMW_QOS_COMPATIBILITY_WARNING;
      if (reason != NULL && reason_size > 0) {
        (void)snprintf(
          reason, reason_size,
          "rmw_weld: publisher/subscriber QoS differ: %s", details);
        reason[reason_size - 1] = '\0';
      }
      return RMW_RET_OK;
    }
  }
  *compatibility = RMW_QOS_COMPATIBILITY_OK;
  if (reason != NULL && reason_size > 0) {
    reason[0] = '\0';
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_serialize(
  const void * ros_message, const rosidl_message_type_support_t * type_support,
  rmw_serialized_message_t * serialized_message)
{
  // CDR encode copies the wire bytes into the caller buffer, growing it
  // when the sample does not fit. The message must already be
  // initialized; encoding an uninitialized message is undefined, same as
  // the encoder contract.
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  char errbuf[256];
  weld_cdr_status_t st;
  rcutils_ret_t rc = RCUTILS_RET_OK;
  if (ros_message == NULL || type_support == NULL || serialized_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_serialize null argument");
  }
  members = weld_resolve_members(type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_serialize type support is not introspection");
    return RMW_RET_ERROR;
  }
  errbuf[0] = '\0';
  st = weld_cdr_encode_members(
    members, ros_message, true, &wire, &wire_len, errbuf, sizeof(errbuf));
  if (st != WELD_CDR_OK) {
    free(wire);
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_serialize encode failed");
    }
    return RMW_RET_ERROR;
  }
  if (wire_len > serialized_message->buffer_capacity) {
    rc = rmw_serialized_message_resize(serialized_message, wire_len);
    if (rc != RCUTILS_RET_OK) {
      free(wire);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_serialize resize failed");
      return RMW_RET_BAD_ALLOC;
    }
  }
  if (wire_len > 0) {
    if (serialized_message->buffer == NULL) {
      free(wire);
      rmw_reset_error();
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_serialize null buffer");
      return RMW_RET_INVALID_ARGUMENT;
    }
    (void)memcpy(serialized_message->buffer, wire, wire_len);
  }
  free(wire);
  serialized_message->buffer_length = wire_len;
  return RMW_RET_OK;
}

rmw_ret_t rmw_deserialize(
  const rmw_serialized_message_t * serialized_message,
  const rosidl_message_type_support_t * type_support, void * ros_message)
{
  // CDR decode copies the wire bytes into ros_message, which the caller
  // must have initialized with the generated init function. Extra zero
  // pad is accepted; any other trailing bytes fail loudly.
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  char errbuf[256];
  weld_cdr_status_t st;
  if (serialized_message == NULL || type_support == NULL || ros_message == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_deserialize null argument");
  }
  members = weld_resolve_members(type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_deserialize type support is not introspection");
    return RMW_RET_ERROR;
  }
  if (serialized_message->buffer_length > 0 && serialized_message->buffer == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_deserialize null buffer");
  }
  errbuf[0] = '\0';
  st = weld_cdr_decode_members(
    members, serialized_message->buffer, serialized_message->buffer_length,
    ros_message, errbuf, sizeof(errbuf));
  if (st != WELD_CDR_OK) {
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_deserialize decode failed");
    }
    return RMW_RET_ERROR;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_get_serialized_message_size(
  const rosidl_message_type_support_t * type_support,
  const rosidl_runtime_c__Sequence__bound * message_bounds, size_t * size)
{
  // The maximum encoding size comes from a member walk over upper
  // bounds, mirroring the encoder byte for byte. Unbounded members
  // have no finite size and fail loudly with UNSUPPORTED instead of
  // guessing. The bounds handle is required by the API but carries no
  // additional data the walk needs.
  const rosidl_typesupport_introspection_c__MessageMembers * members = NULL;
  char errbuf[256];
  weld_cdr_status_t st;
  (void)message_bounds;
  if (type_support == NULL || message_bounds == NULL || size == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_get_serialized_message_size null argument");
  }
  members = weld_resolve_members(type_support);
  if (members == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_get_serialized_message_size type support is not introspection");
    return RMW_RET_ERROR;
  }
  errbuf[0] = '\0';
  st = weld_cdr_max_serialized_size(members, size, errbuf, sizeof(errbuf));
  if (st != WELD_CDR_OK) {
    rmw_reset_error();
    if (errbuf[0] != '\0') {
      RCUTILS_SET_ERROR_MSG(errbuf);
    } else {
      RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_get_serialized_message_size failed");
    }
    return st == WELD_CDR_UNSUPPORTED ? RMW_RET_UNSUPPORTED : RMW_RET_ERROR;
  }
  return RMW_RET_OK;
}

// Context finalize plus serialization format plus GID compare plus log
// severity plus liveliness plus ack plus allocation plus take variants
// plus client GID. Liveliness asserts report to the Go core when the
// FFI symbol is linked and validate only otherwise; acks return
// immediately because the transport sends best effort with no
// acknowledgments.
