// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//
// weld_cdr: plain XCDR1 encode and decode driven by ROS 2 C introspection.
//
// The wire form mirrors internal/types/cdr.go: a 4 byte header of 0x00
// plus an endian flag plus two reserved bytes, alignment rebased after
// the header, strings as length plus NUL with no trailing pad, and a
// zero pad to a 4 byte boundary at the end. Only the decode entry
// points validate input. Encode assumes initialized messages.

#ifndef RMW_WELD_WELD_CDR_H_
#define RMW_WELD_WELD_CDR_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "rosidl_runtime_c/message_type_support_struct.h"
#include "rosidl_typesupport_introspection_c/message_introspection.h"

#ifdef __cplusplus
extern "C"
{
#endif

// Status codes for encode and decode. Zero means success.
typedef enum weld_cdr_status
{
  WELD_CDR_OK = 0,
  WELD_CDR_BAD_ARG = 1,
  WELD_CDR_NO_MEM = 2,
  WELD_CDR_TRUNCATED = 3,
  WELD_CDR_INVALID = 4,
  WELD_CDR_UNSUPPORTED = 5
} weld_cdr_status_t;

// Caps bound one length prefix so corrupt input cannot force huge use.
enum weld_cdr_caps
{
  WELD_CDR_MAX_STRING_BYTES = 33554432,
  WELD_CDR_MAX_SEQUENCE_ELEMS = 33554432,
  WELD_CDR_MAX_WSTRING_UNITS = 16777216
};

// Size of the 4 byte XCDR1 encapsulation header.
#define WELD_CDR_HEADER_SIZE 4

// Encode one message plus header into a malloc owned buffer.
//
// members describes the message, ros_message points at the C struct,
// little_endian selects the wire order. On success out holds the wire
// bytes including header and pad, and out_len holds the count. The
// caller frees out with free. errbuf takes a bounded message, or NULL.
weld_cdr_status_t weld_cdr_encode_members(
  const rosidl_typesupport_introspection_c__MessageMembers * members,
  const void * ros_message,
  bool little_endian,
  uint8_t ** out,
  size_t * out_len,
  char * errbuf,
  size_t errbuf_len);

// Decode one message including its header into ros_message.
//
// ros_message must already be initialized with the generated init
// function. Sequences are resized through the member callbacks and
// strings are assigned through the runtime helpers. Extra zero pad is
// accepted, any other trailing bytes fail.
weld_cdr_status_t weld_cdr_decode_members(
  const rosidl_typesupport_introspection_c__MessageMembers * members,
  const uint8_t * data,
  size_t size,
  void * ros_message,
  char * errbuf,
  size_t errbuf_len);

// Encode through a type support handle, resolving introspection first.
weld_cdr_status_t weld_cdr_encode(
  const rosidl_message_type_support_t * type_support,
  const void * ros_message,
  bool little_endian,
  uint8_t ** out,
  size_t * out_len,
  char * errbuf,
  size_t errbuf_len);

// Decode through a type support handle, resolving introspection first.
weld_cdr_status_t weld_cdr_decode(
  const rosidl_message_type_support_t * type_support,
  const uint8_t * data,
  size_t size,
  void * ros_message,
  char * errbuf,
  size_t errbuf_len);

#ifdef __cplusplus
}
#endif

#endif  // RMW_WELD_WELD_CDR_H_
