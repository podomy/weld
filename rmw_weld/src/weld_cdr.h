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

// Report the maximum encoded size for a message, including the 4 byte
// header and the final zero pad.
//
// Only bounded members have a finite size: unbounded strings,
// wstrings, and sequences report WELD_CDR_UNSUPPORTED, as does a long
// double that is not 16 bytes wide. On success out_size holds the byte
// count. errbuf takes a bounded message, or NULL.
weld_cdr_status_t weld_cdr_max_serialized_size(
  const rosidl_typesupport_introspection_c__MessageMembers * members,
  size_t * out_size,
  char * errbuf,
  size_t errbuf_len);

// XCDR extensibility kinds for Table 32 emit.
typedef enum weld_xt6b_ext
{
  WELD_xt6b_FINAL = 0,
  WELD_xt6b_APPENDABLE = 1,
  WELD_xt6b_MUTABLE = 2
} weld_xt6b_ext_t;

// XCDR wire versions.
typedef enum weld_xt6b_ver
{
  WELD_xt6b_V1 = 1,
  WELD_xt6b_V2 = 2
} weld_xt6b_ver_t;

// Table 57 representation IDs.
enum weld_xt6b_rep
{
  WELD_xt6b_CDR_BE = 0x0000,
  WELD_xt6b_CDR_LE = 0x0001,
  WELD_xt6b_PL_CDR_BE = 0x0002,
  WELD_xt6b_PL_CDR_LE = 0x0003,
  WELD_xt6b_CDR2_BE = 0x0006,
  WELD_xt6b_CDR2_LE = 0x0007,
  WELD_xt6b_D_CDR2_BE = 0x0008,
  WELD_xt6b_D_CDR2_LE = 0x0009,
  WELD_xt6b_PL_CDR2_BE = 0x000A,
  WELD_xt6b_PL_CDR2_LE = 0x000B
};

// Select the emitted ID for a kind plus version.
weld_cdr_status_t weld_cdr_xt6b_representation_for(
  weld_xt6b_ext_t ext,
  weld_xt6b_ver_t ver,
  bool little_endian,
  uint16_t * out);

// Decode a 4 byte header into kind plus version.
weld_cdr_status_t weld_cdr_xt6b_parse_encapsulation(
  const uint8_t hdr[4],
  weld_xt6b_ext_t * ext,
  weld_xt6b_ver_t * ver,
  bool * little_endian,
  uint16_t * representation);

// Build the 4 byte header for an emitted sample.
void weld_cdr_xt6b_make_encapsulation(
  uint8_t hdr[4],
  weld_xt6b_ext_t ext,
  bool little_endian);

// Report the max alignment for a version: 4 for V2, 8 for V1.
size_t weld_cdr_xt6b_maxalign(weld_xt6b_ver_t ver);

// Map a width to its V2 alignment: 8 and 16 become 4.
size_t weld_cdr_xt6b_align_width(size_t width);

// Report long double as unsupported.
weld_cdr_status_t weld_cdr_xt6b_check_long_double(void);

// Compare map keys of one IDL kind, negative, zero, or positive.
int weld_cdr_xt6b_cmp_i8(int8_t a, int8_t b);
int weld_cdr_xt6b_cmp_u8(uint8_t a, uint8_t b);
int weld_cdr_xt6b_cmp_i16(int16_t a, int16_t b);
int weld_cdr_xt6b_cmp_u16(uint16_t a, uint16_t b);
int weld_cdr_xt6b_cmp_i32(int32_t a, int32_t b);
int weld_cdr_xt6b_cmp_u32(uint32_t a, uint32_t b);
int weld_cdr_xt6b_cmp_i64(int64_t a, int64_t b);
int weld_cdr_xt6b_cmp_u64(uint64_t a, uint64_t b);
int weld_cdr_xt6b_cmp_str(const char * a, const char * b);
int weld_cdr_xt6b_cmp_wstr(const uint16_t * a, size_t an, const uint16_t * b, size_t bn);

// Encode a DHEADER delimiter holding the content size.
void weld_cdr_xt6b_dheader_encode(
  uint8_t out[4],
  uint32_t size,
  bool little_endian);

// Decode a DHEADER delimiter, reporting the content size.
weld_cdr_status_t weld_cdr_xt6b_dheader_decode(
  const uint8_t * data,
  size_t remaining,
  bool little_endian,
  uint32_t * out_size);

// Map a member size to its EMHEADER length code.
uint8_t weld_cdr_xt6b_length_code(uint32_t size);

// Build the first EMHEADER word from flag, code, and member id.
weld_cdr_status_t weld_cdr_xt6b_emheader_encode(
  uint32_t * out,
  bool must_understand,
  uint8_t lc,
  uint32_t member_id);

// Split an EMHEADER1 word into flag, code, and member id.
void weld_cdr_xt6b_emheader_decode(
  uint32_t v,
  bool * must_understand,
  uint8_t * lc,
  uint32_t * member_id);

// Report whether an unknown member must discard the sample.
weld_cdr_status_t weld_cdr_xt6b_must_understand(
  bool must_understand,
  uint32_t member_id);

#ifdef __cplusplus
}
#endif

#endif  // RMW_WELD_WELD_CDR_H_
