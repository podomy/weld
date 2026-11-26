// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: Apache-2.0
//
// weld_cdr: plain XCDR1 encode and decode driven by ROS 2 C introspection.
//
// Wire rules mirror internal/types/cdr.go. The 4 byte header is 0x00,
// the endian flag, 0x00, 0x00. Alignment is rebased after the header.
// Strings carry length plus NUL with no trailing pad. Bool is one byte
// holding 0 or 1. Wchar is 2 bytes. An empty message carries one dummy
// zero byte, and every sample is padded to 4 bytes at the end.
//
// Decode never trusts length prefixes. String and sequence lengths are
// capped at 32 MiB, missing NUL bytes fail, short input fails, and all
// failures return a status code. No path aborts or asserts.

#include "weld_cdr.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rosidl_runtime_c/string.h"
#include "rosidl_runtime_c/string_functions.h"
#include "rosidl_runtime_c/u16string.h"
#include "rosidl_runtime_c/u16string_functions.h"
#include "rosidl_typesupport_introspection_c/field_types.h"
#include "rosidl_typesupport_introspection_c/identifier.h"

// Short local names for the long introspection types.
typedef rosidl_typesupport_introspection_c__MessageMember weld_member_t;
typedef rosidl_typesupport_introspection_c__MessageMembers weld_members_t;

// Growable byte sink for the encoder.
struct weld_writer
{
  uint8_t * buf;
  size_t len;
  size_t cap;
  bool le;
  size_t payload_start;
  bool no_mem;
  char * err;
  size_t err_len;
};

// Bounded cursor for the decoder.
struct weld_reader
{
  const uint8_t * data;
  size_t size;
  size_t off;
  bool le;
  size_t payload_start;
  char * err;
  size_t err_len;
};

// Store a bounded error message, always NUL terminated.
static void set_err(char * buf, size_t len, const char * fmt, ...)
{
  va_list ap;

  if (buf == NULL || len == 0 || fmt == NULL) {
    return;
  }
  va_start(ap, fmt);
  (void)vsnprintf(buf, len, fmt, ap);
  va_end(ap);
  buf[len - 1] = '\0';
}

// Report true on little endian hosts using a byte probe.
static bool host_is_le(void)
{
  uint16_t probe = 1;
  uint8_t first = 0;

  (void)memcpy(&first, &probe, 1);
  return first == 1;
}

// Reverse 2, 4, and 8 byte values for big endian wire order.
static uint16_t swap16(uint16_t v)
{
  return (uint16_t)((uint16_t)(v << 8) | (uint16_t)(v >> 8));
}

// Reverse 4 byte values.
static uint32_t swap32(uint32_t v)
{
  return ((v & 0x000000FFU) << 24) |
    ((v & 0x0000FF00U) << 8) |
    ((v & 0x00FF0000U) >> 8) |
    ((v & 0xFF000000U) >> 24);
}

// Reverse 8 byte values.
static uint64_t swap64(uint64_t v)
{
  return ((v & 0x00000000000000FFULL) << 56) |
    ((v & 0x000000000000FF00ULL) << 40) |
    ((v & 0x0000000000FF0000ULL) << 24) |
    ((v & 0x00000000FF000000ULL) << 8) |
    ((v & 0x000000FF00000000ULL) >> 8) |
    ((v & 0x0000FF0000000000ULL) >> 24) |
    ((v & 0x00FF000000000000ULL) >> 40) |
    ((v & 0xFF00000000000000ULL) >> 56);
}

// Return the pad needed to move absolute offset off to a multiple of n.
static size_t pad_to(size_t off, size_t n)
{
  return (n - (off % n)) % n;
}

// Reserve room for extra bytes, growing by doubling with overflow caps.
static bool writer_reserve(struct weld_writer * w, size_t extra)
{
  size_t need = 0;
  size_t cap = 0;
  uint8_t * next = NULL;

  if (w->no_mem) {
    return false;
  }
  if (extra > (size_t)-1 - w->len) {
    w->no_mem = true;
    set_err(w->err, w->err_len, "weld: buffer size overflows");
    return false;
  }
  need = w->len + extra;
  if (need <= w->cap) {
    return true;
  }
  cap = w->cap != 0 ? w->cap : 128;
  while (cap < need) {
    if (cap > ((size_t)-1) / 2) {
      cap = need;
      break;
    }
    cap *= 2;
  }
  next = (uint8_t *)realloc(w->buf, cap);
  if (next == NULL) {
    w->no_mem = true;
    set_err(w->err, w->err_len, "weld: out of memory");
    return false;
  }
  w->buf = next;
  w->cap = cap;
  return true;
}

// Append raw bytes, recording an out of memory sticky flag on failure.
static void writer_bytes(struct weld_writer * w, const void * src, size_t n)
{
  if (n == 0) {
    return;
  }
  if (src == NULL) {
    w->no_mem = true;
    set_err(w->err, w->err_len, "weld: null source bytes");
    return;
  }
  if (!writer_reserve(w, n)) {
    return;
  }
  (void)memcpy(w->buf + w->len, src, n);
  w->len += n;
}

// Pad the buffer until the payload relative offset is a multiple of n.
static bool writer_align(struct weld_writer * w, size_t n)
{
  size_t pad = 0;
  size_t i = 0;

  if (n != 1 && n != 2 && n != 4 && n != 8) {
    set_err(w->err, w->err_len, "weld: bad align %lu", (unsigned long)n);
    return false;
  }
  pad = pad_to(w->len - w->payload_start, n);
  for (i = 0; i < pad; i++) {
    if (!writer_reserve(w, 1)) {
      return false;
    }
    w->buf[w->len] = 0x00;
    w->len++;
  }
  return true;
}

// Append one raw byte.
static void writer_byte(struct weld_writer * w, uint8_t v)
{
  if (!writer_reserve(w, 1)) {
    return;
  }
  w->buf[w->len] = v;
  w->len++;
}

// Append a 2 byte value with 2 byte alignment in wire order.
static bool writer_u16(struct weld_writer * w, uint16_t v)
{
  uint8_t raw[2];
  uint16_t wire = 0;

  if (!writer_align(w, 2)) {
    return false;
  }
  wire = (w->le == host_is_le()) ? v : swap16(v);
  (void)memcpy(raw, &wire, 2);
  writer_bytes(w, raw, 2);
  return !w->no_mem;
}

// Append a 4 byte value with 4 byte alignment in wire order.
static bool writer_u32(struct weld_writer * w, uint32_t v)
{
  uint8_t raw[4];
  uint32_t wire = 0;

  if (!writer_align(w, 4)) {
    return false;
  }
  wire = (w->le == host_is_le()) ? v : swap32(v);
  (void)memcpy(raw, &wire, 4);
  writer_bytes(w, raw, 4);
  return !w->no_mem;
}

// Append an 8 byte value with 8 byte alignment in wire order.
static bool writer_u64(struct weld_writer * w, uint64_t v)
{
  uint8_t raw[8];
  uint64_t wire = 0;

  if (!writer_align(w, 8)) {
    return false;
  }
  wire = (w->le == host_is_le()) ? v : swap64(v);
  (void)memcpy(raw, &wire, 8);
  writer_bytes(w, raw, 8);
  return !w->no_mem;
}

// Resolve a nested member handle to its introspection table.
static const weld_members_t * sub_table(
  const weld_member_t * m, char * err, size_t err_len)
{
  const rosidl_message_type_support_t * h = NULL;

  if (m->members_ == NULL) {
    set_err(err, err_len, "weld: member %s has no sub table", m->name_);
    return NULL;
  }
  h = get_message_typesupport_handle(
    m->members_, rosidl_typesupport_introspection_c__identifier);
  if (h == NULL || h->data == NULL) {
    set_err(err, err_len, "weld: member %s is not introspection", m->name_);
    return NULL;
  }
  return (const weld_members_t *)h->data;
}

// Resolve a top level handle to its introspection table.
static const weld_members_t * top_table(
  const rosidl_message_type_support_t * ts, char * err, size_t err_len)
{
  const rosidl_message_type_support_t * h = NULL;

  if (ts == NULL) {
    set_err(err, err_len, "weld: null type support");
    return NULL;
  }
  h = get_message_typesupport_handle(
    ts, rosidl_typesupport_introspection_c__identifier);
  if (h == NULL || h->data == NULL) {
    set_err(err, err_len, "weld: type support is not introspection");
    return NULL;
  }
  return (const weld_members_t *)h->data;
}

// Count UTF-8 code points tolerantly by skipping continuation bytes.
static size_t count_runes(const char * s, size_t n)
{
  size_t out = 0;
  size_t i = 0;

  for (i = 0; i < n; i++) {
    unsigned char c = (unsigned char)s[i];
    if ((c & 0xC0U) != 0x80U) {
      out++;
    }
  }
  return out;
}

// Count UTF-16 code points, folding each lead plus trail pair into one.
static size_t count_utf16(const uint16_t * u, size_t n)
{
  size_t out = 0;
  size_t i = 0;

  for (i = 0; i < n; i++) {
    if (u[i] >= 0xDC00U && u[i] <= 0xDFFFU) {
      continue;
    }
    out++;
  }
  return out;
}

// Forward declaration for recursion through nested messages.
static weld_cdr_status_t encode_body(
  const weld_members_t * m, const uint8_t * base, struct weld_writer * w);

// Encode one scalar or string element already stripped of array framing.
static weld_cdr_status_t encode_one(
  const weld_member_t * m, const void * elem, struct weld_writer * w)
{
  const weld_members_t * sub = NULL;
  float f = 0.0f;
  double d = 0.0;
  uint32_t bits32 = 0;
  uint64_t bits64 = 0;

  switch (m->type_id_) {
    case rosidl_typesupport_introspection_c__ROS_TYPE_FLOAT:
      (void)memcpy(&f, elem, sizeof(f));
      (void)memcpy(&bits32, &f, sizeof(bits32));
      if (!writer_u32(w, bits32)) {
        return WELD_CDR_NO_MEM;
      }
      return WELD_CDR_OK;
    case rosidl_typesupport_introspection_c__ROS_TYPE_DOUBLE:
      (void)memcpy(&d, elem, sizeof(d));
      (void)memcpy(&bits64, &d, sizeof(bits64));
      if (!writer_u64(w, bits64)) {
        return WELD_CDR_NO_MEM;
      }
      return WELD_CDR_OK;
    case rosidl_typesupport_introspection_c__ROS_TYPE_LONG_DOUBLE:
      if (sizeof(long double) != 16) {
        set_err(w->err, w->err_len, "weld: long double width is not 16");
        return WELD_CDR_UNSUPPORTED;
      }
      if (w->le != host_is_le()) {
        set_err(w->err, w->err_len, "weld: long double needs host order");
        return WELD_CDR_UNSUPPORTED;
      }
      if (!writer_align(w, 8)) {
        return WELD_CDR_NO_MEM;
      }
      writer_bytes(w, elem, 16);
      return w->no_mem ? WELD_CDR_NO_MEM : WELD_CDR_OK;
    case rosidl_typesupport_introspection_c__ROS_TYPE_CHAR:
    case rosidl_typesupport_introspection_c__ROS_TYPE_OCTET:
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT8:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT8: {
      uint8_t v = 0;
      (void)memcpy(&v, elem, 1);
      writer_byte(w, v);
      return w->no_mem ? WELD_CDR_NO_MEM : WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_BOOLEAN: {
      bool v = false;
      (void)memcpy(&v, elem, sizeof(v));
      writer_byte(w, v ? 0x01 : 0x00);
      return w->no_mem ? WELD_CDR_NO_MEM : WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_WCHAR:
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT16:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT16: {
      uint16_t v = 0;
      (void)memcpy(&v, elem, sizeof(v));
      if (!writer_u16(w, v)) {
        return WELD_CDR_NO_MEM;
      }
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT32:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT32: {
      uint32_t v = 0;
      (void)memcpy(&v, elem, sizeof(v));
      if (!writer_u32(w, v)) {
        return WELD_CDR_NO_MEM;
      }
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT64:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT64: {
      uint64_t v = 0;
      (void)memcpy(&v, elem, sizeof(v));
      if (!writer_u64(w, v)) {
        return WELD_CDR_NO_MEM;
      }
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_STRING: {
      const rosidl_runtime_c__String * s =
        (const rosidl_runtime_c__String *)elem;
      const char * data = (s->data != NULL) ? s->data : "";
      size_t len = (s->data != NULL) ? s->size : 0;
      uint64_t total = (uint64_t)len + 1;
      if (total > (uint64_t)0xFFFFFFFFU) {
        set_err(w->err, w->err_len, "weld: string overflows uint32");
        return WELD_CDR_INVALID;
      }
      if (m->string_upper_bound_ > 0 && len > m->string_upper_bound_) {
        set_err(w->err, w->err_len, "weld: string over bound");
        return WELD_CDR_INVALID;
      }
      if (!writer_u32(w, (uint32_t)total)) {
        return WELD_CDR_NO_MEM;
      }
      writer_bytes(w, data, len);
      writer_byte(w, 0x00);
      return w->no_mem ? WELD_CDR_NO_MEM : WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_WSTRING: {
      const rosidl_runtime_c__U16String * s =
        (const rosidl_runtime_c__U16String *)elem;
      size_t units = (s->data != NULL) ? s->size : 0;
      uint64_t total = (uint64_t)units + 1;
      size_t i = 0;
      if (total > (uint64_t)0xFFFFFFFFU) {
        set_err(w->err, w->err_len, "weld: wstring overflows uint32");
        return WELD_CDR_INVALID;
      }
      if (m->string_upper_bound_ > 0 && units > m->string_upper_bound_) {
        set_err(w->err, w->err_len, "weld: wstring over bound");
        return WELD_CDR_INVALID;
      }
      if (!writer_u32(w, (uint32_t)total)) {
        return WELD_CDR_NO_MEM;
      }
      // Units are packed with no per unit alignment, like encodeWString.
      if (!writer_reserve(w, (units + 1) * 2)) {
        return WELD_CDR_NO_MEM;
      }
      for (i = 0; i < units; i++) {
        uint16_t v = (uint16_t)s->data[i];
        uint16_t wire = (w->le == host_is_le()) ? v : swap16(v);
        (void)memcpy(w->buf + w->len, &wire, 2);
        w->len += 2;
      }
      (void)memset(w->buf + w->len, 0, 2);
      w->len += 2;
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_MESSAGE:
      sub = sub_table(m, w->err, w->err_len);
      if (sub == NULL) {
        return WELD_CDR_INVALID;
      }
      if (sub->member_count_ == 0) {
        // A shared encoder has no per message finalize step, so an
        // empty nested struct contributes one dummy byte inline.
        writer_byte(w, 0x00);
        return w->no_mem ? WELD_CDR_NO_MEM : WELD_CDR_OK;
      }
      return encode_body(sub, (const uint8_t *)elem, w);
    default:
      break;
  }
  set_err(w->err, w->err_len, "weld: unknown type %u", m->type_id_);
  return WELD_CDR_UNSUPPORTED;
}

// Report true for members framed as sequences rather than fixed arrays.
static bool is_sequence(const weld_member_t * m)
{
  return m->is_array_ && (m->is_upper_bound_ || m->array_size_ == 0);
}

// Encode one member, dispatching scalar, sequence, and fixed array.
static weld_cdr_status_t encode_member(
  const weld_member_t * m, const uint8_t * field, struct weld_writer * w)
{
  size_t n = 0;
  size_t i = 0;

  if (!m->is_array_) {
    return encode_one(m, field, w);
  }
  if (is_sequence(m)) {
    if (m->size_function == NULL || m->get_const_function == NULL) {
      set_err(w->err, w->err_len, "weld: member %s lacks callbacks", m->name_);
      return WELD_CDR_UNSUPPORTED;
    }
    n = m->size_function(field);
    if (m->is_upper_bound_ && n > m->array_size_) {
      set_err(w->err, w->err_len, "weld: member %s exceeds bound", m->name_);
      return WELD_CDR_INVALID;
    }
    if (n > WELD_CDR_MAX_SEQUENCE_ELEMS) {
      set_err(w->err, w->err_len, "weld: member %s length over cap", m->name_);
      return WELD_CDR_INVALID;
    }
    if (!writer_u32(w, (uint32_t)n)) {
      return WELD_CDR_NO_MEM;
    }
    for (i = 0; i < n; i++) {
      const void * elem = m->get_const_function(field, i);
      weld_cdr_status_t st = WELD_CDR_OK;
      if (elem == NULL) {
        set_err(w->err, w->err_len, "weld: member %s null elem", m->name_);
        return WELD_CDR_INVALID;
      }
      st = encode_one(m, elem, w);
      if (st != WELD_CDR_OK) {
        return st;
      }
    }
    return WELD_CDR_OK;
  }
  for (i = 0; i < m->array_size_; i++) {
    const void * elem = NULL;
    weld_cdr_status_t st = WELD_CDR_OK;
    if (m->get_const_function != NULL) {
      elem = m->get_const_function(field, i);
    } else {
      // No callback: only plain numeric widths have a known stride.
      set_err(w->err, w->err_len, "weld: member %s lacks getter", m->name_);
      return WELD_CDR_UNSUPPORTED;
    }
    if (elem == NULL) {
      set_err(w->err, w->err_len, "weld: member %s null elem", m->name_);
      return WELD_CDR_INVALID;
    }
    st = encode_one(m, elem, w);
    if (st != WELD_CDR_OK) {
      return st;
    }
  }
  return WELD_CDR_OK;
}

// Encode every member of one message struct in declaration order.
static weld_cdr_status_t encode_body(
  const weld_members_t * m, const uint8_t * base, struct weld_writer * w)
{
  uint32_t i = 0;

  for (i = 0; i < m->member_count_; i++) {
    const weld_member_t * mem = &m->members_[i];
    weld_cdr_status_t st = encode_member(mem, base + mem->offset_, w);
    if (st != WELD_CDR_OK) {
      return st;
    }
  }
  return WELD_CDR_OK;
}

weld_cdr_status_t weld_cdr_encode_members(
  const rosidl_typesupport_introspection_c__MessageMembers * members,
  const void * ros_message,
  bool little_endian,
  uint8_t ** out,
  size_t * out_len,
  char * errbuf,
  size_t errbuf_len)
{
  struct weld_writer w;
  weld_cdr_status_t st = WELD_CDR_OK;
  size_t pad = 0;
  uint8_t header[WELD_CDR_HEADER_SIZE];

  if (members == NULL || ros_message == NULL || out == NULL ||
    out_len == NULL)
  {
    set_err(errbuf, errbuf_len, "weld: null encode argument");
    return WELD_CDR_BAD_ARG;
  }
  (void)memset(&w, 0, sizeof(w));
  w.le = little_endian;
  w.payload_start = WELD_CDR_HEADER_SIZE;
  w.err = errbuf;
  w.err_len = errbuf_len;
  weld_cdr_xt6b_make_encapsulation(header, WELD_xt6b_FINAL, little_endian);
  writer_bytes(&w, header, sizeof(header));
  if (w.no_mem) {
    free(w.buf);
    return WELD_CDR_NO_MEM;
  }
  st = encode_body(members, (const uint8_t *)ros_message, &w);
  if (st != WELD_CDR_OK) {
    free(w.buf);
    return st;
  }
  if (w.len == w.payload_start) {
    writer_byte(&w, 0x00);
    if (w.no_mem) {
      free(w.buf);
      return WELD_CDR_NO_MEM;
    }
  }
  pad = pad_to(w.len, 4);
  if (pad > 0) {
    if (!writer_reserve(&w, pad)) {
      free(w.buf);
      return WELD_CDR_NO_MEM;
    }
    (void)memset(w.buf + w.len, 0, pad);
    w.len += pad;
  }
  *out = w.buf;
  *out_len = w.len;
  return WELD_CDR_OK;
}

// Fail with truncated when n bytes are not available at the cursor.
static bool reader_need(struct weld_reader * r, size_t n)
{
  if (n > r->size - r->off) {
    set_err(r->err, r->err_len, "weld: truncated need %lu have %lu",
      (unsigned long)n, (unsigned long)(r->size - r->off));
    return false;
  }
  return true;
}

// Advance the cursor to a multiple of n relative to the payload start.
static bool reader_align(struct weld_reader * r, size_t n)
{
  size_t pad = 0;

  if (n != 1 && n != 2 && n != 4 && n != 8) {
    set_err(r->err, r->err_len, "weld: bad align %lu", (unsigned long)n);
    return false;
  }
  pad = pad_to(r->off - r->payload_start, n);
  if (!reader_need(r, pad)) {
    return false;
  }
  r->off += pad;
  return true;
}

// Read one raw byte.
static bool reader_byte(struct weld_reader * r, uint8_t * v)
{
  if (!reader_need(r, 1)) {
    return false;
  }
  *v = r->data[r->off];
  r->off++;
  return true;
}

// Read a 2 byte value with 2 byte alignment in host order.
static bool reader_u16(struct weld_reader * r, uint16_t * v)
{
  uint16_t wire = 0;

  if (!reader_align(r, 2)) {
    return false;
  }
  if (!reader_need(r, 2)) {
    return false;
  }
  (void)memcpy(&wire, r->data + r->off, 2);
  r->off += 2;
  *v = (r->le == host_is_le()) ? wire : swap16(wire);
  return true;
}

// Read a 4 byte value with 4 byte alignment in host order.
static bool reader_u32(struct weld_reader * r, uint32_t * v)
{
  uint32_t wire = 0;

  if (!reader_align(r, 4)) {
    return false;
  }
  if (!reader_need(r, 4)) {
    return false;
  }
  (void)memcpy(&wire, r->data + r->off, 4);
  r->off += 4;
  *v = (r->le == host_is_le()) ? wire : swap32(wire);
  return true;
}

// Read an 8 byte value with 8 byte alignment in host order.
static bool reader_u64(struct weld_reader * r, uint64_t * v)
{
  uint64_t wire = 0;

  if (!reader_align(r, 8)) {
    return false;
  }
  if (!reader_need(r, 8)) {
    return false;
  }
  (void)memcpy(&wire, r->data + r->off, 8);
  r->off += 8;
  *v = (r->le == host_is_le()) ? wire : swap64(wire);
  return true;
}

// Forward declaration for recursion through nested messages.
static weld_cdr_status_t decode_body(
  const weld_members_t * m, uint8_t * base, struct weld_reader * r);

// Report the storage width used for the allocation guard, if known.
static bool guard_width(
  const weld_member_t * m, const weld_members_t * sub, size_t * width)
{
  switch (m->type_id_) {
    case rosidl_typesupport_introspection_c__ROS_TYPE_CHAR:
    case rosidl_typesupport_introspection_c__ROS_TYPE_BOOLEAN:
    case rosidl_typesupport_introspection_c__ROS_TYPE_OCTET:
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT8:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT8:
      *width = 1;
      return true;
    case rosidl_typesupport_introspection_c__ROS_TYPE_WCHAR:
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT16:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT16:
      *width = 2;
      return true;
    case rosidl_typesupport_introspection_c__ROS_TYPE_FLOAT:
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT32:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT32:
      *width = 4;
      return true;
    case rosidl_typesupport_introspection_c__ROS_TYPE_DOUBLE:
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT64:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT64:
      *width = 8;
      return true;
    case rosidl_typesupport_introspection_c__ROS_TYPE_LONG_DOUBLE:
      *width = 16;
      return true;
    case rosidl_typesupport_introspection_c__ROS_TYPE_STRING:
      *width = sizeof(rosidl_runtime_c__String);
      return true;
    case rosidl_typesupport_introspection_c__ROS_TYPE_WSTRING:
      *width = sizeof(rosidl_runtime_c__U16String);
      return true;
    case rosidl_typesupport_introspection_c__ROS_TYPE_MESSAGE:
      if (sub != NULL && sub->size_of_ > 0) {
        *width = sub->size_of_;
        return true;
      }
      return false;
    default:
      break;
  }
  return false;
}

// Decode one scalar or string element already stripped of array framing.
static weld_cdr_status_t decode_one(
  const weld_member_t * m, void * elem, struct weld_reader * r)
{
  const weld_members_t * sub = NULL;

  switch (m->type_id_) {
    case rosidl_typesupport_introspection_c__ROS_TYPE_FLOAT: {
      uint32_t bits = 0;
      float f = 0.0f;
      if (!reader_u32(r, &bits)) {
        return WELD_CDR_TRUNCATED;
      }
      (void)memcpy(&f, &bits, sizeof(f));
      (void)memcpy(elem, &f, sizeof(f));
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_DOUBLE: {
      uint64_t bits = 0;
      double d = 0.0;
      if (!reader_u64(r, &bits)) {
        return WELD_CDR_TRUNCATED;
      }
      (void)memcpy(&d, &bits, sizeof(d));
      (void)memcpy(elem, &d, sizeof(d));
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_LONG_DOUBLE: {
      long double v = 0.0L;
      if (sizeof(long double) != 16) {
        set_err(r->err, r->err_len, "weld: long double width is not 16");
        return WELD_CDR_UNSUPPORTED;
      }
      if (r->le != host_is_le()) {
        set_err(r->err, r->err_len, "weld: long double needs host order");
        return WELD_CDR_UNSUPPORTED;
      }
      if (!reader_align(r, 8)) {
        return WELD_CDR_TRUNCATED;
      }
      if (!reader_need(r, 16)) {
        return WELD_CDR_TRUNCATED;
      }
      (void)memcpy(&v, r->data + r->off, 16);
      r->off += 16;
      (void)memcpy(elem, &v, sizeof(v));
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_CHAR:
    case rosidl_typesupport_introspection_c__ROS_TYPE_OCTET:
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT8:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT8: {
      uint8_t v = 0;
      if (!reader_byte(r, &v)) {
        return WELD_CDR_TRUNCATED;
      }
      (void)memcpy(elem, &v, 1);
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_BOOLEAN: {
      uint8_t v = 0;
      bool b = false;
      if (!reader_byte(r, &v)) {
        return WELD_CDR_TRUNCATED;
      }
      if (v != 0x00 && v != 0x01) {
        set_err(r->err, r->err_len, "weld: bad bool byte 0x%02x", v);
        return WELD_CDR_INVALID;
      }
      b = v == 0x01;
      (void)memcpy(elem, &b, sizeof(b));
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_WCHAR:
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT16:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT16: {
      uint16_t v = 0;
      if (!reader_u16(r, &v)) {
        return WELD_CDR_TRUNCATED;
      }
      (void)memcpy(elem, &v, sizeof(v));
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT32:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT32: {
      uint32_t v = 0;
      if (!reader_u32(r, &v)) {
        return WELD_CDR_TRUNCATED;
      }
      (void)memcpy(elem, &v, sizeof(v));
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_UINT64:
    case rosidl_typesupport_introspection_c__ROS_TYPE_INT64: {
      uint64_t v = 0;
      if (!reader_u64(r, &v)) {
        return WELD_CDR_TRUNCATED;
      }
      (void)memcpy(elem, &v, sizeof(v));
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_STRING: {
      rosidl_runtime_c__String * s = (rosidl_runtime_c__String *)elem;
      uint32_t total = 0;
      if (!reader_u32(r, &total)) {
        return WELD_CDR_TRUNCATED;
      }
      if (total == 0) {
        set_err(r->err, r->err_len, "weld: string misses NUL");
        return WELD_CDR_INVALID;
      }
      if (total > WELD_CDR_MAX_STRING_BYTES) {
        set_err(r->err, r->err_len, "weld: string length over cap");
        return WELD_CDR_INVALID;
      }
      if (!reader_need(r, total)) {
        return WELD_CDR_TRUNCATED;
      }
      if (r->data[r->off + total - 1] != 0x00) {
        set_err(r->err, r->err_len, "weld: string misses NUL");
        return WELD_CDR_INVALID;
      }
      if (!rosidl_runtime_c__String__assignn(s,
        (const char *)(r->data + r->off), (size_t)total - 1))
      {
        set_err(r->err, r->err_len, "weld: string assign failed");
        return WELD_CDR_NO_MEM;
      }
      r->off += total;
      if (m->string_upper_bound_ > 0) {
        if (s->size > m->string_upper_bound_) {
          set_err(r->err, r->err_len, "weld: string over bound");
          return WELD_CDR_INVALID;
        }
      }
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_WSTRING: {
      rosidl_runtime_c__U16String * s = (rosidl_runtime_c__U16String *)elem;
      uint32_t total = 0;
      uint64_t bytes = 0;
      uint16_t * tmp = NULL;
      size_t units = 0;
      uint16_t last = 0;
      size_t i = 0;
      bool ok = false;
      if (!reader_u32(r, &total)) {
        return WELD_CDR_TRUNCATED;
      }
      if (total == 0) {
        set_err(r->err, r->err_len, "weld: wstring misses NUL");
        return WELD_CDR_INVALID;
      }
      if (total > WELD_CDR_MAX_WSTRING_UNITS) {
        set_err(r->err, r->err_len, "weld: wstring length over cap");
        return WELD_CDR_INVALID;
      }
      bytes = (uint64_t)total * 2;
      if (!reader_need(r, (size_t)bytes)) {
        return WELD_CDR_TRUNCATED;
      }
      units = (size_t)total - 1;
      tmp = (uint16_t *)malloc((size_t)bytes);
      if (tmp == NULL && bytes > 0) {
        set_err(r->err, r->err_len, "weld: out of memory");
        return WELD_CDR_NO_MEM;
      }
      for (i = 0; i < (size_t)total; i++) {
        uint16_t wire = 0;
        (void)memcpy(&wire, r->data + r->off + i * 2, 2);
        tmp[i] = (r->le == host_is_le()) ? wire : swap16(wire);
      }
      last = tmp[(size_t)total - 1];
      if (last != 0) {
        free(tmp);
        set_err(r->err, r->err_len, "weld: wstring misses NUL");
        return WELD_CDR_INVALID;
      }
      ok = rosidl_runtime_c__U16String__assignn(s, tmp, units);
      free(tmp);
      if (!ok) {
        set_err(r->err, r->err_len, "weld: wstring assign failed");
        return WELD_CDR_NO_MEM;
      }
      r->off += (size_t)bytes;
      if (m->string_upper_bound_ > 0) {
        if (s->size > m->string_upper_bound_) {
          set_err(r->err, r->err_len, "weld: wstring over bound");
          return WELD_CDR_INVALID;
        }
      }
      return WELD_CDR_OK;
    }
    case rosidl_typesupport_introspection_c__ROS_TYPE_MESSAGE:
      sub = sub_table(m, r->err, r->err_len);
      if (sub == NULL) {
        return WELD_CDR_INVALID;
      }
      if (sub->member_count_ == 0) {
        uint8_t dummy = 0;
        if (!reader_byte(r, &dummy)) {
          return WELD_CDR_TRUNCATED;
        }
        if (dummy != 0x00) {
          set_err(r->err, r->err_len, "weld: bad empty dummy");
          return WELD_CDR_INVALID;
        }
        return WELD_CDR_OK;
      }
      return decode_body(sub, (uint8_t *)elem, r);
    default:
      break;
  }
  set_err(r->err, r->err_len, "weld: unknown type %u", m->type_id_);
  return WELD_CDR_UNSUPPORTED;
}

// Decode one member, dispatching scalar, sequence, and fixed array.
static weld_cdr_status_t decode_member(
  const weld_member_t * m, uint8_t * field, struct weld_reader * r)
{
  uint32_t n = 0;
  size_t count = 0;
  size_t i = 0;

  if (!m->is_array_) {
    return decode_one(m, field, r);
  }
  if (is_sequence(m)) {
    const weld_members_t * sub = NULL;
    size_t width = 0;
    bool known = false;
    if (m->resize_function == NULL || m->get_function == NULL) {
      set_err(r->err, r->err_len, "weld: member %s lacks callbacks", m->name_);
      return WELD_CDR_UNSUPPORTED;
    }
    if (m->type_id_ ==
      rosidl_typesupport_introspection_c__ROS_TYPE_MESSAGE)
    {
      sub = sub_table(m, r->err, r->err_len);
      if (sub == NULL) {
        return WELD_CDR_INVALID;
      }
    }
    if (!reader_u32(r, &n)) {
      return WELD_CDR_TRUNCATED;
    }
    if (n > WELD_CDR_MAX_SEQUENCE_ELEMS) {
      set_err(r->err, r->err_len, "weld: member %s length over cap",
        m->name_);
      return WELD_CDR_INVALID;
    }
    if (m->is_upper_bound_ && (size_t)n > m->array_size_) {
      set_err(r->err, r->err_len, "weld: member %s length over bound",
        m->name_);
      return WELD_CDR_INVALID;
    }
    // Guard the resize against corrupt counts that fit the cap but
    // exceed the wire, mirroring CheckSequenceAllocation in cdr.go.
    known = guard_width(m, sub, &width);
    if (known && width > 0 && n > 0) {
      uint64_t total = (uint64_t)n * (uint64_t)width;
      if (total > (uint64_t)(r->size - r->off)) {
        set_err(r->err, r->err_len, "weld: member %s needs %llu have %lu",
          m->name_, (unsigned long long)total,
          (unsigned long)(r->size - r->off));
        return WELD_CDR_TRUNCATED;
      }
    }
    if (!m->resize_function(field, (size_t)n)) {
      set_err(r->err, r->err_len, "weld: member %s resize failed",
        m->name_);
      return WELD_CDR_NO_MEM;
    }
    count = (size_t)n;
    for (i = 0; i < count; i++) {
      void * elem = m->get_function(field, i);
      weld_cdr_status_t st = WELD_CDR_OK;
      if (elem == NULL) {
        set_err(r->err, r->err_len, "weld: member %s null elem", m->name_);
        return WELD_CDR_INVALID;
      }
      st = decode_one(m, elem, r);
      if (st != WELD_CDR_OK) {
        return st;
      }
    }
    return WELD_CDR_OK;
  }
  for (i = 0; i < m->array_size_; i++) {
    void * elem = NULL;
    weld_cdr_status_t st = WELD_CDR_OK;
    if (m->get_function == NULL) {
      set_err(r->err, r->err_len, "weld: member %s lacks getter", m->name_);
      return WELD_CDR_UNSUPPORTED;
    }
    elem = m->get_function(field, i);
    if (elem == NULL) {
      set_err(r->err, r->err_len, "weld: member %s null elem", m->name_);
      return WELD_CDR_INVALID;
    }
    st = decode_one(m, elem, r);
    if (st != WELD_CDR_OK) {
      return st;
    }
  }
  return WELD_CDR_OK;
}

// Decode every member of one message struct in declaration order.
static weld_cdr_status_t decode_body(
  const weld_members_t * m, uint8_t * base, struct weld_reader * r)
{
  uint32_t i = 0;

  for (i = 0; i < m->member_count_; i++) {
    const weld_member_t * mem = &m->members_[i];
    weld_cdr_status_t st = decode_member(mem, base + mem->offset_, r);
    if (st != WELD_CDR_OK) {
      return st;
    }
  }
  return WELD_CDR_OK;
}

// Report true when every byte in the span is zero.
static bool all_zero(const uint8_t * p, size_t n)
{
  size_t i = 0;

  for (i = 0; i < n; i++) {
    if (p[i] != 0x00) {
      return false;
    }
  }
  return true;
}

// Accept the final zero pad, and the plain empty dummy plus pad.
static weld_cdr_status_t reader_finish_top(
  const weld_members_t * m, struct weld_reader * r)
{
  size_t rem = r->size - r->off;
  size_t want = 0;
  size_t expected = 0;

  (void)m;
  if (rem == 0) {
    return WELD_CDR_OK;
  }
  if (r->off == r->payload_start &&
    r->payload_start == WELD_CDR_HEADER_SIZE)
  {
    // Plain empty payload: one dummy byte plus pad to 4, all zero.
    want = 1 + pad_to(r->off + 1, 4);
    if (rem == want && all_zero(r->data + r->off, rem)) {
      r->off = r->size;
      return WELD_CDR_OK;
    }
  }
  expected = pad_to(r->off - r->payload_start, 4);
  if (rem == expected && all_zero(r->data + r->off, rem)) {
    r->off = r->size;
    return WELD_CDR_OK;
  }
  set_err(r->err, r->err_len, "weld: %lu trailing bytes",
    (unsigned long)rem);
  return WELD_CDR_INVALID;
}

weld_cdr_status_t weld_cdr_decode_members(
  const rosidl_typesupport_introspection_c__MessageMembers * members,
  const uint8_t * data,
  size_t size,
  void * ros_message,
  char * errbuf,
  size_t errbuf_len)
{
  struct weld_reader r;
  weld_cdr_status_t st = WELD_CDR_OK;

  if (members == NULL || ros_message == NULL) {
    set_err(errbuf, errbuf_len, "weld: null decode argument");
    return WELD_CDR_BAD_ARG;
  }
  if (data == NULL && size > 0) {
    set_err(errbuf, errbuf_len, "weld: null decode buffer");
    return WELD_CDR_BAD_ARG;
  }
  (void)memset(&r, 0, sizeof(r));
  r.data = data;
  r.size = size;
  r.payload_start = WELD_CDR_HEADER_SIZE;
  r.err = errbuf;
  r.err_len = errbuf_len;
  if (size < WELD_CDR_HEADER_SIZE) {
    set_err(errbuf, errbuf_len, "weld: buffer too short for header");
    return WELD_CDR_TRUNCATED;
  }
  {
    weld_xt6b_ext_t ext = WELD_xt6b_FINAL;
    weld_xt6b_ver_t ver = WELD_xt6b_V1;
    bool le = false;
    weld_cdr_status_t pst = weld_cdr_xt6b_parse_encapsulation(
      data, &ext, &ver, &le, NULL);
    if (pst != WELD_CDR_OK) {
      set_err(errbuf, errbuf_len, "weld: bad encapsulation header");
      return pst;
    }
    (void)ext;
    (void)ver;
    r.le = le;
  }
  r.off = WELD_CDR_HEADER_SIZE;
  st = decode_body(members, (uint8_t *)ros_message, &r);
  if (st != WELD_CDR_OK) {
    return st;
  }
  return reader_finish_top(members, &r);
}

weld_cdr_status_t weld_cdr_encode(
  const rosidl_message_type_support_t * type_support,
  const void * ros_message,
  bool little_endian,
  uint8_t ** out,
  size_t * out_len,
  char * errbuf,
  size_t errbuf_len)
{
  const weld_members_t * m = top_table(type_support, errbuf, errbuf_len);

  if (m == NULL) {
    if (type_support == NULL || ros_message == NULL || out == NULL ||
      out_len == NULL)
    {
      return WELD_CDR_BAD_ARG;
    }
    return WELD_CDR_INVALID;
  }
  return weld_cdr_encode_members(m, ros_message, little_endian, out,
    out_len, errbuf, errbuf_len);
}

weld_cdr_status_t weld_cdr_decode(
  const rosidl_message_type_support_t * type_support,
  const uint8_t * data,
  size_t size,
  void * ros_message,
  char * errbuf,
  size_t errbuf_len)
{
  const weld_members_t * m = top_table(type_support, errbuf, errbuf_len);

  if (m == NULL) {
    if (type_support == NULL || ros_message == NULL) {
      return WELD_CDR_BAD_ARG;
    }
    return WELD_CDR_INVALID;
  }
  return weld_cdr_decode_members(m, data, size, ros_message, errbuf,
    errbuf_len);
}

// Select the V1 ID for a kind.
static weld_cdr_status_t xt6b_rep_v1(weld_xt6b_ext_t ext, bool le, uint16_t * out)
{
  switch (ext) {
    case WELD_xt6b_FINAL:
    case WELD_xt6b_APPENDABLE:
      *out = le ? (uint16_t)WELD_xt6b_CDR_LE : (uint16_t)WELD_xt6b_CDR_BE;
      return WELD_CDR_OK;
    case WELD_xt6b_MUTABLE:
      *out = le ? (uint16_t)WELD_xt6b_PL_CDR_LE : (uint16_t)WELD_xt6b_PL_CDR_BE;
      return WELD_CDR_OK;
    default:
      break;
  }
  return WELD_CDR_UNSUPPORTED;
}

// Select the V2 ID for a kind.
static weld_cdr_status_t xt6b_rep_v2(weld_xt6b_ext_t ext, bool le, uint16_t * out)
{
  switch (ext) {
    case WELD_xt6b_FINAL:
      *out = le ? (uint16_t)WELD_xt6b_CDR2_LE : (uint16_t)WELD_xt6b_CDR2_BE;
      return WELD_CDR_OK;
    case WELD_xt6b_APPENDABLE:
      *out = le ? (uint16_t)WELD_xt6b_D_CDR2_LE : (uint16_t)WELD_xt6b_D_CDR2_BE;
      return WELD_CDR_OK;
    case WELD_xt6b_MUTABLE:
      *out = le ? (uint16_t)WELD_xt6b_PL_CDR2_LE : (uint16_t)WELD_xt6b_PL_CDR2_BE;
      return WELD_CDR_OK;
    default:
      break;
  }
  return WELD_CDR_UNSUPPORTED;
}

weld_cdr_status_t weld_cdr_xt6b_representation_for(
  weld_xt6b_ext_t ext,
  weld_xt6b_ver_t ver,
  bool little_endian,
  uint16_t * out)
{
  if (out == NULL) {
    return WELD_CDR_BAD_ARG;
  }
  if (ver == WELD_xt6b_V2) {
    return xt6b_rep_v2(ext, little_endian, out);
  }
  if (ver == WELD_xt6b_V1) {
    return xt6b_rep_v1(ext, little_endian, out);
  }
  return WELD_CDR_UNSUPPORTED;
}

weld_cdr_status_t weld_cdr_xt6b_parse_encapsulation(
  const uint8_t hdr[4],
  weld_xt6b_ext_t * ext,
  weld_xt6b_ver_t * ver,
  bool * little_endian,
  uint16_t * representation)
{
  uint16_t id = 0;

  if (hdr == NULL) {
    return WELD_CDR_BAD_ARG;
  }
  if (hdr[0] != 0x00) {
    return WELD_CDR_INVALID;
  }
  id = (uint16_t)(((uint16_t)hdr[0] << 8) | (uint16_t)hdr[1]);
  if (representation != NULL) {
    *representation = id;
  }
  if (little_endian != NULL) {
    *little_endian = (id & 0x0001U) != 0;
  }
  switch (id) {
    case (uint16_t)WELD_xt6b_CDR_BE:
    case (uint16_t)WELD_xt6b_CDR_LE:
      if (ext != NULL) {
        *ext = WELD_xt6b_FINAL;
      }
      if (ver != NULL) {
        *ver = WELD_xt6b_V1;
      }
      return WELD_CDR_OK;
    case (uint16_t)WELD_xt6b_PL_CDR_BE:
    case (uint16_t)WELD_xt6b_PL_CDR_LE:
      if (ext != NULL) {
        *ext = WELD_xt6b_MUTABLE;
      }
      if (ver != NULL) {
        *ver = WELD_xt6b_V1;
      }
      return WELD_CDR_OK;
    case (uint16_t)WELD_xt6b_CDR2_BE:
    case (uint16_t)WELD_xt6b_CDR2_LE:
      if (ext != NULL) {
        *ext = WELD_xt6b_FINAL;
      }
      if (ver != NULL) {
        *ver = WELD_xt6b_V2;
      }
      return WELD_CDR_OK;
    case (uint16_t)WELD_xt6b_D_CDR2_BE:
    case (uint16_t)WELD_xt6b_D_CDR2_LE:
      if (ext != NULL) {
        *ext = WELD_xt6b_APPENDABLE;
      }
      if (ver != NULL) {
        *ver = WELD_xt6b_V2;
      }
      return WELD_CDR_OK;
    case (uint16_t)WELD_xt6b_PL_CDR2_BE:
    case (uint16_t)WELD_xt6b_PL_CDR2_LE:
      if (ext != NULL) {
        *ext = WELD_xt6b_MUTABLE;
      }
      if (ver != NULL) {
        *ver = WELD_xt6b_V2;
      }
      return WELD_CDR_OK;
    default:
      break;
  }
  return WELD_CDR_INVALID;
}

void weld_cdr_xt6b_make_encapsulation(
  uint8_t hdr[4],
  weld_xt6b_ext_t ext,
  bool little_endian)
{
  weld_xt6b_ver_t ver = WELD_xt6b_V1;
  uint16_t id = 0;

  if (hdr == NULL) {
    return;
  }
  if (ext != WELD_xt6b_FINAL) {
    ver = WELD_xt6b_V2;
  }
  if (weld_cdr_xt6b_representation_for(ext, ver, little_endian, &id) !=
    WELD_CDR_OK)
  {
    id = little_endian ? (uint16_t)WELD_xt6b_CDR_LE : (uint16_t)WELD_xt6b_CDR_BE;
  }
  hdr[0] = (uint8_t)(id >> 8);
  hdr[1] = (uint8_t)(id & 0x00FFU);
  hdr[2] = 0x00;
  hdr[3] = 0x00;
}

size_t weld_cdr_xt6b_maxalign(weld_xt6b_ver_t ver)
{
  if (ver == WELD_xt6b_V2) {
    return 4;
  }
  return 8;
}

size_t weld_cdr_xt6b_align_width(size_t width)
{
  if (width == 8 || width == 16) {
    return 4;
  }
  return width;
}

weld_cdr_status_t weld_cdr_xt6b_check_long_double(void)
{
  return WELD_CDR_UNSUPPORTED;
}

int weld_cdr_xt6b_cmp_i8(int8_t a, int8_t b)
{
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}

int weld_cdr_xt6b_cmp_u8(uint8_t a, uint8_t b)
{
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}

int weld_cdr_xt6b_cmp_i16(int16_t a, int16_t b)
{
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}

int weld_cdr_xt6b_cmp_u16(uint16_t a, uint16_t b)
{
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}

int weld_cdr_xt6b_cmp_i32(int32_t a, int32_t b)
{
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}

int weld_cdr_xt6b_cmp_u32(uint32_t a, uint32_t b)
{
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}

int weld_cdr_xt6b_cmp_i64(int64_t a, int64_t b)
{
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}

int weld_cdr_xt6b_cmp_u64(uint64_t a, uint64_t b)
{
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}

int weld_cdr_xt6b_cmp_str(const char * a, const char * b)
{
  if (a == b) {
    return 0;
  }
  if (a == NULL) {
    return -1;
  }
  if (b == NULL) {
    return 1;
  }
  return strcmp(a, b);
}

int weld_cdr_xt6b_cmp_wstr(const uint16_t * a, size_t an, const uint16_t * b, size_t bn)
{
  size_t i = 0;
  size_t n = 0;

  if (a == b && an == bn) {
    return 0;
  }
  if (a == NULL && b == NULL) {
    if (an < bn) {
      return -1;
    }
    if (an > bn) {
      return 1;
    }
    return 0;
  }
  if (a == NULL) {
    return -1;
  }
  if (b == NULL) {
    return 1;
  }
  n = an < bn ? an : bn;
  for (i = 0; i < n; i++) {
    if (a[i] < b[i]) {
      return -1;
    }
    if (a[i] > b[i]) {
      return 1;
    }
  }
  if (an < bn) {
    return -1;
  }
  if (an > bn) {
    return 1;
  }
  return 0;
}

// Store a 32 bit value in wire order without alignment assumptions.
static void xt6b_store_u32(uint8_t out[4], uint32_t v, bool le)
{
  if (le) {
    out[0] = (uint8_t)v;
    out[1] = (uint8_t)(v >> 8);
    out[2] = (uint8_t)(v >> 16);
    out[3] = (uint8_t)(v >> 24);
  } else {
    out[0] = (uint8_t)(v >> 24);
    out[1] = (uint8_t)(v >> 16);
    out[2] = (uint8_t)(v >> 8);
    out[3] = (uint8_t)v;
  }
}

// Load a 32 bit value in wire order without alignment assumptions.
static uint32_t xt6b_load_u32(const uint8_t data[4], bool le)
{
  if (le) {
    return (uint32_t)data[0] | ((uint32_t)data[1] << 8) |
      ((uint32_t)data[2] << 16) | ((uint32_t)data[3] << 24);
  }
  return ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16) |
    ((uint32_t)data[2] << 8) | (uint32_t)data[3];
}

void weld_cdr_xt6b_dheader_encode(
  uint8_t out[4],
  uint32_t size,
  bool little_endian)
{
  if (out == NULL) {
    return;
  }
  xt6b_store_u32(out, size, little_endian);
}

weld_cdr_status_t weld_cdr_xt6b_dheader_decode(
  const uint8_t * data,
  size_t remaining,
  bool little_endian,
  uint32_t * out_size)
{
  uint32_t size = 0;

  if (data == NULL || out_size == NULL) {
    return WELD_CDR_BAD_ARG;
  }
  if (remaining < 4) {
    return WELD_CDR_TRUNCATED;
  }
  size = xt6b_load_u32(data, little_endian);
  if ((uint64_t)size > (uint64_t)(remaining - 4)) {
    return WELD_CDR_TRUNCATED;
  }
  *out_size = size;
  return WELD_CDR_OK;
}

uint8_t weld_cdr_xt6b_length_code(uint32_t size)
{
  switch (size) {
    case 1:
      return 0;
    case 2:
      return 1;
    case 4:
      return 2;
    case 8:
      return 3;
    default:
      break;
  }
  return 4;
}

weld_cdr_status_t weld_cdr_xt6b_emheader_encode(
  uint32_t * out,
  bool must_understand,
  uint8_t lc,
  uint32_t member_id)
{
  uint32_t v = 0;

  if (out == NULL) {
    return WELD_CDR_BAD_ARG;
  }
  if (lc > 7) {
    return WELD_CDR_INVALID;
  }
  if (member_id > 0x0FFFFFFFU) {
    return WELD_CDR_INVALID;
  }
  v = ((uint32_t)lc << 28) | member_id;
  if (must_understand) {
    v |= 0x80000000U;
  }
  *out = v;
  return WELD_CDR_OK;
}

void weld_cdr_xt6b_emheader_decode(
  uint32_t v,
  bool * must_understand,
  uint8_t * lc,
  uint32_t * member_id)
{
  if (must_understand != NULL) {
    *must_understand = (v & 0x80000000U) != 0;
  }
  if (lc != NULL) {
    *lc = (uint8_t)((v >> 28) & 0x07U);
  }
  if (member_id != NULL) {
    *member_id = v & 0x0FFFFFFFU;
  }
}

weld_cdr_status_t weld_cdr_xt6b_must_understand(
  bool must_understand,
  uint32_t member_id)
{
  (void)member_id;
  if (must_understand) {
    return WELD_CDR_INVALID;
  }
  return WELD_CDR_OK;
}
