// SPDX-License-Identifier: Apache-2.0
// rmw_dynamic.c: dynamic typesupport takes.
#include "rmw_common.h"

// Dynamic subscription type identity, resolved at runtime.
//
// Static subscriptions carry an introspection handle, but dynamic ones
// arrive with the rosidl_dynamic_typesupport_c identifier and only a
// type description plus hash in the impl. These helpers detect that
// handle, copy the slash form type name, and format the RIHS hash for
// the Go declare path. All dlopen resolution reuses the no link time
// dependency pattern from weld_dyn_lookup. Only present where
// rmw/dynamic_message_type_support.h exists; Humble builds skip it.
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")

typedef const char * (* weld_dyn_ident_fn)(void);

static weld_dyn_ident_fn g_weld_dyn_ident = NULL;

static bool g_weld_dyn_ident_resolved = false;

// Look up the dynamic identifier string, resolving once per process.
// Returns NULL when the runtime has no dynamic support.
static const char * weld_dyn_identifier(void)
{
  const char * ident = NULL;
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  if (!g_weld_dyn_ident_resolved) {
#ifdef WELD_HAVE_DLOPEN
    void * lib = dlopen(
      "librosidl_dynamic_typesupport.so", RTLD_LAZY | RTLD_NOLOAD);
    if (lib == NULL) {
      lib = dlopen("librosidl_dynamic_typesupport.so", RTLD_LAZY);
    }
    if (lib != NULL) {
      void * sym = dlsym(lib, "rosidl_get_dynamic_typesupport_identifier");
      if (sym != NULL) {
        weld_dyn_ident_fn fn = NULL;
        (void)memcpy(&fn, &sym, sizeof(fn));
        g_weld_dyn_ident = fn;
      }
    }
#endif
    g_weld_dyn_ident_resolved = true;
  }
  if (g_weld_dyn_ident != NULL) {
    ident = g_weld_dyn_ident();
  }
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return ident;
}

// Report whether type_support is a runtime dynamic handle.
// Anything else, including NULL, reports false.
bool weld_dynamic_is_ts(const rosidl_message_type_support_t * type_support)
{
  const char * ident = NULL;
  if (type_support == NULL || type_support->typesupport_identifier == NULL) {
    return false;
  }
  ident = weld_dyn_identifier();
  if (ident == NULL) {
    return false;
  }
  return strcmp(type_support->typesupport_identifier, ident) == 0;
}

// Copy the slash form type name from a dynamic handle impl.
// Returns 0 with the name in buf, -1 when the impl or name is missing
// or does not fit.
static int weld_dynamic_type_name(
  const rosidl_message_type_support_t * type_support,
  char * buf, size_t buflen)
{
  const rosidl_dynamic_message_type_support_impl_t * dimpl = NULL;
  const rosidl_runtime_c__String * name = NULL;
  size_t n = 0;
  if (type_support == NULL || buf == NULL || buflen == 0) {
    return -1;
  }
  dimpl = (const rosidl_dynamic_message_type_support_impl_t *)type_support->data;
  if (dimpl == NULL) {
    return -1;
  }
  name = &dimpl->type_description.type_description.type_name;
  if (name->data == NULL || name->data[0] == '\0') {
    return -1;
  }
  n = strlen(name->data) + 1;
  if (n > buflen) {
    return -1;
  }
  (void)memcpy(buf, name->data, n);
  return 0;
}

// Fill the keep alive dynamic identity for a subscription.
// The slash form name plus RIHS hash are heap owned by the caller. The
// hash stays empty when the runtime provides none, matching the
// hashless wildcard report. Returns 0 on success and -1 with no side
// effects on failure.
int weld_dynamic_fill(
  const rosidl_message_type_support_t * type_support,
  char ** name_out, char ** hash_out)
{
  char name[512];
  char hash[WELD_TYPE_HASH_MAX];
  char * name_copy = NULL;
  char * hash_copy = NULL;
  if (type_support == NULL || name_out == NULL || hash_out == NULL) {
    return -1;
  }
  *name_out = NULL;
  *hash_out = NULL;
  if (weld_dynamic_type_name(type_support, name, sizeof(name)) != 0) {
    return -1;
  }
  name_copy = weld_dup_str(name);
  if (name_copy == NULL) {
    return -1;
  }
  if (weld_type_hash(type_support, hash, sizeof(hash)) == 0) {
    hash_copy = weld_dup_str(hash);
  } else {
    hash_copy = weld_dup_str("");
  }
  if (hash_copy == NULL) {
    free(name_copy);
    return -1;
  }
  *name_out = name_copy;
  *hash_out = hash_copy;
  return 0;
}

// Report the runtime dynamic type to the Go backend.
// A missing symbol (old Go library) skips silently.
void weld_report_dynamic_sub_type(
  long long handle, const char * type_name, const char * type_hash)
{
  if (weld_ffi_sub_set_dynamic_type == NULL) {
    return;
  }
  if (type_name == NULL || type_hash == NULL) {
    return;
  }
  (void)weld_ffi_sub_set_dynamic_type(handle, type_name, type_hash);
}

#endif /* dynamic subscription identity available */

// Dynamic deserialize entry resolved without a link time dependency.
//
// The probe link lines carry no -lrosidl_dynamic_typesupport, so a direct
// call breaks the link. The symbol is resolved once from the loaded
// library (rcl already depends on it) via dlopen plus dlsym, falling back
// to loading by soname. A NULL result means the runtime has no dynamic
// support and the take reports UNSUPPORTED. dlopen lives in libc on the
// supported platforms, so no extra link flag is needed. The handle is
// never closed so the resolved entry stays valid. Humble predates
// the dynamic typesupport API, so this helper exists only where
// rmw/dynamic_message_type_support.h does, matching the take functions.
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")

typedef rcutils_ret_t (* weld_dyn_deserialize_fn)(
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message,
  rcutils_uint8_array_t * buffer);

static weld_dyn_deserialize_fn g_weld_dyn_deserialize = NULL;

static bool g_weld_dyn_resolved = false;

// Look up the dynamic deserialize entry, resolving once per process.
// Returns NULL when the runtime has no dynamic support.
static weld_dyn_deserialize_fn weld_dyn_lookup(void)
{
  weld_dyn_deserialize_fn fn = NULL;
  (void)pthread_mutex_lock(&g_weld_loan_mutex);
  if (!g_weld_dyn_resolved) {
#ifdef WELD_HAVE_DLOPEN
    void * lib = dlopen(
      "librosidl_dynamic_typesupport.so", RTLD_LAZY | RTLD_NOLOAD);
    if (lib == NULL) {
      lib = dlopen("librosidl_dynamic_typesupport.so", RTLD_LAZY);
    }
    if (lib != NULL) {
      void * sym = dlsym(
        lib, "rosidl_dynamic_typesupport_dynamic_data_deserialize");
      if (sym != NULL) {
        (void)memcpy(&fn, &sym, sizeof(fn));
        g_weld_dyn_deserialize = fn;
      }
    }
#endif
    g_weld_dyn_resolved = true;
  }
  fn = g_weld_dyn_deserialize;
  (void)pthread_mutex_unlock(&g_weld_loan_mutex);
  return fn;
}

#endif /* dynamic lookup helper available */

// Dynamic message take: the wire sample is taken the same way as the
// static path (wait stash first via weld_take_wire) and populated into
// the caller provided dynamic data through
// rosidl_dynamic_typesupport_dynamic_data_deserialize. The caller binds
// the dynamic data to its type plus serialization support; this shim
// only moves the taken CDR bytes (including the 4 byte encapsulation
// header, which matches what the serialization libraries expect) into
// it. The deserialize entry is resolved at runtime (see
// weld_dyn_lookup) so no link time dependency is needed. Humble predates
// the dynamic typesupport API entirely, so these entry points exist only
// where rmw/dynamic_message_type_support.h does.
#if defined(__has_include) && __has_include("rmw/dynamic_message_type_support.h")

typedef rcutils_ret_t (* weld_dyn_ss_impl_fn)(
  rcutils_allocator_t *,
  rosidl_dynamic_typesupport_serialization_support_impl_t *);

typedef rcutils_ret_t (* weld_dyn_ss_iface_fn)(
  rcutils_allocator_t *,
  rosidl_dynamic_typesupport_serialization_support_interface_t *);

typedef rcutils_ret_t (* weld_dyn_ss_init_fn)(
  rosidl_dynamic_typesupport_serialization_support_impl_t *,
  rosidl_dynamic_typesupport_serialization_support_interface_t *,
  rcutils_allocator_t *,
  rosidl_dynamic_typesupport_serialization_support_t *);

// Resolve one dynamic serialization support symbol without a link time
// dependency. Returns NULL when the library or symbol is absent.
static void * weld_dyn_sym(const char * soname, const char * symname)
{
  void * sym = NULL;
#ifdef WELD_HAVE_DLOPEN
  void * lib = NULL;
  if (soname == NULL || symname == NULL) {
    return NULL;
  }
  lib = dlopen(soname, RTLD_LAZY | RTLD_NOLOAD);
  if (lib == NULL) {
    lib = dlopen(soname, RTLD_LAZY);
  }
  if (lib != NULL) {
    sym = dlsym(lib, symname);
  }
#else
  (void)soname;
  (void)symname;
#endif
  return sym;
}

rmw_ret_t rmw_serialization_support_init(
  const char * serialization_lib_name,
  rcutils_allocator_t * allocator,
  rosidl_dynamic_typesupport_serialization_support_t * serialization_support)
{
  // Initialize the polymorphic serialization support for a named
  // dynamic typesupport library (such as
  // rosidl_dynamic_typesupport_fastrtps). The library soname plus the
  // impl and interface init symbols derive from the name, and the core
  // init entry comes from librosidl_dynamic_typesupport, all resolved
  // by dlopen so the probe link line carries no extra library. Only
  // plain leaf names are accepted so the derived soname and symbol
  // strings cannot traverse paths. The interface init runs first
  // because it only fills the function-pointer table and allocates
  // nothing; the impl init allocates the library handle and runs
  // second, immediately before core assembly, so an impl success is
  // never abandoned by a later interface failure. On any failure the
  // output is untouched and the error is loud.
  char soname[512];
  char impl_sym[512];
  char iface_sym[512];
  size_t name_len = 0;
  int n = 0;
  void * impl_addr = NULL;
  void * iface_addr = NULL;
  void * core_addr = NULL;
  weld_dyn_ss_impl_fn impl_fn = NULL;
  weld_dyn_ss_iface_fn iface_fn = NULL;
  weld_dyn_ss_init_fn core_fn = NULL;
  rosidl_dynamic_typesupport_serialization_support_impl_t impl;
  rosidl_dynamic_typesupport_serialization_support_interface_t methods;
  rcutils_ret_t rc = RCUTILS_RET_OK;
  if (serialization_lib_name == NULL || serialization_lib_name[0] == '\0') {
    return weld_fail_invalid(
      "rmw_weld: rmw_serialization_support_init null library name");
  }
  if (allocator == NULL || !rcutils_allocator_is_valid(allocator)) {
    return weld_fail_invalid(
      "rmw_weld: rmw_serialization_support_init bad allocator");
  }
  if (serialization_support == NULL) {
    return weld_fail_invalid(
      "rmw_weld: rmw_serialization_support_init null output");
  }
  name_len = strlen(serialization_lib_name);
  if (name_len > 200) {
    return weld_fail_invalid(
      "rmw_weld: rmw_serialization_support_init library name too long");
  }
  // Only letters, digits, and underscores are accepted: slashes, dots,
  // and separators would turn the derived soname and symbol strings
  // into a path traversal past the intended library.
  for (size_t i = 0; i < name_len; i++) {
    char c = serialization_lib_name[i];
    bool plain = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
      (c >= '0' && c <= '9') || c == '_';
    if (!plain) {
      return weld_fail_invalid(
        "rmw_weld: rmw_serialization_support_init bad library name");
    }
  }
  n = snprintf(soname, sizeof(soname), "lib%s.so", serialization_lib_name);
  if (n < 0 || (size_t)n >= sizeof(soname)) {
    return weld_fail_error(
      "rmw_weld: rmw_serialization_support_init soname overflow");
  }
  n = snprintf(
    impl_sym, sizeof(impl_sym), "%s_init_serialization_support_impl",
    serialization_lib_name);
  if (n < 0 || (size_t)n >= sizeof(impl_sym)) {
    return weld_fail_error(
      "rmw_weld: rmw_serialization_support_init impl symbol overflow");
  }
  n = snprintf(
    iface_sym, sizeof(iface_sym),
    "%s_init_serialization_support_interface", serialization_lib_name);
  if (n < 0 || (size_t)n >= sizeof(iface_sym)) {
    return weld_fail_error(
      "rmw_weld: rmw_serialization_support_init iface symbol overflow");
  }
  impl_addr = weld_dyn_sym(soname, impl_sym);
  iface_addr = weld_dyn_sym(soname, iface_sym);
  core_addr = weld_dyn_sym(
    "librosidl_dynamic_typesupport.so",
    "rosidl_dynamic_typesupport_serialization_support_init");
  if (impl_addr == NULL || iface_addr == NULL || core_addr == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_serialization_support_init runtime missing symbols");
    return RMW_RET_ERROR;
  }
  (void)memcpy(&impl_fn, &impl_addr, sizeof(impl_fn));
  (void)memcpy(&iface_fn, &iface_addr, sizeof(iface_fn));
  (void)memcpy(&core_fn, &core_addr, sizeof(core_fn));
  (void)memset(&impl, 0, sizeof(impl));
  (void)memset(&methods, 0, sizeof(methods));
  // Interface first: it only fills the function-pointer table, so a
  // failure leaves no partial allocation behind.
  rc = iface_fn(allocator, &methods);
  if (rc != RCUTILS_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_serialization_support_init iface init failed");
    return RMW_RET_ERROR;
  }
  rc = impl_fn(allocator, &impl);
  if (rc != RCUTILS_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_serialization_support_init impl init failed");
    return RMW_RET_ERROR;
  }
  rc = core_fn(&impl, &methods, allocator, serialization_support);
  if (rc != RCUTILS_RET_OK) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_serialization_support_init core init failed");
    return RMW_RET_ERROR;
  }
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_dynamic_message(
  const rmw_subscription_t * subscription,
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message, bool * taken,
  rmw_subscription_allocation_t * allocation)
{
  struct weld_sub_data * sd = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  rcutils_uint8_array_t buf;
  weld_dyn_deserialize_fn dyn_fn = NULL;
  rcutils_ret_t dyn_rc = RCUTILS_RET_OK;
  int empty = 0;
  (void)allocation;
  if (subscription == NULL || dynamic_message == NULL || taken == NULL) {
    return weld_fail_invalid("rmw_weld: rmw_take_dynamic_message null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl("rmw_weld: rmw_take_dynamic_message wrong impl");
  }
  *taken = false;
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG("rmw_weld: rmw_take_dynamic_message no data");
    return RMW_RET_INVALID_ARGUMENT;
  }
  dyn_fn = weld_dyn_lookup();
  if (dyn_fn == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_dynamic_message no dynamic runtime loaded");
    return RMW_RET_UNSUPPORTED;
  }
  empty = weld_take_wire(sd, &wire, &wire_len);
  if (empty == 1) {
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  buf.buffer = wire;
  buf.buffer_length = wire_len;
  buf.buffer_capacity = wire_len;
  buf.allocator = rcutils_get_default_allocator();
  dyn_rc = dyn_fn(dynamic_message, &buf);
  free(wire);
  if (dyn_rc != RCUTILS_RET_OK) {
    weld_drop_add();
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_dynamic_message deserialize failed");
    return RMW_RET_ERROR;
  }
  *taken = true;
  return RMW_RET_OK;
}

rmw_ret_t rmw_take_dynamic_message_with_info(
  const rmw_subscription_t * subscription,
  rosidl_dynamic_typesupport_dynamic_data_t * dynamic_message, bool * taken,
  rmw_message_info_t * message_info, rmw_subscription_allocation_t * allocation)
{
  struct weld_sub_data * sd = NULL;
  uint8_t * wire = NULL;
  size_t wire_len = 0;
  rcutils_uint8_array_t buf;
  weld_dyn_deserialize_fn dyn_fn = NULL;
  rcutils_ret_t dyn_rc = RCUTILS_RET_OK;
  int empty = 0;
  (void)allocation;
  if (subscription == NULL || dynamic_message == NULL || taken == NULL ||
    message_info == NULL)
  {
    return weld_fail_invalid(
      "rmw_weld: rmw_take_dynamic_message_with_info null arg");
  }
  if (!weld_sub_ok(subscription)) {
    return weld_fail_impl(
      "rmw_weld: rmw_take_dynamic_message_with_info wrong impl");
  }
  *taken = false;
  sd = (struct weld_sub_data *)subscription->data;
  if (sd == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_dynamic_message_with_info no data");
    return RMW_RET_INVALID_ARGUMENT;
  }
  dyn_fn = weld_dyn_lookup();
  if (dyn_fn == NULL) {
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_dynamic_message_with_info no dynamic runtime");
    return RMW_RET_UNSUPPORTED;
  }
  empty = weld_take_wire(sd, &wire, &wire_len);
  if (empty == 1) {
    return RMW_RET_OK;
  }
  if (empty != 0) {
    return RMW_RET_ERROR;
  }
  buf.buffer = wire;
  buf.buffer_length = wire_len;
  buf.buffer_capacity = wire_len;
  buf.allocator = rcutils_get_default_allocator();
  dyn_rc = dyn_fn(dynamic_message, &buf);
  free(wire);
  if (dyn_rc != RCUTILS_RET_OK) {
    weld_drop_add();
    rmw_reset_error();
    RCUTILS_SET_ERROR_MSG(
      "rmw_weld: rmw_take_dynamic_message_with_info deserialize failed");
    return RMW_RET_ERROR;
  }
  weld_fill_take_info(message_info);
  *taken = true;
  return RMW_RET_OK;
}

#endif /* dynamic message typesupport available */
