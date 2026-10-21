# The ROS Stack: From User Code to the Wire

This chapter maps how ROS 2 works from user code down to the wire. The goal is to name each layer that moves bytes from user code to the network and the structure that owns it.

Start with a concrete failure. A planner named `planner_server` sends `/cmd_vel` at 20 Hz, a driver named `motor_driver` subscribes, and nothing moves. The cause can live in naming, QoS, discovery, time, or the wait loop. This tour names each layer that can hide such a fault and the structure that owns it.

Clones used, all on branch `rolling`:

- ros2/rclcpp
- ros2/rcl
- ros2/rmw

Reading rule used here: plain words first, then the technical term. Every behavior below names the structure that owns it so you can find it in source. Inline citations are kept only for load bearing claims. Everything else lives in the Sources list that closes each section.

## Messages on Paper and Bytes on the Wire

Before the stack, two ideas carry the whole book. IDL describes shape. CDR describes layout.

IDL stands for Interface Definition Language. In ROS it usually means a `.msg` file. That file says what fields a message has and what type each field holds. It says nothing about sockets or bytes.

A tiny example:

```
# Pose.msg, simplified
float64 x
float64 y
float64 theta
```

That text means a pose holds three double precision numbers in that order. Code generators turn the same description into a C struct, a Python class, and a Go struct. The names and order stay fixed across languages.

CDR stands for Common Data Representation. It says how a shaped value flattens into bytes for the wire: field order, alignment, and padding. Receivers use the same rules in reverse to rebuild the struct.

Alignment is the part that surprises newcomers. Many types must start at an offset that is a multiple of their size. A four byte integer must start at offset zero, four, eight, and so on. If the previous field ends early, CDR inserts padding bytes that carry no data.

A second tiny example shows the idea:

```
uint8 flag
uint32 count
```

Suppose `flag` holds 1 and `count` holds 42. CDR lays the struct out like this, shown here little endian with a four byte encapsulation header that names the encoding:

```
00 01 00 00   encapsulation, little endian CDR
01            flag = 1
00 00 00      padding, three bytes so count aligns to four
2A 00 00 00   count = 42
```

The padding bytes are load bearing. A decoder that ignores alignment reads the wrong value for `count`.

Type hashes add the safety check. Discovery compares a fixed size hash of the type description before data flows. A mismatch means an incompatible type event, never a silent misdecode.

**Sources**

- docs/ros/03-type-system.md
- docs/ros/04-message-catalog.md
- rosidl IDL descriptions and generated type support
- rmw serialize and deserialize contracts in rmw/rmw/include/rmw/rmw.h

## Map of Companion Chapters

This file is the hub. The companion chapters in this folder each cover one slice in depth. Read them by topic need, not by number. Each entry below names the chapter to reach for.

- `03-type-system.md`: IDL pipeline, type support, type hashes, dynamic type support. Reach for this when bytes, hashes, or codegen questions arise.
- `09-observability-tracing.md`: tracetools tracepoints and compile out levels. Reach for this when observability or overhead questions arise.
- `05-dds-wire-protocol.md`: CycloneDDS driver, QoS tables, topic and type naming, CDR envelope, services, discovery, waitset. Reach for this when DDS wire behavior matters.
- `06-shared-discovery-and-qos.md`: shared graph cache, RxO truth table, type hash user data. Reach for this when matching or graph cache behavior matters.
- `07-vendor-differences.md`: FastDDS versus CycloneDDS deltas and fork hazards. Reach for this when vendor drift matters.
- `08-zenoh-protocol.md`: Zenoh key mapping, liveliness tokens, QoS mapping, attachments. Reach for this when Zenoh interop matters.
- `11-conformance-tools.md`: ros2cli, rosbag2, and launch_ros conformance tests Weld must pass. Reach for this when tool or bag behavior matters.
- `13-navigation-and-manipulation.md`: Nav2 and MoveIt 2 application stacks as conformance targets. Reach for this when the planner or arm stack matters.
- `12-control-stack.md`: ros2_control, ros2_controllers, and the Gazebo control bridge. Reach for this when hardware control matters.
- `10-transforms.md`: tf2 transform tree contract. Reach for this when `/tf` or `/tf_static` matters.
- `04-message-catalog.md`: common_interfaces codegen and CDR fixtures. Reach for this when standard message bytes matter.
- `14-perception.md`: vision_opencv, image_pipeline, and perception_pcl. Reach for this when camera or point cloud paths matter.
- `15-localization-and-mapping.md`: robot_localization and slam_toolbox. Reach for this when odometry or mapping matters.
- `16-simulation.md`: ros_gz bridge for sim in the loop. Reach for this when simulation time or bridging matters.
- `17-visualization.md`: RViz subscriber contract for rendering Weld scenes. Reach for this when visualization matters.
- `18-hardware-drivers.md`: ros-drivers wrapper pattern with velodyne, usb_cam, and um7 mapped. Reach for this when sensor drivers matter.
- `19-failure-analysis.md`: cross repo problem digest with Weld rules. Reach for this when failure classes and eliminations matter.
- `01-why-robots-run-ros.md`: the application stacks that make ROS worth staying compatible with, and why Weld carries them instead of reimplementing them.

**Sources**

- docs/ros/01-why-robots-run-ros.md through docs/ros/19-failure-analysis.md

## Big Picture

Plain words: your code talks to a C++ helper, the helper talks to a C core, the C core talks to a plug socket, and one vendor driver sits in that socket and speaks to the network.

Picture a talker named `talker` publishing `std_msgs/msg/String` on `/chatter` with data `hello 42`, and a listener named `listener` subscribing on the same topic. The talker code never touches sockets. It calls into layers that narrow from convenience to bytes.

```
your code (nodes, publishers, subscriptions, services, timers)
  | C++ classes, templates, callbacks
  v
rclcpp (plus rclcpp_action, rclcpp_lifecycle)
  executor, callback groups, parameters, actions, lifecycle
  | C structs and functions, no templates
  v
rcl
  entity lifecycle, argument parsing, wait set, graph queries,
  clocks, events, logging, introspection
  | vendor neutral C API
  v
rmw (interface only)
  node, publisher, subscription, service, client, wait, events,
  discovery options, security options, QoS profiles
  | vendor driver
  v
rmw_cyclonedds_cpp OR rmw_fastrtps_cpp OR rmw_zenoh_cpp
  RTPS over UDP, or Zenoh over TCP/UDP
```

Supporting libraries appear in signatures but live outside this tour: `rcutils` for allocators, logging, time, and error state, `rosidl` for generated type support, type hashes, and introspection, plus `rcpputils` and `tracetools` on the C++ side.

| Layer | Language | Owns | Does not own |
| :--- | :--- | :--- | :--- |
| `rclcpp` | C++ | callbacks, executor order, callback groups, parameters, actions, lifecycle, generic tools | wire protocol, discovery packets |
| `rcl` | C | entity init and fini, wait set, graph queries, clocks, events, rosout, introspection topics | callback dispatch, parameter logic |
| `rmw` | C interface | discovery API, type binding contract, CDR I/O contract, QoS profiles, event contract | any transport bytes |
| vendor `rmw_*` | C/C++ | RTPS or Zenoh transport, matching, history caches, retransmission | ROS graph semantics |

**Sources**

- rclcpp/rclcpp/include/rclcpp/executor.hpp
- rclcpp/rclcpp/src/rclcpp/executor.cpp
- rcl/rcl/include/rcl/node.h
- rcl/rcl/include/rcl/wait.h
- rcl/rcl/include/rcl/publisher.h
- rcl/rcl/include/rcl/subscription.h
- rmw/rmw/include/rmw/rmw.h
- rmw/rmw/include/rmw/types.h

## Context and Startup

This group decides whether two processes ever find each other. Domain, discovery range, enclave, and remapping all resolve before any topic flows. A mismatch here looks like silence later, for example `ros2 topic echo /scan` showing nothing because the publisher lives on domain 5 while the echo listens on domain 0.

### The context object

Plain words: the context is the process handle for a domain. Everything else hangs off it.

The `rcl` context holds the parsed global arguments, a heap part that starts as NULL until init, and an eight byte instance id slot read through an accessor. The heap part holds the allocator, a copy of the init options, a deep copy of argument count and vector, and the rmw context.

Lifecycle moves through distinct states: zero initialized, then initialized and valid, then shut down but still holding memory, then finalized with memory freed. Zero init sets the heap pointer to NULL and clears global arguments. Cleanup zeroes the instance id, finalizes arguments, init options, and the rmw context, then frees the argument copy and the heap part. Finalize on a zero initialized context is fine. Finalize on a still valid context is invalid. The legal path is shutdown, then finalize all entities, then finalize the context.

The C++ `Context` wraps the shared C context with a shutdown reason, a logging mutex, sub contexts, pre shutdown and on shutdown callback lists, an interrupt condition, a graph listener, and a weak registry. Init calls the C init, configures logging once per process by reference count, rejects unparsed ROS arguments, and installs a graph listener. Shutdown guards against double shutdown, runs pre callbacks, calls the C shutdown, records the reason, runs on shutdown callbacks, interrupts clock sleeps, removes itself from the registry, and finalizes logging at zero references. Executors wake only indirectly through their own guard conditions, not by a direct call from the context.

### Startup order

Startup is a genuine sequence where each phase depends on the last. First the code validates its inputs and allocator and auto inits logging. Then it rejects double init when the heap part is already set. Next it allocates the heap part and copies init options, including a deep copy through the rmw options copy. After that it deep copies the argument vector. Then it parses arguments into global arguments. Next it assigns the instance id from a process wide atomic starting at one, with rollover to zero treated as error, and mirrors the id into the rmw init options. Then it resolves the domain id, reading the environment when the default marker is set. After domain comes discovery: resolve the discovery range and static peers when not set explicitly, and warn when static peers are set while discovery is off. Then it resolves the enclave name from global arguments or defaults to slash, validates it, and loads security options from the environment for that enclave. Then it calls rmw init with the rmw init options and the rmw context slot, converting failures to the matching code. Finally, on any failure it runs cleanup and returns the first failure code.

Shutdown calls the rmw shutdown and zeroes the instance id. It does not free memory. Context finalize must follow after all entities are finalized.

### Domain identity

Domain isolation decides which processes can meet. A Velodyne driver publishing `/velodyne_points` on domain 12 is invisible to RViz on domain 0, by design.

The default marker aliases the rmw default. The environment variable name is `ROS_DOMAIN_ID`. Unset or empty leaves the caller value untouched, else the code parses with `strtoul` and rejects non integral input, range errors, and values above the platform maximum. Init options hold the configured value. Query paths read the actual domain from the rmw context. The node domain getter delegates unconditionally to the node context. There is no per node domain field: node options hold only allocator, global argument use, local arguments, rosout enable, and rosout QoS. The apparent node default domain alias is an unused alias, not a selectable mode.

### Discovery choices

The range enum carries a not set sentinel that init must replace, plus off, localhost, subnet, and system default. Static peers are capped at 256 entries, where each peer is a hostname, IPv4, IPv6, or subnet. The struct holds the range, a peer array with count, and an allocator, with init, equal, copy, and fini helpers. Defaulting upgrades not set to localhost unconditionally before the size zero early return, so even a zero size init ends up localhost.

The `rcl` init wires this through in prose: first check whether the range is set, and if not query the default range. Then, when no static peers are configured and discovery is not off, query static peers from the environment. Finally warn when peers exist while discovery is off.

### Security and enclaves

Plain words: the enclave names a security group, and the policy decides whether security is optional or required.

The rmw security struct holds an enforcement policy plus a root path. Zero init is all zeros, default init is permissive with NULL path. The enclave string is owned by the rmw init options, with copy and fini helpers. The graph exposes enclaves through the node names with enclaves query.

The `rcl` side takes the enclave from global arguments or defaults to slash, validates it, then loads security options from the environment for that enclave before rmw init.

### Arguments, remapping, and name expansion

Launch arguments decide names before entities exist. A launch file that remaps `/cmd_vel` to `/diff_drive/cmd_vel` must affect every publisher created later in `controller_server`.

Constants name the flags: the ros args marker, the bare double dash scope end, param and its short form, params file, remap and its short form, enclave and its short form, log level, log file name, log config file, plus enable and disable flags for stdout, rosout, and external library logs.

The arguments struct holds an impl pointer. The impl holds unparsed ROS args, unparsed non ROS args, parameter overrides, parameter files, remap rules, log levels, log file settings, disable flags, enclave, and allocator.

Parsing works in phases. First it requires a zero initialized output. Then it over allocates working arrays sized by argument count, then shrinks them. Outside a ros args scope, only a bare deprecated mapping is accepted, else the token goes to unparsed args. Inside the scope, it handles params, remaps, params files, log levels, log file names, enclave, and enable and disable flags. A lone double dash ends the scope. Anything else goes to unparsed ROS args. Failure returns invalid ROS args.

Remapping uses a bitmask of rule types: topic, service, node name, namespace. A static matcher scans a single rule array, filters by node name prefix, expands the match pattern, and compares strings. The public remap tries local rules first then global rules through that matcher, so first match across both arrays wins. It expands the replacement for topic and service kinds and duplicates it verbatim for node and namespace kinds. Public helpers cover topic, service, node name, and node namespace.

Node scoped resolution proceeds in order. First build the default substitution map for the node. Then expand relative names. Unless expansion only was requested, apply topic or service remapping. Finally validate the full name.

Copy and fini for arguments deep copy the arrays. A strip helper removes ROS args so the application sees only its own argv.

**Sources**

- rcl/rcl/include/rcl/context.h
- rcl/rcl/src/rcl/context_impl.h
- rcl/rcl/src/rcl/context.c
- rclcpp/rclcpp/include/rclcpp/context.hpp
- rclcpp/rclcpp/src/rclcpp/context.cpp
- rcl/rcl/include/rcl/init.h
- rcl/rcl/src/rcl/init.c
- rcl/rcl/include/rcl/init_options.h
- rcl/rcl/src/rcl/init_options_impl.h
- rcl/rcl/src/rcl/init_options.c
- rcl/rcl/include/rcl/domain_id.h
- rcl/rcl/src/rcl/domain_id.c
- rcl/rcl/include/rcl/node_options.h
- rcl/rcl/src/rcl/node_options.c
- rmw/rmw/include/rmw/discovery_options.h
- rmw/rmw/src/discovery_options.c
- rmw/rmw/include/rmw/security_options.h
- rmw/rmw/src/security_options.c
- rmw/rmw/include/rmw/init_options.h
- rmw/rmw/include/rmw/enclave.h
- rmw/rmw/include/rmw/rmw.h
- rcl/rcl/include/rcl/arguments.h
- rcl/rcl/src/rcl/arguments_impl.h
- rcl/rcl/src/rcl/arguments.c
- rcl/rcl/src/rcl/remap.c
- rcl/rcl/src/rcl/remap_impl.h
- rcl/rcl/src/rcl/node_resolve_name.c

## Nodes

Nodes give names to graph participants. Without a node there is no publisher, no subscription, no parameter service, and no entry in `ros2 node list`.

### Node handles

The `rcl` node holds a context pointer plus one impl pointer. The impl holds a deep copy of node options, the rmw node handle, the graph guard condition, the dotted logger name, the fully qualified name, and a per node type cache keyed by type hash.

Node options hold allocator, global argument use, local arguments, rosout enable, and rosout QoS. Defaults use the default allocator, global arguments on, rosout on, and the rosout QoS preset. Copy deep copies arguments. Fini finalizes them.

Init options hold an impl pointer. The impl holds an allocator plus rmw init options, with zero, init, copy, and fini helpers. Init calls the rmw options init and marks discovery not set.

### Startup and teardown

The declaration validates the name, prefixes the namespace with slash when needed, treats empty namespace as slash, and maps failures to invalid name and invalid namespace codes.

Implementation follows a strict order with unwind on failure. First reject missing options, allocator, name, namespace, or node handle, and reject a node whose impl is already set. Then require a non NULL context that is still valid. Next validate the node name and normalize the namespace. Then allocate the impl, copy node options, and pick global arguments when enabled. After that remap the node name and namespace. Then build the fully qualified name and the dotted logger name. Next call rmw create node and preserve the rmw error for the caller. Then fetch the graph guard condition from rmw and wrap it. Finally init the per node type cache before any publisher or subscription is created. On failure, unwind in reverse: type cache, guard condition, rmw node, logger and fully qualified strings, node options, impl, temporaries, then zero the node handle.

Fini is repeat safe: NULL impl is fine. It finalizes the type cache, destroys the rmw node, finalizes and frees the graph guard condition, frees logger and fully qualified strings, finalizes local arguments, frees the impl, and clears it.

Validity and getters check the node, the impl, and the rmw handle, with the fuller check adding context validity. Getters expose name, namespace, fully qualified name, options, rmw handle, instance id, graph guard condition, and logger name.

The C++ `Node` builds the base first with the context and the resolved node options, then graph, logging, timers, topics, services, clock, parameters, time source, type descriptions, and waitables. The base is the sole owner of the C node handle. It maps name and namespace errors to matching C++ exceptions, starts rosout publishing when enabled, destroys the node in its deleter, and creates the default callback group. All other C++ node interfaces hold pointers to facets, never a second C node handle.

**Sources**

- rcl/rcl/include/rcl/node.h
- rcl/rcl/src/rcl/node_impl.h
- rcl/rcl/include/rcl/node_options.h
- rcl/rcl/src/rcl/node_options.c
- rcl/rcl/include/rcl/init_options.h
- rcl/rcl/src/rcl/init_options_impl.h
- rcl/rcl/src/rcl/init_options.c
- rcl/rcl/src/rcl/node.c
- rclcpp/rclcpp/src/rclcpp/node.cpp
- rclcpp/rclcpp/src/rclcpp/node_interfaces/node_base.cpp

## Publishers

Publishers move bytes one way. Their QoS is fixed at creation and lives for the life of the entity. There is no per message override.

### Handles and options

The `rcl` publisher holds one impl pointer. Options hold QoS, allocator, rmw publisher options, and a loaned message disable flag. The impl holds an options copy, the actual QoS, a context pointer, the rmw publisher handle, and the type hash.

### Creation

Creation proceeds in phases. First validate options, allocator, publisher, node, type support, and topic name. Then resolve the topic name for the node, which applies expansion and remapping. For example `chatter` inside namespace `/demo` becomes `/demo/chatter` unless remapped. Next allocate the impl. Then call rmw create publisher with the rmw node, type support, the remapped name, QoS, and rmw publisher options. After creation read back the actual QoS, then patch the namespace convention avoidance flag from the requested profile. Finally copy the options, register the type in the node type cache with its hash, and store the node context.

Fini destroys the rmw publisher, unregisters the type, and frees the impl.

Defaults use the default QoS preset, default rmw publisher options, and loaned messages following the environment gate that disables them. Loaned messages are enabled unless explicitly disabled.

The C++ creation path declares QoS override parameters when enabled, calls topic creation through a factory that builds the typed publisher and runs post init setup for shared pointer safety, then adds the publisher to a callback group. Post init setup resolves intra process settings, reads the actual QoS, checks keep last and transient local rules, and registers with the intra process manager.

### Send paths

All send paths live in the same C file. Plain publish calls rmw publish with message and allocation hint. Serialized publish calls the serialized variant. Loaned publish calls the loaned publish. Borrow and return wrap the rmw borrow and return. Assert liveliness wraps the rmw call and serves manual by topic liveliness. Wait for all acked converts a nanosecond timeout to rmw time, calls the rmw wait for acked, and maps timeout and unsupported codes.

There is no intra process branch in `rcl`; that shortcut lives above in `rclcpp` through its intra process manager and below in rmw loaned message support.

Getters expose topic name from the rmw handle, an options copy, the rmw handle, the context, the actual QoS, whether loaning is possible, and the matched subscription count.

**Sources**

- rcl/rcl/include/rcl/publisher.h
- rcl/rcl/src/rcl/publisher_impl.h
- rcl/rcl/src/rcl/publisher.c
- rclcpp/rclcpp/include/rclcpp/create_publisher.hpp
- rclcpp/rclcpp/include/rclcpp/publisher.hpp

## Subscriptions

Subscriptions are the mirror of publishers with one extra job: they decide how to take data without losing the contract about duplicates and emptiness.

### Handles and options

The `rcl` subscription holds one impl pointer. Options hold QoS, allocator, rmw subscription options, and a loaned message disable flag. Content filter options wrap the rmw filter options. The impl holds an options copy, the actual QoS, the rmw handle, the type hash, and a flag used to reject double adds to wait sets.

### Creation

Creation mirrors publisher init: resolve the name, allocate the impl, call rmw create subscription, read back actual QoS, patch the namespace convention flag, and register the type hash. A subscription to `/scan` with sensor data QoS therefore records keep last 5 and best effort as its actual values.

Fini destroys the rmw subscription, finalizes subscription options including content filter state, and unregisters the type.

Defaults use the default QoS preset with default rmw subscription options, and loaned messages are disabled by default. They turn on only when the disable environment variable is explicitly zero. Note the inversion relative to publishers.

The C++ creation path builds a type erased callback, optionally creates a topic statistics publisher and timer, declares QoS overrides, builds the subscription through a factory, and adds it to a callback group plus intra process waitables. The C++ constructor resolves intra process compatibility and registers with the intra process manager.

### Take paths

All take paths live in the same C file. Plain take calls rmw take with info, using a dummy info struct when the caller passes NULL. No message taken maps to take failed. Sequence take calls rmw take sequence with capacity checks, where zero taken is a take failure rather than success. Serialized take calls the serialized with info variant. Dynamic take calls the dynamic with info variant and requires dynamic type support. Loaned take calls the loaned with info variant and rejects a non NULL output pointer on entry. Return wraps the rmw return.

Dispatch in `rclcpp` routes copy, serialized, and loaned takes into the type erased callback and drops intra process duplicates that match local publishers.

### Content filters and buffer hints

The filter options setter forwards to rmw with backup and restore, capped at 100 filter parameters. The set filter call forwards to rmw and mirrors the value into stored options. Getters read back rmw state. A support query reads the rmw handle flag. A buffer backend setter duplicates the backend string into rmw options, where NULL, empty, or cpu means CPU only. The new message callback setter forwards to rmw. The callback type aliases the rmw event callback type.

Other getters mirror publishers: actual QoS, loan gate, and matched publisher count.

**Sources**

- rcl/rcl/include/rcl/subscription.h
- rcl/rcl/src/rcl/subscription_impl.h
- rcl/rcl/src/rcl/subscription.c
- rclcpp/rclcpp/include/rclcpp/create_subscription.hpp
- rclcpp/rclcpp/include/rclcpp/subscription.hpp
- rcl/rcl/include/rcl/event_callback.h

## Services and Clients

Services and clients share one pattern: one input QoS fans out to two rmw entities, a request side and a response side, with dual actual QoS getters. Neither supports loaned messages. Think of `/add_two_ints` with `a` set to 2 and `b` set to 3: the client sends one request, the server takes it, then the server sends one response keyed by sequence number.

### Service side

The service struct holds one impl pointer. Options are QoS plus allocator, with no rmw options and no loaned flag. The impl holds options, actual request subscription QoS, actual response publisher QoS, the rmw handle, a service event publisher for introspection, the remapped service name, the type hash, and a wait set usage flag.

Init runs in phases. First allocate the impl. Then resolve the service name in service mode. Next warn when durability is transient local, which services do not use. Then call rmw create service with the single requested QoS. After that read back both actual QoS values, then patch the namespace convention flag on both. Finally register the type hash in the node type cache.

Fini unconfigures introspection, destroys the rmw service, unregisters the type, and frees the remapped name. Defaults use the services preset.

The data path has three calls. Take request calls rmw take request, where empty maps to take failed, and when introspection is on emits a request received event with sequence number and client GUID. A plain take wrapper exists for compatibility. Send response calls rmw send response, maps timeout, then emits a response sent event when enabled. A new request callback setter forwards to rmw.

The C++ service creates requests and headers, dispatches through a type erased callback, and sends the response. Take and send stay in `rcl`.

### Client side

The client struct holds one impl pointer. Options are QoS plus allocator. The impl holds options, actual request publisher QoS, actual response subscription QoS, the rmw handle, an atomic sequence number, a service event publisher, the remapped name, the type hash, and a wait set usage flag.

Init mirrors service init, then zeroes the sequence number and checks the type hash before registering it. Fini mirrors service fini. Defaults use the services preset.

The data path has matching calls. Send request loads the sequence number, calls rmw send request which fills the sequence value, then atomically exchanges the stored counter. When introspection is on, it fetches the client GUID and emits a request sent event. Take response calls rmw take response, where empty maps to take failed, and when enabled emits a response received event. A plain take wrapper exists for compatibility. A new response callback setter forwards to rmw.

The C++ client stores pending requests keyed by sequence number with promises, futures, or callbacks, sends through the C send, matches responses in its handler, and implements service ready checks plus timed wait for service with a graph event loop.

**Sources**

- rcl/rcl/include/rcl/service.h
- rcl/rcl/src/rcl/service_impl.h
- rcl/rcl/src/rcl/service.c
- rclcpp/rclcpp/include/rclcpp/service.hpp
- rcl/rcl/include/rcl/client.h
- rcl/rcl/src/rcl/client_impl.h
- rcl/rcl/src/rcl/client.c
- rclcpp/rclcpp/include/rclcpp/client.hpp
- rclcpp/rclcpp/src/rclcpp/client.cpp

## Timers and Clocks

Timers have no rmw data path. They are a clock plus a guard condition plus arithmetic. This is why sim time bugs surface here: a jump on `/clock` from second 1000 back to 900 must move every ROS timer consistently.

### Timer handle

The `rcl` timer holds one impl pointer, with callback and call info types that carry the timer, time since last call, and call times. Note the third callback parameter is `uintptr_t`, not pointer width by accident: Go callbacks and FFI trampolines must match that width.

The impl holds a clock pointer rather than a copy, a context pointer, an embedded guard condition by value rather than by pointer, atomic callback and callback data, atomic period, last call, next call, time credit, an atomic canceled flag, an allocator, reset callback data, and a wait set usage flag.

### Timer init and firing

Init runs in phases. First read the current time from the clock. Then init the internal guard condition. Next set last call to now, next call to now plus period, and canceled to the inverse of autostart. Callback and data start as atomics with data NULL. There is no heap copy of callback data. Finally, for ROS time, add a jump callback with change notification on and tight forward and backward thresholds.

Fini cancels the timer, removes the jump callback, and finalizes the guard condition.

Call with info also runs in order. First reject canceled timers. Then read now, exchange last call time, and load next call and period. Next advance next call by one period, with catch up when far behind. Zero period timers are always ready. Then invoke the typed callback with time since last call. Finally fill expected and actual call times.

The ready check compares time until next call against zero. Reset sets next call to now plus period, clears canceled, triggers the guard condition to wake the wait set, and fires the reset callback.

The jump handler saves next minus now as credit before the jump, restores it after, triggers the guard when a forward jump passes the next call, and resets to now plus period on any backward jump past the last call time. There is no deeper than one period threshold.

The C++ timer wraps this with a base plus generic functors and a wall timer that forces a steady clock. Call maps to call with info.

### Clocks

Clock types cover uninitialized, ROS time, system time, and steady time. Change kinds cover no change, activation, deactivation, and system no change. The jump struct pairs a change with a delta. The threshold struct gates callbacks by change flag and minimum forward and backward deltas. The clock struct holds the type, a jump callback list, a get now function pointer with data, and an allocator.

Steady time reads the steady source, system time reads the system source. ROS time reads an atomic, or falls back to system time when inactive. Init and fini cover each clock type, including ROS clock storage init with zero time and inactive flag. Difference requires matching clock types and returns signed finish minus start. Jump callbacks fire on change or when the delta passes a threshold, with uniqueness and removal by pointer. The ROS override enables and disables with activation callbacks, while set override computes the delta and fires before and after callbacks when active.

The C++ clock wraps these calls with jump handlers and sleep helpers. A time source subscribes to `/clock` and gates sim time through parameters.

**Sources**

- rcl/rcl/include/rcl/timer.h
- rcl/rcl/src/rcl/timer_impl.h
- rcl/rcl/src/rcl/timer.c
- rclcpp/rclcpp/include/rclcpp/timer.hpp
- rcl/rcl/include/rcl/time.h
- rcl/rcl/src/rcl/time.c
- rclcpp/rclcpp/include/rclcpp/clock.hpp
- rclcpp/rclcpp/src/rclcpp/clock.cpp
- rclcpp/rclcpp/src/rclcpp/time_source.cpp

## Guard Conditions

Plain words: a manual wake up signal. Trigger it and any wait set that holds it wakes up. A lifecycle change in a lifecycle manager uses this to interrupt an executor blocked in wait.

The struct holds a context plus impl. Options hold only an allocator. The impl holds the rmw handle, a flag for whether `rcl` allocated it, options, and the wait set usage flag.

Flows share an init helper: allocate the impl, or create a fresh rmw guard condition when no rmw handle was supplied. Plain init and init from rmw cover the two cases. The from rmw form borrows lifetime from the rmw handle, used for the node graph guard condition and for timer guards. Fini destroys the rmw guard only when `rcl` allocated it. Trigger calls rmw trigger and is safe to call concurrently with itself.

Users in `rcl` include timer reset and timer jump triggering the timer guard, nodes owning a graph guard condition, and graph wait helpers building a one guard wait set.

**Sources**

- rcl/rcl/include/rcl/guard_condition.h
- rcl/rcl/src/rcl/guard_condition_impl.h
- rcl/rcl/src/rcl/guard_condition.c

## Wait Sets

The wait set is the single blocking point in `rcl`. Everything else, from subscriptions to timers to services, funnels through it. An executor that spins at 100 Hz with 50 subscriptions and 10 timers spends most of its life inside the wait call.

### Handle layout

The wait struct holds six typed arrays with sizes: subscriptions, guard conditions, timers, clients, services, events, plus an opaque impl. The impl holds per type indices, rmw mirror arrays for subscriptions, guard conditions, clients, services, and events, an rmw wait set handle, a timer index, a context pointer, and an allocator.

### Init, add, clear, and resize

Zero init returns a static zero struct. Init stores context and allocator, computes twice subscriptions plus guards plus clients plus services plus events as the rmw condition count, where timers share guard storage, calls rmw create wait set, then delegates storage to resize. Fini destroys the rmw set, resizes all arrays to zero, and frees the impl.

Add helpers bounds check the typed array and store the pointer, with a parallel helper mirroring the rmw handle data pointer and bumping the rmw count. Timer adds are special: the timer guard pointer is appended after the plain guard conditions. Event adds are special: the rmw slot holds the event pointer itself, not its data field.

Clear zeroes arrays and resets indices and counts. It does not clear usage flags. Those flags are checked and reset inside the next wait. Resize reallocates typed arrays and rmw mirrors, where guard plus timer share one rmw storage block sized as the sum of both.

Double add detection uses a usage flag on subscriptions, guard conditions, timers, clients, and services only. The check runs at wait entry, not at add time, so adds always succeed. Events carry no usage flag and are never checked. Clear plus wait is the cycle that releases ownership.

### The wait call

The signature takes a wait set and an `int64` timeout. There is no NULL timeout at this layer. NULL infinite timeout exists one layer down in rmw wait.

Implementation runs as a pipeline. First reject empty and invalid sets. Then reject entities already owned by another wait set, for subscriptions, guard conditions, timers, clients, and services, with events exempt. Next compact timer guard conditions to the front of the rmw guard array. Then take the fast path: when ROS time override is on and a timer is already ready, skip blocking. Otherwise compute the minimum next call time across timers per clock type, nulling canceled timers out of this cycle. Next synthesize the timeout as the minimum of the caller timeout and the earliest timer deadline. Zero means poll. With no timers and a negative caller timeout, there is no valid deadline, so the rmw timeout argument stays NULL and the call blocks. The clamp to zero applies only when a timer deadline produced the minimum. Then call rmw wait on the rmw mirrors. After the block, sweep timers for readiness and null out non ready timers. Then map a non OK and non timeout rmw return to error. Next prune typed arrays where the rmw slot came back NULL. Finally return timeout only when rmw timed out and no timer is ready. Otherwise return OK with a possibly sparse set.

The `rclcpp` executor collects entities from callback groups into this wait set, waits, then picks the next ready executable in order: timers, subscriptions, services, clients, waitables. Guard conditions are also collected but never surface as executables; a notify waitable consumes them through the wait result. Locking is split: picking the next executable clears the taken flag for the chosen exclusive group, and executing restores it after the callback returns. Timer entries are additionally cleared from the wait result before call.

**Sources**

- rcl/rcl/include/rcl/wait.h
- rcl/rcl/src/rcl/wait.c
- rmw/rmw/include/rmw/rmw.h
- rclcpp/rclcpp/src/rclcpp/executor.cpp
- rclcpp/rclcpp/include/rclcpp/callback_group.hpp
- rclcpp/rclcpp/src/rclcpp/callback_group.cpp

## Graph and Discovery Queries

Graph queries answer the questions tools ask: who is there, who talks to whom, and with what types. `ros2 node list` and `ros2 topic info /chatter` both bottom out here.

By node queries each validate the node and the name plus namespace, then delegate to rmw: publisher names and types by node, subscriber names and types by node, service names and types by node, client names and types by node.

Global queries cover topic names and types, service names and types, names and types init and fini, node names with empty and NULL name checks, and node names with enclaves.

Counts are thin rmw wrappers for publishers, subscribers, clients, and services.

Wait for entities shares a helper that fast paths the count, builds a one guard wait set on the node graph guard condition, then loops wait with remaining time recomputation until the count is met or the timeout expires. Public wrappers wait for publishers, subscribers, clients, and servers.

Endpoint info covers publishers info by topic and subscriptions info by topic through a shared by topic helper, plus clients info by service and servers info by service through a shared by service helper, plus a server availability check.

The C++ graph layer calls all of these through the stored C node handle.

Discovery behavior itself lives below rmw. The C layer only queries the cache that the vendor driver fills. DDS uses SPDP for participants and SEDP for endpoints. Zenoh maps the same announcements to keys and queries.

**Sources**

- rcl/rcl/include/rcl/graph.h
- rcl/rcl/src/rcl/graph.c
- rclcpp/rclcpp/src/rclcpp/node_interfaces/node_graph.cpp

## Events

Events turn silent conditions into observable ones. A deadline missed on `/joint_states` at 100 Hz or a QoS mismatch on `/scan` surfaces here instead of as missing data.

### Publisher and subscription event kinds

Publisher events cover offered deadline missed, liveliness lost, offered incompatible QoS, incompatible type, and publication matched. Subscription events cover requested deadline missed, liveliness changed, requested incompatible QoS, message lost, incompatible type, and subscription matched.

Structs hold one impl pointer. The impl holds an rmw event plus an allocator.

### Init, take, and callbacks

Publisher event init maps each `rcl` type to its rmw constant, allocates the impl, and calls rmw publisher event init. Subscription event init does the same for the subscription side. Take calls rmw take event, where no event taken maps to take failed. Fini calls rmw event fini and frees. Set callback calls rmw event set callback. Support queries call the rmw support probe.

The callback type aliases the rmw event callback type.

Wait set integration adds events alongside data entities, so deadline misses and QoS incompatibilities can wake the executor.

### Incompatible QoS payloads

The rmw incompatible event status holds total count, count change, and last policy kind as a bit from the policy kind list. Aliases exist for requested and offered variants. Event IDs for requested and offered incompatible QoS live next to liveliness, deadline, message lost, incompatible type, and matched events.

A best effort publisher on `/scan` against a reliable subscription reports reliability as the last policy kind with rising counts.

**Sources**

- rcl/rcl/include/rcl/event.h
- rcl/rcl/src/rcl/event_impl.h
- rcl/rcl/src/rcl/event.c
- rcl/rcl/include/rcl/event_callback.h
- rcl/rcl/include/rcl/wait.h
- rcl/rcl/src/rcl/wait.c
- rmw/rmw/include/rmw/events_statuses/incompatible_qos.h
- rmw/rmw/include/rmw/event.h
- rmw/rmw/include/rmw/qos_policy_kind.h
- rmw/rmw/include/rmw/qos_string_conversions.h

## QoS Reference

QoS decides whether two endpoints that name the same topic ever connect. Name match without QoS match gives silence.

### Profile shape

The rmw QoS profile holds history, depth, reliability, durability, deadline, lifespan, liveliness, lease duration, and a flag for avoiding ROS namespace conventions. History covers keep last, keep all, and system default, with depth meaningful for keep last. Reliability covers reliable, best effort, and system default. Durability covers transient local, volatile, and system default. Unspecified deadline means implementation default. Lifespan is publisher only and names message expiry. Liveliness covers automatic, manual by topic, and system default, with lease duration naming how long silence means gone. The avoidance flag skips the `rt` prefix for native DDS topics.

The time base holds seconds plus nanoseconds, with infinite near max int64 and unspecified as zero.

Policy kind bits cover invalid, durability, deadline, liveliness, reliability, history, lifespan, depth, lease duration, and avoid conventions. String conversion helpers use lowercase names such as `best_effort`.

### Presets

| Preset | History and depth | Reliability | Durability | Extra |
| :--- | :--- | :--- | :--- | :--- |
| sensor data | keep last 5 | best effort | volatile | defaults |
| parameters | keep last 1000 | reliable | volatile | defaults |
| default | keep last 10 | reliable | volatile | defaults |
| services default | keep last 10 | reliable | volatile | defaults |
| parameter events | keep last 1000 | reliable | volatile | defaults |
| rosout default | keep last 1000 | reliable | transient local | lifespan 10 s |
| system default | system default | system default | system default | all defaults |
| best available | keep last 10 | best available | best available | deadline and liveliness best available |
| unknown | unknown history, depth 0 | unknown | unknown | unknown liveliness |

Depth system default is 0, meaning not set. Deadline, lifespan, and lease defaults are unspecified. Best available durations are infinite minus one.

The C++ QoS presets mirror these: parameters, services, parameter events, plus sensor data and system defaults. A Velodyne node typically uses sensor data for `/velodyne_points`, while `/rosout` uses rosout default with its 10 s lifespan.

### Offered versus requested matching

Plain words: the publisher states what it promises, the subscription states what it needs. They connect only when the offer satisfies the request. This check is called RxO matching.

| Policy | Flows | Blocked | Reason |
| :--- | :--- | :--- | :--- |
| Reliable publisher, best effort subscription | yes | - | offer exceeds request |
| Best effort publisher, reliable subscription | - | yes | request exceeds offer |
| Transient local publisher, volatile subscription | yes | - | history available |
| Volatile publisher, transient local subscription | - | yes | no history kept |
| Publisher deadline 10 ms, subscription 100 ms | yes | - | publisher faster than needed |
| Publisher deadline 100 ms, subscription 10 ms | - | yes | publisher too slow |

The API returns ok, warning, or error. Any system default or unknown policy yields a warning with a reason string. Errors come before warnings, joined by semicolons. The suggested reason buffer is 2048 bytes. No truth table ships in the `rmw` repo; the check lives in each vendor implementation.

Best available resolution picks reliability, durability, deadline, liveliness, and lease at endpoint creation from currently discovered peers, readable back through endpoint info queries, and never updated on later discovery. Two identical launches of `talker` and `listener` can otherwise resolve differently depending on startup order.

QoS override parameters in `rclcpp` declare per topic per entity per policy parameters and apply them before entity creation. Conversion to rmw profiles happens in the publisher and subscription options layers. Actual QoS readback happens in `rcl` with the publisher and subscription actual QoS getters.

### Publisher and subscription options

Publisher options hold a vendor payload pointer plus a unique network flow endpoints requirement, defaulting to not required. Subscription options hold a vendor payload, an ignore local publications flag that drops same context loopback, a flow endpoint requirement, content filter options, and acceptable buffer backends where NULL, empty, or cpu means CPU only and any means all. Defaults are no ignore, not required, no filter, and no backend hint.

**Sources**

- rmw/rmw/include/rmw/types.h
- rmw/rmw/include/rmw/time.h
- rmw/rmw/include/rmw/qos_policy_kind.h
- rmw/rmw/include/rmw/qos_string_conversions.h
- rmw/rmw/include/rmw/qos_profiles.h
- rclcpp/rclcpp/include/rclcpp/qos.hpp
- rclcpp/rclcpp/include/rclcpp/detail/qos_parameters.hpp
- rclcpp/rclcpp/include/rclcpp/publisher_options.hpp
- rclcpp/rclcpp/include/rclcpp/subscription_options.hpp
- rmw/rmw/src/publisher_options.c
- rmw/rmw/src/subscription_options.c

## RMW Handle and Function Reference

This section collects the vendor neutral contracts. Each group below names the behavior the contract requires.

### Return codes

Codes cover OK, error, timeout, unsupported, bad alloc, invalid argument, incorrect implementation, and node name non existent. Converters between rcutils, rmw, and rcl codes translate across layers. Creators return pointers with NULL on error plus a set error string; other functions return codes from the central `rcl` types list.

Error macros alias rcutils: check argument for NULL, check for NULL with message, set error message, and expect error is set for preserving a callee error such as a failed rmw create node. The idiom across `rcl` is validate, attempt, unwind in reverse on failure, and return the first failure.

### Context and init

The rmw context holds instance id, implementation identifier, init options, actual domain id, and an impl that may be NULL. The rmw init options hold instance id, implementation identifier, domain id where the default marker is max size, security options, discovery options, enclave which must be set before rmw init, allocator, and impl.

Flow in prose: first zero init options, then init options with an allocator, then set domain, enclave, discovery, and security, then rmw init, then rmw shutdown, then context fini. Actual domain id is set on success when a non default domain was requested.

### Nodes

Create node takes context, name, and namespace and returns NULL on error. It fails on bad name or namespace, invalid or shutdown context, and allocation failure. Destroy node frees even on error except for early invalid argument and wrong implementation cases. Callers should destroy all entities first, but only some vendor drivers verify this and return an error. The core contract does not guarantee the check. The graph guard getter returns a node owned guard triggered on join, leave, and match changes.

### Publishers

Calls cover allocation init and fini for preallocation hints, create and destroy publisher, the loaned trio of borrow, return from publisher, and publish loaned, plain publish with an optional allocation hint that may be NULL, serialized publish for the generic tools path, matched subscription count, actual QoS readback which resolves system defaults but not the namespace convention flag, serialized size query for sizing buffers, assert liveliness for manual by topic, wait for all acked with rmw time timeout where timeout maps to timeout code and non reliable is a no op OK, plus serialize and deserialize between ROS messages and serialized bytes.

### Subscriptions

Calls cover create and destroy subscription, matched publisher count and actual QoS getters, content filter set and get where empty filter resets and unsupported maps to unsupported code. All take forms are synchronous and nonblocking. OK with taken false means empty, never an error. Takes never deliver duplicates. Publish and take are thread safe except around finalize. Variants cover plain take and take with info for copies, sequence take for batches with capacity checks and order guarantee, serialized take and with info variants for tools, loaned take and with info variants where output must be NULL on entry and unsupported means the vendor has no loaning, plus loaned return.

Message info holds source and received timestamps, publication and reception sequence numbers with max uint64 when unsupported, publisher GUID, and an intra process flag. GUID storage is 16 bytes. Sequence helpers cover message and info arrays with init and fini.

### Services and clients

Calls cover create and destroy client, send request with sequence id where the sequence pairs request with response, take response with service info header, client actual QoS getters for the request publisher side and the response subscription side, create and destroy service, take request with service info header, send response where timeout fires when the reader is not ready, and service actual QoS getters for the request subscription side and the response publisher side.

Header types hold a 16 byte writer GUID plus a sequence number for the request id, and source plus received timestamps plus the request id for the service info. Take calls use the service info header; send response uses the legacy request id.

### Wait, guard, and callbacks

Containers hold subscription, service, client, event, and guard condition arrays plus a wait set with identifier, guard array, and vendor data.

Calls cover create, destroy, and trigger guard condition, create wait set with max conditions where zero means unbounded and safe across contexts, destroy wait set, and wait over subscriptions, guards, services, clients, and events with timeout where NULL blocks forever and zero polls, attaching type erased conditions, blocking until ready or timeout, then NULLing non triggered entries. Arrays stay caller owned. Concurrent use of one wait set or one entity is not safe.

Event callbacks cover new message, new request, new response, and event set. All are thread safe, may fire from a middleware thread, NULL clears, and unsupported maps to unsupported code.

### Serialized messages and time

The serialized message aliases the byte array with buffer, length, and capacity, plus init, fini, and resize. This is the generic tools path and the bridge path for bags and introspection. Topic echo without compiled types depends on it.

The rmw time holds seconds plus nanoseconds with infinite, unspecified, equal, total nanoseconds, from nanoseconds, and normalize helpers.

### Type support and discovery features

Static type support headers thread as opaque pointers through every create, serialize, and take call. The core validates non NULL and fully specified QoS and forwards the rest. Wrong implementation is caught by identifier comparison. Message bounds size preallocation and serialized size queries follow. The QoS compatibility check is covered in the QoS reference. Dynamic messages carry take variants and serialization support init from a library name. Feature probes cover message info sequence numbers, type discovery support, and dynamic message take support. Graph queries cover node names with and without enclaves, counts, GUID getters and comparison, server availability, and log severity. Endpoint info structs carry node name, namespace, type name, type hash, endpoint kind, GUID, and QoS profile, with array helpers and the service parallel.

**Sources**

- rmw/rmw/include/rmw/ret_types.h
- rmw/rmw/include/rmw/convert_rcutils_ret_to_rmw_ret.h
- rcl/rcl/src/rcl/common.c
- rcl/rcl/include/rcl/types.h
- rcl/rcl/include/rcl/error_handling.h
- rmw/rmw/include/rmw/init.h
- rmw/rmw/include/rmw/rmw.h
- rmw/rmw/include/rmw/types.h
- rmw/rmw/include/rmw/serialized_message.h
- rmw/rmw/include/rmw/time.h
- rmw/rmw/include/rmw/check_type_identifiers_match.h
- rmw/rmw/include/rmw/dynamic_message_type_support.h
- rmw/rmw/include/rmw/features.h
- rmw/rmw/include/rmw/topic_endpoint_info.h
- rmw/rmw/include/rmw/topic_endpoint_info_array.h
- rmw/rmw/include/rmw/message_sequence.h

## Types, Serialization, and Hashes

Bytes must decode identically on every vendor. A `sensor_msgs/msg/Image` of 640 by 480 published by `usb_cam` must land with the same pixels whether the subscriber uses CycloneDDS or FastDDS.

Static types pair generated structs with a type support handle per message and service. The C layer registers the hash per node in its type cache on entity creation and unregisters on fini.

The type hash is a fixed size hash of the type description. Discovery compares hashes before data flows. Mismatched hashes mean incompatible type events, not silent misdecode.

CDR is the wire encoding for DDS. Field order, alignment, padding, bounded sequences, and strings must match exactly. Serialize and deserialize convert between ROS structs and serialized bytes.

Dynamic types provide typeless take and runtime loaded type info for tools. Echoing an unknown Velodyne type depends on it.

Introspection services for types wrap the node type description service init in `rclcpp`. Services also publish event messages on the service event topic when introspection is enabled, covered in the next section.

**Sources**

- rcl/rcl/src/rcl/node_impl.h
- rclcpp/rclcpp/src/rclcpp/node_interfaces/node_type_descriptions.cpp
- docs/ros/03-type-system.md
- docs/ros/04-message-catalog.md

## Service Introspection and Rosout

Introspection and logging share a pattern: ordinary topics carry meta traffic about services and console output.

### Service event messages

The topic suffix is `/_service_event`. States are off, metadata only, and full contents.

The event publisher struct holds an event publisher, topic name, state, clock, publisher options, and service type support.

Implementation builds the topic name as service name plus suffix, so calling `/add_two_ints` produces `/add_two_ints/_service_event`. Zero, validity check, and publisher creation use a normal publisher on the event type. Init builds that topic name. Fini destroys the publisher and frees strings. Send rejects off state, nulls the payload in metadata mode, stamps with the service clock, builds the event with the type support create handle, publishes, then destroys the event. A state change helper flips modes.

Wiring lazily allocates the event publisher and changes state, or tears it down when set to off, for both services and clients. Take and send paths emit request received, request sent, response sent, and response received events with sequence numbers and GUIDs.

### Rosout logging

Configure calls apply global arguments, log levels, console output, rosout output, and external library handlers. Fini resets to console. Implementation keeps up to four handlers, an allocator, and enable flags for stdout, rosout, and external libraries.

Rosout publishers use topic `/rosout`, with one publisher per node keyed by logger name and a per node fini. The output handler builds a log message with timestamp, level, line, name, message, file, and function, then publishes.

Node init starts rosout publishing when both logging and the node option allow it, through the C++ base and logger naming helpers. Node logging services for getting and setting logger levels are created in `rclcpp` when enabled.

**Sources**

- rcl/rcl/include/rcl/service_introspection.h
- rcl/rcl/src/rcl/service_event_publisher.h
- rcl/rcl/src/rcl/service_event_publisher.c
- rcl/rcl/src/rcl/service.c
- rcl/rcl/src/rcl/client.c
- rcl/rcl/include/rcl/logging.h
- rcl/rcl/src/rcl/logging.c
- rcl/rcl/src/rcl/logging_rosout.c
- rclcpp/rclcpp/src/rclcpp/node_interfaces/node_base.cpp
- rclcpp/rclcpp/src/rclcpp/node_interfaces/node_logging.cpp
- rclcpp/rclcpp/src/rclcpp/node.cpp

## Network Flow Endpoints

When latency spikes on `/velodyne_points` at 10 Hz, the question is which interface and address the traffic uses. Flow endpoints answer it.

Types alias rmw. Queries cover publisher and subscription flow endpoints. Implementation validates the entity and the array, then delegates to the rmw publisher and subscription getters. The result names the interfaces and addresses in use, which is purely a debugging aid.

**Sources**

- rcl/rcl/include/rcl/network_flow_endpoints.h
- rcl/rcl/src/rcl/network_flow_endpoints.c
