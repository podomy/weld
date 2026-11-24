# Weld Invariants

This list is buildable. Every bullet is a testable promise in RFC style: MUST is required, MUST NOT is forbidden, SHOULD is recommended with a test either way. OPEN ITEM marks a decision the sources leave open; each names what must be decided before its gate passes.

The invariants are drawn from nineteen archived reference chapters covering the ROS 2 stack, the application layer, and 6000+ upstream issues, plus the architecture in `docs/design.md`. Where the invariants and the design disagree, the invariants win and the design gets amended.

How to read it: Part A is the wire. Part B is the execution core. Part C is what applications observe. Part D is everything cross-cutting. Each part stands alone; together they are the whole system.

## Part A: Wire Compatibility Core

### Type System and Serialization
- Weld MUST reproduce `IDL` to C mapping byte for byte where float maps to float, double maps to double, char maps to signed char, wchar maps to uint16_t, bool maps to bool, octet and uint8 map to uint8_t, string maps to a runtime string struct, arrays map to inline C arrays, and sequences map to sequence structs, and any deviation MUST be treated as incompatible.
- Weld MUST emit struct fields in `IDL` order with C offsetof matching generator output, and Weld MUST NOT reorder fields even when names and types stay the same.
- Weld MUST emit int32 constants with l suffix, uint32 constants with ul suffix, floats with f suffix, and bools as true or false.
- Weld MUST classify one struct definition as message, two as service with synthesized Request plus Response plus Event where Event holds ServiceEventInfo info plus bounded sequences of length one for request and response, and three as action with synthesized Goal plus Result plus Feedback plus SendGoal plus GetResult plus FeedbackMessage, and Weld MUST implicitly include the Time and UUID message definitions for every action.
- Weld MUST use type support library name <pkg>__typesupport and message symbol shape typesupport__get_message_type_support_handle__<pkg>__msg__<Name> with service and action parallels plus service event create and destroy functions, and lookup MUST match by string compare and MUST return zero when nothing matches.
- Weld MUST carry message handle with identifier string plus opaque data pointer plus handle function plus get type hash plus get type description plus get type description sources functions, service handle with introspection info plus stamp seconds plus nanoseconds plus 16 byte client `GUID` plus sequence number plus event create and destroy plus request plus response plus event supports, and action handle bundling goal plus result plus cancel services plus feedback plus status supports.
- Weld MUST implement type hash struct as one version byte plus 32 bytes of SHA-256 with `VERSION_UNSET` equal to 0, string form `RIHS01_` plus 64 lowercase hex digits for 71 chars total, and parse MUST accept only version 1 with exact length and MUST reject all other lengths or versions.
- Weld MUST compute type hash input as deep copy of description with every `default_value` deleted, then JSON dump with separators (', ', ': '), unsorted keys, ASCII enforced, then SHA-256 hex, and Weld MUST NOT use pretty printed or key sorted or default retaining JSON because that changes the digest.
- Weld MUST emit hash as static table with 8 bytes per row over 4 rows.
- Weld MUST use FieldType numeric IDs 0 not set, 1 nested, 2 to 9 signed and unsigned integers 8 to 64, 10 to 12 float plus double plus long double, 13 to 14 char plus wchar, 15 bool, 16 byte, 17 to 18 string plus wstring, 19 to 20 fixed string plus fixed wstring, 21 to 22 bounded string plus bounded wstring, 49 to 70 fixed arrays, 97 to 118 bounded sequences, 145 to 166 unbounded sequences, with symbolic names from `FIELD_TYPE_NOT_SET` through nested, integer, float, string, array, and sequence kinds, and Weld MUST NOT confuse this numbering with introspection `ROS_TYPE_*` numbering.
- Weld MUST cap type names at 255 chars, MUST use field type record with type id plus capacity plus string capacity plus nested type name, field record with name plus type plus default, individual type with name plus fields, full description with one description plus referenced descriptions, and source record with type name plus encoding plus raw file contents where encoding MUST be one of idl, msg, srv, action, dynamic, or implicit.
- Weld MUST build transitive closure with sorted references where every nested type brings its dependencies recursively until nothing new remains.
- Weld MUST expose introspection identifier `rosidl_typesupport_introspection_c`, member record with name plus type id plus string upper bound plus nested members plus key flag plus array flags plus size plus offsetof offset plus size, get, fetch, assign, and resize function pointers, container with namespace plus name plus member count plus struct size plus key presence plus init plus fini functions, and service introspection with request plus response plus event tables, and default value pointer MUST always be NULL.
- Weld MUST fill introspection tables with offsetof offsets, MUST set resize as fini plus init with NULL for non sequences, and MUST patch nested member pointers lazily on first call before use.
- Weld MUST implement sequences as data plus size plus capacity triples and strings with capacity including NUL terminator, and code that forgets the terminator MUST be treated as defective because it overruns or truncates by one.
- Weld MUST enforce bounds from `__MAX_SIZE` plus `__MAX_STRING_SIZE` enums plus introspection `string_upper_bound_`, `array_size_`, `is_upper_bound_`, and description capacity plus string capacity at serialize time.
- Weld MUST support init modes all, skip, zero, and defaults only with explicit zeroing or default fill behavior per mode, and Weld MUST honor the `is_rosidl_buffer` flag on `uint8[]` while ignoring other C++ runtime and buffer backend differences.
- Weld MUST handle corpus families `std_msgs`, `geometry_msgs`, `sensor_msgs` plus `SetCameraInfo` service, `nav_msgs` with map services, `trajectory_msgs`, plus diagnostic, shape, stereo, visualization, and `std_srvs` packages, and Weld MUST treat point cloud Python helpers as utility code and MUST NOT treat them as codegen target.
- Weld MUST preserve header semantics where Header carries stamp plus frame id.
- Weld MUST preserve header semantics where Time carries int32 seconds plus uint32 nanoseconds with Duration alongside.
- Weld MUST preserve header semantics where Odometry header frame is pose parent with twist in child frame.
- Weld MUST preserve header semantics where LaserScan stamp is first ray acquisition with angles about positive Z from positive X.
- Weld MUST preserve header semantics where Image stamp is acquisition in optical frame with X right plus Y down plus Z forward.
- Weld MUST preserve header semantics where Path header is path frame with fallback headers for poses.
- Weld MUST preserve header semantics where TransformStamped header is reference with child as target at stamp.
- Weld MUST preserve header semantics where NavSat stamp is ROS time with antenna frame.
- Weld MUST preserve header semantics where joint and trajectory stamps are recording and reference times.
- Weld MUST preserve header semantics where Marker lifetime zero means forever.
- Weld MUST treat fixed `float64[36]` covariance as inline with 8 byte alignment and no length prefix, fixed arrays as inline with 8 byte alignment and no prefix, all unbounded sequences with uint32 length prefix, and strings as length prefixed plus NUL in `CDR`.
- Weld MUST treat the single bounded sequence in the corpus, `shape_msgs`/`SolidPrimitive` float64 with max 3 dimensions, as the bounded path fixture, and Weld MUST test that path explicitly.
- Weld MUST NOT send constants on the wire and MUST emit them as Go consts only, MUST keep bool plus uint8 endian plus byte plus char distinct despite equal size, MUST validate encoding and format strings out of band, and MUST apply the single `IDL` default in the corpus which is the identity quaternion.
- Weld MUST apply float sentinel rules per message including discard outside range, NaN means unmeasured, IMU covariance rules with minus 1 meaning disregard and all zero meaning unknown, zeroed camera matrices meaning uncalibrated, signed byte row major occupancy data interpreted through resolution plus origin, and uint8 enums with NaN and Inf semantics.
- Weld MUST generate code in topological order builtin_interfaces then `std_msgs` then `geometry_msgs` then `sensor_msgs` plus `trajectory_msgs` plus `nav_msgs` then stereo plus shape plus visualization plus diagnostic packages so each package finds its imports.
- OPEN ITEM: source chapters leave bounded string wire behavior with no corpus fixture, so Weld MUST gate bounded string support as unproven until an external fixture proves it and MUST NOT claim corpus coverage for that path.

### CDR Envelope
- Weld MUST use `XCDR1` only on `DDS` paths and MUST NOT emit `XCDR2`, with Cyclone advertising `XCDR1` allowed.
- Weld MUST encode base envelope as 4 byte header followed by payload rebased 4 bytes forward where header bytes are 0x00 plus endian flag with 0x01 little endian and 0x00 big endian plus 0x00 plus 0x00, and services MUST add 8 byte `GUID` plus 8 byte sequence in negotiated endianness after the header.
- Weld MUST deserialize by rejecting nonzero first byte or second byte above 1, MUST derive byte swap from flag versus host order and swap the header, MUST include header in sizes, MUST pad serialized data to 4 bytes and zero the tail, and MUST produce normally empty key extraction for unkeyed ROS types.
- Weld MUST encode payload as plain `XCDR1` per introspection with aligned primitives plus 4 byte length prefixes for strings and sequences plus bool as byte plus wchar as 2 bytes, where a string payload MUST be 4 byte length followed by that many bytes padded to 4 byte boundary after the 4 byte header.
- Weld MUST validate header plus length before parsing, MUST fuzz truncated plus empty buffers, MUST handle string plus bounded kinds explicitly, MUST drop truncated plus empty plus extra field input with metric and MUST NOT panic or segfault the receiver.
- OPEN ITEM: sources report nonstandard string handling plus `XCDR1` versus `XCDR2` negotiation divergence plus truncated `CDR` faults plus cross vendor service payload misdeserialize, so Weld MUST pin `XCDR1` plus length checks and MUST prove behavior with interop plus fuzz gates.

### Topic and Type Naming
- Weld MUST map ROS topic chatter to `DDS` topic rt/chatter, MUST map service add_two_ints to rq/add_two_intsRequest for requests and rr/add_two_intsReply for replies, MUST use prefix rt for topics plus rq for requests plus rr for replies, and MUST use bare names when `avoid_ros_namespace_conventions` is set.
- Weld MUST form `DDS` type names by mapping double underscores to :: then forming <ns>::dds_::<Name>_ with Request_ plus Response_ suffixes for services, and demangle MUST reverse dds_:: plus trailing underscore and :: to slash with service request plus reply stripping requiring suffix at end.
- Weld MUST form value types from introspection where strings map to bounded string value types plus sequences map to span sequences plus vector<bool> is special cased, and any string plus sequence plus bool vector MUST clear self containment which disables loaning plus trivial memcpy path.
- Weld MUST create sertype with `XCDR1` only, MUST set memcpy safe flag exactly when self contained, MUST use no key, MUST size samples from value type size, MUST transfer sertype ownership at topic creation while keeping it for serialized forwarding.
- OPEN ITEM: sources note Humble peers without hashes use hashless keys while Iron peers include hashes, so Weld MUST treat hashless versus hashed keys as different rendezvous strings and MUST negotiate per peer.

### QoS Matching and Facade
- Weld MUST implement RMW QoS profile with history plus depth plus reliability plus durability plus deadline plus lifespan plus liveliness plus lease duration plus avoid namespace conventions flag.
- Weld MUST implement RMW QoS profile where history covers keep last plus keep all plus system default with depth meaningful for keep last.
- Weld MUST implement RMW QoS profile where reliability covers reliable plus best effort plus system default.
- Weld MUST implement RMW QoS profile where durability covers transient local plus volatile plus system default.
- Weld MUST implement RMW QoS profile where unspecified deadline means implementation default.
- Weld MUST implement RMW QoS profile where lifespan is publisher only naming message expiry.
- Weld MUST implement RMW QoS profile where liveliness covers automatic plus manual by topic plus system default with lease duration naming how long silence means gone.
- Weld MUST implement RMW QoS profile where avoidance flag skips rt prefix for native `DDS` topics.
- Weld MUST use time base seconds plus nanoseconds with infinite near max int64 and unspecified as zero, and string conversions with lowercase names such as best_effort.
- Weld MUST support presets sensor data keep last 5 best effort volatile, parameters keep last 1000 reliable volatile, default keep last 10 reliable volatile, services default keep last 10 reliable volatile, parameter events keep last 1000 reliable volatile, rosout default keep last 1000 reliable transient local with lifespan 10 s, system default all system default, best available keep last 10 best available, unknown history depth 0 with unknown policies, with depth system default 0 meaning not set, deadline plus lifespan plus lease defaults unspecified, and best available durations infinite minus one.
- Weld MUST apply offered versus requested RxO so reliable publisher to best effort subscription flows because offer exceeds request, best effort publisher to reliable subscription is blocked because request exceeds offer, transient local publisher to volatile subscription flows because history is available, volatile publisher to transient local subscription is blocked because no history is kept, publisher deadline 10 ms to subscription 100 ms flows because publisher is faster than needed, and publisher deadline 100 ms to subscription 10 ms is blocked because publisher is too slow.
- Weld MUST return ok plus warning plus error from compatibility check, MUST treat any system default or unknown policy as warning with reason string, MUST order errors before warnings joined by semicolons, and SHOULD use suggested reason buffer 2048 bytes.
- Weld MUST implement shared `DDS` truth table errors exactly covering best effort publisher with reliable subscription, volatile publisher with transient local subscription, default deadline publisher with non default subscription deadline, shorter subscription deadline than publisher deadline, automatic liveliness publisher with manual by topic subscription, and default lease publisher with non default subscription lease, and any single hit MUST yield incompatibility error.
- Weld MUST pass all other `DDS` combinations including reliable to best effort plus transient local to volatile plus equal values plus reverse liveliness, and MUST never check history plus depth plus lifespan plus avoid conventions.
- Weld MUST warn only when no error is present, covering both sides unknown and unknown publisher meeting strict subscription.
- Weld MUST resolve best available once at endpoint creation from currently visible peers, MUST store resolved profile, MUST expose it through actual QoS getters, MUST record which peers were visible at resolve time, and MUST never update it on later discovery.
- Weld MUST implement `DDS` subscriber best available as reliable exactly when at least one peer exists and all are reliable else best effort, transient local exactly when all peers are transient local else volatile, manual by topic exactly when all peers are manual else automatic, deadline plus lease default when all default else maximum, and publisher best available as always reliable plus always transient local plus manual when any subscriber is manual plus deadline plus lease default when all default else minimum, with services replacing best available with the services default profile.
- Weld MUST speak QoS on the wire as facade advertising reliable plus transient local with bounded keep last which satisfies every standard ROS subscription, MUST answer only the matching question with the profile, MUST implement behavior from core with backpressure between Weld endpoints plus bounded retention with explicit eviction for late joiners plus drop oldest with lost counters on overflow, MUST treat deadlines as monitoring firing alerts with topic plus endpoint names and MUST NOT silently unmatch a pair, and MUST surface genuinely exotic profiles loudly in logs plus metrics plus CLI with policy kind.
- Weld MUST translate forward exactly per the CycloneDDS table including keep last depth handling, reliable with infinite blocking, best effort with zero blocking, transient local plus durability service carrying history settings, lifespan and deadline from nanoseconds, lease handling, liveliness mapping, ignorelocal participant, writer data lifecycle without autodispose, and userdata with extra data plus encoded type hash.
- Weld MUST translate reverse per the CycloneDDS table including keep all mapping, direct best effort and reliable mapping, transient and persistent becoming unknown, and duration conversions.
- Weld MUST expose QoS override parameters per topic per entity per policy before entity creation, MUST convert to RMW profiles, and MUST read back actual QoS with actual getters.
- Weld MUST reject strictly required network flow endpoints.
- Weld MUST make mismatch loud in logs plus metrics plus CLI with topic plus endpoint pair plus policy kind, and silence MUST be treated as defect never state.
- OPEN ITEM: Cyclone forces concrete values, so Weld MUST resolve system defaults explicitly in unified core before entity creation.

### Discovery and Graph Index
- Weld MUST join ros_discovery_info plus builtin topic reads into one locked graph index with `GUID` GIDs plus demangled names plus translated QoS plus parsed hashes, and all graph queries MUST read the cache never `DDS` directly.
- Weld MUST maintain writer plus reader maps `GID` to entity info plus service map `GID` to service info plus participant map `GID` to participant info plus on change callback plus mutex where every method locks the mutex.
- Weld MUST store entity info with topic name plus topic type plus topic type hash plus participant `GID` plus QoS profile, service entity info with service type hash only, and participant info with node entity sequence plus enclave string.
- Weld MUST implement dual channel updates with `DDS` discovery channel add and remove paths where writer adds also record service hashes, ROS discovery channel from reliable participant entities topic with upsert plus overwrite plus remove plus add paths, and local channel mutating local participant and returning publishable message published atomically under node update mutex with rollback on publish failure for clients.
- Weld MUST run introspection under lock with writer plus reader counts by topic, entity info by topic filling node name plus namespace through `GID` lookup with unknown node plus bare `DDS` app sentinels for foreign endpoints, entity info by service matched reader to writer by node plus type, names plus types merged across readers plus writers, by node lists through node lookup, and node names with counts.
- Weld MUST fire set plus clear on change callback on every mutation with generation counter plus guard poke per change.
- Weld MUST describe the index as partition local describing the segment this node can currently see never global truth, MUST NOT assume two nodes see the same graph, MUST elect nothing, and MUST keep data plane leaderless.
- Weld MUST carry endpoint records with type hash plus unique `GID` from day one, MUST serve snapshots without blocking data path with hidden filtering plus same name disambiguation, and MUST offer optional static topology with presized tables and no thread for embedded targets.
- Weld MUST expose discovery range enum not set sentinel plus off plus localhost plus subnet plus system default, MUST cap static peers at 256 entries, MUST upgrade not set to localhost unconditionally before size zero early return, MUST warn when peers exist while discovery is off, and MUST query default range then static peers from environment when not set with discovery not off.
- Weld MUST resolve domain from environment `ROS_DOMAIN_ID` where unset plus empty leaves caller value, else parses rejecting non integral input plus range errors plus values above platform maximum, with configured id else zero as actual domain.
- Weld MUST implement participant init order create QoS plus set userdata to enclave=<enclave>; plus apply security properties when built with security plus create participant plus derive `GID` from `GUID`, with enclave from global arguments or slash default validated then security options loaded from environment for that enclave before init.
- Weld MUST create builtin readers for participant plus subscription topics plus one shared `DDS` publisher plus subscriber for all user endpoints, MUST create graph guard condition with on change trigger, MUST register local participant, and MUST start discovery thread waiting on private waitset over graph guard plus participant info plus builtin topics.
- Weld MUST use discovery info publisher QoS keep last depth 1 transient local reliable and subscription QoS keep all transient local reliable with ignore local publications.
- Weld MUST build Cyclone domain `XML` exactly as subnet default plus localhost disabling multicast plus adding localhost peer plus at most 32 auto participants plus off disabling multicast with participant index none plus tagged name, and MUST delete domain at last reference.
- Weld MUST update node to endpoint bindings from participant info messages, MUST add or remove participants by `GUID` with enclave from userdata ignoring self, MUST translate endpoint `DDS` QoS to RMW QoS plus parse type plus sertype hashes from userdata plus add entities with non alive instances removing, and MUST skip topics starting with `DCPS`.
- Weld MUST parse endpoint userdata format key=value;..., MUST parse type hash with key typehash where missing means zero hash plus OK and malformed means error, MUST encode where unset version encodes to empty string, MUST parse sertype hash with key sertypehash where missing means zero plus unsupported, and MUST encode accordingly.
- OPEN ITEM: sources show stale plus missing graph entries plus slow first topic list plus daemon divergence plus same name confusion plus incomplete tokens plus flaky multicast plus stale caches plus unexposed type hashes plus unsupported static topologies plus cross vendor localhost leaks, so Weld MUST version index with generation plus expose type hash plus `GID` and MUST NOT present partial view as global truth.

### DDS Transport Specifics
- Weld MUST create publishers by adapting best available reliability plus durability plus liveliness against discovered subscriptions before creating `DDS` writer.
- Weld MUST reject strictly required network flow endpoints.
- Weld MUST derive type name from message type name plus fully qualified topic with rt prefix unless avoidance set.
- Weld MUST build value type plus check self containment plus size plus create sertype plus optional dynamic type plus create `DDS` topic transferring sertype ownership while keeping it for serialized forwarding.
- Weld MUST attach event listener plus translate QoS plus create writer plus record instance handle plus `GID`.
- Weld MUST report loan availability exactly as the vendor would (self-contained type plus shared memory available) for can_loan_messages queries from ROS peers, without implementing loan borrow or return APIs in v1.
- Weld MUST create subscriptions with read condition with any state, MUST install data available listener calling callback or bumping unread count, and MUST provide no content filter or loan support on Cyclone subscriptions.
- Weld MUST publish with current time, MUST forward serialized publish by building serdata from `CDR` buffer without deserializing, and MUST NOT require a loaned sample to do so.
- Weld MUST take by looping `DDS` take one sample at a time skipping invalid data, converting sample info to message info with publication handle as `GID` keeping source timestamp stamping received time as now and leaving sequence numbers unsupported, MUST batch sequence take then compact valid samples to front, and MUST use `CDR` take with shared memory fast path for serialized take.
- Weld MUST map services subscribing to rq/<svc>Request plus publishing rr/<svc>Reply and client doing reverse with same avoidance rule so bare name is used when avoidance set.
- Weld MUST use header variant sertype with 16 byte header, MUST carry userdata client or service id hex plus sertype hash, and MUST derive client plus service ids from participant `GID` plus big endian counter.
- Weld MUST wrap send with header plus data written with timestamp where request header `GUID` is client publisher instance handle with atomic increment sequence, yielding service packet 4 byte `CDR` header plus 8 byte `GUID` plus 8 byte sequence plus `XCDR1` user fields.
- Weld MUST split response handling 16 byte writer `GUID` into request `GUID` plus publisher handle, MUST wait up to 100 ms for matched response reader where gone reader is OK and possible reader is timeout, MUST rebuild take request id from header `GUID` plus publication handle, and MUST filter client side on own publisher handle.
- Weld MUST check server availability with graph counts for request readers plus reply writers then match on service id userdata with fallback to non empty reader plus writer sets.
- Weld MUST implement waitset with `DDS` waitset plus trigger list plus lock plus in use flag plus entity snapshots.
- Weld MUST attach global dummy guard keeping empty waitsets from returning immediately.
- Weld MUST reject concurrent use.
- Weld MUST reattach only when cached pointers changed.
- Weld MUST attach subscription plus guard plus service plus client conditions plus per entity event status masks.
- Weld MUST wait with nanosecond timeout else forever.
- Weld MUST sort triggers plus null non fired entries while taking guard conditions.
- Weld MUST handle active events through status changes.
- Weld MUST report timeout only when nothing but sentinel fired.
- Weld MUST map event masks to liveliness changed plus lost, requested plus offered deadline missed, requested plus offered incompatible QoS, message plus sample lost, incompatible types both sides, and subscription plus publication matched, with takes reading `DDS` status structs.
- Weld MUST memcpy `GUID` bytes into gid storage but MUST return instance handle not `GUID` for publisher `GID` call, and message info `GID` MUST be publication handle.
- OPEN ITEM: sources report service request ID headers plus native keyed types unable to talk to ROS types, so Weld MUST treat services plus keyed topics as high risk paths with explicit interop tests and MUST assume ROS types normally unkeyed.

### Vendor Difference Handling
- Weld MUST assume `XCDR1` plus shared topic plus type naming plus shared graph cache shape through unified core with no vendor driver embedded, and core MUST never import transport packages with transports implementing publish bytes plus take bytes plus join discovery plus report liveliness while matching plus QoS plus routing plus timeouts plus retries plus state machines live once in core.
- Weld MUST resolve system default history plus reliability plus durability plus liveliness plus unknown parallels explicitly in core before entities would be created so CycloneDDS peers converge, and Weld MUST never rely on keyed topics plus content filters plus loaning for CycloneDDS paths.
- Weld MUST keep every fork buffer extension behind capability detection with base transport fallback, MUST always create base reader plus writer so fallback matching works, and MUST NOT assume buffer channel GIDs equal base GIDs without parsing.
- Weld MUST enforce Cyclone validation rejecting unknown policies plus out of range depths at creation, and Weld MUST reject unknown policies plus bad depths early in core.
- Weld MUST program Cyclone transient local durability service with history settings, and Weld MUST verify late joiner behavior per policy rather than hoping.
- Weld MUST map unspecified lease to infinity on Cyclone, and MUST normalize lease handling in core with explicit tests.
- Weld MUST handle time conversion where Cyclone uses total nanoseconds with `DDS` infinity, and MUST use saturating clamp at max seconds consistently.
- Weld MUST enforce one `DDS` topic per name with refcount where type mismatch on existing topic name fails creation, MUST minimize status masks to matched plus data available with counts from listener tracked `GUID` sets, and MUST report no mangle services unsupported in endpoint info queries.
- Weld MUST observe Cyclone gating on shared memory availability when answering loan availability queries from ROS peers, without implementing loan borrow or return APIs in v1, and Weld itself MUST copy once at ingress and share by reference internally independent of Cyclone loaning.
- Weld MUST negotiate each peer individually with type hash present meaning Iron or newer plus introspection topics meaning service tracing available plus message hashes identifying exact field sets, MUST key registry by name plus hash so Humble publisher plus Rolling subscriber each match own version simultaneously, MUST parse tolerant and send strict so new keys plus fields never break old peers, and MUST prove one binary across distros with a tested release matrix in the testbed (live gates need Docker peers; CI has no fleet).
- Weld MUST use finite service waits with clear errors, MUST never assume symmetric request plus response discovery, MUST test bringup under CycloneDDS with parallel lifecycle transitions, and MUST route services exact one to one with per request timeouts where gone clients error one call never stall server.
- OPEN ITEM: sources disagree on depth zero handling where system default depth 0 means not set versus Cyclone mapping to keep last 1 versus publisher always reliable plus transient local, so Weld MUST document chosen normalization explicitly and MUST expose resolved values through actual getters for verification.

## Part B: Execution Core

### Structured Delivery Instead of Executors
- The core MUST deliver each subscription to a buffered channel owned by that subscriber, with depth from QoS and explicit overflow policy. User code ranges over the channel, runs callbacks in its own goroutines, or attaches to a supervision group with declared concurrency: serial (ordered, one at a time) or pooled (bounded parallelism with fairness). Serial replaces exclusive ordering without starvation ambiguity; pooled replaces reentrant ordering without unbounded fan out.
- No warmup spins, no per cycle allocation, no shared status conditions. Ordering guarantees are stated per mode and tested.
- Delivery MUST be introspectable for tests so a test can assert which path a message took, and the publish path under try publish MUST be wait free in practice with no allocation, no blocking mutex, bounded queues, and serialization without page faults.
- Copy once at ingress is the default, not an optimization mode. The first copy happens where data enters the system, from the device or kernel buffer Weld does not own. After that everything shares by reference: multiple subscribers, history caches, retransmits, intra process handoff. One copy, reference counted, freed when the last reader releases it. Users never see loans; core picks the shared path wherever the transport allows and serializes only at the network edge.

### Services as Calls and Actions as Workflows
- Services MUST be plain RPC with context deadlines: exact one to one routing, per call timeouts, gone servers and gone clients error one call. Actions MUST be durable workflows in core: goal admission, cancel propagation, feedback streaming with buffering before handle attachment, result caching independent of handle lifetime, terminal states guaranteed. Timeouts attach at the transition that starts them.
- The wire stays standard (request plus reply topics, goal plus feedback plus result plus status topics), but Weld users never hand assemble the machine.
- Take plus dispatch MUST be one atomic step so take with nothing ready returns an empty signal with a metric rather than crashing.
- Result timeout MUST start at completion, not at acceptance.
- Service discovery MUST be event driven with finite waits and clear errors, never infinite waits, and MUST NOT assume symmetric request and response discovery.
- Deactivation MUST terminal result in flight goals and MUST reject new work, with 500 ms deactivate timeout bounding teardown of in flight work.
- Receding horizon goals with wall clock stamps against sim time MUST NOT freeze silently; a clock mismatch MUST surface as an explicit error.
- RESOLVED: core carries an explicit per-goal timeout defaulting to 60 s from acceptance (`DefaultActionGoalTimeoutNanos` in `internal/core/action.go`); unset means the default, never a silent infinite wait. OPEN ITEM: exact activation gap semantics (activate and deactivate for subscriptions, action clients and servers, service clients and servers) MUST be decided before Nav2 conformance is declared.

### Config as a Versioned Store
- Parameters MUST form a versioned store with schema, types, validation, atomic updates, watches, and history. Startup applies files plus CLI plus code in a fixed precedence with one batched event, not N events.
- Schemas MUST validate generated `YAML` before launch, catching missing wrappers, wrong units, and unknown joints at build time.
- `on_set` validation MUST split from post set apply; apply MUST NOT run when validation rejects.
- There MUST be no fixed size parse buffers, and remap MUST apply as a pure first class rewrite table at creation with first match across global plus local arrays winning.
- OPEN ITEM: whether batched startup is atomic and whether remap can update after creation MUST be decided before launch parity is declared.

### Time as One Domain per Context
- Each context MUST have one clock domain with explicit wall, steady, and sim sources plus a construction time fallback that is never silent.
- Steady timers MUST be the default. Per subsystem rate choices (Nav2 wall loops versus ROS behaviors) MUST be declared policy, never leaked across contexts by default.
- `use_sim_time` agreement on action clients MUST be enforced with a loud error on mismatch.
- Jump callbacks MUST be allocation free with ordered deregistration, and backward jumps MUST clear or invalidate caches.
- Overload policy MUST be explicit and observable with backlog counters; silent coalescing that hides backlog MUST NOT be allowed.
- Deterministic fake clocks MUST drive chaos tests covering bag loops, backward jumps, pauses, and multi rate simulation.

### Serial and Pooled Supervision
- Serial groups MUST deliver callbacks ordered one at a time with no starvation ambiguity, MUST preserve insertion order for ready entities of equal priority, and MUST guarantee that destroying a group while spinning skips pending callbacks and joins cleanly without segfault or deadlock.
- Pooled groups MUST provide bounded parallelism with fairness and a declared worker bound, MUST NOT spawn one timer plus one thread per goal, MUST NOT permanently drop an exclusive style declaration, and a soak with parallel goals MUST show worker count at or below the bound.
- Pooled groups MUST guarantee fairness so group isolation starvation MUST NOT occur, verified by a test with contending subscriptions where each ready subscription makes progress.
- A logger on `/odom` at 50 Hz SHOULD use a pooled group with depth 100 and tolerate drops with a counter while a controller uses a serial group with depth 10 and requires order, with independent overflow counters.
- Detached supervision groups with dedicated threads MUST serve where ROS uses detached executors, and MoveIt private nodes with dedicated TF listener threads MUST run unmodified on this mapping.
- The controller list guarding MUST use priority inherit mutexes plus double buffered swaps so the real time thread never blocks on service work, and command buffers MUST show non real time subscription writes plus real time update reads with NaN initialization until the first write.

### Service Introspection and Dual QoS
- One input QoS MUST fan out to two entities on the wire with a request side and a response side, and the core MUST expose dual actual QoS getters for request publisher plus response subscription on clients and request subscription plus response publisher on services.
- The core MUST warn when durability is transient local on services, since services do not use it, with service name and policy kind.
- The core MUST emit request sent plus response received on clients with sequence number and client `GUID`, and request received plus response sent on servers, when introspection is enabled, and MUST NOT emit when disabled.
- Actual QoS readback MUST resolve system defaults to concrete values but MUST leave the namespace avoidance flag unresolved.
- Timeout on send response when the reader is not ready MUST map to a timeout error with a service name metric.

### Goal Admission, Preempt, Feedback, Terminal States
- Managed action servers MUST gate goals on active state: MUST reject goals when inactive and MUST accept cancel when active, with a 500 ms admission and deactivation timeout bounding both.
- Servers MUST implement one active plus one pending preempt slot with a detached work thread; back to back replacement MUST preempt deterministically and MUST NOT preempt both goals on real time kernels.
- Deactivation MUST terminal result in flight goals and MUST reject new work, matching the gripper deactivation contract, with `SIGINT` teardown tests ending in terminal results rather than use after free.
- Feedback at 5 Hz MUST buffer even if the client attaches late, the result MUST survive after the server handle closes independent of goal lifetime, and take plus dispatch MUST be one atomic step returning an empty signal with a metric instead of crashing.
- Result timeout MUST start at completion, NOT at acceptance, and timeouts MUST attach at the transition that starts them.
- Receding horizon goals with wall clock stamps against sim time MUST NOT freeze silently; a clock mismatch MUST surface as an explicit error with goal stamp and active clock domain.
- Treat success with dirty diagnostics as failure; limits, joint names, touch links, QoS, and clocks MUST be validated before sending anything to MoveIt.
- RESOLVED: core carries an explicit per-goal timeout defaulting to 60 s from acceptance, with result retention 60 s from completion (`DefaultActionGoalTimeoutNanos` and `DefaultActionResultTimeoutNanos` in `internal/core/action.go`); zero goal time takes the default, never waits forever. OPEN ITEM: exact activation gap semantics for subscriptions, action clients and servers, and service clients and servers MUST be decided before Nav2 conformance is declared.

### Lifecycle Supervision, Single Node, No Elections
- There MUST be one node type with lifecycle as a supervision policy attached to entities, not a second node class, and the supervisor MUST own startup and shutdown order: forward for configure plus activate, reverse for cleanup plus deactivate plus shutdown.
- Late created entities MUST auto activate when the node is active;Node main bringup MUST support argument forwarding where CLI argv appends to node options, and node options MUST allow undeclared parameters with auto declare from overrides.
- The supervisor MUST serve standard lifecycle services with transition callbacks and lifecycle IDs, MUST declare bond parameters with 0.25 s heartbeat period and autostart flag, MUST register a preshutdown callback, MUST check bond liveness with bond topics plus 200 ms check timers plus respawn polling, and MUST honor manager bond timeout 4.0 s and service timeout 5.0 s with respawn after bond loss demonstrated in test.
- MoveIt main move group node with undeclared auto declare MUST be served, multiple nodes per process with private name remaps MUST be supported, and components MUST load in composable containers with clock plus supervision injection where ROS expects executor plus clock injection.
- The core MUST elect nothing and the data plane MUST stay leaderless: every topic, service, and parameter has exactly one owner, its creator; discovery merges deterministically and never by vote; where ROS expects exactly one manager, map server, or bag recorder, configuration names it explicitly and the core reports absence loudly through the graph instead of holding an election.
- Bringup sequences and action preemptions MUST follow deterministic rules over sorted names and generations so two nodes in the same partition compute the same order independently.

### Jump Handling and Timer Behavior
- Jump callbacks MUST be allocation free with ordered deregistration; the timer jump handler MUST save next minus now as credit before the jump, MUST restore it after, MUST trigger the guard when a forward jump passes the next call, and MUST reset to now plus period on any backward jump past the last call time.
- The ROS time jump path MUST add a jump callback with change notification on plus tight forward and backward thresholds; backward sim resets MUST NOT stall control nodes.
- Timer init MUST read current time from the clock, MUST set last call to now and next call to now plus period, MUST set canceled to the inverse of autostart, and MUST use atomics for callback, callback data, period, and canceled flag with no heap copy of callback data.
- Timer call MUST reject canceled timers, MUST advance next call by one period with catch up when far behind, MUST invoke the typed callback with time since last call, and MUST fill expected plus actual call times; zero period timers MUST always be ready.
- Reset MUST set next call to now plus period, MUST clear canceled, MUST trigger the guard condition, and MUST fire the reset callback.
- Wall timers MUST NOT hang pooled supervision under load.
- OPEN ITEM: timer overload coalescing versus skip ahead versus drift MUST have its default recorded before control soak passes.

### Config Batching, Precedence, Watches, Remap
- Startup MUST apply files plus CLI plus code in a fixed precedence with one batched event rather than N events; wildcard versus fully qualified name precedence MUST be tested and documented.
- The store MUST support undeclared auto declare, param plus params file args, per node namespaces, rewritten `YAML` per namespace with autostart and zone rewrites, and `SDF` parameter files appended as node arguments with sim time forced on.
- Watches MUST be versioned with history, and remap MUST apply as a pure first class rewrite table at creation with first match across global plus local arrays winning, node scoped expansion, and full name validation.
- OPEN ITEM: whether batched startup is atomic and whether remap can update after creation MUST be decided before launch parity is declared.

### Overflow and Backpressure Policy
- Each channel MUST declare an overflow policy at creation; the default for bounded topics MUST be drop oldest with a lost counter, and drops MUST be visible in counters, never silent.
- Keep all MUST be bounded with an explicit drop or block policy plus a lost counter rather than unbounded growth; small keep last plus reliable under repair storm MUST bound retransmit rate and MUST surface a storm counter, with a bounded memory test under a slow consumer.
- The core MUST apply backpressure between Weld endpoints instead of blind drops or unbounded growth.
- QoS depth MUST be honored at delivery or MUST be refused at creation loudly with topic, endpoint pair, and policy kind; silent QoS loss MUST NOT occur.
- Message filter and routing drops MUST carry machine readable drop reasons with a test asserting the reason code for each dropped message.

### Discovery as an Index, No Elections
- Discovery feeds one index with a generation counter and a change hook. The index is explicitly partition local: it describes the segment this node can currently see, never a global truth. No quorum is pretended, no query blocks across a dead link, and no Weld decision assumes two nodes see the same graph.
- Weld elects nothing. The data plane is leaderless by construction. Every topic, service, and parameter has exactly one owner, its creator. Discovery merges observations deterministically through truth tables, demangling, and hashes, never by vote. Where ROS expects exactly one of something, configuration names it explicitly; if it dies, the core reports the absence loudly through the graph instead of holding an election.
- Bringup sequences and action preemptions MUST follow deterministic rules over sorted names and generations, so two nodes in the same partition compute the same order independently.
- Graph snapshots MUST serve tools with hidden filtering and same name disambiguation without blocking the data path, and an optional static topology with presized tables and no thread MUST serve embedded use.


## Part C: Application Conformance (summary)

TF, CLI, bags, launch, RViz, drivers, Nav2 and MoveIt conformance is pinned in `docs/conformance.md` and proven in `docs/testbed.md`. Rules:

- `/tf` reliable volatile depth 100, `/tf_static` reliable transient-local with replay; lookup errors are exact strings; jumps clear caches.
- CLI verbs (`list`, `echo`, `pub`, `info`, `call`, `params`, `doctor`, `node info`) read from the graph index with type hashes.
- Bags read and write exchange formats with recorded QoS; launch accepts ROS args with last-wins ordering.
- Nav2 and MoveIt run unmodified on the interaction surface; no `rmw` internals are reproduced.

## Part D: Cross-Cutting (summary)

Logging, metrics, shutdown, determinism, and release rules:

- Every mismatch is loud in logs, metrics, and CLI with topic, endpoint pair, and policy kind; silence is a defect.
- Every blocking call takes `context.Context`; teardown is idempotent and never deadlocks.
- Each feature passes three gates: unit tables with fake clocks, interop against `rclcpp` on CycloneDDS, chaos under race detection.
- One binary with no C ABI; Humble, Jazzy, and Rolling negotiated per peer; live matrix runs in the testbed, not in CI.
