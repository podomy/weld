# rmw_weld
Weld RMW shim over the Weld Go RTPS core (`rmw_weld` identifier).
Build with: `make -C rmw_weld linktest-in-jazzy`.
Install with: `make -C rmw_weld install PREFIX=/opt/weld`.
Run with: `RMW_IMPLEMENTATION=rmw_weld ros2 run <pkg> <node>`.
Coverage: init plus node plus pub plus sub plus take plus serialized
plus QoS compare (all policies) plus max-size wired to the live Go
backend with XCDR encode and decode; service plus client wired to the
in-memory rendezvous with an RTPS cross-process leg (single-process
fast path kept); guard plus waitset wired with cond-wait (10 ms
quantum) over subscriptions, guards, services, clients, and
MESSAGE_LOST events; loans are owned-heap buffers with borrow, publish,
take, and return tracking (not zero-copy shared memory); content
filters attach through the backend with expression plus parameter
round-trip; node and publisher liveliness report through the backend;
dynamic takes deserialize through the runtime when linked; graph
by-node returns local endpoint entries while info-by-topic merges
locals (stored QoS, synthetic GID) with SEDP remotes (announced type,
raw 16-byte GUID); MESSAGE_LOST is the only backed event type, all
other event types report UNSUPPORTED per type; network flow endpoints
stay UNSUPPORTED; wait_for_all_acked returns immediately (best effort,
no acks); rmw_feature_supported is true only for dynamic takes.
Go Backend mapping (PublishBytes, TakeBytes, JoinDiscovery,
ReportLiveliness) is wired through `internal/rmwbridge/weldffi`.
Interop is imperfect: hashless wildcard announce, single-process
service fast path preferred, no loan transport.
Go Backend mapping (PublishBytes, TakeBytes, JoinDiscovery,
ReportLiveliness) is wired through `internal/rmwbridge/weldffi`.
Interop is imperfect: hashless wildcard announce, single-process service
rendezvous, no loan transport.
