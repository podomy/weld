# rmw_weld
Weld RMW shim over the Weld Go RTPS core (`rmw_weld` identifier).
Build with: `make -C rmw_weld linktest-in-jazzy`.
Install with: `make -C rmw_weld install PREFIX=/opt/weld`.
Run with: `RMW_IMPLEMENTATION=rmw_weld ros2 run <pkg> <node>`.
Coverage: init plus node plus pub plus sub plus QoS wired to the live Go
backend with XCDR encode and decode; service plus client plus guard plus
waitset wired to the in-memory bridge with wait polling subscriptions
into a take stash so ros2 topic echo wakes; graph backed by live
remotes with empty fallback; loans, content filters, network flow,
dynamic types, and events report honest UNSUPPORTED.
Go Backend mapping (PublishBytes, TakeBytes, JoinDiscovery,
ReportLiveliness) is wired through `internal/rmwbridge/weldffi`.
Interop is imperfect: hashless wildcard announce, single-process service
rendezvous, no loan transport.
