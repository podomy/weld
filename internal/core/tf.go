// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package core

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"sort"
	"strings"
	"sync"
)

// TF topic split with latched statics.
//
// Dynamic transforms flow on TFTopic as volatile reliable keep last depth
// TFHistoryDepth. Static transforms flow on TFStaticTopic as transient local
// reliable latched depth TFStaticHistoryDepth. Late joiners replay the full
// static set through SnapshotStatics, never through dynamics. Static
// overwrite is replace on rebroadcast keyed by parent plus child. Clear is
// sticky: Clear drops dynamics and preserves statics, so latched data
// survives dynamic resets; use ClearStatics to drop the latched set.
// Lookup implements the TF taxonomy with exact strings and zero time means
// latest. Jump handling clears dynamics on backward jumps and on sim
// activation changes while preserving statics, because statics ignore time.
// Timeout accounting uses the buffer clock passed to LookupWithTimeout.
// Lookup never holds the store lock across callbacks: it snapshots under
// lock then searches and composes without the lock.

// TFTopic carries dynamic transforms as volatile reliable.
const TFTopic = "/tf"

// TFStaticTopic carries latched static transforms as transient local reliable.
const TFStaticTopic = "/tf_static"

// TFHistoryDepth bounds dynamic retention per frame pair.
//
// Derivation: matches FacadeHistoryDepth, large enough for 10 Hz to 100 Hz
// streams to survive a short outage while staying bounded. Counts bound
// entries; transports bound bytes alongside counts.
const TFHistoryDepth = FacadeHistoryDepth

// TFStaticHistoryDepth is the wire depth for the latched static topic.
//
// A single latched message holds the full static set, so depth one suffices
// on the wire. The store itself keeps one entry per frame pair and replays
// the full set to late joiners through SnapshotStatics, independent of this
// wire depth.
const TFStaticHistoryDepth uint64 = 1

// TFQoS returns the profile Weld publishes and subscribes for dynamic
// transforms: reliable volatile keep last depth TFHistoryDepth.
func TFQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryKeepLast,
		Depth:       TFHistoryDepth,
		Reliability: ReliabilityReliable,
		Durability:  DurabilityVolatile,
		Liveliness:  LivelinessAutomatic,
	}
}

// TFStaticQoS returns the profile Weld publishes and subscribes for static
// transforms: reliable transient local keep last depth TFStaticHistoryDepth.
func TFStaticQoS() QoSProfile {
	return QoSProfile{
		History:     HistoryKeepLast,
		Depth:       TFStaticHistoryDepth,
		Reliability: ReliabilityReliable,
		Durability:  DurabilityTransientLocal,
		Liveliness:  LivelinessAutomatic,
	}
}

// Vector3 is a translation in meters.
type Vector3 struct {
	X float64
	Y float64
	Z float64
}

// Quaternion is a rotation as x, y, z, w.
type Quaternion struct {
	X float64
	Y float64
	Z float64
	W float64
}

// Transform is a rigid child pose in the parent frame.
type Transform struct {
	Translation Vector3
	Rotation    Quaternion
}

// TransformStamped is one stamped edge with parent frame plus child target.
type TransformStamped struct {
	StampNanos   int64
	FrameID      string
	ChildFrameID string
	Transform    Transform
}

// IdentityTransform returns zero translation with unit quaternion.
func IdentityTransform() Transform {
	return Transform{Rotation: Quaternion{X: 0, Y: 0, Z: 0, W: 1}}
}

// NormalizeFrameID returns the lookup key form of a frame id.
//
// It trims surrounding whitespace and strips all leading slashes, so
// "/base_link" and "base_link" name the same frame. Empty input stays empty
// and is rejected by stores and lookups as no such frame.
func NormalizeFrameID(id string) string {
	trimmed := strings.TrimSpace(id)
	return strings.TrimLeft(trimmed, "/")
}

// tfKey identifies one directed edge from parent to child in normalized form.
type tfKey struct {
	parent string
	child  string
}

// TFStore holds dynamic plus latched static transforms.
//
// The zero value is unusable because the maps are nil; build stores with
// NewTFStore and do not copy a store after first use.
type TFStore struct {
	mu       sync.Mutex
	dynamics map[tfKey][]TransformStamped
	statics  map[tfKey]TransformStamped
}

// NewTFStore returns an empty store.
func NewTFStore() *TFStore {
	return &TFStore{
		dynamics: make(map[tfKey][]TransformStamped),
		statics:  make(map[tfKey]TransformStamped),
	}
}

// StoreDynamic appends volatile samples keyed by frame pair.
//
// Each element is validated: normalized frame and child must be nonempty,
// stamp must be positive because zero means latest on lookup, and the
// quaternion must be finite with nonzero norm. Invalid input returns an
// error and stores nothing from the whole call, so a bad batch never
// partially applies. Valid batches append per key and drop the oldest when
// a key exceeds TFHistoryDepth, matching drop oldest with no silent growth.
func (s *TFStore) StoreDynamic(transforms ...TransformStamped) error {
	for i := range transforms {
		err := validateDynamicTransform(&transforms[i])
		if err != nil {
			return fmt.Errorf("core: store dynamic index %d: %w", i, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ts := range transforms {
		key := tfKey{parent: NormalizeFrameID(ts.FrameID), child: NormalizeFrameID(ts.ChildFrameID)}
		stored := TransformStamped{
			StampNanos:   ts.StampNanos,
			FrameID:      key.parent,
			ChildFrameID: key.child,
			Transform:    ts.Transform,
		}
		history := s.dynamics[key]
		history = append(history, stored)
		if uint64(len(history)) > TFHistoryDepth {
			history = append([]TransformStamped(nil), history[len(history)-int(TFHistoryDepth):]...)
		}
		s.dynamics[key] = history
	}
	return nil
}

// StoreStatic replaces latched samples keyed by frame pair.
//
// Each element is validated: normalized frame and child must be nonempty,
// stamp must be nonnegative, and the quaternion must be finite with nonzero
// norm. Rebroadcast of the same parent plus child replaces the entry in
// place, pinning replace on rebroadcast. Invalid input returns an error and
// stores nothing from the whole call.
func (s *TFStore) StoreStatic(transforms ...TransformStamped) error {
	for i := range transforms {
		err := validateStaticTransform(&transforms[i])
		if err != nil {
			return fmt.Errorf("core: store static index %d: %w", i, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ts := range transforms {
		key := tfKey{parent: NormalizeFrameID(ts.FrameID), child: NormalizeFrameID(ts.ChildFrameID)}
		s.statics[key] = TransformStamped{
			StampNanos:   ts.StampNanos,
			FrameID:      key.parent,
			ChildFrameID: key.child,
			Transform:    ts.Transform,
		}
	}
	return nil
}

// Clear drops dynamics and preserves statics.
//
// Statics are sticky across Clear: latched data survives dynamic resets and
// jump clears, and late joiner replay still serves the full static set
// after Clear. Use ClearStatics to drop the latched set. Clear on an empty
// store is a no op.
func (s *TFStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dynamics = make(map[tfKey][]TransformStamped)
}

// ClearDynamics drops only dynamic samples.
func (s *TFStore) ClearDynamics() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dynamics = make(map[tfKey][]TransformStamped)
}

// ClearStatics drops the latched static set.
//
// The cleared state is sticky: SnapshotStatics stays empty and lookups that
// need statics report empty or connectivity until a new StoreStatic
// rebroadcast arrives. Clearing twice stays cleared.
func (s *TFStore) ClearStatics() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statics = make(map[tfKey]TransformStamped)
}

// SnapshotStatics returns the full latched set oldest sorted for late
// joiners.
//
// The result is sorted by parent then child so replays are deterministic.
// The slice is a copy: mutating it never affects the store. An empty store
// yields an empty non nil slice, never nil.
func (s *TFStore) SnapshotStatics() []TransformStamped {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TransformStamped, 0, len(s.statics))
	for _, ts := range s.statics {
		out = append(out, ts)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].FrameID != out[j].FrameID {
			return out[i].FrameID < out[j].FrameID
		}
		return out[i].ChildFrameID < out[j].ChildFrameID
	})
	return out
}

// DynamicCount returns the total retained dynamic samples across keys.
func (s *TFStore) DynamicCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, history := range s.dynamics {
		total += len(history)
	}
	return total
}

// StaticCount returns the number of latched static frame pairs.
func (s *TFStore) StaticCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.statics)
}

// Lookup resolves the pose of source in the target frame.
//
// Zero stamp means latest: dynamics use the newest sample at or before now
// per edge, statics ignore time. Static-only paths ignore time and stamp
// the output with the requested time when requested is nonzero, else with
// the stored static stamp. Same frame returns identity only for valid
// nonempty frames: the frame must be known and storage must be nonempty,
// else it reports empty storage or no such frame like any other lookup.
// Timed lookup performs no interpolation: each dynamic edge on the path
// contributes its newest sample at or before the requested stamp. Requests
// outside the retained window report extrapolation, requests with no path
// report connectivity loss, unknown frames report no such frame, and empty
// storage reports empty storage. Exact strings are pinned by tests.
//
//nolint:gocognit,nestif,cyclop // lookup sequences validation, snapshot, path search, and taxonomy in one flow, splitting would obscure intent
func (s *TFStore) Lookup(target, source string, stampNanos int64) (TransformStamped, error) {
	nTarget := NormalizeFrameID(target)
	nSource := NormalizeFrameID(source)
	if nTarget == "" {
		return TransformStamped{}, fmt.Errorf("tf: no such frame %q", target)
	}
	if nSource == "" {
		return TransformStamped{}, fmt.Errorf("tf: no such frame %q", source)
	}
	if stampNanos < 0 {
		return TransformStamped{}, fmt.Errorf("tf: invalid stamp %d: stamp must be nonnegative or zero for latest", stampNanos)
	}
	s.mu.Lock()
	if len(s.dynamics) == 0 && len(s.statics) == 0 {
		s.mu.Unlock()
		return TransformStamped{}, errors.New("tf: empty storage") //nolint:wrapcheck // new error, nothing to wrap
	}
	dynamicsCopy := make(map[tfKey][]TransformStamped, len(s.dynamics))
	for k, v := range s.dynamics {
		cp := make([]TransformStamped, len(v))
		copy(cp, v)
		dynamicsCopy[k] = cp
	}
	staticsCopy := make(map[tfKey]TransformStamped, len(s.statics))
	maps.Copy(staticsCopy, s.statics)
	s.mu.Unlock()
	known := tfKnownFrames(dynamicsCopy, staticsCopy)
	if nTarget == nSource {
		if !known[nTarget] {
			return TransformStamped{}, fmt.Errorf("tf: no such frame %q", nTarget)
		}
		stamp := stampNanos
		if stamp == 0 {
			stamp = tfLatestForFrame(dynamicsCopy, staticsCopy, nTarget)
		}
		return TransformStamped{
			StampNanos:   stamp,
			FrameID:      nTarget,
			ChildFrameID: nSource,
			Transform:    IdentityTransform(),
		}, nil
	}
	if !known[nTarget] {
		return TransformStamped{}, fmt.Errorf("tf: no such frame %q", nTarget)
	}
	if !known[nSource] {
		return TransformStamped{}, fmt.Errorf("tf: no such frame %q", nSource)
	}
	path, ok := tfFindPath(dynamicsCopy, staticsCopy, nTarget, nSource, stampNanos)
	if ok {
		if stampNanos != 0 && !tfPathIsStaticOnly(staticsCopy, path) {
			latest := tfPathLatest(dynamicsCopy, staticsCopy, path)
			if stampNanos > latest {
				return TransformStamped{}, fmt.Errorf("tf: extrapolation into future: requested %d latest %d for %q to %q", stampNanos, latest, nTarget, nSource)
			}
		}
		composed, err := tfComposePath(dynamicsCopy, staticsCopy, path, stampNanos)
		if err != nil {
			return TransformStamped{}, err
		}
		composed.FrameID = nTarget
		composed.ChildFrameID = nSource
		switch {
		case tfPathIsStaticOnly(staticsCopy, path) && stampNanos != 0:
			composed.StampNanos = stampNanos
		case stampNanos == 0:
			composed.StampNanos = tfPathLatest(dynamicsCopy, staticsCopy, path)
		default:
			composed.StampNanos = stampNanos
		}
		return composed, nil
	}
	if stampNanos != 0 {
		if from, to, earliest, latest, isPast, isFuture := tfExtrapolationBounds(dynamicsCopy, staticsCopy, nTarget, nSource, stampNanos); isPast || isFuture {
			_ = from
			_ = to
			if isPast {
				return TransformStamped{}, fmt.Errorf("tf: extrapolation into past: requested %d earliest %d for %q to %q", stampNanos, earliest, nTarget, nSource)
			}
			return TransformStamped{}, fmt.Errorf("tf: extrapolation into future: requested %d latest %d for %q to %q", stampNanos, latest, nTarget, nSource)
		}
	}
	return TransformStamped{}, fmt.Errorf("tf: connectivity loss: no path from %q to %q", nTarget, nSource)
}

// LookupWithTimeout polls Lookup until success or the buffer clock deadline.
// Timeout accounting uses the buffer clock: deadline equals clock.Now plus
// timeoutNanos, and expiry is decided by clock.Now, never by wall. Zero
// timeout means one attempt with no polling. Positive timeout polls with a
// short wall pause between attempts but the stop decision stays on the
// buffer clock, so a frozen buffer clock with no new data stays pending
// until the buffer deadline passes. A nil clock is an error. The last
// lookup error is wrapped with timeout context on expiry so the taxonomy
// stays visible.
func (s *TFStore) LookupWithTimeout(target, source string, stampNanos, timeoutNanos int64, clock *ClockState) (TransformStamped, error) {
	if clock == nil {
		return TransformStamped{}, fmt.Errorf("core: lookup with timeout %q to %q: clock is nil", target, source)
	}
	if timeoutNanos < 0 {
		return TransformStamped{}, fmt.Errorf("core: lookup with timeout %q to %q: timeout %d must be nonnegative", target, source, timeoutNanos)
	}
	got, err := s.Lookup(target, source, stampNanos)
	if err == nil {
		return got, nil
	}
	if timeoutNanos == 0 {
		return TransformStamped{}, err
	}
	start := clock.Now()
	deadline := tfAddSaturating(start, timeoutNanos)
	lastErr := err
	for {
		if clock.Now() >= deadline {
			return TransformStamped{}, fmt.Errorf("core: lookup with timeout %q to %q timed out after %d ns: %w", target, source, timeoutNanos, lastErr)
		}
		clock.SleepWall(1000000)
		next, retryErr := s.Lookup(target, source, stampNanos)
		if retryErr == nil {
			return next, nil
		}
		lastErr = retryErr
	}
}

// HandleClockJump clears dynamics on backward jumps and on sim activation
// changes while preserving latched statics.
//
// Statics ignore time, so they survive jumps. Dynamics carry sim stamps that
// are invalid after a backward bag loop or after ROS time activation or
// deactivation, so they are dropped. A backward tick observed while sim is
// not active does not clear wall dynamics; only backward jumps with sim
// active or any activation change clear. Forward jumps without activation
// change preserve dynamics.
func (s *TFStore) HandleClockJump(jump ClockJump) {
	if jump.ActivationChanged {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.dynamics = make(map[tfKey][]TransformStamped)
		return
	}
	if !jump.Backward || !jump.SimActive {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dynamics = make(map[tfKey][]TransformStamped)
}

// validateDynamicTransform checks one dynamic sample.
func validateDynamicTransform(ts *TransformStamped) error {
	if ts == nil {
		return errors.New("tf: dynamic transform is nil") //nolint:wrapcheck // new error, nothing to wrap
	}
	if NormalizeFrameID(ts.FrameID) == "" {
		return fmt.Errorf("tf: invalid frame id %q: frame must be nonempty", ts.FrameID)
	}
	if NormalizeFrameID(ts.ChildFrameID) == "" {
		return fmt.Errorf("tf: invalid child frame id %q: child must be nonempty", ts.ChildFrameID)
	}
	if ts.StampNanos <= 0 {
		return fmt.Errorf("tf: dynamic stamp must be positive, got %d", ts.StampNanos)
	}
	err := validateQuaternion(ts.Transform.Rotation)
	if err != nil {
		return fmt.Errorf("tf: invalid quaternion: %w", err)
	}
	return nil
}

// validateStaticTransform checks one static sample.
func validateStaticTransform(ts *TransformStamped) error {
	if ts == nil {
		return errors.New("tf: static transform is nil") //nolint:wrapcheck // new error, nothing to wrap
	}
	if NormalizeFrameID(ts.FrameID) == "" {
		return fmt.Errorf("tf: invalid frame id %q: frame must be nonempty", ts.FrameID)
	}
	if NormalizeFrameID(ts.ChildFrameID) == "" {
		return fmt.Errorf("tf: invalid child frame id %q: child must be nonempty", ts.ChildFrameID)
	}
	if ts.StampNanos < 0 {
		return fmt.Errorf("tf: static stamp must be nonnegative, got %d", ts.StampNanos)
	}
	err := validateQuaternion(ts.Transform.Rotation)
	if err != nil {
		return fmt.Errorf("tf: invalid quaternion: %w", err)
	}
	return nil
}

// validateQuaternion rejects NaN, infinite, and zero norm rotations.
func validateQuaternion(q Quaternion) error {
	if math.IsNaN(q.X) || math.IsNaN(q.Y) || math.IsNaN(q.Z) || math.IsNaN(q.W) {
		return errors.New("tf: quaternion carries NaN") //nolint:wrapcheck // new error, nothing to wrap
	}
	if math.IsInf(q.X, 0) || math.IsInf(q.Y, 0) || math.IsInf(q.Z, 0) || math.IsInf(q.W, 0) {
		return errors.New("tf: quaternion carries Inf") //nolint:wrapcheck // new error, nothing to wrap
	}
	norm := q.X*q.X + q.Y*q.Y + q.Z*q.Z + q.W*q.W
	if norm < 1e-18 {
		return errors.New("tf: quaternion has zero norm") //nolint:wrapcheck // new error, nothing to wrap
	}
	return nil
}

// tfKnownFrames collects every frame appearing in either map.
func tfKnownFrames(dynamics map[tfKey][]TransformStamped, statics map[tfKey]TransformStamped) map[string]bool {
	known := make(map[string]bool)
	for k := range statics {
		known[k.parent] = true
		known[k.child] = true
	}
	for k := range dynamics {
		known[k.parent] = true
		known[k.child] = true
	}
	return known
}

// tfLatestForFrame returns the newest stamp touching one frame, or zero.
func tfLatestForFrame(dynamics map[tfKey][]TransformStamped, statics map[tfKey]TransformStamped, frame string) int64 {
	var latest int64
	for k, history := range dynamics {
		if k.parent != frame && k.child != frame {
			continue
		}
		for _, ts := range history {
			if ts.StampNanos > latest {
				latest = ts.StampNanos
			}
		}
	}
	for k, ts := range statics {
		if k.parent != frame && k.child != frame {
			continue
		}
		if ts.StampNanos > latest {
			latest = ts.StampNanos
		}
	}
	return latest
}

// tfEdge describes one BFS step with its key and forward flag.
type tfEdge struct {
	key     tfKey
	forward bool
}

// tfFindPath searches the frame graph for a path from target to source.
//
// For zero stamp it uses every edge. For nonzero stamp it admits static
// edges always and dynamic edges only when they hold a sample at or before
// the stamp, which pins no interpolation. It returns the edge list in walk
// order from target to source.
//
//nolint:gocognit,cyclop // path search sequences admission, BFS, and deterministic ordering in one traversal, splitting would obscure intent
func tfFindPath(dynamics map[tfKey][]TransformStamped, statics map[tfKey]TransformStamped, target, source string, stamp int64) ([]tfEdge, bool) {
	adj := make(map[string][]tfEdge)
	addEdge := func(parent, child string) {
		key := tfKey{parent: parent, child: child}
		adj[parent] = append(adj[parent], tfEdge{key: key, forward: true})
		adj[child] = append(adj[child], tfEdge{key: key, forward: false})
	}
	for k := range statics {
		addEdge(k.parent, k.child)
	}
	for k, history := range dynamics {
		if stamp == 0 {
			if len(history) == 0 {
				continue
			}
			addEdge(k.parent, k.child)
			continue
		}
		admitted := false
		for _, ts := range history {
			if ts.StampNanos <= stamp {
				admitted = true
				break
			}
		}
		if admitted {
			addEdge(k.parent, k.child)
		}
	}
	if target == source {
		return nil, true
	}
	prev := make(map[string]tfEdge)
	prevNode := make(map[string]string)
	visited := map[string]bool{target: true}
	queue := []string{target}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == source {
			var path []tfEdge
			node := source
			for node != target {
				edge := prev[node]
				path = append([]tfEdge{edge}, path...)
				node = prevNode[node]
			}
			return path, true
		}
		edges := adj[cur]
		sort.Slice(edges, func(i, j int) bool {
			if edges[i].key.parent != edges[j].key.parent {
				return edges[i].key.parent < edges[j].key.parent
			}
			if edges[i].key.child != edges[j].key.child {
				return edges[i].key.child < edges[j].key.child
			}
			return edges[i].forward && !edges[j].forward
		})
		for _, edge := range edges {
			var next string
			if edge.forward {
				if cur != edge.key.parent {
					continue
				}
				next = edge.key.child
			} else {
				if cur != edge.key.child {
					continue
				}
				next = edge.key.parent
			}
			if visited[next] {
				continue
			}
			visited[next] = true
			prev[next] = edge
			prevNode[next] = cur
			queue = append(queue, next)
		}
	}
	return nil, false
}

// tfComposePath multiplies the transforms along a found path.
func tfComposePath(dynamics map[tfKey][]TransformStamped, statics map[tfKey]TransformStamped, path []tfEdge, stamp int64) (TransformStamped, error) {
	result := IdentityTransform()
	for _, edge := range path {
		var sample TransformStamped
		var ok bool
		if stored, isStatic := statics[edge.key]; isStatic {
			sample = stored
			ok = true
		} else {
			history := dynamics[edge.key]
			if stamp == 0 {
				sample, ok = tfNewestSample(history)
			} else {
				sample, ok = tfNewestAtOrBefore(history, stamp)
			}
		}
		if !ok {
			return TransformStamped{}, fmt.Errorf("tf: connectivity loss: no path from %q to %q", "", "")
		}
		step := sample.Transform
		if !edge.forward {
			step = step.Inverse()
		}
		result = result.Multiply(step)
	}
	return TransformStamped{Transform: result}, nil
}

// tfNewestSample returns the newest sample in a history.
func tfNewestSample(history []TransformStamped) (TransformStamped, bool) {
	if len(history) == 0 {
		return TransformStamped{}, false
	}
	best := history[0]
	for _, ts := range history[1:] {
		if ts.StampNanos > best.StampNanos {
			best = ts
		}
	}
	return best, true
}

// tfNewestAtOrBefore returns the newest sample at or before the stamp.
func tfNewestAtOrBefore(history []TransformStamped, stamp int64) (TransformStamped, bool) {
	var best TransformStamped
	found := false
	for _, ts := range history {
		if ts.StampNanos > stamp {
			continue
		}
		if !found || ts.StampNanos > best.StampNanos {
			best = ts
			found = true
		}
	}
	return best, found
}

// tfPathIsStaticOnly reports whether every edge on the path is static.
func tfPathIsStaticOnly(statics map[tfKey]TransformStamped, path []tfEdge) bool {
	for _, edge := range path {
		if _, ok := statics[edge.key]; !ok {
			return false
		}
	}
	return true
}

// tfPathLatest returns the newest stamp along a path for zero time stamps.
func tfPathLatest(dynamics map[tfKey][]TransformStamped, statics map[tfKey]TransformStamped, path []tfEdge) int64 {
	var latest int64
	for _, edge := range path {
		if stored, ok := statics[edge.key]; ok {
			if stored.StampNanos > latest {
				latest = stored.StampNanos
			}
			continue
		}
		if best, ok := tfNewestSample(dynamics[edge.key]); ok {
			if best.StampNanos > latest {
				latest = best.StampNanos
			}
		}
	}
	return latest
}

// tfExtrapolationBounds decides past versus future for a timed miss.
//
// It assumes target and source are known but no time admitted path exists.
// It reports past when the request precedes the earliest retained sample on
// the latest connectivity path, and future when the request follows the
// latest retained sample. Static only paths never extrapolate.
//
//nolint:gocognit,cyclop // bounds search sequences connectivity fallback, static filtering, and earliest-latest reduction in one pass, splitting would obscure intent
func tfExtrapolationBounds(dynamics map[tfKey][]TransformStamped, statics map[tfKey]TransformStamped, target, source string, stamp int64) (string, string, int64, int64, bool, bool) {
	path, ok := tfFindPath(dynamics, statics, target, source, 0)
	if !ok {
		return "", "", 0, 0, false, false
	}
	if tfPathIsStaticOnly(statics, path) {
		return "", "", 0, 0, false, false
	}
	var earliest int64
	var latest int64
	first := true
	for _, edge := range path {
		if _, ok := statics[edge.key]; ok {
			continue
		}
		history := dynamics[edge.key]
		if len(history) == 0 {
			continue
		}
		edgeEarliest := history[0].StampNanos
		edgeLatest := history[0].StampNanos
		for _, ts := range history[1:] {
			if ts.StampNanos < edgeEarliest {
				edgeEarliest = ts.StampNanos
			}
			if ts.StampNanos > edgeLatest {
				edgeLatest = ts.StampNanos
			}
		}
		if first {
			earliest = edgeEarliest
			latest = edgeLatest
			first = false
			continue
		}
		if edgeEarliest > earliest {
			earliest = edgeEarliest
		}
		if edgeLatest < latest {
			latest = edgeLatest
		}
	}
	if first {
		return "", "", 0, 0, false, false
	}
	if stamp < earliest {
		return target, source, earliest, latest, true, false
	}
	if stamp > latest {
		return target, source, earliest, latest, false, true
	}
	return target, source, earliest, latest, false, false
}

// Inverse returns the reverse rigid transform.
func (t Transform) Inverse() Transform {
	inv := conjugateQuaternion(normalizeQuaternion(t.Rotation))
	neg := Vector3{X: -t.Translation.X, Y: -t.Translation.Y, Z: -t.Translation.Z}
	rotated := rotateVector(inv, neg)
	return Transform{Translation: rotated, Rotation: inv}
}

// Multiply composes a followed by b: applying b first, then a.
func (a Transform) Multiply(b Transform) Transform {
	qa := normalizeQuaternion(a.Rotation)
	qb := normalizeQuaternion(b.Rotation)
	q := quatMultiply(qa, qb)
	rotated := rotateVector(qa, b.Translation)
	return Transform{
		Translation: Vector3{X: a.Translation.X + rotated.X, Y: a.Translation.Y + rotated.Y, Z: a.Translation.Z + rotated.Z},
		Rotation:    q,
	}
}

// normalizeQuaternion scales to unit norm, mapping zero norm to identity.
func normalizeQuaternion(q Quaternion) Quaternion {
	norm := math.Sqrt(q.X*q.X + q.Y*q.Y + q.Z*q.Z + q.W*q.W)
	if norm < 1e-18 || math.IsNaN(norm) || math.IsInf(norm, 0) {
		return Quaternion{X: 0, Y: 0, Z: 0, W: 1}
	}
	return Quaternion{X: q.X / norm, Y: q.Y / norm, Z: q.Z / norm, W: q.W / norm}
}

// conjugateQuaternion returns the conjugate of a unit quaternion.
func conjugateQuaternion(q Quaternion) Quaternion {
	return Quaternion{X: -q.X, Y: -q.Y, Z: -q.Z, W: q.W}
}

// quatMultiply returns a times b for unit quaternions.
func quatMultiply(a, b Quaternion) Quaternion {
	return Quaternion{
		X: a.W*b.X + a.X*b.W + a.Y*b.Z - a.Z*b.Y,
		Y: a.W*b.Y - a.X*b.Z + a.Y*b.W + a.Z*b.X,
		Z: a.W*b.Z + a.X*b.Y - a.Y*b.X + a.Z*b.W,
		W: a.W*b.W - a.X*b.X - a.Y*b.Y - a.Z*b.Z,
	}
}

// rotateVector applies a unit quaternion to a vector.
func rotateVector(q Quaternion, v Vector3) Vector3 {
	qv := Quaternion{X: v.X, Y: v.Y, Z: v.Z, W: 0}
	tmp := quatMultiply(q, qv)
	back := quatMultiply(tmp, conjugateQuaternion(q))
	return Vector3{X: back.X, Y: back.Y, Z: back.Z}
}
