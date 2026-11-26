// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/podomy/weld/internal/rmwbridge"
)

// serveRoundtripReply serves one request for the roundtrip test.
func serveRoundtripReply(svc *rmwbridge.Service, serverErr chan error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, respond, err := svc.Serve(ctx)
	if err != nil {
		serverErr <- err
		return
	}
	if string(req) != "request" {
		serverErr <- errors.New("bad request payload")
		return
	}
	err = respond([]byte("reply:" + string(req)))
	if err != nil {
		serverErr <- fmt.Errorf("respond: %w", err)
		return
	}
	serverErr <- nil
}

// checkServiceRoundtripCall pins client call and destroy paths.
func checkServiceRoundtripCall(t *testing.T, bridge *rmwbridge.Bridge, client *rmwbridge.Client, serverErr chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := client.Call(ctx, []byte("request"))
	requireNoError(t, err, "client call")
	if string(resp) != "reply:request" {
		t.Fatalf("reply = %q, want reply:request", string(resp))
	}
	err = <-serverErr
	requireNoError(t, err, "serve")
	err = bridge.DestroyClient("add_two_ints")
	requireNoError(t, err, "destroy client")
	err = bridge.DestroyService("add_two_ints")
	requireNoError(t, err, "destroy service")
}

// TestServiceClientRoundtrip covers client Call against server Serve.
func TestServiceClientRoundtrip(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	svc, err := bridge.CreateService("add_two_ints")
	requireNoError(t, err, "create service")
	client, err := bridge.CreateClient("add_two_ints")
	requireNoError(t, err, "create client")
	if !client.Exists() {
		t.Fatalf("client exists = false, want true")
	}
	serverErr := make(chan error, 1)
	go serveRoundtripReply(svc, serverErr)
	checkServiceRoundtripCall(t, bridge, client, serverErr)
}

// checkSvcAvailabilityBefore pins client exists false before server.
func checkSvcAvailabilityBefore(t *testing.T, client *rmwbridge.Client) {
	t.Helper()
	if client.Exists() {
		t.Fatalf("client exists before server, want false")
	}
}

// checkSvcAvailabilityAfter pins exists after create.
func checkSvcAvailabilityAfter(t *testing.T, client *rmwbridge.Client, svc *rmwbridge.Service) {
	t.Helper()
	if !svc.Exists() {
		t.Fatalf("service exists = false, want true")
	}
	if !client.Exists() {
		t.Fatalf("client exists after server create = false, want true")
	}
}

// checkSvcAvailabilityDestroy pins destroy and second destroy.
func checkSvcAvailabilityDestroy(t *testing.T, bridge *rmwbridge.Bridge, svc *rmwbridge.Service, client *rmwbridge.Client) {
	t.Helper()
	err := bridge.DestroyService("late_service")
	requireNoError(t, err, "destroy service")
	if svc.Exists() {
		t.Fatalf("service exists after destroy, want false")
	}
	if client.Exists() {
		t.Fatalf("client exists after server destroy, want false")
	}
	err = bridge.DestroyClient("late_service")
	requireNoError(t, err, "destroy client")
	err = bridge.DestroyClient("late_service")
	requireError(t, err, "second destroy client")
}

// TestServiceAvailability covers Exists before create, after create, and
// after destroy on both server and client handles.
func TestServiceAvailability(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	client, err := bridge.CreateClient("late_service")
	requireNoError(t, err, "create client")
	checkSvcAvailabilityBefore(t, client)
	svc, err := bridge.CreateService("late_service")
	requireNoError(t, err, "create service")
	checkSvcAvailabilityAfter(t, client, svc)
	extra, err := bridge.CreateService("late_service")
	requireNoError(t, err, "second service shares queue")
	if !extra.Exists() {
		t.Fatalf("extra service exists = false, want true")
	}
	extraClient, err := bridge.CreateClient("late_service")
	requireNoError(t, err, "second client shares service")
	if !extraClient.Exists() {
		t.Fatalf("extra client exists = false, want true")
	}
	checkSvcAvailabilityDestroy(t, bridge, svc, client)
}

// TestServiceDeadlineCancel covers context timeout passthrough on Call
// with no responder and on an already canceled context.
func TestServiceDeadlineCancel(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	svc, err := bridge.CreateService("slow_service")
	requireNoError(t, err, "create service")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = svc.Call(ctx, []byte("request"))
	requireTimeout(t, err, "call without responder")
	canceled, stop := context.WithCancel(context.Background())
	stop()
	_, err = svc.Call(canceled, []byte("request"))
	requireErrorIs(t, err, context.Canceled, "canceled call")
}

// requireQoSDepth fails when a QoS depth differs from want.
func requireQoSDepth(t *testing.T, got string, want string, op string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s depth = %q, want %s", op, got, want)
	}
}

// checkDualQoSDefaults pins default request and reply depths.
func checkDualQoSDefaults(t *testing.T, svc *rmwbridge.Service) {
	t.Helper()
	requireQoSDepth(t, svc.RequestActualQoS().Depth, "10", "default request")
	requireQoSDepth(t, svc.ReplyActualQoS().Depth, "10", "default reply")
}

// checkDualQoSSplit pins split request and reply depths.
func checkDualQoSSplit(t *testing.T, svc2 *rmwbridge.Service) {
	t.Helper()
	requireQoSDepth(t, svc2.RequestActualQoS().Depth, "5", "request")
	requireQoSDepth(t, svc2.ReplyActualQoS().Depth, "7", "reply")
	requireQoSDepth(t, svc2.RequestSubscriptionActualQoS().Depth, "5", "request subscription")
	requireQoSDepth(t, svc2.ResponsePublisherActualQoS().Depth, "7", "response publisher")
}

// checkDualQoSClient pins client request and reply depths.
func checkDualQoSClient(t *testing.T, client *rmwbridge.Client) {
	t.Helper()
	requireQoSDepth(t, client.RequestActualQoS().Depth, "3", "client request")
	requireQoSDepth(t, client.ReplyActualQoS().Depth, "3", "client reply")
	requireQoSDepth(t, client.RequestPublisherActualQoS().Depth, "3", "client request publisher")
	requireQoSDepth(t, client.ResponseSubscriptionActualQoS().Depth, "3", "client response subscription")
}

// sharedQueueProbe tracks one multiserver service test run.
//
// Servers record every delivery here so the test pins exactly-once
// service across concurrent servers and clients.
type sharedQueueProbe struct {
	mu          sync.Mutex
	served      map[string]int
	perServer   map[*rmwbridge.Service]int
	done        int
	total       int
	clientsOpen bool
	serverErr   chan error
	clientErr   chan error
	serversDone sync.WaitGroup
}

// newSharedQueueProbe returns a probe expecting total requests.
func newSharedQueueProbe(total int) *sharedQueueProbe {
	return &sharedQueueProbe{
		served:      make(map[string]int),
		perServer:   make(map[*rmwbridge.Service]int),
		total:       total,
		clientsOpen: true,
		serverErr:   make(chan error, 2),
		clientErr:   make(chan error, 64),
	}
}

// probeFinished reports whether every request was served.
func (p *sharedQueueProbe) probeFinished() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done >= p.total
}

// probeNoteServed records one delivery by a server.
func (p *sharedQueueProbe) probeNoteServed(svc *rmwbridge.Service, req []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.served[string(req)]++
	p.perServer[svc]++
	p.done++
}

// probeKeepPolling reports whether a timed-out server retries.
//
// An idle timeout while clients are still sending is normal; only a
// quiet queue after the last client finished means a request was lost.
func (p *sharedQueueProbe) probeKeepPolling() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done < p.total && p.clientsOpen
}

// serveLoop serves until every request is done.
func (p *sharedQueueProbe) serveLoop(svc *rmwbridge.Service) {
	defer p.serversDone.Done()
	for {
		if p.probeFinished() {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		req, respond, err := svc.Serve(ctx)
		cancel()
		if err != nil {
			if p.probeFinished() {
				return
			}
			if p.probeKeepPolling() {
				continue
			}
			p.serverErr <- fmt.Errorf("serve: %w", err)
			return
		}
		p.probeNoteServed(svc, req)
		err = respond([]byte("resp:" + string(req)))
		if err != nil {
			p.serverErr <- fmt.Errorf("respond: %w", err)
			return
		}
	}
}

// startServers runs one serve loop per server.
func (p *sharedQueueProbe) startServers(servers ...*rmwbridge.Service) {
	p.serversDone.Add(len(servers))
	for _, svc := range servers {
		go p.serveLoop(svc)
	}
}

// runOneClient sends perClient sequential calls on one client.
func (p *sharedQueueProbe) runOneClient(ci int, client *rmwbridge.Client, perClient int, done chan struct{}) {
	defer close(done)
	for i := range perClient {
		payload := fmt.Sprintf("req-%d-%d", ci, i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := client.Call(ctx, []byte(payload))
		cancel()
		if err != nil {
			p.clientErr <- fmt.Errorf("call %s: %w", payload, err)
			return
		}
		if string(resp) != "resp:"+payload {
			p.clientErr <- fmt.Errorf("reply %s = %q, want resp:%s", payload, string(resp), payload)
			return
		}
	}
}

// runClients sends perClient calls on every client concurrently.
func (p *sharedQueueProbe) runClients(t *testing.T, clients []*rmwbridge.Client, perClient int) {
	t.Helper()
	dones := make([]chan struct{}, 0, len(clients))
	for ci, client := range clients {
		done := make(chan struct{})
		dones = append(dones, done)
		go p.runOneClient(ci, client, perClient, done)
	}
	for _, done := range dones {
		<-done
	}
	p.mu.Lock()
	p.clientsOpen = false
	p.mu.Unlock()
	p.serversDone.Wait()
}

// checkServed drains errors and pins exactly-once service plus sharing.
func (p *sharedQueueProbe) checkServed(t *testing.T, svc1, svc2 *rmwbridge.Service) {
	t.Helper()
	close(p.serverErr)
	close(p.clientErr)
	for err := range p.serverErr {
		t.Fatalf("server: %v", err)
	}
	for err := range p.clientErr {
		t.Fatalf("client: %v", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done != p.total {
		t.Fatalf("served = %d, want %d: requests lost", p.done, p.total)
	}
	if len(p.served) != p.total {
		t.Fatalf("distinct payloads = %d, want %d: loss or duplication", len(p.served), p.total)
	}
	for payload, n := range p.served {
		if n != 1 {
			t.Fatalf("payload %q served %d times, want exactly once", payload, n)
		}
	}
	if p.perServer[svc1] == 0 || p.perServer[svc2] == 0 {
		t.Fatalf("server split = %d/%d, want both positive: load not shared", p.perServer[svc1], p.perServer[svc2])
	}
}

// TestServiceSharedQueueMultiServer pins N servers on one service name.
//
// Two server handles share one FIFO request queue work-queue style:
// every request from N concurrent clients is served exactly once with
// no loss and no double serve, and both servers share the load.
// First-available-server-wins with FIFO fairness holds by channel
// construction: Serve pops one queued request atomically, so concurrent
// servers divide the queue instead of stealing repeats from each other.
func TestServiceSharedQueueMultiServer(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	svc1, err := bridge.CreateService("shared_work")
	requireNoError(t, err, "create first server")
	svc2, err := bridge.CreateService("shared_work")
	requireNoError(t, err, "create second server shares queue")
	if svc1 == svc2 {
		t.Fatalf("second server aliases first, want distinct handles")
	}
	if !svc1.Exists() || !svc2.Exists() {
		t.Fatalf("server exists = %v/%v, want true/true", svc1.Exists(), svc2.Exists())
	}
	if got := svc1.RequestActualQoS().Depth; got != "10" {
		t.Fatalf("extra server request depth = %q, want inherited 10", got)
	}

	const nClients = 4
	const perClient = 16
	const total = nClients * perClient
	clients := make([]*rmwbridge.Client, 0, nClients)
	for range nClients {
		client, err := bridge.CreateClient("shared_work")
		requireNoError(t, err, "create client shares service")
		clients = append(clients, client)
	}

	probe := newSharedQueueProbe(total)
	probe.startServers(svc1, svc2)
	probe.runClients(t, clients, perClient)
	probe.checkServed(t, svc1, svc2)

	// Closing one handle leaves the queue live for the other server.
	requireNoError(t, svc1.Close(), "close first server")
	if svc1.Exists() {
		t.Fatalf("closed server exists = true, want false")
	}
	if !svc2.Exists() {
		t.Fatalf("remaining server exists = false, want true")
	}
	// DestroyService closes every handle sharing the name at once.
	requireNoError(t, bridge.DestroyService("shared_work"), "destroy shared service")
	if svc2.Exists() {
		t.Fatalf("server exists after destroy = true, want false")
	}
	requireErrorIs(t, bridge.DestroyService("shared_work"), rmwbridge.ErrServiceNotFound, "second destroy service")
}

// TestServiceDualQoS covers fanout of one input and storage of two inputs.
func TestServiceDualQoS(t *testing.T) {
	t.Parallel()
	bridge := rmwbridge.NewBridge()
	defer func() {
		closeBridgeChecked(t, bridge)
	}()
	svc, err := bridge.CreateService("qos_default")
	requireNoError(t, err, "create service")
	checkDualQoSDefaults(t, svc)
	svc2, err := bridge.CreateService("qos_split", rmwbridge.QoS{Depth: "5"}, rmwbridge.QoS{Depth: "7"})
	requireNoError(t, err, "create service with qos")
	checkDualQoSSplit(t, svc2)
	client, err := bridge.CreateClient("qos_client", rmwbridge.QoS{Depth: "3"})
	requireNoError(t, err, "create client")
	checkDualQoSClient(t, client)
}
