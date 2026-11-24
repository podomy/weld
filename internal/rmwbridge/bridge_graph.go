// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package rmwbridge

import (
	"errors"
	"fmt"
	"sort"
)

// NodeInfo is one discovered node in the graph snapshot.
type NodeInfo struct {
	// Name is the node name.
	Name string
	// Namespace is the node namespace.
	Namespace string
	// DomainID is the domain the node was created in.
	DomainID uint32
}

// TopicInfo is one topic with its distinct type names.
type TopicInfo struct {
	// Name is the fully qualified ROS topic name.
	Name string
	// Types holds distinct non-empty type names in sorted order.
	Types []string
}

// ServiceInfo is one service with its distinct type names.
type ServiceInfo struct {
	// Name is the fully qualified ROS service name.
	Name string
	// Types holds distinct non-empty type names in sorted order.
	Types []string
}

// DomainID returns the domain joined by JoinDiscovery.
func (b *Bridge) DomainID() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.domainID
}

// CreateNodeWithNamespace registers a node with an explicit namespace.
//
// It uses the current bridge domain from the last JoinDiscovery.
func (b *Bridge) CreateNodeWithNamespace(name, namespace string) (*Node, error) {
	if name == "" {
		return nil, errors.New("rmwbridge: create node: empty name") //nolint:wrapcheck // new error, nothing to wrap
	}
	if namespace == "" {
		namespace = "/"
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, fmt.Errorf("rmwbridge: create node: %w", ErrBridgeClosed)
	}
	key := nodeKey{domain: b.domainID, name: name}
	_, exists := b.nodes[key]
	if exists {
		return nil, fmt.Errorf("rmwbridge: create node %q: %w", name, ErrNodeExists)
	}
	node := &Node{Name: name, Namespace: namespace, DomainID: b.domainID, bridge: b}
	b.nodes[key] = node
	return node, nil
}

// CreatePublisherWithType creates a topic writer with a known type name.
func (b *Bridge) CreatePublisherWithType(node *Node, topic, typeName string, qos QoS) (*Publisher, error) {
	pub, err := b.CreatePublisher(node, topic, qos)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	pub.TypeName = typeName
	return pub, nil
}

// CreateSubscriptionWithType creates a topic reader with a known type name.
func (b *Bridge) CreateSubscriptionWithType(node *Node, topic, typeName string, qos QoS) (*Subscription, error) {
	sub, err := b.CreateSubscription(node, topic, qos)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	sub.TypeName = typeName
	return sub, nil
}

// CreateServiceWithType registers a service with a known type name.
func (b *Bridge) CreateServiceWithType(name, typeName string, qos ...QoS) (*Service, error) {
	svc, err := b.CreateService(name, qos...)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	svc.TypeName = typeName
	return svc, nil
}

// CreateClientWithType registers a client with a known type name.
func (b *Bridge) CreateClientWithType(name, typeName string, qos ...QoS) (*Client, error) {
	client, err := b.CreateClient(name, qos...)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	client.TypeName = typeName
	return client, nil
}

// NodeNames returns a sorted snapshot of registered nodes across domains.
func (b *Bridge) NodeNames() []NodeInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]NodeInfo, 0, len(b.nodes))
	for _, n := range b.nodes {
		ns := n.Namespace
		if ns == "" {
			ns = "/"
		}
		out = append(out, NodeInfo{Name: n.Name, Namespace: ns, DomainID: n.DomainID})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].DomainID != out[j].DomainID {
			return out[i].DomainID < out[j].DomainID
		}
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// ensureTopicSet returns the type set for a topic, creating it when missing.
func ensureTopicSet(byTopic map[string]map[string]struct{}, topic string) map[string]struct{} {
	set, ok := byTopic[topic]
	if !ok {
		set = make(map[string]struct{})
		byTopic[topic] = set
	}
	return set
}

// collectPubTopicTypes merges publisher type names into byTopic.
func collectPubTopicTypes(byTopic map[string]map[string]struct{}, pubs map[topicKey]map[*Publisher]struct{}) {
	for key, set := range pubs {
		entry := ensureTopicSet(byTopic, key.topic)
		for pub := range set {
			if pub.TypeName != "" {
				entry[pub.TypeName] = struct{}{}
			}
		}
	}
}

// collectSubTopicTypes merges subscription type names into byTopic.
func collectSubTopicTypes(byTopic map[string]map[string]struct{}, subs map[topicKey]map[*Subscription]struct{}) {
	for key, set := range subs {
		entry := ensureTopicSet(byTopic, key.topic)
		for sub := range set {
			if sub.TypeName != "" {
				entry[sub.TypeName] = struct{}{}
			}
		}
	}
}

// buildTopicInfos returns sorted topic infos from merged type sets.
func buildTopicInfos(byTopic map[string]map[string]struct{}) []TopicInfo {
	out := make([]TopicInfo, 0, len(byTopic))
	for name, types := range byTopic {
		list := make([]string, 0, len(types))
		for t := range types {
			list = append(list, t)
		}
		sort.Strings(list)
		out = append(out, TopicInfo{Name: name, Types: list})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

// TopicNamesAndTypes returns a sorted snapshot of topics with types.
//
// Entries merge across domains by topic name; counts stay per topic name
// across domains so single-domain tests observe exact numbers.
func (b *Bridge) TopicNamesAndTypes() []TopicInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	byTopic := make(map[string]map[string]struct{})
	collectPubTopicTypes(byTopic, b.pubs)
	collectSubTopicTypes(byTopic, b.subs)
	return buildTopicInfos(byTopic)
}

// ServiceNamesAndTypes returns a sorted snapshot of services with types.
func (b *Bridge) ServiceNamesAndTypes() []ServiceInfo {
	b.mu.Lock()
	defer b.mu.Unlock()
	byService := make(map[string]map[string]struct{})
	for name, svc := range b.services {
		if _, ok := byService[name]; !ok {
			byService[name] = make(map[string]struct{})
		}
		if svc.TypeName != "" {
			byService[name][svc.TypeName] = struct{}{}
		}
	}
	for name, client := range b.clients {
		if _, ok := byService[name]; !ok {
			byService[name] = make(map[string]struct{})
		}
		if client.TypeName != "" {
			byService[name][client.TypeName] = struct{}{}
		}
	}
	out := make([]ServiceInfo, 0, len(byService))
	for name, types := range byService {
		list := make([]string, 0, len(types))
		for t := range types {
			list = append(list, t)
		}
		sort.Strings(list)
		out = append(out, ServiceInfo{Name: name, Types: list})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

// CountPublishers returns the number of publishers on a topic across domains.
func (b *Bridge) CountPublishers(topic string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for key, set := range b.pubs {
		if key.topic == topic {
			n += len(set)
		}
	}
	return n
}

// CountSubscribers returns the number of subscriptions on a topic across domains.
func (b *Bridge) CountSubscribers(topic string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := 0
	for key, set := range b.subs {
		if key.topic == topic {
			n += len(set)
		}
	}
	return n
}

// CountServices returns the number of service servers for a service.
func (b *Bridge) CountServices(service string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.services[service]
	if ok {
		return 1
	}
	return 0
}

// CountClients returns the number of service clients for a service.
func (b *Bridge) CountClients(service string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.clients[service]
	if ok {
		return 1
	}
	return 0
}
