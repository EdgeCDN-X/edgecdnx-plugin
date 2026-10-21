package edgecdnxplugin

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"strings"
	"sync"
	"sync/atomic"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/miekg/dns"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

// roundRobinResetThreshold caps the counter well below the uint32 range so Add never wraps around.
const roundRobinResetThreshold = math.MaxUint32 - (1 << 16)

var errConflictingDNS = errors.New("conflicting DNS resources")

type DNSEndpointManagerConfiguration struct {
	Namespace string
}

type DNSEndpointManager struct {
	fac       dynamicinformer.DynamicSharedInformerFactory
	Informer  cache.SharedIndexInformer
	Sync      *sync.RWMutex
	endpoints atomic.Pointer[dnsEndpointSnapshot]
	sources   map[string]*infrastructurev1alpha1.DNSEndpoint
}

// Published snapshots and their endpoint configuration must never be mutated.
type dnsEndpointSnapshot struct {
	endpoints map[endpointKey]*dnsEndpointEntry
	conflicts map[endpointKey]bool
}

type endpointKey struct {
	name  string
	qtype uint16
}

type dnsEndpointEntry struct {
	*infrastructurev1alpha1.DNSEndpoint
	counter  *atomic.Uint32
	weighted *weightedRoundRobinState
	targets  atomic.Pointer[endpointLocationTargets]
	geo      atomic.Pointer[endpointGeoCandidates]
	targetMu sync.Mutex
}

type endpointLocationTargets struct {
	manager  *LocationManager
	revision uint64
	targets  []weightedTarget
}

// matchingLocationTargets returns an immutable slice tied to this entry's selector.
func (entry *dnsEndpointEntry) matchingLocationTargets(manager *LocationManager) []weightedTarget {
	revision := manager.revision.Load()
	if cached := entry.targets.Load(); cached != nil && cached.manager == manager && cached.revision == revision {
		return cached.targets
	}

	entry.targetMu.Lock()
	defer entry.targetMu.Unlock()
	manager.Sync.RLock()
	defer manager.Sync.RUnlock()

	revision = manager.revision.Load()
	if cached := entry.targets.Load(); cached != nil && cached.manager == manager && cached.revision == revision {
		return cached.targets
	}
	targets := manager.findMatchingTargetsWithWeightsLocked(entry.Spec.RouteSelector, scopeFor(entry.Namespace, entry.Spec.Project))
	entry.targets.Store(&endpointLocationTargets{manager: manager, revision: revision, targets: targets})
	return targets
}

type weightedTarget struct {
	Name   string
	Weight int32
}

type weightedRoundRobinState struct {
	sync.Mutex
	targets     []weightedTarget
	current     []int64
	totalWeight int64
}

func dnsEndpointKey(dnsName string, recordType string) endpointKey {
	return endpointKey{
		name:  dns.Fqdn(strings.ToLower(dnsName)),
		qtype: dns.StringToType[strings.ToUpper(recordType)],
	}
}

func isRoundRobinPolicy(routingPolicy string) bool {
	return strings.EqualFold(routingPolicy, "roundrobin")
}

// GetNext increments the round-robin counter for the DNSEndpoint identified by qname/qtype and
// returns it modulo mod. If mod is 0, 0 is returned. If no counter exists (e.g. the DNSEndpoint
// is not RoundRobin routed), 0 is returned.
func (dm *DNSEndpointManager) GetNext(qname string, qtype uint16, mod int) int {
	entry, err := dm.getDNSEndpoint(qname, qtype)
	if err != nil {
		return 0
	}
	return entry.getNext(mod)
}

func (entry *dnsEndpointEntry) getNext(mod int) int {
	if mod == 0 {
		return 0
	}

	counter := entry.counter
	if counter == nil {
		return 0
	}

	next := counter.Add(1)
	if next >= roundRobinResetThreshold {
		counter.Store(0)
	}

	return int(next % uint32(mod))
}

// GetWeightedNext selects a target using smooth weighted round robin. The current
// weights are used as the scheduling quantum, so the target list is never expanded.
func (dm *DNSEndpointManager) GetWeightedNext(qname string, qtype uint16, targets []weightedTarget) int {
	entry, err := dm.getDNSEndpoint(qname, qtype)
	if err != nil {
		log.Errorf("edgecdnx: cannot select weighted target: %v", err)
		return 0
	}
	return entry.getWeightedNext(targets)
}

func (entry *dnsEndpointEntry) getWeightedNext(targets []weightedTarget) int {
	if len(targets) == 0 {
		return 0
	}
	state := entry.weighted
	state.Lock()
	defer state.Unlock()

	if !sameWeightedTargets(state.targets, targets) {
		state.targets = append(state.targets[:0], targets...)
		state.current = make([]int64, len(targets))
		state.totalWeight = 0
		for _, target := range targets {
			if target.Weight > 0 {
				state.totalWeight += int64(target.Weight)
			}
		}
	}

	allWeightsZero := state.totalWeight == 0
	if allWeightsZero {
		state.totalWeight = int64(len(state.targets))
	}

	selected := 0
	for i, target := range state.targets {
		weight := int64(target.Weight)
		if allWeightsZero {
			weight = 1
		}
		if weight <= 0 {
			continue
		}

		state.current[i] += weight
		if state.current[i] > state.current[selected] {
			selected = i
		}
	}

	state.current[selected] -= state.totalWeight
	return selected
}

func sameWeightedTargets(left, right []weightedTarget) bool {
	if len(left) != len(right) {
		return false
	}

	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
}

// GetDNSEndpoint returns read-only configuration from the current snapshot.
// Callers must not mutate its nested maps, slices, or pointers.
func (dm *DNSEndpointManager) GetDNSEndpoint(qname string, qtype uint16) (infrastructurev1alpha1.DNSEndpoint, error) {
	entry, err := dm.getDNSEndpoint(qname, qtype)
	if err != nil {
		return infrastructurev1alpha1.DNSEndpoint{}, err
	}
	return *entry.DNSEndpoint, nil
}

func (dm *DNSEndpointManager) getDNSEndpoint(qname string, qtype uint16) (*dnsEndpointEntry, error) {
	if log.D.Value() {
		log.Debugf("edgecdnx: Looking up DNSEndpoint for %s %s", qname, dns.TypeToString[qtype])
	}
	normalizedName := dns.Fqdn(strings.ToLower(qname))
	key := endpointKey{name: normalizedName, qtype: qtype}
	if snapshot := dm.endpoints.Load(); snapshot != nil {
		if snapshot.conflicts[key] {
			return nil, fmt.Errorf("%w for %s %s", errConflictingDNS, qname, dns.TypeToString[qtype])
		}
		if dnsEndpoint, ok := snapshot.endpoints[key]; ok {
			return dnsEndpoint, nil
		}

		if dot := strings.IndexByte(normalizedName, '.'); dot > 0 && dot < len(normalizedName)-1 {
			wildcardKey := endpointKey{name: "*." + normalizedName[dot+1:], qtype: qtype}
			if snapshot.conflicts[wildcardKey] {
				return nil, fmt.Errorf("%w for wildcard %s", errConflictingDNS, wildcardKey.name)
			}
			if dnsEndpoint, ok := snapshot.endpoints[wildcardKey]; ok {
				return dnsEndpoint, nil
			}
		}
	}

	return nil, fmt.Errorf("DNSEndpoint not found for %s %s", qname, dns.TypeToString[qtype])
}

// publishEndpoint requires Sync's write lock to serialize copy-on-write updates.
// Readers may retain the old snapshot while a replacement is published.
func (dm *DNSEndpointManager) publishEndpoint(oldKey endpointKey, endpoint *infrastructurev1alpha1.DNSEndpoint, conflict ...bool) {
	endpoints := make(map[endpointKey]*dnsEndpointEntry)
	conflicts := make(map[endpointKey]bool)
	if snapshot := dm.endpoints.Load(); snapshot != nil {
		endpoints = maps.Clone(snapshot.endpoints)
		if snapshot.conflicts != nil {
			conflicts = maps.Clone(snapshot.conflicts)
		}
	}
	previous := endpoints[oldKey]
	delete(endpoints, oldKey)
	delete(conflicts, oldKey)
	if len(conflict) != 0 && conflict[0] {
		conflicts[oldKey] = true
	}
	if endpoint != nil {
		key := dnsEndpointKey(endpoint.Spec.DNSName, endpoint.Spec.RecordType)
		entry := &dnsEndpointEntry{
			DNSEndpoint: endpoint.DeepCopy(),
			weighted:    &weightedRoundRobinState{},
		}
		if isRoundRobinPolicy(endpoint.Spec.RoutingPolicy) {
			entry.counter = &atomic.Uint32{}
		}
		if previous != nil && oldKey == key {
			entry.weighted = previous.weighted
			if entry.counter != nil && previous.counter != nil {
				entry.counter = previous.counter
			}
		}
		endpoints[key] = entry
	}
	dm.endpoints.Store(&dnsEndpointSnapshot{endpoints: endpoints, conflicts: conflicts})
}

func (dm *DNSEndpointManager) updateEndpointSources(oldEndpoint, newEndpoint *infrastructurev1alpha1.DNSEndpoint) {
	if dm.sources == nil {
		dm.sources = make(map[string]*infrastructurev1alpha1.DNSEndpoint)
		if snapshot := dm.endpoints.Load(); snapshot != nil {
			for _, entry := range snapshot.endpoints {
				dm.sources[resourceKey(entry.Namespace, entry.Name)] = entry.DNSEndpoint
			}
		}
	}
	keys := make(map[endpointKey]struct{})
	if oldEndpoint != nil {
		delete(dm.sources, resourceKey(oldEndpoint.Namespace, oldEndpoint.Name))
		keys[dnsEndpointKey(oldEndpoint.Spec.DNSName, oldEndpoint.Spec.RecordType)] = struct{}{}
	}
	if newEndpoint != nil {
		dm.sources[resourceKey(newEndpoint.Namespace, newEndpoint.Name)] = newEndpoint.DeepCopy()
		keys[dnsEndpointKey(newEndpoint.Spec.DNSName, newEndpoint.Spec.RecordType)] = struct{}{}
	}
	for key := range keys {
		var candidate *infrastructurev1alpha1.DNSEndpoint
		count := 0
		for _, source := range dm.sources {
			if dnsEndpointKey(source.Spec.DNSName, source.Spec.RecordType) == key {
				candidate = source
				count++
			}
		}
		if count > 1 {
			log.Errorf("edgecdnx: %d DNSEndpoints claim DNS identity %s %s; refusing ambiguous answers", count, key.name, dns.TypeToString[key.qtype])
			dm.publishEndpoint(key, nil, true)
		} else {
			dm.publishEndpoint(key, candidate)
		}
	}
}

func NewDNSEndpointManager(factory dynamicinformer.DynamicSharedInformerFactory, config DNSEndpointManagerConfiguration) *DNSEndpointManager {
	dm := &DNSEndpointManager{
		fac:  factory,
		Sync: &sync.RWMutex{},
	}

	dnsEndpointInformer := factory.ForResource(schema.GroupVersionResource{
		Group:    infrastructurev1alpha1.SchemeGroupVersion.Group,
		Version:  infrastructurev1alpha1.SchemeGroupVersion.Version,
		Resource: "dnsendpoints",
	}).Informer()

	log.Infof("edgecdnx: Watching DNSEndpoints in namespace %s", config.Namespace)

	dnsEndpointInformer.AddEventHandler(dm.endpointEventHandlers())
	dm.Informer = dnsEndpointInformer

	return dm
}

func (dm *DNSEndpointManager) endpointEventHandlers() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			dnsEndpoint, err := dnsEndpointFromObject(obj)
			if err != nil {
				log.Errorf("edgecdnx: failed to add DNSEndpoint: %v", err)
				return
			}

			dm.Sync.Lock()
			defer dm.Sync.Unlock()
			dm.updateEndpointSources(nil, dnsEndpoint)
			log.Infof("edgecdnx: Added DNSEndpoint %s", dnsEndpoint.Name)
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldDNSEndpoint, err := dnsEndpointFromObject(oldObj)
			if err != nil {
				log.Errorf("edgecdnx: failed to read old DNSEndpoint: %v", err)
				return
			}
			newDNSEndpoint, err := dnsEndpointFromObject(newObj)
			if err != nil {
				log.Errorf("edgecdnx: failed to update DNSEndpoint: %v", err)
				return
			}

			dm.Sync.Lock()
			defer dm.Sync.Unlock()
			dm.updateEndpointSources(oldDNSEndpoint, newDNSEndpoint)
			log.Infof("edgecdnx: Updated DNSEndpoint %s", newDNSEndpoint.Name)
		},
		DeleteFunc: func(obj any) {
			dnsEndpoint, err := dnsEndpointFromObject(obj)
			if err != nil {
				log.Errorf("edgecdnx: failed to delete DNSEndpoint: %v", err)
				return
			}

			dm.Sync.Lock()
			defer dm.Sync.Unlock()
			dm.updateEndpointSources(dnsEndpoint, nil)
			log.Infof("edgecdnx: Deleted DNSEndpoint %s", dnsEndpoint.Name)
		},
	}
}

func dnsEndpointFromObject(obj any) (*infrastructurev1alpha1.DNSEndpoint, error) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}

	raw, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("expected DNSEndpoint object, got %T", obj)
	}

	encoded, err := json.Marshal(raw.Object)
	if err != nil {
		return nil, fmt.Errorf("marshal DNSEndpoint: %w", err)
	}

	dnsEndpoint := &infrastructurev1alpha1.DNSEndpoint{}
	if err := json.Unmarshal(encoded, dnsEndpoint); err != nil {
		return nil, fmt.Errorf("unmarshal DNSEndpoint: %w", err)
	}
	if _, ok := dns.StringToType[strings.ToUpper(dnsEndpoint.Spec.RecordType)]; !ok {
		return nil, fmt.Errorf("unsupported DNSEndpoint record type %q", dnsEndpoint.Spec.RecordType)
	}

	return dnsEndpoint, nil
}
