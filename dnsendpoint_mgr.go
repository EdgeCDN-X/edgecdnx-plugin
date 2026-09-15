package edgecdnxplugin

import (
	"encoding/json"
	"fmt"
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

type DNSEndpointManagerConfiguration struct {
	Namespace string
}

type DNSEndpointManager struct {
	fac                dynamicinformer.DynamicSharedInformerFactory
	Informer           cache.SharedIndexInformer
	Sync               *sync.RWMutex
	DNSEndpoints       map[string]infrastructurev1alpha1.DNSEndpoint
	roundRobinCounters map[string]*atomic.Uint32
}

func dnsEndpointKey(dnsName string, recordType string) string {
	return dns.Fqdn(strings.ToLower(dnsName)) + "/" + strings.ToUpper(recordType)
}

func isRoundRobinPolicy(routingPolicy string) bool {
	return strings.EqualFold(routingPolicy, "roundrobin")
}

// GetNext increments the round-robin counter for the DNSEndpoint identified by qname/qtype and
// returns it modulo mod. If mod is 0, 0 is returned. If no counter exists (e.g. the DNSEndpoint
// is not RoundRobin routed), 0 is returned.
func (dm *DNSEndpointManager) GetNext(qname string, qtype uint16, mod int) int {
	if mod == 0 {
		return 0
	}

	recordType := dns.TypeToString[qtype]
	key := dnsEndpointKey(qname, recordType)

	dm.Sync.RLock()
	counter, ok := dm.roundRobinCounters[key]
	dm.Sync.RUnlock()
	if !ok {
		return 0
	}

	next := counter.Add(1)
	if next >= roundRobinResetThreshold {
		counter.Store(0)
	}

	return int(next % uint32(mod))
}

func (dm *DNSEndpointManager) GetDNSEndpoint(qname string, qtype uint16) (infrastructurev1alpha1.DNSEndpoint, error) {
	recordType := dns.TypeToString[qtype]
	log.Debugf("edgecdnx: Looking up DNSEndpoint for %s %s", qname, recordType)

	dm.Sync.RLock()
	defer dm.Sync.RUnlock()

	if dnsEndpoint, ok := dm.DNSEndpoints[dnsEndpointKey(qname, recordType)]; ok {
		return dnsEndpoint, nil
	}

	if dnsEndpoint, ok := dm.DNSEndpoints[dnsEndpointKey(qname, "CNAME")]; ok {
		return dnsEndpoint, nil
	}

	return infrastructurev1alpha1.DNSEndpoint{}, fmt.Errorf("DNSEndpoint not found for %s %s", qname, recordType)
}

func NewDNSEndpointManager(factory dynamicinformer.DynamicSharedInformerFactory, config DNSEndpointManagerConfiguration) *DNSEndpointManager {
	dm := &DNSEndpointManager{
		fac:                factory,
		Sync:               &sync.RWMutex{},
		DNSEndpoints:       make(map[string]infrastructurev1alpha1.DNSEndpoint),
		roundRobinCounters: make(map[string]*atomic.Uint32),
	}

	dnsEndpointInformer := factory.ForResource(schema.GroupVersionResource{
		Group:    infrastructurev1alpha1.SchemeGroupVersion.Group,
		Version:  infrastructurev1alpha1.SchemeGroupVersion.Version,
		Resource: "dnsendpoints",
	}).Informer()

	log.Infof("edgecdnx: Watching DNSEndpoints in namespace %s", config.Namespace)

	dnsEndpointInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			dnsEndpoint, err := dnsEndpointFromObject(obj)
			if err != nil {
				log.Errorf("edgecdnx: failed to add DNSEndpoint: %v", err)
				return
			}

			dm.Sync.Lock()
			defer dm.Sync.Unlock()
			key := dnsEndpointKey(dnsEndpoint.Spec.DNSName, dnsEndpoint.Spec.RecordType)
			dm.DNSEndpoints[key] = *dnsEndpoint
			if isRoundRobinPolicy(dnsEndpoint.Spec.RoutingPolicy) {
				dm.roundRobinCounters[key] = &atomic.Uint32{}
			}
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
			oldKey := dnsEndpointKey(oldDNSEndpoint.Spec.DNSName, oldDNSEndpoint.Spec.RecordType)
			newKey := dnsEndpointKey(newDNSEndpoint.Spec.DNSName, newDNSEndpoint.Spec.RecordType)
			delete(dm.DNSEndpoints, oldKey)
			dm.DNSEndpoints[newKey] = *newDNSEndpoint

			if oldKey != newKey {
				delete(dm.roundRobinCounters, oldKey)
			}
			if isRoundRobinPolicy(newDNSEndpoint.Spec.RoutingPolicy) {
				if _, ok := dm.roundRobinCounters[newKey]; !ok {
					dm.roundRobinCounters[newKey] = &atomic.Uint32{}
				}
			} else {
				delete(dm.roundRobinCounters, newKey)
			}
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
			key := dnsEndpointKey(dnsEndpoint.Spec.DNSName, dnsEndpoint.Spec.RecordType)
			delete(dm.DNSEndpoints, key)
			delete(dm.roundRobinCounters, key)
			log.Infof("edgecdnx: Deleted DNSEndpoint %s", dnsEndpoint.Name)
		},
	})

	dm.Informer = dnsEndpointInformer

	return dm
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

	return dnsEndpoint, nil
}
