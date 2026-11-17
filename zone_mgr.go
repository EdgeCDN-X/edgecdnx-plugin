package edgecdnxplugin

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/miekg/dns"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type ZoneManager struct {
	fac                 dynamicinformer.DynamicSharedInformerFactory
	Informer            cache.SharedIndexInformer
	HandlerRegistration cache.ResourceEventHandlerRegistration
	Sync                *sync.RWMutex
	snapshot            atomic.Pointer[zoneSnapshot]
	sources             map[string]*infrastructurev1alpha1.Zone
}

type zoneSnapshot struct {
	zones map[string]*zoneEntry
}

type zoneEntry struct {
	name     string
	records  []dns.RR
	soa      dns.RR
	conflict bool
}

// lookup takes a normalized FQDN and checks every ancestor for conflicts.
func (s *zoneSnapshot) lookup(name string) (zone *zoneEntry, conflict bool) {
	if s == nil {
		return nil, false
	}
	for {
		if entry := s.zones[name]; entry != nil {
			if entry.conflict {
				conflict = true
			} else if zone == nil {
				zone = entry
			}
		}
		if name == "." {
			break
		}
		offset, end := dns.NextLabel(name, 0)
		if end {
			name = "."
		} else {
			name = name[offset:]
		}
	}
	return zone, conflict
}

type ZoneManagerConfiguration struct {
	Namespace string
	SOA       string
	NS        []NSRecord
	Origins   []string
}

// buildZoneRecords builds the DNS records for a given Zone CRD
func buildZoneRecords(zone infrastructurev1alpha1.Zone, soaRec string, ns []NSRecord) ([]dns.RR, error) {
	zoneNormalized := fmt.Sprintf("%s.", zone.Spec.Zone)

	// NS records + SOA
	recordList := make([]dns.RR, 0)

	serial := time.Now().Format("20060102") + "00"
	// Create SOA Record
	soa, err := dns.NewRR(fmt.Sprintf("$ORIGIN %s\n@ IN SOA %s %s %s 7200 3600 1209600 3600", zoneNormalized, soaRec, zone.Spec.Email, serial))
	if err != nil {
		log.Errorf("edgecdnx: failed to create SOA record: %v", err)
		return nil, err
	}
	log.Debugf("edgecdnx: Crafted SOA record for zone %s: %s", zoneNormalized, soa.String())
	recordList = append(recordList, soa)

	// Allowed in this block lets continue
	for _, n := range ns {
		re, err := dns.NewRR(fmt.Sprintf("$ORIGIN %s\n@ IN NS %s\n", zoneNormalized, n.Target))
		if err != nil {
			log.Errorf("edgecdnx: failed to create NS record: %v", err)
			return nil, err
		}
		recordList = append(recordList, re)
		log.Debugf("edgecdnx: Crafted NS record for zone %s: %s", zoneNormalized, re.String())
	}

	return recordList, nil
}

// NewZoneManager creates a new ZoneManager that watches Zone CRDs in the specified namespace
func NewZoneManager(factory dynamicinformer.DynamicSharedInformerFactory, config ZoneManagerConfiguration) *ZoneManager {
	zm := &ZoneManager{
		fac:  factory,
		Sync: &sync.RWMutex{},
	}

	zoneInformer := factory.ForResource(schema.GroupVersionResource{
		Group:    infrastructurev1alpha1.SchemeGroupVersion.Group,
		Version:  infrastructurev1alpha1.SchemeGroupVersion.Version,
		Resource: "zones",
	}).Informer()

	log.Infof("edgecdnx: Watching Zones in namespace %s", config.Namespace)

	registration, err := zoneInformer.AddEventHandler(zm.zoneEventHandlers(config))
	if err != nil {
		log.Errorf("edgecdnx: failed to register Zone event handler: %v", err)
	} else {
		zm.HandlerRegistration = registration
	}

	zm.Informer = zoneInformer

	return zm
}

func (zm *ZoneManager) zoneEventHandlers(config ZoneManagerConfiguration) cache.ResourceEventHandlerFuncs {
	handle := func(object any, deleted bool) {
		if tombstone, ok := object.(cache.DeletedFinalStateUnknown); ok {
			object = tombstone.Obj
		}
		resource, ok := object.(*unstructured.Unstructured)
		if !ok {
			log.Errorf("edgecdnx: expected Zone object, got %T", object)
			return
		}
		zone := &infrastructurev1alpha1.Zone{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(resource.Object, zone); err != nil {
			log.Errorf("edgecdnx: failed to decode Zone: %v", err)
			return
		}
		zm.Sync.Lock()
		defer zm.Sync.Unlock()
		if zm.sources == nil {
			zm.sources = make(map[string]*infrastructurev1alpha1.Zone)
		}
		key := resourceKey(zone.Namespace, zone.Name)
		if deleted {
			delete(zm.sources, key)
		} else {
			zm.sources[key] = zone
		}
		zm.rebuildZonesLocked(config)
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(object any) { handle(object, false) },
		UpdateFunc: func(_, object any) { handle(object, false) },
		DeleteFunc: func(object any) { handle(object, true) },
	}
}

func (zm *ZoneManager) rebuildZonesLocked(config ZoneManagerConfiguration) {
	byDomain := make(map[string][]*infrastructurev1alpha1.Zone)
	for _, zone := range zm.sources {
		domain := dns.Fqdn(strings.ToLower(zone.Spec.Zone))
		if plugin.Zones(config.Origins).Matches(domain) != "" {
			byDomain[domain] = append(byDomain[domain], zone)
		}
	}
	snapshot := &zoneSnapshot{zones: make(map[string]*zoneEntry, len(byDomain))}
	for domain, zones := range byDomain {
		if len(zones) > 1 {
			snapshot.zones[domain] = &zoneEntry{name: domain, conflict: true}
			log.Errorf("edgecdnx: multiple Zones claim %s; refusing ambiguous answers", domain)
			continue
		}
		records, err := buildZoneRecords(*zones[0], config.SOA, config.NS)
		if err != nil {
			continue
		}
		entry := &zoneEntry{name: domain, records: records}
		for _, rr := range records {
			if rr.Header().Rrtype == dns.TypeSOA {
				entry.soa = rr
				break
			}
		}
		snapshot.zones[domain] = entry
	}
	zm.snapshot.Store(snapshot)
}

func (zm *ZoneManager) hasConflict(qname string) bool {
	_, conflict := zm.snapshot.Load().lookup(dns.Fqdn(strings.ToLower(qname)))
	return conflict
}
