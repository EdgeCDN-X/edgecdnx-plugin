package edgecdnxplugin

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/miekg/dns"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type ZoneManager struct {
	fac      dynamicinformer.DynamicSharedInformerFactory
	Informer cache.SharedIndexInformer
	Sync     *sync.RWMutex
	Zones    []string
	Records  map[string][]dns.RR
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
	soa, err := dns.NewRR(fmt.Sprintf("$ORIGIN %s\n@ IN SOA %s.%s %s. %s 7200 3600 1209600 3600", zoneNormalized, soaRec, zoneNormalized, zone.Spec.Email, serial))
	if err != nil {
		log.Errorf("edgecdnx: failed to create SOA record: %v", err)
		return nil, err
	}
	log.Debugf("edgecdnx: Crafted SOA record for zone %s: %s", zoneNormalized, soa.String())
	recordList = append(recordList, soa)

	// Allowed in this block lets continue
	for _, n := range ns {
		re, err := dns.NewRR(fmt.Sprintf("$ORIGIN %s\n@ IN NS %s\n", zoneNormalized, n.Name))
		if err != nil {
			log.Errorf("edgecdnx: failed to create NS record: %v", err)
			return nil, err
		}
		recordList = append(recordList, re)
		log.Debugf("edgecdnx: Crafted NS record for zone %s: %s", zoneNormalized, re.String())

		re, err = dns.NewRR(fmt.Sprintf("$ORIGIN %s\n%s IN A %s", zoneNormalized, n.Name, n.IPv4))
		if err != nil {
			log.Errorf("edgecdnx: failed to create NS A record: %v", err)
			return nil, err
		}
		recordList = append(recordList, re)

		log.Debugf("edgecdnx: Crafted NS A record for zone %s: %s", zoneNormalized, re.String())
	}

	return recordList, nil
}

// NewZoneManager creates a new ZoneManager that watches Zone CRDs in the specified namespace
func NewZoneManager(factory dynamicinformer.DynamicSharedInformerFactory, config ZoneManagerConfiguration) *ZoneManager {
	zm := &ZoneManager{
		fac:     factory,
		Sync:    &sync.RWMutex{},
		Zones:   []string{},
		Records: make(map[string][]dns.RR),
	}

	zoneInformer := factory.ForResource(schema.GroupVersionResource{
		Group:    infrastructurev1alpha1.SchemeGroupVersion.Group,
		Version:  infrastructurev1alpha1.SchemeGroupVersion.Version,
		Resource: "zones",
	}).Informer()

	log.Infof("edgecdnx: Watching Zones in namespace %s", config.Namespace)

	zoneInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			s_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnx: expected Zone object, got %T", obj)
				return
			}

			temp, err := json.Marshal(s_raw.Object)
			if err != nil {
				log.Errorf("edgecdnx: failed to marshal Zone object: %v", err)
				return
			}
			zone := &infrastructurev1alpha1.Zone{}
			err = json.Unmarshal(temp, zone)
			if err != nil {
				log.Errorf("edgecdnx: failed to unmarshal Zone object: %v", err)
				return
			}

			zm.Sync.Lock()
			defer zm.Sync.Unlock()

			zoneNormalized := fmt.Sprintf("%s.", zone.Spec.Zone)
			lookup := plugin.Zones(config.Origins).Matches(zoneNormalized)

			if lookup != "" {
				match := plugin.Zones(zm.Zones).Matches(zoneNormalized)
				if match != "" {
					log.Warningf("edgecdnx: Zone %s already covered by %s, ignoring", zoneNormalized, match)
					return
				}

				recordList, err := buildZoneRecords(*zone, config.SOA, config.NS)
				if err != nil {
					log.Errorf("edgecdnx: failed to build zone records for zone %s: %v", zoneNormalized, err)
					return
				}

				zm.Zones = append(zm.Zones, zoneNormalized)
				zm.Records[zoneNormalized] = recordList
				log.Infof("edgecdnx: Added Zone %s, with the following records: %v", zoneNormalized, recordList)
			} else {
				log.Warningf("edgecdnx: Zone %s is not served in this block. Ignoring", zoneNormalized)
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			z_new_raw, ok := newObj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnx: expected zone object, got %T", z_new_raw)
				return
			}

			temp, err := json.Marshal(z_new_raw.Object)
			if err != nil {
				log.Errorf("edgecdnx: failed to marshal zone object: %v", err)
				return
			}
			newZone := &infrastructurev1alpha1.Zone{}
			err = json.Unmarshal(temp, newZone)
			if err != nil {
				log.Errorf("edgecdnx: failed to unmarshal zone object: %v", err)
				return
			}

			zm.Sync.Lock()
			defer zm.Sync.Unlock()

			zoneNormalized := fmt.Sprintf("%s.", newZone.Spec.Zone)
			recordList, err := buildZoneRecords(*newZone, config.SOA, config.NS)
			if err != nil {
				log.Errorf("edgecdnx: failed to build zone records for zone %s: %v", zoneNormalized, err)
				return
			}

			delete(zm.Records, zoneNormalized)
			zm.Records[zoneNormalized] = recordList
		},
		DeleteFunc: func(obj any) {
			z_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnx: expected Zone object, got %T", obj)
				return
			}

			temp, err := json.Marshal(z_raw.Object)
			if err != nil {
				log.Errorf("edgecdnx: failed to marshal Zone object: %v", err)
				return
			}
			zone := &infrastructurev1alpha1.Zone{}
			err = json.Unmarshal(temp, zone)
			if err != nil {
				log.Errorf("edgecdnx: failed to unmarshal Service object: %v", err)
				return
			}

			zm.Sync.Lock()
			defer zm.Sync.Unlock()

			zoneNormalized := fmt.Sprintf("%s.", zone.Spec.Zone)
			delete(zm.Records, zoneNormalized)
			zm.Zones = slices.DeleteFunc(zm.Zones, func(z string) bool {
				return z == zoneNormalized
			})
			log.Infof("edgecdnx: Deleted Zone %s", zoneNormalized)
		},
	})

	zm.Informer = zoneInformer

	return zm
}
