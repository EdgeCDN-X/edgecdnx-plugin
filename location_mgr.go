package edgecdnxplugin

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin/metadata"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/miekg/dns"
)

type LocationManagerConfiguration struct {
	Namespace string
	RecrodTTL uint32
}

type LocationManager struct {
	fac       dynamicinformer.DynamicSharedInformerFactory
	Informer  cache.SharedIndexInformer
	Sync      *sync.RWMutex
	Locations map[string]infrastructurev1alpha1.Location
}

type HashFilters struct {
	Cache string
	Qtype uint16
}

func (l LocationManager) ApplyHash(location *infrastructurev1alpha1.Location, hashInput string, filters HashFilters) (infrastructurev1alpha1.NodeSpec, error) {
	filteredNodes := make([]infrastructurev1alpha1.NodeSpec, 0)

	if location.Spec.MaintenanceMode {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: Location %s is in maintenance mode", location.Name))
		return infrastructurev1alpha1.NodeSpec{}, fmt.Errorf("Location %s is in maintenance mode", location.Name)
	}

	// Add only nodes which are not in maintenance mode and match the cache filter
	for _, ng := range location.Spec.NodeGroups {
		if ng.Name == filters.Cache {
			for _, node := range ng.Nodes {
				if node.MaintenanceMode {
					continue
				}
				filteredNodes = append(filteredNodes, node)
			}
		}
	}

	for {
		if len(filteredNodes) == 0 {
			return infrastructurev1alpha1.NodeSpec{}, fmt.Errorf("No healthy nodes found in location %s with cache %s", location.Name, filters.Cache)
		}

		hash := md5.Sum([]byte(hashInput))
		lastFourBytes := hash[len(hash)-4:]
		hashValue := uint32(lastFourBytes[0])<<24 | uint32(lastFourBytes[1])<<16 | uint32(lastFourBytes[2])<<8 | uint32(lastFourBytes[3])
		nodeIndex := int(hashValue % uint32(len(filteredNodes)))
		nodeName := filteredNodes[nodeIndex].Name

		nodeStatus, exists := location.Status.NodeStatus[nodeName] // Access to ensure node status exists
		if !exists {
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: Node %s has no status, assuming healthy", nodeName))
			return filteredNodes[nodeIndex], nil
		}

		if idx := slices.IndexFunc(nodeStatus.Conditions, func(c infrastructurev1alpha1.NodeCondition) bool {
			switch filters.Qtype {
			case dns.TypeA:
				return c.Type == infrastructurev1alpha1.IPV4HealthCheckSuccessful
			case dns.TypeAAAA:
				return c.Type == infrastructurev1alpha1.IPV6HealthCheckSuccessful
			default:
				return false
			}
		}); idx != -1 {
			condition := nodeStatus.Conditions[idx]
			if !condition.Status {
				log.Debugf("edgecdnxgeolookup: Node %s is not healthy for qtype %d, trying next node", nodeName, filters.Qtype)
				filteredNodes = slices.Delete(filteredNodes, nodeIndex, nodeIndex+1)
				continue
			}
			return filteredNodes[nodeIndex], nil
		} else {
			log.Debugf("edgecdnxgeolookup: Node %s has no health check condition for qtype %d, assuming healthy", nodeName, filters.Qtype)
			return filteredNodes[nodeIndex], nil
		}
	}
}

func (l LocationManager) PerformGeoLookup(ctx context.Context, cache string) (string, error) {
	maxValue := 0
	locationScore := make(map[string]int)

	l.Sync.RLock()
	defer l.Sync.RUnlock()

	for locationName, location := range l.Locations {
		if slices.IndexFunc(location.Spec.NodeGroups, func(ng infrastructurev1alpha1.NodeGroupSpec) bool { return ng.Name == cache }) == -1 {
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: skipping location %s as it does not have node group for cache %s", locationName, cache))
			continue
		}

		for attrName, attribute := range location.Spec.GeoLookup.Attributes {
			if lookupFunc := metadata.ValueFunc(ctx, attrName); lookupFunc != nil {
				if lookupValue := lookupFunc(); lookupValue != "" {
					for _, attributeValue := range attribute.Values {
						if attributeValue.Value == lookupValue {
							log.Debug(fmt.Sprintf("edgecdnxgeolookup: found attribute %s with value %s", attrName, lookupValue))

							currScore, ok := locationScore[locationName]
							if !ok {
								currScore = 0
							}
							if currScore+attribute.Weight > maxValue {
								maxValue = currScore + attribute.Weight + attributeValue.Weight
							}
							locationScore[locationName] = currScore + attribute.Weight + attributeValue.Weight
						}
					}
				}
			}
		}
	}

	winners := make([]string, 0)

	for locationName, score := range locationScore {
		if score == maxValue {
			winners = append(winners, locationName)
		}
	}

	log.Debug(fmt.Sprintf("edgecdnxgeolookup: found %d locations with score %d: %v", len(winners), maxValue, winners))

	if len(winners) > 1 {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: multiple locations found with same score %d: %v", maxValue, winners))

		randomNumber := rand.Float64()
		totalWeigth := 0

		for _, locationName := range winners {
			location := l.Locations[locationName]
			totalWeigth = totalWeigth + location.Spec.GeoLookup.Weight
		}

		selector := (float64(totalWeigth) * randomNumber)

		currentWeight := 0
		for _, locationName := range winners {
			location := l.Locations[locationName]
			currentWeight += location.Spec.GeoLookup.Weight
			if int(selector) <= currentWeight {
				return locationName, nil
			}
		}
	}

	if len(winners) == 1 {
		return winners[0], nil
	}

	return "", errors.New("No Location Found")
}

func (l LocationManager) HasCacheType(cacheType string, location string) bool {
	l.Sync.RLock()
	defer l.Sync.RUnlock()

	loc, ok := l.Locations[location]
	if !ok {
		return false
	}

	for _, ct := range loc.Spec.NodeGroups {
		if ct.Name == cacheType {
			return true
		}
	}

	return false
}

func NewLocationManager(factory dynamicinformer.DynamicSharedInformerFactory, config LocationManagerConfiguration) *LocationManager {
	locationMgr := &LocationManager{
		fac:       factory,
		Sync:      &sync.RWMutex{},
		Locations: make(map[string]infrastructurev1alpha1.Location),
	}

	// TODO populate caches based on location name
	locationInformer := locationMgr.fac.ForResource(infrastructurev1alpha1.SchemeGroupVersion.WithResource("locations")).Informer()

	locationInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			l_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxgeolookup: Failed to cast object to unstructured.Unstructured")
				return
			}

			temp, err := json.Marshal(l_raw.Object)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: Failed to marshal location object: %v", err)
				return
			}
			location := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, location)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: Failed to unmarshal location object: %v", err)
				return
			}

			locationMgr.Sync.Lock()
			defer locationMgr.Sync.Unlock()
			locationMgr.Locations[location.Name] = *location
			log.Infof("edgecdnxgeolookup: Added Location %s", location.Name)
		},
		UpdateFunc: func(oldObj, newObj any) {
			s_new_raw, ok := newObj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxgeolookup: expected Location object, got %T", s_new_raw)
				return
			}
			temp, err := json.Marshal(s_new_raw.Object)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: failed to marshal Location object: %v", err)
				return
			}
			location := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, location)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: failed to unmarshal Location object: %v", err)
				return
			}
			locationMgr.Sync.Lock()
			defer locationMgr.Sync.Unlock()
			locationMgr.Locations[location.Name] = *location
			log.Infof("edgecdnxgeolookup: Updated Location %s", location.Name)
		},
		DeleteFunc: func(obj any) {
			s_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxgeolookup: expected Location object, got %T", obj)
				return
			}

			temp, err := json.Marshal(s_raw.Object)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: failed to marshal Location object: %v", err)
				return
			}
			location := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, location)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: failed to unmarshal Location object: %v", err)
				return
			}

			locationMgr.Sync.Lock()
			defer locationMgr.Sync.Unlock()
			delete(locationMgr.Locations, location.Name)
			log.Infof("edgecdnxgeolookup: Deleted Location %s", location.Name)
		},
	})

	locationMgr.Informer = locationInformer

	return locationMgr
}
