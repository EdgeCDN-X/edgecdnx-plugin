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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
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
	Config    LocationManagerConfiguration
}

type HashFilters struct {
	Cache         string
	Qtype         uint16
	RouteSelector *metav1.LabelSelector
	ServiceName   string
}

type FilteredNodeWithMeta struct {
	Node         infrastructurev1alpha1.NodeSpec
	LocationName string
	NodeStatus   infrastructurev1alpha1.NodeInstanceStatus
}

func matchesLabelSelector(objectLabels map[string]string, selector *metav1.LabelSelector) bool {
	if selector == nil {
		return true
	}
	if len(objectLabels) == 0 && len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 0 {
		return true
	}

	labelSelector, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		log.Warningf("edgecdnx: invalid LabelSelector %v: %v", selector, err)
		return true
	}

	return labelSelector.Matches(labels.Set(objectLabels))
}

func (l LocationManager) GetLocationByName(name string) (infrastructurev1alpha1.Location, error) {
	l.Sync.RLock()
	defer l.Sync.RUnlock()

	location, exists := l.Locations[name]
	if !exists {
		return infrastructurev1alpha1.Location{}, fmt.Errorf("Location %s not found", name)
	}

	return location, nil
}

func (l LocationManager) ApplyHash(location *infrastructurev1alpha1.Location, hashInput string, filters HashFilters) (FilteredNodeWithMeta, error) {
	filteredNodes := make([]FilteredNodeWithMeta, 0)

	if location.Spec.MaintenanceMode {
		log.Debug(fmt.Sprintf("edgecdnxgeolookup: Location %s is in maintenance mode", location.Name))
		return FilteredNodeWithMeta{}, fmt.Errorf("Location %s is in maintenance mode", location.Name)
	}

	if location.Status.Alerts != nil && len(location.Status.Alerts) > 0 {
		log.Debugf("edgecdnxgeolookup: Location %s has active alerts. %v", location.Name, func() []string {
			alertNames := make([]string, 0, len(location.Status.Alerts))
			for _, alert := range location.Status.Alerts {
				alertNames = append(alertNames, alert.AlertName)
			}
			return alertNames
		}())
		return FilteredNodeWithMeta{}, fmt.Errorf("Location %s has active alerts", location.Name)
	}

	// Add only nodes which are not in maintenance mode and match the cache filter
	for _, ng := range location.Spec.NodeGroups {
		if ng.Name == filters.Cache {
			for _, node := range ng.Nodes {
				if node.MaintenanceMode {
					continue
				}

				nodeStatus, exists := location.Status.NodeStatus[node.Name]
				if !exists {
					nodeStatus = infrastructurev1alpha1.NodeInstanceStatus{
						Conditions: []infrastructurev1alpha1.NodeCondition{},
						Alerts:     []infrastructurev1alpha1.PrometheusAlertStatus{},
					}
				}

				filteredNodes = append(filteredNodes, FilteredNodeWithMeta{
					Node:         node,
					LocationName: location.Name,
					NodeStatus:   nodeStatus,
				})
			}
		}
	}

	locations_raw, err := l.Informer.GetIndexer().ByIndex("byParent", location.Name)
	if err != nil {
		log.Errorf("edgecdnxgeolookup: failed to get child locations for location %s: %v", location.Name, err)
	} else {
		for _, loc := range locations_raw {
			childLocationUnstructured, ok := loc.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxgeolookup: expected Location object, got %T", loc)
				continue
			}

			temp, err := json.Marshal(childLocationUnstructured.Object)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: failed to marshal child location object: %v", err)
				continue
			}
			childLocation := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, childLocation)
			if err != nil {
				log.Errorf("edgecdnxgeolookup: failed to unmarshal child location object: %v", err)
				continue
			}

			if !matchesLabelSelector(childLocation.Labels, filters.RouteSelector) {
				log.Debugf("edgecdnxgeolookup: Child Location %s does not match routeSelector for service %s", childLocation.Name, filters.ServiceName)
				continue
			}

			if childLocation.Spec.MaintenanceMode {
				log.Debug(fmt.Sprintf("edgecdnxgeolookup: Child Location %s is in maintenance mode, skipping", childLocation.Name))
				continue
			}

			if childLocation.Status.Alerts != nil && len(childLocation.Status.Alerts) > 0 {
				log.Debugf("edgecdnxgeolookup: Child Location %s has active alerts, skipping. %v", childLocation.Name, func() []string {
					alertNames := make([]string, 0, len(childLocation.Status.Alerts))
					for _, alert := range childLocation.Status.Alerts {
						alertNames = append(alertNames, alert.AlertName)
					}
					return alertNames
				}())
				continue
			}

			for _, ng := range childLocation.Spec.NodeGroups {
				if ng.Name == filters.Cache {
					for _, node := range ng.Nodes {
						if node.MaintenanceMode {
							continue
						}

						nodeStatus, exists := childLocation.Status.NodeStatus[node.Name]
						if !exists {
							nodeStatus = infrastructurev1alpha1.NodeInstanceStatus{
								Conditions: []infrastructurev1alpha1.NodeCondition{},
								Alerts:     []infrastructurev1alpha1.PrometheusAlertStatus{},
							}
						}

						filteredNodes = append(filteredNodes, FilteredNodeWithMeta{
							Node:         node,
							LocationName: childLocation.Name,
							NodeStatus:   nodeStatus,
						})
					}
				}
			}
		}
	}

	log.Debugf("edgecdnxgeolookup: Found %d nodes in location %s matching cache %s", len(filteredNodes), location.Name, filters.Cache)

	for {
		if len(filteredNodes) == 0 {
			return FilteredNodeWithMeta{}, fmt.Errorf("No healthy nodes found in location %s with cache %s", location.Name, filters.Cache)
		}

		hash := md5.Sum([]byte(hashInput))
		lastFourBytes := hash[len(hash)-4:]
		hashValue := uint32(lastFourBytes[0])<<24 | uint32(lastFourBytes[1])<<16 | uint32(lastFourBytes[2])<<8 | uint32(lastFourBytes[3])
		nodeIndex := int(hashValue % uint32(len(filteredNodes)))
		node := filteredNodes[nodeIndex]

		if node.NodeStatus.Conditions == nil || len(node.NodeStatus.Conditions) == 0 {
			log.Debugf("edgecdnxgeolookup: Node %s in location %s has no status, assuming healthy", node.Node.Name, node.LocationName)
			return node, nil
		}

		if idx := slices.IndexFunc(node.NodeStatus.Conditions, func(c infrastructurev1alpha1.NodeCondition) bool {
			switch filters.Qtype {
			case dns.TypeA:
				return c.Type == infrastructurev1alpha1.IPV4HealthCheckSuccessful
			case dns.TypeAAAA:
				return c.Type == infrastructurev1alpha1.IPV6HealthCheckSuccessful
			default:
				return false
			}
		}); idx != -1 {
			condition := node.NodeStatus.Conditions[idx]
			if !condition.Status {
				log.Debugf("edgecdnxgeolookup: Node %s is not healthy for qtype %d, trying next node", node.Node.Name, filters.Qtype)
				filteredNodes = slices.Delete(filteredNodes, nodeIndex, nodeIndex+1)
				continue
			}
		} else {
			log.Debugf("edgecdnxgeolookup: Node %s has no health check condition for qtype %d, assuming healthy", node.Node.Name, filters.Qtype)
		}

		if node.NodeStatus.Alerts != nil && len(node.NodeStatus.Alerts) > 0 {
			log.Debugf("edgecdnxgeolookup: Node %s has active alerts, trying next node. %v", node.Node.Name, func() []string {
				alertNames := make([]string, 0, len(node.NodeStatus.Alerts))
				for _, alert := range node.NodeStatus.Alerts {
					alertNames = append(alertNames, alert.AlertName)
				}
				return alertNames
			}())
			filteredNodes = slices.Delete(filteredNodes, nodeIndex, nodeIndex+1)
			continue
		}

		return filteredNodes[nodeIndex], nil
	}
}

func (l LocationManager) PerformGeoLookup(ctx context.Context, service infrastructurev1alpha1.Service, cache string) (string, error) {
	maxValue := 0
	locationScore := make(map[string]int)

	l.Sync.RLock()
	defer l.Sync.RUnlock()

	for locationName, location := range l.Locations {
		if !matchesLabelSelector(location.Labels, service.Spec.RouteSelector) {
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: skipping location %s as it does not match routeSelector for service %s", locationName, service.Name))
			continue
		}
		if slices.IndexFunc(location.Spec.NodeGroups, func(ng infrastructurev1alpha1.NodeGroupSpec) bool { return ng.Name == cache }) == -1 {
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: skipping location %s as it does not have node group for cache %s", locationName, cache))
			continue
		}

		for attrName, attribute := range location.Spec.GeoLookup.Attributes {
			if lookupFunc := metadata.ValueFunc(ctx, attrName); lookupFunc != nil {
				if lookupValue := lookupFunc(); lookupValue != "" {
					log.Debugf("edgecdnx: looking up attribute %s with value %s", attrName, lookupValue)
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
		Config:    config,
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

	err := locationInformer.GetIndexer().AddIndexers(cache.Indexers{
		"byParent": func(obj any) ([]string, error) {
			location, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return []string{}, fmt.Errorf("expected Location object, got %T", obj)
			}

			temp, err := json.Marshal(location.Object)
			if err != nil {
				return []string{}, fmt.Errorf("failed to marshal location object: %v", err)
			}
			locationObj := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, locationObj)
			if err != nil {
				return []string{}, fmt.Errorf("failed to unmarshal location object: %v", err)
			}

			if locationObj.Spec.Parent != "" {
				return []string{locationObj.Spec.Parent}, nil
			}
			return []string{}, nil
		},
	})
	if err != nil {
		log.Errorf("edgecdnxgeolookup: failed to add indexer to location informer: %v", err)
	}

	locationMgr.Informer = locationInformer

	return locationMgr
}
