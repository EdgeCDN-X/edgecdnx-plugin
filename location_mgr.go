package edgecdnxplugin

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/miekg/dns"
)

type LocationManagerConfiguration struct {
	Namespace string
	RecordTTL uint32
}

type LocationManager struct {
	fac       dynamicinformer.DynamicSharedInformerFactory
	Informer  cache.SharedIndexInformer
	Sync      *sync.RWMutex
	Locations map[string]infrastructurev1alpha1.Location
	Config    LocationManagerConfiguration
	revision  atomic.Uint64
}

type HashFilters struct {
	Qtype         uint16
	RouteSelector *metav1.LabelSelector
	EndpointName  string
}

type FilteredNodeWithMeta struct {
	Node         infrastructurev1alpha1.NodeSpec
	LocationName string
	NodeStatus   infrastructurev1alpha1.NodeInstanceStatus
}

func resourceKey(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
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

func (l *LocationManager) GetLocationByName(name string, scopes ...routingScope) (infrastructurev1alpha1.Location, error) {
	l.Sync.RLock()
	defer l.Sync.RUnlock()

	if len(scopes) != 0 && !strings.Contains(name, "/") {
		name = resourceKey(scopes[0].namespace, name)
	}
	location, exists := l.Locations[name]
	if !exists || !matchesRoutingScope(location.Namespace, location.Spec.Project, scopes) {
		return infrastructurev1alpha1.Location{}, fmt.Errorf("Location %s not found", name)
	}

	return location, nil
}

func (l *LocationManager) ApplyHash(location *infrastructurev1alpha1.Location, hashInput string, filters HashFilters) (FilteredNodeWithMeta, error) {
	filteredNodes := make([]FilteredNodeWithMeta, 0)

	if location.Spec.MaintenanceMode {
		log.Debug(fmt.Sprintf("edgecdnx: Location %s is in maintenance mode", location.Name))
		return FilteredNodeWithMeta{}, fmt.Errorf("Location %s is in maintenance mode", location.Name)
	}

	if location.Status.Alerts != nil && len(location.Status.Alerts) > 0 {
		log.Debugf("edgecdnx: Location %s has active alerts. %v", location.Name, func() []string {
			alertNames := make([]string, 0, len(location.Status.Alerts))
			for _, alert := range location.Status.Alerts {
				alertNames = append(alertNames, alert.AlertName)
			}
			return alertNames
		}())
		return FilteredNodeWithMeta{}, fmt.Errorf("Location %s has active alerts", location.Name)
	}

	// Add only nodes which are not in maintenance mode and match the nodeGroup label selector.
	// Route selectors match against the owning location labels combined with node group labels.
	for _, ng := range location.Spec.NodeGroups {
		fullLabels := make(map[string]string)
		maps.Copy(fullLabels, location.Labels)
		maps.Copy(fullLabels, ng.Labels)

		if !matchesLabelSelector(fullLabels, filters.RouteSelector) {
			log.Debugf("edgecdnx: Node group %s does not match routeSelector for DNSEndpoint %s", ng.Name, filters.EndpointName)
			continue
		}

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

	locations_raw, err := l.Informer.GetIndexer().ByIndex("byParent", resourceKey(location.Namespace, location.Name))
	if err != nil {
		log.Errorf("edgecdnx: failed to get child locations for location %s: %v", location.Name, err)
	} else {
		for _, loc := range locations_raw {
			childLocationUnstructured, ok := loc.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnx: expected Location object, got %T", loc)
				continue
			}

			temp, err := json.Marshal(childLocationUnstructured.Object)
			if err != nil {
				log.Errorf("edgecdnx: failed to marshal child location object: %v", err)
				continue
			}
			childLocation := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, childLocation)
			if err != nil {
				log.Errorf("edgecdnx: failed to unmarshal child location object: %v", err)
				continue
			}
			if scopeFor(childLocation.Namespace, childLocation.Spec.Project) != scopeFor(location.Namespace, location.Spec.Project) {
				continue
			}

			if childLocation.Spec.MaintenanceMode {
				log.Debug(fmt.Sprintf("edgecdnx: Child Location %s is in maintenance mode, skipping", childLocation.Name))
				continue
			}

			if childLocation.Status.Alerts != nil && len(childLocation.Status.Alerts) > 0 {
				log.Debugf("edgecdnx: Child Location %s has active alerts, skipping. %v", childLocation.Name, func() []string {
					alertNames := make([]string, 0, len(childLocation.Status.Alerts))
					for _, alert := range childLocation.Status.Alerts {
						alertNames = append(alertNames, alert.AlertName)
					}
					return alertNames
				}())
				continue
			}

			// Child node groups are eligible when the route selector matches the child
			// location labels combined with the child node group labels.
			for _, ng := range childLocation.Spec.NodeGroups {
				fullLabels := make(map[string]string)
				maps.Copy(fullLabels, childLocation.Labels)
				maps.Copy(fullLabels, ng.Labels)

				if matchesLabelSelector(fullLabels, filters.RouteSelector) {
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

	log.Debugf("edgecdnx: %d node available in location %s matching label selector %s", len(filteredNodes), location.Name, filters.RouteSelector)

	for {
		if len(filteredNodes) == 0 {
			return FilteredNodeWithMeta{}, fmt.Errorf("No healthy nodes found in location %s matching label selector %s", location.Name, filters.RouteSelector)
		}

		hash := md5.Sum([]byte(hashInput))
		lastFourBytes := hash[len(hash)-4:]
		hashValue := uint32(lastFourBytes[0])<<24 | uint32(lastFourBytes[1])<<16 | uint32(lastFourBytes[2])<<8 | uint32(lastFourBytes[3])
		nodeIndex := int(hashValue % uint32(len(filteredNodes)))
		node := filteredNodes[nodeIndex]

		if node.NodeStatus.Conditions == nil || len(node.NodeStatus.Conditions) == 0 {
			log.Debugf("edgecdnx: Node %s in location %s has no status, assuming healthy", node.Node.Name, node.LocationName)
			return node, nil
		}

		if idx := slices.IndexFunc(node.NodeStatus.Conditions, func(c infrastructurev1alpha1.NodeCondition) bool {
			switch filters.Qtype {
			case dns.TypeA:
				return c.Type == infrastructurev1alpha1.HealthCheckSuccessful && c.Status == false && (c.Stack == infrastructurev1alpha1.StackTypeIPv4 || c.Stack == infrastructurev1alpha1.StackTypeDual)
			case dns.TypeAAAA:
				return c.Type == infrastructurev1alpha1.HealthCheckSuccessful && c.Status == false && (c.Stack == infrastructurev1alpha1.StackTypeIPv6 || c.Stack == infrastructurev1alpha1.StackTypeDual)
			default:
				return false
			}
		}); idx > -1 {
			condition := node.NodeStatus.Conditions[idx]
			log.Debugf("edgecdnx: Node %s is not Healthy, HealthCheck %s is failing with reason %s", node.Node.Name, condition.HealthCheckKey, condition.Reason)
			filteredNodes = slices.Delete(filteredNodes, nodeIndex, nodeIndex+1)
			continue
		}

		if node.NodeStatus.Alerts != nil && len(node.NodeStatus.Alerts) > 0 {
			log.Debugf("edgecdnx: Node %s has active alerts, trying next node. %v", node.Node.Name, func() []string {
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

func (l *LocationManager) FindMatchingTargets(ctx context.Context, routeSelector *metav1.LabelSelector, endpointName string) []string {
	weightedLocations := l.FindMatchingTargetsWithWeights(ctx, routeSelector, endpointName)
	locations := make([]string, 0, len(weightedLocations))
	for _, location := range weightedLocations {
		locations = append(locations, location.Name)
	}
	return locations
}

func (l *LocationManager) FindMatchingTargetsWithWeights(ctx context.Context, routeSelector *metav1.LabelSelector, endpointName string) []weightedTarget {
	l.Sync.RLock()
	defer l.Sync.RUnlock()

	return l.findMatchingTargetsWithWeightsLocked(routeSelector)
}

func (l *LocationManager) findMatchingTargetsWithWeightsLocked(routeSelector *metav1.LabelSelector, scopes ...routingScope) []weightedTarget {
	locations := make([]weightedTarget, 0)
	selector := compileRouteSelector(routeSelector)

	for locationName, location := range l.Locations {
		if matchesRoutingScope(location.Namespace, location.Spec.Project, scopes) && selector.Matches(labels.Set(location.Labels)) {
			locations = append(locations, weightedTarget{Name: locationName, Weight: location.Spec.Weight})
		}
	}
	// Stable order so round-robin indices consistently map to the same location.
	sort.Slice(locations, func(i, j int) bool {
		return locations[i].Name < locations[j].Name
	})
	return locations
}

func (l *LocationManager) PerformGeoLookup(ctx context.Context, routeSelector *metav1.LabelSelector, endpointName string) (string, error) {
	l.Sync.RLock()
	candidates := l.findGeoCandidatesLocked(routeSelector)
	l.Sync.RUnlock()
	return candidates.lookup(ctx)
}

func (l *LocationManager) MatchesNodeGroupLabels(location string, selector *metav1.LabelSelector) bool {
	l.Sync.RLock()
	defer l.Sync.RUnlock()

	loc, ok := l.Locations[location]
	if !ok {
		return false
	}

	for _, ng := range loc.Spec.NodeGroups {
		fullLabels := make(map[string]string)
		maps.Copy(fullLabels, loc.ObjectMeta.Labels)
		maps.Copy(fullLabels, ng.Labels)

		if matchesLabelSelector(fullLabels, selector) {
			return true
		}
	}

	return false
}

// setLocationLocked publishes current status but only invalidates targets for routing changes.
func (l *LocationManager) setLocationLocked(location infrastructurev1alpha1.Location) {
	key := resourceKey(location.Namespace, location.Name)
	previous, exists := l.Locations[key]
	l.Locations[key] = location
	if !exists || !equality.Semantic.DeepEqual(previous.Labels, location.Labels) ||
		!equality.Semantic.DeepEqual(previous.Spec, location.Spec) {
		l.revision.Add(1)
	}
}

func NewLocationManager(factory dynamicinformer.DynamicSharedInformerFactory, config LocationManagerConfiguration) *LocationManager {
	locationMgr := &LocationManager{
		fac:       factory,
		Sync:      &sync.RWMutex{},
		Locations: make(map[string]infrastructurev1alpha1.Location),
		Config:    config,
	}

	locationInformer := locationMgr.fac.ForResource(infrastructurev1alpha1.SchemeGroupVersion.WithResource("locations")).Informer()

	locationInformer.AddEventHandler(locationMgr.locationEventHandlers())

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
				return []string{resourceKey(locationObj.Namespace, locationObj.Spec.Parent)}, nil
			}
			return []string{}, nil
		},
	})
	if err != nil {
		log.Errorf("edgecdnx: failed to add indexer to location informer: %v", err)
	}

	locationMgr.Informer = locationInformer

	return locationMgr
}

func (locationMgr *LocationManager) locationEventHandlers() cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			l_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnx: Failed to cast object to unstructured.Unstructured")
				return
			}

			temp, err := json.Marshal(l_raw.Object)
			if err != nil {
				log.Errorf("edgecdnx: Failed to marshal location object: %v", err)
				return
			}
			location := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, location)
			if err != nil {
				log.Errorf("edgecdnx: Failed to unmarshal location object: %v", err)
				return
			}

			locationMgr.Sync.Lock()
			defer locationMgr.Sync.Unlock()
			locationMgr.setLocationLocked(*location)
			log.Infof("edgecdnx: Added Location %s", location.Name)
		},
		UpdateFunc: func(oldObj, newObj any) {
			s_new_raw, ok := newObj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnx: expected Location object, got %T", s_new_raw)
				return
			}
			temp, err := json.Marshal(s_new_raw.Object)
			if err != nil {
				log.Errorf("edgecdnx: failed to marshal Location object: %v", err)
				return
			}
			location := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, location)
			if err != nil {
				log.Errorf("edgecdnx: failed to unmarshal Location object: %v", err)
				return
			}
			locationMgr.Sync.Lock()
			defer locationMgr.Sync.Unlock()
			locationMgr.setLocationLocked(*location)
			log.Infof("edgecdnx: Updated Location %s", location.Name)
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			s_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnx: expected Location object, got %T", obj)
				return
			}

			temp, err := json.Marshal(s_raw.Object)
			if err != nil {
				log.Errorf("edgecdnx: failed to marshal Location object: %v", err)
				return
			}
			location := &infrastructurev1alpha1.Location{}
			err = json.Unmarshal(temp, location)
			if err != nil {
				log.Errorf("edgecdnx: failed to unmarshal Location object: %v", err)
				return
			}

			locationMgr.Sync.Lock()
			defer locationMgr.Sync.Unlock()
			delete(locationMgr.Locations, resourceKey(location.Namespace, location.Name))
			locationMgr.revision.Add(1)
			log.Infof("edgecdnx: Deleted Location %s", location.Name)
		},
	}
}
