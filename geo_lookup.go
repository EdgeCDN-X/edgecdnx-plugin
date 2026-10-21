package edgecdnxplugin

import (
	"context"
	"errors"
	"maps"
	"math/rand/v2"
	"sort"

	"github.com/coredns/coredns/plugin/metadata"
	"github.com/coredns/coredns/plugin/pkg/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

type endpointGeoCandidates struct {
	manager    *LocationManager
	revision   uint64
	candidates []geoCandidate
	attributes []string
}

type geoCandidate struct {
	name          string
	weight        int
	defaultWeight int
	hasDefault    bool
	attributes    []geoAttribute
}

type geoAttribute struct {
	index  int
	values map[string]int
}

func compileRouteSelector(routeSelector *metav1.LabelSelector) labels.Selector {
	if routeSelector == nil {
		return labels.Everything()
	}
	selector, err := metav1.LabelSelectorAsSelector(routeSelector)
	if err != nil {
		log.Warningf("edgecdnx: invalid LabelSelector %v: %v", routeSelector, err)
		return labels.Everything()
	}
	return selector
}

func (entry *dnsEndpointEntry) matchingGeoCandidates(manager *LocationManager) *endpointGeoCandidates {
	revision := manager.revision.Load()
	if cached := entry.geo.Load(); cached != nil && cached.manager == manager && cached.revision == revision {
		return cached
	}

	entry.targetMu.Lock()
	defer entry.targetMu.Unlock()
	manager.Sync.RLock()
	defer manager.Sync.RUnlock()

	revision = manager.revision.Load()
	if cached := entry.geo.Load(); cached != nil && cached.manager == manager && cached.revision == revision {
		return cached
	}
	candidates := manager.findGeoCandidatesLocked(entry.Spec.RouteSelector, scopeFor(entry.Namespace, entry.Spec.Project))
	entry.geo.Store(candidates)
	return candidates
}

func (l *LocationManager) findGeoCandidatesLocked(routeSelector *metav1.LabelSelector, scopes ...routingScope) *endpointGeoCandidates {
	result := &endpointGeoCandidates{manager: l, revision: l.revision.Load()}
	selector := compileRouteSelector(routeSelector)
	attributeIndices := make(map[string]int)
	for locationName, location := range l.Locations {
		if !matchesRoutingScope(location.Namespace, location.Spec.Project, scopes) {
			continue
		}
		matches := false
		for _, ng := range location.Spec.NodeGroups {
			fullLabels := make(map[string]string, len(location.Labels)+len(ng.Labels))
			maps.Copy(fullLabels, location.Labels)
			maps.Copy(fullLabels, ng.Labels)
			if selector.Matches(labels.Set(fullLabels)) {
				matches = true
				break
			}
		}
		if !matches {
			continue
		}

		candidate := geoCandidate{name: locationName, weight: location.Spec.GeoLookup.Weight}
		for name, attribute := range location.Spec.GeoLookup.Attributes {
			if name == "default" {
				candidate.hasDefault = true
				candidate.defaultWeight = attribute.Weight
			}
			if len(attribute.Values) == 0 {
				continue
			}
			index, exists := attributeIndices[name]
			if !exists {
				index = len(result.attributes)
				attributeIndices[name] = index
				result.attributes = append(result.attributes, name)
			}
			values := make(map[string]int, len(attribute.Values))
			for _, value := range attribute.Values {
				// Duplicate values contribute once per occurrence, as in the original scoring.
				values[value.Value] += attribute.Weight + value.Weight
			}
			candidate.attributes = append(candidate.attributes, geoAttribute{index: index, values: values})
		}
		result.candidates = append(result.candidates, candidate)
	}
	sort.Slice(result.candidates, func(i, j int) bool {
		return result.candidates[i].name < result.candidates[j].name
	})
	return result
}

func (geo *endpointGeoCandidates) lookup(ctx context.Context) (string, error) {
	values := make([]string, len(geo.attributes))
	for i, name := range geo.attributes {
		if valueFunc := metadata.ValueFunc(ctx, name); valueFunc != nil {
			values[i] = valueFunc()
		}
	}

	maxScore := 0
	winners := make([]int, 0, len(geo.candidates))
	for i, candidate := range geo.candidates {
		score, matched := candidate.defaultWeight, candidate.hasDefault
		for _, attribute := range candidate.attributes {
			value := values[attribute.index]
			if value == "" {
				continue
			}
			if contribution, exists := attribute.values[value]; exists {
				score += contribution
				matched = true
			}
		}
		if !matched {
			continue
		}
		// Compare complete totals, including negative modifiers, not intermediate scores.
		if len(winners) == 0 || score > maxScore {
			maxScore = score
			winners = append(winners[:0], i)
		} else if score == maxScore {
			winners = append(winners, i)
		}
	}
	if log.D.Value() {
		log.Debugf("edgecdnx: found %d locations with geo score %d", len(winners), maxScore)
	}
	if len(winners) == 0 {
		return "", errors.New("No Location Found")
	}
	if len(winners) == 1 {
		return geo.candidates[winners[0]].name, nil
	}
	return geo.chooseWinner(winners, rand.Float64()), nil
}

func (geo *endpointGeoCandidates) chooseWinner(winners []int, random float64) string {
	totalWeight := 0
	for _, index := range winners {
		totalWeight += geo.candidates[index].weight
	}
	if totalWeight == 0 {
		return geo.candidates[winners[int(random*float64(len(winners)))]].name
	}
	threshold := random * float64(totalWeight)
	currentWeight := 0
	for _, index := range winners {
		currentWeight += geo.candidates[index].weight
		if threshold < float64(currentWeight) {
			return geo.candidates[index].name
		}
	}
	return geo.candidates[winners[len(winners)-1]].name
}
