package edgecdnxplugin

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/miekg/dns"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

func testLocationObject(t *testing.T, location infrastructurev1alpha1.Location) *unstructured.Unstructured {
	t.Helper()
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&location)
	if err != nil {
		t.Fatal(err)
	}
	return &unstructured.Unstructured{Object: object}
}

func TestEndpointLocationCacheInvalidation(t *testing.T) {
	manager := newTestLocationManager(t)
	handlers := manager.locationEventHandlers()
	endpoint := infrastructurev1alpha1.DNSEndpoint{
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName: "example.com", RecordType: "A", RoutingPolicy: "Weighted",
			RouteSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "acme"}},
		},
	}
	dm := newTestDNSEndpointManager(endpoint)
	entry, err := dm.getDNSEndpoint("example.com", dns.TypeA)
	if err != nil {
		t.Fatal(err)
	}
	assertTargets := func(want ...weightedTarget) *endpointLocationTargets {
		t.Helper()
		got := entry.matchingLocationTargets(&manager)
		if !slices.Equal(got, want) {
			t.Fatalf("targets = %v, want %v", got, want)
		}
		cached := entry.targets.Load()
		entry.matchingLocationTargets(&manager)
		if entry.targets.Load() != cached {
			t.Fatal("warm lookup rebuilt the cache")
		}
		return cached
	}
	empty := assertTargets()
	location := newTestLocation("loc-b", map[string]string{"tenant": "acme"}, "", "node")
	location.Spec.Weight = 3
	object := testLocationObject(t, location)
	handlers.AddFunc(object)
	initial := assertTargets(weightedTarget{Name: "loc-b", Weight: 3})
	if initial == empty {
		t.Fatal("add did not invalidate empty cache")
	}

	statusUpdate := location.DeepCopy()
	statusUpdate.Status.Alerts = []infrastructurev1alpha1.PrometheusAlertStatus{{AlertName: "unhealthy"}}
	statusObject := testLocationObject(t, *statusUpdate)
	handlers.UpdateFunc(object, statusObject)
	if assertTargets(weightedTarget{Name: "loc-b", Weight: 3}) != initial {
		t.Fatal("status-only update invalidated cache")
	}
	current, err := manager.GetLocationByName(location.Name)
	if err != nil || len(current.Status.Alerts) != 1 {
		t.Fatalf("current health status was not published: %v", err)
	}

	weightUpdate := statusUpdate.DeepCopy()
	weightUpdate.Spec.Weight = 7
	weightObject := testLocationObject(t, *weightUpdate)
	handlers.UpdateFunc(statusObject, weightObject)
	weighted := assertTargets(weightedTarget{Name: "loc-b", Weight: 7})
	if weighted == initial || initial.targets[0].Weight != 3 {
		t.Fatal("weight change did not publish a new immutable cache")
	}
	specUpdate := weightUpdate.DeepCopy()
	specUpdate.Spec.MaintenanceMode = true
	specObject := testLocationObject(t, *specUpdate)
	handlers.UpdateFunc(weightObject, specObject)
	if assertTargets(weightedTarget{Name: "loc-b", Weight: 7}) == weighted {
		t.Fatal("spec change did not invalidate cache")
	}

	labelUpdate := specUpdate.DeepCopy()
	labelUpdate.Labels["tenant"] = "globex"
	labelObject := testLocationObject(t, *labelUpdate)
	handlers.UpdateFunc(specObject, labelObject)
	assertTargets()

	updatedEndpoint := endpoint.DeepCopy()
	updatedEndpoint.Spec.RouteSelector.MatchLabels["tenant"] = "globex"
	dm.Sync.Lock()
	dm.publishEndpoint(dnsEndpointKey("example.com", "A"), updatedEndpoint)
	dm.Sync.Unlock()
	previousEntry := entry
	entry, err = dm.getDNSEndpoint("example.com", dns.TypeA)
	if err != nil {
		t.Fatal(err)
	}
	if entry == previousEntry || entry.targets.Load() != nil {
		t.Fatal("endpoint update did not create a fresh cache")
	}
	assertTargets(weightedTarget{Name: "loc-b", Weight: 7})
	if len(previousEntry.matchingLocationTargets(&manager)) != 0 {
		t.Fatal("old endpoint entry no longer uses its original selector")
	}

	other := newTestLocation("loc-a", map[string]string{"tenant": "globex"}, "", "")
	handlers.AddFunc(testLocationObject(t, other))
	assertTargets(weightedTarget{Name: "loc-a"}, weightedTarget{Name: "loc-b", Weight: 7})
	handlers.DeleteFunc(cache.DeletedFinalStateUnknown{Obj: labelObject})
	assertTargets(weightedTarget{Name: "loc-a"})
	handlers.DeleteFunc(testLocationObject(t, other))
	assertTargets()
}

func TestEndpointLocationCacheSelectors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		selector *metav1.LabelSelector
		want     []weightedTarget
	}{
		{name: "nil", want: []weightedTarget{{Name: "a"}, {Name: "b"}}},
		{name: "empty", selector: &metav1.LabelSelector{}, want: []weightedTarget{{Name: "a"}, {Name: "b"}}},
		{name: "expression", selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "tenant", Operator: metav1.LabelSelectorOpIn, Values: []string{"acme"}},
		}}, want: []weightedTarget{{Name: "a"}}},
		{name: "unmatched", selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "missing"}}},
		{name: "invalid preserves existing behavior", selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "tenant", Operator: "invalid"},
		}}, want: []weightedTarget{{Name: "a"}, {Name: "b"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newTestLocationManager(t)
			manager.Locations["b"] = newTestLocation("b", nil, "", "")
			manager.Locations["a"] = newTestLocation("a", map[string]string{"tenant": "acme"}, "", "")
			entry := &dnsEndpointEntry{DNSEndpoint: &infrastructurev1alpha1.DNSEndpoint{
				Spec: infrastructurev1alpha1.DNSEndpointSpec{RouteSelector: tc.selector},
			}}
			if got := entry.matchingLocationTargets(&manager); !slices.Equal(got, tc.want) {
				t.Fatalf("targets = %v, want %v", got, tc.want)
			}
			other := newTestLocationManager(t)
			if got := entry.matchingLocationTargets(&other); len(got) != 0 {
				t.Fatalf("reused cache across location managers: %v", got)
			}
		})
	}
}

func TestEndpointLocationCacheConcurrentUpdates(t *testing.T) {
	manager := newTestLocationManager(t)
	location := newTestLocation("location", nil, "", "")
	manager.setLocationLocked(location)
	entry := &dnsEndpointEntry{DNSEndpoint: &infrastructurev1alpha1.DNSEndpoint{}}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for j := 0; j < 1000; j++ {
				got := entry.matchingLocationTargets(&manager)
				if len(got) != 1 || got[0].Name != "location" {
					t.Errorf("inconsistent targets: %v", got)
					return
				}
			}
		})
	}
	for i := 0; i < 1000; i++ {
		manager.Sync.Lock()
		location.Spec.Weight = int32(i)
		manager.setLocationLocked(location)
		manager.Sync.Unlock()
	}
	wg.Wait()
	if got := entry.matchingLocationTargets(&manager); got[0].Weight != 999 {
		t.Fatalf("final weight = %d, want 999", got[0].Weight)
	}
}

func BenchmarkEndpointLocationTargets(b *testing.B) {
	for _, count := range []int{100, 1000} {
		manager := &LocationManager{Sync: &sync.RWMutex{}, Locations: make(map[string]infrastructurev1alpha1.Location)}
		for i := 0; i < count; i++ {
			name := fmt.Sprintf("location-%04d", i)
			manager.Locations[name] = newTestLocation(name, map[string]string{"tenant": "acme"}, "", "")
		}
		entry := &dnsEndpointEntry{DNSEndpoint: &infrastructurev1alpha1.DNSEndpoint{
			Spec: infrastructurev1alpha1.DNSEndpointSpec{
				RouteSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "acme"}},
			},
		}}
		entry.matchingLocationTargets(manager)
		b.Run(fmt.Sprintf("cached/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if len(entry.matchingLocationTargets(manager)) != count {
					b.Fatal("incorrect target count")
				}
			}
		})
		b.Run(fmt.Sprintf("uncached/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if len(manager.FindMatchingTargetsWithWeights(context.Background(), entry.Spec.RouteSelector, "")) != count {
					b.Fatal("incorrect target count")
				}
			}
		})
	}
}
