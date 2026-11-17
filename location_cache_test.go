package edgecdnxplugin

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/miekg/dns"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

func TestLocationCacheSeparatesNamespaces(t *testing.T) {
	manager := newTestLocationManager(t)
	handlers := manager.locationEventHandlers()
	first := newTestLocation("edge", nil, "", "first-node")
	first.Namespace = "tenant-a"
	second := newTestLocation("edge", nil, "", "second-node")
	second.Namespace = "tenant-b"
	handlers.AddFunc(testLocationObject(t, first))
	handlers.AddFunc(testLocationObject(t, second))
	for _, location := range []infrastructurev1alpha1.Location{first, second} {
		got, err := manager.GetLocationByName(resourceKey(location.Namespace, location.Name))
		if err != nil || got.Namespace != location.Namespace {
			t.Fatalf("namespace %s was overwritten: %+v, %v", location.Namespace, got, err)
		}
	}
	handlers.DeleteFunc(testLocationObject(t, first))
	if _, err := manager.GetLocationByName(resourceKey(second.Namespace, second.Name)); err != nil {
		t.Fatal("deleting one tenant's Location removed another tenant's Location")
	}
}

func TestEndpointLocationCandidatesStayWithinTenant(t *testing.T) {
	manager := newTestLocationManager(t)
	for _, tenant := range []struct{ namespace, project, name string }{
		{"tenant-a", "one", "edge"}, {"tenant-b", "one", "edge"}, {"tenant-a", "two", "foreign"},
	} {
		location := newTestLocation(tenant.name, nil, "", "node")
		location.Namespace = tenant.namespace
		location.Spec.Project = &corev1.LocalObjectReference{Name: tenant.project}
		manager.locationEventHandlers().AddFunc(testLocationObject(t, location))
	}
	endpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "tenant-a"},
		Spec:       infrastructurev1alpha1.DNSEndpointSpec{Project: &corev1.LocalObjectReference{Name: "one"}, DNSName: "example.com", RecordType: "A"},
	}
	entry, err := newTestDNSEndpointManager(endpoint).getDNSEndpoint("example.com", dns.TypeA)
	if err != nil {
		t.Fatal(err)
	}
	targets := entry.matchingLocationTargets(&manager)
	if len(targets) != 1 || targets[0].Name != "tenant-a/edge" {
		t.Fatalf("weighted/round-robin targets crossed tenants: %v", targets)
	}
	geo := entry.matchingGeoCandidates(&manager)
	if len(geo.candidates) != 1 || geo.candidates[0].name != "tenant-a/edge" {
		t.Fatalf("geolocation candidates crossed tenants: %v", geo.candidates)
	}
	scope := scopeFor(endpoint.Namespace, endpoint.Spec.Project)
	for _, name := range []string{"foreign", "tenant-b/edge"} {
		if _, err := manager.GetLocationByName(name, scope); err == nil {
			t.Fatalf("cross-tenant reference %q was accepted", name)
		}
	}
}

func TestEndpointLocationCandidatesHonorSharingAndRevocation(t *testing.T) {
	manager := newTestLocationManager(t)
	location := newTestLocation("edge", map[string]string{"role": "edge"}, "", "node")
	location.Namespace = "tenant-b"
	location.Spec.Project = &corev1.LocalObjectReference{Name: "platform"}
	location.Spec.Sharing = &infrastructurev1alpha1.ResourceSharing{
		Projects: []infrastructurev1alpha1.ProjectReference{{Namespace: "tenant-a", Name: "frontend"}},
	}
	manager.locationEventHandlers().AddFunc(testLocationObject(t, location))
	endpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "tenant-a"},
		Spec:       infrastructurev1alpha1.DNSEndpointSpec{Project: &corev1.LocalObjectReference{Name: "frontend"}, DNSName: "example.com", RecordType: "A"},
	}
	entry, err := newTestDNSEndpointManager(endpoint).getDNSEndpoint("example.com", dns.TypeA)
	if err != nil {
		t.Fatal(err)
	}
	assertAvailable := func(want bool) {
		t.Helper()
		if got := len(entry.matchingLocationTargets(&manager)) == 1; got != want {
			t.Fatalf("weighted/round-robin availability = %t, want %t", got, want)
		}
		if got := len(entry.matchingGeoCandidates(&manager).candidates) == 1; got != want {
			t.Fatalf("geolocation availability = %t, want %t", got, want)
		}
		_, err := manager.GetLocationByName("edge", scopeFor(endpoint.Namespace, endpoint.Spec.Project))
		if (err == nil) != want {
			t.Fatalf("name lookup error = %v, want available %t", err, want)
		}
	}
	assertAvailable(true)

	location.Spec.Sharing = nil
	manager.locationEventHandlers().UpdateFunc(testLocationObject(t, *location.DeepCopy()), testLocationObject(t, location))
	assertAvailable(false)
}

func TestEndpointLocationSharingAllProjectsAndSameNameIsolation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		sharing     *infrastructurev1alpha1.ResourceSharing
		namespace   string
		project     string
		wantAllowed bool
	}{
		{
			name:        "all projects",
			sharing:     &infrastructurev1alpha1.ResourceSharing{AllProjects: true},
			namespace:   "tenant-b",
			project:     "platform",
			wantAllowed: true,
		},
		{
			name:        "project reference defaults to target namespace",
			sharing:     &infrastructurev1alpha1.ResourceSharing{Projects: []infrastructurev1alpha1.ProjectReference{{Name: "frontend"}}},
			namespace:   "tenant-a",
			project:     "platform",
			wantAllowed: true,
		},
		{
			name:        "same project name in another namespace is not the source project",
			sharing:     &infrastructurev1alpha1.ResourceSharing{Projects: []infrastructurev1alpha1.ProjectReference{{Name: "frontend"}}},
			namespace:   "tenant-b",
			project:     "platform",
			wantAllowed: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newTestLocationManager(t)
			location := newTestLocation("edge", nil, "", "node")
			location.Namespace = tc.namespace
			location.Spec.Project = &corev1.LocalObjectReference{Name: tc.project}
			location.Spec.Sharing = tc.sharing
			manager.locationEventHandlers().AddFunc(testLocationObject(t, location))
			scope := routingScope{namespace: "tenant-a", project: "frontend"}
			_, err := manager.GetLocationByName("edge", scope)
			if (err == nil) != tc.wantAllowed {
				t.Fatalf("shared lookup error = %v, want available %t", err, tc.wantAllowed)
			}
		})
	}
}

func TestSharedLocationNameCollisionDoesNotBypassLocalScope(t *testing.T) {
	manager := newTestLocationManager(t)
	local := newTestLocation("edge", nil, "", "local")
	local.Namespace = "tenant-a"
	local.Spec.Project = &corev1.LocalObjectReference{Name: "other"}
	remote := newTestLocation("edge", nil, "", "remote")
	remote.Namespace = "tenant-b"
	remote.Spec.Project = &corev1.LocalObjectReference{Name: "platform"}
	remote.Spec.Sharing = &infrastructurev1alpha1.ResourceSharing{AllProjects: true}
	manager.locationEventHandlers().AddFunc(testLocationObject(t, local))
	manager.locationEventHandlers().AddFunc(testLocationObject(t, remote))

	if _, err := manager.GetLocationByName("edge", routingScope{namespace: "tenant-a", project: "frontend"}); err == nil {
		t.Fatal("a foreign local Location name must not resolve to a shared cross-namespace collision")
	}
}

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
