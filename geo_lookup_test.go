package edgecdnxplugin

import (
	"context"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin/metadata"
	"github.com/miekg/dns"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func newTestGeoLocation(name string, defaultWeight int) infrastructurev1alpha1.Location {
	location := newTestLocation(name, nil, "", "node")
	location.Spec.GeoLookup = infrastructurev1alpha1.GeoLookupSpec{
		Weight: 1,
		Attributes: map[string]infrastructurev1alpha1.GeoLookupAttributeSpec{
			"default": {Weight: defaultWeight},
		},
	}
	return location
}

func geoCandidateNames(geo *endpointGeoCandidates) []string {
	names := make([]string, 0, len(geo.candidates))
	for _, candidate := range geo.candidates {
		names = append(names, candidate.name)
	}
	return names
}

func TestGeoCandidateSelectors(t *testing.T) {
	location := newTestGeoLocation("location", 1)
	location.Labels = map[string]string{"tenant": "acme", "role": "ingress"}
	location.Spec.NodeGroups[0].Labels = map[string]string{"role": "edge"}
	noGroups := newTestGeoLocation("no-groups", 10)
	noGroups.Spec.NodeGroups = nil
	for _, tc := range []struct {
		name     string
		selector *metav1.LabelSelector
		matches  bool
	}{
		{name: "nil", matches: true},
		{name: "empty", selector: &metav1.LabelSelector{}, matches: true},
		{name: "combined", selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "acme", "role": "edge"}}, matches: true},
		{name: "node group overrides location", selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "ingress"}}},
		{name: "unmatched", selector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "globex"}}},
		{name: "expression", selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "role", Operator: metav1.LabelSelectorOpIn, Values: []string{"edge"}},
		}}, matches: true},
		{name: "invalid retains existing behavior", selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "role", Operator: "invalid"},
		}}, matches: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newTestLocationManager(t)
			manager.Locations[location.Name] = location
			manager.Locations[noGroups.Name] = noGroups
			entry := &dnsEndpointEntry{DNSEndpoint: &infrastructurev1alpha1.DNSEndpoint{
				Spec: infrastructurev1alpha1.DNSEndpointSpec{RouteSelector: tc.selector},
			}}
			var want []string
			if tc.matches {
				want = []string{"location"}
			}
			got := geoCandidateNames(entry.matchingGeoCandidates(&manager))
			if !slices.Equal(got, want) {
				t.Fatalf("candidates = %v, want %v", got, want)
			}
		})
	}
}

func TestGeoCandidateCacheInvalidation(t *testing.T) {
	manager := newTestLocationManager(t)
	handlers := manager.locationEventHandlers()
	endpoint := infrastructurev1alpha1.DNSEndpoint{Spec: infrastructurev1alpha1.DNSEndpointSpec{
		DNSName: "example.com", RecordType: "A", RoutingPolicy: "Geolocation",
		RouteSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "edge"}},
	}}
	dm := newTestDNSEndpointManager(endpoint)
	entry, err := dm.getDNSEndpoint("example.com", dns.TypeA)
	if err != nil {
		t.Fatal(err)
	}
	assertNames := func(want ...string) *endpointGeoCandidates {
		t.Helper()
		cached := entry.matchingGeoCandidates(&manager)
		if got := geoCandidateNames(cached); !slices.Equal(got, want) {
			t.Fatalf("candidates = %v, want %v", got, want)
		}
		if entry.matchingGeoCandidates(&manager) != cached {
			t.Fatal("warm lookup rebuilt candidates")
		}
		return cached
	}
	empty := assertNames()
	location := newTestGeoLocation("location", 1)
	location.Spec.NodeGroups[0].Labels = map[string]string{"role": "edge"}
	object := testLocationObject(t, location)
	handlers.AddFunc(object)
	initial := assertNames("location")
	if initial == empty {
		t.Fatal("add did not invalidate empty cache")
	}
	update := func(location infrastructurev1alpha1.Location) {
		t.Helper()
		newObject := testLocationObject(t, location)
		handlers.UpdateFunc(object, newObject)
		object = newObject
	}

	location.Status.Alerts = []infrastructurev1alpha1.PrometheusAlertStatus{{AlertName: "unhealthy"}}
	update(location)
	if assertNames("location") != initial {
		t.Fatal("status update invalidated geo cache")
	}
	current, err := manager.GetLocationByName("location")
	if err != nil || len(current.Status.Alerts) != 1 {
		t.Fatal("status update was not published")
	}

	location.Spec.GeoLookup.Weight = 7
	location.Spec.GeoLookup.Attributes["default"] = infrastructurev1alpha1.GeoLookupAttributeSpec{Weight: 20}
	location.Spec.GeoLookup.Attributes["geo/continent"] = infrastructurev1alpha1.GeoLookupAttributeSpec{
		Weight: 10, Values: []infrastructurev1alpha1.GeoLookupAttributeValuesSpec{{Value: "EU", Weight: 5}},
	}
	update(location)
	updated := assertNames("location")
	if updated == initial || updated.candidates[0].weight != 7 || updated.candidates[0].defaultWeight != 20 {
		t.Fatal("GeoLookup spec update did not refresh scoring data")
	}
	if initial.candidates[0].weight != 1 || initial.candidates[0].defaultWeight != 1 {
		t.Fatal("old snapshot was mutated")
	}
	if len(updated.attributes) != 1 || updated.attributes[0] != "geo/continent" ||
		updated.candidates[0].attributes[0].values["EU"] != 15 {
		t.Fatal("GeoLookup attribute update was not compiled")
	}
	location.Labels = map[string]string{"tenant": "acme"}
	update(location)
	if assertNames("location") == updated {
		t.Fatal("location label update did not invalidate")
	}
	location.Spec.NodeGroups[0].Labels["role"] = "ingress"
	update(location)
	assertNames()
	location.Spec.NodeGroups = append(location.Spec.NodeGroups, infrastructurev1alpha1.NodeGroupSpec{
		Name: "edge", Labels: map[string]string{"role": "edge"},
	})
	update(location)
	assertNames("location")
	location.Spec.NodeGroups = location.Spec.NodeGroups[:1]
	update(location)
	assertNames()

	updatedEndpoint := endpoint.DeepCopy()
	updatedEndpoint.Spec.RouteSelector.MatchLabels["role"] = "ingress"
	dm.Sync.Lock()
	dm.publishEndpoint(dnsEndpointKey("example.com", "A"), updatedEndpoint)
	dm.Sync.Unlock()
	oldEntry := entry
	entry, err = dm.getDNSEndpoint("example.com", dns.TypeA)
	if err != nil {
		t.Fatal(err)
	}
	if entry.geo.Load() != nil {
		t.Fatal("endpoint update retained old geo cache")
	}
	assertNames("location")
	if len(oldEntry.matchingGeoCandidates(&manager).candidates) != 0 {
		t.Fatal("old entry lost its original selector")
	}
	handlers.DeleteFunc(cache.DeletedFinalStateUnknown{Obj: object})
	assertNames()
	other := newTestLocationManager(t)
	otherLocation := newTestGeoLocation("other", 1)
	otherLocation.Spec.NodeGroups[0].Labels = map[string]string{"role": "ingress"}
	other.Locations["other"] = otherLocation
	if got := geoCandidateNames(entry.matchingGeoCandidates(&other)); !slices.Equal(got, []string{"other"}) {
		t.Fatalf("cache reused across managers: %v", got)
	}
}

func TestGeoLookupScores(t *testing.T) {
	type attribute = infrastructurev1alpha1.GeoLookupAttributeSpec
	type value = infrastructurev1alpha1.GeoLookupAttributeValuesSpec
	for _, tc := range []struct {
		name       string
		attributes []map[string]attribute
		metadata   map[string]string
		want       string
	}{
		{name: "no candidates"},
		{name: "no scoring attributes", attributes: []map[string]attribute{{}}},
		{name: "default without metadata", attributes: []map[string]attribute{{"default": {Weight: 1}}}, want: "loc-0"},
		{name: "missing metadata is not a zero score", attributes: []map[string]attribute{
			{"geo/continent": {Weight: 10, Values: []value{{Value: "EU"}}}},
		}},
		{name: "empty metadata cannot match", attributes: []map[string]attribute{
			{"geo/continent": {Values: []value{{Value: "", Weight: 100}}}},
		}, metadata: map[string]string{"geo/continent": ""}},
		{name: "unmatched metadata", attributes: []map[string]attribute{
			{"geo/continent": {Values: []value{{Value: "EU", Weight: 100}}}},
		}, metadata: map[string]string{"geo/continent": "NA"}},
		{name: "value weight participates in maximum", attributes: []map[string]attribute{
			{"geo/continent": {Weight: 1, Values: []value{{Value: "EU", Weight: 99}}}},
			{"default": {Weight: 50}},
		}, metadata: map[string]string{"geo/continent": "EU"}, want: "loc-0"},
		{name: "final totals include negative modifiers", attributes: []map[string]attribute{
			{"default": {Weight: 100}, "geo/continent": {Values: []value{{Value: "EU", Weight: -90}}}},
			{"default": {Weight: 20}},
		}, metadata: map[string]string{"geo/continent": "EU"}, want: "loc-1"},
		{name: "all negative scores", attributes: []map[string]attribute{
			{"default": {Weight: -2}}, {"default": {Weight: -1}},
		}, want: "loc-1"},
		{name: "duplicate values accumulate", attributes: []map[string]attribute{
			{"geo/continent": {Weight: 10, Values: []value{{Value: "EU", Weight: 1}, {Value: "EU", Weight: 2}}}},
			{"default": {Weight: 22}},
		}, metadata: map[string]string{"geo/continent": "EU"}, want: "loc-0"},
		{name: "attributes accumulate", attributes: []map[string]attribute{
			{"default": {Weight: 1}, "geo/continent": {Weight: 10, Values: []value{{Value: "EU"}}},
				"geo/country": {Weight: 10, Values: []value{{Value: "DE"}}}},
			{"default": {Weight: 20}},
		}, metadata: map[string]string{"geo/continent": "EU", "geo/country": "DE"}, want: "loc-0"},
		{name: "default can also match metadata", attributes: []map[string]attribute{
			{"default": {Weight: 1, Values: []value{{Value: "extra", Weight: 10}}}},
			{"default": {Weight: 11}},
		}, metadata: map[string]string{"default": "extra"}, want: "loc-0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newTestLocationManager(t)
			for i, attrs := range tc.attributes {
				location := newTestGeoLocation(fmt.Sprintf("loc-%d", i), 0)
				location.Spec.GeoLookup.Attributes = attrs
				manager.Locations[location.Name] = location
			}
			entry := &dnsEndpointEntry{DNSEndpoint: &infrastructurev1alpha1.DNSEndpoint{}}
			geo := entry.matchingGeoCandidates(&manager)
			ctx := metadata.ContextWithMetadata(context.Background())
			calls := make(map[string]int)
			for name, val := range tc.metadata {
				if !metadata.SetValueFunc(ctx, name, func() string {
					calls[name]++
					return val
				}) {
					t.Fatal("metadata context was not initialized")
				}
			}
			for i := 0; i < 10; i++ {
				got, err := geo.lookup(ctx)
				if got != tc.want || (err != nil) != (tc.want == "") {
					t.Fatalf("lookup = %q, %v; want %q", got, err, tc.want)
				}
			}
			for name, count := range calls {
				if count != 10 {
					t.Fatalf("metadata %s called %d times, want once per request", name, count)
				}
			}
			got, err := manager.PerformGeoLookup(ctx, nil, "")
			if got != tc.want || (err != nil) != (tc.want == "") {
				t.Fatalf("uncached lookup = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestGeoLookupMetadataRemainsRequestSpecific(t *testing.T) {
	manager := newTestLocationManager(t)
	for _, continent := range []string{"EU", "NA"} {
		location := newTestGeoLocation(continent, 0)
		location.Spec.GeoLookup.Attributes = map[string]infrastructurev1alpha1.GeoLookupAttributeSpec{
			"geo/continent": {Weight: 10, Values: []infrastructurev1alpha1.GeoLookupAttributeValuesSpec{{Value: continent}}},
		}
		manager.Locations[location.Name] = location
	}
	entry := &dnsEndpointEntry{DNSEndpoint: &infrastructurev1alpha1.DNSEndpoint{}}
	snapshot := entry.matchingGeoCandidates(&manager)
	for _, continent := range []string{"EU", "NA", "EU"} {
		ctx := metadata.ContextWithMetadata(context.Background())
		calls := 0
		metadata.SetValueFunc(ctx, "geo/continent", func() string {
			calls++
			return continent
		})
		got, err := entry.matchingGeoCandidates(&manager).lookup(ctx)
		if err != nil || got != continent || calls != 1 {
			t.Fatalf("lookup = %q, %v, calls = %d; want %q, nil, 1", got, err, calls, continent)
		}
		if entry.geo.Load() != snapshot {
			t.Fatal("request metadata rebuilt candidates")
		}
	}
}

func TestGeoLookupWeightedTieBoundaries(t *testing.T) {
	geo := &endpointGeoCandidates{candidates: []geoCandidate{
		{name: "zero"}, {name: "one", weight: 1}, {name: "three", weight: 3},
	}}
	winners := []int{0, 1, 2}
	for _, tc := range []struct {
		random float64
		want   string
	}{
		{0, "one"}, {0.249999, "one"}, {0.25, "three"}, {0.999999, "three"},
	} {
		if got := geo.chooseWinner(winners, tc.random); got != tc.want {
			t.Fatalf("random %v selected %s, want %s", tc.random, got, tc.want)
		}
	}
	geo.candidates[1].weight, geo.candidates[2].weight = 0, 0
	for i, random := range []float64{0, 0.5, 0.999999} {
		if got := geo.chooseWinner(winners, random); got != geo.candidates[i].name {
			t.Fatalf("all-zero tie selected %s, want %s", got, geo.candidates[i].name)
		}
	}
}

func TestGeoCandidateCacheConcurrentUpdates(t *testing.T) {
	manager := newTestLocationManager(t)
	location := newTestGeoLocation("location", 1)
	manager.setLocationLocked(location)
	entry := &dnsEndpointEntry{DNSEndpoint: &infrastructurev1alpha1.DNSEndpoint{}}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			for j := 0; j < 1000; j++ {
				geo := entry.matchingGeoCandidates(&manager)
				if got, err := geo.lookup(context.Background()); err != nil || got != "location" {
					t.Errorf("lookup = %q, %v", got, err)
					return
				}
			}
		})
	}
	for i := 0; i < 1000; i++ {
		updated := location.DeepCopy()
		updated.Spec.GeoLookup.Attributes["default"] = infrastructurev1alpha1.GeoLookupAttributeSpec{Weight: i}
		manager.Sync.Lock()
		manager.setLocationLocked(*updated)
		manager.Sync.Unlock()
	}
	wg.Wait()
	if got := entry.matchingGeoCandidates(&manager).candidates[0].defaultWeight; got != 999 {
		t.Fatalf("final weight = %d, want 999", got)
	}
}

func TestServeDNSGeoLookupTargetsOnlyMatchingNodeGroups(t *testing.T) {
	endpoint := infrastructurev1alpha1.DNSEndpoint{Spec: infrastructurev1alpha1.DNSEndpointSpec{
		DNSName: "example.com", RecordType: "A", RoutingPolicy: "Geolocation", RecordTTL: 45,
		RouteSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "acme", "role": "edge"}},
	}}
	dm := newTestDNSEndpointManager(endpoint)
	location := newTestGeoLocation("location", 1)
	location.Labels = map[string]string{"tenant": "acme"}
	location.Spec.NodeGroups[0].Labels = map[string]string{"role": "edge"}
	location.Spec.NodeGroups = append(location.Spec.NodeGroups, infrastructurev1alpha1.NodeGroupSpec{
		Name: "ingress", Labels: map[string]string{"role": "ingress"},
		Nodes: []infrastructurev1alpha1.NodeSpec{{Name: "ingress-node", Ipv4: "192.0.2.2"}},
	})
	excluded := newTestGeoLocation("excluded", 1000)
	excluded.Labels = map[string]string{"tenant": "acme"}
	excluded.Spec.NodeGroups[0].Labels = map[string]string{"role": "ingress"}
	excluded.Spec.NodeGroups[0].Nodes[0].Ipv4 = "192.0.2.3"
	manager := newTestLocationManager(t)
	manager.Locations[location.Name], manager.Locations[excluded.Name] = location, excluded
	prefixManager := newTestPrefixListRoutingManager()
	plugin := EdgeCDNX{DNSEndpointManager: dm, LocationManager: &manager,
		PrefixListRoutingManager: &prefixManager}
	assertAnswer := func(want string) {
		t.Helper()
		for i := 0; i < 10; i++ {
			request := new(dns.Msg)
			request.SetQuestion("example.com.", dns.TypeA)
			writer := &captureResponseWriter{testResponseWriter: testResponseWriter{
				remoteAddr: &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 5353},
			}}
			rcode, err := plugin.ServeDNS(context.Background(), writer, request)
			if err != nil || rcode != dns.RcodeSuccess || writer.msg == nil || len(writer.msg.Answer) != 1 {
				t.Fatalf("ServeDNS = %d, %v, response = %v", rcode, err, writer.msg)
			}
			answer, ok := writer.msg.Answer[0].(*dns.A)
			if !ok || answer.A.String() != want || answer.Hdr.Ttl != 45 {
				t.Fatalf("answer = %v, want A %s with TTL 45", writer.msg.Answer[0], want)
			}
		}
	}
	assertAnswer("192.0.2.1")
	entry, err := dm.getDNSEndpoint("example.com", dns.TypeA)
	if err != nil || entry.geo.Load() == nil {
		t.Fatal("ServeDNS did not populate the geo candidate cache")
	}
	updatedEndpoint := endpoint.DeepCopy()
	updatedEndpoint.Spec.RouteSelector.MatchLabels["role"] = "ingress"
	dm.endpointEventHandlers().UpdateFunc(testDNSEndpointObject(t, endpoint), testDNSEndpointObject(t, *updatedEndpoint))
	assertAnswer("192.0.2.3")

	updatedLocation := excluded.DeepCopy()
	updatedLocation.Spec.GeoLookup.Attributes["default"] = infrastructurev1alpha1.GeoLookupAttributeSpec{Weight: 0}
	manager.locationEventHandlers().UpdateFunc(testLocationObject(t, excluded), testLocationObject(t, *updatedLocation))
	assertAnswer("192.0.2.2")
}

func BenchmarkGeoLookup(b *testing.B) {
	for _, count := range []int{100, 1000} {
		manager := &LocationManager{Sync: &sync.RWMutex{}, Locations: make(map[string]infrastructurev1alpha1.Location)}
		for i := 0; i < count; i++ {
			location := newTestGeoLocation(fmt.Sprintf("location-%04d", i), 1)
			location.Spec.NodeGroups[0].Labels = map[string]string{"role": "edge"}
			location.Spec.GeoLookup.Attributes["geo/continent"] = infrastructurev1alpha1.GeoLookupAttributeSpec{
				Weight: 10, Values: []infrastructurev1alpha1.GeoLookupAttributeValuesSpec{{Value: "EU", Weight: i}},
			}
			for j := 0; j < 3; j++ {
				location.Spec.NodeGroups = append([]infrastructurev1alpha1.NodeGroupSpec{
					{Name: fmt.Sprintf("other-%d", j), Labels: map[string]string{"role": "ingress"}},
				}, location.Spec.NodeGroups...)
			}
			manager.Locations[location.Name] = location
		}
		entry := &dnsEndpointEntry{DNSEndpoint: &infrastructurev1alpha1.DNSEndpoint{
			Spec: infrastructurev1alpha1.DNSEndpointSpec{RouteSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"role": "edge"},
			}},
		}}
		entry.matchingGeoCandidates(manager)
		ctx := metadata.ContextWithMetadata(context.Background())
		metadata.SetValueFunc(ctx, "geo/continent", func() string { return "EU" })
		b.Run(fmt.Sprintf("candidate-cache/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if len(entry.matchingGeoCandidates(manager).candidates) != count {
					b.Fatal("wrong candidate count")
				}
			}
		})
		b.Run(fmt.Sprintf("cached-scoring/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := entry.matchingGeoCandidates(manager).lookup(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("uncached-build-and-scoring/%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := manager.PerformGeoLookup(ctx, entry.Spec.RouteSelector, ""); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
