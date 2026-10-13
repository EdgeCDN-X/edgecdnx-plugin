package edgecdnxplugin

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/miekg/dns"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

func newTestDNSEndpointManager(endpoints ...infrastructurev1alpha1.DNSEndpoint) *DNSEndpointManager {
	manager := &DNSEndpointManager{Sync: &sync.RWMutex{}}
	manager.Sync.Lock()
	defer manager.Sync.Unlock()
	for _, endpoint := range endpoints {
		manager.publishEndpoint(endpointKey{}, &endpoint)
	}
	return manager
}

func testDNSEndpointObject(t *testing.T, endpoint infrastructurev1alpha1.DNSEndpoint) *unstructured.Unstructured {
	t.Helper()
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&endpoint)
	if err != nil {
		t.Fatalf("convert DNSEndpoint: %v", err)
	}
	return &unstructured.Unstructured{Object: object}
}

func TestDNSEndpointSnapshotEvents(t *testing.T) {
	manager := newTestDNSEndpointManager()
	handlers := manager.endpointEventHandlers()
	endpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName: "example.com", RecordType: "A", RoutingPolicy: "RoundRobin",
			Targets: []string{"192.0.2.1"},
		},
	}
	assertEndpoint := func(name string, qtype uint16, want string) {
		t.Helper()
		got, err := manager.GetDNSEndpoint(name, qtype)
		if err != nil {
			t.Fatalf("lookup %s: %v", name, err)
		}
		if got.Spec.Targets[0] != want {
			t.Fatalf("lookup %s target = %q, want %q", name, got.Spec.Targets[0], want)
		}
	}
	assertMissing := func(name string, qtype uint16) {
		t.Helper()
		if _, err := manager.GetDNSEndpoint(name, qtype); err == nil {
			t.Fatalf("expected missing endpoint for %s", name)
		}
	}

	assertMissing("example.com.", dns.TypeA)
	object := testDNSEndpointObject(t, endpoint)
	handlers.AddFunc(object)
	assertEndpoint("EXAMPLE.COM", dns.TypeA, "192.0.2.1")
	initial := manager.endpoints.Load()
	key := dnsEndpointKey("example.com", "A")
	counter := initial.endpoints[key].counter
	if counter == nil {
		t.Fatal("add did not initialize round-robin counter")
	}

	updated := endpoint.DeepCopy()
	updated.Spec.Targets[0] = "192.0.2.2"
	updatedObject := testDNSEndpointObject(t, *updated)
	handlers.UpdateFunc(object, updatedObject)
	assertEndpoint("example.com.", dns.TypeA, "192.0.2.2")
	if manager.endpoints.Load().endpoints[key].counter != counter {
		t.Fatal("same-key update replaced round-robin counter")
	}
	if got := initial.endpoints[key].Spec.Targets[0]; got != "192.0.2.1" {
		t.Fatalf("old snapshot was mutated: target = %q", got)
	}

	renamed := updated.DeepCopy()
	renamed.Spec.DNSName = "other.com"
	renamed.Spec.RecordType = "AAAA"
	renamed.Spec.RoutingPolicy = "Simple"
	renamedObject := testDNSEndpointObject(t, *renamed)
	handlers.UpdateFunc(updatedObject, renamedObject)
	assertMissing("example.com.", dns.TypeA)
	assertEndpoint("other.com.", dns.TypeAAAA, "192.0.2.2")
	if _, ok := manager.endpoints.Load().endpoints[key]; ok {
		t.Fatal("key change left old endpoint entry")
	}

	beforeInvalid := manager.endpoints.Load()
	handlers.UpdateFunc(renamedObject, "invalid")
	if manager.endpoints.Load() != beforeInvalid {
		t.Fatal("invalid event changed snapshot")
	}
	invalid := renamed.DeepCopy()
	invalid.Spec.RecordType = "INVALID"
	handlers.UpdateFunc(renamedObject, testDNSEndpointObject(t, *invalid))
	if manager.endpoints.Load() != beforeInvalid {
		t.Fatal("invalid record type changed snapshot")
	}
	handlers.DeleteFunc(cache.DeletedFinalStateUnknown{Obj: renamedObject})
	assertMissing("other.com.", dns.TypeAAAA)
	if len(manager.endpoints.Load().endpoints) != 0 {
		t.Fatal("delete left endpoints in snapshot")
	}
}

func TestDNSEndpointTypedKeysSeparateRecordTypes(t *testing.T) {
	manager := newTestDNSEndpointManager()
	for _, recordType := range []string{"a", "AAAA", "CNAME", "TXT", "MX", "SRV", "NS"} {
		endpoint := infrastructurev1alpha1.DNSEndpoint{
			ObjectMeta: metav1.ObjectMeta{Name: recordType},
			Spec: infrastructurev1alpha1.DNSEndpointSpec{
				DNSName: "Example.COM", RecordType: recordType,
			},
		}
		manager.endpointEventHandlers().AddFunc(testDNSEndpointObject(t, endpoint))
	}
	for _, recordType := range []string{"a", "AAAA", "CNAME", "TXT", "MX", "SRV", "NS"} {
		got, err := manager.getDNSEndpoint("EXAMPLE.COM.", dns.StringToType[strings.ToUpper(recordType)])
		if err != nil {
			t.Fatal(err)
		}
		if got.Name != recordType {
			t.Fatalf("lookup %s returned %s", recordType, got.Name)
		}
	}
	for _, qtype := range []uint16{0, 65535} {
		if _, err := manager.getDNSEndpoint("example.com.", qtype); err == nil {
			t.Fatalf("unknown query type %d matched endpoint", qtype)
		}
	}
}

func TestDNSEndpointSnapshotOwnsConfiguration(t *testing.T) {
	endpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"tenant": "original"}},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName: "example.com", RecordType: "A", Targets: []string{"192.0.2.1"},
			RouteSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "original"}},
		},
	}
	manager := newTestDNSEndpointManager(endpoint)
	endpoint.Labels["tenant"] = "changed"
	endpoint.Spec.Targets[0] = "192.0.2.2"
	endpoint.Spec.RouteSelector.MatchLabels["tenant"] = "changed"
	got, err := manager.GetDNSEndpoint("example.com.", dns.TypeA)
	if err != nil {
		t.Fatal(err)
	}
	if got.Labels["tenant"] != "original" || got.Spec.Targets[0] != "192.0.2.1" ||
		got.Spec.RouteSelector.MatchLabels["tenant"] != "original" {
		t.Fatal("snapshot shares mutable configuration with publisher")
	}
}

func TestGetDNSEndpointDoesNotAcquireManagerLock(t *testing.T) {
	manager := newTestDNSEndpointManager()
	// A nil lock makes any accidental synchronization on the lookup path panic.
	manager.Sync = nil
	if _, err := manager.GetDNSEndpoint("missing.com.", dns.TypeA); err == nil {
		t.Fatal("expected lookup miss")
	}
	manager = newTestDNSEndpointManager(infrastructurev1alpha1.DNSEndpoint{
		Spec: infrastructurev1alpha1.DNSEndpointSpec{DNSName: "example.com", RecordType: "A"},
	})
	manager.Sync = nil
	if _, err := manager.GetDNSEndpoint("example.com.", dns.TypeA); err != nil {
		t.Fatal(err)
	}
}

func TestDNSEndpointSnapshotConcurrentReadsAndWrites(t *testing.T) {
	stable := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "stable"},
		Spec:       infrastructurev1alpha1.DNSEndpointSpec{DNSName: "stable.com", RecordType: "A"},
	}
	manager := newTestDNSEndpointManager(stable)
	handlers := manager.endpointEventHandlers()
	changing := stable.DeepCopy()
	changing.Name = "changing"
	changing.Spec.DNSName = "changing.com"
	object := testDNSEndpointObject(t, *changing)
	updated := changing.DeepCopy()
	updated.Spec.RecordTTL = 60
	updatedObject := testDNSEndpointObject(t, *updated)

	var workers sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		workers.Go(func() {
			<-start
			for range 1000 {
				got, err := manager.GetDNSEndpoint("stable.com.", dns.TypeA)
				if err != nil || got.Name != "stable" {
					t.Errorf("stable endpoint lost during update: name=%q err=%v", got.Name, err)
					return
				}
				got, err = manager.GetDNSEndpoint("changing.com.", dns.TypeA)
				if err == nil && (got.Name != "changing" || (got.Spec.RecordTTL != 0 && got.Spec.RecordTTL != 60)) {
					t.Errorf("inconsistent changing endpoint: %+v", got)
					return
				}
			}
		})
	}
	for range 2 {
		workers.Go(func() {
			<-start
			for range 100 {
				handlers.AddFunc(object)
				handlers.UpdateFunc(object, updatedObject)
				handlers.DeleteFunc(object)
			}
		})
	}
	close(start)
	workers.Wait()
}

func BenchmarkDNSEndpointLookup(b *testing.B) {
	endpoint := infrastructurev1alpha1.DNSEndpoint{
		Spec: infrastructurev1alpha1.DNSEndpointSpec{DNSName: "example.com", RecordType: "A"},
	}
	manager := newTestDNSEndpointManager(endpoint)
	b.Run("Entry", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := manager.getDNSEndpoint("example.com.", dns.TypeA); err != nil {
					b.Error(err)
				}
			}
		})
	})
	b.Run("Snapshot", func(b *testing.B) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := manager.GetDNSEndpoint("example.com.", dns.TypeA); err != nil {
					b.Error(err)
				}
			}
		})
	})
	b.Run("RWMutexBaseline", func(b *testing.B) {
		endpoints := map[string]infrastructurev1alpha1.DNSEndpoint{
			"example.com./A": endpoint,
		}
		lookup := func(qname string, qtype uint16) (infrastructurev1alpha1.DNSEndpoint, error) {
			recordType := dns.TypeToString[qtype]
			log.Debugf("edgecdnx: Looking up DNSEndpoint for %s %s", qname, recordType)
			manager.Sync.RLock()
			defer manager.Sync.RUnlock()
			if endpoint, ok := endpoints[dns.Fqdn(strings.ToLower(qname))+"/"+strings.ToUpper(recordType)]; ok {
				return endpoint, nil
			}
			return infrastructurev1alpha1.DNSEndpoint{}, fmt.Errorf("DNSEndpoint not found for %s %s", qname, recordType)
		}
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := lookup("example.com.", dns.TypeA); err != nil {
					b.Error(err)
				}
			}
		})
	})
	b.Run("PreviousSnapshotBaseline", func(b *testing.B) {
		endpoints := map[string]infrastructurev1alpha1.DNSEndpoint{"example.com./A": endpoint}
		lookup := func(qname string, qtype uint16) (infrastructurev1alpha1.DNSEndpoint, error) {
			recordType := dns.TypeToString[qtype]
			log.Debugf("edgecdnx: Looking up DNSEndpoint for %s %s", qname, recordType)
			if endpoint, ok := endpoints[dns.Fqdn(strings.ToLower(qname))+"/"+strings.ToUpper(recordType)]; ok {
				return endpoint, nil
			}
			return infrastructurev1alpha1.DNSEndpoint{}, fmt.Errorf("DNSEndpoint not found for %s %s", qname, recordType)
		}
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := lookup("example.com.", dns.TypeA); err != nil {
					b.Error(err)
				}
			}
		})
	})
}

func TestDNSEndpointEntrySchedulingLifecycle(t *testing.T) {
	for _, policy := range []string{"RoundRobin", "Weighted"} {
		t.Run(policy, func(t *testing.T) {
			endpoint := infrastructurev1alpha1.DNSEndpoint{
				Spec: infrastructurev1alpha1.DNSEndpointSpec{
					DNSName: "example.com", RecordType: "a", RoutingPolicy: policy,
				},
			}
			manager := newTestDNSEndpointManager(endpoint)
			entry, err := manager.getDNSEndpoint("EXAMPLE.COM", dns.TypeA)
			if err != nil {
				t.Fatal(err)
			}
			targets := []weightedTarget{{Name: "a", Weight: 1}, {Name: "b", Weight: 2}}
			next := func(entry *dnsEndpointEntry) int {
				if policy == "RoundRobin" {
					return entry.getNext(3)
				}
				return entry.getWeightedNext(targets)
			}
			if got := next(entry); got != 1 {
				t.Fatalf("first selection = %d, want 1", got)
			}
			updated := endpoint.DeepCopy()
			updated.Spec.RecordTTL = 60
			manager.Sync.Lock()
			manager.publishEndpoint(dnsEndpointKey(endpoint.Spec.DNSName, endpoint.Spec.RecordType), updated)
			manager.Sync.Unlock()
			replacement, err := manager.getDNSEndpoint("example.com.", dns.TypeA)
			if err != nil {
				t.Fatal(err)
			}
			if entry == replacement || entry.Spec.RecordTTL != 0 || replacement.Spec.RecordTTL != 60 {
				t.Fatal("update did not replace configuration independently")
			}
			want := 2
			if policy == "Weighted" {
				want = 0
			}
			manager.Sync = nil
			if got := next(replacement); got != want {
				t.Fatalf("selection after same-key update = %d, want %d", got, want)
			}

			manager.Sync = &sync.RWMutex{}
			manager.Sync.Lock()
			manager.publishEndpoint(dnsEndpointKey("example.com", "A"), nil)
			manager.publishEndpoint(endpointKey{}, &endpoint)
			manager.Sync.Unlock()
			fresh, err := manager.getDNSEndpoint("example.com.", dns.TypeA)
			if err != nil {
				t.Fatal(err)
			}
			if fresh.counter == entry.counter && policy == "RoundRobin" ||
				fresh.weighted == entry.weighted && policy == "Weighted" {
				t.Fatal("recreated endpoint reused deleted scheduling state")
			}
			if got := next(fresh); got != 1 {
				t.Fatalf("recreated endpoint selection = %d, want 1", got)
			}
			// In-flight requests can safely finish with a deleted entry.
			next(entry)
		})
	}
}

func TestDNSEndpointEntryConcurrentScheduling(t *testing.T) {
	for _, policy := range []string{"RoundRobin", "Weighted"} {
		t.Run(policy, func(t *testing.T) {
			manager := newTestDNSEndpointManager(infrastructurev1alpha1.DNSEndpoint{
				Spec: infrastructurev1alpha1.DNSEndpointSpec{
					DNSName: "example.com", RecordType: "A", RoutingPolicy: policy,
				},
			})
			entry, err := manager.getDNSEndpoint("example.com.", dns.TypeA)
			if err != nil {
				t.Fatal(err)
			}
			var counts [3]atomic.Int64
			var workers sync.WaitGroup
			workers.Go(func() {
				updated := entry.DNSEndpoint.DeepCopy()
				for i := range 100 {
					updated.Spec.RecordTTL = i
					manager.Sync.Lock()
					manager.publishEndpoint(dnsEndpointKey("example.com", "A"), updated)
					manager.Sync.Unlock()
				}
			})
			for range 8 {
				workers.Go(func() {
					for range 300 {
						current, err := manager.getDNSEndpoint("example.com.", dns.TypeA)
						if err != nil {
							t.Error(err)
							return
						}
						idx := current.getNext(3)
						if policy == "Weighted" {
							idx = current.getWeightedNext([]weightedTarget{
								{Name: "a", Weight: 1}, {Name: "b", Weight: 2},
							})
						}
						counts[idx].Add(1)
					}
				})
			}
			workers.Wait()
			if policy == "RoundRobin" {
				for i := range counts {
					if got := counts[i].Load(); got != 800 {
						t.Fatalf("target %d count = %d, want 800", i, got)
					}
				}
			} else if counts[0].Load() != 800 || counts[1].Load() != 1600 {
				t.Fatalf("weighted counts = %d/%d, want 800/1600", counts[0].Load(), counts[1].Load())
			}
		})
	}
}

func TestGetDNSEndpointNormalizesDNSNameAndRecordType(t *testing.T) {
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example-a"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:    "Example.COM",
			RecordType: "A",
		},
	}
	manager := newTestDNSEndpointManager(dnsEndpoint)

	got, err := manager.GetDNSEndpoint("example.com.", 1)
	if err != nil {
		t.Fatalf("GetDNSEndpoint() returned an error: %v", err)
	}
	if got.Name != dnsEndpoint.Name {
		t.Fatalf("GetDNSEndpoint().Name = %q, want %q", got.Name, dnsEndpoint.Name)
	}
}

func TestGetDNSEndpointFallsBackToSingleLabelWildcard(t *testing.T) {
	wildcardEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "wildcard"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName: "*.me.example.com", RecordType: "A",
		},
	}
	manager := newTestDNSEndpointManager(wildcardEndpoint)

	got, err := manager.GetDNSEndpoint("API.ME.EXAMPLE.COM.", dns.TypeA)
	if err != nil {
		t.Fatalf("GetDNSEndpoint() returned an error: %v", err)
	}
	if got.Name != wildcardEndpoint.Name {
		t.Fatalf("GetDNSEndpoint().Name = %q, want %q", got.Name, wildcardEndpoint.Name)
	}
	if _, err := manager.GetDNSEndpoint("api.example.com.", dns.TypeA); err == nil {
		t.Fatal("GetDNSEndpoint() matched a wildcard with a different suffix")
	}

	deeperWildcard := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "deeper-wildcard"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName: "*.example.com", RecordType: "A",
		},
	}
	manager = newTestDNSEndpointManager(deeperWildcard)
	if _, err := manager.GetDNSEndpoint("api.me.example.com.", dns.TypeA); err == nil {
		t.Fatal("GetDNSEndpoint() matched a wildcard across multiple labels")
	}
}

func TestGetDNSEndpointPrefersExactMatchOverWildcard(t *testing.T) {
	exactEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "exact"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName: "api.me.example.com", RecordType: "A",
		},
	}
	wildcardEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "wildcard"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName: "*.me.example.com", RecordType: "A",
		},
	}
	manager := newTestDNSEndpointManager(wildcardEndpoint, exactEndpoint)

	got, err := manager.GetDNSEndpoint("api.me.example.com.", dns.TypeA)
	if err != nil {
		t.Fatalf("GetDNSEndpoint() returned an error: %v", err)
	}
	if got.Name != exactEndpoint.Name {
		t.Fatalf("GetDNSEndpoint().Name = %q, want exact endpoint %q", got.Name, exactEndpoint.Name)
	}
}

func TestGetDNSEndpointCNAMERequiresExactRecordType(t *testing.T) {
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example-cname"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:    "example.com",
			RecordType: "CNAME",
		},
	}
	manager := newTestDNSEndpointManager(dnsEndpoint)

	for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA} {
		t.Run(dns.TypeToString[qtype], func(t *testing.T) {
			got, err := manager.GetDNSEndpoint("example.com.", qtype)
			if err == nil {
				t.Fatalf("GetDNSEndpoint() returned %q, want a lookup miss", got.Name)
			}
		})
	}

	got, err := manager.GetDNSEndpoint("example.com.", dns.TypeCNAME)
	if err != nil {
		t.Fatalf("GetDNSEndpoint() returned an error: %v", err)
	}
	if got.Name != dnsEndpoint.Name {
		t.Fatalf("GetDNSEndpoint().Name = %q, want %q", got.Name, dnsEndpoint.Name)
	}
}

func TestGetNextReturnsZeroWhenModIsZero(t *testing.T) {
	entry := &dnsEndpointEntry{counter: &atomic.Uint32{}}

	if got := entry.getNext(0); got != 0 {
		t.Fatalf("GetNext() = %d, want 0", got)
	}
}

func TestGetNextReturnsZeroWhenNoCounterExists(t *testing.T) {
	entry := &dnsEndpointEntry{}

	if got := entry.getNext(3); got != 0 {
		t.Fatalf("GetNext() = %d, want 0", got)
	}
}

func TestGetNextIncrementsAndWrapsModulo(t *testing.T) {
	entry := &dnsEndpointEntry{counter: &atomic.Uint32{}}

	want := []int{1, 2, 0, 1, 2, 0}
	for i, w := range want {
		if got := entry.getNext(3); got != w {
			t.Fatalf("call %d: GetNext() = %d, want %d", i, got, w)
		}
	}
}

func TestGetNextResetsCounterBeforeOverflow(t *testing.T) {
	counter := &atomic.Uint32{}
	counter.Store(roundRobinResetThreshold - 1)
	entry := &dnsEndpointEntry{counter: counter}

	// This call pushes the counter to the reset threshold, triggering a reset to 0.
	entry.getNext(5)

	if got := counter.Load(); got != 0 {
		t.Fatalf("counter after reset = %d, want 0", got)
	}
}

func TestGetWeightedNextUsesSmoothWeightedRoundRobin(t *testing.T) {
	entry := &dnsEndpointEntry{weighted: &weightedRoundRobinState{}}
	targets := []weightedTarget{
		{Name: "loc-a", Weight: 1},
		{Name: "loc-b", Weight: 2},
	}

	want := []int{1, 0, 1, 1, 0, 1}
	for i, expected := range want {
		if got := entry.getWeightedNext(targets); got != expected {
			t.Fatalf("call %d: GetWeightedNext() = %d, want %d", i, got, expected)
		}
	}
}

func TestGetWeightedNextResetsWhenWeightsChange(t *testing.T) {
	entry := &dnsEndpointEntry{weighted: &weightedRoundRobinState{}}

	entry.getWeightedNext([]weightedTarget{{Name: "loc-a", Weight: 1}, {Name: "loc-b", Weight: 2}})
	got := entry.getWeightedNext([]weightedTarget{{Name: "loc-a", Weight: 2}, {Name: "loc-b", Weight: 1}})
	if got != 0 {
		t.Fatalf("GetWeightedNext() after weight change = %d, want 0", got)
	}
}
