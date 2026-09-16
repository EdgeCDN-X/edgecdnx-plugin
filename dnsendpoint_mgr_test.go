package edgecdnxplugin

import (
	"sync"
	"sync/atomic"
	"testing"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetDNSEndpointNormalizesDNSNameAndRecordType(t *testing.T) {
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example-a"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:    "Example.COM",
			RecordType: "A",
		},
	}
	manager := DNSEndpointManager{
		Sync: &sync.RWMutex{},
		DNSEndpoints: map[string]infrastructurev1alpha1.DNSEndpoint{
			dnsEndpointKey(dnsEndpoint.Spec.DNSName, dnsEndpoint.Spec.RecordType): dnsEndpoint,
		},
	}

	got, err := manager.GetDNSEndpoint("example.com.", 1)
	if err != nil {
		t.Fatalf("GetDNSEndpoint() returned an error: %v", err)
	}
	if got.Name != dnsEndpoint.Name {
		t.Fatalf("GetDNSEndpoint().Name = %q, want %q", got.Name, dnsEndpoint.Name)
	}
}

func TestGetDNSEndpointFallsBackToCNAME(t *testing.T) {
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example-cname"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:    "example.com",
			RecordType: "CNAME",
		},
	}
	manager := DNSEndpointManager{
		Sync: &sync.RWMutex{},
		DNSEndpoints: map[string]infrastructurev1alpha1.DNSEndpoint{
			dnsEndpointKey(dnsEndpoint.Spec.DNSName, dnsEndpoint.Spec.RecordType): dnsEndpoint,
		},
	}

	got, err := manager.GetDNSEndpoint("example.com.", 28)
	if err != nil {
		t.Fatalf("GetDNSEndpoint() returned an error: %v", err)
	}
	if got.Name != dnsEndpoint.Name {
		t.Fatalf("GetDNSEndpoint().Name = %q, want %q", got.Name, dnsEndpoint.Name)
	}
}

func TestGetNextReturnsZeroWhenModIsZero(t *testing.T) {
	key := dnsEndpointKey("example.com", "A")
	manager := &DNSEndpointManager{
		Sync:               &sync.RWMutex{},
		roundRobinCounters: map[string]*atomic.Uint32{key: {}},
	}

	if got := manager.GetNext("example.com.", 1, 0); got != 0 {
		t.Fatalf("GetNext() = %d, want 0", got)
	}
}

func TestGetNextReturnsZeroWhenNoCounterExists(t *testing.T) {
	manager := &DNSEndpointManager{
		Sync:               &sync.RWMutex{},
		roundRobinCounters: map[string]*atomic.Uint32{},
	}

	if got := manager.GetNext("example.com.", 1, 3); got != 0 {
		t.Fatalf("GetNext() = %d, want 0", got)
	}
}

func TestGetNextIncrementsAndWrapsModulo(t *testing.T) {
	key := dnsEndpointKey("example.com", "A")
	manager := &DNSEndpointManager{
		Sync:               &sync.RWMutex{},
		roundRobinCounters: map[string]*atomic.Uint32{key: {}},
	}

	want := []int{1, 2, 0, 1, 2, 0}
	for i, w := range want {
		if got := manager.GetNext("example.com.", 1, 3); got != w {
			t.Fatalf("call %d: GetNext() = %d, want %d", i, got, w)
		}
	}
}

func TestGetNextResetsCounterBeforeOverflow(t *testing.T) {
	key := dnsEndpointKey("example.com", "A")
	counter := &atomic.Uint32{}
	counter.Store(roundRobinResetThreshold - 1)
	manager := &DNSEndpointManager{
		Sync:               &sync.RWMutex{},
		roundRobinCounters: map[string]*atomic.Uint32{key: counter},
	}

	// This call pushes the counter to the reset threshold, triggering a reset to 0.
	manager.GetNext("example.com.", 1, 5)

	if got := counter.Load(); got != 0 {
		t.Fatalf("counter after reset = %d, want 0", got)
	}
}

func TestGetWeightedNextUsesSmoothWeightedRoundRobin(t *testing.T) {
	manager := &DNSEndpointManager{Sync: &sync.RWMutex{}}
	targets := []weightedTarget{
		{Name: "loc-a", Weight: 1},
		{Name: "loc-b", Weight: 2},
	}

	want := []int{1, 0, 1, 1, 0, 1}
	for i, expected := range want {
		if got := manager.GetWeightedNext("example.com.", 1, targets); got != expected {
			t.Fatalf("call %d: GetWeightedNext() = %d, want %d", i, got, expected)
		}
	}
}

func TestGetWeightedNextResetsWhenWeightsChange(t *testing.T) {
	manager := &DNSEndpointManager{Sync: &sync.RWMutex{}}

	manager.GetWeightedNext("example.com.", 1, []weightedTarget{{Name: "loc-a", Weight: 1}, {Name: "loc-b", Weight: 2}})
	got := manager.GetWeightedNext("example.com.", 1, []weightedTarget{{Name: "loc-a", Weight: 2}, {Name: "loc-b", Weight: 1}})
	if got != 0 {
		t.Fatalf("GetWeightedNext() after weight change = %d, want 0", got)
	}
}
