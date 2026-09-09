package edgecdnxplugin

import (
	"sync"
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
