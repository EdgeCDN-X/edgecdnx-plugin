package edgecdnxplugin

import (
	"context"
	"net"
	"sync"
	"testing"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/miekg/dns"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type captureResponseWriter struct {
	testResponseWriter
	msg *dns.Msg
}

func (w *captureResponseWriter) WriteMsg(msg *dns.Msg) error {
	w.msg = msg.Copy()
	return nil
}

func TestServeDNSSimpleEndpointReturnsTargets(t *testing.T) {
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example-a"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:       "example.com",
			RoutingPolicy: "Simple",
			RecordTTL:     120,
			RecordType:    "A",
			Targets:       []string{"192.0.2.1", "192.0.2.2"},
		},
	}
	manager := &DNSEndpointManager{
		Sync: &sync.RWMutex{},
		DNSEndpoints: map[string]infrastructurev1alpha1.DNSEndpoint{
			dnsEndpointKey(dnsEndpoint.Spec.DNSName, dnsEndpoint.Spec.RecordType): dnsEndpoint,
		},
	}
	plugin := EdgeCDNX{DNSEndpointManager: manager}
	request := new(dns.Msg)
	request.SetQuestion("example.com.", dns.TypeA)
	writer := &captureResponseWriter{testResponseWriter: testResponseWriter{
		remoteAddr: &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 5353},
	}}

	rcode, err := plugin.ServeDNS(context.Background(), writer, request)
	if err != nil {
		t.Fatalf("ServeDNS() returned an error: %v", err)
	}
	if rcode != dns.RcodeSuccess {
		t.Fatalf("ServeDNS() rcode = %d, want %d", rcode, dns.RcodeSuccess)
	}
	if writer.msg == nil {
		t.Fatal("ServeDNS() did not write a response")
	}
	if len(writer.msg.Answer) != 2 {
		t.Fatalf("len(Answer) = %d, want 2", len(writer.msg.Answer))
	}
	for i, answer := range writer.msg.Answer {
		if answer.Header().Ttl != 120 {
			t.Errorf("Answer[%d].TTL = %d, want 120", i, answer.Header().Ttl)
		}
		if _, ok := answer.(*dns.A); !ok {
			t.Errorf("Answer[%d] type = %T, want *dns.A", i, answer)
		}
	}
}

func TestServeDNSSimpleEndpointReturnsTXTTargets(t *testing.T) {
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example-txt"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:       "example.com",
			RoutingPolicy: "Simple",
			RecordTTL:     60,
			RecordType:    "TXT",
			Targets:       []string{`"verification=value"`},
		},
	}
	manager := &DNSEndpointManager{
		Sync: &sync.RWMutex{},
		DNSEndpoints: map[string]infrastructurev1alpha1.DNSEndpoint{
			dnsEndpointKey(dnsEndpoint.Spec.DNSName, dnsEndpoint.Spec.RecordType): dnsEndpoint,
		},
	}
	plugin := EdgeCDNX{DNSEndpointManager: manager}
	request := new(dns.Msg)
	request.SetQuestion("example.com.", dns.TypeTXT)
	writer := &captureResponseWriter{testResponseWriter: testResponseWriter{
		remoteAddr: &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 5353},
	}}

	rcode, err := plugin.ServeDNS(context.Background(), writer, request)
	if err != nil {
		t.Fatalf("ServeDNS() returned an error: %v", err)
	}
	if rcode != dns.RcodeSuccess {
		t.Fatalf("ServeDNS() rcode = %d, want %d", rcode, dns.RcodeSuccess)
	}
	if len(writer.msg.Answer) != 1 {
		t.Fatalf("len(Answer) = %d, want 1", len(writer.msg.Answer))
	}
	answer, ok := writer.msg.Answer[0].(*dns.TXT)
	if !ok {
		t.Fatalf("Answer[0] type = %T, want *dns.TXT", writer.msg.Answer[0])
	}
	if len(answer.Txt) != 1 || answer.Txt[0] != "verification=value" {
		t.Fatalf("TXT values = %v, want [verification=value]", answer.Txt)
	}
}

func TestServeDNSGeolocationEndpointUsesPrefixRoutingAndSelector(t *testing.T) {
	routeSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "acme"}}
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example-a"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:       "example.com",
			RoutingPolicy: "Geolocation",
			RecordTTL:     45,
			RecordType:    "A",
			RouteSelector: routeSelector,
		},
	}
	dnsEndpointManager := &DNSEndpointManager{
		Sync: &sync.RWMutex{},
		DNSEndpoints: map[string]infrastructurev1alpha1.DNSEndpoint{
			dnsEndpointKey(dnsEndpoint.Spec.DNSName, dnsEndpoint.Spec.RecordType): dnsEndpoint,
		},
	}
	prefixManager := newTestPrefixListRoutingManager()
	addTestPrefixRoute(t, prefixManager, map[string]string{"tenant": "acme"}, "198.51.100.0/24", "edge-location")

	location := newTestLocation("edge-location", nil, "", "edge-node")
	location.Spec.NodeGroups[0].Labels = map[string]string{"tenant": "acme"}
	locationManager := newTestLocationManager(t)
	locationManager.Locations[location.Name] = location
	locationManager.Config.RecrodTTL = 60

	plugin := EdgeCDNX{
		DNSEndpointManager:       dnsEndpointManager,
		PrefixListRoutingManager: &prefixManager,
		LocationManager:          &locationManager,
		DNSResponseType:          A_AAAA,
	}
	request := new(dns.Msg)
	request.SetQuestion("example.com.", dns.TypeA)
	writer := &captureResponseWriter{testResponseWriter: testResponseWriter{
		remoteAddr: &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 5353},
	}}

	rcode, err := plugin.ServeDNS(context.Background(), writer, request)
	if err != nil {
		t.Fatalf("ServeDNS() returned an error: %v", err)
	}
	if rcode != dns.RcodeSuccess {
		t.Fatalf("ServeDNS() rcode = %d, want %d", rcode, dns.RcodeSuccess)
	}
	if len(writer.msg.Answer) != 1 {
		t.Fatalf("len(Answer) = %d, want 1", len(writer.msg.Answer))
	}
	answer, ok := writer.msg.Answer[0].(*dns.A)
	if !ok {
		t.Fatalf("Answer[0] type = %T, want *dns.A", writer.msg.Answer[0])
	}
	if got, want := answer.A.String(), "192.0.2.1"; got != want {
		t.Fatalf("A target = %q, want %q", got, want)
	}
	if answer.Hdr.Ttl != 45 {
		t.Fatalf("A TTL = %d, want 45", answer.Hdr.Ttl)
	}
}
