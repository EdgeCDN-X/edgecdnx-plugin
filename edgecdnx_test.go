package edgecdnxplugin

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
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

func TestServeDNSRoundRobinEndpointCyclesThroughLocations(t *testing.T) {
	routeSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "acme"}}
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example-rr"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:       "example.com",
			RoutingPolicy: "RoundRobin",
			RecordTTL:     30,
			RecordType:    "A",
			RouteSelector: routeSelector,
		},
	}
	key := dnsEndpointKey(dnsEndpoint.Spec.DNSName, dnsEndpoint.Spec.RecordType)
	dnsEndpointManager := &DNSEndpointManager{
		Sync: &sync.RWMutex{},
		DNSEndpoints: map[string]infrastructurev1alpha1.DNSEndpoint{
			key: dnsEndpoint,
		},
		roundRobinCounters: map[string]*atomic.Uint32{key: {}},
	}

	locA := newTestLocation("loc-a", map[string]string{"tenant": "acme"}, "", "node-a")
	locA.Spec.NodeGroups[0].Labels = map[string]string{"tenant": "acme"}
	locA.Spec.NodeGroups[0].Nodes[0].Ipv4 = "192.0.2.1"

	locB := newTestLocation("loc-b", map[string]string{"tenant": "acme"}, "", "node-b")
	locB.Spec.NodeGroups[0].Labels = map[string]string{"tenant": "acme"}
	locB.Spec.NodeGroups[0].Nodes[0].Ipv4 = "192.0.2.2"

	locC := newTestLocation("loc-c", map[string]string{"tenant": "acme"}, "", "node-c")
	locC.Spec.NodeGroups[0].Labels = map[string]string{"tenant": "acme"}
	locC.Spec.NodeGroups[0].Nodes[0].Ipv4 = "192.0.2.3"

	locationManager := newTestLocationManager(t)
	locationManager.Locations[locA.Name] = locA
	locationManager.Locations[locB.Name] = locB
	locationManager.Locations[locC.Name] = locC
	locationManager.Config.RecrodTTL = 60

	plugin := EdgeCDNX{
		DNSEndpointManager: dnsEndpointManager,
		LocationManager:    &locationManager,
		DNSResponseType:    A_AAAA,
	}

	// Sorted locations are [loc-a, loc-b, loc-c]; GetNext starts by incrementing to 1, so
	// the first response lands on loc-b and then wraps every 3 calls.
	wantIPs := []string{"192.0.2.2", "192.0.2.3", "192.0.2.1", "192.0.2.2"}
	for i, want := range wantIPs {
		request := new(dns.Msg)
		request.SetQuestion("example.com.", dns.TypeA)
		writer := &captureResponseWriter{testResponseWriter: testResponseWriter{
			remoteAddr: &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 5353},
		}}

		rcode, err := plugin.ServeDNS(context.Background(), writer, request)
		if err != nil {
			t.Fatalf("call %d: ServeDNS() returned an error: %v", i, err)
		}
		if rcode != dns.RcodeSuccess {
			t.Fatalf("call %d: ServeDNS() rcode = %d, want %d", i, rcode, dns.RcodeSuccess)
		}
		if len(writer.msg.Answer) != 1 {
			t.Fatalf("call %d: len(Answer) = %d, want 1", i, len(writer.msg.Answer))
		}
		answer, ok := writer.msg.Answer[0].(*dns.A)
		if !ok {
			t.Fatalf("call %d: Answer[0] type = %T, want *dns.A", i, writer.msg.Answer[0])
		}
		if got := answer.A.String(); got != want {
			t.Fatalf("call %d: A target = %q, want %q", i, got, want)
		}
	}
}

func TestServeDNSRoundRobinEndpointFallsThroughWhenNoLocationsMatch(t *testing.T) {
	routeSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "acme"}}
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example-rr"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:       "example.com",
			RoutingPolicy: "RoundRobin",
			RecordTTL:     30,
			RecordType:    "A",
			RouteSelector: routeSelector,
		},
	}
	key := dnsEndpointKey(dnsEndpoint.Spec.DNSName, dnsEndpoint.Spec.RecordType)
	dnsEndpointManager := &DNSEndpointManager{
		Sync: &sync.RWMutex{},
		DNSEndpoints: map[string]infrastructurev1alpha1.DNSEndpoint{
			key: dnsEndpoint,
		},
		roundRobinCounters: map[string]*atomic.Uint32{key: {}},
	}

	locationManager := newTestLocationManager(t)

	plugin := EdgeCDNX{
		DNSEndpointManager: dnsEndpointManager,
		LocationManager:    &locationManager,
		DNSResponseType:    A_AAAA,
	}
	request := new(dns.Msg)
	request.SetQuestion("example.com.", dns.TypeA)
	writer := &captureResponseWriter{testResponseWriter: testResponseWriter{
		remoteAddr: &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 5353},
	}}

	_, err := plugin.ServeDNS(context.Background(), writer, request)
	if err == nil {
		t.Fatal("expected an error when no locations match the route selector")
	}
}

func TestServeDNSFailoverEndpointUsesPrimaryLocation(t *testing.T) {
	plugin, endpointKey := newFailoverTestPlugin(t, false, false)
	request := new(dns.Msg)
	request.SetQuestion("example.com.", dns.TypeA)
	writer := newCaptureResponseWriter()

	rcode, err := plugin.ServeDNS(context.Background(), writer, request)
	if err != nil {
		t.Fatalf("ServeDNS() returned an error: %v", err)
	}
	if rcode != dns.RcodeSuccess {
		t.Fatalf("ServeDNS() rcode = %d, want %d", rcode, dns.RcodeSuccess)
	}
	assertFailoverAnswer(t, writer, "192.0.2.1")
	if endpointKey == "" {
		t.Fatal("failover endpoint key must not be empty")
	}
}

func TestServeDNSFailoverEndpointUsesFirstHealthyFallback(t *testing.T) {
	plugin, _ := newFailoverTestPlugin(t, true, false)
	request := new(dns.Msg)
	request.SetQuestion("example.com.", dns.TypeA)
	writer := newCaptureResponseWriter()

	rcode, err := plugin.ServeDNS(context.Background(), writer, request)
	if err != nil {
		t.Fatalf("ServeDNS() returned an error: %v", err)
	}
	if rcode != dns.RcodeSuccess {
		t.Fatalf("ServeDNS() rcode = %d, want %d", rcode, dns.RcodeSuccess)
	}
	assertFailoverAnswer(t, writer, "192.0.2.2")
}

func TestServeDNSFailoverEndpointFailsWhenAllLocationsUnhealthy(t *testing.T) {
	plugin, _ := newFailoverTestPlugin(t, true, true)
	request := new(dns.Msg)
	request.SetQuestion("example.com.", dns.TypeA)
	writer := newCaptureResponseWriter()

	_, err := plugin.ServeDNS(context.Background(), writer, request)
	if err == nil {
		t.Fatal("expected an error when the primary and fallback locations are unhealthy")
	}
	if writer.msg != nil {
		t.Fatal("ServeDNS() wrote a response despite all failover locations being unhealthy")
	}
}

func newFailoverTestPlugin(t *testing.T, primaryUnhealthy, fallbackUnhealthy bool) (EdgeCDNX, string) {
	t.Helper()
	routeSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"tenant": "acme"}}
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "example-failover"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:       "example.com",
			RoutingPolicy: "Failover",
			RecordTTL:     30,
			RecordType:    "A",
			Targets:       []string{"primary"},
			RouteSelector: routeSelector,
		},
	}
	key := dnsEndpointKey(dnsEndpoint.Spec.DNSName, dnsEndpoint.Spec.RecordType)
	dnsEndpointManager := &DNSEndpointManager{
		Sync: &sync.RWMutex{},
		DNSEndpoints: map[string]infrastructurev1alpha1.DNSEndpoint{
			key: dnsEndpoint,
		},
	}

	primary := newTestLocation("primary", map[string]string{"tenant": "acme"}, "", "primary-node")
	primary.Spec.FallbackLocations = []string{"backup"}
	backup := newTestLocation("backup", map[string]string{"tenant": "acme"}, "", "backup-node")
	backup.Spec.NodeGroups[0].Nodes[0].Ipv4 = "192.0.2.2"
	if primaryUnhealthy {
		setTestNodeHealth(&primary, false)
	}
	if fallbackUnhealthy {
		setTestNodeHealth(&backup, false)
	}

	locationManager := newTestLocationManager(t)
	locationManager.Locations[primary.Name] = primary
	locationManager.Locations[backup.Name] = backup
	locationManager.Config.RecrodTTL = 60

	return EdgeCDNX{
		DNSEndpointManager: &DNSEndpointManager{
			Sync:               dnsEndpointManager.Sync,
			DNSEndpoints:       dnsEndpointManager.DNSEndpoints,
			roundRobinCounters: map[string]*atomic.Uint32{},
		},
		LocationManager: &locationManager,
		DNSResponseType: A_AAAA,
	}, key
}

func setTestNodeHealth(location *infrastructurev1alpha1.Location, healthy bool) {
	nodeName := location.Spec.NodeGroups[0].Nodes[0].Name
	location.Status.NodeStatus = map[string]infrastructurev1alpha1.NodeInstanceStatus{
		nodeName: {
			Conditions: []infrastructurev1alpha1.NodeCondition{{
				Type:   infrastructurev1alpha1.IPV4HealthCheckSuccessful,
				Status: healthy,
			}},
		},
	}
}

func newCaptureResponseWriter() *captureResponseWriter {
	return &captureResponseWriter{testResponseWriter: testResponseWriter{
		remoteAddr: &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 5353},
	}}
}

func assertFailoverAnswer(t *testing.T, writer *captureResponseWriter, want string) {
	t.Helper()
	if writer.msg == nil {
		t.Fatal("ServeDNS() did not write a response")
	}
	if len(writer.msg.Answer) != 1 {
		t.Fatalf("len(Answer) = %d, want 1", len(writer.msg.Answer))
	}
	answer, ok := writer.msg.Answer[0].(*dns.A)
	if !ok {
		t.Fatalf("Answer[0] type = %T, want *dns.A", writer.msg.Answer[0])
	}
	if got := answer.A.String(); got != want {
		t.Fatalf("A target = %q, want %q", got, want)
	}
}
