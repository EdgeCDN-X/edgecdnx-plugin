package edgecdnxplugin

import (
	"context"
	"net"
	"testing"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/miekg/dns"
	corev1 "k8s.io/api/core/v1"
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
	manager := newTestDNSEndpointManager(dnsEndpoint)
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

func TestServeDNSWildcardEndpointAnswersForQueriedName(t *testing.T) {
	dnsEndpoint := infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "wildcard-a"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName:       "*.me.example.com",
			RoutingPolicy: "Simple",
			RecordTTL:     60,
			RecordType:    "A",
			Targets:       []string{"192.0.2.1"},
		},
	}
	plugin := EdgeCDNX{DNSEndpointManager: newTestDNSEndpointManager(dnsEndpoint)}
	request := new(dns.Msg)
	request.SetQuestion("api.me.example.com.", dns.TypeA)
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
	if got, want := writer.msg.Answer[0].Header().Name, "api.me.example.com."; got != want {
		t.Fatalf("Answer owner = %q, want queried name %q", got, want)
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
	manager := newTestDNSEndpointManager(dnsEndpoint)
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

func TestServeDNSDuplicateLocationNamesStayWithinTenant(t *testing.T) {
	for _, policy := range []string{"RoundRobin", "Weighted", "Geolocation", "Failover"} {
		t.Run(policy, func(t *testing.T) {
			endpoint := infrastructurev1alpha1.DNSEndpoint{
				ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "tenant-a"},
				Spec: infrastructurev1alpha1.DNSEndpointSpec{
					Project: &corev1.LocalObjectReference{Name: "one"}, DNSName: "example.com", RecordType: "A", RoutingPolicy: policy,
					Targets: []string{"edge"},
				},
			}
			locations := newTestLocationManager(t)
			locations.Config.RecordTTL = 60
			for _, tenant := range []struct{ namespace, project, name, ip string }{
				{"tenant-a", "one", "edge", "192.0.2.1"}, {"tenant-b", "one", "edge", "192.0.2.2"}, {"tenant-a", "two", "foreign", "192.0.2.3"},
			} {
				location := newTestLocation(tenant.name, nil, "", "node")
				location.Namespace = tenant.namespace
				location.Spec.Project = &corev1.LocalObjectReference{Name: tenant.project}
				location.Spec.GeoLookup.Attributes = map[string]infrastructurev1alpha1.GeoLookupAttributeSpec{"default": {Weight: 1}}
				location.Spec.NodeGroups[0].Nodes[0].Ipv4 = tenant.ip
				locations.locationEventHandlers().AddFunc(testLocationObject(t, location))
			}
			prefixes := newTestPrefixListRoutingManager()
			plugin := EdgeCDNX{DNSEndpointManager: newTestDNSEndpointManager(endpoint), LocationManager: &locations, PrefixListRoutingManager: &prefixes}
			for range 4 {
				request := new(dns.Msg)
				request.SetQuestion("example.com.", dns.TypeA)
				writer := &captureResponseWriter{testResponseWriter: testResponseWriter{remoteAddr: &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 5353}}}
				rcode, err := plugin.ServeDNS(context.Background(), writer, request)
				if err != nil || rcode != dns.RcodeSuccess || writer.msg == nil || len(writer.msg.Answer) != 1 {
					t.Fatalf("ServeDNS failed: rcode=%d err=%v answer=%v", rcode, err, writer.msg)
				}
				if answer, ok := writer.msg.Answer[0].(*dns.A); !ok || answer.A.String() != "192.0.2.1" {
					t.Fatalf("cross-tenant DNS answer: %v", writer.msg.Answer)
				}
			}
		})
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
	dnsEndpointManager := newTestDNSEndpointManager(dnsEndpoint)
	prefixManager := newTestPrefixListRoutingManager()
	addTestPrefixRoute(t, prefixManager, map[string]string{"tenant": "acme"}, "198.51.100.0/24", "edge-location")

	location := newTestLocation("edge-location", nil, "", "edge-node")
	location.Spec.NodeGroups[0].Labels = map[string]string{"tenant": "acme"}
	locationManager := newTestLocationManager(t)
	locationManager.Locations[location.Name] = location
	locationManager.Config.RecordTTL = 60

	plugin := EdgeCDNX{
		DNSEndpointManager:       dnsEndpointManager,
		PrefixListRoutingManager: &prefixManager,
		LocationManager:          &locationManager,
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
	dnsEndpointManager := newTestDNSEndpointManager(dnsEndpoint)

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
	locationManager.Config.RecordTTL = 60

	plugin := EdgeCDNX{
		DNSEndpointManager: dnsEndpointManager,
		LocationManager:    &locationManager,
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
	dnsEndpointManager := newTestDNSEndpointManager(dnsEndpoint)

	locationManager := newTestLocationManager(t)

	plugin := EdgeCDNX{
		DNSEndpointManager: dnsEndpointManager,
		LocationManager:    &locationManager,
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

func TestServeDNSWeightedEndpointUsesEntryState(t *testing.T) {
	manager := newTestDNSEndpointManager(infrastructurev1alpha1.DNSEndpoint{
		ObjectMeta: metav1.ObjectMeta{Name: "weighted"},
		Spec: infrastructurev1alpha1.DNSEndpointSpec{
			DNSName: "example.com", RecordType: "A", RoutingPolicy: "Weighted",
		},
	})
	manager.Sync = nil
	locationManager := newTestLocationManager(t)
	for i, name := range []string{"loc-a", "loc-b"} {
		location := newTestLocation(name, nil, "", "node")
		location.Spec.Weight = int32(i + 1)
		location.Spec.NodeGroups[0].Nodes[0].Ipv4 = []string{"192.0.2.1", "192.0.2.2"}[i]
		locationManager.Locations[name] = location
	}
	plugin := EdgeCDNX{
		DNSEndpointManager: manager,
		LocationManager:    &locationManager,
	}
	for _, want := range []string{"192.0.2.2", "192.0.2.1", "192.0.2.2", "192.0.2.2", "192.0.2.1", "192.0.2.2"} {
		request := new(dns.Msg)
		request.SetQuestion("example.com.", dns.TypeA)
		writer := newCaptureResponseWriter()
		rcode, err := plugin.ServeDNS(context.Background(), writer, request)
		if err != nil || rcode != dns.RcodeSuccess {
			t.Fatalf("ServeDNS() = %d, %v", rcode, err)
		}
		assertFailoverAnswer(t, writer, want)
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
	if endpointKey.name == "" {
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

func newFailoverTestPlugin(t *testing.T, primaryUnhealthy, fallbackUnhealthy bool) (EdgeCDNX, endpointKey) {
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
	dnsEndpointManager := newTestDNSEndpointManager(dnsEndpoint)

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
	locationManager.Config.RecordTTL = 60

	return EdgeCDNX{
		DNSEndpointManager: dnsEndpointManager,
		LocationManager:    &locationManager,
	}, key
}

func setTestNodeHealth(location *infrastructurev1alpha1.Location, healthy bool) {
	nodeName := location.Spec.NodeGroups[0].Nodes[0].Name
	location.Status.NodeStatus = map[string]infrastructurev1alpha1.NodeInstanceStatus{
		nodeName: {
			Conditions: []infrastructurev1alpha1.NodeCondition{{
				Type:   infrastructurev1alpha1.HealthCheckSuccessful,
				Stack:  infrastructurev1alpha1.StackTypeIPv4,
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
