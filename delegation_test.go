package edgecdnxplugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"

	api "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
)

func delegationEndpoint(name, recordType string, targets ...string) api.DNSEndpoint {
	return api.DNSEndpoint{Spec: api.DNSEndpointSpec{
		DNSName: name, RecordType: recordType, RoutingPolicy: "Simple", RecordTTL: 300, Targets: targets,
	}}
}

func delegationZoneManager(t testing.TB, zones ...string) *ZoneManager {
	t.Helper()
	manager := &ZoneManager{Sync: &sync.RWMutex{}}
	snapshot := &zoneSnapshot{zones: make(map[string]*zoneEntry)}
	for _, zone := range zones {
		soa, err := dns.NewRR(zone + " 300 IN SOA ns.example.net. hostmaster.example.net. 1 7200 3600 1209600 300")
		if err != nil {
			t.Fatal(err)
		}
		snapshot.zones[zone] = &zoneEntry{name: zone, records: []dns.RR{soa}, soa: soa}
	}
	manager.snapshot.Store(snapshot)
	return manager
}

func delegationQuery(t *testing.T, plugin EdgeCDNX, name string, qtype uint16) (*dns.Msg, int, error) {
	t.Helper()
	req := new(dns.Msg)
	req.SetQuestion(name, qtype)
	writer := &captureResponseWriter{testResponseWriter: testResponseWriter{
		remoteAddr: &net.UDPAddr{IP: net.ParseIP("198.51.100.10"), Port: 5353},
	}}
	rcode, err := plugin.ServeDNS(context.Background(), writer, req)
	return writer.msg, rcode, err
}

func TestDelegationReferral(t *testing.T) {
	manager := newTestDNSEndpointManager(
		delegationEndpoint("demo.tbotech.sk", "NS", "ns1.randomnsserver.com.", "ns2.randomnsserver.com."),
		delegationEndpoint("www.demo.tbotech.sk", "A", "192.0.2.10"),
		delegationEndpoint("nested.demo.tbotech.sk", "NS", "ns.other.example."),
		delegationEndpoint("tbotech.sk", "NS", "ns.parent.example."),
	)
	plugin := EdgeCDNX{ZoneManager: delegationZoneManager(t, "tbotech.sk."), DNSEndpointManager: manager}
	for _, tc := range []struct {
		name  string
		qtype uint16
	}{
		{"demo.tbotech.sk.", dns.TypeNS},
		{"demo.tbotech.sk.", dns.TypeA},
		{"www.demo.tbotech.sk.", dns.TypeA},
		{"nested.demo.tbotech.sk.", dns.TypeDS},
	} {
		t.Run(tc.name+dns.TypeToString[tc.qtype], func(t *testing.T) {
			msg, rcode, err := delegationQuery(t, plugin, tc.name, tc.qtype)
			if err != nil || rcode != dns.RcodeSuccess || msg == nil {
				t.Fatalf("query: msg=%v rcode=%d err=%v", msg, rcode, err)
			}
			if msg.Rcode != dns.RcodeSuccess || msg.Authoritative || len(msg.Answer) != 0 || len(msg.Ns) != 2 || len(msg.Extra) != 0 {
				t.Fatalf("unexpected referral: %v", msg)
			}
			for _, rr := range msg.Ns {
				if rr.Header().Name != "demo.tbotech.sk." || rr.Header().Ttl != 300 || rr.Header().Rrtype != dns.TypeNS {
					t.Errorf("unexpected referral record: %v", rr)
				}
			}
		})
	}
}

func TestDelegationGlue(t *testing.T) {
	manager := newTestDNSEndpointManager(
		delegationEndpoint("demo.tbotech.sk", "NS", "ns1.demo.tbotech.sk.", "ns2.other.net."),
		delegationEndpoint("ns1.demo.tbotech.sk", "A", "192.0.2.53"),
		delegationEndpoint("ns1.demo.tbotech.sk", "AAAA", "2001:db8::53"),
		delegationEndpoint("ns2.other.net", "A", "192.0.2.54"),
	)
	plugin := EdgeCDNX{ZoneManager: delegationZoneManager(t, "tbotech.sk."), DNSEndpointManager: manager}
	msg, rcode, err := delegationQuery(t, plugin, "www.demo.tbotech.sk.", dns.TypeA)
	if err != nil || rcode != dns.RcodeSuccess || msg == nil {
		t.Fatalf("query: msg=%v rcode=%d err=%v", msg, rcode, err)
	}
	if len(msg.Extra) != 2 {
		t.Fatalf("glue = %v, want A and AAAA only", msg.Extra)
	}
	if _, ok := msg.Extra[0].(*dns.A); !ok {
		t.Errorf("first glue record = %T, want A", msg.Extra[0])
	}
	if _, ok := msg.Extra[1].(*dns.AAAA); !ok {
		t.Errorf("second glue record = %T, want AAAA", msg.Extra[1])
	}
}

func TestDelegationTruncatesIncompleteGlue(t *testing.T) {
	targets := make([]string, 0, 100)
	for i := 1; i <= 100; i++ {
		targets = append(targets, fmt.Sprintf("192.0.2.%d", i))
	}
	manager := newTestDNSEndpointManager(
		delegationEndpoint("demo.tbotech.sk", "NS", "ns.demo.tbotech.sk."),
		delegationEndpoint("ns.demo.tbotech.sk", "A", targets...),
	)
	plugin := EdgeCDNX{ZoneManager: delegationZoneManager(t, "tbotech.sk."), DNSEndpointManager: manager}
	msg, rcode, err := delegationQuery(t, plugin, "www.demo.tbotech.sk.", dns.TypeA)
	if err != nil || rcode != dns.RcodeSuccess || msg == nil {
		t.Fatalf("query: msg=%v rcode=%d err=%v", msg, rcode, err)
	}
	packed, err := msg.Pack()
	if err != nil || !msg.Truncated || len(packed) > dns.MinMsgSize {
		t.Fatalf("expected truncated UDP response <= %d bytes: size=%d TC=%t err=%v", dns.MinMsgSize, len(packed), msg.Truncated, err)
	}
}

func TestDelegationDSAndHostedChild(t *testing.T) {
	manager := newTestDNSEndpointManager(
		delegationEndpoint("demo.tbotech.sk", "NS", "ns.child.example."),
		delegationEndpoint("www.demo.tbotech.sk", "A", "192.0.2.10"),
	)
	for _, tc := range []struct {
		name  string
		zones []string
	}{
		{"external child", []string{"tbotech.sk."}},
		{"locally hosted child", []string{"tbotech.sk.", "demo.tbotech.sk."}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin := EdgeCDNX{ZoneManager: delegationZoneManager(t, tc.zones...), DNSEndpointManager: manager}
			msg, rcode, err := delegationQuery(t, plugin, "demo.tbotech.sk.", dns.TypeDS)
			if err != nil || rcode != dns.RcodeSuccess || msg == nil {
				t.Fatalf("DS query: msg=%v rcode=%d err=%v", msg, rcode, err)
			}
			if !msg.Authoritative || msg.Rcode != dns.RcodeSuccess || len(msg.Answer) != 0 || len(msg.Ns) != 1 ||
				msg.Ns[0].Header().Rrtype != dns.TypeSOA || msg.Ns[0].Header().Name != "tbotech.sk." {
				t.Fatalf("unexpected parent DS response: %v", msg)
			}
			if len(tc.zones) > 1 {
				msg, rcode, err = delegationQuery(t, plugin, "www.demo.tbotech.sk.", dns.TypeA)
				if err != nil || rcode != dns.RcodeSuccess || msg == nil || !msg.Authoritative || len(msg.Answer) != 1 {
					t.Fatalf("hosted child response: msg=%v rcode=%d err=%v", msg, rcode, err)
				}
			}
		})
	}
}

func TestDelegationIgnoresApexAndWildcardNS(t *testing.T) {
	manager := newTestDNSEndpointManager(
		delegationEndpoint("tbotech.sk", "NS", "ns.parent.example."),
		delegationEndpoint("*.tbotech.sk", "NS", "ns.wildcard.example."),
		delegationEndpoint("www.tbotech.sk", "A", "192.0.2.20"),
	)
	plugin := EdgeCDNX{ZoneManager: delegationZoneManager(t, "tbotech.sk."), DNSEndpointManager: manager}
	msg, rcode, err := delegationQuery(t, plugin, "www.tbotech.sk.", dns.TypeA)
	if err != nil || rcode != dns.RcodeSuccess || msg == nil || !msg.Authoritative || len(msg.Answer) != 1 {
		t.Fatalf("normal response: msg=%v rcode=%d err=%v", msg, rcode, err)
	}
}

func TestDelegationConflictAndInvalidTargets(t *testing.T) {
	for _, tc := range []struct {
		name      string
		endpoints []api.DNSEndpoint
		conflict  endpointKey
	}{
		{"conflicting cut", nil, endpointKey{name: "demo.tbotech.sk.", qtype: dns.TypeNS}},
		{"invalid NS", []api.DNSEndpoint{delegationEndpoint("demo.tbotech.sk", "NS", "bad target")}, endpointKey{}},
		{"conflicting glue", []api.DNSEndpoint{delegationEndpoint("demo.tbotech.sk", "NS", "ns.demo.tbotech.sk.")}, endpointKey{name: "ns.demo.tbotech.sk.", qtype: dns.TypeA}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newTestDNSEndpointManager(tc.endpoints...)
			if tc.conflict.name != "" {
				manager.publishEndpoint(tc.conflict, nil, true)
			}
			plugin := EdgeCDNX{ZoneManager: delegationZoneManager(t, "tbotech.sk."), DNSEndpointManager: manager}
			_, rcode, err := delegationQuery(t, plugin, "www.demo.tbotech.sk.", dns.TypeA)
			if rcode != dns.RcodeServerFailure || err == nil {
				t.Fatalf("rcode=%d err=%v, want SERVFAIL", rcode, err)
			}
			if tc.conflict.name != "" && !errors.Is(err, errConflictingDNS) {
				t.Fatalf("error = %v, want conflicting DNS", err)
			}
		})
	}
}

func TestDelegationSnapshotUpdatesGlue(t *testing.T) {
	ns := delegationEndpoint("demo.tbotech.sk", "NS", "ns.demo.tbotech.sk.")
	address := delegationEndpoint("ns.demo.tbotech.sk", "A", "192.0.2.1")
	manager := newTestDNSEndpointManager(ns, address)
	old := manager.endpoints.Load()
	address.Spec.Targets = []string{"192.0.2.2"}
	manager.publishEndpoint(dnsEndpointKey(address.Spec.DNSName, "A"), &address)
	current := manager.endpoints.Load()
	glueAddress := func(s *dnsEndpointSnapshot) string {
		return s.delegations["demo.tbotech.sk."].glue[0].records[0].(*dns.A).A.String()
	}
	if glueAddress(old) != "192.0.2.1" || glueAddress(current) != "192.0.2.2" {
		t.Fatal("glue update must rebuild referrals without mutating retained snapshots")
	}
	manager.publishEndpoint(dnsEndpointKey(address.Spec.DNSName, "A"), nil)
	if len(manager.endpoints.Load().delegations["demo.tbotech.sk."].glue[0].records) != 0 {
		t.Fatal("deleted glue remains in compiled referral")
	}
	manager.publishEndpoint(dnsEndpointKey(ns.Spec.DNSName, "NS"), nil)
	if len(manager.endpoints.Load().delegations) != 0 {
		t.Fatal("deleted cut remains in delegation index")
	}
}

func TestDelegationLookupsDoNotAllocate(t *testing.T) {
	zones := delegationZoneManager(t, "tbotech.sk.").snapshot.Load()
	manager := newTestDNSEndpointManager(delegationEndpoint("demo.tbotech.sk", "NS", "ns.example.net."))
	snapshot := manager.endpoints.Load()
	allocations := testing.AllocsPerRun(100, func() {
		zone, conflict := zones.lookup("www.demo.tbotech.sk.")
		if conflict || zone == nil || snapshot.findZoneCut(zone.name, "www.demo.tbotech.sk.") == nil {
			panic("lookup failed")
		}
	})
	if allocations != 0 {
		t.Fatalf("lookup allocations = %g, want 0", allocations)
	}
}

func TestDelegationNoCutsFastPath(t *testing.T) {
	plugin := EdgeCDNX{DNSEndpointManager: newTestDNSEndpointManager()}
	state := request.Request{}
	zone := &zoneEntry{name: "tbotech.sk."}
	allocations := testing.AllocsPerRun(100, func() {
		handled, _, err := plugin.serveDelegation(state, nil, zone)
		if handled || err != nil {
			panic("empty delegation index should not handle query")
		}
	})
	if allocations != 0 {
		t.Fatalf("fast-path allocations = %g, want 0", allocations)
	}
}

func BenchmarkDelegationLookup(b *testing.B) {
	for _, count := range []int{1, 10000} {
		b.Run(fmt.Sprintf("zones=%d", count), func(b *testing.B) {
			zones := delegationZoneManager(b, "tbotech.sk.").snapshot.Load()
			for i := 1; i < count; i++ {
				name := fmt.Sprintf("zone%d.example.", i)
				zones.zones[name] = &zoneEntry{name: name}
			}
			manager := newTestDNSEndpointManager(delegationEndpoint("demo.tbotech.sk", "NS", "ns.example.net."))
			snapshot := manager.endpoints.Load()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				zone, conflict := zones.lookup("www.demo.tbotech.sk.")
				if conflict || zone == nil || snapshot.findZoneCut(zone.name, "www.demo.tbotech.sk.") == nil {
					b.Fatal("lookup failed")
				}
			}
		})
	}
}

func BenchmarkDelegationNoCuts(b *testing.B) {
	plugin := EdgeCDNX{DNSEndpointManager: newTestDNSEndpointManager()}
	zone := &zoneEntry{name: "tbotech.sk."}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if handled, _, err := plugin.serveDelegation(request.Request{}, nil, zone); handled || err != nil {
			b.Fatal("unexpected delegation")
		}
	}
}

func TestDelegationServingDoesNotAcquireManagerLocks(t *testing.T) {
	zones := delegationZoneManager(t, "tbotech.sk.")
	endpoints := newTestDNSEndpointManager(
		delegationEndpoint("demo.tbotech.sk", "NS", "ns.example.net."),
	)
	zones.Sync.Lock()
	defer zones.Sync.Unlock()
	endpoints.Sync.Lock()
	defer endpoints.Sync.Unlock()
	msg, rcode, err := delegationQuery(t, EdgeCDNX{
		ZoneManager: zones, DNSEndpointManager: endpoints,
	}, "www.demo.tbotech.sk.", dns.TypeA)
	if err != nil || rcode != dns.RcodeSuccess || msg == nil || len(msg.Ns) != 1 {
		t.Fatalf("referral: msg=%v rcode=%d err=%v", msg, rcode, err)
	}
}
