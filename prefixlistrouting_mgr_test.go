package edgecdnxplugin

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type testResponseWriter struct {
	remoteAddr net.Addr
}

func (t testResponseWriter) LocalAddr() net.Addr                  { return &net.UDPAddr{} }
func (t testResponseWriter) RemoteAddr() net.Addr                 { return t.remoteAddr }
func (t testResponseWriter) WriteMsg(*dns.Msg) error              { return nil }
func (t testResponseWriter) Write([]byte) (int, error)            { return 0, nil }
func (t testResponseWriter) Close() error                         { return nil }
func (t testResponseWriter) TsigStatus() error                    { return nil }
func (t testResponseWriter) TsigTimersOnly(bool)                  {}
func (t testResponseWriter) Hijack()                              {}
func (t testResponseWriter) MsgAcceptFunc(dns.MsgAcceptFunc)      {}
func (t testResponseWriter) MsgInvalidFunc(dns.MsgInvalidFunc)    {}
func (t testResponseWriter) DecorateWriter(dns.DecorateWriter)    {}
func (t testResponseWriter) SetWriteDeadline(time.Time) error     { return nil }
func (t testResponseWriter) SetReadDeadline(time.Time) error      { return nil }
func (t testResponseWriter) SetDeadline(time.Time) error          { return nil }
func (t testResponseWriter) SetTsigSecret(map[string]string)      {}
func (t testResponseWriter) SetMsgInvalidFunc(dns.MsgInvalidFunc) {}

func newTestPrefixListRoutingManager() PrefixListRoutingManager {
	return PrefixListRoutingManager{
		Sync:          &sync.RWMutex{},
		RoutingTables: make(map[string]*PrefixRoutingTable),
	}
}

func addTestPrefixRoute(t *testing.T, manager PrefixListRoutingManager, labels map[string]string, cidr string, location string) {
	t.Helper()

	_, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		t.Fatalf("failed to parse test CIDR %s: %v", cidr, err)
	}

	routingTable := manager.routingTableFor(labels)
	entry := PrefixTreeEntry{
		Location: location,
		Prefix:   *ipnet,
	}

	if ipnet.IP.To4() != nil {
		routingTable.RoutingV4.Add(entry)
		return
	}

	routingTable.RoutingV6.Add(entry)
}

func newTestRequest(sourceIP string) request.Request {
	return request.Request{
		W: testResponseWriter{
			remoteAddr: &net.UDPAddr{IP: net.ParseIP(sourceIP), Port: 5353},
		},
		Req: new(dns.Msg),
	}
}

func TestPrefixListLabelKeySortsLabels(t *testing.T) {
	labels := map[string]string{
		"tenant": "acme",
		"env":    "prod",
	}

	if got, want := prefixListLabelKey(labels), "env=prod,tenant=acme"; got != want {
		t.Fatalf("prefixListLabelKey() = %q, want %q", got, want)
	}
}

func TestIsPrefixRoutedSelectsMatchingRoutingTable(t *testing.T) {
	manager := newTestPrefixListRoutingManager()
	addTestPrefixRoute(t, manager, map[string]string{"tenant": "acme"}, "10.0.0.0/8", "acme-location")
	addTestPrefixRoute(t, manager, map[string]string{"tenant": "globex"}, "10.0.0.0/8", "globex-location")

	routeSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"tenant": "acme"},
	}

	routed, location := manager.IsPrefixRouted(newTestRequest("10.1.2.3"), routeSelector)
	if !routed {
		t.Fatal("expected request to be prefix routed")
	}
	if location != "acme-location" {
		t.Fatalf("location = %q, want %q", location, "acme-location")
	}
}

func TestIsPrefixRoutedWithoutSelectorUsesMostSpecificPrefixAcrossAllTables(t *testing.T) {
	manager := newTestPrefixListRoutingManager()
	addTestPrefixRoute(t, manager, map[string]string{"tenant": "acme"}, "10.0.0.0/8", "broad-location")
	addTestPrefixRoute(t, manager, map[string]string{"tenant": "globex"}, "10.1.0.0/16", "specific-location")

	routed, location := manager.IsPrefixRouted(newTestRequest("10.1.2.3"), nil)
	if !routed {
		t.Fatal("expected request to be prefix routed")
	}
	if location != "specific-location" {
		t.Fatalf("location = %q, want %q", location, "specific-location")
	}
}

func TestIsPrefixRoutedWithoutSelectorMatchesUnlabeledRoutingTable(t *testing.T) {
	manager := newTestPrefixListRoutingManager()
	addTestPrefixRoute(t, manager, nil, "10.1.0.0/16", "unlabeled-location")

	routed, location := manager.IsPrefixRouted(newTestRequest("10.1.2.3"), nil)
	if !routed {
		t.Fatal("expected request to be prefix routed")
	}
	if location != "unlabeled-location" {
		t.Fatalf("location = %q, want %q", location, "unlabeled-location")
	}
	if _, ok := manager.RoutingTables[""]; !ok {
		t.Fatal("expected unlabeled prefix route to use the empty routing table key")
	}
}

func TestIsPrefixRoutedWithSelectorIgnoresUnlabeledRoutingTable(t *testing.T) {
	manager := newTestPrefixListRoutingManager()
	addTestPrefixRoute(t, manager, nil, "10.1.0.0/16", "unlabeled-location")

	routeSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"tenant": "acme"},
	}

	routed, location := manager.IsPrefixRouted(newTestRequest("10.1.2.3"), routeSelector)
	if routed {
		t.Fatalf("expected request not to be routed, got location %q", location)
	}
}

func TestIsPrefixRoutedIgnoresNonMatchingRoutingTables(t *testing.T) {
	manager := newTestPrefixListRoutingManager()
	addTestPrefixRoute(t, manager, map[string]string{"tenant": "globex"}, "10.1.0.0/16", "globex-location")

	routeSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"tenant": "acme"},
	}

	routed, location := manager.IsPrefixRouted(newTestRequest("10.1.2.3"), routeSelector)
	if routed {
		t.Fatalf("expected request not to be routed, got location %q", location)
	}
}

func TestIsPrefixRoutedWithMatchExpressionInMatchesMultipleRoutingTables(t *testing.T) {
	manager := newTestPrefixListRoutingManager()
	addTestPrefixRoute(t, manager, map[string]string{"env": "prod", "tenant": "acme"}, "10.0.0.0/8", "prod-location")
	addTestPrefixRoute(t, manager, map[string]string{"env": "staging", "tenant": "acme"}, "10.1.0.0/16", "staging-location")
	addTestPrefixRoute(t, manager, map[string]string{"env": "dev", "tenant": "acme"}, "10.1.2.0/24", "dev-location")

	routeSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{"tenant": "acme"},
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "env",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"prod", "staging"},
			},
		},
	}

	routed, location := manager.IsPrefixRouted(newTestRequest("10.1.2.3"), routeSelector)
	if !routed {
		t.Fatal("expected request to be prefix routed")
	}
	if location != "staging-location" {
		t.Fatalf("location = %q, want %q", location, "staging-location")
	}
}

func TestIsPrefixRoutedWithMatchExpressionsExistsAndNotInMatchesMultipleRoutingTables(t *testing.T) {
	manager := newTestPrefixListRoutingManager()
	addTestPrefixRoute(t, manager, map[string]string{"tier": "edge", "region": "eu"}, "10.0.0.0/8", "eu-location")
	addTestPrefixRoute(t, manager, map[string]string{"tier": "edge", "region": "us"}, "10.1.0.0/16", "us-location")
	addTestPrefixRoute(t, manager, map[string]string{"tier": "edge", "region": "test"}, "10.1.2.0/24", "test-location")

	routeSelector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "tier",
				Operator: metav1.LabelSelectorOpExists,
			},
			{
				Key:      "region",
				Operator: metav1.LabelSelectorOpNotIn,
				Values:   []string{"test"},
			},
		},
	}

	routed, location := manager.IsPrefixRouted(newTestRequest("10.1.2.3"), routeSelector)
	if !routed {
		t.Fatal("expected request to be prefix routed")
	}
	if location != "us-location" {
		t.Fatalf("location = %q, want %q", location, "us-location")
	}
}
