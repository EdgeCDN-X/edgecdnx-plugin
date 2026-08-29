package edgecdnxplugin

import (
	"fmt"
	"testing"
	"time"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/EdgeCDN-X/edgecdnx-plugin/internal/routing"
	"github.com/miekg/dns"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/dynamicinformer"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestLocationManagerUsesDynamicWeights(t *testing.T) {
	location := testLocation("sydney",
		testNode("edge-a", "192.0.2.10", "2001:db8::10"),
		testNode("edge-b", "192.0.2.20", "2001:db8::20"),
		testNode("edge-c", "192.0.2.30", "2001:db8::30"),
	)
	manager := testLocationManager(100, []routing.NodeQuality{
		{Location: "sydney", Node: "edge-a", EffectiveWeight: 70, State: "Healthy"},
		{Location: "sydney", Node: "edge-b", EffectiveWeight: 20, State: "Degraded"},
		{Location: "sydney", Node: "edge-c", EffectiveWeight: 10, State: "Recovering"},
	})

	counts := map[string]int{}
	const samples = 50_000
	for i := 0; i < samples; i++ {
		selected, err := manager.ApplyHash(&location, fmt.Sprintf("client-%d|video.example.|1", i), HashFilters{Cache: "hls", Qtype: dns.TypeA})
		if err != nil {
			t.Fatalf("select node: %v", err)
		}
		counts[selected.Node.Name]++
	}
	for node, limits := range map[string][2]float64{
		"edge-a": {.68, .72},
		"edge-b": {.18, .22},
		"edge-c": {.08, .12},
	} {
		share := float64(counts[node]) / samples
		if share < limits[0] || share > limits[1] {
			t.Fatalf("%s share %.4f outside [%.2f, %.2f]: %#v", node, share, limits[0], limits[1], counts)
		}
	}
}

func TestLocationManagerExcludesEjectedDisabledAndZeroWeight(t *testing.T) {
	location := testLocation("sydney",
		testNode("healthy", "192.0.2.10", "2001:db8::10"),
		testNode("ejected", "192.0.2.20", "2001:db8::20"),
		testNode("disabled", "192.0.2.30", "2001:db8::30"),
		testNode("zero", "192.0.2.40", "2001:db8::40"),
	)
	manager := testLocationManager(100, []routing.NodeQuality{
		{Location: "sydney", Node: "healthy", EffectiveWeight: 1, State: "Healthy"},
		{Location: "sydney", Node: "ejected", EffectiveWeight: 100, State: "Ejected"},
		{Location: "sydney", Node: "disabled", EffectiveWeight: 100, State: "Disabled"},
		{Location: "sydney", Node: "zero", EffectiveWeight: 0, State: "Healthy"},
	})
	for i := 0; i < 10_000; i++ {
		selected, err := manager.ApplyHash(&location, fmt.Sprintf("key-%d", i), HashFilters{Cache: "hls", Qtype: dns.TypeA})
		if err != nil || selected.Node.Name != "healthy" {
			t.Fatalf("unsafe node selected: node=%s err=%v", selected.Node.Name, err)
		}
	}
}

func TestLocationManagerFallsOpenToStaticWeightsWithoutNodeQuality(t *testing.T) {
	location := testLocation("sydney",
		testNode("edge-a", "192.0.2.10", "2001:db8::10"),
		testNode("edge-b", "192.0.2.20", "2001:db8::20"),
	)
	manager := testLocationManagerWithoutQuality(50)
	seen := map[string]bool{}
	for i := 0; i < 1_000; i++ {
		selected, err := manager.ApplyHash(&location, fmt.Sprintf("key-%d", i), HashFilters{Cache: "hls", Qtype: dns.TypeA})
		if err != nil {
			t.Fatalf("static fail-open selection: %v", err)
		}
		seen[selected.Node.Name] = true
	}
	if !seen["edge-a"] || !seen["edge-b"] {
		t.Fatalf("static fail-open did not use all eligible nodes: %#v", seen)
	}
}

func TestLocationManagerFiltersAddressFamilyAndActiveHealth(t *testing.T) {
	location := testLocation("sydney",
		testNode("ipv4", "192.0.2.10", ""),
		testNode("ipv6", "", "2001:db8::20"),
		testNode("unhealthy-v4", "192.0.2.30", "2001:db8::30"),
	)
	location.Status.NodeStatus = map[string]infrastructurev1alpha1.NodeInstanceStatus{
		"unhealthy-v4": {Conditions: []infrastructurev1alpha1.NodeCondition{{Type: infrastructurev1alpha1.IPV4HealthCheckSuccessful, Status: false}}},
	}
	manager := testLocationManagerWithoutQuality(100)

	v4, err := manager.ApplyHash(&location, "v4", HashFilters{Cache: "hls", Qtype: dns.TypeA})
	if err != nil || v4.Node.Name != "ipv4" {
		t.Fatalf("IPv4 filter selected %q: %v", v4.Node.Name, err)
	}
	v6, err := manager.ApplyHash(&location, "v6", HashFilters{Cache: "hls", Qtype: dns.TypeAAAA})
	if err != nil || (v6.Node.Name != "ipv6" && v6.Node.Name != "unhealthy-v4") {
		t.Fatalf("IPv6 filter selected %q: %v", v6.Node.Name, err)
	}
}

func TestLocationManagerAllZeroWeightsReturnsNoCandidate(t *testing.T) {
	location := testLocation("sydney", testNode("edge-a", "192.0.2.10", "2001:db8::10"))
	manager := testLocationManager(100, []routing.NodeQuality{{Location: "sydney", Node: "edge-a", EffectiveWeight: 0, State: "Healthy"}})
	if _, err := manager.ApplyHash(&location, "key", HashFilters{Cache: "hls", Qtype: dns.TypeA}); err == nil {
		t.Fatal("all-zero weights did not return no-candidate for parent/fallback")
	}
}

func TestLocationManagerDeterministicBaselineIgnoresNodeQuality(t *testing.T) {
	location := testLocation("sydney",
		testNode("edge-a", "192.0.2.10", "2001:db8::10"),
		testNode("edge-b", "192.0.2.20", "2001:db8::20"),
	)
	manager := testLocationManager(100, []routing.NodeQuality{
		{Location: "sydney", Node: "edge-a", EffectiveWeight: 0, State: "Ejected"},
		{Location: "sydney", Node: "edge-b", EffectiveWeight: 100, State: "Healthy"},
	})
	manager.Config.RoutingMode = RoutingModeDeterministic

	seen := map[string]bool{}
	for i := 0; i < 1_000; i++ {
		selected, err := manager.ApplyHash(&location, fmt.Sprintf("baseline-%d", i), HashFilters{Cache: "hls", Qtype: dns.TypeA})
		if err != nil {
			t.Fatalf("deterministic baseline selection: %v", err)
		}
		seen[selected.Node.Name] = true
	}
	if !seen["edge-a"] || !seen["edge-b"] {
		t.Fatalf("baseline unexpectedly consumed NodeQuality: %#v", seen)
	}
}

func TestLocationManagerDeterministicBaselineKeepsActiveHealthFilter(t *testing.T) {
	location := testLocation("sydney",
		testNode("unhealthy", "192.0.2.10", "2001:db8::10"),
		testNode("healthy", "192.0.2.20", "2001:db8::20"),
	)
	location.Status.NodeStatus = map[string]infrastructurev1alpha1.NodeInstanceStatus{
		"unhealthy": {Conditions: []infrastructurev1alpha1.NodeCondition{{Type: infrastructurev1alpha1.IPV4HealthCheckSuccessful, Status: false}}},
	}
	manager := testLocationManagerWithoutQuality(100)
	manager.Config.RoutingMode = RoutingModeDeterministic

	for i := 0; i < 1_000; i++ {
		selected, err := manager.ApplyHash(&location, fmt.Sprintf("health-%d", i), HashFilters{Cache: "hls", Qtype: dns.TypeA})
		if err != nil || selected.Node.Name != "healthy" {
			t.Fatalf("baseline selected unhealthy node: node=%q err=%v", selected.Node.Name, err)
		}
	}
}

func TestLocationManagerStaticRendezvousIgnoresNodeQuality(t *testing.T) {
	location := testLocation("sydney",
		testNode("edge-a", "192.0.2.10", "2001:db8::10"),
		testNode("edge-b", "192.0.2.20", "2001:db8::20"),
	)
	manager := testLocationManager(100, []routing.NodeQuality{
		{Location: "sydney", Node: "edge-a", EffectiveWeight: 0, State: "Ejected"},
		{Location: "sydney", Node: "edge-b", EffectiveWeight: 100, State: "Healthy"},
	})
	manager.Config.RoutingMode = RoutingModeStaticRendezvous

	counts := map[string]int{}
	const samples = 20_000
	for i := 0; i < samples; i++ {
		selected, err := manager.ApplyHash(&location, fmt.Sprintf("static-%d", i), HashFilters{Cache: "hls", Qtype: dns.TypeA})
		if err != nil {
			t.Fatalf("static rendezvous selection: %v", err)
		}
		counts[selected.Node.Name]++
	}
	for _, node := range []string{"edge-a", "edge-b"} {
		share := float64(counts[node]) / samples
		if share < 0.47 || share > 0.53 {
			t.Fatalf("%s static share %.4f outside [0.47, 0.53]: %#v", node, share, counts)
		}
	}
}

func testLocationManager(staticWeight int32, entries []routing.NodeQuality) *LocationManager {
	manager := testLocationManagerWithoutQuality(staticWeight)
	quality := testNodeQualityManager(time.Now())
	quality.entries = make(map[string]routing.NodeQuality, len(entries))
	for _, entry := range entries {
		quality.entries[nodeQualityKey(entry.Location, entry.Node)] = entry
	}
	quality.publishLocked()
	manager.Quality = quality
	return manager
}

func testLocationManagerWithoutQuality(staticWeight int32) *LocationManager {
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	factory := dynamicinformer.NewDynamicSharedInformerFactory(client, 0)
	return NewLocationManager(factory, LocationManagerConfiguration{Namespace: "default", RecrodTTL: 60, StaticDefaultWeight: staticWeight, RoutingMode: RoutingModeAdaptive}, nil)
}

func testLocation(name string, nodes ...infrastructurev1alpha1.NodeSpec) infrastructurev1alpha1.Location {
	return infrastructurev1alpha1.Location{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: infrastructurev1alpha1.LocationSpec{
			NodeGroups: []infrastructurev1alpha1.NodeGroupSpec{{Name: "hls", Flavor: "nginx", Nodes: nodes}},
		},
	}
}

func testNode(name, ipv4, ipv6 string) infrastructurev1alpha1.NodeSpec {
	return infrastructurev1alpha1.NodeSpec{Name: name, Ipv4: ipv4, Ipv6: ipv6}
}
