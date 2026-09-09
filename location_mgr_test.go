package edgecdnxplugin

import (
	"sync"
	"testing"
	"time"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

func newTestLocationManager(t *testing.T, locations ...infrastructurev1alpha1.Location) LocationManager {
	t.Helper()

	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &unstructured.Unstructured{}, time.Hour, cache.Indexers{
		"byParent": func(obj any) ([]string, error) {
			location, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return []string{}, nil
			}

			parent, ok, err := unstructured.NestedString(location.Object, "spec", "parent")
			if err != nil || !ok || parent == "" {
				return []string{}, err
			}

			return []string{parent}, nil
		},
	})

	for _, location := range locations {
		object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&location)
		if err != nil {
			t.Fatalf("failed to convert location %s to unstructured: %v", location.Name, err)
		}
		if err := informer.GetIndexer().Add(&unstructured.Unstructured{Object: object}); err != nil {
			t.Fatalf("failed to add location %s to informer indexer: %v", location.Name, err)
		}
	}

	return LocationManager{
		Informer:  informer,
		Sync:      &sync.RWMutex{},
		Locations: make(map[string]infrastructurev1alpha1.Location),
	}
}

func newTestLocation(name string, labels map[string]string, parent string, nodeName string) infrastructurev1alpha1.Location {
	location := infrastructurev1alpha1.Location{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: infrastructurev1alpha1.LocationSpec{
			Parent: parent,
		},
		Status: infrastructurev1alpha1.LocationStatus{
			NodeStatus: make(map[string]infrastructurev1alpha1.NodeInstanceStatus),
		},
	}

	if nodeName != "" {
		location.Spec.NodeGroups = []infrastructurev1alpha1.NodeGroupSpec{
			{
				Name:   "cache-a",
				Flavor: "default",
				Nodes: []infrastructurev1alpha1.NodeSpec{
					{
						Name: nodeName,
						Ipv4: "192.0.2.1",
					},
				},
			},
		}
	}

	return location
}

func TestApplyHashSkipsChildLocationsThatDoNotMatchRouteSelector(t *testing.T) {
	parentLocation := newTestLocation("parent", map[string]string{"tenant": "acme"}, "", "")
	childLocation := newTestLocation("child", map[string]string{"tenant": "globex"}, "parent", "child-node")
	manager := newTestLocationManager(t, childLocation)

	_, err := manager.ApplyHash(&parentLocation, "example.com.", HashFilters{
		Qtype: 1,
		RouteSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"tenant": "acme"},
		},
		EndpointName: "endpoint-acme",
	})
	if err == nil {
		t.Fatal("expected no healthy nodes after non-matching child location was skipped")
	}
}

func TestApplyHashIncludesChildLocationsWhenRouteSelectorIsNil(t *testing.T) {
	parentLocation := newTestLocation("parent", nil, "", "")
	childLocation := newTestLocation("child", nil, "parent", "child-node")
	manager := newTestLocationManager(t, childLocation)

	node, err := manager.ApplyHash(&parentLocation, "example.com.", HashFilters{
		Qtype: 1,
	})
	if err != nil {
		t.Fatalf("expected child location node to be available without route selector: %v", err)
	}
	if node.LocationName != "child" {
		t.Fatalf("node.LocationName = %q, want %q", node.LocationName, "child")
	}
	if node.Node.Name != "child-node" {
		t.Fatalf("node.Node.Name = %q, want %q", node.Node.Name, "child-node")
	}
}

func TestApplyHashMatchesNodeGroupLabelsInsteadOfName(t *testing.T) {
	location := newTestLocation("location", nil, "", "node")
	location.Spec.NodeGroups[0].Name = "legacy-cache-name"
	location.Spec.NodeGroups[0].Labels = map[string]string{"cache": "cache-a"}

	manager := newTestLocationManager(t)
	node, err := manager.ApplyHash(&location, "example.com.", HashFilters{
		Qtype: 1,
		RouteSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"cache": "cache-a"},
		},
	})
	if err != nil {
		t.Fatalf("expected node group labels to match: %v", err)
	}
	if node.Node.Name != "node" {
		t.Fatalf("node.Name = %q, want %q", node.Node.Name, "node")
	}
}

func TestMatchesNodeGroupLabelsAllowsLabelsOnlyOnNodeGroup(t *testing.T) {
	location := newTestLocation("location", nil, "", "node")
	location.Spec.NodeGroups[0].Labels = map[string]string{"tenant": "acme"}
	manager := newTestLocationManager(t, location)
	manager.Locations[location.Name] = location

	if !manager.MatchesNodeGroupLabels("location", &metav1.LabelSelector{
		MatchLabels: map[string]string{"tenant": "acme"},
	}) {
		t.Fatal("expected selector to match node group labels")
	}
}

func TestMatchesNodeGroupLabelsCombinesLocationAndNodeGroupLabels(t *testing.T) {
	location := newTestLocation("location", map[string]string{"tenant": "acme"}, "", "node")
	location.Spec.NodeGroups[0].Labels = map[string]string{"cache": "cache-a"}
	manager := newTestLocationManager(t, location)
	manager.Locations[location.Name] = location

	if !manager.MatchesNodeGroupLabels("location", &metav1.LabelSelector{
		MatchLabels: map[string]string{
			"tenant": "acme",
			"cache":  "cache-a",
		},
	}) {
		t.Fatal("expected selector to match combined location and node group labels")
	}
}
