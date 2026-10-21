package edgecdnxplugin

import (
	"sync"
	"testing"

	api "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/miekg/dns"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

func TestZoneNamespaceIdentityAndDNSConflicts(t *testing.T) {
	manager := &ZoneManager{Sync: &sync.RWMutex{}}
	handlers := manager.zoneEventHandlers(ZoneManagerConfiguration{Origins: []string{"."}, SOA: "ns.example.com."})
	object := func(zone *api.Zone) *unstructured.Unstructured {
		t.Helper()
		encoded, err := runtime.DefaultUnstructuredConverter.ToUnstructured(zone)
		if err != nil {
			t.Fatal(err)
		}
		return &unstructured.Unstructured{Object: encoded}
	}
	first := &api.Zone{ObjectMeta: metav1.ObjectMeta{Name: "same-name", Namespace: "tenant-a"}, Spec: api.ZoneSpec{Zone: "a.example.com", Email: "noc.a.example.com."}}
	second := first.DeepCopy()
	second.Namespace = "tenant-b"
	second.Spec.Zone = "b.example.com"
	handlers.AddFunc(object(first))
	handlers.AddFunc(object(second))
	if len(manager.Records) != 2 {
		t.Fatal("same Kubernetes resource names must not collide across namespaces")
	}
	previous := second.DeepCopy()
	second.Spec.Zone = first.Spec.Zone
	handlers.UpdateFunc(object(previous), object(second))
	if !manager.hasConflict("www.a.example.com") || len(manager.Records["a.example.com."]) != 0 {
		t.Fatal("duplicate DNS zones must fail closed")
	}
	handlers.DeleteFunc(cache.DeletedFinalStateUnknown{Obj: object(first)})
	if manager.hasConflict("www.a.example.com") || len(manager.Records["a.example.com."]) == 0 {
		t.Fatal("deleting one duplicate must restore the remaining Zone")
	}
	if manager.Records["a.example.com."][0].Header().Rrtype != dns.TypeSOA {
		t.Fatal("restored Zone has no SOA")
	}
}
