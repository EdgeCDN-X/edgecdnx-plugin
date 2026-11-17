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

func TestZoneSnapshotRetainsOldView(t *testing.T) {
	manager := &ZoneManager{Sync: &sync.RWMutex{}, sources: map[string]*api.Zone{
		"parent": {Spec: api.ZoneSpec{Zone: "example.com", Email: "noc.example.com."}},
	}}
	config := ZoneManagerConfiguration{Origins: []string{"."}, SOA: "ns.example.com."}
	manager.rebuildZonesLocked(config)
	old := manager.snapshot.Load()
	manager.sources["child"] = &api.Zone{Spec: api.ZoneSpec{Zone: "child.example.com", Email: "noc.example.com."}}
	manager.rebuildZonesLocked(config)
	current := manager.snapshot.Load()
	oldZone, _ := old.lookup("www.child.example.com.")
	newZone, _ := current.lookup("www.child.example.com.")
	if oldZone.name != "example.com." || newZone.name != "child.example.com." {
		t.Fatal("zone update must preserve old snapshots and publish the closest child")
	}
	delete(manager.sources, "child")
	manager.rebuildZonesLocked(config)
	zone, _ := manager.snapshot.Load().lookup("www.child.example.com.")
	if zone.name != "example.com." {
		t.Fatal("zone deletion must restore parent lookup")
	}
}

func TestZoneSnapshotConcurrentPublication(t *testing.T) {
	manager := &ZoneManager{Sync: &sync.RWMutex{}, sources: map[string]*api.Zone{
		"parent": {Spec: api.ZoneSpec{Zone: "example.com", Email: "noc.example.com."}},
	}}
	config := ZoneManagerConfiguration{Origins: []string{"."}, SOA: "ns.example.com."}
	manager.rebuildZonesLocked(config)
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 100; j++ {
				zone, conflict := manager.snapshot.Load().lookup("www.example.com.")
				if conflict || zone == nil || zone.soa == nil {
					t.Error("incomplete published zone snapshot")
					return
				}
			}
		}()
	}
	for i := 0; i < 20; i++ {
		manager.rebuildZonesLocked(config)
	}
	readers.Wait()
}

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
	if len(manager.snapshot.Load().zones) != 2 {
		t.Fatal("same Kubernetes resource names must not collide across namespaces")
	}
	previous := second.DeepCopy()
	second.Spec.Zone = first.Spec.Zone
	handlers.UpdateFunc(object(previous), object(second))
	if !manager.hasConflict("www.a.example.com") || len(manager.snapshot.Load().zones["a.example.com."].records) != 0 {
		t.Fatal("duplicate DNS zones must fail closed")
	}
	handlers.DeleteFunc(cache.DeletedFinalStateUnknown{Obj: object(first)})
	if manager.hasConflict("www.a.example.com") || len(manager.snapshot.Load().zones["a.example.com."].records) == 0 {
		t.Fatal("deleting one duplicate must restore the remaining Zone")
	}
	if manager.snapshot.Load().zones["a.example.com."].records[0].Header().Rrtype != dns.TypeSOA {
		t.Fatal("restored Zone has no SOA")
	}
}

func TestZoneSnapshotLookupAncestors(t *testing.T) {
	root := &zoneEntry{name: "."}
	child := &zoneEntry{name: "child.example.com."}
	snapshot := &zoneSnapshot{zones: map[string]*zoneEntry{
		".":                  root,
		"example.com.":       {name: "example.com.", conflict: true},
		"child.example.com.": child,
	}}
	zone, conflict := snapshot.lookup("www.child.example.com.")
	if zone != child || !conflict {
		t.Fatal("an ancestor conflict must not be hidden by a more specific served zone")
	}
	zone, conflict = snapshot.lookup("other.net.")
	if zone != root || conflict {
		t.Fatal("lookup must fall back to a served root zone")
	}
}
