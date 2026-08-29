package edgecdnxplugin

import (
	"fmt"
	"sync"
	"testing"
	"time"

	adaptivev1alpha1 "github.com/EdgeCDN-X/edgecdnx-plugin/api/v1alpha1"
	"github.com/EdgeCDN-X/edgecdnx-plugin/internal/routing"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

func TestNodeQualityManagerPublishesImmutableSnapshots(t *testing.T) {
	now := time.Date(2026, time.August, 28, 12, 0, 0, 0, time.UTC)
	manager := testNodeQualityManager(now)
	first := testNodeQualityObject(t, "sydney", "edge-a", "Healthy", 70, now)
	manager.upsert(first, nil)

	snapshot1 := manager.Snapshot()
	quality, found := snapshot1.Lookup("sydney", "edge-a")
	if !found || quality.EffectiveWeight != 70 || snapshot1.Version != 1 || snapshot1.UpdatedAt != now {
		t.Fatalf("unexpected first snapshot: found=%v quality=%+v snapshot=%+v", found, quality, snapshot1)
	}

	second := testNodeQualityObject(t, "sydney", "edge-a", "Degraded", 20, now.Add(time.Minute))
	manager.upsert(second, first)
	snapshot2 := manager.Snapshot()
	updated, found := snapshot2.Lookup("sydney", "edge-a")
	if !found || updated.EffectiveWeight != 20 || updated.State != "Degraded" || snapshot2.Version != 2 {
		t.Fatalf("unexpected updated snapshot: found=%v quality=%+v snapshot=%+v", found, updated, snapshot2)
	}
	old, _ := snapshot1.Lookup("sydney", "edge-a")
	if old.EffectiveWeight != 70 || snapshot1.Version != 1 {
		t.Fatalf("published snapshot was mutated: %+v", old)
	}
}

func TestNodeQualityManagerDeleteSupportsPointerTombstone(t *testing.T) {
	manager := testNodeQualityManager(time.Now())
	object := testNodeQualityObject(t, "sydney", "edge-a", "Healthy", 70, time.Now())
	manager.upsert(object, nil)
	manager.remove(&cache.DeletedFinalStateUnknown{Key: "default/edge-a", Obj: object})
	if manager.Snapshot().Len() != 0 || manager.Snapshot().Version != 2 {
		t.Fatalf("delete did not publish an empty snapshot: %+v", manager.Snapshot())
	}
}

func TestNodeQualityManagerDisabledIsReadyAndFailOpen(t *testing.T) {
	manager := NewNodeQualityManager(nil, false)
	if !manager.Ready() || manager.Enabled() || manager.Snapshot().Len() != 0 {
		t.Fatalf("disabled manager is not fail-open ready: ready=%v enabled=%v len=%d", manager.Ready(), manager.Enabled(), manager.Snapshot().Len())
	}
}

func TestNodeQualityManagerConcurrentPublishAndRead(t *testing.T) {
	manager := testNodeQualityManager(time.Now())
	var wait sync.WaitGroup
	for reader := 0; reader < 16; reader++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for i := 0; i < 2_000; i++ {
				_, _ = manager.Snapshot().Lookup("sydney", "edge-a")
			}
		}()
	}
	for writer := 0; writer < 4; writer++ {
		wait.Add(1)
		go func(writer int) {
			defer wait.Done()
			for i := 0; i < 200; i++ {
				manager.upsert(testNodeQualityObject(t, "sydney", fmt.Sprintf("edge-%d", writer), "Healthy", int32(i%100+1), time.Now()), nil)
			}
		}(writer)
	}
	wait.Wait()
	if manager.Snapshot().Len() != 4 {
		t.Fatalf("unexpected final snapshot size: %d", manager.Snapshot().Len())
	}
}

func testNodeQualityManager(now time.Time) *NodeQualityManager {
	manager := &NodeQualityManager{
		enabled: true,
		entries: make(map[string]routing.NodeQuality),
		now:     func() time.Time { return now },
	}
	manager.snapshot.Store(routing.EmptySnapshot())
	return manager
}

func testNodeQualityObject(t *testing.T, location, node, state string, weight int32, observedAt time.Time) *unstructured.Unstructured {
	t.Helper()
	observed := metav1.NewTime(observedAt)
	object := &adaptivev1alpha1.NodeQuality{
		TypeMeta: metav1.TypeMeta{APIVersion: "adaptive.edgecdnx.io/v1alpha1", Kind: "NodeQuality"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      node,
			Namespace: "default",
		},
		Spec: adaptivev1alpha1.NodeQualitySpec{Location: location, NodeName: node, Enabled: true, StaticCapacity: 100},
		Status: adaptivev1alpha1.NodeQualityStatus{
			ObservedAt:      &observed,
			State:           state,
			EffectiveWeight: weight,
		},
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		t.Fatalf("convert NodeQuality: %v", err)
	}
	return &unstructured.Unstructured{Object: raw}
}
