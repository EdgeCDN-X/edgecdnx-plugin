package edgecdnxplugin

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	adaptivev1alpha1 "github.com/EdgeCDN-X/edgecdnx-plugin/api/v1alpha1"
	"github.com/EdgeCDN-X/edgecdnx-plugin/internal/routing"
	"github.com/coredns/coredns/plugin/pkg/log"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

var nodeQualityGVR = schema.GroupVersionResource{Group: "adaptive.edgecdnx.io", Version: "v1alpha1", Resource: "nodequalities"}

type NodeQualityManager struct {
	Informer cache.SharedIndexInformer
	enabled  bool
	mu       sync.Mutex
	entries  map[string]routing.NodeQuality
	version  uint64
	snapshot atomic.Pointer[routing.Snapshot]
	now      func() time.Time
}

func NewNodeQualityManager(factory dynamicinformer.DynamicSharedInformerFactory, enabled bool) *NodeQualityManager {
	manager := &NodeQualityManager{enabled: enabled, entries: make(map[string]routing.NodeQuality), now: time.Now}
	manager.snapshot.Store(routing.EmptySnapshot())
	if !enabled {
		return manager
	}
	manager.Informer = factory.ForResource(nodeQualityGVR).Informer()
	manager.Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) { manager.upsert(obj, nil) },
		UpdateFunc: func(oldObj, newObj any) {
			manager.upsert(newObj, oldObj)
		},
		DeleteFunc: manager.remove,
	})
	return manager
}

func (m *NodeQualityManager) Ready() bool {
	return !m.enabled || (m.Informer != nil && m.Informer.HasSynced())
}

func (m *NodeQualityManager) Enabled() bool { return m.enabled }

func (m *NodeQualityManager) Snapshot() *routing.Snapshot {
	snapshot := m.snapshot.Load()
	if snapshot == nil {
		return routing.EmptySnapshot()
	}
	return snapshot
}

func (m *NodeQualityManager) upsert(obj any, oldObj any) {
	quality, err := nodeQualityFromObject(obj)
	if err != nil {
		log.Errorf("edgeroute: parse NodeQuality: %v", err)
		return
	}
	if oldObj != nil {
		old, oldErr := nodeQualityFromObject(oldObj)
		if oldErr == nil && old.State != quality.State {
			log.Infof("edgeroute: node state transition node=%s location=%s from=%s to=%s", quality.Node, quality.Location, old.State, quality.State)
		}
	}
	m.mu.Lock()
	m.entries[nodeQualityKey(quality.Location, quality.Node)] = quality
	m.publishLocked()
	m.mu.Unlock()
}

func (m *NodeQualityManager) remove(obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	} else if tombstone, ok := obj.(*cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	quality, err := nodeQualityFromObject(obj)
	if err != nil {
		log.Errorf("edgeroute: parse deleted NodeQuality: %v", err)
		return
	}
	m.mu.Lock()
	delete(m.entries, nodeQualityKey(quality.Location, quality.Node))
	m.publishLocked()
	m.mu.Unlock()
}

func (m *NodeQualityManager) publishLocked() {
	entries := make([]routing.NodeQuality, 0, len(m.entries))
	for _, quality := range m.entries {
		entries = append(entries, quality)
	}
	m.version++
	m.snapshot.Store(routing.NewSnapshot(m.version, m.now(), entries))
}

func nodeQualityFromObject(obj any) (routing.NodeQuality, error) {
	raw, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return routing.NodeQuality{}, fmt.Errorf("expected unstructured NodeQuality, got %T", obj)
	}
	var quality adaptivev1alpha1.NodeQuality
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw.Object, &quality); err != nil {
		return routing.NodeQuality{}, err
	}
	observedAt := time.Time{}
	if quality.Status.ObservedAt != nil {
		observedAt = quality.Status.ObservedAt.Time
	}
	return routing.NodeQuality{
		Location:        quality.Spec.Location,
		Node:            quality.Spec.NodeName,
		EffectiveWeight: quality.Status.EffectiveWeight,
		State:           quality.Status.State,
		ObservedAt:      observedAt,
	}, nil
}

func nodeQualityKey(location, node string) string { return location + "\x00" + node }
