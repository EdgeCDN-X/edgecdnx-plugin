package edgecdnxplugin

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/ancientlore/go-avltree"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type PrefixListRoutingManagerConfiguration struct {
	Namespace string
}

type PrefixListRoutingManager struct {
	fac       dynamicinformer.DynamicSharedInformerFactory
	Informer  cache.SharedIndexInformer
	Sync      *sync.RWMutex
	RoutingV4 *avltree.Tree
	RoutingV6 *avltree.Tree
}

type PrefixTreeEntry struct {
	Location string
	Prefix   net.IPNet
}

func (p PrefixListRoutingManager) IsPrefixRouted(state request.Request) (bool, string) {
	srcIP := net.ParseIP(state.IP())

	if o := state.Req.IsEdns0(); o != nil {
		for _, s := range o.Option {
			if e, ok := s.(*dns.EDNS0_SUBNET); ok {
				srcIP = e.Address
				break
			}
		}
	}
	p.Sync.RLock()
	defer p.Sync.RUnlock()

	if srcIP.To4() != nil {
		dest := p.RoutingV4.Find(PrefixTreeEntry{
			Prefix: net.IPNet{
				IP:   srcIP,
				Mask: net.CIDRMask(32, 32),
			},
		})

		if dest != nil {
			log.Debug(fmt.Sprintf("edgecdnx: Found V4 prefix %s", dest))
			return true, dest.(PrefixTreeEntry).Location
		}
	}

	if srcIP.To16() != nil {
		dest := p.RoutingV6.Find(PrefixTreeEntry{
			Prefix: net.IPNet{
				IP:   srcIP,
				Mask: net.CIDRMask(128, 128),
			},
		})

		if dest != nil {
			log.Debug(fmt.Sprintf("edgecdnx: Found V6 prefix %s", dest))
			return true, dest.(PrefixTreeEntry).Location
		}
	}

	return false, ""
}

func NewPrefixListRoutingManager(factory dynamicinformer.DynamicSharedInformerFactory, config PrefixListRoutingManagerConfiguration) *PrefixListRoutingManager {
	prefixListMgr := &PrefixListRoutingManager{
		fac:  factory,
		Sync: &sync.RWMutex{},
		RoutingV4: avltree.New(func(a any, b any) int {
			starta := a.(PrefixTreeEntry).Prefix.IP.To4()
			enda := make(net.IP, len(starta))
			copy(enda, starta)

			for i := 0; i < len(a.(PrefixTreeEntry).Prefix.Mask); i++ {
				enda[i] |= ^a.(PrefixTreeEntry).Prefix.Mask[i]
			}

			startb := b.(PrefixTreeEntry).Prefix.IP.To4()
			endb := make(net.IP, len(startb))
			copy(endb, startb)

			for i := 0; i < len(b.(PrefixTreeEntry).Prefix.Mask); i++ {
				endb[i] |= ^b.(PrefixTreeEntry).Prefix.Mask[i]
			}

			if bytes.Compare(enda, startb) == -1 {
				return -1
			}

			if bytes.Compare(starta, endb) == 1 {
				return 1
			}

			return 0
		}, 0),
		RoutingV6: avltree.New(func(a any, b any) int {
			starta := a.(PrefixTreeEntry).Prefix.IP.To16()
			enda := make(net.IP, len(starta))
			copy(enda, starta)
			for i := 0; i < len(a.(PrefixTreeEntry).Prefix.Mask); i++ {
				enda[i] |= ^a.(PrefixTreeEntry).Prefix.Mask[i]
			}

			startb := b.(PrefixTreeEntry).Prefix.IP.To16()
			endb := make(net.IP, len(startb))
			copy(endb, startb)
			for i := 0; i < len(b.(PrefixTreeEntry).Prefix.Mask); i++ {
				endb[i] |= ^b.(PrefixTreeEntry).Prefix.Mask[i]
			}

			if bytes.Compare(enda, startb) == -1 {
				return -1
			}

			if bytes.Compare(starta, endb) == 1 {
				return 1
			}

			return 0
		}, 0),
	}

	prefixListInformer := prefixListMgr.fac.ForResource(schema.GroupVersionResource{
		Group:    infrastructurev1alpha1.SchemeGroupVersion.Group,
		Version:  infrastructurev1alpha1.SchemeGroupVersion.Version,
		Resource: "prefixlists",
	}).Informer()

	prefixListInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			p_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxprefixlist: expected PrefixList object, got %T", p_raw)
				return
			}
			temp, err := json.Marshal(p_raw.Object)
			if err != nil {
				log.Errorf("edgecdnxprefixlist: failed to marshal PrefixList object: %v", err)
				return
			}
			prefixList := &infrastructurev1alpha1.PrefixList{}
			err = json.Unmarshal(temp, prefixList)
			if err != nil {
				log.Errorf("edgecdnxprefixlist: failed to unmarshal PrefixList object: %v", err)
				return
			}
			prefixListMgr.Sync.Lock()
			defer prefixListMgr.Sync.Unlock()
			for _, v := range prefixList.Spec.Prefix.V4 {
				_, ipnet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", v.Address, v.Size))
				if err != nil {
					log.Error(fmt.Sprintf("parse cidr error %v", err))
					return
				}
				log.Debug(fmt.Sprintf("Adding V4 CIDR %s/%d\n", v.Address, v.Size))
				prefixListMgr.RoutingV4.Add(PrefixTreeEntry{
					Location: prefixList.Spec.Destination,
					Prefix:   *ipnet,
				})
			}
			for _, v := range prefixList.Spec.Prefix.V6 {
				_, ipnet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", v.Address, v.Size))
				if err != nil {
					log.Error(fmt.Sprintf("parse cidr error %v", err))
					return
				}
				log.Debug(fmt.Sprintf("Adding V6 CIDR %s/%d\n", v.Address, v.Size))
				prefixListMgr.RoutingV6.Add(PrefixTreeEntry{
					Location: prefixList.Spec.Destination,
					Prefix:   *ipnet,
				})
			}
			log.Infof("edgecdnxprefixlist: Added PrefixList %s", prefixList.Name)
		},
		UpdateFunc: func(oldObj, newObj any) {
			p_old_raw, ok := oldObj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxprefixlist: expected PrefixList object, got %T", p_old_raw)
				return
			}
			temp, err := json.Marshal(p_old_raw.Object)
			if err != nil {
				log.Errorf("edgecdnxprefixlist: failed to marshal PrefixList object: %v", err)
				return
			}
			oldPrefixList := &infrastructurev1alpha1.PrefixList{}
			err = json.Unmarshal(temp, oldPrefixList)
			if err != nil {
				log.Errorf("edgecdnxprefixlist: failed to unmarshal PrefixList object: %v", err)
				return
			}

			p_new_raw, ok := newObj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxprefixlist: expected PrefixList object, got %T", p_new_raw)
				return
			}
			temp, err = json.Marshal(p_new_raw.Object)
			if err != nil {
				log.Errorf("edgecdnxprefixlist: failed to marshal PrefixList object: %v", err)
				return
			}
			newPrefixList := &infrastructurev1alpha1.PrefixList{}
			err = json.Unmarshal(temp, newPrefixList)
			if err != nil {
				log.Errorf("edgecdnxprefixlist: failed to unmarshal PrefixList object: %v", err)
				return
			}

			prefixListMgr.Sync.Lock()
			defer prefixListMgr.Sync.Unlock()
			for _, v := range oldPrefixList.Spec.Prefix.V4 {
				_, ipnet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", v.Address, v.Size))
				if err != nil {
					log.Error(fmt.Sprintf("parse cidr error %v", err))
					return
				}
				log.Debug(fmt.Sprintf("Removing V4 CIDR %s/%d\n", v.Address, v.Size))
				prefixListMgr.RoutingV4.Remove(PrefixTreeEntry{
					Location: oldPrefixList.Spec.Destination,
					Prefix:   *ipnet,
				})
			}
			for _, v := range oldPrefixList.Spec.Prefix.V6 {
				_, ipnet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", v.Address, v.Size))
				if err != nil {
					log.Error(fmt.Sprintf("parse cidr error %v", err))
					return
				}
				log.Debug(fmt.Sprintf("Removing V6 CIDR %s/%d\n", v.Address, v.Size))
				prefixListMgr.RoutingV6.Remove(PrefixTreeEntry{
					Location: oldPrefixList.Spec.Destination,
					Prefix:   *ipnet,
				})
			}
			for _, v := range newPrefixList.Spec.Prefix.V4 {
				_, ipnet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", v.Address, v.Size))
				if err != nil {
					log.Error(fmt.Sprintf("parse cidr error %v", err))
					return
				}
				log.Debug(fmt.Sprintf("Adding V4 CIDR %s/%d\n", v.Address, v.Size))
				prefixListMgr.RoutingV4.Add(PrefixTreeEntry{
					Location: newPrefixList.Spec.Destination,
					Prefix:   *ipnet,
				})
			}
			for _, v := range newPrefixList.Spec.Prefix.V6 {
				_, ipnet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", v.Address, v.Size))
				if err != nil {
					log.Error(fmt.Sprintf("parse cidr error %v", err))
					return
				}
				log.Debug(fmt.Sprintf("Adding V6 CIDR %s/%d\n", v.Address, v.Size))
				prefixListMgr.RoutingV6.Add(PrefixTreeEntry{
					Location: newPrefixList.Spec.Destination,
					Prefix:   *ipnet,
				})
			}
			log.Infof("edgecdnxprefixlist: Updated PrefixList %s", newPrefixList.Name)
		},
		DeleteFunc: func(obj any) {
			p_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnxprefixlist: expected PrefixList object, got %T", p_raw)
				return
			}
			temp, err := json.Marshal(p_raw.Object)
			if err != nil {
				log.Errorf("edgecdnxprefixlist: failed to marshal PrefixList object: %v", err)
				return
			}
			prefixList := &infrastructurev1alpha1.PrefixList{}
			err = json.Unmarshal(temp, prefixList)
			if err != nil {
				log.Errorf("edgecdnxprefixlist: failed to unmarshal PrefixList object: %v", err)
				return
			}

			prefixListMgr.Sync.Lock()
			defer prefixListMgr.Sync.Unlock()
			for _, v := range prefixList.Spec.Prefix.V4 {
				_, ipnet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", v.Address, v.Size))
				if err != nil {
					log.Error(fmt.Sprintf("parse cidr error %v", err))
					return
				}
				log.Debug(fmt.Sprintf("Removing V4 CIDR %s/%d\n", v.Address, v.Size))
				prefixListMgr.RoutingV4.Remove(PrefixTreeEntry{
					Location: prefixList.Spec.Destination,
					Prefix:   *ipnet,
				})
			}
			for _, v := range prefixList.Spec.Prefix.V6 {
				_, ipnet, err := net.ParseCIDR(fmt.Sprintf("%s/%d", v.Address, v.Size))
				if err != nil {
					log.Error(fmt.Sprintf("parse cidr error %v", err))
					return
				}
				log.Debug(fmt.Sprintf("Removing V6 CIDR %s/%d\n", v.Address, v.Size))
				prefixListMgr.RoutingV6.Remove(PrefixTreeEntry{
					Location: prefixList.Spec.Destination,
					Prefix:   *ipnet,
				})
			}
			log.Infof("edgecdnxprefixlist: Deleted PrefixList %s", prefixList.Name)
		},
	})

	prefixListMgr.Informer = prefixListInformer

	return prefixListMgr
}
