package edgecdnxplugin

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin/pkg/log"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type ServiceManagerConfiguration struct {
	Namespace string
}

type ServiceManager struct {
	fac      dynamicinformer.DynamicSharedInformerFactory
	Informer cache.SharedIndexInformer
	Sync     *sync.RWMutex
	Services []infrastructurev1alpha1.Service
}

func (sm *ServiceManager) GetService(qname string) (infrastructurev1alpha1.Service, error) {
	log.Debug(fmt.Sprintf("edgecdnx: Looking up Service for Q %s", qname))

	sm.Sync.RLock()
	defer sm.Sync.RUnlock()

	for _, service := range sm.Services {
		if fmt.Sprintf("%s.", service.Spec.Domain) == qname {
			return service, nil
		}

		for _, alias := range service.Spec.HostAliases {
			if fmt.Sprintf("%s.", alias.Name) == qname {
				return service, nil
			}
		}
	}

	return infrastructurev1alpha1.Service{}, fmt.Errorf("Could not get Service Cache type for %s", qname)
}

func NewServiceManager(factory dynamicinformer.DynamicSharedInformerFactory, config ServiceManagerConfiguration) *ServiceManager {
	sm := &ServiceManager{
		fac:  factory,
		Sync: &sync.RWMutex{},
	}

	serviceInformer := factory.ForResource(schema.GroupVersionResource{
		Group:    infrastructurev1alpha1.SchemeGroupVersion.Group,
		Version:  infrastructurev1alpha1.SchemeGroupVersion.Version,
		Resource: "services",
	}).Informer()

	log.Infof("edgecdnx: Watching Services in namespace %s", config.Namespace)

	serviceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			s_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnx: expected Service object, got %T", obj)
				return
			}

			temp, err := json.Marshal(s_raw.Object)
			if err != nil {
				log.Errorf("edgecdnx: failed to marshal Service object: %v", err)
				return
			}
			service := &infrastructurev1alpha1.Service{}
			err = json.Unmarshal(temp, service)
			if err != nil {
				log.Errorf("edgecdnx: failed to unmarshal Service object: %v", err)
				return
			}

			sm.Sync.Lock()
			defer sm.Sync.Unlock()
			sm.Services = append(sm.Services, *service)
			log.Infof("edgecdnx: Added Service %s", service.Name)
		},
		UpdateFunc: func(oldObj, newObj any) {
			s_new_raw, ok := newObj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnx: expected Service object, got %T", s_new_raw)
				return
			}

			temp, err := json.Marshal(s_new_raw.Object)
			if err != nil {
				log.Errorf("edgecdnx: failed to marshal Service object: %v", err)
				return
			}
			newService := &infrastructurev1alpha1.Service{}
			err = json.Unmarshal(temp, newService)
			if err != nil {
				log.Errorf("edgecdnx: failed to unmarshal Service object: %v", err)
				return
			}

			sm.Sync.Lock()
			defer sm.Sync.Unlock()
			for i, service := range sm.Services {
				if service.Name == newService.Name {
					sm.Services[i] = *newService
					break
				}
			}
			log.Infof("edgecdnx: Updated Service %s", newService.Name)
		},
		DeleteFunc: func(obj any) {
			s_raw, ok := obj.(*unstructured.Unstructured)
			if !ok {
				log.Errorf("edgecdnx: expected Service object, got %T", obj)
				return
			}

			temp, err := json.Marshal(s_raw.Object)
			if err != nil {
				log.Errorf("edgecdnx: failed to marshal Service object: %v", err)
				return
			}
			service := &infrastructurev1alpha1.Service{}
			err = json.Unmarshal(temp, service)
			if err != nil {
				log.Errorf("edgecdnx: failed to unmarshal Service object: %v", err)
				return
			}

			sm.Sync.Lock()
			defer sm.Sync.Unlock()

			sm.Services = slices.DeleteFunc(sm.Services, func(s infrastructurev1alpha1.Service) bool {
				return s.Name == service.Name
			})
			log.Infof("edgecdnx: Deleted Service %s", service.Name)
		},
	})

	sm.Informer = serviceInformer

	return sm
}
