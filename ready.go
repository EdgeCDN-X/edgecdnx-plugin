package edgecdnxplugin

import "k8s.io/client-go/tools/cache"

// Ready implements the ready.Readiness interface, once this flips to true CoreDNS
// assumes this plugin is ready for queries; it is not checked again.
func (e EdgeCDNX) Ready() bool {
	return informerAndHandlerSynced(e.ZoneManager.Informer, e.ZoneManager.HandlerRegistration) &&
		informerAndHandlerSynced(e.DNSEndpointManager.Informer, e.DNSEndpointManager.HandlerRegistration) &&
		informerAndHandlerSynced(e.PrefixListRoutingManager.Informer, e.PrefixListRoutingManager.HandlerRegistration) &&
		informerAndHandlerSynced(e.LocationManager.Informer, e.LocationManager.HandlerRegistration)
}

func informerAndHandlerSynced(informer cache.SharedIndexInformer, registration cache.ResourceEventHandlerRegistration) bool {
	return informer != nil && registration != nil && informer.HasSynced() && registration.HasSynced()
}
