package edgecdnxplugin

// Ready implements the ready.Readiness interface, once this flips to true CoreDNS
// assumes this plugin is ready for queries; it is not checked again.
func (e EdgeCDNX) Ready() bool {
	return e.ZoneManager.Informer.HasSynced() && e.ServiceManager.Informer.HasSynced() && e.PrefixListRoutingManager.Informer.HasSynced()
}
