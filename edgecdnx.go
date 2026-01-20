package edgecdnxplugin

import (
	"context"
	"fmt"
	"net"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/metadata"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
)

// Example is an example plugin to show how to write a plugin.
type EdgeCDNX struct {
	Next                     plugin.Handler
	ZoneManager              *ZoneManager
	ServiceManager           *ServiceManager
	PrefixListRoutingManager *PrefixListRoutingManager
	LocationManager          *LocationManager
}

type EdgeCDNXResponseWriter struct {
}

func (e EdgeCDNX) GetServiceCacheType(ctx context.Context) (string, error) {
	if cacheFunc := metadata.ValueFunc(ctx, "edgecdnxservices/cache"); cacheFunc != nil {
		if serviceCache := cacheFunc(); serviceCache != "" {
			return serviceCache, nil
		}
		return "", fmt.Errorf("No service cache found")
	}
	return "", fmt.Errorf("Service cache metadata module not initialized")
}

func (e EdgeCDNX) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}
	qname := state.Name()

	// if cache is set means the service is present and configured for
	cache, err := e.GetServiceCacheType(ctx)

	if err == nil {
		// Service is configured and Qtype is A or AAAA
		prefixRouted, locationName := e.PrefixListRoutingManager.IsPrefixRouted(state)

		if !prefixRouted || !e.LocationManager.HasCacheType(cache, locationName) {
			locationName, err = e.LocationManager.PerformGeoLookup(ctx, cache)
			if err != nil {
				log.Infof("edgecdnx: GeoLookup failed: %v", err)
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}
		}

		location, ok := e.LocationManager.Locations[locationName]
		if !ok {
			log.Error(fmt.Sprintf("edgecdnxgeolookup: Location not found - %v", err))
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}

		log.Debug(fmt.Sprintf("edgecdnxgeolookup: Routing to location: %s\n", location.Name))

		filter := HashFilters{
			Cache: cache,
			Qtype: state.Req.Question[0].Qtype,
		}

		node, err := e.LocationManager.ApplyHash(&location, state.Name(), filter)
		if err != nil {
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: Hashing error - %v", err))

			for _, fbLoc := range location.Spec.FallbackLocations {
				fbLocation, ok := e.LocationManager.Locations[fbLoc]
				log.Debug(fmt.Sprintf("edgecdnxgeolookup: Falling back to location %s", fbLoc))
				if !ok {
					log.Error(fmt.Sprintf("edgecdnxgeolookup: Fallback location %s not found", fbLoc))
					continue
				}

				node, err = e.LocationManager.ApplyHash(&fbLocation, state.Name(), filter)
				if err == nil {
					log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback to location %s successful", fbLoc))
					break
				}
				log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback to location %s failed - %v", fbLoc, err))
			}

			if err != nil {
				log.Error(fmt.Sprintf("edgecdnxgeolookup: No nodes found for request %s - %v", state.Name(), err))
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}
		}

		m := new(dns.Msg)
		m.SetReply(r)
		m.Authoritative = true

		srcIP := net.ParseIP(state.IP())
		if o := state.Req.IsEdns0(); o != nil {
			for _, s := range o.Option {
				if e, ok := s.(*dns.EDNS0_SUBNET); ok {
					srcIP = e.Address
					break
				}
			}
		}

		log.Debug(fmt.Sprintf("edgecdnxgeolookup: srcIP %s", srcIP))

		if state.Req.Question[0].Qtype == dns.TypeA {
			res := new(dns.A)
			// TODO, Perhaps move this block to one of the managers and use the TTL value from there
			res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 60}
			parsed := net.ParseIP(node.Ipv4)
			res.A = parsed
			m.Answer = append(m.Answer, res)
		}

		if state.Req.Question[0].Qtype == dns.TypeAAAA {
			res := new(dns.AAAA)
			// TODO, Perhaps move this block to one of the managers and use the TTL value from there
			res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: 60}
			parsed := net.ParseIP(node.Ipv6)
			res.AAAA = parsed
			m.Answer = append(m.Answer, res)
		}

		state.SizeAndDo(m)
		m = state.Scrub(m)
		err = w.WriteMsg(m)

		if err != nil {
			log.Error(fmt.Sprintf("edgecdnxgeolookup: DNS response write failure %v", err))
			return dns.RcodeServerFailure, err
		}
		log.Info(fmt.Sprintf("edgecdnxgeolookup: DNS response %s %v", state.Name(), m.Answer))
		return dns.RcodeSuccess, nil

	}

	e.ZoneManager.Sync.RLock()
	defer e.ZoneManager.Sync.RUnlock()

	zone := plugin.Zones(e.ZoneManager.Zones).Matches(qname)

	if zone == "" {
		return dns.RcodeServerFailure, nil
	} else {
		m := new(dns.Msg)
		m.SetReply(r)
		m.Authoritative = true

		nxdomain := true
		var soa dns.RR
		for _, r := range e.ZoneManager.Records[zone] {
			if r.Header().Rrtype == dns.TypeSOA && soa == nil {
				soa = r
			}
			if r.Header().Name == qname {
				nxdomain = false
				if r.Header().Rrtype == state.QType() {
					m.Answer = append(m.Answer, r)
				}
			}
		}

		// handle NXDOMAIN, NODATA and normal response here.
		if nxdomain {
			m.Rcode = dns.RcodeNameError
			if soa != nil {
				m.Ns = []dns.RR{soa}
			}
			w.WriteMsg(m)
			return dns.RcodeSuccess, nil
		}

		if len(m.Answer) == 0 {
			if soa != nil {
				m.Ns = []dns.RR{soa}
			}
		}

		w.WriteMsg(m)
		return dns.RcodeSuccess, nil
	}
}

func (g EdgeCDNX) setServiceMeta(ctx context.Context, service *infrastructurev1alpha1.Service) context.Context {
	metadata.SetValueFunc(ctx, g.Name()+"/customer", func() string {
		return fmt.Sprintf("%d", service.Spec.Customer.Id)
	})

	metadata.SetValueFunc(ctx, g.Name()+"/cache", func() string {
		return service.Spec.Cache
	})

	return ctx
}

func (g EdgeCDNX) Metadata(ctx context.Context, state request.Request) context.Context {
	if state.QType() != dns.TypeA && state.QType() != dns.TypeAAAA {
		return ctx
	}

	g.ServiceManager.Sync.RLock()
	defer g.ServiceManager.Sync.RUnlock()
	for i := range g.ServiceManager.Services {
		service := g.ServiceManager.Services[i]
		if fmt.Sprintf("%s.", service.Spec.Domain) == state.Name() {
			return g.setServiceMeta(ctx, &service)
		}

		for _, ha := range service.Spec.HostAliases {
			if fmt.Sprintf("%s.", ha.Name) == state.Name() {
				return g.setServiceMeta(ctx, &service)
			}
		}
	}

	return ctx
}

// Name implements the Handler interface.
func (e EdgeCDNX) Name() string { return "EdgeCDNX" }

// ResponsePrinter wrap a dns.ResponseWriter and will write example to standard output when WriteMsg is called.
type ResponsePrinter struct {
	dns.ResponseWriter
}

// NewResponsePrinter returns ResponseWriter.
func NewResponsePrinter(w dns.ResponseWriter) *ResponsePrinter {
	return &ResponsePrinter{ResponseWriter: w}
}

// WriteMsg calls the underlying ResponseWriter's WriteMsg method and prints "example" to standard output.
func (r *ResponsePrinter) WriteMsg(res *dns.Msg) error {
	return r.ResponseWriter.WriteMsg(res)
}
