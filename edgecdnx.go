package edgecdnxplugin

import (
	"context"
	"fmt"
	"net"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin"
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

func (e EdgeCDNX) BuildNodeReponse(node infrastructurev1alpha1.NodeSpec, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}

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

	log.Debug(fmt.Sprintf("edgecdnx: Request Source IP %s", srcIP))

	if state.Req.Question[0].Qtype == dns.TypeA {
		res := new(dns.A)
		res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: e.LocationManager.Config.RecrodTTL}
		parsed := net.ParseIP(node.Ipv4)
		res.A = parsed
		m.Answer = append(m.Answer, res)
	}

	if state.Req.Question[0].Qtype == dns.TypeAAAA {
		res := new(dns.AAAA)
		res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: e.LocationManager.Config.RecrodTTL}
		parsed := net.ParseIP(node.Ipv6)
		res.AAAA = parsed
		m.Answer = append(m.Answer, res)
	}

	state.SizeAndDo(m)
	m = state.Scrub(m)
	err := w.WriteMsg(m)

	if err != nil {
		log.Error(fmt.Sprintf("edgecdnx: DNS response write failure %v", err))
		return dns.RcodeServerFailure, err
	}
	log.Debug(fmt.Sprintf("edgecdnx: DNS response %s %v", state.Name(), m.Answer))
	return dns.RcodeSuccess, nil
}

func (e EdgeCDNX) ServeDNS(ctx context.Context, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}
	qname := state.Name()

	// If requesting A or AAAA, we do the routing
	if state.QType() == dns.TypeA || state.QType() == dns.TypeAAAA {
		service, err := e.ServiceManager.GetService(qname)

		if err == nil {
			//Cache type found
			prefixRouted, locationName := e.PrefixListRoutingManager.IsPrefixRouted(state)

			if !prefixRouted || !e.LocationManager.HasCacheType(service.Spec.Cache, locationName) {
				locationName, err = e.LocationManager.PerformGeoLookup(ctx, service.Spec.Cache)
				if err != nil {
					log.Errorf("edgecdnx: GeoLookup failed: %v", err)
					return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
				}
			}

			location, err := e.LocationManager.GetLocationByName(locationName)
			if err != nil {
				log.Error(fmt.Sprintf("edgecdnxgeolookup: Location not found - %v", err))
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}

			log.Debug(fmt.Sprintf("edgecdnxgeolookup: Routing to location: %s\n", location.Name))

			filter := HashFilters{
				Cache: service.Spec.Cache,
				Qtype: state.Req.Question[0].Qtype,
			}

			node, err := e.LocationManager.ApplyHash(&location, state.Name(), filter)
			if err != nil {
				log.Debug(fmt.Sprintf("edgecdnxgeolookup: Hashing error - %v", err))

				for _, fbLoc := range location.Spec.FallbackLocations {
					fallBackLocation, err := e.LocationManager.GetLocationByName(fbLoc)
					log.Debug(fmt.Sprintf("edgecdnxgeolookup: Falling back to location %s", fbLoc))
					if err != nil {
						log.Error(fmt.Sprintf("edgecdnxgeolookup: Fallback location %s not found", fbLoc))
						continue
					}
					node, err := e.LocationManager.ApplyHash(&fallBackLocation, state.Name(), filter)
					if err == nil {
						log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback to location %s successful", fbLoc))
						return e.BuildNodeReponse(node, w, r)
					}
					log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback to location %s failed - %v", fbLoc, err))
				}

				log.Error(fmt.Sprintf("edgecdnxgeolookup: No nodes found for request %s - %v", state.Name(), err))
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}

			return e.BuildNodeReponse(node, w, r)
		} else {
			log.Debug(fmt.Sprintf("edgecdnxgeolookup: No service found for %s, passing to next plugin", qname))
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}
	}

	e.ZoneManager.Sync.RLock()
	defer e.ZoneManager.Sync.RUnlock()

	zone := plugin.Zones(e.ZoneManager.Zones).Matches(qname)

	if zone == "" {
		return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
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

// Name implements the Handler interface.
func (e EdgeCDNX) Name() string { return "edgecdnx" }

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
