package edgecdnxplugin

import (
	"context"
	"fmt"
	"net"
	"regexp"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	grpcmetadata "google.golang.org/grpc/metadata"
)

var nodeLocationServicePattern = regexp.MustCompile(`^([^.]+)\.([^.]+)\.node\.(.+)\.$`)

type ResponseType string

const (
	CNAME  ResponseType = "CNAME"
	A_AAAA ResponseType = "A_AAAA"
)

// Example is an example plugin to show how to write a plugin.
type EdgeCDNX struct {
	Next                     plugin.Handler
	ZoneManager              *ZoneManager
	ServiceManager           *ServiceManager
	PrefixListRoutingManager *PrefixListRoutingManager
	LocationManager          *LocationManager
	DNSResponseType          ResponseType
	GRPCResponseType         ResponseType
}

type EdgeCDNXResponseWriter struct {
}

func findNodeInLocation(location infrastructurev1alpha1.Location, cache string, nodeName string) (infrastructurev1alpha1.NodeSpec, error) {
	for _, nodeGroup := range location.Spec.NodeGroups {
		if nodeGroup.Name != cache {
			continue
		}
		for _, node := range nodeGroup.Nodes {
			if node.Name == nodeName {
				return node, nil
			}
		}
	}

	return infrastructurev1alpha1.NodeSpec{}, fmt.Errorf("node %s not found in location %s for cache %s", nodeName, location.Name, cache)
}

func (e EdgeCDNX) BuildNodeReponse(node infrastructurev1alpha1.NodeSpec, locationName string, responseType ResponseType, w dns.ResponseWriter, r *dns.Msg) (int, error) {
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

	if responseType == CNAME {
		res := new(dns.CNAME)
		res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: e.LocationManager.Config.RecrodTTL}
		res.Target = dns.Fqdn(fmt.Sprintf("%s.%s.node.%s", node.Name, locationName, state.Name()))
		m.Answer = append(m.Answer, res)
	} else {
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
	responseType := e.DNSResponseType

	md, ok := grpcmetadata.FromIncomingContext(ctx)
	if ok {
		log.Debug(fmt.Sprintf("edgecdnx: gRPC metadata: %v", md))
		responseType = e.GRPCResponseType
	}

	// If requesting A or AAAA, we do the routing
	if state.QType() == dns.TypeA || state.QType() == dns.TypeAAAA {
		// Check if the query matches the node location service pattern
		if matches := nodeLocationServicePattern.FindStringSubmatch(qname); len(matches) == 4 {
			nodeName := matches[1]
			locationName := matches[2]
			serviceName := dns.Fqdn(matches[3])

			service, err := e.ServiceManager.GetService(serviceName)
			if err != nil {
				log.Debug(fmt.Sprintf("edgecdnx: service %s not found for node request %s", serviceName, qname))
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}

			location, err := e.LocationManager.GetLocationByName(locationName)
			if err != nil {
				log.Debug(fmt.Sprintf("edgecdnx: location %s not found for node request %s", locationName, qname))
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}

			// Ensure the location matches the service's route selector
			if !matchesLabelSelector(location.Labels, service.Spec.RouteSelector) {
				log.Debug(fmt.Sprintf("edgecdnx: location %s does not match route selector for service %s", location.Name, serviceName))
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}

			node, err := findNodeInLocation(location, service.Spec.Cache, nodeName)
			if err != nil {
				log.Debug(fmt.Sprintf("edgecdnx: %v", err))
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}

			return e.BuildNodeReponse(node, location.Name, A_AAAA, w, r)
		}

		// Standard request, find service based on qname and route to correct location based on geo lookup and prefix list routing
		service, err := e.ServiceManager.GetService(qname)

		if err == nil {
			//Cache type found
			prefixRouted, locationName := e.PrefixListRoutingManager.IsPrefixRouted(state, service)
			if prefixRouted {
				location, locationErr := e.LocationManager.GetLocationByName(locationName)
				if locationErr != nil || !matchesLabelSelector(location.Labels, service.Spec.RouteSelector) {
					prefixRouted = false
				}
			}

			if !prefixRouted || !e.LocationManager.HasCacheType(service.Spec.Cache, locationName) {
				locationName, err = e.LocationManager.PerformGeoLookup(ctx, service, service.Spec.Cache)
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
				Cache:         service.Spec.Cache,
				Qtype:         state.Req.Question[0].Qtype,
				RouteSelector: service.Spec.RouteSelector,
				ServiceName:   service.Name,
			}

			node, err := e.LocationManager.ApplyHash(&location, state.Name(), filter)
			if err != nil {
				log.Debug(fmt.Sprintf("edgecdnxgeolookup: Hashing error - %v", err))

				if location.Spec.Parent != "" {
					// If a parent location is specified, attempt to route to the parent location
					parentLocation, err := e.LocationManager.GetLocationByName(location.Spec.Parent)
					log.Debug(fmt.Sprintf("edgecdnxgeolookup: Falling back to parent location %s", location.Spec.Parent))
					if err != nil {
						log.Error(fmt.Sprintf("edgecdnxgeolookup: Fallback location %s not found", location.Spec.Parent))
						return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
					}
					if !matchesLabelSelector(parentLocation.Labels, service.Spec.RouteSelector) {
						log.Debugf("edgecdnxgeolookup: Parent location %s does not match routeSelector for service %s", parentLocation.Name, service.Name)
						return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
					}

					node, err := e.LocationManager.ApplyHash(&parentLocation, state.Name(), filter)
					if err == nil {
						log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback to location %s successful", node.LocationName))
						return e.BuildNodeReponse(node.Node, node.LocationName, responseType, w, r)
					}
					log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback to location %s failed - %v", parentLocation.Name, err))

					// Continue down the fallback chain with the parent location as the new location
					location = parentLocation
				}

				for _, fbLoc := range location.Spec.FallbackLocations {
					fallBackLocation, err := e.LocationManager.GetLocationByName(fbLoc)
					log.Debug(fmt.Sprintf("edgecdnxgeolookup: Falling back to location %s", fbLoc))
					if err != nil {
						log.Error(fmt.Sprintf("edgecdnxgeolookup: Fallback location %s not found", fbLoc))
						continue
					}
					if !matchesLabelSelector(fallBackLocation.Labels, service.Spec.RouteSelector) {
						log.Debugf("edgecdnxgeolookup: Fallback location %s does not match routeSelector for service %s", fbLoc, service.Name)
						continue
					}
					node, err := e.LocationManager.ApplyHash(&fallBackLocation, state.Name(), filter)
					if err == nil {
						log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback to location %s successful", node.LocationName))
						return e.BuildNodeReponse(node.Node, node.LocationName, responseType, w, r)
					}
					log.Debug(fmt.Sprintf("edgecdnxgeolookup: Fallback to location %s failed - %v", fbLoc, err))
				}

				log.Error(fmt.Sprintf("edgecdnxgeolookup: No nodes found for request %s - %v", state.Name(), err))
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}

			return e.BuildNodeReponse(node.Node, node.LocationName, responseType, w, r)
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
