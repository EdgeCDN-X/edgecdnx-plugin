package edgecdnxplugin

import (
	"context"
	"fmt"
	"maps"
	"net"
	"regexp"
	"strings"

	infrastructurev1alpha1 "github.com/EdgeCDN-X/edgecdnx-controller/api/v1alpha1"
	"github.com/coredns/coredns/plugin"
	"github.com/coredns/coredns/plugin/pkg/log"
	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
	grpcmetadata "google.golang.org/grpc/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var nodeLocationEndpointPattern = regexp.MustCompile(`^([^.]+)\.([^.]+)\.node\.(.+)\.$`)

type ResponseType string

const (
	CNAME  ResponseType = "CNAME"
	A_AAAA ResponseType = "A_AAAA"
)

// Example is an example plugin to show how to write a plugin.
type EdgeCDNX struct {
	Next                     plugin.Handler
	ZoneManager              *ZoneManager
	DNSEndpointManager       *DNSEndpointManager
	PrefixListRoutingManager *PrefixListRoutingManager
	LocationManager          *LocationManager
	DNSResponseType          ResponseType
	GRPCResponseType         ResponseType
}

type EdgeCDNXResponseWriter struct {
}

func findNodeInLocation(location infrastructurev1alpha1.Location, nodeName string, selector *metav1.LabelSelector) (infrastructurev1alpha1.NodeSpec, error) {
	for _, nodeGroup := range location.Spec.NodeGroups {
		fullLabels := make(map[string]string)
		maps.Copy(fullLabels, location.Labels)
		maps.Copy(fullLabels, nodeGroup.Labels)
		if !matchesLabelSelector(fullLabels, selector) {
			continue
		}
		for _, node := range nodeGroup.Nodes {
			if node.Name == nodeName {
				return node, nil
			}
		}
	}

	return infrastructurev1alpha1.NodeSpec{}, fmt.Errorf("node %s not found in location %s", nodeName, location.Name)
}

func (e EdgeCDNX) BuildNodeReponse(node infrastructurev1alpha1.NodeSpec, locationName string, responseType ResponseType, ttl uint32, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}

	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true

	if responseType == CNAME {
		res := new(dns.CNAME)
		res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: ttl}
		res.Target = dns.Fqdn(fmt.Sprintf("%s.%s.node.%s", node.Name, locationName, state.Name()))
		m.Answer = append(m.Answer, res)
	} else {
		if state.Req.Question[0].Qtype == dns.TypeA {
			res := new(dns.A)
			res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: ttl}
			parsed := net.ParseIP(node.Ipv4)
			res.A = parsed
			m.Answer = append(m.Answer, res)
		}

		if state.Req.Question[0].Qtype == dns.TypeAAAA {
			res := new(dns.AAAA)
			res.Hdr = dns.RR_Header{Name: state.Name(), Rrtype: dns.TypeAAAA, Class: dns.ClassINET, Ttl: ttl}
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

func (e EdgeCDNX) BuildSimpleResponse(dnsEndpoint infrastructurev1alpha1.DNSEndpoint, w dns.ResponseWriter, r *dns.Msg) (int, error) {
	state := request.Request{W: w, Req: r}
	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true

	for _, target := range dnsEndpoint.Spec.Targets {
		record, err := dns.NewRR(fmt.Sprintf("%s %d IN %s %s", state.Name(), dnsEndpoint.Spec.RecordTTL, dnsEndpoint.Spec.RecordType, target))
		if err != nil {
			log.Errorf("edgecdnx: invalid target %q for DNSEndpoint %s: %v", target, dnsEndpoint.Name, err)
			return dns.RcodeServerFailure, err
		}
		m.Answer = append(m.Answer, record)
	}

	state.SizeAndDo(m)
	m = state.Scrub(m)
	if err := w.WriteMsg(m); err != nil {
		return dns.RcodeServerFailure, err
	}

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

	// Fetch DNS Endpoint first
	dnsEndpoint, dnsEndpointErr := e.DNSEndpointManager.GetDNSEndpoint(qname, state.QType())

	// Check if the query matches the node location endpoint pattern
	if matches := nodeLocationEndpointPattern.FindStringSubmatch(qname); len(matches) == 4 && (state.QType() == dns.TypeA || state.QType() == dns.TypeAAAA) {
		nodeName := matches[1]
		locationName := matches[2]
		endpointDNSName := dns.Fqdn(matches[3])

		dnsEndpoint, err := e.DNSEndpointManager.GetDNSEndpoint(endpointDNSName, state.QType())
		if err != nil {
			log.Debugf("edgecdnx: DNSEndpoint for %s not found for node request %s", endpointDNSName, qname)
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}
		if !strings.EqualFold(dnsEndpoint.Spec.RoutingPolicy, "Geolocation") {
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}

		location, err := e.LocationManager.GetLocationByName(locationName)
		if err != nil {
			log.Debug(fmt.Sprintf("edgecdnx: location %s not found for node request %s", locationName, qname))
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}

		node, err := findNodeInLocation(location, nodeName, dnsEndpoint.Spec.RouteSelector)
		if err != nil {
			log.Debug(fmt.Sprintf("edgecdnx: %v", err))
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}

		return e.BuildNodeReponse(node, location.Name, A_AAAA, endpointTTL(dnsEndpoint, e.LocationManager.Config.RecrodTTL), w, r)
	}

	// Main Routing Component
	if dnsEndpointErr == nil {
		targetLocation := ""
		switch strings.ToLower(dnsEndpoint.Spec.RoutingPolicy) {

		case "simple":
			// Simple routing returns as is. No fallbacks
			return e.BuildSimpleResponse(dnsEndpoint, w, r)
		case "roundrobin":
			locations := e.LocationManager.FindMatchingTargets(ctx, dnsEndpoint.Spec.RouteSelector, dnsEndpoint.Name)
			if len(locations) == 0 {
				log.Debugf("edgecdnx: No matching locations found for roundrobin routing of DNSEndpoint %s", dnsEndpoint.Name)
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}
			targetIdx := e.DNSEndpointManager.GetNext(qname, state.QType(), len(locations))
			targetLocation = locations[targetIdx]
		case "weighted":
			locations := e.LocationManager.FindMatchingTargetsWithWeights(ctx, dnsEndpoint.Spec.RouteSelector, dnsEndpoint.Name)
			if len(locations) == 0 {
				log.Debugf("edgecdnx: No matching locations found for weighted routing of DNSEndpoint %s", dnsEndpoint.Name)
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}
			targetIdx := e.DNSEndpointManager.GetWeightedNext(qname, state.QType(), locations)
			targetLocation = locations[targetIdx].Name
		case "failover":
			// Selects Primary target. Primary target fails over to the next if unavailable
			if len(dnsEndpoint.Spec.Targets) == 0 {
				log.Debugf("edgecdnx: No primary target specified for failover routing of DNSEndpoint %s", dnsEndpoint.Name)
				return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
			}
			targetLocation = dnsEndpoint.Spec.Targets[0]
		case "geolocation":
			// Geolocation for checks available Prefix routed endpoints
			prefixRouted, matchedLocation := e.PrefixListRoutingManager.IsPrefixRouted(state, dnsEndpoint.Spec.RouteSelector)

			if !prefixRouted {
				geoLookupLocation, geoLookupErr := e.LocationManager.PerformGeoLookup(ctx, dnsEndpoint.Spec.RouteSelector, dnsEndpoint.Name)
				if geoLookupErr != nil {
					log.Errorf("edgecdnx: GeoLookup failed: %v", geoLookupErr)
					return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
				}
				targetLocation = geoLookupLocation
			} else {
				targetLocation = matchedLocation
			}
		default:
			log.Warningf("edgecdnx: unsupported routing policy %q for DNSEndpoint %s", dnsEndpoint.Spec.RoutingPolicy, dnsEndpoint.Name)
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}

		location, err := e.LocationManager.GetLocationByName(targetLocation)
		if err != nil {
			log.Error(fmt.Sprintf("edgecdnx: Location not found - %v", err))
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}

		log.Debug(fmt.Sprintf("edgecdnx: Routing to location: %s\n", location.Name))

		// Filter filters on additional node labels if routeSelector is present
		filter := HashFilters{
			Qtype:         state.Req.Question[0].Qtype,
			RouteSelector: dnsEndpoint.Spec.RouteSelector,
			EndpointName:  dnsEndpoint.Name,
		}

		node, err := e.LocationManager.ApplyHash(&location, state.Name(), filter)
		if err != nil {
			log.Debug(fmt.Sprintf("edgecdnx: Hashing error - %v", err))

			if location.Spec.Parent != "" {
				// If a parent location is specified, attempt to route to the parent location
				parentLocation, err := e.LocationManager.GetLocationByName(location.Spec.Parent)
				log.Debug(fmt.Sprintf("edgecdnx: Falling back to parent location %s", location.Spec.Parent))
				if err != nil {
					log.Error(fmt.Sprintf("edgecdnx: Fallback location %s not found", location.Spec.Parent))
					return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
				}
				if !e.LocationManager.MatchesNodeGroupLabels(parentLocation.Name, dnsEndpoint.Spec.RouteSelector) {
					log.Debugf("edgecdnx: Parent location %s does not match routeSelector for DNSEndpoint %s", parentLocation.Name, dnsEndpoint.Name)
					return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
				}

				node, err := e.LocationManager.ApplyHash(&parentLocation, state.Name(), filter)
				if err == nil {
					log.Debug(fmt.Sprintf("edgecdnx: Fallback to location %s successful", node.LocationName))
					return e.BuildNodeReponse(node.Node, node.LocationName, responseType, endpointTTL(dnsEndpoint, e.LocationManager.Config.RecrodTTL), w, r)
				}
				log.Debug(fmt.Sprintf("edgecdnx: Fallback to location %s failed - %v", parentLocation.Name, err))

				// Continue down the fallback chain with the parent location as the new location
				location = parentLocation
			}

			for _, fbLoc := range location.Spec.FallbackLocations {
				fallBackLocation, err := e.LocationManager.GetLocationByName(fbLoc)
				log.Debug(fmt.Sprintf("edgecdnx: Falling back to location %s", fbLoc))
				if err != nil {
					log.Error(fmt.Sprintf("edgecdnx: Fallback location %s not found", fbLoc))
					continue
				}
				if !e.LocationManager.MatchesNodeGroupLabels(fallBackLocation.Name, dnsEndpoint.Spec.RouteSelector) {
					log.Debugf("edgecdnx: Fallback location %s does not match routeSelector for DNSEndpoint %s", fbLoc, dnsEndpoint.Name)
					continue
				}
				node, err := e.LocationManager.ApplyHash(&fallBackLocation, state.Name(), filter)
				if err == nil {
					log.Debug(fmt.Sprintf("edgecdnx: Fallback to location %s successful", node.LocationName))
					return e.BuildNodeReponse(node.Node, node.LocationName, responseType, endpointTTL(dnsEndpoint, e.LocationManager.Config.RecrodTTL), w, r)
				}
				log.Debug(fmt.Sprintf("edgecdnx: Fallback to location %s failed - %v", fbLoc, err))
			}

			log.Error(fmt.Sprintf("edgecdnx: No nodes found for request %s - %v", state.Name(), err))
			return plugin.NextOrFailure(e.Name(), e.Next, ctx, w, r)
		}

		return e.BuildNodeReponse(node.Node, node.LocationName, responseType, endpointTTL(dnsEndpoint, e.LocationManager.Config.RecrodTTL), w, r)
	}

	// Local Zone Handling
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
			// Handle SOA records
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

func endpointTTL(dnsEndpoint infrastructurev1alpha1.DNSEndpoint, fallback uint32) uint32 {
	if dnsEndpoint.Spec.RecordTTL <= 0 {
		return fallback
	}
	return uint32(dnsEndpoint.Spec.RecordTTL)
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
