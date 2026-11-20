package edgecdnxplugin

import (
	"fmt"
	"strings"

	"github.com/coredns/coredns/request"
	"github.com/miekg/dns"
)

type delegationAnswer struct {
	name string
	ns   []dns.RR
	glue []delegationGlue
	err  error
}

type delegationGlue struct {
	name    string
	records []dns.RR
	err     error
}

func (e EdgeCDNX) serveDelegation(state request.Request, zones *zoneSnapshot, zone *zoneEntry) (bool, int, error) {
	if e.DNSEndpointManager == nil {
		return false, 0, nil
	}
	snapshot := e.DNSEndpointManager.endpoints.Load()
	if snapshot == nil || len(snapshot.delegations) == 0 || zone == nil {
		return false, 0, nil
	}
	qname := state.Name()
	// DS at a locally hosted child apex belongs to the parent, if present.
	if state.QType() == dns.TypeDS && zone.name == qname {
		if offset, end := dns.NextLabel(qname, 0); !end {
			if parent, _ := zones.lookup(qname[offset:]); parent != nil {
				zone = parent
			}
		}
	}
	cut := snapshot.findZoneCut(zone.name, qname)
	if cut == nil {
		return false, 0, nil
	}
	if cut.err != nil {
		return true, dns.RcodeServerFailure, cut.err
	}
	m := new(dns.Msg)
	m.SetReply(state.Req)
	if state.QType() == dns.TypeDS && qname == cut.name {
		if zone.soa == nil {
			return true, dns.RcodeServerFailure, fmt.Errorf("zone %s has no SOA for delegation DS response", zone.name)
		}
		m.Authoritative = true
		m.Ns = []dns.RR{zone.soa}
	} else {
		// Copy slices so truncation and EDNS handling cannot modify the snapshot.
		m.Ns = append([]dns.RR(nil), cut.ns...)
		for _, glue := range cut.glue {
			if !dns.IsSubDomain(zone.name, glue.name) {
				continue
			}
			if glue.err != nil {
				return true, dns.RcodeServerFailure, glue.err
			}
			m.Extra = append(m.Extra, glue.records...)
		}
	}
	state.SizeAndDo(m)
	m = state.Scrub(m)
	if err := state.W.WriteMsg(m); err != nil {
		return true, dns.RcodeServerFailure, err
	}
	return true, dns.RcodeSuccess, nil
}

// Walking towards the apex lets outer cuts override hidden inner cuts and conflicts.
func (s *dnsEndpointSnapshot) findZoneCut(zone, qname string) *delegationAnswer {
	var cut *delegationAnswer
	for name := qname; name != zone; {
		if candidate := s.delegations[name]; candidate != nil {
			cut = candidate
		}
		offset, end := dns.NextLabel(name, 0)
		if end {
			break
		}
		name = name[offset:]
	}
	return cut
}

func (s *dnsEndpointSnapshot) buildDelegations() {
	s.delegations = make(map[string]*delegationAnswer)
	for key, endpoint := range s.endpoints {
		if key.qtype != dns.TypeNS || strings.HasPrefix(key.name, "*.") {
			continue
		}
		cut := &delegationAnswer{name: key.name}
		s.delegations[key.name] = cut
		if !strings.EqualFold(endpoint.Spec.RoutingPolicy, "Simple") || len(endpoint.Spec.Targets) == 0 {
			cut.err = fmt.Errorf("NS delegation %s must have Simple routing and at least one target", key.name)
			continue
		}
		seen := make(map[string]bool)
		for _, target := range endpoint.Spec.Targets {
			rr, err := dns.NewRR(fmt.Sprintf("%s %d IN NS %s", key.name, endpoint.Spec.RecordTTL, target))
			if err != nil {
				cut.err = fmt.Errorf("invalid NS target %q for %s: %w", target, key.name, err)
				break
			}
			ns, ok := rr.(*dns.NS)
			if !ok {
				cut.err = fmt.Errorf("invalid NS record for %s", key.name)
				break
			}
			cut.ns = append(cut.ns, ns)
			name := dns.Fqdn(strings.ToLower(ns.Ns))
			if !seen[name] {
				seen[name] = true
				cut.glue = append(cut.glue, s.compileGlue(name))
			}
		}
	}
	for key := range s.conflicts {
		if key.qtype == dns.TypeNS && !strings.HasPrefix(key.name, "*.") {
			s.delegations[key.name] = &delegationAnswer{
				name: key.name,
				err:  fmt.Errorf("%w for %s NS", errConflictingDNS, key.name),
			}
		}
	}
}

func (s *dnsEndpointSnapshot) compileGlue(name string) delegationGlue {
	glue := delegationGlue{name: name}
	for _, qtype := range []uint16{dns.TypeA, dns.TypeAAAA} {
		key := endpointKey{name: name, qtype: qtype}
		if s.conflicts[key] {
			glue.err = fmt.Errorf("%w for glue %s %s", errConflictingDNS, name, dns.TypeToString[qtype])
			return glue
		}
		address := s.endpoints[key]
		if address == nil {
			continue
		}
		if !strings.EqualFold(address.Spec.RoutingPolicy, "Simple") {
			glue.err = fmt.Errorf("glue %s %s must use Simple routing", name, dns.TypeToString[qtype])
			return glue
		}
		for _, target := range address.Spec.Targets {
			rr, err := dns.NewRR(fmt.Sprintf("%s %d IN %s %s", name, address.Spec.RecordTTL, dns.TypeToString[qtype], target))
			if err != nil {
				glue.err = fmt.Errorf("invalid glue target %q for %s: %w", target, name, err)
				return glue
			}
			glue.records = append(glue.records, rr)
		}
	}
	return glue
}
