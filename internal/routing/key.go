package routing

import (
	"net"
	"net/netip"
	"strconv"
	"strings"
)

func BuildRoutingKey(clientIP net.IP, qname string, queryType uint16) string {
	name := strings.ToLower(strings.TrimSpace(qname))
	if name != "" && !strings.HasSuffix(name, ".") {
		name += "."
	}
	return NormalizeClientSubnet(clientIP) + "|" + name + "|" + strconv.FormatUint(uint64(queryType), 10)
}

func NormalizeClientSubnet(ip net.IP) string {
	if ipv4 := ip.To4(); ipv4 != nil {
		var raw [4]byte
		copy(raw[:], ipv4)
		return netip.PrefixFrom(netip.AddrFrom4(raw), 24).Masked().String()
	}
	if ipv6 := ip.To16(); ipv6 != nil {
		var raw [16]byte
		copy(raw[:], ipv6)
		return netip.PrefixFrom(netip.AddrFrom16(raw), 56).Masked().String()
	}
	return "unknown"
}
