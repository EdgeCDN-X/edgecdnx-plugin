# EdgeCDN-X CoreDNS Plugin

`edgecdnx` is a CoreDNS plugin that routes DNS queries to EdgeCDN-X locations using Kubernetes CRDs and request metadata.

It supports:
- Dynamic `DNSEndpoint` routing for `A` and `AAAA` queries
- Direct answers from `Simple` DNSEndpoint targets
- `Simple`, `Weighted`, `Failover`, `Geolocation`, and `RoundRobin` routing policies
- DNS record types `A`, `AAAA`, `CNAME`, `TXT`, `MX`, `SRV`, and `NS`
- Configurable dynamic answers as `A`/`AAAA` or `CNAME`
- Alternate response mode for gRPC-originated requests detected from incoming context metadata
- Direct node resolution for hostnames in the form `node.location.node.service`
- Prefix-list routing (IP/CIDR to location)
- Geo metadata lookup fallback when no prefix match is found
- Hash-based node selection with health-aware filtering, spanning child locations
- Parent location fallback, followed by configured fallback locations, when a primary location has no healthy node
- Authoritative zone responses for configured Zone CRDs (SOA/NS and related behavior)

## DNSEndpoint Types

The plugin implements the `DNSEndpoint` CRD routing modes and record types defined by the API schema:

### Routing policies

| Policy | Purpose | Typical fields |
| --- | --- | --- |
| `Simple` | Return `spec.targets` directly, using `spec.recordType` and `spec.recordTTL`. | `targets`, `recordType`, `recordTTL` |
| `Weighted` | Select a matching location or node set using `routeSelector` and location weights. | `routeSelector`, `recordType`, `recordTTL` |
| `Failover` | Prefer the primary location named in `spec.targets[0]`, then fall back through healthy alternatives in that location's `fallbackLocations` or configured hierarchy. | `targets`, `routeSelector`, `recordType`, `recordTTL` |
| `Geolocation` | Resolve the best location from `routeSelector` using prefix routing or geo metadata. | `routeSelector`, `recordType`, `recordTTL` |
| `RoundRobin` | Rotate through matching locations in a deterministic round-robin sequence. | `routeSelector`, `recordType`, `recordTTL` |

### Routing policy examples

The examples below live under [examples/dnsendpoint-routing](examples/dnsendpoint-routing) and each policy has its own subfolder.

#### Node-group targeting inside a location

This pattern is useful when a single `Location` contains multiple node groups, each with different labels, and you want the DNSEndpoint to match only a subset of the node groups.

The selection is made by combining location labels with node-group labels before evaluating `routeSelector`.

See: [examples/dnsendpoint-routing/node-groups-targeting/nodegroup-targeting-dnsendpoint.yaml](examples/dnsendpoint-routing/node-groups-targeting/nodegroup-targeting-dnsendpoint.yaml)

```yaml
apiVersion: infrastructure.edgecdnx.com/v1alpha1
kind: DNSEndpoint
metadata:
  name: nodegroup-targeting-demo
  namespace: edgecdnx
spec:
  dnsName: app.example.com
  routingPolicy: Geolocation
  recordTTL: 60
  recordType: A
  routeSelector:
    matchLabels:
      edgecdnx.com/tenant: tbotech
      role: edge
---
apiVersion: infrastructure.edgecdnx.com/v1alpha1
kind: Location
metadata:
  name: us-east
  labels:
    edgecdnx.com/routing-instance: edgecdnx
    edgecdnx.com/tenant: tbotech
spec:
  geoLookup:
    weight: 100
    attributes:
      geoip/continent/code:
        weight: 1000
        values:
          - value: NA
          - value: SA
  nodeGroups:
    - name: edge
      labels:
        role: edge
      nodes:
        - name: us-east-edge-1
          ipv4: 198.51.100.41
    - name: ingress
      labels:
        role: ingress
      nodes:
        - name: us-east-ingress-1
          ipv4: 198.51.100.42
    - name: cache
      labels:
        role: cache
      nodes:
        - name: us-east-cache-1
          ipv4: 198.51.100.43
```

This makes the endpoint select only the `edge` node group in the `us-east` location, while ignoring `ingress` and `cache` nodes in the same location.

#### Simple

Use `Simple` for explicit static targets.

See: [examples/dnsendpoint-routing/simple/simple-dnsendpoint.yaml](examples/dnsendpoint-routing/simple/simple-dnsendpoint.yaml)

```yaml
apiVersion: infrastructure.edgecdnx.com/v1alpha1
kind: DNSEndpoint
metadata:
  name: simple-demo
  namespace: edgecdnx
spec:
  dnsName: app.example.com
  routingPolicy: Simple
  recordTTL: 60
  recordType: A
  targets:
    - 203.0.113.10
    - 203.0.113.11
```

#### Weighted

Use `Weighted` when response selection should prefer some matching locations over others based on weighting metadata.

See: [examples/dnsendpoint-routing/weighted/weighted-dnsendpoint.yaml](examples/dnsendpoint-routing/weighted/weighted-dnsendpoint.yaml)

```yaml
apiVersion: infrastructure.edgecdnx.com/v1alpha1
kind: DNSEndpoint
metadata:
  name: weighted-demo
  namespace: edgecdnx
spec:
  dnsName: app.example.com
  routingPolicy: Weighted
  recordTTL: 60
  recordType: A
  routeSelector:
    matchLabels:
      edgecdnx.com/tenant: tbotech
      edgecdnx.com/region: us-east
```

#### Failover

Use `Failover` to prefer a primary location and move to healthier alternatives when needed. In this implementation, the first entry in `spec.targets` is treated as a `Location` name, not a literal DNS hostname.

See: [examples/dnsendpoint-routing/failover/failover-dnsendpoint.yaml](examples/dnsendpoint-routing/failover/failover-dnsendpoint.yaml)

```yaml
apiVersion: infrastructure.edgecdnx.com/v1alpha1
kind: DNSEndpoint
metadata:
  name: failover-demo
  namespace: edgecdnx
spec:
  dnsName: app.example.com
  routingPolicy: Failover
  recordTTL: 60
  recordType: A
  targets:
    - us-east
  routeSelector:
    matchLabels:
      edgecdnx.com/tenant: tbotech
```

#### Geolocation

Use `Geolocation` to choose a location based on `routeSelector`, prefix routing, or geo metadata lookup. The `Location.spec.geoLookup.attributes` map should include CoreDNS GeoIP metadata such as `geoip/continent/code` with a weight of `1000` for the matching continent values.

See: [examples/dnsendpoint-routing/geolocation/geolocation-dnsendpoint.yaml](examples/dnsendpoint-routing/geolocation/geolocation-dnsendpoint.yaml)

```yaml
apiVersion: infrastructure.edgecdnx.com/v1alpha1
kind: DNSEndpoint
metadata:
  name: geolocation-demo
  namespace: edgecdnx
spec:
  dnsName: app.example.com
  routingPolicy: Geolocation
  recordTTL: 60
  recordType: A
  routeSelector:
    matchLabels:
      edgecdnx.com/tenant: tbotech
      edgecdnx.com/region: us-east
---
apiVersion: infrastructure.edgecdnx.com/v1alpha1
kind: Location
metadata:
  name: us-east
  labels:
    edgecdnx.com/routing-instance: edgecdnx
    edgecdnx.com/tenant: tbotech
    edgecdnx.com/region: us-east
spec:
  weight: 100
  geoLookup:
    weight: 100
    attributes:
      geoip/continent/code:
        weight: 1000
        values:
          - value: NA
          - value: SA
  fallbackLocations:
    - eu-central
  nodeGroups:
    - name: default
      nodes:
        - name: us-east-node-1
          ipv4: 198.51.100.21
```

#### RoundRobin

Use `RoundRobin` to rotate across multiple matching locations or node groups in a stable sequence.

See: [examples/dnsendpoint-routing/roundrobin/roundrobin-dnsendpoint.yaml](examples/dnsendpoint-routing/roundrobin/roundrobin-dnsendpoint.yaml)

```yaml
apiVersion: infrastructure.edgecdnx.com/v1alpha1
kind: DNSEndpoint
metadata:
  name: roundrobin-demo
  namespace: edgecdnx
spec:
  dnsName: app.example.com
  routingPolicy: RoundRobin
  recordTTL: 60
  recordType: A
  routeSelector:
    matchLabels:
      edgecdnx.com/tenant: tbotech
      edgecdnx.com/site: edge
```

### Supported record types

`DNSEndpoint.spec.recordType` supports:

- `A`
- `AAAA`
- `CNAME`
- `TXT`
- `MX`
- `SRV`
- `NS`

These values are validated by the CRD and are used when building DNS responses or validating target data for `Simple` and rule-driven endpoints.

## How It Works

For each DNS query:

1. If query type is `A` or `AAAA`:
- First check for a direct node request matching `nodename.location.node.service.`.
  - Validate that the referenced `DNSEndpoint` exists.
  - Load the referenced `Location`.
  - Find the named node inside a node group matching the endpoint's route selector.
  - Return an authoritative `A` or `AAAA` answer pointing at that node.
- Try to map `qname` and query type to a `DNSEndpoint` CRD.
- For a `Simple` endpoint, return `spec.targets` directly using `spec.recordType` and `spec.recordTTL`.
- For a `Geolocation` endpoint, determine a location using `spec.routeSelector`:
  - First from prefix routing (`PrefixList` CRDs using source IP or EDNS client subnet).
  - If prefix is missing or cache type is not available there, use geo lookup.
- If the location has active Prometheus alerts (`status.alerts` is non-empty), skip it and try fallback locations instead.
- Build the candidate node pool for the chosen location:
  - Include all nodes in node groups that match `spec.routeSelector` and are not in maintenance mode. The selector is matched against the location labels combined with the node group labels.
  - Also include nodes from **child locations** (locations whose `spec.parent` equals the chosen location), provided the child location itself is not in maintenance mode and has no active alerts.
  - For child locations, `spec.routeSelector` is matched against the child location labels combined with the child node group labels.
  - Use deterministic hash on query name to select a node.
  - Enforce IPv4/IPv6 health condition based on query type.
  - Skip nodes with active Prometheus alerts (`status.nodeStatus[node].alerts` is non-empty); try next node in hash order.
- If no healthy node is found in the candidate pool:
  - If the chosen location has a `spec.parent`, try that parent location next (same hash/filter logic).
  - If the parent also has no healthy node, continue with the parent's `spec.fallbackLocations`.
  - Otherwise (no parent), iterate the chosen location's `spec.fallbackLocations` directly.
  - Choose response mode:
    - Use `DNSResponseType` by default.
    - If gRPC incoming metadata is present in request context, use `GRPCResponseType` instead.
  - Return either:
    - `A` or `AAAA` with the selected node IP when response type is `A_AAAA`
    - `CNAME` to `node_name.location.node.original-request.` when response type is `CNAME`

2. Otherwise (or if no matching DNSEndpoint):
- Fall back to zone-authoritative behavior backed by `Zone` CRDs.
- Return:
  - `NXDOMAIN` (+ SOA in authority section) if name does not exist
  - `NODATA` style response (empty answer + SOA in authority) if name exists but no RR of requested type
  - Normal answer when matching records exist

3. If this plugin cannot answer, request is passed to the next plugin in chain.

## Dependencies and Inputs

This plugin watches EdgeCDN-X CRDs via Kubernetes dynamic informers:
- `dnsendpoints`
- `locations`
- `prefixlists`
- `zones`

A working Kubernetes client configuration is required (`controller-runtime` `GetConfigOrDie()`), typically from in-cluster config or kubeconfig in the environment.

## Corefile Configuration

Syntax:

```txt
edgecdnx [ZONES...] {
  namespace <k8s-namespace>
  soa <primary-nameserver-label>
  ns <ns-hostname> <ipv4>
  ns <ns-hostname> <ipv4>
  recordttl <seconds>
  dnsresponsetype <CNAME|A_AAAA>
  grpcresponsetype <CNAME|A_AAAA>
}
```

Directives:

| Directive | Required | Default | Description |
| --- | --- | --- | --- |
| `namespace` | Yes | none | Kubernetes namespace to watch for EdgeCDN-X CRDs. |
| `soa` | Yes | none | SOA MNAME label prefix used when crafting SOA records (`<soa>.<zone>`). |
| `ns` | Recommended (repeatable) | empty | Adds NS and NS A records for each served zone. Format: `ns <hostname> <ipv4>`. |
| `recordttl` | No | `60` | Fallback TTL for generated node answers when a DNSEndpoint does not specify one. |
| `dnsresponsetype` | No | `A_AAAA` | Allowed values: `CNAME`, `A_AAAA`. Used for normal DNS-originated dynamic responses. |
| `grpcresponsetype` | No | `CNAME` | Allowed values: `CNAME`, `A_AAAA`. Parsed and stored in plugin state. |

Notes:
- `dnsresponsetype` and `grpcresponsetype` are validated and set in plugin configuration.
- Normal dynamic geolocation routing uses `dnsresponsetype`.
- If gRPC metadata is present in the incoming request context, dynamic geolocation routing uses `grpcresponsetype` instead.
- `CNAME` responses use the target format `node_name.location.node.original-request.`.
- Direct node requests matching `nodename.location.node.service.` always return `A` or `AAAA` from the resolved node IP.
- Values are case-insensitive in Corefile input (converted to uppercase before validation).

Example:

```txt
.:53 {
  errors
  health
  ready

  edgecdnx . {
    namespace edgecdnx
    soa ns1
    ns ns1.edge.example.com. 203.0.113.10
    ns ns2.edge.example.com. 203.0.113.11
    recordttl 60
    dnsresponsetype A_AAAA
    grpcresponsetype CNAME
  }

  prometheus :9153
  forward . 1.1.1.1 8.8.8.8
  cache 30
  reload
}
```

## Build and Patch Workflow

This repository builds a patched CoreDNS that includes `edgecdnx` in the directive list and plugin registry.

### Local Build

```bash
make build
```

What this does:
- Downloads CoreDNS source tarball for configured version
- Extracts source
- Applies patch from `patches/<version>/coredns.patch`
- Updates CoreDNS version string with `-edgecdnx-<gitsha|dev>` suffix
- Builds CoreDNS binary in `coredns-<version>/coredns`

Useful targets:

```bash
make download
make extract
make patch
make clean
```

### Container Image

The provided Dockerfile expects a built `coredns` binary in repository root:

```bash
cp coredns-1.14.1/coredns ./coredns
docker build -t edgecdnx-coredns:local .
```

Runtime details:
- Runs as non-root (distroless base)
- Grants `cap_net_bind_service` to bind DNS port 53
- Exposes `53/tcp` and `53/udp`

## Readiness and Operational Notes

- Plugin readiness is tied to informer sync for Zone, Service, and PrefixList watchers.
- If informers have not synced yet, CoreDNS `ready` integration will report not ready for this plugin.
- Logging uses CoreDNS plugin logger under `edgecdnx*` prefixes.

## Error Behavior

- Invalid Corefile directive arguments return plugin startup errors.
- Invalid `dnsresponsetype`/`grpcresponsetype` values are rejected at startup.
- Direct node requests fall through to the next plugin if the referenced service, location, or node cannot be resolved.
- On routing failures (service not found, geolookup failure, no healthy nodes), plugin falls through to next handler where applicable.

## Metrics

A Prometheus counter vector is defined:
- `coredns_edgecdnx_request_count_total{server="..."}`

At present, the counter is declared but not incremented in request handling code.

## Troubleshooting

### Plugin does not appear in CoreDNS

- Ensure patch was applied (`patches/1.14.1/coredns.patch`).
- Confirm rebuilt binary is used at runtime.
- Verify `edgecdnx` appears in patched CoreDNS plugin list.

### Startup panic or config errors

- Validate required directives are set with arguments:
  - `namespace`
  - `soa`
- Check each `ns` line has exactly 2 arguments.
- Check `recordttl` is an integer.
- Check response type values are one of `CNAME`, `A_AAAA`.

### Unexpected fallback to next plugin

- Verify Service CRD domain/host alias exactly matches queried FQDN.
- Verify Location has node group matching Service cache.
- Verify health conditions for selected `A`/`AAAA` path.
- Verify PrefixList destination location exists.
- Check `status.alerts` on the Location — any active Prometheus alert causes the location to be skipped and fallback locations to be tried.
- Check `status.nodeStatus[node].alerts` on each node — any active Prometheus alert on a node removes it from the candidate pool for that request.

### Direct node hostname does not resolve

- Query name must match `nodename.location.node.service.` exactly.
- The `service` suffix must resolve to an existing Service CRD domain or host alias.
- The referenced location must exist.
- The referenced node must exist inside the location node group matching the service cache.

### Unexpected CNAME instead of A or AAAA

- Check `dnsresponsetype` in Corefile for normal DNS-originated requests.
- Check `grpcresponsetype` for requests that carry gRPC incoming metadata in context.

## Development

Minimum toolchain:
- Go `1.25.x`
- `make`
- `patch`
- `curl`
- Docker (optional, for image build)

Basic checks:

```bash
go test ./...
```

## License

See project-level licensing in this repository or organization policy.
