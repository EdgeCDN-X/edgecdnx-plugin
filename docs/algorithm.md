# EdgeRoute routing algorithm

## Control-plane and request-path boundary

The Quality Controller queries Prometheus and writes bounded `effectiveWeight` and state values to `NodeQuality.status`. CoreDNS watches those objects with a client-go dynamic informer. Each event creates a new immutable map and publishes it through `atomic.Pointer`; DNS request goroutines only read the current snapshot. There is no Prometheus or Kubernetes API request in `ServeDNS`.

If the NodeQuality CRD is absent at startup, CoreDNS logs a warning and uses the configured static `defaultweight`. If the CRD exists, readiness remains false until the NodeQuality and EdgeCDN-X informer caches have synced.

## Routing key

The stable key is:

```text
normalizedClientSubnet + "|" + normalizedQName + "|" + queryType
```

- IPv4 is masked to `/24`.
- IPv6 is masked to `/56`.
- EDNS Client Subnet is preferred when present; otherwise the transport peer address is used.
- The normalized subnet, not the complete client IP, enters the key and request logs do not contain the address.

DNS cannot see the HLS URL path, video identifier, playlist, or segment. EdgeRoute therefore makes a stable client-subnet/service/query-type decision, not a per-video or per-segment decision.

## Weighted Rendezvous selection

For every eligible node, EdgeRoute hashes `key || 0x00 || nodeID` with `github.com/cespare/xxhash/v2` (XXH64), maps the result to `u` in `(0,1]`, and calculates:

```text
rank = -ln(u) / effectiveWeight
```

The node with the smallest rank wins. Empty IDs, non-positive weights, NaN and infinite weights are ignored. A node-ID lexical tie-break makes the result independent of candidate iteration order.

This exponential-rank form follows the weighted distributed hashing formulation described by Schindelhauer and Schomaker. The hash primitive is the maintained [cespare/xxhash](https://github.com/cespare/xxhash) Go module; EdgeRoute does not implement its own hash function. The independent [go-rendezvous](https://github.com/dgryski/go-rendezvous) project was used as a compact API and test reference, but no source was copied.

## Candidate order and safety behavior

1. Reuse EdgeCDN-X prefix/ECS/Geo logic to select a Location.
2. Exclude a Location or node in maintenance and nodes with active-health or alert failures.
3. Exclude nodes without an address matching the A/AAAA query.
4. Exclude `Disabled` and `Ejected` NodeQuality states.
5. Use `effectiveWeight`; exclude non-positive values.
6. Use `defaultweight` when a node has no NodeQuality entry or the CRD is absent.
7. Select with Weighted Rendezvous.
8. Preserve the upstream parent, then configured fallback Location chain when no candidate remains.

`Degraded`, `Recovering`, and `Stale` nodes remain eligible only when the controller published a positive bounded weight. `Disabled` and `Ejected` never fail open.

## Verified properties

Automated tests cover deterministic and order-independent selection, 70/20/10 distribution, controlled migration after removal, zero and invalid weights, IPv4/IPv6 filtering, immutable snapshots, concurrent publish/read, static fail-open, and Ejected exclusion.

Windows/amd64 benchmark on the lab host (Intel i5-12400F, Go 1.25.0):

| Candidate count | Serial | Parallel | Request-path allocations |
|---:|---:|---:|---:|
| 8 | 532.6 ns/op | 60.64 ns/op | 0 B/op, 0 allocs/op |
| 64 | 4.368 µs/op | 525.1 ns/op | 0 B/op, 0 allocs/op |
| 512 | 35.51 µs/op | 4.389 µs/op | 0 B/op, 0 allocs/op |

Snapshot replacement for 512 entries measured 43.28 µs/op and 114,728 B/op. That allocation occurs only on informer updates, not DNS queries. These local numbers are reproducible evidence for this machine, not a claim about global production throughput.

## References

- Christian Schindelhauer and Gunnar Schomaker, [Weighted Distributed Hash Tables](https://citeseerx.ist.psu.edu/document?doi=8c55282dc37d1e3b46b15c2d97f60568ccb9c9cd&repid=rep1&type=pdf).
- [cespare/xxhash](https://github.com/cespare/xxhash), the pinned XXH64 Go implementation.
- [dgryski/go-rendezvous](https://github.com/dgryski/go-rendezvous), an independent Rendezvous Hashing implementation used as a design reference.
