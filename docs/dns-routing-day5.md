# Day 5: Weighted Rendezvous and CoreDNS integration evidence

Date: 2026-08-28 (Australia/Sydney)

## Scope

This stage integrates controller-produced NodeQuality state into the upstream EdgeCDN-X CoreDNS data path. Mature components remain responsible for DNS serving, Kubernetes watches, metrics, and hashing primitives. EdgeRoute implements the CDN-specific candidate filters, immutable snapshot publication, weighted selection, fallback behavior, tests, and experiments.

## Automated verification

The following checks passed before the cluster experiment:

```text
go mod tidy
go test ./...
go vet ./...
go test ./internal/routing -run '^$' -bench Benchmark -benchmem -count=1
```

Coverage includes:

- stable and order-independent Weighted Rendezvous output;
- 100,000-key 70/20/10 distribution;
- controlled migration after node removal;
- zero, negative, NaN and infinite weights;
- IPv4 `/24` and IPv6 `/56` routing-key normalization;
- immutable snapshot reads during concurrent publication;
- NodeQuality update/delete and pointer tombstones;
- static fail-open when NodeQuality is disabled or absent;
- A/AAAA, active-health, Ejected, Disabled and zero-weight filtering.

The request path uses `cespare/xxhash/v2` rather than a project-local hash implementation. The benchmark recorded 0 B/op and 0 allocs/op for 8, 64 and 512 candidates. Exact host results are recorded in [algorithm.md](algorithm.md).

## Runtime image and deployment

The lab image was built with Go 1.25.0, `CGO_ENABLED=0`, and the patched CoreDNS 1.14.2 source. Runtime checks returned:

```text
CoreDNS-1.14.2-edgecdnx-230e774
linux/amd64, go1.25.0
edgecdnx
```

The separate `edge-system/edgeroute-coredns` Deployment runs two ready replicas. It does not replace kind's cluster DNS. RBAC grants only `get/list/watch` for the four EdgeCDN-X CRDs and NodeQuality. Pods run as UID/GID 65532, use a read-only root filesystem, disallow privilege escalation, drop all capabilities, and add only `NET_BIND_SERVICE`.

After the final no-debug rollout, both replicas returned authoritative DNS answers and exposed non-zero `coredns_edgecdnx_request_count_total`, `coredns_edgeroute_routing_total`, and `coredns_edgeroute_snapshot_age_seconds` metrics. This verifies that requests reached EdgeRoute before the upstream `forward` plugin and that both replicas consumed the informer snapshot.

## Defects caught by runtime validation

Three integration defects were found and corrected before acceptance:

1. `.dockerignore` excluded the generated `coredns` binary, so Docker received an empty build context. The binary is now explicitly included.
2. A CGO-linked binary could build but not run in distroless static. The reproducible Makefile now forces `CGO_ENABLED=0` and refreshes the commit-based version string.
3. The upstream patch appended `edgecdnx` after `forward`. Metrics proved `forward` answered NXDOMAIN before EdgeRoute's request counter or selection path ran. The patch and generated plugin configuration now place `edgecdnx` before `forward`.

The initial file capability also conflicted with Kubernetes `allowPrivilegeEscalation: false`. File capabilities were removed from the image; the Pod receives only the runtime capability needed to bind port 53.

## Live DNS acceptance

The test service was `video.edgeroute.test`. A static PrefixList maps lab ECS addresses under `198.0.0.0/8` to the Sydney Location. Each sample varies the normalized `/24` ECS key and queries the two-replica DNS Service without a DNS cache directive.

### Safety fallback with all Sydney weights at zero

Both Sydney nodes initially had zero effective weight. The primary Location produced no candidate and the existing configured fallback returned Singapore:

```text
video.edgeroute.test. 30 IN A 10.96.10.13
```

Debug evidence during diagnosis showed the sequence: PrefixList selected Sydney, Sydney had no healthy weighted candidate, fallback selected Singapore, and CoreDNS returned an authoritative answer.

### Dynamic distribution change

The Quality Controller was temporarily scaled to zero so it could not overwrite controlled status input. It was restored immediately after the experiment.

| NodeQuality input | Queries | edge-syd-a (`10.96.10.11`) | edge-syd-b (`10.96.10.12`) |
|---|---:|---:|---:|
| A=70, B=30 | 2,000 | 1,393 (69.65%) | 607 (30.35%) |
| A=10, B=90 | 2,000 | 197 (9.85%) | 1,803 (90.15%) |

The same key-generation procedure produced a measurable distribution shift after informer-driven weight updates.

### Ejected exclusion overrides weight

The deliberately adversarial input was A=`Ejected`, weight=100 and B=`Healthy`, weight=100. All 2,000 queries returned B:

```text
2000 10.96.10.12
```

The Ejected node was returned zero times even with a positive maximum weight. This satisfies the Day 5 safety acceptance criterion.

## Cleanup and limits

- The Quality Controller was restored to one ready replica.
- The temporary netshoot DNS probe Pod was deleted.
- Temporary CoreDNS debug logging was removed from the committed Corefile.
- The experiment uses one local kind cluster and logical regions; it does not represent global production traffic or intercontinental latency.
