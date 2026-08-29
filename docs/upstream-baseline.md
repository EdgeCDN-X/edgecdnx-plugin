# Upstream baseline

The experiment baseline is the unmodified EdgeCDN-X CoreDNS plugin at commit
`1dd32f2f970831b880d5015f63adb74433d767d3`, preserved by the annotated tag
`upstream-baseline-1dd32f2`.

## Checks

```text
go test ./...  PASS (the upstream repository contains no test files)
go build ./... PASS
CoreDNS image build PASS
CoreDNS runtime version CoreDNS-1.14.2-edgecdnx-upstream-1dd32f2
CoreDNS plugin registration edgecdnx PRESENT
EdgeCDN-X CRDs INSTALLED (five resources)
minimal Location sample server-side dry-run PASS
minimal Location sample apply PASS (four logical locations)
```

The image is named `edgeroute-coredns:upstream-1dd32f2`. It is retained for
later baseline-versus-adaptive routing experiments. Tool versions and the kind
cluster state are recorded in `docs/environment.md`; upstream provenance and
license boundaries are recorded in `UPSTREAM.md` and `NOTICE.md`.
