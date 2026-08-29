# Upstream sources

EdgeRoute extends the EdgeCDN-X CoreDNS plugin. Upstream code and project-specific additions must remain distinguishable in Git history and documentation.

## Main upstream

| Field | Value |
| --- | --- |
| Repository | `https://github.com/EdgeCDN-X/edgecdnx-plugin` |
| Baseline commit | `1dd32f2f970831b880d5015f63adb74433d767d3` |
| Baseline date | `2026-04-14T14:46:55+02:00` |
| Use | Fork and runtime scaffold; modified in this repository |
| License status | No standalone `LICENSE` file was present at the pinned commit; README defers to project/organization policy |

Git remotes:

```text
origin    https://github.com/BitForLI/EdgeRoute.git
upstream  https://github.com/EdgeCDN-X/edgecdnx-plugin.git
```

## Read-only deployment references

| Repository | Pinned commit | Use | Modified here |
| --- | --- | --- | --- |
| `EdgeCDN-X/edgecdnx-gitops` | `8e2f6fbdba57e94d41a4d272696cfe59799755f9` | Platform deployment reference | No |
| `EdgeCDN-X/edgecdnx-controller` | `aafe2fee61194890c365b16a5a9107c3e09d1cc9` | CRDs and official samples | No |
| `EdgeCDN-X/helm-charts` | `4f4680a20a8c3e9a5afb41fff9119fa2f11b3ff5` | Helm deployment reference | No |

These repositories are cloned next to the main repository for local study and deployment only. Their source is not vendored into EdgeRoute.

## Baseline verification

The unmodified main upstream commit was checked with Go 1.25.0:

```text
go test ./...   PASS (upstream contains no *_test.go files)
go build ./...  PASS
```

CoreDNS image build and Kubernetes baseline are tracked separately because they require a running container engine and kind/Helm tooling.
