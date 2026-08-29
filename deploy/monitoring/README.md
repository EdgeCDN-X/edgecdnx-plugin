# Monitoring stack

The local testbed pins `kube-prometheus-stack` chart version `88.5.4` from the
official Prometheus Community OCI registry. The values disable Grafana and
Alertmanager for the resource-constrained kind experiment while retaining
Prometheus Operator, Prometheus, kube-state-metrics, and node-exporter.

```powershell
./.tools/windows-amd64/helm.exe upgrade --install monitoring `
  oci://ghcr.io/prometheus-community/charts/kube-prometheus-stack `
  --version 88.5.4 `
  --namespace monitoring `
  --create-namespace `
  --values deploy/monitoring/values.yaml
```

This chart installation alone is not evidence that EdgeRoute telemetry works.
All EdgeRoute ServiceMonitor targets and representative PromQL queries must be
verified separately.

Observed on 2026-08-27: the three NGINX exporter targets, three Toxiproxy
targets, and MediaMTX target were all `UP`. The query
`nginx_connections_active{service="hls-demo"}` returned separate series for
`edge-syd-a`, `edge-syd-b`, and `edge-sin-a` with stable location labels.
