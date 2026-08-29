# Edge Pod outage

- Target: `deployment/edge-syd-a-edge`.
- Injection: scale the Deployment to zero.
- Expected adaptive behavior: health/telemetry removes the node from DNS answers and shifts sessions to `edge-syd-b`.
- Recovery: scale to one and wait for readiness; the script never deletes the Deployment or its configuration.
