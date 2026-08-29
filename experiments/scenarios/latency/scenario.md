# Origin latency

- Target: `edge-syd-a-origin`.
- Injection: Toxiproxy downstream latency `150 ms` with `20 ms` jitter.
- Expected adaptive behavior: `edge-syd-a` degrades and loses weight without requiring a 5xx spike; traffic moves toward `edge-syd-b`.
- Recovery: Toxiproxy `/reset`, followed by controller-driven bounded weight recovery.
