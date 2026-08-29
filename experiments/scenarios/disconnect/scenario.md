# Origin connection reset

- Target: `edge-syd-a-origin`.
- Injection: Toxiproxy downstream `reset_peer` at 100% toxicity.
- Expected adaptive behavior: consecutive failures eject `edge-syd-a`; DNS moves traffic to `edge-syd-b`.
- Recovery: Toxiproxy `/reset`, then cooldown and staged recovery.
