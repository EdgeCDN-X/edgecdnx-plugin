# Cold-cache recovery

- Target: `edge-syd-a` NGINX cache backed by `emptyDir`.
- Injection: delete only the selected Pod; the Deployment recreates it with an empty cache.
- Expected adaptive behavior: recovering weight increases gradually, limiting the origin MISS spike.
- Recovery: wait for readiness and reset Toxiproxy. Raw NGINX/Prometheus data is retained by the experiment runner.
