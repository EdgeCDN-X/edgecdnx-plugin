# HLS reliability testbed

This testbed composes pinned MediaMTX, FFmpeg, NGINX, and Toxiproxy images. It
does not reimplement any of those systems. EdgeRoute-owned configuration maps a
real HLS stream to independently fault-injectable cache nodes.

## Topology

```text
FFmpeg testsrc2 + sine -> RTMP -> MediaMTX /live/demo
                                      |
              +-----------------------+-----------------------+
              |                       |                       |
        edge-syd-a              edge-syd-b              edge-sin-a
      NGINX -> Toxiproxy      NGINX -> Toxiproxy      NGINX -> Toxiproxy
```

The two Sydney nodes run on the worker labeled `edge-role=sydney`; the Singapore
fallback runs on `edge-role=singapore`. They are logical locations in a local
kind cluster, not evidence of real intercontinental links.

Each node is rendered from `deploy/hls/edge/base` through a Kustomize overlay,
but has its own Deployment, Service, cache volume, stable labels, and uniquely
named Toxiproxy proxy. MediaMTX configuration lives in `deploy/mediamtx`.

## Cache behavior

- playlists (`.m3u8`): 1 second TTL;
- immutable HLS fMP4 objects (`.mp4`): 10 minute TTL;
- errors: not cached;
- stale segments: allowed on origin error or timeout;
- segment cache key: scheme, method, host, and URI; MediaMTX's transient session
  query parameter is deliberately excluded.

## Deploy and verify

```powershell
kubectl apply --server-side --dry-run=server -k deploy
kubectl apply --server-side -k deploy
powershell -File scripts/verify-hls.ps1
```

Expected result for every edge is `Playlist=PASS`, then `MISS`, then `HIT` for
the same complete media segment. The script also checks the MediaMTX metrics
endpoint and each node's unique Toxiproxy name.

## Observed Day 2 evidence

On 2026-08-27, all five Deployments became ready and all three edge nodes passed
the automated playlist and segment checks. A downstream toxic of 150 ms latency
and 20 ms jitter was applied only to `edge-syd-a-origin`: the proxied request
measured 280 ms versus 129 ms on `edge-syd-b-origin`. The toxic was then deleted
and its absence verified. These are local experimental measurements, not
production performance claims.
