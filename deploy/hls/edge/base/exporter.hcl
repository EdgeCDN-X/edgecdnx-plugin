listen {
  port = 4040
  address = "0.0.0.0"
  metrics_endpoint = "/metrics"
}

namespace "nginxlog" {
  source = {
    files = ["/var/log/nginx/edge-access.log"]
  }
  format = "$remote_addr - $remote_user [$time_local] \"$request\" $status $body_bytes_sent $request_length $request_time $upstream_response_time $upstream_cache_status"
  histogram_buckets = [.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10]
  relabel "cache_status" {
    from = "upstream_cache_status"
    whitelist = ["HIT", "MISS", "BYPASS", "STALE", "EXPIRED", "UPDATING", "REVALIDATED", "-"]
  }
}
