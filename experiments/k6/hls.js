import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const playlistFailures = new Counter('hls_playlist_failures');
const playlistRequests = new Counter('hls_playlist_requests');
const segmentFailures = new Counter('hls_segment_failures');
const segmentRequests = new Counter('hls_segment_requests');
const segmentDuration = new Trend('hls_segment_duration', true);
const sessionFailures = new Rate('hls_session_failures');
const stallEvents = new Counter('hls_stall_events');
const selectedEdgeNode = new Counter('selected_edge_node');

const baseURL = __ENV.HLS_BASE_URL || 'http://video.edgeroute.test:8080';
const paceSeconds = Number(__ENV.PACE_SECONDS || '1');
const targetVUs = Number(__ENV.TARGET_VUS || '20');
const peakVUs = Number(__ENV.PEAK_VUS || '100');

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  stages: [
    { duration: __ENV.WARMUP_DURATION || '1m', target: targetVUs },
    { duration: __ENV.STEADY_DURATION || '3m', target: targetVUs },
    { duration: __ENV.RAMP_DURATION || '2m', target: peakVUs },
    { duration: __ENV.PEAK_DURATION || '3m', target: peakVUs },
    { duration: __ENV.RECOVERY_DURATION || '2m', target: targetVUs },
  ],
  gracefulStop: '5s',
  noConnectionReuse: true,
  dns: {
    ttl: __ENV.DNS_TTL || '30s',
    select: 'random',
    policy: 'preferIPv4',
  },
  thresholds: {
    hls_playlist_failures: ['count>=0'],
    hls_segment_failures: ['count>=0'],
    hls_session_failures: ['rate>=0'],
  },
  tags: {
    run_id: __ENV.RUN_ID || 'manual',
    variant: __ENV.VARIANT || 'adaptive',
    fault_scenario: __ENV.FAULT_SCENARIO || 'none',
  },
};

function firstMediaLine(body, suffix) {
  const lines = String(body || '').split(/\r?\n/);
  for (const line of lines) {
    const value = line.trim();
    if (value && !value.startsWith('#') && value.includes(suffix)) {
      return value;
    }
  }
  return '';
}

function latestSegmentLine(body) {
  const lines = String(body || '').split(/\r?\n/);
  let selected = '';
  for (const line of lines) {
    const value = line.trim();
    if (/_seg[0-9]+\.mp4(?:\?.*)?$/.test(value)) selected = value;
  }
  return selected;
}

function resolveRelative(parentURL, child) {
  if (/^https?:\/\//.test(child)) return child;
  return `${parentURL.slice(0, parentURL.lastIndexOf('/') + 1)}${child}`;
}

function recordNode(response) {
  const remote = response && response.remote_ip ? response.remote_ip : 'unknown';
  selectedEdgeNode.add(1, { node: remote });
}

export default function () {
  let failed = false;
  const masterURL = `${baseURL}/live/demo/index.m3u8?run=${__ENV.RUN_ID || 'manual'}`;
  const master = http.get(masterURL, { tags: { resource: 'master_playlist' }, timeout: '5s' });
  playlistRequests.add(1, { playlist: 'master' });
  recordNode(master);
  if (!check(master, { 'master playlist is 200': (r) => r.status === 200 })) {
    playlistFailures.add(1, { playlist: 'master' });
    sessionFailures.add(true);
    sleep(paceSeconds);
    return;
  }

  const variantName = firstMediaLine(master.body, '.m3u8');
  if (!variantName) {
    playlistFailures.add(1, { playlist: 'master_parse' });
    sessionFailures.add(true);
    sleep(paceSeconds);
    return;
  }

  const variantURL = resolveRelative(masterURL.split('?')[0], variantName);
  const playlist = http.get(variantURL, { tags: { resource: 'media_playlist' }, timeout: '5s' });
  playlistRequests.add(1, { playlist: 'media' });
  recordNode(playlist);
  if (!check(playlist, { 'media playlist is 200': (r) => r.status === 200 })) {
    playlistFailures.add(1, { playlist: 'media' });
    sessionFailures.add(true);
    sleep(paceSeconds);
    return;
  }

  const segmentName = latestSegmentLine(playlist.body);
  if (!segmentName) {
    playlistFailures.add(1, { playlist: 'segment_parse' });
    sessionFailures.add(true);
    sleep(paceSeconds);
    return;
  }

  const segmentURL = resolveRelative(variantURL, segmentName);
  const segment = http.get(segmentURL, { tags: { resource: 'segment' }, timeout: '5s' });
  segmentRequests.add(1);
  recordNode(segment);
  segmentDuration.add(segment.timings.duration, { node: segment.remote_ip || 'unknown' });
  if (!check(segment, { 'segment is 200': (r) => r.status === 200 })) {
    segmentFailures.add(1, { node: segment.remote_ip || 'unknown' });
    failed = true;
  }
  if (segment.timings.duration > Number(__ENV.STALL_THRESHOLD_MS || '1000')) {
    stallEvents.add(1, { node: segment.remote_ip || 'unknown' });
  }
  sessionFailures.add(failed);
  sleep(paceSeconds);
}
