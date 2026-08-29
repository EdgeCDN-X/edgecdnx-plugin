package quality

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type PrometheusProvider struct {
	baseURL string
	client  *http.Client
	queries PrometheusQueries
}

type PrometheusQueries struct {
	ActiveRequests  string
	RequestCount    string
	ErrorCount      string
	P95Latency      string
	BaselineLatency string
	CacheHits       string
	Cacheable       string
}

func DefaultPrometheusQueries() PrometheusQueries {
	return PrometheusQueries{
		ActiveRequests:  `nginx_connections_active{node=$NODE}`,
		RequestCount:    `sum(increase(nginxlog_http_response_count_total{node=$NODE}[1m]))`,
		ErrorCount:      `sum(increase(nginxlog_http_response_count_total{node=$NODE,status=~"5.."}[1m]))`,
		P95Latency:      `histogram_quantile(0.95,sum by (le) (rate(nginxlog_http_response_time_seconds_hist_bucket{node=$NODE}[1m])))`,
		BaselineLatency: `histogram_quantile(0.10,sum by (le) (rate(nginxlog_http_response_time_seconds_hist_bucket{node=$NODE}[30m])))`,
		CacheHits:       `sum(increase(nginxlog_http_response_count_total{node=$NODE,cache_status="HIT"}[1m]))`,
		Cacheable:       `sum(increase(nginxlog_http_response_count_total{node=$NODE,cache_status=~"HIT|MISS"}[1m]))`,
	}
}

func (q PrometheusQueries) Validate() error {
	queries := map[string]string{
		"active requests": q.ActiveRequests, "request count": q.RequestCount, "error count": q.ErrorCount,
		"P95 latency": q.P95Latency, "baseline latency": q.BaselineLatency, "cache hits": q.CacheHits, "cacheable": q.Cacheable,
	}
	for name, query := range queries {
		if query == "" || !strings.Contains(query, "$NODE") {
			return fmt.Errorf("%s query must contain $NODE", name)
		}
	}
	return nil
}

func NewPrometheusProvider(baseURL string, timeout time.Duration, queries PrometheusQueries) (*PrometheusProvider, error) {
	if _, err := url.ParseRequestURI(baseURL); err != nil {
		return nil, fmt.Errorf("invalid Prometheus URL: %w", err)
	}
	if err := queries.Validate(); err != nil {
		return nil, err
	}
	return &PrometheusProvider{baseURL: strings.TrimRight(baseURL, "/"), client: &http.Client{Timeout: timeout}, queries: queries}, nil
}

func (p *PrometheusProvider) QueryNode(ctx context.Context, node string, at time.Time) (NodeSample, error) {
	active, err := p.queryRequired(ctx, renderQuery(p.queries.ActiveRequests, node), at)
	if err != nil {
		return NodeSample{}, fmt.Errorf("active requests: %w", err)
	}
	queries := []struct {
		name       string
		expression string
	}{
		{"request count", p.queries.RequestCount},
		{"error count", p.queries.ErrorCount},
		{"P95 latency", p.queries.P95Latency},
		{"baseline latency", p.queries.BaselineLatency},
		{"cache hits", p.queries.CacheHits},
		{"cacheable requests", p.queries.Cacheable},
	}
	values := make([]float64, len(queries))
	for i, query := range queries {
		values[i], err = p.queryOptional(ctx, renderQuery(query.expression, node), at)
		if err != nil {
			return NodeSample{}, fmt.Errorf("%s: %w", query.name, err)
		}
	}
	cacheRatio := 0.0
	if values[5] > 0 {
		cacheRatio = values[4] / values[5]
	}
	return NodeSample{Timestamp: at, ActiveRequests: int(active), RequestCount: uint64(math.Round(values[0])), ErrorCount: uint64(math.Round(values[1])), P95Latency: time.Duration(values[2] * float64(time.Second)), BaselineLatency: time.Duration(values[3] * float64(time.Second)), CacheHitRatio: cacheRatio}, nil
}

func renderQuery(template, node string) string {
	return strings.ReplaceAll(template, "$NODE", strconv.Quote(node))
}

func (p *PrometheusProvider) queryRequired(ctx context.Context, expression string, at time.Time) (float64, error) {
	value, found, err := p.query(ctx, expression, at)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, fmt.Errorf("expected one sample")
	}
	return value, nil
}

func (p *PrometheusProvider) queryOptional(ctx context.Context, expression string, at time.Time) (float64, error) {
	value, _, err := p.query(ctx, expression, at)
	return value, err
}

func (p *PrometheusProvider) query(ctx context.Context, expression string, at time.Time) (float64, bool, error) {
	u, err := url.Parse(p.baseURL + "/api/v1/query")
	if err != nil {
		return 0, false, err
	}
	q := u.Query()
	q.Set("query", expression)
	q.Set("time", strconv.FormatInt(at.Unix(), 10))
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, false, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("prometheus returned %s", resp.Status)
	}
	var body struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, false, err
	}
	if body.Status != "success" {
		return 0, false, fmt.Errorf("query status %q", body.Status)
	}
	if len(body.Data.Result) == 0 {
		return 0, false, nil
	}
	if len(body.Data.Result) != 1 || len(body.Data.Result[0].Value) != 2 {
		return 0, false, fmt.Errorf("expected one sample")
	}
	raw, ok := body.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, false, fmt.Errorf("sample is not a string")
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0, false, fmt.Errorf("invalid sample %q", raw)
	}
	return value, true, nil
}
