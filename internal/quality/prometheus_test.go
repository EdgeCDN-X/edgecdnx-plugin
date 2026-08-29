package quality

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPrometheusProviderNormalResponse(t *testing.T) {
	values := []string{"3", "100", "5", "0.2", "0.05", "80", "100"}
	var request int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		if !strings.Contains(query, `node="edge-syd-a"`) {
			t.Errorf("node placeholder was not safely rendered: %s", query)
		}
		index := int(atomic.AddInt32(&request, 1)) - 1
		prometheusResponse(w, values[index], true)
	}))
	defer server.Close()
	provider, err := NewPrometheusProvider(server.URL, time.Second, DefaultPrometheusQueries())
	if err != nil {
		t.Fatal(err)
	}
	sample, err := provider.QueryNode(context.Background(), "edge-syd-a", time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	if sample.ActiveRequests != 3 || sample.RequestCount != 100 || sample.ErrorCount != 5 || sample.P95Latency != 200*time.Millisecond || sample.BaselineLatency != 50*time.Millisecond || sample.CacheHitRatio != .8 {
		t.Fatalf("unexpected sample: %+v", sample)
	}
}

func TestPrometheusProviderEmptyAndPartialVectors(t *testing.T) {
	t.Run("required vector", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { prometheusResponse(w, "", false) }))
		defer server.Close()
		provider, _ := NewPrometheusProvider(server.URL, time.Second, DefaultPrometheusQueries())
		if _, err := provider.QueryNode(context.Background(), "edge-syd-a", time.Now()); err == nil {
			t.Fatal("empty required vector accepted")
		}
	})
	t.Run("optional vector", func(t *testing.T) {
		var request int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			index := atomic.AddInt32(&request, 1)
			if index == 2 {
				prometheusResponse(w, "", false)
				return
			}
			prometheusResponse(w, "0", true)
		}))
		defer server.Close()
		provider, _ := NewPrometheusProvider(server.URL, time.Second, DefaultPrometheusQueries())
		sample, err := provider.QueryNode(context.Background(), "edge-syd-a", time.Now())
		if err != nil || sample.RequestCount != 0 {
			t.Fatalf("sample=%+v err=%v", sample, err)
		}
	})
}

func TestPrometheusProviderTimeoutAndInvalidSample(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(40 * time.Millisecond)
			prometheusResponse(w, "1", true)
		}))
		defer server.Close()
		provider, _ := NewPrometheusProvider(server.URL, 5*time.Millisecond, DefaultPrometheusQueries())
		if _, err := provider.QueryNode(context.Background(), "edge-syd-a", time.Now()); err == nil {
			t.Fatal("timeout was not returned")
		}
	})
	t.Run("NaN", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { prometheusResponse(w, "NaN", true) }))
		defer server.Close()
		provider, _ := NewPrometheusProvider(server.URL, time.Second, DefaultPrometheusQueries())
		if _, err := provider.QueryNode(context.Background(), "edge-syd-a", time.Now()); err == nil {
			t.Fatal("NaN sample was accepted")
		}
	})
}

func TestPrometheusQueryConfigurationValidation(t *testing.T) {
	queries := DefaultPrometheusQueries()
	queries.ErrorCount = "sum(errors_total)"
	if _, err := NewPrometheusProvider("http://prometheus", time.Second, queries); err == nil {
		t.Fatal("query without $NODE accepted")
	}
}

func prometheusResponse(w http.ResponseWriter, value string, found bool) {
	w.Header().Set("Content-Type", "application/json")
	result := "[]"
	if found {
		result = fmt.Sprintf(`[{"metric":{},"value":[100,%q]}]`, value)
	}
	fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":%s}}`, result)
}
