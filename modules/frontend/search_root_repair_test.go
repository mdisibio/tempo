package frontend

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	resourcev1 "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	tracev1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"

	"github.com/grafana/tempo/pkg/tempopb"
)

func resourceWithServiceName(name string) *resourcev1.Resource {
	return &resourcev1.Resource{
		Attributes: []*commonv1.KeyValue{
			{Key: "service.name", Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: name}}},
		},
	}
}

func TestRootSpanFromTrace(t *testing.T) {
	t.Run("nil trace", func(t *testing.T) {
		svc, name, ok := rootSpanFromTrace(nil)
		require.False(t, ok)
		require.Empty(t, svc)
		require.Empty(t, name)
	})

	t.Run("no root span", func(t *testing.T) {
		trace := &tempopb.Trace{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: resourceWithServiceName("router"),
					ScopeSpans: []*tracev1.ScopeSpans{
						{Spans: []*tracev1.Span{
							{Name: "egress", ParentSpanId: []byte{1}, StartTimeUnixNano: 100},
						}},
					},
				},
			},
		}

		svc, name, ok := rootSpanFromTrace(trace)
		require.False(t, ok)
		require.Empty(t, svc)
		require.Empty(t, name)
	})

	t.Run("finds root span with empty parent", func(t *testing.T) {
		trace := &tempopb.Trace{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: resourceWithServiceName("router"),
					ScopeSpans: []*tracev1.ScopeSpans{
						{Spans: []*tracev1.Span{
							{Name: "egress", ParentSpanId: []byte{1}, StartTimeUnixNano: 200},
						}},
					},
				},
				{
					Resource: resourceWithServiceName("envoy"),
					ScopeSpans: []*tracev1.ScopeSpans{
						{Spans: []*tracev1.Span{
							{Name: "ingress", ParentSpanId: nil, StartTimeUnixNano: 100},
						}},
					},
				},
			},
		}

		svc, name, ok := rootSpanFromTrace(trace)
		require.True(t, ok)
		require.Equal(t, "envoy", svc)
		require.Equal(t, "ingress", name)
	})

	t.Run("multiple parentless spans picks the earliest", func(t *testing.T) {
		trace := &tempopb.Trace{
			ResourceSpans: []*tracev1.ResourceSpans{
				{
					Resource: resourceWithServiceName("late"),
					ScopeSpans: []*tracev1.ScopeSpans{
						{Spans: []*tracev1.Span{
							{Name: "late-root", ParentSpanId: nil, StartTimeUnixNano: 500},
						}},
					},
				},
				{
					Resource: resourceWithServiceName("early"),
					ScopeSpans: []*tracev1.ScopeSpans{
						{Spans: []*tracev1.Span{
							{Name: "early-root", ParentSpanId: nil, StartTimeUnixNano: 100},
						}},
					},
				},
			},
		}

		svc, name, ok := rootSpanFromTrace(trace)
		require.True(t, ok)
		require.Equal(t, "early", svc)
		require.Equal(t, "early-root", name)
	})
}

func TestBuildRootSpanRepairRequest(t *testing.T) {
	t.Run("sets a +/- 1 hour time range around the start time", func(t *testing.T) {
		startTime := time.Unix(1_700_000_000, 0)
		req := buildRootSpanRepairRequest(context.Background(), "", "1234", uint64(startTime.UnixNano()), http.Header{}) //nolint:gosec // test-only overflow

		q := req.URL.Query()
		require.Equal(t, strconv.FormatInt(startTime.Add(-time.Hour).Unix(), 10), q.Get(traceByIDStartParam))
		require.Equal(t, strconv.FormatInt(startTime.Add(time.Hour).Unix(), 10), q.Get(traceByIDEndParam))
	})

	t.Run("omits the time range when the start time is unknown", func(t *testing.T) {
		req := buildRootSpanRepairRequest(context.Background(), "", "1234", 0, http.Header{})

		q := req.URL.Query()
		require.Empty(t, q.Get(traceByIDStartParam))
		require.Empty(t, q.Get(traceByIDEndParam))
	})
}
