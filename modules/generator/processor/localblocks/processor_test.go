package localblocks

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/tempo/pkg/tempopb"
	commonv1proto "github.com/grafana/tempo/pkg/tempopb/common/v1"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/grafana/tempo/tempodb/wal"
)

type mockOverrides struct{}

var _ ProcessorOverrides = (*mockOverrides)(nil)

func (m *mockOverrides) DedicatedColumns(string) backend.DedicatedColumns {
	return nil
}

func (m *mockOverrides) MaxBytesPerTrace(string) int {
	return 0
}

func TestProcessorDoesNotRace(t *testing.T) {
	wal, err := wal.New(&wal.Config{
		Filepath: t.TempDir(),
		Version:  encoding.DefaultEncoding().Version(),
	})
	require.NoError(t, err)

	var (
		ctx    = context.Background()
		tenant = "fake"
		cfg    = Config{
			FlushCheckPeriod:     10 * time.Millisecond,
			TraceIdlePeriod:      time.Second,
			CompleteBlockTimeout: time.Minute,
			ConcurrentBlocks:     10,
			Block: &common.BlockConfig{
				BloomShardSizeBytes: 100_000,
				BloomFP:             0.05,
				Version:             encoding.DefaultEncoding().Version(),
			},
		}
		overrides = &mockOverrides{}
	)

	p, err := New(cfg, tenant, wal, overrides)
	require.NoError(t, err)

	var (
		end = make(chan struct{})
		wg  = sync.WaitGroup{}
	)

	concurrent := func(f func()) {
		wg.Add(1)
		defer wg.Done()

		for {
			select {
			case <-end:
				return
			default:
				f()
			}
		}
	}

	go concurrent(func() {
		tr := test.MakeTrace(10, nil)
		for _, b := range tr.Batches {
			for _, ss := range b.ScopeSpans {
				for _, s := range ss.Spans {
					s.Kind = v1.Span_SPAN_KIND_SERVER
				}
			}
		}

		req := &tempopb.PushSpansRequest{
			Batches: tr.Batches,
		}
		p.PushSpans(ctx, req)
	})

	go concurrent(func() {
		err := p.cutIdleTraces(true)
		require.NoError(t, err, "cutting idle traces")
	})

	go concurrent(func() {
		err := p.cutBlocks(true)
		require.NoError(t, err, "cutting blocks")
	})

	go concurrent(func() {
		err := p.completeBlock()
		require.NoError(t, err, "completing block")
	})

	go concurrent(func() {
		err := p.deleteOldBlocks()
		require.NoError(t, err, "deleting old blocks")
	})

	// Run multiple queries
	go concurrent(func() {
		_, err := p.GetMetrics(ctx, &tempopb.SpanMetricsRequest{
			Query:   "{}",
			GroupBy: "status",
		})
		require.NoError(t, err)
	})

	go concurrent(func() {
		_, err := p.GetMetrics(ctx, &tempopb.SpanMetricsRequest{
			Query:   "{}",
			GroupBy: "status",
		})
		require.NoError(t, err)
	})

	go concurrent(func() {
		_, err := p.QueryRange(ctx, &tempopb.QueryRangeRequest{
			Query: "{} | rate() by (resource.service.name)",
			Start: uint64(time.Now().Add(-5 * time.Minute).UnixNano()),
			End:   uint64(time.Now().UnixNano()),
			Step:  uint64(30 * time.Second),
		})
		require.NoError(t, err)
	})

	// Run for a bit
	time.Sleep(2000 * time.Millisecond)

	// Cleanup
	close(end)
	wg.Wait()
	p.Shutdown(ctx)
}

func TestQueryRangeTraceQLToProto(t *testing.T) {
	req := &tempopb.QueryRangeRequest{
		Start: 1700143700617413958, // 3 minute window
		End:   1700143880619139505,
		Step:  30000000000, // 30 seconds
	}

	ts := queryRangeTraceQLToProto(traceql.SeriesSet{
		"": traceql.TimeSeries{
			Labels: labels.FromStrings("a", "b"),
			Values: []float64{17.566666666666666, 18.133333333333333, 17.3, 14.533333333333333, 0, 0, 0},
		},
	}, req)

	expected := &tempopb.QueryRangeResponse{
		Series: []*tempopb.TimeSeries{
			{
				Labels: []commonv1proto.KeyValue{
					{
						Key: "a",
						Value: &commonv1proto.AnyValue{
							Value: &commonv1proto.AnyValue_StringValue{StringValue: "b"},
						},
					},
				},
				Samples: []tempopb.Sample{
					{
						TimestampMs: 1700143700617,
						Value:       17.566666666666666,
					},
					{
						TimestampMs: 1700143730617,
						Value:       18.133333333333333,
					},
					{
						TimestampMs: 1700143760617,
						Value:       17.3,
					},
					{
						TimestampMs: 1700143790617,
						Value:       14.533333333333333,
					},
					{
						TimestampMs: 1700143820617,
						Value:       0,
					},
					{
						TimestampMs: 1700143850617,
						Value:       0,
					},
					{
						TimestampMs: 1700143880617,
						Value:       0,
					},
				},
			},
		},
	}

	require.Equal(t, len(expected.Series), len(ts))

	for i, e := range expected.Series {
		require.Equal(t, e, ts[i])
	}
}
