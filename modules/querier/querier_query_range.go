package querier

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/grafana/tempo/pkg/boundedwaitgroup"
	"github.com/grafana/tempo/pkg/tempopb"
	v1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/atomic"
)

func (q *Querier) QueryRange(ctx context.Context, req *tempopb.QueryRangeRequest) (*tempopb.QueryRangeResponse, error) {
	if req.QueryMode == "recent" {
		return q.queryRangeRecent(ctx, req)
	}

	// Backend requests go here
	return q.queryBackend(ctx, req)
}

func (q *Querier) queryRangeRecent(ctx context.Context, req *tempopb.QueryRangeRequest) (*tempopb.QueryRangeResponse, error) {
	// // Get results from all generators
	replicationSet, err := q.generatorRing.GetReplicationSetForOperation(ring.Read)
	if err != nil {
		return nil, fmt.Errorf("error finding generators in Querier.SpanMetricsSummary: %w", err)
	}
	lookupResults, err := q.forGivenGenerators(
		ctx,
		replicationSet,
		func(ctx context.Context, client tempopb.MetricsGeneratorClient) (interface{}, error) {
			return client.QueryRange(ctx, req)
		},
	)
	if err != nil {
		_ = level.Error(log.Logger).Log("error querying generators in Querier.MetricsQueryRange", "err", err)

		return nil, fmt.Errorf("error querying generators in Querier.MetricsQueryRange: %w", err)
	}

	c := traceql.QueryRangeCombiner{}
	for _, result := range lookupResults {
		c.Combine(result.response.(*tempopb.QueryRangeResponse))
	}

	return c.Response(), nil
}

func (q *Querier) queryBackend(ctx context.Context, req *tempopb.QueryRangeRequest) (*tempopb.QueryRangeResponse, error) {
	tenantID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	eval, err := traceql.NewEngine().CompileMetricsQueryRange(req, true)
	if err != nil {
		return nil, err
	}

	// Get blocks that overlap this time range
	metas := q.store.BlockMetas(tenantID)
	withinTimeRange := metas[:0]
	for _, m := range metas {
		if m.StartTime.UnixNano() <= int64(req.End) && m.EndTime.UnixNano() > int64(req.Start) {
			withinTimeRange = append(withinTimeRange, m)
		}
	}

	var (
		inspectedBytes  = atomic.Uint64{}
		totalBlockBytes = uint64(0)
		wg              = boundedwaitgroup.New(3)
		jobErr          = atomic.Error{}
	)

	for _, m := range withinTimeRange {

		wg.Add(1)
		totalBlockBytes += m.Size

		go func(m *backend.BlockMeta) {
			defer wg.Done()

			span, ctx2 := opentracing.StartSpanFromContext(ctx, "querier.queryBackEnd.Block", opentracing.Tags{
				"block":     m.BlockID.String(),
				"blockSize": m.Size,
			})
			defer span.Finish()

			var resp traceql.FetchSpansResponse
			defer func() {
				if resp.Bytes != nil {
					span.SetTag("inspectedBytes", resp.Bytes())
					inspectedBytes.Add(resp.Bytes())
				}
			}()

			f := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
				resp, err = q.store.Fetch(ctx, m, req, common.DefaultSearchOptions())
				return resp, err
			})

			err := eval.Do(ctx2, f)
			if err != nil {
				jobErr.Store(err)
			}
		}(m)
	}

	wg.Wait()

	if err := jobErr.Load(); err != nil {
		return nil, err
	}

	res, err := eval.Results()
	if err != nil {
		return nil, err
	}

	return &tempopb.QueryRangeResponse{
		Series: queryRangeTraceQLToProto(res, req),
		Metrics: &tempopb.SearchMetrics{
			TotalBlocks:     uint32(len(withinTimeRange)),
			TotalBlockBytes: totalBlockBytes,
			InspectedBytes:  inspectedBytes.Load(),
		},
	}, nil
}

func queryRangeTraceQLToProto(set traceql.SeriesSet, req *tempopb.QueryRangeRequest) []*tempopb.TimeSeries {
	resp := make([]*tempopb.TimeSeries, 0, len(set))

	for promLabels, s := range set {
		labels := make([]v1.KeyValue, 0, len(s.Labels))
		for _, label := range s.Labels {
			labels = append(labels,
				v1.KeyValue{
					Key:   label.Name,
					Value: traceql.NewStaticString(label.Value).AsAnyValue(),
				},
			)
		}

		intervals := traceql.IntervalCount(req.Start, req.End, req.Step)
		samples := make([]tempopb.Sample, 0, intervals)
		for i, value := range s.Values {

			ts := traceql.TimestampOf(uint64(i), req.Start, req.Step)

			samples = append(samples, tempopb.Sample{
				TimestampMs: time.Unix(0, int64(ts)).UnixMilli(),
				Value:       value,
			})
		}

		ss := &tempopb.TimeSeries{
			PromLabels: promLabels,
			Labels:     labels,
			Samples:    samples,
		}

		resp = append(resp, ss)
	}

	return resp
}
