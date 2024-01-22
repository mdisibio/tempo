package querier

import (
	"context"
	"fmt"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/grafana/tempo/pkg/boundedwaitgroup"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/atomic"
)

func (q *Querier) QueryRange(ctx context.Context, req *tempopb.QueryRangeRequest) (*tempopb.QueryRangeResponse, error) {
	if req.QueryMode == QueryModeRecent {
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
	span, ctx := opentracing.StartSpanFromContext(ctx, "querier.queryBackend")
	defer span.Finish()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

	wg := boundedwaitgroup.New(2)
	jobErr := atomic.Error{}

	for _, m := range withinTimeRange {
		// If a job errored then quit immediately.
		if err := jobErr.Load(); err != nil {
			return nil, err
		}

		wg.Add(1)
		go func(m *backend.BlockMeta) {
			defer wg.Done()

			span, ctx := opentracing.StartSpanFromContext(ctx, "querier.queryBackend.block", opentracing.Tags{
				"block":     m.BlockID.String(),
				"blockSize": m.Size,
			})
			defer span.Finish()

			f := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
				return q.store.Fetch(ctx, m, req, common.DefaultSearchOptions())
			})

			err := eval.Do(ctx, f)
			if err != nil {
				jobErr.Store(err)
			}
		}(m)
	}

	wg.Wait()
	if err := jobErr.Load(); err != nil {
		return nil, err
	}

	res := eval.Results()
	bytes, _, _ := eval.Metrics()

	span.SetTag("inspectedBytes", bytes)

	return &tempopb.QueryRangeResponse{
		Series: res.ToProto(*req),
		Metrics: &tempopb.SearchMetrics{
			InspectedBytes: bytes,
		},
	}, nil
}
