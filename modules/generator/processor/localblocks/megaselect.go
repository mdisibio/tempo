package localblocks

import (
	"context"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/traceqlmetrics"
	"github.com/grafana/tempo/tempodb/encoding/common"
)

func (p *Processor) MegaSelect(ctx context.Context, req *tempopb.SpanMetricsMegaSelectRequest) (*tempopb.MegaSelectRawResponse, error) {
	p.blocksMtx.RLock()
	defer p.blocksMtx.RUnlock()

	var (
		err       error
		startNano = uint64(time.Unix(int64(req.Start), 0).UnixNano())
		endNano   = uint64(time.Unix(int64(req.End), 0).UnixNano())
	)

	// Blocks to check
	blocks := make([]common.BackendBlock, 0, 1+len(p.walBlocks)+len(p.completeBlocks))
	if p.headBlock != nil {
		blocks = append(blocks, p.headBlock)
	}
	for _, b := range p.walBlocks {
		blocks = append(blocks, b)
	}
	for _, b := range p.completeBlocks {
		blocks = append(blocks, b)
	}

	m := traceqlmetrics.NewGrubbleResults()
	for _, b := range blocks {

		var (
		// meta       = b.BlockMeta()
		// blockStart = uint32(meta.StartTime.Unix())
		// blockEnd   = uint32(meta.EndTime.Unix())
		// We can only cache the results of this query on this block
		// if the time range fully covers this block (not partial).
		// cacheable = req.Start <= blockStart && req.End >= blockEnd
		// Including the trace count in the cache key means we can safely
		// cache results for a wal block which can receive new data
		// key = fmt.Sprintf("b:%s-c:%d-q:%s-g:%s", meta.BlockID.String(), meta.TotalObjects, req.Query, req.GroupBy)
		)

		var r *traceqlmetrics.GrubbleResults

		//if cacheable {
		//	r = p.metricsCacheGet(key)
		//}

		// Uncacheable or not found in cache
		if r == nil {

			f := traceql.NewSpansetFetcherWrapper(func(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
				return b.Fetch(ctx, req, common.DefaultSearchOptions())
			})

			r, err = traceqlmetrics.MegaSelect(ctx, req.Query, startNano, endNano, f)
			if err != nil {
				return nil, err
			}

			//if cacheable {
			//	p.metricsCacheSet(key, r)
			//}
		}

		m.Combine(r)
	}

	// Convert to proto
	resp := &tempopb.MegaSelectRawResponse{}
	for l, series := range m.Series {

		s := &tempopb.MegaSelectRawSeries{
			Label: &tempopb.KeyValue{
				Key:   l.Key.String(),
				Value: staticToProto(l.Value),
			},
			Timeseries: make([]*tempopb.MegaSelectRawHistogram, 0, len(series.Timestamps)),
		}

		for timestamp, histogram := range series.Timestamps {

			h := []*tempopb.RawHistogram{}
			for bucket, count := range histogram.Buckets {
				if count > 0 {
					h = append(h, &tempopb.RawHistogram{
						Bucket: uint64(bucket),
						Count:  uint64(count),
					})
				}
			}

			s.Timeseries = append(s.Timeseries, &tempopb.MegaSelectRawHistogram{
				Time:             timestamp,
				LatencyHistogram: h,
				Exemplar: &tempopb.RawExemplar{
					TraceID: histogram.ExemplarTraceID,
					Val:     histogram.ExemplarDurationNano,
				},
			})
		}

		resp.Series = append(resp.Series, s)
	}

	return resp, nil
}
