package traceqlmetrics

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util"
	"github.com/pkg/errors"
)

type Label struct {
	Key   traceql.Attribute
	Value traceql.Static
}

type GrubbleTimeSeries struct {
	TotalCount int
	Label      Label
	Timestamps map[uint32]*LatencyHistogram
}

func NewGrubbleTimeSeries(v Label) *GrubbleTimeSeries {
	return &GrubbleTimeSeries{
		Label:      v,
		Timestamps: map[uint32]*LatencyHistogram{},
	}
}

func (ts *GrubbleTimeSeries) Record(timestamp uint32, durationNanos uint64, traceID []byte) {
	ts.TotalCount++

	s := ts.Timestamps[timestamp]
	if s == nil {
		s = &LatencyHistogram{}
		ts.Timestamps[timestamp] = s
	}
	s.RecordExemplar(durationNanos, traceID)
}

func (ts *GrubbleTimeSeries) Combine(other *GrubbleTimeSeries) {
	for timestamp, otherh := range other.Timestamps {
		h := ts.Timestamps[timestamp]
		if h == nil {
			h = &LatencyHistogram{}
			ts.Timestamps[timestamp] = h
		}
		h.Combine(*otherh)
	}
}

func (ts *GrubbleTimeSeries) PercentileVector(p float64) ([]uint32, []uint64) {
	timestamps := make([]uint32, 0, len(ts.Timestamps))
	for i := range ts.Timestamps {
		timestamps = append(timestamps, i)
	}
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})

	percentiles := make([]uint64, 0, len(ts.Timestamps))
	for _, i := range timestamps {
		percentiles = append(percentiles, ts.Timestamps[i].Percentile(p))
	}

	return timestamps, percentiles
}

func (ts *GrubbleTimeSeries) PercentileVectorWithExemplar(p float64) ([]uint32, []uint64, [][]byte, []uint64) {
	timestamps := make([]uint32, 0, len(ts.Timestamps))
	for i := range ts.Timestamps {
		timestamps = append(timestamps, i)
	}
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})

	percentiles := make([]uint64, 0, len(ts.Timestamps))
	traceIDs := make([][]byte, 0, len(ts.Timestamps))
	durations := make([]uint64, 0, len(ts.Timestamps))
	for _, i := range timestamps {
		percentiles = append(percentiles, ts.Timestamps[i].Percentile(p))
		traceIDs = append(traceIDs, ts.Timestamps[i].ExemplarTraceID)
		durations = append(durations, ts.Timestamps[i].ExemplarDurationNano)
	}

	return timestamps, percentiles, traceIDs, durations
}

type GrubbleResults struct {
	Series map[Label]*GrubbleTimeSeries
}

func NewGrubbleResults() *GrubbleResults {
	return &GrubbleResults{
		Series: map[Label]*GrubbleTimeSeries{},
	}
}

func (m *GrubbleResults) Record(v Label, timestamp uint32, durationNanos uint64, traceID []byte) {
	s := m.Series[v]
	if s == nil {
		s = NewGrubbleTimeSeries(v)
		s.Label = v
		m.Series[v] = s
	}
	s.Record(timestamp, durationNanos, traceID)
}

func (m *GrubbleResults) Combine(other *GrubbleResults) {
	for l, otherSeries := range other.Series {
		s := m.Series[l]
		if s == nil {
			s = NewGrubbleTimeSeries(l)
			m.Series[l] = s
		}
		s.Combine(otherSeries)
	}
}

func (m *GrubbleResults) CombineTimeseries(other *GrubbleTimeSeries) {
	l := other.Label
	s := m.Series[l]
	if s == nil {
		s = NewGrubbleTimeSeries(l)
		m.Series[l] = s
	}
	s.Combine(other)
}

func (m *GrubbleResults) Sorted() []*GrubbleTimeSeries {
	all := make([]*GrubbleTimeSeries, 0, len(m.Series))

	for _, s := range m.Series {
		all = append(all, s)
	}

	sort.Slice(all, func(i, j int) bool {
		switch strings.Compare(all[i].Label.Key.String(), all[j].Label.Key.String()) {
		case -1:
			return true
		case 0:
			return strings.Compare(all[i].Label.Value.String(), all[j].Label.Value.String()) == -1
		default:
			return false
		}
	})
	return all
}

func MegaSelect(ctx context.Context, query string, start, end uint64, fetcher traceql.SpansetFetcher) (*GrubbleResults, error) {
	eval, req, err := traceql.NewEngine().Compile(query)
	if err != nil {
		return nil, errors.Wrap(err, "compiling query")
	}

	var (
		duration = traceql.NewIntrinsic(traceql.IntrinsicDuration)
		// name       = traceql.NewIntrinsic(traceql.IntrinsicName)
		startTime  = traceql.NewIntrinsic(traceql.IntrinsicSpanStartTime)
		startValue = traceql.NewStaticInt(int(start))
		status     = traceql.NewIntrinsic(traceql.IntrinsicStatus)
		interval   = uint64(time.Minute)
		// statusErr  = traceql.NewStaticStatus(traceql.StatusError)
		// spanCount  = 0
		// results    = NewMetricsResults()
	)

	if start > 0 {
		req.StartTimeUnixNanos = start
		req.Conditions = append(req.Conditions, traceql.Condition{Attribute: startTime, Op: traceql.OpGreaterEqual, Operands: []traceql.Static{startValue}})
	}
	if end > 0 {
		req.EndTimeUnixNanos = end
		// There is only an intrinsic for the span start time, so use it as the cutoff.
		req.Conditions = append(req.Conditions, traceql.Condition{Attribute: startTime, Op: traceql.OpLess, Operands: []traceql.Static{startValue}})
	}

	// Ensure that we select the span duration, status, and group-by attributes
	// in the second pass if they are not already part of the first pass.
	addConditionIfNotPresent := func(a traceql.Attribute) {
		for _, c := range req.Conditions {
			if c.Attribute == a {
				return
			}
		}

		req.SecondPassConditions = append(req.SecondPassConditions, traceql.Condition{Attribute: a})
	}
	addConditionIfNotPresent(status)
	addConditionIfNotPresent(duration)
	// addConditionIfNotPresent(name)
	addConditionIfNotPresent(startTime)
	req.SecondPassConditions = append(req.SecondPassConditions, traceql.Condition{Attribute: traceql.NewIntrinsic(traceql.IntrinsicTraceID)}, traceql.Condition{MegaSelect: true})

	req.SecondPass = func(s *traceql.Spanset) ([]*traceql.Spanset, error) {
		return eval([]*traceql.Spanset{s})
	}

	// Perform the fetch and process the results inside the SecondPass
	// callback.  No actual results will be returned from this fetch call,
	// But we still need to call Next() at least once.
	res, err := fetcher.Fetch(ctx, *req)
	if err == util.ErrUnsupported {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	defer res.Results.Close()

	results := NewGrubbleResults()

	for {
		ss, err := res.Results.Next(ctx)
		if err != nil {
			return nil, err
		}
		if ss == nil {
			break
		}

		for _, s := range ss.Spans {

			startTime := uint32(time.Unix(0, int64(s.StartTimeUnixNanos()/interval*interval)).Unix())
			dur := s.DurationNanos()

			// All-span series
			results.Record(Label{}, startTime, dur, ss.TraceID)

			// Per-attribute series
			for k, v := range s.Attributes() {
				// fmt.Println("  ", k, v)
				if k != duration {
					results.Record(Label{k, v}, startTime, dur, ss.TraceID)
				}
			}
		}

		ss.Release()
	}

	return results, nil
}
