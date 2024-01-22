package traceql

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	commonv1 "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestDefaultQueryRangeStep(t *testing.T) {
	tc := []struct {
		start, end time.Time
		expected   time.Duration
	}{
		{time.Unix(0, 0), time.Unix(100, 0), time.Second},
		{time.Unix(0, 0), time.Unix(600, 0), 2 * time.Second},
		{time.Unix(0, 0), time.Unix(3600, 0), 15 * time.Second},
	}

	for _, c := range tc {
		require.Equal(t, c.expected, time.Duration(DefaultQueryRangeStep(uint64(c.start.UnixNano()), uint64(c.end.UnixNano()))))
	}
}

func TestStepRangeToIntervals(t *testing.T) {
	tc := []struct {
		start, end, step uint64
		expected         int
	}{
		{
			start:    0,
			end:      1,
			step:     1,
			expected: 2, // 0, 1, even multiple
		},
		{
			start:    0,
			end:      10,
			step:     3,
			expected: 4, // 0, 3, 6, 9
		},
	}

	for _, c := range tc {
		require.Equal(t, c.expected, IntervalCount(c.start, c.end, c.step))
	}
}

func TestTimestampOf(t *testing.T) {
	tc := []struct {
		interval, start, step uint64
		expected              uint64
	}{
		{
			expected: 0,
		},
		{
			interval: 2,
			start:    10,
			step:     3,
			expected: 16,
		},
	}

	for _, c := range tc {
		require.Equal(t, c.expected, TimestampOf(c.interval, c.start, c.step))
	}
}

func TestIntervalOf(t *testing.T) {
	tc := []struct {
		ts, start, end, step uint64
		expected             int
	}{
		{expected: -1},
		{
			ts:   0,
			end:  1,
			step: 1,
		},
		{
			ts:       10,
			end:      10,
			step:     1,
			expected: 10,
		},
	}

	for _, c := range tc {
		require.Equal(t, c.expected, IntervalOf(c.ts, c.start, c.end, c.step))
	}
}

func TestCompileMetricsQueryRange(t *testing.T) {
	tc := map[string]struct {
		q           string
		start, end  uint64
		step        uint64
		expectedErr error
	}{
		"start": {
			expectedErr: fmt.Errorf("start required"),
		},
		"end": {
			start:       1,
			expectedErr: fmt.Errorf("end required"),
		},
		"range": {
			start:       2,
			end:         1,
			expectedErr: fmt.Errorf("end must be greater than start"),
		},
		"step": {
			start:       1,
			end:         2,
			expectedErr: fmt.Errorf("step required"),
		},
		"notmetrics": {
			start:       1,
			end:         2,
			step:        3,
			q:           "{}",
			expectedErr: fmt.Errorf("not a metrics query"),
		},
		"notsupported": {
			start:       1,
			end:         2,
			step:        3,
			q:           "{} | rate() by (.a,.b,.c,.d,.e,.f)",
			expectedErr: fmt.Errorf("compiling query: metrics group by 6 values not yet supported"),
		},
		"ok": {
			start: 1,
			end:   2,
			step:  3,
			q:     "{} | rate()",
		},
	}

	for n, c := range tc {
		t.Run(n, func(t *testing.T) {
			_, err := NewEngine().CompileMetricsQueryRange(&tempopb.QueryRangeRequest{
				Query: c.q,
				Start: c.start,
				End:   c.end,
				Step:  c.step,
			}, false)

			if c.expectedErr != nil {
				require.EqualError(t, err, c.expectedErr.Error())
			}
		})
	}
}

func TestCompileMetricsQueryRangeFetchSpansRequest(t *testing.T) {
	tc := map[string]struct {
		q           string
		shardID     uint32
		shardCount  uint32
		dedupe      bool
		expectedReq FetchSpansRequest
	}{
		"minimal": {
			q: "{} | rate()",
			expectedReq: FetchSpansRequest{
				AllConditions: true,
				Conditions: []Condition{
					{
						// In this case start time is in the first pass
						Attribute: NewIntrinsic(IntrinsicSpanStartTime),
					},
				},
			},
		},
		"dedupe": {
			q:      "{} | rate()",
			dedupe: true,
			expectedReq: FetchSpansRequest{
				AllConditions: true,
				Conditions: []Condition{
					{
						Attribute: NewIntrinsic(IntrinsicSpanStartTime),
					},
					{
						Attribute: NewIntrinsic(IntrinsicTraceID), // Required for dedupe
					},
				},
			},
		},
		"complex": {
			q:          "{duration > 10s} | rate() by (resource.service.name)",
			shardID:    123,
			shardCount: 456,
			expectedReq: FetchSpansRequest{
				AllConditions: true,
				ShardID:       123,
				ShardCount:    456,
				Conditions: []Condition{
					{
						Attribute: NewIntrinsic(IntrinsicDuration),
						Op:        OpGreater,
						Operands:  Operands{NewStaticDuration(10 * time.Second)},
					},
					{
						Attribute: NewIntrinsic(IntrinsicTraceID), // Required for sharding
					},
					{
						Attribute: NewIntrinsic(IntrinsicSpanStartTime),
					},
				},
				SecondPassConditions: []Condition{
					{
						// Group-by attributes (non-intrinsic) must be in the second pass
						Attribute: NewScopedAttribute(AttributeScopeResource, false, "service.name"),
					},
				},
			},
		},
	}

	for n, tc := range tc {
		t.Run(n, func(t *testing.T) {
			eval, err := NewEngine().CompileMetricsQueryRange(&tempopb.QueryRangeRequest{
				Query:      tc.q,
				ShardID:    tc.shardID,
				ShardCount: tc.shardCount,
				Start:      1,
				End:        2,
				Step:       3,
			}, tc.dedupe)
			require.NoError(t, err)

			// Nil out func to Equal works
			eval.storageReq.SecondPass = nil
			require.Equal(t, tc.expectedReq, *eval.storageReq)
		})
	}
}

func TestSeriesSetToProto(t *testing.T) {
	req := tempopb.QueryRangeRequest{
		Start: 0,
		End:   uint64(3 * time.Minute), // 3 minute window
		Step:  uint64(30 * time.Second),
	}

	ts := SeriesSet{
		"": TimeSeries{
			Labels: labels.FromStrings("a", "b"),
			Values: []float64{6, 5, 4, 3, 2, 1, 0},
		},
	}.ToProto(req)

	expected := []*tempopb.TimeSeries{
		{
			Labels: []commonv1.KeyValue{
				{
					Key: "a",
					Value: &commonv1.AnyValue{
						Value: &commonv1.AnyValue_StringValue{StringValue: "b"},
					},
				},
			},
			Samples: []tempopb.Sample{
				{TimestampMs: 0, Value: 6},
				{TimestampMs: 30000, Value: 5},
				{TimestampMs: 60000, Value: 4},
				{TimestampMs: 90000, Value: 3},
				{TimestampMs: 120000, Value: 2},
				{TimestampMs: 150000, Value: 1},
				{TimestampMs: 180000, Value: 0},
			},
		},
	}

	require.Equal(t, expected, ts)
}
