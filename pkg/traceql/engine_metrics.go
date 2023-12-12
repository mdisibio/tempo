package traceql

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util"
)

func DefaultQueryRangeStep(start, end uint64) uint64 {

	delta := time.Duration(end - start)

	// Try to get this many data points
	// Our baseline is is 1 hour @ 15s intervals
	baseline := delta / 240

	// Round down in intervals of 5s
	interval := baseline / (5 * time.Second) * (5 * time.Second)

	if interval < 5*time.Second {
		// Round down in intervals of 1s
		interval = baseline / time.Second * time.Second
	}

	if interval < time.Second {
		return uint64(time.Second.Nanoseconds())
	}

	return uint64(interval.Nanoseconds())
}

// IntervalCount is the number of intervals in the range with step.
func IntervalCount(start, end, step uint64) int {
	intervals := (end - start) / step
	intervals++
	return int(intervals)
}

// TimestampOf the given interval with the start and step.
func TimestampOf(interval, start, step uint64) uint64 {
	return start + interval*step
}

// IntervalOf the given timestamp within the range and step.
func IntervalOf(ts, start, end, step uint64) int {
	if ts < start || ts > end || end == start || step == 0 {
		// Invalid
		return -1
	}

	return int((ts - start) / step)
}

const maxGroupBys = 5 // TODO - Delete me
type FastValues [maxGroupBys]Static

type TimeSeries struct {
	//Labels LabelSet
	Labels labels.Labels
	Values []float64
}

// TODO - Make an analog of me in tempopb proto for over-the-wire transmission
type SeriesSet map[string]TimeSeries

// VectorAggregator turns a vector of spans into a single numeric scalar
type VectorAggregator interface {
	Observe(s Span)
	Sample() float64
}

// RangeAggregator sorts spans into time slots
// TODO - for efficiency we probably combine this with VectorAggregator (see todo about CountOverTimeAggregator)
type RangeAggregator interface {
	Observe(s Span)
	Samples() []float64
}

// SpanAggregator sorts spans into series
type SpanAggregator interface {
	Observe(Span)
	Series() SeriesSet
}

// CountOverTimeAggregator counts the number of spans. It can also
// calculate the rate when given a multiplier.
// TODO - Rewrite me to be []float64 which is more efficient
type CountOverTimeAggregator struct {
	count    float64
	rateMult float64
}

var _ VectorAggregator = (*CountOverTimeAggregator)(nil)

func NewCountOverTimeAggregator() *CountOverTimeAggregator {
	return &CountOverTimeAggregator{
		rateMult: 1.0,
	}
}

func NewRateAggregator(rateMult float64) *CountOverTimeAggregator {
	return &CountOverTimeAggregator{
		rateMult: rateMult,
	}
}

func (c *CountOverTimeAggregator) Observe(_ Span) {
	c.count++
}

func (c *CountOverTimeAggregator) Sample() float64 {
	return c.count * c.rateMult
}

// StepAggregator sorts spans into time slots using a step interval like 30s or 1m
type StepAggregator struct {
	start   uint64
	end     uint64
	step    uint64
	vectors []VectorAggregator
}

var _ RangeAggregator = (*StepAggregator)(nil)

func NewStepAggregator(start, end, step uint64, innerAgg func() VectorAggregator) *StepAggregator {
	intervals := IntervalCount(start, end, step)
	vectors := make([]VectorAggregator, intervals)
	for i := range vectors {
		vectors[i] = innerAgg()
	}

	return &StepAggregator{
		start:   start,
		end:     end,
		step:    step,
		vectors: vectors,
	}
}

func (s *StepAggregator) Observe(span Span) {
	interval := IntervalOf(span.StartTimeUnixNanos(), s.start, s.end, s.step)
	if interval == -1 {
		return
	}
	s.vectors[interval].Observe(span)
}

func (s *StepAggregator) Samples() []float64 {
	ss := make([]float64, len(s.vectors))
	for i, v := range s.vectors {
		ss[i] = v.Sample()
	}
	return ss
}

type GroupingAggregator struct {
	// Config
	by        []Attribute   // Original attributes: .foo
	byLookups [][]Attribute // Lookups: span.foo resource.foo
	innerAgg  func() RangeAggregator

	// Data
	series map[FastValues]RangeAggregator
	buf    FastValues
}

var _ SpanAggregator = (*GroupingAggregator)(nil)

func NewGroupingAggregator(aggName string, innerAgg func() RangeAggregator, by []Attribute) SpanAggregator {
	if len(by) == 0 {
		return &UngroupedAggregator{
			name:     aggName,
			innerAgg: innerAgg(),
		}
	}

	lookups := make([][]Attribute, len(by))
	for i, attr := range by {
		if attr.Intrinsic == IntrinsicNone && attr.Scope == AttributeScopeNone {
			// Unscoped attribute. Check span-level, then resource-level.
			lookups[i] = []Attribute{
				NewScopedAttribute(AttributeScopeSpan, false, attr.Name),
				NewScopedAttribute(AttributeScopeResource, false, attr.Name),
			}
		} else {
			lookups[i] = []Attribute{attr}
		}
	}

	return &GroupingAggregator{
		series:    map[FastValues]RangeAggregator{},
		by:        by,
		byLookups: lookups,
		innerAgg:  innerAgg,
	}
}

func (g *GroupingAggregator) Observe(span Span) {

	// Get grouping values
	for i, lookups := range g.byLookups {
		g.buf[i] = lookup(lookups, span)
	}

	agg, ok := g.series[g.buf]
	if !ok {
		agg = g.innerAgg()
		g.series[g.buf] = agg
	}

	agg.Observe(span)
}

// labelsFor gives the final labels for the series. Slower and can't be on the hot path.
// This is tweaked to match what prometheus does.  For grouped metrics we don't
// include the metric name, just the group labels.
// rate() by (x) => {x=a}, {x=b}, ...
func (g *GroupingAggregator) labelsFor(vals FastValues) labels.Labels {
	b := labels.NewBuilder(nil)

	any := false
	for i, v := range vals {
		if v.Type != TypeNil {
			b.Set(g.by[i].String(), v.EncodeToString(false))
			any = true
		}
	}

	if !any {
		// Force at least 1 label or else this series is displayed weird
		b.Set(g.by[0].String(), "<nil>")
	}

	return b.Labels()
}

func (g *GroupingAggregator) Series() SeriesSet {
	ss := SeriesSet{}

	for vals, agg := range g.series {
		l := g.labelsFor(vals)

		ss[l.String()] = TimeSeries{
			Labels: l,
			Values: agg.Samples(),
		}
	}

	return ss
}

// UngroupedAggregator builds a single series with no labels. e.g. {} | rate()
type UngroupedAggregator struct {
	name     string
	innerAgg RangeAggregator
}

var _ SpanAggregator = (*UngroupedAggregator)(nil)

func (u *UngroupedAggregator) Observe(span Span) {
	u.innerAgg.Observe(span)
}

// Series output.
// This is tweaked to match what prometheus does.  For ungrouped metrics we
// fill in a placeholder metric name with the name of the aggregation.
// rate() => {__name__=rate}
func (u *UngroupedAggregator) Series() SeriesSet {
	l := labels.FromStrings(labels.MetricName, u.name)
	return SeriesSet{
		l.String(): {
			Labels: l,
			Values: u.innerAgg.Samples(),
		},
	}
}

// ExecuteMetricsQueryRange - Execute the given metrics query. Just a wrapper around CompileMetricsQueryRange
func (e *Engine) ExecuteMetricsQueryRange(ctx context.Context, req *tempopb.QueryRangeRequest, fetcher SpansetFetcher) (results SeriesSet, err error) {
	eval, err := e.CompileMetricsQueryRange(req, false)
	if err != nil {
		return nil, err
	}

	err = eval.Do(ctx, fetcher)
	if err != nil {
		return nil, err
	}

	return eval.Results()
}

// CompileMetricsQueryRange returns an evalulator that can be reused across multiple data sources.
func (e *Engine) CompileMetricsQueryRange(req *tempopb.QueryRangeRequest, dedupeSpans bool) (*MetricsEvalulator, error) {
	if req.Start <= 0 {
		return nil, fmt.Errorf("start required")
	}
	if req.End <= 0 {
		return nil, fmt.Errorf("end required")
	}
	if req.End <= req.Start {
		return nil, fmt.Errorf("end must be greater than start")
	}
	if req.Step <= 0 {
		return nil, fmt.Errorf("step required")
	}

	eval, metricsPipeline, storageReq, err := e.Compile(req.Query)
	if err != nil {
		return nil, fmt.Errorf("compiling query: %w", err)
	}

	if metricsPipeline == nil {
		return nil, fmt.Errorf("not a metrics query")
	}

	// This initializes all step buffers, counters, etc
	metricsPipeline.init(req)

	me := &MetricsEvalulator{
		storageReq:      storageReq,
		metricsPipeline: metricsPipeline,
		dedupeSpans:     dedupeSpans,
	}

	if req.Of > 1 {
		// Trace id sharding
		// Select traceID if not already present.  It must be in the first pass
		// so that we only evalulate our traces.
		storageReq.Shard = int(req.Shard)
		storageReq.Of = int(req.Of)
		traceID := NewIntrinsic(IntrinsicTraceID)
		if !storageReq.HasAttribute(traceID) {
			storageReq.Conditions = append(storageReq.Conditions, Condition{Attribute: traceID})
		}
	}

	// Sharding algorithm
	// In order to scale out queries like {A} >> {B} | rate()  we need specific
	// rules about the data is divided across time boundary shards.  These
	// spans can cross hours or days and the simple idea to just check span
	// start time won't work.
	// Therefore results are matched with the following rules:
	// (1) Evalulate the query for any overlapping trace
	// (2) For any matching spans: only include the ones that started in this time frame.
	// This will increase redundant trace evalulation, but it ensures that matching spans are
	// gauranteed to be found and included in the metrcs.
	startTime := NewIntrinsic(IntrinsicSpanStartTime)
	if !storageReq.HasAttribute(startTime) {
		if storageReq.AllConditions {
			// The most efficient case.  We can add it to the primary pass
			// without affecting correctness. And this lets us avoid the
			// entire second pass.
			storageReq.Conditions = append(storageReq.Conditions, Condition{Attribute: startTime})
		} else {
			// Complex query with a second pass. In this case it is better to
			// add it to the second pass so that it's only returned for the matches.
			storageReq.SecondPassConditions = append(storageReq.SecondPassConditions, Condition{Attribute: startTime})
		}
	}

	// Special optimization for queries like {} | rate() by (rootName)
	// If first pass is only StartTime, then move any intrinsics to the first
	// pass and try to avoid a second pass.  It's ok and beneficial to move
	// intrinsics because they exist for every span and are never nil.
	// But we can't move attributes because that changes the handling of nils.
	// Moving attributes to the first pass is like asserting non-nil on them.
	// TODO

	if dedupeSpans {
		// We dedupe based on trace ID and start time.  Obviously this doesn't
		// work if 2 spans have the same start time, but this doesn't impose any
		// more data on the query when sharding is already present above (most cases)
		traceID := NewIntrinsic(IntrinsicTraceID)
		if !storageReq.HasAttribute(traceID) {
			storageReq.Conditions = append(storageReq.Conditions, Condition{Attribute: traceID})
		}
	}

	// (1) any overlapping trace
	// TODO - Make this dynamic since it can be faster to skip
	// the trace-level timestamp check when all or most of the traces
	// overlap the window.
	//storageReq.StartTimeUnixNanos = req.Start
	//storageReq.EndTimeUnixNanos = req.End // Should this be exclusive?
	// (2) Only include spans that started in this time frame.
	//     This is checked inside the evaluator
	me.checkTime = true
	me.start = req.Start
	me.end = req.End

	// Avoid a second pass when not needed for much better performance.
	// TODO - Is there any case where eval() needs to be called but AllConditions=true?
	if !storageReq.AllConditions || len(storageReq.SecondPassConditions) > 0 {
		storageReq.SecondPass = func(s *Spanset) ([]*Spanset, error) {
			return eval([]*Spanset{s})
		}
	}

	return me, nil
}

func lookup(needles []Attribute, haystack Span) Static {
	for _, n := range needles {
		if v, ok := haystack.AttributeFor(n); ok {
			return v
		}
	}

	return Static{}
}

type MetricsEvalulator struct {
	start, end  uint64
	checkTime   bool
	dedupeSpans bool
	//deduper         *SpanBloomDeduper
	deduper         *SpanDeduper2
	storageReq      *FetchSpansRequest
	metricsPipeline metricsFirstStageElement
	count           int
	deduped         int
	mtx             sync.Mutex
}

func (e *MetricsEvalulator) Do(ctx context.Context, f SpansetFetcher) error {
	fetch, err := f.Fetch(ctx, *e.storageReq)
	if errors.Is(err, util.ErrUnsupported) {
		return nil
	}
	if err != nil {
		return err
	}

	if e.dedupeSpans && e.deduper == nil {
		e.deduper = NewSpanDeduper2()
		//e.deduper = NewSpanBloomDeduper()
	}

	defer fetch.Results.Close()

	for {
		ss, err := fetch.Results.Next(ctx)
		if err != nil {
			return err
		}
		if ss == nil {
			break
		}

		for _, s := range ss.Spans {
			if e.checkTime {
				st := s.StartTimeUnixNanos()
				if st < e.start || st >= e.end {
					continue
				}
			}

			e.mtx.Lock()
			if e.dedupeSpans && e.deduper.Skip(ss.TraceID, s.StartTimeUnixNanos()) {
				e.deduped++
				e.mtx.Unlock()
				continue
			}

			e.count++
			e.metricsPipeline.observe(s)
			e.mtx.Unlock()
		}

		ss.Release()
	}

	return nil
}

func (e *MetricsEvalulator) SpanCount() {
	fmt.Println(e.count, e.deduped)
}

func (e *MetricsEvalulator) Results() (SeriesSet, error) {
	return e.metricsPipeline.result(), nil
}

// SpanDeduper using sharded maps in-memory.  So far this is the most
// performant.  We are effectively only using the bottom 5 bytes of
// every span ID.  Byte [3] is used to select a sharded map and the
// next 4 are the uint32 within that map. Is this good enough? Maybe... let's find out!
/*type SpanDeduper struct {
	m []map[uint32]struct{}
}

func NewSpanDeduper() *SpanDeduper {
	maps := make([]map[uint32]struct{}, 256)
	for i := range maps {
		maps[i] = make(map[uint32]struct{}, 1000)
	}
	return &SpanDeduper{
		m: maps,
	}
}

func (d *SpanDeduper) Skip(id []byte) bool {
	if len(id) != 8 {
		return false
	}

	m := d.m[id[3]]
	v := binary.BigEndian.Uint32(id[4:8])

	if _, ok := m[v]; ok {
		return true
	}

	m[v] = struct{}{}
	return false
}*/

/*
// This dedupes span IDs using a chain of bloom filters.  I thought it
// was going to be awesome but it is WAY TOO SLOW
type SpanBloomDeduper struct {
	blooms []*bloom.BloomFilter
	count  int
}

func NewSpanBloomDeduper() *SpanBloomDeduper {
	//first := bloom.NewWithEstimates(1_000_000, 0.001)
	first := bloom.New(999499917, 1)

	return &SpanBloomDeduper{
		blooms: []*bloom.BloomFilter{first},
	}
}

func (d *SpanBloomDeduper) Skip(id []byte) bool {

	for _, b := range d.blooms {
		if b.Test(id) {
			return true
		}
	}

	d.count++
	if d.count%1_000_000 == 0 {
		// Time for another filter
		//d.blooms = append(d.blooms, bloom.NewWithEstimates(1_000_000, 0.001))
		d.blooms = append(d.blooms, bloom.New(999499917, 1))
	}

	// Set in latest filter
	d.blooms[len(d.blooms)-1].Add(id)

	return false
}*/

// SpanDeduper2 is EXTREMELY LAZY. It attempts to dedupe spans for metrics
// without requiring any new data fields.  It uses trace ID and span start time
// which are already loaded. This of course terrible, but did I mention that
// this is extremely lazy?  Additionally it uses sharded maps by the lowest byte
// of the trace ID to reduce the pressure on any single map.  Maybe it's good enough.  Let's find out!
type SpanDeduper2 struct {
	m       []map[uint32]struct{}
	h       hash.Hash32
	buf     []byte
	traceID Attribute
}

func NewSpanDeduper2() *SpanDeduper2 {
	maps := make([]map[uint32]struct{}, 256)
	for i := range maps {
		maps[i] = make(map[uint32]struct{}, 1000)
	}
	return &SpanDeduper2{
		m:       maps,
		h:       fnv.New32a(),
		buf:     make([]byte, 8),
		traceID: NewIntrinsic(IntrinsicTraceID),
	}
}

func (d *SpanDeduper2) Skip(tid []byte, startTime uint64) bool {

	d.h.Reset()
	d.h.Write([]byte(tid))
	binary.BigEndian.PutUint64(d.buf, startTime)
	d.h.Write(d.buf)

	v := d.h.Sum32()
	m := d.m[tid[len(tid)-1]]

	if _, ok := m[v]; ok {
		return true
	}

	m[v] = struct{}{}
	return false
}
