package vparquet

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/google/uuid"
	tempo_io "github.com/grafana/tempo/pkg/io"
	"github.com/grafana/tempo/pkg/tempopb"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util"
	"github.com/grafana/tempo/pkg/util/test"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/encoding/common"
	"github.com/prometheus/common/model"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestBackendBlockSearch(t *testing.T) {

	// Helper functions to make pointers
	strPtr := func(s string) *string { return &s }
	intPtr := func(i int64) *int64 { return &i }

	// Trace
	// This is a fully-populated trace that we search for every condition
	wantTr := &Trace{
		TraceID:           test.ValidTraceID(nil),
		StartTimeUnixNano: uint64(1000 * time.Second),
		EndTimeUnixNano:   uint64(2000 * time.Second),
		DurationNanos:     uint64((100 * time.Millisecond).Nanoseconds()),
		RootServiceName:   "RootService",
		RootSpanName:      "RootSpan",
		ResourceSpans: []ResourceSpans{
			{
				Resource: Resource{
					ServiceName:      "myservice",
					Cluster:          strPtr("cluster"),
					Namespace:        strPtr("namespace"),
					Pod:              strPtr("pod"),
					Container:        strPtr("container"),
					K8sClusterName:   strPtr("k8scluster"),
					K8sNamespaceName: strPtr("k8snamespace"),
					K8sPodName:       strPtr("k8spod"),
					K8sContainerName: strPtr("k8scontainer"),
					Attrs: []Attribute{
						{Key: "bat", Value: strPtr("baz")},
					},
				},
				ScopeSpans: []ScopeSpan{
					{
						Spans: []Span{
							{
								Name:           "hello",
								HttpMethod:     strPtr("get"),
								HttpUrl:        strPtr("url/hello/world"),
								HttpStatusCode: intPtr(500),
								ID:             []byte{},
								ParentSpanID:   []byte{},
								StatusCode:     int(v1.Status_STATUS_CODE_ERROR),
								Attrs: []Attribute{
									{Key: "foo", Value: strPtr("bar")},
								},
							},
						},
					},
				},
			},
		},
	}

	// make a bunch of traces and include our wantTr above
	total := 1000
	insertAt := rand.Intn(total)
	allTraces := make([]*Trace, 0, total)
	for i := 0; i < total; i++ {
		if i == insertAt {
			allTraces = append(allTraces, wantTr)
			continue
		}

		id := test.ValidTraceID(nil)
		pbTrace := test.MakeTrace(10, id)
		pqTrace := traceToParquet(id, pbTrace, nil)
		allTraces = append(allTraces, pqTrace)
	}

	b := makeBackendBlockWithTraces(t, allTraces)
	ctx := context.TODO()

	// Helper function to make a tag search
	makeReq := func(k, v string) *tempopb.SearchRequest {
		return &tempopb.SearchRequest{
			Tags: map[string]string{
				k: v,
			},
		}
	}

	// Matches
	searchesThatMatch := []*tempopb.SearchRequest{
		{
			// Empty request
		},
		{
			MinDurationMs: 99,
			MaxDurationMs: 101,
		},
		{
			Start: 1000,
			End:   2000,
		},
		{
			// Overlaps start
			Start: 999,
			End:   1001,
		},
		{
			// Overlaps end
			Start: 1999,
			End:   2001,
		},

		// Well-known resource attributes
		makeReq(LabelServiceName, "service"),
		makeReq(LabelCluster, "cluster"),
		makeReq(LabelNamespace, "namespace"),
		makeReq(LabelPod, "pod"),
		makeReq(LabelContainer, "container"),
		makeReq(LabelK8sClusterName, "k8scluster"),
		makeReq(LabelK8sNamespaceName, "k8snamespace"),
		makeReq(LabelK8sPodName, "k8spod"),
		makeReq(LabelK8sContainerName, "k8scontainer"),

		// Well-known span attributes
		makeReq(LabelName, "ell"),
		makeReq(LabelHTTPMethod, "get"),
		makeReq(LabelHTTPUrl, "hello"),
		makeReq(LabelHTTPStatusCode, "500"),
		makeReq(LabelStatusCode, StatusCodeError),

		// Span attributes
		makeReq("foo", "bar"),
		// Resource attributes
		makeReq("bat", "baz"),

		// Multiple
		{
			Tags: map[string]string{
				"service.name": "service",
				"http.method":  "get",
				"foo":          "bar",
			},
		},
	}
	expected := &tempopb.TraceSearchMetadata{
		TraceID:           util.TraceIDToHexString(wantTr.TraceID),
		StartTimeUnixNano: wantTr.StartTimeUnixNano,
		DurationMs:        uint32(wantTr.DurationNanos / uint64(time.Millisecond)),
		RootServiceName:   wantTr.RootServiceName,
		RootTraceName:     wantTr.RootSpanName,
	}

	findInResults := func(id string, res []*tempopb.TraceSearchMetadata) *tempopb.TraceSearchMetadata {
		for _, r := range res {
			if r.TraceID == id {
				return r
			}
		}
		return nil
	}

	for _, req := range searchesThatMatch {
		res, err := b.Search(ctx, req, common.DefaultSearchOptions())
		require.NoError(t, err)

		meta := findInResults(expected.TraceID, res.Traces)
		require.NotNil(t, meta, "search request:", req)
		require.Equal(t, expected, meta, "search request:", req)
	}

	// Excludes
	searchesThatDontMatch := []*tempopb.SearchRequest{
		{
			MinDurationMs: 101,
		},
		{
			MaxDurationMs: 99,
		},
		{
			Start: 100,
			End:   200,
		},

		// Well-known resource attributes
		makeReq(LabelServiceName, "foo"),
		makeReq(LabelCluster, "foo"),
		makeReq(LabelNamespace, "foo"),
		makeReq(LabelPod, "foo"),
		makeReq(LabelContainer, "foo"),

		// Well-known span attributes
		makeReq(LabelHTTPMethod, "post"),
		makeReq(LabelHTTPUrl, "asdf"),
		makeReq(LabelHTTPStatusCode, "200"),
		makeReq(LabelStatusCode, StatusCodeOK),

		// Span attributes
		makeReq("foo", "baz"),

		// Multiple
		{
			Tags: map[string]string{
				"http.status_code": "500",
				"service.name":     "asdf",
			},
		},
	}
	for _, req := range searchesThatDontMatch {
		res, err := b.Search(ctx, req, common.DefaultSearchOptions())
		require.NoError(t, err)
		meta := findInResults(expected.TraceID, res.Traces)
		require.Nil(t, meta, req)
	}
}

func makeBackendBlockWithTraces(t *testing.T, trs []*Trace) *backendBlock {
	rawR, rawW, _, err := local.New(&local.Config{
		Path: t.TempDir(),
	})
	require.NoError(t, err)

	r := backend.NewReader(rawR)
	w := backend.NewWriter(rawW)
	ctx := context.Background()

	cfg := &common.BlockConfig{
		BloomFP:             0.01,
		BloomShardSizeBytes: 100 * 1024,
	}

	meta := backend.NewBlockMeta("fake", uuid.New(), VersionString, backend.EncNone, "")
	meta.TotalObjects = 1

	s := newStreamingBlock(ctx, cfg, meta, r, w, tempo_io.NewBufferedWriter)

	for i, tr := range trs {
		err = s.Add(tr, 0, 0)
		require.NoError(t, err)
		if i%100 == 0 {
			_, err := s.Flush()
			require.NoError(t, err)
		}
	}

	_, err = s.Complete()
	require.NoError(t, err)

	b := newBackendBlock(s.meta, r)

	return b
}

func makeTraces() ([]*Trace, map[string]string) {
	traces := []*Trace{}
	attrVals := make(map[string]string)

	ptr := func(s string) *string { return &s }

	attrVals[LabelCluster] = "cluster"
	attrVals[LabelServiceName] = "servicename"
	attrVals[LabelRootServiceName] = "rootsvc"
	attrVals[LabelNamespace] = "ns"
	attrVals[LabelPod] = "pod"
	attrVals[LabelContainer] = "con"
	attrVals[LabelK8sClusterName] = "kclust"
	attrVals[LabelK8sNamespaceName] = "kns"
	attrVals[LabelK8sPodName] = "kpod"
	attrVals[LabelK8sContainerName] = "k8scon"

	attrVals[LabelName] = "span"
	attrVals[LabelRootSpanName] = "rootspan"
	attrVals[LabelHTTPMethod] = "method"
	attrVals[LabelHTTPUrl] = "url"
	attrVals[LabelHTTPStatusCode] = "404"
	attrVals[LabelStatusCode] = "2"

	for i := 0; i < 10; i++ {
		tr := &Trace{
			RootServiceName: "rootsvc",
			RootSpanName:    "rootspan",
		}

		for j := 0; j < 3; j++ {
			key := test.RandomString()
			val := test.RandomString()
			attrVals[key] = val

			rs := ResourceSpans{
				Resource: Resource{
					ServiceName:      "servicename",
					Cluster:          ptr("cluster"),
					Namespace:        ptr("ns"),
					Pod:              ptr("pod"),
					Container:        ptr("con"),
					K8sClusterName:   ptr("kclust"),
					K8sNamespaceName: ptr("kns"),
					K8sPodName:       ptr("kpod"),
					K8sContainerName: ptr("k8scon"),
					Attrs: []Attribute{
						{
							Key:   key,
							Value: &val,
						},
					},
				},
				ScopeSpans: []ScopeSpan{
					{},
				},
			}
			tr.ResourceSpans = append(tr.ResourceSpans, rs)

			for k := 0; k < 10; k++ {
				key := test.RandomString()
				val := test.RandomString()
				attrVals[key] = val

				sts := int64(404)
				span := Span{
					Name:           "span",
					HttpMethod:     ptr("method"),
					HttpUrl:        ptr("url"),
					HttpStatusCode: &sts,
					StatusCode:     2,
					Attrs: []Attribute{
						{
							Key:   key,
							Value: &val,
						},
					},
				}

				rs.ScopeSpans[0].Spans = append(rs.ScopeSpans[0].Spans, span)
			}

		}

		traces = append(traces, tr)
	}

	return traces, attrVals
}

func BenchmarkBackendBlockSearchTraces(b *testing.B) {
	testCases := []struct {
		name string
		tags map[string]string
	}{
		{"noMatch", map[string]string{"foo": "bar"}},
		{"partialMatch", map[string]string{"foo": "bar", "component": "gRPC"}},
		{"service.name", map[string]string{"service.name": "a"}},
	}

	ctx := context.TODO()
	tenantID := "1"
	blockID := uuid.MustParse("3685ee3d-cbbf-4f36-bf28-93447a19dea6")

	r, _, _, err := local.New(&local.Config{
		Path: path.Join("/Users/marty/src/tmp/"),
	})
	require.NoError(b, err)

	rr := backend.NewReader(r)
	meta, err := rr.BlockMeta(ctx, blockID, tenantID)
	require.NoError(b, err)

	block := newBackendBlock(meta, rr)

	opts := common.DefaultSearchOptions()
	opts.StartPage = 10
	opts.TotalPages = 10

	for _, tc := range testCases {

		req := &tempopb.SearchRequest{
			Tags:  tc.tags,
			Limit: 20,
		}

		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			bytesRead := 0
			for i := 0; i < b.N; i++ {
				resp, err := block.Search(ctx, req, opts)
				require.NoError(b, err)
				bytesRead += int(resp.Metrics.InspectedBytes)
			}
			b.SetBytes(int64(bytesRead) / int64(b.N))
			b.ReportMetric(float64(bytesRead)/float64(b.N), "bytes/op")
		})
	}
}

func TestDynamicOutputSchema(t *testing.T) {
	sch := parquet.SchemaOf(new(TimestampRow))
	fmt.Println(sch.String())
}

func TestDynamicMetrics(t *testing.T) {
	ctx := context.TODO()
	tenantID := "1"
	blockID := uuid.MustParse("f04f746c-b463-40be-b7ec-408f7d291b21")

	r, _, _, err := local.New(&local.Config{
		Path: path.Join("/Users/marty/src/tmp/roblox_files"),
	})
	require.NoError(t, err)

	rr := backend.NewReader(r)
	meta, err := rr.BlockMeta(ctx, blockID, tenantID)
	require.NoError(t, err)

	pool := newRowPool(100_000)

	block := newBackendBlock(meta, rr)
	sch := parquet.SchemaOf(new(Trace))

	iter, err := block.RawIterator(ctx, pool)
	require.NoError(t, err)
	defer iter.Close()

	data := map[TimestampBucket]*TimestampMetrics{}

	//count := 1_500_000
	count := 100_000
	//count := 1_000_000

	//timeStampCounts := map[TimestampBucket]*TimestampMetrics{}

	getTimestampBucket := func(s *Span) *TimestampMetrics {
		granularity := uint64(15 * time.Second)
		//granularity := uint64(time.Minute)

		bucket := TimestampBucket(s.EndUnixNanos / granularity * granularity)

		if metrics := data[bucket]; metrics == nil {
			data[bucket] = &TimestampMetrics{
				Bucket:    time.Unix(0, int64(bucket)),
				Resources: map[ResourceHash]*ResourceMetrics{},
			}
		}
		return data[bucket]
	}

	serverSpans := 0
	nonServerSpans := 0
	i := 0
	tr := new(Trace)
	for ; i < count; i++ {

		if i%100_000 == 0 {
			fmt.Println("Processing row", i)
		}

		_, row, err := iter.Next(ctx)
		if err != nil {
			fmt.Println("Iter err:", err)
			break
		}
		if len(row) == 0 {
			break
		}

		err = sch.Reconstruct(tr, row)
		if err != nil {
			fmt.Println("Iter err:", err)
			break
		}
		pool.Put(row)

		for _, rs := range tr.ResourceSpans {

			var resData *ResourceMetrics

			for _, ss := range rs.ScopeSpans {
				for _, s := range ss.Spans {
					if s.Kind != int(v1.Span_SPAN_KIND_SERVER) {
						nonServerSpans++
						continue
					}
					serverSpans++

					ts := getTimestampBucket(&s)
					ts.TotalSpans++

					if resData == nil {
						resAttrs := getAttrs(rs.Resource)
						//hash := ResourceHash(resAttrs)
						hash := ResourceHash(hashAttrs(resAttrs))

						if ts.Resources[hash] == nil {
							ts.Resources[hash] = &ResourceMetrics{
								Attributes: resAttrs,
								Spans:      map[SpanHash]*SpanMetrics{},
							}
						}
						resData = ts.Resources[hash]
					}
					resData.Count++

					name, attrs := getSpanAttrs(s)
					spanHash := hashSpanAttrs(name, attrs)

					if resData.Spans[spanHash] == nil {
						resData.Spans[spanHash] = &SpanMetrics{
							Name:       name,
							Attributes: attrs,
						}
					}
					spanData := resData.Spans[spanHash]
					spanData.Count++
					spanData.Durations = append(spanData.Durations, int(s.EndUnixNanos-s.StartUnixNanos))
				}
			}
		}
	}

	var (
		numSpanSeries     = 0
		numSpans          = 0
		numResources      = 0
		distinctResources = map[ResourceHash]struct{}{}
		distinctSpans     = map[SpanHash]struct{}{}
		dataSlice         = []*TimestampMetrics{}
		spanSeriesSlice   = []*SpanMetrics{}
	)
	for _, ts := range data {
		/*fmt.Println()
		fmt.Println("Count=", v.Count)
		for _, a := range v.Attributes {
			fmt.Println(a.Key, "=", a.String)
		}*/
		numResources += len(ts.Resources)
		numSpans += ts.TotalSpans
		dataSlice = append(dataSlice, ts)

		for resHash, res := range ts.Resources {
			numSpanSeries += len(res.Spans)

			distinctResources[resHash] = struct{}{}

			for spanHash, spans := range res.Spans {
				distinctSpans[spanHash] = struct{}{}

				spanSeriesSlice = append(spanSeriesSlice, spans)
			}
		}
	}
	fmt.Println("Num Traces:", i)
	fmt.Println("Num Time buckets:", len(data))
	fmt.Println("Distinct resources", len(distinctResources))
	fmt.Println("Distinct spans", len(distinctSpans))
	fmt.Println("Total resource series", numResources, "Avg per time bucket:", float32(numResources)/float32(len(data)))
	fmt.Println("Num Span Series:", numSpanSeries, "Avg per resource:", float32(numSpanSeries)/float32(numResources), "Avg per time bucket:", float32(numSpanSeries)/float32(len(data)))
	fmt.Println("Num spans:", numSpans, "Avg per resource:", float32(numSpans)/float32(numResources), "Avg per time bucket:", float32(numSpans)/float32(len(data)))
	fmt.Println("Num Server spans:", serverSpans, "Non-server spans:", nonServerSpans)

	sort.Slice(dataSlice, func(i, j int) bool {
		return dataSlice[j].TotalSpans < dataSlice[i].TotalSpans
	})
	for i := 0; i < 10 && i < len(dataSlice); i++ {
		fmt.Println("Bucket", dataSlice[i].Bucket, "Span count", dataSlice[i].TotalSpans)
	}

	sort.Slice(spanSeriesSlice, func(i, j int) bool {
		return spanSeriesSlice[j].Count < spanSeriesSlice[i].Count
	})
	for i := 0; i < 10 && i < len(spanSeriesSlice); i++ {
		fmt.Println("Top Span Series", spanSeriesSlice[i].Name, spanSeriesSlice[i].Attributes, "Count", spanSeriesSlice[i].Count)
	}

	for i := len(spanSeriesSlice) - 1; i >= 0 && i >= len(spanSeriesSlice)-10; i-- {
		fmt.Println("Bottom Span Series", spanSeriesSlice[i].Name, spanSeriesSlice[i].Attributes, "Count", spanSeriesSlice[i].Count)
	}

	// Write to output
	sort.Slice(dataSlice, func(i, j int) bool {
		return dataSlice[i].Bucket.Before(dataSlice[j].Bucket)
	})

	outf, err := os.Create("output.parquet")
	require.NoError(t, err)
	defer outf.Close()

	outw := parquet.NewGenericWriter[TimestampRow](outf)
	defer outw.Close()

	for i, bucket := range dataSlice {
		tsRow := TimestampRow{
			Bucket:     uint32(bucket.Bucket.Unix()),
			TotalSpans: bucket.TotalSpans,
		}

		for _, res := range bucket.Resources {
			resRow := ResourceRow{
				Attributes: res.Attributes,
				TotalSpans: res.Count,
			}

			for _, span := range res.Spans {
				spanRow := SpanRow{
					Name:       span.Name,
					Attributes: span.Attributes,
					TotalSpans: span.Count,
				}
				sort.Ints(span.Durations)
				for _, dur := range span.Durations {
					spanRow.Durations = append(spanRow.Durations, SpanDuration{Value: dur})
				}

				resRow.Spans = append(resRow.Spans, spanRow)
			}

			//sort.Slice(resRow.Spans, func(i, j int) bool {
			//	return resRow.Spans[i].Attributes < resRow.Spans[j].Attributes
			//})
			tsRow.Resources = append(tsRow.Resources, resRow)
		}

		//sort.Slice(tsRow.Resources, func(i, j int) bool {
		//	return tsRow.Resources[i].Attributes < tsRow.Resources[j].Attributes
		//})

		outw.Write([]TimestampRow{tsRow})
		outw.Flush()
		fmt.Println("Flushed rowgroup", i)
	}
}

/*type AttributeValue struct {
	Key    string  `parquet:",dict"`
	String *string `parquet:",dict"`
	Int    *int64  `parquet:",delta"`
	Float  *float64
}*/

type SpanMetrics struct {
	Name       string
	Attributes []Attribute

	Count     int
	Durations []int
}

//type ResourceSeries map[string]AttributeValue

type ResourceMetrics struct {
	Attributes []Attribute
	Count      int

	Spans map[SpanHash]*SpanMetrics
}

type TimestampMetrics struct {
	Bucket     time.Time
	TotalSpans int
	Resources  map[ResourceHash]*ResourceMetrics
}

type TimestampBucket uint64
type ResourceHash uint64
type SpanHash uint64

func getAttrs(r Resource) (attrs []Attribute) {

	quickAdd := func(name string, value *string) {
		if value != nil {
			attrs = append(attrs, Attribute{Key: name, Value: value})
		}
	}
	quickAddInt := func(name string, value *int64) {
		if value != nil {
			attrs = append(attrs, Attribute{Key: name, ValueInt: value})
		}
	}
	quickAdd("service.name", &r.ServiceName)
	for _, a := range r.Attrs {
		quickAdd(a.Key, a.Value)
		quickAddInt(a.Key, a.ValueInt)
	}
	sort.Slice(attrs, func(i, j int) bool {
		return attrs[i].Key < attrs[j].Key
	})
	return attrs
}

func getResAttrs(r Resource) (serviceName string, attrs []Attribute) {
	r.ServiceName = serviceName

	quickAdd := func(name string, value *string) {
		if value != nil {
			attrs = append(attrs, Attribute{Key: name, Value: value})
		}
	}
	quickAddInt := func(name string, value *int64) {
		if value != nil {
			attrs = append(attrs, Attribute{Key: name, ValueInt: value})
		}
	}

	for _, a := range r.Attrs {
		quickAdd(a.Key, a.Value)
		quickAddInt(a.Key, a.ValueInt)
	}

	sort.Slice(attrs, func(i, j int) bool {
		return attrs[i].Key < attrs[j].Key
	})

	return
}

func hashAttrs(attrs []Attribute) uint64 {
	buf := make([]byte, 8)
	h := newHash()
	for _, a := range attrs {
		h.Write([]byte(a.Key))
		if a.Value != nil {
			h.Write([]byte(*a.Value))
		}
		if a.ValueInt != nil {
			binary.BigEndian.PutUint64(buf, uint64(*a.ValueInt))
			h.Write([]byte(buf))
		}
	}

	return h.Sum64()
}

func getSpanAttrs(s Span) (name string, attrs []Attribute) {
	name = s.Name
	quickAdd := func(name string, value *string) {
		if value != nil {
			attrs = append(attrs, Attribute{Key: name, Value: value})
		}
	}
	quickAddInt := func(name string, value *int64) {
		if value != nil {
			attrs = append(attrs, Attribute{Key: name, ValueInt: value})
		}
	}
	quickAdd("http.url", s.HttpUrl)
	quickAdd("http.method", s.HttpMethod)
	quickAddInt("http.status_code", s.HttpStatusCode)
	for _, a := range s.Attrs {
		quickAdd(a.Key, a.Value)
		quickAddInt(a.Key, a.ValueInt)
	}
	sort.Slice(attrs, func(i, j int) bool {
		return attrs[i].Key < attrs[j].Key
	})
	return
}

func hashSpanAttrs(name string, attrs []Attribute) SpanHash {
	buf := make([]byte, 8)

	h := newHash()
	h.Write([]byte(name))
	for _, a := range attrs {
		h.Write([]byte(a.Key))
		if a.Value != nil {
			h.Write([]byte(*a.Value))
		}
		if a.ValueInt != nil {
			binary.BigEndian.PutUint64(buf, uint64(*a.ValueInt))
			h.Write(buf)
		}
	}

	return SpanHash(h.Sum64())
}

type SpanDuration struct {
	Value int `parquet:",delta"`
}

type SpanRow struct {
	Name       string         `parquet:",dict"`
	Attributes []Attribute    `parquet:""`
	TotalSpans int            `parquet:",delta"`
	Durations  []SpanDuration `parquet:""`
}

type ResourceRow struct {
	Attributes []Attribute `parquet:""`
	TotalSpans int         `parquet:",delta"`
	Spans      []SpanRow   `parquet:""`
}

type TimestampRow struct {
	Bucket     uint32        `parquet:",delta"`
	TotalSpans int           `parquet:",delta"`
	Resources  []ResourceRow `parquet:""`
}

type ResourceMetrics2 struct {
	sortingLabels string
	ServiceName   string
	Attributes    []Attribute
	Count         int

	Spans map[SpanHash]*SpanMetrics
}

func toSortingLabels(name string, attrs []Attribute) string {
	ls := model.LabelSet{}
	ls[model.MetricNameLabel] = model.LabelValue(name)

	for _, a := range attrs {
		if a.Value != nil {
			ls[model.LabelName(a.Key)] = model.LabelValue(*a.Value)
			continue
		}
		if a.ValueInt != nil {
			ls[model.LabelName(a.Key)] = model.LabelValue(strconv.Itoa(int(*a.ValueInt)))
			continue
		}
	}

	return ls.String()
}

func TestDynamicMetricsBucketByResource(t *testing.T) {
	ctx := context.TODO()
	tenantID := "1"
	blockID := uuid.MustParse("f04f746c-b463-40be-b7ec-408f7d291b21")

	r, _, _, err := local.New(&local.Config{
		Path: path.Join("/Users/marty/src/tmp/roblox_files"),
	})
	require.NoError(t, err)

	rr := backend.NewReader(r)
	meta, err := rr.BlockMeta(ctx, blockID, tenantID)
	require.NoError(t, err)

	pool := newRowPool(100_000)

	block := newBackendBlock(meta, rr)
	sch := parquet.SchemaOf(new(Trace))

	iter, err := block.RawIterator(ctx, pool)
	require.NoError(t, err)
	defer iter.Close()

	data := map[ResourceHash]*ResourceMetrics2{}

	//count := 1_500_000
	count := 100_000
	//count := 1_000_000

	serverSpans := 0
	nonServerSpans := 0
	i := 0
	tr := new(Trace)
	for ; i < count; i++ {

		if i%100_000 == 0 {
			fmt.Println("Processing row", i)
		}

		_, row, err := iter.Next(ctx)
		if err != nil {
			fmt.Println("Iter err:", err)
			break
		}
		if len(row) == 0 {
			break
		}

		err = sch.Reconstruct(tr, row)
		if err != nil {
			fmt.Println("Iter err:", err)
			break
		}
		pool.Put(row)

		for _, rs := range tr.ResourceSpans {

			var resData *ResourceMetrics2

			for _, ss := range rs.ScopeSpans {
				for _, s := range ss.Spans {
					if s.Kind != int(v1.Span_SPAN_KIND_SERVER) {
						nonServerSpans++
						continue
					}
					serverSpans++

					if resData == nil {
						name, resAttrs := getResAttrs(rs.Resource)
						//hash := ResourceHash(resAttrs)
						hash := ResourceHash(hashSpanAttrs(name, resAttrs))

						resData = &ResourceMetrics2{
							sortingLabels: toSortingLabels(name, resAttrs),
							ServiceName:   name,
							Attributes:    resAttrs,
							Spans:         map[SpanHash]*SpanMetrics{},
						}
						data[hash] = resData
					}
					resData.Count++

					name, attrs := getSpanAttrs(s)
					spanHash := hashSpanAttrs(name, attrs)

					if resData.Spans[spanHash] == nil {
						resData.Spans[spanHash] = &SpanMetrics{
							Name:       name,
							Attributes: attrs,
						}
					}
					spanData := resData.Spans[spanHash]
					spanData.Count++
					spanData.Durations = append(spanData.Durations, int(s.EndUnixNanos-s.StartUnixNanos))
				}
			}
		}
	}

	var (
		numSpanSeries = 0
		numSpans      = 0
		numResources  = 0
		distinctSpans = map[SpanHash]struct{}{}
		dataSlice     = []*ResourceMetrics2{}
	)

	numResources = len(data)
	for _, res := range data {
		numSpanSeries += len(res.Spans)
		dataSlice = append(dataSlice, res)
		for spanHash := range res.Spans {
			distinctSpans[spanHash] = struct{}{}
		}
	}

	fmt.Println("Num Traces:", i)
	fmt.Println("Distinct resources", len(data))
	fmt.Println("Distinct spans", len(distinctSpans))
	fmt.Println("Num Span Series:", numSpanSeries, "Avg per resource:", float32(numSpanSeries)/float32(numResources), "Avg per time bucket:", float32(numSpanSeries)/float32(len(data)))
	fmt.Println("Num spans:", numSpans, "Avg per resource:", float32(numSpans)/float32(numResources), "Avg per time bucket:", float32(numSpans)/float32(len(data)))
	fmt.Println("Num Server spans:", serverSpans, "Non-server spans:", nonServerSpans)

	sort.Slice(dataSlice, func(i, j int) bool {
		return strings.Compare(dataSlice[i].sortingLabels, dataSlice[j].sortingLabels) == -1
	})

	// Write to output

	outf, err := os.Create("output.parquet")
	require.NoError(t, err)
	defer outf.Close()

	outw := parquet.NewGenericWriter[ResourceRow](outf)
	defer outw.Close()

	rows := []ResourceRow{}
	for _, res := range dataSlice {
		resRow := ResourceRow{
			Attributes: res.Attributes,
			TotalSpans: res.Count,
		}

		for _, span := range res.Spans {
			spanRow := SpanRow{
				Name:       span.Name,
				Attributes: span.Attributes,
				TotalSpans: span.Count,
			}
			sort.Ints(span.Durations)
			for _, dur := range span.Durations {
				spanRow.Durations = append(spanRow.Durations, SpanDuration{Value: dur})
			}

			resRow.Spans = append(resRow.Spans, spanRow)
		}
		rows = append(rows, resRow)
	}

	outw.Write(rows)

}

func TestDynamicMetricsOnExistingBlock(t *testing.T) {
	ctx := context.TODO()
	tenantID := "1"
	blockID := uuid.MustParse("735a37ac-75c7-4bfc-96b2-f525c904b2b9")

	r, _, _, err := local.New(&local.Config{
		Path: path.Join("/Users/marty/src/tmp"),
	})
	require.NoError(t, err)

	rr := backend.NewReader(r)
	meta, err := rr.BlockMeta(ctx, blockID, tenantID)
	require.NoError(t, err)

	block := newBackendBlock(meta, rr)

	//q := `{resource.service.name="tempo-gateway"}`
	q := `{}`
	groupBy := traceql.MustParseIdentifier("resource.cluster")
	series := map[traceql.Static]*metrics{}
	//spanLimit := 10_000
	spanLimit := 100_000
	spanCount := 0

	fmt.Println("Query:", q)
	fmt.Println("GroupBy:", groupBy)

	req := traceql.MustExtractFetchSpansRequest(q)
	req.Conditions = append(req.Conditions, traceql.Condition{Attribute: traceql.NewIntrinsic(traceql.IntrinsicDuration)})
	req.Conditions = append(req.Conditions, traceql.Condition{Attribute: groupBy})

	req.Filter = func(ss *traceql.Spanset) ([]*traceql.Spanset, error) {
		for _, s := range ss.Spans {
			groupByValue := s.Attributes()[groupBy]
			dur := s.EndtimeUnixNanos() - s.StartTimeUnixNanos()

			m := series[groupByValue]
			if m == nil {
				m = &metrics{}
				series[groupByValue] = m
			}
			m.PutDurationNanos(dur)
			spanCount++
		}

		if spanCount >= spanLimit {
			return nil, io.EOF
		}

		return nil, nil
	}

	opts := common.SearchOptions{}

	res, err := block.Fetch(ctx, req, opts)
	require.NoError(t, err)

	for {
		ss, err := res.Results.Next(ctx)
		require.NoError(t, err)
		if ss == nil {
			break
		}
	}
	fmt.Println("Results:")
	ks := []traceql.Static{}
	for k := range series {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		return strings.Compare(ks[i].String(), ks[j].String()) == -1
	})
	for _, k := range ks {
		m := series[k]
		fmt.Printf("Series: %v\t\tCount:%d\t\tAvg:%v\tp95: %v\tp95Est: %v\n", k,
			m.Count(),
			m.AvgDuration(),
			m.Percentile(0.95),
			m.PercentileEst(0.95),
		)
	}

	fmt.Println("Spans matched:", spanCount)
	fmt.Println("Bytes I/O:", humanize.Bytes(res.Bytes()))
	fmt.Println("Pool Gets", getCount, "Puts", putCount)
}

/*type metrics struct {
	count uint64
	dur   uint64
}

func (m *metrics) PutDurationNanos(d uint64) {
	m.count++
	m.dur += d
}

func (m *metrics) Count() uint64 {
	return m.count
}

func (m *metrics) AvgDuration() time.Duration {
	return time.Duration(float64(m.dur) / float64(m.count))
}*/

type metrics struct {
	durs []int
}

func (m *metrics) PutDurationNanos(d uint64) {
	m.durs = append(m.durs, int(d))
}

func (m *metrics) Count() int {
	return len(m.durs)
}

func (m *metrics) AvgDuration() time.Duration {
	count := 0.0
	total := 0.0

	for _, dur := range m.durs {
		count++
		total += float64(dur)
	}

	return time.Duration(total / count)
}

func (m *metrics) Percentile(p float32) time.Duration {
	sort.Ints(m.durs)

	i := int(float32(len(m.durs)) * p)

	return time.Duration(m.durs[i])
}

func (m *metrics) PercentileEst(p float32) time.Duration {

	// Exponential buckets, powers of 2
	buckets := [64]int{}

	for _, dur := range m.durs {
		var bucket int
		if dur >= 2 {
			bucket = int(math.Ceil(math.Log2(float64(dur))))
		}
		buckets[bucket]++
	}

	// Maximum amount of samples to include. We round up to better handle low
	// percentiles on low sample counts (<100).
	maxSamples := int(math.Ceil(float64(p) * float64(len(m.durs))))

	// Find the bucket where the percentile falls in
	// and the total sample count less than or equal
	// to that bucket.
	var total, bucket int
	for b, count := range buckets {
		if total+count < maxSamples {
			bucket = b
			total += count
			continue
		}

		// We have enough
		break
	}

	// Fraction to interpolate between buckets, sample-count wise.
	// 0.5 means halfway
	interp := float64(maxSamples-total) / float64(buckets[bucket+1])

	// Exponential interpolation between buckets
	minDur := math.Pow(2, float64(bucket))
	dur := minDur * math.Pow(2, float64(interp))

	return time.Duration(dur)
}
