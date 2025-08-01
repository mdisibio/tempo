package frontend

import (
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level" //nolint:all //deprecated
	"go.opentelemetry.io/otel"

	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/tempo/modules/frontend/combiner"
	"github.com/grafana/tempo/modules/frontend/pipeline"
	"github.com/grafana/tempo/modules/overrides"
	"github.com/grafana/tempo/pkg/api"
	"github.com/grafana/tempo/pkg/cache"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/tempodb"
	"github.com/grafana/tempo/tempodb/backend"
)

type RoundTripperFunc func(*http.Request) (*http.Response, error)

// RoundTrip implememnts http.RoundTripper
func (fn RoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

// these handler funcs could likely be removed and the code written directly into the respective
// gRPC functions
type (
	streamingSearchHandler       func(req *tempopb.SearchRequest, srv tempopb.StreamingQuerier_SearchServer) error
	streamingTagsHandler         func(req *tempopb.SearchTagsRequest, srv tempopb.StreamingQuerier_SearchTagsServer) error
	streamingTagsV2Handler       func(req *tempopb.SearchTagsRequest, srv tempopb.StreamingQuerier_SearchTagsV2Server) error
	streamingTagValuesHandler    func(req *tempopb.SearchTagValuesRequest, srv tempopb.StreamingQuerier_SearchTagValuesServer) error
	streamingTagValuesV2Handler  func(req *tempopb.SearchTagValuesRequest, srv tempopb.StreamingQuerier_SearchTagValuesV2Server) error
	streamingQueryRangeHandler   func(req *tempopb.QueryRangeRequest, srv tempopb.StreamingQuerier_MetricsQueryRangeServer) error
	streamingQueryInstantHandler func(req *tempopb.QueryInstantRequest, srv tempopb.StreamingQuerier_MetricsQueryInstantServer) error
)

type QueryFrontend struct {
	TraceByIDHandler, TraceByIDHandlerV2, SearchHandler, MetricsSummaryHandler                 http.Handler
	SearchTagsHandler, SearchTagsV2Handler, SearchTagsValuesHandler, SearchTagsValuesV2Handler http.Handler
	MetricsQueryInstantHandler, MetricsQueryRangeHandler                                       http.Handler
	MCPHandler                                                                                 http.Handler
	cacheProvider                                                                              cache.Provider
	streamingSearch                                                                            streamingSearchHandler
	streamingTags                                                                              streamingTagsHandler
	streamingTagsV2                                                                            streamingTagsV2Handler
	streamingTagValues                                                                         streamingTagValuesHandler
	streamingTagValuesV2                                                                       streamingTagValuesV2Handler
	streamingQueryRange                                                                        streamingQueryRangeHandler
	streamingQueryInstant                                                                      streamingQueryInstantHandler
	logger                                                                                     log.Logger
}

var tracer = otel.Tracer("modules/frontend")

// New returns a new QueryFrontend
func New(cfg Config, next pipeline.RoundTripper, o overrides.Interface, reader tempodb.Reader, cacheProvider cache.Provider, apiPrefix string, authMiddleware middleware.Interface, logger log.Logger, registerer prometheus.Registerer) (*QueryFrontend, error) {
	level.Info(logger).Log("msg", "creating middleware in query frontend")

	if cfg.TraceByID.QueryShards < minQueryShards || cfg.TraceByID.QueryShards > maxQueryShards {
		return nil, fmt.Errorf("frontend query shards should be between %d and %d (both inclusive)", minQueryShards, maxQueryShards)
	}

	if cfg.Search.Sharder.ConcurrentRequests <= 0 {
		return nil, fmt.Errorf("frontend search concurrent requests should be greater than 0")
	}

	if cfg.Search.Sharder.TargetBytesPerRequest <= 0 {
		return nil, fmt.Errorf("frontend search target bytes per request should be greater than 0")
	}

	if cfg.Search.Sharder.QueryIngestersUntil < cfg.Search.Sharder.QueryBackendAfter {
		return nil, fmt.Errorf("query backend after should be less than or equal to query ingester until")
	}

	if cfg.Search.Sharder.MostRecentShards <= 0 {
		return nil, fmt.Errorf("most recent shards must be greater than 0")
	}

	if cfg.Metrics.Sharder.ConcurrentRequests <= 0 {
		return nil, fmt.Errorf("frontend metrics concurrent requests should be greater than 0")
	}

	if cfg.Metrics.Sharder.TargetBytesPerRequest <= 0 {
		return nil, fmt.Errorf("frontend metrics target bytes per request should be greater than 0")
	}

	if cfg.Metrics.Sharder.Interval <= 0 {
		return nil, fmt.Errorf("frontend metrics interval should be greater than 0")
	}

	// Propagate RF1After to search and traceByID sharders
	cfg.Search.Sharder.RF1After = cfg.RF1After
	cfg.TraceByID.RF1After = cfg.RF1After

	retryWare := pipeline.NewRetryWare(cfg.MaxRetries, cfg.Weights.RetryWithWeights, registerer)
	cacheWare := pipeline.NewCachingWare(cacheProvider, cache.RoleFrontendSearch, logger)
	statusCodeWare := pipeline.NewStatusCodeAdjustWare()
	traceIDStatusCodeWare := pipeline.NewStatusCodeAdjustWareWithAllowedCode(http.StatusNotFound)
	urlDenyListWare := pipeline.NewURLDenyListWare(cfg.URLDenyList)
	queryValidatorWare := pipeline.NewQueryValidatorWare(cfg.MaxQueryExpressionSizeBytes)
	headerStripWare := pipeline.NewStripHeadersWare(cfg.AllowedHeaders)

	tracePipeline := pipeline.Build(
		[]pipeline.AsyncMiddleware[combiner.PipelineResponse]{
			headerStripWare,
			urlDenyListWare,
			pipeline.NewWeightRequestWare(pipeline.TraceByID, cfg.Weights),
			multiTenantMiddleware(cfg, logger),
			newAsyncTraceIDSharder(&cfg.TraceByID, logger),
		},
		[]pipeline.Middleware{traceIDStatusCodeWare, retryWare},
		next)

	searchPipeline := pipeline.Build(
		[]pipeline.AsyncMiddleware[combiner.PipelineResponse]{
			headerStripWare,
			urlDenyListWare,
			queryValidatorWare,
			pipeline.NewWeightRequestWare(pipeline.TraceQLSearch, cfg.Weights),
			multiTenantMiddleware(cfg, logger),
			newAsyncSearchSharder(reader, o, cfg.Search.Sharder, logger),
		},
		[]pipeline.Middleware{cacheWare, statusCodeWare, retryWare},
		next)

	searchTagsPipeline := pipeline.Build(
		[]pipeline.AsyncMiddleware[combiner.PipelineResponse]{
			headerStripWare,
			urlDenyListWare,
			pipeline.NewWeightRequestWare(pipeline.Default, cfg.Weights),
			multiTenantMiddleware(cfg, logger),
			newAsyncTagSharder(reader, o, cfg.Search.Sharder, parseTagsRequest, logger),
		},
		[]pipeline.Middleware{cacheWare, statusCodeWare, retryWare},
		next)

	searchTagValuesPipeline := pipeline.Build(
		[]pipeline.AsyncMiddleware[combiner.PipelineResponse]{
			headerStripWare,
			urlDenyListWare,
			pipeline.NewWeightRequestWare(pipeline.Default, cfg.Weights),
			multiTenantMiddleware(cfg, logger),
			newAsyncTagSharder(reader, o, cfg.Search.Sharder, parseTagValuesRequest, logger),
		},
		[]pipeline.Middleware{cacheWare, statusCodeWare, retryWare},
		next)

	// metrics summary
	metricsPipeline := pipeline.Build(
		[]pipeline.AsyncMiddleware[combiner.PipelineResponse]{
			urlDenyListWare,
			queryValidatorWare,
			pipeline.NewWeightRequestWare(pipeline.Default, cfg.Weights),
			multiTenantUnsupportedMiddleware(cfg, logger),
		},
		[]pipeline.Middleware{statusCodeWare, retryWare},
		next)

	// traceql metrics
	queryRangePipeline := pipeline.Build(
		[]pipeline.AsyncMiddleware[combiner.PipelineResponse]{
			headerStripWare,
			urlDenyListWare,
			queryValidatorWare,
			pipeline.NewWeightRequestWare(pipeline.TraceQLMetrics, cfg.Weights),
			multiTenantMiddleware(cfg, logger),
			newAsyncQueryRangeSharder(reader, o, cfg.Metrics.Sharder, false, logger),
		},
		[]pipeline.Middleware{cacheWare, statusCodeWare, retryWare},
		next)

	queryInstantPipeline := pipeline.Build(
		[]pipeline.AsyncMiddleware[combiner.PipelineResponse]{
			headerStripWare,
			urlDenyListWare,
			queryValidatorWare,
			pipeline.NewWeightRequestWare(pipeline.TraceQLMetrics, cfg.Weights),
			multiTenantMiddleware(cfg, logger),
			newAsyncQueryRangeSharder(reader, o, cfg.Metrics.Sharder, true, logger),
		},
		[]pipeline.Middleware{cacheWare, statusCodeWare, retryWare},
		next)

	traces := newTraceIDHandler(cfg, tracePipeline, o, combiner.NewTypedTraceByID, logger)
	tracesV2 := newTraceIDV2Handler(cfg, tracePipeline, o, combiner.NewTypedTraceByIDV2, logger)
	search := newSearchHTTPHandler(cfg, searchPipeline, logger)
	searchTags := newTagsHTTPHandler(cfg, searchTagsPipeline, o, logger)
	searchTagsV2 := newTagsV2HTTPHandler(cfg, searchTagsPipeline, o, logger)
	searchTagValues := newTagValuesHTTPHandler(cfg, searchTagValuesPipeline, o, logger)
	searchTagValuesV2 := newTagValuesV2HTTPHandler(cfg, searchTagValuesPipeline, o, logger)
	metrics := newMetricsSummaryHandler(metricsPipeline, logger)
	queryInstant := newMetricsQueryInstantHTTPHandler(cfg, queryInstantPipeline, logger) // Reuses the same pipeline
	queryRange := newMetricsQueryRangeHTTPHandler(cfg, queryRangePipeline, logger)

	f := &QueryFrontend{
		// http/discrete
		TraceByIDHandler:           newHandler(cfg.Config.LogQueryRequestHeaders, traces, logger),
		TraceByIDHandlerV2:         newHandler(cfg.Config.LogQueryRequestHeaders, tracesV2, logger),
		SearchHandler:              newHandler(cfg.Config.LogQueryRequestHeaders, search, logger),
		SearchTagsHandler:          newHandler(cfg.Config.LogQueryRequestHeaders, searchTags, logger),
		SearchTagsV2Handler:        newHandler(cfg.Config.LogQueryRequestHeaders, searchTagsV2, logger),
		SearchTagsValuesHandler:    newHandler(cfg.Config.LogQueryRequestHeaders, searchTagValues, logger),
		SearchTagsValuesV2Handler:  newHandler(cfg.Config.LogQueryRequestHeaders, searchTagValuesV2, logger),
		MetricsSummaryHandler:      newHandler(cfg.Config.LogQueryRequestHeaders, metrics, logger),
		MetricsQueryInstantHandler: newHandler(cfg.Config.LogQueryRequestHeaders, queryInstant, logger),
		MetricsQueryRangeHandler:   newHandler(cfg.Config.LogQueryRequestHeaders, queryRange, logger),

		// grpc/streaming
		streamingSearch:       newSearchStreamingGRPCHandler(cfg, searchPipeline, apiPrefix, logger),
		streamingTags:         newTagsStreamingGRPCHandler(cfg, searchTagsPipeline, apiPrefix, o, logger),
		streamingTagsV2:       newTagsV2StreamingGRPCHandler(cfg, searchTagsPipeline, apiPrefix, o, logger),
		streamingTagValues:    newTagValuesStreamingGRPCHandler(cfg, searchTagValuesPipeline, apiPrefix, o, logger),
		streamingTagValuesV2:  newTagValuesV2StreamingGRPCHandler(cfg, searchTagValuesPipeline, apiPrefix, o, logger),
		streamingQueryRange:   newQueryRangeStreamingGRPCHandler(cfg, queryRangePipeline, apiPrefix, logger),
		streamingQueryInstant: newQueryInstantStreamingGRPCHandler(cfg, queryRangePipeline, apiPrefix, logger), // Reuses the same pipeline

		cacheProvider: cacheProvider,
		logger:        logger,
	}

	if cfg.MCPServer.Enabled {
		// Initialize MCP server
		mcpServer := NewMCPServer(f, apiPrefix, logger, authMiddleware)
		f.MCPHandler = mcpServer
	} else {
		f.MCPHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.NotFound(w, r)
		})
	}

	return f, nil
}

// Search implements StreamingQuerierServer interface for streaming search
func (q *QueryFrontend) Search(req *tempopb.SearchRequest, srv tempopb.StreamingQuerier_SearchServer) error {
	return q.streamingSearch(req, srv)
}

func (q *QueryFrontend) SearchTags(req *tempopb.SearchTagsRequest, srv tempopb.StreamingQuerier_SearchTagsServer) error {
	return q.streamingTags(req, srv)
}

func (q *QueryFrontend) SearchTagsV2(req *tempopb.SearchTagsRequest, srv tempopb.StreamingQuerier_SearchTagsV2Server) error {
	return q.streamingTagsV2(req, srv)
}

func (q *QueryFrontend) SearchTagValues(req *tempopb.SearchTagValuesRequest, srv tempopb.StreamingQuerier_SearchTagValuesServer) error {
	return q.streamingTagValues(req, srv)
}

func (q *QueryFrontend) SearchTagValuesV2(req *tempopb.SearchTagValuesRequest, srv tempopb.StreamingQuerier_SearchTagValuesV2Server) error {
	return q.streamingTagValuesV2(req, srv)
}

func (q *QueryFrontend) MetricsQueryRange(req *tempopb.QueryRangeRequest, srv tempopb.StreamingQuerier_MetricsQueryRangeServer) error {
	return q.streamingQueryRange(req, srv)
}

func (q *QueryFrontend) MetricsQueryInstant(req *tempopb.QueryInstantRequest, srv tempopb.StreamingQuerier_MetricsQueryInstantServer) error {
	return q.streamingQueryInstant(req, srv)
}

// newSpanMetricsMiddleware creates a new frontend middleware to handle metrics-generator requests.
func newMetricsSummaryHandler(next pipeline.AsyncRoundTripper[combiner.PipelineResponse], logger log.Logger) http.RoundTripper {
	return RoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		tenant, err := user.ExtractOrgID(req.Context())
		if err != nil {
			level.Error(logger).Log("msg", "metrics summary: failed to extract tenant id", "err", err)
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Status:     http.StatusText(http.StatusBadRequest),
				Body:       io.NopCloser(strings.NewReader(err.Error())),
			}, nil
		}
		prepareRequestForQueriers(req, tenant)
		// This API is always json because it only ever has 1 job and this
		// lets us return the response as-is.
		req.Header.Set(api.HeaderAccept, api.HeaderAcceptJSON)

		level.Info(logger).Log(
			"msg", "metrics summary request",
			"tenant", tenant,
			"path", req.URL.Path)

		resps, err := next.RoundTrip(pipeline.NewHTTPRequest(req))
		if err != nil {
			return nil, err
		}

		resp, _, err := resps.Next(req.Context()) // metrics path will only ever have one response

		level.Info(logger).Log(
			"msg", "metrics summary response",
			"tenant", tenant,
			"path", req.URL.Path,
			"err", err)

		return resp.HTTPResponse(), err
	})
}

// cloneRequestforQueriers returns a cloned pipeline.Request from the passed pipeline.Request ready for queriers. The caller is given an opportunity
// to modify the internal http.Request before it is returned using the modHTTP param. If modHTTP is nil, the internal http.Request is returned.
func cloneRequestforQueriers(parent pipeline.Request, tenant string, modHTTP func(*http.Request) (*http.Request, error)) (pipeline.Request, error) {
	req := parent.HTTPRequest()
	clonedHTTPReq := req.Clone(req.Context())

	// give the caller a chance to modify the internal http request
	if modHTTP != nil {
		var err error
		clonedHTTPReq, err = modHTTP(clonedHTTPReq)
		if err != nil {
			return nil, err
		}
	}

	prepareRequestForQueriers(clonedHTTPReq, tenant)

	return parent.CloneFromHTTPRequest(clonedHTTPReq), nil
}

// prepareRequestForQueriers modifies the request so they will be farmed correctly to the queriers
//   - adds the tenant header
//   - sets the requesturi (see below for details)
func prepareRequestForQueriers(req *http.Request, tenant string) {
	// set the tenant header
	req.Header.Set(user.OrgIDHeaderName, tenant)

	// All communication with the queriers should be proto for efficiency
	// NOTE - This isn't strict and queriers may still return json if we missed
	// an endpoint. But cache and response unmarshalling still work.
	req.Header.Set(api.HeaderAccept, api.HeaderAcceptProtobuf)

	// copy the url (which is correct) to the RequestURI
	// we do this because dskit/common uses the RequestURI field to translate from http.Request to httpgrpc.Request
	// https://github.com/grafana/dskit/blob/f5bd38371e1cfae5479b2c23b3893c1a97868bdf/httpgrpc/httpgrpc.go#L53
	const queryDelimiter = "?"

	uri := path.Join(api.PathPrefixQuerier, req.URL.Path)
	if len(req.URL.RawQuery) > 0 {
		uri += queryDelimiter + req.URL.RawQuery
	}
	req.RequestURI = uri
}

func multiTenantMiddleware(cfg Config, logger log.Logger) pipeline.AsyncMiddleware[combiner.PipelineResponse] {
	if cfg.MultiTenantQueriesEnabled {
		return pipeline.NewMultiTenantMiddleware(logger)
	}

	return pipeline.NewNoopMiddleware()
}

func multiTenantUnsupportedMiddleware(cfg Config, logger log.Logger) pipeline.AsyncMiddleware[combiner.PipelineResponse] {
	if cfg.MultiTenantQueriesEnabled {
		return pipeline.NewMultiTenantUnsupportedMiddleware(logger)
	}

	return pipeline.NewNoopMiddleware()
}

// blockMetasForSearch returns a list of blocks that are relevant to the search query.
// start and end are unix timestamps in seconds. rf is the replication factor of the blocks to return.
func blockMetasForSearch(allBlocks []*backend.BlockMeta, start, end time.Time, filterFn func(m *backend.BlockMeta) bool) []*backend.BlockMeta {
	blocks := make([]*backend.BlockMeta, 0, len(allBlocks)/50) // divide by 50 for luck
	for _, m := range allBlocks {
		// Block overlaps with search range if:
		// block start is before or equal to search end AND block end is after or equal to search start
		if !m.StartTime.After(end) && // block start <= search end
			!m.EndTime.Before(start) && // block end >= search start
			filterFn(m) { // This check skips generator blocks (RF=1)
			blocks = append(blocks, m)
		}
	}

	// search backwards in time with deterministic ordering
	sort.Slice(blocks, func(i, j int) bool {
		if !blocks[i].EndTime.Equal(blocks[j].EndTime) {
			return blocks[i].EndTime.After(blocks[j].EndTime)
		}
		return blocks[i].BlockID.String() < blocks[j].BlockID.String()
	})

	return blocks
}
