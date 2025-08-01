package app

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/dns"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/memberlist"
	"github.com/grafana/dskit/middleware"
	"github.com/grafana/dskit/modules"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/server"
	"github.com/grafana/dskit/services"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/grafana/tempo/modules/backendscheduler"
	"github.com/grafana/tempo/modules/backendworker"
	"github.com/grafana/tempo/modules/blockbuilder"
	"github.com/grafana/tempo/modules/cache"
	"github.com/grafana/tempo/modules/compactor"
	"github.com/grafana/tempo/modules/distributor"
	"github.com/grafana/tempo/modules/frontend"
	"github.com/grafana/tempo/modules/frontend/interceptor"
	frontend_v1pb "github.com/grafana/tempo/modules/frontend/v1/frontendv1pb"
	"github.com/grafana/tempo/modules/generator"
	"github.com/grafana/tempo/modules/ingester"
	"github.com/grafana/tempo/modules/overrides"
	userconfigurableoverridesapi "github.com/grafana/tempo/modules/overrides/userconfigurable/api"
	"github.com/grafana/tempo/modules/querier"
	tempo_storage "github.com/grafana/tempo/modules/storage"
	"github.com/grafana/tempo/pkg/api"
	tempo_ring "github.com/grafana/tempo/pkg/ring"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/usagestats"
	"github.com/grafana/tempo/pkg/util/log"
	util_log "github.com/grafana/tempo/pkg/util/log"
	"github.com/grafana/tempo/tempodb/backend"
	"github.com/grafana/tempo/tempodb/backend/azure"
	"github.com/grafana/tempo/tempodb/backend/gcs"
	"github.com/grafana/tempo/tempodb/backend/local"
	"github.com/grafana/tempo/tempodb/backend/s3"
)

// The various modules that make up tempo.
const (
	// utilities
	Server         string = "server"
	InternalServer string = "internal-server"
	Store          string = "store"
	OptionalStore  string = "optional-store"
	MemberlistKV   string = "memberlist-kv"
	UsageReport    string = "usage-report"
	Overrides      string = "overrides"
	OverridesAPI   string = "overrides-api"
	CacheProvider  string = "cache-provider"

	// rings
	IngesterRing          string = "ring"
	SecondaryIngesterRing string = "secondary-ring"
	MetricsGeneratorRing  string = "metrics-generator-ring"
	PartitionRing         string = "partition-ring"
	GeneratorRingWatcher  string = "generator-ring-watcher"

	// individual targets
	Distributor                   string = "distributor"
	Ingester                      string = "ingester"
	MetricsGenerator              string = "metrics-generator"
	MetricsGeneratorNoLocalBlocks string = "metrics-generator-no-local-blocks"
	Querier                       string = "querier"
	QueryFrontend                 string = "query-frontend"
	Compactor                     string = "compactor"
	BlockBuilder                  string = "block-builder"
	BackendScheduler              string = "backend-scheduler"
	BackendWorker                 string = "backend-worker"

	// composite targets
	SingleBinary         string = "all"
	ScalableSingleBinary string = "scalable-single-binary"

	// ring names
	ringIngester          string = "ingester"
	ringMetricsGenerator  string = "metrics-generator"
	ringSecondaryIngester string = "secondary-ingester"
)

func (t *App) initServer() (services.Service, error) {
	t.cfg.Server.MetricsNamespace = metricsNamespace
	t.cfg.Server.ExcludeRequestInLog = true

	if t.cfg.EnableGoRuntimeMetrics {
		// unregister default Go collector
		prometheus.Unregister(collectors.NewGoCollector())
		// register Go collector with all available runtime metrics
		prometheus.MustRegister(collectors.NewGoCollector(
			collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsAll),
		))
	}

	servicesToWaitFor := func() []services.Service {
		svs := []services.Service(nil)
		for m, s := range t.serviceMap {
			// Server should not wait for itself.
			if m != Server && m != InternalServer {
				svs = append(svs, s)
			}
		}
		return svs
	}

	// add unary and stream timeout interceptors for the query-frontend if configured
	// this same timeout is enforced for http in the initQueryFrontend() function
	if t.cfg.Frontend.APITimeout > 0 && t.isModuleActive(QueryFrontend) {
		t.cfg.Server.GRPCMiddleware = append(t.cfg.Server.GRPCMiddleware, interceptor.NewFrontendAPIUnaryTimeout(t.cfg.Frontend.APITimeout))
		t.cfg.Server.GRPCStreamMiddleware = append(t.cfg.Server.GRPCStreamMiddleware, interceptor.NewFrontendAPIStreamTimeout(t.cfg.Frontend.APITimeout))
	}

	return t.Server.StartAndReturnService(t.cfg.Server, t.cfg.StreamOverHTTPEnabled, servicesToWaitFor)
}

func (t *App) initInternalServer() (services.Service, error) {
	if !t.cfg.InternalServer.Enable {
		return services.NewIdleService(nil, nil), nil
	}

	DisableSignalHandling(&t.cfg.InternalServer.Config)
	serv, err := server.New(t.cfg.InternalServer.Config)
	if err != nil {
		return nil, err
	}

	servicesToWaitFor := func() []services.Service {
		svs := []services.Service(nil)
		for m, s := range t.serviceMap {
			// Server should not wait for itself or the server
			if m != InternalServer && m != Server {
				svs = append(svs, s)
			}
		}
		return svs
	}

	t.InternalServer = serv
	s := NewServerService(t.InternalServer, servicesToWaitFor)

	return s, nil
}

func (t *App) initIngesterRing() (services.Service, error) {
	return t.initReadRing(t.cfg.Ingester.LifecyclerConfig.RingConfig, ringIngester, t.cfg.Ingester.OverrideRingKey)
}

func (t *App) initGeneratorRing() (services.Service, error) {
	return t.initReadRing(t.cfg.Generator.Ring.ToRingConfig(), ringMetricsGenerator, t.cfg.Generator.OverrideRingKey)
}

// initSecondaryIngesterRing is an optional ring for the queriers. This secondary ring is useful in edge cases and should
// not be used generally. Use this if you need one set of queries to query 2 different sets of ingesters.
func (t *App) initSecondaryIngesterRing() (services.Service, error) {
	// if no secondary ring is configured, then bail by returning a dummy service
	if t.cfg.Querier.SecondaryIngesterRing == "" {
		return services.NewIdleService(nil, nil), nil
	}

	// note that this is using the same cnofig as above. both rings have to be configured the same
	return t.initReadRing(t.cfg.Ingester.LifecyclerConfig.RingConfig, ringSecondaryIngester, t.cfg.Querier.SecondaryIngesterRing)
}

func (t *App) initReadRing(cfg ring.Config, name, key string) (*ring.Ring, error) {
	ring, err := tempo_ring.New(cfg, name, key, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, fmt.Errorf("failed to create ring %s: %w", name, err)
	}

	t.Server.HTTPRouter().Handle("/"+name+"/ring", ring)
	t.readRings[name] = ring

	return ring, nil
}

func (t *App) initPartitionRing() (services.Service, error) {
	if !t.cfg.Ingest.Enabled {
		return nil, nil
	}

	kvClient, err := kv.NewClient(t.cfg.Ingester.IngesterPartitionRing.KVStore, ring.GetPartitionRingCodec(), kv.RegistererWithKVName(prometheus.DefaultRegisterer, ingester.PartitionRingName+"-watcher"), util_log.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating KV store for ingester partitions ring watcher: %w", err)
	}

	t.partitionRingWatcher = ring.NewPartitionRingWatcher(ingester.PartitionRingName, ingester.PartitionRingKey, kvClient, util_log.Logger, prometheus.WrapRegistererWithPrefix("tempo_", prometheus.DefaultRegisterer))
	t.partitionRing = ring.NewPartitionInstanceRing(t.partitionRingWatcher, t.readRings[ringIngester], t.cfg.Ingester.LifecyclerConfig.RingConfig.HeartbeatTimeout)

	// Expose a web page to view the partitions ring state.
	t.Server.HTTPRouter().Path("/partition-ring").Methods("GET", "POST").Handler(ring.NewPartitionRingPageHandler(t.partitionRingWatcher, ring.NewPartitionRingEditor(ingester.PartitionRingKey, kvClient)))

	return t.partitionRingWatcher, nil
}

func (t *App) initOverrides() (services.Service, error) {
	o, err := overrides.NewOverrides(t.cfg.Overrides, newRuntimeConfigValidator(&t.cfg), prometheus.DefaultRegisterer)
	if err != nil {
		return nil, fmt.Errorf("failed to create overrides: %w", err)
	}
	t.Overrides = o

	prometheus.MustRegister(&t.cfg.Overrides)

	if t.cfg.Overrides.PerTenantOverrideConfig != "" {
		prometheus.MustRegister(t.Overrides)
	}

	t.Server.HTTPRouter().Path("/status/overrides").HandlerFunc(overrides.TenantsHandler(t.Overrides)).Methods("GET")
	t.Server.HTTPRouter().Path("/status/overrides/{tenant}").HandlerFunc(overrides.TenantStatusHandler(t.Overrides)).Methods("GET")

	return t.Overrides, nil
}

func (t *App) initOverridesAPI() (services.Service, error) {
	cfg := t.cfg.Overrides.UserConfigurableOverridesConfig

	if !cfg.Enabled {
		return services.NewIdleService(nil, nil), nil
	}

	userConfigOverridesAPI, err := userconfigurableoverridesapi.New(&cfg.API, &cfg.Client, t.Overrides, newOverridesValidator(&t.cfg))
	if err != nil {
		return nil, fmt.Errorf("failed to create user-configurable overrides API: %w", err)
	}

	overridesPath := addHTTPAPIPrefix(&t.cfg, api.PathOverrides)
	wrapHandler := func(h http.HandlerFunc) http.Handler {
		return t.HTTPAuthMiddleware.Wrap(h)
	}

	t.Server.HTTPRouter().Path(overridesPath).Methods(http.MethodGet).Handler(wrapHandler(userConfigOverridesAPI.GetHandler))
	t.Server.HTTPRouter().Path(overridesPath).Methods(http.MethodPost).Handler(wrapHandler(userConfigOverridesAPI.PostHandler))
	t.Server.HTTPRouter().Path(overridesPath).Methods(http.MethodPatch).Handler(wrapHandler(userConfigOverridesAPI.PatchHandler))
	t.Server.HTTPRouter().Path(overridesPath).Methods(http.MethodDelete).Handler(wrapHandler(userConfigOverridesAPI.DeleteHandler))

	return userConfigOverridesAPI, nil
}

func (t *App) initDistributor() (services.Service, error) {
	t.cfg.Distributor.KafkaConfig = t.cfg.Ingest.Kafka
	t.cfg.Distributor.KafkaWritePathEnabled = t.cfg.Ingest.Enabled // TODO: Don't mix config params

	// todo: make ingester client a module instead of passing the config everywhere
	distributor, err := distributor.New(t.cfg.Distributor,
		t.cfg.IngesterClient,
		t.readRings[ringIngester],
		t.cfg.GeneratorClient,
		t.readRings[ringMetricsGenerator],
		t.partitionRing,
		t.Overrides,
		t.TracesConsumerMiddleware,
		log.Logger, t.cfg.Server.LogLevel, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, fmt.Errorf("failed to create distributor: %w", err)
	}
	t.distributor = distributor

	if distributor.DistributorRing != nil {
		t.Server.HTTPRouter().Handle("/distributor/ring", distributor.DistributorRing)
	}

	if usageHandler := distributor.UsageTrackerHandler(); usageHandler != nil {
		t.Server.HTTPRouter().Handle("/usage_metrics", usageHandler)
	}

	return t.distributor, nil
}

func (t *App) initIngester() (services.Service, error) {
	t.cfg.Ingester.LifecyclerConfig.ListenPort = t.cfg.Server.GRPCListenPort
	t.cfg.Ingester.DedicatedColumns = t.cfg.StorageConfig.Trace.Block.DedicatedColumns
	t.cfg.Ingester.IngestStorageConfig = t.cfg.Ingest

	// In SingleBinary mode don't try to discover parition from host name. Always use
	// partition 0. This is for small installs or local/debugging setups.
	singlePartition := t.cfg.Target == SingleBinary

	ingester, err := ingester.New(t.cfg.Ingester, t.store, t.Overrides, prometheus.DefaultRegisterer, singlePartition)
	if err != nil {
		return nil, fmt.Errorf("failed to create ingester: %w", err)
	}
	t.ingester = ingester

	tempopb.RegisterPusherServer(t.Server.GRPC(), t.ingester)
	tempopb.RegisterQuerierServer(t.Server.GRPC(), t.ingester)
	t.Server.HTTPRouter().Path("/flush").Handler(http.HandlerFunc(t.ingester.FlushHandler))
	t.Server.HTTPRouter().Path("/shutdown").Handler(http.HandlerFunc(t.ingester.ShutdownHandler))
	t.Server.HTTPRouter().Methods(http.MethodGet, http.MethodPost, http.MethodDelete).
		Path("/ingester/prepare-partition-downscale").
		Handler(http.HandlerFunc(t.ingester.PreparePartitionDownscaleHandler))
	return t.ingester, nil
}

func (t *App) initGenerator() (services.Service, error) {
	if t.cfg.Generator.Processor.LocalBlocks.FlushToStorage &&
		t.store == nil {
		return nil, fmt.Errorf("generator.processor.local-blocks.flush-to-storage is enabled but no storage backend is configured")
	}

	t.cfg.Generator.Ring.ListenPort = t.cfg.Server.GRPCListenPort

	t.cfg.Generator.Ingest = t.cfg.Ingest
	t.cfg.Generator.Ingest.Kafka.ConsumerGroup = generator.ConsumerGroup

	genSvc, err := generator.New(&t.cfg.Generator, t.Overrides, prometheus.DefaultRegisterer, t.partitionRing, t.store, log.Logger)
	if errors.Is(err, generator.ErrUnconfigured) && t.cfg.Target != MetricsGenerator { // just warn if we're not running the metrics-generator
		level.Warn(log.Logger).Log("msg", "metrics-generator is not configured.", "err", err)
		return services.NewIdleService(nil, nil), nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics-generator: %w", err)
	}
	t.generator = genSvc

	spanStatsHandler := t.HTTPAuthMiddleware.Wrap(http.HandlerFunc(t.generator.SpanMetricsHandler))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixGenerator, addHTTPAPIPrefix(&t.cfg, api.PathSpanMetrics)), spanStatsHandler)

	queryRangeHandler := t.HTTPAuthMiddleware.Wrap(http.HandlerFunc(t.generator.QueryRangeHandler))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixGenerator, addHTTPAPIPrefix(&t.cfg, api.PathMetricsQueryRange)), queryRangeHandler)

	tempopb.RegisterMetricsGeneratorServer(t.Server.GRPC(), t.generator)

	return t.generator, nil
}

func (t *App) initGeneratorNoLocalBlocks() (services.Service, error) {
	reg := prometheus.DefaultRegisterer

	t.cfg.Generator.Ingest = t.cfg.Ingest

	// In this mode, the generator runs as a stateless queue consumer that reads from
	// Kafka and remote writes to a Prometheus-compatible metrics store.
	if !t.cfg.Ingest.Enabled {
		return nil, errors.New("ingest storage must be enabled to run metrics generator in this mode")
	}
	// The localblocks processor is disabled in this mode.
	t.cfg.Generator.DisableLocalBlocks = true
	// The store is used only by the localblocks processor. We don't need it when
	// running with that processor disabled so we keep the default zero value.
	var store tempo_storage.Store
	// In this mode, the generator does not need to become available to serve
	// queries, so we can skip setting up a gRPC server.
	t.cfg.Generator.DisableGRPC = true

	var err error
	t.generator, err = generator.New(&t.cfg.Generator, t.Overrides, reg, t.generatorRingWatcher, store, log.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics-generator: %w", err)
	}

	return t.generator, nil
}

func (t *App) initGeneratorRingWatcher() (services.Service, error) {
	reg := prometheus.DefaultRegisterer

	kvRegisterer := kv.RegistererWithKVName(reg, t.cfg.Generator.OverrideRingKey+"-watcher")
	kvClient, err := kv.NewClient(t.cfg.Generator.Ring.KVStore, ring.GetPartitionRingCodec(), kvRegisterer, util_log.Logger)
	if err != nil {
		return nil, fmt.Errorf("creating KV store for generator partition ring watcher: %w", err)
	}

	t.generatorRingWatcher = ring.NewPartitionRingWatcher(
		t.cfg.Generator.OverrideRingKey,
		t.cfg.Generator.OverrideRingKey,
		kvClient,
		util_log.Logger,
		prometheus.WrapRegistererWithPrefix("tempo_", reg),
	)

	// Expose a web page to view the partition ring state.
	editor := ring.NewPartitionRingEditor(t.cfg.Generator.OverrideRingKey, kvClient)
	t.Server.HTTPRouter().Path("/partition/ring").Methods("GET", "POST").
		Handler(ring.NewPartitionRingPageHandler(t.generatorRingWatcher, editor))

	return t.generatorRingWatcher, nil
}

func (t *App) initBlockBuilder() (services.Service, error) {
	if !t.cfg.Ingest.Enabled {
		return services.NewIdleService(nil, nil), nil
	}

	t.cfg.BlockBuilder.IngestStorageConfig = t.cfg.Ingest
	t.cfg.BlockBuilder.IngestStorageConfig.Kafka.ConsumerGroup = blockbuilder.ConsumerGroup

	if t.cfg.Target == SingleBinary && len(t.cfg.BlockBuilder.AssignedPartitions) == 0 {
		// In SingleBinary mode always use partition 0. This is for small installs or local/debugging setups.
		t.cfg.BlockBuilder.AssignedPartitions = map[string][]int32{t.cfg.BlockBuilder.InstanceID: {0}}
	}

	bb, err := blockbuilder.New(t.cfg.BlockBuilder, log.Logger, t.partitionRing, t.Overrides, t.store)
	if err != nil {
		return nil, fmt.Errorf("failed to create block-builder: %w", err)
	}
	t.blockBuilder = bb

	return t.blockBuilder, nil
}

func (t *App) initQuerier() (services.Service, error) {
	// validate worker config
	// if we're not in single binary mode and worker address is not specified - bail
	if t.cfg.Target != SingleBinary && t.cfg.Querier.Worker.FrontendAddress == "" {
		return nil, fmt.Errorf("frontend worker address not specified")
	} else if t.cfg.Target == SingleBinary {
		// if we're in single binary mode with no worker address specified, register default endpoint
		if t.cfg.Querier.Worker.FrontendAddress == "" {
			t.cfg.Querier.Worker.FrontendAddress = fmt.Sprintf("127.0.0.1:%d", t.cfg.Server.GRPCListenPort)
			level.Warn(log.Logger).Log("msg", "Worker address is empty in single binary mode. Attempting automatic worker configuration. If queries are unresponsive consider configuring the worker explicitly.", "address", t.cfg.Querier.Worker.FrontendAddress)
		}
	}

	// do not enable polling if this is the single binary. in that case the compactor will take care of polling
	if t.cfg.Target == Querier {
		t.store.EnablePolling(context.Background(), nil, false)
	}

	ingesterRings := []ring.ReadRing{t.readRings[ringIngester]}
	if ring := t.readRings[ringSecondaryIngester]; ring != nil {
		ingesterRings = append(ingesterRings, ring)
	}

	querier, err := querier.New(
		t.cfg.Querier,
		t.cfg.IngesterClient,
		ingesterRings,
		t.cfg.GeneratorClient,
		t.readRings[ringMetricsGenerator],
		t.store,
		t.Overrides,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create querier: %w", err)
	}
	t.querier = querier

	middleware := middleware.Merge(
		t.HTTPAuthMiddleware,
	)

	tracesHandler := middleware.Wrap(http.HandlerFunc(t.querier.TraceByIDHandler))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixQuerier, addHTTPAPIPrefix(&t.cfg, api.PathTraces)), tracesHandler)

	tracesHandlerV2 := middleware.Wrap(http.HandlerFunc(t.querier.TraceByIDHandlerV2))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixQuerier, addHTTPAPIPrefix(&t.cfg, api.PathTracesV2)), tracesHandlerV2)

	searchHandler := t.HTTPAuthMiddleware.Wrap(http.HandlerFunc(t.querier.SearchHandler))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixQuerier, addHTTPAPIPrefix(&t.cfg, api.PathSearch)), searchHandler)

	searchTagsHandler := t.HTTPAuthMiddleware.Wrap(http.HandlerFunc(t.querier.SearchTagsHandler))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixQuerier, addHTTPAPIPrefix(&t.cfg, api.PathSearchTags)), searchTagsHandler)

	searchTagsV2Handler := t.HTTPAuthMiddleware.Wrap(http.HandlerFunc(t.querier.SearchTagsV2Handler))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixQuerier, addHTTPAPIPrefix(&t.cfg, api.PathSearchTagsV2)), searchTagsV2Handler)

	searchTagValuesHandler := t.HTTPAuthMiddleware.Wrap(http.HandlerFunc(t.querier.SearchTagValuesHandler))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixQuerier, addHTTPAPIPrefix(&t.cfg, api.PathSearchTagValues)), searchTagValuesHandler)

	searchTagValuesV2Handler := t.HTTPAuthMiddleware.Wrap(http.HandlerFunc(t.querier.SearchTagValuesV2Handler))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixQuerier, addHTTPAPIPrefix(&t.cfg, api.PathSearchTagValuesV2)), searchTagValuesV2Handler)

	spanMetricsSummaryHandler := t.HTTPAuthMiddleware.Wrap(http.HandlerFunc(t.querier.SpanMetricsSummaryHandler))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixQuerier, addHTTPAPIPrefix(&t.cfg, api.PathSpanMetricsSummary)), spanMetricsSummaryHandler)

	queryRangeHandler := t.HTTPAuthMiddleware.Wrap(http.HandlerFunc(t.querier.QueryRangeHandler))
	t.Server.HTTPRouter().Handle(path.Join(api.PathPrefixQuerier, addHTTPAPIPrefix(&t.cfg, api.PathMetricsQueryRange)), queryRangeHandler)

	return t.querier, t.querier.CreateAndRegisterWorker(t.Server.HTTPHandler())
}

func (t *App) initQueryFrontend() (services.Service, error) {
	// cortexTripper is a bridge between http and httpgrpc.
	// It does the job of passing data to the cortex frontend code.
	cortexTripper, v1, err := frontend.InitFrontend(t.cfg.Frontend.Config, log.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}
	t.frontend = v1

	// create query frontend
	queryFrontend, err := frontend.New(t.cfg.Frontend, cortexTripper, t.Overrides, t.store, t.cacheProvider, t.cfg.HTTPAPIPrefix, t.HTTPAuthMiddleware, log.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, err
	}

	// register grpc server for queriers to connect to
	frontend_v1pb.RegisterFrontendServer(t.Server.GRPC(), t.frontend)
	// we register the streaming querier service on both the http and grpc servers. Grafana expects
	// this GRPC service to be available on the HTTP server.
	tempopb.RegisterStreamingQuerierServer(t.Server.GRPC(), queryFrontend)

	httpAPIMiddleware := []middleware.Interface{
		t.HTTPAuthMiddleware,
		httpGzipMiddleware(),
	}

	// use the api timeout for http requests if set. note that this is set in initServer() for
	// grpc requests
	if t.cfg.Frontend.APITimeout > 0 {
		httpAPIMiddleware = append(httpAPIMiddleware, middleware.NewTimeoutMiddleware(t.cfg.Frontend.APITimeout, "unable to process request in the configured timeout", kitlog.NewNopLogger()))
	}

	// wrap handlers with auth
	base := middleware.Merge(httpAPIMiddleware...)

	// http trace by id endpoint
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathTraces), base.Wrap(queryFrontend.TraceByIDHandler))
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathTracesV2), base.Wrap(queryFrontend.TraceByIDHandlerV2))

	// http search endpoints
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathSearch), base.Wrap(queryFrontend.SearchHandler))
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathSearchTags), base.Wrap(queryFrontend.SearchTagsHandler))
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathSearchTagsV2), base.Wrap(queryFrontend.SearchTagsV2Handler))
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathSearchTagValues), base.Wrap(queryFrontend.SearchTagsValuesHandler))
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathSearchTagValuesV2), base.Wrap(queryFrontend.SearchTagsValuesV2Handler))

	// http metrics endpoints
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathSpanMetricsSummary), base.Wrap(queryFrontend.MetricsSummaryHandler))
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathMetricsQueryInstant), base.Wrap(queryFrontend.MetricsQueryInstantHandler))
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathMetricsQueryRange), base.Wrap(queryFrontend.MetricsQueryRangeHandler))

	// http mcp endpoint
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathMCP), base.Wrap(queryFrontend.MCPHandler))

	// the query frontend needs to have knowledge of the blocks so it can shard search jobs
	if t.cfg.Target == QueryFrontend {
		t.store.EnablePolling(context.Background(), nil, false)
	}

	// http query echo endpoint
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathEcho), echoHandler())

	// http endpoint to see usage stats data
	t.Server.HTTPRouter().Handle(addHTTPAPIPrefix(&t.cfg, api.PathUsageStats), usageStatsHandler(t.cfg.UsageReport))

	// todo: queryFrontend should implement service.Service and take the cortex frontend a submodule
	return t.frontend, nil
}

//go:embed static
var staticFiles embed.FS

func (t *App) initCompactor() (services.Service, error) {
	if t.cfg.Target == ScalableSingleBinary && t.cfg.Compactor.ShardingRing.KVStore.Store == "" {
		t.cfg.Compactor.ShardingRing.KVStore.Store = "memberlist"
	}

	compactor, err := compactor.New(t.cfg.Compactor, t.store, t.Overrides, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, fmt.Errorf("failed to create compactor: %w", err)
	}
	t.compactor = compactor

	if t.compactor.Ring != nil {
		t.Server.HTTPRouter().Handle("/compactor/ring", t.compactor.Ring)
	}

	return t.compactor, nil
}

func (t *App) initOptionalStore() (services.Service, error) {
	// Used by the local-blocs processor to flush RF1 blocks to storage.
	// Only initialize if it's configured.
	if t.cfg.StorageConfig.Trace.Backend == "" {
		return services.NewIdleService(nil, nil), nil
	}

	return t.initStore()
}

func (t *App) initStore() (services.Service, error) {
	// the only component that needs a functioning tempodb pool are the queriers. all other components will just spin up
	// hundreds of never used pool goroutines. set pool size to 0 here to avoid that.
	if t.cfg.Target != Querier && t.cfg.Target != SingleBinary && t.cfg.Target != ScalableSingleBinary {
		t.cfg.StorageConfig.Trace.Pool.MaxWorkers = 0
		t.cfg.StorageConfig.Trace.Pool.QueueDepth = 0
	}

	store, err := tempo_storage.NewStore(t.cfg.StorageConfig, t.cacheProvider, log.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}
	t.store = store

	return t.store, nil
}

func (t *App) initMemberlistKV() (services.Service, error) {
	reg := prometheus.DefaultRegisterer
	t.cfg.MemberlistKV.MetricsNamespace = metricsNamespace
	t.cfg.MemberlistKV.Codecs = append(t.cfg.MemberlistKV.Codecs,
		ring.GetCodec(),
		ring.GetPartitionRingCodec(),
		usagestats.JSONCodec,
	)

	dnsProviderReg := prometheus.WrapRegistererWithPrefix(
		"tempo_",
		prometheus.WrapRegistererWith(
			prometheus.Labels{"name": "memberlist"},
			reg,
		),
	)

	dnsProvider := dns.NewProvider(log.Logger, dnsProviderReg, dns.GolangResolverType)
	t.MemberlistKV = memberlist.NewKVInitService(&t.cfg.MemberlistKV, log.Logger, dnsProvider, reg)

	t.cfg.Ingester.LifecyclerConfig.RingConfig.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.cfg.Ingester.IngesterPartitionRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.cfg.Generator.Ring.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.cfg.Distributor.DistributorRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.cfg.Compactor.ShardingRing.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV
	t.cfg.BackendWorker.Ring.KVStore.MemberlistKV = t.MemberlistKV.GetMemberlistKV

	// Only the memberlist endpoint uses static files currently
	t.Server.HTTPRouter().PathPrefix("/static/").HandlerFunc(http.FileServer(http.FS(staticFiles)).ServeHTTP).Methods("GET")

	t.Server.HTTPRouter().Handle("/memberlist", memberlistStatusHandler("", t.MemberlistKV))

	return t.MemberlistKV, nil
}

func (t *App) initUsageReport() (services.Service, error) {
	if !t.cfg.UsageReport.Enabled {
		return nil, nil
	}

	t.cfg.UsageReport.Leader = false
	if t.isModuleActive(Ingester) {
		t.cfg.UsageReport.Leader = true
	}

	usagestats.Target(t.cfg.Target)

	var err error
	var reader backend.RawReader
	var writer backend.RawWriter

	switch t.cfg.StorageConfig.Trace.Backend {
	case backend.Local:
		reader, writer, _, err = local.New(t.cfg.StorageConfig.Trace.Local)
	case backend.GCS:
		reader, writer, _, err = gcs.New(t.cfg.StorageConfig.Trace.GCS)
	case backend.S3:
		reader, writer, _, err = s3.New(t.cfg.StorageConfig.Trace.S3)
	case backend.Azure:
		reader, writer, _, err = azure.New(t.cfg.StorageConfig.Trace.Azure)
	default:
		err = fmt.Errorf("unknown backend %s", t.cfg.StorageConfig.Trace.Backend)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to initialize usage report: %w", err)
	}

	ur, err := usagestats.NewReporter(t.cfg.UsageReport, t.cfg.Ingester.LifecyclerConfig.RingConfig.KVStore, reader, writer, util_log.Logger, prometheus.DefaultRegisterer)
	if err != nil {
		level.Info(util_log.Logger).Log("msg", "failed to initialize usage report", "err", err)
		return nil, nil
	}
	t.usageReport = ur
	return ur, nil
}

func (t *App) initCacheProvider() (services.Service, error) {
	c, err := cache.NewProvider(&t.cfg.CacheProvider, util_log.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache provider: %w", err)
	}

	t.cacheProvider = c
	return c, nil
}

func (t *App) initBackendScheduler() (services.Service, error) {
	if t.cfg.Target == BackendScheduler {
		t.cfg.BackendScheduler.Poll = true
	}

	var (
		err    error
		reader backend.RawReader
		writer backend.RawWriter
	)

	switch t.cfg.StorageConfig.Trace.Backend {
	case backend.Local:
		reader, writer, _, err = local.New(t.cfg.StorageConfig.Trace.Local)
	case backend.GCS:
		reader, writer, _, err = gcs.New(t.cfg.StorageConfig.Trace.GCS)
	case backend.S3:
		reader, writer, _, err = s3.New(t.cfg.StorageConfig.Trace.S3)
	case backend.Azure:
		reader, writer, _, err = azure.New(t.cfg.StorageConfig.Trace.Azure)
	default:
		err = fmt.Errorf("unknown backend %s", t.cfg.StorageConfig.Trace.Backend)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to initialize backendscheduler reader/writer: %w", err)
	}

	scheduler, err := backendscheduler.New(t.cfg.BackendScheduler, t.store, t.Overrides, reader, writer)
	if err != nil {
		return nil, fmt.Errorf("failed to create backend scheduler: %w", err)
	}

	// Register the GRPC service
	tempopb.RegisterBackendSchedulerServer(t.Server.GRPC(), scheduler)

	t.Server.HTTPRouter().Path("/status/backendscheduler").HandlerFunc(scheduler.StatusHandler)

	t.backendScheduler = scheduler

	return scheduler, nil
}

func (t *App) initBackendWorker() (services.Service, error) {
	if t.cfg.Target == BackendWorker {
		t.cfg.BackendWorker.Poll = true
	}

	worker, err := backendworker.New(t.cfg.BackendWorker, t.cfg.BackenSchedulerClient, t.store, t.Overrides, prometheus.DefaultRegisterer)
	if err != nil {
		return nil, fmt.Errorf("failed to create backend scheduler: %w", err)
	}
	t.backendWorker = worker

	if t.backendWorker.Ring != nil {
		t.Server.HTTPRouter().Handle("/backend-worker/ring", t.backendWorker.Ring)
	}

	return worker, nil
}

func (t *App) setupModuleManager() error {
	mm := modules.NewManager(log.Logger)

	// Common is a module that exists only to map dependencies
	const Common = "common"

	mm.RegisterModule(Store, t.initStore, modules.UserInvisibleModule)
	mm.RegisterModule(OptionalStore, t.initOptionalStore, modules.UserInvisibleModule)
	mm.RegisterModule(Server, t.initServer, modules.UserInvisibleModule)
	mm.RegisterModule(InternalServer, t.initInternalServer, modules.UserInvisibleModule)
	mm.RegisterModule(MemberlistKV, t.initMemberlistKV, modules.UserInvisibleModule)
	mm.RegisterModule(Overrides, t.initOverrides, modules.UserInvisibleModule)
	mm.RegisterModule(OverridesAPI, t.initOverridesAPI)
	mm.RegisterModule(UsageReport, t.initUsageReport)
	mm.RegisterModule(CacheProvider, t.initCacheProvider, modules.UserInvisibleModule)
	mm.RegisterModule(IngesterRing, t.initIngesterRing, modules.UserInvisibleModule)
	mm.RegisterModule(MetricsGeneratorRing, t.initGeneratorRing, modules.UserInvisibleModule)
	mm.RegisterModule(GeneratorRingWatcher, t.initGeneratorRingWatcher, modules.UserInvisibleModule)
	mm.RegisterModule(SecondaryIngesterRing, t.initSecondaryIngesterRing, modules.UserInvisibleModule)
	mm.RegisterModule(PartitionRing, t.initPartitionRing, modules.UserInvisibleModule)

	mm.RegisterModule(Common, nil, modules.UserInvisibleModule)

	mm.RegisterModule(Distributor, t.initDistributor)
	mm.RegisterModule(Ingester, t.initIngester)
	mm.RegisterModule(Querier, t.initQuerier)
	mm.RegisterModule(QueryFrontend, t.initQueryFrontend)
	mm.RegisterModule(Compactor, t.initCompactor)
	mm.RegisterModule(MetricsGenerator, t.initGenerator)
	mm.RegisterModule(MetricsGeneratorNoLocalBlocks, t.initGeneratorNoLocalBlocks)
	mm.RegisterModule(BlockBuilder, t.initBlockBuilder)
	mm.RegisterModule(BackendScheduler, t.initBackendScheduler)
	mm.RegisterModule(BackendWorker, t.initBackendWorker)

	mm.RegisterModule(SingleBinary, nil)
	mm.RegisterModule(ScalableSingleBinary, nil)

	deps := map[string][]string{
		// InternalServer: nil,
		// CacheProvider:  nil,
		Store:                 {CacheProvider},
		Server:                {InternalServer},
		Overrides:             {Server},
		OverridesAPI:          {Server, Overrides},
		MemberlistKV:          {Server},
		UsageReport:           {MemberlistKV},
		IngesterRing:          {Server, MemberlistKV},
		SecondaryIngesterRing: {Server, MemberlistKV},
		MetricsGeneratorRing:  {Server, MemberlistKV},
		PartitionRing:         {MemberlistKV, Server, IngesterRing},
		GeneratorRingWatcher:  {MemberlistKV},

		Common: {UsageReport, Server, Overrides},

		// individual targets
		QueryFrontend:                 {Common, Store, OverridesAPI},
		Distributor:                   {Common, IngesterRing, MetricsGeneratorRing, PartitionRing},
		Ingester:                      {Common, Store, MemberlistKV, PartitionRing},
		MetricsGenerator:              {Common, OptionalStore, MemberlistKV, PartitionRing},
		MetricsGeneratorNoLocalBlocks: {Common, GeneratorRingWatcher},
		Querier:                       {Common, Store, IngesterRing, MetricsGeneratorRing, SecondaryIngesterRing},
		Compactor:                     {Common, Store, MemberlistKV},
		BlockBuilder:                  {Common, Store, MemberlistKV, PartitionRing},
		BackendScheduler:              {Common, Store},
		BackendWorker:                 {Common, Store, MemberlistKV},

		// composite targets
		SingleBinary:         {Compactor, QueryFrontend, Querier, Ingester, Distributor, MetricsGenerator, BlockBuilder},
		ScalableSingleBinary: {SingleBinary},
	}

	for mod, targets := range deps {
		if err := mm.AddDependency(mod, targets...); err != nil {
			return err
		}
	}

	t.ModuleManager = mm

	t.deps = deps

	return nil
}

func (t *App) isModuleActive(m string) bool {
	if t.cfg.Target == m {
		return true
	}
	if t.recursiveIsModuleActive(t.cfg.Target, m) {
		return true
	}

	return false
}

func (t *App) recursiveIsModuleActive(target, m string) bool {
	if targetDeps, ok := t.deps[target]; ok {
		for _, dep := range targetDeps {
			if dep == m {
				return true
			}
			if t.recursiveIsModuleActive(dep, m) {
				return true
			}
		}
	}
	return false
}

func addHTTPAPIPrefix(cfg *Config, apiPath string) string {
	return path.Join(cfg.HTTPAPIPrefix, apiPath)
}

func echoHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "echo", http.StatusOK)
	}
}

func usageStatsHandler(urCfg usagestats.Config) http.HandlerFunc {
	if !urCfg.Enabled {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "usage-stats is not enabled", http.StatusOK)
		}
	}

	// usage stats is Enabled, build and return usage stats json
	reportStr, err := jsoniter.MarshalToString(usagestats.BuildStats())
	if err != nil {
		return func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "error building usage report", http.StatusInternalServerError)
		}
	}

	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, reportStr)
	}
}
