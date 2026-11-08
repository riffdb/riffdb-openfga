package openfgarun

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/fsnotify/fsnotify"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	authzenv1 "github.com/openfga/api/proto/authzen/v1"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/gateway"
	"github.com/openfga/openfga/pkg/logger"
	httpmiddleware "github.com/openfga/openfga/pkg/middleware/http"
	"github.com/openfga/openfga/pkg/middleware/recovery"
	"github.com/openfga/openfga/pkg/server"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"

	riffdbopenfga "github.com/riffdb/riffdb-openfga"
)

const (
	// ProjectName is the metric namespace matching stock OpenFGA.
	ProjectName = "openfga"

	datastoreEngineFlag = "datastore-engine"
	datastoreURIFlag    = "datastore-uri"
	defaultEngineRiffDB = "riffdb"
)

var (
	// Version is the pinned OpenFGA version, overridable via -ldflags.
	Version = "v1.18.3"
	// Commit is the build commit SHA, overridable via -ldflags.
	Commit = "unknown"
	// Date is the build timestamp, overridable via -ldflags.
	Date = "unknown"
)

var buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: ProjectName,
	Name:      "build_info",
	Help:      "Applied build information,Labeled by version, build commit, date and the Go version cleanly.",
}, []string{"version", "commit", "date", "goversion"})

func init() {
	prometheus.MustRegister(buildInfo)
	buildInfo.WithLabelValues(Version, Commit, Date, runtime.Version()).Set(1)
}

// RiffDBConfig holds the RiffDB transport session and adapter settings for the runner.
type RiffDBConfig struct {
	riffdbopenfga.ConnectConfig
	CommandAttempts uint32
}

// DefaultRiffDBConfig returns the default RiffDB runner configuration populated
// from locked identity constants and environment variables.
func DefaultRiffDBConfig() RiffDBConfig {
	connCfg, err := riffdbopenfga.ConnectConfigFromEnv()
	if err != nil {
		connCfg = riffdbopenfga.ConnectConfig{
			Database:                riffdbopenfga.DefaultDatabase,
			Role:                    riffdbopenfga.DefaultRole,
			RoleDefinitionHash:      riffdbopenfga.DefaultRoleDefinitionHash,
			OperationCatalogHash:    riffdbopenfga.OperationCatalogHash,
			ApplicationManifestHash: riffdbopenfga.ApplicationManifestHash,
			ContractLineage:         riffdbopenfga.ContractLineage,
			ContractVersion:         riffdbopenfga.ContractVersion,
			ContractBundleHash:      riffdbopenfga.ContractBundleHash,
		}
	}
	return RiffDBConfig{
		ConnectConfig:   connCfg,
		CommandAttempts: 3,
	}
}

// ApplyLoopbackArgs checks whether args match the 10 positional arguments
// passed by `riffdb dev --run --go-runner-package cmd/openfga-riffdb`. If they
// do, it populates the standard RIFFDB_LOOPBACK_* environment variables and
// returns `[]string{"run"}, true`.
func ApplyLoopbackArgs(args []string) ([]string, bool) {
	envKeys := riffdbopenfga.LoopbackEnvironmentVariables[:]
	if len(args) != len(envKeys) {
		return args, false
	}
	first := strings.TrimSpace(args[0])
	if first == "" || strings.HasPrefix(first, "-") {
		return args, false
	}
	switch first {
	case "run", "validate-models", "version", "help", "completion":
		return args, false
	}
	for i, key := range envKeys {
		_ = os.Setenv(key, args[i])
	}
	return []string{"run"}, true
}

// NewRootCommand creates the root CLI command (`openfga-riffdb`) with `run`,
// `validate-models`, and `version` subcommands.
func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "openfga",
		Short: "A high performance and flexible authorization/permission engine built for developers and inspired by Google Zanzibar",
		Long: `A high performance and flexible authorization/permission engine built for developers and inspired by Google Zanzibar.

OpenFGA is designed to make it easy for developers to model their application permissions, and to add and integrate fine-grained authorization into their applications.

Complete documentation is available at https://openfga.dev`,
	}

	rootCmd.AddCommand(
		NewRunCommand(),
		NewValidateCommand(),
		NewVersionCommand(),
	)

	return rootCmd
}

// NewVersionCommand returns the `version` subcommand.
func NewVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Reports the OpenFGA version",
		Run: func(cmd *cobra.Command, _ []string) {
			cmd.Printf("OpenFGA version `%s` build from `%s` on `%s` (engine: riffdb, contract: %s@%d)\n",
				Version,
				Commit,
				Date,
				riffdbopenfga.ContractLineage,
				riffdbopenfga.ContractVersion,
			)
		},
	}
}

// NewRunCommand returns the `run` subcommand with all stock OpenFGA flags plus RiffDB flags.
func NewRunCommand() *cobra.Command {
	defaultConfig := serverconfig.DefaultConfig()
	defaultConfig.Datastore.Engine = defaultEngineRiffDB

	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the OpenFGA server",
		Long:  "Run the OpenFGA server backed by RiffDB.",
		RunE:  runServer,
		Args:  cobra.NoArgs,
	}

	flags := cmd.Flags()
	registerOpenFGARunFlags(flags, defaultConfig)
	registerRiffDBFlags(flags)

	cmd.PreRun = func(command *cobra.Command, args []string) {
		bindRunFlagsFunc(flags)(command, args)
		bindRiffDBFlags(flags)
	}

	return cmd
}

// NewValidateCommand returns the `validate-models` subcommand.
func NewValidateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate-models",
		Short: "Validate all authorization models",
		Long:  "Validate all authorization models across all stores in RiffDB.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			openfgaCfg, err := ReadConfig()
			if err != nil {
				return err
			}
			riffCfg, err := ReadRiffDBConfig(openfgaCfg)
			if err != nil {
				return err
			}
			if openfgaCfg.Datastore.Engine != "" && openfgaCfg.Datastore.Engine != defaultEngineRiffDB {
				return fmt.Errorf("unsupported datastore engine %q: only %q is supported by openfga-riffdb", openfgaCfg.Datastore.Engine, defaultEngineRiffDB)
			}

			log := logger.MustNewLogger(openfgaCfg.Log.Format, "ERROR", openfgaCfg.Log.TimestampFormat)
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}

			session, err := riffdbopenfga.Connect(ctx, riffCfg.ConnectConfig)
			if err != nil {
				return fmt.Errorf("connect to riffdb: %w", err)
			}
			ds, err := riffdbopenfga.NewDatastore(riffdbopenfga.Config{
				Session:         session,
				CommandAttempts: riffCfg.CommandAttempts,
				CloseSession:    true,
			})
			if err != nil {
				_ = session.Close()
				return fmt.Errorf("initialize riffdb datastore: %w", err)
			}
			defer ds.Close()

			results, err := ValidateModels(ctx, log, ds)
			if err != nil {
				return err
			}

			encoded, err := json.Marshal(results)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), string(encoded))
			return nil
		},
	}

	defaultConfig := serverconfig.DefaultConfig()
	defaultConfig.Datastore.Engine = defaultEngineRiffDB

	flags := cmd.Flags()
	flags.String(datastoreEngineFlag, defaultConfig.Datastore.Engine, "the datastore engine that will be used for persistence (must be 'riffdb')")
	flags.String(datastoreURIFlag, defaultConfig.Datastore.URI, "the connection uri to use to connect to the RiffDB unix domain socket")
	registerRiffDBFlags(flags)

	cmd.PreRun = func(_ *cobra.Command, _ []string) {
		mustBindPFlag("datastore.engine", flags.Lookup(datastoreEngineFlag))
		mustBindEnv("datastore.engine", "OPENFGA_DATASTORE_ENGINE")
		mustBindPFlag("datastore.uri", flags.Lookup(datastoreURIFlag))
		mustBindEnv("datastore.uri", "OPENFGA_DATASTORE_URI")
		bindRiffDBFlags(flags)
	}

	return cmd
}

// ValidationResult represents the validation result for a single authorization model.
type ValidationResult struct {
	StoreID       string `json:"store_id"`
	StoreName     string `json:"store_name"`
	ModelID       string `json:"model_id"`
	SchemaVersion string `json:"schema_version"`
	Error         string `json:"error,omitempty"`
	Invalid       bool   `json:"invalid"`
}

// ValidateModels iterates through all stores and their authorization models and validates them against the typesystem.
func ValidateModels(ctx context.Context, _ logger.Logger, ds storage.OpenFGADatastore) ([]ValidationResult, error) {
	results := make([]ValidationResult, 0)
	var storeContinuationToken string

	for {
		stores, nextStoreToken, err := ds.ListStores(ctx, storage.ListStoresOptions{
			Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, storeContinuationToken),
		})
		if err != nil {
			return nil, err
		}

		for _, store := range stores {
			var modelContinuationToken string
			for {
				models, nextModelToken, err := ds.ReadAuthorizationModels(ctx, store.GetId(), storage.ReadAuthorizationModelsOptions{
					Pagination: storage.NewPaginationOptions(storage.DefaultPageSize, modelContinuationToken),
				})
				if err != nil {
					return nil, err
				}

				for _, model := range models {
					res := ValidationResult{
						StoreID:       store.GetId(),
						StoreName:     store.GetName(),
						ModelID:       model.GetId(),
						SchemaVersion: model.GetSchemaVersion(),
					}
					if _, err := typesystem.NewAndValidate(ctx, model); err != nil {
						res.Invalid = true
						res.Error = err.Error()
					}
					results = append(results, res)
				}

				if nextModelToken == "" {
					break
				}
				modelContinuationToken = nextModelToken
			}
		}

		if nextStoreToken == "" {
			break
		}
		storeContinuationToken = nextStoreToken
	}

	return results, nil
}

// ReadConfig reads the OpenFGA server configuration from flags, environment variables, and config.yaml.
func ReadConfig() (*serverconfig.Config, error) {
	cfg := serverconfig.DefaultConfig()
	cfg.Datastore.Engine = defaultEngineRiffDB

	viper.SetTypeByDefaultValue(true)
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	configPaths := []string{"/etc/openfga", "$HOME/.openfga", "."}
	for _, path := range configPaths {
		viper.AddConfigPath(path)
	}
	viper.SetEnvPrefix("OPENFGA")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		var configFileNotFoundErr viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundErr) {
			return nil, fmt.Errorf("failed to load server config: %w", err)
		}
	}

	if err := viper.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal server config: %w", err)
	}

	if cfg.Datastore.Engine == "" {
		cfg.Datastore.Engine = defaultEngineRiffDB
	}

	return cfg, nil
}

// ReadRiffDBConfig resolves the RiffDB transport and session configuration from
// flags, environment variables, and the OpenFGA datastore URI fallback.
func ReadRiffDBConfig(openfgaCfg *serverconfig.Config) (RiffDBConfig, error) {
	connCfg, err := riffdbopenfga.ConnectConfigFromEnv()
	if err != nil {
		return RiffDBConfig{}, err
	}
	cfg := RiffDBConfig{
		ConnectConfig:   connCfg,
		CommandAttempts: 3,
	}

	if v := strings.TrimSpace(viper.GetString("riffdb.socket")); v != "" {
		cfg.SocketPath = riffdbopenfga.NormalizeSocketPath(v)
	}
	if cfg.SocketPath == "" && openfgaCfg != nil && strings.TrimSpace(openfgaCfg.Datastore.URI) != "" {
		cfg.SocketPath = riffdbopenfga.NormalizeSocketPath(openfgaCfg.Datastore.URI)
	}

	if v := strings.TrimSpace(viper.GetString("riffdb.database")); v != "" {
		cfg.Database = v
	}
	if v := strings.TrimSpace(viper.GetString("riffdb.remoteIdentityHash")); v != "" {
		cfg.RemoteIdentityHash = v
	}
	if v := strings.TrimSpace(viper.GetString("riffdb.role")); v != "" {
		cfg.Role = v
	}
	if v := strings.TrimSpace(viper.GetString("riffdb.roleHash")); v != "" {
		cfg.RoleDefinitionHash = v
	}
	if v := strings.TrimSpace(viper.GetString("riffdb.catalogHash")); v != "" {
		cfg.OperationCatalogHash = v
	}
	if v := strings.TrimSpace(viper.GetString("riffdb.manifestHash")); v != "" {
		cfg.ApplicationManifestHash = v
	}
	if v := strings.TrimSpace(viper.GetString("riffdb.bundleHash")); v != "" {
		cfg.ContractBundleHash = v
	}
	if v := strings.TrimSpace(viper.GetString("riffdb.tenant")); v != "" {
		cfg.Tenant = v
	}
	if attempts := viper.GetUint32("riffdb.commandAttempts"); attempts > 0 {
		cfg.CommandAttempts = attempts
	}

	return cfg, nil
}

func runServer(_ *cobra.Command, _ []string) error {
	openfgaCfg, err := ReadConfig()
	if err != nil {
		return err
	}
	riffCfg, err := ReadRiffDBConfig(openfgaCfg)
	if err != nil {
		return err
	}

	log := logger.MustNewLogger(openfgaCfg.Log.Format, openfgaCfg.Log.Level, openfgaCfg.Log.TimestampFormat)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	gCtx, gCancel := context.WithCancel(ctx)
	defer gCancel()

	serverCtx := &ServerContext{Logger: log}
	return serverCtx.Run(gCtx, openfgaCfg, riffCfg)
}

func convertStringArrayToUintArray(stringArray []string) []uint {
	uintArray := []uint{}
	for _, val := range stringArray {
		valInt, err := strconv.Atoi(val)
		if err == nil {
			uintArray = append(uintArray, uint(valInt))
		}
	}
	return uintArray
}

type cleanup func(context.Context) error

func cleanupWithMessage(fn cleanup, msg string) cleanup {
	return func(ctx context.Context) error {
		if err := fn(ctx); err != nil {
			return fmt.Errorf("%s: %w", msg, err)
		}
		return nil
	}
}

func cleanupFromPlainFunc(fn func(), msg string) cleanup {
	return func(ctx context.Context) error {
		done := make(chan struct{})
		go func() {
			fn()
			close(done)
		}()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", msg, ctx.Err())
		}
	}
}

// ServerContext orchestrates the standalone OpenFGA server lifecycle backed by RiffDB.
type ServerContext struct {
	Logger    logger.Logger
	Datastore storage.OpenFGADatastore
}

func (s *ServerContext) verifyConfig(config *serverconfig.Config) error {
	if config.Datastore.Engine != "" && config.Datastore.Engine != defaultEngineRiffDB {
		return fmt.Errorf("unsupported datastore engine %q: only %q is supported by openfga-riffdb", config.Datastore.Engine, defaultEngineRiffDB)
	}
	return config.Verify()
}

// Run starts the OpenFGA gRPC server, HTTP gateway, Playground, Prometheus metrics,
// pprof profiler, and OTLP tracer, and blocks until ctx is cancelled.
func (s *ServerContext) Run(ctx context.Context, config *serverconfig.Config, riffCfg RiffDBConfig) error {
	if s.Logger == nil {
		s.Logger = logger.NewNoopLogger()
	}

	if err := s.verifyConfig(config); err != nil {
		return err
	}

	var cleanups []cleanup

	traceCleanup := configureTracing(config, s.Logger, Version)
	cleanups = append(cleanups, cleanupWithMessage(traceCleanup, "tracer Notice"))

	datastore := s.Datastore
	if datastore == nil {
		session, err := riffdbopenfga.Connect(ctx, riffCfg.ConnectConfig)
		if err != nil {
			return fmt.Errorf("failed to connect to riffdb datastore: %w", err)
		}
		datastore, err = riffdbopenfga.NewDatastore(riffdbopenfga.Config{
			Session:         session,
			CommandAttempts: riffCfg.CommandAttempts,
			CloseSession:    true,
		})
		if err != nil {
			_ = session.Close()
			return fmt.Errorf("failed to initialize riffdb datastore: %w", err)
		}
		cleanups = append(cleanups, cleanupFromPlainFunc(datastore.Close, "datastore"))
	}

	authenticator, err := s.buildAuthenticator(config)
	if err != nil {
		return err
	}
	cleanups = append(cleanups, cleanupFromPlainFunc(authenticator.Close, "authenticator"))

	var metricsCollector *grpcServerMetrics
	if config.Metrics.Enabled {
		metricsCollector = newGRPCServerMetrics(config.Metrics.EnableRPCHistograms)
	}

	grpcHostAddr, grpcHostPort, err := net.SplitHostPort(config.GRPC.Addr)
	if err != nil {
		return fmt.Errorf("`grpc.addr` config must be in the form [host]:port")
	}
	if grpcHostAddr == "" {
		grpcHostAddr = "0.0.0.0"
	}
	grpcBindAddr := net.JoinHostPort(grpcHostAddr, grpcHostPort)

	if config.Profiler.Enabled {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		s.Logger.Info(fmt.Sprintf("📈 starting profiler on '%s'", config.Profiler.Addr))
		profilerServer := &http.Server{
			Addr:        config.Profiler.Addr,
			Handler:     mux,
			ReadTimeout: 3 * time.Second,
		}
		go func() {
			if err := profilerServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.Logger.Fatal("profiler server failed with unexpected error", zap.Error(err))
			}
		}()
		cleanups = append(cleanups, cleanupWithMessage(profilerServer.Shutdown, "profiler server"))
		s.Logger.Info(fmt.Sprintf("📈 profiler listening on '%s'", config.Profiler.Addr))
	}

	if config.Metrics.Enabled {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		s.Logger.Info(fmt.Sprintf("📊 starting prometheus metrics server on '%s'", config.Metrics.Addr))
		metricsServer := &http.Server{
			Addr:        config.Metrics.Addr,
			Handler:     mux,
			ReadTimeout: 3 * time.Second,
		}
		go func() {
			if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.Logger.Fatal("metrics server failed with unexpected error", zap.Error(err))
			}
		}()
		cleanups = append(cleanups, cleanupWithMessage(metricsServer.Shutdown, "metrics server"))
		s.Logger.Info(fmt.Sprintf("📊 prometheus metrics server listening on '%s'", config.Metrics.Addr))
	}

	rpcTransport := gateway.NewRPCTransport(s.Logger)
	openfgaSrv, err := server.NewServerWithOpts(
		server.WithDatastore(datastore),
		server.WithAuthzenBaseURL(config.Authzen.BaseURL),
		server.WithAuthorizationModelCacheSize(config.Datastore.MaxCacheSize),
		server.WithTypesystemCacheSize(config.Datastore.MaxTypesystemCacheSize),
		server.WithLogger(s.Logger),
		server.WithTransport(rpcTransport),
		server.WithResolveNodeLimit(config.ResolveNodeLimit),
		server.WithResolveNodeBreadthLimit(config.ResolveNodeBreadthLimit),
		server.WithChangelogHorizonOffset(config.ChangelogHorizonOffset),
		server.WithReadChangesMaxPageSize(config.ReadChangesMaxPageSize),
		server.WithListObjectsDeadline(config.ListObjectsDeadline),
		server.WithListObjectsMaxResults(config.ListObjectsMaxResults),
		server.WithListObjectsPipelineEnabled(config.ListObjectsPipelineEnabled),
		server.WithListUsersDeadline(config.ListUsersDeadline),
		server.WithListUsersMaxResults(config.ListUsersMaxResults),
		server.WithMaxConcurrentReadsForCheck(config.MaxConcurrentReadsForCheck),
		server.WithMaxConcurrentReadsForListObjects(config.MaxConcurrentReadsForListObjects),
		server.WithMaxConcurrentReadsForListUsers(config.MaxConcurrentReadsForListUsers),
		server.WithMaxAuthorizationModelSizeInBytes(config.MaxAuthorizationModelSizeInBytes),
		server.WithDispatchThrottlingCheckResolverEnabled(config.CheckDispatchThrottling.Enabled),
		server.WithDispatchThrottlingCheckResolverFrequency(config.CheckDispatchThrottling.Frequency),
		server.WithDispatchThrottlingCheckResolverThreshold(config.CheckDispatchThrottling.Threshold),
		server.WithDispatchThrottlingCheckResolverMaxThreshold(config.CheckDispatchThrottling.MaxThreshold),
		server.WithListObjectsDispatchThrottlingEnabled(config.ListObjectsDispatchThrottling.Enabled),
		server.WithListObjectsDispatchThrottlingFrequency(config.ListObjectsDispatchThrottling.Frequency),
		server.WithListObjectsDispatchThrottlingThreshold(config.ListObjectsDispatchThrottling.Threshold),
		server.WithListObjectsDispatchThrottlingMaxThreshold(config.ListObjectsDispatchThrottling.MaxThreshold),
		server.WithListUsersDispatchThrottlingEnabled(config.ListUsersDispatchThrottling.Enabled),
		server.WithListUsersDispatchThrottlingFrequency(config.ListUsersDispatchThrottling.Frequency),
		server.WithListUsersDispatchThrottlingThreshold(config.ListUsersDispatchThrottling.Threshold),
		server.WithListUsersDispatchThrottlingMaxThreshold(config.ListUsersDispatchThrottling.MaxThreshold),
		server.WithTokenEncoder(encoder.NewBase64Encoder()),
		server.WithExperimentals(config.Experimentals...),
		server.WithAccessControlParams(config.AccessControl.Enabled, config.AccessControl.StoreID, config.AccessControl.ModelID, config.Authn.Method),
		server.WithCheckQueryCacheEnabled(config.CheckQueryCache.Enabled),
		server.WithCheckCacheLimit(config.CheckCache.Limit),
		server.WithCheckQueryCacheTTL(config.CheckQueryCache.TTL),
		server.WithCacheControllerEnabled(config.CacheController.Enabled),
		server.WithCacheControllerTTL(config.CacheController.TTL),
		server.WithCheckIteratorCacheEnabled(config.CheckIteratorCache.Enabled),
		server.WithCheckIteratorCacheMaxResults(config.CheckIteratorCache.MaxResults),
		server.WithCheckIteratorCacheTTL(config.CheckIteratorCache.TTL),
		server.WithListObjectsIteratorCacheEnabled(config.ListObjectsIteratorCache.Enabled),
		server.WithListObjectsIteratorCacheMaxResults(config.ListObjectsIteratorCache.MaxResults),
		server.WithListObjectsIteratorCacheTTL(config.ListObjectsIteratorCache.TTL),
		server.WithCacheTTLJitterPercentage(config.CacheTTLJitterPercentage),
		server.WithSharedIteratorEnabled(config.SharedIterator.Enabled),
		server.WithSharedIteratorLimit(config.SharedIterator.Limit),
		server.WithSharedIteratorTTL(config.RequestTimeout+2*time.Second),
		server.WithRequestDurationByQueryHistogramBuckets(convertStringArrayToUintArray(config.RequestDurationDatastoreQueryCountBuckets)),
		server.WithRequestDurationByDispatchCountHistogramBuckets(convertStringArrayToUintArray(config.RequestDurationDispatchCountBuckets)),
		server.WithMaxChecksPerBatchCheck(config.MaxChecksPerBatchCheck),
		server.WithMaxConcurrentChecksPerBatchCheck(config.MaxConcurrentChecksPerBatchCheck),
		server.WithCheckDatabaseThrottle(config.CheckDatastoreThrottle.Threshold, config.CheckDatastoreThrottle.Duration),
		server.WithListObjectsDatabaseThrottle(config.ListObjectsDatastoreThrottle.Threshold, config.ListObjectsDatastoreThrottle.Duration),
		server.WithListUsersDatabaseThrottle(config.ListUsersDatastoreThrottle.Threshold, config.ListUsersDatastoreThrottle.Duration),
		server.WithContext(ctx),
		server.WithContextPropagationToDatastore(config.ContextPropagationToDatastore),
		server.WithRequestTimeout(config.RequestTimeout),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize openfga server: %w", err)
	}
	cleanups = append(cleanups, cleanupFromPlainFunc(openfgaSrv.Close, "openfga server"))

	interceptedSrv := &interceptedServer{
		inner:          openfgaSrv,
		transport:      rpcTransport,
		authenticator:  authenticator,
		metrics:        metricsCollector,
		logger:         s.Logger,
		requestTimeout: config.RequestTimeout,
		traceEnabled:   config.Trace.Enabled,
	}

	gwruntime.DefaultContextTimeout = config.HTTP.UpstreamTimeout
	muxOpts := []gwruntime.ServeMuxOption{
		gwruntime.WithForwardResponseOption(httpmiddleware.HTTPResponseModifier),
		gwruntime.WithErrorHandler(func(c context.Context, _ *gwruntime.ServeMux, _ gwruntime.Marshaler, w http.ResponseWriter, r *http.Request, e error) {
			httpmiddleware.CustomHTTPErrorHandler(c, w, r, toEncodedError(e))
		}),
		gwruntime.WithIncomingHeaderMatcher(func(key string) (string, bool) {
			if strings.EqualFold(key, server.AuthorizationModelIDHeader) {
				return strings.ToLower(key), true
			}
			return gwruntime.DefaultHeaderMatcher(key)
		}),
		gwruntime.WithOutgoingHeaderMatcher(func(s string) (string, bool) { return s, true }),
	}
	gwMux := gwruntime.NewServeMux(muxOpts...)
	if err := openfgav1.RegisterOpenFGAServiceHandlerServer(ctx, gwMux, interceptedSrv); err != nil {
		return err
	}
	if err := authzenv1.RegisterAuthZenServiceHandlerServer(ctx, gwMux, interceptedSrv); err != nil {
		return err
	}
	gatewayHandler := wrapGatewayHTTPHandler(gwMux, interceptedSrv)

	grpcProtocolHandler := newGRPCProtocolHandler(interceptedSrv, config.GRPC.MaxRecvMsgBytes, gatewayHandler)
	grpcHTTPServer := &http.Server{
		Addr:    grpcBindAddr,
		Handler: h2c.NewHandler(grpcProtocolHandler, &http2.Server{}),
	}

	if config.GRPC.TLS.Enabled {
		if config.GRPC.TLS.CertPath == "" || config.GRPC.TLS.KeyPath == "" {
			return errors.New("'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
		}
		getCertificate, closeWatcher, err := watchAndLoadCertificate(ctx, config.GRPC.TLS.CertPath, config.GRPC.TLS.KeyPath, s.Logger)
		if err != nil {
			return err
		}
		cleanups = append(cleanups, cleanupFromPlainFunc(closeWatcher, "grpc certwatcher"))

		grpcHTTPServer.TLSConfig = &tls.Config{
			GetCertificate: getCertificate,
			NextProtos:     []string{"h2", "http/1.1"},
		}
		grpcHTTPServer.Handler = grpcProtocolHandler
	}

	s.Logger.Info(fmt.Sprintf("🚀 starting gRPC server on '%s'...", grpcBindAddr))
	lis, err := net.Listen("tcp", grpcBindAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	go func() {
		var serveErr error
		if config.GRPC.TLS.Enabled {
			serveErr = grpcHTTPServer.ServeTLS(lis, "", "")
		} else {
			serveErr = grpcHTTPServer.Serve(lis)
		}
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			s.Logger.Fatal("failed to start gRPC server", zap.Error(serveErr))
		}
	}()
	cleanups = append(cleanups, cleanupWithMessage(grpcHTTPServer.Shutdown, "grpc server"))
	s.Logger.Info(fmt.Sprintf("🚀 gRPC server listening on '%s'...", lis.Addr().String()))

	var httpServer *http.Server
	if config.HTTP.Enabled {
		var handler http.Handler = gatewayHandler
		if config.Trace.Enabled {
			handler = otelhttp.NewHandler(handler, "grpc-gateway", otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
				return fmt.Sprintf("%s %s", r.Method, r.URL.Path)
			}))
		}

		httpServer = &http.Server{
			Addr: config.HTTP.Addr,
			Handler: recovery.HTTPPanicRecoveryHandler(
				corsMiddleware(config.HTTP.CORSAllowedOrigins, config.HTTP.CORSAllowedHeaders, handler),
				s.Logger,
			),
		}

		if config.HTTP.TLS.Enabled {
			if config.HTTP.TLS.CertPath == "" || config.HTTP.TLS.KeyPath == "" {
				return errors.New("'http.tls.cert' and 'http.tls.key' configs must be set")
			}
			getCertificate, closeWatcher, err := watchAndLoadCertificate(ctx, config.HTTP.TLS.CertPath, config.HTTP.TLS.KeyPath, s.Logger)
			if err != nil {
				return err
			}
			cleanups = append(cleanups, cleanupFromPlainFunc(closeWatcher, "http certwatcher"))

			httpServer.TLSConfig = &tls.Config{
				GetCertificate: getCertificate,
			}
		}

		httpLis, err := net.Listen("tcp", httpServer.Addr)
		if err != nil {
			return fmt.Errorf("failed to listen on %s: %w", httpServer.Addr, err)
		}

		s.Logger.Info(fmt.Sprintf("🚀 starting HTTP server on '%s'...", httpLis.Addr().String()))
		go func() {
			var err error
			if config.HTTP.TLS.Enabled {
				err = httpServer.ServeTLS(httpLis, "", "")
			} else {
				err = httpServer.Serve(httpLis)
			}
			if !errors.Is(err, http.ErrServerClosed) {
				s.Logger.Fatal("HTTP server closed with unexpected error", zap.Error(err))
			}
		}()
		cleanups = append(cleanups, cleanupWithMessage(httpServer.Shutdown, "http server"))
		s.Logger.Info(fmt.Sprintf("🚀 HTTP server listening on '%s'...", httpLis.Addr().String()))
	}

	var playground *http.Server
	if config.Playground.Enabled {
		if !config.HTTP.Enabled {
			return errors.New("HTTP server must be enabled to run the openfga playground")
		}
		if config.Authn.Method != "none" {
			return errors.New("the playground only supports authn method 'none'")
		}

		playgroundAddr := config.Playground.PlaygroundAddr()
		s.Logger.Info(fmt.Sprintf("🛝 starting openfga playground on http://%s/playground", playgroundAddr))

		tmpl, err := template.ParseFS(assets.EmbedPlayground, "playground/index.html")
		if err != nil {
			return fmt.Errorf("failed to parse Playground index.html as Go template: %w", err)
		}

		fileServer := http.FileServer(http.FS(assets.EmbedPlayground))

		mux := http.NewServeMux()
		mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/playground") {
				if r.URL.Path == "/playground" || r.URL.Path == "/playground/index.html" {
					err = tmpl.Execute(w, struct {
						HTTPServerURL      string
						PlaygroundAPIToken string
					}{
						HTTPServerURL:      config.HTTP.Addr,
						PlaygroundAPIToken: "",
					})
					if err != nil {
						w.WriteHeader(http.StatusInternalServerError)
						s.Logger.Error("failed to execute/render the Playground web template", zap.Error(err))
					}
					return
				}
				fileServer.ServeHTTP(w, r)
				return
			}
			http.NotFound(w, r)
		}))

		playground = &http.Server{
			Addr:        playgroundAddr,
			Handler:     mux,
			ReadTimeout: 3 * time.Second,
		}

		playgroundLis, err := net.Listen("tcp", playground.Addr)
		if err != nil {
			return fmt.Errorf("failed to listen on %s: %w", playground.Addr, err)
		}

		go func() {
			if err := playground.Serve(playgroundLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.Logger.Fatal("playground server stopped", zap.Error(err))
			}
		}()
		cleanups = append(cleanups, cleanupWithMessage(playground.Shutdown, "playground server"))
		s.Logger.Info(fmt.Sprintf("🛝 playground listening on http://%s/playground", playgroundLis.Addr().String()))
	}

	s.waitForHealthy(ctx, openfgaSrv)

	<-ctx.Done()
	s.Logger.Info("Termination signal received! Gracefully shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
	defer cancel()

	for i := len(cleanups) - 1; i >= 0; i-- {
		if err := cleanups[i](ctx); err != nil {
			s.Logger.Error("Resource cleanup failed on server shutdown", zap.Error(err))
		}
	}

	s.Logger.Info("Server exited Cleanly.")
	return nil
}

func (s *ServerContext) buildAuthenticator(config *serverconfig.Config) (Authenticator, error) {
	switch config.Authn.Method {
	case "none":
		s.Logger.Warn("authentication is disabled")
		return NoopAuthenticator{}, nil
	case "preshared":
		s.Logger.Info("using 'preshared' authentication")
		if config.Authn.AuthnPresharedKeyConfig == nil {
			return nil, errors.New("missing preshared key config")
		}
		return NewPresharedKeyAuthenticator(config.Authn.Keys)
	case "oidc":
		s.Logger.Info("using 'oidc' authentication")
		if config.Authn.AuthnOIDCConfig == nil {
			return nil, errors.New("missing oidc config")
		}
		return NewRemoteOidcAuthenticator(
			config.Authn.Issuer,
			config.Authn.IssuerAliases,
			config.Authn.Audience,
			config.Authn.Subjects,
			config.Authn.ClientIDClaims,
		)
	default:
		return nil, fmt.Errorf("unsupported authentication method '%v'", config.Authn.Method)
	}
}

func (s *ServerContext) waitForHealthy(ctx context.Context, srv *server.Server) {
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 30 * time.Second

	_ = backoff.Retry(func() error {
		ready, err := srv.IsReady(ctx)
		if err != nil {
			return err
		}
		if !ready {
			return errors.New("not serving")
		}
		return nil
	}, backoff.WithContext(policy, ctx))
}

func watchAndLoadCertificate(ctx context.Context, certPath, keyPath string, log logger.Logger) (func(*tls.ClientHelloInfo) (*tls.Certificate, error), func(), error) {
	initialCert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load TLS certificate: %w", err)
	}

	var mu sync.RWMutex
	currentCert := &initialCert

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize TLS certificate watcher: %w", err)
	}

	_ = watcher.Add(filepath.Dir(certPath))
	_ = watcher.Add(filepath.Dir(keyPath))

	watchCtx, cancel := context.WithCancel(ctx)
	var closeOnce sync.Once
	closeFn := func() {
		closeOnce.Do(func() {
			cancel()
			_ = watcher.Close()
		})
	}

	go func() {
		for {
			select {
			case <-watchCtx.Done():
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Rename) {
					updated, err := tls.LoadX509KeyPair(certPath, keyPath)
					if err != nil {
						log.Warn("failed to reload TLS certificate", zap.Error(err))
						continue
					}
					mu.Lock()
					currentCert = &updated
					mu.Unlock()
				}
			case <-watcher.Errors:
			}
		}
	}()

	getCertificate := func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
		mu.RLock()
		defer mu.RUnlock()
		return currentCert, nil
	}
	return getCertificate, closeFn, nil
}

func mustBindPFlag(key string, flag *pflag.Flag) {
	if err := viper.BindPFlag(key, flag); err != nil {
		panic("failed to bind pflag: " + err.Error())
	}
}

func mustBindEnv(input ...string) {
	if err := viper.BindEnv(input...); err != nil {
		panic("failed to bind env key: " + err.Error())
	}
}

func registerRiffDBFlags(flags *pflag.FlagSet) {
	defaults := DefaultRiffDBConfig()
	flags.String("riffdb-socket", defaults.SocketPath, "the RiffDB unix domain socket path or URI (defaults to RIFFDB_LOOPBACK_SOCKET or --datastore-uri)")
	flags.String("riffdb-database", defaults.Database, "the RiffDB database name")
	flags.String("riffdb-remote-identity-hash", defaults.RemoteIdentityHash, "the expected RiffDB remote identity SHA-256 hash")
	flags.String("riffdb-role", defaults.Role, "the RiffDB role name")
	flags.String("riffdb-role-hash", defaults.RoleDefinitionHash, "the RiffDB role definition SHA-256 hash")
	flags.String("riffdb-catalog-hash", defaults.OperationCatalogHash, "the RiffDB catalog identity SHA-256 hash")
	flags.String("riffdb-manifest-hash", defaults.ApplicationManifestHash, "the RiffDB application manifest SHA-256 hash")
	flags.String("riffdb-bundle-hash", defaults.ContractBundleHash, "the RiffDB contract bundle SHA-256 hash")
	flags.String("riffdb-tenant", defaults.Tenant, "the optional RiffDB tenant scope")
	flags.Uint32("riffdb-command-attempts", defaults.CommandAttempts, "the maximum number of attempts for RiffDB command execution")
}

func bindRiffDBFlags(flags *pflag.FlagSet) {
	mustBindPFlag("riffdb.socket", flags.Lookup("riffdb-socket"))
	mustBindEnv("riffdb.socket", "OPENFGA_RIFFDB_SOCKET", "RIFFDB_DRIVER_SOCKET", "RIFFDB_SOCKET", "RIFFDB_LOOPBACK_SOCKET")

	mustBindPFlag("riffdb.database", flags.Lookup("riffdb-database"))
	mustBindEnv("riffdb.database", "OPENFGA_RIFFDB_DATABASE", "RIFFDB_DATABASE", "RIFFDB_LOOPBACK_DATABASE")

	mustBindPFlag("riffdb.remoteIdentityHash", flags.Lookup("riffdb-remote-identity-hash"))
	mustBindEnv("riffdb.remoteIdentityHash", "OPENFGA_RIFFDB_REMOTE_IDENTITY_HASH", "RIFFDB_REMOTE_IDENTITY_HASH", "RIFFDB_REMOTE_HASH", "RIFFDB_LOOPBACK_REMOTE_HASH")

	mustBindPFlag("riffdb.role", flags.Lookup("riffdb-role"))
	mustBindEnv("riffdb.role", "OPENFGA_RIFFDB_ROLE", "RIFFDB_ROLE", "RIFFDB_LOOPBACK_ROLE")

	mustBindPFlag("riffdb.roleHash", flags.Lookup("riffdb-role-hash"))
	mustBindEnv("riffdb.roleHash", "OPENFGA_RIFFDB_ROLE_HASH", "RIFFDB_ROLE_HASH", "RIFFDB_LOOPBACK_ROLE_HASH")

	mustBindPFlag("riffdb.catalogHash", flags.Lookup("riffdb-catalog-hash"))
	mustBindEnv("riffdb.catalogHash", "OPENFGA_RIFFDB_CATALOG_HASH", "RIFFDB_CATALOG_HASH", "RIFFDB_LOOPBACK_CATALOG_HASH")

	mustBindPFlag("riffdb.manifestHash", flags.Lookup("riffdb-manifest-hash"))
	mustBindEnv("riffdb.manifestHash", "OPENFGA_RIFFDB_MANIFEST_HASH", "RIFFDB_MANIFEST_HASH", "RIFFDB_LOOPBACK_MANIFEST_HASH")

	mustBindPFlag("riffdb.bundleHash", flags.Lookup("riffdb-bundle-hash"))
	mustBindEnv("riffdb.bundleHash", "OPENFGA_RIFFDB_BUNDLE_HASH", "RIFFDB_BUNDLE_HASH", "RIFFDB_LOOPBACK_BUNDLE_HASH")

	mustBindPFlag("riffdb.tenant", flags.Lookup("riffdb-tenant"))
	mustBindEnv("riffdb.tenant", "OPENFGA_RIFFDB_TENANT", "RIFFDB_TENANT")

	mustBindPFlag("riffdb.commandAttempts", flags.Lookup("riffdb-command-attempts"))
	mustBindEnv("riffdb.commandAttempts", "OPENFGA_RIFFDB_COMMAND_ATTEMPTS")
}

func registerOpenFGARunFlags(flags *pflag.FlagSet, defaultConfig *serverconfig.Config) {
	flags.SortFlags = false

	flags.StringSlice("experimentals", defaultConfig.Experimentals, "a list of experimental features to enable")
	flags.Bool("access-control-enabled", defaultConfig.AccessControl.Enabled, "enable access control")
	flags.String("access-control-store-id", defaultConfig.AccessControl.StoreID, "the store ID to use for access control")
	flags.String("access-control-model-id", defaultConfig.AccessControl.ModelID, "the model ID to use for access control")

	flags.String("grpc-addr", defaultConfig.GRPC.Addr, "the host:port address to serve the grpc server on")
	flags.Bool("grpc-tls-enabled", defaultConfig.GRPC.TLS.Enabled, "enable/disable transport layer security (TLS)")
	flags.String("grpc-tls-cert", defaultConfig.GRPC.TLS.CertPath, "the (absolute) file path of the certificate to use for the TLS connection")
	flags.String("grpc-tls-key", defaultConfig.GRPC.TLS.KeyPath, "the (absolute) file path of the TLS key that should be used for the TLS connection")
	flags.Int("grpc-max-recv-msg-bytes", defaultConfig.GRPC.MaxRecvMsgBytes, "the maximum message size in bytes the grpc server can receive")

	flags.Bool("http-enabled", defaultConfig.HTTP.Enabled, "enable/disable the openfga HTTP server")
	flags.String("http-addr", defaultConfig.HTTP.Addr, "the host:port address to serve the HTTP server on")
	flags.Bool("http-tls-enabled", defaultConfig.HTTP.TLS.Enabled, "enable/disable transport layer security (TLS)")
	flags.String("http-tls-cert", defaultConfig.HTTP.TLS.CertPath, "the (absolute) file path of the certificate to use for the TLS connection")
	flags.String("http-tls-key", defaultConfig.HTTP.TLS.KeyPath, "the (absolute) file path of the TLS key that should be used for the TLS connection")
	flags.Duration("http-upstream-timeout", defaultConfig.HTTP.UpstreamTimeout, "the timeout duration for proxying HTTP requests upstream to the grpc endpoint")
	flags.StringSlice("http-cors-allowed-origins", defaultConfig.HTTP.CORSAllowedOrigins, "specify the CORS allowed origins")
	flags.StringSlice("http-cors-allowed-headers", defaultConfig.HTTP.CORSAllowedHeaders, "specify the CORS allowed headers")

	flags.String("authzen-base-url", defaultConfig.Authzen.BaseURL, "the canonical base URL for AuthZEN discovery metadata")

	flags.String("authn-method", defaultConfig.Authn.Method, "the authentication method to use")
	flags.StringSlice("authn-preshared-keys", defaultConfig.Authn.Keys, "one or more preshared keys to use for authentication")
	flags.String("authn-oidc-audience", defaultConfig.Authn.Audience, "the OIDC audience of the tokens being signed by the authorization server")
	flags.String("authn-oidc-issuer", defaultConfig.Authn.Issuer, "the OIDC issuer (authorization server) signing the tokens")
	flags.StringSlice("authn-oidc-issuer-aliases", defaultConfig.Authn.IssuerAliases, "the OIDC issuer DNS aliases that will be accepted as valid")
	flags.StringSlice("authn-oidc-subjects", defaultConfig.Authn.Subjects, "the OIDC subject names that will be accepted as valid")
	flags.StringSlice("authn-oidc-client-id-claims", defaultConfig.Authn.ClientIDClaims, "the ClientID claims that will be used to parse the clientID - configure in order of priority")

	flags.String(datastoreEngineFlag, defaultConfig.Datastore.Engine, "the datastore engine that will be used for persistence (must be 'riffdb')")
	flags.String(datastoreURIFlag, defaultConfig.Datastore.URI, "the connection uri to use to connect to the RiffDB unix domain socket")
	flags.String("datastore-secondary-uri", defaultConfig.Datastore.SecondaryURI, "the secondary connection uri (unused with riffdb)")
	flags.String("datastore-username", defaultConfig.Datastore.Username, "the connection username (unused with riffdb)")
	flags.String("datastore-password", defaultConfig.Datastore.Password, "the connection password (unused with riffdb)")
	flags.String("datastore-secondary-username", defaultConfig.Datastore.SecondaryUsername, "the secondary connection username (unused with riffdb)")
	flags.String("datastore-secondary-password", defaultConfig.Datastore.SecondaryPassword, "the secondary connection password (unused with riffdb)")
	flags.Int("datastore-max-cache-size", defaultConfig.Datastore.MaxCacheSize, "the maximum number of authorization models that will be cached in memory")
	flags.Int("datastore-max-typesystem-cache-size", defaultConfig.Datastore.MaxTypesystemCacheSize, "the maximum number of type system models that will be cached in memory")
	flags.Int("datastore-min-open-conns", defaultConfig.Datastore.MinOpenConns, "the minimum number of open connections to the datastore")
	flags.Int("datastore-max-open-conns", defaultConfig.Datastore.MaxOpenConns, "the maximum number of open connections to the datastore")
	flags.Int("datastore-min-idle-conns", defaultConfig.Datastore.MinIdleConns, "the minimum number of connections to the datastore in the idle connection pool")
	flags.Int("datastore-max-idle-conns", defaultConfig.Datastore.MaxIdleConns, "the maximum number of connections to the datastore in the idle connection pool")
	flags.Duration("datastore-conn-max-idle-time", defaultConfig.Datastore.ConnMaxIdleTime, "the maximum amount of time a connection to the datastore may be idle")
	flags.Duration("datastore-conn-max-lifetime", defaultConfig.Datastore.ConnMaxLifetime, "the maximum amount of time a connection to the datastore may be reused")
	flags.Bool("datastore-metrics-enabled", defaultConfig.Datastore.Metrics.Enabled, "enable/disable datastore metrics")

	flags.Bool("playground-enabled", defaultConfig.Playground.Enabled, "enable/disable the OpenFGA Playground")
	flags.Int("playground-port", defaultConfig.Playground.Port, "the port to serve the local OpenFGA Playground on")
	_ = flags.MarkDeprecated("playground-port", "use `playground-addr` instead")
	flags.String("playground-addr", defaultConfig.Playground.Addr, "the host:port address to serve the local OpenFGA Playground on")

	flags.Bool("profiler-enabled", defaultConfig.Profiler.Enabled, "enable/disable pprof profiling")
	flags.String("profiler-addr", defaultConfig.Profiler.Addr, "the host:port address to serve the pprof profiler server on")

	flags.String("log-format", defaultConfig.Log.Format, "the log format to output logs in")
	flags.String("log-level", defaultConfig.Log.Level, "the log level to use for logging")
	flags.String("log-timestamp-format", defaultConfig.Log.TimestampFormat, "the timestamp format to use for logged messages")

	flags.Bool("trace-enabled", defaultConfig.Trace.Enabled, "enable tracing")
	flags.String("trace-otlp-endpoint", defaultConfig.Trace.OTLP.Endpoint, "the endpoint of the trace collector")
	flags.Bool("trace-otlp-tls-enabled", defaultConfig.Trace.OTLP.TLS.Enabled, "enable TLS connection for trace collector")
	flags.Float64("trace-sample-ratio", defaultConfig.Trace.SampleRatio, "the fraction of traces to sample")
	flags.String("trace-sampler", defaultConfig.Trace.Sampler, "the sampler to use for tracing (ratio, always_on, always_off, parentbased_always_on, parentbased_always_off, parentbased_traceidratio)")
	flags.String("trace-service-name", defaultConfig.Trace.ServiceName, "the service name included in sampled traces")
	flags.String("trace-resource-attributes", defaultConfig.Trace.ResourceAttributes, "key-value pairs to be used as resource attributes")

	flags.Bool("metrics-enabled", defaultConfig.Metrics.Enabled, "enable/disable prometheus metrics on the '/metrics' endpoint")
	flags.String("metrics-addr", defaultConfig.Metrics.Addr, "the host:port address to serve the prometheus metrics server on")
	flags.Bool("metrics-enable-rpc-histograms", defaultConfig.Metrics.EnableRPCHistograms, "enables prometheus histogram metrics for RPC latency distributions")

	flags.Int("max-tuples-per-write", defaultConfig.MaxTuplesPerWrite, "the maximum allowed number of tuples per Write transaction")
	flags.Int("max-types-per-authorization-model", defaultConfig.MaxTypesPerAuthorizationModel, "the maximum allowed number of type definitions per authorization model")
	flags.Int("max-authorization-model-size-in-bytes", defaultConfig.MaxAuthorizationModelSizeInBytes, "the maximum size in bytes allowed for persisting an Authorization Model.")
	flags.Uint32("max-concurrent-reads-for-list-objects", defaultConfig.MaxConcurrentReadsForListObjects, "the maximum allowed number of concurrent datastore reads in a single ListObjects or StreamedListObjects query")
	flags.Uint32("max-concurrent-reads-for-check", defaultConfig.MaxConcurrentReadsForCheck, "the maximum allowed number of concurrent datastore reads in a single Check query")
	flags.Uint32("max-concurrent-reads-for-list-users", defaultConfig.MaxConcurrentReadsForListUsers, "the maximum allowed number of concurrent datastore reads in a single ListUsers query")
	flags.Uint64("max-condition-evaluation-cost", defaultConfig.MaxConditionEvaluationCost, "the maximum cost for CEL condition evaluation before a request returns an error")
	flags.Int("changelog-horizon-offset", defaultConfig.ChangelogHorizonOffset, "the offset (in minutes) from the current time. Changes that occur after this offset will not be included in the response of ReadChanges")
	flags.Uint32("resolve-node-limit", defaultConfig.ResolveNodeLimit, "maximum resolution depth to attempt before throwing an error (defines how deeply nested an authorization model can be)")
	flags.Uint32("resolve-node-breadth-limit", defaultConfig.ResolveNodeBreadthLimit, "defines how many nodes on a given level can be evaluated concurrently in a query resolution tree")
	flags.Duration("listObjects-deadline", defaultConfig.ListObjectsDeadline, "the timeout deadline for serving ListObjects and StreamedListObjects requests")
	flags.Uint32("listObjects-max-results", defaultConfig.ListObjectsMaxResults, "the maximum results to return in non-streaming ListObjects API responses. If 0, all results can be returned")
	flags.Bool("listObjects-pipeline-enabled", defaultConfig.ListObjectsPipelineEnabled, "enable list objects pipeline optimization")
	flags.Duration("listUsers-deadline", defaultConfig.ListUsersDeadline, "the timeout deadline for serving ListUsers requests")
	flags.Uint32("listUsers-max-results", defaultConfig.ListUsersMaxResults, "the maximum results to return in ListUsers API responses. If 0, all results can be returned")
	flags.Uint32("readChanges-max-page-size", defaultConfig.ReadChangesMaxPageSize, "the maximum page size allowed in ReadChanges API requests")

	flags.Uint32("check-cache-limit", defaultConfig.CheckCache.Limit, "if caching of check and list objects is enabled, this is the size limit of the cache")

	flags.Bool("cache-controller-enabled", defaultConfig.CacheController.Enabled, "enable check cache controller that invalidates cache based on recent writes")
	flags.Duration("cache-controller-ttl", defaultConfig.CacheController.TTL, "the TTL for each cache controller execution to check whether there are recent writes to invalidate cache")

	flags.Uint32("cache-ttl-jitter-percentage", defaultConfig.CacheTTLJitterPercentage, "percentage of TTL to add as random jitter to cache entries to prevent thundering herd (0-100)")

	flags.Bool("check-iterator-cache-enabled", defaultConfig.CheckIteratorCache.Enabled, "enable caching of datastore iterators")
	flags.Uint32("check-iterator-cache-max-results", defaultConfig.CheckIteratorCache.MaxResults, "if caching of check iterators is enabled, this is the max result limit to cache")
	flags.Duration("check-iterator-cache-ttl", defaultConfig.CheckIteratorCache.TTL, "if caching of check iterators is enabled, this is the TTL of each value")

	flags.Bool("check-query-cache-enabled", defaultConfig.CheckQueryCache.Enabled, "enable caching of Check results")
	flags.Uint32("check-query-cache-limit", defaultConfig.CheckCache.Limit, "if caching of check and list objects is enabled, this is the size limit of the cache")
	_ = flags.MarkDeprecated("check-query-cache-limit", "use `check-cache-limit` instead, which applies to both query and iterator caches")
	flags.Duration("check-query-cache-ttl", defaultConfig.CheckQueryCache.TTL, "if caching of check and list objects is enabled, this is the TTL of each value")

	flags.Bool("list-objects-iterator-cache-enabled", defaultConfig.ListObjectsIteratorCache.Enabled, "enable caching of list objects datastore iterators")
	flags.Uint32("list-objects-iterator-cache-max-results", defaultConfig.ListObjectsIteratorCache.MaxResults, "if caching of list objects iterators is enabled, this is the max result limit to cache")
	flags.Duration("list-objects-iterator-cache-ttl", defaultConfig.ListObjectsIteratorCache.TTL, "if caching of list objects iterators is enabled, this is the TTL of each value")

	flags.Bool("shared-iterator-enabled", defaultConfig.SharedIterator.Enabled, "enable shared datastore iterator across concurrent requests")
	flags.Uint32("shared-iterator-limit", defaultConfig.SharedIterator.Limit, "if shared iterator is enabled, this is the max limit of active iterators")

	flags.StringSlice("request-duration-datastore-query-count-buckets", defaultConfig.RequestDurationDatastoreQueryCountBuckets, "datastore query count buckets used in labelling request_duration_ms.")
	flags.StringSlice("request-duration-dispatch-count-buckets", defaultConfig.RequestDurationDispatchCountBuckets, "dispatch count buckets used in labelling request_duration_ms.")

	flags.Bool("check-dispatch-throttling-enabled", defaultConfig.CheckDispatchThrottling.Enabled, "enable throttling when a Check request's dispatch count is high")
	flags.Duration("check-dispatch-throttling-frequency", defaultConfig.CheckDispatchThrottling.Frequency, "defines how frequent dispatch throttling will be evaluated for Check requests")
	flags.Uint32("check-dispatch-throttling-threshold", defaultConfig.CheckDispatchThrottling.Threshold, "define the number of dispatches to be throttled for Check requests")
	flags.Uint32("check-dispatch-throttling-max-threshold", defaultConfig.CheckDispatchThrottling.MaxThreshold, "define the maximum dispatch threshold values allowed for Check requests")

	flags.Bool("listObjects-dispatch-throttling-enabled", defaultConfig.ListObjectsDispatchThrottling.Enabled, "enable throttling when a ListObjects request's dispatch count is high")
	flags.Duration("listObjects-dispatch-throttling-frequency", defaultConfig.ListObjectsDispatchThrottling.Frequency, "defines how frequent dispatch throttling will be evaluated for ListObjects requests")
	flags.Uint32("listObjects-dispatch-throttling-threshold", defaultConfig.ListObjectsDispatchThrottling.Threshold, "define the number of dispatches to be throttled for ListObjects requests")
	flags.Uint32("listObjects-dispatch-throttling-max-threshold", defaultConfig.ListObjectsDispatchThrottling.MaxThreshold, "define the maximum dispatch threshold values allowed for ListObjects requests")

	flags.Bool("listUsers-dispatch-throttling-enabled", defaultConfig.ListUsersDispatchThrottling.Enabled, "enable throttling when a ListUsers request's dispatch count is high")
	flags.Duration("listUsers-dispatch-throttling-frequency", defaultConfig.ListUsersDispatchThrottling.Frequency, "defines how frequent dispatch throttling will be evaluated for ListUsers requests")
	flags.Uint32("listUsers-dispatch-throttling-threshold", defaultConfig.ListUsersDispatchThrottling.Threshold, "define the number of dispatches to be throttled for ListUsers requests")
	flags.Uint32("listUsers-dispatch-throttling-max-threshold", defaultConfig.ListUsersDispatchThrottling.MaxThreshold, "define the maximum dispatch threshold values allowed for ListUsers requests")

	flags.Uint32("max-checks-per-batch-check", defaultConfig.MaxChecksPerBatchCheck, "define the maximum number of checks allowed in a single BatchCheck request")
	flags.Uint32("max-concurrent-checks-per-batch-check", defaultConfig.MaxConcurrentChecksPerBatchCheck, "define the maximum number of checks that may be evaluated concurrently in a single BatchCheck request")

	flags.Int("check-datastore-throttle-threshold", defaultConfig.CheckDatastoreThrottle.Threshold, "define the number of datastore queries in a Check request before it will be throttled")
	flags.Duration("check-datastore-throttle-duration", defaultConfig.CheckDatastoreThrottle.Duration, "define the duration wait time once a Check request's datastore queries exceeds the threshold")
	flags.Int("listObjects-datastore-throttle-threshold", defaultConfig.ListObjectsDatastoreThrottle.Threshold, "define the number of datastore queries in a ListObjects request before it will be throttled")
	flags.Duration("listObjects-datastore-throttle-duration", defaultConfig.ListObjectsDatastoreThrottle.Duration, "define the duration wait time once a ListObjects request's datastore queries exceeds the threshold")
	flags.Int("listUsers-datastore-throttle-threshold", defaultConfig.ListUsersDatastoreThrottle.Threshold, "define the number of datastore queries in a ListUsers request before it will be throttled")
	flags.Duration("listUsers-datastore-throttle-duration", defaultConfig.ListUsersDatastoreThrottle.Duration, "define the duration wait time once a ListUsers request's datastore queries exceeds the threshold")

	flags.Bool("context-propagation-to-datastore", defaultConfig.ContextPropagationToDatastore, "enable propagation of API request context to the datastore")

	flags.Duration("request-timeout", defaultConfig.RequestTimeout, "configures request timeout. Look at https://openfga.dev/docs/getting-started/setup-openfga/configuration#HOW_TO_CONFIGURE_TIMEOUTS for details")
	flags.Duration("shutdown-timeout", defaultConfig.ShutdownTimeout, "the timeout duration for graceful shutdown of the server")

	flags.Duration("planner-eviction-threshold", defaultConfig.Planner.EvictionThreshold, "duration since last update before a key is evicted from the planner (0 disables eviction)")
	flags.Duration("planner-cleanup-interval", defaultConfig.Planner.CleanupInterval, "interval between planner eviction sweeps (0 disables cleanup)")
}

func bindRunFlagsFunc(flags *pflag.FlagSet) func(*cobra.Command, []string) {
	return func(command *cobra.Command, _ []string) {
		mustBindPFlag("experimentals", flags.Lookup("experimentals"))
		mustBindEnv("experimentals", "OPENFGA_EXPERIMENTALS")

		mustBindPFlag("accessControl.enabled", flags.Lookup("access-control-enabled"))
		mustBindEnv("accessControl.enabled", "OPENFGA_ACCESS_CONTROL_ENABLED")

		mustBindPFlag("accessControl.storeId", flags.Lookup("access-control-store-id"))
		mustBindEnv("accessControl.storeId", "OPENFGA_ACCESS_CONTROL_STORE_ID")

		mustBindPFlag("accessControl.modelId", flags.Lookup("access-control-model-id"))
		mustBindEnv("accessControl.modelId", "OPENFGA_ACCESS_CONTROL_MODEL_ID")

		command.MarkFlagsRequiredTogether("access-control-enabled", "access-control-store-id", "access-control-model-id")

		mustBindPFlag("grpc.addr", flags.Lookup("grpc-addr"))
		mustBindEnv("grpc.addr", "OPENFGA_GRPC_ADDR")

		mustBindPFlag("grpc.tls.enabled", flags.Lookup("grpc-tls-enabled"))
		mustBindEnv("grpc.tls.enabled", "OPENFGA_GRPC_TLS_ENABLED")

		mustBindPFlag("grpc.tls.cert", flags.Lookup("grpc-tls-cert"))
		mustBindEnv("grpc.tls.cert", "OPENFGA_GRPC_TLS_CERT")

		mustBindPFlag("grpc.tls.key", flags.Lookup("grpc-tls-key"))
		mustBindEnv("grpc.tls.key", "OPENFGA_GRPC_TLS_KEY")

		command.MarkFlagsRequiredTogether("grpc-tls-enabled", "grpc-tls-cert", "grpc-tls-key")

		mustBindPFlag("grpc.maxRecvMsgBytes", flags.Lookup("grpc-max-recv-msg-bytes"))
		mustBindEnv("grpc.maxRecvMsgBytes", "OPENFGA_GRPC_MAX_RECV_MSG_BYTES")

		mustBindPFlag("http.enabled", flags.Lookup("http-enabled"))
		mustBindEnv("http.enabled", "OPENFGA_HTTP_ENABLED")

		mustBindPFlag("http.addr", flags.Lookup("http-addr"))
		mustBindEnv("http.addr", "OPENFGA_HTTP_ADDR")

		mustBindPFlag("http.tls.enabled", flags.Lookup("http-tls-enabled"))
		mustBindEnv("http.tls.enabled", "OPENFGA_HTTP_TLS_ENABLED")

		mustBindPFlag("http.tls.cert", flags.Lookup("http-tls-cert"))
		mustBindEnv("http.tls.cert", "OPENFGA_HTTP_TLS_CERT")

		mustBindPFlag("http.tls.key", flags.Lookup("http-tls-key"))
		mustBindEnv("http.tls.key", "OPENFGA_HTTP_TLS_KEY")

		command.MarkFlagsRequiredTogether("http-tls-enabled", "http-tls-cert", "http-tls-key")

		mustBindPFlag("http.upstreamTimeout", flags.Lookup("http-upstream-timeout"))
		mustBindEnv("http.upstreamTimeout", "OPENFGA_HTTP_UPSTREAM_TIMEOUT", "OPENFGA_HTTP_UPSTREAMTIMEOUT")

		mustBindPFlag("http.corsAllowedOrigins", flags.Lookup("http-cors-allowed-origins"))
		mustBindEnv("http.corsAllowedOrigins", "OPENFGA_HTTP_CORS_ALLOWED_ORIGINS", "OPENFGA_HTTP_CORSALLOWEDORIGINS")

		mustBindPFlag("http.corsAllowedHeaders", flags.Lookup("http-cors-allowed-headers"))
		mustBindEnv("http.corsAllowedHeaders", "OPENFGA_HTTP_CORS_ALLOWED_HEADERS", "OPENFGA_HTTP_CORSALLOWEDHEADERS")

		mustBindPFlag("authzen.baseURL", flags.Lookup("authzen-base-url"))
		mustBindEnv("authzen.baseURL", "OPENFGA_AUTHZEN_BASE_URL")

		mustBindPFlag("authn.method", flags.Lookup("authn-method"))
		mustBindEnv("authn.method", "OPENFGA_AUTHN_METHOD")

		mustBindPFlag("authn.preshared.keys", flags.Lookup("authn-preshared-keys"))
		mustBindEnv("authn.preshared.keys", "OPENFGA_AUTHN_PRESHARED_KEYS")

		mustBindPFlag("authn.oidc.audience", flags.Lookup("authn-oidc-audience"))
		mustBindEnv("authn.oidc.audience", "OPENFGA_AUTHN_OIDC_AUDIENCE")

		mustBindPFlag("authn.oidc.issuer", flags.Lookup("authn-oidc-issuer"))
		mustBindEnv("authn.oidc.issuer", "OPENFGA_AUTHN_OIDC_ISSUER")

		mustBindPFlag("authn.oidc.issuerAliases", flags.Lookup("authn-oidc-issuer-aliases"))
		mustBindEnv("authn.oidc.issuerAliases", "OPENFGA_AUTHN_OIDC_ISSUER_ALIASES")

		mustBindPFlag("authn.oidc.subjects", flags.Lookup("authn-oidc-subjects"))
		mustBindEnv("authn.oidc.subjects", "OPENFGA_AUTHN_OIDC_SUBJECTS")

		mustBindPFlag("authn.oidc.clientIdClaims", flags.Lookup("authn-oidc-client-id-claims"))
		mustBindEnv("authn.oidc.clientIdClaims", "OPENFGA_AUTHN_OIDC_CLIENT_ID_CLAIMS")

		mustBindPFlag("datastore.engine", flags.Lookup("datastore-engine"))
		mustBindEnv("datastore.engine", "OPENFGA_DATASTORE_ENGINE")

		mustBindPFlag("datastore.uri", flags.Lookup("datastore-uri"))
		mustBindEnv("datastore.uri", "OPENFGA_DATASTORE_URI")

		mustBindPFlag("datastore.secondaryUri", flags.Lookup("datastore-secondary-uri"))
		mustBindEnv("datastore.secondaryUri", "OPENFGA_DATASTORE_SECONDARY_URI")

		mustBindPFlag("datastore.secondaryUsername", flags.Lookup("datastore-secondary-username"))
		mustBindEnv("datastore.secondaryUsername", "OPENFGA_DATASTORE_SECONDARY_USERNAME")

		mustBindPFlag("datastore.secondaryPassword", flags.Lookup("datastore-secondary-password"))
		mustBindEnv("datastore.secondaryPassword", "OPENFGA_DATASTORE_SECONDARY_PASSWORD")

		mustBindPFlag("datastore.username", flags.Lookup("datastore-username"))
		mustBindEnv("datastore.username", "OPENFGA_DATASTORE_USERNAME")

		mustBindPFlag("datastore.password", flags.Lookup("datastore-password"))
		mustBindEnv("datastore.password", "OPENFGA_DATASTORE_PASSWORD")

		mustBindPFlag("datastore.maxCacheSize", flags.Lookup("datastore-max-cache-size"))
		mustBindEnv("datastore.maxCacheSize", "OPENFGA_DATASTORE_MAX_CACHE_SIZE", "OPENFGA_DATASTORE_MAXCACHESIZE")

		mustBindPFlag("datastore.maxTypesystemCacheSize", flags.Lookup("datastore-max-typesystem-cache-size"))
		mustBindEnv("datastore.maxTypesystemCacheSize", "OPENFGA_DATASTORE_MAX_TYPESYSTEM_CACHE_SIZE", "OPENFGA_DATASTORE_MAXTYPESYSTEMCACHESIZE")

		mustBindPFlag("datastore.minOpenConns", flags.Lookup("datastore-min-open-conns"))
		mustBindEnv("datastore.minOpenConns", "OPENFGA_DATASTORE_MIN_OPEN_CONNS")

		mustBindPFlag("datastore.maxOpenConns", flags.Lookup("datastore-max-open-conns"))
		mustBindEnv("datastore.maxOpenConns", "OPENFGA_DATASTORE_MAX_OPEN_CONNS", "OPENFGA_DATASTORE_MAXOPENCONNS")

		mustBindPFlag("datastore.minIdleConns", flags.Lookup("datastore-min-idle-conns"))
		mustBindEnv("datastore.minIdleConns", "OPENFGA_DATASTORE_MIN_IDLE_CONNS")

		mustBindPFlag("datastore.maxIdleConns", flags.Lookup("datastore-max-idle-conns"))
		mustBindEnv("datastore.maxIdleConns", "OPENFGA_DATASTORE_MAX_IDLE_CONNS", "OPENFGA_DATASTORE_MAXIDLECONNS")

		mustBindPFlag("datastore.connMaxIdleTime", flags.Lookup("datastore-conn-max-idle-time"))
		mustBindEnv("datastore.connMaxIdleTime", "OPENFGA_DATASTORE_CONN_MAX_IDLE_TIME", "OPENFGA_DATASTORE_CONNMAXIDLETIME")

		mustBindPFlag("datastore.connMaxLifetime", flags.Lookup("datastore-conn-max-lifetime"))
		mustBindEnv("datastore.connMaxLifetime", "OPENFGA_DATASTORE_CONN_MAX_LIFETIME", "OPENFGA_DATASTORE_CONNMAXLIFETIME")

		mustBindPFlag("datastore.metrics.enabled", flags.Lookup("datastore-metrics-enabled"))
		mustBindEnv("datastore.metrics.enabled", "OPENFGA_DATASTORE_METRICS_ENABLED")

		mustBindPFlag("playground.enabled", flags.Lookup("playground-enabled"))
		mustBindEnv("playground.enabled", "OPENFGA_PLAYGROUND_ENABLED")

		mustBindPFlag("playground.port", flags.Lookup("playground-port"))
		mustBindEnv("playground.port", "OPENFGA_PLAYGROUND_PORT")

		mustBindPFlag("playground.addr", flags.Lookup("playground-addr"))
		mustBindEnv("playground.addr", "OPENFGA_PLAYGROUND_ADDR")

		mustBindPFlag("profiler.enabled", flags.Lookup("profiler-enabled"))
		mustBindEnv("profiler.enabled", "OPENFGA_PROFILER_ENABLED")

		mustBindPFlag("profiler.addr", flags.Lookup("profiler-addr"))
		mustBindEnv("profiler.addr", "OPENFGA_PROFILER_ADDRESS")

		mustBindPFlag("log.format", flags.Lookup("log-format"))
		mustBindEnv("log.format", "OPENFGA_LOG_FORMAT")

		mustBindPFlag("log.level", flags.Lookup("log-level"))
		mustBindEnv("log.level", "OPENFGA_LOG_LEVEL")

		mustBindPFlag("log.timestampFormat", flags.Lookup("log-timestamp-format"))
		mustBindEnv("log.timestampFormat", "OPENFGA_LOG_TIMESTAMP_FORMAT")

		mustBindPFlag("trace.enabled", flags.Lookup("trace-enabled"))
		mustBindEnv("trace.enabled", "OPENFGA_TRACE_ENABLED")

		mustBindPFlag("trace.otlp.endpoint", flags.Lookup("trace-otlp-endpoint"))
		mustBindEnv("trace.otlp.endpoint", "OPENFGA_TRACE_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT")

		mustBindPFlag("trace.otlp.tls.enabled", flags.Lookup("trace-otlp-tls-enabled"))
		mustBindEnv("trace.otlp.tls.enabled", "OPENFGA_TRACE_OTLP_TLS_ENABLED")

		mustBindPFlag("trace.sampleRatio", flags.Lookup("trace-sample-ratio"))
		mustBindEnv("trace.sampleRatio", "OPENFGA_TRACE_SAMPLE_RATIO", "OTEL_TRACES_SAMPLER_ARG")

		mustBindPFlag("trace.sampler", flags.Lookup("trace-sampler"))
		mustBindEnv("trace.sampler", "OPENFGA_TRACE_SAMPLER", "OTEL_TRACES_SAMPLER")

		mustBindPFlag("trace.serviceName", flags.Lookup("trace-service-name"))
		mustBindEnv("trace.serviceName", "OPENFGA_TRACE_SERVICE_NAME", "OTEL_SERVICE_NAME")

		mustBindPFlag("trace.resourceAttributes", flags.Lookup("trace-resource-attributes"))
		mustBindEnv("trace.resourceAttributes", "OTEL_RESOURCE_ATTRIBUTES")

		mustBindPFlag("metrics.enabled", flags.Lookup("metrics-enabled"))
		mustBindEnv("metrics.enabled", "OPENFGA_METRICS_ENABLED")

		mustBindPFlag("metrics.addr", flags.Lookup("metrics-addr"))
		mustBindEnv("metrics.addr", "OPENFGA_METRICS_ADDR")

		mustBindPFlag("metrics.enableRPCHistograms", flags.Lookup("metrics-enable-rpc-histograms"))
		mustBindEnv("metrics.enableRPCHistograms", "OPENFGA_METRICS_ENABLE_RPC_HISTOGRAMS")

		mustBindPFlag("maxChecksPerBatchCheck", flags.Lookup("max-checks-per-batch-check"))
		mustBindEnv("maxChecksPerBatchCheck", "OPENFGA_MAX_CHECKS_PER_BATCH_CHECK")

		mustBindPFlag("maxConcurrentChecksPerBatchCheck", flags.Lookup("max-concurrent-checks-per-batch-check"))
		mustBindEnv("maxConcurrentChecksPerBatchCheck", "OPENFGA_MAX_CONCURRENT_CHECKS_PER_BATCH_CHECK")

		mustBindPFlag("maxTuplesPerWrite", flags.Lookup("max-tuples-per-write"))
		mustBindEnv("maxTuplesPerWrite", "OPENFGA_MAX_TUPLES_PER_WRITE", "OPENFGA_MAXTUPLESPERWRITE")

		mustBindPFlag("maxTypesPerAuthorizationModel", flags.Lookup("max-types-per-authorization-model"))
		mustBindEnv("maxTypesPerAuthorizationModel", "OPENFGA_MAX_TYPES_PER_AUTHORIZATION_MODEL", "OPENFGA_MAXTYPESPERAUTHORIZATIONMODEL")

		mustBindPFlag("maxAuthorizationModelSizeInBytes", flags.Lookup("max-authorization-model-size-in-bytes"))
		mustBindEnv("maxAuthorizationModelSizeInBytes", "OPENFGA_MAX_AUTHORIZATION_MODEL_SIZE_IN_BYTES", "OPENFGA_MAXAUTHORIZATIONMODELSIZEINBYTES")

		mustBindPFlag("maxConcurrentReadsForListObjects", flags.Lookup("max-concurrent-reads-for-list-objects"))
		mustBindEnv("maxConcurrentReadsForListObjects", "OPENFGA_MAX_CONCURRENT_READS_FOR_LIST_OBJECTS", "OPENFGA_MAXCONCURRENTREADSFORLISTOBJECTS")

		mustBindPFlag("maxConcurrentReadsForListUsers", flags.Lookup("max-concurrent-reads-for-list-users"))
		mustBindEnv("maxConcurrentReadsForListUsers", "OPENFGA_MAX_CONCURRENT_READS_FOR_LIST_USERS", "OPENFGA_MAXCONCURRENTREADSFORLISTUSERS")

		mustBindPFlag("maxConcurrentReadsForCheck", flags.Lookup("max-concurrent-reads-for-check"))
		mustBindEnv("maxConcurrentReadsForCheck", "OPENFGA_MAX_CONCURRENT_READS_FOR_CHECK", "OPENFGA_MAXCONCURRENTREADSFORCHECK")

		mustBindPFlag("maxConditionEvaluationCost", flags.Lookup("max-condition-evaluation-cost"))
		mustBindEnv("maxConditionEvaluationCost", "OPENFGA_MAX_CONDITION_EVALUATION_COST", "OPENFGA_MAXCONDITIONEVALUATIONCOST")

		mustBindPFlag("changelogHorizonOffset", flags.Lookup("changelog-horizon-offset"))
		mustBindEnv("changelogHorizonOffset", "OPENFGA_CHANGELOG_HORIZON_OFFSET", "OPENFGA_CHANGELOGHORIZONOFFSET")

		mustBindPFlag("resolveNodeLimit", flags.Lookup("resolve-node-limit"))
		mustBindEnv("resolveNodeLimit", "OPENFGA_RESOLVE_NODE_LIMIT", "OPENFGA_RESOLVENODELIMIT")

		mustBindPFlag("resolveNodeBreadthLimit", flags.Lookup("resolve-node-breadth-limit"))
		mustBindEnv("resolveNodeBreadthLimit", "OPENFGA_RESOLVE_NODE_BREADTH_LIMIT", "OPENFGA_RESOLVENODEBREADTHLIMIT")

		mustBindPFlag("listObjectsDeadline", flags.Lookup("listObjects-deadline"))
		mustBindEnv("listObjectsDeadline", "OPENFGA_LIST_OBJECTS_DEADLINE", "OPENFGA_LISTOBJECTSDEADLINE")

		mustBindPFlag("listObjectsMaxResults", flags.Lookup("listObjects-max-results"))
		mustBindEnv("listObjectsMaxResults", "OPENFGA_LIST_OBJECTS_MAX_RESULTS", "OPENFGA_LISTOBJECTSMAXRESULTS")

		mustBindPFlag("listObjectsPipelineEnabled", flags.Lookup("listObjects-pipeline-enabled"))
		mustBindEnv("listObjectsPipelineEnabled", "OPENFGA_LIST_OBJECTS_PIPELINE_ENABLED", "OPENFGA_LISTOBJECTSPIPELINEENABLED")

		mustBindPFlag("listUsersDeadline", flags.Lookup("listUsers-deadline"))
		mustBindEnv("listUsersDeadline", "OPENFGA_LIST_USERS_DEADLINE", "OPENFGA_LISTUSERSDEADLINE")

		mustBindPFlag("listUsersMaxResults", flags.Lookup("listUsers-max-results"))
		mustBindEnv("listUsersMaxResults", "OPENFGA_LIST_USERS_MAX_RESULTS", "OPENFGA_LISTUSERSMAXRESULTS")

		mustBindPFlag("readChangesMaxPageSize", flags.Lookup("readChanges-max-page-size"))
		mustBindEnv("readChangesMaxPageSize", "OPENFGA_READ_CHANGES_MAX_PAGE_SIZE", "OPENFGA_READCHANGESMAXPAGESIZE")

		mustBindPFlag("checkCache.limit", flags.Lookup("check-cache-limit"))
		mustBindEnv("checkCache.limit", "OPENFGA_CHECK_CACHE_LIMIT")

		mustBindPFlag("cache.limit", flags.Lookup("check-query-cache-limit"))
		mustBindEnv("cache.limit", "OPENFGA_CHECK_QUERY_CACHE_LIMIT")

		mustBindPFlag("cacheController.enabled", flags.Lookup("cache-controller-enabled"))
		mustBindEnv("cacheController.enabled", "OPENFGA_CACHE_CONTROLLER_ENABLED")

		mustBindPFlag("cacheController.ttl", flags.Lookup("cache-controller-ttl"))
		mustBindEnv("cacheController.ttl", "OPENFGA_CACHE_CONTROLLER_TTL")

		mustBindPFlag("cacheTTLJitterPercentage", flags.Lookup("cache-ttl-jitter-percentage"))
		mustBindEnv("cacheTTLJitterPercentage", "OPENFGA_CACHE_TTL_JITTER_PERCENTAGE")

		mustBindPFlag("checkIteratorCache.enabled", flags.Lookup("check-iterator-cache-enabled"))
		mustBindEnv("checkIteratorCache.enabled", "OPENFGA_CHECK_ITERATOR_CACHE_ENABLED")

		mustBindPFlag("checkIteratorCache.maxResults", flags.Lookup("check-iterator-cache-max-results"))
		mustBindEnv("checkIteratorCache.maxResults", "OPENFGA_CHECK_ITERATOR_CACHE_MAX_RESULTS")

		mustBindPFlag("checkIteratorCache.ttl", flags.Lookup("check-iterator-cache-ttl"))
		mustBindEnv("checkIteratorCache.ttl", "OPENFGA_CHECK_ITERATOR_CACHE_TTL")

		mustBindPFlag("checkQueryCache.enabled", flags.Lookup("check-query-cache-enabled"))
		mustBindEnv("checkQueryCache.enabled", "OPENFGA_CHECK_QUERY_CACHE_ENABLED")

		mustBindPFlag("checkQueryCache.ttl", flags.Lookup("check-query-cache-ttl"))
		mustBindEnv("checkQueryCache.ttl", "OPENFGA_CHECK_QUERY_CACHE_TTL")

		mustBindPFlag("listObjectsIteratorCache.enabled", flags.Lookup("list-objects-iterator-cache-enabled"))
		mustBindEnv("listObjectsIteratorCache.enabled", "OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_ENABLED")

		mustBindPFlag("listObjectsIteratorCache.maxResults", flags.Lookup("list-objects-iterator-cache-max-results"))
		mustBindEnv("listObjectsIteratorCache.maxResults", "OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_MAX_RESULTS")

		mustBindPFlag("listObjectsIteratorCache.ttl", flags.Lookup("list-objects-iterator-cache-ttl"))
		mustBindEnv("listObjectsIteratorCache.ttl", "OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_TTL")

		mustBindPFlag("sharedIterator.enabled", flags.Lookup("shared-iterator-enabled"))
		mustBindEnv("sharedIterator.enabled", "OPENFGA_SHARED_ITERATOR_ENABLED")

		mustBindPFlag("sharedIterator.limit", flags.Lookup("shared-iterator-limit"))
		mustBindEnv("sharedIterator.limit", "OPENFGA_SHARED_ITERATOR_LIMIT")

		mustBindPFlag("requestDurationDatastoreQueryCountBuckets", flags.Lookup("request-duration-datastore-query-count-buckets"))
		mustBindEnv("requestDurationDatastoreQueryCountBuckets", "OPENFGA_REQUEST_DURATION_DATASTORE_QUERY_COUNT_BUCKETS")

		mustBindPFlag("requestDurationDispatchCountBuckets", flags.Lookup("request-duration-dispatch-count-buckets"))
		mustBindEnv("requestDurationDispatchCountBuckets", "OPENFGA_REQUEST_DURATION_DISPATCH_COUNT_BUCKETS")

		mustBindPFlag("contextPropagationToDatastore", flags.Lookup("context-propagation-to-datastore"))
		mustBindEnv("contextPropagationToDatastore", "OPENFGA_CONTEXT_PROPAGATION_TO_DATASTORE")

		mustBindPFlag("checkDispatchThrottling.enabled", flags.Lookup("check-dispatch-throttling-enabled"))
		mustBindEnv("checkDispatchThrottling.enabled", "OPENFGA_CHECK_DISPATCH_THROTTLING_ENABLED")

		mustBindPFlag("checkDispatchThrottling.frequency", flags.Lookup("check-dispatch-throttling-frequency"))
		mustBindEnv("checkDispatchThrottling.frequency", "OPENFGA_CHECK_DISPATCH_THROTTLING_FREQUENCY")

		mustBindPFlag("checkDispatchThrottling.threshold", flags.Lookup("check-dispatch-throttling-threshold"))
		mustBindEnv("checkDispatchThrottling.threshold", "OPENFGA_CHECK_DISPATCH_THROTTLING_THRESHOLD")

		mustBindPFlag("checkDispatchThrottling.maxThreshold", flags.Lookup("check-dispatch-throttling-max-threshold"))
		mustBindEnv("checkDispatchThrottling.maxThreshold", "OPENFGA_CHECK_DISPATCH_THROTTLING_MAX_THRESHOLD")

		mustBindPFlag("listObjectsDispatchThrottling.enabled", flags.Lookup("listObjects-dispatch-throttling-enabled"))
		mustBindEnv("listObjectsDispatchThrottling.enabled", "OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_ENABLED")

		mustBindPFlag("listObjectsDispatchThrottling.frequency", flags.Lookup("listObjects-dispatch-throttling-frequency"))
		mustBindEnv("listObjectsDispatchThrottling.frequency", "OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_FREQUENCY")

		mustBindPFlag("listObjectsDispatchThrottling.threshold", flags.Lookup("listObjects-dispatch-throttling-threshold"))
		mustBindEnv("listObjectsDispatchThrottling.threshold", "OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_THRESHOLD")

		mustBindPFlag("listObjectsDispatchThrottling.maxThreshold", flags.Lookup("listObjects-dispatch-throttling-max-threshold"))
		mustBindEnv("listObjectsDispatchThrottling.maxThreshold", "OPENFGA_LIST_OBJECTS_DISPATCH_THROTTLING_MAX_THRESHOLD")

		mustBindPFlag("listUsersDispatchThrottling.enabled", flags.Lookup("listUsers-dispatch-throttling-enabled"))
		mustBindEnv("listUsersDispatchThrottling.enabled", "OPENFGA_LIST_USERS_DISPATCH_THROTTLING_ENABLED")

		mustBindPFlag("listUsersDispatchThrottling.frequency", flags.Lookup("listUsers-dispatch-throttling-frequency"))
		mustBindEnv("listUsersDispatchThrottling.frequency", "OPENFGA_LIST_USERS_DISPATCH_THROTTLING_FREQUENCY")

		mustBindPFlag("listUsersDispatchThrottling.threshold", flags.Lookup("listUsers-dispatch-throttling-threshold"))
		mustBindEnv("listUsersDispatchThrottling.threshold", "OPENFGA_LIST_USERS_DISPATCH_THROTTLING_THRESHOLD")

		mustBindPFlag("listUsersDispatchThrottling.maxThreshold", flags.Lookup("listUsers-dispatch-throttling-max-threshold"))
		mustBindEnv("listUsersDispatchThrottling.maxThreshold", "OPENFGA_LIST_USERS_DISPATCH_THROTTLING_MAX_THRESHOLD")

		mustBindPFlag("checkDatastoreThrottle.threshold", flags.Lookup("check-datastore-throttle-threshold"))
		mustBindEnv("checkDatastoreThrottle.threshold", "OPENFGA_CHECK_DATASTORE_THROTTLE_THRESHOLD")

		mustBindPFlag("checkDatastoreThrottle.duration", flags.Lookup("check-datastore-throttle-duration"))
		mustBindEnv("checkDatastoreThrottle.duration", "OPENFGA_CHECK_DATASTORE_THROTTLE_DURATION")

		mustBindPFlag("listObjectsDatastoreThrottle.threshold", flags.Lookup("listObjects-datastore-throttle-threshold"))
		mustBindEnv("listObjectsDatastoreThrottle.threshold", "OPENFGA_LIST_OBJECTS_DATASTORE_THROTTLE_THRESHOLD")

		mustBindPFlag("listObjectsDatastoreThrottle.duration", flags.Lookup("listObjects-datastore-throttle-duration"))
		mustBindEnv("listObjectsDatastoreThrottle.duration", "OPENFGA_LIST_OBJECTS_DATASTORE_THROTTLE_DURATION")

		mustBindPFlag("listUsersDatastoreThrottle.threshold", flags.Lookup("listUsers-datastore-throttle-threshold"))
		mustBindEnv("listUsersDatastoreThrottle.threshold", "OPENFGA_LIST_USERS_DATASTORE_THROTTLE_THRESHOLD")

		mustBindPFlag("listUsersDatastoreThrottle.duration", flags.Lookup("listUsers-datastore-throttle-duration"))
		mustBindEnv("listUsersDatastoreThrottle.duration", "OPENFGA_LIST_USERS_DATASTORE_THROTTLE_DURATION")

		mustBindPFlag("requestTimeout", flags.Lookup("request-timeout"))
		mustBindEnv("requestTimeout", "OPENFGA_REQUEST_TIMEOUT")

		mustBindPFlag("shutdownTimeout", flags.Lookup("shutdown-timeout"))
		mustBindEnv("shutdownTimeout", "OPENFGA_SHUTDOWN_TIMEOUT")

		mustBindPFlag("planner.evictionThreshold", flags.Lookup("planner-eviction-threshold"))
		mustBindEnv("planner.evictionThreshold", "OPENFGA_PLANNER_EVICTION_THRESHOLD")
		mustBindPFlag("planner.cleanupInterval", flags.Lookup("planner-cleanup-interval"))
		mustBindEnv("planner.cleanupInterval", "OPENFGA_PLANNER_CLEANUP_INTERVAL")
	}
}

// Ensure unused imports are not left around.
var (
	_ = io.EOF
	_ = strconv.IntSize
	_ = otel.GetTracerProvider
)
