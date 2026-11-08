package openfgarun

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// ParseOTLPEndpoint strips an http:// or https:// scheme from an OTLP endpoint
// string and reports whether the scheme implies TLS.
func ParseOTLPEndpoint(endpoint string) (string, bool) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return endpoint, false
	}
	switch u.Scheme {
	case "https":
		return u.Host, true
	case "http":
		return u.Host, false
	default:
		return endpoint, false
	}
}

// ResolveOTLPSecurity returns true if TLS should be used for OTLP export.
func ResolveOTLPSecurity(configSecure, schemeSecure bool) bool {
	return configSecure || schemeSecure
}

// ResolveSampler maps an OpenFGA/OTel sampler name and ratio to an sdktrace.Sampler.
func ResolveSampler(name string, ratio float64) sdktrace.Sampler {
	switch strings.ToLower(name) {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(ratio)
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
	default:
		return sdktrace.TraceIDRatioBased(ratio)
	}
}

func configureTracing(config *serverconfig.Config, log logger.Logger, serviceVersion string) func(context.Context) error {
	if !config.Trace.Enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }
	}

	endpoint, schemeSecure := ParseOTLPEndpoint(config.Trace.OTLP.Endpoint)
	effectiveTLS := ResolveOTLPSecurity(config.Trace.OTLP.TLS.Enabled, schemeSecure)
	resolvedSampler := ResolveSampler(config.Trace.Sampler, config.Trace.SampleRatio)
	log.Info(fmt.Sprintf(
		"🕵 tracing enabled: sampler is '%s' (resolved to '%s'), sampling ratio is %v, sending traces to '%s', tls: %t",
		config.Trace.Sampler,
		resolvedSampler.Description(),
		config.Trace.SampleRatio,
		endpoint,
		effectiveTLS,
	))

	attrs := []attribute.KeyValue{
		attribute.String("service.name", config.Trace.ServiceName),
		attribute.String("service.version", serviceVersion),
	}
	for _, pair := range strings.Split(config.Trace.ResourceAttributes, ",") {
		pair = strings.TrimSpace(pair)
		if key, val, ok := strings.Cut(pair, "="); ok && key != "" {
			attrs = append(attrs, attribute.String(strings.TrimSpace(key), strings.TrimSpace(val)))
		}
	}

	baseRes, err := resource.Merge(resource.Default(), resource.NewSchemaless(attrs...))
	if err != nil {
		panic(err)
	}
	res, err := resource.Merge(baseRes, resource.Environment())
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	options := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(endpoint),
	}
	if !effectiveTLS {
		options = append(options, otlptracegrpc.WithInsecure())
	}

	exp, err := otlptracegrpc.New(ctx, options...)
	if err != nil {
		panic(fmt.Sprintf("failed to establish a connection with the otlp exporter: %v", err))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(resolvedSampler),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exp)),
	)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	otel.SetTracerProvider(tp)

	return func(shutdownCtx context.Context) error {
		return errors.Join(tp.ForceFlush(shutdownCtx), tp.Shutdown(shutdownCtx))
	}
}

// grpcServerMetrics implements the standard grpc_prometheus server metrics
// collector used by OpenFGA's /metrics endpoint.
type grpcServerMetrics struct {
	started         *prometheus.CounterVec
	handled         *prometheus.CounterVec
	msgReceived     *prometheus.CounterVec
	msgSent         *prometheus.CounterVec
	handlingSeconds *prometheus.HistogramVec
}

func newGRPCServerMetrics(enableHistograms bool) *grpcServerMetrics {
	m := &grpcServerMetrics{
		started: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_server_started_total",
			Help: "Total number of RPCs started on the server.",
		}, []string{"grpc_type", "grpc_service", "grpc_method"}),
		handled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_server_handled_total",
			Help: "Total number of RPCs completed on the server, regardless of success or failure.",
		}, []string{"grpc_type", "grpc_service", "grpc_method", "grpc_code"}),
		msgReceived: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_server_msg_received_total",
			Help: "Total number of RPC stream messages received on the server.",
		}, []string{"grpc_type", "grpc_service", "grpc_method"}),
		msgSent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "grpc_server_msg_sent_total",
			Help: "Total number of gRPC stream messages sent by the server.",
		}, []string{"grpc_type", "grpc_service", "grpc_method"}),
	}
	if enableHistograms {
		m.handlingSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "grpc_server_handling_seconds",
			Help:    "Histogram of response latency (seconds) of gRPC that had been application-level handled by the server.",
			Buckets: prometheus.DefBuckets,
		}, []string{"grpc_type", "grpc_service", "grpc_method"})
	}
	if err := prometheus.Register(m); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			prometheus.Unregister(alreadyRegistered.ExistingCollector)
			_ = prometheus.Register(m)
		}
	}
	return m
}

func (m *grpcServerMetrics) Describe(ch chan<- *prometheus.Desc) {
	m.started.Describe(ch)
	m.handled.Describe(ch)
	m.msgReceived.Describe(ch)
	m.msgSent.Describe(ch)
	if m.handlingSeconds != nil {
		m.handlingSeconds.Describe(ch)
	}
}

func (m *grpcServerMetrics) Collect(ch chan<- prometheus.Metric) {
	m.started.Collect(ch)
	m.handled.Collect(ch)
	m.msgReceived.Collect(ch)
	m.msgSent.Collect(ch)
	if m.handlingSeconds != nil {
		m.handlingSeconds.Collect(ch)
	}
}

func (m *grpcServerMetrics) observeRPC(grpcType, service, method, code string, duration time.Duration, sentCount int) {
	if m == nil {
		return
	}
	m.started.WithLabelValues(grpcType, service, method).Inc()
	m.msgReceived.WithLabelValues(grpcType, service, method).Inc()
	for range sentCount {
		m.msgSent.WithLabelValues(grpcType, service, method).Inc()
	}
	m.handled.WithLabelValues(grpcType, service, method, code).Inc()
	if m.handlingSeconds != nil {
		m.handlingSeconds.WithLabelValues(grpcType, service, method).Observe(duration.Seconds())
	}
}

// corsMiddleware implements the CORS policy configured via
// --http-cors-allowed-origins and --http-cors-allowed-headers.
func corsMiddleware(allowedOrigins, allowedHeaders []string, next http.Handler) http.Handler {
	allowAllOrigins := slices.Contains(allowedOrigins, "*")
	allowAllHeaders := slices.Contains(allowedHeaders, "*")
	methodsHeader := strings.Join([]string{
		http.MethodGet,
		http.MethodPost,
		http.MethodHead,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodPut,
	}, ", ")
	headersHeader := strings.Join(allowedHeaders, ", ")

	originAllowed := func(origin string) bool {
		if origin == "" {
			return false
		}
		if allowAllOrigins {
			return true
		}
		for _, allowed := range allowedOrigins {
			if strings.EqualFold(allowed, origin) {
				return true
			}
		}
		return false
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if originAllowed(origin) {
			if allowAllOrigins {
				w.Header().Set("Access-Control-Allow-Origin", "*")
			} else {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Add("Vary", "Origin")
			}
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				w.Header().Set("Access-Control-Allow-Methods", methodsHeader)
				if allowAllHeaders {
					if reqHeaders := r.Header.Get("Access-Control-Request-Headers"); reqHeaders != "" {
						w.Header().Set("Access-Control-Allow-Headers", reqHeaders)
					}
				} else if headersHeader != "" {
					w.Header().Set("Access-Control-Allow-Headers", headersHeader)
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
