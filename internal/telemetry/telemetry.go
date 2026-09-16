// Package telemetry installs the OpenTelemetry SDK pipeline (ТЗ 14.3,
// recommended by the specification). It is the only place the SDK and an
// exporter are linked: the mesh code itself uses internal/tracing, which depends
// on the OTel API alone.
//
// Tracing is off by default. With Enabled=false Setup constructs nothing, no
// exporter dials anywhere, and the global providers stay the OTel API's no-ops —
// so a node that never mentions telemetry pays nothing for it.
//
// The exporter is OTLP over HTTP (otlptracehttp), chosen over gRPC because it
// pulls in no additional transport stack beyond what protobuf already provides
// and because every common collector (Jaeger, Tempo, OTel Collector itself)
// speaks it on 4318.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

// DefaultServiceName is reported to the collector unless configured otherwise.
const DefaultServiceName = "zeptomesh-node"

// Config mirrors config.TracingConfig without importing it: the config package
// stays dependency-light, and the node adapts the two shapes where they meet.
type Config struct {
	Enabled bool
	// Endpoint is a "host:port" pair for OTLP over HTTP, e.g.
	// "localhost:4318". Empty means: let the exporter read the standard
	// OTEL_EXPORTER_OTLP_ENDPOINT environment variables.
	Endpoint string
	// Insecure sends plaintext HTTP. With a scheme-less endpoint this is what a
	// local collector (Jaeger, Tempo) usually expects.
	Insecure bool
	// SampleRatio is the head-based probability for traces this node starts.
	// A trace that arrives already sampled stays sampled (ParentBased), so a
	// ratio below 1 does not truncate multi-node traces mid-flight.
	SampleRatio float64
	ServiceName string
	Environment string
	// ShutdownTimeout bounds the final flush. Zero means 5s.
	ShutdownTimeout time.Duration
}

// Setup installs the global tracer provider and text-map propagator. It returns
// a shutdown function that must always be called (a working no-op when tracing
// is disabled), and the reason tracing was not configured.
func Setup(ctx context.Context, cfg Config, logger *slog.Logger) (func(context.Context) error, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := validate(cfg); err != nil {
		return func(context.Context) error { return nil }, err
	}

	name := strings.TrimSpace(cfg.ServiceName)
	if name == "" {
		name = DefaultServiceName
	}
	attrs := []attribute.KeyValue{attribute.String("service.name", name)}
	if env := strings.TrimSpace(cfg.Environment); env != "" {
		attrs = append(attrs, attribute.String("deployment.environment.name", env))
	}
	// Merge with resource.Default() (host/process/SDK identity): a collector
	// needs that to attribute spans to a machine, and re-declaring it here
	// would only duplicate what the SDK already provides.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(attrs...))
	if err != nil {
		return func(context.Context) error { return nil }, fmt.Errorf("telemetry: resource: %w", err)
	}

	var opts []otlptracehttp.Option
	if ep := strings.TrimSpace(cfg.Endpoint); ep != "" {
		opts = append(opts, otlptracehttp.WithEndpoint(ep))
		if cfg.Insecure {
			opts = append(opts, otlptracehttp.WithInsecure())
		}
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return func(context.Context) error { return nil }, fmt.Errorf("telemetry: otlp exporter: %w", err)
	}

	ratio := cfg.SampleRatio
	if ratio <= 0 || ratio > 1 {
		ratio = 1
	}
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithResource(res),
		tracesdk.WithSampler(tracesdk.ParentBased(tracesdk.TraceIDRatioBased(ratio))),
		tracesdk.WithSpanProcessor(tracesdk.NewBatchSpanProcessor(exp)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	logger.Info("tracing_enabled", "service", name, "endpoint", displayEndpoint(cfg),
		"sample_ratio", ratio)

	timeout := cfg.ShutdownTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	shutdown := func(shutdownCtx context.Context) error {
		// Bound the flush: a collector that stopped answering must not be able to
		// hold the node's shutdown open.
		cctx, cancel := context.WithTimeout(shutdownCtx, timeout)
		defer cancel()
		if err := tp.Shutdown(cctx); err != nil {
			return fmt.Errorf("telemetry: shutdown: %w", err)
		}
		return nil
	}
	return shutdown, nil
}

// validate rejects the configurations that would otherwise fail later, at the
// first export attempt, where the error message names neither the field nor the
// reason an operator could act on.
func validate(cfg Config) error {
	if cfg.SampleRatio < 0 || cfg.SampleRatio > 1 {
		return fmt.Errorf("telemetry: tracing.sample_ratio %v outside 0..1", cfg.SampleRatio)
	}
	ep := strings.TrimSpace(cfg.Endpoint)
	if ep != "" && (strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") ||
		strings.ContainsAny(ep, " \t/")) || strings.HasSuffix(ep, ":") {
		return fmt.Errorf("telemetry: tracing.endpoint %q must be host:port — no scheme, no path, no spaces (use tracing.insecure for plaintext)", cfg.Endpoint)
	}
	return nil
}

func displayEndpoint(cfg Config) string {
	ep := strings.TrimSpace(cfg.Endpoint)
	if ep == "" {
		return "env(OTEL_EXPORTER_OTLP_ENDPOINT)"
	}
	if cfg.Insecure {
		return "http://" + ep
	}
	return "https://" + ep
}
