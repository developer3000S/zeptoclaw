package telemetry

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestSetupDisabledTouchesNothing is the ТЗ 14.3 cost boundary: a node with
// tracing off must not replace the global providers, and its shutdown must be
// callable and harmless — main.go always calls it.
func TestSetupDisabledTouchesNothing(t *testing.T) {
	before := otel.GetTracerProvider()
	shutdown, err := Setup(context.Background(), Config{Enabled: false, Endpoint: "anywhere:4318"}, discardLogger())
	if err != nil {
		t.Fatalf("disabled Setup must not error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown must always be returned")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("disabled shutdown: %v", err)
	}
	if otel.GetTracerProvider() != before {
		t.Fatal("a disabled setup replaced the global tracer provider")
	}
}

// TestSetupValidatesConfig rejects at startup what would otherwise fail at the
// first export, where the message names neither field nor fix.
func TestSetupValidatesConfig(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{"scheme in endpoint", Config{Enabled: true, Endpoint: "http://localhost:4318"}, "host:port"},
		{"path in endpoint", Config{Enabled: true, Endpoint: "localhost:4318/v1/traces"}, "host:port"},
		{"ratio too high", Config{Enabled: true, SampleRatio: 1.5}, "sample_ratio"},
		{"ratio negative", Config{Enabled: true, SampleRatio: -0.1}, "sample_ratio"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shutdown, err := Setup(context.Background(), tc.cfg, discardLogger())
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q must mention %q", err, tc.want)
			}
			if shutdown == nil {
				t.Fatal("shutdown must be returned even on failure (main always calls it)")
			}
			if err := shutdown(context.Background()); err != nil {
				t.Fatalf("failed-setup shutdown: %v", err)
			}
		})
	}
}

// TestSetupEnabledInstallsProviderAndShutsDownCleanly covers the enabled path
// against no collector at all: the OTLP/HTTP exporter dials lazily, so Setup
// must succeed and the final flush must return within its bound instead of
// hanging on an endpoint nobody listens on.
func TestSetupEnabledInstallsProviderAndShutsDownCleanly(t *testing.T) {
	prev := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(prev) })

	shutdown, err := Setup(context.Background(), Config{
		Enabled: true, Endpoint: "127.0.0.1:1", SampleRatio: 1,
		ServiceName: "test-node", Environment: "ci", ShutdownTimeout: 2 * time.Second,
	}, discardLogger())
	if err != nil {
		t.Fatalf("setup: %v", err)
	}
	if otel.GetTracerProvider() == prev {
		t.Fatal("the enabled path must install a new global provider")
	}
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Fatalf("global provider = %T, want the SDK provider", otel.GetTracerProvider())
	}
	// A span goes through the processor into the (unreachable) exporter: the
	// point is that shutdown completes anyway, bounded.
	_, span := otel.Tracer("telemetry-test").Start(context.Background(), "probe")
	span.End()

	done := make(chan error, 1)
	go func() { done <- shutdown(context.Background()) }()
	select {
	case err := <-done:
		// An unreachable collector makes the flush report an export error; the
		// requirement here is bounded completion, not silence.
		t.Logf("shutdown returned: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("shutdown did not complete within its bound — an unreachable collector held it open")
	}
}
