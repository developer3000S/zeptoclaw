// Package tracing holds the OpenTelemetry pieces the mesh itself uses: span
// names, attributes, and the transport of a trace across node boundaries.
//
// It deliberately depends only on the OTel API (already in the module graph via
// libp2p), never on the SDK or an exporter. The SDK pipeline is installed by
// internal/telemetry at startup; where that is not linked or tracing is off,
// the global providers stay OTel's no-ops and every call here costs nothing.
//
// Trace context crosses a hop through the task envelope's labels. Labels are
// covered by the content digest and the author signature (wire.TaskContent
// encodes them), so a relay cannot rewrite the trace context any more than it
// can rewrite the instruction — and a peer that predates this change simply
// carries two harmless extra label keys. That is why the trace continues on the
// executing node instead of restarting there, and why no protocol field was
// needed (ТЗ 14.3 asks for task_id as the cross-cutting identifier; OpenTelemetry
// is only recommended).
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// Scope is the instrumentation library name reported to the collector.
const Scope = "zeptomesh"

// Span names. One trace per task: submit on the origin, receive on every hop,
// then either forward (delegation) or execute (this node runs it). plan is the
// origin's decomposition step and the parent of each subtask's onward spans.
const (
	SpanSubmit  = "task.submit"
	SpanReceive = "task.receive"
	SpanForward = "task.forward"
	SpanExecute = "task.execute"
	SpanPlan    = "task.plan"
)

// Tracer hands out spans for one mesh subsystem (name: "tasks", "node", …).
func Tracer(name string) trace.Tracer { return otel.Tracer(Scope + "." + name) }

// TaskAttrs returns the identifying attributes every task span carries. The
// task_id is the cross-cutting key from ТЗ 14.3: a log line, a metric label and
// a span attribute can all be joined on it.
func TaskAttrs(env *pb.TaskEnvelope) []attribute.KeyValue {
	if env == nil {
		return nil
	}
	attrs := []attribute.KeyValue{attribute.String("zeptomesh.task_id", env.GetTaskId())}
	if o := env.GetOriginPeerId(); o != "" {
		attrs = append(attrs, attribute.String("zeptomesh.origin_peer_id", o))
	}
	if s := env.GetSenderPeerId(); s != "" {
		attrs = append(attrs, attribute.String("zeptomesh.sender_peer_id", s))
	}
	if sk := env.GetRequiredSkills(); len(sk) > 0 {
		attrs = append(attrs, attribute.StringSlice("zeptomesh.required_skills", sk))
	}
	return attrs
}

// Inject writes the current span's W3C trace context into a task's labels.
// It is a no-op when the context carries no recording span (tracing off, or not
// sampled at the origin): an unsampled task propagates no headers, which is how
// head-based sampling is meant to behave. The caller must pass a map it owns —
// this writes into it.
func Inject(ctx context.Context, labels map[string]string) {
	if labels == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(labels))
}

// Extract continues a remote task's trace here by pulling traceparent/tracestate
// back out of the envelope labels. When none are present — tracing was off at
// the origin, or an older peer submitted the task — the caller's context comes
// back unchanged and spans start a fresh trace: that trace is missing its
// upstream leg rather than being wrong.
func Extract(ctx context.Context, env *pb.TaskEnvelope) context.Context {
	labels := env.GetPayload().GetLabels()
	if labels == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(labels))
}
