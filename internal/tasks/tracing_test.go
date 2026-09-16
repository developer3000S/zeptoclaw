package tasks

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/developer3000S/zeptoclaw/internal/tracing"
)

// installRecorder points the OTel globals at an in-memory span recorder for
// the duration of one test: the manager takes its tracer from the global
// provider (as does every node in production through internal/telemetry), so
// this is the same code path, only wired to a collector that keeps spans in a
// slice instead of exporting them.
func installRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	sr := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})
	return sr
}

func endedNamed(sr *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

func spanAttr(s sdktrace.ReadOnlySpan, key attribute.Key) (string, bool) {
	for _, a := range s.Attributes() {
		if a.Key == key {
			return a.Value.AsString(), true
		}
	}
	return "", false
}

// awaitEnded waits for want spans of the given name. It is not test slop: the
// result reaches the waiter inside runLocal while the deferred span End()s are
// still unwinding, so a test that read the recorder right after AwaitResult
// would race the very instrumentation it checks.
func awaitEnded(t *testing.T, sr *tracetest.SpanRecorder, name string, want int) []sdktrace.ReadOnlySpan {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := endedNamed(sr, name)
		if len(got) >= want {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %d %q span(s) ended, want %d (all: %v)",
				len(got), name, want, names(sr.Ended()))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func names(spans []sdktrace.ReadOnlySpan) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name())
	}
	return out
}

// TestSubmitSpansOneTraceOnOneNode is the ТЗ 14.3 in-process shape: Submit
// opens task.submit, local execution opens task.execute under it, and both
// carry the cross-cutting task id.
func TestSubmitSpansOneTraceOnOneNode(t *testing.T) {
	sr := installRecorder(t)
	m := newLocalManager(t)

	id, err := m.Submit(context.Background(), SubmitRequest{Instruction: "trace me", TTL: 3})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := m.AwaitResult(context.Background(), id); err != nil {
		t.Fatalf("await: %v", err)
	}

	submits := awaitEnded(t, sr, tracing.SpanSubmit, 1)
	executes := awaitEnded(t, sr, tracing.SpanExecute, 1)
	sub, exec := submits[0], executes[0]
	if sub.SpanContext().TraceID() != exec.SpanContext().TraceID() {
		t.Fatalf("execute is in another trace: %s vs %s", exec.SpanContext().TraceID(), sub.SpanContext().TraceID())
	}
	if exec.Parent().SpanID() != sub.SpanContext().SpanID() {
		t.Fatalf("execute parent = %s, want the submit span %s", exec.Parent().SpanID(), sub.SpanContext().SpanID())
	}
	for _, s := range []sdktrace.ReadOnlySpan{sub, exec} {
		if got, ok := spanAttr(s, attribute.Key("zeptomesh.task_id")); !ok || got != id {
			t.Fatalf("%s task_id attr = %q (present %v), want %q", s.Name(), got, ok, id)
		}
	}
	if got, ok := spanAttr(exec, attribute.Key("zeptomesh.worker_peer_id")); !ok || got != m.self.String() {
		t.Fatalf("execute worker attr = %q (present %v), want %s", got, ok, m.self)
	}
}

// TestTraceContextRidesTheSignedLabels is the hop-crossing half: the stamp
// survives a digest recomputation and a Validate (i.e. it can be signed and
// sent), and Extract on the receiving side continues the sender's trace.
// Without the re-stamped digest every receiving node would reject the task as
// tampered — this is exactly the invariant Submit/injectChild maintain.
func TestTraceContextRidesTheSignedLabels(t *testing.T) {
	installRecorder(t)
	ctx, root := otel.Tracer("test").Start(context.Background(), "root")
	defer root.End()

	labels := map[string]string{"source": "trigger"}
	env := Build("peer-a", "", BuildRequest{
		Instruction: "x",
		Labels:      labels,
	}, time.Now().UTC())

	injectTraceContext(ctx, env)
	if _, injected := env.Payload.Labels["traceparent"]; !injected {
		t.Fatal("traceparent was not injected")
	}
	// Build shares the caller's map with the envelope; the injector must copy
	// rather than write through, or a Submit caller's own labels would mutate.
	if _, leaked := labels["traceparent"]; leaked {
		t.Fatal("injectTraceContext mutated the caller's label map")
	}
	env.ContextDigest = ContentDigest(env)
	if err := Validate(env, 1<<20, time.Now().UTC()); err != nil {
		t.Fatalf("an envelope carrying trace context must validate: %v", err)
	}

	// A receiving hop rebuilds the sender's span context from the labels.
	rcvx := tracing.Extract(context.Background(), env)
	sc := trace.SpanContextFromContext(rcvx)
	if !sc.IsValid() || sc.TraceID() != root.SpanContext().TraceID() {
		t.Fatalf("extracted trace %v, want the root's %s", sc.TraceID(), root.SpanContext().TraceID())
	}
	if sc.SpanID() != root.SpanContext().SpanID() {
		t.Fatalf("extracted parent span %v, want the root's %s", sc.SpanID(), root.SpanContext().SpanID())
	}
}

// TestTraceLabelsAreDigestProtected proves the security property the design
// rests on: labels are author-signed content, so a relay that edits them
// breaks the content digest. That is what makes riding the trace in labels
// safe.
func TestTraceLabelsAreDigestProtected(t *testing.T) {
	installRecorder(t)
	env := Build("peer-a", "t1", BuildRequest{Instruction: "x"}, time.Now().UTC())
	injectTraceContext(context.Background(), env)
	env.ContextDigest = ContentDigest(env)

	env.Payload.Labels["traceparent"] = "00-00000000000000000000000000000000-0000000000000000-01"
	if err := VerifyDigest(env); err == nil {
		t.Fatal("a rewritten traceparent must invalidate the content digest")
	}
}
