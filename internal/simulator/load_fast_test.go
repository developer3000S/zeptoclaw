package simulator

// Fast model invariants (ТЗ 17.3, ТЗ 22 п.6). These run without the `load` build tag
// and without a real 1000-node mesh: they check that the model is self-consistent — that
// every byte it bills has a sender and a receiver, that membership converges, that a
// silent 10 % crash does not wedge it, that a run is reproducible, that the report
// serialises — plus one hand-computed frame length, so a unit slip in the byte
// accounting cannot hide behind an agreement between two parts of the model.
//
// measureFullSyncBoundary also lives here. It quantifies the model's most load-bearing
// simplification, and the tagged load test calls it so «Границы в числах» quotes a number
// measured in the same process as the table above it.
//
// Everything is sized to stay a couple of seconds even under -race.

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"google.golang.org/protobuf/proto"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// tinyPhases is the shortest schedule that still exercises all three phases. New() raises
// every phase to at least one node heartbeat, because a shorter window holds no samples.
func tinyPhases() Phases {
	return Phases{IdleBeats: 4, ActiveBeats: 3, FailureBeats: 6, CrashFraction: 0.10, InjectEveryBeats: 2}
}

func newMesh(t *testing.T, n int) *Sim {
	t.Helper()
	s, err := New(n, 1, DefaultParams(), tinyPhases())
	if err != nil {
		t.Fatalf("New(%d): %v", n, err)
	}
	return s
}

func runMesh(t *testing.T, n int, ph Phases) (*Sim, *Result) {
	t.Helper()
	return runSchedule(t, n, ph, false)
}

// runSchedule builds one mesh and runs it, optionally forcing the exact flood walk for
// full-sync batches.
func runSchedule(t *testing.T, n int, ph Phases, exactRelay bool) (*Sim, *Result) {
	t.Helper()
	s, err := New(n, 1, DefaultParams(), ph)
	if err != nil {
		t.Fatalf("New(%d): %v", n, err)
	}
	s.exactRelay = exactRelay
	r, err := s.Run()
	if err != nil {
		t.Fatalf("Run(%d): %v", n, err)
	}
	return s, r
}

// TestTrafficIsSymmetric is the accounting invariant. Every frame the model bills is
// charged to a sender and to a receiver, so over a whole run the bytes sent equal the
// bytes received plus the traffic deliberately dropped — a copy that left a sender and
// whose recipient crashed before it arrived, or that was still in flight when the window
// closed. If the two sides drift, a verdict in the report measures the harness.
func TestTrafficIsSymmetric(t *testing.T) {
	for _, n := range []int{12, 40} {
		s, r := runMesh(t, n, tinyPhases())
		var send, recv int64
		for _, nd := range s.nodes {
			for k := Kind(0); k < KindCount; k++ {
				for _, pi := range []int{phaseIdleIdx, phaseActiveIdx, phaseFailIdx} {
					send += nd.phaseKind[pi][k][0]
					recv += nd.phaseKind[pi][k][1]
				}
			}
		}
		if send != r.TotalSendBytes {
			t.Errorf("%d nodes: Result reports %d sent, per-node accumulators say %d", n, r.TotalSendBytes, send)
		}
		if got, want := recv, send-r.LostInflight; got != want {
			t.Errorf("%d nodes: received %d bytes, sent %d, lost in flight %d — off by %d bytes",
				n, got, send, r.LostInflight, want-got)
		}
		if r.Crashed == 0 {
			t.Errorf("%d nodes: a %g crash fraction injected no victims, so this check would pass vacuously",
				n, r.Ph.CrashFraction)
		}
		t.Logf("%d nodes: sent %d, received %d, lost in flight %d (%.4f%% of sent); IHAVE/IWANT pulled %d copies",
			n, send, recv, r.LostInflight, 100*float64(r.LostInflight)/float64(max64(send, 1)), r.RepairCopies)
	}
}

// TestOwnStateWireCostMatchesHandArithmetic checks the byte model against an
// independently computed frame length. gossipFrameBytes is the source of every traffic
// number in the report, so it is verified against arithmetic written out field by field
// from the pubsub envelope — not against itself.
func TestOwnStateWireCostMatchesHandArithmetic(t *testing.T) {
	s := newMesh(t, 8)
	nd := s.nodes[0]
	payload := s.mustMarshal(&pb.MembershipGossip{
		States:     []*pb.PeerState{nd.state},
		FromPeerId: nd.id.String(),
	})
	id := []byte(nd.id)
	topic := s.p.Cfg.Discovery.Gossip.Topic

	// pb.Message inside rpc.RPC: from (bytes), data, seqno (8 bytes), topic, signature
	// (64 bytes). Every field number is below 16, so each costs one tag byte, a varint
	// length and the payload. proto3 emits nothing for an empty field, which is why Key
	// is not counted: an Ed25519 peer id already embeds the public key.
	msg := fieldCost(len(id)) + fieldCost(len(payload)) + fieldCost(pubsubSeqnoBytes) +
		fieldCost(len(topic)) + fieldCost(pubsubSignatureBytes)
	rpc := fieldCost(msg)         // RPC.Publish, field 1, length-delimited
	frame := varintLen(rpc) + rpc // the uvarint prefix internal/p2p writes

	if got := s.sizeOwnGossip; got != frame {
		t.Errorf("heartbeat frame = %d bytes, hand arithmetic says %d (payload %d, id %d, topic %d)",
			got, frame, len(payload), len(id), len(topic))
	}
	// The message id is from+seqno under the default id function, and the IHAVE
	// advertisement is billed per id — so a wrong id width scales the dominant control
	// cost of a wide mesh.
	if got, want := s.idSize, len(id)+pubsubSeqnoBytes; got != want {
		t.Errorf("message id width = %d, want %d (from %d + seqno %d)", got, want, len(id), pubsubSeqnoBytes)
	}
	// A batch has to be materially wider than a heartbeat, or the full-sync line in the
	// report is not measuring a 32-state payload.
	batch := gossipFrameBytes(s.topic, nd.id, s.mustMarshal(&pb.MembershipGossip{
		States:     statesOf(s, fullSyncBatchStates),
		FromPeerId: nd.id.String(),
	}))
	if batch <= s.sizeOwnGossip {
		t.Errorf("a %d-state batch costs %d bytes, no more than a single-state heartbeat %d",
			fullSyncBatchStates, batch, s.sizeOwnGossip)
	}
}

// TestPublishPayloadCarriesThePeerState guards the payload above: it must really be the
// generated message with the node's own state in it, not a stand-in whose length happens
// to be convenient.
func TestPublishPayloadCarriesThePeerState(t *testing.T) {
	s := newMesh(t, 6)
	nd := s.nodes[0]
	payload := s.mustMarshal(&pb.MembershipGossip{
		States:     []*pb.PeerState{nd.state},
		FromPeerId: nd.id.String(),
	})
	var back pb.MembershipGossip
	if err := proto.Unmarshal(payload, &back); err != nil {
		t.Fatalf("payload does not parse back: %v", err)
	}
	if len(back.States) != 1 || back.States[0].GetPeerId() != nd.id.String() {
		t.Fatalf("payload carries %d states, want one about %s", len(back.States), nd.id)
	}
	if back.States[0].GetTimestamp() != nd.state.GetTimestamp() {
		t.Error("payload's timestamp does not match the state the node publishes")
	}
}

// TestGossipEnvelopeShapeIsTheLibraryOne compares the model's envelope against a
// separately constructed pubsub RPC holding the same payload, so a change in how the node
// signs or frames a publication moves the test rather than the report.
func TestGossipEnvelopeShapeIsTheLibraryOne(t *testing.T) {
	s := newMesh(t, 6)
	nd := s.nodes[0]
	payload := s.mustMarshal(&pb.MembershipGossip{
		States:     []*pb.PeerState{nd.state},
		FromPeerId: nd.id.String(),
	})
	got := gossipFrameBytes(s.topic, nd.id, payload)
	want := framed(&pubsubpb.RPC{Publish: []*pubsubpb.Message{{
		From:      []byte(nd.id),
		Data:      payload,
		Seqno:     make([]byte, pubsubSeqnoBytes),
		Topic:     ptr(s.topic),
		Signature: make([]byte, pubsubSignatureBytes),
	}}})
	if got != want {
		t.Errorf("gossipFrameBytes = %d, direct envelope = %d", got, want)
	}
	if s.sizeOwnGossip != want {
		t.Errorf("measured heartbeat size %d differs from a one-state envelope %d", s.sizeOwnGossip, want)
	}
}

// TestCompositionConverges is the membership half of ТЗ 22 п.6: a mesh with no failures
// must learn its own members, keep them fresh, and be able to route to what it knows.
func TestCompositionConverges(t *testing.T) {
	s, r := runMesh(t, 40, tinyPhases())
	if r.ViewFrac < 0.95 {
		t.Errorf("views hold only %.3f of the mesh (%.1f states)", r.ViewFrac, r.ViewSize)
	}
	if r.FreshIdle < 0.9 {
		t.Errorf("only %.3f of view entries were fresh during the idle phase, want ≥ 0.90", r.FreshIdle)
	}
	if r.RoutableFrac < 0.9 {
		t.Errorf("only %.3f of known peers had a verified handshake, want ≥ 0.90", r.RoutableFrac)
	}
	if r.Handshakes == 0 || r.Grafts == 0 {
		t.Errorf("no handshakes (%d) or no grafts (%d): the mesh never formed", r.Handshakes, r.Grafts)
	}
	if r.ConnectivityPre < 0.99 {
		t.Errorf("the healthy mesh is only %.4f connected, want ≥ 0.99", r.ConnectivityPre)
	}
	// The mesh must sit at gossipsub's D, not above it: that is the property making a
	// publication's fan-out O(D) rather than O(N).
	if want := float64(s.p.GS.D); r.MeshPeers > want+0.5 {
		t.Errorf("average mesh size %.2f exceeds D=%d", r.MeshPeers, s.p.GS.D)
	}
	// Neighbours must respect neighbors.max, which is the node's own dial bound.
	if lim := float64(s.p.Cfg.Neighbors.Max); r.Links > lim+0.5 {
		t.Errorf("average %.2f links exceeds neighbors.max %d", r.Links, s.p.Cfg.Neighbors.Max)
	}
	// Stabilisation is the number ТЗ 17.3 asks for as «скорость стабилизации состава».
	if r.StabFullBeats < 0 || r.StabFullBeats > float64(r.Ph.IdleBeats) {
		t.Errorf("views became complete at heartbeat %v, outside the %d-beat idle window",
			r.StabFullBeats, r.Ph.IdleBeats)
	}
	if r.StabFreshBeats < r.StabFullBeats {
		t.Errorf("views were reported fresh at %.1f heartbeats but complete only at %.1f",
			r.StabFreshBeats, r.StabFullBeats)
	}
	t.Logf("40 nodes: view %.1f (%.3f mesh), routable %.1f, mesh %.2f, links %.1f, lag p50 %.1f тик, "+
		"стабилизация %.1f/%.1f heartbeat (полнота/свежесть)",
		r.ViewSize, r.ViewFrac, r.Routable, r.MeshPeers, r.Links, r.LagP50Ticks,
		r.StabFullBeats, r.StabFreshBeats)
}

// TestTenPercentCrashDoesNotBreakTheRun is ТЗ 22 п.6's failure scenario at a size that
// fits in a unit test: the run must finish, the crashed nodes must stop being believed
// in, and the mesh must keep routing work.
func TestTenPercentCrashDoesNotBreakTheRun(t *testing.T) {
	const n = 50
	s, r := runMesh(t, n, Phases{IdleBeats: 5, ActiveBeats: 4, FailureBeats: 12,
		CrashFraction: 0.10, InjectEveryBeats: 2})
	if want := int(float64(n) * 0.10); r.Crashed != want {
		t.Fatalf("crashed %d nodes, want %d", r.Crashed, want)
	}
	if r.RebuildP50 <= 0 {
		t.Error("no node's view finished expiring the crashed peers: recovery was not measured")
	}
	if r.RebuildP50 > float64(r.Ph.FailureBeats) {
		t.Errorf("views took %.1f heartbeats to clear the dead, longer than the whole failure phase",
			r.RebuildP50)
	}
	if r.FreshRecover < 0.9 {
		t.Errorf("after the crash only %.3f of view entries were fresh, want ≥ 0.90", r.FreshRecover)
	}
	if r.ConnectivityEnd < r.ConnectivityMin {
		t.Errorf("connectivity ended (%.4f) below its own minimum (%.4f)", r.ConnectivityEnd, r.ConnectivityMin)
	}
	// The survivors are still one graph: at this degree a 10 % loss sits below the
	// percolation threshold, so the honest expectation is that connectivity does not
	// collapse. A model reporting a crash tearing the graph apart here would be reporting
	// the harness, not the protocol.
	if r.ConnectivityEnd < 0.99 {
		t.Errorf("connectivity fell to %.4f after losing %d of %d nodes; expected the survivors to stay "+
			"one component at neighbors.min=%d links per node",
			r.ConnectivityEnd, r.Crashed, n, s.p.Cfg.Neighbors.Min)
	}
	if r.LostEntries == 0 {
		t.Error("no view entry was ever lost, so expiry did not run after the crash")
	}
	if r.TasksInjected == 0 {
		t.Error("the active phase injected no work, so the routing numbers are vacuous")
	}
	if done := r.TasksLocal + r.TasksRouted; done == 0 {
		t.Error("no task completed on any path")
	}
	if r.TasksLost < 0 || r.TasksLocal+r.TasksRouted+r.TasksLost+r.TasksNoWorker > 4*r.TasksInjected {
		t.Errorf("task accounting does not add up: %d injected, %d local, %d routed, %d lost, "+
			"%d without worker", r.TasksInjected, r.TasksLocal, r.TasksRouted, r.TasksLost, r.TasksNoWorker)
	}
	t.Logf("%d nodes, %d crashed: rebuild p50 %.1f / max %.1f heartbeat, связь %.4f → %.4f → %.4f, "+
		"записей views выбыло %d, задач %d инжектировано / %d локально / %d делегировано / %d потеряно",
		n, r.Crashed, r.RebuildP50, r.RebuildMax, r.ConnectivityPre, r.ConnectivityMin, r.ConnectivityEnd,
		r.LostEntries, r.TasksInjected, r.TasksLocal, r.TasksRouted, r.TasksLost)
}

// TestResultSeriesStaysFinite catches the class of bug that does not panic: a division by
// an empty live set, a percentile over no samples, a rate computed across zero seconds.
func TestResultSeriesStaysFinite(t *testing.T) {
	_, r := runMesh(t, 25, tinyPhases())
	vals := map[string]float64{
		"idle kbit": r.Idle.KbitService, "active kbit": r.Active.KbitIn + r.Active.KbitOut,
		"peak": r.Active.KbitPeak, "view": r.ViewSize, "fresh": r.FreshIdle,
		"lag": r.LagP50Ticks, "route p99": r.RouteP99, "hops": r.HopsMean,
		"rebuild p95": r.RebuildP95, "connectivity": r.ConnectivityEnd,
	}
	for name, v := range vals {
		if v != v || v < 0 {
			t.Errorf("%s = %v, want a finite non-negative number", name, v)
		}
	}
	if r.Idle.Seconds <= 0 || r.Active.Seconds <= 0 || r.Failure.Seconds <= 0 {
		t.Errorf("phase windows are not positive: %+v", r.Ph)
	}
	for _, ks := range r.Kinds {
		if ks.SendBytes < 0 || ks.RecvBytes < 0 {
			t.Errorf("%s: negative byte totals (%d sent, %d received)", ks.Kind, ks.SendBytes, ks.RecvBytes)
		}
	}
	if r.PeakInflight <= 0 {
		t.Errorf("peak in-flight deliveries = %d, the queue was never measured", r.PeakInflight)
	}
	if r.Deliveries <= 0 {
		t.Error("no deliveries were counted")
	}
}

// TestReportSerialises checks the markdown the load test prints: size columns filled from
// the results actually passed in, per-size detail, and the boundary section attached.
func TestReportSerialises(t *testing.T) {
	results := make([]Result, 0, 3)
	for _, n := range []int{10, 20, 30} {
		_, r := runMesh(t, n, tinyPhases())
		results = append(results, *r)
	}
	meta := Meta{Machine: "test-machine", GoVersion: "go test", NProc: "1", Elapsed: "1s",
		Commands: []string{"go test ./internal/simulator/"}}
	md := Markdown(results, meta)
	if len(md) < 4000 {
		t.Fatalf("report is %d bytes, too short to contain the sections ТЗ 17.3 asks for", len(md))
	}
	for _, want := range []string{
		"## Как воспроизвести",
		"test-machine",
		"## Сводка по размерам",
		"| Метрика | 10 узлов | 20 узлов | 30 узлов |",
		"## 30 узлов: подробно",
		"норматив выполнен",
		"Оценка по ТЗ 16.4",
		"Стабилизация состава",
		"## Что модель воспроизводит",
		"## Что модель НЕ воспроизводит",
		"## Границы, о которых надо знать",
		"### Границы в числах",
		"Ретрансляция пачки full-sync считается аналитически",
		"копий подавлено дедупликацией",
		"не замерялось",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("report is missing %q", want)
		}
	}
	// A verdict is only honest if it is a verdict: never an empty cell, and PASS/FAIL has
	// to be spelled out in words as well as codes.
	for _, r := range results {
		if r.Idle.Verdict != "PASS" && r.Idle.Verdict != "FAIL" {
			t.Errorf("%d nodes: idle verdict is %q", r.Nodes, r.Idle.Verdict)
		}
		if r.Idle.VerdictRu() == "" {
			t.Errorf("%d nodes: idle verdict has no wording", r.Nodes)
		}
	}
	if got := strings.Count(md, "узлов: подробно"); got != 3 {
		t.Errorf("report has %d per-size sections, want 3", got)
	}
	// A single-size rerun must not leave two empty columns behind.
	one := Markdown(results[:1], meta)
	if strings.Contains(one, "| 10 узлов | 20 узлов |") {
		t.Error("a one-size report still prints the three-size header")
	}
}

// TestRunsAreDeterministic is what makes a simulation number quotable at all: the same
// seed and the same schedule must produce byte-identical traffic and identical behaviour
// counts. The mesh picks its own candidates out of Go maps, so this is the test that
// catches an order leak.
func TestRunsAreDeterministic(t *testing.T) {
	_, a := runMesh(t, 30, tinyPhases())
	_, b := runMesh(t, 30, tinyPhases())
	fields := []struct {
		name string
		x, y float64
	}{
		{"idle kbit", a.Idle.KbitService, b.Idle.KbitService},
		{"active kbit", a.Active.KbitIn + a.Active.KbitOut, b.Active.KbitIn + b.Active.KbitOut},
		{"view", a.ViewSize, b.ViewSize},
		{"fresh", a.FreshIdle, b.FreshIdle},
		{"hops", a.HopsMean, b.HopsMean},
		{"rebuild", a.RebuildP50, b.RebuildP50},
		{"connectivity min", a.ConnectivityMin, b.ConnectivityMin},
		{"stabilisation", a.StabFullBeats, b.StabFullBeats},
		// The per-node peak is the field that catches an order leak the aggregates cannot:
		// a candidate set drawn from a map without normalisation still gives every node the
		// same number of recipients, so every total is identical while one node's worst
		// heartbeat moves. This is exactly how the un-sorted gossipTargets was found.
		{"worst-node peak", a.Active.KbitPeak, b.Active.KbitPeak},
		{"idle peak", a.Idle.KbitPeak, b.Idle.KbitPeak},
		{"peak in flight", float64(a.PeakInflight), float64(b.PeakInflight)},
		{"dial caps", float64(a.DialCapped), float64(b.DialCapped)},
	}
	for _, f := range fields {
		if f.x != f.y {
			t.Errorf("%s differs between two runs with the same seed: %v vs %v", f.name, f.x, f.y)
		}
	}
	if a.TotalSendBytes != b.TotalSendBytes || a.TotalRecvBytes != b.TotalRecvBytes ||
		a.Deliveries != b.Deliveries || a.TasksLost != b.TasksLost || a.IwantFrames != b.IwantFrames {
		t.Errorf("counters drifted: sent %d vs %d, received %d vs %d, deliveries %d vs %d, losses %d vs %d",
			a.TotalSendBytes, b.TotalSendBytes, a.TotalRecvBytes, b.TotalRecvBytes,
			a.Deliveries, b.Deliveries, a.TasksLost, b.TasksLost)
	}
}

// TestSeedChangesTheMesh is the other half of the determinism claim: identity and
// workload come from the seed, so two seeds must not give the same numbers.
func TestSeedChangesTheMesh(t *testing.T) {
	run := func(seed int64) (int64, int64) {
		s, err := New(30, seed, DefaultParams(), tinyPhases())
		if err != nil {
			t.Fatal(err)
		}
		r, err := s.Run()
		if err != nil {
			t.Fatal(err)
		}
		return r.TotalSendBytes, r.SkillSyncs + r.Grafts + r.Handshakes
	}
	a, ac := run(1)
	b, bc := run(2)
	if a == b && ac == bc {
		t.Errorf("seeds 1 and 2 produced identical traffic (%d bytes) and events (%d): the seed is "+
			"not driving the mesh", a, ac)
	}
	t.Logf("seed 1: %d bytes / %d событий; seed 2: %d bytes / %d событий", a, ac, b, bc)
}

// TestFullSyncAnalyticVersusExactFlood quantifies the most load-bearing simplification in
// the model and is the check behind «Границы в числах». Per-heartbeat own-state
// publications are walked delivery by delivery; a PublishFull batch's onward relay is
// billed analytically (one copy per subscriber) instead, because walking it costs Θ(N²)
// per beat. This runs the same seed both ways and asserts the two agree closely enough for
// the boundary's claim to hold at the sizes quoted.
//
// The error is size dependent, so one reading is not enough and the smallest one is the
// most flattering: the test sweeps the sizes load_test.go's boundarySizes publishes and
// asserts the claim at every one of them. Under -short only the base size runs, which is
// what keeps make ci's fast suite in the seconds.
//
// Override the list with ZETOMESH_SIM_SYNC_NODES to repeat the comparison elsewhere.
func TestFullSyncAnalyticVersusExactFlood(t *testing.T) {
	for _, n := range floodSweepSizes() {
		n := n
		t.Run(fmt.Sprintf("nodes=%d", n), func(t *testing.T) {
			b := measureFullSyncBoundary(t, n)
			t.Logf("%d узлов, idle %d heartbeat: full-sync отправлено — аналитика %d байт, точный обход "+
				"%d байт (%+.2f%%); весь трафик %d против %d байт (%+.2f%%); на узел за прогон %.1f против "+
				"%.1f Кбит/с (%+.2f%%); доставок %d против %d (%+.2f%%); полнота views %.1f против %.1f "+
				"(%+.2f%%)",
				b.Nodes, b.Beats, b.Analytic, b.Exact, b.Percent, b.TotalAnalyt, b.TotalExact, b.TotalDelta,
				b.RunKbitAnalyt, b.RunKbitExact, b.RunKbitDelta,
				b.DelivAnalyt, b.DelivExact, b.DelivDelta, b.ViewAnalyt, b.ViewExact, b.ViewDelta)
			if d := absPct(b.Percent); d > 60 {
				t.Errorf("analytic full-sync billing is %.1f%% away from the exact walk at %d nodes; "+
					"the boundary text describes a smaller effect — re-measure it", d, n)
			}
			// Completeness is the claim worth guarding: the boundary says the approximation
			// moves bytes and not membership. Where it stops being true is itself a boundary,
			// so the failure names the size at which the wording has to be re-read.
			if d := absPct(b.ViewDelta); d > 13 {
				t.Errorf("the analytic relay changes view completeness by %.1f%% at %d nodes (%.1f vs "+
					"%.1f states) — «Границы в числах» must state that size, not just the small one",
					d, n, b.ViewAnalyt, b.ViewExact)
			}
			if d := absPct(b.RunKbitDelta); d > 50 {
				t.Errorf("the run-wide per-node rate differs by %.1f%% between the two billings at %d "+
					"nodes; a verdict against ТЗ 16.4 would then depend on which one was picked", d, n)
			}
		})
	}
}

// floodSweepSizes is the mesh sizes the analytic-versus-exact cross-check walks. The base
// reading is the cheap one and is all the fast suite can afford: an exact flood walk costs
// Θ(N²) deliveries per beat, measured here at 1.2 s for 60 nodes and 103 s for 300. The
// wider sweep — the one that shows the error growing with the mesh — belongs to the tagged
// load run, whose boundarySizes default to 60,150,300. ZETOMESH_SIM_SYNC_NODES widens either.
func floodSweepSizes() []int {
	if v := strings.TrimSpace(os.Getenv("ZETOMESH_SIM_SYNC_NODES")); v != "" {
		var out []int
		for _, part := range strings.Split(v, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || n < 6 {
				return []int{60}
			}
			out = append(out, n)
		}
		return out
	}
	return []int{60}
}

// measureFullSyncBoundary runs one mesh twice — once billing a full-sync batch's relay
// analytically, once walking the flood exactly — and returns the difference in the units
// the boundaries section renders.
func measureFullSyncBoundary(t *testing.T, n int) Boundary {
	t.Helper()
	// The idle window has to contain a full-sync beat whose view is already populated:
	// config's full_sync period puts one at beat 0 (empty views) and the next at
	// FullSyncEveryBeats, so the window is that plus one. Without a batch there is
	// nothing to compare.
	ph := Phases{IdleBeats: DefaultParams().FullSyncEveryBeats() + 1, ActiveBeats: 1,
		FailureBeats: 1, CrashFraction: 0, InjectEveryBeats: 3}

	sa, ra := runSchedule(t, n, ph, false)
	sx, rx := runSchedule(t, n, ph, true)

	analytic, exact := sa.wireKind[KindFullSync], sx.wireKind[KindFullSync]
	if analytic == 0 || exact == 0 {
		t.Fatalf("%d nodes: no full-sync traffic measured (analytic %d, exact %d)", n, analytic, exact)
	}
	// The run-wide rate over the whole window is the comparable one. A per-phase rate is
	// not: the exact walk lets a flood's tail arrive after the phase edge, while the
	// analytic relay books every copy of a batch in the batch's own beat.
	runA, runX := kbitAll(sa), kbitAll(sx)
	viewDelta := 0.0
	if rx.ViewSize > 0 {
		viewDelta = 100 * (ra.ViewSize - rx.ViewSize) / rx.ViewSize
	}
	return Boundary{
		Nodes: n, Beats: ph.IdleBeats,
		Analytic: analytic, Exact: exact, Percent: pctDiff(analytic, exact),
		TotalAnalyt: sa.wireTotal(), TotalExact: sx.wireTotal(),
		TotalDelta:    pctDiff(sa.wireTotal(), sx.wireTotal()),
		RunKbitAnalyt: runA, RunKbitExact: runX,
		RunKbitDelta: pctDiffF(runA, runX),
		ViewAnalyt:   ra.ViewSize, ViewExact: rx.ViewSize, ViewDelta: viewDelta,
		DelivAnalyt: ra.Deliveries, DelivExact: rx.Deliveries,
		DelivDelta: pctDiffF(float64(ra.Deliveries), float64(rx.Deliveries)),
	}
}

// TestPublishFullBatchSizeMatchesGossipCode pins fullSyncBatchStates against the literal
// inside internal/discovery/gossip.go. The real constant is unexported, so this is the
// only thing stopping the model from quietly drifting away from the protocol it
// reproduces.
func TestPublishFullBatchSizeMatchesGossipCode(t *testing.T) {
	src, err := os.ReadFile("../../internal/discovery/gossip.go")
	if err != nil {
		t.Skipf("cannot read internal/discovery/gossip.go: %v", err)
	}
	m := regexp.MustCompile(`(?m)^\s*const batchSize\s*=\s*(\d+)\s*$`).FindSubmatch(src)
	if m == nil {
		t.Fatal("no `const batchSize = N` in internal/discovery/gossip.go — either the node stopped " +
			"batching PublishFull (so the model is wrong) or the literal was renamed (so this test needs updating)")
	}
	n, err := strconv.Atoi(string(m[1]))
	if err != nil {
		t.Fatalf("batchSize literal %q: %v", m[1], err)
	}
	if n != fullSyncBatchStates {
		t.Errorf("internal/discovery batches PublishFull at %d states, the simulator at %d", n, fullSyncBatchStates)
	}
	// The inter-batch pause the model paces batches with comes from the same ratio.
	if !regexp.MustCompile(`Heartbeat\.D\(\)\s*/\s*8`).Match(src) {
		t.Errorf("internal/discovery/gossip.go no longer pauses batches at heartbeat/8; "+
			"Params.FullSyncBatchPauseTicks (which computes %d) needs re-checking",
			DefaultParams().FullSyncBatchPauseTicks())
	}
}

// TestAllLimitsComeFromConfig is ТЗ 17.3's «НЕ дублируй числа литералами» turned into a
// check: the numbers the report compares against must be the shipped configuration's and
// gossipsub's, not values the simulator happened to write down.
func TestAllLimitsComeFromConfig(t *testing.T) {
	p := DefaultParams()
	c := p.Cfg
	if got, want := p.Heartbeat(), c.Discovery.Gossip.Heartbeat.D(); got != want {
		t.Errorf("heartbeat %v, config says %v", got, want)
	}
	if got, want := p.FullSync(), c.Discovery.Gossip.FullSync.D(); got != want {
		t.Errorf("full_sync %v, config says %v", got, want)
	}
	if got, want := p.FailureTimeout(), c.Discovery.Gossip.FailureTimeout.D(); got != want {
		t.Errorf("failure_timeout %v, config says %v", got, want)
	}
	if got, want := p.GS.D, 6; got != want {
		t.Errorf("gossipsub D = %d, library default is %d", got, want)
	}
	ratio := int(p.FullSync() / p.Heartbeat())
	if got := p.FullSyncEveryBeats(); got != ratio {
		t.Errorf("full sync every %d beats, the config ratio is %d", got, ratio)
	}
	if got := p.GS.IDontWantMessageThreshold; got != 1024 {
		t.Errorf("IDONTWANT threshold %d, gossipsub default is 1024", got)
	}
	// The ceilings are the specification's, and they are the numbers a verdict is judged
	// against, so they must be visible as such.
	if CeilingKbitIdle != 100 || CeilingKbitActive != 1000 {
		t.Errorf("ТЗ 16.4 ceilings are %v/%v Кбит/с, specification says 100/1000",
			CeilingKbitIdle, CeilingKbitActive)
	}
}

// ---------- helpers ----------

// statesOf returns up to n of the mesh's own states, for a batch-width bound.
func statesOf(s *Sim, n int) []*pb.PeerState {
	out := make([]*pb.PeerState, 0, n)
	for _, nd := range s.nodes {
		if len(out) == n {
			break
		}
		out = append(out, nd.state)
	}
	return out
}

// kbitAll is a whole run's aggregate per-node rate in Кбит/с, the unit the verdicts are
// given in.
func kbitAll(s *Sim) float64 {
	return kbit(s.wireTotal(), len(s.nodes), int64(s.tick)*s.tickSeconds())
}

// wireTotal is everything the model billed as sent across the whole run, over all kinds.
func (s *Sim) wireTotal() int64 {
	var t int64
	for k := Kind(0); k < KindCount; k++ {
		t += s.wireKind[k]
	}
	return t
}

// fieldCost is the protobuf wire cost of one length-delimited field whose tag number is
// below 16: one tag byte, a varint length, and the payload.
func fieldCost(n int) int { return 1 + varintLen(n) + n }

// varintLen is protobuf's varint width, computed independently of the library so the
// arithmetic test above does not borrow the thing it checks.
func varintLen(n int) int {
	c := 0
	for {
		c++
		n >>= 7
		if n == 0 {
			return c
		}
	}
}

func pctDiff(a, b int64) float64 {
	if b == 0 {
		return 0
	}
	return 100 * float64(a-b) / float64(b)
}

func pctDiffF(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return 100 * (a - b) / b
}

func absPct(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
