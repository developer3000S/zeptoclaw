//go:build integration

// Integration scenarios of ТЗ 17.2. Several real nodes run inside one test
// process: real libp2p hosts on 127.0.0.1, real protobuf on the wire, real
// discovery, real routing and real task forwarding. Only the agent itself is
// simulated (the offline stub adapter), which is what makes these integration
// rather than unit tests.
//
// Run with: make test-integration
//
//	go test -timeout 900s -tags=integration -count=1 ./internal/node/...
//
// Two topologies are used, and the difference matters:
//
//   - registry fabric — discovery on, one shared unix-socket registry, the
//     mechanism install.sh already uses for N instances of one host. The
//     registry is authoritative on a single machine, so every member learns and
//     dials every other member: this is a full mesh. It is the right shape for
//     "the nodes find each other" and "the task is routed to whoever has the
//     skill", and the wrong shape for testing multi-hop delegation.
//   - manual fabric (skipDiscovery) — nobody dials anybody except the edges the
//     scenario connects. ТЗ 17.2's "delegation through an intermediate node"
//     needs A to have no direct view of C, and no discovery mechanism can give
//     that guarantee on one host, because discovery's whole job is to remove
//     exactly that blindness.
//
// Gossip is off everywhere: it would re-introduce the direct view the manual
// fabric exists to withhold. Stub latency is 0, so a task that can complete
// finishes in milliseconds and every wait below is an upper bound, never an
// expected duration.
package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/proto"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/picoclaw"
	"github.com/developer3000S/zeptoclaw/internal/security"
	"github.com/developer3000S/zeptoclaw/internal/storage"
	"github.com/developer3000S/zeptoclaw/internal/tasks"
	"github.com/developer3000S/zeptoclaw/internal/tracing"
	"github.com/developer3000S/zeptoclaw/internal/triggers"
)

// testNode is one live mesh node with the handles assertions need: its key
// material (to author traffic on its behalf) and its config (to read budgets).
type testNode struct {
	name string
	*Node
	id  *security.Identity
	cfg *config.Config
}

type mesh struct {
	t        *testing.T
	shared   string
	nodes    []*testNode
	verbose  bool
	skipDisc bool
}

// meshSeq numbers each scenario's registry directory. The path is kept short on
// purpose: a unix socket path is capped at 108 bytes, and t.TempDir() is long
// enough that <tmp>/TestIntegration…/001/shared/<52-char peer id>.sock
// overflows it. The overflow is not fatal — node.New logs probe_socket_disabled
// and carries on — which is precisely why it must be avoided: the local registry
// decides a peer's liveness by that probe, so a silently missing socket means
// the nodes never learn about each other.
var meshSeq atomic.Uint64

// newMesh builds a registry fabric.
func newMesh(t *testing.T) *mesh {
	t.Helper()
	return newFabric(t, false)
}

// newManualMesh builds a fabric where every edge is dialed by the scenario.
func newManualMesh(t *testing.T) *mesh {
	t.Helper()
	return newFabric(t, true)
}

func newFabric(t *testing.T, manual bool) *mesh {
	t.Helper()
	shared := filepath.Join(os.TempDir(), fmt.Sprintf("ztm-%d-%d", os.Getpid(), meshSeq.Add(1)))
	if err := os.MkdirAll(shared, 0o700); err != nil {
		t.Fatalf("registry dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(shared) })
	return &mesh{
		t:        t,
		shared:   shared,
		verbose:  os.Getenv("ZETOMESH_TEST_VERBOSE") != "",
		skipDisc: manual,
	}
}

// node starts one member of the fabric. tune is the only way a scenario reaches
// into the configuration, so the shared baseline stays in one place; wrap, when
// given, decorates the executor — and must be applied here rather than by
// assigning to n.Adapter afterwards, because the Manager was handed the adapter
// while the node was being built.
func (m *mesh) node(name string, skills []string, tune func(*config.Config), wrap ...func(picoclaw.Adapter) picoclaw.Adapter) *testNode {
	m.t.Helper()
	dir := filepath.Join(m.t.TempDir(), name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		m.t.Fatalf("%s: mkdir: %v", name, err)
	}
	cfg := config.Default()
	cfg.Node.Name = name
	cfg.Node.DataDir = dir
	// One TCP listener is enough in-process; QUIC only adds variance.
	cfg.Node.Listen = []string{"/ip4/127.0.0.1/tcp/0"}

	cfg.Discovery.LocalRegistry = !m.skipDisc
	cfg.Discovery.LocalSocketDir = m.shared
	cfg.Discovery.MDNS = false
	cfg.Discovery.DHT = false
	cfg.Discovery.PeerExchange = false
	cfg.Discovery.Bootstrap = nil
	cfg.Discovery.Gossip.Enabled = false

	// Skill exchange stays on: it belongs to the join handshake, so disabling
	// it would hide wiring mistakes instead of removing flakiness.
	cfg.Capabilities.SkillExchange.Enabled = true
	cfg.Capabilities.SkillExchange.DiscloseTo = "any"
	cfg.Capabilities.SkillExchange.Interval = config.Duration(time.Second)

	// The skills argument is the sole authority on execution rights; empty
	// skill_docs means these nodes advertise names without descriptions.
	cfg.Capabilities.Skills = skills
	cfg.Capabilities.SkillDocs = nil
	cfg.Capabilities.AcceptExternalTasks = true

	cfg.Tasks.Forwarding.SearchRelay.Enabled = false
	cfg.Tasks.Forwarding.AttemptTimeout = config.Duration(20 * time.Second)
	cfg.Tasks.DedupWindow = config.Duration(10 * time.Minute)
	// Zero stub latency: a task that can complete finishes in milliseconds, so
	// every wait in these scenarios is an upper bound, never an expected one.
	cfg.PicoClaw.Stub = config.StubConfig{Echo: true}
	if tune != nil {
		tune(cfg)
	}
	if err := cfg.Validate(); err != nil {
		m.t.Fatalf("%s: config: %v", name, err)
	}

	opts := Options{
		Config:        cfg,
		ConfigPath:    filepath.Join(dir, "node.yaml"),
		SkipDiscovery: m.skipDisc,
	}
	if !m.verbose {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if len(wrap) > 0 {
		var adapter picoclaw.Adapter = picoclaw.NewStub(cfg.PicoClaw.Stub, cfg.Capabilities.Skills)
		for _, w := range wrap {
			adapter = w(adapter)
		}
		opts.Adapter = adapter
	}

	n, err := New(opts)
	if err != nil {
		m.t.Fatalf("%s: new: %v", name, err)
	}
	if err := n.Start(context.Background()); err != nil {
		m.t.Fatalf("%s: start: %v", name, err)
	}
	tn := &testNode{name: name, Node: n, id: n.Identity, cfg: cfg}
	m.nodes = append(m.nodes, tn)
	m.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := n.Stop(ctx); err != nil {
			m.t.Logf("%s: stop: %v", name, err)
		}
	})
	return tn
}

// wait polls cond until it holds or the bound expires, then dumps every node's
// table of contents — a timeout here is nearly always a routing or trust
// problem, and the tables are what identifies it.
func (m *mesh) wait(what string, cond func() bool) {
	m.t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			for _, n := range m.nodes {
				m.t.Logf("%s table: %+v", n.name, n.Table.List())
			}
			m.t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func (m *mesh) connect(a, b *testNode) {
	m.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Host.Connect(ctx, b.Host.AddrInfo()); err != nil {
		m.t.Fatalf("%s → %s: connect: %v", a.name, b.name, err)
	}
	m.wait("table entry for "+b.name+" on "+a.name, func() bool {
		_, ok := a.Table.Get(b.ID())
		return ok
	})
}

// handshook reports a completed capabilities handshake. Both halves matter and
// neither follows from an open socket: completedHandshake is set only after the
// peer's signed Capabilities were verified (the same signal that makes the node
// share its rebind ledger), and the security policy must admit the peer as a
// delegate. The table holds no trust of its own — security.Policy decides that,
// and asking it is the only honest way to phrase "may delegate work to".
func (m *mesh) handshook(a, b *testNode) bool {
	nb, ok := a.Table.Get(b.ID())
	return ok && nb.Connected && a.completedHandshake(b.ID()) &&
		a.Policy.AllowDelegationTo(b.ID())
}

// link connects two nodes and waits for the handshake on both sides.
func (m *mesh) link(a, b *testNode) {
	m.t.Helper()
	m.connect(a, b)
	m.wait(a.name+" completed the handshake with "+b.name, func() bool { return m.handshook(a, b) })
	m.wait(b.name+" completed the handshake with "+a.name, func() bool { return m.handshook(b, a) })
}

func (m *mesh) knows(a, b *testNode) bool {
	_, ok := a.Table.Get(b.ID())
	return ok
}

// waitJournal polls one node's journal until the record satisfies want.
func (m *mesh) waitJournal(n *testNode, id string, want func(*storage.TaskRecord) bool) *storage.TaskRecord {
	m.t.Helper()
	var got *storage.TaskRecord
	m.wait(n.name+" journal for "+id, func() bool {
		rec, err := n.Store.GetTask(id)
		if err != nil || rec == nil {
			return false
		}
		got = rec
		return want(rec)
	})
	return got
}

// neighborSkills reports what a node quotes about a peer, i.e. the outcome of
// the verified-Capabilities handshake rather than local configuration.
func neighborSkills(n *Node, pid peer.ID) []string {
	if nb, ok := n.Table.Get(pid); ok {
		return nb.Skills
	}
	return nil
}

func submitTask(t *testing.T, n *testNode, instruction string, skills []string, ttl int) string {
	t.Helper()
	id, err := n.Manager.Submit(context.Background(), tasks.SubmitRequest{
		Instruction:    instruction,
		RequiredSkills: skills,
		TTL:            int32(ttl),
		Priority:       5,
	})
	if err != nil {
		t.Fatalf("%s: submit: %v", n.name, err)
	}
	return id
}

func awaitResult(t *testing.T, n *testNode, id string) *pb.TaskResult {
	t.Helper()
	res, err := n.Manager.AwaitResult(context.Background(), id)
	if err != nil {
		t.Fatalf("%s: await %s: %v", n.name, id, err)
	}
	return res
}

// completedFrom asserts the ТЗ phrasing literally: not "some result", but a
// COMPLETED result that the claimed worker really signed. The signature is
// self-certifying (Ed25519 public keys are recovered from the peer id), so this
// proves who ran the task regardless of who delivered it.
func completedFrom(t *testing.T, n *testNode, id string, worker *testNode) *pb.TaskResult {
	t.Helper()
	res := awaitResult(t, n, id)
	if res.GetStatus() != pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Fatalf("%s: task %s = %s (%s), want COMPLETED", n.name, id, res.GetStatus(), res.GetErrorMessage())
	}
	if res.GetWorkerPeerId() != worker.ID().String() {
		t.Fatalf("%s: task %s ran on %s, want %s", n.name, id, res.GetWorkerPeerId(), worker.name)
	}
	if err := security.VerifyWorkerResult(res, nil); err != nil {
		t.Fatalf("%s: result of %s is not signed by %s: %v", n.name, id, worker.name, err)
	}
	return res
}

// authorEnvelope builds and signs a task as if n had submitted it, so a
// scenario can put a crafted envelope on the wire. The transport signature
// covers content ‖ sender ‖ ttl ‖ route_stack (wire.TaskBody) and must be
// applied last, after the routing fields are final; origin_signature covers the
// content alone and survives relays rewriting the rest.
func authorEnvelope(t *testing.T, n *testNode, req tasks.BuildRequest, id string) *pb.TaskEnvelope {
	t.Helper()
	if id == "" {
		id = tasks.NewID()
	}
	env := tasks.Build(n.ID().String(), id, req, time.Now().UTC())
	signer := security.NewSigner(n.id)
	if err := signer.SignTaskOrigin(env); err != nil {
		t.Fatalf("%s: sign origin: %v", n.name, err)
	}
	if err := signer.SignTask(env); err != nil {
		t.Fatalf("%s: sign task: %v", n.name, err)
	}
	return env
}

func sendDirect(t *testing.T, a, b *testNode, env *pb.TaskEnvelope) *pb.TaskAck {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ack, err := a.Service.SendTask(ctx, b.Host.AddrInfo(), env)
	if err != nil {
		t.Fatalf("%s → %s: send task: %v", a.name, b.name, err)
	}
	return ack
}

// ТЗ 17.2.1–2: два узла находят друг друга.
func TestIntegrationTwoNodesDiscoverEachOther(t *testing.T) {
	m := newMesh(t)
	a := m.node("a", []string{"research"}, nil)
	b := m.node("b", []string{"coding"}, nil)

	// Discovery is the registry handing out addresses, not the socket: each side
	// must have learned the other's peer id and dialed it.
	m.wait("a discovers b", func() bool { return m.knows(a, b) && a.Host.IsConnected(b.ID()) })
	m.wait("b discovers a", func() bool { return m.knows(b, a) && b.Host.IsConnected(a.ID()) })
	m.link(a, b)

	// Skills are quoted per side and only from verified Capabilities.
	if !contains(neighborSkills(a.Node, b.ID()), "coding") {
		t.Fatalf("a does not see b's skills: %v", neighborSkills(a.Node, b.ID()))
	}
	if !contains(neighborSkills(b.Node, a.ID()), "research") {
		t.Fatalf("b does not see a's skills: %v", neighborSkills(b.Node, a.ID()))
	}
	// Learning a neighbour's skills must not grant them locally: routing
	// authority and execution authority stay separate.
	if contains(a.Skills.Names(), "coding") {
		t.Fatalf("a adopted b's execution rights: %v", a.Skills.Names())
	}
	// The registry only ever lists this scenario's members, so a table larger
	// than one neighbour would mean the test reached another socket namespace.
	if got := a.Table.Len(); got != 1 {
		t.Fatalf("a knows %d peers, want exactly b", got)
	}
}

// ТЗ 17.2.2–4: три узла образуют mesh, задача с А исполняется Б, результат
// возвращается на А.
func TestIntegrationThreeNodeMeshAndTaskRouting(t *testing.T) {
	m := newMesh(t)
	a := m.node("a", []string{"research"}, nil)
	b := m.node("b", []string{"coding", "research"}, nil)
	c := m.node("c", []string{"translate"}, nil)

	// A triangle: the registry makes every pair visible, and every pair must
	// still complete its own handshake before work can flow.
	m.link(a, b)
	m.link(a, c)
	m.link(b, c)

	id := submitTask(t, a, "compile the module", []string{"coding"}, 5)
	res := completedFrom(t, a, id, b)

	// The stub echoes the instruction, so a completed-but-unrelated result is
	// impossible: the text pins this to the task that was submitted.
	if !strings.Contains(res.GetText(), "compile the module") {
		t.Fatalf("result is not the answer to the task: %q", res.GetText())
	}
	if len(res.GetResultDigest()) == 0 {
		t.Fatal("result carries no digest")
	}
	// The task must be journalled as executed by B on both ends: A records the
	// round trip, B records that it did the work for A.
	recA := m.waitJournal(a, id, func(r *storage.TaskRecord) bool { return r.WorkerPeerID != "" })
	if recA.Status != tasks.Completed.String() {
		t.Fatalf("a's journal = %s, want %s", recA.Status, tasks.Completed)
	}
	if recA.OriginPeerID != a.ID().String() || recA.WorkerPeerID != b.ID().String() {
		t.Fatalf("a's journal lost the origin/worker pair: %+v", recA)
	}
	recB := m.waitJournal(b, id, func(r *storage.TaskRecord) bool { return r.WorkerPeerID != "" })
	if recB.WorkerPeerID != b.ID().String() || recB.OriginPeerID != a.ID().String() {
		t.Fatalf("b's journal lost the origin/worker pair: %+v", recB)
	}
	// c was never on the path.
	if _, err := c.Store.GetTask(id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("c journalled a task it never saw (err %v)", err)
	}
}

// Часть ТЗ 17.2.3: задача не находит исполнителя — отказ обязан назвать причину.
func TestIntegrationTaskFailsWithReasonWhenNoExecutorExists(t *testing.T) {
	m := newMesh(t)
	a := m.node("a", []string{"research"}, nil)
	b := m.node("b", []string{"coding"}, nil)
	m.link(a, b)

	id := submitTask(t, a, "anneal a graph", []string{"quantum-annealing"}, 5)
	res := awaitResult(t, a, id)
	if res.GetStatus() != pb.TaskStatus_TASK_STATUS_FAILED {
		t.Fatalf("status = %s, want FAILED", res.GetStatus())
	}
	if res.GetErrorClass() != pb.TaskErrorClass_TASK_ERROR_CLASS_NO_WORKER {
		t.Fatalf("error class = %s, want NO_WORKER", res.GetErrorClass())
	}
	// "no eligible peers reachable" alone sends the operator hunting for a
	// connectivity problem; the missing skill has to be named.
	if !strings.Contains(res.GetErrorMessage(), "quantum-annealing") {
		t.Fatalf("failure reason lost: %q", res.GetErrorMessage())
	}
}

// ТЗ 17.2.6: делегирование через промежуточный узел. Топология обязана быть
// ручной: в registry-фабрике А увидел бы В напрямую, и двухходовый маршрут
// вообще не состоялся бы — назначение discovery именно в том, чтобы убирать
// такую «слепоту».
func TestIntegrationDelegationThroughIntermediateNode(t *testing.T) {
	m := newManualMesh(t)
	a := m.node("a", []string{"research"}, nil)
	// Простой relay без навыков кандидатом в Select не станет: skillsMatch
	// требует полного покрытия запрошенных навыков (general/any — единственный
	// обход). Поэтому промежуточный узел обязан уметь сам навык, но отказывать в
	// его исполнении внешним узлам — тогда canExecute даёт «нет», и единственный
	// законный ход это переслать.
	b := m.node("b", []string{"summarize"}, func(cfg *config.Config) {
		cfg.Capabilities.AcceptExternalTasks = false
	})
	c := m.node("c", []string{"summarize"}, nil)

	m.link(a, b)
	m.link(b, c)
	if m.knows(a, c) {
		t.Fatal("a learned c directly; the relay hop would not have been exercised")
	}

	id := submitTask(t, a, "summarize the log", []string{"summarize"}, 7)
	res := completedFrom(t, a, id, c)

	// Маршрут обязан состоять ровно из двух рёбер. route_stack результата —
	// копия стека окружения на момент исполнения (baseResult), то есть узлы,
	// через которые задача шла; сам исполнитель в него не дописывается, его
	// имя — отдельное поле worker_peer_id.
	if !contains(res.GetRouteStack(), a.ID().String()) {
		t.Fatalf("result does not carry the origin in the route: %v", res.GetRouteStack())
	}
	if !contains(res.GetRouteStack(), b.ID().String()) {
		t.Fatalf("result did not travel through %s: %v", b.name, res.GetRouteStack())
	}
	if contains(res.GetRouteStack(), c.ID().String()) {
		t.Fatalf("worker %s listed itself as a transit hop: %v", c.name, res.GetRouteStack())
	}
	// Журнал инициатора помнит, кому он передал задачу, и кто её выполнил.
	// Оба поля пишут разные горутины: worker+статус — обработчик результата,
	// DelegatedTo — пост-ack бухгалтерия forward'а, и она успевает приземлиться
	// позже (ack и результат — разные потоки). Ждём осевшее состояние, а не
	// одно из полей.
	recA := m.waitJournal(a, id, func(r *storage.TaskRecord) bool {
		return r.WorkerPeerID != "" && len(r.DelegatedTo) > 0
	})
	if recA.Status != tasks.Completed.String() {
		t.Fatalf("a's journal = %s, want %s", recA.Status, tasks.Completed)
	}
	if !contains(recA.DelegatedTo, b.ID().String()) {
		t.Fatalf("a's journal does not name the relay it delegated to: %+v", recA)
	}
	// Ретранслятор передал задачу дальше и сам ничего не исполнил. Какое из
	// двух состояний лежит в журнале, зависит от того, что успеет раньше:
	// post-ack учёт пишет FORWARDED, а возврат результата от c закрывает запись
	// как COMPLETED с worker=c. Быстрый исполнитель (stub — единицы миллисекунд)
	// выигрывает гонку, и транзитного состояния никто не observes: терминальная
	// запись — истина, и страж в forward её не понижает. Поэтому проверяются
	// инварианты, общие для обоих исходов, а не преходящий статус.
	recB := m.waitJournal(b, id, func(r *storage.TaskRecord) bool {
		return contains(r.DelegatedTo, c.ID().String())
	})
	if recB.OriginPeerID != a.ID().String() {
		t.Fatalf("b's journal lost the origin: %+v", recB)
	}
	if recB.WorkerPeerID == b.ID().String() {
		t.Fatalf("relay b executed the task it should only forward: %+v", recB)
	}
	if s := recB.Status; s != tasks.Forwarded.String() && s != tasks.Completed.String() {
		t.Fatalf("b's journal status = %s, want %s (transit) or %s (result already relayed)",
			s, tasks.Forwarded, tasks.Completed)
	}
	if recB.Status == tasks.Completed.String() && recB.WorkerPeerID != c.ID().String() {
		t.Fatalf("b closed the record as %s without naming c the worker: %+v", tasks.Completed, recB)
	}
	if got := testutil.ToFloat64(b.Metrics.ForwardAttempts.WithLabelValues("accepted")); got < 1 {
		t.Fatalf("b recorded %v accepted forwards, want ≥1", got)
	}
	if n := b.Manager.RunningCount(); n != 0 {
		t.Fatalf("relay b is running %d tasks", n)
	}
}

// ТЗ 17.2.5: исполнитель падает с задачей в руках. Сеть обязана сообщить об
// этом инициатору сразу, а не держать его до истечения TTL.
func TestIntegrationWorkerDiesMidTask(t *testing.T) {
	m := newManualMesh(t)
	a := m.node("a", []string{"research"}, nil)
	gate := newGateAdapter()
	b := m.node("b", []string{"coding"}, nil, gate.wrap())
	m.link(a, b)

	// The task is submitted with a ttl far longer than the test, so the only
	// way it can resolve early is the failure path under test — not a timeout.
	id := submitTask(t, a, "compile it and die", []string{"coding"}, 10)
	m.wait("b started executing", gate.entered)
	// The task must be genuinely unresolved, or the scenario proves nothing.
	if _, err := a.Store.GetResult(id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("task already resolved before the worker died (err %v)", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// The gate stays closed forever, which is exactly a process killed mid-task:
	// the worker never produces an answer, so the origin can only learn of the
	// loss from the dead connection.
	if err := b.Stop(ctx); err != nil {
		t.Fatalf("stop worker: %v", err)
	}

	res := awaitResult(t, a, id)
	if res.GetStatus() == pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Fatal("task completed although its worker died before answering")
	}
	// The loss is reported as a timeout, which is retryable by classification —
	// the origin is entitled to submit the work again.
	if res.GetErrorClass() != pb.TaskErrorClass_TASK_ERROR_CLASS_TIMEOUT {
		t.Fatalf("error class = %s, want TIMEOUT: %s", res.GetErrorClass(), res.GetErrorMessage())
	}
	if !strings.Contains(res.GetErrorMessage(), "downstream peer disconnected") {
		t.Fatalf("failure does not say what happened: %q", res.GetErrorMessage())
	}
	if got := testutil.ToFloat64(a.Metrics.TasksTimedOut); got < 1 {
		t.Fatalf("a counted %v timeouts, want ≥1", got)
	}
	// The origin's journal must agree with the result it delivered, and the
	// answer must have arrived well before the task deadline.
	rec := m.waitJournal(a, id, func(r *storage.TaskRecord) bool { return r.Status == tasks.TimedOut.String() })
	if rec.FinishedAt.After(rec.ReceivedAt.Add(3 * time.Minute)) {
		t.Fatalf("the origin learned of the loss only after %s — that is a timeout, not a failure report",
			rec.FinishedAt.Sub(rec.ReceivedAt))
	}
	// The mesh converged: the dead worker left the origin's view instead of
	// staying a phantom candidate.
	m.wait("a drops b", func() bool { return !a.Host.IsConnected(b.ID()) })
}

// ТЗ 17.2.7: повторная отправка той же задачи не приводит к повторному
// исполнению.
func TestIntegrationDuplicateDeliveryIsNotExecutedTwice(t *testing.T) {
	m := newMesh(t)
	a := m.node("a", []string{"research"}, nil)
	runs := &countingAdapter{}
	b := m.node("b", []string{"coding"}, nil, runs.wrap())
	m.link(a, b)

	id := submitTask(t, a, "build it once", []string{"coding"}, 5)
	completedFrom(t, a, id, b)
	if n := runs.started(); n != 1 {
		t.Fatalf("adapter ran the task %d times before any duplicate, want 1", n)
	}

	// The replay is authored by hand because the scenario must deliver the same
	// task id twice, and Submit mints a fresh id on every call. It is signed by
	// a, so the only thing wrong with it is that b has seen this id already.
	env := authorEnvelope(t, a, tasks.BuildRequest{
		Instruction: "build it once", RequiredSkills: []string{"coding"}, TTL: 5, Priority: 5,
	}, id)

	// What dedup actually is: a claim on the task id (storage.ClaimDedup),
	// content-blind. So the honest expectation is "exactly one execution plus an
	// explicit duplicate signal" — the mesh never compares payloads.
	second := sendDirect(t, a, b, env)
	if second.GetStatus() != pb.AckStatus_ACK_STATUS_DUPLICATE {
		t.Fatalf("duplicate ack = %s (%s), want DUPLICATE", second.GetStatus(), second.GetReason())
	}
	if got := testutil.ToFloat64(b.Metrics.TasksDuplicate); got < 1 {
		t.Fatalf("b counted %v duplicates, want ≥1", got)
	}
	if n := runs.started(); n != 1 {
		t.Fatalf("adapter ran the task %d times, want exactly 1", n)
	}
	// b's journal still carries the single completed lifecycle of this task: the
	// refusal did not open a second one, and it did not overwrite the answer.
	rec, err := b.Store.GetTask(id)
	if err != nil {
		t.Fatalf("b journal: %v", err)
	}
	if rec.Status != tasks.Completed.String() {
		t.Fatalf("b's journal = %s after a duplicate delivery, want %s", rec.Status, tasks.Completed)
	}
	res, err := b.Store.GetResult(id)
	if err != nil {
		t.Fatalf("b result: %v", err)
	}
	if res.WorkerPeerID != b.ID().String() || res.Status != tasks.Completed.String() {
		t.Fatalf("b's stored answer changed after the duplicate: %+v", res)
	}
}

// ТЗ 17.2.8: задача с истёкшим TTL не распространяется.
//
// Что именно здесь проверяется: отказ обязан произойти на входе узла и быть
// названным по причине, а не «задача потерялась». Тот факт, что узел вообще
// получил конверт, ничего не доказывает: TasksReceived считается первой строкой
// OnTask, до всех проверок. Поэтому измерением служит журнал и исполнение —
// их у отказавшего узла быть не должно.
func TestIntegrationExpiredTTLDoesNotSpread(t *testing.T) {
	m := newMesh(t)
	a := m.node("a", []string{"research"}, nil)
	// b владеет навыком, но внешнюю работу не исполняет: значит, принятая им
	// задача обязана уйти дальше (к c), и её остановка по ttl становится
	// наблюдаемой. Без этого узла «не распространяется» нельзя отличить от
	// «нечему распространять».
	b := m.node("b", []string{"coding"}, func(cfg *config.Config) {
		cfg.Capabilities.AcceptExternalTasks = false
	})
	c := m.node("c", []string{"coding"}, nil)
	m.link(a, b)
	m.link(a, c)
	m.link(b, c)

	// (1) ttl уже израсходован: отказ на входе, причина названа, журнал пуст.
	env := authorEnvelope(t, a, tasks.BuildRequest{
		Instruction: "must not run", RequiredSkills: []string{"coding"}, TTL: 5, Priority: 5,
	}, "")
	// ttl входит в подписываемое тело (wire.TaskBody), поэтому «честный» ttl=0
	// требует новой подписи — ровно так выглядит устаревшая копия для получателя.
	env.Ttl = 0
	if err := security.NewSigner(a.id).SignTask(env); err != nil {
		t.Fatalf("resign expired: %v", err)
	}
	ack := sendDirect(t, a, b, env)
	if ack.GetStatus() != pb.AckStatus_ACK_STATUS_REJECTED {
		t.Fatalf("expired task ack = %s (%s), want REJECTED", ack.GetStatus(), ack.GetReason())
	}
	if !strings.Contains(ack.GetReason(), "ttl") {
		t.Fatalf("refusal does not name ttl: %q", ack.GetReason())
	}
	// Отказ произошёл до регистрации: узел не завёл ни lifecycle задачи, ни
	// чего-либо, что можно было бы переслать.
	assertNoTrace(t, b, env.GetTaskId())
	assertNoTrace(t, c, env.GetTaskId())

	// (2) ttl хватает на приём, но не на тот шаг, которого требует маршрут:
	// b не может исполнить сам и обязан послать дальше с ttl 0, который
	// отвергает уже c. b при этом регистрирует задачу (проверка маршрута
	// пройдена) и завершает её отказом — в отличие от первого случая, где
	// отказ происходит до регистрации.
	env2 := authorEnvelope(t, a, tasks.BuildRequest{
		Instruction: "one hop too far", RequiredSkills: []string{"coding"}, TTL: 1, Priority: 5,
	}, "")
	ack2 := sendDirect(t, a, b, env2)
	if ack2.GetStatus() != pb.AckStatus_ACK_STATUS_REJECTED {
		t.Fatalf("short-ttl task ack = %s (%s), want REJECTED", ack2.GetStatus(), ack2.GetReason())
	}
	if !strings.Contains(ack2.GetReason(), "ttl") {
		t.Fatalf("short-ttl refusal lost its cause: %q", ack2.GetReason())
	}
	// Распространение остановилось: ни один узел не исполнил задачу.
	recB := m.waitJournal(b, env2.GetTaskId(), func(r *storage.TaskRecord) bool {
		return r.Status == tasks.Rejected.String()
	})
	if recB.Attempts > 1 {
		t.Fatalf("b tried to delegate %d times, want ≤1 with no hop left", recB.Attempts)
	}
	assertNoTrace(t, c, env2.GetTaskId())
	for _, n := range []*testNode{b, c} {
		if got := testutil.ToFloat64(n.Metrics.TasksCompleted); got != 0 {
			t.Fatalf("%s completed %v tasks from a task with no hop left", n.name, got)
		}
	}
}

// ТЗ 17.4.1–2: конверт без подписи и конверт с переписанным содержимым.
//
// Второй случай — тот, ради которого существует origin_signature. Злоумышленник
// на пути владеет ключом отправителя (своего или украденного), подписывает тело
// заново и пересчитывает digest, поэтому каждая проверка «на входе» проходит:
// Validate сверяет digest и он совпадает, sender_peer_id совпадает с потоком,
// транспортная подпись корректна. Ловить подмену должен именно авторский
// подпись-инициатора, и именно её тест и проверяет на живом транспорте.
func TestIntegrationUnsignedAndRewrittenTasksAreRefused(t *testing.T) {
	m := newManualMesh(t)
	a := m.node("a", []string{"research"}, nil)
	runs := &countingAdapter{}
	b := m.node("b", []string{"coding"}, nil, runs.wrap())
	m.link(a, b)

	// (1) Подписан только автором, транспортной подписи отправителя нет.
	unsigned := tasks.Build(a.ID().String(), tasks.NewID(), tasks.BuildRequest{
		Instruction: "unsigned", RequiredSkills: []string{"coding"}, TTL: 5, Priority: 5,
	}, time.Now().UTC())
	if err := security.NewSigner(a.id).SignTaskOrigin(unsigned); err != nil {
		t.Fatalf("%s: sign origin: %v", a.name, err)
	}
	ack := sendDirect(t, a, b, unsigned)
	if ack.GetStatus() != pb.AckStatus_ACK_STATUS_REJECTED {
		t.Fatalf("unsigned task ack = %s (%s), want REJECTED", ack.GetStatus(), ack.GetReason())
	}
	if !strings.Contains(ack.GetReason(), "unsigned") {
		t.Fatalf("refusal does not name the missing signature: %q", ack.GetReason())
	}
	assertNoTrace(t, b, unsigned.GetTaskId())

	// (2) Полностью «валидный» конверт, у которого по дороге переписали
	// формулировку: digest пересчитан, транспортная подпись наведена заново.
	rewritten := authorEnvelope(t, a, tasks.BuildRequest{
		Instruction: "harmless original text", RequiredSkills: []string{"coding"}, TTL: 5, Priority: 5,
	}, "")
	rewritten.Payload.Instruction = "do something else entirely"
	rewritten.ContextDigest = tasks.ContentDigest(rewritten)
	if err := security.NewSigner(a.id).SignTask(rewritten); err != nil {
		t.Fatalf("%s: re-sign tampered: %v", a.name, err)
	}
	ack2 := sendDirect(t, a, b, rewritten)
	if ack2.GetStatus() != pb.AckStatus_ACK_STATUS_REJECTED {
		t.Fatalf("rewritten task ack = %s (%s), want REJECTED", ack2.GetStatus(), ack2.GetReason())
	}
	if !strings.Contains(ack2.GetReason(), "origin signature") {
		t.Fatalf("the rewrite was caught by %q, want the authorship check", ack2.GetReason())
	}
	assertNoTrace(t, b, rewritten.GetTaskId())
	if n := runs.started(); n != 0 {
		t.Fatalf("the worker executed %d refused task(s)", n)
	}

	// Контроль: тот же канал, те же узлы — честная задача исполняется. Значит,
	// отказ выше вызван проверкой, а не сломанной связью.
	id := submitTask(t, a, "harmless original text", []string{"coding"}, 5)
	completedFrom(t, a, id, b)
}

// ТЗ 17.4.3: при политике limited узел, которому не доверяют, задач не получает.
//
// Проверяется на узле, который прошёл рукопожатие до конца: он уже не чужак
// (policy поднял его до known), и всё равно обязан получить отказ, пока планка
// доверия выше. Это честная форма требования — «неизвестный» в limited означает
// «не дорос до требуемого уровня», а не «никогда не соединялся».
func TestIntegrationTaskAdmissionFollowsTrustPolicy(t *testing.T) {
	m := newManualMesh(t)
	a := m.node("a", []string{"research"}, nil)
	b := m.node("b", []string{"coding"}, func(cfg *config.Config) {
		cfg.Security.MinTrustForTasks = "trusted"
	})
	m.link(a, b)

	if got := b.Policy.TrustOf(a.ID()); got != security.TrustKnown {
		t.Fatalf("a's standing on b = %s, want known: the handshake must have completed, so "+
			"the refusal below can only come from the trust bar", got)
	}
	env := authorEnvelope(t, a, tasks.BuildRequest{
		Instruction: "not for you", RequiredSkills: []string{"coding"}, TTL: 5, Priority: 5,
	}, "")
	ack := sendDirect(t, a, b, env)
	if ack.GetStatus() != pb.AckStatus_ACK_STATUS_REJECTED {
		t.Fatalf("ack = %s (%s), want REJECTED below the trust bar", ack.GetStatus(), ack.GetReason())
	}
	if !strings.Contains(ack.GetReason(), "not trusted") {
		t.Fatalf("refusal does not name trust: %q", ack.GetReason())
	}
	// Отказ происходит до dedup-заявки и до регистрации: узел не взял задачу.
	assertNoTrace(t, b, env.GetTaskId())

	// Позитивный контроль: опускаем планку до уровня, которого узел уже достиг.
	// Связь, подписи и содержимое не менялись — изменилось только решение
	// оператора, и вместе с ним исход.
	b.Policy.SetPosture(security.ModeLimited, security.TrustKnown)
	id := submitTask(t, a, "summarize the findings", []string{"coding"}, 5)
	completedFrom(t, a, id, b)
}

// ТЗ 17.4.4: заблокированный узел не может подключиться.
//
// Отказ обязан произойти на уровне транспорта, а не задачи: блокировка действует
// до появления потока, поэтому заблокированный пир не получает даже права
// спросить навыки.
//
// Измерение — состояние соединения и таблиц, а не возвращаемое значение
// Connect. Замер показал, что они значат разное в двух направлениях: когда
// блокирует инициатор, его собственный гейтер отсекает попытку до рукопожатия и
// Connect падает с явной причиной; когда блокирует принимающая сторона, TCP уже
// установлено с точки зрения инициатора, обрыв происходит сразу после Noise, и
// Connect успевает вернуть nil. Но в обоих случаях наблюдаемый итог одинаков и
// именно он требует по ТЗ: живого соединения нет, в таблице соседей пира нет,
// поток не открывается.
func TestIntegrationBlockedPeerCannotConnect(t *testing.T) {
	t.Run("responder blocks", func(t *testing.T) {
		m := newManualMesh(t)
		a := m.node("a", []string{"research"}, nil)
		b := m.node("b", []string{"coding"}, nil)
		b.Policy.SetListed(a.ID(), false)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err := a.Host.Connect(ctx, b.Host.AddrInfo())
		cancel()
		// Settling time: the claim is that nothing survives, and "nothing yet"
		// needs a moment before it means anything.
		time.Sleep(time.Second)
		if a.Host.IsConnected(b.ID()) || b.Host.IsConnected(a.ID()) {
			t.Fatalf("the blocked pair holds a live connection despite the refusal (err %v)", err)
		}
		if m.knows(b, a) {
			t.Fatal("the blocked peer reached b's neighbour table")
		}
		// The refusal is at the transport, so no protocol message is exchanged:
		// even a task cannot be offered, let alone accepted.
		sctx, scancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer scancel()
		env := authorEnvelope(t, a, tasks.BuildRequest{
			Instruction: "never delivered", RequiredSkills: []string{"coding"}, TTL: 5, Priority: 5,
		}, "")
		if _, err := a.Service.SendTask(sctx, b.Host.AddrInfo(), env); err == nil {
			t.Fatal("a task stream opened to a node that blocks this peer")
		}
		assertNoTrace(t, b, env.GetTaskId())

		// Negative control: same addresses, same keys, the block lifted — the
		// handshake completes. So the refusal above came from the operator's
		// list and not from a broken transport.
		b.Policy.SetListed(a.ID(), true)
		m.link(a, b)
	})

	t.Run("dialer blocks", func(t *testing.T) {
		m := newManualMesh(t)
		a := m.node("a", []string{"research"}, nil)
		b := m.node("b", []string{"coding"}, nil)
		a.Policy.SetListed(b.ID(), false)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err := a.Host.Connect(ctx, b.Host.AddrInfo())
		cancel()
		if err == nil {
			t.Fatal("a dialled a peer it blocks itself")
		}
		if !strings.Contains(err.Error(), "gater") {
			t.Fatalf("the dial failed as %v, want the local gate as the named cause", err)
		}
		if a.Host.IsConnected(b.ID()) {
			t.Fatal("the dialer keeps a connection it refused")
		}
		// Blocking outbound must not blind the other side into a half-open link.
		time.Sleep(time.Second)
		if b.Host.IsConnected(a.ID()) {
			t.Fatal("b accepted a connection a refused to complete")
		}
	})
}

// ТЗ 17.4.6: превышение лимита запросов ведёт к отказу.
func TestIntegrationRateLimitRefusesExcessTasks(t *testing.T) {
	m := newManualMesh(t)
	a := m.node("a", []string{"research"}, nil)
	b := m.node("b", []string{"coding"}, func(cfg *config.Config) {
		cfg.Security.RateLimit = config.RateLimitConfig{RequestsPerSecond: 1, Burst: 1}
	})
	m.link(a, b)

	// Задачи обязаны быть разными: лимитер стоит до dedup, и повторный id
	// отвергся бы по другой причине, измерив не то, что проверяется.
	var limited, accepted int
	for i := 0; i < 6; i++ {
		env := authorEnvelope(t, a, tasks.BuildRequest{
			Instruction: fmt.Sprintf("burst item %d", i), RequiredSkills: []string{"coding"},
			TTL: 5, Priority: 5,
		}, "")
		ack := sendDirect(t, a, b, env)
		switch {
		case ack.GetStatus() == pb.AckStatus_ACK_STATUS_REJECTED &&
			strings.Contains(ack.GetReason(), "rate limit"):
			limited++
			assertNoTrace(t, b, env.GetTaskId())
		case ack.GetStatus() == pb.AckStatus_ACK_STATUS_QUEUED:
			accepted++
		default:
			t.Fatalf("burst item %d: unexpected ack %s (%s)", i, ack.GetStatus(), ack.GetReason())
		}
	}
	if limited == 0 {
		t.Fatal("six tasks through a 1 req/s · burst 1 gate were all accepted")
	}
	if accepted == 0 {
		t.Fatal("the rate limiter refused the very first task: nothing got through to measure")
	}
	if got := testutil.ToFloat64(b.Metrics.TasksRejected); got < float64(limited) {
		t.Fatalf("b counted %v rejections, want ≥%d", got, limited)
	}
	// Лимит защищает узел от потока задач, а не калечит его: принятые исполняются.
	m.wait("b settles its accepted work", func() bool { return b.Manager.RunningCount() == 0 })
	if got := testutil.ToFloat64(b.Metrics.TasksCompleted); got < float64(accepted) {
		t.Fatalf("b completed %v of %d accepted tasks", got, accepted)
	}
}

// ТЗ 6.12.1: «при повторной попытке выбор соседей учитывает, что они уже
// получили задачу». Выбор в этом mesh детерминирован, поэтому без исключения
// отработанных кандидатов «повтор» — это второй вопрос тому же узлу, а второй,
// способный исполнить, так и остаётся неиспользованным.
//
// Причина отказа выбрана постоянная (доверие, как в проверке 17.4.3), иначе
// повтор мог бы «вылечить» её сам: лимит скорости восстанавливает токен за
// секунду. Порядок кандидатов тоже сделан устойчивым: отказ записывает соседу
// неудачу, а это всего 0.0225 очка, поэтому c заранее отдан медленный измеренный
// RTT (вес 0.10) — разрыв в 0.05 переживает неудачу b, и без механизма
// исключений все три попытки ушли бы в b, что тест и ловит.
func TestIntegrationRetryAsksAFreshPeerAfterARefusal(t *testing.T) {
	m := newManualMesh(t)
	a := m.node("a", []string{"research"}, func(cfg *config.Config) {
		// Одна задача на попытку: иначе первая же попытка спросила бы обоих, и
		// сценарий перестал бы различать «спросил нового» и «повторил тому же».
		cfg.Tasks.Forwarding.MaxParallelCandidates = 1
		cfg.Tasks.Forwarding.RetryInterval = config.Duration(100 * time.Millisecond)
	})
	// b владеет навыком, но держит планку доверия выше, чем достиг a: отказ
	// гарантированный и не зависит от нагрузки.
	b := m.node("b", []string{"coding"}, func(cfg *config.Config) {
		cfg.Security.MinTrustForTasks = "trusted"
	})
	ran := &countingAdapter{}
	c := m.node("c", []string{"coding"}, nil, ran.wrap())
	m.link(a, b)
	m.link(a, c)
	a.Table.RecordRTT(c.ID(), 3*time.Second)

	// Порядок кандидатов — то, без чего сценарий ничего не проверяет: отказавший
	// обязан стоять первым, иначе ретрай ушёл бы к другому и без исключений.
	pref := a.Table.Select([]string{"coding"}, a.Policy, a.ID(), 5, nil)
	if len(pref) != 2 {
		t.Fatalf("a sees %d candidates for coding, want 2: %+v", len(pref), pref)
	}
	if pref[0].Neighbor.PeerID != b.ID() {
		t.Fatalf("top candidate is %s, want the refusing peer %s", pref[0].Neighbor.PeerID, b.name)
	}
	if got := b.Policy.TrustOf(a.ID()); got != security.TrustKnown {
		t.Fatalf("a's standing on b = %s, want known: the refusal below must come from the trust bar", got)
	}

	id := submitTask(t, a, "retried onto a fresh peer", []string{"coding"}, 6)
	completedFrom(t, a, id, c)
	if got := ran.started(); got != 1 {
		t.Fatalf("c executed the task %d times, want exactly 1", got)
	}
	// Ровно один отказ и ровно одна успешная делегация: первая попытка ушла в b и
	// получила «not trusted», вторая — в c. Три отказа означали бы, что повтор
	// снова спрашивал того же соседа.
	rejected := testutil.ToFloat64(a.Metrics.ForwardAttempts.WithLabelValues("rejected"))
	if rejected != 1 {
		t.Fatalf("a recorded %v refused delegation attempts, want exactly 1", rejected)
	}
	if got := testutil.ToFloat64(a.Metrics.TasksDelegated); got != 1 {
		t.Fatalf("a delegated the task %v times, want exactly 1", got)
	}
	// b так и не взял задачу: повтор его не касался, ни журнала, ни результата.
	assertNoTrace(t, b, id)
}

// ТЗ 17.4.5 «реле не может расшифровать содержимое задачи» + ТЗ 7.2 «не
// допускается передача задач в открытом виде».
//
// Формулировка ТЗ предполагает сквозное шифрование полезной нагрузки, которого в
// mesh нет и быть не может: каждый исполнитель читает инструкцию, чтобы её
// выполнить, а `route_stack` открыт. Реальные границы здесь две, и обе проверяемы:
//
//  1. каждое ребро между узлами шифровано (Noise для TCP, TLS 1.3 для QUIC) —
//     иначе «в открытом виде» был бы сам канал, и наблюдатель сети читал бы задачи;
//  2. содержимое задачи не попадает ни в один канал, доступный узлу, который её
//     не получал. Именно это и делает «реле» слепым: не магическое шифрование, а
//     то, что служебная плоскость оперирует именами навыков и адресами, а не
//     текстом.
//
// Проверка (2) перечисляет служебные ответы узла полными protobuf-байтами, а не
// «посмотрели бы в журнал»: если отладочное поле с инструкцией когда-нибудь
// приедет в gossip или в ответ peer-exchange, тест это поймает.
func TestIntegrationTaskContentStaysOffTheServicePlane(t *testing.T) {
	const secret = "confidential instruction: rotate the plutonium keys"

	m := newManualMesh(t)
	a := m.node("a", []string{"research"}, nil)
	b := m.node("b", []string{"coding"}, nil)
	// d — сосед b и потенциальное реле: он участвует в mesh, но не в этой задаче.
	d := m.node("d", []string{"translate"}, nil)
	m.link(a, b)
	m.link(b, d)

	id := submitTask(t, a, secret, []string{"coding"}, 5)
	completedFrom(t, a, id, b)

	// (1) Шифрование канала: у обоих узлов, связанных с задачей, каждое живое
	// ребро использует security-протокол. Плоское «он есть» — тоже измерение:
	// голому TCP-перехвату здесь взяться не откуда.
	for _, e := range []struct{ from, to *testNode }{{a, b}, {b, a}, {b, d}, {d, b}} {
		conns := e.from.Host.Underlying().Network().ConnsToPeer(e.to.ID())
		if len(conns) == 0 {
			t.Fatalf("%s → %s: no connection to inspect", e.from.name, e.to.name)
		}
		for _, cn := range conns {
			switch sec := string(cn.ConnState().Security); sec {
			case "/noise", "/tls/1.0.0":
			default:
				t.Fatalf("%s → %s: connection secured by %q, want noise or tls",
					e.from.name, e.to.name, sec)
			}
		}
	}

	// (2) Узел вне пути не знает задачи ничего, кроме того, что её нет у него.
	assertNoTrace(t, d, id)

	// Полные protobuf-байты всего, что d публикует и на что отвечает.
	var payloads [][]byte
	if st := d.peerState(); st != nil {
		raw, err := proto.Marshal(st)
		if err != nil {
			t.Fatalf("marshal peer state: %v", err)
		}
		payloads = append(payloads, raw)
	}
	if caps := d.Capabilities(); caps != nil {
		raw, err := proto.Marshal(caps)
		if err != nil {
			t.Fatalf("marshal capabilities: %v", err)
		}
		payloads = append(payloads, raw)
	}
	// Ответы служебного RPC, снятые с живого узла: ровно те, что d отдаёт соседу.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, req := range []*pb.RpcRequest{
		{Kind: &pb.RpcRequest_PeerExchange{PeerExchange: &pb.PeerExchangeRequest{Count: 32}}},
		{Kind: &pb.RpcRequest_SkillLookup{SkillLookup: &pb.SkillLookupRequest{
			Skills: []string{"coding"}, FullRefresh: true, RelayBudget: 2}}},
		{Kind: &pb.RpcRequest_SkillsSync{SkillsSync: &pb.SkillsSyncRequest{}}},
		{Kind: &pb.RpcRequest_Capabilities{Capabilities: &pb.CapabilitiesRequest{}}},
	} {
		resp, err := b.Service.RPC(ctx, d.ID(), req)
		if err != nil {
			t.Fatalf("%s: rpc to d: %v", b.name, err)
		}
		raw, err := proto.Marshal(resp)
		if err != nil {
			t.Fatalf("marshal rpc response: %v", err)
		}
		payloads = append(payloads, raw)
	}
	// Достижимое наблюдение: журнал, статус и метрики узла.
	for _, rec := range d.journalForTest(t) {
		payloads = append(payloads, []byte(fmt.Sprintf("%+v", rec)))
	}
	payloads = append(payloads, []byte(fmt.Sprintf("%+v", d.Status())))
	mfs, err := d.Metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		payloads = append(payloads, []byte(mf.String()))
	}

	for i, p := range payloads {
		if strings.Contains(string(p), secret) {
			t.Fatalf("payload %d of %d discloses the task instruction on the service plane "+
				"of a node that never received it", i, len(payloads))
		}
	}
	// Тот же тест с обратным знаком: текст задачи действительно существует в
	// системе и проходит по узлам, которые вправе его видеть. Иначе «не нашли»
	// ничего не значило бы.
	if rec, err := b.Store.GetTask(id); err != nil || rec == nil {
		t.Fatalf("b (worker of the path) has no journal record: %v", err)
	}
	res, err := a.Store.GetResult(id)
	if err != nil {
		t.Fatalf("origin result: %v", err)
	}
	if !strings.Contains(res.Text, secret) {
		t.Fatalf("the answer does not echo the instruction (%q): the task never really carried it", res.Text)
	}
}

// journalForTest dumps every record this node kept, as the flat text the
// disclosure check reads.
func (n *testNode) journalForTest(t *testing.T) []*storage.TaskRecord {
	t.Helper()
	recs, err := n.Store.ListTasks(500, "")
	if err != nil {
		t.Fatalf("%s: list journal: %v", n.name, err)
	}
	return recs
}

// assertNoTrace checks that a node never took ownership of a task: no journal
// record and no result. This is the observable form of «не распространяется».
func assertNoTrace(t *testing.T, n *testNode, id string) {
	t.Helper()
	if _, err := n.Store.GetTask(id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("%s journalled task %s it must have refused (err %v)", n.name, id, err)
	}
	if _, err := n.Store.GetResult(id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("%s stored a result for task %s it must have refused (err %v)", n.name, id, err)
	}
}

// ---------- test doubles ----------

// gateAdapter models an agent that is wedged: it never returns, not even when
// its context is canceled. That is deliberate. A cancellation-respecting stub
// would let the worker report its own failure as soon as the node starts to
// shut down, and the assertion would then depend on which of the two signals
// reaches the origin first. Parking the goroutine makes "the worker died" the
// only thing that can resolve the task, so what the origin reports is the
// behaviour under test — the loss detected over the dead connection.
type gateAdapter struct {
	inner   picoclaw.Adapter
	gate    chan struct{}
	started chan struct{}
	once    sync.Once
}

func newGateAdapter() *gateAdapter {
	return &gateAdapter{gate: make(chan struct{}), started: make(chan struct{})}
}

// wrap decorates the node's executor; pass it to mesh.node.
func (g *gateAdapter) wrap() func(picoclaw.Adapter) picoclaw.Adapter {
	return func(inner picoclaw.Adapter) picoclaw.Adapter {
		g.inner = inner
		return g
	}
}

func (g *gateAdapter) Name() string { return "gated-" + g.inner.Name() }

func (g *gateAdapter) Healthy(ctx context.Context) error { return g.inner.Healthy(ctx) }

func (g *gateAdapter) Capabilities(ctx context.Context) ([]string, error) {
	return g.inner.Capabilities(ctx)
}

func (g *gateAdapter) Close() error {
	if g.inner == nil {
		return nil
	}
	return g.inner.Close()
}

func (g *gateAdapter) Execute(_ context.Context, _ picoclaw.Request) (*picoclaw.Response, error) {
	g.once.Do(func() { close(g.started) })
	<-g.gate
	return nil, errors.New("gate adapter: never released")
}

// entered reports that the adapter was called at least once.
func (g *gateAdapter) entered() bool {
	select {
	case <-g.started:
		return true
	default:
		return false
	}
}

// countingAdapter records how many times the executor was invoked.
type countingAdapter struct {
	mu    sync.Mutex
	inner picoclaw.Adapter
	n     int
}

func (c *countingAdapter) wrap() func(picoclaw.Adapter) picoclaw.Adapter {
	return func(inner picoclaw.Adapter) picoclaw.Adapter {
		c.mu.Lock()
		c.inner = inner
		c.mu.Unlock()
		return c
	}
}

func (c *countingAdapter) Name() string { return "counted-" + c.adapter().Name() }

func (c *countingAdapter) Healthy(ctx context.Context) error { return c.adapter().Healthy(ctx) }

func (c *countingAdapter) Capabilities(ctx context.Context) ([]string, error) {
	return c.adapter().Capabilities(ctx)
}

func (c *countingAdapter) Close() error { return c.adapter().Close() }

func (c *countingAdapter) Execute(ctx context.Context, req picoclaw.Request) (*picoclaw.Response, error) {
	c.mu.Lock()
	c.n++
	inner := c.inner
	c.mu.Unlock()
	return inner.Execute(ctx, req)
}

func (c *countingAdapter) started() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// adapter resolves the wrapped executor for the read-only parts of the
// interface. Resolution is lazy because the node asks its adapter for skills
// while it is being built, i.e. before wrap() has run.
func (c *countingAdapter) adapter() picoclaw.Adapter {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inner
}

// TestIntegrationScheduledTriggerInjectsATask covers ТЗ 6.6.1 п.4 end to end:
// a cron schedule declared in the node config runs through the real scheduler,
// the real submit path and the real router, and comes back as a signed result
// from the peer that executed it. The firing is driven by an explicit Tick on a
// "* * * * *" schedule, so the scenario does not race the wall clock.
func TestIntegrationScheduledTriggerInjectsATask(t *testing.T) {
	m := newManualMesh(t)
	// "a" must not be able to run the job itself: its skill set deliberately
	// excludes "general", which is the wildcard that would keep the task local
	// and hide the routing leg this scenario is about.
	a := m.node("a", []string{"ops"}, func(cfg *config.Config) {
		cfg.Triggers = []config.TriggerConfig{{
			ID: "sweep", Schedule: "* * * * *",
			Job: config.JobConfig{
				Instruction: "scheduled sweep", RequiredSkills: []string{"coding"}, TTL: 3,
			},
		}}
	})
	b := m.node("b", []string{"coding"}, nil)
	m.link(a, b)

	// Take the loop out of the picture: from here on every firing is a Tick the
	// scenario initiates, so the minute boundary cannot race the assertions.
	sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer scancel()
	if err := a.Scheduler.Stop(sctx); err != nil {
		t.Fatalf("stop scheduler: %v", err)
	}

	views, err := a.Scheduler.Views(time.Now().UTC())
	if err != nil {
		t.Fatalf("Views: %v", err)
	}
	if len(views) != 1 || views[0].ID != "sweep" || !views[0].Enabled {
		t.Fatalf("config trigger not visible: %+v", views)
	}

	// "* * * * *" fires on the next minute boundary as well as here; whichever
	// pass got there first, exactly one run per minute is the contract, so the
	// assertion reads the recorded state rather than the return of one tick.
	a.Scheduler.Tick(context.Background())
	var run triggers.View
	m.wait("scheduled run recorded", func() bool {
		vs, err := a.Scheduler.Views(time.Now().UTC())
		if err != nil || len(vs) != 1 || vs[0].LastTaskID == "" {
			return false
		}
		run = vs[0]
		return true
	})
	if run.RunCount < 1 || run.LastError != "" {
		t.Fatalf("trigger state after the run = %+v", run)
	}
	completedFrom(t, a, run.LastTaskID, b)

	// A config-declared schedule is YAML's, not the store's: it must not leak
	// into the persisted list, and it cannot be deleted through the API facade.
	stored, err := triggers.List(a.Store)
	if err != nil {
		t.Fatalf("List(store): %v", err)
	}
	if len(stored) != 0 {
		t.Fatalf("config trigger leaked into the store: %+v", stored)
	}
	if removed, err := a.Scheduler.Delete("sweep"); err != nil || removed {
		t.Fatalf("Delete of a config id = %v, %v; want a no-op", removed, err)
	}

	// A stored trigger created over the API is persisted and shares the listing
	// with the config one; it may not shadow a config-declared id.
	if _, err := a.Scheduler.Add(&triggers.Trigger{ID: "manual", Schedule: "0 3 * * *",
		Job: triggers.Job{Instruction: "nightly", RequiredSkills: []string{"general"}}}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	views, err = a.Scheduler.Views(time.Now().UTC())
	if err != nil || len(views) != 2 {
		t.Fatalf("Views after Add = %+v (%v), want sweep+manual", views, err)
	}
	if st, err := triggers.List(a.Store); err != nil || len(st) != 1 || st[0].ID != "manual" {
		t.Fatalf("stored list = %+v (%v), want only the manual schedule", st, err)
	}
	if _, err := a.Scheduler.Add(&triggers.Trigger{ID: "sweep", Schedule: "0 4 * * *",
		Job: triggers.Job{Instruction: "shadow"}}); err == nil {
		t.Fatal("an API write shadowed a config-declared id")
	}
}

// ТЗ 6.5.3 — исключение по молчанию проверено адресным опросом. Сосед, который
// жив, но ничего не присылает (gossip best-effort, а тут он ещё и выключен), не
// должен исчезать из вида маршрутизации: раньше он вычёркивался по одному лишь
// таймауту, и оператор видел «no eligible peers reachable» там, где исполнитель
// был на месте. Мёртвый сосед при этом обязан быть удалён — иначе таблица
// набита недосягаемыми записями.
func TestIntegrationSilentButLivingPeerSurvivesEviction(t *testing.T) {
	m := newManualMesh(t)
	// suspectAfter = failure_timeout*3; heartbeat задаёт период прохода.
	tune := func(cfg *config.Config) {
		// Validate requires failure_timeout > heartbeat; the product (×3) is the
		// suspect threshold, the heartbeat is how often the sweep runs.
		cfg.Discovery.Gossip.FailureTimeout = config.Duration(900 * time.Millisecond)
		cfg.Discovery.Gossip.Heartbeat = config.Duration(300 * time.Millisecond)
		// Min=0 — иначе тонкий вид сам вызывает peer exchange, который обновляет
		// LastSeen, и «молчаливость» соседа перестанет быть тем, что проверяется.
		cfg.Neighbors.Min = 0
		cfg.Capabilities.SkillExchange.Enabled = false
	}
	a := m.node("a", []string{"ops"}, tune)
	b := m.node("b", []string{"coding"}, tune)
	m.link(a, b)

	// Фаза 1: время течёт, записей о b не приходит — он становится suspect'ом,
	// но отвечает на прямой ping, значит остаётся.
	isSuspect := func() bool {
		for _, s := range a.Table.Suspects(a.suspectAfter()) {
			if s.PeerID == b.ID() {
				return true
			}
		}
		return false
	}
	deadline := time.Now().Add(a.suspectAfter() + 20*time.Second)
	sawSuspect := false
	for time.Now().Before(deadline) {
		if _, ok := a.Table.Get(b.ID()); !ok {
			t.Fatal("b vanished from a's table while it was still connected and answering")
		}
		if isSuspect() {
			sawSuspect = true
		} else if sawSuspect {
			// Suspect-состояние снято: значит опрос прошёл, и запись пережила
			// молчание. Это и есть проверяемое поведение.
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !sawSuspect {
		t.Fatalf("b never became a suspect within %v: the probe path was not exercised",
			a.suspectAfter()+20*time.Second)
	}
	nb, ok := a.Table.Get(b.ID())
	if !ok {
		t.Fatal("the reconfirmed peer is gone from the table")
	}
	if !nb.Connected {
		t.Fatalf("a peer we just probed over a live connection reads as disconnected: %+v", nb)
	}

	// Фаза 2: тот же путь, но сосед действительно мёртв — опрос не получает
	// ответа, и запись обязана уйти.
	if err := b.Stop(context.Background()); err != nil {
		t.Fatalf("stop b: %v", err)
	}
	m.wait("a evicts the unreachable peer", func() bool {
		_, still := a.Table.Get(b.ID())
		return !still
	})
}

func contains(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}

// TestIntegrationTraceContinuesAcrossNodes is the ТЗ 14.3 acceptance shape:
// a task submitted on A and executed on B produces ONE trace containing A's
// submit and B's receive/execute spans. The trace context rode the wire inside
// the signed labels — no protocol field, no side channel — and B's manager
// joined its spans to A's trace rather than starting its own. Both nodes run
// in this process, so one recorder sees both halves; the global OTel provider
// delegates to whoever was installed last, which is why the recorder goes in
// before the nodes start.
func TestIntegrationTraceContinuesAcrossNodes(t *testing.T) {
	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()
	sr := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
	})

	m := newMesh(t)
	a := m.node("a", []string{"research"}, nil)
	b := m.node("b", []string{"coding"}, nil)
	m.link(a, b)

	id := submitTask(t, a, "build the thing", []string{"coding"}, 5)
	completedFrom(t, a, id, b)

	// AwaitResult returns before the deferred span End()s unwind on either
	// node: poll until the expected three are in, or say honestly which is
	// missing.
	want := map[string]string{
		tracing.SpanSubmit:  "",
		tracing.SpanReceive: "",
		tracing.SpanExecute: "",
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		for _, s := range sr.Ended() {
			delete(want, s.Name())
		}
		if len(want) == 0 {
			break
		}
		if time.Now().After(deadline) {
			var seen []string
			for _, s := range sr.Ended() {
				seen = append(seen, s.Name())
			}
			t.Fatalf("missing spans %v; ended so far: %v", want, seen)
		}
		time.Sleep(20 * time.Millisecond)
	}

	var traceIDs map[string]string
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range sr.Ended() {
		switch s.Name() {
		case tracing.SpanSubmit, tracing.SpanReceive, tracing.SpanExecute:
			byName[s.Name()] = s
			if tid, seen := traceIDs[s.Name()]; seen && tid != s.SpanContext().TraceID().String() {
				t.Fatalf("two %s spans in different traces: %s vs %s", s.Name(), tid, s.SpanContext().TraceID())
			}
			if traceIDs == nil {
				traceIDs = map[string]string{}
			}
			traceIDs[s.Name()] = s.SpanContext().TraceID().String()
		}
	}
	sub, recv, exec := byName[tracing.SpanSubmit], byName[tracing.SpanReceive], byName[tracing.SpanExecute]
	if sub.SpanContext().TraceID() != recv.SpanContext().TraceID() ||
		sub.SpanContext().TraceID() != exec.SpanContext().TraceID() {
		t.Fatalf("trace broken across the hop: submit=%s receive=%s execute=%s",
			sub.SpanContext().TraceID(), recv.SpanContext().TraceID(), exec.SpanContext().TraceID())
	}
	// The remote parent recorded on B's receive span must be A's submit span:
	// that link is what a collector uses to stitch the two nodes into one
	// waterfall.
	if recv.Parent().SpanID() != sub.SpanContext().SpanID() {
		t.Fatalf("receive parent = %s, want a's submit span %s", recv.Parent().SpanID(), sub.SpanContext().SpanID())
	}
	if exec.Parent().SpanID() != recv.SpanContext().SpanID() {
		t.Fatalf("execute parent = %s, want b's receive span %s", exec.Parent().SpanID(), recv.SpanContext().SpanID())
	}
	// task_id — the cross-cutting identifier ТЗ 14.3 does require — is on all
	// three, so a journal query and a trace query join on the same key.
	for _, s := range []sdktrace.ReadOnlySpan{sub, recv, exec} {
		var got string
		for _, attr := range s.Attributes() {
			if attr.Key == attribute.Key("zeptomesh.task_id") {
				got = attr.Value.AsString()
			}
		}
		if got != id {
			t.Fatalf("%s span zeptomesh.task_id = %q, want %q", s.Name(), got, id)
		}
	}
}

// TestIntegrationSearchTopicFindsUnacquaintedWorker is the ТЗ 6.9.5 п.5
// end-to-end shape: the task needs "research"; A cannot serve it, B cannot
// serve it, and A has never met C — the addressed widening is switched off,
// which is exactly the hole the epidemic plane exists for. A publishes a
// signed, skills-only request on the search topic; B (which met C) and C
// (which is C) can both answer; A adopts the named peer — dial plus verified
// signed capabilities — and routes the task there.
func TestIntegrationSearchTopicFindsUnacquaintedWorker(t *testing.T) {
	m := newManualMesh(t)
	topic := func(cfg *config.Config) {
		cfg.Tasks.Forwarding.SearchRelay.Topic.Enabled = true
		cfg.Tasks.Forwarding.SearchRelay.Topic.AnswerCooldown = config.Duration(2 * time.Second)
	}
	a := m.node("search-a", []string{"coding"}, topic)
	b := m.node("search-b", []string{"coding"}, topic)
	c := m.node("search-c", []string{"research"}, topic)

	m.link(a, b)
	m.link(b, c)
	if m.knows(a, c) {
		t.Fatal("scenario premise broken: A must not already know C")
	}
	// The topic mesh forms by GRAFT at the ~1 s heartbeat, and the first
	// publish before it lands is silently lost. A fixed sleep passed on an idle
	// box and flaked when other suites shared the four cores, so wait on the
	// fact itself: probe the plane with a search until somebody answers. The
	// probe does not populate the routing table (adoption happens only in the
	// task path), so the task below still has to discover C through the topic.
	deadline := time.Now().Add(30 * time.Second)
	var warm bool
	for time.Now().Before(deadline) && !warm {
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		for _, rec := range a.SearchTopic.Search(sctx, []string{"research"}) {
			if rec.GetPeerId() == c.ID().String() {
				warm = true
				break
			}
		}
		cancel()
		if !warm {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if !warm {
		t.Fatal("search topic never grafted: A got no answer for research within 30 s")
	}

	id := submitTask(t, a, "summarise the paper", []string{"research"}, 4)
	completedFrom(t, a, id, c)

	// The topic really carried the discovery: A published, somebody answered.
	if got := testutil.ToFloat64(a.Metrics.SearchTopicRequests); got < 1 {
		t.Fatalf("A published %v search-topic requests, want >= 1", got)
	}
	answered := testutil.ToFloat64(a.Metrics.SearchTopicAnswers) +
		testutil.ToFloat64(b.Metrics.SearchTopicAnswers) +
		testutil.ToFloat64(c.Metrics.SearchTopicAnswers)
	if answered < 1 {
		t.Fatalf("no node answered the search (a=%v b=%v c=%v), want >= 1",
			testutil.ToFloat64(a.Metrics.SearchTopicAnswers),
			testutil.ToFloat64(b.Metrics.SearchTopicAnswers),
			testutil.ToFloat64(c.Metrics.SearchTopicAnswers))
	}
	// A itself must not answer its own request (self-echo is skipped).
	if got := testutil.ToFloat64(a.Metrics.SearchTopicAnswers); got != 0 {
		t.Fatalf("A answered its own search %v times, want 0 (self-echo skip)", got)
	}
	rec := m.waitJournal(a, id, func(r *storage.TaskRecord) bool { return r.WorkerPeerID == c.ID().String() })
	if rec == nil {
		t.Fatal("A's journal never recorded C as the worker")
	}
}

// TestIntegrationSearchTopicStaysSilentWhenDisabled is the negative control
// for the same scenario: with the topic plane off (and no addressed relay),
// the task must fail with no worker — proving the previous test's success
// came from the topic rather than from incidental reachability.
func TestIntegrationSearchTopicStaysSilentWhenDisabled(t *testing.T) {
	m := newManualMesh(t)
	a := m.node("silence-a", []string{"coding"}, nil)
	b := m.node("silence-b", []string{"coding"}, nil)
	c := m.node("silence-c", []string{"research"}, nil)
	m.link(a, b)
	m.link(b, c)

	id := submitTask(t, a, "summarise the paper", []string{"research"}, 4)
	res := awaitResult(t, a, id)
	if res.GetStatus() == pb.TaskStatus_TASK_STATUS_COMPLETED {
		t.Fatalf("task completed without the search plane on a topology that cannot reach C: %+v", res)
	}
	if got := testutil.ToFloat64(a.Metrics.SearchTopicRequests); got != 0 {
		t.Fatalf("disabled topic published %v requests, want 0", got)
	}
}

// TestIntegrationScanLocal verifies the agent's environment sweep end to end:
// A scans its local scope, discovers B through the shared registry, dials it
// and verifies its signed capabilities, so B becomes a routable neighbour.
func TestIntegrationScanLocal(t *testing.T) {
	m := newMesh(t)
	a := m.node("scan-a", []string{"coding"}, nil)
	b := m.node("scan-b", []string{"research"}, nil)

	// A's registry is the source of truth on one host: it lists B once B has
	// published, so wait for that before scanning.
	m.wait("registry lists b", func() bool {
		recs, err := a.Registry.List()
		if err != nil {
			return false
		}
		for _, r := range recs {
			if r.PeerID == b.ID().String() {
				return true
			}
		}
		return false
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res := a.ScanNetwork(ctx, ScanRequest{Scopes: []string{"local"}})

	if res.Candidates == 0 {
		t.Fatalf("scan found no agents; report: %+v", res)
	}
	var found bool
	for _, ag := range res.Agents {
		if ag.PeerID == b.ID().String() {
			found = true
		}
	}
	if !found {
		t.Fatalf("scan did not list B: %+v", res.Agents)
	}
	// The scan must do more than enumerate: B ends up connected and
	// capabilities-verified, exactly like any other discovery path.
	m.wait("a connected to b after scan", func() bool { return m.handshook(a, b) })
	if !m.knows(a, b) {
		t.Fatalf("A's table does not know B after a successful scan")
	}
}

// TestIntegrationScanBlockedPeer checks the sweep honours the security policy:
// a blocked peer is never dialed, even though the scan finds it.
func TestIntegrationScanBlockedPeer(t *testing.T) {
	m := newMesh(t)
	a := m.node("scan-block-a", []string{"coding"}, nil)
	b := m.node("scan-block-b", []string{"research"}, nil)

	m.wait("registry lists b", func() bool {
		recs, err := a.Registry.List()
		if err != nil {
			return false
		}
		for _, r := range recs {
			if r.PeerID == b.ID().String() {
				return true
			}
		}
		return false
	})

	// A blocked peer is refused in every trust mode (AllowConnection treats
	// TrustBlocked as fatal), so the scan must leave it untouched.
	a.Policy.SetListed(b.ID(), false)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	res := a.ScanNetwork(ctx, ScanRequest{Scopes: []string{"local"}})

	for _, ag := range res.Agents {
		if ag.PeerID == b.ID().String() && ag.Connected {
			t.Fatalf("blocked peer B was connected by the scan: %+v", res.Agents)
		}
	}
	if a.Host.IsConnected(b.ID()) {
		t.Fatalf("blocked peer B is connected after the scan")
	}
}
