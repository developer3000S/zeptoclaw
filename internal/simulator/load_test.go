//go:build load

package simulator

// TestLoadSim is the entry point ТЗ 17.3 asks for: the same membership and routing
// protocol the node runs, driven at 100, 500 and 1000 nodes, with the four numbers
// ТЗ 16.4 puts ceilings on and the behaviours ТЗ 22 п.6 lists. It is behind a build
// tag because it is minutes of CPU, not milliseconds — the Makefile's test-load
// target is the supported way to run it, and it tees the whole markdown report to
// load-test-output.txt.
//
// Everything printed here is computed by the simulation in the same process. The
// environment overrides exist so a report can be reassembled at a different length
// or a different seed without editing code:
//
//	ZETOMESH_SIM_SIZES   comma-separated mesh sizes       (default 100,500,1000)
//	ZETOMESH_SIM_STEPS   idle:active:failure heartbeats   (default 40:40:60)
//	ZETOMESH_SIM_SEED    run seed                         (default 1)
//	ZETOMESH_SIM_REPORT  write markdown here               (default: stdout only)
//	ZETOMESH_SIM_CRASH   crash fraction, 0..1             (default 0.10)

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	envSizes  = "ZETOMESH_SIM_SIZES"
	envSteps  = "ZETOMESH_SIM_STEPS"
	envSeed   = "ZETOMESH_SIM_SEED"
	envReport = "ZETOMESH_SIM_REPORT"
	envCrash  = "ZETOMESH_SIM_CRASH"
	// envSyncNodes sizes the analytic-versus-exact full-sync cross-check, which the
	// boundaries section measures at a size where walking the flood is affordable.
	envSyncNodes = "ZETOMESH_SIM_SYNC_NODES"
)

func TestLoadSim(t *testing.T) {
	sizes := parseSizes(os.Getenv(envSizes))
	idle, active, failure := parseSteps(os.Getenv(envSteps))
	seed := parseSeed(os.Getenv(envSeed))
	crash := parseCrash(os.Getenv(envCrash))
	ph := Phases{IdleBeats: idle, ActiveBeats: active, FailureBeats: failure,
		CrashFraction: crash, InjectEveryBeats: DefaultPhases().InjectEveryBeats}

	if err := ph.Validate(); err != nil {
		t.Fatal(err)
	}
	p := DefaultParams()

	started := time.Now()
	results := make([]Result, 0, len(sizes))
	for _, n := range sizes {
		n := n
		t.Run(fmt.Sprintf("nodes=%d", n), func(t *testing.T) {
			simStart := time.Now()
			s, err := New(n, seed, p, ph)
			if err != nil {
				t.Fatalf("%d nodes: %v", n, err)
			}
			r, err := s.Run()
			if err != nil {
				t.Fatalf("%d nodes: %v", n, err)
			}
			// The cost of the run is a property of the harness, not of the protocol, so
			// it is recorded here rather than measured inside Sim. VmHWM is a process
			// high-water mark: the value after the last size is the value that describes
			// the whole run, which the report states instead of implying per-size precision.
			r.Wall = time.Since(simStart)
			r.PeakRSSKb = SelfPeakRSSKb()
			t.Logf("%d nodes: simulated %d heartbeats (%d s) in %v of wall clock, "+
				"peak RSS of the test process %.2f GiB",
				n, ph.beats(), ph.beats()*int(s.beatSeconds()), r.Wall.Round(time.Millisecond),
				float64(r.PeakRSSKb)/1048576)
			results = append(results, *r)
		})
	}

	// The boundaries section has to quote the analytic-versus-exact full-sync error, so
	// it is measured here, in this process, at sizes where the exact walk is affordable.
	// Nothing about the headline runs is changed by it: each reading is a separate pair of
	// meshes with its own schedule. The error grows with the mesh, so one reading would be
	// an anecdote — the smallest size flatters the approximation most.
	var boundaries []Boundary
	for _, n := range boundarySizes() {
		boundaries = append(boundaries, measureFullSyncBoundary(t, n))
	}

	meta := Meta{
		FullSyncBoundaries: boundaries,
		Machine:            shellOut("uname", "-a"),
		GoVersion:          shellOut("go", "version"),
		NProc:              fmt.Sprintf("%d (%s/%s)", runtime.NumCPU(), runtime.GOOS, runtime.GOARCH),
		Elapsed:            time.Since(started).Round(time.Second).String(),
		Commands: []string{
			"export PATH=$PATH:/usr/local/go/bin:/root/go/bin",
			"make test-load",
			"# или напрямую, с теми же параметрами:",
			"go test -mod=mod -tags=load -timeout 60m -run TestLoadSim -v ./internal/simulator/...",
			fmt.Sprintf("#   ZETOMESH_SIM_SIZES=%s ZETOMESH_SIM_STEPS=%d:%d:%d ZETOMESH_SIM_SEED=%d",
				strings.Join(itoaAll(sizes), ","), idle, active, failure, seed),
		},
	}
	md := Markdown(results, meta)
	fmt.Println(md)
	if path := strings.TrimSpace(os.Getenv(envReport)); path != "" {
		if err := os.WriteFile(path, []byte(md), 0o644); err != nil {
			t.Fatalf("write report: %v", err)
		}
		fmt.Fprintf(os.Stderr, "simulator: отчёт записан в %s\n", path)
	}

	// The verdict is the point of the run, so a ceiling missed is a test failure:
	// it keeps a regression in the traffic budget from being noticed only by
	// someone reading a markdown file.
	for _, r := range results {
		t.Logf("%d узлов: покой %.1f Кбит/с (норматив ≤%.0f) — %s; активность %.1f Кбит/с (норматив ≤%.0f) — %s",
			r.Nodes, r.Idle.KbitService, r.Idle.Ceiling, verdictWord(r.Idle.Verdict),
			r.Active.KbitIn+r.Active.KbitOut, r.Active.Ceiling, verdictWord(r.Active.Verdict))
		if r.Idle.Verdict == "FAIL" || r.Active.Verdict == "FAIL" {
			t.Errorf("%d узлов: норматив ТЗ 16.4 НЕ выполнен (покой %.1f Кбит/с при ≤%.0f, "+
				"активность %.1f Кбит/с при ≤%.0f) — параметры не подгонялись",
				r.Nodes, r.Idle.KbitService, r.Idle.Ceiling,
				r.Active.KbitIn+r.Active.KbitOut, r.Active.Ceiling)
		}
	}
}

func verdictWord(v string) string {
	if v == "PASS" {
		return "выполнен"
	}
	return "НЕ выполнен"
}

func shellOut(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return fmt.Sprintf("не удалось получить: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func parseSizes(v string) []int {
	v = strings.TrimSpace(v)
	if v == "" {
		return []int{100, 500, 1000}
	}
	var out []int
	for _, part := range strings.Split(v, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 3 {
			return []int{100, 500, 1000}
		}
		out = append(out, n)
	}
	return out
}

func parseSteps(v string) (int, int, int) {
	d := DefaultPhases()
	parts := strings.Split(strings.TrimSpace(v), ":")
	if len(parts) != 3 {
		return d.IdleBeats, d.ActiveBeats, d.FailureBeats
	}
	get := func(s string, def int) int {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n <= 0 {
			return def
		}
		return n
	}
	return get(parts[0], d.IdleBeats), get(parts[1], d.ActiveBeats), get(parts[2], d.FailureBeats)
}

func parseSeed(v string) int64 {
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 1
	}
	return n
}

func parseCrash(v string) float64 {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
	if err != nil || f < 0 || f >= 1 {
		return DefaultPhases().CrashFraction
	}
	return f
}

// boundarySizes are the mesh sizes the full-sync cross-check runs at. The exact walk
// costs Θ(N²) deliveries per beat, so the list stops well below the headline sizes; it is
// a list rather than one number because the error it measures grows with the mesh, and a
// report quoting only the smallest reading would describe a scale-dependent approximation
// by its most flattering point. ZETOMESH_SIM_SYNC_NODES accepts a comma-separated list.
func boundarySizes() []int {
	v := strings.TrimSpace(os.Getenv(envSyncNodes))
	if v == "" {
		return []int{60, 150, 300}
	}
	var out []int
	for _, part := range strings.Split(v, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < 6 {
			return []int{60, 150, 300}
		}
		out = append(out, n)
	}
	return out
}

func itoaAll(a []int) []string {
	out := make([]string, len(a))
	for i, v := range a {
		out[i] = strconv.Itoa(v)
	}
	return out
}
