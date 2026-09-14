// Package api implements the node-local administrative HTTP/JSON interface.
//
// It is deliberately loopback-first: the mesh speaks protobuf over libp2p;
// this surface exists for operators and scripts. Bearer auth activates only
// when the configured token environment variable is non-empty.
package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/metrics"
	"github.com/developer3000S/zeptoclaw/internal/node"
	"github.com/developer3000S/zeptoclaw/internal/skills"
	"github.com/developer3000S/zeptoclaw/internal/tasks"

	pb "github.com/developer3000S/zeptoclaw/gen/zeptomesh/v1"
)

// Server hosts the admin API.
type Server struct {
	httpSrv *http.Server
	node    *node.Node
	cfg     *config.Config
	token   string
	log     *slog.Logger
}

// New wires routes.
func New(n *node.Node, cfg *config.Config, mets *metrics.Collector, logger *slog.Logger) *Server {
	token := ""
	if cfg.API.AuthTokenEnv != "" {
		token = os.Getenv(cfg.API.AuthTokenEnv)
	}
	if cfg.API.Enabled && cfg.Security.TrustMode == "private" && token == "" {
		logger.Warn("admin_api_unauthenticated", "hint", "set "+cfg.API.AuthTokenEnv+" or bind api.listen to localhost")
	}
	mux := http.NewServeMux()
	s := &Server{node: n, cfg: cfg, token: token, log: logger}

	mux.HandleFunc("GET /healthz", s.handleHealth)
	s.authed(mux, "GET /api/v1/status", s.handleStatus)
	s.authed(mux, "GET /api/v1/peers", s.handlePeers)
	s.authed(mux, "GET /api/v1/capabilities", s.handleCapabilities)
	s.authed(mux, "GET /api/v1/skills", s.handleSkills)
	s.authed(mux, "POST /api/v1/skills", s.handleUpsertSkill)
	s.authed(mux, "DELETE /api/v1/skills/{name}", s.handleDropSkillDoc)
	s.authed(mux, "POST /api/v1/skills/sync", s.handleSyncSkills)
	s.authed(mux, "POST /api/v1/tasks", s.handleSubmit)
	s.authed(mux, "GET /api/v1/tasks", s.handleList)
	s.authed(mux, "GET /api/v1/tasks/{id}", s.handleGetTask)
	s.authed(mux, "POST /api/v1/tasks/{id}/cancel", s.handleCancel)
	s.authed(mux, "DELETE /api/v1/tasks/{id}", s.handleCancel)
	s.authed(mux, "POST /api/v1/tasks/{id}/resubmit", s.handleResubmit)
	s.authed(mux, "GET /api/v1/config", s.handleConfigView)
	s.authed(mux, "GET /api/v1/rebinds", s.handleRebinds)
	s.authed(mux, "POST /api/v1/admin/reload-config", s.handleReloadConfig)
	s.authed(mux, "POST /api/v1/admin/rotate-key", s.handleRotateKey)
	s.authed(mux, "POST /api/v1/admin/revoke", s.handleRevoke)
	s.authed(mux, "POST /api/v1/admin/leave", s.handleLeave)
	if mets != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(mets.Registry(), promhttp.HandlerOpts{}))
	}

	readTimeout := cfg.API.ReadTimeout.D()
	if readTimeout <= 0 {
		readTimeout = 15 * time.Second
	}
	writeTimeout := cfg.API.WriteTimeout.D()
	if writeTimeout <= 0 {
		writeTimeout = 10 * time.Minute // task submissions may block on --wait
	}
	s.httpSrv = &http.Server{
		Addr:         cfg.API.Listen,
		Handler:      s.limitBody(mux),
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  60 * time.Second,
	}
	return s
}

// Serve runs the API until ctx ends. Call after node start.
func (s *Server) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.httpSrv.Addr)
	if err != nil {
		return fmt.Errorf("api: listen %s: %w", s.httpSrv.Addr, err)
	}
	s.log.Info("admin_api_listening", "addr", ln.Addr().String(), "auth", s.token != "")
	errCh := make(chan error, 1)
	go func() { errCh <- s.httpSrv.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpSrv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// ListenAndServe starts serving in the background (tests).
func (s *Server) ListenAndServe() error { return s.httpSrv.ListenAndServe() }

// Shutdown stops the server.
func (s *Server) Shutdown(ctx context.Context) error { return s.httpSrv.Shutdown(ctx) }

// Handler exposes the router for in-process tests.
func (s *Server) Handler() http.Handler { return s.limitBody(s.httpSrv.Handler) }

// ---------- middleware ----------

func (s *Server) authed(mux *http.ServeMux, pattern string, h func(http.ResponseWriter, *http.Request)) {
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if !s.authorize(r) {
			writeErr(w, http.StatusUnauthorized, "missing or bad bearer token")
			return
		}
		h(w, r)
	})
}

func (s *Server) authorize(r *http.Request) bool {
	if s.token == "" {
		return true
	}
	const prefix = "Bearer "
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, prefix) {
		return false
	}
	got := strings.TrimSpace(auth[len(prefix):])
	return subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1
}

func (s *Server) limitBody(next http.Handler) http.Handler {
	max := s.cfg.Tasks.MaxPayloadBytes
	if max <= 0 {
		max = 1 << 20
	}
	return http.MaxBytesHandler(next, max)
}

// ---------- handlers ----------

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "peer_id": s.node.ID().String()})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.node.Status())
}

type peerView struct {
	PeerID       string   `json:"peer_id"`
	Addrs        []string `json:"addrs,omitempty"`
	Skills       []string `json:"skills,omitempty"`
	Category     string   `json:"category,omitempty"`
	Trust        string   `json:"trust"`
	Version      string   `json:"version,omitempty"`
	Load         float64  `json:"load"`
	Connected    bool     `json:"connected"`
	Left         bool     `json:"left,omitempty"`
	RTTMillis    int64    `json:"rtt_ms,omitempty"`
	Successes    uint64   `json:"successes"`
	Failures     uint64   `json:"failures"`
	LastSeenUnix int64    `json:"last_seen_unix"`
	Score        float64  `json:"score,omitempty"`
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 200)
	minimal := r.URL.Query().Get("minimal") == "true"
	var out []peerView
	for _, n := range s.node.Table.List() {
		v := peerView{
			PeerID: n.PeerID.String(), Addrs: n.Addrs,
			// Answered from the policy, not from the table: what the operator sees
			// must be the standing that admission decisions actually apply.
			Trust:        s.node.Table.TrustOf(n.PeerID).String(),
			Category:     n.Category,
			Connected:    n.Connected,
			Left:         n.Left,
			Successes:    n.Successes,
			Failures:     n.Failures,
			LastSeenUnix: n.LastSeen.Unix(),
			RTTMillis:    n.RTT.Milliseconds(),
		}
		if !minimal {
			v.Skills = n.Skills
			v.Version = n.Version
			v.Load = n.Load
		}
		out = append(out, v)
		if len(out) >= limit {
			break
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(out), "peers": out})
}

func (s *Server) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.node.Capabilities())
}

// submitRequest is the client-side task description.
type submitRequest struct {
	Instruction    string            `json:"instruction"`
	RequiredSkills []string          `json:"required_skills,omitempty"`
	TTL            int               `json:"ttl,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	AllowShell     bool              `json:"allow_shell,omitempty"`
	AllowNetwork   *bool             `json:"allow_network_tools,omitempty"`
	AllowDeleg     *bool             `json:"allow_delegation,omitempty"`
	AllowSubtasks  *bool             `json:"allow_subtasks,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	// Subtasks is an optional decomposition plan (ТЗ 6.9.1): when present the
	// task is executed as a set of independently routed children whose results
	// return as one signed aggregate.
	Subtasks []subtaskRequest `json:"subtasks,omitempty"`
	// Wait blocks the response until the task resolves or wait_seconds passes.
	Wait       bool `json:"wait,omitempty"`
	WaitSecond int  `json:"wait_seconds,omitempty"`
}

// subtaskRequest is one entry of a decomposition plan.
type subtaskRequest struct {
	Instruction    string   `json:"instruction"`
	RequiredSkills []string `json:"required_skills,omitempty"`
	TTL            int      `json:"ttl,omitempty"`
	Priority       int      `json:"priority,omitempty"`
}

func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var req submitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Instruction) == "" {
		writeErr(w, http.StatusBadRequest, "instruction is required")
		return
	}
	if req.Priority < 0 || req.Priority > 9 {
		writeErr(w, http.StatusBadRequest, "priority must be within 1..9 (0 = default)")
		return
	}
	if len(req.Subtasks) > tasks.MaxSubtasks {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("too many subtasks (max %d)", tasks.MaxSubtasks))
		return
	}
	allowNet := true
	if req.AllowNetwork != nil {
		allowNet = *req.AllowNetwork
	}
	var subs []*tasks.SubtaskRequest
	for _, st := range req.Subtasks {
		subs = append(subs, &tasks.SubtaskRequest{
			Instruction:    st.Instruction,
			RequiredSkills: st.RequiredSkills,
			TTL:            int32(st.TTL),
			Priority:       int32(st.Priority),
		})
	}
	n := s.node
	id, err := n.Manager.Submit(r.Context(), node.SubmitRequest{
		Instruction:    req.Instruction,
		RequiredSkills: req.RequiredSkills,
		TTL:            int32(req.TTL),
		Priority:       int32(req.Priority),
		TimeoutSeconds: int32(req.TimeoutSeconds),
		AllowShell:     req.AllowShell,
		AllowNetwork:   allowNet,
		AllowDeleg:     req.AllowDeleg,
		AllowSubtasks:  req.AllowSubtasks,
		Labels:         req.Labels,
		Subtasks:       subs,
	})
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	resp := map[string]any{"task_id": id}
	if req.Wait {
		waitFor := time.Duration(req.WaitSecond) * time.Second
		if waitFor <= 0 {
			waitFor = time.Duration(s.cfg.Tasks.DefaultTimeoutSeconds+s.cfg.Tasks.DefaultTTL*int(s.cfg.Tasks.Forwarding.AttemptTimeout.D().Seconds())) * time.Second
		}
		wctx, cancel := context.WithTimeout(r.Context(), waitFor)
		defer cancel()
		res, err := n.Manager.AwaitResult(wctx, id)
		if err != nil {
			resp["error"] = err.Error()
			writeJSON(w, http.StatusOK, resp)
			return
		}
		resp["result"] = resultJSON(res)
	}
	writeJSON(w, http.StatusAccepted, resp)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 50)
	recs, err := s.node.Manager.ListTasks(limit, strings.ToUpper(r.URL.Query().Get("status")))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(recs), "tasks": recs})
}

func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, res, err := s.node.Manager.TaskStatus(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "task not found: "+id)
		return
	}
	out := map[string]any{"task": rec}
	if res != nil {
		out["result"] = res
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	reason := r.URL.Query().Get("reason")
	if reason == "" {
		reason = "canceled via admin API"
	}
	if err := s.node.Manager.Cancel(r.Context(), id, reason); err != nil {
		writeErr(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"task_id": id, "status": "canceled"})
}

// handleResubmit re-injects a task that failed or timed out: the journal entry
// is copied into a fresh root task (new task_id ⇒ bypasses deduplication).
func (s *Server) handleResubmit(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rec, _, err := s.node.Manager.TaskStatus(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, "task not found: "+id)
		return
	}
	instruction, err := s.node.Manager.OriginalInstruction(id)
	if err != nil {
		writeErr(w, http.StatusConflict, "cannot recover instruction: "+err.Error())
		return
	}
	newID, err := s.node.Manager.Submit(r.Context(), node.SubmitRequest{
		Instruction: instruction, RequiredSkills: rec.RequiredSkills,
		Priority: rec.Priority, Labels: map[string]string{"resubmit_of": id},
	})
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"task_id": newID, "resubmit_of": id})
}

func (s *Server) handleConfigView(w http.ResponseWriter, r *http.Request) {
	cfg := *s.cfg
	// Never expose filesystem-independent identity material beyond the path.
	writeJSON(w, http.StatusOK, map[string]any{"config": cfg, "data_dir": s.cfg.Node.DataDir})
}

// handleRebinds reports the identity-handover ledger: which peer ids this node
// considers rotated or revoked. That is the record an operator consults when a
// peer that "should" be trusted is refused (ТЗ 11.2).
func (s *Server) handleRebinds(w http.ResponseWriter, r *http.Request) {
	type stmt struct {
		OldPeerID string `json:"old_peer_id"`
		NewPeerID string `json:"new_peer_id,omitempty"`
		Sequence  int64  `json:"sequence"`
		IssuedAt  int64  `json:"issued_at"`
		Reason    string `json:"reason,omitempty"`
		Kind      string `json:"kind"`
	}
	var out []stmt
	for _, k := range s.node.Rebinds.Statements() {
		kind := "rotate"
		if k.GetNewPeerId() == "" {
			kind = "revoke"
		}
		out = append(out, stmt{
			OldPeerID: k.GetOldPeerId(), NewPeerID: k.GetNewPeerId(), Sequence: k.GetSequence(),
			IssuedAt: k.GetIssuedAt(), Reason: k.GetReason(), Kind: kind,
		})
	}
	// Signatures are intentionally omitted: the ledger is verified on load and on
	// ingest, and the statements only matter as signed — a copy pasted into a log
	// or a file loses that, so echoing them adds exposure without adding value.
	writeJSON(w, http.StatusOK, map[string]any{
		"statements": out, "count": len(out), "self_revoked": s.node.Rebinds.Revoked(s.node.ID()),
	})
}

// handleReloadConfig re-reads the node's config file and applies the
// hot-reloadable sections, reporting honestly which changes need a restart
// (ТЗ 13.1.1).
func (s *Server) handleReloadConfig(w http.ResponseWriter, r *http.Request) {
	d, err := s.node.ReloadConfig()
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	resp := map[string]any{"applied": d.Hot, "requires_restart": d.RequiresRestart}
	if len(d.RequiresRestart) > 0 {
		resp["hint"] = "POST /api/v1/admin/leave (or systemctl restart) to apply them"
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleLeave announces departure from the mesh and makes the daemon exit so
// the supervisor relaunches it with the on-disk configuration (ТЗ 13.1.1).
func (s *Server) handleLeave(w http.ResponseWriter, r *http.Request) {
	s.log.Info("admin_leave_requested")
	writeJSON(w, http.StatusOK, map[string]any{"status": "leaving"})
	// Flush the response before the process begins its shutdown.
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	s.node.Leave(r.Context())
}

// handleRotateKey replaces the node identity key and announces the handover
// (ТЗ 11.2 п.3). The announcement is spread before the new key is installed, so
// peers see a successor of a trusted node rather than a stranger.
func (s *Server) handleRotateKey(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Reason string `json:"reason,omitempty"`
	}
	if r.Body != nil {
		// An empty or absent body is fine: the reason is operator annotation.
		_ = json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&in)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	res, err := s.node.RotateKey(ctx, in.Reason)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"old_peer_id":      res.OldPeerID.String(),
		"new_peer_id":      res.NewPeerID.String(),
		"announced_to":     res.AnnouncedTo,
		"restart_required": res.Restart,
		"hint":             "restart the service (POST /api/v1/admin/leave or systemctl restart) to run under the new key",
	})
}

// handleRevoke retires this node's identity for good (ТЗ 11.2 п.4). Unlike a
// rotation there is no successor: the process stops and the supervisor must not
// start it again, which is what node.Halted() signals.
func (s *Server) handleRevoke(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Reason string `json:"reason,omitempty"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(io.LimitReader(r.Body, 4<<10)).Decode(&in)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := s.node.RevokeSelf(ctx, in.Reason); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"revoked": s.node.ID().String(),
		"status":  "halting",
	})
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	// The statement is already spread; only the local shutdown is left.
	s.node.Leave(r.Context())
}

// ---------- helpers ----------

// handleSkills reports this node's advertised skills with their documentation
// and epoch, plus the learned view of every peer. That is the operator's entry
// point for "why was this task not routed to that node".
func (s *Server) handleSkills(w http.ResponseWriter, r *http.Request) {
	ex := s.cfg.Capabilities.SkillExchange
	writeJSON(w, http.StatusOK, map[string]any{
		"skills_version": s.node.Skills.Epoch(),
		"advertised":     s.cfg.EffectiveSkills(),
		"descriptors":    s.node.Skills.Descriptors(),
		"exchange": map[string]any{
			"enabled":     ex.Enabled,
			"disclose_to": ex.DiscloseTo,
			"interval":    ex.Interval.String(),
		},
		"peers": s.node.PeerSkillViews(),
	})
}

// skillDocRequest is the operator-supplied documentation for one skill.
type skillDocRequest struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Models      []string          `json:"models,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
}

// handleUpsertSkill documents (or re-documents) one advertised skill. Bumping
// its version is what makes peers re-pull it; the ability to execute is not
// granted here, so a name this node does not advertise is refused.
func (s *Server) handleUpsertSkill(w http.ResponseWriter, r *http.Request) {
	var in skillDocRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "bad request: "+err.Error())
		return
	}
	if strings.TrimSpace(in.Name) == "" {
		writeErr(w, http.StatusBadRequest, "name is required")
		return
	}
	stored, err := s.node.SetSkillDoc(skills.Descriptor{
		Name: in.Name, Description: in.Description, Models: in.Models, Attributes: in.Attributes,
	})
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"skill": stored, "skills_version": s.node.Skills.Epoch(),
	})
}

// handleDropSkillDoc retires a skill's documentation (the name stays advertised).
func (s *Server) handleDropSkillDoc(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		writeErr(w, http.StatusBadRequest, "skill name is required")
		return
	}
	dropped := s.node.RemoveSkillDoc(name)
	writeJSON(w, http.StatusOK, map[string]any{
		"dropped": dropped, "name": name, "skills_version": s.node.Skills.Epoch(),
	})
}

// handleSyncSkills reconciles peer skill descriptors on demand instead of
// waiting for the periodic pass.
func (s *Server) handleSyncSkills(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	peers := s.node.SyncSkillsNow(ctx)
	writeJSON(w, http.StatusOK, map[string]any{"peers_queried": peers})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		slog.Default().Warn("api_write", "err", err.Error())
	}
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}

func queryInt(r *http.Request, name string, def int) int {
	if v := r.URL.Query().Get(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func resultJSON(res *pb.TaskResult) map[string]any {
	arts := make([]map[string]any, 0, len(res.GetArtifacts()))
	for _, a := range res.GetArtifacts() {
		arts = append(arts, map[string]any{"name": a.GetName(), "hash": a.GetHash(), "size": a.GetSize()})
	}
	m := map[string]any{
		"task_id":     res.GetTaskId(),
		"status":      res.GetStatus().String(),
		"text":        res.GetText(),
		"worker":      res.GetWorkerPeerId(),
		"sender":      res.GetSenderPeerId(),
		"started_at":  res.GetStartedAt(),
		"finished_at": res.GetFinishedAt(),
		"route_stack": res.GetRouteStack(),
		"artifacts":   arts,
		"signed":      len(res.GetSignature()) > 0,
	}
	if ec := res.GetErrorClass(); ec != pb.TaskErrorClass_TASK_ERROR_CLASS_UNSPECIFIED {
		m["error_class"] = ec.String()
	}
	if res.GetAggregated() {
		m["aggregated"] = true
	}
	if len(res.GetWorkerSignature()) > 0 {
		m["worker_signed"] = true
	}
	if d := res.GetResultDigest(); len(d) > 0 {
		m["result_digest"] = fmt.Sprintf("%x", d)
	}
	if res.GetErrorMessage() != "" {
		m["error"] = res.GetErrorMessage()
	}
	return m
}
