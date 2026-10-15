// promote.go is the outbound half of the candidate-promotion skill: it calls a
// FRIEND agent's admin API (POST /api/v1/candidates/promote) and asks that
// agent to promote one of its catalog candidates into its backend pool. The
// friend authenticates the call with its own bearer token, whose value this
// node only holds in memory (config names the environment variable).
package skills

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// PromoteSkill implements the skill for promoting discovered candidates to
// backends on a friend agent.
type PromoteSkill struct {
	token  string
	client *http.Client
	log    *slog.Logger
}

// NewPromoteSkill creates the promote skill. token is the friend's admin API
// bearer token (empty for a tokenless API).
func NewPromoteSkill(token string, log *slog.Logger) *PromoteSkill {
	return &PromoteSkill{
		token:  strings.TrimSpace(token),
		client: &http.Client{Timeout: 30 * time.Second},
		log:    log,
	}
}

// PromoteResult reports what the friend answered.
type PromoteResult struct {
	// Verified is true when the friend's own probe answered with a model list.
	Verified bool `json:"verified"`
	// BaseURL is the promoted endpoint.
	BaseURL string `json:"base_url,omitempty"`
	// Models lists the models the friend's probe saw.
	Models []string `json:"models,omitempty"`
}

// PromoteCandidate promotes a discovered candidate on a friend agent. The
// friend decides autonomously whether the endpoint is verified — the caller
// only relays the request (the mesh has no central verifier).
func (s *PromoteSkill) PromoteCandidate(ctx context.Context, nodeAddr, candidateID, reason string) (PromoteResult, error) {
	if strings.TrimSpace(candidateID) == "" {
		return PromoteResult{}, fmt.Errorf("skills: candidate_id is required")
	}
	reqBody := map[string]string{
		"candidate_id": candidateID,
		"reason":       reason,
	}
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return PromoteResult{}, fmt.Errorf("skills: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(nodeAddr, "/")+"/api/v1/candidates/promote", bytes.NewReader(reqBytes))
	if err != nil {
		return PromoteResult{}, fmt.Errorf("skills: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return PromoteResult{}, fmt.Errorf("skills: friend %s unreachable: %w", nodeAddr, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return PromoteResult{}, fmt.Errorf("skills: read friend answer: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return PromoteResult{}, fmt.Errorf("skills: friend %s status %d: %s", nodeAddr, resp.StatusCode, truncateAnswer(body))
	}
	var out struct {
		Status  string        `json:"status"`
		Backend PromoteResult `json:"backend"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return PromoteResult{}, fmt.Errorf("skills: decode friend answer: %w", err)
	}
	if out.Status != "promoted" {
		return PromoteResult{}, fmt.Errorf("skills: unexpected friend status %q", out.Status)
	}
	if s.log != nil {
		s.log.Info("skill_promote_done", "friend", nodeAddr, "candidate", candidateID, "verified", out.Backend.Verified)
	}
	return out.Backend, nil
}

func truncateAnswer(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		return s[:200] + "…"
	}
	return s
}
