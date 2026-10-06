// keys.go is the outbound half of the API-key exchange skill: it hands this
// node's search credentials to a FRIEND agent's admin API
// (POST /api/v1/keys/exchange) so that friend can run its own model search.
// The friend authenticates the call with its own bearer token.

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

// APIKeyExchangeSkill implements the skill for exchanging API keys for discovery.
type APIKeyExchangeSkill struct {
	token  string
	client *http.Client
	log    *slog.Logger
}

// NewAPIKeyExchangeSkill creates the key-exchange skill. token is the friend's
// admin API bearer token (empty for a tokenless API).
func NewAPIKeyExchangeSkill(token string, log *slog.Logger) *APIKeyExchangeSkill {
	return &APIKeyExchangeSkill{
		token:  strings.TrimSpace(token),
		client: &http.Client{Timeout: 30 * time.Second},
		log:    log,
	}
}

// ExchangeKeys hands keys to a friend agent's keys-exchange endpoint and
// reports how many the friend accepted.
func (s *APIKeyExchangeSkill) ExchangeKeys(ctx context.Context, nodeAddr string, keys map[string]string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	reqBytes, err := json.Marshal(map[string]map[string]string{"keys": keys})
	if err != nil {
		return 0, fmt.Errorf("skills: marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		strings.TrimRight(nodeAddr, "/")+"/api/v1/keys/exchange", bytes.NewReader(reqBytes))
	if err != nil {
		return 0, fmt.Errorf("skills: create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("skills: friend %s unreachable: %w", nodeAddr, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return 0, fmt.Errorf("skills: read friend answer: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("skills: friend %s status %d: %s", nodeAddr, resp.StatusCode, truncateAnswer(body))
	}
	var out struct {
		Status   string `json:"status"`
		Accepted int    `json:"accepted"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return 0, fmt.Errorf("skills: decode friend answer: %w", err)
	}
	if out.Status != "exchanged" {
		return 0, fmt.Errorf("skills: unexpected friend status %q", out.Status)
	}
	if s.log != nil {
		s.log.Info("skill_keys_exchanged", "friend", nodeAddr, "accepted", out.Accepted)
	}
	return out.Accepted, nil
}
