// Package skills implements the node's skill system.

package skills

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// PromoteSkill implements the skill for promoting discovered candidates to backends
type PromoteSkill struct {
	client *http.Client
	log    *slog.Logger
}

// NewPromoteSkill creates a new promote skill
func NewPromoteSkill(log *slog.Logger) *PromoteSkill {
	return &PromoteSkill{
		client: &http.Client{Timeout: 30 * time.Second},
		log:    log,
	}
}

// PromoteCandidate promotes a discovered candidate to a usable backend
func (s *PromoteSkill) PromoteCandidate(nodeAddr string, candidateID string, reason string) error {
	reqBody := map[string]string{
		"candidate_id": candidateID,
		"reason":      reason,
	}
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/candidates/promote", nodeAddr), bytes.NewBuffer(reqBytes))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if result["status"] != "promoted" {
		return fmt.Errorf("unexpected response status: %s", result["status"])
	}

	return nil
}