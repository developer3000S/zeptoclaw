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

// APIKeyExchangeSkill implements the skill for exchanging API keys for discovery
type APIKeyExchangeSkill struct {
	client *http.Client
	log    *slog.Logger
}

// NewAPIKeyExchangeSkill creates a new API key exchange skill
func NewAPIKeyExchangeSkill(log *slog.Logger) *APIKeyExchangeSkill {
	return &APIKeyExchangeSkill{
		client: &http.Client{Timeout: 30 * time.Second},
		log:    log,
	}
}

// ExchangeKeys exchanges API keys with another agent
func (s *APIKeyExchangeSkill) ExchangeKeys(nodeAddr string, keys map[string]string) error {
	reqBody := map[string]interface{}{
		"keys": keys,
	}
	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/keys/exchange", nodeAddr), bytes.NewBuffer(reqBytes))
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

	if result["status"] != "exchanged" {
		return fmt.Errorf("unexpected response status: %s", result["status"])
	}

	return nil
}