package skills

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The key-exchange skill sends this node's credentials with the friend's bearer
// token and reports how many the friend accepted.
func TestExchangeKeys_SendsBearerAndParsesAccepted(t *testing.T) {
	var seen map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/keys/exchange" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer friend-token" {
			t.Errorf("auth = %q", r.Header.Get("Authorization"))
		}
		var in struct {
			Keys map[string]string `json:"keys"`
		}
		payload, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(payload, &in)
		seen = in.Keys
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "exchanged", "accepted": len(in.Keys)})
	}))
	defer srv.Close()

	c := NewAPIKeyExchangeSkill("friend-token", slog.New(slog.DiscardHandler))
	keys := map[string]string{"SHODAN_API_KEY": "s3cr3t", "CENSYS_API_KEY": "c3nsys"}
	accepted, err := c.ExchangeKeys(context.Background(), srv.URL, keys)
	if err != nil {
		t.Fatalf("ExchangeKeys: %v", err)
	}
	if accepted != 2 {
		t.Fatalf("accepted = %d, want 2", accepted)
	}
	if seen["SHODAN_API_KEY"] != "s3cr3t" {
		t.Fatalf("keys never reached the friend: %v", seen)
	}
}

// An empty key set means "nothing to disclose" and the call is skipped outright.
func TestExchangeKeys_EmptyKeysIsANoop(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	c := NewAPIKeyExchangeSkill("", slog.New(slog.DiscardHandler))
	if accepted, err := c.ExchangeKeys(context.Background(), srv.URL, nil); err != nil || accepted != 0 {
		t.Fatalf("empty exchange: accepted=%d err=%v", accepted, err)
	}
	if called {
		t.Fatalf("an empty key set must not produce a request")
	}
}

// A friend that rejects the credentials surfaces the failure instead of
// reporting a silent success.
func TestExchangeKeys_FriendRefusalIsReported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"key exchange disabled"}`, http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := NewAPIKeyExchangeSkill("", slog.New(slog.DiscardHandler))
	if _, err := c.ExchangeKeys(context.Background(), srv.URL,
		map[string]string{"SHODAN_API_KEY": "x"}); err == nil {
		t.Fatalf("a friend's refusal must be an error")
	} else if !strings.Contains(err.Error(), "503") {
		t.Fatalf("error should name the status: %v", err)
	}
}

// The promote skill relays a candidate to a friend and reports the friend's own
// verification verdict — the caller never decides verification itself.
func TestPromoteCandidate_ParsesFriendVerdict(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/candidates/promote" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer friend-token" {
			t.Errorf("auth = %q", r.Header.Get("Authorization"))
		}
		var in map[string]string
		payload, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(payload, &in)
		if in["candidate_id"] != "cnd_1234" {
			t.Errorf("candidate_id = %q", in["candidate_id"])
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "promoted",
			"backend": map[string]any{
				"verified": true, "base_url": "http://203.0.113.10:11434",
				"models": []any{"qwen2.5:7b"},
			},
		})
	}))
	defer srv.Close()

	c := NewPromoteSkill("friend-token", slog.New(slog.DiscardHandler))
	res, err := c.PromoteCandidate(context.Background(), srv.URL, "cnd_1234", "auto")
	if err != nil {
		t.Fatalf("PromoteCandidate: %v", err)
	}
	if !res.Verified || res.BaseURL != "http://203.0.113.10:11434" || len(res.Models) != 1 {
		t.Fatalf("friend verdict = %+v", res)
	}
	if res.Models[0] != "qwen2.5:7b" {
		t.Fatalf("models = %v", res.Models)
	}
}

// A friend that did not verify the endpoint reports it honestly, and so does
// the skill: verified=false is a result, not an error.
func TestPromoteCandidate_UnverifiedIsAResultNotAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":  "promoted",
			"backend": map[string]any{"verified": false, "base_url": "http://203.0.113.11:11434"},
		})
	}))
	defer srv.Close()

	c := NewPromoteSkill("", slog.New(slog.DiscardHandler))
	res, err := c.PromoteCandidate(context.Background(), srv.URL, "cnd_5678", "")
	if err != nil {
		t.Fatalf("an unverified promotion must not be an error: %v", err)
	}
	if res.Verified {
		t.Fatalf("expected verified=false, got %+v", res)
	}
}

// A candidate id is required: relaying nothing would make the friend's
// verification verdict meaningless.
func TestPromoteCandidate_RequiresCandidateID(t *testing.T) {
	c := NewPromoteSkill("", slog.New(slog.DiscardHandler))
	if _, err := c.PromoteCandidate(context.Background(), "http://example.invalid", "  ", ""); err == nil {
		t.Fatalf("empty candidate id must be rejected")
	}
}

// A non-"promoted" status from the friend is an error, so a partial or
// unexpected answer can never be mistaken for a promotion.
func TestPromoteCandidate_UnexpectedStatusIsAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "throttled"})
	}))
	defer srv.Close()

	c := NewPromoteSkill("", slog.New(slog.DiscardHandler))
	if _, err := c.PromoteCandidate(context.Background(), srv.URL, "cnd_1", ""); err == nil {
		t.Fatalf("unexpected status must be an error")
	}
}
