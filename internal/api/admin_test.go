package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/developer3000S/zeptoclaw/internal/config"
	"github.com/developer3000S/zeptoclaw/internal/node"
	"github.com/developer3000S/zeptoclaw/internal/security"
)

func TestRBAC(t *testing.T) {
	// Create a test node
	n, err := node.New(&config.Config{
		API: config.APIConfig{
			Enabled: true,
			Listen:  "127.0.0.1:8081",
			Tokens:  []config.APIToken{
				{
					Name:     "test-token",
					Role:     "admin",
					TokenEnv: "TEST_TOKEN",
				},
			},
		},
		Security: config.SecurityConfig{
			TrustMode: "private",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// Create a test server
	s := New(n, n.Cfg, nil, nil)
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	// Test admin access
	req, err := http.NewRequest("GET", ts.URL+"/api/v1/status", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-token")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status code %d, got %d", http.StatusOK, resp.StatusCode)
	}

	// Test insufficient permissions
	req, err = http.NewRequest("POST", ts.URL+"/api/v1/tasks", nil)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-token")

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("Expected status code %d, got %d", http.StatusForbidden, resp.StatusCode)
	}
}
