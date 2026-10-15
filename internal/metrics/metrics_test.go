package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSetTaskInventorySetsAndZeroesStaleStatuses(t *testing.T) {
	c := New("test")
	c.SetTaskInventory(map[string]int{"completed": 3, "failed": 1})
	if got := testutil.ToFloat64(c.TaskInventory.WithLabelValues("completed")); got != 3 {
		t.Fatalf("completed = %v", got)
	}
	c.SetTaskInventory(map[string]int{"completed": 1})
	if got := testutil.ToFloat64(c.TaskInventory.WithLabelValues("failed")); got != 0 {
		t.Fatalf("stale status must read zero, got %v", got)
	}
	if got := testutil.ToFloat64(c.TaskInventory.WithLabelValues("completed")); got != 1 {
		t.Fatalf("completed after update = %v", got)
	}
}

func TestHTTPRequestCountsByPatternAndStatus(t *testing.T) {
	c := New("test")
	c.HTTPRequest("GET", "GET /api/v1/tasks/{id}", 200)
	c.HTTPRequest("GET", "GET /api/v1/tasks/{id}", 404)
	c.HTTPRequest("POST", "POST /api/v1/tasks", 403)
	if got := testutil.ToFloat64(c.HTTPRequests.WithLabelValues("GET", "GET /api/v1/tasks/{id}", "200")); got != 1 {
		t.Fatalf("get counter = %v", got)
	}
	if got := testutil.ToFloat64(c.HTTPRequests.WithLabelValues("GET", "GET /api/v1/tasks/{id}", "404")); got != 1 {
		t.Fatalf("get-404 counter = %v", got)
	}
	if got := testutil.ToFloat64(c.HTTPRequests.WithLabelValues("POST", "POST /api/v1/tasks", "403")); got != 1 {
		t.Fatalf("post counter = %v", got)
	}
}

func TestNilCollectorIsSafe(t *testing.T) {
	var c *Collector
	c.HTTPRequest("GET", "GET /x", 200)
	c.SetTaskInventory(map[string]int{"completed": 1})
	c.Security("noop")
}
