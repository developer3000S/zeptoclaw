package metrics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServeMetrics exposes only /metrics (and /healthz) on a dedicated listener,
// separate from the admin API. Operators scrape this port; it carries no
// authorisation and is therefore expected to be bound to a private interface
// (e.g. 127.0.0.1:9464 or an internal mesh address), never to the public one.
//
// It blocks until ctx is cancelled or the listener fails.
func (c *Collector) ServeMetrics(ctx context.Context, addr string) error {
	if c == nil {
		return errors.New("metrics: nil collector")
	}
	if addr == "" {
		return errors.New("metrics: empty listen address")
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(c.Registry(), promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("metrics: listen %s: %w", addr, err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
