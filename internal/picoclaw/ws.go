package picoclaw

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/developer3000S/zeptoclaw/internal/config"
)

// Pico Protocol message types, as defined by PicoClaw's pico channel.
const (
	typeMessageSend   = "message.send"
	typeMessageCreate = "message.create"
	typeMessageUpdate = "message.update"
	typeTypingStart   = "typing.start"
	typeTypingStop    = "typing.stop"
	typeError         = "error"
	typePing          = "ping"
	typePong          = "pong"

	kindThought   = "thought"
	kindToolCalls = "tool_calls"
)

// picoMessage mirrors PicoClaw's wire struct.
type picoMessage struct {
	Type      string         `json:"type"`
	ID        string         `json:"id,omitempty"`
	SessionID string         `json:"session_id,omitempty"`
	Timestamp int64          `json:"timestamp,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
}

// WSAdapter talks to a running `picoclaw gateway` over the Pico Protocol.
//
// This is the preferred mode in production: unlike the CLI it carries no
// banner noise, reports errors in-band, and reuses one warm agent process.
// The gateway exposes no REST endpoint that accepts a prompt, so the only
// programmatic path is this WebSocket.
type WSAdapter struct {
	wsURL    string
	httpBase string
	health   string
	token    string
	log      *slog.Logger
	dialer   websocket.Dialer
	timeout  time.Duration
	slots    chan struct{}

	mu     sync.Mutex
	nextID int
}

// NewWS builds a Pico Protocol adapter.
func NewWS(cfg config.PicoClawConfig, logger *slog.Logger) (*WSAdapter, error) {
	base := strings.TrimRight(cfg.HTTP.BaseURL, "/")
	if base == "" {
		return nil, errors.New("picoclaw: http mode requires picoclaw.http.base_url")
	}
	wsURL, err := wsURLFromHTTP(base, cfg.HTTP.Path)
	if err != nil {
		return nil, err
	}
	healthPath := cfg.HTTP.HealthPath
	if healthPath == "" {
		healthPath = "/health"
	}
	if !strings.HasPrefix(healthPath, "/") {
		healthPath = "/" + healthPath
	}
	token := ""
	if cfg.HTTP.TokenEnv != "" {
		token = os.Getenv(cfg.HTTP.TokenEnv)
	}
	if token == "" {
		return nil, fmt.Errorf("picoclaw: pico token is empty; set %s (PicoClaw requires channels.pico.token)", cfg.HTTP.TokenEnv)
	}
	timeout := time.Duration(cfg.HTTP.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(cfg.TimeoutSeconds) * time.Second
	}
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	slots := cfg.MaxConcurrentAgents
	if slots <= 0 {
		slots = 1
	}
	if logger != nil && strings.TrimSpace(cfg.Model) != "" {
		// The Pico Protocol has no verified request field for model selection, so
		// the gateway's own configuration decides there. Saying nothing would let
		// an operator believe picoclaw.model applies to every mode.
		logger.Warn("picoclaw_model_not_applicable", "mode", "http", "model", strings.TrimSpace(cfg.Model))
	}
	return &WSAdapter{
		wsURL:    wsURL,
		httpBase: base,
		health:   healthPath,
		token:    token,
		log:      logger,
		timeout:  timeout,
		slots:    make(chan struct{}, slots),
		dialer: websocket.Dialer{
			HandshakeTimeout: 10 * time.Second,
			// The gateway echoes "token.<value>" as the negotiated subprotocol.
			Subprotocols: []string{"token." + token},
		},
	}, nil
}

// Name implements Adapter.
func (a *WSAdapter) Name() string { return "picoclaw-pico-ws" }

// Healthy checks the gateway's /health endpoint.
func (a *WSAdapter) Healthy(ctx context.Context) error {
	health := strings.TrimRight(a.httpBase, "/") + a.health
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(cctx, http.MethodGet, health, nil)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	hres, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: gateway health at %s: %v", ErrUnavailable, health, err)
	}
	defer hres.Body.Close()
	if hres.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: gateway health returned %d", ErrUnavailable, hres.StatusCode)
	}
	return nil
}

// Capabilities is not advertised over the Pico Protocol.
func (a *WSAdapter) Capabilities(context.Context) ([]string, error) { return nil, nil }

// Execute sends one prompt and waits for the turn to finish.
//
// Turn completion is detected via typing.stop, which the gateway emits after
// the final message. Intermediate message.create frames with kind "thought" or
// "tool_calls" are progress, not the answer, so they are ignored.
func (a *WSAdapter) Execute(ctx context.Context, req Request) (*Response, error) {
	if strings.TrimSpace(req.Instruction) == "" {
		return nil, errors.New("picoclaw: empty instruction")
	}
	select {
	case a.slots <- struct{}{}:
		defer func() { <-a.slots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = a.timeout
	}
	ectx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	started := time.Now().UTC()
	conn, err := a.dial(ectx, req.SessionKey)
	if err != nil {
		return &Response{StartedAt: started, FinishedAt: time.Now().UTC()}, err
	}

	id := fmt.Sprintf("zeptomesh-%d", a.seq())
	send := picoMessage{
		Type:      typeMessageSend,
		ID:        id,
		SessionID: sessionOf(req.SessionKey),
		Payload:   map[string]any{"content": req.Instruction},
	}
	if err := conn.WriteJSON(send); err != nil {
		conn.Close()
		return nil, fmt.Errorf("picoclaw: send: %w", err)
	}

	text, model, err := a.readTurn(ectx, conn, id)
	finished := time.Now().UTC()
	arts, _ := collectArtifacts(req.Workspace, req.MaxWorkspaceBytes)
	resp := &Response{
		Text:       text,
		Model:      model,
		StartedAt:  started,
		FinishedAt: finished,
		Artifacts:  arts,
	}
	if err != nil {
		return resp, err
	}
	if strings.TrimSpace(text) == "" {
		return resp, errors.New("picoclaw: gateway returned an empty answer")
	}
	return resp, nil
}

// readTurn consumes frames until the turn ends.
func (a *WSAdapter) readTurn(ctx context.Context, conn *websocket.Conn, reqID string) (string, string, error) {
	defer conn.Close()

	var (
		final  string
		model  string
		latest string
	)
	for {
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return final, model, fmt.Errorf("%w: %v", ErrTimeout, err)
			}
			return final, model, err
		}
		var msg picoMessage
		if err := conn.ReadJSON(&msg); err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return final, model, fmt.Errorf("picoclaw: read: %w", err)
			}
			// A clean close after we already have an answer is a normal end of turn.
			if final != "" {
				return final, model, nil
			}
			return final, model, fmt.Errorf("picoclaw: connection closed before an answer: %w", err)
		}
		switch msg.Type {
		case typeMessageCreate, typeMessageUpdate:
			kind, _ := msg.Payload["kind"].(string)
			content, _ := msg.Payload["content"].(string)
			if m, ok := msg.Payload["model_name"].(string); ok && m != "" {
				model = m
			}
			switch kind {
			case kindThought, kindToolCalls:
				continue // progress, not the answer
			}
			if content == "" {
				continue
			}
			latest = content
			final = content
		case typeTypingStop:
			if final == "" {
				final = latest
			}
			if final == "" {
				return "", model, errors.New("picoclaw: turn ended without an answer")
			}
			return final, model, nil
		case typeError:
			code, _ := msg.Payload["code"].(string)
			m, _ := msg.Payload["message"].(string)
			return final, model, fmt.Errorf("picoclaw: gateway error %s: %s", code, m)
		case typePong:
			continue
		default:
			if a.log != nil {
				a.log.Debug("picoclaw_unexpected_frame", "type", msg.Type, "req", reqID)
			}
		}
	}
}

// dial opens a per-task WebSocket. A dedicated connection per task keeps
// sessions isolated and avoids multiplexing answers between tasks.
func (a *WSAdapter) dial(ctx context.Context, sessionKey string) (*websocket.Conn, error) {
	url := a.wsURL
	if s := sessionOf(sessionKey); s != "" {
		sep := "?"
		if strings.Contains(url, "?") {
			sep = "&"
		}
		url = fmt.Sprintf("%s%ssession_id=%s", url, sep, s)
	}
	header := http.Header{}
	header.Set("Authorization", "Bearer "+a.token)

	conn, res, err := a.dialer.DialContext(ctx, url, header)
	if err != nil {
		if res != nil {
			return nil, fmt.Errorf("picoclaw: ws handshake %s: %w (http %d)", url, err, res.StatusCode)
		}
		return nil, fmt.Errorf("picoclaw: ws handshake %s: %w", url, err)
	}
	return conn, nil
}

// Close drops pooled resources.
func (a *WSAdapter) Close() error { return nil }

func (a *WSAdapter) seq() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nextID++
	return a.nextID
}

// sessionOf maps a mesh session key to a pico session id. PicoClaw prefixes
// the chat id internally ("pico:<session_id>"), so a per-task key is enough.
func sessionOf(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	return strings.NewReplacer(":", "-", "/", "-", " ", "-").Replace(key)
}

// wsURLFromHTTP converts the gateway's base URL into the pico WebSocket URL.
func wsURLFromHTTP(base, path string) (string, error) {
	switch {
	case strings.HasPrefix(base, "https://"):
		base = "wss://" + strings.TrimPrefix(base, "https://")
	case strings.HasPrefix(base, "http://"):
		base = "ws://" + strings.TrimPrefix(base, "http://")
	case strings.HasPrefix(base, "ws://"), strings.HasPrefix(base, "wss://"):
	default:
		return "", fmt.Errorf("picoclaw: unsupported base_url %q", base)
	}
	if path == "" {
		path = "/pico/ws"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimRight(base, "/") + path, nil
}

var _ Adapter = (*WSAdapter)(nil)
