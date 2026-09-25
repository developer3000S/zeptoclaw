package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/developer3000S/zeptoclaw/internal/config"
)

// client is a tiny admin-API HTTP client.
type client struct {
	base  string
	token string
	http  *http.Client
}

func newClient(addr, tokenEnv string) (*client, error) {
	if addr == "" {
		cfgPath := os.Getenv("ZETOMESH_CONFIG")
		if cfgPath != "" {
			if cfg, err := config.Load(cfgPath); err == nil {
				addr = cfg.API.Listen
				tokenEnv = cfg.API.AuthTokenEnv
			}
		}
	}
	if addr == "" {
		addr = "127.0.0.1:8081"
	}
	token := ""
	if tokenEnv != "" {
		token = os.Getenv(tokenEnv)
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return &client{
		base:  strings.TrimRight(addr, "/"),
		token: token,
		http:  &http.Client{Timeout: 2 * time.Minute},
	}, nil
}

func (c *client) do(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w (is the node running at %s?)", method, path, err, c.base)
	}
	defer res.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return err
	}
	if res.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(payload, &e)
		if e.Error != "" {
			return fmt.Errorf("%s %s: http %d: %s", method, path, res.StatusCode, e.Error)
		}
		return fmt.Errorf("%s %s: http %d: %s", method, path, res.StatusCode, truncate(string(payload), 300))
	}
	if out != nil {
		return json.Unmarshal(payload, out)
	}
	_, err = os.Stdout.Write(payload)
	return err
}

// printJSON pretty-prints v.
func printJSON(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// clientFlags parses the shared client flags.
func clientFlags(fs *flag.FlagSet) (*string, *string) {
	addr := fs.String("addr", "", "admin API address (default: from ZETOMESH_CONFIG or 127.0.0.1:8081)")
	tokenEnv := fs.String("token-env", "", "environment variable holding the bearer token")
	return addr, tokenEnv
}

func dial(addr, tokenEnv *string) (*client, error) {
	c, err := newClient(*addr, *tokenEnv)
	if err != nil {
		return nil, err
	}
	// Local daemons commonly bind plain HTTP; allow https:// explicitly if
	// an operator fronts the API with a TLS proxy that uses a self-signed cert.
	if strings.HasPrefix(c.base, "https://") {
		c.http.Transport = &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}
	}
	return c, nil
}

func clientStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	jsonOut := fs.Bool("json", true, "pretty JSON output")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	ctx := context.Background()
	if *jsonOut {
		var v any
		if err := c.do(ctx, http.MethodGet, "/api/v1/status", nil, &v); err != nil {
			return err
		}
		return printJSON(v)
	}
	return c.do(ctx, http.MethodGet, "/api/v1/status", nil, nil)
}

func clientPeers(args []string) error {
	fs := flag.NewFlagSet("peers", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	limit := fs.Int("limit", 200, "maximum peers to print")
	quiet := fs.Bool("quiet", false, "print peer ids only")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodGet,
		fmt.Sprintf("/api/v1/peers?limit=%d&minimal=%t", *limit, !*quiet), nil, &v); err != nil {
		return err
	}
	if *quiet {
		m, _ := v.(map[string]any)
		peers, _ := m["peers"].([]any)
		for _, p := range peers {
			if pm, ok := p.(map[string]any); ok {
				fmt.Println(pm["peer_id"])
			}
		}
		return nil
	}
	return printJSON(v)
}

func clientCapabilities(args []string) error {
	fs := flag.NewFlagSet("capabilities", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodGet, "/api/v1/capabilities", nil, &v); err != nil {
		return err
	}
	return printJSON(v)
}

func clientSubmit(args []string) error {
	fs := flag.NewFlagSet("submit", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	instruction := fs.String("i", "", "task instruction (required)")
	skills := fs.String("skills", "", "comma-separated required skills")
	ttl := fs.Int("ttl", 0, "delegation hops remaining (0 = node default)")
	priority := fs.Int("priority", 0, "1..9 (0 = default 5)")
	timeout := fs.Int("timeout", 0, "execution timeout seconds (0 = node default)")
	allowShell := fs.Bool("allow-shell", false, "permit shell tools for this task")
	allowNet := fs.Bool("allow-network", true, "permit network tools for this task")
	noDeleg := fs.Bool("no-delegation", false, "forbid forwarding this task")
	var subs stringList
	fs.Var(&subs, "subtask", "decomposition entry '<skills>|<instruction>' (repeatable); the parent then only aggregates")
	wait := fs.Bool("w", false, "wait for the result")
	waitSec := fs.Int("wait-seconds", 0, "wait budget when -w (0 = task timeout + slack)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*instruction) == "" {
		return fmt.Errorf("-i <instruction> is required")
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	body := map[string]any{
		"instruction":         *instruction,
		"ttl":                 *ttl,
		"priority":            *priority,
		"timeout_seconds":     *timeout,
		"allow_shell":         *allowShell,
		"allow_network_tools": *allowNet,
		"wait":                *wait,
		"wait_seconds":        *waitSec,
	}
	if *skills != "" {
		var list []string
		for _, s := range strings.Split(*skills, ",") {
			if s = strings.TrimSpace(s); s != "" {
				list = append(list, s)
			}
		}
		body["required_skills"] = list
	}
	if *noDeleg {
		body["allow_delegation"] = false
	}
	if len(subs) > 0 {
		list := make([]map[string]any, 0, len(subs))
		for _, s := range subs {
			skillsPart, instrPart, _ := strings.Cut(s, "|")
			if strings.TrimSpace(instrPart) == "" {
				return fmt.Errorf("--subtask expects '<skills>|<instruction>', got %q", s)
			}
			entry := map[string]any{"instruction": strings.TrimSpace(instrPart)}
			var sl []string
			for _, q := range strings.Split(skillsPart, ",") {
				if q = strings.TrimSpace(q); q != "" {
					sl = append(sl, q)
				}
			}
			if len(sl) > 0 {
				entry["required_skills"] = sl
			}
			list = append(list, entry)
		}
		body["subtasks"] = list
	}
	var out map[string]any
	if err := c.do(context.Background(), http.MethodPost, "/api/v1/tasks", body, &out); err != nil {
		return err
	}
	return printJSON(out)
}

// stringList is a repeatable string flag value (--subtask a --subtask b).
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, "; ") }

func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func clientReload(args []string) error {
	fs := flag.NewFlagSet("reload", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodPost, "/api/v1/admin/reload-config", map[string]any{}, &v); err != nil {
		return err
	}
	return printJSON(v)
}

func clientLeave(args []string) error {
	fs := flag.NewFlagSet("leave", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodPost, "/api/v1/admin/leave", map[string]any{}, &v); err != nil {
		return err
	}
	return printJSON(v)
}

// clientRotate announces a key handover and installs the new key, then leaves
// the restart to the caller: in-flight work belongs to the operator.
func clientRotate(args []string) error {
	fs := flag.NewFlagSet("rotate", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	reason := fs.String("reason", "rotation", "why the identity is being replaced")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodPost, "/api/v1/admin/rotate-key",
		map[string]any{"reason": *reason}, &v); err != nil {
		return err
	}
	if err := printJSON(v); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "zeptomesh-node: restart the service to run under the new key (leave or systemctl restart)")
	return nil
}

// clientRevoke retires this node's identity; the peer id is unusable afterwards.
func clientRevoke(args []string) error {
	fs := flag.NewFlagSet("revoke", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	reason := fs.String("reason", "retire", "why the identity is being retired")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodPost, "/api/v1/admin/revoke",
		map[string]any{"reason": *reason}, &v); err != nil {
		return err
	}
	if err := printJSON(v); err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "zeptomesh-node: the service must not be started again under this key")
	return nil
}

// clientSkills shows this node's advertised skills with documentation and the
// learned, versioned view of peers' skills.
func clientSkills(args []string) error {
	fs := flag.NewFlagSet("skills", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodGet, "/api/v1/skills", nil, &v); err != nil {
		return err
	}
	return printJSON(v)
}

// clientSkillSet documents (or re-documents) one advertised skill; the version
// bump is announced so peers re-pull it.
func clientSkillSet(args []string) error {
	fs := flag.NewFlagSet("skill-set", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	name := fs.String("name", "", "skill to document (must be advertised by this node)")
	desc := fs.String("desc", "", "human-readable description of the skill")
	var models stringList
	fs.Var(&models, "model", "model this skill may use (repeatable)")
	attrs := fs.String("attr", "", "key=value attribute pairs, comma separated")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*name) == "" {
		return fmt.Errorf("skill-set: -name is required")
	}
	body := map[string]any{"name": *name, "description": *desc}
	if len(models) > 0 {
		body["models"] = []string(models)
	}
	if *attrs != "" {
		m := map[string]string{}
		for _, kv := range strings.Split(*attrs, ",") {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				return fmt.Errorf("skill-set: bad -attr %q, want key=value", kv)
			}
			m[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
		body["attributes"] = m
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodPost, "/api/v1/skills", body, &v); err != nil {
		return err
	}
	return printJSON(v)
}

// clientSkillRm retires a skill's documentation (the name stays advertised).
func clientSkillRm(args []string) error {
	fs := flag.NewFlagSet("skill-rm", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.Arg(0) == "" {
		return fmt.Errorf("usage: zeptomesh-node skill-rm <skill>")
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	path := "/api/v1/skills/" + url.PathEscape(fs.Arg(0))
	if err := c.do(context.Background(), http.MethodDelete, path, nil, &v); err != nil {
		return err
	}
	return printJSON(v)
}

// clientSkillsSync asks neighbours for skill descriptors newer than ours now,
// instead of waiting for the periodic reconciliation.
func clientSkillsSync(args []string) error {
	fs := flag.NewFlagSet("skills-sync", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodPost, "/api/v1/skills/sync", map[string]any{}, &v); err != nil {
		return err
	}
	return printJSON(v)
}

// clientRebinds shows which identities this node has retired or rotated into.
func clientRebinds(args []string) error {
	fs := flag.NewFlagSet("rebinds", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodGet, "/api/v1/rebinds", nil, &v); err != nil {
		return err
	}
	return printJSON(v)
}

func clientTasks(args []string) error {
	fs := flag.NewFlagSet("tasks", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	limit := fs.Int("limit", 50, "maximum tasks to print")
	status := fs.String("status", "", "filter by status (RUNNING, COMPLETED, …)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	path := fmt.Sprintf("/api/v1/tasks?limit=%d", *limit)
	if *status != "" {
		path += "&status=" + strings.ToUpper(*status)
	}
	if err := c.do(context.Background(), http.MethodGet, path, nil, &v); err != nil {
		return err
	}
	return printJSON(v)
}

func clientGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	wait := fs.Bool("w", false, "wait for the result")
	waitSec := fs.Int("wait-seconds", 120, "wait budget when -w")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: zeptomesh-node get [-w] <task_id>")
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	id := fs.Arg(0)
	ctx := context.Background()
	deadline := time.Now().Add(time.Duration(*waitSec) * time.Second)
	for {
		var v map[string]any
		if err := c.do(ctx, http.MethodGet, "/api/v1/tasks/"+id, nil, &v); err != nil {
			return err
		}
		rec, _ := v["task"].(map[string]any)
		terminal := false
		if rec != nil {
			switch rec["status"] {
			case "COMPLETED", "FAILED", "TIMEOUT", "CANCELED", "REJECTED":
				terminal = true
			}
		}
		if !*wait || terminal || time.Now().After(deadline) {
			return printJSON(v)
		}
		time.Sleep(time.Second)
	}
}

func clientCancel(args []string) error {
	fs := flag.NewFlagSet("cancel", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	reason := fs.String("reason", "canceled by operator", "cancellation reason")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: zeptomesh-node cancel <task_id>")
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	path := "/api/v1/tasks/" + fs.Arg(0) + "/cancel?reason=" + *reason
	if err := c.do(context.Background(), http.MethodPost, path, map[string]any{}, &v); err != nil {
		return err
	}
	return printJSON(v)
}

func clientResubmit(args []string) error {
	fs := flag.NewFlagSet("resubmit", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: zeptomesh-node resubmit <task_id>")
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodPost, "/api/v1/tasks/"+fs.Arg(0)+"/resubmit", map[string]any{}, &v); err != nil {
		return err
	}
	return printJSON(v)
}

// clientTriggers lists the node's schedules — stored and config-declared —
// with their next run (ТЗ 6.6.1 п.4).
func clientTriggers(args []string) error {
	fs := flag.NewFlagSet("triggers", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodGet, "/api/v1/triggers", nil, &v); err != nil {
		return err
	}
	return printJSON(v)
}

// clientTriggerAdd creates or replaces a stored trigger. The body mirrors the
// admin API document; a schedule whose id is pinned in node.yaml is refused by
// the node, because the config definition shadows the stored one.
func clientTriggerAdd(args []string) error {
	fs := flag.NewFlagSet("trigger-add", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	id := fs.String("id", "", "trigger id (required, unique)")
	schedule := fs.String("schedule", "", `cron expression, e.g. "0 3 * * *" (required)`)
	instruction := fs.String("instruction", "", "task instruction to inject (required)")
	name := fs.String("name", "", "human label for the schedule")
	ttl := fs.Int("ttl", 0, "task TTL in hops (0 = node default)")
	priority := fs.Int("priority", 0, "task priority 1..9 (0 = default)")
	timeout := fs.Int("timeout", 0, "execution timeout, seconds (0 = node default)")
	maxRuns := fs.Int("max-runs", 0, "stop after this many runs (0 = unlimited)")
	disabled := fs.Bool("disabled", false, "create the trigger switched off")
	allowShell := fs.Bool("allow-shell", false, "the scheduled task may use shell tools")
	allowNetwork := fs.Bool("allow-network", true, "the scheduled task may use network tools")
	var skills stringList
	fs.Var(&skills, "skill", "required skill (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*id) == "" || strings.TrimSpace(*schedule) == "" || strings.TrimSpace(*instruction) == "" {
		return fmt.Errorf("trigger-add: -id, -schedule and -instruction are required")
	}
	job := map[string]any{
		"instruction":         *instruction,
		"allow_network_tools": *allowNetwork,
	}
	if len(skills) > 0 {
		job["required_skills"] = []string(skills)
	}
	if *ttl != 0 {
		job["ttl"] = int32(*ttl)
	}
	if *priority != 0 {
		job["priority"] = int32(*priority)
	}
	if *timeout != 0 {
		job["timeout_seconds"] = int32(*timeout)
	}
	if *allowShell {
		job["allow_shell"] = true
	}
	body := map[string]any{"id": *id, "schedule": *schedule, "job": job}
	if strings.TrimSpace(*name) != "" {
		body["name"] = *name
	}
	if *maxRuns != 0 {
		body["max_runs"] = *maxRuns
	}
	if *disabled {
		body["enabled"] = false
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if err := c.do(context.Background(), http.MethodPost, "/api/v1/triggers", body, &v); err != nil {
		return err
	}
	return printJSON(v)
}

// clientTriggerRm deletes a stored trigger by id. Config-declared schedules
// live in node.yaml and cannot be removed here.
func clientTriggerRm(args []string) error {
	fs := flag.NewFlagSet("trigger-rm", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.Arg(0) == "" {
		return fmt.Errorf("usage: zeptomesh-node trigger-rm <trigger-id>")
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	path := "/api/v1/triggers/" + url.PathEscape(fs.Arg(0))
	if err := c.do(context.Background(), http.MethodDelete, path, nil, &v); err != nil {
		return err
	}
	return printJSON(v)
}

// clientScan probes the environment for other agents: the local host, the LAN
// and the Internet. Every agent found is dialed and its capabilities verified,
// so it becomes a neighbour. With -last it prints the previous report instead
// of running a new sweep.
func clientScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ContinueOnError)
	addr, tokenEnv := clientFlags(fs)
	var scopes stringList
	fs.Var(&scopes, "scope", "source to probe (repeatable: local, subnet, wan; default: all enabled)")
	maxCand := fs.Int("max-candidates", 0, "cap on peers dialed in one pass (0 = node default)")
	timeout := fs.Int("timeout", 0, "whole-pass budget, seconds (0 = node default)")
	last := fs.Bool("last", false, "print the most recent scan report instead of scanning")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c, err := dial(addr, tokenEnv)
	if err != nil {
		return err
	}
	var v any
	if *last {
		if err := c.do(context.Background(), http.MethodGet, "/api/v1/admin/scan", nil, &v); err != nil {
			return err
		}
		return printJSON(v)
	}
	body := map[string]any{}
	if len(scopes) > 0 {
		body["scopes"] = []string(scopes)
	}
	if *maxCand > 0 {
		body["max_candidates"] = *maxCand
	}
	if *timeout > 0 {
		body["timeout_seconds"] = *timeout
	}
	if err := c.do(context.Background(), http.MethodPost, "/api/v1/admin/scan", body, &v); err != nil {
		return err
	}
	return printJSON(v)
}
