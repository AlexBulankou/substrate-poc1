// Package main hosts the substrate-poc1 ADK agent. It registers the
// calculate tool from pkg/calc (which carries the singleflight + state-cache
// dedup), wires a real ADK Gemini llmagent around it, and serves two HTTP
// surfaces. Substrate atenet routes per-session traffic to this process
// (actorID = Host header); pkg/calc.Calculator handles the actual
// session-keyed dedup.
//
//   - /api/ — the ADK REST surface (LLM-driven). The llmagent wiring is the
//     production integration shape, included so the agent can also be driven by
//     an LLM caller. Not exercised by the PoC e2e dedup invariant.
//   - /v1/calculate — the direct-tool route (#10). Substrate's PoC client
//     (cmd/calc-client) POSTs {"expression": "..."} here with the sessionID in
//     the Host header and decodes {"value": <int>}. This is the contract the
//     local e2e harness (test/e2e) proves the dedup invariant against.
//
// Both surfaces drive the SAME long-lived pkg/calc.Calculator, so the
// singleflight.Group + per-session cache dedup is shared across them.
//
// Required environment:
//   - GOOGLE_API_KEY   — for the Gemini model (substrate ActorTemplate env)
//
// Optional:
//   - PORT             — HTTP listen port (default 8080)
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"google.golang.org/genai"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/agent/llmagent"
	"google.golang.org/adk/model/gemini"
	"google.golang.org/adk/server/adkrest"
	"google.golang.org/adk/session"
	"google.golang.org/adk/tool"
	"google.golang.org/adk/tool/functiontool"

	"github.com/AlexBulankou/substrate-poc1/pkg/calc"
)

const (
	agentName       = "substrate-poc1-agent"
	toolName        = "calculate"
	toolDescription = "Parses an expression like '5+10', waits 20 seconds, returns the integer result. Session-keyed dedup: retries within the same session return the original result."
	defaultModel    = "gemini-3.1-flash-lite"
)

// adkStateAdapter wraps ADK Go's session.State so it satisfies the
// pkg/calc.StateStore (key, ok)-pattern interface. ADK's State.Get returns
// (any, error) with ErrStateKeyNotExist on miss; we map that — and any
// other error — to (nil, false) for the ok-pattern consumer. Cache-only
// usage in pkg/calc makes the error-class distinction unimportant in the
// PoC; if a future caller needs to surface ADK-side errors, this is the
// single place to change.
type adkStateAdapter struct {
	s session.State
}

func (a adkStateAdapter) Get(key string) (any, bool) {
	v, err := a.s.Get(key)
	if err != nil {
		return nil, false
	}
	return v, true
}

func (a adkStateAdapter) Set(key string, value any) {
	// pkg/calc.StateStore.Set has no error return; ADK's State.Set returns one.
	// A Set failure means Path 1 (cache-hit on retry) won't fire, but Path 2
	// dedup still works — degraded, not incorrect. Log so it surfaces.
	if err := a.s.Set(key, value); err != nil {
		slog.Warn("state.Set failed", "key", key, "value_type", fmt.Sprintf("%T", value), "err", err)
	}
}

// extractToolContext is the shared unwrap. pkg/calc takes a context.Context
// (zero-ADK-dep at registration time) and the tool handler passes a
// tool.Context — since tool.Context embeds context.Context via
// agent.ReadonlyContext, a type-assertion gets us the ADK accessors back.
func extractToolContext(ctx context.Context) (tool.Context, error) {
	tc, ok := ctx.(tool.Context)
	if !ok {
		return nil, fmt.Errorf("ctx is not tool.Context: %T", ctx)
	}
	return tc, nil
}

// --- /v1/calculate direct-tool route (cluster-mode client/agent contract) ----
//
// substrate's cmd/calc-client POSTs {"expression": "..."} to {actor}/v1/calculate
// with the sessionID carried in the Host header (atenet routes by actorID=Host),
// and decodes {"value": <int>}. The ADK /api/ surface is LLM-driven and speaks a
// different protocol, so the client needs this direct route. It drives the SAME
// long-lived Calculator the ADK tool wraps — shared singleflight.Group + the
// same dedup invariant (Path 1 cache hit + Path 2 in-flight join) the local e2e
// harness (test/e2e) proves against an identical stand-in.

type httpCtxKey int

const (
	httpSessKey httpCtxKey = iota
	httpStateKey
)

// mapState is a per-session calc.StateStore backed by a guarded map. Mirrors
// the e2e harness's per-session state so Path 1 (post-completion retry) sees the
// cache across separate HTTP requests for the same session.
type mapState struct {
	mu sync.Mutex
	m  map[string]any
}

func (s *mapState) Get(k string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok
}

func (s *mapState) Set(k string, v any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] = v
}

// sessionStateProvider hands out one persistent calc.StateStore per sessionID.
// Small interface (one impl: httpSessionStore) so the direct-route handler is
// testable with an eval-counting store, matching the e2e harness's countingState.
type sessionStateProvider interface {
	get(sid string) calc.StateStore
}

// httpSessionStore is the production sessionStateProvider: one mapState per
// sessionID, persisting across requests for that session.
type httpSessionStore struct {
	mu   sync.Mutex
	byID map[string]*mapState
}

func newHTTPSessionStore() *httpSessionStore {
	return &httpSessionStore{byID: map[string]*mapState{}}
}

func (s *httpSessionStore) get(sid string) calc.StateStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.byID[sid]
	if !ok {
		st = &mapState{m: map[string]any{}}
		s.byID[sid] = st
	}
	return st
}

// newCalculator builds the single long-lived Calculator. Its extractors are
// dual-mode: the /v1/calculate route stashes sessionID + StateStore as context
// values (HTTP branch), while the ADK functiontool path passes a tool.Context
// (ADK branch). One Calculator → one shared singleflight.Group across both
// surfaces, which is the dedup property the PoC proves.
func newCalculator(httpStore sessionStateProvider) *calc.Calculator {
	return calc.New(
		func(ctx context.Context) (string, error) {
			if sid, ok := ctx.Value(httpSessKey).(string); ok {
				if sid == "" {
					return "", fmt.Errorf("empty session id (Host header)")
				}
				return sid, nil
			}
			tc, err := extractToolContext(ctx)
			if err != nil {
				return "", err
			}
			return tc.SessionID(), nil
		},
		func(ctx context.Context) (calc.StateStore, error) {
			if st, ok := ctx.Value(httpStateKey).(calc.StateStore); ok {
				return st, nil
			}
			tc, err := extractToolContext(ctx)
			if err != nil {
				return nil, err
			}
			return adkStateAdapter{s: tc.State()}, nil
		},
	)
}

// calcDirectHandler builds the /v1/calculate HTTP handler. Split out of main()
// so it's testable without a Gemini model / GOOGLE_API_KEY.
func calcDirectHandler(c *calc.Calculator, store sessionStateProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		sid := r.Host // atenet routes by Host header → actorID=sessionID
		if sid == "" {
			http.Error(w, "missing Host header (session id)", http.StatusBadRequest)
			return
		}
		var args calc.CalculateArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		st := store.get(sid)
		ctx := context.WithValue(context.WithValue(r.Context(), httpSessKey, sid), httpStateKey, st)
		res, err := c.Calculate(ctx, args)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(calc.CalculateResult{Value: res.Value})
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	apiKey := os.Getenv("GOOGLE_API_KEY")
	if apiKey == "" {
		slog.Error("GOOGLE_API_KEY is required")
		os.Exit(1)
	}

	ctx := context.Background()

	model, err := gemini.NewModel(ctx, defaultModel, &genai.ClientConfig{APIKey: apiKey})
	if err != nil {
		slog.Error("create gemini model", "err", err)
		os.Exit(1)
	}

	// Single long-lived Calculator — the singleflight.Group state across all
	// invocations is what dedupes concurrent same-session callers. A fresh
	// Calculator per invocation would defeat Path 2 (in-flight retry). The
	// httpStore backs the /v1/calculate direct route's per-session state.
	httpStore := newHTTPSessionStore()
	c := newCalculator(httpStore)

	handler := func(tc tool.Context, args calc.CalculateArgs) (calc.CalculateResult, error) {
		return c.Calculate(tc, args)
	}

	calcTool, err := functiontool.New(functiontool.Config{
		Name:        toolName,
		Description: toolDescription,
	}, handler)
	if err != nil {
		slog.Error("register calculate tool", "err", err)
		os.Exit(1)
	}

	a, err := llmagent.New(llmagent.Config{
		Name:        agentName,
		Model:       model,
		Description: "PoC agent demonstrating session-keyed tool-result dedup across actor connection drops via substrate atenet.",
		Instruction: "Use the calculate tool to evaluate arithmetic expressions. The tool simulates 20s of work to model real long-running computation.",
		Tools:       []tool.Tool{calcTool},
	})
	if err != nil {
		slog.Error("create llmagent", "err", err)
		os.Exit(1)
	}

	restServer, err := adkrest.NewServer(adkrest.ServerConfig{
		AgentLoader:     agent.NewSingleLoader(a),
		SessionService:  session.InMemoryService(),
		SSEWriteTimeout: 120 * time.Second,
	})
	if err != nil {
		slog.Error("create rest server", "err", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/api/", http.StripPrefix("/api", restServer))
	mux.Handle("/v1/calculate", calcDirectHandler(c, httpStore))
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port

	slog.Info("agent ready", "addr", addr, "tool", toolName, "agent", agentName, "routes", "/api/,/v1/calculate,/health")
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}
