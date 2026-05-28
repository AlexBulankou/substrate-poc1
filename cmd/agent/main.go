// Package main hosts the substrate-poc1 ADK agent. It registers the
// calculate tool from pkg/calc (which carries the singleflight + state-cache
// dedup), wires a real ADK Gemini llmagent around it, and serves the ADK
// REST API over HTTP. Substrate atenet routes per-session traffic to this
// process; pkg/calc.Calculator handles the actual session-keyed dedup.
//
// Substrate's PoC client (cmd/calc-client) talks directly to this server's
// tool endpoint by sessionID — the LLM path is not exercised in the PoC e2e
// invariant ("retry returns original result"), but the llmagent wiring is the
// production integration shape and is included so the agent can also be
// driven by an LLM caller if needed later.
//
// Required environment:
//   - GOOGLE_API_KEY   — for the Gemini model (substrate ActorTemplate env)
//
// Optional:
//   - PORT             — HTTP listen port (default 8080)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
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
	// Calculator per invocation would defeat Path 2 (in-flight retry).
	c := calc.New(
		func(ctx context.Context) (string, error) {
			tc, err := extractToolContext(ctx)
			if err != nil {
				return "", err
			}
			return tc.SessionID(), nil
		},
		func(ctx context.Context) (calc.StateStore, error) {
			tc, err := extractToolContext(ctx)
			if err != nil {
				return nil, err
			}
			return adkStateAdapter{s: tc.State()}, nil
		},
	)

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
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	addr := ":" + port

	slog.Info("agent ready", "addr", addr, "tool", toolName, "agent", agentName)
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
