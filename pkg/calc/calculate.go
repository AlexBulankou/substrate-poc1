// Package calc implements the substrate-poc1 calculate tool with session-keyed
// retry dedup. Two paths are covered:
//
//   - Path 1 (post-completion retry): on TCP drop AFTER the 20s tool wait, the
//     retry hits the session state cache and returns the original result
//     immediately.
//   - Path 2 (in-flight retry): on TCP drop DURING the 20s tool wait, the retry
//     calls into the same singleflight slot and blocks on the original work,
//     waking with the same result.
//
// Neither ADK Go nor ADK Python ships built-in tool-result memoization keyed by
// sessionID, so the dedup is implemented here as a tool wrapper. The
// singleflight primitive matches substrate atenet's ResumeActor pattern (see
// agent-substrate/substrate cmd/atenet/internal/app/router/resumer.go) and
// handles N>=3 concurrent waiters cleanly.
//
// ADK plumbing (SessionIDFromContext + StateStoreFromContext) is injected at
// construction so this package has zero dependency on the ADK Go SDK; unit
// tests run hermetically. The ADK-hosting main.go wires the real extractors:
//
//	calc.New(
//	    func(ctx context.Context) string {
//	        ic, _ := agent.InvocationContextFromContext(ctx)
//	        return ic.Session().ID()
//	    },
//	    func(ctx context.Context) calc.StateStore {
//	        ic, _ := agent.InvocationContextFromContext(ctx)
//	        return adkStateAdapter{ic.Session().State()}
//	    },
//	)
package calc

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
)

// Default simulated work duration. Mirrors Dima's PoC spec ("waits 20 seconds").
const DefaultWorkDuration = 20 * time.Second

// CalculateArgs is the JSON-marshaled tool input. Field name "expression" must
// match ADK tool registration metadata.
type CalculateArgs struct {
	Expression string `json:"expression"`
}

// CalculateResult is the JSON-marshaled tool output.
type CalculateResult struct {
	Value int `json:"value"`
}

// SessionIDFromContext extracts the actor session ID from a tool-invocation
// context. Wired by the ADK-hosting main.go.
type SessionIDFromContext func(ctx context.Context) string

// StateStore is the minimal session-state interface the tool needs. Maps to
// ADK Go's session.State (Get/Set methods).
type StateStore interface {
	Get(key string) (any, bool)
	Set(key string, value any)
}

// StateStoreFromContext extracts the per-session state store from a
// tool-invocation context. Wired by the ADK-hosting main.go.
type StateStoreFromContext func(ctx context.Context) StateStore

// Calculator wraps the tool entry-point with injectable plumbing.
type Calculator struct {
	sidFromCtx   SessionIDFromContext
	stateFromCtx StateStoreFromContext
	workDuration time.Duration
	sf           singleflight.Group
}

// New constructs a Calculator with production defaults (DefaultWorkDuration).
func New(sidFromCtx SessionIDFromContext, stateFromCtx StateStoreFromContext) *Calculator {
	return &Calculator{
		sidFromCtx:   sidFromCtx,
		stateFromCtx: stateFromCtx,
		workDuration: DefaultWorkDuration,
	}
}

// WithWorkDuration overrides the simulated work duration. For tests.
func (c *Calculator) WithWorkDuration(d time.Duration) *Calculator {
	c.workDuration = d
	return c
}

// Calculate is the tool entry-point. ADK registers via tool.MustFromFunc which
// reflects over (ctx, args) -> (result, error).
func (c *Calculator) Calculate(ctx context.Context, args CalculateArgs) (CalculateResult, error) {
	sid := c.sidFromCtx(ctx)
	state := c.stateFromCtx(ctx)
	key := cleanCalcKey(args.Expression)
	cacheKey := "calc:" + key

	// Path 1: post-completion retry — state cache hit returns immediately.
	if v, ok := state.Get(cacheKey); ok {
		if r, ok := v.(int); ok {
			return CalculateResult{Value: r}, nil
		}
	}

	// Path 2: in-flight retry — singleflight keyed on session+expr.
	// Different sessions get distinct slots so concurrent unrelated calls don't
	// collide. Same session+expr concurrent calls (N>=2) all block on the
	// same future; the leader runs once.
	sfKey := sid + "|" + key
	raw, err, _ := c.sf.Do(sfKey, func() (any, error) {
		time.Sleep(c.workDuration)
		r, evalErr := eval(args.Expression)
		if evalErr != nil {
			return 0, evalErr
		}
		state.Set(cacheKey, r)
		return r, nil
	})
	if err != nil {
		return CalculateResult{}, err
	}
	return CalculateResult{Value: raw.(int)}, nil
}

// cleanCalcKey canonicalizes expressions so "5+10" and "5 + 10" share a cache
// slot. Whitespace-only normalization for the PoC; more sophisticated
// canonicalization (operator-order, parens) is out of scope.
func cleanCalcKey(expr string) string {
	return strings.ReplaceAll(expr, " ", "")
}

// eval is a minimal expression evaluator: two integer operands joined by one
// of + - * /. Matches Dima's PoC spec example ("calculate 5+10") with a small
// extension to cover the other three binary ops. Recursive expressions and
// negative literals are out of PoC scope.
func eval(expr string) (int, error) {
	s := strings.ReplaceAll(expr, " ", "")
	for i := 1; i < len(s); i++ {
		switch s[i] {
		case '+', '-', '*', '/':
			lhs, err := strconv.Atoi(s[:i])
			if err != nil {
				return 0, fmt.Errorf("calc: parse lhs %q: %w", s[:i], err)
			}
			rhs, err := strconv.Atoi(s[i+1:])
			if err != nil {
				return 0, fmt.Errorf("calc: parse rhs %q: %w", s[i+1:], err)
			}
			switch s[i] {
			case '+':
				return lhs + rhs, nil
			case '-':
				return lhs - rhs, nil
			case '*':
				return lhs * rhs, nil
			case '/':
				if rhs == 0 {
					return 0, fmt.Errorf("calc: division by zero")
				}
				return lhs / rhs, nil
			}
		}
	}
	return 0, fmt.Errorf("calc: no operator found in %q", expr)
}
