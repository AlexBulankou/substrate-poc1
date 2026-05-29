//go:build e2e

// Package e2e is the substrate-poc1 local-mode end-to-end harness for issue #5.
//
// It exercises the full client<->server<->dedup path over a REAL TCP socket:
// the real cmd/calc-client binary (with its real conn-drop transport) drives a
// test HTTP server that wraps the real pkg/calc.Calculator. This validates the
// dedup INVARIANT end-to-end — the thing the PoC exists to prove — without the
// substrate cluster, without Gemini, and without the #782 ate-system install.
//
// What this DOES validate (runnable now):
//   - Case 1 Happy path: client -> workDuration wait -> result.
//   - Case 2 Path 1 (post-completion retry): conn drops AFTER the result is
//     read; the retry hits pkg/calc's session state cache and returns the
//     original value without re-running the work.
//   - Case 3 Path 2 (in-flight retry): conn drops DURING the work; the retry
//     joins the same singleflight slot and both wake with one shared result —
//     the server runs the work exactly ONCE (evalCount==1).
//   - Case 4 Concurrent same-sessionID: two client processes race the same
//     session+expr; singleflight collapses them to one execution (evalCount==1).
//
// What this does NOT validate (deferred, see README "Cluster-mode"):
//   - substrate atenet routing (same-actorID-same-process). Gated on #782.
//   - CRIU checkpoint/restore preserving the dedup primitives (the §4 smoke
//     trio). Gated on #782 + runsc.
//
// The test server is a faithful stand-in for the agent's direct-tool endpoint:
// one long-lived Calculator (shared singleflight.Group) + per-session state,
// keyed on the Host header the client sets to the sessionID. cmd/agent now
// exposes the matching /v1/calculate route (#10), driving the same Calculator;
// cmd/agent/direct_route_test.go asserts the same evalCount==1 dedup signal
// directly against that real route, so the stand-in here and the real actor
// stay in sync.
//
// Run: `go test -tags e2e -v ./test/e2e` (or `make demo`).
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexBulankou/substrate-poc1/pkg/calc"
)

// clientBin is the path to the calc-client binary built once in TestMain.
var clientBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "poc1-e2e-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: mkdtemp: %v\n", err)
		os.Exit(1)
	}
	clientBin = filepath.Join(dir, "calc-client")
	build := exec.Command("go", "build", "-o", clientBin,
		"github.com/AlexBulankou/substrate-poc1/cmd/calc-client")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: build calc-client: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// --- test server: faithful stand-in for the actor direct-tool endpoint -------

type ctxKey int

const (
	sessKey ctxKey = iota
	stateKey
)

// countingState wraps a per-session map and bumps a server-wide eval counter on
// each Set. pkg/calc calls Set exactly once per ACTUAL execution (inside the
// singleflight leader), so the counter equals the number of real evaluations —
// the dedup signal cases 3 and 4 assert on.
type countingState struct {
	mu        sync.Mutex
	m         map[string]any
	evalCount *atomic.Int32
}

func (s *countingState) Get(k string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[k]
	return v, ok
}

func (s *countingState) Set(k string, v any) {
	s.evalCount.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[k] = v
}

// sessionStore hands out one countingState per sessionID, persisting across
// requests for that session (so Path 1's post-completion retry sees the cache).
type sessionStore struct {
	mu        sync.Mutex
	byID      map[string]*countingState
	evalCount *atomic.Int32
}

func (s *sessionStore) get(sid string) *countingState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.byID[sid]
	if !ok {
		st = &countingState{m: map[string]any{}, evalCount: s.evalCount}
		s.byID[sid] = st
	}
	return st
}

// newTestServer starts an httptest server wrapping a single long-lived
// Calculator with the given simulated work duration. Returns the server and the
// server-wide eval counter.
func newTestServer(workDur time.Duration) (*httptest.Server, *atomic.Int32) {
	var evalCount atomic.Int32
	store := &sessionStore{byID: map[string]*countingState{}, evalCount: &evalCount}

	c := calc.New(
		func(ctx context.Context) (string, error) {
			return ctx.Value(sessKey).(string), nil
		},
		func(ctx context.Context) (calc.StateStore, error) {
			return ctx.Value(stateKey).(calc.StateStore), nil
		},
	).WithWorkDuration(workDur)

	h := func(w http.ResponseWriter, r *http.Request) {
		sid := r.Host // client sets req.Host = sessionID (atenet routing shape)
		var req struct {
			Expression string `json:"expression"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
			return
		}
		st := store.get(sid)
		// Base on r.Context() so a client disconnect cancels it — pkg/calc uses
		// time.Sleep (not ctx-aware) so the singleflight leader keeps running
		// regardless, which is exactly the Path 2 behavior we want to prove.
		ctx := context.WithValue(context.WithValue(r.Context(), sessKey, sid), stateKey, calc.StateStore(st))
		res, err := c.Calculate(ctx, calc.CalculateArgs{Expression: req.Expression})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]int{"value": res.Value})
	}
	return httptest.NewServer(http.HandlerFunc(h)), &evalCount
}

// --- client driver -----------------------------------------------------------

var resultRe = regexp.MustCompile(`RESULT: value=(-?\d+) \(attempt (\d+)\)`)

type clientResult struct {
	value   int
	attempt int
	elapsed time.Duration
	stderr  string
}

// runClient runs the calc-client binary with the given flags and parses its
// "RESULT: value=N (attempt M)" stderr line.
func runClient(t *testing.T, url string, args ...string) (clientResult, error) {
	t.Helper()
	full := append([]string{"-url", url}, args...)
	cmd := exec.Command(clientBin, full...)
	var se bytes.Buffer
	cmd.Stderr = &se
	start := time.Now()
	err := cmd.Run()
	elapsed := time.Since(start)
	res := clientResult{elapsed: elapsed, stderr: se.String()}
	if err != nil {
		return res, fmt.Errorf("calc-client exited non-zero: %w\nstderr:\n%s", err, res.stderr)
	}
	m := resultRe.FindStringSubmatch(res.stderr)
	if m == nil {
		return res, fmt.Errorf("no RESULT line in stderr:\n%s", res.stderr)
	}
	res.value, _ = strconv.Atoi(m[1])
	res.attempt, _ = strconv.Atoi(m[2])
	return res, nil
}

// --- the 4-case matrix -------------------------------------------------------

func TestE2E_Case1_HappyPath(t *testing.T) {
	const workDur = 2 * time.Second
	ts, evalCount := newTestServer(workDur)
	defer ts.Close()

	t.Logf("T+0.0s  client POST session=sid-happy expr=5+10 (server work=%s)", workDur)
	r, err := runClient(t, ts.URL, "-session", "sid-happy", "-expr", "5+10")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("T+%.1fs  result value=%d attempt=%d", r.elapsed.Seconds(), r.value, r.attempt)

	if r.value != 15 {
		t.Errorf("value=%d, want 15", r.value)
	}
	if r.attempt != 0 {
		t.Errorf("attempt=%d, want 0 (no retry on happy path)", r.attempt)
	}
	if r.elapsed < workDur {
		t.Errorf("returned in %s < workDuration %s — work did not run", r.elapsed, workDur)
	}
	if got := evalCount.Load(); got != 1 {
		t.Errorf("evalCount=%d, want 1", got)
	}
}

// Case 2 — Path 1 (post-completion retry): the conn drops AFTER the client has
// read the result. The retry hits the session state cache and returns the
// original value WITHOUT re-running the work, so total time stays ~one
// workDuration (not two) and the server evaluates exactly once.
func TestE2E_Case2_Path1_PostCompletion(t *testing.T) {
	const workDur = 2 * time.Second
	ts, evalCount := newTestServer(workDur)
	defer ts.Close()

	t.Logf("T+0.0s  client POST session=sid-path1 expr=7+8")
	t.Logf("        ...server works %s, returns result, client reads it...", workDur)
	t.Logf("        client drops conn AFTER reading -> retries (attempt 1)")
	r, err := runClient(t, ts.URL,
		"-session", "sid-path1", "-expr", "7+8",
		"-simulate-drop-at", "after-completion", "-retry-gap", "200ms")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("T+%.1fs  result value=%d attempt=%d (cache hit, no re-run)", r.elapsed.Seconds(), r.value, r.attempt)

	if r.value != 15 {
		t.Errorf("value=%d, want 15", r.value)
	}
	if r.attempt != 1 {
		t.Errorf("attempt=%d, want 1 (post-completion drop forces one retry)", r.attempt)
	}
	if r.elapsed >= 2*workDur {
		t.Errorf("elapsed %s >= 2*workDuration — retry re-ran the work instead of hitting cache", r.elapsed)
	}
	if got := evalCount.Load(); got != 1 {
		t.Errorf("evalCount=%d, want 1 (Path 1 cache hit must not re-evaluate)", got)
	}
}

// Case 3 — Path 2 (in-flight retry): the conn drops DURING the work (t=2s of a
// 5s job). The retry (t=3s) arrives while the original is still running, joins
// the same singleflight slot, and both wake at t=5s with one shared result. The
// server runs the work exactly ONCE.
func TestE2E_Case3_Path2_InFlight(t *testing.T) {
	const workDur = 5 * time.Second
	ts, evalCount := newTestServer(workDur)
	defer ts.Close()

	t.Logf("T+0.0s  client POST session=sid-path2 expr=6*7 (server work=%s)", workDur)
	t.Logf("T+2.0s  client drops conn (work still in flight)")
	t.Logf("T+3.0s  client re-POSTs -> joins singleflight slot, blocks")
	t.Logf("T+5.0s  leader wakes; both calls share one result")
	r, err := runClient(t, ts.URL,
		"-session", "sid-path2", "-expr", "6*7",
		"-simulate-drop-at", "2", "-retry-gap", "1s")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("T+%.1fs  result value=%d attempt=%d", r.elapsed.Seconds(), r.value, r.attempt)

	if r.value != 42 {
		t.Errorf("value=%d, want 42", r.value)
	}
	if r.attempt != 1 {
		t.Errorf("attempt=%d, want 1 (in-flight drop forces one retry)", r.attempt)
	}
	if r.elapsed >= 2*workDur {
		t.Errorf("elapsed %s >= 2*workDuration — retry ran a second evaluation instead of joining singleflight", r.elapsed)
	}
	if got := evalCount.Load(); got != 1 {
		t.Errorf("evalCount=%d, want 1 (singleflight must collapse in-flight retry)", got)
	}
}

// Case 4 — Concurrent same-sessionID: two client processes race the same
// session+expr with no drop. singleflight collapses them to one execution; both
// receive the same value.
func TestE2E_Case4_ConcurrentSameSession(t *testing.T) {
	const workDur = 3 * time.Second
	ts, evalCount := newTestServer(workDur)
	defer ts.Close()

	t.Logf("T+0.0s  TWO clients POST session=sid-concurrent expr=100*200 simultaneously")
	t.Logf("        ...singleflight: leader runs, second blocks on the same future...")

	type out struct {
		r   clientResult
		err error
	}
	results := make([]out, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, err := runClient(t, ts.URL, "-session", "sid-concurrent", "-expr", "100*200")
			results[idx] = out{r, err}
		}(i)
	}
	wg.Wait()

	for i, o := range results {
		if o.err != nil {
			t.Fatalf("client %d: %v", i, o.err)
		}
		t.Logf("T+%.1fs  client %d result value=%d attempt=%d", o.r.elapsed.Seconds(), i, o.r.value, o.r.attempt)
		if o.r.value != 20000 {
			t.Errorf("client %d value=%d, want 20000", i, o.r.value)
		}
		if o.r.elapsed >= 2*workDur {
			t.Errorf("client %d elapsed %s >= 2*workDuration — calls ran serially, not deduped", i, o.r.elapsed)
		}
	}
	if got := evalCount.Load(); got != 1 {
		t.Errorf("evalCount=%d, want 1 (concurrent same-session must collapse to one eval)", got)
	}
}
