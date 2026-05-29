// Tests for the /v1/calculate direct-tool route (poc1 #10). These exercise the
// REAL handler + REAL dual-mode Calculator wiring (no Gemini / GOOGLE_API_KEY),
// closing the gap the e2e harness (test/e2e) called out: that harness proves the
// dedup invariant against a faithful stand-in, but the merged cmd/agent did not
// expose the route the cluster client actually POSTs to. Here we drive the
// genuine handler and assert the same evalCount==1 dedup signal.
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexBulankou/substrate-poc1/pkg/calc"
)

// countingState bumps a shared counter on each Set. pkg/calc calls Set exactly
// once per ACTUAL evaluation (inside the singleflight leader), so the counter
// equals the number of real evaluations — the dedup signal.
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

// countingStore is a test sessionStateProvider that hands out persistent
// countingStates sharing one eval counter.
type countingStore struct {
	mu        sync.Mutex
	byID      map[string]*countingState
	evalCount *atomic.Int32
}

func newCountingStore() *countingStore {
	return &countingStore{byID: map[string]*countingState{}, evalCount: &atomic.Int32{}}
}

func (s *countingStore) get(sid string) calc.StateStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.byID[sid]
	if !ok {
		st = &countingState{m: map[string]any{}, evalCount: s.evalCount}
		s.byID[sid] = st
	}
	return st
}

// newDirectTestServer wires the real calcDirectHandler around the real
// newCalculator (dual-mode extractors) with a short work duration.
func newDirectTestServer(workDur time.Duration) (*httptest.Server, *countingStore) {
	store := newCountingStore()
	c := newCalculator(store).WithWorkDuration(workDur)
	mux := http.NewServeMux()
	mux.Handle("/v1/calculate", calcDirectHandler(c, store))
	return httptest.NewServer(mux), store
}

// postCalc sends one POST /v1/calculate with the sessionID in the Host header
// (the atenet routing shape the client uses) and returns the decoded value.
func postCalc(t *testing.T, url, sid, expr string) (int, int) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"expression": expr})
	req, err := http.NewRequest(http.MethodPost, url+"/v1/calculate", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = sid
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, rb)
	}
	var out struct {
		Value int `json:"value"`
	}
	if err := json.Unmarshal(rb, &out); err != nil {
		t.Fatalf("unmarshal %q: %v", rb, err)
	}
	return out.Value, resp.StatusCode
}

func TestDirectRoute_HappyPath(t *testing.T) {
	ts, store := newDirectTestServer(50 * time.Millisecond)
	defer ts.Close()

	v, _ := postCalc(t, ts.URL, "sid-happy", "5+10")
	if v != 15 {
		t.Errorf("value=%d, want 15", v)
	}
	if got := store.evalCount.Load(); got != 1 {
		t.Errorf("evalCount=%d, want 1", got)
	}
}

// Path 1: a sequential second request for the same session+expr hits the state
// cache and returns the original value without re-evaluating.
func TestDirectRoute_Path1_CacheHit(t *testing.T) {
	ts, store := newDirectTestServer(50 * time.Millisecond)
	defer ts.Close()

	if v, _ := postCalc(t, ts.URL, "sid-p1", "7+8"); v != 15 {
		t.Fatalf("first value=%d, want 15", v)
	}
	if v, _ := postCalc(t, ts.URL, "sid-p1", "7+8"); v != 15 {
		t.Fatalf("retry value=%d, want 15", v)
	}
	if got := store.evalCount.Load(); got != 1 {
		t.Errorf("evalCount=%d, want 1 (cache hit must not re-evaluate)", got)
	}
}

// Path 2: two concurrent requests for the same session+expr collapse to one
// evaluation via singleflight; both receive the same value.
func TestDirectRoute_Path2_ConcurrentSingleflight(t *testing.T) {
	ts, store := newDirectTestServer(500 * time.Millisecond)
	defer ts.Close()

	var wg sync.WaitGroup
	vals := make([]int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			v, _ := postCalc(t, ts.URL, "sid-p2", "6*7")
			vals[idx] = v
		}(i)
	}
	wg.Wait()

	for i, v := range vals {
		if v != 42 {
			t.Errorf("client %d value=%d, want 42", i, v)
		}
	}
	if got := store.evalCount.Load(); got != 1 {
		t.Errorf("evalCount=%d, want 1 (singleflight must collapse concurrent same-session)", got)
	}
}

// Distinct sessions get distinct singleflight slots and evaluate independently.
func TestDirectRoute_DistinctSessions(t *testing.T) {
	ts, store := newDirectTestServer(50 * time.Millisecond)
	defer ts.Close()

	if v, _ := postCalc(t, ts.URL, "sid-a", "2+2"); v != 4 {
		t.Errorf("sid-a value=%d, want 4", v)
	}
	if v, _ := postCalc(t, ts.URL, "sid-b", "3+3"); v != 6 {
		t.Errorf("sid-b value=%d, want 6", v)
	}
	if got := store.evalCount.Load(); got != 2 {
		t.Errorf("evalCount=%d, want 2 (distinct sessions evaluate independently)", got)
	}
}

func TestDirectRoute_MethodNotAllowed(t *testing.T) {
	ts, _ := newDirectTestServer(50 * time.Millisecond)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/v1/calculate", nil)
	req.Host = "sid-x"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status=%d, want 405", resp.StatusCode)
	}
}

func TestDirectRoute_BadBody(t *testing.T) {
	ts, _ := newDirectTestServer(50 * time.Millisecond)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodPost, ts.URL+"/v1/calculate", bytes.NewReader([]byte("{not json")))
	req.Host = "sid-x"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status=%d, want 400", resp.StatusCode)
	}
}
