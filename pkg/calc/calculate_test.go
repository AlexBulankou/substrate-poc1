package calc

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mapState is a test mock for the StateStore interface.
type mapState struct {
	mu sync.Mutex
	m  map[string]any
}

func newMapState() *mapState { return &mapState{m: map[string]any{}} }

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

// stateKey/sessionKey are typed context keys for per-test plumbing.
type ctxKey int

const (
	stateKey ctxKey = iota
	sessionKey
)

func ctxWith(sid string, st StateStore) context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, sessionKey, sid)
	ctx = context.WithValue(ctx, stateKey, st)
	return ctx
}

func sidFromCtx(ctx context.Context) string { return ctx.Value(sessionKey).(string) }
func stateFromCtx(ctx context.Context) StateStore {
	return ctx.Value(stateKey).(StateStore)
}

// newTestCalc builds a Calculator with a short work duration so tests run fast.
func newTestCalc(workDur time.Duration) *Calculator {
	return New(sidFromCtx, stateFromCtx).WithWorkDuration(workDur)
}

func TestCleanCalcKey(t *testing.T) {
	cases := []struct{ in, want string }{
		{"5+10", "5+10"},
		{"5 + 10", "5+10"},
		{"  5  +  10  ", "5+10"},
		{"100*200", "100*200"},
	}
	for _, c := range cases {
		if got := cleanCalcKey(c.in); got != c.want {
			t.Errorf("cleanCalcKey(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestEval(t *testing.T) {
	cases := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"5+10", 15, false},
		{"5 + 10", 15, false},
		{"20-7", 13, false},
		{"6*7", 42, false},
		{"100/4", 25, false},
		{"10/0", 0, true},
		{"bad", 0, true},
		{"5+", 0, true},
	}
	for _, c := range cases {
		got, err := eval(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("eval(%q) = %d, want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("eval(%q) unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("eval(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// TestCalculate_HappyPath: first call returns the eval after waiting one full
// workDuration.
func TestCalculate_HappyPath(t *testing.T) {
	calc := newTestCalc(50 * time.Millisecond)
	ctx := ctxWith("sid-happy", newMapState())

	start := time.Now()
	got, err := calc.Calculate(ctx, CalculateArgs{Expression: "5+10"})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Calculate: %v", err)
	}
	if got.Value != 15 {
		t.Errorf("Value = %d, want 15", got.Value)
	}
	if elapsed < 40*time.Millisecond {
		t.Errorf("first call returned too fast: %v < workDuration", elapsed)
	}
}

// TestCalculate_Path1_CacheHit: second call for same session+expr returns from
// the state cache (much faster than workDuration).
func TestCalculate_Path1_CacheHit(t *testing.T) {
	calc := newTestCalc(50 * time.Millisecond)
	state := newMapState()
	ctx := ctxWith("sid-path1", state)

	// Prime the cache.
	_, err := calc.Calculate(ctx, CalculateArgs{Expression: "7+8"})
	if err != nil {
		t.Fatalf("prime: %v", err)
	}

	// Second call should hit the cache and return immediately.
	start := time.Now()
	got, err := calc.Calculate(ctx, CalculateArgs{Expression: "7+8"})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got.Value != 15 {
		t.Errorf("Value = %d, want 15", got.Value)
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("cache hit took too long: %v (expected ~0)", elapsed)
	}
}

// TestCalculate_Path1_CanonicalCache: "5+10" and "5 + 10" share a cache slot.
func TestCalculate_Path1_CanonicalCache(t *testing.T) {
	calc := newTestCalc(50 * time.Millisecond)
	state := newMapState()
	ctx := ctxWith("sid-canonical", state)

	_, err := calc.Calculate(ctx, CalculateArgs{Expression: "5+10"})
	if err != nil {
		t.Fatalf("prime: %v", err)
	}

	start := time.Now()
	got, err := calc.Calculate(ctx, CalculateArgs{Expression: "5 + 10"})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("retry with spaces: %v", err)
	}
	if got.Value != 15 {
		t.Errorf("Value = %d, want 15", got.Value)
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("canonical retry took too long: %v", elapsed)
	}
}

// TestCalculate_Path2_InFlightDedup: N=3 concurrent goroutines with same
// session+expr — only ONE actual eval runs, all 3 receive the same result.
// Guards against the chan-cap=1 bug singleflight was chosen to avoid.
func TestCalculate_Path2_InFlightDedup(t *testing.T) {
	const N = 3
	const workDur = 100 * time.Millisecond
	var evalCount atomic.Int32

	// Custom calculator that increments evalCount inside the singleflight slot
	// so we can prove only one execution ran.
	c := &Calculator{
		sidFromCtx:   sidFromCtx,
		stateFromCtx: stateFromCtx,
		workDuration: workDur,
	}
	// Wrap eval via state to count — but simpler: just count via a test-only
	// side channel. Use the StateStore Set to detect first vs cached.
	state := &countingState{inner: newMapState(), evalCount: &evalCount}
	ctx := ctxWith("sid-path2", state)

	results := make([]int, N)
	errs := make([]error, N)
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r, err := c.Calculate(ctx, CalculateArgs{Expression: "5+10"})
			results[idx] = r.Value
			errs[idx] = err
		}(i)
		// Tiny stagger so all 3 enter the singleflight Do concurrently but with
		// a deterministic-enough order for the test.
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
	elapsed := time.Since(start)

	// All N callers should have got the same correct value.
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: err=%v", i, errs[i])
		}
		if results[i] != 15 {
			t.Errorf("goroutine %d: Value=%d, want 15", i, results[i])
		}
	}
	// Only ONE eval should have actually run (counted via state.Set).
	if got := evalCount.Load(); got != 1 {
		t.Errorf("evalCount = %d, want 1 (singleflight collapse failed)", got)
	}
	// Total elapsed should be ~workDur + the staggers, not N*workDur.
	if elapsed > 2*workDur {
		t.Errorf("elapsed %v > 2*workDur — calls ran serially, not deduped", elapsed)
	}
}

// TestCalculate_DifferentSessionsIndependent: two sessions get their own
// singleflight slots, so both runs happen.
func TestCalculate_DifferentSessionsIndependent(t *testing.T) {
	workDur := 50 * time.Millisecond
	var evalCount atomic.Int32
	c := &Calculator{
		sidFromCtx:   sidFromCtx,
		stateFromCtx: stateFromCtx,
		workDuration: workDur,
	}

	state1 := &countingState{inner: newMapState(), evalCount: &evalCount}
	state2 := &countingState{inner: newMapState(), evalCount: &evalCount}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = c.Calculate(ctxWith("sid-A", state1), CalculateArgs{Expression: "5+10"})
	}()
	go func() {
		defer wg.Done()
		_, _ = c.Calculate(ctxWith("sid-B", state2), CalculateArgs{Expression: "5+10"})
	}()
	wg.Wait()

	if got := evalCount.Load(); got != 2 {
		t.Errorf("evalCount = %d, want 2 (different sessions should not share slot)", got)
	}
}

// countingState wraps mapState and increments evalCount each time the tool
// completes (state.Set is called inside the singleflight slot once per actual
// eval, so this gives a clean dedup signal).
type countingState struct {
	inner     *mapState
	evalCount *atomic.Int32
}

func (s *countingState) Get(k string) (any, bool) { return s.inner.Get(k) }
func (s *countingState) Set(k string, v any) {
	s.evalCount.Add(1)
	s.inner.Set(k, v)
}
