// Command calc-client is the substrate-poc1 client: takes (sessionID, expr),
// POSTs the expr to the actor via substrate's atenet HTTP layer, and (when
// requested) simulates a TCP drop mid-request to exercise the retry-invariant
// (atenet routes the retry to the same actor, ADK returns the original tool
// result via in-actor dedup).
//
// Flags:
//
//	-session <sessionID>          Actor sessionID. atenet routes by this.
//	-expr <expression>            Expression to evaluate (e.g. "5+10").
//	-url <atenet-endpoint>        atenet HTTP base URL.
//	-simulate-drop-at <Nsec>      Close TCP at t=N seconds (Path 2 test).
//	-simulate-drop-at after-completion
//	                              Close after reading response (Path 1 test).
//	-retries <N>                  Max retry attempts on conn drop (default 3).
//	-timeout <duration>           Per-request timeout (default 60s).
//
// Behavior on drop+retry: close conn, sleep 1s, re-POST with same sessionID.
// substrate atenet routes the retry to the same actor (Host header carries
// actorID=sessionID); ADK returns the original tool result via in-actor dedup
// (state cache for Path 1, singleflight for Path 2).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const userAgent = "substrate-poc1-calc-client/0.1"

type request struct {
	Expression string `json:"expression"`
}

type response struct {
	Value int `json:"value"`
}

type config struct {
	session  string
	expr     string
	url      string
	dropAt   string // "<Nsec>" or "after-completion" or ""
	retries  int
	timeout  time.Duration
	retryGap time.Duration
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "calc-client: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.session, "session", "", "actor sessionID (required)")
	flag.StringVar(&cfg.expr, "expr", "", "expression to evaluate, e.g. '5+10' (required)")
	flag.StringVar(&cfg.url, "url", "http://localhost:8080", "atenet HTTP base URL")
	flag.StringVar(&cfg.dropAt, "simulate-drop-at", "", "close TCP at t=N seconds, or 'after-completion'")
	flag.IntVar(&cfg.retries, "retries", 3, "max retries on conn drop")
	flag.DurationVar(&cfg.timeout, "timeout", 60*time.Second, "per-request timeout")
	flag.DurationVar(&cfg.retryGap, "retry-gap", time.Second, "sleep between retries")
	flag.Parse()
	if cfg.session == "" || cfg.expr == "" {
		flag.Usage()
		os.Exit(2)
	}
	return cfg
}

func run(cfg config) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout*time.Duration(cfg.retries+1))
	defer cancel()

	body, err := json.Marshal(request{Expression: cfg.expr})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	dropNsec, dropAfter, err := parseDropAt(cfg.dropAt)
	if err != nil {
		return fmt.Errorf("parse -simulate-drop-at: %w", err)
	}

	t0 := time.Now()
	logf(t0, "starting: session=%s expr=%q drop=%s retries=%d", cfg.session, cfg.expr, cfg.dropAt, cfg.retries)

	var lastErr error
	for attempt := 0; attempt <= cfg.retries; attempt++ {
		if attempt > 0 {
			logf(t0, "retry %d/%d after %s", attempt, cfg.retries, cfg.retryGap)
			time.Sleep(cfg.retryGap)
		}
		// Only simulate-drop on the first attempt (retries should succeed).
		simulate := dropNsec
		simulateAfter := dropAfter && attempt == 0
		if attempt > 0 {
			simulate = 0
		}
		resp, err := doRequest(ctx, cfg, body, t0, simulate, simulateAfter)
		if err != nil {
			lastErr = err
			if isDropErr(err) || simulateAfter {
				logf(t0, "attempt %d: %v (will retry)", attempt, err)
				continue
			}
			return fmt.Errorf("attempt %d: %w", attempt, err)
		}
		logf(t0, "RESULT: value=%d (attempt %d)", resp.Value, attempt)
		return nil
	}
	return fmt.Errorf("exhausted %d retries: %w", cfg.retries, lastErr)
}

// parseDropAt accepts "" (no drop), "<int>" (drop at t=N seconds), or
// "after-completion" (read response then drop).
func parseDropAt(s string) (int, bool, error) {
	if s == "" {
		return 0, false, nil
	}
	if s == "after-completion" {
		return 0, true, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, false, fmt.Errorf("expected int or 'after-completion', got %q", s)
	}
	if n <= 0 {
		return 0, false, fmt.Errorf("drop-at must be > 0, got %d", n)
	}
	return n, false, nil
}

// doRequest performs one POST. If dropNsec > 0, a background goroutine closes
// the underlying conn at t=dropNsec. If dropAfter is true, the caller is
// expected to discard the response and retry (handled in run()).
func doRequest(ctx context.Context, cfg config, body []byte, t0 time.Time, dropNsec int, dropAfter bool) (*response, error) {
	// Custom transport so we can grab the underlying net.Conn for drop sim.
	var (
		connMu sync.Mutex
		conn   net.Conn
	)
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := net.Dial(network, addr)
			if err != nil {
				return nil, err
			}
			connMu.Lock()
			conn = c
			connMu.Unlock()
			return c, nil
		},
		DisableKeepAlives: true,
	}
	client := &http.Client{Transport: tr, Timeout: cfg.timeout}

	if dropNsec > 0 {
		go func() {
			time.Sleep(time.Duration(dropNsec) * time.Second)
			connMu.Lock()
			c := conn
			connMu.Unlock()
			if c != nil {
				logf(t0, "DROP: closing conn at t+%ds", dropNsec)
				_ = c.Close()
			}
		}()
	}

	url := cfg.url + "/v1/calculate"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	// atenet routes by Host header → actorID=sessionID.
	req.Host = cfg.session

	logf(t0, "POST %s host=%s body=%s", url, cfg.session, string(body))
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	logf(t0, "RECV: status=%d body=%s", resp.StatusCode, string(respBody))

	var r response
	if err := json.Unmarshal(respBody, &r); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	if dropAfter {
		connMu.Lock()
		c := conn
		connMu.Unlock()
		if c != nil {
			logf(t0, "DROP: closing conn after-completion")
			_ = c.Close()
		}
		// Signal caller to retry to exercise Path 1 cache hit.
		return nil, errors.New("simulated post-completion drop")
	}

	return &r, nil
}

// isDropErr matches the connection-level errors we expect from a mid-request
// drop (close on the underlying net.Conn).
func isDropErr(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	// net.OpError on a closed conn surfaces variously as "use of closed network connection",
	// "connection reset by peer", "EOF", "broken pipe", or "unexpected EOF".
	for _, m := range []string{
		"use of closed network connection",
		"connection reset by peer",
		"broken pipe",
		"EOF",
	} {
		if contains(s, m) {
			return true
		}
	}
	return false
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func logf(t0 time.Time, format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[t+%5.1fs] "+format+"\n", append([]any{time.Since(t0).Seconds()}, args...)...)
}
