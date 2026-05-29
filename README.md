# substrate-poc1

PoC demonstrating **client-perceives-no-reset across actor connection drop** on [agent-substrate](https://github.com/agent-substrate/substrate), via session-keyed retry to the same actor.

Coordinated with Dmitry Berkovich (substrate PR #66 author). Cross-tracked in [`AlexBulankou/a` #761](https://github.com/AlexBulankou/a/issues/761).

## Architecture

Two Go components on a substrate-enabled GKE cluster (no broker — client talks directly to actor via substrate atenet HTTP):

1. **Agent app** ([`cmd/agent/`](./cmd/agent/)) — ADK Go agent hosted as substrate `ActorTemplate`. Registers one tool `calculate(expr)` that simulates 20s of work then returns the eval.
2. **Client** ([`cmd/calc-client/`](./cmd/calc-client/)) — Go CLI taking `(sessionID, expr)`. Calls `ControlClient.CreateActor(actorID=sessionID)` (idempotent), then POSTs `expr` via atenet HTTP (Host header carries actorID).

**Test invariant:** TCP drop mid-tool-execution → client retries same sessionID → atenet routes same actor → ADK returns the ORIGINAL tool result (no re-execute).

## Dedup (DIY)

Neither ADK Go nor ADK Python ships built-in tool-result memoization keyed by sessionID. Implemented in [`pkg/calc/`](./pkg/calc/) as two paths:

- **Path 1** (post-completion retry): state-cache via `ctx.State().Set/Get("calc:<expr>", result)`
- **Path 2** (in-flight retry): [`golang.org/x/sync/singleflight.Group`](https://pkg.go.dev/golang.org/x/sync/singleflight) keyed on `sid|expr` — same primitive substrate atenet uses for `ResumeActor` (direct precedent in `cmd/atenet/internal/app/router/resumer.go`).

## E2E test matrix

1. **Happy path**: client → 20s wait → result
2. **Path 1 (post-completion retry)**: get result → close conn → re-POST → cached result returned immediately
3. **Path 2 (in-flight retry)**: POST → drop at t=5s → re-POST at t=6s → both wake at t=20s with same result
4. **Concurrent same-sessionID**: two clients POST simultaneously → second blocks → both get same result

Plus a CRIU smoke trio validating that `sync.Mutex`+map, blocked goroutines, and Go monotonic clock all survive substrate's checkpoint/restore.

## Running the harness

### Local-mode (runnable now — no cluster, no Gemini, no operator)

```
make test    # pkg/calc unit tests (fast, hermetic)
make demo    # local e2e: real calc-client over real TCP through all 4 cases
```

`make demo` ([`test/e2e/`](./test/e2e/)) drives the **real** `calc-client` binary — with its real conn-drop transport — against a test HTTP server that wraps the **real** `pkg/calc.Calculator`. It exercises the dedup invariant end-to-end over a real TCP socket, printing a per-case timeline:

| Case | What drops | Asserts |
|---|---|---|
| 1 Happy | nothing | result after one work-duration |
| 2 Path 1 | conn, *after* result read | retry returns cached value, **1** evaluation |
| 3 Path 2 | conn, *during* work | retry joins singleflight, both wake together, **1** evaluation |
| 4 Concurrent | nothing (2 clients race) | singleflight collapses to **1** evaluation |

The `evalCount == 1` assertions (cases 2-4) are the dedup proof: the work ran exactly once no matter how the client retried. The test server is a faithful stand-in for the actor's direct-tool endpoint (one long-lived `Calculator` + per-session state, keyed on the `Host` header the client sets to the sessionID); `cmd/agent` now exposes the matching `/v1/calculate` route (#10) and `cmd/agent/direct_route_test.go` asserts the same `evalCount == 1` signal against the real route.

### Cluster-mode (gated on operator install — `AlexBulankou/a` #782)

`make demo-cluster` deploys the agent ActorTemplate to `substrate-demo-cluster` and runs the client against live atenet, additionally validating substrate's same-actorID-same-process routing and the CRIU smoke trio. **Blocked** until the substrate ate-system install lands (alpha-API gap on GKE — `AlexBulankou/a` #782/#808). Remaining pre-req beyond #782:

- Snapshot bucket + `GOOGLE_API_KEY` Secret + GAR image push (see [`k8s/actor-template.yaml`](./k8s/actor-template.yaml) pre-apply gates).

The `/v1/calculate` direct-tool route the client POSTs to (`{"expression"}` → `{"value"}`, sessionID in `Host`) is implemented (#10) — cluster-mode is one unblock (#782) away, not two.

## Status

**IMPL ACTIVE.** Tracking issues: [#1 umbrella](https://github.com/AlexBulankou/substrate-poc1/issues/1), [#2 agent](https://github.com/AlexBulankou/substrate-poc1/issues/2) (a4s2), [#4 client+tool](https://github.com/AlexBulankou/substrate-poc1/issues/4) (a4s1), [#5 joint integration](https://github.com/AlexBulankou/substrate-poc1/issues/5).

E2E execution gated on substrate-operator install (alex pick `AlexBulankou/a` #782). Impl + unit tests parallelize regardless.

## Contributing

PR-based workflow — branch from `main`, open PR, peer-review, squash-merge. The bootstrap commit (this README + scaffold) was pushed directly to `main` as the only exception: empty repos have no base branch and no protection rules, so PRs aren't possible until that first commit lands. Everything since goes through review.

## Refs

- Design v0.2 (a4s1 private share): `a4-read a4s1 dima-poc-design-v0.1`
- Substrate PR #66 (Dima): https://github.com/agent-substrate/substrate/pull/66
- ADK Go: https://github.com/google/adk-go (module `google.golang.org/adk`)

## License

Apache 2.0 — see [LICENSE](./LICENSE).
