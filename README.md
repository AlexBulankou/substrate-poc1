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
