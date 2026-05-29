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

### Cluster-mode

`make demo-cluster` ([`test/e2e-cluster/`](./test/e2e-cluster/)) runs the in-cluster suspend/resume e2e against `substrate-demo-cluster`. It proves the headline invariant: the calculate-tool dedup state — **exactly one real evaluation despite client retries** — *survives a CRIU suspend/resume*, observed live via `/debug/evalcount` on a real actor behind atenet.

The assertion chain:

1. Create an actor from the `substrate-poc1-agent` ActorTemplate (starts SUSPENDED).
2. Drive `6*7` → auto-resume from golden snapshot; assert `value==42`, `evalCount==1`.
3. Retry the calc 3× → assert `evalCount` stays `1` (Path-1 cache dedup).
4. `SuspendActor` (CRIU checkpoint) → re-drive `6*7` (resume from the actor's own CRIU snapshot).
5. Assert `value==42` and `evalCount` is **still `1`** — the dedup state survived suspend/resume.

`atenet-router` and the per-actor DNS name (`<actor>.actors.resources.substrate.ate.dev`) resolve cluster-internal only, so the e2e drives HTTP from an in-cluster curl pod ([`curl-driver.yaml`](./test/e2e-cluster/curl-driver.yaml)) while orchestration runs wherever you invoke it.

Prereqs:

- `KUBECONFIG` points at `substrate-demo-cluster` (admin) — e.g. `gcloud container clusters get-credentials substrate-demo-cluster --region us-central1 --project alexbu-gke-dev-d`.
- `KUBECTL_ATE` = path to the `kubectl-ate` binary (default: `kubectl-ate` on PATH). Suspend/resume go through the gRPC CRIU workflow, so plain `kubectl` cannot drive them.
- ActorTemplate `substrate-poc1-agent` is Ready — [`k8s/actor-template.yaml`](./k8s/actor-template.yaml) applied with the golden snapshot built on an agent image that serves `/debug/evalcount` (#17). Snapshot bucket + GAR image push are the manifest's documented pre-apply gates.
- The single-replica WorkerPool's worker must be free — clean up any leftover actors before a run (the script logs a `500 "no free workers available"` body if it isn't).

The substrate ate-system install (`AlexBulankou/a` #782/#808) and the keyless agent boot are DONE; cluster-mode is live, not gated.

## Status

**IMPL ACTIVE.** Tracking issues: [#1 umbrella](https://github.com/AlexBulankou/substrate-poc1/issues/1), [#2 agent](https://github.com/AlexBulankou/substrate-poc1/issues/2) (a4s2), [#4 client+tool](https://github.com/AlexBulankou/substrate-poc1/issues/4) (a4s1), [#5 joint integration](https://github.com/AlexBulankou/substrate-poc1/issues/5).

Both local-mode (`make demo`) and cluster-mode (`make demo-cluster`) e2e are live: the cluster e2e proves the dedup invariant survives a CRIU suspend/resume on `substrate-demo-cluster`.

## Contributing

PR-based workflow — branch from `main`, open PR, peer-review, squash-merge. The bootstrap commit (this README + scaffold) was pushed directly to `main` as the only exception: empty repos have no base branch and no protection rules, so PRs aren't possible until that first commit lands. Everything since goes through review.

## Refs

- Design v0.2 (a4s1 private share): `a4-read a4s1 dima-poc-design-v0.1`
- Substrate PR #66 (Dima): https://github.com/agent-substrate/substrate/pull/66
- ADK Go: https://github.com/google/adk-go (module `google.golang.org/adk`)

## License

Apache 2.0 — see [LICENSE](./LICENSE).
