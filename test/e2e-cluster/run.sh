#!/usr/bin/env bash
#
# Cluster-mode e2e for substrate-poc1.
#
# Proves the calculate-tool dedup invariant — exactly ONE real evaluation despite
# client retries — SURVIVES a CRIU suspend/resume, observed live via
# /debug/evalcount on a real actor behind atenet on substrate-demo-cluster.
#
# This codifies the manual runbook in README "Cluster-mode". Orchestration
# (kubectl-ate create/suspend, assertions) runs wherever you invoke this; HTTP is
# driven from INSIDE the cluster via a curl pod, because atenet-router and the
# per-actor DNS name (<actor>.actors.resources.substrate.ate.dev) resolve
# cluster-internal only.
#
# Assertion chain:
#   golden (evalCount=0) → drive 6*7 (auto-resume golden) → value==42, evalCount==1
#     → retries → evalCount stays 1 (Path-1 cache dedup)
#     → SuspendActor (CRIU checkpoint) → re-drive 6*7 (resume from CRIU snapshot)
#     → value==42, evalCount STILL 1  ← the dedup state survived suspend/resume.
#
# Prereqs:
#   - KUBECONFIG points at substrate-demo-cluster (admin). From an a4s pod:
#       KUBECONFIG=/tmp/kc.yaml gcloud container clusters get-credentials \
#         substrate-demo-cluster --region us-central1 --project alexbu-gke-dev-d
#   - KUBECTL_ATE = path to the kubectl-ate binary (default: kubectl-ate on PATH).
#   - ActorTemplate substrate-poc1-agent is Ready — k8s/actor-template.yaml applied
#     with the golden snapshot built on the current agent image (the one that
#     serves /debug/evalcount).
#
# Exit 0 = PASS, non-zero = FAIL.

set -euo pipefail

NS=substrate-poc1
TEMPLATE=substrate-poc1-agent
KUBECTL_ATE="${KUBECTL_ATE:-kubectl-ate}"
DRIVER=poc1-e2e-curl
ACTOR="poc1-e2e-$(date +%s)"
EXPR='6*7'
WANT_VALUE=42
HOST="${ACTOR}.actors.resources.substrate.ate.dev"
BASE="http://atenet-router.ate-system.svc.cluster.local"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

info() { echo "[e2e-cluster] $*"; }
fail() { echo "[e2e-cluster] FAIL: $*" >&2; exit 1; }

cleanup() {
  info "cleanup: suspend+delete actor $ACTOR"
  "$KUBECTL_ATE" suspend actor "$ACTOR" >/dev/null 2>&1 || true
  "$KUBECTL_ATE" delete actor "$ACTOR" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Drive an HTTP request from inside the cluster via the driver pod.
incurl() { kubectl exec "$DRIVER" -n "$NS" -- curl -s "$@"; }

# Drive one calc, retrying on a non-200 until 200 or attempts exhausted.
# The expected non-200 is a cold-race while the actor resumes (from golden the
# first time, from its CRIU snapshot after suspend) — atenet returns 504/503
# until the worker is up. No evaluation runs on a non-200, so retries don't
# perturb the eval count. We log the response body on each non-200 so other
# failure modes are self-diagnosing — most notably a 500 "no free workers
# available" when the single-replica WorkerPool's worker is held by a stale
# actor (clean up leftover actors so the pool has a free slot before a run).
drive_calc() {
  local i resp code body
  for i in $(seq 1 10); do
    resp=$(incurl -m 60 -w '\n%{http_code}' \
      -H "Host: $HOST" -H 'Content-Type: application/json' \
      -d "{\"expression\":\"$EXPR\"}" "$BASE/v1/calculate")
    code=${resp##*$'\n'}
    body=${resp%$'\n'*}
    [ "$code" = "200" ] && return 0
    info "  calc attempt $i -> $code, body: ${body:0:120} (retrying)"
    sleep 2
  done
  return 1
}

read_value() {
  incurl -m 60 -H "Host: $HOST" -H 'Content-Type: application/json' \
    -d "{\"expression\":\"$EXPR\"}" "$BASE/v1/calculate" \
    | sed -n 's/.*"value":\([0-9-]*\).*/\1/p'
}

read_evalcount() {
  incurl -m 30 -H "Host: $HOST" "$BASE/debug/evalcount" \
    | sed -n 's/.*"evalCount":\([0-9]*\).*/\1/p'
}

# Suspend+delete any leftover poc1-e2e-* actor from a prior run that died
# before its cleanup trap fired (e.g. a pod respawn SIGKILLs us mid-run). Such
# an orphan stays SUSPENDED but still pins the single-replica WorkerPool's only
# worker slot, so the next run's create/resume fails with a 500 "no free
# workers available". Sweeping before we create ours guarantees a free slot.
# Best-effort: a get/suspend/delete hiccup must never fail the run.
# Column layout of `kubectl-ate get actor`: $1=NAMESPACE $3=ID.
sweep_stale_actors() {
  local stale a
  stale=$("$KUBECTL_ATE" get actor 2>/dev/null \
    | awk -v ns="$NS" '$1==ns && $3 ~ /^poc1-e2e-/ {print $3}') || return 0
  [ -n "$stale" ] || { info "no stale poc1-e2e-* actors to sweep"; return 0; }
  for a in $stale; do
    info "sweeping stale actor $a (suspend+delete)"
    "$KUBECTL_ATE" suspend actor "$a" >/dev/null 2>&1 || true
    "$KUBECTL_ATE" delete actor "$a" >/dev/null 2>&1 || true
  done
}

# --- 0. sweep stale actors, then ensure the in-cluster HTTP driver pod is Ready ---
sweep_stale_actors
if ! kubectl get pod "$DRIVER" -n "$NS" >/dev/null 2>&1; then
  info "creating driver pod $DRIVER"
  kubectl apply -f "$SCRIPT_DIR/curl-driver.yaml" >/dev/null
fi
kubectl wait --for=condition=Ready "pod/$DRIVER" -n "$NS" --timeout=120s >/dev/null
info "driver pod $DRIVER Ready"

# --- 1. create the actor (golden-based, starts SUSPENDED) ---
info "create actor $ACTOR from $NS/$TEMPLATE"
"$KUBECTL_ATE" create actor "$ACTOR" -t "$NS/$TEMPLATE" >/dev/null

# --- 2. first drive: auto-resume from golden, tolerate cold-race ---
info "drive #1 ($EXPR) — auto-resume from golden snapshot"
drive_calc || fail "calc never returned 200 (cold-race attempts exhausted)"
v=$(read_value)
[ "$v" = "$WANT_VALUE" ] || fail "value=$v want=$WANT_VALUE"
info "  value=$v OK"

# --- 3. baseline evalCount + Path-1 dedup (retries must not re-evaluate) ---
base=$(read_evalcount)
info "evalCount baseline=$base"
[ "$base" = "1" ] || fail "baseline evalCount=$base want=1 (exactly one real eval expected)"
for i in 1 2 3; do read_value >/dev/null; done
after=$(read_evalcount)
[ "$after" = "$base" ] || fail "evalCount moved on retry: $base -> $after (Path-1 dedup broken)"
info "  evalCount stable at $after across retries OK (Path-1 cache dedup)"

# --- 4. CRIU suspend (checkpoint the running actor process) ---
info "suspend actor $ACTOR (CRIU checkpoint)"
"$KUBECTL_ATE" suspend actor "$ACTOR" >/dev/null

# --- 5. re-drive: auto-resume from the actor's OWN CRIU snapshot ---
info "drive #2 ($EXPR) — resume from CRIU snapshot"
drive_calc || fail "calc never returned 200 after CRIU resume"
v=$(read_value)
[ "$v" = "$WANT_VALUE" ] || fail "post-CRIU value=$v want=$WANT_VALUE"
info "  value=$v OK"

# --- 6. THE ASSERTION: evalCount survived CRIU (no re-eval after resume) ---
final=$(read_evalcount)
info "evalCount post-CRIU=$final"
[ "$final" = "$base" ] || fail "evalCount changed across CRIU: $base -> $final (state did NOT survive suspend/resume)"

echo
echo "[e2e-cluster] PASS: dedup invariant survived CRIU — evalCount stayed $final (one real eval) across suspend/resume"
