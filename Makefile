# substrate-poc1 — build, test, and demo targets.
#
# Local-mode (runnable now, no cluster / no Gemini / no #782):
#   make test   — pkg/calc unit tests (fast, hermetic)
#   make demo   — local e2e harness: real calc-client over real TCP through the
#                 4-case dedup matrix (happy / Path1 / Path2 / concurrent)
#   make build  — compile the agent + calc-client binaries to bin/
#
# Cluster-mode (live on substrate-demo-cluster; ate-system install is #782-DONE):
#   make demo-cluster — run the in-cluster suspend/resume e2e: create an actor,
#                       drive 6*7 via atenet, assert the dedup invariant
#                       (evalCount==1) SURVIVES a CRIU suspend/resume.
#                       Prereqs + KUBECONFIG/KUBECTL_ATE: see README "Cluster-mode".

GO ?= go
BIN := bin

.PHONY: test demo demo-cluster build clean

test:
	$(GO) test ./...

# One-command local e2e. -v surfaces the per-case timeline (T+0 POST, T+2 drop,
# ...). -count=1 disables the test cache so the demo always re-runs.
demo:
	$(GO) test -tags e2e -v -count=1 ./test/e2e

build:
	mkdir -p $(BIN)
	$(GO) build -o $(BIN)/agent ./cmd/agent
	$(GO) build -o $(BIN)/calc-client ./cmd/calc-client

demo-cluster:
	bash test/e2e-cluster/run.sh

clean:
	rm -rf $(BIN)
