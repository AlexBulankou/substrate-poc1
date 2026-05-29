# substrate-poc1 — build, test, and demo targets.
#
# Local-mode (runnable now, no cluster / no Gemini / no #782):
#   make test   — pkg/calc unit tests (fast, hermetic)
#   make demo   — local e2e harness: real calc-client over real TCP through the
#                 4-case dedup matrix (happy / Path1 / Path2 / concurrent)
#   make build  — compile the agent + calc-client binaries to bin/
#
# Cluster-mode (gated on AlexBulankou/a #782 ate-system install):
#   make demo-cluster — deploy ActorTemplate to substrate-demo-cluster and run
#                       the client against live atenet. See README "Cluster-mode".

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
	@echo "demo-cluster is gated on the substrate ate-system install (AlexBulankou/a #782)."
	@echo "See README.md -> 'E2E harness' -> 'Cluster-mode' for the manual runbook."
	@exit 1

clean:
	rm -rf $(BIN)
