# Multi-stage build: compile in golang:1.25-alpine, ship as distroless static
# :nonroot. The agent binary is a static CGO_ENABLED=0 build so it runs in
# `gcr.io/distroless/static-debian12:nonroot` without a glibc.
#
# Built and pushed by CI / `gcloud builds submit` to:
#   us-central1-docker.pkg.dev/alexbu-gke-dev-d/substrate-poc1/agent:latest
FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ ./cmd/
COPY pkg/ ./pkg/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/agent ./cmd/agent

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /
COPY --from=build /out/agent /agent
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/agent"]
