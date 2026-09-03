# Lab 28 answers

## Chosen path and trade-offs

This submission uses **route 2 — the core system** in GitHub Codespaces. It
runs Kafka, FastAPI, Envoy, Feast, Qdrant, MLflow, OpenTelemetry/Jaeger,
Prometheus and Grafana without consuming resources on the local laptop. The
trade-off is explicit: this profile proves the synchronous ingest/retrieval
platform boundaries, but it cannot prove Airflow-to-Delta processing or a real
GPU-backed vLLM endpoint.

The API accepts an ingestion request only after Kafka acknowledges it with
`acks=all`; the idempotency key is stable while each delivery receives a unique
event ID. Envoy enforces a 10-request/s token bucket and stamps a request ID.
The seed command retries only HTTP 429 after the bucket refills, while schema or
server errors remain rejected and visible.

Feast is optional on the serving readiness path because a cold feature row can
degrade personalization without making retrieval unavailable. Kafka, Qdrant
and MLflow are mandatory. vLLM is optional only in the route-2 Compose profile;
the Kubernetes and full-integration gate requires a verifiable real vLLM.

## Evidence interpretation

- `evidence/fast-suite.txt` is contract/static proof, not live integration proof.
- `evidence/core-happy-path.json` correlates the route-2 run ID, trace ID,
  Kafka position and MLflow version. Its Delta version is deliberately `null`.
- IP01, IP05, IP06 and IP08 have live core evidence. IP09 records the actual
  Prometheus/Grafana state. IP10 records only the gateway/API/Kafka-produce leg
  and lists every missing full-profile span.
- IP02, IP03 and IP04 are not claimed because Airflow/Delta materialization is
  outside route 2. IP07 remains failed until a real vLLM `/version`, model list
  and `vllm:` metrics can be captured. The LangSmith export leg also remains
  unverified without `LANGSMITH_API_KEY`.

## Failure and recovery

The observed Codespaces incident was an inter-container forwarding failure:
containers were healthy individually, but Envoy reported
`failed_active_hc/active_hc_timeout`, Kafka producers timed out, and OTLP export
failed. The legacy firewall had no allow rule for the Compose bridge. Adding an
allow rule scoped to that bridge and restarting only Envoy restored gateway,
Kafka, Feast, MLflow and telemetry connectivity. After recovery, all 25 bundled
records were accepted through the gateway with zero rejects.

Within route 2, no-data-loss proof is the acknowledged Kafka record in
`ip01-kafka-consume.json`: its key, partition, offset, payload and W3C
`traceparent` match the accepted HTTP response. End-to-end no-data-loss into
Delta requires the full-profile replay journey and is not claimed here.

## Load profile and bottleneck

`load-profile.json` records P50/P95/P99, throughput, 2xx, 429 and network-error
counts for concurrent `/ready` requests through Envoy. The expected first
bottleneck is the gateway's 10 request/s token bucket. Readiness is also more
expensive than liveness because it probes Kafka, MLflow, Qdrant, vLLM and Feast;
production traffic should use `/health` for liveness and reserve `/ready` for
orchestrator checks rather than treating it as a business endpoint benchmark.

## Kubernetes and GitOps

The checked-in manifests provide two API replicas, startup/readiness/liveness
probes, non-root execution, read-only root filesystem, dropped capabilities,
resource requests/limits, HPA, PDB, NetworkPolicy and Gateway API routing.
Argo CD follows `main` in the submitted repository, enables pruning and
self-heal, and retains five revisions. A production promotion should replace
`main` with an immutable signed tag or commit; rollback changes the desired Git
revision/image and syncs instead of performing an undocumented live edit.

Static validation is recorded in `evidence/static-validation.txt`. Live drift,
self-heal and rollback are **not** claimed in route 2 because it has no
Kubernetes/Argo CD cluster.

## Production gaps

- Only the API/Gateway Kubernetes surface is declared; managed or production
  definitions for Kafka, Delta object storage, Feast, Qdrant, MLflow and the
  observability backends are still required.
- Add TLS, authentication/authorization, external secret management, encrypted
  persistent storage, backups/restore drills and tenant/network isolation.
- Pin container images by digest, add environment overlays, admission policies,
  vulnerability/SBOM gates and signed provenance.
- Validate HPA thresholds with business-endpoint load, define error-budget
  policy and retention/cost controls, and run live disaster recovery.
- Supply a real GPU vLLM endpoint and LangSmith key only through secret storage;
  never commit either credential.

## Individual contribution

This is an individual submission by **Nguyen Phuc Huy Hoang**
(`hoangnph951`). The student-authored changes complete the four starter
functions (Kafka headers, latest-record deduplication, Feast online request and
readiness severity), configure the browser Codespace, fix rate-limit-aware
seeding, enrich the load profile, add reproducible core evidence capture, and
document architecture, ownership, trade-offs and production gaps. The original
platform scaffold and teaching assets remain attributed to their upstream
authors in Git history.
