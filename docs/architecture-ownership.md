# Architecture and ownership

```mermaid
flowchart LR
    U[Client] -->|IP08 HTTP + request ID| G[Envoy Gateway]
    G --> A[FastAPI]
    A -->|IP01 event + traceparent| K[Kafka]
    K -.->|IP02 full profile| F[Airflow]
    F -.->|IP03 MERGE| D[Delta Lake]
    D -.->|IP04 materialize| Feast[Feast]
    D -.->|IP05 index| Q[Qdrant]
    D -.->|IP06 provenance| M[MLflow]
    A -->|retrieve| Q
    A -->|features| Feast
    A -->|resolve champion| M
    A -.->|IP07 full GPU gate| V[vLLM]
    G & A & K & F & D & Feast & Q & M & V -->|IP09 metrics| P[Prometheus + Grafana]
    G & A & K & F & D & Feast & Q & M & V -->|IP10 OTLP| T[Collector + Jaeger/LangSmith]

    subgraph Ingestion[team-ingestion]
      K
      F
    end
    subgraph DataML[team-data]
      D
      Feast
      M
    end
    subgraph Serving[team-serving]
      A
      Q
      V
    end
    subgraph Platform[team-platform]
      G
      P
      T
    end
```

Solid arrows are exercised by the route-2 core run. Dashed arrows need the full
profile or the GPU gate. The presenter/incident-commander role owns the demo
sequence and the evidence index across all four technical owners.

| Owner | Integration points | State or contract owned |
|---|---|---|
| `team-ingestion` | IP01, IP02 | Kafka schemas, partition key, offsets, retry/DLQ, Airflow run |
| `team-data` | IP03, IP04, IP06 | Delta versions, Feast snapshot/materialization, MLflow release |
| `team-serving` | IP05, IP07 | deterministic vector IDs, retrieval, grounded prompt, vLLM identity |
| `team-platform` | IP08, IP09, IP10 | gateway policy, metrics/alerts, trace pipeline, K8s/GitOps |
| `team-presenter` | all | evidence index, incident narration, Q&A |
