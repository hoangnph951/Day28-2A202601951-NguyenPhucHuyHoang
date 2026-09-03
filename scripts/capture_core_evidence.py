"""Capture reproducible live evidence for the route-2 (core) stack.

This script only writes evidence it actually observes.  Airflow, Delta and a
real vLLM are deliberately absent from the core profile, so their matrix files
remain outstanding instead of being replaced by placeholders.
"""

from __future__ import annotations

import argparse
import json
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import yaml
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, TopicPartition

from lab28_platform.model_registry import ReleaseRegistry
from lab28_platform.settings import Settings

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "evidence"
MATRIX = ROOT / "contracts" / "integration-matrix.yaml"


def write(out: Path, name: str, payload: Any) -> Path:
    out.mkdir(parents=True, exist_ok=True)
    path = out / name
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    return path


def post_after_refill(
    client: httpx.Client, path: str, payload: dict[str, Any]
) -> httpx.Response:
    response: httpx.Response | None = None
    for _ in range(8):
        response = client.post(path, json=payload)
        if response.status_code != 429:
            return response
        time.sleep(1.0)
    assert response is not None
    return response


def matching_kafka_record(
    bootstrap_servers: str,
    topic: str,
    idempotency_key: str,
    *,
    timeout: float = 30.0,
) -> dict[str, Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"lab28-core-evidence-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=10.0)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"topic {topic!r} is unavailable")
        consumer.assign(
            [
                TopicPartition(topic, partition, OFFSET_BEGINNING)
                for partition in topic_metadata.partitions
            ]
        )

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))
            value = json.loads((message.value() or b"{}").decode("utf-8"))
            if value.get("idempotency_key") != idempotency_key:
                continue
            headers = {
                name: (raw or b"").decode("utf-8", "replace")
                for name, raw in (message.headers() or [])
            }
            traceparent = headers.get("traceparent", "")
            return {
                "topic": message.topic(),
                "key": message.key().decode("utf-8") if message.key() else None,
                "partition": message.partition(),
                "offset": message.offset(),
                "headers": headers,
                "trace_id": traceparent.split("-")[1] if traceparent.count("-") == 3 else None,
                "value": value,
            }
    finally:
        consumer.close()
    raise RuntimeError(f"no Kafka record found for {idempotency_key}")


def gateway_evidence(base_url: str, admin_url: str) -> dict[str, Any]:
    with httpx.Client(base_url=base_url, timeout=10.0) as client:
        accepted: httpx.Response | None = None
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            response = client.get("/health")
            if response.status_code == 200:
                accepted = response
                break
            time.sleep(1.0)
        if accepted is None:
            raise RuntimeError("gateway token bucket did not admit a health request")

        statuses = [accepted.status_code]
        rejected: httpx.Response | None = None
        for _ in range(30):
            response = client.get("/health")
            statuses.append(response.status_code)
            if response.status_code == 429 and rejected is None:
                rejected = response

        recovered: httpx.Response | None = None
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            response = client.get("/health")
            if response.status_code == 200:
                recovered = response
                break
            time.sleep(1.0)

    stats = httpx.get(f"{admin_url.rstrip('/')}/stats/prometheus", timeout=10.0).text
    rate_limited = sum(
        float(line.rsplit(" ", 1)[1])
        for line in stats.splitlines()
        if line.startswith("envoy_http_local_rate_limit_rate_limited")
    )
    return {
        "gateway_url": base_url,
        "route": "/health",
        "requests_sent": len(statuses),
        "status_counts": {str(code): statuses.count(code) for code in sorted(set(statuses))},
        "rate_limited_stat": rate_limited,
        "sample_200": {
            "status": accepted.status_code,
            "x-request-id": accepted.headers.get("x-request-id"),
        },
        "sample_429": {
            "status": rejected.status_code if rejected else None,
            "x-request-id": rejected.headers.get("x-request-id") if rejected else None,
        },
        "recovered": recovered is not None,
        "recovery_status": recovered.status_code if recovered else None,
    }


def prometheus_evidence(base_url: str) -> dict[str, Any]:
    targets = httpx.get(f"{base_url.rstrip('/')}/api/v1/targets", timeout=10.0)
    rules = httpx.get(f"{base_url.rstrip('/')}/api/v1/rules", timeout=10.0)
    targets.raise_for_status()
    rules.raise_for_status()
    active = targets.json()["data"]["activeTargets"]
    groups = rules.json()["data"]["groups"]
    return {
        "prometheus_url": base_url,
        "targets": [
            {
                "job": target.get("labels", {}).get("job"),
                "url": target.get("scrapeUrl"),
                "health": target.get("health"),
                "last_error": target.get("lastError"),
            }
            for target in active
        ],
        "rules": [
            {
                "group": group.get("name"),
                "rules": [
                    {
                        "name": rule.get("name"),
                        "type": rule.get("type"),
                        "health": rule.get("health"),
                    }
                    for rule in group.get("rules", [])
                ],
            }
            for group in groups
        ],
    }


def grafana_evidence(base_url: str) -> dict[str, Any]:
    auth = ("admin", "admin")
    dashboards = httpx.get(
        f"{base_url.rstrip('/')}/api/search",
        params={"type": "dash-db"},
        auth=auth,
        timeout=10.0,
    )
    datasources = httpx.get(
        f"{base_url.rstrip('/')}/api/datasources", auth=auth, timeout=10.0
    )
    dashboards.raise_for_status()
    datasources.raise_for_status()
    return {
        "grafana_url": base_url,
        "dashboards": [
            {"title": item.get("title"), "uid": item.get("uid"), "url": item.get("url")}
            for item in dashboards.json()
        ],
        "datasources": [
            {"name": item.get("name"), "type": item.get("type")}
            for item in datasources.json()
        ],
    }


def trace_evidence(base_url: str, trace_id: str) -> dict[str, Any]:
    trace: dict[str, Any] | None = None
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        response = httpx.get(f"{base_url.rstrip('/')}/api/traces/{trace_id}", timeout=10.0)
        if response.status_code == 404:
            time.sleep(1.0)
            continue
        response.raise_for_status()
        data = response.json().get("data") or []
        if data:
            trace = data[0]
            break
        time.sleep(1.0)
    if trace is None:
        raise RuntimeError(f"trace {trace_id} was not exported to Jaeger")

    matrix = yaml.safe_load(MATRIX.read_text(encoding="utf-8"))
    required = set(matrix["required_spans"])
    spans = trace.get("spans") or []
    names = {span.get("operationName") for span in spans}
    processes = trace.get("processes") or {}
    return {
        "trace_id": trace_id,
        "scope": "route-2 core path: gateway -> API -> Kafka",
        "services": sorted(
            {
                process.get("serviceName")
                for process in processes.values()
                if process.get("serviceName")
            }
        ),
        "span_names": sorted(name for name in names if name),
        "required_spans_present": sorted(required & names),
        "required_spans_missing": sorted(required - names),
        "full_matrix_trace_verified": required <= names,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--gateway", default="http://localhost:8080")
    parser.add_argument("--gateway-admin", default="http://localhost:9901")
    parser.add_argument("--prometheus", default="http://localhost:9090")
    parser.add_argument("--grafana", default="http://localhost:3000")
    parser.add_argument("--jaeger", default="http://localhost:16686")
    args = parser.parse_args()

    settings = Settings.from_env()
    run_id = f"route2-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:8]}"
    trace_id = uuid.uuid4().hex
    traceparent = f"00-{trace_id}-{uuid.uuid4().hex[:16]}-01"
    payload = {
        "asker_id": run_id,
        "text": "Route-2 evidence request proving gateway, API, Kafka and trace continuity.",
        "rating": 5,
        "label": "positive",
    }
    with httpx.Client(
        base_url=args.gateway, headers={"traceparent": traceparent}, timeout=15.0
    ) as client:
        ingest = post_after_refill(client, "/api/v1/feedback", payload)
    ingest.raise_for_status()
    ingest_body = ingest.json()

    kafka = matching_kafka_record(
        settings.kafka.bootstrap_servers,
        settings.kafka.topic_raw,
        ingest_body["idempotency_key"],
    )
    kafka["run_id"] = run_id
    write(args.out, "ip01-kafka-consume.json", kafka)

    gateway = gateway_evidence(args.gateway, args.gateway_admin)
    gateway["run_id"] = run_id
    write(args.out, "ip08-gateway.json", gateway)
    write(args.out, "ip09-prometheus-targets.json", prometheus_evidence(args.prometheus))
    write(args.out, "ip09-grafana-dashboards.json", grafana_evidence(args.grafana))

    trace = trace_evidence(args.jaeger, trace_id)
    trace["run_id"] = run_id
    trace["traceparent"] = traceparent
    write(args.out, "ip10-trace.json", trace)

    release = ReleaseRegistry(settings.mlflow).health()
    summary = {
        "profile": "route-2-core",
        "run_id": run_id,
        "trace_id": trace_id,
        "ingestion_status": ingest.status_code,
        "kafka": {
            "topic": kafka["topic"],
            "partition": kafka["partition"],
            "offset": kafka["offset"],
        },
        "delta_version": None,
        "delta_status": "UNVERIFIED: Delta requires the full profile",
        "mlflow_version": release.get("version"),
        "mlflow_run_id": release.get("run_id"),
        "trace_services": trace["services"],
        "trace_span_names": trace["span_names"],
    }
    write(args.out, "core-happy-path.json", summary)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
