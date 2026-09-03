"""Inject and recover a route-2 optional-dependency incident with Kafka proof."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from capture_core_evidence import matching_kafka_record, post_after_refill, write
from lab28_platform.settings import Settings

ROOT = Path(__file__).resolve().parents[1]


def readiness(base_url: str) -> dict[str, Any]:
    deadline = time.monotonic() + 20.0
    last = "no response"
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{base_url.rstrip('/')}/ready", timeout=10.0)
            if response.status_code == 429:
                time.sleep(1.0)
                continue
            last = f"HTTP {response.status_code}: {response.text[:200]}"
            if response.status_code in {200, 503}:
                return {"http_status": response.status_code, **response.json()}
        except httpx.HTTPError as error:
            last = f"{type(error).__name__}: {error}"
        time.sleep(1.0)
    raise RuntimeError(f"readiness was unavailable: {last}")


def wait_for_component(base_url: str, name: str, expected: bool) -> dict[str, Any]:
    deadline = time.monotonic() + 60.0
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = readiness(base_url)
        components = {item["name"]: item for item in last.get("components", [])}
        if name in components and bool(components[name]["ready"]) is expected:
            return last
        time.sleep(1.0)
    raise RuntimeError(f"{name} did not become ready={expected}: {last}")


def compose(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", "compose", "--env-file", "ports.template", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )


def fingerprint(record: dict[str, Any]) -> str:
    canonical = json.dumps(record["value"], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=ROOT / "evidence")
    parser.add_argument("--gateway", default="http://localhost:8080")
    args = parser.parse_args()

    settings = Settings.from_env()
    run_id = f"incident-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:8]}"
    trace_id = uuid.uuid4().hex
    traceparent = f"00-{trace_id}-{uuid.uuid4().hex[:16]}-01"
    baseline = readiness(args.gateway)
    stopped_at = datetime.now(UTC).isoformat()
    compose("stop", "feast")

    during: dict[str, Any] = {}
    accepted: dict[str, Any] = {}
    before_recovery: dict[str, Any] = {}
    recovery: dict[str, Any] = {}
    try:
        during = wait_for_component(args.gateway, "feast", False)
        payload = {
            "asker_id": run_id,
            "text": "Accepted while optional Feast is unavailable; Kafka must retain this event.",
            "rating": 4,
            "label": "positive",
        }
        with httpx.Client(
            base_url=args.gateway,
            headers={"traceparent": traceparent},
            timeout=15.0,
        ) as client:
            response = post_after_refill(client, "/api/v1/feedback", payload)
        response.raise_for_status()
        accepted = {
            "http_status": response.status_code,
            "x-request-id": response.headers.get("x-request-id"),
            "body": response.json(),
        }
        before_recovery = matching_kafka_record(
            settings.kafka.bootstrap_servers,
            settings.kafka.topic_raw,
            accepted["body"]["idempotency_key"],
        )
    finally:
        compose("start", "feast")
        recovery = wait_for_component(args.gateway, "feast", True)

    after_recovery = matching_kafka_record(
        settings.kafka.bootstrap_servers,
        settings.kafka.topic_raw,
        accepted["body"]["idempotency_key"],
    )
    same_coordinates = all(
        before_recovery[key] == after_recovery[key]
        for key in ("topic", "partition", "offset", "key")
    )
    same_payload = fingerprint(before_recovery) == fingerprint(after_recovery)
    result = {
        "profile": "route-2-core",
        "run_id": run_id,
        "trace_id": trace_id,
        "incident": "Feast container stopped (optional serving dependency)",
        "hypothesis": (
            "readiness remains HTTP 200/degraded, Feast becomes not ready, "
            "and ingestion remains durable in Kafka"
        ),
        "stopped_at": stopped_at,
        "baseline_readiness": baseline,
        "during_incident_readiness": during,
        "accepted_during_incident": accepted,
        "kafka_before_recovery": before_recovery,
        "recovered_readiness": recovery,
        "kafka_after_recovery": after_recovery,
        "no_data_loss": same_coordinates and same_payload,
        "proof": {
            "same_topic_partition_offset_key": same_coordinates,
            "same_payload_sha256": same_payload,
            "payload_sha256": fingerprint(after_recovery),
            "scope": "Kafka durability in route 2; Delta replay requires the full profile",
        },
    }
    write(args.out, "failure-recovery.json", result)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
