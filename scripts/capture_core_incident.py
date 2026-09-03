"""Inject and recover a route-2 gateway incident with Kafka durability proof."""

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


def gateway_health(base_url: str) -> dict[str, Any]:
    try:
        response = httpx.get(f"{base_url.rstrip('/')}/healthz", timeout=5.0)
    except httpx.HTTPError as error:
        return {"reachable": False, "error": f"{type(error).__name__}: {error}"}
    return {
        "reachable": True,
        "http_status": response.status_code,
        "body": response.text,
        "x-request-id": response.headers.get("x-request-id"),
    }


def wait_for_gateway(base_url: str) -> dict[str, Any]:
    deadline = time.monotonic() + 45.0
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = gateway_health(base_url)
        if last.get("http_status") == 200:
            return last
        time.sleep(1.0)
    raise RuntimeError(f"gateway did not recover: {last}")


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
    baseline = wait_for_gateway(args.gateway)
    payload = {
        "asker_id": run_id,
        "text": "Durable Kafka record created before the controlled gateway outage.",
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

    stopped_at = datetime.now(UTC).isoformat()
    compose("stop", "gateway")
    during = gateway_health(args.gateway)
    recovery: dict[str, Any] = {}
    try:
        if during.get("reachable"):
            raise RuntimeError(f"gateway still reachable after stop: {during}")
    finally:
        compose("start", "gateway")
        recovery = wait_for_gateway(args.gateway)

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
        "incident": "Envoy gateway container stopped",
        "hypothesis": (
            "the edge becomes unreachable, then recovers after restart, while "
            "the already acknowledged Kafka record keeps identical coordinates and payload"
        ),
        "stopped_at": stopped_at,
        "baseline_gateway": baseline,
        "accepted_before_incident": accepted,
        "kafka_before_recovery": before_recovery,
        "during_incident_gateway": during,
        "recovered_gateway": recovery,
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
