from __future__ import annotations

from typing import Any

from lab28_platform import cli


class Response:
    def __init__(self, status_code: int, retry_after: str | None = None) -> None:
        self.status_code = status_code
        self.headers = {"retry-after": retry_after} if retry_after is not None else {}


class Client:
    def __init__(self, responses: list[Response]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def post(self, path: str, *, json: dict[str, Any]) -> Response:
        self.calls.append((path, json))
        return self.responses.pop(0)


def test_seed_retries_after_the_gateway_bucket_refills(monkeypatch: Any) -> None:
    client = Client([Response(429, "0.25"), Response(202)])
    sleeps: list[float] = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    response = cli._post_with_rate_limit_retry(client, "/api/v1/documents", {"doc_id": "d1"})

    assert response.status_code == 202
    assert len(client.calls) == 2
    assert sleeps == [0.25]


def test_seed_does_not_retry_validation_failures(monkeypatch: Any) -> None:
    client = Client([Response(422)])
    sleeps: list[float] = []
    monkeypatch.setattr(cli.time, "sleep", sleeps.append)

    response = cli._post_with_rate_limit_retry(client, "/api/v1/documents", {})

    assert response.status_code == 422
    assert len(client.calls) == 1
    assert sleeps == []
