# Submission — Day 28 Track 2

Nộp repo nhóm và evidence; không nộp secret, `.env`, database, cache, weights hay `.lab28/`.

1. `integration-report.json` và output fast suite.
2. 10 evidence files đúng tên trong integration matrix.
3. Architecture/ownership diagram.
4. Happy-path trace có run ID, trace ID, Delta version, MLflow version.
5. Failure/recovery record + no-data-loss proof.
6. Load profile P50/P95/P99 và bottleneck analysis.
7. Kubernetes/GitOps validation + drift/rollback evidence.
8. `ANSWERS.md`: trade-offs, production gaps, contribution từng thành viên.

```text
uv run ruff check .
uv run python scripts/verify_matrix.py
uv run python scripts/check_portability.py
uv run python scripts/validate_manifests.py
uv run pytest tests -q
uv run pytest integration-tests -m "not gpu and not langsmith" -q
```

GPU/LangSmith là gate theo môi trường. Nếu lớp không cấp endpoint/credential, báo
`UNVERIFIED` và dùng local evidence tương ứng; không giả lập. Xem [rubric](docs/rubric.md).

## Trạng thái nộp — route 2/core

| Hạng mục | Trạng thái | Bằng chứng |
|---|---|---|
| Fast suite và static validation | PASS | `evidence/fast-suite.txt`, `evidence/static-validation.txt` |
| Core happy path | PASS | `evidence/core-happy-path.json` |
| Failure/recovery + Kafka durability | PASS | `evidence/failure-recovery.json` |
| Load P50/P95/P99 | PASS | `evidence/load-profile.json` |
| Architecture/ownership | PASS | `docs/architecture-ownership.md` |
| Kubernetes/GitOps contracts | PASS (static) | `evidence/kubernetes-gitops-validation.json` |
| IP01, IP05, IP06, IP08, IP09 | PASS (live core) | các tệp đúng tên trong `evidence/` |
| IP10 core trace leg | PASS (partial) | `evidence/ip10-trace.json` liệt kê span có/thiếu |
| IP02, IP03, IP04 | UNVERIFIED | cần profile full: Airflow + Spark/Delta |
| IP07 | UNVERIFIED | cần endpoint vLLM GPU thật |
| LangSmith export | UNVERIFIED | cần `LANGSMITH_API_KEY` |

Không diễn giải `UNVERIFIED` thành `PASS`: route 2 không thể tạo live evidence
của Airflow/Delta/vLLM. Xem `ANSWERS.md` để biết trade-off và production gaps.
