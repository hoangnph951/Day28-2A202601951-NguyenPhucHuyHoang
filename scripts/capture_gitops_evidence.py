"""Record static Kubernetes/GitOps validation without claiming a live cluster."""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
BASE = ROOT / "deploy" / "kubernetes" / "base"
APPLICATION = ROOT / "gitops" / "application.yaml"
OUT = ROOT / "evidence" / "kubernetes-gitops-validation.json"


def run(command: list[str]) -> dict[str, Any]:
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    return {
        "command": " ".join(command),
        "exit_code": result.returncode,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }


def main() -> None:
    validation = run([".venv/bin/python", "scripts/validate_manifests.py"])
    if validation["exit_code"] != 0:
        raise SystemExit(validation["stderr"] or validation["stdout"])

    documents: list[dict[str, Any]] = []
    digests: dict[str, str] = {}
    for path in sorted(BASE.glob("*.yaml")):
        raw = path.read_bytes()
        digests[str(path.relative_to(ROOT))] = hashlib.sha256(raw).hexdigest()
        documents.extend(item for item in yaml.safe_load_all(raw) if item)

    application = yaml.safe_load(APPLICATION.read_text(encoding="utf-8"))
    deployment = next(item for item in documents if item.get("kind") == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    source = application["spec"]["source"]
    sync = application["spec"]["syncPolicy"]
    payload = {
        "status": "PASS_STATIC",
        "validation": validation,
        "manifest_count": len(documents),
        "manifest_sha256": digests,
        "desired_state": {
            "repo_url": source["repoURL"],
            "target_revision": source["targetRevision"],
            "path": source["path"],
            "destination_namespace": application["spec"]["destination"]["namespace"],
        },
        "drift_policy": {
            "automated": True,
            "prune": bool(sync["automated"]["prune"]),
            "self_heal": bool(sync["automated"]["selfHeal"]),
            "revision_history_limit": application["spec"]["revisionHistoryLimit"],
        },
        "rollback": {
            "mechanism": "revert desired Git revision/image and let Argo CD sync",
            "live_rollback_executed": False,
            "reason": "route 2 has no Kubernetes or Argo CD runtime",
        },
        "workload_controls": {
            "replicas": deployment["spec"]["replicas"],
            "image": container["image"],
            "readiness_probe": container["readinessProbe"],
            "liveness_probe": container["livenessProbe"],
            "startup_probe": container["startupProbe"],
            "resources": container["resources"],
            "security_context": container["securityContext"],
        },
        "live_drift_self_heal_evidence": "UNVERIFIED: requires a Kubernetes/Argo CD cluster",
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
