from __future__ import annotations

import json
from pathlib import Path
import sys
import uuid

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from performance.clients import MongoBenchmarkClient, RespBenchmarkClient
from performance.config import load_config


def run_connection_check(client_factory, backend_name: str) -> dict[str, object]:
    key = f"perf:{backend_name}:check:{uuid.uuid4().hex}"
    value = "connection-check"

    with client_factory() as client:
        client.set_value(key, value)
        received = client.get_value(key)

    return {
        "ok": received == value,
        "key": key,
        "expected": value,
        "received": received,
    }


def main() -> int:
    resp_config, mongo_config, _ = load_config()
    checks = {
        "resp": {
            "label": resp_config.label,
            "target": f"{resp_config.host}:{resp_config.port}",
        },
        "mongo": {
            "label": mongo_config.label,
            "target": mongo_config.database,
        },
    }

    failures = 0
    for backend_name, client_factory in [
        ("resp", lambda: RespBenchmarkClient(resp_config)),
        ("mongo", lambda: MongoBenchmarkClient(mongo_config)),
    ]:
        try:
            checks[backend_name]["result"] = run_connection_check(client_factory, backend_name)
        except Exception as exc:
            checks[backend_name]["result"] = {"ok": False, "error": str(exc)}
            failures += 1

    print(json.dumps(checks, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
