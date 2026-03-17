from __future__ import annotations


def readiness_payload(status: str) -> dict[str, str]:
    return {"status": status}
