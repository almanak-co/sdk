"""Retain managed fork handles until local QA finishes terminal observation."""

import asyncio
import hashlib
import json
import time

from almanak.gateway.data.price.qa_pool import _canonical, _owned_bytes, _retain


class ForkCustody:
    def __init__(self, route):
        self.root = route.root
        self.manifest_hash = route.manifest_hash
        startup = json.loads(_owned_bytes(self.root / "gateway-startup.json"))
        if (
            startup["run_id"] != route.manifest.run_id
            or startup["fork_identity"]["manifest_sha256"] != self.manifest_hash
        ):
            raise ValueError("Fork custody requires the bound gateway startup")
        self.expected = {
            "schema_version": 1,
            "scope": "qa_fork_release",
            "run_id": route.manifest.run_id,
            "manifest_sha256": self.manifest_hash,
            "fork_identity": startup["fork_identity"],
        }
        _retain(self.root / "fork-custody-enabled.json", _canonical(self.expected))

    async def wait_for_release(self, *, timeout: float = 600) -> str:
        if not 0 < timeout <= 600:
            raise ValueError("Fork observation retention must be bounded by 600 seconds")
        _retain(self.root / "fork-custody.json", _canonical({**self.expected, "state": "AWAITING_RELEASE"}))
        deadline = time.monotonic() + timeout
        request = self.root / "fork-release.json"
        while time.monotonic() < deadline:
            if request.exists() or request.is_symlink():
                try:
                    raw = _owned_bytes(request)
                    value = json.loads(raw)
                    if value != self.expected or raw != _canonical(self.expected):
                        raise ValueError("Fork release belongs to another run or fork")
                except (ValueError, OSError):
                    return "INVALID_RELEASE"
                _retain(
                    self.root / "fork-release-observed.json",
                    _canonical(
                        {
                            **self.expected,
                            "request_sha256": hashlib.sha256(raw).hexdigest(),
                        }
                    ),
                )
                return "RELEASED"
            await asyncio.sleep(min(0.1, max(0, deadline - time.monotonic())))
        return "OBSERVATION_TIMEOUT"

    def record_gateway_stopped(self) -> None:
        _retain(
            self.root / "subject-gateway-stopped.json",
            _canonical(
                {**self.expected, "scope": "qa_subject_gateway_stopped", "stopped_monotonic_ns": time.monotonic_ns()}
            ),
        )

    def record_stopped(self, reason: str, managers: dict) -> None:
        _retain(
            self.root / "fork-shutdown.json",
            _canonical(
                {
                    **self.expected,
                    "reason": reason,
                    "processes_stopped": all(not manager.is_running for manager in managers.values()),
                    "observation_complete": reason == "RELEASED",
                }
            ),
        )
