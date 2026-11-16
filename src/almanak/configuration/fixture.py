"""Freeze an unprovisioned fixture plan in the runtime's private workspace."""

import fcntl
import os
from pathlib import Path

from almanak.configuration.resolution import ReadContext, ResolutionAnchors
from almanak.gateway.fixture_preparation import FixturePlan, compile_fixture
from almanak.gateway.operator_config import OperatorConfig
from almanak.workspace import save


def prepare_fixture(
    path: Path, context: ReadContext, operator: OperatorConfig, anchors: ResolutionAnchors
) -> FixturePlan:
    plan = compile_fixture(context, operator, anchors)
    with path.with_suffix(".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            raise RuntimeError("fixture preparation already has an active owner") from None
        if path.exists():
            retained = FixturePlan.model_validate_json(path.read_text())
            if retained != plan:
                raise ValueError(
                    "retained fixture plan differs; automatic reprovisioning is refused"
                )
            return retained
        save(path, plan.model_dump(mode="json"))
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return plan
