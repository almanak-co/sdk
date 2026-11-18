"""Frozen pool sources and acquisition policy identify reusable datasets."""

import hashlib
import json
from typing import Protocol

from almanak.backtest.prepared import DatasetManifest


def targets(config):
    return tuple(config.target(a.name) for a in config.allocations)


def identity(config, settings, model, revision):
    body = {
        "targets": [t.model_dump(mode="json") for t in targets(config)],
        "data": settings.data.model_dump(mode="json"),
        "start": int(settings.schedule.start),
        "end": int(settings.schedule.deadline),
        "providers": [h.model_dump(mode="json", exclude={"collection"}) for h in settings.history],
        "model": model,
        "acquisition_revision": revision,
    }
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


class PoolDataset(Protocol):
    manifest: DatasetManifest

    def __init__(self, directory, config, settings, expected_hash=None): ...

    @staticmethod
    def identity(config, settings) -> str: ...

    def close(self) -> None: ...
