"""Protocol-owned deterministic checks shared by local and gateway execution."""

from typing import Any

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.framework.models.reproduction_bundle import ActionBundle


def validate_connector_execution(
    bundle: ActionBundle,
    *,
    chain: str,
    wallet: str,
    is_safe: bool,
    observer_factory: Any = None,
    managed_fork: bool | None = None,
) -> list[dict[str, Any]]:
    # Validators recognize their own transaction targets; a mutable metadata
    # label must not be able to disable a protocol's execution boundary.
    observations = []
    for connector in CONNECTOR_REGISTRY.all():
        if connector.execution_validator is None:
            continue
        validator = connector.execution_validator.load()
        observer = observer_factory() if callable(observer_factory) else None
        observation = validator(
            bundle, chain=chain, wallet=wallet, is_safe=is_safe, gateway=observer, managed_fork=managed_fork
        )
        if observation is not None:
            observations.append(observation)
    return observations
