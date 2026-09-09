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
) -> None:
    # Validators recognize their own transaction targets; a mutable metadata
    # label must not be able to disable a protocol's execution boundary.
    for connector in CONNECTOR_REGISTRY.all():
        if connector.execution_validator is None:
            continue
        validator = connector.execution_validator.load()
        observer = observer_factory() if callable(observer_factory) else None
        validator(bundle, chain=chain, wallet=wallet, is_safe=is_safe, gateway=observer)
