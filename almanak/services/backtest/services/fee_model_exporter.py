"""Fee model export — serializes registered fee models to JSON."""

from __future__ import annotations

import logging

from almanak.services.backtest.models import FeeModelDetail, FeeModelSummary

logger = logging.getLogger(__name__)

_REGISTRY_LOADED = False


def _ensure_registry_loaded() -> None:
    """Import the fee_models package to trigger all registrations.

    The package __init__.py imports every protocol module and calls
    ``FeeModelRegistry.register()`` for each one. A single package
    import is sufficient to populate the full registry.
    """
    global _REGISTRY_LOADED
    if _REGISTRY_LOADED:
        return
    try:
        import almanak.framework.backtesting.pnl.fee_models  # noqa: F401

        _REGISTRY_LOADED = True
    except ImportError:
        logger.warning("Fee model package could not be imported — fee model endpoints will return empty lists")


def list_fee_models() -> list[FeeModelSummary]:
    """List all registered fee models as summaries."""
    from almanak.framework.backtesting.pnl.fee_models.base import FeeModelRegistry

    _ensure_registry_loaded()

    summaries = []
    for name, metadata in FeeModelRegistry.list_all().items():
        summaries.append(
            FeeModelSummary(
                protocol=name,
                model_name=metadata.name,
                supported_chains=metadata.protocols or [name],
            )
        )
    return summaries


def get_fee_model_detail(protocol: str) -> FeeModelDetail | None:
    """Get detailed fee model info for a specific protocol."""
    from almanak.framework.backtesting.pnl.fee_models.base import FeeModelRegistry

    _ensure_registry_loaded()

    metadata = FeeModelRegistry.get_metadata(protocol)
    if metadata is None:
        return None

    model_class = metadata.model_class
    try:
        instance = model_class()
        model_dict = instance.to_dict()
    except Exception:
        model_dict = {}

    return FeeModelDetail(
        protocol=protocol,
        model_name=metadata.name,
        fee_tiers=model_dict.get("fee_tiers", []),
        default_fee=model_dict.get("default_fee"),
        slippage_model=model_dict.get("slippage_model", "default"),
        supported_intent_types=model_dict.get("supported_intent_types", []),
        supported_chains=metadata.protocols or [protocol],
        gas_estimates=model_dict.get("gas_estimates", {}),
    )
