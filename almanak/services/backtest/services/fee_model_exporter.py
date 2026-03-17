"""Fee model export — serializes registered fee models to JSON."""

from __future__ import annotations

import logging
from typing import Any

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


# ---------------------------------------------------------------------------
# Protocol-specific metadata that cannot be derived from to_dict() alone.
# Maps protocol name -> standard fields for the FeeModelDetail response.
# ---------------------------------------------------------------------------

_PROTOCOL_METADATA: dict[str, dict[str, Any]] = {
    "uniswap_v3": {
        "fee_tiers": [0.0001, 0.0005, 0.003, 0.01],
        "default_fee": 0.003,
        "slippage_model": "sqrt_impact",
        "supported_intent_types": ["SWAP", "LP_OPEN", "LP_CLOSE"],
        "supported_chains": ["ethereum", "arbitrum", "optimism", "base", "polygon", "bsc", "avalanche"],
        "gas_estimates": {"swap": 150_000, "lp_open": 350_000, "lp_close": 250_000},
    },
    "pancakeswap_v3": {
        "fee_tiers": [0.0001, 0.0005, 0.0025, 0.01],
        "default_fee": 0.0025,
        "slippage_model": "sqrt_impact",
        "supported_intent_types": ["SWAP", "LP_OPEN", "LP_CLOSE"],
        "supported_chains": ["bsc", "ethereum", "arbitrum", "base"],
        "gas_estimates": {"swap": 160_000, "lp_open": 360_000, "lp_close": 260_000},
    },
    "aerodrome": {
        "fee_tiers": [0.0001, 0.003],
        "default_fee": 0.003,
        "slippage_model": "constant_product",
        "supported_intent_types": ["SWAP", "LP_OPEN", "LP_CLOSE"],
        "supported_chains": ["base"],
        "gas_estimates": {"swap": 180_000, "lp_open": 300_000, "lp_close": 200_000},
    },
    "curve": {
        "fee_tiers": [0.0001, 0.0004, 0.0013],
        "default_fee": 0.0004,
        "slippage_model": "stableswap",
        "supported_intent_types": ["SWAP"],
        "supported_chains": ["ethereum", "arbitrum", "optimism", "base", "polygon", "avalanche"],
        "gas_estimates": {"swap": 250_000},
    },
    "aave_v3": {
        "fee_tiers": [0.0001],
        "default_fee": 0.0001,
        "slippage_model": "none",
        "supported_intent_types": ["SUPPLY", "BORROW", "WITHDRAW", "REPAY"],
        "supported_chains": ["ethereum", "arbitrum", "optimism", "base", "polygon", "avalanche"],
        "gas_estimates": {"supply": 200_000, "borrow": 300_000, "repay": 250_000, "withdraw": 200_000},
    },
    "morpho": {
        "fee_tiers": [],
        "default_fee": 0.0,
        "slippage_model": "none",
        "supported_intent_types": ["SUPPLY", "BORROW", "WITHDRAW", "REPAY"],
        "supported_chains": ["ethereum", "base"],
        "gas_estimates": {"supply": 180_000, "borrow": 280_000, "repay": 230_000, "withdraw": 180_000},
    },
    "compound_v3": {
        "fee_tiers": [],
        "default_fee": 0.0,
        "slippage_model": "none",
        "supported_intent_types": ["SUPPLY", "BORROW", "WITHDRAW", "REPAY"],
        "supported_chains": ["ethereum", "arbitrum", "optimism", "base", "polygon"],
        "gas_estimates": {"supply": 150_000, "borrow": 250_000, "repay": 200_000, "withdraw": 150_000},
    },
    "gmx": {
        "fee_tiers": [0.0005, 0.001],
        "default_fee": 0.001,
        "slippage_model": "price_impact",
        "supported_intent_types": ["PERP_OPEN", "PERP_CLOSE", "SWAP"],
        "supported_chains": ["arbitrum", "avalanche"],
        "gas_estimates": {"perp_open": 800_000, "perp_close": 600_000, "swap": 400_000},
    },
    "hyperliquid": {
        "fee_tiers": [0.00026, 0.00027, 0.00028, 0.0003, 0.00035, 0.0004, 0.00045],
        "default_fee": 0.00045,
        "slippage_model": "orderbook",
        "supported_intent_types": ["PERP_OPEN", "PERP_CLOSE"],
        "supported_chains": ["hyperliquid"],
        "gas_estimates": {},
    },
}


def list_fee_models() -> list[FeeModelSummary]:
    """List all registered fee models as summaries."""
    from almanak.framework.backtesting.pnl.fee_models.base import FeeModelRegistry

    _ensure_registry_loaded()

    summaries = []
    for name, metadata in FeeModelRegistry.list_all().items():
        # Use protocol metadata for chains if available, else fall back to registry
        proto_meta = _PROTOCOL_METADATA.get(name, {})
        chains = proto_meta.get("supported_chains", metadata.protocols or [name])

        summaries.append(
            FeeModelSummary(
                protocol=name,
                model_name=metadata.model_class.__name__,
                supported_chains=chains,
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

    # Try to get protocol-specific standard fields
    proto_meta = _PROTOCOL_METADATA.get(metadata.name, {})

    # Also get model's own to_dict() for any extra data
    model_class = metadata.model_class
    try:
        instance = model_class()
        model_dict = instance.to_dict()
    except Exception:
        logger.warning("Failed to instantiate %s for to_dict() — raw_config will be empty", model_class.__name__)
        model_dict = {}

    return FeeModelDetail(
        protocol=metadata.name,
        model_name=metadata.model_class.__name__,
        fee_tiers=proto_meta.get("fee_tiers", []),
        default_fee=proto_meta.get("default_fee"),
        slippage_model=proto_meta.get("slippage_model", "default"),
        supported_intent_types=proto_meta.get("supported_intent_types", []),
        supported_chains=proto_meta.get(
            "supported_chains",
            metadata.protocols or [metadata.name],
        ),
        gas_estimates=proto_meta.get("gas_estimates", {}),
        raw_config=model_dict,
    )
