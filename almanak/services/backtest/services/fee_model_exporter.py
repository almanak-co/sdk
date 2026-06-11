"""Fee model export — serializes registered fee models to JSON.

VIB-4851 Phase D: the per-protocol standard fields (fee tiers, default fee,
slippage-model id, supported intents/chains, gas estimates) live on each
connector's ``fee_model`` module as ``BACKTEST_EXPORT_METADATA``, next to the
model class the registry derives from the connector manifest. This service
names no protocol — adding a connector ships its backtest-service metadata in
the same folder (previously this module held a central ``_PROTOCOL_METADATA``
table keyed by protocol name).
"""

from __future__ import annotations

import importlib
import logging
from typing import Any

from almanak.services.backtest.models import FeeModelDetail, FeeModelSummary

logger = logging.getLogger(__name__)


def _export_metadata_for(model_class: type) -> dict[str, Any]:
    """Return the owning module's ``BACKTEST_EXPORT_METADATA`` (or ``{}``).

    The fee-model module contract: a connector's ``fee_model`` module MAY
    publish a module-level ``BACKTEST_EXPORT_METADATA`` dict carrying the
    FeeModelDetail standard fields. Models without one (e.g. runtime-registered
    custom models) fall back to the same defaults as before.
    """
    try:
        module = importlib.import_module(model_class.__module__)
    except ImportError:  # pragma: no cover - the class itself was importable
        return {}
    metadata = getattr(module, "BACKTEST_EXPORT_METADATA", {})
    return metadata if isinstance(metadata, dict) else {}


def list_fee_models() -> list[FeeModelSummary]:
    """List all registered fee models as summaries."""
    from almanak.framework.backtesting.pnl.fee_models.base import FeeModelRegistry

    summaries = []
    for name, metadata in FeeModelRegistry.list_all().items():
        # Use connector-owned metadata for chains if available, else fall
        # back to the registry's protocol list.
        proto_meta = _export_metadata_for(metadata.model_class)
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

    metadata = FeeModelRegistry.get_metadata(protocol)
    if metadata is None:
        return None

    # Connector-owned standard fields for this model
    proto_meta = _export_metadata_for(metadata.model_class)

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
