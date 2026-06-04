"""Polymarket prediction-read spec (VIB-4989).

Publishes :data:`PREDICTION_READ_SPEC` for
:class:`~almanak.connectors._strategy_base.prediction_read_registry.PredictionReadRegistry`:
builds the relocated :class:`PredictionMarketDataProvider` wired to the
gateway-routed CLOB client, mirroring the legacy ``cli/run`` construction.
"""

from almanak.connectors._strategy_base.prediction_read_base import PredictionReadSpec


def _build_provider(*, gateway_client, wallet=None):
    # Mirror the legacy cli/run wiring: a gateway-routed CLOB client wrapped by the
    # provider. ``wallet`` is part of the registry's factory contract but unused at
    # construction (positions are queried per-call with an explicit wallet).
    from almanak.connectors.polymarket.gateway_client import GatewayPolymarketClient
    from almanak.connectors.polymarket.prediction_provider import PredictionMarketDataProvider

    return PredictionMarketDataProvider(GatewayPolymarketClient(gateway_client))  # type: ignore[arg-type]


PREDICTION_READ_SPEC = PredictionReadSpec(build_provider=_build_provider, chains=frozenset({"polygon"}))
