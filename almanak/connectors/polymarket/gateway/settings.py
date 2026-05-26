"""Polymarket gateway-side configuration fragment (VIB-4812).

This fragment is composed into the central ``GatewaySettings`` via
multi-inheritance — it is NOT a standalone ``BaseSettings`` model. The
gateway carries a single env-loader (``GatewaySettings`` itself) so that
env-prefix discipline, validators, and parity tests all live in one place.

Why a separate module:
- Phase 4 of the connector-self-containment program (VIB-4808) moves every
  protocol-specific field out of ``almanak/gateway/core/settings.py``. The
  central settings class composes the fragments via multi-inheritance and
  the env-var surface is preserved byte-identically:

      ALMANAK_GATEWAY_POLYMARKET_NETWORK
      ALMANAK_GATEWAY_POLYMARKET_MARKET_CACHE_TTL_SECONDS
      ALMANAK_GATEWAY_POLYMARKET_WALLET_ADDRESS
      ALMANAK_GATEWAY_POLYMARKET_PRIVATE_KEY
      ALMANAK_GATEWAY_POLYMARKET_API_KEY
      ALMANAK_GATEWAY_POLYMARKET_SECRET
      ALMANAK_GATEWAY_POLYMARKET_PASSPHRASE

  The unprefixed and bare-name fallback ladders (POLYMARKET_PRIVATE_KEY,
  ALMANAK_POLYMARKET_*) continue to live at the service boundary in
  ``almanak/config/env.py`` — they are not the model's concern.

Strategy-side code MUST NOT import this module — credentials live in the
gateway. The import-graph lint enforces it.
"""

from __future__ import annotations

import math

from pydantic import BaseModel, field_validator


class PolymarketGatewaySettings(BaseModel):
    """Polymarket gateway-side fields.

    Inherited by ``GatewaySettings``. The pydantic-settings ``env_prefix``
    is configured on the composed class; declarations here only need to
    name fields and validators.
    """

    # Polymarket runtime network — "mainnet" or "anvil". Surfaced on the
    # ``settings.polymarket_network`` attribute by both the prefixed
    # ``ALMANAK_GATEWAY_POLYMARKET_NETWORK`` env var and the bare-name
    # ``ALMANAK_POLYMARKET_NETWORK`` fallback applied at the service
    # boundary in ``almanak/config/env.py``.
    polymarket_network: str = "mainnet"

    # Market shape cache TTL (seconds). Used by the LRU cache in
    # ``PolymarketServiceServicer`` to age out stale market metadata.
    # ``[0, 24h]`` is enforced for the bare-name fallback at the service
    # boundary; the validator below mirrors the floor + NaN reject for
    # the kwargs / ALMANAK_GATEWAY_* path so all three branches agree.
    polymarket_market_cache_ttl_seconds: float = 60.0

    # CLOB credentials. These are optional: local EOA mode derives the
    # signer from the gateway execution identity and lazy-derives L2
    # credentials automatically when absent.
    polymarket_wallet_address: str | None = None
    polymarket_private_key: str | None = None
    polymarket_api_key: str | None = None
    polymarket_secret: str | None = None
    polymarket_passphrase: str | None = None

    @field_validator("polymarket_market_cache_ttl_seconds")
    @classmethod
    def _validate_cache_ttl(cls, value: float) -> float:
        # The legacy ``_parse_polymarket_market_cache_ttl_seconds`` helper
        # clamps to ``[0, 24h]`` for the unprefixed fallback path; mirror
        # ``>= 0`` here so the kwargs / ALMANAK_GATEWAY_* paths agree on the
        # floor, and reject NaN that would defeat the clamp.
        if not math.isfinite(value):
            raise ValueError(f"polymarket_market_cache_ttl_seconds must be a finite number (got {value!r})")
        if value < 0:
            raise ValueError(f"polymarket_market_cache_ttl_seconds must be >= 0 (got {value})")
        return value


__all__ = ["PolymarketGatewaySettings"]
