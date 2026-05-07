"""Service-boundary env-variable collection (Phase 1).

Owns every os.environ read for unprefixed ALMANAK_* and bare-name fallbacks
that previously lived in GatewaySettings._fallback_env_vars and the
_resolve_polymarket_* methods. Single source of truth post Phase 1.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from almanak.config.base import GatewayConfig

# Logger name preserved verbatim because tests in
# tests/unit/config/test_env_fallbacks.py assert the unified-signer INFO log
# is emitted on this logger (legacy "almanak.gateway.core.settings" channel).
logger = logging.getLogger("almanak.gateway.core.settings")

# Path-aware load tracking. The SDK genuinely has more than one dotenv
# source per process: the Click main group loads the cwd default, and a
# per-strategy command (``strat run``, ``strat test``, ``ax``, teardown,
# permissions, paper) layers a strategy-folder ``.env`` on top. The earlier
# process-wide boolean made the second load a silent no-op (PR #2152
# review — Codex P1 + CodeRabbit). The set below keys on the resolved
# absolute path so each distinct file is loaded exactly once, while
# different paths layer additively.
_LOADED_DOTENVS: set[str] = set()
# The no-arg cwd ladder is also load-once, AND is suppressed once any
# explicit path has been loaded — an explicit path is always strictly
# more authoritative than the cwd default, so re-reading the cwd would
# only merge in unintended values from the dev's working directory.
_DEFAULT_LOADED: bool = False


def _load_dotenv_once(dotenv_path: str | None = None) -> None:
    """Load a dotenv source at most once per resolved path per process.

    Two distinct invariants are pinned here — don't conflate them:

    * **Once-per-source**: each distinct absolute ``dotenv_path`` is loaded
      exactly once, and the no-arg cwd ladder is also load-once. Different
      explicit paths layer additively (the SDK's strategy-folder model
      genuinely has more than one ``.env`` per process: the Click main
      group loads the cwd default, then ``strat run`` / ``strat test`` /
      ``ax`` / teardown / permissions / paper layer a strategy-folder
      ``.env`` on top).

    * **No-arg suppression after explicit**: a no-arg call (``dotenv_path
      is None``) becomes a no-op once any explicit path has loaded. This
      is the *invocation* contract from Gemini's PR #2107 finding —
      ``load_config(dotenv_path="custom.env")`` triggers an inner
      ``gateway_config_from_env`` that calls ``_load_dotenv_once()`` no-arg;
      without this guard that inner call would silently merge in the
      dev's cwd ``.env``. It is NOT a per-key value-precedence claim.

    **Per-key value precedence** is whatever ``python-dotenv`` gives you:
    the default ``override=False`` means values already in ``os.environ``
    (incl. shell exports and earlier dotenv loads) win. So when multiple
    explicit paths layer additively, the *first-loaded* file's values
    stick where keys overlap; later loads only fill in the gaps. Concrete
    example: cwd ``.env`` loads first at the Click main group, then a
    strategy ``.env`` loads in ``strat run`` — for keys present in both,
    cwd wins. Shell exports beat both. Reorder loads (or load the most
    specific source first) if a different precedence is required.
    """
    global _DEFAULT_LOADED
    if dotenv_path:
        key = str(Path(dotenv_path).resolve())
        if key in _LOADED_DOTENVS:
            return
        load_dotenv(dotenv_path)
        _LOADED_DOTENVS.add(key)
        return
    if _DEFAULT_LOADED or _LOADED_DOTENVS:
        return
    load_dotenv()
    _DEFAULT_LOADED = True


def _apply_gateway_env_fallbacks(gateway: GatewayConfig) -> None:  # noqa: C901
    """Replicate GatewaySettings._fallback_env_vars at the service boundary.

    Mutates gateway in-place, only setting fields that are currently falsy.
    Preserves bit-for-bit behavior of the deleted in-class validator.
    """
    if not gateway.private_key:
        if v := os.environ.get("ALMANAK_PRIVATE_KEY"):
            gateway.private_key = v
    if not gateway.solana_private_key:
        if v := os.environ.get("SOLANA_PRIVATE_KEY"):
            gateway.solana_private_key = v
    if not gateway.eoa_address:
        if v := os.environ.get("ALMANAK_EOA_ADDRESS"):
            gateway.eoa_address = v
    if not gateway.safe_address:
        if v := os.environ.get("ALMANAK_SAFE_ADDRESS"):
            gateway.safe_address = v
    if not gateway.zodiac_roles_address:
        # NB: legacy env name is ALMANAK_ZODIAC_ADDRESS — not the field-name
        # mirror ALMANAK_ZODIAC_ROLES_ADDRESS.
        if v := os.environ.get("ALMANAK_ZODIAC_ADDRESS"):
            gateway.zodiac_roles_address = v
    if not gateway.signer_service_url:
        if v := os.environ.get("ALMANAK_SIGNER_SERVICE_URL"):
            gateway.signer_service_url = v
    if not gateway.signer_service_jwt:
        if v := os.environ.get("ALMANAK_SIGNER_SERVICE_JWT"):
            gateway.signer_service_jwt = v
    # Third-party API keys are injected by deployers under bare names
    # (ALCHEMY_API_KEY, not ALMANAK_GATEWAY_ALCHEMY_API_KEY) so the same env
    # var feeds both pydantic-settings consumers and direct os.environ readers.
    if not gateway.alchemy_api_key:
        if v := os.environ.get("ALCHEMY_API_KEY"):
            gateway.alchemy_api_key = v
    if not gateway.coingecko_api_key:
        if v := os.environ.get("COINGECKO_API_KEY"):
            gateway.coingecko_api_key = v
    if not gateway.enso_api_key:
        if v := os.environ.get("ENSO_API_KEY"):
            gateway.enso_api_key = v
    if not gateway.portfolio_api_key:
        if v := os.environ.get("ALMANAK_PORTFOLIO_API_KEY") or os.environ.get("ZERION_API_KEY"):
            gateway.portfolio_api_key = v
    if not gateway.portfolio_providers:
        if v := os.environ.get("PORTFOLIO_PROVIDERS"):
            gateway.portfolio_providers = v


def _resolve_polymarket_credentials(gateway: GatewayConfig) -> None:
    """Polymarket-specific fallback ladders.

    Was ``GatewaySettings._resolve_polymarket_credentials`` — moved to the
    service boundary so the only env reader for Polymarket-related variables
    lives in this module.
    """
    _resolve_polymarket_wallet_address(gateway)
    _resolve_polymarket_private_key(gateway)
    _resolve_polymarket_api_credentials(gateway)


def _resolve_polymarket_wallet_address(gateway: GatewayConfig) -> None:
    """Polymarket wallet-address fallback ladder (most specific -> least).

    Rung 1 (``ALMANAK_GATEWAY_POLYMARKET_WALLET_ADDRESS``) is handled by the
    pydantic prefix on the model itself; this function only fills rungs 2-3.
    """
    if gateway.polymarket_wallet_address:
        return
    for env_name in ("POLYMARKET_WALLET_ADDRESS", "ALMANAK_POLYMARKET_WALLET_ADDRESS"):
        if v := os.environ.get(env_name):
            gateway.polymarket_wallet_address = v
            return


def _resolve_polymarket_private_key(gateway: GatewayConfig) -> None:
    """Polymarket private-key fallback ladder (VIB-3772).

    Rung order — first non-empty wins:

    1. Already-set field (incl. constructor kwarg / pydantic-prefix env).
    2. ``POLYMARKET_PRIVATE_KEY`` (legacy bare name).
    3. ``ALMANAK_POLYMARKET_PRIVATE_KEY`` (almanak-prefixed alias).
    4. ``self.private_key`` — the already-resolved primary signer key.
       Unifies the credential surface so a single ``ALMANAK_PRIVATE_KEY`` is
       enough to start signing Polymarket orders.

    Rung 4 is intentionally a copy of an already-resolved field, not a
    re-read of the env var, so explicit constructor kwargs and the
    gateway-prefixed env var both flow through the same primary signer the
    rest of the gateway uses.
    """
    polymarket_key_was_unset = not gateway.polymarket_private_key
    if gateway.polymarket_private_key:
        return  # rung 1
    for env_name in ("POLYMARKET_PRIVATE_KEY", "ALMANAK_POLYMARKET_PRIVATE_KEY"):
        if v := os.environ.get(env_name):
            gateway.polymarket_private_key = v
            return
    # rung 4: copy from already-resolved primary signer.
    if gateway.private_key:
        gateway.polymarket_private_key = gateway.private_key
        if polymarket_key_was_unset:
            _log_polymarket_unified_signer()


def _log_polymarket_unified_signer() -> None:
    """Emit unified-signer log with dynamic source label.

    Format and source-label resolution match the deleted GatewaySettings
    method exactly (asserted by tests in tests/unit/config/test_env_fallbacks.py).
    """
    if os.environ.get("ALMANAK_GATEWAY_PRIVATE_KEY"):
        source_label = "ALMANAK_GATEWAY_PRIVATE_KEY"
    elif os.environ.get("ALMANAK_PRIVATE_KEY"):
        source_label = "ALMANAK_PRIVATE_KEY"
    else:
        source_label = "explicit private_key (constructor)"
    logger.info(
        "Polymarket signing using %s "
        "(set POLYMARKET_PRIVATE_KEY or ALMANAK_GATEWAY_POLYMARKET_PRIVATE_KEY "
        "to use a separate wallet for Polymarket).",
        source_label,
    )


def _resolve_polymarket_api_credentials(gateway: GatewayConfig) -> None:
    """Polymarket CLOB API credential fallbacks (api_key / secret / passphrase).

    Each falls back ``POLYMARKET_X`` then ``ALMANAK_POLYMARKET_X``.
    """
    if not gateway.polymarket_api_key:
        for env_name in ("POLYMARKET_API_KEY", "ALMANAK_POLYMARKET_API_KEY"):
            if v := os.environ.get(env_name):
                gateway.polymarket_api_key = v
                break
    if not gateway.polymarket_secret:
        for env_name in ("POLYMARKET_SECRET", "ALMANAK_POLYMARKET_SECRET"):
            if v := os.environ.get(env_name):
                gateway.polymarket_secret = v
                break
    if not gateway.polymarket_passphrase:
        for env_name in ("POLYMARKET_PASSPHRASE", "ALMANAK_POLYMARKET_PASSPHRASE"):
            if v := os.environ.get(env_name):
                gateway.polymarket_passphrase = v
                break


def gateway_config_from_env(**overrides: Any) -> GatewayConfig:
    """Public API: construct a fully-resolved GatewayConfig.

    Equivalent to ``GatewaySettings(**overrides)`` before Phase 1 — this is
    the function tests should call instead of constructing ``GatewaySettings``
    directly. Production code paths route through
    :func:`almanak.config.service.load_config`.

    Override precedence (highest -> lowest): explicit kwargs > prefixed env
    (``ALMANAK_GATEWAY_*``) > unprefixed-fallback env (handled here) > field
    defaults. To preserve that order this function lets pydantic-settings
    populate the fields first, then applies the unprefixed fallbacks via
    post-construction mutation — splatting the unprefixed env as kwargs would
    invert priority.

    The dotenv ingest is performed here via :func:`_load_dotenv_once` so
    callers (including direct callers that don't go through
    :func:`almanak.config.service.load_config`) get the same ``.env``
    behaviour. The model itself no longer has ``env_file=".env"`` —
    this function is the single dotenv boundary for gateway construction.
    """
    _load_dotenv_once()
    gateway = GatewayConfig(**overrides)
    _apply_gateway_env_fallbacks(gateway)
    _resolve_polymarket_credentials(gateway)
    return gateway


__all__ = [
    "_apply_gateway_env_fallbacks",
    "_load_dotenv_once",
    "_resolve_polymarket_credentials",
    "gateway_config_from_env",
]
