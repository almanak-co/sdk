"""Service-boundary env-variable collection (Phase 1).

Owns every os.environ read for unprefixed ALMANAK_* and bare-name fallbacks
that previously lived in GatewaySettings._fallback_env_vars and the
_resolve_polymarket_* methods. Single source of truth post Phase 1.
"""

from __future__ import annotations

import logging
import math
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from almanak.config.base import GatewayConfig

# Logger name preserved verbatim because tests in
# tests/unit/config/test_env_fallbacks.py assert the unified-signer INFO log
# is emitted on this logger (legacy "almanak.gateway.core.settings" channel).
logger = logging.getLogger("almanak.gateway.core.settings")

_POLYMARKET_MARKET_CACHE_TTL_MAX_SECONDS = 24 * 3600.0

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


def _parse_float_with_default(
    env_var: str,
    default: float,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
    min_inclusive: bool = True,
    max_inclusive: bool = True,
) -> float:
    """Parse a float env var, logging and falling back to ``default`` on error.

    Non-finite values (``nan`` / ``inf`` / ``-inf``) are rejected the same way
    as unparseable strings — otherwise they would propagate into downstream
    gate comparisons (DexScreener thresholds, watchdog interval) and silently
    disable the safety checks.

    Optional ``min_value`` / ``max_value`` bounds keep the unprefixed-env
    fallback path in sync with the model validators on ``GatewaySettings``:
    ``GatewaySettings`` does not enable ``validate_assignment``, so a
    negative / out-of-range value coming through this helper would
    otherwise reach the field without triggering ``_validate_positive_float``
    or ``_validate_turnover_ratio``. Set ``min_inclusive=False`` to enforce
    a strict ``>`` lower bound (matching the ``> 0`` validators);
    ``max_inclusive=False`` does the symmetric thing on the upper bound.
    """
    raw = os.environ.get(env_var)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("Invalid float in env %s=%r; using default %s", env_var, raw, default)
        return default
    if not math.isfinite(value):
        logger.warning("%s=%r is not a finite number; using default %s", env_var, raw, default)
        return default
    if min_value is not None:
        below = value < min_value if min_inclusive else value <= min_value
        if below:
            logger.warning(
                "%s=%r is below the allowed minimum (%s%s); using default %s",
                env_var,
                raw,
                ">=" if min_inclusive else ">",
                min_value,
                default,
            )
            return default
    if max_value is not None:
        above = value > max_value if max_inclusive else value >= max_value
        if above:
            logger.warning(
                "%s=%r is above the allowed maximum (%s%s); using default %s",
                env_var,
                raw,
                "<=" if max_inclusive else "<",
                max_value,
                default,
            )
            return default
    return value


def _parse_polymarket_market_cache_ttl_seconds(default: float) -> float:
    """Parse the Polymarket market-cache TTL with the legacy clamp/fallback semantics."""
    env_var = "ALMANAK_POLYMARKET_MARKET_CACHE_TTL_SECONDS"
    raw = os.environ.get(env_var)
    if not raw:
        return default
    try:
        ttl = float(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; falling back to default %ss", env_var, raw, default)
        return default
    if not math.isfinite(ttl):
        logger.warning("%s=%r is not a finite number; falling back to default %ss", env_var, raw, default)
        return default
    return max(0.0, min(ttl, _POLYMARKET_MARKET_CACHE_TTL_MAX_SECONDS))


# Falsy-checked secret/identifier fallbacks: ``(field_name, env_var)`` pairs.
# Legacy contract from the deleted ``GatewaySettings._fallback_env_vars``:
# fill ``gateway.<field>`` from the bare-name env var only when the gateway
# field is falsy (``None`` or ``""``). Third-party API keys are injected by
# deployers under bare names so the same env var feeds both pydantic-settings
# consumers and direct ``os.environ`` readers.
#
# NB: ``zodiac_roles_address`` uses ``ALMANAK_ZODIAC_ADDRESS`` — the legacy
# env name does not mirror the field name.
_FALSY_FALLBACK_PAIRS: tuple[tuple[str, str], ...] = (
    ("private_key", "ALMANAK_PRIVATE_KEY"),
    ("solana_private_key", "SOLANA_PRIVATE_KEY"),
    ("eoa_address", "ALMANAK_EOA_ADDRESS"),
    ("safe_address", "ALMANAK_SAFE_ADDRESS"),
    ("zodiac_roles_address", "ALMANAK_ZODIAC_ADDRESS"),
    ("signer_service_url", "ALMANAK_SIGNER_SERVICE_URL"),
    ("signer_service_jwt", "ALMANAK_SIGNER_SERVICE_JWT"),
    ("alchemy_api_key", "ALCHEMY_API_KEY"),
    ("coingecko_api_key", "COINGECKO_API_KEY"),
    ("enso_api_key", "ENSO_API_KEY"),
    ("portfolio_providers", "PORTFOLIO_PROVIDERS"),
)

# ``is None`` fallbacks: same pattern, but the explicit-None check preserves
# an empty-string sentinel. A Tenderly operator setting
# ``TENDERLY_ACCESS_KEY=""`` to disable the integration must not be silently
# re-filled from a higher-up env layer.
_OPTIONAL_STRING_FALLBACK_PAIRS: tuple[tuple[str, str], ...] = (
    ("thegraph_api_key", "THEGRAPH_API_KEY"),
    ("tenderly_account_slug", "TENDERLY_ACCOUNT_SLUG"),
    ("tenderly_project_slug", "TENDERLY_PROJECT_SLUG"),
    ("tenderly_access_key", "TENDERLY_ACCESS_KEY"),
)


def _apply_gateway_env_fallbacks(gateway: GatewayConfig) -> None:
    """Replicate GatewaySettings._fallback_env_vars at the service boundary.

    Mutates ``gateway`` in-place. Three classes of fallback live here; each
    helper documents its own precedence rule:

    * :func:`_apply_secret_string_fallbacks` — falsy-check fallback for
      credentials and required identifiers.
    * :func:`_apply_optional_string_fallbacks` — ``is None`` fallback for
      optional integration keys (preserves empty-string sentinel).
    * :func:`_apply_dexscreener_threshold_fallbacks`,
      :func:`_apply_polymarket_runtime_fallbacks`,
      :func:`_apply_anvil_watchdog_fallback` — ``model_fields_set`` fallback
      for typed-numeric fields with non-``None`` defaults, with bounds that
      mirror the matching ``GatewaySettings`` validator.
    """
    _apply_secret_string_fallbacks(gateway)
    _apply_optional_string_fallbacks(gateway)
    _apply_dexscreener_threshold_fallbacks(gateway)
    _apply_polymarket_runtime_fallbacks(gateway)
    _apply_anvil_watchdog_fallback(gateway)


def _apply_secret_string_fallbacks(gateway: GatewayConfig) -> None:
    """Fill secret-string fields from bare-name env vars when falsy.

    Covers credentials and third-party API keys. Empty-string fields trigger
    the fallback — matches the bit-for-bit semantics of the deleted
    ``_fallback_env_vars`` validator.
    """
    for field_name, env_var in _FALSY_FALLBACK_PAIRS:
        if not getattr(gateway, field_name):
            if value := os.environ.get(env_var):
                setattr(gateway, field_name, value)
    # ``portfolio_api_key`` has a two-name fallback ladder — handled inline
    # because the loop above is single-env-var per field.
    if not gateway.portfolio_api_key:
        if value := os.environ.get("ALMANAK_PORTFOLIO_API_KEY") or os.environ.get("ZERION_API_KEY"):
            gateway.portfolio_api_key = value


def _apply_optional_string_fallbacks(gateway: GatewayConfig) -> None:
    """Fill optional-string fields from bare-name env vars when ``None``.

    Distinct from :func:`_apply_secret_string_fallbacks`: the explicit
    ``is None`` check preserves an empty-string sentinel so an operator can
    disable an integration (e.g. ``TENDERLY_ACCESS_KEY=""``) without the
    env-fallback layer treating it as unset.
    """
    for field_name, env_var in _OPTIONAL_STRING_FALLBACK_PAIRS:
        if getattr(gateway, field_name) is None:
            raw = os.environ.get(env_var)
            if raw is not None:
                setattr(gateway, field_name, raw)


def _apply_dexscreener_threshold_fallbacks(gateway: GatewayConfig) -> None:
    """Fill DexScreener numeric thresholds from unprefixed env vars.

    Documented precedence is ``kwargs > ALMANAK_GATEWAY_* > unprefixed >
    defaults``. Guarded on ``gateway.model_fields_set`` so an unprefixed
    env var only fills values that the higher-precedence sources have not
    already supplied. Bounds mirror the matching ``GatewaySettings``
    validator — strict ``> 0`` for liquidity/volume/dominance, ``[0, 1]``
    for the turnover ratio — because ``validate_assignment`` is not
    enabled on the model, so an out-of-range value coming through this
    path would otherwise reach the field without firing the validator.
    """
    if "dexscreener_min_liquidity_usd" not in gateway.model_fields_set:
        gateway.dexscreener_min_liquidity_usd = _parse_float_with_default(
            "ALMANAK_DEXSCREENER_MIN_LIQUIDITY_USD",
            gateway.dexscreener_min_liquidity_usd,
            min_value=0.0,
            min_inclusive=False,
        )
    if "dexscreener_min_volume_usd" not in gateway.model_fields_set:
        gateway.dexscreener_min_volume_usd = _parse_float_with_default(
            "ALMANAK_DEXSCREENER_MIN_VOLUME_USD",
            gateway.dexscreener_min_volume_usd,
            min_value=0.0,
            min_inclusive=False,
        )
    if "dexscreener_min_turnover_ratio" not in gateway.model_fields_set:
        gateway.dexscreener_min_turnover_ratio = _parse_float_with_default(
            "ALMANAK_DEXSCREENER_MIN_TURNOVER_RATIO",
            gateway.dexscreener_min_turnover_ratio,
            min_value=0.0,
            max_value=1.0,
        )
    if "dexscreener_dominance_multiple" not in gateway.model_fields_set:
        gateway.dexscreener_dominance_multiple = _parse_float_with_default(
            "ALMANAK_DEXSCREENER_DOMINANCE_MULTIPLE",
            gateway.dexscreener_dominance_multiple,
            min_value=0.0,
            min_inclusive=False,
        )


def _apply_polymarket_runtime_fallbacks(gateway: GatewayConfig) -> None:
    """Fill Polymarket runtime fields (cache TTL + network) from env vars.

    ``polymarket_market_cache_ttl_seconds`` uses the legacy
    clamp/fallback helper which enforces ``[0, 24h]`` and rejects NaN /
    ``inf``. ``polymarket_network`` is a string with no validator so the
    bare ``model_fields_set`` guard is sufficient.
    """
    if "polymarket_market_cache_ttl_seconds" not in gateway.model_fields_set:
        gateway.polymarket_market_cache_ttl_seconds = _parse_polymarket_market_cache_ttl_seconds(
            gateway.polymarket_market_cache_ttl_seconds
        )
    if "polymarket_network" not in gateway.model_fields_set:
        polymarket_network = os.environ.get("ALMANAK_POLYMARKET_NETWORK")
        if polymarket_network is not None:
            gateway.polymarket_network = polymarket_network


def _apply_anvil_watchdog_fallback(gateway: GatewayConfig) -> None:
    """Fill ``anvil_watchdog_interval`` from the unprefixed env var.

    Same ``model_fields_set`` + bounds pattern as the DexScreener fields.
    A non-positive value would hot-loop the watchdog, so the lower bound
    is strict ``> 0`` to match ``_validate_positive_float``.
    """
    if "anvil_watchdog_interval" not in gateway.model_fields_set:
        gateway.anvil_watchdog_interval = _parse_float_with_default(
            "ALMANAK_ANVIL_WATCHDOG_INTERVAL",
            gateway.anvil_watchdog_interval,
            min_value=0.0,
            min_inclusive=False,
        )


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
