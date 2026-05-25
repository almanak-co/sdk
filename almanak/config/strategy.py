"""Strategy config.json schema (Phase 3, #2098 / #2101).

The framework consumes per-strategy ``config.json`` (or ``config.yaml``) at
runner boot. Phase 3 introduces Pydantic validation around the load: typed
numeric fields that today are stringly-typed (e.g. ``"max_slippage": "0.005"``)
get coerced to ``Decimal`` and validated for sanity.

Schema discipline:

* ``extra="allow"`` — Phase 3 is lenient. The 102 demo configs and the
  per-strategy dataclass extension mechanism (``__orig_bases__`` in
  ``framework/cli/run_helpers.py:1340``) need this until a follow-up PR
  migrates them. Tightening to ``extra="forbid"`` is a separate phase.
* Numeric fields default to ``Decimal`` for monetary precision; Pydantic
  coerces both ``0.5`` and ``"0.5"`` so already-typed and stringly-typed
  configs both validate.
* All fields are optional. The framework's existing fallbacks
  (decorator metadata for ``chain``, derived wallet, generated
  ``deployment_id``) keep working.
"""

from __future__ import annotations

import json
import os
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Hosted-platform env var carrying the user's UI-edited strategy config.
# The V2 agent deployer injects this on the strategy container as JSON;
# ``framework/cli/run.py:load_strategy_config`` deep-merges the parsed dict
# on top of whatever was loaded from disk so UI edits actually reach the
# running strategy (the dashboard reads its own
# ``ALMANAK_DASHBOARD_STRATEGY_CONFIG`` path).
STRATEGY_CONFIG_OVERRIDE_ENV = "ALMANAK_STRATEGY_CONFIG"


class StrategyConfig(BaseModel):
    """Pydantic schema for strategy ``config.json`` files.

    Phase 3 (#2098 / #2101): introduces schema validation around the loader so
    typos and type mismatches surface at boot, not as silent runtime
    degradations later. Lenient on unknown fields — strict tightening is a
    follow-up PR once demo configs and per-strategy dataclass extensions are
    migrated.
    """

    # Chain selection — both optional. The decorator's ``default_chain`` and
    # ``supported_chains`` provide the fallback. ``chain`` and ``chains`` are
    # mutually exclusive (single-chain vs multi-chain strategies).
    chain: str | None = None
    chains: list[str] | None = None
    network: str | None = None  # mainnet | anvil | sepolia | fork

    # Identity — framework auto-fills from wallet derivation / decorator name
    # if absent.
    wallet_address: str | None = None
    deployment_id: str | None = None
    strategy_display_name: str | None = None

    # Pool / market shape (optional; per-strategy interpretation).
    pool: str | None = None
    pool_address: str | None = None
    starting_asset: str | None = None

    # Sizing / risk parameters. ``Decimal`` so Pydantic coerces strings
    # (``"0.005"``) and floats (``0.005``) identically. ``None`` preserves
    # "not specified" semantics.
    total_value_usd: Decimal | None = None
    swap_split_pct: Decimal | None = None
    range_width_pct: Decimal | None = None
    max_slippage: Decimal | None = None

    # Funding / token-resolution maps. ``anvil_funding`` is symbol -> amount
    # (heterogeneous: high-precision tokens use string, normal use numeric).
    # ``token_funding`` shape varies — list of token records is the dominant
    # form across the 102 demo configs; a dict form also exists in some
    # strategies. Phase 3 accepts both as ``Any``-typed because the framework
    # consumes them through downstream resolvers, not directly.
    anvil_funding: dict[str, Decimal | int | float | str] = Field(default_factory=dict)
    token_funding: list[dict[str, Any]] | dict[str, Any] | None = None

    # Integrations (per-strategy shape varies; framework consumes them as dicts).
    copy_trading: dict[str, Any] | None = None
    vault_address: str | None = None
    vault_chain: str | None = None
    vault_safe_address: str | None = None

    model_config = ConfigDict(
        # Phase 3: lenient. Tightened to ``extra="forbid"`` in a follow-up
        # once per-strategy extensions migrate from dataclass to BaseModel.
        extra="allow",
        arbitrary_types_allowed=True,
    )

    @model_validator(mode="after")
    def _exclusive_chain_or_chains(self) -> StrategyConfig:
        """``chain`` and ``chains`` cannot both be set with *meaningful* values.

        Single-chain strategies use ``chain``; multi-chain strategies use
        ``chains``. The framework's chain resolution (``chain_resolution.py``)
        does not define a precedence rule when both are set — disallow it at
        the schema layer.

        Truthiness check (not ``is not None``) is intentional: an empty string
        ``""`` or empty list ``[]`` is the same downstream as omission and
        will fall through to the decorator's ``default_chain`` /
        ``supported_chains``. Tightening to ``is not None`` would reject
        configs emitted by tooling that uses empty placeholders for "no
        value", and the resulting "cannot set both" error message would
        misdescribe the situation when only one field has a real value. We
        only forbid genuine conflicts.
        """
        if self.chain and self.chains:
            raise ValueError(
                "config.json cannot set both 'chain' and 'chains'; pick one. "
                f"Got chain={self.chain!r} and chains={self.chains!r}."
            )
        return self


class StrategyConfigEnvError(ValueError):
    """Raised when ``ALMANAK_STRATEGY_CONFIG`` is set but cannot be parsed.

    Separate from a generic ``ValueError`` so callers can wrap into their own
    UX (``framework/cli/run.py:load_strategy_config`` re-raises as
    ``click.ClickException`` naming the env var) without intercepting unrelated
    value errors that bubble up through ``json.loads`` validation.
    """


def strategy_config_override_from_env() -> dict[str, Any] | None:
    """Return the parsed ``ALMANAK_STRATEGY_CONFIG`` override, or ``None`` if unset.

    Centralises the env read inside ``almanak.config`` so framework code does
    not call ``os.environ`` directly (preserves the config-service boundary
    enforced by ``scripts/ci/check_config_boundary.py``).

    Raises ``StrategyConfigEnvError`` for malformed (non-JSON) or wrong-type
    (non-object) env-var contents; returns ``None`` for unset or
    whitespace-only values. An empty JSON object ``{}`` returns an empty
    dict (the loader treats it as a no-op merge), distinct from ``None``.
    """
    raw = os.environ.get(STRATEGY_CONFIG_OVERRIDE_ENV, "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise StrategyConfigEnvError(f"{STRATEGY_CONFIG_OVERRIDE_ENV} env var is not valid JSON: {e}") from e
    if not isinstance(parsed, dict):
        raise StrategyConfigEnvError(
            f"{STRATEGY_CONFIG_OVERRIDE_ENV} env var must encode a JSON object, got {type(parsed).__name__}."
        )
    return parsed


__all__ = [
    "STRATEGY_CONFIG_OVERRIDE_ENV",
    "StrategyConfig",
    "StrategyConfigEnvError",
    "strategy_config_override_from_env",
]
