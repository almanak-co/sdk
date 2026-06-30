"""Market-condition / scenario injection for ``almanak strat test`` (VIB-5529).

The ``strat test`` harness drives ``decide()`` by mutating
``strategy.force_action`` between iterations. That short-circuits ``decide()``
*before* any market-condition branch runs (see
``almanak/demo_strategies/curve_3pool_lp/strategy.py`` STEP 1), so the
indicator / price / depeg / drawdown trigger logic a strategy actually ships is
never exercised by a force-action test.

This module is the missing seam: it parses a small, typed override document
(``--inject``) and applies it to the ``MarketSnapshot`` the runner feeds into
``decide()`` — using only the **already-public** ``MarketSnapshot`` setters
(``set_price`` / ``set_balance`` / ``set_rsi`` …). Nothing here changes the
``MarketSnapshot`` or ``decide()`` production contract; it is a thin,
additive, test-only applier that runs after the runner builds the snapshot.

Design (blueprint 01-data-layer.md §"MarketSnapshot", §"Testing Approach"):

* The snapshot's read methods (``price`` / ``balance`` / ``rsi``) consult their
  pre-populated caches *before* hitting any oracle/provider, so a seeded value
  deterministically wins. The setters used here are the same ones the data-layer
  blueprint documents for tests and the same ones the dry-run
  ``inject_simulated_balances`` path uses.
* ``decide()`` therefore reads the injected condition through the normal API and
  takes its real condition branch — the bug-class this feature exists to catch.

Schema (inline JSON or a path to a ``.json`` file)::

    {
      "prices":   {"USDC": "0.95", "WETH": "2400"},
      "balances": {"USDC": "10000", "WETH": "5"},
      "indicators": {"rsi": {"WETH": 25}}
    }

Condition mapping (no synthetic snapshot methods are invented — depeg and
drawdown are *derived* conditions strategies compute from these primitives):

* **indicator** — ``indicators.rsi`` seeds ``market.rsi(token)``. The seed is
  honoured only for the **default RSI period (14)**: ``MarketSnapshot.rsi``
  returns a pre-populated value only when the requested ``period`` matches the
  stored ``RSIData.period`` (``snapshot.py``), and the applier stores the
  framework default (``RSIData.period == 14``). A strategy that calls
  ``market.rsi(token, period != 14)`` falls back to the provider. The injected
  RSI matches **any** timeframe (stored with ``timeframe=None``).
* **price** — ``prices`` seeds ``market.price(token)``.
* **depeg** — a depeg *is* an off-peg price: put the stablecoin in ``prices``
  away from ``1.0`` (e.g. ``"USDC": "0.95"``).
* **drawdown** — a drawdown is a fall in portfolio value: lower an asset's
  ``prices`` and/or ``balances`` so ``market.balance_usd`` /
  ``total_portfolio_usd`` drop below the strategy's threshold.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class ScenarioParseError(ValueError):
    """Raised when an ``--inject`` document is malformed.

    Subclasses ``ValueError`` so existing CLI error handling (which renders
    ``ValueError`` as a clean message) keeps working.
    """


@dataclass(frozen=True)
class ScenarioOverrides:
    """Typed, validated market-condition overrides for a single test run.

    All values are normalised to ``Decimal`` at parse time so the applier never
    has to re-validate. An empty instance (no keys set) is a no-op applier.
    """

    prices: dict[str, Decimal] = field(default_factory=dict)
    balances: dict[str, Decimal] = field(default_factory=dict)
    rsi: dict[str, Decimal] = field(default_factory=dict)

    def is_empty(self) -> bool:
        return not (self.prices or self.balances or self.rsi)


def _to_decimal(label: str, raw: Any) -> Decimal:
    """Coerce a JSON scalar to a finite ``Decimal`` or raise ScenarioParseError."""
    if isinstance(raw, bool):
        # bool is an int subclass — reject explicitly; a price of ``true`` is a typo.
        raise ScenarioParseError(f"{label}: expected a number, got boolean {raw!r}")
    try:
        value = Decimal(str(raw))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ScenarioParseError(f"{label}: not a valid number: {raw!r}") from exc
    if not value.is_finite():
        raise ScenarioParseError(f"{label}: must be finite, got {raw!r}")
    return value


def _parse_token_map(label: str, raw: Any) -> dict[str, Decimal]:
    if not isinstance(raw, dict):
        raise ScenarioParseError(f"{label}: expected an object of {{token: number}}, got {type(raw).__name__}")
    out: dict[str, Decimal] = {}
    for token, amount in raw.items():
        if not isinstance(token, str) or not token.strip():
            raise ScenarioParseError(f"{label}: token keys must be non-empty strings, got {token!r}")
        out[token.strip()] = _to_decimal(f"{label}.{token}", amount)
    return out


def parse_scenario(raw: str) -> ScenarioOverrides:
    """Parse an ``--inject`` argument into validated :class:`ScenarioOverrides`.

    ``raw`` is either inline JSON (starts with ``{``) or a path to a ``.json``
    file. Unknown top-level keys are rejected so a typo (``"price"`` vs
    ``"prices"``) fails loudly instead of silently injecting nothing.

    Raises:
        ScenarioParseError: on any malformed / unreadable / unknown-key input.
    """
    text = raw.strip()
    if not text:
        raise ScenarioParseError("--inject was empty")

    # Path vs inline JSON: anything that isn't obviously a JSON object is treated
    # as a path. This keeps the common `--inject scenario.json` ergonomic while
    # still allowing `--inject '{"prices": ...}'`.
    if not text.startswith("{"):
        path = Path(text).expanduser()
        if not path.is_file():
            raise ScenarioParseError(
                f"--inject: '{text}' is neither inline JSON (must start with '{{') nor a readable file"
            )
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ScenarioParseError(f"--inject: could not read file '{path}': {exc}") from exc

    try:
        doc = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ScenarioParseError(f"--inject: invalid JSON: {exc}") from exc

    if not isinstance(doc, dict):
        raise ScenarioParseError(f"--inject: top-level value must be a JSON object, got {type(doc).__name__}")

    allowed = {"prices", "balances", "indicators"}
    unknown = set(doc) - allowed
    if unknown:
        raise ScenarioParseError(f"--inject: unknown key(s) {sorted(unknown)}; supported keys are {sorted(allowed)}")

    prices = _parse_token_map("prices", doc["prices"]) if "prices" in doc else {}
    balances = _parse_token_map("balances", doc["balances"]) if "balances" in doc else {}

    rsi: dict[str, Decimal] = {}
    if "indicators" in doc:
        indicators = doc["indicators"]
        if not isinstance(indicators, dict):
            raise ScenarioParseError(f"indicators: expected an object, got {type(indicators).__name__}")
        unknown_ind = set(indicators) - {"rsi"}
        if unknown_ind:
            raise ScenarioParseError(f"indicators: unknown indicator(s) {sorted(unknown_ind)}; only 'rsi' is supported")
        if "rsi" in indicators:
            rsi = _parse_token_map("indicators.rsi", indicators["rsi"])
            for token, value in rsi.items():
                if not (Decimal("0") <= value <= Decimal("100")):
                    raise ScenarioParseError(f"indicators.rsi.{token}: RSI must be within 0..100, got {value}")

    overrides = ScenarioOverrides(prices=prices, balances=balances, rsi=rsi)
    if overrides.is_empty():
        raise ScenarioParseError("--inject: document contained no overrides (prices/balances/indicators all empty)")
    return overrides


def apply_scenario(market: Any, overrides: ScenarioOverrides) -> list[str]:
    """Apply ``overrides`` to a freshly-built ``MarketSnapshot``.

    Uses only the public ``MarketSnapshot`` setters. Prices are applied before
    balances so the per-balance USD valuation reflects the overridden price.
    Returns a list of human-readable descriptions of what was applied (for the
    harness log), never raises for a recoverable per-token issue.
    """
    from almanak.framework.market.models import RSIData, TokenBalance

    applied: list[str] = []

    for token, price in overrides.prices.items():
        market.set_price(token, price)
        applied.append(f"price[{token}]={price}")

    for token, amount in overrides.balances.items():
        balance_usd = Decimal("0")
        try:
            price = Decimal(str(market.price(token)))
            balance_usd = amount * price
        except Exception:  # noqa: BLE001 — best-effort USD valuation, mirrors inject_simulated_balances
            logger.debug("scenario inject: no price for %s; balance_usd defaults to 0", token)
        market.set_balance(token, TokenBalance(symbol=token, balance=amount, balance_usd=balance_usd))
        applied.append(f"balance[{token}]={amount} (${balance_usd})")

    for token, value in overrides.rsi.items():
        # Stored with the framework-default period (14) and timeframe=None
        # (matches any). `market.rsi(token, period != 14)` will NOT see this
        # seed — see the module docstring "indicator" note.
        market.set_rsi(token, RSIData(value=value))
        applied.append(f"rsi[{token}]={value}")

    return applied
