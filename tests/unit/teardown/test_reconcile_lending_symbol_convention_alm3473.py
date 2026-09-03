"""ALM-3473 defect A — ``_reconcile_lending`` must resolve the cross-asset
``collateral_token`` / ``borrow_token`` detail-key convention, not only the
legacy single-asset ``asset_symbol`` / ``asset`` keys.

Before the fix, a cross-asset lending ``PositionInfo`` (Morpho Blue, and any
strategy following ``generate_lending_unwind()``'s own parameter convention)
resolved to an EMPTY symbol, which ``redrive_lending_position`` then passed to
``market.price("")`` — a doomed lookup that produces "Gateway price request
failed for /USD@base" log noise on every post-teardown reconciliation call
(reproduced live on managed Anvil; see the ALM-3473 investigation notes). The
symbol never affects the CHECK's verdict (``collateral_value_usd`` /
``debt_value_usd`` come from ``position_health``, not the symbol) — this is a
noise/latency fix, not a correctness fix for the verdict itself.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.teardown.plan_a_reconciliation import ReconciliationVerdict, _reconcile_lending

_CHAIN = "base"
_MARKET_ID = "0x13c42741a359ac4a8aa8287d2be109dcf28344484f91185f9a79bd5a805a55ae"


class _Health:
    def __init__(self, collateral_usd: Decimal, debt_usd: Decimal) -> None:
        self.collateral_value_usd = collateral_usd
        self.debt_value_usd = debt_usd
        self.health_factor = None


class _SpyMarket:
    """Records every token ``price()`` is called with, so the test can assert
    the REAL symbol was resolved rather than an empty string."""

    def __init__(self, health: _Health) -> None:
        self._health = health
        self.priced_tokens: list[str] = []

    def position_health(self, protocol: str, market_id: str, **_kw: Any) -> _Health:  # noqa: ARG002
        return self._health

    def price(self, token: str) -> Decimal:
        self.priced_tokens.append(token)
        if not token:
            raise KeyError("empty token symbol")
        return Decimal("1")


def _cross_asset_supply_position(*, closed: bool) -> PositionInfo:
    """The EXACT detail-key shape ALM-3473 reports: collateral_token /
    borrow_token, no asset_symbol / asset key at all."""
    health = Decimal("0") if closed else Decimal("147.49")
    return PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id=_MARKET_ID,
        chain=_CHAIN,
        protocol="morpho_blue",
        value_usd=health,
        details={
            "market_id": _MARKET_ID,
            "collateral_token": "wstETH",
            "borrow_token": "USDC",
        },
    )


def test_cross_asset_supply_resolves_real_symbol_not_empty_string():
    """ALM-3473: the SUPPLY leg's price lookup must use 'wstETH' (from
    details['collateral_token']), never '' — the empty-symbol defect."""
    market = _SpyMarket(_Health(Decimal("0"), Decimal("0")))  # genuinely closed
    verdict, reason = _reconcile_lending(position=_cross_asset_supply_position(closed=True), market=market)

    assert verdict is ReconciliationVerdict.DIVERGED_CLOSED, reason
    assert "" not in market.priced_tokens, (
        f"_reconcile_lending must not call market.price('') — priced_tokens={market.priced_tokens!r}"
    )
    assert "wstETH" in market.priced_tokens


def test_cross_asset_borrow_resolves_real_symbol_not_empty_string():
    """Same convention, BORROW leg: must resolve 'USDC' from
    details['borrow_token'], never the empty string."""
    market = _SpyMarket(_Health(Decimal("0"), Decimal("0")))
    borrow_position = PositionInfo(
        position_type=PositionType.BORROW,
        position_id=_MARKET_ID,
        chain=_CHAIN,
        protocol="morpho_blue",
        value_usd=Decimal("0"),
        details={
            "market_id": _MARKET_ID,
            "collateral_token": "wstETH",
            "borrow_token": "USDC",
        },
    )
    verdict, reason = _reconcile_lending(position=borrow_position, market=market)

    assert verdict is ReconciliationVerdict.DIVERGED_CLOSED, reason
    assert "" not in market.priced_tokens, (
        f"_reconcile_lending must not call market.price('') — priced_tokens={market.priced_tokens!r}"
    )
    assert "USDC" in market.priced_tokens


def test_cross_asset_supply_still_flips_confirmed_open_on_residual():
    """The verdict itself is unaffected by the symbol resolution fix — a
    residual collateral value must still flip CONFIRMED_OPEN, exactly as
    before (the symbol only feeds the discarded amount conversion)."""
    market = _SpyMarket(_Health(Decimal("147.49"), Decimal("0")))  # still open
    verdict, reason = _reconcile_lending(position=_cross_asset_supply_position(closed=False), market=market)

    assert verdict is ReconciliationVerdict.CONFIRMED_OPEN, reason


def test_legacy_asset_key_convention_still_resolves():
    """Backward compatibility: single-asset strategies (Compound V3 / Aave /
    Benqi) using the legacy 'asset' key must be unaffected by this fix."""
    market = _SpyMarket(_Health(Decimal("0"), Decimal("0")))
    legacy_position = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id=_MARKET_ID,
        chain=_CHAIN,
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={"market_id": _MARKET_ID, "asset": "USDC"},
    )
    verdict, reason = _reconcile_lending(position=legacy_position, market=market)

    assert verdict is ReconciliationVerdict.DIVERGED_CLOSED, reason
    assert "USDC" in market.priced_tokens
    assert "" not in market.priced_tokens
