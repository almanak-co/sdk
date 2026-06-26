"""VIB-5463 / TD-05 — live per-KNOWN-position chain re-derivation for teardown.

Pins the two capabilities that let teardown honour blueprint 14:811 on a
wiped / ``--fresh`` / corrupt-WARM restart:

* ``redrive_lending_position`` re-derives a config-known lending market's live
  collateral / debt / HF from chain (generalises the ``morpho_looping`` pattern),
  and fails CLOSED to ``None`` (caller fall-back) when the read is unavailable —
  never a fabricated zero (Empty ≠ Zero).
* ``chain_verify_lp_open`` verifies a single KNOWN LP NFT's open-ness on-chain
  (per-position, never a wallet scan), returning True / False / None.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.teardown.live_position_reads import (
    LiveLendingPosition,
    chain_verify_lp_open,
    redrive_lending_position,
)
from almanak.framework.teardown.models import PositionInfo, PositionType


class _Health:
    def __init__(self, collateral_value_usd, debt_value_usd, health_factor):
        self.collateral_value_usd = collateral_value_usd
        self.debt_value_usd = debt_value_usd
        self.health_factor = health_factor


class _FakeMarket:
    """MarketSnapshot double exposing only ``position_health`` + ``price``."""

    def __init__(self, *, health=None, raise_health=False, prices=None):
        self._health = health
        self._raise_health = raise_health
        self._prices = prices or {}

    def position_health(self, protocol, market_id, *, collateral_price_usd=None, debt_price_usd=None):
        if self._raise_health:
            raise RuntimeError("gateway down")
        return self._health

    def price(self, token):
        if token not in self._prices:
            raise KeyError(token)
        return self._prices[token]


# ---------------------------------------------------------------------------
# redrive_lending_position
# ---------------------------------------------------------------------------


def test_redrive_returns_live_values_and_token_amounts() -> None:
    market = _FakeMarket(
        health=_Health(Decimal("3400"), Decimal("1700"), Decimal("1.72")),
        prices={"wstETH": Decimal("3400"), "USDC": Decimal("1")},
    )
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
    )
    assert live is not None
    assert live.collateral_value_usd == Decimal("3400")
    assert live.debt_value_usd == Decimal("1700")
    assert live.health_factor == Decimal("1.72")
    assert live.collateral_amount == Decimal("1")  # 3400 / 3400
    assert live.debt_amount == Decimal("1700")  # 1700 / 1
    assert live.has_exposure() is True


def test_redrive_unavailable_read_returns_none_not_zero() -> None:
    # position_health raising ⇒ UNMEASURED ⇒ None (caller fall-backs to cache).
    market = _FakeMarket(raise_health=True, prices={"wstETH": Decimal("3400"), "USDC": Decimal("1")})
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
    )
    assert live is None  # Empty != Zero — never fabricate a closed position


def test_redrive_none_health_is_unavailable_not_closed() -> None:
    # A provider/mock returning None (rather than raising) must be treated as
    # UNAVAILABLE, never as a measured-zero closed market — else a live position
    # would be silently stranded (Gemini review).
    market = _FakeMarket(health=None, prices={"wstETH": Decimal("3400"), "USDC": Decimal("1")})
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
    )
    assert live is None


def test_redrive_measured_zero_is_closed_position() -> None:
    # A clean read of an all-zero market is a genuinely CLOSED position (not None).
    market = _FakeMarket(
        health=_Health(Decimal("0"), Decimal("0"), None),
        prices={"wstETH": Decimal("3400"), "USDC": Decimal("1")},
    )
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
    )
    assert live is not None
    assert live.has_exposure() is False


def test_redrive_missing_price_leaves_amount_none() -> None:
    # Collateral priced, debt token not ⇒ debt_amount None (unmeasured), not 0.
    market = _FakeMarket(
        health=_Health(Decimal("3400"), Decimal("1700"), Decimal("1.7")),
        prices={"wstETH": Decimal("3400")},
    )
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
    )
    assert live is not None
    assert live.collateral_amount == Decimal("1")
    assert live.debt_amount is None


def test_redrive_price_override_takes_precedence() -> None:
    market = _FakeMarket(
        health=_Health(Decimal("3400"), Decimal("0"), None),
        prices={"wstETH": Decimal("9999")},  # snapshot price would be wrong
    )
    live = redrive_lending_position(
        market=market,
        protocol="morpho_blue",
        market_id="0xMARKET",
        collateral_token="wstETH",
        borrow_token="USDC",
        collateral_price_usd=Decimal("3400"),
    )
    assert live is not None
    assert live.collateral_amount == Decimal("1")  # uses the override, not 9999


# ---------------------------------------------------------------------------
# chain_verify_lp_open
# ---------------------------------------------------------------------------


def _lp(position_id: str = "555", chain: str = "arbitrum") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain=chain,
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
    )


class _Discovered:
    def __init__(self, liquidity: int) -> None:
        self.liquidity = liquidity


@pytest.mark.asyncio
async def test_chain_verify_none_without_gateway() -> None:
    assert await chain_verify_lp_open(gateway_client=None, position=_lp()) is None


@pytest.mark.asyncio
async def test_chain_verify_none_for_non_int_token_id() -> None:
    client = object()
    assert await chain_verify_lp_open(gateway_client=client, position=_lp("pool0xABC:555")) is None


@pytest.mark.asyncio
async def test_chain_verify_open_when_liquidity_positive(monkeypatch) -> None:
    import almanak.framework.teardown.discovery as discovery

    monkeypatch.setattr(discovery, "_npms_for_chain", lambda chain: [("uniswap_v3", "0xNPM")])

    async def _read(client, chain, npm, token_id, network="", protocol="uniswap_v3"):
        return _Discovered(liquidity=12345)

    monkeypatch.setattr(discovery, "_read_position", _read)
    assert await chain_verify_lp_open(gateway_client=object(), position=_lp("555")) is True


@pytest.mark.asyncio
async def test_chain_verify_closed_when_liquidity_zero(monkeypatch) -> None:
    import almanak.framework.teardown.discovery as discovery

    monkeypatch.setattr(discovery, "_npms_for_chain", lambda chain: [("uniswap_v3", "0xNPM")])

    async def _read(client, chain, npm, token_id, network="", protocol="uniswap_v3"):
        return _Discovered(liquidity=0)

    monkeypatch.setattr(discovery, "_read_position", _read)
    assert await chain_verify_lp_open(gateway_client=object(), position=_lp("555")) is False


@pytest.mark.asyncio
async def test_chain_verify_none_when_not_found_on_any_npm(monkeypatch) -> None:
    import almanak.framework.teardown.discovery as discovery

    monkeypatch.setattr(discovery, "_npms_for_chain", lambda chain: [("uniswap_v3", "0xNPM")])

    async def _read(client, chain, npm, token_id, network="", protocol="uniswap_v3"):
        return None  # e.g. a UniV4 lp_v4 token id — not on a V3 NPM

    monkeypatch.setattr(discovery, "_read_position", _read)
    assert await chain_verify_lp_open(gateway_client=object(), position=_lp("555")) is None


@pytest.mark.asyncio
async def test_chain_verify_none_when_no_npm_on_chain(monkeypatch) -> None:
    import almanak.framework.teardown.discovery as discovery

    monkeypatch.setattr(discovery, "_npms_for_chain", lambda chain: [])
    assert await chain_verify_lp_open(gateway_client=object(), position=_lp("555", chain="zzz")) is None


def test_live_lending_position_dust_threshold() -> None:
    p = LiveLendingPosition(
        collateral_value_usd=Decimal("0.005"),
        debt_value_usd=Decimal("0"),
        health_factor=None,
        collateral_amount=None,
        debt_amount=None,
    )
    assert p.has_exposure(dust_usd=Decimal("0.01")) is False
    assert p.has_exposure(dust_usd=Decimal("0.001")) is True
