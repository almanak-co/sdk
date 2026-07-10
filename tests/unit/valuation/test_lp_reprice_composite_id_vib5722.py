"""VIB-5722 — V3 LP reprice must not silently miss on the framework composite id.

The DN-LP Anvil acceptance run surfaced a residual defect: right after LP_OPEN
the freshly minted Uniswap V3 NFT silently un-repriced → sole open position →
"no_path" → snapshot UNAVAILABLE → legacy strategy fallback stamped $0.00 @ HIGH.

Two causes, both fixed:
  1. `lp_repricer.extract_token_id` could not resolve the framework's OWN composite
     position id `uniswap_v3-WETH/USDC/500-<token_id>` (int() on the string failed
     and it never read the bare id from `details["position_id"]`). Now delegates to
     the shared `resolve_nft_token_id`.
  2. The shared `reprice_lp_position` engine's `price_fn` was bound chain-less, so a
     V3 LP price read on a multi-chain snapshot raised AmbiguousChainError →
     swallowed → miss. `_reprice_lp_on_chain_enriched` now binds the position's chain.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from types import SimpleNamespace

from almanak.framework.market.builders import MarketSnapshotBuilder
from almanak.framework.teardown.models import PositionInfo, PositionType
from almanak.framework.valuation.lp_repricer import extract_token_id, reprice_lp_position
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

# Real arbitrum addresses so resolve_token_symbol resolves via the registry.
WETH_ARB = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_ARB = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
TOKEN_ID = 5580763


def _dnlp_position() -> PositionInfo:
    """The exact shape the DN-LP strategy's get_open_positions() reports."""
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=f"uniswap_v3-WETH/USDC/500-{TOKEN_ID}",  # composite framework id
        chain="arbitrum",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={"pool": "WETH/USDC/500", "position_id": str(TOKEN_ID)},  # symbolic pool, bare id
    )


def _on_chain_read():
    return SimpleNamespace(
        token0=WETH_ARB,
        token1=USDC_ARB,
        tick_lower=-887000,
        tick_upper=887000,  # full range → always in range, value > 0
        liquidity=10**18,
        tokens_owed0=0,
        tokens_owed1=0,
        tick_spacing=None,
    )


class _Reader:
    def __init__(self, on_chain):
        self._on_chain = on_chain
        self.seen_token_id = None

    def read_position(self, chain, token_id, protocol):
        self.seen_token_id = token_id
        return self._on_chain

    def read_pool_slot0(self, chain, pool_address):  # not reached (symbolic pool)
        return None


def _multichain_market(prices: dict[tuple[str, str], Decimal]):
    def _oracle(token, quote="USD", chain=None):
        key = (chain or "", (token or "").upper())
        if key in prices:
            return prices[key]
        raise ValueError(f"no price for {token} on {chain}")

    return MarketSnapshotBuilder.for_strategy_runner(
        strategy=SimpleNamespace(chain="arbitrum", wallet_address="0x" + "1" * 40),
        chain="arbitrum",
        chains=("arbitrum", "hyperevm"),
        multi_chain_price_oracle=_oracle,
        runtime_surface="unit_test",
    )


# ---------------------------------------------------------------------------
# extract_token_id — the shape resolution
# ---------------------------------------------------------------------------


def test_extract_token_id_resolves_framework_composite_id():
    assert extract_token_id(_dnlp_position()) == TOKEN_ID


def test_extract_token_id_bare_numeric_still_resolves():
    pos = PositionInfo(
        position_type=PositionType.LP,
        position_id=str(TOKEN_ID),
        chain="arbitrum",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={},
    )
    assert extract_token_id(pos) == TOKEN_ID


def test_extract_token_id_composite_without_details_is_none():
    pos = PositionInfo(
        position_type=PositionType.LP,
        position_id="uniswap_v3-WETH/USDC/500",  # no trailing id, none in details
        chain="arbitrum",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={"pool": "WETH/USDC/500"},
    )
    assert extract_token_id(pos) is None


# ---------------------------------------------------------------------------
# Full reprice path on a REAL multi-chain snapshot (both fixes together)
# ---------------------------------------------------------------------------


def test_dnlp_composite_reprices_on_multichain_snapshot(monkeypatch):
    valuer = PortfolioValuer()
    reader = _Reader(_on_chain_read())
    valuer._lp_reader = reader  # type: ignore[attr-defined]
    monkeypatch.setattr(valuer, "_get_token_decimals", lambda sym, chain: 18 if sym.upper() == "WETH" else 6)

    market = _multichain_market({("arbitrum", "WETH"): Decimal("2000"), ("arbitrum", "USDC"): Decimal("1")})

    result = valuer._reprice_lp_on_chain_enriched(_dnlp_position(), "arbitrum", market)

    assert result is not None, "DN-LP composite id must reprice (not silently miss)"
    value_usd, enriched = result
    assert value_usd > 0
    assert reader.seen_token_id == TOKEN_ID  # id resolved from the composite/details
    assert enriched["token0_symbol"] == "WETH"
    assert enriched["token1_symbol"] == "USDC"


def test_bare_numeric_id_still_reprices_single_chain(monkeypatch):
    # Existing single-chain behaviour preserved.
    valuer = PortfolioValuer()
    reader = _Reader(_on_chain_read())
    valuer._lp_reader = reader  # type: ignore[attr-defined]
    monkeypatch.setattr(valuer, "_get_token_decimals", lambda sym, chain: 18 if sym.upper() == "WETH" else 6)

    market = MarketSnapshotBuilder.seeded(
        chain="arbitrum",
        prices={"WETH": Decimal("2000"), "USDC": Decimal("1")},
    )
    pos = PositionInfo(
        position_type=PositionType.LP,
        position_id=str(TOKEN_ID),
        chain="arbitrum",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={},
    )
    result = valuer._reprice_lp_on_chain_enriched(pos, "arbitrum", market)
    assert result is not None
    assert result[0] > 0
    assert reader.seen_token_id == TOKEN_ID


def test_composite_without_id_logs_miss_reason(caplog):
    # A composite id with NO resolvable token id must log WHY it missed (so the
    # next silent miss is visible in a run log), then return None.
    reader = _Reader(_on_chain_read())
    pos = PositionInfo(
        position_type=PositionType.LP,
        position_id="uniswap_v3-WETH/USDC/500",
        chain="arbitrum",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={"pool": "WETH/USDC/500"},
    )
    # Captured at WARNING to prove the miss is visible under the default logger
    # (VIB-5722 review — a per-snapshot mis-valuation must not hide at DEBUG).
    with caplog.at_level(logging.WARNING, logger="almanak.framework.valuation.lp_repricer"):
        result = reprice_lp_position(reader, pos, "arbitrum", lambda s: Decimal("1"), lambda s, c: 18)
    assert result is None
    assert any(
        "could not resolve NFT token id" in r.message and r.levelno == logging.WARNING for r in caplog.records
    )
