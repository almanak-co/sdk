"""VIB-5985: Morpho market discoverability through ``list_lending_reserves``.

The incident behind this ticket: an AlmanakCode-generated strategy shipped
``morpho_market_id: ""`` because no sanctioned surface made the market id
findable. The market WAS in the connector's curated catalog all along — but
the tool required the exact pair string (``--asset "sUSDe/USDC"``), buried
the market id in ``--json`` detail, and presented the curated catalog as if
it were a complete live enumeration.

Covers:

1. The pair-aware filter (``asset`` matches either leg; ``collateral`` /
   ``loan`` pin a leg; reserve-keyed symbols unchanged).
2. Executor behaviour for morpho_blue (static curated plan, zero RPC):
   single-leg filtering, market_id in row detail, LLTV as ltv_bps, and the
   ``enumeration_source="curated_catalog"`` honesty flag.
3. The ``--collateral``/``--loan`` guard on protocols that are not
   pair-keyed (live-enumerated Aave), and ``enumeration_source="live"``.
4. The human table renderer surfacing per-row market ids and the
   curated-catalog note.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.agent_tools.executor import ToolExecutor, _reserve_matches_filters
from almanak.framework.agent_tools.policy import AgentPolicy

# The sUSDe/USDC market that the incident strategy needed (curated in
# almanak/connectors/morpho_blue/addresses.py; LLTV 91.5% -> 9150 bps).
SUSDE_USDC_MARKET_ID = "0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab"


# ---------------------------------------------------------------------------
# 1. Pure filter semantics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("symbol", "asset", "collateral", "loan", "expected"),
    [
        # asset: full-symbol match (reserve-keyed and pair-keyed), case-insensitive.
        ("WMATIC", "wmatic", "", "", True),
        ("WMATIC", "ETH", "", "", False),
        ("sUSDe/USDC", "sUSDe/USDC", "", "", True),
        # asset: single-leg match on pair-keyed symbols — the VIB-5985 fix.
        ("sUSDe/USDC", "sUSDe", "", "", True),
        ("sUSDe/USDC", "usdc", "", "", True),
        ("sUSDe/USDC", "WETH", "", "", False),
        # collateral pins the FIRST leg only.
        ("sUSDe/USDC", "", "sUSDe", "", True),
        ("sUSDe/USDC", "", "USDC", "", False),
        # loan pins the LAST leg only.
        ("sUSDe/USDC", "", "", "USDC", True),
        ("sUSDe/USDC", "", "", "sUSDe", False),
        # combined collateral + loan.
        ("sUSDe/USDC", "", "sUSDe", "USDC", True),
        # leg filters never match bare (non-pair) symbols.
        ("WETH", "", "WETH", "", False),
        ("WETH", "", "", "WETH", False),
        # empty filters pass everything.
        ("anything", "", "", "", True),
    ],
)
def test_reserve_matches_filters(symbol: str, asset: str, collateral: str, loan: str, expected: bool) -> None:
    assert _reserve_matches_filters(symbol, asset, collateral, loan) is expected


# ---------------------------------------------------------------------------
# 2. Executor: morpho_blue static curated plan (no RPC involved)
# ---------------------------------------------------------------------------


def _make_executor() -> ToolExecutor:
    policy = AgentPolicy(
        allowed_protocols={"aave_v3", "morpho_blue"},
        allowed_chains=None,
        max_tool_calls_per_minute=1000,
    )
    return ToolExecutor(
        MagicMock(),  # scripted gateway; the static Morpho plan issues zero RPC
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        deployment_id="test-vib5985",
        default_chain="ethereum",
    )


@pytest.mark.asyncio
async def test_morpho_full_list_carries_market_id_and_honesty_flag() -> None:
    executor = _make_executor()
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "ethereum", "protocol": "morpho_blue"},
    )
    assert result.status == "success", result.error
    data = result.data
    assert data["enumeration_source"] == "curated_catalog"
    assert data["count"] >= 1
    for row in data["reserves"]:
        # Every curated Morpho market row must expose its immutable identity
        # and a real LLTV — the two values a strategy config pins.
        assert (row.get("detail") or {}).get("market_id", "").startswith("0x")
        assert row["ltv_bps"] and row["ltv_bps"] > 0


@pytest.mark.asyncio
async def test_morpho_single_leg_asset_filter_finds_susde_markets() -> None:
    """`--asset sUSDe` must work; pre-VIB-5985 only the exact pair string did."""
    executor = _make_executor()
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "ethereum", "protocol": "morpho_blue", "asset": "sUSDe"},
    )
    assert result.status == "success", result.error
    reserves = result.data["reserves"]
    assert reserves, "expected at least one sUSDe market in the curated catalog"
    for row in reserves:
        assert "susde" in [part.lower() for part in row["symbol"].split("/")]


@pytest.mark.asyncio
async def test_morpho_collateral_loan_filter_pins_incident_market() -> None:
    executor = _make_executor()
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "ethereum", "protocol": "morpho_blue", "collateral": "sUSDe", "loan": "USDC"},
    )
    assert result.status == "success", result.error
    market_ids = {(row.get("detail") or {}).get("market_id") for row in result.data["reserves"]}
    assert SUSDE_USDC_MARKET_ID in market_ids


@pytest.mark.asyncio
async def test_morpho_unknown_filter_lists_known_markets() -> None:
    executor = _make_executor()
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "ethereum", "protocol": "morpho_blue", "asset": "DOGE"},
    )
    assert result.status == "error"
    assert "not a listed reserve" in result.error["message"]
    assert "sUSDe/USDC" in result.error["message"]


# ---------------------------------------------------------------------------
# 3. Executor: leg filters on a live-enumerated (non-pair-keyed) protocol
# ---------------------------------------------------------------------------


def _aave_gateway(tokens: list[tuple[str, str]]) -> Any:
    """Scripted gateway answering getAllReservesTokens() + any config read."""
    from eth_abi import encode as abi_encode

    def _ok(result_hex: str) -> Any:
        resp = MagicMock()
        resp.success = True
        resp.result = json.dumps("0x" + result_hex)
        resp.error = ""
        return resp

    def _call(req: Any, **kwargs: Any) -> Any:
        if req.id == "aave_all_reserves":
            return _ok(abi_encode(["(string,address)[]"], [tokens]).hex())
        # 10-word getReserveConfigurationData: any plausible payload works —
        # these tests only exercise the filter path, not config decoding.
        return _ok("".join(f"{v:064x}" for v in (6, 6800, 8250, 10500, 1000, 1, 1, 0, 1, 0)))

    gateway = MagicMock()
    gateway.rpc.Call.side_effect = _call
    return gateway


def _make_aave_executor(tokens: list[tuple[str, str]]) -> ToolExecutor:
    policy = AgentPolicy(
        allowed_protocols={"aave_v3", "morpho_blue"},
        allowed_chains=None,
        max_tool_calls_per_minute=1000,
    )
    return ToolExecutor(
        _aave_gateway(tokens),
        policy=policy,
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        deployment_id="test-vib5985",
        default_chain="polygon",
    )


_WMATIC = "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270"
_USDC = "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359"


@pytest.mark.asyncio
async def test_leg_filters_rejected_on_reserve_keyed_protocol() -> None:
    executor = _make_aave_executor([("WMATIC", _WMATIC), ("USDC", _USDC)])
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "aave_v3", "collateral": "WMATIC"},
    )
    assert result.status == "error"
    assert "not pair-keyed" in result.error["message"]


@pytest.mark.asyncio
async def test_live_enumeration_flags_source_live() -> None:
    executor = _make_aave_executor([("WMATIC", _WMATIC), ("USDC", _USDC)])
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "aave_v3"},
    )
    assert result.status == "success", result.error
    assert result.data["enumeration_source"] == "live"


@pytest.mark.asyncio
async def test_exact_symbol_filter_still_works_on_aave() -> None:
    """The pre-existing exact-match contract must survive the pair-aware filter."""
    executor = _make_aave_executor([("WMATIC", _WMATIC), ("USDC", _USDC)])
    result = await executor.execute(
        "list_lending_reserves",
        {"chain": "polygon", "protocol": "aave_v3", "asset": "wmatic"},
    )
    assert result.status == "success", result.error
    assert result.data["count"] == 1
    assert result.data["reserves"][0]["symbol"] == "WMATIC"


# ---------------------------------------------------------------------------
# 4. Human table renderer: market ids + curated-catalog note
# ---------------------------------------------------------------------------


def test_table_renders_market_id_and_curated_note(capsys: pytest.CaptureFixture[str]) -> None:
    from almanak.framework.cli.ax import _render_reserves_table

    response = SimpleNamespace(
        data={
            "count": 1,
            "pool_data_provider": "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb",
            "enumeration_source": "curated_catalog",
            "reserves": [
                {
                    "symbol": "sUSDe/USDC",
                    "address": "0x" + "aa" * 20,
                    "borrowing_enabled": True,
                    "usage_as_collateral_enabled": True,
                    "is_active": True,
                    "is_frozen": False,
                    "ltv_bps": 9150,
                    "liquidation_threshold_bps": 9150,
                    "detail": {"market_id": SUSDE_USDC_MARKET_ID},
                    "error": "",
                }
            ],
        }
    )
    _render_reserves_table(response, protocol="morpho_blue", chain="ethereum")
    out = capsys.readouterr().out
    assert f"market_id={SUSDE_USDC_MARKET_ID}" in out
    assert "curated catalog" in out
    assert "91.5%" in out


def test_table_omits_note_and_ids_for_live_reserves(capsys: pytest.CaptureFixture[str]) -> None:
    from almanak.framework.cli.ax import _render_reserves_table

    response = SimpleNamespace(
        data={
            "count": 1,
            "pool_data_provider": "0x" + "bb" * 20,
            "enumeration_source": "live",
            "reserves": [
                {
                    "symbol": "WMATIC",
                    "address": "0x" + "cc" * 20,
                    "borrowing_enabled": False,
                    "usage_as_collateral_enabled": True,
                    "is_active": True,
                    "is_frozen": False,
                    "ltv_bps": 6800,
                    "liquidation_threshold_bps": 8250,
                    "error": "",
                }
            ],
        }
    )
    _render_reserves_table(response, protocol="aave_v3", chain="polygon")
    out = capsys.readouterr().out
    assert "market_id=" not in out
    assert "curated catalog" not in out
