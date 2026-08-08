"""`list_token_pools` executor handler (VIB-6599).

Covers the agent-tool half of venue discovery: token resolution across chain
families, the payload contract the CLI and the LLM both read, and the LOUD
failure semantics — unlike `_fetch_pool_analytics`, which is best-effort
garnish and fails open, this handler IS the answer, so a broken lookup must
never be indistinguishable from "no venues".
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from almanak.core.finality import DataFinality
from almanak.framework.agent_tools.errors import AgentErrorCode
from almanak.framework.agent_tools.executor import ToolExecutor
from almanak.framework.agent_tools.policy import AgentPolicy
from almanak.framework.agent_tools.schemas import ToolResponseStatus
from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.models import DataClassification, DataEnvelope, DataMeta
from almanak.framework.data.pools.analytics import TokenPool, TokenPools

_WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
_SOLANA_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


@pytest.fixture
def executor() -> ToolExecutor:
    client = MagicMock()
    client.is_connected = True
    return ToolExecutor(
        client,
        policy=AgentPolicy(allowed_chains={"arbitrum", "solana"}, cooldown_seconds=0),
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        deployment_id="test-strategy",
    )


def _pool(name: str, reserve: Decimal | None, volume: Decimal | None = None) -> TokenPool:
    return TokenPool(
        pool_address="0x" + "1" * 40,
        dex_id="uniswap_v3",
        name=name,
        reserve_usd=reserve,
        volume_24h_usd=volume,
        base_token_address=_WETH,
        quote_token_address="",
    )


def _envelope(pools: tuple[TokenPool, ...], *, complete: bool = True, product_distinct: bool = True):
    return DataEnvelope(
        value=TokenPools(
            chain="arbitrum",
            token_address=_WETH,
            pools=pools,
            source="coingecko_onchain",
            complete=complete,
            product_distinct_dex_id=product_distinct,
        ),
        meta=DataMeta(
            source="coingecko_onchain",
            observed_at=datetime.now(UTC),
            finality=DataFinality.OFF_CHAIN,
            staleness_ms=0,
            latency_ms=0,
            confidence=0.85,
            cache_hit=False,
        ),
        classification=DataClassification.INFORMATIONAL,
    )


def _patched_reader(envelope=None, side_effect=None):
    reader = MagicMock()
    if side_effect is not None:
        reader.list_token_pools.side_effect = side_effect
    else:
        reader.list_token_pools.return_value = envelope
    return patch(
        "almanak.framework.data.pools.analytics.PoolAnalyticsReader",
        return_value=reader,
    ), reader


async def _run(executor: ToolExecutor, args: dict, envelope=None, side_effect=None):
    patcher, reader = _patched_reader(envelope, side_effect)
    with patcher:
        return await executor._execute_list_token_pools(args), reader


@pytest.mark.asyncio
async def test_happy_path_payload_contract(executor: ToolExecutor):
    envelope = _envelope((_pool("deep", Decimal("211219"), Decimal("84")), _pool("shallow", Decimal("10"))))
    response, reader = await _run(executor, {"token": _WETH, "chain": "arbitrum"}, envelope)

    assert response.status == ToolResponseStatus.SUCCESS
    data = response.data
    assert data["count"] == 2
    assert data["unfiltered_count"] == 2
    assert data["source"] == "coingecko_onchain"
    assert data["complete"] is True
    assert data["product_distinct_dex_id"] is True
    assert [p["name"] for p in data["pools"]] == ["deep", "shallow"]
    # Operator discovery opts INTO the fallback: on a gateway with no CoinGecko
    # key the alternative is no answer at all.
    assert reader.list_token_pools.call_args.kwargs["allow_fallback_provider"] is True


@pytest.mark.asyncio
async def test_unmeasured_money_stays_empty_never_zero(executor: ToolExecutor):
    """Empty != Zero has to survive the wire->dataclass->payload round trip: a
    venue that reported no figure must not read as one that measured zero."""
    response, _ = await _run(executor, {"token": _WETH}, _envelope((_pool("unknown", None, None),)))

    pool = response.data["pools"][0]
    assert pool["reserve_usd"] == ""
    assert pool["volume_24h_usd"] == ""


@pytest.mark.asyncio
async def test_floor_filters_but_reports_the_unfiltered_count(executor: ToolExecutor):
    """`unfiltered_count` is what stops a strict floor masquerading as absence."""
    envelope = _envelope((_pool("deep", Decimal("211219")), _pool("shallow", Decimal("10"))))
    response, _ = await _run(executor, {"token": _WETH, "min_liquidity_usd": 1000}, envelope)

    assert response.data["count"] == 1
    assert response.data["unfiltered_count"] == 2
    assert response.data["min_liquidity_usd"] == "1000"


@pytest.mark.asyncio
async def test_evm_address_passes_through_lowercased(executor: ToolExecutor):
    _, reader = await _run(executor, {"token": _WETH.upper().replace("0X", "0x")}, _envelope(()))
    assert reader.list_token_pools.call_args.kwargs["token_address"] == _WETH


@pytest.mark.asyncio
async def test_solana_mint_passes_through_with_case_intact(executor: ToolExecutor):
    """Base58 is case-sensitive — lower-casing a mint yields a DIFFERENT token,
    and routing it through the symbol resolver reports a valid address as
    unresolvable. Solana tail tokens are exactly this tool's audience."""
    _, reader = await _run(executor, {"token": _SOLANA_MINT, "chain": "solana"}, _envelope(()))

    sent = reader.list_token_pools.call_args.kwargs["token_address"]
    assert sent == _SOLANA_MINT
    assert sent != _SOLANA_MINT.lower()


@pytest.mark.asyncio
@pytest.mark.parametrize("token", ["", "   "])
async def test_missing_token_is_a_validation_error(executor: ToolExecutor, token: str):
    response, _ = await _run(executor, {"token": token}, _envelope(()))
    assert response.status == ToolResponseStatus.ERROR
    assert response.error["error_code"] == AgentErrorCode.VALIDATION_ERROR


@pytest.mark.asyncio
async def test_unresolvable_symbol_is_a_validation_error_naming_the_fix(executor: ToolExecutor):
    response, _ = await _run(executor, {"token": "NOT-A-REAL-SYMBOL-XYZ"}, _envelope(()))

    assert response.status == ToolResponseStatus.ERROR
    assert response.error["error_code"] == AgentErrorCode.VALIDATION_ERROR
    assert "contract address" in response.error["message"]


@pytest.mark.asyncio
async def test_provider_outage_fails_loud_not_empty(executor: ToolExecutor):
    """THE fail-open trap: an empty venue list is a real finding, so a broken
    lookup must never be able to produce one. It errors instead."""
    response, _ = await _run(
        executor,
        {"token": _WETH},
        side_effect=DataSourceUnavailable(source="pool_analytics", reason="providers exhausted"),
    )

    assert response.status == ToolResponseStatus.ERROR
    assert response.error["error_code"] == AgentErrorCode.UPSTREAM_UNAVAILABLE
    assert response.data is None or not response.data.get("pools")


@pytest.mark.asyncio
async def test_empty_venue_list_is_a_success_not_an_error(executor: ToolExecutor):
    """The counterpart to the test above — "nothing trades here" is the most
    decision-relevant answer this tool gives and must stay actionable."""
    response, _ = await _run(executor, {"token": _WETH}, _envelope(()))

    assert response.status == ToolResponseStatus.SUCCESS
    assert response.data["pools"] == []
    assert response.data["unfiltered_count"] == 0
    assert response.data["complete"] is True
