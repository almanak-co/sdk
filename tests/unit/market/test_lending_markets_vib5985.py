"""MarketSnapshot.lending_markets / lending_market accessors (VIB-5985).

Mirrors the ``pt_price`` gateway-stub pattern: a ``MagicMock`` client whose
``client.market.<RPC>`` returns a real proto. Asserts the typed return, the
outbound request proto, verified/candidate provenance, pagination collection,
and fail-closed behaviour (no client, RPC error, success=false).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.market.errors import LendingMarketResolutionError
from almanak.framework.market.models import LendingMarketInfo
from almanak.framework.market.snapshot import MarketSnapshot
from almanak.gateway.proto import gateway_pb2

_ID = "0x85c7f4374f3a403b36d54cc284983b2b02bbd8581ee0f3c36494447b87d9fcab"


def _market(**kw) -> gateway_pb2.LendingMarket:
    base = dict(
        kind=gateway_pb2.LENDING_MARKET_KIND_ISOLATED_PAIR,
        protocol="morpho_blue",
        chain="ethereum",
        market_id=_ID,
        collateral_token="0x9d39a5de30e57443bff2a8307a4256c8797a3497",
        collateral_symbol="sUSDe",
        loan_token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        loan_symbol="USDC",
        lltv_bps=9150,
        oracle="0x873cd44b860dedfe139f93e12a4acca0926ffb87",
        irm="0x870ac11d48b15db9a138cf899d20f13f79ba00bc",
    )
    base.update(kw)
    return gateway_pb2.LendingMarket(**base)


def _snapshot(client: MagicMock) -> MarketSnapshot:
    client.is_connected = True
    client.config.timeout = 30
    return MarketSnapshot(chain="ethereum", gateway_client=client)


# ---------------------------------------------------------------------------
# lending_markets
# ---------------------------------------------------------------------------


def test_lending_markets_maps_candidate_and_sends_request():
    client = MagicMock()
    client.market.ListLendingMarkets.return_value = gateway_pb2.ListLendingMarketsResponse(
        markets=[_market(source=gateway_pb2.LENDING_MARKET_SOURCE_CURATED_CATALOG, verified=False)],
        next_page_token="",
        total_matches=1,
        success=True,
    )
    snap = _snapshot(client)
    result = snap.lending_markets("morpho_blue", collateral="sUSDe", loan="USDC", lltv_bps=9150)

    assert len(result) == 1
    m = result[0]
    assert isinstance(m, LendingMarketInfo)
    assert m.market_id == _ID
    assert m.kind == "isolated_pair"
    assert m.verified is False
    assert m.source == "curated_catalog"
    assert m.lltv_bps == 9150
    assert m.lltv_percent == Decimal("91.5")

    sent = client.market.ListLendingMarkets.call_args.args[0]
    assert sent.protocol == "morpho_blue"
    assert sent.chain == "ethereum"
    assert sent.collateral_token == "sUSDe"
    assert sent.loan_token == "USDC"
    assert sent.lltv_bps == 9150
    # Deadline forwarded.
    assert client.market.ListLendingMarkets.call_args.kwargs.get("timeout") == 30


def test_lending_markets_collects_all_pages():
    client = MagicMock()
    page1 = gateway_pb2.ListLendingMarketsResponse(
        markets=[_market(market_id="0xaaa")], next_page_token="2", total_matches=2, success=True
    )
    page2 = gateway_pb2.ListLendingMarketsResponse(
        markets=[_market(market_id="0xbbb")], next_page_token="", total_matches=2, success=True
    )
    client.market.ListLendingMarkets.side_effect = [page1, page2]
    snap = _snapshot(client)
    result = snap.lending_markets("morpho_blue")
    assert [m.market_id for m in result] == ["0xaaa", "0xbbb"]
    assert client.market.ListLendingMarkets.call_count == 2
    # Second call carried the continuation token from page 1.
    second = client.market.ListLendingMarkets.call_args_list[1].args[0]
    assert second.page_token == "2"


def test_lending_markets_empty_is_not_error():
    client = MagicMock()
    client.market.ListLendingMarkets.return_value = gateway_pb2.ListLendingMarketsResponse(
        markets=[], next_page_token="", total_matches=0, success=True
    )
    snap = _snapshot(client)
    assert snap.lending_markets("morpho_blue", loan="USDC") == []


def test_lending_markets_success_false_raises():
    client = MagicMock()
    client.market.ListLendingMarkets.return_value = gateway_pb2.ListLendingMarketsResponse(
        success=False, error="unsupported protocol: 'aave_v3' (known: ['morpho_blue'])"
    )
    snap = _snapshot(client)
    with pytest.raises(LendingMarketResolutionError, match="unsupported protocol"):
        snap.lending_markets("aave_v3")


def test_lending_markets_rpc_error_fails_closed():
    client = MagicMock()
    client.market.ListLendingMarkets.side_effect = RuntimeError("channel down")
    snap = _snapshot(client)
    with pytest.raises(LendingMarketResolutionError, match="channel down"):
        snap.lending_markets("morpho_blue")


def test_lending_markets_no_client_raises():
    snap = MarketSnapshot(chain="ethereum", gateway_client=None)
    with pytest.raises(LendingMarketResolutionError, match="connected GatewayClient"):
        snap.lending_markets("morpho_blue")


# ---------------------------------------------------------------------------
# lending_market
# ---------------------------------------------------------------------------


def test_lending_market_verified():
    client = MagicMock()
    client.market.GetLendingMarket.return_value = gateway_pb2.LendingMarketResponse(
        market=_market(verified=True, source=gateway_pb2.LENDING_MARKET_SOURCE_ONCHAIN_VERIFY),
        success=True,
    )
    snap = _snapshot(client)
    m = snap.lending_market("morpho_blue", _ID)
    assert m.verified is True
    assert m.source == "onchain_verify"
    assert m.market_id == _ID
    sent = client.market.GetLendingMarket.call_args.args[0]
    assert sent.protocol == "morpho_blue"
    assert sent.market_id == _ID
    assert sent.chain == "ethereum"


def test_lending_market_verification_failure_raises_not_retryable():
    client = MagicMock()
    client.market.GetLendingMarket.return_value = gateway_pb2.LendingMarketResponse(
        success=False, error="Morpho market id verification failed on ethereum: ..."
    )
    snap = _snapshot(client)
    with pytest.raises(LendingMarketResolutionError) as exc:
        snap.lending_market("morpho_blue", _ID)
    assert "verification failed" in exc.value.reason
    assert exc.value.retryable is False


def test_lending_market_rpc_error_fails_closed():
    client = MagicMock()
    client.market.GetLendingMarket.side_effect = RuntimeError("deadline exceeded")
    snap = _snapshot(client)
    with pytest.raises(LendingMarketResolutionError, match="deadline exceeded"):
        snap.lending_market("morpho_blue", _ID)


def test_lending_market_success_but_unverified_fails_closed():
    # Defense-in-depth: a future/misbehaving gateway that returns success=True
    # with verified=False must NOT be trusted — the client fails closed.
    client = MagicMock()
    client.market.GetLendingMarket.return_value = gateway_pb2.LendingMarketResponse(
        market=_market(verified=False, source=gateway_pb2.LENDING_MARKET_SOURCE_ONCHAIN_VERIFY),
        success=True,
    )
    snap = _snapshot(client)
    with pytest.raises(LendingMarketResolutionError, match="UNVERIFIED"):
        snap.lending_market("morpho_blue", _ID)


def test_lending_market_chain_override_forwarded():
    client = MagicMock()
    client.market.GetLendingMarket.return_value = gateway_pb2.LendingMarketResponse(
        market=_market(chain="base", verified=True, source=gateway_pb2.LENDING_MARKET_SOURCE_ONCHAIN_VERIFY),
        success=True,
    )
    # Multi-chain snapshot so an explicit override is honoured.
    client.is_connected = True
    client.config.timeout = 30
    snap = MarketSnapshot(chains=["ethereum", "base"], gateway_client=client)
    snap.lending_market("morpho_blue", _ID, chain="base")
    sent = client.market.GetLendingMarket.call_args.args[0]
    assert sent.chain == "base"
