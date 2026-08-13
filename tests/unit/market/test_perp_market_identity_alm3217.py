"""ALM-3217: strategy market data binds to verified perp identity."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.framework.data import MarketSnapshotError
from almanak.framework.market import MarketSnapshotBuilder


def _snapshot(gateway_client=None):
    strategy = SimpleNamespace(chain="arbitrum", wallet_address="0x1")
    return MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy,
        gateway_client=gateway_client,
        runtime_surface="unit_test",
    )


def _client(*, verified: bool = True, symbol: str = "XMR") -> SimpleNamespace:
    stub = MagicMock()
    stub.return_value = SimpleNamespace(
        success=True,
        error="",
        market=SimpleNamespace(
            verified=verified,
            label="XMR/USD",
            market_token="0x" + "11" * 20,
            index_token="0x" + "22" * 20,
            index_symbol=symbol,
        ),
    )
    return SimpleNamespace(market=SimpleNamespace(GetPerpMarket=stub), config=SimpleNamespace(timeout=7.0))


def test_perp_market_returns_verified_index_identity() -> None:
    client = _client()
    snapshot = _snapshot(client)

    result = snapshot.perp_market("gmx_v2", "0x" + "11" * 20)

    assert result.index_symbol == "XMR"
    request = client.market.GetPerpMarket.call_args.args[0]
    assert (request.protocol, request.chain, request.market, request.require_listed) == (
        "gmx_v2",
        "arbitrum",
        "0x" + "11" * 20,
        False,
    )
    assert client.market.GetPerpMarket.call_args.kwargs["timeout"] == 7.0


@pytest.mark.parametrize("verified,symbol", [(False, "XMR"), (True, "")])
def test_perp_market_rejects_unusable_identity(verified: bool, symbol: str) -> None:
    snapshot = _snapshot(_client(verified=verified, symbol=symbol))

    with pytest.raises(MarketSnapshotError):
        snapshot.perp_market("gmx_v2", "0x" + "11" * 20)


def test_perp_market_uses_connector_metadata_without_gateway(monkeypatch) -> None:
    from almanak.connectors._strategy_base.perps_read_base import PerpsMarketMeta
    from almanak.connectors._strategy_base.perps_read_registry import PerpsReadRegistry

    monkeypatch.setattr(
        PerpsReadRegistry,
        "market_metadata",
        lambda protocol, market, chain: PerpsMarketMeta(index_token_symbol="XMR", index_token_decimals=12),
    )
    snapshot = _snapshot()

    assert snapshot.perp_market("gmx_v2", "0x" + "11" * 20).index_symbol == "XMR"
