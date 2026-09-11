"""Snapshot identities follow actual gateway balance requests, never strategy defaults."""

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.cli.run import create_sync_balance_func
from almanak.framework.data.balance.gateway_multichain import MultiChainGatewayBalanceProvider
from almanak.framework.data.balance.gateway_provider import GatewayBalanceProvider
from almanak.framework.market.builders import MarketSnapshotBuilder
from almanak.framework.market.models import TokenBalance as MarketBalance
from almanak.framework.portfolio.models import PortfolioSnapshot, TokenBalance
from almanak.framework.teardown.models import TeardownPositionSummary
from almanak.framework.valuation.portfolio_valuer import PortfolioValuer
from almanak.framework.valuation.wallet_scope import WalletScopeCapture
from almanak.gateway.proto import gateway_pb2

SAFE = "0xAb1234567890123456789012345678901234567890"
SOLANA = "9xQeWvG816bUx9EPfEZwga53zhC2yCjVxAe6FUhFBc7"


def response(balance="2", usd="0"):
    return gateway_pb2.BalanceResponse(balance=balance, balance_usd=usd, raw_balance="2", decimals=6)


def test_single_provider_sync_usd_enrichment_and_json_roundtrip():
    client = MagicMock()
    client.market.GetBalance.return_value = response()
    provider = GatewayBalanceProvider(client, SAFE, "bsc")
    oracle = SimpleNamespace(get_aggregated_price=AsyncMock(side_effect=ValueError("no initial price")))
    wrapped = create_sync_balance_func(provider, oracle)
    strategy = SimpleNamespace(chain="bsc", wallet_address="0xUnrelated", _balance_provider=wrapped)
    market = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, runtime_surface="hosted")
    raw = wrapped("USDT")
    assert raw.wallet_address == SAFE
    assert raw.chain == "bsc"
    normalized, measured = market._coerce_balance_result("USDT", asyncio.run(provider.get_balance("USDT")))
    assert not measured
    filled = market._fill_balance_usd(normalized, "USDT", "bsc", price=Decimal("3"), usd_measured=measured)
    assert filled.wallet_address == SAFE and filled.chain == "bsc"
    assert filled.balance_usd == Decimal("6")
    capture = WalletScopeCapture()
    capture.observe("USDT", "bsc", filled)
    rows = [TokenBalance("USDT", Decimal("2"), Decimal("6"))]
    capture.apply(rows)
    snapshot = PortfolioSnapshot(
        datetime.now(UTC),
        "deployment:test",
        Decimal("0"),
        Decimal("6"),
        wallet_balances=rows,
        snapshot_metadata={"wallet_scope": capture.metadata()},
    )
    restored = PortfolioSnapshot.from_dict(json.loads(json.dumps(snapshot.to_dict())))
    assert restored.wallet_balances[0].wallet_address == SAFE.lower()
    assert restored.wallet_balances[0].chain == "bsc"
    assert restored.to_positions_payload()["metadata"]["wallet_scope"]["chain_wallets"] == {"bsc": SAFE.lower()}
    assert client.market.GetBalance.call_args.args[0].wallet_address == SAFE


@pytest.mark.parametrize("balance", ["2", "0"])
def test_multichain_request_uses_safe_and_distinct_solana_wallet(balance):
    client = MagicMock()
    client.market.GetBalance.return_value = response(balance)
    provider = MultiChainGatewayBalanceProvider(client, SAFE, ["bsc", "solana"], {"solana": SOLANA})
    capture = WalletScopeCapture()
    for chain, token, wallet in [("bsc", "BNB", SAFE), ("solana", "SOL", SOLANA)]:
        result = provider.get_balance(token, chain)
        assert client.market.GetBalance.call_args.args[0].wallet_address == wallet
        capture.observe(token, chain, result)
    assert capture.metadata()["chain_wallets"] == {"bsc": SAFE.lower(), "solana": SOLANA}


@pytest.mark.parametrize("kind", ["failure", "empty", "unconfigured"])
def test_unmeasured_provider_results_do_not_claim_scope(kind):
    client = MagicMock()
    client.market.GetBalance.return_value = response("")
    if kind == "failure":
        client.market.GetBalance.side_effect = ValueError("invalid request")
    provider = MultiChainGatewayBalanceProvider(client, SAFE, ["bsc"])
    chain = "solana" if kind == "unconfigured" else "bsc"
    result = provider.get_balance("USDT", chain)
    capture = WalletScopeCapture()
    capture.observe("USDT", chain, result)
    assert capture.metadata()["chain_wallets"] == {}


@pytest.mark.parametrize("second", [None, ("base", SAFE), ("bsc", "0xOther")])
def test_mixed_or_unknown_origins_never_receive_unique_row_identity(second):
    capture = WalletScopeCapture()
    capture.observe("USDT", "bsc", MarketBalance("USDT", Decimal("2"), Decimal("2"), chain="bsc", wallet_address=SAFE))
    chain, wallet = second or ("bsc", None)
    capture.observe(
        "USDT", chain, MarketBalance("USDT", Decimal("0"), Decimal("0"), chain=chain, wallet_address=wallet)
    )
    row = TokenBalance("USDT", Decimal("2"), Decimal("2"))
    capture.apply([row])
    assert row.chain is None and row.wallet_address is None
    if second and second[0] == "bsc":
        assert capture.metadata()["chain_wallets"] == {}


def test_solana_never_inherits_evm_fallback():
    capture = WalletScopeCapture()
    capture.observe(
        "SOL", "solana", MarketBalance("SOL", Decimal("0"), Decimal("0"), chain="solana", wallet_address=SAFE)
    )
    assert capture.metadata()["chain_wallets"] == {}


@pytest.mark.parametrize("balance", ["0", "2"])
def test_actual_valuer_capture_persists_wallet_context(balance):
    client = MagicMock()
    client.market.GetBalance.return_value = response(balance, balance)
    provider = MultiChainGatewayBalanceProvider(client, SAFE, ["bsc"])
    strategy = SimpleNamespace(
        chain="bsc",
        wallet_address="0xUnrelated",
        deployment_id="deployment:scope",
        STRATEGY_METADATA=None,
        _get_tracked_tokens=lambda: ["USDT"],
        get_open_positions=lambda: TeardownPositionSummary(
            deployment_id="deployment:scope", timestamp=None, positions=[]
        ),
    )
    market = MarketSnapshotBuilder.for_strategy_runner(
        strategy=strategy,
        chain="bsc",
        chains=["bsc"],
        runtime_surface="hosted",
        multi_chain_balance_provider=provider,
        multi_chain_price_oracle=lambda token, quote="USD", chain=None: Decimal("1"),
    )
    valuer = PortfolioValuer()
    snapshot = valuer.value(strategy, market)
    assert snapshot.error is None
    assert snapshot.to_positions_payload()["metadata"]["wallet_scope"]["chain_wallets"] == {"bsc": SAFE.lower()}
    assert any(row.symbol == "USDT" for row in snapshot.wallet_balances) == (balance != "0")
    assert snapshot.wallet_balances[0].balance == Decimal(balance)
    assert snapshot.wallet_balances[0].wallet_address == SAFE.lower()


def test_multichain_valuer_aggregation_keeps_quantity_but_not_ambiguous_scope():
    client = MagicMock()
    client.market.GetBalance.return_value = response("2", "2")
    provider = MultiChainGatewayBalanceProvider(client, SAFE, ["bsc", "base"])
    market = SimpleNamespace(balance=provider.get_balance, price=lambda token, chain=None: Decimal("1"))
    capture = WalletScopeCapture()
    balances, prices, incomplete = PortfolioValuer._fetch_wallet_balances_and_prices(
        ["bsc", "base"], market, ["USDT"], capture
    )
    assert not incomplete and balances == {"USDT": Decimal("4")}
    assert prices == {"USDT": Decimal("1")}
    row = TokenBalance("USDT", balances["USDT"], Decimal("4"))
    capture.apply([row])
    assert row.chain is None and row.wallet_address is None
    assert capture.metadata()["chain_wallets"] == {"bsc": SAFE.lower(), "base": SAFE.lower()}


def test_native_aggregation_does_not_stamp_first_chain_wallet():
    client = MagicMock()
    client.market.GetBalance.return_value = response("2", "2")
    provider = MultiChainGatewayBalanceProvider(client, SAFE, ["arbitrum", "base"])
    market = SimpleNamespace(balance=provider.get_balance, price=lambda token, chain=None: Decimal("1"))
    capture = WalletScopeCapture()
    status, rows = PortfolioValuer._resolve_native_gas_rows(["arbitrum", "base"], market, {}, {}, capture)
    capture.apply(rows)
    assert status == "ok" and len(rows) == 1 and rows[0].balance == Decimal("4")
    assert rows[0].chain is None and rows[0].wallet_address is None


def test_legacy_snapshot_roundtrip_never_infers_wallet_from_deployment_or_chain():
    snapshot = PortfolioSnapshot(
        datetime.now(UTC),
        "deployment:wallet-hash",
        Decimal("0"),
        Decimal("2"),
        chain="bsc",
        wallet_balances=[TokenBalance("USDT", Decimal("2"), Decimal("2"))],
    )
    payload = snapshot.to_dict()
    payload["wallet_balances"][0].pop("chain")
    payload["wallet_balances"][0].pop("wallet_address")
    restored = PortfolioSnapshot.from_dict(payload)
    assert restored.wallet_balances[0].chain is None
    assert restored.wallet_balances[0].wallet_address is None
    assert "wallet_scope" not in restored.to_positions_payload()["metadata"]


def test_case_distinct_solana_mints_cannot_certify_collapsed_row():
    capture = WalletScopeCapture()
    for token in ("AbCdefGhijk", "aBCdefGhijk"):
        capture.observe(
            token, "solana", MarketBalance(token, Decimal("2"), Decimal("2"), chain="solana", wallet_address=SOLANA)
        )
    row = TokenBalance("ABCDEFGHIJK", Decimal("4"), Decimal("4"))
    capture.apply([row])
    assert row.chain is None and row.wallet_address is None
    assert capture.metadata()["chain_wallets"] == {"solana": SOLANA}


def test_result_chain_mismatch_cannot_certify_request_context():
    capture = WalletScopeCapture()
    capture.observe("USDT", "bsc", MarketBalance("USDT", Decimal("2"), Decimal("2"), chain="base", wallet_address=SAFE))
    assert capture.metadata()["chain_wallets"] == {}


@pytest.mark.parametrize("kind", ["single", "multi"])
def test_failed_current_read_cannot_certify_cached_identity(kind):
    client = MagicMock()
    client.market.GetBalance.side_effect = [response("2", "2"), ValueError("invalid request")]
    if kind == "single":
        provider = GatewayBalanceProvider(client, SAFE, "bsc")

        def read():
            return asyncio.run(provider.get_balance("USDT"))
    else:
        provider = MultiChainGatewayBalanceProvider(client, SAFE, ["bsc"])

        def read():
            return provider.get_balance("USDT", "bsc")

    successful = read()
    fallback = read()
    assert successful.wallet_address == SAFE
    assert fallback.balance == successful.balance == Decimal("2")
    assert fallback.chain is None and fallback.wallet_address is None
    assert successful.wallet_address == SAFE
    capture = WalletScopeCapture()
    capture.observe("USDT", "bsc", fallback)
    assert capture.metadata()["chain_wallets"] == {}


@pytest.mark.parametrize("kind", ["single", "multi"])
def test_explicit_gateway_stale_response_is_unscoped(kind):
    client = MagicMock()
    stale = response("2", "2")
    stale.stale = True
    client.market.GetBalance.return_value = stale
    if kind == "single":
        result = asyncio.run(GatewayBalanceProvider(client, SAFE, "bsc").get_balance("USDT"))
    else:
        result = MultiChainGatewayBalanceProvider(client, SAFE, ["bsc"]).get_balance("USDT", "bsc")
    assert result.balance == Decimal("2")
    assert result.chain is None and result.wallet_address is None


def test_partial_multichain_read_failure_cannot_certify_aggregate_row():
    def balance(token, chain):
        if chain == "base":
            raise ValueError("missing endpoint")
        return MarketBalance(token, Decimal("2"), Decimal("2"), chain=chain, wallet_address=SAFE)

    market = SimpleNamespace(balance=balance, price=lambda token, chain=None: Decimal("1"))
    capture = WalletScopeCapture()
    balances, _, _ = PortfolioValuer._fetch_wallet_balances_and_prices(["bsc", "base"], market, ["USDT"], capture)
    row = TokenBalance("USDT", balances["USDT"], Decimal("2"))
    capture.apply([row])
    assert row.chain is None and row.wallet_address is None
    assert capture.metadata()["chain_wallets"] == {"bsc": SAFE.lower()}


@pytest.mark.parametrize("value", [Decimal("NaN"), Decimal("Infinity"), Decimal("-1"), None])
def test_malformed_or_unmeasured_balance_does_not_certify_scope(value):
    capture = WalletScopeCapture()
    capture.observe("USDT", "bsc", SimpleNamespace(balance=value, chain="bsc", wallet_address=SAFE))
    assert capture.metadata()["chain_wallets"] == {}


def test_empty_gateway_token_address_does_not_put_the_wallet_in_the_token_column():
    """`BalanceResult.address` is `response.address or self._wallet_address`, so an
    empty gateway token address yields the WALLET. That column is the token contract
    address; the fallback must not be propagated into it."""
    client = MagicMock()
    client.market.GetBalance.return_value = response()  # no address on the response
    provider = GatewayBalanceProvider(client, SAFE, "bsc")
    strategy = SimpleNamespace(chain="bsc", wallet_address="0xUnrelated", _balance_provider=None)
    market = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, runtime_surface="hosted")

    raw = asyncio.run(provider.get_balance("USDT"))
    assert raw.address == SAFE, "precondition: the provider really does fall back to the wallet"

    normalized, _ = market._coerce_balance_result("USDT", raw)
    assert normalized.address == ""
    assert normalized.wallet_address == SAFE, "the wallet still reaches its own field"


def test_real_token_address_still_propagates():
    client = MagicMock()
    token = "0x55d398326f99059fF775485246999027B3197955"
    client.market.GetBalance.return_value = gateway_pb2.BalanceResponse(
        balance="2", balance_usd="0", raw_balance="2", decimals=6, address=token
    )
    provider = GatewayBalanceProvider(client, SAFE, "bsc")
    strategy = SimpleNamespace(chain="bsc", wallet_address="0xUnrelated", _balance_provider=None)
    market = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, runtime_surface="hosted")

    normalized, _ = market._coerce_balance_result("USDT", asyncio.run(provider.get_balance("USDT")))
    assert normalized.address == token
    assert normalized.wallet_address == SAFE


def test_balance_result_to_dict_carries_the_provenance_fields():
    """Both fields were added to the dataclass but not to its hand-written to_dict,
    so any serializer using it dropped them silently."""
    client = MagicMock()
    client.market.GetBalance.return_value = response()
    provider = GatewayBalanceProvider(client, SAFE, "bsc")
    payload = asyncio.run(provider.get_balance("USDT")).to_dict()
    assert payload["chain"] == "bsc"
    assert payload["wallet_address"] == SAFE
