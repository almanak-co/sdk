"""Receipt-backed native balance reconciliation, including external gas payers."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.execution.interfaces import TransactionReceipt
from almanak.framework.intents.vocabulary import SwapIntent
from almanak.framework.runner.reconciliation import BalanceSnapshot, build_reconciliation_report
from almanak.framework.runner.runner_state import _resolve_gas_context

WALLET = "0x" + "ab" * 20
RELAYER = "0x" + "cd" * 20


def receipt(sender=WALLET, gas=224374, price=10**9, **extra):
    return {"from": sender, "gasUsed": gas, "effectiveGasPrice": price, "l1Fee": 0, **extra}


def resolve(receipts, wallet=WALLET, chain="base"):
    return _resolve_gas_context(
        SimpleNamespace(chain=None), SimpleNamespace(receipts=receipts), chain=chain, wallet_address=wallet
    )


@pytest.mark.parametrize("chain,symbol", [("base", "ETH"), ("arbitrum", "ETH"), ("polygon", "MATIC"), ("bsc", "BNB")])
def test_default_intent_chain_uses_balance_provider_chain(chain, symbol):
    assert resolve([receipt()], chain=chain) == (symbol, Decimal("0.000224374"))


def test_external_safe_payer_is_measured_zero_for_safe():
    assert resolve([receipt(RELAYER)]) == ("ETH", Decimal(0))


def test_mixed_bundle_counts_only_wallet_fees_and_explicit_l1_fee():
    assert resolve([receipt(WALLET.upper(), gas="0x2", price="0x3", l1Fee="0x4"), receipt(RELAYER)]) == (
        "ETH",
        Decimal(10) / 10**18,
    )


def test_typed_local_receipt_preserves_payer_and_l1_fee():
    r = TransactionReceipt("0xabc", 1, "0xblock", 2, 3, 1, from_address=WALLET, l1_fee_wei=4)
    result = SimpleNamespace(transaction_results=[SimpleNamespace(receipt=r)])
    assert _resolve_gas_context(SimpleNamespace(chain="base"), result, wallet_address=WALLET) == (
        "ETH",
        Decimal(10) / 10**18,
    )


@pytest.mark.parametrize(
    "receipts,wallet",
    [
        ([], WALLET),
        ([receipt(None)], WALLET),
        ([receipt()], None),
        ([receipt(gas=None)], WALLET),
        ([receipt(price=-1)], WALLET),
        ([receipt(l1Fee="invalid")], WALLET),
    ],
)
def test_unknown_cost_or_payer_is_not_zero(receipts, wallet):
    assert resolve(receipts, wallet) == ("ETH", None)


def test_zero_measured_gas_remains_zero():
    assert resolve([receipt(gas=0)]) == ("ETH", Decimal(0))


@pytest.mark.parametrize("chain", [None, "unknown", "solana"])
def test_unknown_or_non_evm_chain_not_treated_as_wei(chain):
    assert resolve([receipt()], chain=chain) == (None, None)


@pytest.mark.parametrize("native_input", [False, True])
@pytest.mark.parametrize("payer", [WALLET, RELAYER])
def test_native_swap_net_balance_matches_fill_without_widening(native_input, payer):
    fill = Decimal("0.001240726504188265")
    symbol, gas = resolve([receipt(payer)])
    source, target = ("ETH", "USDC") if native_input else ("USDC", "ETH")
    amount_in, amount_out = (fill, Decimal("3.10")) if native_input else (Decimal("3.10"), fill)
    intent = SwapIntent(from_token=source, to_token=target, amount=amount_in, max_slippage=Decimal("0.005"))
    result = SimpleNamespace(
        success=True, swap_amounts=SimpleNamespace(amount_in_decimal=amount_in, amount_out_decimal=amount_out)
    )
    pre = {"ETH": Decimal("1"), "USDC": Decimal("10")}
    post = {**pre, source: pre[source] - amount_in, target: pre[target] + amount_out}
    post["ETH"] -= gas
    report = build_reconciliation_report(
        BalanceSnapshot(datetime.now(UTC), pre),
        BalanceSnapshot(datetime.now(UTC), post),
        intent,
        result,
        gas_token=symbol,
        gas_cost_native=gas,
    )
    assert not report.incident
    expected = report.expected_ranges["ETH"]
    assert expected.max - expected.min == fill * Decimal("0.01")
    assert report.actual_deltas["ETH"] == (-fill if native_input else fill) - gas
    # An unexplained extra debit still fails with the original tolerance.
    post["ETH"] -= Decimal("0.0001")
    bad = build_reconciliation_report(
        BalanceSnapshot(datetime.now(UTC), pre),
        BalanceSnapshot(datetime.now(UTC), post),
        intent,
        result,
        gas_token=symbol,
        gas_cost_native=gas,
    )
    assert bad.incident


@pytest.mark.asyncio
@pytest.mark.parametrize("payer", [WALLET, RELAYER, None])
async def test_runner_passes_bracket_wallet_and_chain_to_gas_reconciliation(payer):
    from unittest.mock import AsyncMock, MagicMock

    from almanak.framework.runner.runner_models import RunnerConfig
    from almanak.framework.runner.strategy_runner import StrategyRunner

    fill = Decimal("0.001240726504188265")
    gas = Decimal("0.000224374") if payer == WALLET else Decimal(0)
    provider = SimpleNamespace(chain="base", wallet_address=WALLET)
    provider.get_balance = AsyncMock(
        side_effect=lambda token, **kwargs: SimpleNamespace(
            balance=Decimal("6.90") if token == "USDC" else Decimal(1) + fill - gas
        )
    )
    runner = StrategyRunner(
        price_oracle=MagicMock(),
        balance_provider=provider,
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        config=RunnerConfig(enable_state_persistence=False, enable_alerting=False),
    )
    result = SimpleNamespace(
        success=True,
        receipts=[receipt(payer)],
        transaction_results=[],
        swap_amounts=SimpleNamespace(amount_in_decimal=Decimal("3.10"), amount_out_decimal=fill),
    )
    intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("3.10"))
    report = await runner._reconcile_post_execution_balances(
        SimpleNamespace(chain="base", deployment_id="test"),
        intent,
        result,
        pre_snapshot=BalanceSnapshot.now({"USDC": Decimal(10), "ETH": Decimal(1)}),
    )
    assert report is not None
    assert not report["incident"]
    assert bool(report["warnings"]) == (payer is None)


@pytest.mark.parametrize("chain", ["base", "optimism"])
def test_op_missing_l1_fee_is_incomplete_even_with_execution_cost(chain):
    raw = receipt()
    raw.pop("l1Fee")
    assert resolve([raw], chain=chain) == ("ETH", None)
    assert resolve([raw], wallet=RELAYER, chain=chain) == ("ETH", Decimal(0))


@pytest.mark.parametrize("chain", ["ethereum", "arbitrum"])
def test_non_additive_fee_chains_do_not_require_op_receipt_field(chain):
    raw = receipt()
    raw.pop("l1Fee")
    assert resolve([raw], chain=chain) == ("ETH", Decimal("0.000224374"))


@pytest.mark.parametrize(
    "version,declared,success,expected",
    [
        ("anvil/v1.0", "anvil", True, Decimal("0.000224374")),
        ("Geth/v1.0", "anvil", True, None),
        ("anvil/v1.0", "mainnet", True, None),
        ("anvil/v1.0", "anvil", False, None),
    ],
)
def test_op_execution_only_cost_requires_actual_gateway_node_proof(version, declared, success, expected):
    import json
    from unittest.mock import MagicMock

    raw = receipt()
    raw.pop("l1Fee")
    gateway = MagicMock()
    gateway.rpc.Call.return_value = SimpleNamespace(success=success, result=json.dumps(version))
    assert _resolve_gas_context(
        SimpleNamespace(chain="base"),
        SimpleNamespace(receipts=[raw]),
        wallet_address=WALLET,
        gateway_client=gateway,
        declared_network=declared,
    ) == ("ETH", expected)
