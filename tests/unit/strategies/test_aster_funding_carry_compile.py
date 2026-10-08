"""Every intent the Aster funding-carry strategy emits compiles through the real IntentCompiler.

The strategy addresses its margin and quote token by the BSC USDT contract
address rather than the "USDT" symbol, so this pins that the Aster compiler
still recognises the address as its margin asset and that the PancakeSwap legs
resolve both tokens. Compilation runs with no RPC and placeholder prices: it
proves the intent shapes, not quotes.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak import IntentCompiler, IntentCompilerConfig
from almanak.framework.intents.compiler_models import CompilationStatus
from almanak.framework.intents.vocabulary import IntentType
from strategies.incubating.aster_funding_carry.strategy import AsterFundingCarryStrategy, CarryConfig

_CONFIG = json.loads(
    (Path(__file__).resolve().parents[3] / "strategies/incubating/aster_funding_carry/config.json").read_text()
)
USDT = "0x55d398326f99059ff775485246999027b3197955"
ETH = "0x2170ed0880ac9a755fd29b2688956bd959f933f8"
ASTER_VAULT = "0x128463A60784c4D3f46c23Af3f65Ed859Ba87974"
WALLET = "0x" + "ab" * 20


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(IntentCompiler, "_get_chain_rpc_url", lambda self: None)


def _strategy(state: dict[str, Any]) -> AsterFundingCarryStrategy:
    strategy = AsterFundingCarryStrategy.__new__(AsterFundingCarryStrategy)
    strategy.config = dict(_CONFIG)
    strategy.carry = CarryConfig(strategy.config)
    strategy.state = state
    strategy._deployment_id = "test-aster-carry-compile"
    strategy._chain = "bsc"
    return strategy


def _market(rate_8h: str) -> MagicMock:
    market = MagicMock()
    market.funding_rate.return_value = SimpleNamespace(rate_8h=Decimal(rate_8h), is_live_data=True)
    market.price.return_value = Decimal("2566")
    market.balance.return_value = SimpleNamespace(balance=Decimal("0"))
    return market


def _compile(intent: Any) -> dict[str, Any]:
    compiler = IntentCompiler(
        chain="bsc", wallet_address=WALLET, config=IntentCompilerConfig(allow_placeholder_prices=True)
    )
    result = compiler.compile(intent)
    assert result.status == CompilationStatus.SUCCESS, result.error
    assert result.action_bundle is not None
    return {"metadata": result.action_bundle.metadata, "transactions": result.action_bundle.transactions}


def _to(tx: Any) -> str:
    return str(tx.get("to") if isinstance(tx, dict) else tx.to).lower()


def test_perp_deposit_with_the_usdt_address_compiles_to_a_vault_deposit() -> None:
    intent = _strategy({"phase": "deposit"}).decide(_market("0.0002"))
    assert intent.intent_type == IntentType.PERP_DEPOSIT and intent.asset.lower() == USDT

    bundle = _compile(intent)

    assert bundle["metadata"]["asset"] == "USDT"
    assert bundle["metadata"]["amount"] == "3"
    assert bundle["metadata"]["amount_wei"] == str(3 * 10**18)
    assert bundle["metadata"]["token_address"].lower() == USDT
    assert [_to(tx) for tx in bundle["transactions"]] == [USDT, ASTER_VAULT.lower()]


def test_perp_open_short_compiles_to_an_eth_usdt_order() -> None:
    intent = _strategy({"phase": "idle"}).decide(_market("0.0002"))
    assert intent.intent_type == IntentType.PERP_OPEN

    order = _compile(intent)["metadata"]["order_request"]

    assert order["symbol"] == "ETHUSDT"
    assert order["is_long"] is False
    assert order["close_position"] is False
    assert order["notional_usd"] == "6.41"
    assert order["leverage"] == 3
    assert order["max_slippage"] == "0.005"


def test_perp_close_compiles_to_a_full_reduce_only_close() -> None:
    state = {"phase": "carry", "perp_qty": "0.002", "spot_amount": "0.00199"}
    intent = _strategy(state).decide(_market("0"))
    assert intent.intent_type == IntentType.PERP_CLOSE

    order = _compile(intent)["metadata"]["order_request"]

    assert order["symbol"] == "ETHUSDT"
    assert order["is_long"] is False
    assert order["close_position"] is True


def test_perp_withdraw_all_with_the_usdt_address_compiles() -> None:
    intent = _strategy({"phase": "idle", "deposit_sent_at": 1.0})._withdraw()

    request = _compile(intent)["metadata"]["withdraw_request"]

    assert request["asset"] == "USDT"
    assert request["amount"] == "all"


@pytest.mark.parametrize(
    ("state", "from_token", "to_token", "amount_in_wei"),
    [
        ({"phase": "hedge_spot", "perp_qty": "0.002"}, USDT, ETH, str(Decimal("5.14") * 10**18)),
        ({"phase": "exit_spot", "spot_amount": "0.00199"}, ETH, USDT, str(Decimal("0.00199") * 10**18)),
    ],
    ids=["buy-spot", "sell-spot"],
)
def test_pancakeswap_legs_compile_with_both_tokens_resolved(
    state: dict[str, Any], from_token: str, to_token: str, amount_in_wei: str
) -> None:
    intent = _strategy(state).decide(_market("0.0002"))
    assert intent.intent_type == IntentType.SWAP

    metadata = _compile(intent)["metadata"]

    assert metadata["protocol"] == "pancakeswap_v3"
    assert metadata["from_token"]["address"].lower() == from_token
    assert metadata["to_token"]["address"].lower() == to_token
    assert metadata["amount_in"] == str(int(Decimal(amount_in_wei)))
