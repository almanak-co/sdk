"""Allocation and receipt ownership contracts for the deployable spot example."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from almanak.framework.execution.extracted_data import SwapAmounts
from almanak.framework.market.models import PriceData
from almanak.framework.strategies.exceptions import ConfigValidationError
from strategies.bstocks_spot.strategy import BASE, QUOTE, BStocksSpotStrategy


def strategy(**config):
    return BStocksSpotStrategy(config=config, chain="bsc", wallet_address="0x" + "1" * 40)


def market(quote="100", base="0", age=0):
    now = datetime.now(UTC)
    m = Mock(timestamp=now)
    m.price_data.return_value = PriceData(price=Decimal("300"), timestamp=now - timedelta(seconds=age))
    m.price.return_value = Decimal("300")
    m.balance.side_effect = lambda token: SimpleNamespace(balance=Decimal(quote if token == QUOTE else base))
    return m


def result(incoming="7", outgoing="0.02", resolved=True):
    return SimpleNamespace(
        swap_amounts=SwapAmounts(
            amount_in=1,
            amount_out=1,
            amount_in_decimal=Decimal(incoming),
            amount_out_decimal=None if outgoing is None else Decimal(outgoing),
            amount_out_decimal_resolved=resolved,
        )
    )


def entered():
    s = strategy()
    intent = s.decide(market())
    s.on_intent_executed(intent, True, result())
    return s


def test_allocation_does_not_consume_ambient_quote_or_base():
    s = strategy()
    buy = s.decide(market())
    assert buy.amount == Decimal("7")
    s.on_intent_executed(buy, True, result())
    (sell,) = s.generate_teardown_intents(market=market(base="900.02"))
    assert sell.amount == Decimal("0.02")
    assert sell.from_token == BASE and sell.to_token == QUOTE


@pytest.mark.parametrize("age", [121, -1])
def test_stale_and_future_prices_do_not_consume_entry(age):
    s = strategy()
    assert s.decide(market(age=age)).intent_type.value == "HOLD"
    assert s.get_persistent_state()["phase"] == "ready"
    assert s.decide(market()).intent_type.value == "SWAP"


def test_unknown_price_timestamp_and_insufficient_balance_hold():
    s = strategy()
    m = market()
    m.price_data.return_value.timestamp = None
    assert s.decide(m).intent_type.value == "HOLD"
    assert s.decide(market(quote="6.99")).intent_type.value == "HOLD"


def test_entry_ceiling_waits_without_spending():
    s = strategy(entry_price_ceiling_usd="299")
    assert s.decide(market()).intent_type.value == "HOLD"


@pytest.mark.parametrize(
    "success,execution_result",
    [
        (False, None),
        (True, None),
        (True, result(outgoing=None)),
        (True, result(resolved=False)),
        (True, result(incoming="6")),
    ],
)
def test_unknown_or_failed_execution_never_replays_or_fabricates_inventory(success, execution_result):
    s = strategy()
    buy = s.decide(market())
    if success:
        with pytest.raises(ValueError, match="Landed swap"):
            s.on_intent_executed(buy, success, execution_result)
    else:
        s.on_intent_executed(buy, success, execution_result)
    restarted = strategy()
    restarted.load_persistent_state(s.get_persistent_state())
    assert restarted.decide(market()).intent_type.value == "HOLD"
    assert restarted.get_persistent_state()["owned_base"] == "0"
    with pytest.raises(RuntimeError, match="Reconcile"):
        restarted.generate_teardown_intents(market=market())


def test_restart_receipt_recovery_is_idempotent():
    s = strategy()
    buy = s.decide(market())
    restarted = strategy()
    restarted.load_persistent_state(s.get_persistent_state())
    restarted.on_intent_executed(buy, True, result())
    restarted.on_intent_executed(buy, True, result(outgoing="10"))
    assert restarted.get_persistent_state()["owned_base"] == "0.02"
    assert restarted.decide(market()).intent_type.value == "HOLD"


def test_restart_holding_and_exit_receipt_close_only_owned_inventory():
    s = strategy()
    s.load_persistent_state(entered().get_persistent_state())
    (sell,) = s.generate_teardown_intents(market=market(base="0.02"))
    s.generate_teardown_intents(market=market(base="0.02"))
    assert s.get_persistent_state()["phase"] == "holding"
    s.on_intent_executed(sell, True, result(incoming="0.02", outgoing="6.98"))
    assert s.get_persistent_state()["phase"] == "done"
    assert s.generate_teardown_intents(market=market(base="0.02")) == []
    assert s.decide(market()).intent_type.value == "HOLD"


def test_missing_wallet_inventory_is_not_silently_written_off():
    s = entered()
    with pytest.raises(RuntimeError, match="below receipt-owned"):
        s.generate_teardown_intents(market=market(base="0"))
    assert s.get_persistent_state()["owned_base"] == "0.02"


def test_measured_zero_is_completed_entry_not_missing_result():
    s = strategy()
    buy = s.decide(market())
    s.on_intent_executed(buy, True, result(outgoing="0"))
    assert s.get_persistent_state()["phase"] == "done"


@pytest.mark.parametrize(
    "config",
    [
        {"quote_allocation": "NaN"},
        {"quote_allocation": "0"},
        {"swap_max_slippage": "0.99"},
        {"max_price_age_seconds": "Infinity"},
        {"protocol": "uniswap_v3"},
        {"base_token": QUOTE},
    ],
)
def test_invalid_configuration_refuses(config):
    with pytest.raises(ConfigValidationError):
        strategy(**config)


def test_existing_allocation_cannot_be_changed_on_restart():
    with pytest.raises(ValueError, match="Allocation"):
        strategy(quote_allocation="8").load_persistent_state(entered().get_persistent_state())


def test_observation_newer_than_snapshot_is_fresh():
    s = strategy()
    m = market()
    m.timestamp -= timedelta(seconds=10)
    assert s.decide(m).intent_type.value == "SWAP"


def test_alternative_explicit_pool_is_configurable_and_restart_identity_bound():
    config = {"base_token": "0x" + "2" * 40, "pool_address": "0x" + "3" * 40, "fee_tier": 500}
    s = strategy(**config)
    buy = s.decide(market())
    assert buy.to_token == config["base_token"]
    assert buy.swap_params == {"pool": config["pool_address"], "fee_tier": 500}
    with pytest.raises(ValueError, match="different market"):
        strategy().load_persistent_state(s.get_persistent_state())


def test_changed_pair_without_corresponding_pool_refuses():
    with pytest.raises(ConfigValidationError, match="requires its own"):
        strategy(base_token="0x" + "2" * 40)


def test_provider_unavailability_holds_before_committing_allocation():
    from almanak.framework.data.exceptions import DataUnavailableError

    s = strategy()
    m = market()
    m.price_data.side_effect = DataUnavailableError("price", BASE, "provider unavailable")
    assert s.decide(m).intent_type.value == "HOLD"
    assert s.get_persistent_state()["phase"] == "ready"


def test_default_teardown_policy_does_not_consolidate_wallet():
    from almanak.framework.teardown import TeardownAssetPolicy

    assert strategy().get_teardown_profile().preferred_asset_policy == TeardownAssetPolicy.KEEP_OUTPUTS


def test_teardown_preview_planning_has_no_state_side_effects():
    s = entered()
    before = s.get_persistent_state()
    s.generate_teardown_intents(market=market(base="0.02"))
    s.generate_teardown_intents(market=market(base="0.02"))
    assert s.get_persistent_state() == before


def test_shipped_config_is_accepted_by_managed_funding_parser():
    import json
    from pathlib import Path

    from almanak.gateway.managed import ManagedGateway

    config = json.loads((Path(__file__).parents[3] / "strategies/bstocks_spot/config.json").read_text())
    gateway = object.__new__(ManagedGateway)
    gateway._anvil_funding = config["anvil_funding"]
    symbol, native, tokens = gateway._parse_anvil_funding_for_chain("bsc")
    assert symbol == "BNB" and native == Decimal("1")
    assert tokens == {QUOTE: Decimal("20")}


@pytest.mark.parametrize("include_canonical_identity", [True, False])
def test_real_portfolio_valuation_counts_receipt_inventory_once_with_ambient_holdings(include_canonical_identity):
    import json
    from unittest.mock import MagicMock

    from almanak.framework.cli._strategy_config import DictConfigWrapper
    from almanak.framework.valuation.portfolio_valuer import PortfolioValuer

    s = BStocksSpotStrategy(
        config=DictConfigWrapper({"base_token": BASE, "quote_token": QUOTE}),
        chain="bsc",
        wallet_address="0x" + "1" * 40,
    )
    s._deployment_id = "deployment:spot-valuation"
    buy = s.decide(market())
    s.on_intent_executed(buy, True, result())
    m = MagicMock(timestamp=datetime.now(UTC))
    prices = {BASE: Decimal("300"), "GOOGLB": Decimal("300"), QUOTE: Decimal("1"), "USDT": Decimal("1")}
    balances = {BASE: Decimal("0.03"), "GOOGLB": Decimal("0.03"), QUOTE: Decimal("13"), "USDT": Decimal("13")}

    def price(token, *args, **kwargs):
        if token not in prices:
            raise ValueError(token)
        return prices[token]

    def balance(token, *args, **kwargs):
        if token not in balances:
            raise ValueError(token)
        return SimpleNamespace(balance=balances[token])

    m.price.side_effect = price
    m.balance.side_effect = balance
    s.create_market_snapshot = lambda: m
    positions = s.get_open_positions()
    assert len(positions.positions) == 1
    assert positions.positions[0].details["amount"] == "0.02"
    if not include_canonical_identity:
        positions.positions[0].details.pop("asset")
        positions.positions[0].details.pop("address")
    s.get_open_positions = lambda: positions
    store = MagicMock()
    store.get_accounting_events_sync.return_value = [
        {
            "event_type": "SWAP",
            "deployment_id": s.deployment_id,
            "position_key": "",
            "chain": "bsc",
            "wallet_address": s.wallet_address,
            "timestamp": datetime.now(UTC).isoformat(),
            "payload_json": json.dumps(
                {
                    "event_type": "SWAP",
                    "swap_position_key": f"swap:bsc:{s.wallet_address}",
                    "token_in": "USDT",
                    "amount_in": "7",
                    "token_out": "GOOGLB",
                    "amount_out": "0.02",
                    "amount_out_usd": "7",
                }
            ),
        }
    ]
    valuer = PortfolioValuer()
    valuer.set_accounting_context(store, s.deployment_id)
    snapshot = valuer.value(s, m)
    expected_duplicate = Decimal("0") if include_canonical_identity else Decimal("6")
    assert snapshot.total_value_usd == Decimal("6") + expected_duplicate
    assert snapshot.available_cash_usd == Decimal("16")
    assert snapshot.wallet_total_value_usd == Decimal("22") + expected_duplicate
    assert len(snapshot.positions) == 2
    assert sum(p.value_usd for p in snapshot.positions if p.details.get("source") == "swap_inventory_lots") == Decimal(
        "6"
    )


def test_preexisting_base_holds_entry_without_spending_ambient_quote():
    s = strategy()
    assert s.decide(market(base="0.01")).intent_type.value == "HOLD"
    assert s.get_persistent_state()["phase"] == "ready"
    assert s.get_persistent_state()["owned_base"] == "0"


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["allocation", "schema", "inventory", "phase"])
async def test_async_loader_cannot_rebuy_after_rejected_completed_state(corruption):
    from unittest.mock import AsyncMock

    saved = strategy().get_persistent_state()
    saved["phase"] = "done"
    if corruption == "allocation":
        saved["quote_allocation"] = "8"
    elif corruption == "schema":
        saved["schema_version"] = 99
    elif corruption == "inventory":
        saved["owned_base"] = "NaN"
    else:
        saved["phase"] = "invalid"
    resumed = strategy()
    resumed._deployment_id = "deployment:restore-validation"
    resumed._state_manager = SimpleNamespace(
        load_state=AsyncMock(return_value=SimpleNamespace(state=saved, version=1))
    )
    assert await resumed.load_state_async() is False
    assert resumed.decide(market()).intent_type.value == "HOLD"
    assert resumed.get_persistent_state() == saved
    with pytest.raises(RuntimeError, match="Reconcile"):
        resumed.generate_teardown_intents(market=market())
    with pytest.raises(RuntimeError, match="Reconcile"):
        resumed.get_open_positions()
