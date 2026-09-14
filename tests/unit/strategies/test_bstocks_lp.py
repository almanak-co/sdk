"""Allocation and receipt ownership boundaries for the LP starter."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.execution.extracted_data import LPOpenData, SwapAmounts
from almanak.framework.execution.orchestrator import ExecutionPhase, ExecutionResult
from almanak.framework.intents import Intent
from almanak.framework.market.models import TokenBalance
from almanak.framework.teardown import TeardownAssetPolicy, TeardownMode
from strategies.bstocks_lp.strategy import BStocksLPConfig, BStocksLPStrategy


def strategy(**changes):
    changes.setdefault("quote_allocation", Decimal("6"))
    s = BStocksLPStrategy(
        config=BStocksLPConfig(**changes), chain="bsc", wallet_address="0x0000000000000000000000000000000000000001"
    )
    s.create_market_snapshot = lambda: SimpleNamespace(
        lp_position_value=lambda *args, **kwargs: SimpleNamespace(total_usd=Decimal("6"))
    )
    return s


def market(stock="100", quote="100", price="300"):
    cfg = BStocksLPConfig()
    amounts = {cfg.token0_address: Decimal(stock), cfg.token1_address: Decimal(quote)}
    now = datetime.now(UTC)
    return SimpleNamespace(
        balance=lambda token: TokenBalance(
            symbol=token, balance=amounts[token], balance_usd=Decimal("0"), address=token
        ),
        timestamp=now,
        price_data=lambda token: SimpleNamespace(
            price=Decimal(price) if token == cfg.token0_address else Decimal("1"), timestamp=now, stale=False
        ),
    )


def test_reversed_pool_labels_cannot_reassign_the_allocation_token():
    with pytest.raises(ValueError, match="symbols must match"):
        strategy(pool="USDT/GOOGLB/2500")


@pytest.mark.parametrize("field", ["pool_address", "token0_address", "token1_address"])
def test_zero_contract_identity_is_refused(field):
    with pytest.raises(ValueError, match="nonzero"):
        strategy(**{field: "0x" + "0" * 40})


def test_teardown_profile_preserves_the_exact_quote_contract():
    s = strategy()
    profile = s.get_teardown_profile()
    assert profile.preferred_asset_policy == TeardownAssetPolicy.KEEP_OUTPUTS
    assert profile.natural_exit_assets == [s.settings.token1_address]
    assert profile.recommended_target == s.settings.token1_address


def fund(s):
    buy = s.decide(market())
    s.on_intent_executed(
        buy,
        True,
        ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            swap_amounts=SwapAmounts(
                amount_in=3 * 10**18,
                amount_out=10**16,
                amount_in_decimal=Decimal("3"),
                amount_out_decimal=Decimal("0.01"),
            ),
        ),
    )
    return buy


def mint(s):
    fund(s)
    intent = s.decide(market())
    s.on_intent_executed(
        intent,
        True,
        ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            position_id=42,
            extracted_data={
                "lp_open_data": LPOpenData(
                    position_id=42,
                    pool_address="0x89001d846f7ca36ee089f73eefc25657e1798144",
                    amount0=9_000_000_000_000_000,
                    amount1=2_800_000_000_000_000_000,
                )
            },
        ),
    )
    return intent


def test_ambient_wallet_balances_never_increase_allocation():
    s = strategy()
    buy = fund(s)
    assert buy.amount == Decimal("3")
    opening = s.decide(market())
    assert opening.amount0 == Decimal("0.01")
    assert opening.amount1 == Decimal("3")
    assert (opening.range_lower, opening.range_upper) == (Decimal("270"), Decimal("330"))


def test_missing_balance_does_not_consume_entry():
    s = strategy()
    assert s.decide(market(quote="5")).intent_type.value == "HOLD"
    assert s.phase == "ready"
    assert s.decide(market()).intent_type.value == "SWAP"


@pytest.mark.parametrize("price", ["0", "-1", "NaN", "Infinity"])
def test_invalid_prices_hold_without_pending_mutation(price):
    s = strategy()
    assert s.decide(market(price=price)).intent_type.value == "HOLD"
    assert s.phase == "ready"


def test_failure_cannot_create_duplicate_entry_after_restart():
    s = strategy()
    intent = s.decide(market())
    s.on_intent_executed(intent, False, None)
    resumed = strategy()
    resumed.load_persistent_state(s.get_persistent_state())
    assert resumed.decide(market()).intent_type.value == "HOLD"
    assert resumed.pending_intent_id == intent.intent_id


def test_unrelated_receipt_does_not_claim_inventory():
    s = strategy()
    s.decide(market())
    s.on_intent_executed(Intent.swap(from_token="USDT", to_token="GOOGLB", amount=Decimal("3")), True, None)
    assert s.phase == "entry_pending"
    assert s.owned["GOOGLB"] == 0


def test_missing_mint_amounts_preserve_nft_for_teardown():
    s = strategy()
    fund(s)
    opening = s.decide(market())
    with pytest.raises(ValueError, match="Receipt ownership is unmeasured"):
        s.on_intent_executed(
            opening,
            True,
            ExecutionResult(
                success=True,
                phase=ExecutionPhase.COMPLETE,
                position_id=42,
                extracted_data={"lp_open_data": LPOpenData(position_id=42)},
            ),
        )
    assert s.position_id == "42"
    assert s.decide(market()).intent_type.value == "HOLD"
    assert s.generate_teardown_intents(TeardownMode.SOFT, market())[0].position_id == "42"


def test_restart_preserves_owned_nft_and_unused_funds():
    s = strategy()
    mint(s)
    resumed = strategy()
    resumed.load_persistent_state(s.get_persistent_state())
    assert resumed.phase == "holding"
    assert resumed.owned == {"GOOGLB": Decimal("0.001"), "USDT": Decimal("0.2")}
    assert resumed.get_open_positions().positions[0].position_id == "42"
    assert resumed.decide(market(price="1000")).intent_type.value == "HOLD"


def test_teardown_uses_explicit_withdrawal_policy_without_market_dependency():
    s = strategy(withdrawal_max_slippage=Decimal("0.8"))
    mint(s)
    close, swap = s.generate_teardown_intents(TeardownMode.SOFT, market())
    assert close.max_slippage == Decimal("0.8")
    assert close.position_id == "42"
    assert swap.amount == "all"
    assert len(s.generate_teardown_intents(TeardownMode.SOFT)) == 2


def test_configuration_change_on_resume_is_refused():
    s = strategy()
    fund(s)
    with pytest.raises(ValueError, match="configuration differs"):
        strategy(quote_allocation=Decimal("60")).load_persistent_state(s.get_persistent_state())


@pytest.mark.parametrize(
    "changes",
    [
        {"quote_allocation": "0"},
        {"swap_fraction": "1"},
        {"range_width_fraction": "2"},
        {"withdrawal_max_slippage": "1"},
    ],
)
def test_invalid_allocation_configuration_refused(changes):
    with pytest.raises(ValueError):
        strategy(**changes)


def test_mint_without_payload_retains_top_level_nft():
    s = strategy()
    fund(s)
    opening = s.decide(market())
    with pytest.raises(ValueError, match="Receipt ownership is unmeasured"):
        s.on_intent_executed(opening, True, ExecutionResult(success=True, phase=ExecutionPhase.COMPLETE, position_id=42))
    assert s.position_id == "42"
    assert s.phase == "mint_pending"
    assert s.generate_teardown_intents(TeardownMode.SOFT, market())[0].position_id == "42"


def test_unmeasured_close_does_not_erase_position():
    s = strategy()
    mint(s)
    close = s.generate_teardown_intents(TeardownMode.SOFT, market())[0]
    with pytest.raises(ValueError, match="Receipt ownership is unmeasured"):
        s.on_intent_executed(close, True, None)
    assert s.position_id == "42"
    assert s.phase == "holding"


def test_unmeasured_live_valuation_is_not_reported_as_zero():
    s = strategy()
    mint(s)
    s.create_market_snapshot = lambda: SimpleNamespace(lp_position_value=lambda *args, **kwargs: None)
    with pytest.raises(ValueError, match="valuation is unmeasured"):
        s.get_open_positions()
    assert s.generate_teardown_intents(TeardownMode.SOFT, market())[0].position_id == "42"


def test_restore_refusal_cannot_leave_new_entry_enabled():
    s = strategy()
    fund(s)
    target = strategy(quote_allocation=Decimal("60"))
    with pytest.raises(ValueError):
        target.load_persistent_state(s.get_persistent_state())
    assert target.decide(market()).intent_type.value == "HOLD"


def test_missing_nft_pending_mint_does_not_claim_empty_teardown():
    s = strategy()
    fund(s)
    s.decide(market())
    with pytest.raises(ValueError, match="Execution outcome is unmeasured"):
        s.get_open_positions()


def test_gateway_result_uses_typed_extracted_mint_contract():
    from almanak.framework.execution.gateway_orchestrator import GatewayExecutionResult

    s = strategy()
    fund(s)
    opening = s.decide(market())
    result = GatewayExecutionResult(success=True, tx_hashes=[], total_gas_used=0, receipts=[], execution_id="unit")
    result.position_id = 42
    result.extracted_data["lp_open_data"] = LPOpenData(
        position_id=42,
        amount0=9_000_000_000_000_000,
        amount1=2_800_000_000_000_000_000,
        pool_address=s.settings.pool_address,
    )
    assert not hasattr(result, "lp_open_data")
    s.on_intent_executed(opening, True, result)
    assert s.phase == "holding"
    assert s.position_id == "42"
    assert s.owned["GOOGLB"] == Decimal("0.001")


def test_entry_and_exit_pin_compiler_pool_and_fee_keys():
    s = strategy()
    buy = fund(s)
    expected = {"pool": s.settings.pool_address, "fee_tier": 2500}
    assert buy.swap_params == expected
    assert s.generate_teardown_intents(TeardownMode.SOFT, market())[-1].swap_params == expected


def test_validated_recovered_entry_clears_old_diagnostic():
    s = strategy()
    entry = s.decide(market())
    s.on_intent_executed(entry, False, None)
    assert s.problem
    s.on_intent_executed(
        entry,
        True,
        ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            swap_amounts=SwapAmounts(
                amount_in=3 * 10**18,
                amount_out=10**16,
                amount_in_decimal=Decimal("3"),
                amount_out_decimal=Decimal("0.01"),
            ),
        ),
    )
    assert s.phase == "funded"
    assert s.problem is None


def test_captured_fork_mint_recovery_clears_diagnostic_without_losing_ownership():
    s = strategy(quote_allocation=Decimal("7"))
    s.phase = "funded"
    s.owned = {"GOOGLB": Decimal("0.01"), "USDT": Decimal("3.5")}
    opening = s.decide(market())
    with pytest.raises(ValueError, match="Receipt ownership is unmeasured"):
        s.on_intent_executed(
            opening,
            True,
            ExecutionResult(
                success=True,
                phase=ExecutionPhase.COMPLETE,
                position_id=7429518,
            ),
        )
    assert s.problem
    # Captured typed LP payload from the managed BSC fork, without transaction transport metadata.
    payload = LPOpenData(
        position_id=7429518,
        tick_lower=57150,
        tick_upper=59150,
        liquidity=3669909805018836683,
        amount0=9111716642966999,
        amount1=3499999999999999666,
        current_tick=58216,
        pool_address="0x89001d846f7ca36ee089f73eefc25657e1798144",
        currency0="0x3f53de71c126bdabae20f9cd64848d317f6c3238",
        currency1="0x55d398326f99059ff775485246999027b3197955",
    )
    s.on_intent_executed(
        opening,
        True,
        ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            position_id=7429518,
            extracted_data={"lp_open_data": payload},
        ),
    )
    assert s.phase == "holding"
    assert s.problem is None
    assert s.position_id == "7429518"
    assert s.owned == {"GOOGLB": Decimal("0.000888283357033001"), "USDT": Decimal("0.000000000000000334")}


def test_unknown_callback_does_not_clear_existing_diagnostic():
    s = strategy()
    entry = s.decide(market())
    s.on_intent_executed(entry, False, None)
    problem = s.problem
    wrong = entry.model_copy(update={"intent_id": "unrelated"})
    s.on_intent_executed(wrong, True, None)
    assert s.problem == problem
    assert s.pending_intent_id == entry.intent_id
    s.on_intent_executed(Intent.hold(reason="unrelated callback"), True, None)
    assert s.problem == problem
    assert s.pending_intent_id == entry.intent_id


def test_validated_close_clears_prior_missing_receipt_diagnostic():
    from almanak.framework.execution.extracted_data import LPCloseData

    s = strategy()
    mint(s)
    close = s.generate_teardown_intents(TeardownMode.SOFT, market())[0]
    with pytest.raises(ValueError, match="Receipt ownership is unmeasured"):
        s.on_intent_executed(close, True, None)
    assert s.problem
    s.on_intent_executed(
        close,
        True,
        ExecutionResult(
            success=True,
            phase=ExecutionPhase.COMPLETE,
            lp_close_data=LPCloseData(amount0_collected=0, amount1_collected=1),
        ),
    )
    assert s.phase == "closed"
    assert s.position_id is None
    assert s.problem is None
