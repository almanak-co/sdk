"""Unit tests for `almanak.framework.intents.vocabulary`.

Covers every model, validator, factory method, and helper function in
vocabulary.py. Complements the per-intent tests in this directory by
stressing the central entry points (Intent.* factories, IntentSequence,
Intent.serialize_result / deserialize_result, normalize_decide_result,
get_amount_field, set_resolved_amount, validate_chain, etc.).

The goal is to exercise validator branches (both success and failure)
and edge-case coercions without reaching into the compiler or any
gateway-backed infrastructure.
"""

from datetime import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.intents.intent_errors import (
    InvalidAmountError,
    InvalidChainError,
    InvalidSequenceError,
)
from almanak.framework.intents.lending_intents import (
    BorrowIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from almanak.framework.intents.vocabulary import (
    PROTOCOL_CAPABILITIES,
    CollectFeesIntent,
    HoldIntent,
    Intent,
    IntentSequence,
    IntentType,
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
)

# =============================================================================
# SwapIntent validators
# =============================================================================


class TestSwapIntentValidators:
    def test_default_slippage_and_amount_usd(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount_usd=Decimal("1000"))
        assert intent.max_slippage == Decimal("0.005")
        assert intent.amount is None
        assert intent.intent_type == IntentType.SWAP
        assert intent.is_chained_amount is False
        assert intent.is_cross_chain is False

    def test_amount_all_sets_chained(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")
        assert intent.is_chained_amount is True

    def test_amount_integer_coerces_to_decimal(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=5)
        assert intent.amount == Decimal("5")
        assert isinstance(intent.amount, Decimal)

    def test_amount_string_numeric_coerces_to_decimal(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="2.5")
        assert intent.amount == Decimal("2.5")

    def test_amount_float_rejected(self):
        with pytest.raises(ValidationError, match="Float values are not allowed"):
            SwapIntent(from_token="USDC", to_token="ETH", amount=1.5)

    def test_amount_usd_float_rejected(self):
        with pytest.raises(ValidationError, match="Float values are not allowed"):
            SwapIntent(from_token="USDC", to_token="ETH", amount_usd=1.5)

    def test_missing_both_amounts_raises(self):
        with pytest.raises(ValidationError, match="Either amount_usd or amount must be provided"):
            SwapIntent(from_token="USDC", to_token="ETH")

    def test_both_amounts_raises(self):
        with pytest.raises(ValidationError, match="Only one of amount_usd or amount"):
            SwapIntent(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("100"),
                amount=Decimal("1"),
            )

    @pytest.mark.parametrize("bad_usd", [Decimal("0"), Decimal("-1")])
    def test_non_positive_amount_usd_raises(self, bad_usd):
        with pytest.raises(ValidationError, match="amount_usd must be positive"):
            SwapIntent(from_token="USDC", to_token="ETH", amount_usd=bad_usd)

    @pytest.mark.parametrize("bad_amount", [Decimal("0"), Decimal("-0.1")])
    def test_non_positive_amount_raises(self, bad_amount):
        with pytest.raises(ValidationError, match="amount must be positive"):
            SwapIntent(from_token="USDC", to_token="ETH", amount=bad_amount)

    def test_invalid_amount_string_raises(self):
        # The base ChainedAmount validator rejects anything that isn't a
        # valid decimal literal or the "all" sentinel.
        with pytest.raises(ValidationError, match="invalid number format"):
            SwapIntent(from_token="USDC", to_token="ETH", amount="maximum")

    @pytest.mark.parametrize("bad_slippage", [Decimal("-0.01"), Decimal("1.5")])
    def test_slippage_out_of_bounds(self, bad_slippage):
        with pytest.raises(ValidationError, match="max_slippage must be between 0 and 1"):
            SwapIntent(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("100"),
                max_slippage=bad_slippage,
            )

    @pytest.mark.parametrize("bad_impact", [Decimal("0"), Decimal("-0.1"), Decimal("1.5")])
    def test_max_price_impact_out_of_bounds(self, bad_impact):
        with pytest.raises(ValidationError, match="max_price_impact must be between"):
            SwapIntent(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("100"),
                max_price_impact=bad_impact,
            )

    def test_max_price_impact_upper_bound_inclusive(self):
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount_usd=Decimal("100"),
            max_price_impact=Decimal("1"),
        )
        assert intent.max_price_impact == Decimal("1")

    def test_cross_chain_without_aggregator_protocol(self):
        with pytest.raises(ValidationError, match="Cross-chain swaps require protocol='enso' or protocol='lifi'"):
            SwapIntent(
                from_token="USDC",
                to_token="WETH",
                amount_usd=Decimal("1000"),
                chain="base",
                destination_chain="arbitrum",
                protocol="uniswap_v3",
            )

    @pytest.mark.parametrize("agg", ["enso", "Enso", "lifi", "LIFI"])
    def test_cross_chain_with_aggregator_ok(self, agg):
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            chain="base",
            destination_chain="arbitrum",
            protocol=agg,
        )
        assert intent.is_cross_chain is True
        assert intent.protocol == agg

    def test_is_cross_chain_same_chain_is_false(self):
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            chain="base",
            destination_chain="base",
        )
        assert intent.is_cross_chain is False

    def test_cross_chain_without_source_chain_is_false(self):
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount_usd=Decimal("1000"),
            destination_chain="arbitrum",
        )
        assert intent.is_cross_chain is False

    def test_serialize_preserves_all_literal(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")
        data = intent.serialize()
        assert data["type"] == "SWAP"
        assert data["amount"] == "all"

    def test_serialize_deserialize_roundtrip(self):
        intent = SwapIntent(
            from_token="USDC",
            to_token="ETH",
            amount=Decimal("1.5"),
            max_slippage=Decimal("0.01"),
            chain="arbitrum",
        )
        data = intent.serialize()
        rebuilt = SwapIntent.deserialize(data)
        assert rebuilt.from_token == "USDC"
        assert rebuilt.to_token == "ETH"
        assert rebuilt.amount == Decimal("1.5")
        assert rebuilt.max_slippage == Decimal("0.01")
        assert rebuilt.chain == "arbitrum"
        assert rebuilt.created_at == intent.created_at

    def test_deserialize_accepts_iso_created_at(self):
        payload = {
            "type": "SWAP",
            "from_token": "USDC",
            "to_token": "ETH",
            "amount_usd": "100",
            "created_at": "2024-01-01T00:00:00+00:00",
            "intent_id": "abc",
        }
        intent = SwapIntent.deserialize(payload)
        assert intent.amount_usd == Decimal("100")
        assert isinstance(intent.created_at, datetime)

    def test_model_forbids_extra_fields(self):
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"), unknown="x")


# =============================================================================
# Intent.swap factory
# =============================================================================


class TestIntentSwapFactory:
    def test_swap_factory_usd(self):
        intent = Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))
        assert isinstance(intent, SwapIntent)
        assert intent.amount_usd == Decimal("1000")
        assert intent.max_slippage == Decimal("0.005")

    def test_swap_factory_with_all(self):
        intent = Intent.swap("ETH", "USDC", amount="all", chain="base")
        assert intent.amount == "all"
        assert intent.is_chained_amount is True

    def test_swap_factory_cross_chain_enso(self):
        intent = Intent.swap(
            "USDC",
            "WETH",
            amount_usd=Decimal("500"),
            chain="base",
            destination_chain="arbitrum",
            protocol="enso",
        )
        assert intent.is_cross_chain is True
        assert intent.protocol == "enso"

    def test_swap_factory_with_price_impact(self):
        intent = Intent.swap(
            "USDC",
            "ETH",
            amount_usd=Decimal("500"),
            max_price_impact=Decimal("0.5"),
        )
        assert intent.max_price_impact == Decimal("0.5")


# =============================================================================
# LPOpenIntent / LPCloseIntent
# =============================================================================


class TestLPOpenIntent:
    def _kwargs(self, **overrides):
        base = {
            "pool": "0x" + "a" * 40,
            "amount0": Decimal("1"),
            "amount1": Decimal("1000"),
            "range_lower": Decimal("1800"),
            "range_upper": Decimal("2200"),
        }
        base.update(overrides)
        return base

    def test_construct_defaults(self):
        intent = LPOpenIntent(**self._kwargs())
        assert intent.protocol == "uniswap_v3"
        assert intent.intent_type == IntentType.LP_OPEN
        assert intent.protocol_params is None

    def test_factory_matches_direct(self):
        a = LPOpenIntent(**self._kwargs())
        b = Intent.lp_open(
            pool=a.pool,
            amount0=a.amount0,
            amount1=a.amount1,
            range_lower=a.range_lower,
            range_upper=a.range_upper,
        )
        assert a.pool == b.pool
        assert a.amount0 == b.amount0

    def test_negative_amount0_raises(self):
        with pytest.raises(ValidationError, match="amount0 must be non-negative"):
            LPOpenIntent(**self._kwargs(amount0=Decimal("-1")))

    def test_negative_amount1_raises(self):
        with pytest.raises(ValidationError, match="amount1 must be non-negative"):
            LPOpenIntent(**self._kwargs(amount1=Decimal("-1")))

    def test_both_amounts_zero_raises(self):
        with pytest.raises(ValidationError, match="At least one amount must be positive"):
            LPOpenIntent(**self._kwargs(amount0=Decimal("0"), amount1=Decimal("0")))

    def test_single_sided_allowed(self):
        # One side zero is fine as long as the other is positive.
        intent = LPOpenIntent(**self._kwargs(amount1=Decimal("0")))
        assert intent.amount1 == Decimal("0")
        assert intent.amount0 > 0

    def test_range_inverted_raises(self):
        with pytest.raises(ValidationError, match="range_lower must be less than range_upper"):
            LPOpenIntent(**self._kwargs(range_lower=Decimal("2200"), range_upper=Decimal("1800")))

    def test_range_equal_raises(self):
        with pytest.raises(ValidationError, match="range_lower must be less than range_upper"):
            LPOpenIntent(**self._kwargs(range_lower=Decimal("2000"), range_upper=Decimal("2000")))

    def test_non_positive_range_lower_raises(self):
        with pytest.raises(ValidationError, match="range_lower must be positive"):
            LPOpenIntent(**self._kwargs(range_lower=Decimal("0"), range_upper=Decimal("1")))

    def test_protocol_params_bin_range_valid(self):
        intent = LPOpenIntent(**self._kwargs(protocol_params={"bin_range": 10}))
        assert intent.protocol_params == {"bin_range": 10}

    @pytest.mark.parametrize("bad_bin", [0, 101, True, "10"])
    def test_protocol_params_bin_range_invalid(self, bad_bin):
        with pytest.raises(ValidationError, match="bin_range must be an integer between 1 and 100"):
            LPOpenIntent(**self._kwargs(protocol_params={"bin_range": bad_bin}))

    def test_serialize_roundtrip(self):
        intent = LPOpenIntent(**self._kwargs(chain="arbitrum"))
        data = intent.serialize()
        assert data["type"] == "LP_OPEN"
        rebuilt = LPOpenIntent.deserialize(data)
        assert rebuilt.pool == intent.pool
        assert rebuilt.created_at == intent.created_at


class TestLPCloseIntent:
    def test_construct_defaults(self):
        intent = LPCloseIntent(position_id="42")
        assert intent.collect_fees is True
        assert intent.protocol == "uniswap_v3"
        assert intent.intent_type == IntentType.LP_CLOSE

    def test_factory(self):
        intent = Intent.lp_close(
            position_id="42",
            pool="0xpool",
            collect_fees=False,
            protocol="uniswap_v4",
            chain="base",
            protocol_params={"liquidity": 1},
        )
        assert intent.collect_fees is False
        assert intent.protocol == "uniswap_v4"
        assert intent.protocol_params == {"liquidity": 1}

    def test_serialize_roundtrip(self):
        intent = LPCloseIntent(position_id="42", pool="0xpool", chain="arbitrum")
        data = intent.serialize()
        assert data["type"] == "LP_CLOSE"
        rebuilt = LPCloseIntent.deserialize(data)
        assert rebuilt.position_id == "42"
        assert rebuilt.chain == "arbitrum"


# =============================================================================
# CollectFeesIntent
# =============================================================================


class TestCollectFeesIntent:
    def test_construct_requires_protocol(self):
        # VIB-4468 W6 — protocol no longer defaults to "traderjoe_v2"
        # on the CollectFeesIntent model. The old default was a silent
        # mis-routing footgun on multi-protocol strategies.
        with pytest.raises(ValidationError, match="protocol"):
            CollectFeesIntent(pool="WAVAX/USDC/20")  # type: ignore[call-arg]

    def test_construct_with_protocol(self):
        intent = CollectFeesIntent(pool="WAVAX/USDC/20", protocol="traderjoe_v2")
        assert intent.protocol == "traderjoe_v2"
        assert intent.intent_type == IntentType.LP_COLLECT_FEES

    def test_factory(self):
        intent = Intent.collect_fees(pool="WAVAX/USDC/20", protocol="traderjoe_v2", chain="avalanche")
        assert intent.pool == "WAVAX/USDC/20"
        assert intent.chain == "avalanche"

    def test_empty_pool_raises(self):
        with pytest.raises(ValidationError, match="pool is required"):
            CollectFeesIntent(pool="", protocol="traderjoe_v2")

    def test_serialize_roundtrip(self):
        intent = CollectFeesIntent(pool="WAVAX/USDC/20", protocol="traderjoe_v2", chain="avalanche")
        data = intent.serialize()
        assert data["type"] == "LP_COLLECT_FEES"
        rebuilt = CollectFeesIntent.deserialize(data)
        assert rebuilt.pool == intent.pool
        assert rebuilt.chain == intent.chain


# =============================================================================
# HoldIntent
# =============================================================================


class TestHoldIntent:
    def test_empty_hold(self):
        intent = HoldIntent()
        assert intent.reason is None
        assert intent.reason_code is None
        assert intent.chain is None
        assert intent.intent_type == IntentType.HOLD

    def test_factory_with_reason(self):
        intent = Intent.hold(reason="waiting", reason_code="RSI_NEUTRAL", reason_details={"rsi": "52"})
        assert intent.reason == "waiting"
        assert intent.reason_code == "RSI_NEUTRAL"
        assert intent.reason_details == {"rsi": "52"}

    def test_serialize_roundtrip(self):
        intent = HoldIntent(reason="test", chain="base")
        data = intent.serialize()
        assert data["type"] == "HOLD"
        rebuilt = HoldIntent.deserialize(data)
        assert rebuilt.reason == "test"
        assert rebuilt.chain == "base"


# =============================================================================
# BorrowIntent / RepayIntent (lending)
# =============================================================================


class TestBorrowIntentValidators:
    def _kwargs(self, **overrides):
        # Default to the now-canonical standalone-borrow form
        # (collateral_amount == 0). Bundled collateral (> 0 or "all") is rejected
        # by BorrowIntent's validator; collateral_token remains metadata only.
        base = {
            "protocol": "aave_v3",
            "collateral_token": "WETH",
            "collateral_amount": Decimal("0"),
            "borrow_token": "USDC",
            "borrow_amount": Decimal("1000"),
        }
        base.update(overrides)
        return base

    def test_ok_variable_rate(self):
        intent = BorrowIntent(**self._kwargs(interest_rate_mode="variable"))
        assert intent.intent_type == IntentType.BORROW
        assert intent.is_chained_amount is False

    def test_collateral_all_bundled_rejected(self):
        # Bundled chained collateral ("all") is now rejected at construction:
        # the accounting layer cannot record the implicit on-chain supply as a
        # distinct SUPPLY event.
        with pytest.raises(ValidationError, match="Bundled collateralized borrow is not supported"):
            BorrowIntent(**self._kwargs(collateral_amount="all"))

    def test_negative_collateral_raises(self):
        with pytest.raises(ValidationError, match="collateral_amount must be non-negative"):
            BorrowIntent(**self._kwargs(collateral_amount=Decimal("-1")))

    def test_borrow_amount_non_positive_raises(self):
        with pytest.raises(ValidationError, match="borrow_amount must be positive"):
            BorrowIntent(**self._kwargs(borrow_amount=Decimal("0")))

    def test_morpho_without_market_id_raises(self):
        with pytest.raises(ValidationError, match="requires market_id"):
            BorrowIntent(**self._kwargs(protocol="morpho_blue"))

    def test_compound_v3_rejects_rate_mode(self):
        with pytest.raises(ValidationError, match="does not support interest rate mode"):
            BorrowIntent(**self._kwargs(protocol="compound_v3", interest_rate_mode="variable"))

    def test_rate_mode_not_in_valid_modes(self):
        # Craft a capability that supports rate mode but has no "variable" entry.
        original = PROTOCOL_CAPABILITIES["aave_v3"]["interest_rate_modes"]
        PROTOCOL_CAPABILITIES["aave_v3"]["interest_rate_modes"] = ["other"]
        try:
            with pytest.raises(ValidationError, match="Valid modes for 'aave_v3'"):
                BorrowIntent(**self._kwargs(interest_rate_mode="variable"))
        finally:
            PROTOCOL_CAPABILITIES["aave_v3"]["interest_rate_modes"] = original

    def test_factory(self):
        # The factory routes through validation, so it uses the canonical
        # standalone-borrow form (collateral_amount == 0).
        intent = Intent.borrow(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("1000"),
            interest_rate_mode="variable",
            chain="arbitrum",
        )
        assert intent.protocol == "aave_v3"
        assert intent.borrow_amount == Decimal("1000")

    def test_factory_bundled_collateral_rejected(self):
        # A bundled (nonzero) collateral through the factory must now raise,
        # since the factory routes through BorrowIntent validation.
        with pytest.raises(ValidationError, match="Bundled collateralized borrow is not supported"):
            Intent.borrow(
                protocol="aave_v3",
                collateral_token="WETH",
                collateral_amount=Decimal("1"),
                borrow_token="USDC",
                borrow_amount=Decimal("1000"),
                interest_rate_mode="variable",
                chain="arbitrum",
            )

    def test_serialize_preserves_all(self):
        # The "all"-form intent is rejected by the validator, so build the
        # fixture via model_construct; serialize operates on an already-
        # constructed intent and must still emit the "all" literal.
        intent = BorrowIntent.model_construct(**self._kwargs(collateral_amount="all"))
        data = intent.serialize()
        assert data["collateral_amount"] == "all"
        # deserialize re-runs validation, which now rejects bundled collateral,
        # so re-hydrating an "all"-form borrow raises rather than round-tripping.
        with pytest.raises(ValidationError, match="Bundled collateralized borrow is not supported"):
            BorrowIntent.deserialize(data)


class TestRepayIntentValidators:
    def test_repay_full_factory_no_amount(self):
        intent = Intent.repay(protocol="aave_v3", token="USDC", repay_full=True)
        assert intent.repay_full is True
        assert intent.amount == Decimal("0")

    def test_repay_not_full_missing_amount_raises(self):
        with pytest.raises(ValueError, match="amount is required when repay_full=False"):
            Intent.repay(protocol="aave_v3", token="USDC")

    def test_repay_negative_raises(self):
        with pytest.raises(ValidationError, match="amount must be positive when not repaying full"):
            RepayIntent(protocol="aave_v3", token="USDC", amount=Decimal("-1"))

    def test_repay_zero_without_full_raises(self):
        with pytest.raises(ValidationError, match="amount must be positive"):
            RepayIntent(protocol="aave_v3", token="USDC", amount=Decimal("0"))

    def test_repay_full_zero_amount_ok(self):
        intent = RepayIntent(protocol="aave_v3", token="USDC", amount=Decimal("0"), repay_full=True)
        assert intent.repay_full is True

    def test_repay_all_serializes(self):
        intent = RepayIntent(protocol="aave_v3", token="USDC", amount="all")
        data = intent.serialize()
        assert data["amount"] == "all"

    def test_repay_morpho_without_market_id_raises(self):
        with pytest.raises(ValidationError, match="requires market_id"):
            RepayIntent(protocol="morpho_blue", token="USDC", amount=Decimal("100"))


# =============================================================================
# SupplyIntent / WithdrawIntent
# =============================================================================


class TestSupplyWithdrawIntents:
    def test_supply_factory(self):
        intent = Intent.supply(protocol="aave_v3", token="WETH", amount=Decimal("1"))
        assert isinstance(intent, SupplyIntent)

    def test_supply_amount_zero_raises(self):
        with pytest.raises(ValidationError, match="amount must be positive"):
            SupplyIntent(protocol="aave_v3", token="USDC", amount=Decimal("0"))

    def test_supply_morpho_collateral_toggle_off_ok(self):
        # morpho supports collateral toggle per capabilities table
        intent = SupplyIntent(
            protocol="morpho_blue",
            token="USDC",
            amount=Decimal("100"),
            use_as_collateral=False,
            market_id="0x" + "b" * 64,
        )
        assert intent.use_as_collateral is False

    def test_supply_silo_v2_rejects_collateral_toggle_off(self):
        with pytest.raises(ValidationError, match="does not support disabling collateral"):
            SupplyIntent(
                protocol="silo_v2",
                token="USDC",
                amount=Decimal("100"),
                use_as_collateral=False,
            )

    def test_withdraw_factory(self):
        intent = Intent.withdraw(
            protocol="morpho_blue",
            token="USDC",
            amount=Decimal("100"),
            market_id="0x" + "a" * 64,
            is_collateral=False,
        )
        assert isinstance(intent, WithdrawIntent)
        assert intent.is_collateral is False

    def test_withdraw_zero_without_flag_raises(self):
        with pytest.raises(ValidationError, match="amount must be positive when not withdrawing all"):
            WithdrawIntent(protocol="aave_v3", token="USDC", amount=Decimal("0"))

    def test_withdraw_all_flag_allows_zero(self):
        intent = WithdrawIntent(
            protocol="aave_v3",
            token="USDC",
            amount=Decimal("0"),
            withdraw_all=True,
        )
        assert intent.withdraw_all is True

    def test_withdraw_morpho_without_market_id_raises(self):
        with pytest.raises(ValidationError, match="requires market_id"):
            WithdrawIntent(protocol="morpho_blue", token="USDC", amount=Decimal("100"))


# =============================================================================
# Perp / Flash-loan / Stake / Unstake / Vault / Wrap factories
# =============================================================================


class TestMiscFactories:
    def test_perp_open_factory(self):
        intent = Intent.perp_open(
            market="ETH/USD",
            collateral_token="WETH",
            collateral_amount=Decimal("0.1"),
            size_usd=Decimal("1000"),
            leverage=Decimal("5"),
            protocol="gmx_v2",
            chain="arbitrum",
        )
        assert intent.intent_type == IntentType.PERP_OPEN

    def test_perp_close_factory(self):
        intent = Intent.perp_close(
            market="ETH/USD",
            collateral_token="WETH",
            is_long=True,
            protocol="gmx_v2",
        )
        assert intent.intent_type == IntentType.PERP_CLOSE

    def test_stake_factory(self):
        intent = Intent.stake(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1"),
            chain="ethereum",
        )
        assert intent.intent_type == IntentType.STAKE

    def test_unstake_factory(self):
        intent = Intent.unstake(
            protocol="lido",
            token_in="wstETH",
            amount=Decimal("1"),
            chain="ethereum",
        )
        assert intent.intent_type == IntentType.UNSTAKE

    def test_wrap_factory(self):
        intent = Intent.wrap(token="WETH", amount=Decimal("0.01"), chain="arbitrum")
        assert intent.intent_type == IntentType.WRAP_NATIVE

    def test_unwrap_factory(self):
        intent = Intent.unwrap(token="WETH", amount=Decimal("0.01"), chain="arbitrum")
        assert intent.intent_type == IntentType.UNWRAP_NATIVE

    def test_vault_deposit_factory(self):
        intent = Intent.vault_deposit(
            protocol="metamorpho",
            vault_address="0x" + "c" * 40,
            amount=Decimal("1000"),
            deposit_token="USDC",
            chain="ethereum",
        )
        assert intent.intent_type == IntentType.VAULT_DEPOSIT

    def test_vault_redeem_factory(self):
        intent = Intent.vault_redeem(
            protocol="metamorpho",
            vault_address="0x" + "c" * 40,
            shares="all",
            chain="ethereum",
        )
        assert intent.intent_type == IntentType.VAULT_REDEEM

    def test_bridge_factory_returns_bridge_intent(self):
        bridge = Intent.bridge(
            token="USDC",
            amount=Decimal("100"),
            from_chain="base",
            to_chain="arbitrum",
        )
        assert type(bridge).__name__ == "BridgeIntent"

    def test_ensure_balance_factory(self):
        eb = Intent.ensure_balance(
            token="USDC",
            min_amount=Decimal("1000"),
            target_chain="arbitrum",
            max_slippage=Decimal("0.01"),
            preferred_bridge="across",
        )
        assert type(eb).__name__ == "EnsureBalanceIntent"

    def test_flash_loan_factory(self):
        callback = SwapIntent(from_token="USDC", to_token="WETH", amount=Decimal("100000"))
        intent = Intent.flash_loan(
            provider="aave",
            token="USDC",
            amount=Decimal("100000"),
            callback_intents=[callback],
            chain="ethereum",
        )
        assert intent.intent_type == IntentType.FLASH_LOAN
        assert len(intent.callback_intents) == 1

    def test_prediction_buy_factory(self):
        intent = Intent.prediction_buy(
            market_id="slug",
            outcome="YES",
            amount_usd=Decimal("10"),
        )
        assert intent.intent_type == IntentType.PREDICTION_BUY

    def test_prediction_sell_factory(self):
        intent = Intent.prediction_sell(
            market_id="slug",
            outcome="YES",
            shares="all",
        )
        assert intent.intent_type == IntentType.PREDICTION_SELL

    def test_prediction_redeem_factory(self):
        intent = Intent.prediction_redeem(market_id="slug")
        assert intent.intent_type == IntentType.PREDICTION_REDEEM


# =============================================================================
# IntentSequence
# =============================================================================


def _swap(amount: Decimal | str = Decimal("1")) -> SwapIntent:
    return SwapIntent(from_token="USDC", to_token="ETH", amount=amount)


class TestIntentSequence:
    def test_empty_sequence_raises(self):
        with pytest.raises(InvalidSequenceError):
            IntentSequence(intents=[])

    def test_len_iter_getitem(self):
        seq = IntentSequence(intents=[_swap(), _swap(Decimal("2"))])
        assert len(seq) == 2
        assert seq[0].amount == Decimal("1")
        assert [i.amount for i in seq] == [Decimal("1"), Decimal("2")]

    def test_first_last_properties(self):
        a = _swap(Decimal("1"))
        b = _swap(Decimal("2"))
        seq = IntentSequence(intents=[a, b])
        assert seq.first is a
        assert seq.last is b

    def test_factory_sequence(self):
        seq = Intent.sequence(
            intents=[_swap(), _swap(Decimal("2"))],
            description="demo",
        )
        assert len(seq) == 2
        assert seq.description == "demo"

    def test_serialize_roundtrip(self):
        seq = IntentSequence(intents=[_swap()], description="demo")
        data = seq.serialize()
        assert data["type"] == "SEQUENCE"
        rebuilt = IntentSequence.deserialize(data)
        assert rebuilt.description == "demo"
        assert len(rebuilt) == 1
        assert rebuilt.sequence_id == seq.sequence_id

    def test_deserialize_missing_created_at_defaults_to_now(self):
        inner = _swap().serialize()
        data = {"intents": [inner], "description": None}
        rebuilt = IntentSequence.deserialize(data)
        assert isinstance(rebuilt.created_at, datetime)


# =============================================================================
# Intent helpers: serialize / deserialize / get_type / get_chain
# =============================================================================


class TestIntentTopLevelHelpers:
    def test_serialize_and_deserialize_swap(self):
        intent = _swap()
        data = Intent.serialize(intent)
        assert data["type"] == "SWAP"
        rebuilt = Intent.deserialize(data)
        assert isinstance(rebuilt, SwapIntent)
        assert rebuilt.from_token == "USDC"

    def test_deserialize_each_known_type(self):
        candidates = [
            SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1")),
            HoldIntent(reason="wait"),
            LPOpenIntent(
                pool="0x" + "a" * 40,
                amount0=Decimal("1"),
                amount1=Decimal("1"),
                range_lower=Decimal("1"),
                range_upper=Decimal("2"),
            ),
            LPCloseIntent(position_id="1"),
            CollectFeesIntent(pool="WAVAX/USDC/20", protocol="traderjoe_v2"),
            BorrowIntent(
                protocol="aave_v3",
                collateral_token="WETH",
                collateral_amount=Decimal("0"),
                borrow_token="USDC",
                borrow_amount=Decimal("100"),
            ),
            RepayIntent(protocol="aave_v3", token="USDC", amount=Decimal("1")),
            SupplyIntent(protocol="aave_v3", token="USDC", amount=Decimal("1")),
            WithdrawIntent(protocol="aave_v3", token="USDC", amount=Decimal("1")),
        ]
        for intent in candidates:
            rebuilt = Intent.deserialize(intent.serialize())
            assert type(rebuilt).__name__ == type(intent).__name__

    def test_deserialize_bridge(self):
        bridge = Intent.bridge(token="USDC", amount=Decimal("100"), from_chain="base", to_chain="arbitrum")
        data = bridge.serialize()
        rebuilt = Intent.deserialize(data)
        assert type(rebuilt).__name__ == "BridgeIntent"

    def test_deserialize_ensure_balance(self):
        eb = Intent.ensure_balance(token="USDC", min_amount=Decimal("1"), target_chain="arbitrum")
        data = eb.serialize()
        rebuilt = Intent.deserialize(data)
        assert type(rebuilt).__name__ == "EnsureBalanceIntent"

    def test_deserialize_missing_type_raises(self):
        with pytest.raises(ValueError, match="Missing 'type' field"):
            Intent.deserialize({"from_token": "USDC"})

    def test_deserialize_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown intent type"):
            Intent.deserialize({"type": "NOPE"})

    def test_get_type(self):
        intent = _swap()
        assert Intent.get_type(intent) == IntentType.SWAP

    def test_get_chain_none_by_default(self):
        assert Intent.get_chain(_swap()) is None

    def test_get_chain_returns_value(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"), chain="base")
        assert Intent.get_chain(intent) == "base"

    def test_is_sequence(self):
        assert Intent.is_sequence(_swap()) is False
        seq = IntentSequence(intents=[_swap()])
        assert Intent.is_sequence(seq) is True


# =============================================================================
# validate_chain
# =============================================================================


class TestValidateChain:
    def test_no_configured_chains_raises(self):
        with pytest.raises(ValueError, match="No chains configured"):
            Intent.validate_chain(_swap(), configured_chains=[])

    def test_intent_chain_in_configured(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"), chain="Arbitrum")
        resolved = Intent.validate_chain(intent, ["arbitrum", "base"])
        assert resolved == "arbitrum"

    def test_intent_chain_not_configured_raises(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"), chain="polygon")
        with pytest.raises(InvalidChainError):
            Intent.validate_chain(intent, ["arbitrum", "base"])

    def test_default_chain_used_when_intent_has_none(self):
        resolved = Intent.validate_chain(_swap(), ["arbitrum", "base"], default_chain="Base")
        assert resolved == "base"

    def test_default_chain_not_configured_raises(self):
        with pytest.raises(InvalidChainError):
            Intent.validate_chain(_swap(), ["arbitrum"], default_chain="base")

    def test_no_intent_chain_no_default_uses_first(self):
        resolved = Intent.validate_chain(_swap(), ["Arbitrum", "Base"])
        assert resolved == "arbitrum"


# =============================================================================
# normalize_decide_result / count_intents
# =============================================================================


class TestNormalizeAndCount:
    def test_normalize_none(self):
        assert Intent.normalize_decide_result(None) == []

    def test_normalize_single_intent(self):
        intent = _swap()
        result = Intent.normalize_decide_result(intent)
        assert result == [intent]

    def test_normalize_sequence(self):
        seq = IntentSequence(intents=[_swap()])
        assert Intent.normalize_decide_result(seq) == [seq]

    def test_normalize_list(self):
        items = [_swap(), IntentSequence(intents=[_swap()])]
        assert Intent.normalize_decide_result(items) == items

    def test_count_none(self):
        assert Intent.count_intents(None) == 0

    def test_count_single_intent(self):
        assert Intent.count_intents(_swap()) == 1

    def test_count_sequence_counts_inner(self):
        seq = IntentSequence(intents=[_swap(), _swap(Decimal("2"))])
        assert Intent.count_intents(seq) == 2

    def test_count_mixed_list(self):
        items = [
            _swap(),
            IntentSequence(intents=[_swap(), _swap(Decimal("2"))]),
        ]
        assert Intent.count_intents(items) == 3


# =============================================================================
# serialize_result / deserialize_result
# =============================================================================


class TestResultSerialization:
    def test_serialize_none(self):
        assert Intent.serialize_result(None) is None

    def test_deserialize_none(self):
        assert Intent.deserialize_result(None) is None

    def test_serialize_single_intent(self):
        intent = _swap()
        data = Intent.serialize_result(intent)
        assert data["type"] == "SWAP"
        rebuilt = Intent.deserialize_result(data)
        assert isinstance(rebuilt, SwapIntent)

    def test_serialize_sequence(self):
        seq = IntentSequence(intents=[_swap()], description="demo")
        data = Intent.serialize_result(seq)
        assert data["type"] == "SEQUENCE"
        rebuilt = Intent.deserialize_result(data)
        assert isinstance(rebuilt, IntentSequence)
        assert rebuilt.description == "demo"

    def test_serialize_parallel_list(self):
        items = [
            _swap(),
            IntentSequence(intents=[_swap()]),
        ]
        data = Intent.serialize_result(items)
        assert data["type"] == "PARALLEL"
        rebuilt = Intent.deserialize_result(data)
        assert isinstance(rebuilt, list)
        assert len(rebuilt) == 2
        assert isinstance(rebuilt[0], SwapIntent)
        assert isinstance(rebuilt[1], IntentSequence)


# =============================================================================
# has_chained_amount / validate_chained_amounts / get_amount_field /
# set_resolved_amount
# =============================================================================


class TestChainedAmountHelpers:
    def test_has_chained_amount_true(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")
        assert Intent.has_chained_amount(intent) is True

    def test_has_chained_amount_false(self):
        assert Intent.has_chained_amount(_swap()) is False

    def test_has_chained_amount_hold_returns_false(self):
        # HoldIntent has no is_chained_amount attribute.
        assert Intent.has_chained_amount(HoldIntent()) is False

    def test_validate_chained_amounts_empty_no_op(self):
        # Bypass __post_init__ validation by mutating after construction so we
        # can exercise the early-return branch of validate_chained_amounts.
        seq = IntentSequence(intents=[_swap()])
        seq.intents.clear()
        # Should not raise.
        Intent.validate_chained_amounts(seq)

    def test_validate_chained_amounts_first_all_raises(self):
        first = SwapIntent(from_token="USDC", to_token="ETH", amount="all")
        second = SwapIntent(from_token="ETH", to_token="USDC", amount="all")
        seq = IntentSequence(intents=[first, second])
        with pytest.raises(InvalidAmountError):
            Intent.validate_chained_amounts(seq)

    def test_validate_chained_amounts_ok(self):
        first = _swap(Decimal("100"))
        second = SwapIntent(from_token="ETH", to_token="USDC", amount="all")
        seq = IntentSequence(intents=[first, second])
        Intent.validate_chained_amounts(seq)

    def test_get_amount_field_swap_amount(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("5"))
        assert Intent.get_amount_field(intent) == Decimal("5")

    def test_get_amount_field_swap_usd(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount_usd=Decimal("500"))
        assert Intent.get_amount_field(intent) == Decimal("500")

    def test_get_amount_field_all(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")
        assert Intent.get_amount_field(intent) == "all"

    def test_get_amount_field_borrow_uses_borrow_amount(self):
        intent = BorrowIntent(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount=Decimal("0"),
            borrow_token="USDC",
            borrow_amount=Decimal("1500"),
        )
        assert Intent.get_amount_field(intent) == Decimal("1500")

    def test_get_amount_field_perp_open_uses_collateral_amount(self):
        intent = Intent.perp_open(
            market="ETH/USD",
            collateral_token="WETH",
            collateral_amount=Decimal("0.1"),
            size_usd=Decimal("1000"),
            leverage=Decimal("5"),
            protocol="gmx_v2",
        )
        assert Intent.get_amount_field(intent) == Decimal("0.1")

    def test_get_amount_field_hold_is_none(self):
        assert Intent.get_amount_field(HoldIntent()) is None

    def test_set_resolved_amount_swap_all(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount="all")
        resolved = Intent.set_resolved_amount(intent, Decimal("7.5"))
        assert resolved.amount == Decimal("7.5")

    def test_set_resolved_amount_borrow_collateral_all_rejected(self):
        # A chained collateral_amount="all" on a borrow is the bundled form the
        # validator now forbids. The "all" fixture can only be built via
        # model_construct; resolving it re-serializes and re-validates through
        # Intent.deserialize, which rejects the now-positive bundled collateral.
        # The resolution path therefore raises rather than producing a bundled
        # borrow with a concrete collateral amount.
        intent = BorrowIntent.model_construct(
            protocol="aave_v3",
            collateral_token="WETH",
            collateral_amount="all",
            borrow_token="USDC",
            borrow_amount=Decimal("1500"),
        )
        with pytest.raises(ValidationError, match="Bundled collateralized borrow is not supported"):
            Intent.set_resolved_amount(intent, Decimal("2"))

    def test_set_resolved_amount_without_all_is_noop(self):
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount=Decimal("1"))
        resolved = Intent.set_resolved_amount(intent, Decimal("9"))
        # Because neither "amount" nor "collateral_amount" held "all", both
        # branches are skipped; the amount flows through unchanged.
        assert resolved.amount == Decimal("1")


# =============================================================================
# Defensive branches reachable only via model_construct / direct validator
# =============================================================================


class TestDefensiveValidatorBranches:
    """Covers branches that Pydantic's BeforeValidators normally keep
    unreachable. We use model_construct to bypass pre-validation so we
    can exercise the defensive isinstance checks in the model_validator."""

    def test_swap_amount_non_decimal_non_all_defensive(self):
        intent = SwapIntent.model_construct(
            from_token="USDC",
            to_token="ETH",
            amount_usd=None,
            amount="weird-not-all",
            max_slippage=Decimal("0.005"),
            max_price_impact=None,
            protocol=None,
            chain=None,
            destination_chain=None,
            priority_fee_level=None,
            priority_fee_max_lamports=None,
        )
        with pytest.raises(ValueError, match="amount must be a positive Decimal or 'all'"):
            intent.validate_swap_intent()

    def test_lp_open_protocol_params_not_dict_defensive(self):
        # The model validator's defensive isinstance dict check (line 415) is
        # unreachable through normal construction because pydantic rejects the
        # non-dict first. model_construct bypasses pre-validation.
        intent = LPOpenIntent.model_construct(
            pool="0x" + "a" * 40,
            amount0=Decimal("1"),
            amount1=Decimal("1"),
            range_lower=Decimal("1"),
            range_upper=Decimal("2"),
            protocol="uniswap_v3",
            chain=None,
            protocol_params="not a dict",  # type: ignore[arg-type]
        )
        with pytest.raises(ValueError, match="protocol_params must be a dict"):
            intent.validate_lp_open_intent()
