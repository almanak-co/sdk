"""Isolation tests for ``almanak.framework.intents._compiler_helpers``.

Phase 6B.2 of the compiler refactor extracts pure skeleton helpers shared
by ``IntentCompiler._compile_swap`` and ``IntentCompiler._compile_lp_open``.
These tests pin the helpers' behaviour before they get wired into the
real compile methods in Phase 6B.3 / 6B.4.

Each helper is pure (no I/O, no side effects), so tests can exercise every
branch without Anvil / RPC / Web3 mocks.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.intents._compiler_helpers import (
    PriceImpactDecision,
    assemble_action_bundle,
    check_price_impact,
    choose_safer_quote,
    compute_deadline,
    compute_min_amount_out,
    sum_transaction_gas,
)
from almanak.framework.intents.compiler_models import TransactionData
from almanak.framework.models.reproduction_bundle import ActionBundle

# ---------------------------------------------------------------------------
# compute_min_amount_out
# ---------------------------------------------------------------------------


class TestComputeMinAmountOut:
    def test_one_percent_slippage(self) -> None:
        # 1_000_000 * 0.99 == 990_000 exactly.
        assert compute_min_amount_out(1_000_000, Decimal("0.01")) == 990_000

    def test_zero_slippage_returns_expected(self) -> None:
        assert compute_min_amount_out(1_000_000, Decimal("0")) == 1_000_000

    def test_full_slippage_yields_zero(self) -> None:
        # max_slippage=1 is the "zero minimum" boundary used by LP safe-for-testing.
        assert compute_min_amount_out(1_000_000, Decimal("1")) == 0

    def test_truncates_not_rounds(self) -> None:
        # Real compiler uses ``int(Decimal(...) * ...)`` which truncates.
        # 999 * 0.99 = 989.01 -> truncated to 989 (not banker's-rounded to 989).
        assert compute_min_amount_out(999, Decimal("0.01")) == 989

    def test_large_amount_does_not_overflow(self) -> None:
        # A swap of ~1_000_000 ETH in wei (1e24) with 0.5% slippage — the
        # Decimal path handles this without precision loss.
        expected = 10**24
        result = compute_min_amount_out(expected, Decimal("0.005"))
        assert result == int(Decimal(str(expected)) * Decimal("0.995"))

    def test_negative_expected_raises(self) -> None:
        with pytest.raises(ValueError, match=">= 0"):
            compute_min_amount_out(-1, Decimal("0.01"))

    def test_slippage_above_one_raises(self) -> None:
        with pytest.raises(ValueError, match=r"\[0, 1\]"):
            compute_min_amount_out(1_000_000, Decimal("1.5"))

    def test_negative_slippage_raises(self) -> None:
        with pytest.raises(ValueError, match=r"\[0, 1\]"):
            compute_min_amount_out(1_000_000, Decimal("-0.01"))

    def test_matches_compiler_line_1554(self) -> None:
        """Pin the exact math used at compiler.py:1554.

        ``int(Decimal(str(expected_output)) * (Decimal("1") - intent.max_slippage))``
        """
        expected_output = 49_850_000_000_000_000
        max_slippage = Decimal("0.005")
        compiler_result = int(Decimal(str(expected_output)) * (Decimal("1") - max_slippage))
        assert compute_min_amount_out(expected_output, max_slippage) == compiler_result


# ---------------------------------------------------------------------------
# choose_safer_quote
# ---------------------------------------------------------------------------


class TestChooseSaferQuote:
    def test_quoter_lower_than_oracle_uses_quoter(self) -> None:
        safer, used_quoter = choose_safer_quote(1_000_000, 900_000)
        assert safer == 900_000
        assert used_quoter is True

    def test_quoter_equal_to_oracle_prefers_oracle(self) -> None:
        # Equality is NOT "strictly less", so oracle wins and used_quoter=False.
        # This matches compiler.py:1501: ``if quoter_amount is not None and quoter_amount < expected_output``.
        safer, used_quoter = choose_safer_quote(1_000_000, 1_000_000)
        assert safer == 1_000_000
        assert used_quoter is False

    def test_quoter_higher_than_oracle_keeps_oracle(self) -> None:
        safer, used_quoter = choose_safer_quote(1_000_000, 1_200_000)
        assert safer == 1_000_000
        assert used_quoter is False

    def test_quoter_none_keeps_oracle(self) -> None:
        safer, used_quoter = choose_safer_quote(1_000_000, None)
        assert safer == 1_000_000
        assert used_quoter is False


# ---------------------------------------------------------------------------
# check_price_impact
# ---------------------------------------------------------------------------


class TestCheckPriceImpact:
    def test_ok_when_impact_within_max(self) -> None:
        # Oracle 1000, quoter 950 => 5% impact, max 30% default.
        result = check_price_impact(
            oracle_estimate=1000,
            quoter_amount=950,
            intent_max_impact=None,
            config_max_impact=Decimal("0.30"),
            offline_mode=False,
            using_placeholders=False,
        )
        assert result.decision == PriceImpactDecision.OK
        assert result.price_impact == Decimal("0.05")
        assert result.effective_max_impact == Decimal("0.30")

    def test_impact_too_high_fails(self) -> None:
        # Oracle 1000, quoter 100 => 90% impact, max 30% default.
        result = check_price_impact(
            oracle_estimate=1000,
            quoter_amount=100,
            intent_max_impact=None,
            config_max_impact=Decimal("0.30"),
            offline_mode=False,
            using_placeholders=False,
        )
        assert result.decision == PriceImpactDecision.IMPACT_TOO_HIGH
        assert result.price_impact == Decimal("0.9")
        assert result.effective_max_impact == Decimal("0.30")

    def test_intent_override_beats_config_default(self) -> None:
        """Per-intent max_price_impact takes precedence over config."""
        # 10% impact, intent allows 5%, config allows 30%. Intent wins => FAIL.
        result = check_price_impact(
            oracle_estimate=1000,
            quoter_amount=900,
            intent_max_impact=Decimal("0.05"),
            config_max_impact=Decimal("0.30"),
            offline_mode=False,
            using_placeholders=False,
        )
        assert result.decision == PriceImpactDecision.IMPACT_TOO_HIGH
        assert result.effective_max_impact == Decimal("0.05")

    def test_intent_override_allows_looser_bound(self) -> None:
        # 10% impact, intent allows 20%, config 5%. Intent wins => OK.
        result = check_price_impact(
            oracle_estimate=1000,
            quoter_amount=900,
            intent_max_impact=Decimal("0.20"),
            config_max_impact=Decimal("0.05"),
            offline_mode=False,
            using_placeholders=False,
        )
        assert result.decision == PriceImpactDecision.OK
        assert result.effective_max_impact == Decimal("0.20")

    def test_boundary_equal_impact_is_ok(self) -> None:
        """Impact == max is OK (strict > in the real compiler)."""
        # 30% impact, max 30% — compiler.py:1521 uses ``if price_impact > max_impact``.
        result = check_price_impact(
            oracle_estimate=1000,
            quoter_amount=700,
            intent_max_impact=None,
            config_max_impact=Decimal("0.30"),
            offline_mode=False,
            using_placeholders=False,
        )
        assert result.decision == PriceImpactDecision.OK

    def test_placeholders_skips_impact_check(self) -> None:
        """using_placeholders bypasses the impact check entirely."""
        result = check_price_impact(
            oracle_estimate=1000,
            quoter_amount=100,  # would normally be 90% impact => FAIL
            intent_max_impact=None,
            config_max_impact=Decimal("0.30"),
            offline_mode=True,
            using_placeholders=True,
        )
        assert result.decision == PriceImpactDecision.SKIPPED_OFFLINE

    def test_zero_oracle_skips(self) -> None:
        result = check_price_impact(
            oracle_estimate=0,
            quoter_amount=1000,
            intent_max_impact=None,
            config_max_impact=Decimal("0.30"),
            offline_mode=False,
            using_placeholders=False,
        )
        assert result.decision == PriceImpactDecision.SKIPPED_NO_ORACLE

    def test_none_quoter_online_fails_closed(self) -> None:
        """VIB-3160: no quoter + online => fail closed."""
        result = check_price_impact(
            oracle_estimate=1000,
            quoter_amount=None,
            intent_max_impact=None,
            config_max_impact=Decimal("0.30"),
            offline_mode=False,
            using_placeholders=False,
        )
        assert result.decision == PriceImpactDecision.QUOTER_MISSING_FAIL_CLOSED
        assert result.price_impact is None

    def test_none_quoter_offline_skips(self) -> None:
        """offline_mode (placeholders OR permission_discovery) allows no quoter."""
        result = check_price_impact(
            oracle_estimate=1000,
            quoter_amount=None,
            intent_max_impact=None,
            config_max_impact=Decimal("0.30"),
            offline_mode=True,
            using_placeholders=False,  # e.g. permission_discovery=True
        )
        assert result.decision == PriceImpactDecision.SKIPPED_OFFLINE

    def test_none_quoter_offline_via_placeholders_skips(self) -> None:
        result = check_price_impact(
            oracle_estimate=1000,
            quoter_amount=None,
            intent_max_impact=None,
            config_max_impact=Decimal("0.30"),
            offline_mode=True,
            using_placeholders=True,
        )
        assert result.decision == PriceImpactDecision.SKIPPED_OFFLINE

    def test_pure_no_mutation_of_inputs(self) -> None:
        """Helper must not mutate the Decimal inputs."""
        config_max = Decimal("0.30")
        intent_max = Decimal("0.05")
        check_price_impact(
            oracle_estimate=1000,
            quoter_amount=900,
            intent_max_impact=intent_max,
            config_max_impact=config_max,
            offline_mode=False,
            using_placeholders=False,
        )
        assert config_max == Decimal("0.30")
        assert intent_max == Decimal("0.05")

    def test_result_is_frozen(self) -> None:
        """PriceImpactCheckResult is frozen to keep helper output immutable."""
        from dataclasses import FrozenInstanceError

        result = check_price_impact(
            oracle_estimate=1000,
            quoter_amount=950,
            intent_max_impact=None,
            config_max_impact=Decimal("0.30"),
            offline_mode=False,
            using_placeholders=False,
        )
        with pytest.raises(FrozenInstanceError):
            result.decision = PriceImpactDecision.IMPACT_TOO_HIGH  # type: ignore[misc]


# ---------------------------------------------------------------------------
# compute_deadline
# ---------------------------------------------------------------------------


class TestComputeDeadline:
    def test_adds_seconds_to_explicit_now(self) -> None:
        assert compute_deadline(600, now_ts=1_700_000_000) == 1_700_000_600

    def test_uses_current_time_when_now_ts_none(self) -> None:
        import time

        before = int(time.time())
        result = compute_deadline(300)
        after = int(time.time())
        assert before + 300 <= result <= after + 300

    def test_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="> 0"):
            compute_deadline(0, now_ts=1_700_000_000)

    def test_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="> 0"):
            compute_deadline(-1, now_ts=1_700_000_000)

    def test_explicit_zero_now_ts_allowed(self) -> None:
        """now_ts=0 is a legitimate (if absurd) value; helper must not reject it."""
        assert compute_deadline(600, now_ts=0) == 600


# ---------------------------------------------------------------------------
# sum_transaction_gas + assemble_action_bundle
# ---------------------------------------------------------------------------


def _make_tx(gas: int = 100_000, tx_type: str = "swap") -> TransactionData:
    return TransactionData(
        to="0x" + "11" * 20,
        value=0,
        data="0x",
        gas_estimate=gas,
        description=f"test-{tx_type}",
        tx_type=tx_type,
    )


class TestSumTransactionGas:
    def test_empty_list_returns_zero(self) -> None:
        assert sum_transaction_gas([]) == 0

    def test_single_tx(self) -> None:
        assert sum_transaction_gas([_make_tx(gas=42_000)]) == 42_000

    def test_multiple_txs_sum(self) -> None:
        txs = [_make_tx(gas=10_000), _make_tx(gas=20_000), _make_tx(gas=30_000)]
        assert sum_transaction_gas(txs) == 60_000


class TestAssembleActionBundle:
    def test_builds_bundle_with_dict_transactions(self) -> None:
        tx = _make_tx()
        bundle = assemble_action_bundle(
            intent_type="SWAP",
            transactions=[tx],
            metadata={"from_token": "USDC", "amount_in": "1000000"},
        )
        assert isinstance(bundle, ActionBundle)
        assert bundle.intent_type == "SWAP"
        # Transactions are dicts in the bundle, not dataclass instances.
        assert bundle.transactions == [tx.to_dict()]
        assert bundle.metadata == {"from_token": "USDC", "amount_in": "1000000"}

    def test_metadata_passed_through_untouched(self) -> None:
        """Caller-owned metadata dict is preserved byte-for-byte.

        The wiring PR relies on this: tests grep specific keys
        (``amount_in``, ``min_amount_out``, ``tick_lower``, ...) on the
        resulting action_bundle.metadata and any modification would break
        characterization coverage.
        """
        meta = {
            "from_token": {"symbol": "USDC", "address": "0xabc", "decimals": 6},
            "to_token": {"symbol": "WETH", "address": "0xdef", "decimals": 18},
            "amount_in": "1000000",
            "min_amount_out": "490000000000000000",
            "slippage": "0.005",
            "protocol": "uniswap_v3",
            "selected_fee_tier": 3000,
            "deadline": 1_700_000_000,
            "chain": "arbitrum",
        }
        bundle = assemble_action_bundle(
            intent_type="SWAP",
            transactions=[_make_tx()],
            metadata=meta,
        )
        # Same keys, same values.
        assert bundle.metadata == meta
        # Bundle stores a reference (compiler.py currently passes dict literals so
        # aliasing is acceptable). Pin the current behaviour to catch accidental
        # copies: a mutation to the caller's dict shows up on the bundle.
        meta["deadline"] = 1_700_000_001
        assert bundle.metadata["deadline"] == 1_700_000_001

    def test_empty_transactions_allowed(self) -> None:
        """Some cross-chain / deferred flows build empty transaction lists."""
        bundle = assemble_action_bundle(
            intent_type="LP_OPEN",
            transactions=[],
            metadata={},
        )
        assert bundle.transactions == []

    def test_transaction_order_preserved(self) -> None:
        """Approval ordering is consensus-critical — helper must not reorder."""
        approve_reset = _make_tx(tx_type="approve_reset")
        approve = _make_tx(tx_type="approve")
        swap = _make_tx(tx_type="swap")
        bundle = assemble_action_bundle(
            intent_type="SWAP",
            transactions=[approve_reset, approve, swap],
            metadata={},
        )
        got_types = [tx["tx_type"] for tx in bundle.transactions]
        assert got_types == ["approve_reset", "approve", "swap"]

    def test_intent_type_string_stored_as_given(self) -> None:
        bundle = assemble_action_bundle(
            intent_type="LP_OPEN",
            transactions=[_make_tx()],
            metadata={},
        )
        assert bundle.intent_type == "LP_OPEN"


# ---------------------------------------------------------------------------
# Integration: helpers together pin swap-like flow
# ---------------------------------------------------------------------------


class TestHelperComposition:
    """Smoke check that helpers compose into the swap skeleton shape.

    Not a replacement for the wired compiler tests — just a canary that the
    five helpers line up into a plausible swap-compile flow.
    """

    def test_swap_like_composition(self) -> None:
        oracle_estimate = 49_850_000_000_000_000
        quoter = 49_000_000_000_000_000
        max_slippage = Decimal("0.005")

        safer, used_quoter = choose_safer_quote(oracle_estimate, quoter)
        assert safer == quoter
        assert used_quoter is True

        impact = check_price_impact(
            oracle_estimate=oracle_estimate,
            quoter_amount=quoter,
            intent_max_impact=None,
            config_max_impact=Decimal("0.30"),
            offline_mode=False,
            using_placeholders=False,
        )
        assert impact.decision == PriceImpactDecision.OK

        min_out = compute_min_amount_out(safer, max_slippage)
        # ``49_000_000_000_000_000 * 0.995 == 48_755_000_000_000_000`` exactly.
        assert min_out == 48_755_000_000_000_000

        deadline = compute_deadline(600, now_ts=1_700_000_000)
        assert deadline == 1_700_000_600

        tx = _make_tx(gas=200_000, tx_type="swap")
        assert sum_transaction_gas([tx]) == 200_000

        bundle = assemble_action_bundle(
            intent_type="SWAP",
            transactions=[tx],
            metadata={"min_amount_out": str(min_out), "deadline": deadline},
        )
        assert bundle.metadata["min_amount_out"] == "48755000000000000"
        assert bundle.metadata["deadline"] == 1_700_000_600
