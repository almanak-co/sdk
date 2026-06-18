"""Tests for extracted_data_json serialization in the transaction ledger.

Validates Phase 1b of the Dashboard Accounting PRD:
- Type-tagged serialization preserves types across round-trips
- All extracted data dataclasses (SwapAmounts, LPOpenData, PerpData, etc.)
  serialize and deserialize correctly
- Multi-tx bundle tx_hashes are captured in extracted_data_json
"""

from decimal import Decimal

from almanak.framework.execution.extracted_data import (
    BorrowData,
    LPCloseData,
    LPOpenData,
    PerpData,
    StakeData,
    SupplyData,
    SwapAmounts,
)
from almanak.framework.observability.ledger import (
    deserialize_extracted_data,
    serialize_extracted_data,
)


class TestSerializeExtractedData:
    """Test type-tagged serialization of extracted data."""

    def test_empty_dict(self):
        result = serialize_extracted_data({})
        assert result == "{}"

    def test_swap_amounts_round_trip(self):
        original = SwapAmounts(
            amount_in=1000000,
            amount_out=500000000000000000,
            amount_in_decimal=Decimal("1000.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("2000.0"),
            slippage_bps=15,
            token_in="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            token_out="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
        )

        json_str = serialize_extracted_data({"swap_amounts": original})
        assert json_str  # non-empty

        restored = deserialize_extracted_data(json_str)
        assert "swap_amounts" in restored
        sa = restored["swap_amounts"]
        assert isinstance(sa, SwapAmounts)
        assert sa.amount_in == 1000000
        assert sa.amount_out == 500000000000000000
        assert sa.effective_price == Decimal("2000.0")
        assert sa.slippage_bps == 15

    def test_lp_open_data_round_trip(self):
        original = LPOpenData(
            position_id=12345,
            tick_lower=-887220,
            tick_upper=887220,
            liquidity=1000000000,
            amount0=500000,
            amount1=250000000000000000,
        )

        json_str = serialize_extracted_data({"lp_open": original})
        restored = deserialize_extracted_data(json_str)
        lp = restored["lp_open"]
        assert isinstance(lp, LPOpenData)
        assert lp.position_id == 12345
        assert lp.tick_lower == -887220
        assert lp.tick_upper == 887220
        assert lp.liquidity == 1000000000

    def test_lp_close_data_round_trip(self):
        original = LPCloseData(
            amount0_collected=480000,
            amount1_collected=170000000000000000,
            fees0=5000,
            fees1=2000000000000000,
            liquidity_removed=1000000000,
        )

        json_str = serialize_extracted_data({"lp_close": original})
        restored = deserialize_extracted_data(json_str)
        lp = restored["lp_close"]
        assert isinstance(lp, LPCloseData)
        assert lp.amount0_collected == 480000
        assert lp.fees0 == 5000

    def test_perp_data_round_trip(self):
        original = PerpData(
            position_id="0xabc123",
            size_delta=1000000000000000000,
            collateral=500000000,
            entry_price=Decimal("3450.00"),
            leverage=Decimal("2.0"),
            realized_pnl=Decimal("150.25"),
            exit_price=Decimal("3500.00"),
            fees_paid=12000000,
        )

        json_str = serialize_extracted_data({"perp": original})
        restored = deserialize_extracted_data(json_str)
        perp = restored["perp"]
        assert isinstance(perp, PerpData)
        assert perp.position_id == "0xabc123"
        assert perp.entry_price == Decimal("3450.00")
        assert perp.realized_pnl == Decimal("150.25")

    def test_borrow_data_round_trip(self):
        original = BorrowData(
            borrow_amount=1000000,
            borrow_rate=Decimal("0.035"),
            debt_token="0xdebt",
            health_factor=Decimal("1.85"),
        )

        json_str = serialize_extracted_data({"borrow": original})
        restored = deserialize_extracted_data(json_str)
        b = restored["borrow"]
        assert isinstance(b, BorrowData)
        assert b.borrow_amount == 1000000
        assert b.health_factor == Decimal("1.85")

    def test_supply_data_round_trip(self):
        original = SupplyData(
            supply_amount=5000000,
            a_token_received=4999000,
            supply_rate=Decimal("0.025"),
        )

        json_str = serialize_extracted_data({"supply": original})
        restored = deserialize_extracted_data(json_str)
        s = restored["supply"]
        assert isinstance(s, SupplyData)
        assert s.supply_amount == 5000000

    def test_stake_data_round_trip(self):
        original = StakeData(
            stake_amount=1000000000000000000,
            shares_received=900000000000000000,
            stake_token="0xstake",
        )

        json_str = serialize_extracted_data({"stake": original})
        restored = deserialize_extracted_data(json_str)
        s = restored["stake"]
        assert isinstance(s, StakeData)
        assert s.stake_amount == 1000000000000000000

    def test_mixed_types(self):
        """Extracted data often has a mix of typed and raw values."""
        data = {
            "swap_amounts": SwapAmounts(
                amount_in=100,
                amount_out=200,
                amount_in_decimal=Decimal("0.1"),
                amount_out_decimal=Decimal("0.2"),
                effective_price=Decimal("2.0"),
            ),
            "position_id": 12345,
            "custom_field": "some_value",
        }

        json_str = serialize_extracted_data(data)
        restored = deserialize_extracted_data(json_str)

        assert isinstance(restored["swap_amounts"], SwapAmounts)
        assert restored["position_id"] == 12345
        assert restored["custom_field"] == "some_value"

    def test_decimal_value_round_trip(self):
        data = {"price": Decimal("3450.123456789")}
        json_str = serialize_extracted_data(data)
        restored = deserialize_extracted_data(json_str)
        assert restored["price"] == Decimal("3450.123456789")

    def test_empty_string_returns_empty_dict(self):
        assert deserialize_extracted_data("") == {}
        assert deserialize_extracted_data(None) == {}

    def test_invalid_json_returns_empty_dict(self):
        assert deserialize_extracted_data("not json") == {}


class TestBuildLedgerEntryExtractedData:
    """Test that build_ledger_entry captures extracted_data_json."""

    def test_build_with_extracted_data(self):
        from unittest.mock import MagicMock

        from almanak.framework.observability.ledger import build_ledger_entry

        intent = MagicMock()
        intent.intent_type = MagicMock()
        intent.intent_type.value = "SWAP"
        intent.protocol = "uniswap_v3"
        intent.from_token = "USDC"
        intent.to_token = "ETH"

        result = MagicMock()
        result.swap_amounts = SwapAmounts(
            amount_in=1000000,
            amount_out=500000000000000000,
            amount_in_decimal=Decimal("1000.0"),
            amount_out_decimal=Decimal("0.5"),
            effective_price=Decimal("2000.0"),
            slippage_bps=15,
            token_in="USDC",
            token_out="ETH",
        )
        result.extracted_data = {"swap_amounts": result.swap_amounts}
        result.transaction_results = [MagicMock(tx_hash="0xabc", gas_used=100000, success=True)]
        result.total_gas_used = 100000
        result.gas_cost_usd = Decimal("1.50")

        entry = build_ledger_entry(
            deployment_id="test",
            cycle_id="cycle-1",
            intent=intent,
            result=result,
            chain="ethereum",
        )

        assert entry.extracted_data_json  # non-empty
        restored = deserialize_extracted_data(entry.extracted_data_json)
        assert isinstance(restored["swap_amounts"], SwapAmounts)
        assert restored["swap_amounts"].effective_price == Decimal("2000.0")

    def test_build_with_multi_tx_bundle(self):
        from unittest.mock import MagicMock

        from almanak.framework.observability.ledger import build_ledger_entry

        intent = MagicMock()
        intent.intent_type = MagicMock()
        intent.intent_type.value = "SUPPLY"
        intent.protocol = "aave_v3"
        intent.supply_token = "USDC"
        intent.from_token = None
        intent.to_token = None

        result = MagicMock()
        result.swap_amounts = None
        result.extracted_data = {"supply": SupplyData(supply_amount=5000000, a_token_received=4999000)}
        # Multi-tx: approve + supply
        tx1 = MagicMock(tx_hash="0xapprove", gas_used=50000, success=True)
        tx2 = MagicMock(tx_hash="0xsupply", gas_used=200000, success=True)
        result.transaction_results = [tx1, tx2]
        result.total_gas_used = 250000
        result.gas_cost_usd = Decimal("3.75")

        entry = build_ledger_entry(
            deployment_id="test",
            cycle_id="cycle-2",
            intent=intent,
            result=result,
            chain="ethereum",
        )

        import json

        parsed = json.loads(entry.extracted_data_json)
        assert "all_tx_results" in parsed
        assert len(parsed["all_tx_results"]) == 2
        assert parsed["all_tx_results"][0]["tx_hash"] == "0xapprove"
        assert parsed["all_tx_results"][1]["tx_hash"] == "0xsupply"

    def test_build_without_result_has_empty_extracted_data(self):
        from unittest.mock import MagicMock

        from almanak.framework.observability.ledger import build_ledger_entry

        intent = MagicMock()
        intent.intent_type = MagicMock()
        intent.intent_type.value = "HOLD"
        intent.protocol = ""
        intent.from_token = None
        intent.to_token = None

        entry = build_ledger_entry(
            deployment_id="test",
            cycle_id="cycle-3",
            intent=intent,
            result=None,
            chain="ethereum",
        )

        assert entry.extracted_data_json == ""


class TestRepayWithdrawAmountIn:
    """VIB-3939 — receipt-resolved REPAY/WITHDRAW lands on transaction_ledger.amount_in.

    Pre-fix: ``RepayIntent(repay_full=True)`` and ``WithdrawIntent(withdraw_all=True)``
    submit ``uint256.max`` to Aave; ``intent.amount`` defaults to ``Decimal(0)``;
    the ledger's intent-attr fallback wrote ``amount_in=""`` even though the on-
    chain ``Repay`` / ``Withdraw`` event carries the resolved amount which the
    receipt parser already extracted onto ``result.extracted_data["repay_amount"]``
    / ``["withdraw_amount"]``.

    The fix routes REPAY/WITHDRAW through ``_extract_from_lending`` which reads
    the receipt-resolved raw int and scales to human units via the token
    resolver, mirroring ``accounting/category_handlers/lending_handler.py``.
    """

    def _make_intent(self, intent_type: str, token: str, amount: Decimal):
        from unittest.mock import MagicMock

        intent = MagicMock()
        intent.intent_type = MagicMock()
        intent.intent_type.value = intent_type
        intent.protocol = "aave_v3"
        intent.token = token
        intent.amount = amount
        # Explicitly None out every attribute on the intent-attr fallback
        # precedence chain so the fallback test below sees only `token` /
        # `amount` (not auto-generated MagicMock attributes that the `or`
        # chain would treat as truthy garbage).
        intent.from_token = None
        intent.to_token = None
        intent.borrow_token = None
        intent.supply_token = None
        intent.borrow_amount = None
        intent.supply_amount = None
        intent.amount_usd = None
        intent.collateral_token = None
        intent.collateral_amount = None
        return intent

    def _make_result(self, extracted: dict):
        from unittest.mock import MagicMock

        result = MagicMock()
        result.swap_amounts = None
        result.extracted_data = extracted
        result.transaction_results = [MagicMock(tx_hash="0xrepayhash", gas_used=53570, success=True)]
        result.total_gas_used = 53570
        result.total_gas_cost_wei = 0  # don't trigger gas_usd warn path
        result.gas_cost_usd = Decimal("0")
        return result

    def test_repay_full_uint256_max_uses_receipt_resolved_amount(self):
        """RepayIntent(repay_full=True, amount=Decimal(0)) — pre-fix wrote
        ``amount_in=""``; post-fix reads the receipt-resolved raw int."""
        from almanak.framework.observability.ledger import build_ledger_entry

        # Mirror the May-3 looping run: 2.000001 USDT (6 decimals) repaid.
        intent = self._make_intent("REPAY", "USDT", Decimal(0))
        result = self._make_result({"repay_amount": 2_000_001})

        entry = build_ledger_entry(
            deployment_id="test",
            cycle_id="cycle-repay",
            intent=intent,
            result=result,
            chain="arbitrum",
        )

        assert entry.intent_type == "REPAY"
        assert entry.token_in == "USDT"
        assert entry.amount_in == "2.000001"

    def test_withdraw_all_uint256_max_uses_receipt_resolved_amount(self):
        """WithdrawIntent(withdraw_all=True, amount=Decimal(0)) — pre-fix wrote
        ``amount_in=""``; post-fix reads the receipt-resolved raw int."""
        from almanak.framework.observability.ledger import build_ledger_entry

        # Residual 0.5 USDC (6 decimals) withdrawn.
        intent = self._make_intent("WITHDRAW", "USDC", Decimal(0))
        result = self._make_result({"withdraw_amount": 500_000})

        entry = build_ledger_entry(
            deployment_id="test",
            cycle_id="cycle-withdraw",
            intent=intent,
            result=result,
            chain="arbitrum",
        )

        assert entry.intent_type == "WITHDRAW"
        assert entry.token_in == "USDC"
        assert entry.amount_in == "0.5"

    def test_repay_receipt_wins_over_intent_for_partial(self):
        """Even when the intent carries a Decimal amount, prefer the receipt-
        resolved value. Aave can repay strictly less than the requested amount
        when the wallet balance is below it (the auditor needs the real number,
        not the request)."""
        from almanak.framework.observability.ledger import build_ledger_entry

        # User asked to repay 5 USDT; protocol resolved to 2.000001 USDT
        # (e.g. wallet balance was 2.000001 at repay time).
        intent = self._make_intent("REPAY", "USDT", Decimal("5"))
        result = self._make_result({"repay_amount": 2_000_001})

        entry = build_ledger_entry(
            deployment_id="test",
            cycle_id="cycle-repay-partial",
            intent=intent,
            result=result,
            chain="arbitrum",
        )

        assert entry.amount_in == "2.000001"  # receipt wins

    def test_repay_no_receipt_falls_back_to_intent(self):
        """When the receipt produced no resolved amount (parser absent / parse
        failed / receipt of a different shape), fall back to the intent-attr
        path. Preserves historical behaviour for non-uint256.max cases."""
        from almanak.framework.observability.ledger import build_ledger_entry

        intent = self._make_intent("REPAY", "USDT", Decimal("3.5"))
        result = self._make_result({})  # no repay_amount key

        entry = build_ledger_entry(
            deployment_id="test",
            cycle_id="cycle-repay-fallback",
            intent=intent,
            result=result,
            chain="arbitrum",
        )

        # Falls back to intent-attr fallback: token_in via getattr(intent, "token", ...)
        # and amount via getattr(intent, "amount", ...). Both populated.
        assert entry.token_in == "USDT"
        assert entry.amount_in == "3.5"

    def test_repay_unresolvable_token_leaves_amount_empty_not_zero(self):
        """Empty != zero. If we have a raw int but cannot scale (token resolver
        can't resolve the symbol on the chain), leave ``amount_in=""`` rather
        than write the unscaled raw int (which would be 18 orders of magnitude
        wrong for a 6-decimal stablecoin) or substitute ``"0"`` (which would
        lie about a measured non-zero amount)."""
        from almanak.framework.observability.ledger import build_ledger_entry

        intent = self._make_intent("REPAY", "MADE_UP_SYMBOL_NEVER_REGISTERED", Decimal(0))
        result = self._make_result({"repay_amount": 2_000_001})

        entry = build_ledger_entry(
            deployment_id="test",
            cycle_id="cycle-repay-noresolve",
            intent=intent,
            result=result,
            chain="arbitrum",
        )

        assert entry.token_in == "MADE_UP_SYMBOL_NEVER_REGISTERED"
        # Empty, not "0" — the latter would falsely claim "measured zero".
        assert entry.amount_in == ""

    def test_repay_non_int_extracted_value_falls_back_safely(self):
        """A buggy parser that emits a non-int repay_amount must not crash
        the ledger writer. Fall back to the intent-attr path."""
        from almanak.framework.observability.ledger import build_ledger_entry

        intent = self._make_intent("REPAY", "USDT", Decimal("1.5"))
        result = self._make_result({"repay_amount": "not-an-int"})

        entry = build_ledger_entry(
            deployment_id="test",
            cycle_id="cycle-repay-badtype",
            intent=intent,
            result=result,
            chain="arbitrum",
        )

        assert entry.token_in == "USDT"
        assert entry.amount_in == "1.5"  # intent-attr fallback

    def test_deleverage_repay_full_uses_receipt_resolved_amount_codex_x2(self):
        """Codex X2 (2026-05-04 PR #2017 audit): DELEVERAGE intents that close
        a borrow leg in full submit ``uint256.max`` to Aave the same way
        REPAY does, and the lending accounting path treats DELEVERAGE as a
        repay class. Pre-fix the ledger excluded DELEVERAGE from the
        receipt-resolved branch, so ``Intent.deleverage(repay_full=True)``'s
        default ``Decimal(0)`` fell through the intent-attr fallback and the
        ledger row landed ``amount_in=""`` despite the receipt carrying the
        resolved repaid amount.
        """
        from almanak.framework.observability.ledger import build_ledger_entry

        intent = self._make_intent("DELEVERAGE", "USDT", Decimal(0))
        result = self._make_result({"repay_amount": 1_999_500})

        entry = build_ledger_entry(
            deployment_id="test",
            cycle_id="cycle-deleverage",
            intent=intent,
            result=result,
            chain="arbitrum",
        )

        assert entry.intent_type == "DELEVERAGE"
        assert entry.token_in == "USDT"
        assert entry.amount_in == "1.9995"


class TestMeasuredAmountToRowVIB5214:
    """VIB-5214 (US-005, gate (b)) — MeasuredMoney at the ledger-extraction boundary.

    Before this story the extraction helpers typed amounts with the ad-hoc
    ``str(x) if x is not None else ""`` idiom. That let the ``intent_fallback``
    path LAUNDER a non-measured value into the ledger: an ABSENT field, or a
    fabricated non-numeric placeholder (the #2885 / #2895 blank-token bug class),
    was booked as a value-bearing string; and a measured ``Decimal("0")`` could
    not be told apart from "no value" by a bare ``is not None`` check.

    The fix routes every amount through ``_measured_amount_to_row`` →
    ``MeasuredMoney.from_raw``: the three Empty≠Zero states are explicit, an
    unresolved amount becomes the unmeasured/absent ``""`` row form (NEVER a
    value-bearing string, NEVER a measured zero), and a real ``Decimal`` (incl.
    measured zero) still serializes byte-identically to the legacy ``str(amt)``.
    """

    def test_measured_decimal_serializes_to_canonical_str(self):
        """A real Decimal → measured → ``str(value)`` (byte-compatible row form)."""
        from almanak.framework.observability.ledger import _measured_amount_to_row

        assert _measured_amount_to_row(Decimal("2.000001")) == "2.000001"
        assert _measured_amount_to_row(Decimal("0.5")) == "0.5"

    def test_measured_zero_is_preserved_as_zero(self):
        """``Decimal("0")`` is a MEASURED value (measured zero) → ``"0"``, not ``""``."""
        from almanak.framework.observability.ledger import _measured_amount_to_row

        assert _measured_amount_to_row(Decimal("0")) == "0"

    def test_none_is_unmeasured_empty_not_zero(self):
        """``None`` (unmeasured) → ``""`` — NEVER laundered to a measured ``"0"``."""
        from almanak.framework.observability.ledger import _measured_amount_to_row

        assert _measured_amount_to_row(None) == ""

    def test_empty_string_is_absent_empty_not_zero(self):
        """``""`` (absent — parser didn't emit) → ``""``, never a measured ``"0"``."""
        from almanak.framework.observability.ledger import _measured_amount_to_row

        assert _measured_amount_to_row("") == ""
        assert _measured_amount_to_row("   ") == ""

    def test_numeric_string_round_trips_byte_compatible(self):
        """A numeric string (e.g. ``_lp_amount_to_human`` output) round-trips
        byte-identically through the MeasuredMoney conversion."""
        from almanak.framework.observability.ledger import _measured_amount_to_row

        for s in ("0", "1.5", "0.000001", "1E-18"):
            assert _measured_amount_to_row(s) == s

    def test_non_numeric_placeholder_is_not_laundered(self):
        """A fabricated non-numeric placeholder (e.g. an unresolved ``"all"``)
        is NOT a measured number → unmeasured ``""``. Closes the value-bearing
        placeholder laundering (#2885 / #2895 bug class) and never crashes the
        ledger write."""
        from almanak.framework.observability.ledger import _measured_amount_to_row

        assert _measured_amount_to_row("all") == ""
        assert _measured_amount_to_row("n/a") == ""

    def test_out_of_domain_type_is_fail_safe_unmeasured(self):
        """A value outside the Decimal|str|None money domain (stray int/float)
        maps to unmeasured ``""`` rather than raising on the ledger hot path."""
        from almanak.framework.observability.ledger import _measured_amount_to_row

        assert _measured_amount_to_row(5) == ""  # type: ignore[arg-type]
        assert _measured_amount_to_row(1.5) == ""  # type: ignore[arg-type]


class TestIntentFallbackLaunderingVIB5214:
    """VIB-5214 — the named ``intent_fallback`` ``""``-laundering path.

    These assert behaviour through ``_extract_from_intent_fallback`` directly:
    the BEFORE/AFTER delta the story closes.
    """

    def _make_intent(self, **attrs):
        from unittest.mock import MagicMock

        intent = MagicMock()
        # Null out the whole precedence chain so only the attrs we set are seen
        # (otherwise auto-generated MagicMock attributes read as truthy garbage).
        for name in (
            "from_token",
            "to_token",
            "borrow_token",
            "supply_token",
            "token",
            "amount",
            "borrow_amount",
            "supply_amount",
            "amount_usd",
        ):
            setattr(intent, name, None)
        for name, value in attrs.items():
            setattr(intent, name, value)
        return intent

    def test_resolved_amount_is_measured(self):
        """A fully-resolved intent amount yields the measured value (correct)."""
        from almanak.framework.observability.ledger import _extract_from_intent_fallback

        intent = self._make_intent(token="USDC", amount=Decimal("3.5"))
        token_in, token_out, amount_in, amount_out, eff, slip = _extract_from_intent_fallback(intent)
        assert token_in == "USDC"
        assert amount_in == "3.5"
        assert amount_out == ""  # always-absent out leg

    def test_measured_zero_amount_preserved(self):
        """A measured ``Decimal("0")`` amount is preserved as ``"0"`` (Empty≠Zero)."""
        from almanak.framework.observability.ledger import _extract_from_intent_fallback

        intent = self._make_intent(token="USDC", amount=Decimal("0"))
        _, _, amount_in, *_ = _extract_from_intent_fallback(intent)
        assert amount_in == "0"

    def test_unresolved_amount_is_empty_not_zero(self):
        """No amount on any precedence link → unmeasured ``""`` (never ``"0"``)."""
        from almanak.framework.observability.ledger import _extract_from_intent_fallback

        intent = self._make_intent(token="USDC")  # amount stays None
        _, _, amount_in, *_ = _extract_from_intent_fallback(intent)
        assert amount_in == ""

    def test_placeholder_amount_is_not_booked_as_money(self):
        """A fabricated non-numeric placeholder amount must NOT be booked as a
        value-bearing string (the #2885 / #2895 bug class). It becomes ``""``.

        Pre-fix: ``str("all")`` → ``amount_in="all"`` (a non-money string on the
        trade tape, laundered downstream into a measured ``Decimal("0")`` by the
        aggregation's lenient parse). Post-fix: unmeasured ``""``.
        """
        from almanak.framework.observability.ledger import _extract_from_intent_fallback

        intent = self._make_intent(token="WETH", amount="all")
        _, _, amount_in, *_ = _extract_from_intent_fallback(intent)
        assert amount_in == ""


class TestDeclaredMoneyLegsVIB5218:
    """VIB-5218 (US-009) — the dispatcher prefers a connector-DECLARED
    ``PrimitiveMoneyLegs`` over the legacy guesser, and the intent-fallback path
    is observable (WARN + ``ledger_intent_fallback_total`` metric).

    (a) declared legs present → dispatcher uses them;
    (b) absent → falls back to legacy AND emits WARN + metric;
    (c) the canonical category-dispatched paths (swap_amounts / lending) are
        byte-unchanged AND do NOT trip the fallback observability.
    """

    # -- builders ---------------------------------------------------------------

    def _legs(self, *legs):
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLegs

        return PrimitiveMoneyLegs.of(*legs)

    def _input(self, token: str, amount):
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg

        return PrimitiveMoneyLeg.input(token, amount)

    def _output(self, token: str, amount):
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg

        return PrimitiveMoneyLeg.output(token, amount)

    def _principal(self, token: str, amount):
        from almanak.connectors._strategy_base.primitive_money_leg import PrimitiveMoneyLeg

        return PrimitiveMoneyLeg.principal(token, amount)

    def _measured(self, value: str):
        from almanak.framework.accounting.measured import MeasuredMoney

        return MeasuredMoney.measured(Decimal(value))

    def _unmeasured(self):
        from almanak.framework.accounting.measured import MeasuredMoney

        return MeasuredMoney.unmeasured()

    def _result(self, **attrs):
        import types

        base = {"swap_amounts": None, "extracted_data": {}}
        base.update(attrs)
        return types.SimpleNamespace(**base)

    def _intent(self, intent_type: str = "SWAP", **attrs):
        from unittest.mock import MagicMock

        intent = MagicMock()
        intent.intent_type = MagicMock()
        intent.intent_type.value = intent_type
        for name in (
            "from_token",
            "to_token",
            "borrow_token",
            "supply_token",
            "token",
            "amount",
            "borrow_amount",
            "supply_amount",
            "amount_usd",
            "collateral_token",
            "collateral_amount",
        ):
            setattr(intent, name, None)
        for name, value in attrs.items():
            setattr(intent, name, value)
        return intent

    def _fallback_counter(self, intent_type: str) -> float:
        from almanak.framework.observability.metrics import LEDGER_INTENT_FALLBACK_TOTAL

        return LEDGER_INTENT_FALLBACK_TOTAL.labels(intent_type=intent_type or "unknown")._value.get()

    # -- (a) declared legs are used + projected onto the flat columns -----------

    def test_swap_declared_legs_project_input_output(self):
        """A two-sided declaration (one input + one output) → in/out by role."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        legs = self._legs(
            self._input("USDC", self._measured("100")),
            self._output("WETH", self._measured("0.03")),
        )
        result = self._result(primitive_money_legs=legs)
        assert _extract_tokens_and_amounts(object(), result) == ("USDC", "WETH", "100", "0.03", "", None)

    def test_lp_open_two_inputs_fill_in_out_positionally(self):
        """Two INPUT legs (LP_OPEN deposits) fill the in/out slots positionally
        (leg0 → in, leg1 → out) — lane symmetry with ``_extract_from_lp_open``."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        legs = self._legs(
            self._input("WETH", self._measured("1")),
            self._input("USDC", self._measured("2000")),
        )
        result = self._result(primitive_money_legs=legs)
        assert _extract_tokens_and_amounts(object(), result) == ("WETH", "USDC", "1", "2000", "", None)

    def test_lp_close_two_outputs_fill_in_out_positionally(self):
        """Two OUTPUT legs (LP_CLOSE proceeds) fill in/out positionally — lane
        symmetry with ``_extract_from_lp_close`` (leg0 → in, leg1 → out)."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        legs = self._legs(
            self._output("WETH", self._measured("1")),
            self._output("USDC", self._measured("2000")),
        )
        result = self._result(primitive_money_legs=legs)
        assert _extract_tokens_and_amounts(object(), result) == ("WETH", "USDC", "1", "2000", "", None)

    def test_principal_only_fills_in_slot(self):
        """A single PRINCIPAL leg (perp collateral / lending) → in slot only."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        legs = self._legs(self._principal("USDC", self._measured("50")))
        result = self._result(primitive_money_legs=legs)
        assert _extract_tokens_and_amounts(object(), result) == ("USDC", "", "50", "", "", None)

    def test_unmeasured_leg_amount_is_empty_not_zero(self):
        """An unmeasured leg amount projects to ``""`` — never a fabricated zero."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        legs = self._legs(self._input("USDC", self._unmeasured()))
        result = self._result(primitive_money_legs=legs)
        token_in, _, amount_in, *_ = _extract_tokens_and_amounts(object(), result)
        assert token_in == "USDC"
        assert amount_in == ""

    def test_measured_zero_leg_amount_preserved(self):
        """A measured ``Decimal("0")`` leg amount is preserved as ``"0"`` (Empty≠Zero)."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        legs = self._legs(self._input("USDC", self._measured("0")))
        result = self._result(primitive_money_legs=legs)
        _, _, amount_in, *_ = _extract_tokens_and_amounts(object(), result)
        assert amount_in == "0"

    def test_declared_legs_via_extracted_data_key(self):
        """A connector may declare under ``extracted_data["primitive_money_legs"]``
        (the same flexible dict as lp_open_data / lp_close_data)."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        legs = self._legs(
            self._input("USDC", self._measured("100")),
            self._output("WETH", self._measured("0.03")),
        )
        result = self._result(extracted_data={"primitive_money_legs": legs})
        assert _extract_tokens_and_amounts(object(), result) == ("USDC", "WETH", "100", "0.03", "", None)

    def test_declared_legs_take_precedence_over_swap_amounts(self):
        """Declared legs are authoritative — preferred over a truthy swap_amounts."""
        from almanak.framework.execution.extracted_data import SwapAmounts
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        swap = SwapAmounts(
            amount_in=999,
            amount_out=9,
            token_in="DAI",
            token_out="WBTC",
            amount_in_decimal=Decimal("999"),
            amount_out_decimal=Decimal("9"),
        )
        legs = self._legs(
            self._input("USDC", self._measured("100")),
            self._output("WETH", self._measured("0.03")),
        )
        result = self._result(swap_amounts=swap, primitive_money_legs=legs)
        # Declared legs win — the swap_amounts DAI/WBTC row is NOT used.
        assert _extract_tokens_and_amounts(object(), result) == ("USDC", "WETH", "100", "0.03", "", None)

    def test_non_primitive_legs_value_is_ignored_and_falls_back(self):
        """A malformed (non-PrimitiveMoneyLegs) value under the key is ignored;
        the dispatcher falls back to the legacy path rather than crashing."""
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        result = self._result(extracted_data={"primitive_money_legs": "garbage"})
        intent = self._intent("SWAP", from_token="USDC", to_token="WETH", amount=Decimal("5"))
        token_in, token_out, amount_in, *_ = _extract_tokens_and_amounts(intent, result)
        assert (token_in, token_out, amount_in) == ("USDC", "WETH", "5")

    def test_declared_path_does_not_emit_fallback_observability(self, caplog):
        """When declared legs drive the row, the fallback WARN + metric must NOT fire."""
        import logging

        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        legs = self._legs(
            self._input("USDC", self._measured("100")),
            self._output("WETH", self._measured("0.03")),
        )
        result = self._result(primitive_money_legs=legs)
        before = self._fallback_counter("SWAP")
        with caplog.at_level(logging.WARNING):
            _extract_tokens_and_amounts(object(), result)
        assert self._fallback_counter("SWAP") == before
        assert "intent_fallback" not in caplog.text

    # -- (b) fallback is observable (WARN + metric) ----------------------------

    def test_fallback_money_row_emits_warn_and_metric(self, caplog):
        """No declared legs / no swap_amounts → legacy fallback fires WARN + metric,
        tagged with the intent type for diagnosis."""
        import logging

        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        intent = self._intent("SWAP", token="USDC", amount=Decimal("3.5"))
        before = self._fallback_counter("SWAP")
        with caplog.at_level(logging.WARNING):
            token_in, _, amount_in, *_ = _extract_tokens_and_amounts(intent, None)
        assert (token_in, amount_in) == ("USDC", "3.5")
        assert self._fallback_counter("SWAP") == before + 1
        assert "intent_fallback produced a money row" in caplog.text
        assert "intent_type=SWAP" in caplog.text

    def test_fallback_empty_row_does_not_emit(self, caplog):
        """A money-less fallback row (no token, no amount) is NOT the patch-hub
        signal — no WARN, no metric."""
        import logging

        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        intent = self._intent("SWAP")  # every money attr nulled
        before = self._fallback_counter("SWAP")
        with caplog.at_level(logging.WARNING):
            token_in, _, amount_in, *_ = _extract_tokens_and_amounts(intent, None)
        assert (token_in, amount_in) == ("", "")
        assert self._fallback_counter("SWAP") == before
        assert "intent_fallback" not in caplog.text

    # -- (c) canonical category-dispatched paths unchanged + no false emission --

    def test_swap_amounts_row_unchanged_and_no_fallback_emission(self, caplog):
        """The swap_amounts category path is byte-unchanged AND must not trip the
        fallback observability (it is not a guess)."""
        import logging

        from almanak.framework.execution.extracted_data import SwapAmounts
        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        swap = SwapAmounts(
            amount_in=100,
            amount_out=3,
            token_in="USDC",
            token_out="WETH",
            amount_in_decimal=Decimal("100"),
            amount_out_decimal=Decimal("0.03"),
            effective_price=Decimal("0.0003"),
            slippage_bps=12.0,
        )
        result = self._result(swap_amounts=swap)
        intent = self._intent("SWAP")
        before = self._fallback_counter("SWAP")
        with caplog.at_level(logging.WARNING):
            row = _extract_tokens_and_amounts(intent, result)
        assert row == ("USDC", "WETH", "100", "0.03", "0.0003", 12.0)
        assert self._fallback_counter("SWAP") == before
        assert "intent_fallback" not in caplog.text

    def test_lending_token_reuse_does_not_emit_fallback(self, caplog):
        """REPAY with a receipt-resolved amount routes through the lending lane,
        which reuses the PURE token-precedence helper — it must NOT trip the
        fallback observability (VIB-5218 anti-double-count)."""
        import logging

        from almanak.framework.observability.ledger import _extract_tokens_and_amounts

        intent = self._intent("REPAY", token="USDC", amount=Decimal("0"))
        result = self._result(extracted_data={"repay_amount": 2_000_001})
        before = self._fallback_counter("REPAY")
        with caplog.at_level(logging.WARNING):
            token_in, _, amount_in, *_ = _extract_tokens_and_amounts(intent, result, chain="arbitrum")
        assert token_in == "USDC"
        assert amount_in == "2.000001"  # lending lane produced the row
        assert self._fallback_counter("REPAY") == before
        assert "intent_fallback" not in caplog.text
