"""Tests for VIB-4213 — Aave V3 pre-state ``e_mode_category`` + ``interest_rate_mode``.

Covers the Phase 0 UAT card at ``docs/internal/uat-cards/VIB-4213.md`` (frozen
at SHA ``bcb59c1f04b5ecc8e85e867ac73c522b7200c0c0`` after Phase 1 SPEC_OK):

D1 (Correctness):
  - test_aave_account_state_includes_emode_when_gateway_succeeds
  - test_emode_selector_constant_value
  - test_lending_state_to_dict_emits_emode_when_set
  - test_lending_state_to_dict_emits_interest_rate_mode_when_set
  - test_lending_state_to_dict_omits_emode_when_none (emits JSON null)
  - test_lending_state_to_dict_omits_interest_rate_mode_when_none
  - test_capture_aave_pre_state_threads_intent_interest_rate_mode_for_borrow
  - test_capture_aave_pre_state_threads_intent_interest_rate_mode_for_repay
  - test_capture_aave_pre_state_interest_rate_mode_none_for_supply
  - test_capture_aave_pre_state_interest_rate_mode_none_for_withdraw

D3 (Robustness — NO SILENT FAILURE):
  - F1: test_aave_account_state_emode_none_when_gateway_returns_none
  - F2: test_aave_account_state_emode_none_when_gateway_returns_malformed
  - F3: (covered by existing VIB-3489 tests — read_aave_account_state returns
        None entirely when pool address missing; new fields default to None)
  - F4: test_non_aave_intents_unaffected_by_vib4213_fields
  - F5: test_intent_without_interest_rate_mode_attribute_safe
  - F6: test_aave_account_state_emode_zero_is_distinguishable_from_none
        (THE LOAD-BEARING test for Empty ≠ Zero on the new field)
  - test_aave_account_state_emode_none_when_value_outside_uint8_range
        (defense-in-depth: a misrouted/misshapen response decoded to a >255
        value is treated as a read failure, not a fabricated category id)
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from almanak.connectors._strategy_base.lending_read_base import (
    _AAVE_GET_USER_EMODE_SELECTOR,
    LendingAccountState,
)
from almanak.framework.accounting.lending_accounting import (
    _GENERIC_PRE_STATE_PROTOCOLS,
    capture_lending_pre_state,
    lending_state_to_dict,
    read_aave_user_emode,
    read_lending_account_state,
)

# VIB-4929 PR-3a: the per-protocol AaveAccountState / MorphoBlueAccountState are
# gone — Aave + Morpho share the unified LendingAccountState, read through the
# generic read_lending_account_state, and the Aave intent-metadata overlay
# (interest_rate_mode) is applied by capture_lending_pre_state. These helpers
# build the unified state with the correct family discriminator so the existing
# VIB-4213 assertions (which pin the persisted e_mode/interest_rate_mode shape)
# stay valid.


def _aave_state(
    *,
    collateral_usd: Decimal,
    debt_usd: Decimal,
    health_factor: Decimal,
    liquidation_threshold_bps: int,
    e_mode_category: int | None = None,
    interest_rate_mode: str | None = None,
) -> LendingAccountState:
    return LendingAccountState(
        collateral_usd=collateral_usd,
        debt_usd=debt_usd,
        health_factor=health_factor,
        liquidation_threshold_bps=liquidation_threshold_bps,
        e_mode_category=e_mode_category,
        interest_rate_mode=interest_rate_mode,
        family="aave",
    )


def _morpho_state(
    *,
    collateral_usd: Decimal,
    debt_usd: Decimal,
    health_factor: Decimal,
    lltv: Decimal,
) -> LendingAccountState:
    return LendingAccountState(
        collateral_usd=collateral_usd,
        debt_usd=debt_usd,
        health_factor=health_factor,
        liquidation_threshold_bps=None,
        e_mode_category=None,
        lltv=lltv,
    )


def _read_aave(gateway: Any, chain: str, wallet: str) -> LendingAccountState | None:
    """Drive the generic reader for the Aave family (whole-account)."""
    return read_lending_account_state(
        protocol="aave_v3",
        chain=chain,
        wallet_address=wallet,
        market_id=None,
        gateway_client=gateway,
        price_oracle=None,
    )


# ─── Shared helpers (parity with VIB-3489 test file) ──────────────────────────


def _encode_word(value: int) -> str:
    return hex(value)[2:].zfill(64)


def _mock_aave_account_response(
    collateral_e8: int = 10_000 * 10**8,
    debt_e8: int = 0,
    available_borrows_e8: int = 0,
    liquidation_threshold_bps: int = 8500,
    ltv_bps: int = 7500,
    health_factor_e18: int = int(30.1 * 1e18),
) -> str:
    """Build the 6-word ``getUserAccountData`` ABI return."""
    return (
        "0x"
        + _encode_word(collateral_e8)
        + _encode_word(debt_e8)
        + _encode_word(available_borrows_e8)
        + _encode_word(liquidation_threshold_bps)
        + _encode_word(ltv_bps)
        + _encode_word(health_factor_e18)
    )


def _mock_emode_response(category: int) -> str:
    """Build the single-word ``getUserEMode`` ABI return."""
    return "0x" + _encode_word(category)


_WALLET = "0x1234567890123456789012345678901234567890"
_CHAIN = "arbitrum"
_AAVE_ARBITRUM_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"  # constant from AAVE_V3_POOL_ADDRESSES


def _make_aave_intent(
    intent_type: str = "SUPPLY",
    *,
    interest_rate_mode: str | None = None,
    omit_irm_attribute: bool = False,
) -> MagicMock:
    """Build a MagicMock that satisfies _intent_type_value + interest_rate_mode access.

    omit_irm_attribute=True simulates a SUPPLY/WITHDRAW intent that NEVER has
    an ``interest_rate_mode`` attribute at all (F5 — defensive ``getattr`` path).
    """
    intent = MagicMock(
        spec=["intent_type", "protocol", "token", "borrow_token", "collateral_token", "market_id"]
        + ([] if omit_irm_attribute else ["interest_rate_mode"])
    )
    intent.intent_type.value = intent_type
    intent.protocol = "aave_v3"
    intent.token = "USDC"
    intent.borrow_token = None
    intent.collateral_token = None
    intent.market_id = None
    if not omit_irm_attribute:
        intent.interest_rate_mode = interest_rate_mode
    return intent


def _make_lending_intent(protocol: str, intent_type: str = "SUPPLY") -> MagicMock:
    """A minimal lending intent (any protocol) for the generic-path gate tests."""
    intent = MagicMock(
        spec=["intent_type", "protocol", "token", "borrow_token", "collateral_token", "market_id", "interest_rate_mode"]
    )
    intent.intent_type.value = intent_type
    intent.protocol = protocol
    intent.token = "USDC"
    intent.borrow_token = None
    intent.collateral_token = None
    intent.market_id = None
    intent.interest_rate_mode = None
    return intent


# =====================================================================
# VIB-4929 PR-3a — generic read-path is opt-in per verified protocol
# =====================================================================


class TestGenericPreStateProtocolGate:
    """The generic ``read_lending_account_state`` path is enabled per-protocol.

    ``_GENERIC_PRE_STATE_PROTOCOLS`` is the explicit enablement allowlist: registering
    an account-state spec (``_ACCOUNT_STATE_LOADERS``) makes a connector spec-*capable*,
    but it reaches the live-money read path only once added here, after its generic read
    is fork-verified. Spark predated its verification — it opted into
    ``_ACCOUNT_STATE_LOADERS`` while gated OUT here (VIB-4963; before the gate it
    silently read HIGH) — and joined in VIB-4929 PR-3c once fork-verified on ethereum.
    These Anvil-free tests pin the membership so a future spec-capable-but-unverified
    connector cannot silently read HIGH.
    """

    def test_enabled_set_matches_verified_protocols(self) -> None:
        # Every lending connector enabled on the live-money read path, each verified in
        # its PR: Aave family (+ alias), Morpho (PR-3a), Compound (PR-3b), Spark (PR-3c).
        assert {"aave_v3", "aave", "morpho_blue", "compound_v3", "spark"} <= _GENERIC_PRE_STATE_PROTOCOLS
        # A non-lending protocol is never enabled here (the gate is not a free-for-all).
        assert "uniswap_v3" not in _GENERIC_PRE_STATE_PROTOCOLS

    def test_spark_intent_reaches_generic_reader(self) -> None:
        """Spark joined the enabled set (VIB-4929 PR-3c) → it now reaches the generic reader."""
        intent = _make_lending_intent("spark")
        sentinel = MagicMock()
        sentinel.family = None  # skip the Aave interest_rate_mode overlay branch (tested elsewhere)
        with patch(
            "almanak.framework.accounting.lending_accounting.read_lending_account_state",
            return_value=sentinel,
        ) as mock_read:
            result = capture_lending_pre_state(
                intent=intent,
                chain=_CHAIN,
                wallet_address=_WALLET,
                gateway_client=MagicMock(),
                price_oracle=None,
            )
        mock_read.assert_called_once()
        assert result is sentinel

    def test_enabled_protocol_reaches_generic_reader(self) -> None:
        """A migrated+enabled protocol (Aave V3) is NOT over-blocked by the gate."""
        intent = _make_lending_intent("aave_v3")
        sentinel = MagicMock()
        sentinel.family = None  # skip the Aave interest_rate_mode overlay branch (tested elsewhere)
        with patch(
            "almanak.framework.accounting.lending_accounting.read_lending_account_state",
            return_value=sentinel,
        ) as mock_read:
            result = capture_lending_pre_state(
                intent=intent,
                chain=_CHAIN,
                wallet_address=_WALLET,
                gateway_client=MagicMock(),
                price_oracle=None,
            )
        mock_read.assert_called_once()
        assert result is sentinel


# =====================================================================
# Selector constant
# =====================================================================


def test_emode_selector_constant_value() -> None:
    """The selector constant must match keccak256('getUserEMode(address)')[:4].

    Independently verified: ``eth_utils.function_signature_to_4byte_selector
    ('getUserEMode(address)').hex() == 'eddf1b79'``.
    """
    assert _AAVE_GET_USER_EMODE_SELECTOR == "0xeddf1b79"


# =====================================================================
# D1 — Correctness: e-mode read on success
# =====================================================================


class TestAaveAccountStateIncludesEmode:
    """When both getUserAccountData and getUserEMode succeed, the state populates."""

    def test_aave_account_state_includes_emode_when_gateway_succeeds(self) -> None:
        """getUserEMode returns 1 → AaveAccountState.e_mode_category == 1."""
        gateway = MagicMock()
        gateway.eth_call.side_effect = [
            _mock_aave_account_response(collateral_e8=10_000 * 10**8, debt_e8=2_000 * 10**8),
            _mock_emode_response(1),
        ]

        state = _read_aave(gateway, _CHAIN, _WALLET)
        assert state is not None
        assert isinstance(state, LendingAccountState)
        assert state.collateral_usd == Decimal("10000")
        assert state.debt_usd == Decimal("2000")
        assert state.e_mode_category == 1
        # interest_rate_mode is set by _capture_aave_v3_pre_state, NOT
        # read_aave_account_state — at this lower layer it stays None.
        assert state.interest_rate_mode is None

    def test_read_aave_user_emode_returns_category(self) -> None:
        """Direct unit test of the new helper."""
        gateway = MagicMock()
        gateway.eth_call.return_value = _mock_emode_response(42)
        assert read_aave_user_emode(gateway, _CHAIN, _WALLET, _AAVE_ARBITRUM_POOL) == 42


# =====================================================================
# D3 F6 — Empty ≠ Zero (LOAD-BEARING)
# =====================================================================


class TestEmodeZeroVsNone:
    """The most important test in this file: F6 — silent-error guard.

    ``e_mode_category == 0`` (real zero: user is not in any e-mode category)
    MUST be distinguishable from ``e_mode_category is None`` (read failed /
    unmeasured). Otherwise the registry consumer cannot tell whether a
    position should be slotted into "not e-mode" or "we don't know" — and
    the wrong answer produces silently-corrupt registry identity tuples.
    """

    def test_aave_account_state_emode_zero_is_distinguishable_from_none(self) -> None:
        state_with_emode_zero = _aave_state(
            collateral_usd=Decimal("10000"),
            debt_usd=Decimal("0"),
            health_factor=Decimal("999999"),
            liquidation_threshold_bps=8500,
            e_mode_category=0,
        )
        state_with_emode_failed = _aave_state(
            collateral_usd=Decimal("10000"),
            debt_usd=Decimal("0"),
            health_factor=Decimal("999999"),
            liquidation_threshold_bps=8500,
            e_mode_category=None,
        )

        # Direct equality
        assert state_with_emode_zero.e_mode_category == 0
        assert state_with_emode_failed.e_mode_category is None
        # Identity check (the load-bearing assertion)
        assert state_with_emode_zero.e_mode_category is not None
        # JSON round-trip preserves the distinction
        zero_json = lending_state_to_dict(state_with_emode_zero, protocol="aave_v3")
        failed_json = lending_state_to_dict(state_with_emode_failed, protocol="aave_v3")
        assert zero_json is not None and failed_json is not None
        assert zero_json["e_mode_category"] == 0
        assert failed_json["e_mode_category"] is None
        # And the two dicts are distinguishable on this exact key
        assert zero_json["e_mode_category"] != failed_json["e_mode_category"]

    def test_aave_account_state_emode_zero_from_real_gateway_response(self) -> None:
        """End-to-end: an actual 0 word from getUserEMode lands as ``0``, not ``None``."""
        gateway = MagicMock()
        gateway.eth_call.side_effect = [
            _mock_aave_account_response(collateral_e8=10_000 * 10**8),
            _mock_emode_response(0),
        ]
        state = _read_aave(gateway, _CHAIN, _WALLET)
        assert state is not None
        assert state.e_mode_category == 0
        assert state.e_mode_category is not None


# =====================================================================
# D3 F1, F2 — read failure paths
# =====================================================================


class TestEmodeReadFailures:
    """Gateway failure / malformed response → e_mode_category is None.

    These tests demonstrate that a failure in the e-mode arm does NOT poison
    the rest of the state — collateral_usd, debt_usd, health_factor still
    populate. The state captures what it could.
    """

    def test_aave_account_state_emode_none_when_gateway_returns_none(self) -> None:
        """F1: gateway returns None for the e-mode call → e_mode_category=None."""
        gateway = MagicMock()
        gateway.eth_call.side_effect = [
            _mock_aave_account_response(collateral_e8=10_000 * 10**8),
            None,  # getUserEMode returns no data
        ]
        state = _read_aave(gateway, _CHAIN, _WALLET)
        assert state is not None  # the primary read succeeded
        assert state.collateral_usd == Decimal("10000")
        assert state.e_mode_category is None

    def test_aave_account_state_emode_none_when_gateway_returns_malformed(self) -> None:
        """F2: gateway returns short / non-hex string → e_mode_category=None."""
        gateway = MagicMock()
        gateway.eth_call.side_effect = [
            _mock_aave_account_response(collateral_e8=10_000 * 10**8),
            "0x",  # empty payload — < 64 hex chars
        ]
        state = _read_aave(gateway, _CHAIN, _WALLET)
        assert state is not None
        assert state.e_mode_category is None

    def test_aave_account_state_emode_none_when_eth_call_raises(self) -> None:
        """F1 variant: side_effect runs out (StopIteration) → caught by _gateway_eth_call."""
        gateway = MagicMock()
        # Only one response provided — the e-mode call raises StopIteration.
        gateway.eth_call.side_effect = [_mock_aave_account_response(collateral_e8=10_000 * 10**8)]
        state = _read_aave(gateway, _CHAIN, _WALLET)
        assert state is not None
        assert state.e_mode_category is None

    def test_aave_account_state_emode_none_when_value_outside_uint8_range(self) -> None:
        """Defensive: decoded value > 255 = treated as read failure (mock reused the wrong hex)."""
        gateway = MagicMock()
        # E-mode arm returns the SAME 6-word hex as the primary read — its
        # word 0 is collateral_e8 = 10_000 * 10**8 = 1e12, way above uint8.
        big_response = _mock_aave_account_response(collateral_e8=10_000 * 10**8)
        gateway.eth_call.return_value = big_response
        state = _read_aave(gateway, _CHAIN, _WALLET)
        assert state is not None
        assert state.e_mode_category is None  # NOT 1_000_000_000_000


# =====================================================================
# D1 — interest_rate_mode pass-through
# =====================================================================


class TestInterestRateModePassThrough:
    """_capture_aave_v3_pre_state threads intent.interest_rate_mode for BORROW/REPAY only."""

    def _gateway_returning(self, account_response: str, emode_value: int = 0) -> MagicMock:
        gw = MagicMock()
        gw.eth_call.side_effect = [account_response, _mock_emode_response(emode_value)]
        return gw

    def test_capture_aave_pre_state_threads_intent_interest_rate_mode_for_borrow(self) -> None:
        gateway = self._gateway_returning(_mock_aave_account_response(debt_e8=5_000 * 10**8))
        intent = _make_aave_intent("BORROW", interest_rate_mode="variable")
        state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            price_oracle=None,
        )
        assert state is not None
        assert state.interest_rate_mode == "variable"

    def test_capture_aave_pre_state_threads_intent_interest_rate_mode_for_repay(self) -> None:
        gateway = self._gateway_returning(_mock_aave_account_response(debt_e8=5_000 * 10**8))
        intent = _make_aave_intent("REPAY", interest_rate_mode="variable")
        state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            price_oracle=None,
        )
        assert state is not None
        assert state.interest_rate_mode == "variable"

    def test_capture_aave_pre_state_interest_rate_mode_none_for_supply(self) -> None:
        gateway = self._gateway_returning(_mock_aave_account_response())
        intent = _make_aave_intent("SUPPLY")
        state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            price_oracle=None,
        )
        assert state is not None
        assert state.interest_rate_mode is None

    def test_capture_aave_pre_state_interest_rate_mode_none_for_withdraw(self) -> None:
        gateway = self._gateway_returning(_mock_aave_account_response())
        intent = _make_aave_intent("WITHDRAW")
        state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            price_oracle=None,
        )
        assert state is not None
        assert state.interest_rate_mode is None

    def test_intent_without_interest_rate_mode_attribute_safe(self) -> None:
        """F5: BORROW intent missing the attribute entirely — defensive getattr path.

        Even when the attribute is absent, the compiler will dispatch the BORROW
        with ``AAVE_VARIABLE_RATE_MODE`` (codex review). Surface that fact in
        pre_state_json so registry/PnL consumers see the actual on-chain rate.
        """
        gateway = self._gateway_returning(_mock_aave_account_response())
        intent = _make_aave_intent("BORROW", omit_irm_attribute=True)
        state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            price_oracle=None,
        )
        assert state is not None
        # The attribute is absent → getattr returns None → BORROW path
        # normalizes to the compiler default "variable".
        assert state.interest_rate_mode == "variable"

    def test_capture_aave_pre_state_borrow_with_explicit_none_defaults_to_variable(self) -> None:
        """codex review: when intent.interest_rate_mode is explicitly None for a
        BORROW, the compiler will still dispatch variable. Surface that.
        """
        gateway = self._gateway_returning(_mock_aave_account_response())
        intent = _make_aave_intent("BORROW", interest_rate_mode=None)
        state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            price_oracle=None,
        )
        assert state is not None
        assert state.interest_rate_mode == "variable"

    def test_capture_aave_pre_state_repay_with_explicit_none_defaults_to_variable(self) -> None:
        """Same normalization for REPAY: the compiler defaults to variable repay."""
        gateway = self._gateway_returning(_mock_aave_account_response())
        intent = _make_aave_intent("REPAY", interest_rate_mode=None)
        state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            price_oracle=None,
        )
        assert state is not None
        assert state.interest_rate_mode == "variable"

    def test_capture_aave_pre_state_returns_none_when_primary_read_fails(self) -> None:
        """When read_aave_account_state returns None, the capture wrapper returns None
        too — neither e_mode_category nor interest_rate_mode get fabricated."""
        gateway = MagicMock()
        gateway.eth_call.side_effect = [None]  # primary read fails
        intent = _make_aave_intent("BORROW", interest_rate_mode="variable")
        state = capture_lending_pre_state(
            intent=intent,
            chain=_CHAIN,
            wallet_address=_WALLET,
            gateway_client=gateway,
            price_oracle=None,
        )
        assert state is None


# =====================================================================
# D1 — JSON serializer
# =====================================================================


class TestLendingStateToDict:
    """lending_state_to_dict emits both new fields for AaveAccountState."""

    def test_lending_state_to_dict_emits_emode_when_set(self) -> None:
        state = _aave_state(
            collateral_usd=Decimal("10000"),
            debt_usd=Decimal("0"),
            health_factor=Decimal("999999"),
            liquidation_threshold_bps=8500,
            e_mode_category=1,
        )
        out = lending_state_to_dict(state, protocol="aave_v3")
        assert out is not None
        assert out["protocol"] == "aave_v3"
        assert out["e_mode_category"] == 1

    def test_lending_state_to_dict_omits_emode_when_none(self) -> None:
        """When e_mode_category is None (read failed), JSON key is null (not absent).

        Null-vs-absent is the consumer's contract: a json_extract on a null key
        returns NULL; on a missing key it also returns NULL. We emit null
        explicitly so the schema is uniform across rows.
        """
        state = _aave_state(
            collateral_usd=Decimal("10000"),
            debt_usd=Decimal("0"),
            health_factor=Decimal("999999"),
            liquidation_threshold_bps=8500,
            e_mode_category=None,
        )
        out = lending_state_to_dict(state, protocol="aave_v3")
        assert out is not None
        assert "e_mode_category" in out
        assert out["e_mode_category"] is None

    def test_lending_state_to_dict_emits_interest_rate_mode_when_set(self) -> None:
        state = _aave_state(
            collateral_usd=Decimal("10000"),
            debt_usd=Decimal("5000"),
            health_factor=Decimal("1.7"),
            liquidation_threshold_bps=8500,
            e_mode_category=0,
            interest_rate_mode="variable",
        )
        out = lending_state_to_dict(state, protocol="aave_v3")
        assert out is not None
        assert out["interest_rate_mode"] == "variable"

    def test_lending_state_to_dict_omits_interest_rate_mode_when_none(self) -> None:
        state = _aave_state(
            collateral_usd=Decimal("10000"),
            debt_usd=Decimal("0"),
            health_factor=Decimal("999999"),
            liquidation_threshold_bps=8500,
            e_mode_category=0,
            interest_rate_mode=None,
        )
        out = lending_state_to_dict(state, protocol="aave_v3")
        assert out is not None
        assert "interest_rate_mode" in out
        assert out["interest_rate_mode"] is None

    def test_lending_state_to_dict_does_not_emit_aave_fields_for_morpho(self) -> None:
        """F4: Morpho/Compound states do NOT carry e_mode_category / interest_rate_mode keys."""
        morpho_state = _morpho_state(
            collateral_usd=Decimal("10000"),
            debt_usd=Decimal("5000"),
            health_factor=Decimal("1.6"),
            lltv=Decimal("0.86"),
        )
        out = lending_state_to_dict(morpho_state, protocol="morpho_blue")
        assert out is not None
        assert "e_mode_category" not in out
        assert "interest_rate_mode" not in out
        assert "lltv" in out

    def test_lending_state_to_dict_legacy_aave_signature_still_works(self) -> None:
        """Backwards-compat: a code path that builds AaveAccountState without
        the new fields (e.g., a future synthetic test or migration shim) still
        produces a valid dict with None for both new keys."""
        state = _aave_state(
            collateral_usd=Decimal("100"),
            debt_usd=Decimal("0"),
            health_factor=Decimal("999999"),
            liquidation_threshold_bps=8500,
            # e_mode_category and interest_rate_mode default to None
        )
        out = lending_state_to_dict(state, protocol="aave_v3")
        assert out is not None
        assert out["e_mode_category"] is None
        assert out["interest_rate_mode"] is None
        # Existing VIB-3474 fields still populate
        assert out["collateral_usd"] == "100"
        assert out["liquidation_threshold_bps"] == 8500


# =====================================================================
# D3 F4 — negative parity: non-Aave protocols unaffected
# =====================================================================


class TestNonAaveProtocolsUnaffected:
    """A BORROW intent on Morpho Blue / Compound V3 must NOT pick up Aave fields."""

    def test_non_aave_intents_unaffected_by_vib4213_fields(self) -> None:
        """When the protocol dispatch routes to Morpho / Compound (NOT Aave),
        the returned state has no e_mode_category / interest_rate_mode keys
        and the Aave-specific code path is never invoked.

        We construct a Morpho state directly (the dispatch already prevents
        Aave-arm execution for Morpho intents; this test pins the JSON shape).
        """
        morpho_state = _morpho_state(
            collateral_usd=Decimal("10000"),
            debt_usd=Decimal("5000"),
            health_factor=Decimal("1.6"),
            lltv=Decimal("0.86"),
        )
        # VIB-4929 PR-3a: Aave + Morpho share the unified LendingAccountState, so
        # the Aave-only fields exist on the dataclass — but a Morpho state carries
        # NO Aave family discriminator and leaves them unmeasured (None).
        assert morpho_state.family != "aave"
        assert morpho_state.e_mode_category is None
        assert morpho_state.interest_rate_mode is None
        # Serializer omits the keys for non-Aave states (gated on the family
        # discriminator, NOT value-presence).
        out = lending_state_to_dict(morpho_state, protocol="morpho_blue")
        assert out is not None
        assert "e_mode_category" not in out
        assert "interest_rate_mode" not in out


# =====================================================================
# Smoke: existing VIB-3474 path still works (no regression on the baseline)
# =====================================================================


def test_vib3474_existing_path_still_works() -> None:
    """An Aave SUPPLY intent with a healthy gateway still returns the
    VIB-3474 fields populated. The new fields are additive."""
    gateway = MagicMock()
    gateway.eth_call.side_effect = [
        _mock_aave_account_response(
            collateral_e8=15_420 * 10**8 + 50_000_000,  # $15420.50
            debt_e8=8_200 * 10**8,
            liquidation_threshold_bps=8500,
            health_factor_e18=int(1.882 * 1e18),
        ),
        _mock_emode_response(0),
    ]
    intent = _make_aave_intent("SUPPLY")
    state = capture_lending_pre_state(
        intent=intent,
        chain=_CHAIN,
        wallet_address=_WALLET,
        gateway_client=gateway,
        price_oracle=None,
    )
    assert state is not None
    assert isinstance(state, LendingAccountState)
    # VIB-3474 fields
    assert state.collateral_usd == Decimal("15420.50")
    assert state.debt_usd == Decimal("8200")
    assert state.liquidation_threshold_bps == 8500
    # VIB-4213 fields
    assert state.e_mode_category == 0
    assert state.interest_rate_mode is None  # SUPPLY has no rate mode

    # JSON has all of them. Numeric Decimals are stringified — compare back as
    # Decimals so trailing zeros from the 1e8 scale division don't trip us up.
    out: dict[str, Any] = lending_state_to_dict(state, protocol="aave_v3")  # type: ignore[assignment]
    assert out is not None
    assert Decimal(out["collateral_usd"]) == Decimal("15420.50")
    assert Decimal(out["debt_usd"]) == Decimal("8200")
    assert out["liquidation_threshold_bps"] == 8500
    assert out["e_mode_category"] == 0
    assert out["interest_rate_mode"] is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
