"""Regression guards for VIB-3818 (QA-PostFixes April31 NEW-6).

After VIB-3753 (BUG-61) closed the Solana validator program-cloning gap, the
``edge_sol_orca_sol_usdc_lp`` Orca Whirlpool LP demo started reaching the
on-chain Whirlpool program — and crashing at the second CPI invoke with
program error ``0xbbf`` (3007 — ``InitializedTickArrayNotFound``). The
range chosen by the strategy maps to tick-array PDAs that exist on mainnet
but were not cloned into the local validator (and on real mainnet, can fall
outside currently-active arrays for thinly-traded pools).

VIB-3818 is the framework-level "fail before on-chain" mirror of
VIB-3744 / VIB-3815 / VIB-3823:

1. ``OrcaWhirlpoolSDK.validate_tick_arrays_initialized`` does a single
   ``getMultipleAccounts`` call against the lower / upper tick array PDAs
   and raises :class:`OrcaTickArrayUninitializedError` (stable
   ``ERROR_PREFIX = "Orca tick array(s) not initialized"``) if either is
   absent.
2. ``OrcaAdapter.compile_lp_open_intent`` calls the validator after building
   the open-position transaction, so the typed error rides into the standard
   error-bundle path.
3. ``IntentStateMachine._categorize_error`` lists the ERROR_PREFIX in
   ``permanent_keywords`` so retrying with the same range never enters the
   retry-storm.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.orca.exceptions import (
    OrcaTickArrayUninitializedError,
)
from almanak.framework.connectors.orca.models import OrcaPool
from almanak.framework.connectors.orca.sdk import OrcaWhirlpoolSDK
from almanak.framework.intents.state_machine import IntentStateMachine

WALLET = "BWv2BZTNAQjLkS5K17W3oVZqYxKLT7uNGoiEpxoBRvbm"
POOL_ADDR = "7qbRF6YsyGuLUVs6Y1q64bdVrfe4ZcUUz1JRdoVNUJnm"


def _make_pool() -> OrcaPool:
    return OrcaPool(
        address=POOL_ADDR,
        mint_a="So11111111111111111111111111111111111111112",
        mint_b="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        symbol_a="SOL",
        symbol_b="USDC",
        decimals_a=9,
        decimals_b=6,
        tick_spacing=64,
        current_price=80.0,
        vault_a="2N8onv9hbe6sCXk1nh6Y2KtEv2j76mEQE6KRZpr3wG62",
        vault_b="DjJqLPzM4dgF9MeUCXhi9VxbwgvmrTnPgnNakzY9JpL3",
    )


def _build_rpc_response(values: list[dict | None]) -> MagicMock:
    response = MagicMock()
    response.json.return_value = {"result": {"value": values}}
    response.text = ""
    response.raise_for_status = MagicMock()
    return response


class TestValidateTickArraysInitialized:
    @pytest.fixture()
    def sdk(self) -> OrcaWhirlpoolSDK:
        return OrcaWhirlpoolSDK(wallet_address=WALLET)

    def test_both_initialized_passes(self, sdk: OrcaWhirlpoolSDK) -> None:
        """Two non-null entries with non-empty data → no raise."""
        # tick range that produces two distinct PDAs
        valid = {"data": ["AAAA", "base64"]}
        with patch.object(sdk.session, "post") as post:
            post.return_value = _build_rpc_response([valid, valid])
            sdk.validate_tick_arrays_initialized(
                pool=_make_pool(),
                tick_lower=-12_800,
                tick_upper=12_800,
                rpc_url="http://localhost:8899",
            )

    def test_lower_uninitialized_raises(self, sdk: OrcaWhirlpoolSDK) -> None:
        with patch.object(sdk.session, "post") as post:
            post.return_value = _build_rpc_response([None, {"data": ["AAAA", "base64"]}])
            with pytest.raises(OrcaTickArrayUninitializedError) as exc_info:
                sdk.validate_tick_arrays_initialized(
                    pool=_make_pool(),
                    tick_lower=-12_800,
                    tick_upper=12_800,
                    rpc_url="http://localhost:8899",
                )
        assert exc_info.value.tick_lower == -12_800
        assert exc_info.value.tick_upper == 12_800
        assert len(exc_info.value.missing_tick_arrays) == 1

    def test_both_uninitialized_raises_with_both_pdas(
        self, sdk: OrcaWhirlpoolSDK
    ) -> None:
        with patch.object(sdk.session, "post") as post:
            post.return_value = _build_rpc_response([None, None])
            with pytest.raises(OrcaTickArrayUninitializedError) as exc_info:
                sdk.validate_tick_arrays_initialized(
                    pool=_make_pool(),
                    tick_lower=-12_800,
                    tick_upper=12_800,
                    rpc_url="http://localhost:8899",
                )
        assert len(exc_info.value.missing_tick_arrays) == 2

    def test_empty_data_treated_as_uninitialized(self, sdk: OrcaWhirlpoolSDK) -> None:
        """Account exists but has empty data (rent-exempt placeholder) — treat as uninitialized."""
        empty = {"data": ["", "base64"]}
        with patch.object(sdk.session, "post") as post:
            post.return_value = _build_rpc_response([empty, empty])
            with pytest.raises(OrcaTickArrayUninitializedError):
                sdk.validate_tick_arrays_initialized(
                    pool=_make_pool(),
                    tick_lower=-12_800,
                    tick_upper=12_800,
                    rpc_url="http://localhost:8899",
                )

    def test_narrow_range_collides_to_one_pda_passes(
        self, sdk: OrcaWhirlpoolSDK
    ) -> None:
        """When tick_lower and tick_upper land in the same array, only one PDA is queried."""
        valid = {"data": ["AAAA", "base64"]}
        with patch.object(sdk.session, "post") as post:
            post.return_value = _build_rpc_response([valid])
            sdk.validate_tick_arrays_initialized(
                pool=_make_pool(),
                tick_lower=64,
                tick_upper=128,  # both inside [0, 88*64)
                rpc_url="http://localhost:8899",
            )
            # confirm only one PDA went into the request
            request_keys = post.call_args.kwargs["json"]["params"][0]
            assert len(request_keys) == 1

    def test_rpc_no_value_skips_silently(self, sdk: OrcaWhirlpoolSDK) -> None:
        """Transient RPC failure → log + let on-chain surface the real issue."""
        response = MagicMock()
        response.json.return_value = {}
        response.text = "rpc transient"
        response.raise_for_status = MagicMock()
        with patch.object(sdk.session, "post", return_value=response):
            sdk.validate_tick_arrays_initialized(
                pool=_make_pool(),
                tick_lower=-12_800,
                tick_upper=12_800,
                rpc_url="http://localhost:8899",
            )


class TestStateMachineClassifiesTickArrayErrorAsPermanent:
    def test_real_error_message_classifies_as_compilation_permanent(self) -> None:
        """Use the real `str(OrcaTickArrayUninitializedError(...))` so that any
        future edit to the rendered message is automatically validated against
        the state machine's permanent-keyword classification — and so that
        accidental reintroduction of the literal "revert" (which would be
        captured by the generic REVERT branch ahead of permanent_keywords) is
        caught at unit-test time.
        """
        err = OrcaTickArrayUninitializedError(
            pool_address="7qbRF6YsAbcDefGhIjKlMnOpQrStUvWxYzAaaaaaaaa",
            tick_lower=-12_800,
            tick_upper=12_800,
            missing_tick_arrays=["ABCdefghIJklmnOPqrstUVwxyz1111"],
        )
        msg = str(err)
        sm = IntentStateMachine.__new__(IntentStateMachine)  # method-only access
        category = sm._categorize_error(msg)
        assert category == "COMPILATION_PERMANENT", (
            f"Real error message classified as {category!r}, not COMPILATION_PERMANENT. "
            f"Likely cause: message contains a generic substring (e.g. 'revert' / "
            f"'timeout') that wins ahead of the permanent_keywords table. "
            f"Message was: {msg!r}"
        )

    def test_error_message_avoids_revert_substring(self) -> None:
        """Guard the well-known regex hazard: any 'revert' in the message is
        captured by the generic REVERT class before COMPILATION_PERMANENT
        keyword matching runs. This guard pins the absence so future edits
        cannot silently neutralize the retry-storm prevention contract.
        """
        err = OrcaTickArrayUninitializedError(
            pool_address="7qbRF6YsAbcDefGhIjKlMnOpQrStUvWxYzAaaaaaaaa",
            tick_lower=-12_800,
            tick_upper=12_800,
            missing_tick_arrays=["ABCdefghIJklmnOPqrstUVwxyz1111"],
        )
        assert "revert" not in str(err).lower(), (
            "OrcaTickArrayUninitializedError message must not contain the literal "
            "'revert' — the state machine REVERT class fires before "
            "COMPILATION_PERMANENT keyword matching, which would defeat the "
            "entire VIB-3818 retry-storm prevention contract."
        )

    def test_error_prefix_constant_matches_keyword(self) -> None:
        """Guard against silent ERROR_PREFIX edits."""
        prefix = OrcaTickArrayUninitializedError.ERROR_PREFIX
        assert prefix.lower() == "orca tick array(s) not initialized"


class TestStrategyWidensRangeOnTickArrayFailure:
    """The edge_sol_orca_sol_usdc_lp strategy must widen its range and retry
    after an LP_OPEN failure caused by the new pre-flight error.
    """

    def test_widening_state_increments_and_resets(self) -> None:
        # Direct import is import-cheap; the strategy class doesn't open RPC.
        from strategies.incubating.edge_sol_orca_sol_usdc_lp.strategy import (
            EdgeSolOrcaSolUsdcLpStrategy,
        )

        strat = EdgeSolOrcaSolUsdcLpStrategy.__new__(EdgeSolOrcaSolUsdcLpStrategy)
        # Bypass __init__ side-effects (logger config, signal wiring).
        strat._state = "opening"
        strat._position_id = None
        strat._position_opened_at = None
        strat._entry_price = None
        strat._entry_value_usd = None
        strat._range_lower = None
        strat._range_upper = None
        strat._open_attempts = 0
        strat.max_position_usd = 0  # not used in this test
        strat.max_range_widen_attempts = 3
        strat.range_widen_factor = 2
        strat.max_range_pct = 95  # only used by the cap-reached log line

        # Simulate LP_OPEN failure: state machine should bump attempts + return to idle.
        intent = MagicMock()
        intent.intent_type.value = "LP_OPEN"
        strat.on_intent_executed(intent, success=False, result=None)
        assert strat._state == "idle"
        assert strat._open_attempts == 1

        # Second failure
        strat._state = "opening"
        strat.on_intent_executed(intent, success=False, result=None)
        assert strat._open_attempts == 2

        # Third failure: counter reaches max — the clamp must hold.
        strat._state = "opening"
        strat.on_intent_executed(intent, success=False, result=None)
        assert strat._open_attempts == 3

        # Fourth failure must NOT push the counter past max — otherwise a
        # permanently-broken pool would compound the widening multiplier
        # forever (range_widen_factor ** _open_attempts grows unbounded).
        strat._state = "opening"
        strat.on_intent_executed(intent, success=False, result=None)
        assert strat._open_attempts == 3, (
            "Open-attempts counter must clamp at max_range_widen_attempts. "
            "Without the clamp, the widening exponent is unbounded and a "
            "permanently-broken pool would compound the multiplier every "
            "iteration forever."
        )

        # Success resets the counter
        strat._state = "opening"
        result = MagicMock()
        result.position_id = "ABC123"
        strat.on_intent_executed(intent, success=True, result=result)
        assert strat._state == "open"
        assert strat._open_attempts == 0
