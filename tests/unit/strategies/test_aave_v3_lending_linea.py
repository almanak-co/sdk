"""Regression tests for VIB-3745 (BUG-52) — Linea Aave teardown crash.

QA April29 Batch 17: ``aave_v3_lending_linea`` crashed during teardown with
``'AaveV3LendingLineaStrategy' object has no attribute 'state'``. The strategy
read ``self.state.get(...)`` in ``get_open_positions`` and
``generate_teardown_intents`` but never initialized that attribute. The fix
reads from the existing ``self._supplied_amount`` / ``self._borrowed_amount``
instance attributes (which are populated by ``load_persistent_state``).

These tests pin the contract:

* ``get_open_positions`` and ``generate_teardown_intents`` do not raise
  ``AttributeError`` when ``self.state`` does not exist.
* They report the position values from the framework-restored instance
  attributes.
"""

from __future__ import annotations

from decimal import Decimal

from strategies.incubating.aave_v3_lending.strategy import (
    AaveV3LendingStrategy,
)


def _make_strategy() -> AaveV3LendingStrategy:
    return AaveV3LendingStrategy(
        config={
            "chain": "linea",
            "wallet_address": "0x" + "aa" * 20,
        },
        chain="linea",
        wallet_address="0x" + "aa" * 20,
    )


class TestLineaTeardownDoesNotRequireSelfState:
    """The strategy must not depend on a ``self.state`` dict that is never set."""

    def test_get_open_positions_no_position_no_self_state_attr(self):
        s = _make_strategy()
        assert not hasattr(s, "state"), "self.state must not be auto-set"

        # Pre-fix: this raised AttributeError on self.state.get(...).
        summary = s.get_open_positions()
        assert summary.positions == []

    def test_generate_teardown_intents_no_position_no_self_state_attr(self):
        s = _make_strategy()
        assert not hasattr(s, "state")

        intents = s.generate_teardown_intents(mode="graceful")
        assert intents == []

    def test_get_open_positions_reports_supply_and_borrow_from_instance_attrs(self):
        s = _make_strategy()
        # load_persistent_state restores these in the real flow; simulate that.
        s._supplied_amount = Decimal("0.5")
        s._borrowed_amount = Decimal("100")

        summary = s.get_open_positions()

        assert len(summary.positions) == 2
        # Order: supply first, then borrow (matches strategy's emit order).
        supply, borrow = summary.positions
        assert str(supply.position_type).endswith("SUPPLY")
        assert supply.protocol == "aave_v3"
        assert supply.chain == "linea"
        assert supply.value_usd == Decimal("0.5")
        assert str(borrow.position_type).endswith("BORROW")
        assert borrow.value_usd == Decimal("100")

    def test_generate_teardown_intents_emits_repay_then_withdraw(self):
        s = _make_strategy()
        s._supplied_amount = Decimal("0.5")
        s._borrowed_amount = Decimal("100")

        intents = s.generate_teardown_intents(mode="graceful")

        # Order matters: must repay debt before withdrawing collateral.
        assert len(intents) == 2
        assert intents[0].intent_type.value == "REPAY"
        assert intents[1].intent_type.value == "WITHDRAW"

    def test_generate_teardown_intents_skips_unset_legs(self):
        # Borrow only (no supply) — the conditional branches must work
        # independently. (Unrealistic state, but it pins the conditionals.)
        s = _make_strategy()
        s._supplied_amount = Decimal("0")
        s._borrowed_amount = Decimal("50")
        intents = s.generate_teardown_intents(mode="graceful")
        assert len(intents) == 1
        assert intents[0].intent_type.value == "REPAY"

        # Supply only.
        s._supplied_amount = Decimal("0.25")
        s._borrowed_amount = Decimal("0")
        intents = s.generate_teardown_intents(mode="graceful")
        assert len(intents) == 1
        assert intents[0].intent_type.value == "WITHDRAW"


# =============================================================================
# VIB-3744 (QA April29 BUG-51): frozen-pool awareness on Aave V3 Linea
#
# As of 2026-04-30, WETH on Aave V3 Linea is frozen (isFrozen=true, ltv=0).
# Mirrors the VIB-3749 pre-flight pattern.
# =============================================================================

from unittest.mock import MagicMock, patch  # noqa: E402

from almanak.connectors._strategy_base.base.lending.aave_helpers import PoolReserveFrozenError  # noqa: E402
from almanak.framework.intents.vocabulary import (  # noqa: E402
    HoldIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from strategies.incubating.aave_v3_lending.strategy import (  # noqa: E402
    _looks_like_freeze_revert,
)

_LINEA_MOD = "strategies.incubating.aave_v3_lending.strategy"
_WETH_LINEA = "0xe5D7C2a44FfDDf6b295A15c148167daaAf5Cf34f"


def _build_freeze_aware_strategy():
    """Construct a strategy with the deps stubbed (no live gateway)."""
    cfg = {
        "chain": "linea",
        "wallet_address": "0x" + "aa" * 20,
        "collateral_token": "WETH",
        "collateral_amount": "0.5",
        "borrow_token": "USDC",
        "ltv_target": "0.3",
        "force_action": "",
        "check_frozen_reserve": True,
    }
    s = AaveV3LendingStrategy(
        config=cfg,
        chain="linea",
        wallet_address="0x" + "aa" * 20,
    )
    # Pretend the runner attached a live gateway so `_check_frozen_pool`
    # exercises the helper path instead of the no-gateway short-circuit.
    gateway = MagicMock()
    gateway.is_connected = True
    s._gateway_client = gateway
    return s


def _patch_token_resolver():
    """Patch the lazy import inside `_check_frozen_pool` to return WETH/Linea."""
    token = MagicMock()
    token.address = _WETH_LINEA
    token.symbol = "WETH"
    resolver = MagicMock()
    resolver.resolve.return_value = token
    return patch("almanak.framework.data.tokens.get_token_resolver", return_value=resolver)


def _market_snapshot() -> MagicMock:
    market = MagicMock()

    def price_side_effect(token: str):
        if token == "WETH":
            return Decimal("3000")
        if token == "USDC":
            return Decimal("1")
        raise ValueError(f"Unknown token: {token}")

    market.price.side_effect = price_side_effect
    return market


class TestLineaFrozenPoolPreflight:
    def test_frozen_pool_emits_hold_on_first_iteration(self):
        """The headline VIB-3744 case: a frozen WETH reserve must produce HOLD,
        not submit a SUPPLY that the on-chain Aave V3 freeze guard reverts."""
        s = _build_freeze_aware_strategy()
        with (
            _patch_token_resolver(),
            patch(
                f"{_LINEA_MOD}.assert_lending_reserve_active",
                side_effect=PoolReserveFrozenError(
                    "Reserve WETH on aave_v3 linea is not active "
                    "(isActive=True, isFrozen=True). The pool / asset is paused."
                ),
            ),
        ):
            intent = s.decide(_market_snapshot())

        assert isinstance(intent, HoldIntent)
        assert "isFrozen=True" in (intent.reason or "")
        assert s._frozen_detected is True
        assert any("PRE_FLIGHT" in d for d in s._failure_details)

    def test_active_pool_proceeds_to_supply(self):
        """Healthy reserve: pre-flight returns None → strategy emits SUPPLY."""
        s = _build_freeze_aware_strategy()
        with (
            _patch_token_resolver(),
            patch(f"{_LINEA_MOD}.assert_lending_reserve_active", return_value=None),
        ):
            intent = s.decide(_market_snapshot())

        assert isinstance(intent, SupplyIntent)
        assert intent.protocol == "aave_v3"
        assert intent.token == "WETH"
        assert s._frozen_detected is False
        # Idle should have transitioned to "supplying" before emitting SUPPLY
        # so the post-revert classifier is reachable on iter-1 fallback.
        assert s._state == "supplying"

    def test_preflight_skipped_in_post_supply_states(self):
        """Once supplied, downstream paths shouldn't pay the RPC tax."""
        s = _build_freeze_aware_strategy()
        s._state = "supplied"
        s._supplied_amount = Decimal("0.5")

        with patch(f"{_LINEA_MOD}.assert_lending_reserve_active") as mock_check:
            intent = s.decide(_market_snapshot())

        mock_check.assert_not_called()
        assert isinstance(intent, HoldIntent)
        assert intent.reason == "State: supplied"

    def test_already_frozen_short_circuits_without_rpc(self):
        """Once `_frozen_detected` is set, subsequent iterations HOLD without
        re-running the RPC pre-flight."""
        s = _build_freeze_aware_strategy()
        s._frozen_detected = True
        s._failure_details = ["PRE_FLIGHT: previously frozen"]

        with patch(f"{_LINEA_MOD}.assert_lending_reserve_active") as mock_check:
            intent = s.decide(_market_snapshot())

        mock_check.assert_not_called()
        assert isinstance(intent, HoldIntent)
        assert "frozen" in (intent.reason or "").lower()

    def test_force_repay_runs_even_when_frozen(self):
        """Operator-forced repay must NOT be gated by the freeze guard so a
        position on a now-frozen reserve can still be unwound. Mirrors the
        VIB-3749 contract."""
        s = _build_freeze_aware_strategy()
        s.force_action = "repay"
        s._frozen_detected = True
        s._borrowed_amount = Decimal("100")

        with patch(f"{_LINEA_MOD}.assert_lending_reserve_active") as mock_check:
            intent = s.decide(_market_snapshot())

        mock_check.assert_not_called()
        assert isinstance(intent, RepayIntent)

    def test_force_withdraw_runs_even_when_frozen(self):
        """Same as force_repay: explicit withdraw must succeed past the
        frozen guard so a frozen pool can't lock funds out of teardown."""
        s = _build_freeze_aware_strategy()
        s.force_action = "withdraw"
        s._frozen_detected = True
        s._supplied_amount = Decimal("0.5")

        with patch(f"{_LINEA_MOD}.assert_lending_reserve_active") as mock_check:
            intent = s.decide(_market_snapshot())

        mock_check.assert_not_called()
        assert isinstance(intent, WithdrawIntent)

    def test_no_gateway_fails_open(self):
        """Without a connected gateway, the pre-flight must fail open and
        let the compile path / on-chain revert remain the final guard. This
        guarantees we don't break offline / placeholder-mode compiles."""
        s = _build_freeze_aware_strategy()
        s._gateway_client = None  # force the no-gateway branch

        with patch(f"{_LINEA_MOD}.assert_lending_reserve_active") as mock_check:
            intent = s.decide(_market_snapshot())

        mock_check.assert_not_called()
        # Idle → SUPPLY emit since pre-flight could not run.
        assert isinstance(intent, SupplyIntent)
        assert s._frozen_detected is False

    def test_idle_supply_transitions_state_to_supplying_first(self):
        """The lifecycle/idle SUPPLY emit must transition state BEFORE the
        intent is returned. Without this, the post-revert freeze classifier
        in `on_intent_executed` (gated on `_state == "supplying"`) never
        fires and the strategy keeps re-submitting SUPPLYs that revert."""
        s = _build_freeze_aware_strategy()
        with (
            _patch_token_resolver(),
            patch(f"{_LINEA_MOD}.assert_lending_reserve_active", return_value=None),
        ):
            intent = s.decide(_market_snapshot())

        assert isinstance(intent, SupplyIntent)
        assert s._state == "supplying"
        assert s._previous_stable_state == "idle"


class TestLineaPostRevertFreezeClassifier:
    """`_looks_like_freeze_revert` heuristic + on_intent_executed integration."""

    def test_aave_v3_short_reason_28_classifies_as_freeze(self):
        """Aave V3 RESERVE_FROZEN code: revert("28")."""
        assert _looks_like_freeze_revert("28") is True
        assert _looks_like_freeze_revert('"28"') is True
        assert _looks_like_freeze_revert("'28'") is True

    def test_client_wrapped_revert_classifies_as_freeze(self):
        """Common wrapper formats produced by web3.py / ethers / cast."""
        # web3.py ContractLogicError shape
        assert _looks_like_freeze_revert("execution reverted: 28") is True
        assert _looks_like_freeze_revert("execution reverted: '28'") is True
        assert _looks_like_freeze_revert('execution reverted: "28"') is True
        # ethers / hardhat shape
        assert _looks_like_freeze_revert("execution reverted with reason string '28'") is True
        # anvil / hardhat raw revert
        assert _looks_like_freeze_revert("VM Exception while processing transaction: revert 28") is True

    def test_aave_v3_short_reason_27_classifies_as_freeze(self):
        """Aave V3 RESERVE_INACTIVE code: revert("27")."""
        assert _looks_like_freeze_revert("27") is True

    def test_aave_v3_short_reason_29_classifies_as_freeze(self):
        """Aave V3 RESERVE_PAUSED code: revert("29")."""
        assert _looks_like_freeze_revert("29") is True

    def test_human_readable_reserve_frozen_classifies_as_freeze(self):
        """Some clients pretty-print the code: 'execution reverted: RESERVE_FROZEN'."""
        assert _looks_like_freeze_revert("execution reverted: RESERVE_FROZEN") is True
        assert _looks_like_freeze_revert("Reserve frozen on Aave V3") is True

    def test_generic_revert_does_not_classify_as_freeze(self):
        """Insufficient balance / nonce / allowance must NOT trip the classifier
        — that would permanently lock the strategy on transient failures."""
        assert _looks_like_freeze_revert("insufficient funds for gas") is False
        assert _looks_like_freeze_revert("nonce too low") is False
        assert _looks_like_freeze_revert("ERC20: transfer amount exceeds balance") is False
        assert _looks_like_freeze_revert("execution reverted") is False
        assert _looks_like_freeze_revert("") is False

    def test_unrelated_revert_with_digit_28_does_not_false_positive(self):
        """Substring matching would catch '28' inside any text. Exact short-
        reason matching guards against this false positive."""
        # "Insufficient balance: 0.28 WETH" mentions 28 but is not a freeze.
        assert _looks_like_freeze_revert("Insufficient balance: 0.28 WETH") is False

    def test_post_revert_supply_failure_with_freeze_signature_flips_flag(self):
        """End-to-end: idle → SUPPLY emit → on-chain revert with code "28" →
        `_frozen_detected` flips so iter 2+ HOLDs cleanly. Belt-and-suspenders
        path for when the pre-flight had to fail open (no gateway)."""
        s = _build_freeze_aware_strategy()
        # Mirror the runner-side path: the strategy emitted SUPPLY from the
        # idle branch, the runner transitioned _state to "supplying".
        s._transition("supplying")

        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"

        s.on_intent_executed(intent, success=False, result="execution reverted: 28")

        assert s._frozen_detected is True
        assert s._state == "idle"  # rolled back to previous_stable_state

    def test_post_revert_generic_failure_does_not_flip_flag(self):
        """A generic SUPPLY failure (not freeze-shaped) must NOT permanently
        strand the strategy."""
        s = _build_freeze_aware_strategy()
        s._transition("supplying")

        intent = MagicMock()
        intent.intent_type.value = "SUPPLY"

        s.on_intent_executed(intent, success=False, result="nonce too low")

        assert s._frozen_detected is False  # NOT flipped


class TestLineaPersistenceRoundTrip:
    def test_persistent_state_round_trip_with_frozen_flag(self):
        """`_frozen_detected` and `_failure_details` survive resume."""
        s = _build_freeze_aware_strategy()
        s._frozen_detected = True
        s._failure_details = ["PRE_FLIGHT: WETH frozen"]
        s._supplied_amount = Decimal("0.25")
        s._borrowed_amount = Decimal("50")

        snapshot = s.get_persistent_state()
        assert snapshot["frozen_detected"] is True
        assert snapshot["failure_details"] == ["PRE_FLIGHT: WETH frozen"]

        # Reload into a fresh instance.
        s2 = _build_freeze_aware_strategy()
        s2.load_persistent_state(snapshot)

        assert s2._frozen_detected is True
        assert s2._failure_details == ["PRE_FLIGHT: WETH frozen"]
        assert s2._supplied_amount == Decimal("0.25")
        assert s2._borrowed_amount == Decimal("50")

    def test_load_persistent_state_caps_failure_details(self):
        """Defends against a corrupted state with unbounded failure_details."""
        s = _build_freeze_aware_strategy()
        s.load_persistent_state(
            {
                "state": "idle",
                "previous_stable_state": "idle",
                "supplied_amount": "0",
                "borrowed_amount": "0",
                "frozen_detected": True,
                "failure_details": [f"err-{i}" for i in range(100)],
            }
        )
        # The 20-entry cap keeps memory bounded across resumes.
        assert len(s._failure_details) == 20
        # The MOST RECENT entries are kept (tail), not the oldest.
        assert s._failure_details[-1] == "err-99"
