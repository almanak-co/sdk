"""Regression tests for VIB-3813 (QA April31 NEW-1) — Mantle Aave HOLD-before-SUPPLY.

QA-PostFixesApril31-Tests.md NEW-1: after VIB-3746 closed the Mantle Anvil
gas-limit blocker, ``aave_v3_lending_mantle`` started reaching SUPPLY against
a frozen WETH reserve (``isActive=true, isFrozen=true``) and reverting on-chain.
The fix replicates the VIB-3744 pre-flight pattern proven on Linea.

These tests pin the contract:

* The strategy emits HOLD on iteration 1 when the collateral reserve is
  frozen, instead of submitting a SUPPLY that the on-chain Aave V3 freeze
  guard reverts.
* Operator-forced repay/withdraw run even when ``_frozen_detected`` is set,
  so a position on a now-frozen reserve can still be unwound.
* The freeze-shaped revert classifier flips ``_frozen_detected`` when the
  pre-flight had to fail open (no gateway), and does NOT false-positive on
  generic reverts.
* Persistent state round-trips ``_frozen_detected`` and ``_failure_details``
  with the 20-entry cap.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.framework.intents import PoolReserveFrozenError
from almanak.framework.intents.vocabulary import (
    HoldIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawIntent,
)
from strategies.incubating.aave_v3_lending_mantle.strategy import (
    AaveV3LendingMantleStrategy,
    _looks_like_freeze_revert,
)

_MANTLE_MOD = "strategies.incubating.aave_v3_lending_mantle.strategy"
_WETH_MANTLE = "0xdEAddEaDdeadDEadDEADDEAddEADDEAddead1111"


def _build_freeze_aware_strategy() -> AaveV3LendingMantleStrategy:
    """Construct a strategy with the deps stubbed (no live gateway)."""
    cfg = {
        "chain": "mantle",
        "wallet_address": "0x" + "aa" * 20,
        "collateral_token": "WETH",
        "collateral_amount": "0.01",
        "borrow_token": "USDC",
        "ltv_target": "0.25",
        "force_action": "",
    }
    s = AaveV3LendingMantleStrategy(
        config=cfg,
        chain="mantle",
        wallet_address="0x" + "aa" * 20,
    )
    # Pretend the runner attached a live gateway so `_check_frozen_pool`
    # exercises the helper path instead of the no-gateway short-circuit.
    gateway = MagicMock()
    gateway.is_connected = True
    s._gateway_client = gateway
    return s


def _patch_token_resolver():
    """Patch the lazy import inside `_check_frozen_pool` to return WETH/Mantle."""
    token = MagicMock()
    token.address = _WETH_MANTLE
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


class TestMantleFrozenPoolPreflight:
    def test_frozen_pool_emits_hold_on_first_iteration(self):
        """The headline VIB-3813 case: a frozen WETH reserve must produce HOLD,
        not submit a SUPPLY that the on-chain Aave V3 freeze guard reverts."""
        s = _build_freeze_aware_strategy()
        with (
            _patch_token_resolver(),
            patch(
                f"{_MANTLE_MOD}.assert_lending_reserve_active",
                side_effect=PoolReserveFrozenError(
                    "Reserve WETH on aave_v3 mantle is not active "
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
            patch(f"{_MANTLE_MOD}.assert_lending_reserve_active", return_value=None),
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
        s._supplied_amount = Decimal("0.01")

        with patch(f"{_MANTLE_MOD}.assert_lending_reserve_active") as mock_check:
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

        with patch(f"{_MANTLE_MOD}.assert_lending_reserve_active") as mock_check:
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

        with patch(f"{_MANTLE_MOD}.assert_lending_reserve_active") as mock_check:
            intent = s.decide(_market_snapshot())

        mock_check.assert_not_called()
        assert isinstance(intent, RepayIntent)

    def test_force_withdraw_runs_even_when_frozen(self):
        """Same as force_repay: explicit withdraw must succeed past the
        frozen guard so a frozen pool can't lock funds out of teardown."""
        s = _build_freeze_aware_strategy()
        s.force_action = "withdraw"
        s._frozen_detected = True
        s._supplied_amount = Decimal("0.01")

        with patch(f"{_MANTLE_MOD}.assert_lending_reserve_active") as mock_check:
            intent = s.decide(_market_snapshot())

        mock_check.assert_not_called()
        assert isinstance(intent, WithdrawIntent)

    def test_force_supply_from_post_supply_state_runs_freeze_preflight(self):
        """CodeRabbit feedback on PR #1987: ``force_action="supply"`` from a
        post-supply state must STILL run the freeze pre-flight, otherwise a
        forced re-supply on a now-frozen reserve would burn gas on a
        ``RESERVE_FROZEN`` revert (the very condition VIB-3813 is closing)."""
        s = _build_freeze_aware_strategy()
        s.force_action = "supply"
        s._state = "borrowed"  # post-supply state — preflight would normally skip
        s._supplied_amount = Decimal("0.01")
        s._borrowed_amount = Decimal("100")

        with patch(
            f"{_MANTLE_MOD}.assert_lending_reserve_active",
            side_effect=PoolReserveFrozenError("Aave V3 WETH reserve frozen"),
        ) as mock_check:
            intent = s.decide(_market_snapshot())

        mock_check.assert_called_once()
        assert isinstance(intent, HoldIntent)
        assert "frozen" in (intent.reason or "").lower()

    def test_no_gateway_fails_open(self):
        """Without a connected gateway, the pre-flight must fail open and
        let the compile path / on-chain revert remain the final guard."""
        s = _build_freeze_aware_strategy()
        s._gateway_client = None  # force the no-gateway branch

        with patch(f"{_MANTLE_MOD}.assert_lending_reserve_active") as mock_check:
            intent = s.decide(_market_snapshot())

        mock_check.assert_not_called()
        # Idle → SUPPLY emit since pre-flight could not run.
        assert isinstance(intent, SupplyIntent)
        assert s._frozen_detected is False

    def test_idle_supply_transitions_state_to_supplying_first(self):
        """The idle SUPPLY emit must transition state BEFORE the intent is
        returned. Without this, the post-revert freeze classifier in
        `on_intent_executed` (gated on `_state == "supplying"`) never fires
        and the strategy keeps re-submitting SUPPLYs that revert."""
        s = _build_freeze_aware_strategy()
        with (
            _patch_token_resolver(),
            patch(f"{_MANTLE_MOD}.assert_lending_reserve_active", return_value=None),
        ):
            intent = s.decide(_market_snapshot())

        assert isinstance(intent, SupplyIntent)
        assert s._state == "supplying"
        assert s._previous_stable_state == "idle"


class TestMantlePostRevertFreezeClassifier:
    """`_looks_like_freeze_revert` heuristic + on_intent_executed integration."""

    def test_aave_v3_short_reason_28_classifies_as_freeze(self):
        """Aave V3 RESERVE_FROZEN code: revert("28")."""
        assert _looks_like_freeze_revert("28") is True
        assert _looks_like_freeze_revert('"28"') is True
        assert _looks_like_freeze_revert("'28'") is True

    def test_client_wrapped_revert_classifies_as_freeze(self):
        """Common wrapper formats produced by web3.py / ethers / cast."""
        assert _looks_like_freeze_revert("execution reverted: 28") is True
        assert _looks_like_freeze_revert("execution reverted: '28'") is True
        assert _looks_like_freeze_revert('execution reverted: "28"') is True
        assert _looks_like_freeze_revert("execution reverted with reason string '28'") is True
        assert _looks_like_freeze_revert("VM Exception while processing transaction: revert 28") is True

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
        assert _looks_like_freeze_revert("Insufficient balance: 0.28 WETH") is False

    def test_post_revert_supply_failure_with_freeze_signature_flips_flag(self):
        """End-to-end: idle → SUPPLY emit → on-chain revert with code "28" →
        `_frozen_detected` flips so iter 2+ HOLDs cleanly."""
        s = _build_freeze_aware_strategy()
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


class TestMantlePersistenceRoundTrip:
    def test_persistent_state_round_trip_with_frozen_flag(self):
        """`_frozen_detected` and `_failure_details` survive resume."""
        s = _build_freeze_aware_strategy()
        s._frozen_detected = True
        s._failure_details = ["PRE_FLIGHT: WETH frozen"]
        s._supplied_amount = Decimal("0.005")
        s._borrowed_amount = Decimal("10")

        snapshot = s.get_persistent_state()
        assert snapshot["frozen_detected"] is True
        assert snapshot["failure_details"] == ["PRE_FLIGHT: WETH frozen"]

        # Reload into a fresh instance.
        s2 = _build_freeze_aware_strategy()
        s2.load_persistent_state(snapshot)

        assert s2._frozen_detected is True
        assert s2._failure_details == ["PRE_FLIGHT: WETH frozen"]
        assert s2._supplied_amount == Decimal("0.005")
        assert s2._borrowed_amount == Decimal("10")

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

    def test_load_persistent_state_handles_string_frozen_flag(self):
        """CodeRabbit feedback on PR #1987: ``bool("false")`` is ``True``;
        a JSON-as-string round-trip of the frozen flag must not lock the
        strategy into a permanent HOLD."""
        s = _build_freeze_aware_strategy()
        # The truthy-string form should resolve to True.
        s.load_persistent_state(
            {"state": "idle", "supplied_amount": "0", "borrowed_amount": "0", "frozen_detected": "true"}
        )
        assert s._frozen_detected is True
        # The falsy-string form must resolve to False (not bool("false") == True).
        s2 = _build_freeze_aware_strategy()
        s2.load_persistent_state(
            {"state": "idle", "supplied_amount": "0", "borrowed_amount": "0", "frozen_detected": "false"}
        )
        assert s2._frozen_detected is False
        # "0" / "off" / "no" / empty string also resolve to False.
        for falsy in ("0", "off", "no", "", "  False  "):
            s3 = _build_freeze_aware_strategy()
            s3.load_persistent_state(
                {"state": "idle", "supplied_amount": "0", "borrowed_amount": "0", "frozen_detected": falsy}
            )
            assert s3._frozen_detected is False, f"Expected False for falsy={falsy!r}"
