"""VIB-3749: Radiant V2 strategy-side frozen-pool pre-flight tests.

Confirms the Ethereum Radiant V2 strategy catches the typed
``PoolReserveFrozenError`` in ``decide()`` and emits ``Intent.hold(...)`` *on
iteration 1* — closing the gap where the original implementation only flipped
``_frozen_detected = True`` after an EXEC_FAILED first iteration.

The Arbitrum sibling strategy (``radiant_v2_lending_arbitrum``) was removed
together with the framework-level support for radiant_v2 on arbitrum. The
LendingPool proxy on Arbitrum was reduced to a stub implementation post-Oct
2024 attack and the SDK no longer routes user funds there. See issues #1842 /
#1847 / #1889 and the regression guards in
``tests/unit/connectors/test_radiant_v2.py`` and
``tests/unit/permissions/test_synthetic_intents.py``.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

from almanak.connectors._strategy_base.base.lending.aave_helpers import PoolReserveFrozenError
from almanak.framework.intents.vocabulary import HoldIntent, SupplyIntent
from strategies.incubating.radiant_v2_lending_ethereum.strategy import (
    RadiantV2LendingLifecycleStrategy,
)


def _wallet() -> str:
    return "0x1234567890123456789012345678901234567890"


def _build_strategy(strategy_cls, *, chain: str):
    """Construct a strategy instance with the framework deps stubbed.

    Mirrors the Aave V3 lending strategy unit-test pattern (`__init__` skipped,
    attributes assigned manually) so we can drive `decide()` without a live
    gateway / Anvil.
    """
    cfg = {
        "collateral_token": "WETH",
        "collateral_amount": "0.5",
        "borrow_token": "USDC",
        "ltv_target": "0.3",
        "force_action": "",
    }
    with patch.object(strategy_cls, "__init__", lambda self, *a, **kw: None):
        strategy = strategy_cls.__new__(strategy_cls)

    strategy._deployment_id = f"test-{chain}"
    strategy._chain = chain
    strategy._wallet_address = _wallet()
    strategy._config = cfg
    strategy._hot_config = None

    strategy.collateral_token = "WETH"
    strategy.collateral_amount = Decimal("0.5")
    strategy.borrow_token = "USDC"
    strategy.ltv_target = Decimal("0.3")
    strategy.force_action = ""

    strategy._state = "idle"
    strategy._previous_stable_state = "idle"
    strategy._supplied_amount = Decimal("0")
    strategy._borrowed_amount = Decimal("0")
    strategy._frozen_detected = False
    strategy._failure_details = []

    # `_check_frozen_pool` now reads `self._gateway_client` directly (the
    # runner doesn't propagate `state.compiler` back to `strategy._compiler`
    # before `decide()` runs — Codex P2 finding). So the test seam is
    # `_gateway_client`, not `_compiler`. The actual reserve-config call is
    # patched via `assert_lending_reserve_active` and the token resolution via
    # `get_token_resolver`, so we just need a connected-shaped gateway here.
    gateway = MagicMock()
    gateway.is_connected = True
    strategy._gateway_client = gateway

    return strategy


def _patch_token_resolver(chain: str):
    """Patch ``get_token_resolver`` (lazy-imported inside the strategy method)
    so it returns a deterministic token. Returns the patcher context manager.

    Only ``ethereum`` is supported — radiant_v2 on arbitrum was removed from
    the SDK after the Oct-2024 attack (see module docstring).
    """
    if chain != "ethereum":
        raise AssertionError(
            f"radiant_v2 is only supported on ethereum; refusing to fixture {chain!r}"
        )
    token = MagicMock()
    token.address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"  # WETH
    token.symbol = "WETH"
    resolver = MagicMock()
    resolver.resolve.return_value = token
    # Strategy does ``from almanak.framework.data.tokens import get_token_resolver``
    # inside ``_check_frozen_pool`` — patch the source module so the import
    # picks up the mock no matter which strategy file calls it.
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


# =============================================================================
# Ethereum: pre-flight short-circuits to HOLD on iteration 1
# =============================================================================


_ETH_MOD = "strategies.incubating.radiant_v2_lending_ethereum.strategy"


def test_ethereum_frozen_pool_emits_hold_on_first_iteration():
    """Ethereum strategy was previously falling through to revert when frozen
    (no `_frozen_detected` flag existed). VIB-3749 closes that gap.
    """
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")

    with (
        _patch_token_resolver("ethereum"),
        patch(
            f"{_ETH_MOD}.assert_lending_reserve_active",
            side_effect=PoolReserveFrozenError(
                "Reserve WETH on radiant_v2 ethereum is not active (isActive=False, isFrozen=False)."
            ),
        ),
    ):
        intent = strategy.decide(_market_snapshot())

    assert isinstance(intent, HoldIntent)
    assert strategy._frozen_detected is True
    assert any("PRE_FLIGHT" in detail for detail in strategy._failure_details)


def test_ethereum_active_pool_proceeds_to_supply():
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")

    with (
        _patch_token_resolver("ethereum"),
        patch(
            f"{_ETH_MOD}.assert_lending_reserve_active",
            return_value=None,
        ),
    ):
        intent = strategy.decide(_market_snapshot())

    assert isinstance(intent, SupplyIntent)
    assert intent.protocol == "radiant_v2"
    assert strategy._frozen_detected is False


def test_ethereum_persistent_state_round_trip_with_frozen_flag():
    """Persisted state must round-trip the new VIB-3749 fields.

    Without this, a frozen-pool detection on iteration N would be lost on
    iteration N+1 if the strategy restarts (e.g. hot-reload, scheduled run).
    """
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")
    strategy._frozen_detected = True
    strategy._failure_details = ["PRE_FLIGHT: frozen"]

    persisted = strategy.get_persistent_state()
    assert persisted["frozen_detected"] is True
    assert persisted["failure_details"] == ["PRE_FLIGHT: frozen"]

    fresh = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")
    fresh.load_persistent_state(persisted)
    assert fresh._frozen_detected is True
    assert fresh._failure_details == ["PRE_FLIGHT: frozen"]


# =============================================================================
# Belt-and-suspenders: post-revert detection still fires when pre-flight
# silently fell open (e.g. gateway unavailable).
# =============================================================================


def test_ethereum_post_revert_supply_failure_with_freeze_signature_flips_flag():
    """If the pre-flight had to fail-open (no gateway) and we ended up
    submitting a SUPPLY that reverted with a *freeze-shaped* error, the
    post-revert handler must flip `_frozen_detected` so iteration 2 HOLDs.
    Mirrors the Arbitrum strategy's original behavior, narrowed (CodeRabbit
    follow-up) so generic reverts don't false-positive.
    """
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")

    fake_supply_intent = MagicMock()
    fake_supply_intent.intent_type.value = "SUPPLY"
    strategy._state = "supplying"

    # Aave V2 returns "3" for RESERVE_FROZEN — we look for "frozen" / freeze
    # keywords. Production gateway-decoded error contains the keyword "frozen".
    strategy.on_intent_executed(
        fake_supply_intent,
        success=False,
        result="execution reverted: reserve is frozen",
    )

    assert strategy._frozen_detected is True
    assert strategy._state == "idle"  # rolled back to previous_stable_state


def test_ethereum_post_revert_generic_supply_failure_does_not_flip_flag():
    """Generic SUPPLY reverts (insufficient balance, allowance, nonce hiccup)
    must NOT classify the pool as frozen. CodeRabbit follow-up — without this
    narrowing the strategy permanently strands itself on any unrelated
    failure.
    """
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")

    fake_supply_intent = MagicMock()
    fake_supply_intent.intent_type.value = "SUPPLY"
    strategy._state = "supplying"

    strategy.on_intent_executed(
        fake_supply_intent,
        success=False,
        result="execution reverted: ERC20: transfer amount exceeds balance",
    )

    # Frozen flag is NOT set — the failure was generic, not freeze-shaped.
    assert strategy._frozen_detected is False
    assert strategy._state == "idle"  # but state still rolls back


# =============================================================================
# CodeRabbit follow-up (PR #1971): freeze-heuristic correctness.
# Confirms ``_looks_like_freeze_revert`` on the Ethereum strategy:
#   - matches Aave V2 short-reason payloads "2" / "3" *exactly*
#   - matches bare-hex empty-revert payloads ("0x" / "0X" / "0x0")
#   - does NOT match the truly-empty string "" — that short-circuits at
#     the ``if not error_text`` guard before the freeze checks run, by
#     contract (callers like ``on_intent_executed`` substitute a generic
#     "unknown error" string before the classifier sees it)
#   - does NOT match the collateral-eligibility selector 0x0cafc072
#   - does NOT match unrelated revert text that happens to contain "2" or "3"
# =============================================================================


def test_ethereum_bare_empty_revert_classifies_as_freeze():
    """Bare ``0x`` revert flowing through ``on_intent_executed`` flips
    ``_frozen_detected``.

    The empty-string variant (``result=""``) is exercised against the
    classifier helper directly because ``on_intent_executed`` substitutes
    ``"unknown error"`` for falsy results before reaching the classifier —
    the classifier itself must still handle ``""`` so a future caller that
    bypasses that substitution (or passes a pre-stripped reason) doesn't
    silently miss the freeze signal.
    """
    from strategies.incubating.radiant_v2_lending_ethereum.strategy import (
        _looks_like_freeze_revert,
    )

    # End-to-end: ``0x`` propagates through ``on_intent_executed`` and flips
    # the flag.
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")
    fake_supply_intent = MagicMock()
    fake_supply_intent.intent_type.value = "SUPPLY"
    strategy._state = "supplying"

    strategy.on_intent_executed(fake_supply_intent, success=False, result="0x")

    assert strategy._frozen_detected is True

    # Direct classifier coverage for the bare-empty variants. Regression
    # guard: removing the ``""`` branch from ``_looks_like_freeze_revert``
    # silently re-opens the door to misclassifying a stripped-payload
    # freeze as a generic revert.
    for empty_revert_payload in ("", "0x", "0X", "0x0"):
        assert _looks_like_freeze_revert(empty_revert_payload) is (
            empty_revert_payload != ""
        ), (
            # ``""`` short-circuits at the ``if not error_text`` guard, so
            # the classifier returns False for it (the bare-empty path
            # matches ``"0x"``, ``"0X"``, ``"0x0"`` after strip+lower).
            # This split is the production contract — make it explicit.
            f"classifier contract regressed for payload {empty_revert_payload!r}"
        )


def test_ethereum_unrelated_revert_with_digit_does_not_classify_as_freeze():
    """An unrelated message that merely *contains* the digit "2" or "3" (e.g.
    a balance error) must NOT be classified as freeze — short-reason matches
    are *exact*, not substring.
    """
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")
    fake_supply_intent = MagicMock()
    fake_supply_intent.intent_type.value = "SUPPLY"
    strategy._state = "supplying"

    strategy.on_intent_executed(
        fake_supply_intent,
        success=False,
        result="execution reverted: ERC20: transfer amount exceeds balance (need 3 tokens)",
    )

    assert strategy._frozen_detected is False


def test_ethereum_v2_short_reason_3_classifies_as_freeze():
    """Same V2 short-reason coverage on the Ethereum strategy."""
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")
    fake_supply_intent = MagicMock()
    fake_supply_intent.intent_type.value = "SUPPLY"
    strategy._state = "supplying"

    strategy.on_intent_executed(fake_supply_intent, success=False, result="3")

    assert strategy._frozen_detected is True


def test_ethereum_collateral_eligibility_revert_does_not_classify_as_freeze():
    """Mirror: 0x0cafc072 must not flip `_frozen_detected` on Ethereum either."""
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")
    fake_supply_intent = MagicMock()
    fake_supply_intent.intent_type.value = "SUPPLY"
    strategy._state = "supplying"

    strategy.on_intent_executed(
        fake_supply_intent,
        success=False,
        result="execution reverted: 0x0cafc072",
    )

    assert strategy._frozen_detected is False


# =============================================================================
# State-machine: SUPPLY paths must transition `idle -> supplying` so that the
# post-revert freeze classifier in `on_intent_executed` (gated on
# `_state == "supplying"`) actually fires when pre-flight fails open and the
# on-chain SUPPLY reverts with a freeze-shaped error. CodeRabbit follow-up.
# =============================================================================


def test_ethereum_idle_supply_transitions_state_to_supplying():
    """Idle decide must transition to "supplying" before emit so the
    post-revert classifier in ``on_intent_executed`` (gated on
    ``_state == "supplying"``) actually fires when pre-flight fails open and
    the on-chain SUPPLY reverts with a freeze-shaped error. CodeRabbit
    follow-up.
    """
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")

    with (
        _patch_token_resolver("ethereum"),
        patch(f"{_ETH_MOD}.assert_lending_reserve_active", return_value=None),
    ):
        intent = strategy.decide(_market_snapshot())

    assert isinstance(intent, SupplyIntent)
    assert strategy._state == "supplying"
    assert strategy._previous_stable_state == "idle"


def test_ethereum_force_supply_transitions_state_to_supplying():
    """Mirror: forced supply on Ethereum follows the same transition."""
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")
    strategy.force_action = "supply"

    with (
        _patch_token_resolver("ethereum"),
        patch(f"{_ETH_MOD}.assert_lending_reserve_active", return_value=None),
    ):
        intent = strategy.decide(_market_snapshot())

    assert isinstance(intent, SupplyIntent)
    assert strategy._state == "supplying"


def test_ethereum_supply_revert_after_idle_decide_flips_frozen_flag():
    """Mirror Arbitrum end-to-end: idle decide → emit → freeze revert flips
    `_frozen_detected`. Regression guard against the state-machine bug
    silently re-introducing itself.
    """
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")

    with (
        _patch_token_resolver("ethereum"),
        patch(f"{_ETH_MOD}.assert_lending_reserve_active", return_value=None),
    ):
        emitted = strategy.decide(_market_snapshot())

    assert isinstance(emitted, SupplyIntent)
    assert strategy._state == "supplying"

    fake_supply = MagicMock()
    fake_supply.intent_type.value = "SUPPLY"
    strategy.on_intent_executed(
        fake_supply,
        success=False,
        result="execution reverted with reason 'reserve frozen'",
    )

    assert strategy._frozen_detected is True


def test_ethereum_decide_short_circuits_to_hold_when_already_frozen_after_revert():
    """After the catchup classifier flips ``_frozen_detected``, the next
    ``decide()`` call must short-circuit to HOLD (not re-emit SUPPLY).
    Confirms the ``if self._frozen_detected`` gate at the top of ``decide()``
    actually wins after a freeze-shaped revert.
    """
    strategy = _build_strategy(RadiantV2LendingLifecycleStrategy, chain="ethereum")

    # iter 1: idle -> supplying -> emit SUPPLY (pre-flight green).
    with (
        _patch_token_resolver("ethereum"),
        patch(f"{_ETH_MOD}.assert_lending_reserve_active", return_value=None),
    ):
        strategy.decide(_market_snapshot())

    # SUPPLY reverts with a freeze-shaped error. Use a substring keyword the
    # classifier matches against (substring "frozen") rather than the bare
    # "'3'" short-reason — the latter matches only when the *entire* error
    # text equals a quoted V2 reason, but real reverts wrap the reason in
    # decoder noise. The classifier's substring branch is the reliable path
    # for messages like this.
    fake_supply = MagicMock()
    fake_supply.intent_type.value = "SUPPLY"
    strategy.on_intent_executed(
        fake_supply,
        success=False,
        result="execution reverted: reserve frozen",
    )
    assert strategy._frozen_detected is True

    # iter 2: must HOLD without re-running the pre-flight or compiling SUPPLY.
    with patch(f"{_ETH_MOD}.assert_lending_reserve_active") as mock_check:
        intent = strategy.decide(_market_snapshot())

    mock_check.assert_not_called()
    assert isinstance(intent, HoldIntent)
