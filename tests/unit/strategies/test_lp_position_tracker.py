"""Unit tests for LPPositionTracker (VIB-3742).

Verifies the framework default that captures bin_ids on LP_OPEN and
auto-injects them onto LP_CLOSE / LP_COLLECT_FEES intents the strategy
returns from ``decide()``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.intents.vocabulary import (
    CollectFeesIntent,
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
)
from almanak.framework.strategies.lp_position_tracker import (
    PERSISTENT_STATE_KEY,
    LPPositionTracker,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _open_intent(
    pool: str = "WAVAX/USDC/20",
    chain: str = "avalanche",
    protocol: str = "traderjoe_v2",
) -> LPOpenIntent:
    return LPOpenIntent(
        pool=pool,
        amount0=Decimal("1.0"),
        amount1=Decimal("20"),
        range_lower=Decimal("5"),
        range_upper=Decimal("500"),
        protocol=protocol,
        chain=chain,
    )


def _close_intent(
    pool: str = "WAVAX/USDC/20",
    chain: str = "avalanche",
    protocol: str = "traderjoe_v2",
    protocol_params: dict | None = None,
) -> LPCloseIntent:
    return LPCloseIntent(
        position_id="0",
        pool=pool,
        protocol=protocol,
        chain=chain,
        protocol_params=protocol_params,
    )


def _result_with_bin_ids(bin_ids: list[int]) -> SimpleNamespace:
    return SimpleNamespace(
        bin_ids=list(bin_ids),
        extracted_data={"bin_ids": list(bin_ids)},
    )


# ---------------------------------------------------------------------------
# Capture
# ---------------------------------------------------------------------------


class TestRecordIntentExecution:
    def test_records_bin_ids_on_lp_open_success(self) -> None:
        tracker = LPPositionTracker()
        bin_ids = [8388605, 8388606, 8388607]

        tracker.record_intent_execution(
            _open_intent(),
            success=True,
            result=_result_with_bin_ids(bin_ids),
            default_chain="avalanche",
        )

        positions = tracker.known_positions()
        assert len(positions) == 1
        ((_, tracked),) = positions.items()
        assert tracked.bin_ids == bin_ids

    def test_does_not_record_on_failure(self) -> None:
        tracker = LPPositionTracker()
        tracker.record_intent_execution(
            _open_intent(),
            success=False,
            result=_result_with_bin_ids([1, 2, 3]),
            default_chain="avalanche",
        )
        assert tracker.known_positions() == {}

    def test_lp_close_clears_tracking(self) -> None:
        tracker = LPPositionTracker()
        tracker.record_intent_execution(
            _open_intent(),
            success=True,
            result=_result_with_bin_ids([1, 2, 3]),
            default_chain="avalanche",
        )
        assert tracker.known_positions()

        tracker.record_intent_execution(
            _close_intent(),
            success=True,
            result=SimpleNamespace(),
            default_chain="avalanche",
        )
        assert tracker.known_positions() == {}

    def test_ignores_non_lp_intents(self) -> None:
        tracker = LPPositionTracker()
        swap = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=Decimal("100"),
            chain="arbitrum",
        )
        tracker.record_intent_execution(
            swap, success=True, result=SimpleNamespace(), default_chain="arbitrum"
        )
        assert tracker.known_positions() == {}

    def test_ignores_unknown_protocols(self) -> None:
        tracker = LPPositionTracker()
        # Made-up protocol — neither bin-based nor NFT-based.
        tracker.record_intent_execution(
            _open_intent(protocol="some_imaginary_protocol"),
            success=True,
            result=_result_with_bin_ids([1, 2, 3]),
            default_chain="avalanche",
        )
        assert tracker.known_positions() == {}

    def test_record_uses_default_chain_when_intent_chain_missing(self) -> None:
        tracker = LPPositionTracker()
        # Intent without chain (use a dict-shaped fake)
        intent = SimpleNamespace(
            intent_type=SimpleNamespace(value="LP_OPEN"),
            protocol="traderjoe_v2",
            pool="WAVAX/USDC/20",
            chain=None,
        )
        tracker.record_intent_execution(
            intent,
            success=True,
            result=_result_with_bin_ids([1, 2, 3]),
            default_chain="avalanche",
        )
        assert len(tracker.known_positions(chain="avalanche")) == 1


# ---------------------------------------------------------------------------
# Injection
# ---------------------------------------------------------------------------


class TestMaybeInject:
    def test_injects_bin_ids_when_missing(self) -> None:
        tracker = LPPositionTracker()
        bin_ids = [10, 11, 12]
        tracker.record_intent_execution(
            _open_intent(),
            success=True,
            result=_result_with_bin_ids(bin_ids),
            default_chain="avalanche",
        )

        intent = _close_intent(protocol_params=None)
        injected = tracker.maybe_inject(intent, default_chain="avalanche")

        assert injected is not intent
        assert injected.protocol_params == {"bin_ids": bin_ids}

    def test_preserves_caller_supplied_bin_ids(self) -> None:
        tracker = LPPositionTracker()
        tracker.record_intent_execution(
            _open_intent(),
            success=True,
            result=_result_with_bin_ids([1, 2, 3]),
            default_chain="avalanche",
        )

        manual = _close_intent(protocol_params={"bin_ids": [99]})
        injected = tracker.maybe_inject(manual, default_chain="avalanche")

        # Tracker must NEVER overwrite caller-supplied bin_ids.
        assert injected.protocol_params == {"bin_ids": [99]}

    def test_returns_same_instance_when_nothing_to_inject(self) -> None:
        tracker = LPPositionTracker()
        # No prior LP_OPEN — nothing to inject.
        intent = _close_intent()
        result = tracker.maybe_inject(intent, default_chain="avalanche")
        assert result is intent

    def test_injects_into_collect_fees_intent(self) -> None:
        tracker = LPPositionTracker()
        bin_ids = [50, 51, 52]
        tracker.record_intent_execution(
            _open_intent(),
            success=True,
            result=_result_with_bin_ids(bin_ids),
            default_chain="avalanche",
        )

        collect = CollectFeesIntent(
            pool="WAVAX/USDC/20",
            protocol="traderjoe_v2",
            chain="avalanche",
        )
        injected = tracker.maybe_inject(collect, default_chain="avalanche")
        assert injected.protocol_params == {"bin_ids": bin_ids}

    def test_no_injection_for_swap(self) -> None:
        tracker = LPPositionTracker()
        tracker.record_intent_execution(
            _open_intent(),
            success=True,
            result=_result_with_bin_ids([1]),
            default_chain="avalanche",
        )
        swap = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount=Decimal("10"),
            chain="avalanche",
        )
        # Swap intents do not carry pool — maybe_inject is a no-op.
        out = tracker.maybe_inject(swap, default_chain="avalanche")
        assert out is swap

    def test_pool_mismatch_does_not_inject(self) -> None:
        tracker = LPPositionTracker()
        tracker.record_intent_execution(
            _open_intent(pool="WAVAX/USDC/20"),
            success=True,
            result=_result_with_bin_ids([1, 2]),
            default_chain="avalanche",
        )

        # Different pool — no injection.
        other = _close_intent(pool="WETH/USDC/20")
        out = tracker.maybe_inject(other, default_chain="avalanche")
        assert out is other


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


class TestPersistence:
    def test_round_trips_through_dict(self) -> None:
        tracker = LPPositionTracker()
        bin_ids = [100, 101, 102]
        tracker.record_intent_execution(
            _open_intent(),
            success=True,
            result=_result_with_bin_ids(bin_ids),
            default_chain="avalanche",
        )

        data = tracker.to_persistent_dict()
        # Pool is canonicalised to lowercase per _PositionKey for case-insensitive
        # lookup symmetry between LP_OPEN and later LP_CLOSE intents.
        assert "traderjoe_v2|avalanche|wavax/usdc/20" in data

        restored = LPPositionTracker()
        restored.load_persistent_dict(data)

        out = restored.maybe_inject(_close_intent(), default_chain="avalanche")
        assert out.protocol_params == {"bin_ids": bin_ids}

    def test_load_tolerates_malformed_keys(self) -> None:
        tracker = LPPositionTracker()
        tracker.load_persistent_dict({"not-a-valid-key": {"bin_ids": [1]}})
        # Should not raise; the malformed key is dropped.
        assert tracker.known_positions() == {}

    def test_persistent_state_key_is_namespaced(self) -> None:
        # Documented invariant: the reserved key uses the framework prefix
        # so user persistent_state never collides with it.
        assert PERSISTENT_STATE_KEY.startswith("__framework_")
        assert PERSISTENT_STATE_KEY.endswith("__")


# ---------------------------------------------------------------------------
# Defensive behaviour
# ---------------------------------------------------------------------------


class TestDefensive:
    def test_inject_never_raises(self) -> None:
        """A tracker fault must never block the strategy intent."""
        tracker = LPPositionTracker()

        # Force the internals into a state where injection might raise.
        class Broken:
            intent_type = SimpleNamespace(value="LP_CLOSE")
            protocol = "traderjoe_v2"
            chain = "avalanche"
            pool = "X/Y/1"

            @property
            def protocol_params(self) -> dict:
                raise RuntimeError("explode")

        out = tracker.maybe_inject(Broken(), default_chain="avalanche")
        # Should return the original (or at least not raise).
        assert out is not None

    def test_record_swallows_errors(self) -> None:
        tracker = LPPositionTracker()

        class Broken:
            @property
            def intent_type(self) -> str:
                raise RuntimeError("explode")

        # Must not raise.
        tracker.record_intent_execution(
            Broken(), success=True, result=SimpleNamespace(), default_chain="avalanche"
        )


# ---------------------------------------------------------------------------
# VIB-5346 fail-closed fungible-LP-chaining allowlist
# ---------------------------------------------------------------------------


def test_fungible_chaining_allowlist_disjoint_from_nft_and_bin() -> None:
    """A connector cannot be both fungible-LP-chaining AND NFT/bin-identity.
    Guards future drift: adding a protocol to the chaining allowlist that also
    appears in the NFT/bin sets would be a contradiction (its position_id is an
    identity, not a fungible amount)."""
    from almanak.framework.strategies.lp_position_tracker import (
        _BIN_BASED_PROTOCOLS,
        _FUNGIBLE_LP_CHAINING_PROTOCOLS,
        _NFT_BASED_PROTOCOLS,
    )

    assert _FUNGIBLE_LP_CHAINING_PROTOCOLS.isdisjoint(_NFT_BASED_PROTOCOLS)
    assert _FUNGIBLE_LP_CHAINING_PROTOCOLS.isdisjoint(_BIN_BASED_PROTOCOLS)


@pytest.mark.parametrize(
    ("protocol", "expected"),
    [
        ("pendle", True),
        ("uniswap_v3", False),
        ("uniswap_v4", False),
        ("traderjoe_v2", False),
        ("aerodrome_slipstream", False),
        ("velodrome_slipstream", False),
        ("sushiswap_v3", False),
        ("pancakeswap_v3", False),
        ("curve", False),
        ("fluid_dex_lp", False),
        (None, False),
        ("", False),
    ],
)
def test_lp_close_amount_chaining_supported(protocol, expected) -> None:
    from almanak.framework.strategies.lp_position_tracker import (
        lp_close_amount_chaining_supported,
    )

    assert lp_close_amount_chaining_supported(protocol) is expected


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
