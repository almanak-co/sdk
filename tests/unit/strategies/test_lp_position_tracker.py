"""Unit tests for LPPositionTracker (VIB-3742).

Verifies the framework default that captures bin_ids on LP_OPEN and
auto-injects them onto LP_CLOSE / LP_COLLECT_FEES intents the strategy
returns from ``decide()``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace
from typing import Any

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


_NO_REGISTRY_LOOKUP = object()


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
        tracker.record_intent_execution(swap, success=True, result=SimpleNamespace(), default_chain="arbitrum")
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

    @pytest.mark.parametrize(
        ("existing_params", "expected_params", "expect_copy"),
        [
            (None, {"bin_ids": [10, 11]}, True),
            ({"bin_ids": []}, {"bin_ids": [10, 11]}, True),
            ({"bin_ids": [99]}, {"bin_ids": [99]}, False),
            ({"slippage_bps": 25}, {"slippage_bps": 25, "bin_ids": [10, 11]}, True),
        ],
        ids=["missing", "explicit-falsy", "explicit-truthy", "preserve-other-params"],
    )
    def test_bin_id_injection_truth_table(
        self,
        existing_params: dict[str, Any] | None,
        expected_params: dict[str, Any],
        expect_copy: bool,
    ) -> None:
        tracker = LPPositionTracker()
        tracker.record_intent_execution(
            _open_intent(),
            success=True,
            result=_result_with_bin_ids([10, 11]),
            default_chain="avalanche",
        )
        intent = _close_intent(protocol_params=existing_params)

        result = tracker.maybe_inject(intent, default_chain="avalanche")

        assert (result is not intent) is expect_copy
        assert result.protocol_params == expected_params
        assert intent.protocol_params == existing_params

    @pytest.mark.parametrize(
        ("registry_value", "tracked_value", "existing_params", "expected_params", "expect_copy"),
        [
            (_NO_REGISTRY_LOOKUP, None, None, None, False),
            ("registry-7", None, None, {"position_id": "registry-7"}, True),
            (_NO_REGISTRY_LOOKUP, "tracker-8", None, {"position_id": "tracker-8"}, True),
            (None, "tracker-8", None, {"position_id": "tracker-8"}, True),
            ("registry-7", "tracker-8", None, {"position_id": "registry-7"}, True),
            ("registry-7", "tracker-8", {"position_id": "manual-9"}, {"position_id": "manual-9"}, False),
            ("registry-7", "tracker-8", {"token_id": "manual-9"}, {"token_id": "manual-9"}, False),
            (
                "registry-7",
                "tracker-8",
                {"position_id": "", "token_id": ""},
                {"position_id": "registry-7", "token_id": ""},
                True,
            ),
            ("", None, None, None, False),
            (0, None, None, {"position_id": "0"}, True),
        ],
        ids=[
            "no-source",
            "registry-only",
            "tracker-only",
            "registry-miss-falls-back",
            "registry-wins",
            "manual-position-id-wins",
            "manual-token-id-wins",
            "explicit-falsy-values-are-replaced",
            "empty-registry-id-is-not-injected",
            "integer-zero-registry-id-is-stringified",
        ],
    )
    def test_nft_position_id_injection_truth_table(
        self,
        registry_value: object,
        tracked_value: str | None,
        existing_params: dict[str, Any] | None,
        expected_params: dict[str, Any] | None,
        expect_copy: bool,
    ) -> None:
        tracker = LPPositionTracker()
        if tracked_value is not None:
            tracker.record_intent_execution(
                _open_intent(protocol="uniswap_v3"),
                success=True,
                result=SimpleNamespace(position_id=tracked_value),
                default_chain="avalanche",
            )
        if registry_value is not _NO_REGISTRY_LOOKUP:
            tracker.attach_registry_lookup(lambda **_: registry_value)
        intent = _close_intent(protocol="uniswap_v3", protocol_params=existing_params)

        result = tracker.maybe_inject(intent, default_chain="avalanche")

        assert (result is not intent) is expect_copy
        assert result.protocol_params == expected_params
        assert intent.protocol_params == existing_params

    @pytest.mark.parametrize(
        "protocol",
        [
            "uniswap_v3",
            "uniswap_v4",
            "sushiswap_v3",
            "pancakeswap_v3",
            "aerodrome_slipstream",
            "velodrome_slipstream",
        ],
    )
    def test_registry_injection_supports_each_nft_protocol(self, protocol: str) -> None:
        tracker = LPPositionTracker()
        tracker.attach_registry_lookup(lambda **_: "registry-position")
        intent = _close_intent(protocol=protocol)

        result = tracker.maybe_inject(intent, default_chain="avalanche")

        assert result is not intent
        assert result.protocol_params == {"position_id": "registry-position"}

    @pytest.mark.parametrize(
        ("protocol", "chain", "pool", "default_chain"),
        [
            (None, "avalanche", "pool", "avalanche"),
            ("traderjoe_v2", None, "pool", None),
            ("traderjoe_v2", "avalanche", None, "avalanche"),
        ],
        ids=["missing-protocol", "missing-chain", "missing-pool"],
    )
    def test_missing_position_identity_is_noop(
        self,
        protocol: str | None,
        chain: str | None,
        pool: str | None,
        default_chain: str | None,
    ) -> None:
        tracker = LPPositionTracker()
        intent = SimpleNamespace(
            intent_type="LP_CLOSE",
            protocol=protocol,
            chain=chain,
            pool=pool,
            protocol_params={},
        )

        result = tracker.maybe_inject(intent, default_chain=default_chain)

        assert result is intent

    def test_tracked_metadata_for_unknown_protocol_is_noop(self) -> None:
        tracker = LPPositionTracker()
        tracker.load_persistent_dict({"unknown|avalanche|pool": {"bin_ids": [1], "position_id": "2"}})
        intent = _close_intent(pool="pool", protocol="unknown")

        result = tracker.maybe_inject(intent, default_chain="avalanche")

        assert result is intent

    def test_registry_lookup_precedes_params_read_and_model_copy(self) -> None:
        events: list[object] = []
        tracker = LPPositionTracker()

        def lookup(**kwargs: str) -> str:
            events.append(("registry", kwargs))
            return "registry-position"

        class OrderedIntent:
            intent_type = "LP_CLOSE"
            protocol = "uniswap_v3"
            chain = None
            pool = " POOL "

            @property
            def protocol_params(self) -> dict[str, Any]:
                events.append("protocol_params")
                return {}

            def model_copy(self, *, update: dict[str, Any]) -> SimpleNamespace:
                events.append(("model_copy", update))
                return SimpleNamespace(protocol_params=update["protocol_params"])

        tracker.attach_registry_lookup(lookup)

        result = tracker.maybe_inject(OrderedIntent(), default_chain="AVALANCHE")

        assert result.protocol_params == {"position_id": "registry-position"}
        assert events == [
            (
                "registry",
                {"protocol": "uniswap_v3", "chain": "avalanche", "pool": "pool"},
            ),
            "protocol_params",
            ("model_copy", {"protocol_params": {"position_id": "registry-position"}}),
        ]

    def test_legacy_model_reconstruction_injects_without_mutating_original(self) -> None:
        tracker = LPPositionTracker()
        tracker.load_persistent_dict({"traderjoe_v2|avalanche|pool": {"bin_ids": [1, 2]}})

        class LegacyIntent:
            intent_type = "LP_CLOSE"
            protocol = "traderjoe_v2"
            chain = "avalanche"
            pool = "pool"

            def __init__(self, protocol_params: dict[str, Any] | None = None) -> None:
                self.protocol_params = protocol_params

            def model_dump(self) -> dict[str, Any]:
                return {"protocol_params": self.protocol_params}

            @classmethod
            def model_validate(cls, data: dict[str, Any]) -> LegacyIntent:
                return cls(protocol_params=data["protocol_params"])

        intent = LegacyIntent()

        result = tracker.maybe_inject(intent, default_chain="avalanche")

        assert result is not intent
        assert result.protocol_params == {"bin_ids": [1, 2]}
        assert intent.protocol_params is None

    def test_failed_legacy_model_reconstruction_returns_original_without_warning(self, caplog) -> None:
        tracker = LPPositionTracker()
        tracker.load_persistent_dict({"traderjoe_v2|avalanche|pool": {"bin_ids": [1]}})

        class InvalidLegacyIntent:
            intent_type = "LP_CLOSE"
            protocol = "traderjoe_v2"
            chain = "avalanche"
            pool = "pool"
            protocol_params: dict[str, Any] = {}

            def model_dump(self) -> dict[str, Any]:
                raise ValueError("invalid legacy model")

        intent = InvalidLegacyIntent()

        with caplog.at_level("WARNING"):
            result = tracker.maybe_inject(intent, default_chain="avalanche")

        assert result is intent
        assert not caplog.records

    def test_registry_lookup_failure_logs_debug_and_uses_tracker(self, caplog) -> None:
        tracker = LPPositionTracker()
        tracker.record_intent_execution(
            _open_intent(protocol="uniswap_v3"),
            success=True,
            result=SimpleNamespace(position_id="tracker-position"),
            default_chain="avalanche",
        )

        def failing_lookup(**_: str) -> str:
            raise RuntimeError("registry unavailable")

        tracker.attach_registry_lookup(failing_lookup)

        with caplog.at_level("DEBUG"):
            result = tracker.maybe_inject(
                _close_intent(protocol="uniswap_v3"),
                default_chain="avalanche",
            )

        assert result.protocol_params == {"position_id": "tracker-position"}
        assert "_lookup_registry_position_id failed (non-fatal)" in caplog.text


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
    def test_inject_never_raises(self, caplog) -> None:
        """A tracker fault must never block the strategy intent."""
        tracker = LPPositionTracker()
        tracker.load_persistent_dict({"traderjoe_v2|avalanche|x/y/1": {"bin_ids": [1]}})

        # Force the internals into a state where injection might raise.
        class Broken:
            intent_type = SimpleNamespace(value="LP_CLOSE")
            protocol = "traderjoe_v2"
            chain = "avalanche"
            pool = "X/Y/1"

            @property
            def protocol_params(self) -> dict:
                raise RuntimeError("explode")

        intent = Broken()
        with caplog.at_level("WARNING"):
            out = tracker.maybe_inject(intent, default_chain="avalanche")

        assert out is intent
        assert "LPPositionTracker.maybe_inject failed (non-fatal): explode" in caplog.text

    def test_record_swallows_errors(self) -> None:
        tracker = LPPositionTracker()

        class Broken:
            @property
            def intent_type(self) -> str:
                raise RuntimeError("explode")

        # Must not raise.
        tracker.record_intent_execution(Broken(), success=True, result=SimpleNamespace(), default_chain="avalanche")


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


# ---------------------------------------------------------------------------
# _extract_position_id
# ---------------------------------------------------------------------------


class TestExtractPositionId:
    """Branch coverage for the static NFT position_id extraction helper."""

    def test_none_result_returns_none(self) -> None:
        assert LPPositionTracker._extract_position_id(None) is None

    def test_direct_attribute_wins(self) -> None:
        result = SimpleNamespace(position_id=12345, extracted_data={"position_id": "999"})
        assert LPPositionTracker._extract_position_id(result) == "12345"

    def test_falls_back_to_extracted_data(self) -> None:
        result = SimpleNamespace(position_id=None, extracted_data={"position_id": "678"})
        assert LPPositionTracker._extract_position_id(result) == "678"

    def test_extracted_data_none_returns_none(self) -> None:
        result = SimpleNamespace(position_id=None, extracted_data=None)
        assert LPPositionTracker._extract_position_id(result) is None

    def test_extracted_data_not_a_dict_returns_none(self) -> None:
        result = SimpleNamespace(position_id=None, extracted_data=["position_id", "1"])
        assert LPPositionTracker._extract_position_id(result) is None

    def test_extracted_data_missing_key_returns_none(self) -> None:
        result = SimpleNamespace(position_id=None, extracted_data={"bin_ids": [1]})
        assert LPPositionTracker._extract_position_id(result) is None

    def test_result_without_attributes_returns_none(self) -> None:
        assert LPPositionTracker._extract_position_id(SimpleNamespace()) is None

    def test_int_position_id_stringified(self) -> None:
        result = SimpleNamespace(position_id=None, extracted_data={"position_id": 42})
        assert LPPositionTracker._extract_position_id(result) == "42"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
