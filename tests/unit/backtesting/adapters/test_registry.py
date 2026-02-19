"""Tests for strategy type detection and adapter resolution."""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.adapters import (
    AdapterRegistry,
    StrategyBacktestAdapter,
    StrategyTypeHint,
    detect_strategy_type,
    get_adapter,
    get_adapter_for_strategy,
    get_adapter_info,
    list_available_adapters,
    register_adapter,
)
from almanak.framework.backtesting.adapters.registry import (
    KNOWN_STRATEGY_TYPES,
    STRATEGY_TYPE_ARBITRAGE,
    STRATEGY_TYPE_LENDING,
    STRATEGY_TYPE_LP,
    STRATEGY_TYPE_PERP,
    STRATEGY_TYPE_SWAP,
    STRATEGY_TYPE_YIELD,
    _detect_from_intents,
    _detect_from_protocols,
    _detect_from_tags,
)

# =============================================================================
# Mock Adapters for Testing
# =============================================================================


class MockLPAdapter(StrategyBacktestAdapter):
    """Mock LP adapter for testing."""

    @property
    def adapter_name(self) -> str:
        return "lp"

    def execute_intent(self, intent: Any, portfolio: Any, market_state: Any) -> Any:
        return None

    def update_position(
        self, position: Any, market_state: Any, elapsed_seconds: float
    ) -> None:
        pass

    def value_position(self, position: Any, market_state: Any) -> Decimal:
        return Decimal("0")

    def should_rebalance(self, position: Any, market_state: Any) -> bool:
        return False


class MockPerpAdapter(StrategyBacktestAdapter):
    """Mock perp adapter for testing."""

    @property
    def adapter_name(self) -> str:
        return "perp"

    def execute_intent(self, intent: Any, portfolio: Any, market_state: Any) -> Any:
        return None

    def update_position(
        self, position: Any, market_state: Any, elapsed_seconds: float
    ) -> None:
        pass

    def value_position(self, position: Any, market_state: Any) -> Decimal:
        return Decimal("0")

    def should_rebalance(self, position: Any, market_state: Any) -> bool:
        return False


# =============================================================================
# Mock Strategy Classes for Testing
# =============================================================================


@dataclass
class MockStrategyMetadata:
    """Mock strategy metadata for testing."""

    name: str = "test_strategy"
    description: str = "Test strategy"
    version: str = "1.0.0"
    author: str = "test"
    tags: list[str] = field(default_factory=list)
    supported_chains: list[str] = field(default_factory=list)
    supported_protocols: list[str] = field(default_factory=list)
    intent_types: list[str] = field(default_factory=list)


class MockStrategy:
    """Mock strategy for testing."""

    STRATEGY_METADATA: MockStrategyMetadata | None = None

    def __init__(
        self,
        tags: list[str] | None = None,
        protocols: list[str] | None = None,
        intent_types: list[str] | None = None,
    ):
        self.STRATEGY_METADATA = MockStrategyMetadata(
            tags=tags or [],
            supported_protocols=protocols or [],
            intent_types=intent_types or [],
        )

    def get_metadata(self) -> MockStrategyMetadata | None:
        return self.STRATEGY_METADATA


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def clean_registry():
    """Clean registry before and after each test."""
    AdapterRegistry.clear()
    yield
    AdapterRegistry.clear()


@pytest.fixture
def registered_adapters():
    """Register test adapters."""
    AdapterRegistry.register(
        "lp", MockLPAdapter, description="LP adapter", aliases=["liquidity"]
    )
    AdapterRegistry.register(
        "perp", MockPerpAdapter, description="Perp adapter", aliases=["perpetual"]
    )
    return {"lp": MockLPAdapter, "perp": MockPerpAdapter}


# =============================================================================
# Tests: Strategy Type Constants
# =============================================================================


def test_known_strategy_types():
    """Test that all expected strategy types are defined."""
    assert STRATEGY_TYPE_LP in KNOWN_STRATEGY_TYPES
    assert STRATEGY_TYPE_PERP in KNOWN_STRATEGY_TYPES
    assert STRATEGY_TYPE_LENDING in KNOWN_STRATEGY_TYPES
    assert STRATEGY_TYPE_ARBITRAGE in KNOWN_STRATEGY_TYPES
    assert STRATEGY_TYPE_SWAP in KNOWN_STRATEGY_TYPES
    assert STRATEGY_TYPE_YIELD in KNOWN_STRATEGY_TYPES


def test_strategy_type_values():
    """Test strategy type constant values."""
    assert STRATEGY_TYPE_LP == "lp"
    assert STRATEGY_TYPE_PERP == "perp"
    assert STRATEGY_TYPE_LENDING == "lending"
    assert STRATEGY_TYPE_ARBITRAGE == "arbitrage"
    assert STRATEGY_TYPE_SWAP == "swap"
    assert STRATEGY_TYPE_YIELD == "yield"


# =============================================================================
# Tests: Tag-based Detection
# =============================================================================


def test_detect_from_tags_lp():
    """Test LP detection from tags."""
    hint = _detect_from_tags(["lp", "trading"])
    assert hint.strategy_type == STRATEGY_TYPE_LP
    assert hint.source == "tags"
    assert "lp" in hint.details


def test_detect_from_tags_perp():
    """Test perp detection from tags."""
    hint = _detect_from_tags(["perpetual", "leverage"])
    assert hint.strategy_type == STRATEGY_TYPE_PERP
    assert hint.confidence == "high"  # Two matching tags


def test_detect_from_tags_lending():
    """Test lending detection from tags."""
    hint = _detect_from_tags(["lending", "borrow"])
    assert hint.strategy_type == STRATEGY_TYPE_LENDING
    assert hint.confidence == "high"


def test_detect_from_tags_arbitrage():
    """Test arbitrage detection from tags."""
    hint = _detect_from_tags(["arb", "mev"])
    assert hint.strategy_type == STRATEGY_TYPE_ARBITRAGE


def test_detect_from_tags_swap():
    """Test swap detection from tags."""
    hint = _detect_from_tags(["trading", "dca"])
    assert hint.strategy_type == STRATEGY_TYPE_SWAP


def test_detect_from_tags_yield():
    """Test yield detection from tags."""
    hint = _detect_from_tags(["yield-farming", "staking"])
    assert hint.strategy_type == STRATEGY_TYPE_YIELD


def test_detect_from_tags_case_insensitive():
    """Test that tag detection is case-insensitive."""
    hint = _detect_from_tags(["LP", "TRADING"])
    assert hint.strategy_type == STRATEGY_TYPE_LP


def test_detect_from_tags_empty():
    """Test detection with empty tags."""
    hint = _detect_from_tags([])
    assert hint.strategy_type is None


def test_detect_from_tags_no_match():
    """Test detection with non-matching tags."""
    hint = _detect_from_tags(["unknown", "custom"])
    assert hint.strategy_type is None
    assert hint.source == "tags"


def test_detect_from_tags_confidence():
    """Test confidence levels based on match count."""
    # Single match = medium confidence
    hint1 = _detect_from_tags(["lp"])
    assert hint1.confidence == "medium"

    # Multiple matches = high confidence
    hint2 = _detect_from_tags(["lp", "liquidity", "amm"])
    assert hint2.confidence == "high"


# =============================================================================
# Tests: Protocol-based Detection
# =============================================================================


def test_detect_from_protocols_lp():
    """Test LP detection from protocols."""
    hint = _detect_from_protocols(["uniswap_v3", "aerodrome"])
    assert hint.strategy_type == STRATEGY_TYPE_LP
    assert hint.source == "protocols"


def test_detect_from_protocols_perp():
    """Test perp detection from protocols."""
    hint = _detect_from_protocols(["gmx_v2", "hyperliquid"])
    assert hint.strategy_type == STRATEGY_TYPE_PERP


def test_detect_from_protocols_lending():
    """Test lending detection from protocols."""
    hint = _detect_from_protocols(["aave_v3", "morpho_blue"])
    assert hint.strategy_type == STRATEGY_TYPE_LENDING


def test_detect_from_protocols_yield():
    """Test yield detection from protocols."""
    hint = _detect_from_protocols(["lido", "ethena"])
    assert hint.strategy_type == STRATEGY_TYPE_YIELD


def test_detect_from_protocols_case_insensitive():
    """Test that protocol detection is case-insensitive."""
    hint = _detect_from_protocols(["UNISWAP_V3"])
    assert hint.strategy_type == STRATEGY_TYPE_LP


def test_detect_from_protocols_empty():
    """Test detection with empty protocols."""
    hint = _detect_from_protocols([])
    assert hint.strategy_type is None


def test_detect_from_protocols_no_match():
    """Test detection with non-matching protocols."""
    hint = _detect_from_protocols(["unknown_protocol"])
    assert hint.strategy_type is None


# =============================================================================
# Tests: Intent-based Detection
# =============================================================================


def test_detect_from_intents_lp():
    """Test LP detection from intents."""
    hint = _detect_from_intents(["LP_OPEN", "LP_CLOSE"])
    assert hint.strategy_type == STRATEGY_TYPE_LP
    assert hint.source == "intents"


def test_detect_from_intents_perp():
    """Test perp detection from intents."""
    hint = _detect_from_intents(["PERP_OPEN", "PERP_CLOSE"])
    assert hint.strategy_type == STRATEGY_TYPE_PERP


def test_detect_from_intents_lending():
    """Test lending detection from intents."""
    hint = _detect_from_intents(["SUPPLY", "BORROW"])
    assert hint.strategy_type == STRATEGY_TYPE_LENDING


def test_detect_from_intents_swap():
    """Test swap detection from intents."""
    hint = _detect_from_intents(["SWAP"])
    assert hint.strategy_type == STRATEGY_TYPE_SWAP


def test_detect_from_intents_priority():
    """Test that LP intents have priority over SWAP."""
    # When strategy uses both LP and SWAP, LP should win
    hint = _detect_from_intents(["SWAP", "LP_OPEN"])
    assert hint.strategy_type == STRATEGY_TYPE_LP


def test_detect_from_intents_case_insensitive():
    """Test that intent detection is case-insensitive."""
    hint = _detect_from_intents(["lp_open", "swap"])
    assert hint.strategy_type == STRATEGY_TYPE_LP


def test_detect_from_intents_empty():
    """Test detection with empty intents."""
    hint = _detect_from_intents([])
    assert hint.strategy_type is None


def test_detect_from_intents_no_match():
    """Test detection with non-matching intents."""
    hint = _detect_from_intents(["UNKNOWN_INTENT"])
    assert hint.strategy_type is None


# =============================================================================
# Tests: Main detect_strategy_type Function
# =============================================================================


def test_detect_strategy_type_explicit_config():
    """Test detection with explicit strategy_type in config."""
    strategy = MockStrategy()
    hint = detect_strategy_type(strategy, config={"strategy_type": "perp"})
    assert hint.strategy_type == STRATEGY_TYPE_PERP
    assert hint.source == "explicit"
    assert hint.confidence == "high"


def test_detect_strategy_type_explicit_unknown():
    """Test detection with unknown explicit type falls through."""
    strategy = MockStrategy(tags=["lp"])
    hint = detect_strategy_type(strategy, config={"strategy_type": "unknown_type"})
    # Should fall through to tag detection
    assert hint.strategy_type == STRATEGY_TYPE_LP
    assert hint.source == "tags"


def test_detect_strategy_type_from_strategy_tags():
    """Test detection from strategy instance tags."""
    strategy = MockStrategy(tags=["liquidity-provider", "concentrated-liquidity"])
    hint = detect_strategy_type(strategy)
    assert hint.strategy_type == STRATEGY_TYPE_LP
    assert hint.source == "tags"


def test_detect_strategy_type_from_strategy_protocols():
    """Test detection from strategy instance protocols."""
    strategy = MockStrategy(protocols=["aave_v3", "compound_v3"])
    hint = detect_strategy_type(strategy)
    assert hint.strategy_type == STRATEGY_TYPE_LENDING
    assert hint.source == "protocols"


def test_detect_strategy_type_from_strategy_intents():
    """Test detection from strategy instance intents."""
    strategy = MockStrategy(intent_types=["PERP_OPEN", "PERP_CLOSE"])
    hint = detect_strategy_type(strategy)
    assert hint.strategy_type == STRATEGY_TYPE_PERP
    assert hint.source == "intents"


def test_detect_strategy_type_priority():
    """Test detection priority: tags > protocols > intents."""
    # Tags should win even when protocols and intents suggest different
    strategy = MockStrategy(
        tags=["lending"],
        protocols=["uniswap_v3"],  # Would suggest LP
        intent_types=["SWAP"],  # Would suggest SWAP
    )
    hint = detect_strategy_type(strategy)
    assert hint.strategy_type == STRATEGY_TYPE_LENDING
    assert hint.source == "tags"


def test_detect_strategy_type_dict_strategy():
    """Test detection from dict-based strategy spec."""
    strategy_dict = {
        "metadata": {
            "tags": ["perp", "leverage"],
            "supported_protocols": ["gmx_v2"],
            "intent_types": ["PERP_OPEN"],
        }
    }
    hint = detect_strategy_type(strategy_dict)
    assert hint.strategy_type == STRATEGY_TYPE_PERP


def test_detect_strategy_type_no_metadata():
    """Test detection with strategy that has no metadata."""

    class BareBoneStrategy:
        pass

    strategy = BareBoneStrategy()
    hint = detect_strategy_type(strategy)
    assert hint.strategy_type is None
    assert hint.source == "none"


def test_detect_strategy_type_none_config():
    """Test detection with None config."""
    strategy = MockStrategy(tags=["swap"])
    hint = detect_strategy_type(strategy, config=None)
    assert hint.strategy_type == STRATEGY_TYPE_SWAP


# =============================================================================
# Tests: Adapter Registry Integration
# =============================================================================


def test_get_adapter_registered(registered_adapters):
    """Test getting a registered adapter."""
    adapter = get_adapter("lp")
    assert adapter is not None
    assert isinstance(adapter, MockLPAdapter)


def test_get_adapter_alias(registered_adapters):
    """Test getting adapter via alias."""
    adapter = get_adapter("liquidity")
    assert adapter is not None
    assert isinstance(adapter, MockLPAdapter)


def test_get_adapter_not_registered():
    """Test getting unregistered adapter returns None."""
    adapter = get_adapter("nonexistent")
    assert adapter is None


def test_get_adapter_case_insensitive(registered_adapters):
    """Test that adapter lookup is case-insensitive."""
    adapter = get_adapter("LP")
    assert adapter is not None
    assert isinstance(adapter, MockLPAdapter)


def test_list_available_adapters(registered_adapters):
    """Test listing available adapters."""
    adapters = list_available_adapters()
    assert "lp" in adapters
    assert "perp" in adapters


def test_list_available_adapters_empty():
    """Test listing when no adapters registered."""
    adapters = list_available_adapters()
    assert adapters == []


def test_get_adapter_info(registered_adapters):
    """Test getting adapter info."""
    info = get_adapter_info("lp")
    assert info is not None
    assert info["name"] == "lp"
    assert info["description"] == "LP adapter"
    assert "liquidity" in info["aliases"]
    assert info["adapter_class"] == "MockLPAdapter"


def test_get_adapter_info_not_found():
    """Test getting info for unregistered adapter."""
    info = get_adapter_info("nonexistent")
    assert info is None


# =============================================================================
# Tests: get_adapter_for_strategy Function
# =============================================================================


def test_get_adapter_for_strategy_detected(registered_adapters):
    """Test getting adapter for strategy with detected type."""
    strategy = MockStrategy(tags=["lp", "liquidity"])
    adapter = get_adapter_for_strategy(strategy)
    assert adapter is not None
    assert isinstance(adapter, MockLPAdapter)


def test_get_adapter_for_strategy_explicit(registered_adapters):
    """Test getting adapter with explicit config type."""
    strategy = MockStrategy()
    adapter = get_adapter_for_strategy(strategy, config={"strategy_type": "perp"})
    assert adapter is not None
    assert isinstance(adapter, MockPerpAdapter)


def test_get_adapter_for_strategy_no_match():
    """Test that None is returned when no adapter matches."""
    strategy = MockStrategy(tags=["unknown"])
    adapter = get_adapter_for_strategy(strategy)
    assert adapter is None


def test_get_adapter_for_strategy_detected_but_not_registered():
    """Test when type is detected but no adapter registered."""
    # No adapters registered in this test
    strategy = MockStrategy(tags=["lp"])
    adapter = get_adapter_for_strategy(strategy)
    assert adapter is None  # Type detected but adapter not registered


# =============================================================================
# Tests: Register Adapter Decorator
# =============================================================================


def test_register_adapter_decorator():
    """Test the register_adapter decorator."""

    @register_adapter("test_adapter", description="Test adapter")
    class TestAdapter(StrategyBacktestAdapter):
        @property
        def adapter_name(self) -> str:
            return "test_adapter"

        def execute_intent(self, i: Any, p: Any, m: Any) -> Any:
            return None

        def update_position(self, p: Any, m: Any, e: float) -> None:
            pass

        def value_position(self, p: Any, m: Any) -> Decimal:
            return Decimal("0")

        def should_rebalance(self, p: Any, m: Any) -> bool:
            return False

    adapter = get_adapter("test_adapter")
    assert adapter is not None
    assert adapter.adapter_name == "test_adapter"


def test_register_adapter_with_aliases():
    """Test registering adapter with aliases."""

    @register_adapter("main_name", aliases=["alias1", "alias2"])
    class AliasedAdapter(StrategyBacktestAdapter):
        @property
        def adapter_name(self) -> str:
            return "main_name"

        def execute_intent(self, i: Any, p: Any, m: Any) -> Any:
            return None

        def update_position(self, p: Any, m: Any, e: float) -> None:
            pass

        def value_position(self, p: Any, m: Any) -> Decimal:
            return Decimal("0")

        def should_rebalance(self, p: Any, m: Any) -> bool:
            return False

    # Should be accessible via main name and aliases
    assert get_adapter("main_name") is not None
    assert get_adapter("alias1") is not None
    assert get_adapter("alias2") is not None


# =============================================================================
# Tests: StrategyTypeHint Dataclass
# =============================================================================


def test_strategy_type_hint_defaults():
    """Test StrategyTypeHint default values."""
    hint = StrategyTypeHint(strategy_type="lp")
    assert hint.strategy_type == "lp"
    assert hint.confidence == "low"
    assert hint.source == "none"
    assert hint.details == ""


def test_strategy_type_hint_with_values():
    """Test StrategyTypeHint with all values."""
    hint = StrategyTypeHint(
        strategy_type="perp",
        confidence="high",
        source="tags",
        details="Matched tags: perp, leverage",
    )
    assert hint.strategy_type == "perp"
    assert hint.confidence == "high"
    assert hint.source == "tags"
    assert "perp" in hint.details


def test_strategy_type_hint_none_type():
    """Test StrategyTypeHint with None type."""
    hint = StrategyTypeHint(strategy_type=None)
    assert hint.strategy_type is None
