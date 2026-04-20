"""Tests for VIB-1816: multi-chain detection via decorator metadata fallback.

The decorator's supported_chains parameter is portability metadata stored in
STRATEGY_METADATA. The decorator must NOT set the SUPPORTED_CHAINS class attribute,
because the CLI's is_multi_chain_strategy() treats that as a runtime multi-chain
signal (MultiChainOrchestrator). Instead, is_multi_chain() and get_supported_chains()
fall back to STRATEGY_METADATA.supported_chains when SUPPORTED_CHAINS is not manually set.
"""

from types import SimpleNamespace

import pytest

from almanak.framework.strategies.intent_strategy import IntentStrategy, almanak_strategy


def _stub_decide(self, market):
    return None


def _stub_get_open_positions(self):
    return None


def _stub_generate_teardown_intents(self, mode, market=None):
    return []


def _make_concrete_class(name, bases, attrs=None):
    """Create a concrete (non-abstract) subclass of IntentStrategy."""
    d = {
        "decide": _stub_decide,
        "get_open_positions": _stub_get_open_positions,
        "generate_teardown_intents": _stub_generate_teardown_intents,
    }
    if attrs:
        d.update(attrs)
    return type(name, bases, d)


class TestDecoratorDoesNotSetSupportedChains:
    """Verify @almanak_strategy does NOT set SUPPORTED_CHAINS class attribute.

    The CLI's is_multi_chain_strategy() treats SUPPORTED_CHAINS as a runtime
    multi-chain signal. The decorator must only store chains in STRATEGY_METADATA
    (portability metadata), not SUPPORTED_CHAINS (runtime signal).
    """

    def test_decorator_does_not_set_supported_chains_for_multi(self):
        """Multiple supported_chains must NOT set SUPPORTED_CHAINS on the class."""
        TestStrategy = _make_concrete_class("TestMulti", (IntentStrategy,))

        decorated = almanak_strategy(
            name="test_multi",
            supported_chains=["base", "avalanche"],
        )(TestStrategy)

        # SUPPORTED_CHAINS must NOT be set — it's a runtime multi-chain signal
        assert not hasattr(decorated, "SUPPORTED_CHAINS")
        # But metadata must have the chains
        assert decorated.STRATEGY_METADATA.supported_chains == ["base", "avalanche"]

    def test_decorator_does_not_set_supported_chains_for_single(self):
        """Single chain should NOT set SUPPORTED_CHAINS."""
        TestStrategy = _make_concrete_class("TestSingle", (IntentStrategy,))

        decorated = almanak_strategy(
            name="test_single",
            supported_chains=["base"],
        )(TestStrategy)

        assert not hasattr(decorated, "SUPPORTED_CHAINS")

    def test_decorator_does_not_set_supported_chains_when_none(self):
        """No supported_chains should NOT set SUPPORTED_CHAINS."""
        TestStrategy = _make_concrete_class("TestNone", (IntentStrategy,))

        decorated = almanak_strategy(
            name="test_none",
        )(TestStrategy)

        assert not hasattr(decorated, "SUPPORTED_CHAINS")

    def test_metadata_populated_for_multi_chain(self):
        """STRATEGY_METADATA.supported_chains should be populated."""
        TestStrategy = _make_concrete_class("TestMeta", (IntentStrategy,))

        decorated = almanak_strategy(
            name="test_meta_multi",
            supported_chains=["base", "arbitrum"],
        )(TestStrategy)

        assert decorated.STRATEGY_METADATA.supported_chains == ["base", "arbitrum"]


class TestIsMultiChainInstanceMethod:
    """Test IntentStrategy.is_multi_chain() with decorator-set attributes."""

    def _make_strategy(self, cls_attrs=None, chain="base"):
        """Create a minimal strategy instance with a concrete subclass."""
        attrs = {
            "decide": _stub_decide,
            "get_open_positions": _stub_get_open_positions,
            "generate_teardown_intents": _stub_generate_teardown_intents,
        }
        if cls_attrs:
            attrs.update(cls_attrs)
        klass = type("FakeStrategy", (IntentStrategy,), attrs)
        instance = object.__new__(klass)
        instance._chain = chain
        instance._wallet_address = "0x1234"
        return instance

    def test_is_multi_chain_with_supported_chains_attr(self):
        """is_multi_chain() returns True when SUPPORTED_CHAINS has >1 chain."""
        strategy = self._make_strategy(cls_attrs={"SUPPORTED_CHAINS": ["base", "avalanche"]})
        assert strategy.is_multi_chain() is True

    def test_is_multi_chain_false_with_single_chain(self):
        """is_multi_chain() returns False when SUPPORTED_CHAINS has 1 chain."""
        strategy = self._make_strategy(cls_attrs={"SUPPORTED_CHAINS": ["base"]})
        assert strategy.is_multi_chain() is False

    def test_is_multi_chain_ignores_metadata(self):
        """is_multi_chain() does NOT fall back to STRATEGY_METADATA.supported_chains.

        Decorator metadata is portability info, not a runtime multi-chain signal.
        The CLI decides multi-chain mode based on config.chains.
        """
        metadata = SimpleNamespace(supported_chains=["base", "arbitrum", "optimism"])
        strategy = self._make_strategy(cls_attrs={"STRATEGY_METADATA": metadata})
        assert strategy.is_multi_chain() is False

    def test_is_multi_chain_no_attrs_false(self):
        """is_multi_chain() returns False when no chain info is available."""
        strategy = self._make_strategy()
        assert strategy.is_multi_chain() is False


class TestGetSupportedChainsInstanceMethod:
    """Test IntentStrategy.get_supported_chains() fallback logic."""

    def _make_strategy(self, cls_attrs=None, chain="base"):
        attrs = {
            "decide": _stub_decide,
            "get_open_positions": _stub_get_open_positions,
            "generate_teardown_intents": _stub_generate_teardown_intents,
        }
        if cls_attrs:
            attrs.update(cls_attrs)
        klass = type("FakeStrategy", (IntentStrategy,), attrs)
        instance = object.__new__(klass)
        instance._chain = chain
        instance._wallet_address = "0x1234"
        return instance

    def test_returns_supported_chains_attr(self):
        """get_supported_chains() returns SUPPORTED_CHAINS when set."""
        strategy = self._make_strategy(cls_attrs={"SUPPORTED_CHAINS": ["base", "avalanche"]})
        assert strategy.get_supported_chains() == ["base", "avalanche"]

    def test_fallback_to_metadata(self):
        """get_supported_chains() falls back to STRATEGY_METADATA.supported_chains."""
        metadata = SimpleNamespace(supported_chains=["base", "arbitrum"])
        strategy = self._make_strategy(cls_attrs={"STRATEGY_METADATA": metadata})
        assert strategy.get_supported_chains() == ["base", "arbitrum"]

    def test_fallback_to_instance_chain(self):
        """get_supported_chains() falls back to [self._chain] when nothing else set."""
        strategy = self._make_strategy(chain="arbitrum")
        assert strategy.get_supported_chains() == ["arbitrum"]
