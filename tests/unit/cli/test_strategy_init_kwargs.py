"""Tests for strategy __init__ kwargs filtering in run.py (VIB-1987).

Validates that the runner only passes kwargs the strategy constructor accepts,
preventing TypeError for user strategies that don't accept chains/chain_wallets.
"""

import inspect
from unittest.mock import patch


def _build_init_kwargs(
    config_instance=None,
    primary_chain: str = "arbitrum",
    wallet_address: str = "0x1234",
    chain_wallets: dict | None = None,
) -> tuple[dict, dict]:
    """Build init_kwargs the same way run.py does (post VIB-1987 fix).

    Returns (init_kwargs, base_kwargs) so tests can verify fallback behavior.
    """
    base_kwargs = {
        "config": config_instance or {"some": "config"},
        "chain": primary_chain,
        "wallet_address": wallet_address,
    }
    optional_kwargs = {}
    if chain_wallets:
        optional_kwargs["chains"] = list(chain_wallets.keys())
        optional_kwargs["chain_wallets"] = chain_wallets
    return {**base_kwargs, **optional_kwargs}, base_kwargs


def _filter_init_kwargs(strategy_class: type, init_kwargs: dict, base_kwargs: dict | None = None) -> dict:
    """Reproduce the filtering logic from run.py for unit testing.

    When introspection fails, falls back to base_kwargs only (VIB-1987 fix).
    """
    if base_kwargs is None:
        base_kwargs = {k: v for k, v in init_kwargs.items() if k in ("config", "chain", "wallet_address")}
    try:
        sig = inspect.signature(strategy_class.__init__)
        params = sig.parameters
        has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        if not has_var_keyword:
            return {k: v for k, v in init_kwargs.items() if k in params}
    except (ValueError, TypeError):
        return base_kwargs
    return init_kwargs


class _FullStrategy:
    """Strategy that accepts all framework kwargs."""

    def __init__(self, config, chain, wallet_address, chains=None, chain_wallets=None):
        self.config = config
        self.chain = chain
        self.wallet_address = wallet_address
        self.chains = chains
        self.chain_wallets = chain_wallets


class _MinimalStrategy:
    """Strategy that only accepts basic params (no chains/chain_wallets)."""

    def __init__(self, config, chain, wallet_address):
        self.config = config
        self.chain = chain
        self.wallet_address = wallet_address


class _KwargsStrategy:
    """Strategy that accepts **kwargs."""

    def __init__(self, config, chain, wallet_address, **kwargs):
        self.config = config
        self.chain = chain
        self.wallet_address = wallet_address
        self.extra = kwargs


class _LegacyDictStrategy:
    """Strategy that only accepts a config dict."""

    def __init__(self, config):
        self.config = config


MULTI_CHAIN_WALLETS = {"arbitrum": "0x1234", "base": "0x5678"}

ALL_KWARGS = {
    "config": {"some": "config"},
    "chain": "arbitrum",
    "wallet_address": "0x1234",
    "chains": ["arbitrum", "base"],
    "chain_wallets": MULTI_CHAIN_WALLETS,
}

BASE_KWARGS = {
    "config": {"some": "config"},
    "chain": "arbitrum",
    "wallet_address": "0x1234",
}


class TestStrategyInitKwargsFiltering:
    """The runner must filter kwargs to match strategy constructor signature."""

    def test_full_strategy_gets_all_kwargs(self):
        filtered = _filter_init_kwargs(_FullStrategy, ALL_KWARGS, BASE_KWARGS)
        assert filtered == ALL_KWARGS
        # Should construct without error
        instance = _FullStrategy(**filtered)
        assert instance.chains == ["arbitrum", "base"]

    def test_minimal_strategy_chains_stripped(self):
        """Strategies without chains/chain_wallets must not receive them."""
        filtered = _filter_init_kwargs(_MinimalStrategy, ALL_KWARGS, BASE_KWARGS)
        assert "chains" not in filtered
        assert "chain_wallets" not in filtered
        assert filtered == BASE_KWARGS
        # Should construct without error
        instance = _MinimalStrategy(**filtered)
        assert instance.chain == "arbitrum"

    def test_kwargs_strategy_gets_everything(self):
        """Strategy with **kwargs should receive all kwargs (no filtering)."""
        filtered = _filter_init_kwargs(_KwargsStrategy, ALL_KWARGS, BASE_KWARGS)
        assert filtered == ALL_KWARGS
        instance = _KwargsStrategy(**filtered)
        assert instance.extra["chains"] == ["arbitrum", "base"]

    def test_legacy_dict_strategy(self):
        """Legacy strategy that only accepts config."""
        filtered = _filter_init_kwargs(_LegacyDictStrategy, ALL_KWARGS, BASE_KWARGS)
        assert filtered == {"config": {"some": "config"}}
        instance = _LegacyDictStrategy(**filtered)
        assert instance.config == {"some": "config"}

    def test_minimal_strategy_without_filtering_raises(self):
        """Without filtering, passing chains to minimal strategy raises TypeError."""
        try:
            _MinimalStrategy(**ALL_KWARGS)
            assert False, "Should have raised TypeError"
        except TypeError as e:
            assert "chains" in str(e)


class TestSingleChainNoChains:
    """VIB-1987: single-chain strategies must never see chains kwarg."""

    def test_single_chain_build_excludes_chains(self):
        """When chain_wallets is None, chains should not be in kwargs."""
        init_kwargs, base_kwargs = _build_init_kwargs(chain_wallets=None)
        assert "chains" not in init_kwargs
        assert "chain_wallets" not in init_kwargs
        assert init_kwargs == base_kwargs

    def test_multi_chain_build_includes_chains(self):
        """When chain_wallets is set, chains should be in kwargs."""
        init_kwargs, _ = _build_init_kwargs(chain_wallets=MULTI_CHAIN_WALLETS)
        assert init_kwargs["chains"] == ["arbitrum", "base"]
        assert init_kwargs["chain_wallets"] == MULTI_CHAIN_WALLETS


class TestIntrospectionFallback:
    """VIB-1987: when inspect.signature fails, fall back to base kwargs only."""

    def test_fallback_excludes_chains(self):
        """Introspection failure must NOT pass chains/chain_wallets."""
        with patch("inspect.signature", side_effect=ValueError("cannot introspect")):
            filtered = _filter_init_kwargs(_MinimalStrategy, ALL_KWARGS, BASE_KWARGS)
        assert "chains" not in filtered
        assert "chain_wallets" not in filtered
        assert filtered == BASE_KWARGS

    def test_fallback_includes_base_kwargs(self):
        """Introspection failure should still pass config, chain, wallet_address."""
        with patch("inspect.signature", side_effect=TypeError("introspection error")):
            filtered = _filter_init_kwargs(_MinimalStrategy, ALL_KWARGS, BASE_KWARGS)
        assert filtered["config"] == {"some": "config"}
        assert filtered["chain"] == "arbitrum"
        assert filtered["wallet_address"] == "0x1234"

    def test_fallback_strategy_instantiates(self):
        """Strategy should instantiate correctly even after introspection failure."""
        with patch("inspect.signature", side_effect=ValueError("cannot introspect")):
            filtered = _filter_init_kwargs(_MinimalStrategy, ALL_KWARGS, BASE_KWARGS)
        instance = _MinimalStrategy(**filtered)
        assert instance.chain == "arbitrum"
