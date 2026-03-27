"""Tests for strategy __init__ kwargs filtering in run.py.

Validates that the runner only passes kwargs the strategy constructor accepts,
preventing TypeError for user strategies that don't accept chains/chain_wallets.
"""

import inspect


def _filter_init_kwargs(strategy_class: type, init_kwargs: dict) -> dict:
    """Reproduce the filtering logic from run.py for unit testing."""
    try:
        sig = inspect.signature(strategy_class.__init__)
        params = sig.parameters
        has_var_keyword = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values())
        if not has_var_keyword:
            return {k: v for k, v in init_kwargs.items() if k in params}
    except (ValueError, TypeError):
        pass
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


ALL_KWARGS = {
    "config": {"some": "config"},
    "chain": "arbitrum",
    "wallet_address": "0x1234",
    "chains": ["arbitrum", "base"],
    "chain_wallets": {"arbitrum": "0x1234", "base": "0x5678"},
}


class TestStrategyInitKwargsFiltering:
    """The runner must filter kwargs to match strategy constructor signature."""

    def test_full_strategy_gets_all_kwargs(self):
        filtered = _filter_init_kwargs(_FullStrategy, ALL_KWARGS)
        assert filtered == ALL_KWARGS
        # Should construct without error
        instance = _FullStrategy(**filtered)
        assert instance.chains == ["arbitrum", "base"]

    def test_minimal_strategy_chains_stripped(self):
        """Strategies without chains/chain_wallets must not receive them."""
        filtered = _filter_init_kwargs(_MinimalStrategy, ALL_KWARGS)
        assert "chains" not in filtered
        assert "chain_wallets" not in filtered
        assert filtered == {"config": {"some": "config"}, "chain": "arbitrum", "wallet_address": "0x1234"}
        # Should construct without error
        instance = _MinimalStrategy(**filtered)
        assert instance.chain == "arbitrum"

    def test_kwargs_strategy_gets_everything(self):
        """Strategy with **kwargs should receive all kwargs (no filtering)."""
        filtered = _filter_init_kwargs(_KwargsStrategy, ALL_KWARGS)
        assert filtered == ALL_KWARGS
        instance = _KwargsStrategy(**filtered)
        assert instance.extra["chains"] == ["arbitrum", "base"]

    def test_legacy_dict_strategy(self):
        """Legacy strategy that only accepts config."""
        filtered = _filter_init_kwargs(_LegacyDictStrategy, ALL_KWARGS)
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
