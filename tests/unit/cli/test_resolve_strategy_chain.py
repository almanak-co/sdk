"""Tests for resolve_strategy_chain() — VIB-3058 regression guard.

Reproduces the failure mode where running `uniswap_lp` on Optimism via the
multi-chain demo smoke harness silently used Arbitrum's chain context (and
therefore Arbitrum's USDC address) because config.json's `chain` field always
won over `ALMANAK_CHAIN` env. The fix in run.py promotes env over config.json
for single-chain runs while validating against the strategy's
supported_chains so a typo can't fall through to a half-broken run.
"""

from types import SimpleNamespace

import click
import pytest

from almanak.framework.cli.run import resolve_strategy_chain
from almanak.framework.data.tokens import get_token_resolver


def _strategy(supported: list[str], default: str | None = None) -> type:
    """Build a fake strategy class carrying STRATEGY_METADATA."""
    metadata = SimpleNamespace(default_chain=default, supported_chains=supported)
    return type("FakeStrategy", (), {"STRATEGY_METADATA": metadata})


class TestResolveStrategyChain:
    def test_env_overrides_config(self):
        """ALMANAK_CHAIN=optimism wins over config.json chain=arbitrum (root cause of VIB-3058)."""
        cls = _strategy(supported=["arbitrum", "optimism", "base"])
        chain = resolve_strategy_chain(
            cls,
            {"chain": "arbitrum"},
            env_chain="optimism",
            multi_chain=False,
        )
        assert chain == "optimism"

    def test_env_unset_uses_config(self):
        """Env unset → config.json chain wins over decorator default."""
        cls = _strategy(supported=["arbitrum", "optimism"], default="arbitrum")
        chain = resolve_strategy_chain(
            cls,
            {"chain": "optimism"},
            env_chain=None,
            multi_chain=False,
        )
        assert chain == "optimism"

    def test_no_env_no_config_uses_decorator_default(self):
        """No env, no config → decorator default_chain wins."""
        cls = _strategy(supported=["arbitrum", "optimism"], default="arbitrum")
        chain = resolve_strategy_chain(cls, {}, env_chain=None, multi_chain=False)
        assert chain == "arbitrum"

    def test_unsupported_env_chain_raises(self):
        """ALMANAK_CHAIN pointing at a chain the strategy doesn't declare must fail loudly,
        not silently fall back to config.json — that hid VIB-3058 for the smoke runs."""
        cls = _strategy(supported=["arbitrum"], default="arbitrum")
        with pytest.raises(click.ClickException, match="not in this strategy's supported_chains"):
            resolve_strategy_chain(
                cls,
                {"chain": "arbitrum"},
                env_chain="optimism",
                multi_chain=False,
            )

    def test_empty_env_string_treated_as_unset(self):
        """ALMANAK_CHAIN='' (empty) is treated as unset, not as a chain literal."""
        cls = _strategy(supported=["arbitrum"])
        chain = resolve_strategy_chain(
            cls,
            {"chain": "arbitrum"},
            env_chain="",
            multi_chain=False,
        )
        assert chain == "arbitrum"

    def test_env_chain_lowercased(self):
        """Caller passes already-lowercased env, but supported_chains check is case-tolerant."""
        cls = _strategy(supported=["Arbitrum", "Optimism"])
        chain = resolve_strategy_chain(
            cls,
            {"chain": "arbitrum"},
            env_chain="optimism",
            multi_chain=False,
        )
        assert chain == "optimism"

    def test_multi_chain_skips_supported_check(self):
        """Multi-chain mode bypasses single-chain resolution entirely;
        the override / validation path must not raise."""
        cls = _strategy(supported=["arbitrum"])
        chain = resolve_strategy_chain(
            cls,
            {"chains": ["arbitrum", "base"]},
            env_chain="optimism",
            multi_chain=True,
        )
        # Returns env (which is fine — caller uses MultiChainRuntimeConfig anyway).
        assert chain == "optimism"

    def test_config_chain_lowercased(self):
        """Config chain with mixed case is normalized to lowercase (the docstring
        promises the returned chain is lowercased; downstream dicts index by lowercase)."""
        cls = _strategy(supported=["optimism", "arbitrum"], default="arbitrum")
        chain = resolve_strategy_chain(
            cls,
            {"chain": "Optimism"},
            env_chain=None,
            multi_chain=False,
        )
        assert chain == "optimism"

    def test_default_chain_lowercased(self):
        """Decorator default falling through to resolve is also normalized to lowercase."""
        cls = _strategy(supported=["Arbitrum"], default="Arbitrum")
        chain = resolve_strategy_chain(cls, {}, env_chain=None, multi_chain=False)
        assert chain == "arbitrum"

    def test_non_string_config_chain_falls_through(self):
        """A malformed config.json with chain=null (or non-string) must not crash —
        it should behave as if chain is unset and fall through to decorator default."""
        cls = _strategy(supported=["arbitrum"], default="arbitrum")
        # JSON null decodes to Python None
        chain = resolve_strategy_chain(cls, {"chain": None}, env_chain=None, multi_chain=False)
        assert chain == "arbitrum"
        # Defensive: even an int (corrupted config) must not raise
        chain = resolve_strategy_chain(cls, {"chain": 42}, env_chain=None, multi_chain=False)
        assert chain == "arbitrum"


class TestUsdcChainAddressDistinction:
    """Defends the static token registry: USDC must resolve to chain-specific
    addresses, not collapse to a default. If anyone refactors the registry such
    that Optimism falls back to Arbitrum's entry (e.g. by sharing a default),
    these asserts fail before a strategy ever queries balances against the
    wrong contract on a fork."""

    # Native USDC addresses, recorded as ground truth from the chain explorers.
    OPTIMISM_USDC = "0x0b2c639c533813f4aa9d7837caf62653d097ff85"
    ARBITRUM_USDC = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"

    def test_optimism_usdc_address(self):
        token = get_token_resolver().resolve("USDC", "optimism", skip_gateway=True)
        assert token.address.lower() == self.OPTIMISM_USDC

    def test_arbitrum_usdc_address(self):
        token = get_token_resolver().resolve("USDC", "arbitrum", skip_gateway=True)
        assert token.address.lower() == self.ARBITRUM_USDC

    def test_chains_distinct(self):
        opt = get_token_resolver().resolve("USDC", "optimism", skip_gateway=True)
        arb = get_token_resolver().resolve("USDC", "arbitrum", skip_gateway=True)
        assert opt.address.lower() != arb.address.lower()
