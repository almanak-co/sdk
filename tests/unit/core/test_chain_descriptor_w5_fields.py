"""W5 (VIB-4857): coverage for new ChainDescriptor fields.

The W5 wave added five Optional fields to the descriptor::

  GasProfile.operation_overrides       (CHAIN_GAS_OVERRIDES chain half)
  GasProfile.fallback_base_fee_gwei    (DEFAULT_GAS_PRICES base)
  GasProfile.fallback_priority_fee_gwei
  Timeouts.receipt_polling             (CHAIN_RECEIPT_TIMEOUTS)
  RpcProfile.block_time_seconds        (gas.py inline block_times)
  Explorer.api_url + api_key_env       (ETHERSCAN_API_URLS / KEY_ENV_VARS)

Each field is :data:`Optional` and the consumer falls back to a
framework-default when the descriptor leaves it ``None``. These tests
lock the asymmetric-coverage shape: chains that had no entry in the
legacy dict keep ``None``; chains that did have an entry get the exact
legacy value back through the registry.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.core.chains import ChainRegistry


# =============================================================================
# Frozen historical snapshots — the legacy values these descriptors mirror.
# Kept inline so a future regression diff against ``main`` is obvious.
# =============================================================================


HISTORICAL_OPERATION_OVERRIDES: dict[str, dict[str, int]] = {
    "ethereum": {
        "swap_simple": 180000,
        "swap_multi_hop": 300000,
    },
    "avalanche": {
        "swap_simple": 180000,
    },
    "bsc": {
        "lp_decrease_liquidity": 400000,
        "lp_collect": 300000,
        "lp_burn": 150000,
    },
    "mantle": {
        "approve": 250_000_000,
        "swap_simple": 500_000_000,
        "swap_multi_hop": 800_000_000,
        "wrap_eth": 200_000_000,
        "unwrap_eth": 200_000_000,
        "lp_mint": 1_000_000_000,
        "lp_increase_liquidity": 400_000_000,
        "lp_decrease_liquidity": 500_000_000,
        "lp_collect": 400_000_000,
        "lp_burn": 200_000_000,
        "lending_supply": 600_000_000,
        "lending_borrow": 900_000_000,
        "vault_deposit": 400_000_000,
    },
}


HISTORICAL_RECEIPT_POLLING: dict[str, int] = {
    "bsc": 300,
    "avalanche": 180,
}


HISTORICAL_FALLBACK_GAS_PRICES: dict[str, dict[str, Decimal]] = {
    "ethereum": {"base_fee": Decimal("20"), "priority_fee": Decimal("2")},
    "arbitrum": {"base_fee": Decimal("0.1"), "priority_fee": Decimal("0")},
    "optimism": {"base_fee": Decimal("0.001"), "priority_fee": Decimal("0.001")},
    "base": {"base_fee": Decimal("0.001"), "priority_fee": Decimal("0.001")},
    "polygon": {"base_fee": Decimal("30"), "priority_fee": Decimal("30")},
    "bsc": {"base_fee": Decimal("3"), "priority_fee": Decimal("0")},
    "avalanche": {"base_fee": Decimal("25"), "priority_fee": Decimal("1")},
}


HISTORICAL_BLOCK_TIMES: dict[str, float] = {
    "ethereum": 12.0,
    "arbitrum": 0.25,
    "optimism": 2.0,
    "base": 2.0,
    "polygon": 2.0,
    "avalanche": 2.0,
}


HISTORICAL_EXPLORER_URLS: dict[str, str] = {
    "ethereum": "https://api.etherscan.io/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "optimism": "https://api-optimistic.etherscan.io/api",
    "base": "https://api.basescan.org/api",
    "polygon": "https://api.polygonscan.com/api",
    "bsc": "https://api.bscscan.com/api",
    "avalanche": "https://api.snowtrace.io/api",
}


HISTORICAL_EXPLORER_KEY_ENVS: dict[str, str] = {
    "ethereum": "ETHERSCAN_API_KEY",
    "arbitrum": "ARBISCAN_API_KEY",
    "optimism": "OPTIMISTIC_ETHERSCAN_API_KEY",
    "base": "BASESCAN_API_KEY",
    "polygon": "POLYGONSCAN_API_KEY",
    "bsc": "BSCSCAN_API_KEY",
    "avalanche": "SNOWTRACE_API_KEY",
}


# =============================================================================
# Populated-field byte-equivalence
# =============================================================================


class TestPopulatedFields:
    """Chains that had an entry in the legacy dict must return the same value."""

    @pytest.mark.parametrize(
        "chain_name,expected",
        sorted(HISTORICAL_OPERATION_OVERRIDES.items()),
    )
    def test_operation_overrides_byte_equivalent(
        self,
        chain_name: str,
        expected: dict[str, int],
    ) -> None:
        d = ChainRegistry.resolve(chain_name)
        assert d.gas.operation_overrides is not None, (
            f"{chain_name} lost its operation_overrides entry"
        )
        assert dict(d.gas.operation_overrides) == expected, (
            f"{chain_name} operation_overrides diverged from the legacy CHAIN_GAS_OVERRIDES value"
        )

    @pytest.mark.parametrize(
        "chain_name,expected",
        sorted(HISTORICAL_RECEIPT_POLLING.items()),
    )
    def test_receipt_polling_byte_equivalent(
        self,
        chain_name: str,
        expected: int,
    ) -> None:
        d = ChainRegistry.resolve(chain_name)
        assert d.timeouts.receipt_polling == expected, (
            f"{chain_name} receipt_polling diverged from the legacy CHAIN_RECEIPT_TIMEOUTS value"
        )

    @pytest.mark.parametrize(
        "chain_name,expected",
        sorted(HISTORICAL_FALLBACK_GAS_PRICES.items()),
    )
    def test_fallback_gas_prices_byte_equivalent(
        self,
        chain_name: str,
        expected: dict[str, Decimal],
    ) -> None:
        d = ChainRegistry.resolve(chain_name)
        assert d.gas.fallback_base_fee_gwei is not None
        assert d.gas.fallback_priority_fee_gwei is not None
        assert Decimal(str(d.gas.fallback_base_fee_gwei)) == expected["base_fee"]
        assert Decimal(str(d.gas.fallback_priority_fee_gwei)) == expected["priority_fee"]

    @pytest.mark.parametrize(
        "chain_name,expected",
        sorted(HISTORICAL_BLOCK_TIMES.items()),
    )
    def test_block_time_byte_equivalent(self, chain_name: str, expected: float) -> None:
        d = ChainRegistry.resolve(chain_name)
        assert d.rpc.block_time_seconds == expected

    @pytest.mark.parametrize(
        "chain_name,expected",
        sorted(HISTORICAL_EXPLORER_URLS.items()),
    )
    def test_explorer_api_url_byte_equivalent(self, chain_name: str, expected: str) -> None:
        d = ChainRegistry.resolve(chain_name)
        assert d.explorer.api_url == expected

    @pytest.mark.parametrize(
        "chain_name,expected",
        sorted(HISTORICAL_EXPLORER_KEY_ENVS.items()),
    )
    def test_explorer_api_key_env_byte_equivalent(self, chain_name: str, expected: str) -> None:
        d = ChainRegistry.resolve(chain_name)
        assert d.explorer.api_key_env == expected


# =============================================================================
# Asymmetric-coverage / fallback semantics
# =============================================================================


class TestNoneFieldFallbackSemantics:
    """Chains with no entry in the legacy dict must propagate ``None`` and let
    the consumer fall back to the framework default — never coerce ``None`` to
    the default at the registry level.
    """

    def test_chains_without_legacy_operation_overrides_have_none(self) -> None:
        for d in ChainRegistry.all():
            if d.name in HISTORICAL_OPERATION_OVERRIDES:
                continue
            assert d.gas.operation_overrides is None, (
                f"{d.name} should have operation_overrides=None (no legacy entry); "
                f"got {d.gas.operation_overrides!r}"
            )

    def test_chains_without_legacy_receipt_polling_have_none(self) -> None:
        for d in ChainRegistry.all():
            if d.name in HISTORICAL_RECEIPT_POLLING:
                continue
            assert d.timeouts.receipt_polling is None, (
                f"{d.name} should have receipt_polling=None (no legacy entry); "
                f"got {d.timeouts.receipt_polling!r}"
            )

    def test_chains_without_legacy_fallback_gas_prices_have_none(self) -> None:
        for d in ChainRegistry.all():
            if d.name in HISTORICAL_FALLBACK_GAS_PRICES:
                continue
            assert d.gas.fallback_base_fee_gwei is None, (
                f"{d.name} fallback_base_fee_gwei should be None"
            )
            assert d.gas.fallback_priority_fee_gwei is None, (
                f"{d.name} fallback_priority_fee_gwei should be None"
            )

    def test_chains_without_legacy_block_time_have_none(self) -> None:
        for d in ChainRegistry.all():
            if d.name in HISTORICAL_BLOCK_TIMES:
                continue
            assert d.rpc.block_time_seconds is None, (
                f"{d.name} block_time_seconds should be None"
            )

    def test_chains_without_legacy_explorer_have_none(self) -> None:
        for d in ChainRegistry.all():
            if d.name in HISTORICAL_EXPLORER_URLS:
                continue
            assert d.explorer.api_url is None, (
                f"{d.name} explorer.api_url should be None"
            )
            assert d.explorer.api_key_env is None, (
                f"{d.name} explorer.api_key_env should be None"
            )


# =============================================================================
# Consumer-level fallback semantics — None propagates through the helper.
# =============================================================================


class TestConsumerFallbackSemantics:
    """The W5 helpers (``receipt_timeout_for``, ``get_gas_estimate``, etc.)
    fall back to a framework default for chains whose descriptor leaves the
    field ``None``.
    """

    def test_receipt_timeout_for_unknown_chain_returns_default(self) -> None:
        from almanak.core.chains._helpers import (
            DEFAULT_RECEIPT_TIMEOUT,
            receipt_timeout_for,
        )

        assert receipt_timeout_for("not-a-real-chain") == DEFAULT_RECEIPT_TIMEOUT

    def test_receipt_timeout_for_chain_without_entry_returns_default(self) -> None:
        from almanak.core.chains._helpers import (
            DEFAULT_RECEIPT_TIMEOUT,
            receipt_timeout_for,
        )

        # ethereum has no entry in HISTORICAL_RECEIPT_POLLING — falls back.
        assert receipt_timeout_for("ethereum") == DEFAULT_RECEIPT_TIMEOUT

    def test_receipt_timeout_for_chain_with_entry_returns_override(self) -> None:
        from almanak.core.chains._helpers import receipt_timeout_for

        assert receipt_timeout_for("bsc") == 300
        assert receipt_timeout_for("avalanche") == 180

    def test_get_gas_estimate_unknown_chain_returns_default(self) -> None:
        from almanak.framework.intents.compiler_constants import (
            DEFAULT_GAS_ESTIMATES,
            get_gas_estimate,
        )

        # Unknown chain → DEFAULT_GAS_ESTIMATES lookup.
        assert get_gas_estimate("not-a-real-chain", "swap_simple") == DEFAULT_GAS_ESTIMATES["swap_simple"]

    def test_get_gas_estimate_chain_without_override_returns_default(self) -> None:
        from almanak.framework.intents.compiler_constants import (
            DEFAULT_GAS_ESTIMATES,
            get_gas_estimate,
        )

        # arbitrum has no operation_overrides — falls back.
        assert get_gas_estimate("arbitrum", "swap_simple") == DEFAULT_GAS_ESTIMATES["swap_simple"]

    def test_get_gas_estimate_chain_with_override_returns_override(self) -> None:
        from almanak.framework.intents.compiler_constants import get_gas_estimate

        # ethereum overrides swap_simple to 180000.
        assert get_gas_estimate("ethereum", "swap_simple") == 180000
        # bsc overrides lp_collect to 300000.
        assert get_gas_estimate("bsc", "lp_collect") == 300000

    def test_get_gas_estimate_alias_resolution(self) -> None:
        from almanak.framework.intents.compiler_constants import get_gas_estimate

        # "bnb" alias resolves to bsc descriptor.
        assert get_gas_estimate("bnb", "lp_collect") == 300000
        # "eth" alias resolves to ethereum.
        assert get_gas_estimate("eth", "swap_simple") == 180000

    def test_block_time_for_unknown_chain_returns_default(self) -> None:
        from almanak.framework.backtesting.pnl.providers.gas import (
            _DEFAULT_BLOCK_TIME_SECONDS,
            _block_time_for,
        )

        assert _block_time_for("not-a-real-chain") == _DEFAULT_BLOCK_TIME_SECONDS

    def test_block_time_for_chain_without_entry_returns_default(self) -> None:
        from almanak.framework.backtesting.pnl.providers.gas import (
            _DEFAULT_BLOCK_TIME_SECONDS,
            _block_time_for,
        )

        # bsc has no entry in HISTORICAL_BLOCK_TIMES -> falls back.
        assert _block_time_for("bsc") == _DEFAULT_BLOCK_TIME_SECONDS

    def test_block_time_for_chain_with_entry_returns_value(self) -> None:
        from almanak.framework.backtesting.pnl.providers.gas import _block_time_for

        assert _block_time_for("ethereum") == 12.0
        assert _block_time_for("arbitrum") == 0.25
        assert _block_time_for("base") == 2.0


# =============================================================================
# Derived-views byte equivalence (gas.py module-level constants).
# =============================================================================


class TestDerivedViews:
    """The legacy module-level dicts in gas.py are now derived views over
    the registry. They must remain byte-equivalent for tests + downstream
    consumers that import them by name.
    """

    def test_etherscan_api_urls_derived_view_matches_history(self) -> None:
        from almanak.framework.backtesting.pnl.providers.gas import ETHERSCAN_API_URLS

        assert dict(ETHERSCAN_API_URLS) == HISTORICAL_EXPLORER_URLS

    def test_etherscan_api_key_env_vars_derived_view_matches_history(self) -> None:
        from almanak.framework.backtesting.pnl.providers.gas import ETHERSCAN_API_KEY_ENV_VARS

        assert dict(ETHERSCAN_API_KEY_ENV_VARS) == HISTORICAL_EXPLORER_KEY_ENVS

    def test_default_gas_prices_derived_view_matches_history(self) -> None:
        from almanak.framework.backtesting.pnl.providers.gas import DEFAULT_GAS_PRICES

        actual = {chain: dict(v) for chain, v in DEFAULT_GAS_PRICES.items()}
        assert actual == HISTORICAL_FALLBACK_GAS_PRICES

    def test_archive_rpc_chains_derived_view_matches_history(self) -> None:
        from almanak.framework.backtesting.pnl.providers.gas import ARCHIVE_RPC_CHAINS

        assert sorted(ARCHIVE_RPC_CHAINS) == sorted(HISTORICAL_BLOCK_TIMES.keys())

    def test_default_archive_rpc_chains_config_view_matches_history(self) -> None:
        from almanak.config.backtest import DEFAULT_ARCHIVE_RPC_CHAINS

        assert sorted(DEFAULT_ARCHIVE_RPC_CHAINS) == sorted(HISTORICAL_BLOCK_TIMES.keys())

    def test_default_gas_api_key_env_vars_config_view_matches_history(self) -> None:
        from almanak.config.backtest import DEFAULT_GAS_API_KEY_ENV_VARS

        assert dict(DEFAULT_GAS_API_KEY_ENV_VARS) == HISTORICAL_EXPLORER_KEY_ENVS

    def test_chain_gas_overrides_back_compat_view_matches_history(self) -> None:
        """W5 preserves ``CHAIN_GAS_OVERRIDES`` as a derived view for SDK back-compat.

        Codex review of PR #2472: existing SDK consumers may import
        ``from almanak.framework.intents.compiler_constants import CHAIN_GAS_OVERRIDES``
        (or via the ``compiler`` re-export). The W5 migration moved the data
        onto ``ChainDescriptor.gas.operation_overrides`` but the module-
        level dict is preserved as a read-only materialised view.
        """
        from almanak.framework.intents.compiler import (
            CHAIN_GAS_OVERRIDES as VIA_COMPILER,
        )
        from almanak.framework.intents.compiler_constants import CHAIN_GAS_OVERRIDES

        # Byte-equivalent to the historical literal we captured.
        assert CHAIN_GAS_OVERRIDES == HISTORICAL_OPERATION_OVERRIDES
        # Re-export through compiler must point at the same data.
        assert VIA_COMPILER == HISTORICAL_OPERATION_OVERRIDES


# =============================================================================
# Immutability — operation_overrides is frozen at construction.
# =============================================================================


class TestOperationOverridesImmutable:
    """Descriptors are frozen dataclasses; the operation_overrides mapping
    is wrapped in MappingProxyType in __post_init__ so callers cannot
    mutate the registered chain config via a leaked reference.
    """

    def test_operation_overrides_is_read_only(self) -> None:
        from types import MappingProxyType

        d = ChainRegistry.resolve("ethereum")
        assert d.gas.operation_overrides is not None
        assert isinstance(d.gas.operation_overrides, MappingProxyType)
        with pytest.raises(TypeError):
            d.gas.operation_overrides["swap_simple"] = 999  # type: ignore[index]

    def test_operation_overrides_freezes_proxy_backed_by_caller_dict(self) -> None:
        """A caller passing a ``MappingProxyType`` over a mutable dict must
        still see the descriptor's view stay constant after mutation of the
        backing dict.

        Before the post-VIB-4857 fix, ``GasProfile.__post_init__`` skipped
        re-wrapping when the argument was already a ``MappingProxyType``.
        Since a proxy mirrors its backing dict, a caller-held reference
        could still mutate the descriptor's view of
        ``operation_overrides`` — contradicting the "truly immutable"
        contract of the ``frozen=True`` dataclass.

        Locks in CodeRabbit's finding on PR #2472.
        """
        from types import MappingProxyType

        from almanak.core.chains._descriptor import GasProfile

        backing: dict[str, int] = {"swap_simple": 180_000}
        proxy = MappingProxyType(backing)

        gp = GasProfile(operation_overrides=proxy)
        assert gp.operation_overrides is not None
        assert gp.operation_overrides["swap_simple"] == 180_000

        # Mutating the caller's backing dict must NOT leak into the
        # descriptor's view.
        backing["swap_simple"] = 999_999
        assert gp.operation_overrides["swap_simple"] == 180_000
