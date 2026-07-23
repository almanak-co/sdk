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
        # Corrected 2026-07-22: the pre-existing ~2000x-inflated values were
        # stale (Mantle's real gas metering is now L1-equivalent scale; the
        # old floor exceeded the real 60M block gas limit by 10x+ and made
        # every Mantle swap unsubmittable). See almanak/core/chains/mantle.py
        # for the live evidence. This fixture is a byte-equivalence pin, not
        # a correctness claim — update it whenever mantle.py's real values
        # intentionally change.
        "approve": 125_000,
        "swap_simple": 250_000,
        "swap_multi_hop": 400_000,
        "wrap_eth": 100_000,
        "unwrap_eth": 100_000,
        "lp_mint": 500_000,
        "lp_increase_liquidity": 200_000,
        "lp_decrease_liquidity": 250_000,
        "lp_collect": 200_000,
        "lp_burn": 100_000,
        "lending_supply": 300_000,
        "lending_borrow": 450_000,
        "vault_deposit": 200_000,
    },
}


HISTORICAL_RECEIPT_POLLING: dict[str, int] = {
    "bsc": 300,
    "avalanche": 180,
}


HISTORICAL_FALLBACK_GAS_PRICES: dict[str, dict[str, Decimal]] = {
    # ethereum's legacy 20/2 mirror is superseded by the measured 2026-07
    # retune in MEASURED_FALLBACK_GAS_PRICES, which overrides it in the union.
    "ethereum": {"base_fee": Decimal("20"), "priority_fee": Decimal("2")},
    "arbitrum": {"base_fee": Decimal("0.1"), "priority_fee": Decimal("0")},
    "optimism": {"base_fee": Decimal("0.001"), "priority_fee": Decimal("0.001")},
    "base": {"base_fee": Decimal("0.001"), "priority_fee": Decimal("0.001")},
    # polygon/bsc/avalanche legacy mirrors are superseded by the measured
    # 2026-07-24 retunes in MEASURED_FALLBACK_GAS_PRICES (union override below).
    "polygon": {"base_fee": Decimal("30"), "priority_fee": Decimal("30")},
    "bsc": {"base_fee": Decimal("3"), "priority_fee": Decimal("0")},
    "avalanche": {"base_fee": Decimal("25"), "priority_fee": Decimal("1")},
}


# Measured values: chains whose fallback fees were measured from the live
# chain instead of inherited from the legacy dicts — either post-legacy
# additions with no legacy counterpart to mirror, or legacy chains whose
# mirror was retired after the value drifted from reality. Kept OUT of
# HISTORICAL_FALLBACK_GAS_PRICES on purpose — that map is a frozen legacy
# snapshot whose value is that a diff against ``main`` stays obvious; entries
# here override it in the union below.
#
# robinhood (VIB-5811): measured 2026-07-14 from baseFeePerGas sampled every
# 1000 blocks over the last 20_000 (min 0.05293 / median 0.05328 / max 0.05427
# gwei); 0.055 rounds the median up, the conservative direction for backtest
# cost estimation. priority_fee is a measured 0.0 (Orbit sequencer is FCFS with
# no priority auction), NOT an unmeasured blank — same shape as arbitrum.
#
# ethereum (2026-07-24): retuned from the legacy 20+2=22 gwei, which was
# calibrated for pre-blob L1 and overstated post-blob mainnet gas ~140x
# (observed ~0.156 gwei total, 2026-07). base_fee matches the
# OBSERVED_TYPICAL_GAS_GWEI snapshot (0.16, 2026-05-27/28 multi-RPC sweep in
# ``framework/execution/gas/constants.py``); priority_fee matches the ~0.05
# gwei landable tip from the VIB-5673 investigation. 0.21 total rounds up
# from observed — the conservative direction for backtest cost estimation.
#
# avalanche (2026-07-24): retuned from the legacy 25+1=26 gwei (the pre-Etna
# 25 gwei minimum-base-fee era), which overstated measured live gas ~470x.
# base_fee 0.06 rounds the 2026-07-24 sweep median up (baseFeePerGas every
# 1000 blocks over the last 20_000: min 0.040 / median 0.055 / max 0.097
# gwei); priority_fee 0.02 is the VIB-5673 live-lane tip floor — the observed
# p50 tip is ~0, but our own transactions always pay at least 0.02.
#
# bsc (2026-07-24): retuned from the legacy 3+0=3 gwei (~60x overstatement).
# baseFeePerGas is a measured 0 on every sampled block (BEP-227 zero base
# fee); the whole price is the 0.05 gwei validator-minimum tip (eth_gasPrice,
# eth_maxPriorityFeePerGas, and p50 feeHistory rewards all agree), so the
# zero moved from priority_fee to base_fee relative to the legacy mirror.
#
# polygon (2026-07-24): retuned from the legacy 30+30=60 gwei, which
# UNDERSTATED PoS-era gas ~5x. base_fee 285 rounds the 2026-05-27/28
# OBSERVED_TYPICAL_GAS_GWEI snapshot (283.95) up — a fresh 2026-07-24 sweep
# measured median 251.1 gwei, same magnitude, keeping the higher evidence as
# the conservative pin; priority_fee 30 is the protocol-enforced validator
# tip floor (mirrors the descriptor's min_priority_fee_gwei).
MEASURED_FALLBACK_GAS_PRICES: dict[str, dict[str, Decimal]] = {
    "robinhood": {"base_fee": Decimal("0.055"), "priority_fee": Decimal("0")},
    "ethereum": {"base_fee": Decimal("0.16"), "priority_fee": Decimal("0.05")},
    "avalanche": {"base_fee": Decimal("0.06"), "priority_fee": Decimal("0.02")},
    "bsc": {"base_fee": Decimal("0"), "priority_fee": Decimal("0.05")},
    "polygon": {"base_fee": Decimal("285"), "priority_fee": Decimal("30")},
}


# Union of both: the full set of chains the registry should expose fallback gas
# prices for. DEFAULT_GAS_PRICES is derived from descriptors, so it sees both.
ALL_FALLBACK_GAS_PRICES: dict[str, dict[str, Decimal]] = {
    **HISTORICAL_FALLBACK_GAS_PRICES,
    **MEASURED_FALLBACK_GAS_PRICES,
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
        assert d.gas.operation_overrides is not None, f"{chain_name} lost its operation_overrides entry"
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
        sorted(ALL_FALLBACK_GAS_PRICES.items()),
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
                f"{d.name} should have operation_overrides=None (no legacy entry); got {d.gas.operation_overrides!r}"
            )

    def test_chains_without_legacy_receipt_polling_have_none(self) -> None:
        for d in ChainRegistry.all():
            if d.name in HISTORICAL_RECEIPT_POLLING:
                continue
            assert d.timeouts.receipt_polling is None, (
                f"{d.name} should have receipt_polling=None (no legacy entry); got {d.timeouts.receipt_polling!r}"
            )

    def test_chains_without_legacy_fallback_gas_prices_have_none(self) -> None:
        for d in ChainRegistry.all():
            if d.name in ALL_FALLBACK_GAS_PRICES:
                continue
            assert d.gas.fallback_base_fee_gwei is None, f"{d.name} fallback_base_fee_gwei should be None"
            assert d.gas.fallback_priority_fee_gwei is None, f"{d.name} fallback_priority_fee_gwei should be None"

    def test_chains_without_legacy_block_time_have_none(self) -> None:
        for d in ChainRegistry.all():
            if d.name in HISTORICAL_BLOCK_TIMES:
                continue
            assert d.rpc.block_time_seconds is None, f"{d.name} block_time_seconds should be None"

    def test_chains_without_legacy_explorer_have_none(self) -> None:
        for d in ChainRegistry.all():
            if d.name in HISTORICAL_EXPLORER_URLS:
                continue
            assert d.explorer.api_url is None, f"{d.name} explorer.api_url should be None"
            assert d.explorer.api_key_env is None, f"{d.name} explorer.api_key_env should be None"


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

    def test_reorg_safe_depth_for_unknown_chain_returns_default(self) -> None:
        from almanak.core.chains._helpers import (
            DEFAULT_REORG_SAFE_DEPTH,
            reorg_safe_depth_for,
        )

        assert reorg_safe_depth_for("not-a-real-chain") == DEFAULT_REORG_SAFE_DEPTH
        assert reorg_safe_depth_for("") == DEFAULT_REORG_SAFE_DEPTH

    def test_reorg_safe_depth_for_chain_without_entry_returns_default(self) -> None:
        from almanak.core.chains._helpers import (
            DEFAULT_REORG_SAFE_DEPTH,
            reorg_safe_depth_for,
        )

        # base / arbitrum declare no reorg_safe_depth → generic-L2 default.
        assert reorg_safe_depth_for("base") == DEFAULT_REORG_SAFE_DEPTH
        assert reorg_safe_depth_for("arbitrum") == DEFAULT_REORG_SAFE_DEPTH

    def test_reorg_safe_depth_for_chain_with_entry_returns_override(self) -> None:
        from almanak.core.chains._helpers import reorg_safe_depth_for

        assert reorg_safe_depth_for("ethereum") == 12
        assert reorg_safe_depth_for("polygon") == 10
        assert reorg_safe_depth_for("avalanche") == 5
        # alias resolves through the registry
        assert reorg_safe_depth_for("avax") == 5

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
        assert actual == ALL_FALLBACK_GAS_PRICES

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


# =============================================================================
# VIB-5811: Robinhood's deliberate W5 shape.
#
# Robinhood is the one chain that carries measured fallback gas fees while
# deliberately leaving ``rpc.block_time_seconds`` and ``explorer.api_url`` at
# ``None``. Both omissions are verified findings, not unfinished work (the
# reasons live in ``almanak/core/chains/robinhood.py``), so "helpfully" filling
# them in is a regression this class exists to catch:
#
# * ``explorer.api_url`` — Blockscout on 4663 implements no ``gastracker``
#   module, the only Etherscan-style query the SDK issues. Setting it buys a
#   different error string and an extra rate-limited round-trip.
# * ``rpc.block_time_seconds`` — the chain's realised block time ranges from
#   ~1310 s/block (block 1) to 0.1002 (since 2026-07-08). The gas provider
#   extrapolates linearly across the whole span, so any value clamps long
#   windows to block 1 and returns wrong-era gas at HIGH confidence.
# =============================================================================


class TestRobinhoodDeliberateOmissions:
    """Lock Robinhood's intended descriptor shape (VIB-5811)."""

    def test_robinhood_has_measured_fallback_gas_fees(self) -> None:
        d = ChainRegistry.resolve("robinhood")
        # Empty≠Zero: priority_fee is a measured 0.0 (FCFS Orbit sequencer, no
        # priority auction), not an unmeasured blank. Consumers gate on
        # ``is None``, so the 0.0 must survive as a real measurement.
        assert d.gas.fallback_base_fee_gwei == 0.055
        assert d.gas.fallback_priority_fee_gwei == 0.0
        assert d.gas.fallback_priority_fee_gwei is not None

    def test_robinhood_gas_default_is_not_the_ethereum_fallback(self) -> None:
        """The bug VIB-5811 fixed: robinhood priced gas at ethereum's 22 gwei."""
        from almanak.framework.backtesting.pnl.config import default_gas_price_gwei_for_chain

        resolved = default_gas_price_gwei_for_chain("robinhood")
        assert resolved == Decimal("0.055")
        assert resolved != default_gas_price_gwei_for_chain("ethereum")

    def test_robinhood_block_time_stays_unset(self) -> None:
        assert ChainRegistry.resolve("robinhood").rpc.block_time_seconds is None

    def test_robinhood_explorer_api_surface_stays_unset(self) -> None:
        explorer = ChainRegistry.resolve("robinhood").explorer
        assert explorer.api_url is None
        assert explorer.api_key_env is None
        # The human-facing browse URL is still declared.
        assert explorer.browse_url == "https://robinhoodchain.blockscout.com"

    def test_robinhood_absent_from_every_archive_membership(self) -> None:
        """Setting either omitted field silently widens these four surfaces."""
        from almanak.config.backtest import DEFAULT_ARCHIVE_RPC_CHAINS
        from almanak.core.chains._helpers import blocks_per_day_map
        from almanak.framework.backtesting.pnl.providers.gas import (
            ARCHIVE_RPC_CHAINS,
            ETHERSCAN_API_URLS,
        )

        assert "robinhood" not in DEFAULT_ARCHIVE_RPC_CHAINS
        assert "robinhood" not in ARCHIVE_RPC_CHAINS
        assert "robinhood" not in ETHERSCAN_API_URLS
        # Backs the replay --chain choices and backtest BLOCKS_PER_DAY.
        assert "robinhood" not in blocks_per_day_map()
