"""VIB-4879: deprecation contract for the legacy global gwei env var.

Pre-VIB-4879, ``ALMANAK_MAX_GAS_PRICE_GWEI`` (and the unprefixed legacy
``MAX_GAS_PRICE_GWEI``) overrode every chain's descriptor default — this
is the bug the user reported on Polygon, where a 100-gwei global env
silently blocked every Polygon intent (live ~280 gwei). Post-VIB-4879,
the global form is **deprecated and ignored** on mainnet. The chain
descriptor default wins; a one-time deprecation WARNING is emitted.

This file is the deprecation contract:
1. The env var is ignored at the ``LocalRuntimeConfig`` layer (the chain
   default carries through to ``config.max_gas_price_gwei``).
2. The env var is ignored at the ``TransactionRiskConfig`` application
   layer (``_apply_runtime_gas_risk_overrides`` does not copy
   ``config.max_gas_price_gwei`` to ``tx_risk_config``).
3. A WARNING is emitted exactly once per (process, chain) pair.
4. The Anvil path is unchanged (developer convenience surface; gas costs
   nothing locally so the env is harmless there).
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

from almanak.config import runtime as _runtime_module
from almanak.config.runtime import _gas_cap_for_chain


@pytest.fixture(autouse=True)
def _isolate_dedupe_state():
    """Snapshot + restore the process-wide warn-once dedupe set per test.

    CodeRabbit (VIB-4879): clearing without restoring left other tests in the
    same pytest-xdist worker reasoning about an empty dedupe set that's
    actually populated in production-like flows. Snapshot up front, clear so
    the test starts clean, restore in ``finally``.
    """
    original = set(_runtime_module._VIB_4879_DEPRECATED_GWEI_ENV_WARNED)
    _runtime_module._VIB_4879_DEPRECATED_GWEI_ENV_WARNED.clear()
    try:
        yield
    finally:
        _runtime_module._VIB_4879_DEPRECATED_GWEI_ENV_WARNED.clear()
        _runtime_module._VIB_4879_DEPRECATED_GWEI_ENV_WARNED.update(original)


class TestMainnetGwei_EnvIgnored:
    """The global gwei env must not influence the resolved cap on mainnet."""

    @pytest.mark.parametrize(
        ("chain", "expected_cap"),
        [
            ("polygon", 1000),  # VIB-4879 bumped from 500
            ("arbitrum", 10),
            ("ethereum", 300),
            ("base", 10),
            ("mantle", 100),  # VIB-4879 bumped from 10
            ("sonic", 200),  # VIB-4879 bumped from 100
        ],
    )
    def test_chain_default_wins_over_global_env_on_mainnet(
        self, chain: str, expected_cap: int, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Per chain, the descriptor cap wins over an absurd global env."""
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI", "999999")

        def get_optional_int(name: str, default: int) -> int:
            # Mimic the runtime config's int-getter shape for legacy callers.
            raw = os.environ.get(name)
            if raw is None:
                return default
            return int(raw)

        cap = _gas_cap_for_chain(chain=chain, network="mainnet", prefix="ALMANAK_", get_optional_int=get_optional_int)
        assert cap == expected_cap, (
            f"Chain {chain!r} resolved to {cap}, expected {expected_cap}. "
            f"The global env (999999) must NOT clobber chain defaults post-VIB-4879."
        )

    def test_legacy_unprefixed_env_is_also_ignored(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Both ``ALMANAK_MAX_GAS_PRICE_GWEI`` and the unprefixed legacy form
        are deprecated; the chain default wins regardless."""
        monkeypatch.setenv("MAX_GAS_PRICE_GWEI", "999999")
        monkeypatch.delenv("ALMANAK_MAX_GAS_PRICE_GWEI", raising=False)

        def get_optional_int(name: str, default: int) -> int:
            raw = os.environ.get(name)
            return int(raw) if raw is not None else default

        cap = _gas_cap_for_chain(
            chain="polygon",
            network="mainnet",
            prefix="ALMANAK_",
            get_optional_int=get_optional_int,
        )
        assert cap == 1000  # polygon descriptor default


class TestDeprecationWarning:
    """The deprecation WARNING must fire once per (process, chain)."""

    def test_warning_fired_when_env_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI", "100")
        mock_logger = MagicMock()
        with patch("almanak.config.runtime.logger", mock_logger):
            _gas_cap_for_chain(
                chain="polygon",
                network="mainnet",
                prefix="ALMANAK_",
                get_optional_int=lambda name, default: int(os.environ.get(name, default)),
            )
        warning_messages = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("deprecated" in m and "MAX_GAS_PRICE_GWEI" in m for m in warning_messages), (
            f"Expected deprecation warning, got: {warning_messages}"
        )

    def test_no_warning_when_env_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("ALMANAK_MAX_GAS_PRICE_GWEI", raising=False)
        monkeypatch.delenv("MAX_GAS_PRICE_GWEI", raising=False)
        mock_logger = MagicMock()
        with patch("almanak.config.runtime.logger", mock_logger):
            _gas_cap_for_chain(
                chain="polygon",
                network="mainnet",
                prefix="ALMANAK_",
                get_optional_int=lambda name, default: int(os.environ.get(name, default)),
            )
        warning_messages = [str(call) for call in mock_logger.warning.call_args_list]
        assert not any("deprecated" in m and "MAX_GAS_PRICE_GWEI" in m for m in warning_messages), (
            f"Expected NO deprecation warning when env is unset, got: {warning_messages}"
        )

    def test_warning_dedupes_per_chain_per_process(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI", "100")
        mock_logger = MagicMock()
        with patch("almanak.config.runtime.logger", mock_logger):
            for _ in range(3):
                _gas_cap_for_chain(
                    chain="polygon",
                    network="mainnet",
                    prefix="ALMANAK_",
                    get_optional_int=lambda name, default: int(os.environ.get(name, default)),
                )
        deprecation_calls = [
            call
            for call in mock_logger.warning.call_args_list
            if "deprecated" in str(call) and "MAX_GAS_PRICE_GWEI" in str(call)
        ]
        assert len(deprecation_calls) == 1, (
            f"Expected exactly 1 deprecation warning across 3 calls for the same "
            f"chain, got {len(deprecation_calls)}: {deprecation_calls}"
        )

    def test_warning_fires_per_chain_in_multi_chain_boot(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Multi-chain boots resolve the cap per chain. Each chain should
        see exactly one warning (so the operator gets one prompt per
        chain to migrate)."""
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI", "100")
        mock_logger = MagicMock()
        with patch("almanak.config.runtime.logger", mock_logger):
            for chain in ("polygon", "arbitrum", "base"):
                _gas_cap_for_chain(
                    chain=chain,
                    network="mainnet",
                    prefix="ALMANAK_",
                    get_optional_int=lambda name, default: int(os.environ.get(name, default)),
                )
        deprecation_calls = [
            call
            for call in mock_logger.warning.call_args_list
            if "deprecated" in str(call) and "MAX_GAS_PRICE_GWEI" in str(call)
        ]
        assert len(deprecation_calls) == 3, (
            f"Expected one warning per chain (3), got {len(deprecation_calls)}: {deprecation_calls}"
        )


class TestAnvilPathUnchanged:
    """The Anvil path is intentionally untouched — gas costs nothing locally
    and the env-read only drives the "you set it too low for Anvil" warning."""

    def test_anvil_returns_anvil_cap_regardless_of_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.framework.execution.gas.constants import ANVIL_GAS_PRICE_CAP_GWEI

        monkeypatch.setenv("MAX_GAS_PRICE_GWEI", "50")  # below ANVIL_GAS_PRICE_CAP_GWEI
        cap = _gas_cap_for_chain(
            chain="polygon",
            network="anvil",
            prefix="ALMANAK_",
            get_optional_int=lambda name, default: int(os.environ.get(name, default)),
        )
        assert cap == ANVIL_GAS_PRICE_CAP_GWEI

    def test_anvil_does_not_emit_deprecation_warning(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The deprecation warning is mainnet-only; Anvil keeps its own
        'too low for Anvil' warning that pre-dates VIB-4879."""
        monkeypatch.setenv("ALMANAK_MAX_GAS_PRICE_GWEI", "50")
        mock_logger = MagicMock()
        with patch("almanak.config.runtime.logger", mock_logger):
            _gas_cap_for_chain(
                chain="polygon",
                network="anvil",
                prefix="ALMANAK_",
                get_optional_int=lambda name, default: int(os.environ.get(name, default)),
            )
        warning_messages = [str(call) for call in mock_logger.warning.call_args_list]
        deprecation_count = sum(1 for m in warning_messages if "deprecated" in m and "MAX_GAS_PRICE_GWEI" in m)
        assert deprecation_count == 0, (
            "Anvil path should not emit the VIB-4879 deprecation warning; it has its "
            f"own 'too low for Anvil' warning. Got messages: {warning_messages}"
        )
