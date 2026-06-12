"""Regression tests for ``_create_backtest_strategy`` config coercion.

The backtest CLI used to pass the raw ``config.json`` dict straight into
``strategy_class(config, chain, wallet)``. Every ``IntentStrategy[ConfigT]``
subclass reads ``self.config.<field>`` in ``__init__``, so construction blew
up with ``AttributeError: 'dict' object has no attribute 'pool'`` -- meaning
``almanak strat backtest pnl`` could never run a real LP strategy
(discovered while validating VIB-5096). The helper now routes the dict
through the runner's shared coercion path
(``almanak/framework/cli/_strategy_config.py``).

These tests construct REAL LP strategies through the helper, not mocks.
"""

from __future__ import annotations

import importlib.util
import json
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.cli.backtest.helpers import (
    _BACKTEST_WALLET,
    _create_backtest_strategy,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_LIFECYCLE_DIR = _REPO_ROOT / "strategies" / "incubating" / "uniswap_v3_lp_lifecycle_arbitrum"


class TestDemoUniswapLPStrategy:
    """Demo LP strategy ships inside the ``almanak`` package -- always present."""

    def test_constructs_with_typed_config(self) -> None:
        import almanak.demo_strategies.uniswap_lp as demo_pkg
        from almanak.demo_strategies.uniswap_lp.strategy import (
            UniswapLPConfig,
            UniswapLPStrategy,
        )

        config = json.loads((Path(demo_pkg.__file__).parent / "config.json").read_text())

        strategy = _create_backtest_strategy(UniswapLPStrategy, config, "arbitrum")

        # The config dict was coerced into the declared dataclass.
        assert isinstance(strategy.config, UniswapLPConfig)
        assert strategy.config.pool == "WETH/USDC/500"
        assert isinstance(strategy.config.range_width_pct, Decimal)
        # __init__ got past `self.config.pool` -- the line that crashed on a raw dict.
        assert strategy.token0_symbol == "WETH"
        assert strategy.token1_symbol == "USDC"
        assert strategy.chain == "arbitrum"
        assert strategy.wallet_address == _BACKTEST_WALLET

    def test_construction_errors_propagate(self) -> None:
        """A broken IntentStrategy config must fail the backtest loudly.

        The old signature-ladder caught TypeError and silently retried with
        fewer arguments, masking real construction failures.
        """
        from almanak.demo_strategies.uniswap_lp.strategy import UniswapLPStrategy

        with pytest.raises(AttributeError, match="split"):
            # `pool` typed as str; an int makes `self.pool.split("/")` raise.
            _create_backtest_strategy(UniswapLPStrategy, {"pool": 12345}, "arbitrum")


class TestIncubatingLPLifecycleStrategy:
    """The exact strategy from the original failure report."""

    @staticmethod
    def _load_module():
        path = _LIFECYCLE_DIR / "strategy.py"
        if not path.exists():
            pytest.skip("strategies/incubating not present in this checkout")
        spec = importlib.util.spec_from_file_location(
            "_uniswap_v3_lp_lifecycle_arbitrum_backtest_regression", path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_constructs_with_typed_config(self) -> None:
        module = self._load_module()
        config = json.loads((_LIFECYCLE_DIR / "config.json").read_text())

        strategy = _create_backtest_strategy(module.UniswapV3LPLifecycleArbitrum, config, "arbitrum")

        assert isinstance(strategy.config, module.LPLifecycleConfig)
        assert strategy.config.pool == "WETH/USDC/3000"
        assert strategy.config.range_width_pct == Decimal("0.10")
        # strategy.py:128 reads `self.config.pool` -- the original AttributeError site.
        assert strategy.token0_symbol == "WETH"
        assert strategy.fee_tier == 3000


class TestLegacySignatureLadder:
    """Non-IntentStrategy classes keep the old fallback behaviour."""

    def test_mock_strategy_config_only_signature(self) -> None:
        from almanak.framework.backtesting import MockBacktestStrategy

        strategy = _create_backtest_strategy(MockBacktestStrategy, {"x": 1}, "arbitrum")

        assert isinstance(strategy, MockBacktestStrategy)
        assert strategy.config == {"x": 1}

    def test_no_arg_fallback(self) -> None:
        class _NoArgs:
            def __init__(self) -> None:
                self.created = True

        strategy = _create_backtest_strategy(_NoArgs, {"ignored": True}, "arbitrum")

        assert strategy.created is True
