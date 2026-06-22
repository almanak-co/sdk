"""Safety-gate regression tests for the gmx_v2_directional_perp golden seed.

Locks in the two money-path properties a directional-perp seed must get right
(both were review findings on the seed's introduction):

1. The open is funded — the wallet balance must cover the ACTUAL required margin
   (notional / leverage), not just a static minimum.
2. Collateral is sized in COLLATERAL-TOKEN units (USD margin / price), so a
   non-stablecoin collateral does not deposit `price`x too many tokens.
"""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_SEED_DIR = (
    Path(__file__).resolve().parents[3]
    / "almanak"
    / "demo_strategies"
    / "gmx_v2_directional_perp"
)


def _load_module():
    spec = importlib.util.spec_from_file_location("gmx_seed", _SEED_DIR / "strategy.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def gmx():
    module = _load_module()
    cls = module.GmxV2DirectionalPerp
    cfg = json.loads((_SEED_DIR / "config.json").read_text(encoding="utf-8"))
    with patch(
        "almanak.framework.strategies.intent_strategy.IntentStrategy.__init__",
        return_value=None,
    ):
        strat = cls.__new__(cls)
        strat._config = cfg
        strat.get_config = lambda k, d=None: cfg.get(k, d)
        cls.__init__(strat)
    strat._position_side = None
    return module, strat


def _market(balance_usd: str, collateral_price: str = "2500"):
    market = MagicMock()
    bal = MagicMock()
    bal.balance_usd = Decimal(balance_usd)
    market.balance.return_value = bal
    market.price.return_value = Decimal(collateral_price)
    return market


class TestBalanceGate:
    """Defaults: position_size_usd=100, leverage=2 -> required margin = $50."""

    def test_holds_when_wallet_below_required_margin(self, gmx):
        module, strat = gmx
        # $25 clears the old $20 min_collateral_usd floor but cannot fund the $50 margin.
        intent = strat._enter(_market("25"), module.LONG, Decimal("0"))
        assert intent.intent_type.value == "HOLD"
        # Pin the exact failure mode: the margin gate, not some unrelated HOLD.
        assert "required margin" in intent.reason

    def test_opens_when_wallet_covers_required_margin(self, gmx):
        module, strat = gmx
        intent = strat._enter(_market("60"), module.LONG, Decimal("0"))
        assert intent.intent_type.value == "PERP_OPEN"


class TestCollateralUnits:
    def test_collateral_is_token_units_not_usd(self, gmx):
        module, strat = gmx
        # $50 margin / $2500 collateral price = 0.02 tokens (NOT 50).
        intent = strat._enter(_market("100", collateral_price="2500"), module.LONG, Decimal("0"))
        assert intent.intent_type.value == "PERP_OPEN"
        assert intent.collateral_amount == Decimal("0.02")
