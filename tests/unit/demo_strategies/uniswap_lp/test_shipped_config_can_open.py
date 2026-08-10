"""VIB-5415 — shipped Uniswap LP configs must not inherit a hidden $100 floor.

The strategy's dataclass intentionally retains a conservative fallback for callers
that provide no setting.  Shipped demos are a different contract: their user-visible
config must declare the dust guard explicitly and keep it below their own funding.
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

import almanak

_CONFIG_DIR = Path(almanak.__file__).parent / "demo_strategies" / "uniswap_lp"
_CONFIG_NAMES = ("config.json", "config.optimism.json", "config.robinhood.json")


@pytest.mark.parametrize("config_name", _CONFIG_NAMES)
def test_shipped_config_declares_positive_position_floor(config_name: str) -> None:
    config = json.loads((_CONFIG_DIR / config_name).read_text())

    assert "min_position_usd" in config, (
        f"{config_name} must declare min_position_usd; omission silently inherits "
        "the strategy dataclass's $100 fallback"
    )
    threshold = Decimal(str(config["min_position_usd"]))
    assert threshold > Decimal("0"), f"{config_name} min_position_usd must be positive, got ${threshold}"


@pytest.mark.parametrize("config_name", _CONFIG_NAMES)
def test_shipped_mainnet_funding_clears_floor_with_reopen_headroom(config_name: str) -> None:
    config = json.loads((_CONFIG_DIR / config_name).read_text())
    # ``amount_type: usd`` is the funding schema's denomination contract: the
    # amount is already a dollar notional even when the funded token is volatile.
    # Counting every such entry is deterministic and intentionally more general
    # than hard-coding today's USDC/USDG symbols.
    usd_denominated_funding_floor = sum(
        (
            Decimal(str(entry["amount"]))
            for entry in config.get("token_funding", [])
            if entry.get("amount_type") == "usd"
        ),
        Decimal("0"),
    )
    threshold = Decimal(str(config["min_position_usd"]))

    # The live path deploys 95% of wallet inventory. Ignore token-denominated
    # entries entirely; USD-denominated entries already declare their dollar
    # notional, keeping this a deterministic lower bound rather than a price guess.
    assert threshold < usd_denominated_funding_floor * Decimal("0.95"), (
        f"{config_name} funds at least ${usd_denominated_funding_floor}, but its "
        f"min_position_usd=${threshold} could block a post-close reopen"
    )
