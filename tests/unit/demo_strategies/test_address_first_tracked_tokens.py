"""Packaged EVM demos must keep wallet/NAV tracking address-native."""

import json
import re
from pathlib import Path
from typing import Any

from almanak.framework.strategies import IntentStrategy
from almanak.framework.teardown import TeardownPositionSummary

_ROOT = Path(__file__).resolve().parents[3]
_DEMOS = (
    "benqi_lending_lifecycle",
    "benqi_looping",
    "euler_v2_supply_ethereum",
    "mantle_mnt_accumulator",
    "morpho_blue_collateral_rotator_ethereum",
    "morpho_looping",
    "pancakeswap_aave_carry_bsc",
    "spark_lender",
    "traderjoe_lp",
    "uniswap_lp",
    "uniswap_rsi",
    "uniswap_v4_hooks",
)

_EXPECTED_ADDRESS_PATHS = {
    "benqi_lending_lifecycle": ("collateral_token_address", "borrow_token_address"),
    "benqi_looping": ("collateral_token_address", "borrow_token_address"),
    "euler_v2_supply_ethereum": ("supply_token_address",),
    "mantle_mnt_accumulator": ("target_token_address", "stable_token_address"),
    "morpho_blue_collateral_rotator_ethereum": ("collateral_token_address",),
    "morpho_looping": ("collateral_token_address", "borrow_token_address"),
    "pancakeswap_aave_carry_bsc": ("collateral_token_address", "borrow_token_address", "token_funding:USDT"),
    "spark_lender": ("supply_token_address",),
    "traderjoe_lp": ("token_x_address", "token_y_address"),
    "uniswap_lp": ("token0_address", "token1_address"),
    "uniswap_rsi": ("base_token_address", "quote_token_address"),
    "uniswap_v4_hooks": ("token0_address", "token1_address"),
}
_EVM_ADDRESS = re.compile(r"0x[0-9a-fA-F]{40}\Z")


def _configured_address(payload: dict[str, Any], path: str) -> str:
    if not path.startswith("token_funding:"):
        return payload[path]
    symbol = path.partition(":")[2]
    return next(entry["address"] for entry in payload["token_funding"] if entry["symbol"] == symbol)


class _Config:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def to_dict(self) -> dict[str, Any]:
        return dict(self._payload)


class _Strategy(IntentStrategy):
    def decide(self, market):
        return None

    def get_open_positions(self):
        return TeardownPositionSummary.empty("test")

    def generate_teardown_intents(self, mode=None, market=None):
        return []


def test_changed_evm_demos_derive_only_configured_addresses() -> None:
    for demo in _DEMOS:
        config_path = _ROOT / "almanak" / "demo_strategies" / demo / "config.json"
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        strategy = object.__new__(_Strategy)
        strategy.config = _Config(payload)

        tracked = strategy._derive_tokens_from_config()
        expected = [_configured_address(payload, path) for path in _EXPECTED_ADDRESS_PATHS[demo]]

        assert tracked, demo
        assert all(_EVM_ADDRESS.fullmatch(token) for token in tracked), (demo, tracked)
        assert {token.lower() for token in tracked} == {token.lower() for token in expected}, (demo, tracked, expected)
