"""Packaged demos must never send display symbols through token price lanes."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import pytest

from almanak.demo_strategies._address_config import require_evm_address

_DEMOS = Path(__file__).resolve().parents[3] / "almanak" / "demo_strategies"
_TOKEN_DATA_ACCESSORS = {
    "price",
    "price_data",
    "reference_price",
    "collateral_value_usd",
    "rsi",
    "sma",
    "ema",
    "macd",
    "atr",
    "adx",
    "obv",
    "cci",
    "ichimoku",
    "stochastic",
    "volatility",
}
_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_TOKEN_KEYWORDS = {"address", "asset", "token", "token_address"}


def _is_literal_symbol(node: ast.expr) -> bool:
    """Return whether a lookup argument is provably a non-address string.

    Variables and computed expressions are valid: callers may resolve an address
    before invoking the market-data API. The AST guard only rejects the unsafe
    case it can establish without data-flow analysis, such as ``price("USDC")``.
    """
    return isinstance(node, ast.Constant) and isinstance(node.value, str) and not _EVM_ADDRESS_RE.fullmatch(node.value)


def _lookup_token_argument(node: ast.Call) -> ast.expr | None:
    """Select the positional or keyword token identity supplied to a lookup."""
    if node.args:
        return node.args[0]
    return next((keyword.value for keyword in node.keywords if keyword.arg in _TOKEN_KEYWORDS), None)


def test_demo_token_data_lookups_do_not_use_literal_symbols() -> None:
    violations: list[str] = []
    for path in sorted(_DEMOS.glob("*/strategy.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr not in _TOKEN_DATA_ACCESSORS:
                continue
            token_argument = _lookup_token_argument(node)
            if token_argument is not None and _is_literal_symbol(token_argument):
                violations.append(f"{path.relative_to(_DEMOS)}:{node.lineno} {node.func.attr}()")

    assert violations == [], "Symbol-based demo token-data lookup(s):\n" + "\n".join(violations)


def test_required_demo_price_addresses_are_declared_in_config() -> None:
    violations: list[str] = []
    for path in sorted(_DEMOS.glob("*/strategy.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        required_keys = {
            node.args[1].value
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "require_evm_address"
            and len(node.args) >= 2
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        }
        if not required_keys:
            continue
        config_path = path.with_name("config.json")
        config = json.loads(config_path.read_text(encoding="utf-8"))
        for key in sorted(required_keys):
            value = config.get(key)
            if not isinstance(value, str) or not _EVM_ADDRESS_RE.fullmatch(value):
                violations.append(f"{config_path.relative_to(_DEMOS)}: invalid or missing {key!r}")

    assert violations == [], "Invalid demo price address config(s):\n" + "\n".join(violations)


class _ConfiguredStrategy:
    def __init__(self, values: dict[str, object]) -> None:
        self.values = values

    def get_config(self, key: str, default: object = None) -> object:
        return self.values.get(key, default)


@pytest.mark.parametrize("value", [None, 123, "", "0x1", "0x" + "z" * 40])
def test_require_evm_address_rejects_missing_non_string_and_malformed_values(value: object) -> None:
    strategy = _ConfiguredStrategy({"base_token_address": value})

    with pytest.raises(ValueError) as exc_info:
        require_evm_address(strategy, "base_token_address")

    message = str(exc_info.value)
    assert "_ConfiguredStrategy" in message
    assert "base_token_address" in message


def test_require_evm_address_returns_a_stripped_valid_address() -> None:
    address = "0x" + "aB" * 20
    strategy = _ConfiguredStrategy({"base_token_address": f"  {address}  "})

    assert require_evm_address(strategy, "base_token_address") == address


@pytest.mark.parametrize(
    ("source", "is_symbol"),
    [
        ('market.price("WETH")', True),
        ('market.price(token="WETH")', True),
        ('market.price(token="0x1111111111111111111111111111111111111111")', False),
        ("market.price(token=price_address)", False),
    ],
)
def test_lookup_token_argument_inspects_positional_and_keyword_tokens(source: str, is_symbol: bool) -> None:
    statement = ast.parse(source).body[0]
    assert isinstance(statement, ast.Expr)
    call = statement.value
    assert isinstance(call, ast.Call)
    argument = _lookup_token_argument(call)
    assert argument is not None
    assert _is_literal_symbol(argument) is is_symbol
