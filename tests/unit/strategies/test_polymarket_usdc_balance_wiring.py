"""Regression guard for VIB-3219.

Polymarket V2 (April 2026 cutover) settles on Polygon in PUSD — the
in-system collateral minted from USDC.e (or native USDC) via the
CollateralOnramp — not native USDC. The protocol-aware balance API
``market.balance("USDC", protocol="polymarket")`` routes to the PUSD variant
via ``PROTOCOL_TOKEN_VARIANTS``. If a polymarket strategy forgets the
``protocol="polymarket"`` kwarg it sees plain USDC and may size incorrectly
(or hold when funds are actually present, or vice-versa).

This test pins the three call sites enumerated in VIB-3219 so a future refactor
can't silently drop the kwarg. The source guard is AST-based (not regex) per
CodeRabbit review of PR #1609 -- regex matching is brittle against formatting
changes and could be fooled by commented-out code.
"""

from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

import pytest

from almanak.framework.strategies.intent_strategy import (
    MarketSnapshot,
    TokenBalance,
)

REPO_ROOT = Path(__file__).resolve().parents[3]

STRATEGY_FILES = [
    REPO_ROOT / "strategies/incubating/edge_polymarket_megaeth_tail/strategy.py",
    REPO_ROOT / "strategies/incubating/polymarket_signal_trader/strategy.py",
    REPO_ROOT / "strategies/incubating/polymarket_arbitrage/strategy.py",
]

# Methods on MarketSnapshot that accept a ``protocol`` kwarg for token-variant
# resolution. Gemini's suggestion on PR #1609: cover ``balance_usd`` too so a
# refactor that swaps ``balance().balance_usd`` for ``balance_usd()`` can't
# silently drop the kwarg.
BALANCE_METHODS: tuple[str, ...] = ("balance", "balance_usd")


def _find_usdc_calls_on_market(tree: ast.AST) -> list[ast.Call]:
    """Return every ``market.<balance-method>("USDC", ...)`` call in the tree.

    Accepts ``market.balance("USDC")``, ``market.balance_usd("USDC")``, and
    their protocol-kwarg variants. Falls back to string form (``ast.Str`` in
    older Pythons) though our target Python is 3.12.
    """
    calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        if func.attr not in BALANCE_METHODS:
            continue
        # Only flag calls whose FIRST positional arg is the literal "USDC".
        # Protocol-agnostic helpers (e.g. ``balance(token_var)``) are out of
        # scope -- we only need to guard hardcoded "USDC".
        if not node.args:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and first.value == "USDC":
            calls.append(node)
    return calls


def _has_polymarket_protocol_kwarg(call: ast.Call) -> bool:
    """Does the call pass ``protocol="polymarket"`` as a keyword argument?"""
    for kw in call.keywords:
        if kw.arg == "protocol" and isinstance(kw.value, ast.Constant) and kw.value.value == "polymarket":
            return True
    return False


@pytest.mark.parametrize("strategy_path", STRATEGY_FILES, ids=lambda p: p.parent.name)
def test_polymarket_strategy_passes_protocol_kwarg(strategy_path: Path) -> None:
    """Every ``market.balance(_usd)?("USDC", ...)`` call must pass protocol="polymarket"."""
    source = strategy_path.read_text()
    tree = ast.parse(source, filename=str(strategy_path))
    calls = _find_usdc_calls_on_market(tree)

    assert calls, (
        f'no market.balance("USDC", ...) / market.balance_usd("USDC", ...) call '
        f"found in {strategy_path}"
    )

    bad = [call for call in calls if not _has_polymarket_protocol_kwarg(call)]
    if bad:
        bad_lines = ", ".join(str(call.lineno) for call in bad)
        pytest.fail(
            f'{strategy_path}: USDC balance calls must pass protocol="polymarket". '
            f"Missing the kwarg at line(s): {bad_lines}"
        )


def test_market_snapshot_returns_pusd_for_polymarket() -> None:
    """Functional check: with USDC=2.00 and PUSD=1.21, polymarket protocol resolves to 1.21."""
    market = MarketSnapshot(chain="polygon", wallet_address="0xtest")
    market._balances["USDC"] = TokenBalance(symbol="USDC", balance=Decimal("2.00"), balance_usd=Decimal("2.00"))
    market._balances["PUSD"] = TokenBalance(symbol="PUSD", balance=Decimal("1.21"), balance_usd=Decimal("1.21"))

    result = market.balance("USDC", protocol="polymarket")
    assert result.symbol == "PUSD"
    assert result.balance == Decimal("1.21")
    assert result.balance_usd == Decimal("1.21")


def test_commented_out_balance_call_is_ignored() -> None:
    """Regex-based detection would flag commented ``market.balance("USDC")`` as a miss.

    AST-based detection ignores comments entirely -- this test pins that behavior
    so a future well-intentioned refactor back to regex is caught.
    """
    sample = (
        "def f(market):\n"
        '    # legacy: usdc = market.balance("USDC")  # pre-VIB-3219\n'
        '    return market.balance("USDC", protocol="polymarket")\n'
    )
    tree = ast.parse(sample)
    calls = _find_usdc_calls_on_market(tree)
    assert len(calls) == 1
    assert _has_polymarket_protocol_kwarg(calls[0]) is True
