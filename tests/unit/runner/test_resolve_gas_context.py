"""Tests for ``_resolve_gas_context`` (runner_state).

Covers every branch of the gas-context resolution helper so the CRAP gate
sees real coverage on the function. The helper is intentionally defensive
(five early returns plus a happy path); without this file ``_resolve_gas_context``
ships at ~29% coverage and trips the CRAP gate on any mechanical edit.

The function maps an intent's ``chain`` plus an ``ExecutionResult`` to
``(native_gas_symbol, gas_cost_native_in_eth_units)`` via the
``ChainRegistry``. The happy path returns the chain's native symbol
(e.g. "ETH", "MATIC") and a ``Decimal`` of the gas cost expressed in
the native token. Every defensive branch returns ``(None, None)``.

VIB-4801: coverage uplift to complement the mechanical chain-registry
cutover that replaced ``NATIVE_TOKEN_INFO`` with ``ChainRegistry``.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.runner.runner_state import _resolve_gas_context


def _make_execution_result(*, total_gas_cost_wei: int = 10**15) -> SimpleNamespace:
    """Minimal duck-typed ExecutionResult that exposes ``total_gas_cost_wei``.

    The helper reads only that one attribute, so ``SimpleNamespace`` is
    sufficient and avoids constructing the full dataclass with all its
    required fields and enums.
    """
    return SimpleNamespace(total_gas_cost_wei=total_gas_cost_wei)


def _make_intent(*, chain: str | None = "ethereum") -> SimpleNamespace:
    """Minimal duck-typed intent that exposes ``chain``.

    ``_resolve_gas_context`` only does ``getattr(intent, 'chain', None)`` so
    no real ``Intent`` factory is required here.
    """
    return SimpleNamespace(chain=chain)


class TestResolveGasContextDefensivePaths:
    """Each early-return must yield ``(None, None)`` without raising."""

    def test_execution_result_none_returns_none_none(self):
        intent = _make_intent(chain="ethereum")
        assert _resolve_gas_context(intent, None) == (None, None)

    def test_missing_chain_attr_returns_none_none(self):
        # ``getattr(intent, 'chain', None)`` falls through to None when the
        # attribute is absent.
        intent = SimpleNamespace()  # no .chain attribute at all
        result = _make_execution_result()
        assert _resolve_gas_context(intent, result) == (None, None)

    def test_empty_string_chain_returns_none_none(self):
        intent = _make_intent(chain="")
        result = _make_execution_result()
        assert _resolve_gas_context(intent, result) == (None, None)

    def test_none_chain_returns_none_none(self):
        intent = _make_intent(chain=None)
        result = _make_execution_result()
        assert _resolve_gas_context(intent, result) == (None, None)

    def test_zero_gas_cost_returns_none_none(self):
        intent = _make_intent(chain="ethereum")
        result = _make_execution_result(total_gas_cost_wei=0)
        assert _resolve_gas_context(intent, result) == (None, None)

    def test_negative_gas_cost_returns_none_none(self):
        intent = _make_intent(chain="ethereum")
        result = _make_execution_result(total_gas_cost_wei=-1)
        assert _resolve_gas_context(intent, result) == (None, None)

    def test_unknown_chain_returns_none_none(self):
        # ChainRegistry.try_resolve returns ``None`` for unknown chains.
        intent = _make_intent(chain="not-a-real-chain")
        result = _make_execution_result()
        assert _resolve_gas_context(intent, result) == (None, None)


class TestResolveGasContextHappyPath:
    """Happy-path returns (native_symbol, gas_cost_in_native_units)."""

    def test_ethereum_returns_eth_and_native_amount(self):
        intent = _make_intent(chain="ethereum")
        # 1e15 wei = 0.001 ETH
        result = _make_execution_result(total_gas_cost_wei=10**15)
        symbol, native = _resolve_gas_context(intent, result)
        assert symbol == "ETH"
        assert native == Decimal("0.001")

    def test_polygon_returns_matic(self):
        # Polygon's native symbol per ChainRegistry. Pinned here so the
        # ticker stays observable to the runner; if the registry migrates
        # to "POL", update this test in the same PR.
        intent = _make_intent(chain="polygon")
        result = _make_execution_result(total_gas_cost_wei=2 * 10**18)
        symbol, native = _resolve_gas_context(intent, result)
        assert symbol == "MATIC"
        assert native == Decimal("2")

    def test_arbitrum_returns_eth(self):
        intent = _make_intent(chain="arbitrum")
        result = _make_execution_result(total_gas_cost_wei=10**18)
        symbol, native = _resolve_gas_context(intent, result)
        assert symbol == "ETH"
        assert native == Decimal("1")

    def test_chain_resolution_is_case_insensitive(self):
        # ChainRegistry.try_resolve does ``key.lower().strip()`` itself; this
        # test pins that contract so a future change there can't silently
        # break the gas-context lookup.
        intent = _make_intent(chain="ETHEREUM")
        result = _make_execution_result(total_gas_cost_wei=10**17)
        symbol, native = _resolve_gas_context(intent, result)
        assert symbol == "ETH"
        assert native == Decimal("0.1")

    def test_gas_cost_is_always_18_decimals(self):
        # EVM native gas tokens are always 18 decimals by protocol design.
        # The helper hardcodes ``10**18`` rather than reading
        # ``descriptor.native.decimals`` — that's intentional (see the
        # comment in ``_resolve_gas_context``). Pin the invariant here.
        intent = _make_intent(chain="ethereum")
        # 1 wei -> 1e-18 ETH (smallest non-zero unit).
        result = _make_execution_result(total_gas_cost_wei=1)
        symbol, native = _resolve_gas_context(intent, result)
        assert symbol == "ETH"
        assert native == Decimal(1) / Decimal(10**18)


@pytest.mark.parametrize(
    "chain,expected_symbol",
    [
        ("ethereum", "ETH"),
        ("arbitrum", "ETH"),
        ("optimism", "ETH"),
        ("base", "ETH"),
        ("polygon", "MATIC"),
        ("avalanche", "AVAX"),
        ("bsc", "BNB"),
    ],
)
def test_native_symbol_matches_registry(chain, expected_symbol):
    """Spot-check the native-symbol mapping against a handful of chains.

    If a future ChainRegistry refactor accidentally drops a chain or renames
    its native symbol, this parametrised test fails before the runner does.
    """
    intent = _make_intent(chain=chain)
    result = _make_execution_result(total_gas_cost_wei=10**18)
    symbol, native = _resolve_gas_context(intent, result)
    assert symbol == expected_symbol
    assert native == Decimal("1")
