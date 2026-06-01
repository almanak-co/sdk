"""Tests for Uniswap V4 PositionManager / PoolManager custom error selectors (VIB-2703).

Verifies the 7 V4 selectors added to ``KNOWN_CUSTOM_ERRORS`` map to the correct
signature strings, that the source-level dict literal has no duplicate selector
keys (the guard that would catch an accidental double-add of an already-present
selector such as ``0xe450d38c``), that the live decoder resolves a V4 selector,
and that unknown selectors still fall back to a raw-hex string.

Decode entry point under test: ``PublicMempoolSubmitter._decode_revert_data`` —
a synchronous method ``(self, revert_data: str | bytes) -> str`` in
``almanak/framework/execution/submitter/public.py`` (line ~862) that maps a
4-byte selector to ``f"Custom error: {KNOWN_CUSTOM_ERRORS[selector]}"``.
"""

from __future__ import annotations

import ast
import inspect

import pytest

from almanak.framework.execution.submitter import public as public_module
from almanak.framework.execution.submitter.public import (
    KNOWN_CUSTOM_ERRORS,
    PublicMempoolSubmitter,
)

# The 7 Uniswap V4 selectors added under VIB-2703, with their exact signatures.
V4_SELECTORS: dict[str, str] = {
    "0x0ca968d8": "NotApproved(address)",
    "0x1ad777f8": "TickUpperOutOfBounds(int24)",
    "0x24df576f": "TooMuchRequested()",
    "0x486aa307": "PoolNotInitialized()",
    "0xa74f97ab": "NoLiquidityToReceiveFees()",
    "0xbfb22adf": "DeadlinePassed(uint256)",
    "0xd5e2f7ab": "TickLowerOutOfBounds(int24)",
}


@pytest.fixture
def submitter() -> PublicMempoolSubmitter:
    """Construct a submitter for decode-only unit tests."""
    return PublicMempoolSubmitter(rpc_url="http://localhost:8545")


def test_v4_selectors_present_and_mapped() -> None:
    """Each of the 7 V4 selectors must map to its exact signature string."""
    for selector, signature in V4_SELECTORS.items():
        assert selector in KNOWN_CUSTOM_ERRORS, f"missing V4 selector {selector}"
        assert KNOWN_CUSTOM_ERRORS[selector] == signature, (
            f"{selector} maps to {KNOWN_CUSTOM_ERRORS[selector]!r}, expected {signature!r}"
        )


def test_source_level_selector_uniqueness() -> None:
    """No duplicate selector keys in the KNOWN_CUSTOM_ERRORS source literal.

    A dict *literal* silently collapses duplicate string keys, so a runtime
    ``len(dict) == len(set(dict))`` check is tautological and cannot catch an
    accidental double-add (e.g. re-adding the already-present ``0xe450d38c``).
    Instead, parse the module source, locate the ``KNOWN_CUSTOM_ERRORS``
    assignment's ``ast.Dict`` node, collect the string-literal keys in source
    order, and assert there are no duplicates across the whole literal.
    """
    source = inspect.getsource(public_module)
    tree = ast.parse(source)

    dict_node: ast.Dict | None = None
    for node in ast.walk(tree):
        # Match both `KNOWN_CUSTOM_ERRORS = {...}` and the annotated form
        # `KNOWN_CUSTOM_ERRORS: dict[str, str] = {...}`.
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
            value = node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets = [node.target]
            value = node.value
        else:
            continue

        for target in targets:
            if isinstance(target, ast.Name) and target.id == "KNOWN_CUSTOM_ERRORS":
                assert isinstance(value, ast.Dict), (
                    "KNOWN_CUSTOM_ERRORS is not assigned a dict literal"
                )
                dict_node = value
                break
        if dict_node is not None:
            break

    assert dict_node is not None, "could not find KNOWN_CUSTOM_ERRORS dict literal in source"

    keys: list[str] = []
    for key in dict_node.keys:
        assert isinstance(key, ast.Constant) and isinstance(key.value, str), (
            f"non-string-literal key in KNOWN_CUSTOM_ERRORS: {ast.dump(key) if key else key}"
        )
        keys.append(key.value)

    duplicates = sorted({k for k in keys if keys.count(k) > 1})
    assert not duplicates, f"duplicate selector keys in KNOWN_CUSTOM_ERRORS source: {duplicates}"


def test_v4_pool_not_initialized_decodes(submitter: PublicMempoolSubmitter) -> None:
    """The live decoder resolves the V4 PoolNotInitialized() selector."""
    result = submitter._decode_revert_data("0x486aa307")
    assert "PoolNotInitialized" in result
    assert result == "Custom error: PoolNotInitialized()"


def test_unknown_selector_falls_back_to_hex(submitter: PublicMempoolSubmitter) -> None:
    """A selector not in the dict still returns a raw-hex string without raising."""
    result = submitter._decode_revert_data("0xdeadbeef")
    assert "deadbeef" in result.lower()
    assert "Unknown revert" in result
