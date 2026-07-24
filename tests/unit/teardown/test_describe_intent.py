"""Unit tests for ``TeardownManager._describe_intent``.

The manager preview renders one step per generated intent. Every teardown
close-intent type should map to readable operator text — the ``Execute
{intent_type}`` fallback keeps unknown types visible (never dropped), but
teardown-recognised types (``completeness.py`` matches VAULT_REDEEM/UNSTAKE
to VAULT/STAKE positions) deserve explicit descriptions, mirroring the
position-type coverage the API preview gained in PR #3406.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from almanak.framework.teardown.teardown_manager import TeardownManager


def _manager() -> TeardownManager:
    return object.__new__(TeardownManager)


@pytest.mark.parametrize(
    ("intent_type", "expected"),
    [
        ("PERP_CLOSE", "Close perpetual position"),
        ("LP_CLOSE", "Close LP position"),
        ("REPAY", "Repay borrowed amount"),
        ("WITHDRAW", "Withdraw collateral"),
        ("VAULT_REDEEM", "Redeem vault shares"),
        ("UNSTAKE", "Unstake staked tokens"),
        ("SWAP", "Swap to target token"),
    ],
)
def test_known_intent_types_have_readable_text(intent_type: str, expected: str) -> None:
    assert _manager()._describe_intent(SimpleNamespace(intent_type=intent_type)) == expected


def test_unknown_intent_type_falls_back_but_stays_visible() -> None:
    assert _manager()._describe_intent(SimpleNamespace(intent_type="FLASH_LOAN")) == "Execute FLASH_LOAN"


def test_object_without_intent_type_falls_back() -> None:
    assert _manager()._describe_intent(object()) == "Execute intent"
