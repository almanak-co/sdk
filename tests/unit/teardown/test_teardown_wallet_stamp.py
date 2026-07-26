"""The teardown lane must carry the effective execution address (VIB-6043).

Teardown is a parallel intent-dispatch lane. Its ``ExecutionContext`` was built
**without** ``wallet_address`` at all, so the ``ResultEnricher`` had nothing to
stamp onto the closing trades' receipts and every parser fell back to
``receipt["from"]`` — the agent EOA under Safe / Zodiac execution, not the Safe
the money moves on. Closing trades are exactly where an accounting blackout is
most expensive: the position is being unwound and the books go quiet.

Two properties are pinned here:

1. the context carries a wallet at all, and
2. it is the wallet for **that leg's chain** — multi-chain teardown is
   supported and per-chain wallets can legitimately differ, so stamping the
   primary-chain wallet on a non-primary leg would be *worse* than not stamping
   (the stamp outranks ``receipt["from"]``).
"""

from __future__ import annotations

import ast
from pathlib import Path

from almanak.framework.teardown.teardown_manager import _teardown_wallet_for_chain

REPO_ROOT = Path(__file__).resolve().parents[3]
TEARDOWN_MANAGER = REPO_ROOT / "almanak" / "framework" / "teardown" / "teardown_manager.py"

SAFE_PRIMARY = "0x4c373c8D5c486F601874EF02A2Cc19b5F4E9e837"
SAFE_BASE = "0x1234567890AbcdEF1234567890aBcdef12345678"


class _MultiChainStrategy:
    """Strategy exposing a per-chain wallet map, like ``IntentStrategy``."""

    wallet_address = SAFE_PRIMARY

    def __init__(self, chain_wallets: dict[str, str]) -> None:
        self._chain_wallets = chain_wallets

    def get_wallet_for_chain(self, chain: str) -> str:
        return self._chain_wallets.get(chain.lower(), self.wallet_address)


class _SingleChainStrategy:
    wallet_address = SAFE_PRIMARY


class _BrokenStrategy:
    wallet_address = SAFE_PRIMARY

    def get_wallet_for_chain(self, chain: str) -> str:
        raise RuntimeError("boom")


def test_non_primary_leg_gets_its_own_chain_wallet():
    strategy = _MultiChainStrategy({"arbitrum": SAFE_PRIMARY, "base": SAFE_BASE})
    assert _teardown_wallet_for_chain(strategy, "base") == SAFE_BASE
    assert _teardown_wallet_for_chain(strategy, "arbitrum") == SAFE_PRIMARY


def test_unmapped_chain_falls_back_to_the_default_wallet():
    strategy = _MultiChainStrategy({"base": SAFE_BASE})
    assert _teardown_wallet_for_chain(strategy, "polygon") == SAFE_PRIMARY


def test_single_chain_strategy_uses_wallet_address():
    assert _teardown_wallet_for_chain(_SingleChainStrategy(), "arbitrum") == SAFE_PRIMARY


def test_wallet_resolution_never_breaks_teardown():
    """Teardown's first job is removing on-chain risk — never raise here."""
    assert _teardown_wallet_for_chain(_BrokenStrategy(), "base") == SAFE_PRIMARY
    assert _teardown_wallet_for_chain(object(), "base") == ""


def test_teardown_context_is_constructed_with_the_chain_scoped_wallet():
    """Static guard on the integration seam itself.

    The unit tests above pin the resolver; this pins that the teardown
    ``ExecutionContext`` actually *uses* it, and uses the same chain value it
    passes as ``chain=`` — the seam that regressed silently before.
    """
    tree = ast.parse(TEARDOWN_MANAGER.read_text(), filename=str(TEARDOWN_MANAGER))

    contexts = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "ExecutionContext"
    ]
    assert contexts, "no ExecutionContext construction found in teardown_manager"

    for call in contexts:
        kwargs = {kw.arg: kw.value for kw in call.keywords if kw.arg}
        assert "wallet_address" in kwargs, (
            "teardown ExecutionContext must carry wallet_address — without it the "
            "enricher has nothing to stamp and parsers fall back to receipt['from'] "
            "(the agent EOA under Safe execution). VIB-6043."
        )
        wallet_expr = kwargs["wallet_address"]
        assert isinstance(wallet_expr, ast.Call) and isinstance(wallet_expr.func, ast.Name), (
            "expected wallet_address=_teardown_wallet_for_chain(...)"
        )
        assert wallet_expr.func.id == "_teardown_wallet_for_chain", (
            f"teardown must resolve the wallet per chain, got {ast.dump(wallet_expr)}"
        )
        # ...and with the SAME chain expression the context is scoped to.
        chain_expr = kwargs.get("chain")
        assert chain_expr is not None, "ExecutionContext must set chain"
        assert ast.dump(wallet_expr.args[1]) == ast.dump(chain_expr), (
            "wallet_address must be resolved for the same chain the context declares"
        )
