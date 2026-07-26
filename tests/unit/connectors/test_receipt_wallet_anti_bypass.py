"""Static guard: no receipt parser may derive the trading wallet from the sender.

VIB-6043 was a **pattern**, not a bug in one connector: nine receipt parsers
independently wrote ``receipt["from"]`` (or its ``from_address`` alias) and used
it as "the strategy's wallet" to select ERC-20 ``Transfer`` legs. Under Safe /
Zodiac execution — the only mode the hosted platform runs — that address is the
agent EOA that signs ``execTransactionWithRole``, while the money moves on the
Safe. The result was an accounting blackout on Curve (money on-chain, zero
ledger rows, circuit breaker) and empty-amount success rows on PancakeSwap V3.

A one-time sweep fixes today's nine. This guard is what stops the tenth
connector from reintroducing it — the same shape as
``tests/unit/teardown/test_teardown_accounting_anti_bypass.py``.

**The rule**: inside ``almanak/connectors/**/receipt_parser.py``, reading
``receipt["from"]`` / ``receipt.get("from")`` / ``receipt.get("from_address")``
is forbidden. Ask ``resolve_trading_wallet(receipt)`` instead — it consumes the
effective execution address the framework stamps (the Safe under Safe
execution) and only falls back to the sender when there is nothing better.

Decoded log fields named ``from`` / ``from_address`` (i.e. the participants of a
``Transfer`` event) are a different thing entirely and are untouched by this
guard: it only looks at subscripts/`.get()` calls on a name that refers to the
receipt itself.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
CONNECTORS = REPO_ROOT / "almanak" / "connectors"

#: The resolver itself must read the sender — it is the fallback.
_ALLOWED = {CONNECTORS / "_strategy_base" / "base" / "receipt_wallet.py"}

#: Receipt-ish parameter names. A subscript/`.get()` on one of these for the
#: sender key is what the guard rejects.
_RECEIPT_NAMES = {"receipt", "receipt_dict", "tx_receipt", "raw_receipt"}

_SENDER_KEYS = {"from", "from_address"}


def _sender_reads(tree: ast.AST) -> list[tuple[int, str]]:
    """(lineno, snippet) for every sender read off a receipt-shaped name."""
    hits: list[tuple[int, str]] = []

    for node in ast.walk(tree):
        # receipt["from"] / receipt['from_address']
        if isinstance(node, ast.Subscript):
            value = node.value
            key = node.slice
            if (
                isinstance(value, ast.Name)
                and value.id in _RECEIPT_NAMES
                and isinstance(key, ast.Constant)
                and key.value in _SENDER_KEYS
            ):
                hits.append((node.lineno, f'{value.id}["{key.value}"]'))

        # receipt.get("from") / receipt.get("from_address", ...)
        if isinstance(node, ast.Call):
            func = node.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr == "get"
                and isinstance(func.value, ast.Name)
                and func.value.id in _RECEIPT_NAMES
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value in _SENDER_KEYS
            ):
                hits.append((node.lineno, f'{func.value.id}.get("{node.args[0].value}")'))

    return hits


def test_no_receipt_parser_reads_the_sender_as_the_wallet():
    offenders: list[str] = []

    for path in sorted(CONNECTORS.rglob("receipt_parser.py")):
        if path in _ALLOWED:
            continue
        tree = ast.parse(path.read_text(), filename=str(path))
        for lineno, snippet in _sender_reads(tree):
            offenders.append(f"{path.relative_to(REPO_ROOT)}:{lineno}: {snippet}")

    assert not offenders, (
        "Receipt parsers must not derive the trading wallet from the transaction "
        "sender (VIB-6043) — under Safe/Zodiac execution that is the agent EOA, "
        "not the address the money moves on. Call "
        "`resolve_trading_wallet(receipt)` from "
        "`almanak.connectors._strategy_base.base` instead.\n  " + "\n  ".join(offenders)
    )


def test_the_guard_can_actually_see_a_violation():
    """Anti-vacuity: the matcher must fire on the pre-VIB-6043 shape.

    A guard that silently matches nothing is worse than no guard — it reads as
    coverage. This pins the detector against both spellings.
    """
    source = (
        "def _find(receipt):\n"
        "    wallet = receipt.get('from', '')\n"
        "    other = receipt['from_address']\n"
        "    decoded = log['from_address']\n"  # decoded event field — must NOT match
        "    return wallet, other, decoded\n"
    )
    hits = _sender_reads(ast.parse(source))
    assert len(hits) == 2, hits
    assert {snippet for _, snippet in hits} == {'receipt.get("from")', 'receipt["from_address"]'}


def test_at_least_one_parser_uses_the_shared_resolver():
    """Anti-vacuity: prove the guard is passing because parsers migrated."""
    users = [
        path.relative_to(REPO_ROOT)
        for path in sorted(CONNECTORS.rglob("receipt_parser.py"))
        if "resolve_trading_wallet" in path.read_text()
    ]
    assert len(users) >= 9, f"expected the VIB-6043 sweep's nine call sites, found {users}"
