"""Static guard: no accounting producer may equate a tx sender with the wallet.

VIB-6050, the accounting-producer twin of the VIB-6043 receipt-parser guard
(``tests/unit/connectors/test_receipt_wallet_anti_bypass.py``).

Why a *separate* guard rather than widening the VIB-6043 one
============================================================

The receipt-parser guard walks ``almanak/connectors/**/receipt_parser.py`` for
``receipt["from"]`` / ``receipt.get("from")``. Pointing that same guard at the
accounting tree would be wrong in both directions:

* **It would not have caught VIB-6050.** That site never touched a receipt. It
  read a sender from ``eth_getTransactionByHash`` and compared it to the
  execution wallet — ``sender == wallet``. No ``receipt["from"]`` anywhere.
* **It would over-fire.** ``execution/chain_executor.py``,
  ``execution/submitter/public.py`` and ``execution/gateway_orchestrator.py``
  read ``receipt["from"]`` legitimately — as the tx sender, recorded as
  provenance metadata. They are not claiming it is the trading wallet.

The bug class is not "reading the sender". It is **equating the sender with the
wallet**: a predicate that is only true while the wallet is a plain EOA, and is
permanently false once the wallet is a Safe — which is the only hosted mode.
So this guard matches the *comparison*, not the read.

The sanctioned way to ask the question is
``almanak.framework.accounting.capital_flows.wallet_initiated``, which accepts
both the EOA shape (``tx.from == wallet``) and the smart-account shape
(``tx.to == wallet``). It is the single allowlisted site below.
"""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

#: Accounting producers — every tree that writes a money-bearing surface and
#: therefore has a wallet-identity opinion.
SCANNED_ROOTS = (
    REPO_ROOT / "almanak" / "framework" / "accounting",
    REPO_ROOT / "almanak" / "framework" / "observability",
    REPO_ROOT / "almanak" / "framework" / "portfolio",
    REPO_ROOT / "almanak" / "framework" / "services" / "copy_trading",
)

#: The one place permitted to make the comparison — it is the abstraction.
#: Allowlisted by MODULE + function, not by bare name: a bare-name allowlist
#: would hand a free pass to any new local helper that happens to be called
#: ``wallet_initiated``.
_ALLOWED_QUALIFIED = {
    ("almanak/framework/accounting/capital_flows.py", "wallet_initiated"),
    # The arm selector the public predicate delegates to — same module, same
    # contract, kept separate only so the evidence-gathering wrapper stays
    # readable. Listed explicitly rather than allowed by proximity.
    ("almanak/framework/accounting/capital_flows.py", "_is_smart_account_arm"),
}

#: Identifiers that denote "who sent / targeted the transaction".
_SENDER_NAMES = frozenset(
    {
        "sender",
        "tx_sender",
        "tx_from",
        "from",
        "from_address",
        "from_addr",
        "txn_sender",
        "originator",
        "origin",
        "signer",
        "submitter",
        "eoa",
        "agent_eoa",
        "caller",
        "msg_sender",
    }
)

#: Identifiers that denote "the wallet whose books we are keeping".
_WALLET_NAMES = frozenset(
    {
        "wallet",
        "wallet_addr",
        "wallet_address",
        "execution_address",
        "trading_wallet",
        "safe",
        "safe_address",
        "leader",
        "leader_address",
    }
)


def _identifier(node: ast.AST) -> str | None:
    """Identifier a node reduces to, across every shape the read can take.

    Beyond ``Name`` and ``Attribute`` this covers the two forms an earlier
    version of this guard was blind to and which are at least as natural as the
    ones it did catch (adversarial review of PR #3471):

    * ``tx["from"]`` — a ``Subscript`` with a constant string key.
    * ``tx.get("from")`` — a ``Call`` on ``.get`` with a constant first arg.
    """
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Constant):
        key = node.slice.value
        return key if isinstance(key, str) else None
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "get"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ):
        return node.args[0].value
    return None


def sender_wallet_equalities(tree: ast.AST, *, rel_path: str = "") -> list[tuple[int, str]]:
    """(lineno, snippet) for every ``sender == wallet``-shaped comparison.

    Skips anything lexically inside the allowlisted (module, function) pair,
    which is where the sanctioned predicate lives.
    """
    allowed_lines: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and (rel_path, node.name) in _ALLOWED_QUALIFIED:
            allowed_lines.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))

    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare) or node.lineno in allowed_lines:
            continue
        if not all(isinstance(op, ast.Eq | ast.NotEq) for op in node.ops):
            continue
        operands = [node.left, *node.comparators]
        identifiers = [_identifier(operand) for operand in operands]
        has_sender = any(name in _SENDER_NAMES for name in identifiers if name)
        has_wallet = any(name in _WALLET_NAMES for name in identifiers if name)
        if has_sender and has_wallet:
            hits.append((node.lineno, " == ".join(name or "<expr>" for name in identifiers)))
    return hits


def test_no_accounting_producer_equates_the_tx_sender_with_the_wallet() -> None:
    offenders: list[str] = []
    for root in SCANNED_ROOTS:
        for path in sorted(root.rglob("*.py")):
            rel = path.relative_to(REPO_ROOT).as_posix()
            tree = ast.parse(path.read_text(), filename=str(path))
            for lineno, snippet in sender_wallet_equalities(tree, rel_path=rel):
                offenders.append(f"{rel}:{lineno}: {snippet}")

    assert not offenders, (
        "An accounting producer equates the transaction sender with the trading "
        "wallet. That predicate is permanently false under Safe / Zodiac "
        "execution — the only hosted mode — because the Safe is never tx.from "
        "(VIB-6043, VIB-6050). Use "
        "`almanak.framework.accounting.capital_flows.wallet_initiated`, which "
        "also accepts the smart-account shape (tx.to == wallet).\n"
        + "\n".join(offenders)
    )


def test_the_guard_can_actually_see_the_pre_fix_source() -> None:
    """Anti-vacuity: the exact VIB-6050 line must be detected."""
    pre_fix = (
        "def _build(sender, wallet, other):\n"
        "    tx_sender_is_wallet = None if sender is None else sender == wallet\n"
        "    unrelated = other == wallet\n"
        "    also_unrelated = sender == counterparty\n"
        "    return tx_sender_is_wallet, unrelated, also_unrelated\n"
    )
    hits = sender_wallet_equalities(ast.parse(pre_fix))
    assert len(hits) == 1, hits
    assert hits[0][1] == "sender == wallet"


def test_the_guard_sees_the_subscript_and_get_shapes_too() -> None:
    """The two shapes an earlier version of this guard was blind to."""
    source = (
        "def _f(tx, wallet):\n"
        "    a = tx['from'] == wallet\n"
        "    b = wallet == tx.get('from_address')\n"
        "    c = tx['value'] == wallet\n"
        "    return a, b, c\n"
    )
    hits = sender_wallet_equalities(ast.parse(source))
    assert len(hits) == 2, hits


def test_the_allowlist_is_module_scoped_not_name_scoped() -> None:
    """A local helper that merely reuses the name gets no free pass."""
    source = "def wallet_initiated(sender, wallet):\n    return sender == wallet\n"
    assert sender_wallet_equalities(ast.parse(source), rel_path="almanak/framework/accounting/other.py")
    assert not sender_wallet_equalities(
        ast.parse(source), rel_path="almanak/framework/accounting/capital_flows.py"
    )


def test_the_guard_allows_the_sanctioned_predicate() -> None:
    """The abstraction itself must not be flagged — else the guard is unusable."""
    sanctioned = (
        "def wallet_initiated(endpoints, wallet):\n"
        "    sender = endpoints.sender\n"
        "    return sender == wallet or endpoints.to == wallet\n"
    )
    assert (
        sender_wallet_equalities(
            ast.parse(sanctioned), rel_path="almanak/framework/accounting/capital_flows.py"
        )
        == []
    )


def test_the_guard_actually_scanned_the_capital_flow_producer() -> None:
    """Anti-vacuity: prove the walk reaches the module the bug lived in."""
    scanned = [path for root in SCANNED_ROOTS for path in root.rglob("*.py")]
    assert any(path.name == "capital_flows.py" for path in scanned), scanned
    assert len(scanned) >= 10, f"scan is suspiciously narrow: {scanned}"
