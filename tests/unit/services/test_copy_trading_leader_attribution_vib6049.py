"""VIB-6049 link 3 — attribute a copy signal to the leader, not the signer.

**This PR does not make copy-trading work with Safe leaders.** It closes one of
three links, and the other two are pinned as failing-by-design below so this
cannot be mistaken for a complete fix.

The three links, and where they stand
=====================================

1. **Discovery** — ``WalletMonitor`` matches leaders on ``tx.from``. A Safe is
   never a transaction sender, so configuring a Safe as leader yields **zero
   events, ever**. **NOT FIXED HERE.**
2. **Protocol identification** — ``registry.lookup(chain, tx.to)``; for a Safe
   leader ``tx.to`` is the Safe or its Zodiac Roles modifier, so the event is
   dropped as ``unknown_protocol`` before any parser runs. **NOT FIXED HERE.**
3. **Wallet context + attribution** — the receipt reaches parsers unstamped, and
   ``CopySignal.leader_address`` is taken from the transaction sender. **Fixed
   here.**

Why land link 3 first
=====================

A per-parser sweep of the copy-trading registry found that only a **small
minority** of reachable parsers fail closed under Safe execution: ``sushiswap_v3``
and ``pancakeswap_v3`` swap (the two VIB-6043 hardened), plus ``aerodrome``,
which bails to ``("", "", 0, 0)`` when ``resolve_trading_wallet`` yields nothing
(``aerodrome/receipt_parser.py``). **Treat this as "a few, not most" rather than
an exact roster** — it is a hand sweep, it was already off by one (the count read
"two" until the merge audit found ``aerodrome``), and nothing enforces it. Do not
build an argument on the precise membership.

The majority of reachable parsers key off pool/protocol events and extract
**correct amounts** from a Safe-executed receipt while attribution stays
EOA-shaped. That direction is what matters here, and it is the dangerous shape:
numbers that look healthy under the wrong identity.

It is latent only because links 1 and 2 drop the event first. **It goes live the
moment someone fixes discovery without also fixing attribution** —
amount-correct, wrongly-attributed signals with nothing flagged. Landing
attribution first is therefore not a partial fix; it is pre-positioning, so
whoever closes discovery next cannot create that state.

The identity matters beyond decoding: ``get_leader_weight`` / ``get_leader_cap``
(`copy_trading_models.py`) key on the **configured** leader address, so a Safe
leader previously got neither weight nor cap applied.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from decimal import Decimal
from typing import Any

import pytest

from almanak.connectors._strategy_base.base.receipt_wallet import resolve_trading_wallet
from almanak.connectors._strategy_base.contract_registry import ContractRegistry
from almanak.framework.services.copy_trading.copy_signal_engine import CopySignalEngine
from almanak.framework.services.copy_trading.copy_trading_models import LeaderEvent

CHAIN = "arbitrum"

LEADER_SAFE = "0x4c373c8d5c486f601874ef02a2cc19b5f4e9e837"
LEADER_AGENT_EOA = "0x42657d7c6fe1bc3fb0af01a702b88cc31a93661b"
ROLES_MODIFIER = "0x1111111111111111111111111111111111111111"

USDT = "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"
WETH = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
POOL = "0x7f90122bf0700f9e7e1f688fe926940e8839f353"

#: The PancakeSwap V3 swap router as the DEFAULT registry registers it on
#: arbitrum. Deliberately a real registration, not a hand-invented one — and
#: deliberately a parser that fails CLOSED under Safe, so measuring an amount
#: here means something.
PCS_SWAP_ROUTER = "0x32226588378236fd0c7c4053999f88ac0e5cac77"

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
PCS_SWAP_TOPIC = "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"

AMOUNT_IN_RAW = 50_000_000  # 50 USDT
AMOUNT_OUT_RAW = 26_577_044_991_779_670  # 0.02657704499177967 WETH

TX_HASH = "0x" + "ab" * 32
BLOCK_NUM = 100
BLOCK_TIMESTAMP = 1_700_000_000


def _topic_addr(address: str) -> str:
    return "0x" + "0" * 24 + address[2:].lower()


def _transfer_log(token: str, sender: str, recipient: str, amount: int, index: int) -> dict[str, Any]:
    return {
        "address": token,
        "topics": [TRANSFER_TOPIC, _topic_addr(sender), _topic_addr(recipient)],
        "data": f"0x{amount:064x}",
        "logIndex": index,
        "transactionHash": TX_HASH,
    }


def _pcs_swap_log(wallet: str, index: int) -> dict[str, Any]:
    data = f"{(1 << 256) - AMOUNT_IN_RAW:064x}{AMOUNT_OUT_RAW:064x}{0:064x}{0:064x}{0:064x}{0:064x}"
    return {
        "address": POOL,
        "topics": [PCS_SWAP_TOPIC, _topic_addr(wallet), _topic_addr(wallet)],
        "data": f"0x{data}",
        "logIndex": index,
        "transactionHash": TX_HASH,
    }


def _receipt(wallet: str, sender: str) -> dict[str, Any]:
    """A PancakeSwap V3 swap whose money legs move on ``wallet``, sent by ``sender``."""
    return {
        "from": sender,
        "status": 1,
        "transactionHash": TX_HASH,
        "blockNumber": BLOCK_NUM,
        "logs": [
            _transfer_log(USDT, wallet, POOL, AMOUNT_IN_RAW, 0),
            _transfer_log(WETH, POOL, wallet, AMOUNT_OUT_RAW, 1),
            _pcs_swap_log(wallet, 2),
        ],
    }


def _event(*, to_address: str, wallet: str, sender: str, leader: str = "") -> LeaderEvent:
    return LeaderEvent(
        chain=CHAIN,
        block_number=BLOCK_NUM,
        tx_hash=TX_HASH,
        log_index=0,
        timestamp=BLOCK_TIMESTAMP,
        from_address=sender,
        to_address=to_address,
        receipt=_receipt(wallet, sender),
        leader_address=leader,
    )


@pytest.fixture
def registry() -> ContractRegistry:
    """The REAL default registry — asserting the row exists, not inventing one."""
    from almanak.connectors._strategy_base.contract_registry import get_default_registry

    reg = get_default_registry()
    info = reg.lookup(CHAIN, PCS_SWAP_ROUTER)
    assert info is not None and info.protocol == "pancakeswap_v3", (
        "the pancakeswap_v3 swap_router registration this test relies on has moved"
    )
    return reg


@pytest.fixture
def engine(registry: ContractRegistry) -> CopySignalEngine:
    return CopySignalEngine(registry, max_age_seconds=10**9)


# --------------------------------------------------------------------------
# The ticket's stated fix is inert
# --------------------------------------------------------------------------


def test_the_ticket_s_stated_fix_is_a_no_op() -> None:
    """Stamping ``from_address`` cannot change anything — the reason this PR exists.

    VIB-6049 proposes ``stamp_trading_wallet(event.receipt, leader_wallet)``.
    ``WalletMonitor`` sets ``from_address = tx["from"]`` and
    ``resolve_trading_wallet`` already falls back to ``receipt["from"]``, so the
    stamp and the fallback resolve to the same address. Only the MATCHED leader
    moves the answer.
    """
    from almanak.connectors._strategy_base.base import stamp_trading_wallet

    receipt = _receipt(LEADER_SAFE, LEADER_AGENT_EOA)
    assert resolve_trading_wallet(receipt) == LEADER_AGENT_EOA
    assert resolve_trading_wallet(stamp_trading_wallet(receipt, LEADER_AGENT_EOA)) == LEADER_AGENT_EOA
    assert resolve_trading_wallet(stamp_trading_wallet(receipt, LEADER_SAFE)) == LEADER_SAFE


# --------------------------------------------------------------------------
# Link 3 — what this PR fixes
# --------------------------------------------------------------------------


def test_effective_leader_prefers_the_matched_leader_over_the_sender() -> None:
    event = _event(to_address=ROLES_MODIFIER, wallet=LEADER_SAFE, sender=LEADER_AGENT_EOA, leader=LEADER_SAFE)
    assert event.effective_leader == LEADER_SAFE


def test_effective_leader_falls_back_to_the_sender_for_a_plain_eoa_leader() -> None:
    """Unmatched events keep exactly the pre-VIB-6049 behaviour."""
    event = _event(to_address=PCS_SWAP_ROUTER, wallet=LEADER_AGENT_EOA, sender=LEADER_AGENT_EOA)
    assert event.effective_leader == LEADER_AGENT_EOA


def test_engine_stamps_the_matched_leader_without_mutating_the_event(engine: CopySignalEngine) -> None:
    event = _event(to_address=ROLES_MODIFIER, wallet=LEADER_SAFE, sender=LEADER_AGENT_EOA, leader=LEADER_SAFE)
    stamped = engine._with_leader_wallet_stamp(event)
    assert resolve_trading_wallet(stamped.receipt) == LEADER_SAFE
    # Receipts are shared downstream — the original must be untouched.
    assert resolve_trading_wallet(event.receipt) == LEADER_AGENT_EOA


def test_signal_is_attributed_to_the_matched_leader_not_the_signer(engine: CopySignalEngine) -> None:
    """The attribution fix, end to end through a real parser and the real registry.

    A leader whose funds move on one address while another signs: the signal
    must key on the former. ``get_leader_weight`` / ``get_leader_cap`` look up
    the configured address, so the pre-fix value silently disabled both.
    """
    event = _event(
        to_address=PCS_SWAP_ROUTER,
        wallet=LEADER_SAFE,
        sender=LEADER_AGENT_EOA,
        leader=LEADER_SAFE,
    )
    (signal,) = engine.process_events([event], current_time=BLOCK_TIMESTAMP + 1)
    assert signal.leader_address == LEADER_SAFE, "attributed to the signer instead of the leader"
    # MEASURED, and through a parser that fails CLOSED without the stamp — so a
    # measured amount here is proof the stamp did the work.
    assert signal.amounts["USDT"] == Decimal("50")
    assert signal.amounts["WETH"] == Decimal("0.02657704499177967")


def test_without_the_stamp_the_same_receipt_yields_nothing(engine: CopySignalEngine) -> None:
    """The control that makes the test above discriminating."""
    event = _event(
        to_address=PCS_SWAP_ROUTER,
        wallet=LEADER_SAFE,
        sender=LEADER_AGENT_EOA,
        leader=LEADER_SAFE,
    )
    assert engine.process_events([replace(event, leader_address="")], current_time=BLOCK_TIMESTAMP + 1) == []


def test_a_plain_eoa_leader_is_completely_unaffected(engine: CopySignalEngine) -> None:
    event = _event(
        to_address=PCS_SWAP_ROUTER,
        wallet=LEADER_AGENT_EOA,
        sender=LEADER_AGENT_EOA,
        leader=LEADER_AGENT_EOA,
    )
    (signal,) = engine.process_events([event], current_time=BLOCK_TIMESTAMP + 1)
    assert signal.leader_address == LEADER_AGENT_EOA
    assert signal.amounts["USDT"] == Decimal("50")


# --------------------------------------------------------------------------
# Links 1 and 2 — NOT fixed here, pinned so that stays visible
# --------------------------------------------------------------------------


def test_link_2_protocol_identification_is_still_broken(engine: CopySignalEngine) -> None:
    """KNOWN GAP, pinned **on the missing capability**, not on an end state.

    An earlier version of this test asserted ``process_events(...) == []``. That
    is ``[]`` for two different reasons, so it would have stayed green after
    link 2 was fixed — a pin that cannot fire. Caught in review, and it is the
    same defect class this PR keeps surfacing: something that reads as coverage
    and is not.

    The prescribed fix for link 2 is to unwrap the Safe's inner call target from
    the transaction's own calldata (``execTransactionWithRole(to, ...)``). So the
    thing to pin is that **``LeaderEvent`` carries no calldata at all** — an
    unwrap implementation has nothing to read. When someone adds that field,
    this fails and points them here.
    """
    fields = set(LeaderEvent.__dataclass_fields__)
    assert not fields & {"input", "calldata", "tx_input", "data", "input_data", "raw_tx", "tx_data"}, (
        "LeaderEvent gained a calldata field — link 2 is now implementable, so this "
        "pin and the protocol-resolution path must be revisited"
    )
    # And the lookup is still keyed on tx.to, which for a Safe leader is the
    # Safe or its Roles modifier — never a registered contract.
    assert engine._registry.lookup(CHAIN, ROLES_MODIFIER) is None
    event = _event(to_address=ROLES_MODIFIER, wallet=LEADER_SAFE, sender=LEADER_AGENT_EOA, leader=LEADER_SAFE)
    assert engine.process_events([event], current_time=BLOCK_TIMESTAMP + 1) == []


def test_link_1_discovery_is_still_broken() -> None:
    """KNOWN GAP, pinned **behaviourally**. A source grep could not fire.

    An earlier version asserted ``"eth_getLogs" not in getsource(poll)``. That
    clause was dead by construction — every RPC literal in this module lives in
    a ``_get_*`` helper, never in ``poll`` — so the natural fix (a
    ``_poll_leader_logs`` helper called from ``poll``) would have left it green.
    It also fired *falsely* on a pure refactor, which trains the next maintainer
    to delete the assertion. Caught in review.

    This drives the real monitor instead: a Safe-executed leader transaction,
    where the configured leader is the Safe and the sender is the agent EOA.
    A Safe is never ``tx.from``, so discovery yields nothing.
    """
    import json
    from unittest.mock import MagicMock

    from almanak.framework.services.wallet_monitor import WalletMonitor, WalletMonitorConfig

    block = {
        "hash": "0xblockhash",
        "number": hex(BLOCK_NUM),
        "timestamp": hex(BLOCK_TIMESTAMP),
        "transactions": [
            {
                "hash": TX_HASH,
                "from": LEADER_AGENT_EOA,  # the agent signs, never the Safe
                "to": ROLES_MODIFIER,
                "transactionIndex": "0x0",
            }
        ],
    }
    receipt = _receipt(LEADER_SAFE, LEADER_AGENT_EOA)

    #: Every RPC method the monitor actually reached. Asserted in the TEST BODY
    #: below, not inside the mock — see the comment on the ``else`` branch.
    called: set[str] = set()

    def rpc_call(request, timeout=30.0):  # noqa: ARG001
        called.add(request.method)
        resp = MagicMock()
        resp.success, resp.error = True, ""
        if request.method == "eth_blockNumber":
            resp.result = json.dumps(hex(BLOCK_NUM + 1))
        elif request.method == "eth_getBlockByNumber":
            resp.result = json.dumps(block)
        elif request.method == "eth_getTransactionReceipt":
            resp.result = json.dumps(receipt)
        elif request.method == "eth_getLogs":
            # Serve the Safe's REAL transfer legs, not ``[]``. A log-based
            # discovery fix is the most natural way to find Safe activity —
            # precisely because a Safe is never ``tx.from`` — and an empty
            # result would let such a fix produce zero events and leave this
            # pin passing while asserting "link 1 is still broken".
            resp.result = json.dumps(receipt["logs"])
        else:
            # Raising here is a BACKSTOP ONLY — it cannot be relied on, because
            # every ``WalletMonitor._get_*`` helper wraps its gateway call in
            # ``except Exception: ... return None`` and ``AssertionError`` is an
            # ``Exception``. A raise would therefore degrade to exactly the
            # ``None`` that ``success=False`` produced, one layer down, and the
            # pin would go green again. The assertion that actually discriminates
            # is the ``called`` subset check in the test body, where no
            # production ``except`` can swallow it.
            raise AssertionError(f"unmocked RPC {request.method!r}")
        return resp

    gateway = MagicMock()
    gateway.rpc.Call = MagicMock(side_effect=rpc_call)
    monitor = WalletMonitor(
        config=WalletMonitorConfig(leader_addresses=[LEADER_SAFE], chain=CHAIN, lookback_blocks=1),
        gateway_client=gateway,
    )

    events, _ = monitor.poll({"last_processed_block": BLOCK_NUM - 1})

    #: The RPC surface this fixture can faithfully emulate. If the monitor grows
    #: a new discovery *method* on ``rpc.Call`` (``eth_getBlockReceipts``,
    #: ``eth_getTransactionByHash``, ``trace_block``, …), the fixture serves it
    #: nothing and the ``events == []`` below would pass for the WRONG reason —
    #: "the mock starved it", not "a Safe is never tx.from". This check is in the
    #: test body precisely so no production ``except Exception`` can swallow it.
    #:
    #: **Scope limit:** ``called`` only records calls routed through
    #: ``gateway.rpc.Call``. ``gateway`` is a bare ``MagicMock``, so a discovery
    #: fix using a *different transport* (``rpc.BatchCall``, another stub) would
    #: leave ``called`` empty and this subset check would pass vacuously. It
    #: catches a new method, not a new transport. Tightening that needs a
    #: recording stub over every attribute access — tracked on VIB-6049.
    emulated = {"eth_blockNumber", "eth_getBlockByNumber", "eth_getTransactionReceipt", "eth_getLogs"}
    assert called <= emulated, (
        f"WalletMonitor reached new RPC path(s) {sorted(called - emulated)!r}. This pin cannot "
        "judge that implementation — extend the fixture to serve them, then re-check whether "
        "link 1 is genuinely still broken."
    )

    assert events == [], (
        "WalletMonitor discovered a Safe-executed leader — link 1 is fixed. "
        "Update this pin AND confirm attribution is populated, or you have just "
        "enabled amount-correct, wrongly-attributed signals."
    )


def test_the_stamp_equals_the_receipt_sender_for_an_unmatched_event(engine: CopySignalEngine) -> None:
    """The invariant that makes an unmatched event a genuine no-op.

    ``effective_leader`` is falsy only when BOTH ``leader_address`` and
    ``from_address`` are empty, so an unmatched event IS stamped — with the
    sender. That is harmless only because ``WalletMonitor`` sources
    ``from_address`` and the receipt from the same transaction. Asserting it
    here so a discovery fix that breaks the equality cannot do so silently.
    """
    event = _event(to_address=PCS_SWAP_ROUTER, wallet=LEADER_AGENT_EOA, sender=LEADER_AGENT_EOA)
    assert event.leader_address == ""
    stamped = engine._with_leader_wallet_stamp(event)
    assert resolve_trading_wallet(stamped.receipt) == resolve_trading_wallet(event.receipt)


def test_a_non_mapping_receipt_does_not_abort_the_batch(
    engine: CopySignalEngine, caplog: pytest.LogCaptureFixture
) -> None:
    """A degraded provider must not cost every event in the poll.

    ``stamp_trading_wallet`` does ``dict(receipt)`` with no type guard. On main
    ``_process_single_event`` was exception-free, so a malformed receipt was a
    contained skip; raising here would abort the batch AFTER the poll cursor
    advanced, losing every leader event in it permanently.

    This is the "documented log + degraded-but-correct state" branch of the
    silent-error gate, so BOTH halves are asserted here — the state AND the log.
    Asserting only the state would let the degradation go silent without any
    test noticing.

    **The batch must contain a SECOND, VALID event** (CodeRabbit). Passing only
    the malformed one proves nothing about batch survival, which is the entire
    justification for swallowing rather than raising — the test would have
    matched its own name without testing it.
    """
    good = _event(to_address=PCS_SWAP_ROUTER, wallet=LEADER_SAFE, sender=LEADER_AGENT_EOA, leader=LEADER_SAFE)
    broken = replace(
        good,
        receipt="not-a-mapping",  # type: ignore[arg-type]
        tx_hash="0x" + "cd" * 32,  # distinct event_id, or dedup would hide the result
    )

    with caplog.at_level(logging.ERROR, logger="almanak.framework.services.copy_trading.copy_signal_engine"):
        assert engine._with_leader_wallet_stamp(broken) is broken

    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors, "a malformed receipt degraded silently — no ERROR was emitted"
    assert any(broken.event_id in r.getMessage() for r in errors), (
        "the ERROR does not name the event id, so the lost event cannot be traced"
    )

    # The malformed event alone yields nothing...
    assert engine.process_events([broken], current_time=BLOCK_TIMESTAMP + 1) == []

    # ...and, the actual claim: a valid event LATER IN THE SAME BATCH survives it.
    signals = engine.process_events([broken, good], current_time=BLOCK_TIMESTAMP + 1)
    assert len(signals) == 1, (
        f"the malformed event cost the rest of the batch (got {len(signals)} signals) — "
        "every leader event in the poll would be lost permanently, because the cursor "
        "has already advanced"
    )
    assert signals[0].leader_address == LEADER_SAFE


def test_effective_leader_rejects_a_malformed_address(engine: CopySignalEngine) -> None:
    """Attribution and the receipt stamp must never disagree about validity.

    The stamp drops anything that is not a 20-byte hex address. If attribution
    accepted a laxer form, a signal could be attributed to the leader while its
    money legs resolved against the *signer* — amount-correct and
    wrongly-attributed, the exact shape this field exists to prevent.
    """
    event = _event(
        to_address=PCS_SWAP_ROUTER,
        wallet=LEADER_SAFE,
        sender=LEADER_AGENT_EOA,
        leader="0xNOT_AN_ADDRESS",
    )
    # Falls through to the sender rather than carrying an unusable value.
    assert event.effective_leader == LEADER_AGENT_EOA
    stamped = engine._with_leader_wallet_stamp(event)
    assert resolve_trading_wallet(stamped.receipt) == LEADER_AGENT_EOA


#: 42 characters, ``0x``-prefixed, and ``int(x, 16)`` **accepts every one**.
#:
#: Two separate reasons, which is the point: Python treats ``_`` as a digit
#: separator, AND ``int()`` accepts non-ASCII decimal digit families. Pinning
#: only the underscore case would leave the class open — a "fix" that stripped
#: ``_`` and then called ``int()`` again would pass an underscore-only test
#: while still admitting every unicode form below. The guard has to be
#: "payload is ASCII hex", so the regression enumerates the class, not the
#: instance that happened to be reported.
_INT16_ACCEPTED_NON_ADDRESSES = [
    "0x" + "a" * 38 + "_b",  # underscore separator (trailing)
    "0x" + "a" * 19 + "_" + "b" * 20,  # underscore separator (middle)
    "0x" + "٠" * 40,  # arabic-indic digits
    "0x" + "०" * 40,  # devanagari digits
    "0x" + "０" * 40,  # fullwidth digits
]


@pytest.mark.parametrize("bad", _INT16_ACCEPTED_NON_ADDRESSES)
def test_an_int16_parsable_string_is_not_automatically_an_address(bad: str, engine: CopySignalEngine) -> None:
    """Regression: ``int(x, 16)`` cannot validate an address — several ways.

    Caught by CodeRabbit and the Claude auditor on this PR. The original
    ``_normalize_evm_address`` validated with ``len == 42`` + ``int(text, 16)``;
    every parametrised string passes both while being nothing like an address.

    Why it matters rather than being a curiosity: the stamp is **authoritative**
    and outranks ``receipt["from"]``. A malformed leader address accepted as
    usable would be stamped onto the receipt, and every parser would then resolve
    the leader's money legs against an address that can never match a transfer —
    yielding **no signal at all, silently**. That is precisely the silent-drop
    class VIB-6049 is about, reintroduced one level down.
    """
    # Re-run the OLD validator's exact check and assert it ACCEPTS — that is the
    # premise of this regression. Note `assert int(bad, 16)` would be wrong: the
    # unicode cases are zero-digits, so they parse to 0 and a truthiness check
    # would fail for the wrong reason.
    assert len(bad) == 42 and bad.startswith("0x")
    try:
        int(bad, 16)
    except ValueError:  # pragma: no cover - would invalidate the premise
        pytest.fail(f"{bad!r} is not int()-parsable, so it does not demonstrate the old hole")

    event = _event(to_address=PCS_SWAP_ROUTER, wallet=LEADER_SAFE, sender=LEADER_AGENT_EOA, leader=bad)
    assert event.effective_leader == LEADER_AGENT_EOA, "malformed address leaked into attribution"
    stamped = engine._with_leader_wallet_stamp(event)
    assert resolve_trading_wallet(stamped.receipt) == LEADER_AGENT_EOA


def test_attribution_and_the_stamp_share_one_acceptance_test() -> None:
    """The two must agree on EVERY shape, not merely on the ones a test remembers.

    Asserting equality against the shared normalizer — rather than listing
    expected outputs — is what makes this fire if the two ever diverge again.
    They previously did: the duplicated copy rejected ``bytes`` / ``HexBytes``
    that the stamp accepts (found independently by Grok and by the merge audit).

    **This equality is structural, not behavioural** — at HEAD
    ``_normalize_evm_address`` is a one-line delegation, so the assertion is
    ``f(x) == f(x)`` and cannot fail on its own. That is deliberate: it is a
    *drift* pin, firing the moment someone re-inlines the logic. Because an
    equality-only test would stay green if the shared function itself
    regressed, the expected VALUES are asserted below too, so the test carries
    its own contract rather than deferring all of it elsewhere.
    """
    from almanak.connectors._strategy_base.base import normalize_wallet_address
    from almanak.framework.services.copy_trading.copy_trading_models import _normalize_evm_address

    #: (input, expected) — the CONTRACT, so a regression in the shared
    #: function itself is caught here and not only by equality.
    expected: list[tuple[Any, str]] = [
        (LEADER_SAFE, LEADER_SAFE),
        (LEADER_SAFE.upper().replace("0X", "0x"), LEADER_SAFE),  # EIP-55 / uppercase
        (f"  {LEADER_SAFE}  ", LEADER_SAFE),  # surrounding whitespace
        (bytes.fromhex(LEADER_SAFE[2:]), LEADER_SAFE),  # bytes / HexBytes
        (bytearray.fromhex(LEADER_SAFE[2:]), LEADER_SAFE),
        *[(bad, "") for bad in _INT16_ACCEPTED_NON_ADDRESSES],
        ("0x" + "z" * 40, ""),
        ("0x123", ""),
        ("", ""),
        ("   ", ""),
        (None, ""),
        (12345, ""),
    ]
    # Prove the DELEGATION, not merely equal returns for the sampled inputs:
    # two independent implementations could agree on every shape here and still
    # diverge on the next one. Reading the delegated call out of the AST pins
    # the structural contract that makes divergence impossible.
    import ast
    import inspect
    import textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(_normalize_evm_address)))
    # Accept BOTH `normalize_wallet_address(...)` and
    # `mod.normalize_wallet_address(...)`. Pinning only bare Name calls would
    # fire on a harmless refactor to a module-qualified call, and a pin that
    # cries wolf gets deleted rather than heeded (raised by Grok).
    delegated: set[str] = set()
    for n in ast.walk(tree):
        if not isinstance(n, ast.Call):
            continue
        if isinstance(n.func, ast.Name):
            delegated.add(n.func.id)
        elif isinstance(n.func, ast.Attribute):
            delegated.add(n.func.attr)
    assert "normalize_wallet_address" in delegated, (
        "_normalize_evm_address no longer CALLS normalize_wallet_address — the logic was "
        "re-inlined, so attribution and the stamp can drift again (they already did once)"
    )

    for shape, want in expected:
        got = normalize_wallet_address(shape)
        assert got == want, f"shared normalizer regressed on {shape!r}: got {got!r}, want {want!r}"
        assert _normalize_evm_address(shape) == got, (
            f"attribution and the stamp disagree on {shape!r} — a signal could be attributed "
            "to the leader while its money legs resolve against the signer"
        )


def test_a_mixed_case_configured_leader_still_gets_its_weight_and_cap() -> None:
    """Lower-casing attribution must not desync it from an EIP-55 config entry.

    ``CopySignal.leader_address`` is now always lower-cased (it comes from
    ``effective_leader``), whereas before it was the raw ``from_address``. If any
    per-leader lookup were case-sensitive and operators store EIP-55 addresses —
    which is the form every block explorer copies — the leader would silently
    trade with **no weight and no cap**. That is a money-path failure with no log.

    The lookups are case-insensitive today; this pins it through the REAL
    ``CopyTradingConfigV2`` path rather than trusting that, because the coupling
    is invisible from either side alone (raised by Grok on this PR).
    """
    from almanak.framework.services.copy_trading.copy_trading_models import CopyTradingConfigV2

    checksummed = "0x4C373c8D5c486F601874EF02A2cc19B5f4E9e837"  # EIP-55, same address
    assert checksummed != LEADER_SAFE, "fixture must actually differ in case"
    assert checksummed.lower() == LEADER_SAFE

    cfg = CopyTradingConfigV2.from_config(
        {
            "version": 2,
            "leaders": [{"address": checksummed, "weight": "0.25", "max_notional_usd": "750"}],
        }
    )

    event = _event(to_address=PCS_SWAP_ROUTER, wallet=LEADER_SAFE, sender=LEADER_AGENT_EOA, leader=checksummed)
    assert event.effective_leader == LEADER_SAFE, "attribution should be normalised lower-case"

    assert cfg.get_leader_weight(event.effective_leader) == Decimal("0.25"), (
        "a mixed-case configured leader lost its weight — it would trade unweighted"
    )
    assert cfg.get_leader_cap(event.effective_leader) == Decimal("750"), (
        "a mixed-case configured leader lost its cap — it would trade uncapped"
    )
