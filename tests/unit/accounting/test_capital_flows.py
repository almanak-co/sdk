"""Unit tests for the capital-flow transfer provenance reader (VIB-5866 leg B).

All RPC is served by an in-process fake provider — no sockets, no gateway.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.accounting.capital_flows import (
    CHUNK_BLOCKS,
    MAX_BACKLOG_BLOCKS,
    MAX_BLOCKS_PER_CYCLE,
    MIN_CHUNK_BLOCKS,
    SAFE_EXEC_TRANSACTION_SELECTOR,
    SAFE_EXECUTION_SUCCESS_TOPIC,
    TRANSFER_SIG,
    ZERO_ADDRESS,
    ChainScanResult,
    CounterpartyKind,
    FlowClassification,
    ScanStatus,
    TokenInfo,
    TransferDirection,
    TxEndpoints,
    _normalize_address_value,
    clear_provenance_caches,
    pad_address_topic,
    scan_chain_transfers,
    wallet_initiated,
)

WALLET = "0x1111111111111111111111111111111111111111"
EOA = "0x2222222222222222222222222222222222222222"
CONTRACT = "0x3333333333333333333333333333333333333333"
SPENDER = "0x4444444444444444444444444444444444444444"
USDC = "0x" + "aa" * 20
WETH = "0x" + "bb" * 20

UNIVERSE = {
    USDC: TokenInfo(symbol="USDC", decimals=6),
    WETH: TokenInfo(symbol="WETH", decimals=18),
}


def test_accounting_address_coercion_delegates_chain_sensitive_casing_to_core() -> None:
    solana_address = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    assert _normalize_address_value("0x" + "Aa" * 20, "arbitrum") == "0x" + "aa" * 20
    assert _normalize_address_value(solana_address, "solana") == solana_address


def _log(
    *,
    token: str = USDC,
    sender: str,
    recipient: str,
    amount: int = 1_000_000,
    tx_hash: str = "0xdead01",
    block: int = 100,
    log_index: int = 0,
    extra_topic: str | None = None,
) -> dict:
    topics = [TRANSFER_SIG, pad_address_topic(sender, "arbitrum"), pad_address_topic(recipient, "arbitrum")]
    if extra_topic is not None:
        topics.append(extra_topic)
    return {
        "address": token,
        "topics": topics,
        "data": hex(amount),
        "transactionHash": tx_hash,
        "blockNumber": block,
        "logIndex": log_index,
    }


class FakeEth:
    """Minimal eth namespace honouring the filter shape the reader emits."""

    def __init__(
        self,
        logs: list[dict],
        code: dict[str, str] | None = None,
        txs: dict[str, dict] | None = None,
        *,
        max_span: int | None = None,
        fail_get_code: bool = False,
        fail_get_tx: bool = False,
        receipts: dict[str, dict] | None = None,
        fail_get_receipt: bool = False,
    ) -> None:
        self.logs = logs
        self.code = {k.lower(): v for k, v in (code or {}).items()}
        self.txs = {k.lower(): v for k, v in (txs or {}).items()}
        self.receipts = {k.lower(): v for k, v in (receipts or {}).items()}
        self.fail_get_receipt = fail_get_receipt
        self.receipt_calls: list[str] = []
        self.max_span = max_span
        self.fail_get_code = fail_get_code
        self.fail_get_tx = fail_get_tx
        self.log_calls: list[tuple[int, int]] = []
        self.code_calls: list[str] = []
        self.tx_calls: list[str] = []

    def get_logs(self, params: dict) -> list[dict]:
        span = params["toBlock"] - params["fromBlock"] + 1
        self.log_calls.append((params["fromBlock"], params["toBlock"]))
        if self.max_span is not None and span > self.max_span:
            raise ValueError(f"query returned more than 10000 results / range {span} too wide")

        addresses = {a.lower() for a in params["address"]}
        sig, topic_from, topic_to = params["topics"]
        out = []
        for log in self.logs:
            if log["address"].lower() not in addresses:
                continue
            if not (params["fromBlock"] <= log["blockNumber"] <= params["toBlock"]):
                continue
            if log["topics"][0].lower() != sig.lower():
                continue
            if topic_from is not None and log["topics"][1].lower() != topic_from.lower():
                continue
            if topic_to is not None and log["topics"][2].lower() != topic_to.lower():
                continue
            out.append(log)
        return out

    def get_code(self, address: str) -> str:
        self.code_calls.append(address.lower())
        if self.fail_get_code:
            raise ConnectionError("provider down")
        return self.code.get(address.lower(), "0x")

    def get_transaction(self, tx_hash: str) -> dict:
        self.tx_calls.append(tx_hash.lower())
        if self.fail_get_tx:
            raise ConnectionError("provider down")
        if tx_hash.lower() not in self.txs:
            raise ValueError("not found")
        return self.txs[tx_hash.lower()]

    def get_transaction_receipt(self, tx_hash: str) -> dict:
        self.receipt_calls.append(tx_hash.lower())
        if self.fail_get_receipt:
            raise ConnectionError("provider down")
        if tx_hash.lower() not in self.receipts:
            raise ValueError("not found")
        return self.receipts[tx_hash.lower()]


class FakeWeb3:
    def __init__(self, eth: FakeEth) -> None:
        self.eth = eth


@pytest.fixture(autouse=True)
def _clear_caches():
    clear_provenance_caches()
    yield
    clear_provenance_caches()


def _scan(
    logs: list[dict],
    *,
    code: dict[str, str] | None = None,
    txs: dict[str, dict] | None = None,
    ledger: tuple[str, ...] = (),
    universe: dict | None = None,
    from_block: int = 0,
    head: int = 1_000,
    max_span: int | None = None,
    fail_get_code: bool = False,
    fail_get_tx: bool = False,
    receipts: dict[str, dict] | None = None,
    fail_get_receipt: bool = False,
) -> ChainScanResult:
    eth = FakeEth(
        logs,
        code,
        txs,
        max_span=max_span,
        fail_get_code=fail_get_code,
        fail_get_tx=fail_get_tx,
        receipts=receipts,
        fail_get_receipt=fail_get_receipt,
    )
    return scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=from_block,
        head_block=head,
        token_universe=universe if universe is not None else UNIVERSE,
        ledger_tx_hashes=ledger,
    )


# --------------------------------------------------------------------------
# Classification
# --------------------------------------------------------------------------


def test_ledger_tx_is_strategy_tx_and_skips_code_lookup():
    eth = FakeEth(
        [_log(sender=CONTRACT, recipient=WALLET, tx_hash="0xABC1")],
        {CONTRACT: "0x6080"},
    )
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        token_universe=UNIVERSE,
        ledger_tx_hashes=("0xabc1",),
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.STRATEGY_TX
    assert obs.counterparty_kind is CounterpartyKind.UNKNOWN
    # The ledger short-circuit must actually skip the code probe, not
    # fetch-and-discard it.
    assert eth.code_calls == []


def test_eoa_inflow_is_deposit():
    result = _scan([_log(sender=EOA, recipient=WALLET, amount=5_000_000)])
    (obs,) = result.observations
    assert obs.classification is FlowClassification.DEPOSIT
    assert obs.direction is TransferDirection.IN
    assert obs.counterparty == EOA.lower()
    assert obs.counterparty_kind is CounterpartyKind.EOA
    assert obs.amount == Decimal("5")
    assert obs.symbol == "USDC"
    assert obs.measurable is True


def test_wallet_sent_eoa_outflow_is_withdrawal():
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xfeed")],
        txs={"0xfeed": {"from": WALLET}},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.WITHDRAWAL
    assert obs.direction is TransferDirection.OUT


def test_eip7702_delegated_counterparty_is_eoa_deposit():
    result = _scan(
        [_log(sender=EOA, recipient=WALLET)],
        code={EOA: "0xef0100" + "11" * 20},
    )
    (obs,) = result.observations
    assert obs.counterparty_kind is CounterpartyKind.EOA
    assert obs.classification is FlowClassification.DEPOSIT


def test_contract_inflow_is_unclassified_not_deposit():
    result = _scan(
        [_log(sender=CONTRACT, recipient=WALLET)],
        code={CONTRACT: "0x6080604052"},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_IN
    assert obs.counterparty_kind is CounterpartyKind.CONTRACT


def test_transfer_from_pull_is_unclassified_out_never_withdrawal():
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xpull")],
        txs={"0xpull": {"from": SPENDER}},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


# --------------------------------------------------------------------------
# VIB-6050 — the wallet is a smart account (Safe / Zodiac), never tx.from
# --------------------------------------------------------------------------


#: ``keccak256("ExecutionFromModuleSuccess(address)")`` — what a module entry
#: actually emits, as opposed to ``ExecutionSuccess``.
_MODULE_SUCCESS_TOPIC = "0x6895c13664aa4f67288b25d7a21d7aaa34916e355fb9b6fae0a139a9085becb8"


def _module_success_log() -> dict:
    """The log a genuine ``execTransactionFromModule`` emits."""
    return {"address": WALLET, "topics": [_MODULE_SUCCESS_TOPIC, "0x" + "22" * 32], "data": "0x"}


def _safe_execution_receipt(*, emitter: str = WALLET) -> dict:
    """A receipt carrying ``ExecutionSuccess`` emitted by ``emitter``."""
    return {
        "logs": [
            {
                "address": emitter,
                "topics": [SAFE_EXECUTION_SUCCESS_TOPIC, "0x" + "11" * 32],
                "data": "0x0",
            }
        ]
    }


def test_safe_withdrawal_is_withdrawal_when_the_safe_executed_it():
    """A Safe owner's ``execTransaction`` withdrawal must book as WITHDRAWAL.

    The hosted platform is Safe-only. ``tx.from`` is the signing owner / agent
    EOA and the Safe is ``tx.to``, so the pre-VIB-6050 ``sender == wallet``
    predicate was permanently false and every genuine capital outflow booked as
    UNCLASSIFIED_OUT, poisoning deposit/withdrawal-adjusted PnL.
    """
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xsafe01")],
        txs={"0xsafe01": {"from": EOA, "to": WALLET, "input": SAFE_EXEC_TRANSACTION_SELECTOR + "00" * 32}},
        receipts={"0xsafe01": _safe_execution_receipt()},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.WITHDRAWAL
    assert obs.direction is TransferDirection.OUT


def test_module_backdoor_drain_is_not_a_withdrawal_even_though_the_safe_is_tx_to():
    """THE ATTACK the ``execTransaction`` SELECTOR check exists to stop. Do not simplify.

    Entry into the account is NOT authorisation. An attacker EOA **enabled as a
    Safe module** — the canonical Safe backdoor, installed with one phished
    ``execTransaction`` — calls ``execTransactionFromModule`` **directly on the
    Safe**:

        tx.from  = attacker EOA
        tx.to    = the Safe            <-- satisfies `tx.to == wallet`
        selector = 0x468721a7          <-- execTransactionFromModule
        no owner signature anywhere

    A custom fallback handler, or one that can reach an enabled module, does the
    same. Under the first draft of this fix (bare ``tx.to == wallet``) that drain
    classified as a **WITHDRAWAL** — the theft netted out of PnL and the strategy
    reporting flat instead of a loss. Strictly WORSE than the bug being fixed,
    which merely under-reported withdrawals.

    **What catches it, and what does NOT.** The load-bearing guard is the outer
    selector: ``execTransaction`` (``0x6a761202``) is the only Safe entry point
    that runs ``checkSignatures``, and it lives in the transaction's own signed
    calldata where no executed contract can reach it. This transaction's
    selector is ``0x468721a7``, so it never reaches the event check at all.

    The **second** draft of this fix relied on the Safe's own
    ``ExecutionSuccess`` instead, reasoning that a module path emits
    ``ExecutionFromModuleSuccess``. **That reasoning is superseded and this test
    must not be read as endorsing it**: the same module can call
    ``execTransactionFromModule`` with ``operation = DELEGATECALL``, which runs
    attacker code *in the Safe's context*, so its ``LOG`` opcodes emit with
    ``log.address == safe``. A fork run produced a synthetic ``ExecutionSuccess``
    indistinguishable by emitter from a real one — see
    ``test_delegatecall_forged_execution_success_is_not_a_withdrawal`` and
    ``tests/reports/vib6050-safe-withdrawal-realfork-proof.md``. Emitter-pinning
    does not save an event from delegatecall.

    ``ExecutionSuccess`` is therefore retained only as SECONDARY evidence that
    the execution succeeded (``execTransaction`` emits ``ExecutionFailure``
    instead when the inner call reverts). **Never drop the selector check and
    keep the event check** — that combination is exactly the regression this
    test exists to fail on, and a reader following the superseded rationale
    would make it.

    Raised independently by three adversarial reviews of PR #3471: the
    ``tx.to``-alone break, the DELEGATECALL forge, and the observation that an
    earlier revision of this very test fed the wrong selector and so did not
    perform the attack it is named after.
    """
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xsafe04")],
        # A MODULE entry — this is the attack this test is named after. An
        # earlier revision fed the execTransaction selector here, which made it
        # silently test "execTransaction without ExecutionSuccess" instead: a
        # regression that dropped the SELECTOR check but kept the event check
        # would not have failed it. Caught by adversarial review (Grok), and it
        # is the same class as everything else this fix has turned up — a test
        # that reads as coverage and is not.
        txs={"0xsafe04": {"from": EOA, "to": WALLET, "input": "0x468721a7" + "00" * 32}},
        receipts={"0xsafe04": {"logs": [_module_success_log()]}},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_exec_transaction_without_execution_success_is_not_a_withdrawal():
    """The SECONDARY check, isolated: owner-signed entry that did not succeed.

    ``execTransaction`` emits ``ExecutionFailure`` instead of ``ExecutionSuccess``
    when the inner call reverts. Distinct from the module-entry case above —
    kept separate so each check has a test that fails for its own reason.
    """
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xsafe07")],
        txs={"0xsafe07": {"from": EOA, "to": WALLET, "input": SAFE_EXEC_TRANSACTION_SELECTOR + "00" * 32}},
        receipts={"0xsafe07": {"logs": []}},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_execution_event_emitted_by_someone_else_does_not_corroborate():
    """The emitter must BE the wallet — otherwise topic0 collision IS authentication.

    This is the VIB-6043 lesson applied to the corroboration: any contract can
    emit a log whose ``topic0`` collides with ``ExecutionSuccess``. VIB-6043
    removed a step that *derived* the Safe address from such an event for
    exactly that reason. Here the wallet is already known from configuration
    and is used as the FILTER on the emitter, so a colliding event from an
    unrelated contract matches nothing.

    **The emitter filter is necessary but NOT sufficient, and this test must not
    be read as claiming otherwise.** Delegatecalled code executes in the Safe's
    context, so it can emit an ``ExecutionSuccess`` that passes this very filter
    — measured on a fork, see
    ``test_delegatecall_forged_execution_success_is_not_a_withdrawal``.
    Authorisation rests on the ``execTransaction`` selector in the transaction's
    signed calldata; this filter only stops the cruder, unrelated-contract case.
    """
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xsafe05")],
        txs={"0xsafe05": {"from": EOA, "to": WALLET, "input": SAFE_EXEC_TRANSACTION_SELECTOR + "00" * 32}},
        receipts={"0xsafe05": _safe_execution_receipt(emitter=CONTRACT)},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_unreadable_receipt_leaves_the_smart_account_arm_unknown():
    """Empty != Zero: a failed receipt read is unknown, never a measured "no"."""
    assert (
        wallet_initiated(
            TxEndpoints(sender=EOA, to=WALLET, selector=SAFE_EXEC_TRANSACTION_SELECTOR),
            WALLET,
            chain="arbitrum",
            wallet_executed_as_safe=None,
        )
        is None
    )
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xsafe06")],
        txs={"0xsafe06": {"from": EOA, "to": WALLET, "input": SAFE_EXEC_TRANSACTION_SELECTOR + "00" * 32}},
        fail_get_receipt=True,
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_delegatecall_forged_execution_success_is_not_a_withdrawal():
    """MEASURED BYPASS of the first fix. Do not weaken the selector check.

    The first version of this fix accepted ``tx.to == wallet`` plus an
    ``ExecutionSuccess`` emitted by the wallet's own address. **That event is
    forgeable**, and it was verified on a fork, not argued:

    An attacker EOA enabled as a Safe module calls
    ``execTransactionFromModule(forger, 0, "", DELEGATECALL)``. Delegatecalled
    code executes in the SAFE's context, so its ``LOG`` opcodes emit with
    ``log.address == safe``. The fork run produced exactly this receipt shape —
    a synthetic ``ExecutionSuccess`` indistinguishable, by emitter, from a real
    one:

        tx.from  = attacker EOA
        tx.to    = the Safe
        selector = 0x468721a7   (execTransactionFromModule)
        logs     = [ExecutionSuccess, emitter == the Safe]   <-- FORGED

    Emitter-pinning does not save an event from delegatecall. What does is the
    outer selector: ``execTransaction`` (0x6a761202) is the only entry point
    that runs ``checkSignatures``, and it lives in the transaction's own signed
    calldata where no executed contract can reach it.
    """
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xforge01")],
        txs={"0xforge01": {"from": EOA, "to": WALLET, "input": "0x468721a7" + "00" * 32}},
        receipts={"0xforge01": _safe_execution_receipt()},  # emitter IS the Safe
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_a_module_entry_never_pays_for_a_receipt():
    """The selector rejects module entries before any receipt fetch."""
    eth = FakeEth(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xforge02")],
        txs={"0xforge02": {"from": EOA, "to": WALLET, "input": "0x468721a7"}},
        receipts={"0xforge02": _safe_execution_receipt()},
    )
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        token_universe=UNIVERSE,
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT
    assert eth.receipt_calls == [], "selector must short-circuit before the receipt fetch"


def test_the_exec_transaction_selector_is_derived_not_trusted():
    """Drift guard: re-derive the constant from the canonical signature.

    ``execTransaction``'s parameter list is byte-identical across Safe 1.1.1 /
    1.3.0 / 1.4.1, so one selector covers every version this SDK deploys
    against. If the constant is ever edited by hand, this fails.
    """
    from web3 import Web3

    signature = "execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes)"
    assert SAFE_EXEC_TRANSACTION_SELECTOR == "0x" + Web3.keccak(text=signature).hex()[:8]


def test_nested_safe_and_4337_entries_fail_conservatively():
    """Both fail as false NEGATIVES, which is the safe direction.

    Reasoned + selector-verified rather than fork-measured; both reduce to
    ``endpoints.to != wallet``, which is already covered on-chain.

    * **Nested Safes** — if ``wallet``'s owner is itself a Safe, a withdrawal
      entered through the owner Safe has ``tx.to == owner_safe``. It does not
      classify. The same structure is why nesting cannot manufacture a false
      POSITIVE: reaching the ``tx.to == wallet`` arm still requires entering
      ``wallet`` through its own signature-validating entry point.
    * **ERC-4337** — a UserOp routes through the EntryPoint, so ``tx.to`` is the
      EntryPoint, and ``Safe4337Module.executeUserOp`` (0x7bb37428) is a
      different selector besides.
    """
    owner_safe = "0x5555555555555555555555555555555555555555"
    entry_point = "0x6666666666666666666666666666666666666666"

    nested = TxEndpoints(sender=EOA, to=owner_safe, selector=SAFE_EXEC_TRANSACTION_SELECTOR)
    assert wallet_initiated(nested, WALLET, chain="arbitrum", wallet_executed_as_safe=True) is False

    erc4337 = TxEndpoints(sender=EOA, to=entry_point, selector="0x7bb37428")
    assert wallet_initiated(erc4337, WALLET, chain="arbitrum", wallet_executed_as_safe=True) is False

    # And the 4337 module selector is rejected even if tx.to WERE the wallet.
    assert (
        wallet_initiated(
            TxEndpoints(sender=EOA, to=WALLET, selector="0x7bb37428"),
            WALLET,
            chain="arbitrum",
            wallet_executed_as_safe=True,
        )
        is False
    )


def test_eoa_arm_never_pays_for_a_receipt():
    """The corroboration is lazy — the EOA lane must not gain an RPC."""
    eth = FakeEth(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xeoa01")],
        txs={"0xeoa01": {"from": WALLET, "to": USDC}},
    )
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        token_universe=UNIVERSE,
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.WITHDRAWAL
    assert eth.receipt_calls == []


def test_safe_outflow_pulled_by_a_third_party_is_still_unclassified_out():
    """Widening to ``tx.to`` must not weaken the theft/sweep guard.

    A ``transferFrom`` pull addressed at the token contract (not at the Safe)
    stays conservative — the Safe was not the entry point of the call.
    """
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xsafe02")],
        txs={"0xsafe02": {"from": SPENDER, "to": USDC}},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_zodiac_module_routed_outflow_is_not_a_withdrawal():
    """A tx routed through the Roles modifier is a strategy tx, not a withdrawal.

    Deliberately excluded from :func:`wallet_initiated`: an unledgered
    module-routed outflow is a strategy transaction whose ledger row is missing,
    and booking it as a WITHDRAWAL would net a real loss out of PnL.
    """
    roles_modifier = "0x5555555555555555555555555555555555555555"
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xsafe03")],
        txs={"0xsafe03": {"from": EOA, "to": roles_modifier}},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_wallet_initiated_is_unknown_not_false_when_endpoints_are_unknown():
    """Empty != Zero at the provenance layer: unknown endpoints are ``None``."""
    assert wallet_initiated(None, WALLET, chain="arbitrum") is None
    assert wallet_initiated(TxEndpoints(sender=EOA, to=None), WALLET, chain="arbitrum") is False
    assert wallet_initiated(TxEndpoints(sender=WALLET, to=USDC), WALLET, chain="arbitrum") is True
    # The smart-account arm is corroborated, never assumed.
    signed = TxEndpoints(sender=EOA, to=WALLET, selector=SAFE_EXEC_TRANSACTION_SELECTOR)
    assert wallet_initiated(signed, WALLET, chain="arbitrum", wallet_executed_as_safe=True) is True
    assert wallet_initiated(signed, WALLET, chain="arbitrum", wallet_executed_as_safe=False) is False
    assert wallet_initiated(signed, WALLET, chain="arbitrum", wallet_executed_as_safe=None) is None
    # A module entry never reaches the event check at all.
    module_entry = TxEndpoints(sender=EOA, to=WALLET, selector="0x468721a7")
    assert wallet_initiated(module_entry, WALLET, chain="arbitrum", wallet_executed_as_safe=True) is False
    # A malformed wallet must never make every endpoint match.
    assert wallet_initiated(TxEndpoints(sender=EOA, to=None), "", chain="arbitrum") is None


def test_contract_creation_tx_has_no_target_and_does_not_match():
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xcreate")],
        txs={"0xcreate": {"from": SPENDER, "to": None}},
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_tx_fetch_failure_is_unclassified_out():
    result = _scan(
        [_log(sender=WALLET, recipient=EOA, tx_hash="0xgone")],
        fail_get_tx=True,
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT


def test_code_fetch_failure_is_unknown_and_unclassified():
    result = _scan([_log(sender=EOA, recipient=WALLET)], fail_get_code=True)
    (obs,) = result.observations
    assert obs.counterparty_kind is CounterpartyKind.UNKNOWN
    assert obs.classification is FlowClassification.UNCLASSIFIED_IN


def test_mint_from_zero_address_is_mint_burn_never_deposit():
    eth = FakeEth([_log(sender=ZERO_ADDRESS, recipient=WALLET)])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        token_universe=UNIVERSE,
    )
    (obs,) = result.observations
    assert obs.counterparty_kind is CounterpartyKind.MINT_BURN
    assert obs.classification is FlowClassification.UNCLASSIFIED_IN
    # address(0) must never be probed with eth_getCode — it has no code and
    # would masquerade as an EOA.
    assert eth.code_calls == []


def test_burn_to_zero_address_is_mint_burn_never_withdrawal():
    eth = FakeEth([_log(sender=WALLET, recipient=ZERO_ADDRESS, tx_hash="0xburn")])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        token_universe=UNIVERSE,
    )
    (obs,) = result.observations
    assert obs.counterparty_kind is CounterpartyKind.MINT_BURN
    assert obs.classification is FlowClassification.UNCLASSIFIED_OUT
    assert eth.code_calls == []
    assert eth.tx_calls == []


# --------------------------------------------------------------------------
# Scan mechanics
# --------------------------------------------------------------------------


def test_self_transfer_is_dropped():
    result = _scan([_log(sender=WALLET, recipient=WALLET)])
    assert result.observations == ()
    assert result.status is ScanStatus.OK


def test_transfer_seen_by_both_topic_scans_is_deduped():
    log = _log(sender=EOA, recipient=WALLET, tx_hash="0xdupe", log_index=3)

    class EchoEth(FakeEth):
        """Returns the same log from both the inbound and outbound filter."""

        def get_logs(self, params):
            self.log_calls.append((params["fromBlock"], params["toBlock"]))
            return [log]

    eth = EchoEth([log])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=100,
        token_universe=UNIVERSE,
    )
    assert len(eth.log_calls) == 2
    assert len(result.observations) == 1


def test_erc721_four_topic_transfer_is_ignored():
    result = _scan(
        [
            _log(
                sender=EOA,
                recipient=WALLET,
                extra_topic="0x" + "00" * 31 + "07",
            )
        ]
    )
    assert result.observations == ()


def test_same_symbol_on_two_chains_keyed_apart():
    logs = [_log(sender=EOA, recipient=WALLET, tx_hash="0xa1")]
    arb = _scan(logs)
    eth = FakeEth(logs)
    base = scan_chain_transfers(
        FakeWeb3(eth),
        chain="base",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        token_universe=UNIVERSE,
    )
    (a,) = arb.observations
    (b,) = base.observations
    assert a.symbol == b.symbol == "USDC"
    assert a.key != b.key
    assert (a.chain, a.token_address) == ("arbitrum", USDC.lower())
    assert (b.chain, b.token_address) == ("base", USDC.lower())


def test_missing_decimals_marks_observation_unmeasurable():
    result = _scan(
        [_log(token=WETH, sender=EOA, recipient=WALLET, amount=10**18)],
        universe={WETH: TokenInfo(symbol="WETH", decimals=None)},
    )
    (obs,) = result.observations
    assert obs.measurable is False
    assert obs.amount is None
    assert obs.raw_amount == 10**18
    assert result.has_unmeasurable is True


def test_measurable_scan_has_no_unmeasurable_flag():
    result = _scan([_log(sender=EOA, recipient=WALLET)])
    assert result.has_unmeasurable is False


def test_hex_case_normalization_of_hashes_and_addresses():
    result = _scan(
        [_log(token=USDC.upper(), sender=EOA.upper(), recipient=WALLET.upper(), tx_hash="0xDEADBEEF")],
        ledger=("0xDeAdBeEf",),
    )
    (obs,) = result.observations
    assert obs.classification is FlowClassification.STRATEGY_TX
    assert obs.tx_hash == "0xdeadbeef"
    assert obs.token_address == USDC.lower()
    assert obs.counterparty == EOA.lower()


def test_block_budget_truncation_within_backlog_cap():
    result = _scan([], from_block=1_000, head=1_000 + MAX_BLOCKS_PER_CYCLE + 25_000)
    assert result.status is ScanStatus.OK
    assert result.to_block == 1_000 + MAX_BLOCKS_PER_CYCLE


def test_backlog_beyond_cap_is_range_unmeasurable():
    result = _scan([], from_block=0, head=MAX_BACKLOG_BLOCKS + 1)
    assert result.status is ScanStatus.RANGE_UNMEASURABLE
    assert result.observations == ()
    assert result.error is not None


def test_solana_scan_fails_closed_without_evm_rpc():
    eth = FakeEth([])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="solana",
        wallet="6qXE4b9HQiLSkhVcCdEYJ1J6cv8nSyAVthAcZKGHsfYC",
        from_block_exclusive=10,
        head_block=20,
        token_universe={"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": TokenInfo(symbol="USDC", decimals=6)},
    )

    assert result.status is ScanStatus.RANGE_UNMEASURABLE
    assert result.to_block == 20
    assert result.observations == ()
    assert result.error is not None and "unsupported for solana chains" in result.error
    assert eth.log_calls == []
    assert eth.code_calls == []
    assert eth.tx_calls == []
    assert eth.receipt_calls == []


def test_chunk_halving_then_success():
    eth = FakeEth([], max_span=CHUNK_BLOCKS // 2)
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=CHUNK_BLOCKS,
        token_universe=UNIVERSE,
    )
    assert result.status is ScanStatus.OK
    assert result.to_block == CHUNK_BLOCKS
    # First attempt used the full chunk and failed; the retry halved it, then
    # two half-chunks x two directions completed the range — no extra calls.
    assert eth.log_calls[0] == (1, CHUNK_BLOCKS)
    assert eth.log_calls[1] == (1, CHUNK_BLOCKS // 2)
    assert len(eth.log_calls) == 5


def test_chunk_halving_hits_floor_and_returns_transient_failure():
    eth = FakeEth([], max_span=MIN_CHUNK_BLOCKS - 1)
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=500,
        head_block=20_000,
        token_universe=UNIVERSE,
    )
    assert result.status is ScanStatus.TRANSIENT_FAILURE
    assert result.error is not None
    # Nothing was scanned, so the caller's cursor must not advance.
    assert result.to_block == 500


def test_transient_failure_keeps_earlier_chunk_observations():
    # Chunk 1 succeeds at the halved size, then the provider dies outright.
    class FlakyEth(FakeEth):
        def get_logs(self, params):
            if params["fromBlock"] > CHUNK_BLOCKS:
                raise ValueError("range too wide")
            return super().get_logs(params)

    eth = FlakyEth([_log(sender=EOA, recipient=WALLET, block=42)])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=20_000,
        token_universe=UNIVERSE,
    )
    assert result.status is ScanStatus.TRANSIENT_FAILURE
    assert result.to_block == CHUNK_BLOCKS
    assert [o.classification for o in result.observations] == [FlowClassification.DEPOSIT]


def test_empty_universe_short_circuits():
    eth = FakeEth([_log(sender=EOA, recipient=WALLET)])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=100,
        token_universe={},
    )
    assert result.observations == ()
    assert eth.log_calls == []


def test_code_lookup_is_cached_across_transfers():
    logs = [
        _log(sender=EOA, recipient=WALLET, tx_hash="0xa1", log_index=0),
        _log(sender=EOA, recipient=WALLET, tx_hash="0xa2", log_index=1),
    ]
    eth = FakeEth(logs)
    scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=100,
        token_universe=UNIVERSE,
    )
    assert eth.code_calls == [EOA.lower()]


def test_observations_are_block_ordered():
    logs = [
        _log(sender=EOA, recipient=WALLET, tx_hash="0xb2", block=200, log_index=1),
        _log(sender=EOA, recipient=WALLET, tx_hash="0xb1", block=100, log_index=0),
    ]
    result = _scan(logs)
    assert [o.block_number for o in result.observations] == [100, 200]


def test_rpc_address_params_are_checksummed_like_web3_middleware():
    """web3.py's validation middleware rejects lowercase address params.

    Found live in the VIB-5866 real-fork proof run: a lowercased ``address``
    filter makes every ``eth_getLogs`` call fail before reaching the node,
    which a case-insensitive fake provider can never catch. This fake enforces
    EIP-55 exactly like the middleware.
    """
    from web3 import Web3

    token = "0x00000000000000000000000000000000000000ab"
    eoa = "0x00000000000000000000000000000000000000cd"

    class ChecksumEnforcingEth(FakeEth):
        def get_logs(self, params: dict) -> list[dict]:
            for entry in params["address"]:
                if entry != Web3.to_checksum_address(entry):
                    raise ValueError("middleware: address param is not EIP-55 checksummed")
            return super().get_logs(params)

        def get_code(self, address: str) -> str:
            if address != Web3.to_checksum_address(address):
                raise ValueError("middleware: address param is not EIP-55 checksummed")
            return super().get_code(address)

    eth = ChecksumEnforcingEth([_log(sender=eoa, recipient=WALLET, token=token)])
    result = scan_chain_transfers(
        FakeWeb3(eth),
        chain="arbitrum",
        wallet=WALLET,
        from_block_exclusive=0,
        head_block=1_000,
        # Mixed-case caller input must still reach the wire checksummed.
        token_universe={token.upper().replace("0X", "0x"): TokenInfo(symbol="TOK", decimals=6)},
    )
    assert result.status is ScanStatus.OK
    (obs,) = result.observations
    assert obs.classification is FlowClassification.DEPOSIT
    assert eth.code_calls, "code lookup must have gone through the checksum path"
