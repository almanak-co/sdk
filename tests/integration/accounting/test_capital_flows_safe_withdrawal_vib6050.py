"""Real-fork proof for VIB-6050 — a Safe withdrawal must classify as WITHDRAWAL.

Why this is a fork test and not a unit test
===========================================

The whole VIB-6043 / VIB-6050 defect class is "which address actually shows up
at runtime". A unit test that hand-builds ``{"from": owner, "to": safe}`` proves
the classifier's *logic*; it cannot prove that a real Safe withdrawal on a real
chain produces that shape. Two things have to be true simultaneously, and only a
chain can assert both:

1. ``tx.from`` is the Safe **owner**, never the Safe — the pre-fix
   ``sender == wallet`` predicate is structurally impossible to satisfy.
2. ``tx.to`` **is** the Safe — the post-fix :func:`wallet_initiated` predicate
   has something real to match on.

This test deploys a genuine Safe v1.4.1 (the same canonical CREATE2 factory the
hosted platform uses), funds it with real USDC on an Arbitrum fork, withdraws
via ``Safe.execTransaction`` (the operator-withdrawal shape), then runs the
production ``scan_chain_transfers`` reader against the resulting chain state.

Run:

    uv run pytest tests/integration/accounting/test_capital_flows_safe_withdrawal_vib6050.py \
        -m integration -v -n0 --import-mode=importlib

Requires ``ALCHEMY_API_KEY`` (fork RPC) and ``anvil`` / ``cast`` on PATH.
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest
from eth_account import Account
from web3 import Web3

from almanak.framework.accounting.capital_flows import (
    SAFE_EXECUTION_SUCCESS_TOPIC,
    CounterpartyKind,
    FlowClassification,
    ScanStatus,
    TokenInfo,
    TransferDirection,
    clear_provenance_caches,
    scan_chain_transfers,
)
from almanak.gateway.utils.rpc_provider import get_rpc_url
from tests.conftest_gateway import AnvilFixture
from tests.intents._zodiac_helpers import _exec_safe_tx, deploy_test_safe

pytestmark = [pytest.mark.integration, pytest.mark.anvil]

CHAIN = "arbitrum"
USDC = Web3.to_checksum_address("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
USDC_BALANCE_SLOT = 9
USDC_DECIMALS = 6

# Anvil default account 1 — the Safe owner / withdrawal signer.
OWNER_EOA = Web3.to_checksum_address("0x70997970C51812dc3A010C7d01b50e0d17dc79C8")
OWNER_PRIVATE_KEY = "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"

SEED_USDC = 1_000 * 10**USDC_DECIMALS
WITHDRAW_USDC = 250 * 10**USDC_DECIMALS

_ERC20_TRANSFER_ABI = [
    {
        "name": "transfer",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [{"name": "to", "type": "address"}, {"name": "amount", "type": "uint256"}],
        "outputs": [{"name": "", "type": "bool"}],
    },
    {
        "name": "balanceOf",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]


@pytest.fixture(scope="module")
def anvil() -> Iterator[AnvilFixture]:
    """A dedicated Arbitrum fork on a freshly allocated port.

    Module-scoped and self-owned so this proof never contends with another
    session's long-lived forks or gateway ports.
    """
    if not os.environ.get("ALCHEMY_API_KEY"):
        pytest.skip("ALCHEMY_API_KEY required for the Arbitrum fork")
    fixture = AnvilFixture(CHAIN, get_rpc_url(CHAIN))
    fixture.start()
    try:
        yield fixture
    finally:
        fixture.stop()


@pytest.fixture(scope="module")
def web3(anvil: AnvilFixture) -> Web3:
    return Web3(Web3.HTTPProvider(f"http://127.0.0.1:{anvil.port}", request_kwargs={"timeout": 60}))


@pytest.fixture(autouse=True)
def _clear_caches() -> Iterator[None]:
    clear_provenance_caches()
    yield
    clear_provenance_caches()


def _fund_native(web3: Web3, address: str, wei: int) -> None:
    web3.provider.make_request("anvil_setBalance", [Web3.to_checksum_address(address), hex(wei)])


def _fund_usdc(web3: Web3, address: str, amount: int) -> None:
    """Seed an ERC-20 balance by writing the balanceOf mapping slot directly."""
    slot = Web3.keccak(
        bytes.fromhex(Web3.to_checksum_address(address)[2:].rjust(64, "0"))
        + USDC_BALANCE_SLOT.to_bytes(32, "big")
    )
    web3.provider.make_request("anvil_setStorageAt", [USDC, "0x" + slot.hex(), f"0x{amount:064x}"])


@pytest.fixture(scope="module")
def safe_withdrawal(web3: Web3) -> dict:
    """Deploy a Safe, seed it with USDC, and withdraw to a fresh EOA.

    Returns the addresses + block bracket the reader needs.
    """
    _fund_native(web3, OWNER_EOA, 10 * 10**18)
    safe = deploy_test_safe(web3, OWNER_EOA, OWNER_PRIVATE_KEY)
    _fund_native(web3, safe, 10 * 10**18)
    _fund_usdc(web3, safe, SEED_USDC)

    usdc = web3.eth.contract(address=USDC, abi=_ERC20_TRANSFER_ABI)
    assert usdc.functions.balanceOf(Web3.to_checksum_address(safe)).call() == SEED_USDC, (
        "storage seeding did not land — wrong balanceOf slot for USDC on this fork"
    )

    # A fresh, code-less EOA: the withdrawal destination must be an EOA for
    # rule 5 (an outflow to a contract stays UNCLASSIFIED_OUT by design).
    recipient = Account.create().address
    assert web3.eth.get_code(recipient) == b"", "recipient must be a bare EOA"

    from_block = web3.eth.block_number

    calldata = usdc.encode_abi("transfer", args=[Web3.to_checksum_address(recipient), WITHDRAW_USDC])
    receipt = _exec_safe_tx(
        web3,
        safe,
        USDC,
        bytes.fromhex(calldata[2:]),
        0,  # SafeOperation.CALL
        OWNER_EOA,
        OWNER_PRIVATE_KEY,
    )

    assert usdc.functions.balanceOf(Web3.to_checksum_address(recipient)).call() == WITHDRAW_USDC, (
        "the withdrawal did not actually move USDC on-chain"
    )

    return {
        "safe": safe.lower(),
        "recipient": recipient.lower(),
        "tx_hash": receipt["transactionHash"].hex(),
        "from_block": from_block,
        "head_block": web3.eth.block_number,
    }


def test_safe_withdrawal_tx_is_shaped_the_way_the_bug_requires(web3: Web3, safe_withdrawal: dict) -> None:
    """Ground truth first: the Safe is the tx TARGET and never the tx SENDER.

    This is the fact the unit tests can only assume. If it ever stops holding,
    the fix below is aimed at the wrong thing and this test says so directly.
    """
    tx_hash = safe_withdrawal["tx_hash"]
    tx = web3.eth.get_transaction(tx_hash if tx_hash.startswith("0x") else "0x" + tx_hash)
    sender = tx["from"].lower()
    target = (tx["to"] or "").lower()

    assert sender == OWNER_EOA.lower()
    calldata = tx["input"]
    selector = (calldata.hex() if hasattr(calldata, "hex") else str(calldata)).removeprefix("0x")[:8]
    assert selector == "6a761202", (
        "the operator-withdrawal shape must be execTransaction — the only Safe entry "
        "point that runs checkSignatures, and the load-bearing authorisation signal"
    )
    assert sender != safe_withdrawal["safe"], (
        "pre-condition of VIB-6050: a Safe can never be the tx sender — "
        "`tx.from == wallet` is structurally unsatisfiable in Safe mode"
    )
    assert target == safe_withdrawal["safe"], "the Safe must be the tx target for an operator withdrawal"


def test_safe_withdrawal_classifies_as_withdrawal_on_a_real_fork(web3: Web3, safe_withdrawal: dict) -> None:
    """The production reader must book a real Safe withdrawal as WITHDRAWAL.

    Before VIB-6050 this asserted ``UNCLASSIFIED_OUT``: every hosted deployment
    (Safe-only) systematically mis-bucketed genuine capital outflows, which then
    fed deposit/withdrawal-adjusted PnL.
    """
    result = scan_chain_transfers(
        web3,
        chain=CHAIN,
        wallet=safe_withdrawal["safe"],
        from_block_exclusive=safe_withdrawal["from_block"],
        head_block=safe_withdrawal["head_block"],
        token_universe={USDC.lower(): TokenInfo(symbol="USDC", decimals=USDC_DECIMALS)},
        ledger_tx_hashes=(),  # NOT a strategy tx — an operator withdrawal
    )

    assert result.status is ScanStatus.OK, result.error
    outflows = [obs for obs in result.observations if obs.direction is TransferDirection.OUT]
    assert len(outflows) == 1, f"expected exactly one outflow, got {result.observations}"

    (obs,) = outflows
    assert obs.counterparty == safe_withdrawal["recipient"]
    assert obs.counterparty_kind is CounterpartyKind.EOA
    assert obs.classification is FlowClassification.WITHDRAWAL
    assert obs.measurable is True
    assert obs.raw_amount == WITHDRAW_USDC


@pytest.fixture(scope="module")
def module_backdoor_drain(web3: Web3) -> dict:
    """A REAL module-backdoor drain that forges ExecutionSuccess via DELEGATECALL.

    The attack the selector check exists to stop, executed on-chain rather than
    hand-built: an attacker EOA is enabled as a Safe module, then calls
    ``execTransactionFromModule(forger, 0, "", DELEGATECALL)``. Delegatecalled
    code runs in the Safe's context, so the forger's ``LOG`` carries
    ``log.address == safe`` — an ``ExecutionSuccess`` indistinguishable by
    emitter from a genuine one. The same delegatecall moves the Safe's USDC.
    """
    attacker_key = "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a"
    attacker = Account.from_key(attacker_key).address
    _fund_native(web3, attacker, 10 * 10**18)
    _fund_native(web3, OWNER_EOA, 10 * 10**18)

    safe = deploy_test_safe(web3, OWNER_EOA, OWNER_PRIVATE_KEY)
    _fund_native(web3, safe, 10 * 10**18)
    _fund_usdc(web3, safe, SEED_USDC)

    victim_recipient = Account.create().address
    # Runtime code: USDC.transfer(victim_recipient, WITHDRAW_USDC), then
    # LOG2(ExecutionSuccess, 0). Both execute as the Safe under DELEGATECALL.
    forger_code = (
        "63a9059cbb60e01b600052"  # PUSH4 transfer selector; shift; MSTORE at 0
        f"73{victim_recipient[2:].lower()}600452"  # PUSH20 recipient; MSTORE at 4
        f"7f{WITHDRAW_USDC:064x}602452"  # PUSH32 amount; MSTORE at 0x24
        "600060006044600060007f" + "0" * 24 + USDC[2:].lower() + "5af150"  # CALL usdc
        "7f" + "00" * 32  # topic2 = 0
        + "7f" + SAFE_EXECUTION_SUCCESS_TOPIC[2:]  # topic1 = ExecutionSuccess
        + "60006000a200"  # size, offset, LOG2, STOP
    )
    forger = Web3.to_checksum_address("0x00000000000000000000000000000000dEaDBe01")
    web3.provider.make_request("anvil_setCode", [forger, "0x" + forger_code])

    enable = web3.eth.contract(
        abi=[{"name": "enableModule", "type": "function", "stateMutability": "nonpayable",
              "inputs": [{"name": "module", "type": "address"}], "outputs": []}]
    ).encode_abi("enableModule", args=[attacker])
    _exec_safe_tx(web3, safe, safe, bytes.fromhex(enable[2:]), 0, OWNER_EOA, OWNER_PRIVATE_KEY)

    from_block = web3.eth.block_number
    mod = web3.eth.contract(
        address=Web3.to_checksum_address(safe),
        abi=[{"name": "execTransactionFromModule", "type": "function", "stateMutability": "nonpayable",
              "inputs": [{"name": "to", "type": "address"}, {"name": "value", "type": "uint256"},
                         {"name": "data", "type": "bytes"}, {"name": "operation", "type": "uint8"}],
              "outputs": [{"name": "success", "type": "bool"}]}],
    )
    tx = mod.functions.execTransactionFromModule(forger, 0, b"", 1).build_transaction(
        {"from": attacker, "nonce": web3.eth.get_transaction_count(attacker), "gas": 600_000}
    )
    signed = Account.sign_transaction(tx, attacker_key)
    receipt = web3.eth.wait_for_transaction_receipt(web3.eth.send_raw_transaction(signed.raw_transaction))
    assert receipt["status"] == 1

    return {
        "safe": safe.lower(),
        "recipient": victim_recipient.lower(),
        "from_block": from_block,
        "head_block": web3.eth.block_number,
    }


def test_module_backdoor_drain_forges_execution_success_but_is_not_a_withdrawal(
    web3: Web3, module_backdoor_drain: dict
) -> None:
    """The measured bypass of the first fix — now closed, and pinned on-chain.

    Asserts BOTH halves: that the forgery genuinely succeeds at the chain level
    (so this test cannot silently stop testing anything), and that the reader
    still refuses to call it a WITHDRAWAL.
    """
    usdc = web3.eth.contract(address=USDC, abi=_ERC20_TRANSFER_ABI)
    assert usdc.functions.balanceOf(Web3.to_checksum_address(module_backdoor_drain["recipient"])).call() > 0, (
        "the attacker did not actually drain the Safe — the fixture no longer reproduces the attack"
    )

    logs = web3.eth.get_logs(
        {
            "fromBlock": module_backdoor_drain["from_block"] + 1,
            "toBlock": module_backdoor_drain["head_block"],
            "address": Web3.to_checksum_address(module_backdoor_drain["safe"]),
            "topics": [SAFE_EXECUTION_SUCCESS_TOPIC],
        }
    )
    assert logs, "DELEGATECALL did not forge ExecutionSuccess from the Safe — attack premise gone"

    result = scan_chain_transfers(
        web3,
        chain=CHAIN,
        wallet=module_backdoor_drain["safe"],
        from_block_exclusive=module_backdoor_drain["from_block"],
        head_block=module_backdoor_drain["head_block"],
        token_universe={USDC.lower(): TokenInfo(symbol="USDC", decimals=USDC_DECIMALS)},
        ledger_tx_hashes=(),
    )
    outflows = [o for o in result.observations if o.direction is TransferDirection.OUT]
    assert outflows, "the drain produced no outflow observation at all"
    for obs in outflows:
        assert obs.classification is not FlowClassification.WITHDRAWAL, (
            "a module-backdoor drain with a forged ExecutionSuccess booked as a WITHDRAWAL — "
            "the theft would be netted out of PnL"
        )


def test_participation_filter_shape_is_valid_against_a_real_node(web3: Web3, safe_withdrawal: dict) -> None:
    """VIB-6049's discovery filter must actually work on a node, not just a mock.

    ``WalletMonitor._leader_participation_txs`` finds a smart-account leader
    with two ``eth_getLogs`` calls whose indexed-address topic position carries
    an **array** of leader addresses. A mocked gateway will happily "match" a
    filter a real node rejects or answers with ``[]``, so the one piece most
    likely to be silently wrong is asserted here against Anvil: the same Safe
    withdrawal proven above must be discoverable by that exact filter shape.
    """
    # Constants inlined rather than imported: the monitor that consumes this
    # filter shape lives on a different branch (VIB-6049), and this assertion is
    # about the NODE accepting the filter, not about that module's code.
    transfer_topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    leader_topics = ["0x" + safe_withdrawal["safe"].removeprefix("0x").rjust(64, "0")]
    logs = web3.eth.get_logs(
        {
            "fromBlock": safe_withdrawal["from_block"] + 1,
            "toBlock": safe_withdrawal["head_block"],
            # Leader is the SENDER: topic position 1, address ARRAY.
            "topics": [transfer_topic, leader_topics, None],
        }
    )

    hashes = {log["transactionHash"].hex().lower().removeprefix("0x") for log in logs}
    assert safe_withdrawal["tx_hash"].lower().removeprefix("0x") in hashes, (
        "the participation filter did not find a withdrawal the Safe demonstrably made"
    )
