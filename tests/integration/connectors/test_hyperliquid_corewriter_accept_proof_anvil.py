"""HyperEVM CoreWriter accept-proof — on-chain, real fork of chain 999.

Proves that the CoreWriter system contract on HyperEVM
(``0x3333…3333``) **accepts** our byte-exact-encoded limit-order action and
emits the ``RawAction`` event when the calldata built by
``almanak.connectors.hyperliquid.sdk`` is submitted on a **managed Anvil fork
of HyperEVM mainnet (chain id 999)**.

This is layers 1–3 of the intent-test model (compile calldata / execute on real
chain state / observe the submission log). The balance-delta layer (layer 4) is
**structurally impossible** here and is excused in
``scripts/ci/intent-coverage-excused.yml`` (hyperliquid PERP_OPEN / PERP_CLOSE):
CoreWriter only *queues* the order — the fill, the position, and the USDC-margin
change settle asynchronously on HyperCore, **off the EVM**. A fork cannot emulate
HyperCore's off-EVM matching engine, so there is no on-fork balance/position delta
to assert. We therefore assert ONLY that the EVM accepted + logged the action; we
do NOT assert on fills, positions, or balances (there are none on a fork).

Why this test lives under ``tests/integration/connectors/`` and NOT
``tests/intents/``: ``tests/intents/`` is reserved for canonical four-layer intent
tests, and ``scripts/ci/validate_intent_test_layers.py`` scans that tree and
REQUIRES a balance-delta layer in every test function. This test has no balance
layer by construction (see above), so it lives here — the same placement the
Aave-Linea frozen-pool anvil repro uses for helper-only on-chain checks.

Why we bypass the compiler: ``HyperliquidCompiler`` reads the HyperCore oracle
(``0x0807``) and position (``0x0800``) **precompiles** via ``eth_call`` to build a
fail-closed price band. Those are node-level HyperCore features that a plain Anvil
fork does NOT emulate (they return empty on a fork → the compiler would fail
closed). So we build the CoreWriter calldata DIRECTLY from the ``sdk.py`` encoders
— which is exactly the encode path the compiler ultimately calls — and submit it.
This isolates and proves the encode/execute contract against real chain bytecode.

Run (self-contained — starts + tears down its own Anvil)::

    uv run pytest \
        tests/integration/connectors/test_hyperliquid_corewriter_accept_proof_anvil.py \
        -v -s --import-mode=importlib -p no:cacheprovider

Skips gracefully when ``anvil`` is not on PATH or the HyperEVM RPC is unreachable,
so the offline unit lane stays green.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from decimal import Decimal

import pytest
from eth_account import Account
from web3 import Web3

from almanak.connectors.hyperliquid import sdk
from almanak.connectors.hyperliquid.addresses import (
    CORE_WRITER_ADDRESS,
    RAW_ACTION_EVENT_TOPIC,
)

# --------------------------------------------------------------------------- #
# Fork / harness configuration
# --------------------------------------------------------------------------- #

# HyperEVM mainnet. The chain descriptor (almanak/core/chains/hyperevm.py) pins
# the same public RPC and chain id; ALMANAK_HYPEREVM_RPC_URL may override.
HYPEREVM_RPC_URL = os.environ.get("ALMANAK_HYPEREVM_RPC_URL", "https://rpc.hyperliquid.xyz/evm")
HYPEREVM_CHAIN_ID = 999

# Anvil default dev account 0 — deterministic, publicly known test key. Used ONLY
# to sign a fork transaction on a throwaway local Anvil (no real funds, no real
# key material). This is test egress to a local fork, permitted by AGENTS.md.
ANVIL_DEV_ADDRESS = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ANVIL_DEV_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"

# CoreWriter "emit an event + queue an action" is cheap. A blind cross-check that
# the tx is event-emit semantics (not some heavy on-chain settlement) — kept
# generous vs the observed ~40–70k so a small opcode/fork variation does not
# false-fail, while still proving the tx is NOT doing expensive on-chain work.
GAS_CEILING_EVENT_EMIT = 100_000

_ANVIL_READY_TIMEOUT_S = 30.0


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _rpc_reachable(url: str) -> bool:
    """Cheap liveness probe so an offline lane skips instead of hanging on fork."""
    try:
        w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 8}))
        return w3.is_connected() and w3.eth.chain_id == HYPEREVM_CHAIN_ID
    except Exception:
        return False


@pytest.fixture(scope="module")
def anvil_fork():
    """Start a self-contained Anvil fork of HyperEVM (chain 999); tear it down.

    Skips (does not fail) when ``anvil`` is missing or the upstream RPC is
    unreachable — keeping the offline unit lane green.
    """
    if shutil.which("anvil") is None:
        pytest.skip("anvil binary not on PATH — install Foundry to run the CoreWriter accept-proof")
    if not _rpc_reachable(HYPEREVM_RPC_URL):
        pytest.skip(f"HyperEVM RPC {HYPEREVM_RPC_URL} unreachable (offline / rate-limited) — skipping fork")

    port = _free_port()
    proc = subprocess.Popen(
        [
            "anvil",
            "--fork-url",
            HYPEREVM_RPC_URL,
            "--chain-id",
            str(HYPEREVM_CHAIN_ID),
            "--port",
            str(port),
            "--silent",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    rpc = f"http://127.0.0.1:{port}"
    try:
        w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
        deadline = time.time() + _ANVIL_READY_TIMEOUT_S
        ready = False
        while time.time() < deadline:
            if proc.poll() is not None:
                pytest.skip("anvil exited during startup (fork could not be established) — skipping")
            try:
                if w3.is_connected() and w3.eth.chain_id == HYPEREVM_CHAIN_ID:
                    ready = True
                    break
            except Exception:
                pass
            time.sleep(0.5)
        if not ready:
            pytest.skip("anvil fork did not become ready within timeout — skipping")
        yield w3
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def _build_limit_order_calldata() -> bytes:
    """Byte-exact CoreWriter limit-order calldata via the production sdk encoders.

    BTC (asset 0, szDecimals=5), aggressive IOC buy at a $70k limit for 0.001 BTC.
    The price is deliberately high (aggressive) so that, on a live HyperCore, this
    would cross — but on a fork nothing settles; we only prove EVM acceptance.
    """
    action = sdk.LimitOrderAction(
        asset=0,
        is_buy=True,
        limit_px=sdk.price_to_wire(Decimal("70000"), 5),
        sz=sdk.size_to_wire(Decimal("0.001"), 5),
        reduce_only=False,
        tif=sdk.TIF_IOC,
        cloid=1,
    )
    return sdk.encode_send_raw_action_calldata(sdk.encode_limit_order_action(action))


# --------------------------------------------------------------------------- #
# The accept-proof (layers 1–3, on a real fork of chain 999)
# --------------------------------------------------------------------------- #


def test_corewriter_accepts_and_emits_raw_action(anvil_fork):
    """CoreWriter accepts our encoded limit order and emits RawAction on a fork.

    Assertions (the accept-proof):
      1. receipt.status == 1        — CoreWriter accepted; the tx did NOT revert.
      2. a log with topics[0] == RAW_ACTION_EVENT_TOPIC exists AND its
         log.address == CoreWriter (0x3333…3333) — the submission was recorded.
      3. gas used is small (< 100k) — consistent with "emit an event" semantics,
         NOT heavy on-chain settlement (settlement is off-EVM on HyperCore).

    NOT asserted (structurally impossible on a fork — off-EVM settlement):
    fills, positions, USDC-margin/balance deltas. See module docstring +
    scripts/ci/intent-coverage-excused.yml.
    """
    w3 = anvil_fork
    calldata = _build_limit_order_calldata()
    core_writer = Web3.to_checksum_address(CORE_WRITER_ADDRESS)

    # Fund the dev signer explicitly (robust even if fork default balances vary).
    w3.provider.make_request("anvil_setBalance", [ANVIL_DEV_ADDRESS, hex(10 * 10**18)])

    acct = Account.from_key(ANVIL_DEV_PRIVATE_KEY)
    assert acct.address == Web3.to_checksum_address(ANVIL_DEV_ADDRESS)

    tx = {
        "chainId": HYPEREVM_CHAIN_ID,
        "from": acct.address,
        "to": core_writer,
        "value": 0,
        "data": "0x" + calldata.hex(),
        "nonce": w3.eth.get_transaction_count(acct.address),
        "gas": 300_000,
        "gasPrice": w3.eth.gas_price,
    }

    signed = acct.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

    # --- Assertion 1: accepted, did not revert ---------------------------- #
    assert receipt["status"] == 1, (
        f"CoreWriter REVERTED our encoded limit-order action (status={receipt['status']}). "
        f"This is a REAL encoding/ABI regression — the sendRawAction blob was rejected on-chain. "
        f"tx={tx_hash.hex()}"
    )

    # --- Assertion 2: RawAction emitted by CoreWriter --------------------- #
    raw_action_topic = Web3.to_bytes(hexstr=RAW_ACTION_EVENT_TOPIC)
    matching = [
        log
        for log in receipt["logs"]
        if log["topics"]
        and bytes(log["topics"][0]) == raw_action_topic
        and Web3.to_checksum_address(log["address"]) == core_writer
    ]
    assert matching, (
        "CoreWriter accepted the tx but did NOT emit a RawAction log at 0x3333…3333. "
        f"Logs found: {[(Web3.to_checksum_address(lg['address']), bytes(lg['topics'][0]).hex() if lg['topics'] else None) for lg in receipt['logs']]}. "
        "If this fires, either the RawAction topic constant drifted or CoreWriter's "
        f"event ABI changed — investigate before shipping. tx={tx_hash.hex()}"
    )

    # --- Assertion 3: cheap, event-emit gas profile ----------------------- #
    gas_used = receipt["gasUsed"]
    assert gas_used < GAS_CEILING_EVENT_EMIT, (
        f"gasUsed={gas_used} exceeds the {GAS_CEILING_EVENT_EMIT} event-emit ceiling — "
        "CoreWriter should only emit a log + queue the action, not do heavy on-chain work. "
        "A large gas figure suggests the calldata triggered unexpected on-chain execution."
    )

    print(
        f"\nCoreWriter accept-proof OK: status={receipt['status']} gasUsed={gas_used} "
        f"RawAction logs={len(matching)} tx={tx_hash.hex()}"
    )


# --------------------------------------------------------------------------- #
# Negative sanity — fail-closed encoding (pure Python, no chain needed)
# --------------------------------------------------------------------------- #


def test_malformed_actions_rejected_by_encoder_before_submission():
    """The sdk encoder rejects malformed actions with ValueError BEFORE any submit.

    Proves fail-closed encoding: a bad TIF, a zero size, or an out-of-range asset
    can never become on-chain calldata. This is the encode-side guard that keeps
    a garbage action from ever reaching CoreWriter — pure Python, no fork needed.
    """
    good_px = sdk.price_to_wire(Decimal("70000"), 5)
    good_sz = sdk.size_to_wire(Decimal("0.001"), 5)

    # Invalid TIF (only ALO/GTC/IOC = 1/2/3 are legal).
    with pytest.raises(ValueError, match="invalid tif"):
        sdk.encode_limit_order_action(
            sdk.LimitOrderAction(asset=0, is_buy=True, limit_px=good_px, sz=good_sz, reduce_only=False, tif=9)
        )

    # Asset index out of uint32 range.
    with pytest.raises(ValueError):
        sdk.encode_limit_order_action(
            sdk.LimitOrderAction(
                asset=2**32,
                is_buy=True,
                limit_px=good_px,
                sz=good_sz,
                reduce_only=False,
                tif=sdk.TIF_IOC,
            )
        )

    # Zero size never scales to a valid wire value (rounds to zero).
    with pytest.raises(ValueError):
        sdk.size_to_wire(Decimal("0"), 5)
