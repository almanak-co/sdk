"""Capital-flow transfer provenance reader (VIB-5866 leg B).

Wallet PnL must separate *earnings* from *capital movements*. A USDC balance
that grows because the operator wired in funds is not profit; a balance that
shrinks because someone with an approval pulled tokens out is not a withdrawal
— it is a loss. This module reads raw ERC-20 ``Transfer`` logs for a wallet
over a block range and classifies every transfer that touches it, so the
metrics builder can subtract genuine deposits/withdrawals and refuse to net out
anything it could not prove.

Design notes:

- **Gateway boundary.** Every RPC goes through an injected web3-like handle,
  which in production is the gateway-backed ``Web3`` from
  ``almanak.framework.web3.gateway_provider.get_gateway_web3``. Only
  ``eth_getLogs`` / ``eth_getCode`` / ``eth_getTransactionByHash`` /
  ``eth_getTransactionReceipt`` are used and all four are on the gateway
  allowlist. Unit tests inject a fake handle — no
  sockets.
- **Empty is not zero.** A transfer on a token whose decimals we do not know
  yields ``amount=None`` and ``measurable=False``; it is never coerced to
  ``Decimal("0")``. The caller must poison the affected interval rather than
  book a silent zero.
- **Token universe, decimals and prices are inputs**, not something this module
  discovers. Discovery lives with the caller (PR-B), which already owns the
  token registry.
- **The wallet may be a smart account.** ``wallet`` is the deployment's
  *execution address*, which is the Safe under Safe / Zodiac execution. A Safe
  never appears as ``tx.from``; the agent EOA or a Safe owner signs and the
  Safe is the ``tx.to``. Provenance therefore asks "did the wallet *authorise*
  this transaction" (:func:`wallet_initiated`), not "is the wallet the sender"
  — see VIB-6050. Entry is not authorisation, so the smart-account arm requires
  the transaction's own ``execTransaction`` selector (the only Safe entry point
  that validates owner signatures, and the one signal an executed contract
  cannot author), with the Safe's ``ExecutionSuccess`` as secondary evidence
  that the execution succeeded.
"""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol

logger = logging.getLogger(__name__)

# keccak256("Transfer(address,address,uint256)")
TRANSFER_SIG = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# keccak256("ExecutionSuccess(bytes32,uint256)") — emitted by a Safe on a
# successful ``execTransaction``.
#
# SECONDARY corroboration ONLY. **This event is forgeable** and that was
# measured, not assumed: a module calling ``execTransactionFromModule`` with
# ``operation = DELEGATECALL`` runs attacker code *in the Safe's context*, so
# its ``LOG`` opcodes emit with the Safe's own address. A fork run confirmed a
# synthetic ``ExecutionSuccess`` carrying ``log.address == safe``. Whatever this
# event proves, it does not prove owner authorisation on its own.
SAFE_EXECUTION_SUCCESS_TOPIC = "0x442e715f626346e8c54381002da614f62bee8d27386535b2521ec8540898556e"

# ``execTransaction(address,uint256,bytes,uint8,uint256,uint256,uint256,address,address,bytes)``
# — the ONLY Safe entry point that validates owner signatures (``checkSignatures``
# reverts otherwise). This selector is the LOAD-BEARING authorisation signal: it
# lives in the transaction's own authenticated calldata, so unlike an event it
# cannot be synthesised by delegatecalled code.
SAFE_EXEC_TRANSACTION_SELECTOR = "0x6a761202"

# EIP-7702 delegation designator prefix. An EOA that has delegated to an
# implementation still reports code, but it is an EOA for provenance purposes:
# a wire-in from a 7702 account is a deposit, not a contract interaction.
EIP7702_PREFIX = "0xef0100"

# Scan budget constants (VIB-5866). Sized so a cycle costs a bounded number of
# eth_getLogs calls even on fast chains, while still catching up over a few
# cycles after a restart.
CHUNK_BLOCKS = 5_000
MIN_CHUNK_BLOCKS = 500
MAX_BLOCKS_PER_CYCLE = 60_000
MAX_BACKLOG_BLOCKS = 1_000_000

# Provenance lookups are dominated by a handful of repeat counterparties, so a
# small process-wide LRU removes nearly all of the RPC cost.
_CODE_CACHE_SIZE = 1024
_TX_ENDPOINTS_CACHE_SIZE = 256
_TX_LOGS_CACHE_SIZE = 128


class TransferDirection(StrEnum):
    """Direction of a transfer relative to the wallet under observation."""

    IN = "IN"
    OUT = "OUT"


class CounterpartyKind(StrEnum):
    """What the other side of a transfer is."""

    EOA = "EOA"
    CONTRACT = "CONTRACT"
    MINT_BURN = "MINT_BURN"
    UNKNOWN = "UNKNOWN"


class FlowClassification(StrEnum):
    """Provenance verdict for a single transfer."""

    STRATEGY_TX = "STRATEGY_TX"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    UNCLASSIFIED_IN = "UNCLASSIFIED_IN"
    UNCLASSIFIED_OUT = "UNCLASSIFIED_OUT"


class ScanStatus(StrEnum):
    """Outcome of scanning one chain for one cycle."""

    OK = "OK"
    TRANSIENT_FAILURE = "TRANSIENT_FAILURE"
    RANGE_UNMEASURABLE = "RANGE_UNMEASURABLE"


@dataclass(frozen=True)
class TxEndpoints:
    """The ``from`` / ``to`` / 4-byte selector of a transaction.

    ``to`` is ``None`` only for a contract-creation transaction. ``selector`` is
    ``""`` when the transaction carries no calldata (a plain value transfer) or
    fewer than 4 bytes of it.
    """

    sender: str
    to: str | None
    selector: str = ""


@dataclass(frozen=True)
class TokenInfo:
    """Caller-supplied metadata for one token in the scan universe."""

    symbol: str | None = None
    decimals: int | None = None


@dataclass(frozen=True)
class TransferObservation:
    """One ERC-20 transfer touching the wallet, with its provenance verdict."""

    chain: str
    token_address: str
    symbol: str | None
    amount: Decimal | None
    raw_amount: int
    direction: TransferDirection
    counterparty: str
    counterparty_kind: CounterpartyKind
    tx_hash: str
    block_number: int
    log_index: int
    classification: FlowClassification
    measurable: bool

    @property
    def key(self) -> tuple[str, str, int]:
        """Identity of the underlying log — chain, tx hash, log index."""
        return (self.chain, self.tx_hash, self.log_index)


@dataclass(frozen=True)
class ChainScanResult:
    """Result of one chain scan cycle.

    ``to_block`` is the last block the caller may treat as scanned, including
    on ``TRANSIENT_FAILURE`` where it marks the last fully-scanned chunk.
    """

    chain: str
    from_block: int
    to_block: int
    observations: tuple[TransferObservation, ...] = field(default_factory=tuple)
    status: ScanStatus = ScanStatus.OK
    error: str | None = None

    @property
    def has_unmeasurable(self) -> bool:
        """True when any observation could not be converted to human units."""
        return any(not obs.measurable for obs in self.observations)


class Web3Like(Protocol):
    """Minimal structural view of the gateway-backed ``Web3`` handle."""

    eth: Any


# --------------------------------------------------------------------------
# Pure helpers
# --------------------------------------------------------------------------


def _norm_hex(value: Any) -> str:
    """Normalize bytes / HexBytes / str to a lowercase 0x-prefixed string."""
    if value is None:
        return "0x"
    if isinstance(value, bytes | bytearray):
        return "0x" + bytes(value).hex()
    text = str(value).strip().lower()
    if not text or text == "none":
        return "0x"
    return text if text.startswith("0x") else "0x" + text


def normalize_address(value: Any) -> str:
    """Lowercase 0x-prefixed 20-byte address form used as the module's key."""
    return _norm_hex(value)


def normalize_tx_hash(value: Any) -> str:
    """Lowercase 0x-prefixed tx-hash form; ledger hashes vary in case."""
    return _norm_hex(value)


def _topic_to_address(topic: Any) -> str:
    """Extract the address from a 32-byte indexed topic."""
    return "0x" + _norm_hex(topic)[2:].rjust(64, "0")[-40:]


def pad_address_topic(address: str) -> str:
    """Left-pad an address into the 32-byte topic form used by eth_getLogs."""
    return "0x" + normalize_address(address)[2:].rjust(64, "0")


def _selector_of(calldata: Any) -> str:
    """First 4 bytes of ``calldata`` as ``0x``-prefixed lowercase hex, or ``""``.

    Unforgeable by construction: this is the transaction's own signed calldata,
    not something emitted contracts can influence.
    """
    text = _norm_hex(calldata)
    if len(text) < 10:
        return ""
    return text[:10]


def _to_int(value: Any) -> int:
    """Parse a log data field that may arrive as int, bytes or hex string."""
    if isinstance(value, int):
        return value
    if isinstance(value, bytes | bytearray):
        return int.from_bytes(bytes(value), "big") if value else 0
    text = str(value).strip()
    if not text or text in ("0x", "0X"):
        return 0
    return int(text, 16) if text.lower().startswith("0x") else int(text)


def _log_field(log: Any, key: str) -> Any:
    """Read a field from a log that may be a Mapping or an attribute object."""
    if isinstance(log, Mapping):
        return log.get(key)
    return getattr(log, key, None)


def to_rpc_address(address: str) -> str:
    """EIP-55 checksum an address for the RPC wire.

    The module keys everything by lowercase address, but web3.py's validation
    middleware rejects non-checksummed ``address`` params on ``eth_getLogs`` /
    ``eth_getCode`` unconditionally — a lowercased filter never reaches the
    node (found live in the VIB-5866 real-fork proof run; invisible to
    fake-provider unit tests). Checksum exactly at the call boundary.
    """
    from web3 import Web3  # checksum utility only; no provider is constructed

    return Web3.to_checksum_address(normalize_address(address))


def classify_counterparty_code(code: Any) -> CounterpartyKind:
    """Map ``eth_getCode`` output to a counterparty kind."""
    hex_code = _norm_hex(code) if code is not None else "0x"
    if hex_code in ("0x", "0x0"):
        return CounterpartyKind.EOA
    if hex_code.startswith(EIP7702_PREFIX):
        return CounterpartyKind.EOA
    return CounterpartyKind.CONTRACT


def classify_transfer(
    *,
    direction: TransferDirection,
    counterparty_kind: CounterpartyKind,
    is_ledger_tx: bool,
    wallet_initiated_tx: bool | None,
) -> FlowClassification:
    """Provenance verdict for a single transfer, in strict precedence order.

    ``wallet_initiated_tx`` is only consulted for outflows; ``None`` means the
    transaction's endpoints could not be determined.
    """
    # 1. Anything the strategy itself committed to the ledger — including the
    #    teardown lane and settlement txs — is never a capital movement.
    if is_ledger_tx:
        return FlowClassification.STRATEGY_TX

    unclassified = (
        FlowClassification.UNCLASSIFIED_IN if direction is TransferDirection.IN else FlowClassification.UNCLASSIFIED_OUT
    )

    # 2. address(0) has no code, so eth_getCode would call it an EOA and a
    #    push-airdrop mint would book as a DEPOSIT. Zero-address is decided
    #    structurally, before any code lookup.
    if counterparty_kind is CounterpartyKind.MINT_BURN:
        return unclassified

    if direction is TransferDirection.IN:
        # 4. Only an EOA-to-wallet push is provably external capital.
        return (
            FlowClassification.DEPOSIT
            if counterparty_kind is CounterpartyKind.EOA
            else FlowClassification.UNCLASSIFIED_IN
        )

    # 5. An outflow is a withdrawal only if the wallet itself initiated the tx.
    #    A transferFrom pull by an approved spender may be a sweep or a theft;
    #    booking it as a WITHDRAWAL would net a real loss out of PnL.
    #    See :func:`wallet_initiated` for what "initiated" means once the
    #    wallet is a smart account (VIB-6050).
    if wallet_initiated_tx and counterparty_kind is CounterpartyKind.EOA:
        return FlowClassification.WITHDRAWAL
    return FlowClassification.UNCLASSIFIED_OUT


def to_human_amount(raw_amount: int, decimals: int | None) -> Decimal | None:
    """Convert a raw token amount to human units, or ``None`` if unmeasurable.

    Decimals beyond 78 (uint256 digit bound) can only come from corrupt token
    metadata — treat as unmeasurable rather than risk pathological exponents.
    """
    if decimals is None or decimals < 0 or decimals > 78:
        return None
    return Decimal(raw_amount) / (Decimal(10) ** decimals)


# --------------------------------------------------------------------------
# Bounded caches
# --------------------------------------------------------------------------


class _BoundedCache:
    """Tiny LRU keyed by arbitrary hashables.

    Lock-guarded: the module-level instances are shared process-wide and the
    scan runs inside ``asyncio.to_thread`` workers, so concurrent multi-chain
    scans mutate these caches from different threads.
    """

    def __init__(self, maxsize: int) -> None:
        self._maxsize = maxsize
        self._data: OrderedDict[Any, Any] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: Any) -> Any:
        with self._lock:
            if key not in self._data:
                return None
            self._data.move_to_end(key)
            return self._data[key]

    def put(self, key: Any, value: Any) -> None:
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            while len(self._data) > self._maxsize:
                self._data.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


_CODE_CACHE = _BoundedCache(_CODE_CACHE_SIZE)
_TX_ENDPOINTS_CACHE = _BoundedCache(_TX_ENDPOINTS_CACHE_SIZE)
_TX_LOGS_CACHE = _BoundedCache(_TX_LOGS_CACHE_SIZE)


def clear_provenance_caches() -> None:
    """Drop the process-wide code / tx caches (tests, long-lived runs)."""
    _CODE_CACHE.clear()
    _TX_ENDPOINTS_CACHE.clear()
    _TX_LOGS_CACHE.clear()


# --------------------------------------------------------------------------
# IO
# --------------------------------------------------------------------------


def resolve_counterparty_kind(web3: Web3Like, chain: str, address: str) -> CounterpartyKind:
    """Resolve EOA vs CONTRACT via ``eth_getCode``, cached by (chain, address)."""
    address = normalize_address(address)
    if address == ZERO_ADDRESS:
        return CounterpartyKind.MINT_BURN

    cache_key = (chain, address)
    cached = _CODE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        code = web3.eth.get_code(to_rpc_address(address))
    except Exception as exc:  # noqa: BLE001 - provider errors are opaque
        logger.warning("eth_getCode failed for %s on %s: %s", address, chain, exc)
        return CounterpartyKind.UNKNOWN

    kind = classify_counterparty_code(code)
    _CODE_CACHE.put(cache_key, kind)
    return kind


def resolve_tx_endpoints(web3: Web3Like, chain: str, tx_hash: str) -> TxEndpoints | None:
    """Resolve a transaction's ``from`` / ``to``, cached by (chain, tx_hash).

    ``to`` is ``None`` for a contract-creation transaction — the only shape
    where an EVM transaction genuinely has no target.
    """
    tx_hash = normalize_tx_hash(tx_hash)
    cache_key = (chain, tx_hash)
    cached = _TX_ENDPOINTS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        tx = web3.eth.get_transaction(tx_hash)
    except Exception as exc:  # noqa: BLE001 - provider errors are opaque
        logger.warning("eth_getTransactionByHash failed for %s on %s: %s", tx_hash, chain, exc)
        return None

    sender = _log_field(tx, "from")
    if sender is None:
        return None
    raw_to = _log_field(tx, "to")
    endpoints = TxEndpoints(
        sender=normalize_address(sender),
        to=normalize_address(raw_to) if raw_to is not None else None,
        selector=_selector_of(_log_field(tx, "input")),
    )
    _TX_ENDPOINTS_CACHE.put(cache_key, endpoints)
    return endpoints


def resolve_tx_logs(web3: Web3Like, chain: str, tx_hash: str) -> list[Any] | None:
    """Logs of ``tx_hash``'s receipt, cached by (chain, tx_hash). ``None`` = unknown.

    Only fetched for the rare candidate-withdrawal path (a non-ledger outflow
    whose ``tx.to`` is the wallet), so this adds at most one
    ``eth_getTransactionReceipt`` per such transfer — the same order of cost as
    the ``eth_getTransactionByHash`` that path already pays.
    """
    tx_hash = normalize_tx_hash(tx_hash)
    cache_key = (chain, tx_hash)
    cached = _TX_LOGS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        receipt = web3.eth.get_transaction_receipt(tx_hash)
    except Exception as exc:  # noqa: BLE001 - provider errors are opaque
        logger.warning("eth_getTransactionReceipt failed for %s on %s: %s", tx_hash, chain, exc)
        return None

    logs = _log_field(receipt, "logs")
    if logs is None:
        return None
    resolved = list(logs)
    _TX_LOGS_CACHE.put(cache_key, resolved)
    return resolved


def wallet_initiated(
    endpoints: TxEndpoints | None,
    wallet: str,
    *,
    wallet_executed_as_safe: bool | None = None,
) -> bool | None:
    """Did ``wallet`` itself authorise this transaction? ``None`` = unknown.

    VIB-6050. The predicate this replaces was ``tx.from == wallet``, which is
    only correct while the wallet is a plain EOA. Under Safe + Zodiac — the
    ONLY hosted execution mode — the transaction is signed by the agent EOA (or
    a Safe owner) and the Safe is the *target*, never the sender. ``tx.from ==
    safe`` is therefore **permanently false** on the hosted platform, so a
    genuine operator withdrawal out of the Safe booked as ``UNCLASSIFIED_OUT``,
    which poisons the era and skews deposit/withdrawal-adjusted PnL.

    **Reachable surface, measured**: local-SDK Safe wallets. Hosted deployments
    are short-circuited to permanently-unmeasured *before* this reader runs
    (``runner_state`` poisons with ``REASON_HOSTED_OWNERSHIP_UNVERIFIED``,
    VIB-5917), so the ticket's "every hosted deployment" framing — which this
    docstring also carried in review — is wrong today. It becomes the hosted
    path's behaviour the moment that attestation gate lands.

    Two shapes count, and the second one requires proof:

    * ``tx.from == wallet`` — plain EOA execution. Unchanged.
    * ``tx.to == wallet`` **and** the transaction's own selector is
      ``execTransaction`` **and** the wallet emitted ``ExecutionSuccess``.

    **Why entry is not authorisation.** ``tx.to == wallet`` alone proves only
    that execution *entered* the account. An attacker EOA enabled as a Safe
    module — the canonical backdoor, installed with one phished
    ``execTransaction`` — can call ``execTransactionFromModule`` **directly on
    the Safe** with no owner signature anywhere. Booking that drain as a
    ``WITHDRAWAL`` would net a real theft out of PnL, precisely the failure
    rule 5 exists to prevent, and strictly worse than the bug being fixed.

    **Why the selector, and not just the event.** The first fix for the above
    required only the Safe's own ``ExecutionSuccess``. **That is forgeable, and
    it was measured rather than argued**: the same module can call
    ``execTransactionFromModule`` with ``operation = DELEGATECALL``, which runs
    attacker code *in the Safe's context*, so its ``LOG`` opcodes emit with the
    Safe's own address. A fork run produced a synthetic ``ExecutionSuccess``
    with ``log.address == safe`` from an attacker contract. Emitter-pinning does
    not save an event from delegatecall.

    ``execTransaction`` is the only Safe entry point that runs
    ``checkSignatures``, which reverts without an owner-threshold signature set.
    Its selector sits in the transaction's own signed calldata, where no
    executed contract — delegatecalled or otherwise — can reach it. So the
    selector is the **load-bearing** signal; the event is retained as secondary
    evidence that the execution actually succeeded (``execTransaction`` emits
    ``ExecutionFailure`` instead when the inner call reverts). **Never revert to
    the event alone.**

    This is **corroboration, not derivation**. VIB-6043 removed an
    event-signature-based step that *derived* the Safe address from a receipt,
    because any contract can emit a colliding ``topic0``. Here the wallet is
    known from configuration and used as the filter — but as the delegatecall
    result shows, "the emitter is the wallet" is a weaker statement than it
    looks, which is exactly why authorisation now rests on calldata.

    Residual, and deliberately accepted: an attacker-directed ``transferFrom``
    leg riding inside an otherwise owner-authorised transaction is still booked
    as a withdrawal. That hole is identical in the EOA arm (``tx.from ==
    wallet`` says nothing about the individual legs either) and is not
    introduced here; closing it needs leg-level calldata attribution, which is
    a separate piece of work.

    Shapes that fail CONSERVATIVELY (false negative — an unclassified outflow
    poisons the era, which is the safe direction):

    * **Nested Safes.** If ``wallet``'s owner is itself a Safe, a withdrawal
      entered through the *owner* Safe has ``tx.to == owner_safe``, not
      ``wallet``, so it does not classify. Note this also means a nested
      arrangement cannot be used to manufacture a false *positive*: reaching the
      ``tx.to == wallet`` arm at all still requires entering ``wallet`` through
      its own signature-validating entry point.
    * **ERC-4337.** A UserOp routes through the EntryPoint, so ``tx.to`` is the
      EntryPoint and ``Safe4337Module.executeUserOp`` (``0x7bb37428``) is a
      different selector besides. Bundler-executed withdrawals stay
      ``UNCLASSIFIED_OUT``.

    Selector stability: ``execTransaction``'s signature is byte-identical across
    Safe 1.1.1 / 1.3.0 / 1.4.1, so ``0x6a761202`` covers every version this SDK
    deploys against. Pinned by a test that re-derives it from the signature
    string rather than trusting the literal.

    Deliberately NOT included: a Zodiac Roles modifier (or any other module
    address) as ``tx.to``. Those are the *agent's* transactions, which the
    ledger already claims as ``STRATEGY_TX``; an unledgered one is a strategy
    tx we failed to record, not a capital withdrawal, and the conservative
    ``UNCLASSIFIED_OUT`` is the correct verdict for it. Widening the predicate
    to modules would silently convert missing ledger rows into fake
    withdrawals.

    Args:
        endpoints: the transaction's ``from`` / ``to``, or ``None`` if unknown.
        wallet: the deployment's execution address.
        wallet_executed_as_safe: whether ``wallet`` emitted ``ExecutionSuccess``
            in this transaction. Only consulted on the ``tx.to == wallet`` arm,
            and only after the selector check has already passed. ``None`` there
            means the receipt could not be read — unknown, not "no".

    Returns ``None`` whenever the evidence could not be gathered, preserving
    the module's Empty != Zero discipline: unknown is not "no".
    """
    if endpoints is None:
        return None
    wallet = normalize_address(wallet)
    # ``normalize_address`` returns the sentinel "0x" for an absent/malformed
    # value; matching against it would make every endpoint "the wallet".
    if len(wallet) != 42:
        return None
    if endpoints.sender == wallet:
        return True
    if endpoints.to != wallet:
        return False
    # Smart-account arm — entry alone is not authorisation.
    if endpoints.selector != SAFE_EXEC_TRANSACTION_SELECTOR:
        # A module / fallback-handler entry. Conservative by design: this is
        # also where a module-backdoor drain lands.
        return False
    return wallet_executed_as_safe


def resolve_wallet_initiated(web3: Web3Like, chain: str, tx_hash: str, wallet: str) -> bool | None:
    """Full provenance answer for one transaction: did ``wallet`` authorise it?

    The single entry point callers use. Gathers exactly the evidence each arm
    of :func:`wallet_initiated` needs and no more: the receipt is fetched
    **only** for the smart-account arm, so the plain-EOA lane keeps its
    pre-VIB-6050 RPC cost of one ``eth_getTransactionByHash``.
    """
    endpoints = resolve_tx_endpoints(web3, chain, tx_hash)
    if endpoints is None:
        return None
    executed = (
        wallet_executed_as_safe(web3, chain, tx_hash, wallet) if _is_smart_account_arm(endpoints, wallet) else None
    )
    return wallet_initiated(endpoints, wallet, wallet_executed_as_safe=executed)


def _is_smart_account_arm(endpoints: TxEndpoints, wallet: str) -> bool:
    """True when the ``tx.to == wallet`` arm applies and needs corroborating."""
    wallet = normalize_address(wallet)
    return (
        len(wallet) == 42
        and endpoints.sender != wallet
        and endpoints.to == wallet
        # Selector first: it is free (the tx is already fetched) and it rejects
        # every module / fallback entry, so the receipt fetch below only ever
        # happens for a genuinely owner-signed Safe transaction.
        and endpoints.selector == SAFE_EXEC_TRANSACTION_SELECTOR
    )


def wallet_executed_as_safe(web3: Web3Like, chain: str, tx_hash: str, wallet: str) -> bool | None:
    """Did ``wallet`` emit a Safe ``ExecutionSuccess`` in ``tx_hash``?

    ``None`` when the receipt could not be read (unknown, not "no").

    **SECONDARY evidence only — never the authorisation decision.** This answers
    "did the Safe transaction succeed", not "did the owners authorise it": a
    module can emit this event from the Safe's own address via a DELEGATECALL,
    which was measured on a fork. Authorisation rests on the ``execTransaction``
    selector, which :func:`wallet_initiated` checks FIRST — so this function is
    only ever reached for a transaction that already entered through the
    signature-validating path. See :func:`wallet_initiated` for the full
    reasoning.
    """
    wallet = normalize_address(wallet)
    if len(wallet) != 42:
        return None
    logs = resolve_tx_logs(web3, chain, tx_hash)
    if logs is None:
        return None
    for log in logs:
        topics = _log_field(log, "topics") or []
        if not topics or _norm_hex(topics[0]) != SAFE_EXECUTION_SUCCESS_TOPIC:
            continue
        if normalize_address(_log_field(log, "address")) == wallet:
            return True
    return False


@dataclass(frozen=True)
class _RawTransfer:
    """A parsed Transfer log, before provenance resolution."""

    token_address: str
    from_address: str
    to_address: str
    raw_amount: int
    tx_hash: str
    block_number: int
    log_index: int


def _parse_transfer_log(log: Any) -> _RawTransfer | None:
    """Parse one Transfer log, or ``None`` when it is not a plain ERC-20 one."""
    topics = _log_field(log, "topics") or []
    # ERC-721 shares the Transfer signature but indexes the tokenId as a fourth
    # topic. Only the 3-topic ERC-20 shape carries a fungible value in data.
    if len(topics) != 3:
        return None
    try:
        return _RawTransfer(
            token_address=normalize_address(_log_field(log, "address")),
            from_address=_topic_to_address(topics[1]),
            to_address=_topic_to_address(topics[2]),
            raw_amount=_to_int(_log_field(log, "data")),
            tx_hash=normalize_tx_hash(_log_field(log, "transactionHash")),
            block_number=int(_log_field(log, "blockNumber")),
            log_index=int(_log_field(log, "logIndex")),
        )
    except (TypeError, ValueError) as exc:
        logger.warning("Skipping malformed Transfer log: %s", exc)
        return None


def _build_observation(
    raw: _RawTransfer,
    *,
    web3: Web3Like,
    chain: str,
    wallet: str,
    token_universe: Mapping[str, TokenInfo],
    ledger_tx_hashes: frozenset[str],
) -> TransferObservation:
    """Resolve provenance for one parsed transfer and build the observation."""
    direction = TransferDirection.IN if raw.to_address == wallet else TransferDirection.OUT
    counterparty = raw.from_address if direction is TransferDirection.IN else raw.to_address
    is_ledger_tx = raw.tx_hash in ledger_tx_hashes

    if counterparty == ZERO_ADDRESS:
        kind = CounterpartyKind.MINT_BURN
    elif is_ledger_tx:
        # Precedence rule 1 already decides the classification; skip the RPC.
        kind = CounterpartyKind.UNKNOWN
    else:
        kind = resolve_counterparty_kind(web3, chain, counterparty)

    wallet_initiated_tx: bool | None = None
    if not is_ledger_tx and direction is TransferDirection.OUT and kind is not CounterpartyKind.MINT_BURN:
        wallet_initiated_tx = resolve_wallet_initiated(web3, chain, raw.tx_hash, wallet)

    token = token_universe.get(raw.token_address, TokenInfo())
    amount = to_human_amount(raw.raw_amount, token.decimals)

    return TransferObservation(
        chain=chain,
        token_address=raw.token_address,
        symbol=token.symbol,
        amount=amount,
        raw_amount=raw.raw_amount,
        direction=direction,
        counterparty=counterparty,
        counterparty_kind=kind,
        tx_hash=raw.tx_hash,
        block_number=raw.block_number,
        log_index=raw.log_index,
        classification=classify_transfer(
            direction=direction,
            counterparty_kind=kind,
            is_ledger_tx=is_ledger_tx,
            wallet_initiated_tx=wallet_initiated_tx,
        ),
        measurable=amount is not None,
    )


def _fetch_chunk_logs(
    web3: Web3Like,
    *,
    wallet: str,
    token_addresses: list[str],
    from_block: int,
    to_block: int,
) -> list[Any]:
    """Two eth_getLogs calls (inbound + outbound) for one block chunk."""
    wallet_topic = pad_address_topic(wallet)
    base = {
        "fromBlock": from_block,
        "toBlock": to_block,
        # Checksummed at the wire: web3.py's middleware rejects lowercase
        # address filters outright (VIB-5866 real-fork finding).
        "address": [to_rpc_address(addr) for addr in token_addresses],
    }
    inflows = web3.eth.get_logs({**base, "topics": [TRANSFER_SIG, None, wallet_topic]})
    outflows = web3.eth.get_logs({**base, "topics": [TRANSFER_SIG, wallet_topic, None]})
    return list(inflows) + list(outflows)


def scan_chain_transfers(
    web3: Web3Like,
    *,
    chain: str,
    wallet: str,
    from_block_exclusive: int,
    head_block: int,
    token_universe: Mapping[str, TokenInfo],
    ledger_tx_hashes: Iterable[str] = (),
) -> ChainScanResult:
    """Scan ``(from_block_exclusive, min(head, from+MAX_BLOCKS_PER_CYCLE)]``.

    Returns every ERC-20 transfer in the token universe that touches ``wallet``,
    classified. Self-transfers are dropped and a transfer seen by both topic
    filters is emitted once.
    """
    chain = chain.lower()
    wallet = normalize_address(wallet)
    universe = {normalize_address(addr): info for addr, info in token_universe.items()}
    ledger = frozenset(normalize_tx_hash(h) for h in ledger_tx_hashes)

    if head_block - from_block_exclusive > MAX_BACKLOG_BLOCKS:
        # Too far behind to reconstruct: the caller advances the cursor and
        # poisons the interval to unmeasured rather than inventing flows.
        return ChainScanResult(
            chain=chain,
            from_block=from_block_exclusive,
            to_block=head_block,
            status=ScanStatus.RANGE_UNMEASURABLE,
            error=f"backlog {head_block - from_block_exclusive} blocks exceeds {MAX_BACKLOG_BLOCKS}",
        )

    to_block = min(head_block, from_block_exclusive + MAX_BLOCKS_PER_CYCLE)
    if to_block <= from_block_exclusive or not universe:
        return ChainScanResult(chain=chain, from_block=from_block_exclusive, to_block=to_block)

    token_addresses = sorted(universe)
    collected: dict[tuple[str, int], _RawTransfer] = {}
    cursor = from_block_exclusive + 1
    chunk = CHUNK_BLOCKS

    while cursor <= to_block:
        chunk_end = min(cursor + chunk - 1, to_block)
        try:
            logs = _fetch_chunk_logs(
                web3,
                wallet=wallet,
                token_addresses=token_addresses,
                from_block=cursor,
                to_block=chunk_end,
            )
        except Exception as exc:  # noqa: BLE001 - provider range errors are opaque
            chunk //= 2
            if chunk < MIN_CHUNK_BLOCKS:
                return _finalize(
                    raws=collected,
                    web3=web3,
                    chain=chain,
                    wallet=wallet,
                    universe=universe,
                    ledger=ledger,
                    from_block=from_block_exclusive,
                    to_block=cursor - 1,
                    status=ScanStatus.TRANSIENT_FAILURE,
                    error=str(exc),
                )
            logger.info(
                "eth_getLogs failed on %s [%d,%d]; retrying with chunk=%d: %s", chain, cursor, chunk_end, chunk, exc
            )
            continue

        for log in logs:
            raw = _parse_transfer_log(log)
            if raw is None or raw.from_address == raw.to_address:
                continue
            collected[(raw.tx_hash, raw.log_index)] = raw
        cursor = chunk_end + 1

    return _finalize(
        raws=collected,
        web3=web3,
        chain=chain,
        wallet=wallet,
        universe=universe,
        ledger=ledger,
        from_block=from_block_exclusive,
        to_block=to_block,
        status=ScanStatus.OK,
        error=None,
    )


def _finalize(
    *,
    raws: Mapping[tuple[str, int], _RawTransfer],
    web3: Web3Like,
    chain: str,
    wallet: str,
    universe: Mapping[str, TokenInfo],
    ledger: frozenset[str],
    from_block: int,
    to_block: int,
    status: ScanStatus,
    error: str | None,
) -> ChainScanResult:
    """Resolve provenance for the collected transfers and build the result."""
    ordered: Sequence[_RawTransfer] = sorted(raws.values(), key=lambda r: (r.block_number, r.log_index, r.tx_hash))
    observations = tuple(
        _build_observation(
            raw,
            web3=web3,
            chain=chain,
            wallet=wallet,
            token_universe=universe,
            ledger_tx_hashes=ledger,
        )
        for raw in ordered
    )
    return ChainScanResult(
        chain=chain,
        from_block=from_block,
        to_block=to_block,
        observations=observations,
        status=status,
        error=error,
    )
