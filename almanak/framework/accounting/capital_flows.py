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
  ``eth_getLogs`` / ``eth_getCode`` / ``eth_getTransactionByHash`` are used and
  all three are on the gateway allowlist. Unit tests inject a fake handle — no
  sockets.
- **Empty is not zero.** A transfer on a token whose decimals we do not know
  yields ``amount=None`` and ``measurable=False``; it is never coerced to
  ``Decimal("0")``. The caller must poison the affected interval rather than
  book a silent zero.
- **Token universe, decimals and prices are inputs**, not something this module
  discovers. Discovery lives with the caller (PR-B), which already owns the
  token registry.
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
_TX_SENDER_CACHE_SIZE = 256


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
    tx_sender_is_wallet: bool | None,
) -> FlowClassification:
    """Provenance verdict for a single transfer, in strict precedence order.

    ``tx_sender_is_wallet`` is only consulted for outflows; ``None`` means the
    sender could not be determined.
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

    # 5. An outflow is a withdrawal only if the wallet itself sent the tx.
    #    A transferFrom pull by an approved spender (tx.from != wallet) may be
    #    a sweep or a theft; booking it as a WITHDRAWAL would net a real loss
    #    out of PnL.
    if tx_sender_is_wallet and counterparty_kind is CounterpartyKind.EOA:
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
_TX_SENDER_CACHE = _BoundedCache(_TX_SENDER_CACHE_SIZE)


def clear_provenance_caches() -> None:
    """Drop the process-wide code / tx-sender caches (tests, long-lived runs)."""
    _CODE_CACHE.clear()
    _TX_SENDER_CACHE.clear()


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


def resolve_tx_sender(web3: Web3Like, chain: str, tx_hash: str) -> str | None:
    """Resolve the ``from`` of a transaction, cached by (chain, tx_hash)."""
    tx_hash = normalize_tx_hash(tx_hash)
    cache_key = (chain, tx_hash)
    cached = _TX_SENDER_CACHE.get(cache_key)
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
    normalized = normalize_address(sender)
    _TX_SENDER_CACHE.put(cache_key, normalized)
    return normalized


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

    tx_sender_is_wallet: bool | None = None
    if not is_ledger_tx and direction is TransferDirection.OUT and kind is not CounterpartyKind.MINT_BURN:
        sender = resolve_tx_sender(web3, chain, raw.tx_hash)
        tx_sender_is_wallet = None if sender is None else sender == wallet

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
            tx_sender_is_wallet=tx_sender_is_wallet,
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
