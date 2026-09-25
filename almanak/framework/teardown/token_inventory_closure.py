"""Transaction-bound ERC-20 inventory closure, excluding proven pre-existing wallet balance.

``balanceOf(wallet)`` is a whole-account read: a wallet that already held the
token before the strategy traded it keeps that balance after a correct exit, and
the TOKEN post-condition cannot tell the two apart. This proof attributes the
exit instead of reading the account: the exact tracked quantity left the wallet
in the teardown's own transactions and nothing else moved the balance.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
from almanak.connectors._strategy_base.vault_post_condition import _is_evm_address
from almanak.framework.teardown.native_inventory_closure import (
    _recheck_blocks,
    field,
    native_input,
    position_key,
    quantity,
    rpc,
)
from almanak.framework.teardown.swap_clamp import decide_swap_clamp
from almanak.framework.teardown.token_post_condition import _resolve_token_address


def _resolve(token: Any, chain: str) -> tuple[str, int] | None:
    """Resolve a token reference to (lowercase address, decimals), or None when unresolved."""
    if not isinstance(token, str) or not token.strip() or native_input(token, chain):
        return None
    try:
        from almanak.framework.data.tokens import get_token_resolver

        resolved = get_token_resolver().resolve(token.strip(), chain)
    except Exception:  # noqa: BLE001 — an unresolved identity is never closure proof
        return None
    address = str(getattr(resolved, "address", "") or "")
    decimals = getattr(resolved, "decimals", None)
    if not _is_evm_address(address) or isinstance(decimals, bool) or not isinstance(decimals, int) or decimals < 0:
        return None
    return address.lower(), decimals


def held_token(position: Any) -> str | None:
    """The ERC-20 address a TOKEN position declares, or None for any other position."""
    if str(field(position, "position_type", "")).upper() not in {"TOKEN", "POSITIONTYPE.TOKEN"}:
        return None
    chain = str(field(position, "chain", "") or "")
    address = _resolve_token_address(field(position, "details", {}) or {}, field(position, "position_id", ""), chain)
    if not _is_evm_address(address) or native_input(address, chain):
        return None
    return address.lower()


def tracked_token_raw(tracked: Any, from_token: str, chain: str, decimals: int, *, allow_absent: bool = False) -> int:
    decision = decide_swap_clamp(from_token=from_token, tracked_map=tracked, live_balance=Decimal(10**60), chain=chain)
    if allow_absent and isinstance(tracked, dict) and decision.reason == "untracked_token":
        return 0
    if decision.degraded or decision.reason == "untracked_token":
        raise ValueError("ERC-20 tracked inventory unmeasured")
    amount = decision.amount if not decision.skip else Decimal(0)
    if amount is None:
        raise ValueError("ERC-20 tracked inventory amount unmeasured")
    raw = amount * Decimal(10**decimals)
    if raw != raw.to_integral_value() or raw < 0:
        raise ValueError("Invalid ERC-20 inventory precision")
    return int(raw)


class TokenClosureRejected(ValueError):
    """The chain contradicts the proof: the proven token's inventory or balance moved after the exit.

    Distinct from an unmeasured read, which stays a plain ``ValueError``: only a
    contradiction is evidence that the whole-account balance should be re-measured.
    ``block`` is the latest block the rejection observed; a re-measurement pins to
    it so a read replica trailing the writer cannot serve the pre-exit balance.
    ``terminal_balance`` is the wallet's proven post-exit holding of the token,
    which belongs to the wallet, not to the closed position.
    """

    def __init__(self, message: str, *, block: int, terminal_balance: int) -> None:
        super().__init__(message)
        self.block = block
        self.terminal_balance = terminal_balance


def _erc20_balance(gateway: Any, chain: str, token: str, wallet: str, block: int) -> int:
    return quantity(gateway.query_erc20_balance(chain=chain, token_address=token, wallet_address=wallet, block=block))


@dataclass(frozen=True)
class TokenExitAnchor:
    deployment_id: str
    key: tuple[str, str, str]
    wallet: str
    token: str
    from_token: str
    decimals: int
    amount: int
    block_number: int
    block_hash: str
    balance: int
    nonce: int


@dataclass(frozen=True)
class TokenClosureProof:
    anchor: TokenExitAnchor
    terminal_block: int
    terminal_hash: str
    terminal_balance: int
    transaction_hashes: tuple[str, ...]
    receipt_blocks: tuple[tuple[int, str], ...]

    def verify(
        self, position: Any, wallet: str, gateway: Any, *, deployment_id: str, tracked: Any
    ) -> ClosureCheckResult:
        if (
            deployment_id != self.anchor.deployment_id
            or held_token(position) != self.anchor.token
            or position_key(position) != self.anchor.key
            or wallet.lower() != self.anchor.wallet
        ):
            raise ValueError("ERC-20 proof identity mismatch")
        chain = self.anchor.key[1]
        latest = rpc(gateway, chain, "eth_getBlockByNumber", ["latest", False])
        latest_number = quantity(latest["number"])
        if latest_number < self.terminal_block:
            raise ValueError("ERC-20 closure final chain read trails the receipt")
        if tracked_token_raw(tracked, self.anchor.from_token, chain, self.anchor.decimals, allow_absent=True) != 0:
            raise TokenClosureRejected(
                "ERC-20 inventory was reacquired", block=latest_number, terminal_balance=self.terminal_balance
            )
        # Later wallet transactions (the next teardown exit, a consolidation swap)
        # do not invalidate the proof: unlike native inventory, an ERC-20 balance
        # is not spent by gas, so an unchanged balance at the latest block is the
        # direct evidence that nothing moved this token after the exit.
        nonce = quantity(rpc(gateway, chain, "eth_getTransactionCount", [wallet, hex(latest_number)]))
        if nonce < self.anchor.nonce + len(self.transaction_hashes):
            raise ValueError("ERC-20 closure nonce trails the exit's own transactions")
        if _erc20_balance(gateway, chain, self.anchor.token, wallet, latest_number) != self.terminal_balance:
            raise TokenClosureRejected(
                "ERC-20 closure invalidated by a later balance change",
                block=latest_number,
                terminal_balance=self.terminal_balance,
            )
        balance = _erc20_balance(gateway, chain, self.anchor.token, wallet, self.terminal_block)
        if balance != self.terminal_balance or balance + self.anchor.amount != self.anchor.balance:
            raise ValueError("ERC-20 closure balance conservation failed")
        _recheck_blocks(
            gateway,
            chain,
            [
                (self.anchor.block_number, self.anchor.block_hash),
                *self.receipt_blocks,
                (latest_number, latest["hash"].lower()),
            ],
        )
        return ClosureCheckResult(closed=True, protocol=self.anchor.key[0], position_id=self.anchor.key[2])


def capture_token_exit(
    *, strategy: Any, intent: Any, positions: list, tracked: Any, gateway: Any
) -> TokenExitAnchor | None:
    """Anchor an ERC-20 exit of one TOKEN position, or None when the intent sells no such position."""
    chain = field(intent, "chain") or strategy.chain
    from_token = field(intent, "from_token", "")
    resolved = _resolve(from_token, chain)
    if resolved is None:
        return None
    token, decimals = resolved
    candidates = [p for p in positions if field(p, "chain") == chain and held_token(p) == token]
    if not candidates:
        return None
    if len(candidates) != 1:
        raise ValueError("ERC-20 exit requires one unambiguous tracked position")
    amount = tracked_token_raw(tracked, from_token, chain, decimals)
    requested = Decimal(str(field(intent, "amount"))) * Decimal(10**decimals)
    if amount <= 0 or requested != amount:
        raise ValueError("ERC-20 exit must equal complete measured tracked inventory")
    getter = getattr(strategy, "get_wallet_for_chain", None)
    wallet = (getter(chain) if callable(getter) else strategy.wallet_address).lower()
    block = rpc(gateway, chain, "eth_getBlockByNumber", ["latest", False])
    number = quantity(block["number"])
    if rpc(gateway, chain, "eth_getCode", [wallet, hex(number)]) != "0x":
        raise ValueError("ERC-20 closure supports an EOA sender without delegation only")
    balance = _erc20_balance(gateway, chain, token, wallet, number)
    if balance < amount:
        raise ValueError("ERC-20 exit exceeds the measured wallet balance")
    nonce = quantity(rpc(gateway, chain, "eth_getTransactionCount", [wallet, hex(number)]))
    return TokenExitAnchor(
        strategy.deployment_id,
        position_key(candidates[0]),
        wallet,
        token,
        from_token,
        decimals,
        amount,
        number,
        block["hash"].lower(),
        balance,
        nonce,
    )


def _verified_exit_transaction(anchor: TokenExitAnchor, gateway: Any, chain: str, tx_hash: str) -> tuple[int, int, str]:
    """(nonce, block, canonical block hash) of one exit transaction sent by the anchored wallet."""
    tx = rpc(gateway, chain, "eth_getTransactionByHash", [tx_hash])
    receipt = rpc(gateway, chain, "eth_getTransactionReceipt", [tx_hash])
    if tx["hash"].lower() != tx_hash.lower() or receipt["transactionHash"].lower() != tx_hash.lower():
        raise ValueError("ERC-20 exit receipt hash mismatch")
    if (
        tx["from"].lower() != anchor.wallet
        or receipt["from"].lower() != anchor.wallet
        or quantity(receipt["status"]) != 1
    ):
        raise ValueError("ERC-20 exit sender/status mismatch")
    block = quantity(receipt["blockNumber"])
    block_hash = receipt["blockHash"].lower()
    if block <= anchor.block_number or quantity(tx["blockNumber"]) != block or tx["blockHash"].lower() != block_hash:
        raise ValueError("ERC-20 exit block mismatch")
    if rpc(gateway, chain, "eth_getBlockByNumber", [hex(block), False])["hash"].lower() != block_hash:
        raise ValueError("ERC-20 exit receipt reorg")
    return quantity(tx["nonce"]), block, block_hash


def complete_token_exit(anchor: TokenExitAnchor, result: Any, tracked: Any, gateway: Any) -> TokenClosureProof:
    chain = anchor.key[1]
    if not field(result, "success"):
        raise ValueError("ERC-20 exit execution did not succeed")
    try:
        remaining = tracked_token_raw(tracked, anchor.from_token, chain, anchor.decimals, allow_absent=True)
    except ValueError as exc:
        raise ValueError("ERC-20 exit post-commit accounting inventory is unmeasured") from exc
    if remaining != 0:
        raise ValueError(f"ERC-20 exit post-commit accounting inventory is not closed: remaining_raw={remaining}")
    amounts = field(result, "swap_amounts") or (field(result, "extracted_data", {}) or {}).get("swap_amounts")
    input_token = _resolve(field(amounts, "token_in"), chain)
    if (
        field(amounts, "amount_in_decimal_resolved") is not True
        or input_token is None
        or input_token[0] != anchor.token
    ):
        raise ValueError("ERC-20 input receipt identity is unmeasured")
    if quantity(field(amounts, "amount_in")) != anchor.amount:
        raise ValueError("Measured ERC-20 fill differs from tracked inventory")
    hashes = [field(tx, "tx_hash") for tx in field(result, "transaction_results", [])]
    if not hashes or any(not isinstance(h, str) for h in hashes) or len(set(hashes)) != len(hashes):
        raise ValueError("ERC-20 exit transaction set missing or duplicated")
    nonces: list[int] = []
    blocks: dict[int, str] = {}
    for tx_hash in hashes:
        nonce, block, block_hash = _verified_exit_transaction(anchor, gateway, chain, tx_hash)
        nonces.append(nonce)
        if block in blocks and blocks[block] != block_hash:
            raise ValueError("ERC-20 exit receipts disagree on canonical block")
        blocks[block] = block_hash
    terminal = max(blocks)
    end_nonce = quantity(rpc(gateway, chain, "eth_getTransactionCount", [anchor.wallet, hex(terminal)]))
    if end_nonce - anchor.nonce != len(hashes) or sorted(nonces) != list(
        range(anchor.nonce, anchor.nonce + len(hashes))
    ):
        raise ValueError("ERC-20 exit nonce coverage mismatch")
    balance = _erc20_balance(gateway, chain, anchor.token, anchor.wallet, terminal)
    if balance + anchor.amount != anchor.balance:
        raise ValueError("ERC-20 exit balance conservation failed")
    _recheck_blocks(gateway, chain, [(anchor.block_number, anchor.block_hash), *blocks.items()])
    return TokenClosureProof(anchor, terminal, blocks[terminal], balance, tuple(hashes), tuple(sorted(blocks.items())))
