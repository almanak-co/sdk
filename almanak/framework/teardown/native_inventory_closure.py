"""Transaction-bound native inventory closure, excluding proven pre-existing gas funds."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult
from almanak.core.chains import ChainRegistry
from almanak.core.chains._helpers import native_symbols_for
from almanak.framework.data.tokens import NATIVE_SENTINEL
from almanak.framework.execution.receipt_costs import measured_gas_cost_wei, receipt_l1_fee_wei
from almanak.framework.teardown.swap_clamp import decide_swap_clamp
from almanak.framework.teardown.token_post_condition import _resolve_token_address
from almanak.gateway.proto import gateway_pb2


def field(value: Any, name: str, default: Any = None) -> Any:
    return value.get(name, default) if isinstance(value, dict) else getattr(value, name, default)


def quantity(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int | str):
        raise ValueError("Unmeasured integer quantity")
    result = int(value, 16 if value.startswith("0x") else 10) if isinstance(value, str) else value
    if result < 0:
        raise ValueError("Negative quantity")
    return result


def rpc(gateway: Any, chain: str, method: str, params: list) -> Any:
    response = gateway.rpc.Call(
        gateway_pb2.RpcRequest(chain=chain, method=method, params=json.dumps(params)), timeout=30
    )
    if not response.success or not response.result:
        raise ValueError(f"Native closure {method} unmeasured")
    result = json.loads(response.result)
    if result is None:
        raise ValueError(f"Native closure {method} returned null")
    return result


def position_key(position: Any) -> tuple[str, str, str]:
    return (
        str(field(position, "protocol", "")).lower(),
        str(field(position, "chain", "")).lower(),
        str(field(position, "position_id", "")).lower(),
    )


def is_native_token(position: Any) -> bool:
    if str(field(position, "position_type", "")).upper() not in {"TOKEN", "POSITIONTYPE.TOKEN"}:
        return False
    chain = field(position, "chain", "")
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None:
        return False
    address = _resolve_token_address(field(position, "details", {}) or {}, field(position, "position_id", ""), chain)
    return address.lower() in {
        NATIVE_SENTINEL.lower(),
        "0x" + "0" * 40,
        *(a.lower() for a in descriptor.native.address_aliases),
    }


def native_input(token: Any, chain: str) -> bool:
    if not isinstance(token, str):
        return False
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None:
        return False
    addresses = {NATIVE_SENTINEL.lower(), "0x" + "0" * 40, *(a.lower() for a in descriptor.native.address_aliases)}
    return token.upper() in native_symbols_for(chain) or token.lower() in addresses


def tracked_raw(tracked: Any, chain: str, *, allow_absent: bool = False) -> int:
    descriptor = ChainRegistry.resolve(chain)
    decision = decide_swap_clamp(
        from_token=descriptor.native.symbol, tracked_map=tracked, live_balance=Decimal(10**60), chain=chain
    )
    if allow_absent and isinstance(tracked, dict) and decision.reason == "untracked_token":
        return 0
    if decision.degraded or decision.reason == "untracked_token":
        raise ValueError("Native tracked inventory unmeasured")
    amount = decision.amount if not decision.skip else Decimal(0)
    if amount is None:
        raise ValueError("Native tracked inventory amount unmeasured")
    raw = amount * Decimal(10**descriptor.native.decimals)
    if raw != raw.to_integral_value() or raw < 0:
        raise ValueError("Invalid native inventory precision")
    return int(raw)


@dataclass(frozen=True)
class NativeExitAnchor:
    deployment_id: str
    key: tuple[str, str, str]
    wallet: str
    amount: int
    block_number: int
    block_hash: str
    balance: int
    nonce: int
    managed_fork: bool = False


@dataclass(frozen=True)
class NativeClosureProof:
    anchor: NativeExitAnchor
    terminal_block: int
    terminal_hash: str
    terminal_balance: int
    transaction_hashes: tuple[str, ...]
    gas_paid: int
    receipt_blocks: tuple[tuple[int, str], ...]

    def verify(
        self, position: Any, wallet: str, gateway: Any, *, deployment_id: str, tracked: Any
    ) -> ClosureCheckResult:
        if (
            deployment_id != self.anchor.deployment_id
            or not is_native_token(position)
            or position_key(position) != self.anchor.key
            or wallet.lower() != self.anchor.wallet
        ):
            raise ValueError("Native proof identity mismatch")
        chain = self.anchor.key[1]
        if tracked_raw(tracked, chain, allow_absent=True) != 0:
            raise ValueError("Native inventory was reacquired or is unmeasured")
        latest = rpc(gateway, chain, "eth_getBlockByNumber", ["latest", False])
        latest_number = quantity(latest["number"])
        if latest_number < self.terminal_block:
            raise ValueError("Native closure final chain read trails the receipt")
        nonce = quantity(rpc(gateway, chain, "eth_getTransactionCount", [wallet, hex(latest_number)]))
        if nonce != self.anchor.nonce + len(self.transaction_hashes):
            raise ValueError("Native closure invalidated by later wallet transactions")
        fresh_balance = quantity(gateway.query_native_balance(chain=chain, wallet_address=wallet, block=latest_number))
        if fresh_balance != self.terminal_balance:
            raise ValueError("Native closure invalidated by a later balance change")
        balance = quantity(gateway.query_native_balance(chain=chain, wallet_address=wallet, block=self.terminal_block))
        if balance != self.terminal_balance or balance + self.anchor.amount + self.gas_paid != self.anchor.balance:
            raise ValueError("Native closure balance conservation failed")
        for number, expected_hash in [
            (self.anchor.block_number, self.anchor.block_hash),
            *self.receipt_blocks,
            (latest_number, latest["hash"].lower()),
        ]:
            if rpc(gateway, chain, "eth_getBlockByNumber", [hex(number), False])["hash"].lower() != expected_hash:
                raise ValueError("Native closure anchor reorg")
        return ClosureCheckResult(closed=True, protocol=self.anchor.key[0], position_id=self.anchor.key[2])


def capture_native_exit(
    *, strategy: Any, intent: Any, positions: list, tracked: Any, gateway: Any
) -> NativeExitAnchor | None:
    chain = field(intent, "chain") or strategy.chain
    if not native_input(field(intent, "from_token", ""), chain):
        return None
    candidates = [p for p in positions if field(p, "chain") == chain and is_native_token(p)]
    if len(candidates) != 1:
        raise ValueError("Native exit requires one unambiguous tracked position")
    amount = tracked_raw(tracked, chain)
    decimals = ChainRegistry.resolve(chain).native.decimals
    requested = Decimal(str(field(intent, "amount"))) * Decimal(10**decimals)
    if amount <= 0 or requested != amount:
        raise ValueError("Native exit must equal complete measured tracked inventory")
    getter = getattr(strategy, "get_wallet_for_chain", None)
    wallet = (getter(chain) if callable(getter) else strategy.wallet_address).lower()
    block = rpc(gateway, chain, "eth_getBlockByNumber", ["latest", False])
    number = quantity(block["number"])
    if rpc(gateway, chain, "eth_getCode", [wallet, hex(number)]) != "0x":
        raise ValueError("Native closure supports an EOA sender without delegation only")
    balance = quantity(gateway.query_native_balance(chain=chain, wallet_address=wallet, block=number))
    if balance <= amount:
        raise ValueError("Native exit has no measured gas reserve")
    nonce = quantity(rpc(gateway, chain, "eth_getTransactionCount", [wallet, hex(number)]))
    from almanak.framework.execution.fork_signal import gateway_confirms_managed_fork

    managed_fork = gateway_confirms_managed_fork(
        gateway, chain, declared_network=getattr(strategy, "_gateway_network", None)
    )
    return NativeExitAnchor(
        strategy.deployment_id,
        position_key(candidates[0]),
        wallet,
        amount,
        number,
        block["hash"].lower(),
        balance,
        nonce,
        managed_fork,
    )


def _recheck_blocks(gateway: Any, chain: str, blocks: list[tuple[int, str]]) -> None:
    for number, expected_hash in blocks:
        if rpc(gateway, chain, "eth_getBlockByNumber", [hex(number), False])["hash"].lower() != expected_hash:
            raise ValueError("Native exit anchor or receipt reorg during final state reads")


def _require_closed_native_inventory(result: Any, tracked: Any, chain: str) -> None:
    if not field(result, "success"):
        raise ValueError("Native exit execution did not succeed")
    try:
        remaining = tracked_raw(tracked, chain, allow_absent=True)
    except ValueError as exc:
        raise ValueError("Native exit post-commit accounting inventory is unmeasured") from exc
    if remaining != 0:
        raise ValueError(f"Native exit post-commit accounting inventory is not closed: remaining_raw={remaining}")


def _native_exit_receipt_gas_cost(anchor: NativeExitAnchor, receipt: dict[str, Any], chain: str) -> int:
    l1 = receipt_l1_fee_wei(receipt)
    if (
        ChainRegistry.resolve(chain).gas.l1_fee_oracle_kind == "op_gaspriceoracle"
        and l1 is None
        and not anchor.managed_fork
    ):
        raise ValueError("Native exit L1 fee unmeasured")
    if any(
        quantity(receipt[k]) != 0
        for k in ("operatorFee", "operatorFeeScalar", "operatorFeeConstant")
        if receipt.get(k) is not None
    ):
        raise ValueError("Native exit additional operator fee unsupported")
    return measured_gas_cost_wei(quantity(receipt["gasUsed"]), quantity(receipt["effectiveGasPrice"]), l1)


def complete_native_exit(anchor: NativeExitAnchor, result: Any, tracked: Any, gateway: Any) -> NativeClosureProof:
    chain = anchor.key[1]
    _require_closed_native_inventory(result, tracked, chain)
    amounts = field(result, "swap_amounts") or (field(result, "extracted_data", {}) or {}).get("swap_amounts")
    if field(amounts, "amount_in_decimal_resolved") is not True or not native_input(field(amounts, "token_in"), chain):
        raise ValueError("Native input receipt identity is unmeasured")
    if quantity(field(amounts, "amount_in")) != anchor.amount:
        raise ValueError("Measured native fill differs from tracked inventory")
    hashes = [field(tx, "tx_hash") for tx in field(result, "transaction_results", [])]
    if not hashes or any(not isinstance(h, str) for h in hashes) or len(set(hashes)) != len(hashes):
        raise ValueError("Native exit transaction set missing or duplicated")
    nonces: list[int] = []
    blocks: dict[int, str] = {}
    gas, sent = 0, 0
    for tx_hash in hashes:
        tx = rpc(gateway, chain, "eth_getTransactionByHash", [tx_hash])
        receipt = rpc(gateway, chain, "eth_getTransactionReceipt", [tx_hash])
        if tx["hash"].lower() != tx_hash.lower() or receipt["transactionHash"].lower() != tx_hash.lower():
            raise ValueError("Native exit receipt hash mismatch")
        if (
            tx["from"].lower() != anchor.wallet
            or receipt["from"].lower() != anchor.wallet
            or quantity(receipt["status"]) != 1
        ):
            raise ValueError("Native exit sender/status mismatch")
        block = quantity(receipt["blockNumber"])
        if (
            block <= anchor.block_number
            or quantity(tx["blockNumber"]) != block
            or tx["blockHash"].lower() != receipt["blockHash"].lower()
        ):
            raise ValueError("Native exit block mismatch")
        if (
            rpc(gateway, chain, "eth_getBlockByNumber", [hex(block), False])["hash"].lower()
            != receipt["blockHash"].lower()
        ):
            raise ValueError("Native exit receipt reorg")
        gas += _native_exit_receipt_gas_cost(anchor, receipt, chain)
        sent += quantity(tx["value"])
        nonces.append(quantity(tx["nonce"]))
        if block in blocks and blocks[block] != receipt["blockHash"].lower():
            raise ValueError("Native exit receipts disagree on canonical block")
        blocks[block] = receipt["blockHash"].lower()
    terminal = max(blocks)
    end_nonce = quantity(rpc(gateway, chain, "eth_getTransactionCount", [anchor.wallet, hex(terminal)]))
    if (
        end_nonce - anchor.nonce != len(hashes)
        or sorted(nonces) != list(range(anchor.nonce, anchor.nonce + len(hashes)))
        or sent != anchor.amount
    ):
        raise ValueError("Native exit nonce coverage or value mismatch")
    balance = quantity(gateway.query_native_balance(chain=chain, wallet_address=anchor.wallet, block=terminal))
    if balance + anchor.amount + gas != anchor.balance:
        raise ValueError("Native exit balance conservation failed")
    _recheck_blocks(gateway, chain, [(anchor.block_number, anchor.block_hash), *blocks.items()])
    return NativeClosureProof(
        anchor, terminal, blocks[terminal], balance, tuple(hashes), gas, tuple(sorted(blocks.items()))
    )


def read_native_inventory(state_manager: Any, deployment_id: str, chain: str, wallet: str) -> dict | None:
    """Read measured accounting provenance scoped to the exact chain and EOA wallet."""
    from almanak.framework.accounting.basis import sum_open_wallet_basis_by_token

    reader = getattr(state_manager, "read_accounting_events_measured", None)
    if not callable(reader):
        return None
    events, measured = reader(deployment_id)
    if measured is not True or events is None:
        return None
    ledger_reader = getattr(state_manager, "read_ledger_entries_measured", None)
    if not callable(ledger_reader):
        return None
    ledger_rows, ledger_measured = ledger_reader(deployment_id)
    if ledger_measured is not True or ledger_rows is None:
        return None
    for row in ledger_rows:
        if field(row, "deployment_id") != deployment_id or str(field(row, "chain", "")).lower() != chain.lower():
            raise ValueError("Ledger inventory provenance mismatch")
    scoped = []
    expected_key = f"swap:{chain.lower()}:{wallet.lower()}"
    for event in events:
        if event.get("deployment_id") != deployment_id:
            raise ValueError("Accounting deployment provenance mismatch")
        if (
            str(event.get("chain", "")).lower() != chain.lower()
            or str(event.get("wallet_address", "")).lower() != wallet.lower()
        ):
            raise ValueError("Accounting chain/wallet provenance mismatch")
        if event.get("event_type") == "SWAP":
            payload = json.loads(event.get("payload_json") or "{}")
            if (payload.get("swap_position_key") or event.get("position_key") or expected_key).lower() != expected_key:
                raise ValueError("Accounting swap identity mismatch")
        scoped.append(event)
    return sum_open_wallet_basis_by_token(
        scoped, deployment_id, ledger_rows=ledger_rows, chain=chain, wallet_address=wallet
    )
