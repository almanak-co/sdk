"""Versioned operation evidence and execution-time calldata continuity checks."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import asdict
from typing import Any

from eth_abi import decode

from almanak.connectors._strategy_base.slippage import compute_min_amount_out_from_bps
from almanak.framework.models.reproduction_bundle import ActionBundle
from almanak.framework.venues import VenueBindingFailure, VenueVerificationGateway

from .addresses import UNISWAP_V4
from .pool_key import PoolKey
from .sdk import SwapQuote, UniswapV4SDK


def transaction_digest(transactions: list[dict[str, Any]]) -> str:
    canonical = [
        {"to": tx["to"].lower(), "value": str(int(tx.get("value", 0))), "data": tx["data"].lower()}
        for tx in transactions
    ]
    return hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def bind_swap_operation(*, result: Any, chain: str, wallet: str, slippage_bps: int) -> dict[str, Any]:
    from .adapter import tx_to_dict

    verified = result.verified_venue
    transactions = [tx_to_dict(tx) for tx in result.transactions]
    _, _, deadline = decode(["bytes", "bytes[]", "uint256"], bytes.fromhex(transactions[-1]["data"][10:]))
    return {
        "schema_version": 1,
        "operation": "swap_exact_in",
        "chain": chain,
        "wallet": wallet.lower(),
        "pool_key": result.pool_key.to_wire(),
        "pool_manager": UNISWAP_V4[chain]["pool_manager"].lower(),
        "venue_binding_hash": verified.binding.binding_hash,
        "quote_block": verified.evidence.block_number,
        "quote_block_hash": verified.evidence.block_hash,
        "quoted_at": int(time.time()),
        "expires_at": deadline,
        "amount_in": str(result.amount_in),
        "amount_out": str(result.amount_out_quoted),
        "minimum_out": str(result.amount_out_minimum),
        "slippage_bps": slippage_bps,
        "token_in": result.token_in,
        "token_out": result.token_out,
        "hook_data": "0x" + result.hook_data.hex(),
        "hook_evidence": asdict(result.hook_evidence) if result.hook_evidence is not None else None,
        "quote_method": "quoter_amount_estimate",
        "transaction_digest": transaction_digest(transactions),
        "deployment_code": [
            (fact.target_ref.reference, fact.value)
            for fact in verified.evidence.observed_facts
            if fact.name == "code_hash" and fact.target_ref is not None
        ],
    }


def bind_lp_operation(
    bundle: ActionBundle,
    *,
    key: PoolKey,
    verified: Any,
    hook_evidence: Any,
    operation: str,
    wallet: str,
    hook_data: bytes,
    exit_hook_evidence: Any = None,
) -> None:
    _, deadline = decode(["bytes", "uint256"], bytes.fromhex(bundle.transactions[-1]["data"][10:]))
    bundle.metadata.update(
        {
            "pool_key": key.to_wire(),
            "pool_id": key.pool_id,
            "venue_binding_hash": verified.binding.binding_hash,
            "venue_binding": verified.binding.to_preimage_wire(),
            "protocol": "uniswap_v4",
        }
    )
    bundle.metadata["v4_operation"] = {
        "schema_version": 1,
        "operation": operation,
        "chain": verified.binding.chain,
        "wallet": wallet.lower(),
        "pool_key": key.to_wire(),
        "pool_manager": UNISWAP_V4[verified.binding.chain]["pool_manager"].lower(),
        "venue_binding_hash": verified.binding.binding_hash,
        "quote_block": verified.evidence.block_number,
        "quote_block_hash": verified.evidence.block_hash,
        "quoted_at": int(time.time()),
        "expires_at": deadline,
        "hook_data": "0x" + hook_data.hex(),
        "hook_evidence": asdict(hook_evidence) if hook_evidence is not None else None,
        "transaction_digest": transaction_digest(bundle.transactions),
        "deployment_code": [
            (fact.target_ref.reference, fact.value)
            for fact in verified.evidence.observed_facts
            if fact.name == "code_hash" and fact.target_ref is not None
        ],
    }
    if operation == "lp_open":
        bundle.metadata["v4_operation"]["exit_hook_evidence"] = (
            asdict(exit_hook_evidence) if exit_hook_evidence is not None else None
        )


def validate_execution(
    bundle: ActionBundle,
    *,
    chain: str,
    wallet: str,
    is_safe: bool,
    now: int | None = None,
    gateway: VenueVerificationGateway | None = None,
) -> None:
    """Refuse stale or changed operations before signing and before submitting.

    The hash protects continuity, not authorization: the gateway's policy, signer
    and actual Safe outer execution remain authoritative for wallet permissions.
    """
    addresses = UNISWAP_V4.get(chain, {})
    targets = {addresses[name].lower() for name in ("universal_router", "position_manager") if name in addresses}
    applies = (
        "v4_operation" in bundle.metadata
        or bundle.metadata.get("protocol") == "uniswap_v4"
        or any(tx.get("to", "").lower() in targets for tx in bundle.transactions)
    )
    if not applies:
        return
    if bundle.metadata.get("protocol") != "uniswap_v4" or bundle.intent_type not in (
        "SWAP",
        "LP_OPEN",
        "LP_CLOSE",
        "LP_COLLECT_FEES",
    ):
        raise ValueError("V4 transaction targets require the bound protocol and supported operation type")
    artifact = bundle.metadata.get("v4_operation")
    if not isinstance(artifact, dict) or artifact.get("schema_version") != 1:
        raise ValueError("V4 swaps require a verified version-1 operation artifact; offline estimates cannot execute")
    if artifact["chain"] != chain or artifact["wallet"] != wallet.lower():
        raise ValueError("V4 operation belongs to a different chain or wallet")
    if artifact["expires_at"] <= (int(time.time()) if now is None else now):
        raise ValueError("V4 operation expired; obtain a fresh quote and fresh authorization")
    key = PoolKey.from_wire(artifact["pool_key"])
    if artifact["pool_manager"] != UNISWAP_V4[chain]["pool_manager"].lower():
        raise ValueError("V4 operation PoolManager changed")
    if artifact["venue_binding_hash"] != bundle.metadata.get("venue_binding_hash"):
        raise ValueError("V4 operation venue binding changed")
    if key.to_wire() != bundle.metadata.get("pool_key") or key.pool_id != bundle.metadata.get("pool_id"):
        raise ValueError("V4 operation selected pool changed")
    if transaction_digest(bundle.transactions) != artifact["transaction_digest"]:
        raise ValueError("V4 operation transactions changed after quoting")
    if artifact["hook_evidence"] is not None and is_safe:
        raise ValueError("Hooked Safe routes require separate outer-route qualification")
    _validate_approvals(bundle, artifact, key)
    if bundle.intent_type != "SWAP":
        _validate_lp_encoding(bundle, artifact, key, wallet)
        _validate_fresh_evidence(artifact, key, gateway)
        _validate_position_continuity(bundle, key, gateway, chain, wallet)
        return
    _validate_swap_encoding(bundle, artifact, key, wallet, chain)
    _validate_fresh_evidence(artifact, key, gateway)


def _validate_swap_encoding(
    bundle: ActionBundle, artifact: dict[str, Any], key: PoolKey, wallet: str, chain: str
) -> None:
    if artifact["operation"] != "swap_exact_in" or not 0 <= artifact["slippage_bps"] < 10000:
        raise ValueError("V4 swap operation or slippage is invalid")
    quote = SwapQuote(
        amount_in=int(artifact["amount_in"]),
        amount_out=int(artifact["amount_out"]),
        fee_tier=key.fee,
        token_in=artifact["token_in"],
        token_out=artifact["token_out"],
        pool_key=key,
        hook_data=bytes.fromhex(artifact["hook_data"][2:]),
    )
    expected = UniswapV4SDK(chain).build_swap_tx(
        quote,
        recipient=wallet,
        slippage_bps=artifact["slippage_bps"],
        deadline=artifact["expires_at"],
    )
    actual = bundle.transactions[-1]
    if (actual["to"].lower(), int(actual["value"]), actual["data"].lower()) != (
        expected.to.lower(),
        expected.value,
        expected.data.lower(),
    ):
        raise ValueError("V4 nested router actions do not match the bound pool, data, recipient and limits")
    computed_minimum = compute_min_amount_out_from_bps(int(artifact["amount_out"]), artifact["slippage_bps"])
    if (
        int(artifact["minimum_out"]) <= 0
        or int(artifact["minimum_out"]) != computed_minimum
        or computed_minimum != int(bundle.metadata["amount_out_minimum"])
    ):
        raise ValueError("V4 swap minimum output is absent, zero or inconsistent")


def _validate_approvals(bundle: ActionBundle, artifact: dict[str, Any], key: PoolKey) -> None:
    from .sdk import PERMIT2_ADDRESS

    sdk = UniswapV4SDK(artifact["chain"])
    if bundle.intent_type == "SWAP":
        budgets = [(artifact["token_in"], int(artifact["amount_in"]))]
        spender = sdk.addresses["universal_router"]
    elif bundle.intent_type == "LP_OPEN":
        budgets = [
            (key.currency0, int(bundle.metadata["amount0_desired"])),
            (key.currency1, int(bundle.metadata["amount1_desired"])),
        ]
        spender = sdk.addresses["position_manager"]
    else:
        budgets = []
        spender = sdk.addresses["position_manager"]
    budgets = [(token, amount) for token, amount in budgets if int(token, 16) != 0]
    approvals = bundle.transactions[:-1]
    if len(approvals) != 2 * len(budgets):
        raise ValueError("V4 operation contains unexpected approval or auxiliary transactions")
    for index, (token, amount) in enumerate(budgets):
        erc20, permit = approvals[index * 2 : index * 2 + 2]
        expected = sdk.build_approve_tx(token, PERMIT2_ADDRESS, amount)
        if (erc20["to"].lower(), int(erc20.get("value", 0)), erc20["data"].lower()) != (
            expected.to.lower(),
            expected.value,
            expected.data.lower(),
        ):
            raise ValueError("V4 ERC20 approval exceeds or differs from the operation budget")
        raw = bytes.fromhex(permit["data"][2:])
        if len(raw) != 132:
            raise ValueError("V4 Permit2 approval has unexpected encoding")
        expiration = decode(["address", "address", "uint160", "uint48"], raw[4:])[3]
        if not artifact["expires_at"] <= expiration <= artifact["quoted_at"] + 30 * 86400:
            raise ValueError("V4 Permit2 approval expiration exceeds its bound lifetime")
        expected = sdk.build_permit2_approve_tx(token, spender, amount, expiration)
        if (permit["to"].lower(), int(permit.get("value", 0)), permit["data"].lower()) != (
            expected.to.lower(),
            expected.value,
            expected.data.lower(),
        ):
            raise ValueError("V4 Permit2 approval differs from the operation token, spender or budget")


def _validate_position_continuity(
    bundle: ActionBundle,
    key: PoolKey,
    gateway: VenueVerificationGateway | None,
    chain: str,
    wallet: str,
) -> None:
    if bundle.intent_type == "LP_OPEN":
        return
    from .position import observe_position

    if gateway is None:
        raise ValueError("V4 position continuity requires gateway observation")
    position = observe_position(gateway, chain=chain, token_id=int(bundle.metadata["position_id"]), wallet=wallet)
    if position.key != key:
        raise ValueError("V4 NFT key changed before execution")
    if int(bundle.metadata.get("liquidity_removed", 0)) > position.liquidity:
        raise ValueError("V4 withdrawal exceeds the current position liquidity")
    if bundle.metadata.get("close_all") and int(bundle.metadata["liquidity_removed"]) != position.liquidity:
        raise ValueError("V4 full-close liquidity changed; recompile against the current owned NFT")


def _validate_fresh_evidence(artifact: dict[str, Any], key: PoolKey, gateway: VenueVerificationGateway | None) -> None:
    from almanak.framework.primitives.types import Primitive

    from .behavior import admit_hook
    from .venue_verifier import V4VenueVerifier, verification_request

    if gateway is None:
        raise ValueError("V4 execution requires gateway-mediated fresh venue observations")
    chain = artifact["chain"]
    head = gateway.block_number(chain=chain)
    # Conservative age windows complement the on-chain deadline. Arbitrum's
    # sequencer emits subsecond blocks; elapsed seconds alone miss reorgs.
    max_lag = {"arbitrum": 1200, "base": 150, "optimism": 150}.get(chain, 24)
    if not 0 <= head - artifact["quote_block"] <= max_lag:
        raise ValueError("V4 quote exceeds its block age bound")
    if gateway.block_hash(chain=chain, block_number=artifact["quote_block"]) != artifact["quote_block_hash"]:
        raise ValueError("V4 quote block was reorganized")
    operation = artifact["operation"]
    primitive = Primitive.SWAP if operation == "swap_exact_in" else Primitive.LP
    fresh = V4VenueVerifier().verify_venue(verification_request(chain, key, primitive), gateway, block_number=head)
    if isinstance(fresh, VenueBindingFailure):
        raise ValueError(f"V4 execution identity unavailable: {fresh.detail}")
    if fresh.binding.binding_hash != artifact["venue_binding_hash"]:
        raise ValueError("V4 execution venue binding changed")
    code = [
        (fact.target_ref.reference, fact.value)
        for fact in fresh.evidence.observed_facts
        if fact.name == "code_hash" and fact.target_ref is not None
    ]
    if code != [tuple(entry) for entry in artifact["deployment_code"]]:
        raise ValueError("V4 deployed operation route changed since quote")
    admissions = [(operation, artifact["hook_evidence"])]
    if operation == "lp_open":
        admissions.append(("lp_close", artifact["exit_hook_evidence"]))
    for admitted_operation, old in admissions:
        evidence = admit_hook(
            chain=chain,
            key=key,
            operation=admitted_operation,
            route="universal_router_eoa" if admitted_operation == "swap_exact_in" else "position_manager_eoa",
            hook_data=bytes.fromhex(artifact["hook_data"][2:]),
            gateway=gateway,
            block_number=head,
        )
        if evidence is not None and (
            old is None
            or (evidence.profile, evidence.version, evidence.dependency_digest)
            != (old["profile"], old["version"], old["dependency_digest"])
        ):
            raise ValueError("V4 hook admission evidence changed since quote")


def _validate_lp_encoding(bundle: ActionBundle, artifact: dict[str, Any], key: PoolKey, wallet: str) -> None:
    from .sdk import LPDecreaseParams, LPMintParams

    metadata = bundle.metadata
    sdk = UniswapV4SDK(artifact["chain"])
    data = bytes.fromhex(artifact["hook_data"][2:])
    if bundle.intent_type == "LP_OPEN":
        if artifact["operation"] != "lp_open" or metadata.get("price_source") != "on_chain":
            raise ValueError("V4 liquidity entry requires measured pool state")
        expected = sdk.build_mint_position_tx(
            LPMintParams(
                pool_key=key,
                tick_lower=metadata["tick_lower"],
                tick_upper=metadata["tick_upper"],
                liquidity=int(metadata["liquidity"]),
                amount0_max=int(metadata["amount0_desired"]),
                amount1_max=int(metadata["amount1_desired"]),
                owner=wallet,
                hook_data=data,
            ),
            deadline=artifact["expires_at"],
        )
    elif bundle.intent_type == "LP_CLOSE":
        if artifact["operation"] != "lp_close":
            raise ValueError("V4 withdrawal operation mismatch")
        expected = sdk.build_decrease_liquidity_tx(
            LPDecreaseParams(
                token_id=int(metadata["position_id"]),
                liquidity=int(metadata["liquidity_removed"]),
                amount0_min=int(metadata["amount0_min"]),
                amount1_min=int(metadata["amount1_min"]),
                hook_data=data,
            ),
            currency0=key.currency0,
            currency1=key.currency1,
            recipient=wallet,
            deadline=artifact["expires_at"],
            burn=False,
        )
    else:
        if artifact["operation"] != "lp_collect_fees":
            raise ValueError("V4 collection operation mismatch")
        expected = sdk.build_collect_fees_tx(
            token_id=int(metadata["position_id"]),
            currency0=key.currency0,
            currency1=key.currency1,
            recipient=wallet,
            hook_data=data,
            deadline=artifact["expires_at"],
        )
    actual = bundle.transactions[-1]
    if (actual["to"].lower(), int(actual["value"]), actual["data"].lower()) != (
        expected.to.lower(),
        expected.value,
        expected.data.lower(),
    ):
        raise ValueError("V4 liquidity calldata differs from its bound identity, recipient or amount limits")
