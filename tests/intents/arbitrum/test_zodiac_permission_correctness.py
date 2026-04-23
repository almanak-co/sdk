"""Q2b on-chain verification: generated permission manifests authorise compiled intents.

These tests deploy a fresh Safe + Zodiac Roles Modifier v2 on the Anvil fork,
apply a permission manifest produced by
``almanak.framework.permissions.generator.generate_manifest()``, and execute a
compiled intent through ``execTransactionWithRole`` to prove the manifest's
Zodiac target list actually authorises the intent end-to-end.

Unlike the 4-layer intent tests (``.claude/rules/intent-tests.md``) these
verify **authorisation**, not intent semantics — the 4-layer correctness is
already covered by the existing per-protocol tests. Each test here uses
``# noqa: layers`` with a pointer to ``docs/internal/zodiac-q2b-implementation-plan.md``.

The MVP (this file) covers a single intent type (Uniswap V3 SWAP) on a single
chain (Arbitrum). Other chains + intent types land in follow-up PRs.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from eth_account import Account
from web3 import Web3

from almanak.framework.connectors.uniswap_v3.sdk import EXACT_INPUT_SINGLE_SELECTOR
from almanak.framework.execution.signer.safe.constants import (
    ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
    SafeOperation,
    get_operation_type,
)
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.permissions.generator import generate_manifest
from tests.intents._zodiac_helpers import (
    apply_manifest_targets,
    assign_role_to_member,
    deploy_test_safe,
    deploy_test_zodiac_roles,
    revoke_target,
)
from tests.intents.conftest import CHAIN_CONFIGS, fund_erc20_token, get_token_balance

# =============================================================================
# Test configuration
# =============================================================================

CHAIN_NAME = "arbitrum"

# ERC-20 approve(address,uint256) — used by the plumbing spike.
ERC20_APPROVE_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "spender", "type": "address"},
            {"internalType": "uint256", "name": "amount", "type": "uint256"},
        ],
        "name": "approve",
        "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "owner", "type": "address"},
            {"internalType": "address", "name": "spender", "type": "address"},
        ],
        "name": "allowance",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]


# =============================================================================
# Plumbing smoke test (Phase 2c spike)
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.asyncio
async def test_zodiac_plumbing_allows_approve_on_wildcarded_usdc(  # noqa: layers
    web3: Web3,
    funded_wallet: str,
    test_private_key: str,
) -> None:
    """Integration spike: Safe deploy → Roles deploy → assignRoles → allowTarget → approve.

    # noqa: layers — Q2b tests verify authorisation, not intent semantics; the
    # 4-layer mandate (compile/execute/parse/balance-delta) is enforced by the
    # existing per-protocol intent tests. See
    # docs/internal/zodiac-q2b-implementation-plan.md § D7.

    Proves the full plumbing works end-to-end before the manifest-driven
    tests layer on top:

    1. Deploy Safe v1.4.1 with ``funded_wallet`` as sole owner (threshold 1).
    2. Deploy Roles Modifier v2 with owner=avatar=target=Safe; enable on Safe.
    3. Grant ``funded_wallet`` membership of a test role + set it as default.
    4. ``allowTarget(role_key, USDC, ExecutionOptions.None)`` — wildcard USDC.
    5. From ``funded_wallet`` EOA, call
       ``Roles.execTransactionWithRole(USDC, 0, approve(spender, 1 USDC),
       CALL, role_key, shouldRevert=true)``.
    6. Assert ``USDC.allowance(safe, spender) == 1_000_000`` (1 USDC in 6dp).

    If this passes, every primitive needed by the manifest-driven tests is
    wired correctly.
    """
    usdc_address = Web3.to_checksum_address(CHAIN_CONFIGS[CHAIN_NAME]["tokens"]["USDC"])
    role_key = b"Q2bPlumbingRole".ljust(32, b"\0")
    arbitrary_spender = Web3.to_checksum_address("0x00000000000000000000000000000000DEAdBEef")
    approve_amount = 1_000_000  # 1 USDC, 6 decimals

    # --- 1. Deploy Safe
    safe = deploy_test_safe(web3, funded_wallet, test_private_key)
    assert Web3.is_checksum_address(safe)

    # --- 2. Deploy Roles + enable on Safe
    roles = deploy_test_zodiac_roles(web3, safe, funded_wallet, test_private_key)
    assert Web3.is_checksum_address(roles)
    assert roles != safe

    # --- 3. Assign role + set as default
    assign_role_to_member(
        web3,
        roles,
        safe,
        role_key,
        member_eoa=funded_wallet,
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )

    # --- 4. Wildcard USDC (clearance=1 in manifest terms, allowTarget on-chain)
    apply_manifest_targets(
        web3,
        roles,
        safe,
        role_key,
        targets=[{"address": usdc_address, "clearance": 1, "executionOptions": 0}],
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )

    # --- 5. From EOA, execTransactionWithRole(USDC, 0, approve(spender, 1 USDC), CALL, role_key, true)
    usdc = web3.eth.contract(address=usdc_address, abi=ERC20_APPROVE_ABI)
    approve_data = bytes.fromhex(usdc.encode_abi("approve", args=[arbitrary_spender, approve_amount])[2:])

    roles_c = web3.eth.contract(
        address=Web3.to_checksum_address(roles),
        abi=ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
    )
    tx = roles_c.functions.execTransactionWithRole(
        usdc_address,
        0,
        approve_data,
        int(SafeOperation.CALL),
        role_key,
        True,  # shouldRevert — surface permission failures as reverts
    ).build_transaction(
        {
            "from": Web3.to_checksum_address(funded_wallet),
            "nonce": web3.eth.get_transaction_count(Web3.to_checksum_address(funded_wallet)),
        }
    )
    signed = Account.sign_transaction(tx, test_private_key)
    tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
    receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
    assert receipt["status"] == 1, f"execTransactionWithRole reverted (tx={tx_hash.hex()})"

    # --- 6. Assert the Safe's USDC allowance to spender is now 1 USDC
    allowance = usdc.functions.allowance(safe, arbitrary_spender).call()
    assert allowance == approve_amount, (
        f"Expected Safe.USDC.allowance({arbitrary_spender}) == {approve_amount}, got {allowance}. "
        f"Safe={safe}, Roles={roles}. Zodiac routed the approve but the Safe's allowance did not update — "
        f"this means the approve was sent but from the wrong msg.sender (should be the Safe)."
    )


# =============================================================================
# Shared setup helpers (module-local; not worth promoting to _zodiac_helpers.py)
# =============================================================================


def _setup_zodiac_env(
    web3: Web3,
    owner_eoa: str,
    owner_private_key: str,
) -> tuple[str, str, bytes]:
    """Deploy Safe + Roles, grant a test role to ``owner_eoa``, return (safe, roles, role_key)."""
    safe = deploy_test_safe(web3, owner_eoa, owner_private_key)
    roles = deploy_test_zodiac_roles(web3, safe, owner_eoa, owner_private_key)
    role_key = b"Q2bSwapRole".ljust(32, b"\0")
    assign_role_to_member(
        web3,
        roles,
        safe,
        role_key,
        member_eoa=owner_eoa,
        owner_eoa=owner_eoa,
        owner_private_key=owner_private_key,
    )
    return safe, roles, role_key


def _fund_safe_with_usdc(safe: str, amount_wei: int, anvil_rpc_url: str) -> None:
    """Seed the Safe's USDC balance via Anvil storage slot manipulation."""
    usdc_addr = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]["USDC"]
    slot = CHAIN_CONFIGS[CHAIN_NAME]["balance_slots"]["USDC"]
    fund_erc20_token(safe, usdc_addr, amount_wei, slot, anvil_rpc_url)


def _exec_bundle_via_zodiac(
    web3: Web3,
    roles: str,
    role_key: bytes,
    bundle_txs,
    member_eoa: str,
    member_private_key: str,
    *,
    should_revert: bool = True,
) -> list[dict]:
    """Submit each transaction in a compiled ActionBundle through
    ``Roles.execTransactionWithRole`` from the role member EOA.

    Returns the list of receipts in submission order. Raises if ``should_revert``
    is True and any receipt has ``status != 1``.
    """
    roles_c = web3.eth.contract(
        address=Web3.to_checksum_address(roles),
        abi=ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
    )
    receipts = []
    member_addr = Web3.to_checksum_address(member_eoa)

    for tx in bundle_txs:
        # IntentCompiler produces plain dicts; other call sites may pass
        # UnsignedTransaction dataclass instances. Normalise.
        if isinstance(tx, dict):
            to_addr = tx["to"]
            raw_value = tx.get("value", 0)
            raw_data = tx.get("data", "0x")
        else:
            to_addr = tx.to
            raw_value = tx.value
            raw_data = tx.data

        if isinstance(raw_value, str):
            value = int(raw_value, 16) if raw_value.startswith("0x") else int(raw_value)
        else:
            value = int(raw_value or 0)

        if isinstance(raw_data, bytes):
            data = raw_data
        else:
            data = bytes.fromhex(raw_data[2:] if raw_data.startswith("0x") else raw_data)

        # Match production ZodiacSigner: DELEGATECALL for Enso delegates, CALL otherwise.
        # Q2b MVP exercises Uniswap V3 only (CALL-only), but using the helper keeps
        # the test infra correct for the follow-up PRs that add Enso / other protocols.
        op_type = get_operation_type(to_addr)
        built = roles_c.functions.execTransactionWithRole(
            Web3.to_checksum_address(to_addr),
            value,
            data,
            int(op_type),
            role_key,
            should_revert,
        ).build_transaction(
            {
                "from": member_addr,
                "nonce": web3.eth.get_transaction_count(member_addr),
                # Static gas — Zodiac wrapper adds ~80k overhead over the inner call.
                "gas": 1_500_000,
            }
        )
        signed = Account.sign_transaction(built, member_private_key)
        tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
        receipts.append(receipt)
        if should_revert and receipt["status"] != 1:
            raise RuntimeError(
                f"execTransactionWithRole reverted mid-bundle (to={to_addr}, tx={tx_hash.hex()}). "
                f"Inner call likely blocked by Roles Modifier — permission missing or misapplied."
            )
    return receipts


# =============================================================================
# Phase 4 — positive test: manifest authorises a compiled Uniswap V3 swap
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.asyncio
async def test_manifest_authorises_uniswap_v3_swap(  # noqa: layers
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Permissions from ``generate_manifest()`` authorise a compiled USDC→WETH swap.

    # noqa: layers — see module docstring and Q2b plan § D7.

    End-to-end: the manifest for a ``uniswap_v3`` SWAP strategy produces a
    set of Zodiac targets (USDC/WETH approvals + SwapRouter02 + MultiSend).
    When those targets are applied on-chain, a real ``SwapIntent`` compiled
    for the Safe executes successfully through
    ``execTransactionWithRole`` from the member EOA, with the Safe's USDC
    balance decreasing by exactly the swap amount and WETH increasing by > 0.

    This is the inverse of ``test_manifest_coverage``'s static check: that
    one asserts ``manifest ⊇ compiled (target, selector)``; this one asserts
    the manifest **actually authorises** the compiled bundle on-chain.
    """
    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    usdc = Web3.to_checksum_address(tokens["USDC"])
    weth = Web3.to_checksum_address(tokens["WETH"])

    swap_amount = Decimal("100")  # 100 USDC
    swap_amount_wei = 100 * 10**6  # USDC has 6 decimals on Arbitrum

    # --- 1. Deploy Safe + Roles, assign role to funded_wallet EOA
    safe, roles, role_key = _setup_zodiac_env(web3, funded_wallet, test_private_key)

    # --- 2. Fund the Safe with USDC (enough for the swap)
    _fund_safe_with_usdc(safe, swap_amount_wei * 2, anvil_rpc_url)
    assert get_token_balance(web3, usdc, safe) >= swap_amount_wei, "Safe must have USDC to swap"

    # --- 3. Generate the permission manifest for uniswap_v3 SWAP
    manifest = generate_manifest(
        strategy_name="q2b_uniswap_swap",
        chain=CHAIN_NAME,
        supported_protocols=["uniswap_v3"],
        intent_types=["SWAP"],
        config={"base_token": "USDC", "quote_token": "WETH"},
    )
    targets = manifest.to_zodiac_targets()
    assert targets, "manifest must include at least one target"

    # --- 4. Apply the targets on-chain under the test role
    apply_manifest_targets(
        web3,
        roles,
        safe,
        role_key,
        targets=targets,
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )

    # --- 5. Compile a real SwapIntent with wallet=safe
    intent = SwapIntent(
        from_token="USDC",
        to_token="WETH",
        amount=swap_amount,
        max_slippage=Decimal("0.01"),
        protocol="uniswap_v3",
        chain=CHAIN_NAME,
    )
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=safe,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    compilation = compiler.compile(intent)
    assert compilation.status.value == "SUCCESS", f"Compile failed: {compilation.error}"
    bundle = compilation.action_bundle
    assert bundle is not None and bundle.transactions, "ActionBundle must contain at least one tx"

    # --- 6. Record balances, execute bundle via execTransactionWithRole
    usdc_before = get_token_balance(web3, usdc, safe)
    weth_before = get_token_balance(web3, weth, safe)

    _exec_bundle_via_zodiac(
        web3,
        roles,
        role_key,
        bundle.transactions,
        member_eoa=funded_wallet,
        member_private_key=test_private_key,
        should_revert=True,
    )

    # --- 7. Assert balance deltas
    usdc_after = get_token_balance(web3, usdc, safe)
    weth_after = get_token_balance(web3, weth, safe)

    assert usdc_before - usdc_after == swap_amount_wei, (
        f"Safe USDC should have decreased by exactly {swap_amount_wei}; "
        f"got {usdc_before - usdc_after} (before={usdc_before}, after={usdc_after})"
    )
    assert weth_after > weth_before, f"Safe WETH should have increased; got before={weth_before}, after={weth_after}"


# =============================================================================
# Phase 5 — negative test: revoking a target causes the same intent to revert
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.asyncio
async def test_manifest_denies_swap_after_router_revoked(  # noqa: layers
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Removing the SwapRouter target from Roles causes the same intent to revert.

    # noqa: layers — see module docstring and Q2b plan § D7.

    Sanity counterpart to the positive test. Proves that each target in the
    manifest is load-bearing: if we strip the Uniswap V3 SwapRouter02
    permission after applying the full manifest, Zodiac blocks the swap
    call and the Safe's balances remain unchanged — confirming the
    authorisation decision is actually being enforced.
    """
    tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
    usdc = Web3.to_checksum_address(tokens["USDC"])
    weth = Web3.to_checksum_address(tokens["WETH"])

    swap_amount = Decimal("100")
    swap_amount_wei = 100 * 10**6

    # --- 1-4. Same setup as the positive test
    safe, roles, role_key = _setup_zodiac_env(web3, funded_wallet, test_private_key)
    _fund_safe_with_usdc(safe, swap_amount_wei * 2, anvil_rpc_url)
    manifest = generate_manifest(
        strategy_name="q2b_uniswap_swap_negative",
        chain=CHAIN_NAME,
        supported_protocols=["uniswap_v3"],
        intent_types=["SWAP"],
        config={"base_token": "USDC", "quote_token": "WETH"},
    )
    targets = manifest.to_zodiac_targets()
    apply_manifest_targets(
        web3,
        roles,
        safe,
        role_key,
        targets=targets,
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )

    # --- 5. Revoke the Uniswap V3 SwapRouter02 target under the role.
    #    SwapRouter02 on Arbitrum is the only function-scoped target whose
    #    selector is exactInputSingle — the approve targets (USDC, WETH) have
    #    the ERC-20 approve selector. Sourced from the connector SDK rather
    #    than hardcoded so it stays in sync with the production encoder.
    swap_router_selector = EXACT_INPUT_SINGLE_SELECTOR.lower()
    swap_router_targets = [
        t
        for t in targets
        if t["clearance"] == 2 and any(fn["selector"].lower() == swap_router_selector for fn in t.get("functions", []))
    ]
    assert len(swap_router_targets) == 1, (
        f"Expected exactly one SwapRouter target in the manifest, got {len(swap_router_targets)}: "
        f"{[t['address'] for t in swap_router_targets]}"
    )
    router_addr = swap_router_targets[0]["address"]
    revoke_target(
        web3,
        roles,
        safe,
        role_key,
        router_addr,
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )

    # --- 6. Compile same SwapIntent
    intent = SwapIntent(
        from_token="USDC",
        to_token="WETH",
        amount=swap_amount,
        max_slippage=Decimal("0.01"),
        protocol="uniswap_v3",
        chain=CHAIN_NAME,
    )
    compiler = IntentCompiler(
        chain=CHAIN_NAME,
        wallet_address=safe,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    compilation = compiler.compile(intent)
    assert compilation.status.value == "SUCCESS", (
        f"Compile should still succeed (authz is on-chain only): {compilation.error}"
    )

    # --- 7. Record balances, attempt execution — expect Roles to block the router call.
    usdc_before = get_token_balance(web3, usdc, safe)
    weth_before = get_token_balance(web3, weth, safe)

    with pytest.raises(RuntimeError, match="execTransactionWithRole reverted mid-bundle"):
        _exec_bundle_via_zodiac(
            web3,
            roles,
            role_key,
            compilation.action_bundle.transactions,
            member_eoa=funded_wallet,
            member_private_key=test_private_key,
            should_revert=True,
        )

    # --- 8. Balance conservation: the swap must not have happened.
    usdc_after = get_token_balance(web3, usdc, safe)
    weth_after = get_token_balance(web3, weth, safe)
    # Note: the approve may have landed before the swap was blocked — that's OK,
    # allowance is not a balance. We only require no value moved.
    assert usdc_before == usdc_after, (
        f"Safe USDC should be unchanged after blocked swap; before={usdc_before}, after={usdc_after}"
    )
    assert weth_before == weth_after, (
        f"Safe WETH should be unchanged after blocked swap; before={weth_before}, after={weth_after}"
    )
