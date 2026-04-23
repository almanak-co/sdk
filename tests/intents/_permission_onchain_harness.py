"""Parametrizable harness for on-chain permission authorisation tests.

Purpose: given a ``PermissionTestCase`` that names a `(chain, protocol,
intent_type, config)` tuple, deploy a Safe + Zodiac Roles Modifier on the
Anvil fork, apply the manifest the generator produces for that tuple, and
prove the manifest ``authorises`` the compiled intent end-to-end under
``execTransactionWithRole``. The negative counterpart proves the manifest is
load-bearing — strip a required target, rerun the same intent, expect revert.

This harness is the execution half of the auto-discovery coverage model (plan
doc: ``docs/internal/zodiac-permission-onchain-coverage-plan.md``). The
declaration half lives in ``tests/intents/permission_cases/<protocol>.py``;
the coverage gate that connects the two lives in
``tests/unit/permissions/test_onchain_case_coverage.py``.

Phase A implements the SWAP dispatch branch. LEND / LP / BRIDGE / VAULT /
PERP branches are stubbed with ``NotImplementedError`` — they land alongside
the connector coverage in phases B–E.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

import pytest
from eth_account import Account
from web3 import Web3

from almanak.framework.execution.signer.safe.constants import (
    ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
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
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_swap_bilateral_deltas,
    assert_swap_conservation,
    fund_erc20_token,
    get_token_balance,
    get_token_decimals,
)

# Gas ceiling for the Roles wrapper — inner call + ~80k Zodiac overhead.
_ZODIAC_WRAPPER_GAS = 1_500_000


@dataclass(frozen=True)
class PermissionTestCase:
    """Declarative case for on-chain permission authorisation testing.

    ``config`` is intentionally unopinionated — fields are unpacked into the
    underlying intent constructor by the per-intent-type dispatcher. If the
    intent schema changes, cases break at construction time (good) rather
    than silently accepting a stale payload (bad).
    """

    chain: str
    protocol: str
    intent_type: str
    config: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        # Basic shape check — richer validation is the dispatcher's job.
        if not self.chain:
            raise ValueError("PermissionTestCase.chain is required")
        if not self.protocol:
            raise ValueError("PermissionTestCase.protocol is required")
        if not self.intent_type:
            raise ValueError("PermissionTestCase.intent_type is required")


# =============================================================================
# Internal setup primitives
# =============================================================================


def _setup_zodiac_env(
    web3: Web3,
    owner_eoa: str,
    owner_private_key: str,
    *,
    role_label: str,
) -> tuple[str, str, bytes]:
    """Deploy Safe + Roles, grant a per-test role to ``owner_eoa``.

    Returns ``(safe_address, roles_address, role_key)``. The role label is
    truncated/padded to 32 bytes so different test cases in the same fork
    can't collide on role identity.
    """
    safe = deploy_test_safe(web3, owner_eoa, owner_private_key)
    roles = deploy_test_zodiac_roles(web3, safe, owner_eoa, owner_private_key)
    role_key = role_label.encode("utf-8")[:32].ljust(32, b"\0")
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


def _fund_safe_with_token(
    safe: str,
    token_symbol: str,
    amount_wei: int,
    chain: str,
    anvil_rpc_url: str,
) -> None:
    """Seed the Safe's balance of ``token_symbol`` via Anvil storage slot writes."""
    token_addr = CHAIN_CONFIGS[chain]["tokens"][token_symbol]
    slot = CHAIN_CONFIGS[chain]["balance_slots"][token_symbol]
    fund_erc20_token(safe, token_addr, amount_wei, slot, anvil_rpc_url)


def _exec_bundle_via_zodiac(
    web3: Web3,
    roles: str,
    role_key: bytes,
    bundle_txs: Sequence[Any],
    *,
    member_eoa: str,
    member_private_key: str,
    should_revert: bool = True,
) -> list[dict]:
    """Submit each tx in a compiled ActionBundle through ``execTransactionWithRole``.

    Returns the list of receipts in submission order. Raises ``RuntimeError``
    if ``should_revert`` is True and any receipt has ``status != 1`` — this is
    the signal the caller uses to detect permission denials on the negative path.
    """
    roles_c = web3.eth.contract(
        address=Web3.to_checksum_address(roles),
        abi=ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
    )
    member_addr = Web3.to_checksum_address(member_eoa)
    receipts: list[dict] = []

    for tx in bundle_txs:
        # IntentCompiler emits plain dicts; other callers may pass dataclass-ish
        # UnsignedTransaction instances. Normalise both.
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

        # Match production ZodiacSigner: DELEGATECALL for Enso delegate targets,
        # CALL otherwise. ``get_operation_type`` owns this decision so the test
        # infra stays in lockstep with the real signer path.
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
                "gas": tx.get("gas", _ZODIAC_WRAPPER_GAS) if isinstance(tx, dict) else getattr(tx, "gas", _ZODIAC_WRAPPER_GAS),
            }
        )
        signed = Account.sign_transaction(built, member_private_key)
        tx_hash = web3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = web3.eth.wait_for_transaction_receipt(tx_hash)
        receipts.append(receipt)
        if should_revert and receipt["status"] != 1:
            raise RuntimeError(
                f"execTransactionWithRole reverted mid-bundle (to={to_addr}, tx={tx_hash.hex()}). "
                "Inner call likely blocked by Roles Modifier — permission missing or misapplied."
            )
    return receipts


def _find_target_by_selector(targets: list[dict], selector_hex: str) -> dict:
    """Return the function-scoped (``clearance == 2``) target that owns ``selector_hex``.

    Raises if zero or multiple targets match — the negative-test caller must
    be pointing at exactly one load-bearing target for the assertion to be
    meaningful.
    """
    needle = selector_hex.lower()
    matches = [
        t
        for t in targets
        if t.get("clearance") == 2
        and any(fn.get("selector", "").lower() == needle for fn in t.get("functions", []))
    ]
    if len(matches) != 1:
        raise AssertionError(
            f"Expected exactly one function-scoped target with selector {needle}, got {len(matches)}: "
            f"{[t.get('address') for t in matches]}"
        )
    return matches[0]


# =============================================================================
# Intent construction dispatch (Phase A = SWAP only)
# =============================================================================


def _build_swap_intent(case: PermissionTestCase) -> SwapIntent:
    """Construct a ``SwapIntent`` from the case config.

    Required config keys: ``from_token``, ``to_token``, ``amount``.
    Optional: ``max_slippage`` (default ``"0.01"``).
    """
    cfg = case.config
    return SwapIntent(
        from_token=cfg["from_token"],
        to_token=cfg["to_token"],
        amount=Decimal(str(cfg["amount"])),
        max_slippage=Decimal(str(cfg.get("max_slippage", "0.01"))),
        protocol=case.protocol,
        chain=case.chain,
    )


def _build_intent(case: PermissionTestCase):
    """Dispatch on ``case.intent_type`` to the matching intent builder.

    Phase A implements SWAP. Subsequent phases add LP_OPEN, LP_CLOSE,
    SUPPLY/WITHDRAW/BORROW/REPAY, BRIDGE, VAULT_DEPOSIT/REDEEM, etc.
    """
    it = case.intent_type.upper()
    if it == "SWAP":
        return _build_swap_intent(case)
    raise NotImplementedError(
        f"Intent type {it!r} not yet supported by the on-chain permission harness. "
        "See docs/internal/zodiac-permission-onchain-coverage-plan.md phases B–E."
    )


# =============================================================================
# Top-level entrypoints
# =============================================================================


def run_positive_authorisation_case(
    case: PermissionTestCase,
    *,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
    role_label: str | None = None,
) -> None:
    """Assert the generator's manifest authorises the compiled intent end-to-end.

    Flow: deploy Safe + Roles → fund Safe with input token → generate manifest
    → apply targets → compile intent with ``wallet=safe`` → execute each tx via
    ``execTransactionWithRole`` → assert bilateral balance deltas.

    A failure here means either (a) the manifest omits a target the compiled
    bundle actually calls, (b) the Zodiac encoder produced wrong selectors /
    clearances / executionOptions, or (c) the compiler changed and the
    manifest generator didn't follow. All three are shipping-blocking.
    """
    label = role_label or f"PermOnchain:{case.protocol}:{case.intent_type}"
    tokens = CHAIN_CONFIGS[case.chain]["tokens"]

    it = case.intent_type.upper()
    if it != "SWAP":
        raise NotImplementedError(f"Positive path for {it!r} lands in a follow-up phase.")

    # --- Intent-type-specific: SWAP --------------------------------------
    from_symbol = case.config["from_token"]
    to_symbol = case.config["to_token"]
    from_addr = Web3.to_checksum_address(tokens[from_symbol])
    to_addr = Web3.to_checksum_address(tokens[to_symbol])
    from_decimals = get_token_decimals(web3, from_addr)
    to_decimals = get_token_decimals(web3, to_addr)
    amount_decimal = Decimal(str(case.config["amount"]))
    amount_wei = int(amount_decimal * Decimal(10**from_decimals))

    safe, roles, role_key = _setup_zodiac_env(
        web3, funded_wallet, test_private_key, role_label=label
    )
    # Seed 2x the swap amount so we have headroom for approve + gas edge cases.
    _fund_safe_with_token(safe, from_symbol, amount_wei * 2, case.chain, anvil_rpc_url)
    assert get_token_balance(web3, from_addr, safe) >= amount_wei, (
        f"Safe funding failed for {from_symbol} on {case.chain}"
    )

    intent_types = [case.intent_type.upper()]
    manifest = generate_manifest(
        strategy_name=f"perm_onchain_{case.protocol}_{case.intent_type.lower()}",
        chain=case.chain,
        supported_protocols=[case.protocol],
        intent_types=intent_types,
        config={"base_token": from_symbol, "quote_token": to_symbol},
    )
    targets = manifest.to_zodiac_targets()
    assert targets, f"Manifest for ({case.protocol}, {case.intent_type}) produced no targets"

    apply_manifest_targets(
        web3,
        roles,
        safe,
        role_key,
        targets=targets,
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )

    intent = _build_intent(case)
    compiler = IntentCompiler(
        chain=case.chain,
        wallet_address=safe,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    compilation = compiler.compile(intent)
    assert compilation.status.value == "SUCCESS", f"Compile failed: {compilation.error}"
    bundle = compilation.action_bundle
    assert bundle is not None and bundle.transactions, "ActionBundle must contain at least one tx"

    from_before = get_token_balance(web3, from_addr, safe)
    to_before = get_token_balance(web3, to_addr, safe)

    _exec_bundle_via_zodiac(
        web3,
        roles,
        role_key,
        bundle.transactions,
        member_eoa=funded_wallet,
        member_private_key=test_private_key,
        should_revert=True,
    )

    assert_swap_bilateral_deltas(
        web3,
        from_addr,
        to_addr,
        safe,
        from_before,
        to_before,
        amount_wei,
        in_decimals=from_decimals,
        out_decimals=to_decimals,
    )


def run_negative_authorisation_case(
    case: PermissionTestCase,
    *,
    load_bearing_selector: str,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
    role_label: str | None = None,
) -> None:
    """Assert that revoking a load-bearing target causes the same intent to revert.

    ``load_bearing_selector`` is the 4-byte function selector (``0x...``) of a
    function the compiled bundle must call. The harness finds the sole
    function-scoped target carrying that selector and revokes it, then runs
    the same intent and asserts both execution reverts and balance conservation.
    """
    label = role_label or f"PermOnchain:{case.protocol}:{case.intent_type}:neg"
    tokens = CHAIN_CONFIGS[case.chain]["tokens"]

    it = case.intent_type.upper()
    if it != "SWAP":
        raise NotImplementedError(f"Negative path for {it!r} lands in a follow-up phase.")

    # --- Intent-type-specific: SWAP --------------------------------------
    from_symbol = case.config["from_token"]
    to_symbol = case.config["to_token"]
    from_addr = Web3.to_checksum_address(tokens[from_symbol])
    to_addr = Web3.to_checksum_address(tokens[to_symbol])
    from_decimals = get_token_decimals(web3, from_addr)
    amount_decimal = Decimal(str(case.config["amount"]))
    amount_wei = int(amount_decimal * Decimal(10**from_decimals))

    safe, roles, role_key = _setup_zodiac_env(
        web3, funded_wallet, test_private_key, role_label=label
    )
    _fund_safe_with_token(safe, from_symbol, amount_wei * 2, case.chain, anvil_rpc_url)
    assert get_token_balance(web3, from_addr, safe) >= amount_wei, (
        f"Safe funding failed for {from_symbol} on {case.chain}"
    )

    manifest = generate_manifest(
        strategy_name=f"perm_onchain_{case.protocol}_{case.intent_type.lower()}_neg",
        chain=case.chain,
        supported_protocols=[case.protocol],
        intent_types=[case.intent_type.upper()],
        config={"base_token": from_symbol, "quote_token": to_symbol},
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

    load_bearing = _find_target_by_selector(targets, load_bearing_selector)
    revoke_target(
        web3,
        roles,
        safe,
        role_key,
        load_bearing["address"],
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )

    intent = _build_intent(case)
    compiler = IntentCompiler(
        chain=case.chain,
        wallet_address=safe,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    compilation = compiler.compile(intent)
    assert compilation.status.value == "SUCCESS", (
        f"Compile should still succeed (authz is on-chain only): {compilation.error}"
    )

    from_before = get_token_balance(web3, from_addr, safe)
    to_before = get_token_balance(web3, to_addr, safe)

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

    # Allowance may have landed before the swap was blocked — allowance is not
    # a balance. We only require no value moved. ``assert_swap_conservation``
    # checks both sides of the pair for conservation.
    assert_swap_conservation(web3, from_addr, to_addr, safe, from_before, to_before)
