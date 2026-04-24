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

Intent-type dispatch covers SWAP, the LEND family
(SUPPLY/WITHDRAW/BORROW/REPAY), and the LP family (LP_OPEN, LP_CLOSE).
BRIDGE / VAULT / PERP branches are stubbed with ``NotImplementedError`` —
they land alongside the connector coverage in later phases.
"""

from __future__ import annotations

import importlib.util
from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from eth_account import Account
from web3 import Web3

from almanak.framework.execution.signer.safe.constants import (
    ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
    get_operation_type,
)
from almanak.framework.intents import (
    BorrowIntent,
    LPCloseIntent,
    LPOpenIntent,
    RepayIntent,
    SupplyIntent,
    SwapIntent,
    WithdrawIntent,
)
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

# Keys a case config can carry for pre-funding that are NOT intent constructor
# kwargs. The harness pops these before unpacking the rest into the intent.
_LP_FUNDING_KEYS = ("token0", "token1")


@dataclass(frozen=True)
class PermissionTestCase:
    """Declarative case for on-chain permission authorisation testing.

    ``config`` is intentionally unopinionated — fields are unpacked into the
    underlying intent constructor by the per-intent-type dispatcher. If the
    intent schema changes, cases break at construction time (good) rather
    than silently accepting a stale payload (bad).

    ``negative_selector`` (optional): the 4-byte function selector (``0x...``)
    that the negative authorisation test should strip from the applied manifest.
    When set, a case file can declare its own load-bearing selector so the
    per-chain test runner does not need to hardcode it. Callers of
    ``run_negative_authorisation_case`` can still pass ``load_bearing_selector``
    explicitly to override the case-declared value.
    """

    chain: str
    protocol: str
    intent_type: str
    config: dict[str, Any] = field(default_factory=dict)
    negative_selector: str | None = None

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


def _token_amount_wei(web3: Web3, chain: str, symbol: str, amount: Any) -> tuple[str, int, int]:
    """Resolve ``(token_address, decimals, amount_wei)`` for ``(chain, symbol)``.

    ``amount`` is accepted as str / int / Decimal — whatever the case config
    carries. Uses the live token decimals so the math aligns with production.
    """
    addr = Web3.to_checksum_address(CHAIN_CONFIGS[chain]["tokens"][symbol])
    decimals = get_token_decimals(web3, addr)
    wei = int(Decimal(str(amount)) * Decimal(10**decimals))
    return addr, decimals, wei


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
                "gas": (tx.get("gas") if isinstance(tx, dict) else getattr(tx, "gas", None)) or _ZODIAC_WRAPPER_GAS,
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
# Intent construction dispatch
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


def _build_supply_intent(case: PermissionTestCase) -> SupplyIntent:
    """Construct a ``SupplyIntent`` from the case config via ``**config``."""
    return SupplyIntent(**case.config, protocol=case.protocol, chain=case.chain)


def _build_withdraw_intent(case: PermissionTestCase) -> WithdrawIntent:
    """Construct a ``WithdrawIntent`` from the case config via ``**config``."""
    return WithdrawIntent(**case.config, protocol=case.protocol, chain=case.chain)


def _build_borrow_intent(case: PermissionTestCase) -> BorrowIntent:
    """Construct a ``BorrowIntent`` from the case config via ``**config``."""
    return BorrowIntent(**case.config, protocol=case.protocol, chain=case.chain)


def _build_repay_intent(case: PermissionTestCase) -> RepayIntent:
    """Construct a ``RepayIntent`` from the case config via ``**config``."""
    return RepayIntent(**case.config, protocol=case.protocol, chain=case.chain)


def _build_lp_open_intent(case: PermissionTestCase) -> LPOpenIntent:
    """Construct an ``LPOpenIntent`` from the case config.

    ``token0``/``token1`` are pre-funding hints for the harness and are
    stripped before the remaining fields are unpacked into ``LPOpenIntent``.
    """
    cfg = {k: v for k, v in case.config.items() if k not in _LP_FUNDING_KEYS}
    return LPOpenIntent(**cfg, protocol=case.protocol, chain=case.chain)


def _build_lp_close_intent(case: PermissionTestCase) -> LPCloseIntent:
    """Construct an ``LPCloseIntent`` from the case config via ``**config``."""
    return LPCloseIntent(**case.config, protocol=case.protocol, chain=case.chain)


def _build_intent(case: PermissionTestCase):
    """Dispatch on ``case.intent_type`` to the matching intent builder."""
    it = case.intent_type.upper()
    if it == "SWAP":
        return _build_swap_intent(case)
    if it == "SUPPLY":
        return _build_supply_intent(case)
    if it == "WITHDRAW":
        return _build_withdraw_intent(case)
    if it == "BORROW":
        return _build_borrow_intent(case)
    if it == "REPAY":
        return _build_repay_intent(case)
    if it == "LP_OPEN":
        return _build_lp_open_intent(case)
    if it == "LP_CLOSE":
        return _build_lp_close_intent(case)
    raise NotImplementedError(
        f"Intent type {it!r} not yet supported by the on-chain permission harness. "
        "See docs/internal/zodiac-permission-onchain-coverage-plan.md phases B–E."
    )


# =============================================================================
# Family-specific balance-direction assertions
# =============================================================================


def _assert_balance_decreased(
    web3: Web3,
    token_addr: str,
    wallet: str,
    balance_before: int,
    *,
    token_label: str,
    decimals: int,
) -> int:
    """Assert ``wallet``'s balance of ``token_addr`` strictly decreased. Returns delta."""
    after = get_token_balance(web3, token_addr, wallet)
    delta = balance_before - after
    assert delta > 0, (
        f"{token_label} balance must decrease (no-op guard). "
        f"Before: {balance_before}, After: {after}, decimals: {decimals}"
    )
    return delta


def _assert_balance_increased(
    web3: Web3,
    token_addr: str,
    wallet: str,
    balance_before: int,
    *,
    token_label: str,
    decimals: int,
) -> int:
    """Assert ``wallet``'s balance of ``token_addr`` strictly increased. Returns delta."""
    after = get_token_balance(web3, token_addr, wallet)
    delta = after - balance_before
    assert delta > 0, (
        f"{token_label} balance must increase (no-op guard). "
        f"Before: {balance_before}, After: {after}, decimals: {decimals}"
    )
    return delta


def _assert_balances_any_increased(
    web3: Web3,
    wallet: str,
    snapshots: Sequence[tuple[str, str, int]],
    *,
    context: str,
) -> None:
    """Assert at least one balance in ``snapshots`` strictly increased.

    ``snapshots`` is a sequence of ``(token_addr, label, balance_before)``.
    """
    report: list[tuple[str, int]] = []
    any_gained = False
    for token_addr, label, before in snapshots:
        after = get_token_balance(web3, token_addr, wallet)
        delta = after - before
        report.append((label, delta))
        if delta > 0:
            any_gained = True
    assert any_gained, (
        f"{context}: expected at least one of {[r[0] for r in report]} to increase, "
        f"got deltas {report}."
    )


# =============================================================================
# Shared plumbing: Zodiac setup + manifest application
# =============================================================================


def _setup_zodiac_and_apply_manifest(
    case: PermissionTestCase,
    *,
    web3: Web3,
    funded_wallet: str,
    test_private_key: str,
    role_label: str,
    strategy_suffix: str = "",
) -> tuple[str, str, bytes, list[dict]]:
    """Deploy Safe + Roles, generate the manifest, apply its targets.

    Returns ``(safe, roles, role_key, targets)``. The targets list is
    returned so the negative path can pick a load-bearing entry to revoke.
    """
    safe, roles, role_key = _setup_zodiac_env(
        web3, funded_wallet, test_private_key, role_label=role_label
    )

    manifest_config = dict(case.config)
    # The manifest generator infers ERC-20 approve permissions by scanning
    # specific token field names (``_TOKEN_CONFIG_FIELDS`` in the generator).
    # Map intent-constructor keys used in case configs onto those names so
    # approvals land on the manifest without forcing case files to duplicate
    # token fields under alias names.
    it = case.intent_type.upper()
    if it == "SWAP":
        if "base_token" not in manifest_config and "from_token" in case.config:
            manifest_config["base_token"] = case.config["from_token"]
        if "quote_token" not in manifest_config and "to_token" in case.config:
            manifest_config["quote_token"] = case.config["to_token"]
    elif it == "SUPPLY":
        if "supply_token" not in manifest_config and "token" in case.config:
            manifest_config["supply_token"] = case.config["token"]
    elif it == "WITHDRAW":
        if "withdraw_token" not in manifest_config and "token" in case.config:
            manifest_config["withdraw_token"] = case.config["token"]
    elif it == "REPAY":
        if "repay_token" not in manifest_config and "token" in case.config:
            manifest_config["repay_token"] = case.config["token"]
    elif it == "LP_OPEN":
        # token0 / token1 are already recognised aliases via the case config
        # keys themselves — but the generator keys on specific names. Surface
        # the two tokens under ``base_token``/``quote_token`` so approvals
        # for both sides of the pair are inferred.
        if "base_token" not in manifest_config and "token0" in case.config:
            manifest_config["base_token"] = case.config["token0"]
        if "quote_token" not in manifest_config and "token1" in case.config:
            manifest_config["quote_token"] = case.config["token1"]
    # BORROW uses ``collateral_token`` and ``borrow_token`` — both are already
    # in ``_TOKEN_CONFIG_FIELDS`` and the case config carries those exact keys
    # via ``**config`` unpacking, so no mapping needed. LP_CLOSE derives
    # allowances from on-chain position metadata, so no token surface mapping.

    strategy_name = (
        f"perm_onchain_{case.protocol}_{case.intent_type.lower()}{strategy_suffix}"
    )
    manifest = generate_manifest(
        strategy_name=strategy_name,
        chain=case.chain,
        supported_protocols=[case.protocol],
        intent_types=[case.intent_type.upper()],
        config=manifest_config,
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
    return safe, roles, role_key, targets


def _compile_for_safe(
    case: PermissionTestCase,
    *,
    safe: str,
    anvil_rpc_url: str,
    price_oracle,
):
    """Compile ``case``'s intent with ``wallet=safe``. Returns the ``CompilationResult``."""
    intent = _build_intent(case)
    compiler = IntentCompiler(
        chain=case.chain,
        wallet_address=safe,
        price_oracle=price_oracle,
        rpc_url=anvil_rpc_url,
    )
    return compiler.compile(intent)


# =============================================================================
# Per-family positive executors
# =============================================================================


def _run_swap_positive(
    case: PermissionTestCase,
    *,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
    role_label: str,
) -> None:
    """SWAP positive path: fund input token, execute, assert bilateral deltas."""
    from_symbol = case.config["from_token"]
    to_symbol = case.config["to_token"]
    from_addr, from_decimals, amount_wei = _token_amount_wei(
        web3, case.chain, from_symbol, case.config["amount"]
    )
    to_addr, to_decimals, _ = _token_amount_wei(
        web3, case.chain, to_symbol, 0
    )

    safe, roles, role_key, _targets = _setup_zodiac_and_apply_manifest(
        case,
        web3=web3,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        role_label=role_label,
    )
    # Seed 2x the swap amount so we have headroom for approve + gas edge cases.
    _fund_safe_with_token(safe, from_symbol, amount_wei * 2, case.chain, anvil_rpc_url)
    assert get_token_balance(web3, from_addr, safe) >= amount_wei, (
        f"Safe funding failed for {from_symbol} on {case.chain}"
    )

    compilation = _compile_for_safe(
        case, safe=safe, anvil_rpc_url=anvil_rpc_url, price_oracle=price_oracle
    )
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


def _run_lend_positive(
    case: PermissionTestCase,
    *,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
    role_label: str,
) -> None:
    """LEND positive path.

    SUPPLY / WITHDRAW / REPAY: single ``config["token"]`` is the operand;
    fund that token (WITHDRAW funds too, so the Safe's aToken/collateral
    balance exists before the withdraw attempt).

    BORROW: ``collateral_token`` + ``collateral_amount`` seed the Safe;
    ``borrow_token`` is the expected asset received.

    Directions checked:
      - SUPPLY / REPAY: operand token balance decreases.
      - WITHDRAW: operand token balance increases (principal returned).
      - BORROW: ``borrow_token`` balance increases.
    """
    it = case.intent_type.upper()
    if it == "BORROW":
        _run_lend_borrow_positive(
            case,
            web3=web3,
            anvil_rpc_url=anvil_rpc_url,
            funded_wallet=funded_wallet,
            test_private_key=test_private_key,
            price_oracle=price_oracle,
            role_label=role_label,
        )
        return

    cfg = case.config
    token_symbol = cfg["token"]
    token_addr, token_decimals, amount_wei = _token_amount_wei(
        web3, case.chain, token_symbol, cfg.get("amount", 0)
    )

    safe, roles, role_key, _targets = _setup_zodiac_and_apply_manifest(
        case,
        web3=web3,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        role_label=role_label,
    )

    # Even WITHDRAW funds the operand: the connector path SUPPLY-then-WITHDRAW
    # within a single compiled bundle is protocol-dependent. Ensure the Safe
    # owns the operand token so approvals / transferFroms don't fail for a
    # reason unrelated to authorisation.
    fund_wei = amount_wei * 2
    if fund_wei > 0:
        _fund_safe_with_token(safe, token_symbol, fund_wei, case.chain, anvil_rpc_url)

    compilation = _compile_for_safe(
        case, safe=safe, anvil_rpc_url=anvil_rpc_url, price_oracle=price_oracle
    )
    assert compilation.status.value == "SUCCESS", f"Compile failed: {compilation.error}"
    bundle = compilation.action_bundle
    assert bundle is not None and bundle.transactions, "ActionBundle must contain at least one tx"

    primary_before = get_token_balance(web3, token_addr, safe)

    _exec_bundle_via_zodiac(
        web3,
        roles,
        role_key,
        bundle.transactions,
        member_eoa=funded_wallet,
        member_private_key=test_private_key,
        should_revert=True,
    )

    if it in {"SUPPLY", "REPAY"}:
        _assert_balance_decreased(
            web3,
            token_addr,
            safe,
            primary_before,
            token_label=f"{token_symbol} (operand)",
            decimals=token_decimals,
        )
    elif it == "WITHDRAW":
        _assert_balance_increased(
            web3,
            token_addr,
            safe,
            primary_before,
            token_label=f"{token_symbol} (withdrawn)",
            decimals=token_decimals,
        )
    else:  # pragma: no cover — defensive guard
        raise AssertionError(f"Unhandled single-operand LEND intent type {it!r}")


def _run_lend_borrow_positive(
    case: PermissionTestCase,
    *,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
    role_label: str,
) -> None:
    """BORROW positive path — split out from ``_run_lend_positive`` for clarity.

    Two tokens matter: ``collateral_token`` (seeded) and ``borrow_token``
    (expected delta). Direction: ``borrow_token`` balance must increase.
    """
    cfg = case.config
    collat_symbol = cfg["collateral_token"]
    borrow_symbol = cfg["borrow_token"]
    collat_addr, _, collat_wei = _token_amount_wei(
        web3, case.chain, collat_symbol, cfg["collateral_amount"]
    )
    borrow_addr, borrow_decimals, _ = _token_amount_wei(
        web3, case.chain, borrow_symbol, 0
    )

    safe, roles, role_key, _targets = _setup_zodiac_and_apply_manifest(
        case,
        web3=web3,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        role_label=role_label,
    )
    # 2x headroom mirrors SWAP — covers rounding + any transient buffering the
    # compiler/connector adds before the borrow executes.
    _fund_safe_with_token(safe, collat_symbol, collat_wei * 2, case.chain, anvil_rpc_url)
    assert get_token_balance(web3, collat_addr, safe) >= collat_wei, (
        f"Safe collateral funding failed for {collat_symbol} on {case.chain}"
    )

    compilation = _compile_for_safe(
        case, safe=safe, anvil_rpc_url=anvil_rpc_url, price_oracle=price_oracle
    )
    assert compilation.status.value == "SUCCESS", f"Compile failed: {compilation.error}"
    bundle = compilation.action_bundle
    assert bundle is not None and bundle.transactions, "ActionBundle must contain at least one tx"

    borrow_before = get_token_balance(web3, borrow_addr, safe)

    _exec_bundle_via_zodiac(
        web3,
        roles,
        role_key,
        bundle.transactions,
        member_eoa=funded_wallet,
        member_private_key=test_private_key,
        should_revert=True,
    )

    _assert_balance_increased(
        web3,
        borrow_addr,
        safe,
        borrow_before,
        token_label=f"{borrow_symbol} (borrowed)",
        decimals=borrow_decimals,
    )


def _run_lp_open_positive(
    case: PermissionTestCase,
    *,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
    role_label: str,
) -> None:
    """LP_OPEN positive path: fund both tokens, execute, assert at least one moved.

    Required config keys for funding: ``token0``, ``token1`` (symbols) —
    plus whatever amounts the LPOpenIntent constructor needs
    (``amount0``, ``amount1``, etc.). The Position-NFT mint check is
    intentionally out of scope here; this harness asserts authorisation +
    economic direction, not LP semantics.
    """
    cfg = case.config
    token0_symbol = cfg["token0"]
    token1_symbol = cfg["token1"]
    token0_addr, token0_decimals, amount0_wei = _token_amount_wei(
        web3, case.chain, token0_symbol, cfg.get("amount0", 0)
    )
    token1_addr, token1_decimals, amount1_wei = _token_amount_wei(
        web3, case.chain, token1_symbol, cfg.get("amount1", 0)
    )

    safe, roles, role_key, _targets = _setup_zodiac_and_apply_manifest(
        case,
        web3=web3,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        role_label=role_label,
    )
    # 2x headroom on each side — LP add can consume slightly more than the
    # exact requested amount under rounding, and approvals must succeed before
    # the mint call is even attempted.
    if amount0_wei > 0:
        _fund_safe_with_token(safe, token0_symbol, amount0_wei * 2, case.chain, anvil_rpc_url)
    if amount1_wei > 0:
        _fund_safe_with_token(safe, token1_symbol, amount1_wei * 2, case.chain, anvil_rpc_url)

    compilation = _compile_for_safe(
        case, safe=safe, anvil_rpc_url=anvil_rpc_url, price_oracle=price_oracle
    )
    assert compilation.status.value == "SUCCESS", f"Compile failed: {compilation.error}"
    bundle = compilation.action_bundle
    assert bundle is not None and bundle.transactions, "ActionBundle must contain at least one tx"

    token0_before = get_token_balance(web3, token0_addr, safe)
    token1_before = get_token_balance(web3, token1_addr, safe)

    _exec_bundle_via_zodiac(
        web3,
        roles,
        role_key,
        bundle.transactions,
        member_eoa=funded_wallet,
        member_private_key=test_private_key,
        should_revert=True,
    )

    token0_after = get_token_balance(web3, token0_addr, safe)
    token1_after = get_token_balance(web3, token1_addr, safe)
    moved = (token0_before - token0_after) > 0 or (token1_before - token1_after) > 0
    assert moved, (
        f"LP_OPEN no-op guard: expected {token0_symbol} or {token1_symbol} to decrease, "
        f"got {token0_symbol}: {token0_before}->{token0_after} (decimals {token0_decimals}), "
        f"{token1_symbol}: {token1_before}->{token1_after} (decimals {token1_decimals})."
    )


def _run_lp_close_positive(
    case: PermissionTestCase,
    *,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
    role_label: str,
) -> None:
    """LP_CLOSE positive path: no pre-funding, assert ≥1 token balance increased.

    Required config keys: ``position_id`` (and ``token0``/``token1`` symbols
    so the harness knows which balances to watch). Amounts are irrelevant —
    we only assert direction, not magnitude.
    """
    cfg = case.config
    token0_symbol = cfg["token0"]
    token1_symbol = cfg["token1"]
    token0_addr, _, _ = _token_amount_wei(web3, case.chain, token0_symbol, 0)
    token1_addr, _, _ = _token_amount_wei(web3, case.chain, token1_symbol, 0)

    safe, roles, role_key, _targets = _setup_zodiac_and_apply_manifest(
        case,
        web3=web3,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        role_label=role_label,
    )

    compilation = _compile_for_safe(
        case, safe=safe, anvil_rpc_url=anvil_rpc_url, price_oracle=price_oracle
    )
    assert compilation.status.value == "SUCCESS", f"Compile failed: {compilation.error}"
    bundle = compilation.action_bundle
    assert bundle is not None and bundle.transactions, "ActionBundle must contain at least one tx"

    token0_before = get_token_balance(web3, token0_addr, safe)
    token1_before = get_token_balance(web3, token1_addr, safe)

    _exec_bundle_via_zodiac(
        web3,
        roles,
        role_key,
        bundle.transactions,
        member_eoa=funded_wallet,
        member_private_key=test_private_key,
        should_revert=True,
    )

    _assert_balances_any_increased(
        web3,
        safe,
        snapshots=[
            (token0_addr, token0_symbol, token0_before),
            (token1_addr, token1_symbol, token1_before),
        ],
        context=f"LP_CLOSE position_id={cfg.get('position_id')!r}",
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

    Flow: deploy Safe + Roles → fund Safe per intent family → generate
    manifest → apply targets → compile intent with ``wallet=safe`` → execute
    each tx via ``execTransactionWithRole`` → assert the family-appropriate
    balance-delta direction.

    A failure here means either (a) the manifest omits a target the compiled
    bundle actually calls, (b) the Zodiac encoder produced wrong selectors /
    clearances / executionOptions, or (c) the compiler changed and the
    manifest generator didn't follow. All three are shipping-blocking.
    """
    label = role_label or f"PermOnchain:{case.protocol}:{case.intent_type}"
    it = case.intent_type.upper()

    if it == "SWAP":
        _run_swap_positive(
            case,
            web3=web3,
            anvil_rpc_url=anvil_rpc_url,
            funded_wallet=funded_wallet,
            test_private_key=test_private_key,
            price_oracle=price_oracle,
            role_label=label,
        )
        return
    if it in {"SUPPLY", "WITHDRAW", "BORROW", "REPAY"}:
        _run_lend_positive(
            case,
            web3=web3,
            anvil_rpc_url=anvil_rpc_url,
            funded_wallet=funded_wallet,
            test_private_key=test_private_key,
            price_oracle=price_oracle,
            role_label=label,
        )
        return
    if it == "LP_OPEN":
        _run_lp_open_positive(
            case,
            web3=web3,
            anvil_rpc_url=anvil_rpc_url,
            funded_wallet=funded_wallet,
            test_private_key=test_private_key,
            price_oracle=price_oracle,
            role_label=label,
        )
        return
    if it == "LP_CLOSE":
        _run_lp_close_positive(
            case,
            web3=web3,
            anvil_rpc_url=anvil_rpc_url,
            funded_wallet=funded_wallet,
            test_private_key=test_private_key,
            price_oracle=price_oracle,
            role_label=label,
        )
        return
    raise NotImplementedError(
        f"Positive path for {it!r} lands in a follow-up phase. "
        "See docs/internal/zodiac-permission-onchain-coverage-plan.md phases B–E."
    )


def run_negative_authorisation_case(
    case: PermissionTestCase,
    *,
    load_bearing_selector: str | None = None,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
    role_label: str | None = None,
) -> None:
    """Assert that revoking a load-bearing target causes the same intent to revert.

    ``load_bearing_selector`` is the 4-byte function selector (``0x...``) of a
    function the compiled bundle must call. When ``None``, the harness falls
    back to ``case.negative_selector``. The harness finds the sole
    function-scoped target carrying that selector and revokes it, then runs
    the same intent and asserts both execution reverts and token conservation.
    """
    selector = load_bearing_selector or case.negative_selector
    if not selector:
        raise ValueError(
            "run_negative_authorisation_case requires a load-bearing selector: pass "
            "``load_bearing_selector=`` explicitly or set "
            "``PermissionTestCase.negative_selector`` on the case."
        )

    label = role_label or f"PermOnchain:{case.protocol}:{case.intent_type}:neg"
    it = case.intent_type.upper()

    safe, roles, role_key, targets = _setup_zodiac_and_apply_manifest(
        case,
        web3=web3,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        role_label=label,
        strategy_suffix="_neg",
    )

    # Fund the Safe's operand tokens identically to the positive path. The
    # negative path must fail on authorisation, not on a missing balance —
    # otherwise the assertion proves nothing about the Roles Modifier.
    snapshot_tokens = _prefund_for_negative(
        case,
        web3=web3,
        safe=safe,
        anvil_rpc_url=anvil_rpc_url,
    )

    load_bearing = _find_target_by_selector(targets, selector)
    revoke_target(
        web3,
        roles,
        safe,
        role_key,
        load_bearing["address"],
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )

    compilation = _compile_for_safe(
        case, safe=safe, anvil_rpc_url=anvil_rpc_url, price_oracle=price_oracle
    )
    assert compilation.status.value == "SUCCESS", (
        f"Compile should still succeed (authz is on-chain only): {compilation.error}"
    )
    bundle = compilation.action_bundle
    assert bundle is not None and bundle.transactions, "ActionBundle must contain at least one tx"

    # Record pre-execution balances for the conservation check.
    balances_before = {
        addr: get_token_balance(web3, addr, safe) for addr, _label in snapshot_tokens
    }

    with pytest.raises(RuntimeError, match="execTransactionWithRole reverted mid-bundle"):
        _exec_bundle_via_zodiac(
            web3,
            roles,
            role_key,
            bundle.transactions,
            member_eoa=funded_wallet,
            member_private_key=test_private_key,
            should_revert=True,
        )

    # Allowance may have landed before the inner call was blocked — allowance
    # is not a balance. We only require that no value moved.
    if it == "SWAP":
        # SWAP uses the bilateral helper for parity with the positive path.
        from_symbol = case.config["from_token"]
        to_symbol = case.config["to_token"]
        from_addr, _, _ = _token_amount_wei(web3, case.chain, from_symbol, 0)
        to_addr, _, _ = _token_amount_wei(web3, case.chain, to_symbol, 0)
        assert_swap_conservation(
            web3,
            from_addr,
            to_addr,
            safe,
            balances_before[from_addr],
            balances_before[to_addr],
        )
    else:
        for addr, token_label in snapshot_tokens:
            after = get_token_balance(web3, addr, safe)
            assert after == balances_before[addr], (
                f"{token_label} balance must be unchanged after failed {it}. "
                f"Before: {balances_before[addr]}, After: {after}. "
                "A balance move past the revoked target means the Zodiac Modifier did not "
                "actually block the call — regression in enforcement or test plumbing."
            )


def _prefund_for_negative(
    case: PermissionTestCase,
    *,
    web3: Web3,
    safe: str,
    anvil_rpc_url: str,
) -> list[tuple[str, str]]:
    """Pre-fund the Safe per-family and return ``[(token_addr, label)]`` to watch.

    Mirrors the positive path's funding so the negative assertion isolates
    the authorisation failure from funding failures. Returns the list of
    token addresses the caller should include in the conservation check.
    """
    it = case.intent_type.upper()
    cfg = case.config

    if it == "SWAP":
        from_symbol = cfg["from_token"]
        to_symbol = cfg["to_token"]
        from_addr, _, amount_wei = _token_amount_wei(
            web3, case.chain, from_symbol, cfg["amount"]
        )
        to_addr, _, _ = _token_amount_wei(web3, case.chain, to_symbol, 0)
        _fund_safe_with_token(safe, from_symbol, amount_wei * 2, case.chain, anvil_rpc_url)
        return [(from_addr, from_symbol), (to_addr, to_symbol)]

    if it in {"SUPPLY", "WITHDRAW", "REPAY"}:
        token_symbol = cfg["token"]
        token_addr, _, amount_wei = _token_amount_wei(
            web3, case.chain, token_symbol, cfg.get("amount", 0)
        )
        fund_wei = amount_wei * 2
        if fund_wei > 0:
            _fund_safe_with_token(safe, token_symbol, fund_wei, case.chain, anvil_rpc_url)
        return [(token_addr, token_symbol)]

    if it == "BORROW":
        collat_symbol = cfg["collateral_token"]
        borrow_symbol = cfg["borrow_token"]
        collat_addr, _, collat_wei = _token_amount_wei(
            web3, case.chain, collat_symbol, cfg["collateral_amount"]
        )
        borrow_addr, _, _ = _token_amount_wei(web3, case.chain, borrow_symbol, 0)
        _fund_safe_with_token(safe, collat_symbol, collat_wei * 2, case.chain, anvil_rpc_url)
        return [(collat_addr, collat_symbol), (borrow_addr, borrow_symbol)]

    if it == "LP_OPEN":
        token0_symbol = cfg["token0"]
        token1_symbol = cfg["token1"]
        token0_addr, _, amount0_wei = _token_amount_wei(
            web3, case.chain, token0_symbol, cfg.get("amount0", 0)
        )
        token1_addr, _, amount1_wei = _token_amount_wei(
            web3, case.chain, token1_symbol, cfg.get("amount1", 0)
        )
        if amount0_wei > 0:
            _fund_safe_with_token(safe, token0_symbol, amount0_wei * 2, case.chain, anvil_rpc_url)
        if amount1_wei > 0:
            _fund_safe_with_token(safe, token1_symbol, amount1_wei * 2, case.chain, anvil_rpc_url)
        return [(token0_addr, token0_symbol), (token1_addr, token1_symbol)]

    if it == "LP_CLOSE":
        token0_symbol = cfg["token0"]
        token1_symbol = cfg["token1"]
        token0_addr, _, _ = _token_amount_wei(web3, case.chain, token0_symbol, 0)
        token1_addr, _, _ = _token_amount_wei(web3, case.chain, token1_symbol, 0)
        return [(token0_addr, token0_symbol), (token1_addr, token1_symbol)]

    raise NotImplementedError(
        f"Negative path for {it!r} lands in a follow-up phase. "
        "See docs/internal/zodiac-permission-onchain-coverage-plan.md phases B–E."
    )


# =============================================================================
# Case discovery (Phase F)
# =============================================================================


_CASES_DIR = Path(__file__).resolve().parent / "permission_cases"


@lru_cache(maxsize=1)
def _get_case_modules() -> tuple[tuple[str, ModuleType], ...]:
    """Return ``(protocol_name, module)`` for every ``permission_cases/<proto>.py``.

    Import-by-path mirrors what the coverage gate does in
    ``tests/unit/permissions/test_onchain_case_coverage.py`` — the two stay in
    lockstep so runtime discovery and the gate agree on which files count.

    Cached via ``lru_cache`` because ``discover_cases`` / ``discover_negative_cases``
    are called at collection time by each per-chain runner (7 chains × 2 calls
    each = 14+ invocations per session), and re-executing every case module on
    every call is pure waste. Returning a tuple (not a generator) so the
    cache works.
    """
    collected: list[tuple[str, ModuleType]] = []
    for case_file in sorted(_CASES_DIR.glob("*.py")):
        if case_file.name == "__init__.py":
            continue
        protocol = case_file.stem
        spec = importlib.util.spec_from_file_location(
            f"_perm_cases_runtime.{protocol}", case_file
        )
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Failed to load spec for {case_file}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        collected.append((protocol, module))
    return tuple(collected)


def _deferred_intent_types(module) -> frozenset[str]:
    """Return the uppercase set of intent types this case file defers at runtime."""
    deferred = getattr(module, "DEFERRED_INTENT_TYPES", ())
    return frozenset(str(t).upper() for t in deferred)


def discover_cases(chain: str) -> list[PermissionTestCase]:
    """Return active cases for ``chain``, honouring per-file ``DEFERRED_INTENT_TYPES``.

    Filters:
      - ``case.chain == chain`` (exact match — ``"bsc"`` does not match ``"bnb"``).
      - ``case.intent_type.upper()`` not in the module's ``DEFERRED_INTENT_TYPES``.

    Sorted deterministically by ``(protocol, intent_type)`` so pytest test
    IDs stay stable across runs. Returns a flat list; the per-chain runner
    parametrizes over it directly.
    """
    target = chain
    collected: list[PermissionTestCase] = []
    for _protocol, module in _get_case_modules():
        cases = getattr(module, "CASES", None)
        if not cases:
            continue
        deferred = _deferred_intent_types(module)
        for case in cases:
            if not isinstance(case, PermissionTestCase):
                continue
            if case.chain != target:
                continue
            if case.intent_type.upper() in deferred:
                continue
            collected.append(case)
    collected.sort(key=lambda c: (c.protocol, c.intent_type.upper()))
    return collected


def discover_negative_cases(chain: str) -> list[PermissionTestCase]:
    """Return ``discover_cases(chain)`` filtered to cases that declare ``negative_selector``.

    The negative-authz runner only makes sense for cases that have declared
    a load-bearing selector on the case itself — callers that still want
    ad-hoc selectors can continue to use ``run_negative_authorisation_case``
    with an explicit ``load_bearing_selector=``.
    """
    return [c for c in discover_cases(chain) if c.negative_selector is not None]
