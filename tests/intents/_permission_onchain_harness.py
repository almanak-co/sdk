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
    SafeOperation,
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
    _exec_safe_tx,
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


class SeedingFailed(RuntimeError):
    """Raised when on-chain state setup fails — distinct from authz failure.

    The triage runbook (``docs/internal/permission-onchain-failure-triage.md``)
    treats these as infrastructure errors, not Zodiac-manifest bugs. A
    ``SeedingFailed`` in the nightly means the pre-test setup (SUPPLY before
    WITHDRAW, LP_OPEN before LP_CLOSE, etc.) could not complete — the Zodiac
    authz assertion has not yet been exercised when this fires, so the failure
    does NOT indicate a manifest/generator regression.
    """


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


# ERC-20 ``approve(address,uint256)`` — the approval selector the manifest
# generator writes onto every operand-token target. The load-bearing target
# the negative test wants to revoke is the protocol's *core* call (swap /
# supply / mint / etc.), NOT the approve. Exclude it when auto-deriving.
_ERC20_APPROVE_SELECTOR = "0x095ea7b3"

# Selectors on "infrastructure" targets that the manifest generator always
# emits but which aren't necessarily hit by every compiled bundle. Revoking
# these produces a false negative (the bundle still succeeds through a
# non-infrastructure path), so they must be excluded from auto-derivation.
#
# - ``0x8d80ff0a`` — Safe MultiSend ``multiSend(bytes)``. Declared on every
#   manifest as a DELEGATECALL target so bundles that DO batch through
#   MultiSend are authorised, but a simple single-leg swap won't hit it.
_SAFE_MULTISEND_SELECTOR = "0x8d80ff0a"

_NON_LOAD_BEARING_SELECTORS = frozenset(
    {
        _ERC20_APPROVE_SELECTOR,
        _SAFE_MULTISEND_SELECTOR,
    }
)


def _auto_derive_load_bearing_selector(targets: list[dict]) -> tuple[str, str] | None:
    """Pick the ``(address, selector)`` pair for the 'load-bearing' negative test.

    Heuristic: scan ``targets`` for function-scoped (``clearance == 2``)
    entries whose selectors are NOT in ``_NON_LOAD_BEARING_SELECTORS`` (ERC-20
    ``approve`` + Safe MultiSend). Among those, pick the one with the lowest
    ``(target_address, selector)`` tuple so the choice is deterministic
    across runs and Python versions.

    Returns the winning ``(address, selector)`` tuple — caller revokes the
    exact target without a secondary lookup. Previously this returned just
    the selector, which forced a second pass via
    ``_find_target_by_selector`` and went ambiguous when two targets shared
    a selector (uncommon but possible — e.g. two router deployments at
    distinct addresses exposing the same core call).

    Returns ``None`` when no candidate exists (manifest is approve-only —
    e.g. a connector whose core call is issued to a wildcard-scoped target).
    The caller should skip the negative test cleanly in that case; there is
    no load-bearing function-scoped target to strip.

    Why function-scoped only: wildcard-scoped (``clearance == 1``) targets
    can't be "selector-revoked" — revoking them removes the whole address,
    which the existing ``revoke_target`` flow already handles. The negative
    path is specifically about proving a *selector-narrowed* permission is
    load-bearing; wildcards don't participate.

    Why exclude ``approve``: every ERC-20 operand target shares the approve
    selector. Revoking it causes the bundle to revert on the first ``approve``
    tx, which proves nothing about whether the core call is gated — the
    negative test would then pass for trivial reasons (approval blocked, not
    the core call). The intent is to prove the *protocol* call is gated.

    Why exclude Safe MultiSend: it's a batching primitive included in every
    manifest as DELEGATECALL so multi-leg bundles CAN batch through it — but
    single-leg bundles (e.g. a bare SWAP) don't hit MultiSend at execution
    time. Revoking MultiSend then leaves the bundle succeeding through the
    non-MultiSend path, which surfaces as "DID NOT RAISE" and hides the real
    signal the negative test exists to produce.
    """
    candidates: list[tuple[str, str]] = []  # (target_address, selector)
    for t in targets:
        if t.get("clearance") != 2:
            continue
        address = t.get("address", "").lower()
        if not address:
            continue
        for fn in t.get("functions", []):
            sel = fn.get("selector", "").lower()
            if sel and sel not in _NON_LOAD_BEARING_SELECTORS:
                candidates.append((address, sel))
    if not candidates:
        return None
    # Deterministic pick: lowest address, then lowest selector. Tuple compare
    # handles both naturally — lexicographic on hex strings matches numeric
    # ordering since every hex string is fixed-width and lowercase here.
    candidates.sort()
    return candidates[0]


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
    """Construct an ``LPCloseIntent`` from the case config.

    ``token0`` / ``token1`` are harness funding hints (used by
    ``_run_lp_close_positive`` to know which balances to watch), not
    LPCloseIntent fields — strip them. Also strip LP_OPEN-side hints
    (``amount0`` / ``amount1`` / ``range_lower`` / ``range_upper``) that the
    harness reuses for the seeding LP_OPEN step; LPCloseIntent does not
    declare them and Pydantic would reject unknown fields.
    """
    drop_keys = set(_LP_FUNDING_KEYS) | {"amount0", "amount1", "range_lower", "range_upper"}
    cfg = {k: v for k, v in case.config.items() if k not in drop_keys}
    return LPCloseIntent(**cfg, protocol=case.protocol, chain=case.chain)


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


def _build_manifest_config(case: PermissionTestCase) -> dict[str, Any]:
    """Translate a case config into the shape the manifest generator expects.

    The manifest generator keys on specific token-field names
    (``_TOKEN_CONFIG_FIELDS`` in the generator) to infer ERC-20 approve
    permissions. Map intent-constructor keys used in case configs onto those
    names so approvals land on the manifest without forcing case files to
    duplicate token fields under alias names.

    Extracted from ``_setup_zodiac_and_apply_manifest`` so the same shape is
    used both for ``_apply_manifest_for_case`` and for future callers that
    need the manifest-config without applying targets.
    """
    manifest_config: dict[str, Any] = dict(case.config)
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
        if "base_token" not in manifest_config and "token0" in case.config:
            manifest_config["base_token"] = case.config["token0"]
        if "quote_token" not in manifest_config and "token1" in case.config:
            manifest_config["quote_token"] = case.config["token1"]
    return manifest_config


def _deploy_and_setup_zodiac(
    web3: Web3,
    funded_wallet: str,
    test_private_key: str,
    *,
    role_label: str,
) -> tuple[str, str, bytes]:
    """Deploy Safe + Roles and assign a per-test role.

    Thin wrapper around ``_setup_zodiac_env`` — exposed so P1 seeding can
    deploy the Safe BEFORE the manifest is applied (seeding runs in between).
    """
    return _setup_zodiac_env(
        web3, funded_wallet, test_private_key, role_label=role_label
    )


def _apply_manifest_for_case(
    case: PermissionTestCase,
    *,
    web3: Web3,
    safe: str,
    roles: str,
    role_key: bytes,
    funded_wallet: str,
    test_private_key: str,
    strategy_suffix: str = "",
) -> list[dict]:
    """Generate the case's manifest and apply its targets to the Roles modifier.

    Returns the ``targets`` list so the negative path can pick a load-bearing
    entry to revoke. Split out of ``_setup_zodiac_and_apply_manifest`` so
    seeding can run between deploy and apply.
    """
    manifest_config = _build_manifest_config(case)
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
    return targets


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

    Retained as a convenience wrapper over ``_deploy_and_setup_zodiac`` +
    ``_apply_manifest_for_case``. Paths that need to interleave seeding
    between the two halves (WITHDRAW, BORROW, REPAY, LP_CLOSE) should call
    them directly.
    """
    safe, roles, role_key = _deploy_and_setup_zodiac(
        web3, funded_wallet, test_private_key, role_label=role_label
    )
    targets = _apply_manifest_for_case(
        case,
        web3=web3,
        safe=safe,
        roles=roles,
        role_key=role_key,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        strategy_suffix=strategy_suffix,
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

    # Phase: deploy Safe + Roles FIRST, seed prior state BEFORE applying the
    # manifest. Seeding runs via Safe.execTransaction (owner-signed), so the
    # absence of manifest targets is the point — we do not want the seeding
    # bundle accidentally authorised and passing the authz assertion trivially.
    safe, roles, role_key = _deploy_and_setup_zodiac(
        web3, funded_wallet, test_private_key, role_label=role_label
    )

    # P1 seeding — land prior on-chain state so WITHDRAW / REPAY have something
    # to operate on. SUPPLY needs no seeding.
    if it == "WITHDRAW":
        _seed_supply(
            case,
            safe=safe,
            web3=web3,
            anvil_rpc_url=anvil_rpc_url,
            funded_wallet=funded_wallet,
            test_private_key=test_private_key,
            price_oracle=price_oracle,
        )
    elif it == "REPAY":
        _seed_supply_then_borrow(
            case,
            safe=safe,
            web3=web3,
            anvil_rpc_url=anvil_rpc_url,
            funded_wallet=funded_wallet,
            test_private_key=test_private_key,
            price_oracle=price_oracle,
        )

    # NOW apply the manifest — the case intent is the only thing the Zodiac
    # Roles Modifier must authorise.
    _apply_manifest_for_case(
        case,
        web3=web3,
        safe=safe,
        roles=roles,
        role_key=role_key,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
    )

    # Even WITHDRAW funds the operand: the connector path SUPPLY-then-WITHDRAW
    # within a single compiled bundle is protocol-dependent. Ensure the Safe
    # owns the operand token so approvals / transferFroms don't fail for a
    # reason unrelated to authorisation. After seeding, WITHDRAW usually has
    # shares but the adapter may still call transferFrom for partial-state
    # protocols — fund defensively.
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

    Protocol shape drives the seeding strategy:

    - **compound_v3**: the compiled BORROW bundle is *atomic* — a single bundle
      emits ``approve`` + ``supplyCollateral`` + ``borrow``. The collateral
      does not need to pre-exist as a supplied position on the Comet because
      the same tx supplies it. The manifest authorises all three calls. No
      seeding helper.

    - **aave_v3 / spark / morpho_blue / radiant_v2**: the compiled BORROW
      bundle ONLY calls ``borrow`` (plus any debt-token approvals). The
      protocol requires a prior collateral supply on the lending pool —
      without it the borrow reverts for lack of account collateral, not for
      authorisation. Seed collateral via ``_seed_supply_collateral`` BEFORE
      the manifest is applied so the manifest authorises only the BORROW.
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

    # Deploy Safe + Roles first; seed (if needed) BEFORE the manifest is
    # applied; THEN apply the manifest. Collateral seeding is a plain SUPPLY
    # tx executed by the Safe owner — Zodiac is not involved until step 3.
    safe, roles, role_key = _deploy_and_setup_zodiac(
        web3, funded_wallet, test_private_key, role_label=role_label
    )

    if case.protocol != "compound_v3":
        _seed_borrow_collateral(
            case,
            safe=safe,
            web3=web3,
            anvil_rpc_url=anvil_rpc_url,
            funded_wallet=funded_wallet,
            test_private_key=test_private_key,
            price_oracle=price_oracle,
        )

    _apply_manifest_for_case(
        case,
        web3=web3,
        safe=safe,
        roles=roles,
        role_key=role_key,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
    )
    # 2x headroom mirrors SWAP — covers rounding + any transient buffering the
    # compiler/connector adds before the borrow executes. Atomic compound_v3
    # BORROW needs its collateral on the Safe so supplyCollateral succeeds;
    # non-atomic connectors already supplied via the seed but keep the funding
    # for any residual transferFrom a different compile path might emit.
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
    """LP_CLOSE positive path: seed an LP_OPEN first, then close the minted position.

    The CLOSE case config must carry ``token0`` / ``token1`` / ``pool`` so the
    seed step can execute an LP_OPEN against the same pool. The returned
    position identity (tokenId for V3-style, pool address for Aerodrome
    classic, bin_ids for TJv2 LB) is merged into the CLOSE config BEFORE the
    CLOSE intent is compiled. Seeding runs via Safe.execTransaction (no
    Zodiac); the manifest is applied AFTER seeding so it only authorises
    the CLOSE tx.

    Asserts at least one of ``token0`` / ``token1`` balance increases on the
    Safe (principal + fees returned). Amounts are irrelevant — direction only.
    """
    cfg = case.config
    token0_symbol = cfg["token0"]
    token1_symbol = cfg["token1"]
    token0_addr, _, _ = _token_amount_wei(web3, case.chain, token0_symbol, 0)
    token1_addr, _, _ = _token_amount_wei(web3, case.chain, token1_symbol, 0)

    # Deploy Safe + Roles, seed an LP_OPEN via Safe (no Zodiac), then apply
    # the CLOSE manifest. The seeded OPEN mints the position whose identity
    # is merged into the CLOSE case config.
    safe, roles, role_key = _deploy_and_setup_zodiac(
        web3, funded_wallet, test_private_key, role_label=role_label
    )
    position_identity = _seed_lp_position(
        case,
        safe=safe,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
    )

    # Rebuild the case with position identity merged in. PermissionTestCase
    # is frozen — construct a new one instead of mutating.
    merged_cfg = dict(cfg)
    merged_cfg.update(position_identity)
    close_case = PermissionTestCase(
        chain=case.chain,
        protocol=case.protocol,
        intent_type=case.intent_type,
        config=merged_cfg,
        negative_selector=case.negative_selector,
    )

    _apply_manifest_for_case(
        close_case,
        web3=web3,
        safe=safe,
        roles=roles,
        role_key=role_key,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
    )

    compilation = _compile_for_safe(
        close_case, safe=safe, anvil_rpc_url=anvil_rpc_url, price_oracle=price_oracle
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
        context=f"LP_CLOSE position={position_identity!r}",
    )


# =============================================================================
# Seeding helpers — compile-then-execute via Safe.execTransaction (no Zodiac)
# =============================================================================
#
# Seeding exists so WITHDRAW / BORROW / REPAY and LP_CLOSE can run against a
# freshly deployed Safe. Fresh Safe => no aToken / cToken / shares / NFT
# position => the authz test would revert for a STATE reason, not a
# MANIFEST reason. That makes the test meaningless.
#
# Design: seeding reuses the same IntentCompiler as the real test
# (``wallet_address=safe``), so the compiled bundle references the same tokens
# and targets the real authz test will touch. But seeding executes via
# ``Safe.execTransaction`` (owner-signed, pre-validated signature) rather than
# ``Roles.execTransactionWithRole``. Two consequences:
#
# 1. The Zodiac Roles Modifier does not gate the seeding bundle. This is
#    deliberate: seeding runs BEFORE the manifest is applied (see step order
#    in ``run_positive_authorisation_case``), so there is no role to check
#    against — but even if there were, seeding is "the Safe owner did a
#    supply", not "the agent did an authorised action".
# 2. Any revert inside a seeding tx surfaces as ``SeedingFailed`` — NOT as a
#    Zodiac authz failure. This keeps the triage bucket clean: if seeding
#    breaks, it's infra / adapter / price-oracle staleness; if the post-
#    seeding test breaks, it's a genuine manifest/generator regression.


def _exec_bundle_via_safe(
    web3: Web3,
    safe: str,
    bundle_txs: Sequence[Any],
    *,
    owner_eoa: str,
    owner_private_key: str,
) -> list[dict]:
    """Submit each tx in a compiled ActionBundle via ``Safe.execTransaction``.

    Owner-signed (pre-validated v=1 signature) — the Zodiac Roles Modifier is
    NOT consulted. Used exclusively by seeding paths. Mirrors the operation-
    type decision (CALL vs DELEGATE_CALL) that
    ``_exec_bundle_via_zodiac`` makes, so Enso-delegate seeding paths (if any
    materialise) stay consistent with the authz path.

    Raises ``SeedingFailed`` on any revert; distinct from authz reverts so
    triage does not mis-file infra breakage as a manifest regression.
    """
    receipts: list[dict] = []
    for tx in bundle_txs:
        # Mirror _exec_bundle_via_zodiac's tx-normalisation so dataclass-ish
        # UnsignedTransaction instances and plain dicts both work.
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

        op_type = get_operation_type(to_addr)
        safe_op = SafeOperation(int(op_type))
        try:
            receipt = _exec_safe_tx(
                web3,
                safe,
                to_addr,
                data,
                safe_op,
                owner_eoa,
                owner_private_key,
                value=value,
            )
        except Exception as e:  # noqa: BLE001 — wrap anything that escapes the Safe tx
            # estimate_gas can raise ContractLogicError (GS013 = Safe inner revert)
            # before the tx is submitted; _exec_safe_tx itself raises RuntimeError
            # on a post-submission receipt revert. Both land here so the triage
            # bucket stays "seeding failed" (not "authz failed") regardless of
            # which web3.py layer surfaces the revert.
            raise SeedingFailed(
                f"Seeding tx reverted via Safe.execTransaction (to={to_addr}). "
                f"The pre-test state setup could not complete — this is infrastructure, "
                f"not an authz regression. Original: {type(e).__name__}: {e}"
            ) from e
        receipts.append(receipt)
    return receipts


def _compile_and_seed(
    case_like: PermissionTestCase,
    *,
    safe: str,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
    step_label: str,
) -> list[dict]:
    """Compile ``case_like``'s intent (with ``wallet=safe``) and execute via Safe.

    Returns the list of receipts (one per inner tx). Any compile failure or
    revert raises ``SeedingFailed`` with ``step_label`` in the message.
    """
    compilation = _compile_for_safe(
        case_like, safe=safe, anvil_rpc_url=anvil_rpc_url, price_oracle=price_oracle
    )
    if compilation.status.value != "SUCCESS":
        raise SeedingFailed(
            f"Seeding step {step_label!r}: compile failed: {compilation.error}"
        )
    bundle = compilation.action_bundle
    if bundle is None or not bundle.transactions:
        raise SeedingFailed(
            f"Seeding step {step_label!r}: empty ActionBundle — compiler did not emit any tx"
        )
    return _exec_bundle_via_safe(
        web3,
        safe,
        bundle.transactions,
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )


def _seed_supply(
    case: PermissionTestCase,
    *,
    safe: str,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Supply the case's ``token`` so aToken / cToken / shares exist on the Safe.

    Compiles a ``SupplyIntent`` mirroring the case's protocol/chain/token and
    executes via ``Safe.execTransaction`` (owner-signed, no Zodiac). Funds the
    Safe with the supply amount first so the inner ``transferFrom`` succeeds.

    Used by WITHDRAW positive path.
    """
    cfg = case.config
    # Scale the seeding SUPPLY to 2x the WITHDRAW amount so the subsequent
    # WITHDRAW has strict headroom — adapter-side rounding / dust on Aave
    # shares can otherwise pin the aToken balance just below the requested
    # withdraw amount.
    withdraw_amount = Decimal(str(cfg.get("amount", "0")))
    supply_amount = withdraw_amount * Decimal(2) if withdraw_amount > 0 else Decimal("100")
    seed_cfg: dict[str, Any] = {"token": cfg["token"], "amount": str(supply_amount)}
    # Preserve market_id / anything else the case carries that is not an
    # "amount" override — the SUPPLY must land on the same market the
    # WITHDRAW will target.
    for key, value in cfg.items():
        if key in {"token", "amount"}:
            continue
        # WITHDRAW-only flags that would break SUPPLY construction:
        if key == "is_collateral":
            continue
        # use_as_collateral is set protocol-by-protocol below — skip it here
        # so the per-protocol block is the sole source of truth.
        if key == "use_as_collateral":
            continue
        seed_cfg[key] = value

    # Protocol-specific use_as_collateral semantics:
    # - aave_v3 / spark / radiant_v2: first supply auto-enables collateral
    #   at the reserve level; the default compiler path also emits a
    #   setUserUseReserveAsCollateral call that can revert as no-op. We
    #   only care about the aToken existing for WITHDRAW, not the
    #   collateral flag, so request False on the seed.
    # - morpho_blue: routes to supply (not supply_collateral) — the WITHDRAW
    #   case declares use_as_collateral=False in its cfg; preserve that
    #   intent for the seed too (without this branch, the compiler default
    #   True would route to supply_collateral which is the wrong side).
    if case.protocol in {"aave_v3", "spark", "radiant_v2"}:
        seed_cfg["use_as_collateral"] = False
    elif case.protocol == "morpho_blue":
        # Mirror the case's own flag for the loan-token vs collateral-token
        # split. If the WITHDRAW targets the loan token, seed the loan
        # token with use_as_collateral=False. The case file already sets
        # is_collateral=False on the WITHDRAW — mirror it on the SUPPLY.
        seed_cfg["use_as_collateral"] = bool(cfg.get("is_collateral", False))

    seed_case = PermissionTestCase(
        chain=case.chain,
        protocol=case.protocol,
        intent_type="SUPPLY",
        config=seed_cfg,
    )

    token_symbol = cfg["token"]
    _, _, supply_wei = _token_amount_wei(web3, case.chain, token_symbol, supply_amount)
    # 2x headroom mirrors the positive paths — covers approve + adapter rounding.
    _fund_safe_with_token(safe, token_symbol, supply_wei * 2, case.chain, anvil_rpc_url)

    _compile_and_seed(
        seed_case,
        safe=safe,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
        step_label=f"SUPPLY seed for {case.protocol} {case.intent_type}",
    )


def _seed_borrow_collateral(
    case: PermissionTestCase,
    *,
    safe: str,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Seed ONLY the collateral side for a subsequent BORROW intent.

    Compiles a ``SupplyIntent`` for ``collateral_token`` / ``collateral_amount``
    and executes via Safe. Leaves the Safe with collateral but NO outstanding
    debt — exactly the state the BORROW test needs.

    Used by non-atomic BORROW positive paths (aave_v3, spark, morpho_blue,
    radiant_v2). Not used by compound_v3 — its BORROW bundle supplies
    collateral in-line.
    """
    cfg = case.config
    collat_symbol = cfg["collateral_token"]
    collat_amount = Decimal(str(cfg["collateral_amount"]))

    seed_cfg: dict[str, Any] = {
        "token": collat_symbol,
        "amount": str(collat_amount),
    }
    # Carry market_id for protocols that need it (morpho_blue).
    if "market_id" in cfg:
        seed_cfg["market_id"] = cfg["market_id"]
    # Protocol-specific ``use_as_collateral`` semantics:
    #
    # - **aave_v3 / spark / radiant_v2**: first-supply auto-enables the asset
    #   as collateral at the reserve level. The compiler's default
    #   ``use_as_collateral=True`` THEN emits an extra
    #   ``setUserUseReserveAsCollateral(asset, true)`` call that reverts
    #   because the state would not change (the reserve's collateral flag is
    #   already true). Set False on the SEED path so the compiled bundle
    #   stops at approve+supply — the collateral is still available for the
    #   subsequent BORROW because the supply itself enabled it.
    # - **morpho_blue**: isolated-market; ``use_as_collateral=True`` routes
    #   to ``supply_collateral`` (not ``supply``), which is what the BORROW
    #   test needs.
    if case.protocol == "morpho_blue":
        seed_cfg["use_as_collateral"] = True
    elif case.protocol in {"aave_v3", "spark", "radiant_v2"}:
        seed_cfg["use_as_collateral"] = False

    seed_case = PermissionTestCase(
        chain=case.chain,
        protocol=case.protocol,
        intent_type="SUPPLY",
        config=seed_cfg,
    )

    _, _, collat_wei = _token_amount_wei(web3, case.chain, collat_symbol, collat_amount)
    _fund_safe_with_token(safe, collat_symbol, collat_wei * 2, case.chain, anvil_rpc_url)

    _compile_and_seed(
        seed_case,
        safe=safe,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
        step_label=f"collateral SUPPLY seed for {case.protocol} BORROW",
    )


def _seed_supply_then_borrow(
    case: PermissionTestCase,
    *,
    collateral_token_symbol: str | None = None,
    safe: str,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Supply collateral then borrow the target token — leaves outstanding debt.

    Used to set up REPAY state. The seeded SUPPLY and BORROW target the same
    tokens the compiled REPAY intent will touch, so the manifest (applied
    later) authorises the actual REPAY tx, not the setup.

    ``collateral_token_symbol`` defaults to the first candidate from the
    case's ``collateral_token`` / ``config["collateral_token"]`` if present,
    else a sensible default (``WETH``) to pair against a stablecoin borrow.
    """
    cfg = case.config
    borrow_symbol = cfg["token"]
    # REPAY amount is what the test repays — we need at least that much debt
    # outstanding so the repay succeeds with a positive decrease. Borrow 2x
    # the repay amount for headroom; accrued interest since the borrow block
    # otherwise pushes the aToken debt slightly higher than the repay side.
    repay_amount = Decimal(str(cfg.get("amount", "0")))
    borrow_amount_dec = repay_amount * Decimal(2) if repay_amount > 0 else Decimal("100")

    # Pick the collateral token. For stablecoin REPAYs (USDC), borrow
    # against WETH. morpho_blue carries market_id that fixes the
    # collateral/loan pair — use it.
    if collateral_token_symbol is None:
        collateral_token_symbol = "WETH" if borrow_symbol in {"USDC", "USDT", "DAI"} else "USDC"

    # Size collateral by USD value so mixed-token pairs (e.g. WETH borrow
    # against USDC collateral) don't trip the connector LTV cap. Target
    # ~20% LTV — well under the 30% cap in .claude/rules/intent-tests.md
    # and resilient to block-to-block oracle drift on the fork.
    #
    # ``price_oracle`` is a ``dict[str, Decimal]`` (symbol → USD price) —
    # see ``_create_price_oracle_fixture`` in ``tests/intents/conftest.py``.
    # If either price is missing or non-positive, fall back to the original
    # unit-based heuristic (safe only when both tokens share a unit scale,
    # e.g. stablecoin-on-stablecoin).
    _TARGET_LTV = Decimal("0.20")
    borrow_price = (
        price_oracle.get(borrow_symbol) if isinstance(price_oracle, dict) else None
    )
    collateral_price = (
        price_oracle.get(collateral_token_symbol)
        if isinstance(price_oracle, dict)
        else None
    )
    if (
        borrow_price is not None
        and collateral_price is not None
        and borrow_price > 0
        and collateral_price > 0
    ):
        collateral_value_usd = borrow_amount_dec * Decimal(str(borrow_price)) / _TARGET_LTV
        collateral_amount_dec = collateral_value_usd / Decimal(str(collateral_price))
        # Guard against sub-unit WETH rounding — 1 WETH is always safe headroom.
        if collateral_token_symbol == "WETH" and collateral_amount_dec < Decimal("1"):
            collateral_amount_dec = Decimal("1")
    else:
        print(
            f"[_seed_supply_then_borrow] price_oracle missing USD price for "
            f"{borrow_symbol}={borrow_price!r} or {collateral_token_symbol}="
            f"{collateral_price!r} — falling back to unit-based sizing."
        )
        # Legacy heuristic: 1 WETH against any stablecoin debt, else 10x units.
        # Correct only when borrow and collateral share a unit scale.
        collateral_amount_dec = (
            Decimal("1")
            if collateral_token_symbol == "WETH"
            else borrow_amount_dec * Decimal(10)
        )

    # 1. SUPPLY collateral.
    supply_cfg: dict[str, Any] = {
        "token": collateral_token_symbol,
        "amount": str(collateral_amount_dec),
    }
    if "market_id" in cfg:
        supply_cfg["market_id"] = cfg["market_id"]
    # See ``_seed_borrow_collateral`` — same protocol-specific semantics.
    if case.protocol == "morpho_blue":
        supply_cfg["use_as_collateral"] = True
    elif case.protocol in {"aave_v3", "spark", "radiant_v2"}:
        supply_cfg["use_as_collateral"] = False
    supply_case = PermissionTestCase(
        chain=case.chain,
        protocol=case.protocol,
        intent_type="SUPPLY",
        config=supply_cfg,
    )

    _, _, collat_wei = _token_amount_wei(
        web3, case.chain, collateral_token_symbol, collateral_amount_dec
    )
    _fund_safe_with_token(safe, collateral_token_symbol, collat_wei * 2, case.chain, anvil_rpc_url)

    _compile_and_seed(
        supply_case,
        safe=safe,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
        step_label=f"collateral SUPPLY seed for {case.protocol} REPAY",
    )

    # 2. BORROW the target token — leaves an outstanding debt position.
    borrow_cfg: dict[str, Any] = {
        "collateral_token": collateral_token_symbol,
        "collateral_amount": str(collateral_amount_dec),
        "borrow_token": borrow_symbol,
        "borrow_amount": str(borrow_amount_dec),
    }
    if "market_id" in cfg:
        borrow_cfg["market_id"] = cfg["market_id"]
    borrow_case = PermissionTestCase(
        chain=case.chain,
        protocol=case.protocol,
        intent_type="BORROW",
        config=borrow_cfg,
    )
    _compile_and_seed(
        borrow_case,
        safe=safe,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
        step_label=f"BORROW seed for {case.protocol} REPAY",
    )


def _seed_lp_position(
    case: PermissionTestCase,
    *,
    safe: str,
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> dict[str, Any]:
    """Execute LP_OPEN via Safe, parse the mint event, return position identity.

    Returns a dict containing whatever the protocol family needs to identify
    the position for the subsequent CLOSE:

    - Uniswap-V3-style (uniswap_v3, pancakeswap_v3, sushiswap_v3):
      ``{"position_id": str(tokenId)}`` — LPCloseIntent.position_id is a str.
    - Aerodrome (classic Solidly, fungible): ``{"position_id": pool_address}``.
    - TraderJoe V2 LB (bin-based, fungible): ``{"position_id": "tjv2",
      "protocol_params": {"bin_ids": [...]}}`` — the compiler reads
      protocol_params for bin_ids and uses intent.pool for the pair.

    The returned dict is merged into the CLOSE case's config BEFORE the CLOSE
    intent is compiled.

    Raises ``SeedingFailed`` if the mint reverts or the expected event is missing.
    """
    # Build the LP_OPEN seed config from the LP_CLOSE case. The case file's
    # LP_CLOSE config carries ``token0`` / ``token1`` / ``pool`` / (optionally)
    # ``amount0`` / ``amount1`` / ``range_lower`` / ``range_upper`` so the
    # open-then-close sequence uses the same pool.
    cfg = case.config
    seed_cfg: dict[str, Any] = {
        "token0": cfg["token0"],
        "token1": cfg["token1"],
        "pool": cfg["pool"],
        "amount0": cfg.get("amount0", "100"),
        "amount1": cfg.get("amount1", "0.05"),
        "range_lower": cfg.get("range_lower", "1500"),
        "range_upper": cfg.get("range_upper", "4000"),
    }
    seed_case = PermissionTestCase(
        chain=case.chain,
        protocol=case.protocol,
        intent_type="LP_OPEN",
        config=seed_cfg,
    )

    # Pre-fund both sides — same shape as _run_lp_open_positive.
    token0_symbol = seed_cfg["token0"]
    token1_symbol = seed_cfg["token1"]
    _, _, amount0_wei = _token_amount_wei(web3, case.chain, token0_symbol, seed_cfg["amount0"])
    _, _, amount1_wei = _token_amount_wei(web3, case.chain, token1_symbol, seed_cfg["amount1"])
    if amount0_wei > 0:
        _fund_safe_with_token(safe, token0_symbol, amount0_wei * 2, case.chain, anvil_rpc_url)
    if amount1_wei > 0:
        _fund_safe_with_token(safe, token1_symbol, amount1_wei * 2, case.chain, anvil_rpc_url)

    receipts = _compile_and_seed(
        seed_case,
        safe=safe,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
        step_label=f"LP_OPEN seed for {case.protocol} LP_CLOSE",
    )
    return _extract_lp_position_identity(case.protocol, case.chain, receipts)


def _extract_lp_position_identity(
    protocol: str,
    chain: str,
    receipts: list[dict],
) -> dict[str, Any]:
    """Parse the LP_OPEN receipts and return the protocol-specific position identity.

    Isolated from ``_seed_lp_position`` so protocol branches stay readable.
    The caller merges this dict into the CLOSE case config before compilation.
    """
    # The mint event lives on the last receipt (after approvals). Search all
    # receipts in order so parser-emitting-on-earlier-tx cases also work.
    if protocol in {"uniswap_v3", "pancakeswap_v3", "sushiswap_v3"}:
        # Each protocol has its own receipt parser — pick dynamically so we
        # don't import three modules unconditionally.
        if protocol == "uniswap_v3":
            from almanak.framework.connectors.uniswap_v3.receipt_parser import (
                UniswapV3ReceiptParser as _Parser,
            )
        elif protocol == "pancakeswap_v3":
            from almanak.framework.connectors.pancakeswap_v3.receipt_parser import (
                PancakeSwapV3ReceiptParser as _Parser,
            )
        else:
            from almanak.framework.connectors.sushiswap_v3.receipt_parser import (
                SushiSwapV3ReceiptParser as _Parser,
            )
        parser = _Parser(chain=chain)
        for receipt in reversed(receipts):
            token_id = parser.extract_position_id(dict(receipt))
            if token_id is not None:
                # LPCloseIntent.position_id is a str — stringify for the V3
                # family even though the source is a uint256.
                return {"position_id": str(token_id)}
        raise SeedingFailed(
            f"LP_OPEN seed for {protocol} did not emit an NFT mint (Transfer from 0x0) event — "
            "cannot extract tokenId for the subsequent CLOSE."
        )

    if protocol == "aerodrome":
        from almanak.framework.connectors.aerodrome.receipt_parser import (
            AerodromeReceiptParser,
        )

        parser = AerodromeReceiptParser(chain=chain)
        for receipt in reversed(receipts):
            pool_addr = parser.extract_position_id(dict(receipt))
            if pool_addr:
                # Aerodrome classic LP_CLOSE accepts a bare pool address as
                # the position_id (see compile_lp_close_aerodrome branch).
                return {"position_id": pool_addr}
        raise SeedingFailed(
            "LP_OPEN seed for aerodrome did not emit a Mint event with a pool address — "
            "cannot identify the fungible LP position for the subsequent CLOSE."
        )

    if protocol == "traderjoe_v2":
        # TJv2 LP positions are ERC-1155 (Liquidity Book Tokens) keyed by
        # (pair, bin_id). The compiler reads protocol_params["bin_ids"] and
        # uses intent.pool for the pair. The case's LP_CLOSE config carries
        # pool already — we only need bin_ids from the mint receipt.
        from almanak.framework.connectors.traderjoe_v2.receipt_parser import (
            TraderJoeV2ReceiptParser,
        )

        parser = TraderJoeV2ReceiptParser()
        for receipt in reversed(receipts):
            bin_ids = parser.extract_bin_ids(dict(receipt))
            if bin_ids:
                # position_id is a required str on LPCloseIntent — pass a
                # descriptive sentinel; the TJv2 compiler ignores it and
                # keys on pool + protocol_params["bin_ids"].
                return {
                    "position_id": "tjv2-lb-position",
                    "protocol_params": {"bin_ids": bin_ids},
                }
        raise SeedingFailed(
            "LP_OPEN seed for traderjoe_v2 did not emit a DepositedToBins event with bin IDs — "
            "cannot identify the LBT position for the subsequent CLOSE."
        )

    raise SeedingFailed(
        f"LP position-identity extraction not implemented for protocol {protocol!r}. "
        "Add a branch in _extract_lp_position_identity when wiring a new LP connector."
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

    Selector resolution order:
      1. ``load_bearing_selector`` kwarg (explicit — pilot path).
      2. ``case.negative_selector`` (explicit on the case file).
      3. Auto-derivation from the generated manifest: pick the
         ``(address, selector)`` tuple with the lowest ordering among
         function-scoped targets whose selector is not ERC-20 ``approve``.

    When auto-derivation returns ``None`` (manifest has no load-bearing
    non-approve function-scoped target), the test is skipped with a clear
    message — there is nothing to strip, so the assertion would be vacuous.

    Paths (1) and (2) locate the target via ``_find_target_by_selector``
    (unambiguous by construction: the case/pilot author knows which target
    carries that selector). Path (3) returns both ``(address, selector)`` up
    front, so the harness revokes that address directly — no second lookup,
    no ambiguity if two manifest targets ever share a selector.
    """
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

    # Resolve the selector AFTER manifest generation so auto-derivation can
    # introspect ``targets``. Explicit selectors (kwarg or case-declared) still
    # take precedence — they're the belt-and-suspenders path for the pilot.
    explicit_selector = load_bearing_selector or case.negative_selector
    auto_derived_address: str | None = None
    if explicit_selector:
        selector = explicit_selector
    else:
        derived = _auto_derive_load_bearing_selector(targets)
        if derived is None:
            pytest.skip(
                f"No load-bearing non-approve target in manifest for "
                f"({case.protocol}, {case.intent_type}) on {case.chain} — "
                "nothing to strip. Likely an approve-only or wildcard-only manifest."
            )
        auto_derived_address, selector = derived

    # Fund the Safe's operand tokens identically to the positive path. The
    # negative path must fail on authorisation, not on a missing balance —
    # otherwise the assertion proves nothing about the Roles Modifier.
    snapshot_tokens = _prefund_for_negative(
        case,
        web3=web3,
        safe=safe,
        anvil_rpc_url=anvil_rpc_url,
    )

    # Auto-derivation hands us the exact address — use it directly. The
    # explicit-selector paths still do the ``_find_target_by_selector`` lookup
    # so a typo in a case file fails loudly rather than revoking the wrong
    # target.
    if auto_derived_address is not None:
        target_address = auto_derived_address
    else:
        target_address = _find_target_by_selector(targets, selector)["address"]
    revoke_target(
        web3,
        roles,
        safe,
        role_key,
        target_address,
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
    """Return every active case for ``chain`` — each is a negative-test candidate.

    Previously this helper filtered to cases declaring ``negative_selector``
    explicitly. That stopped making sense once ``run_negative_authorisation_case``
    learned to auto-derive a load-bearing selector from the manifest: every
    active case is now a candidate, and the per-case outcome (run or skip)
    is decided at execution time by inspecting the generated manifest.

    Static discovery can't predict the introspection outcome without
    generating the manifest — that would defeat the point of a cheap
    collection-time helper. So we return all active cases and let the harness
    ``pytest.skip`` cleanly when no non-approve target exists to strip.

    The per-chain runners now parametrize both positive and negative tests
    over the same list, which keeps the two paths in lockstep: any case the
    positive path covers, the negative path either runs or skips with a clear
    reason. No silent divergence.
    """
    return discover_cases(chain)
