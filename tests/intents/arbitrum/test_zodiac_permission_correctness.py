"""Arbitrum on-chain permission authorisation pilot.

Each test here proves that the manifest returned by
``almanak.framework.permissions.generator.generate_manifest()`` actually
authorises — or, in the negative case, is load-bearing enough to block —
a compiled intent when applied to a real Zodiac Roles Modifier on the
Anvil fork.

Pre-Phase-G.4 the test payloads were sourced from
``tests/intents/permission_cases/<protocol>.py`` (now retired). Under
the default-on Zodiac model that supersedes the parallel runtime,
the canonical authorisation surface is the regular intent-test matrix
under ``tests/intents/<chain>/`` — every test there runs through Safe +
Roles + ``execTransactionWithRole`` automatically. This pilot is kept
as the **known-good anchor**: it deploys Safe + Roles directly and
exercises ``run_positive_authorisation_case`` /
``run_negative_authorisation_case`` against a single inline-constructed
``PermissionTestCase``, so a regression in the harness primitives surfaces
here even if the fixture path silently changes shape.

Plan doc:
``docs/internal/zodiac-permission-onchain-coverage-plan.md`` (Phase G.4
keep list — the pilot is one of the surfaces explicitly retained).

Unlike the 4-layer intent tests (``.claude/rules/intent-tests.md``), these
verify **authorisation**, not intent semantics — the 4-layer correctness
is already covered by the existing per-protocol intent tests. Each test
uses ``# noqa: layers`` with a pointer back to the plan doc.
"""

from __future__ import annotations

import pytest
from eth_account import Account
from web3 import Web3

from almanak.connectors.uniswap_v3.sdk import EXACT_INPUT_SINGLE_SELECTOR
from almanak.framework.execution.signer.safe.constants import (
    ZODIAC_EXEC_TRANSACTION_WITH_ROLE_ABI,
    SafeOperation,
)
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._permission_onchain_harness import (
    PermissionTestCase,
    run_negative_authorisation_case,
    run_positive_authorisation_case,
)
from tests.intents._zodiac_helpers import (
    apply_manifest_targets,
    assign_role_to_member,
    deploy_test_safe,
    deploy_test_zodiac_roles,
)
from tests.intents.conftest import CHAIN_CONFIGS

pytestmark = pytest.mark.no_zodiac(reason="Pilot test deploys its own Safe+Roles; conftest fixture would conflict")

CHAIN_NAME = "arbitrum"


def _select_case(chain: str, intent_type: str) -> PermissionTestCase:
    """Build the canonical uniswap_v3 case for ``(chain, intent_type)``.

    Pre-Phase-G.4 this read from ``permission_cases/uniswap_v3.py`` (now
    retired). The pilot only ever needed one case (arbitrum SWAP), so we
    inline its construction here. Calls for any other ``(chain, intent_type)``
    combo raise — this fixture is intentionally minimal, not a generic
    case registry.
    """
    if chain == "arbitrum" and intent_type.upper() == "SWAP":
        return PermissionTestCase(
            chain="arbitrum",
            protocol="uniswap_v3",
            intent_type="SWAP",
            config={"from_token": "USDC", "to_token": "WETH", "amount": "100"},
        )
    raise AssertionError(
        f"Pilot only knows the arbitrum SWAP case; got ({chain!r}, {intent_type!r}). "
        f"Add the construction inline if you need another."
    )


# =============================================================================
# Plumbing smoke test
# =============================================================================

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


@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.arbitrum
@pytest.mark.asyncio
async def test_zodiac_plumbing_allows_approve_on_wildcarded_usdc(  # noqa: layers
    web3: Web3,
    funded_wallet: str,
    test_private_key: str,
) -> None:
    """Integration spike: Safe deploy → Roles deploy → assignRoles → allowTarget → approve.

    # noqa: layers — authorisation tests, not intent-semantics tests. See
    # ``docs/internal/zodiac-permission-onchain-coverage-plan.md``.

    Exercises every primitive the harness depends on before the manifest-
    driven tests layer on top:

    1. Deploy Safe v1.4.1 with ``funded_wallet`` as sole owner (threshold 1).
    2. Deploy Roles Modifier v2 with owner=avatar=target=Safe; enable on Safe.
    3. Grant ``funded_wallet`` membership of a test role + set it as default.
    4. ``allowTarget(role_key, USDC, ExecutionOptions.None)`` — wildcard USDC.
    5. From ``funded_wallet`` EOA, call
       ``Roles.execTransactionWithRole(USDC, 0, approve(spender, 1 USDC),
       CALL, role_key, shouldRevert=true)``.
    6. Assert ``USDC.allowance(safe, spender) == 1_000_000`` (1 USDC in 6dp).
    """
    usdc_address = Web3.to_checksum_address(CHAIN_CONFIGS[CHAIN_NAME]["tokens"]["USDC"])
    role_key = b"PermOnchainPlumbing".ljust(32, b"\0")
    arbitrary_spender = Web3.to_checksum_address("0x00000000000000000000000000000000DEAdBEef")
    approve_amount = 1_000_000  # 1 USDC, 6 decimals

    safe = deploy_test_safe(web3, funded_wallet, test_private_key)
    roles = deploy_test_zodiac_roles(web3, safe, funded_wallet, test_private_key)
    assert roles != safe
    assign_role_to_member(
        web3,
        roles,
        safe,
        role_key,
        member_eoa=funded_wallet,
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )
    apply_manifest_targets(
        web3,
        roles,
        safe,
        role_key,
        targets=[{"address": usdc_address, "clearance": 1, "executionOptions": 0}],
        owner_eoa=funded_wallet,
        owner_private_key=test_private_key,
    )

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
        True,
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

    allowance = usdc.functions.allowance(safe, arbitrary_spender).call()
    assert allowance == approve_amount, (
        f"Expected Safe.USDC.allowance({arbitrary_spender}) == {approve_amount}, got {allowance}. "
        f"Safe={safe}, Roles={roles}. Zodiac routed the approve but the Safe's allowance did not update — "
        "this means the approve was sent but from the wrong msg.sender (should be the Safe)."
    )


# =============================================================================
# Manifest-driven authorisation tests
# =============================================================================


@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.arbitrum
@pytest.mark.asyncio
async def test_manifest_authorises_uniswap_v3_swap(  # noqa: layers
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Positive path: generated manifest authorises a compiled USDC→WETH swap.

    # noqa: layers — authorisation, not semantics. See plan doc.
    """
    case = _select_case(CHAIN_NAME, "SWAP")
    run_positive_authorisation_case(
        case,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
    )


@pytest.mark.intent(IntentType.SWAP)
@pytest.mark.arbitrum
@pytest.mark.asyncio
async def test_manifest_denies_swap_after_router_revoked(  # noqa: layers
    web3: Web3,
    anvil_rpc_url: str,
    funded_wallet: str,
    test_private_key: str,
    price_oracle,
) -> None:
    """Negative path: stripping the SwapRouter target makes the same intent revert.

    # noqa: layers — authorisation, not semantics. See plan doc.

    SwapRouter02 on Arbitrum is the only function-scoped target whose selector
    is ``exactInputSingle`` — the approve targets (USDC, WETH) use the ERC-20
    ``approve`` selector. Sourced from the connector SDK rather than hardcoded
    so it stays in sync with the production encoder.
    """
    case = _select_case(CHAIN_NAME, "SWAP")
    run_negative_authorisation_case(
        case,
        load_bearing_selector=EXACT_INPUT_SINGLE_SELECTOR,
        web3=web3,
        anvil_rpc_url=anvil_rpc_url,
        funded_wallet=funded_wallet,
        test_private_key=test_private_key,
        price_oracle=price_oracle,
    )
