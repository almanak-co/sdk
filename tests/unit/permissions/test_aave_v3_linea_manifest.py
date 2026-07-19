"""Regression tests for the Aave V3 Safe permission manifest on Linea (VIB-5916).

A Safe-wallet Aave V3 strategy on Linea authorises every ``(target, selector)``
it may call through a Zodiac Roles manifest. The connector participates in
compilation-based synthetic discovery (``permission_hints.py`` declares
``synthetic_discovery_intents = {SUPPLY, WITHDRAW, BORROW, REPAY}``), so the
manifest is derived by compiling those four synthetic intents offline. If the
Linea Pool address or an ERC-20 approval target went missing, a live strategy
would revert at ``execTransactionWithRole`` (unauthorized) despite a green
``permission_hints.py`` existence check — mere presence of the hints file is
NOT proof the manifest is correct.

These tests pin the concrete Linea surface:

- the Aave V3 Pool at 0xc47b8C00b0f69a36fa203Ffeac0334874574a8Ac is authorised
  for supply / withdraw / borrow / repay (selectors derived from the real
  function signatures, never hand-typed),
- the two ERC-20 approve targets a Safe strategy needs — USDC (collateral
  supply) and WETH (debt repay) — are present with the real ``approve`` selector,
- every token address is imported from the connector's own constants
  (``almanak.connectors.aave_v3.addresses``), so the assertion can never drift
  from the addresses the compiler actually emits.
"""

from __future__ import annotations

from eth_utils import function_signature_to_4byte_selector

from almanak.connectors.aave_v3.addresses import AAVE_V3, AAVE_V3_TOKENS
from almanak.framework.permissions.discovery import discover_permissions

CHAIN = "linea"

# Ground-truth addresses, sourced from the connector's own constants so this
# test tracks the compiler's emitted calldata rather than a hand-typed literal.
LINEA_POOL = AAVE_V3[CHAIN]["pool"].lower()
LINEA_USDC = AAVE_V3_TOKENS[CHAIN]["USDC"].lower()
LINEA_WETH = AAVE_V3_TOKENS[CHAIN]["WETH"].lower()

# The Linea Pool address asserted verbatim in the ticket — pinned literally as a
# second, independent guard so a silent edit to the connector constant that also
# broke discovery cannot pass unnoticed.
EXPECTED_LINEA_POOL = "0xc47b8c00b0f69a36fa203ffeac0334874574a8ac"

# Selectors derived from the real signatures — NOT copied from the connector —
# so the test fails if the connector ever authorises a selector that isn't the
# real 4-byte selector the compiler emits.
_SEL_APPROVE = "0x" + function_signature_to_4byte_selector("approve(address,uint256)").hex()
_SEL_SUPPLY = "0x" + function_signature_to_4byte_selector("supply(address,uint256,address,uint16)").hex()
_SEL_WITHDRAW = "0x" + function_signature_to_4byte_selector("withdraw(address,uint256,address)").hex()
_SEL_BORROW = "0x" + function_signature_to_4byte_selector("borrow(address,uint256,uint256,uint16,address)").hex()
_SEL_REPAY = "0x" + function_signature_to_4byte_selector("repay(address,uint256,uint256,address)").hex()
_SEL_SET_COLLATERAL = (
    "0x" + function_signature_to_4byte_selector("setUserUseReserveAsCollateral(address,bool)").hex()
)

_LENDING_INTENTS = ["SUPPLY", "WITHDRAW", "BORROW", "REPAY"]


def _discover() -> list:
    """Run synthetic permission discovery for aave_v3 on Linea.

    Uses ``discover_permissions`` directly (no RPC) — the aave_v3 connector's
    synthetic intents compile fully offline, so the returned permission list is
    exactly what a Safe manifest would authorise.
    """
    permissions, warnings = discover_permissions(
        chain=CHAIN,
        protocols=["aave_v3"],
        intent_types=_LENDING_INTENTS,
    )
    assert not warnings, f"Discovery emitted warnings for aave_v3 on {CHAIN}: {warnings}"
    return permissions


def _selectors_for(permissions: list, target: str) -> set[str]:
    """Return the lower-cased selector set authorised on ``target``."""
    return {
        sel.selector.lower()
        for perm in permissions
        if perm.target.lower() == target.lower()
        for sel in perm.function_selectors
    }


class TestAaveV3LineaManifest:
    """The Linea aave_v3 manifest must be non-empty and authorise the exact
    Pool + ERC-20-approve surface the compiler emits."""

    def test_manifest_is_non_empty(self) -> None:
        """Presence of permission_hints.py is not proof — discovery must yield
        actual permissions for aave_v3 on Linea."""
        permissions = _discover()
        assert permissions, (
            "aave_v3 discovery on Linea produced an EMPTY manifest — a Safe "
            "strategy would revert at execTransactionWithRole for every call."
        )

    def test_connector_constant_matches_ticket_pool_address(self) -> None:
        """The connector's Linea Pool constant must equal the ticket's verified
        address; both are asserted so neither can drift silently."""
        assert LINEA_POOL == EXPECTED_LINEA_POOL, (
            f"aave_v3 addresses.py Linea pool {LINEA_POOL} != verified "
            f"{EXPECTED_LINEA_POOL} (VIB-5916)"
        )

    def test_pool_is_authorised_for_all_lending_selectors(self) -> None:
        """The Linea Aave Pool must carry supply/withdraw/borrow/repay, with
        selectors equal to keccak(signature)[:4]."""
        permissions = _discover()
        targets = {perm.target.lower() for perm in permissions}
        assert LINEA_POOL in targets, (
            f"Linea Aave Pool {LINEA_POOL} missing from manifest targets {sorted(targets)}"
        )
        assert LINEA_POOL == EXPECTED_LINEA_POOL, "Pool constant drifted from the ticket address"

        pool_selectors = _selectors_for(permissions, LINEA_POOL)
        for name, sel in (
            ("supply", _SEL_SUPPLY),
            ("withdraw", _SEL_WITHDRAW),
            ("borrow", _SEL_BORROW),
            ("repay", _SEL_REPAY),
        ):
            assert sel in pool_selectors, (
                f"Linea Aave Pool manifest missing {name} selector {sel}. "
                f"Authorised selectors: {sorted(pool_selectors)}"
            )

    def test_usdc_and_weth_approve_targets_present(self) -> None:
        """Supply (USDC collateral) and repay (WETH debt) both require an ERC-20
        approve — the token approve targets/selectors must be authorised."""
        permissions = _discover()
        targets = {perm.target.lower() for perm in permissions}

        assert LINEA_USDC in targets, (
            f"USDC approve target {LINEA_USDC} missing — a Safe strategy could "
            f"not approve collateral for supply. Targets: {sorted(targets)}"
        )
        assert LINEA_WETH in targets, (
            f"WETH approve target {LINEA_WETH} missing — a Safe strategy could "
            f"not approve WETH for debt repay. Targets: {sorted(targets)}"
        )

        assert _SEL_APPROVE in _selectors_for(permissions, LINEA_USDC), (
            f"USDC {LINEA_USDC} must authorise the real approve selector {_SEL_APPROVE}"
        )
        assert _SEL_APPROVE in _selectors_for(permissions, LINEA_WETH), (
            f"WETH {LINEA_WETH} must authorise the real approve selector {_SEL_APPROVE}"
        )

    def test_manifest_is_exactly_the_least_privilege_surface(self) -> None:
        """Least-privilege, asserted EXACTLY: the manifest authorises the Linea
        Pool and the two required approve tokens — nothing more — and each
        target carries exactly its required selector set. A broadened Roles
        manifest (extra target, extra selector on a token, unexpected Pool
        method) fails here even if every required entry is present."""
        permissions = _discover()
        emitted = {perm.target.lower() for perm in permissions}
        expected_targets = {LINEA_POOL, LINEA_USDC, LINEA_WETH}
        assert emitted == expected_targets, (
            f"Manifest target set must be exactly the least-privilege surface "
            f"{sorted(expected_targets)}; got {sorted(emitted)}"
        )
        assert _selectors_for(permissions, LINEA_POOL) == {
            _SEL_SUPPLY,
            _SEL_WITHDRAW,
            _SEL_BORROW,
            _SEL_REPAY,
            # Emitted by SUPPLY compilation (use_as_collateral leg) — part of
            # the real lifecycle calldata, so it belongs in the exact set.
            _SEL_SET_COLLATERAL,
        }, "Pool selector set drifted from the compiled lifecycle surface"
        assert _selectors_for(permissions, LINEA_USDC) == {_SEL_APPROVE}, (
            "USDC must authorise exactly the approve selector"
        )
        assert _selectors_for(permissions, LINEA_WETH) == {_SEL_APPROVE}, (
            "WETH must authorise exactly the approve selector"
        )
