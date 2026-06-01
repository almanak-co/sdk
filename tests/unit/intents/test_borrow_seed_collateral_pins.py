"""Pin the per-chain default collateral the BORROW/REPAY seeder picks.

Background — issue #1845
========================

When a ``PermissionTestCase`` for a stablecoin REPAY runs through the on-chain
permission harness, ``_seed_supply_then_borrow`` chooses an ETH-correlated
collateral to pair against the borrow. Hardcoding "WETH" worked when this was
written, but Aave governance later reshaped the WETH reserves on multiple
chains (frozen on Arbitrum/Base, ``ltv=0`` on Ethereum), turning every
seeded supply into a ``ReserveFrozen()`` revert before the authz test even
ran. PR for #1845 moved the default into a per-chain map; this gate keeps
the map honest:

1. **Every chain in the map must list a token that exists in the harness's
   chain config.** A typo or refactor that drops the token symbol would
   silently regress the seeder back to the WETH fallback on the next CI run.

2. **Every chain in the map must list a token Aave V3 ships on that chain.**
   The seeder only seeds the lend protocols the harness drives (aave_v3 /
   spark / radiant_v2 / morpho_blue); for protocols that share Aave's
   ``Pool.supply()`` shape, an unsupported token revert with
   ``ReserveDoesNotExist`` would also surface as ``SeedingFailed``.

3. **The forbidden combinations are pinned.** ``arbitrum -> WETH``,
   ``base -> WETH``, ``ethereum -> WETH`` are exactly the regressions #1845
   fixed; this test fails loudly if anyone re-introduces them.

This test does NOT touch the network — the reserve-status reasoning lives in
the PR description (probed via ``cast getReserveConfigurationData`` against
each chain's AaveProtocolDataProvider, snapshotted 2026-05-04). This unit
test only enforces the *symbolic* pinning so the per-chain map stays in sync
with the contract registry. A future contributor who wants to switch back
to WETH must justify it with a fresh reserve probe and update both the map
and this test together.
"""

from __future__ import annotations

import pytest

from almanak.connectors.aave_v3.addresses import AAVE_V3_TOKENS
from tests.intents._permission_onchain_harness import (
    _BORROW_SEED_DEFAULT_COLLATERAL_BY_CHAIN,
    _BORROW_SEED_FALLBACK_COLLATERAL,
    _resolve_borrow_seed_collateral,
)
from tests.intents.conftest import CHAIN_CONFIGS

# Forbidden chain ↦ token combos that #1845 explicitly fixed. A future
# contributor re-introducing one of these MUST update the reserve probe in
# the PR description AND remove the entry here in the same change — no silent
# regressions.
_KNOWN_FROZEN_OR_LTV_ZERO: dict[str, set[str]] = {
    # Arbitrum: Aave Pool reverts ``ReserveFrozen()`` on supply().
    "arbitrum": {"WETH"},
    # Base: Aave Pool reverts ``ReserveFrozen()`` on supply().
    "base": {"WETH"},
    # Ethereum: supply() succeeds but ``ltv=0`` ⇒ no borrowing power.
    "ethereum": {"WETH"},
}


@pytest.mark.parametrize(
    ("chain", "expected"),
    sorted(_BORROW_SEED_DEFAULT_COLLATERAL_BY_CHAIN.items()),
)
def test_default_collateral_is_in_chain_config(chain: str, expected: str) -> None:
    """Every default-collateral pick must exist in ``CHAIN_CONFIGS[chain]['tokens']``.

    The harness funds the Safe via the chain config's token map and reads the
    ERC-20 address from there to compile the SUPPLY intent. A symbol that's
    not in the chain config causes ``_token_amount_wei`` to raise a ``KeyError``
    well before the on-chain SUPPLY runs, but the failure mode is opaque —
    pinning it here turns the regression into a clean unit failure.
    """
    chain_tokens = CHAIN_CONFIGS[chain]["tokens"]
    assert expected in chain_tokens, (
        f"Default collateral {expected!r} for chain {chain!r} is not in "
        f"CHAIN_CONFIGS[{chain!r}]['tokens'] (found: {sorted(chain_tokens)}). "
        f"Add the token to the chain config first, or change the per-chain "
        f"map in tests/intents/_permission_onchain_harness.py."
    )


@pytest.mark.parametrize(
    ("chain", "expected"),
    sorted(_BORROW_SEED_DEFAULT_COLLATERAL_BY_CHAIN.items()),
)
def test_default_collateral_is_an_aave_v3_reserve(chain: str, expected: str) -> None:
    """Every default-collateral pick must be a known Aave V3 reserve on the chain.

    Aave V3 is the most-driven protocol in the harness's BORROW/REPAY paths,
    and the per-chain default is what the seeder uses when a case file does
    not explicitly name a collateral. If the symbol is not in
    ``AAVE_V3_TOKENS[chain]``, the harness will compile against an address
    Aave's pool doesn't recognise and the SUPPLY tx will revert with
    ``ReserveDoesNotExist`` — same triage bucket as the original #1845 bug.

    spark / radiant_v2 / morpho_blue ride the same default; spark is
    Ethereum-only, radiant_v2 is Arbitrum-only and morpho_blue is
    market_id-driven (the case overrides the default downstream of the
    compiler), so the Aave V3 cross-check covers the realistic seed paths.
    """
    aave_tokens = AAVE_V3_TOKENS.get(chain, {})
    assert expected in aave_tokens, (
        f"Default collateral {expected!r} for chain {chain!r} is not in "
        f"AAVE_V3_TOKENS[{chain!r}] (found: {sorted(aave_tokens)}). The "
        f"harness's BORROW/REPAY seeder routes the SUPPLY through the host "
        f"lending protocol's pool and a token absent from the protocol's "
        f"reserve list will revert. Pick a different collateral or extend "
        f"AAVE_V3_TOKENS first."
    )


@pytest.mark.parametrize(
    ("chain", "forbidden"),
    sorted(
        (chain, token)
        for chain, tokens in _KNOWN_FROZEN_OR_LTV_ZERO.items()
        for token in sorted(tokens)
    ),
)
def test_forbidden_collateral_combinations_stay_blocked(chain: str, forbidden: str) -> None:
    """Pin the regressions #1845 fixed: chain ↦ token must not match the per-chain map."""
    actual = _BORROW_SEED_DEFAULT_COLLATERAL_BY_CHAIN.get(chain)
    assert actual != forbidden, (
        f"Chain {chain!r} default collateral is {actual!r} — but {forbidden!r} "
        f"is on the known frozen / ltv=0 list (issue #1845). Re-probe the "
        f"reserve via "
        f"`cast call <pool_data_provider> getReserveConfigurationData(address)"
        f" <token>` and update the per-chain map AND this guard together."
    )


@pytest.mark.parametrize(
    ("chain", "borrow_symbol", "expected_collateral"),
    [
        # Stablecoin borrows on the chains where WETH is broken: must pick
        # wstETH (the #1845 fix).
        ("arbitrum", "USDC", "wstETH"),
        ("arbitrum", "USDT", "wstETH"),
        ("arbitrum", "DAI", "wstETH"),
        ("base", "USDC", "wstETH"),
        ("ethereum", "USDC", "wstETH"),
        ("ethereum", "USDT", "wstETH"),
        ("ethereum", "DAI", "wstETH"),
        # Stablecoin borrows on chains where WETH is still good — keep WETH.
        ("optimism", "USDC", "WETH"),
        ("polygon", "USDC", "WETH"),
        # Non-stablecoin borrow keeps the legacy "USDC collateral" pick.
        # Stablecoin reserves are rarely frozen, so this default is safe.
        ("arbitrum", "WETH", "USDC"),
        ("arbitrum", "wstETH", "USDC"),
        ("ethereum", "WBTC", "USDC"),
    ],
)
def test_resolve_borrow_seed_collateral_per_chain(
    chain: str,
    borrow_symbol: str,
    expected_collateral: str,
) -> None:
    """The pure helper picks the chain-appropriate collateral.

    This is the behavioural pin matching the data pin above: a future change
    that touches the per-chain map but breaks the resolver (e.g. forgets to
    consult the map for stablecoin borrows) fails this test loudly.
    """
    actual = _resolve_borrow_seed_collateral(chain, borrow_symbol)
    assert actual == expected_collateral, (
        f"_resolve_borrow_seed_collateral({chain!r}, {borrow_symbol!r}) "
        f"returned {actual!r}, expected {expected_collateral!r}. "
        f"Chain map: {_BORROW_SEED_DEFAULT_COLLATERAL_BY_CHAIN!r}, "
        f"fallback: {_BORROW_SEED_FALLBACK_COLLATERAL!r}."
    )


def test_resolve_borrow_seed_collateral_unknown_chain_falls_back() -> None:
    """Stablecoin borrow on an unprobed chain returns the fallback (WETH)."""
    actual = _resolve_borrow_seed_collateral("madeup_chain_xyz", "USDC")
    assert actual == _BORROW_SEED_FALLBACK_COLLATERAL


def test_fallback_is_a_real_token_symbol() -> None:
    """The fallback symbol must be a real ERC-20 we can fund — defensive sanity check."""
    # The fallback only fires for chains the per-chain map doesn't enumerate;
    # those chains don't necessarily have wstETH so we can't pick wstETH as
    # the fallback. WETH is the safest default because most Aave V3 chains
    # still treat WETH as collateral. Confirm at least one chain we know
    # supports WETH still has it in CHAIN_CONFIGS so an accidental rename
    # doesn't silently break funding.
    assert _BORROW_SEED_FALLBACK_COLLATERAL == "WETH", (
        "Fallback collateral is no longer 'WETH'. If this is intentional, "
        "verify the new symbol is funded via the chain configs of every "
        "lend-supporting chain not listed in the per-chain map."
    )
    # Any chain that has WETH in its config proves the symbol is funded.
    chains_with_weth = [
        chain for chain, cfg in CHAIN_CONFIGS.items() if "WETH" in cfg.get("tokens", {})
    ]
    assert chains_with_weth, "No chain config carries WETH — fallback is unfunded."
