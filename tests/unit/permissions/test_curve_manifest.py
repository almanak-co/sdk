"""Regression tests for the Curve permission manifest (#1903).

Curve pools are pair-specific (StableSwap, CryptoSwap, Tricrypto). The
prior synthetic-discovery surface consumed a single
``synthetic_swap_pair`` hint per chain, which compiled to exactly one
pool — leaving every other registered pool unauthorised on the Safe.
The cryptoswap intent test on ethereum routes through tricrypto2
(USDT/WETH); that pool's address never landed in the manifest and
``execTransactionWithRole`` reverted with ``AuthorizationFailed``.

The fix replaces the single-pair source with iteration over
``CURVE_POOLS[chain]`` in
``almanak.framework.connectors.curve.permission_hints.build_discovery_vectors``
(the connector self-contains its discovery vectors — dispatched via
``almanak.framework.permissions.hints.get_discovery_vectors_override``).
These tests pin the expected per-chain pool surface so the regression
cannot reappear.
"""

from __future__ import annotations

import pytest

from almanak.framework.connectors.curve.adapter import CURVE_POOLS
from almanak.framework.permissions.generator import generate_manifest


def _manifest_targets(chain: str) -> set[str]:
    """Return the set of authorised target addresses for a curve SWAP manifest."""
    manifest = generate_manifest(
        strategy_name=f"curve-manifest-regression-{chain}",
        chain=chain,
        supported_protocols=["curve"],
        intent_types=["SWAP"],
    )
    return {perm.target.lower() for perm in manifest.permissions}


# Per-chain authoritative list of curve pool addresses that MUST appear on
# the manifest when SWAP is requested. Sourced from
# ``CURVE_POOLS[chain]`` so the assertion stays in lockstep with the
# curated registry — adding a new pool to the registry automatically
# tightens this regression.
_EXPECTED_POOLS_BY_CHAIN: dict[str, list[tuple[str, str]]] = {
    chain: [(name, data["address"]) for name, data in pools.items()]
    for chain, pools in CURVE_POOLS.items()
}


class TestCurveManifestPoolCoverage:
    """The curve SWAP manifest must authorise every registered pool."""

    def test_ethereum_manifest_includes_3pool_and_tricrypto2(self) -> None:
        """#1903 explicit regression.

        Both 3pool (StableSwap, USDC/USDT) AND tricrypto2 (Tricrypto,
        USDT/WETH) must be authorised. tricrypto2 is the address that
        was missing in the bug report — without iteration over
        ``CURVE_POOLS[chain]``, the synthetic discovery only matched
        3pool via the USDC/USDT hint.
        """
        targets = _manifest_targets("ethereum")
        # 3pool — StableSwap, USDC/USDT/DAI
        assert "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7" in targets, (
            "ethereum SWAP manifest missing 3pool — synthetic discovery "
            "must iterate CURVE_POOLS[chain] (#1903)"
        )
        # tricrypto2 — Tricrypto, USDT/WBTC/WETH (the #1903 missing target)
        assert "0xd51a44d3fae010294c616388b506acda1bfaae46" in targets, (
            "ethereum SWAP manifest missing tricrypto2 — this was the "
            "regression in #1903; without it execTransactionWithRole "
            "reverts with AuthorizationFailed when routing USDT->WETH."
        )

    @pytest.mark.parametrize(
        ("chain", "expected_pools"),
        [
            (chain, pools)
            for chain, pools in _EXPECTED_POOLS_BY_CHAIN.items()
            if pools
        ],
        ids=lambda v: v if isinstance(v, str) else "",
    )
    def test_every_registered_pool_is_authorised(
        self, chain: str, expected_pools: list[tuple[str, str]]
    ) -> None:
        """Every entry in ``CURVE_POOLS[chain]`` must land on the manifest.

        Drives off the curated registry so adding a new pool to
        ``CURVE_POOLS`` automatically extends the regression coverage —
        no manual update to this test required.
        """
        targets = _manifest_targets(chain)
        missing = [
            (name, addr) for name, addr in expected_pools if addr.lower() not in targets
        ]
        assert not missing, (
            f"{chain} curve SWAP manifest missing pools: {missing}. "
            "Synthetic discovery must iterate CURVE_POOLS[chain] so every "
            "registered pool's address is authorised on the Safe."
        )
