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
``almanak.connectors.curve.permission_hints.build_discovery_vectors``
(the connector self-contains its discovery vectors — dispatched via
``almanak.framework.permissions.hints.get_discovery_vectors_override``).
These tests pin the expected per-chain pool surface so the regression
cannot reappear.

VIB-6046 extended the same failure to the LP verbs:
``discover_permissions(chain, protocols=["curve"], intent_types=["LP_OPEN",
"LP_CLOSE"])`` returned ``([], [])`` on all five chains, because
``PERMISSION_HINTS`` declared ``SWAP`` only and ``build_discovery_vectors``
returned ``None`` for everything else. AGENTS.md §Connector additions #5 warns
that "an empty ``PermissionHints()`` passes the coverage gate while yielding
zero permissions, so presence is NOT proof" — the ``TestCurveLp*`` classes
below are that proof, asserting the *contents* of what discovery yields.

Note on how this stayed invisible: curve LP kept executing fine under Safe,
because the wildcarded MultiSend DELEGATECALL grant carried the batch past the
Roles allowlist without checking inner calls (VIB-6057). Curve LP was never
blocked — it was **unconstrained**.
"""

from __future__ import annotations

import inspect
import re
from typing import Any

import pytest

from almanak.connectors.curve import permission_hints as curve_hints
from almanak.connectors.curve.adapter import CURVE_POOLS
from almanak.connectors.curve.receipt_parser import CURVE_NATIVE_ETH_PLACEHOLDER
from almanak.core.intent_types import IntentType
from almanak.framework.intents.compiler import ERC20_APPROVE_SELECTOR
from almanak.framework.permissions.discovery import discover_permissions
from almanak.framework.permissions.generator import generate_manifest
from almanak.framework.permissions.hints import DiscoveryContext, get_permission_hints
from almanak.framework.permissions.synthetic_intents import build_synthetic_intents

_CURVE_CHAINS = sorted(CURVE_POOLS)
_CTX = DiscoveryContext(usdc="USDC", weth="WETH")
# Curve's sentinel for "this coin is native gas token, not an ERC-20".
# EXACT match against the connector's own constant, never a prefix: matching
# on "0xeeeeeeee" would also skip any legitimate ERC-20 whose address happens
# to start with those four bytes, silently dropping it from the approve
# coverage assertions below (CodeRabbit).
_NATIVE_COIN = CURVE_NATIVE_ETH_PLACEHOLDER.lower()


def _targets(permissions: list[Any]) -> dict[str, Any]:
    return {p.target.lower(): p for p in permissions}


def _selectors(permission: Any) -> set[str]:
    return {s.selector for s in permission.function_selectors}


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
    chain: [(name, data["address"]) for name, data in pools.items()] for chain, pools in CURVE_POOLS.items()
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
            "ethereum SWAP manifest missing 3pool — synthetic discovery must iterate CURVE_POOLS[chain] (#1903)"
        )
        # tricrypto2 — Tricrypto, USDT/WBTC/WETH (the #1903 missing target)
        assert "0xd51a44d3fae010294c616388b506acda1bfaae46" in targets, (
            "ethereum SWAP manifest missing tricrypto2 — this was the "
            "regression in #1903; without it execTransactionWithRole "
            "reverts with AuthorizationFailed when routing USDT->WETH."
        )

    @pytest.mark.parametrize(
        ("chain", "expected_pools"),
        [(chain, pools) for chain, pools in _EXPECTED_POOLS_BY_CHAIN.items() if pools],
        ids=lambda v: v if isinstance(v, str) else "",
    )
    def test_every_registered_pool_is_authorised(self, chain: str, expected_pools: list[tuple[str, str]]) -> None:
        """Every entry in ``CURVE_POOLS[chain]`` must land on the manifest.

        Drives off the curated registry so adding a new pool to
        ``CURVE_POOLS`` automatically extends the regression coverage —
        no manual update to this test required.
        """
        targets = _manifest_targets(chain)
        missing = [(name, addr) for name, addr in expected_pools if addr.lower() not in targets]
        assert not missing, (
            f"{chain} curve SWAP manifest missing pools: {missing}. "
            "Synthetic discovery must iterate CURVE_POOLS[chain] so every "
            "registered pool's address is authorised on the Safe."
        )


# ===========================================================================
# VIB-6046 — LP_OPEN / LP_CLOSE
# ===========================================================================


class TestCurveLpDeclaration:
    """Declaration-layer guards: cheap, and they fail first when LP drops out."""

    def test_curve_declares_swap_and_lp_synthetic_discovery(self) -> None:
        declared = get_permission_hints("curve").synthetic_discovery_intents
        assert {IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE} <= declared, (
            f"curve declares {sorted(value.value for value in declared)}; LP_OPEN/LP_CLOSE are required or the "
            "Zodiac manifest for curve LP is empty on every chain (VIB-6046)."
        )

    def test_permission_hints_module_has_no_hardcoded_chain_literals(self) -> None:
        """Targets/selectors must be derived from the connector's own constants.

        A hand-typed pool address or 4-byte selector can silently drift from the
        calldata the compiler emits — the exact failure mode compilation-based
        discovery exists to prevent (AGENTS.md §Connector additions #5). The
        module may build *intents*; it must never name a contract or selector.
        """
        source = inspect.getsource(curve_hints)
        body = source.split('"""', 2)[-1]  # drop the module docstring
        offenders = re.findall(r"['\"]0x[0-9a-fA-F]{8,}['\"]", body)
        assert not offenders, f"hardcoded address/selector literals in curve permission_hints: {offenders}"

    def test_unowned_intent_types_fall_through_to_the_framework(self) -> None:
        for intent_type in (IntentType.SUPPLY, IntentType.PERP_OPEN, IntentType.LP_COLLECT_FEES):
            assert curve_hints.build_discovery_vectors("curve", intent_type, "arbitrum", _CTX) is None


class TestCurveLpDiscoveryVectors:
    """Vector layer — fast, no compilation, sweeps every chain and pool."""

    @pytest.mark.parametrize("chain", _CURVE_CHAINS)
    def test_lp_open_vectors_cover_every_registered_pool(self, chain: str) -> None:
        vectors = curve_hints.build_discovery_vectors("curve", IntentType.LP_OPEN, chain, _CTX)
        assert vectors, f"no LP_OPEN discovery vectors for curve on {chain}"
        pools = CURVE_POOLS[chain]
        covered = {v.pool for v in vectors}
        assert covered == set(pools), f"LP_OPEN vectors on {chain} cover {sorted(covered)}, expected {sorted(pools)}"
        # A metapool gets a second (underlying/zap) vector; every other pool one.
        expected = sum(2 if p.get("is_metapool") else 1 for p in pools.values())
        assert len(vectors) == expected

    @pytest.mark.parametrize("chain", _CURVE_CHAINS)
    def test_lp_open_vectors_fund_every_pool_coin(self, chain: str) -> None:
        """The legacy ``amount0``/``amount1`` two-slot form zero-fills the tail,
        so coins at index 2+ of a 3- or 4-coin pool would never get an
        ``approve`` on the manifest. Discovery must use the full
        ``coin_amounts`` allocation vector."""
        vectors = curve_hints.build_discovery_vectors("curve", IntentType.LP_OPEN, chain, _CTX)
        for vector in vectors:
            pool_data = CURVE_POOLS[chain][vector.pool]
            native_len = int(pool_data["n_coins"])
            combined_len = 1 + len(pool_data.get("base_pool_coins") or []) if pool_data.get("is_metapool") else None
            assert vector.coin_amounts is not None, f"{chain}/{vector.pool}: LP_OPEN vector must set coin_amounts"
            assert len(vector.coin_amounts) in {native_len, combined_len}, (
                f"{chain}/{vector.pool}: coin_amounts has {len(vector.coin_amounts)} entries, "
                f"expected {native_len} (native) or {combined_len} (underlying)"
            )
            assert all(a > 0 for a in vector.coin_amounts)

    @pytest.mark.parametrize("chain", _CURVE_CHAINS)
    def test_lp_close_vectors_cover_every_exit_shape(self, chain: str) -> None:
        """``_dispatch_remove_liquidity`` emits a different selector per exit
        shape. Missing one means a strategy closing via ``coin_index`` reverts
        unauthorized mid-teardown — the worst moment to find a permission gap.
        ``remove_liquidity_imbalance`` is StableSwap-only; the compiler rejects
        it elsewhere, so emitting it there would only add a guaranteed
        compilation-failure warning to every manifest."""
        vectors = curve_hints.build_discovery_vectors("curve", IntentType.LP_CLOSE, chain, _CTX)
        assert vectors, f"no LP_CLOSE discovery vectors for curve on {chain}"
        for pool_name, pool_data in CURVE_POOLS[chain].items():
            shapes = [v for v in vectors if v.pool == pool_name]
            assert any(v.coin_index is None and v.imbalanced_amounts is None for v in shapes), (
                f"{chain}/{pool_name}: missing proportional remove_liquidity vector"
            )
            assert any(v.coin_index is not None for v in shapes), (
                f"{chain}/{pool_name}: missing remove_liquidity_one_coin vector"
            )
            is_stableswap = str(pool_data.get("pool_type") or "").lower() == "stableswap"
            has_imbalanced = any(v.imbalanced_amounts is not None for v in shapes)
            assert has_imbalanced is is_stableswap, (
                f"{chain}/{pool_name}: imbalanced vector present={has_imbalanced} but "
                f"pool_type={pool_data.get('pool_type')!r}"
            )

    @pytest.mark.parametrize("chain", _CURVE_CHAINS)
    @pytest.mark.parametrize("intent_type", ["LP_OPEN", "LP_CLOSE"])
    def test_framework_dispatch_reaches_the_curve_override(self, chain: str, intent_type: str) -> None:
        """Assert through the framework entry point, not the connector
        function: the override is only consulted when curve is in the LP
        membership set, so a membership regression must fail here."""
        assert build_synthetic_intents("curve", intent_type, chain), (
            f"build_synthetic_intents('curve', {intent_type!r}, {chain!r}) is empty — "
            "curve dropped out of the LP membership set"
        )


@pytest.fixture(scope="module")
def arbitrum_lp_permissions() -> tuple[list[Any], list[str]]:
    """Full LP manifest for arbitrum: a 2-coin StableSwap plus a 3-coin
    Tricrypto, which between them exercise both selector families and a
    pool whose LP token is a separate contract."""
    return discover_permissions("arbitrum", ["curve"], ["LP_OPEN", "LP_CLOSE"])


@pytest.fixture(scope="module")
def ethereum_lp_open_permissions() -> tuple[list[Any], list[str]]:
    """LP_OPEN manifest for ethereum: the only chain with a metapool (zap
    target) and the only pool that deposits native ETH (``send_allowed``)."""
    return discover_permissions("ethereum", ["curve"], ["LP_OPEN"])


class TestCurveLpCompiledManifest:
    """Compiled layer — what ``discover_permissions`` actually yields.

    This is the layer that would have caught VIB-6046: every assertion below
    fails against the pre-fix connector, and none of them can be satisfied by a
    ``permission_hints.py`` that merely exists.
    """

    def test_arbitrum_lp_manifest_is_not_empty(self, arbitrum_lp_permissions: tuple[list[Any], list[str]]) -> None:
        """The literal VIB-6046 measurement: this returned ``([], [])``."""
        permissions, warnings = arbitrum_lp_permissions
        assert permissions, "curve LP_OPEN/LP_CLOSE on arbitrum yields an EMPTY Zodiac manifest (VIB-6046)"
        assert not warnings, f"curve LP discovery on arbitrum emitted warnings: {warnings}"

    def test_arbitrum_lp_manifest_authorises_every_pool_and_coin(
        self, arbitrum_lp_permissions: tuple[list[Any], list[str]]
    ) -> None:
        permissions, _ = arbitrum_lp_permissions
        targets = _targets(permissions)

        for pool_name, pool_data in CURVE_POOLS["arbitrum"].items():
            pool_address = pool_data["address"].lower()
            assert pool_address in targets, (
                f"curve pool {pool_name} ({pool_address}) missing from the arbitrum LP manifest"
            )
            # Both directions of the lifecycle must be authorised on the pool:
            # an open-only manifest strands the position at teardown.
            pool_selectors = _selectors(targets[pool_address])
            non_approve = pool_selectors - {ERC20_APPROVE_SELECTOR}
            assert len(non_approve) >= 2, (
                f"{pool_name}: expected at least an add_liquidity and a remove_liquidity "
                f"selector on the pool, got {sorted(pool_selectors)}"
            )

            for coin in pool_data["coin_addresses"]:
                if coin.lower() == _NATIVE_COIN:
                    continue  # native gas token — no ERC-20 approve exists
                assert coin.lower() in targets, f"{pool_name}: coin {coin} has no approve permission"
                assert ERC20_APPROVE_SELECTOR in _selectors(targets[coin.lower()])

            # LP_CLOSE approves the LP token before burning it.
            lp_token = pool_data["lp_token"].lower()
            assert lp_token in targets and ERC20_APPROVE_SELECTOR in _selectors(targets[lp_token]), (
                f"{pool_name}: LP token {lp_token} has no approve permission — remove_liquidity would revert"
            )

    @pytest.mark.parametrize("chain", _CURVE_CHAINS)
    def test_lp_manifest_matches_the_compiled_calldata_exactly(self, chain: str) -> None:
        """Pin the full grant shape against calldata built by the real compiler — EXACTLY.

        Derived, never typed: the expected set is recomputed from the same
        synthetic vectors discovery uses, so this asserts "the manifest contains
        exactly what the compiler emits" without hardcoding a 4-byte literal
        that could itself drift from the connector.

        **Exact equality in BOTH directions, deliberately.** An earlier version
        asserted only ``expected - actual == {}``, which passes when the manifest
        contains *extra* targets or selectors. A test that cannot detect an
        over-grant is the precise failure class this whole effort is about — it
        is what let the wildcarded MultiSend DELEGATECALL survive (VIB-6057) and
        what let ``supports_standalone_fee_collection=False`` read as correct
        (VIB-6149).

        All four axes of the grant are pinned, per chain, because each one is
        independently exploitable:

        * **target** — an extra address is an unaudited contract the Safe may call;
        * **selector** — an extra 4-byte is an unaudited method on an audited contract;
        * **send_allowed** — an unnoticed ``True`` silently permits moving native
          value out of the Safe;
        * **operation** — ``DELEGATECALL`` (1) hands the callee the Safe's own
          storage. Every curve grant must be ``CALL`` (0). This is the exact
          shape of the wildcarded MultiSend DELEGATECALL grant in VIB-6057, so
          a curve pool silently acquiring it must turn this test red.
        """
        from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig
        from almanak.framework.permissions.hints import get_permission_hints

        permissions, _warnings = discover_permissions(chain, ["curve"], ["LP_OPEN", "LP_CLOSE"])
        targets = _targets(permissions)

        # Both sides must be compiled under the SAME transport policy, and it is
        # read from the connector's own declaration rather than restated here.
        #
        # This assertion compares a manifest against calldata. If the two sides
        # are built differently they can only agree by luck: discovery honours
        # ``PermissionHints.offline_discovery``, so for curve it compiles hard
        # offline, while this side previously omitted the flag and therefore let
        # ``_get_chain_rpc_url`` resolve an implicit public RPC. The expected set
        # was then a function of RPC weather while the actual set was not —
        # exactly the nondeterminism this PR exists to remove, reintroduced
        # inside the test meant to prove it gone.
        #
        # Deriving the flag from the hints (not hardcoding ``True``) keeps the
        # two sides tied together: if curve ever turns offline discovery off,
        # both move at once and this test cannot silently start comparing a
        # hard-offline manifest against a network-fed expectation.
        offline = get_permission_hints("curve").offline_discovery
        compiler = IntentCompiler(
            chain=chain,
            rpc_url=None,
            config=IntentCompilerConfig(
                allow_placeholder_prices=True,
                swap_pool_selection_mode="auto",
                fixed_swap_fee_tier=3000,
                permission_discovery=True,
                offline_discovery=offline,
            ),
        )
        expected: dict[str, set[str]] = {}
        expected_send: dict[str, bool] = {}
        for intent_type in ("LP_OPEN", "LP_CLOSE"):
            for intent in build_synthetic_intents("curve", intent_type, chain):
                result = compiler.compile(intent)
                assert result.status.value == "SUCCESS", f"{intent_type} synthetic failed to compile: {result.error}"
                for tx in result.transactions:
                    target = tx.to.lower()
                    expected.setdefault(target, set()).add(tx.data[:10])
                    expected_send[target] = expected_send.get(target, False) or tx.value > 0

        assert expected, f"no calldata produced by the curve LP synthetics on {chain}"

        # Targets: exact set equality. An EXTRA target is an over-grant.
        assert set(targets) == set(expected), (
            f"{chain}: manifest targets differ from compiled targets.\n"
            f"  over-granted (in manifest, never compiled): {sorted(set(targets) - set(expected))}\n"
            f"  missing (compiled, not authorised):         {sorted(set(expected) - set(targets))}"
        )

        for target, selectors in expected.items():
            actual = _selectors(targets[target])
            assert actual == selectors, (
                f"{chain}/{target}: selector set differs.\n"
                f"  over-granted: {sorted(actual - selectors)}\n"
                f"  missing:      {sorted(selectors - actual)}"
            )
            assert targets[target].send_allowed is expected_send[target], (
                f"{chain}/{target}: send_allowed={targets[target].send_allowed}, compiler emitted "
                f"value>0={expected_send[target]}. A spurious True is a silent permission to "
                f"move native value out of the Safe."
            )
            assert targets[target].operation == 0, (
                f"{chain}/{target}: operation={targets[target].operation}, expected 0 (CALL). "
                "A DELEGATECALL grant hands the callee the Safe's own storage — this is the "
                "VIB-6057 over-grant shape."
            )

    def test_ethereum_metapool_zap_is_authorised(
        self, ethereum_lp_open_permissions: tuple[list[Any], list[str]]
    ) -> None:
        """A metapool underlying deposit routes through the zap — a target the
        native deposit vector never touches."""
        permissions, warnings = ethereum_lp_open_permissions
        assert not warnings, f"curve LP_OPEN discovery on ethereum emitted warnings: {warnings}"
        targets = _targets(permissions)

        zaps = [p["zap_address"].lower() for p in CURVE_POOLS["ethereum"].values() if p.get("zap_address")]
        assert zaps, "ethereum registry no longer has a metapool — drop or retarget this test"
        for zap in zaps:
            assert zap in targets, f"metapool zap {zap} missing from the ethereum LP_OPEN manifest"
            assert _selectors(targets[zap]), f"metapool zap {zap} authorised with no selectors"

    def test_native_eth_pool_gets_send_allowed(self, ethereum_lp_open_permissions: tuple[list[Any], list[str]]) -> None:
        """Curve's stETH pool takes native ETH as coin 0, so ``add_liquidity``
        is value-bearing. Zodiac Roles denies a value-bearing call unless the
        target's execution options carry the Send bit."""
        permissions, _ = ethereum_lp_open_permissions
        targets = _targets(permissions)

        native_pools = [
            p["address"].lower()
            for p in CURVE_POOLS["ethereum"].values()
            if any(c.lower() == _NATIVE_COIN for c in p["coin_addresses"])
        ]
        assert native_pools, "ethereum registry no longer has a native-ETH pool — drop or retarget this test"
        for pool in native_pools:
            assert pool in targets, f"native-ETH pool {pool} missing from the manifest"
            assert targets[pool].send_allowed is True, (
                f"native-ETH pool {pool} has send_allowed=False — a value-bearing add_liquidity "
                "would be denied at execTransactionWithRole"
            )
