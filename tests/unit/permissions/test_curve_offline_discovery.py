"""Curve permission discovery is offline, deterministic, and covers both ABI variants.

VIB-6046 D5. Before this landed, ``discover_permissions`` for curve LP issued 43
live ``eth_call``s to a public RPC and returned a *different manifest each run*
(7 / 3 / 7 targets across three consecutive runs), because a 429 on the
``is_ng`` ABI probe silently flipped which selector family got authorised.

The tests here pin the three properties that fix depends on:

* discovery performs **no network I/O at all** (the strongest available
  statement — asserting "few calls" would pass on a partially-online run);
* the manifest is **byte-identical across runs**;
* **both** ABI variants are authorised, so a live probe that disagrees with the
  static registry at execution time still finds its selector on the manifest;
* the discovery-only escape hatches are **not reachable** from a funds-moving
  path, and the fail-closed slippage guards still refuse without them.
"""

from __future__ import annotations

import pytest

from almanak.framework.permissions.discovery import discover_permissions
from almanak.framework.permissions.hints import get_permission_hints
from tests.support.curve_adapter import (
    ADD_LIQUIDITY_2_SELECTOR,
    ADD_LIQUIDITY_3_SELECTOR,
    ADD_LIQUIDITY_4_SELECTOR,
    ADD_LIQUIDITY_DYN_SELECTOR,
    CURVE_TEST_POOLS,
    REMOVE_LIQUIDITY_2_SELECTOR,
    REMOVE_LIQUIDITY_3_SELECTOR,
    REMOVE_LIQUIDITY_4_SELECTOR,
    REMOVE_LIQUIDITY_DYN_SELECTOR,
    CurveAdapter,
    CurveConfig,
    PoolType,
)

CURVE_CHAINS = sorted(CURVE_TEST_POOLS)

# Chains whose registry currently carries a volatile-family pool. Derived, not
# typed, so adding/removing a pool cannot leave the per-family assertion below
# silently unexercised.
_CHAINS_WITH_VOLATILE_POOLS = {
    chain
    for chain, pools in CURVE_TEST_POOLS.items()
    if any(p.get("pool_type") in {PoolType.CRYPTOSWAP.value, PoolType.TRICRYPTO.value} for p in pools.values())
}
LP_INTENTS = ["LP_OPEN", "LP_CLOSE"]


def _manifest_signature(chain: str) -> tuple:
    """A total, order-stable fingerprint of the discovered manifest."""
    permissions, warnings = discover_permissions(chain, ["curve"], LP_INTENTS)
    return (
        tuple(
            (
                p.target.lower(),
                p.send_allowed,
                p.operation,
                tuple(sorted(s.selector for s in p.function_selectors)),
            )
            for p in sorted(permissions, key=lambda x: x.target.lower())
        ),
        tuple(sorted(warnings)),
    )


class _NoNetwork:
    """Fails the test on ANY outbound HTTP call made inside the block.

    Patches ``requests.Session.request`` — the single chokepoint every helper in
    this repo's RPC path funnels through (``eth_call_uint256`` included). A
    counter would let a "mostly offline" regression through; raising makes the
    first call fatal and names it.
    """

    def __init__(self) -> None:
        self.calls: list[str] = []

    def __enter__(self) -> _NoNetwork:
        import requests

        self._orig = requests.Session.request

        def _blocked(_self, method, url, *args, **kwargs):
            self.calls.append(f"{method} {url}")
            raise AssertionError(
                f"Permission discovery made a network call: {method} {url}. "
                "The curve manifest must be a pure function of CURVE_TEST_POOLS "
                "(VIB-6046 D5) — a live read makes it depend on RPC weather."
            )

        requests.Session.request = _blocked  # type: ignore[method-assign]
        return self

    def __exit__(self, *exc: object) -> None:
        import requests

        requests.Session.request = self._orig  # type: ignore[method-assign]


class TestDiscoveryIsOffline:
    """No network I/O during curve LP permission discovery."""

    @pytest.mark.parametrize("chain", CURVE_CHAINS)
    def test_discovery_makes_no_network_calls(self, chain: str) -> None:
        with _NoNetwork():
            permissions, warnings = discover_permissions(chain, ["curve"], LP_INTENTS)
        assert permissions, f"{chain}: empty manifest"
        assert not warnings, f"{chain}: discovery warnings {warnings}"

    def test_curve_declares_offline_discovery(self) -> None:
        """The opt-in is what suppresses the compiler's implicit RPC fallback."""
        assert get_permission_hints("curve").offline_discovery is True


class TestDiscoveryIsDeterministic:
    """Identical inputs produce an identical manifest, run over run."""

    @pytest.mark.parametrize("chain", CURVE_CHAINS)
    def test_manifest_is_stable_across_runs(self, chain: str) -> None:
        first = _manifest_signature(chain)
        second = _manifest_signature(chain)
        assert first == second, f"{chain}: manifest differs between two consecutive discovery runs"


class TestBothAbiVariantsAuthorised:
    """``is_ng`` drift cannot strand a StableSwap position — and cannot over-grant elsewhere.

    Asserted PER FAMILY, and that distinction is the whole point.

    ``is_ng`` selects between the StableSwap-NG dynamic-array ABI
    (``add_liquidity(uint256[],uint256)``) and the legacy fixed-size one. The
    registry's value can disagree with a live probe, and a StableSwap pool that
    compiled under the wrong assumption reverts at teardown — so for that family
    BOTH variants are authorised deliberately.

    CryptoSwap/Tricrypto pools implement NEITHER NG form. Granting them the NG
    selectors authorises methods the pool does not have and the compiler would
    never emit for it. That is an over-grant, which is the same class of defect
    this PR exists to close, so it is asserted absent rather than left unchecked.

    The previous version of this test required all four selectors on EVERY pool,
    which made it assert the over-grant: it would have gone red on the fix and
    green on the bug. Measured before the fix: 3 registered pools
    (arbitrum/tricrypto, base/weth_cbeth, ethereum/tricrypto2) carried
    ``0xb72df5de`` and ``0xd40ddb8c`` for nothing.
    """

    @pytest.mark.parametrize("chain", CURVE_CHAINS)
    def test_variants_are_authorised_per_pool_family(self, chain: str) -> None:
        permissions, _ = discover_permissions(chain, ["curve"], LP_INTENTS)
        by_target = {p.target.lower(): {s.selector for s in p.function_selectors} for p in permissions}

        legacy_add = {ADD_LIQUIDITY_2_SELECTOR, ADD_LIQUIDITY_3_SELECTOR, ADD_LIQUIDITY_4_SELECTOR}
        legacy_remove = {REMOVE_LIQUIDITY_2_SELECTOR, REMOVE_LIQUIDITY_3_SELECTOR, REMOVE_LIQUIDITY_4_SELECTOR}
        # Same discriminator the adapter uses (``CurveAdapter._is_cryptoswap``),
        # read from the shared enum so the test cannot drift from the code.
        volatile_families = {PoolType.CRYPTOSWAP.value, PoolType.TRICRYPTO.value}

        checked_stableswap = 0
        checked_volatile = 0
        for name, pool in CURVE_TEST_POOLS[chain].items():
            selectors = by_target.get(pool["address"].lower())
            assert selectors, f"{chain}/{name}: pool not authorised at all"

            if pool.get("pool_type") in volatile_families:
                checked_volatile += 1
                # NG is a StableSwap-only ABI. Its presence here is an over-grant.
                assert ADD_LIQUIDITY_DYN_SELECTOR not in selectors, (
                    f"{chain}/{name}: NG add_liquidity ({ADD_LIQUIDITY_DYN_SELECTOR}) authorised on a "
                    f"{pool['pool_type']} pool, which does not implement it — over-grant"
                )
                assert REMOVE_LIQUIDITY_DYN_SELECTOR not in selectors, (
                    f"{chain}/{name}: NG remove_liquidity ({REMOVE_LIQUIDITY_DYN_SELECTOR}) authorised on a "
                    f"{pool['pool_type']} pool, which does not implement it — over-grant"
                )
                # It must still be able to open AND close, in its own ABI.
                assert selectors & legacy_add, f"{chain}/{name}: no add_liquidity variant authorised"
                assert selectors & legacy_remove, f"{chain}/{name}: no remove_liquidity variant authorised"
                continue

            checked_stableswap += 1
            # StableSwap: whatever the registry says ``is_ng`` is, the OTHER
            # variant must also be present — a live probe may disagree, and
            # being wrong strands the position at teardown.
            assert ADD_LIQUIDITY_DYN_SELECTOR in selectors, f"{chain}/{name}: NG add_liquidity variant missing"
            assert selectors & legacy_add, f"{chain}/{name}: legacy add_liquidity variant missing"
            assert REMOVE_LIQUIDITY_DYN_SELECTOR in selectors, f"{chain}/{name}: NG remove_liquidity variant missing"
            assert selectors & legacy_remove, f"{chain}/{name}: legacy remove_liquidity variant missing"

        # Neither branch may be vacuous: if the registry ever stops carrying a
        # family on a chain, this test would silently stop checking that family
        # rather than failing — the absence-as-success shape.
        assert checked_stableswap or checked_volatile, f"{chain}: no curve pools checked at all"
        if chain in _CHAINS_WITH_VOLATILE_POOLS:
            assert checked_volatile, f"{chain}: expected a cryptoswap/tricrypto pool, checked none"


class TestDiscoveryFlagsAreNotReachableFromProduction:
    """The synthetic-bound escape hatches must never be set on a funds-moving path."""

    def test_flags_default_to_off(self) -> None:
        config = CurveConfig(chain="ethereum", wallet_address="0x" + "11" * 20)
        assert config.permission_discovery is False
        assert config.force_is_ng is None

    def test_compiler_only_sets_the_flag_from_the_context(self) -> None:
        """Every ``permission_discovery=`` in the curve compiler is fed by the ctx probe.

        A literal ``True`` here would be a production path silently opting into
        synthetic slippage bounds. Guarding the source text is crude but it is
        the property that matters and it cannot be satisfied accidentally.
        """
        from pathlib import Path

        import almanak.connectors.curve.compiler as curve_compiler

        source = Path(curve_compiler.__file__).read_text()
        assignments = [
            line.strip()
            for line in source.splitlines()
            if "permission_discovery=" in line and "def " not in line and "getattr" not in line
        ]
        assert assignments, "expected the compiler to thread permission_discovery through"
        for line in assignments:
            assert "_discovery_mode(ctx)" in line, f"permission_discovery not sourced from the ctx probe: {line!r}"

    def test_discovery_mode_defaults_false_on_a_context_without_the_field(self) -> None:
        """A duck-typed context missing the field must mean 'execution', not crash."""
        from almanak.connectors.curve.compiler import _discovery_mode

        assert _discovery_mode(object()) is False

    def test_fail_closed_guards_still_refuse_without_the_flag(self) -> None:
        """The guards were not relaxed — only bypassed under discovery."""
        chain = "arbitrum"
        tricrypto = CURVE_TEST_POOLS[chain].get("tricrypto")
        if tricrypto is None:  # pragma: no cover - registry shape guard
            pytest.skip("no tricrypto pool registered on arbitrum")

        adapter = CurveAdapter(
            CurveConfig(chain=chain, wallet_address="0x" + "11" * 20)  # no rpc_url, no gateway, no discovery flag
        )
        pool_info = adapter.get_pool_info(tricrypto["address"], refresh=False)
        assert pool_info is not None
        with pytest.raises(ValueError, match="refusing to ship min_lp=0"):
            adapter._estimate_add_liquidity(pool_info, [10**18] * pool_info.n_coins)

    def test_discovery_flag_makes_the_same_call_succeed(self) -> None:
        """Same input, discovery flag on -> a positive bound instead of a refusal."""
        chain = "arbitrum"
        tricrypto = CURVE_TEST_POOLS[chain].get("tricrypto")
        if tricrypto is None:  # pragma: no cover - registry shape guard
            pytest.skip("no tricrypto pool registered on arbitrum")

        adapter = CurveAdapter(CurveConfig(chain=chain, wallet_address="0x" + "11" * 20, permission_discovery=True))
        pool_info = adapter.get_pool_info(tricrypto["address"], refresh=False)
        assert pool_info is not None
        quote = adapter._estimate_add_liquidity(pool_info, [10**18] * pool_info.n_coins)
        assert quote > 0, "synthetic discovery quote must be positive or the guard trips anyway"
