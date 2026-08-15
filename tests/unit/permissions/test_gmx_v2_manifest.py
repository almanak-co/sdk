"""Regression tests for the GMX V2 Safe manifest (VIB-5569).

PERP_CANCEL_ORDER is the teardown-recovery verb that cancels a stranded
(never-filled) GMX V2 pending order and refunds its committed collateral. It is
a DIRECT ``ExchangeRouter.cancelOrder(bytes32)`` call — a different selector
(``0x7489ec23``) than the ``multicall`` (``0xac9650d8``) grant that PERP_OPEN /
PERP_CLOSE synthetic discovery produces.

Before VIB-5569 the cancel selector was never discovered, so on a hosted
Safe-wallet deployment its Zodiac module permission was never authorised and a
teardown cancel was rejected at ``execTransactionWithRole`` (funds safe but
collateral not auto-recovered; VIB-5116 fail-closed semantics). These tests pin
that the manifest now authorises (ExchangeRouter, cancelOrder) for the cancel
intent, that the authorised selector is the REAL keccak of the signature (not a
hand-typed literal that could silently diverge from the compiler's calldata),
and that it is scoped to the cancel intent type (least privilege).
"""

from __future__ import annotations

import pytest
from eth_utils import function_signature_to_4byte_selector

from almanak.connectors.gmx_v2.adapter import GMX_CANCEL_ORDER_SELECTOR, GMX_V2_ADDRESSES
from almanak.framework.permissions.discovery import discover_permissions
from almanak.framework.permissions.generator import generate_manifest

# Derived from the signature here — NOT copied from the connector — so the test
# fails if the connector ever authorises a selector that isn't the real
# cancelOrder(bytes32) 4-byte selector. Cross-checked against the connector's own
# GMX_CANCEL_ORDER_SELECTOR below so a drift in either surfaces.
_CANCEL_ORDER_SEL = "0x" + function_signature_to_4byte_selector("cancelOrder(bytes32)").hex()


def _exchange_router_selectors(intent_types: list[str], chain: str = "arbitrum") -> set[str]:
    manifest = generate_manifest(
        strategy_name="gmx-v2-manifest-regression",
        chain=chain,
        supported_protocols=["gmx_v2"],
        intent_types=intent_types,
    )
    return {
        sel.selector.lower()
        for perm in manifest.permissions
        if perm.target.lower() == GMX_V2_ADDRESSES[chain]["exchange_router"].lower()
        for sel in perm.function_selectors
    }


class TestGmxV2Manifest:
    @pytest.mark.parametrize("chain", ["arbitrum", "avalanche"])
    @pytest.mark.parametrize("intent_type", ["PERP_OPEN", "PERP_CLOSE"])
    def test_open_and_close_manifest_authorises_exchange_router_multicall(
        self,
        chain: str,
        intent_type: str,
    ) -> None:
        """Each production chain and position verb authorises the compiler's
        real ExchangeRouter.multicall surface, independently of cancellation."""
        multicall = "0x" + function_signature_to_4byte_selector("multicall(bytes[])").hex()
        assert multicall in _exchange_router_selectors([intent_type], chain)

    def test_selector_constant_matches_real_signature(self) -> None:
        """The connector's GMX_CANCEL_ORDER_SELECTOR must equal the real
        keccak('cancelOrder(bytes32)')[:4] — pins the calldata source constant
        the manifest is built from against signature drift."""
        assert GMX_CANCEL_ORDER_SELECTOR.lower() == _CANCEL_ORDER_SEL

    def test_cancel_manifest_authorises_exchange_router_cancel_order(self) -> None:
        """A PERP_CANCEL_ORDER manifest must authorise ExchangeRouter.cancelOrder
        — the direct recovery path for a stranded pending order. Without it a
        Safe-wallet teardown cancel reverts at execTransactionWithRole."""
        selectors = _exchange_router_selectors(["PERP_CANCEL_ORDER"])
        assert _CANCEL_ORDER_SEL in selectors, (
            "Safe manifest missing ExchangeRouter.cancelOrder(bytes32) — a "
            "Safe-wallet gmx_v2 teardown cancel would revert at "
            "execTransactionWithRole and collateral would not be recovered."
        )

    def test_authorised_cancel_selector_matches_calldata(self) -> None:
        """The authorised cancel selector must equal the selector the compiler
        actually emits (GMX_CANCEL_ORDER_SELECTOR). If they diverge, the Safe
        authorises one selector while the compiler emits another → every cancel
        is rejected on-chain despite a green manifest."""
        selectors = _exchange_router_selectors(["PERP_CANCEL_ORDER"])
        assert GMX_CANCEL_ORDER_SELECTOR.lower() in selectors

    def test_non_perp_manifest_excludes_cancel_order(self) -> None:
        """Least privilege: a non-perp manifest (SWAP) must NOT authorise the
        cancelOrder selector."""
        assert _CANCEL_ORDER_SEL not in _exchange_router_selectors(["SWAP"])

    def test_perp_open_strategy_manifest_auto_includes_cancel(self) -> None:
        """END-TO-END: a gmx_v2 strategy that declares only PERP_OPEN must get
        cancelOrder in its Safe manifest without authoring the cancel verb.

        The manifest generator auto-expands PERP_OPEN → (PERP_CLOSE,
        PERP_CANCEL_ORDER) as teardown-recovery complements (VIB-5569). ALM-3101
        also makes cancellation strategy-authorable, but the expansion remains
        necessary for older strategies that declare only PERP_OPEN: teardown
        must still be able to recover a stranded pending order.
        """
        selectors = _exchange_router_selectors(["PERP_OPEN"])
        assert _CANCEL_ORDER_SEL in selectors, (
            "PERP_OPEN manifest must auto-include cancelOrder via teardown-complement "
            "expansion — otherwise a real gmx_v2 Safe strategy cannot recover a "
            "stranded pending order during teardown."
        )

    def test_cancel_selector_send_allowed_is_false(self) -> None:
        """A cancel carries value == 0 (no keeper execution fee) — the Safe must
        not be granted native-value send on the ExchangeRouter for cancel."""
        manifest = generate_manifest(
            strategy_name="gmx-v2-cancel-send-allowed-regression",
            chain="arbitrum",
            supported_protocols=["gmx_v2"],
            intent_types=["PERP_CANCEL_ORDER"],
        )
        exchange_router = GMX_V2_ADDRESSES["arbitrum"]["exchange_router"].lower()
        er_perms = [p for p in manifest.permissions if p.target.lower() == exchange_router]
        assert er_perms, "ExchangeRouter permission missing from cancel manifest"
        assert all(not p.send_allowed for p in er_perms), (
            "ExchangeRouter cancel must not allow native-value send (value == 0)"
        )


class TestNonGmxPerpCancelHarmless:
    """Guard the "harmless for non-gmx" claim: cancel is a gmx_v2-only compile
    path, so wiring PERP_CANCEL_ORDER into the shared teardown-complement
    expansion MUST NOT produce a cancel permission — or any warning/error — for
    perp connectors that cannot compile it. This is the test that breaks loudly
    if someone later hands a cancel builder to a connector with no cancel support.
    """

    @pytest.mark.parametrize(
        ("protocol", "chain"),
        [
            ("hyperliquid", "hyperevm"),
            ("aster_perps", "bsc"),
            ("pancakeswap_perps", "bsc"),
        ],
    )
    def test_non_gmx_perp_cancel_yields_nothing_and_no_warnings(self, protocol: str, chain: str) -> None:
        permissions, warnings = discover_permissions(
            chain=chain,
            protocols=[protocol],
            intent_types=["PERP_CANCEL_ORDER"],
        )
        # No connector emits a synthetic cancel intent except gmx_v2, so the
        # (protocol, PERP_CANCEL_ORDER) combination is skipped before compilation:
        # no cancel selector authorised anywhere, and no compile warning/error.
        cancel_selectors = {sel.selector.lower() for perm in permissions for sel in perm.function_selectors} & {
            _CANCEL_ORDER_SEL,
            GMX_CANCEL_ORDER_SELECTOR.lower(),
        }
        assert not cancel_selectors, f"{protocol} unexpectedly authorised a cancelOrder selector"
        assert warnings == [], f"{protocol} PERP_CANCEL_ORDER discovery produced warnings: {warnings}"
