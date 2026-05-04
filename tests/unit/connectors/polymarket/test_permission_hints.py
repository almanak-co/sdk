"""V2 permission-hints tests.

V2 cutover added a static permission set for Polymarket because the
IntentCompiler can't discover the on-chain entry points via synthetic
intent compilation (PREDICTION_BUY / SELL produce ActionBundles with
empty `transactions=[]`). This test pins the manifest so a future
refactor can't silently drop a contract or selector.
"""

from __future__ import annotations

from web3 import Web3

from almanak.framework.connectors.polymarket.models import (
    COLLATERAL_OFFRAMP,
    COLLATERAL_ONRAMP,
    CONDITIONAL_TOKENS,
    CTF_EXCHANGE_V2,
    NEG_RISK_ADAPTER,
    NEG_RISK_EXCHANGE_V2,
    PUSD,
    USDC_NATIVE_POLYGON,
    USDCE_POLYGON,
)
from almanak.framework.connectors.polymarket.permission_hints import PERMISSION_HINTS


def _by_target(targets: list, address: str):
    """Pick the StaticPermissionEntry whose target matches address (case-insensitive)."""
    for entry in targets:
        if entry.target.lower() == address.lower():
            return entry
    return None


class TestV2PermissionHints:
    """Pin the V2 static permission entries: every contract address that
    the strategy *initiates* an on-chain call against must be listed."""

    def test_polygon_chain_present(self) -> None:
        assert "polygon" in PERMISSION_HINTS.static_permissions

    def test_static_entries_cover_all_v2_targets(self) -> None:
        """Each of the 9 V2 contracts the strategy ever calls must be listed
        as a target. Missing one means a manifest gap → real-world rejection."""
        targets = PERMISSION_HINTS.static_permissions["polygon"]
        target_addresses = {e.target.lower() for e in targets}
        for required in [
            USDCE_POLYGON,
            USDC_NATIVE_POLYGON,
            PUSD,
            COLLATERAL_ONRAMP,
            COLLATERAL_OFFRAMP,
            CONDITIONAL_TOKENS,
            CTF_EXCHANGE_V2,
            NEG_RISK_EXCHANGE_V2,
            NEG_RISK_ADAPTER,
        ]:
            assert required.lower() in target_addresses, f"Missing target: {required}"

    def test_source_assets_have_approve(self) -> None:
        """Both source assets need approve() so the wallet can wrap to pUSD."""
        targets = PERMISSION_HINTS.static_permissions["polygon"]
        approve = "0x095ea7b3"
        for asset in (USDCE_POLYGON, USDC_NATIVE_POLYGON):
            entry = _by_target(targets, asset)
            assert entry is not None
            assert approve in entry.selectors, f"{asset} missing approve selector"

    def test_pusd_has_approve(self) -> None:
        """pUSD needs approve() for both V2 exchanges and the NegRisk Adapter."""
        targets = PERMISSION_HINTS.static_permissions["polygon"]
        entry = _by_target(targets, PUSD)
        assert entry is not None
        assert "0x095ea7b3" in entry.selectors

    def test_onramp_has_wrap(self) -> None:
        """CollateralOnramp.wrap selector must be in the manifest."""
        targets = PERMISSION_HINTS.static_permissions["polygon"]
        entry = _by_target(targets, COLLATERAL_ONRAMP)
        assert entry is not None
        wrap_selector = "0x" + Web3.keccak(text="wrap(address,address,uint256)")[:4].hex()
        assert wrap_selector in entry.selectors

    def test_offramp_has_unwrap(self) -> None:
        """CollateralOfframp.unwrap selector must be in the manifest."""
        targets = PERMISSION_HINTS.static_permissions["polygon"]
        entry = _by_target(targets, COLLATERAL_OFFRAMP)
        assert entry is not None
        unwrap_selector = "0x" + Web3.keccak(text="unwrap(address,address,uint256)")[:4].hex()
        assert unwrap_selector in entry.selectors

    def test_selector_labels_match_canonical_signatures(self) -> None:
        """The label table is consumed by manifest renderers; ensure the
        keccak prefixes match the labelled signatures."""
        for selector, signature in PERMISSION_HINTS.selector_labels.items():
            expected = "0x" + Web3.keccak(text=signature)[:4].hex()
            assert selector == expected, f"Mismatch: {signature} → {selector} != {expected}"

    def test_v2_exchanges_listed_as_operator_targets(self) -> None:
        """V2 exchanges receive setApprovalForAll(CTF, true) as the operator —
        listed but with empty selectors (the call is on CTF, not the exchange)."""
        targets = PERMISSION_HINTS.static_permissions["polygon"]
        for exchange in (CTF_EXCHANGE_V2, NEG_RISK_EXCHANGE_V2):
            entry = _by_target(targets, exchange)
            assert entry is not None
            # The strategy never directly calls the exchange — operator approval
            # is a setApprovalForAll on the CTF token.
            assert entry.selectors == {}

    def test_neg_risk_adapter_listed(self) -> None:
        """NegRisk Adapter handles split/merge for multi-outcome markets."""
        targets = PERMISSION_HINTS.static_permissions["polygon"]
        entry = _by_target(targets, NEG_RISK_ADAPTER)
        assert entry is not None
