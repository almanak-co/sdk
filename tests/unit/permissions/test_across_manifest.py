"""Regression tests for the Across (V3 SpokePool) Safe manifest — VIB-5921.

Across cannot use compilation-based permission discovery at all: ``BRIDGE`` is
not in ``_VALID_SYNTHETIC_INTENTS`` (declaring it raises at import), and
``BridgeCompiler`` needs a live Across API quote before it can build any
transaction, so nothing compiles offline for a selector to be discovered from.
The connector therefore declares the single SpokePool permission per chain via
``static_permissions`` (``permission_hints.py``).

Before VIB-5921 ``across/permission_hints.py`` was the empty boilerplate, so a
Safe-wallet Across strategy got a manifest with ZERO Across targets and every
bridge reverted at ``execTransactionWithRole`` — silently, since the connector
coverage gate only asserts the file exists.

These tests pin: the authorised selector is the REAL keccak of the depositV3
signature (validating the hand-typed ``7b939232`` constant for the first time),
every declared strategy chain authorises its own SpokePool with native-value
send enabled, non-BRIDGE manifests exclude it (least privilege), and the
addresses equal the connector's own constant table.
"""

from __future__ import annotations

import pytest
from eth_utils import function_signature_to_4byte_selector

from almanak.connectors.across.adapter import (
    ACROSS_CHAIN_IDS,
    ACROSS_SPOKE_POOL_ADDRESSES,
    DEPOSIT_V3_SELECTOR,
)
from almanak.connectors.across.connector import CONNECTOR
from almanak.framework.permissions.generator import generate_manifest
from almanak.framework.permissions.models import ContractPermission

# Derived from the signature HERE — never copied from the connector — so the
# test fails if the connector authorises a selector that isn't the real
# depositV3 4-byte selector (the class of bug where the Safe authorises one
# selector while the compiler emits calldata with another).
_DEPOSIT_V3_SEL = (
    "0x"
    + function_signature_to_4byte_selector(
        "depositV3(address,address,address,address,uint256,uint256,uint256,address,uint32,uint32,uint32,bytes)"
    ).hex()
)
_APPROVE_SEL = "0x" + function_signature_to_4byte_selector("approve(address,uint256)").hex()

_DECLARED_CHAINS = tuple(CONNECTOR.strategy_chains or ())


def _spoke_pool(chain: str) -> str:
    return ACROSS_SPOKE_POOL_ADDRESSES[ACROSS_CHAIN_IDS[chain]].lower()


def _across_permissions(
    chain: str,
    intent_types: list[str] | None = None,
    config: dict | None = None,
) -> list[ContractPermission]:
    manifest = generate_manifest(
        strategy_name="across-manifest-regression",
        chain=chain,
        supported_protocols=["across"],
        intent_types=intent_types or ["BRIDGE"],
        config=config,
    )
    return [p for p in manifest.permissions if p.target.lower() == _spoke_pool(chain)]


class TestAcrossManifest:
    def test_connector_declares_chains(self) -> None:
        """Guard the parametrisation below against an empty universe."""
        assert _DECLARED_CHAINS, "across declares no strategy_chains"

    def test_selector_constant_matches_real_signature(self) -> None:
        """``DEPOSIT_V3_SELECTOR`` must equal keccak(depositV3(...))[:4].

        The constant was hand-typed inline in ``adapter.build_deposit_tx``; this
        is the first check that it is the real selector. If it diverges, every
        Across deposit builds calldata no SpokePool can dispatch — and the Safe
        manifest would authorise the same wrong selector, hiding it.
        """
        assert "0x" + DEPOSIT_V3_SELECTOR.hex() == _DEPOSIT_V3_SEL

    @pytest.mark.parametrize("chain", _DECLARED_CHAINS)
    def test_bridge_manifest_authorises_spoke_pool_deposit_v3(self, chain: str) -> None:
        """Every declared chain must authorise exactly SpokePool.depositV3 with
        native-value send (ETH/WETH bridges carry ``value == amount_wei``)."""
        perms = _across_permissions(chain)
        assert perms, (
            f"Safe manifest for {chain} has no Across SpokePool target — a "
            "Safe-wallet bridge would revert at execTransactionWithRole."
        )
        selectors = {sel.selector.lower() for p in perms for sel in p.function_selectors}
        assert selectors == {_DEPOSIT_V3_SEL}, (
            f"{chain}: SpokePool must authorise exactly the real depositV3 selector; got {selectors}"
        )
        assert all(p.send_allowed for p in perms), (
            f"{chain}: SpokePool must allow native-value send — an ETH bridge sends value with depositV3."
        )

    @pytest.mark.parametrize("chain", _DECLARED_CHAINS)
    def test_spoke_pool_address_matches_connector_constant(self, chain: str) -> None:
        """The authorised target is the connector's own SpokePool constant for
        that chain's id — no hand-typed address can drift into the manifest."""
        expected = ACROSS_SPOKE_POOL_ADDRESSES[ACROSS_CHAIN_IDS[chain]].lower()
        assert [p.target.lower() for p in _across_permissions(chain)] == [expected]

    def test_non_bridge_manifest_excludes_spoke_pool(self) -> None:
        """Least privilege: the static entry is scoped to BRIDGE, so a SWAP-only
        strategy must not authorise the SpokePool."""
        assert _across_permissions("base", intent_types=["SWAP"]) == []

    def test_undeclared_chain_has_no_spoke_pool(self) -> None:
        """zkSync has a SpokePool constant but is NOT in ``strategy_chains`` —
        the manifest universe is the declared chain set, so it must stay out."""
        manifest = generate_manifest(
            strategy_name="across-undeclared-chain",
            chain="zksync",
            supported_protocols=["across"],
            intent_types=["BRIDGE"],
        )
        zk_spoke = ACROSS_SPOKE_POOL_ADDRESSES[ACROSS_CHAIN_IDS["zksync"]].lower()
        assert all(p.target.lower() != zk_spoke for p in manifest.permissions)

    def test_bridged_token_approve_comes_from_config(self) -> None:
        """The ERC-20 approve leg (``approve(SpokePool, amount)``, emitted by
        BridgeCompiler for non-native tokens) is NOT declared by the connector —
        it is produced generically from the strategy config by
        ``generator._extract_token_permissions``. Pin that path so the
        documented division of labour (see ``permission_hints`` docstring)
        cannot silently disappear.
        """
        manifest = generate_manifest(
            strategy_name="across-approve-surface",
            chain="base",
            supported_protocols=["across"],
            intent_types=["BRIDGE"],
            config={"from_token": "USDC"},
        )
        approve_targets = {
            p.target.lower()
            for p in manifest.permissions
            if any(s.selector.lower() == _APPROVE_SEL for s in p.function_selectors)
        }
        assert approve_targets, (
            "No ERC-20 approve target in the manifest — a Safe bridging USDC could not approve the SpokePool."
        )


class TestLoudFailureOnMissingMappings:
    """VIB-5921 D4.2 (spec-critique round 2): the silent-omission failure mode
    needs a RUNNABLE oracle, not a code read. A declared strategy chain that is
    missing from either connector constant table must RAISE when the static
    permissions are built — a silently-omitted chain reproduces the exact
    zero-permission ``execTransactionWithRole`` revert this ticket fixes, one
    chain at a time.
    """

    def test_declared_chain_missing_chain_id_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.connectors.across import adapter, permission_hints

        stripped = {k: v for k, v in adapter.ACROSS_CHAIN_IDS.items() if k != "linea"}
        monkeypatch.setattr(adapter, "ACROSS_CHAIN_IDS", stripped)
        # permission_hints binds the tables at import; patch its module globals
        # the same way so the builder sees the mutated view.
        monkeypatch.setattr(permission_hints, "ACROSS_CHAIN_IDS", stripped)
        with pytest.raises(ValueError, match="linea"):
            permission_hints._build_static_permissions()

    def test_declared_chain_missing_spoke_pool_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from almanak.connectors.across import adapter, permission_hints

        linea_id = adapter.ACROSS_CHAIN_IDS["linea"]
        stripped = {k: v for k, v in adapter.ACROSS_SPOKE_POOL_ADDRESSES.items() if k != linea_id}
        monkeypatch.setattr(adapter, "ACROSS_SPOKE_POOL_ADDRESSES", stripped)
        monkeypatch.setattr(permission_hints, "ACROSS_SPOKE_POOL_ADDRESSES", stripped)
        with pytest.raises(ValueError, match="linea"):
            permission_hints._build_static_permissions()

    def test_intact_tables_build_every_declared_chain(self) -> None:
        from almanak.connectors.across import permission_hints

        built = permission_hints._build_static_permissions()
        assert set(built) == set(_DECLARED_CHAINS)
        assert all(len(entries) == 1 for entries in built.values())

    def test_module_import_itself_fails_loudly_when_table_incomplete(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Spec-critique round 3: the loud failure must fire at MODULE IMPORT —
        an implementation that catches the builder's ValueError during module
        initialization and publishes empty permissions would pass the
        direct-call tests above while shipping the exact silent-omission this
        ticket fixes. Reload the module under a mutated table and require the
        import itself to raise."""
        import importlib

        from almanak.connectors.across import adapter, permission_hints

        stripped = {k: v for k, v in adapter.ACROSS_CHAIN_IDS.items() if k != "linea"}
        monkeypatch.setattr(adapter, "ACROSS_CHAIN_IDS", stripped)
        try:
            with pytest.raises(ValueError, match="linea"):
                importlib.reload(permission_hints)
        finally:
            # Restore the pristine module for every later test: undo the table
            # patch first, then re-execute the module so PERMISSION_HINTS is
            # rebuilt from the intact constants.
            monkeypatch.undo()
            importlib.reload(permission_hints)
