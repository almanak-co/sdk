"""Permission discovery hints for Across (V3 SpokePool bridge).

Safe-wallet strategies authorise every ``(target, selector)`` via a Zodiac Roles
manifest. Most connectors let that manifest be *discovered* by compiling
synthetic intents offline (``framework/permissions/discovery.py``) — but Across
cannot use that path at all:

* ``BRIDGE`` is not a member of ``_VALID_SYNTHETIC_INTENTS``
  (``framework/permissions/synthetic_intents.py``) — synthetic discovery only
  covers SWAP / LP / lending / perp, and *declaring* ``"BRIDGE"`` in
  ``synthetic_discovery_intents`` raises at import time.
* Even if it were declarable, ``BridgeCompiler`` needs a **live Across API
  quote** (fees, output amount, quote timestamp, exclusive relayer) before it
  can build any transaction, so it hard-fails offline and emits no calldata to
  discover a selector from.

That is exactly the case ``StaticPermissionEntry`` exists for. Before VIB-5921
this module was the empty boilerplate, so an Across Safe strategy got **zero**
permissions on every chain and each bridge reverted at
``execTransactionWithRole`` (Zodiac Roles: unauthorized) — silently, because the
``test_connector_coverage.py`` gate only checks that this file exists.

**Shape of an Across bridge** (``_strategy_base/bridge_compiler.py``): an ERC-20
``approve(SpokePool, amount)`` on the bridged token (skipped for native ETH),
then a single ``SpokePool.depositV3(...)`` — value-bearing when the token is
ETH/WETH (``adapter.build_deposit_tx``), hence ``send_allowed=True`` below.

**ERC-20 approve leg**: NOT declared here. Token-approve permissions are
produced generically by the manifest generator from the strategy's own config
(``generator._extract_token_permissions`` scans ``from_token`` / ``to_token`` /
… and ``anvil_funding`` keys, and emits ``approve`` on each resolved token,
un-scoped by spender). Declaring per-chain token approves here would duplicate —
and could contradict — that surface. A strategy whose config never names the
bridged token gets no approve permission; that is a generic generator property,
not an Across-specific gap.

The target addresses and the selector are taken from the connector's own
``adapter.py`` constants (``ACROSS_SPOKE_POOL_ADDRESSES``, ``ACROSS_CHAIN_IDS``,
``DEPOSIT_V3_SELECTOR``) and the chain universe from the connector manifest's
``strategy_chains`` — nothing is hand-typed, so the manifest cannot drift from
the encoder that actually builds the calldata.

VIB-5929 tracks the general hole (13 connectors shipping empty
``PermissionHints`` + a CI gate that would catch a zero-permission manifest).
"""

from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry

from .adapter import ACROSS_CHAIN_IDS, ACROSS_SPOKE_POOL_ADDRESSES, DEPOSIT_V3_SELECTOR
from .connector import CONNECTOR

_DEPOSIT_V3_SELECTOR = "0x" + DEPOSIT_V3_SELECTOR.hex()
_DEPOSIT_V3_LABEL = (
    "depositV3(address,address,address,address,uint256,uint256,uint256,address,uint32,uint32,uint32,bytes)"
)


def _build_static_permissions() -> dict[str, list[StaticPermissionEntry]]:
    """One SpokePool entry per chain the connector declares strategy support for.

    Built from the connector's own constants so the Safe manifest cannot drift
    from the calldata. Fails loudly at import if a declared chain has no chain-id
    or SpokePool address — a silently-omitted chain is exactly the failure mode
    VIB-5921 fixes (empty manifest → every bridge reverts).
    """
    result: dict[str, list[StaticPermissionEntry]] = {}
    for chain in CONNECTOR.strategy_chains or ():
        chain_id = ACROSS_CHAIN_IDS.get(chain)
        if chain_id is None:
            raise ValueError(
                f"across declares strategy_chains entry {chain!r} with no ACROSS_CHAIN_IDS mapping — "
                "the Safe manifest would silently omit that chain and every bridge would revert "
                "at execTransactionWithRole."
            )
        spoke_pool = ACROSS_SPOKE_POOL_ADDRESSES.get(chain_id)
        if spoke_pool is None:
            raise ValueError(
                f"across declares strategy_chains entry {chain!r} (chain id {chain_id}) with no "
                "ACROSS_SPOKE_POOL_ADDRESSES entry — the Safe manifest would silently omit that chain."
            )
        result[chain] = [
            StaticPermissionEntry(
                target=spoke_pool.lower(),
                label="Across SpokePool",
                selectors={_DEPOSIT_V3_SELECTOR: _DEPOSIT_V3_LABEL},
                # ETH/WETH bridges send native value with the deposit
                # (adapter.build_deposit_tx: value = amount_wei) — Zodiac Roles
                # rejects a value-bearing call without this.
                send_allowed=True,
                intent_types=frozenset({"BRIDGE"}),
            )
        ]
    return result


PERMISSION_HINTS = PermissionHints(
    selector_labels={_DEPOSIT_V3_SELECTOR: _DEPOSIT_V3_LABEL},
    static_permissions=_build_static_permissions(),
)
