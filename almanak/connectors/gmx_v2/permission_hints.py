"""GMX V2 permission hints for permission discovery."""

from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints(
    selector_labels={
        "0xac9650d8": "multicall(bytes[])",
        # PERP_CANCEL_ORDER (VIB-5568) calls this DIRECTLY on the ExchangeRouter
        # (not via multicall). Labelled for discovery observability.
        "0x7489ec23": "cancelOrder(bytes32)",
    },
    # Synthetic-discovery participation (VIB-4928): perp open + close.
    #
    # PERP_CANCEL_ORDER (VIB-5568) is intentionally NOT here yet. A cancel is a
    # direct ``cancelOrder(bytes32)`` call (selector 0x7489ec23 above) — a
    # different selector than the ``multicall`` PERP_OPEN/PERP_CLOSE grant. On a
    # hosted SAFE-WALLET deployment its module permission is therefore not
    # pre-approved by open/close discovery, so a Safe-wallet teardown cancel is
    # REJECTED by the module and the pending order is surfaced LOUD + fail-closed
    # (VIB-5116 semantics: no silent loss, manual-check flagged) rather than
    # recovered. Managed-Anvil (auto-impersonation) and EOA hosted deployments
    # recover normally. Wiring PERP_CANCEL_ORDER into synthetic discovery (builder
    # + derived-membership fold + pinned equivalence test) is a separable
    # hosted-perimeter change tracked as VIB-5569.
    synthetic_discovery_intents=frozenset({"PERP_OPEN", "PERP_CLOSE"}),
)
