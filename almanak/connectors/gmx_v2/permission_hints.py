"""GMX V2 permission hints for permission discovery."""

from almanak.core.intent_types import IntentType
from almanak.framework.permissions.hints import PermissionHints

PERMISSION_HINTS = PermissionHints(
    selector_labels={
        "0xac9650d8": "multicall(bytes[])",
        # PERP_CANCEL_ORDER (VIB-5568) calls this DIRECTLY on the ExchangeRouter
        # (not via multicall). Labelled for discovery observability.
        "0x7489ec23": "cancelOrder(bytes32)",
    },
    # Synthetic-discovery participation (VIB-4928): perp open + close + cancel.
    #
    # PERP_CANCEL_ORDER (VIB-5569) is a direct ``cancelOrder(bytes32)`` call
    # (selector 0x7489ec23 above) — a DIFFERENT selector than the ``multicall``
    # PERP_OPEN/PERP_CLOSE grant. Before VIB-5569 it was NOT discovered, so on a
    # hosted SAFE-WALLET deployment its Zodiac module permission was never
    # pre-approved by open/close discovery: a Safe-wallet teardown cancel was
    # REJECTED by the module and the pending order surfaced LOUD + fail-closed
    # (VIB-5116 semantics: no silent loss, manual-check flagged) rather than
    # recovered. Declaring it here wires it into synthetic discovery (builder in
    # ``permissions/synthetic_intents.py`` + derived-membership fold), so the Safe
    # manifest now authorises (ExchangeRouter, 0x7489ec23) scoped to
    # PERP_CANCEL_ORDER and the teardown cancel recovers collateral. gmx_v2 is the
    # ONLY perp connector that supports cancel; the builder gates on this
    # declaration so no other perp connector inherits a cancel it cannot compile.
    # ALM-3199 gives open/close a bounded generated market record, and the SDK's
    # discovery mode builds deterministic calldata without a transport. Opt out
    # of IntentCompiler's implicit public-RPC fallback so manifest bytes cannot
    # depend on RPC weather.
    offline_discovery=True,
    synthetic_discovery_intents=frozenset({IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.PERP_CANCEL_ORDER}),
)
