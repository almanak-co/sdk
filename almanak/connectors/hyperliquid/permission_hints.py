"""Permission discovery hints for Hyperliquid (HyperEVM CoreWriter).

Safe-wallet strategies authorise contracts via the Zodiac Roles manifest. Most
connectors let the manifest be *discovered* by compiling synthetic intents
offline (``framework/permissions/discovery.py``) — but hyperliquid's compiler
cannot run offline: both ``PERP_OPEN`` and ``PERP_CLOSE`` require live HyperCore
reads (oracle price via precompile ``0x0807``; open-position via ``0x0800``) to
anchor a fail-closed order, and it hard-fails when those reads are unavailable
(by design — never send a blind order). That is exactly the case
``StaticPermissionEntry`` exists for ("protocols where compilation requires
external state (RPC) not available during offline discovery").

So the Safe permission is declared **statically**, which is also the *more
correct* model here: hyperliquid executes every perp action through a SINGLE
fixed system contract — ``CoreWriter`` (``0x3333…3333``, a protocol constant, not
a discovered address) via ``sendRawAction(bytes)`` — and needs NO ERC-20 approve
(margin lives on HyperCore, gas is native HYPE, ``value == 0``). There is nothing
dynamic to discover. The target address and selector below are taken from the
connector's own ``addresses.py`` / ``sdk.py`` constants so they cannot drift from
the encoder that actually builds the calldata.

Without this entry a Safe-wallet hyperliquid strategy gets NO permission for
``CoreWriter.sendRawAction`` and every order reverts at ``execTransactionWithRole``
(Zodiac Roles: unauthorized).
"""

from almanak.framework.permissions.hints import PermissionHints, StaticPermissionEntry

from .addresses import CORE_WRITER_ADDRESS
from .sdk import SELECTOR_SEND_RAW_ACTION

# ``sendRawAction(bytes)`` selector, derived from the signature at import time in
# ``sdk.py`` (keccak(b"sendRawAction(bytes)")[:4]) — referencing it here keeps the
# Safe manifest byte-identical to the calldata the compiler emits.
_SEND_RAW_ACTION_SELECTOR = "0x" + SELECTOR_SEND_RAW_ACTION.hex()
_SEND_RAW_ACTION_LABEL = "sendRawAction(bytes)"

PERMISSION_HINTS = PermissionHints(
    selector_labels={_SEND_RAW_ACTION_SELECTOR: _SEND_RAW_ACTION_LABEL},
    static_permissions={
        "hyperevm": [
            StaticPermissionEntry(
                target=CORE_WRITER_ADDRESS.lower(),
                label="Hyperliquid CoreWriter",
                selectors={_SEND_RAW_ACTION_SELECTOR: _SEND_RAW_ACTION_LABEL},
                send_allowed=False,  # CoreWriter calls carry value == 0 (see compiler._core_writer_tx)
                intent_types=frozenset({"PERP_OPEN", "PERP_CLOSE"}),
            )
        ]
    },
)
