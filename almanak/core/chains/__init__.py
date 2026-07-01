"""Single source of truth for per-chain configuration.

This package replaces the ~8 chain-keyed dicts that were previously
scattered across ``almanak/core/enums.py``, ``almanak/core/constants.py``,
``almanak/gateway/validation.py``,
``almanak/framework/execution/gas/constants.py``,
``almanak/gateway/services/onchain_lookup.py``, and others.

Per-chain descriptor files (``ethereum.py``, ``arbitrum.py``, …) register
themselves into :class:`ChainRegistry` at import time. Importing this
package guarantees every supported chain is loaded, so consumers can rely
on ``ChainRegistry.get(...)`` / ``ChainRegistry.resolve(...)`` /
``ChainRegistry.all()`` without any further setup.

VIB-4801 (parent epic VIB-4800).

Public API::

    from almanak.core.chains import (
        ChainDescriptor, ChainRegistry, GasProfile,
        NativeToken, Timeouts, register_chain,
    )
"""

# Public types and the registry singleton must be importable BEFORE we
# trigger any per-chain registration, because the chain modules import
# ``ChainDescriptor`` etc. from these private submodules.
# Import the enum after the side-effect block so the runtime cross-check
# is performed once every descriptor has registered.
from almanak.core.enums import Chain  # noqa: E402

# Side-effect imports: each module calls ``register_chain`` at import
# time. Keep them sorted by canonical name so a missing chain is easy to
# spot in a code review.
from . import (  # noqa: F401  (side-effect imports — registration)
    arbitrum,
    avalanche,
    base,
    berachain,
    blast,
    bsc,
    ethereum,
    hyperevm,
    linea,
    mantle,
    monad,
    optimism,
    plasma,
    polygon,
    solana,
    sonic,
    xlayer,
    zerog,
)
from ._descriptor import (
    ChainDescriptor,
    GasProfile,
    NativeToken,
    RpcProfile,
    SimulationProfile,
    Timeouts,
)
from ._registry import ChainRegistry, register_chain
from .caip import parse_caip2, to_caip2
from .defaults import DEFAULT_CHAIN, DEFAULT_VAULT_CHAIN, LEGACY_SERIALIZED_CHAIN

# Runtime cross-check: every Chain enum member must have a descriptor.
# This catches the recurring "added a chain to the enum but forgot the
# descriptor file" failure mode at import time, rather than at the first
# lookup site (which might never run in CI for a rarely-touched chain).
_missing = [c for c in Chain if c not in {d.enum for d in ChainRegistry.all()}]
if _missing:
    raise RuntimeError(
        "ChainRegistry is missing descriptors for: "
        f"{[c.name for c in _missing]}. Add a file under "
        "almanak/core/chains/ for each missing chain."
    )

del _missing


__all__ = [
    "DEFAULT_CHAIN",
    "DEFAULT_VAULT_CHAIN",
    "LEGACY_SERIALIZED_CHAIN",
    "ChainDescriptor",
    "ChainRegistry",
    "GasProfile",
    "NativeToken",
    "RpcProfile",
    "SimulationProfile",
    "Timeouts",
    "parse_caip2",
    "register_chain",
    "to_caip2",
]
