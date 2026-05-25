"""ChainFamily — behavior protocol that owns state-machine-specific operations.

VIB-4803 (parent epic VIB-4800) promotes :class:`almanak.core.enums.ChainFamily`
from a label enum into a real behavior seam. The enum stays as a label / kind
discriminator (preserved verbatim for ``CHAIN_FAMILY_MAP`` byte-identity in
VIB-4801), and this module adds a :class:`ChainFamilyAdapter` protocol with
:class:`EvmFamily` and :class:`SvmFamily` implementations that own the
state-machine-specific operations: signing, address formatting, intent
compilation, and (eventually) receipt envelope normalization.

Why this lives in ``almanak/framework/`` and not in ``almanak/core/chains/``:

    The adapters call into framework code (the intent compiler, the EVM signer
    hierarchy). ``almanak/core/chains/`` is guarded by ``TestImportGraphIsolation``
    — it must remain importable without pulling in any of ``almanak.framework.*``
    or ``almanak.gateway.*``. Placing the adapters here keeps that boundary
    clean: ``ChainDescriptor.family`` is still the pure enum kind, and the
    behavior adapter is looked up via :func:`family_for` keyed off the enum.

Adding a new family is now strictly local:

    1. Add a new member to :class:`almanak.core.enums.ChainFamily` (and a row in
       ``CHAIN_FAMILY_MAP``).
    2. Write a new ``MyFamily`` adapter implementing :class:`ChainFamilyAdapter`.
    3. Register it in :data:`_FAMILY_ADAPTERS` below.

The compiler / signer / receipt-parser path does NOT need to change.
"""

from __future__ import annotations

from ._family import (
    ChainFamilyAdapter,
    CompileContext,
    EvmFamily,
    SvmFamily,
    all_families,
    family_for,
    family_for_chain_enum,
    family_for_kind,
)

__all__ = [
    "ChainFamilyAdapter",
    "CompileContext",
    "EvmFamily",
    "SvmFamily",
    "all_families",
    "family_for",
    "family_for_chain_enum",
    "family_for_kind",
]
