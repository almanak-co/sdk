"""SDK-wide default-chain POLICY constants (VIB-4851 Phase E, CS-1).

These three constants replace ~140 scattered ``"arbitrum"`` / ``"base"``
default literals across the framework, CLI, gateway, and agent-tools
layers. They live in ``almanak/core/chains/`` (the canonical chain home)
because "which chain do we assume when the caller didn't say" is
chain-domain policy — but note they are **policy**, not per-chain facts:
nothing about the Arbitrum descriptor makes it the default; the product
chose it.

Three constants, three distinct meanings — do not merge them:

``DEFAULT_CHAIN``
    Current policy: the chain a NEW operation targets when the caller does
    not specify one (CLI ``--chain`` flags, agent-tool schema defaults,
    function-signature defaults). Changing this value is a product
    decision; it changes behaviour for every unspecified-chain call site
    at once.

``LEGACY_SERIALIZED_CHAIN``
    Frozen history: the chain implied by serialized records (DB rows,
    saved configs, state snapshots) written before the ``chain`` field
    existed. Used exclusively by ``data.get("chain", ...)``-style
    deserialization fallbacks. **Never change this value** — it is a fact
    about old data, not a preference. It is split from ``DEFAULT_CHAIN``
    precisely so a future default-chain flip cannot silently rewrite how
    legacy records deserialize.

``DEFAULT_VAULT_CHAIN``
    Agent-tools vault (ERC-4626) tool default. The vault tooling launched
    on Base and its schemas default there; kept separate because it is a
    different product surface with a different home chain.

The agent-tools call sites are SPEND-CONTROL surface (``AgentPolicy``
defaults, tool schema defaults). ``tests/unit/core/test_chain_defaults.py``
freezes all three values so a change here fails loudly and demands
explicit review.
"""

from __future__ import annotations

from typing import Final

DEFAULT_CHAIN: Final[str] = "arbitrum"

LEGACY_SERIALIZED_CHAIN: Final[str] = "arbitrum"

DEFAULT_VAULT_CHAIN: Final[str] = "base"
