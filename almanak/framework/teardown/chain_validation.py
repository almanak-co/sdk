"""Validate explicit teardown swap chains before reading or spending inventory."""

from typing import Any

from almanak.core.chains import ChainRegistry


def teardown_intent_type(intent: Any) -> str:
    """Read the serialized discriminator or the live intent enum."""
    kind = (
        (intent.get("type") or intent.get("intent_type"))
        if isinstance(intent, dict)
        else getattr(intent, "intent_type", None)
    )
    return str(getattr(kind, "value", kind) or "").rsplit(".", 1)[-1].upper()


def teardown_swap_chain_error(intent: Any, strategy: Any, market: Any) -> str | None:
    def field(name: str) -> Any:
        return intent.get(name) if isinstance(intent, dict) else getattr(intent, name, None)

    if teardown_intent_type(intent) != "SWAP":
        return None
    chain = field("chain")
    if not isinstance(chain, str) or not chain:
        return "Teardown SWAP requires an explicit chain"
    if chain not in ChainRegistry.names():
        return f"Teardown SWAP chain {chain!r} is unsupported; use a canonical chain name"
    configured = getattr(market, "chains", None)
    if not isinstance(configured, tuple | list | set):
        configured = getattr(strategy, "chains", None)
    if not isinstance(configured, tuple | list | set):
        configured = (getattr(strategy, "chain", None),)
    if chain not in configured:
        return f"Teardown SWAP chain {chain!r} conflicts with configured chains {tuple(configured)!r}"
    return None
