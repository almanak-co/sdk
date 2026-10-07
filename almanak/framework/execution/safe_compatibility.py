"""Which strategy protocols can be operated from a Safe wallet."""

from __future__ import annotations

from collections.abc import Mapping, Sequence


def safe_unsupported_protocol_error(strategy_protocols: Mapping[str, Sequence[str]] | None) -> str | None:
    """Explain why a Safe-mode run cannot use the strategy's protocols, or ``None``.

    Connectors declaring ``safe_supported=False`` authorize accounts with EOA
    signatures only; a Safe-held account on such a venue could never trade.
    """
    from almanak.connectors._connector import CONNECTOR_REGISTRY

    blocked = CONNECTOR_REGISTRY.safe_unsupported_names()
    if not blocked or not isinstance(strategy_protocols, Mapping):
        return None
    used = {str(p).lower() for protocols in strategy_protocols.values() for p in (protocols or [])}
    offending = sorted(used & blocked)
    if not offending:
        return None
    return (
        f"Protocol(s) {', '.join(offending)} cannot be operated from a Safe wallet: the venue only "
        "authorizes EOA accounts. Run this strategy with an EOA wallet."
    )
