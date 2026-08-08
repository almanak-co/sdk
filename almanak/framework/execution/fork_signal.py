"""Positive "this is a managed fork" signal for money-path guards (ALM-3184).

Why this module exists
----------------------
``almanak.framework.execution.simulator.config.is_local_rpc`` answers a
*different* question than the one money-path guards need. It returns True for
any URL containing ``anvil`` / ``localhost`` / ``hardhat`` / … **or any host on
port 8545-8550**. For choosing a simulation vendor that is fine: a false
positive only costs a skipped Tenderly call.

Money-path consumers used it to key real safety behaviour instead:

* the Uniswap-V3-family compilers skipped the **oracle price-impact guard** —
  the only independent cross-check that an on-chain quote has not been
  manipulated or drained (a real ~99.85% value-destroying TraderJoe swap was
  caught by that guard, VIB-5740); and
* the Enso deferred-refresh provider force-widened slippage to 500 bps.

A production RPC proxy listening on ``:8545``, or an internal hostname that
merely contains the substring ``anvil``, therefore ran **mainnet** swaps with
the manipulation guard off and 5% slippage granted. The URL is attacker- and
ops-influenced input; it is not evidence about what is behind it.

The contract: declaration only
------------------------------
A guard may relax **only** when the deployment positively declares a managed
fork. ``--network anvil`` / ``ALMANAK_NETWORK=anvil`` /
``GatewaySettings.network`` resolve to :attr:`Network.ANVIL`, which
:func:`is_managed_fork_network` maps to the boolean threaded through config as
``managed_fork``.

There is deliberately **no runtime detection here.** An earlier revision of
this module probed the endpoint with ``anvil_nodeInfo`` to auto-detect forks
that nothing had declared. That was a gateway-boundary bypass and was removed:
``almanak/framework/`` is strategy-container code, whose only sanctioned egress
is the gateway gRPC channel (AGENTS.md §Gateway boundary). Loopback is not an
exception — blueprint 20 records that the sidecar guard rejects every
non-gateway loopback connect — and because this module is re-exported from
``almanak.framework.execution``, the probe was reachable by strategy code as a
general-purpose loopback socket primitive against any local service. Narrowing
which URLs it would dial mitigated the blast radius; it did not make the socket
legitimate.

If runtime fork detection is ever genuinely required, it belongs behind a typed
**gateway capability** — the gateway is the egress layer — not behind a helper
in the strategy container.

**Fail-safe direction: anything other than a literal ``True`` is PRODUCTION**
(guard ON, slippage NOT widened). Absent, unknown, and malformed all land on
the safe side. A missed fork signal makes an Anvil test stricter; a missed
*production* signal loses money. Only one of those is recoverable — and the
remedy for the first is to declare the signal, which is cheap and explicit.
"""

from __future__ import annotations

import logging

from almanak.core.rpc_network import Network

logger = logging.getLogger(__name__)

__all__ = [
    "is_managed_fork_network",
    "resolve_managed_fork",
]


def is_managed_fork_network(network: Network | str | None) -> bool:
    """Return True only when ``network`` explicitly declares a managed Anvil fork.

    The single mapping from a declared network environment to the fork signal.
    Anything unparseable is production — a malformed declaration is not a
    licence to disable a guard.
    """
    if network is None:
        return False
    try:
        parsed = Network.parse(network)
    except (ValueError, TypeError):
        logger.debug("Unparseable network %r treated as production for fork-signal purposes", network)
        return False
    return parsed is Network.ANVIL


def resolve_managed_fork(declared: bool | None) -> bool:
    """Resolve the managed-fork signal for a money-path guard.

    The check is by **identity**, not truthiness. Only the literal ``True``
    relaxes a guard: a truthy non-bool that reached this boundary — the string
    ``"false"``, a stray object, a test double's auto-attribute — is a
    malformed declaration, and a malformed declaration must never be worth more
    than an absent one. ``MagicMock()`` is the case that matters in practice,
    and it is why this is ``is True`` rather than ``if declared``.

    Args:
        declared: The threaded config declaration. ``None`` means nobody
            declared, which resolves to production.

    Returns:
        True only for an explicit ``True``. Never derived from a URL, and never
        from contacting anything.
    """
    if declared is True:
        return True
    if declared is not False and declared is not None:
        logger.warning(
            "Malformed managed-fork declaration of type %r (expected bool | None); treating as production",
            type(declared).__name__,
        )
    return False
